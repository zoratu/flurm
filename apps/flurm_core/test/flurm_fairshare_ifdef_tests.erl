%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_fairshare internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise the calculate_priority_factor function which
%%% is exported only when compiled with -DTEST.
%%%
%%% Note: calculate_priority_factor/3 requires ETS tables to exist.
%%% Tests use try/catch to handle missing ETS gracefully.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_fairshare_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% ETS table names used by flurm_fairshare
-define(USAGE_TABLE, flurm_fairshare_usage).
-define(SHARES_TABLE, flurm_fairshare_shares).
-define(DEFAULT_SHARES, 1).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup_ets() ->
    %% Create ETS tables if they don't exist
    case ets:whereis(?USAGE_TABLE) of
        undefined ->
            ets:new(?USAGE_TABLE, [named_table, public, set]);
        _ ->
            ets:delete_all_objects(?USAGE_TABLE)
    end,
    case ets:whereis(?SHARES_TABLE) of
        undefined ->
            ets:new(?SHARES_TABLE, [named_table, public, set]);
        _ ->
            ets:delete_all_objects(?SHARES_TABLE)
    end,
    ok.

cleanup_ets() ->
    catch ets:delete(?USAGE_TABLE),
    catch ets:delete(?SHARES_TABLE),
    ok.

%% Create a mock state record for testing
mock_state(TotalShares, TotalUsage) ->
    {state, undefined, TotalShares, TotalUsage}.

%%====================================================================
%% calculate_priority_factor/3 Tests
%%====================================================================

calculate_priority_factor_test_() ->
    {"calculate_priority_factor/3 calculates fairshare priority",
     {setup,
      fun setup_ets/0,
      fun(_) -> cleanup_ets() end,
      [
       {"returns 1.0 for user with no usage",
        fun() ->
            State = mock_state(100, 1000.0),
            %% User has no usage recorded
            Factor = flurm_fairshare:calculate_priority_factor(
                <<"newuser">>, <<"account1">>, State),
            %% With no usage, should get high factor (close to 1.0)
            ?assert(Factor >= 0.9),
            ?assert(Factor =< 1.0)
        end},

       {"returns lower factor for user with high usage",
        fun() ->
            State = mock_state(100, 10000.0),
            %% Set up high usage for this user
            Key = {<<"heavyuser">>, <<"account1">>},
            ets:insert(?USAGE_TABLE, {Key, 5000.0, erlang:system_time(second)}),
            %% Set shares
            ets:insert(?SHARES_TABLE, {Key, 10}),

            Factor = flurm_fairshare:calculate_priority_factor(
                <<"heavyuser">>, <<"account1">>, State),
            %% High usage should result in lower factor
            ?assert(Factor < 1.0),
            ?assert(Factor >= 0.0)
        end},

       {"returns 0.5 for user at exactly their fair share",
        fun() ->
            %% Set up state where user has used exactly their share
            TotalShares = 100,
            TotalUsage = 10000.0,
            State = mock_state(TotalShares, TotalUsage),

            Key = {<<"fairuser">>, <<"account1">>},
            UserShares = 10,  % 10% of total shares
            UserUsage = 1000.0,  % 10% of total usage

            ets:insert(?SHARES_TABLE, {Key, UserShares}),
            ets:insert(?USAGE_TABLE, {Key, UserUsage, erlang:system_time(second)}),

            Factor = flurm_fairshare:calculate_priority_factor(
                <<"fairuser">>, <<"account1">>, State),
            %% At fair share, factor should be around 0.5
            ?assert(Factor >= 0.4),
            ?assert(Factor =< 0.6)
        end},

       {"returns 0.0 for user with zero shares",
        fun() ->
            State = mock_state(100, 1000.0),
            Key = {<<"noshares">>, <<"account1">>},
            ets:insert(?SHARES_TABLE, {Key, 0}),

            Factor = flurm_fairshare:calculate_priority_factor(
                <<"noshares">>, <<"account1">>, State),
            %% Zero shares = lowest priority
            ?assertEqual(0.0, Factor)
        end},

       {"uses default shares for unknown user",
        fun() ->
            State = mock_state(100, 1000.0),
            %% User not in shares table - should use default
            Factor = flurm_fairshare:calculate_priority_factor(
                <<"unknownuser">>, <<"unknownaccount">>, State),
            %% Should get a valid factor
            ?assert(Factor >= 0.0),
            ?assert(Factor =< 1.0)
        end},

       {"handles edge case of zero total shares",
        fun() ->
            State = mock_state(0, 1000.0),
            %% With zero total shares, calculation should handle gracefully
            Factor = flurm_fairshare:calculate_priority_factor(
                <<"testuser">>, <<"account1">>, State),
            ?assert(Factor >= 0.0),
            ?assert(Factor =< 1.0)
        end},

       {"handles edge case of zero total usage",
        fun() ->
            State = mock_state(100, 0.0),
            Key = {<<"testuser">>, <<"account1">>},
            ets:insert(?SHARES_TABLE, {Key, 10}),

            Factor = flurm_fairshare:calculate_priority_factor(
                <<"testuser">>, <<"account1">>, State),
            %% Zero total usage means everyone has used nothing
            ?assert(Factor >= 0.0),
            ?assert(Factor =< 1.0)
        end}
      ]}}.

%%====================================================================
%% Priority factor formula verification
%%====================================================================

priority_factor_formula_test_() ->
    {"Priority factor follows exponential decay formula",
     {setup,
      fun setup_ets/0,
      fun(_) -> cleanup_ets() end,
      [
       {"factor decreases as usage ratio increases",
        fun() ->
            TotalShares = 100,
            TotalUsage = 10000.0,
            UserShares = 10,

            Key = {<<"testuser">>, <<"testaccount">>},
            ets:insert(?SHARES_TABLE, {Key, UserShares}),

            %% Test with increasing usage levels
            Usages = [100.0, 500.0, 1000.0, 2000.0, 5000.0],
            Factors = lists:map(
                fun(Usage) ->
                    ets:insert(?USAGE_TABLE, {Key, Usage, erlang:system_time(second)}),
                    State = mock_state(TotalShares, TotalUsage),
                    flurm_fairshare:calculate_priority_factor(
                        <<"testuser">>, <<"testaccount">>, State)
                end,
                Usages),

            %% Each factor should be less than or equal to the previous
            pairs_check_descending(Factors)
        end},

       {"factor is bounded between 0 and 1",
        fun() ->
            State = mock_state(100, 10000.0),
            Key = {<<"boundeduser">>, <<"account1">>},
            ets:insert(?SHARES_TABLE, {Key, 50}),

            %% Test with extreme usage values
            ExtremeUsages = [0.0, 1.0, 100.0, 10000.0, 100000.0],
            lists:foreach(
                fun(Usage) ->
                    ets:insert(?USAGE_TABLE, {Key, Usage, erlang:system_time(second)}),
                    Factor = flurm_fairshare:calculate_priority_factor(
                        <<"boundeduser">>, <<"account1">>, State),
                    ?assert(Factor >= 0.0),
                    ?assert(Factor =< 1.0)
                end,
                ExtremeUsages)
        end}
      ]}}.

%%====================================================================
%% Helper Functions
%%====================================================================

pairs_check_descending([]) -> ok;
pairs_check_descending([_]) -> ok;
pairs_check_descending([A, B | Rest]) ->
    ?assert(A >= B),
    pairs_check_descending([B | Rest]).
