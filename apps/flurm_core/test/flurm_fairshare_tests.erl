%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_fairshare module
%%%-------------------------------------------------------------------
-module(flurm_fairshare_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start the fairshare server
    case whereis(flurm_fairshare) of
        undefined ->
            {ok, Pid} = flurm_fairshare:start_link(),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, _Pid}) ->
    %% Clean up ETS tables
    catch ets:delete(flurm_fairshare_usage),
    catch ets:delete(flurm_fairshare_shares),
    gen_server:stop(flurm_fairshare);
cleanup({existing, _Pid}) ->
    ok.

fairshare_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"initial priority factor", fun test_initial_priority/0},
      {"set and get shares", fun test_set_get_shares/0},
      {"record usage", fun test_record_usage/0},
      {"priority factor after usage", fun test_priority_after_usage/0},
      {"reset usage", fun test_reset_usage/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_initial_priority() ->
    %% New user with no usage should have high priority factor
    Factor = flurm_fairshare:get_priority_factor(<<"newuser">>, <<"account">>),
    %% With no total usage, should be close to 1.0
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

test_set_get_shares() ->
    User = <<"testuser">>,
    Account = <<"testaccount">>,

    %% Default shares should be 1
    {ok, DefaultShares} = flurm_fairshare:get_shares(User, Account),
    ?assertEqual(1, DefaultShares),

    %% Set custom shares
    ok = flurm_fairshare:set_shares(User, Account, 100),

    %% Retrieve should match
    {ok, NewShares} = flurm_fairshare:get_shares(User, Account),
    ?assertEqual(100, NewShares).

test_record_usage() ->
    User = <<"usageuser">>,
    Account = <<"usageaccount">>,

    %% Initial usage should be 0
    {ok, InitUsage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(0.0, InitUsage),

    %% Record some CPU usage
    flurm_fairshare:record_usage(User, Account, 1000, 100),

    %% Usage should increase
    {ok, NewUsage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(1000.0, NewUsage).

test_priority_after_usage() ->
    User1 = <<"heavyuser">>,
    User2 = <<"lightuser">>,
    Account = <<"sharedaccount">>,

    %% Set equal shares
    flurm_fairshare:set_shares(User1, Account, 100),
    flurm_fairshare:set_shares(User2, Account, 100),

    %% Record heavy usage for User1
    flurm_fairshare:record_usage(User1, Account, 10000, 1000),

    %% Record light usage for User2
    flurm_fairshare:record_usage(User2, Account, 100, 10),

    %% Heavy user should have lower priority
    Factor1 = flurm_fairshare:get_priority_factor(User1, Account),
    Factor2 = flurm_fairshare:get_priority_factor(User2, Account),

    ?assert(Factor2 > Factor1).

test_reset_usage() ->
    User = <<"resetuser">>,
    Account = <<"resetaccount">>,

    %% Record some usage
    flurm_fairshare:record_usage(User, Account, 5000, 500),

    %% Verify usage exists
    {ok, Usage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(5000.0, Usage),

    %% Reset
    flurm_fairshare:reset_usage(User, Account),

    %% Should be back to 0
    {ok, ResetUsage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(0.0, ResetUsage).
