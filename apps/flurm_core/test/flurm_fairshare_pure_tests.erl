%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_fairshare module
%%%
%%% Tests the fair-share scheduling system without mocking:
%%% - Direct gen_server callback testing (init, handle_call, handle_cast, handle_info)
%%% - Share allocation and retrieval
%%% - Usage tracking and recording
%%% - Priority factor calculation
%%% - Usage decay mechanism
%%% - Terminate and code_change callbacks
%%%
%%% NO MECK - all tests call functions directly
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_fairshare_pure_tests).
-include_lib("eunit/include/eunit.hrl").

%% ETS table names (same as in the module)
-define(USAGE_TABLE, flurm_fairshare_usage).
-define(SHARES_TABLE, flurm_fairshare_shares).

%% State record (same as in the module)
-record(state, {
    decay_timer :: reference() | undefined,
    total_shares :: non_neg_integer(),
    total_usage :: float()
}).

%%====================================================================
%% Test Setup/Teardown for Direct Callback Tests
%%====================================================================

setup_ets() ->
    %% Clean up any existing tables
    catch ets:delete(?USAGE_TABLE),
    catch ets:delete(?SHARES_TABLE),
    %% Create ETS tables (same as init does)
    ets:new(?USAGE_TABLE, [named_table, public, set]),
    ets:new(?SHARES_TABLE, [named_table, public, set]),
    ok.

cleanup_ets(_) ->
    catch ets:delete(?USAGE_TABLE),
    catch ets:delete(?SHARES_TABLE),
    ok.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {"init/1 tests",
     {foreach,
      fun() ->
          catch ets:delete(?USAGE_TABLE),
          catch ets:delete(?SHARES_TABLE),
          ok
      end,
      fun(_) ->
          catch ets:delete(?USAGE_TABLE),
          catch ets:delete(?SHARES_TABLE),
          ok
      end,
      [
       {"init creates ETS tables and returns ok", fun test_init_creates_tables/0},
       {"init returns correct initial state", fun test_init_state/0},
       {"init starts decay timer", fun test_init_decay_timer/0}
      ]}}.

test_init_creates_tables() ->
    {ok, State} = flurm_fairshare:init([]),
    %% Tables should exist
    ?assertEqual(?USAGE_TABLE, ets:info(?USAGE_TABLE, name)),
    ?assertEqual(?SHARES_TABLE, ets:info(?SHARES_TABLE, name)),
    %% Clean up timer
    erlang:cancel_timer(State#state.decay_timer).

test_init_state() ->
    {ok, State} = flurm_fairshare:init([]),
    ?assertEqual(0, State#state.total_shares),
    ?assertEqual(0.0, State#state.total_usage),
    ?assertNotEqual(undefined, State#state.decay_timer),
    %% Clean up timer
    erlang:cancel_timer(State#state.decay_timer).

test_init_decay_timer() ->
    {ok, State} = flurm_fairshare:init([]),
    Timer = State#state.decay_timer,
    ?assert(is_reference(Timer)),
    %% Timer should be active
    ?assertNotEqual(false, erlang:read_timer(Timer)),
    %% Clean up timer
    erlang:cancel_timer(Timer).

%%====================================================================
%% handle_call/3 Tests - get_shares
%%====================================================================

handle_call_get_shares_test_() ->
    {"handle_call get_shares tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"get_shares returns default for unknown user", fun test_get_shares_default/0},
       {"get_shares returns configured shares", fun test_get_shares_configured/0}
      ]}}.

test_get_shares_default() ->
    State = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},
    User = <<"unknown_user">>,
    Account = <<"unknown_account">>,

    {reply, Result, NewState} = flurm_fairshare:handle_call({get_shares, User, Account}, {self(), make_ref()}, State),

    ?assertEqual({ok, 1}, Result),  % Default shares = 1
    ?assertEqual(State, NewState).

test_get_shares_configured() ->
    State = #state{total_shares = 100, total_usage = 0.0, decay_timer = undefined},
    User = <<"configured_user">>,
    Account = <<"configured_account">>,
    Key = {User, Account},

    %% Insert shares into ETS
    ets:insert(?SHARES_TABLE, {Key, 500}),

    {reply, Result, NewState} = flurm_fairshare:handle_call({get_shares, User, Account}, {self(), make_ref()}, State),

    ?assertEqual({ok, 500}, Result),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - set_shares
%%====================================================================

handle_call_set_shares_test_() ->
    {"handle_call set_shares tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"set_shares creates new entry", fun test_set_shares_new/0},
       {"set_shares updates existing entry", fun test_set_shares_update/0},
       {"set_shares updates total_shares", fun test_set_shares_total/0}
      ]}}.

test_set_shares_new() ->
    State = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},
    User = <<"new_user">>,
    Account = <<"new_account">>,

    {reply, Result, NewState} = flurm_fairshare:handle_call({set_shares, User, Account, 100}, {self(), make_ref()}, State),

    ?assertEqual(ok, Result),
    ?assertEqual(100, NewState#state.total_shares),

    %% Verify ETS entry
    [{_Key, Shares}] = ets:lookup(?SHARES_TABLE, {User, Account}),
    ?assertEqual(100, Shares).

test_set_shares_update() ->
    State = #state{total_shares = 50, total_usage = 0.0, decay_timer = undefined},
    User = <<"existing_user">>,
    Account = <<"existing_account">>,
    Key = {User, Account},

    %% Insert initial shares
    ets:insert(?SHARES_TABLE, {Key, 50}),

    {reply, Result, NewState} = flurm_fairshare:handle_call({set_shares, User, Account, 200}, {self(), make_ref()}, State),

    ?assertEqual(ok, Result),
    %% total_shares = 50 - 50 + 200 = 200
    ?assertEqual(200, NewState#state.total_shares),

    %% Verify ETS entry updated
    [{_Key, Shares}] = ets:lookup(?SHARES_TABLE, Key),
    ?assertEqual(200, Shares).

test_set_shares_total() ->
    State = #state{total_shares = 100, total_usage = 0.0, decay_timer = undefined},

    %% Add multiple users
    {reply, ok, State1} = flurm_fairshare:handle_call({set_shares, <<"user1">>, <<"acct">>, 50}, {self(), make_ref()}, State),
    ?assertEqual(150, State1#state.total_shares),

    {reply, ok, State2} = flurm_fairshare:handle_call({set_shares, <<"user2">>, <<"acct">>, 75}, {self(), make_ref()}, State1),
    ?assertEqual(225, State2#state.total_shares).

%%====================================================================
%% handle_call/3 Tests - get_usage
%%====================================================================

handle_call_get_usage_test_() ->
    {"handle_call get_usage tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"get_usage returns 0.0 for unknown user", fun test_get_usage_unknown/0},
       {"get_usage returns recorded usage", fun test_get_usage_recorded/0}
      ]}}.

test_get_usage_unknown() ->
    State = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},
    User = <<"unknown_user">>,
    Account = <<"unknown_account">>,

    {reply, Result, NewState} = flurm_fairshare:handle_call({get_usage, User, Account}, {self(), make_ref()}, State),

    ?assertEqual({ok, 0.0}, Result),
    ?assertEqual(State, NewState).

test_get_usage_recorded() ->
    State = #state{total_shares = 0, total_usage = 1000.0, decay_timer = undefined},
    User = <<"user_with_usage">>,
    Account = <<"account">>,
    Key = {User, Account},
    Now = erlang:system_time(second),

    %% Insert usage into ETS
    ets:insert(?USAGE_TABLE, {Key, 1000.0, Now}),

    {reply, Result, NewState} = flurm_fairshare:handle_call({get_usage, User, Account}, {self(), make_ref()}, State),

    ?assertEqual({ok, 1000.0}, Result),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - get_priority_factor
%%====================================================================

handle_call_priority_factor_test_() ->
    {"handle_call get_priority_factor tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"priority factor for new user", fun test_priority_factor_new_user/0},
       {"priority factor with usage and shares", fun test_priority_factor_with_data/0},
       {"priority factor is between 0 and 1", fun test_priority_factor_range/0},
       {"heavy user gets lower priority", fun test_priority_factor_heavy_user/0}
      ]}}.

test_priority_factor_new_user() ->
    State = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},
    User = <<"new_user">>,
    Account = <<"new_account">>,

    {reply, Factor, NewState} = flurm_fairshare:handle_call({get_priority_factor, User, Account}, {self(), make_ref()}, State),

    ?assert(is_float(Factor)),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0),
    ?assertEqual(State, NewState).

test_priority_factor_with_data() ->
    State = #state{total_shares = 100, total_usage = 500.0, decay_timer = undefined},
    User = <<"test_user">>,
    Account = <<"test_account">>,
    Key = {User, Account},
    Now = erlang:system_time(second),

    %% Set shares and usage
    ets:insert(?SHARES_TABLE, {Key, 100}),
    ets:insert(?USAGE_TABLE, {Key, 250.0, Now}),  % Used half their share proportion

    {reply, Factor, _NewState} = flurm_fairshare:handle_call({get_priority_factor, User, Account}, {self(), make_ref()}, State),

    ?assert(is_float(Factor)),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

test_priority_factor_range() ->
    State = #state{total_shares = 100, total_usage = 10000.0, decay_timer = undefined},
    User = <<"range_user">>,
    Account = <<"range_account">>,
    Key = {User, Account},
    Now = erlang:system_time(second),

    %% Test with very high usage
    ets:insert(?SHARES_TABLE, {Key, 10}),
    ets:insert(?USAGE_TABLE, {Key, 9000.0, Now}),  % Used way more than share

    {reply, Factor, _NewState} = flurm_fairshare:handle_call({get_priority_factor, User, Account}, {self(), make_ref()}, State),

    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

test_priority_factor_heavy_user() ->
    State = #state{total_shares = 200, total_usage = 1000.0, decay_timer = undefined},
    Now = erlang:system_time(second),

    %% Light user - 10% of total usage, 50% of shares
    ets:insert(?SHARES_TABLE, {{<<"light">>, <<"acct">>}, 100}),
    ets:insert(?USAGE_TABLE, {{<<"light">>, <<"acct">>}, 100.0, Now}),

    %% Heavy user - 90% of total usage, 50% of shares
    ets:insert(?SHARES_TABLE, {{<<"heavy">>, <<"acct">>}, 100}),
    ets:insert(?USAGE_TABLE, {{<<"heavy">>, <<"acct">>}, 900.0, Now}),

    {reply, LightFactor, _} = flurm_fairshare:handle_call({get_priority_factor, <<"light">>, <<"acct">>}, {self(), make_ref()}, State),
    {reply, HeavyFactor, _} = flurm_fairshare:handle_call({get_priority_factor, <<"heavy">>, <<"acct">>}, {self(), make_ref()}, State),

    %% Light user should have higher priority factor
    ?assert(LightFactor > HeavyFactor).

%%====================================================================
%% handle_call/3 Tests - get_all_accounts
%%====================================================================

handle_call_get_all_accounts_test_() ->
    {"handle_call get_all_accounts tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"get_all_accounts returns empty list", fun test_get_all_accounts_empty/0},
       {"get_all_accounts returns all entries", fun test_get_all_accounts_data/0},
       {"get_all_accounts includes default shares", fun test_get_all_accounts_default_shares/0}
      ]}}.

test_get_all_accounts_empty() ->
    State = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},

    {reply, Result, NewState} = flurm_fairshare:handle_call(get_all_accounts, {self(), make_ref()}, State),

    ?assertEqual([], Result),
    ?assertEqual(State, NewState).

test_get_all_accounts_data() ->
    State = #state{total_shares = 300, total_usage = 3000.0, decay_timer = undefined},
    Now = erlang:system_time(second),

    %% Add multiple users with usage and shares
    ets:insert(?SHARES_TABLE, {{<<"user1">>, <<"acct1">>}, 100}),
    ets:insert(?USAGE_TABLE, {{<<"user1">>, <<"acct1">>}, 1000.0, Now}),

    ets:insert(?SHARES_TABLE, {{<<"user2">>, <<"acct2">>}, 200}),
    ets:insert(?USAGE_TABLE, {{<<"user2">>, <<"acct2">>}, 2000.0, Now}),

    {reply, Result, _NewState} = flurm_fairshare:handle_call(get_all_accounts, {self(), make_ref()}, State),

    ?assertEqual(2, length(Result)),

    %% Check that both entries are present
    ?assert(lists:member({<<"user1">>, <<"acct1">>, 1000.0, 100}, Result)),
    ?assert(lists:member({<<"user2">>, <<"acct2">>, 2000.0, 200}, Result)).

test_get_all_accounts_default_shares() ->
    State = #state{total_shares = 0, total_usage = 500.0, decay_timer = undefined},
    Now = erlang:system_time(second),

    %% User with usage but no shares configured
    ets:insert(?USAGE_TABLE, {{<<"noSharesUser">>, <<"acct">>}, 500.0, Now}),

    {reply, Result, _NewState} = flurm_fairshare:handle_call(get_all_accounts, {self(), make_ref()}, State),

    ?assertEqual(1, length(Result)),
    [{User, Account, Usage, Shares}] = Result,
    ?assertEqual(<<"noSharesUser">>, User),
    ?assertEqual(<<"acct">>, Account),
    ?assertEqual(500.0, Usage),
    ?assertEqual(1, Shares).  % Default shares

%%====================================================================
%% handle_call/3 Tests - Unknown Request
%%====================================================================

handle_call_unknown_test_() ->
    {"handle_call unknown request tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"unknown call returns error", fun test_unknown_call/0}
      ]}}.

test_unknown_call() ->
    State = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},

    {reply, Result, NewState} = flurm_fairshare:handle_call({some_unknown_request, arg1, arg2}, {self(), make_ref()}, State),

    ?assertEqual({error, unknown_request}, Result),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_cast/2 Tests - record_usage
%%====================================================================

handle_cast_record_usage_test_() ->
    {"handle_cast record_usage tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"record_usage creates new entry", fun test_record_usage_new/0},
       {"record_usage accumulates usage", fun test_record_usage_accumulate/0},
       {"record_usage updates total_usage", fun test_record_usage_total/0}
      ]}}.

test_record_usage_new() ->
    State = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},
    User = <<"new_user">>,
    Account = <<"new_account">>,

    {noreply, NewState} = flurm_fairshare:handle_cast({record_usage, User, Account, 1000, 100}, State),

    ?assertEqual(1000.0, NewState#state.total_usage),

    %% Verify ETS entry
    [{_Key, Usage, _Timestamp}] = ets:lookup(?USAGE_TABLE, {User, Account}),
    ?assertEqual(1000.0, Usage).

test_record_usage_accumulate() ->
    State = #state{total_shares = 0, total_usage = 500.0, decay_timer = undefined},
    User = <<"existing_user">>,
    Account = <<"existing_account">>,
    Key = {User, Account},
    Now = erlang:system_time(second),

    %% Insert initial usage
    ets:insert(?USAGE_TABLE, {Key, 500.0, Now}),

    {noreply, NewState} = flurm_fairshare:handle_cast({record_usage, User, Account, 300, 30}, State),

    ?assertEqual(800.0, NewState#state.total_usage),

    %% Verify accumulated usage
    [{_Key, Usage, _Timestamp}] = ets:lookup(?USAGE_TABLE, Key),
    ?assertEqual(800.0, Usage).

test_record_usage_total() ->
    State = #state{total_shares = 0, total_usage = 1000.0, decay_timer = undefined},

    {noreply, State1} = flurm_fairshare:handle_cast({record_usage, <<"u1">>, <<"a1">>, 100, 10}, State),
    ?assertEqual(1100.0, State1#state.total_usage),

    {noreply, State2} = flurm_fairshare:handle_cast({record_usage, <<"u2">>, <<"a2">>, 200, 20}, State1),
    ?assertEqual(1300.0, State2#state.total_usage).

%%====================================================================
%% handle_cast/2 Tests - decay_usage
%%====================================================================

handle_cast_decay_usage_test_() ->
    {"handle_cast decay_usage tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"decay_usage reduces usage values", fun test_decay_usage/0},
       {"decay_usage removes negligible entries", fun test_decay_usage_removes_small/0},
       {"decay_usage with empty table", fun test_decay_usage_empty/0}
      ]}}.

test_decay_usage() ->
    State = #state{total_shares = 100, total_usage = 10000.0, decay_timer = undefined},
    Now = erlang:system_time(second),
    User = <<"decay_user">>,
    Account = <<"decay_account">>,
    Key = {User, Account},

    %% Insert usage with recent timestamp
    ets:insert(?USAGE_TABLE, {Key, 10000.0, Now}),

    {noreply, NewState} = flurm_fairshare:handle_cast(decay_usage, State),

    %% Usage should decrease after decay
    [{_Key, NewUsage, _Timestamp}] = ets:lookup(?USAGE_TABLE, Key),
    ?assert(NewUsage < 10000.0),
    ?assert(NewState#state.total_usage < State#state.total_usage).

test_decay_usage_removes_small() ->
    State = #state{total_shares = 0, total_usage = 0.001, decay_timer = undefined},
    %% Use old timestamp to trigger more decay
    OldTime = erlang:system_time(second) - 86400 * 30,  % 30 days ago
    User = <<"small_usage_user">>,
    Account = <<"small_usage_account">>,
    Key = {User, Account},

    %% Insert very small usage with old timestamp
    ets:insert(?USAGE_TABLE, {Key, 0.001, OldTime}),

    {noreply, NewState} = flurm_fairshare:handle_cast(decay_usage, State),

    %% Entry should be removed since it's < 0.01
    ?assertEqual([], ets:lookup(?USAGE_TABLE, Key)),
    ?assertEqual(0.0, NewState#state.total_usage).

test_decay_usage_empty() ->
    State = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},

    {noreply, NewState} = flurm_fairshare:handle_cast(decay_usage, State),

    ?assertEqual(0.0, NewState#state.total_usage).

%%====================================================================
%% handle_cast/2 Tests - reset_usage
%%====================================================================

handle_cast_reset_usage_test_() ->
    {"handle_cast reset_usage tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"reset_usage removes entry", fun test_reset_usage_removes/0},
       {"reset_usage updates total", fun test_reset_usage_total/0},
       {"reset_usage handles nonexistent", fun test_reset_usage_nonexistent/0}
      ]}}.

test_reset_usage_removes() ->
    State = #state{total_shares = 0, total_usage = 5000.0, decay_timer = undefined},
    Now = erlang:system_time(second),
    User = <<"reset_user">>,
    Account = <<"reset_account">>,
    Key = {User, Account},

    ets:insert(?USAGE_TABLE, {Key, 5000.0, Now}),

    {noreply, NewState} = flurm_fairshare:handle_cast({reset_usage, User, Account}, State),

    ?assertEqual([], ets:lookup(?USAGE_TABLE, Key)),
    ?assertEqual(0.0, NewState#state.total_usage).

test_reset_usage_total() ->
    State = #state{total_shares = 0, total_usage = 8000.0, decay_timer = undefined},
    Now = erlang:system_time(second),

    ets:insert(?USAGE_TABLE, {{<<"u1">>, <<"a1">>}, 3000.0, Now}),
    ets:insert(?USAGE_TABLE, {{<<"u2">>, <<"a2">>}, 5000.0, Now}),

    {noreply, NewState} = flurm_fairshare:handle_cast({reset_usage, <<"u1">>, <<"a1">>}, State),

    %% Total should decrease by 3000
    ?assertEqual(5000.0, NewState#state.total_usage).

test_reset_usage_nonexistent() ->
    State = #state{total_shares = 0, total_usage = 1000.0, decay_timer = undefined},

    {noreply, NewState} = flurm_fairshare:handle_cast({reset_usage, <<"nonexistent">>, <<"account">>}, State),

    %% State should be unchanged
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_cast/2 Tests - Unknown Message
%%====================================================================

handle_cast_unknown_test_() ->
    {"handle_cast unknown message tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"unknown cast returns noreply", fun test_unknown_cast/0}
      ]}}.

test_unknown_cast() ->
    State = #state{total_shares = 100, total_usage = 500.0, decay_timer = undefined},

    {noreply, NewState} = flurm_fairshare:handle_cast({unknown_message, arg1}, State),

    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_test_() ->
    {"handle_info tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"decay timer triggers decay", fun test_handle_info_decay/0},
       {"unknown info message", fun test_handle_info_unknown/0}
      ]}}.

test_handle_info_decay() ->
    State = #state{total_shares = 100, total_usage = 5000.0, decay_timer = undefined},
    Now = erlang:system_time(second),

    ets:insert(?USAGE_TABLE, {{<<"user">>, <<"account">>}, 5000.0, Now}),

    {noreply, NewState} = flurm_fairshare:handle_info(decay, State),

    %% Should have scheduled a new timer
    ?assertNotEqual(undefined, NewState#state.decay_timer),
    ?assert(is_reference(NewState#state.decay_timer)),

    %% Clean up the timer
    erlang:cancel_timer(NewState#state.decay_timer).

test_handle_info_unknown() ->
    State = #state{total_shares = 50, total_usage = 250.0, decay_timer = undefined},

    {noreply, NewState} = flurm_fairshare:handle_info(some_random_message, State),

    ?assertEqual(State, NewState).

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_test_() ->
    {"terminate tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"terminate cancels timer", fun test_terminate_cancels_timer/0},
       {"terminate with undefined timer", fun test_terminate_no_timer/0}
      ]}}.

test_terminate_cancels_timer() ->
    Timer = erlang:send_after(60000, self(), test_message),
    State = #state{total_shares = 0, total_usage = 0.0, decay_timer = Timer},

    Result = flurm_fairshare:terminate(shutdown, State),

    ?assertEqual(ok, Result),
    %% Timer should be cancelled
    ?assertEqual(false, erlang:read_timer(Timer)).

test_terminate_no_timer() ->
    State = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},

    Result = flurm_fairshare:terminate(normal, State),

    ?assertEqual(ok, Result).

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_test_() ->
    {"code_change tests",
     [
      {"code_change returns state unchanged", fun test_code_change/0}
     ]}.

test_code_change() ->
    State = #state{total_shares = 100, total_usage = 500.0, decay_timer = undefined},

    {ok, NewState} = flurm_fairshare:code_change("1.0.0", State, []),

    ?assertEqual(State, NewState).

%%====================================================================
%% Priority Factor Calculation Tests
%%====================================================================

priority_calculation_test_() ->
    {"priority calculation edge cases",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"zero total usage", fun test_priority_zero_total_usage/0},
       {"zero total shares", fun test_priority_zero_total_shares/0},
       {"user with no shares in table uses default", fun test_priority_default_shares/0},
       {"user with no usage has high priority", fun test_priority_no_usage/0}
      ]}}.

test_priority_zero_total_usage() ->
    %% When total_usage is 0, it gets maxed to 1.0
    State = #state{total_shares = 100, total_usage = 0.0, decay_timer = undefined},

    {reply, Factor, _NewState} = flurm_fairshare:handle_call({get_priority_factor, <<"user">>, <<"acct">>}, {self(), make_ref()}, State),

    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

test_priority_zero_total_shares() ->
    %% When total_shares is 0, it gets maxed to 1
    State = #state{total_shares = 0, total_usage = 1000.0, decay_timer = undefined},
    Now = erlang:system_time(second),

    ets:insert(?USAGE_TABLE, {{<<"user">>, <<"acct">>}, 500.0, Now}),

    {reply, Factor, _NewState} = flurm_fairshare:handle_call({get_priority_factor, <<"user">>, <<"acct">>}, {self(), make_ref()}, State),

    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

test_priority_default_shares() ->
    %% User not in shares table uses default shares of 1
    State = #state{total_shares = 100, total_usage = 1000.0, decay_timer = undefined},
    Now = erlang:system_time(second),

    %% User has usage but no shares configured
    ets:insert(?USAGE_TABLE, {{<<"noSharesUser">>, <<"acct">>}, 500.0, Now}),

    {reply, Factor, _NewState} = flurm_fairshare:handle_call({get_priority_factor, <<"noSharesUser">>, <<"acct">>}, {self(), make_ref()}, State),

    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

test_priority_no_usage() ->
    %% User with shares but no usage should have high priority (factor = 1.0)
    State = #state{total_shares = 100, total_usage = 1000.0, decay_timer = undefined},
    Now = erlang:system_time(second),

    %% Another user has usage
    ets:insert(?SHARES_TABLE, {{<<"other">>, <<"acct">>}, 50}),
    ets:insert(?USAGE_TABLE, {{<<"other">>, <<"acct">>}, 1000.0, Now}),

    %% Our user has shares but no usage
    ets:insert(?SHARES_TABLE, {{<<"noUsageUser">>, <<"acct">>}, 50}),

    {reply, Factor, _NewState} = flurm_fairshare:handle_call({get_priority_factor, <<"noUsageUser">>, <<"acct">>}, {self(), make_ref()}, State),

    %% User with no usage should have maximum factor (1.0)
    ?assertEqual(1.0, Factor).

%%====================================================================
%% Zero Share Edge Case Test
%%====================================================================

zero_share_edge_case_test_() ->
    {"zero share edge case test",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"zero shares gives lowest priority", fun test_zero_shares_lowest_priority/0}
      ]}}.

test_zero_shares_lowest_priority() ->
    %% This tests the edge case where ShareFraction == 0.0
    %% We need to set up a state where Shares = 0 via direct ETS insertion
    %% (This bypasses normal API which enforces minimum shares of 1)
    State = #state{total_shares = 100, total_usage = 1000.0, decay_timer = undefined},
    Now = erlang:system_time(second),

    User = <<"zero_share_user">>,
    Account = <<"zero_share_acct">>,
    Key = {User, Account},

    %% Directly insert zero shares (bypassing API validation)
    ets:insert(?SHARES_TABLE, {Key, 0}),
    ets:insert(?USAGE_TABLE, {Key, 100.0, Now}),

    {reply, Factor, _NewState} = flurm_fairshare:handle_call({get_priority_factor, User, Account}, {self(), make_ref()}, State),

    %% User with zero shares should get lowest priority (0.0)
    ?assertEqual(0.0, Factor).

%%====================================================================
%% Integration-style Tests (still pure, no meck)
%%====================================================================

integration_test_() ->
    {"integration-style tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"full workflow test", fun test_full_workflow/0},
       {"multiple users fairshare", fun test_multiple_users_fairshare/0}
      ]}}.

test_full_workflow() ->
    State0 = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},

    %% 1. Set shares for user
    {reply, ok, State1} = flurm_fairshare:handle_call({set_shares, <<"alice">>, <<"research">>, 100}, {self(), make_ref()}, State0),

    %% 2. Get initial priority (should be high - no usage)
    {reply, InitFactor, _} = flurm_fairshare:handle_call({get_priority_factor, <<"alice">>, <<"research">>}, {self(), make_ref()}, State1),
    ?assertEqual(1.0, InitFactor),

    %% 3. Record some usage
    {noreply, State2} = flurm_fairshare:handle_cast({record_usage, <<"alice">>, <<"research">>, 5000, 500}, State1),

    %% 4. Check usage recorded
    {reply, {ok, Usage}, _} = flurm_fairshare:handle_call({get_usage, <<"alice">>, <<"research">>}, {self(), make_ref()}, State2),
    ?assertEqual(5000.0, Usage),

    %% 5. Priority should now be lower
    {reply, NewFactor, _} = flurm_fairshare:handle_call({get_priority_factor, <<"alice">>, <<"research">>}, {self(), make_ref()}, State2),
    ?assert(NewFactor < InitFactor),

    %% 6. Get all accounts
    {reply, Accounts, _} = flurm_fairshare:handle_call(get_all_accounts, {self(), make_ref()}, State2),
    ?assertEqual(1, length(Accounts)),
    [{<<"alice">>, <<"research">>, 5000.0, 100}] = Accounts.

test_multiple_users_fairshare() ->
    State0 = #state{total_shares = 0, total_usage = 0.0, decay_timer = undefined},

    %% Set up two users with equal shares
    {reply, ok, State1} = flurm_fairshare:handle_call({set_shares, <<"bob">>, <<"compute">>, 100}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_fairshare:handle_call({set_shares, <<"carol">>, <<"compute">>, 100}, {self(), make_ref()}, State1),

    %% Bob uses a lot, Carol uses little
    {noreply, State3} = flurm_fairshare:handle_cast({record_usage, <<"bob">>, <<"compute">>, 9000, 900}, State2),
    {noreply, State4} = flurm_fairshare:handle_cast({record_usage, <<"carol">>, <<"compute">>, 1000, 100}, State3),

    %% Carol should have higher priority than Bob
    {reply, BobFactor, _} = flurm_fairshare:handle_call({get_priority_factor, <<"bob">>, <<"compute">>}, {self(), make_ref()}, State4),
    {reply, CarolFactor, _} = flurm_fairshare:handle_call({get_priority_factor, <<"carol">>, <<"compute">>}, {self(), make_ref()}, State4),

    ?assert(CarolFactor > BobFactor).

%%====================================================================
%% Edge Cases for max() function in reset_usage
%%====================================================================

reset_usage_edge_test_() ->
    {"reset_usage edge cases",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
       {"reset prevents negative total", fun test_reset_prevents_negative/0}
      ]}}.

test_reset_prevents_negative() ->
    %% If total_usage is less than the usage being reset (shouldn't happen normally),
    %% the max(0.0, ...) should prevent negative totals
    State = #state{total_shares = 0, total_usage = 100.0, decay_timer = undefined},
    Now = erlang:system_time(second),

    %% Insert usage that's higher than total (abnormal state)
    ets:insert(?USAGE_TABLE, {{<<"user">>, <<"acct">>}, 500.0, Now}),

    {noreply, NewState} = flurm_fairshare:handle_cast({reset_usage, <<"user">>, <<"acct">>}, State),

    %% Total should be clamped to 0.0, not -400.0
    ?assertEqual(0.0, NewState#state.total_usage).

%%====================================================================
%% API Function Tests (with running gen_server)
%%====================================================================

%% Setup/teardown for API tests
setup_server() ->
    %% Clean up any existing server
    case whereis(flurm_fairshare) of
        undefined -> ok;
        OldPid ->
            catch unlink(OldPid),
            catch exit(OldPid, kill),
            timer:sleep(100)
    end,
    %% Clean up any existing tables
    catch ets:delete(?USAGE_TABLE),
    catch ets:delete(?SHARES_TABLE),
    timer:sleep(50),
    %% Start the server
    {ok, NewPid} = flurm_fairshare:start_link(),
    NewPid.

cleanup_server(Pid) ->
    catch unlink(Pid),
    catch exit(Pid, kill),
    timer:sleep(100),
    catch ets:delete(?USAGE_TABLE),
    catch ets:delete(?SHARES_TABLE),
    ok.

api_test_() ->
    {"API function tests",
     {foreach,
      fun setup_server/0,
      fun cleanup_server/1,
      [
       {"start_link creates server", fun test_api_start_link/0},
       {"get_shares API", fun test_api_get_shares/0},
       {"set_shares API", fun test_api_set_shares/0},
       {"get_usage API", fun test_api_get_usage/0},
       {"record_usage API", fun test_api_record_usage/0},
       {"get_priority_factor API", fun test_api_get_priority_factor/0},
       {"decay_usage API", fun test_api_decay_usage/0},
       {"reset_usage API", fun test_api_reset_usage/0},
       {"get_all_accounts API", fun test_api_get_all_accounts/0}
      ]}}.

test_api_start_link() ->
    %% Server should already be running from setup
    ?assert(is_pid(whereis(flurm_fairshare))).

test_api_get_shares() ->
    %% Default shares for unknown user
    {ok, Shares} = flurm_fairshare:get_shares(<<"api_user">>, <<"api_account">>),
    ?assertEqual(1, Shares).

test_api_set_shares() ->
    User = <<"api_set_user">>,
    Account = <<"api_set_account">>,

    ok = flurm_fairshare:set_shares(User, Account, 250),
    {ok, Shares} = flurm_fairshare:get_shares(User, Account),
    ?assertEqual(250, Shares).

test_api_get_usage() ->
    {ok, Usage} = flurm_fairshare:get_usage(<<"api_usage_user">>, <<"api_usage_account">>),
    ?assertEqual(0.0, Usage).

test_api_record_usage() ->
    User = <<"api_record_user">>,
    Account = <<"api_record_account">>,

    ok = flurm_fairshare:record_usage(User, Account, 5000, 500),
    timer:sleep(50),  % Allow cast to complete

    {ok, Usage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(5000.0, Usage).

test_api_get_priority_factor() ->
    User = <<"api_priority_user">>,
    Account = <<"api_priority_account">>,

    Factor = flurm_fairshare:get_priority_factor(User, Account),
    ?assert(is_float(Factor)),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

test_api_decay_usage() ->
    User = <<"api_decay_user">>,
    Account = <<"api_decay_account">>,

    %% Record usage
    ok = flurm_fairshare:record_usage(User, Account, 10000, 1000),
    timer:sleep(50),

    %% Trigger decay
    ok = flurm_fairshare:decay_usage(),
    timer:sleep(50),

    %% Server should still be responsive
    {ok, _} = flurm_fairshare:get_shares(User, Account).

test_api_reset_usage() ->
    User = <<"api_reset_user">>,
    Account = <<"api_reset_account">>,

    %% Record usage
    ok = flurm_fairshare:record_usage(User, Account, 3000, 300),
    timer:sleep(50),

    {ok, Usage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(3000.0, Usage),

    %% Reset
    ok = flurm_fairshare:reset_usage(User, Account),
    timer:sleep(50),

    {ok, ResetUsage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(0.0, ResetUsage).

test_api_get_all_accounts() ->
    User1 = <<"api_all_user1">>,
    User2 = <<"api_all_user2">>,
    Account = <<"api_all_account">>,

    %% Set up data
    ok = flurm_fairshare:set_shares(User1, Account, 100),
    ok = flurm_fairshare:set_shares(User2, Account, 200),
    ok = flurm_fairshare:record_usage(User1, Account, 1000, 100),
    ok = flurm_fairshare:record_usage(User2, Account, 2000, 200),
    timer:sleep(50),

    %% Get all accounts
    Accounts = flurm_fairshare:get_all_accounts(),
    ?assert(is_list(Accounts)),
    ?assertEqual(2, length(Accounts)).

%%====================================================================
%% Additional Callback Coverage Tests
%%====================================================================

additional_callback_test_() ->
    {"additional callback coverage tests",
     {foreach,
      fun setup_server/0,
      fun cleanup_server/1,
      [
       {"unknown call via gen_server:call", fun test_unknown_call_via_api/0},
       {"unknown cast via gen_server:cast", fun test_unknown_cast_via_api/0},
       {"unknown info via message", fun test_unknown_info_via_api/0}
      ]}}.

test_unknown_call_via_api() ->
    Result = gen_server:call(flurm_fairshare, {completely_unknown_request}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast_via_api() ->
    gen_server:cast(flurm_fairshare, {completely_unknown_cast}),
    timer:sleep(50),
    %% Server should still be responsive
    {ok, _} = flurm_fairshare:get_shares(<<"user">>, <<"account">>).

test_unknown_info_via_api() ->
    flurm_fairshare ! completely_unknown_info_message,
    timer:sleep(50),
    %% Server should still be responsive
    {ok, _} = flurm_fairshare:get_shares(<<"user">>, <<"account">>).

%%====================================================================
%% Complex Scenario Tests
%%====================================================================

complex_scenario_test_() ->
    {"complex scenario tests",
     {foreach,
      fun setup_server/0,
      fun cleanup_server/1,
      [
       {"fairshare priority comparison", fun test_fairshare_priority_comparison/0},
       {"multiple accounts and users", fun test_multiple_accounts_users/0},
       {"share updates affect priority", fun test_share_updates_affect_priority/0}
      ]}}.

test_fairshare_priority_comparison() ->
    %% Set up two users with equal shares
    ok = flurm_fairshare:set_shares(<<"light">>, <<"dept">>, 100),
    ok = flurm_fairshare:set_shares(<<"heavy">>, <<"dept">>, 100),

    %% Light user uses a little
    ok = flurm_fairshare:record_usage(<<"light">>, <<"dept">>, 100, 10),
    %% Heavy user uses a lot
    ok = flurm_fairshare:record_usage(<<"heavy">>, <<"dept">>, 9900, 990),
    timer:sleep(50),

    %% Light user should have higher priority
    LightFactor = flurm_fairshare:get_priority_factor(<<"light">>, <<"dept">>),
    HeavyFactor = flurm_fairshare:get_priority_factor(<<"heavy">>, <<"dept">>),

    ?assert(LightFactor > HeavyFactor).

test_multiple_accounts_users() ->
    %% User in multiple accounts
    ok = flurm_fairshare:set_shares(<<"multi_user">>, <<"acct1">>, 50),
    ok = flurm_fairshare:set_shares(<<"multi_user">>, <<"acct2">>, 150),

    ok = flurm_fairshare:record_usage(<<"multi_user">>, <<"acct1">>, 1000, 100),
    ok = flurm_fairshare:record_usage(<<"multi_user">>, <<"acct2">>, 500, 50),
    timer:sleep(50),

    %% Each account should have independent tracking
    {ok, Usage1} = flurm_fairshare:get_usage(<<"multi_user">>, <<"acct1">>),
    {ok, Usage2} = flurm_fairshare:get_usage(<<"multi_user">>, <<"acct2">>),

    ?assertEqual(1000.0, Usage1),
    ?assertEqual(500.0, Usage2).

test_share_updates_affect_priority() ->
    %% Test that a user with more shares gets higher priority
    %% than a user with fewer shares when they have the same usage
    User1 = <<"highshare_user">>,
    User2 = <<"lowshare_user">>,
    Account = <<"share_compare_account">>,

    %% User1 has high shares, User2 has low shares
    ok = flurm_fairshare:set_shares(User1, Account, 1000),
    ok = flurm_fairshare:set_shares(User2, Account, 10),

    %% Both have same usage
    ok = flurm_fairshare:record_usage(User1, Account, 500, 50),
    ok = flurm_fairshare:record_usage(User2, Account, 500, 50),
    timer:sleep(50),

    Factor1 = flurm_fairshare:get_priority_factor(User1, Account),
    Factor2 = flurm_fairshare:get_priority_factor(User2, Account),

    %% User with more shares (User1) should have higher priority
    %% because they used less of their share proportion
    ?assert(Factor1 > Factor2).
