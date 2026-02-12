%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit tests for flurm_fairshare module
%%%
%%% Targets 100% line coverage by exercising every function, every
%%% branch, and every clause in the gen_server implementation.
%%%
%%% Strategy:
%%% - Use foreach fixtures that start/stop the real gen_server
%%% - Call real exported functions (no mocking)
%%% - Use sys:get_state/1 to synchronize after casts
%%% - Directly call TEST-exported calculate_priority_factor/3
%%% - Directly call gen_server callbacks with hand-built state records
%%%   to reach branches that are hard to trigger through the API
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_fairshare_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%% Mirror the ETS table names from the source module
-define(USAGE_TABLE, flurm_fairshare_usage).
-define(SHARES_TABLE, flurm_fairshare_shares).
-define(DEFAULT_SHARES, 1).

%% Mirror the state record from the source module so we can
%% construct it in direct-callback tests and in terminate tests.
-record(state, {
    decay_timer :: reference() | undefined,
    total_shares :: non_neg_integer(),
    total_usage :: float()
}).

%%====================================================================
%% Setup / Cleanup for gen_server-based tests
%%====================================================================

setup() ->
    %% Ensure any prior instance is fully dead before starting fresh.
    case whereis(flurm_fairshare) of
        undefined -> ok;
        Old ->
            unlink(Old),
            MRef = monitor(process, Old),
            catch gen_server:stop(Old, shutdown, 5000),
            receive {'DOWN', MRef, _, _, _} -> ok
            after 5000 -> exit(Old, kill), ok
            end
    end,
    catch ets:delete(?USAGE_TABLE),
    catch ets:delete(?SHARES_TABLE),
    {ok, Pid} = flurm_fairshare:start_link(),
    Pid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true ->
            unlink(Pid),
            MRef = monitor(process, Pid),
            gen_server:stop(Pid, normal, 5000),
            receive {'DOWN', MRef, _, _, _} -> ok
            after 5000 -> exit(Pid, kill), ok
            end;
        false ->
            ok
    end,
    catch ets:delete(?USAGE_TABLE),
    catch ets:delete(?SHARES_TABLE),
    ok.

%%====================================================================
%% Setup / Cleanup for bare-ETS tests (no gen_server running)
%%====================================================================

setup_ets() ->
    %% Make sure no gen_server is running (it owns the tables)
    case whereis(flurm_fairshare) of
        undefined -> ok;
        Old ->
            unlink(Old),
            MRef = monitor(process, Old),
            catch gen_server:stop(Old, shutdown, 5000),
            receive {'DOWN', MRef, _, _, _} -> ok
            after 5000 -> exit(Old, kill), ok
            end
    end,
    catch ets:delete(?USAGE_TABLE),
    catch ets:delete(?SHARES_TABLE),
    ets:new(?USAGE_TABLE, [named_table, public, set]),
    ets:new(?SHARES_TABLE, [named_table, public, set]),
    ok.

cleanup_ets(_) ->
    catch ets:delete(?USAGE_TABLE),
    catch ets:delete(?SHARES_TABLE),
    ok.

%%====================================================================
%% Test generators -- one {foreach, ...} per logical group
%%====================================================================

%% ---- start_link / init -------------------------------------------
start_link_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"start_link returns {ok, Pid}", fun start_link_ok/0},
        {"start_link already_started returns existing pid",
         fun start_link_already_started/0}
    ]}.

%% ---- shares management (set_shares, get_shares) -------------------
shares_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"get_shares default is DEFAULT_SHARES", fun get_shares_default/0},
        {"set_shares then get_shares round-trips", fun set_get_shares/0},
        {"set_shares on new entry (OldShares = 0 branch)", fun set_shares_new/0},
        {"set_shares updating existing entry", fun set_shares_update_existing/0}
    ]}.

%% ---- usage management (record_usage, get_usage, reset_usage) ------
usage_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"get_usage returns 0.0 for unknown key", fun get_usage_unknown/0},
        {"record_usage on new key creates entry", fun record_usage_new/0},
        {"record_usage accumulates on existing key", fun record_usage_existing/0},
        {"reset_usage removes existing entry", fun reset_usage_existing/0},
        {"reset_usage on non-existent key is no-op", fun reset_usage_missing/0}
    ]}.

%% ---- get_priority_factor via API ----------------------------------
priority_api_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"get_priority_factor for new user (no usage, no shares)",
         fun priority_new_user/0},
        {"get_priority_factor after usage and shares configured",
         fun priority_with_usage_and_shares/0}
    ]}.

%% ---- decay (handle_cast decay_usage) ------------------------------
decay_cast_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"decay_usage cast reduces usage", fun decay_cast_reduces/0},
        {"decay_usage on empty table is safe", fun decay_cast_empty/0}
    ]}.

%% ---- decay via handle_info (timer message) ------------------------
decay_info_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"handle_info decay triggers decay and reschedules timer",
         fun decay_info_message/0}
    ]}.

%% ---- get_all_accounts ---------------------------------------------
all_accounts_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"get_all_accounts empty", fun all_accounts_empty/0},
        {"get_all_accounts with data, default shares fallback",
         fun all_accounts_with_data/0}
    ]}.

%% ---- catch-all clauses (handle_call, handle_cast, handle_info) ----
catchall_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"unknown call returns {error, unknown_request}",
         fun unknown_call/0},
        {"unknown cast is silently ignored", fun unknown_cast/0},
        {"unknown info is silently ignored", fun unknown_info/0}
    ]}.

%% ---- terminate (both branches) + code_change ----------------------
terminate_code_change_test_() ->
    {"terminate and code_change via direct callback",
     {foreach, fun setup_ets/0, fun cleanup_ets/1, [
        {"terminate with a real timer reference cancels timer",
         fun terminate_with_timer/0},
        {"terminate with undefined timer is safe",
         fun terminate_undefined_timer/0},
        {"code_change returns {ok, State}", fun code_change_passthrough/0}
    ]}}.

%% ---- calculate_priority_factor/3 (TEST-exported) ------------------
calc_priority_test_() ->
    {"calculate_priority_factor/3 direct tests",
     {foreach, fun setup_ets/0, fun cleanup_ets/1, [
        {"no usage, no shares in ETS (defaults)", fun calc_no_usage_no_shares/0},
        {"usage present, shares present, normal case",
         fun calc_normal_case/0},
        {"ShareFraction == 0.0 returns 0.0", fun calc_zero_share_fraction/0},
        {"user not in shares table -> DEFAULT_SHARES + TotalShares branch",
         fun calc_default_shares_branch/0},
        {"user in shares table -> TotalShares + 0 branch",
         fun calc_existing_shares_branch/0},
        {"extreme high usage clamps factor to >= 0.0",
         fun calc_extreme_high_usage/0},
        {"zero total_usage forces max(1.0) denominator",
         fun calc_zero_total_usage/0}
    ]}}.

%% ---- do_decay_usage via handle_cast (covers both if branches) -----
do_decay_via_cast_test_() ->
    {"do_decay_usage internal via handle_cast",
     {foreach, fun setup_ets/0, fun cleanup_ets/1, [
        {"large usage is decayed but kept", fun decay_keeps_large/0},
        {"tiny usage is deleted (NewUsage < 0.01 branch)",
         fun decay_deletes_tiny/0},
        {"empty table produces 0.0 total", fun decay_empty_table/0}
    ]}}.

%% ---- init direct callback test ------------------------------------
init_direct_test_() ->
    {"init/1 direct callback",
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
        {"init creates tables and returns state with timer",
         fun init_direct/0}
      ]}}.

%%====================================================================
%% Test implementations -- start_link / init
%%====================================================================

start_link_ok() ->
    %% The server is already started by setup/0. Verify it registered.
    Pid = whereis(flurm_fairshare),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)).

start_link_already_started() ->
    %% Calling start_link a second time should return {ok, ExistingPid}
    %% because of the {error, {already_started, Pid}} clause.
    ExistingPid = whereis(flurm_fairshare),
    {ok, ReturnedPid} = flurm_fairshare:start_link(),
    ?assertEqual(ExistingPid, ReturnedPid).

%%====================================================================
%% Test implementations -- shares
%%====================================================================

get_shares_default() ->
    {ok, Shares} = flurm_fairshare:get_shares(<<"unknown_user">>, <<"unknown_acct">>),
    ?assertEqual(?DEFAULT_SHARES, Shares).

set_get_shares() ->
    ok = flurm_fairshare:set_shares(<<"alice">>, <<"engineering">>, 200),
    {ok, Shares} = flurm_fairshare:get_shares(<<"alice">>, <<"engineering">>),
    ?assertEqual(200, Shares).

set_shares_new() ->
    %% First set_shares for a key that has no prior entry.
    %% This exercises the [] -> 0 branch in handle_call set_shares.
    ok = flurm_fairshare:set_shares(<<"brand_new">>, <<"acct">>, 42),
    {ok, S} = flurm_fairshare:get_shares(<<"brand_new">>, <<"acct">>),
    ?assertEqual(42, S).

set_shares_update_existing() ->
    %% First insert, then update to exercise [{Key, S}] -> S branch.
    ok = flurm_fairshare:set_shares(<<"bob">>, <<"research">>, 50),
    ok = flurm_fairshare:set_shares(<<"bob">>, <<"research">>, 300),
    {ok, S} = flurm_fairshare:get_shares(<<"bob">>, <<"research">>),
    ?assertEqual(300, S).

%%====================================================================
%% Test implementations -- usage
%%====================================================================

get_usage_unknown() ->
    {ok, Usage} = flurm_fairshare:get_usage(<<"nobody">>, <<"nowhere">>),
    ?assertEqual(0.0, Usage).

record_usage_new() ->
    %% Recording usage on a brand-new key exercises the [] -> {0.0, ok} branch.
    ok = flurm_fairshare:record_usage(<<"new_user">>, <<"new_acct">>, 1000, 100),
    _ = sys:get_state(flurm_fairshare),
    {ok, Usage} = flurm_fairshare:get_usage(<<"new_user">>, <<"new_acct">>),
    ?assertEqual(1000.0, Usage).

record_usage_existing() ->
    %% Recording twice exercises the [{Key, U, _}] -> {U, ok} branch.
    ok = flurm_fairshare:record_usage(<<"existing">>, <<"acct">>, 500, 50),
    _ = sys:get_state(flurm_fairshare),
    ok = flurm_fairshare:record_usage(<<"existing">>, <<"acct">>, 300, 30),
    _ = sys:get_state(flurm_fairshare),
    {ok, Usage} = flurm_fairshare:get_usage(<<"existing">>, <<"acct">>),
    ?assertEqual(800.0, Usage).

reset_usage_existing() ->
    ok = flurm_fairshare:record_usage(<<"resettable">>, <<"acct">>, 5000, 500),
    _ = sys:get_state(flurm_fairshare),
    {ok, Before} = flurm_fairshare:get_usage(<<"resettable">>, <<"acct">>),
    ?assertEqual(5000.0, Before),
    ok = flurm_fairshare:reset_usage(<<"resettable">>, <<"acct">>),
    _ = sys:get_state(flurm_fairshare),
    {ok, After} = flurm_fairshare:get_usage(<<"resettable">>, <<"acct">>),
    ?assertEqual(0.0, After).

reset_usage_missing() ->
    %% Reset on non-existent key hits the [] -> {noreply, State} branch.
    ok = flurm_fairshare:reset_usage(<<"ghost">>, <<"acct">>),
    _ = sys:get_state(flurm_fairshare),
    %% Server is still alive.
    {ok, _} = flurm_fairshare:get_usage(<<"ghost">>, <<"acct">>).

%%====================================================================
%% Test implementations -- get_priority_factor (API)
%%====================================================================

priority_new_user() ->
    Factor = flurm_fairshare:get_priority_factor(<<"fresh">>, <<"acct">>),
    ?assert(is_float(Factor)),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

priority_with_usage_and_shares() ->
    ok = flurm_fairshare:set_shares(<<"alice">>, <<"eng">>, 100),
    ok = flurm_fairshare:record_usage(<<"alice">>, <<"eng">>, 5000, 500),
    _ = sys:get_state(flurm_fairshare),
    Factor = flurm_fairshare:get_priority_factor(<<"alice">>, <<"eng">>),
    ?assert(is_float(Factor)),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

%%====================================================================
%% Test implementations -- decay via cast
%%====================================================================

decay_cast_reduces() ->
    ok = flurm_fairshare:record_usage(<<"dec_user">>, <<"dec_acct">>, 10000, 1000),
    _ = sys:get_state(flurm_fairshare),
    {ok, Before} = flurm_fairshare:get_usage(<<"dec_user">>, <<"dec_acct">>),
    ?assertEqual(10000.0, Before),
    ok = flurm_fairshare:decay_usage(),
    _ = sys:get_state(flurm_fairshare),
    {ok, After} = flurm_fairshare:get_usage(<<"dec_user">>, <<"dec_acct">>),
    ?assert(After =< Before).

decay_cast_empty() ->
    ok = flurm_fairshare:decay_usage(),
    _ = sys:get_state(flurm_fairshare),
    %% Still responsive.
    {ok, _} = flurm_fairshare:get_shares(<<"x">>, <<"y">>).

%%====================================================================
%% Test implementations -- decay via handle_info (Pid ! decay)
%%====================================================================

decay_info_message() ->
    %% Record some usage so decay has work to do.
    ok = flurm_fairshare:record_usage(<<"info_user">>, <<"info_acct">>, 50000, 5000),
    _ = sys:get_state(flurm_fairshare),

    %% Send the decay atom directly. This exercises handle_info(decay, ...).
    Pid = whereis(flurm_fairshare),
    Pid ! decay,
    _ = sys:get_state(flurm_fairshare),

    %% The server should still be alive and the timer rescheduled.
    ?assert(is_process_alive(Pid)),
    {ok, _} = flurm_fairshare:get_shares(<<"x">>, <<"y">>).

%%====================================================================
%% Test implementations -- get_all_accounts
%%====================================================================

all_accounts_empty() ->
    Accounts = flurm_fairshare:get_all_accounts(),
    ?assertEqual([], Accounts).

all_accounts_with_data() ->
    %% Set up one user with shares and usage, another with usage only.
    ok = flurm_fairshare:set_shares(<<"u1">>, <<"a1">>, 100),
    ok = flurm_fairshare:record_usage(<<"u1">>, <<"a1">>, 1000, 100),
    %% u2 has usage but no explicit shares -> default shares fallback in
    %% maps:get({User,Account}, SharesMap, DEFAULT_SHARES).
    ok = flurm_fairshare:record_usage(<<"u2">>, <<"a2">>, 2000, 200),
    _ = sys:get_state(flurm_fairshare),

    Accounts = flurm_fairshare:get_all_accounts(),
    ?assertEqual(2, length(Accounts)),

    %% Verify each tuple structure {User, Account, Usage, Shares}.
    Sorted = lists:sort(Accounts),
    [{<<"u1">>, <<"a1">>, U1, S1}, {<<"u2">>, <<"a2">>, U2, S2}] = Sorted,
    ?assertEqual(1000.0, U1),
    ?assertEqual(100, S1),
    ?assertEqual(2000.0, U2),
    ?assertEqual(?DEFAULT_SHARES, S2).

%%====================================================================
%% Test implementations -- catch-all clauses
%%====================================================================

unknown_call() ->
    Result = gen_server:call(flurm_fairshare, {totally_unknown, 1, 2, 3}),
    ?assertEqual({error, unknown_request}, Result).

unknown_cast() ->
    gen_server:cast(flurm_fairshare, {totally_unknown_cast}),
    _ = sys:get_state(flurm_fairshare),
    %% Still alive.
    ?assert(is_process_alive(whereis(flurm_fairshare))).

unknown_info() ->
    whereis(flurm_fairshare) ! {some_random_info, 42},
    _ = sys:get_state(flurm_fairshare),
    ?assert(is_process_alive(whereis(flurm_fairshare))).

%%====================================================================
%% Test implementations -- terminate / code_change (direct callbacks)
%%====================================================================

terminate_with_timer() ->
    Timer = erlang:send_after(60000, self(), dummy_timer_msg),
    State = #state{decay_timer = Timer, total_shares = 0, total_usage = 0.0},
    ok = flurm_fairshare:terminate(shutdown, State),
    %% Timer should have been cancelled.
    ?assertEqual(false, erlang:read_timer(Timer)).

terminate_undefined_timer() ->
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    ok = flurm_fairshare:terminate(normal, State).

code_change_passthrough() ->
    State = #state{decay_timer = undefined, total_shares = 50, total_usage = 123.0},
    {ok, ReturnedState} = flurm_fairshare:code_change("1.0.0", State, []),
    ?assertEqual(State, ReturnedState).

%%====================================================================
%% Test implementations -- calculate_priority_factor/3 (TEST export)
%%====================================================================

calc_no_usage_no_shares() ->
    %% User not in either table. Exercises:
    %% - Usage lookup [] -> 0.0
    %% - Shares lookup [] -> DEFAULT_SHARES
    %% - Second shares lookup [] -> DEFAULT_SHARES added to TotalShares
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    Factor = flurm_fairshare:calculate_priority_factor(
        <<"nobody">>, <<"nowhere">>, State),
    ?assert(is_float(Factor)),
    %% With 0 usage and 0 total_usage, UsageFraction = 0/max(0,1.0) = 0.0
    %% Factor = 2^(-0/ShareFraction) = 2^0 = 1.0
    ?assertEqual(1.0, Factor).

calc_normal_case() ->
    %% User has shares and usage. Exercises the normal (non-zero) path.
    Now = erlang:system_time(second),
    Key = {<<"alice">>, <<"eng">>},
    ets:insert(?SHARES_TABLE, {Key, 50}),
    ets:insert(?USAGE_TABLE, {Key, 500.0, Now}),

    State = #state{decay_timer = undefined, total_shares = 100, total_usage = 1000.0},
    Factor = flurm_fairshare:calculate_priority_factor(
        <<"alice">>, <<"eng">>, State),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0),
    %% ShareFraction = 50/100 = 0.5, UsageFraction = 500/1000 = 0.5
    %% Ratio = 0.5/0.5 = 1.0, Factor = 2^(-1.0) = 0.5
    ?assertEqual(0.5, Factor).

calc_zero_share_fraction() ->
    %% When Shares = 0 and user IS in shares table, ShareFraction = 0/TotalShares.
    %% The second lookup finds an entry -> adds 0 to TotalShares.
    %% ShareFraction = 0 / (TotalShares + 0) = 0.0 -> returns 0.0.
    Now = erlang:system_time(second),
    Key = {<<"zero_shares">>, <<"acct">>},
    ets:insert(?SHARES_TABLE, {Key, 0}),
    ets:insert(?USAGE_TABLE, {Key, 100.0, Now}),

    State = #state{decay_timer = undefined, total_shares = 100, total_usage = 1000.0},
    Factor = flurm_fairshare:calculate_priority_factor(
        <<"zero_shares">>, <<"acct">>, State),
    ?assertEqual(0.0, Factor).

calc_default_shares_branch() ->
    %% User NOT in shares table.
    %% First lookup: [] -> Shares = DEFAULT_SHARES (1)
    %% Second lookup: [] -> adds DEFAULT_SHARES to TotalShares
    %% This exercises the `[] -> ?DEFAULT_SHARES` branch in the second lookup.
    Now = erlang:system_time(second),
    Key = {<<"no_shares_entry">>, <<"acct">>},
    ets:insert(?USAGE_TABLE, {Key, 200.0, Now}),

    State = #state{decay_timer = undefined, total_shares = 100, total_usage = 1000.0},
    Factor = flurm_fairshare:calculate_priority_factor(
        <<"no_shares_entry">>, <<"acct">>, State),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

calc_existing_shares_branch() ->
    %% User IS in shares table.
    %% First lookup finds {Key, S} -> Shares = S.
    %% Second lookup finds entry -> adds 0 to TotalShares.
    %% This exercises the `_ -> 0` branch in the second lookup.
    Now = erlang:system_time(second),
    Key = {<<"in_shares">>, <<"acct">>},
    ets:insert(?SHARES_TABLE, {Key, 50}),
    ets:insert(?USAGE_TABLE, {Key, 300.0, Now}),

    State = #state{decay_timer = undefined, total_shares = 100, total_usage = 1000.0},
    Factor = flurm_fairshare:calculate_priority_factor(
        <<"in_shares">>, <<"acct">>, State),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

calc_extreme_high_usage() ->
    %% Very high usage relative to shares -> factor should be clamped >= 0.0.
    Now = erlang:system_time(second),
    Key = {<<"extreme">>, <<"acct">>},
    ets:insert(?SHARES_TABLE, {Key, 1}),
    ets:insert(?USAGE_TABLE, {Key, 999999.0, Now}),

    State = #state{decay_timer = undefined, total_shares = 100, total_usage = 1000000.0},
    Factor = flurm_fairshare:calculate_priority_factor(
        <<"extreme">>, <<"acct">>, State),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

calc_zero_total_usage() ->
    %% total_usage = 0.0 forces max(1.0, 0.0) = 1.0 denominator.
    Now = erlang:system_time(second),
    Key = {<<"user_zero_total">>, <<"acct">>},
    ets:insert(?SHARES_TABLE, {Key, 50}),
    ets:insert(?USAGE_TABLE, {Key, 0.0, Now}),

    State = #state{decay_timer = undefined, total_shares = 100, total_usage = 0.0},
    Factor = flurm_fairshare:calculate_priority_factor(
        <<"user_zero_total">>, <<"acct">>, State),
    %% Usage = 0.0, TotalUsage = max(1.0, 0.0) = 1.0
    %% UsageFraction = 0.0 / 1.0 = 0.0
    %% Ratio = 0.0 / ShareFraction = 0.0
    %% Factor = 2^(-0.0) = 1.0
    ?assertEqual(1.0, Factor).

%%====================================================================
%% Test implementations -- do_decay_usage (via handle_cast)
%%====================================================================

decay_keeps_large() ->
    %% A recent, large usage value should survive decay (NewUsage >= 0.01).
    Now = erlang:system_time(second),
    Key = {<<"large_decay">>, <<"acct">>},
    ets:insert(?USAGE_TABLE, {Key, 100000.0, Now}),

    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 100000.0},
    {noreply, NewState} = flurm_fairshare:handle_cast(decay_usage, State),

    %% Entry should still exist but decayed.
    [{_, NewUsage, _}] = ets:lookup(?USAGE_TABLE, Key),
    ?assert(NewUsage > 0.01),
    ?assert(NewUsage < 100000.0),
    ?assert(NewState#state.total_usage > 0.0).

decay_deletes_tiny() ->
    %% A very old, very small usage value should be deleted (NewUsage < 0.01).
    OldTime = erlang:system_time(second) - 86400 * 60, % 60 days ago
    Key = {<<"tiny_decay">>, <<"acct">>},
    ets:insert(?USAGE_TABLE, {Key, 0.001, OldTime}),

    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.001},
    {noreply, NewState} = flurm_fairshare:handle_cast(decay_usage, State),

    %% Entry should have been deleted.
    ?assertEqual([], ets:lookup(?USAGE_TABLE, Key)),
    ?assertEqual(0.0, NewState#state.total_usage).

decay_empty_table() ->
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    {noreply, NewState} = flurm_fairshare:handle_cast(decay_usage, State),
    ?assertEqual(0.0, NewState#state.total_usage).

%%====================================================================
%% Test implementations -- init direct callback
%%====================================================================

init_direct() ->
    {ok, State} = flurm_fairshare:init([]),
    %% Tables should exist.
    ?assertEqual(?USAGE_TABLE, ets:info(?USAGE_TABLE, name)),
    ?assertEqual(?SHARES_TABLE, ets:info(?SHARES_TABLE, name)),
    %% State fields.
    ?assertEqual(0, State#state.total_shares),
    ?assertEqual(0.0, State#state.total_usage),
    ?assert(is_reference(State#state.decay_timer)),
    %% Clean up the timer we just created.
    erlang:cancel_timer(State#state.decay_timer).

%%====================================================================
%% Additional direct-callback tests for handle_call / handle_cast
%% to ensure every branch is hit even if the gen_server-based tests
%% cover the same lines (belt-and-suspenders).
%%====================================================================

direct_callback_test_() ->
    {"direct gen_server callback invocations",
     {foreach, fun setup_ets/0, fun cleanup_ets/1, [
        {"handle_call get_usage found branch", fun direct_get_usage_found/0},
        {"handle_call get_usage not-found branch", fun direct_get_usage_not_found/0},
        {"handle_call set_shares existing branch", fun direct_set_shares_existing/0},
        {"handle_call set_shares new branch", fun direct_set_shares_new/0},
        {"handle_call get_shares found branch", fun direct_get_shares_found/0},
        {"handle_call get_shares default branch", fun direct_get_shares_default/0},
        {"handle_call get_all_accounts with data", fun direct_get_all_accounts/0},
        {"handle_call unknown request", fun direct_unknown_call/0},
        {"handle_cast record_usage new key", fun direct_record_usage_new/0},
        {"handle_cast record_usage existing key", fun direct_record_usage_existing/0},
        {"handle_cast reset_usage found", fun direct_reset_usage_found/0},
        {"handle_cast reset_usage not found", fun direct_reset_usage_not_found/0},
        {"handle_cast unknown message", fun direct_unknown_cast/0},
        {"handle_info decay reschedules timer", fun direct_handle_info_decay/0},
        {"handle_info unknown message", fun direct_handle_info_unknown/0},
        {"reset_usage prevents negative total_usage", fun direct_reset_negative_guard/0}
    ]}}.

from() -> {self(), make_ref()}.

direct_get_usage_found() ->
    Now = erlang:system_time(second),
    ets:insert(?USAGE_TABLE, {{<<"u">>, <<"a">>}, 42.0, Now}),
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 42.0},
    {reply, {ok, 42.0}, State} =
        flurm_fairshare:handle_call({get_usage, <<"u">>, <<"a">>}, from(), State).

direct_get_usage_not_found() ->
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    {reply, {ok, +0.0}, State} =
        flurm_fairshare:handle_call({get_usage, <<"x">>, <<"y">>}, from(), State).

direct_set_shares_existing() ->
    ets:insert(?SHARES_TABLE, {{<<"u">>, <<"a">>}, 50}),
    State = #state{decay_timer = undefined, total_shares = 50, total_usage = 0.0},
    {reply, ok, NewState} =
        flurm_fairshare:handle_call({set_shares, <<"u">>, <<"a">>, 200}, from(), State),
    %% 50 - 50 + 200 = 200
    ?assertEqual(200, NewState#state.total_shares).

direct_set_shares_new() ->
    State = #state{decay_timer = undefined, total_shares = 100, total_usage = 0.0},
    {reply, ok, NewState} =
        flurm_fairshare:handle_call({set_shares, <<"new">>, <<"a">>, 75}, from(), State),
    %% 100 - 0 + 75 = 175
    ?assertEqual(175, NewState#state.total_shares).

direct_get_shares_found() ->
    ets:insert(?SHARES_TABLE, {{<<"u">>, <<"a">>}, 999}),
    State = #state{decay_timer = undefined, total_shares = 999, total_usage = 0.0},
    {reply, {ok, 999}, State} =
        flurm_fairshare:handle_call({get_shares, <<"u">>, <<"a">>}, from(), State).

direct_get_shares_default() ->
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    {reply, {ok, ?DEFAULT_SHARES}, State} =
        flurm_fairshare:handle_call({get_shares, <<"x">>, <<"y">>}, from(), State).

direct_get_all_accounts() ->
    Now = erlang:system_time(second),
    ets:insert(?USAGE_TABLE, {{<<"u1">>, <<"a1">>}, 100.0, Now}),
    ets:insert(?SHARES_TABLE, {{<<"u1">>, <<"a1">>}, 50}),
    ets:insert(?USAGE_TABLE, {{<<"u2">>, <<"a2">>}, 200.0, Now}),
    %% u2 has no shares entry -> maps:get default
    State = #state{decay_timer = undefined, total_shares = 50, total_usage = 300.0},
    {reply, Accounts, State} =
        flurm_fairshare:handle_call(get_all_accounts, from(), State),
    ?assertEqual(2, length(Accounts)),
    Sorted = lists:sort(Accounts),
    [{<<"u1">>, <<"a1">>, 100.0, 50}, {<<"u2">>, <<"a2">>, 200.0, ?DEFAULT_SHARES}] = Sorted.

direct_unknown_call() ->
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    {reply, {error, unknown_request}, State} =
        flurm_fairshare:handle_call(banana, from(), State).

direct_record_usage_new() ->
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    {noreply, NewState} =
        flurm_fairshare:handle_cast({record_usage, <<"u">>, <<"a">>, 500, 50}, State),
    ?assertEqual(500.0, NewState#state.total_usage),
    [{_, 500.0, _}] = ets:lookup(?USAGE_TABLE, {<<"u">>, <<"a">>}).

direct_record_usage_existing() ->
    Now = erlang:system_time(second),
    ets:insert(?USAGE_TABLE, {{<<"u">>, <<"a">>}, 300.0, Now}),
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 300.0},
    {noreply, NewState} =
        flurm_fairshare:handle_cast({record_usage, <<"u">>, <<"a">>, 200, 20}, State),
    ?assertEqual(500.0, NewState#state.total_usage),
    [{_, 500.0, _}] = ets:lookup(?USAGE_TABLE, {<<"u">>, <<"a">>}).

direct_reset_usage_found() ->
    Now = erlang:system_time(second),
    ets:insert(?USAGE_TABLE, {{<<"u">>, <<"a">>}, 1000.0, Now}),
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 1000.0},
    {noreply, NewState} =
        flurm_fairshare:handle_cast({reset_usage, <<"u">>, <<"a">>}, State),
    ?assertEqual(0.0, NewState#state.total_usage),
    ?assertEqual([], ets:lookup(?USAGE_TABLE, {<<"u">>, <<"a">>})).

direct_reset_usage_not_found() ->
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    {noreply, State} =
        flurm_fairshare:handle_cast({reset_usage, <<"ghost">>, <<"acct">>}, State).

direct_unknown_cast() ->
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    {noreply, State} =
        flurm_fairshare:handle_cast(some_weird_msg, State).

direct_handle_info_decay() ->
    Now = erlang:system_time(second),
    ets:insert(?USAGE_TABLE, {{<<"u">>, <<"a">>}, 5000.0, Now}),
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 5000.0},
    {noreply, NewState} = flurm_fairshare:handle_info(decay, State),
    %% A new timer should have been scheduled.
    ?assert(is_reference(NewState#state.decay_timer)),
    erlang:cancel_timer(NewState#state.decay_timer).

direct_handle_info_unknown() ->
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 0.0},
    {noreply, State} =
        flurm_fairshare:handle_info({unexpected, message}, State).

direct_reset_negative_guard() ->
    %% When total_usage < OldUsage (should not happen normally),
    %% the max(0.0, ...) guard prevents negative total.
    Now = erlang:system_time(second),
    ets:insert(?USAGE_TABLE, {{<<"u">>, <<"a">>}, 9999.0, Now}),
    State = #state{decay_timer = undefined, total_shares = 0, total_usage = 10.0},
    {noreply, NewState} =
        flurm_fairshare:handle_cast({reset_usage, <<"u">>, <<"a">>}, State),
    ?assertEqual(0.0, NewState#state.total_usage).
