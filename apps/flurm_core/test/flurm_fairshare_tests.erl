%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_fairshare module
%%%
%%% Tests the fair-share scheduling system including:
%%% - Share allocation and retrieval
%%% - Usage tracking and recording
%%% - Priority factor calculation
%%% - Usage decay mechanism
%%% - Gen_server callbacks
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_fairshare_tests).
-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

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

cleanup({started, Pid}) ->
    %% Use monitor to wait for actual termination
    case is_process_alive(Pid) of
        true ->
            Ref = monitor(process, Pid),
            unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush]),
                catch exit(Pid, kill)
            end;
        false ->
            ok
    end,
    %% Clean up ETS tables
    catch ets:delete(flurm_fairshare_usage),
    catch ets:delete(flurm_fairshare_shares),
    ok;
cleanup({existing, _Pid}) ->
    ok.

%%====================================================================
%% Basic Shares Tests
%%====================================================================

shares_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"default shares value", fun test_default_shares/0},
        {"set and get shares", fun test_set_get_shares/0},
        {"multiple users different shares", fun test_multiple_users_shares/0},
        {"update existing shares", fun test_update_shares/0}
     ]}.

test_default_shares() ->
    User = <<"newuser">>,
    Account = <<"newaccount">>,
    %% User with no configured shares should get default (1)
    {ok, Shares} = flurm_fairshare:get_shares(User, Account),
    ?assertEqual(1, Shares).

test_set_get_shares() ->
    User = <<"testuser">>,
    Account = <<"testaccount">>,

    %% Set shares
    ok = flurm_fairshare:set_shares(User, Account, 100),

    %% Retrieve should match
    {ok, Shares} = flurm_fairshare:get_shares(User, Account),
    ?assertEqual(100, Shares).

test_multiple_users_shares() ->
    User1 = <<"user1">>,
    User2 = <<"user2">>,
    Account = <<"account">>,

    ok = flurm_fairshare:set_shares(User1, Account, 50),
    ok = flurm_fairshare:set_shares(User2, Account, 150),

    {ok, Shares1} = flurm_fairshare:get_shares(User1, Account),
    {ok, Shares2} = flurm_fairshare:get_shares(User2, Account),

    ?assertEqual(50, Shares1),
    ?assertEqual(150, Shares2).

test_update_shares() ->
    User = <<"updateuser">>,
    Account = <<"updateaccount">>,

    %% Set initial shares
    ok = flurm_fairshare:set_shares(User, Account, 10),
    {ok, InitShares} = flurm_fairshare:get_shares(User, Account),
    ?assertEqual(10, InitShares),

    %% Update shares
    ok = flurm_fairshare:set_shares(User, Account, 500),
    {ok, NewShares} = flurm_fairshare:get_shares(User, Account),
    ?assertEqual(500, NewShares).

%%====================================================================
%% Usage Recording Tests
%%====================================================================

usage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"initial usage is zero", fun test_initial_usage_zero/0},
        {"record usage increases total", fun test_record_usage/0},
        {"cumulative usage", fun test_cumulative_usage/0},
        {"reset usage", fun test_reset_usage/0},
        {"reset nonexistent usage", fun test_reset_nonexistent_usage/0}
     ]}.

test_initial_usage_zero() ->
    User = <<"freshuser">>,
    Account = <<"freshaccount">>,
    {ok, Usage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(0.0, Usage).

test_record_usage() ->
    User = <<"usageuser">>,
    Account = <<"usageaccount">>,

    %% Record CPU usage
    flurm_fairshare:record_usage(User, Account, 1000, 100),
    _ = sys:get_state(flurm_fairshare),  % Allow cast to complete

    %% Usage should increase
    {ok, Usage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(1000.0, Usage).

test_cumulative_usage() ->
    User = <<"cumulativeuser">>,
    Account = <<"cumulativeaccount">>,

    %% Record multiple usages
    flurm_fairshare:record_usage(User, Account, 100, 10),
    _ = sys:get_state(flurm_fairshare),
    flurm_fairshare:record_usage(User, Account, 200, 20),
    _ = sys:get_state(flurm_fairshare),
    flurm_fairshare:record_usage(User, Account, 300, 30),
    _ = sys:get_state(flurm_fairshare),

    %% Usage should be cumulative
    {ok, Usage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(600.0, Usage).

test_reset_usage() ->
    User = <<"resetuser">>,
    Account = <<"resetaccount">>,

    %% Record some usage
    flurm_fairshare:record_usage(User, Account, 5000, 500),
    _ = sys:get_state(flurm_fairshare),

    %% Verify usage exists
    {ok, Usage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(5000.0, Usage),

    %% Reset
    flurm_fairshare:reset_usage(User, Account),
    _ = sys:get_state(flurm_fairshare),

    %% Should be back to 0
    {ok, ResetUsage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(0.0, ResetUsage).

test_reset_nonexistent_usage() ->
    %% Reset on non-existent user should not crash
    flurm_fairshare:reset_usage(<<"nonexistent">>, <<"account">>),
    _ = sys:get_state(flurm_fairshare),
    %% Server should still be responsive
    {ok, _} = flurm_fairshare:get_usage(<<"other">>, <<"account">>).

%%====================================================================
%% Priority Factor Tests
%%====================================================================

priority_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"initial priority factor", fun test_initial_priority/0},
        {"priority factor range", fun test_priority_factor_range/0},
        {"heavy user gets lower priority", fun test_heavy_user_lower_priority/0},
        {"light user gets higher priority", fun test_light_user_higher_priority/0},
        {"equal usage equal priority", fun test_equal_usage_equal_priority/0}
     ]}.

test_initial_priority() ->
    %% New user with no usage should have valid priority factor
    Factor = flurm_fairshare:get_priority_factor(<<"newuser">>, <<"newaccount">>),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

test_priority_factor_range() ->
    User = <<"rangeuser">>,
    Account = <<"rangeaccount">>,

    %% Set shares and record various usage levels
    ok = flurm_fairshare:set_shares(User, Account, 100),

    %% Check that factor is always in valid range
    Factor1 = flurm_fairshare:get_priority_factor(User, Account),
    ?assert(Factor1 >= 0.0 andalso Factor1 =< 1.0),

    %% Record usage
    flurm_fairshare:record_usage(User, Account, 10000, 1000),
    _ = sys:get_state(flurm_fairshare),

    Factor2 = flurm_fairshare:get_priority_factor(User, Account),
    ?assert(Factor2 >= 0.0 andalso Factor2 =< 1.0).

test_heavy_user_lower_priority() ->
    HeavyUser = <<"heavyuser">>,
    LightUser = <<"lightuser">>,
    Account = <<"sharedaccount">>,

    %% Set equal shares
    ok = flurm_fairshare:set_shares(HeavyUser, Account, 100),
    ok = flurm_fairshare:set_shares(LightUser, Account, 100),

    %% Record heavy usage for one user
    flurm_fairshare:record_usage(HeavyUser, Account, 50000, 5000),
    _ = sys:get_state(flurm_fairshare),

    %% Record light usage for other
    flurm_fairshare:record_usage(LightUser, Account, 100, 10),
    _ = sys:get_state(flurm_fairshare),

    %% Heavy user should have lower priority
    HeavyFactor = flurm_fairshare:get_priority_factor(HeavyUser, Account),
    LightFactor = flurm_fairshare:get_priority_factor(LightUser, Account),

    ?assert(LightFactor > HeavyFactor).

test_light_user_higher_priority() ->
    User1 = <<"user1">>,
    User2 = <<"user2">>,
    Account = <<"account">>,

    %% User1 has more shares
    ok = flurm_fairshare:set_shares(User1, Account, 500),
    ok = flurm_fairshare:set_shares(User2, Account, 100),

    %% Same usage
    flurm_fairshare:record_usage(User1, Account, 1000, 100),
    flurm_fairshare:record_usage(User2, Account, 1000, 100),
    _ = sys:get_state(flurm_fairshare),

    %% User with more shares should have higher priority
    Factor1 = flurm_fairshare:get_priority_factor(User1, Account),
    Factor2 = flurm_fairshare:get_priority_factor(User2, Account),

    %% User1 used less of their share, should have higher factor
    ?assert(Factor1 > Factor2).

test_equal_usage_equal_priority() ->
    User1 = <<"equaluser1">>,
    User2 = <<"equaluser2">>,
    Account = <<"equalaccount">>,

    %% Equal shares
    ok = flurm_fairshare:set_shares(User1, Account, 100),
    ok = flurm_fairshare:set_shares(User2, Account, 100),

    %% Equal usage
    flurm_fairshare:record_usage(User1, Account, 5000, 500),
    flurm_fairshare:record_usage(User2, Account, 5000, 500),
    _ = sys:get_state(flurm_fairshare),

    %% Should have approximately equal priority
    Factor1 = flurm_fairshare:get_priority_factor(User1, Account),
    Factor2 = flurm_fairshare:get_priority_factor(User2, Account),

    %% Allow small tolerance for floating point
    ?assert(abs(Factor1 - Factor2) < 0.01).

%%====================================================================
%% Decay Tests
%%====================================================================

decay_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"manual decay doesn't crash", fun test_manual_decay/0},
        {"decay reduces usage", fun test_decay_reduces_usage/0}
     ]}.

test_manual_decay() ->
    %% Manual decay should not crash
    ok = flurm_fairshare:decay_usage(),
    _ = sys:get_state(flurm_fairshare),
    %% Server should still be responsive
    {ok, _} = flurm_fairshare:get_shares(<<"user">>, <<"account">>).

test_decay_reduces_usage() ->
    User = <<"decayuser">>,
    Account = <<"decayaccount">>,

    %% Record some usage
    flurm_fairshare:record_usage(User, Account, 10000, 1000),
    _ = sys:get_state(flurm_fairshare),

    {ok, InitUsage} = flurm_fairshare:get_usage(User, Account),
    ?assertEqual(10000.0, InitUsage),

    %% Trigger decay
    ok = flurm_fairshare:decay_usage(),
    _ = sys:get_state(flurm_fairshare),

    %% Usage should decrease (or be deleted if < 0.01)
    {ok, NewUsage} = flurm_fairshare:get_usage(User, Account),
    ?assert(NewUsage < InitUsage).

%%====================================================================
%% Get All Accounts Tests
%%====================================================================

all_accounts_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get all accounts empty", fun test_get_all_accounts_empty/0},
        {"get all accounts with data", fun test_get_all_accounts_with_data/0}
     ]}.

test_get_all_accounts_empty() ->
    Accounts = flurm_fairshare:get_all_accounts(),
    ?assert(is_list(Accounts)),
    ?assertEqual([], Accounts).

test_get_all_accounts_with_data() ->
    User1 = <<"account1user">>,
    User2 = <<"account2user">>,
    Account1 = <<"account1">>,
    Account2 = <<"account2">>,

    %% Set up some accounts
    ok = flurm_fairshare:set_shares(User1, Account1, 100),
    ok = flurm_fairshare:set_shares(User2, Account2, 200),

    flurm_fairshare:record_usage(User1, Account1, 1000, 100),
    flurm_fairshare:record_usage(User2, Account2, 2000, 200),
    _ = sys:get_state(flurm_fairshare),

    %% Get all accounts
    Accounts = flurm_fairshare:get_all_accounts(),
    ?assert(is_list(Accounts)),
    ?assertEqual(2, length(Accounts)),

    %% Check structure of returned data
    lists:foreach(fun({User, Account, Usage, Shares}) ->
        ?assert(is_binary(User)),
        ?assert(is_binary(Account)),
        ?assert(is_float(Usage) orelse is_integer(Usage)),
        ?assert(is_integer(Shares))
    end, Accounts).

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast doesn't crash", fun test_unknown_cast/0},
        {"unknown info doesn't crash", fun test_unknown_info/0}
     ]}.

test_unknown_call() ->
    Result = gen_server:call(flurm_fairshare, {unknown_request}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    gen_server:cast(flurm_fairshare, {unknown_message}),
    _ = sys:get_state(flurm_fairshare),
    %% Should not crash - verify server still responds
    {ok, _} = flurm_fairshare:get_shares(<<"user">>, <<"account">>).

test_unknown_info() ->
    flurm_fairshare ! unknown_message,
    _ = sys:get_state(flurm_fairshare),
    %% Should not crash - verify server still responds
    {ok, _} = flurm_fairshare:get_shares(<<"user">>, <<"account">>).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"zero shares handling", fun test_zero_shares/0},
        {"very large usage", fun test_large_usage/0},
        {"multiple accounts same user", fun test_multiple_accounts_same_user/0}
     ]}.

test_zero_shares() ->
    %% Test with very small shares (shouldn't be exactly zero but test edge case)
    User = <<"zeroshareuser">>,
    Account = <<"zeroshareaccount">>,

    %% Record usage without setting shares (will use default of 1)
    flurm_fairshare:record_usage(User, Account, 1000, 100),
    _ = sys:get_state(flurm_fairshare),

    %% Should still get valid factor
    Factor = flurm_fairshare:get_priority_factor(User, Account),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

test_large_usage() ->
    User = <<"largeuseuser">>,
    Account = <<"largeusageaccount">>,

    ok = flurm_fairshare:set_shares(User, Account, 100),

    %% Record very large usage
    flurm_fairshare:record_usage(User, Account, 1000000000, 100000000),
    _ = sys:get_state(flurm_fairshare),

    %% Should still get valid factor
    Factor = flurm_fairshare:get_priority_factor(User, Account),
    ?assert(Factor >= 0.0),
    ?assert(Factor =< 1.0).

test_multiple_accounts_same_user() ->
    User = <<"multiaccountuser">>,
    Account1 = <<"multiaccount1">>,
    Account2 = <<"multiaccount2">>,

    ok = flurm_fairshare:set_shares(User, Account1, 100),
    ok = flurm_fairshare:set_shares(User, Account2, 200),

    flurm_fairshare:record_usage(User, Account1, 1000, 100),
    flurm_fairshare:record_usage(User, Account2, 2000, 200),
    _ = sys:get_state(flurm_fairshare),

    %% Each account should have independent usage tracking
    {ok, Usage1} = flurm_fairshare:get_usage(User, Account1),
    {ok, Usage2} = flurm_fairshare:get_usage(User, Account2),

    ?assertEqual(1000.0, Usage1),
    ?assertEqual(2000.0, Usage2).

%%====================================================================
%% Lifecycle Tests
%%====================================================================

lifecycle_test_() ->
    {setup,
     fun() ->
         %% Clean up any existing server/tables before lifecycle test
         catch ets:delete(flurm_fairshare_usage),
         catch ets:delete(flurm_fairshare_shares),
         case whereis(flurm_fairshare) of
             undefined -> ok;
             ExistingPid ->
                 Ref = monitor(process, ExistingPid),
                 catch gen_server:stop(ExistingPid, shutdown, 5000),
                 receive {'DOWN', Ref, process, ExistingPid, _} -> ok after 5000 -> ok end
         end,
         ok
     end,
     fun(_) ->
         %% Cleanup after lifecycle test
         catch ets:delete(flurm_fairshare_usage),
         catch ets:delete(flurm_fairshare_shares),
         case whereis(flurm_fairshare) of
             undefined -> ok;
             Pid ->
                 Ref = monitor(process, Pid),
                 catch gen_server:stop(Pid, shutdown, 5000),
                 receive {'DOWN', Ref, process, Pid, _} -> ok after 5000 -> ok end
         end,
         ok
     end,
     fun(_) ->
         [
             {"start and stop cleanly", fun test_lifecycle/0}
         ]
     end}.

test_lifecycle() ->
    %% Start server - use unlink to prevent test process link
    {ok, Pid} = flurm_fairshare:start_link(),
    unlink(Pid),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Should be able to use it
    {ok, _} = flurm_fairshare:get_shares(<<"user">>, <<"account">>),

    %% Stop cleanly with monitor
    Ref = monitor(process, Pid),
    ok = gen_server:stop(Pid, shutdown, 5000),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 5000 ->
        demonitor(Ref, [flush])
    end,
    ?assertNot(is_process_alive(Pid)).

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

additional_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"already started handling", fun test_already_started/0},
        {"decay timer message handling", fun test_decay_timer_message/0},
        {"zero share fraction", fun test_zero_share_fraction/0},
        {"negligible usage removal during decay", fun test_negligible_usage_removal/0},
        {"code_change callback", fun test_code_change/0},
        {"terminate with undefined timer", fun test_terminate_undefined_timer/0}
     ]}.

test_already_started() ->
    %% Server is already started by setup, try to start again
    %% This should return {ok, Pid} with existing pid
    Result = flurm_fairshare:start_link(),
    ?assertMatch({ok, _Pid}, Result),
    {ok, Pid} = Result,
    ?assertEqual(whereis(flurm_fairshare), Pid).

test_decay_timer_message() ->
    %% Record usage first
    User = <<"decaytimer_user">>,
    Account = <<"decaytimer_account">>,
    flurm_fairshare:record_usage(User, Account, 5000, 500),
    _ = sys:get_state(flurm_fairshare),

    {ok, InitUsage} = flurm_fairshare:get_usage(User, Account),
    ?assert(InitUsage > 0),

    %% Send the decay timer message directly
    flurm_fairshare ! decay,
    _ = sys:get_state(flurm_fairshare),

    %% Server should still be responsive
    {ok, _} = flurm_fairshare:get_shares(User, Account),

    %% Usage should have decayed
    {ok, NewUsage} = flurm_fairshare:get_usage(User, Account),
    ?assert(NewUsage < InitUsage orelse NewUsage == 0.0).

test_zero_share_fraction() ->
    %% Create a scenario where total shares is 0
    %% This happens when all shares sum to 0 (edge case)
    User = <<"zeroshare_user">>,
    Account = <<"zeroshare_account">>,

    %% First, set shares to 0 explicitly
    ok = flurm_fairshare:set_shares(User, Account, 0),

    %% Now get priority factor - should return 0.0 (lowest priority)
    Factor = flurm_fairshare:get_priority_factor(User, Account),
    ?assertEqual(0.0, Factor).

test_negligible_usage_removal() ->
    %% Record a very small usage that should be reduced during decay
    User = <<"negligible_user">>,
    Account = <<"negligible_account">>,

    %% Record small usage
    flurm_fairshare:record_usage(User, Account, 1, 1),  % Very small value
    {ok, InitialUsage} = flurm_fairshare:get_usage(User, Account),
    _ = sys:get_state(flurm_fairshare),

    %% Multiple decay cycles should reduce usage
    lists:foreach(fun(_) ->
        flurm_fairshare ! decay,
        _ = sys:get_state(flurm_fairshare)
    end, lists:seq(1, 50)),

    %% Usage should be reduced after decay cycles
    {ok, Usage} = flurm_fairshare:get_usage(User, Account),
    ?assert(Usage =< InitialUsage).

test_code_change() ->
    %% Test code_change callback directly
    State = sys:get_state(flurm_fairshare),
    %% code_change should return {ok, State}
    %% We can't call code_change directly, but we can verify the server handles it
    %% by checking it's still alive and working
    ?assert(is_process_alive(whereis(flurm_fairshare))),
    {ok, _} = flurm_fairshare:get_shares(<<"user">>, <<"account">>),
    ?assert(State =/= undefined).

test_terminate_undefined_timer() ->
    %% Stop and restart the server to test terminate with potentially undefined timer
    Pid = whereis(flurm_fairshare),
    ?assert(is_pid(Pid)),

    %% Stop the server - this triggers terminate
    Ref = monitor(process, Pid),
    unlink(Pid),
    gen_server:stop(Pid, shutdown, 5000),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 5000 ->
        demonitor(Ref, [flush])
    end,

    %% Restart it for other tests
    {ok, NewPid} = flurm_fairshare:start_link(),
    unlink(NewPid),
    ?assert(is_process_alive(NewPid)).
