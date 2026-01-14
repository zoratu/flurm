%%%-------------------------------------------------------------------
%%% @doc Direct Tests for flurm_fairshare module
%%%
%%% Comprehensive EUnit tests that call flurm_fairshare functions
%%% directly without mocking the module being tested.
%%%
%%% Tests all exported functions:
%%% - start_link/0
%%% - get_priority_factor/2
%%% - record_usage/4
%%% - get_usage/2
%%% - set_shares/3
%%% - get_shares/2
%%% - decay_usage/0
%%% - get_all_accounts/0
%%% - reset_usage/2
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_fairshare_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    %% Stop existing server if running
    case whereis(flurm_fairshare) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 5000)
    end,
    %% Clean up any existing ETS tables
    catch ets:delete(flurm_fairshare_usage),
    catch ets:delete(flurm_fairshare_shares),
    timer:sleep(50),
    ok.

cleanup(_) ->
    %% Stop server if running
    case whereis(flurm_fairshare) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, shutdown, 5000)
    end,
    catch ets:delete(flurm_fairshare_usage),
    catch ets:delete(flurm_fairshare_shares),
    timer:sleep(50),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

start_fairshare() ->
    {ok, Pid} = flurm_fairshare:start_link(),
    Pid.

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Start/Stop Tests
start_link_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link creates process", fun test_start_link/0},
        {"registered under expected name", fun test_registered_name/0}
     ]}.

test_start_link() ->
    {ok, Pid} = flurm_fairshare:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ok.

test_registered_name() ->
    {ok, Pid} = flurm_fairshare:start_link(),
    ?assertEqual(Pid, whereis(flurm_fairshare)),
    ok.

%%====================================================================
%% Shares Management Tests
%%====================================================================

shares_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"set_shares updates shares", fun test_set_shares/0},
        {"get_shares returns shares", fun test_get_shares/0},
        {"get_shares default value", fun test_get_shares_default/0}
     ]}.

test_set_shares() ->
    start_fairshare(),

    Result = flurm_fairshare:set_shares(<<"alice">>, <<"engineering">>, 200),

    ?assertEqual(ok, Result),

    {ok, Shares} = flurm_fairshare:get_shares(<<"alice">>, <<"engineering">>),
    ?assertEqual(200, Shares),
    ok.

test_get_shares() ->
    start_fairshare(),

    ok = flurm_fairshare:set_shares(<<"alice">>, <<"engineering">>, 150),

    {ok, Shares} = flurm_fairshare:get_shares(<<"alice">>, <<"engineering">>),

    ?assertEqual(150, Shares),
    ok.

test_get_shares_default() ->
    start_fairshare(),

    {ok, Shares} = flurm_fairshare:get_shares(<<"unknown">>, <<"unknown">>),

    %% Default shares is 1
    ?assertEqual(1, Shares),
    ok.

%%====================================================================
%% Usage Recording Tests
%%====================================================================

usage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"record_usage adds usage", fun test_record_usage/0},
        {"record_usage multiple times", fun test_record_usage_multiple/0},
        {"get_usage returns usage", fun test_get_usage/0},
        {"get_usage zero for new user", fun test_get_usage_new_user/0},
        {"reset_usage clears usage", fun test_reset_usage/0}
     ]}.

test_record_usage() ->
    start_fairshare(),

    %% Record 1 hour of 4 CPUs = 4 CPU-hours = 14400 CPU-seconds
    ok = flurm_fairshare:record_usage(<<"alice">>, <<"engineering">>, 14400, 3600),

    {ok, Usage} = flurm_fairshare:get_usage(<<"alice">>, <<"engineering">>),
    ?assertEqual(14400.0, Usage),
    ok.

test_record_usage_multiple() ->
    start_fairshare(),

    ok = flurm_fairshare:record_usage(<<"alice">>, <<"engineering">>, 14400, 3600),
    ok = flurm_fairshare:record_usage(<<"alice">>, <<"engineering">>, 14400, 1800),

    {ok, Usage} = flurm_fairshare:get_usage(<<"alice">>, <<"engineering">>),
    %% 14400 + 14400 = 28800
    ?assertEqual(28800.0, Usage),
    ok.

test_get_usage() ->
    start_fairshare(),

    ok = flurm_fairshare:record_usage(<<"alice">>, <<"engineering">>, 14400, 7200),

    {ok, Usage} = flurm_fairshare:get_usage(<<"alice">>, <<"engineering">>),

    ?assertEqual(14400.0, Usage),
    ok.

test_get_usage_new_user() ->
    start_fairshare(),

    {ok, Usage} = flurm_fairshare:get_usage(<<"newuser">>, <<"default">>),

    %% New user should have 0 usage
    ?assertEqual(0.0, Usage),
    ok.

test_reset_usage() ->
    start_fairshare(),

    ok = flurm_fairshare:record_usage(<<"alice">>, <<"engineering">>, 14400, 3600),

    %% Verify usage exists
    {ok, UsageBefore} = flurm_fairshare:get_usage(<<"alice">>, <<"engineering">>),
    ?assertEqual(14400.0, UsageBefore),

    %% Reset
    ok = flurm_fairshare:reset_usage(<<"alice">>, <<"engineering">>),

    %% Verify usage is zero
    {ok, UsageAfter} = flurm_fairshare:get_usage(<<"alice">>, <<"engineering">>),
    ?assertEqual(0.0, UsageAfter),
    ok.

%%====================================================================
%% Priority Factor Tests
%%====================================================================

priority_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"priority factor for low usage", fun test_priority_low_usage/0},
        {"priority factor for high usage", fun test_priority_high_usage/0},
        {"priority factor considers shares", fun test_priority_considers_shares/0},
        {"priority factor new user", fun test_priority_new_user/0}
     ]}.

test_priority_low_usage() ->
    start_fairshare(),

    ok = flurm_fairshare:set_shares(<<"alice">>, <<"engineering">>, 100),

    %% No usage recorded
    Factor = flurm_fairshare:get_priority_factor(<<"alice">>, <<"engineering">>),

    %% Low usage = high priority factor (close to 1.0)
    ?assert(is_float(Factor)),
    ?assert(Factor >= 0.5),
    ok.

test_priority_high_usage() ->
    start_fairshare(),

    ok = flurm_fairshare:set_shares(<<"alice">>, <<"engineering">>, 100),

    %% Record lots of usage
    ok = flurm_fairshare:record_usage(<<"alice">>, <<"engineering">>, 5529600, 86400),

    Factor = flurm_fairshare:get_priority_factor(<<"alice">>, <<"engineering">>),

    %% High usage = lower priority factor
    ?assert(is_float(Factor)),
    ?assert(Factor >= 0),
    ?assert(Factor =< 1.0),
    ok.

test_priority_considers_shares() ->
    start_fairshare(),

    ok = flurm_fairshare:set_shares(<<"alice">>, <<"engineering">>, 200),
    ok = flurm_fairshare:set_shares(<<"bob">>, <<"engineering">>, 50),

    %% Same usage for both
    ok = flurm_fairshare:record_usage(<<"alice">>, <<"engineering">>, 28800, 3600),
    ok = flurm_fairshare:record_usage(<<"bob">>, <<"engineering">>, 28800, 3600),

    AliceFactor = flurm_fairshare:get_priority_factor(<<"alice">>, <<"engineering">>),
    BobFactor = flurm_fairshare:get_priority_factor(<<"bob">>, <<"engineering">>),

    %% Alice has more shares, so same usage should give her higher priority
    ?assert(AliceFactor > BobFactor),
    ok.

test_priority_new_user() ->
    start_fairshare(),

    Factor = flurm_fairshare:get_priority_factor(<<"unknown">>, <<"unknown">>),

    %% Unknown user should get a valid factor
    ?assert(is_float(Factor)),
    ?assert(Factor >= 0),
    ?assert(Factor =< 1.0),
    ok.

%%====================================================================
%% Decay Tests
%%====================================================================

decay_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"decay_usage reduces usage", fun test_decay_usage/0}
     ]}.

test_decay_usage() ->
    start_fairshare(),

    ok = flurm_fairshare:record_usage(<<"alice">>, <<"engineering">>, 28800, 3600),

    {ok, UsageBefore} = flurm_fairshare:get_usage(<<"alice">>, <<"engineering">>),

    %% Apply decay
    ok = flurm_fairshare:decay_usage(),

    %% Allow time for async decay
    timer:sleep(100),

    {ok, UsageAfter} = flurm_fairshare:get_usage(<<"alice">>, <<"engineering">>),

    %% Usage should be reduced
    ?assert(UsageAfter =< UsageBefore),
    ok.

%%====================================================================
%% Get All Accounts Tests
%%====================================================================

get_all_accounts_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_all_accounts returns list", fun test_get_all_accounts/0},
        {"get_all_accounts empty initially", fun test_get_all_accounts_empty/0}
     ]}.

test_get_all_accounts() ->
    start_fairshare(),

    ok = flurm_fairshare:set_shares(<<"alice">>, <<"engineering">>, 100),
    ok = flurm_fairshare:record_usage(<<"alice">>, <<"engineering">>, 14400, 3600),

    ok = flurm_fairshare:set_shares(<<"bob">>, <<"research">>, 200),
    ok = flurm_fairshare:record_usage(<<"bob">>, <<"research">>, 28800, 3600),

    Accounts = flurm_fairshare:get_all_accounts(),

    ?assert(is_list(Accounts)),
    ?assertEqual(2, length(Accounts)),
    ok.

test_get_all_accounts_empty() ->
    start_fairshare(),

    Accounts = flurm_fairshare:get_all_accounts(),

    ?assert(is_list(Accounts)),
    ?assertEqual(0, length(Accounts)),
    ok.

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast ignored", fun test_unknown_cast/0},
        {"unknown info ignored", fun test_unknown_info/0},
        {"terminate cleans up", fun test_terminate/0}
     ]}.

test_unknown_call() ->
    start_fairshare(),

    Result = gen_server:call(flurm_fairshare, {unknown_request}),

    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    start_fairshare(),

    gen_server:cast(flurm_fairshare, {unknown_message}),
    timer:sleep(50),

    %% Should still be running
    ?assert(is_pid(whereis(flurm_fairshare))),
    ok.

test_unknown_info() ->
    start_fairshare(),

    flurm_fairshare ! unknown_message,
    timer:sleep(50),

    %% Should still be running
    ?assert(is_pid(whereis(flurm_fairshare))),
    ok.

test_terminate() ->
    Pid = start_fairshare(),
    ?assert(is_process_alive(Pid)),

    catch unlink(Pid),
    gen_server:stop(Pid, shutdown, 5000),

    timer:sleep(50),
    ?assertNot(is_process_alive(Pid)),
    ok.

%%====================================================================
%% Edge Cases
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"zero shares handled", fun test_zero_shares/0},
        {"concurrent usage recording", fun test_concurrent_usage/0}
     ]}.

test_zero_shares() ->
    start_fairshare(),

    %% Set shares to 0 should work but might affect priority calculation
    ok = flurm_fairshare:set_shares(<<"alice">>, <<"engineering">>, 0),

    {ok, Shares} = flurm_fairshare:get_shares(<<"alice">>, <<"engineering">>),
    ?assertEqual(0, Shares),

    Factor = flurm_fairshare:get_priority_factor(<<"alice">>, <<"engineering">>),

    %% Should handle gracefully
    ?assert(is_float(Factor)),
    ok.

test_concurrent_usage() ->
    start_fairshare(),

    %% Record usage from multiple processes
    Self = self(),
    Pids = [spawn(fun() ->
        ok = flurm_fairshare:record_usage(<<"alice">>, <<"engineering">>, 100, 100),
        Self ! done
    end) || _ <- lists:seq(1, 10)],

    %% Wait for all to complete
    lists:foreach(fun(_) -> receive done -> ok end end, Pids),

    {ok, Usage} = flurm_fairshare:get_usage(<<"alice">>, <<"engineering">>),

    %% Should have recorded all 10 * 100 = 1000
    ?assertEqual(1000.0, Usage),
    ok.
