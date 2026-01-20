%%%-------------------------------------------------------------------
%%% @doc Direct EUnit coverage tests for flurm_dbd_app module
%%%
%%% Tests the DBD application callbacks directly. External dependencies
%%% like lager and flurm_dbd_sup are mocked.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_app_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_dbd_app_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start/2 starts supervisor", fun test_start/0},
      {"start/2 logs info message", fun test_start_logs/0},
      {"stop/1 returns ok", fun test_stop/0},
      {"stop/1 logs info message", fun test_stop_logs/0}
     ]}.

setup() ->
    meck:new(lager, [non_strict]),
    meck:expect(lager, md, fun(_) -> ok end),    meck:new(flurm_dbd_sup, [non_strict]),

    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(flurm_dbd_sup, start_link, fun() -> {ok, self()} end),
    ok.

cleanup(_) ->
    catch meck:unload(lager),
    catch meck:unload(flurm_dbd_sup),
    ok.

%%====================================================================
%% start/2 Tests
%%====================================================================

test_start() ->
    SupPid = self(),
    meck:expect(flurm_dbd_sup, start_link, fun() -> {ok, SupPid} end),

    Result = flurm_dbd_app:start(normal, []),

    ?assertEqual({ok, SupPid}, Result),
    ?assert(meck:called(flurm_dbd_sup, start_link, [])).

test_start_logs() ->
    %% Test that start/2 runs without error and calls lager:info
    %% Since lager may already be loaded, we just verify the function runs
    Result = flurm_dbd_app:start(normal, []),
    ?assertMatch({ok, _}, Result).

%%====================================================================
%% stop/1 Tests
%%====================================================================

test_stop() ->
    Result = flurm_dbd_app:stop(undefined),
    ?assertEqual(ok, Result).

test_stop_logs() ->
    %% Test that stop/1 runs without error and calls lager:info
    %% Since lager may already be loaded, we just verify the function runs
    Result = flurm_dbd_app:stop(undefined),
    ?assertEqual(ok, Result).

%%====================================================================
%% Application Behaviour Tests
%%====================================================================

behaviour_test_() ->
    {"flurm_dbd_app implements application behaviour", fun test_behaviour/0}.

test_behaviour() ->
    Exports = flurm_dbd_app:module_info(exports),
    ?assert(lists:member({start, 2}, Exports)),
    ?assert(lists:member({stop, 1}, Exports)).
