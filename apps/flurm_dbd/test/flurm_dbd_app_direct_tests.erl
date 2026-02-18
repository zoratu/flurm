%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_dbd_app module
%%%
%%% These tests call the actual flurm_dbd_app application callbacks
%%% directly to achieve code coverage. External dependencies like
%%% lager and flurm_dbd_sup are mocked.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_app_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test fixtures
%%====================================================================

flurm_dbd_app_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start/2 starts supervisor", fun test_start/0},
      {"stop/1 logs and returns ok", fun test_stop/0},
      {"start with different start types", fun test_start_types/0}
     ]}.

setup() ->
    %% Ensure meck is not already mocking
    catch meck:unload(lager),
    catch meck:unload(flurm_dbd_sup),

    %% Mock lager for logging
    meck:new(lager, [no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    %% Mock flurm_dbd_sup to avoid starting actual supervisor
    catch meck:unload(flurm_dbd_sup),
    meck:new(flurm_dbd_sup, [passthrough, no_link]),
    meck:expect(flurm_dbd_sup, start_link, fun() -> {ok, self()} end),
    ok.

cleanup(_) ->
    catch meck:unload(flurm_dbd_sup),
    catch meck:unload(lager),
    ok.

%%====================================================================
%% Application Callback Tests
%%====================================================================

test_start() ->
    %% Test start/2 callback
    Result = flurm_dbd_app:start(normal, []),
    ?assertMatch({ok, _Pid}, Result),
    %% Verify supervisor was started
    ?assert(meck:called(flurm_dbd_sup, start_link, [])).

test_stop() ->
    %% Test stop/1 callback
    Result = flurm_dbd_app:stop(undefined),
    ?assertEqual(ok, Result).

test_start_types() ->
    %% Test with different start types (normal, takeover, failover)
    %% They should all work the same way

    %% Normal start
    ?assertMatch({ok, _}, flurm_dbd_app:start(normal, [])),

    %% Takeover
    ?assertMatch({ok, _}, flurm_dbd_app:start({takeover, 'node@host'}, [])),

    %% Failover
    ?assertMatch({ok, _}, flurm_dbd_app:start({failover, 'node@host'}, [])),

    %% With different args (should be ignored)
    ?assertMatch({ok, _}, flurm_dbd_app:start(normal, [some, args])).

%%====================================================================
%% Supervisor Failure Test
%%====================================================================

supervisor_failure_test_() ->
    {foreach,
     fun() ->
         catch meck:unload(lager),
         catch meck:unload(flurm_dbd_sup),

         meck:new(lager, [no_link, non_strict]),
         meck:expect(lager, info, fun(_) -> ok end),
         meck:expect(lager, info, fun(_, _) -> ok end),
         meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

         catch meck:unload(flurm_dbd_sup),
         meck:new(flurm_dbd_sup, [passthrough, no_link]),
         meck:expect(flurm_dbd_sup, start_link, fun() ->
             {error, {shutdown, {failed_to_start_child, flurm_dbd_server, reason}}}
         end),
         ok
     end,
     fun(_) ->
         catch meck:unload(flurm_dbd_sup),
         catch meck:unload(lager),
         ok
     end,
     [
      {"start handles supervisor failure", fun test_supervisor_failure/0}
     ]}.

test_supervisor_failure() ->
    Result = flurm_dbd_app:start(normal, []),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% Stop with State Test
%%====================================================================

stop_with_state_test_() ->
    {foreach,
     fun() ->
         catch meck:unload(lager),
         meck:new(lager, [no_link, non_strict]),
         meck:expect(lager, info, fun(_) -> ok end),
         ok
     end,
     fun(_) ->
         catch meck:unload(lager),
         ok
     end,
     [
      {"stop with various state values", fun test_stop_with_state/0}
     ]}.

test_stop_with_state() ->
    %% stop/1 should always return ok regardless of state
    ?assertEqual(ok, flurm_dbd_app:stop(undefined)),
    ?assertEqual(ok, flurm_dbd_app:stop(#{})),
    ?assertEqual(ok, flurm_dbd_app:stop({some, state})),
    ?assertEqual(ok, flurm_dbd_app:stop([list, state])).
