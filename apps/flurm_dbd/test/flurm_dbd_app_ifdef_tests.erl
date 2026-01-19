%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_dbd_app module
%%% Tests the application callbacks directly
%%%-------------------------------------------------------------------
-module(flurm_dbd_app_ifdef_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Cases for stop/1
%%====================================================================

stop_test_() ->
    [
     {"stop/1 returns ok", fun() ->
         %% stop/1 should always return ok regardless of state
         ?assertEqual(ok, flurm_dbd_app:stop(undefined)),
         ?assertEqual(ok, flurm_dbd_app:stop([])),
         ?assertEqual(ok, flurm_dbd_app:stop(some_state))
     end}
    ].

%%====================================================================
%% Module attribute tests
%%====================================================================

module_info_test_() ->
    [
     {"module exports application callbacks", fun() ->
         Exports = flurm_dbd_app:module_info(exports),
         ?assert(lists:member({start, 2}, Exports)),
         ?assert(lists:member({stop, 1}, Exports))
     end},
     {"module has application behaviour", fun() ->
         %% Check if the module compiles with application behaviour
         Attrs = flurm_dbd_app:module_info(attributes),
         Behaviours = proplists:get_value(behaviour, Attrs, []),
         ?assert(lists:member(application, Behaviours))
     end}
    ].

%%====================================================================
%% Note on start/2 testing
%%====================================================================
%% start/2 requires the full application environment and supervisor
%% dependencies. It is tested in flurm_dbd_app_tests.erl and
%% flurm_dbd_app_pure_tests.erl with proper setup/teardown.
