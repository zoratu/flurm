%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_dbd_app
%%%
%%% Tests the application module's pure functions without mocking.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_app_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Stop Callback Test
%%====================================================================

stop_callback_test() ->
    %% stop/1 should return ok for any state
    ?assertEqual(ok, flurm_dbd_app:stop(undefined)),
    ?assertEqual(ok, flurm_dbd_app:stop(some_state)),
    ?assertEqual(ok, flurm_dbd_app:stop([])).

%%====================================================================
%% Module Attributes Tests
%%====================================================================

module_exports_test() ->
    %% Verify the module exports the required application callbacks
    Exports = flurm_dbd_app:module_info(exports),

    ?assert(lists:member({start, 2}, Exports)),
    ?assert(lists:member({stop, 1}, Exports)).

behaviour_test() ->
    %% Verify the module implements application behaviour
    Attributes = flurm_dbd_app:module_info(attributes),
    Behaviours = proplists:get_value(behaviour, Attributes, []),
    ?assert(lists:member(application, Behaviours)).
