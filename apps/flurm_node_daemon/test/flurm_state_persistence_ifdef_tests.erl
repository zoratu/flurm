%%%-------------------------------------------------------------------
%%% @doc Test suite for flurm_state_persistence internal functions
%%%
%%% These tests exercise the internal helper functions exported via
%%% -ifdef(TEST) to achieve code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_state_persistence_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup
%%====================================================================

setup() ->
    application:ensure_all_started(lager),
    %% Use a temp file for testing
    TestStateFile = "/tmp/flurm_test_state_" ++
                    integer_to_list(erlang:unique_integer([positive])) ++ ".dat",
    application:set_env(flurm_node_daemon, state_file, TestStateFile),
    TestStateFile.

cleanup(TestStateFile) ->
    file:delete(TestStateFile),
    file:delete(TestStateFile ++ ".tmp"),
    application:unset_env(flurm_node_daemon, state_file),
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

state_persistence_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun validate_state_valid_test/1,
      fun validate_state_missing_version_test/1,
      fun validate_state_unknown_version_test/1,
      fun validate_state_not_map_test/1,
      fun get_state_file_test/1,
      fun save_and_load_state_test/1,
      fun clear_state_test/1,
      fun load_state_not_found_test/1
     ]}.

%%====================================================================
%% Tests
%%====================================================================

validate_state_valid_test(_TestStateFile) ->
    fun() ->
        State = #{version => 1, data => some_data},
        Result = flurm_state_persistence:validate_state(State),
        ?assertEqual(ok, Result)
    end.

validate_state_missing_version_test(_TestStateFile) ->
    fun() ->
        State = #{data => some_data},
        Result = flurm_state_persistence:validate_state(State),
        ?assertEqual({error, missing_version}, Result)
    end.

validate_state_unknown_version_test(_TestStateFile) ->
    fun() ->
        State = #{version => 99, data => some_data},
        Result = flurm_state_persistence:validate_state(State),
        ?assertEqual({error, {unknown_version, 99}}, Result)
    end.

validate_state_not_map_test(_TestStateFile) ->
    fun() ->
        NotAMap = [1, 2, 3],
        Result = flurm_state_persistence:validate_state(NotAMap),
        ?assertEqual({error, not_a_map}, Result)
    end.

get_state_file_test(TestStateFile) ->
    fun() ->
        Result = flurm_state_persistence:get_state_file(),
        ?assertEqual(TestStateFile, Result)
    end.

save_and_load_state_test(_TestStateFile) ->
    fun() ->
        %% Save state
        OrigState = #{
            running_jobs => 5,
            gpu_allocation => [0, 1],
            custom_data => <<"test">>
        },

        SaveResult = flurm_state_persistence:save_state(OrigState),
        ?assertEqual(ok, SaveResult),

        %% Load state
        {ok, LoadedState} = flurm_state_persistence:load_state(),

        %% Verify loaded state contains original data
        ?assertEqual(5, maps:get(running_jobs, LoadedState)),
        ?assertEqual([0, 1], maps:get(gpu_allocation, LoadedState)),
        ?assertEqual(<<"test">>, maps:get(custom_data, LoadedState)),

        %% Should also have metadata
        ?assertEqual(1, maps:get(version, LoadedState)),
        ?assert(is_integer(maps:get(saved_at, LoadedState)))
    end.

clear_state_test(TestStateFile) ->
    fun() ->
        %% Save state first
        flurm_state_persistence:save_state(#{test => true}),
        ?assert(filelib:is_regular(TestStateFile)),

        %% Clear state
        Result = flurm_state_persistence:clear_state(),
        ?assertEqual(ok, Result),

        %% File should be gone
        ?assertNot(filelib:is_regular(TestStateFile))
    end.

load_state_not_found_test(_TestStateFile) ->
    fun() ->
        %% Ensure no state file exists
        flurm_state_persistence:clear_state(),

        Result = flurm_state_persistence:load_state(),
        ?assertEqual({error, not_found}, Result)
    end.
