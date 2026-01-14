%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_state_persistence module
%%%
%%% Tests the state persistence functions directly without mocking
%%% the module itself. Only external dependencies are mocked if needed.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_state_persistence_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

state_persistence_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_state_file returns configured path", fun test_get_state_file_configured/0},
      {"get_state_file returns default when not configured", fun test_get_state_file_default/0},
      {"save_state creates state file with metadata", fun test_save_state_success/0},
      {"load_state reads back saved state", fun test_load_state_success/0},
      {"load_state returns error for non-existent file", fun test_load_state_not_found/0},
      {"load_state handles corrupted files", fun test_load_state_corrupted/0},
      {"load_state validates state version", fun test_load_state_invalid_version/0},
      {"load_state rejects non-map state", fun test_load_state_not_map/0},
      {"clear_state removes state file", fun test_clear_state_success/0},
      {"clear_state succeeds when file missing", fun test_clear_state_not_found/0},
      {"save_state handles ensure_dir failure", fun test_save_state_ensure_dir_failure/0}
     ]}.

setup() ->
    %% Use a temp directory for tests
    TestDir = "/tmp/flurm_state_persistence_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    ok = filelib:ensure_dir(TestDir ++ "/"),
    file:make_dir(TestDir),
    TestStateFile = TestDir ++ "/node_state.dat",
    application:set_env(flurm_node_daemon, state_file, TestStateFile),
    %% Start lager for tests
    catch application:start(lager),
    #{test_dir => TestDir, state_file => TestStateFile}.

cleanup(#{test_dir := TestDir}) ->
    %% Clean up test directory
    os:cmd("rm -rf " ++ TestDir),
    application:unset_env(flurm_node_daemon, state_file),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_get_state_file_configured() ->
    %% The state file should be configured from setup
    StateFile = flurm_state_persistence:get_state_file(),
    ?assert(is_list(StateFile)),
    ?assertMatch("/tmp/flurm_state_persistence_test_" ++ _, StateFile).

test_get_state_file_default() ->
    %% Temporarily unset the env to test default
    {ok, Original} = application:get_env(flurm_node_daemon, state_file),
    application:unset_env(flurm_node_daemon, state_file),

    StateFile = flurm_state_persistence:get_state_file(),
    ?assertEqual("/var/lib/flurm/node_state.dat", StateFile),

    %% Restore original
    application:set_env(flurm_node_daemon, state_file, Original).

test_save_state_success() ->
    State = #{
        running_jobs => 5,
        gpu_allocation => #{0 => 1001, 1 => 1002},
        draining => false,
        drain_reason => undefined
    },

    Result = flurm_state_persistence:save_state(State),
    ?assertEqual(ok, Result),

    %% Verify file exists
    StateFile = flurm_state_persistence:get_state_file(),
    ?assert(filelib:is_file(StateFile)).

test_load_state_success() ->
    %% First save a state
    OriginalState = #{
        running_jobs => 3,
        gpu_allocation => #{0 => 2001},
        draining => true,
        drain_reason => <<"maintenance">>
    },
    ok = flurm_state_persistence:save_state(OriginalState),

    %% Now load it back
    {ok, LoadedState} = flurm_state_persistence:load_state(),

    %% Verify the original fields are preserved
    ?assertEqual(3, maps:get(running_jobs, LoadedState)),
    ?assertEqual(#{0 => 2001}, maps:get(gpu_allocation, LoadedState)),
    ?assertEqual(true, maps:get(draining, LoadedState)),

    %% Verify metadata was added
    ?assertEqual(1, maps:get(version, LoadedState)),
    ?assert(is_integer(maps:get(saved_at, LoadedState))),
    ?assert(is_atom(maps:get(node, LoadedState))).

test_load_state_not_found() ->
    %% Clear any existing state first
    flurm_state_persistence:clear_state(),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, not_found}, Result).

test_load_state_corrupted() ->
    %% Write garbage to the state file
    StateFile = flurm_state_persistence:get_state_file(),
    ok = file:write_file(StateFile, <<"not a valid erlang term">>),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, corrupted}, Result).

test_load_state_invalid_version() ->
    %% Write a state with unknown version
    StateFile = flurm_state_persistence:get_state_file(),
    InvalidState = #{
        version => 999,
        running_jobs => 1
    },
    Binary = term_to_binary(InvalidState),
    ok = file:write_file(StateFile, Binary),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result).

test_load_state_not_map() ->
    %% Write a non-map term to state file
    StateFile = flurm_state_persistence:get_state_file(),
    Binary = term_to_binary({tuple, not_a_map}),
    ok = file:write_file(StateFile, Binary),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result).

test_clear_state_success() ->
    %% First save a state
    ok = flurm_state_persistence:save_state(#{running_jobs => 1}),

    %% Verify it exists
    StateFile = flurm_state_persistence:get_state_file(),
    ?assert(filelib:is_file(StateFile)),

    %% Clear it
    Result = flurm_state_persistence:clear_state(),
    ?assertEqual(ok, Result),

    %% Verify it's gone
    ?assertNot(filelib:is_file(StateFile)).

test_clear_state_not_found() ->
    %% Make sure file doesn't exist
    StateFile = flurm_state_persistence:get_state_file(),
    file:delete(StateFile),

    %% Should succeed even if file doesn't exist
    Result = flurm_state_persistence:clear_state(),
    ?assertEqual(ok, Result).

test_save_state_ensure_dir_failure() ->
    %% Save original state file path
    {ok, OriginalPath} = application:get_env(flurm_node_daemon, state_file),

    %% Set state file to invalid path (permission denied type situation)
    %% Use a path that can't be created
    application:set_env(flurm_node_daemon, state_file, "/root/nonexistent_dir/state.dat"),

    State = #{running_jobs => 1},
    Result = flurm_state_persistence:save_state(State),

    %% Should return error
    ?assertMatch({error, _}, Result),

    %% Restore original path
    application:set_env(flurm_node_daemon, state_file, OriginalPath).

%%====================================================================
%% Additional coverage tests
%%====================================================================

validate_state_coverage_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"state with missing version field", fun test_state_missing_version/0}
     ]}.

test_state_missing_version() ->
    %% Write a state without version field
    StateFile = flurm_state_persistence:get_state_file(),
    StateNoVersion = #{
        running_jobs => 5,
        saved_at => erlang:system_time(millisecond)
    },
    Binary = term_to_binary(StateNoVersion),
    ok = file:write_file(StateFile, Binary),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result).
