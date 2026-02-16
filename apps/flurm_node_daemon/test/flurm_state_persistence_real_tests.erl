%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_state_persistence module
%%%
%%% Tests state persistence functions with actual file operations
%%% using temporary files.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_state_persistence_real_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

state_persistence_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_state_file returns default when not configured", fun test_get_state_file_default/0},
      {"get_state_file returns configured path", fun test_get_state_file_configured/0},
      {"save_state and load_state round trip", fun test_save_load_roundtrip/0},
      {"load_state returns not_found for missing file", fun test_load_missing_file/0},
      {"clear_state removes file", fun test_clear_state/0},
      {"clear_state ok when file doesn't exist", fun test_clear_state_missing/0},
      {"validate_state accepts valid state", fun test_validate_state_valid/0},
      {"validate_state rejects missing version", fun test_validate_state_no_version/0},
      {"validate_state rejects unknown version", fun test_validate_state_unknown_version/0},
      {"validate_state rejects non-map", fun test_validate_state_non_map/0},
      {"save_state handles directory creation", fun test_save_creates_directory/0}
     ]}.

setup() ->
    %% Create a unique temp directory for each test
    TempDir = "/tmp/flurm_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    file:make_dir(TempDir),
    %% Set the state file path in application env
    application:set_env(flurm_node_daemon, state_file, TempDir ++ "/test_state.dat"),
    TempDir.

cleanup(TempDir) ->
    %% Clean up temp files and directory
    file:delete(TempDir ++ "/test_state.dat"),
    file:delete(TempDir ++ "/test_state.dat.tmp"),
    file:del_dir(TempDir),
    application:unset_env(flurm_node_daemon, state_file),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_get_state_file_default() ->
    application:unset_env(flurm_node_daemon, state_file),
    Path = flurm_state_persistence:get_state_file(),
    ?assertEqual("/var/lib/flurm/node_state.dat", Path).

test_get_state_file_configured() ->
    Path = flurm_state_persistence:get_state_file(),
    ?assertMatch("/tmp/flurm_test_" ++ _, Path).

test_save_load_roundtrip() ->
    State = #{
        version => 1,
        running_jobs => [1, 2, 3],
        gpu_allocations => #{0 => 1, 1 => 2}
    },
    ?assertEqual(ok, flurm_state_persistence:save_state(State)),
    {ok, Loaded} = flurm_state_persistence:load_state(),
    ?assertEqual(1, maps:get(version, Loaded)),
    ?assertEqual([1, 2, 3], maps:get(running_jobs, Loaded)),
    ?assertEqual(#{0 => 1, 1 => 2}, maps:get(gpu_allocations, Loaded)),
    %% Check metadata was added
    ?assert(maps:is_key(saved_at, Loaded)),
    ?assert(maps:is_key(node, Loaded)).

test_load_missing_file() ->
    %% Clear any existing state
    flurm_state_persistence:clear_state(),
    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, not_found}, Result).

test_clear_state() ->
    %% First save some state
    State = #{version => 1, data => test},
    ok = flurm_state_persistence:save_state(State),
    %% Verify it exists
    {ok, _} = flurm_state_persistence:load_state(),
    %% Clear it
    ?assertEqual(ok, flurm_state_persistence:clear_state()),
    %% Verify it's gone
    ?assertEqual({error, not_found}, flurm_state_persistence:load_state()).

test_clear_state_missing() ->
    %% Clear any existing state first
    flurm_state_persistence:clear_state(),
    %% Clearing again should be ok
    ?assertEqual(ok, flurm_state_persistence:clear_state()).

test_validate_state_valid() ->
    State = #{version => 1, data => test},
    Result = flurm_state_persistence:validate_state(State),
    ?assertEqual(ok, Result).

test_validate_state_no_version() ->
    State = #{data => test},
    Result = flurm_state_persistence:validate_state(State),
    ?assertEqual({error, missing_version}, Result).

test_validate_state_unknown_version() ->
    State = #{version => 999, data => test},
    Result = flurm_state_persistence:validate_state(State),
    ?assertEqual({error, {unknown_version, 999}}, Result).

test_validate_state_non_map() ->
    Result = flurm_state_persistence:validate_state(not_a_map),
    ?assertEqual({error, not_a_map}, Result).

test_save_creates_directory() ->
    %% Set path to a nested directory that doesn't exist
    TempDir = "/tmp/flurm_nested_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    NestedPath = TempDir ++ "/subdir/state.dat",
    application:set_env(flurm_node_daemon, state_file, NestedPath),

    State = #{version => 1, test => nested_dir},
    Result = flurm_state_persistence:save_state(State),
    ?assertEqual(ok, Result),

    %% Verify file was created
    ?assert(filelib:is_regular(NestedPath)),

    %% Cleanup
    file:delete(NestedPath),
    file:del_dir(TempDir ++ "/subdir"),
    file:del_dir(TempDir).
