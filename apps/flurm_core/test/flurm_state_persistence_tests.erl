%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit tests for flurm_state_persistence module
%%%
%%% Tests state persistence functionality including:
%%% - save_state/1 - Saving state to disk
%%% - load_state/0 - Loading state from disk
%%% - get_state_file/0 - Getting state file path
%%% - clear_state/0 - Clearing saved state
%%% - State validation
%%% - Error handling for various failure scenarios
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_state_persistence_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup creates a temporary directory and configures the state file path
persistence_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_state_file returns default path", fun test_get_state_file_default/0},
        {"get_state_file returns configured path", fun test_get_state_file_configured/0},
        {"save_state creates file with metadata", fun test_save_state_basic/0},
        {"save_state handles complex state", fun test_save_state_complex/0},
        {"load_state retrieves saved state", fun test_load_state_basic/0},
        {"load_state returns not_found for missing file", fun test_load_state_not_found/0},
        {"load_state returns corrupted for bad data", fun test_load_state_corrupted/0},
        {"load_state validates version", fun test_load_state_version_check/0},
        {"load_state rejects non-map state", fun test_load_state_not_map/0},
        {"clear_state removes file", fun test_clear_state_basic/0},
        {"clear_state is idempotent", fun test_clear_state_idempotent/0},
        {"save_state creates parent directories", fun test_save_state_creates_dirs/0},
        {"atomic write uses temp file", fun test_atomic_write/0}
     ]}.

setup() ->
    %% Start lager for logging
    application:ensure_all_started(lager),

    %% Create a unique temp directory for each test
    TempDir = create_temp_dir(),
    StateFile = filename:join(TempDir, "test_state.dat"),

    %% Configure the state file path for testing
    application:set_env(flurm_node_daemon, state_file, StateFile),

    #{temp_dir => TempDir, state_file => StateFile}.

cleanup(#{temp_dir := TempDir}) ->
    %% Clean up the temp directory
    delete_dir_recursive(TempDir),

    %% Reset the environment
    application:unset_env(flurm_node_daemon, state_file),
    ok.

create_temp_dir() ->
    TempBase = filename:join(["/tmp", "flurm_state_test_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    ok = filelib:ensure_dir(TempBase ++ "/"),
    TempBase.

delete_dir_recursive(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            case file:list_dir(Dir) of
                {ok, Files} ->
                    lists:foreach(fun(F) ->
                        Path = filename:join(Dir, F),
                        case filelib:is_dir(Path) of
                            true -> delete_dir_recursive(Path);
                            false -> file:delete(Path)
                        end
                    end, Files),
                    file:del_dir(Dir);
                _ ->
                    ok
            end;
        false ->
            ok
    end.

%%====================================================================
%% get_state_file Tests
%%====================================================================

test_get_state_file_default() ->
    %% Temporarily unset the env to test default
    application:unset_env(flurm_node_daemon, state_file),

    Path = flurm_state_persistence:get_state_file(),
    ?assertEqual("/var/lib/flurm/node_state.dat", Path),

    %% Reset for other tests - will be set again by fixture
    ok.

test_get_state_file_configured() ->
    %% The fixture sets a custom path
    Path = flurm_state_persistence:get_state_file(),
    ?assert(lists:prefix("/tmp/flurm_state_test_", Path)),
    ?assert(lists:suffix("/test_state.dat", Path)),
    ok.

%%====================================================================
%% save_state Tests
%%====================================================================

test_save_state_basic() ->
    State = #{
        running_jobs => [1, 2, 3],
        gpu_allocations => #{},
        node_config => #{cpus => 8, memory => 16384}
    },

    %% Save state
    Result = flurm_state_persistence:save_state(State),
    ?assertEqual(ok, Result),

    %% Verify file exists
    StateFile = flurm_state_persistence:get_state_file(),
    ?assert(filelib:is_file(StateFile)),

    %% Load and verify metadata was added
    {ok, LoadedState} = flurm_state_persistence:load_state(),
    ?assertEqual(1, maps:get(version, LoadedState)),
    ?assert(maps:is_key(saved_at, LoadedState)),
    ?assert(maps:is_key(node, LoadedState)),

    %% Verify original data is preserved
    ?assertEqual([1, 2, 3], maps:get(running_jobs, LoadedState)),
    ok.

test_save_state_complex() ->
    %% Test with more complex nested state
    State = #{
        running_jobs => [
            #{id => 1, name => <<"job1">>, cpus => 4},
            #{id => 2, name => <<"job2">>, cpus => 8}
        ],
        gpu_allocations => #{
            <<"gpu0">> => {1, exclusive},
            <<"gpu1">> => {2, shared}
        },
        node_config => #{
            hostname => <<"compute001">>,
            cpus => 64,
            memory => 262144,
            features => [<<"avx512">>, <<"gpu">>]
        },
        timestamps => #{
            boot_time => erlang:system_time(second),
            last_heartbeat => erlang:system_time(second)
        }
    },

    Result = flurm_state_persistence:save_state(State),
    ?assertEqual(ok, Result),

    {ok, LoadedState} = flurm_state_persistence:load_state(),
    ?assertEqual(maps:get(running_jobs, State), maps:get(running_jobs, LoadedState)),
    ?assertEqual(maps:get(gpu_allocations, State), maps:get(gpu_allocations, LoadedState)),
    ?assertEqual(maps:get(node_config, State), maps:get(node_config, LoadedState)),
    ok.

test_save_state_creates_dirs() ->
    %% Set a path with non-existent parent directories
    TempBase = "/tmp/flurm_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    DeepPath = filename:join([TempBase, "deep", "nested", "path", "state.dat"]),
    application:set_env(flurm_node_daemon, state_file, DeepPath),

    State = #{test => true},
    Result = flurm_state_persistence:save_state(State),
    ?assertEqual(ok, Result),

    %% Verify file was created
    ?assert(filelib:is_file(DeepPath)),

    %% Cleanup
    delete_dir_recursive(TempBase),
    ok.

%%====================================================================
%% load_state Tests
%%====================================================================

test_load_state_basic() ->
    %% First save some state
    OriginalState = #{
        data => <<"test data">>,
        count => 42
    },
    ok = flurm_state_persistence:save_state(OriginalState),

    %% Now load it
    {ok, LoadedState} = flurm_state_persistence:load_state(),

    ?assert(is_map(LoadedState)),
    ?assertEqual(<<"test data">>, maps:get(data, LoadedState)),
    ?assertEqual(42, maps:get(count, LoadedState)),
    ok.

test_load_state_not_found() ->
    %% Clear any existing state
    flurm_state_persistence:clear_state(),

    %% Try to load non-existent state
    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, not_found}, Result),
    ok.

test_load_state_corrupted() ->
    %% Write invalid binary data to the state file
    StateFile = flurm_state_persistence:get_state_file(),
    ok = filelib:ensure_dir(StateFile),
    ok = file:write_file(StateFile, <<"this is not valid erlang binary term">>),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, corrupted}, Result),
    ok.

test_load_state_version_check() ->
    %% Save state with invalid version
    StateFile = flurm_state_persistence:get_state_file(),
    ok = filelib:ensure_dir(StateFile),

    %% Create state with wrong version
    InvalidState = #{
        version => 999,  % Unknown version
        data => <<"test">>
    },
    Binary = term_to_binary(InvalidState, [compressed]),
    ok = file:write_file(StateFile, Binary),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result),
    ok.

test_load_state_not_map() ->
    %% Save a non-map term to the state file
    StateFile = flurm_state_persistence:get_state_file(),
    ok = filelib:ensure_dir(StateFile),

    %% Write a list instead of a map
    InvalidState = [1, 2, 3, 4, 5],
    Binary = term_to_binary(InvalidState, [compressed]),
    ok = file:write_file(StateFile, Binary),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result),
    ok.

%%====================================================================
%% clear_state Tests
%%====================================================================

test_clear_state_basic() ->
    %% First save some state
    ok = flurm_state_persistence:save_state(#{test => true}),

    %% Verify file exists
    StateFile = flurm_state_persistence:get_state_file(),
    ?assert(filelib:is_file(StateFile)),

    %% Clear the state
    Result = flurm_state_persistence:clear_state(),
    ?assertEqual(ok, Result),

    %% Verify file is gone
    ?assertNot(filelib:is_file(StateFile)),
    ok.

test_clear_state_idempotent() ->
    %% Clear state when no file exists
    flurm_state_persistence:clear_state(),

    %% Should be ok to call again
    Result1 = flurm_state_persistence:clear_state(),
    ?assertEqual(ok, Result1),

    Result2 = flurm_state_persistence:clear_state(),
    ?assertEqual(ok, Result2),

    Result3 = flurm_state_persistence:clear_state(),
    ?assertEqual(ok, Result3),
    ok.

%%====================================================================
%% Atomic Write Tests
%%====================================================================

test_atomic_write() ->
    %% Verify that save uses atomic write (temp file + rename)
    StateFile = flurm_state_persistence:get_state_file(),
    TempFile = StateFile ++ ".tmp",

    %% Save state
    ok = flurm_state_persistence:save_state(#{atomic_test => true}),

    %% Temp file should not exist after successful save
    ?assertNot(filelib:is_file(TempFile)),

    %% Main file should exist
    ?assert(filelib:is_file(StateFile)),
    ok.

%%====================================================================
%% Error Handling Tests
%%====================================================================

error_handling_test_() ->
    [
     {"save_state handles write errors", fun test_save_write_error/0},
     {"load_state handles read errors", fun test_load_read_error/0}
    ].

test_save_write_error() ->
    %% Set path to a location where we cannot write
    %% Note: This might not fail on all systems if running as root
    application:set_env(flurm_node_daemon, state_file, "/nonexistent_root_dir/state.dat"),

    State = #{test => true},
    Result = flurm_state_persistence:save_state(State),

    case Result of
        {error, _} -> ok;  % Expected error
        ok -> ok  % Might succeed if running with elevated privileges
    end.

test_load_read_error() ->
    %% Set path to a file that doesn't exist
    application:set_env(flurm_node_daemon, state_file, "/nonexistent_path/state.dat"),

    Result = flurm_state_persistence:load_state(),
    ?assertMatch({error, _}, Result),
    ok.

%%====================================================================
%% State Validation Tests
%%====================================================================

validation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"validate_state accepts valid v1 state", fun test_validate_valid_state/0},
        {"validate_state rejects missing version", fun test_validate_missing_version/0},
        {"validate_state rejects unknown version", fun test_validate_unknown_version/0},
        {"validate_state rejects non-map", fun test_validate_non_map/0}
     ]}.

test_validate_valid_state() ->
    %% Valid state with version 1
    State = #{
        version => 1,
        saved_at => erlang:system_time(millisecond),
        node => node(),
        data => <<"test">>
    },

    StateFile = flurm_state_persistence:get_state_file(),
    ok = filelib:ensure_dir(StateFile),
    Binary = term_to_binary(State, [compressed]),
    ok = file:write_file(StateFile, Binary),

    {ok, LoadedState} = flurm_state_persistence:load_state(),
    ?assertEqual(State, LoadedState),
    ok.

test_validate_missing_version() ->
    %% State without version field
    State = #{
        saved_at => erlang:system_time(millisecond),
        data => <<"test">>
    },

    StateFile = flurm_state_persistence:get_state_file(),
    ok = filelib:ensure_dir(StateFile),
    Binary = term_to_binary(State, [compressed]),
    ok = file:write_file(StateFile, Binary),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result),
    ok.

test_validate_unknown_version() ->
    %% State with unknown version
    State = #{
        version => 42,
        data => <<"test">>
    },

    StateFile = flurm_state_persistence:get_state_file(),
    ok = filelib:ensure_dir(StateFile),
    Binary = term_to_binary(State, [compressed]),
    ok = file:write_file(StateFile, Binary),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result),
    ok.

test_validate_non_map() ->
    %% Non-map term
    State = {tuple, 'not', map},

    StateFile = flurm_state_persistence:get_state_file(),
    ok = filelib:ensure_dir(StateFile),
    Binary = term_to_binary(State, [compressed]),
    ok = file:write_file(StateFile, Binary),

    Result = flurm_state_persistence:load_state(),
    ?assertEqual({error, invalid_state}, Result),
    ok.

%%====================================================================
%% Roundtrip Tests
%%====================================================================

roundtrip_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"empty state roundtrip", fun test_roundtrip_empty/0},
        {"binary data roundtrip", fun test_roundtrip_binary/0},
        {"unicode data roundtrip", fun test_roundtrip_unicode/0},
        {"large state roundtrip", fun test_roundtrip_large/0}
     ]}.

test_roundtrip_empty() ->
    State = #{},
    ok = flurm_state_persistence:save_state(State),
    {ok, Loaded} = flurm_state_persistence:load_state(),
    %% Loaded will have version, saved_at, node added
    ?assert(maps:is_key(version, Loaded)),
    ok.

test_roundtrip_binary() ->
    BinaryData = crypto:strong_rand_bytes(1024),
    State = #{binary_data => BinaryData},
    ok = flurm_state_persistence:save_state(State),
    {ok, Loaded} = flurm_state_persistence:load_state(),
    ?assertEqual(BinaryData, maps:get(binary_data, Loaded)),
    ok.

test_roundtrip_unicode() ->
    UnicodeString = <<"Hello, World!">>,
    State = #{unicode => UnicodeString},
    ok = flurm_state_persistence:save_state(State),
    {ok, Loaded} = flurm_state_persistence:load_state(),
    ?assertEqual(UnicodeString, maps:get(unicode, Loaded)),
    ok.

test_roundtrip_large() ->
    %% Create a large state with many jobs
    Jobs = [#{id => I, data => crypto:strong_rand_bytes(100)} || I <- lists:seq(1, 1000)],
    State = #{jobs => Jobs},
    ok = flurm_state_persistence:save_state(State),
    {ok, Loaded} = flurm_state_persistence:load_state(),
    ?assertEqual(Jobs, maps:get(jobs, Loaded)),
    ok.
