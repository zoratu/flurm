%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_config module
%%%
%%% These tests exercise the flurm_config module without using meck.
%%% Tests focus on:
%%% - ETS-based configuration storage (get/set)
%%% - Environment variable parsing
%%% - File loading
%%% - Key conversion functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_config.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup: ensure clean ETS table state
setup() ->
    %% Clean up any existing table
    case ets:whereis(?CONFIG_TABLE) of
        undefined -> ok;
        _ -> ets:delete(?CONFIG_TABLE)
    end,
    %% Clean up any test environment variables
    os:unsetenv("FLURM_TEST_KEY"),
    os:unsetenv("FLURM_CONFIG_SOME_KEY"),
    os:unsetenv("FLURM_MYAPP_MYKEY"),
    ok.

cleanup(_) ->
    %% Clean up ETS table
    case ets:whereis(?CONFIG_TABLE) of
        undefined -> ok;
        _ -> ets:delete(?CONFIG_TABLE)
    end,
    %% Clean up environment variables
    os:unsetenv("FLURM_TEST_KEY"),
    os:unsetenv("FLURM_CONFIG_SOME_KEY"),
    os:unsetenv("FLURM_MYAPP_MYKEY"),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

flurm_config_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get with default when key not found", fun get_with_default_not_found/0},
      {"set and get value from ETS", fun set_and_get_from_ets/0},
      {"set overwrites existing value", fun set_overwrites_existing/0},
      {"get from environment variable - string", fun get_from_env_string/0},
      {"get from environment variable - integer", fun get_from_env_integer/0},
      {"get from environment variable - float", fun get_from_env_float/0},
      {"get from environment variable - boolean true", fun get_from_env_bool_true/0},
      {"get from environment variable - boolean false", fun get_from_env_bool_false/0},
      {"get from app env", fun get_from_app_env/0},
      {"get priority: env > app_env > ets > default", fun get_priority/0},
      {"get/1 returns value or undefined", fun get_1_returns_undefined/0},
      {"load_file success", fun load_file_success/0},
      {"load_file error", fun load_file_error/0},
      {"reload with no config file", fun reload_no_config_file/0},
      {"reload with config file", fun reload_with_config_file/0},
      {"tuple key to env name", fun tuple_key_to_env_name/0},
      {"atom key to env name", fun atom_key_to_env_name/0},
      {"ensure_table creates table", fun ensure_table_creates/0},
      {"ensure_table idempotent", fun ensure_table_idempotent/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

get_with_default_not_found() ->
    %% When key doesn't exist anywhere, default is returned
    Result = flurm_config:get(nonexistent_key, my_default),
    ?assertEqual(my_default, Result).

set_and_get_from_ets() ->
    %% Set a value and retrieve it
    ok = flurm_config:set(my_key, my_value),
    Result = flurm_config:get(my_key, undefined),
    ?assertEqual(my_value, Result).

set_overwrites_existing() ->
    %% Set a value twice, second should overwrite
    ok = flurm_config:set(overwrite_key, first_value),
    ok = flurm_config:set(overwrite_key, second_value),
    Result = flurm_config:get(overwrite_key, undefined),
    ?assertEqual(second_value, Result).

get_from_env_string() ->
    %% Set environment variable and retrieve it
    os:putenv("FLURM_TEST_KEY", "test_string_value"),
    Result = flurm_config:get(test_key, undefined),
    ?assertEqual(<<"test_string_value">>, Result).

get_from_env_integer() ->
    %% Integer parsing from environment
    os:putenv("FLURM_TEST_KEY", "12345"),
    Result = flurm_config:get(test_key, undefined),
    ?assertEqual(12345, Result).

get_from_env_float() ->
    %% Float parsing from environment
    os:putenv("FLURM_TEST_KEY", "3.14159"),
    Result = flurm_config:get(test_key, undefined),
    ?assertEqual(3.14159, Result).

get_from_env_bool_true() ->
    %% Boolean true parsing from environment
    os:putenv("FLURM_TEST_KEY", "true"),
    Result = flurm_config:get(test_key, undefined),
    ?assertEqual(true, Result).

get_from_env_bool_false() ->
    %% Boolean false parsing from environment
    os:putenv("FLURM_TEST_KEY", "false"),
    Result = flurm_config:get(test_key, undefined),
    ?assertEqual(false, Result).

get_from_app_env() ->
    %% Set application environment and retrieve it
    application:set_env(flurm_config, some_key, some_app_value),
    Result = flurm_config:get(some_key, undefined),
    ?assertEqual(some_app_value, Result),
    %% Cleanup
    application:unset_env(flurm_config, some_key).

get_priority() ->
    %% Test priority: env > app_env > ets > default

    %% 1. Only default
    ?assertEqual(default_val, flurm_config:get(priority_test_key, default_val)),

    %% 2. ETS value set
    ok = flurm_config:set(priority_test_key, ets_value),
    ?assertEqual(ets_value, flurm_config:get(priority_test_key, default_val)),

    %% 3. App env takes priority over ETS
    application:set_env(flurm_config, priority_test_key, app_env_value),
    ?assertEqual(app_env_value, flurm_config:get(priority_test_key, default_val)),

    %% 4. Environment variable takes priority over all
    os:putenv("FLURM_PRIORITY_TEST_KEY", "env_value"),
    ?assertEqual(<<"env_value">>, flurm_config:get(priority_test_key, default_val)),

    %% Cleanup
    application:unset_env(flurm_config, priority_test_key),
    os:unsetenv("FLURM_PRIORITY_TEST_KEY").

get_1_returns_undefined() ->
    %% get/1 should return undefined when key not found
    Result = flurm_config:get(totally_nonexistent_key),
    ?assertEqual(undefined, Result).

load_file_success() ->
    %% Create a temporary config file
    TempFile = "/tmp/flurm_test_config_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "{file_key1, value1}.\n{file_key2, 42}.\n",
    ok = file:write_file(TempFile, Content),

    %% Load the file
    Result = flurm_config:load_file(TempFile),
    ?assertEqual(ok, Result),

    %% Verify values were loaded
    ?assertEqual(value1, flurm_config:get(file_key1, undefined)),
    ?assertEqual(42, flurm_config:get(file_key2, undefined)),

    %% Cleanup
    file:delete(TempFile).

load_file_error() ->
    %% Try to load non-existent file
    Result = flurm_config:load_file("/tmp/nonexistent_file_12345.config"),
    ?assertMatch({error, _}, Result).

reload_no_config_file() ->
    %% When no config_file is set in application env, reload should fail
    application:unset_env(flurm_config, config_file),
    Result = flurm_config:reload(),
    ?assertEqual({error, no_config_file}, Result).

reload_with_config_file() ->
    %% Create a temporary config file
    TempFile = "/tmp/flurm_reload_test_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".config",
    Content = "{reload_key, reload_value}.\n",
    ok = file:write_file(TempFile, Content),

    %% Set config_file in application env
    application:set_env(flurm_config, config_file, TempFile),

    %% Reload should now work
    Result = flurm_config:reload(),
    ?assertEqual(ok, Result),

    %% Verify value was loaded
    ?assertEqual(reload_value, flurm_config:get(reload_key, undefined)),

    %% Cleanup
    application:unset_env(flurm_config, config_file),
    file:delete(TempFile).

tuple_key_to_env_name() ->
    %% Test tuple key environment variable lookup
    os:putenv("MYAPP_MYKEY", "tuple_env_value"),
    Result = flurm_config:get({myapp, mykey}, undefined),
    ?assertEqual(<<"tuple_env_value">>, Result),
    os:unsetenv("MYAPP_MYKEY").

atom_key_to_env_name() ->
    %% Test atom key environment variable lookup (uses FLURM_ prefix)
    os:putenv("FLURM_SIMPLE_KEY", "simple_env_value"),
    Result = flurm_config:get(simple_key, undefined),
    ?assertEqual(<<"simple_env_value">>, Result),
    os:unsetenv("FLURM_SIMPLE_KEY").

ensure_table_creates() ->
    %% Table should be created when set is called
    ?assertEqual(undefined, ets:whereis(?CONFIG_TABLE)),
    ok = flurm_config:set(create_table_key, value),
    ?assertNotEqual(undefined, ets:whereis(?CONFIG_TABLE)).

ensure_table_idempotent() ->
    %% Multiple set calls should not fail (table already exists)
    ok = flurm_config:set(key1, val1),
    ok = flurm_config:set(key2, val2),
    ok = flurm_config:set(key3, val3),
    ?assertEqual(val1, flurm_config:get(key1, undefined)),
    ?assertEqual(val2, flurm_config:get(key2, undefined)),
    ?assertEqual(val3, flurm_config:get(key3, undefined)).

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

parse_env_value_edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"negative integer from env", fun negative_integer_from_env/0},
      {"zero from env", fun zero_from_env/0},
      {"negative float from env", fun negative_float_from_env/0},
      {"empty string from env", fun empty_string_from_env/0}
     ]}.

negative_integer_from_env() ->
    os:putenv("FLURM_NEG_INT", "-42"),
    Result = flurm_config:get(neg_int, undefined),
    ?assertEqual(-42, Result),
    os:unsetenv("FLURM_NEG_INT").

zero_from_env() ->
    os:putenv("FLURM_ZERO_VAL", "0"),
    Result = flurm_config:get(zero_val, undefined),
    ?assertEqual(0, Result),
    os:unsetenv("FLURM_ZERO_VAL").

negative_float_from_env() ->
    os:putenv("FLURM_NEG_FLOAT", "-3.14"),
    Result = flurm_config:get(neg_float, undefined),
    ?assertEqual(-3.14, Result),
    os:unsetenv("FLURM_NEG_FLOAT").

empty_string_from_env() ->
    %% Empty environment variable should return as empty binary
    os:putenv("FLURM_EMPTY_STR", ""),
    Result = flurm_config:get(empty_str, default_empty),
    %% On macOS/BSD, os:getenv returns "" for empty strings (not false)
    %% which gets parsed as <<>> binary
    ?assertEqual(<<>>, Result),
    os:unsetenv("FLURM_EMPTY_STR").

%%====================================================================
%% Multiple Value Types Tests
%%====================================================================

multiple_types_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"store and retrieve list", fun store_list/0},
      {"store and retrieve map", fun store_map/0},
      {"store and retrieve tuple", fun store_tuple/0},
      {"store and retrieve binary", fun store_binary/0},
      {"store and retrieve pid", fun store_pid/0}
     ]}.

store_list() ->
    ok = flurm_config:set(list_key, [1, 2, 3, 4, 5]),
    ?assertEqual([1, 2, 3, 4, 5], flurm_config:get(list_key, [])).

store_map() ->
    TestMap = #{a => 1, b => 2},
    ok = flurm_config:set(map_key, TestMap),
    ?assertEqual(TestMap, flurm_config:get(map_key, #{})).

store_tuple() ->
    TestTuple = {node, "host1", 8080},
    ok = flurm_config:set(tuple_key, TestTuple),
    ?assertEqual(TestTuple, flurm_config:get(tuple_key, undefined)).

store_binary() ->
    ok = flurm_config:set(binary_key, <<"binary value">>),
    ?assertEqual(<<"binary value">>, flurm_config:get(binary_key, <<>>)).

store_pid() ->
    TestPid = self(),
    ok = flurm_config:set(pid_key, TestPid),
    ?assertEqual(TestPid, flurm_config:get(pid_key, undefined)).

%%====================================================================
%% Tuple Key Tests
%%====================================================================

tuple_key_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"tuple key in ETS", fun tuple_key_ets/0},
      {"tuple key in app env", fun tuple_key_app_env/0}
     ]}.

tuple_key_ets() ->
    ok = flurm_config:set({myapp, myoption}, tuple_ets_value),
    Result = flurm_config:get({myapp, myoption}, undefined),
    ?assertEqual(tuple_ets_value, Result).

tuple_key_app_env() ->
    %% Set in specific app's env
    application:set_env(testapp, testoption, app_specific_value),
    Result = flurm_config:get({testapp, testoption}, undefined),
    ?assertEqual(app_specific_value, Result),
    application:unset_env(testapp, testoption).
