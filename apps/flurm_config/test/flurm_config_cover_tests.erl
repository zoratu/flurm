%%%-------------------------------------------------------------------
%%% @doc FLURM Config Coverage Integration Tests
%%%
%%% Integration tests designed to call through public APIs to improve
%%% code coverage tracking. Cover only tracks calls from test modules
%%% to the actual module code, so these tests call public functions
%%% that exercise internal code paths.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_config.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

config_cover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get with default returns default", fun test_get_default/0},
        {"set stores value", fun test_set/0},
        {"get retrieves stored value", fun test_get_stored/0},
        {"get with tuple key", fun test_get_tuple_key/0},
        {"set and get multiple values", fun test_multiple_values/0},
        {"load_file loads config", fun test_load_file/0},
        {"load_file nonexistent returns error", fun test_load_nonexistent/0},
        {"reload without config returns error", fun test_reload_no_config/0},
        {"environment variable lookup", fun test_env_lookup/0},
        {"application env lookup", fun test_app_env_lookup/0}
     ]}.

setup() ->
    %% Clean up any existing config table
    catch ets:delete(?CONFIG_TABLE),
    ok.

cleanup(_) ->
    %% Clean up config table
    catch ets:delete(?CONFIG_TABLE),
    ok.

%%====================================================================
%% Coverage Tests - Public API Calls
%%====================================================================

test_get_default() ->
    %% Get non-existent key with default
    Result = flurm_config:get(nonexistent_key, default_value),
    ?assertEqual(default_value, Result),
    %% Get without default returns undefined
    Result2 = flurm_config:get(another_nonexistent),
    ?assertEqual(undefined, Result2),
    ok.

test_set() ->
    %% Set a value
    ok = flurm_config:set(test_key, test_value),
    %% Verify it was stored
    Result = flurm_config:get(test_key),
    ?assertEqual(test_value, Result),
    ok.

test_get_stored() ->
    %% Set and get various types
    ok = flurm_config:set(string_key, <<"hello">>),
    ok = flurm_config:set(int_key, 42),
    ok = flurm_config:set(float_key, 3.14),
    ok = flurm_config:set(atom_key, some_atom),
    ok = flurm_config:set(list_key, [1, 2, 3]),
    ok = flurm_config:set(map_key, #{a => 1}),
    ?assertEqual(<<"hello">>, flurm_config:get(string_key)),
    ?assertEqual(42, flurm_config:get(int_key)),
    ?assertEqual(3.14, flurm_config:get(float_key)),
    ?assertEqual(some_atom, flurm_config:get(atom_key)),
    ?assertEqual([1, 2, 3], flurm_config:get(list_key)),
    ?assertEqual(#{a => 1}, flurm_config:get(map_key)),
    ok.

test_get_tuple_key() ->
    %% Set with tuple key
    ok = flurm_config:set({myapp, setting1}, value1),
    ok = flurm_config:set({myapp, setting2}, value2),
    %% Get with tuple key
    ?assertEqual(value1, flurm_config:get({myapp, setting1})),
    ?assertEqual(value2, flurm_config:get({myapp, setting2})),
    %% Non-existent tuple key
    ?assertEqual(undefined, flurm_config:get({myapp, nonexistent})),
    ?assertEqual(default, flurm_config:get({myapp, nonexistent}, default)),
    ok.

test_multiple_values() ->
    %% Set many values
    lists:foreach(fun(I) ->
        Key = list_to_atom("key_" ++ integer_to_list(I)),
        ok = flurm_config:set(Key, I * 10)
    end, lists:seq(1, 20)),
    %% Verify all values
    lists:foreach(fun(I) ->
        Key = list_to_atom("key_" ++ integer_to_list(I)),
        ?assertEqual(I * 10, flurm_config:get(Key))
    end, lists:seq(1, 20)),
    ok.

test_load_file() ->
    %% Create a temporary config file
    TmpFile = "/tmp/flurm_test_config.config",
    Content = "{test_loaded_key, loaded_value}.\n{another_key, 123}.\n",
    ok = file:write_file(TmpFile, Content),
    %% Load the file
    ok = flurm_config:load_file(TmpFile),
    %% Verify values were loaded
    ?assertEqual(loaded_value, flurm_config:get(test_loaded_key)),
    ?assertEqual(123, flurm_config:get(another_key)),
    %% Clean up
    file:delete(TmpFile),
    ok.

test_load_nonexistent() ->
    %% Load non-existent file returns error
    {error, _Reason} = flurm_config:load_file("/nonexistent/path/config.config"),
    ok.

test_reload_no_config() ->
    %% Reload without config_file set returns error
    {error, no_config_file} = flurm_config:reload(),
    ok.

test_env_lookup() ->
    %% This tests the environment variable lookup path
    %% Set an env var that we can check
    EnvKey = "FLURM_TEST_ENV_KEY",
    os:putenv(EnvKey, "env_value"),
    %% The get function will try env first via key_to_env_name
    %% For atom key 'test_env_key', it becomes FLURM_TEST_ENV_KEY
    Result = flurm_config:get(test_env_key, default),
    ?assertEqual(<<"env_value">>, Result),
    %% Clean up env
    os:unsetenv(EnvKey),
    ok.

test_app_env_lookup() ->
    %% Set application environment
    application:set_env(flurm_config, test_app_key, app_value),
    %% Get should find it via app env
    Result = flurm_config:get(test_app_key, default),
    ?assertEqual(app_value, Result),
    %% Clean up
    application:unset_env(flurm_config, test_app_key),
    ok.

%%====================================================================
%% Additional Coverage Tests - Internal Functions
%%====================================================================

config_internal_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"env value parsing - boolean", fun test_env_parse_boolean/0},
        {"env value parsing - integer", fun test_env_parse_integer/0},
        {"env value parsing - float", fun test_env_parse_float/0},
        {"env value parsing - string", fun test_env_parse_string/0},
        {"priority: env > app > ets", fun test_lookup_priority/0}
     ]}.

test_env_parse_boolean() ->
    %% Test true
    os:putenv("FLURM_BOOL_TRUE", "true"),
    ?assertEqual(true, flurm_config:get(bool_true)),
    os:unsetenv("FLURM_BOOL_TRUE"),
    %% Test false
    os:putenv("FLURM_BOOL_FALSE", "false"),
    ?assertEqual(false, flurm_config:get(bool_false)),
    os:unsetenv("FLURM_BOOL_FALSE"),
    ok.

test_env_parse_integer() ->
    os:putenv("FLURM_INT_VAL", "12345"),
    ?assertEqual(12345, flurm_config:get(int_val)),
    os:unsetenv("FLURM_INT_VAL"),
    %% Negative integer
    os:putenv("FLURM_NEG_INT", "-42"),
    ?assertEqual(-42, flurm_config:get(neg_int)),
    os:unsetenv("FLURM_NEG_INT"),
    ok.

test_env_parse_float() ->
    os:putenv("FLURM_FLOAT_VAL", "3.14159"),
    ?assertEqual(3.14159, flurm_config:get(float_val)),
    os:unsetenv("FLURM_FLOAT_VAL"),
    %% Negative float
    os:putenv("FLURM_NEG_FLOAT", "-2.718"),
    ?assertEqual(-2.718, flurm_config:get(neg_float)),
    os:unsetenv("FLURM_NEG_FLOAT"),
    ok.

test_env_parse_string() ->
    os:putenv("FLURM_STR_VAL", "hello_world"),
    %% Non-numeric, non-boolean string becomes binary
    ?assertEqual(<<"hello_world">>, flurm_config:get(str_val)),
    os:unsetenv("FLURM_STR_VAL"),
    ok.

test_lookup_priority() ->
    %% Test that env takes priority over app env over ets
    TestKey = priority_test_key,

    %% Set in ETS
    ok = flurm_config:set(TestKey, ets_value),
    ?assertEqual(ets_value, flurm_config:get(TestKey)),

    %% Set in app env - should take priority
    application:set_env(flurm_config, TestKey, app_value),
    ?assertEqual(app_value, flurm_config:get(TestKey)),

    %% Set in env - should take priority over app
    EnvName = "FLURM_CONFIG_PRIORITY_TEST_KEY",
    os:putenv(EnvName, "env_value"),
    ?assertEqual(<<"env_value">>, flurm_config:get({flurm_config, TestKey})),

    %% Clean up
    os:unsetenv(EnvName),
    application:unset_env(flurm_config, TestKey),
    ok.

%%====================================================================
%% Tuple Key Environment Variable Tests
%%====================================================================

config_tuple_key_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"tuple key to env name", fun test_tuple_key_env_name/0},
        {"tuple key to app key", fun test_tuple_key_app_key/0}
     ]}.

test_tuple_key_env_name() ->
    %% Tuple key {app, setting} becomes APP_SETTING
    os:putenv("MYAPP_MYSETTING", "tuple_env_value"),
    Result = flurm_config:get({myapp, mysetting}, default),
    ?assertEqual(<<"tuple_env_value">>, Result),
    os:unsetenv("MYAPP_MYSETTING"),
    ok.

test_tuple_key_app_key() ->
    %% Tuple key {app, setting} looks up app:setting in application env
    application:set_env(testapp, testsetting, tuple_app_value),
    Result = flurm_config:get({testapp, testsetting}, default),
    ?assertEqual(tuple_app_value, Result),
    application:unset_env(testapp, testsetting),
    ok.
