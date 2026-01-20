%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_config module
%%% Tests configuration management via ETS and environment
%%%-------------------------------------------------------------------
-module(flurm_config_tests).
-include_lib("eunit/include/eunit.hrl").

-define(CONFIG_TABLE, flurm_config_table).

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Clean up any existing ETS table
    catch ets:delete(?CONFIG_TABLE),
    %% Clean up any test env vars
    os:unsetenv("FLURM_TEST_KEY"),
    os:unsetenv("FLURM_CORE_SOME_KEY"),
    ok.

cleanup(_) ->
    catch ets:delete(?CONFIG_TABLE),
    os:unsetenv("FLURM_TEST_KEY"),
    os:unsetenv("FLURM_CORE_SOME_KEY"),
    ok.

%%====================================================================
%% Test Generator
%%====================================================================

config_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Get with default - key not found", fun test_get_default/0},
      {"Get without default - undefined", fun test_get_no_default/0},
      {"Set and get value", fun test_set_get/0},
      {"Get from environment variable - atom key", fun test_get_from_env_atom/0},
      {"Get from environment variable - tuple key", fun test_get_from_env_tuple/0},
      {"Load file - success", fun test_load_file_success/0},
      {"Load file - error", fun test_load_file_error/0},
      {"Reload - no config file", fun test_reload_no_file/0},
      {"Reload - from config file", fun test_reload_from_file/0},
      {"Parse env value - boolean true", fun test_parse_env_true/0},
      {"Parse env value - boolean false", fun test_parse_env_false/0},
      {"Parse env value - integer", fun test_parse_env_integer/0},
      {"Parse env value - float", fun test_parse_env_float/0},
      {"Parse env value - string", fun test_parse_env_string/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_get_default() ->
    %% Key doesn't exist anywhere, should return default
    Value = flurm_config:get(nonexistent_key, my_default),
    ?assertEqual(my_default, Value).

test_get_no_default() ->
    %% get/1 should return the default value (undefined in this case)
    Value = flurm_config:get(nonexistent_key),
    ?assertEqual(undefined, Value).

test_set_get() ->
    ok = flurm_config:set(my_test_key, my_test_value),
    Value = flurm_config:get(my_test_key),
    ?assertEqual(my_test_value, Value).

test_get_from_env_atom() ->
    %% Set environment variable
    os:putenv("FLURM_TEST_KEY", "env_value"),

    %% Should get from env
    Value = flurm_config:get(test_key),
    ?assertEqual(<<"env_value">>, Value).

test_get_from_env_tuple() ->
    %% Set environment variable for tuple key
    os:putenv("FLURM_CORE_SOME_KEY", "tuple_env_value"),

    %% Should get from env
    Value = flurm_config:get({flurm_core, some_key}),
    ?assertEqual(<<"tuple_env_value">>, Value).

test_load_file_success() ->
    %% Create a test config file
    TestDir = "/tmp/flurm_config_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    file:make_dir(TestDir),
    TestFile = TestDir ++ "/test.config",
    ConfigContent = "{key1, value1}.\n{key2, 42}.\n",
    ok = file:write_file(TestFile, ConfigContent),

    %% Load the file
    Result = flurm_config:load_file(TestFile),
    ?assertEqual(ok, Result),

    %% Verify values were loaded
    ?assertEqual(value1, flurm_config:get(key1)),
    ?assertEqual(42, flurm_config:get(key2)),

    %% Cleanup
    os:cmd("rm -rf " ++ TestDir).

test_load_file_error() ->
    %% Try to load non-existent file
    Result = flurm_config:load_file("/nonexistent/path/file.config"),
    ?assertMatch({error, _}, Result).

test_reload_no_file() ->
    %% With no config_file set, reload should return error
    application:unset_env(flurm_config, config_file),
    Result = flurm_config:reload(),
    ?assertEqual({error, no_config_file}, Result).

test_reload_from_file() ->
    %% Create a test config file
    TestDir = "/tmp/flurm_config_reload_" ++ integer_to_list(erlang:unique_integer([positive])),
    file:make_dir(TestDir),
    TestFile = TestDir ++ "/reload.config",
    ConfigContent = "{reload_key, reload_value}.\n",
    ok = file:write_file(TestFile, ConfigContent),

    %% Set the config file in app env
    application:set_env(flurm_config, config_file, TestFile),

    %% Reload
    Result = flurm_config:reload(),
    ?assertEqual(ok, Result),

    %% Verify value was loaded
    ?assertEqual(reload_value, flurm_config:get(reload_key)),

    %% Cleanup
    application:unset_env(flurm_config, config_file),
    os:cmd("rm -rf " ++ TestDir).

test_parse_env_true() ->
    os:putenv("FLURM_BOOL_KEY", "true"),
    Value = flurm_config:get(bool_key),
    ?assertEqual(true, Value).

test_parse_env_false() ->
    os:putenv("FLURM_BOOL_KEY2", "false"),
    Value = flurm_config:get(bool_key2),
    ?assertEqual(false, Value).

test_parse_env_integer() ->
    os:putenv("FLURM_INT_KEY", "12345"),
    Value = flurm_config:get(int_key),
    ?assertEqual(12345, Value).

test_parse_env_float() ->
    os:putenv("FLURM_FLOAT_KEY", "3.14159"),
    Value = flurm_config:get(float_key),
    ?assertEqual(3.14159, Value).

test_parse_env_string() ->
    os:putenv("FLURM_STR_KEY", "hello world"),
    Value = flurm_config:get(str_key),
    ?assertEqual(<<"hello world">>, Value).

%%====================================================================
%% Application Environment Fallback Tests
%%====================================================================

app_env_test_() ->
    {setup,
     fun() ->
         catch ets:delete(?CONFIG_TABLE),
         os:unsetenv("FLURM_APP_KEY"),
         ok
     end,
     fun(_) ->
         catch ets:delete(?CONFIG_TABLE),
         application:unset_env(flurm_config, app_key),
         ok
     end,
     [
      {"Get from application env", fun() ->
          %% Set in application env (not OS env)
          application:set_env(flurm_config, app_key, app_value),

          %% Should get from app env
          Value = flurm_config:get(app_key),
          ?assertEqual(app_value, Value)
      end},
      {"Get from application env with tuple key", fun() ->
          %% Set in application env
          application:set_env(flurm_core, tuple_app_key, tuple_app_value),

          %% Should get from app env
          Value = flurm_config:get({flurm_core, tuple_app_key}),
          ?assertEqual(tuple_app_value, Value),

          %% Cleanup
          application:unset_env(flurm_core, tuple_app_key)
      end}
     ]}.

%%====================================================================
%% Priority Tests - Env > App Env > ETS
%%====================================================================

priority_test_() ->
    {setup,
     fun() ->
         catch ets:delete(?CONFIG_TABLE),
         ok
     end,
     fun(_) ->
         catch ets:delete(?CONFIG_TABLE),
         os:unsetenv("FLURM_PRIORITY_KEY"),
         application:unset_env(flurm_config, priority_key),
         ok
     end,
     [
      {"Env takes priority over app env", fun() ->
          %% Set in both
          application:set_env(flurm_config, priority_key, app_value),
          os:putenv("FLURM_PRIORITY_KEY", "env_value"),

          %% Should get from env
          Value = flurm_config:get(priority_key),
          ?assertEqual(<<"env_value">>, Value)
      end},
      {"App env takes priority over ETS", fun() ->
          %% Set in ETS
          ok = flurm_config:set(priority_key2, ets_value),

          %% Set in app env
          application:set_env(flurm_config, priority_key2, app_value),

          %% Should get from app env
          Value = flurm_config:get(priority_key2),
          ?assertEqual(app_value, Value),

          %% Cleanup
          application:unset_env(flurm_config, priority_key2)
      end}
     ]}.

%%====================================================================
%% ETS Table Tests
%%====================================================================

ets_table_test_() ->
    [
     {"Table is created on first use", fun() ->
         catch ets:delete(?CONFIG_TABLE),
         %% Table should not exist
         ?assertEqual(undefined, ets:whereis(?CONFIG_TABLE)),

         %% Set a value - should create table
         ok = flurm_config:set(auto_create_key, auto_create_value),

         %% Table should now exist
         ?assert(ets:whereis(?CONFIG_TABLE) =/= undefined)
     end},
     {"Multiple sets to same key", fun() ->
         catch ets:delete(?CONFIG_TABLE),
         ok = flurm_config:set(multi_key, value1),
         ?assertEqual(value1, flurm_config:get(multi_key)),

         ok = flurm_config:set(multi_key, value2),
         ?assertEqual(value2, flurm_config:get(multi_key))
     end},
     {"Table already exists - ensure_table returns ok", fun() ->
         %% First delete if exists
         catch ets:delete(?CONFIG_TABLE),
         %% Create table via first set
         ok = flurm_config:set(first_key, first_value),
         ?assert(ets:whereis(?CONFIG_TABLE) =/= undefined),

         %% Second set should reuse existing table (ensure_table returns ok)
         ok = flurm_config:set(second_key, second_value),
         ?assert(ets:whereis(?CONFIG_TABLE) =/= undefined),

         %% Both values should be retrievable
         ?assertEqual(first_value, flurm_config:get(first_key)),
         ?assertEqual(second_value, flurm_config:get(second_key))
     end},
     {"Get from ETS when table exists but key not found", fun() ->
         catch ets:delete(?CONFIG_TABLE),
         ok = flurm_config:set(existing_key, existing_value),

         %% Get a non-existing key should return default
         Value = flurm_config:get(nonexistent_ets_key, my_default),
         ?assertEqual(my_default, Value)
     end}
    ].
