%%%-------------------------------------------------------------------
%%% @doc Tests for FLURM Hot Upgrade System
%%%
%%% Tests cover:
%%% - Module hot reload functionality
%%% - State transformation during upgrades
%%% - Upgrade safety checks
%%% - Version tracking
%%%-------------------------------------------------------------------
-module(flurm_upgrade_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

upgrade_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"versions returns app versions", fun test_versions/0},
      {"reload_module loads new code", fun test_reload_module/0},
      {"reload_modules handles multiple modules", fun test_reload_modules/0},
      {"check_upgrade validates prerequisites", fun test_check_upgrade/0},
      {"upgrade_status returns details", fun test_upgrade_status/0},
      {"transform_state handles state changes", fun test_transform_state/0}
     ]}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup() ->
    %% Start required applications
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_versions() ->
    Versions = flurm_upgrade:versions(),
    ?assert(is_list(Versions)),
    %% Check that flurm_core is in the list
    case lists:keyfind(flurm_core, 1, Versions) of
        {flurm_core, Vsn} ->
            ?assert(is_list(Vsn));
        false ->
            %% App might not be started in test
            ok
    end.

test_reload_module() ->
    %% Test reloading a stateless module
    %% Use a module that doesn't have active processes
    Module = flurm_upgrade,
    Result = flurm_upgrade:reload_module(Module),
    ?assertEqual(ok, Result).

test_reload_modules() ->
    %% Test reloading multiple modules
    Modules = [flurm_upgrade],
    Result = flurm_upgrade:reload_modules(Modules),
    ?assertEqual(ok, Result).

test_check_upgrade() ->
    %% Test upgrade check for non-existent version
    Result = flurm_upgrade:check_upgrade("99.99.99"),
    ?assertMatch({error, _}, Result),

    %% Check that error includes release_exists failure
    {error, Failures} = Result,
    ?assert(lists:keymember(release_exists, 1, Failures)).

test_upgrade_status() ->
    %% Note: release_handler may not be running in test environment
    try
        Status = flurm_upgrade:upgrade_status(),
        ?assert(is_map(Status)),
        ?assert(maps:is_key(versions, Status)),
        ?assert(maps:is_key(releases, Status)),
        ?assert(maps:is_key(loaded_modules, Status)),
        ?assert(maps:is_key(old_code_modules, Status))
    catch
        exit:{noproc, _} ->
            %% release_handler not running - this is expected in test env
            ok
    end.

test_transform_state() ->
    %% Test state transformation helper
    OldState = #{value => 1},
    Extra = [],

    %% Default behavior: return state unchanged
    Result = flurm_upgrade:transform_state(nonexistent_module, OldState, Extra),
    ?assertEqual(OldState, Result).

%%====================================================================
%% Code Change Tests
%%====================================================================

code_change_test_() ->
    {"code_change callback tests",
     {setup,
      fun() -> ok end,
      fun(_) -> ok end,
      [
       {"gen_server code_change preserves state", fun test_gen_server_code_change/0}
      ]}}.

test_gen_server_code_change() ->
    %% Test that our gen_servers properly implement code_change
    %% by checking that calling code_change returns {ok, State}

    %% Test flurm_config_server code_change
    TestState = {state, #{}, undefined, [], undefined, 1},
    {ok, NewState} = flurm_config_server:code_change(undefined, TestState, []),
    ?assertEqual(TestState, NewState).

%%====================================================================
%% Integration Tests
%%====================================================================

integration_test_() ->
    {timeout, 60,
     {"Integration tests for hot upgrades",
      {setup,
       fun setup_integration/0,
       fun cleanup_integration/1,
       [
        {"reload preserves running state", fun test_reload_preserves_state/0}
       ]}}}.

setup_integration() ->
    %% Start the config server for integration testing
    case whereis(flurm_config_server) of
        undefined ->
            {ok, _} = flurm_config_server:start_link();
        _ ->
            ok
    end,
    ok.

cleanup_integration(_) ->
    %% Stop the config server if we started it
    case whereis(flurm_config_server) of
        Pid when is_pid(Pid) ->
            gen_server:stop(Pid);
        _ ->
            ok
    end.

test_reload_preserves_state() ->
    %% Set a config value
    ok = flurm_config_server:set(test_key, test_value),

    %% Verify it's set
    ?assertEqual(test_value, flurm_config_server:get(test_key)),

    %% Reload the module
    Result = flurm_upgrade:reload_module(flurm_config_server),
    %% Note: This may fail if there are processes using the module
    %% In that case, we use upgrade_module_with_state internally
    case Result of
        ok -> ok;
        {error, _} -> ok  % Expected if process has old code
    end,

    %% Verify state is preserved
    %% Note: If code_change was properly called, state should be preserved
    ?assertEqual(test_value, flurm_config_server:get(test_key)).

%%====================================================================
%% Property Tests (would use PropEr if available)
%%====================================================================

property_test_() ->
    {"Property-based tests for upgrades",
     [
      {"version strings are valid", fun prop_version_strings/0}
     ]}.

prop_version_strings() ->
    Versions = flurm_upgrade:versions(),
    lists:foreach(fun({_App, Vsn}) ->
        %% Version should be a string
        ?assert(is_list(Vsn)),
        %% Version should match semver pattern (roughly)
        ?assert(length(Vsn) > 0)
    end, Versions).
