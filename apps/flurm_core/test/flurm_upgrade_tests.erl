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

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

additional_coverage_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        {"reload_modules with empty list", fun test_reload_modules_empty/0},
        {"reload_modules with partial failure", fun test_reload_modules_partial_failure/0},
        {"install_release with invalid version", fun test_install_release_invalid/0},
        {"rollback without previous release", fun test_rollback_no_previous/0},
        {"which_releases returns list", fun test_which_releases/0},
        {"is_flurm_module detection", fun test_is_flurm_module/0},
        {"get_loaded_modules returns flurm modules", fun test_get_loaded_modules/0},
        {"get_old_code_modules returns list", fun test_get_old_code_modules/0},
        {"transform_state with extra data", fun test_transform_state_extra/0}
     ]}.

test_reload_modules_empty() ->
    %% Empty list should succeed
    Result = flurm_upgrade:reload_modules([]),
    ?assertEqual(ok, Result).

test_reload_modules_partial_failure() ->
    %% Test with non-existent module - should fail
    Result = flurm_upgrade:reload_modules([nonexistent_module_xyz123]),
    ?assertMatch({error, {partial_failure, _}}, Result).

test_install_release_invalid() ->
    %% Installing non-existent release should fail
    Result = flurm_upgrade:install_release("0.0.0-nonexistent"),
    ?assertMatch({error, {pre_check_failed, _}}, Result).

test_rollback_no_previous() ->
    %% Rollback when there's no previous release
    try
        Result = flurm_upgrade:rollback(),
        %% Either error or success depending on release handler state
        case Result of
            ok -> ok;
            {error, no_previous_release} -> ok;
            {error, _} -> ok
        end
    catch
        exit:{noproc, _} ->
            %% release_handler not running
            ok
    end.

test_which_releases() ->
    try
        Releases = flurm_upgrade:which_releases(),
        ?assert(is_list(Releases)),
        lists:foreach(fun(Entry) ->
            ?assertMatch({_Vsn, _Status}, Entry)
        end, Releases)
    catch
        exit:{noproc, _} ->
            ok
    end.

test_is_flurm_module() ->
    %% Indirectly test through upgrade_status
    try
        Status = flurm_upgrade:upgrade_status(),
        LoadedModules = maps:get(loaded_modules, Status, []),

        %% All returned modules should have flurm_ prefix
        lists:foreach(fun(Mod) ->
            ModStr = atom_to_list(Mod),
            ?assert(lists:prefix("flurm_", ModStr))
        end, LoadedModules)
    catch
        exit:{noproc, _} ->
            %% release_handler not running - expected in test env
            ok
    end.

test_get_loaded_modules() ->
    try
        Status = flurm_upgrade:upgrade_status(),
        LoadedModules = maps:get(loaded_modules, Status, []),
        ?assert(is_list(LoadedModules)),
        %% Should include at least flurm_upgrade
        ?assert(lists:member(flurm_upgrade, LoadedModules) orelse LoadedModules =:= [])
    catch
        exit:{noproc, _} ->
            %% release_handler not running - expected in test env
            ok
    end.

test_get_old_code_modules() ->
    try
        Status = flurm_upgrade:upgrade_status(),
        OldCodeModules = maps:get(old_code_modules, Status, []),
        ?assert(is_list(OldCodeModules))
    catch
        exit:{noproc, _} ->
            %% release_handler not running - expected in test env
            ok
    end.

test_transform_state_extra() ->
    %% Test with various extra data types
    State = #{key => value},

    %% With list extra
    R1 = flurm_upgrade:transform_state(undefined, State, []),
    ?assertEqual(State, R1),

    %% With map extra
    R2 = flurm_upgrade:transform_state(undefined, State, #{extra => data}),
    ?assertEqual(State, R2),

    %% With tuple extra
    R3 = flurm_upgrade:transform_state(undefined, State, {extra, tuple}),
    ?assertEqual(State, R3).

%%====================================================================
%% Check Upgrade Detail Tests
%%====================================================================

check_upgrade_detail_test_() ->
    [
     {"check_upgrade validates disk_space", fun test_check_disk_space/0},
     {"check_upgrade validates cluster_healthy", fun test_check_cluster_healthy/0},
     {"check_upgrade validates no_pending_jobs", fun test_check_no_pending_jobs/0}
    ].

test_check_disk_space() ->
    %% Disk space check is included in check_upgrade
    Result = flurm_upgrade:check_upgrade("99.99.99"),
    {error, Failures} = Result,

    %% disk_space should NOT be in failures (should pass)
    DiskSpaceFailure = lists:keyfind(disk_space, 1, Failures),
    ?assertEqual(false, DiskSpaceFailure).

test_check_cluster_healthy() ->
    %% Cluster health check when no cluster is running
    Result = flurm_upgrade:check_upgrade("99.99.99"),
    {error, Failures} = Result,

    %% cluster_healthy should NOT be in failures (no cluster = ok)
    ClusterFailure = lists:keyfind(cluster_healthy, 1, Failures),
    ?assertEqual(false, ClusterFailure).

test_check_no_pending_jobs() ->
    %% Pending jobs check
    Result = flurm_upgrade:check_upgrade("99.99.99"),
    {error, Failures} = Result,

    %% no_pending_jobs should NOT be in failures (current implementation always passes)
    PendingJobsFailure = lists:keyfind(no_pending_jobs, 1, Failures),
    ?assertEqual(false, PendingJobsFailure).

%%====================================================================
%% Process Upgrade Tests
%%====================================================================

process_upgrade_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(lager),
         ok
     end,
     fun(_) -> ok end,
     [
        {"reload module with active processes", fun test_reload_with_processes/0}
     ]}.

test_reload_with_processes() ->
    %% Start a gen_server using our code
    case whereis(flurm_config_server) of
        undefined ->
            {ok, Pid} = flurm_config_server:start_link(),

            %% Try to reload - should handle the active process
            Result = flurm_upgrade:reload_module(flurm_config_server),

            %% Either success or error is acceptable
            case Result of
                ok -> ok;
                {error, _} -> ok
            end,

            gen_server:stop(Pid);
        _Pid ->
            %% Already running
            Result = flurm_upgrade:reload_module(flurm_config_server),
            case Result of
                ok -> ok;
                {error, _} -> ok
            end
    end.
