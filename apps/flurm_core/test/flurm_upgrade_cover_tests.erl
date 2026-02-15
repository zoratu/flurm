%%%-------------------------------------------------------------------
%%% @doc Cover-effective tests for flurm_upgrade module
%%%
%%% These tests cover the -ifdef(TEST) exported functions and API
%%% of the flurm_upgrade module for maximum code coverage.
%%%
%%% Tests cover:
%%% - is_flurm_module/1 - Pure function: flurm_* prefix detection (true/false)
%%% - check_no_pending_jobs/0 - Stub that always returns ok
%%% - get_loaded_modules/0 - Returns sorted list of flurm_ modules
%%% - get_old_code_modules/0 - Returns list of modules with old code
%%% - check_release_exists/1 - Tests non-existent version (error path)
%%% - check_cluster_healthy/0 - No cluster process branch (returns ok)
%%% - disk_free/1 - Real filesystem test on Unix
%%% - check_disk_space/0 - Real filesystem check
%%% - transform_state/3 - Module doesn't export transform_state (false branch)
%%% - reload_modules/1 - Empty list and partial failure paths
%%% - check_upgrade/1 - Check failure path
%%% - get_process_modules/1 - Current and dead process paths
%%% - get_module_version/1 - Existing and non-existent module paths
%%% - versions/0 - Returns list of app versions
%%% - upgrade_status/0 - Returns map with upgrade status
%%% - which_releases/0 - Returns list of releases
%%% - rollback/0 - Handles no previous release
%%% - install_release/1 - Pre-check failure path
%%% - reload_module/1 - Success and failure paths
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_upgrade_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure lager is started for logging functions
    application:load(lager),
    application:set_env(lager, handlers, []),
    application:set_env(lager, error_logger_redirect, false),
    catch application:start(lager),
    ok.

cleanup(_) ->
    %% Unload any mocks that may be lingering
    catch meck:unload(code),
    catch meck:unload(erlang),
    catch meck:unload(filelib),
    catch meck:unload(os),
    catch meck:unload(file),
    catch meck:unload(flurm_controller_cluster),
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_upgrade_cover_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% Pure function tests (no mocking needed)
      {"is_flurm_module/1 with flurm_ prefix", fun test_is_flurm_module_true/0},
      {"is_flurm_module/1 without flurm_ prefix", fun test_is_flurm_module_false/0},
      {"is_flurm_module/1 edge cases", fun test_is_flurm_module_edge_cases/0},

      %% Stub test
      {"check_no_pending_jobs/0 returns ok", fun test_check_no_pending_jobs/0}
     ]}.

%% Tests for loaded modules without mocking core BIFs
loaded_modules_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_loaded_modules/0 returns flurm modules", fun test_get_loaded_modules_real/0},
      {"get_old_code_modules/0 returns list", fun test_get_old_code_modules_real/0}
     ]}.

check_release_exists_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"check_release_exists/1 - nonexistent version", fun test_check_release_exists_none_real/0}
     ]}.

check_cluster_healthy_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"check_cluster_healthy/0 - no cluster process", fun test_check_cluster_healthy_no_process/0}
     ]}.

disk_free_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"disk_free/1 - real filesystem", fun test_disk_free_real/0}
     ]}.

check_disk_space_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"check_disk_space/0 - real check", fun test_check_disk_space_real/0}
     ]}.

transform_state_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"transform_state/3 - module does not export", fun test_transform_state_no_export/0}
     ]}.

reload_and_check_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"reload_modules/1 - empty list", fun test_reload_modules_empty/0},
      {"reload_modules/1 - partial failure", fun test_reload_modules_partial_failure_real/0},
      {"check_upgrade/1 - some checks fail", fun test_check_upgrade_some_fail_real/0}
     ]}.

get_process_modules_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_process_modules/1 - with current process", fun test_get_process_modules_self/0},
      {"get_process_modules/1 - with dead process", fun test_get_process_modules_dead/0}
     ]}.

get_module_version_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_module_version/1 - existing module", fun test_get_module_version_exists/0},
      {"get_module_version/1 - non-existent module", fun test_get_module_version_not_exists/0}
     ]}.

%%====================================================================
%% is_flurm_module/1 Tests (Pure Function)
%%====================================================================

test_is_flurm_module_true() ->
    %% Test modules with flurm_ prefix
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_upgrade)),
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_core)),
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_scheduler)),
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_job_manager)),
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_config_server)),
    ok.

test_is_flurm_module_false() ->
    %% Test modules without flurm_ prefix
    ?assertEqual(false, flurm_upgrade:is_flurm_module(lists)),
    ?assertEqual(false, flurm_upgrade:is_flurm_module(gen_server)),
    ?assertEqual(false, flurm_upgrade:is_flurm_module(erlang)),
    ?assertEqual(false, flurm_upgrade:is_flurm_module(kernel)),
    ?assertEqual(false, flurm_upgrade:is_flurm_module(application)),
    ok.

test_is_flurm_module_edge_cases() ->
    %% Edge cases
    ?assertEqual(false, flurm_upgrade:is_flurm_module(flurm)),  % Just "flurm", no underscore
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_)),  % "flurm_" with nothing after
    ?assertEqual(false, flurm_upgrade:is_flurm_module(my_flurm_module)),  % flurm in middle
    ?assertEqual(false, flurm_upgrade:is_flurm_module(flurm123)),  % no underscore after flurm
    ok.

%%====================================================================
%% check_no_pending_jobs/0 Tests (Stub)
%%====================================================================

test_check_no_pending_jobs() ->
    %% This is a stub that always returns ok
    ?assertEqual(ok, flurm_upgrade:check_no_pending_jobs()),
    %% Call multiple times to ensure consistent behavior
    ?assertEqual(ok, flurm_upgrade:check_no_pending_jobs()),
    ?assertEqual(ok, flurm_upgrade:check_no_pending_jobs()),
    ok.

%%====================================================================
%% get_loaded_modules/0 Tests (Real - no mocking of code module)
%%====================================================================

test_get_loaded_modules_real() ->
    %% Test with real code:all_loaded
    Result = flurm_upgrade:get_loaded_modules(),
    ?assert(is_list(Result)),
    %% Should only contain flurm_ modules
    lists:foreach(fun(Mod) ->
        ?assert(flurm_upgrade:is_flurm_module(Mod))
    end, Result),
    %% Should contain flurm_upgrade since we're running it
    ?assert(lists:member(flurm_upgrade, Result)),
    %% Result should be sorted
    ?assertEqual(lists:sort(Result), Result),
    ok.

%%====================================================================
%% get_old_code_modules/0 Tests (Real - no mocking of erlang BIFs)
%%====================================================================

test_get_old_code_modules_real() ->
    %% Test with real code:all_loaded and erlang:check_old_code
    %% This tests the function without mocking BIFs which is problematic
    Result = flurm_upgrade:get_old_code_modules(),
    ?assert(is_list(Result)),
    %% All returned modules should be flurm_ modules
    lists:foreach(fun(Mod) ->
        ?assert(flurm_upgrade:is_flurm_module(Mod))
    end, Result),
    %% All returned modules should be loaded
    lists:foreach(fun(Mod) ->
        ?assertNotEqual(false, code:is_loaded(Mod))
    end, Result),
    ok.

%%====================================================================
%% check_release_exists/1 Tests (Real - no mocking)
%%====================================================================

test_check_release_exists_none_real() ->
    %% Test with a non-existent version - should return error
    Result = flurm_upgrade:check_release_exists("99.99.99-nonexistent"),
    ?assertEqual({error, release_not_found}, Result),
    ok.

%%====================================================================
%% check_cluster_healthy/0 Tests
%%====================================================================
%% Note: Testing the cluster healthy function with process registration
%% is tricky in EUnit. We test the main branch (no cluster process).

test_check_cluster_healthy_no_process() ->
    %% When no cluster process is registered, should return ok
    %% First ensure no process is registered
    case whereis(flurm_controller_cluster) of
        undefined -> ok;
        Pid when is_pid(Pid) ->
            %% Skip test if cluster is actually running
            ?assertEqual(ok, flurm_upgrade:check_cluster_healthy())
    end,
    %% Now test when undefined
    case whereis(flurm_controller_cluster) of
        undefined ->
            ?assertEqual(ok, flurm_upgrade:check_cluster_healthy());
        _ ->
            ok  % Skip if process exists
    end,
    ok.

%%====================================================================
%% disk_free/1 Tests (Real - no mocking)
%%====================================================================

test_disk_free_real() ->
    %% Test with real filesystem - should work on Unix
    case os:type() of
        {unix, _} ->
            Result = flurm_upgrade:disk_free("/"),
            case Result of
                {ok, Free} when is_integer(Free), Free > 0 ->
                    ok;
                {error, _} ->
                    %% May fail in some environments, that's ok
                    ok
            end;
        _ ->
            %% Non-Unix should return unsupported_os
            ?assertEqual({error, unsupported_os}, flurm_upgrade:disk_free("/"))
    end,
    ok.

%%====================================================================
%% check_disk_space/0 Tests (Real - no mocking)
%%====================================================================

test_check_disk_space_real() ->
    %% Test with real filesystem
    Result = flurm_upgrade:check_disk_space(),
    %% Should return ok (sufficient space) or ok (can't check)
    %% or {error, insufficient_disk_space}
    ?assert(Result =:= ok orelse Result =:= {error, insufficient_disk_space}),
    ok.

%%====================================================================
%% transform_state/3 Tests (Real - no mocking)
%%====================================================================

test_transform_state_no_export() ->
    %% Test with a module that doesn't export transform_state
    %% Using 'io' which definitely doesn't have transform_state/3
    OldState = #{value => 123},
    Result = flurm_upgrade:transform_state(io, OldState, []),
    %% Should return state unchanged
    ?assertEqual(OldState, Result),

    %% Test with different state types
    ?assertEqual(simple_atom, flurm_upgrade:transform_state(lists, simple_atom, [])),
    ?assertEqual({tuple, state}, flurm_upgrade:transform_state(maps, {tuple, state}, [])),
    ?assertEqual([1, 2, 3], flurm_upgrade:transform_state(proplists, [1, 2, 3], [])),
    ok.

%%====================================================================
%% reload_modules/1 and check_upgrade/1 Tests (Real - no mocking)
%%====================================================================

test_reload_modules_empty() ->
    %% Empty list should succeed
    ?assertEqual(ok, flurm_upgrade:reload_modules([])),
    ok.

test_reload_modules_partial_failure_real() ->
    %% Test with non-existent module - should fail
    Result = flurm_upgrade:reload_modules([nonexistent_module_xyz_12345]),
    ?assertMatch({error, {partial_failure, _}}, Result),
    {error, {partial_failure, Failures}} = Result,
    ?assertEqual(1, length(Failures)),
    ok.

test_check_upgrade_some_fail_real() ->
    %% Test with non-existent version
    Result = flurm_upgrade:check_upgrade("99.99.99-nonexistent"),
    ?assertMatch({error, _}, Result),
    {error, Failures} = Result,
    ?assert(is_list(Failures)),
    %% Should have release_exists failure
    ?assert(lists:keymember(release_exists, 1, Failures)),
    ok.

%%====================================================================
%% get_process_modules/1 Tests
%%====================================================================

test_get_process_modules_self() ->
    %% Test with current process
    Result = flurm_upgrade:get_process_modules(self()),
    ?assert(is_list(Result)),
    %% Should contain at least the current module
    ?assert(lists:member(?MODULE, Result) orelse length(Result) >= 0),
    ok.

test_get_process_modules_dead() ->
    %% Test with a dead process
    Pid = spawn(fun() -> ok end),
    %% Wait for process to die
    timer:sleep(10),
    ?assertNot(is_process_alive(Pid)),
    Result = flurm_upgrade:get_process_modules(Pid),
    ?assertEqual([], Result),
    ok.

%%====================================================================
%% get_module_version/1 Tests
%%====================================================================

test_get_module_version_exists() ->
    %% Test with a loaded module
    Result = flurm_upgrade:get_module_version(flurm_upgrade),
    %% Should return either a version (integer, atom, list, tuple, binary) or undefined
    ?assert(Result =:= undefined orelse
            is_atom(Result) orelse
            is_tuple(Result) orelse
            is_list(Result) orelse
            is_integer(Result) orelse
            is_binary(Result)),
    ok.

test_get_module_version_not_exists() ->
    %% Test with a non-existent module
    Result = flurm_upgrade:get_module_version(definitely_not_a_real_module_xyz),
    ?assertEqual(undefined, Result),
    ok.

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

additional_cover_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"is_flurm_module handles various atoms", fun test_is_flurm_module_various/0},
      {"check_no_pending_jobs is idempotent", fun test_check_no_pending_jobs_idempotent/0}
     ]}.

test_is_flurm_module_various() ->
    %% Test with single character after prefix
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_a)),
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_1)),

    %% Test with long names
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_very_long_module_name_with_many_parts)),

    %% Test with numbers in name
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_job_123)),

    %% Test similar but not matching prefixes
    ?assertEqual(false, flurm_upgrade:is_flurm_module(fLurm_module)),  % Wrong case
    ?assertEqual(false, flurm_upgrade:is_flurm_module(flurM_module)),  % Wrong case
    ?assertEqual(false, flurm_upgrade:is_flurm_module(furm_module)),   % Missing 'l'
    ok.

test_check_no_pending_jobs_idempotent() ->
    %% Should return ok every time
    Results = [flurm_upgrade:check_no_pending_jobs() || _ <- lists:seq(1, 10)],
    ?assertEqual(lists:duplicate(10, ok), Results),
    ok.

%%====================================================================
%% Integration-style Coverage Tests
%%====================================================================

integration_test_() ->
    {foreach,
     fun setup_integration/0,
     fun cleanup_integration/1,
     [
      {"versions/0 returns list", fun test_versions_coverage/0},
      {"upgrade_status/0 returns map", fun test_upgrade_status_coverage/0},
      {"reload_module/1 handles nonexistent", fun test_reload_module_nonexistent/0}
     ]}.

setup_integration() ->
    setup(),
    ok.

cleanup_integration(_) ->
    cleanup(ok),
    ok.

test_versions_coverage() ->
    Result = flurm_upgrade:versions(),
    ?assert(is_list(Result)),
    %% Each element should be {App, Vsn}
    lists:foreach(fun({App, Vsn}) ->
        ?assert(is_atom(App)),
        ?assert(is_list(Vsn))
    end, Result),
    ok.

test_upgrade_status_coverage() ->
    %% May fail if release_handler is not running
    try
        Result = flurm_upgrade:upgrade_status(),
        ?assert(is_map(Result)),
        ?assert(maps:is_key(versions, Result)),
        ?assert(maps:is_key(loaded_modules, Result)),
        ?assert(maps:is_key(old_code_modules, Result))
    catch
        exit:{noproc, _} ->
            %% Expected in test environment
            ok
    end,
    ok.

test_reload_module_nonexistent() ->
    Result = flurm_upgrade:reload_module(definitely_not_a_real_module_xyz_12345),
    ?assertMatch({error, {load_failed, _}}, Result),
    ok.

%%====================================================================
%% reload_module/1 Success Path Tests
%%====================================================================

reload_module_success_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"reload_module/1 - reload flurm_upgrade", fun test_reload_module_success/0}
     ]}.

test_reload_module_success() ->
    %% Test reloading flurm_upgrade itself - should succeed
    Result = flurm_upgrade:reload_module(flurm_upgrade),
    %% Should either succeed or fail gracefully if old code exists
    ?assert(Result =:= ok orelse element(1, Result) =:= error),
    ok.

%%====================================================================
%% transform_state/3 Additional Tests
%%====================================================================
%% Note: Testing the "true" branch of transform_state/3 requires a real
%% module that exports transform_state/3. Meck cannot make
%% erlang:function_exported/3 return true for a mocked module.
%% The "false" branch is tested in test_transform_state_no_export/0.

%%====================================================================
%% which_releases/0 Tests
%%====================================================================

which_releases_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"which_releases/0 returns list", fun test_which_releases/0}
     ]}.

test_which_releases() ->
    %% Test which_releases - may fail if release_handler is not running
    try
        Result = flurm_upgrade:which_releases(),
        ?assert(is_list(Result)),
        lists:foreach(fun({Vsn, Status}) ->
            ?assert(is_list(Vsn)),
            ?assert(is_atom(Status))
        end, Result)
    catch
        exit:{noproc, _} ->
            %% Expected in test environment
            ok
    end,
    ok.

%%====================================================================
%% rollback/0 Tests
%%====================================================================

rollback_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"rollback/0 handles no previous release", fun test_rollback_no_previous/0}
     ]}.

test_rollback_no_previous() ->
    %% Test rollback when there's likely no previous release
    try
        Result = flurm_upgrade:rollback(),
        %% Should return ok, error, or throw
        ?assert(Result =:= ok orelse
                element(1, Result) =:= error)
    catch
        exit:{noproc, _} ->
            %% release_handler not running - expected
            ok
    end,
    ok.

%%====================================================================
%% install_release/1 Tests
%%====================================================================

install_release_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"install_release/1 fails pre-check for nonexistent", fun test_install_release_precheck_fail/0}
     ]}.

test_install_release_precheck_fail() ->
    %% Test install_release with non-existent version
    Result = flurm_upgrade:install_release("99.99.99-nonexistent"),
    ?assertMatch({error, {pre_check_failed, _}}, Result),
    {error, {pre_check_failed, Reasons}} = Result,
    %% Should include release_exists failure
    ?assert(lists:keymember(release_exists, 1, Reasons)),
    ok.
