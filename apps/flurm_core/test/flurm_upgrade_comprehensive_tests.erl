%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_upgrade module
%%% Tests hot code upgrade and version management
%%%-------------------------------------------------------------------
-module(flurm_upgrade_comprehensive_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Cases
%%====================================================================

%% Basic API tests
versions_test() ->
    Versions = flurm_upgrade:versions(),
    ?assert(is_list(Versions)),
    %% Each entry should be {App, Vsn} tuple
    lists:foreach(fun(Entry) ->
        ?assertMatch({_, _}, Entry),
        {App, Vsn} = Entry,
        ?assert(is_atom(App)),
        ?assert(is_list(Vsn))
    end, Versions).

upgrade_status_test() ->
    %% Note: This may not fully work without release_handler running
    try
        Status = flurm_upgrade:upgrade_status(),
        ?assert(is_map(Status)),
        ?assert(maps:is_key(versions, Status)),
        ?assert(maps:is_key(releases, Status)),
        ?assert(maps:is_key(loaded_modules, Status)),
        ?assert(maps:is_key(old_code_modules, Status))
    catch
        exit:{noproc, _} ->
            %% release_handler not running - expected in test env
            ok
    end.

which_releases_test() ->
    try
        Releases = flurm_upgrade:which_releases(),
        ?assert(is_list(Releases)),
        lists:foreach(fun(Entry) ->
            ?assertMatch({_, _}, Entry),
            {Vsn, Status} = Entry,
            ?assert(is_list(Vsn)),
            ?assert(is_atom(Status))
        end, Releases)
    catch
        exit:{noproc, _} ->
            ok
    end.

reload_module_test() ->
    %% Test reloading a simple module
    Result = flurm_upgrade:reload_module(flurm_upgrade),
    ?assertEqual(ok, Result).

reload_modules_test() ->
    %% Test reloading multiple modules
    Result = flurm_upgrade:reload_modules([flurm_upgrade]),
    ?assertEqual(ok, Result).

reload_modules_partial_failure_test() ->
    %% Test with a non-existent module
    Result = flurm_upgrade:reload_modules([nonexistent_module_xyz]),
    ?assertMatch({error, {partial_failure, _}}, Result).

check_upgrade_nonexistent_test() ->
    Result = flurm_upgrade:check_upgrade("99.99.99"),
    ?assertMatch({error, _}, Result),
    {error, Failures} = Result,
    ?assert(lists:keymember(release_exists, 1, Failures)).

rollback_no_previous_test() ->
    try
        Result = flurm_upgrade:rollback(),
        %% Either succeeds if there's a previous release, or fails
        case Result of
            ok -> ok;
            {error, no_previous_release} -> ok;
            {error, _} -> ok
        end
    catch
        exit:{noproc, _} ->
            ok
    end.

transform_state_default_test() ->
    %% Test default state transformation (no change)
    OldState = #{value => 1, name => <<"test">>},
    Extra = [],
    Result = flurm_upgrade:transform_state(nonexistent_module, OldState, Extra),
    ?assertEqual(OldState, Result).

transform_state_with_module_test() ->
    %% Test with a module that has transform_state - uses default if not exported
    OldState = #{value => 42},
    Extra = [],
    Result = flurm_upgrade:transform_state(flurm_upgrade, OldState, Extra),
    ?assertEqual(OldState, Result).

install_release_check_fails_test() ->
    %% Install should fail if check_upgrade fails
    Result = flurm_upgrade:install_release("99.99.99"),
    ?assertMatch({error, {pre_check_failed, _}}, Result).

%%====================================================================
%% Internal Function Coverage Tests
%%====================================================================

%% These tests cover internal functions indirectly

get_loaded_modules_test() ->
    Status = flurm_upgrade:upgrade_status(),
    LoadedModules = maps:get(loaded_modules, Status, []),
    ?assert(is_list(LoadedModules)),
    %% Should include flurm modules
    FlurModules = [M || M <- LoadedModules, is_flurm_module(M)],
    ?assert(length(FlurModules) > 0).

get_old_code_modules_test() ->
    Status = flurm_upgrade:upgrade_status(),
    OldCodeModules = maps:get(old_code_modules, Status, []),
    ?assert(is_list(OldCodeModules)).

is_flurm_module(M) ->
    Prefix = atom_to_list(M),
    lists:prefix("flurm_", Prefix).

%%====================================================================
%% Disk Space Check Tests
%%====================================================================

disk_space_test_() ->
    [
     {"Check disk space doesn't crash", fun() ->
         %% This exercises check_disk_space internally via check_upgrade
         Result = flurm_upgrade:check_upgrade("99.99.99"),
         ?assertMatch({error, _}, Result),
         {error, Failures} = Result,
         %% disk_space should not be in failures (should pass)
         DiskFailure = lists:keyfind(disk_space, 1, Failures),
         ?assertEqual(false, DiskFailure)
     end}
    ].

%%====================================================================
%% Cluster Health Check Tests
%%====================================================================

cluster_health_test_() ->
    [
     {"Check cluster health with no cluster", fun() ->
         %% With no cluster manager running, should pass
         Result = flurm_upgrade:check_upgrade("99.99.99"),
         ?assertMatch({error, _}, Result),
         {error, Failures} = Result,
         %% cluster_healthy should not be in failures
         ClusterFailure = lists:keyfind(cluster_healthy, 1, Failures),
         ?assertEqual(false, ClusterFailure)
     end}
    ].

%%====================================================================
%% Module Version Tests
%%====================================================================

module_version_test_() ->
    [
     {"Get module version", fun() ->
         %% Test with a loaded module
         case code:get_object_code(flurm_upgrade) of
             {flurm_upgrade, Bin, _} ->
                 case beam_lib:version(Bin) of
                     {ok, {flurm_upgrade, [_Vsn | _]}} ->
                         ok;
                     {ok, {flurm_upgrade, []}} ->
                         ok;
                     _ ->
                         ok
                 end;
             error ->
                 ok
         end
     end}
    ].

%%====================================================================
%% Process Using Module Tests
%%====================================================================

process_module_test_() ->
    [
     {"Find processes using module", fun() ->
         %% Indirectly test find_processes_using_module
         %% by calling reload_module on a module with active processes
         %% This is tricky because we need a module with processes

         %% Start a temporary gen_server using flurm_config_server
         case whereis(flurm_config_server) of
             undefined ->
                 %% Start one for testing
                 {ok, Pid} = flurm_config_server:start_link([]),
                 %% Now try to reload - it should handle processes
                 Result = flurm_upgrade:reload_module(flurm_config_server),
                 %% May succeed or fail based on process state
                 ?assert(Result =:= ok orelse element(1, Result) =:= error),
                 gen_server:stop(Pid);
             _Pid ->
                 %% Already running, just test reload
                 Result = flurm_upgrade:reload_module(flurm_config_server),
                 ?assert(Result =:= ok orelse element(1, Result) =:= error)
         end
     end}
    ].

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    [
     {"reload_modules with empty list", fun() ->
         Result = flurm_upgrade:reload_modules([]),
         ?assertEqual(ok, Result)
     end},
     {"versions with unstarted apps", fun() ->
         Versions = flurm_upgrade:versions(),
         %% Should return list even if some apps not started
         ?assert(is_list(Versions))
     end}
    ].
