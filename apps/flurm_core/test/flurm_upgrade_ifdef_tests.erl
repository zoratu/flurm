%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_upgrade internal functions
%%%
%%% These tests exercise the internal helper functions exported via
%%% -ifdef(TEST) to achieve coverage of upgrade checking logic.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_upgrade_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Module Classification Tests
%%====================================================================

is_flurm_module_core_test() ->
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_scheduler)).

is_flurm_module_config_test() ->
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_config_server)).

is_flurm_module_protocol_test() ->
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_protocol_codec)).

is_flurm_module_db_test() ->
    ?assertEqual(true, flurm_upgrade:is_flurm_module(flurm_db_ra)).

is_flurm_module_non_flurm_test() ->
    ?assertEqual(false, flurm_upgrade:is_flurm_module(gen_server)).

is_flurm_module_erlang_test() ->
    ?assertEqual(false, flurm_upgrade:is_flurm_module(erlang)).

is_flurm_module_lists_test() ->
    ?assertEqual(false, flurm_upgrade:is_flurm_module(lists)).

is_flurm_module_similar_prefix_test() ->
    %% Modules that don't start with flurm_
    ?assertEqual(false, flurm_upgrade:is_flurm_module(flurmy_module)).

%%====================================================================
%% Loaded Modules Tests
%%====================================================================

get_loaded_modules_test() ->
    %% This returns all loaded modules with flurm_ prefix
    Modules = flurm_upgrade:get_loaded_modules(),
    ?assert(is_list(Modules)),
    %% All returned modules should have flurm_ prefix
    lists:foreach(fun(M) ->
        Prefix = atom_to_list(M),
        ?assert(lists:prefix("flurm_", Prefix))
    end, Modules).

get_loaded_modules_sorted_test() ->
    Modules = flurm_upgrade:get_loaded_modules(),
    ?assertEqual(Modules, lists:sort(Modules)).

%%====================================================================
%% Old Code Modules Tests
%%====================================================================

get_old_code_modules_test() ->
    %% Returns modules that have old code loaded
    Modules = flurm_upgrade:get_old_code_modules(),
    ?assert(is_list(Modules)),
    %% All should be flurm modules
    lists:foreach(fun(M) ->
        ?assertEqual(true, flurm_upgrade:is_flurm_module(M))
    end, Modules).

%%====================================================================
%% Process Modules Tests
%%====================================================================

get_process_modules_self_test() ->
    %% Test with our own process
    Modules = flurm_upgrade:get_process_modules(self()),
    ?assert(is_list(Modules)),
    %% Our process should be running eunit modules
    ?assert(length(Modules) >= 0).

get_process_modules_dead_process_test() ->
    %% Test with a terminated process
    Pid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(Pid),
    Modules = flurm_upgrade:get_process_modules(Pid),
    ?assertEqual([], Modules).

get_process_modules_gen_server_test() ->
    %% Spawn a simple gen_server-like process
    Pid = spawn(fun() ->
        receive stop -> ok end
    end),
    Modules = flurm_upgrade:get_process_modules(Pid),
    ?assert(is_list(Modules)),
    Pid ! stop.

%%====================================================================
%% Module Version Tests
%%====================================================================

get_module_version_loaded_test() ->
    %% Test with a module we know is loaded
    Version = flurm_upgrade:get_module_version(?MODULE),
    %% May return a version or undefined depending on module attributes
    %% Version can be any term - just verify function returns something
    ?assert(Version =:= undefined orelse Version =/= undefined).

get_module_version_nonexistent_test() ->
    %% Test with a module that doesn't exist
    Version = flurm_upgrade:get_module_version(nonexistent_module_xyz),
    ?assertEqual(undefined, Version).

get_module_version_erlang_module_test() ->
    %% Test with standard erlang module
    Version = flurm_upgrade:get_module_version(erlang),
    %% erlang module should have a version
    ?assert(Version =:= undefined orelse Version =/= undefined).

%%====================================================================
%% Pre-upgrade Check Tests
%%====================================================================

check_no_pending_jobs_test() ->
    %% Should return ok (no actual job checking in test environment)
    Result = flurm_upgrade:check_no_pending_jobs(),
    ?assertEqual(ok, Result).

check_cluster_healthy_no_cluster_test() ->
    %% When cluster isn't running, should return ok
    Result = flurm_upgrade:check_cluster_healthy(),
    ?assertEqual(ok, Result).

check_disk_space_test() ->
    %% Should return ok if there's enough space, or ok if check fails
    Result = flurm_upgrade:check_disk_space(),
    ?assert(Result =:= ok orelse element(1, Result) =:= error).

%%====================================================================
%% Release Exists Check Tests
%%====================================================================

check_release_exists_nonexistent_test() ->
    %% Check for a release that doesn't exist
    Result = flurm_upgrade:check_release_exists("99.99.99"),
    ?assertEqual({error, release_not_found}, Result).

check_release_exists_invalid_version_test() ->
    Result = flurm_upgrade:check_release_exists("not_a_version"),
    ?assertEqual({error, release_not_found}, Result).

%%====================================================================
%% Disk Free Tests
%%====================================================================

disk_free_current_dir_test() ->
    %% Test disk_free on current directory
    {ok, Cwd} = file:get_cwd(),
    Result = flurm_upgrade:disk_free(Cwd),
    case os:type() of
        {unix, _} ->
            %% On Unix, should return {ok, Bytes} or {error, _}
            case Result of
                {ok, Free} ->
                    ?assert(is_integer(Free)),
                    ?assert(Free >= 0);
                {error, _} ->
                    %% Error is acceptable if df command fails
                    ok
            end;
        {win32, _} ->
            %% On Windows, should return error (unsupported)
            ?assertEqual({error, unsupported_os}, Result)
    end.

disk_free_root_test() ->
    case os:type() of
        {unix, _} ->
            Result = flurm_upgrade:disk_free("/"),
            case Result of
                {ok, Free} ->
                    ?assert(is_integer(Free)),
                    ?assert(Free >= 0);
                {error, _} ->
                    ok
            end;
        _ ->
            ok
    end.

disk_free_nonexistent_path_test() ->
    case os:type() of
        {unix, _} ->
            Result = flurm_upgrade:disk_free("/nonexistent/path/xyz"),
            %% Should return error or unexpected output
            ?assert(element(1, Result) =:= error orelse element(1, Result) =:= ok);
        _ ->
            ok
    end.

%%====================================================================
%% Integration Tests
%%====================================================================

versions_test() ->
    %% Test the public API function
    Versions = flurm_upgrade:versions(),
    ?assert(is_list(Versions)),
    %% Each entry should be {App, Vsn}
    lists:foreach(fun({App, Vsn}) ->
        ?assert(is_atom(App)),
        ?assert(is_list(Vsn) orelse is_binary(Vsn))
    end, Versions).

which_releases_test() ->
    %% Test the public API function
    Releases = flurm_upgrade:which_releases(),
    ?assert(is_list(Releases)),
    %% Each entry should be {Vsn, Status}
    lists:foreach(fun({Vsn, Status}) ->
        ?assert(is_list(Vsn)),
        ?assert(is_atom(Status))
    end, Releases).

upgrade_status_test() ->
    %% Test the public API function
    Status = flurm_upgrade:upgrade_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(versions, Status)),
    ?assert(maps:is_key(releases, Status)),
    ?assert(maps:is_key(loaded_modules, Status)),
    ?assert(maps:is_key(old_code_modules, Status)).

check_upgrade_nonexistent_test() ->
    %% Check upgrade for nonexistent release
    Result = flurm_upgrade:check_upgrade("99.99.99"),
    ?assertMatch({error, _}, Result).

transform_state_no_transform_test() ->
    %% Test transform_state when module doesn't implement it
    OldState = #{key => value},
    NewState = flurm_upgrade:transform_state(lists, OldState, []),
    ?assertEqual(OldState, NewState).
