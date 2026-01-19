%%%-------------------------------------------------------------------
%%% @doc Test suite for flurm_node_daemon_app internal functions
%%%
%%% These tests exercise the internal helper functions exported via
%%% -ifdef(TEST) to achieve code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_app_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup
%%====================================================================

setup() ->
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

node_daemon_app_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"cleanup_orphaned_scripts cleans temp files", fun cleanup_orphaned_scripts_test/0},
      {"cleanup_orphaned_cgroups runs without error", fun cleanup_orphaned_cgroups_test/0},
      {"cleanup_orphaned_resources runs without error", fun cleanup_orphaned_resources_test/0},
      {"cleanup_cgroup with nonexistent path", fun cleanup_cgroup_nonexistent_test/0},
      {"handle_restored_state with empty state", fun handle_restored_state_empty_test/0},
      {"handle_restored_state with orphaned jobs", fun handle_restored_state_orphaned_test/0}
     ]}.

%%====================================================================
%% Tests
%%====================================================================

cleanup_orphaned_scripts_test() ->
    %% Create some test script files that look like orphaned flurm jobs
    TestScripts = [
        "/tmp/flurm_job_999001.sh",
        "/tmp/flurm_job_999002.sh"
    ],

    %% Create the files
    lists:foreach(fun(Path) ->
        file:write_file(Path, <<"#!/bin/bash\necho test\n">>)
    end, TestScripts),

    %% Verify they exist
    lists:foreach(fun(Path) ->
        ?assert(filelib:is_regular(Path))
    end, TestScripts),

    %% Run cleanup
    Result = flurm_node_daemon_app:cleanup_orphaned_scripts(),
    ?assertEqual(ok, Result),

    %% Verify files are removed
    lists:foreach(fun(Path) ->
        ?assertNot(filelib:is_regular(Path))
    end, TestScripts).

cleanup_orphaned_cgroups_test() ->
    %% This function should complete without error
    %% On non-Linux or without proper permissions, it just returns ok
    Result = flurm_node_daemon_app:cleanup_orphaned_cgroups(),
    ?assertEqual(ok, Result).

cleanup_orphaned_resources_test() ->
    %% Should complete without error
    Result = flurm_node_daemon_app:cleanup_orphaned_resources(),
    ?assertEqual(ok, Result).

cleanup_cgroup_nonexistent_test() ->
    %% Cleaning up a nonexistent cgroup should not crash
    %% The function reads cgroup.procs and deletes the directory
    %% With a nonexistent path, returns error
    Result = flurm_node_daemon_app:cleanup_cgroup("/nonexistent/cgroup/path"),
    ?assert(Result =:= ok orelse Result =:= {error, enoent}).

handle_restored_state_empty_test() ->
    %% Clear any existing state
    flurm_state_persistence:clear_state(),

    %% Handle a state with no orphaned jobs
    State = #{
        running_jobs => 0,
        saved_at => erlang:system_time(millisecond) - 1000
    },

    Result = flurm_node_daemon_app:handle_restored_state(State),
    ?assertEqual(ok, Result).

handle_restored_state_orphaned_test() ->
    %% Clear any existing state
    flurm_state_persistence:clear_state(),

    %% Handle a state with orphaned jobs
    State = #{
        running_jobs => 3,
        saved_at => erlang:system_time(millisecond) - 5000
    },

    Result = flurm_node_daemon_app:handle_restored_state(State),
    ?assertEqual(ok, Result).

%%====================================================================
%% save_current_state requires running processes
%% We can't easily test it without the full application running
%%====================================================================
