#!/usr/bin/env escript
%%% Test controller restart while jobs are running
%%% This test submits a job, restarts the controller, and verifies:
%%% 1. Node daemon reconnects after controller restart
%%% 2. Job state is handled appropriately (lost or recovered)
%%% 3. System continues to function after restart

-mode(compile).

main(_Args) ->
    io:format("=== Testing Controller Restart While Jobs Running ===~n~n"),

    %% Add code paths
    code:add_paths(filelib:wildcard("_build/default/lib/*/ebin")),

    %% Start required applications
    application:ensure_all_started(lager),
    lager:set_loglevel(lager_console_backend, error),

    io:format("1. Starting FLURM controller...~n"),
    {ok, _} = application:ensure_all_started(flurm_controller),
    timer:sleep(500),

    io:format("2. Starting FLURM node daemon...~n"),
    {ok, _} = application:ensure_all_started(flurm_node_daemon),
    timer:sleep(1000),

    %% Wait for node registration
    io:format("3. Waiting for node registration...~n"),
    case wait_for_node(10) of
        ok -> ok;
        error ->
            io:format("   FAILED: Node did not register~n"),
            halt(1)
    end,

    [Node|_] = flurm_node_manager:list_nodes(),
    Hostname = element(2, Node),
    io:format("   Node hostname: ~s~n", [Hostname]),

    io:format("4. Submitting long-running job (120 second sleep)...~n"),
    JobSpec = #{
        name => <<"restart_test_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        script => <<"#!/bin/bash\necho 'Job started at' $(date)\nfor i in $(seq 1 120); do\n  echo \"Running iteration $i at $(date)\"\n  sleep 1\ndone\necho 'Job completed at' $(date)\n">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 256,
        time_limit => 300
    },

    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),
    io:format("   Job submitted with ID: ~p~n", [JobId]),

    %% Wait for job to start running
    io:format("5. Waiting for job to start running...~n"),
    wait_for_job_state(JobId, running, 10),

    {ok, Job1} = flurm_job_manager:get_job(JobId),
    State1 = element(6, Job1),
    io:format("   Job state before restart: ~p~n", [State1]),

    %% Let the job run for a bit
    io:format("6. Letting job run for 3 seconds...~n"),
    timer:sleep(3000),

    io:format("7. Checking node daemon connection state before restart...~n"),
    ConnStateBefore = flurm_controller_connector:get_state(),
    io:format("   Node daemon connected: ~p~n", [maps:get(connected, ConnStateBefore)]),
    io:format("   Node daemon registered: ~p~n", [maps:get(registered, ConnStateBefore)]),

    io:format("~n8. RESTARTING CONTROLLER...~n"),
    io:format("   Stopping flurm_controller application...~n"),
    ok = application:stop(flurm_controller),
    io:format("   Controller stopped~n"),

    %% Brief pause
    timer:sleep(1000),

    io:format("   Starting flurm_controller application...~n"),
    {ok, _} = application:ensure_all_started(flurm_controller),
    io:format("   Controller restarted~n"),

    %% Wait for things to stabilize
    timer:sleep(2000),

    io:format("~n9. Checking state after controller restart...~n"),

    %% Check if node daemon reconnected
    io:format("   a) Checking node daemon reconnection...~n"),
    timer:sleep(1000),  % Give time for reconnection
    ConnStateAfter = flurm_controller_connector:get_state(),
    io:format("      Node daemon connected: ~p~n", [maps:get(connected, ConnStateAfter)]),
    io:format("      Node daemon registered: ~p~n", [maps:get(registered, ConnStateAfter)]),

    %% Check if nodes are registered with new controller
    io:format("   b) Checking node registration with controller...~n"),
    case wait_for_node(10) of
        ok ->
            Nodes = flurm_node_manager:list_nodes(),
            io:format("      Registered nodes: ~p~n", [length(Nodes)]),
            lists:foreach(fun(N) ->
                NH = element(2, N),
                NS = element(5, N),
                io:format("      - ~s: state=~p~n", [NH, NS])
            end, Nodes);
        error ->
            io:format("      WARNING: No nodes registered after restart~n")
    end,

    %% Check job state - it will likely be lost since we don't persist state
    io:format("   c) Checking job state...~n"),
    case flurm_job_manager:get_job(JobId) of
        {ok, Job2} ->
            State2 = element(6, Job2),
            io:format("      Job ~p state: ~p (state preserved!)~n", [JobId, State2]);
        {error, not_found} ->
            io:format("      Job ~p not found (state lost - expected without persistence)~n", [JobId])
    end,

    %% List all jobs
    AllJobs = flurm_job_manager:list_jobs(),
    io:format("      Total jobs in manager: ~p~n", [length(AllJobs)]),

    io:format("~n10. Testing job submission after restart...~n"),
    JobSpec2 = #{
        name => <<"post_restart_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        script => <<"#!/bin/bash\necho 'Post-restart job running'\nsleep 5\necho 'Done'\n">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 256,
        time_limit => 60
    },

    case flurm_job_manager:submit_job(JobSpec2) of
        {ok, JobId2} ->
            io:format("    New job submitted with ID: ~p~n", [JobId2]),
            %% Wait for it to complete
            io:format("    Waiting for new job to complete...~n"),
            FinalState = wait_for_job_terminal_state(JobId2, 15),
            io:format("    New job final state: ~p~n", [FinalState]),
            case FinalState of
                completed ->
                    io:format("    SUCCESS: System functional after restart~n");
                failed ->
                    io:format("    Job failed (check if node reconnected)~n");
                _ ->
                    io:format("    Job state: ~p~n", [FinalState])
            end;
        {error, Reason} ->
            io:format("    FAILED to submit job: ~p~n", [Reason])
    end,

    io:format("~n=== Summary ===~n"),
    io:format("1. Controller restart: SUCCESS~n"),
    case maps:get(connected, ConnStateAfter) of
        true -> io:format("2. Node daemon reconnection: SUCCESS~n");
        false -> io:format("2. Node daemon reconnection: FAILED~n")
    end,
    case flurm_job_manager:get_job(JobId) of
        {ok, _} -> io:format("3. Job state persistence: YES (state recovered)~n");
        {error, not_found} -> io:format("3. Job state persistence: NO (expected - in-memory only)~n")
    end,

    %% Cleanup
    io:format("~n11. Cleaning up...~n"),
    application:stop(flurm_node_daemon),
    application:stop(flurm_controller),

    io:format("~nTest complete!~n"),
    halt(0).

wait_for_node(0) ->
    io:format("   Timeout waiting for node~n"),
    error;
wait_for_node(Attempts) ->
    case flurm_node_manager:list_nodes() of
        [] ->
            timer:sleep(500),
            wait_for_node(Attempts - 1);
        [Node|_] ->
            Hostname = element(2, Node),
            io:format("   Node registered: ~p~n", [Hostname]),
            ok
    end.

wait_for_job_state(_JobId, _TargetState, 0) ->
    io:format("   Timeout waiting for job state~n"),
    timeout;
wait_for_job_state(JobId, TargetState, Attempts) ->
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            State = element(6, Job),
            case State of
                TargetState ->
                    io:format("   Job reached state: ~p~n", [State]),
                    ok;
                _ ->
                    timer:sleep(500),
                    wait_for_job_state(JobId, TargetState, Attempts - 1)
            end;
        {error, not_found} ->
            timer:sleep(500),
            wait_for_job_state(JobId, TargetState, Attempts - 1)
    end.

wait_for_job_terminal_state(JobId, Attempts) ->
    wait_for_job_terminal_state(JobId, Attempts, pending).

wait_for_job_terminal_state(_JobId, 0, LastState) ->
    LastState;
wait_for_job_terminal_state(JobId, Attempts, _LastState) ->
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            State = element(6, Job),
            case State of
                completed -> completed;
                failed -> failed;
                timeout -> timeout;
                cancelled -> cancelled;
                node_fail -> node_fail;
                _ ->
                    timer:sleep(1000),
                    wait_for_job_terminal_state(JobId, Attempts - 1, State)
            end;
        {error, not_found} ->
            not_found
    end.
