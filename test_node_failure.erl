#!/usr/bin/env escript
%%% Test node failure while job is running
%%% This test submits a job, then kills the node daemon to simulate node failure.
%%% The controller should detect the disconnection and mark the job as failed.

-mode(compile).

main(_Args) ->
    io:format("=== Testing Node Failure While Job Running ===~n~n"),

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
    wait_for_node(10),

    %% Get node info before submitting job
    [Node|_] = flurm_node_manager:list_nodes(),
    Hostname = element(2, Node),
    io:format("   Node hostname: ~s~n", [Hostname]),

    io:format("4. Submitting long-running job (60 second sleep)...~n"),
    JobSpec = #{
        name => <<"node_failure_test_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        script => <<"#!/bin/bash\necho 'Starting long running job...'\nfor i in $(seq 1 60); do echo \"Running... $i\"; sleep 1; done\necho 'Done'\n">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 256,
        time_limit => 120  % Long time limit so it doesn't timeout
    },

    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),
    io:format("   Job submitted with ID: ~p~n", [JobId]),

    %% Wait for job to start running
    io:format("5. Waiting for job to start running...~n"),
    wait_for_job_state(JobId, running, 10),

    %% Verify job is running
    {ok, Job1} = flurm_job_manager:get_job(JobId),
    State1 = element(6, Job1),
    io:format("   Job state: ~p~n", [State1]),

    case State1 of
        running ->
            io:format("   Job is running, proceeding with node failure simulation~n");
        _ ->
            io:format("   ERROR: Job not in running state, cannot proceed~n"),
            halt(1)
    end,

    %% Give the job a moment to produce some output
    timer:sleep(2000),

    io:format("6. Simulating node failure (stopping node daemon)...~n"),
    %% Stop the node daemon application - this will close the TCP connection
    application:stop(flurm_node_daemon),
    io:format("   Node daemon stopped~n"),

    %% Wait for controller to detect disconnection and update job state
    io:format("7. Waiting for controller to detect node failure...~n"),
    timer:sleep(1000),

    %% Check if node is marked as down
    io:format("8. Checking node state...~n"),
    case flurm_node_manager:list_nodes() of
        [] ->
            io:format("   Node removed from list (expected after disconnect)~n");
        Nodes ->
            io:format("   Nodes still registered: ~p~n", [length(Nodes)]),
            lists:foreach(fun(N) ->
                NH = element(2, N),
                NS = element(5, N),
                io:format("   - ~s: state=~p~n", [NH, NS])
            end, Nodes)
    end,

    %% Check job state - should be failed or node_fail
    io:format("9. Checking job state...~n"),
    FinalState = wait_for_job_terminal_state(JobId, 10),

    io:format("~n=== Results ===~n"),
    io:format("Job final state: ~p~n", [FinalState]),

    %% Re-check node state after failure handling
    io:format("~nNode state after failure:~n"),
    case flurm_node_manager:list_nodes() of
        [] ->
            io:format("  No nodes registered~n");
        NodesAfter ->
            lists:foreach(fun(N) ->
                NH = element(2, N),
                NS = element(5, N),
                io:format("  - ~s: state=~p~n", [NH, NS])
            end, NodesAfter)
    end,

    case FinalState of
        node_fail ->
            io:format("~nSUCCESS: Job marked as node_fail after node failure (expected)~n");
        failed ->
            io:format("~nPARTIAL SUCCESS: Job marked as failed (node_fail would be more specific)~n");
        running ->
            io:format("~nFAILED: Job still shows as running - failure detection not working~n");
        State ->
            io:format("~nJob in state: ~p~n", [State])
    end,

    %% Get final job details
    case flurm_job_manager:get_job(JobId) of
        {ok, FinalJob} ->
            io:format("~nFinal job details:~n"),
            io:format("  State: ~p~n", [element(6, FinalJob)]),
            io:format("  Exit code: ~p~n", [element(17, FinalJob)]),
            io:format("  End time: ~p~n", [element(15, FinalJob)]);
        {error, not_found} ->
            io:format("~nJob not found (may have been cleaned up)~n")
    end,

    %% Cleanup
    io:format("~n10. Cleaning up...~n"),
    application:stop(flurm_controller),

    io:format("~nTest complete!~n"),
    halt(0).

wait_for_node(0) ->
    io:format("   Warning: Node not registered after timeout~n"),
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
    wait_for_job_terminal_state(JobId, Attempts, running).

wait_for_job_terminal_state(_JobId, 0, LastState) ->
    LastState;
wait_for_job_terminal_state(JobId, Attempts, _LastState) ->
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            State = element(6, Job),
            case State of
                running ->
                    io:format("   [~p] Job still running...~n", [10 - Attempts + 1]),
                    timer:sleep(1000),
                    wait_for_job_terminal_state(JobId, Attempts - 1, State);
                pending ->
                    timer:sleep(500),
                    wait_for_job_terminal_state(JobId, Attempts - 1, State);
                _ ->
                    %% Terminal state reached
                    State
            end;
        {error, not_found} ->
            not_found
    end.
