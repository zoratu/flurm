#!/usr/bin/env escript
%%% Test abrupt node crash (simulating power failure or kernel panic)
%%% This test submits a job, then kills the node daemon's acceptor process
%%% to simulate an abrupt crash where no cleanup messages can be sent.

-mode(compile).

main(_Args) ->
    io:format("=== Testing Abrupt Node Crash (No Cleanup Messages) ===~n~n"),

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

    [Node|_] = flurm_node_manager:list_nodes(),
    Hostname = element(2, Node),
    io:format("   Node hostname: ~s~n", [Hostname]),

    io:format("4. Submitting long-running job...~n"),
    JobSpec = #{
        name => <<"crash_test_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        script => <<"#!/bin/bash\necho 'Starting job...'\nfor i in $(seq 1 60); do echo \"Running... $i\"; sleep 1; done\n">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 256,
        time_limit => 120
    },

    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),
    io:format("   Job submitted with ID: ~p~n", [JobId]),

    %% Wait for job to start running
    io:format("5. Waiting for job to start running...~n"),
    wait_for_job_state(JobId, running, 10),

    {ok, Job1} = flurm_job_manager:get_job(JobId),
    State1 = element(6, Job1),
    io:format("   Job state: ~p~n", [State1]),

    %% Give the job a moment
    timer:sleep(2000),

    io:format("6. Simulating abrupt node crash (brutally killing node daemon processes)...~n"),

    %% Find and kill the node daemon's controller_connector process
    %% This simulates a crash where no cleanup messages can be sent
    ConnectorPid = whereis(flurm_controller_connector),
    io:format("   Controller connector PID: ~p~n", [ConnectorPid]),

    %% Also find the job executor
    ExecutorSup = whereis(flurm_job_executor_sup),
    io:format("   Job executor supervisor PID: ~p~n", [ExecutorSup]),

    %% Kill all node daemon processes brutally (like kill -9)
    %% This prevents any cleanup/termination messages from being sent
    case ConnectorPid of
        undefined -> ok;
        _ -> exit(ConnectorPid, kill)
    end,
    case ExecutorSup of
        undefined -> ok;
        _ -> exit(ExecutorSup, kill)
    end,
    io:format("   Node daemon processes killed abruptly~n"),

    %% Wait for controller to detect the crash
    io:format("7. Waiting for controller to detect crash...~n"),
    timer:sleep(2000),

    %% Check node state
    io:format("8. Checking node state...~n"),
    case flurm_node_manager:list_nodes() of
        [] ->
            io:format("   No nodes registered~n");
        NodesAfter ->
            lists:foreach(fun(N) ->
                NH = element(2, N),
                NS = element(5, N),
                io:format("   - ~s: state=~p~n", [NH, NS])
            end, NodesAfter)
    end,

    %% Check job state
    io:format("9. Checking job state...~n"),
    FinalState = wait_for_job_terminal_state(JobId, 10),

    io:format("~n=== Results ===~n"),
    io:format("Job final state: ~p~n", [FinalState]),

    case FinalState of
        node_fail ->
            io:format("~nSUCCESS: Job correctly marked as node_fail after abrupt crash~n");
        failed ->
            io:format("~nPARTIAL: Job marked as failed (acceptable for crash)~n");
        running ->
            io:format("~nFAILED: Job still running - crash not detected~n");
        _ ->
            io:format("~nJob state: ~p~n", [FinalState])
    end,

    %% Cleanup
    io:format("~n10. Cleaning up...~n"),
    application:stop(flurm_node_daemon),
    application:stop(flurm_controller),

    io:format("~nTest complete!~n"),
    halt(0).

wait_for_node(0) ->
    io:format("   Warning: Node not registered~n"),
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
    timeout;
wait_for_job_state(JobId, TargetState, Attempts) ->
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            State = element(6, Job),
            case State of
                TargetState -> ok;
                _ ->
                    timer:sleep(500),
                    wait_for_job_state(JobId, TargetState, Attempts - 1)
            end;
        _ ->
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
                    State
            end;
        {error, not_found} ->
            not_found
    end.
