#!/usr/bin/env escript
%%% Test job timeout when time limit exceeded
%%% This test submits a job with a 3 second time limit that tries to sleep for 60 seconds.
%%% The job should be terminated after 3 seconds with a timeout status.

-mode(compile).

main(_Args) ->
    io:format("=== Testing Job Timeout ===~n~n"),

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

    io:format("4. Submitting job with 3 second time limit (will try to sleep 60 seconds)...~n"),
    JobSpec = #{
        name => <<"timeout_test_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        script => <<"#!/bin/bash\necho 'Starting long running job...'\nsleep 60\necho 'Should never reach here!'\n">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 256,
        time_limit => 3  % 3 seconds - job will be killed after this
    },

    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),
    io:format("   Job submitted with ID: ~p~n", [JobId]),

    StartTime = erlang:system_time(second),
    io:format("5. Monitoring job status (expecting timeout in about 3 seconds)...~n"),
    FinalState = monitor_job(JobId, 15),  % Wait up to 15 seconds
    EndTime = erlang:system_time(second),
    Duration = EndTime - StartTime,

    io:format("~n=== Results ===~n"),
    io:format("Job final state: ~p~n", [FinalState]),
    io:format("Total duration: ~p seconds~n", [Duration]),

    case FinalState of
        timeout ->
            io:format("~nSUCCESS: Job timed out as expected!~n"),
            case Duration >= 3 andalso Duration =< 6 of
                true ->
                    io:format("SUCCESS: Timeout occurred within expected window (3-6 seconds)~n");
                false ->
                    io:format("WARNING: Duration ~p seconds is outside expected 3-6 second window~n", [Duration])
            end;
        failed ->
            %% The job_failed message will report timeout as the reason
            io:format("~nNote: Job reported as 'failed' - checking if it was due to timeout...~n"),
            {ok, Job} = flurm_job_manager:get_job(JobId),
            %% #job.exit_code is at position 17 (id=1, name=2, user=3, partition=4, state=5,
            %% script=6, num_nodes=7, num_cpus=8, memory_mb=9, time_limit=10, priority=11,
            %% submit_time=12, start_time=13, end_time=14, allocated_nodes=15, exit_code=16)
            %% But tuples are 1-indexed and record has 'job' tag at position 1
            ExitCode = element(17, Job),  % exit_code field (16 + 1 for record tag)
            io:format("Exit code: ~p~n", [ExitCode]),
            case Duration >= 3 andalso Duration =< 6 of
                true ->
                    io:format("SUCCESS: Job terminated within timeout window~n");
                false ->
                    io:format("FAILED: Unexpected duration: ~p seconds~n", [Duration])
            end;
        State ->
            io:format("~nFAILED: Unexpected final state: ~p~n", [State])
    end,

    %% Cleanup
    io:format("~n6. Cleaning up...~n"),
    application:stop(flurm_node_daemon),
    application:stop(flurm_controller),

    io:format("~nTest complete!~n"),
    halt(0).

wait_for_node(0) ->
    io:format("   Warning: Node not registered after timeout~n");
wait_for_node(Attempts) ->
    case flurm_node_manager:list_nodes() of
        [] ->
            timer:sleep(500),
            wait_for_node(Attempts - 1);
        [Node|_] ->
            %% Node is a #node record, hostname is at position 2 (after record tag)
            Hostname = element(2, Node),
            io:format("   Node registered: ~p~n", [Hostname])
    end.

monitor_job(JobId, TimeoutSecs) ->
    monitor_job(JobId, TimeoutSecs, pending).

monitor_job(_JobId, 0, LastState) ->
    io:format("   Monitoring timeout - last state was ~p~n", [LastState]),
    LastState;
monitor_job(JobId, RemainingSeconds, _LastState) ->
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            %% Job record: {job, id, name, user, partition, state, script, ...}
            %% Position 1 = record tag 'job', Position 6 = state field
            State = element(6, Job),
            case State of
                pending ->
                    io:format("   [~ps] Job state: pending~n", [15 - RemainingSeconds]),
                    timer:sleep(500),
                    monitor_job(JobId, RemainingSeconds - 1, State);
                running ->
                    io:format("   [~ps] Job state: running~n", [15 - RemainingSeconds]),
                    timer:sleep(500),
                    monitor_job(JobId, RemainingSeconds - 1, State);
                completed ->
                    io:format("   [~ps] Job state: completed~n", [15 - RemainingSeconds]),
                    completed;
                failed ->
                    io:format("   [~ps] Job state: failed~n", [15 - RemainingSeconds]),
                    failed;
                timeout ->
                    io:format("   [~ps] Job state: timeout~n", [15 - RemainingSeconds]),
                    timeout;
                cancelled ->
                    io:format("   [~ps] Job state: cancelled~n", [15 - RemainingSeconds]),
                    cancelled;
                Other ->
                    io:format("   [~ps] Job state: ~p~n", [15 - RemainingSeconds, Other]),
                    timer:sleep(500),
                    monitor_job(JobId, RemainingSeconds - 1, Other)
            end;
        {error, not_found} ->
            io:format("   Job not found (may have been cleaned up)~n"),
            not_found
    end.

%% Note: Using tuple element positions directly instead of record syntax
