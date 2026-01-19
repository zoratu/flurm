%%%-------------------------------------------------------------------
%%% @doc Test runner for FLURM job lifecycle
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_test_runner).

-include_lib("flurm_core/include/flurm_core.hrl").

-export([run/0]).

%% Test exports for internal functions
%% Note: These functions require external processes (flurm_job_manager,
%% flurm_node_connection_manager) so they cannot be unit tested in isolation.
%% They are exported for integration testing purposes.
-ifdef(TEST).
-export([
    run_job_test/0,
    monitor_job/2
]).
-endif.

run() ->
    io:format("~n=== Testing Full Job Lifecycle ===~n"),

    %% Give the node daemon time to connect
    timer:sleep(2000),

    %% Check connected nodes
    ConnectedNodes = flurm_node_connection_manager:list_connected_nodes(),
    io:format("Connected nodes: ~p~n", [ConnectedNodes]),

    case ConnectedNodes of
        [] ->
            io:format("ERROR: No nodes connected~n"),
            {error, no_nodes};
        _ ->
            run_job_test()
    end.

run_job_test() ->
    io:format("~nSubmitting job...~n"),
    JobSpec = #{
        name => <<"test_job">>,
        script => <<"#!/bin/bash\necho Hello from FLURM\nsleep 2\necho Done">>,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 512,
        time_limit => 60
    },

    case flurm_job_manager:submit_job(JobSpec) of
        {ok, JobId} ->
            io:format("Job ~p submitted~n", [JobId]),
            monitor_job(JobId, 20);  % 20 iterations = 10 seconds max
        {error, Reason} ->
            io:format("Submit error: ~p~n", [Reason]),
            {error, Reason}
    end.

monitor_job(_JobId, 0) ->
    io:format("~nTimeout waiting for job~n"),
    {error, timeout};
monitor_job(JobId, Remaining) ->
    timer:sleep(500),
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            State = Job#job.state,
            io:format("  Job ~p state: ~p~n", [JobId, State]),
            case State of
                completed ->
                    io:format("~n=== RESULT ===~n"),
                    io:format("Job ID: ~p~n", [JobId]),
                    io:format("Job state: ~p~n", [State]),
                    io:format("Allocated nodes: ~p~n", [Job#job.allocated_nodes]),
                    io:format("Exit code: ~p~n", [Job#job.exit_code]),
                    io:format("~nSUCCESS: Job completed!~n"),
                    ok;
                failed ->
                    io:format("~nFAILED: Job failed~n"),
                    io:format("Exit code: ~p~n", [Job#job.exit_code]),
                    {error, job_failed};
                _ ->
                    monitor_job(JobId, Remaining - 1)
            end;
        {error, E} ->
            io:format("Job lookup error: ~p~n", [E]),
            {error, E}
    end.
