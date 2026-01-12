%%%-------------------------------------------------------------------
%%% @doc FLURM Accounting Helper Module
%%%
%%% Provides safe wrappers around flurm_dbd_server accounting calls.
%%% All calls are wrapped in try/catch to ensure accounting failures
%%% do not crash job processes.
%%%
%%% This module is called from flurm_job state machine at key lifecycle
%%% points to record accounting events.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_accounting).

-include("flurm_core.hrl").

%% API
-export([
    record_job_submit/1,
    record_job_start/2,
    record_job_end/3,
    record_job_cancelled/2
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Record job submission event.
%% Called when a job is submitted and enters pending state.
%% Safely wraps the call to prevent crashes on accounting failures.
-spec record_job_submit(#job_data{}) -> ok.
record_job_submit(#job_data{} = JobData) ->
    safe_call(fun() ->
        JobMap = job_data_to_map(JobData),
        flurm_dbd_server:record_job_submit(JobMap)
    end, "record_job_submit", JobData#job_data.job_id).

%% @doc Record job start event.
%% Called when a job transitions to running state.
%% Safely wraps the call to prevent crashes on accounting failures.
-spec record_job_start(job_id(), [binary()]) -> ok.
record_job_start(JobId, AllocatedNodes) ->
    safe_call(fun() ->
        flurm_dbd_server:record_job_start(JobId, AllocatedNodes)
    end, "record_job_start", JobId).

%% @doc Record job completion event.
%% Called when a job completes (successfully or with failure).
%% Safely wraps the call to prevent crashes on accounting failures.
-spec record_job_end(job_id(), integer(), atom()) -> ok.
record_job_end(JobId, ExitCode, FinalState) ->
    safe_call(fun() ->
        flurm_dbd_server:record_job_end(JobId, ExitCode, FinalState)
    end, "record_job_end", JobId).

%% @doc Record job cancellation event.
%% Called when a job is cancelled.
%% Safely wraps the call to prevent crashes on accounting failures.
-spec record_job_cancelled(job_id(), atom()) -> ok.
record_job_cancelled(JobId, Reason) ->
    safe_call(fun() ->
        flurm_dbd_server:record_job_cancelled(JobId, Reason)
    end, "record_job_cancelled", JobId).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Safely execute an accounting call, catching any exceptions.
%% Returns ok regardless of success or failure to prevent
%% accounting issues from affecting job execution.
-spec safe_call(fun(() -> ok), string(), job_id()) -> ok.
safe_call(Fun, Operation, JobId) ->
    try
        Fun()
    catch
        Class:Reason:Stacktrace ->
            %% Log the error but don't crash
            error_logger:warning_msg(
                "Accounting ~s failed for job ~p: ~p:~p~n  Stack: ~p",
                [Operation, JobId, Class, Reason, Stacktrace]),
            ok
    end.

%% @private
%% Convert job_data record to a map for the dbd server.
-spec job_data_to_map(#job_data{}) -> map().
job_data_to_map(#job_data{} = Data) ->
    SubmitTime = case Data#job_data.submit_time of
        undefined -> erlang:system_time(second);
        {MegaSecs, Secs, _MicroSecs} -> MegaSecs * 1000000 + Secs;
        T when is_integer(T) -> T
    end,
    #{
        job_id => Data#job_data.job_id,
        user_id => Data#job_data.user_id,
        group_id => Data#job_data.group_id,
        partition => Data#job_data.partition,
        num_nodes => Data#job_data.num_nodes,
        num_cpus => Data#job_data.num_cpus,
        time_limit => Data#job_data.time_limit,
        submit_time => SubmitTime,
        priority => Data#job_data.priority
    }.
