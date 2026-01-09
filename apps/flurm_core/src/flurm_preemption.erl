%%%-------------------------------------------------------------------
%%% @doc FLURM Job Preemption
%%%
%%% Implements job preemption to allow high-priority jobs to obtain
%%% resources from lower-priority running jobs.
%%%
%%% Preemption modes:
%%% - requeue: Stop job and put back in pending queue
%%% - cancel: Terminate job completely
%%% - checkpoint: Save job state before stopping (if supported)
%%% - suspend: Suspend job (keep in memory, don't release resources)
%%%
%%% Preemption is triggered when:
%%% 1. High-priority job cannot get resources
%%% 2. QOS preemption rules are configured
%%% 3. Partition preemption is enabled
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_preemption).

-include("flurm_core.hrl").

%% API
-export([
    check_preemption/1,
    preempt_jobs/2,
    can_preempt/2,
    get_preemptable_jobs/2,
    set_preemption_mode/1,
    get_preemption_mode/0,
    set_grace_time/1,
    get_grace_time/0
]).

%% Types
-type preemption_mode() :: requeue | cancel | checkpoint | suspend | off.
-type preemption_result() :: {ok, [pos_integer()]} | {error, term()}.

-export_type([preemption_mode/0, preemption_result/0]).

%% Configuration (could be moved to ETS/config)
-define(DEFAULT_PREEMPTION_MODE, requeue).
-define(DEFAULT_GRACE_TIME, 60).  % Seconds to allow graceful shutdown
-define(PREEMPTION_CONFIG, flurm_preemption_config).

%%====================================================================
%% API
%%====================================================================

%% @doc Check if preemption should occur for a pending job.
%% Returns list of jobs that would need to be preempted.
-spec check_preemption(map()) -> {ok, [map()]} | {error, no_preemption_needed}.
check_preemption(PendingJob) ->
    Mode = get_preemption_mode(),

    case Mode of
        off ->
            {error, preemption_disabled};
        _ ->
            %% Get resource requirements
            NumNodes = maps:get(num_nodes, PendingJob, 1),
            NumCpus = maps:get(num_cpus, PendingJob, 1),
            MemoryMb = maps:get(memory_mb, PendingJob, 1024),
            Priority = maps:get(priority, PendingJob, ?DEFAULT_PRIORITY),

            %% Find jobs that could be preempted
            Candidates = get_preemptable_jobs(PendingJob, Priority),

            %% Check if preempting candidates provides enough resources
            case find_preemption_set(Candidates, NumNodes, NumCpus, MemoryMb) of
                {ok, ToPreempt} when length(ToPreempt) > 0 ->
                    {ok, ToPreempt};
                _ ->
                    {error, no_preemption_needed}
            end
    end.

%% @doc Execute preemption on a list of jobs.
%% Stops the jobs according to the preemption mode.
-spec preempt_jobs([map()], preemption_mode()) -> preemption_result().
preempt_jobs(Jobs, Mode) ->
    GraceTime = get_grace_time(),

    Results = lists:map(
        fun(Job) ->
            JobId = maps:get(job_id, Job),
            JobPid = maps:get(pid, Job, undefined),
            preempt_single_job(JobId, JobPid, Mode, GraceTime)
        end,
        Jobs
    ),

    %% Collect successfully preempted job IDs
    Preempted = [JobId || {ok, JobId} <- Results],
    {ok, Preempted}.

%% @doc Check if one job can preempt another.
%% Returns true if Preemptor has higher priority than Preemptee.
-spec can_preempt(map(), map()) -> boolean().
can_preempt(Preemptor, Preemptee) ->
    PreemptorPriority = maps:get(priority, Preemptor, ?DEFAULT_PRIORITY),
    PreemptorQOS = maps:get(qos, Preemptor, <<"normal">>),

    PreempteePriority = maps:get(priority, Preemptee, ?DEFAULT_PRIORITY),
    PreempteeQOS = maps:get(qos, Preemptee, <<"normal">>),

    %% Check QOS-based preemption first
    case {PreemptorQOS, PreempteeQOS} of
        {<<"high">>, <<"low">>} -> true;
        {<<"high">>, <<"normal">>} -> true;
        {<<"normal">>, <<"low">>} -> true;
        _ ->
            %% Fall back to priority comparison
            %% Require significant priority difference to preempt
            PreemptorPriority > PreempteePriority + 1000
    end.

%% @doc Get list of jobs that could potentially be preempted.
%% Returns jobs with lower priority than the threshold.
-spec get_preemptable_jobs(map(), integer()) -> [map()].
get_preemptable_jobs(PendingJob, MinPriority) ->
    %% Get all running jobs
    RunningJobs = flurm_job_registry:list_jobs_by_state(running),

    %% Get job info and filter by priority
    lists:filtermap(
        fun({JobId, Pid}) ->
            case catch flurm_job:get_info(Pid) of
                {ok, Info} ->
                    JobPriority = maps:get(priority, Info, ?DEFAULT_PRIORITY),
                    case JobPriority < MinPriority andalso
                         can_preempt(PendingJob, Info) of
                        true ->
                            {true, Info#{job_id => JobId, pid => Pid}};
                        false ->
                            false
                    end;
                _ ->
                    false
            end
        end,
        RunningJobs
    ).

%% @doc Set the preemption mode.
-spec set_preemption_mode(preemption_mode()) -> ok.
set_preemption_mode(Mode) when Mode =:= requeue;
                                Mode =:= cancel;
                                Mode =:= checkpoint;
                                Mode =:= suspend;
                                Mode =:= off ->
    ensure_config_table(),
    ets:insert(?PREEMPTION_CONFIG, {mode, Mode}),
    ok.

%% @doc Get the current preemption mode.
-spec get_preemption_mode() -> preemption_mode().
get_preemption_mode() ->
    ensure_config_table(),
    case ets:lookup(?PREEMPTION_CONFIG, mode) of
        [{mode, Mode}] -> Mode;
        [] -> ?DEFAULT_PREEMPTION_MODE
    end.

%% @doc Set the grace time for preemption (seconds).
-spec set_grace_time(pos_integer()) -> ok.
set_grace_time(Seconds) when is_integer(Seconds), Seconds > 0 ->
    ensure_config_table(),
    ets:insert(?PREEMPTION_CONFIG, {grace_time, Seconds}),
    ok.

%% @doc Get the grace time for preemption.
-spec get_grace_time() -> pos_integer().
get_grace_time() ->
    ensure_config_table(),
    case ets:lookup(?PREEMPTION_CONFIG, grace_time) of
        [{grace_time, Time}] -> Time;
        [] -> ?DEFAULT_GRACE_TIME
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
ensure_config_table() ->
    case ets:whereis(?PREEMPTION_CONFIG) of
        undefined ->
            ets:new(?PREEMPTION_CONFIG, [named_table, public, set]);
        _ ->
            ok
    end.

%% @private
%% Find a set of jobs to preempt that provides needed resources.
%% Uses a greedy algorithm - preempt lowest priority first.
find_preemption_set(Candidates, NumNodes, NumCpus, MemoryMb) ->
    %% Sort by priority (ascending - lowest priority first)
    Sorted = lists:sort(
        fun(A, B) ->
            maps:get(priority, A, 0) < maps:get(priority, B, 0)
        end,
        Candidates
    ),

    %% Accumulate until we have enough resources
    find_sufficient_set(Sorted, NumNodes, NumCpus, MemoryMb, [], 0, 0, 0).

%% @private
find_sufficient_set(_Jobs, NeededNodes, _NeededCpus, _NeededMem,
                    Acc, AccNodes, _AccCpus, _AccMem)
  when AccNodes >= NeededNodes ->
    {ok, Acc};

find_sufficient_set([], _NeededNodes, _NeededCpus, _NeededMem,
                    _Acc, _AccNodes, _AccCpus, _AccMem) ->
    {error, insufficient_preemptable_resources};

find_sufficient_set([Job | Rest], NeededNodes, NeededCpus, NeededMem,
                    Acc, AccNodes, AccCpus, AccMem) ->
    JobNodes = maps:get(num_nodes, Job, 1),
    JobCpus = maps:get(num_cpus, Job, 1),
    JobMem = maps:get(memory_mb, Job, 1024),

    find_sufficient_set(
        Rest, NeededNodes, NeededCpus, NeededMem,
        [Job | Acc],
        AccNodes + JobNodes,
        AccCpus + JobCpus,
        AccMem + JobMem
    ).

%% @private
%% Preempt a single job according to the mode.
preempt_single_job(JobId, JobPid, Mode, GraceTime) ->
    case JobPid of
        undefined ->
            {error, job_not_found};
        Pid when is_pid(Pid) ->
            case Mode of
                requeue ->
                    preempt_requeue(JobId, Pid, GraceTime);
                cancel ->
                    preempt_cancel(JobId, Pid, GraceTime);
                checkpoint ->
                    preempt_checkpoint(JobId, Pid, GraceTime);
                suspend ->
                    preempt_suspend(JobId, Pid);
                off ->
                    {error, preemption_disabled}
            end
    end.

%% @private
%% Requeue preemption - stop job and put back in pending queue.
preempt_requeue(JobId, Pid, GraceTime) ->
    %% Signal job to stop gracefully
    case flurm_job:preempt(Pid, requeue, GraceTime) of
        ok ->
            error_logger:info_msg("Job ~p preempted and requeued~n", [JobId]),
            {ok, JobId};
        {error, Reason} ->
            error_logger:warning_msg("Failed to preempt job ~p: ~p~n",
                                    [JobId, Reason]),
            {error, Reason}
    end.

%% @private
%% Cancel preemption - terminate job completely.
preempt_cancel(JobId, Pid, GraceTime) ->
    %% Signal job to stop, then cancel
    case flurm_job:preempt(Pid, cancel, GraceTime) of
        ok ->
            error_logger:info_msg("Job ~p preempted and cancelled~n", [JobId]),
            {ok, JobId};
        {error, Reason} ->
            error_logger:warning_msg("Failed to cancel job ~p: ~p~n",
                                    [JobId, Reason]),
            {error, Reason}
    end.

%% @private
%% Checkpoint preemption - save state then stop.
preempt_checkpoint(JobId, Pid, GraceTime) ->
    %% Signal job to checkpoint then stop
    case flurm_job:preempt(Pid, checkpoint, GraceTime) of
        ok ->
            error_logger:info_msg("Job ~p checkpointed and preempted~n", [JobId]),
            {ok, JobId};
        {error, Reason} ->
            %% Fall back to requeue if checkpoint fails
            error_logger:warning_msg(
                "Checkpoint failed for job ~p (~p), falling back to requeue~n",
                [JobId, Reason]),
            preempt_requeue(JobId, Pid, GraceTime)
    end.

%% @private
%% Suspend preemption - pause job without releasing resources.
preempt_suspend(JobId, Pid) ->
    case flurm_job:suspend(Pid) of
        ok ->
            error_logger:info_msg("Job ~p suspended~n", [JobId]),
            {ok, JobId};
        {error, Reason} ->
            error_logger:warning_msg("Failed to suspend job ~p: ~p~n",
                                    [JobId, Reason]),
            {error, Reason}
    end.
