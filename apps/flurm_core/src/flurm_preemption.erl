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
    get_preemption_mode/1,           % Get mode for partition/QOS
    set_grace_time/1,
    get_grace_time/0,
    get_grace_time/1,                % Get grace time for partition
    %% Preemption rules configuration
    set_priority_threshold/1,
    get_priority_threshold/0,
    set_qos_preemption_rules/1,
    get_qos_preemption_rules/0,
    set_partition_preemption_mode/2, % Set preemption mode per partition
    set_partition_grace_time/2,      % Set grace time per partition
    %% Scheduler integration
    find_preemptable_jobs/2,         % Find jobs that can be preempted for a pending job
    calculate_preemption_cost/1,     % Score preemption candidates
    check_preemption_opportunity/2,
    execute_preemption/2,
    handle_preempted_job/2,
    %% Graceful preemption
    graceful_preempt/3               % Graceful preemption with SIGTERM/SIGKILL
]).

%% Types
-type preemption_mode() :: requeue | cancel | checkpoint | suspend | off.
-type preemption_result() :: {ok, [pos_integer()]} | {error, term()}.

-export_type([preemption_mode/0, preemption_result/0]).

%% Test exports
-ifdef(TEST).
-export([
    %% Configuration helpers
    ensure_config_table/0,
    get_qos_preemption_mode/1,
    %% Preemption set finding
    find_preemption_set/4,
    find_sufficient_set/8,
    %% Resource calculation
    calculate_freed_resources/1,
    %% Job preemption internals
    preempt_single_job/4,
    preempt_single_job_with_handling/4,
    preempt_requeue/3,
    preempt_cancel/3,
    preempt_checkpoint/3,
    preempt_suspend/2,
    %% Graceful preemption
    do_graceful_preempt/4,
    wait_for_job_exit/3
]).
-endif.

%% Configuration (could be moved to ETS/config)
-define(DEFAULT_PREEMPTION_MODE, requeue).
-define(DEFAULT_GRACE_TIME, 60).  % Seconds to allow graceful shutdown
-define(DEFAULT_PRIORITY_THRESHOLD, 1000).  % Minimum priority difference for preemption
-define(PREEMPTION_CONFIG, flurm_preemption_config).

%% QOS preemption levels (higher can preempt lower)
%% Default order: high > normal > low
-define(DEFAULT_QOS_LEVELS, #{
    <<"high">> => 300,
    <<"normal">> => 200,
    <<"low">> => 100
}).

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
%% Uses QOS-based preemption rules and configurable priority threshold.
-spec can_preempt(map(), map()) -> boolean().
can_preempt(Preemptor, Preemptee) ->
    PreemptorPriority = maps:get(priority, Preemptor, ?DEFAULT_PRIORITY),
    PreemptorQOS = maps:get(qos, Preemptor, <<"normal">>),

    PreempteePriority = maps:get(priority, Preemptee, ?DEFAULT_PRIORITY),
    PreempteeQOS = maps:get(qos, Preemptee, <<"normal">>),

    %% Check QOS-based preemption first using configurable rules
    QOSRules = get_qos_preemption_rules(),
    PreemptorQOSLevel = maps:get(PreemptorQOS, QOSRules, 0),
    PreempteeQOSLevel = maps:get(PreempteeQOS, QOSRules, 0),

    case PreemptorQOSLevel > PreempteeQOSLevel of
        true ->
            %% Higher QOS can always preempt lower QOS
            true;
        false when PreemptorQOSLevel =:= PreempteeQOSLevel ->
            %% Same QOS level - use priority threshold
            PriorityThreshold = get_priority_threshold(),
            PreemptorPriority > PreempteePriority + PriorityThreshold;
        false ->
            %% Lower QOS cannot preempt higher QOS
            false
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

%% @doc Set the priority threshold for preemption.
%% Jobs must have priority difference greater than this threshold to preempt.
-spec set_priority_threshold(non_neg_integer()) -> ok.
set_priority_threshold(Threshold) when is_integer(Threshold), Threshold >= 0 ->
    ensure_config_table(),
    ets:insert(?PREEMPTION_CONFIG, {priority_threshold, Threshold}),
    ok.

%% @doc Get the priority threshold for preemption.
-spec get_priority_threshold() -> non_neg_integer().
get_priority_threshold() ->
    ensure_config_table(),
    case ets:lookup(?PREEMPTION_CONFIG, priority_threshold) of
        [{priority_threshold, Threshold}] -> Threshold;
        [] -> ?DEFAULT_PRIORITY_THRESHOLD
    end.

%% @doc Set QOS preemption rules.
%% Rules is a map of QOS name to numeric level (higher can preempt lower).
-spec set_qos_preemption_rules(map()) -> ok.
set_qos_preemption_rules(Rules) when is_map(Rules) ->
    ensure_config_table(),
    ets:insert(?PREEMPTION_CONFIG, {qos_rules, Rules}),
    ok.

%% @doc Get QOS preemption rules.
-spec get_qos_preemption_rules() -> map().
get_qos_preemption_rules() ->
    ensure_config_table(),
    case ets:lookup(?PREEMPTION_CONFIG, qos_rules) of
        [{qos_rules, Rules}] -> Rules;
        [] -> ?DEFAULT_QOS_LEVELS
    end.

%% @doc Set preemption mode for a specific partition.
-spec set_partition_preemption_mode(binary(), preemption_mode()) -> ok.
set_partition_preemption_mode(Partition, Mode) when is_binary(Partition),
                                                     Mode =:= requeue;
                                                     Mode =:= cancel;
                                                     Mode =:= checkpoint;
                                                     Mode =:= suspend;
                                                     Mode =:= off ->
    ensure_config_table(),
    PartitionModes = case ets:lookup(?PREEMPTION_CONFIG, partition_modes) of
        [{partition_modes, Existing}] -> Existing;
        [] -> #{}
    end,
    NewModes = maps:put(Partition, Mode, PartitionModes),
    ets:insert(?PREEMPTION_CONFIG, {partition_modes, NewModes}),
    ok.

%% @doc Get preemption mode for a specific partition or QOS.
%% Falls back to global mode if partition-specific not set.
%% Takes a map with optional partition and qos keys.
-spec get_preemption_mode(map()) -> preemption_mode().
get_preemption_mode(#{partition := Partition} = Opts) when is_binary(Partition) ->
    ensure_config_table(),
    %% Check partition-specific mode first
    PartitionMode = case ets:lookup(?PREEMPTION_CONFIG, partition_modes) of
        [{partition_modes, Modes}] -> maps:get(Partition, Modes, undefined);
        [] -> undefined
    end,
    case PartitionMode of
        undefined ->
            %% Check QOS-specific mode if provided
            case maps:get(qos, Opts, undefined) of
                undefined -> get_preemption_mode();
                QOS -> get_qos_preemption_mode(QOS)
            end;
        Mode -> Mode
    end;
get_preemption_mode(#{qos := QOS}) when is_binary(QOS) ->
    get_qos_preemption_mode(QOS);
get_preemption_mode(_) ->
    get_preemption_mode().

%% @doc Get preemption mode from QOS definition.
%% Returns the QOS preempt_mode if configured, otherwise global default.
-spec get_qos_preemption_mode(binary()) -> preemption_mode().
get_qos_preemption_mode(QOS) ->
    %% Try to get QOS from flurm_qos module if available
    case catch flurm_qos:get_qos(QOS) of
        {ok, #qos{preempt_mode = Mode}} when Mode =/= off ->
            Mode;
        _ ->
            get_preemption_mode()
    end.

%% @doc Get grace time for a specific partition.
%% Falls back to global grace time if partition-specific not set.
-spec get_grace_time(binary()) -> pos_integer().
get_grace_time(Partition) when is_binary(Partition) ->
    ensure_config_table(),
    case ets:lookup(?PREEMPTION_CONFIG, partition_grace_times) of
        [{partition_grace_times, Times}] ->
            maps:get(Partition, Times, get_grace_time());
        [] ->
            get_grace_time()
    end.

%% @doc Set grace time for a specific partition.
-spec set_partition_grace_time(binary(), pos_integer()) -> ok.
set_partition_grace_time(Partition, Seconds) when is_binary(Partition),
                                                   is_integer(Seconds),
                                                   Seconds > 0 ->
    ensure_config_table(),
    GraceTimes = case ets:lookup(?PREEMPTION_CONFIG, partition_grace_times) of
        [{partition_grace_times, Existing}] -> Existing;
        [] -> #{}
    end,
    NewTimes = maps:put(Partition, Seconds, GraceTimes),
    ets:insert(?PREEMPTION_CONFIG, {partition_grace_times, NewTimes}),
    ok.

%%====================================================================
%% Scheduler Integration Functions
%%====================================================================

%% @doc Find jobs that can be preempted to make room for a pending job.
%% Returns a list of job maps sorted by preemption cost (lowest cost first).
%% This is the main entry point for the scheduler to find preemption victims.
-spec find_preemptable_jobs(map(), map()) ->
    {ok, [map()]} | {error, term()}.
find_preemptable_jobs(PendingJob, ResourcesNeeded) ->
    Mode = get_preemption_mode(#{
        partition => maps:get(partition, PendingJob, <<"default">>),
        qos => maps:get(qos, PendingJob, <<"normal">>)
    }),
    case Mode of
        off ->
            {error, preemption_disabled};
        _ ->
            Priority = maps:get(priority, PendingJob, ?DEFAULT_PRIORITY),
            %% Get all preemptable candidates
            Candidates = get_preemptable_jobs(PendingJob, Priority),

            case Candidates of
                [] ->
                    {error, no_preemptable_jobs};
                _ ->
                    %% Sort by preemption cost (lowest first)
                    SortedCandidates = lists:sort(
                        fun(A, B) ->
                            calculate_preemption_cost(A) =< calculate_preemption_cost(B)
                        end,
                        Candidates
                    ),

                    %% Find minimum set that provides needed resources
                    NumNodes = maps:get(num_nodes, ResourcesNeeded,
                               maps:get(num_nodes, PendingJob, 1)),
                    NumCpus = maps:get(num_cpus, ResourcesNeeded,
                              maps:get(num_cpus, PendingJob, 1)),
                    MemoryMb = maps:get(memory_mb, ResourcesNeeded,
                               maps:get(memory_mb, PendingJob, 1024)),

                    case find_preemption_set(SortedCandidates, NumNodes, NumCpus, MemoryMb) of
                        {ok, JobsToPreempt} when length(JobsToPreempt) > 0 ->
                            {ok, JobsToPreempt};
                        {ok, []} ->
                            {error, insufficient_preemptable_resources};
                        {error, _} = Err ->
                            Err
                    end
            end
    end.

%% @doc Calculate the cost of preempting a job.
%% Lower cost = better candidate for preemption.
%% Factors considered:
%% - Priority: Lower priority jobs are better candidates
%% - Run time: Jobs that have run longer are less desirable to preempt
%% - Resource usage: Jobs using more resources are better candidates if we need those resources
%% - QOS level: Lower QOS jobs are better candidates
-spec calculate_preemption_cost(map()) -> float().
calculate_preemption_cost(Job) ->
    Priority = maps:get(priority, Job, ?DEFAULT_PRIORITY),
    StartTime = maps:get(start_time, Job, erlang:system_time(second)),
    QOS = maps:get(qos, Job, <<"normal">>),
    NumNodes = maps:get(num_nodes, Job, 1),
    NumCpus = maps:get(num_cpus, Job, 1),

    %% Calculate run time (in minutes)
    CurrentTime = erlang:system_time(second),
    RunTimeMinutes = case StartTime of
        undefined -> 0;
        T when is_integer(T) -> (CurrentTime - T) / 60;
        _ -> 0
    end,

    %% Get QOS level for scoring
    QOSRules = get_qos_preemption_rules(),
    QOSLevel = maps:get(QOS, QOSRules, 200),  % Default to normal level

    %% Calculate cost components:
    %% - Priority cost: Higher priority = higher cost (don't preempt)
    PriorityCost = Priority / ?MAX_PRIORITY,  % Normalize 0-1

    %% - Run time cost: Longer run time = higher cost (wasted work)
    RunTimeCost = math:log(1 + RunTimeMinutes) / 10,  % Logarithmic scaling

    %% - QOS cost: Higher QOS = higher cost
    QOSCost = QOSLevel / 300,  % Normalize based on max QOS level

    %% - Resource benefit: More resources = negative cost (we want to preempt)
    ResourceBenefit = -0.1 * (NumNodes + NumCpus / 10),

    %% Total cost (lower is better for preemption)
    TotalCost = PriorityCost * 0.4 +      % 40% weight on priority
                RunTimeCost * 0.3 +        % 30% weight on run time
                QOSCost * 0.2 +            % 20% weight on QOS
                ResourceBenefit * 0.1,     % 10% benefit for resources

    max(0.0, TotalCost).

%% @doc Check for preemption opportunities for a pending job.
%% Called by the scheduler when a high-priority job cannot be scheduled.
%% Returns {ok, PreemptionPlan} if preemption can free enough resources,
%% or {error, Reason} if preemption is not possible/beneficial.
-spec check_preemption_opportunity(map(), map()) ->
    {ok, #{jobs_to_preempt := [map()], resources_freed := map()}} |
    {error, term()}.
check_preemption_opportunity(PendingJob, ResourcesNeeded) ->
    Mode = get_preemption_mode(),
    case Mode of
        off ->
            {error, preemption_disabled};
        _ ->
            %% Get job requirements
            NumNodes = maps:get(num_nodes, ResourcesNeeded,
                       maps:get(num_nodes, PendingJob, 1)),
            NumCpus = maps:get(num_cpus, ResourcesNeeded,
                      maps:get(num_cpus, PendingJob, 1)),
            MemoryMb = maps:get(memory_mb, ResourcesNeeded,
                       maps:get(memory_mb, PendingJob, 1024)),
            Priority = maps:get(priority, PendingJob, ?DEFAULT_PRIORITY),

            %% Find preemptable candidates
            Candidates = get_preemptable_jobs(PendingJob, Priority),

            case Candidates of
                [] ->
                    {error, no_preemptable_jobs};
                _ ->
                    %% Calculate preemption plan
                    case find_preemption_set(Candidates, NumNodes, NumCpus, MemoryMb) of
                        {ok, ToPreempt} when length(ToPreempt) > 0 ->
                            %% Calculate total resources that would be freed
                            FreedResources = calculate_freed_resources(ToPreempt),
                            {ok, #{
                                jobs_to_preempt => ToPreempt,
                                resources_freed => FreedResources,
                                preemption_mode => Mode,
                                grace_time => get_grace_time()
                            }};
                        {ok, []} ->
                            {error, insufficient_preemptable_resources};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
    end.

%% @doc Execute a preemption plan.
%% Called by the scheduler to actually preempt the jobs.
%% Returns list of successfully preempted job IDs.
-spec execute_preemption(#{jobs_to_preempt := [map()], _ => _}, map()) ->
    {ok, [pos_integer()]} | {error, term()}.
execute_preemption(#{jobs_to_preempt := JobsToPreempt} = Plan, _PendingJob) ->
    Mode = maps:get(preemption_mode, Plan, get_preemption_mode()),
    GraceTime = maps:get(grace_time, Plan, get_grace_time()),

    %% Record preemption event for metrics
    catch flurm_metrics:increment(flurm_preemptions_total, length(JobsToPreempt)),

    %% Execute preemption for each job
    Results = lists:map(
        fun(Job) ->
            JobId = maps:get(job_id, Job),
            error_logger:info_msg("Preempting job ~p for higher priority job~n", [JobId]),
            preempt_single_job_with_handling(JobId, Job, Mode, GraceTime)
        end,
        JobsToPreempt
    ),

    %% Return successfully preempted jobs
    Preempted = [JobId || {ok, JobId} <- Results],
    case Preempted of
        [] ->
            {error, all_preemptions_failed};
        _ ->
            {ok, Preempted}
    end.

%% @doc Handle a preempted job based on the preemption mode.
%% Called after a job has been preempted to update its state.
-spec handle_preempted_job(pos_integer(), preemption_mode()) -> ok | {error, term()}.
handle_preempted_job(JobId, requeue) ->
    %% Requeue the job - move back to pending state
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            %% Release allocated nodes
            AllocatedNodes = Job#job.allocated_nodes,
            lists:foreach(
                fun(NodeName) ->
                    flurm_node_manager:release_resources(NodeName, JobId)
                end,
                AllocatedNodes
            ),
            %% Update job state to pending (requeued)
            flurm_job_manager:update_job(JobId, #{
                state => pending,
                allocated_nodes => [],
                start_time => undefined
            }),
            %% Resubmit to scheduler
            flurm_scheduler:submit_job(JobId),
            error_logger:info_msg("Job ~p requeued after preemption~n", [JobId]),
            ok;
        {error, not_found} ->
            {error, job_not_found}
    end;
handle_preempted_job(JobId, cancel) ->
    %% Cancel the job completely
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            AllocatedNodes = Job#job.allocated_nodes,
            lists:foreach(
                fun(NodeName) ->
                    flurm_node_manager:release_resources(NodeName, JobId)
                end,
                AllocatedNodes
            ),
            %% Update job state to cancelled
            flurm_job_manager:update_job(JobId, #{
                state => cancelled,
                end_time => erlang:system_time(second)
            }),
            %% Notify scheduler
            flurm_scheduler:job_failed(JobId),
            error_logger:info_msg("Job ~p cancelled due to preemption~n", [JobId]),
            ok;
        {error, not_found} ->
            {error, job_not_found}
    end;
handle_preempted_job(JobId, checkpoint) ->
    %% Checkpoint preemption - save state then requeue
    %% For now, treat same as requeue (checkpoint logic would go here)
    error_logger:info_msg("Job ~p checkpointing before preemption~n", [JobId]),
    handle_preempted_job(JobId, requeue);
handle_preempted_job(JobId, suspend) ->
    %% Suspend the job - keep resources but pause execution
    %% Use suspend_job/1 which sends SIGSTOP to the job and updates state
    case flurm_job_manager:suspend_job(JobId) of
        ok ->
            error_logger:info_msg("Job ~p suspended due to preemption~n", [JobId]),
            ok;
        {error, {invalid_state, State}} ->
            error_logger:warning_msg("Cannot suspend job ~p in state ~p~n", [JobId, State]),
            %% Fall back to requeue if suspend fails
            handle_preempted_job(JobId, requeue);
        {error, not_found} ->
            {error, job_not_found};
        {error, Reason} ->
            error_logger:warning_msg("Failed to suspend job ~p: ~p~n", [JobId, Reason]),
            {error, Reason}
    end;
handle_preempted_job(_JobId, off) ->
    {error, preemption_disabled}.

%% @doc Perform graceful preemption with SIGTERM followed by SIGKILL.
%% First sends SIGTERM to allow the job to cleanup gracefully.
%% If the job doesn't exit within the grace period, sends SIGKILL.
%% Returns {ok, JobId} on success, {error, Reason} on failure.
-spec graceful_preempt(pos_integer(), [binary()], preemption_mode()) ->
    {ok, pos_integer()} | {error, term()}.
graceful_preempt(JobId, Nodes, Mode) ->
    %% Get partition for this job to determine grace time
    GraceTime = case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            Partition = Job#job.partition,
            get_grace_time(Partition);
        _ ->
            get_grace_time()
    end,

    %% Spawn a process to handle the graceful shutdown
    Self = self(),
    spawn_link(fun() ->
        Result = do_graceful_preempt(JobId, Nodes, Mode, GraceTime),
        Self ! {graceful_preempt_done, JobId, Result}
    end),

    %% Wait for completion or timeout (grace_time + 10 seconds buffer)
    Timeout = (GraceTime + 10) * 1000,
    receive
        {graceful_preempt_done, JobId, Result} ->
            Result
    after Timeout ->
        %% Force kill if graceful preempt timed out
        error_logger:warning_msg("Graceful preempt for job ~p timed out, forcing~n", [JobId]),
        flurm_job_dispatcher:preempt_job(JobId, #{signal => sigkill, nodes => Nodes}),
        handle_preempted_job(JobId, Mode),
        {ok, JobId}
    end.

%% @private
%% Internal implementation of graceful preemption
do_graceful_preempt(JobId, Nodes, Mode, GraceTime) ->
    %% Step 1: Send SIGTERM to allow graceful shutdown
    error_logger:info_msg("Sending SIGTERM to job ~p (grace period: ~p seconds)~n",
                         [JobId, GraceTime]),

    case flurm_job_dispatcher:preempt_job(JobId, #{signal => sigterm, nodes => Nodes}) of
        ok ->
            %% Step 2: Wait for grace period, checking if job has exited
            GraceTimeMs = GraceTime * 1000,
            CheckInterval = 1000,  % Check every second
            case wait_for_job_exit(JobId, GraceTimeMs, CheckInterval) of
                true ->
                    %% Job exited gracefully
                    error_logger:info_msg("Job ~p exited gracefully after SIGTERM~n", [JobId]),
                    handle_preempted_job(JobId, Mode),
                    {ok, JobId};
                false ->
                    %% Job didn't exit in time, send SIGKILL
                    error_logger:warning_msg("Job ~p didn't exit gracefully, sending SIGKILL~n",
                                            [JobId]),
                    case flurm_job_dispatcher:preempt_job(JobId, #{signal => sigkill, nodes => Nodes}) of
                        ok ->
                            %% Give it a moment to be killed
                            timer:sleep(1000),
                            handle_preempted_job(JobId, Mode),
                            {ok, JobId};
                        {error, Reason} ->
                            error_logger:error_msg("Failed to SIGKILL job ~p: ~p~n",
                                                  [JobId, Reason]),
                            %% Still try to handle the preempted job
                            handle_preempted_job(JobId, Mode),
                            {ok, JobId}
                    end
            end;
        {error, Reason} ->
            error_logger:error_msg("Failed to send SIGTERM to job ~p: ~p~n",
                                  [JobId, Reason]),
            {error, Reason}
    end.

%% @private
%% Wait for a job to exit, checking periodically
wait_for_job_exit(_JobId, TimeRemaining, _CheckInterval) when TimeRemaining =< 0 ->
    false;
wait_for_job_exit(JobId, TimeRemaining, CheckInterval) ->
    timer:sleep(CheckInterval),
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            State = Job#job.state,
            case State of
                completed -> true;
                failed -> true;
                cancelled -> true;
                _ -> wait_for_job_exit(JobId, TimeRemaining - CheckInterval, CheckInterval)
            end;
        {error, not_found} ->
            %% Job no longer exists, consider it exited
            true
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Calculate total resources that would be freed by preempting jobs
calculate_freed_resources(Jobs) ->
    lists:foldl(
        fun(Job, Acc) ->
            #{
                nodes => maps:get(nodes, Acc, 0) + maps:get(num_nodes, Job, 1),
                cpus => maps:get(cpus, Acc, 0) + maps:get(num_cpus, Job, 1),
                memory_mb => maps:get(memory_mb, Acc, 0) + maps:get(memory_mb, Job, 1024)
            }
        end,
        #{nodes => 0, cpus => 0, memory_mb => 0},
        Jobs
    ).

%% @private
%% Preempt a single job with proper handling of state transitions
preempt_single_job_with_handling(JobId, Job, Mode, GraceTime) ->
    JobPid = maps:get(pid, Job, undefined),

    %% First try to preempt via the job process if it exists
    Result = case JobPid of
        undefined ->
            %% No pid - handle directly via job manager
            handle_preempted_job(JobId, Mode);
        Pid when is_pid(Pid) ->
            %% Try to preempt via the job process
            case catch flurm_job:preempt(Pid, Mode, GraceTime) of
                ok ->
                    %% Job process handled preemption
                    handle_preempted_job(JobId, Mode);
                {error, Reason} ->
                    error_logger:warning_msg("Job ~p preempt failed: ~p, handling directly~n",
                                           [JobId, Reason]),
                    %% Fall back to direct handling
                    handle_preempted_job(JobId, Mode);
                {'EXIT', _} ->
                    %% Process might have crashed
                    handle_preempted_job(JobId, Mode)
            end
    end,

    case Result of
        ok -> {ok, JobId};
        Error -> Error
    end.

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
