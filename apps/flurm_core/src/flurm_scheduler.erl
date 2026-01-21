%%%-------------------------------------------------------------------
%%% @doc FLURM Scheduler
%%%
%%% A gen_server implementing FIFO job scheduling for the FLURM
%%% workload manager. The scheduler maintains a queue of pending jobs
%%% and periodically attempts to schedule them on available nodes.
%%%
%%% Scheduling Algorithm:
%%%   - FIFO ordering: first pending job gets scheduled first
%%%   - Match job requirements to available node resources
%%%   - Handle partial allocation (wait if not enough resources)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    submit_job/1,
    job_completed/1,
    job_failed/1,
    trigger_schedule/0,
    get_stats/0,
    job_deps_satisfied/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

%% Test exports for internal functions
-ifdef(TEST).
-export([
    job_to_info/1,
    job_to_limit_spec/1,
    job_to_backfill_map/1,
    build_limit_info/1,
    calculate_resources_to_free/1,
    remove_jobs_from_queue/2
]).
-endif.

%% Internal state
-record(scheduler_state, {
    pending_jobs    :: queue:queue(pos_integer()),
    running_jobs    :: sets:set(pos_integer()),
    nodes_cache     :: ets:tid(),
    schedule_timer  :: reference() | undefined,
    schedule_cycles :: non_neg_integer(),
    completed_count :: non_neg_integer(),
    failed_count    :: non_neg_integer(),
    %% Track why jobs are pending (for debugging and user info)
    %% #{JobId => {Reason, Timestamp}}
    pending_reasons :: #{pos_integer() => {term(), non_neg_integer()}},
    %% Track if a schedule trigger is already pending (for coalescing)
    schedule_pending :: boolean()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the scheduler.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    case gen_server:start_link({local, ?SERVER}, ?MODULE, [], []) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            %% Process already running - return existing pid
            %% This handles race conditions during startup and restarts
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Add a job to the pending queue.
-spec submit_job(pos_integer()) -> ok.
submit_job(JobId) when is_integer(JobId), JobId > 0 ->
    gen_server:cast(?SERVER, {submit_job, JobId}).

%% @doc Notify scheduler that a job has completed successfully.
-spec job_completed(pos_integer()) -> ok.
job_completed(JobId) when is_integer(JobId), JobId > 0 ->
    gen_server:cast(?SERVER, {job_completed, JobId}).

%% @doc Notify scheduler that a job has failed.
-spec job_failed(pos_integer()) -> ok.
job_failed(JobId) when is_integer(JobId), JobId > 0 ->
    gen_server:cast(?SERVER, {job_failed, JobId}).

%% @doc Force a scheduling cycle.
-spec trigger_schedule() -> ok.
trigger_schedule() ->
    gen_server:cast(?SERVER, trigger_schedule).

%% @doc Get scheduler statistics.
-spec get_stats() -> {ok, map()}.
get_stats() ->
    gen_server:call(?SERVER, get_stats).

%% @doc Notify scheduler that a job's dependencies are now satisfied.
%% Called by flurm_job_deps when all dependencies become satisfied.
-spec job_deps_satisfied(pos_integer()) -> ok.
job_deps_satisfied(JobId) when is_integer(JobId), JobId > 0 ->
    gen_server:cast(?SERVER, {job_deps_satisfied, JobId}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init([]) ->
    %% Create ETS table for caching node state
    NodesCache = ets:new(flurm_scheduler_nodes_cache, [
        set,
        private,
        {keypos, 1}
    ]),

    %% Subscribe to config changes for partitions, nodes, and scheduler settings
    catch flurm_config_server:subscribe_changes([partitions, nodes, schedulertype]),

    %% Start the periodic scheduling timer
    Timer = schedule_next_cycle(),

    State = #scheduler_state{
        pending_jobs = queue:new(),
        running_jobs = sets:new(),
        nodes_cache = NodesCache,
        schedule_timer = Timer,
        schedule_cycles = 0,
        completed_count = 0,
        failed_count = 0,
        pending_reasons = #{},
        schedule_pending = false
    },
    {ok, State}.

%% @private
handle_call(get_stats, _From, State) ->
    Stats = #{
        pending_count => queue:len(State#scheduler_state.pending_jobs),
        running_count => sets:size(State#scheduler_state.running_jobs),
        completed_count => State#scheduler_state.completed_count,
        failed_count => State#scheduler_state.failed_count,
        schedule_cycles => State#scheduler_state.schedule_cycles
    },
    {reply, {ok, Stats}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast({submit_job, JobId}, State) ->
    lager:debug("Scheduler received job ~p for scheduling", [JobId]),
    %% Add job to pending queue
    NewPending = queue:in(JobId, State#scheduler_state.pending_jobs),
    NewState = State#scheduler_state{pending_jobs = NewPending},
    %% Trigger scheduling - coalesce with pending trigger if already scheduled
    FinalState = maybe_trigger_schedule(NewState),
    {noreply, FinalState};

handle_cast({job_completed, JobId}, State) ->
    NewState = handle_job_finished(JobId, completed, State),
    State1 = NewState#scheduler_state{
        completed_count = NewState#scheduler_state.completed_count + 1
    },
    %% Trigger scheduling to fill freed resources (coalesced)
    FinalState = maybe_trigger_schedule(State1),
    {noreply, FinalState};

handle_cast({job_failed, JobId}, State) ->
    NewState = handle_job_finished(JobId, failed, State),
    State1 = NewState#scheduler_state{
        failed_count = NewState#scheduler_state.failed_count + 1
    },
    %% Trigger scheduling to fill freed resources (coalesced)
    FinalState = maybe_trigger_schedule(State1),
    {noreply, FinalState};

handle_cast(trigger_schedule, State) ->
    NewState = maybe_trigger_schedule(State),
    {noreply, NewState};

handle_cast({job_deps_satisfied, JobId}, State) ->
    %% Job dependencies are satisfied, trigger scheduling
    lager:debug("Job ~p dependencies satisfied, triggering schedule", [JobId]),
    NewState = maybe_trigger_schedule(State),
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(schedule_cycle, State) ->
    StartTime = erlang:monotonic_time(millisecond),
    PendingCount = queue:len(State#scheduler_state.pending_jobs),
    lager:debug("schedule_cycle: ~p pending, ~p running, cycle ~p",
               [PendingCount, sets:size(State#scheduler_state.running_jobs),
                State#scheduler_state.schedule_cycles]),
    %% Cancel any existing timer
    case State#scheduler_state.schedule_timer of
        undefined -> ok;
        OldTimer -> erlang:cancel_timer(OldTimer)
    end,

    %% Clear schedule_pending flag since we're running the cycle now
    State1 = State#scheduler_state{schedule_pending = false},

    %% Run scheduling cycle
    NewState = schedule_cycle(State1),

    %% Record metrics (batch update)
    Duration = erlang:monotonic_time(millisecond) - StartTime,
    catch flurm_metrics:increment(flurm_scheduler_cycles_total),
    catch flurm_metrics:histogram(flurm_scheduler_duration_ms, Duration),
    catch flurm_metrics:gauge(flurm_jobs_pending, queue:len(NewState#scheduler_state.pending_jobs)),
    catch flurm_metrics:gauge(flurm_jobs_running, sets:size(NewState#scheduler_state.running_jobs)),

    %% Schedule next cycle
    Timer = schedule_next_cycle(),
    FinalState = NewState#scheduler_state{
        schedule_timer = Timer,
        schedule_cycles = NewState#scheduler_state.schedule_cycles + 1
    },
    {noreply, FinalState};

%% Handle config changes from flurm_config_server
handle_info({config_changed, partitions, _OldPartitions, NewPartitions}, State) ->
    lager:info("Scheduler received partition config change: ~p partitions", [length(NewPartitions)]),
    %% Trigger a scheduling cycle to re-evaluate jobs with new partition definitions
    NewState = maybe_trigger_schedule(State),
    {noreply, NewState};

handle_info({config_changed, nodes, _OldNodes, NewNodes}, State) ->
    lager:info("Scheduler received node config change: ~p node definitions", [length(NewNodes)]),
    %% Trigger a scheduling cycle to re-evaluate jobs with new node definitions
    NewState = maybe_trigger_schedule(State),
    {noreply, NewState};

handle_info({config_changed, schedulertype, _OldType, NewType}, State) ->
    lager:info("Scheduler type changed to ~p - applying new scheduler configuration", [NewType]),
    %% Could implement scheduler type switching here (FIFO, backfill, etc.)
    NewState = maybe_trigger_schedule(State),
    {noreply, NewState};

handle_info({config_changed, _Key, _OldValue, _NewValue}, State) ->
    %% Ignore other config changes
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, State) ->
    case State#scheduler_state.schedule_timer of
        undefined -> ok;
        Timer -> erlang:cancel_timer(Timer)
    end,
    ets:delete(State#scheduler_state.nodes_cache),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Schedule the next cycle timer
schedule_next_cycle() ->
    erlang:send_after(?SCHEDULE_INTERVAL, self(), schedule_cycle).

%% @private
%% Trigger a schedule cycle if one isn't already pending
%% This coalesces multiple rapid submit/complete events into a single cycle
%% Returns the updated state with schedule_pending flag set
maybe_trigger_schedule(#scheduler_state{schedule_pending = true} = State) ->
    %% Schedule cycle already pending, don't send another message
    State;
maybe_trigger_schedule(State) ->
    %% No pending cycle, trigger one and mark as pending
    self() ! schedule_cycle,
    State#scheduler_state{schedule_pending = true}.

%% @private
%% Handle job finished (completed or failed) - release resources
%% Also notifies flurm_job_deps to release dependent jobs
handle_job_finished(JobId, FinalState, State) ->
    %% Remove from running set
    NewRunning = sets:del_element(JobId, State#scheduler_state.running_jobs),

    %% Release resources on nodes via node manager
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            AllocatedNodes = Job#job.allocated_nodes,
            lists:foreach(
                fun(NodeName) ->
                    flurm_node_manager:release_resources(NodeName, JobId)
                end,
                AllocatedNodes
            ),

            %% Release licenses allocated to this job
            Licenses = Job#job.licenses,
            case Licenses of
                [] -> ok;
                _ ->
                    lager:info("Releasing licenses for job ~p: ~p", [JobId, Licenses]),
                    flurm_license:deallocate(JobId, Licenses)
            end,

            %% Notify limits module that job has stopped (for usage tracking)
            %% Calculate wall time for TRES minutes accounting
            WallTime = case {Job#job.start_time, Job#job.end_time} of
                {undefined, _} -> 0;
                {_, undefined} ->
                    %% Job still ending, use current time
                    erlang:system_time(second) - Job#job.start_time;
                {Start, End} -> End - Start
            end,
            LimitInfo = #{
                user => Job#job.user,
                account => Job#job.account,
                tres => #{
                    cpu => Job#job.num_cpus,
                    mem => Job#job.memory_mb,
                    node => Job#job.num_nodes
                },
                wall_time => WallTime
            },
            flurm_limits:enforce_limit(stop, JobId, LimitInfo),

            %% Notify flurm_job_deps about job completion to release dependent jobs
            %% This allows afterok/afternotok/afterany dependencies to be satisfied
            notify_job_deps_completion(JobId, FinalState);
        _ ->
            %% Job not found, still try to notify deps
            notify_job_deps_completion(JobId, FinalState)
    end,

    State#scheduler_state{running_jobs = NewRunning}.

%% @private
%% Notify flurm_job_deps about job completion/failure
%% This triggers dependency resolution for waiting jobs
notify_job_deps_completion(JobId, FinalState) ->
    case catch flurm_job_deps:notify_completion(JobId, FinalState) of
        ok -> ok;
        {'EXIT', {noproc, _}} ->
            %% flurm_job_deps not running, ignore
            ok;
        {'EXIT', Reason} ->
            lager:warning("Failed to notify job deps for job ~p: ~p", [JobId, Reason]),
            ok
    end.

%% Maximum number of jobs to schedule per cycle (for batch processing)
-define(MAX_JOBS_PER_CYCLE, 100).

%% @private
%% Run a scheduling cycle - try to schedule pending jobs in batches
schedule_cycle(State) ->
    PendingCount = queue:len(State#scheduler_state.pending_jobs),
    case PendingCount > 0 of
        true ->
            lager:debug("Schedule cycle: ~p pending jobs", [PendingCount]);
        false ->
            ok
    end,
    %% Process pending jobs in batches for better throughput
    schedule_pending_jobs_batch(State, ?MAX_JOBS_PER_CYCLE).

%% @private
%% Schedule pending jobs in batches for better throughput
%% Processes up to MaxJobs in a single cycle
schedule_pending_jobs_batch(State, MaxJobs) ->
    schedule_pending_jobs_batch(State, MaxJobs, 0).

schedule_pending_jobs_batch(State, MaxJobs, Scheduled) when Scheduled >= MaxJobs ->
    %% Reached batch limit, stop for this cycle
    State;
schedule_pending_jobs_batch(State, MaxJobs, Scheduled) ->
    case queue:out(State#scheduler_state.pending_jobs) of
        {empty, _} ->
            %% No more pending jobs
            State;
        {{value, JobId}, RestQueue} ->
            %% Try to schedule this job
            case try_schedule_job(JobId, State) of
                {ok, NewState} ->
                    %% Job scheduled, continue with rest of queue
                    schedule_pending_jobs_batch(
                        NewState#scheduler_state{pending_jobs = RestQueue},
                        MaxJobs,
                        Scheduled + 1
                    );
                {wait, Reason} ->
                    %% Job cannot be scheduled (insufficient resources)
                    %% Try backfill: schedule smaller jobs that can fit
                    lager:debug("Job ~p waiting (~p), trying backfill", [JobId, Reason]),
                    BackfillState = try_backfill_jobs(RestQueue, State),
                    %% Keep original job at front of queue
                    BackfillState;
                {error, job_not_found} ->
                    %% Job no longer exists, skip it and continue
                    schedule_pending_jobs_batch(
                        State#scheduler_state{pending_jobs = RestQueue},
                        MaxJobs,
                        Scheduled
                    );
                {error, _Reason} ->
                    %% Other error, skip this job and continue
                    schedule_pending_jobs_batch(
                        State#scheduler_state{pending_jobs = RestQueue},
                        MaxJobs,
                        Scheduled
                    )
            end
    end.


%% @private
%% Try to backfill smaller jobs while high-priority job waits
%% This improves cluster utilization by using the flurm_backfill module
%% to find jobs that can complete before the blocked job needs resources.
try_backfill_jobs(Queue, State) ->
    %% Get the blocker job (first in queue that couldn't be scheduled)
    case queue:peek(State#scheduler_state.pending_jobs) of
        empty ->
            State;
        {value, BlockerJobId} ->
            try_backfill_with_blocker(BlockerJobId, Queue, State)
    end.

%% @private
%% Try backfill using the blocker job for shadow time calculation
try_backfill_with_blocker(BlockerJobId, Queue, State) ->
    %% Get blocker job info
    BlockerJob = case flurm_job_manager:get_job(BlockerJobId) of
        {ok, Job} ->
            job_to_backfill_map(Job);
        _ ->
            undefined
    end,

    case BlockerJob of
        undefined ->
            %% No blocker info, use simple backfill
            simple_backfill(Queue, State);
        _ ->
            %% Check if advanced backfill is enabled
            case flurm_backfill:is_backfill_enabled() of
                true ->
                    advanced_backfill(BlockerJob, Queue, State);
                false ->
                    simple_backfill(Queue, State)
            end
    end.

%% @private
%% Convert job record to map for backfill processing
job_to_backfill_map(#job{} = Job) ->
    #{
        job_id => Job#job.id,
        name => Job#job.name,
        user => Job#job.user,
        partition => Job#job.partition,
        state => Job#job.state,
        num_nodes => Job#job.num_nodes,
        num_cpus => Job#job.num_cpus,
        memory_mb => Job#job.memory_mb,
        time_limit => Job#job.time_limit,
        priority => Job#job.priority,
        submit_time => Job#job.submit_time,
        account => Job#job.account,
        qos => Job#job.qos
    }.

%% @private
%% Advanced backfill using shadow time calculation
%% Only schedules jobs that won't delay the blocker job
advanced_backfill(BlockerJob, Queue, State) ->
    %% Get candidate jobs from queue
    JobIds = queue:to_list(Queue),
    CandidateJobs = flurm_backfill:get_backfill_candidates(JobIds),

    lager:debug("Advanced backfill: blocker job ~p, ~p candidates",
               [maps:get(job_id, BlockerJob), length(CandidateJobs)]),

    %% Run the backfill algorithm
    BackfillResults = flurm_backfill:run_backfill_cycle(BlockerJob, CandidateJobs),

    %% Schedule the jobs that can backfill
    {ScheduledJobs, NewState} = schedule_backfill_results(BackfillResults, State, []),

    %% Remove scheduled jobs from the pending queue
    NewQueue = remove_jobs_from_queue(ScheduledJobs, State#scheduler_state.pending_jobs),
    NewState#scheduler_state{pending_jobs = NewQueue}.

%% @private
%% Schedule jobs returned by the backfill algorithm
schedule_backfill_results([], State, ScheduledAcc) ->
    {lists:reverse(ScheduledAcc), State};
schedule_backfill_results([{JobId, _SuggestedNodes} | Rest], State, ScheduledAcc) ->
    %% Try to schedule the job
    %% Note: We use try_schedule_job instead of directly using suggested nodes
    %% to ensure proper resource allocation and limit checking
    case try_schedule_job(JobId, State) of
        {ok, NewState} ->
            lager:info("Advanced backfill: scheduled job ~p", [JobId]),
            catch flurm_metrics:increment(flurm_scheduler_backfill_jobs),
            schedule_backfill_results(Rest, NewState, [JobId | ScheduledAcc]);
        _ ->
            %% Job couldn't be scheduled, skip it
            lager:debug("Advanced backfill: job ~p couldn't be scheduled", [JobId]),
            schedule_backfill_results(Rest, State, ScheduledAcc)
    end.

%% @private
%% Simple backfill - just try to schedule any job that fits
%% Used when advanced backfill is disabled or blocker info unavailable
simple_backfill(Queue, State) ->
    JobsList = queue:to_list(Queue),
    lager:debug("Simple backfill: checking ~p jobs", [length(JobsList)]),

    {ScheduledJobs, NewState} = simple_backfill_loop(JobsList, State, []),

    NewQueue = remove_jobs_from_queue(ScheduledJobs, State#scheduler_state.pending_jobs),
    NewState#scheduler_state{pending_jobs = NewQueue}.

%% @private
%% Loop through candidate jobs for simple backfill
simple_backfill_loop([], State, ScheduledAcc) ->
    {lists:reverse(ScheduledAcc), State};
simple_backfill_loop([JobId | Rest], State, ScheduledAcc) ->
    case try_schedule_job(JobId, State) of
        {ok, NewState} ->
            lager:info("Simple backfill: scheduled job ~p", [JobId]),
            catch flurm_metrics:increment(flurm_scheduler_backfill_jobs),
            simple_backfill_loop(Rest, NewState, [JobId | ScheduledAcc]);
        _ ->
            simple_backfill_loop(Rest, State, ScheduledAcc)
    end.

%% @private
%% Remove scheduled jobs from the queue
remove_jobs_from_queue([], Queue) ->
    Queue;
remove_jobs_from_queue(JobsToRemove, Queue) ->
    ScheduledSet = sets:from_list(JobsToRemove),
    queue:filter(
        fun(JobId) -> not sets:is_element(JobId, ScheduledSet) end,
        Queue
    ).

%% @private
%% Try to schedule a single job
%% Uses flurm_job_manager to get job info (not flurm_job_registry)
%% Now includes resource limits checking via flurm_limits module
%% Also checks job dependencies via flurm_job_deps before scheduling
try_schedule_job(JobId, State) ->
    lager:debug("Trying to schedule job ~p", [JobId]),
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            JobState = flurm_core:job_state(Job),
            lager:debug("Job ~p state: ~p", [JobId, JobState]),
            case JobState of
                pending ->
                    %% Check job dependencies before scheduling
                    case check_job_dependencies(JobId) of
                        {ok, satisfied} ->
                            %% Dependencies satisfied, proceed with scheduling
                            schedule_job_with_limits(JobId, Job, State);
                        {wait, deps_not_satisfied} ->
                            %% Dependencies not satisfied, skip this job
                            lager:debug("Job ~p waiting for dependencies", [JobId]),
                            {wait, dependencies_not_satisfied};
                        {error, DepError} ->
                            %% Dependency error (e.g., missing dependency job)
                            lager:warning("Job ~p dependency error: ~p", [JobId, DepError]),
                            {error, {dependency_error, DepError}}
                    end;
                held ->
                    %% Job is held (possibly for dependencies), skip
                    lager:debug("Job ~p is held, skipping", [JobId]),
                    {wait, job_held};
                _OtherState ->
                    %% Job is no longer pending
                    lager:info("Job ~p not pending, skipping", [JobId]),
                    {error, job_not_pending}
            end;
        {error, not_found} ->
            lager:warning("Job ~p not found in job_manager", [JobId]),
            io:format("*** SCHED: Job ~p NOT FOUND in job_manager!~n", [JobId]),
            {error, job_not_found}
    end.

%% @private
%% Check if a job's dependencies are satisfied
%% Returns {ok, satisfied} if all deps met, {wait, deps_not_satisfied} if waiting,
%% or {error, Reason} if there's a problem with the dependencies
check_job_dependencies(JobId) ->
    Result = catch flurm_job_deps:check_dependencies(JobId),
    case Result of
        {ok, []} ->
            %% No dependencies or all satisfied
            {ok, satisfied};
        {waiting, _UnsatisfiedDeps} ->
            %% Has unsatisfied dependencies
            {wait, deps_not_satisfied};
        {'EXIT', {noproc, _}} ->
            %% flurm_job_deps not running, assume no dependencies
            {ok, satisfied};
        {'EXIT', {badarg, _}} ->
            %% flurm_job_deps ETS table doesn't exist, assume no dependencies
            {ok, satisfied};
        {'EXIT', _Reason} ->
            %% Other error in deps module, assume no dependencies to not block jobs
            {ok, satisfied};
        _ ->
            %% Unexpected result - treat as satisfied to not block jobs
            {ok, satisfied}
    end.

%% @private
%% Schedule a job after dependency check passes
%% Checks resource limits and reservations before attempting allocation
%% Also attempts preemption if high-priority job can't be scheduled
schedule_job_with_limits(JobId, Job, State) ->
    %% Convert job record to info map for allocation
    Info = job_to_info(Job),
    %% Check resource limits before attempting to schedule
    LimitCheckSpec = job_to_limit_spec(Job),
    case flurm_limits:check_limits(LimitCheckSpec) of
        ok ->
            %% Limits OK, check reservation constraints
            case check_job_reservation_constraints(Job) of
                {ok, no_reservation} ->
                    %% No reservation, proceed with normal allocation (excluding reserved nodes)
                    case try_allocate_job_excluding_reserved(JobId, Info, State) of
                        {ok, NewState} ->
                            {ok, NewState};
                        {wait, insufficient_nodes} ->
                            %% Not enough resources - try preemption for high priority jobs
                            try_preemption_for_job(JobId, Job, Info, State);
                        Other ->
                            Other
                    end;
                {ok, use_reservation, ReservedNodes} ->
                    %% Job has valid reservation, use reserved nodes
                    lager:debug("Job ~p using reservation nodes: ~p", [JobId, ReservedNodes]),
                    try_allocate_job_with_reservation(JobId, Info, ReservedNodes, State);
                {wait, ReservationReason} ->
                    %% Reservation not yet active or other wait condition
                    lager:debug("Job ~p waiting for reservation: ~p", [JobId, ReservationReason]),
                    {wait, {reservation_wait, ReservationReason}};
                {error, ReservationError} ->
                    %% Reservation error (invalid, expired, no access)
                    lager:warning("Job ~p reservation error: ~p", [JobId, ReservationError]),
                    {error, {reservation_error, ReservationError}}
            end;
        {error, LimitReason} ->
            %% Limits exceeded, keep job pending
            lager:debug("Job ~p blocked by limits: ~p", [JobId, LimitReason]),
            {wait, {limit_exceeded, LimitReason}}
    end.

%% @private
%% Check reservation constraints for a job
%% Returns:
%%   {ok, no_reservation} - Job has no reservation, schedule normally
%%   {ok, use_reservation, Nodes} - Job has active reservation, use these nodes
%%   {wait, Reason} - Reservation not yet active, wait
%%   {error, Reason} - Reservation error
check_job_reservation_constraints(#job{reservation = <<>>}) ->
    {ok, no_reservation};
check_job_reservation_constraints(#job{reservation = undefined}) ->
    {ok, no_reservation};
check_job_reservation_constraints(#job{} = Job) ->
    %% Job has reservation specified, check it
    ResName = Job#job.reservation,
    case catch flurm_reservation:check_reservation_access(Job, ResName) of
        {ok, ReservedNodes} ->
            {ok, use_reservation, ReservedNodes};
        {error, {reservation_not_started, StartTime}} ->
            %% Reservation exists but not yet started
            {wait, {not_started, StartTime}};
        {error, access_denied} ->
            {error, reservation_access_denied};
        {error, reservation_not_found} ->
            {error, reservation_not_found};
        {error, reservation_expired} ->
            {error, reservation_expired};
        {error, Reason} ->
            {error, Reason};
        {'EXIT', {noproc, _}} ->
            %% flurm_reservation not running, assume no reservation system
            {ok, no_reservation};
        {'EXIT', Reason} ->
            {error, Reason}
    end.

%% @private
%% Try to allocate job excluding reserved nodes (for jobs without reservation)
try_allocate_job_excluding_reserved(JobId, JobInfo, State) ->
    NumNodes = maps:get(num_nodes, JobInfo, 1),
    NumCpus = maps:get(num_cpus, JobInfo, 1),
    Partition = maps:get(partition, JobInfo, <<"default">>),
    MemoryMb = maps:get(memory_mb, JobInfo, 256),
    Licenses = maps:get(licenses, JobInfo, []),

    lager:debug("try_allocate_job_excluding_reserved: job ~p needs ~p nodes, ~p cpus, ~p MB, partition ~p",
               [JobId, NumNodes, NumCpus, MemoryMb, Partition]),

    %% First check if required licenses are available
    case check_license_availability(Licenses) of
        {ok, available} ->
            %% Get available nodes excluding reserved ones
            case find_nodes_excluding_reserved(NumNodes, NumCpus, MemoryMb, Partition) of
                {ok, Nodes} ->
                    complete_job_allocation(JobId, JobInfo, Nodes, NumCpus, MemoryMb, Licenses, State);
                {wait, Reason} ->
                    {wait, Reason}
            end;
        {wait, licenses_unavailable} ->
            lager:info("Job ~p waiting for licenses: ~p", [JobId, Licenses]),
            {wait, licenses_unavailable}
    end.

%% @private
%% Try to allocate job using reserved nodes
try_allocate_job_with_reservation(JobId, JobInfo, ReservedNodes, State) ->
    NumNodes = maps:get(num_nodes, JobInfo, 1),
    NumCpus = maps:get(num_cpus, JobInfo, 1),
    MemoryMb = maps:get(memory_mb, JobInfo, 256),
    Partition = maps:get(partition, JobInfo, <<"default">>),
    Licenses = maps:get(licenses, JobInfo, []),

    lager:debug("try_allocate_job_with_reservation: job ~p needs ~p nodes from reserved: ~p",
               [JobId, NumNodes, ReservedNodes]),

    %% First check if required licenses are available
    case check_license_availability(Licenses) of
        {ok, available} ->
            %% Get available nodes from the reserved set only
            case find_nodes_from_reserved(NumNodes, NumCpus, MemoryMb, Partition, ReservedNodes) of
                {ok, Nodes} ->
                    %% Confirm reservation usage before allocation
                    ResName = maps:get(reservation, JobInfo, <<>>),
                    catch flurm_reservation:confirm_reservation(ResName),
                    complete_job_allocation(JobId, JobInfo, Nodes, NumCpus, MemoryMb, Licenses, State);
                {wait, Reason} ->
                    {wait, Reason}
            end;
        {wait, licenses_unavailable} ->
            lager:info("Job ~p waiting for licenses: ~p", [JobId, Licenses]),
            {wait, licenses_unavailable}
    end.

%% @private
%% Complete job allocation after nodes are selected
complete_job_allocation(JobId, JobInfo, Nodes, NumCpus, MemoryMb, Licenses, State) ->
    case allocate_job(JobId, Nodes, NumCpus, MemoryMb, 0) of
        ok ->
            %% Node resources allocated, now allocate licenses
            case allocate_licenses(JobId, Licenses) of
                ok ->
                    NodeNames = [N#node.hostname || N <- Nodes],
                    %% Update job in job manager with allocated nodes
                    flurm_job_manager:update_job(JobId, #{
                        state => configuring,
                        allocated_nodes => NodeNames
                    }),
                    %% Dispatch job to nodes
                    UpdatedInfo = JobInfo#{allocated_nodes => NodeNames},
                    case flurm_job_dispatcher:dispatch_job(JobId, UpdatedInfo) of
                        ok ->
                            %% Mark job as running
                            flurm_job_manager:update_job(JobId, #{state => running}),
                            %% Notify limits module that job has started
                            LimitInfo = build_limit_info(JobInfo),
                            flurm_limits:enforce_limit(start, JobId, LimitInfo),
                            NewRunning = sets:add_element(JobId, State#scheduler_state.running_jobs),
                            lager:info("Job ~p scheduled on nodes: ~p (licenses: ~p)", [JobId, NodeNames, Licenses]),
                            {ok, State#scheduler_state{running_jobs = NewRunning}};
                        {error, DispatchErr} ->
                            lager:warning("Job ~p dispatch failed: ~p, will retry", [JobId, DispatchErr]),
                            %% Release licenses
                            deallocate_licenses(JobId, Licenses),
                            %% Release node resources
                            lists:foreach(
                                fun(Node) ->
                                    flurm_node_manager:release_resources(Node#node.hostname, JobId)
                                end,
                                Nodes
                            ),
                            flurm_job_manager:update_job(JobId, #{
                                state => pending,
                                allocated_nodes => []
                            }),
                            {wait, {dispatch_failed, DispatchErr}}
                    end;
                {error, LicAllocErr} ->
                    %% License allocation failed - release node resources
                    lager:warning("Job ~p license allocation failed: ~p", [JobId, LicAllocErr]),
                    lists:foreach(
                        fun(Node) ->
                            flurm_node_manager:release_resources(Node#node.hostname, JobId)
                        end,
                        Nodes
                    ),
                    {wait, {licenses_unavailable, LicAllocErr}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% Find nodes excluding those in active reservations
find_nodes_excluding_reserved(NumNodes, NumCpus, MemoryMb, Partition) ->
    %% Get all available nodes
    AllNodes = flurm_node_manager:get_available_nodes_for_job(NumCpus, MemoryMb, Partition),
    AllNodeNames = [N#node.hostname || N <- AllNodes],

    %% Filter out reserved nodes
    AvailableNodeNames = case catch flurm_reservation:get_available_nodes_excluding_reserved(AllNodeNames) of
        FilteredNames when is_list(FilteredNames) -> FilteredNames;
        {'EXIT', _} -> AllNodeNames  % Reservation module not available
    end,

    %% Get the full node records for available nodes
    AvailableNodes = [N || N <- AllNodes, lists:member(N#node.hostname, AvailableNodeNames)],

    lager:debug("find_nodes_excluding_reserved: need ~p nodes with ~p cpus, ~p MB, found ~p after excluding reserved",
               [NumNodes, NumCpus, MemoryMb, length(AvailableNodes)]),

    if
        length(AvailableNodes) >= NumNodes ->
            SelectedNodes = lists:sublist(AvailableNodes, NumNodes),
            lager:debug("Selected nodes (excluding reserved): ~p", [[N#node.hostname || N <- SelectedNodes]]),
            {ok, SelectedNodes};
        true ->
            lager:warning("Insufficient nodes: need ~p, have ~p (after excluding reserved)",
                         [NumNodes, length(AvailableNodes)]),
            {wait, insufficient_nodes}
    end.

%% @private
%% Find nodes from a reserved set that can run the job
find_nodes_from_reserved(NumNodes, NumCpus, MemoryMb, Partition, ReservedNodeNames) ->
    %% Get all nodes with available resources
    AllNodes = flurm_node_manager:get_available_nodes_for_job(NumCpus, MemoryMb, Partition),

    %% Filter to only reserved nodes
    AvailableReserved = [N || N <- AllNodes, lists:member(N#node.hostname, ReservedNodeNames)],

    lager:debug("find_nodes_from_reserved: need ~p nodes, ~p available in reserved set",
               [NumNodes, length(AvailableReserved)]),

    if
        length(AvailableReserved) >= NumNodes ->
            SelectedNodes = lists:sublist(AvailableReserved, NumNodes),
            {ok, SelectedNodes};
        true ->
            {wait, {insufficient_reserved_nodes, length(AvailableReserved), NumNodes}}
    end.

%% @private
%% Attempt preemption for a high-priority job that can't be scheduled
%% Only preempts if the job has sufficient priority and preemption is enabled
try_preemption_for_job(JobId, Job, JobInfo, State) ->
    Priority = Job#job.priority,
    %% Only consider preemption for jobs above normal priority threshold
    MinPreemptPriority = ?DEFAULT_PRIORITY + flurm_preemption:get_priority_threshold(),
    case Priority >= MinPreemptPriority of
        false ->
            %% Job priority too low for preemption
            lager:debug("Job ~p priority ~p below preemption threshold ~p",
                       [JobId, Priority, MinPreemptPriority]),
            {wait, insufficient_nodes};
        true ->
            %% Try to find preemptable jobs
            ResourcesNeeded = #{
                num_nodes => Job#job.num_nodes,
                num_cpus => Job#job.num_cpus,
                memory_mb => Job#job.memory_mb
            },
            %% Add partition and QOS to job info for preemption mode lookup
            PendingJobInfo = JobInfo#{
                priority => Priority,
                partition => Job#job.partition,
                qos => Job#job.qos
            },
            case flurm_preemption:find_preemptable_jobs(PendingJobInfo, ResourcesNeeded) of
                {ok, JobsToPreempt} ->
                    lager:info("Job ~p (priority ~p): found ~p jobs to preempt",
                              [JobId, Priority, length(JobsToPreempt)]),
                    %% Execute preemption
                    execute_preemption_and_schedule(JobId, Job, JobInfo, JobsToPreempt, State);
                {error, Reason} ->
                    lager:debug("Job ~p: preemption not possible: ~p", [JobId, Reason]),
                    {wait, insufficient_nodes}
            end
    end.

%% @private
%% Execute preemption for selected jobs and then schedule the high-priority job
execute_preemption_and_schedule(JobId, Job, JobInfo, JobsToPreempt, State) ->
    %% Get preemption mode and grace time
    Partition = Job#job.partition,
    QOS = Job#job.qos,
    Mode = flurm_preemption:get_preemption_mode(#{partition => Partition, qos => QOS}),
    GraceTime = flurm_preemption:get_grace_time(Partition),

    %% Build preemption plan
    PreemptionPlan = #{
        jobs_to_preempt => JobsToPreempt,
        preemption_mode => Mode,
        grace_time => GraceTime,
        resources_freed => calculate_resources_to_free(JobsToPreempt)
    },

    %% Execute the preemption
    case flurm_preemption:execute_preemption(PreemptionPlan, JobInfo) of
        {ok, PreemptedJobIds} ->
            lager:info("Successfully preempted ~p jobs for job ~p: ~p",
                      [length(PreemptedJobIds), JobId, PreemptedJobIds]),
            %% Record metrics
            catch flurm_metrics:increment(flurm_preemptions_for_scheduling, length(PreemptedJobIds)),

            %% Wait a moment for resources to be released
            timer:sleep(100),

            %% Now try to schedule the high-priority job again
            case try_allocate_job_simple(JobId, JobInfo, State) of
                {ok, NewState} ->
                    lager:info("Job ~p scheduled after preemption", [JobId]),
                    {ok, NewState};
                Other ->
                    %% Still couldn't schedule - resources may not be freed yet
                    lager:warning("Job ~p still couldn't be scheduled after preemption: ~p",
                                 [JobId, Other]),
                    Other
            end;
        {error, Reason} ->
            lager:warning("Preemption failed for job ~p: ~p", [JobId, Reason]),
            {wait, {preemption_failed, Reason}}
    end.

%% @private
%% Calculate total resources that would be freed by preempting jobs
calculate_resources_to_free(Jobs) ->
    lists:foldl(
        fun(JobMap, Acc) ->
            #{
                nodes => maps:get(nodes, Acc, 0) + maps:get(num_nodes, JobMap, 1),
                cpus => maps:get(cpus, Acc, 0) + maps:get(num_cpus, JobMap, 1),
                memory_mb => maps:get(memory_mb, Acc, 0) + maps:get(memory_mb, JobMap, 1024)
            }
        end,
        #{nodes => 0, cpus => 0, memory_mb => 0},
        Jobs
    ).

%% @private
%% Find nodes that can run a job with optional GRES requirements
%% GRESSpec can be <<>> for no GRES, or a string like "gpu:2" or "gpu:a100:4"
find_nodes_for_job(NumNodes, NumCpus, MemoryMb, Partition, GRESSpec) ->
    %% Get nodes with available resources from node manager
    AllNodes = case GRESSpec of
        <<>> ->
            flurm_node_manager:get_available_nodes_for_job(NumCpus, MemoryMb, Partition);
        _ ->
            %% Use GRES-aware node selection
            flurm_node_manager:get_available_nodes_with_gres(NumCpus, MemoryMb, Partition, GRESSpec)
    end,
    lager:debug("find_nodes_for_job: need ~p nodes with ~p cpus, ~p MB, GRES ~p for partition ~p, found ~p nodes",
                [NumNodes, NumCpus, MemoryMb, GRESSpec, Partition, length(AllNodes)]),

    %% If GRES is specified, also filter through flurm_gres module for advanced constraints
    FilteredNodes = case GRESSpec of
        <<>> -> AllNodes;
        _ ->
            NodeHostnames = [N#node.hostname || N <- AllNodes],
            FilteredHostnames = flurm_gres:filter_nodes_by_gres(NodeHostnames, GRESSpec),
            [N || N <- AllNodes, lists:member(N#node.hostname, FilteredHostnames)]
    end,

    %% Check if we have enough nodes
    if
        length(FilteredNodes) >= NumNodes ->
            %% Take first N nodes (could be improved with better selection based on GRES topology)
            SelectedNodes = lists:sublist(FilteredNodes, NumNodes),
            lager:debug("Selected nodes: ~p", [[N#node.hostname || N <- SelectedNodes]]),
            {ok, SelectedNodes};
        true ->
            case GRESSpec of
                <<>> ->
                    lager:warning("Insufficient nodes: need ~p, have ~p", [NumNodes, length(FilteredNodes)]);
                _ ->
                    lager:warning("Insufficient nodes with GRES ~p: need ~p, have ~p",
                                  [GRESSpec, NumNodes, length(FilteredNodes)])
            end,
            {wait, insufficient_nodes}
    end.

%% @private
%% Allocate resources on nodes for a job
%% Uses flurm_node_manager for connected compute nodes
allocate_job(JobId, Nodes, Cpus, Memory, _Gpus) ->
    %% Try to allocate on all nodes
    Results = lists:map(
        fun(#node{hostname = Hostname}) ->
            flurm_node_manager:allocate_resources(Hostname, JobId, Cpus, Memory)
        end,
        Nodes
    ),

    %% Check if all allocations succeeded
    case lists:all(fun(R) -> R =:= ok end, Results) of
        true ->
            ok;
        false ->
            %% Rollback successful allocations
            lists:foreach(
                fun({#node{hostname = Hostname}, Result}) ->
                    case Result of
                        ok -> flurm_node_manager:release_resources(Hostname, JobId);
                        _ -> ok
                    end
                end,
                lists:zip(Nodes, Results)
            ),
            {error, allocation_failed}
    end.

%% @private
%% Convert a job record to an info map for scheduling
job_to_info(#job{} = Job) ->
    #{
        job_id => Job#job.id,
        name => Job#job.name,
        state => Job#job.state,
        partition => Job#job.partition,
        num_nodes => Job#job.num_nodes,
        num_cpus => Job#job.num_cpus,
        memory_mb => Job#job.memory_mb,
        time_limit => Job#job.time_limit,
        script => Job#job.script,
        allocated_nodes => Job#job.allocated_nodes,
        work_dir => Job#job.work_dir,
        std_out => Job#job.std_out,
        std_err => Job#job.std_err,
        user => Job#job.user,
        account => Job#job.account,
        licenses => Job#job.licenses,
        %% GRES fields for GPU/accelerator scheduling
        gres => Job#job.gres,
        gres_per_node => Job#job.gres_per_node,
        gres_per_task => Job#job.gres_per_task,
        gpu_type => Job#job.gpu_type,
        gpu_memory_mb => Job#job.gpu_memory_mb,
        gpu_exclusive => Job#job.gpu_exclusive
    }.

%% @private
%% Convert a job record to a limit check spec for flurm_limits:check_limits/1
%% The spec must include user, account, partition, and resource requirements
job_to_limit_spec(#job{} = Job) ->
    #{
        user => Job#job.user,
        account => Job#job.account,
        partition => Job#job.partition,
        num_nodes => Job#job.num_nodes,
        num_cpus => Job#job.num_cpus,
        memory_mb => Job#job.memory_mb,
        time_limit => Job#job.time_limit
    }.

%% @private
%% Build info map for flurm_limits:enforce_limit/3
%% Contains user, account, and TRES (trackable resources) for usage accounting
build_limit_info(JobInfo) ->
    #{
        user => maps:get(user, JobInfo, <<"unknown">>),
        account => maps:get(account, JobInfo, <<>>),
        tres => #{
            cpu => maps:get(num_cpus, JobInfo, 1),
            mem => maps:get(memory_mb, JobInfo, 0),
            node => maps:get(num_nodes, JobInfo, 1)
        }
    }.

%% @private
%% Simple allocation without flurm_job process
%% Used when jobs are stored in flurm_job_manager instead of flurm_job_registry
%% Now includes license checking, GRES allocation, and license allocation
try_allocate_job_simple(JobId, JobInfo, State) ->
    NumNodes = maps:get(num_nodes, JobInfo, 1),
    NumCpus = maps:get(num_cpus, JobInfo, 1),
    Partition = maps:get(partition, JobInfo, <<"default">>),
    MemoryMb = maps:get(memory_mb, JobInfo, 256),  % Default 256 MB for container compatibility
    Licenses = maps:get(licenses, JobInfo, []),
    %% Get GRES requirements
    GRESSpec = maps:get(gres, JobInfo, <<>>),
    GPUExclusive = maps:get(gpu_exclusive, JobInfo, true),
    lager:debug("try_allocate_job_simple: job ~p needs ~p nodes, ~p cpus, ~p MB, partition ~p, licenses ~p, gres ~p",
               [JobId, NumNodes, NumCpus, MemoryMb, Partition, Licenses, GRESSpec]),

    %% First check if required licenses are available
    case check_license_availability(Licenses) of
        {ok, available} ->
            %% Licenses available, continue with node allocation (including GRES filtering)
            case find_nodes_for_job(NumNodes, NumCpus, MemoryMb, Partition, GRESSpec) of
                {ok, Nodes} ->
                    case allocate_job(JobId, Nodes, NumCpus, MemoryMb, 0) of
                        ok ->
                            %% Node CPU/memory resources allocated, now allocate GRES if needed
                            case allocate_gres_on_nodes(JobId, Nodes, GRESSpec, GPUExclusive) of
                                {ok, _GRESAllocations} ->
                                    %% GRES allocated, now allocate licenses
                                    case allocate_licenses(JobId, Licenses) of
                                        ok ->
                                            NodeNames = [N#node.hostname || N <- Nodes],
                                            %% Update job in job manager with allocated nodes
                                            flurm_job_manager:update_job(JobId, #{
                                                state => configuring,
                                                allocated_nodes => NodeNames
                                            }),
                                            %% Dispatch job to nodes
                                            UpdatedInfo = JobInfo#{allocated_nodes => NodeNames},
                                            case flurm_job_dispatcher:dispatch_job(JobId, UpdatedInfo) of
                                                ok ->
                                                    %% Mark job as running
                                                    flurm_job_manager:update_job(JobId, #{state => running}),
                                                    %% Notify limits module that job has started (for usage tracking)
                                                    LimitInfo = build_limit_info(JobInfo),
                                                    flurm_limits:enforce_limit(start, JobId, LimitInfo),
                                                    NewRunning = sets:add_element(JobId, State#scheduler_state.running_jobs),
                                                    lager:info("Job ~p scheduled on nodes: ~p (licenses: ~p, gres: ~p)",
                                                              [JobId, NodeNames, Licenses, GRESSpec]),
                                                    {ok, State#scheduler_state{running_jobs = NewRunning}};
                                                {error, DispatchErr} ->
                                                    lager:warning("Job ~p dispatch failed: ~p, will retry", [JobId, DispatchErr]),
                                                    %% Rollback: release licenses
                                                    deallocate_licenses(JobId, Licenses),
                                                    %% Rollback: release GRES
                                                    release_gres_on_nodes(JobId, Nodes),
                                                    %% Rollback: release node resources
                                                    lists:foreach(
                                                        fun(Node) ->
                                                            flurm_node_manager:release_resources(Node#node.hostname, JobId)
                                                        end,
                                                        Nodes
                                                    ),
                                                    flurm_job_manager:update_job(JobId, #{
                                                        state => pending,
                                                        allocated_nodes => []
                                                    }),
                                                    %% Return wait so job stays in queue and can retry
                                                    {wait, {dispatch_failed, DispatchErr}}
                                            end;
                                        {error, LicAllocErr} ->
                                            %% License allocation failed - rollback GRES and node resources
                                            lager:warning("Job ~p license allocation failed: ~p", [JobId, LicAllocErr]),
                                            release_gres_on_nodes(JobId, Nodes),
                                            lists:foreach(
                                                fun(Node) ->
                                                    flurm_node_manager:release_resources(Node#node.hostname, JobId)
                                                end,
                                                Nodes
                                            ),
                                            {wait, {licenses_unavailable, LicAllocErr}}
                                    end;
                                {error, GRESErr} ->
                                    %% GRES allocation failed - rollback node resources
                                    lager:warning("Job ~p GRES allocation failed: ~p", [JobId, GRESErr]),
                                    lists:foreach(
                                        fun(Node) ->
                                            flurm_node_manager:release_resources(Node#node.hostname, JobId)
                                        end,
                                        Nodes
                                    ),
                                    {wait, {gres_unavailable, GRESErr}}
                            end;
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {wait, Reason} ->
                    {wait, Reason}
            end;
        {wait, licenses_unavailable} ->
            %% Licenses not available, job must wait
            lager:info("Job ~p waiting for licenses: ~p", [JobId, Licenses]),
            {wait, licenses_unavailable}
    end.

%% @private
%% Allocate GRES on all nodes for a job
%% Returns {ok, Allocations} or {error, Reason}
allocate_gres_on_nodes(_JobId, _Nodes, <<>>, _Exclusive) ->
    {ok, []};  % No GRES requirements
allocate_gres_on_nodes(JobId, Nodes, GRESSpec, Exclusive) ->
    %% Try to allocate GRES on each node
    Results = lists:map(
        fun(#node{hostname = Hostname}) ->
            case flurm_node_manager:allocate_gres(Hostname, JobId, GRESSpec, Exclusive) of
                {ok, Alloc} -> {ok, {Hostname, Alloc}};
                {error, Reason} -> {error, {Hostname, Reason}}
            end
        end,
        Nodes
    ),
    %% Check if all allocations succeeded
    Errors = [E || {error, E} <- Results],
    case Errors of
        [] ->
            Allocations = [{H, A} || {ok, {H, A}} <- Results],
            %% Also register with flurm_gres module
            lists:foreach(
                fun(#node{hostname = Hostname}) ->
                    catch flurm_gres:allocate(JobId, Hostname, GRESSpec)
                end,
                Nodes
            ),
            {ok, Allocations};
        [FirstErr | _] ->
            %% Rollback successful allocations
            lists:foreach(
                fun
                    ({ok, {Hostname, _}}) -> flurm_node_manager:release_gres(Hostname, JobId);
                    (_) -> ok
                end,
                Results
            ),
            {error, FirstErr}
    end.

%% @private
%% Release GRES on all nodes for a job
release_gres_on_nodes(JobId, Nodes) ->
    lists:foreach(
        fun(#node{hostname = Hostname}) ->
            flurm_node_manager:release_gres(Hostname, JobId),
            catch flurm_gres:deallocate(JobId, Hostname)
        end,
        Nodes
    ).

%% @private
%% Check if licenses are available for the job
%% Returns {ok, available} or {wait, licenses_unavailable}
check_license_availability([]) ->
    {ok, available};
check_license_availability(Licenses) ->
    case flurm_license:check_availability(Licenses) of
        true -> {ok, available};
        false -> {wait, licenses_unavailable}
    end.

%% @private
%% Allocate licenses for the job
allocate_licenses(_JobId, []) ->
    ok;
allocate_licenses(JobId, Licenses) ->
    flurm_license:allocate(JobId, Licenses).

%% @private
%% Deallocate licenses for the job
deallocate_licenses(_JobId, []) ->
    ok;
deallocate_licenses(JobId, Licenses) ->
    flurm_license:deallocate(JobId, Licenses).
