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
    get_stats/0
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
    pending_reasons :: #{pos_integer() => {term(), non_neg_integer()}}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the scheduler.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
        pending_reasons = #{}
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
    lager:info("Scheduler received job ~p for scheduling", [JobId]),
    %% Add job to pending queue
    NewPending = queue:in(JobId, State#scheduler_state.pending_jobs),
    NewState = State#scheduler_state{pending_jobs = NewPending},
    %% Trigger immediate scheduling attempt
    self() ! schedule_cycle,
    {noreply, NewState};

handle_cast({job_completed, JobId}, State) ->
    NewState = handle_job_finished(JobId, State),
    FinalState = NewState#scheduler_state{
        completed_count = NewState#scheduler_state.completed_count + 1
    },
    %% Trigger scheduling to fill freed resources
    self() ! schedule_cycle,
    {noreply, FinalState};

handle_cast({job_failed, JobId}, State) ->
    NewState = handle_job_finished(JobId, State),
    FinalState = NewState#scheduler_state{
        failed_count = NewState#scheduler_state.failed_count + 1
    },
    %% Trigger scheduling to fill freed resources
    self() ! schedule_cycle,
    {noreply, FinalState};

handle_cast(trigger_schedule, State) ->
    self() ! schedule_cycle,
    {noreply, State};

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

    %% Run scheduling cycle
    NewState = schedule_cycle(State),

    %% Record metrics
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
    self() ! schedule_cycle,
    {noreply, State};

handle_info({config_changed, nodes, _OldNodes, NewNodes}, State) ->
    lager:info("Scheduler received node config change: ~p node definitions", [length(NewNodes)]),
    %% Trigger a scheduling cycle to re-evaluate jobs with new node definitions
    self() ! schedule_cycle,
    {noreply, State};

handle_info({config_changed, schedulertype, _OldType, NewType}, State) ->
    lager:info("Scheduler type changed to ~p - applying new scheduler configuration", [NewType]),
    %% Could implement scheduler type switching here (FIFO, backfill, etc.)
    %% For now, just trigger a schedule cycle
    self() ! schedule_cycle,
    {noreply, State};

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
%% Handle job finished (completed or failed) - release resources
handle_job_finished(JobId, State) ->
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
                account => <<>>,  % TODO: Add account field to job record if needed
                tres => #{
                    cpu => Job#job.num_cpus,
                    mem => Job#job.memory_mb,
                    node => Job#job.num_nodes
                },
                wall_time => WallTime
            },
            flurm_limits:enforce_limit(stop, JobId, LimitInfo);
        _ ->
            ok
    end,

    State#scheduler_state{running_jobs = NewRunning}.

%% @private
%% Run a scheduling cycle - try to schedule pending jobs
schedule_cycle(State) ->
    PendingCount = queue:len(State#scheduler_state.pending_jobs),
    case PendingCount > 0 of
        true ->
            lager:debug("Schedule cycle: ~p pending jobs", [PendingCount]);
        false ->
            ok
    end,
    %% Process pending jobs in FIFO order
    schedule_pending_jobs(State).

%% @private
%% Try to schedule pending jobs with backfill support
schedule_pending_jobs(State) ->
    case queue:out(State#scheduler_state.pending_jobs) of
        {empty, _} ->
            %% No pending jobs
            State;
        {{value, JobId}, RestQueue} ->
            %% Try to schedule this job (highest priority / FIFO first)
            case try_schedule_job(JobId, State) of
                {ok, NewState} ->
                    %% Job scheduled, continue with rest of queue
                    schedule_pending_jobs(NewState#scheduler_state{
                        pending_jobs = RestQueue
                    });
                {wait, Reason} ->
                    %% Job cannot be scheduled (insufficient resources)
                    %% Try backfill: schedule smaller jobs that can fit
                    lager:info("Job ~p waiting (~p), trying backfill", [JobId, Reason]),
                    BackfillState = try_backfill_jobs(RestQueue, State),
                    %% Keep original job at front of queue
                    BackfillState;
                {error, job_not_found} ->
                    %% Job no longer exists, skip it
                    schedule_pending_jobs(State#scheduler_state{
                        pending_jobs = RestQueue
                    });
                {error, _Reason} ->
                    %% Other error, skip this job
                    schedule_pending_jobs(State#scheduler_state{
                        pending_jobs = RestQueue
                    })
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
try_schedule_job(JobId, State) ->
    lager:info("Trying to schedule job ~p", [JobId]),
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            JobState = flurm_core:job_state(Job),
            lager:debug("Job ~p state: ~p", [JobId, JobState]),
            case JobState of
                pending ->
                    %% Convert job record to info map for allocation
                    Info = job_to_info(Job),
                    %% Check resource limits before attempting to schedule
                    LimitCheckSpec = job_to_limit_spec(Job),
                    case flurm_limits:check_limits(LimitCheckSpec) of
                        ok ->
                            %% Limits OK, proceed with allocation
                            try_allocate_job_simple(JobId, Info, State);
                        {error, LimitReason} ->
                            %% Limits exceeded, keep job pending
                            lager:info("Job ~p blocked by limits: ~p", [JobId, LimitReason]),
                            {wait, {limit_exceeded, LimitReason}}
                    end;
                _OtherState ->
                    %% Job is no longer pending
                    lager:info("Job ~p not pending, skipping", [JobId]),
                    {error, job_not_pending}
            end;
        {error, not_found} ->
            lager:warning("Job ~p not found in job_manager", [JobId]),
            {error, job_not_found}
    end.

%% @private
%% Find nodes that can run a job
%% Uses flurm_node_manager to find connected compute nodes with available resources
find_nodes_for_job(NumNodes, NumCpus, MemoryMb, Partition) ->
    %% Get nodes with available resources from node manager
    AllNodes = flurm_node_manager:get_available_nodes_for_job(NumCpus, MemoryMb, Partition),
    lager:info("find_nodes_for_job: need ~p nodes with ~p cpus, ~p MB for partition ~p, found ~p nodes",
                [NumNodes, NumCpus, MemoryMb, Partition, length(AllNodes)]),

    %% Check if we have enough nodes
    if
        length(AllNodes) >= NumNodes ->
            %% Take first N nodes (could be improved with better selection)
            SelectedNodes = lists:sublist(AllNodes, NumNodes),
            lager:info("Selected nodes: ~p", [[N#node.hostname || N <- SelectedNodes]]),
            {ok, SelectedNodes};
        true ->
            lager:warning("Insufficient nodes: need ~p, have ~p", [NumNodes, length(AllNodes)]),
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
        user => Job#job.user
    }.

%% @private
%% Convert a job record to a limit check spec for flurm_limits:check_limits/1
%% The spec must include user, account, partition, and resource requirements
job_to_limit_spec(#job{} = Job) ->
    #{
        user => Job#job.user,
        account => <<>>,  % TODO: Add account field to job record if needed
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
try_allocate_job_simple(JobId, JobInfo, State) ->
    NumNodes = maps:get(num_nodes, JobInfo, 1),
    NumCpus = maps:get(num_cpus, JobInfo, 1),
    Partition = maps:get(partition, JobInfo, <<"default">>),
    MemoryMb = maps:get(memory_mb, JobInfo, 256),  % Default 256 MB for container compatibility
    lager:debug("try_allocate_job_simple: job ~p needs ~p nodes, ~p cpus, ~p MB, partition ~p",
               [JobId, NumNodes, NumCpus, MemoryMb, Partition]),

    case find_nodes_for_job(NumNodes, NumCpus, MemoryMb, Partition) of
        {ok, Nodes} ->
            case allocate_job(JobId, Nodes, NumCpus, MemoryMb, 0) of
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
                            lager:info("Job ~p scheduled on nodes: ~p", [JobId, NodeNames]),
                            {ok, State#scheduler_state{running_jobs = NewRunning}};
                        {error, DispatchErr} ->
                            lager:warning("Job ~p dispatch failed: ~p, will retry", [JobId, DispatchErr]),
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
                {error, Reason} ->
                    {error, Reason}
            end;
        {wait, Reason} ->
            {wait, Reason}
    end.
