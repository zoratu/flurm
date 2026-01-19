%%%-------------------------------------------------------------------
%%% @doc FLURM Advanced Scheduler
%%%
%%% A comprehensive scheduler implementing:
%%% - Multi-factor job priority (age, size, partition, QOS, fairshare)
%%% - Backfill scheduling for improved utilization
%%% - Job preemption for high-priority jobs
%%% - Fair-share aware scheduling
%%%
%%% Scheduling Algorithm:
%%% 1. Sort pending jobs by calculated priority (highest first)
%%% 2. Try to schedule highest priority job
%%% 3. If blocked, consider backfill opportunities
%%% 4. If high-priority job still can't run, consider preemption
%%% 5. Repeat for remaining pending jobs
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_advanced).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    start_link/1,
    submit_job/1,
    job_completed/1,
    job_failed/1,
    trigger_schedule/0,
    get_stats/0,
    enable_backfill/1,
    enable_preemption/1,
    set_scheduler_type/1
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
    insert_by_priority/3,
    recalculate_priorities/1,
    cancel_timer/1
]).
-endif.

%% Scheduler types
-define(SCHED_FIFO, fifo).
-define(SCHED_PRIORITY, priority).
-define(SCHED_BACKFILL, backfill).
-define(SCHED_PREEMPT, preempt).

%% Internal state
-record(state, {
    pending_jobs    :: [pos_integer()],  % Sorted by priority
    running_jobs    :: sets:set(pos_integer()),
    schedule_timer  :: reference() | undefined,
    schedule_cycles :: non_neg_integer(),
    completed_count :: non_neg_integer(),
    failed_count    :: non_neg_integer(),
    preempt_count   :: non_neg_integer(),
    backfill_count  :: non_neg_integer(),
    scheduler_type  :: atom(),
    backfill_enabled :: boolean(),
    preemption_enabled :: boolean(),
    priority_decay_timer :: reference() | undefined
}).

%% Priority recalculation interval (recalc age factors)
-define(PRIORITY_DECAY_INTERVAL, 60000).  % 1 minute

%%====================================================================
%% API
%%====================================================================

%% @doc Start the advanced scheduler with default options.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start the scheduler with options.
%% Options: [{scheduler_type, fifo|priority|backfill|preempt}]
-spec start_link(proplists:proplist()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Options) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Options, []).

%% @doc Submit a job to the scheduler.
-spec submit_job(pos_integer()) -> ok.
submit_job(JobId) when is_integer(JobId), JobId > 0 ->
    gen_server:cast(?SERVER, {submit_job, JobId}).

%% @doc Notify scheduler of job completion.
-spec job_completed(pos_integer()) -> ok.
job_completed(JobId) when is_integer(JobId), JobId > 0 ->
    gen_server:cast(?SERVER, {job_completed, JobId}).

%% @doc Notify scheduler of job failure.
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

%% @doc Enable or disable backfill scheduling.
-spec enable_backfill(boolean()) -> ok.
enable_backfill(Enable) ->
    gen_server:call(?SERVER, {enable_backfill, Enable}).

%% @doc Enable or disable preemption.
-spec enable_preemption(boolean()) -> ok.
enable_preemption(Enable) ->
    gen_server:call(?SERVER, {enable_preemption, Enable}).

%% @doc Set scheduler type.
-spec set_scheduler_type(atom()) -> ok | {error, invalid_type}.
set_scheduler_type(Type) when Type =:= fifo;
                              Type =:= priority;
                              Type =:= backfill;
                              Type =:= preempt ->
    gen_server:call(?SERVER, {set_scheduler_type, Type}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Options) ->
    SchedType = proplists:get_value(scheduler_type, Options, ?SCHED_BACKFILL),
    BackfillEnabled = proplists:get_value(backfill_enabled, Options, true),
    PreemptionEnabled = proplists:get_value(preemption_enabled, Options, false),

    %% Start periodic scheduling
    Timer = schedule_next_cycle(),

    %% Start priority decay timer
    PriorityTimer = erlang:send_after(?PRIORITY_DECAY_INTERVAL, self(),
                                      priority_decay),

    State = #state{
        pending_jobs = [],
        running_jobs = sets:new(),
        schedule_timer = Timer,
        schedule_cycles = 0,
        completed_count = 0,
        failed_count = 0,
        preempt_count = 0,
        backfill_count = 0,
        scheduler_type = SchedType,
        backfill_enabled = BackfillEnabled,
        preemption_enabled = PreemptionEnabled,
        priority_decay_timer = PriorityTimer
    },

    {ok, State}.

handle_call(get_stats, _From, State) ->
    Stats = #{
        pending_count => length(State#state.pending_jobs),
        running_count => sets:size(State#state.running_jobs),
        completed_count => State#state.completed_count,
        failed_count => State#state.failed_count,
        preempt_count => State#state.preempt_count,
        backfill_count => State#state.backfill_count,
        schedule_cycles => State#state.schedule_cycles,
        scheduler_type => State#state.scheduler_type,
        backfill_enabled => State#state.backfill_enabled,
        preemption_enabled => State#state.preemption_enabled
    },
    {reply, {ok, Stats}, State};

handle_call({enable_backfill, Enable}, _From, State) ->
    {reply, ok, State#state{backfill_enabled = Enable}};

handle_call({enable_preemption, Enable}, _From, State) ->
    {reply, ok, State#state{preemption_enabled = Enable}};

handle_call({set_scheduler_type, Type}, _From, State) ->
    {reply, ok, State#state{scheduler_type = Type}};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({submit_job, JobId}, State) ->
    %% Calculate initial priority
    Priority = calculate_job_priority(JobId),

    %% Insert into sorted pending list
    NewPending = insert_by_priority(JobId, Priority, State#state.pending_jobs),
    NewState = State#state{pending_jobs = NewPending},

    %% Trigger immediate scheduling
    self() ! schedule_cycle,
    {noreply, NewState};

handle_cast({job_completed, JobId}, State) ->
    NewState = handle_job_finished(JobId, State),
    FinalState = NewState#state{
        completed_count = NewState#state.completed_count + 1
    },
    %% Record usage for fair-share
    record_job_usage(JobId),
    %% Trigger scheduling
    self() ! schedule_cycle,
    {noreply, FinalState};

handle_cast({job_failed, JobId}, State) ->
    NewState = handle_job_finished(JobId, State),
    FinalState = NewState#state{
        failed_count = NewState#state.failed_count + 1
    },
    self() ! schedule_cycle,
    {noreply, FinalState};

handle_cast(trigger_schedule, State) ->
    self() ! schedule_cycle,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(schedule_cycle, State) ->
    %% Cancel existing timer
    cancel_timer(State#state.schedule_timer),

    %% Run scheduling cycle
    NewState = run_schedule_cycle(State),

    %% Schedule next cycle
    Timer = schedule_next_cycle(),
    FinalState = NewState#state{
        schedule_timer = Timer,
        schedule_cycles = NewState#state.schedule_cycles + 1
    },
    {noreply, FinalState};

handle_info(priority_decay, State) ->
    %% Recalculate priorities for all pending jobs
    NewPending = recalculate_priorities(State#state.pending_jobs),

    %% Schedule next decay
    Timer = erlang:send_after(?PRIORITY_DECAY_INTERVAL, self(), priority_decay),

    {noreply, State#state{
        pending_jobs = NewPending,
        priority_decay_timer = Timer
    }};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    cancel_timer(State#state.schedule_timer),
    cancel_timer(State#state.priority_decay_timer),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions - Scheduling
%%====================================================================

%% @private
schedule_next_cycle() ->
    erlang:send_after(?SCHEDULE_INTERVAL, self(), schedule_cycle).

%% @private
cancel_timer(undefined) -> ok;
cancel_timer(Timer) -> erlang:cancel_timer(Timer).

%% @private
%% Main scheduling cycle
run_schedule_cycle(State) ->
    case State#state.scheduler_type of
        ?SCHED_FIFO ->
            schedule_fifo(State);
        ?SCHED_PRIORITY ->
            schedule_priority(State);
        ?SCHED_BACKFILL ->
            schedule_with_backfill(State);
        ?SCHED_PREEMPT ->
            schedule_with_preemption(State)
    end.

%% @private
%% Simple FIFO scheduling
schedule_fifo(State) ->
    schedule_in_order(State#state.pending_jobs, State).

%% @private
%% Priority-based scheduling (no backfill)
schedule_priority(State) ->
    %% Jobs already sorted by priority
    schedule_in_order(State#state.pending_jobs, State).

%% @private
%% Schedule jobs in order until one blocks
schedule_in_order([], State) ->
    State;
schedule_in_order([JobId | Rest], State) ->
    case try_schedule_job(JobId, State) of
        {ok, NewState} ->
            NewPending = lists:delete(JobId, NewState#state.pending_jobs),
            schedule_in_order(Rest, NewState#state{pending_jobs = NewPending});
        {wait, _Reason} ->
            %% First job can't run, stop (FIFO-like behavior)
            State;
        {error, job_not_found} ->
            %% Job gone, skip it
            NewPending = lists:delete(JobId, State#state.pending_jobs),
            schedule_in_order(Rest, State#state{pending_jobs = NewPending});
        {error, _Reason} ->
            %% Other error, skip
            NewPending = lists:delete(JobId, State#state.pending_jobs),
            schedule_in_order(Rest, State#state{pending_jobs = NewPending})
    end.

%% @private
%% Schedule with backfill enabled
schedule_with_backfill(State) ->
    case State#state.pending_jobs of
        [] ->
            State;
        [TopJob | Rest] ->
            %% Try to schedule highest priority job
            case try_schedule_job(TopJob, State) of
                {ok, NewState} ->
                    NewPending = Rest,
                    %% Continue with remaining jobs
                    schedule_with_backfill(NewState#state{pending_jobs = NewPending});

                {wait, _Reason} ->
                    %% Top job is blocked, try backfill
                    case State#state.backfill_enabled of
                        true ->
                            try_backfill(TopJob, Rest, State);
                        false ->
                            State
                    end;

                {error, job_not_found} ->
                    NewPending = Rest,
                    schedule_with_backfill(State#state{pending_jobs = NewPending});

                {error, _} ->
                    NewPending = Rest,
                    schedule_with_backfill(State#state{pending_jobs = NewPending})
            end
    end.

%% @private
%% Try backfill scheduling
try_backfill(BlockerJobId, CandidateJobIds, State) ->
    %% Get blocker job info
    BlockerJob = get_job_info_map(BlockerJobId),

    %% Get candidate job infos
    CandidateJobs = lists:filtermap(
        fun(JobId) ->
            case get_job_info_map(JobId) of
                undefined -> false;
                Info -> {true, Info#{job_id => JobId}}
            end
        end,
        CandidateJobIds
    ),

    %% Find jobs that can backfill
    BackfillJobs = flurm_backfill:find_backfill_jobs(BlockerJob, CandidateJobs),

    %% Schedule backfill jobs
    lists:foldl(
        fun({JobId, _Pid, _Nodes}, AccState) ->
            case try_schedule_job(JobId, AccState) of
                {ok, NewState} ->
                    NewPending = lists:delete(JobId, NewState#state.pending_jobs),
                    NewState#state{
                        pending_jobs = NewPending,
                        backfill_count = NewState#state.backfill_count + 1
                    };
                _ ->
                    AccState
            end
        end,
        State,
        BackfillJobs
    ).

%% @private
%% Schedule with preemption enabled
schedule_with_preemption(State) ->
    case State#state.pending_jobs of
        [] ->
            State;
        [TopJob | Rest] ->
            case try_schedule_job(TopJob, State) of
                {ok, NewState} ->
                    schedule_with_preemption(
                        NewState#state{pending_jobs = Rest});

                {wait, _Reason} when State#state.preemption_enabled ->
                    %% Try preemption for high-priority job
                    JobInfo = get_job_info_map(TopJob),
                    case flurm_preemption:check_preemption(JobInfo) of
                        {ok, ToPreempt} ->
                            %% Preempt jobs and retry
                            Mode = flurm_preemption:get_preemption_mode(),
                            {ok, PreemptedIds} = flurm_preemption:preempt_jobs(
                                ToPreempt, Mode),

                            %% Remove preempted from running
                            NewRunning = lists:foldl(
                                fun(Id, Acc) -> sets:del_element(Id, Acc) end,
                                State#state.running_jobs,
                                PreemptedIds
                            ),

                            %% Requeue preempted jobs (for requeue mode)
                            NewPending = case Mode of
                                requeue ->
                                    State#state.pending_jobs ++ PreemptedIds;
                                _ ->
                                    State#state.pending_jobs
                            end,

                            %% Retry scheduling
                            NewState = State#state{
                                running_jobs = NewRunning,
                                pending_jobs = NewPending,
                                preempt_count = State#state.preempt_count +
                                               length(PreemptedIds)
                            },
                            schedule_with_preemption(NewState);

                        {error, _} ->
                            %% Can't preempt, try backfill
                            try_backfill(TopJob, Rest, State)
                    end;

                {wait, _Reason} ->
                    %% Preemption disabled, try backfill
                    try_backfill(TopJob, Rest, State);

                {error, job_not_found} ->
                    schedule_with_preemption(
                        State#state{pending_jobs = Rest});

                {error, _} ->
                    schedule_with_preemption(
                        State#state{pending_jobs = Rest})
            end
    end.

%%====================================================================
%% Internal functions - Job Management
%%====================================================================

%% @private
%% Handle job finished (remove from running, release resources)
handle_job_finished(JobId, State) ->
    NewRunning = sets:del_element(JobId, State#state.running_jobs),

    %% Release resources
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} ->
            case catch flurm_job:get_info(Pid) of
                {ok, Info} ->
                    AllocatedNodes = maps:get(allocated_nodes, Info, []),
                    lists:foreach(
                        fun(NodeName) ->
                            catch flurm_node:release(NodeName, JobId)
                        end,
                        AllocatedNodes
                    );
                _ ->
                    ok
            end;
        _ ->
            ok
    end,

    State#state{running_jobs = NewRunning}.

%% @private
%% Try to schedule a specific job
try_schedule_job(JobId, State) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} ->
            case catch flurm_job:get_info(Pid) of
                {ok, Info} ->
                    case maps:get(state, Info) of
                        pending ->
                            try_allocate_job(JobId, Pid, Info, State);
                        _ ->
                            {error, job_not_pending}
                    end;
                _ ->
                    {error, job_info_error}
            end;
        {error, not_found} ->
            {error, job_not_found}
    end.

%% @private
%% Try to allocate resources for a job
try_allocate_job(JobId, JobPid, JobInfo, State) ->
    NumNodes = maps:get(num_nodes, JobInfo, 1),
    NumCpus = maps:get(num_cpus, JobInfo, 1),
    MemoryMb = maps:get(memory_mb, JobInfo, 1024),
    Partition = maps:get(partition, JobInfo, <<"default">>),

    case find_available_nodes(NumNodes, NumCpus, MemoryMb, Partition) of
        {ok, Nodes} ->
            case allocate_resources(JobId, Nodes, NumCpus, MemoryMb) of
                ok ->
                    NodeNames = [N || {N, _, _} <- Nodes],
                    case flurm_job:allocate(JobPid, NodeNames) of
                        ok ->
                            NewRunning = sets:add_element(
                                JobId, State#state.running_jobs),
                            {ok, State#state{running_jobs = NewRunning}};
                        {error, Reason} ->
                            rollback_allocation(JobId, Nodes),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {wait, Reason} ->
            {wait, Reason}
    end.

%% @private
find_available_nodes(NumNodes, NumCpus, MemoryMb, Partition) ->
    Nodes = case Partition of
        <<"default">> ->
            flurm_node_registry:get_available_nodes({NumCpus, MemoryMb, 0});
        _ ->
            get_partition_nodes(Partition, NumCpus, MemoryMb)
    end,

    case length(Nodes) >= NumNodes of
        true ->
            {ok, lists:sublist(Nodes, NumNodes)};
        false ->
            {wait, insufficient_nodes}
    end.

%% @private
get_partition_nodes(Partition, NumCpus, MemoryMb) ->
    PartitionNodes = flurm_node_registry:list_nodes_by_partition(Partition),
    lists:filtermap(
        fun({Name, _Pid}) ->
            case flurm_node_registry:get_node_entry(Name) of
                {ok, #node_entry{state = up,
                                cpus_avail = Cpus,
                                memory_avail = Mem} = _Entry}
                  when Cpus >= NumCpus, Mem >= MemoryMb ->
                    {true, {Name, Cpus, Mem}};
                _ ->
                    false
            end
        end,
        PartitionNodes
    ).

%% @private
allocate_resources(JobId, Nodes, Cpus, Memory) ->
    Results = lists:map(
        fun({Name, _, _}) ->
            flurm_node:allocate(Name, JobId, {Cpus, Memory, 0})
        end,
        Nodes
    ),
    case lists:all(fun(R) -> R =:= ok end, Results) of
        true -> ok;
        false ->
            rollback_allocation(JobId, Nodes),
            {error, allocation_failed}
    end.

%% @private
rollback_allocation(JobId, Nodes) ->
    lists:foreach(
        fun({Name, _, _}) -> catch flurm_node:release(Name, JobId) end,
        Nodes
    ).

%%====================================================================
%% Internal functions - Priority
%%====================================================================

%% @private
calculate_job_priority(JobId) ->
    case get_job_info_map(JobId) of
        undefined ->
            ?DEFAULT_PRIORITY;
        Info ->
            flurm_priority:calculate_priority(Info)
    end.

%% @private
get_job_info_map(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} ->
            case catch flurm_job:get_info(Pid) of
                {ok, Info} -> Info#{job_id => JobId, pid => Pid};
                _ -> undefined
            end;
        _ ->
            undefined
    end.

%% @private
%% Insert job into sorted list by priority
insert_by_priority(JobId, Priority, []) ->
    [{JobId, Priority}];
insert_by_priority(JobId, Priority, [{_, P} = H | T]) when Priority > P ->
    [{JobId, Priority}, H | T];
insert_by_priority(JobId, Priority, [H | T]) ->
    [H | insert_by_priority(JobId, Priority, T)].

%% @private
%% Recalculate priorities and re-sort
recalculate_priorities(PendingJobs) ->
    %% Recalculate priority for each job
    WithPriorities = lists:filtermap(
        fun(JobId) when is_integer(JobId) ->
            P = calculate_job_priority(JobId),
            {true, {JobId, P}};
           ({JobId, _OldP}) ->
            P = calculate_job_priority(JobId),
            {true, {JobId, P}}
        end,
        PendingJobs
    ),

    %% Sort by priority (highest first)
    Sorted = lists:sort(
        fun({_, P1}, {_, P2}) -> P1 > P2 end,
        WithPriorities
    ),

    %% Extract just job IDs (keeping priority tuples for now)
    Sorted.

%% @private
%% Record job usage for fair-share
record_job_usage(JobId) ->
    case get_job_info_map(JobId) of
        undefined ->
            ok;
        Info ->
            User = maps:get(user, Info, <<"unknown">>),
            Account = maps:get(account, Info, <<"default">>),

            %% Calculate CPU-seconds used
            StartTime = maps:get(start_time, Info),
            EndTime = erlang:system_time(second),
            NumCpus = maps:get(num_cpus, Info, 1),

            WallTime = case StartTime of
                {Ms, S, _} -> EndTime - (Ms * 1000000 + S);
                T when is_integer(T) -> EndTime - T;
                _ -> 0
            end,

            CpuSeconds = max(0, WallTime * NumCpus),

            %% Record usage
            catch flurm_fairshare:record_usage(User, Account, CpuSeconds, WallTime)
    end.
