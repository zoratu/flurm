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
    failed_count    :: non_neg_integer()
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

    %% Start the periodic scheduling timer
    Timer = schedule_next_cycle(),

    State = #scheduler_state{
        pending_jobs = queue:new(),
        running_jobs = sets:new(),
        nodes_cache = NodesCache,
        schedule_timer = Timer,
        schedule_cycles = 0,
        completed_count = 0,
        failed_count = 0
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

    %% Schedule next cycle
    Timer = schedule_next_cycle(),
    FinalState = NewState#scheduler_state{
        schedule_timer = Timer,
        schedule_cycles = NewState#scheduler_state.schedule_cycles + 1
    },
    {noreply, FinalState};

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
            );
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
%% Try to schedule pending jobs
schedule_pending_jobs(State) ->
    case queue:out(State#scheduler_state.pending_jobs) of
        {empty, _} ->
            %% No pending jobs
            State;
        {{value, JobId}, RestQueue} ->
            %% Try to schedule this job
            case try_schedule_job(JobId, State) of
                {ok, NewState} ->
                    %% Job scheduled, continue with rest of queue
                    schedule_pending_jobs(NewState#scheduler_state{
                        pending_jobs = RestQueue
                    });
                {wait, _Reason} ->
                    %% Job cannot be scheduled (insufficient resources)
                    %% Keep it at the front of the queue (FIFO)
                    %% Stop trying to schedule more jobs
                    State;
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
%% Try to schedule a single job
%% Uses flurm_job_manager to get job info (not flurm_job_registry)
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
                    try_allocate_job_simple(JobId, Info, State);
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
        allocated_nodes => Job#job.allocated_nodes
    }.

%% @private
%% Simple allocation without flurm_job process
%% Used when jobs are stored in flurm_job_manager instead of flurm_job_registry
try_allocate_job_simple(JobId, JobInfo, State) ->
    NumNodes = maps:get(num_nodes, JobInfo, 1),
    NumCpus = maps:get(num_cpus, JobInfo, 1),
    Partition = maps:get(partition, JobInfo, <<"default">>),
    MemoryMb = maps:get(memory_mb, JobInfo, 1024),
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
