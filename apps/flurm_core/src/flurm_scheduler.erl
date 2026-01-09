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

    %% Release resources on nodes
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} ->
            case flurm_job:get_info(Pid) of
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

    State#scheduler_state{running_jobs = NewRunning}.

%% @private
%% Run a scheduling cycle - try to schedule pending jobs
schedule_cycle(State) ->
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
try_schedule_job(JobId, State) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} ->
            case flurm_job:get_info(Pid) of
                {ok, Info} ->
                    %% Check if job is still pending
                    case maps:get(state, Info) of
                        pending ->
                            try_allocate_job(JobId, Pid, Info, State);
                        _OtherState ->
                            %% Job is no longer pending
                            {error, job_not_pending}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, not_found} ->
            {error, job_not_found}
    end.

%% @private
%% Try to allocate resources for a job
try_allocate_job(JobId, JobPid, JobInfo, State) ->
    NumNodes = maps:get(num_nodes, JobInfo, 1),
    NumCpus = maps:get(num_cpus, JobInfo, 1),
    Partition = maps:get(partition, JobInfo, <<"default">>),

    %% Get memory requirement from job_data
    %% Use a default of 1024 MB if not specified
    MemoryMb = maps:get(memory_mb, JobInfo, 1024),

    %% Find available nodes
    case find_nodes_for_job(NumNodes, NumCpus, MemoryMb, Partition) of
        {ok, Nodes} ->
            %% Allocate resources on nodes
            case allocate_job(JobId, Nodes, NumCpus, MemoryMb, 0) of
                ok ->
                    %% Notify job of allocation
                    NodeNames = [N#node_entry.name || N <- Nodes],
                    case flurm_job:allocate(JobPid, NodeNames) of
                        ok ->
                            %% Add to running set
                            NewRunning = sets:add_element(
                                JobId,
                                State#scheduler_state.running_jobs
                            ),
                            {ok, State#scheduler_state{running_jobs = NewRunning}};
                        {error, Reason} ->
                            %% Rollback allocation
                            lists:foreach(
                                fun(Node) ->
                                    catch flurm_node:release(Node#node_entry.name, JobId)
                                end,
                                Nodes
                            ),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {wait, Reason} ->
            {wait, Reason}
    end.

%% @private
%% Find nodes that can run a job
find_nodes_for_job(NumNodes, NumCpus, MemoryMb, Partition) ->
    %% Get nodes in the partition with available resources
    AllNodes = case Partition of
        <<"default">> ->
            %% For default partition, get all up nodes
            flurm_node_registry:get_available_nodes({NumCpus, MemoryMb, 0});
        _ ->
            %% Get nodes in specific partition
            PartitionNodes = flurm_node_registry:list_nodes_by_partition(Partition),
            %% Filter to available nodes
            lists:filtermap(
                fun({Name, _Pid}) ->
                    case flurm_node_registry:get_node_entry(Name) of
                        {ok, #node_entry{state = up,
                                        cpus_avail = CpusAvail,
                                        memory_avail = MemAvail} = Entry} ->
                            if
                                CpusAvail >= NumCpus andalso MemAvail >= MemoryMb ->
                                    {true, Entry};
                                true ->
                                    false
                            end;
                        _ ->
                            false
                    end
                end,
                PartitionNodes
            )
    end,

    %% Check if we have enough nodes
    if
        length(AllNodes) >= NumNodes ->
            %% Take first N nodes (could be improved with better selection)
            {ok, lists:sublist(AllNodes, NumNodes)};
        true ->
            {wait, insufficient_nodes}
    end.

%% @private
%% Allocate resources on nodes for a job
allocate_job(JobId, Nodes, Cpus, Memory, Gpus) ->
    %% Try to allocate on all nodes
    Results = lists:map(
        fun(#node_entry{name = Name}) ->
            flurm_node:allocate(Name, JobId, {Cpus, Memory, Gpus})
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
                fun({#node_entry{name = Name}, Result}) ->
                    case Result of
                        ok -> catch flurm_node:release(Name, JobId);
                        _ -> ok
                    end
                end,
                lists:zip(Nodes, Results)
            ),
            {error, allocation_failed}
    end.
