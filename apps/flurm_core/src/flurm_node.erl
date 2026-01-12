%%%-------------------------------------------------------------------
%%% @doc FLURM Node Process
%%%
%%% A gen_server representing a compute node in the FLURM cluster.
%%% Tracks node resources (CPUs, memory, GPUs), running jobs, and
%%% handles resource allocation/release.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/1,
    register_node/1,
    heartbeat/1,
    allocate/3,
    release/2,
    set_state/2,
    get_info/1,
    list_jobs/1,
    %% Drain mode API
    drain_node/1,
    drain_node/2,
    undrain_node/1,
    get_drain_reason/1,
    is_draining/1
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

%%====================================================================
%% API
%%====================================================================

%% @doc Start a node process with the given node specification.
-spec start_link(#node_spec{}) -> {ok, pid()} | {error, term()}.
start_link(#node_spec{} = NodeSpec) ->
    gen_server:start_link(?MODULE, [NodeSpec], []).

%% @doc Create and register a node from a registration message.
%% This is typically called when a node daemon sends a registration request.
-spec register_node(#node_spec{}) -> {ok, pid(), binary()} | {error, term()}.
register_node(#node_spec{} = NodeSpec) ->
    case flurm_node_sup:start_node(NodeSpec) of
        {ok, Pid} ->
            NodeName = NodeSpec#node_spec.name,
            ok = flurm_node_registry:register_node(NodeName, Pid),
            {ok, Pid, NodeName};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Update heartbeat timestamp for a node.
-spec heartbeat(pid() | binary()) -> ok | {error, term()}.
heartbeat(Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, heartbeat);
heartbeat(NodeName) when is_binary(NodeName) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} -> heartbeat(Pid);
        {error, not_found} -> {error, node_not_found}
    end.

%% @doc Allocate resources on a node for a job.
%% Returns ok if allocation succeeds, {error, insufficient_resources} otherwise.
-spec allocate(pid() | binary(), pos_integer(), {pos_integer(), pos_integer(), non_neg_integer()}) ->
    ok | {error, term()}.
allocate(Pid, JobId, {Cpus, Memory, Gpus}) when is_pid(Pid) ->
    gen_server:call(Pid, {allocate, JobId, Cpus, Memory, Gpus});
allocate(NodeName, JobId, Resources) when is_binary(NodeName) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} -> allocate(Pid, JobId, Resources);
        {error, not_found} -> {error, node_not_found}
    end.

%% @doc Release resources from a job on a node.
-spec release(pid() | binary(), pos_integer()) -> ok | {error, term()}.
release(Pid, JobId) when is_pid(Pid) ->
    gen_server:call(Pid, {release, JobId});
release(NodeName, JobId) when is_binary(NodeName) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} -> release(Pid, JobId);
        {error, not_found} -> {error, node_not_found}
    end.

%% @doc Set the node state (up, down, drain, maint).
-spec set_state(pid() | binary(), up | down | drain | maint) -> ok | {error, term()}.
set_state(Pid, State) when is_pid(Pid), is_atom(State) ->
    gen_server:call(Pid, {set_state, State});
set_state(NodeName, State) when is_binary(NodeName) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} -> set_state(Pid, State);
        {error, not_found} -> {error, node_not_found}
    end.

%% @doc Get node information as a map.
-spec get_info(pid() | binary()) -> {ok, map()} | {error, term()}.
get_info(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, get_info);
get_info(NodeName) when is_binary(NodeName) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} -> get_info(Pid);
        {error, not_found} -> {error, node_not_found}
    end.

%% @doc List jobs running on a node.
-spec list_jobs(pid() | binary()) -> {ok, [pos_integer()]} | {error, term()}.
list_jobs(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, list_jobs);
list_jobs(NodeName) when is_binary(NodeName) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} -> list_jobs(Pid);
        {error, not_found} -> {error, node_not_found}
    end.

%% @doc Drain a node, preventing new job assignments.
%% Existing jobs will continue to run until completion.
%% Uses default reason "admin request".
-spec drain_node(pid() | binary()) -> ok | {error, term()}.
drain_node(NodeOrName) ->
    drain_node(NodeOrName, <<"admin request">>).

%% @doc Drain a node with a specific reason.
%% Existing jobs will continue to run until completion.
-spec drain_node(pid() | binary(), binary()) -> ok | {error, term()}.
drain_node(Pid, Reason) when is_pid(Pid) ->
    gen_server:call(Pid, {drain, Reason});
drain_node(NodeName, Reason) when is_binary(NodeName) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} -> drain_node(Pid, Reason);
        {error, not_found} -> {error, node_not_found}
    end.

%% @doc Remove drain state from a node, allowing new job assignments.
-spec undrain_node(pid() | binary()) -> ok | {error, term()}.
undrain_node(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, undrain);
undrain_node(NodeName) when is_binary(NodeName) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} -> undrain_node(Pid);
        {error, not_found} -> {error, node_not_found}
    end.

%% @doc Get the drain reason for a node.
%% Returns {ok, Reason} if the node is draining, {error, not_draining} otherwise.
-spec get_drain_reason(pid() | binary()) -> {ok, binary()} | {error, not_draining | node_not_found}.
get_drain_reason(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, get_drain_reason);
get_drain_reason(NodeName) when is_binary(NodeName) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} -> get_drain_reason(Pid);
        {error, not_found} -> {error, node_not_found}
    end.

%% @doc Check if a node is in drain state.
-spec is_draining(pid() | binary()) -> boolean() | {error, node_not_found}.
is_draining(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, is_draining);
is_draining(NodeName) when is_binary(NodeName) ->
    case flurm_node_registry:lookup_node(NodeName) of
        {ok, Pid} -> is_draining(Pid);
        {error, not_found} -> {error, node_not_found}
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init([#node_spec{} = Spec]) ->
    State = #node_state{
        name = Spec#node_spec.name,
        hostname = Spec#node_spec.hostname,
        port = Spec#node_spec.port,
        cpus = Spec#node_spec.cpus,
        cpus_used = 0,
        memory = Spec#node_spec.memory,
        memory_used = 0,
        gpus = Spec#node_spec.gpus,
        gpus_used = 0,
        state = up,
        drain_reason = undefined,
        features = Spec#node_spec.features,
        partitions = Spec#node_spec.partitions,
        jobs = [],
        last_heartbeat = erlang:timestamp(),
        version = ?NODE_STATE_VERSION
    },
    {ok, State}.

%% @private
handle_call({allocate, JobId, Cpus, Memory, Gpus}, _From, State) ->
    case can_allocate(State, Cpus, Memory, Gpus) of
        true ->
            NewState = State#node_state{
                cpus_used = State#node_state.cpus_used + Cpus,
                memory_used = State#node_state.memory_used + Memory,
                gpus_used = State#node_state.gpus_used + Gpus,
                jobs = [JobId | State#node_state.jobs]
            },
            %% Notify registry of resource change
            notify_resource_change(NewState),
            {reply, ok, NewState};
        false ->
            {reply, {error, insufficient_resources}, State}
    end;

handle_call({release, JobId}, _From, State) ->
    case lists:member(JobId, State#node_state.jobs) of
        true ->
            %% For simplicity, we track resources per-job in a map
            %% In production, you'd want to track exact allocations
            %% For now, we assume uniform allocation (divide by job count)
            NumJobs = length(State#node_state.jobs),
            CpusPerJob = State#node_state.cpus_used div max(1, NumJobs),
            MemoryPerJob = State#node_state.memory_used div max(1, NumJobs),
            GpusPerJob = State#node_state.gpus_used div max(1, NumJobs),
            NewState = State#node_state{
                cpus_used = max(0, State#node_state.cpus_used - CpusPerJob),
                memory_used = max(0, State#node_state.memory_used - MemoryPerJob),
                gpus_used = max(0, State#node_state.gpus_used - GpusPerJob),
                jobs = lists:delete(JobId, State#node_state.jobs)
            },
            notify_resource_change(NewState),
            {reply, ok, NewState};
        false ->
            {reply, {error, job_not_found}, State}
    end;

handle_call({set_state, NewNodeState}, _From, State) ->
    UpdatedState = State#node_state{state = NewNodeState},
    notify_state_change(UpdatedState),
    {reply, ok, UpdatedState};

handle_call(get_info, _From, State) ->
    Info = build_node_info(State),
    {reply, {ok, Info}, State};

handle_call(list_jobs, _From, State) ->
    {reply, {ok, State#node_state.jobs}, State};

%% Drain mode operations
handle_call({drain, Reason}, _From, State) ->
    UpdatedState = State#node_state{
        state = drain,
        drain_reason = Reason
    },
    notify_state_change(UpdatedState),
    %% Notify scheduler about drain
    catch flurm_scheduler:trigger_schedule(),
    {reply, ok, UpdatedState};

handle_call(undrain, _From, State) ->
    %% Only undrain if currently in drain state
    case State#node_state.state of
        drain ->
            UpdatedState = State#node_state{
                state = up,
                drain_reason = undefined
            },
            notify_state_change(UpdatedState),
            %% Notify scheduler that node is available again
            catch flurm_scheduler:trigger_schedule(),
            {reply, ok, UpdatedState};
        _ ->
            {reply, {error, not_draining}, State}
    end;

handle_call(get_drain_reason, _From, State) ->
    case State#node_state.state of
        drain ->
            {reply, {ok, State#node_state.drain_reason}, State};
        _ ->
            {reply, {error, not_draining}, State}
    end;

handle_call(is_draining, _From, State) ->
    {reply, State#node_state.state =:= drain, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(heartbeat, State) ->
    NewState = State#node_state{last_heartbeat = erlang:timestamp()},
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, #node_state{name = Name}) ->
    flurm_node_registry:unregister_node(Name),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    NewState = maybe_upgrade_state(State),
    {ok, NewState}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Check if node has sufficient resources for allocation
can_allocate(#node_state{state = NodeState}, _Cpus, _Memory, _Gpus)
  when NodeState =/= up ->
    false;
can_allocate(State, Cpus, Memory, Gpus) ->
    AvailCpus = State#node_state.cpus - State#node_state.cpus_used,
    AvailMemory = State#node_state.memory - State#node_state.memory_used,
    AvailGpus = State#node_state.gpus - State#node_state.gpus_used,
    (Cpus =< AvailCpus) andalso
    (Memory =< AvailMemory) andalso
    (Gpus =< AvailGpus).

%% @private
%% Build a map of node information
build_node_info(#node_state{} = State) ->
    #{
        name => State#node_state.name,
        hostname => State#node_state.hostname,
        port => State#node_state.port,
        cpus => State#node_state.cpus,
        cpus_used => State#node_state.cpus_used,
        cpus_available => State#node_state.cpus - State#node_state.cpus_used,
        memory => State#node_state.memory,
        memory_used => State#node_state.memory_used,
        memory_available => State#node_state.memory - State#node_state.memory_used,
        gpus => State#node_state.gpus,
        gpus_used => State#node_state.gpus_used,
        gpus_available => State#node_state.gpus - State#node_state.gpus_used,
        state => State#node_state.state,
        drain_reason => State#node_state.drain_reason,
        features => State#node_state.features,
        partitions => State#node_state.partitions,
        jobs => State#node_state.jobs,
        job_count => length(State#node_state.jobs),
        last_heartbeat => State#node_state.last_heartbeat
    }.

%% @private
%% Notify registry of resource change
notify_resource_change(#node_state{name = Name} = State) ->
    Entry = #node_entry{
        name = Name,
        pid = self(),
        hostname = State#node_state.hostname,
        state = State#node_state.state,
        partitions = State#node_state.partitions,
        cpus_total = State#node_state.cpus,
        cpus_avail = State#node_state.cpus - State#node_state.cpus_used,
        memory_total = State#node_state.memory,
        memory_avail = State#node_state.memory - State#node_state.memory_used,
        gpus_total = State#node_state.gpus,
        gpus_avail = State#node_state.gpus - State#node_state.gpus_used
    },
    catch flurm_node_registry:update_entry(Name, Entry),
    ok.

%% @private
%% Notify registry of state change
notify_state_change(#node_state{name = Name, state = NodeState}) ->
    catch flurm_node_registry:update_state(Name, NodeState),
    ok.

%% @private
%% Handle state upgrades during hot code changes
maybe_upgrade_state(#node_state{version = ?NODE_STATE_VERSION} = State) ->
    State;
maybe_upgrade_state(#node_state{version = OldVersion} = State)
  when OldVersion < ?NODE_STATE_VERSION ->
    State#node_state{version = ?NODE_STATE_VERSION};
maybe_upgrade_state(State) when is_record(State, node_state) ->
    State#node_state{version = ?NODE_STATE_VERSION}.
