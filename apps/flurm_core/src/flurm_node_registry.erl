%%%-------------------------------------------------------------------
%%% @doc FLURM Node Registry
%%%
%%% A gen_server that maintains a registry of all compute node processes
%%% in the system. Uses ETS for fast lookups by node name, state, and
%%% partition.
%%%
%%% The registry monitors node processes and automatically removes
%%% them when they terminate.
%%%
%%% ETS Tables:
%%%   flurm_nodes_by_name      - Primary lookup by node name
%%%   flurm_nodes_by_state     - Bag indexed by state
%%%   flurm_nodes_by_partition - Bag indexed by partition
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_registry).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    register_node/2,
    register_node_direct/1,
    unregister_node/1,
    lookup_node/1,
    list_nodes/0,
    list_all_nodes/0,
    list_nodes_by_state/1,
    list_nodes_by_partition/1,
    get_available_nodes/1,
    update_state/2,
    update_entry/2,
    get_node_entry/1,
    count_by_state/0,
    count_nodes/0,
    allocate_resources/3,
    allocate_resources/4,
    release_resources/2,
    release_resources/3,
    get_job_allocation/2
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

%% ETS table names
-define(NODES_BY_NAME, flurm_nodes_by_name).
-define(NODES_BY_STATE, flurm_nodes_by_state).
-define(NODES_BY_PARTITION, flurm_nodes_by_partition).

%% Internal state
-record(state, {
    monitors = #{} :: #{reference() => binary()}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the node registry.
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

%% @doc Register a node with its pid.
-spec register_node(binary(), pid()) -> ok | {error, already_registered}.
register_node(NodeName, Pid) when is_binary(NodeName), is_pid(Pid) ->
    gen_server:call(?SERVER, {register, NodeName, Pid}).

%% @doc Register a node directly with a map of properties.
%% Used by node acceptor when no flurm_node process exists (remote node daemons).
%% NodeInfo must contain: hostname, cpus, memory_mb, state, partitions
-spec register_node_direct(map()) -> ok | {error, already_registered}.
register_node_direct(NodeInfo) when is_map(NodeInfo) ->
    gen_server:call(?SERVER, {register_direct, NodeInfo}).

%% @doc Unregister a node.
-spec unregister_node(binary()) -> ok.
unregister_node(NodeName) when is_binary(NodeName) ->
    gen_server:call(?SERVER, {unregister, NodeName}).

%% @doc Lookup a node pid by name.
%% This is a direct ETS lookup for performance.
-spec lookup_node(binary()) -> {ok, pid()} | {error, not_found}.
lookup_node(NodeName) when is_binary(NodeName) ->
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [#node_entry{pid = Pid}] -> {ok, Pid};
        [] -> {error, not_found}
    end.

%% @doc Get full node entry by name.
-spec get_node_entry(binary()) -> {ok, #node_entry{}} | {error, not_found}.
get_node_entry(NodeName) when is_binary(NodeName) ->
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [Entry] -> {ok, Entry};
        [] -> {error, not_found}
    end.

%% @doc List all registered nodes.
%% Returns a list of {node_name, pid} tuples.
-spec list_nodes() -> [{binary(), pid()}].
list_nodes() ->
    ets:foldl(
        fun(#node_entry{name = Name, pid = Pid}, Acc) ->
            [{Name, Pid} | Acc]
        end,
        [],
        ?NODES_BY_NAME
    ).

%% @doc List nodes by state.
-spec list_nodes_by_state(up | down | drain | maint) -> [{binary(), pid()}].
list_nodes_by_state(State) when is_atom(State) ->
    case ets:lookup(?NODES_BY_STATE, State) of
        [] -> [];
        Results ->
            [{Name, Pid} || {_, Name, Pid} <- Results]
    end.

%% @doc List nodes by partition.
-spec list_nodes_by_partition(binary()) -> [{binary(), pid()}].
list_nodes_by_partition(Partition) when is_binary(Partition) ->
    case ets:lookup(?NODES_BY_PARTITION, Partition) of
        [] -> [];
        Results ->
            [{Name, Pid} || {_, Name, Pid} <- Results]
    end.

%% @doc Get nodes that can run jobs (up state, resources available).
%% ResourceReq is {MinCpus, MinMemory, MinGpus}.
-spec get_available_nodes({non_neg_integer(), non_neg_integer(), non_neg_integer()}) ->
    [#node_entry{}].
get_available_nodes({MinCpus, MinMemory, MinGpus}) ->
    %% First get all up nodes
    UpNodes = list_nodes_by_state(up),
    %% Then filter by available resources
    lists:filtermap(
        fun({Name, _Pid}) ->
            case ets:lookup(?NODES_BY_NAME, Name) of
                [#node_entry{cpus_avail = CpusAvail,
                            memory_avail = MemoryAvail,
                            gpus_avail = GpusAvail} = Entry] ->
                    if
                        CpusAvail >= MinCpus andalso
                        MemoryAvail >= MinMemory andalso
                        GpusAvail >= MinGpus ->
                            {true, Entry};
                        true ->
                            false
                    end;
                [] ->
                    false
            end
        end,
        UpNodes
    ).

%% @doc Update the state of a node in the registry.
-spec update_state(binary(), up | down | drain | maint) -> ok | {error, not_found}.
update_state(NodeName, NewState) when is_binary(NodeName), is_atom(NewState) ->
    gen_server:call(?SERVER, {update_state, NodeName, NewState}).

%% @doc Update a node entry in the registry (for resource changes).
-spec update_entry(binary(), #node_entry{}) -> ok | {error, not_found}.
update_entry(NodeName, #node_entry{} = Entry) when is_binary(NodeName) ->
    gen_server:call(?SERVER, {update_entry, NodeName, Entry}).

%% @doc Get count of nodes by state.
-spec count_by_state() -> #{atom() => non_neg_integer()}.
count_by_state() ->
    States = [up, down, drain, maint],
    maps:from_list([{S, length(list_nodes_by_state(S))} || S <- States]).

%% @doc Get total count of all registered nodes.
-spec count_nodes() -> non_neg_integer().
count_nodes() ->
    ets:info(?NODES_BY_NAME, size).

%% @doc List all registered nodes (alias for list_nodes/0).
%% Returns a list of {node_name, pid} tuples.
-spec list_all_nodes() -> [{binary(), pid()}].
list_all_nodes() ->
    list_nodes().

%% @doc Allocate resources on a node (legacy version without job tracking).
%% Decrements cpus_avail and memory_avail in the node entry.
-spec allocate_resources(binary(), pos_integer(), pos_integer()) -> ok | {error, term()}.
allocate_resources(NodeName, Cpus, Memory) when is_binary(NodeName) ->
    allocate_resources(NodeName, 0, Cpus, Memory).

%% @doc Allocate resources on a node for a specific job.
%% Decrements cpus_avail and memory_avail, and tracks the allocation per job.
-spec allocate_resources(binary(), pos_integer(), pos_integer(), pos_integer()) -> ok | {error, term()}.
allocate_resources(NodeName, JobId, Cpus, Memory) when is_binary(NodeName) ->
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [#node_entry{cpus_avail = CpusAvail, memory_avail = MemAvail,
                     allocations = Allocations} = Entry] ->
            if
                CpusAvail >= Cpus andalso MemAvail >= Memory ->
                    %% Track allocation by job ID
                    NewAllocations = case JobId of
                        0 -> Allocations;  % Legacy call, don't track
                        _ -> maps:put(JobId, {Cpus, Memory}, Allocations)
                    end,
                    NewEntry = Entry#node_entry{
                        cpus_avail = CpusAvail - Cpus,
                        memory_avail = MemAvail - Memory,
                        allocations = NewAllocations
                    },
                    ets:insert(?NODES_BY_NAME, NewEntry),
                    ok;
                true ->
                    {error, insufficient_resources}
            end;
        [] ->
            {error, not_found}
    end.

%% @doc Release resources on a node for a specific job.
%% Uses tracked allocation to determine how much to release.
-spec release_resources(binary(), pos_integer()) -> ok | {error, term()}.
release_resources(NodeName, JobId) when is_binary(NodeName) ->
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [#node_entry{cpus_avail = CpusAvail, memory_avail = MemAvail,
                     cpus_total = CpusTotal, memory_total = MemTotal,
                     allocations = Allocations} = Entry] ->
            %% Look up allocation for this job
            {Cpus, Memory} = maps:get(JobId, Allocations, {1, 256}),
            NewAllocations = maps:remove(JobId, Allocations),
            NewEntry = Entry#node_entry{
                cpus_avail = min(CpusAvail + Cpus, CpusTotal),
                memory_avail = min(MemAvail + Memory, MemTotal),
                allocations = NewAllocations
            },
            ets:insert(?NODES_BY_NAME, NewEntry),
            ok;
        [] ->
            {error, not_found}
    end.

%% @doc Release resources on a node (legacy version with explicit amounts).
%% Increments cpus_avail and memory_avail back in the node entry.
-spec release_resources(binary(), pos_integer(), pos_integer()) -> ok | {error, term()}.
release_resources(NodeName, Cpus, Memory) when is_binary(NodeName) ->
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [#node_entry{cpus_avail = CpusAvail, memory_avail = MemAvail,
                     cpus_total = CpusTotal, memory_total = MemTotal} = Entry] ->
            NewEntry = Entry#node_entry{
                cpus_avail = min(CpusAvail + Cpus, CpusTotal),
                memory_avail = min(MemAvail + Memory, MemTotal)
            },
            ets:insert(?NODES_BY_NAME, NewEntry),
            ok;
        [] ->
            {error, not_found}
    end.

%% @doc Get the resource allocation for a specific job on a node.
-spec get_job_allocation(binary(), pos_integer()) -> {ok, {pos_integer(), pos_integer()}} | {error, term()}.
get_job_allocation(NodeName, JobId) when is_binary(NodeName) ->
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [#node_entry{allocations = Allocations}] ->
            case maps:find(JobId, Allocations) of
                {ok, Allocation} -> {ok, Allocation};
                error -> {error, not_found}
            end;
        [] ->
            {error, node_not_found}
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init([]) ->
    lager:info("[node_registry] Starting Node Registry..."),
    %% Create ETS tables
    %% Primary table: set indexed by node name
    ets:new(?NODES_BY_NAME, [
        named_table,
        set,
        public,
        {keypos, #node_entry.name},
        {read_concurrency, true}
    ]),
    %% Secondary index: bag by state
    ets:new(?NODES_BY_STATE, [
        named_table,
        bag,
        public,
        {read_concurrency, true}
    ]),
    %% Secondary index: bag by partition
    ets:new(?NODES_BY_PARTITION, [
        named_table,
        bag,
        public,
        {read_concurrency, true}
    ]),
    lager:info("[node_registry] Node Registry started, ETS tables created"),
    {ok, #state{}}.

%% @private
handle_call({register, NodeName, Pid}, _From, State) ->
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [_] ->
            {reply, {error, already_registered}, State};
        [] ->
            %% Get node info from the process
            {ok, Info} = flurm_node:get_info(Pid),

            %% Create entry with initial values
            Entry = #node_entry{
                name = NodeName,
                pid = Pid,
                hostname = maps:get(hostname, Info),
                state = maps:get(state, Info),
                partitions = maps:get(partitions, Info),
                cpus_total = maps:get(cpus, Info),
                cpus_avail = maps:get(cpus_available, Info),
                memory_total = maps:get(memory, Info),
                memory_avail = maps:get(memory_available, Info),
                gpus_total = maps:get(gpus, Info),
                gpus_avail = maps:get(gpus_available, Info)
            },

            %% Insert into primary table
            ets:insert(?NODES_BY_NAME, Entry),

            %% Insert into state index
            NodeState = maps:get(state, Info),
            ets:insert(?NODES_BY_STATE, {NodeState, NodeName, Pid}),

            %% Insert into partition indexes
            Partitions = maps:get(partitions, Info),
            lists:foreach(
                fun(Partition) ->
                    ets:insert(?NODES_BY_PARTITION, {Partition, NodeName, Pid})
                end,
                Partitions
            ),

            %% Monitor the process
            MonRef = erlang:monitor(process, Pid),
            NewMonitors = maps:put(MonRef, NodeName, State#state.monitors),

            {reply, ok, State#state{monitors = NewMonitors}}
    end;

%% Direct registration without a local pid (for remote node daemons)
handle_call({register_direct, NodeInfo}, _From, State) ->
    NodeName = maps:get(hostname, NodeInfo),
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [ExistingEntry] ->
            %% Node already registered - update state to 'up' and reset resources
            %% This handles node reconnection after disconnect/restart
            Cpus = maps:get(cpus, NodeInfo, ExistingEntry#node_entry.cpus_total),
            MemoryMb = maps:get(memory_mb, NodeInfo, ExistingEntry#node_entry.memory_total),
            NewState = maps:get(state, NodeInfo, up),
            OldState = ExistingEntry#node_entry.state,
            OldPid = ExistingEntry#node_entry.pid,
            NewPid = self(),

            %% Update entry with fresh state, pid, and reset available resources
            %% Clear any stale job allocations on re-registration
            UpdatedEntry = ExistingEntry#node_entry{
                pid = NewPid,
                state = NewState,
                cpus_total = Cpus,
                cpus_avail = Cpus,
                memory_total = MemoryMb,
                memory_avail = MemoryMb,
                allocations = #{}  % Clear allocations on re-register
            },
            ets:insert(?NODES_BY_NAME, UpdatedEntry),

            %% Clean up state index - remove ALL entries for this node first
            %% This handles pid changes after gen_server restart
            ets:match_delete(?NODES_BY_STATE, {OldState, NodeName, '_'}),
            %% Also clean up any other state entries (in case state changed)
            AllStates = [up, down, drain, maint],
            lists:foreach(fun(S) ->
                ets:match_delete(?NODES_BY_STATE, {S, NodeName, '_'})
            end, AllStates -- [OldState]),
            %% Insert fresh entry with new state and pid
            ets:insert(?NODES_BY_STATE, {NewState, NodeName, NewPid}),

            lager:info("Node ~s re-registered (state: ~p -> ~p, cpus=~p, mem=~p, pid: ~p -> ~p)",
                      [NodeName, OldState, NewState, Cpus, MemoryMb, OldPid, NewPid]),
            {reply, ok, State};
        [] ->
            Cpus = maps:get(cpus, NodeInfo, 1),
            MemoryMb = maps:get(memory_mb, NodeInfo, 1024),
            NodeState = maps:get(state, NodeInfo, up),
            Partitions = maps:get(partitions, NodeInfo, [<<"default">>]),

            %% Create entry with available = total (node starts idle)
            Entry = #node_entry{
                name = NodeName,
                pid = self(),  %% Use calling process as placeholder
                hostname = NodeName,
                state = NodeState,
                partitions = Partitions,
                cpus_total = Cpus,
                cpus_avail = Cpus,
                memory_total = MemoryMb,
                memory_avail = MemoryMb,
                gpus_total = 0,
                gpus_avail = 0
            },

            %% Insert into primary table
            ets:insert(?NODES_BY_NAME, Entry),

            %% Insert into state index
            ets:insert(?NODES_BY_STATE, {NodeState, NodeName, self()}),

            %% Insert into partition indexes
            lists:foreach(
                fun(Partition) ->
                    ets:insert(?NODES_BY_PARTITION, {Partition, NodeName, self()})
                end,
                Partitions
            ),

            lager:info("Node ~s registered directly (cpus=~p, mem=~p, state=~p)",
                      [NodeName, Cpus, MemoryMb, NodeState]),
            {reply, ok, State}
    end;

handle_call({unregister, NodeName}, _From, State) ->
    NewState = do_unregister(NodeName, State),
    {reply, ok, NewState};

handle_call({update_state, NodeName, NewState}, _From, State) ->
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [#node_entry{state = OldState, pid = Pid} = Entry] ->
            %% Update main table
            NewEntry = Entry#node_entry{state = NewState},
            ets:insert(?NODES_BY_NAME, NewEntry),

            %% Update state index - remove old, add new
            ets:delete_object(?NODES_BY_STATE, {OldState, NodeName, Pid}),
            ets:insert(?NODES_BY_STATE, {NewState, NodeName, Pid}),

            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({update_entry, NodeName, NewEntry}, _From, State) ->
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [#node_entry{state = OldState, pid = Pid}] ->
            %% Update main table
            ets:insert(?NODES_BY_NAME, NewEntry),

            %% Update state index if state changed
            NewState = NewEntry#node_entry.state,
            if
                NewState =/= OldState ->
                    ets:delete_object(?NODES_BY_STATE, {OldState, NodeName, Pid}),
                    ets:insert(?NODES_BY_STATE, {NewState, NodeName, Pid});
                true ->
                    ok
            end,

            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({'DOWN', MonRef, process, _Pid, _Reason}, State) ->
    case maps:get(MonRef, State#state.monitors, undefined) of
        undefined ->
            {noreply, State};
        NodeName ->
            NewState = do_unregister(NodeName, State),
            {noreply, NewState#state{
                monitors = maps:remove(MonRef, NewState#state.monitors)
            }}
    end;

%% Handle config reload notification from flurm_config_server
handle_info({config_reload_nodes, NodeDefs}, State) ->
    lager:info("Node registry received config reload with ~p node definitions", [length(NodeDefs)]),
    %% Process each node definition to update features, resources, etc.
    %% This doesn't register new nodes, but updates existing registered nodes
    %% with any changed configuration (features, partitions, etc.)
    lists:foreach(fun(NodeDef) ->
        update_node_from_config(NodeDef)
    end, NodeDefs),
    {noreply, State};

%% Handle config changes from flurm_config_server (via subscribe_changes)
handle_info({config_changed, nodes, _OldNodes, NewNodes}, State) ->
    lager:info("Node registry received node config change: ~p definitions", [length(NewNodes)]),
    lists:foreach(fun(NodeDef) ->
        update_node_from_config(NodeDef)
    end, NewNodes),
    {noreply, State};

handle_info({config_changed, _Key, _OldValue, _NewValue}, State) ->
    %% Ignore other config changes
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Remove a node from all ETS tables
do_unregister(NodeName, State) ->
    case ets:lookup(?NODES_BY_NAME, NodeName) of
        [#node_entry{state = NodeState, pid = Pid, partitions = Partitions}] ->
            %% Remove from primary table
            ets:delete(?NODES_BY_NAME, NodeName),

            %% Remove from state index
            ets:delete_object(?NODES_BY_STATE, {NodeState, NodeName, Pid}),

            %% Remove from partition indexes
            lists:foreach(
                fun(Partition) ->
                    ets:delete_object(?NODES_BY_PARTITION, {Partition, NodeName, Pid})
                end,
                Partitions
            ),

            %% Find and remove monitor
            MonRef = find_monitor_ref(NodeName, State#state.monitors),
            case MonRef of
                undefined ->
                    State;
                Ref ->
                    erlang:demonitor(Ref, [flush]),
                    State#state{monitors = maps:remove(Ref, State#state.monitors)}
            end;
        [] ->
            State
    end.

%% @private
%% Find the monitor reference for a node name
find_monitor_ref(NodeName, Monitors) ->
    case maps:to_list(maps:filter(fun(_, V) -> V =:= NodeName end, Monitors)) of
        [{Ref, _}] -> Ref;
        [] -> undefined
    end.

%% @private
%% Update a registered node's configuration from a config file node definition
update_node_from_config(NodeDef) ->
    %% Get the node name pattern and expand it
    case maps:get(nodename, NodeDef, undefined) of
        undefined -> ok;
        NodePattern ->
            %% Expand hostlist pattern (e.g., node[001-100])
            ExpandedNodes = case catch flurm_config_slurm:expand_hostlist(NodePattern) of
                NodeList when is_list(NodeList) -> NodeList;
                _ -> [NodePattern]  % Fallback if expansion fails
            end,
            %% Update each matching registered node
            lists:foreach(fun(NodeName) ->
                case ets:lookup(?NODES_BY_NAME, NodeName) of
                    [#node_entry{} = Entry] ->
                        %% Update features if specified in config
                        NewEntry = update_entry_from_config(Entry, NodeDef),
                        ets:insert(?NODES_BY_NAME, NewEntry),
                        lager:debug("Updated node ~s from config", [NodeName]);
                    [] ->
                        %% Node not registered yet, skip
                        ok
                end
            end, ExpandedNodes)
    end.

%% @private
%% Apply config definition updates to a node entry
update_entry_from_config(Entry, NodeDef) ->
    %% Update partitions if specified
    Entry1 = case maps:get(partitions, NodeDef, undefined) of
        undefined -> Entry;
        Partitions when is_list(Partitions) ->
            Entry#node_entry{partitions = Partitions};
        _ -> Entry
    end,
    %% Update CPU count if specified
    Entry2 = case maps:get(cpus, NodeDef, undefined) of
        undefined -> Entry1;
        Cpus when is_integer(Cpus) ->
            Entry1#node_entry{cpus_total = Cpus};
        _ -> Entry1
    end,
    %% Update memory if specified
    Entry3 = case maps:get(realmemory, NodeDef, undefined) of
        undefined -> Entry2;
        Memory when is_integer(Memory) ->
            Entry2#node_entry{memory_total = Memory};
        _ -> Entry2
    end,
    %% Update state if specified
    case maps:get(state, NodeDef, undefined) of
        undefined -> Entry3;
        down -> Entry3#node_entry{state = down};
        drain -> Entry3#node_entry{state = drain};
        _ -> Entry3
    end.
