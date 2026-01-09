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
    unregister_node/1,
    lookup_node/1,
    list_nodes/0,
    list_nodes_by_state/1,
    list_nodes_by_partition/1,
    get_available_nodes/1,
    update_state/2,
    update_entry/2,
    get_node_entry/1,
    count_by_state/0
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
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register a node with its pid.
-spec register_node(binary(), pid()) -> ok | {error, already_registered}.
register_node(NodeName, Pid) when is_binary(NodeName), is_pid(Pid) ->
    gen_server:call(?SERVER, {register, NodeName, Pid}).

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

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init([]) ->
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
