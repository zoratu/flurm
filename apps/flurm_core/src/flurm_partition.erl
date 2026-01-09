%%%-------------------------------------------------------------------
%%% @doc FLURM Partition Process
%%%
%%% A gen_server representing a partition in the FLURM cluster.
%%% Partitions are logical groupings of nodes with shared policies
%%% such as maximum job time, priority, and node limits.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/1,
    get_nodes/1,
    add_node/2,
    remove_node/2,
    get_info/1,
    set_state/2,
    get_state/1
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

%% @doc Start a partition process with the given specification.
-spec start_link(#partition_spec{}) -> {ok, pid()} | {error, term()}.
start_link(#partition_spec{} = PartitionSpec) ->
    Name = PartitionSpec#partition_spec.name,
    gen_server:start_link({local, partition_name(Name)}, ?MODULE, [PartitionSpec], []).

%% @doc Get the list of nodes in a partition.
-spec get_nodes(pid() | binary()) -> {ok, [binary()]} | {error, term()}.
get_nodes(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, get_nodes);
get_nodes(Name) when is_binary(Name) ->
    gen_server:call(partition_name(Name), get_nodes).

%% @doc Add a node to a partition.
-spec add_node(pid() | binary(), binary()) -> ok | {error, term()}.
add_node(Pid, NodeName) when is_pid(Pid), is_binary(NodeName) ->
    gen_server:call(Pid, {add_node, NodeName});
add_node(PartitionName, NodeName) when is_binary(PartitionName), is_binary(NodeName) ->
    gen_server:call(partition_name(PartitionName), {add_node, NodeName}).

%% @doc Remove a node from a partition.
-spec remove_node(pid() | binary(), binary()) -> ok | {error, term()}.
remove_node(Pid, NodeName) when is_pid(Pid), is_binary(NodeName) ->
    gen_server:call(Pid, {remove_node, NodeName});
remove_node(PartitionName, NodeName) when is_binary(PartitionName), is_binary(NodeName) ->
    gen_server:call(partition_name(PartitionName), {remove_node, NodeName}).

%% @doc Get partition information as a map.
-spec get_info(pid() | binary()) -> {ok, map()} | {error, term()}.
get_info(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, get_info);
get_info(Name) when is_binary(Name) ->
    gen_server:call(partition_name(Name), get_info).

%% @doc Set the partition state (up, down, drain).
-spec set_state(pid() | binary(), up | down | drain) -> ok | {error, term()}.
set_state(Pid, State) when is_pid(Pid), is_atom(State) ->
    gen_server:call(Pid, {set_state, State});
set_state(Name, State) when is_binary(Name), is_atom(State) ->
    gen_server:call(partition_name(Name), {set_state, State}).

%% @doc Get the current partition state.
-spec get_state(pid() | binary()) -> {ok, up | down | drain} | {error, term()}.
get_state(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, get_state);
get_state(Name) when is_binary(Name) ->
    gen_server:call(partition_name(Name), get_state).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init([#partition_spec{} = Spec]) ->
    State = #partition_state{
        name = Spec#partition_spec.name,
        nodes = Spec#partition_spec.nodes,
        max_time = Spec#partition_spec.max_time,
        default_time = Spec#partition_spec.default_time,
        max_nodes = Spec#partition_spec.max_nodes,
        priority = Spec#partition_spec.priority,
        state = up
    },
    {ok, State}.

%% @private
handle_call(get_nodes, _From, State) ->
    {reply, {ok, State#partition_state.nodes}, State};

handle_call({add_node, NodeName}, _From, State) ->
    Nodes = State#partition_state.nodes,
    case lists:member(NodeName, Nodes) of
        true ->
            {reply, {error, already_member}, State};
        false ->
            MaxNodes = State#partition_state.max_nodes,
            case length(Nodes) >= MaxNodes of
                true ->
                    {reply, {error, max_nodes_reached}, State};
                false ->
                    NewState = State#partition_state{nodes = [NodeName | Nodes]},
                    {reply, ok, NewState}
            end
    end;

handle_call({remove_node, NodeName}, _From, State) ->
    Nodes = State#partition_state.nodes,
    case lists:member(NodeName, Nodes) of
        true ->
            NewNodes = lists:delete(NodeName, Nodes),
            NewState = State#partition_state{nodes = NewNodes},
            {reply, ok, NewState};
        false ->
            {reply, {error, not_member}, State}
    end;

handle_call(get_info, _From, State) ->
    Info = build_partition_info(State),
    {reply, {ok, Info}, State};

handle_call({set_state, NewPartState}, _From, State) ->
    case validate_partition_state(NewPartState) of
        true ->
            NewState = State#partition_state{state = NewPartState},
            {reply, ok, NewState};
        false ->
            {reply, {error, invalid_state}, State}
    end;

handle_call(get_state, _From, State) ->
    {reply, {ok, State#partition_state.state}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
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
%% Convert partition name to registered process name
partition_name(Name) when is_binary(Name) ->
    %% Create an atom from the partition name
    %% Note: In production, consider using a registry instead of atoms
    list_to_atom("flurm_partition_" ++ binary_to_list(Name)).

%% @private
%% Build a map of partition information
build_partition_info(#partition_state{} = State) ->
    #{
        name => State#partition_state.name,
        nodes => State#partition_state.nodes,
        node_count => length(State#partition_state.nodes),
        max_time => State#partition_state.max_time,
        default_time => State#partition_state.default_time,
        max_nodes => State#partition_state.max_nodes,
        priority => State#partition_state.priority,
        state => State#partition_state.state
    }.

%% @private
%% Validate partition state
validate_partition_state(up) -> true;
validate_partition_state(down) -> true;
validate_partition_state(drain) -> true;
validate_partition_state(_) -> false.
