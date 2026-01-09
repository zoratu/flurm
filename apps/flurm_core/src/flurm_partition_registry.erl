%%%-------------------------------------------------------------------
%%% @doc FLURM Partition Registry
%%%
%%% Maintains a registry of partitions and their configurations.
%%% Partitions are used to group nodes and control job scheduling
%%% (similar to SLURM's partition concept).
%%%
%%% Each partition has:
%%% - Name: unique identifier
%%% - Priority: affects job scheduling order
%%% - Nodes: list of node names in partition
%%% - State: up, down, drain
%%% - MaxTime: maximum job time limit
%%% - DefaultTime: default job time limit
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_registry).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    register_partition/2,
    unregister_partition/1,
    get_partition/1,
    get_partition_priority/1,
    set_partition_priority/2,
    list_partitions/0,
    get_default_partition/0,
    set_default_partition/1,
    get_partition_nodes/1,
    add_node_to_partition/2,
    remove_node_from_partition/2,
    update_partition/2
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
-define(PARTITIONS_TABLE, flurm_partitions).
-define(DEFAULT_PARTITION_KEY, '$default_partition').

%%====================================================================
%% API
%%====================================================================

%% @doc Start the partition registry server.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register a new partition.
-spec register_partition(binary(), map()) -> ok | {error, already_exists}.
register_partition(Name, Config) when is_binary(Name), is_map(Config) ->
    gen_server:call(?SERVER, {register, Name, Config}).

%% @doc Unregister a partition.
-spec unregister_partition(binary()) -> ok | {error, not_found}.
unregister_partition(Name) when is_binary(Name) ->
    gen_server:call(?SERVER, {unregister, Name}).

%% @doc Get partition configuration.
-spec get_partition(binary()) -> {ok, map()} | {error, not_found}.
get_partition(Name) when is_binary(Name) ->
    case ets:lookup(?PARTITIONS_TABLE, Name) of
        [{Name, #partition{} = P}] -> {ok, partition_to_map(P)};
        [] -> {error, not_found}
    end.

%% @doc Get partition priority.
-spec get_partition_priority(binary()) -> {ok, non_neg_integer()} | {error, not_found}.
get_partition_priority(Name) when is_binary(Name) ->
    case ets:lookup(?PARTITIONS_TABLE, Name) of
        [{Name, #partition{priority = Priority}}] -> {ok, Priority};
        [] -> {error, not_found}
    end.

%% @doc Set partition priority.
-spec set_partition_priority(binary(), non_neg_integer()) -> ok | {error, not_found}.
set_partition_priority(Name, Priority) when is_binary(Name), is_integer(Priority), Priority >= 0 ->
    gen_server:call(?SERVER, {set_priority, Name, Priority}).

%% @doc List all partitions.
-spec list_partitions() -> [binary()].
list_partitions() ->
    ets:foldl(
        fun({?DEFAULT_PARTITION_KEY, _}, Acc) -> Acc;
           ({Name, _}, Acc) -> [Name | Acc]
        end,
        [],
        ?PARTITIONS_TABLE
    ).

%% @doc Get the default partition name.
-spec get_default_partition() -> {ok, binary()} | {error, not_set}.
get_default_partition() ->
    case ets:lookup(?PARTITIONS_TABLE, ?DEFAULT_PARTITION_KEY) of
        [{?DEFAULT_PARTITION_KEY, Name}] -> {ok, Name};
        [] -> {error, not_set}
    end.

%% @doc Set the default partition.
-spec set_default_partition(binary()) -> ok | {error, not_found}.
set_default_partition(Name) when is_binary(Name) ->
    gen_server:call(?SERVER, {set_default, Name}).

%% @doc Get nodes in a partition.
-spec get_partition_nodes(binary()) -> {ok, [binary()]} | {error, not_found}.
get_partition_nodes(Name) when is_binary(Name) ->
    case ets:lookup(?PARTITIONS_TABLE, Name) of
        [{Name, #partition{nodes = Nodes}}] -> {ok, Nodes};
        [] -> {error, not_found}
    end.

%% @doc Add a node to a partition.
-spec add_node_to_partition(binary(), binary()) -> ok | {error, term()}.
add_node_to_partition(PartitionName, NodeName)
  when is_binary(PartitionName), is_binary(NodeName) ->
    gen_server:call(?SERVER, {add_node, PartitionName, NodeName}).

%% @doc Remove a node from a partition.
-spec remove_node_from_partition(binary(), binary()) -> ok | {error, term()}.
remove_node_from_partition(PartitionName, NodeName)
  when is_binary(PartitionName), is_binary(NodeName) ->
    gen_server:call(?SERVER, {remove_node, PartitionName, NodeName}).

%% @doc Update partition configuration.
-spec update_partition(binary(), map()) -> ok | {error, not_found}.
update_partition(Name, Updates) when is_binary(Name), is_map(Updates) ->
    gen_server:call(?SERVER, {update, Name, Updates}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS table for partitions
    ets:new(?PARTITIONS_TABLE, [
        named_table,
        set,
        public,
        {read_concurrency, true}
    ]),

    %% Create a default partition
    DefaultPartition = #partition{
        name = <<"default">>,
        priority = 1000,
        state = up,
        nodes = [],
        max_time = 86400,      % 24 hours
        default_time = 3600,   % 1 hour
        max_nodes = 0,         % unlimited
        allow_root = false
    },
    ets:insert(?PARTITIONS_TABLE, {<<"default">>, DefaultPartition}),
    ets:insert(?PARTITIONS_TABLE, {?DEFAULT_PARTITION_KEY, <<"default">>}),

    {ok, #{}}.

handle_call({register, Name, Config}, _From, State) ->
    case ets:lookup(?PARTITIONS_TABLE, Name) of
        [] ->
            Partition = map_to_partition(Name, Config),
            ets:insert(?PARTITIONS_TABLE, {Name, Partition}),
            %% If this is marked as default, update default pointer
            case maps:get(default, Config, false) of
                true -> ets:insert(?PARTITIONS_TABLE, {?DEFAULT_PARTITION_KEY, Name});
                false -> ok
            end,
            {reply, ok, State};
        [_] ->
            {reply, {error, already_exists}, State}
    end;

handle_call({unregister, Name}, _From, State) ->
    case ets:lookup(?PARTITIONS_TABLE, Name) of
        [] ->
            {reply, {error, not_found}, State};
        [_] ->
            ets:delete(?PARTITIONS_TABLE, Name),
            %% If this was the default, clear it
            case ets:lookup(?PARTITIONS_TABLE, ?DEFAULT_PARTITION_KEY) of
                [{?DEFAULT_PARTITION_KEY, Name}] ->
                    ets:delete(?PARTITIONS_TABLE, ?DEFAULT_PARTITION_KEY);
                _ ->
                    ok
            end,
            {reply, ok, State}
    end;

handle_call({set_priority, Name, Priority}, _From, State) ->
    case ets:lookup(?PARTITIONS_TABLE, Name) of
        [{Name, #partition{} = P}] ->
            ets:insert(?PARTITIONS_TABLE, {Name, P#partition{priority = Priority}}),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({set_default, Name}, _From, State) ->
    case ets:lookup(?PARTITIONS_TABLE, Name) of
        [{Name, #partition{}}] ->
            ets:insert(?PARTITIONS_TABLE, {?DEFAULT_PARTITION_KEY, Name}),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({add_node, PartitionName, NodeName}, _From, State) ->
    case ets:lookup(?PARTITIONS_TABLE, PartitionName) of
        [{PartitionName, #partition{nodes = Nodes} = P}] ->
            case lists:member(NodeName, Nodes) of
                true ->
                    {reply, {error, already_member}, State};
                false ->
                    ets:insert(?PARTITIONS_TABLE,
                              {PartitionName, P#partition{nodes = [NodeName | Nodes]}}),
                    {reply, ok, State}
            end;
        [] ->
            {reply, {error, partition_not_found}, State}
    end;

handle_call({remove_node, PartitionName, NodeName}, _From, State) ->
    case ets:lookup(?PARTITIONS_TABLE, PartitionName) of
        [{PartitionName, #partition{nodes = Nodes} = P}] ->
            NewNodes = lists:delete(NodeName, Nodes),
            ets:insert(?PARTITIONS_TABLE, {PartitionName, P#partition{nodes = NewNodes}}),
            {reply, ok, State};
        [] ->
            {reply, {error, partition_not_found}, State}
    end;

handle_call({update, Name, Updates}, _From, State) ->
    case ets:lookup(?PARTITIONS_TABLE, Name) of
        [{Name, #partition{} = P}] ->
            UpdatedPartition = apply_updates(P, Updates),
            ets:insert(?PARTITIONS_TABLE, {Name, UpdatedPartition}),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Convert a map to a partition record
map_to_partition(Name, Config) ->
    #partition{
        name = Name,
        priority = maps:get(priority, Config, 1000),
        nodes = maps:get(nodes, Config, []),
        state = maps:get(state, Config, up),
        max_time = maps:get(max_time, Config, 86400),
        default_time = maps:get(default_time, Config, 3600),
        max_nodes = maps:get(max_nodes, Config, 0),
        allow_root = maps:get(allow_root, Config, false)
    }.

%% @private Convert a partition record to a map
partition_to_map(#partition{} = P) ->
    #{
        name => P#partition.name,
        priority => P#partition.priority,
        nodes => P#partition.nodes,
        state => P#partition.state,
        max_time => P#partition.max_time,
        default_time => P#partition.default_time,
        max_nodes => P#partition.max_nodes,
        allow_root => P#partition.allow_root
    }.

%% @private Apply updates to a partition record
apply_updates(P, Updates) ->
    P#partition{
        priority = maps:get(priority, Updates, P#partition.priority),
        nodes = maps:get(nodes, Updates, P#partition.nodes),
        state = maps:get(state, Updates, P#partition.state),
        max_time = maps:get(max_time, Updates, P#partition.max_time),
        default_time = maps:get(default_time, Updates, P#partition.default_time),
        max_nodes = maps:get(max_nodes, Updates, P#partition.max_nodes),
        allow_root = maps:get(allow_root, Updates, P#partition.allow_root)
    }.
