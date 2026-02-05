%%%-------------------------------------------------------------------
%%% @doc FLURM Partition Manager
%%%
%%% Manages cluster partitions (queues) that group nodes for
%%% job scheduling purposes.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_partition_manager).

-behaviour(gen_server).

-export([start_link/0]).
-export([create_partition/1, update_partition/2, delete_partition/1]).
-export([get_partition/1, list_partitions/0]).
-export([add_node/2, remove_node/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Test exports - internal helpers for direct callback testing
-ifdef(TEST).
-export([
    apply_partition_updates/2
]).
-endif.

-include_lib("flurm_core/include/flurm_core.hrl").

-record(state, {
    partitions = #{} :: #{binary() => #partition{}}
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            %% Process already running - return existing pid
            %% This handles race conditions during startup and restarts
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

-spec create_partition(map()) -> ok | {error, term()}.
create_partition(PartitionSpec) ->
    gen_server:call(?MODULE, {create_partition, PartitionSpec}).

-spec update_partition(binary(), map()) -> ok | {error, term()}.
update_partition(Name, Updates) ->
    gen_server:call(?MODULE, {update_partition, Name, Updates}).

-spec delete_partition(binary()) -> ok | {error, term()}.
delete_partition(Name) ->
    gen_server:call(?MODULE, {delete_partition, Name}).

-spec get_partition(binary()) -> {ok, #partition{}} | {error, not_found}.
get_partition(Name) ->
    gen_server:call(?MODULE, {get_partition, Name}).

-spec list_partitions() -> [#partition{}].
list_partitions() ->
    gen_server:call(?MODULE, list_partitions).

-spec add_node(binary(), binary()) -> ok | {error, term()}.
add_node(PartitionName, NodeName) ->
    gen_server:call(?MODULE, {add_node, PartitionName, NodeName}).

-spec remove_node(binary(), binary()) -> ok | {error, term()}.
remove_node(PartitionName, NodeName) ->
    gen_server:call(?MODULE, {remove_node, PartitionName, NodeName}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("Partition Manager started"),
    %% Load partitions from config or create default
    ConfigPartitions = case application:get_env(flurm_controller, partitions) of
        {ok, PartitionList} when is_list(PartitionList) ->
            lager:info("Loading ~p partitions from config", [length(PartitionList)]),
            lists:foldl(fun({Name, Spec}, Acc) when is_binary(Name), is_map(Spec) ->
                            %% Add name to spec
                            FullSpec = Spec#{name => Name, state => up},
                            P = flurm_core:new_partition(FullSpec),
                            lager:info("Created partition: ~s", [Name]),
                            maps:put(Name, P, Acc);
                           ({Name, Spec}, Acc) when is_list(Name), is_map(Spec) ->
                            %% Convert list to binary
                            BinName = list_to_binary(Name),
                            FullSpec = Spec#{name => BinName, state => up},
                            P = flurm_core:new_partition(FullSpec),
                            lager:info("Created partition: ~s", [BinName]),
                            maps:put(BinName, P, Acc);
                           (_, Acc) ->
                            Acc
                        end, #{}, PartitionList);
        _ ->
            lager:info("No partitions configured, creating default"),
            DefaultPartition = flurm_core:new_partition(#{
                name => <<"default">>,
                state => up,
                nodes => [],
                max_time => 86400,      % 24 hours
                default_time => 3600,   % 1 hour
                max_nodes => 1000,
                priority => 100
            }),
            #{<<"default">> => DefaultPartition}
    end,
    %% Ensure at least default partition exists
    Partitions = case maps:is_key(<<"default">>, ConfigPartitions) of
        true -> ConfigPartitions;
        false ->
            DefaultP = flurm_core:new_partition(#{
                name => <<"default">>,
                state => up,
                nodes => [],
                max_time => 86400,
                default_time => 3600,
                max_nodes => 1000,
                priority => 100
            }),
            maps:put(<<"default">>, DefaultP, ConfigPartitions)
    end,
    {ok, #state{partitions = Partitions}}.

handle_call({create_partition, PartitionSpec}, _From, #state{partitions = Partitions} = State) ->
    Partition = flurm_core:new_partition(PartitionSpec),
    Name = flurm_core:partition_name(Partition),
    case maps:is_key(Name, Partitions) of
        true ->
            {reply, {error, already_exists}, State};
        false ->
            lager:info("Partition ~s created", [Name]),
            NewPartitions = maps:put(Name, Partition, Partitions),
            {reply, ok, State#state{partitions = NewPartitions}}
    end;

handle_call({update_partition, Name, Updates}, _From, #state{partitions = Partitions} = State) ->
    case maps:find(Name, Partitions) of
        {ok, Partition} ->
            UpdatedPartition = apply_partition_updates(Partition, Updates),
            NewPartitions = maps:put(Name, UpdatedPartition, Partitions),
            {reply, ok, State#state{partitions = NewPartitions}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({delete_partition, Name}, _From, #state{partitions = Partitions} = State) ->
    case Name of
        <<"default">> ->
            {reply, {error, cannot_delete_default}, State};
        _ ->
            case maps:is_key(Name, Partitions) of
                true ->
                    lager:info("Partition ~s deleted", [Name]),
                    NewPartitions = maps:remove(Name, Partitions),
                    {reply, ok, State#state{partitions = NewPartitions}};
                false ->
                    {reply, {error, not_found}, State}
            end
    end;

handle_call({get_partition, Name}, _From, #state{partitions = Partitions} = State) ->
    case maps:find(Name, Partitions) of
        {ok, Partition} ->
            {reply, {ok, Partition}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(list_partitions, _From, #state{partitions = Partitions} = State) ->
    {reply, maps:values(Partitions), State};

handle_call({add_node, PartitionName, NodeName}, _From, #state{partitions = Partitions} = State) ->
    case maps:find(PartitionName, Partitions) of
        {ok, Partition} ->
            UpdatedPartition = flurm_core:add_node_to_partition(Partition, NodeName),
            NewPartitions = maps:put(PartitionName, UpdatedPartition, Partitions),
            lager:info("Node ~s added to partition ~s", [NodeName, PartitionName]),
            {reply, ok, State#state{partitions = NewPartitions}};
        error ->
            {reply, {error, partition_not_found}, State}
    end;

handle_call({remove_node, PartitionName, NodeName}, _From, #state{partitions = Partitions} = State) ->
    case maps:find(PartitionName, Partitions) of
        {ok, #partition{nodes = Nodes} = Partition} ->
            UpdatedPartition = Partition#partition{nodes = lists:delete(NodeName, Nodes)},
            NewPartitions = maps:put(PartitionName, UpdatedPartition, Partitions),
            lager:info("Node ~s removed from partition ~s", [NodeName, PartitionName]),
            {reply, ok, State#state{partitions = NewPartitions}};
        error ->
            {reply, {error, partition_not_found}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

apply_partition_updates(Partition, Updates) ->
    maps:fold(fun
        (state, Value, P) -> P#partition{state = Value};
        (max_time, Value, P) -> P#partition{max_time = Value};
        (default_time, Value, P) -> P#partition{default_time = Value};
        (max_nodes, Value, P) -> P#partition{max_nodes = Value};
        (priority, Value, P) -> P#partition{priority = Value};
        (allow_root, Value, P) -> P#partition{allow_root = Value};
        (_, _, P) -> P
    end, Partition, Updates).
