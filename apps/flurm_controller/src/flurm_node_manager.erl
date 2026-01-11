%%%-------------------------------------------------------------------
%%% @doc FLURM Node Manager
%%%
%%% Tracks compute node status, handles node registration, heartbeats,
%%% and maintains the cluster node inventory.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_manager).

-behaviour(gen_server).

-export([start_link/0]).
-export([register_node/1, update_node/2, get_node/1, list_nodes/0]).
-export([heartbeat/1, get_available_nodes/0]).
-export([get_available_nodes_for_job/3, allocate_resources/4, release_resources/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("flurm_core/include/flurm_core.hrl").

-define(HEARTBEAT_TIMEOUT, 30000). % 30 seconds

-record(state, {
    nodes = #{} :: #{binary() => #node{}}
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register_node(map()) -> ok | {error, term()}.
register_node(NodeSpec) ->
    gen_server:call(?MODULE, {register_node, NodeSpec}).

-spec update_node(binary(), map()) -> ok | {error, term()}.
update_node(Hostname, Updates) ->
    gen_server:call(?MODULE, {update_node, Hostname, Updates}).

-spec get_node(binary()) -> {ok, #node{}} | {error, not_found}.
get_node(Hostname) ->
    gen_server:call(?MODULE, {get_node, Hostname}).

-spec list_nodes() -> [#node{}].
list_nodes() ->
    gen_server:call(?MODULE, list_nodes).

-spec heartbeat(map()) -> ok.
heartbeat(HeartbeatData) ->
    gen_server:cast(?MODULE, {heartbeat, HeartbeatData}).

-spec get_available_nodes() -> [#node{}].
get_available_nodes() ->
    gen_server:call(?MODULE, get_available_nodes).

%% @doc Get nodes that can run a job with specified requirements.
%% Returns nodes with sufficient available resources.
-spec get_available_nodes_for_job(NumCpus :: pos_integer(),
                                   MemoryMb :: pos_integer(),
                                   Partition :: binary()) -> [#node{}].
get_available_nodes_for_job(NumCpus, MemoryMb, Partition) ->
    gen_server:call(?MODULE, {get_available_nodes_for_job, NumCpus, MemoryMb, Partition}).

%% @doc Allocate resources on a node for a job.
-spec allocate_resources(Hostname :: binary(),
                         JobId :: pos_integer(),
                         Cpus :: pos_integer(),
                         MemoryMb :: pos_integer()) -> ok | {error, term()}.
allocate_resources(Hostname, JobId, Cpus, MemoryMb) ->
    gen_server:call(?MODULE, {allocate_resources, Hostname, JobId, Cpus, MemoryMb}).

%% @doc Release resources on a node when a job completes.
-spec release_resources(Hostname :: binary(), JobId :: pos_integer()) -> ok.
release_resources(Hostname, JobId) ->
    gen_server:cast(?MODULE, {release_resources, Hostname, JobId}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("Node Manager started"),
    %% Start heartbeat checker
    erlang:send_after(?HEARTBEAT_TIMEOUT, self(), check_heartbeats),
    {ok, #state{}}.

handle_call({register_node, NodeSpec}, _From, #state{nodes = Nodes} = State) ->
    Node = flurm_core:new_node(NodeSpec),
    Hostname = flurm_core:node_hostname(Node),
    UpdatedNode = flurm_core:update_node_state(Node, idle),
    UpdatedNode2 = UpdatedNode#node{last_heartbeat = erlang:system_time(second)},
    lager:info("Node ~s registered", [Hostname]),
    NewNodes = maps:put(Hostname, UpdatedNode2, Nodes),
    {reply, ok, State#state{nodes = NewNodes}};

handle_call({update_node, Hostname, Updates}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            UpdatedNode = apply_node_updates(Node, Updates),
            NewNodes = maps:put(Hostname, UpdatedNode, Nodes),
            {reply, ok, State#state{nodes = NewNodes}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_node, Hostname}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            {reply, {ok, Node}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(list_nodes, _From, #state{nodes = Nodes} = State) ->
    {reply, maps:values(Nodes), State};

handle_call(get_available_nodes, _From, #state{nodes = Nodes} = State) ->
    Available = [N || N <- maps:values(Nodes),
                      flurm_core:node_state(N) =:= idle orelse
                      flurm_core:node_state(N) =:= mixed],
    {reply, Available, State};

handle_call({get_available_nodes_for_job, NumCpus, MemoryMb, Partition}, _From,
            #state{nodes = Nodes} = State) ->
    %% Filter nodes that:
    %% 1. Are in idle or mixed state
    %% 2. Have enough available resources
    %% 3. Are in the requested partition (or default matches any)
    AllNodes = maps:values(Nodes),
    lager:debug("get_available_nodes_for_job: checking ~p registered nodes for ~p cpus, ~p MB, partition ~p",
               [length(AllNodes), NumCpus, MemoryMb, Partition]),
    Available = lists:filter(
        fun(Node) ->
            NodeState = flurm_core:node_state(Node),
            {CpusAvail, MemAvail} = get_available_resources(Node),
            InPartition = case Partition of
                <<"default">> -> true;
                _ -> lists:member(Partition, Node#node.partitions)
            end,
            Eligible = (NodeState =:= idle orelse NodeState =:= mixed) andalso
                       CpusAvail >= NumCpus andalso
                       MemAvail >= MemoryMb andalso
                       InPartition,
            lager:debug("Node ~s: state=~p, cpus=~p/~p, mem=~p/~p, partition_ok=~p, eligible=~p",
                       [Node#node.hostname, NodeState, CpusAvail, Node#node.cpus,
                        MemAvail, Node#node.memory_mb, InPartition, Eligible]),
            Eligible
        end,
        AllNodes
    ),
    lager:debug("get_available_nodes_for_job: found ~p available nodes", [length(Available)]),
    {reply, Available, State};

handle_call({allocate_resources, Hostname, JobId, Cpus, MemoryMb}, _From,
            #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            {CpusAvail, MemAvail} = get_available_resources(Node),
            if
                CpusAvail >= Cpus andalso MemAvail >= MemoryMb ->
                    %% Add allocation
                    NewAllocations = maps:put(JobId, {Cpus, MemoryMb}, Node#node.allocations),
                    UpdatedNode = Node#node{
                        allocations = NewAllocations,
                        running_jobs = [JobId | Node#node.running_jobs]
                    },
                    %% Update node state if now fully allocated
                    {NewCpusAvail, _} = get_available_resources(UpdatedNode),
                    FinalNode = if
                        NewCpusAvail =:= 0 ->
                            flurm_core:update_node_state(UpdatedNode, allocated);
                        true ->
                            flurm_core:update_node_state(UpdatedNode, mixed)
                    end,
                    NewNodes = maps:put(Hostname, FinalNode, Nodes),
                    lager:info("Allocated ~p CPUs, ~p MB to job ~p on node ~s",
                               [Cpus, MemoryMb, JobId, Hostname]),
                    {reply, ok, State#state{nodes = NewNodes}};
                true ->
                    {reply, {error, insufficient_resources}, State}
            end;
        error ->
            {reply, {error, node_not_found}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({heartbeat, #{hostname := Hostname} = Data}, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            UpdatedNode = Node#node{
                last_heartbeat = erlang:system_time(second),
                load_avg = maps:get(load_avg, Data, Node#node.load_avg),
                free_memory_mb = maps:get(free_memory_mb, Data, Node#node.free_memory_mb),
                running_jobs = maps:get(running_jobs, Data, Node#node.running_jobs)
            },
            NewNodes = maps:put(Hostname, UpdatedNode, Nodes),
            {noreply, State#state{nodes = NewNodes}};
        error ->
            lager:warning("Heartbeat from unknown node: ~s", [Hostname]),
            {noreply, State}
    end;

handle_cast({release_resources, Hostname, JobId}, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            %% Remove allocation
            NewAllocations = maps:remove(JobId, Node#node.allocations),
            NewRunningJobs = lists:delete(JobId, Node#node.running_jobs),
            UpdatedNode = Node#node{
                allocations = NewAllocations,
                running_jobs = NewRunningJobs
            },
            %% Update node state
            FinalNode = case map_size(NewAllocations) of
                0 ->
                    flurm_core:update_node_state(UpdatedNode, idle);
                _ ->
                    flurm_core:update_node_state(UpdatedNode, mixed)
            end,
            NewNodes = maps:put(Hostname, FinalNode, Nodes),
            lager:info("Released resources for job ~p on node ~s", [JobId, Hostname]),
            {noreply, State#state{nodes = NewNodes}};
        error ->
            lager:warning("Release resources for unknown node: ~s", [Hostname]),
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_heartbeats, #state{nodes = Nodes} = State) ->
    Now = erlang:system_time(second),
    Timeout = ?HEARTBEAT_TIMEOUT div 1000,
    NewNodes = maps:map(fun(_Hostname, Node) ->
        case Node#node.last_heartbeat of
            undefined ->
                Node;
            LastHB when Now - LastHB > Timeout * 2 ->
                lager:warning("Node ~s marked as down (no heartbeat)", [Node#node.hostname]),
                flurm_core:update_node_state(Node, down);
            _ ->
                Node
        end
    end, Nodes),
    erlang:send_after(?HEARTBEAT_TIMEOUT, self(), check_heartbeats),
    {noreply, State#state{nodes = NewNodes}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

apply_node_updates(Node, Updates) ->
    maps:fold(fun
        (state, Value, N) -> flurm_core:update_node_state(N, Value);
        (load_avg, Value, N) -> N#node{load_avg = Value};
        (free_memory_mb, Value, N) -> N#node{free_memory_mb = Value};
        (running_jobs, Value, N) -> N#node{running_jobs = Value};
        (_, _, N) -> N
    end, Node, Updates).

%% @private
%% Calculate available resources (total - allocated)
get_available_resources(#node{cpus = TotalCpus, memory_mb = TotalMemory,
                              allocations = Allocations}) ->
    {UsedCpus, UsedMemory} = maps:fold(
        fun(_JobId, {Cpus, Memory}, {AccCpus, AccMem}) ->
            {AccCpus + Cpus, AccMem + Memory}
        end,
        {0, 0},
        Allocations
    ),
    {TotalCpus - UsedCpus, TotalMemory - UsedMemory}.
