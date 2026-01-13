%%%-------------------------------------------------------------------
%%% @doc FLURM Node Manager
%%%
%%% Tracks compute node status, handles node registration, heartbeats,
%%% and maintains the cluster node inventory.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_manager_server).

-behaviour(gen_server).

-export([start_link/0]).
-export([register_node/1, update_node/2, get_node/1, list_nodes/0]).
-export([heartbeat/1, get_available_nodes/0]).
-export([get_available_nodes_for_job/3, allocate_resources/4, release_resources/2]).
%% Drain mode API
-export([drain_node/2, undrain_node/1, get_drain_reason/1, is_node_draining/1]).
%% Dynamic node operations (runtime add/remove)
-export([add_node/1, remove_node/1, remove_node/2, update_node_properties/2]).
-export([get_running_jobs_on_node/1, wait_for_node_drain/2, sync_nodes_from_config/0]).
%% GRES (GPU/accelerator) resource management
-export([register_node_gres/2, get_node_gres/1, allocate_gres/4, release_gres/2]).
-export([get_available_nodes_with_gres/4]).
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

%% @doc Drain a node, preventing new job assignments.
%% Existing jobs will continue running until completion.
-spec drain_node(Hostname :: binary(), Reason :: binary()) -> ok | {error, term()}.
drain_node(Hostname, Reason) ->
    gen_server:call(?MODULE, {drain_node, Hostname, Reason}).

%% @doc Resume a drained node, allowing new job assignments.
-spec undrain_node(Hostname :: binary()) -> ok | {error, term()}.
undrain_node(Hostname) ->
    gen_server:call(?MODULE, {undrain_node, Hostname}).

%% @doc Get the drain reason for a node.
-spec get_drain_reason(Hostname :: binary()) -> {ok, binary()} | {error, not_draining | not_found}.
get_drain_reason(Hostname) ->
    gen_server:call(?MODULE, {get_drain_reason, Hostname}).

%% @doc Check if a node is in draining state.
-spec is_node_draining(Hostname :: binary()) -> boolean() | {error, not_found}.
is_node_draining(Hostname) ->
    gen_server:call(?MODULE, {is_node_draining, Hostname}).

%%====================================================================
%% Dynamic Node Operations API
%%====================================================================

%% @doc Add a new node at runtime.
%% NodeSpec can be a map with node properties or a node name (binary)
%% to load from config. The node will be registered and added to
%% appropriate partitions.
-spec add_node(map() | binary()) -> ok | {error, term()}.
add_node(NodeSpec) when is_map(NodeSpec) ->
    gen_server:call(?MODULE, {add_node, NodeSpec});
add_node(NodeName) when is_binary(NodeName) ->
    %% Try to load node config from config server
    case flurm_config_server:get_node(NodeName) of
        undefined ->
            {error, {node_not_in_config, NodeName}};
        NodeConfig when is_map(NodeConfig) ->
            %% Convert config format to internal format
            NodeSpec = config_to_node_spec(NodeName, NodeConfig),
            gen_server:call(?MODULE, {add_node, NodeSpec})
    end.

%% @doc Remove a node gracefully.
%% This will drain the node first (wait for running jobs to complete)
%% and then remove it from the scheduler's available pool.
%% Uses default timeout of 5 minutes for drain.
-spec remove_node(Hostname :: binary()) -> ok | {error, term()}.
remove_node(Hostname) ->
    remove_node(Hostname, 300000). % 5 minute default timeout

%% @doc Remove a node with a specific drain timeout.
%% If Force is 'true' or timeout is 0, remove immediately without waiting.
-spec remove_node(Hostname :: binary(), Timeout :: non_neg_integer() | force) -> ok | {error, term()}.
remove_node(Hostname, force) ->
    gen_server:call(?MODULE, {remove_node, Hostname, force});
remove_node(Hostname, Timeout) when is_integer(Timeout), Timeout >= 0 ->
    gen_server:call(?MODULE, {remove_node, Hostname, Timeout}, Timeout + 5000).

%% @doc Update node properties at runtime (CPUs, memory, features, etc.).
%% This allows changing node configuration without restarting.
-spec update_node_properties(Hostname :: binary(), Updates :: map()) -> ok | {error, term()}.
update_node_properties(Hostname, Updates) when is_binary(Hostname), is_map(Updates) ->
    gen_server:call(?MODULE, {update_node_properties, Hostname, Updates}).

%% @doc Get list of running jobs on a node.
-spec get_running_jobs_on_node(Hostname :: binary()) -> {ok, [pos_integer()]} | {error, not_found}.
get_running_jobs_on_node(Hostname) ->
    gen_server:call(?MODULE, {get_running_jobs_on_node, Hostname}).

%% @doc Wait for a node to finish draining (all jobs complete).
%% Returns ok when node has no running jobs, or {error, timeout} if Timeout expires.
-spec wait_for_node_drain(Hostname :: binary(), Timeout :: non_neg_integer()) -> ok | {error, term()}.
wait_for_node_drain(Hostname, Timeout) ->
    gen_server:call(?MODULE, {wait_for_node_drain, Hostname, Timeout}, Timeout + 5000).

%% @doc Synchronize nodes with current configuration.
%% Adds new nodes from config, removes nodes not in config, updates changed properties.
-spec sync_nodes_from_config() -> {ok, #{added => [binary()], removed => [binary()], updated => [binary()]}}.
sync_nodes_from_config() ->
    gen_server:call(?MODULE, sync_nodes_from_config, 60000).

%%====================================================================
%% GRES (GPU/Accelerator) API
%%====================================================================

%% @doc Register GRES (GPU/accelerator) resources on a node.
%% GRESList is a list of maps, each describing a GRES device:
%%   #{type => gpu, name => <<"a100">>, count => 4, memory_mb => 40960, indices => [0,1,2,3]}
-spec register_node_gres(binary(), [map()]) -> ok | {error, term()}.
register_node_gres(Hostname, GRESList) ->
    gen_server:call(?MODULE, {register_node_gres, Hostname, GRESList}).

%% @doc Get GRES configuration and availability for a node.
-spec get_node_gres(binary()) -> {ok, #{config => [map()], available => map(), total => map()}} | {error, not_found}.
get_node_gres(Hostname) ->
    gen_server:call(?MODULE, {get_node_gres, Hostname}).

%% @doc Allocate GRES resources on a node for a job.
%% GRESSpec is a parsed GRES spec (e.g., from flurm_gres:parse_gres_string/1)
%% or a binary string like <<"gpu:2">> or <<"gpu:a100:4">>
-spec allocate_gres(binary(), pos_integer(), binary() | term(), boolean()) ->
    {ok, [{atom(), non_neg_integer(), [non_neg_integer()]}]} | {error, term()}.
allocate_gres(Hostname, JobId, GRESSpec, Exclusive) ->
    gen_server:call(?MODULE, {allocate_gres, Hostname, JobId, GRESSpec, Exclusive}).

%% @doc Release GRES resources allocated to a job on a node.
-spec release_gres(binary(), pos_integer()) -> ok.
release_gres(Hostname, JobId) ->
    gen_server:cast(?MODULE, {release_gres, Hostname, JobId}).

%% @doc Get nodes that can run a job with specified CPU, memory, and GRES requirements.
%% GRESSpec is optional; pass <<>> for no GRES requirements.
-spec get_available_nodes_with_gres(pos_integer(), pos_integer(), binary(), binary()) -> [#node{}].
get_available_nodes_with_gres(NumCpus, MemoryMb, Partition, GRESSpec) ->
    gen_server:call(?MODULE, {get_available_nodes_with_gres, NumCpus, MemoryMb, Partition, GRESSpec}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("Node Manager started"),
    %% Start heartbeat checker
    erlang:send_after(?HEARTBEAT_TIMEOUT, self(), check_heartbeats),
    %% Subscribe to node config changes
    catch flurm_config_server:subscribe_changes([nodes]),
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
    %% 1. Are in idle or mixed state (excludes drain, down, allocated)
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
            %% Check state - only idle or mixed can accept new jobs
            %% drain state means node is being removed gracefully
            StateOk = (NodeState =:= idle orelse NodeState =:= mixed),
            Eligible = StateOk andalso
                       CpusAvail >= NumCpus andalso
                       MemAvail >= MemoryMb andalso
                       InPartition,
            %% Log if node is draining (for debugging)
            case NodeState of
                drain ->
                    lager:debug("Node ~s is draining (reason: ~p), skipping",
                               [Node#node.hostname, Node#node.drain_reason]);
                _ ->
                    ok
            end,
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

%% Drain mode operations
handle_call({drain_node, Hostname, Reason}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            %% Set node to drain state with reason
            UpdatedNode = flurm_core:update_node_state(Node, drain),
            FinalNode = UpdatedNode#node{drain_reason = Reason},
            NewNodes = maps:put(Hostname, FinalNode, Nodes),
            lager:info("Node ~s set to drain state: ~s", [Hostname, Reason]),
            %% Trigger scheduler to find new nodes for pending jobs
            catch flurm_scheduler:trigger_schedule(),
            {reply, ok, State#state{nodes = NewNodes}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({undrain_node, Hostname}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, #node{state = drain} = Node} ->
            %% Determine new state based on current allocations
            NewState = case map_size(Node#node.allocations) of
                0 -> idle;
                _ -> mixed
            end,
            UpdatedNode = flurm_core:update_node_state(Node, NewState),
            FinalNode = UpdatedNode#node{drain_reason = undefined},
            NewNodes = maps:put(Hostname, FinalNode, Nodes),
            lager:info("Node ~s resumed from drain state, now ~p", [Hostname, NewState]),
            %% Trigger scheduler since node is now available
            catch flurm_scheduler:trigger_schedule(),
            {reply, ok, State#state{nodes = NewNodes}};
        {ok, _Node} ->
            {reply, {error, not_draining}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_drain_reason, Hostname}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, #node{state = drain, drain_reason = Reason}} ->
            {reply, {ok, Reason}, State};
        {ok, _Node} ->
            {reply, {error, not_draining}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({is_node_draining, Hostname}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, #node{state = drain}} ->
            {reply, true, State};
        {ok, _Node} ->
            {reply, false, State};
        error ->
            {reply, {error, not_found}, State}
    end;

%%--------------------------------------------------------------------
%% Dynamic Node Operations
%%--------------------------------------------------------------------

%% Add a new node at runtime
handle_call({add_node, NodeSpec}, _From, #state{nodes = Nodes} = State) ->
    Hostname = maps:get(hostname, NodeSpec),
    case maps:find(Hostname, Nodes) of
        {ok, _ExistingNode} ->
            {reply, {error, already_registered}, State};
        error ->
            %% Create and register the new node
            Node = flurm_core:new_node(NodeSpec),
            UpdatedNode = flurm_core:update_node_state(Node, idle),
            FinalNode = UpdatedNode#node{last_heartbeat = erlang:system_time(second)},
            NewNodes = maps:put(Hostname, FinalNode, Nodes),
            lager:info("Node ~s added dynamically", [Hostname]),

            %% Add node to its partitions
            Partitions = Node#node.partitions,
            lists:foreach(fun(PartName) ->
                catch flurm_partition_registry:add_node_to_partition(PartName, Hostname)
            end, Partitions),

            %% Trigger scheduler to consider new node
            catch flurm_scheduler:trigger_schedule(),
            {reply, ok, State#state{nodes = NewNodes}}
    end;

%% Remove a node (with drain and wait)
handle_call({remove_node, Hostname, force}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            %% Force remove - don't wait for jobs
            NewState = do_remove_node(Hostname, Node, State),
            {reply, ok, NewState};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({remove_node, Hostname, Timeout}, From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            RunningJobs = Node#node.running_jobs,
            case RunningJobs of
                [] ->
                    %% No running jobs, remove immediately
                    NewState = do_remove_node(Hostname, Node, State),
                    {reply, ok, NewState};
                _ when Timeout =:= 0 ->
                    %% Timeout is 0, remove immediately
                    NewState = do_remove_node(Hostname, Node, State),
                    {reply, ok, NewState};
                _ ->
                    %% Has running jobs, need to drain first
                    %% Set node to drain state
                    UpdatedNode = flurm_core:update_node_state(Node, drain),
                    DrainedNode = UpdatedNode#node{drain_reason = <<"node removal pending">>},
                    NewNodes = maps:put(Hostname, DrainedNode, Nodes),
                    lager:info("Node ~s draining for removal, ~p jobs running", [Hostname, length(RunningJobs)]),

                    %% Spawn async process to wait for drain and complete removal
                    Self = self(),
                    spawn_link(fun() ->
                        Result = wait_for_drain_completion(Hostname, Timeout, Self),
                        gen_server:reply(From, Result)
                    end),
                    {noreply, State#state{nodes = NewNodes}}
            end;
        error ->
            {reply, {error, not_found}, State}
    end;

%% Update node properties
handle_call({update_node_properties, Hostname, Updates}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            UpdatedNode = apply_property_updates(Node, Updates),
            NewNodes = maps:put(Hostname, UpdatedNode, Nodes),
            lager:info("Node ~s properties updated: ~p", [Hostname, maps:keys(Updates)]),

            %% Update partition membership if partitions changed
            case maps:get(partitions, Updates, undefined) of
                undefined -> ok;
                NewPartitions ->
                    OldPartitions = Node#node.partitions,
                    %% Remove from old partitions
                    lists:foreach(fun(PartName) ->
                        case lists:member(PartName, NewPartitions) of
                            false -> catch flurm_partition_registry:remove_node_from_partition(PartName, Hostname);
                            true -> ok
                        end
                    end, OldPartitions),
                    %% Add to new partitions
                    lists:foreach(fun(PartName) ->
                        case lists:member(PartName, OldPartitions) of
                            false -> catch flurm_partition_registry:add_node_to_partition(PartName, Hostname);
                            true -> ok
                        end
                    end, NewPartitions)
            end,
            {reply, ok, State#state{nodes = NewNodes}};
        error ->
            {reply, {error, not_found}, State}
    end;

%% Get running jobs on a node
handle_call({get_running_jobs_on_node, Hostname}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            {reply, {ok, Node#node.running_jobs}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

%% Wait for node drain completion
handle_call({wait_for_node_drain, Hostname, Timeout}, From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            case Node#node.running_jobs of
                [] ->
                    {reply, ok, State};
                _ ->
                    %% Spawn async wait
                    Self = self(),
                    spawn_link(fun() ->
                        Result = wait_for_drain_completion(Hostname, Timeout, Self),
                        gen_server:reply(From, Result)
                    end),
                    {noreply, State}
            end;
        error ->
            {reply, {error, not_found}, State}
    end;

%% Complete the node removal after drain
handle_call({complete_node_removal, Hostname}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            NewState = do_remove_node(Hostname, Node, State),
            {reply, ok, NewState};
        error ->
            %% Node already removed
            {reply, ok, State}
    end;

%% Sync nodes from config
handle_call(sync_nodes_from_config, _From, State) ->
    {Result, NewState} = do_sync_nodes_from_config(State),
    {reply, {ok, Result}, NewState};

%%--------------------------------------------------------------------
%% GRES Operations
%%--------------------------------------------------------------------

%% Register GRES on a node
handle_call({register_node_gres, Hostname, GRESList}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            %% Build GRES config and availability maps
            {GRESConfig, GRESTotal, GRESAvailable} = build_gres_maps(GRESList),
            UpdatedNode = Node#node{
                gres_config = GRESConfig,
                gres_total = GRESTotal,
                gres_available = GRESAvailable
            },
            NewNodes = maps:put(Hostname, UpdatedNode, Nodes),
            lager:info("Node ~s GRES registered: ~p", [Hostname, GRESTotal]),
            %% Also register with flurm_gres module for advanced scheduling
            catch flurm_gres:register_node_gres(Hostname, GRESList),
            {reply, ok, State#state{nodes = NewNodes}};
        error ->
            {reply, {error, not_found}, State}
    end;

%% Get GRES info for a node
handle_call({get_node_gres, Hostname}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            Result = #{
                config => Node#node.gres_config,
                available => Node#node.gres_available,
                total => Node#node.gres_total,
                allocations => Node#node.gres_allocations
            },
            {reply, {ok, Result}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

%% Allocate GRES to a job
handle_call({allocate_gres, Hostname, JobId, GRESSpec, Exclusive}, _From,
            #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            case do_allocate_gres(Node, JobId, GRESSpec, Exclusive) of
                {ok, Allocations, UpdatedNode} ->
                    NewNodes = maps:put(Hostname, UpdatedNode, Nodes),
                    lager:info("Allocated GRES ~p to job ~p on node ~s",
                               [Allocations, JobId, Hostname]),
                    {reply, {ok, Allocations}, State#state{nodes = NewNodes}};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        error ->
            {reply, {error, node_not_found}, State}
    end;

%% Get available nodes with GRES
handle_call({get_available_nodes_with_gres, NumCpus, MemoryMb, Partition, GRESSpec}, _From,
            #state{nodes = Nodes} = State) ->
    AllNodes = maps:values(Nodes),
    %% First filter by CPU, memory, partition, and state
    BaseFiltered = lists:filter(
        fun(Node) ->
            NodeState = flurm_core:node_state(Node),
            {CpusAvail, MemAvail} = get_available_resources(Node),
            InPartition = case Partition of
                <<"default">> -> true;
                _ -> lists:member(Partition, Node#node.partitions)
            end,
            StateOk = (NodeState =:= idle orelse NodeState =:= mixed),
            StateOk andalso CpusAvail >= NumCpus andalso MemAvail >= MemoryMb andalso InPartition
        end,
        AllNodes
    ),
    %% Then filter by GRES if specified
    GRESFiltered = case GRESSpec of
        <<>> -> BaseFiltered;
        _ ->
            lists:filter(
                fun(Node) -> check_node_gres_availability(Node, GRESSpec) end,
                BaseFiltered
            )
    end,
    {reply, GRESFiltered, State};

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
            %% Remove CPU/memory allocation
            NewAllocations = maps:remove(JobId, Node#node.allocations),
            NewRunningJobs = lists:delete(JobId, Node#node.running_jobs),
            %% Also release any GRES allocations
            {NewGRESAvailable, NewGRESAllocations} = do_release_gres(Node, JobId),
            UpdatedNode = Node#node{
                allocations = NewAllocations,
                running_jobs = NewRunningJobs,
                gres_available = NewGRESAvailable,
                gres_allocations = NewGRESAllocations
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
            %% Also notify flurm_gres module
            catch flurm_gres:deallocate(JobId, Hostname),
            {noreply, State#state{nodes = NewNodes}};
        error ->
            lager:warning("Release resources for unknown node: ~s", [Hostname]),
            {noreply, State}
    end;

handle_cast({release_gres, Hostname, JobId}, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            {NewGRESAvailable, NewGRESAllocations} = do_release_gres(Node, JobId),
            UpdatedNode = Node#node{
                gres_available = NewGRESAvailable,
                gres_allocations = NewGRESAllocations
            },
            NewNodes = maps:put(Hostname, UpdatedNode, Nodes),
            lager:info("Released GRES for job ~p on node ~s", [JobId, Hostname]),
            %% Also notify flurm_gres module
            catch flurm_gres:deallocate(JobId, Hostname),
            {noreply, State#state{nodes = NewNodes}};
        error ->
            lager:warning("Release GRES for unknown node: ~s", [Hostname]),
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

%% Handle config reload notification from flurm_config_server
handle_info({config_reload_nodes, NodeDefs}, State) ->
    lager:info("Node manager received config reload with ~p node definitions", [length(NodeDefs)]),
    {_Result, NewState} = do_sync_with_config_defs(NodeDefs, State),
    {noreply, NewState};

%% Handle config changes from flurm_config_server (via subscribe_changes)
handle_info({config_changed, nodes, _OldNodes, NewNodes}, State) ->
    lager:info("Node manager received node config change: ~p definitions", [length(NewNodes)]),
    {_Result, NewState} = do_sync_with_config_defs(NewNodes, State),
    {noreply, NewState};

handle_info({config_changed, _Key, _OldValue, _NewValue}, State) ->
    %% Ignore other config changes
    {noreply, State};

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

%%====================================================================
%% Dynamic Node Operations Internal Functions
%%====================================================================

%% @private
%% Convert config format to internal node spec
config_to_node_spec(NodeName, NodeConfig) ->
    #{
        hostname => NodeName,
        cpus => maps:get(cpus, NodeConfig, maps:get(sockets, NodeConfig, 1) *
                        maps:get(corespersocket, NodeConfig, 1) *
                        maps:get(threadspercore, NodeConfig, 1)),
        memory_mb => maps:get(realmemory, NodeConfig, 1024),
        features => maps:get(feature, NodeConfig, []),
        partitions => maps:get(partitions, NodeConfig, [<<"default">>]),
        state => maps:get(state, NodeConfig, idle)
    }.

%% @private
%% Remove a node from the system completely
do_remove_node(Hostname, Node, #state{nodes = Nodes} = State) ->
    %% Remove from all partitions
    Partitions = Node#node.partitions,
    lists:foreach(fun(PartName) ->
        catch flurm_partition_registry:remove_node_from_partition(PartName, Hostname)
    end, Partitions),

    %% Remove from nodes map
    NewNodes = maps:remove(Hostname, Nodes),
    lager:info("Node ~s removed from cluster", [Hostname]),

    %% Trigger scheduler to re-evaluate jobs
    catch flurm_scheduler:trigger_schedule(),

    State#state{nodes = NewNodes}.

%% @private
%% Apply property updates to a node record
apply_property_updates(Node, Updates) ->
    Node1 = case maps:get(cpus, Updates, undefined) of
        undefined -> Node;
        Cpus -> Node#node{cpus = Cpus}
    end,
    Node2 = case maps:get(memory_mb, Updates, undefined) of
        undefined -> Node1;
        Memory -> Node1#node{memory_mb = Memory}
    end,
    Node3 = case maps:get(features, Updates, undefined) of
        undefined -> Node2;
        Features -> Node2#node{features = Features}
    end,
    Node4 = case maps:get(partitions, Updates, undefined) of
        undefined -> Node3;
        Partitions -> Node3#node{partitions = Partitions}
    end,
    case maps:get(state, Updates, undefined) of
        undefined -> Node4;
        NewState -> flurm_core:update_node_state(Node4, NewState)
    end.

%% @private
%% Wait for drain to complete with timeout
wait_for_drain_completion(Hostname, Timeout, NodeManager) ->
    EndTime = erlang:system_time(millisecond) + Timeout,
    wait_for_drain_loop(Hostname, EndTime, NodeManager).

wait_for_drain_loop(Hostname, EndTime, NodeManager) ->
    Now = erlang:system_time(millisecond),
    case Now >= EndTime of
        true ->
            lager:warning("Node ~s drain timed out, forcing removal", [Hostname]),
            gen_server:call(NodeManager, {complete_node_removal, Hostname}),
            {error, timeout};
        false ->
            %% Check if node still has running jobs
            case gen_server:call(NodeManager, {get_running_jobs_on_node, Hostname}) of
                {ok, []} ->
                    %% No more jobs, complete removal
                    gen_server:call(NodeManager, {complete_node_removal, Hostname}),
                    ok;
                {ok, Jobs} ->
                    lager:debug("Node ~s still has ~p jobs, waiting...", [Hostname, length(Jobs)]),
                    timer:sleep(min(5000, EndTime - Now)),
                    wait_for_drain_loop(Hostname, EndTime, NodeManager);
                {error, not_found} ->
                    %% Node already removed
                    ok
            end
    end.

%% @private
%% Sync nodes from config server
do_sync_nodes_from_config(State) ->
    NodeDefs = flurm_config_server:get_nodes(),
    do_sync_with_config_defs(NodeDefs, State).

%% @private
%% Sync nodes with a list of config definitions
do_sync_with_config_defs(NodeDefs, #state{nodes = CurrentNodes} = State) ->
    %% Expand all node hostlist patterns and collect expected nodes
    ExpectedNodes = lists:foldl(fun(NodeDef, Acc) ->
        case maps:get(nodename, NodeDef, undefined) of
            undefined -> Acc;
            NodePattern ->
                ExpandedNodes = case catch flurm_config_slurm:expand_hostlist(NodePattern) of
                    NodeList when is_list(NodeList) -> NodeList;
                    _ -> [NodePattern]
                end,
                lists:foldl(fun(NodeName, InnerAcc) ->
                    maps:put(NodeName, NodeDef, InnerAcc)
                end, Acc, ExpandedNodes)
        end
    end, #{}, NodeDefs),

    CurrentNodeNames = maps:keys(CurrentNodes),
    ExpectedNodeNames = maps:keys(ExpectedNodes),

    %% Find nodes to add (in config but not registered)
    NodesToAdd = lists:filter(fun(Name) ->
        not lists:member(Name, CurrentNodeNames)
    end, ExpectedNodeNames),

    %% Find nodes to remove (registered but not in config)
    NodesToRemove = lists:filter(fun(Name) ->
        not lists:member(Name, ExpectedNodeNames)
    end, CurrentNodeNames),

    %% Find nodes to update (in both)
    NodesToUpdate = lists:filter(fun(Name) ->
        lists:member(Name, ExpectedNodeNames)
    end, CurrentNodeNames),

    %% Add new nodes
    State1 = lists:foldl(fun(NodeName, AccState) ->
        NodeDef = maps:get(NodeName, ExpectedNodes),
        NodeSpec = config_to_node_spec(NodeName, NodeDef),
        Node = flurm_core:new_node(NodeSpec),
        UpdatedNode = flurm_core:update_node_state(Node, idle),
        FinalNode = UpdatedNode#node{last_heartbeat = erlang:system_time(second)},
        NewNodes = maps:put(NodeName, FinalNode, AccState#state.nodes),
        lager:info("Node ~s added from config sync", [NodeName]),

        %% Add to partitions
        Partitions = Node#node.partitions,
        lists:foreach(fun(PartName) ->
            catch flurm_partition_registry:add_node_to_partition(PartName, NodeName)
        end, Partitions),

        AccState#state{nodes = NewNodes}
    end, State, NodesToAdd),

    %% Remove nodes not in config (gracefully if they have jobs)
    State2 = lists:foldl(fun(NodeName, AccState) ->
        case maps:find(NodeName, AccState#state.nodes) of
            {ok, Node} ->
                case Node#node.running_jobs of
                    [] ->
                        %% No jobs, remove immediately
                        do_remove_node(NodeName, Node, AccState);
                    _Jobs ->
                        %% Has jobs, set to drain state for later removal
                        UpdatedNode = flurm_core:update_node_state(Node, drain),
                        DrainedNode = UpdatedNode#node{drain_reason = <<"removed from config">>},
                        NewNodes = maps:put(NodeName, DrainedNode, AccState#state.nodes),
                        lager:info("Node ~s marked for removal (draining)", [NodeName]),
                        AccState#state{nodes = NewNodes}
                end;
            error ->
                AccState
        end
    end, State1, NodesToRemove),

    %% Update existing nodes with changed properties
    UpdatedNodeNames = lists:foldl(fun(NodeName, Acc) ->
        NodeDef = maps:get(NodeName, ExpectedNodes),
        case maps:find(NodeName, State2#state.nodes) of
            {ok, Node} ->
                Updates = build_updates_from_config(Node, NodeDef),
                case maps:size(Updates) > 0 of
                    true ->
                        [NodeName | Acc];
                    false ->
                        Acc
                end;
            error ->
                Acc
        end
    end, [], NodesToUpdate),

    State3 = lists:foldl(fun(NodeName, AccState) ->
        NodeDef = maps:get(NodeName, ExpectedNodes),
        case maps:find(NodeName, AccState#state.nodes) of
            {ok, Node} ->
                Updates = build_updates_from_config(Node, NodeDef),
                UpdatedNode = apply_property_updates(Node, Updates),
                NewNodes = maps:put(NodeName, UpdatedNode, AccState#state.nodes),
                lager:debug("Node ~s updated from config", [NodeName]),
                AccState#state{nodes = NewNodes};
            error ->
                AccState
        end
    end, State2, UpdatedNodeNames),

    %% Trigger scheduler if any changes were made
    case {NodesToAdd, NodesToRemove, UpdatedNodeNames} of
        {[], [], []} -> ok;
        _ -> catch flurm_scheduler:trigger_schedule()
    end,

    Result = #{
        added => NodesToAdd,
        removed => NodesToRemove,
        updated => UpdatedNodeNames
    },
    {Result, State3}.

%% @private
%% Build updates map from config definition compared to current node
build_updates_from_config(Node, NodeDef) ->
    Updates0 = #{},
    %% Check CPUs
    ConfigCpus = maps:get(cpus, NodeDef,
                    maps:get(sockets, NodeDef, 1) *
                    maps:get(corespersocket, NodeDef, 1) *
                    maps:get(threadspercore, NodeDef, 1)),
    Updates1 = case ConfigCpus =/= Node#node.cpus of
        true -> maps:put(cpus, ConfigCpus, Updates0);
        false -> Updates0
    end,
    %% Check memory
    ConfigMemory = maps:get(realmemory, NodeDef, Node#node.memory_mb),
    Updates2 = case ConfigMemory =/= Node#node.memory_mb of
        true -> maps:put(memory_mb, ConfigMemory, Updates1);
        false -> Updates1
    end,
    %% Check features
    ConfigFeatures = maps:get(feature, NodeDef, Node#node.features),
    Updates3 = case ConfigFeatures =/= Node#node.features of
        true -> maps:put(features, ConfigFeatures, Updates2);
        false -> Updates2
    end,
    %% Check partitions
    ConfigPartitions = maps:get(partitions, NodeDef, Node#node.partitions),
    case ConfigPartitions =/= Node#node.partitions of
        true -> maps:put(partitions, ConfigPartitions, Updates3);
        false -> Updates3
    end.

%%====================================================================
%% GRES Internal Functions
%%====================================================================

%% @private
%% Build GRES maps from a list of GRES device specifications
%% Returns {GRESConfig, GRESTotal, GRESAvailable}
build_gres_maps(GRESList) ->
    %% GRESConfig is the raw list of device specs
    GRESConfig = GRESList,

    %% Build total and available counts
    {GRESTotal, GRESAvailable} = lists:foldl(
        fun(DeviceSpec, {TotalAcc, AvailAcc}) ->
            Type = maps:get(type, DeviceSpec, gpu),
            TypeBin = atom_to_gres_key(Type),
            Name = maps:get(name, DeviceSpec, <<>>),
            Count = maps:get(count, DeviceSpec, 1),

            %% Add to type total (e.g., <<"gpu">> => 4)
            NewTotal1 = maps:update_with(TypeBin, fun(V) -> V + Count end, Count, TotalAcc),
            NewAvail1 = maps:update_with(TypeBin, fun(V) -> V + Count end, Count, AvailAcc),

            %% If name specified, also track by type:name (e.g., <<"gpu:a100">> => 4)
            case Name of
                <<>> ->
                    {NewTotal1, NewAvail1};
                _ ->
                    TypeNameKey = <<TypeBin/binary, ":", Name/binary>>,
                    NewTotal2 = maps:update_with(TypeNameKey, fun(V) -> V + Count end, Count, NewTotal1),
                    NewAvail2 = maps:update_with(TypeNameKey, fun(V) -> V + Count end, Count, NewAvail1),
                    {NewTotal2, NewAvail2}
            end
        end,
        {#{}, #{}},
        GRESList
    ),
    {GRESConfig, GRESTotal, GRESAvailable}.

%% @private
%% Convert GRES type atom to binary key
atom_to_gres_key(gpu) -> <<"gpu">>;
atom_to_gres_key(fpga) -> <<"fpga">>;
atom_to_gres_key(mic) -> <<"mic">>;
atom_to_gres_key(mps) -> <<"mps">>;
atom_to_gres_key(shard) -> <<"shard">>;
atom_to_gres_key(Type) when is_atom(Type) -> atom_to_binary(Type);
atom_to_gres_key(Type) when is_binary(Type) -> Type.

%% @private
%% Check if a node can satisfy GRES requirements
check_node_gres_availability(#node{gres_available = GRESAvailable}, GRESSpec) when is_binary(GRESSpec) ->
    case flurm_gres:parse_gres_string(GRESSpec) of
        {ok, []} -> true;
        {ok, ParsedSpecs} ->
            check_gres_specs_available(ParsedSpecs, GRESAvailable);
        {error, _} -> false
    end;
check_node_gres_availability(#node{gres_available = GRESAvailable}, ParsedSpecs) when is_list(ParsedSpecs) ->
    check_gres_specs_available(ParsedSpecs, GRESAvailable).

%% @private
%% Check if all GRES specs can be satisfied by available resources
check_gres_specs_available([], _Available) -> true;
check_gres_specs_available([Spec | Rest], Available) ->
    case check_single_gres_spec(Spec, Available) of
        true -> check_gres_specs_available(Rest, Available);
        false -> false
    end.

%% @private
%% Check if a single GRES spec can be satisfied
check_single_gres_spec(Spec, Available) when is_map(Spec) ->
    Type = maps:get(type, Spec, gpu),
    Name = maps:get(name, Spec, any),
    Count = maps:get(count, Spec, 1),
    TypeBin = atom_to_gres_key(Type),

    %% Check if we have enough of this type (optionally with specific name)
    case Name of
        any ->
            maps:get(TypeBin, Available, 0) >= Count;
        _ when is_binary(Name) ->
            TypeNameKey = <<TypeBin/binary, ":", Name/binary>>,
            %% First try specific type:name, fallback to just type
            NameAvail = maps:get(TypeNameKey, Available, 0),
            TypeAvail = maps:get(TypeBin, Available, 0),
            NameAvail >= Count orelse TypeAvail >= Count;
        _ ->
            maps:get(TypeBin, Available, 0) >= Count
    end;
check_single_gres_spec(Spec, Available) ->
    %% Handle gres_spec record (from flurm_gres module)
    case erlang:is_tuple(Spec) andalso erlang:element(1, Spec) =:= gres_spec of
        true ->
            Type = erlang:element(2, Spec),  % type field
            Name = erlang:element(3, Spec),  % name field
            Count = erlang:element(4, Spec), % count field
            check_single_gres_spec(#{type => Type, name => Name, count => Count}, Available);
        false ->
            true  % Unknown spec format, assume ok
    end.

%% @private
%% Allocate GRES to a job on a node
do_allocate_gres(Node, JobId, GRESSpec, Exclusive) when is_binary(GRESSpec) ->
    case flurm_gres:parse_gres_string(GRESSpec) of
        {ok, []} -> {ok, [], Node};
        {ok, ParsedSpecs} ->
            do_allocate_gres(Node, JobId, ParsedSpecs, Exclusive);
        {error, Reason} -> {error, {parse_error, Reason}}
    end;
do_allocate_gres(Node, JobId, ParsedSpecs, _Exclusive) when is_list(ParsedSpecs) ->
    %% Check availability first
    case check_gres_specs_available(ParsedSpecs, Node#node.gres_available) of
        false -> {error, insufficient_gres};
        true ->
            %% Allocate each spec
            {Allocations, NewAvailable} = lists:foldl(
                fun(Spec, {AllocAcc, AvailAcc}) ->
                    {Type, Count, Indices} = allocate_single_gres_spec(Spec, AvailAcc, Node#node.gres_config),
                    NewAvailAcc = decrement_gres_available(Type, Count, Spec, AvailAcc),
                    {[{Type, Count, Indices} | AllocAcc], NewAvailAcc}
                end,
                {[], Node#node.gres_available},
                ParsedSpecs
            ),
            %% Record allocation for job
            NewGRESAllocations = maps:put(JobId, Allocations, Node#node.gres_allocations),
            UpdatedNode = Node#node{
                gres_available = NewAvailable,
                gres_allocations = NewGRESAllocations
            },
            {ok, Allocations, UpdatedNode}
    end.

%% @private
%% Allocate a single GRES spec, returning {Type, Count, Indices}
allocate_single_gres_spec(Spec, _Available, GRESConfig) when is_map(Spec) ->
    Type = maps:get(type, Spec, gpu),
    Count = maps:get(count, Spec, 1),
    %% Find available indices from config
    Indices = find_available_indices(Type, Count, GRESConfig),
    {Type, Count, Indices};
allocate_single_gres_spec(Spec, Available, GRESConfig) ->
    %% Handle gres_spec record
    case erlang:is_tuple(Spec) andalso erlang:element(1, Spec) =:= gres_spec of
        true ->
            Type = erlang:element(2, Spec),
            Count = erlang:element(4, Spec),
            allocate_single_gres_spec(#{type => Type, count => Count}, Available, GRESConfig);
        false ->
            {gpu, 0, []}
    end.

%% @private
%% Find available GRES indices from config
find_available_indices(Type, Count, GRESConfig) ->
    %% Get all indices for this type
    AllIndices = lists:flatmap(
        fun(DevSpec) ->
            DevType = maps:get(type, DevSpec, gpu),
            case DevType =:= Type of
                true ->
                    DevCount = maps:get(count, DevSpec, 1),
                    BaseIndex = maps:get(index, DevSpec, 0),
                    lists:seq(BaseIndex, BaseIndex + DevCount - 1);
                false ->
                    []
            end
        end,
        GRESConfig
    ),
    %% Return first Count indices (in production, would track which are actually free)
    lists:sublist(AllIndices, Count).

%% @private
%% Decrement available GRES counts
decrement_gres_available(Type, Count, Spec, Available) ->
    TypeBin = atom_to_gres_key(Type),
    %% Decrement type count
    NewAvail1 = maps:update_with(TypeBin, fun(V) -> max(0, V - Count) end, 0, Available),
    %% Also decrement type:name if specified
    Name = case is_map(Spec) of
        true -> maps:get(name, Spec, any);
        false -> any
    end,
    case Name of
        any -> NewAvail1;
        _ when is_binary(Name) ->
            TypeNameKey = <<TypeBin/binary, ":", Name/binary>>,
            maps:update_with(TypeNameKey, fun(V) -> max(0, V - Count) end, 0, NewAvail1);
        _ -> NewAvail1
    end.

%% @private
%% Release GRES allocated to a job
do_release_gres(Node, JobId) ->
    case maps:find(JobId, Node#node.gres_allocations) of
        {ok, Allocations} ->
            %% Return GRES to available pool
            NewAvailable = lists:foldl(
                fun({Type, Count, _Indices}, AvailAcc) ->
                    TypeBin = atom_to_gres_key(Type),
                    maps:update_with(TypeBin, fun(V) -> V + Count end, Count, AvailAcc)
                end,
                Node#node.gres_available,
                Allocations
            ),
            NewGRESAllocations = maps:remove(JobId, Node#node.gres_allocations),
            {NewAvailable, NewGRESAllocations};
        error ->
            %% No GRES allocated to this job
            {Node#node.gres_available, Node#node.gres_allocations}
    end.
