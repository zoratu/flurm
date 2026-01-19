%%%-------------------------------------------------------------------
%%% @doc FLURM Generic Resource (GRES) Management
%%%
%%% Handles generic resources like GPUs, FPGAs, MICs, and other
%%% accelerators. Compatible with SLURM's GRES specification.
%%%
%%% Supports:
%%% - GPU scheduling (NVIDIA, AMD, Intel)
%%% - GPU type/model constraints
%%% - GPU memory constraints
%%% - GPU topology awareness (NVLink, PCIe)
%%% - MIG (Multi-Instance GPU) support for partitioned GPUs
%%% - MPS (Multi-Process Service) support
%%% - GPU isolation via cgroups/namespaces
%%% - Exclusive vs shared GPU allocation modes
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_gres).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API - GRES Type Management
-export([
    start_link/0,
    register_type/2,
    unregister_type/1,
    list_types/0,
    get_type_info/1
]).

%% API - Node GRES Tracking
-export([
    register_node_gres/2,
    update_node_gres/2,
    get_node_gres/1,
    get_nodes_with_gres/1
]).

%% API - GRES Allocation for Jobs
-export([
    allocate/3,
    deallocate/2,
    get_job_gres/1,
    check_availability/2
]).

%% API - GRES Specification Parsing
-export([
    parse_gres_string/1,
    format_gres_spec/1
]).

%% API - Scheduler Integration
-export([
    filter_nodes_by_gres/2,
    score_node_gres/2
]).

%% Legacy API (kept for backwards compatibility)
-export([
    register_gres/3,
    unregister_gres/2,
    request_gres/3,
    release_gres/3,
    get_available_gres/1,
    get_gres_by_type/2,
    parse_gres_spec/1,
    match_gres_requirements/2
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

%% ETS Table Names
-define(GRES_TYPES_TABLE, flurm_gres_types).
-define(GRES_NODE_TABLE, flurm_gres_nodes).
-define(GRES_ALLOC_TABLE, flurm_gres_allocations).
-define(GRES_JOB_TABLE, flurm_gres_jobs).

%% Suppress unused warnings for detection functions (called externally)
-compile({nowarn_unused_function, [detect_gpus/0, detect_nvidia_gpus/0, detect_amd_gpus/0, detect_mig_instances/1]}).

%% Basic GRES types (defined first for use in records)
-type gres_type() :: gpu | fpga | mic | mps | shard | binary().
-type gres_name() :: binary().
-type gres_index() :: non_neg_integer().
-type node_id() :: binary().
-type gpu_vendor() :: nvidia | amd | intel | unknown.

%% GRES request specification (defined before type reference)
-record(gres_spec, {
    type :: gres_type(),
    name :: gres_name() | any,            % Specific model or any
    count :: pos_integer(),
    per_node :: pos_integer(),            % GRES per node
    per_task :: pos_integer(),            % GRES per task
    memory_mb :: non_neg_integer() | any,
    flags :: [atom()],                    % [exclusive, shared]
    exclusive :: boolean(),
    constraints :: [term()]               % Additional constraints
}).

%% Type for gres_spec record
-type gres_spec() :: #gres_spec{}.

%% GRES device on a node
-record(gres_device, {
    id :: {node_id(), gres_type(), gres_index()},
    node :: node_id(),
    type :: gres_type(),
    index :: gres_index(),
    name :: gres_name(),                  % e.g., <<"nvidia_a100">>
    count :: pos_integer(),               % Usually 1, but can be more for MPS
    memory_mb :: non_neg_integer(),
    flags :: [atom()],                    % [exclusive, shared, mps_enabled]
    links :: [gres_index()],              % Topology links (NVLink peers)
    cores :: [non_neg_integer()],         % CPU cores with affinity
    state :: available | allocated | draining | down,
    allocated_to :: job_id() | undefined,
    allocation_mode :: exclusive | shared | undefined,
    properties :: map()                   % Vendor-specific properties
}).

%% Job GRES allocation
-record(job_gres_alloc, {
    job_id :: job_id(),
    node :: node_id(),
    allocations :: [{gres_type(), [gres_index()]}],
    allocation_mode :: exclusive | shared,
    allocated_at :: non_neg_integer()
}).

%% Server state
-record(state, {
    topology_cache :: map(),
    mig_configs :: map()                  % MIG configurations per GPU
}).

-export_type([gres_type/0, gres_name/0, gres_spec/0, gpu_vendor/0]).

%% Test exports
-ifdef(TEST).
-export([
    %% Parsing helpers
    parse_single_gres/1,
    extract_flags/1,
    parse_gres_type/1,
    parse_count/1,
    parse_count_safe/1,
    parse_memory_constraint/1,
    format_single_gres/1,
    format_with_flags/2,
    format_type/1,
    %% GPU matching
    matches_gpu_model/2,
    check_gpu_aliases/2,
    %% Conversion helpers
    device_to_map/1,
    job_alloc_to_map/1,
    map_to_gres_spec/1,
    determine_allocation_mode/1,
    %% Topology helpers
    sort_by_topology/1,
    select_best_devices/3,
    %% Scoring
    calculate_topology_score/2,
    calculate_fit_score/2,
    calculate_memory_score/2,
    %% Availability checking
    can_satisfy_spec/2
]).
-endif.

%%====================================================================
%% API - GRES Type Management
%%====================================================================

%% @doc Start the GRES coordination gen_server
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register a GRES type with configuration
%% Config should include: count, type_specific (with GPU info for GPUs)
-spec register_type(gres_type(), map()) -> ok | {error, term()}.
register_type(Name, Config) ->
    gen_server:call(?SERVER, {register_type, Name, Config}).

%% @doc Unregister a GRES type
-spec unregister_type(gres_type()) -> ok | {error, term()}.
unregister_type(Name) ->
    gen_server:call(?SERVER, {unregister_type, Name}).

%% @doc List all registered GRES types
-spec list_types() -> [gres_type()].
list_types() ->
    case ets:info(?GRES_TYPES_TABLE) of
        undefined -> [];
        _ ->
            ets:foldl(fun({Name, _Config}, Acc) -> [Name | Acc] end, [], ?GRES_TYPES_TABLE)
    end.

%% @doc Get info about a specific GRES type
-spec get_type_info(gres_type()) -> {ok, map()} | {error, not_found}.
get_type_info(Name) ->
    case ets:lookup(?GRES_TYPES_TABLE, Name) of
        [{Name, Config}] -> {ok, Config};
        [] -> {error, not_found}
    end.

%%====================================================================
%% API - Node GRES Tracking
%%====================================================================

%% @doc Register GRES available on a node
%% GRESList is a list of maps with: type, index, name, memory_mb, flags, etc.
-spec register_node_gres(node_id(), [map()]) -> ok | {error, term()}.
register_node_gres(NodeName, GRESList) ->
    gen_server:call(?SERVER, {register_node_gres, NodeName, GRESList}).

%% @doc Update GRES availability on a node
-spec update_node_gres(node_id(), [map()]) -> ok | {error, term()}.
update_node_gres(NodeName, GRESList) ->
    gen_server:call(?SERVER, {update_node_gres, NodeName, GRESList}).

%% @doc Get all GRES available on a node
-spec get_node_gres(node_id()) -> {ok, [map()]} | {error, term()}.
get_node_gres(NodeId) ->
    case ets:info(?GRES_NODE_TABLE) of
        undefined -> {ok, []};
        _ ->
            Devices = ets:select(?GRES_NODE_TABLE, [{
                #gres_device{id = {NodeId, '_', '_'}, _ = '_'},
                [],
                ['$_']
            }]),
            {ok, [device_to_map(D) || D <- Devices]}
    end.

%% @doc Find nodes that have a specific GRES type available
-spec get_nodes_with_gres(gres_type()) -> [node_id()].
get_nodes_with_gres(GRESType) ->
    case ets:info(?GRES_NODE_TABLE) of
        undefined -> [];
        _ ->
            Devices = ets:select(?GRES_NODE_TABLE, [{
                #gres_device{type = GRESType, state = available, _ = '_'},
                [],
                ['$_']
            }]),
            lists:usort([D#gres_device.node || D <- Devices])
    end.

%%====================================================================
%% API - GRES Allocation for Jobs
%%====================================================================

%% @doc Allocate GRES for a job on a specific node
%% GRESSpec can be a string like "gpu:2" or a parsed spec
-spec allocate(job_id(), node_id(), term()) -> {ok, [gres_index()]} | {error, term()}.
allocate(JobId, NodeName, GRESSpec) when is_binary(GRESSpec); is_list(GRESSpec) ->
    case parse_gres_string(GRESSpec) of
        {ok, ParsedSpecs} ->
            allocate(JobId, NodeName, ParsedSpecs);
        {error, Reason} ->
            {error, {parse_error, Reason}}
    end;
allocate(JobId, NodeName, GRESSpecs) when is_list(GRESSpecs) ->
    gen_server:call(?SERVER, {allocate, JobId, NodeName, GRESSpecs});
allocate(JobId, NodeName, GRESSpec) ->
    allocate(JobId, NodeName, [GRESSpec]).

%% @doc Release GRES allocated to a job on a node
-spec deallocate(job_id(), node_id()) -> ok.
deallocate(JobId, NodeName) ->
    gen_server:call(?SERVER, {deallocate, JobId, NodeName}).

%% @doc Get GRES allocated to a job
-spec get_job_gres(job_id()) -> {ok, [map()]} | {error, not_found}.
get_job_gres(JobId) ->
    case ets:info(?GRES_JOB_TABLE) of
        undefined -> {error, not_found};
        _ ->
            case ets:lookup(?GRES_JOB_TABLE, JobId) of
                [] -> {error, not_found};
                Allocs ->
                    Maps = [job_alloc_to_map(A) || {_, A} <- Allocs],
                    {ok, Maps}
            end
    end.

%% @doc Check if GRES request can be satisfied on a node
-spec check_availability(node_id(), term()) -> boolean() | {error, term()}.
check_availability(NodeName, GRESSpec) when is_binary(GRESSpec); is_list(GRESSpec) ->
    case parse_gres_string(GRESSpec) of
        {ok, ParsedSpecs} ->
            check_availability(NodeName, ParsedSpecs);
        {error, _} ->
            false
    end;
check_availability(NodeName, GRESSpecs) when is_list(GRESSpecs) ->
    gen_server:call(?SERVER, {check_availability, NodeName, GRESSpecs});
check_availability(NodeName, GRESSpec) ->
    check_availability(NodeName, [GRESSpec]).

%%====================================================================
%% API - GRES Specification Parsing
%%====================================================================

%% @doc Parse a GRES string like "gpu:2", "gpu:a100:4", or "gpu:tesla:2,shard"
%% Supports formats:
%%   - "type:count" -> gpu:2
%%   - "type:name:count" -> gpu:a100:4
%%   - "type:name:count,flag" -> gpu:tesla:2,shard
%%   - Multiple specs separated by comma: "gpu:2,fpga:1"
-spec parse_gres_string(binary() | string()) -> {ok, [#gres_spec{}]} | {error, term()}.
parse_gres_string(Spec) when is_list(Spec) ->
    parse_gres_string(list_to_binary(Spec));
parse_gres_string(<<>>) ->
    {ok, []};
parse_gres_string(Spec) when is_binary(Spec) ->
    %% Split by comma for multiple GRES specs
    Parts = binary:split(Spec, <<",">>, [global]),
    try
        Requests = lists:filtermap(fun(Part) ->
            TrimmedPart = string:trim(binary_to_list(Part)),
            case TrimmedPart of
                "" -> false;
                _ -> {true, parse_single_gres(list_to_binary(TrimmedPart))}
            end
        end, Parts),
        {ok, Requests}
    catch
        throw:{parse_error, Reason} ->
            {error, Reason}
    end.

%% @doc Format GRES specs back to string format
-spec format_gres_spec([#gres_spec{}]) -> binary().
format_gres_spec([]) ->
    <<>>;
format_gres_spec(Specs) ->
    Parts = lists:map(fun format_single_gres/1, Specs),
    iolist_to_binary(lists:join(<<",">>, Parts)).

%%====================================================================
%% API - Scheduler Integration
%%====================================================================

%% @doc Filter a list of nodes by GRES requirements
%% Returns nodes that can satisfy the GRES spec
-spec filter_nodes_by_gres([node_id()], term()) -> [node_id()].
filter_nodes_by_gres(Nodes, GRESSpec) when is_binary(GRESSpec); is_list(GRESSpec) ->
    case parse_gres_string(GRESSpec) of
        {ok, []} -> Nodes;  % No GRES requirements
        {ok, ParsedSpecs} ->
            filter_nodes_by_gres(Nodes, ParsedSpecs);
        {error, _} ->
            []
    end;
filter_nodes_by_gres(Nodes, []) ->
    Nodes;
filter_nodes_by_gres(Nodes, GRESSpecs) when is_list(GRESSpecs) ->
    lists:filter(fun(Node) ->
        case check_availability(Node, GRESSpecs) of
            true -> true;
            _ -> false
        end
    end, Nodes).

%% @doc Score a node based on GRES fit for scheduling
%% Higher score = better fit. Returns 0-100.
-spec score_node_gres(node_id(), term()) -> non_neg_integer().
score_node_gres(NodeName, GRESSpec) when is_binary(GRESSpec); is_list(GRESSpec) ->
    case parse_gres_string(GRESSpec) of
        {ok, []} -> 50;  % No GRES requirements, neutral score
        {ok, ParsedSpecs} ->
            score_node_gres(NodeName, ParsedSpecs);
        {error, _} ->
            0
    end;
score_node_gres(_NodeName, []) ->
    50;  % No GRES requirements
score_node_gres(NodeName, GRESSpecs) when is_list(GRESSpecs) ->
    gen_server:call(?SERVER, {score_node_gres, NodeName, GRESSpecs}).

%%====================================================================
%% Legacy API (Backwards Compatibility)
%%====================================================================

%% @doc Legacy: Register GRES devices on a node
-spec register_gres(node_id(), gres_type(), [map()]) -> ok | {error, term()}.
register_gres(NodeId, Type, Devices) ->
    DevicesWithType = [D#{type => Type} || D <- Devices],
    register_node_gres(NodeId, DevicesWithType).

%% @doc Legacy: Unregister all GRES of a type from a node
-spec unregister_gres(node_id(), gres_type()) -> ok.
unregister_gres(NodeId, Type) ->
    gen_server:call(?SERVER, {unregister_gres, NodeId, Type}).

%% @doc Legacy: Request GRES allocation for a job
-spec request_gres(job_id(), node_id(), #gres_spec{}) ->
    {ok, [gres_index()]} | {error, term()}.
request_gres(JobId, NodeId, Request) ->
    allocate(JobId, NodeId, [Request]).

%% @doc Legacy: Release GRES allocated to a job
-spec release_gres(job_id(), node_id(), [gres_index()]) -> ok.
release_gres(JobId, NodeId, _Indices) ->
    deallocate(JobId, NodeId).

%% @doc Legacy: Get available GRES on a node
-spec get_available_gres(node_id()) -> [#gres_device{}].
get_available_gres(NodeId) ->
    case ets:info(?GRES_NODE_TABLE) of
        undefined -> [];
        _ ->
            ets:select(?GRES_NODE_TABLE, [{
                #gres_device{id = {NodeId, '_', '_'}, state = available, _ = '_'},
                [],
                ['$_']
            }])
    end.

%% @doc Legacy: Get GRES by type on a node
-spec get_gres_by_type(node_id(), gres_type()) -> [#gres_device{}].
get_gres_by_type(NodeId, Type) ->
    case ets:info(?GRES_NODE_TABLE) of
        undefined -> [];
        _ ->
            ets:select(?GRES_NODE_TABLE, [{
                #gres_device{id = {NodeId, Type, '_'}, _ = '_'},
                [],
                ['$_']
            }])
    end.

%% @doc Legacy: Parse GRES specification string
-spec parse_gres_spec(binary() | string()) -> {ok, [#gres_spec{}]} | {error, term()}.
parse_gres_spec(Spec) ->
    parse_gres_string(Spec).

%% @doc Legacy: Check if node GRES can satisfy requirements
-spec match_gres_requirements(node_id(), [#gres_spec{}]) -> boolean().
match_gres_requirements(_NodeId, []) ->
    true;
match_gres_requirements(NodeId, Requests) ->
    case check_availability(NodeId, Requests) of
        true -> true;
        _ -> false
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables
    ets:new(?GRES_TYPES_TABLE, [
        named_table, public, set
    ]),
    ets:new(?GRES_NODE_TABLE, [
        named_table, public, set,
        {keypos, #gres_device.id}
    ]),
    ets:new(?GRES_ALLOC_TABLE, [
        named_table, public, bag
    ]),
    ets:new(?GRES_JOB_TABLE, [
        named_table, public, bag
    ]),

    %% Register default GPU type
    ets:insert(?GRES_TYPES_TABLE, {gpu, #{
        count => 0,
        type_specific => #{},
        flags => [exclusive],
        auto_detect => true
    }}),

    {ok, #state{topology_cache = #{}, mig_configs = #{}}}.

handle_call({register_type, Name, Config}, _From, State) ->
    TypeConfig = #{
        count => maps:get(count, Config, 0),
        type_specific => maps:get(type_specific, Config, #{}),
        flags => maps:get(flags, Config, []),
        auto_detect => maps:get(auto_detect, Config, false)
    },
    ets:insert(?GRES_TYPES_TABLE, {Name, TypeConfig}),
    {reply, ok, State};

handle_call({unregister_type, Name}, _From, State) ->
    ets:delete(?GRES_TYPES_TABLE, Name),
    {reply, ok, State};

handle_call({register_node_gres, NodeName, GRESList}, _From, State) ->
    Result = do_register_node_gres(NodeName, GRESList),
    {reply, Result, State};

handle_call({update_node_gres, NodeName, GRESList}, _From, State) ->
    %% First remove existing devices for this node
    Pattern = #gres_device{id = {NodeName, '_', '_'}, _ = '_'},
    ets:select_delete(?GRES_NODE_TABLE, [{Pattern, [], [true]}]),
    %% Then register new ones
    Result = do_register_node_gres(NodeName, GRESList),
    {reply, Result, State};

handle_call({unregister_gres, NodeId, Type}, _From, State) ->
    ets:select_delete(?GRES_NODE_TABLE, [{
        #gres_device{id = {NodeId, Type, '_'}, _ = '_'},
        [],
        [true]
    }]),
    {reply, ok, State};

handle_call({allocate, JobId, NodeName, GRESSpecs}, _From, State) ->
    Result = do_allocate(JobId, NodeName, GRESSpecs),
    {reply, Result, State};

handle_call({deallocate, JobId, NodeName}, _From, State) ->
    do_deallocate(JobId, NodeName),
    {reply, ok, State};

handle_call({check_availability, NodeName, GRESSpecs}, _From, State) ->
    Result = do_check_availability(NodeName, GRESSpecs),
    {reply, Result, State};

handle_call({score_node_gres, NodeName, GRESSpecs}, _From, State) ->
    Score = do_score_node(NodeName, GRESSpecs),
    {reply, Score, State};

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
%% Internal Functions - GRES Registration
%%====================================================================

do_register_node_gres(NodeName, GRESList) ->
    lists:foreach(fun(DeviceMap) ->
        Type = maps:get(type, DeviceMap, gpu),
        Index = maps:get(index, DeviceMap, 0),
        Device = #gres_device{
            id = {NodeName, Type, Index},
            node = NodeName,
            type = Type,
            index = Index,
            name = maps:get(name, DeviceMap, <<>>),
            count = maps:get(count, DeviceMap, 1),
            memory_mb = maps:get(memory_mb, DeviceMap, 0),
            flags = maps:get(flags, DeviceMap, []),
            links = maps:get(links, DeviceMap, []),
            cores = maps:get(cores, DeviceMap, []),
            state = maps:get(state, DeviceMap, available),
            allocated_to = undefined,
            allocation_mode = undefined,
            properties = maps:get(properties, DeviceMap, #{})
        },
        ets:insert(?GRES_NODE_TABLE, Device)
    end, GRESList),
    ok.

%%====================================================================
%% Internal Functions - Allocation
%%====================================================================

do_allocate(JobId, NodeName, GRESSpecs) ->
    %% First check if we can satisfy all requests
    case do_check_availability(NodeName, GRESSpecs) of
        true ->
            %% Perform allocation for each spec
            Results = lists:map(fun(Spec) ->
                allocate_single_spec(JobId, NodeName, Spec)
            end, GRESSpecs),

            %% Check if all succeeded
            case lists:all(fun({ok, _}) -> true; (_) -> false end, Results) of
                true ->
                    AllIndices = lists:flatmap(fun({ok, Indices}) -> Indices end, Results),
                    %% Record job allocation
                    Alloc = #job_gres_alloc{
                        job_id = JobId,
                        node = NodeName,
                        allocations = [{Spec#gres_spec.type, Indices}
                                       || {Spec, {ok, Indices}} <- lists:zip(GRESSpecs, Results)],
                        allocation_mode = determine_allocation_mode(GRESSpecs),
                        allocated_at = erlang:system_time(second)
                    },
                    ets:insert(?GRES_JOB_TABLE, {JobId, Alloc}),
                    {ok, AllIndices};
                false ->
                    %% Rollback any partial allocations
                    do_deallocate(JobId, NodeName),
                    {error, allocation_failed}
            end;
        false ->
            {error, insufficient_gres}
    end.

allocate_single_spec(JobId, NodeName, #gres_spec{type = Type, count = Count,
                                                   name = Name, exclusive = Exclusive} = Spec) ->
    %% Get available devices of this type
    Available = ets:select(?GRES_NODE_TABLE, [{
        #gres_device{id = {NodeName, Type, '_'}, state = available, _ = '_'},
        [],
        ['$_']
    }]),

    %% Filter by name if specified
    Filtered = case Name of
        any -> Available;
        _ -> [D || D <- Available, D#gres_device.name =:= Name orelse
                                   matches_gpu_model(D#gres_device.name, Name)]
    end,

    %% Filter by memory if specified
    FilteredByMem = case Spec#gres_spec.memory_mb of
        any -> Filtered;
        MinMem -> [D || D <- Filtered, D#gres_device.memory_mb >= MinMem]
    end,

    %% Select best devices based on topology
    case select_best_devices(FilteredByMem, Count, Spec) of
        {ok, Selected} ->
            %% Update device states
            Mode = case Exclusive of true -> exclusive; false -> shared end,
            Indices = lists:map(fun(D) ->
                UpdatedDevice = D#gres_device{
                    state = allocated,
                    allocated_to = JobId,
                    allocation_mode = Mode
                },
                ets:insert(?GRES_NODE_TABLE, UpdatedDevice),
                ets:insert(?GRES_ALLOC_TABLE, {JobId, NodeName, D#gres_device.index}),
                D#gres_device.index
            end, Selected),
            {ok, Indices};
        {error, Reason} ->
            {error, Reason}
    end;
allocate_single_spec(JobId, NodeName, Spec) when is_map(Spec) ->
    %% Convert map to gres_spec record
    GresSpec = map_to_gres_spec(Spec),
    allocate_single_spec(JobId, NodeName, GresSpec).

do_deallocate(JobId, NodeName) ->
    %% Find all allocations for this job on this node
    Allocs = ets:select(?GRES_ALLOC_TABLE, [{
        {JobId, NodeName, '$1'},
        [],
        ['$1']
    }]),

    %% Release each device
    lists:foreach(fun(Index) ->
        %% Try to find the device (could be any type)
        Pattern = #gres_device{id = {NodeName, '_', Index}, allocated_to = JobId, _ = '_'},
        case ets:select(?GRES_NODE_TABLE, [{Pattern, [], ['$_']}]) of
            [Device] ->
                UpdatedDevice = Device#gres_device{
                    state = available,
                    allocated_to = undefined,
                    allocation_mode = undefined
                },
                ets:insert(?GRES_NODE_TABLE, UpdatedDevice);
            [] ->
                %% Try all types
                Types = list_types(),
                lists:foreach(fun(Type) ->
                    case ets:lookup(?GRES_NODE_TABLE, {NodeName, Type, Index}) of
                        [D] when D#gres_device.allocated_to =:= JobId ->
                            ets:insert(?GRES_NODE_TABLE, D#gres_device{
                                state = available,
                                allocated_to = undefined,
                                allocation_mode = undefined
                            });
                        _ ->
                            ok
                    end
                end, Types)
        end
    end, Allocs),

    %% Remove allocation records
    ets:select_delete(?GRES_ALLOC_TABLE, [{
        {JobId, NodeName, '_'},
        [],
        [true]
    }]),

    %% Remove job record
    ets:select_delete(?GRES_JOB_TABLE, [{
        {JobId, '_'},
        [],
        [true]
    }]),

    ok.

%%====================================================================
%% Internal Functions - Availability Check
%%====================================================================

do_check_availability(NodeName, GRESSpecs) ->
    %% Get all available devices on the node
    Available = ets:select(?GRES_NODE_TABLE, [{
        #gres_device{id = {NodeName, '_', '_'}, state = available, _ = '_'},
        [],
        ['$_']
    }]),

    %% Check each spec can be satisfied
    lists:all(fun(Spec) ->
        can_satisfy_spec(Spec, Available)
    end, GRESSpecs).

can_satisfy_spec(#gres_spec{type = Type, count = Count, name = Name, memory_mb = MinMem}, Available) ->
    Matching = lists:filter(fun(D) ->
        D#gres_device.type =:= Type andalso
        (Name =:= any orelse D#gres_device.name =:= Name orelse
         matches_gpu_model(D#gres_device.name, Name)) andalso
        (MinMem =:= any orelse D#gres_device.memory_mb >= MinMem)
    end, Available),
    length(Matching) >= Count;
can_satisfy_spec(Spec, Available) when is_map(Spec) ->
    can_satisfy_spec(map_to_gres_spec(Spec), Available).

%%====================================================================
%% Internal Functions - Scoring
%%====================================================================

do_score_node(NodeName, GRESSpecs) ->
    %% Get all devices on the node
    AllDevices = ets:select(?GRES_NODE_TABLE, [{
        #gres_device{id = {NodeName, '_', '_'}, _ = '_'},
        [],
        ['$_']
    }]),

    AvailableDevices = [D || D <- AllDevices, D#gres_device.state =:= available],

    %% Base score: can we satisfy the request?
    case do_check_availability(NodeName, GRESSpecs) of
        false -> 0;
        true ->
            %% Calculate score based on multiple factors
            BaseScore = 50,

            %% Topology score: prefer nodes where requested GPUs have NVLink
            TopologyBonus = calculate_topology_score(AvailableDevices, GRESSpecs),

            %% Fit score: prefer nodes where we use most of the available GRES
            FitBonus = calculate_fit_score(AvailableDevices, GRESSpecs),

            %% Memory score: prefer nodes with matching GPU memory
            MemoryBonus = calculate_memory_score(AvailableDevices, GRESSpecs),

            min(100, BaseScore + TopologyBonus + FitBonus + MemoryBonus)
    end.

calculate_topology_score(Devices, GRESSpecs) ->
    %% Give bonus for nodes where GPUs have NVLink connections
    GPUSpecs = [S || S <- GRESSpecs,
                     is_record(S, gres_spec) andalso S#gres_spec.type =:= gpu],
    case GPUSpecs of
        [] -> 0;
        _ ->
            %% Count devices with topology links
            GPUDevices = [D || D <- Devices, D#gres_device.type =:= gpu],
            LinkedCount = length([D || D <- GPUDevices, length(D#gres_device.links) > 0]),
            case length(GPUDevices) of
                0 -> 0;
                Total -> (LinkedCount * 15) div Total
            end
    end.

calculate_fit_score(Devices, GRESSpecs) ->
    %% Prefer nodes where we use a good portion of available GRES
    TotalAvailable = length(Devices),
    TotalRequested = lists:sum([S#gres_spec.count || S <- GRESSpecs, is_record(S, gres_spec)]),
    case TotalAvailable of
        0 -> 0;
        _ ->
            Utilization = (TotalRequested * 100) div TotalAvailable,
            if
                Utilization >= 80 -> 20;  % Very good fit
                Utilization >= 50 -> 10;  % Reasonable fit
                true -> 5                  % Partial fit
            end
    end.

calculate_memory_score(Devices, GRESSpecs) ->
    %% Give bonus for exact or close memory matches
    MemSpecs = [{S#gres_spec.type, S#gres_spec.memory_mb} || S <- GRESSpecs,
                is_record(S, gres_spec), S#gres_spec.memory_mb =/= any],
    case MemSpecs of
        [] -> 0;
        _ ->
            %% Check if we have devices with matching memory
            lists:sum([begin
                Matching = [D || D <- Devices,
                            D#gres_device.type =:= Type,
                            D#gres_device.memory_mb >= Mem,
                            D#gres_device.memory_mb < Mem * 2],  % Not too much overhead
                if length(Matching) > 0 -> 5; true -> 0 end
            end || {Type, Mem} <- MemSpecs])
    end.

%%====================================================================
%% Internal Functions - Device Selection
%%====================================================================

select_best_devices(Available, Count, _Request) when length(Available) >= Count ->
    %% Sort by topology to prefer connected devices
    Sorted = sort_by_topology(Available),
    {ok, lists:sublist(Sorted, Count)};
select_best_devices(_, _, _) ->
    {error, insufficient_gres}.

%% Sort devices preferring those with NVLink connections to each other
sort_by_topology(Devices) ->
    %% Score each device by connectivity
    Scored = lists:map(fun(D) ->
        %% Count how many of its links are in the available set
        AvailIndices = [Dev#gres_device.index || Dev <- Devices],
        LinkedAvail = length([L || L <- D#gres_device.links, lists:member(L, AvailIndices)]),
        {LinkedAvail, D}
    end, Devices),
    %% Sort descending by connectivity
    SortedScored = lists:reverse(lists:keysort(1, Scored)),
    [D || {_, D} <- SortedScored].

%%====================================================================
%% Internal Functions - Parsing
%%====================================================================

parse_single_gres(Spec) ->
    %% Check for flags at the end (after comma within the spec part)
    {MainPart, Flags} = extract_flags(Spec),

    case binary:split(MainPart, <<":">>, [global]) of
        [Type] ->
            #gres_spec{
                type = parse_gres_type(Type),
                count = 1,
                name = any,
                per_node = 1,
                per_task = 0,
                memory_mb = any,
                flags = Flags,
                exclusive = not lists:member(shared, Flags),
                constraints = []
            };
        [Type, CountOrName] ->
            case parse_count_safe(CountOrName) of
                {ok, Count} ->
                    #gres_spec{
                        type = parse_gres_type(Type),
                        count = Count,
                        name = any,
                        per_node = Count,
                        per_task = 0,
                        memory_mb = any,
                        flags = Flags,
                        exclusive = not lists:member(shared, Flags),
                        constraints = []
                    };
                error ->
                    %% It's a name, not a count, default count to 1
                    #gres_spec{
                        type = parse_gres_type(Type),
                        count = 1,
                        name = CountOrName,
                        per_node = 1,
                        per_task = 0,
                        memory_mb = any,
                        flags = Flags,
                        exclusive = not lists:member(shared, Flags),
                        constraints = []
                    }
            end;
        [Type, Name, Count] ->
            #gres_spec{
                type = parse_gres_type(Type),
                count = parse_count(Count),
                name = Name,
                per_node = parse_count(Count),
                per_task = 0,
                memory_mb = any,
                flags = Flags,
                exclusive = not lists:member(shared, Flags),
                constraints = []
            };
        [Type, Name, Count, Extra | _] ->
            %% Extended format with extra options
            #gres_spec{
                type = parse_gres_type(Type),
                count = parse_count(Count),
                name = Name,
                per_node = parse_count(Count),
                per_task = 0,
                memory_mb = parse_memory_constraint(Extra),
                flags = Flags,
                exclusive = not lists:member(shared, Flags),
                constraints = []
            };
        _ ->
            throw({parse_error, {invalid_gres_spec, Spec}})
    end.

extract_flags(Spec) ->
    %% Look for flag keywords like "shard", "exclusive" in the spec
    Flags = [],
    Flags1 = case binary:match(Spec, <<"shard">>) of
        nomatch -> Flags;
        _ -> [shared | Flags]
    end,
    Flags2 = case binary:match(Spec, <<"exclusive">>) of
        nomatch -> Flags1;
        _ -> [exclusive | Flags1]
    end,
    Flags3 = case binary:match(Spec, <<"mps">>) of
        nomatch -> Flags2;
        _ -> [mps_enabled | Flags2]
    end,
    %% Remove flag keywords from the main part
    CleanSpec = binary:replace(Spec, <<",shard">>, <<>>, [global]),
    CleanSpec2 = binary:replace(CleanSpec, <<",exclusive">>, <<>>, [global]),
    CleanSpec3 = binary:replace(CleanSpec2, <<",mps">>, <<>>, [global]),
    {CleanSpec3, Flags3}.

parse_gres_type(<<"gpu">>) -> gpu;
parse_gres_type(<<"GPU">>) -> gpu;
parse_gres_type(<<"fpga">>) -> fpga;
parse_gres_type(<<"FPGA">>) -> fpga;
parse_gres_type(<<"mic">>) -> mic;
parse_gres_type(<<"MIC">>) -> mic;
parse_gres_type(<<"mps">>) -> mps;
parse_gres_type(<<"MPS">>) -> mps;
parse_gres_type(<<"shard">>) -> shard;
parse_gres_type(Other) -> Other.  % Custom GRES type

parse_count(Binary) ->
    try binary_to_integer(Binary)
    catch _:_ -> throw({parse_error, {invalid_count, Binary}})
    end.

parse_count_safe(Binary) ->
    try
        {ok, binary_to_integer(Binary)}
    catch _:_ ->
        error
    end.

parse_memory_constraint(<<"mem:", MemStr/binary>>) ->
    try binary_to_integer(MemStr)
    catch _:_ -> any
    end;
parse_memory_constraint(_) ->
    any.

format_single_gres(#gres_spec{type = Type, count = Count, name = any, flags = Flags}) ->
    Base = [format_type(Type), <<":">>, integer_to_binary(Count)],
    format_with_flags(Base, Flags);
format_single_gres(#gres_spec{type = Type, count = Count, name = Name, flags = Flags}) ->
    Base = [format_type(Type), <<":">>, Name, <<":">>, integer_to_binary(Count)],
    format_with_flags(Base, Flags).

format_with_flags(Base, Flags) ->
    case Flags of
        [] -> Base;
        _ ->
            FlagStrs = [atom_to_binary(F) || F <- Flags, F =/= exclusive],
            case FlagStrs of
                [] -> Base;
                _ -> [Base, <<",">>, lists:join(<<",">>, FlagStrs)]
            end
    end.

format_type(gpu) -> <<"gpu">>;
format_type(fpga) -> <<"fpga">>;
format_type(mic) -> <<"mic">>;
format_type(mps) -> <<"mps">>;
format_type(shard) -> <<"shard">>;
format_type(Other) when is_binary(Other) -> Other;
format_type(Other) when is_atom(Other) -> atom_to_binary(Other).

%%====================================================================
%% Internal Functions - GPU-Specific
%%====================================================================

%% Check if a device name matches a GPU model pattern
matches_gpu_model(DeviceName, RequestedName) when is_binary(DeviceName), is_binary(RequestedName) ->
    %% Normalize both names for comparison
    NormDevice = string:lowercase(binary_to_list(DeviceName)),
    NormRequest = string:lowercase(binary_to_list(RequestedName)),
    %% Check for substring match or common aliases
    string:find(NormDevice, NormRequest) =/= nomatch orelse
    check_gpu_aliases(NormDevice, NormRequest);
matches_gpu_model(_, _) ->
    false.

check_gpu_aliases(DeviceName, "a100") ->
    string:find(DeviceName, "a100") =/= nomatch;
check_gpu_aliases(DeviceName, "v100") ->
    string:find(DeviceName, "v100") =/= nomatch;
check_gpu_aliases(DeviceName, "h100") ->
    string:find(DeviceName, "h100") =/= nomatch;
check_gpu_aliases(DeviceName, "tesla") ->
    string:find(DeviceName, "tesla") =/= nomatch orelse
    string:find(DeviceName, "v100") =/= nomatch orelse
    string:find(DeviceName, "a100") =/= nomatch;
check_gpu_aliases(DeviceName, "mi250") ->
    string:find(DeviceName, "mi250") =/= nomatch;
check_gpu_aliases(DeviceName, "mi300") ->
    string:find(DeviceName, "mi300") =/= nomatch;
check_gpu_aliases(_, _) ->
    false.

%%====================================================================
%% Internal Functions - Conversion Helpers
%%====================================================================

device_to_map(#gres_device{} = D) ->
    #{
        node => D#gres_device.node,
        type => D#gres_device.type,
        index => D#gres_device.index,
        name => D#gres_device.name,
        count => D#gres_device.count,
        memory_mb => D#gres_device.memory_mb,
        flags => D#gres_device.flags,
        links => D#gres_device.links,
        cores => D#gres_device.cores,
        state => D#gres_device.state,
        allocated_to => D#gres_device.allocated_to,
        allocation_mode => D#gres_device.allocation_mode,
        properties => D#gres_device.properties
    }.

job_alloc_to_map(#job_gres_alloc{} = A) ->
    #{
        job_id => A#job_gres_alloc.job_id,
        node => A#job_gres_alloc.node,
        allocations => A#job_gres_alloc.allocations,
        allocation_mode => A#job_gres_alloc.allocation_mode,
        allocated_at => A#job_gres_alloc.allocated_at
    }.

map_to_gres_spec(Map) when is_map(Map) ->
    #gres_spec{
        type = maps:get(type, Map, gpu),
        name = maps:get(name, Map, any),
        count = maps:get(count, Map, 1),
        per_node = maps:get(per_node, Map, maps:get(count, Map, 1)),
        per_task = maps:get(per_task, Map, 0),
        memory_mb = maps:get(memory_mb, Map, any),
        flags = maps:get(flags, Map, []),
        exclusive = maps:get(exclusive, Map, true),
        constraints = maps:get(constraints, Map, [])
    }.

determine_allocation_mode(GRESSpecs) ->
    %% If any spec requests exclusive, the whole allocation is exclusive
    Exclusive = lists:any(fun
        (#gres_spec{exclusive = E}) -> E;
        (Map) when is_map(Map) -> maps:get(exclusive, Map, true)
    end, GRESSpecs),
    case Exclusive of
        true -> exclusive;
        false -> shared
    end.

%%====================================================================
%% GPU Detection and Auto-Configuration
%%====================================================================

%% @doc Detect GPUs on local system using nvidia-smi or rocm-smi
-spec detect_gpus() -> {ok, [map()]} | {error, term()}.
detect_gpus() ->
    case detect_nvidia_gpus() of
        {ok, Gpus} when Gpus =/= [] -> {ok, Gpus};
        _ ->
            case detect_amd_gpus() of
                {ok, Gpus} when Gpus =/= [] -> {ok, Gpus};
                _ -> {ok, []}  % No GPUs found
            end
    end.

detect_nvidia_gpus() ->
    Cmd = "nvidia-smi --query-gpu=index,name,memory.total,driver_version,cuda_version --format=csv,noheader,nounits 2>/dev/null",
    case os:cmd(Cmd) of
        [] -> {ok, []};
        Output ->
            Lines = string:tokens(Output, "\n"),
            Gpus = lists:filtermap(fun(Line) ->
                case string:tokens(Line, ",") of
                    [IdxStr, Name, MemStr | Rest] ->
                        {DriverVer, CudaVer} = case Rest of
                            [D, C | _] -> {string:trim(D), string:trim(C)};
                            [D] -> {string:trim(D), <<>>};
                            [] -> {<<>>, <<>>}
                        end,
                        {true, #{
                            type => gpu,
                            index => list_to_integer(string:trim(IdxStr)),
                            name => list_to_binary("nvidia_" ++ string:trim(string:lowercase(Name))),
                            memory_mb => list_to_integer(string:trim(MemStr)),
                            flags => [],
                            properties => #{
                                vendor => nvidia,
                                driver_version => list_to_binary(DriverVer),
                                cuda_version => list_to_binary(CudaVer)
                            }
                        }};
                    _ ->
                        false
                end
            end, Lines),
            {ok, Gpus}
    end.

detect_amd_gpus() ->
    Cmd = "rocm-smi --showproductname --showmeminfo vram --csv 2>/dev/null",
    case os:cmd(Cmd) of
        [] -> {ok, []};
        _Output ->
            %% Parse rocm-smi output (format varies by version)
            {ok, []}  % Simplified - would need proper parsing
    end.

%% @doc Check for MIG (Multi-Instance GPU) support
-spec detect_mig_instances(non_neg_integer()) -> {ok, [map()]} | {error, term()}.
detect_mig_instances(GpuIndex) ->
    Cmd = io_lib:format("nvidia-smi mig -i ~p -lgi 2>/dev/null", [GpuIndex]),
    case os:cmd(lists:flatten(Cmd)) of
        [] -> {ok, []};
        "No MIG" ++ _ -> {ok, []};
        Output ->
            %% Parse MIG instance output
            Lines = string:tokens(Output, "\n"),
            Instances = lists:filtermap(fun(Line) ->
                case string:tokens(Line, "|") of
                    [_, IdStr, ProfileStr | _] ->
                        {true, #{
                            instance_id => list_to_integer(string:trim(IdStr)),
                            profile => string:trim(ProfileStr)
                        }};
                    _ ->
                        false
                end
            end, Lines),
            {ok, Instances}
    end.
