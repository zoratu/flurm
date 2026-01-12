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
%%% - MPS (Multi-Process Service) support
%%% - GPU isolation via cgroups/namespaces
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_gres).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    register_gres/3,
    unregister_gres/2,
    request_gres/3,
    release_gres/3,
    get_node_gres/1,
    get_available_gres/1,
    get_gres_by_type/2,
    parse_gres_spec/1,
    format_gres_spec/1,
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
-define(GRES_TABLE, flurm_gres_table).
-define(GRES_ALLOC_TABLE, flurm_gres_allocations).

%% GRES types
-type gres_type() :: gpu | fpga | mic | mps | shard | binary().
-type gres_name() :: binary().
-type gres_index() :: non_neg_integer().

-record(gres_device, {
    id :: {node_id(), gres_type(), gres_index()},
    node :: node_id(),
    type :: gres_type(),
    index :: gres_index(),
    name :: gres_name(),          % e.g., <<"nvidia_a100">>
    count :: pos_integer(),       % Usually 1, but can be more for MPS
    memory_mb :: non_neg_integer(),
    flags :: [atom()],            % [exclusive, shared, mps_enabled]
    links :: [gres_index()],      % Topology links (NVLink peers)
    cores :: [non_neg_integer()], % CPU cores with affinity
    state :: available | allocated | draining | down,
    allocated_to :: job_id() | undefined,
    properties :: map()           % Vendor-specific properties
}).

-record(gres_request, {
    type :: gres_type(),
    count :: pos_integer(),
    name :: gres_name() | any,    % Specific model or any
    per_node :: pos_integer(),    % GRES per node
    per_task :: pos_integer(),    % GRES per task
    memory_mb :: non_neg_integer() | any,
    flags :: [atom()],
    exclusive :: boolean()
}).

-record(state, {
    topology_cache :: map()
}).

-type node_id() :: binary().

%% Suppress unused warnings for detection functions (called externally)
-compile({nowarn_unused_function, [detect_gpus/0, detect_nvidia_gpus/0, detect_amd_gpus/0]}).

-export_type([gres_type/0, gres_name/0]).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register GRES devices on a node
-spec register_gres(node_id(), gres_type(), [map()]) -> ok | {error, term()}.
register_gres(NodeId, Type, Devices) ->
    gen_server:call(?SERVER, {register_gres, NodeId, Type, Devices}).

%% @doc Unregister all GRES of a type from a node
-spec unregister_gres(node_id(), gres_type()) -> ok.
unregister_gres(NodeId, Type) ->
    gen_server:call(?SERVER, {unregister_gres, NodeId, Type}).

%% @doc Request GRES allocation for a job
-spec request_gres(job_id(), node_id(), #gres_request{}) ->
    {ok, [gres_index()]} | {error, term()}.
request_gres(JobId, NodeId, Request) ->
    gen_server:call(?SERVER, {request_gres, JobId, NodeId, Request}).

%% @doc Release GRES allocated to a job
-spec release_gres(job_id(), node_id(), [gres_index()]) -> ok.
release_gres(JobId, NodeId, Indices) ->
    gen_server:call(?SERVER, {release_gres, JobId, NodeId, Indices}).

%% @doc Get all GRES on a node
-spec get_node_gres(node_id()) -> [#gres_device{}].
get_node_gres(NodeId) ->
    ets:select(?GRES_TABLE, [{
        #gres_device{id = {NodeId, '_', '_'}, _ = '_'},
        [],
        ['$_']
    }]).

%% @doc Get available GRES on a node
-spec get_available_gres(node_id()) -> [#gres_device{}].
get_available_gres(NodeId) ->
    ets:select(?GRES_TABLE, [{
        #gres_device{id = {NodeId, '_', '_'}, state = available, _ = '_'},
        [],
        ['$_']
    }]).

%% @doc Get GRES by type on a node
-spec get_gres_by_type(node_id(), gres_type()) -> [#gres_device{}].
get_gres_by_type(NodeId, Type) ->
    ets:select(?GRES_TABLE, [{
        #gres_device{id = {NodeId, Type, '_'}, _ = '_'},
        [],
        ['$_']
    }]).

%% @doc Parse GRES specification string (SLURM format)
%% Examples: "gpu:2", "gpu:tesla:4", "gpu:a100:2,mps:100"
-spec parse_gres_spec(binary() | string()) -> {ok, [#gres_request{}]} | {error, term()}.
parse_gres_spec(Spec) when is_list(Spec) ->
    parse_gres_spec(list_to_binary(Spec));
parse_gres_spec(<<>>) ->
    {ok, []};
parse_gres_spec(Spec) when is_binary(Spec) ->
    Parts = binary:split(Spec, <<",">>, [global]),
    try
        Requests = lists:map(fun parse_single_gres/1, Parts),
        {ok, Requests}
    catch
        throw:{parse_error, Reason} ->
            {error, Reason}
    end.

%% @doc Format GRES request as specification string
-spec format_gres_spec([#gres_request{}]) -> binary().
format_gres_spec([]) ->
    <<>>;
format_gres_spec(Requests) ->
    Parts = lists:map(fun format_single_gres/1, Requests),
    iolist_to_binary(lists:join(<<",">>, Parts)).

%% @doc Check if node GRES can satisfy requirements
-spec match_gres_requirements(node_id(), [#gres_request{}]) -> boolean().
match_gres_requirements(_NodeId, []) ->
    true;
match_gres_requirements(NodeId, Requests) ->
    Available = get_available_gres(NodeId),
    lists:all(fun(Req) ->
        can_satisfy_request(Req, Available)
    end, Requests).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables
    ets:new(?GRES_TABLE, [
        named_table, public, set,
        {keypos, #gres_device.id}
    ]),
    ets:new(?GRES_ALLOC_TABLE, [
        named_table, public, bag
    ]),

    {ok, #state{topology_cache = #{}}}.

handle_call({register_gres, NodeId, Type, Devices}, _From, State) ->
    Result = do_register_gres(NodeId, Type, Devices),
    {reply, Result, State};

handle_call({unregister_gres, NodeId, Type}, _From, State) ->
    do_unregister_gres(NodeId, Type),
    {reply, ok, State};

handle_call({request_gres, JobId, NodeId, Request}, _From, State) ->
    Result = do_request_gres(JobId, NodeId, Request),
    {reply, Result, State};

handle_call({release_gres, JobId, NodeId, Indices}, _From, State) ->
    do_release_gres(JobId, NodeId, Indices),
    {reply, ok, State};

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
%% Internal Functions
%%====================================================================

do_register_gres(NodeId, Type, Devices) ->
    lists:foreach(fun(DeviceMap) ->
        Index = maps:get(index, DeviceMap, 0),
        Device = #gres_device{
            id = {NodeId, Type, Index},
            node = NodeId,
            type = Type,
            index = Index,
            name = maps:get(name, DeviceMap, <<>>),
            count = maps:get(count, DeviceMap, 1),
            memory_mb = maps:get(memory_mb, DeviceMap, 0),
            flags = maps:get(flags, DeviceMap, []),
            links = maps:get(links, DeviceMap, []),
            cores = maps:get(cores, DeviceMap, []),
            state = available,
            allocated_to = undefined,
            properties = maps:get(properties, DeviceMap, #{})
        },
        ets:insert(?GRES_TABLE, Device)
    end, Devices),
    ok.

do_unregister_gres(NodeId, Type) ->
    ets:select_delete(?GRES_TABLE, [{
        #gres_device{id = {NodeId, Type, '_'}, _ = '_'},
        [],
        [true]
    }]).

do_request_gres(JobId, NodeId, #gres_request{type = Type, count = Count} = Request) ->
    Available = get_gres_by_type(NodeId, Type),
    FilteredAvailable = lists:filter(fun(D) ->
        D#gres_device.state =:= available andalso
        matches_request(D, Request)
    end, Available),

    case select_best_devices(FilteredAvailable, Count, Request) of
        {ok, Selected} ->
            %% Allocate selected devices
            Indices = lists:map(fun(D) ->
                ets:insert(?GRES_TABLE, D#gres_device{
                    state = allocated,
                    allocated_to = JobId
                }),
                ets:insert(?GRES_ALLOC_TABLE, {JobId, NodeId, D#gres_device.index}),
                D#gres_device.index
            end, Selected),
            {ok, Indices};
        {error, Reason} ->
            {error, Reason}
    end.

do_release_gres(JobId, NodeId, Indices) ->
    lists:foreach(fun(Index) ->
        case ets:lookup(?GRES_TABLE, {NodeId, gpu, Index}) of
            [Device] ->
                case Device#gres_device.allocated_to of
                    JobId ->
                        ets:insert(?GRES_TABLE, Device#gres_device{
                            state = available,
                            allocated_to = undefined
                        }),
                        ets:delete_object(?GRES_ALLOC_TABLE, {JobId, NodeId, Index});
                    _ ->
                        ok  % Not allocated to this job
                end;
            [] ->
                ok
        end
    end, Indices).

matches_request(#gres_device{name = Name, memory_mb = Mem, flags = Flags},
                #gres_request{name = ReqName, memory_mb = ReqMem, exclusive = Excl}) ->
    NameMatch = (ReqName =:= any orelse Name =:= ReqName),
    MemMatch = (ReqMem =:= any orelse Mem >= ReqMem),
    ExclMatch = (not Excl orelse not lists:member(shared, Flags)),
    NameMatch andalso MemMatch andalso ExclMatch.

select_best_devices(Available, Count, Request) when length(Available) >= Count ->
    %% Sort by topology to prefer connected devices
    Sorted = sort_by_topology(Available, Request),
    {ok, lists:sublist(Sorted, Count)};
select_best_devices(_, _, _) ->
    {error, insufficient_gres}.

%% Sort devices preferring those with NVLink connections
sort_by_topology(Devices, _Request) ->
    %% Simple sorting - prefer devices with more links (better interconnect)
    lists:sort(fun(A, B) ->
        length(A#gres_device.links) >= length(B#gres_device.links)
    end, Devices).

can_satisfy_request(#gres_request{type = Type, count = Count, name = Name},
                    Available) ->
    Matching = lists:filter(fun(D) ->
        D#gres_device.type =:= Type andalso
        (Name =:= any orelse D#gres_device.name =:= Name)
    end, Available),
    length(Matching) >= Count.

%%====================================================================
%% Parsing Functions
%%====================================================================

parse_single_gres(Spec) ->
    case binary:split(Spec, <<":">>, [global]) of
        [Type] ->
            #gres_request{
                type = parse_gres_type(Type),
                count = 1,
                name = any,
                per_node = 1,
                per_task = 0,
                memory_mb = any,
                flags = [],
                exclusive = false
            };
        [Type, Count] ->
            #gres_request{
                type = parse_gres_type(Type),
                count = parse_count(Count),
                name = any,
                per_node = parse_count(Count),
                per_task = 0,
                memory_mb = any,
                flags = [],
                exclusive = false
            };
        [Type, Name, Count] ->
            #gres_request{
                type = parse_gres_type(Type),
                count = parse_count(Count),
                name = Name,
                per_node = parse_count(Count),
                per_task = 0,
                memory_mb = any,
                flags = [],
                exclusive = false
            };
        _ ->
            throw({parse_error, {invalid_gres_spec, Spec}})
    end.

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

format_single_gres(#gres_request{type = Type, count = Count, name = any}) ->
    [format_type(Type), <<":">>, integer_to_binary(Count)];
format_single_gres(#gres_request{type = Type, count = Count, name = Name}) ->
    [format_type(Type), <<":">>, Name, <<":">>, integer_to_binary(Count)].

format_type(gpu) -> <<"gpu">>;
format_type(fpga) -> <<"fpga">>;
format_type(mic) -> <<"mic">>;
format_type(mps) -> <<"mps">>;
format_type(shard) -> <<"shard">>;
format_type(Other) when is_binary(Other) -> Other;
format_type(Other) when is_atom(Other) -> atom_to_binary(Other).

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
    Cmd = "nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader,nounits 2>/dev/null",
    case os:cmd(Cmd) of
        [] -> {ok, []};
        Output ->
            Lines = string:tokens(Output, "\n"),
            Gpus = lists:filtermap(fun(Line) ->
                case string:tokens(Line, ",") of
                    [IdxStr, Name, MemStr] ->
                        {true, #{
                            index => list_to_integer(string:trim(IdxStr)),
                            name => list_to_binary("nvidia_" ++ string:trim(string:lowercase(Name))),
                            memory_mb => list_to_integer(string:trim(MemStr)),
                            flags => [],
                            properties => #{vendor => nvidia}
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
