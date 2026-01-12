%%%-------------------------------------------------------------------
%%% @doc FLURM Burst Buffer Management
%%%
%%% Manages burst buffers for high-speed temporary storage during job
%%% execution. Compatible with SLURM's burst_buffer plugin interface.
%%%
%%% Supports buffer types:
%%% - Generic (SSD/NVMe local storage)
%%% - DataWarp (Cray DataWarp compatible)
%%% - Lua (scriptable burst buffer)
%%%
%%% Features:
%%% - Persistent burst buffers
%%% - Stage-in/stage-out operations
%%% - Capacity tracking and allocation
%%% - Reservations for upcoming jobs
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_burst_buffer).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API - Buffer Pool Management
-export([
    start_link/0,
    register_buffer/2,
    unregister_buffer/1,
    list_buffers/0,
    get_buffer_status/1
]).

%% API - Job Burst Buffer Operations
-export([
    allocate/2,
    deallocate/1,
    stage_in/2,
    stage_out/2,
    get_job_allocation/1
]).

%% API - Directive Parsing
-export([
    parse_directives/1
]).

%% API - Reservations
-export([
    reserve_capacity/3,
    release_reservation/2,
    get_reservations/1
]).

%% API - Legacy/Additional
-export([
    create_pool/2,
    delete_pool/1,
    list_pools/0,
    get_pool_info/1,
    allocate/3,
    deallocate/2,
    stage_in/3,
    stage_out/3,
    parse_bb_spec/1,
    format_bb_spec/1,
    get_stats/0
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
-define(BB_POOLS_TABLE, flurm_bb_pools).
-define(BB_ALLOCS_TABLE, flurm_bb_allocations).
-define(BB_STAGING_TABLE, flurm_bb_staging).
-define(BB_RESERVATIONS_TABLE, flurm_bb_reservations).
-define(BB_PERSISTENT_TABLE, flurm_bb_persistent).

%% Burst buffer types (SLURM compatible)
-type bb_pool_name() :: binary().
-type bb_type() :: generic | datawarp | lua.
-type bb_state() :: up | down | draining.

%% Buffer pool record
-record(bb_pool, {
    name :: bb_pool_name(),
    type :: bb_type(),
    total_capacity :: non_neg_integer(),     % bytes
    allocated_capacity :: non_neg_integer(), % bytes
    available_capacity :: non_neg_integer(), % bytes
    reserved_capacity :: non_neg_integer(),  % bytes for upcoming jobs
    granularity :: pos_integer(),            % allocation unit in bytes
    nodes :: [binary()],                     % nodes with burst buffer
    state :: bb_state(),
    properties :: map(),
    lua_script :: binary() | undefined,      % for lua type
    create_time :: non_neg_integer(),
    update_time :: non_neg_integer()
}).

%% Job allocation record
-record(bb_allocation, {
    id :: {job_id(), bb_pool_name()},
    job_id :: job_id(),
    pool :: bb_pool_name(),
    size :: non_neg_integer(),
    path :: binary(),                         % mount point
    state :: pending | staging_in | ready | staging_out | complete | error,
    create_time :: non_neg_integer(),
    stage_in_files :: [{binary(), binary()}], % [{src, dest}]
    stage_out_files :: [{binary(), binary()}],
    persistent :: boolean(),                  % survives job completion
    persistent_name :: binary() | undefined,
    error_msg :: binary() | undefined
}).

%% Reservation record
-record(bb_reservation, {
    id :: {job_id(), bb_pool_name()},
    job_id :: job_id(),
    pool :: bb_pool_name(),
    size :: non_neg_integer(),
    create_time :: non_neg_integer(),
    expiry_time :: non_neg_integer() | infinity
}).

%% Persistent buffer record
-record(bb_persistent, {
    name :: binary(),
    pool :: bb_pool_name(),
    size :: non_neg_integer(),
    path :: binary(),
    owner :: binary(),
    create_time :: non_neg_integer(),
    access_list :: [binary()]                 % users with access
}).

%% Burst buffer request specification
-record(bb_request, {
    pool :: bb_pool_name() | any,
    size :: non_neg_integer(),
    access :: striped | private,
    type :: scratch | cache | swap,
    stage_in :: [{binary(), binary()}],       % [{src, dest}]
    stage_out :: [{binary(), binary()}],
    persistent :: boolean(),
    persistent_name :: binary() | undefined
}).

%% Parsed directive record
-record(bb_directive, {
    type :: create_persistent | destroy_persistent | stage_in | stage_out | allocate,
    name :: binary() | undefined,             % for persistent buffers
    capacity :: non_neg_integer(),
    pool :: bb_pool_name() | undefined,
    source :: binary() | undefined,
    destination :: binary() | undefined,
    options :: map()
}).

%% gen_server state
-record(state, {
    stage_workers :: map(),                   % Active staging operations
    cleanup_timer :: reference() | undefined
}).

-export_type([bb_pool_name/0, bb_type/0]).

%%====================================================================
%% API - Buffer Pool Management
%%====================================================================

%% @doc Start the burst buffer gen_server
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register a burst buffer pool
%% Config should contain:
%%   - type: generic | datawarp | lua
%%   - capacity: total capacity (bytes or string like "100GB")
%%   - nodes: list of node names with burst buffer storage
%%   - granularity: (optional) allocation unit, defaults to 1MB
%%   - lua_script: (optional) path to lua script for lua type
-spec register_buffer(bb_pool_name(), map()) -> ok | {error, term()}.
register_buffer(Name, Config) ->
    gen_server:call(?SERVER, {register_buffer, Name, Config}).

%% @doc Remove burst buffer pool
-spec unregister_buffer(bb_pool_name()) -> ok | {error, term()}.
unregister_buffer(Name) ->
    gen_server:call(?SERVER, {unregister_buffer, Name}).

%% @doc List all burst buffer pools with status
-spec list_buffers() -> [map()].
list_buffers() ->
    Pools = ets:tab2list(?BB_POOLS_TABLE),
    [pool_to_map(Pool) || Pool <- Pools].

%% @doc Get detailed status of specific buffer
-spec get_buffer_status(bb_pool_name()) -> {ok, map()} | {error, not_found}.
get_buffer_status(Name) ->
    case ets:lookup(?BB_POOLS_TABLE, Name) of
        [Pool] ->
            %% Get allocations for this pool
            Allocations = ets:select(?BB_ALLOCS_TABLE, [{
                #bb_allocation{pool = Name, _ = '_'},
                [],
                ['$_']
            }]),
            %% Get reservations for this pool
            Reservations = ets:select(?BB_RESERVATIONS_TABLE, [{
                #bb_reservation{pool = Name, _ = '_'},
                [],
                ['$_']
            }]),
            Status = pool_to_map(Pool),
            {ok, Status#{
                allocations => [allocation_to_map(A) || A <- Allocations],
                reservations => [reservation_to_map(R) || R <- Reservations],
                allocation_count => length(Allocations),
                reservation_count => length(Reservations)
            }};
        [] ->
            {error, not_found}
    end.

%%====================================================================
%% API - Job Burst Buffer Operations
%%====================================================================

%% @doc Allocate burst buffer space for job
%% Spec can be:
%%   - map with keys: pool, size, persistent, stage_in, stage_out
%%   - binary specification string (will be parsed)
%%   - #bb_request{} record
-spec allocate(job_id(), map() | binary() | #bb_request{}) ->
    {ok, binary()} | {error, term()}.
allocate(JobId, Spec) when is_binary(Spec) ->
    case parse_bb_spec(Spec) of
        {ok, Request} ->
            allocate(JobId, Request);
        {error, _} = Error ->
            Error
    end;
allocate(JobId, Spec) when is_map(Spec) ->
    Request = map_to_request(Spec),
    allocate(JobId, Request);
allocate(JobId, #bb_request{} = Request) ->
    gen_server:call(?SERVER, {allocate_request, JobId, Request}).

%% @doc Release burst buffer for job
-spec deallocate(job_id()) -> ok | {error, term()}.
deallocate(JobId) ->
    gen_server:call(?SERVER, {deallocate_job, JobId}).

%% @doc Stage data into burst buffer before job starts
%% Files is a list of {source, destination} tuples
-spec stage_in(job_id(), [{binary(), binary()}]) ->
    {ok, reference()} | {error, term()}.
stage_in(JobId, Files) ->
    gen_server:call(?SERVER, {stage_in_job, JobId, Files}).

%% @doc Stage data out of burst buffer after job completes
%% Files is a list of {source, destination} tuples
-spec stage_out(job_id(), [{binary(), binary()}]) ->
    {ok, reference()} | {error, term()}.
stage_out(JobId, Files) ->
    gen_server:call(?SERVER, {stage_out_job, JobId, Files}).

%% @doc Get burst buffer allocation for job
-spec get_job_allocation(job_id()) -> {ok, [map()]} | {error, not_found}.
get_job_allocation(JobId) ->
    Allocations = ets:select(?BB_ALLOCS_TABLE, [{
        #bb_allocation{job_id = JobId, _ = '_'},
        [],
        ['$_']
    }]),
    case Allocations of
        [] -> {error, not_found};
        _ -> {ok, [allocation_to_map(A) || A <- Allocations]}
    end.

%%====================================================================
%% API - Directive Parsing
%%====================================================================

%% @doc Parse burst buffer directives from job script
%% Supports:
%%   #BB create_persistent name=NAME capacity=SIZE [pool=POOL]
%%   #BB destroy_persistent name=NAME
%%   #BB stage_in source=PATH destination=PATH
%%   #BB stage_out source=PATH destination=PATH
%%   #BB capacity=SIZE [pool=POOL] [access=striped|private] [type=scratch|cache]
-spec parse_directives(binary()) -> {ok, [#bb_directive{}]} | {error, term()}.
parse_directives(Script) ->
    Lines = binary:split(Script, <<"\n">>, [global]),
    try
        Directives = lists:filtermap(fun parse_directive_line/1, Lines),
        {ok, Directives}
    catch
        throw:{parse_error, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% API - Reservations
%%====================================================================

%% @doc Reserve capacity for an upcoming job
-spec reserve_capacity(job_id(), bb_pool_name(), non_neg_integer()) ->
    ok | {error, term()}.
reserve_capacity(JobId, PoolName, Size) ->
    gen_server:call(?SERVER, {reserve_capacity, JobId, PoolName, Size}).

%% @doc Release a capacity reservation
-spec release_reservation(job_id(), bb_pool_name()) -> ok.
release_reservation(JobId, PoolName) ->
    gen_server:call(?SERVER, {release_reservation, JobId, PoolName}).

%% @doc Get reservations for a pool
-spec get_reservations(bb_pool_name()) -> [map()].
get_reservations(PoolName) ->
    Reservations = ets:select(?BB_RESERVATIONS_TABLE, [{
        #bb_reservation{pool = PoolName, _ = '_'},
        [],
        ['$_']
    }]),
    [reservation_to_map(R) || R <- Reservations].

%%====================================================================
%% API - Legacy Functions (for backwards compatibility)
%%====================================================================

%% @doc Create a new burst buffer pool (alias for register_buffer)
-spec create_pool(bb_pool_name(), map()) -> ok | {error, term()}.
create_pool(Name, Config) ->
    register_buffer(Name, Config).

%% @doc Delete a burst buffer pool (alias for unregister_buffer)
-spec delete_pool(bb_pool_name()) -> ok | {error, term()}.
delete_pool(Name) ->
    unregister_buffer(Name).

%% @doc List all burst buffer pools
-spec list_pools() -> [#bb_pool{}].
list_pools() ->
    ets:tab2list(?BB_POOLS_TABLE).

%% @doc Get detailed info about a pool
-spec get_pool_info(bb_pool_name()) -> {ok, #bb_pool{}} | {error, not_found}.
get_pool_info(Name) ->
    case ets:lookup(?BB_POOLS_TABLE, Name) of
        [Pool] -> {ok, Pool};
        [] -> {error, not_found}
    end.

%% @doc Allocate burst buffer for a job (3-arg legacy version)
-spec allocate(job_id(), bb_pool_name(), non_neg_integer()) ->
    {ok, binary()} | {error, term()}.
allocate(JobId, PoolName, Size) ->
    gen_server:call(?SERVER, {allocate_legacy, JobId, PoolName, Size}).

%% @doc Deallocate burst buffer for a job (2-arg legacy version)
-spec deallocate(job_id(), bb_pool_name()) -> ok.
deallocate(JobId, PoolName) ->
    gen_server:call(?SERVER, {deallocate_legacy, JobId, PoolName}).

%% @doc Start stage-in operation (3-arg legacy version)
-spec stage_in(job_id(), bb_pool_name(), [{binary(), binary()}]) ->
    {ok, reference()} | {error, term()}.
stage_in(JobId, PoolName, Files) ->
    gen_server:call(?SERVER, {stage_in_legacy, JobId, PoolName, Files}).

%% @doc Start stage-out operation (3-arg legacy version)
-spec stage_out(job_id(), bb_pool_name(), [{binary(), binary()}]) ->
    {ok, reference()} | {error, term()}.
stage_out(JobId, PoolName, Files) ->
    gen_server:call(?SERVER, {stage_out_legacy, JobId, PoolName, Files}).

%% @doc Parse burst buffer specification from job script
-spec parse_bb_spec(binary()) -> {ok, #bb_request{}} | {error, term()}.
parse_bb_spec(Spec) ->
    try
        parse_bb_spec_impl(Spec)
    catch
        throw:{parse_error, Reason} ->
            {error, Reason}
    end.

%% @doc Format burst buffer request as specification
-spec format_bb_spec(#bb_request{}) -> binary().
format_bb_spec(Request) ->
    Parts = [
        format_if_set(pool, Request#bb_request.pool),
        format_if_set(capacity, Request#bb_request.size),
        format_if_set(access, Request#bb_request.access),
        format_if_set(type, Request#bb_request.type)
    ],
    iolist_to_binary(lists:join(<<" ">>, lists:filter(fun(X) -> X =/= <<>> end, Parts))).

%% @doc Get burst buffer statistics
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?SERVER, get_stats).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables
    ets:new(?BB_POOLS_TABLE, [
        named_table, public, set,
        {keypos, #bb_pool.name}
    ]),
    ets:new(?BB_ALLOCS_TABLE, [
        named_table, public, set,
        {keypos, #bb_allocation.id}
    ]),
    ets:new(?BB_STAGING_TABLE, [
        named_table, public, set
    ]),
    ets:new(?BB_RESERVATIONS_TABLE, [
        named_table, public, set,
        {keypos, #bb_reservation.id}
    ]),
    ets:new(?BB_PERSISTENT_TABLE, [
        named_table, public, set,
        {keypos, #bb_persistent.name}
    ]),

    %% Create default pools if configured
    create_default_pools(),

    %% Start cleanup timer
    Timer = erlang:send_after(60000, self(), cleanup_expired),

    {ok, #state{stage_workers = #{}, cleanup_timer = Timer}}.

handle_call({register_buffer, Name, Config}, _From, State) ->
    Result = do_register_buffer(Name, Config),
    {reply, Result, State};

handle_call({unregister_buffer, Name}, _From, State) ->
    Result = do_unregister_buffer(Name),
    {reply, Result, State};

handle_call({allocate_request, JobId, Request}, _From, State) ->
    Result = do_allocate_request(JobId, Request),
    {reply, Result, State};

handle_call({deallocate_job, JobId}, _From, State) ->
    Result = do_deallocate_job(JobId),
    {reply, Result, State};

handle_call({stage_in_job, JobId, Files}, _From, State) ->
    {Result, NewState} = do_stage_in_job(JobId, Files, State),
    {reply, Result, NewState};

handle_call({stage_out_job, JobId, Files}, _From, State) ->
    {Result, NewState} = do_stage_out_job(JobId, Files, State),
    {reply, Result, NewState};

handle_call({reserve_capacity, JobId, PoolName, Size}, _From, State) ->
    Result = do_reserve_capacity(JobId, PoolName, Size),
    {reply, Result, State};

handle_call({release_reservation, JobId, PoolName}, _From, State) ->
    do_release_reservation(JobId, PoolName),
    {reply, ok, State};

%% Legacy call handlers
handle_call({create_pool, Name, Config}, _From, State) ->
    Result = do_register_buffer(Name, Config),
    {reply, Result, State};

handle_call({delete_pool, Name}, _From, State) ->
    Result = do_unregister_buffer(Name),
    {reply, Result, State};

handle_call({allocate_legacy, JobId, PoolName, Size}, _From, State) ->
    Result = do_allocate_legacy(JobId, PoolName, Size),
    {reply, Result, State};

handle_call({deallocate_legacy, JobId, PoolName}, _From, State) ->
    do_deallocate_legacy(JobId, PoolName),
    {reply, ok, State};

handle_call({stage_in_legacy, JobId, PoolName, Files}, _From, State) ->
    {Result, NewState} = do_stage_in_legacy(JobId, PoolName, Files, State),
    {reply, Result, NewState};

handle_call({stage_out_legacy, JobId, PoolName, Files}, _From, State) ->
    {Result, NewState} = do_stage_out_legacy(JobId, PoolName, Files, State),
    {reply, Result, NewState};

handle_call(get_stats, _From, State) ->
    Stats = collect_stats(),
    {reply, Stats, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({staging_complete, Ref, Result}, State) ->
    NewState = handle_staging_complete(Ref, Result, State),
    {noreply, NewState};

handle_info(cleanup_expired, State) ->
    cleanup_expired_reservations(),
    Timer = erlang:send_after(60000, self(), cleanup_expired),
    {noreply, State#state{cleanup_timer = Timer}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{cleanup_timer = Timer}) ->
    case Timer of
        undefined -> ok;
        _ -> erlang:cancel_timer(Timer)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions - Buffer Pool Management
%%====================================================================

create_default_pools() ->
    case application:get_env(flurm_core, burst_buffer_pools, []) of
        [] ->
            %% Create minimal default pool
            DefaultPool = #bb_pool{
                name = <<"default">>,
                type = generic,
                total_capacity = 100 * 1024 * 1024 * 1024,  % 100 GB
                allocated_capacity = 0,
                available_capacity = 100 * 1024 * 1024 * 1024,
                reserved_capacity = 0,
                granularity = 1024 * 1024,  % 1 MB
                nodes = [],
                state = up,
                properties = #{},
                lua_script = undefined,
                create_time = erlang:system_time(second),
                update_time = erlang:system_time(second)
            },
            ets:insert(?BB_POOLS_TABLE, DefaultPool);
        Pools ->
            lists:foreach(fun({Name, Config}) ->
                do_register_buffer(Name, Config)
            end, Pools)
    end.

do_register_buffer(Name, Config) ->
    Now = erlang:system_time(second),
    Type = normalize_type(maps:get(type, Config, generic)),
    TotalCapacity = parse_size(maps:get(capacity, Config, maps:get(size, Config, <<"100GB">>))),

    Pool = #bb_pool{
        name = Name,
        type = Type,
        total_capacity = TotalCapacity,
        allocated_capacity = 0,
        available_capacity = TotalCapacity,
        reserved_capacity = 0,
        granularity = parse_size(maps:get(granularity, Config, <<"1MB">>)),
        nodes = maps:get(nodes, Config, []),
        state = up,
        properties = maps:get(properties, Config, #{}),
        lua_script = case Type of
            lua -> maps:get(lua_script, Config, undefined);
            _ -> undefined
        end,
        create_time = Now,
        update_time = Now
    },
    ets:insert(?BB_POOLS_TABLE, Pool),
    ok.

do_unregister_buffer(Name) ->
    %% Check for active allocations
    case ets:select(?BB_ALLOCS_TABLE, [{
        #bb_allocation{pool = Name, _ = '_'},
        [],
        ['$_']
    }]) of
        [] ->
            %% Check for reservations
            case ets:select(?BB_RESERVATIONS_TABLE, [{
                #bb_reservation{pool = Name, _ = '_'},
                [],
                ['$_']
            }]) of
                [] ->
                    ets:delete(?BB_POOLS_TABLE, Name),
                    ok;
                _ ->
                    {error, has_reservations}
            end;
        _ ->
            {error, pool_in_use}
    end.

normalize_type(generic) -> generic;
normalize_type(datawarp) -> datawarp;
normalize_type(lua) -> lua;
normalize_type(<<"generic">>) -> generic;
normalize_type(<<"datawarp">>) -> datawarp;
normalize_type(<<"lua">>) -> lua;
normalize_type("generic") -> generic;
normalize_type("datawarp") -> datawarp;
normalize_type("lua") -> lua;
normalize_type(_) -> generic.

%%====================================================================
%% Internal Functions - Allocation
%%====================================================================

do_allocate_request(JobId, #bb_request{pool = PoolSpec, size = Size} = Request) ->
    %% Find appropriate pool
    PoolName = case PoolSpec of
        any -> find_available_pool(Size);
        Name when is_binary(Name) -> Name
    end,

    case PoolName of
        {error, _} = Error -> Error;
        _ ->
            case ets:lookup(?BB_POOLS_TABLE, PoolName) of
                [Pool] ->
                    %% Check if we have a reservation that can be converted
                    ReservedSize = check_reservation(JobId, PoolName),
                    EffectiveAvailable = Pool#bb_pool.available_capacity + ReservedSize,

                    case EffectiveAvailable >= Size of
                        true ->
                            AllocSize = round_to_granularity(Size, Pool#bb_pool.granularity),
                            Path = generate_bb_path(JobId, PoolName),

                            Allocation = #bb_allocation{
                                id = {JobId, PoolName},
                                job_id = JobId,
                                pool = PoolName,
                                size = AllocSize,
                                path = Path,
                                state = pending,
                                create_time = erlang:system_time(second),
                                stage_in_files = Request#bb_request.stage_in,
                                stage_out_files = Request#bb_request.stage_out,
                                persistent = Request#bb_request.persistent,
                                persistent_name = Request#bb_request.persistent_name,
                                error_msg = undefined
                            },
                            ets:insert(?BB_ALLOCS_TABLE, Allocation),

                            %% Update pool capacity
                            NewAllocated = Pool#bb_pool.allocated_capacity + AllocSize,
                            NewAvailable = Pool#bb_pool.total_capacity - NewAllocated -
                                          (Pool#bb_pool.reserved_capacity - ReservedSize),
                            NewReserved = Pool#bb_pool.reserved_capacity - ReservedSize,

                            ets:insert(?BB_POOLS_TABLE, Pool#bb_pool{
                                allocated_capacity = NewAllocated,
                                available_capacity = NewAvailable,
                                reserved_capacity = NewReserved,
                                update_time = erlang:system_time(second)
                            }),

                            %% Release reservation if used
                            case ReservedSize > 0 of
                                true -> ets:delete(?BB_RESERVATIONS_TABLE, {JobId, PoolName});
                                false -> ok
                            end,

                            %% Handle stage-in if specified
                            case Request#bb_request.stage_in of
                                [] ->
                                    update_allocation_state({JobId, PoolName}, ready);
                                _ ->
                                    update_allocation_state({JobId, PoolName}, staging_in)
                            end,

                            {ok, Path};
                        false ->
                            {error, insufficient_space}
                    end;
                [] ->
                    {error, pool_not_found}
            end
    end.

do_allocate_legacy(JobId, PoolName, Size) ->
    Request = #bb_request{
        pool = PoolName,
        size = Size,
        access = striped,
        type = scratch,
        stage_in = [],
        stage_out = [],
        persistent = false,
        persistent_name = undefined
    },
    do_allocate_request(JobId, Request).

do_deallocate_job(JobId) ->
    %% Find all allocations for this job
    Allocations = ets:select(?BB_ALLOCS_TABLE, [{
        #bb_allocation{job_id = JobId, _ = '_'},
        [],
        ['$_']
    }]),

    case Allocations of
        [] ->
            {error, not_found};
        _ ->
            lists:foreach(fun(Alloc) ->
                do_deallocate_single(Alloc)
            end, Allocations),
            ok
    end.

do_deallocate_legacy(JobId, PoolName) ->
    case ets:lookup(?BB_ALLOCS_TABLE, {JobId, PoolName}) of
        [Alloc] ->
            do_deallocate_single(Alloc);
        [] ->
            ok
    end.

do_deallocate_single(#bb_allocation{persistent = true}) ->
    %% Don't deallocate persistent buffers
    ok;
do_deallocate_single(#bb_allocation{id = Id, pool = PoolName, size = Size}) ->
    %% Return space to pool
    case ets:lookup(?BB_POOLS_TABLE, PoolName) of
        [Pool] ->
            NewAllocated = max(0, Pool#bb_pool.allocated_capacity - Size),
            NewAvailable = Pool#bb_pool.total_capacity - NewAllocated - Pool#bb_pool.reserved_capacity,
            ets:insert(?BB_POOLS_TABLE, Pool#bb_pool{
                allocated_capacity = NewAllocated,
                available_capacity = NewAvailable,
                update_time = erlang:system_time(second)
            });
        [] ->
            ok
    end,
    ets:delete(?BB_ALLOCS_TABLE, Id).

find_available_pool(Size) ->
    Pools = ets:tab2list(?BB_POOLS_TABLE),
    AvailablePools = [P || P <- Pools,
                          P#bb_pool.state =:= up,
                          P#bb_pool.available_capacity >= Size],
    case AvailablePools of
        [] -> {error, no_available_pool};
        [First | _] -> First#bb_pool.name
    end.

check_reservation(JobId, PoolName) ->
    case ets:lookup(?BB_RESERVATIONS_TABLE, {JobId, PoolName}) of
        [#bb_reservation{size = Size}] -> Size;
        [] -> 0
    end.

update_allocation_state(Id, NewState) ->
    case ets:lookup(?BB_ALLOCS_TABLE, Id) of
        [Alloc] ->
            ets:insert(?BB_ALLOCS_TABLE, Alloc#bb_allocation{state = NewState});
        [] ->
            ok
    end.

%%====================================================================
%% Internal Functions - Staging
%%====================================================================

do_stage_in_job(JobId, Files, State) ->
    %% Find allocation for job (use first one if multiple)
    case ets:select(?BB_ALLOCS_TABLE, [{
        #bb_allocation{job_id = JobId, _ = '_'},
        [],
        ['$_']
    }]) of
        [Alloc | _] ->
            do_stage_operation(in, JobId, Alloc#bb_allocation.pool, Alloc#bb_allocation.path, Files, State);
        [] ->
            {{error, allocation_not_found}, State}
    end.

do_stage_out_job(JobId, Files, State) ->
    case ets:select(?BB_ALLOCS_TABLE, [{
        #bb_allocation{job_id = JobId, _ = '_'},
        [],
        ['$_']
    }]) of
        [Alloc | _] ->
            do_stage_operation(out, JobId, Alloc#bb_allocation.pool, Alloc#bb_allocation.path, Files, State);
        [] ->
            {{error, allocation_not_found}, State}
    end.

do_stage_in_legacy(JobId, PoolName, Files, State) ->
    case ets:lookup(?BB_ALLOCS_TABLE, {JobId, PoolName}) of
        [Alloc] ->
            do_stage_operation(in, JobId, PoolName, Alloc#bb_allocation.path, Files, State);
        [] ->
            {{error, allocation_not_found}, State}
    end.

do_stage_out_legacy(JobId, PoolName, Files, State) ->
    case ets:lookup(?BB_ALLOCS_TABLE, {JobId, PoolName}) of
        [Alloc] ->
            do_stage_operation(out, JobId, PoolName, Alloc#bb_allocation.path, Files, State);
        [] ->
            {{error, allocation_not_found}, State}
    end.

do_stage_operation(Direction, JobId, PoolName, BasePath, Files, State) ->
    Id = {JobId, PoolName},
    case ets:lookup(?BB_ALLOCS_TABLE, Id) of
        [Alloc] ->
            NewState = case Direction of
                in -> staging_in;
                out -> staging_out
            end,

            UpdatedFiles = case Direction of
                in -> Alloc#bb_allocation{
                    state = NewState,
                    stage_in_files = Files
                };
                out -> Alloc#bb_allocation{
                    state = NewState,
                    stage_out_files = Files
                }
            end,
            ets:insert(?BB_ALLOCS_TABLE, UpdatedFiles),

            %% Start async staging worker
            Ref = make_ref(),
            Self = self(),
            spawn_link(fun() ->
                Result = execute_staging(Direction, BasePath, Files),
                Self ! {staging_complete, Ref, Result}
            end),

            NewWorkers = maps:put(Ref, {JobId, PoolName, Direction}, State#state.stage_workers),
            {{ok, Ref}, State#state{stage_workers = NewWorkers}};
        [] ->
            {{error, allocation_not_found}, State}
    end.

handle_staging_complete(Ref, Result, State) ->
    case maps:get(Ref, State#state.stage_workers, undefined) of
        {JobId, PoolName, Direction} ->
            Id = {JobId, PoolName},
            case ets:lookup(?BB_ALLOCS_TABLE, Id) of
                [Alloc] ->
                    NewState = case {Direction, Result} of
                        {in, ok} -> ready;
                        {out, ok} -> complete;
                        {_, {error, Reason}} ->
                            ets:insert(?BB_ALLOCS_TABLE, Alloc#bb_allocation{
                                state = error,
                                error_msg = iolist_to_binary(io_lib:format("~p", [Reason]))
                            }),
                            error
                    end,
                    case NewState of
                        error -> ok;
                        _ -> ets:insert(?BB_ALLOCS_TABLE, Alloc#bb_allocation{state = NewState})
                    end;
                [] ->
                    ok
            end,
            State#state{stage_workers = maps:remove(Ref, State#state.stage_workers)};
        undefined ->
            State
    end.

execute_staging(Direction, BasePath, Files) ->
    try
        lists:foreach(fun({Src, Dest}) ->
            {FullSrc, FullDest} = case Direction of
                in -> {Src, filename:join(BasePath, Dest)};
                out -> {filename:join(BasePath, Src), Dest}
            end,
            %% In real implementation, this would use efficient file copy
            %% or distributed file system operations
            ok = filelib:ensure_dir(FullDest),
            case file:copy(FullSrc, FullDest) of
                {ok, _} -> ok;
                {error, Reason} -> throw({copy_failed, Reason, FullSrc, FullDest})
            end
        end, Files),
        ok
    catch
        _:Reason ->
            {error, Reason}
    end.

%%====================================================================
%% Internal Functions - Reservations
%%====================================================================

do_reserve_capacity(JobId, PoolName, Size) ->
    case ets:lookup(?BB_POOLS_TABLE, PoolName) of
        [Pool] ->
            case Pool#bb_pool.available_capacity >= Size of
                true ->
                    Now = erlang:system_time(second),
                    Reservation = #bb_reservation{
                        id = {JobId, PoolName},
                        job_id = JobId,
                        pool = PoolName,
                        size = Size,
                        create_time = Now,
                        expiry_time = Now + 3600  % 1 hour default expiry
                    },
                    ets:insert(?BB_RESERVATIONS_TABLE, Reservation),

                    %% Update pool capacity
                    NewAvailable = Pool#bb_pool.available_capacity - Size,
                    NewReserved = Pool#bb_pool.reserved_capacity + Size,
                    ets:insert(?BB_POOLS_TABLE, Pool#bb_pool{
                        available_capacity = NewAvailable,
                        reserved_capacity = NewReserved,
                        update_time = Now
                    }),
                    ok;
                false ->
                    {error, insufficient_space}
            end;
        [] ->
            {error, pool_not_found}
    end.

do_release_reservation(JobId, PoolName) ->
    case ets:lookup(?BB_RESERVATIONS_TABLE, {JobId, PoolName}) of
        [#bb_reservation{size = Size}] ->
            ets:delete(?BB_RESERVATIONS_TABLE, {JobId, PoolName}),

            %% Return capacity to pool
            case ets:lookup(?BB_POOLS_TABLE, PoolName) of
                [Pool] ->
                    NewAvailable = Pool#bb_pool.available_capacity + Size,
                    NewReserved = max(0, Pool#bb_pool.reserved_capacity - Size),
                    ets:insert(?BB_POOLS_TABLE, Pool#bb_pool{
                        available_capacity = NewAvailable,
                        reserved_capacity = NewReserved,
                        update_time = erlang:system_time(second)
                    });
                [] ->
                    ok
            end;
        [] ->
            ok
    end.

cleanup_expired_reservations() ->
    Now = erlang:system_time(second),
    Expired = ets:select(?BB_RESERVATIONS_TABLE, [{
        #bb_reservation{expiry_time = '$1', _ = '_'},
        [{'<', '$1', Now}],
        ['$_']
    }]),
    lists:foreach(fun(#bb_reservation{job_id = JobId, pool = PoolName}) ->
        do_release_reservation(JobId, PoolName)
    end, Expired).

%%====================================================================
%% Internal Functions - Directive Parsing
%%====================================================================

parse_directive_line(Line) ->
    TrimmedLine = string:trim(binary_to_list(Line)),
    case TrimmedLine of
        "#BB " ++ Rest ->
            {true, parse_directive_content(list_to_binary(Rest))};
        "#BB" ++ Rest when Rest =:= "" orelse hd(Rest) =:= $\s ->
            CleanRest = string:trim(Rest),
            case CleanRest of
                "" -> false;
                _ -> {true, parse_directive_content(list_to_binary(CleanRest))}
            end;
        _ ->
            false
    end.

parse_directive_content(Content) ->
    Parts = binary:split(Content, <<" ">>, [global, trim_all]),
    case Parts of
        [<<"create_persistent">> | Rest] ->
            parse_create_persistent(Rest);
        [<<"destroy_persistent">> | Rest] ->
            parse_destroy_persistent(Rest);
        [<<"stage_in">> | Rest] ->
            parse_stage_directive(stage_in, Rest);
        [<<"stage_out">> | Rest] ->
            parse_stage_directive(stage_out, Rest);
        _ ->
            parse_allocate_directive(Parts)
    end.

parse_create_persistent(Parts) ->
    Opts = parse_directive_opts(Parts),
    #bb_directive{
        type = create_persistent,
        name = maps:get(name, Opts, undefined),
        capacity = parse_size(maps:get(capacity, Opts, <<"0">>)),
        pool = maps:get(pool, Opts, undefined),
        source = undefined,
        destination = undefined,
        options = Opts
    }.

parse_destroy_persistent(Parts) ->
    Opts = parse_directive_opts(Parts),
    #bb_directive{
        type = destroy_persistent,
        name = maps:get(name, Opts, undefined),
        capacity = 0,
        pool = undefined,
        source = undefined,
        destination = undefined,
        options = Opts
    }.

parse_stage_directive(Type, Parts) ->
    Opts = parse_directive_opts(Parts),
    DirectiveType = case Type of
        stage_in -> stage_in;
        stage_out -> stage_out
    end,
    #bb_directive{
        type = DirectiveType,
        name = undefined,
        capacity = 0,
        pool = maps:get(pool, Opts, undefined),
        source = maps:get(source, Opts, undefined),
        destination = maps:get(destination, Opts, undefined),
        options = Opts
    }.

parse_allocate_directive(Parts) ->
    Opts = parse_directive_opts(Parts),
    #bb_directive{
        type = allocate,
        name = maps:get(name, Opts, undefined),
        capacity = parse_size(maps:get(capacity, Opts, maps:get(size, Opts, <<"0">>))),
        pool = maps:get(pool, Opts, undefined),
        source = undefined,
        destination = undefined,
        options = Opts
    }.

parse_directive_opts(Parts) ->
    lists:foldl(fun(Part, Acc) ->
        case binary:split(Part, <<"=">>) of
            [Key, Value] ->
                maps:put(binary_to_atom(Key, utf8), Value, Acc);
            [Key] ->
                maps:put(binary_to_atom(Key, utf8), true, Acc)
        end
    end, #{}, Parts).

%%====================================================================
%% Internal Functions - Parsing
%%====================================================================

parse_bb_spec_impl(Spec) ->
    %% Remove #BB prefix if present
    CleanSpec = case Spec of
        <<"#BB ", Rest/binary>> -> Rest;
        <<"#BB", Rest/binary>> -> Rest;
        _ -> Spec
    end,

    Parts = binary:split(CleanSpec, <<" ">>, [global, trim_all]),
    Request = lists:foldl(fun parse_bb_part/2, #bb_request{
        pool = any,
        size = 0,
        access = striped,
        type = scratch,
        stage_in = [],
        stage_out = [],
        persistent = false,
        persistent_name = undefined
    }, Parts),

    {ok, Request}.

parse_bb_part(Part, Request) ->
    case binary:split(Part, <<"=">>) of
        [<<"pool">>, Value] ->
            Request#bb_request{pool = Value};
        [<<"capacity">>, Value] ->
            Request#bb_request{size = parse_size(Value)};
        [<<"size">>, Value] ->
            Request#bb_request{size = parse_size(Value)};
        [<<"access">>, <<"striped">>] ->
            Request#bb_request{access = striped};
        [<<"access">>, <<"private">>] ->
            Request#bb_request{access = private};
        [<<"type">>, <<"scratch">>] ->
            Request#bb_request{type = scratch};
        [<<"type">>, <<"cache">>] ->
            Request#bb_request{type = cache};
        [<<"type">>, <<"swap">>] ->
            Request#bb_request{type = swap};
        [<<"persistent">>] ->
            Request#bb_request{persistent = true};
        [<<"name">>, Value] ->
            Request#bb_request{persistent_name = Value};
        _ ->
            Request  % Ignore unknown parts
    end.

map_to_request(Map) ->
    #bb_request{
        pool = maps:get(pool, Map, any),
        size = parse_size(maps:get(size, Map, maps:get(capacity, Map, 0))),
        access = maps:get(access, Map, striped),
        type = maps:get(type, Map, scratch),
        stage_in = maps:get(stage_in, Map, []),
        stage_out = maps:get(stage_out, Map, []),
        persistent = maps:get(persistent, Map, false),
        persistent_name = maps:get(persistent_name, Map, undefined)
    }.

%%====================================================================
%% Internal Functions - Utilities
%%====================================================================

generate_bb_path(JobId, PoolName) ->
    iolist_to_binary([
        <<"/bb/">>,
        PoolName,
        <<"/">>,
        integer_to_binary(JobId)
    ]).

round_to_granularity(Size, Granularity) ->
    ((Size + Granularity - 1) div Granularity) * Granularity.

parse_size(Size) when is_integer(Size) ->
    Size;
parse_size(Size) when is_binary(Size) ->
    parse_size(binary_to_list(Size));
parse_size(Size) when is_list(Size) ->
    %% Parse sizes like "100GB", "1TB", "500MB"
    case re:run(Size, "^([0-9]+)([KMGT]?B?)$", [{capture, all_but_first, list}, caseless]) of
        {match, [NumStr, Unit]} ->
            Num = list_to_integer(NumStr),
            Multiplier = case string:uppercase(Unit) of
                "" -> 1;
                "B" -> 1;
                "KB" -> 1024;
                "K" -> 1024;
                "MB" -> 1024 * 1024;
                "M" -> 1024 * 1024;
                "GB" -> 1024 * 1024 * 1024;
                "G" -> 1024 * 1024 * 1024;
                "TB" -> 1024 * 1024 * 1024 * 1024;
                "T" -> 1024 * 1024 * 1024 * 1024
            end,
            Num * Multiplier;
        nomatch ->
            0
    end.

format_if_set(_, any) -> <<>>;
format_if_set(_, undefined) -> <<>>;
format_if_set(_, 0) -> <<>>;
format_if_set(pool, Value) -> <<"pool=", Value/binary>>;
format_if_set(capacity, Value) -> <<"capacity=", (format_size(Value))/binary>>;
format_if_set(access, Value) -> <<"access=", (atom_to_binary(Value))/binary>>;
format_if_set(type, Value) -> <<"type=", (atom_to_binary(Value))/binary>>.

format_size(Bytes) when Bytes >= 1024 * 1024 * 1024 * 1024 ->
    iolist_to_binary([integer_to_binary(Bytes div (1024 * 1024 * 1024 * 1024)), <<"TB">>]);
format_size(Bytes) when Bytes >= 1024 * 1024 * 1024 ->
    iolist_to_binary([integer_to_binary(Bytes div (1024 * 1024 * 1024)), <<"GB">>]);
format_size(Bytes) when Bytes >= 1024 * 1024 ->
    iolist_to_binary([integer_to_binary(Bytes div (1024 * 1024)), <<"MB">>]);
format_size(Bytes) when Bytes >= 1024 ->
    iolist_to_binary([integer_to_binary(Bytes div 1024), <<"KB">>]);
format_size(Bytes) ->
    iolist_to_binary([integer_to_binary(Bytes), <<"B">>]).

%%====================================================================
%% Internal Functions - Statistics and Conversion
%%====================================================================

collect_stats() ->
    Pools = ets:tab2list(?BB_POOLS_TABLE),
    Allocs = ets:tab2list(?BB_ALLOCS_TABLE),
    Reservations = ets:tab2list(?BB_RESERVATIONS_TABLE),

    TotalCapacity = lists:sum([P#bb_pool.total_capacity || P <- Pools]),
    AllocatedCapacity = lists:sum([P#bb_pool.allocated_capacity || P <- Pools]),
    AvailableCapacity = lists:sum([P#bb_pool.available_capacity || P <- Pools]),
    ReservedCapacity = lists:sum([P#bb_pool.reserved_capacity || P <- Pools]),

    %% Count allocations by state
    StateCount = lists:foldl(fun(#bb_allocation{state = S}, Acc) ->
        maps:update_with(S, fun(V) -> V + 1 end, 1, Acc)
    end, #{}, Allocs),

    #{
        pool_count => length(Pools),
        total_capacity => TotalCapacity,
        allocated_capacity => AllocatedCapacity,
        available_capacity => AvailableCapacity,
        reserved_capacity => ReservedCapacity,
        allocation_count => length(Allocs),
        reservation_count => length(Reservations),
        allocations_by_state => StateCount,
        utilization => case TotalCapacity of
            0 -> 0.0;
            _ -> AllocatedCapacity / TotalCapacity
        end,
        pools => [#{
            name => P#bb_pool.name,
            type => P#bb_pool.type,
            state => P#bb_pool.state,
            total => P#bb_pool.total_capacity,
            allocated => P#bb_pool.allocated_capacity,
            available => P#bb_pool.available_capacity,
            reserved => P#bb_pool.reserved_capacity
        } || P <- Pools]
    }.

pool_to_map(#bb_pool{} = P) ->
    #{
        name => P#bb_pool.name,
        type => P#bb_pool.type,
        state => P#bb_pool.state,
        total_capacity => P#bb_pool.total_capacity,
        allocated_capacity => P#bb_pool.allocated_capacity,
        available_capacity => P#bb_pool.available_capacity,
        reserved_capacity => P#bb_pool.reserved_capacity,
        granularity => P#bb_pool.granularity,
        nodes => P#bb_pool.nodes,
        properties => P#bb_pool.properties,
        lua_script => P#bb_pool.lua_script,
        create_time => P#bb_pool.create_time,
        update_time => P#bb_pool.update_time,
        utilization => case P#bb_pool.total_capacity of
            0 -> 0.0;
            Total -> P#bb_pool.allocated_capacity / Total
        end
    }.

allocation_to_map(#bb_allocation{} = A) ->
    #{
        job_id => A#bb_allocation.job_id,
        pool => A#bb_allocation.pool,
        size => A#bb_allocation.size,
        path => A#bb_allocation.path,
        state => A#bb_allocation.state,
        create_time => A#bb_allocation.create_time,
        stage_in_files => A#bb_allocation.stage_in_files,
        stage_out_files => A#bb_allocation.stage_out_files,
        persistent => A#bb_allocation.persistent,
        persistent_name => A#bb_allocation.persistent_name,
        error_msg => A#bb_allocation.error_msg
    }.

reservation_to_map(#bb_reservation{} = R) ->
    #{
        job_id => R#bb_reservation.job_id,
        pool => R#bb_reservation.pool,
        size => R#bb_reservation.size,
        create_time => R#bb_reservation.create_time,
        expiry_time => R#bb_reservation.expiry_time
    }.
