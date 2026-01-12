%%%-------------------------------------------------------------------
%%% @doc FLURM Burst Buffer Management
%%%
%%% Manages burst buffers for high-speed temporary storage during job
%%% execution. Compatible with SLURM's burst_buffer plugin interface.
%%%
%%% Supports:
%%% - DataWarp (Cray)
%%% - Generic burst buffer pools
%%% - Persistent burst buffers
%%% - Stage-in/stage-out operations
%%% - Capacity tracking and allocation
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_burst_buffer).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    create_pool/2,
    delete_pool/1,
    list_pools/0,
    get_pool_info/1,
    allocate/3,
    deallocate/2,
    stage_in/3,
    stage_out/3,
    get_job_allocation/1,
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

%% Burst buffer types
-type bb_pool_name() :: binary().
-type bb_type() :: generic | datawarp | cray | nvme | ssd.

-record(bb_pool, {
    name :: bb_pool_name(),
    type :: bb_type(),
    total_size :: non_neg_integer(),     % bytes
    free_size :: non_neg_integer(),      % bytes
    granularity :: pos_integer(),        % allocation unit in bytes
    nodes :: [binary()],                  % nodes with burst buffer
    state :: up | down | draining,
    properties :: map()
}).

-record(bb_allocation, {
    id :: {job_id(), bb_pool_name()},
    job_id :: job_id(),
    pool :: bb_pool_name(),
    size :: non_neg_integer(),
    path :: binary(),                     % mount point
    state :: pending | staging_in | ready | staging_out | complete,
    create_time :: non_neg_integer(),
    stage_in_files :: [binary()],
    stage_out_files :: [binary()],
    persistent :: boolean()               % survives job completion
}).

-record(bb_request, {
    pool :: bb_pool_name() | any,
    size :: non_neg_integer(),
    access :: striped | private,
    type :: scratch | cache | swap,
    stage_in :: [{binary(), binary()}],   % [{src, dest}]
    stage_out :: [{binary(), binary()}],
    persistent :: boolean(),
    persistent_name :: binary() | undefined
}).

-record(state, {
    stage_workers :: map()                % Active staging operations
}).

-export_type([bb_pool_name/0, bb_type/0]).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Create a new burst buffer pool
-spec create_pool(bb_pool_name(), map()) -> ok | {error, term()}.
create_pool(Name, Config) ->
    gen_server:call(?SERVER, {create_pool, Name, Config}).

%% @doc Delete a burst buffer pool
-spec delete_pool(bb_pool_name()) -> ok | {error, term()}.
delete_pool(Name) ->
    gen_server:call(?SERVER, {delete_pool, Name}).

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

%% @doc Allocate burst buffer for a job
-spec allocate(job_id(), bb_pool_name(), non_neg_integer()) ->
    {ok, binary()} | {error, term()}.
allocate(JobId, PoolName, Size) ->
    gen_server:call(?SERVER, {allocate, JobId, PoolName, Size}).

%% @doc Deallocate burst buffer for a job
-spec deallocate(job_id(), bb_pool_name()) -> ok.
deallocate(JobId, PoolName) ->
    gen_server:call(?SERVER, {deallocate, JobId, PoolName}).

%% @doc Start stage-in operation
-spec stage_in(job_id(), bb_pool_name(), [{binary(), binary()}]) ->
    {ok, reference()} | {error, term()}.
stage_in(JobId, PoolName, Files) ->
    gen_server:call(?SERVER, {stage_in, JobId, PoolName, Files}).

%% @doc Start stage-out operation
-spec stage_out(job_id(), bb_pool_name(), [{binary(), binary()}]) ->
    {ok, reference()} | {error, term()}.
stage_out(JobId, PoolName, Files) ->
    gen_server:call(?SERVER, {stage_out, JobId, PoolName, Files}).

%% @doc Get allocation for a job
-spec get_job_allocation(job_id()) -> [#bb_allocation{}].
get_job_allocation(JobId) ->
    ets:select(?BB_ALLOCS_TABLE, [{
        #bb_allocation{job_id = JobId, _ = '_'},
        [],
        ['$_']
    }]).

%% @doc Parse burst buffer specification from job script
%% Format: #BB pool=name capacity=100GB access=striped type=scratch
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

    %% Create default pool if configured
    create_default_pools(),

    {ok, #state{stage_workers = #{}}}.

handle_call({create_pool, Name, Config}, _From, State) ->
    Result = do_create_pool(Name, Config),
    {reply, Result, State};

handle_call({delete_pool, Name}, _From, State) ->
    Result = do_delete_pool(Name),
    {reply, Result, State};

handle_call({allocate, JobId, PoolName, Size}, _From, State) ->
    Result = do_allocate(JobId, PoolName, Size),
    {reply, Result, State};

handle_call({deallocate, JobId, PoolName}, _From, State) ->
    do_deallocate(JobId, PoolName),
    {reply, ok, State};

handle_call({stage_in, JobId, PoolName, Files}, _From, State) ->
    {Result, NewState} = do_stage_in(JobId, PoolName, Files, State),
    {reply, Result, NewState};

handle_call({stage_out, JobId, PoolName, Files}, _From, State) ->
    {Result, NewState} = do_stage_out(JobId, PoolName, Files, State),
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

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

create_default_pools() ->
    %% Create a default generic pool if configured
    case application:get_env(flurm_core, burst_buffer_pools, []) of
        [] ->
            %% Create minimal default pool
            DefaultPool = #bb_pool{
                name = <<"default">>,
                type = generic,
                total_size = 100 * 1024 * 1024 * 1024,  % 100 GB
                free_size = 100 * 1024 * 1024 * 1024,
                granularity = 1024 * 1024,  % 1 MB
                nodes = [],
                state = up,
                properties = #{}
            },
            ets:insert(?BB_POOLS_TABLE, DefaultPool);
        Pools ->
            lists:foreach(fun({Name, Config}) ->
                do_create_pool(Name, Config)
            end, Pools)
    end.

do_create_pool(Name, Config) ->
    Pool = #bb_pool{
        name = Name,
        type = maps:get(type, Config, generic),
        total_size = parse_size(maps:get(size, Config, <<"100GB">>)),
        free_size = parse_size(maps:get(size, Config, <<"100GB">>)),
        granularity = parse_size(maps:get(granularity, Config, <<"1MB">>)),
        nodes = maps:get(nodes, Config, []),
        state = up,
        properties = maps:get(properties, Config, #{})
    },
    ets:insert(?BB_POOLS_TABLE, Pool),
    ok.

do_delete_pool(Name) ->
    %% Check for active allocations
    case ets:select(?BB_ALLOCS_TABLE, [{
        #bb_allocation{pool = Name, _ = '_'},
        [],
        ['$_']
    }]) of
        [] ->
            ets:delete(?BB_POOLS_TABLE, Name),
            ok;
        _ ->
            {error, pool_in_use}
    end.

do_allocate(JobId, PoolName, Size) ->
    case ets:lookup(?BB_POOLS_TABLE, PoolName) of
        [Pool] ->
            case Pool#bb_pool.free_size >= Size of
                true ->
                    %% Round up to granularity
                    AllocSize = round_to_granularity(Size, Pool#bb_pool.granularity),
                    Path = generate_bb_path(JobId, PoolName),

                    Allocation = #bb_allocation{
                        id = {JobId, PoolName},
                        job_id = JobId,
                        pool = PoolName,
                        size = AllocSize,
                        path = Path,
                        state = ready,
                        create_time = erlang:system_time(second),
                        stage_in_files = [],
                        stage_out_files = [],
                        persistent = false
                    },
                    ets:insert(?BB_ALLOCS_TABLE, Allocation),

                    %% Update pool free space
                    ets:insert(?BB_POOLS_TABLE, Pool#bb_pool{
                        free_size = Pool#bb_pool.free_size - AllocSize
                    }),

                    {ok, Path};
                false ->
                    {error, insufficient_space}
            end;
        [] ->
            {error, pool_not_found}
    end.

do_deallocate(JobId, PoolName) ->
    case ets:lookup(?BB_ALLOCS_TABLE, {JobId, PoolName}) of
        [Alloc] ->
            %% Return space to pool
            case ets:lookup(?BB_POOLS_TABLE, PoolName) of
                [Pool] ->
                    ets:insert(?BB_POOLS_TABLE, Pool#bb_pool{
                        free_size = Pool#bb_pool.free_size + Alloc#bb_allocation.size
                    });
                [] ->
                    ok
            end,
            ets:delete(?BB_ALLOCS_TABLE, {JobId, PoolName});
        [] ->
            ok
    end.

do_stage_in(JobId, PoolName, Files, State) ->
    case ets:lookup(?BB_ALLOCS_TABLE, {JobId, PoolName}) of
        [Alloc] ->
            %% Update allocation state
            ets:insert(?BB_ALLOCS_TABLE, Alloc#bb_allocation{
                state = staging_in,
                stage_in_files = [Src || {Src, _Dest} <- Files]
            }),

            %% Start async staging worker
            Ref = make_ref(),
            spawn_link(fun() ->
                Result = execute_staging(in, Alloc#bb_allocation.path, Files),
                ?SERVER ! {staging_complete, Ref, Result}
            end),

            NewWorkers = maps:put(Ref, {JobId, PoolName, in}, State#state.stage_workers),
            {{ok, Ref}, State#state{stage_workers = NewWorkers}};
        [] ->
            {{error, allocation_not_found}, State}
    end.

do_stage_out(JobId, PoolName, Files, State) ->
    case ets:lookup(?BB_ALLOCS_TABLE, {JobId, PoolName}) of
        [Alloc] ->
            ets:insert(?BB_ALLOCS_TABLE, Alloc#bb_allocation{
                state = staging_out,
                stage_out_files = [Dest || {_Src, Dest} <- Files]
            }),

            Ref = make_ref(),
            spawn_link(fun() ->
                Result = execute_staging(out, Alloc#bb_allocation.path, Files),
                ?SERVER ! {staging_complete, Ref, Result}
            end),

            NewWorkers = maps:put(Ref, {JobId, PoolName, out}, State#state.stage_workers),
            {{ok, Ref}, State#state{stage_workers = NewWorkers}};
        [] ->
            {{error, allocation_not_found}, State}
    end.

handle_staging_complete(Ref, Result, State) ->
    case maps:get(Ref, State#state.stage_workers, undefined) of
        {JobId, PoolName, Direction} ->
            case ets:lookup(?BB_ALLOCS_TABLE, {JobId, PoolName}) of
                [Alloc] ->
                    NewState = case {Direction, Result} of
                        {in, ok} ->
                            ets:insert(?BB_ALLOCS_TABLE, Alloc#bb_allocation{state = ready});
                        {out, ok} ->
                            ets:insert(?BB_ALLOCS_TABLE, Alloc#bb_allocation{state = complete});
                        {_, {error, _}} ->
                            %% Log error but don't change state
                            ok
                    end,
                    NewState;
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
            FullDest = case Direction of
                in -> filename:join(BasePath, Dest);
                out -> Dest
            end,
            FullSrc = case Direction of
                in -> Src;
                out -> filename:join(BasePath, Src)
            end,
            %% In real implementation, this would use efficient file copy
            %% or distributed file system operations
            ok = filelib:ensure_dir(FullDest),
            {ok, _} = file:copy(FullSrc, FullDest)
        end, Files),
        ok
    catch
        _:Reason ->
            {error, Reason}
    end.

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
    case re:run(Size, "^([0-9]+)([KMGT]?B?)$", [{capture, all_but_first, list}]) of
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

collect_stats() ->
    Pools = ets:tab2list(?BB_POOLS_TABLE),
    Allocs = ets:tab2list(?BB_ALLOCS_TABLE),

    TotalSize = lists:sum([P#bb_pool.total_size || P <- Pools]),
    FreeSize = lists:sum([P#bb_pool.free_size || P <- Pools]),
    AllocCount = length(Allocs),
    AllocSize = lists:sum([A#bb_allocation.size || A <- Allocs]),

    #{
        pool_count => length(Pools),
        total_size => TotalSize,
        free_size => FreeSize,
        used_size => TotalSize - FreeSize,
        allocation_count => AllocCount,
        allocated_size => AllocSize,
        utilization => case TotalSize of
            0 -> 0.0;
            _ -> (TotalSize - FreeSize) / TotalSize
        end
    }.

%%====================================================================
%% Parsing Functions
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
