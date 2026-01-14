%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_burst_buffer module
%%%
%%% Tests all exported functions directly without any mocking.
%%% Tests gen_server callbacks directly for coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_burst_buffer_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% ETS table names (must match module)
-define(BB_POOLS_TABLE, flurm_bb_pools).
-define(BB_ALLOCS_TABLE, flurm_bb_allocations).
-define(BB_STAGING_TABLE, flurm_bb_staging).
-define(BB_RESERVATIONS_TABLE, flurm_bb_reservations).
-define(BB_PERSISTENT_TABLE, flurm_bb_persistent).

%% Burst buffer types
-type bb_pool_name() :: binary().
-type bb_type() :: generic | datawarp | lua.
-type bb_state() :: up | down | draining.

%% Buffer pool record (must match module)
-record(bb_pool, {
    name :: bb_pool_name(),
    type :: bb_type(),
    total_capacity :: non_neg_integer(),
    allocated_capacity :: non_neg_integer(),
    available_capacity :: non_neg_integer(),
    reserved_capacity :: non_neg_integer(),
    granularity :: pos_integer(),
    nodes :: [binary()],
    state :: bb_state(),
    properties :: map(),
    lua_script :: binary() | undefined,
    create_time :: non_neg_integer(),
    update_time :: non_neg_integer()
}).

%% Job allocation record
-record(bb_allocation, {
    id :: {pos_integer(), bb_pool_name()},
    job_id :: pos_integer(),
    pool :: bb_pool_name(),
    size :: non_neg_integer(),
    path :: binary(),
    state :: pending | staging_in | ready | staging_out | complete | error,
    create_time :: non_neg_integer(),
    stage_in_files :: [{binary(), binary()}],
    stage_out_files :: [{binary(), binary()}],
    persistent :: boolean(),
    persistent_name :: binary() | undefined,
    error_msg :: binary() | undefined
}).

%% Reservation record
-record(bb_reservation, {
    id :: {pos_integer(), bb_pool_name()},
    job_id :: pos_integer(),
    pool :: bb_pool_name(),
    size :: non_neg_integer(),
    create_time :: non_neg_integer(),
    expiry_time :: non_neg_integer() | infinity
}).

%% Burst buffer request specification
-record(bb_request, {
    pool :: bb_pool_name() | any,
    size :: non_neg_integer(),
    access :: striped | private,
    type :: scratch | cache | swap,
    stage_in :: [{binary(), binary()}],
    stage_out :: [{binary(), binary()}],
    persistent :: boolean(),
    persistent_name :: binary() | undefined
}).

%% Parsed directive record
-record(bb_directive, {
    type :: create_persistent | destroy_persistent | stage_in | stage_out | allocate,
    name :: binary() | undefined,
    capacity :: non_neg_integer(),
    pool :: bb_pool_name() | undefined,
    source :: binary() | undefined,
    destination :: binary() | undefined,
    options :: map()
}).

%% gen_server state record
-record(state, {
    stage_workers :: map(),
    cleanup_timer :: reference() | undefined
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup helper that creates ETS tables manually (without calling init)
setup_ets_tables() ->
    %% Delete tables if they exist
    catch ets:delete(?BB_POOLS_TABLE),
    catch ets:delete(?BB_ALLOCS_TABLE),
    catch ets:delete(?BB_STAGING_TABLE),
    catch ets:delete(?BB_RESERVATIONS_TABLE),
    catch ets:delete(?BB_PERSISTENT_TABLE),
    %% Create tables
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
        named_table, public, set
    ]),
    ok.

cleanup_ets_tables() ->
    catch ets:delete(?BB_POOLS_TABLE),
    catch ets:delete(?BB_ALLOCS_TABLE),
    catch ets:delete(?BB_STAGING_TABLE),
    catch ets:delete(?BB_RESERVATIONS_TABLE),
    catch ets:delete(?BB_PERSISTENT_TABLE),
    ok.

%% Insert a test pool directly into ETS
insert_test_pool(Name, TotalCapacity) ->
    insert_test_pool(Name, TotalCapacity, 0, TotalCapacity, 0).

insert_test_pool(Name, TotalCapacity, Allocated, Available, Reserved) ->
    Now = erlang:system_time(second),
    Pool = #bb_pool{
        name = Name,
        type = generic,
        total_capacity = TotalCapacity,
        allocated_capacity = Allocated,
        available_capacity = Available,
        reserved_capacity = Reserved,
        granularity = 1024 * 1024,  % 1 MB
        nodes = [],
        state = up,
        properties = #{},
        lua_script = undefined,
        create_time = Now,
        update_time = Now
    },
    ets:insert(?BB_POOLS_TABLE, Pool).

insert_test_pool_full(Name, Type, TotalCapacity, Allocated, Available, Reserved, State, LuaScript) ->
    Now = erlang:system_time(second),
    Pool = #bb_pool{
        name = Name,
        type = Type,
        total_capacity = TotalCapacity,
        allocated_capacity = Allocated,
        available_capacity = Available,
        reserved_capacity = Reserved,
        granularity = 1024 * 1024,
        nodes = [<<"node1">>, <<"node2">>],
        state = State,
        properties = #{feature => <<"fast">>},
        lua_script = LuaScript,
        create_time = Now,
        update_time = Now
    },
    ets:insert(?BB_POOLS_TABLE, Pool).

%% Insert a test allocation directly into ETS
insert_test_allocation(JobId, PoolName, Size) ->
    insert_test_allocation(JobId, PoolName, Size, ready).

insert_test_allocation(JobId, PoolName, Size, AllocState) ->
    Alloc = #bb_allocation{
        id = {JobId, PoolName},
        job_id = JobId,
        pool = PoolName,
        size = Size,
        path = <<"/bb/", PoolName/binary, "/", (integer_to_binary(JobId))/binary>>,
        state = AllocState,
        create_time = erlang:system_time(second),
        stage_in_files = [],
        stage_out_files = [],
        persistent = false,
        persistent_name = undefined,
        error_msg = undefined
    },
    ets:insert(?BB_ALLOCS_TABLE, Alloc).

%% Insert a test reservation directly into ETS
insert_test_reservation(JobId, PoolName, Size) ->
    insert_test_reservation(JobId, PoolName, Size, infinity).

insert_test_reservation(JobId, PoolName, Size, ExpiryTime) ->
    Now = erlang:system_time(second),
    Reservation = #bb_reservation{
        id = {JobId, PoolName},
        job_id = JobId,
        pool = PoolName,
        size = Size,
        create_time = Now,
        expiry_time = ExpiryTime
    },
    ets:insert(?BB_RESERVATIONS_TABLE, Reservation).

%%====================================================================
%% parse_bb_spec Tests (Pure Function)
%%====================================================================

parse_bb_spec_test_() ->
    [
        {"parse empty spec",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<>>),
             ?assertEqual(any, Req#bb_request.pool),
             ?assertEqual(0, Req#bb_request.size)
         end},

        {"parse capacity spec",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"capacity=100GB">>),
             ?assertEqual(100 * 1024 * 1024 * 1024, Req#bb_request.size)
         end},

        {"parse size spec",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"size=50MB">>),
             ?assertEqual(50 * 1024 * 1024, Req#bb_request.size)
         end},

        {"parse pool spec",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"pool=nvme">>),
             ?assertEqual(<<"nvme">>, Req#bb_request.pool)
         end},

        {"parse access striped",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"access=striped">>),
             ?assertEqual(striped, Req#bb_request.access)
         end},

        {"parse access private",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"access=private">>),
             ?assertEqual(private, Req#bb_request.access)
         end},

        {"parse type scratch",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"type=scratch">>),
             ?assertEqual(scratch, Req#bb_request.type)
         end},

        {"parse type cache",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"type=cache">>),
             ?assertEqual(cache, Req#bb_request.type)
         end},

        {"parse type swap",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"type=swap">>),
             ?assertEqual(swap, Req#bb_request.type)
         end},

        {"parse persistent flag",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"persistent">>),
             ?assertEqual(true, Req#bb_request.persistent)
         end},

        {"parse persistent name",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"name=mydata">>),
             ?assertEqual(<<"mydata">>, Req#bb_request.persistent_name)
         end},

        {"parse combined spec",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(
                 <<"pool=fast capacity=1TB access=private type=cache persistent name=work">>),
             ?assertEqual(<<"fast">>, Req#bb_request.pool),
             ?assertEqual(1024 * 1024 * 1024 * 1024, Req#bb_request.size),
             ?assertEqual(private, Req#bb_request.access),
             ?assertEqual(cache, Req#bb_request.type),
             ?assertEqual(true, Req#bb_request.persistent),
             ?assertEqual(<<"work">>, Req#bb_request.persistent_name)
         end},

        {"parse with #BB prefix",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"#BB capacity=10GB">>),
             ?assertEqual(10 * 1024 * 1024 * 1024, Req#bb_request.size)
         end},

        {"parse with #BB space prefix",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"#BB pool=ssd">>),
             ?assertEqual(<<"ssd">>, Req#bb_request.pool)
         end},

        {"parse unknown parts ignored",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"unknown=value capacity=5GB">>),
             ?assertEqual(5 * 1024 * 1024 * 1024, Req#bb_request.size)
         end}
    ].

%%====================================================================
%% format_bb_spec Tests (Pure Function)
%%====================================================================

format_bb_spec_test_() ->
    [
        {"format request with default values",
         fun() ->
             Req = #bb_request{
                 pool = any,
                 size = 0,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             Result = flurm_burst_buffer:format_bb_spec(Req),
             %% Default access and type are included in the output
             ?assert(is_binary(Result)),
             ?assert(binary:match(Result, <<"access=striped">>) =/= nomatch),
             ?assert(binary:match(Result, <<"type=scratch">>) =/= nomatch)
         end},

        {"format with pool",
         fun() ->
             Req = #bb_request{
                 pool = <<"nvme">>,
                 size = 0,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             Result = flurm_burst_buffer:format_bb_spec(Req),
             ?assert(binary:match(Result, <<"pool=nvme">>) =/= nomatch)
         end},

        {"format with capacity",
         fun() ->
             Req = #bb_request{
                 pool = any,
                 size = 1024 * 1024 * 1024,  % 1GB
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             Result = flurm_burst_buffer:format_bb_spec(Req),
             ?assert(binary:match(Result, <<"capacity=1GB">>) =/= nomatch)
         end},

        {"format with access",
         fun() ->
             Req = #bb_request{
                 pool = any,
                 size = 0,
                 access = private,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             Result = flurm_burst_buffer:format_bb_spec(Req),
             ?assert(binary:match(Result, <<"access=private">>) =/= nomatch)
         end},

        {"format full request",
         fun() ->
             Req = #bb_request{
                 pool = <<"fast">>,
                 size = 10 * 1024 * 1024 * 1024,  % 10GB
                 access = private,
                 type = cache,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             Result = flurm_burst_buffer:format_bb_spec(Req),
             ?assert(binary:match(Result, <<"pool=fast">>) =/= nomatch),
             ?assert(binary:match(Result, <<"capacity=10GB">>) =/= nomatch),
             ?assert(binary:match(Result, <<"access=private">>) =/= nomatch),
             ?assert(binary:match(Result, <<"type=cache">>) =/= nomatch)
         end}
    ].

%%====================================================================
%% parse_directives Tests (Pure Function)
%%====================================================================

parse_directives_test_() ->
    [
        {"parse empty script",
         fun() ->
             {ok, Directives} = flurm_burst_buffer:parse_directives(<<>>),
             ?assertEqual([], Directives)
         end},

        {"parse script with no BB directives",
         fun() ->
             Script = <<"#!/bin/bash\necho hello\n">>,
             {ok, Directives} = flurm_burst_buffer:parse_directives(Script),
             ?assertEqual([], Directives)
         end},

        {"parse create_persistent directive",
         fun() ->
             Script = <<"#BB create_persistent name=mydata capacity=100GB">>,
             {ok, [Dir]} = flurm_burst_buffer:parse_directives(Script),
             ?assertEqual(create_persistent, Dir#bb_directive.type),
             ?assertEqual(<<"mydata">>, Dir#bb_directive.name),
             ?assertEqual(100 * 1024 * 1024 * 1024, Dir#bb_directive.capacity)
         end},

        {"parse destroy_persistent directive",
         fun() ->
             Script = <<"#BB destroy_persistent name=olddata">>,
             {ok, [Dir]} = flurm_burst_buffer:parse_directives(Script),
             ?assertEqual(destroy_persistent, Dir#bb_directive.type),
             ?assertEqual(<<"olddata">>, Dir#bb_directive.name)
         end},

        {"parse stage_in directive",
         fun() ->
             Script = <<"#BB stage_in source=/data/input.dat destination=input.dat">>,
             {ok, [Dir]} = flurm_burst_buffer:parse_directives(Script),
             ?assertEqual(stage_in, Dir#bb_directive.type),
             ?assertEqual(<<"/data/input.dat">>, Dir#bb_directive.source),
             ?assertEqual(<<"input.dat">>, Dir#bb_directive.destination)
         end},

        {"parse stage_out directive",
         fun() ->
             Script = <<"#BB stage_out source=output.dat destination=/results/output.dat">>,
             {ok, [Dir]} = flurm_burst_buffer:parse_directives(Script),
             ?assertEqual(stage_out, Dir#bb_directive.type),
             ?assertEqual(<<"output.dat">>, Dir#bb_directive.source),
             ?assertEqual(<<"/results/output.dat">>, Dir#bb_directive.destination)
         end},

        {"parse allocate directive",
         fun() ->
             Script = <<"#BB capacity=50GB pool=nvme">>,
             {ok, [Dir]} = flurm_burst_buffer:parse_directives(Script),
             ?assertEqual(allocate, Dir#bb_directive.type),
             ?assertEqual(50 * 1024 * 1024 * 1024, Dir#bb_directive.capacity),
             ?assertEqual(<<"nvme">>, Dir#bb_directive.pool)
         end},

        {"parse multiple directives",
         fun() ->
             Script = <<"#!/bin/bash\n",
                        "#BB capacity=10GB\n",
                        "#BB stage_in source=/input destination=input\n",
                        "srun ./app\n",
                        "#BB stage_out source=output destination=/output\n">>,
             {ok, Directives} = flurm_burst_buffer:parse_directives(Script),
             ?assertEqual(3, length(Directives)),
             [D1, D2, D3] = Directives,
             ?assertEqual(allocate, D1#bb_directive.type),
             ?assertEqual(stage_in, D2#bb_directive.type),
             ?assertEqual(stage_out, D3#bb_directive.type)
         end},

        {"parse directive with pool option",
         fun() ->
             Script = <<"#BB create_persistent name=data capacity=5GB pool=fast">>,
             {ok, [Dir]} = flurm_burst_buffer:parse_directives(Script),
             ?assertEqual(<<"fast">>, Dir#bb_directive.pool)
         end}
    ].

%%====================================================================
%% list_buffers Tests
%%====================================================================

list_buffers_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"list_buffers returns empty when no pools",
         fun() ->
             ?assertEqual([], flurm_burst_buffer:list_buffers())
         end},

        {"list_buffers returns single pool",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             Result = flurm_burst_buffer:list_buffers(),
             ?assertEqual(1, length(Result)),
             [Pool] = Result,
             ?assertEqual(<<"nvme">>, maps:get(name, Pool)),
             ?assertEqual(100 * 1024 * 1024 * 1024, maps:get(total_capacity, Pool))
         end},

        {"list_buffers returns multiple pools",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_pool(<<"ssd">>, 50 * 1024 * 1024 * 1024),
             Result = flurm_burst_buffer:list_buffers(),
             ?assertEqual(2, length(Result)),
             Names = lists:sort([maps:get(name, P) || P <- Result]),
             ?assertEqual([<<"nvme">>, <<"ssd">>], Names)
         end},

        {"list_buffers shows correct capacity info",
         fun() ->
             TotalCap = 100 * 1024 * 1024 * 1024,
             Allocated = 30 * 1024 * 1024 * 1024,
             Available = 60 * 1024 * 1024 * 1024,
             Reserved = 10 * 1024 * 1024 * 1024,
             insert_test_pool(<<"nvme">>, TotalCap, Allocated, Available, Reserved),
             [Pool] = flurm_burst_buffer:list_buffers(),
             ?assertEqual(TotalCap, maps:get(total_capacity, Pool)),
             ?assertEqual(Allocated, maps:get(allocated_capacity, Pool)),
             ?assertEqual(Available, maps:get(available_capacity, Pool)),
             ?assertEqual(Reserved, maps:get(reserved_capacity, Pool))
         end}
     ]}.

%%====================================================================
%% list_pools Tests
%%====================================================================

list_pools_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"list_pools returns empty when no pools",
         fun() ->
             ?assertEqual([], flurm_burst_buffer:list_pools())
         end},

        {"list_pools returns records",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_pool(<<"ssd">>, 50 * 1024 * 1024 * 1024),
             Result = flurm_burst_buffer:list_pools(),
             ?assertEqual(2, length(Result)),
             %% Should be records
             [P1 | _] = Result,
             ?assert(is_record(P1, bb_pool))
         end}
     ]}.

%%====================================================================
%% get_pool_info Tests
%%====================================================================

get_pool_info_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"get_pool_info returns not_found for missing pool",
         fun() ->
             ?assertEqual({error, not_found}, flurm_burst_buffer:get_pool_info(<<"missing">>))
         end},

        {"get_pool_info returns pool record",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assert(is_record(Pool, bb_pool)),
             ?assertEqual(<<"nvme">>, Pool#bb_pool.name)
         end}
     ]}.

%%====================================================================
%% get_buffer_status Tests
%%====================================================================

get_buffer_status_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"get_buffer_status returns not_found for missing pool",
         fun() ->
             ?assertEqual({error, not_found}, flurm_burst_buffer:get_buffer_status(<<"missing">>))
         end},

        {"get_buffer_status returns detailed info",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             {ok, Status} = flurm_burst_buffer:get_buffer_status(<<"nvme">>),
             ?assertEqual(<<"nvme">>, maps:get(name, Status)),
             ?assert(maps:is_key(allocations, Status)),
             ?assert(maps:is_key(reservations, Status)),
             ?assertEqual(0, maps:get(allocation_count, Status)),
             ?assertEqual(0, maps:get(reservation_count, Status))
         end},

        {"get_buffer_status includes allocations",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             insert_test_allocation(1002, <<"nvme">>, 5 * 1024 * 1024 * 1024),
             {ok, Status} = flurm_burst_buffer:get_buffer_status(<<"nvme">>),
             ?assertEqual(2, maps:get(allocation_count, Status)),
             Allocs = maps:get(allocations, Status),
             ?assertEqual(2, length(Allocs))
         end},

        {"get_buffer_status includes reservations",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_reservation(2001, <<"nvme">>, 20 * 1024 * 1024 * 1024),
             {ok, Status} = flurm_burst_buffer:get_buffer_status(<<"nvme">>),
             ?assertEqual(1, maps:get(reservation_count, Status)),
             Reservations = maps:get(reservations, Status),
             ?assertEqual(1, length(Reservations))
         end}
     ]}.

%%====================================================================
%% get_job_allocation Tests
%%====================================================================

get_job_allocation_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"get_job_allocation returns not_found for missing job",
         fun() ->
             ?assertEqual({error, not_found}, flurm_burst_buffer:get_job_allocation(9999))
         end},

        {"get_job_allocation returns allocation info",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             {ok, Allocs} = flurm_burst_buffer:get_job_allocation(1001),
             ?assertEqual(1, length(Allocs)),
             [Alloc] = Allocs,
             ?assertEqual(1001, maps:get(job_id, Alloc)),
             ?assertEqual(<<"nvme">>, maps:get(pool, Alloc))
         end},

        {"get_job_allocation returns multiple allocations",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_pool(<<"ssd">>, 50 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"ssd">>, 5 * 1024 * 1024 * 1024),
             {ok, Allocs} = flurm_burst_buffer:get_job_allocation(1001),
             ?assertEqual(2, length(Allocs))
         end}
     ]}.

%%====================================================================
%% get_reservations Tests
%%====================================================================

get_reservations_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"get_reservations returns empty for pool with no reservations",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             ?assertEqual([], flurm_burst_buffer:get_reservations(<<"nvme">>))
         end},

        {"get_reservations returns reservations for pool",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_reservation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             insert_test_reservation(1002, <<"nvme">>, 20 * 1024 * 1024 * 1024),
             Result = flurm_burst_buffer:get_reservations(<<"nvme">>),
             ?assertEqual(2, length(Result))
         end},

        {"get_reservations only returns reservations for specified pool",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_pool(<<"ssd">>, 50 * 1024 * 1024 * 1024),
             insert_test_reservation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             insert_test_reservation(1002, <<"ssd">>, 5 * 1024 * 1024 * 1024),
             NvmeRes = flurm_burst_buffer:get_reservations(<<"nvme">>),
             SsdRes = flurm_burst_buffer:get_reservations(<<"ssd">>),
             ?assertEqual(1, length(NvmeRes)),
             ?assertEqual(1, length(SsdRes))
         end}
     ]}.

%%====================================================================
%% gen_server init/1 Tests
%%====================================================================

init_test_() ->
    {foreach,
     fun() ->
         %% Clean up any existing tables
         catch ets:delete(?BB_POOLS_TABLE),
         catch ets:delete(?BB_ALLOCS_TABLE),
         catch ets:delete(?BB_STAGING_TABLE),
         catch ets:delete(?BB_RESERVATIONS_TABLE),
         catch ets:delete(?BB_PERSISTENT_TABLE),
         %% Clear any configured pools
         application:unset_env(flurm_core, burst_buffer_pools),
         ok
     end,
     fun(_) ->
         cleanup_ets_tables(),
         application:unset_env(flurm_core, burst_buffer_pools)
     end,
     [
        {"init creates ETS tables",
         fun() ->
             {ok, _State} = flurm_burst_buffer:init([]),
             ?assertNotEqual(undefined, ets:info(?BB_POOLS_TABLE)),
             ?assertNotEqual(undefined, ets:info(?BB_ALLOCS_TABLE)),
             ?assertNotEqual(undefined, ets:info(?BB_STAGING_TABLE)),
             ?assertNotEqual(undefined, ets:info(?BB_RESERVATIONS_TABLE)),
             ?assertNotEqual(undefined, ets:info(?BB_PERSISTENT_TABLE))
         end},

        {"init returns state with empty stage_workers",
         fun() ->
             {ok, State} = flurm_burst_buffer:init([]),
             ?assertEqual(#{}, State#state.stage_workers),
             ?assertNotEqual(undefined, State#state.cleanup_timer)
         end},

        {"init creates default pool when no config",
         fun() ->
             {ok, _State} = flurm_burst_buffer:init([]),
             %% Should have default pool
             ?assertEqual(1, ets:info(?BB_POOLS_TABLE, size)),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"default">>),
             ?assertEqual(<<"default">>, Pool#bb_pool.name),
             ?assertEqual(generic, Pool#bb_pool.type)
         end},

        {"init loads configured pools",
         fun() ->
             application:set_env(flurm_core, burst_buffer_pools, [
                 {<<"nvme">>, #{type => generic, capacity => <<"100GB">>}},
                 {<<"datawarp">>, #{type => datawarp, capacity => <<"1TB">>}}
             ]),
             {ok, _State} = flurm_burst_buffer:init([]),
             ?assertEqual(2, ets:info(?BB_POOLS_TABLE, size)),
             ?assertEqual(true, flurm_burst_buffer:get_pool_info(<<"nvme">>) =/= {error, not_found}),
             ?assertEqual(true, flurm_burst_buffer:get_pool_info(<<"datawarp">>) =/= {error, not_found})
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - register_buffer
%%====================================================================

handle_call_register_buffer_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call register_buffer creates new pool",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Config = #{type => generic, capacity => <<"100GB">>},
             {reply, ok, NewState} = flurm_burst_buffer:handle_call(
                 {register_buffer, <<"nvme">>, Config}, {self(), make_ref()}, State),
             ?assertEqual(State, NewState),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(<<"nvme">>, Pool#bb_pool.name),
             ?assertEqual(generic, Pool#bb_pool.type),
             ?assertEqual(100 * 1024 * 1024 * 1024, Pool#bb_pool.total_capacity)
         end},

        {"handle_call register_buffer with datawarp type",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Config = #{type => datawarp, capacity => <<"50GB">>},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {register_buffer, <<"dw">>, Config}, {self(), make_ref()}, State),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"dw">>),
             ?assertEqual(datawarp, Pool#bb_pool.type)
         end},

        {"handle_call register_buffer with lua type",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Config = #{type => lua, capacity => <<"10GB">>, lua_script => <<"/path/to/script.lua">>},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {register_buffer, <<"lua_bb">>, Config}, {self(), make_ref()}, State),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"lua_bb">>),
             ?assertEqual(lua, Pool#bb_pool.type),
             ?assertEqual(<<"/path/to/script.lua">>, Pool#bb_pool.lua_script)
         end},

        {"handle_call register_buffer with nodes",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Config = #{type => generic, capacity => <<"100GB">>, nodes => [<<"node1">>, <<"node2">>]},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {register_buffer, <<"fast">>, Config}, {self(), make_ref()}, State),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"fast">>),
             ?assertEqual([<<"node1">>, <<"node2">>], Pool#bb_pool.nodes)
         end},

        {"handle_call register_buffer with custom granularity",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Config = #{type => generic, capacity => <<"100GB">>, granularity => <<"4MB">>},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {register_buffer, <<"aligned">>, Config}, {self(), make_ref()}, State),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"aligned">>),
             ?assertEqual(4 * 1024 * 1024, Pool#bb_pool.granularity)
         end},

        {"handle_call register_buffer with binary type string",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Config = #{type => <<"generic">>, capacity => <<"10GB">>},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {register_buffer, <<"test">>, Config}, {self(), make_ref()}, State),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"test">>),
             ?assertEqual(generic, Pool#bb_pool.type)
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - unregister_buffer
%%====================================================================

handle_call_unregister_buffer_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call unregister_buffer removes pool",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {unregister_buffer, <<"nvme">>}, {self(), make_ref()}, State),
             ?assertEqual({error, not_found}, flurm_burst_buffer:get_pool_info(<<"nvme">>))
         end},

        {"handle_call unregister_buffer fails when pool has allocations",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {unregister_buffer, <<"nvme">>}, {self(), make_ref()}, State),
             ?assertEqual({error, pool_in_use}, Result),
             ?assertNotEqual({error, not_found}, flurm_burst_buffer:get_pool_info(<<"nvme">>))
         end},

        {"handle_call unregister_buffer fails when pool has reservations",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_reservation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {unregister_buffer, <<"nvme">>}, {self(), make_ref()}, State),
             ?assertEqual({error, has_reservations}, Result)
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - allocate_request
%%====================================================================

handle_call_allocate_request_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call allocate_request succeeds with sufficient space",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Request = #bb_request{
                 pool = <<"nvme">>,
                 size = 10 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, {ok, Path}, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State),
             ?assert(is_binary(Path)),
             %% Check allocation was created
             {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(1001),
             ?assertEqual(1001, maps:get(job_id, Alloc)),
             ?assertEqual(<<"nvme">>, maps:get(pool, Alloc))
         end},

        {"handle_call allocate_request fails with insufficient space",
         fun() ->
             insert_test_pool(<<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Request = #bb_request{
                 pool = <<"nvme">>,
                 size = 20 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State),
             ?assertEqual({error, insufficient_space}, Result)
         end},

        {"handle_call allocate_request fails for missing pool",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Request = #bb_request{
                 pool = <<"missing">>,
                 size = 10 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State),
             ?assertEqual({error, pool_not_found}, Result)
         end},

        {"handle_call allocate_request uses reservation",
         fun() ->
             %% Pool with 20GB available
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024,
                              0, 20 * 1024 * 1024 * 1024, 80 * 1024 * 1024 * 1024),
             %% Add reservation for job 1001
             insert_test_reservation(1001, <<"nvme">>, 50 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Request = #bb_request{
                 pool = <<"nvme">>,
                 size = 50 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             %% Should succeed because reservation adds to available
             {reply, {ok, _Path}, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State),
             %% Reservation should be consumed
             ?assertEqual([], flurm_burst_buffer:get_reservations(<<"nvme">>))
         end},

        {"handle_call allocate_request with pool=any finds available pool",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Request = #bb_request{
                 pool = any,
                 size = 10 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, {ok, _Path}, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State)
         end},

        {"handle_call allocate_request with pool=any fails when no pool available",
         fun() ->
             %% Pool is down
             insert_test_pool_full(<<"nvme">>, generic, 100 * 1024 * 1024 * 1024,
                                   0, 100 * 1024 * 1024 * 1024, 0, down, undefined),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Request = #bb_request{
                 pool = any,
                 size = 10 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State),
             ?assertEqual({error, no_available_pool}, Result)
         end},

        {"handle_call allocate_request sets state to ready when no stage_in",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Request = #bb_request{
                 pool = <<"nvme">>,
                 size = 10 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, {ok, _}, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State),
             {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(1001),
             ?assertEqual(ready, maps:get(state, Alloc))
         end},

        {"handle_call allocate_request sets state to staging_in when stage_in provided",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Request = #bb_request{
                 pool = <<"nvme">>,
                 size = 10 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [{<<"/data/input">>, <<"input">>}],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, {ok, _}, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State),
             {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(1001),
             ?assertEqual(staging_in, maps:get(state, Alloc))
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - allocate_legacy
%%====================================================================

handle_call_allocate_legacy_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call allocate_legacy succeeds",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, {ok, Path}, _} = flurm_burst_buffer:handle_call(
                 {allocate_legacy, 1001, <<"nvme">>, 10 * 1024 * 1024 * 1024},
                 {self(), make_ref()}, State),
             ?assert(is_binary(Path)),
             {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(1001),
             ?assertEqual(1001, maps:get(job_id, Alloc))
         end},

        {"handle_call allocate_legacy fails with insufficient space",
         fun() ->
             insert_test_pool(<<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {allocate_legacy, 1001, <<"nvme">>, 100 * 1024 * 1024 * 1024},
                 {self(), make_ref()}, State),
             ?assertEqual({error, insufficient_space}, Result)
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - deallocate_job
%%====================================================================

handle_call_deallocate_job_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call deallocate_job removes allocation",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024,
                              10 * 1024 * 1024 * 1024, 90 * 1024 * 1024 * 1024, 0),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {deallocate_job, 1001}, {self(), make_ref()}, State),
             ?assertEqual({error, not_found}, flurm_burst_buffer:get_job_allocation(1001)),
             %% Capacity should be returned
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(100 * 1024 * 1024 * 1024, Pool#bb_pool.available_capacity)
         end},

        {"handle_call deallocate_job returns not_found for missing job",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {deallocate_job, 9999}, {self(), make_ref()}, State),
             ?assertEqual({error, not_found}, Result)
         end},

        {"handle_call deallocate_job removes multiple allocations",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024,
                              15 * 1024 * 1024 * 1024, 85 * 1024 * 1024 * 1024, 0),
             insert_test_pool(<<"ssd">>, 50 * 1024 * 1024 * 1024,
                              5 * 1024 * 1024 * 1024, 45 * 1024 * 1024 * 1024, 0),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"ssd">>, 5 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {deallocate_job, 1001}, {self(), make_ref()}, State),
             ?assertEqual({error, not_found}, flurm_burst_buffer:get_job_allocation(1001))
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - deallocate_legacy
%%====================================================================

handle_call_deallocate_legacy_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call deallocate_legacy removes specific allocation",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024,
                              10 * 1024 * 1024 * 1024, 90 * 1024 * 1024 * 1024, 0),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {deallocate_legacy, 1001, <<"nvme">>}, {self(), make_ref()}, State),
             ?assertEqual({error, not_found}, flurm_burst_buffer:get_job_allocation(1001))
         end},

        {"handle_call deallocate_legacy handles missing allocation gracefully",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {deallocate_legacy, 9999, <<"missing">>}, {self(), make_ref()}, State)
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - reserve_capacity
%%====================================================================

handle_call_reserve_capacity_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call reserve_capacity succeeds with sufficient space",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {reserve_capacity, 1001, <<"nvme">>, 20 * 1024 * 1024 * 1024},
                 {self(), make_ref()}, State),
             Reservations = flurm_burst_buffer:get_reservations(<<"nvme">>),
             ?assertEqual(1, length(Reservations)),
             [Res] = Reservations,
             ?assertEqual(1001, maps:get(job_id, Res)),
             ?assertEqual(20 * 1024 * 1024 * 1024, maps:get(size, Res))
         end},

        {"handle_call reserve_capacity fails with insufficient space",
         fun() ->
             insert_test_pool(<<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {reserve_capacity, 1001, <<"nvme">>, 20 * 1024 * 1024 * 1024},
                 {self(), make_ref()}, State),
             ?assertEqual({error, insufficient_space}, Result)
         end},

        {"handle_call reserve_capacity fails for missing pool",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {reserve_capacity, 1001, <<"missing">>, 10 * 1024 * 1024 * 1024},
                 {self(), make_ref()}, State),
             ?assertEqual({error, pool_not_found}, Result)
         end},

        {"handle_call reserve_capacity updates pool capacity",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {reserve_capacity, 1001, <<"nvme">>, 30 * 1024 * 1024 * 1024},
                 {self(), make_ref()}, State),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(30 * 1024 * 1024 * 1024, Pool#bb_pool.reserved_capacity),
             ?assertEqual(70 * 1024 * 1024 * 1024, Pool#bb_pool.available_capacity)
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - release_reservation
%%====================================================================

handle_call_release_reservation_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call release_reservation removes reservation",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024,
                              0, 80 * 1024 * 1024 * 1024, 20 * 1024 * 1024 * 1024),
             insert_test_reservation(1001, <<"nvme">>, 20 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {release_reservation, 1001, <<"nvme">>}, {self(), make_ref()}, State),
             ?assertEqual([], flurm_burst_buffer:get_reservations(<<"nvme">>)),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(0, Pool#bb_pool.reserved_capacity),
             ?assertEqual(100 * 1024 * 1024 * 1024, Pool#bb_pool.available_capacity)
         end},

        {"handle_call release_reservation handles missing reservation",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {release_reservation, 9999, <<"missing">>}, {self(), make_ref()}, State)
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - stage_in_job
%%====================================================================

handle_call_stage_in_job_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call stage_in_job fails without allocation",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {stage_in_job, 1001, [{<<"/data/in">>, <<"in">>}]},
                 {self(), make_ref()}, State),
             ?assertEqual({error, allocation_not_found}, Result)
         end},

        {"handle_call stage_in_job starts staging with allocation",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, {ok, Ref}, NewState} = flurm_burst_buffer:handle_call(
                 {stage_in_job, 1001, [{<<"/data/in">>, <<"in">>}]},
                 {self(), make_ref()}, State),
             ?assert(is_reference(Ref)),
             ?assert(maps:is_key(Ref, NewState#state.stage_workers))
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - stage_out_job
%%====================================================================

handle_call_stage_out_job_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call stage_out_job fails without allocation",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {stage_out_job, 1001, [{<<"out">>, <<"/data/out">>}]},
                 {self(), make_ref()}, State),
             ?assertEqual({error, allocation_not_found}, Result)
         end},

        {"handle_call stage_out_job starts staging with allocation",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, {ok, Ref}, NewState} = flurm_burst_buffer:handle_call(
                 {stage_out_job, 1001, [{<<"out">>, <<"/data/out">>}]},
                 {self(), make_ref()}, State),
             ?assert(is_reference(Ref)),
             ?assert(maps:is_key(Ref, NewState#state.stage_workers))
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - stage_in/out_legacy
%%====================================================================

handle_call_stage_legacy_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call stage_in_legacy fails without allocation",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {stage_in_legacy, 1001, <<"nvme">>, [{<<"/data/in">>, <<"in">>}]},
                 {self(), make_ref()}, State),
             ?assertEqual({error, allocation_not_found}, Result)
         end},

        {"handle_call stage_in_legacy succeeds with allocation",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, {ok, Ref}, _} = flurm_burst_buffer:handle_call(
                 {stage_in_legacy, 1001, <<"nvme">>, [{<<"/data/in">>, <<"in">>}]},
                 {self(), make_ref()}, State),
             ?assert(is_reference(Ref))
         end},

        {"handle_call stage_out_legacy fails without allocation",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Result, _} = flurm_burst_buffer:handle_call(
                 {stage_out_legacy, 1001, <<"nvme">>, [{<<"out">>, <<"/data/out">>}]},
                 {self(), make_ref()}, State),
             ?assertEqual({error, allocation_not_found}, Result)
         end},

        {"handle_call stage_out_legacy succeeds with allocation",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, {ok, Ref}, _} = flurm_burst_buffer:handle_call(
                 {stage_out_legacy, 1001, <<"nvme">>, [{<<"out">>, <<"/data/out">>}]},
                 {self(), make_ref()}, State),
             ?assert(is_reference(Ref))
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - get_stats
%%====================================================================

handle_call_get_stats_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_call get_stats returns empty stats",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Stats, _} = flurm_burst_buffer:handle_call(
                 get_stats, {self(), make_ref()}, State),
             ?assertEqual(0, maps:get(pool_count, Stats)),
             ?assertEqual(0, maps:get(total_capacity, Stats)),
             ?assertEqual(0, maps:get(allocation_count, Stats))
         end},

        {"handle_call get_stats returns pool stats",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024,
                              20 * 1024 * 1024 * 1024, 80 * 1024 * 1024 * 1024, 0),
             insert_test_pool(<<"ssd">>, 50 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             insert_test_allocation(1002, <<"nvme">>, 10 * 1024 * 1024 * 1024),
             insert_test_reservation(2001, <<"ssd">>, 5 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {reply, Stats, _} = flurm_burst_buffer:handle_call(
                 get_stats, {self(), make_ref()}, State),
             ?assertEqual(2, maps:get(pool_count, Stats)),
             ?assertEqual(150 * 1024 * 1024 * 1024, maps:get(total_capacity, Stats)),
             ?assertEqual(2, maps:get(allocation_count, Stats)),
             ?assertEqual(1, maps:get(reservation_count, Stats)),
             ?assert(maps:is_key(pools, Stats)),
             ?assert(maps:is_key(utilization, Stats))
         end}
     ]}.

%%====================================================================
%% gen_server handle_call Tests - unknown request
%%====================================================================

handle_call_unknown_test() ->
    State = #state{stage_workers = #{}, cleanup_timer = undefined},
    {reply, Result, NewState} = flurm_burst_buffer:handle_call(
        unknown_request, {self(), make_ref()}, State),
    ?assertEqual({error, unknown_request}, Result),
    ?assertEqual(State, NewState).

%%====================================================================
%% gen_server handle_cast Tests
%%====================================================================

handle_cast_test() ->
    State = #state{stage_workers = #{}, cleanup_timer = undefined},
    {noreply, NewState} = flurm_burst_buffer:handle_cast(some_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% gen_server handle_info Tests
%%====================================================================

handle_info_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"handle_info staging_complete updates allocation state",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024, staging_in),
             Ref = make_ref(),
             State = #state{
                 stage_workers = #{Ref => {1001, <<"nvme">>, in}},
                 cleanup_timer = undefined
             },
             {noreply, NewState} = flurm_burst_buffer:handle_info(
                 {staging_complete, Ref, ok}, State),
             ?assertEqual(#{}, NewState#state.stage_workers),
             {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(1001),
             ?assertEqual(ready, maps:get(state, Alloc))
         end},

        {"handle_info staging_complete handles stage_out",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024, staging_out),
             Ref = make_ref(),
             State = #state{
                 stage_workers = #{Ref => {1001, <<"nvme">>, out}},
                 cleanup_timer = undefined
             },
             {noreply, NewState} = flurm_burst_buffer:handle_info(
                 {staging_complete, Ref, ok}, State),
             ?assertEqual(#{}, NewState#state.stage_workers),
             {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(1001),
             ?assertEqual(complete, maps:get(state, Alloc))
         end},

        {"handle_info staging_complete handles error",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             insert_test_allocation(1001, <<"nvme">>, 10 * 1024 * 1024 * 1024, staging_in),
             Ref = make_ref(),
             State = #state{
                 stage_workers = #{Ref => {1001, <<"nvme">>, in}},
                 cleanup_timer = undefined
             },
             {noreply, _NewState} = flurm_burst_buffer:handle_info(
                 {staging_complete, Ref, {error, file_not_found}}, State),
             {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(1001),
             ?assertEqual(error, maps:get(state, Alloc)),
             ?assertNotEqual(undefined, maps:get(error_msg, Alloc))
         end},

        {"handle_info staging_complete ignores unknown ref",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {noreply, NewState} = flurm_burst_buffer:handle_info(
                 {staging_complete, make_ref(), ok}, State),
             ?assertEqual(State, NewState)
         end},

        {"handle_info cleanup_expired reschedules timer",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = make_ref()},
             {noreply, NewState} = flurm_burst_buffer:handle_info(
                 cleanup_expired, State),
             ?assertNotEqual(State#state.cleanup_timer, NewState#state.cleanup_timer)
         end},

        {"handle_info unknown message ignored",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             {noreply, NewState} = flurm_burst_buffer:handle_info(
                 unknown_message, State),
             ?assertEqual(State, NewState)
         end}
     ]}.

%%====================================================================
%% gen_server terminate Tests
%%====================================================================

terminate_test_() ->
    [
        {"terminate with undefined timer",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             ?assertEqual(ok, flurm_burst_buffer:terminate(normal, State))
         end},

        {"terminate cancels timer",
         fun() ->
             Timer = erlang:send_after(60000, self(), test),
             State = #state{stage_workers = #{}, cleanup_timer = Timer},
             ?assertEqual(ok, flurm_burst_buffer:terminate(normal, State)),
             %% Timer should be cancelled
             ?assertEqual(false, erlang:read_timer(Timer) =/= false)
         end},

        {"terminate handles different reasons",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             ?assertEqual(ok, flurm_burst_buffer:terminate(shutdown, State)),
             ?assertEqual(ok, flurm_burst_buffer:terminate({error, reason}, State))
         end}
    ].

%%====================================================================
%% gen_server code_change Tests
%%====================================================================

code_change_test() ->
    State = #state{stage_workers = #{}, cleanup_timer = undefined},
    ?assertEqual({ok, State}, flurm_burst_buffer:code_change("1.0", State, [])),
    ?assertEqual({ok, State}, flurm_burst_buffer:code_change("2.0", State, extra)).

%%====================================================================
%% Integration-style Tests (Using ETS Directly)
%%====================================================================

allocation_workflow_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"full allocation workflow",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},

             %% Register pool
             Config = #{type => generic, capacity => <<"100GB">>},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {register_buffer, <<"nvme">>, Config}, {self(), make_ref()}, State),

             %% Verify initial state
             {ok, Pool1} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(100 * 1024 * 1024 * 1024, Pool1#bb_pool.available_capacity),

             %% Allocate for job 1001
             Request1 = #bb_request{
                 pool = <<"nvme">>,
                 size = 20 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, {ok, _Path1}, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request1}, {self(), make_ref()}, State),

             %% Allocate for job 1002
             Request2 = #bb_request{
                 pool = <<"nvme">>,
                 size = 30 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, {ok, _Path2}, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1002, Request2}, {self(), make_ref()}, State),

             %% Check remaining capacity
             {ok, Pool2} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(50 * 1024 * 1024 * 1024, Pool2#bb_pool.available_capacity),

             %% Deallocate job 1001
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {deallocate_job, 1001}, {self(), make_ref()}, State),

             %% Check capacity returned
             {ok, Pool3} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(70 * 1024 * 1024 * 1024, Pool3#bb_pool.available_capacity)
         end},

        {"reservation workflow",
         fun() ->
             State = #state{stage_workers = #{}, cleanup_timer = undefined},

             %% Register pool
             Config = #{type => generic, capacity => <<"100GB">>},
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {register_buffer, <<"nvme">>, Config}, {self(), make_ref()}, State),

             %% Reserve capacity
             {reply, ok, _} = flurm_burst_buffer:handle_call(
                 {reserve_capacity, 1001, <<"nvme">>, 40 * 1024 * 1024 * 1024},
                 {self(), make_ref()}, State),

             %% Check capacity reduced
             {ok, Pool1} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(60 * 1024 * 1024 * 1024, Pool1#bb_pool.available_capacity),
             ?assertEqual(40 * 1024 * 1024 * 1024, Pool1#bb_pool.reserved_capacity),

             %% Allocate using reservation
             Request = #bb_request{
                 pool = <<"nvme">>,
                 size = 40 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, {ok, _}, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State),

             %% Reservation should be consumed
             ?assertEqual([], flurm_burst_buffer:get_reservations(<<"nvme">>)),
             {ok, Pool2} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(0, Pool2#bb_pool.reserved_capacity)
         end}
     ]}.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup_ets_tables/0,
     fun(_) -> cleanup_ets_tables() end,
     [
        {"allocate exactly all available",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Request = #bb_request{
                 pool = <<"nvme">>,
                 size = 100 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, {ok, _}, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State),
             {ok, Pool} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(0, Pool#bb_pool.available_capacity)
         end},

        {"multiple jobs same pool",
         fun() ->
             insert_test_pool(<<"nvme">>, 100 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             %% Allocate for 10 jobs
             lists:foreach(fun(JobId) ->
                 {reply, {ok, _}, _} = flurm_burst_buffer:handle_call(
                     {allocate_legacy, JobId, <<"nvme">>, 10 * 1024 * 1024 * 1024},
                     {self(), make_ref()}, State)
             end, lists:seq(1, 10)),
             {ok, Pool1} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(0, Pool1#bb_pool.available_capacity),

             %% Deallocate all
             lists:foreach(fun(JobId) ->
                 {reply, ok, _} = flurm_burst_buffer:handle_call(
                     {deallocate_job, JobId}, {self(), make_ref()}, State)
             end, lists:seq(1, 10)),
             {ok, Pool2} = flurm_burst_buffer:get_pool_info(<<"nvme">>),
             ?assertEqual(100 * 1024 * 1024 * 1024, Pool2#bb_pool.available_capacity)
         end},

        {"pool with down state not selected for any allocation",
         fun() ->
             insert_test_pool_full(<<"nvme">>, generic, 100 * 1024 * 1024 * 1024,
                                   0, 100 * 1024 * 1024 * 1024, 0, down, undefined),
             insert_test_pool(<<"ssd">>, 50 * 1024 * 1024 * 1024),
             State = #state{stage_workers = #{}, cleanup_timer = undefined},
             Request = #bb_request{
                 pool = any,
                 size = 10 * 1024 * 1024 * 1024,
                 access = striped,
                 type = scratch,
                 stage_in = [],
                 stage_out = [],
                 persistent = false,
                 persistent_name = undefined
             },
             {reply, {ok, _}, _} = flurm_burst_buffer:handle_call(
                 {allocate_request, 1001, Request}, {self(), make_ref()}, State),
             {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(1001),
             ?assertEqual(<<"ssd">>, maps:get(pool, Alloc))
         end}
     ]}.

%%====================================================================
%% Size Parsing Tests
%%====================================================================

size_parsing_test_() ->
    [
        {"parse GB size",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"capacity=100GB">>),
             ?assertEqual(100 * 1024 * 1024 * 1024, Req#bb_request.size)
         end},

        {"parse TB size",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"capacity=1TB">>),
             ?assertEqual(1024 * 1024 * 1024 * 1024, Req#bb_request.size)
         end},

        {"parse MB size",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"capacity=500MB">>),
             ?assertEqual(500 * 1024 * 1024, Req#bb_request.size)
         end},

        {"parse KB size",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"capacity=1024KB">>),
             ?assertEqual(1024 * 1024, Req#bb_request.size)
         end},

        {"parse bare number",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"capacity=1048576">>),
             ?assertEqual(1048576, Req#bb_request.size)
         end},

        {"parse G shorthand",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"capacity=10G">>),
             ?assertEqual(10 * 1024 * 1024 * 1024, Req#bb_request.size)
         end},

        {"parse M shorthand",
         fun() ->
             {ok, Req} = flurm_burst_buffer:parse_bb_spec(<<"capacity=100M">>),
             ?assertEqual(100 * 1024 * 1024, Req#bb_request.size)
         end}
    ].
