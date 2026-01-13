%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_burst_buffer module
%%%
%%% Tests cover:
%%% - Buffer pool registration and management
%%% - Job allocation and deallocation
%%% - Stage-in/stage-out operations
%%% - Directive parsing (#BB directives)
%%% - Capacity reservations
%%% - Statistics collection
%%%-------------------------------------------------------------------
-module(flurm_burst_buffer_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start the burst buffer server
    case whereis(flurm_burst_buffer) of
        undefined ->
            {ok, Pid} = flurm_burst_buffer:start_link(),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, _Pid}) ->
    %% Clean up ETS tables
    catch ets:delete(flurm_bb_pools),
    catch ets:delete(flurm_bb_allocations),
    catch ets:delete(flurm_bb_staging),
    catch ets:delete(flurm_bb_reservations),
    catch ets:delete(flurm_bb_persistent),
    gen_server:stop(flurm_burst_buffer);
cleanup({existing, _Pid}) ->
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

burst_buffer_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"register and unregister buffer pools", fun test_register_unregister_buffer/0},
      {"list buffers returns registered pools", fun test_list_buffers/0},
      {"get buffer status for existing pool", fun test_get_buffer_status/0},
      {"get buffer status for non-existent pool", fun test_get_buffer_status_not_found/0},
      {"allocate buffer space for job", fun test_allocate/0},
      {"allocate with map spec", fun test_allocate_map_spec/0},
      {"allocate with binary spec", fun test_allocate_binary_spec/0},
      {"allocate insufficient space", fun test_allocate_insufficient_space/0},
      {"deallocate job buffer", fun test_deallocate/0},
      {"deallocate non-existent job", fun test_deallocate_not_found/0},
      {"get job allocation", fun test_get_job_allocation/0},
      {"stage in operation", fun test_stage_in/0},
      {"stage out operation", fun test_stage_out/0},
      {"stage operation without allocation", fun test_stage_without_allocation/0},
      {"reserve capacity", fun test_reserve_capacity/0},
      {"release reservation", fun test_release_reservation/0},
      {"get reservations", fun test_get_reservations/0},
      {"reserve converts to allocation", fun test_reserve_converts_to_allocation/0},
      {"unregister pool with allocations fails", fun test_unregister_pool_in_use/0},
      {"unregister pool with reservations fails", fun test_unregister_pool_has_reservations/0},
      {"legacy API compatibility", fun test_legacy_api/0},
      {"get stats", fun test_get_stats/0},
      {"capacity tracking", fun test_capacity_tracking/0}
     ]}.

parse_directives_test_() ->
    [
     {"parse create_persistent directive", fun test_parse_create_persistent/0},
     {"parse destroy_persistent directive", fun test_parse_destroy_persistent/0},
     {"parse stage_in directive", fun test_parse_stage_in_directive/0},
     {"parse stage_out directive", fun test_parse_stage_out_directive/0},
     {"parse allocate directive", fun test_parse_allocate_directive/0},
     {"parse multiple directives", fun test_parse_multiple_directives/0},
     {"parse script with no directives", fun test_parse_no_directives/0},
     {"parse bb spec string", fun test_parse_bb_spec/0},
     {"format bb spec", fun test_format_bb_spec/0}
    ].

%%====================================================================
%% Buffer Pool Management Tests
%%====================================================================

test_register_unregister_buffer() ->
    PoolName = <<"test_pool">>,
    Config = #{
        type => generic,
        capacity => <<"10GB">>,
        nodes => [<<"node1">>, <<"node2">>],
        granularity => <<"1MB">>
    },

    %% Register pool
    ?assertEqual(ok, flurm_burst_buffer:register_buffer(PoolName, Config)),

    %% Verify it exists
    Buffers = flurm_burst_buffer:list_buffers(),
    PoolNames = [maps:get(name, B) || B <- Buffers],
    ?assert(lists:member(PoolName, PoolNames)),

    %% Unregister pool
    ?assertEqual(ok, flurm_burst_buffer:unregister_buffer(PoolName)),

    %% Verify it's gone
    Buffers2 = flurm_burst_buffer:list_buffers(),
    PoolNames2 = [maps:get(name, B) || B <- Buffers2],
    ?assertNot(lists:member(PoolName, PoolNames2)).

test_list_buffers() ->
    %% Default pool should exist
    Buffers = flurm_burst_buffer:list_buffers(),
    ?assert(length(Buffers) >= 1),

    %% Register additional pools
    ok = flurm_burst_buffer:register_buffer(<<"pool_a">>, #{capacity => <<"5GB">>}),
    ok = flurm_burst_buffer:register_buffer(<<"pool_b">>, #{capacity => <<"10GB">>}),

    Buffers2 = flurm_burst_buffer:list_buffers(),
    ?assert(length(Buffers2) >= 3),

    %% Verify pool info structure
    [Pool | _] = Buffers2,
    ?assert(maps:is_key(name, Pool)),
    ?assert(maps:is_key(type, Pool)),
    ?assert(maps:is_key(total_capacity, Pool)),
    ?assert(maps:is_key(available_capacity, Pool)),
    ?assert(maps:is_key(state, Pool)).

test_get_buffer_status() ->
    %% Default pool exists
    {ok, Status} = flurm_burst_buffer:get_buffer_status(<<"default">>),

    ?assertEqual(<<"default">>, maps:get(name, Status)),
    ?assertEqual(generic, maps:get(type, Status)),
    ?assertEqual(up, maps:get(state, Status)),
    ?assert(maps:is_key(allocations, Status)),
    ?assert(maps:is_key(reservations, Status)),
    ?assert(maps:is_key(allocation_count, Status)),
    ?assert(maps:is_key(reservation_count, Status)),
    ?assert(maps:is_key(utilization, Status)).

test_get_buffer_status_not_found() ->
    ?assertEqual({error, not_found},
                 flurm_burst_buffer:get_buffer_status(<<"nonexistent">>)).

%%====================================================================
%% Allocation Tests
%%====================================================================

test_allocate() ->
    JobId = 1001,
    Spec = #{
        pool => <<"default">>,
        size => 1024 * 1024 * 100  % 100MB
    },

    {ok, Path} = flurm_burst_buffer:allocate(JobId, Spec),
    ?assert(is_binary(Path)),
    ?assertMatch(<<"/bb/default/", _/binary>>, Path),

    %% Verify allocation exists
    {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(JobId),
    ?assertEqual(JobId, maps:get(job_id, Alloc)),
    ?assertEqual(<<"default">>, maps:get(pool, Alloc)),
    ?assertEqual(Path, maps:get(path, Alloc)).

test_allocate_map_spec() ->
    JobId = 1002,
    Spec = #{
        pool => <<"default">>,
        capacity => 50 * 1024 * 1024,  % 50MB
        access => private,
        type => cache,
        persistent => false
    },

    {ok, Path} = flurm_burst_buffer:allocate(JobId, Spec),
    ?assert(is_binary(Path)),

    {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(JobId),
    ?assertEqual(false, maps:get(persistent, Alloc)).

test_allocate_binary_spec() ->
    JobId = 1003,
    Spec = <<"pool=default capacity=100MB access=striped type=scratch">>,

    {ok, Path} = flurm_burst_buffer:allocate(JobId, Spec),
    ?assert(is_binary(Path)),

    {ok, [_Alloc]} = flurm_burst_buffer:get_job_allocation(JobId).

test_allocate_insufficient_space() ->
    %% Create a small pool
    ok = flurm_burst_buffer:register_buffer(<<"small_pool">>, #{
        capacity => <<"10MB">>
    }),

    JobId = 1004,
    Spec = #{
        pool => <<"small_pool">>,
        size => 1024 * 1024 * 1024  % 1GB - more than pool has
    },

    ?assertEqual({error, insufficient_space},
                 flurm_burst_buffer:allocate(JobId, Spec)).

%%====================================================================
%% Deallocation Tests
%%====================================================================

test_deallocate() ->
    JobId = 2001,
    Spec = #{pool => <<"default">>, size => 50 * 1024 * 1024},

    %% Allocate first
    {ok, _Path} = flurm_burst_buffer:allocate(JobId, Spec),

    %% Verify allocation exists
    {ok, _} = flurm_burst_buffer:get_job_allocation(JobId),

    %% Deallocate
    ?assertEqual(ok, flurm_burst_buffer:deallocate(JobId)),

    %% Verify allocation is gone
    ?assertEqual({error, not_found}, flurm_burst_buffer:get_job_allocation(JobId)).

test_deallocate_not_found() ->
    ?assertEqual({error, not_found}, flurm_burst_buffer:deallocate(99999)).

test_get_job_allocation() ->
    JobId = 2002,
    Spec = #{pool => <<"default">>, size => 25 * 1024 * 1024},

    %% Initially no allocation
    ?assertEqual({error, not_found}, flurm_burst_buffer:get_job_allocation(JobId)),

    %% Create allocation
    {ok, Path} = flurm_burst_buffer:allocate(JobId, Spec),

    %% Get allocation
    {ok, Allocations} = flurm_burst_buffer:get_job_allocation(JobId),
    ?assertEqual(1, length(Allocations)),

    [Alloc] = Allocations,
    ?assertEqual(JobId, maps:get(job_id, Alloc)),
    ?assertEqual(Path, maps:get(path, Alloc)),
    ?assert(maps:is_key(state, Alloc)),
    ?assert(maps:is_key(create_time, Alloc)).

%%====================================================================
%% Stage-in/Stage-out Tests
%%====================================================================

test_stage_in() ->
    JobId = 3001,
    Spec = #{pool => <<"default">>, size => 50 * 1024 * 1024},

    %% Allocate first
    {ok, _Path} = flurm_burst_buffer:allocate(JobId, Spec),

    %% Stage in (files don't need to exist for test - operation is async)
    Files = [{<<"/tmp/source1.dat">>, <<"dest1.dat">>}],
    {ok, Ref} = flurm_burst_buffer:stage_in(JobId, Files),
    ?assert(is_reference(Ref)),

    %% Verify allocation state changed
    {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(JobId),
    ?assertEqual(staging_in, maps:get(state, Alloc)).

test_stage_out() ->
    JobId = 3002,
    Spec = #{pool => <<"default">>, size => 50 * 1024 * 1024},

    %% Allocate first
    {ok, _Path} = flurm_burst_buffer:allocate(JobId, Spec),

    %% Stage out (files don't need to exist for test - operation is async)
    Files = [{<<"result.dat">>, <<"/tmp/result.dat">>}],
    {ok, Ref} = flurm_burst_buffer:stage_out(JobId, Files),
    ?assert(is_reference(Ref)),

    %% Verify allocation state changed
    {ok, [Alloc]} = flurm_burst_buffer:get_job_allocation(JobId),
    ?assertEqual(staging_out, maps:get(state, Alloc)).

test_stage_without_allocation() ->
    JobId = 3003,
    Files = [{<<"/tmp/source.dat">>, <<"dest.dat">>}],

    ?assertEqual({error, allocation_not_found},
                 flurm_burst_buffer:stage_in(JobId, Files)),
    ?assertEqual({error, allocation_not_found},
                 flurm_burst_buffer:stage_out(JobId, Files)).

%%====================================================================
%% Reservation Tests
%%====================================================================

test_reserve_capacity() ->
    JobId = 4001,
    PoolName = <<"default">>,
    Size = 100 * 1024 * 1024,  % 100MB

    %% Get initial available capacity
    {ok, Status1} = flurm_burst_buffer:get_buffer_status(PoolName),
    InitialAvailable = maps:get(available_capacity, Status1),

    %% Reserve capacity
    ?assertEqual(ok, flurm_burst_buffer:reserve_capacity(JobId, PoolName, Size)),

    %% Verify available capacity decreased
    {ok, Status2} = flurm_burst_buffer:get_buffer_status(PoolName),
    NewAvailable = maps:get(available_capacity, Status2),
    ?assertEqual(InitialAvailable - Size, NewAvailable),

    %% Verify reserved capacity increased
    ReservedCapacity = maps:get(reserved_capacity, Status2),
    ?assert(ReservedCapacity >= Size).

test_release_reservation() ->
    JobId = 4002,
    PoolName = <<"default">>,
    Size = 50 * 1024 * 1024,  % 50MB

    %% Reserve first
    ok = flurm_burst_buffer:reserve_capacity(JobId, PoolName, Size),

    %% Get capacity after reservation
    {ok, Status1} = flurm_burst_buffer:get_buffer_status(PoolName),
    AvailableAfterReserve = maps:get(available_capacity, Status1),

    %% Release reservation
    ok = flurm_burst_buffer:release_reservation(JobId, PoolName),

    %% Verify capacity restored
    {ok, Status2} = flurm_burst_buffer:get_buffer_status(PoolName),
    AvailableAfterRelease = maps:get(available_capacity, Status2),
    ?assertEqual(AvailableAfterReserve + Size, AvailableAfterRelease).

test_get_reservations() ->
    JobId1 = 4003,
    JobId2 = 4004,
    PoolName = <<"default">>,

    %% Make some reservations
    ok = flurm_burst_buffer:reserve_capacity(JobId1, PoolName, 10 * 1024 * 1024),
    ok = flurm_burst_buffer:reserve_capacity(JobId2, PoolName, 20 * 1024 * 1024),

    %% Get reservations
    Reservations = flurm_burst_buffer:get_reservations(PoolName),
    ?assert(length(Reservations) >= 2),

    %% Verify structure
    [R | _] = Reservations,
    ?assert(maps:is_key(job_id, R)),
    ?assert(maps:is_key(pool, R)),
    ?assert(maps:is_key(size, R)),
    ?assert(maps:is_key(expiry_time, R)).

test_reserve_converts_to_allocation() ->
    JobId = 4005,
    PoolName = <<"default">>,
    Size = 30 * 1024 * 1024,  % 30MB

    %% Get initial available
    {ok, Status1} = flurm_burst_buffer:get_buffer_status(PoolName),
    InitialAvailable = maps:get(available_capacity, Status1),

    %% Make reservation
    ok = flurm_burst_buffer:reserve_capacity(JobId, PoolName, Size),

    %% Now allocate - should use reservation
    Spec = #{pool => PoolName, size => Size},
    {ok, _Path} = flurm_burst_buffer:allocate(JobId, Spec),

    %% Reservation should be gone
    Reservations = flurm_burst_buffer:get_reservations(PoolName),
    JobReservations = [R || R <- Reservations, maps:get(job_id, R) == JobId],
    ?assertEqual([], JobReservations),

    %% Available capacity should be the same as initial minus allocation size
    %% (reservation was converted, not added to allocation)
    {ok, Status2} = flurm_burst_buffer:get_buffer_status(PoolName),
    FinalAvailable = maps:get(available_capacity, Status2),
    %% Account for granularity rounding (size might be rounded up)
    ?assert(FinalAvailable =< InitialAvailable).

%%====================================================================
%% Unregister with Active Resources Tests
%%====================================================================

test_unregister_pool_in_use() ->
    PoolName = <<"pool_with_allocs">>,
    ok = flurm_burst_buffer:register_buffer(PoolName, #{capacity => <<"1GB">>}),

    %% Allocate from pool
    JobId = 5001,
    {ok, _} = flurm_burst_buffer:allocate(JobId, #{pool => PoolName, size => 100 * 1024 * 1024}),

    %% Try to unregister - should fail
    ?assertEqual({error, pool_in_use}, flurm_burst_buffer:unregister_buffer(PoolName)),

    %% Deallocate and try again
    ok = flurm_burst_buffer:deallocate(JobId),
    ?assertEqual(ok, flurm_burst_buffer:unregister_buffer(PoolName)).

test_unregister_pool_has_reservations() ->
    PoolName = <<"pool_with_reservations">>,
    ok = flurm_burst_buffer:register_buffer(PoolName, #{capacity => <<"1GB">>}),

    %% Make reservation
    JobId = 5002,
    ok = flurm_burst_buffer:reserve_capacity(JobId, PoolName, 100 * 1024 * 1024),

    %% Try to unregister - should fail
    ?assertEqual({error, has_reservations}, flurm_burst_buffer:unregister_buffer(PoolName)),

    %% Release reservation and try again
    ok = flurm_burst_buffer:release_reservation(JobId, PoolName),
    ?assertEqual(ok, flurm_burst_buffer:unregister_buffer(PoolName)).

%%====================================================================
%% Legacy API Tests
%%====================================================================

test_legacy_api() ->
    %% Test create_pool/delete_pool (aliases)
    ok = flurm_burst_buffer:create_pool(<<"legacy_pool">>, #{capacity => <<"500MB">>}),
    Pools = flurm_burst_buffer:list_pools(),
    ?assert(length(Pools) >= 1),

    %% Test get_pool_info
    {ok, _PoolInfo} = flurm_burst_buffer:get_pool_info(<<"legacy_pool">>),
    ?assertEqual({error, not_found}, flurm_burst_buffer:get_pool_info(<<"nonexistent">>)),

    %% Test 3-arg allocate/deallocate
    JobId = 6001,
    {ok, Path} = flurm_burst_buffer:allocate(JobId, <<"legacy_pool">>, 10 * 1024 * 1024),
    ?assert(is_binary(Path)),

    ok = flurm_burst_buffer:deallocate(JobId, <<"legacy_pool">>),

    %% Clean up
    ok = flurm_burst_buffer:delete_pool(<<"legacy_pool">>).

%%====================================================================
%% Statistics Tests
%%====================================================================

test_get_stats() ->
    Stats = flurm_burst_buffer:get_stats(),

    ?assert(is_map(Stats)),
    ?assert(maps:is_key(pool_count, Stats)),
    ?assert(maps:is_key(total_capacity, Stats)),
    ?assert(maps:is_key(allocated_capacity, Stats)),
    ?assert(maps:is_key(available_capacity, Stats)),
    ?assert(maps:is_key(reserved_capacity, Stats)),
    ?assert(maps:is_key(allocation_count, Stats)),
    ?assert(maps:is_key(reservation_count, Stats)),
    ?assert(maps:is_key(utilization, Stats)),
    ?assert(maps:is_key(pools, Stats)),

    %% Utilization should be a float between 0 and 1
    Utilization = maps:get(utilization, Stats),
    ?assert(is_float(Utilization)),
    ?assert(Utilization >= 0.0),
    ?assert(Utilization =< 1.0).

%%====================================================================
%% Capacity Tracking Tests
%%====================================================================

test_capacity_tracking() ->
    PoolName = <<"tracking_pool">>,
    TotalCapacity = 1024 * 1024 * 1024,  % 1GB
    ok = flurm_burst_buffer:register_buffer(PoolName, #{capacity => TotalCapacity}),

    %% Check initial state
    {ok, Status1} = flurm_burst_buffer:get_buffer_status(PoolName),
    ?assertEqual(TotalCapacity, maps:get(total_capacity, Status1)),
    ?assertEqual(TotalCapacity, maps:get(available_capacity, Status1)),
    ?assertEqual(0, maps:get(allocated_capacity, Status1)),
    ?assertEqual(0, maps:get(reserved_capacity, Status1)),

    %% Allocate 200MB
    JobId1 = 7001,
    AllocSize1 = 200 * 1024 * 1024,
    {ok, _} = flurm_burst_buffer:allocate(JobId1, #{pool => PoolName, size => AllocSize1}),

    {ok, Status2} = flurm_burst_buffer:get_buffer_status(PoolName),
    ?assertEqual(TotalCapacity, maps:get(total_capacity, Status2)),
    ?assert(maps:get(allocated_capacity, Status2) >= AllocSize1),
    ?assert(maps:get(available_capacity, Status2) < TotalCapacity),

    %% Reserve 100MB
    JobId2 = 7002,
    ReserveSize = 100 * 1024 * 1024,
    ok = flurm_burst_buffer:reserve_capacity(JobId2, PoolName, ReserveSize),

    {ok, Status3} = flurm_burst_buffer:get_buffer_status(PoolName),
    ?assertEqual(ReserveSize, maps:get(reserved_capacity, Status3)),

    %% Allocate another 300MB
    JobId3 = 7003,
    AllocSize3 = 300 * 1024 * 1024,
    {ok, _} = flurm_burst_buffer:allocate(JobId3, #{pool => PoolName, size => AllocSize3}),

    {ok, Status4} = flurm_burst_buffer:get_buffer_status(PoolName),
    TotalAllocated = maps:get(allocated_capacity, Status4),
    TotalReserved = maps:get(reserved_capacity, Status4),
    TotalAvailable = maps:get(available_capacity, Status4),

    %% Verify invariant: total = allocated + reserved + available
    ?assertEqual(TotalCapacity, TotalAllocated + TotalReserved + TotalAvailable),

    %% Deallocate first job
    ok = flurm_burst_buffer:deallocate(JobId1),

    {ok, Status5} = flurm_burst_buffer:get_buffer_status(PoolName),
    ?assert(maps:get(available_capacity, Status5) > maps:get(available_capacity, Status4)),

    %% Clean up
    ok = flurm_burst_buffer:deallocate(JobId3),
    ok = flurm_burst_buffer:release_reservation(JobId2, PoolName),
    ok = flurm_burst_buffer:unregister_buffer(PoolName).

%%====================================================================
%% Directive Parsing Tests
%%====================================================================

test_parse_create_persistent() ->
    Script = <<"#!/bin/bash\n#BB create_persistent name=mydata capacity=100GB pool=nvme\necho hello">>,
    {ok, Directives} = flurm_burst_buffer:parse_directives(Script),

    ?assertEqual(1, length(Directives)),
    [D] = Directives,
    ?assertEqual(create_persistent, element(2, D)),  % type field
    ?assertEqual(<<"mydata">>, element(3, D)),       % name field
    ?assertEqual(100 * 1024 * 1024 * 1024, element(4, D)),  % capacity
    ?assertEqual(<<"nvme">>, element(5, D)).         % pool field

test_parse_destroy_persistent() ->
    Script = <<"#BB destroy_persistent name=olddata\n">>,
    {ok, Directives} = flurm_burst_buffer:parse_directives(Script),

    ?assertEqual(1, length(Directives)),
    [D] = Directives,
    ?assertEqual(destroy_persistent, element(2, D)),
    ?assertEqual(<<"olddata">>, element(3, D)).

test_parse_stage_in_directive() ->
    Script = <<"#BB stage_in source=/data/input.dat destination=input.dat\n">>,
    {ok, Directives} = flurm_burst_buffer:parse_directives(Script),

    ?assertEqual(1, length(Directives)),
    [D] = Directives,
    ?assertEqual(stage_in, element(2, D)),
    ?assertEqual(<<"/data/input.dat">>, element(6, D)),   % source
    ?assertEqual(<<"input.dat">>, element(7, D)).         % destination

test_parse_stage_out_directive() ->
    Script = <<"#BB stage_out source=output.dat destination=/results/output.dat\n">>,
    {ok, Directives} = flurm_burst_buffer:parse_directives(Script),

    ?assertEqual(1, length(Directives)),
    [D] = Directives,
    ?assertEqual(stage_out, element(2, D)),
    ?assertEqual(<<"output.dat">>, element(6, D)),
    ?assertEqual(<<"/results/output.dat">>, element(7, D)).

test_parse_allocate_directive() ->
    Script = <<"#BB capacity=50GB pool=default access=striped type=scratch\n">>,
    {ok, Directives} = flurm_burst_buffer:parse_directives(Script),

    ?assertEqual(1, length(Directives)),
    [D] = Directives,
    ?assertEqual(allocate, element(2, D)),
    ?assertEqual(50 * 1024 * 1024 * 1024, element(4, D)),  % capacity
    ?assertEqual(<<"default">>, element(5, D)).            % pool

test_parse_multiple_directives() ->
    Script = <<"#!/bin/bash
#SBATCH -N 4
#BB capacity=100GB pool=nvme
#BB stage_in source=/data/input.tar destination=input.tar
#BB stage_out source=results.tar destination=/output/results.tar

./run_computation.sh
">>,
    {ok, Directives} = flurm_burst_buffer:parse_directives(Script),

    ?assertEqual(3, length(Directives)),

    %% Verify types in order
    Types = [element(2, D) || D <- Directives],
    ?assertEqual([allocate, stage_in, stage_out], Types).

test_parse_no_directives() ->
    Script = <<"#!/bin/bash\necho hello\n#SBATCH -N 2\n">>,
    {ok, Directives} = flurm_burst_buffer:parse_directives(Script),
    ?assertEqual([], Directives).

test_parse_bb_spec() ->
    %% Test various spec formats
    Spec1 = <<"pool=default capacity=100MB access=striped type=scratch">>,
    {ok, Req1} = flurm_burst_buffer:parse_bb_spec(Spec1),
    ?assertEqual(<<"default">>, element(2, Req1)),       % pool
    ?assertEqual(100 * 1024 * 1024, element(3, Req1)),   % size
    ?assertEqual(striped, element(4, Req1)),             % access
    ?assertEqual(scratch, element(5, Req1)),             % type

    %% With #BB prefix
    Spec2 = <<"#BB capacity=1GB">>,
    {ok, Req2} = flurm_burst_buffer:parse_bb_spec(Spec2),
    ?assertEqual(1024 * 1024 * 1024, element(3, Req2)),

    %% With persistent flag
    Spec3 = <<"capacity=500MB persistent name=mybb">>,
    {ok, Req3} = flurm_burst_buffer:parse_bb_spec(Spec3),
    ?assertEqual(true, element(8, Req3)),                % persistent
    ?assertEqual(<<"mybb">>, element(9, Req3)).          % persistent_name

test_format_bb_spec() ->
    %% Create a request record structure matching #bb_request{}
    %% {bb_request, pool, size, access, type, stage_in, stage_out, persistent, persistent_name}
    Request = {bb_request, <<"nvme">>, 1024 * 1024 * 1024, striped, scratch, [], [], false, undefined},

    Formatted = flurm_burst_buffer:format_bb_spec(Request),
    ?assert(is_binary(Formatted)),
    ?assertMatch(<<_/binary>>, Formatted),
    ?assert(binary:match(Formatted, <<"pool=nvme">>) =/= nomatch),
    ?assert(binary:match(Formatted, <<"capacity=1GB">>) =/= nomatch).
