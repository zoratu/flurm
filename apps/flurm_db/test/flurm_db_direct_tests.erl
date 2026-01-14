%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_db module
%%%
%%% These tests call flurm_db functions directly to get code coverage.
%%% External dependencies (flurm_db_ra, flurm_db_cluster) are mocked
%%% but the module under test (flurm_db) is NOT mocked.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_db_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Legacy ETS API tests", fun legacy_ets_api_test/0},
      {"Table name conversion tests", fun table_name_test/0},
      {"Init ensures tables test", fun init_ensures_tables_test/0}
     ]}.

flurm_db_ra_api_test_() ->
    {setup,
     fun setup_with_ra_mock/0,
     fun cleanup_with_ra_mock/1,
     [
      {"Submit job delegates to Ra", fun submit_job_test/0},
      {"Cancel job delegates to Ra", fun cancel_job_test/0},
      {"Get job delegates to Ra", fun get_job_test/0},
      {"Get job with consistency delegates to Ra", fun get_job_consistency_test/0},
      {"List jobs delegates to Ra", fun list_jobs_test/0},
      {"List jobs with consistency delegates to Ra", fun list_jobs_consistency_test/0},
      {"List pending jobs delegates to Ra", fun list_pending_jobs_test/0},
      {"List running jobs delegates to Ra", fun list_running_jobs_test/0},
      {"Allocate job delegates to Ra", fun allocate_job_test/0},
      {"Complete job delegates to Ra", fun complete_job_test/0},
      {"Register node delegates to Ra", fun register_node_test/0},
      {"Unregister node delegates to Ra", fun unregister_node_test/0},
      {"Update node state delegates to Ra", fun update_node_state_test/0},
      {"Get node delegates to Ra", fun get_node_test/0},
      {"Get node with consistency delegates to Ra", fun get_node_consistency_test/0},
      {"List nodes delegates to Ra", fun list_nodes_test/0},
      {"List nodes by state delegates to Ra", fun list_nodes_by_state_test/0},
      {"List available nodes delegates to Ra", fun list_available_nodes_test/0},
      {"List nodes in partition delegates to Ra", fun list_nodes_in_partition_test/0},
      {"Create partition delegates to Ra", fun create_partition_test/0},
      {"Delete partition delegates to Ra", fun delete_partition_test/0},
      {"Get partition delegates to Ra", fun get_partition_test/0},
      {"List partitions delegates to Ra", fun list_partitions_test/0}
     ]}.

flurm_db_cluster_api_test_() ->
    {setup,
     fun setup_with_cluster_mock/0,
     fun cleanup_with_cluster_mock/1,
     [
      {"Cluster status delegates to cluster module", fun cluster_status_test/0},
      {"Is leader delegates to cluster module", fun is_leader_test/0},
      {"Get leader delegates to cluster module", fun get_leader_test/0}
     ]}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup() ->
    ok.

cleanup(_) ->
    %% Clean up any ETS tables created during tests
    lists:foreach(fun(Table) ->
        case ets:whereis(Table) of
            undefined -> ok;
            _ -> catch ets:delete(Table)
        end
    end, [flurm_db_jobs, flurm_db_nodes, flurm_db_partitions,
          flurm_db_test_table]).

setup_with_ra_mock() ->
    meck:new(flurm_db_ra, [passthrough]),
    ok.

cleanup_with_ra_mock(_) ->
    meck:unload(flurm_db_ra),
    cleanup(ok).

setup_with_cluster_mock() ->
    meck:new(flurm_db_cluster, [passthrough]),
    ok.

cleanup_with_cluster_mock(_) ->
    meck:unload(flurm_db_cluster),
    ok.

%%====================================================================
%% Legacy ETS API Tests
%%====================================================================

legacy_ets_api_test() ->
    %% Test init/0
    ok = flurm_db:init(),

    %% Test put/3
    ok = flurm_db:put(jobs, 1, #{name => <<"test_job">>}),
    ok = flurm_db:put(nodes, <<"node1">>, #{cpus => 4}),
    ok = flurm_db:put(partitions, <<"batch">>, #{max_time => 3600}),

    %% Test get/2 - existing key
    {ok, #{name := <<"test_job">>}} = flurm_db:get(jobs, 1),
    {ok, #{cpus := 4}} = flurm_db:get(nodes, <<"node1">>),
    {ok, #{max_time := 3600}} = flurm_db:get(partitions, <<"batch">>),

    %% Test get/2 - non-existing key
    {error, not_found} = flurm_db:get(jobs, 999),

    %% Test list/1
    [#{name := <<"test_job">>}] = flurm_db:list(jobs),

    %% Test list_keys/1
    [1] = flurm_db:list_keys(jobs),
    [<<"node1">>] = flurm_db:list_keys(nodes),

    %% Test delete/2
    ok = flurm_db:delete(jobs, 1),
    {error, not_found} = flurm_db:get(jobs, 1),
    [] = flurm_db:list(jobs),

    ok.

table_name_test() ->
    %% Test custom table name conversion
    ok = flurm_db:put(test_table, key1, value1),
    {ok, value1} = flurm_db:get(test_table, key1),
    ok = flurm_db:delete(test_table, key1),
    ok.

init_ensures_tables_test() ->
    %% Clean up any existing tables
    lists:foreach(fun(Table) ->
        case ets:whereis(Table) of
            undefined -> ok;
            _ -> catch ets:delete(Table)
        end
    end, [flurm_db_jobs, flurm_db_nodes, flurm_db_partitions]),

    %% Init should create tables
    ok = flurm_db:init(),

    %% Verify tables exist
    ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_nodes)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_partitions)),

    ok.

%%====================================================================
%% Job Operations Tests (Ra-backed)
%%====================================================================

submit_job_test() ->
    JobSpec = #ra_job_spec{
        name = <<"test">>,
        user = <<"user1">>,
        group = <<"users">>,
        partition = <<"batch">>,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100
    },
    meck:expect(flurm_db_ra, submit_job, fun(_) -> {ok, 1} end),
    {ok, 1} = flurm_db:submit_job(JobSpec),
    ?assert(meck:called(flurm_db_ra, submit_job, [JobSpec])).

cancel_job_test() ->
    meck:expect(flurm_db_ra, cancel_job, fun(1) -> ok end),
    ok = flurm_db:cancel_job(1),
    ?assert(meck:called(flurm_db_ra, cancel_job, [1])).

get_job_test() ->
    MockJob = #ra_job{id = 1, name = <<"test">>, user = <<"user1">>,
                      group = <<"users">>, partition = <<"batch">>,
                      state = pending, script = <<"#!/bin/bash">>,
                      num_nodes = 1, num_cpus = 4, memory_mb = 1024,
                      time_limit = 3600, priority = 100,
                      submit_time = 1234567890, allocated_nodes = []},
    meck:expect(flurm_db_ra, get_job, fun(1) -> {ok, MockJob} end),
    {ok, MockJob} = flurm_db:get_job(1),
    ?assert(meck:called(flurm_db_ra, get_job, [1])).

get_job_consistency_test() ->
    MockJob = #ra_job{id = 1, name = <<"test">>, user = <<"user1">>,
                      group = <<"users">>, partition = <<"batch">>,
                      state = pending, script = <<"#!/bin/bash">>,
                      num_nodes = 1, num_cpus = 4, memory_mb = 1024,
                      time_limit = 3600, priority = 100,
                      submit_time = 1234567890, allocated_nodes = []},

    %% Test local consistency
    meck:expect(flurm_db_ra, get_job, fun(1) -> {ok, MockJob} end),
    {ok, MockJob} = flurm_db:get_job(1, local),

    %% Test consistent read
    meck:expect(flurm_db_ra, consistent_get_job, fun(1) -> {ok, MockJob} end),
    {ok, MockJob} = flurm_db:get_job(1, consistent),
    ?assert(meck:called(flurm_db_ra, consistent_get_job, [1])).

list_jobs_test() ->
    meck:expect(flurm_db_ra, list_jobs, fun() -> {ok, []} end),
    {ok, []} = flurm_db:list_jobs(),
    ?assert(meck:called(flurm_db_ra, list_jobs, [])).

list_jobs_consistency_test() ->
    %% Test local
    meck:expect(flurm_db_ra, list_jobs, fun() -> {ok, []} end),
    {ok, []} = flurm_db:list_jobs(local),

    %% Test consistent
    meck:expect(flurm_db_ra, consistent_list_jobs, fun() -> {ok, []} end),
    {ok, []} = flurm_db:list_jobs(consistent),
    ?assert(meck:called(flurm_db_ra, consistent_list_jobs, [])).

list_pending_jobs_test() ->
    meck:expect(flurm_db_ra, get_jobs_by_state, fun(pending) -> {ok, []} end),
    {ok, []} = flurm_db:list_pending_jobs(),
    ?assert(meck:called(flurm_db_ra, get_jobs_by_state, [pending])).

list_running_jobs_test() ->
    meck:expect(flurm_db_ra, get_jobs_by_state, fun(running) -> {ok, []} end),
    {ok, []} = flurm_db:list_running_jobs(),
    ?assert(meck:called(flurm_db_ra, get_jobs_by_state, [running])).

allocate_job_test() ->
    meck:expect(flurm_db_ra, allocate_job, fun(1, [<<"node1">>]) -> ok end),
    ok = flurm_db:allocate_job(1, [<<"node1">>]),
    ?assert(meck:called(flurm_db_ra, allocate_job, [1, [<<"node1">>]])).

complete_job_test() ->
    meck:expect(flurm_db_ra, set_job_exit_code, fun(1, 0) -> ok end),
    ok = flurm_db:complete_job(1, 0),
    ?assert(meck:called(flurm_db_ra, set_job_exit_code, [1, 0])).

%%====================================================================
%% Node Operations Tests (Ra-backed)
%%====================================================================

register_node_test() ->
    NodeSpec = #ra_node_spec{
        name = <<"node1">>,
        hostname = <<"node1.example.com">>,
        port = 6818,
        cpus = 16,
        memory_mb = 32768,
        gpus = 2,
        features = [gpu, fast],
        partitions = [<<"batch">>]
    },
    meck:expect(flurm_db_ra, register_node, fun(_) -> {ok, registered} end),
    {ok, registered} = flurm_db:register_node(NodeSpec),
    ?assert(meck:called(flurm_db_ra, register_node, [NodeSpec])).

unregister_node_test() ->
    meck:expect(flurm_db_ra, unregister_node, fun(<<"node1">>) -> ok end),
    ok = flurm_db:unregister_node(<<"node1">>),
    ?assert(meck:called(flurm_db_ra, unregister_node, [<<"node1">>])).

update_node_state_test() ->
    meck:expect(flurm_db_ra, update_node_state, fun(<<"node1">>, drain) -> ok end),
    ok = flurm_db:update_node_state(<<"node1">>, drain),
    ?assert(meck:called(flurm_db_ra, update_node_state, [<<"node1">>, drain])).

get_node_test() ->
    MockNode = #ra_node{name = <<"node1">>, hostname = <<"node1.example.com">>,
                        port = 6818, cpus = 16, cpus_used = 0,
                        memory_mb = 32768, memory_used = 0,
                        gpus = 2, gpus_used = 0, state = up,
                        features = [], partitions = [], running_jobs = [],
                        last_heartbeat = 1234567890},
    meck:expect(flurm_db_ra, get_node, fun(<<"node1">>) -> {ok, MockNode} end),
    {ok, MockNode} = flurm_db:get_node(<<"node1">>),
    ?assert(meck:called(flurm_db_ra, get_node, [<<"node1">>])).

get_node_consistency_test() ->
    MockNode = #ra_node{name = <<"node1">>, hostname = <<"node1.example.com">>,
                        port = 6818, cpus = 16, cpus_used = 0,
                        memory_mb = 32768, memory_used = 0,
                        gpus = 2, gpus_used = 0, state = up,
                        features = [], partitions = [], running_jobs = [],
                        last_heartbeat = 1234567890},

    %% Test local
    meck:expect(flurm_db_ra, get_node, fun(<<"node1">>) -> {ok, MockNode} end),
    {ok, MockNode} = flurm_db:get_node(<<"node1">>, local),

    %% Test consistent (currently uses local)
    {ok, MockNode} = flurm_db:get_node(<<"node1">>, consistent).

list_nodes_test() ->
    meck:expect(flurm_db_ra, list_nodes, fun() -> {ok, []} end),
    {ok, []} = flurm_db:list_nodes(),
    ?assert(meck:called(flurm_db_ra, list_nodes, [])).

list_nodes_by_state_test() ->
    meck:expect(flurm_db_ra, get_nodes_by_state, fun(up) -> {ok, []} end),
    {ok, []} = flurm_db:list_nodes(up),
    ?assert(meck:called(flurm_db_ra, get_nodes_by_state, [up])).

list_available_nodes_test() ->
    meck:expect(flurm_db_ra, get_nodes_by_state, fun(up) -> {ok, []} end),
    {ok, []} = flurm_db:list_available_nodes(),
    ?assert(meck:called(flurm_db_ra, get_nodes_by_state, [up])).

list_nodes_in_partition_test() ->
    meck:expect(flurm_db_ra, get_nodes_in_partition, fun(<<"batch">>) -> {ok, []} end),
    {ok, []} = flurm_db:list_nodes_in_partition(<<"batch">>),
    ?assert(meck:called(flurm_db_ra, get_nodes_in_partition, [<<"batch">>])).

%%====================================================================
%% Partition Operations Tests (Ra-backed)
%%====================================================================

create_partition_test() ->
    PartSpec = #ra_partition_spec{
        name = <<"batch">>,
        nodes = [<<"node1">>, <<"node2">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 1
    },
    meck:expect(flurm_db_ra, create_partition, fun(_) -> ok end),
    ok = flurm_db:create_partition(PartSpec),
    ?assert(meck:called(flurm_db_ra, create_partition, [PartSpec])).

delete_partition_test() ->
    meck:expect(flurm_db_ra, delete_partition, fun(<<"batch">>) -> ok end),
    ok = flurm_db:delete_partition(<<"batch">>),
    ?assert(meck:called(flurm_db_ra, delete_partition, [<<"batch">>])).

get_partition_test() ->
    MockPart = #ra_partition{name = <<"batch">>, state = up,
                             nodes = [<<"node1">>], max_time = 86400,
                             default_time = 3600, max_nodes = 10, priority = 1},
    meck:expect(flurm_db_ra, get_partition, fun(<<"batch">>) -> {ok, MockPart} end),
    {ok, MockPart} = flurm_db:get_partition(<<"batch">>),
    ?assert(meck:called(flurm_db_ra, get_partition, [<<"batch">>])).

list_partitions_test() ->
    meck:expect(flurm_db_ra, list_partitions, fun() -> {ok, []} end),
    {ok, []} = flurm_db:list_partitions(),
    ?assert(meck:called(flurm_db_ra, list_partitions, [])).

%%====================================================================
%% Cluster Operations Tests
%%====================================================================

cluster_status_test() ->
    meck:expect(flurm_db_cluster, status, fun() -> {ok, #{state => leader}} end),
    {ok, #{state := leader}} = flurm_db:cluster_status(),
    ?assert(meck:called(flurm_db_cluster, status, [])).

is_leader_test() ->
    meck:expect(flurm_db_cluster, is_leader, fun() -> true end),
    true = flurm_db:is_leader(),
    ?assert(meck:called(flurm_db_cluster, is_leader, [])).

get_leader_test() ->
    meck:expect(flurm_db_cluster, get_leader, fun() -> {ok, {flurm_db_ra, node()}} end),
    {ok, {flurm_db_ra, _}} = flurm_db:get_leader(),
    ?assert(meck:called(flurm_db_cluster, get_leader, [])).
