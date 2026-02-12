%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_db Ra-backed API
%%%
%%% Covers the high-level wrapper functions in flurm_db that delegate
%%% to flurm_db_ra and flurm_db_cluster. Uses meck to mock the
%%% underlying Ra modules.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_db_wrapper_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_wrapper_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

db_wrapper_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Job operations
        {"submit_job delegates to flurm_db_ra",
         fun test_submit_job/0},
        {"cancel_job delegates to flurm_db_ra",
         fun test_cancel_job/0},
        {"get_job/1 local read",
         fun test_get_job_local/0},
        {"get_job/2 local read",
         fun test_get_job_2_local/0},
        {"get_job/2 consistent read",
         fun test_get_job_2_consistent/0},
        {"list_jobs/0 local read",
         fun test_list_jobs/0},
        {"list_jobs/1 local read",
         fun test_list_jobs_1_local/0},
        {"list_jobs/1 consistent read",
         fun test_list_jobs_1_consistent/0},
        {"list_pending_jobs delegates",
         fun test_list_pending_jobs/0},
        {"list_running_jobs delegates",
         fun test_list_running_jobs/0},
        {"allocate_job delegates",
         fun test_allocate_job/0},
        {"complete_job delegates to set_job_exit_code",
         fun test_complete_job/0},

        %% Node operations
        {"register_node delegates",
         fun test_register_node/0},
        {"unregister_node delegates",
         fun test_unregister_node/0},
        {"update_node_state delegates",
         fun test_update_node_state/0},
        {"get_node/1 local read",
         fun test_get_node_local/0},
        {"get_node/2 local and consistent",
         fun test_get_node_2/0},
        {"list_nodes/0 delegates",
         fun test_list_nodes/0},
        {"list_nodes/1 by state",
         fun test_list_nodes_1/0},
        {"list_available_nodes delegates",
         fun test_list_available_nodes/0},
        {"list_nodes_in_partition delegates",
         fun test_list_nodes_in_partition/0},

        %% Partition operations
        {"create_partition delegates",
         fun test_create_partition/0},
        {"delete_partition delegates",
         fun test_delete_partition/0},
        {"get_partition delegates",
         fun test_get_partition/0},
        {"list_partitions delegates",
         fun test_list_partitions/0},

        %% Cluster operations
        {"cluster_status delegates",
         fun test_cluster_status/0},
        {"is_leader delegates",
         fun test_is_leader/0},
        {"get_leader delegates",
         fun test_get_leader/0}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    catch meck:unload(flurm_db_ra),
    catch meck:unload(flurm_db_cluster),
    meck:new(flurm_db_ra, [non_strict]),
    meck:new(flurm_db_cluster, [non_strict]),
    ok.

cleanup(_) ->
    catch meck:unload(flurm_db_ra),
    catch meck:unload(flurm_db_cluster),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

make_job_spec() ->
    #ra_job_spec{
        name = <<"test">>,
        user = <<"user">>,
        group = <<"group">>,
        partition = <<"default">>,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 60,
        priority = 100
    }.

make_job(Id) ->
    #ra_job{
        id = Id,
        name = <<"test">>,
        user = <<"user">>,
        group = <<"group">>,
        partition = <<"default">>,
        state = pending,
        script = <<"script">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 60,
        priority = 100,
        submit_time = 1000,
        allocated_nodes = []
    }.

make_node_spec() ->
    #ra_node_spec{
        name = <<"node1">>,
        hostname = <<"node1.example.com">>,
        port = 7000,
        cpus = 8,
        memory_mb = 16384,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    }.

make_node() ->
    #ra_node{
        name = <<"node1">>,
        hostname = <<"node1.example.com">>,
        port = 7000,
        cpus = 8,
        cpus_used = 0,
        memory_mb = 16384,
        memory_used = 0,
        gpus = 0,
        gpus_used = 0,
        state = up,
        features = [],
        partitions = [<<"default">>],
        running_jobs = [],
        last_heartbeat = 1000
    }.

make_partition_spec() ->
    #ra_partition_spec{
        name = <<"batch">>,
        nodes = [<<"n1">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 1
    }.

make_partition() ->
    #ra_partition{
        name = <<"batch">>,
        state = up,
        nodes = [<<"n1">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 1
    }.

%%====================================================================
%% Tests - Job operations
%%====================================================================

test_submit_job() ->
    meck:expect(flurm_db_ra, submit_job, fun(#ra_job_spec{}) -> {ok, 1} end),
    ?assertEqual({ok, 1}, flurm_db:submit_job(make_job_spec())).

test_cancel_job() ->
    meck:expect(flurm_db_ra, cancel_job, fun(1) -> ok end),
    ?assertEqual(ok, flurm_db:cancel_job(1)).

test_get_job_local() ->
    Job = make_job(1),
    meck:expect(flurm_db_ra, get_job, fun(1) -> {ok, Job} end),
    ?assertEqual({ok, Job}, flurm_db:get_job(1)).

test_get_job_2_local() ->
    Job = make_job(2),
    meck:expect(flurm_db_ra, get_job, fun(2) -> {ok, Job} end),
    ?assertEqual({ok, Job}, flurm_db:get_job(2, local)).

test_get_job_2_consistent() ->
    Job = make_job(3),
    meck:expect(flurm_db_ra, consistent_get_job, fun(3) -> {ok, Job} end),
    ?assertEqual({ok, Job}, flurm_db:get_job(3, consistent)).

test_list_jobs() ->
    Jobs = [make_job(1), make_job(2)],
    meck:expect(flurm_db_ra, list_jobs, fun() -> {ok, Jobs} end),
    ?assertEqual({ok, Jobs}, flurm_db:list_jobs()).

test_list_jobs_1_local() ->
    Jobs = [make_job(1)],
    meck:expect(flurm_db_ra, list_jobs, fun() -> {ok, Jobs} end),
    ?assertEqual({ok, Jobs}, flurm_db:list_jobs(local)).

test_list_jobs_1_consistent() ->
    Jobs = [make_job(1), make_job(2)],
    meck:expect(flurm_db_ra, consistent_list_jobs, fun() -> {ok, Jobs} end),
    ?assertEqual({ok, Jobs}, flurm_db:list_jobs(consistent)).

test_list_pending_jobs() ->
    Jobs = [make_job(1)],
    meck:expect(flurm_db_ra, get_jobs_by_state, fun(pending) -> {ok, Jobs} end),
    ?assertEqual({ok, Jobs}, flurm_db:list_pending_jobs()).

test_list_running_jobs() ->
    Job = (make_job(1))#ra_job{state = running},
    meck:expect(flurm_db_ra, get_jobs_by_state, fun(running) -> {ok, [Job]} end),
    ?assertEqual({ok, [Job]}, flurm_db:list_running_jobs()).

test_allocate_job() ->
    meck:expect(flurm_db_ra, allocate_job, fun(1, [<<"n1">>]) -> ok end),
    ?assertEqual(ok, flurm_db:allocate_job(1, [<<"n1">>])).

test_complete_job() ->
    meck:expect(flurm_db_ra, set_job_exit_code, fun(1, 0) -> ok end),
    ?assertEqual(ok, flurm_db:complete_job(1, 0)).

%%====================================================================
%% Tests - Node operations
%%====================================================================

test_register_node() ->
    meck:expect(flurm_db_ra, register_node, fun(#ra_node_spec{}) -> {ok, registered} end),
    ?assertEqual({ok, registered}, flurm_db:register_node(make_node_spec())).

test_unregister_node() ->
    meck:expect(flurm_db_ra, unregister_node, fun(<<"node1">>) -> ok end),
    ?assertEqual(ok, flurm_db:unregister_node(<<"node1">>)).

test_update_node_state() ->
    meck:expect(flurm_db_ra, update_node_state, fun(<<"node1">>, drain) -> ok end),
    ?assertEqual(ok, flurm_db:update_node_state(<<"node1">>, drain)).

test_get_node_local() ->
    Node = make_node(),
    meck:expect(flurm_db_ra, get_node, fun(<<"node1">>) -> {ok, Node} end),
    ?assertEqual({ok, Node}, flurm_db:get_node(<<"node1">>)).

test_get_node_2() ->
    Node = make_node(),
    meck:expect(flurm_db_ra, get_node, fun(<<"node1">>) -> {ok, Node} end),
    %% Both local and consistent currently use local read
    ?assertEqual({ok, Node}, flurm_db:get_node(<<"node1">>, local)),
    ?assertEqual({ok, Node}, flurm_db:get_node(<<"node1">>, consistent)).

test_list_nodes() ->
    Nodes = [make_node()],
    meck:expect(flurm_db_ra, list_nodes, fun() -> {ok, Nodes} end),
    ?assertEqual({ok, Nodes}, flurm_db:list_nodes()).

test_list_nodes_1() ->
    Node = (make_node())#ra_node{state = drain},
    meck:expect(flurm_db_ra, get_nodes_by_state, fun(drain) -> {ok, [Node]} end),
    ?assertEqual({ok, [Node]}, flurm_db:list_nodes(drain)).

test_list_available_nodes() ->
    Nodes = [make_node()],
    meck:expect(flurm_db_ra, get_nodes_by_state, fun(up) -> {ok, Nodes} end),
    ?assertEqual({ok, Nodes}, flurm_db:list_available_nodes()).

test_list_nodes_in_partition() ->
    Nodes = [make_node()],
    meck:expect(flurm_db_ra, get_nodes_in_partition, fun(<<"default">>) -> {ok, Nodes} end),
    ?assertEqual({ok, Nodes}, flurm_db:list_nodes_in_partition(<<"default">>)).

%%====================================================================
%% Tests - Partition operations
%%====================================================================

test_create_partition() ->
    meck:expect(flurm_db_ra, create_partition, fun(#ra_partition_spec{}) -> ok end),
    ?assertEqual(ok, flurm_db:create_partition(make_partition_spec())).

test_delete_partition() ->
    meck:expect(flurm_db_ra, delete_partition, fun(<<"batch">>) -> ok end),
    ?assertEqual(ok, flurm_db:delete_partition(<<"batch">>)).

test_get_partition() ->
    Part = make_partition(),
    meck:expect(flurm_db_ra, get_partition, fun(<<"batch">>) -> {ok, Part} end),
    ?assertEqual({ok, Part}, flurm_db:get_partition(<<"batch">>)).

test_list_partitions() ->
    Parts = [make_partition()],
    meck:expect(flurm_db_ra, list_partitions, fun() -> {ok, Parts} end),
    ?assertEqual({ok, Parts}, flurm_db:list_partitions()).

%%====================================================================
%% Tests - Cluster operations
%%====================================================================

test_cluster_status() ->
    Status = #{state => leader, members => 3},
    meck:expect(flurm_db_cluster, status, fun() -> {ok, Status} end),
    ?assertEqual({ok, Status}, flurm_db:cluster_status()).

test_is_leader() ->
    meck:expect(flurm_db_cluster, is_leader, fun() -> true end),
    ?assertEqual(true, flurm_db:is_leader()).

test_get_leader() ->
    meck:expect(flurm_db_cluster, get_leader, fun() -> {ok, {flurm_db_ra, node()}} end),
    ?assertEqual({ok, {flurm_db_ra, node()}}, flurm_db:get_leader()).
