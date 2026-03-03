%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_db_ra API wrapper/error branches.
%%%-------------------------------------------------------------------
-module(flurm_db_ra_api_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

setup_meck() ->
    catch meck:unload(ra),
    meck:new(ra, [non_strict, no_link]),
    ok.

cleanup_meck(_) ->
    catch meck:unload(ra),
    ok.

start_cluster_branches_test_() ->
    {foreach,
        fun setup_meck/0,
        fun cleanup_meck/1,
        [
            {"start_cluster success", fun() ->
                meck:expect(ra, start_cluster, fun(default, ?RA_CLUSTER_NAME, {module, flurm_db_ra, #{}}, Servers) ->
                    {ok, [hd(Servers)], []}
                end),
                ?assertEqual(ok, flurm_db_ra:start_cluster([node()]))
            end},
            {"start_cluster no servers", fun() ->
                meck:expect(ra, start_cluster, fun(_, _, _, _) ->
                    {ok, [], [not_started]}
                end),
                ?assertEqual({error, {no_servers_started, [not_started]}},
                    flurm_db_ra:start_cluster([node()]))
            end},
            {"start_cluster error", fun() ->
                meck:expect(ra, start_cluster, fun(_, _, _, _) ->
                    {error, startup_failed}
                end),
                ?assertEqual({error, startup_failed}, flurm_db_ra:start_cluster([node()]))
            end}
        ]
    }.

command_wrapper_branches_test_() ->
    {foreach,
        fun setup_meck/0,
        fun cleanup_meck/1,
        [
            {"store_job wrapper", fun() ->
                meck:expect(ra, process_command, fun(_, {store_job, 1, _}, _) ->
                    {ok, {ok, 1}, leader}
                end),
                ?assertEqual({ok, 1}, flurm_db_ra:store_job(1, test_job_spec()))
            end},
            {"allocate_job timeout branch", fun() ->
                meck:expect(ra, process_command, fun(_, {allocate_job, 1, _}, _) ->
                    {timeout, leader}
                end),
                ?assertEqual({error, timeout}, flurm_db_ra:allocate_job(1, [<<"n1">>]))
            end},
            {"set_job_exit_code error branch", fun() ->
                meck:expect(ra, process_command, fun(_, {set_job_exit_code, 1, 1}, _) ->
                    {error, denied}
                end),
                ?assertEqual({error, denied}, flurm_db_ra:set_job_exit_code(1, 1))
            end},
            {"update_job_fields wrapper", fun() ->
                meck:expect(ra, process_command, fun(_, {update_job_fields, 1, _}, _) ->
                    {ok, ok, leader}
                end),
                ?assertEqual(ok, flurm_db_ra:update_job_fields(1, #{priority => 200}))
            end},
            {"allocate_job_id wrapper", fun() ->
                meck:expect(ra, process_command, fun(_, allocate_job_id, _) ->
                    {ok, {ok, 99}, leader}
                end),
                ?assertEqual({ok, 99}, flurm_db_ra:allocate_job_id())
            end}
        ]
    }.

local_query_wrapper_branches_test_() ->
    {foreach,
        fun setup_meck/0,
        fun cleanup_meck/1,
        [
            {"get_job not_found branch", fun() ->
                meck:expect(ra, local_query, fun(_, _, _) ->
                    {ok, {1, error}, leader}
                end),
                ?assertEqual({error, not_found}, flurm_db_ra:get_job(1))
            end},
            {"get_job passthrough branch", fun() ->
                meck:expect(ra, local_query, fun(_, _, _) ->
                    {error, bad_query}
                end),
                ?assertEqual({error, bad_query}, flurm_db_ra:get_job(1))
            end},
            {"get_node timeout branch", fun() ->
                meck:expect(ra, local_query, fun(_, _, _) ->
                    {timeout, leader}
                end),
                ?assertEqual({error, timeout}, flurm_db_ra:get_node(<<"n1">>))
            end},
            {"get_partition error branch", fun() ->
                meck:expect(ra, local_query, fun(_, _, _) ->
                    {error, bad_query}
                end),
                ?assertEqual({error, bad_query}, flurm_db_ra:get_partition(<<"p1">>))
            end},
            {"list_partitions executes query fun", fun() ->
                Partition = test_partition(<<"p1">>, []),
                State = test_state(#{}, #{}, #{<<"p1">> => Partition}),
                meck:expect(ra, local_query, fun(_, QueryFun, _) ->
                    {ok, {1, QueryFun(State)}, leader}
                end),
                {ok, Parts} = flurm_db_ra:list_partitions(),
                ?assertEqual(1, length(Parts))
            end},
            {"get_nodes_in_partition found", fun() ->
                Node = test_node(<<"n1">>),
                Partition = test_partition(<<"p1">>, [<<"n1">>, <<"missing">>]),
                State = test_state(#{}, #{<<"n1">> => Node}, #{<<"p1">> => Partition}),
                meck:expect(ra, local_query, fun(_, QueryFun, _) ->
                    {ok, {1, QueryFun(State)}, leader}
                end),
                {ok, Nodes} = flurm_db_ra:get_nodes_in_partition(<<"p1">>),
                ?assertEqual(1, length(Nodes))
            end},
            {"get_nodes_in_partition not found", fun() ->
                State = test_state(#{}, #{}, #{}),
                meck:expect(ra, local_query, fun(_, QueryFun, _) ->
                    {ok, {1, QueryFun(State)}, leader}
                end),
                ?assertEqual({error, not_found}, flurm_db_ra:get_nodes_in_partition(<<"none">>))
            end}
        ]
    }.

consistent_query_wrapper_branches_test_() ->
    {foreach,
        fun setup_meck/0,
        fun cleanup_meck/1,
        [
            {"consistent_get_job success", fun() ->
                Job = test_job(1),
                State = test_state(#{1 => Job}, #{}, #{}),
                meck:expect(ra, consistent_query, fun(_, QueryFun, _) ->
                    {ok, {1, QueryFun(State)}, leader}
                end),
                ?assertEqual({ok, Job}, flurm_db_ra:consistent_get_job(1))
            end},
            {"consistent_list_jobs success", fun() ->
                Job = test_job(1),
                State = test_state(#{1 => Job}, #{}, #{}),
                meck:expect(ra, consistent_query, fun(_, QueryFun, _) ->
                    {ok, {1, QueryFun(State)}, leader}
                end),
                {ok, Jobs} = flurm_db_ra:consistent_list_jobs(),
                ?assertEqual(1, length(Jobs))
            end},
            {"consistent_get_job timeout", fun() ->
                meck:expect(ra, consistent_query, fun(_, _, _) ->
                    {timeout, leader}
                end),
                ?assertEqual({error, timeout}, flurm_db_ra:consistent_get_job(1))
            end},
            {"consistent_list_jobs error", fun() ->
                meck:expect(ra, consistent_query, fun(_, _, _) ->
                    {error, unavailable}
                end),
                ?assertEqual({error, unavailable}, flurm_db_ra:consistent_list_jobs())
            end}
        ]
    }.

test_job_spec() ->
    #ra_job_spec{
        name = <<"job">>,
        user = <<"user">>,
        group = <<"group">>,
        partition = <<"batch">>,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        num_tasks = 1,
        cpus_per_task = 1,
        memory_mb = 512,
        time_limit = 60,
        priority = 100
    }.

test_job(Id) ->
    #ra_job{
        id = Id,
        name = <<"job">>,
        user = <<"user">>,
        group = <<"group">>,
        partition = <<"batch">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        num_tasks = 1,
        cpus_per_task = 1,
        memory_mb = 512,
        time_limit = 60,
        priority = 100,
        submit_time = 0,
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined,
        array_job_id = 0,
        array_task_id = undefined
    }.

test_node(Name) ->
    #ra_node{
        name = Name,
        hostname = <<"host">>,
        port = 6817,
        cpus = 1,
        cpus_used = 0,
        memory_mb = 1024,
        memory_used = 0,
        gpus = 0,
        gpus_used = 0,
        state = up,
        features = [],
        partitions = [<<"batch">>],
        running_jobs = [],
        last_heartbeat = 0
    }.

test_partition(Name, Nodes) ->
    #ra_partition{
        name = Name,
        state = up,
        nodes = Nodes,
        max_time = 60,
        default_time = 60,
        max_nodes = 1,
        priority = 1
    }.

test_state(Jobs, Nodes, Parts) ->
    #ra_state{
        jobs = Jobs,
        nodes = Nodes,
        partitions = Parts,
        job_counter = 1,
        version = 1
    }.
