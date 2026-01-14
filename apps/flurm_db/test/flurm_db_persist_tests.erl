%%%-------------------------------------------------------------------
%%% @doc FLURM Database Persistence Layer Tests
%%%
%%% Tests for the flurm_db_persist module which provides job persistence
%%% with Ra and ETS fallback.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_persist_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Create ETS tables for fallback storage
    case ets:whereis(flurm_db_jobs_ets) of
        undefined ->
            ets:new(flurm_db_jobs_ets, [named_table, public, set, {read_concurrency, true}]);
        _ ->
            ets:delete_all_objects(flurm_db_jobs_ets)
    end,
    case ets:whereis(flurm_db_job_counter_ets) of
        undefined ->
            ets:new(flurm_db_job_counter_ets, [named_table, public, set]);
        _ ->
            ok
    end,
    ets:insert(flurm_db_job_counter_ets, {counter, 0}),
    ok.

cleanup(_) ->
    catch ets:delete_all_objects(flurm_db_jobs_ets),
    catch ets:insert(flurm_db_job_counter_ets, {counter, 0}),
    ok.

%%====================================================================
%% ETS Backend Tests
%%====================================================================

ets_backend_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"Test is_ra_available", fun test_is_ra_available/0},
         {"Test persistence_mode", fun test_persistence_mode/0},
         {"Test store_job ETS", fun test_store_job_ets/0},
         {"Test get_job ETS", fun test_get_job_ets/0},
         {"Test update_job ETS", fun test_update_job_ets/0},
         {"Test delete_job ETS", fun test_delete_job_ets/0},
         {"Test list_jobs ETS", fun test_list_jobs_ets/0},
         {"Test next_job_id ETS", fun test_next_job_id_ets/0},
         {"Test apply_job_updates state transitions", fun test_apply_job_updates/0}
     ]
    }.

test_is_ra_available() ->
    %% In non-distributed mode, Ra should not be available
    case node() of
        nonode@nohost ->
            ?assertEqual(false, flurm_db_persist:is_ra_available());
        _ ->
            %% With distribution but no Ra cluster, should still be false
            %% unless Ra is actually running
            Result = flurm_db_persist:is_ra_available(),
            ?assert(is_boolean(Result))
    end,
    ok.

test_persistence_mode() ->
    Mode = flurm_db_persist:persistence_mode(),
    ?assert(Mode =:= ra orelse Mode =:= ets orelse Mode =:= none),
    ok.

test_store_job_ets() ->
    Job = #job{
        id = 1,
        name = <<"test_job">>,
        user = <<"testuser">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined
    },
    Result = flurm_db_persist:store_job(Job),
    ?assertEqual(ok, Result),
    %% Verify it was stored
    [{1, StoredJob}] = ets:lookup(flurm_db_jobs_ets, 1),
    ?assertEqual(<<"test_job">>, StoredJob#job.name),
    ok.

test_get_job_ets() ->
    Job = #job{
        id = 2,
        name = <<"get_test">>,
        user = <<"user">>,
        partition = <<"default">>,
        state = pending,
        script = <<"script">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 60,
        priority = 50,
        submit_time = erlang:system_time(second),
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined
    },
    ets:insert(flurm_db_jobs_ets, {2, Job}),
    {ok, Retrieved} = flurm_db_persist:get_job(2),
    ?assertEqual(<<"get_test">>, Retrieved#job.name),
    %% Test not found
    Result = flurm_db_persist:get_job(99999),
    ?assertEqual({error, not_found}, Result),
    ok.

test_update_job_ets() ->
    Job = #job{
        id = 3,
        name = <<"update_test">>,
        user = <<"user">>,
        partition = <<"default">>,
        state = pending,
        script = <<"script">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 60,
        priority = 50,
        submit_time = erlang:system_time(second),
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined
    },
    ets:insert(flurm_db_jobs_ets, {3, Job}),
    %% Update state to running
    ok = flurm_db_persist:update_job(3, #{state => running}),
    {ok, Updated} = flurm_db_persist:get_job(3),
    ?assertEqual(running, Updated#job.state),
    ?assertNotEqual(undefined, Updated#job.start_time),
    %% Update nonexistent job
    Result = flurm_db_persist:update_job(99999, #{state => running}),
    ?assertEqual({error, not_found}, Result),
    ok.

test_delete_job_ets() ->
    Job = #job{
        id = 4,
        name = <<"delete_test">>,
        user = <<"user">>,
        partition = <<"default">>,
        state = pending,
        script = <<"script">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 60,
        priority = 50,
        submit_time = erlang:system_time(second),
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined
    },
    ets:insert(flurm_db_jobs_ets, {4, Job}),
    ok = flurm_db_persist:delete_job(4),
    ?assertEqual({error, not_found}, flurm_db_persist:get_job(4)),
    ok.

test_list_jobs_ets() ->
    ets:delete_all_objects(flurm_db_jobs_ets),
    Job1 = #job{id = 10, name = <<"j1">>, user = <<"u">>, partition = <<"p">>,
                state = pending, script = <<"s">>, num_nodes = 1, num_cpus = 1,
                memory_mb = 256, time_limit = 60, priority = 50,
                submit_time = erlang:system_time(second), start_time = undefined,
                end_time = undefined, allocated_nodes = [], exit_code = undefined},
    Job2 = #job{id = 11, name = <<"j2">>, user = <<"u">>, partition = <<"p">>,
                state = running, script = <<"s">>, num_nodes = 1, num_cpus = 1,
                memory_mb = 256, time_limit = 60, priority = 50,
                submit_time = erlang:system_time(second), start_time = undefined,
                end_time = undefined, allocated_nodes = [], exit_code = undefined},
    ets:insert(flurm_db_jobs_ets, {10, Job1}),
    ets:insert(flurm_db_jobs_ets, {11, Job2}),
    Jobs = flurm_db_persist:list_jobs(),
    ?assertEqual(2, length(Jobs)),
    ok.

test_next_job_id_ets() ->
    ets:insert(flurm_db_job_counter_ets, {counter, 100}),
    Id1 = flurm_db_persist:next_job_id(),
    Id2 = flurm_db_persist:next_job_id(),
    ?assertEqual(101, Id1),
    ?assertEqual(102, Id2),
    ok.

test_apply_job_updates() ->
    BaseJob = #job{
        id = 100,
        name = <<"test">>,
        user = <<"user">>,
        partition = <<"default">>,
        state = pending,
        script = <<"script">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 256,
        time_limit = 60,
        priority = 50,
        submit_time = erlang:system_time(second),
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined
    },
    ets:insert(flurm_db_jobs_ets, {100, BaseJob}),

    %% Test state transition to running (should set start_time)
    ok = flurm_db_persist:update_job(100, #{state => running}),
    {ok, J1} = flurm_db_persist:get_job(100),
    ?assertEqual(running, J1#job.state),
    ?assertNotEqual(undefined, J1#job.start_time),

    %% Test state transition to completed (should set end_time)
    ok = flurm_db_persist:update_job(100, #{state => completed}),
    {ok, J2} = flurm_db_persist:get_job(100),
    ?assertEqual(completed, J2#job.state),
    ?assertNotEqual(undefined, J2#job.end_time),

    %% Test updating allocated_nodes
    BaseJob2 = BaseJob#job{id = 101},
    ets:insert(flurm_db_jobs_ets, {101, BaseJob2}),
    ok = flurm_db_persist:update_job(101, #{allocated_nodes => [<<"node1">>, <<"node2">>]}),
    {ok, J3} = flurm_db_persist:get_job(101),
    ?assertEqual([<<"node1">>, <<"node2">>], J3#job.allocated_nodes),

    %% Test updating exit_code
    BaseJob3 = BaseJob#job{id = 102},
    ets:insert(flurm_db_jobs_ets, {102, BaseJob3}),
    ok = flurm_db_persist:update_job(102, #{exit_code => 0}),
    {ok, J4} = flurm_db_persist:get_job(102),
    ?assertEqual(0, J4#job.exit_code),

    ok.

%%====================================================================
%% State Transition Tests
%%====================================================================

state_transitions_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"Test failed state sets end_time", fun() ->
             Job = #job{id = 200, name = <<"t">>, user = <<"u">>, partition = <<"p">>,
                       state = running, script = <<"s">>, num_nodes = 1, num_cpus = 1,
                       memory_mb = 256, time_limit = 60, priority = 50,
                       submit_time = erlang:system_time(second),
                       start_time = erlang:system_time(second),
                       end_time = undefined, allocated_nodes = [], exit_code = undefined},
             ets:insert(flurm_db_jobs_ets, {200, Job}),
             ok = flurm_db_persist:update_job(200, #{state => failed}),
             {ok, Updated} = flurm_db_persist:get_job(200),
             ?assertEqual(failed, Updated#job.state),
             ?assertNotEqual(undefined, Updated#job.end_time)
         end},
         {"Test cancelled state sets end_time", fun() ->
             Job = #job{id = 201, name = <<"t">>, user = <<"u">>, partition = <<"p">>,
                       state = pending, script = <<"s">>, num_nodes = 1, num_cpus = 1,
                       memory_mb = 256, time_limit = 60, priority = 50,
                       submit_time = erlang:system_time(second),
                       start_time = undefined,
                       end_time = undefined, allocated_nodes = [], exit_code = undefined},
             ets:insert(flurm_db_jobs_ets, {201, Job}),
             ok = flurm_db_persist:update_job(201, #{state => cancelled}),
             {ok, Updated} = flurm_db_persist:get_job(201),
             ?assertEqual(cancelled, Updated#job.state),
             ?assertNotEqual(undefined, Updated#job.end_time)
         end},
         {"Test timeout state sets end_time", fun() ->
             Job = #job{id = 202, name = <<"t">>, user = <<"u">>, partition = <<"p">>,
                       state = running, script = <<"s">>, num_nodes = 1, num_cpus = 1,
                       memory_mb = 256, time_limit = 60, priority = 50,
                       submit_time = erlang:system_time(second),
                       start_time = erlang:system_time(second),
                       end_time = undefined, allocated_nodes = [], exit_code = undefined},
             ets:insert(flurm_db_jobs_ets, {202, Job}),
             ok = flurm_db_persist:update_job(202, #{state => timeout}),
             {ok, Updated} = flurm_db_persist:get_job(202),
             ?assertEqual(timeout, Updated#job.state),
             ?assertNotEqual(undefined, Updated#job.end_time)
         end},
         {"Test node_fail state sets end_time", fun() ->
             Job = #job{id = 203, name = <<"t">>, user = <<"u">>, partition = <<"p">>,
                       state = running, script = <<"s">>, num_nodes = 1, num_cpus = 1,
                       memory_mb = 256, time_limit = 60, priority = 50,
                       submit_time = erlang:system_time(second),
                       start_time = erlang:system_time(second),
                       end_time = undefined, allocated_nodes = [], exit_code = undefined},
             ets:insert(flurm_db_jobs_ets, {203, Job}),
             ok = flurm_db_persist:update_job(203, #{state => node_fail}),
             {ok, Updated} = flurm_db_persist:get_job(203),
             ?assertEqual(node_fail, Updated#job.state),
             ?assertNotEqual(undefined, Updated#job.end_time)
         end}
     ]
    }.
