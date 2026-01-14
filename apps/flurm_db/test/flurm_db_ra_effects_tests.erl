%%%-------------------------------------------------------------------
%%% @doc FLURM Database Ra Effects Handler Tests
%%%
%%% Tests for the flurm_db_ra_effects module which handles side effects
%%% triggered after Ra consensus.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_effects_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start pg if not already started
    case pg:start(pg) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ok.

cleanup(_) ->
    %% Unsubscribe any test processes
    catch pg:leave(flurm_db_effects, self()),
    ok.

%%====================================================================
%% Subscriber Management Tests
%%====================================================================

subscriber_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"subscribe adds process to group", fun() ->
             ok = flurm_db_ra_effects:subscribe(self()),
             Members = pg:get_members(flurm_db_effects),
             ?assert(lists:member(self(), Members)),
             flurm_db_ra_effects:unsubscribe(self())
         end},
         {"unsubscribe removes process from group", fun() ->
             ok = flurm_db_ra_effects:subscribe(self()),
             ok = flurm_db_ra_effects:unsubscribe(self()),
             Members = pg:get_members(flurm_db_effects),
             ?assertNot(lists:member(self(), Members))
         end}
     ]
    }.

%%====================================================================
%% Job Effects Tests
%%====================================================================

job_effects_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"job_submitted notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             Job = #ra_job{
                 id = 1,
                 name = <<"test">>,
                 user = <<"user">>,
                 group = <<"group">>,
                 partition = <<"default">>,
                 state = pending,
                 script = <<"#!/bin/bash">>,
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
             ok = flurm_db_ra_effects:job_submitted(Job),
             receive
                 {flurm_db_event, {job_submitted, ReceivedJob}} ->
                     ?assertEqual(1, ReceivedJob#ra_job.id)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end},
         {"job_cancelled notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             Job = #ra_job{
                 id = 2,
                 name = <<"cancel_test">>,
                 user = <<"user">>,
                 group = <<"group">>,
                 partition = <<"default">>,
                 state = cancelled,
                 script = <<"#!/bin/bash">>,
                 num_nodes = 1,
                 num_cpus = 1,
                 memory_mb = 256,
                 time_limit = 60,
                 priority = 50,
                 submit_time = erlang:system_time(second),
                 start_time = undefined,
                 end_time = erlang:system_time(second),
                 allocated_nodes = [],
                 exit_code = undefined
             },
             ok = flurm_db_ra_effects:job_cancelled(Job),
             receive
                 {flurm_db_event, {job_cancelled, ReceivedJob}} ->
                     ?assertEqual(2, ReceivedJob#ra_job.id)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end},
         {"job_state_changed notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             ok = flurm_db_ra_effects:job_state_changed(123, pending, running),
             receive
                 {flurm_db_event, {job_state_changed, 123, pending, running}} ->
                     ok
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end},
         {"job_allocated notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             Job = #ra_job{
                 id = 3,
                 name = <<"alloc_test">>,
                 user = <<"user">>,
                 group = <<"group">>,
                 partition = <<"default">>,
                 state = configuring,
                 script = <<"#!/bin/bash">>,
                 num_nodes = 2,
                 num_cpus = 4,
                 memory_mb = 1024,
                 time_limit = 3600,
                 priority = 100,
                 submit_time = erlang:system_time(second),
                 start_time = erlang:system_time(second),
                 end_time = undefined,
                 allocated_nodes = [<<"n1">>, <<"n2">>],
                 exit_code = undefined
             },
             Nodes = [<<"n1">>, <<"n2">>],
             ok = flurm_db_ra_effects:job_allocated(Job, Nodes),
             receive
                 {flurm_db_event, {job_allocated, ReceivedJob, ReceivedNodes}} ->
                     ?assertEqual(3, ReceivedJob#ra_job.id),
                     ?assertEqual(Nodes, ReceivedNodes)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end},
         {"job_completed notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             Job = #ra_job{
                 id = 4,
                 name = <<"complete_test">>,
                 user = <<"user">>,
                 group = <<"group">>,
                 partition = <<"default">>,
                 state = completed,
                 script = <<"#!/bin/bash">>,
                 num_nodes = 1,
                 num_cpus = 1,
                 memory_mb = 256,
                 time_limit = 60,
                 priority = 50,
                 submit_time = erlang:system_time(second) - 100,
                 start_time = erlang:system_time(second) - 50,
                 end_time = erlang:system_time(second),
                 allocated_nodes = [<<"n1">>],
                 exit_code = 0
             },
             ok = flurm_db_ra_effects:job_completed(Job, 0),
             receive
                 {flurm_db_event, {job_completed, ReceivedJob, ExitCode}} ->
                     ?assertEqual(4, ReceivedJob#ra_job.id),
                     ?assertEqual(0, ExitCode)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end}
     ]
    }.

%%====================================================================
%% Node Effects Tests
%%====================================================================

node_effects_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"node_registered notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             Node = #ra_node{
                 name = <<"node1">>,
                 hostname = <<"node1.example.com">>,
                 port = 7000,
                 cpus = 32,
                 cpus_used = 0,
                 memory_mb = 65536,
                 memory_used = 0,
                 gpus = 4,
                 gpus_used = 0,
                 state = up,
                 features = [gpu],
                 partitions = [<<"default">>],
                 running_jobs = [],
                 last_heartbeat = erlang:system_time(second)
             },
             ok = flurm_db_ra_effects:node_registered(Node),
             receive
                 {flurm_db_event, {node_registered, ReceivedNode}} ->
                     ?assertEqual(<<"node1">>, ReceivedNode#ra_node.name)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end},
         {"node_updated notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             Node = #ra_node{
                 name = <<"node2">>,
                 hostname = <<"node2.example.com">>,
                 port = 7000,
                 cpus = 16,
                 cpus_used = 8,
                 memory_mb = 32768,
                 memory_used = 16384,
                 gpus = 0,
                 gpus_used = 0,
                 state = up,
                 features = [],
                 partitions = [<<"default">>],
                 running_jobs = [1, 2],
                 last_heartbeat = erlang:system_time(second)
             },
             ok = flurm_db_ra_effects:node_updated(Node),
             receive
                 {flurm_db_event, {node_updated, ReceivedNode}} ->
                     ?assertEqual(<<"node2">>, ReceivedNode#ra_node.name)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end},
         {"node_state_changed notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             ok = flurm_db_ra_effects:node_state_changed(<<"node3">>, up, drain),
             receive
                 {flurm_db_event, {node_state_changed, <<"node3">>, up, drain}} ->
                     ok
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end},
         {"node_unregistered notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             Node = #ra_node{
                 name = <<"node4">>,
                 hostname = <<"node4.example.com">>,
                 port = 7000,
                 cpus = 8,
                 cpus_used = 0,
                 memory_mb = 16384,
                 memory_used = 0,
                 gpus = 0,
                 gpus_used = 0,
                 state = down,
                 features = [],
                 partitions = [<<"default">>],
                 running_jobs = [],
                 last_heartbeat = erlang:system_time(second)
             },
             ok = flurm_db_ra_effects:node_unregistered(Node),
             receive
                 {flurm_db_event, {node_unregistered, ReceivedNode}} ->
                     ?assertEqual(<<"node4">>, ReceivedNode#ra_node.name)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end}
     ]
    }.

%%====================================================================
%% Partition Effects Tests
%%====================================================================

partition_effects_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"partition_created notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             Partition = #ra_partition{
                 name = <<"batch">>,
                 state = up,
                 nodes = [<<"n1">>, <<"n2">>],
                 max_time = 86400,
                 default_time = 3600,
                 max_nodes = 100,
                 priority = 10
             },
             ok = flurm_db_ra_effects:partition_created(Partition),
             receive
                 {flurm_db_event, {partition_created, ReceivedPart}} ->
                     ?assertEqual(<<"batch">>, ReceivedPart#ra_partition.name)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end},
         {"partition_deleted notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             Partition = #ra_partition{
                 name = <<"temp">>,
                 state = up,
                 nodes = [],
                 max_time = 3600,
                 default_time = 60,
                 max_nodes = 10,
                 priority = 1
             },
             ok = flurm_db_ra_effects:partition_deleted(Partition),
             receive
                 {flurm_db_event, {partition_deleted, ReceivedPart}} ->
                     ?assertEqual(<<"temp">>, ReceivedPart#ra_partition.name)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end}
     ]
    }.

%%====================================================================
%% Leadership Effects Tests
%%====================================================================

leadership_effects_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"became_leader notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             ok = flurm_db_ra_effects:became_leader(node()),
             receive
                 {flurm_db_event, {became_leader, Node}} ->
                     ?assertEqual(node(), Node)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end},
         {"became_follower notifies subscribers", fun() ->
             flurm_db_ra_effects:subscribe(self()),
             ok = flurm_db_ra_effects:became_follower(node()),
             receive
                 {flurm_db_event, {became_follower, Node}} ->
                     ?assertEqual(node(), Node)
             after 1000 ->
                 ?assert(false)
             end,
             flurm_db_ra_effects:unsubscribe(self())
         end}
     ]
    }.

%%====================================================================
%% Helper Function Tests
%%====================================================================

helper_functions_test_() ->
    [
        {"job_to_map creates correct map", fun() ->
             Job = #ra_job{
                 id = 99,
                 name = <<"map_test">>,
                 user = <<"testuser">>,
                 group = <<"testgroup">>,
                 partition = <<"default">>,
                 state = running,
                 script = <<"#!/bin/bash">>,
                 num_nodes = 2,
                 num_cpus = 4,
                 memory_mb = 2048,
                 time_limit = 7200,
                 priority = 200,
                 submit_time = 1000,
                 start_time = 1100,
                 end_time = undefined,
                 allocated_nodes = [<<"n1">>],
                 exit_code = undefined
             },
             %% The job_to_map function is internal, test indirectly
             %% by verifying effect handlers work
             ok = flurm_db_ra_effects:job_submitted(Job)
         end},
        {"node_to_map creates correct map", fun() ->
             Node = #ra_node{
                 name = <<"map_node">>,
                 hostname = <<"map.example.com">>,
                 port = 7000,
                 cpus = 16,
                 cpus_used = 0,
                 memory_mb = 32768,
                 memory_used = 0,
                 gpus = 0,
                 gpus_used = 0,
                 state = up,
                 features = [],
                 partitions = [],
                 running_jobs = [],
                 last_heartbeat = 0
             },
             %% Test indirectly
             ok = flurm_db_ra_effects:node_registered(Node)
         end}
    ].
