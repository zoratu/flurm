%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_core module functions
%%%
%%% Tests the pure domain logic functions for jobs, nodes, and partitions.
%%% All functions in this module are already exported as the public API,
%%% so this test module covers them directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_core_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test: new_job/1
%%====================================================================

new_job_minimal_test() ->
    Job = flurm_core:new_job(#{}),
    ?assert(is_record(Job, job)),
    ?assert(is_integer(Job#job.id)),
    ?assertEqual(<<"unnamed">>, Job#job.name),
    ?assertEqual(<<"unknown">>, Job#job.user),
    ?assertEqual(<<"default">>, Job#job.partition),
    ?assertEqual(pending, Job#job.state).

new_job_with_all_fields_test() ->
    Props = #{
        id => 12345,
        name => <<"my_job">>,
        user => <<"testuser">>,
        partition => <<"batch">>,
        state => pending,
        script => <<"#!/bin/bash\necho hello">>,
        num_nodes => 4,
        num_cpus => 16,
        memory_mb => 8192,
        time_limit => 7200,
        priority => 500,
        submit_time => 1700000000,
        start_time => 1700000100,
        end_time => 1700001000,
        allocated_nodes => [<<"node1">>, <<"node2">>],
        exit_code => 0,
        account => <<"research">>,
        qos => <<"high">>
    },
    Job = flurm_core:new_job(Props),
    ?assertEqual(12345, Job#job.id),
    ?assertEqual(<<"my_job">>, Job#job.name),
    ?assertEqual(<<"testuser">>, Job#job.user),
    ?assertEqual(<<"batch">>, Job#job.partition),
    ?assertEqual(pending, Job#job.state),
    ?assertEqual(<<"#!/bin/bash\necho hello">>, Job#job.script),
    ?assertEqual(4, Job#job.num_nodes),
    ?assertEqual(16, Job#job.num_cpus),
    ?assertEqual(8192, Job#job.memory_mb),
    ?assertEqual(7200, Job#job.time_limit),
    ?assertEqual(500, Job#job.priority),
    ?assertEqual(1700000000, Job#job.submit_time),
    ?assertEqual(1700000100, Job#job.start_time),
    ?assertEqual(1700001000, Job#job.end_time),
    ?assertEqual([<<"node1">>, <<"node2">>], Job#job.allocated_nodes),
    ?assertEqual(0, Job#job.exit_code),
    ?assertEqual(<<"research">>, Job#job.account),
    ?assertEqual(<<"high">>, Job#job.qos).

new_job_partial_props_test() ->
    Props = #{
        id => 999,
        name => <<"partial_job">>,
        num_cpus => 8
    },
    Job = flurm_core:new_job(Props),
    ?assertEqual(999, Job#job.id),
    ?assertEqual(<<"partial_job">>, Job#job.name),
    ?assertEqual(8, Job#job.num_cpus),
    %% Defaults for unspecified fields
    ?assertEqual(<<"unknown">>, Job#job.user),
    ?assertEqual(1, Job#job.num_nodes),
    ?assertEqual(1024, Job#job.memory_mb).

%%====================================================================
%% Test: job_id/1
%%====================================================================

job_id_test() ->
    Job = #job{id = 42},
    ?assertEqual(42, flurm_core:job_id(Job)).

job_id_large_number_test() ->
    Job = #job{id = 999999999},
    ?assertEqual(999999999, flurm_core:job_id(Job)).

%%====================================================================
%% Test: job_state/1
%%====================================================================

job_state_pending_test() ->
    Job = #job{state = pending},
    ?assertEqual(pending, flurm_core:job_state(Job)).

job_state_running_test() ->
    Job = #job{state = running},
    ?assertEqual(running, flurm_core:job_state(Job)).

job_state_completed_test() ->
    Job = #job{state = completed},
    ?assertEqual(completed, flurm_core:job_state(Job)).

job_state_failed_test() ->
    Job = #job{state = failed},
    ?assertEqual(failed, flurm_core:job_state(Job)).

job_state_cancelled_test() ->
    Job = #job{state = cancelled},
    ?assertEqual(cancelled, flurm_core:job_state(Job)).

%%====================================================================
%% Test: update_job_state/2
%%====================================================================

update_job_state_to_running_test() ->
    Job = #job{id = 1, state = pending, start_time = undefined},
    UpdatedJob = flurm_core:update_job_state(Job, running),
    ?assertEqual(running, UpdatedJob#job.state),
    ?assert(is_integer(UpdatedJob#job.start_time)),
    Now = erlang:system_time(second),
    ?assert(UpdatedJob#job.start_time =< Now),
    ?assert(UpdatedJob#job.start_time > Now - 10).

update_job_state_to_completed_test() ->
    Job = #job{id = 1, state = running, end_time = undefined},
    UpdatedJob = flurm_core:update_job_state(Job, completed),
    ?assertEqual(completed, UpdatedJob#job.state),
    ?assert(is_integer(UpdatedJob#job.end_time)).

update_job_state_to_failed_test() ->
    Job = #job{id = 1, state = running, end_time = undefined},
    UpdatedJob = flurm_core:update_job_state(Job, failed),
    ?assertEqual(failed, UpdatedJob#job.state),
    ?assert(is_integer(UpdatedJob#job.end_time)).

update_job_state_to_cancelled_test() ->
    Job = #job{id = 1, state = pending, end_time = undefined},
    UpdatedJob = flurm_core:update_job_state(Job, cancelled),
    ?assertEqual(cancelled, UpdatedJob#job.state),
    ?assert(is_integer(UpdatedJob#job.end_time)).

update_job_state_to_pending_test() ->
    Job = #job{id = 1, state = running},
    UpdatedJob = flurm_core:update_job_state(Job, pending),
    ?assertEqual(pending, UpdatedJob#job.state).

update_job_state_preserves_other_fields_test() ->
    Job = #job{id = 123, name = <<"test">>, user = <<"user1">>, state = pending},
    UpdatedJob = flurm_core:update_job_state(Job, running),
    ?assertEqual(123, UpdatedJob#job.id),
    ?assertEqual(<<"test">>, UpdatedJob#job.name),
    ?assertEqual(<<"user1">>, UpdatedJob#job.user).

%%====================================================================
%% Test: new_node/1
%%====================================================================

new_node_minimal_test() ->
    Props = #{hostname => <<"node01">>},
    Node = flurm_core:new_node(Props),
    ?assert(is_record(Node, node)),
    ?assertEqual(<<"node01">>, Node#node.hostname),
    ?assertEqual(1, Node#node.cpus),
    ?assertEqual(1024, Node#node.memory_mb),
    ?assertEqual(down, Node#node.state).

new_node_with_all_fields_test() ->
    Props = #{
        hostname => <<"compute001">>,
        cpus => 64,
        memory_mb => 256000,
        state => idle,
        features => [<<"gpu">>, <<"nvme">>],
        partitions => [<<"batch">>, <<"gpu">>],
        running_jobs => [1, 2, 3],
        load_avg => 2.5,
        free_memory_mb => 128000,
        last_heartbeat => 1700000000
    },
    Node = flurm_core:new_node(Props),
    ?assertEqual(<<"compute001">>, Node#node.hostname),
    ?assertEqual(64, Node#node.cpus),
    ?assertEqual(256000, Node#node.memory_mb),
    ?assertEqual(idle, Node#node.state),
    ?assertEqual([<<"gpu">>, <<"nvme">>], Node#node.features),
    ?assertEqual([<<"batch">>, <<"gpu">>], Node#node.partitions),
    ?assertEqual([1, 2, 3], Node#node.running_jobs),
    ?assertEqual(2.5, Node#node.load_avg),
    ?assertEqual(128000, Node#node.free_memory_mb),
    ?assertEqual(1700000000, Node#node.last_heartbeat).

%%====================================================================
%% Test: node_hostname/1
%%====================================================================

node_hostname_test() ->
    Node = #node{hostname = <<"myhost">>},
    ?assertEqual(<<"myhost">>, flurm_core:node_hostname(Node)).

node_hostname_with_domain_test() ->
    Node = #node{hostname = <<"compute001.cluster.local">>},
    ?assertEqual(<<"compute001.cluster.local">>, flurm_core:node_hostname(Node)).

%%====================================================================
%% Test: node_state/1
%%====================================================================

node_state_idle_test() ->
    Node = #node{state = idle},
    ?assertEqual(idle, flurm_core:node_state(Node)).

node_state_allocated_test() ->
    Node = #node{state = allocated},
    ?assertEqual(allocated, flurm_core:node_state(Node)).

node_state_down_test() ->
    Node = #node{state = down},
    ?assertEqual(down, flurm_core:node_state(Node)).

node_state_drain_test() ->
    Node = #node{state = drain},
    ?assertEqual(drain, flurm_core:node_state(Node)).

%%====================================================================
%% Test: update_node_state/2
%%====================================================================

update_node_state_test() ->
    Node = #node{hostname = <<"host">>, state = down},
    UpdatedNode = flurm_core:update_node_state(Node, idle),
    ?assertEqual(idle, UpdatedNode#node.state),
    ?assertEqual(<<"host">>, UpdatedNode#node.hostname).

update_node_state_preserves_fields_test() ->
    Node = #node{hostname = <<"host">>, cpus = 32, memory_mb = 64000, state = idle},
    UpdatedNode = flurm_core:update_node_state(Node, allocated),
    ?assertEqual(allocated, UpdatedNode#node.state),
    ?assertEqual(32, UpdatedNode#node.cpus),
    ?assertEqual(64000, UpdatedNode#node.memory_mb).

%%====================================================================
%% Test: new_partition/1
%%====================================================================

new_partition_minimal_test() ->
    Props = #{name => <<"batch">>},
    Partition = flurm_core:new_partition(Props),
    ?assert(is_record(Partition, partition)),
    ?assertEqual(<<"batch">>, Partition#partition.name),
    ?assertEqual(up, Partition#partition.state),
    ?assertEqual([], Partition#partition.nodes),
    ?assertEqual(86400, Partition#partition.max_time),
    ?assertEqual(3600, Partition#partition.default_time).

new_partition_with_all_fields_test() ->
    Props = #{
        name => <<"gpu">>,
        state => up,
        nodes => [<<"gpu01">>, <<"gpu02">>],
        max_time => 172800,
        default_time => 7200,
        max_nodes => 50,
        priority => 200,
        allow_root => true
    },
    Partition = flurm_core:new_partition(Props),
    ?assertEqual(<<"gpu">>, Partition#partition.name),
    ?assertEqual(up, Partition#partition.state),
    ?assertEqual([<<"gpu01">>, <<"gpu02">>], Partition#partition.nodes),
    ?assertEqual(172800, Partition#partition.max_time),
    ?assertEqual(7200, Partition#partition.default_time),
    ?assertEqual(50, Partition#partition.max_nodes),
    ?assertEqual(200, Partition#partition.priority),
    ?assertEqual(true, Partition#partition.allow_root).

%%====================================================================
%% Test: partition_name/1
%%====================================================================

partition_name_test() ->
    Partition = #partition{name = <<"default">>},
    ?assertEqual(<<"default">>, flurm_core:partition_name(Partition)).

%%====================================================================
%% Test: partition_nodes/1
%%====================================================================

partition_nodes_empty_test() ->
    Partition = #partition{nodes = []},
    ?assertEqual([], flurm_core:partition_nodes(Partition)).

partition_nodes_multiple_test() ->
    Partition = #partition{nodes = [<<"node1">>, <<"node2">>, <<"node3">>]},
    ?assertEqual([<<"node1">>, <<"node2">>, <<"node3">>], flurm_core:partition_nodes(Partition)).

%%====================================================================
%% Test: add_node_to_partition/2
%%====================================================================

add_node_to_partition_empty_test() ->
    Partition = #partition{name = <<"test">>, nodes = []},
    Updated = flurm_core:add_node_to_partition(Partition, <<"node1">>),
    ?assertEqual([<<"node1">>], Updated#partition.nodes).

add_node_to_partition_existing_test() ->
    Partition = #partition{name = <<"test">>, nodes = [<<"node1">>]},
    Updated = flurm_core:add_node_to_partition(Partition, <<"node2">>),
    ?assertEqual([<<"node2">>, <<"node1">>], Updated#partition.nodes).

add_node_to_partition_duplicate_test() ->
    %% Adding a duplicate node should not change the list
    Partition = #partition{name = <<"test">>, nodes = [<<"node1">>, <<"node2">>]},
    Updated = flurm_core:add_node_to_partition(Partition, <<"node1">>),
    ?assertEqual([<<"node1">>, <<"node2">>], Updated#partition.nodes).

add_node_to_partition_preserves_fields_test() ->
    Partition = #partition{name = <<"batch">>, state = up, max_time = 3600, nodes = []},
    Updated = flurm_core:add_node_to_partition(Partition, <<"node1">>),
    ?assertEqual(<<"batch">>, Updated#partition.name),
    ?assertEqual(up, Updated#partition.state),
    ?assertEqual(3600, Updated#partition.max_time).
