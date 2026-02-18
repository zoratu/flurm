%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_core module
%%% Tests job, node, and partition domain functions
%%%-------------------------------------------------------------------
-module(flurm_core_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Job Tests
%%====================================================================

job_test_() ->
    [
     {"Create job with defaults", fun test_new_job_defaults/0},
     {"Create job with all properties", fun test_new_job_full/0},
     {"Get job id", fun test_job_id/0},
     {"Get job state", fun test_job_state/0},
     {"Update job state to running", fun test_update_job_state_running/0},
     {"Update job state to completed", fun test_update_job_state_completed/0},
     {"Update job state to failed", fun test_update_job_state_failed/0},
     {"Update job state to cancelled", fun test_update_job_state_cancelled/0},
     {"Update job state to pending", fun test_update_job_state_pending/0}
    ].

test_new_job_defaults() ->
    Job = flurm_core:new_job(#{}),
    ?assert(is_integer(Job#job.id)),
    ?assertEqual(<<"unnamed">>, Job#job.name),
    ?assertEqual(<<"unknown">>, Job#job.user),
    ?assertEqual(<<"default">>, Job#job.partition),
    ?assertEqual(pending, Job#job.state),
    ?assertEqual(<<>>, Job#job.script),
    ?assertEqual(1, Job#job.num_nodes),
    ?assertEqual(1, Job#job.num_cpus),
    ?assertEqual(1024, Job#job.memory_mb),
    ?assertEqual(3600, Job#job.time_limit),
    ?assertEqual(100, Job#job.priority),
    ?assert(is_integer(Job#job.submit_time)),
    ?assertEqual(undefined, Job#job.start_time),
    ?assertEqual(undefined, Job#job.end_time),
    ?assertEqual([], Job#job.allocated_nodes),
    ?assertEqual(undefined, Job#job.exit_code),
    ?assertEqual(<<>>, Job#job.account),
    ?assertEqual(<<"normal">>, Job#job.qos).

test_new_job_full() ->
    Props = #{
        id => 12345,
        name => <<"my_job">>,
        user => <<"testuser">>,
        partition => <<"batch">>,
        state => running,
        script => <<"#!/bin/bash\necho hello">>,
        num_nodes => 4,
        num_cpus => 64,
        memory_mb => 8192,
        time_limit => 7200,
        priority => 500,
        submit_time => 1700000000,
        start_time => 1700000100,
        end_time => undefined,
        allocated_nodes => [<<"node01">>, <<"node02">>],
        exit_code => undefined,
        account => <<"myaccount">>,
        qos => <<"high">>
    },
    Job = flurm_core:new_job(Props),
    ?assertEqual(12345, Job#job.id),
    ?assertEqual(<<"my_job">>, Job#job.name),
    ?assertEqual(<<"testuser">>, Job#job.user),
    ?assertEqual(<<"batch">>, Job#job.partition),
    ?assertEqual(running, Job#job.state),
    ?assertEqual(<<"#!/bin/bash\necho hello">>, Job#job.script),
    ?assertEqual(4, Job#job.num_nodes),
    ?assertEqual(64, Job#job.num_cpus),
    ?assertEqual(8192, Job#job.memory_mb),
    ?assertEqual(7200, Job#job.time_limit),
    ?assertEqual(500, Job#job.priority),
    ?assertEqual(1700000000, Job#job.submit_time),
    ?assertEqual(1700000100, Job#job.start_time),
    ?assertEqual([<<"node01">>, <<"node02">>], Job#job.allocated_nodes),
    ?assertEqual(<<"myaccount">>, Job#job.account),
    ?assertEqual(<<"high">>, Job#job.qos).

test_job_id() ->
    Job = flurm_core:new_job(#{id => 999}),
    ?assertEqual(999, flurm_core:job_id(Job)).

test_job_state() ->
    Job = flurm_core:new_job(#{state => running}),
    ?assertEqual(running, flurm_core:job_state(Job)).

test_update_job_state_running() ->
    Job = flurm_core:new_job(#{state => pending}),
    UpdatedJob = flurm_core:update_job_state(Job, running),
    ?assertEqual(running, UpdatedJob#job.state),
    ?assert(is_integer(UpdatedJob#job.start_time)).

test_update_job_state_completed() ->
    Job = flurm_core:new_job(#{state => running}),
    UpdatedJob = flurm_core:update_job_state(Job, completed),
    ?assertEqual(completed, UpdatedJob#job.state),
    ?assert(is_integer(UpdatedJob#job.end_time)).

test_update_job_state_failed() ->
    Job = flurm_core:new_job(#{state => running}),
    UpdatedJob = flurm_core:update_job_state(Job, failed),
    ?assertEqual(failed, UpdatedJob#job.state),
    ?assert(is_integer(UpdatedJob#job.end_time)).

test_update_job_state_cancelled() ->
    Job = flurm_core:new_job(#{state => pending}),
    UpdatedJob = flurm_core:update_job_state(Job, cancelled),
    ?assertEqual(cancelled, UpdatedJob#job.state),
    ?assert(is_integer(UpdatedJob#job.end_time)).

test_update_job_state_pending() ->
    Job = flurm_core:new_job(#{state => held}),
    UpdatedJob = flurm_core:update_job_state(Job, pending),
    ?assertEqual(pending, UpdatedJob#job.state),
    %% Pending shouldn't set timestamps
    ?assertEqual(undefined, UpdatedJob#job.start_time).

%%====================================================================
%% Node Tests
%%====================================================================

node_test_() ->
    [
     {"Create node with defaults", fun test_new_node_defaults/0},
     {"Create node with all properties", fun test_new_node_full/0},
     {"Get node hostname", fun test_node_hostname/0},
     {"Get node state", fun test_node_state/0},
     {"Update node state", fun test_update_node_state/0}
    ].

test_new_node_defaults() ->
    Node = flurm_core:new_node(#{hostname => <<"node01">>}),
    ?assertEqual(<<"node01">>, Node#node.hostname),
    ?assertEqual(1, Node#node.cpus),
    ?assertEqual(1024, Node#node.memory_mb),
    ?assertEqual(down, Node#node.state),
    ?assertEqual([], Node#node.features),
    ?assertEqual([], Node#node.partitions),
    ?assertEqual([], Node#node.running_jobs),
    ?assertEqual(0.0, Node#node.load_avg),
    ?assertEqual(0, Node#node.free_memory_mb),
    ?assertEqual(undefined, Node#node.last_heartbeat).

test_new_node_full() ->
    Props = #{
        hostname => <<"compute-001.cluster.local">>,
        cpus => 128,
        memory_mb => 524288,
        state => idle,
        features => [<<"gpu">>, <<"infiniband">>],
        partitions => [<<"batch">>, <<"gpu">>],
        running_jobs => [1, 2, 3],
        load_avg => 0.5,
        free_memory_mb => 500000,
        last_heartbeat => 1700000000
    },
    Node = flurm_core:new_node(Props),
    ?assertEqual(<<"compute-001.cluster.local">>, Node#node.hostname),
    ?assertEqual(128, Node#node.cpus),
    ?assertEqual(524288, Node#node.memory_mb),
    ?assertEqual(idle, Node#node.state),
    ?assertEqual([<<"gpu">>, <<"infiniband">>], Node#node.features),
    ?assertEqual([<<"batch">>, <<"gpu">>], Node#node.partitions),
    ?assertEqual([1, 2, 3], Node#node.running_jobs),
    ?assertEqual(0.5, Node#node.load_avg),
    ?assertEqual(500000, Node#node.free_memory_mb),
    ?assertEqual(1700000000, Node#node.last_heartbeat).

test_node_hostname() ->
    Node = flurm_core:new_node(#{hostname => <<"myhost">>}),
    ?assertEqual(<<"myhost">>, flurm_core:node_hostname(Node)).

test_node_state() ->
    Node = flurm_core:new_node(#{hostname => <<"node">>, state => allocated}),
    ?assertEqual(allocated, flurm_core:node_state(Node)).

test_update_node_state() ->
    Node = flurm_core:new_node(#{hostname => <<"node">>, state => idle}),
    UpdatedNode = flurm_core:update_node_state(Node, drain),
    ?assertEqual(drain, UpdatedNode#node.state).

%%====================================================================
%% Partition Tests
%%====================================================================

partition_test_() ->
    [
     {"Create partition with defaults", fun test_new_partition_defaults/0},
     {"Create partition with all properties", fun test_new_partition_full/0},
     {"Get partition name", fun test_partition_name/0},
     {"Get partition nodes", fun test_partition_nodes/0},
     {"Add node to partition - new", fun test_add_node_new/0},
     {"Add node to partition - duplicate", fun test_add_node_duplicate/0}
    ].

test_new_partition_defaults() ->
    Partition = flurm_core:new_partition(#{name => <<"batch">>}),
    ?assertEqual(<<"batch">>, Partition#partition.name),
    ?assertEqual(up, Partition#partition.state),
    ?assertEqual([], Partition#partition.nodes),
    ?assertEqual(86400, Partition#partition.max_time),
    ?assertEqual(3600, Partition#partition.default_time),
    ?assertEqual(100, Partition#partition.max_nodes),
    ?assertEqual(100, Partition#partition.priority),
    ?assertEqual(false, Partition#partition.allow_root).

test_new_partition_full() ->
    Props = #{
        name => <<"gpu">>,
        state => up,
        nodes => [<<"gpu01">>, <<"gpu02">>],
        max_time => 172800,
        default_time => 7200,
        max_nodes => 4,
        priority => 200,
        allow_root => true
    },
    Partition = flurm_core:new_partition(Props),
    ?assertEqual(<<"gpu">>, Partition#partition.name),
    ?assertEqual(up, Partition#partition.state),
    ?assertEqual([<<"gpu01">>, <<"gpu02">>], Partition#partition.nodes),
    ?assertEqual(172800, Partition#partition.max_time),
    ?assertEqual(7200, Partition#partition.default_time),
    ?assertEqual(4, Partition#partition.max_nodes),
    ?assertEqual(200, Partition#partition.priority),
    ?assertEqual(true, Partition#partition.allow_root).

test_partition_name() ->
    Partition = flurm_core:new_partition(#{name => <<"mypartition">>}),
    ?assertEqual(<<"mypartition">>, flurm_core:partition_name(Partition)).

test_partition_nodes() ->
    Partition = flurm_core:new_partition(#{name => <<"p">>, nodes => [<<"n1">>, <<"n2">>]}),
    ?assertEqual([<<"n1">>, <<"n2">>], flurm_core:partition_nodes(Partition)).

test_add_node_new() ->
    Partition = flurm_core:new_partition(#{name => <<"p">>, nodes => [<<"n1">>]}),
    UpdatedPartition = flurm_core:add_node_to_partition(Partition, <<"n2">>),
    Nodes = flurm_core:partition_nodes(UpdatedPartition),
    ?assert(lists:member(<<"n1">>, Nodes)),
    ?assert(lists:member(<<"n2">>, Nodes)).

test_add_node_duplicate() ->
    Partition = flurm_core:new_partition(#{name => <<"p">>, nodes => [<<"n1">>]}),
    UpdatedPartition = flurm_core:add_node_to_partition(Partition, <<"n1">>),
    Nodes = flurm_core:partition_nodes(UpdatedPartition),
    %% Should not have duplicates
    ?assertEqual([<<"n1">>], Nodes).

%%====================================================================
%% State Transition Tests
%%====================================================================

state_transition_test_() ->
    [
     {"Job can transition through lifecycle", fun test_job_lifecycle/0},
     {"Update job state to timeout", fun test_update_job_state_timeout/0},
     {"Update job state to node_fail", fun test_update_job_state_node_fail/0}
    ].

test_update_job_state_timeout() ->
    Job = flurm_core:new_job(#{state => running}),
    UpdatedJob = flurm_core:update_job_state(Job, timeout),
    ?assertEqual(timeout, UpdatedJob#job.state),
    ?assert(is_integer(UpdatedJob#job.end_time)).

test_update_job_state_node_fail() ->
    Job = flurm_core:new_job(#{state => running}),
    UpdatedJob = flurm_core:update_job_state(Job, node_fail),
    ?assertEqual(node_fail, UpdatedJob#job.state),
    ?assert(is_integer(UpdatedJob#job.end_time)).

test_job_lifecycle() ->
    %% Create pending job
    Job1 = flurm_core:new_job(#{state => pending}),
    ?assertEqual(pending, flurm_core:job_state(Job1)),
    ?assertEqual(undefined, Job1#job.start_time),

    %% Start running
    Job2 = flurm_core:update_job_state(Job1, running),
    ?assertEqual(running, flurm_core:job_state(Job2)),
    ?assert(is_integer(Job2#job.start_time)),
    ?assertEqual(undefined, Job2#job.end_time),

    %% Complete
    Job3 = flurm_core:update_job_state(Job2, completed),
    ?assertEqual(completed, flurm_core:job_state(Job3)),
    ?assert(is_integer(Job3#job.start_time)),
    ?assert(is_integer(Job3#job.end_time)).
