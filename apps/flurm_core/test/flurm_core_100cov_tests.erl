%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_core module
%%%
%%% Target: 100% coverage with 4.0 test:source ratio
%%% Source module: flurm_core.erl (154 lines)
%%% Target test lines: ~616 lines
%%%
%%% Tests cover:
%%% - Job API: new_job, job_id, job_state, update_job_state
%%% - Node API: new_node, node_hostname, node_state, update_node_state
%%% - Partition API: new_partition, partition_name, partition_nodes, add_node_to_partition
%%% - Edge cases for all state transitions
%%% - Default values and custom values
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_core_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Job API Tests
%%====================================================================

%% new_job tests
new_job_test_() ->
    [
        {"new_job with empty map uses defaults",
         fun() ->
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
            ?assertEqual(<<"normal">>, Job#job.qos)
         end},

        {"new_job with all custom values",
         fun() ->
            Props = #{
                id => 12345,
                name => <<"test_job">>,
                user => <<"alice">>,
                partition => <<"gpu">>,
                state => running,
                script => <<"#!/bin/bash\necho hello">>,
                num_nodes => 4,
                num_cpus => 16,
                memory_mb => 8192,
                time_limit => 7200,
                priority => 500,
                submit_time => 1000000,
                start_time => 1000100,
                end_time => 1007200,
                allocated_nodes => [<<"node1">>, <<"node2">>],
                exit_code => 0,
                account => <<"research">>,
                qos => <<"high">>
            },
            Job = flurm_core:new_job(Props),
            ?assertEqual(12345, Job#job.id),
            ?assertEqual(<<"test_job">>, Job#job.name),
            ?assertEqual(<<"alice">>, Job#job.user),
            ?assertEqual(<<"gpu">>, Job#job.partition),
            ?assertEqual(running, Job#job.state),
            ?assertEqual(<<"#!/bin/bash\necho hello">>, Job#job.script),
            ?assertEqual(4, Job#job.num_nodes),
            ?assertEqual(16, Job#job.num_cpus),
            ?assertEqual(8192, Job#job.memory_mb),
            ?assertEqual(7200, Job#job.time_limit),
            ?assertEqual(500, Job#job.priority),
            ?assertEqual(1000000, Job#job.submit_time),
            ?assertEqual(1000100, Job#job.start_time),
            ?assertEqual(1007200, Job#job.end_time),
            ?assertEqual([<<"node1">>, <<"node2">>], Job#job.allocated_nodes),
            ?assertEqual(0, Job#job.exit_code),
            ?assertEqual(<<"research">>, Job#job.account),
            ?assertEqual(<<"high">>, Job#job.qos)
         end},

        {"new_job with partial props uses defaults for missing",
         fun() ->
            Props = #{
                name => <<"partial_job">>,
                num_nodes => 2
            },
            Job = flurm_core:new_job(Props),
            ?assertEqual(<<"partial_job">>, Job#job.name),
            ?assertEqual(2, Job#job.num_nodes),
            %% Defaults
            ?assertEqual(<<"unknown">>, Job#job.user),
            ?assertEqual(pending, Job#job.state),
            ?assertEqual(1, Job#job.num_cpus)
         end},

        {"new_job generates unique IDs",
         fun() ->
            Job1 = flurm_core:new_job(#{}),
            Job2 = flurm_core:new_job(#{}),
            Job3 = flurm_core:new_job(#{}),
            ?assertNotEqual(Job1#job.id, Job2#job.id),
            ?assertNotEqual(Job2#job.id, Job3#job.id),
            ?assertNotEqual(Job1#job.id, Job3#job.id)
         end},

        {"new_job with zero values",
         fun() ->
            Props = #{
                num_nodes => 0,
                num_cpus => 0,
                memory_mb => 0,
                time_limit => 0,
                priority => 0
            },
            Job = flurm_core:new_job(Props),
            ?assertEqual(0, Job#job.num_nodes),
            ?assertEqual(0, Job#job.num_cpus),
            ?assertEqual(0, Job#job.memory_mb),
            ?assertEqual(0, Job#job.time_limit),
            ?assertEqual(0, Job#job.priority)
         end},

        {"new_job with empty strings",
         fun() ->
            Props = #{
                name => <<>>,
                user => <<>>,
                partition => <<>>,
                script => <<>>,
                account => <<>>,
                qos => <<>>
            },
            Job = flurm_core:new_job(Props),
            ?assertEqual(<<>>, Job#job.name),
            ?assertEqual(<<>>, Job#job.user),
            ?assertEqual(<<>>, Job#job.partition),
            ?assertEqual(<<>>, Job#job.script),
            ?assertEqual(<<>>, Job#job.account),
            ?assertEqual(<<>>, Job#job.qos)
         end},

        {"new_job with large values",
         fun() ->
            Props = #{
                num_nodes => 10000,
                num_cpus => 1000000,
                memory_mb => 999999999,
                time_limit => 31536000,  % 1 year in seconds
                priority => 999999
            },
            Job = flurm_core:new_job(Props),
            ?assertEqual(10000, Job#job.num_nodes),
            ?assertEqual(1000000, Job#job.num_cpus),
            ?assertEqual(999999999, Job#job.memory_mb),
            ?assertEqual(31536000, Job#job.time_limit),
            ?assertEqual(999999, Job#job.priority)
         end}
    ].

%% job_id tests
job_id_test_() ->
    [
        {"job_id returns job ID",
         fun() ->
            Job = flurm_core:new_job(#{id => 42}),
            ?assertEqual(42, flurm_core:job_id(Job))
         end},

        {"job_id returns auto-generated ID",
         fun() ->
            Job = flurm_core:new_job(#{}),
            Id = flurm_core:job_id(Job),
            ?assert(is_integer(Id)),
            ?assert(Id > 0)
         end}
    ].

%% job_state tests
job_state_test_() ->
    [
        {"job_state returns pending for new job",
         fun() ->
            Job = flurm_core:new_job(#{}),
            ?assertEqual(pending, flurm_core:job_state(Job))
         end},

        {"job_state returns custom state",
         fun() ->
            States = [pending, held, configuring, running, suspended, completing,
                      completed, cancelled, failed, timeout, node_fail, requeued],
            lists:foreach(fun(State) ->
                Job = flurm_core:new_job(#{state => State}),
                ?assertEqual(State, flurm_core:job_state(Job))
            end, States)
         end}
    ].

%% update_job_state tests
update_job_state_test_() ->
    [
        {"update_job_state to running sets start_time",
         fun() ->
            Job = flurm_core:new_job(#{state => pending}),
            ?assertEqual(undefined, Job#job.start_time),
            UpdatedJob = flurm_core:update_job_state(Job, running),
            ?assertEqual(running, UpdatedJob#job.state),
            ?assert(is_integer(UpdatedJob#job.start_time)),
            ?assert(UpdatedJob#job.start_time > 0)
         end},

        {"update_job_state to completed sets end_time",
         fun() ->
            Job = flurm_core:new_job(#{state => running, start_time => 1000000}),
            ?assertEqual(undefined, Job#job.end_time),
            UpdatedJob = flurm_core:update_job_state(Job, completed),
            ?assertEqual(completed, UpdatedJob#job.state),
            ?assert(is_integer(UpdatedJob#job.end_time)),
            ?assert(UpdatedJob#job.end_time > 0)
         end},

        {"update_job_state to failed sets end_time",
         fun() ->
            Job = flurm_core:new_job(#{state => running, start_time => 1000000}),
            UpdatedJob = flurm_core:update_job_state(Job, failed),
            ?assertEqual(failed, UpdatedJob#job.state),
            ?assert(is_integer(UpdatedJob#job.end_time))
         end},

        {"update_job_state to cancelled sets end_time",
         fun() ->
            Job = flurm_core:new_job(#{state => running, start_time => 1000000}),
            UpdatedJob = flurm_core:update_job_state(Job, cancelled),
            ?assertEqual(cancelled, UpdatedJob#job.state),
            ?assert(is_integer(UpdatedJob#job.end_time))
         end},

        {"update_job_state to timeout sets end_time",
         fun() ->
            Job = flurm_core:new_job(#{state => running, start_time => 1000000}),
            UpdatedJob = flurm_core:update_job_state(Job, timeout),
            ?assertEqual(timeout, UpdatedJob#job.state),
            ?assert(is_integer(UpdatedJob#job.end_time))
         end},

        {"update_job_state to node_fail sets end_time",
         fun() ->
            Job = flurm_core:new_job(#{state => running, start_time => 1000000}),
            UpdatedJob = flurm_core:update_job_state(Job, node_fail),
            ?assertEqual(node_fail, UpdatedJob#job.state),
            ?assert(is_integer(UpdatedJob#job.end_time))
         end},

        {"update_job_state to pending does not set timestamps",
         fun() ->
            Job = flurm_core:new_job(#{}),
            UpdatedJob = flurm_core:update_job_state(Job, pending),
            ?assertEqual(pending, UpdatedJob#job.state),
            ?assertEqual(undefined, UpdatedJob#job.start_time),
            ?assertEqual(undefined, UpdatedJob#job.end_time)
         end},

        {"update_job_state to held does not set timestamps",
         fun() ->
            Job = flurm_core:new_job(#{}),
            UpdatedJob = flurm_core:update_job_state(Job, held),
            ?assertEqual(held, UpdatedJob#job.state),
            ?assertEqual(undefined, UpdatedJob#job.start_time)
         end},

        {"update_job_state to configuring does not set timestamps",
         fun() ->
            Job = flurm_core:new_job(#{}),
            UpdatedJob = flurm_core:update_job_state(Job, configuring),
            ?assertEqual(configuring, UpdatedJob#job.state)
         end},

        {"update_job_state to suspended does not set end_time",
         fun() ->
            Job = flurm_core:new_job(#{state => running, start_time => 1000000}),
            UpdatedJob = flurm_core:update_job_state(Job, suspended),
            ?assertEqual(suspended, UpdatedJob#job.state),
            ?assertEqual(undefined, UpdatedJob#job.end_time)
         end},

        {"update_job_state to completing does not set end_time",
         fun() ->
            Job = flurm_core:new_job(#{state => running, start_time => 1000000}),
            UpdatedJob = flurm_core:update_job_state(Job, completing),
            ?assertEqual(completing, UpdatedJob#job.state),
            ?assertEqual(undefined, UpdatedJob#job.end_time)
         end},

        {"update_job_state to requeued does not set end_time",
         fun() ->
            Job = flurm_core:new_job(#{state => running, start_time => 1000000}),
            UpdatedJob = flurm_core:update_job_state(Job, requeued),
            ?assertEqual(requeued, UpdatedJob#job.state),
            ?assertEqual(undefined, UpdatedJob#job.end_time)
         end},

        {"update_job_state preserves other fields",
         fun() ->
            Job = flurm_core:new_job(#{
                id => 999,
                name => <<"preserved">>,
                user => <<"bob">>,
                priority => 500
            }),
            UpdatedJob = flurm_core:update_job_state(Job, running),
            ?assertEqual(999, UpdatedJob#job.id),
            ?assertEqual(<<"preserved">>, UpdatedJob#job.name),
            ?assertEqual(<<"bob">>, UpdatedJob#job.user),
            ?assertEqual(500, UpdatedJob#job.priority)
         end}
    ].

%%====================================================================
%% Node API Tests
%%====================================================================

%% new_node tests
new_node_test_() ->
    [
        {"new_node with minimal props (hostname required)",
         fun() ->
            Props = #{hostname => <<"node001">>},
            Node = flurm_core:new_node(Props),
            ?assertEqual(<<"node001">>, Node#node.hostname),
            ?assertEqual(1, Node#node.cpus),
            ?assertEqual(1024, Node#node.memory_mb),
            ?assertEqual(down, Node#node.state),
            ?assertEqual([], Node#node.features),
            ?assertEqual([], Node#node.partitions),
            ?assertEqual([], Node#node.running_jobs),
            ?assertEqual(0.0, Node#node.load_avg),
            ?assertEqual(0, Node#node.free_memory_mb),
            ?assertEqual(undefined, Node#node.last_heartbeat)
         end},

        {"new_node with all custom values",
         fun() ->
            Props = #{
                hostname => <<"compute-001">>,
                cpus => 64,
                memory_mb => 131072,
                state => up,
                features => [<<"gpu">>, <<"ssd">>, <<"highmem">>],
                partitions => [<<"gpu">>, <<"batch">>],
                running_jobs => [100, 101, 102],
                load_avg => 2.5,
                free_memory_mb => 65536,
                last_heartbeat => 1234567890
            },
            Node = flurm_core:new_node(Props),
            ?assertEqual(<<"compute-001">>, Node#node.hostname),
            ?assertEqual(64, Node#node.cpus),
            ?assertEqual(131072, Node#node.memory_mb),
            ?assertEqual(up, Node#node.state),
            ?assertEqual([<<"gpu">>, <<"ssd">>, <<"highmem">>], Node#node.features),
            ?assertEqual([<<"gpu">>, <<"batch">>], Node#node.partitions),
            ?assertEqual([100, 101, 102], Node#node.running_jobs),
            ?assertEqual(2.5, Node#node.load_avg),
            ?assertEqual(65536, Node#node.free_memory_mb),
            ?assertEqual(1234567890, Node#node.last_heartbeat)
         end},

        {"new_node with partial props",
         fun() ->
            Props = #{
                hostname => <<"partial-node">>,
                cpus => 8,
                state => idle
            },
            Node = flurm_core:new_node(Props),
            ?assertEqual(<<"partial-node">>, Node#node.hostname),
            ?assertEqual(8, Node#node.cpus),
            ?assertEqual(idle, Node#node.state),
            %% Defaults
            ?assertEqual(1024, Node#node.memory_mb),
            ?assertEqual([], Node#node.features)
         end},

        {"new_node with all possible states",
         fun() ->
            States = [up, down, drain, idle, allocated, mixed],
            lists:foreach(fun(State) ->
                Node = flurm_core:new_node(#{hostname => <<"test">>, state => State}),
                ?assertEqual(State, Node#node.state)
            end, States)
         end},

        {"new_node with empty lists",
         fun() ->
            Props = #{
                hostname => <<"empty-lists">>,
                features => [],
                partitions => [],
                running_jobs => []
            },
            Node = flurm_core:new_node(Props),
            ?assertEqual([], Node#node.features),
            ?assertEqual([], Node#node.partitions),
            ?assertEqual([], Node#node.running_jobs)
         end},

        {"new_node with zero values",
         fun() ->
            Props = #{
                hostname => <<"zeros">>,
                cpus => 0,
                memory_mb => 0,
                load_avg => 0.0,
                free_memory_mb => 0
            },
            Node = flurm_core:new_node(Props),
            ?assertEqual(0, Node#node.cpus),
            ?assertEqual(0, Node#node.memory_mb),
            ?assertEqual(0.0, Node#node.load_avg),
            ?assertEqual(0, Node#node.free_memory_mb)
         end},

        {"new_node with large values",
         fun() ->
            Props = #{
                hostname => <<"bignode">>,
                cpus => 1024,
                memory_mb => 4194304,  % 4TB
                load_avg => 999.99,
                free_memory_mb => 4000000
            },
            Node = flurm_core:new_node(Props),
            ?assertEqual(1024, Node#node.cpus),
            ?assertEqual(4194304, Node#node.memory_mb),
            ?assertEqual(999.99, Node#node.load_avg),
            ?assertEqual(4000000, Node#node.free_memory_mb)
         end}
    ].

%% node_hostname tests
node_hostname_test_() ->
    [
        {"node_hostname returns hostname",
         fun() ->
            Node = flurm_core:new_node(#{hostname => <<"compute-node-001">>}),
            ?assertEqual(<<"compute-node-001">>, flurm_core:node_hostname(Node))
         end},

        {"node_hostname with unicode",
         fun() ->
            Node = flurm_core:new_node(#{hostname => <<"ノード-001">>}),
            ?assertEqual(<<"ノード-001">>, flurm_core:node_hostname(Node))
         end},

        {"node_hostname with long name",
         fun() ->
            LongName = list_to_binary(lists:duplicate(255, $x)),
            Node = flurm_core:new_node(#{hostname => LongName}),
            ?assertEqual(LongName, flurm_core:node_hostname(Node))
         end}
    ].

%% node_state tests
node_state_test_() ->
    [
        {"node_state returns state",
         fun() ->
            Node = flurm_core:new_node(#{hostname => <<"test">>, state => up}),
            ?assertEqual(up, flurm_core:node_state(Node))
         end},

        {"node_state returns default down",
         fun() ->
            Node = flurm_core:new_node(#{hostname => <<"test">>}),
            ?assertEqual(down, flurm_core:node_state(Node))
         end},

        {"node_state returns all possible states",
         fun() ->
            States = [up, down, drain, idle, allocated, mixed],
            lists:foreach(fun(State) ->
                Node = flurm_core:new_node(#{hostname => <<"test">>, state => State}),
                ?assertEqual(State, flurm_core:node_state(Node))
            end, States)
         end}
    ].

%% update_node_state tests
update_node_state_test_() ->
    [
        {"update_node_state changes state",
         fun() ->
            Node = flurm_core:new_node(#{hostname => <<"test">>, state => down}),
            UpdatedNode = flurm_core:update_node_state(Node, up),
            ?assertEqual(up, UpdatedNode#node.state)
         end},

        {"update_node_state preserves other fields",
         fun() ->
            Node = flurm_core:new_node(#{
                hostname => <<"preserve">>,
                cpus => 32,
                memory_mb => 65536,
                state => down,
                features => [<<"gpu">>]
            }),
            UpdatedNode = flurm_core:update_node_state(Node, up),
            ?assertEqual(up, UpdatedNode#node.state),
            ?assertEqual(<<"preserve">>, UpdatedNode#node.hostname),
            ?assertEqual(32, UpdatedNode#node.cpus),
            ?assertEqual(65536, UpdatedNode#node.memory_mb),
            ?assertEqual([<<"gpu">>], UpdatedNode#node.features)
         end},

        {"update_node_state to same state",
         fun() ->
            Node = flurm_core:new_node(#{hostname => <<"test">>, state => up}),
            UpdatedNode = flurm_core:update_node_state(Node, up),
            ?assertEqual(up, UpdatedNode#node.state)
         end},

        {"update_node_state through all states",
         fun() ->
            States = [up, down, drain, idle, allocated, mixed],
            Node = flurm_core:new_node(#{hostname => <<"cycling">>}),
            lists:foldl(fun(State, CurrentNode) ->
                NewNode = flurm_core:update_node_state(CurrentNode, State),
                ?assertEqual(State, NewNode#node.state),
                NewNode
            end, Node, States)
         end}
    ].

%%====================================================================
%% Partition API Tests
%%====================================================================

%% new_partition tests
new_partition_test_() ->
    [
        {"new_partition with minimal props (name required)",
         fun() ->
            Props = #{name => <<"default">>},
            Partition = flurm_core:new_partition(Props),
            ?assertEqual(<<"default">>, Partition#partition.name),
            ?assertEqual(up, Partition#partition.state),
            ?assertEqual([], Partition#partition.nodes),
            ?assertEqual(86400, Partition#partition.max_time),
            ?assertEqual(3600, Partition#partition.default_time),
            ?assertEqual(100, Partition#partition.max_nodes),
            ?assertEqual(100, Partition#partition.priority),
            ?assertEqual(false, Partition#partition.allow_root)
         end},

        {"new_partition with all custom values",
         fun() ->
            Props = #{
                name => <<"gpu-partition">>,
                state => down,
                nodes => [<<"gpu-001">>, <<"gpu-002">>, <<"gpu-003">>],
                max_time => 604800,  % 1 week
                default_time => 86400,  % 1 day
                max_nodes => 1000,
                priority => 9999,
                allow_root => true
            },
            Partition = flurm_core:new_partition(Props),
            ?assertEqual(<<"gpu-partition">>, Partition#partition.name),
            ?assertEqual(down, Partition#partition.state),
            ?assertEqual([<<"gpu-001">>, <<"gpu-002">>, <<"gpu-003">>], Partition#partition.nodes),
            ?assertEqual(604800, Partition#partition.max_time),
            ?assertEqual(86400, Partition#partition.default_time),
            ?assertEqual(1000, Partition#partition.max_nodes),
            ?assertEqual(9999, Partition#partition.priority),
            ?assertEqual(true, Partition#partition.allow_root)
         end},

        {"new_partition with partial props",
         fun() ->
            Props = #{
                name => <<"partial">>,
                priority => 500
            },
            Partition = flurm_core:new_partition(Props),
            ?assertEqual(<<"partial">>, Partition#partition.name),
            ?assertEqual(500, Partition#partition.priority),
            %% Defaults
            ?assertEqual(up, Partition#partition.state),
            ?assertEqual([], Partition#partition.nodes)
         end},

        {"new_partition with all states",
         fun() ->
            States = [up, down, drain],
            lists:foreach(fun(State) ->
                Partition = flurm_core:new_partition(#{name => <<"test">>, state => State}),
                ?assertEqual(State, Partition#partition.state)
            end, States)
         end},

        {"new_partition with zero values",
         fun() ->
            Props = #{
                name => <<"zeros">>,
                max_time => 0,
                default_time => 0,
                max_nodes => 0,
                priority => 0
            },
            Partition = flurm_core:new_partition(Props),
            ?assertEqual(0, Partition#partition.max_time),
            ?assertEqual(0, Partition#partition.default_time),
            ?assertEqual(0, Partition#partition.max_nodes),
            ?assertEqual(0, Partition#partition.priority)
         end}
    ].

%% partition_name tests
partition_name_test_() ->
    [
        {"partition_name returns name",
         fun() ->
            Partition = flurm_core:new_partition(#{name => <<"batch">>}),
            ?assertEqual(<<"batch">>, flurm_core:partition_name(Partition))
         end},

        {"partition_name with unicode",
         fun() ->
            Partition = flurm_core:new_partition(#{name => <<"パーティション">>}),
            ?assertEqual(<<"パーティション">>, flurm_core:partition_name(Partition))
         end},

        {"partition_name with empty string",
         fun() ->
            Partition = flurm_core:new_partition(#{name => <<>>}),
            ?assertEqual(<<>>, flurm_core:partition_name(Partition))
         end}
    ].

%% partition_nodes tests
partition_nodes_test_() ->
    [
        {"partition_nodes returns empty list",
         fun() ->
            Partition = flurm_core:new_partition(#{name => <<"empty">>}),
            ?assertEqual([], flurm_core:partition_nodes(Partition))
         end},

        {"partition_nodes returns node list",
         fun() ->
            Nodes = [<<"node1">>, <<"node2">>, <<"node3">>],
            Partition = flurm_core:new_partition(#{name => <<"with_nodes">>, nodes => Nodes}),
            ?assertEqual(Nodes, flurm_core:partition_nodes(Partition))
         end},

        {"partition_nodes with many nodes",
         fun() ->
            Nodes = [list_to_binary("node" ++ integer_to_list(I)) || I <- lists:seq(1, 100)],
            Partition = flurm_core:new_partition(#{name => <<"many">>, nodes => Nodes}),
            ?assertEqual(100, length(flurm_core:partition_nodes(Partition)))
         end}
    ].

%% add_node_to_partition tests
add_node_to_partition_test_() ->
    [
        {"add_node_to_partition adds node to empty list",
         fun() ->
            Partition = flurm_core:new_partition(#{name => <<"test">>}),
            UpdatedPartition = flurm_core:add_node_to_partition(Partition, <<"node1">>),
            ?assertEqual([<<"node1">>], UpdatedPartition#partition.nodes)
         end},

        {"add_node_to_partition adds node to existing list",
         fun() ->
            Partition = flurm_core:new_partition(#{name => <<"test">>, nodes => [<<"node1">>]}),
            UpdatedPartition = flurm_core:add_node_to_partition(Partition, <<"node2">>),
            Nodes = UpdatedPartition#partition.nodes,
            ?assertEqual(2, length(Nodes)),
            ?assert(lists:member(<<"node1">>, Nodes)),
            ?assert(lists:member(<<"node2">>, Nodes))
         end},

        {"add_node_to_partition does not add duplicate",
         fun() ->
            Partition = flurm_core:new_partition(#{name => <<"test">>, nodes => [<<"node1">>]}),
            UpdatedPartition = flurm_core:add_node_to_partition(Partition, <<"node1">>),
            ?assertEqual([<<"node1">>], UpdatedPartition#partition.nodes)
         end},

        {"add_node_to_partition preserves other fields",
         fun() ->
            Partition = flurm_core:new_partition(#{
                name => <<"preserve">>,
                state => down,
                priority => 999,
                nodes => [<<"existing">>]
            }),
            UpdatedPartition = flurm_core:add_node_to_partition(Partition, <<"new">>),
            ?assertEqual(<<"preserve">>, UpdatedPartition#partition.name),
            ?assertEqual(down, UpdatedPartition#partition.state),
            ?assertEqual(999, UpdatedPartition#partition.priority)
         end},

        {"add_node_to_partition multiple times",
         fun() ->
            Partition = flurm_core:new_partition(#{name => <<"multi">>}),
            P1 = flurm_core:add_node_to_partition(Partition, <<"node1">>),
            P2 = flurm_core:add_node_to_partition(P1, <<"node2">>),
            P3 = flurm_core:add_node_to_partition(P2, <<"node3">>),
            Nodes = P3#partition.nodes,
            ?assertEqual(3, length(Nodes)),
            ?assert(lists:member(<<"node1">>, Nodes)),
            ?assert(lists:member(<<"node2">>, Nodes)),
            ?assert(lists:member(<<"node3">>, Nodes))
         end},

        {"add_node_to_partition with empty node name",
         fun() ->
            Partition = flurm_core:new_partition(#{name => <<"test">>}),
            UpdatedPartition = flurm_core:add_node_to_partition(Partition, <<>>),
            ?assert(lists:member(<<>>, UpdatedPartition#partition.nodes))
         end},

        {"add_node_to_partition with unicode node name",
         fun() ->
            Partition = flurm_core:new_partition(#{name => <<"test">>}),
            UpdatedPartition = flurm_core:add_node_to_partition(Partition, <<"ノード-001">>),
            ?assert(lists:member(<<"ノード-001">>, UpdatedPartition#partition.nodes))
         end}
    ].

%%====================================================================
%% Integration Tests
%%====================================================================

integration_test_() ->
    [
        {"job lifecycle simulation",
         fun() ->
            %% Create a job
            Job1 = flurm_core:new_job(#{name => <<"simulation">>}),
            ?assertEqual(pending, flurm_core:job_state(Job1)),
            
            %% Job starts running
            Job2 = flurm_core:update_job_state(Job1, running),
            ?assertEqual(running, flurm_core:job_state(Job2)),
            ?assert(Job2#job.start_time =/= undefined),
            
            %% Job completes
            Job3 = flurm_core:update_job_state(Job2, completed),
            ?assertEqual(completed, flurm_core:job_state(Job3)),
            ?assert(Job3#job.end_time =/= undefined),
            ?assert(Job3#job.end_time >= Job3#job.start_time)
         end},

        {"node allocation simulation",
         fun() ->
            %% Create nodes in different states
            Node1 = flurm_core:new_node(#{hostname => <<"n1">>, state => up}),
            Node2 = flurm_core:new_node(#{hostname => <<"n2">>, state => up}),
            
            %% Simulate allocation
            AllocNode1 = flurm_core:update_node_state(Node1, allocated),
            ?assertEqual(allocated, flurm_core:node_state(AllocNode1)),
            ?assertEqual(up, flurm_core:node_state(Node2)),
            
            %% Simulate deallocation
            FreeNode1 = flurm_core:update_node_state(AllocNode1, idle),
            ?assertEqual(idle, flurm_core:node_state(FreeNode1))
         end},

        {"partition with nodes simulation",
         fun() ->
            %% Create partition
            Part1 = flurm_core:new_partition(#{name => <<"compute">>}),
            ?assertEqual([], flurm_core:partition_nodes(Part1)),
            
            %% Add nodes
            Part2 = flurm_core:add_node_to_partition(Part1, <<"compute-001">>),
            Part3 = flurm_core:add_node_to_partition(Part2, <<"compute-002">>),
            Part4 = flurm_core:add_node_to_partition(Part3, <<"compute-003">>),
            
            ?assertEqual(3, length(flurm_core:partition_nodes(Part4))),
            
            %% Try to add duplicate
            Part5 = flurm_core:add_node_to_partition(Part4, <<"compute-001">>),
            ?assertEqual(3, length(flurm_core:partition_nodes(Part5)))
         end},

        {"job failure scenarios",
         fun() ->
            FailureStates = [failed, cancelled, timeout, node_fail],
            lists:foreach(fun(FailState) ->
                Job1 = flurm_core:new_job(#{name => <<"failure_test">>}),
                Job2 = flurm_core:update_job_state(Job1, running),
                Job3 = flurm_core:update_job_state(Job2, FailState),
                ?assertEqual(FailState, flurm_core:job_state(Job3)),
                ?assert(Job3#job.end_time =/= undefined)
            end, FailureStates)
         end}
    ].

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    [
        {"job with very long script",
         fun() ->
            LongScript = list_to_binary(lists:duplicate(100000, $x)),
            Job = flurm_core:new_job(#{script => LongScript}),
            ?assertEqual(100000, byte_size(Job#job.script))
         end},

        {"node with many features",
         fun() ->
            Features = [list_to_binary("feature" ++ integer_to_list(I)) || I <- lists:seq(1, 100)],
            Node = flurm_core:new_node(#{hostname => <<"test">>, features => Features}),
            ?assertEqual(100, length(Node#node.features))
         end},

        {"partition with many nodes",
         fun() ->
            Nodes = [list_to_binary("node" ++ integer_to_list(I)) || I <- lists:seq(1, 10000)],
            Partition = flurm_core:new_partition(#{name => <<"big">>, nodes => Nodes}),
            ?assertEqual(10000, length(flurm_core:partition_nodes(Partition)))
         end}
    ].
