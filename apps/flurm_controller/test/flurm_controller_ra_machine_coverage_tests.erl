%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Ra Machine Coverage Tests
%%%
%%% Comprehensive coverage tests for flurm_controller_ra_machine module.
%%% Tests all exported -ifdef(TEST) helper functions for maximum coverage.
%%%
%%% These tests focus on:
%%% - create_job - job record creation
%%% - update_job_state_internal - job state updates
%%% - apply_job_updates - job field updates
%%% - apply_heartbeat - node heartbeat updates
%%% - apply_partition_updates - partition field updates
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_ra_machine_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% create_job Tests
%%====================================================================

create_job_test_() ->
    [
        {"creates job with id", fun() ->
            Job = flurm_controller_ra_machine:create_job(123, #{}),
            ?assertEqual(123, Job#job.id)
        end},
        {"creates job with name from spec", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{name => <<"test_job">>}),
            ?assertEqual(<<"test_job">>, Job#job.name)
        end},
        {"creates job with default name", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(<<"unnamed">>, Job#job.name)
        end},
        {"creates job with user from spec", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{user => <<"testuser">>}),
            ?assertEqual(<<"testuser">>, Job#job.user)
        end},
        {"creates job with default user", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(<<>>, Job#job.user)
        end},
        {"creates job with partition from spec", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{partition => <<"gpu">>}),
            ?assertEqual(<<"gpu">>, Job#job.partition)
        end},
        {"creates job with default partition", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(<<"default">>, Job#job.partition)
        end},
        {"creates job in pending state", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(pending, Job#job.state)
        end},
        {"creates job with script from spec", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{script => <<"#!/bin/bash\necho hi">>}),
            ?assertEqual(<<"#!/bin/bash\necho hi">>, Job#job.script)
        end},
        {"creates job with num_nodes from spec", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{num_nodes => 4}),
            ?assertEqual(4, Job#job.num_nodes)
        end},
        {"creates job with default num_nodes", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(1, Job#job.num_nodes)
        end},
        {"creates job with num_cpus from spec", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{num_cpus => 8}),
            ?assertEqual(8, Job#job.num_cpus)
        end},
        {"creates job with default num_cpus", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(1, Job#job.num_cpus)
        end},
        {"creates job with memory_mb from spec", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{memory_mb => 4096}),
            ?assertEqual(4096, Job#job.memory_mb)
        end},
        {"creates job with default memory_mb", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(1024, Job#job.memory_mb)
        end},
        {"creates job with time_limit from spec", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{time_limit => 7200}),
            ?assertEqual(7200, Job#job.time_limit)
        end},
        {"creates job with default time_limit", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(3600, Job#job.time_limit)
        end},
        {"creates job with priority from spec", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{priority => 500}),
            ?assertEqual(500, Job#job.priority)
        end},
        {"creates job with default priority", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(100, Job#job.priority)
        end},
        {"creates job with submit_time", fun() ->
            Before = erlang:system_time(second),
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            After = erlang:system_time(second),
            ?assert(Job#job.submit_time >= Before),
            ?assert(Job#job.submit_time =< After)
        end},
        {"creates job with undefined start_time", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(undefined, Job#job.start_time)
        end},
        {"creates job with undefined end_time", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(undefined, Job#job.end_time)
        end},
        {"creates job with empty allocated_nodes", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual([], Job#job.allocated_nodes)
        end},
        {"creates job with undefined exit_code", fun() ->
            Job = flurm_controller_ra_machine:create_job(1, #{}),
            ?assertEqual(undefined, Job#job.exit_code)
        end},
        {"creates full job spec", fun() ->
            Spec = #{
                name => <<"full_job">>,
                user => <<"admin">>,
                partition => <<"batch">>,
                script => <<"#!/bin/bash">>,
                num_nodes => 2,
                num_cpus => 16,
                memory_mb => 8192,
                time_limit => 86400,
                priority => 1000
            },
            Job = flurm_controller_ra_machine:create_job(42, Spec),
            ?assertEqual(42, Job#job.id),
            ?assertEqual(<<"full_job">>, Job#job.name),
            ?assertEqual(<<"admin">>, Job#job.user),
            ?assertEqual(<<"batch">>, Job#job.partition),
            ?assertEqual(<<"#!/bin/bash">>, Job#job.script),
            ?assertEqual(2, Job#job.num_nodes),
            ?assertEqual(16, Job#job.num_cpus),
            ?assertEqual(8192, Job#job.memory_mb),
            ?assertEqual(86400, Job#job.time_limit),
            ?assertEqual(1000, Job#job.priority)
        end}
    ].

%%====================================================================
%% update_job_state_internal Tests
%%====================================================================

update_job_state_internal_test_() ->
    [
        {"running sets start_time", fun() ->
            Before = erlang:system_time(second),
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:update_job_state_internal(Job, running),
            After = erlang:system_time(second),
            ?assertEqual(running, UpdatedJob#job.state),
            ?assert(UpdatedJob#job.start_time >= Before),
            ?assert(UpdatedJob#job.start_time =< After)
        end},
        {"completed sets end_time", fun() ->
            Before = erlang:system_time(second),
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:update_job_state_internal(Job, completed),
            After = erlang:system_time(second),
            ?assertEqual(completed, UpdatedJob#job.state),
            ?assert(UpdatedJob#job.end_time >= Before),
            ?assert(UpdatedJob#job.end_time =< After)
        end},
        {"failed sets end_time", fun() ->
            Before = erlang:system_time(second),
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:update_job_state_internal(Job, failed),
            After = erlang:system_time(second),
            ?assertEqual(failed, UpdatedJob#job.state),
            ?assert(UpdatedJob#job.end_time >= Before),
            ?assert(UpdatedJob#job.end_time =< After)
        end},
        {"cancelled sets end_time", fun() ->
            Before = erlang:system_time(second),
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:update_job_state_internal(Job, cancelled),
            After = erlang:system_time(second),
            ?assertEqual(cancelled, UpdatedJob#job.state),
            ?assert(UpdatedJob#job.end_time >= Before),
            ?assert(UpdatedJob#job.end_time =< After)
        end},
        {"timeout sets end_time", fun() ->
            Before = erlang:system_time(second),
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:update_job_state_internal(Job, timeout),
            After = erlang:system_time(second),
            ?assertEqual(timeout, UpdatedJob#job.state),
            ?assert(UpdatedJob#job.end_time >= Before),
            ?assert(UpdatedJob#job.end_time =< After)
        end},
        {"node_fail sets end_time", fun() ->
            Before = erlang:system_time(second),
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:update_job_state_internal(Job, node_fail),
            After = erlang:system_time(second),
            ?assertEqual(node_fail, UpdatedJob#job.state),
            ?assert(UpdatedJob#job.end_time >= Before),
            ?assert(UpdatedJob#job.end_time =< After)
        end},
        {"pending only sets state", fun() ->
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:update_job_state_internal(Job, pending),
            ?assertEqual(pending, UpdatedJob#job.state),
            ?assertEqual(Job#job.start_time, UpdatedJob#job.start_time),
            ?assertEqual(Job#job.end_time, UpdatedJob#job.end_time)
        end},
        {"configuring only sets state", fun() ->
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:update_job_state_internal(Job, configuring),
            ?assertEqual(configuring, UpdatedJob#job.state)
        end},
        {"held only sets state", fun() ->
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:update_job_state_internal(Job, held),
            ?assertEqual(held, UpdatedJob#job.state)
        end}
    ].

%%====================================================================
%% apply_job_updates Tests
%%====================================================================

apply_job_updates_test_() ->
    [
        {"empty updates returns unchanged", fun() ->
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:apply_job_updates(Job, #{}),
            ?assertEqual(Job, UpdatedJob)
        end},
        {"updates state", fun() ->
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:apply_job_updates(Job, #{state => running}),
            ?assertEqual(running, UpdatedJob#job.state)
        end},
        {"updates allocated_nodes", fun() ->
            Job = create_test_job(),
            Nodes = [<<"node1">>, <<"node2">>],
            UpdatedJob = flurm_controller_ra_machine:apply_job_updates(Job, #{allocated_nodes => Nodes}),
            ?assertEqual(Nodes, UpdatedJob#job.allocated_nodes)
        end},
        {"updates start_time", fun() ->
            Job = create_test_job(),
            Now = erlang:system_time(second),
            UpdatedJob = flurm_controller_ra_machine:apply_job_updates(Job, #{start_time => Now}),
            ?assertEqual(Now, UpdatedJob#job.start_time)
        end},
        {"updates end_time", fun() ->
            Job = create_test_job(),
            Now = erlang:system_time(second),
            UpdatedJob = flurm_controller_ra_machine:apply_job_updates(Job, #{end_time => Now}),
            ?assertEqual(Now, UpdatedJob#job.end_time)
        end},
        {"updates exit_code", fun() ->
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:apply_job_updates(Job, #{exit_code => 0}),
            ?assertEqual(0, UpdatedJob#job.exit_code)
        end},
        {"updates priority", fun() ->
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:apply_job_updates(Job, #{priority => 500}),
            ?assertEqual(500, UpdatedJob#job.priority)
        end},
        {"ignores unknown keys", fun() ->
            Job = create_test_job(),
            UpdatedJob = flurm_controller_ra_machine:apply_job_updates(Job, #{unknown_key => value}),
            ?assertEqual(Job, UpdatedJob)
        end},
        {"applies multiple updates", fun() ->
            Job = create_test_job(),
            Updates = #{
                state => running,
                allocated_nodes => [<<"node1">>],
                start_time => 1700000000,
                priority => 200
            },
            UpdatedJob = flurm_controller_ra_machine:apply_job_updates(Job, Updates),
            ?assertEqual(running, UpdatedJob#job.state),
            ?assertEqual([<<"node1">>], UpdatedJob#job.allocated_nodes),
            ?assertEqual(1700000000, UpdatedJob#job.start_time),
            ?assertEqual(200, UpdatedJob#job.priority)
        end}
    ].

%%====================================================================
%% apply_heartbeat Tests
%%====================================================================

apply_heartbeat_test_() ->
    [
        {"updates load_avg", fun() ->
            Node = create_test_node(),
            UpdatedNode = flurm_controller_ra_machine:apply_heartbeat(Node, #{load_avg => 2.5}),
            ?assertEqual(2.5, UpdatedNode#node.load_avg)
        end},
        {"updates free_memory_mb", fun() ->
            Node = create_test_node(),
            UpdatedNode = flurm_controller_ra_machine:apply_heartbeat(Node, #{free_memory_mb => 32000}),
            ?assertEqual(32000, UpdatedNode#node.free_memory_mb)
        end},
        {"updates running_jobs", fun() ->
            Node = create_test_node(),
            Jobs = [1, 2, 3],
            UpdatedNode = flurm_controller_ra_machine:apply_heartbeat(Node, #{running_jobs => Jobs}),
            ?assertEqual(Jobs, UpdatedNode#node.running_jobs)
        end},
        {"updates last_heartbeat", fun() ->
            Before = erlang:system_time(second),
            Node = create_test_node(),
            UpdatedNode = flurm_controller_ra_machine:apply_heartbeat(Node, #{}),
            After = erlang:system_time(second),
            ?assert(UpdatedNode#node.last_heartbeat >= Before),
            ?assert(UpdatedNode#node.last_heartbeat =< After)
        end},
        {"keeps existing values for missing keys", fun() ->
            Node = create_test_node(),
            %% Only update load_avg
            UpdatedNode = flurm_controller_ra_machine:apply_heartbeat(Node, #{load_avg => 1.0}),
            ?assertEqual(1.0, UpdatedNode#node.load_avg),
            %% Other fields should be unchanged (except last_heartbeat)
            ?assertEqual(Node#node.free_memory_mb, UpdatedNode#node.free_memory_mb),
            ?assertEqual(Node#node.running_jobs, UpdatedNode#node.running_jobs)
        end},
        {"applies all heartbeat data", fun() ->
            Node = create_test_node(),
            HeartbeatData = #{
                load_avg => 3.14,
                free_memory_mb => 50000,
                running_jobs => [10, 20]
            },
            UpdatedNode = flurm_controller_ra_machine:apply_heartbeat(Node, HeartbeatData),
            ?assertEqual(3.14, UpdatedNode#node.load_avg),
            ?assertEqual(50000, UpdatedNode#node.free_memory_mb),
            ?assertEqual([10, 20], UpdatedNode#node.running_jobs)
        end}
    ].

%%====================================================================
%% apply_partition_updates Tests
%%====================================================================

apply_partition_updates_test_() ->
    [
        {"empty updates returns unchanged", fun() ->
            Partition = create_test_partition(),
            UpdatedPartition = flurm_controller_ra_machine:apply_partition_updates(Partition, #{}),
            ?assertEqual(Partition, UpdatedPartition)
        end},
        {"updates state", fun() ->
            Partition = create_test_partition(),
            UpdatedPartition = flurm_controller_ra_machine:apply_partition_updates(Partition, #{state => down}),
            ?assertEqual(down, UpdatedPartition#partition.state)
        end},
        {"updates nodes", fun() ->
            Partition = create_test_partition(),
            Nodes = [<<"node1">>, <<"node2">>, <<"node3">>],
            UpdatedPartition = flurm_controller_ra_machine:apply_partition_updates(Partition, #{nodes => Nodes}),
            ?assertEqual(Nodes, UpdatedPartition#partition.nodes)
        end},
        {"updates max_time", fun() ->
            Partition = create_test_partition(),
            UpdatedPartition = flurm_controller_ra_machine:apply_partition_updates(Partition, #{max_time => 172800}),
            ?assertEqual(172800, UpdatedPartition#partition.max_time)
        end},
        {"updates default_time", fun() ->
            Partition = create_test_partition(),
            UpdatedPartition = flurm_controller_ra_machine:apply_partition_updates(Partition, #{default_time => 7200}),
            ?assertEqual(7200, UpdatedPartition#partition.default_time)
        end},
        {"updates max_nodes", fun() ->
            Partition = create_test_partition(),
            UpdatedPartition = flurm_controller_ra_machine:apply_partition_updates(Partition, #{max_nodes => 100}),
            ?assertEqual(100, UpdatedPartition#partition.max_nodes)
        end},
        {"updates priority", fun() ->
            Partition = create_test_partition(),
            UpdatedPartition = flurm_controller_ra_machine:apply_partition_updates(Partition, #{priority => 500}),
            ?assertEqual(500, UpdatedPartition#partition.priority)
        end},
        {"ignores unknown keys", fun() ->
            Partition = create_test_partition(),
            UpdatedPartition = flurm_controller_ra_machine:apply_partition_updates(Partition, #{unknown => value}),
            ?assertEqual(Partition, UpdatedPartition)
        end},
        {"applies multiple updates", fun() ->
            Partition = create_test_partition(),
            Updates = #{
                state => drain,
                nodes => [<<"node1">>],
                max_time => 43200,
                priority => 200
            },
            UpdatedPartition = flurm_controller_ra_machine:apply_partition_updates(Partition, Updates),
            ?assertEqual(drain, UpdatedPartition#partition.state),
            ?assertEqual([<<"node1">>], UpdatedPartition#partition.nodes),
            ?assertEqual(43200, UpdatedPartition#partition.max_time),
            ?assertEqual(200, UpdatedPartition#partition.priority)
        end}
    ].

%%====================================================================
%% Ra Machine Callback Tests
%%====================================================================

ra_machine_callbacks_test_() ->
    [
        {"init returns state record", fun() ->
            State = flurm_controller_ra_machine:init(#{}),
            %% State should be a record/tuple
            ?assert(is_tuple(State))
        end},
        {"overview returns map", fun() ->
            State = flurm_controller_ra_machine:init(#{}),
            Overview = flurm_controller_ra_machine:overview(State),
            ?assert(is_map(Overview)),
            ?assert(maps:is_key(job_count, Overview)),
            ?assert(maps:is_key(node_count, Overview)),
            ?assert(maps:is_key(partition_count, Overview)),
            ?assert(maps:is_key(next_job_id, Overview)),
            ?assert(maps:is_key(last_applied_index, Overview))
        end},
        {"state_enter leader returns empty effects", fun() ->
            State = flurm_controller_ra_machine:init(#{}),
            Effects = flurm_controller_ra_machine:state_enter(leader, State),
            ?assertEqual([], Effects)
        end},
        {"state_enter follower returns empty effects", fun() ->
            State = flurm_controller_ra_machine:init(#{}),
            Effects = flurm_controller_ra_machine:state_enter(follower, State),
            ?assertEqual([], Effects)
        end},
        {"state_enter candidate returns empty effects", fun() ->
            State = flurm_controller_ra_machine:init(#{}),
            Effects = flurm_controller_ra_machine:state_enter(candidate, State),
            ?assertEqual([], Effects)
        end},
        {"state_enter unknown returns empty effects", fun() ->
            State = flurm_controller_ra_machine:init(#{}),
            Effects = flurm_controller_ra_machine:state_enter(some_state, State),
            ?assertEqual([], Effects)
        end},
        {"snapshot_installed returns ok", fun() ->
            State = flurm_controller_ra_machine:init(#{}),
            Result = flurm_controller_ra_machine:snapshot_installed(#{}, State),
            ?assertEqual(ok, Result)
        end}
    ].

%%====================================================================
%% Helper Functions
%%====================================================================

create_test_job() ->
    #job{
        id = 1,
        name = <<"test">>,
        user = <<"user">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined
    }.

create_test_node() ->
    #node{
        hostname = <<"testnode">>,
        cpus = 32,
        memory_mb = 65536,
        state = idle,
        features = [],
        partitions = [<<"default">>],
        running_jobs = [],
        load_avg = 0.0,
        free_memory_mb = 65536,
        last_heartbeat = erlang:system_time(second)
    }.

create_test_partition() ->
    #partition{
        name = <<"default">>,
        nodes = [],
        state = up,
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 100
    }.
