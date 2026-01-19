%%%-------------------------------------------------------------------
%%% @doc ifdef-exported helper function tests for flurm_controller_ra_machine
%%%
%%% These tests specifically target the internal helper functions that are
%%% exported via -ifdef(TEST) for code coverage. No meck is used, allowing
%%% proper cover instrumentation.
%%%
%%% Tested functions:
%%% - create_job/2
%%% - update_job_state_internal/2
%%% - apply_job_updates/2
%%% - apply_heartbeat/2
%%% - apply_partition_updates/2
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_ra_machine_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% create_job/2 Tests
%%====================================================================

create_job_test_() ->
    [
     {"creates job with empty spec uses defaults", fun create_job_empty_spec/0},
     {"creates job with full spec", fun create_job_full_spec/0},
     {"creates job with partial spec", fun create_job_partial_spec/0},
     {"assigns correct job id", fun create_job_assigns_id/0},
     {"sets submit_time to current time", fun create_job_sets_submit_time/0},
     {"initializes state as pending", fun create_job_initial_state/0},
     {"initializes allocated_nodes as empty list", fun create_job_no_allocated_nodes/0},
     {"initializes exit_code as undefined", fun create_job_no_exit_code/0}
    ].

create_job_empty_spec() ->
    Job = flurm_controller_ra_machine:create_job(1, #{}),
    ?assertEqual(1, Job#job.id),
    ?assertEqual(<<"unnamed">>, Job#job.name),
    ?assertEqual(<<>>, Job#job.user),
    ?assertEqual(<<"default">>, Job#job.partition),
    ?assertEqual(pending, Job#job.state),
    ?assertEqual(<<>>, Job#job.script),
    ?assertEqual(1, Job#job.num_nodes),
    ?assertEqual(1, Job#job.num_cpus),
    ?assertEqual(1024, Job#job.memory_mb),
    ?assertEqual(3600, Job#job.time_limit),
    ?assertEqual(100, Job#job.priority).

create_job_full_spec() ->
    JobSpec = #{
        name => <<"production_job">>,
        user => <<"admin">>,
        partition => <<"gpu">>,
        script => <<"#!/bin/bash\necho hello">>,
        num_nodes => 4,
        num_cpus => 32,
        memory_mb => 65536,
        time_limit => 86400,
        priority => 1000
    },
    Job = flurm_controller_ra_machine:create_job(42, JobSpec),
    ?assertEqual(42, Job#job.id),
    ?assertEqual(<<"production_job">>, Job#job.name),
    ?assertEqual(<<"admin">>, Job#job.user),
    ?assertEqual(<<"gpu">>, Job#job.partition),
    ?assertEqual(<<"#!/bin/bash\necho hello">>, Job#job.script),
    ?assertEqual(4, Job#job.num_nodes),
    ?assertEqual(32, Job#job.num_cpus),
    ?assertEqual(65536, Job#job.memory_mb),
    ?assertEqual(86400, Job#job.time_limit),
    ?assertEqual(1000, Job#job.priority).

create_job_partial_spec() ->
    %% Only provide name and num_cpus, rest should use defaults
    JobSpec = #{name => <<"partial_job">>, num_cpus => 8},
    Job = flurm_controller_ra_machine:create_job(99, JobSpec),
    ?assertEqual(99, Job#job.id),
    ?assertEqual(<<"partial_job">>, Job#job.name),
    ?assertEqual(<<>>, Job#job.user),  % default
    ?assertEqual(<<"default">>, Job#job.partition),  % default
    ?assertEqual(8, Job#job.num_cpus),  % provided
    ?assertEqual(1, Job#job.num_nodes),  % default
    ?assertEqual(1024, Job#job.memory_mb),  % default
    ?assertEqual(3600, Job#job.time_limit),  % default
    ?assertEqual(100, Job#job.priority).  % default

create_job_assigns_id() ->
    Job1 = flurm_controller_ra_machine:create_job(1, #{}),
    Job2 = flurm_controller_ra_machine:create_job(999, #{}),
    Job3 = flurm_controller_ra_machine:create_job(1000000, #{}),
    ?assertEqual(1, Job1#job.id),
    ?assertEqual(999, Job2#job.id),
    ?assertEqual(1000000, Job3#job.id).

create_job_sets_submit_time() ->
    Before = erlang:system_time(second),
    Job = flurm_controller_ra_machine:create_job(1, #{}),
    After = erlang:system_time(second),
    ?assert(Job#job.submit_time >= Before),
    ?assert(Job#job.submit_time =< After).

create_job_initial_state() ->
    Job = flurm_controller_ra_machine:create_job(1, #{}),
    ?assertEqual(pending, Job#job.state).

create_job_no_allocated_nodes() ->
    Job = flurm_controller_ra_machine:create_job(1, #{}),
    ?assertEqual([], Job#job.allocated_nodes).

create_job_no_exit_code() ->
    Job = flurm_controller_ra_machine:create_job(1, #{}),
    ?assertEqual(undefined, Job#job.exit_code).

%%====================================================================
%% update_job_state_internal/2 Tests
%%====================================================================

update_job_state_internal_test_() ->
    [
     {"running state sets start_time", fun update_state_running/0},
     {"completed state sets end_time", fun update_state_completed/0},
     {"failed state sets end_time", fun update_state_failed/0},
     {"cancelled state sets end_time", fun update_state_cancelled/0},
     {"timeout state sets end_time", fun update_state_timeout/0},
     {"node_fail state sets end_time", fun update_state_node_fail/0},
     {"pending state only updates state", fun update_state_pending/0},
     {"configuring state only updates state", fun update_state_configuring/0},
     {"held state only updates state", fun update_state_held/0},
     {"completing state only updates state", fun update_state_completing/0},
     {"requeued state only updates state", fun update_state_requeued/0},
     {"running preserves other fields", fun update_running_preserves_fields/0},
     {"terminal state preserves other fields", fun update_terminal_preserves_fields/0}
    ].

update_state_running() ->
    Job = #job{id = 1, state = pending, start_time = undefined, end_time = undefined},
    Before = erlang:system_time(second),
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, running),
    After = erlang:system_time(second),
    ?assertEqual(running, Updated#job.state),
    ?assert(is_integer(Updated#job.start_time)),
    ?assert(Updated#job.start_time >= Before),
    ?assert(Updated#job.start_time =< After),
    ?assertEqual(undefined, Updated#job.end_time).

update_state_completed() ->
    Job = #job{id = 1, state = running, start_time = 12345, end_time = undefined},
    Before = erlang:system_time(second),
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, completed),
    After = erlang:system_time(second),
    ?assertEqual(completed, Updated#job.state),
    ?assert(is_integer(Updated#job.end_time)),
    ?assert(Updated#job.end_time >= Before),
    ?assert(Updated#job.end_time =< After).

update_state_failed() ->
    Job = #job{id = 1, state = running, start_time = 12345, end_time = undefined},
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, failed),
    ?assertEqual(failed, Updated#job.state),
    ?assert(is_integer(Updated#job.end_time)).

update_state_cancelled() ->
    Job = #job{id = 1, state = pending, start_time = undefined, end_time = undefined},
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, cancelled),
    ?assertEqual(cancelled, Updated#job.state),
    ?assert(is_integer(Updated#job.end_time)).

update_state_timeout() ->
    Job = #job{id = 1, state = running, start_time = 12345, end_time = undefined},
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, timeout),
    ?assertEqual(timeout, Updated#job.state),
    ?assert(is_integer(Updated#job.end_time)).

update_state_node_fail() ->
    Job = #job{id = 1, state = running, start_time = 12345, end_time = undefined},
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, node_fail),
    ?assertEqual(node_fail, Updated#job.state),
    ?assert(is_integer(Updated#job.end_time)).

update_state_pending() ->
    %% Non-terminal, non-running states should only update state field
    Job = #job{id = 1, state = held, start_time = undefined, end_time = undefined},
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, pending),
    ?assertEqual(pending, Updated#job.state),
    ?assertEqual(undefined, Updated#job.start_time),
    ?assertEqual(undefined, Updated#job.end_time).

update_state_configuring() ->
    Job = #job{id = 1, state = pending, start_time = undefined, end_time = undefined},
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, configuring),
    ?assertEqual(configuring, Updated#job.state),
    ?assertEqual(undefined, Updated#job.start_time),
    ?assertEqual(undefined, Updated#job.end_time).

update_state_held() ->
    Job = #job{id = 1, state = pending, start_time = undefined, end_time = undefined},
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, held),
    ?assertEqual(held, Updated#job.state),
    ?assertEqual(undefined, Updated#job.start_time),
    ?assertEqual(undefined, Updated#job.end_time).

update_state_completing() ->
    Job = #job{id = 1, state = running, start_time = 12345, end_time = undefined},
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, completing),
    ?assertEqual(completing, Updated#job.state),
    %% start_time should be preserved
    ?assertEqual(12345, Updated#job.start_time),
    ?assertEqual(undefined, Updated#job.end_time).

update_state_requeued() ->
    Job = #job{id = 1, state = running, start_time = 12345, end_time = undefined},
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, requeued),
    ?assertEqual(requeued, Updated#job.state),
    ?assertEqual(12345, Updated#job.start_time),
    ?assertEqual(undefined, Updated#job.end_time).

update_running_preserves_fields() ->
    Job = #job{
        id = 42,
        name = <<"myname">>,
        user = <<"myuser">>,
        partition = <<"mypart">>,
        state = pending,
        script = <<"echo hi">>,
        num_nodes = 2,
        num_cpus = 8,
        memory_mb = 4096,
        time_limit = 7200,
        priority = 500,
        submit_time = 10000,
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [<<"node1">>],
        exit_code = undefined
    },
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, running),
    ?assertEqual(42, Updated#job.id),
    ?assertEqual(<<"myname">>, Updated#job.name),
    ?assertEqual(<<"myuser">>, Updated#job.user),
    ?assertEqual(<<"mypart">>, Updated#job.partition),
    ?assertEqual(running, Updated#job.state),
    ?assertEqual(<<"echo hi">>, Updated#job.script),
    ?assertEqual(2, Updated#job.num_nodes),
    ?assertEqual(8, Updated#job.num_cpus),
    ?assertEqual(4096, Updated#job.memory_mb),
    ?assertEqual(7200, Updated#job.time_limit),
    ?assertEqual(500, Updated#job.priority),
    ?assertEqual(10000, Updated#job.submit_time),
    ?assert(is_integer(Updated#job.start_time)),  % newly set
    ?assertEqual(undefined, Updated#job.end_time),
    ?assertEqual([<<"node1">>], Updated#job.allocated_nodes),
    ?assertEqual(undefined, Updated#job.exit_code).

update_terminal_preserves_fields() ->
    Job = #job{
        id = 42,
        name = <<"myname">>,
        user = <<"myuser">>,
        partition = <<"mypart">>,
        state = running,
        script = <<"echo hi">>,
        num_nodes = 2,
        num_cpus = 8,
        memory_mb = 4096,
        time_limit = 7200,
        priority = 500,
        submit_time = 10000,
        start_time = 11000,
        end_time = undefined,
        allocated_nodes = [<<"node1">>, <<"node2">>],
        exit_code = undefined
    },
    Updated = flurm_controller_ra_machine:update_job_state_internal(Job, completed),
    ?assertEqual(42, Updated#job.id),
    ?assertEqual(<<"myname">>, Updated#job.name),
    ?assertEqual(running, Job#job.state),  % original
    ?assertEqual(completed, Updated#job.state),  % updated
    ?assertEqual(10000, Updated#job.submit_time),
    ?assertEqual(11000, Updated#job.start_time),  % preserved
    ?assert(is_integer(Updated#job.end_time)),  % newly set
    ?assertEqual([<<"node1">>, <<"node2">>], Updated#job.allocated_nodes).

%%====================================================================
%% apply_job_updates/2 Tests
%%====================================================================

apply_job_updates_test_() ->
    [
     {"empty updates returns unchanged job", fun apply_updates_empty/0},
     {"update state field", fun apply_updates_state/0},
     {"update allocated_nodes field", fun apply_updates_allocated_nodes/0},
     {"update start_time field", fun apply_updates_start_time/0},
     {"update end_time field", fun apply_updates_end_time/0},
     {"update exit_code field", fun apply_updates_exit_code/0},
     {"update priority field", fun apply_updates_priority/0},
     {"update multiple fields", fun apply_updates_multiple/0},
     {"unknown fields are ignored", fun apply_updates_unknown_ignored/0},
     {"all known fields at once", fun apply_updates_all_known/0}
    ].

apply_updates_empty() ->
    Job = #job{id = 1, state = pending, priority = 100},
    Updated = flurm_controller_ra_machine:apply_job_updates(Job, #{}),
    ?assertEqual(Job, Updated).

apply_updates_state() ->
    Job = #job{id = 1, state = pending, priority = 100, allocated_nodes = []},
    Updated = flurm_controller_ra_machine:apply_job_updates(Job, #{state => running}),
    ?assertEqual(running, Updated#job.state),
    ?assertEqual(100, Updated#job.priority).  % unchanged

apply_updates_allocated_nodes() ->
    Job = #job{id = 1, state = pending, allocated_nodes = []},
    Nodes = [<<"node1">>, <<"node2">>, <<"node3">>],
    Updated = flurm_controller_ra_machine:apply_job_updates(Job, #{allocated_nodes => Nodes}),
    ?assertEqual(Nodes, Updated#job.allocated_nodes).

apply_updates_start_time() ->
    Job = #job{id = 1, state = pending, start_time = undefined},
    Updated = flurm_controller_ra_machine:apply_job_updates(Job, #{start_time => 12345}),
    ?assertEqual(12345, Updated#job.start_time).

apply_updates_end_time() ->
    Job = #job{id = 1, state = running, end_time = undefined},
    Updated = flurm_controller_ra_machine:apply_job_updates(Job, #{end_time => 99999}),
    ?assertEqual(99999, Updated#job.end_time).

apply_updates_exit_code() ->
    Job = #job{id = 1, state = completed, exit_code = undefined},
    Updated = flurm_controller_ra_machine:apply_job_updates(Job, #{exit_code => 0}),
    ?assertEqual(0, Updated#job.exit_code).

apply_updates_priority() ->
    Job = #job{id = 1, state = pending, priority = 100},
    Updated = flurm_controller_ra_machine:apply_job_updates(Job, #{priority => 9999}),
    ?assertEqual(9999, Updated#job.priority).

apply_updates_multiple() ->
    Job = #job{
        id = 1,
        state = pending,
        priority = 100,
        allocated_nodes = [],
        exit_code = undefined
    },
    Updates = #{
        state => running,
        priority => 500,
        allocated_nodes => [<<"n1">>],
        exit_code => 0
    },
    Updated = flurm_controller_ra_machine:apply_job_updates(Job, Updates),
    ?assertEqual(running, Updated#job.state),
    ?assertEqual(500, Updated#job.priority),
    ?assertEqual([<<"n1">>], Updated#job.allocated_nodes),
    ?assertEqual(0, Updated#job.exit_code).

apply_updates_unknown_ignored() ->
    Job = #job{id = 1, state = pending, priority = 100},
    Updates = #{
        unknown_field1 => value1,
        another_unknown => value2,
        state => running
    },
    Updated = flurm_controller_ra_machine:apply_job_updates(Job, Updates),
    ?assertEqual(running, Updated#job.state),
    ?assertEqual(100, Updated#job.priority).

apply_updates_all_known() ->
    Job = #job{
        id = 1,
        state = pending,
        priority = 100,
        allocated_nodes = [],
        start_time = undefined,
        end_time = undefined,
        exit_code = undefined
    },
    Updates = #{
        state => completed,
        priority => 1000,
        allocated_nodes => [<<"a">>, <<"b">>],
        start_time => 1000,
        end_time => 2000,
        exit_code => 42
    },
    Updated = flurm_controller_ra_machine:apply_job_updates(Job, Updates),
    ?assertEqual(completed, Updated#job.state),
    ?assertEqual(1000, Updated#job.priority),
    ?assertEqual([<<"a">>, <<"b">>], Updated#job.allocated_nodes),
    ?assertEqual(1000, Updated#job.start_time),
    ?assertEqual(2000, Updated#job.end_time),
    ?assertEqual(42, Updated#job.exit_code).

%%====================================================================
%% apply_heartbeat/2 Tests
%%====================================================================

apply_heartbeat_test_() ->
    [
     {"updates load_avg", fun heartbeat_load_avg/0},
     {"updates free_memory_mb", fun heartbeat_free_memory/0},
     {"updates running_jobs", fun heartbeat_running_jobs/0},
     {"always updates last_heartbeat", fun heartbeat_last_heartbeat/0},
     {"empty heartbeat preserves values but updates timestamp", fun heartbeat_empty/0},
     {"partial heartbeat uses defaults for missing", fun heartbeat_partial/0},
     {"full heartbeat updates all", fun heartbeat_full/0}
    ].

heartbeat_load_avg() ->
    Node = #node{hostname = <<"n1">>, load_avg = 0.0, free_memory_mb = 8192,
                 running_jobs = [], last_heartbeat = 0},
    Updated = flurm_controller_ra_machine:apply_heartbeat(Node, #{load_avg => 5.5}),
    ?assertEqual(5.5, Updated#node.load_avg),
    ?assertEqual(8192, Updated#node.free_memory_mb),
    ?assertEqual([], Updated#node.running_jobs).

heartbeat_free_memory() ->
    Node = #node{hostname = <<"n1">>, load_avg = 1.0, free_memory_mb = 8192,
                 running_jobs = [], last_heartbeat = 0},
    Updated = flurm_controller_ra_machine:apply_heartbeat(Node, #{free_memory_mb => 2048}),
    ?assertEqual(1.0, Updated#node.load_avg),
    ?assertEqual(2048, Updated#node.free_memory_mb).

heartbeat_running_jobs() ->
    Node = #node{hostname = <<"n1">>, load_avg = 1.0, free_memory_mb = 8192,
                 running_jobs = [], last_heartbeat = 0},
    Jobs = [1, 2, 3, 4, 5],
    Updated = flurm_controller_ra_machine:apply_heartbeat(Node, #{running_jobs => Jobs}),
    ?assertEqual(Jobs, Updated#node.running_jobs).

heartbeat_last_heartbeat() ->
    Node = #node{hostname = <<"n1">>, load_avg = 1.0, free_memory_mb = 8192,
                 running_jobs = [], last_heartbeat = 0},
    Before = erlang:system_time(second),
    Updated = flurm_controller_ra_machine:apply_heartbeat(Node, #{}),
    After = erlang:system_time(second),
    ?assert(Updated#node.last_heartbeat >= Before),
    ?assert(Updated#node.last_heartbeat =< After).

heartbeat_empty() ->
    Node = #node{hostname = <<"n1">>, load_avg = 3.14, free_memory_mb = 4096,
                 running_jobs = [99], last_heartbeat = 12345},
    Updated = flurm_controller_ra_machine:apply_heartbeat(Node, #{}),
    ?assertEqual(3.14, Updated#node.load_avg),
    ?assertEqual(4096, Updated#node.free_memory_mb),
    ?assertEqual([99], Updated#node.running_jobs),
    ?assert(Updated#node.last_heartbeat > 12345).

heartbeat_partial() ->
    Node = #node{hostname = <<"n1">>, load_avg = 2.0, free_memory_mb = 16384,
                 running_jobs = [1, 2], last_heartbeat = 0},
    %% Only update load_avg, rest should use node's existing values
    Updated = flurm_controller_ra_machine:apply_heartbeat(Node, #{load_avg => 4.0}),
    ?assertEqual(4.0, Updated#node.load_avg),
    ?assertEqual(16384, Updated#node.free_memory_mb),
    ?assertEqual([1, 2], Updated#node.running_jobs).

heartbeat_full() ->
    Node = #node{hostname = <<"n1">>, load_avg = 0.0, free_memory_mb = 0,
                 running_jobs = [], last_heartbeat = 0},
    HeartbeatData = #{
        load_avg => 7.5,
        free_memory_mb => 32768,
        running_jobs => [100, 200, 300]
    },
    Before = erlang:system_time(second),
    Updated = flurm_controller_ra_machine:apply_heartbeat(Node, HeartbeatData),
    After = erlang:system_time(second),
    ?assertEqual(7.5, Updated#node.load_avg),
    ?assertEqual(32768, Updated#node.free_memory_mb),
    ?assertEqual([100, 200, 300], Updated#node.running_jobs),
    ?assert(Updated#node.last_heartbeat >= Before),
    ?assert(Updated#node.last_heartbeat =< After).

%%====================================================================
%% apply_partition_updates/2 Tests
%%====================================================================

apply_partition_updates_test_() ->
    [
     {"empty updates returns unchanged partition", fun partition_updates_empty/0},
     {"update state field", fun partition_updates_state/0},
     {"update nodes field", fun partition_updates_nodes/0},
     {"update max_time field", fun partition_updates_max_time/0},
     {"update default_time field", fun partition_updates_default_time/0},
     {"update max_nodes field", fun partition_updates_max_nodes/0},
     {"update priority field", fun partition_updates_priority/0},
     {"update multiple fields", fun partition_updates_multiple/0},
     {"unknown fields are ignored", fun partition_updates_unknown_ignored/0},
     {"all known fields at once", fun partition_updates_all_known/0}
    ].

partition_updates_empty() ->
    Part = #partition{name = <<"test">>, state = up, nodes = [], max_time = 3600,
                      default_time = 1800, max_nodes = 10, priority = 1},
    Updated = flurm_controller_ra_machine:apply_partition_updates(Part, #{}),
    ?assertEqual(Part, Updated).

partition_updates_state() ->
    Part = #partition{name = <<"test">>, state = up, nodes = [], max_time = 3600,
                      default_time = 1800, max_nodes = 10, priority = 1},
    Updated = flurm_controller_ra_machine:apply_partition_updates(Part, #{state => down}),
    ?assertEqual(down, Updated#partition.state),
    ?assertEqual([], Updated#partition.nodes).

partition_updates_nodes() ->
    Part = #partition{name = <<"test">>, state = up, nodes = [], max_time = 3600,
                      default_time = 1800, max_nodes = 10, priority = 1},
    Nodes = [<<"node1">>, <<"node2">>, <<"node3">>],
    Updated = flurm_controller_ra_machine:apply_partition_updates(Part, #{nodes => Nodes}),
    ?assertEqual(Nodes, Updated#partition.nodes).

partition_updates_max_time() ->
    Part = #partition{name = <<"test">>, state = up, nodes = [], max_time = 3600,
                      default_time = 1800, max_nodes = 10, priority = 1},
    Updated = flurm_controller_ra_machine:apply_partition_updates(Part, #{max_time => 86400}),
    ?assertEqual(86400, Updated#partition.max_time).

partition_updates_default_time() ->
    Part = #partition{name = <<"test">>, state = up, nodes = [], max_time = 3600,
                      default_time = 1800, max_nodes = 10, priority = 1},
    Updated = flurm_controller_ra_machine:apply_partition_updates(Part, #{default_time => 7200}),
    ?assertEqual(7200, Updated#partition.default_time).

partition_updates_max_nodes() ->
    Part = #partition{name = <<"test">>, state = up, nodes = [], max_time = 3600,
                      default_time = 1800, max_nodes = 10, priority = 1},
    Updated = flurm_controller_ra_machine:apply_partition_updates(Part, #{max_nodes => 100}),
    ?assertEqual(100, Updated#partition.max_nodes).

partition_updates_priority() ->
    Part = #partition{name = <<"test">>, state = up, nodes = [], max_time = 3600,
                      default_time = 1800, max_nodes = 10, priority = 1},
    Updated = flurm_controller_ra_machine:apply_partition_updates(Part, #{priority => 99}),
    ?assertEqual(99, Updated#partition.priority).

partition_updates_multiple() ->
    Part = #partition{name = <<"test">>, state = up, nodes = [], max_time = 3600,
                      default_time = 1800, max_nodes = 10, priority = 1},
    Updates = #{
        state => drain,
        nodes => [<<"n1">>],
        priority => 50
    },
    Updated = flurm_controller_ra_machine:apply_partition_updates(Part, Updates),
    ?assertEqual(drain, Updated#partition.state),
    ?assertEqual([<<"n1">>], Updated#partition.nodes),
    ?assertEqual(50, Updated#partition.priority),
    %% Unchanged fields
    ?assertEqual(3600, Updated#partition.max_time),
    ?assertEqual(1800, Updated#partition.default_time),
    ?assertEqual(10, Updated#partition.max_nodes).

partition_updates_unknown_ignored() ->
    Part = #partition{name = <<"test">>, state = up, nodes = [], max_time = 3600,
                      default_time = 1800, max_nodes = 10, priority = 1},
    Updates = #{
        unknown_field => ignored_value,
        another_unknown => also_ignored,
        state => down
    },
    Updated = flurm_controller_ra_machine:apply_partition_updates(Part, Updates),
    ?assertEqual(down, Updated#partition.state),
    ?assertEqual([], Updated#partition.nodes),
    ?assertEqual(3600, Updated#partition.max_time).

partition_updates_all_known() ->
    Part = #partition{name = <<"test">>, state = up, nodes = [], max_time = 3600,
                      default_time = 1800, max_nodes = 10, priority = 1},
    Updates = #{
        state => inactive,
        nodes => [<<"a">>, <<"b">>, <<"c">>],
        max_time => 172800,
        default_time => 86400,
        max_nodes => 500,
        priority => 1000
    },
    Updated = flurm_controller_ra_machine:apply_partition_updates(Part, Updates),
    ?assertEqual(inactive, Updated#partition.state),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], Updated#partition.nodes),
    ?assertEqual(172800, Updated#partition.max_time),
    ?assertEqual(86400, Updated#partition.default_time),
    ?assertEqual(500, Updated#partition.max_nodes),
    ?assertEqual(1000, Updated#partition.priority).

%%====================================================================
%% Ra Machine Callback Direct Tests (no meck)
%%====================================================================

ra_machine_callbacks_test_() ->
    [
     {"init/1 creates state with record structure", fun test_init_record_structure/0},
     {"overview/1 returns correct map", fun test_overview_structure/0},
     {"state_enter/2 all states return empty effects", fun test_state_enter_all/0},
     {"snapshot_installed/2 returns ok", fun test_snapshot_installed/0}
    ].

test_init_record_structure() ->
    State = flurm_controller_ra_machine:init(#{}),
    %% State is a #state record (tuple starting with 'state')
    ?assertEqual(state, element(1, State)),
    %% Verify overview works on init state
    Overview = flurm_controller_ra_machine:overview(State),
    ?assertEqual(0, maps:get(job_count, Overview)),
    ?assertEqual(0, maps:get(node_count, Overview)),
    ?assertEqual(0, maps:get(partition_count, Overview)),
    ?assertEqual(1, maps:get(next_job_id, Overview)),
    ?assertEqual(0, maps:get(last_applied_index, Overview)).

test_overview_structure() ->
    State = flurm_controller_ra_machine:init(#{}),
    Overview = flurm_controller_ra_machine:overview(State),
    ?assert(is_map(Overview)),
    RequiredKeys = [job_count, node_count, partition_count, next_job_id, last_applied_index],
    lists:foreach(fun(Key) ->
        ?assert(maps:is_key(Key, Overview), {missing_key, Key})
    end, RequiredKeys).

test_state_enter_all() ->
    State = flurm_controller_ra_machine:init(#{}),
    %% All known ra states
    States = [leader, follower, candidate, recover, await_condition,
              receive_snapshot, unknown_state, some_future_state],
    lists:foreach(fun(RaState) ->
        Effects = flurm_controller_ra_machine:state_enter(RaState, State),
        ?assertEqual([], Effects, {state, RaState})
    end, States).

test_snapshot_installed() ->
    State = flurm_controller_ra_machine:init(#{}),
    Meta = #{index => 100, term => 5},
    Result = flurm_controller_ra_machine:snapshot_installed(Meta, State),
    ?assertEqual(ok, Result).

%%====================================================================
%% Apply Command Tests - Comprehensive Coverage
%%====================================================================

apply_commands_test_() ->
    [
     {"submit_job creates job and increments id", fun test_apply_submit_job/0},
     {"submit_job with minimal spec", fun test_apply_submit_job_minimal/0},
     {"cancel_job updates state to cancelled", fun test_apply_cancel_job/0},
     {"cancel_job nonexistent returns error", fun test_apply_cancel_job_not_found/0},
     {"update_job_state transitions state", fun test_apply_update_job_state/0},
     {"update_job modifies job fields", fun test_apply_update_job/0},
     {"register_node adds to nodes map", fun test_apply_register_node/0},
     {"unregister_node removes from nodes map", fun test_apply_unregister_node/0},
     {"update_node_state changes node state", fun test_apply_update_node_state/0},
     {"node_heartbeat updates node data", fun test_apply_node_heartbeat/0},
     {"create_partition adds partition", fun test_apply_create_partition/0},
     {"create_partition duplicate returns error", fun test_apply_create_partition_dup/0},
     {"update_partition modifies partition", fun test_apply_update_partition/0},
     {"delete_partition removes partition", fun test_apply_delete_partition/0},
     {"unknown command returns error", fun test_apply_unknown_command/0}
    ].

test_apply_submit_job() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Meta = #{index => 1},
    JobSpec = #{name => <<"test">>, script => <<"echo">>, user => <<"testuser">>},
    {State1, Reply} = flurm_controller_ra_machine:apply(Meta, {submit_job, JobSpec}, State0),
    ?assertMatch({ok, 1}, Reply),
    Overview = flurm_controller_ra_machine:overview(State1),
    ?assertEqual(1, maps:get(job_count, Overview)),
    ?assertEqual(2, maps:get(next_job_id, Overview)),
    ?assertEqual(1, maps:get(last_applied_index, Overview)).

test_apply_submit_job_minimal() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    %% Empty job spec should use all defaults
    {State1, {ok, 1}} = flurm_controller_ra_machine:apply(#{index => 1}, {submit_job, #{}}, State0),
    {State2, {ok, 2}} = flurm_controller_ra_machine:apply(#{index => 2}, {submit_job, #{}}, State1),
    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(2, maps:get(job_count, Overview)),
    ?assertEqual(3, maps:get(next_job_id, Overview)).

test_apply_cancel_job() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, 1}} = flurm_controller_ra_machine:apply(#{index => 1}, {submit_job, #{}}, State0),
    {State2, ok} = flurm_controller_ra_machine:apply(#{index => 2}, {cancel_job, 1}, State1),
    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(1, maps:get(job_count, Overview)),  % Job still exists, just cancelled
    ?assertEqual(2, maps:get(last_applied_index, Overview)).

test_apply_cancel_job_not_found() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, Result} = flurm_controller_ra_machine:apply(#{index => 1}, {cancel_job, 999}, State0),
    ?assertEqual({error, not_found}, Result),
    ?assertEqual(1, maps:get(last_applied_index, flurm_controller_ra_machine:overview(State1))).

test_apply_update_job_state() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, 1}} = flurm_controller_ra_machine:apply(#{index => 1}, {submit_job, #{}}, State0),
    {State2, ok} = flurm_controller_ra_machine:apply(#{index => 2}, {update_job_state, 1, running}, State1),
    {_State3, ok} = flurm_controller_ra_machine:apply(#{index => 3}, {update_job_state, 1, completed}, State2).

test_apply_update_job() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, {ok, 1}} = flurm_controller_ra_machine:apply(#{index => 1}, {submit_job, #{}}, State0),
    Updates = #{priority => 500, allocated_nodes => [<<"n1">>]},
    {_State2, ok} = flurm_controller_ra_machine:apply(#{index => 2}, {update_job, 1, Updates}, State1).

test_apply_register_node() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = idle,
                 features = [], partitions = [], running_jobs = [], load_avg = 0.0, free_memory_mb = 16384},
    {State1, ok} = flurm_controller_ra_machine:apply(#{index => 1}, {register_node, Node}, State0),
    Overview = flurm_controller_ra_machine:overview(State1),
    ?assertEqual(1, maps:get(node_count, Overview)).

test_apply_unregister_node() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = idle,
                 features = [], partitions = [], running_jobs = [], load_avg = 0.0, free_memory_mb = 16384},
    {State1, ok} = flurm_controller_ra_machine:apply(#{index => 1}, {register_node, Node}, State0),
    {State2, ok} = flurm_controller_ra_machine:apply(#{index => 2}, {unregister_node, <<"node1">>}, State1),
    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(0, maps:get(node_count, Overview)).

test_apply_update_node_state() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = idle,
                 features = [], partitions = [], running_jobs = [], load_avg = 0.0, free_memory_mb = 16384},
    {State1, ok} = flurm_controller_ra_machine:apply(#{index => 1}, {register_node, Node}, State0),
    {_State2, ok} = flurm_controller_ra_machine:apply(#{index => 2}, {update_node_state, <<"node1">>, allocated}, State1).

test_apply_node_heartbeat() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Node = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = idle,
                 features = [], partitions = [], running_jobs = [], load_avg = 0.0, free_memory_mb = 16384},
    {State1, ok} = flurm_controller_ra_machine:apply(#{index => 1}, {register_node, Node}, State0),
    HB = #{load_avg => 2.5, free_memory_mb => 8192, running_jobs => [1, 2]},
    {_State2, ok} = flurm_controller_ra_machine:apply(#{index => 2}, {node_heartbeat, <<"node1">>, HB}, State1).

test_apply_create_partition() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Part = #partition{name = <<"batch">>, state = up, nodes = [], max_time = 86400,
                      default_time = 3600, max_nodes = 100, priority = 1, allow_root = false},
    {State1, ok} = flurm_controller_ra_machine:apply(#{index => 1}, {create_partition, Part}, State0),
    Overview = flurm_controller_ra_machine:overview(State1),
    ?assertEqual(1, maps:get(partition_count, Overview)).

test_apply_create_partition_dup() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Part = #partition{name = <<"batch">>, state = up, nodes = [], max_time = 86400,
                      default_time = 3600, max_nodes = 100, priority = 1, allow_root = false},
    {State1, ok} = flurm_controller_ra_machine:apply(#{index => 1}, {create_partition, Part}, State0),
    {_State2, Result} = flurm_controller_ra_machine:apply(#{index => 2}, {create_partition, Part}, State1),
    ?assertEqual({error, already_exists}, Result).

test_apply_update_partition() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Part = #partition{name = <<"batch">>, state = up, nodes = [], max_time = 86400,
                      default_time = 3600, max_nodes = 100, priority = 1, allow_root = false},
    {State1, ok} = flurm_controller_ra_machine:apply(#{index => 1}, {create_partition, Part}, State0),
    {_State2, ok} = flurm_controller_ra_machine:apply(#{index => 2}, {update_partition, <<"batch">>, #{state => down}}, State1).

test_apply_delete_partition() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    Part = #partition{name = <<"batch">>, state = up, nodes = [], max_time = 86400,
                      default_time = 3600, max_nodes = 100, priority = 1, allow_root = false},
    {State1, ok} = flurm_controller_ra_machine:apply(#{index => 1}, {create_partition, Part}, State0),
    {State2, ok} = flurm_controller_ra_machine:apply(#{index => 2}, {delete_partition, <<"batch">>}, State1),
    Overview = flurm_controller_ra_machine:overview(State2),
    ?assertEqual(0, maps:get(partition_count, Overview)).

test_apply_unknown_command() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, Result} = flurm_controller_ra_machine:apply(#{index => 1}, {totally_unknown_cmd, arg1, arg2}, State0),
    ?assertEqual({error, unknown_command}, Result),
    ?assertEqual(1, maps:get(last_applied_index, flurm_controller_ra_machine:overview(State1))).

%%====================================================================
%% Error Branch Coverage Tests - Not Found Cases
%%====================================================================

error_not_found_test_() ->
    [
     {"update_job_state for non-existent job", fun test_update_job_state_not_found/0},
     {"update_job for non-existent job", fun test_update_job_not_found/0},
     {"update_node_state for non-existent node", fun test_update_node_state_not_found/0},
     {"node_heartbeat for non-existent node", fun test_node_heartbeat_not_found/0},
     {"update_partition for non-existent partition", fun test_update_partition_not_found/0}
    ].

test_update_job_state_not_found() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {update_job_state, 9999, running},
        State0
    ),
    ?assertEqual({error, not_found}, Result),
    ?assertEqual(1, maps:get(last_applied_index, flurm_controller_ra_machine:overview(State1))).

test_update_job_not_found() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {update_job, 9999, #{priority => 500}},
        State0
    ),
    ?assertEqual({error, not_found}, Result),
    ?assertEqual(1, maps:get(last_applied_index, flurm_controller_ra_machine:overview(State1))).

test_update_node_state_not_found() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {update_node_state, <<"nonexistent-node">>, down},
        State0
    ),
    ?assertEqual({error, not_found}, Result),
    ?assertEqual(1, maps:get(last_applied_index, flurm_controller_ra_machine:overview(State1))).

test_node_heartbeat_not_found() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {node_heartbeat, <<"nonexistent-node">>, #{load_avg => 1.0}},
        State0
    ),
    ?assertEqual({error, not_found}, Result),
    ?assertEqual(1, maps:get(last_applied_index, flurm_controller_ra_machine:overview(State1))).

test_update_partition_not_found() ->
    State0 = flurm_controller_ra_machine:init(#{}),
    {State1, Result} = flurm_controller_ra_machine:apply(
        #{index => 1},
        {update_partition, <<"nonexistent-partition">>, #{state => down}},
        State0
    ),
    ?assertEqual({error, not_found}, Result),
    ?assertEqual(1, maps:get(last_applied_index, flurm_controller_ra_machine:overview(State1))).
