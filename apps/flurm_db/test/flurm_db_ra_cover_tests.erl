%%%-------------------------------------------------------------------
%%% @doc Pure Ra State Machine Coverage Tests
%%%
%%% Tests the flurm_db_ra:apply/3 state machine logic directly,
%%% without requiring a running Ra cluster. Also tests helper
%%% functions: init/1, make_job_record/2, make_node_record/1,
%%% make_partition_record/1, update_job_state_record/2.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Helpers
%%====================================================================

meta() ->
    #{index => 1, term => 1, system_time => erlang:system_time(millisecond)}.

fresh_state() ->
    flurm_db_ra:init(#{}).

job_spec() ->
    job_spec(<<"test_job">>).

job_spec(Name) ->
    #ra_job_spec{
        name = Name,
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"batch">>,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        num_tasks = 1,
        cpus_per_task = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = undefined
    }.

node_spec() ->
    node_spec(<<"node01">>).

node_spec(Name) ->
    #ra_node_spec{
        name = Name,
        hostname = <<"node01.cluster.local">>,
        port = 6818,
        cpus = 16,
        memory_mb = 65536,
        gpus = 2,
        features = [gpu, avx2],
        partitions = [<<"batch">>, <<"gpu">>]
    }.

partition_spec() ->
    partition_spec(<<"batch">>).

partition_spec(Name) ->
    #ra_partition_spec{
        name = Name,
        nodes = [<<"node01">>, <<"node02">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 100
    }.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_returns_empty_state_test() ->
    State = flurm_db_ra:init(#{}),
    ?assertMatch(#ra_state{
        jobs = Jobs,
        nodes = Nodes,
        partitions = Parts,
        job_counter = 1,
        version = 1
    } when Jobs =:= #{} andalso Nodes =:= #{} andalso Parts =:= #{}, State).

init_ignores_config_test() ->
    State = flurm_db_ra:init(#{some_key => some_value}),
    ?assertEqual(1, State#ra_state.job_counter).

%%====================================================================
%% make_job_record/2 Tests
%%====================================================================

make_job_record_sets_defaults_test() ->
    Spec = job_spec(),
    Job = flurm_db_ra:make_job_record(42, Spec),
    ?assertEqual(42, Job#ra_job.id),
    ?assertEqual(<<"test_job">>, Job#ra_job.name),
    ?assertEqual(<<"testuser">>, Job#ra_job.user),
    ?assertEqual(<<"testgroup">>, Job#ra_job.group),
    ?assertEqual(<<"batch">>, Job#ra_job.partition),
    ?assertEqual(pending, Job#ra_job.state),
    ?assertEqual(4, Job#ra_job.num_cpus),
    ?assertEqual(1024, Job#ra_job.memory_mb),
    ?assertEqual(3600, Job#ra_job.time_limit),
    %% priority=undefined in spec should default to ?DEFAULT_PRIORITY (100)
    ?assertEqual(?DEFAULT_PRIORITY, Job#ra_job.priority),
    ?assertEqual(undefined, Job#ra_job.start_time),
    ?assertEqual(undefined, Job#ra_job.end_time),
    ?assertEqual([], Job#ra_job.allocated_nodes),
    ?assertEqual(undefined, Job#ra_job.exit_code),
    ?assert(is_integer(Job#ra_job.submit_time)).

make_job_record_explicit_priority_test() ->
    Spec = job_spec(),
    SpecWithPriority = Spec#ra_job_spec{priority = 500},
    Job = flurm_db_ra:make_job_record(1, SpecWithPriority),
    ?assertEqual(500, Job#ra_job.priority).

%%====================================================================
%% make_node_record/1 Tests
%%====================================================================

make_node_record_test() ->
    Spec = node_spec(),
    Node = flurm_db_ra:make_node_record(Spec),
    ?assertEqual(<<"node01">>, Node#ra_node.name),
    ?assertEqual(<<"node01.cluster.local">>, Node#ra_node.hostname),
    ?assertEqual(6818, Node#ra_node.port),
    ?assertEqual(16, Node#ra_node.cpus),
    ?assertEqual(0, Node#ra_node.cpus_used),
    ?assertEqual(65536, Node#ra_node.memory_mb),
    ?assertEqual(0, Node#ra_node.memory_used),
    ?assertEqual(2, Node#ra_node.gpus),
    ?assertEqual(0, Node#ra_node.gpus_used),
    ?assertEqual(up, Node#ra_node.state),
    ?assertEqual([gpu, avx2], Node#ra_node.features),
    ?assertEqual([<<"batch">>, <<"gpu">>], Node#ra_node.partitions),
    ?assertEqual([], Node#ra_node.running_jobs),
    ?assert(is_integer(Node#ra_node.last_heartbeat)).

%%====================================================================
%% make_partition_record/1 Tests
%%====================================================================

make_partition_record_test() ->
    Spec = partition_spec(),
    Part = flurm_db_ra:make_partition_record(Spec),
    ?assertEqual(<<"batch">>, Part#ra_partition.name),
    ?assertEqual(up, Part#ra_partition.state),
    ?assertEqual([<<"node01">>, <<"node02">>], Part#ra_partition.nodes),
    ?assertEqual(86400, Part#ra_partition.max_time),
    ?assertEqual(3600, Part#ra_partition.default_time),
    ?assertEqual(10, Part#ra_partition.max_nodes),
    ?assertEqual(100, Part#ra_partition.priority).

%%====================================================================
%% update_job_state_record/2 Tests
%%====================================================================

update_job_state_running_sets_start_time_test() ->
    Spec = job_spec(),
    Job = flurm_db_ra:make_job_record(1, Spec),
    %% start_time is undefined, so running should set it
    Updated = flurm_db_ra:update_job_state_record(Job, running),
    ?assertEqual(running, Updated#ra_job.state),
    ?assertNotEqual(undefined, Updated#ra_job.start_time).

update_job_state_running_preserves_start_time_test() ->
    Spec = job_spec(),
    Job0 = flurm_db_ra:make_job_record(1, Spec),
    Job = Job0#ra_job{start_time = 1000},
    Updated = flurm_db_ra:update_job_state_record(Job, running),
    ?assertEqual(running, Updated#ra_job.state),
    %% start_time was already set, should keep existing value
    ?assertEqual(1000, Updated#ra_job.start_time).

update_job_state_completed_sets_end_time_test() ->
    Spec = job_spec(),
    Job = flurm_db_ra:make_job_record(1, Spec),
    Updated = flurm_db_ra:update_job_state_record(Job, completed),
    ?assertEqual(completed, Updated#ra_job.state),
    ?assertNotEqual(undefined, Updated#ra_job.end_time).

update_job_state_failed_sets_end_time_test() ->
    Spec = job_spec(),
    Job = flurm_db_ra:make_job_record(1, Spec),
    Updated = flurm_db_ra:update_job_state_record(Job, failed),
    ?assertEqual(failed, Updated#ra_job.state),
    ?assertNotEqual(undefined, Updated#ra_job.end_time).

update_job_state_cancelled_sets_end_time_test() ->
    Spec = job_spec(),
    Job = flurm_db_ra:make_job_record(1, Spec),
    Updated = flurm_db_ra:update_job_state_record(Job, cancelled),
    ?assertEqual(cancelled, Updated#ra_job.state),
    ?assertNotEqual(undefined, Updated#ra_job.end_time).

update_job_state_timeout_sets_end_time_test() ->
    Job = flurm_db_ra:make_job_record(1, job_spec()),
    Updated = flurm_db_ra:update_job_state_record(Job, timeout),
    ?assertEqual(timeout, Updated#ra_job.state),
    ?assertNotEqual(undefined, Updated#ra_job.end_time).

update_job_state_node_fail_sets_end_time_test() ->
    Job = flurm_db_ra:make_job_record(1, job_spec()),
    Updated = flurm_db_ra:update_job_state_record(Job, node_fail),
    ?assertEqual(node_fail, Updated#ra_job.state),
    ?assertNotEqual(undefined, Updated#ra_job.end_time).

update_job_state_configuring_no_end_time_test() ->
    Job = flurm_db_ra:make_job_record(1, job_spec()),
    Updated = flurm_db_ra:update_job_state_record(Job, configuring),
    ?assertEqual(configuring, Updated#ra_job.state),
    ?assertEqual(undefined, Updated#ra_job.end_time).

%%====================================================================
%% submit_job Tests
%%====================================================================

submit_job_assigns_id_test() ->
    S0 = fresh_state(),
    {S1, {ok, JobId}, Effects} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    ?assertEqual(1, JobId),
    ?assertEqual(2, S1#ra_state.job_counter),
    ?assert(maps:is_key(1, S1#ra_state.jobs)),
    Job = maps:get(1, S1#ra_state.jobs),
    ?assertEqual(pending, Job#ra_job.state),
    ?assertEqual(<<"test_job">>, Job#ra_job.name),
    %% Should produce a job_submitted effect
    ?assertMatch([{mod_call, flurm_db_ra_effects, job_submitted, [_]}], Effects).

submit_multiple_jobs_increments_counter_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec(<<"job1">>)}, S0),
    {S2, {ok, 2}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec(<<"job2">>)}, S1),
    {S3, {ok, 3}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec(<<"job3">>)}, S2),
    ?assertEqual(4, S3#ra_state.job_counter),
    ?assertEqual(3, maps:size(S3#ra_state.jobs)).

%%====================================================================
%% store_job Tests
%%====================================================================

store_job_with_specific_id_test() ->
    S0 = fresh_state(),
    {S1, {ok, 100}, []} = flurm_db_ra:apply(meta(), {store_job, 100, job_spec()}, S0),
    ?assert(maps:is_key(100, S1#ra_state.jobs)),
    %% job_counter should advance past the stored ID
    ?assert(S1#ra_state.job_counter >= 101).

store_job_does_not_decrease_counter_test() ->
    S0 = fresh_state(),
    %% Submit a job so counter becomes 2
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    ?assertEqual(2, S1#ra_state.job_counter),
    %% Store a job with ID 1 (lower than counter) -- counter should not decrease
    {S2, {ok, 1}, []} = flurm_db_ra:apply(meta(), {store_job, 1, job_spec(<<"stored">>)}, S1),
    ?assert(S2#ra_state.job_counter >= 2).

%%====================================================================
%% cancel_job Tests
%%====================================================================

cancel_pending_job_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, Effects} = flurm_db_ra:apply(meta(), {cancel_job, 1}, S1),
    Job = maps:get(1, S2#ra_state.jobs),
    ?assertEqual(cancelled, Job#ra_job.state),
    ?assertNotEqual(undefined, Job#ra_job.end_time),
    ?assertMatch([{mod_call, flurm_db_ra_effects, job_cancelled, [_]}], Effects).

cancel_nonexistent_job_test() ->
    S0 = fresh_state(),
    {S0, {error, not_found}, []} = flurm_db_ra:apply(meta(), {cancel_job, 999}, S0).

cancel_completed_job_returns_already_terminal_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    %% Set exit code 0 -> completed
    {S2, ok, _} = flurm_db_ra:apply(meta(), {set_job_exit_code, 1, 0}, S1),
    Job = maps:get(1, S2#ra_state.jobs),
    ?assertEqual(completed, Job#ra_job.state),
    %% Now try to cancel
    {S2, {error, already_terminal}, []} = flurm_db_ra:apply(meta(), {cancel_job, 1}, S2).

cancel_failed_job_returns_already_terminal_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, _} = flurm_db_ra:apply(meta(), {set_job_exit_code, 1, 1}, S1),
    Job = maps:get(1, S2#ra_state.jobs),
    ?assertEqual(failed, Job#ra_job.state),
    {S2, {error, already_terminal}, []} = flurm_db_ra:apply(meta(), {cancel_job, 1}, S2).

cancel_cancelled_job_returns_already_terminal_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, _} = flurm_db_ra:apply(meta(), {cancel_job, 1}, S1),
    {S2, {error, already_terminal}, []} = flurm_db_ra:apply(meta(), {cancel_job, 1}, S2).

%%====================================================================
%% update_job_state Tests
%%====================================================================

update_job_state_pending_to_running_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, Effects} = flurm_db_ra:apply(meta(), {update_job_state, 1, running}, S1),
    Job = maps:get(1, S2#ra_state.jobs),
    ?assertEqual(running, Job#ra_job.state),
    ?assertMatch([{mod_call, flurm_db_ra_effects, job_state_changed, [1, pending, running]}], Effects).

update_job_state_not_found_test() ->
    S0 = fresh_state(),
    {S0, {error, not_found}, []} = flurm_db_ra:apply(meta(), {update_job_state, 999, running}, S0).

%%====================================================================
%% allocate_job Tests
%%====================================================================

allocate_pending_job_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    Nodes = [<<"node01">>, <<"node02">>],
    {S2, ok, Effects} = flurm_db_ra:apply(meta(), {allocate_job, 1, Nodes}, S1),
    Job = maps:get(1, S2#ra_state.jobs),
    ?assertEqual(configuring, Job#ra_job.state),
    ?assertEqual(Nodes, Job#ra_job.allocated_nodes),
    ?assertNotEqual(undefined, Job#ra_job.start_time),
    ?assertMatch([{mod_call, flurm_db_ra_effects, job_allocated, [_, _]}], Effects).

allocate_already_running_job_fails_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, _} = flurm_db_ra:apply(meta(), {update_job_state, 1, running}, S1),
    {S2, {error, {invalid_state, running}}, []} =
        flurm_db_ra:apply(meta(), {allocate_job, 1, [<<"n1">>]}, S2).

allocate_nonexistent_job_fails_test() ->
    S0 = fresh_state(),
    {S0, {error, not_found}, []} =
        flurm_db_ra:apply(meta(), {allocate_job, 42, [<<"n1">>]}, S0).

allocate_configuring_job_preserves_start_time_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    %% First allocation sets start_time
    {S2, ok, _} = flurm_db_ra:apply(meta(), {allocate_job, 1, [<<"n1">>]}, S1),
    Job1 = maps:get(1, S2#ra_state.jobs),
    StartTime1 = Job1#ra_job.start_time,
    ?assertNotEqual(undefined, StartTime1),
    %% Re-allocate while configuring - start_time should be preserved
    {S3, ok, _} = flurm_db_ra:apply(meta(), {allocate_job, 1, [<<"n2">>]}, S2),
    Job2 = maps:get(1, S3#ra_state.jobs),
    ?assertEqual(StartTime1, Job2#ra_job.start_time).

%%====================================================================
%% set_job_exit_code Tests
%%====================================================================

set_exit_code_zero_completes_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, Effects} = flurm_db_ra:apply(meta(), {set_job_exit_code, 1, 0}, S1),
    Job = maps:get(1, S2#ra_state.jobs),
    ?assertEqual(completed, Job#ra_job.state),
    ?assertEqual(0, Job#ra_job.exit_code),
    ?assertNotEqual(undefined, Job#ra_job.end_time),
    ?assertMatch([{mod_call, flurm_db_ra_effects, job_completed, [_, 0]}], Effects).

set_exit_code_nonzero_fails_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, _} = flurm_db_ra:apply(meta(), {set_job_exit_code, 1, 137}, S1),
    Job = maps:get(1, S2#ra_state.jobs),
    ?assertEqual(failed, Job#ra_job.state),
    ?assertEqual(137, Job#ra_job.exit_code).

set_exit_code_not_found_test() ->
    S0 = fresh_state(),
    {S0, {error, not_found}, []} =
        flurm_db_ra:apply(meta(), {set_job_exit_code, 999, 0}, S0).

%%====================================================================
%% update_job_fields Tests
%%====================================================================

update_job_fields_time_limit_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, []} = flurm_db_ra:apply(meta(), {update_job_fields, 1, #{time_limit => 7200}}, S1),
    Job = maps:get(1, S2#ra_state.jobs),
    ?assertEqual(7200, Job#ra_job.time_limit).

update_job_fields_name_and_priority_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    Fields = #{name => <<"new_name">>, priority => 999},
    {S2, ok, []} = flurm_db_ra:apply(meta(), {update_job_fields, 1, Fields}, S1),
    Job = maps:get(1, S2#ra_state.jobs),
    ?assertEqual(<<"new_name">>, Job#ra_job.name),
    ?assertEqual(999, Job#ra_job.priority).

update_job_fields_unknown_field_ignored_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, []} = flurm_db_ra:apply(meta(), {update_job_fields, 1, #{bogus => 42}}, S1),
    Job = maps:get(1, S2#ra_state.jobs),
    %% Name should be unchanged
    ?assertEqual(<<"test_job">>, Job#ra_job.name).

update_job_fields_not_found_test() ->
    S0 = fresh_state(),
    {S0, {error, not_found}, []} =
        flurm_db_ra:apply(meta(), {update_job_fields, 42, #{name => <<"x">>}}, S0).

%%====================================================================
%% allocate_job_id Tests
%%====================================================================

allocate_job_id_increments_counter_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, []} = flurm_db_ra:apply(meta(), allocate_job_id, S0),
    ?assertEqual(2, S1#ra_state.job_counter),
    {S2, {ok, 2}, []} = flurm_db_ra:apply(meta(), allocate_job_id, S1),
    ?assertEqual(3, S2#ra_state.job_counter),
    %% No jobs should actually be created
    ?assertEqual(0, maps:size(S2#ra_state.jobs)).

%%====================================================================
%% register_node Tests
%%====================================================================

register_new_node_test() ->
    S0 = fresh_state(),
    {S1, {ok, registered}, Effects} =
        flurm_db_ra:apply(meta(), {register_node, node_spec()}, S0),
    ?assert(maps:is_key(<<"node01">>, S1#ra_state.nodes)),
    Node = maps:get(<<"node01">>, S1#ra_state.nodes),
    ?assertEqual(up, Node#ra_node.state),
    ?assertEqual(16, Node#ra_node.cpus),
    ?assertMatch([{mod_call, flurm_db_ra_effects, node_registered, [_]}], Effects).

register_existing_node_returns_updated_test() ->
    S0 = fresh_state(),
    {S1, {ok, registered}, _} =
        flurm_db_ra:apply(meta(), {register_node, node_spec()}, S0),
    %% Register again with updated spec
    UpdatedSpec = (node_spec())#ra_node_spec{cpus = 32},
    {S2, {ok, updated}, Effects} =
        flurm_db_ra:apply(meta(), {register_node, UpdatedSpec}, S1),
    Node = maps:get(<<"node01">>, S2#ra_state.nodes),
    ?assertEqual(32, Node#ra_node.cpus),
    ?assertMatch([{mod_call, flurm_db_ra_effects, node_updated, [_]}], Effects).

%%====================================================================
%% update_node_state Tests
%%====================================================================

update_node_state_to_drain_test() ->
    S0 = fresh_state(),
    {S1, {ok, registered}, _} =
        flurm_db_ra:apply(meta(), {register_node, node_spec()}, S0),
    {S2, ok, Effects} =
        flurm_db_ra:apply(meta(), {update_node_state, <<"node01">>, drain}, S1),
    Node = maps:get(<<"node01">>, S2#ra_state.nodes),
    ?assertEqual(drain, Node#ra_node.state),
    ?assertMatch([{mod_call, flurm_db_ra_effects, node_state_changed,
                   [<<"node01">>, up, drain]}], Effects).

update_node_state_not_found_test() ->
    S0 = fresh_state(),
    {S0, {error, not_found}, []} =
        flurm_db_ra:apply(meta(), {update_node_state, <<"noexist">>, down}, S0).

%%====================================================================
%% unregister_node Tests
%%====================================================================

unregister_existing_node_test() ->
    S0 = fresh_state(),
    {S1, {ok, registered}, _} =
        flurm_db_ra:apply(meta(), {register_node, node_spec()}, S0),
    {S2, ok, Effects} =
        flurm_db_ra:apply(meta(), {unregister_node, <<"node01">>}, S1),
    ?assertNot(maps:is_key(<<"node01">>, S2#ra_state.nodes)),
    ?assertMatch([{mod_call, flurm_db_ra_effects, node_unregistered, [_]}], Effects).

unregister_nonexistent_node_test() ->
    S0 = fresh_state(),
    {S0, {error, not_found}, []} =
        flurm_db_ra:apply(meta(), {unregister_node, <<"noexist">>}, S0).

%%====================================================================
%% create_partition Tests
%%====================================================================

create_partition_test() ->
    S0 = fresh_state(),
    {S1, ok, Effects} =
        flurm_db_ra:apply(meta(), {create_partition, partition_spec()}, S0),
    ?assert(maps:is_key(<<"batch">>, S1#ra_state.partitions)),
    Part = maps:get(<<"batch">>, S1#ra_state.partitions),
    ?assertEqual(up, Part#ra_partition.state),
    ?assertEqual(86400, Part#ra_partition.max_time),
    ?assertMatch([{mod_call, flurm_db_ra_effects, partition_created, [_]}], Effects).

create_duplicate_partition_fails_test() ->
    S0 = fresh_state(),
    {S1, ok, _} = flurm_db_ra:apply(meta(), {create_partition, partition_spec()}, S0),
    {S1, {error, already_exists}, []} =
        flurm_db_ra:apply(meta(), {create_partition, partition_spec()}, S1).

%%====================================================================
%% delete_partition Tests
%%====================================================================

delete_existing_partition_test() ->
    S0 = fresh_state(),
    {S1, ok, _} = flurm_db_ra:apply(meta(), {create_partition, partition_spec()}, S0),
    {S2, ok, Effects} = flurm_db_ra:apply(meta(), {delete_partition, <<"batch">>}, S1),
    ?assertNot(maps:is_key(<<"batch">>, S2#ra_state.partitions)),
    ?assertMatch([{mod_call, flurm_db_ra_effects, partition_deleted, [_]}], Effects).

delete_nonexistent_partition_test() ->
    S0 = fresh_state(),
    {S0, {error, not_found}, []} =
        flurm_db_ra:apply(meta(), {delete_partition, <<"noexist">>}, S0).

%%====================================================================
%% Unknown Command Test
%%====================================================================

unknown_command_returns_error_test() ->
    S0 = fresh_state(),
    {S0, {error, {unknown_command, some_garbage}}, []} =
        flurm_db_ra:apply(meta(), some_garbage, S0).

%%====================================================================
%% Full Job Lifecycle Test
%%====================================================================

full_job_lifecycle_test() ->
    S0 = fresh_state(),
    %% 1. Submit
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    ?assertEqual(pending, (maps:get(1, S1#ra_state.jobs))#ra_job.state),
    %% 2. Allocate nodes
    {S2, ok, _} = flurm_db_ra:apply(meta(), {allocate_job, 1, [<<"n1">>]}, S1),
    ?assertEqual(configuring, (maps:get(1, S2#ra_state.jobs))#ra_job.state),
    %% 3. Move to running
    {S3, ok, _} = flurm_db_ra:apply(meta(), {update_job_state, 1, running}, S2),
    ?assertEqual(running, (maps:get(1, S3#ra_state.jobs))#ra_job.state),
    %% 4. Complete with exit code 0
    {S4, ok, _} = flurm_db_ra:apply(meta(), {set_job_exit_code, 1, 0}, S3),
    Job = maps:get(1, S4#ra_state.jobs),
    ?assertEqual(completed, Job#ra_job.state),
    ?assertEqual(0, Job#ra_job.exit_code),
    ?assertNotEqual(undefined, Job#ra_job.end_time).

full_job_lifecycle_failed_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, _} = flurm_db_ra:apply(meta(), {allocate_job, 1, [<<"n1">>]}, S1),
    {S3, ok, _} = flurm_db_ra:apply(meta(), {update_job_state, 1, running}, S2),
    %% Non-zero exit code -> failed
    {S4, ok, _} = flurm_db_ra:apply(meta(), {set_job_exit_code, 1, 1}, S3),
    Job = maps:get(1, S4#ra_state.jobs),
    ?assertEqual(failed, Job#ra_job.state),
    ?assertEqual(1, Job#ra_job.exit_code).

%%====================================================================
%% Job Counter Isolation Test
%%====================================================================

job_counter_survives_other_operations_test() ->
    S0 = fresh_state(),
    %% Submit a job (counter: 1 -> 2)
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    ?assertEqual(2, S1#ra_state.job_counter),
    %% Register a node -- should not affect job_counter
    {S2, {ok, registered}, _} = flurm_db_ra:apply(meta(), {register_node, node_spec()}, S1),
    ?assertEqual(2, S2#ra_state.job_counter),
    %% Create a partition -- should not affect job_counter
    {S3, ok, _} = flurm_db_ra:apply(meta(), {create_partition, partition_spec()}, S2),
    ?assertEqual(2, S3#ra_state.job_counter),
    %% Submit another job (counter: 2 -> 3)
    {S4, {ok, 2}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec(<<"job2">>)}, S3),
    ?assertEqual(3, S4#ra_state.job_counter).

%%====================================================================
%% Cancel Running Job Test
%%====================================================================

cancel_running_job_succeeds_test() ->
    S0 = fresh_state(),
    {S1, {ok, 1}, _} = flurm_db_ra:apply(meta(), {submit_job, job_spec()}, S0),
    {S2, ok, _} = flurm_db_ra:apply(meta(), {update_job_state, 1, running}, S1),
    %% Cancel a running job should succeed
    {S3, ok, _} = flurm_db_ra:apply(meta(), {cancel_job, 1}, S2),
    Job = maps:get(1, S3#ra_state.jobs),
    ?assertEqual(cancelled, Job#ra_job.state).
