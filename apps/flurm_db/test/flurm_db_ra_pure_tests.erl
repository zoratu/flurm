%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_db_ra module
%%%
%%% Tests the Ra state machine callbacks directly without mocking.
%%% Focuses on init/1, apply/3, state_enter/2, and helper functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Data Helpers
%%====================================================================

make_job_spec() ->
    make_job_spec(<<"test_job">>).

make_job_spec(Name) ->
    #ra_job_spec{
        name = Name,
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"default">>,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100
    }.

make_job_spec_no_priority() ->
    #ra_job_spec{
        name = <<"test_job_no_pri">>,
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"default">>,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = undefined
    }.

make_node_spec() ->
    make_node_spec(<<"node1">>).

make_node_spec(Name) ->
    #ra_node_spec{
        name = Name,
        hostname = <<"host1.example.com">>,
        port = 6818,
        cpus = 8,
        memory_mb = 16384,
        gpus = 2,
        features = [gpu, fast],
        partitions = [<<"default">>, <<"gpu">>]
    }.

make_partition_spec() ->
    make_partition_spec(<<"default">>).

make_partition_spec(Name) ->
    #ra_partition_spec{
        name = Name,
        nodes = [<<"node1">>, <<"node2">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 50
    }.

%% Helper to create a state with a pending job
state_with_pending_job() ->
    State = flurm_db_ra:init(#{}),
    JobSpec = make_job_spec(),
    {NewState, {ok, _JobId}, _Effects} = flurm_db_ra:apply(#{}, {submit_job, JobSpec}, State),
    NewState.

%% Helper to create a state with a node
state_with_node() ->
    State = flurm_db_ra:init(#{}),
    NodeSpec = make_node_spec(),
    {NewState, {ok, registered}, _Effects} = flurm_db_ra:apply(#{}, {register_node, NodeSpec}, State),
    NewState.

%% Helper to create a state with a partition
state_with_partition() ->
    State = flurm_db_ra:init(#{}),
    PartSpec = make_partition_spec(),
    {NewState, ok, _Effects} = flurm_db_ra:apply(#{}, {create_partition, PartSpec}, State),
    NewState.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    [
     {"init returns empty state",
      fun() ->
          State = flurm_db_ra:init(#{}),
          ?assertEqual(#{}, State#ra_state.jobs),
          ?assertEqual(#{}, State#ra_state.nodes),
          ?assertEqual(#{}, State#ra_state.partitions),
          ?assertEqual(1, State#ra_state.job_counter),
          ?assertEqual(1, State#ra_state.version)
      end},

     {"init ignores config",
      fun() ->
          State1 = flurm_db_ra:init(#{}),
          State2 = flurm_db_ra:init(#{some => config}),
          State3 = flurm_db_ra:init([{key, value}]),
          ?assertEqual(State1#ra_state.jobs, State2#ra_state.jobs),
          ?assertEqual(State2#ra_state.jobs, State3#ra_state.jobs)
      end}
    ].

%%====================================================================
%% apply/3 - allocate_job_id Tests
%%====================================================================

allocate_job_id_test_() ->
    [
     {"allocate_job_id returns next job id",
      fun() ->
          State = flurm_db_ra:init(#{}),
          {NewState, {ok, JobId}, Effects} = flurm_db_ra:apply(#{}, allocate_job_id, State),
          ?assertEqual(1, JobId),
          ?assertEqual(2, NewState#ra_state.job_counter),
          ?assertEqual([], Effects)
      end},

     {"allocate_job_id increments counter each time",
      fun() ->
          State0 = flurm_db_ra:init(#{}),
          {State1, {ok, 1}, []} = flurm_db_ra:apply(#{}, allocate_job_id, State0),
          {State2, {ok, 2}, []} = flurm_db_ra:apply(#{}, allocate_job_id, State1),
          {State3, {ok, 3}, []} = flurm_db_ra:apply(#{}, allocate_job_id, State2),
          ?assertEqual(4, State3#ra_state.job_counter)
      end}
    ].

%%====================================================================
%% apply/3 - submit_job Tests
%%====================================================================

submit_job_test_() ->
    [
     {"submit_job creates job with correct ID",
      fun() ->
          State = flurm_db_ra:init(#{}),
          JobSpec = make_job_spec(),
          {NewState, {ok, JobId}, Effects} = flurm_db_ra:apply(#{}, {submit_job, JobSpec}, State),
          ?assertEqual(1, JobId),
          ?assertEqual(2, NewState#ra_state.job_counter),
          ?assert(maps:is_key(1, NewState#ra_state.jobs)),
          %% Verify effect
          ?assertEqual(1, length(Effects)),
          [{mod_call, flurm_db_ra_effects, job_submitted, [Job]}] = Effects,
          ?assertEqual(1, Job#ra_job.id)
      end},

     {"submit_job sets job to pending state",
      fun() ->
          State = flurm_db_ra:init(#{}),
          JobSpec = make_job_spec(),
          {NewState, {ok, JobId}, _} = flurm_db_ra:apply(#{}, {submit_job, JobSpec}, State),
          Job = maps:get(JobId, NewState#ra_state.jobs),
          ?assertEqual(pending, Job#ra_job.state)
      end},

     {"submit_job copies spec fields correctly",
      fun() ->
          State = flurm_db_ra:init(#{}),
          JobSpec = make_job_spec(<<"my_job">>),
          {NewState, {ok, JobId}, _} = flurm_db_ra:apply(#{}, {submit_job, JobSpec}, State),
          Job = maps:get(JobId, NewState#ra_state.jobs),
          ?assertEqual(<<"my_job">>, Job#ra_job.name),
          ?assertEqual(<<"testuser">>, Job#ra_job.user),
          ?assertEqual(<<"testgroup">>, Job#ra_job.group),
          ?assertEqual(<<"default">>, Job#ra_job.partition),
          ?assertEqual(1, Job#ra_job.num_nodes),
          ?assertEqual(4, Job#ra_job.num_cpus),
          ?assertEqual(1024, Job#ra_job.memory_mb),
          ?assertEqual(3600, Job#ra_job.time_limit),
          ?assertEqual(100, Job#ra_job.priority)
      end},

     {"submit_job uses default priority when undefined",
      fun() ->
          State = flurm_db_ra:init(#{}),
          JobSpec = make_job_spec_no_priority(),
          {NewState, {ok, JobId}, _} = flurm_db_ra:apply(#{}, {submit_job, JobSpec}, State),
          Job = maps:get(JobId, NewState#ra_state.jobs),
          ?assertEqual(?DEFAULT_PRIORITY, Job#ra_job.priority)
      end},

     {"submit_job initializes time fields",
      fun() ->
          State = flurm_db_ra:init(#{}),
          JobSpec = make_job_spec(),
          {NewState, {ok, JobId}, _} = flurm_db_ra:apply(#{}, {submit_job, JobSpec}, State),
          Job = maps:get(JobId, NewState#ra_state.jobs),
          ?assert(is_integer(Job#ra_job.submit_time)),
          ?assertEqual(undefined, Job#ra_job.start_time),
          ?assertEqual(undefined, Job#ra_job.end_time),
          ?assertEqual([], Job#ra_job.allocated_nodes),
          ?assertEqual(undefined, Job#ra_job.exit_code)
      end},

     {"multiple job submissions get unique IDs",
      fun() ->
          State0 = flurm_db_ra:init(#{}),
          {State1, {ok, 1}, _} = flurm_db_ra:apply(#{}, {submit_job, make_job_spec(<<"job1">>)}, State0),
          {State2, {ok, 2}, _} = flurm_db_ra:apply(#{}, {submit_job, make_job_spec(<<"job2">>)}, State1),
          {State3, {ok, 3}, _} = flurm_db_ra:apply(#{}, {submit_job, make_job_spec(<<"job3">>)}, State2),
          ?assertEqual(3, maps:size(State3#ra_state.jobs)),
          ?assertEqual(4, State3#ra_state.job_counter)
      end}
    ].

%%====================================================================
%% apply/3 - cancel_job Tests
%%====================================================================

cancel_job_test_() ->
    [
     {"cancel_job cancels pending job",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, Effects} = flurm_db_ra:apply(#{}, {cancel_job, 1}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(cancelled, Job#ra_job.state),
          ?assert(is_integer(Job#ra_job.end_time)),
          ?assertEqual(1, length(Effects))
      end},

     {"cancel_job returns not_found for missing job",
      fun() ->
          State = flurm_db_ra:init(#{}),
          {NewState, {error, not_found}, Effects} = flurm_db_ra:apply(#{}, {cancel_job, 999}, State),
          ?assertEqual(State, NewState),
          ?assertEqual([], Effects)
      end},

     {"cancel_job fails for completed job",
      fun() ->
          State = state_with_pending_job(),
          %% Set exit code to complete the job
          {State2, ok, _} = flurm_db_ra:apply(#{}, {set_job_exit_code, 1, 0}, State),
          Job = maps:get(1, State2#ra_state.jobs),
          ?assertEqual(completed, Job#ra_job.state),
          %% Now try to cancel
          {State3, {error, already_terminal}, Effects} = flurm_db_ra:apply(#{}, {cancel_job, 1}, State2),
          ?assertEqual(State2, State3),
          ?assertEqual([], Effects)
      end},

     {"cancel_job fails for cancelled job",
      fun() ->
          State = state_with_pending_job(),
          {State2, ok, _} = flurm_db_ra:apply(#{}, {cancel_job, 1}, State),
          {State3, {error, already_terminal}, []} = flurm_db_ra:apply(#{}, {cancel_job, 1}, State2),
          ?assertEqual(State2, State3)
      end},

     {"cancel_job fails for failed job",
      fun() ->
          State = state_with_pending_job(),
          {State2, ok, _} = flurm_db_ra:apply(#{}, {set_job_exit_code, 1, 1}, State),
          Job = maps:get(1, State2#ra_state.jobs),
          ?assertEqual(failed, Job#ra_job.state),
          {_State3, {error, already_terminal}, []} = flurm_db_ra:apply(#{}, {cancel_job, 1}, State2)
      end}
    ].

%%====================================================================
%% apply/3 - update_job_state Tests
%%====================================================================

update_job_state_test_() ->
    [
     {"update_job_state changes state",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, Effects} = flurm_db_ra:apply(#{}, {update_job_state, 1, configuring}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(configuring, Job#ra_job.state),
          ?assertEqual(1, length(Effects))
      end},

     {"update_job_state to running sets start_time",
      fun() ->
          State = state_with_pending_job(),
          Job0 = maps:get(1, State#ra_state.jobs),
          ?assertEqual(undefined, Job0#ra_job.start_time),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, 1, running}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(running, Job#ra_job.state),
          ?assert(is_integer(Job#ra_job.start_time))
      end},

     {"update_job_state to completed sets end_time",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, 1, completed}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(completed, Job#ra_job.state),
          ?assert(is_integer(Job#ra_job.end_time))
      end},

     {"update_job_state to failed sets end_time",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, 1, failed}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(failed, Job#ra_job.state),
          ?assert(is_integer(Job#ra_job.end_time))
      end},

     {"update_job_state to cancelled sets end_time",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, 1, cancelled}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(cancelled, Job#ra_job.state),
          ?assert(is_integer(Job#ra_job.end_time))
      end},

     {"update_job_state to timeout sets end_time",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, 1, timeout}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(timeout, Job#ra_job.state),
          ?assert(is_integer(Job#ra_job.end_time))
      end},

     {"update_job_state to node_fail sets end_time",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, 1, node_fail}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(node_fail, Job#ra_job.state),
          ?assert(is_integer(Job#ra_job.end_time))
      end},

     {"update_job_state to configuring does not set times",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, 1, configuring}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(configuring, Job#ra_job.state),
          ?assertEqual(undefined, Job#ra_job.start_time),
          ?assertEqual(undefined, Job#ra_job.end_time)
      end},

     {"update_job_state returns not_found for missing job",
      fun() ->
          State = flurm_db_ra:init(#{}),
          {NewState, {error, not_found}, []} = flurm_db_ra:apply(#{}, {update_job_state, 999, running}, State),
          ?assertEqual(State, NewState)
      end}
    ].

%%====================================================================
%% apply/3 - allocate_job Tests
%%====================================================================

allocate_job_test_() ->
    [
     {"allocate_job assigns nodes to pending job",
      fun() ->
          State = state_with_pending_job(),
          Nodes = [<<"node1">>, <<"node2">>],
          {NewState, ok, Effects} = flurm_db_ra:apply(#{}, {allocate_job, 1, Nodes}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(configuring, Job#ra_job.state),
          ?assertEqual(Nodes, Job#ra_job.allocated_nodes),
          ?assert(is_integer(Job#ra_job.start_time)),
          ?assertEqual(1, length(Effects))
      end},

     {"allocate_job fails for non-pending job",
      fun() ->
          State = state_with_pending_job(),
          {State2, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, 1, running}, State),
          {State3, {error, {invalid_state, running}}, []} = flurm_db_ra:apply(#{}, {allocate_job, 1, [<<"n1">>]}, State2),
          ?assertEqual(State2, State3)
      end},

     {"allocate_job returns not_found for missing job",
      fun() ->
          State = flurm_db_ra:init(#{}),
          {NewState, {error, not_found}, []} = flurm_db_ra:apply(#{}, {allocate_job, 999, [<<"n1">>]}, State),
          ?assertEqual(State, NewState)
      end},

     {"allocate_job with empty node list",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {allocate_job, 1, []}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual([], Job#ra_job.allocated_nodes)
      end}
    ].

%%====================================================================
%% apply/3 - set_job_exit_code Tests
%%====================================================================

set_job_exit_code_test_() ->
    [
     {"set_job_exit_code with 0 sets completed state",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, Effects} = flurm_db_ra:apply(#{}, {set_job_exit_code, 1, 0}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(completed, Job#ra_job.state),
          ?assertEqual(0, Job#ra_job.exit_code),
          ?assert(is_integer(Job#ra_job.end_time)),
          ?assertEqual(1, length(Effects))
      end},

     {"set_job_exit_code with non-zero sets failed state",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {set_job_exit_code, 1, 1}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(failed, Job#ra_job.state),
          ?assertEqual(1, Job#ra_job.exit_code)
      end},

     {"set_job_exit_code with negative code sets failed state",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {set_job_exit_code, 1, -1}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(failed, Job#ra_job.state),
          ?assertEqual(-1, Job#ra_job.exit_code)
      end},

     {"set_job_exit_code with large code sets failed state",
      fun() ->
          State = state_with_pending_job(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {set_job_exit_code, 1, 255}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(failed, Job#ra_job.state),
          ?assertEqual(255, Job#ra_job.exit_code)
      end},

     {"set_job_exit_code returns not_found for missing job",
      fun() ->
          State = flurm_db_ra:init(#{}),
          {NewState, {error, not_found}, []} = flurm_db_ra:apply(#{}, {set_job_exit_code, 999, 0}, State),
          ?assertEqual(State, NewState)
      end}
    ].

%%====================================================================
%% apply/3 - register_node Tests
%%====================================================================

register_node_test_() ->
    [
     {"register_node adds new node",
      fun() ->
          State = flurm_db_ra:init(#{}),
          NodeSpec = make_node_spec(),
          {NewState, {ok, registered}, Effects} = flurm_db_ra:apply(#{}, {register_node, NodeSpec}, State),
          ?assert(maps:is_key(<<"node1">>, NewState#ra_state.nodes)),
          Node = maps:get(<<"node1">>, NewState#ra_state.nodes),
          ?assertEqual(<<"node1">>, Node#ra_node.name),
          ?assertEqual(<<"host1.example.com">>, Node#ra_node.hostname),
          ?assertEqual(6818, Node#ra_node.port),
          ?assertEqual(8, Node#ra_node.cpus),
          ?assertEqual(16384, Node#ra_node.memory_mb),
          ?assertEqual(2, Node#ra_node.gpus),
          ?assertEqual(up, Node#ra_node.state),
          ?assertEqual(1, length(Effects))
      end},

     {"register_node updates existing node",
      fun() ->
          State = state_with_node(),
          NodeSpec = make_node_spec(<<"node1">>),
          NodeSpec2 = NodeSpec#ra_node_spec{cpus = 16},
          {NewState, {ok, updated}, Effects} = flurm_db_ra:apply(#{}, {register_node, NodeSpec2}, State),
          Node = maps:get(<<"node1">>, NewState#ra_state.nodes),
          ?assertEqual(16, Node#ra_node.cpus),
          ?assertEqual(1, length(Effects)),
          [{mod_call, flurm_db_ra_effects, node_updated, [_]}] = Effects
      end},

     {"register_node initializes usage fields to zero",
      fun() ->
          State = flurm_db_ra:init(#{}),
          NodeSpec = make_node_spec(),
          {NewState, {ok, registered}, _} = flurm_db_ra:apply(#{}, {register_node, NodeSpec}, State),
          Node = maps:get(<<"node1">>, NewState#ra_state.nodes),
          ?assertEqual(0, Node#ra_node.cpus_used),
          ?assertEqual(0, Node#ra_node.memory_used),
          ?assertEqual(0, Node#ra_node.gpus_used),
          ?assertEqual([], Node#ra_node.running_jobs)
      end},

     {"register_node sets last_heartbeat",
      fun() ->
          State = flurm_db_ra:init(#{}),
          NodeSpec = make_node_spec(),
          {NewState, {ok, registered}, _} = flurm_db_ra:apply(#{}, {register_node, NodeSpec}, State),
          Node = maps:get(<<"node1">>, NewState#ra_state.nodes),
          ?assert(is_integer(Node#ra_node.last_heartbeat))
      end}
    ].

%%====================================================================
%% apply/3 - update_node_state Tests
%%====================================================================

update_node_state_test_() ->
    [
     {"update_node_state changes node state",
      fun() ->
          State = state_with_node(),
          {NewState, ok, Effects} = flurm_db_ra:apply(#{}, {update_node_state, <<"node1">>, down}, State),
          Node = maps:get(<<"node1">>, NewState#ra_state.nodes),
          ?assertEqual(down, Node#ra_node.state),
          ?assertEqual(1, length(Effects))
      end},

     {"update_node_state updates heartbeat",
      fun() ->
          State = state_with_node(),
          Node0 = maps:get(<<"node1">>, State#ra_state.nodes),
          OldHeartbeat = Node0#ra_node.last_heartbeat,
          %% No sleep needed - we just verify heartbeat is >= old value
          %% (monotonic, may be same if called within same microsecond)
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {update_node_state, <<"node1">>, down}, State),
          Node = maps:get(<<"node1">>, NewState#ra_state.nodes),
          ?assert(Node#ra_node.last_heartbeat >= OldHeartbeat)
      end},

     {"update_node_state returns not_found for missing node",
      fun() ->
          State = flurm_db_ra:init(#{}),
          {NewState, {error, not_found}, []} = flurm_db_ra:apply(#{}, {update_node_state, <<"missing">>, down}, State),
          ?assertEqual(State, NewState)
      end},

     {"update_node_state to drain",
      fun() ->
          State = state_with_node(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {update_node_state, <<"node1">>, drain}, State),
          Node = maps:get(<<"node1">>, NewState#ra_state.nodes),
          ?assertEqual(drain, Node#ra_node.state)
      end},

     {"update_node_state to maint",
      fun() ->
          State = state_with_node(),
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {update_node_state, <<"node1">>, maint}, State),
          Node = maps:get(<<"node1">>, NewState#ra_state.nodes),
          ?assertEqual(maint, Node#ra_node.state)
      end}
    ].

%%====================================================================
%% apply/3 - unregister_node Tests
%%====================================================================

unregister_node_test_() ->
    [
     {"unregister_node removes node",
      fun() ->
          State = state_with_node(),
          ?assert(maps:is_key(<<"node1">>, State#ra_state.nodes)),
          {NewState, ok, Effects} = flurm_db_ra:apply(#{}, {unregister_node, <<"node1">>}, State),
          ?assertNot(maps:is_key(<<"node1">>, NewState#ra_state.nodes)),
          ?assertEqual(1, length(Effects))
      end},

     {"unregister_node returns not_found for missing node",
      fun() ->
          State = flurm_db_ra:init(#{}),
          {NewState, {error, not_found}, []} = flurm_db_ra:apply(#{}, {unregister_node, <<"missing">>}, State),
          ?assertEqual(State, NewState)
      end}
    ].

%%====================================================================
%% apply/3 - create_partition Tests
%%====================================================================

create_partition_test_() ->
    [
     {"create_partition adds new partition",
      fun() ->
          State = flurm_db_ra:init(#{}),
          PartSpec = make_partition_spec(),
          {NewState, ok, Effects} = flurm_db_ra:apply(#{}, {create_partition, PartSpec}, State),
          ?assert(maps:is_key(<<"default">>, NewState#ra_state.partitions)),
          Part = maps:get(<<"default">>, NewState#ra_state.partitions),
          ?assertEqual(<<"default">>, Part#ra_partition.name),
          ?assertEqual(up, Part#ra_partition.state),
          ?assertEqual([<<"node1">>, <<"node2">>], Part#ra_partition.nodes),
          ?assertEqual(86400, Part#ra_partition.max_time),
          ?assertEqual(3600, Part#ra_partition.default_time),
          ?assertEqual(10, Part#ra_partition.max_nodes),
          ?assertEqual(50, Part#ra_partition.priority),
          ?assertEqual(1, length(Effects))
      end},

     {"create_partition fails if already exists",
      fun() ->
          State = state_with_partition(),
          PartSpec = make_partition_spec(),
          {NewState, {error, already_exists}, []} = flurm_db_ra:apply(#{}, {create_partition, PartSpec}, State),
          ?assertEqual(State, NewState)
      end}
    ].

%%====================================================================
%% apply/3 - delete_partition Tests
%%====================================================================

delete_partition_test_() ->
    [
     {"delete_partition removes partition",
      fun() ->
          State = state_with_partition(),
          ?assert(maps:is_key(<<"default">>, State#ra_state.partitions)),
          {NewState, ok, Effects} = flurm_db_ra:apply(#{}, {delete_partition, <<"default">>}, State),
          ?assertNot(maps:is_key(<<"default">>, NewState#ra_state.partitions)),
          ?assertEqual(1, length(Effects))
      end},

     {"delete_partition returns not_found for missing partition",
      fun() ->
          State = flurm_db_ra:init(#{}),
          {NewState, {error, not_found}, []} = flurm_db_ra:apply(#{}, {delete_partition, <<"missing">>}, State),
          ?assertEqual(State, NewState)
      end}
    ].

%%====================================================================
%% apply/3 - Unknown Command Tests
%%====================================================================

unknown_command_test_() ->
    [
     {"unknown command returns error",
      fun() ->
          State = flurm_db_ra:init(#{}),
          {NewState, {error, {unknown_command, unknown_cmd}}, []} = flurm_db_ra:apply(#{}, unknown_cmd, State),
          ?assertEqual(State, NewState)
      end},

     {"unknown tuple command returns error",
      fun() ->
          State = flurm_db_ra:init(#{}),
          {NewState, {error, {unknown_command, {bad, command}}}, []} = flurm_db_ra:apply(#{}, {bad, command}, State),
          ?assertEqual(State, NewState)
      end}
    ].

%%====================================================================
%% state_enter/2 Tests
%%====================================================================

state_enter_test_() ->
    [
     {"state_enter leader returns effect",
      fun() ->
          State = flurm_db_ra:init(#{}),
          Effects = flurm_db_ra:state_enter(leader, State),
          ?assertEqual(1, length(Effects)),
          [{mod_call, flurm_db_ra_effects, became_leader, [_Node]}] = Effects
      end},

     {"state_enter follower returns effect",
      fun() ->
          State = flurm_db_ra:init(#{}),
          Effects = flurm_db_ra:state_enter(follower, State),
          ?assertEqual(1, length(Effects)),
          [{mod_call, flurm_db_ra_effects, became_follower, [_Node]}] = Effects
      end},

     {"state_enter recover returns empty",
      fun() ->
          State = flurm_db_ra:init(#{}),
          Effects = flurm_db_ra:state_enter(recover, State),
          ?assertEqual([], Effects)
      end},

     {"state_enter eol returns empty",
      fun() ->
          State = flurm_db_ra:init(#{}),
          Effects = flurm_db_ra:state_enter(eol, State),
          ?assertEqual([], Effects)
      end},

     {"state_enter other returns empty",
      fun() ->
          State = flurm_db_ra:init(#{}),
          ?assertEqual([], flurm_db_ra:state_enter(candidate, State)),
          ?assertEqual([], flurm_db_ra:state_enter(pre_vote, State)),
          ?assertEqual([], flurm_db_ra:state_enter(unknown_state, State))
      end}
    ].

%%====================================================================
%% snapshot_module/0 Tests
%%====================================================================

snapshot_module_test_() ->
    [
     {"snapshot_module returns ra_machine_simple",
      fun() ->
          ?assertEqual(ra_machine_simple, flurm_db_ra:snapshot_module())
      end}
    ].

%%====================================================================
%% make_job_record/2 Tests
%%====================================================================

make_job_record_test_() ->
    [
     {"make_job_record creates correct record",
      fun() ->
          JobSpec = make_job_spec(<<"test">>),
          Job = flurm_db_ra:make_job_record(42, JobSpec),
          ?assertEqual(42, Job#ra_job.id),
          ?assertEqual(<<"test">>, Job#ra_job.name),
          ?assertEqual(<<"testuser">>, Job#ra_job.user),
          ?assertEqual(<<"testgroup">>, Job#ra_job.group),
          ?assertEqual(<<"default">>, Job#ra_job.partition),
          ?assertEqual(pending, Job#ra_job.state),
          ?assertEqual(1, Job#ra_job.num_nodes),
          ?assertEqual(4, Job#ra_job.num_cpus),
          ?assertEqual(1024, Job#ra_job.memory_mb),
          ?assertEqual(3600, Job#ra_job.time_limit),
          ?assertEqual(100, Job#ra_job.priority),
          ?assert(is_integer(Job#ra_job.submit_time)),
          ?assertEqual(undefined, Job#ra_job.start_time),
          ?assertEqual(undefined, Job#ra_job.end_time),
          ?assertEqual([], Job#ra_job.allocated_nodes),
          ?assertEqual(undefined, Job#ra_job.exit_code)
      end},

     {"make_job_record uses default priority when undefined",
      fun() ->
          JobSpec = make_job_spec_no_priority(),
          Job = flurm_db_ra:make_job_record(1, JobSpec),
          ?assertEqual(?DEFAULT_PRIORITY, Job#ra_job.priority)
      end}
    ].

%%====================================================================
%% make_node_record/1 Tests
%%====================================================================

make_node_record_test_() ->
    [
     {"make_node_record creates correct record",
      fun() ->
          NodeSpec = make_node_spec(<<"test_node">>),
          Node = flurm_db_ra:make_node_record(NodeSpec),
          ?assertEqual(<<"test_node">>, Node#ra_node.name),
          ?assertEqual(<<"host1.example.com">>, Node#ra_node.hostname),
          ?assertEqual(6818, Node#ra_node.port),
          ?assertEqual(8, Node#ra_node.cpus),
          ?assertEqual(0, Node#ra_node.cpus_used),
          ?assertEqual(16384, Node#ra_node.memory_mb),
          ?assertEqual(0, Node#ra_node.memory_used),
          ?assertEqual(2, Node#ra_node.gpus),
          ?assertEqual(0, Node#ra_node.gpus_used),
          ?assertEqual(up, Node#ra_node.state),
          ?assertEqual([gpu, fast], Node#ra_node.features),
          ?assertEqual([<<"default">>, <<"gpu">>], Node#ra_node.partitions),
          ?assertEqual([], Node#ra_node.running_jobs),
          ?assert(is_integer(Node#ra_node.last_heartbeat))
      end}
    ].

%%====================================================================
%% make_partition_record/1 Tests
%%====================================================================

make_partition_record_test_() ->
    [
     {"make_partition_record creates correct record",
      fun() ->
          PartSpec = make_partition_spec(<<"test_part">>),
          Part = flurm_db_ra:make_partition_record(PartSpec),
          ?assertEqual(<<"test_part">>, Part#ra_partition.name),
          ?assertEqual(up, Part#ra_partition.state),
          ?assertEqual([<<"node1">>, <<"node2">>], Part#ra_partition.nodes),
          ?assertEqual(86400, Part#ra_partition.max_time),
          ?assertEqual(3600, Part#ra_partition.default_time),
          ?assertEqual(10, Part#ra_partition.max_nodes),
          ?assertEqual(50, Part#ra_partition.priority)
      end}
    ].

%%====================================================================
%% Complex Workflow Tests
%%====================================================================

workflow_test_() ->
    [
     {"full job lifecycle",
      fun() ->
          %% Initialize
          State0 = flurm_db_ra:init(#{}),

          %% Submit job
          JobSpec = make_job_spec(<<"lifecycle_job">>),
          {State1, {ok, JobId}, _} = flurm_db_ra:apply(#{}, {submit_job, JobSpec}, State0),
          ?assertEqual(1, JobId),

          %% Verify pending
          Job1 = maps:get(JobId, State1#ra_state.jobs),
          ?assertEqual(pending, Job1#ra_job.state),

          %% Allocate
          {State2, ok, _} = flurm_db_ra:apply(#{}, {allocate_job, JobId, [<<"node1">>]}, State1),
          Job2 = maps:get(JobId, State2#ra_state.jobs),
          ?assertEqual(configuring, Job2#ra_job.state),
          ?assertEqual([<<"node1">>], Job2#ra_job.allocated_nodes),

          %% Set running
          {State3, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, JobId, running}, State2),
          Job3 = maps:get(JobId, State3#ra_state.jobs),
          ?assertEqual(running, Job3#ra_job.state),

          %% Complete
          {State4, ok, _} = flurm_db_ra:apply(#{}, {set_job_exit_code, JobId, 0}, State3),
          Job4 = maps:get(JobId, State4#ra_state.jobs),
          ?assertEqual(completed, Job4#ra_job.state),
          ?assertEqual(0, Job4#ra_job.exit_code)
      end},

     {"node registration and update workflow",
      fun() ->
          State0 = flurm_db_ra:init(#{}),

          %% Register node
          NodeSpec = make_node_spec(<<"workflow_node">>),
          {State1, {ok, registered}, _} = flurm_db_ra:apply(#{}, {register_node, NodeSpec}, State0),
          Node1 = maps:get(<<"workflow_node">>, State1#ra_state.nodes),
          ?assertEqual(up, Node1#ra_node.state),

          %% Drain node
          {State2, ok, _} = flurm_db_ra:apply(#{}, {update_node_state, <<"workflow_node">>, drain}, State1),
          Node2 = maps:get(<<"workflow_node">>, State2#ra_state.nodes),
          ?assertEqual(drain, Node2#ra_node.state),

          %% Unregister
          {State3, ok, _} = flurm_db_ra:apply(#{}, {unregister_node, <<"workflow_node">>}, State2),
          ?assertNot(maps:is_key(<<"workflow_node">>, State3#ra_state.nodes))
      end},

     {"partition management workflow",
      fun() ->
          State0 = flurm_db_ra:init(#{}),

          %% Create partition
          PartSpec = make_partition_spec(<<"test_part">>),
          {State1, ok, _} = flurm_db_ra:apply(#{}, {create_partition, PartSpec}, State0),
          ?assert(maps:is_key(<<"test_part">>, State1#ra_state.partitions)),

          %% Try to create duplicate
          {State2, {error, already_exists}, []} = flurm_db_ra:apply(#{}, {create_partition, PartSpec}, State1),
          ?assertEqual(State1, State2),

          %% Delete partition
          {State3, ok, _} = flurm_db_ra:apply(#{}, {delete_partition, <<"test_part">>}, State2),
          ?assertNot(maps:is_key(<<"test_part">>, State3#ra_state.partitions)),

          %% Try to delete non-existent
          {State4, {error, not_found}, []} = flurm_db_ra:apply(#{}, {delete_partition, <<"test_part">>}, State3),
          ?assertEqual(State3, State4)
      end},

     {"multiple jobs and nodes",
      fun() ->
          State0 = flurm_db_ra:init(#{}),

          %% Register multiple nodes
          {State1, {ok, registered}, _} = flurm_db_ra:apply(#{}, {register_node, make_node_spec(<<"n1">>)}, State0),
          {State2, {ok, registered}, _} = flurm_db_ra:apply(#{}, {register_node, make_node_spec(<<"n2">>)}, State1),
          {State3, {ok, registered}, _} = flurm_db_ra:apply(#{}, {register_node, make_node_spec(<<"n3">>)}, State2),
          ?assertEqual(3, maps:size(State3#ra_state.nodes)),

          %% Submit multiple jobs
          {State4, {ok, 1}, _} = flurm_db_ra:apply(#{}, {submit_job, make_job_spec(<<"j1">>)}, State3),
          {State5, {ok, 2}, _} = flurm_db_ra:apply(#{}, {submit_job, make_job_spec(<<"j2">>)}, State4),
          {State6, {ok, 3}, _} = flurm_db_ra:apply(#{}, {submit_job, make_job_spec(<<"j3">>)}, State5),
          ?assertEqual(3, maps:size(State6#ra_state.jobs)),
          ?assertEqual(4, State6#ra_state.job_counter)
      end}
    ].

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    [
     {"job with zero time limit",
      fun() ->
          State = flurm_db_ra:init(#{}),
          JobSpec = #ra_job_spec{
              name = <<"zero_time">>,
              user = <<"user">>,
              group = <<"group">>,
              partition = <<"default">>,
              script = <<"#!/bin/bash">>,
              num_nodes = 1,
              num_cpus = 1,
              memory_mb = 1,
              time_limit = 0,
              priority = 1
          },
          {NewState, {ok, 1}, _} = flurm_db_ra:apply(#{}, {submit_job, JobSpec}, State),
          Job = maps:get(1, NewState#ra_state.jobs),
          ?assertEqual(0, Job#ra_job.time_limit)
      end},

     {"node with zero GPUs",
      fun() ->
          State = flurm_db_ra:init(#{}),
          NodeSpec = #ra_node_spec{
              name = <<"nogpu">>,
              hostname = <<"host">>,
              port = 6818,
              cpus = 4,
              memory_mb = 8192,
              gpus = 0,
              features = [],
              partitions = []
          },
          {NewState, {ok, registered}, _} = flurm_db_ra:apply(#{}, {register_node, NodeSpec}, State),
          Node = maps:get(<<"nogpu">>, NewState#ra_state.nodes),
          ?assertEqual(0, Node#ra_node.gpus)
      end},

     {"partition with empty node list",
      fun() ->
          State = flurm_db_ra:init(#{}),
          PartSpec = #ra_partition_spec{
              name = <<"empty">>,
              nodes = [],
              max_time = 3600,
              default_time = 600,
              max_nodes = 0,
              priority = 0
          },
          {NewState, ok, _} = flurm_db_ra:apply(#{}, {create_partition, PartSpec}, State),
          Part = maps:get(<<"empty">>, NewState#ra_state.partitions),
          ?assertEqual([], Part#ra_partition.nodes)
      end},

     {"running job does not overwrite start_time",
      fun() ->
          State0 = flurm_db_ra:init(#{}),
          {State1, {ok, 1}, _} = flurm_db_ra:apply(#{}, {submit_job, make_job_spec()}, State0),
          {State2, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, 1, running}, State1),
          Job2 = maps:get(1, State2#ra_state.jobs),
          StartTime = Job2#ra_job.start_time,
          ?assert(is_integer(StartTime)),

          %% Update to running again should not change start_time
          {State3, ok, _} = flurm_db_ra:apply(#{}, {update_job_state, 1, running}, State2),
          Job3 = maps:get(1, State3#ra_state.jobs),
          ?assertEqual(StartTime, Job3#ra_job.start_time)
      end}
    ].
