%%%-------------------------------------------------------------------
%%% @doc FLURM Database Ra Effects -ifdef(TEST) Exports Tests
%%%
%%% Tests for internal helper functions exported via -ifdef(TEST).
%%% These tests directly call the internal functions to achieve coverage.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_effects_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% log_event/2 Tests
%%====================================================================

log_event_test_() ->
    [
        {"log_event/2 logs job event", fun() ->
            %% log_event uses error_logger, should not crash
            ?assertEqual(ok, flurm_db_ra_effects:log_event(job_submitted, #{job_id => 1}))
        end},

        {"log_event/2 logs node event", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:log_event(node_registered, #{node_name => <<"n1">>}))
        end},

        {"log_event/2 logs state change event", fun() ->
            Data = #{job_id => 42, old_state => pending, new_state => running},
            ?assertEqual(ok, flurm_db_ra_effects:log_event(job_state_changed, Data))
        end},

        {"log_event/2 with empty data", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:log_event(generic_event, #{}))
        end}
    ].

%%====================================================================
%% notify_scheduler/1 Tests
%%====================================================================

notify_scheduler_test_() ->
    [
        {"notify_scheduler/1 returns ok when scheduler not running", fun() ->
            %% When flurm_scheduler is not registered, should return ok
            ?assertEqual(ok, flurm_db_ra_effects:notify_scheduler({job_pending, 1}))
        end},

        {"notify_scheduler/1 with job_running event", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:notify_scheduler({job_running, 42}))
        end},

        {"notify_scheduler/1 with job_completed event", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:notify_scheduler({job_completed, 100}))
        end},

        {"notify_scheduler/1 with node_down event", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:notify_scheduler({node_down, <<"node1">>}))
        end}
    ].

%%====================================================================
%% notify_node_manager/1 Tests
%%====================================================================

notify_node_manager_test_() ->
    [
        {"notify_node_manager/1 returns ok when manager not running", fun() ->
            %% When flurm_node_manager is not registered, should return ok
            Event = {allocate, <<"node1">>, 1, {4, 1024, 0}},
            ?assertEqual(ok, flurm_db_ra_effects:notify_node_manager(Event))
        end},

        {"notify_node_manager/1 with release event", fun() ->
            Event = {release, <<"node2">>, 5},
            ?assertEqual(ok, flurm_db_ra_effects:notify_node_manager(Event))
        end}
    ].

%%====================================================================
%% release_nodes/2 Tests
%%====================================================================

release_nodes_test_() ->
    [
        {"release_nodes/2 with empty list", fun() ->
            %% No nodes to release, should return ok
            ?assertEqual(ok, flurm_db_ra_effects:release_nodes(1, []))
        end},

        {"release_nodes/2 with single node", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:release_nodes(42, [<<"node1">>]))
        end},

        {"release_nodes/2 with multiple nodes", fun() ->
            Nodes = [<<"n1">>, <<"n2">>, <<"n3">>],
            ?assertEqual(ok, flurm_db_ra_effects:release_nodes(100, Nodes))
        end}
    ].

%%====================================================================
%% maybe_start_scheduler/0 Tests
%%====================================================================

maybe_start_scheduler_test() ->
    %% When scheduler is not running, should return ok
    ?assertEqual(ok, flurm_db_ra_effects:maybe_start_scheduler()).

%%====================================================================
%% maybe_stop_scheduler/0 Tests
%%====================================================================

maybe_stop_scheduler_test() ->
    %% When scheduler is not running, should return ok
    ?assertEqual(ok, flurm_db_ra_effects:maybe_stop_scheduler()).

%%====================================================================
%% notify_failover_handler/1 Tests
%%====================================================================

notify_failover_handler_test_() ->
    [
        {"notify_failover_handler/1 with became_leader", fun() ->
            %% When failover handler is not running, should return ok
            ?assertEqual(ok, flurm_db_ra_effects:notify_failover_handler(became_leader))
        end},

        {"notify_failover_handler/1 with became_follower", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:notify_failover_handler(became_follower))
        end}
    ].

%%====================================================================
%% notify_subscribers/1 Tests
%%====================================================================

notify_subscribers_test_() ->
    [
        {"notify_subscribers/1 with job event", fun() ->
            %% When no subscribers, should return ok
            ?assertEqual(ok, flurm_db_ra_effects:notify_subscribers({job_submitted, test_job}))
        end},

        {"notify_subscribers/1 with node event", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:notify_subscribers({node_registered, test_node}))
        end},

        {"notify_subscribers/1 with state change", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:notify_subscribers({became_leader, node()}))
        end}
    ].

%%====================================================================
%% job_to_map/1 Tests
%%====================================================================

job_to_map_test_() ->
    [
        {"job_to_map/1 converts all fields", fun() ->
            Job = create_test_job(),
            Result = flurm_db_ra_effects:job_to_map(Job),
            ?assert(is_map(Result)),
            ?assertEqual(42, maps:get(id, Result)),
            ?assertEqual(<<"test_job">>, maps:get(name, Result)),
            ?assertEqual(<<"testuser">>, maps:get(user, Result)),
            ?assertEqual(<<"default">>, maps:get(partition, Result)),
            ?assertEqual(pending, maps:get(state, Result)),
            ?assertEqual(2, maps:get(num_nodes, Result)),
            ?assertEqual(8, maps:get(num_cpus, Result)),
            ?assertEqual(4096, maps:get(memory_mb, Result)),
            ?assertEqual(100, maps:get(priority, Result))
        end},

        {"job_to_map/1 with different states", fun() ->
            Job = create_test_job(),
            RunningJob = Job#ra_job{state = running},
            Result = flurm_db_ra_effects:job_to_map(RunningJob),
            ?assertEqual(running, maps:get(state, Result))
        end}
    ].

%%====================================================================
%% node_to_map/1 Tests
%%====================================================================

node_to_map_test_() ->
    [
        {"node_to_map/1 converts all fields", fun() ->
            Node = create_test_node(),
            Result = flurm_db_ra_effects:node_to_map(Node),
            ?assert(is_map(Result)),
            ?assertEqual(<<"compute1">>, maps:get(name, Result)),
            ?assertEqual(<<"compute1.cluster.local">>, maps:get(hostname, Result)),
            ?assertEqual(64, maps:get(cpus, Result)),
            ?assertEqual(256000, maps:get(memory_mb, Result)),
            ?assertEqual(up, maps:get(state, Result))
        end},

        {"node_to_map/1 with different states", fun() ->
            Node = create_test_node(),
            DrainNode = Node#ra_node{state = drain},
            Result = flurm_db_ra_effects:node_to_map(DrainNode),
            ?assertEqual(drain, maps:get(state, Result))
        end}
    ].

%%====================================================================
%% Public API integration tests (for additional coverage)
%%====================================================================

subscribe_unsubscribe_test() ->
    %% Test the public subscribe/unsubscribe API
    Self = self(),
    ?assertEqual(ok, flurm_db_ra_effects:subscribe(Self)),
    ?assertEqual(ok, flurm_db_ra_effects:unsubscribe(Self)).

%%====================================================================
%% Effect handler tests (public API but exercises internal functions)
%%====================================================================

job_submitted_effect_test() ->
    Job = create_test_job(),
    ?assertEqual(ok, flurm_db_ra_effects:job_submitted(Job)).

job_cancelled_effect_test() ->
    Job = create_test_job(),
    ?assertEqual(ok, flurm_db_ra_effects:job_cancelled(Job)).

job_cancelled_with_nodes_effect_test() ->
    Job = create_test_job(),
    JobWithNodes = Job#ra_job{allocated_nodes = [<<"n1">>, <<"n2">>]},
    ?assertEqual(ok, flurm_db_ra_effects:job_cancelled(JobWithNodes)).

job_state_changed_effect_test_() ->
    [
        {"pending to configuring", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:job_state_changed(1, pending, configuring))
        end},
        {"configuring to running", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:job_state_changed(2, configuring, running))
        end},
        {"running to completed", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:job_state_changed(3, running, completed))
        end},
        {"running to failed", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:job_state_changed(4, running, failed))
        end},
        {"generic state change", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:job_state_changed(5, pending, pending))
        end}
    ].

job_allocated_effect_test() ->
    Job = create_test_job(),
    Nodes = [<<"node1">>, <<"node2">>],
    ?assertEqual(ok, flurm_db_ra_effects:job_allocated(Job, Nodes)).

job_completed_effect_test_() ->
    [
        {"job completed with exit 0", fun() ->
            Job = create_test_job(),
            ?assertEqual(ok, flurm_db_ra_effects:job_completed(Job, 0))
        end},
        {"job completed with exit 1", fun() ->
            Job = create_test_job(),
            JobWithNodes = Job#ra_job{allocated_nodes = [<<"n1">>]},
            ?assertEqual(ok, flurm_db_ra_effects:job_completed(JobWithNodes, 1))
        end}
    ].

node_registered_effect_test() ->
    Node = create_test_node(),
    ?assertEqual(ok, flurm_db_ra_effects:node_registered(Node)).

node_updated_effect_test() ->
    Node = create_test_node(),
    ?assertEqual(ok, flurm_db_ra_effects:node_updated(Node)).

node_state_changed_effect_test_() ->
    [
        {"node goes down", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:node_state_changed(<<"n1">>, up, down))
        end},
        {"node comes up", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:node_state_changed(<<"n2">>, down, up))
        end},
        {"node starts draining", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:node_state_changed(<<"n3">>, up, drain))
        end},
        {"generic state change", fun() ->
            ?assertEqual(ok, flurm_db_ra_effects:node_state_changed(<<"n4">>, maint, maint))
        end}
    ].

node_unregistered_effect_test() ->
    Node = create_test_node(),
    ?assertEqual(ok, flurm_db_ra_effects:node_unregistered(Node)).

node_unregistered_with_running_jobs_effect_test() ->
    Node = create_test_node(),
    NodeWithJobs = Node#ra_node{running_jobs = [1, 2, 3]},
    %% This will try to update job states, which will fail in test env, but should not crash
    ?assertEqual(ok, flurm_db_ra_effects:node_unregistered(NodeWithJobs)).

partition_created_effect_test() ->
    Part = create_test_partition(),
    ?assertEqual(ok, flurm_db_ra_effects:partition_created(Part)).

partition_deleted_effect_test() ->
    Part = create_test_partition(),
    ?assertEqual(ok, flurm_db_ra_effects:partition_deleted(Part)).

became_leader_effect_test() ->
    ?assertEqual(ok, flurm_db_ra_effects:became_leader(node())).

became_follower_effect_test() ->
    ?assertEqual(ok, flurm_db_ra_effects:became_follower(node())).

%%====================================================================
%% Helper Functions
%%====================================================================

create_test_job() ->
    #ra_job{
        id = 42,
        name = <<"test_job">>,
        user = <<"testuser">>,
        group = <<"testgroup">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash\necho test">>,
        num_nodes = 2,
        num_cpus = 8,
        memory_mb = 4096,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined
    }.

create_test_node() ->
    #ra_node{
        name = <<"compute1">>,
        hostname = <<"compute1.cluster.local">>,
        port = 7000,
        cpus = 64,
        cpus_used = 0,
        memory_mb = 256000,
        memory_used = 0,
        gpus = 8,
        gpus_used = 0,
        state = up,
        features = [gpu, nvme],
        partitions = [<<"batch">>, <<"gpu">>],
        running_jobs = [],
        last_heartbeat = erlang:system_time(second)
    }.

create_test_partition() ->
    #ra_partition{
        name = <<"batch">>,
        state = up,
        nodes = [<<"n1">>, <<"n2">>, <<"n3">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 100,
        priority = 10
    }.
