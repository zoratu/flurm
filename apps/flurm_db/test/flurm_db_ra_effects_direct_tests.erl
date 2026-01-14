%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_db_ra_effects module
%%%
%%% These tests call flurm_db_ra_effects functions directly to get code coverage.
%%% External dependencies are mocked, but the module under test is NOT mocked.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_effects_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

job_effects_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Job submitted effect", fun job_submitted_test/0},
      {"Job cancelled effect - no nodes", fun job_cancelled_no_nodes_test/0},
      {"Job cancelled effect - with nodes", fun job_cancelled_with_nodes_test/0},
      {"Job state changed - pending to configuring", fun job_state_pending_to_configuring_test/0},
      {"Job state changed - configuring to running", fun job_state_configuring_to_running_test/0},
      {"Job state changed - to completed", fun job_state_to_completed_test/0},
      {"Job state changed - to failed", fun job_state_to_failed_test/0},
      {"Job state changed - other transition", fun job_state_other_test/0},
      {"Job allocated effect", fun job_allocated_test/0},
      {"Job completed effect - no nodes", fun job_completed_no_nodes_test/0},
      {"Job completed effect - with nodes", fun job_completed_with_nodes_test/0}
     ]}.

node_effects_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Node registered effect", fun node_registered_test/0},
      {"Node updated effect", fun node_updated_test/0},
      {"Node state changed - to down", fun node_state_to_down_test/0},
      {"Node state changed - to up", fun node_state_to_up_test/0},
      {"Node state changed - to drain", fun node_state_to_drain_test/0},
      {"Node state changed - other", fun node_state_other_test/0},
      {"Node unregistered effect - no jobs", fun node_unregistered_no_jobs_test/0},
      {"Node unregistered effect - with jobs", fun node_unregistered_with_jobs_test/0}
     ]}.

partition_effects_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Partition created effect", fun partition_created_test/0},
      {"Partition deleted effect", fun partition_deleted_test/0}
     ]}.

leadership_effects_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Became leader effect - scheduler not running", fun became_leader_no_scheduler_test/0},
      {"Became leader effect - scheduler running", fun became_leader_scheduler_running_test/0},
      {"Became follower effect - scheduler not running", fun became_follower_no_scheduler_test/0},
      {"Became follower effect - scheduler running", fun became_follower_scheduler_running_test/0}
     ]}.

subscriber_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Subscribe and receive events", fun subscribe_receive_events_test/0},
      {"Unsubscribe", fun unsubscribe_test/0}
     ]}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup() ->
    %% Start pg for subscriber management
    case pg:start(pg) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    ok.

cleanup(_) ->
    %% Clean up any test subscribers
    catch pg:leave(flurm_db_effects, self()),
    ok.

%%====================================================================
%% Job Effects Tests
%%====================================================================

job_submitted_test() ->
    Job = make_test_job(),
    %% Should not crash even without scheduler running
    ok = flurm_db_ra_effects:job_submitted(Job).

job_cancelled_no_nodes_test() ->
    Job = make_test_job(),
    ok = flurm_db_ra_effects:job_cancelled(Job).

job_cancelled_with_nodes_test() ->
    Job = (make_test_job())#ra_job{allocated_nodes = [<<"node1">>, <<"node2">>]},
    ok = flurm_db_ra_effects:job_cancelled(Job).

job_state_pending_to_configuring_test() ->
    ok = flurm_db_ra_effects:job_state_changed(1, pending, configuring).

job_state_configuring_to_running_test() ->
    ok = flurm_db_ra_effects:job_state_changed(1, configuring, running).

job_state_to_completed_test() ->
    ok = flurm_db_ra_effects:job_state_changed(1, running, completed).

job_state_to_failed_test() ->
    ok = flurm_db_ra_effects:job_state_changed(1, running, failed).

job_state_other_test() ->
    ok = flurm_db_ra_effects:job_state_changed(1, pending, held).

job_allocated_test() ->
    Job = make_test_job(),
    Nodes = [<<"node1">>, <<"node2">>],
    ok = flurm_db_ra_effects:job_allocated(Job, Nodes).

job_completed_no_nodes_test() ->
    Job = make_test_job(),
    ok = flurm_db_ra_effects:job_completed(Job, 0).

job_completed_with_nodes_test() ->
    Job = (make_test_job())#ra_job{allocated_nodes = [<<"node1">>]},
    ok = flurm_db_ra_effects:job_completed(Job, 1).

%%====================================================================
%% Node Effects Tests
%%====================================================================

node_registered_test() ->
    Node = make_test_node(),
    ok = flurm_db_ra_effects:node_registered(Node).

node_updated_test() ->
    Node = make_test_node(),
    ok = flurm_db_ra_effects:node_updated(Node).

node_state_to_down_test() ->
    ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, up, down).

node_state_to_up_test() ->
    ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, down, up).

node_state_to_drain_test() ->
    ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, up, drain).

node_state_other_test() ->
    ok = flurm_db_ra_effects:node_state_changed(<<"node1">>, maint, maint).

node_unregistered_no_jobs_test() ->
    Node = make_test_node(),
    ok = flurm_db_ra_effects:node_unregistered(Node).

node_unregistered_with_jobs_test() ->
    Node = (make_test_node())#ra_node{running_jobs = [1, 2, 3]},
    %% Mock flurm_db_ra:update_job_state to avoid actual Ra calls
    meck:new(flurm_db_ra, [passthrough]),
    meck:expect(flurm_db_ra, update_job_state, fun(_, _) -> ok end),
    ok = flurm_db_ra_effects:node_unregistered(Node),
    ?assert(meck:called(flurm_db_ra, update_job_state, [1, node_fail])),
    ?assert(meck:called(flurm_db_ra, update_job_state, [2, node_fail])),
    ?assert(meck:called(flurm_db_ra, update_job_state, [3, node_fail])),
    meck:unload(flurm_db_ra).

%%====================================================================
%% Partition Effects Tests
%%====================================================================

partition_created_test() ->
    Partition = make_test_partition(),
    ok = flurm_db_ra_effects:partition_created(Partition).

partition_deleted_test() ->
    Partition = make_test_partition(),
    ok = flurm_db_ra_effects:partition_deleted(Partition).

%%====================================================================
%% Leadership Effects Tests
%%====================================================================

became_leader_no_scheduler_test() ->
    %% No scheduler registered
    ok = flurm_db_ra_effects:became_leader(node()).

became_leader_scheduler_running_test() ->
    %% Register a mock scheduler
    Scheduler = spawn(fun() -> receive activate_scheduling -> ok end end),
    register(flurm_scheduler, Scheduler),
    ok = flurm_db_ra_effects:became_leader(node()),
    %% Clean up
    unregister(flurm_scheduler),
    exit(Scheduler, kill).

became_follower_no_scheduler_test() ->
    ok = flurm_db_ra_effects:became_follower(node()).

became_follower_scheduler_running_test() ->
    %% Register a mock scheduler
    Scheduler = spawn(fun() -> receive pause_scheduling -> ok end end),
    register(flurm_scheduler, Scheduler),
    ok = flurm_db_ra_effects:became_follower(node()),
    %% Clean up
    unregister(flurm_scheduler),
    exit(Scheduler, kill).

%%====================================================================
%% Subscriber Tests
%%====================================================================

subscribe_receive_events_test() ->
    %% Subscribe
    ok = flurm_db_ra_effects:subscribe(self()),

    %% Trigger an event
    Job = make_test_job(),
    ok = flurm_db_ra_effects:job_submitted(Job),

    %% Should receive the event
    receive
        {flurm_db_event, {job_submitted, _}} -> ok
    after 1000 ->
        ?assert(false)
    end,

    %% Clean up
    ok = flurm_db_ra_effects:unsubscribe(self()).

unsubscribe_test() ->
    %% Subscribe
    ok = flurm_db_ra_effects:subscribe(self()),

    %% Unsubscribe
    ok = flurm_db_ra_effects:unsubscribe(self()),

    %% Trigger an event
    ok = flurm_db_ra_effects:job_submitted(make_test_job()),

    %% Should NOT receive the event
    receive
        {flurm_db_event, _} -> ?assert(false)
    after 100 ->
        ok
    end.

%%====================================================================
%% Test Helpers
%%====================================================================

make_test_job() ->
    #ra_job{
        id = 1,
        name = <<"test_job">>,
        user = <<"user1">>,
        group = <<"users">>,
        partition = <<"batch">>,
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
    }.

make_test_node() ->
    #ra_node{
        name = <<"node1">>,
        hostname = <<"node1.example.com">>,
        port = 6818,
        cpus = 16,
        cpus_used = 0,
        memory_mb = 32768,
        memory_used = 0,
        gpus = 2,
        gpus_used = 0,
        state = up,
        features = [gpu, fast],
        partitions = [<<"batch">>],
        running_jobs = [],
        last_heartbeat = erlang:system_time(second)
    }.

make_test_partition() ->
    #ra_partition{
        name = <<"batch">>,
        state = up,
        nodes = [<<"node1">>, <<"node2">>],
        max_time = 86400,
        default_time = 3600,
        max_nodes = 10,
        priority = 1
    }.
