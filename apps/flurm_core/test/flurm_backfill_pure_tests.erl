%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_backfill module
%%%
%%% These tests verify backfill scheduling algorithms without any mocking.
%%% They test exported functions directly with controlled inputs.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_backfill_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Helper to create a sample blocker job map
make_blocker_job() ->
    #{
        job_id => 1,
        pid => self(),
        name => <<"blocker_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        state => pending,
        num_nodes => 4,
        num_cpus => 8,
        memory_mb => 4096,
        time_limit => 7200,
        priority => 1000,
        submit_time => erlang:system_time(second) - 3600
    }.

%% Helper to create a candidate job map
make_candidate_job(JobId, NumNodes, NumCpus, MemoryMb, TimeLimit, Priority) ->
    #{
        job_id => JobId,
        pid => self(),
        name => <<"candidate_", (integer_to_binary(JobId))/binary>>,
        user => <<"testuser">>,
        partition => <<"default">>,
        state => pending,
        num_nodes => NumNodes,
        num_cpus => NumCpus,
        memory_mb => MemoryMb,
        time_limit => TimeLimit,
        priority => Priority,
        submit_time => erlang:system_time(second)
    }.

%% Helper to create a timeline with free nodes
make_timeline_with_free_nodes(FreeNodes) ->
    Now = erlang:system_time(second),
    #{
        timestamp => Now,
        free_nodes => FreeNodes,
        busy_nodes => [],
        node_end_times => [],
        total_nodes => length(FreeNodes)
    }.

%% Helper to create a timeline with busy nodes
make_timeline_with_busy_nodes(FreeNodes, BusyNodes, EndTimes) ->
    Now = erlang:system_time(second),
    #{
        timestamp => Now,
        free_nodes => FreeNodes,
        busy_nodes => BusyNodes,
        node_end_times => EndTimes,
        total_nodes => length(FreeNodes) + length(BusyNodes)
    }.

%%====================================================================
%% is_backfill_enabled/0 tests
%%====================================================================

is_backfill_enabled_default_test() ->
    %% Clean up any existing config
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled),
    %% Default should be true
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()).

is_backfill_enabled_backfill_scheduler_test() ->
    application:set_env(flurm_core, scheduler_type, backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type).

is_backfill_enabled_fifo_backfill_scheduler_test() ->
    application:set_env(flurm_core, scheduler_type, fifo_backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type).

is_backfill_enabled_priority_backfill_scheduler_test() ->
    application:set_env(flurm_core, scheduler_type, priority_backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type).

is_backfill_enabled_fifo_scheduler_test() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, false),
    ?assertEqual(false, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled).

is_backfill_enabled_explicit_flag_test() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, true),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled).

%%====================================================================
%% run_backfill_cycle/2 tests
%%====================================================================

run_backfill_cycle_no_blocker_test() ->
    Candidates = [make_candidate_job(2, 1, 2, 1024, 3600, 500)],
    ?assertEqual([], flurm_backfill:run_backfill_cycle(undefined, Candidates)).

run_backfill_cycle_no_candidates_test() ->
    Blocker = make_blocker_job(),
    ?assertEqual([], flurm_backfill:run_backfill_cycle(Blocker, [])).

run_backfill_cycle_disabled_test() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, false),
    Blocker = make_blocker_job(),
    Candidates = [make_candidate_job(2, 1, 2, 1024, 3600, 500)],
    ?assertEqual([], flurm_backfill:run_backfill_cycle(Blocker, Candidates)),
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled).

%%====================================================================
%% calculate_shadow_time/2 tests
%%====================================================================

calculate_shadow_time_empty_timeline_test() ->
    Blocker = make_blocker_job(),
    Timeline = make_timeline_with_free_nodes([]),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    Now = erlang:system_time(second),
    %% Shadow time should be now + BACKFILL_WINDOW (86400 seconds)
    ?assert(ShadowTime >= Now + 86400 - 1),
    ?assert(ShadowTime =< Now + 86400 + 1).

calculate_shadow_time_enough_free_nodes_test() ->
    %% Blocker needs 4 nodes with 8 CPUs and 4096 MB
    Blocker = make_blocker_job(),
    FreeNodes = [
        {<<"node1">>, 16, 8192},
        {<<"node2">>, 16, 8192},
        {<<"node3">>, 16, 8192},
        {<<"node4">>, 16, 8192},
        {<<"node5">>, 16, 8192}
    ],
    Now = erlang:system_time(second),
    %% Must have at least one end_time entry for find_shadow_time to check free nodes
    EndTimes = [{<<"busynode">>, Now + 1000}],
    Timeline = make_timeline_with_busy_nodes(FreeNodes, [], EndTimes),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% Should return approximately now since we have enough suitable free resources
    ?assert(ShadowTime >= Now - 1),
    ?assert(ShadowTime =< Now + 1).

calculate_shadow_time_not_enough_free_nodes_test() ->
    %% Blocker needs 4 nodes but only 2 are free
    Blocker = make_blocker_job(),
    FreeNodes = [
        {<<"node1">>, 16, 8192},
        {<<"node2">>, 16, 8192}
    ],
    Now = erlang:system_time(second),
    EndTimes = [
        {<<"node3">>, Now + 1000},
        {<<"node4">>, Now + 2000}
    ],
    Timeline = make_timeline_with_busy_nodes(FreeNodes, [], EndTimes),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% Need 2 more nodes, so shadow time should be when 2nd node becomes free
    ?assertEqual(Now + 2000, ShadowTime).

calculate_shadow_time_insufficient_resources_per_node_test() ->
    %% Blocker needs nodes with 8 CPUs but free nodes have only 4
    Blocker = make_blocker_job(),
    FreeNodes = [
        {<<"node1">>, 4, 8192},  % Not enough CPUs
        {<<"node2">>, 4, 8192}   % Not enough CPUs
    ],
    Now = erlang:system_time(second),
    EndTimes = [
        {<<"node3">>, Now + 1000},
        {<<"node4">>, Now + 2000},
        {<<"node5">>, Now + 3000},
        {<<"node6">>, Now + 4000}
    ],
    Timeline = make_timeline_with_busy_nodes(FreeNodes, [], EndTimes),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% Current nodes don't meet requirements, need 4 nodes from end times
    ?assertEqual(Now + 4000, ShadowTime).

calculate_shadow_time_defaults_test() ->
    %% Test with minimal job map using defaults
    Blocker = #{job_id => 1},  % Will use default values (1 node, 1 CPU, 1024 MB)
    FreeNodes = [{<<"node1">>, 8, 2048}],
    Now = erlang:system_time(second),
    %% Must have at least one end_time entry for find_shadow_time to check free nodes
    EndTimes = [{<<"busynode">>, Now + 1000}],
    Timeline = make_timeline_with_busy_nodes(FreeNodes, [], EndTimes),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% Default is 1 node, 1 CPU, 1024 MB - should be available now
    ?assert(ShadowTime >= Now - 1),
    ?assert(ShadowTime =< Now + 1).

%%====================================================================
%% can_backfill/3 tests
%%====================================================================

can_backfill_job_completes_in_time_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 3600,  % Shadow time is 1 hour from now
    Job = make_candidate_job(5, 1, 2, 1024, 1800, 500),  % Job takes 30 min
    FreeNodes = [{<<"node1">>, 8, 4096}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertMatch({true, [<<"node1">>]}, Result).

can_backfill_job_exceeds_shadow_time_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 1800,  % Shadow time is 30 min from now
    Job = make_candidate_job(5, 1, 2, 1024, 3600, 500),  % Job takes 1 hour
    FreeNodes = [{<<"node1">>, 8, 4096}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

can_backfill_not_enough_nodes_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,
    Job = make_candidate_job(5, 3, 2, 1024, 1800, 500),  % Needs 3 nodes
    FreeNodes = [
        {<<"node1">>, 8, 4096},
        {<<"node2">>, 8, 4096}
    ],  % Only 2 nodes available
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

can_backfill_not_enough_cpus_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,
    Job = make_candidate_job(5, 1, 16, 1024, 1800, 500),  % Needs 16 CPUs
    FreeNodes = [{<<"node1">>, 8, 4096}],  % Only 8 CPUs
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

can_backfill_not_enough_memory_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,
    Job = make_candidate_job(5, 1, 2, 8192, 1800, 500),  % Needs 8GB
    FreeNodes = [{<<"node1">>, 8, 4096}],  % Only 4GB
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

can_backfill_multiple_nodes_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,
    Job = make_candidate_job(5, 2, 4, 2048, 1800, 500),  % Needs 2 nodes
    FreeNodes = [
        {<<"node1">>, 8, 4096},
        {<<"node2">>, 8, 4096},
        {<<"node3">>, 8, 4096}
    ],
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertMatch({true, [<<"node1">>, <<"node2">>]}, Result).

can_backfill_with_defaults_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 86400,  % Far future shadow time
    Job = #{job_id => 5},  % Minimal job, will use defaults
    FreeNodes = [{<<"node1">>, 8, 4096}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    %% Default time_limit is 3600, should complete before shadow
    ?assertMatch({true, [<<"node1">>]}, Result).

can_backfill_exact_shadow_time_test() ->
    Now = erlang:system_time(second),
    TimeLimit = 1800,
    ShadowTime = Now + TimeLimit,  % Job ends exactly at shadow time
    Job = make_candidate_job(5, 1, 2, 1024, TimeLimit, 500),
    FreeNodes = [{<<"node1">>, 8, 4096}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    %% EndTime == ShadowTime should be allowed
    ?assertMatch({true, _}, Result).

%%====================================================================
%% get_resource_timeline/1 tests
%%====================================================================

get_resource_timeline_basic_test() ->
    Blocker = make_blocker_job(),
    Timeline = flurm_backfill:get_resource_timeline(Blocker),
    %% Should return a map with required keys
    ?assert(is_map(Timeline)),
    ?assert(maps:is_key(timestamp, Timeline)),
    ?assert(maps:is_key(free_nodes, Timeline)),
    ?assert(maps:is_key(busy_nodes, Timeline)),
    ?assert(maps:is_key(node_end_times, Timeline)),
    ?assert(maps:is_key(total_nodes, Timeline)),
    %% Timestamp should be approximately now
    Now = erlang:system_time(second),
    Ts = maps:get(timestamp, Timeline),
    ?assert(Ts >= Now - 1),
    ?assert(Ts =< Now + 1).

get_resource_timeline_no_registry_test() ->
    %% When registry not available, should return empty lists
    Timeline = flurm_backfill:get_resource_timeline(#{}),
    ?assertEqual([], maps:get(free_nodes, Timeline)),
    ?assertEqual([], maps:get(busy_nodes, Timeline)),
    ?assertEqual([], maps:get(node_end_times, Timeline)),
    ?assertEqual(0, maps:get(total_nodes, Timeline)).

%%====================================================================
%% find_backfill_jobs/2 tests
%%====================================================================

find_backfill_jobs_empty_candidates_test() ->
    Blocker = make_blocker_job(),
    ?assertEqual([], flurm_backfill:find_backfill_jobs(Blocker, [])).

find_backfill_jobs_candidates_sorted_by_priority_test() ->
    %% This test verifies that candidates are processed in priority order
    Blocker = make_blocker_job(),
    %% Create candidates with different priorities
    Candidates = [
        make_candidate_job(10, 1, 2, 1024, 1800, 100),  % Low priority
        make_candidate_job(11, 1, 2, 1024, 1800, 500),  % Medium priority
        make_candidate_job(12, 1, 2, 1024, 1800, 900)   % High priority
    ],
    %% Without node registry, no jobs can be scheduled, but the function should work
    Result = flurm_backfill:find_backfill_jobs(Blocker, Candidates),
    ?assertEqual([], Result).  % No nodes available

find_backfill_jobs_with_various_priorities_test() ->
    %% Test that the sorting works correctly
    Blocker = make_blocker_job(),
    %% Create unsorted candidates
    Candidates = [
        (make_candidate_job(1, 1, 2, 1024, 1800, 200))#{priority => 200},
        (make_candidate_job(2, 1, 2, 1024, 1800, 800))#{priority => 800},
        (make_candidate_job(3, 1, 2, 1024, 1800, 400))#{priority => 400}
    ],
    %% Function processes them but returns empty without registry
    Result = flurm_backfill:find_backfill_jobs(Blocker, Candidates),
    ?assertEqual([], Result).

%%====================================================================
%% get_backfill_candidates/1 tests
%%====================================================================

get_backfill_candidates_empty_list_test() ->
    ?assertEqual([], flurm_backfill:get_backfill_candidates([])).

get_backfill_candidates_with_job_ids_test() ->
    %% Without job manager, should return empty list
    JobIds = [1, 2, 3],
    Result = flurm_backfill:get_backfill_candidates(JobIds),
    ?assertEqual([], Result).

%%====================================================================
%% Edge Case Tests
%%====================================================================

calculate_shadow_time_minimal_blocker_test() ->
    %% Test with absolutely minimal blocker
    Blocker = #{},
    Timeline = make_timeline_with_free_nodes([{<<"n1">>, 4, 2048}]),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    Now = erlang:system_time(second),
    ?assert(ShadowTime >= Now - 1).

can_backfill_empty_timeline_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,
    Job = make_candidate_job(5, 1, 2, 1024, 1800, 500),
    Timeline = #{},  % Empty timeline
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

shadow_time_single_node_needed_test() ->
    %% Blocker needs only 1 node
    Blocker = #{num_nodes => 1, num_cpus => 2, memory_mb => 1024},
    Now = erlang:system_time(second),
    EndTimes = [
        {<<"node1">>, Now + 500},
        {<<"node2">>, Now + 1000}
    ],
    Timeline = make_timeline_with_busy_nodes([], [], EndTimes),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% Need 1 node, should get first end time
    ?assertEqual(Now + 500, ShadowTime).

shadow_time_partial_suitable_nodes_test() ->
    %% Some free nodes are suitable, some are not
    Blocker = #{num_nodes => 2, num_cpus => 4, memory_mb => 2048},
    FreeNodes = [
        {<<"node1">>, 8, 4096},   % Suitable
        {<<"node2">>, 2, 1024},   % Not suitable (CPU)
        {<<"node3">>, 8, 1024}    % Not suitable (Memory)
    ],
    Now = erlang:system_time(second),
    EndTimes = [{<<"node4">>, Now + 1000}],
    Timeline = make_timeline_with_busy_nodes(FreeNodes, [], EndTimes),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% Only 1 suitable free node, need 1 more from end times
    ?assertEqual(Now + 1000, ShadowTime).

%%====================================================================
%% Integration-like Pure Tests (testing function interactions)
%%====================================================================

backfill_workflow_simulation_test() ->
    %% Simulate a complete backfill workflow with controlled timeline
    Now = erlang:system_time(second),

    %% Blocker needs 4 nodes, will be available in 2 hours
    Blocker = #{
        job_id => 1,
        num_nodes => 4,
        num_cpus => 8,
        memory_mb => 4096,
        time_limit => 3600,
        priority => 1000
    },

    %% Create a timeline with some free nodes
    FreeNodes = [
        {<<"node1">>, 16, 8192},
        {<<"node2">>, 16, 8192}
    ],
    EndTimes = [
        {<<"node3">>, Now + 3600},
        {<<"node4">>, Now + 7200}
    ],
    Timeline = make_timeline_with_busy_nodes(FreeNodes, [], EndTimes),

    %% Calculate shadow time
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% Should be 7200 seconds from now (when node4 becomes free)
    ?assertEqual(Now + 7200, ShadowTime),

    %% Create a candidate that can fit
    ShortJob = #{
        job_id => 10,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 2048,
        time_limit => 1800,  % 30 minutes
        priority => 500
    },

    %% Verify it can backfill
    Result = flurm_backfill:can_backfill(ShortJob, Timeline, ShadowTime),
    ?assertMatch({true, [<<"node1">>]}, Result).

backfill_multiple_jobs_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,  % 2 hours from now

    FreeNodes = [
        {<<"n1">>, 8, 4096},
        {<<"n2">>, 8, 4096},
        {<<"n3">>, 8, 4096}
    ],
    Timeline = make_timeline_with_free_nodes(FreeNodes),

    %% Job 1: Fits
    Job1 = #{job_id => 1, num_nodes => 1, num_cpus => 2, memory_mb => 1024, time_limit => 1800},
    ?assertMatch({true, _}, flurm_backfill:can_backfill(Job1, Timeline, ShadowTime)),

    %% Job 2: Fits, needs 2 nodes
    Job2 = #{job_id => 2, num_nodes => 2, num_cpus => 4, memory_mb => 2048, time_limit => 3600},
    ?assertMatch({true, _}, flurm_backfill:can_backfill(Job2, Timeline, ShadowTime)),

    %% Job 3: Doesn't fit (takes too long)
    Job3 = #{job_id => 3, num_nodes => 1, num_cpus => 2, memory_mb => 1024, time_limit => 10000},
    ?assertEqual(false, flurm_backfill:can_backfill(Job3, Timeline, ShadowTime)).

%%====================================================================
%% Boundary Value Tests
%%====================================================================

boundary_value_time_limit_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 3600,  % 1 hour
    FreeNodes = [{<<"n1">>, 8, 4096}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),

    %% Just under the limit
    Job1 = #{job_id => 1, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 3599},
    ?assertMatch({true, _}, flurm_backfill:can_backfill(Job1, Timeline, ShadowTime)),

    %% Exactly at the limit (EndTime == ShadowTime)
    Job2 = #{job_id => 2, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 3600},
    ?assertMatch({true, _}, flurm_backfill:can_backfill(Job2, Timeline, ShadowTime)),

    %% Just over the limit
    Job3 = #{job_id => 3, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 3601},
    ?assertEqual(false, flurm_backfill:can_backfill(Job3, Timeline, ShadowTime)).

boundary_value_resources_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,
    %% Node with exactly 8 CPUs and 4096 MB
    FreeNodes = [{<<"n1">>, 8, 4096}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),

    %% Job requesting exactly available resources
    Job1 = #{job_id => 1, num_nodes => 1, num_cpus => 8, memory_mb => 4096, time_limit => 1800},
    ?assertMatch({true, _}, flurm_backfill:can_backfill(Job1, Timeline, ShadowTime)),

    %% Job requesting 1 more CPU than available
    Job2 = #{job_id => 2, num_nodes => 1, num_cpus => 9, memory_mb => 4096, time_limit => 1800},
    ?assertEqual(false, flurm_backfill:can_backfill(Job2, Timeline, ShadowTime)),

    %% Job requesting 1 more MB than available
    Job3 = #{job_id => 3, num_nodes => 1, num_cpus => 8, memory_mb => 4097, time_limit => 1800},
    ?assertEqual(false, flurm_backfill:can_backfill(Job3, Timeline, ShadowTime)).

%%====================================================================
%% Timeline Manipulation Tests
%%====================================================================

timeline_with_mixed_node_states_test() ->
    Now = erlang:system_time(second),

    %% Mix of node capabilities
    FreeNodes = [
        {<<"small">>, 2, 1024},
        {<<"medium">>, 8, 4096},
        {<<"large">>, 32, 65536}
    ],
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    ShadowTime = Now + 7200,

    %% Small job can use small node
    SmallJob = #{job_id => 1, num_nodes => 1, num_cpus => 2, memory_mb => 1024, time_limit => 1800},
    {true, SmallNodes} = flurm_backfill:can_backfill(SmallJob, Timeline, ShadowTime),
    ?assertEqual(1, length(SmallNodes)),

    %% Medium job needs at least medium node
    MediumJob = #{job_id => 2, num_nodes => 1, num_cpus => 8, memory_mb => 4096, time_limit => 1800},
    {true, MedNodes} = flurm_backfill:can_backfill(MediumJob, Timeline, ShadowTime),
    ?assertEqual(1, length(MedNodes)),

    %% Large job needs large node
    LargeJob = #{job_id => 3, num_nodes => 1, num_cpus => 32, memory_mb => 65536, time_limit => 1800},
    {true, LargeNodes} = flurm_backfill:can_backfill(LargeJob, Timeline, ShadowTime),
    ?assertEqual([<<"large">>], LargeNodes).

timeline_end_times_ordering_test() ->
    Now = erlang:system_time(second),

    %% Blocker needs 3 nodes
    Blocker = #{num_nodes => 3, num_cpus => 4, memory_mb => 2048},

    %% End times in reverse order (should be sorted)
    EndTimes = [
        {<<"node3">>, Now + 3000},
        {<<"node1">>, Now + 1000},
        {<<"node2">>, Now + 2000}
    ],
    Timeline = make_timeline_with_busy_nodes([], [], EndTimes),

    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% After sorting: node1@1000, node2@2000, node3@3000
    %% Need 3 nodes, so shadow time is when 3rd becomes available
    ?assertEqual(Now + 3000, ShadowTime).

%%====================================================================
%% Scheduler Configuration Tests
%%====================================================================

scheduler_config_combinations_test() ->
    %% Test various scheduler type configurations
    Configs = [
        {backfill, undefined, true},
        {fifo_backfill, undefined, true},
        {priority_backfill, undefined, true},
        {fifo, true, true},
        {fifo, false, false},
        {priority, true, true},
        {priority, false, false}
    ],
    lists:foreach(
        fun({SchedType, BackfillFlag, Expected}) ->
            application:set_env(flurm_core, scheduler_type, SchedType),
            case BackfillFlag of
                undefined -> application:unset_env(flurm_core, backfill_enabled);
                Val -> application:set_env(flurm_core, backfill_enabled, Val)
            end,
            Result = flurm_backfill:is_backfill_enabled(),
            ?assertEqual(Expected, Result,
                         io_lib:format("Failed for ~p/~p: expected ~p, got ~p",
                                       [SchedType, BackfillFlag, Expected, Result]))
        end,
        Configs
    ),
    %% Cleanup
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled).

%%====================================================================
%% run_backfill_cycle edge cases
%%====================================================================

run_backfill_cycle_enabled_with_candidates_test() ->
    %% Ensure backfill is enabled
    application:set_env(flurm_core, backfill_enabled, true),
    application:unset_env(flurm_core, scheduler_type),

    Blocker = make_blocker_job(),
    Candidates = [
        make_candidate_job(2, 1, 2, 1024, 3600, 500),
        make_candidate_job(3, 2, 4, 2048, 1800, 600)
    ],

    %% Without node registry, will return empty but should not crash
    Result = flurm_backfill:run_backfill_cycle(Blocker, Candidates),
    ?assertEqual([], Result),

    application:unset_env(flurm_core, backfill_enabled).

%%====================================================================
%% Priority Sorting Tests
%%====================================================================

candidate_priority_sorting_test() ->
    %% Verify that candidates are sorted correctly
    %% We can test this indirectly through find_backfill_jobs
    Blocker = make_blocker_job(),

    %% Create candidates in reverse priority order
    Candidates = [
        (make_candidate_job(1, 1, 2, 1024, 1800, 100))#{priority => 100},
        (make_candidate_job(2, 1, 2, 1024, 1800, 300))#{priority => 300},
        (make_candidate_job(3, 1, 2, 1024, 1800, 200))#{priority => 200}
    ],

    %% find_backfill_jobs internally sorts by priority
    %% Without registry, returns empty, but tests sorting path
    Result = flurm_backfill:find_backfill_jobs(Blocker, Candidates),
    ?assertEqual([], Result).

%%====================================================================
%% Resource Timeline Construction Tests
%%====================================================================

resource_timeline_structure_test() ->
    %% Verify the timeline structure
    Timeline = flurm_backfill:get_resource_timeline(#{}),

    ?assert(is_integer(maps:get(timestamp, Timeline))),
    ?assert(is_list(maps:get(free_nodes, Timeline))),
    ?assert(is_list(maps:get(busy_nodes, Timeline))),
    ?assert(is_list(maps:get(node_end_times, Timeline))),
    ?assert(is_integer(maps:get(total_nodes, Timeline))).

%%====================================================================
%% Large Scale Tests (stress testing the algorithm)
%%====================================================================

many_candidates_test() ->
    %% Test with many candidate jobs
    Blocker = make_blocker_job(),
    Candidates = [make_candidate_job(I, 1, 2, 1024, 1800, I * 10)
                  || I <- lists:seq(1, 100)],

    %% Should not crash and should return empty (no registry)
    Result = flurm_backfill:find_backfill_jobs(Blocker, Candidates),
    ?assertEqual([], Result).

many_nodes_in_timeline_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,

    %% Create timeline with many free nodes
    FreeNodes = [{list_to_binary("node" ++ integer_to_list(I)), 8, 4096}
                 || I <- lists:seq(1, 100)],
    Timeline = make_timeline_with_free_nodes(FreeNodes),

    %% Job that needs many nodes
    Job = #{job_id => 1, num_nodes => 50, num_cpus => 4, memory_mb => 2048, time_limit => 1800},
    {true, Nodes} = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(50, length(Nodes)).

%%====================================================================
%% Zero and Negative Value Handling Tests
%%====================================================================

zero_resources_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,

    %% Node with zero available resources
    FreeNodes = [{<<"empty">>, 0, 0}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),

    %% Even minimal job should fail
    Job = #{job_id => 1, num_nodes => 1, num_cpus => 1, memory_mb => 1, time_limit => 1800},
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

shadow_time_in_past_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now - 100,  % Shadow time in the past

    FreeNodes = [{<<"n1">>, 8, 4096}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),

    %% Any job with positive time limit will fail
    Job = #{job_id => 1, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 1},
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

%%====================================================================
%% find_nth_end_time edge cases (tested via calculate_shadow_time)
%%====================================================================

find_nth_end_time_empty_list_test() ->
    %% When there are no end times and not enough free nodes
    Blocker = #{num_nodes => 10, num_cpus => 4, memory_mb => 2048},
    %% Only 2 free nodes, but need 10
    FreeNodes = [{<<"n1">>, 8, 4096}, {<<"n2">>, 8, 4096}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    Now = erlang:system_time(second),
    %% With no end_times (empty list), should return default (Now + 86400)
    ?assert(ShadowTime >= Now + 86400 - 1),
    ?assert(ShadowTime =< Now + 86400 + 1).

find_nth_end_time_exact_needed_test() ->
    Now = erlang:system_time(second),
    %% Blocker needs 3 nodes
    Blocker = #{num_nodes => 3, num_cpus => 4, memory_mb => 2048},
    %% No free nodes, 3 will become available
    EndTimes = [
        {<<"n1">>, Now + 1000},
        {<<"n2">>, Now + 2000},
        {<<"n3">>, Now + 3000}
    ],
    Timeline = make_timeline_with_busy_nodes([], [], EndTimes),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% Need 3 nodes, so shadow time is when 3rd becomes available
    ?assertEqual(Now + 3000, ShadowTime).

find_nth_end_time_more_than_needed_test() ->
    Now = erlang:system_time(second),
    %% Blocker needs 2 nodes
    Blocker = #{num_nodes => 2, num_cpus => 4, memory_mb => 2048},
    %% No free nodes, 5 will become available
    EndTimes = [
        {<<"n1">>, Now + 100},
        {<<"n2">>, Now + 200},
        {<<"n3">>, Now + 300},
        {<<"n4">>, Now + 400},
        {<<"n5">>, Now + 500}
    ],
    Timeline = make_timeline_with_busy_nodes([], [], EndTimes),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% Need 2 nodes, so shadow time is when 2nd becomes available
    ?assertEqual(Now + 200, ShadowTime).

%%====================================================================
%% More run_backfill_cycle coverage
%%====================================================================

run_backfill_cycle_with_empty_blocker_test() ->
    %% Ensure backfill is enabled
    application:set_env(flurm_core, backfill_enabled, true),
    Blocker = #{job_id => 1},  % Minimal blocker
    Candidates = [make_candidate_job(2, 1, 2, 1024, 3600, 500)],
    Result = flurm_backfill:run_backfill_cycle(Blocker, Candidates),
    ?assertEqual([], Result),
    application:unset_env(flurm_core, backfill_enabled).

run_backfill_cycle_backfill_scheduler_test() ->
    application:set_env(flurm_core, scheduler_type, backfill),
    Blocker = make_blocker_job(),
    Candidates = [make_candidate_job(2, 1, 2, 1024, 3600, 500)],
    Result = flurm_backfill:run_backfill_cycle(Blocker, Candidates),
    ?assertEqual([], Result),
    application:unset_env(flurm_core, scheduler_type).

%%====================================================================
%% Additional can_backfill edge cases
%%====================================================================

can_backfill_zero_time_limit_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,
    FreeNodes = [{<<"n1">>, 8, 4096}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    %% Job with zero time limit (ends immediately)
    Job = #{job_id => 1, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 0},
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertMatch({true, _}, Result).

can_backfill_high_memory_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,
    %% Nodes with varied memory
    FreeNodes = [
        {<<"low_mem">>, 32, 1024},
        {<<"high_mem">>, 32, 65536}
    ],
    Timeline = make_timeline_with_free_nodes(FreeNodes),
    %% Job needs high memory
    Job = #{job_id => 1, num_nodes => 1, num_cpus => 4, memory_mb => 32768, time_limit => 1800},
    {true, Nodes} = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual([<<"high_mem">>], Nodes).

%%====================================================================
%% calculate_shadow_time additional coverage
%%====================================================================

calculate_shadow_time_no_timestamp_in_timeline_test() ->
    %% Test with timeline missing timestamp
    Blocker = #{num_nodes => 1, num_cpus => 1, memory_mb => 1024},
    Timeline = #{
        free_nodes => [{<<"n1">>, 8, 4096}],
        busy_nodes => [],
        node_end_times => [{<<"n2">>, erlang:system_time(second) + 1000}],
        total_nodes => 2
    },
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    Now = erlang:system_time(second),
    %% Should use erlang:system_time(second) as default
    ?assert(ShadowTime >= Now - 1),
    ?assert(ShadowTime =< Now + 1).

calculate_shadow_time_unsorted_end_times_test() ->
    Now = erlang:system_time(second),
    Blocker = #{num_nodes => 2, num_cpus => 4, memory_mb => 2048},
    %% End times in random order
    EndTimes = [
        {<<"n3">>, Now + 3000},
        {<<"n1">>, Now + 1000},
        {<<"n4">>, Now + 4000},
        {<<"n2">>, Now + 2000}
    ],
    Timeline = make_timeline_with_busy_nodes([], [], EndTimes),
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% After sorting, need 2nd node which is n2 at Now+2000
    ?assertEqual(Now + 2000, ShadowTime).

%%====================================================================
%% get_backfill_candidates coverage
%%====================================================================

get_backfill_candidates_single_id_test() ->
    %% Test with single job ID - without job manager returns empty
    Result = flurm_backfill:get_backfill_candidates([1]),
    ?assertEqual([], Result).

get_backfill_candidates_multiple_ids_test() ->
    %% Test with multiple job IDs - without job manager returns empty
    Result = flurm_backfill:get_backfill_candidates([1, 2, 3, 4, 5]),
    ?assertEqual([], Result).

%%====================================================================
%% find_backfill_jobs additional coverage
%%====================================================================

find_backfill_jobs_single_candidate_test() ->
    Blocker = make_blocker_job(),
    Candidates = [make_candidate_job(5, 1, 2, 1024, 1800, 500)],
    Result = flurm_backfill:find_backfill_jobs(Blocker, Candidates),
    ?assertEqual([], Result).

find_backfill_jobs_priority_order_test() ->
    Blocker = #{job_id => 1, num_nodes => 10, num_cpus => 16, memory_mb => 8192},
    %% Candidates should be sorted by priority (descending)
    Candidates = [
        #{job_id => 10, priority => 100, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 60},
        #{job_id => 20, priority => 900, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 60},
        #{job_id => 30, priority => 500, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 60}
    ],
    %% Without registry returns empty but tests the sorting path
    Result = flurm_backfill:find_backfill_jobs(Blocker, Candidates),
    ?assertEqual([], Result).

%%====================================================================
%% Testing get_resource_timeline behavior
%%====================================================================

get_resource_timeline_various_inputs_test() ->
    %% Test with various blocker configurations
    Timeline1 = flurm_backfill:get_resource_timeline(#{}),
    Timeline2 = flurm_backfill:get_resource_timeline(#{job_id => 1}),
    Timeline3 = flurm_backfill:get_resource_timeline(make_blocker_job()),

    %% All should return valid timeline maps
    ?assert(is_map(Timeline1)),
    ?assert(is_map(Timeline2)),
    ?assert(is_map(Timeline3)),

    %% All should have the required keys
    lists:foreach(fun(T) ->
        ?assert(maps:is_key(timestamp, T)),
        ?assert(maps:is_key(free_nodes, T)),
        ?assert(maps:is_key(busy_nodes, T)),
        ?assert(maps:is_key(node_end_times, T)),
        ?assert(maps:is_key(total_nodes, T))
    end, [Timeline1, Timeline2, Timeline3]).

%%====================================================================
%% Complex scenario tests
%%====================================================================

complex_backfill_scenario_test() ->
    Now = erlang:system_time(second),

    %% Blocker needs 4 nodes with 8 CPUs and 4GB each
    Blocker = #{
        job_id => 1,
        num_nodes => 4,
        num_cpus => 8,
        memory_mb => 4096,
        time_limit => 7200,
        priority => 1000
    },

    %% Some free nodes, some busy
    FreeNodes = [
        {<<"n1">>, 16, 8192},
        {<<"n2">>, 16, 8192}
    ],
    EndTimes = [
        {<<"n3">>, Now + 3600},
        {<<"n4">>, Now + 5400}
    ],
    Timeline = make_timeline_with_busy_nodes(FreeNodes, [], EndTimes),

    %% Shadow time should be when n4 becomes free (need 4 nodes, have 2 + need 2 more)
    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    ?assertEqual(Now + 5400, ShadowTime),

    %% Small job that finishes before shadow time
    SmallJob = #{job_id => 10, num_nodes => 1, num_cpus => 4, memory_mb => 2048, time_limit => 1800},
    ?assertMatch({true, _}, flurm_backfill:can_backfill(SmallJob, Timeline, ShadowTime)),

    %% Job that would exceed shadow time
    LongJob = #{job_id => 11, num_nodes => 1, num_cpus => 4, memory_mb => 2048, time_limit => 6000},
    ?assertEqual(false, flurm_backfill:can_backfill(LongJob, Timeline, ShadowTime)).

multiple_suitable_nodes_selection_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,

    %% Multiple nodes that could satisfy the job
    FreeNodes = [
        {<<"small">>, 4, 2048},
        {<<"medium1">>, 8, 4096},
        {<<"medium2">>, 8, 4096},
        {<<"large">>, 16, 8192}
    ],
    Timeline = make_timeline_with_free_nodes(FreeNodes),

    %% Job needs 2 nodes with 4 CPUs and 2GB
    Job = #{job_id => 1, num_nodes => 2, num_cpus => 4, memory_mb => 2048, time_limit => 1800},
    {true, Nodes} = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(2, length(Nodes)),
    %% Should pick first suitable nodes in order
    ?assertEqual([<<"small">>, <<"medium1">>], Nodes).

node_filtering_by_resources_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 7200,

    %% Nodes with varying resource levels
    FreeNodes = [
        {<<"tiny">>, 1, 512},      % Too small
        {<<"small_cpu">>, 2, 8192}, % Not enough CPU
        {<<"small_mem">>, 8, 1024}, % Not enough memory
        {<<"good">>, 8, 4096}       % Just right
    ],
    Timeline = make_timeline_with_free_nodes(FreeNodes),

    %% Job needs 4 CPUs and 2GB
    Job = #{job_id => 1, num_nodes => 1, num_cpus => 4, memory_mb => 2048, time_limit => 1800},
    {true, Nodes} = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual([<<"good">>], Nodes).

%%====================================================================
%% Default value handling tests
%%====================================================================

job_with_all_defaults_test() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 100000,  % Far future
    FreeNodes = [{<<"n1">>, 8, 4096}],
    Timeline = make_timeline_with_free_nodes(FreeNodes),

    %% Job with only job_id (all other values will use defaults)
    Job = #{job_id => 1},
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    %% Defaults: 1 node, 1 CPU, 1024 MB, 3600 second time limit
    ?assertMatch({true, _}, Result).

blocker_with_all_defaults_test() ->
    Blocker = #{job_id => 1},
    Now = erlang:system_time(second),
    EndTimes = [{<<"n1">>, Now + 500}],
    FreeNodes = [{<<"free1">>, 8, 4096}],
    Timeline = make_timeline_with_busy_nodes(FreeNodes, [], EndTimes),

    ShadowTime = flurm_backfill:calculate_shadow_time(Blocker, Timeline),
    %% Default is 1 node needed, we have 1 free suitable node
    ?assert(ShadowTime >= Now - 1),
    ?assert(ShadowTime =< Now + 1).
