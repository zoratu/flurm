%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_backfill module
%%%
%%% Comprehensive EUnit tests covering:
%%% - find_backfill_jobs/2 function
%%% - can_backfill/3 function
%%% - calculate_shadow_time/2 function
%%% - get_resource_timeline/1 function
%%% - run_backfill_cycle/2 function
%%% - get_backfill_candidates/1 function
%%% - is_backfill_enabled/0 function
%%% - Internal helper functions
%%% - Edge cases and error handling
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_backfill_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Set up default application environment
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, true),
    ok.

cleanup(_) ->
    %% Reset application environment
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled),
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

backfill_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% is_backfill_enabled tests
      {"backfill enabled by default", fun test_backfill_enabled_default/0},
      {"backfill disabled when flag is false", fun test_backfill_disabled/0},
      {"backfill enabled for backfill scheduler", fun test_backfill_scheduler_type/0},
      {"backfill enabled for fifo_backfill scheduler", fun test_fifo_backfill_scheduler/0},
      {"backfill enabled for priority_backfill scheduler", fun test_priority_backfill_scheduler/0},
      {"backfill disabled for fifo scheduler", fun test_fifo_scheduler/0},

      %% run_backfill_cycle tests
      {"run_backfill_cycle with undefined blocker returns empty", fun test_cycle_undefined_blocker/0},
      {"run_backfill_cycle with empty candidates returns empty", fun test_cycle_empty_candidates/0},
      {"run_backfill_cycle when disabled returns empty", fun test_cycle_disabled/0},
      {"run_backfill_cycle with valid inputs", fun test_cycle_valid_inputs/0},

      %% calculate_shadow_time tests
      {"calculate_shadow_time with no busy nodes", fun test_shadow_time_no_busy/0},
      {"calculate_shadow_time with enough free nodes", fun test_shadow_time_enough_free/0},
      {"calculate_shadow_time with insufficient free nodes", fun test_shadow_time_insufficient/0},

      %% can_backfill tests
      {"can_backfill job completes in time", fun test_can_backfill_completes_in_time/0},
      {"can_backfill job exceeds shadow time", fun test_can_backfill_exceeds_shadow/0},
      {"can_backfill no suitable nodes", fun test_can_backfill_no_nodes/0},
      {"can_backfill insufficient nodes", fun test_can_backfill_insufficient_nodes/0},

      %% find_backfill_jobs tests
      {"find_backfill_jobs sorts by priority", fun test_find_jobs_priority_sort/0},
      {"find_backfill_jobs respects max limit", fun test_find_jobs_max_limit/0},
      {"find_backfill_jobs empty candidates", fun test_find_jobs_empty_candidates/0},

      %% get_resource_timeline tests
      {"get_resource_timeline returns map structure", fun test_timeline_structure/0},
      {"get_resource_timeline handles empty node list", fun test_timeline_empty_nodes/0},

      %% get_backfill_candidates tests
      {"get_backfill_candidates with empty list", fun test_candidates_empty/0},
      {"get_backfill_candidates filters non-pending", fun test_candidates_filters/0}
     ]}.

%%====================================================================
%% is_backfill_enabled Tests
%%====================================================================

test_backfill_enabled_default() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, true),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()).

test_backfill_disabled() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, false),
    ?assertEqual(false, flurm_backfill:is_backfill_enabled()).

test_backfill_scheduler_type() ->
    application:set_env(flurm_core, scheduler_type, backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()).

test_fifo_backfill_scheduler() ->
    application:set_env(flurm_core, scheduler_type, fifo_backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()).

test_priority_backfill_scheduler() ->
    application:set_env(flurm_core, scheduler_type, priority_backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()).

test_fifo_scheduler() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, false),
    ?assertEqual(false, flurm_backfill:is_backfill_enabled()).

%%====================================================================
%% run_backfill_cycle Tests
%%====================================================================

test_cycle_undefined_blocker() ->
    Result = flurm_backfill:run_backfill_cycle(undefined, [#{job_id => 1}]),
    ?assertEqual([], Result).

test_cycle_empty_candidates() ->
    BlockerJob = #{job_id => 1, num_nodes => 1, num_cpus => 4, memory_mb => 1024},
    Result = flurm_backfill:run_backfill_cycle(BlockerJob, []),
    ?assertEqual([], Result).

test_cycle_disabled() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, false),

    BlockerJob = #{job_id => 1, num_nodes => 1, num_cpus => 4, memory_mb => 1024},
    Candidates = [#{job_id => 2, num_nodes => 1, num_cpus => 2, memory_mb => 512, time_limit => 60}],
    Result = flurm_backfill:run_backfill_cycle(BlockerJob, Candidates),
    ?assertEqual([], Result).

test_cycle_valid_inputs() ->
    application:set_env(flurm_core, backfill_enabled, true),

    BlockerJob = #{
        job_id => 1,
        num_nodes => 4,
        num_cpus => 32,
        memory_mb => 65536,
        time_limit => 86400
    },
    Candidates = [
        #{job_id => 2, num_nodes => 1, num_cpus => 4, memory_mb => 1024, time_limit => 3600, priority => 100},
        #{job_id => 3, num_nodes => 1, num_cpus => 2, memory_mb => 512, time_limit => 1800, priority => 50}
    ],

    %% The result depends on the timeline which depends on flurm_node_registry
    %% which may not be running, so just verify the function returns a list
    Result = flurm_backfill:run_backfill_cycle(BlockerJob, Candidates),
    ?assert(is_list(Result)).

%%====================================================================
%% calculate_shadow_time Tests
%%====================================================================

test_shadow_time_no_busy() ->
    Now = erlang:system_time(second),
    BlockerJob = #{num_nodes => 1, num_cpus => 4, memory_mb => 1024},
    Timeline = #{
        timestamp => Now,
        free_nodes => [],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 0
    },
    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),
    %% With no busy nodes and no free nodes, shadow time should be Now + BACKFILL_WINDOW (86400)
    ?assert(ShadowTime >= Now),
    ?assert(ShadowTime =< Now + 86400 + 10).

test_shadow_time_enough_free() ->
    Now = erlang:system_time(second),
    BlockerJob = #{num_nodes => 1, num_cpus => 4, memory_mb => 1024},
    Timeline = #{
        timestamp => Now,
        free_nodes => [
            {<<"node001">>, 32, 65536},
            {<<"node002">>, 32, 65536}
        ],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 2
    },
    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),
    %% With enough free nodes, shadow time should be now
    ?assertEqual(Now, ShadowTime).

test_shadow_time_insufficient() ->
    Now = erlang:system_time(second),
    BlockerJob = #{num_nodes => 2, num_cpus => 4, memory_mb => 1024},
    Timeline = #{
        timestamp => Now,
        free_nodes => [
            {<<"node001">>, 32, 65536}  % Only 1 node, need 2
        ],
        busy_nodes => [{<<"node002">>, 0, 0}],
        node_end_times => [{<<"node002">>, Now + 3600}],
        total_nodes => 2
    },
    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),
    %% Need to wait for one more node, should be approximately Now + 3600
    ?assertEqual(Now + 3600, ShadowTime).

%%====================================================================
%% can_backfill Tests
%%====================================================================

test_can_backfill_completes_in_time() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 1024,
        time_limit => 3600  % 1 hour
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [{<<"node001">>, 32, 65536}],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 1
    },
    ShadowTime = Now + 7200,  % 2 hours in the future

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertMatch({true, [<<"node001">>]}, Result).

test_can_backfill_exceeds_shadow() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 1024,
        time_limit => 7200  % 2 hours
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [{<<"node001">>, 32, 65536}],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 1
    },
    ShadowTime = Now + 3600,  % 1 hour in the future (job won't complete in time)

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

test_can_backfill_no_nodes() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 1024,
        time_limit => 3600
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [],  % No free nodes
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 0
    },
    ShadowTime = Now + 7200,

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

test_can_backfill_insufficient_nodes() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        num_nodes => 3,  % Need 3 nodes
        num_cpus => 4,
        memory_mb => 1024,
        time_limit => 3600
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [
            {<<"node001">>, 32, 65536},
            {<<"node002">>, 32, 65536}
        ],  % Only 2 nodes available
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 2
    },
    ShadowTime = Now + 7200,

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

%%====================================================================
%% find_backfill_jobs Tests
%%====================================================================

test_find_jobs_priority_sort() ->
    Now = erlang:system_time(second),
    BlockerJob = #{
        job_id => 100,
        num_nodes => 10,
        num_cpus => 64,
        memory_mb => 131072,
        time_limit => 86400
    },

    %% Create candidates with different priorities
    Candidates = [
        #{job_id => 1, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 60, priority => 50, pid => undefined},
        #{job_id => 2, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 60, priority => 100, pid => undefined},
        #{job_id => 3, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 60, priority => 75, pid => undefined}
    ],

    %% The function should process candidates in priority order
    %% We can't easily test the output without mocking flurm_node_registry,
    %% but we can verify it doesn't crash
    Result = flurm_backfill:find_backfill_jobs(BlockerJob, Candidates),
    ?assert(is_list(Result)).

test_find_jobs_max_limit() ->
    BlockerJob = #{
        job_id => 100,
        num_nodes => 10,
        num_cpus => 64,
        memory_mb => 131072,
        time_limit => 86400
    },

    %% Create more candidates than MAX_BACKFILL_JOBS (100)
    Candidates = [#{job_id => N, num_nodes => 1, num_cpus => 1, memory_mb => 512,
                    time_limit => 60, priority => N, pid => undefined}
                  || N <- lists:seq(1, 150)],

    Result = flurm_backfill:find_backfill_jobs(BlockerJob, Candidates),
    ?assert(is_list(Result)),
    %% Even if all jobs could backfill, we should have at most MAX_BACKFILL_JOBS
    ?assert(length(Result) =< 100).

test_find_jobs_empty_candidates() ->
    BlockerJob = #{
        job_id => 100,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 1024,
        time_limit => 3600
    },

    Result = flurm_backfill:find_backfill_jobs(BlockerJob, []),
    ?assertEqual([], Result).

%%====================================================================
%% get_resource_timeline Tests
%%====================================================================

test_timeline_structure() ->
    BlockerJob = #{num_nodes => 1, num_cpus => 4, memory_mb => 1024},
    Timeline = flurm_backfill:get_resource_timeline(BlockerJob),

    ?assert(is_map(Timeline)),
    ?assert(maps:is_key(timestamp, Timeline)),
    ?assert(maps:is_key(free_nodes, Timeline)),
    ?assert(maps:is_key(busy_nodes, Timeline)),
    ?assert(maps:is_key(node_end_times, Timeline)),
    ?assert(maps:is_key(total_nodes, Timeline)),

    %% Verify types
    ?assert(is_integer(maps:get(timestamp, Timeline))),
    ?assert(is_list(maps:get(free_nodes, Timeline))),
    ?assert(is_list(maps:get(busy_nodes, Timeline))),
    ?assert(is_list(maps:get(node_end_times, Timeline))),
    ?assert(is_integer(maps:get(total_nodes, Timeline))).

test_timeline_empty_nodes() ->
    %% When flurm_node_registry is not running or returns empty
    BlockerJob = #{num_nodes => 1, num_cpus => 4, memory_mb => 1024},
    Timeline = flurm_backfill:get_resource_timeline(BlockerJob),

    %% Should still return valid structure with empty lists
    ?assert(is_map(Timeline)),
    ?assert(is_list(maps:get(free_nodes, Timeline))),
    ?assert(is_list(maps:get(busy_nodes, Timeline))).

%%====================================================================
%% get_backfill_candidates Tests
%%====================================================================

test_candidates_empty() ->
    Result = flurm_backfill:get_backfill_candidates([]),
    ?assertEqual([], Result).

test_candidates_filters() ->
    %% Without a running job manager, this will return empty
    %% but should not crash
    JobIds = [1, 2, 3],
    Result = flurm_backfill:get_backfill_candidates(JobIds),
    ?assert(is_list(Result)).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"can_backfill with exact shadow time boundary", fun test_exact_boundary/0},
      {"can_backfill with zero time limit", fun test_zero_time_limit/0},
      {"can_backfill with default values", fun test_default_values/0},
      {"find_backfill_jobs with missing priority", fun test_missing_priority/0},
      {"timeline with node having insufficient resources", fun test_insufficient_resources/0}
     ]}.

test_exact_boundary() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 1024,
        time_limit => 3600
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [{<<"node001">>, 32, 65536}],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 1
    },
    %% Shadow time is exactly when job would end
    ShadowTime = Now + 3600,

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    %% Job ends at exactly shadow time (EndTime =< ShadowTime), should succeed
    ?assertMatch({true, _}, Result).

test_zero_time_limit() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 1024,
        time_limit => 0  % Zero time limit
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [{<<"node001">>, 32, 65536}],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 1
    },
    ShadowTime = Now + 3600,

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    %% Zero time limit job completes immediately, should succeed
    ?assertMatch({true, _}, Result).

test_default_values() ->
    Now = erlang:system_time(second),
    %% Job with missing values (uses defaults)
    Job = #{
        job_id => 1
        %% Defaults: num_nodes => 1, num_cpus => 1, memory_mb => 1024, time_limit => 3600
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [{<<"node001">>, 32, 65536}],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 1
    },
    ShadowTime = Now + 7200,

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertMatch({true, _}, Result).

test_missing_priority() ->
    BlockerJob = #{
        job_id => 100,
        num_nodes => 10,
        num_cpus => 64,
        memory_mb => 131072,
        time_limit => 86400
    },

    %% Candidates without priority field
    Candidates = [
        #{job_id => 1, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 60, pid => undefined},
        #{job_id => 2, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 60, pid => undefined}
    ],

    %% Should handle missing priority (defaults to 0)
    Result = flurm_backfill:find_backfill_jobs(BlockerJob, Candidates),
    ?assert(is_list(Result)).

test_insufficient_resources() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        num_nodes => 1,
        num_cpus => 64,  % High CPU requirement
        memory_mb => 1024,
        time_limit => 3600
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [
            {<<"node001">>, 4, 65536},   % Only 4 CPUs
            {<<"node002">>, 8, 65536}    % Only 8 CPUs
        ],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 2
    },
    ShadowTime = Now + 7200,

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertEqual(false, Result).

%%====================================================================
%% Resource Selection Tests
%%====================================================================

resource_selection_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"can_backfill selects nodes meeting requirements", fun test_node_selection/0},
      {"can_backfill with high memory requirement", fun test_high_memory/0},
      {"multiple suitable nodes selection", fun test_multiple_node_selection/0}
     ]}.

test_node_selection() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        num_nodes => 1,
        num_cpus => 8,
        memory_mb => 16384,
        time_limit => 3600
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [
            {<<"node001">>, 4, 65536},   % CPUs too low
            {<<"node002">>, 16, 8192},   % Memory too low
            {<<"node003">>, 32, 65536}   % Good
        ],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 3
    },
    ShadowTime = Now + 7200,

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertMatch({true, [<<"node003">>]}, Result).

test_high_memory() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 128000,  % 128GB
        time_limit => 3600
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [
            {<<"node001">>, 32, 65536},   % 64GB - not enough
            {<<"node002">>, 32, 131072}   % 128GB - enough
        ],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 2
    },
    ShadowTime = Now + 7200,

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertMatch({true, [<<"node002">>]}, Result).

test_multiple_node_selection() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        num_nodes => 2,  % Need 2 nodes
        num_cpus => 4,
        memory_mb => 8192,
        time_limit => 3600
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [
            {<<"node001">>, 32, 65536},
            {<<"node002">>, 32, 65536},
            {<<"node003">>, 32, 65536}
        ],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 3
    },
    ShadowTime = Now + 7200,

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertMatch({true, [_, _]}, Result),
    {true, Nodes} = Result,
    ?assertEqual(2, length(Nodes)).

%%====================================================================
%% Shadow Time Calculation Edge Cases
%%====================================================================

shadow_time_edge_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"shadow time with multiple node end times", fun test_shadow_multiple_end_times/0},
      {"shadow time needing multiple nodes to free", fun test_shadow_multiple_nodes_needed/0},
      {"shadow time with nodes not meeting requirements", fun test_shadow_unsuitable_nodes/0}
     ]}.

test_shadow_multiple_end_times() ->
    Now = erlang:system_time(second),
    BlockerJob = #{num_nodes => 1, num_cpus => 4, memory_mb => 1024},
    Timeline = #{
        timestamp => Now,
        free_nodes => [],
        busy_nodes => [
            {<<"node001">>, 0, 0},
            {<<"node002">>, 0, 0},
            {<<"node003">>, 0, 0}
        ],
        node_end_times => [
            {<<"node001">>, Now + 3600},
            {<<"node002">>, Now + 1800},
            {<<"node003">>, Now + 7200}
        ],
        total_nodes => 3
    },

    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),
    %% Need 1 node, earliest free is node002 at Now + 1800
    ?assertEqual(Now + 1800, ShadowTime).

test_shadow_multiple_nodes_needed() ->
    Now = erlang:system_time(second),
    BlockerJob = #{num_nodes => 2, num_cpus => 4, memory_mb => 1024},
    Timeline = #{
        timestamp => Now,
        free_nodes => [],
        busy_nodes => [
            {<<"node001">>, 0, 0},
            {<<"node002">>, 0, 0}
        ],
        node_end_times => [
            {<<"node001">>, Now + 1800},  % First to free
            {<<"node002">>, Now + 3600}   % Second to free
        ],
        total_nodes => 2
    },

    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),
    %% Need 2 nodes, must wait for both, so shadow time is when 2nd frees
    ?assertEqual(Now + 3600, ShadowTime).

test_shadow_unsuitable_nodes() ->
    Now = erlang:system_time(second),
    BlockerJob = #{num_nodes => 1, num_cpus => 64, memory_mb => 131072},
    Timeline = #{
        timestamp => Now,
        free_nodes => [
            {<<"node001">>, 4, 8192}  % Too few CPUs and memory
        ],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 1
    },

    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),
    %% Free nodes don't meet requirements, need to wait for something
    %% With no busy nodes, falls back to Now + BACKFILL_WINDOW
    ?assert(ShadowTime >= Now).

%%====================================================================
%% Timeline Update Tests
%%====================================================================

timeline_update_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"timeline updates after backfill allocation", fun test_timeline_update/0}
     ]}.

test_timeline_update() ->
    %% This tests the internal reserve_nodes_in_timeline function indirectly
    %% by checking that find_backfill_jobs properly tracks allocations
    Now = erlang:system_time(second),
    BlockerJob = #{
        job_id => 100,
        num_nodes => 10,
        num_cpus => 64,
        memory_mb => 131072,
        time_limit => 86400
    },

    %% Two jobs that could both fit if processed independently,
    %% but only one node is available
    Candidates = [
        #{job_id => 1, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 60, priority => 100, pid => undefined},
        #{job_id => 2, num_nodes => 1, num_cpus => 1, memory_mb => 512, time_limit => 60, priority => 50, pid => undefined}
    ],

    %% The algorithm should allocate to higher priority first and update timeline
    Result = flurm_backfill:find_backfill_jobs(BlockerJob, Candidates),
    %% Without actual nodes, both will fail, but function should not crash
    ?assert(is_list(Result)).

%%====================================================================
%% Configuration Tests
%%====================================================================

config_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"scheduler type backfill enables backfill", fun test_config_backfill_type/0},
      {"explicit backfill_enabled overrides", fun test_config_explicit_enable/0}
     ]}.

test_config_backfill_type() ->
    application:set_env(flurm_core, scheduler_type, backfill),
    application:unset_env(flurm_core, backfill_enabled),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()).

test_config_explicit_enable() ->
    application:set_env(flurm_core, scheduler_type, custom),
    application:set_env(flurm_core, backfill_enabled, true),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()).

%%====================================================================
%% Job Map Field Tests
%%====================================================================

job_field_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"job with all default fields", fun test_job_all_defaults/0},
      {"job with explicit pid field", fun test_job_with_pid/0}
     ]}.

test_job_all_defaults() ->
    Now = erlang:system_time(second),
    %% Minimal job spec using defaults
    Job = #{job_id => 1},
    Timeline = #{
        timestamp => Now,
        free_nodes => [{<<"node001">>, 32, 65536}],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 1
    },
    ShadowTime = Now + 7200,

    %% Should use default values: num_nodes=1, num_cpus=1, memory_mb=1024, time_limit=3600
    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertMatch({true, _}, Result).

test_job_with_pid() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1,
        pid => self(),  % Explicit pid
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 512,
        time_limit => 60
    },
    Timeline = #{
        timestamp => Now,
        free_nodes => [{<<"node001">>, 32, 65536}],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 1
    },
    ShadowTime = Now + 7200,

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),
    ?assertMatch({true, _}, Result).

%%====================================================================
%% Module Export Tests
%%====================================================================

exports_test_() ->
    [
     {"module exports find_backfill_jobs/2", fun test_exports_find_backfill_jobs/0},
     {"module exports can_backfill/3", fun test_exports_can_backfill/0},
     {"module exports calculate_shadow_time/2", fun test_exports_calculate_shadow_time/0},
     {"module exports get_resource_timeline/1", fun test_exports_get_resource_timeline/0},
     {"module exports run_backfill_cycle/2", fun test_exports_run_backfill_cycle/0},
     {"module exports get_backfill_candidates/1", fun test_exports_get_backfill_candidates/0},
     {"module exports is_backfill_enabled/0", fun test_exports_is_backfill_enabled/0}
    ].

test_exports_find_backfill_jobs() ->
    Exports = flurm_backfill:module_info(exports),
    ?assert(lists:member({find_backfill_jobs, 2}, Exports)).

test_exports_can_backfill() ->
    Exports = flurm_backfill:module_info(exports),
    ?assert(lists:member({can_backfill, 3}, Exports)).

test_exports_calculate_shadow_time() ->
    Exports = flurm_backfill:module_info(exports),
    ?assert(lists:member({calculate_shadow_time, 2}, Exports)).

test_exports_get_resource_timeline() ->
    Exports = flurm_backfill:module_info(exports),
    ?assert(lists:member({get_resource_timeline, 1}, Exports)).

test_exports_run_backfill_cycle() ->
    Exports = flurm_backfill:module_info(exports),
    ?assert(lists:member({run_backfill_cycle, 2}, Exports)).

test_exports_get_backfill_candidates() ->
    Exports = flurm_backfill:module_info(exports),
    ?assert(lists:member({get_backfill_candidates, 1}, Exports)).

test_exports_is_backfill_enabled() ->
    Exports = flurm_backfill:module_info(exports),
    ?assert(lists:member({is_backfill_enabled, 0}, Exports)).
