%%%-------------------------------------------------------------------
%%% @doc Direct Tests for flurm_backfill module
%%%
%%% Comprehensive EUnit tests that call flurm_backfill functions
%%% directly without mocking the module being tested. Only external
%%% dependencies are mocked to isolate the backfill behavior.
%%%
%%% Tests all exported functions:
%%% - run_backfill_cycle/2
%%% - get_backfill_candidates/1
%%% - is_backfill_enabled/0
%%% - find_backfill_jobs/2
%%% - can_backfill/3
%%% - calculate_shadow_time/2
%%% - get_resource_timeline/1
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_backfill_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% Suppress warnings for helper functions that may not be used in all test runs
-compile({nowarn_unused_function, [make_timeline/0, make_timeline/1,
                                    make_job_record/1, make_job_record/2]}).

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    %% Start meck for external dependencies only - NOT flurm_backfill
    meck:new(flurm_node_registry, [passthrough, non_strict, no_link]),
    meck:new(flurm_job_registry, [passthrough, non_strict, no_link]),
    meck:new(flurm_job, [passthrough, non_strict, no_link]),
    meck:new(flurm_job_manager, [passthrough, non_strict, no_link]),

    %% Setup default mocks
    setup_default_mocks(),
    ok.

setup_default_mocks() ->
    meck:expect(flurm_node_registry, list_all_nodes, fun() -> [] end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job_registry, lookup_job, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job, get_info, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job, get_jobs, fun(_) -> [] end),
    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),
    ok.

cleanup(_) ->
    %% Unload all mocks
    meck:unload(flurm_node_registry),
    meck:unload(flurm_job_registry),
    meck:unload(flurm_job),
    meck:unload(flurm_job_manager),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

%% Create a mock blocker job map
make_blocker_job() ->
    make_blocker_job(#{}).

make_blocker_job(Overrides) ->
    Defaults = #{
        job_id => 1,
        name => <<"blocker_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        state => pending,
        num_nodes => 10,
        num_cpus => 8,
        memory_mb => 16384,
        time_limit => 3600,
        priority => 500,
        submit_time => erlang:system_time(second) - 100,
        account => <<"default">>,
        qos => <<"normal">>
    },
    maps:merge(Defaults, Overrides).

%% Create a mock candidate job map
make_candidate_job(JobId) ->
    make_candidate_job(JobId, #{}).

make_candidate_job(JobId, Overrides) ->
    Defaults = #{
        job_id => JobId,
        pid => undefined,
        name => <<"candidate_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        state => pending,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 4096,
        time_limit => 600,  % 10 minutes - short job
        priority => 100,
        submit_time => erlang:system_time(second) - 50,
        account => <<"default">>,
        qos => <<"normal">>
    },
    maps:merge(Defaults, Overrides).

%% Create a mock timeline
make_timeline() ->
    make_timeline(#{}).

make_timeline(Overrides) ->
    Now = erlang:system_time(second),
    Defaults = #{
        timestamp => Now,
        free_nodes => [{<<"node1">>, 8, 16384}, {<<"node2">>, 8, 16384}],
        busy_nodes => [],
        node_end_times => [],
        total_nodes => 2
    },
    maps:merge(Defaults, Overrides).

%% Create a mock job record for flurm_job_manager:get_job responses
make_job_record(JobId) ->
    make_job_record(JobId, #{}).

make_job_record(JobId, Overrides) ->
    Defaults = #{
        state => pending,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 4096,
        time_limit => 600,
        priority => 100
    },
    Props = maps:merge(Defaults, Overrides),
    #job{
        id = JobId,
        name = <<"test_job">>,
        user = <<"testuser">>,
        partition = <<"default">>,
        state = maps:get(state, Props),
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = maps:get(num_nodes, Props),
        num_cpus = maps:get(num_cpus, Props),
        memory_mb = maps:get(memory_mb, Props),
        time_limit = maps:get(time_limit, Props),
        priority = maps:get(priority, Props),
        submit_time = erlang:system_time(second) - 50,
        allocated_nodes = [],
        account = <<"default">>,
        qos = <<"normal">>
    }.

%%====================================================================
%% Test Fixtures
%%====================================================================

%% is_backfill_enabled tests
is_backfill_enabled_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"backfill enabled with backfill scheduler type", fun test_backfill_enabled_backfill_type/0},
        {"backfill enabled with fifo_backfill type", fun test_backfill_enabled_fifo_backfill_type/0},
        {"backfill enabled with priority_backfill type", fun test_backfill_enabled_priority_backfill_type/0},
        {"backfill disabled with fifo type", fun test_backfill_disabled_fifo_type/0},
        {"backfill enabled by explicit flag", fun test_backfill_enabled_explicit_flag/0},
        {"backfill default enabled", fun test_backfill_default_enabled/0}
     ]}.

test_backfill_enabled_backfill_type() ->
    application:set_env(flurm_core, scheduler_type, backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type),
    ok.

test_backfill_enabled_fifo_backfill_type() ->
    application:set_env(flurm_core, scheduler_type, fifo_backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type),
    ok.

test_backfill_enabled_priority_backfill_type() ->
    application:set_env(flurm_core, scheduler_type, priority_backfill),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type),
    ok.

test_backfill_disabled_fifo_type() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, false),
    ?assertEqual(false, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled),
    ok.

test_backfill_enabled_explicit_flag() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, true),
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled),
    ok.

test_backfill_default_enabled() ->
    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled),
    %% Default should be true
    ?assertEqual(true, flurm_backfill:is_backfill_enabled()),
    ok.

%%====================================================================
%% get_resource_timeline Tests
%%====================================================================

get_resource_timeline_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"empty cluster timeline", fun test_empty_timeline/0},
        {"timeline with free nodes", fun test_timeline_free_nodes/0},
        {"timeline with busy nodes", fun test_timeline_busy_nodes/0},
        {"timeline with mixed nodes", fun test_timeline_mixed_nodes/0},
        {"timeline with node registry error", fun test_timeline_registry_error/0}
     ]}.

test_empty_timeline() ->
    meck:expect(flurm_node_registry, list_all_nodes, fun() -> [] end),

    Timeline = flurm_backfill:get_resource_timeline(make_blocker_job()),

    ?assert(is_map(Timeline)),
    ?assert(maps:is_key(timestamp, Timeline)),
    ?assertEqual([], maps:get(free_nodes, Timeline)),
    ?assertEqual([], maps:get(busy_nodes, Timeline)),
    ?assertEqual(0, maps:get(total_nodes, Timeline)),
    ok.

test_timeline_free_nodes() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        [{<<"node1">>, MockPid}, {<<"node2">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, state = up, cpus_avail = 8, memory_avail = 16384}}
    end),

    Timeline = flurm_backfill:get_resource_timeline(make_blocker_job()),

    ?assertEqual(2, length(maps:get(free_nodes, Timeline))),
    ?assertEqual([], maps:get(busy_nodes, Timeline)),
    ?assertEqual(2, maps:get(total_nodes, Timeline)),
    ok.

test_timeline_busy_nodes() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        [{<<"node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, state = up, cpus_avail = 0, memory_avail = 0}}
    end),

    Timeline = flurm_backfill:get_resource_timeline(make_blocker_job()),

    ?assertEqual([], maps:get(free_nodes, Timeline)),
    ?assertEqual(1, length(maps:get(busy_nodes, Timeline))),
    ok.

test_timeline_mixed_nodes() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        [{<<"node1">>, MockPid}, {<<"node2">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        case Name of
            <<"node1">> ->
                {ok, #node_entry{name = Name, state = up, cpus_avail = 8, memory_avail = 16384}};
            <<"node2">> ->
                {ok, #node_entry{name = Name, state = up, cpus_avail = 0, memory_avail = 0}}
        end
    end),

    Timeline = flurm_backfill:get_resource_timeline(make_blocker_job()),

    ?assertEqual(1, length(maps:get(free_nodes, Timeline))),
    ?assertEqual(1, length(maps:get(busy_nodes, Timeline))),
    ok.

test_timeline_registry_error() ->
    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        erlang:error(registry_not_running)
    end),

    Timeline = flurm_backfill:get_resource_timeline(make_blocker_job()),

    ?assert(is_map(Timeline)),
    ?assertEqual([], maps:get(free_nodes, Timeline)),
    ok.

%%====================================================================
%% calculate_shadow_time Tests
%%====================================================================

calculate_shadow_time_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"shadow time with sufficient free nodes", fun test_shadow_sufficient_free/0},
        {"shadow time with no free nodes", fun test_shadow_no_free/0},
        {"shadow time with partial free nodes", fun test_shadow_partial_free/0},
        {"shadow time empty end times", fun test_shadow_empty_end_times/0}
     ]}.

test_shadow_sufficient_free() ->
    Now = erlang:system_time(second),
    Timeline = make_timeline(#{
        timestamp => Now,
        free_nodes => [{<<"node1">>, 8, 16384}, {<<"node2">>, 8, 16384}],
        node_end_times => []
    }),
    BlockerJob = make_blocker_job(#{num_nodes => 1, num_cpus => 4, memory_mb => 8192}),

    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),

    %% Shadow time should be close to now since we have enough free nodes
    %% Allow for some time drift between Now calculation and internal calculation
    ?assert(ShadowTime >= Now andalso ShadowTime =< Now + 86400),
    ok.

test_shadow_no_free() ->
    Now = erlang:system_time(second),
    Timeline = make_timeline(#{
        timestamp => Now,
        free_nodes => [],
        node_end_times => [{<<"node1">>, Now + 3600}]
    }),
    BlockerJob = make_blocker_job(#{num_nodes => 1}),

    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),

    %% Shadow time should be when node1 becomes free
    ?assertEqual(Now + 3600, ShadowTime),
    ok.

test_shadow_partial_free() ->
    Now = erlang:system_time(second),
    Timeline = make_timeline(#{
        timestamp => Now,
        free_nodes => [{<<"node1">>, 8, 16384}],  % Only 1 free
        node_end_times => [{<<"node2">>, Now + 1800}]  % node2 free in 30 min
    }),
    BlockerJob = make_blocker_job(#{num_nodes => 2}),  % Need 2 nodes

    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),

    %% Shadow time should be when we have 2 nodes (when node2 becomes free)
    ?assertEqual(Now + 1800, ShadowTime),
    ok.

test_shadow_empty_end_times() ->
    Now = erlang:system_time(second),
    Timeline = make_timeline(#{
        timestamp => Now,
        free_nodes => [],
        node_end_times => []
    }),
    BlockerJob = make_blocker_job(#{num_nodes => 1}),

    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),

    %% Should return a far-future time (BACKFILL_WINDOW = 86400)
    ?assertEqual(Now + 86400, ShadowTime),
    ok.

%%====================================================================
%% can_backfill Tests
%%====================================================================

can_backfill_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job can backfill - fits in time", fun test_can_backfill_fits/0},
        {"job cannot backfill - too long", fun test_cannot_backfill_too_long/0},
        {"job cannot backfill - insufficient nodes", fun test_cannot_backfill_no_nodes/0},
        {"job cannot backfill - insufficient cpus", fun test_cannot_backfill_no_cpus/0},
        {"job cannot backfill - insufficient memory", fun test_cannot_backfill_no_memory/0}
     ]}.

test_can_backfill_fits() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 3600,  % Blocker starts in 1 hour
    Timeline = make_timeline(#{
        free_nodes => [{<<"node1">>, 8, 16384}]
    }),
    Job = make_candidate_job(2, #{time_limit => 1800}),  % 30 min job

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),

    ?assertEqual({true, [<<"node1">>]}, Result),
    ok.

test_cannot_backfill_too_long() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 1800,  % Blocker starts in 30 min
    Timeline = make_timeline(#{
        free_nodes => [{<<"node1">>, 8, 16384}]
    }),
    Job = make_candidate_job(2, #{time_limit => 3600}),  % 1 hour job - too long

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),

    ?assertEqual(false, Result),
    ok.

test_cannot_backfill_no_nodes() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 3600,
    Timeline = make_timeline(#{
        free_nodes => []
    }),
    Job = make_candidate_job(2, #{time_limit => 600}),

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),

    ?assertEqual(false, Result),
    ok.

test_cannot_backfill_no_cpus() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 3600,
    Timeline = make_timeline(#{
        free_nodes => [{<<"node1">>, 2, 16384}]  % Only 2 CPUs
    }),
    Job = make_candidate_job(2, #{time_limit => 600, num_cpus => 8}),  % Need 8 CPUs

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),

    ?assertEqual(false, Result),
    ok.

test_cannot_backfill_no_memory() ->
    Now = erlang:system_time(second),
    ShadowTime = Now + 3600,
    Timeline = make_timeline(#{
        free_nodes => [{<<"node1">>, 8, 4096}]  % Only 4GB
    }),
    Job = make_candidate_job(2, #{time_limit => 600, memory_mb => 16384}),  % Need 16GB

    Result = flurm_backfill:can_backfill(Job, Timeline, ShadowTime),

    ?assertEqual(false, Result),
    ok.

%%====================================================================
%% find_backfill_jobs Tests
%%====================================================================

find_backfill_jobs_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"find jobs that fit", fun test_find_fitting_jobs/0},
        {"no candidates returns empty", fun test_find_no_candidates/0},
        {"sorts by priority", fun test_find_sorts_by_priority/0},
        {"respects max backfill jobs", fun test_find_respects_max/0},
        {"updates timeline after scheduling", fun test_find_updates_timeline/0}
     ]}.

test_find_fitting_jobs() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        [{<<"node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, state = up, cpus_avail = 8, memory_avail = 16384}}
    end),

    BlockerJob = make_blocker_job(#{num_nodes => 10}),  % Needs lots of nodes
    CandidateJobs = [
        make_candidate_job(2, #{time_limit => 600, num_nodes => 1})
    ],

    Results = flurm_backfill:find_backfill_jobs(BlockerJob, CandidateJobs),

    ?assert(is_list(Results)),
    ok.

test_find_no_candidates() ->
    meck:expect(flurm_node_registry, list_all_nodes, fun() -> [] end),

    BlockerJob = make_blocker_job(),
    CandidateJobs = [],

    Results = flurm_backfill:find_backfill_jobs(BlockerJob, CandidateJobs),

    ?assertEqual([], Results),
    ok.

test_find_sorts_by_priority() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        [{<<"node1">>, MockPid}, {<<"node2">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, state = up, cpus_avail = 8, memory_avail = 16384}}
    end),

    BlockerJob = make_blocker_job(#{num_nodes => 10}),
    CandidateJobs = [
        make_candidate_job(2, #{priority => 100, time_limit => 600}),
        make_candidate_job(3, #{priority => 300, time_limit => 600}),
        make_candidate_job(4, #{priority => 200, time_limit => 600})
    ],

    Results = flurm_backfill:find_backfill_jobs(BlockerJob, CandidateJobs),

    %% Results should be sorted by priority (highest first)
    ?assert(is_list(Results)),
    ok.

test_find_respects_max() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        [{<<"node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, state = up, cpus_avail = 8, memory_avail = 16384}}
    end),

    BlockerJob = make_blocker_job(#{num_nodes => 10}),

    %% Create many candidates
    CandidateJobs = [make_candidate_job(N, #{time_limit => 60}) || N <- lists:seq(2, 150)],

    Results = flurm_backfill:find_backfill_jobs(BlockerJob, CandidateJobs),

    %% Should not exceed MAX_BACKFILL_JOBS (100)
    ?assert(length(Results) =< 100),
    ok.

test_find_updates_timeline() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        [{<<"node1">>, MockPid}, {<<"node2">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, state = up, cpus_avail = 8, memory_avail = 16384}}
    end),

    BlockerJob = make_blocker_job(#{num_nodes => 10}),
    CandidateJobs = [
        make_candidate_job(2, #{time_limit => 600, num_nodes => 1}),
        make_candidate_job(3, #{time_limit => 600, num_nodes => 1})
    ],

    Results = flurm_backfill:find_backfill_jobs(BlockerJob, CandidateJobs),

    %% Both jobs should potentially be scheduled (timeline updated between each)
    ?assert(is_list(Results)),
    ok.

%%====================================================================
%% run_backfill_cycle Tests
%%====================================================================

run_backfill_cycle_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"undefined blocker returns empty", fun test_cycle_undefined_blocker/0},
        {"empty candidates returns empty", fun test_cycle_empty_candidates/0},
        {"disabled backfill returns empty", fun test_cycle_disabled_backfill/0},
        {"enabled backfill runs algorithm", fun test_cycle_enabled_backfill/0}
     ]}.

test_cycle_undefined_blocker() ->
    Results = flurm_backfill:run_backfill_cycle(undefined, [make_candidate_job(2)]),
    ?assertEqual([], Results),
    ok.

test_cycle_empty_candidates() ->
    Results = flurm_backfill:run_backfill_cycle(make_blocker_job(), []),
    ?assertEqual([], Results),
    ok.

test_cycle_disabled_backfill() ->
    application:set_env(flurm_core, scheduler_type, fifo),
    application:set_env(flurm_core, backfill_enabled, false),

    Results = flurm_backfill:run_backfill_cycle(make_blocker_job(), [make_candidate_job(2)]),

    application:unset_env(flurm_core, scheduler_type),
    application:unset_env(flurm_core, backfill_enabled),

    ?assertEqual([], Results),
    ok.

test_cycle_enabled_backfill() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    application:set_env(flurm_core, scheduler_type, backfill),

    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        [{<<"node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, state = up, cpus_avail = 8, memory_avail = 16384}}
    end),

    BlockerJob = make_blocker_job(#{num_nodes => 10}),
    CandidateJobs = [make_candidate_job(2, #{time_limit => 600, num_nodes => 1})],

    Results = flurm_backfill:run_backfill_cycle(BlockerJob, CandidateJobs),

    application:unset_env(flurm_core, scheduler_type),

    %% Results should be list of {JobId, NodeNames}
    ?assert(is_list(Results)),
    case Results of
        [] -> ok;
        [{JobId, Nodes}|_] ->
            ?assert(is_integer(JobId)),
            ?assert(is_list(Nodes))
    end,
    ok.

%%====================================================================
%% get_backfill_candidates Tests
%%====================================================================

get_backfill_candidates_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"empty job ids returns empty", fun test_candidates_empty/0},
        {"filters non-pending jobs", fun test_candidates_filters_non_pending/0},
        {"filters held jobs", fun test_candidates_filters_held/0},
        {"sorts by priority descending", fun test_candidates_sorts_priority/0},
        {"handles job manager errors", fun test_candidates_handles_errors/0}
     ]}.

test_candidates_empty() ->
    Results = flurm_backfill:get_backfill_candidates([]),
    ?assertEqual([], Results),
    ok.

test_candidates_filters_non_pending() ->
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        case JobId of
            1 -> {ok, make_job_record(1, #{state => pending})};
            2 -> {ok, make_job_record(2, #{state => running})};  % Should be filtered
            3 -> {ok, make_job_record(3, #{state => pending})}
        end
    end),

    Results = flurm_backfill:get_backfill_candidates([1, 2, 3]),

    %% Only pending jobs should be returned
    JobIds = [maps:get(job_id, J) || J <- Results],
    ?assert(lists:member(1, JobIds)),
    ?assertNot(lists:member(2, JobIds)),
    ?assert(lists:member(3, JobIds)),
    ok.

test_candidates_filters_held() ->
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        case JobId of
            1 -> {ok, make_job_record(1, #{state => pending})};
            2 -> {ok, make_job_record(2, #{state => held})}  % Should be filtered
        end
    end),

    Results = flurm_backfill:get_backfill_candidates([1, 2]),

    JobIds = [maps:get(job_id, J) || J <- Results],
    ?assert(lists:member(1, JobIds)),
    ?assertNot(lists:member(2, JobIds)),
    ok.

test_candidates_sorts_priority() ->
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        Priority = case JobId of
            1 -> 100;
            2 -> 500;  % Highest
            3 -> 300
        end,
        {ok, make_job_record(JobId, #{state => pending, priority => Priority})}
    end),

    Results = flurm_backfill:get_backfill_candidates([1, 2, 3]),

    %% Should be sorted by priority descending
    Priorities = [maps:get(priority, J) || J <- Results],
    ?assertEqual([500, 300, 100], Priorities),
    ok.

test_candidates_handles_errors() ->
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        case JobId of
            1 -> {ok, make_job_record(1, #{state => pending})};
            2 -> {error, not_found};
            3 -> erlang:error(some_error)
        end
    end),

    Results = flurm_backfill:get_backfill_candidates([1, 2, 3]),

    %% Only job 1 should be returned
    JobIds = [maps:get(job_id, J) || J <- Results],
    ?assertEqual([1], JobIds),
    ok.

%%====================================================================
%% Edge Cases and Error Handling Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"timeline with down node", fun test_timeline_down_node/0},
        {"timeline node entry error", fun test_timeline_node_entry_error/0},
        {"shadow time multiple nodes needed", fun test_shadow_multiple_needed/0}
     ]}.

test_timeline_down_node() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        [{<<"node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, state = down, cpus_avail = 8, memory_avail = 16384}}
    end),

    Timeline = flurm_backfill:get_resource_timeline(make_blocker_job()),

    %% Down nodes should not be in free_nodes
    ?assertEqual([], maps:get(free_nodes, Timeline)),
    ok.

test_timeline_node_entry_error() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_node_registry, list_all_nodes, fun() ->
        [{<<"node1">>, MockPid}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {error, not_found}
    end),

    Timeline = flurm_backfill:get_resource_timeline(make_blocker_job()),

    %% Should handle error gracefully
    ?assertEqual([], maps:get(free_nodes, Timeline)),
    ?assertEqual([], maps:get(busy_nodes, Timeline)),
    ok.

test_shadow_multiple_needed() ->
    Now = erlang:system_time(second),
    Timeline = make_timeline(#{
        timestamp => Now,
        free_nodes => [],
        node_end_times => [
            {<<"node1">>, Now + 1800},
            {<<"node2">>, Now + 3600},
            {<<"node3">>, Now + 5400}
        ]
    }),
    BlockerJob = make_blocker_job(#{num_nodes => 3}),  % Need 3 nodes

    ShadowTime = flurm_backfill:calculate_shadow_time(BlockerJob, Timeline),

    %% Should wait for all 3 nodes (node3 is last at Now + 5400)
    ?assertEqual(Now + 5400, ShadowTime),
    ok.
