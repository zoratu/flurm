%%%-------------------------------------------------------------------
%%% @doc Direct Tests for flurm_priority module
%%%
%%% Comprehensive EUnit tests that call flurm_priority functions
%%% directly without mocking the module being tested. Only external
%%% dependencies are mocked to isolate the priority calculation.
%%%
%%% Tests all exported functions:
%%% - calculate_priority/1
%%% - calculate_priority/2
%%% - recalculate_all/0
%%% - get_priority_factors/1
%%% - set_weights/1
%%% - get_weights/0
%%% - age_factor/2 (internal export)
%%% - size_factor/2 (internal export)
%%% - partition_factor/1 (internal export)
%%% - nice_factor/1 (internal export)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_priority_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    %% Start meck for external dependencies only - NOT flurm_priority
    meck:new(flurm_fairshare, [passthrough, non_strict, no_link]),
    meck:new(flurm_partition_registry, [passthrough, non_strict, no_link]),
    meck:new(flurm_node_registry, [passthrough, non_strict, no_link]),
    meck:new(flurm_job_registry, [passthrough, non_strict, no_link]),
    meck:new(flurm_job, [passthrough, non_strict, no_link]),

    %% Setup default mocks
    setup_default_mocks(),

    %% Clean up any existing weights table
    case ets:whereis(flurm_priority_weights) of
        undefined -> ok;
        _ -> ets:delete(flurm_priority_weights)
    end,
    ok.

setup_default_mocks() ->
    meck:expect(flurm_fairshare, get_priority_factor, fun(_, _) -> 0.5 end),
    meck:expect(flurm_partition_registry, get_partition_priority, fun(_) -> {ok, 5000} end),
    meck:expect(flurm_node_registry, count_nodes, fun() -> 100 end),
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> [] end),
    meck:expect(flurm_job, get_info, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job, set_priority, fun(_, _) -> ok end),
    ok.

cleanup(_) ->
    %% Clean up weights table
    case ets:whereis(flurm_priority_weights) of
        undefined -> ok;
        _ -> ets:delete(flurm_priority_weights)
    end,

    %% Unload all mocks
    meck:unload(flurm_fairshare),
    meck:unload(flurm_partition_registry),
    meck:unload(flurm_node_registry),
    meck:unload(flurm_job_registry),
    meck:unload(flurm_job),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

%% Create a mock job info map
make_job_info() ->
    make_job_info(#{}).

make_job_info(Overrides) ->
    Now = erlang:system_time(second),
    Defaults = #{
        job_id => 1,
        name => <<"test_job">>,
        user => <<"testuser">>,
        account => <<"default">>,
        partition => <<"default">>,
        state => pending,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 4096,
        time_limit => 3600,
        priority => 100,
        submit_time => Now - 100,  % Submitted 100 seconds ago
        nice => 0,
        qos => <<"normal">>
    },
    maps:merge(Defaults, Overrides).

%%====================================================================
%% Weights Management Tests
%%====================================================================

weights_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_weights returns defaults initially", fun test_get_weights_defaults/0},
        {"set_weights updates weights", fun test_set_weights/0},
        {"set_weights partial update", fun test_set_weights_partial/0},
        {"weights table created on first access", fun test_weights_table_created/0}
     ]}.

test_get_weights_defaults() ->
    Weights = flurm_priority:get_weights(),

    ?assert(is_map(Weights)),
    ?assert(maps:is_key(age, Weights)),
    ?assert(maps:is_key(job_size, Weights)),
    ?assert(maps:is_key(partition, Weights)),
    ?assert(maps:is_key(qos, Weights)),
    ?assert(maps:is_key(nice, Weights)),
    ?assert(maps:is_key(fairshare, Weights)),

    %% Check default values
    ?assertEqual(1000, maps:get(age, Weights)),
    ?assertEqual(200, maps:get(job_size, Weights)),
    ?assertEqual(100, maps:get(partition, Weights)),
    ?assertEqual(500, maps:get(qos, Weights)),
    ?assertEqual(100, maps:get(nice, Weights)),
    ?assertEqual(5000, maps:get(fairshare, Weights)),
    ok.

test_set_weights() ->
    NewWeights = #{
        age => 2000,
        job_size => 400,
        partition => 200,
        qos => 1000,
        nice => 200,
        fairshare => 3000
    },

    ok = flurm_priority:set_weights(NewWeights),
    Weights = flurm_priority:get_weights(),

    ?assertEqual(2000, maps:get(age, Weights)),
    ?assertEqual(400, maps:get(job_size, Weights)),
    ?assertEqual(200, maps:get(partition, Weights)),
    ?assertEqual(1000, maps:get(qos, Weights)),
    ?assertEqual(200, maps:get(nice, Weights)),
    ?assertEqual(3000, maps:get(fairshare, Weights)),
    ok.

test_set_weights_partial() ->
    %% Set partial weights - should replace all
    NewWeights = #{
        age => 1500
    },

    ok = flurm_priority:set_weights(NewWeights),
    Weights = flurm_priority:get_weights(),

    ?assertEqual(1500, maps:get(age, Weights)),
    ok.

test_weights_table_created() ->
    %% Table should not exist initially (cleaned in setup)
    ?assertEqual(undefined, ets:whereis(flurm_priority_weights)),

    %% Accessing weights should create the table
    _ = flurm_priority:get_weights(),

    ?assertNotEqual(undefined, ets:whereis(flurm_priority_weights)),
    ok.

%%====================================================================
%% Age Factor Tests
%%====================================================================

age_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"age factor zero for new job", fun test_age_factor_zero/0},
        {"age factor increases with time", fun test_age_factor_increases/0},
        {"age factor capped at max", fun test_age_factor_capped/0},
        {"age factor handles tuple timestamp", fun test_age_factor_tuple/0},
        {"age factor handles integer timestamp", fun test_age_factor_integer/0},
        {"age factor handles invalid timestamp", fun test_age_factor_invalid/0}
     ]}.

test_age_factor_zero() ->
    Now = erlang:system_time(second),
    JobInfo = make_job_info(#{submit_time => Now}),

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% Should be close to 0 for brand new job
    ?assert(Factor < 0.001),
    ok.

test_age_factor_increases() ->
    Now = erlang:system_time(second),
    JobInfo1 = make_job_info(#{submit_time => Now - 3600}),   % 1 hour old
    JobInfo2 = make_job_info(#{submit_time => Now - 86400}),  % 1 day old

    Factor1 = flurm_priority:age_factor(JobInfo1, Now),
    Factor2 = flurm_priority:age_factor(JobInfo2, Now),

    %% Older job should have higher factor
    ?assert(Factor2 > Factor1),
    ?assert(Factor1 > 0),
    ?assert(Factor2 > 0),
    ok.

test_age_factor_capped() ->
    Now = erlang:system_time(second),
    %% MAX_AGE is 604800 (7 days) - test with job older than that
    JobInfo = make_job_info(#{submit_time => Now - 1000000}),  % Very old

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% Factor should approach but not exceed 1.0
    ?assert(Factor < 1.0),
    ?assert(Factor > 0.9),  % Should be close to 1.0
    ok.

test_age_factor_tuple() ->
    Now = erlang:system_time(second),
    %% Test with tuple timestamp format {MegaSecs, Secs, MicroSecs}
    MegaSecs = Now div 1000000,
    Secs = Now rem 1000000 - 3600,  % 1 hour ago
    JobInfo = make_job_info(#{submit_time => {MegaSecs, Secs, 0}}),

    Factor = flurm_priority:age_factor(JobInfo, Now),

    ?assert(Factor > 0),
    ?assert(Factor < 1.0),
    ok.

test_age_factor_integer() ->
    Now = erlang:system_time(second),
    JobInfo = make_job_info(#{submit_time => Now - 7200}),  % 2 hours ago

    Factor = flurm_priority:age_factor(JobInfo, Now),

    ?assert(Factor > 0),
    ?assert(Factor < 1.0),
    ok.

test_age_factor_invalid() ->
    Now = erlang:system_time(second),
    JobInfo = make_job_info(#{submit_time => invalid}),

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% Should default to 0 (treated as submitted now)
    ?assertEqual(0.0, Factor),
    ok.

%%====================================================================
%% Size Factor Tests
%%====================================================================

size_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"size factor for small job", fun test_size_factor_small/0},
        {"size factor for large job", fun test_size_factor_large/0},
        {"size factor normalized by cluster", fun test_size_factor_normalized/0},
        {"size factor considers cpus", fun test_size_factor_cpus/0}
     ]}.

test_size_factor_small() ->
    JobInfo = make_job_info(#{num_nodes => 1, num_cpus => 4}),
    ClusterSize = 100,

    Factor = flurm_priority:size_factor(JobInfo, ClusterSize),

    %% Small jobs should get higher factor (closer to 1.0)
    ?assert(Factor > 0.5),
    ?assert(Factor =< 1.0),
    ok.

test_size_factor_large() ->
    JobInfo = make_job_info(#{num_nodes => 50, num_cpus => 64}),
    ClusterSize = 100,

    Factor = flurm_priority:size_factor(JobInfo, ClusterSize),

    %% Large jobs should get lower factor
    ?assert(Factor < 0.5),
    ?assert(Factor >= 0),
    ok.

test_size_factor_normalized() ->
    JobInfo = make_job_info(#{num_nodes => 10, num_cpus => 8}),

    Factor1 = flurm_priority:size_factor(JobInfo, 100),
    Factor2 = flurm_priority:size_factor(JobInfo, 1000),

    %% Same job appears smaller in larger cluster
    ?assert(Factor2 > Factor1),
    ok.

test_size_factor_cpus() ->
    JobInfo1 = make_job_info(#{num_nodes => 1, num_cpus => 4}),
    JobInfo2 = make_job_info(#{num_nodes => 1, num_cpus => 64}),
    ClusterSize = 100,

    Factor1 = flurm_priority:size_factor(JobInfo1, ClusterSize),
    Factor2 = flurm_priority:size_factor(JobInfo2, ClusterSize),

    %% Job with more CPUs should get lower factor
    ?assert(Factor1 > Factor2),
    ok.

%%====================================================================
%% Partition Factor Tests
%%====================================================================

partition_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"partition factor from registry", fun test_partition_from_registry/0},
        {"partition factor default on error", fun test_partition_default_error/0},
        {"partition factor normalized", fun test_partition_normalized/0}
     ]}.

test_partition_from_registry() ->
    meck:expect(flurm_partition_registry, get_partition_priority, fun(<<"high_prio">>) ->
        {ok, 8000}
    end),

    JobInfo = make_job_info(#{partition => <<"high_prio">>}),
    Factor = flurm_priority:partition_factor(JobInfo),

    ?assertEqual(0.8, Factor),
    ok.

test_partition_default_error() ->
    meck:expect(flurm_partition_registry, get_partition_priority, fun(_) ->
        {error, not_found}
    end),

    JobInfo = make_job_info(#{partition => <<"unknown">>}),
    Factor = flurm_priority:partition_factor(JobInfo),

    %% Should default to 0.5
    ?assertEqual(0.5, Factor),
    ok.

test_partition_normalized() ->
    meck:expect(flurm_partition_registry, get_partition_priority, fun(_) ->
        {ok, 15000}  % Above max 10000
    end),

    JobInfo = make_job_info(#{partition => <<"default">>}),
    Factor = flurm_priority:partition_factor(JobInfo),

    %% Should be capped at 1.0
    ?assertEqual(1.0, Factor),
    ok.

%%====================================================================
%% Nice Factor Tests
%%====================================================================

nice_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"nice factor zero is neutral", fun test_nice_zero/0},
        {"negative nice increases priority", fun test_nice_negative/0},
        {"positive nice decreases priority", fun test_nice_positive/0},
        {"nice factor bounded", fun test_nice_bounded/0}
     ]}.

test_nice_zero() ->
    JobInfo = make_job_info(#{nice => 0}),
    Factor = flurm_priority:nice_factor(JobInfo),

    ?assertEqual(0.0, Factor),
    ok.

test_nice_negative() ->
    JobInfo = make_job_info(#{nice => -5000}),
    Factor = flurm_priority:nice_factor(JobInfo),

    %% Negative nice = higher priority = positive factor
    ?assertEqual(0.5, Factor),
    ok.

test_nice_positive() ->
    JobInfo = make_job_info(#{nice => 5000}),
    Factor = flurm_priority:nice_factor(JobInfo),

    %% Positive nice = lower priority = negative factor
    ?assertEqual(-0.5, Factor),
    ok.

test_nice_bounded() ->
    JobInfo1 = make_job_info(#{nice => -10000}),
    JobInfo2 = make_job_info(#{nice => 10000}),

    Factor1 = flurm_priority:nice_factor(JobInfo1),
    Factor2 = flurm_priority:nice_factor(JobInfo2),

    ?assertEqual(1.0, Factor1),
    ?assertEqual(-1.0, Factor2),
    ok.

%%====================================================================
%% Calculate Priority Tests
%%====================================================================

calculate_priority_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"calculate_priority returns integer", fun test_calc_returns_integer/0},
        {"calculate_priority uses all factors", fun test_calc_uses_factors/0},
        {"calculate_priority respects weights", fun test_calc_respects_weights/0},
        {"calculate_priority clamped to bounds", fun test_calc_clamped/0},
        {"calculate_priority with context", fun test_calc_with_context/0}
     ]}.

test_calc_returns_integer() ->
    JobInfo = make_job_info(),
    Priority = flurm_priority:calculate_priority(JobInfo),

    ?assert(is_integer(Priority)),
    ok.

test_calc_uses_factors() ->
    Now = erlang:system_time(second),

    %% High priority job: old, small, high QOS
    JobInfo = make_job_info(#{
        submit_time => Now - 86400,  % 1 day old
        num_nodes => 1,
        num_cpus => 4,
        qos => <<"high">>,
        nice => -5000
    }),

    Priority = flurm_priority:calculate_priority(JobInfo),

    %% Should have a reasonably high priority
    ?assert(Priority > 1000),
    ok.

test_calc_respects_weights() ->
    JobInfo = make_job_info(),

    %% Calculate with default weights
    Priority1 = flurm_priority:calculate_priority(JobInfo),

    %% Set high age weight
    ok = flurm_priority:set_weights(#{
        age => 10000,
        job_size => 0,
        partition => 0,
        qos => 0,
        nice => 0,
        fairshare => 0
    }),

    Priority2 = flurm_priority:calculate_priority(JobInfo),

    %% Priority should be different with different weights
    ?assertNotEqual(Priority1, Priority2),
    ok.

test_calc_clamped() ->
    Now = erlang:system_time(second),

    %% Try to create extremely high priority
    JobInfo = make_job_info(#{
        submit_time => Now - 1000000,
        num_nodes => 1,
        num_cpus => 1,
        qos => <<"high">>,
        nice => -10000
    }),

    ok = flurm_priority:set_weights(#{
        age => 100000,
        job_size => 100000,
        partition => 100000,
        qos => 100000,
        nice => 100000,
        fairshare => 100000
    }),

    meck:expect(flurm_fairshare, get_priority_factor, fun(_, _) -> 1.0 end),

    Priority = flurm_priority:calculate_priority(JobInfo),

    %% Should be clamped to MAX_PRIORITY * 100 = 1000000
    ?assert(Priority =< 1000000),
    ?assert(Priority >= 0),
    ok.

test_calc_with_context() ->
    JobInfo = make_job_info(),
    Context = #{fairshare => 0.8},  % High fairshare value

    Priority = flurm_priority:calculate_priority(JobInfo, Context),

    ?assert(is_integer(Priority)),
    ok.

%%====================================================================
%% Get Priority Factors Tests
%%====================================================================

get_priority_factors_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_priority_factors returns all factors", fun test_get_factors_all/0},
        {"get_priority_factors factor values", fun test_get_factors_values/0}
     ]}.

test_get_factors_all() ->
    JobInfo = make_job_info(),
    Factors = flurm_priority:get_priority_factors(JobInfo),

    ?assert(is_map(Factors)),
    ?assert(maps:is_key(age, Factors)),
    ?assert(maps:is_key(job_size, Factors)),
    ?assert(maps:is_key(partition, Factors)),
    ?assert(maps:is_key(qos, Factors)),
    ?assert(maps:is_key(nice, Factors)),
    ?assert(maps:is_key(fairshare, Factors)),
    ok.

test_get_factors_values() ->
    Now = erlang:system_time(second),
    JobInfo = make_job_info(#{
        submit_time => Now - 3600,  % 1 hour old
        num_nodes => 1,
        qos => <<"high">>,
        nice => 0
    }),

    Factors = flurm_priority:get_priority_factors(JobInfo),

    ?assert(maps:get(age, Factors) > 0),
    ?assert(maps:get(job_size, Factors) > 0),
    ?assertEqual(1.0, maps:get(qos, Factors)),  % high QOS
    ?assertEqual(0.0, maps:get(nice, Factors)),  % nice = 0
    ok.

%%====================================================================
%% Recalculate All Tests
%%====================================================================

recalculate_all_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"recalculate_all with no jobs", fun test_recalc_no_jobs/0},
        {"recalculate_all updates priorities", fun test_recalc_updates/0},
        {"recalculate_all handles errors", fun test_recalc_handles_errors/0}
     ]}.

test_recalc_no_jobs() ->
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(pending) -> [] end),

    Result = flurm_priority:recalculate_all(),

    ?assertEqual(ok, Result),
    ok.

test_recalc_updates() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, list_jobs_by_state, fun(pending) ->
        [{1, MockPid}, {2, MockPid}]
    end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {ok, make_job_info()}
    end),
    meck:expect(flurm_job, set_priority, fun(_, _) -> ok end),

    Result = flurm_priority:recalculate_all(),

    ?assertEqual(ok, Result),
    %% set_priority should have been called twice
    ?assert(meck:num_calls(flurm_job, set_priority, ['_', '_']) >= 2),
    ok.

test_recalc_handles_errors() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, list_jobs_by_state, fun(pending) ->
        [{1, MockPid}, {2, MockPid}]
    end),
    meck:expect(flurm_job, get_info, fun(Pid) ->
        case Pid of
            _ when Pid =:= MockPid -> {error, not_found}
        end
    end),

    Result = flurm_priority:recalculate_all(),

    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% QOS Factor Tests
%%====================================================================

qos_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"qos high returns 1.0", fun test_qos_high/0},
        {"qos normal returns 0.5", fun test_qos_normal/0},
        {"qos low returns 0.2", fun test_qos_low/0},
        {"qos unknown returns 0.5", fun test_qos_unknown/0}
     ]}.

test_qos_high() ->
    JobInfo = make_job_info(#{qos => <<"high">>}),
    Factors = flurm_priority:get_priority_factors(JobInfo),

    ?assertEqual(1.0, maps:get(qos, Factors)),
    ok.

test_qos_normal() ->
    JobInfo = make_job_info(#{qos => <<"normal">>}),
    Factors = flurm_priority:get_priority_factors(JobInfo),

    ?assertEqual(0.5, maps:get(qos, Factors)),
    ok.

test_qos_low() ->
    JobInfo = make_job_info(#{qos => <<"low">>}),
    Factors = flurm_priority:get_priority_factors(JobInfo),

    ?assertEqual(0.2, maps:get(qos, Factors)),
    ok.

test_qos_unknown() ->
    JobInfo = make_job_info(#{qos => <<"custom">>}),
    Factors = flurm_priority:get_priority_factors(JobInfo),

    ?assertEqual(0.5, maps:get(qos, Factors)),  % Defaults to 0.5
    ok.

%%====================================================================
%% Fairshare Factor Tests
%%====================================================================

fairshare_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"fairshare from context", fun test_fairshare_context/0},
        {"fairshare from module", fun test_fairshare_module/0},
        {"fairshare default on error", fun test_fairshare_default/0}
     ]}.

test_fairshare_context() ->
    JobInfo = make_job_info(),
    Context = #{fairshare => 0.75},

    %% Use calculate_priority to test context usage
    Priority = flurm_priority:calculate_priority(JobInfo, Context),

    ?assert(is_integer(Priority)),
    ok.

test_fairshare_module() ->
    meck:expect(flurm_fairshare, get_priority_factor, fun(<<"testuser">>, <<"default">>) ->
        0.8
    end),

    JobInfo = make_job_info(#{user => <<"testuser">>, account => <<"default">>}),
    Factors = flurm_priority:get_priority_factors(JobInfo),

    ?assertEqual(0.8, maps:get(fairshare, Factors)),
    ok.

test_fairshare_default() ->
    meck:expect(flurm_fairshare, get_priority_factor, fun(_, _) ->
        erlang:error(not_running)
    end),

    JobInfo = make_job_info(),
    Factors = flurm_priority:get_priority_factors(JobInfo),

    %% Should default to 0.5
    ?assertEqual(0.5, maps:get(fairshare, Factors)),
    ok.

%%====================================================================
%% Cluster Size Tests
%%====================================================================

cluster_size_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"cluster size from registry", fun test_cluster_from_registry/0},
        {"cluster size default on error", fun test_cluster_default/0}
     ]}.

test_cluster_from_registry() ->
    meck:expect(flurm_node_registry, count_nodes, fun() -> 500 end),

    JobInfo = make_job_info(#{num_nodes => 10}),
    Factors = flurm_priority:get_priority_factors(JobInfo),

    %% Size factor should be calculated using cluster size of 500
    SizeFactor = maps:get(job_size, Factors),
    ?assert(SizeFactor > 0.9),  % 10/500 = 0.02 = small job = high factor
    ok.

test_cluster_default() ->
    meck:expect(flurm_node_registry, count_nodes, fun() ->
        erlang:error(not_running)
    end),

    JobInfo = make_job_info(#{num_nodes => 10}),
    Factors = flurm_priority:get_priority_factors(JobInfo),

    %% Should use default cluster size of 100
    SizeFactor = maps:get(job_size, Factors),
    ?assert(SizeFactor > 0),
    ok.

%%====================================================================
%% Edge Cases
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"missing job fields handled", fun test_missing_fields/0},
        {"negative age handled", fun test_negative_age/0}
     ]}.

test_missing_fields() ->
    %% Minimal job info
    JobInfo = #{},

    Priority = flurm_priority:calculate_priority(JobInfo),

    ?assert(is_integer(Priority)),
    ok.

test_negative_age() ->
    Now = erlang:system_time(second),
    %% Submit time in the future (shouldn't happen but handle gracefully)
    JobInfo = make_job_info(#{submit_time => Now + 1000}),

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% Should be 0 (no negative age)
    ?assertEqual(0.0, Factor),
    ok.
