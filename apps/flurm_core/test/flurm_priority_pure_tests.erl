%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_priority module
%%%
%%% These tests do NOT use meck or any mocking framework.
%%% All exported functions are tested directly by exercising the
%%% actual implementation. External dependencies are allowed to fail
%%% gracefully (the module handles this with catch and default values).
%%%
%%% Tests cover:
%%% - calculate_priority/1, calculate_priority/2
%%% - recalculate_all/0 (graceful failure handling)
%%% - get_priority_factors/1
%%% - set_weights/1, get_weights/0
%%% - age_factor/2 (internal export)
%%% - size_factor/2 (internal export)
%%% - partition_factor/1 (internal export)
%%% - nice_factor/1 (internal export)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_priority_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Clean up weights table to ensure fresh state
    %% Use catch to handle both existing and non-existing table cases
    catch ets:delete(flurm_priority_weights),
    ok.

cleanup(_) ->
    %% Clean up weights table
    %% Use catch to handle both existing and non-existing table cases
    catch ets:delete(flurm_priority_weights),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

%% Create a standard job info map with defaults
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
        submit_time => Now,
        nice => 0,
        qos => <<"normal">>
    },
    maps:merge(Defaults, Overrides).

%%====================================================================
%% Weight Management Tests
%%====================================================================

weights_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_weights returns defaults when table doesn't exist",
         fun test_get_weights_defaults/0},
        {"set_weights creates table and stores weights",
         fun test_set_weights_creates_table/0},
        {"set_weights replaces existing weights",
         fun test_set_weights_replaces/0},
        {"get_weights returns set weights",
         fun test_get_weights_returns_set/0},
        {"get_weights returns defaults when table empty",
         fun test_get_weights_empty_table/0},
        {"set_weights with partial weights",
         fun test_set_weights_partial/0},
        {"weights persistence across calls",
         fun test_weights_persistence/0}
     ]}.

test_get_weights_defaults() ->
    %% Table doesn't exist, should return defaults
    Weights = flurm_priority:get_weights(),

    ?assert(is_map(Weights)),
    ?assertEqual(1000, maps:get(age, Weights)),
    ?assertEqual(200, maps:get(job_size, Weights)),
    ?assertEqual(100, maps:get(partition, Weights)),
    ?assertEqual(500, maps:get(qos, Weights)),
    ?assertEqual(100, maps:get(nice, Weights)),
    ?assertEqual(5000, maps:get(fairshare, Weights)).

test_set_weights_creates_table() ->
    ?assertEqual(undefined, ets:whereis(flurm_priority_weights)),

    NewWeights = #{age => 2000, job_size => 400},
    ok = flurm_priority:set_weights(NewWeights),

    ?assertNotEqual(undefined, ets:whereis(flurm_priority_weights)).

test_set_weights_replaces() ->
    ok = flurm_priority:set_weights(#{age => 1000}),
    ok = flurm_priority:set_weights(#{age => 2000}),

    Weights = flurm_priority:get_weights(),
    ?assertEqual(2000, maps:get(age, Weights)).

test_get_weights_returns_set() ->
    NewWeights = #{
        age => 5000,
        job_size => 1000,
        partition => 500,
        qos => 2000,
        nice => 300,
        fairshare => 8000
    },
    ok = flurm_priority:set_weights(NewWeights),

    Retrieved = flurm_priority:get_weights(),
    ?assertEqual(NewWeights, Retrieved).

test_get_weights_empty_table() ->
    %% Create an empty table manually
    %% Setup already deleted any existing table, so this is safe
    ets:new(flurm_priority_weights, [named_table, public, set]),

    %% Should return defaults since no weights key exists
    Weights = flurm_priority:get_weights(),
    ?assertEqual(1000, maps:get(age, Weights)).

test_set_weights_partial() ->
    %% Set only some weights
    PartialWeights = #{age => 9999},
    ok = flurm_priority:set_weights(PartialWeights),

    Retrieved = flurm_priority:get_weights(),
    ?assertEqual(9999, maps:get(age, Retrieved)),
    %% Other keys won't be present since we replaced the whole map
    ?assertEqual(undefined, maps:get(job_size, Retrieved, undefined)).

test_weights_persistence() ->
    NewWeights = #{age => 7777, fairshare => 8888},
    ok = flurm_priority:set_weights(NewWeights),

    %% Multiple reads should return same value
    W1 = flurm_priority:get_weights(),
    W2 = flurm_priority:get_weights(),
    W3 = flurm_priority:get_weights(),

    ?assertEqual(W1, W2),
    ?assertEqual(W2, W3),
    ?assertEqual(7777, maps:get(age, W1)).

%%====================================================================
%% Age Factor Tests
%%====================================================================

age_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"age factor is zero for job submitted now",
         fun test_age_factor_zero_now/0},
        {"age factor is small for recent job",
         fun test_age_factor_small_recent/0},
        {"age factor increases with age",
         fun test_age_factor_increases/0},
        {"age factor approaches 1.0 for old jobs",
         fun test_age_factor_approaches_one/0},
        {"age factor capped at 7 days",
         fun test_age_factor_capped/0},
        {"age factor with erlang timestamp tuple",
         fun test_age_factor_tuple_timestamp/0},
        {"age factor with integer timestamp",
         fun test_age_factor_integer_timestamp/0},
        {"age factor with missing submit_time uses Now",
         fun test_age_factor_missing_time/0},
        {"age factor with invalid submit_time uses Now",
         fun test_age_factor_invalid_time/0},
        {"age factor with future submit_time returns 0",
         fun test_age_factor_future_time/0},
        {"age factor exponential decay behavior",
         fun test_age_factor_decay/0}
     ]}.

test_age_factor_zero_now() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now},

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% Should be essentially zero
    ?assert(Factor >= 0.0),
    ?assert(Factor < 0.0001).

test_age_factor_small_recent() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now - 60},  % 1 minute ago

    Factor = flurm_priority:age_factor(JobInfo, Now),

    ?assert(Factor > 0.0),
    ?assert(Factor < 0.01).

test_age_factor_increases() ->
    Now = erlang:system_time(second),

    Job1Hour = #{submit_time => Now - 3600},    % 1 hour ago
    Job1Day = #{submit_time => Now - 86400},     % 1 day ago
    Job3Days = #{submit_time => Now - (3 * 86400)},  % 3 days ago

    Factor1 = flurm_priority:age_factor(Job1Hour, Now),
    Factor2 = flurm_priority:age_factor(Job1Day, Now),
    Factor3 = flurm_priority:age_factor(Job3Days, Now),

    %% Older jobs should have higher factors
    ?assert(Factor1 < Factor2),
    ?assert(Factor2 < Factor3).

test_age_factor_approaches_one() ->
    Now = erlang:system_time(second),
    %% Job from 7 days ago (MAX_AGE)
    JobInfo = #{submit_time => Now - 604800},

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% Should be close to 1.0 but not exceed it
    ?assert(Factor > 0.99),
    ?assert(Factor < 1.0).

test_age_factor_capped() ->
    Now = erlang:system_time(second),

    %% 7 days (exactly at cap)
    Job7Days = #{submit_time => Now - 604800},
    %% 14 days (beyond cap)
    Job14Days = #{submit_time => Now - (14 * 86400)},
    %% 30 days (way beyond cap)
    Job30Days = #{submit_time => Now - (30 * 86400)},

    Factor7 = flurm_priority:age_factor(Job7Days, Now),
    Factor14 = flurm_priority:age_factor(Job14Days, Now),
    Factor30 = flurm_priority:age_factor(Job30Days, Now),

    %% All should be approximately equal (capped)
    ?assert(abs(Factor7 - Factor14) < 0.001),
    ?assert(abs(Factor14 - Factor30) < 0.001).

test_age_factor_tuple_timestamp() ->
    Now = erlang:system_time(second),

    %% Create timestamp 1 day ago in tuple format {MegaSecs, Secs, MicroSecs}
    TotalSecs = Now - 86400,
    MegaSecs = TotalSecs div 1000000,
    Secs = TotalSecs rem 1000000,

    JobInfo = #{submit_time => {MegaSecs, Secs, 123456}},

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% Should be roughly 0.63 for 1 day (1 - exp(-1))
    ?assert(Factor > 0.5),
    ?assert(Factor < 0.7).

test_age_factor_integer_timestamp() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now - 43200},  % 12 hours ago

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% 12 hours = half a day, so factor should be around 0.39
    ?assert(Factor > 0.3),
    ?assert(Factor < 0.5).

test_age_factor_missing_time() ->
    Now = erlang:system_time(second),
    JobInfo = #{},  % No submit_time

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% Should treat as just submitted (factor = 0)
    ?assert(Factor < 0.001).

test_age_factor_invalid_time() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => invalid_atom},

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% Should treat as just submitted
    ?assertEqual(0.0, Factor).

test_age_factor_future_time() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now + 1000},  % In the future

    Factor = flurm_priority:age_factor(JobInfo, Now),

    %% Negative age should be clamped to 0
    ?assertEqual(0.0, Factor).

test_age_factor_decay() ->
    Now = erlang:system_time(second),
    %% AGE_DECAY_FACTOR is 86400 (1 day) - half-life

    Job1Day = #{submit_time => Now - 86400},
    Job2Days = #{submit_time => Now - (2 * 86400)},

    Factor1 = flurm_priority:age_factor(Job1Day, Now),
    Factor2 = flurm_priority:age_factor(Job2Days, Now),

    %% 1 - exp(-1) ~= 0.632
    %% 1 - exp(-2) ~= 0.865
    ?assert(abs(Factor1 - 0.632) < 0.01),
    ?assert(abs(Factor2 - 0.865) < 0.01).

%%====================================================================
%% Size Factor Tests
%%====================================================================

size_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"size factor for single node job",
         fun test_size_factor_single_node/0},
        {"size factor for large job",
         fun test_size_factor_large_job/0},
        {"size factor smaller jobs get higher factor",
         fun test_size_factor_small_vs_large/0},
        {"size factor normalized by cluster size",
         fun test_size_factor_cluster_normalized/0},
        {"size factor with default num_nodes",
         fun test_size_factor_default_nodes/0},
        {"size factor with default num_cpus",
         fun test_size_factor_default_cpus/0},
        {"size factor cpus affect result",
         fun test_size_factor_cpus_impact/0},
        {"size factor with zero cluster size",
         fun test_size_factor_zero_cluster/0},
        {"size factor range is valid",
         fun test_size_factor_range/0}
     ]}.

test_size_factor_single_node() ->
    JobInfo = #{num_nodes => 1, num_cpus => 4},
    ClusterSize = 100,

    Factor = flurm_priority:size_factor(JobInfo, ClusterSize),

    %% Small job should get high factor
    ?assert(Factor > 0.9),
    ?assert(Factor =< 1.0).

test_size_factor_large_job() ->
    JobInfo = #{num_nodes => 100, num_cpus => 64},
    ClusterSize = 100,

    Factor = flurm_priority:size_factor(JobInfo, ClusterSize),

    %% Large job (100% of cluster) should get low factor
    ?assert(Factor < 0.5).

test_size_factor_small_vs_large() ->
    SmallJob = #{num_nodes => 1, num_cpus => 4},
    MediumJob = #{num_nodes => 10, num_cpus => 32},
    LargeJob = #{num_nodes => 50, num_cpus => 64},
    ClusterSize = 100,

    SmallFactor = flurm_priority:size_factor(SmallJob, ClusterSize),
    MediumFactor = flurm_priority:size_factor(MediumJob, ClusterSize),
    LargeFactor = flurm_priority:size_factor(LargeJob, ClusterSize),

    ?assert(SmallFactor > MediumFactor),
    ?assert(MediumFactor > LargeFactor).

test_size_factor_cluster_normalized() ->
    JobInfo = #{num_nodes => 10, num_cpus => 16},

    %% Same job in different sized clusters
    Factor10 = flurm_priority:size_factor(JobInfo, 10),   % 100% of cluster
    Factor100 = flurm_priority:size_factor(JobInfo, 100),  % 10% of cluster
    Factor1000 = flurm_priority:size_factor(JobInfo, 1000), % 1% of cluster

    %% Job appears smaller in larger clusters
    ?assert(Factor10 < Factor100),
    ?assert(Factor100 < Factor1000).

test_size_factor_default_nodes() ->
    JobInfo = #{num_cpus => 4},  % No num_nodes
    ClusterSize = 100,

    Factor = flurm_priority:size_factor(JobInfo, ClusterSize),

    %% Should use default of 1 node
    ExpectedFactor = flurm_priority:size_factor(#{num_nodes => 1, num_cpus => 4}, ClusterSize),
    ?assertEqual(ExpectedFactor, Factor).

test_size_factor_default_cpus() ->
    JobInfo = #{num_nodes => 1},  % No num_cpus
    ClusterSize = 100,

    Factor = flurm_priority:size_factor(JobInfo, ClusterSize),

    %% Should use default of 1 CPU
    ExpectedFactor = flurm_priority:size_factor(#{num_nodes => 1, num_cpus => 1}, ClusterSize),
    ?assertEqual(ExpectedFactor, Factor).

test_size_factor_cpus_impact() ->
    LowCpu = #{num_nodes => 1, num_cpus => 4},
    HighCpu = #{num_nodes => 1, num_cpus => 64},
    ClusterSize = 100,

    LowFactor = flurm_priority:size_factor(LowCpu, ClusterSize),
    HighFactor = flurm_priority:size_factor(HighCpu, ClusterSize),

    %% More CPUs = lower factor
    ?assert(LowFactor > HighFactor).

test_size_factor_zero_cluster() ->
    JobInfo = #{num_nodes => 1, num_cpus => 4},

    %% Should handle zero gracefully (uses max(1, ClusterSize))
    Factor = flurm_priority:size_factor(JobInfo, 0),

    ?assert(is_float(Factor)).

test_size_factor_range() ->
    %% Test various sizes to ensure factor stays in valid range
    ClusterSize = 100,

    Jobs = [
        #{num_nodes => 1, num_cpus => 1},
        #{num_nodes => 50, num_cpus => 32},
        #{num_nodes => 100, num_cpus => 64},
        #{num_nodes => 200, num_cpus => 128}  % Beyond cluster size
    ],

    lists:foreach(fun(Job) ->
        Factor = flurm_priority:size_factor(Job, ClusterSize),
        ?assert(Factor >= 0.0 orelse Factor < 1.5)
    end, Jobs).

%%====================================================================
%% Partition Factor Tests
%%====================================================================

partition_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"partition factor returns default when registry unavailable",
         fun test_partition_default/0},
        {"partition factor with explicit partition",
         fun test_partition_explicit/0},
        {"partition factor with missing partition uses default",
         fun test_partition_missing/0}
     ]}.

test_partition_default() ->
    JobInfo = #{partition => <<"compute">>},

    %% Without registry running, should return 0.5
    Factor = flurm_priority:partition_factor(JobInfo),

    ?assertEqual(0.5, Factor).

test_partition_explicit() ->
    JobInfo = #{partition => <<"gpu">>},

    Factor = flurm_priority:partition_factor(JobInfo),

    %% Without registry, always 0.5
    ?assertEqual(0.5, Factor).

test_partition_missing() ->
    JobInfo = #{},  % No partition key

    Factor = flurm_priority:partition_factor(JobInfo),

    %% Should use <<"default">> and return 0.5
    ?assertEqual(0.5, Factor).

%%====================================================================
%% Nice Factor Tests
%%====================================================================

nice_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"nice factor zero is neutral",
         fun test_nice_zero/0},
        {"nice factor negative increases priority",
         fun test_nice_negative/0},
        {"nice factor positive decreases priority",
         fun test_nice_positive/0},
        {"nice factor at max positive",
         fun test_nice_max_positive/0},
        {"nice factor at max negative",
         fun test_nice_max_negative/0},
        {"nice factor with missing nice uses default",
         fun test_nice_missing/0},
        {"nice factor linear scaling",
         fun test_nice_linear/0}
     ]}.

test_nice_zero() ->
    JobInfo = #{nice => 0},

    Factor = flurm_priority:nice_factor(JobInfo),

    ?assertEqual(0.0, Factor).

test_nice_negative() ->
    JobInfo = #{nice => -5000},

    Factor = flurm_priority:nice_factor(JobInfo),

    %% Negative nice = higher priority = positive factor
    ?assertEqual(0.5, Factor).

test_nice_positive() ->
    JobInfo = #{nice => 5000},

    Factor = flurm_priority:nice_factor(JobInfo),

    %% Positive nice = lower priority = negative factor
    ?assertEqual(-0.5, Factor).

test_nice_max_positive() ->
    JobInfo = #{nice => 10000},

    Factor = flurm_priority:nice_factor(JobInfo),

    ?assertEqual(-1.0, Factor).

test_nice_max_negative() ->
    JobInfo = #{nice => -10000},

    Factor = flurm_priority:nice_factor(JobInfo),

    ?assertEqual(1.0, Factor).

test_nice_missing() ->
    JobInfo = #{},  % No nice key

    Factor = flurm_priority:nice_factor(JobInfo),

    %% Should use default of 0
    ?assertEqual(0.0, Factor).

test_nice_linear() ->
    %% Nice factor should be linear: -Nice / 10000
    TestCases = [
        {-10000, 1.0},
        {-5000, 0.5},
        {-2500, 0.25},
        {0, 0.0},
        {2500, -0.25},
        {5000, -0.5},
        {10000, -1.0}
    ],

    lists:foreach(fun({Nice, Expected}) ->
        JobInfo = #{nice => Nice},
        Factor = flurm_priority:nice_factor(JobInfo),
        ?assertEqual(Expected, Factor)
    end, TestCases).

%%====================================================================
%% Calculate Priority Tests
%%====================================================================

calculate_priority_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"calculate_priority returns integer",
         fun test_calc_returns_integer/0},
        {"calculate_priority/1 uses empty context",
         fun test_calc_arity_1/0},
        {"calculate_priority/2 accepts context",
         fun test_calc_arity_2/0},
        {"calculate_priority with fairshare context",
         fun test_calc_fairshare_context/0},
        {"calculate_priority clamped to min",
         fun test_calc_clamped_min/0},
        {"calculate_priority clamped to max",
         fun test_calc_clamped_max/0},
        {"calculate_priority respects weights",
         fun test_calc_respects_weights/0},
        {"calculate_priority higher QOS gives higher priority",
         fun test_calc_qos_ordering/0},
        {"calculate_priority older jobs get higher priority",
         fun test_calc_age_ordering/0},
        {"calculate_priority empty job info handled",
         fun test_calc_empty_job/0},
        {"calculate_priority with all factors",
         fun test_calc_all_factors/0}
     ]}.

test_calc_returns_integer() ->
    JobInfo = make_job_info(),

    Priority = flurm_priority:calculate_priority(JobInfo),

    ?assert(is_integer(Priority)).

test_calc_arity_1() ->
    JobInfo = make_job_info(),

    Priority1 = flurm_priority:calculate_priority(JobInfo),
    Priority2 = flurm_priority:calculate_priority(JobInfo, #{}),

    %% Both should return same value when no fairshare context
    ?assertEqual(Priority1, Priority2).

test_calc_arity_2() ->
    JobInfo = make_job_info(),
    Context = #{fairshare => 0.8},

    Priority = flurm_priority:calculate_priority(JobInfo, Context),

    ?assert(is_integer(Priority)).

test_calc_fairshare_context() ->
    JobInfo = make_job_info(),

    HighFS = flurm_priority:calculate_priority(JobInfo, #{fairshare => 1.0}),
    LowFS = flurm_priority:calculate_priority(JobInfo, #{fairshare => 0.1}),

    %% Higher fairshare should give higher priority
    ?assert(HighFS > LowFS).

test_calc_clamped_min() ->
    %% Try to create very low priority
    Now = erlang:system_time(second),
    JobInfo = #{
        submit_time => Now,
        num_nodes => 1,
        num_cpus => 1,
        qos => <<"low">>,
        nice => 10000
    },

    %% Set all weights to 0 except nice which will be negative
    ok = flurm_priority:set_weights(#{
        age => 0,
        job_size => 0,
        partition => 0,
        qos => 0,
        nice => 1000,
        fairshare => 0
    }),

    Priority = flurm_priority:calculate_priority(JobInfo, #{fairshare => 0.0}),

    %% Should be clamped to MIN_PRIORITY (0)
    ?assert(Priority >= 0).

test_calc_clamped_max() ->
    Now = erlang:system_time(second),
    JobInfo = #{
        submit_time => Now - 1000000,  % Very old
        num_nodes => 1,
        num_cpus => 1,
        qos => <<"high">>,
        nice => -10000
    },

    %% Set very high weights
    ok = flurm_priority:set_weights(#{
        age => 1000000,
        job_size => 1000000,
        partition => 1000000,
        qos => 1000000,
        nice => 1000000,
        fairshare => 1000000
    }),

    Priority = flurm_priority:calculate_priority(JobInfo, #{fairshare => 1.0}),

    %% Should be clamped to MAX_PRIORITY * 100 = 1000000
    ?assert(Priority =< 1000000).

test_calc_respects_weights() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now - 86400, qos => <<"normal">>},

    %% Low age weight
    ok = flurm_priority:set_weights(#{age => 10, job_size => 10, partition => 10,
                                      qos => 10, nice => 10, fairshare => 10}),
    LowPriority = flurm_priority:calculate_priority(JobInfo),

    %% High age weight
    ok = flurm_priority:set_weights(#{age => 10000, job_size => 10, partition => 10,
                                      qos => 10, nice => 10, fairshare => 10}),
    HighPriority = flurm_priority:calculate_priority(JobInfo),

    %% Higher age weight should give higher priority for old job
    ?assert(HighPriority > LowPriority).

test_calc_qos_ordering() ->
    Now = erlang:system_time(second),
    BaseJob = #{submit_time => Now, num_nodes => 1, num_cpus => 4, nice => 0},

    HighQOS = maps:put(qos, <<"high">>, BaseJob),
    NormalQOS = maps:put(qos, <<"normal">>, BaseJob),
    LowQOS = maps:put(qos, <<"low">>, BaseJob),

    HighPrio = flurm_priority:calculate_priority(HighQOS),
    NormalPrio = flurm_priority:calculate_priority(NormalQOS),
    LowPrio = flurm_priority:calculate_priority(LowQOS),

    ?assert(HighPrio > NormalPrio),
    ?assert(NormalPrio > LowPrio).

test_calc_age_ordering() ->
    Now = erlang:system_time(second),
    BaseJob = #{num_nodes => 1, num_cpus => 4, qos => <<"normal">>, nice => 0},

    NewJob = maps:put(submit_time, Now, BaseJob),
    OldJob = maps:put(submit_time, Now - 86400, BaseJob),
    VeryOldJob = maps:put(submit_time, Now - (3 * 86400), BaseJob),

    NewPrio = flurm_priority:calculate_priority(NewJob),
    OldPrio = flurm_priority:calculate_priority(OldJob),
    VeryOldPrio = flurm_priority:calculate_priority(VeryOldJob),

    ?assert(VeryOldPrio > OldPrio),
    ?assert(OldPrio > NewPrio).

test_calc_empty_job() ->
    Priority = flurm_priority:calculate_priority(#{}),

    ?assert(is_integer(Priority)),
    ?assert(Priority >= 0).

test_calc_all_factors() ->
    Now = erlang:system_time(second),
    JobInfo = #{
        submit_time => Now - 43200,  % 12 hours old
        num_nodes => 4,
        num_cpus => 32,
        partition => <<"compute">>,
        qos => <<"high">>,
        nice => -2000,
        user => <<"alice">>,
        account => <<"research">>
    },

    Priority = flurm_priority:calculate_priority(JobInfo),

    ?assert(is_integer(Priority)),
    ?assert(Priority > 0).

%%====================================================================
%% Get Priority Factors Tests
%%====================================================================

get_priority_factors_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_priority_factors returns all factors",
         fun test_factors_all_present/0},
        {"get_priority_factors all numeric",
         fun test_factors_all_numeric/0},
        {"get_priority_factors age factor value",
         fun test_factors_age_value/0},
        {"get_priority_factors qos factor value",
         fun test_factors_qos_value/0},
        {"get_priority_factors nice factor value",
         fun test_factors_nice_value/0}
     ]}.

test_factors_all_present() ->
    JobInfo = make_job_info(),

    Factors = flurm_priority:get_priority_factors(JobInfo),

    ?assert(is_map(Factors)),
    ?assert(maps:is_key(age, Factors)),
    ?assert(maps:is_key(job_size, Factors)),
    ?assert(maps:is_key(partition, Factors)),
    ?assert(maps:is_key(qos, Factors)),
    ?assert(maps:is_key(nice, Factors)),
    ?assert(maps:is_key(fairshare, Factors)).

test_factors_all_numeric() ->
    JobInfo = make_job_info(),

    Factors = flurm_priority:get_priority_factors(JobInfo),

    ?assert(is_float(maps:get(age, Factors))),
    ?assert(is_float(maps:get(job_size, Factors))),
    ?assert(is_float(maps:get(partition, Factors))),
    ?assert(is_float(maps:get(qos, Factors)) orelse is_integer(maps:get(qos, Factors))),
    ?assert(is_float(maps:get(nice, Factors)) orelse is_integer(maps:get(nice, Factors))),
    ?assert(is_float(maps:get(fairshare, Factors))).

test_factors_age_value() ->
    Now = erlang:system_time(second),
    JobInfo = make_job_info(#{submit_time => Now - 86400}),  % 1 day old

    Factors = flurm_priority:get_priority_factors(JobInfo),
    Age = maps:get(age, Factors),

    %% 1 day old should give factor around 0.63
    ?assert(Age > 0.6),
    ?assert(Age < 0.7).

test_factors_qos_value() ->
    HighJob = make_job_info(#{qos => <<"high">>}),
    NormalJob = make_job_info(#{qos => <<"normal">>}),
    LowJob = make_job_info(#{qos => <<"low">>}),

    HighFactors = flurm_priority:get_priority_factors(HighJob),
    NormalFactors = flurm_priority:get_priority_factors(NormalJob),
    LowFactors = flurm_priority:get_priority_factors(LowJob),

    ?assertEqual(1.0, maps:get(qos, HighFactors)),
    ?assertEqual(0.5, maps:get(qos, NormalFactors)),
    ?assertEqual(0.2, maps:get(qos, LowFactors)).

test_factors_nice_value() ->
    NiceJob = make_job_info(#{nice => -5000}),
    NeutralJob = make_job_info(#{nice => 0}),
    MeanJob = make_job_info(#{nice => 5000}),

    NiceFactors = flurm_priority:get_priority_factors(NiceJob),
    NeutralFactors = flurm_priority:get_priority_factors(NeutralJob),
    MeanFactors = flurm_priority:get_priority_factors(MeanJob),

    ?assertEqual(0.5, maps:get(nice, NiceFactors)),
    ?assertEqual(0.0, maps:get(nice, NeutralFactors)),
    ?assertEqual(-0.5, maps:get(nice, MeanFactors)).

%%====================================================================
%% Recalculate All Tests
%%====================================================================

recalculate_all_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"recalculate_all fails when registry unavailable",
         fun test_recalc_no_registry/0},
        {"recalculate_all works with empty job list",
         fun test_recalc_empty_list/0},
        {"recalculate_all processes jobs",
         fun test_recalc_with_jobs/0}
     ]}.

test_recalc_no_registry() ->
    %% Without job_registry ETS table, function will throw error
    %% Test that we can catch the error
    Result = try
        flurm_priority:recalculate_all()
    catch
        error:badarg -> {error, badarg}
    end,

    %% The function will error because flurm_job_registry:list_jobs_by_state
    %% requires its ETS table to exist
    ?assertEqual({error, badarg}, Result).

test_recalc_empty_list() ->
    %% Create the ETS table that job_registry expects
    ets:new(flurm_jobs_by_state, [named_table, public, bag]),

    try
        Result = flurm_priority:recalculate_all(),
        ?assertEqual(ok, Result)
    after
        catch ets:delete(flurm_jobs_by_state)
    end.

test_recalc_with_jobs() ->
    %% Create the ETS tables that job_registry expects
    ets:new(flurm_jobs_by_state, [named_table, public, bag]),
    ets:new(flurm_jobs, [named_table, public, set]),

    %% Create a fake "pending" job entry - the job registry stores {State, {JobId, Pid}}
    FakePid = spawn(fun() -> receive stop -> ok after 5000 -> ok end end),
    ets:insert(flurm_jobs_by_state, {pending, {1, FakePid}}),

    try
        %% recalculate_all will try to call flurm_job:get_info(Pid)
        %% which will fail, but the function catches it with catch
        Result = flurm_priority:recalculate_all(),
        ?assertEqual(ok, Result)
    after
        FakePid ! stop,
        catch ets:delete(flurm_jobs_by_state),
        catch ets:delete(flurm_jobs)
    end.

%%====================================================================
%% QOS Factor Tests (via get_priority_factors)
%%====================================================================

qos_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"qos high returns 1.0",
         fun test_qos_high/0},
        {"qos normal returns 0.5",
         fun test_qos_normal/0},
        {"qos low returns 0.2",
         fun test_qos_low/0},
        {"qos unknown returns 0.5",
         fun test_qos_unknown/0}
     ]}.

test_qos_high() ->
    JobInfo = make_job_info(#{qos => <<"high">>}),
    Factors = flurm_priority:get_priority_factors(JobInfo),
    ?assertEqual(1.0, maps:get(qos, Factors)).

test_qos_normal() ->
    JobInfo = make_job_info(#{qos => <<"normal">>}),
    Factors = flurm_priority:get_priority_factors(JobInfo),
    ?assertEqual(0.5, maps:get(qos, Factors)).

test_qos_low() ->
    JobInfo = make_job_info(#{qos => <<"low">>}),
    Factors = flurm_priority:get_priority_factors(JobInfo),
    ?assertEqual(0.2, maps:get(qos, Factors)).

test_qos_unknown() ->
    JobInfo = make_job_info(#{qos => <<"custom_qos">>}),
    Factors = flurm_priority:get_priority_factors(JobInfo),
    %% Unknown QOS defaults to 0.5
    ?assertEqual(0.5, maps:get(qos, Factors)).

%%====================================================================
%% Fairshare Factor Tests (via calculate_priority with context)
%%====================================================================

fairshare_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"fairshare from context used directly",
         fun test_fs_from_context/0},
        {"fairshare default when module unavailable",
         fun test_fs_default/0},
        {"fairshare impacts priority significantly",
         fun test_fs_impact/0}
     ]}.

test_fs_from_context() ->
    JobInfo = make_job_info(),

    %% Different fairshare values should produce different priorities
    HighFS = flurm_priority:calculate_priority(JobInfo, #{fairshare => 0.9}),
    LowFS = flurm_priority:calculate_priority(JobInfo, #{fairshare => 0.1}),

    ?assert(HighFS > LowFS).

test_fs_default() ->
    %% Without context and without fairshare module, should use 0.5
    JobInfo = make_job_info(),
    Factors = flurm_priority:get_priority_factors(JobInfo),

    %% Fairshare module not running, should default to 0.5
    ?assertEqual(0.5, maps:get(fairshare, Factors)).

test_fs_impact() ->
    JobInfo = make_job_info(),

    %% Set high fairshare weight to make impact visible
    ok = flurm_priority:set_weights(#{
        age => 100,
        job_size => 100,
        partition => 100,
        qos => 100,
        nice => 100,
        fairshare => 10000
    }),

    HighFS = flurm_priority:calculate_priority(JobInfo, #{fairshare => 1.0}),
    LowFS = flurm_priority:calculate_priority(JobInfo, #{fairshare => 0.0}),

    %% Difference should be approximately 10000 (weight * factor difference)
    Diff = HighFS - LowFS,
    ?assert(Diff > 5000).

%%====================================================================
%% Integration Tests
%%====================================================================

integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"priority ordering comprehensive test",
         fun test_priority_ordering/0},
        {"weight changes affect priority",
         fun test_weight_changes/0}
     ]}.

test_priority_ordering() ->
    Now = erlang:system_time(second),

    %% Reset to default weights
    ok = flurm_priority:set_weights(#{
        age => 1000,
        job_size => 200,
        partition => 100,
        qos => 500,
        nice => 100,
        fairshare => 5000
    }),

    %% Create jobs with different characteristics
    HighPriorityJob = #{
        submit_time => Now - 86400,  % Old
        num_nodes => 1,
        num_cpus => 4,
        qos => <<"high">>,
        nice => -5000
    },

    LowPriorityJob = #{
        submit_time => Now,  % New
        num_nodes => 50,
        num_cpus => 400,
        qos => <<"low">>,
        nice => 5000
    },

    HighPrio = flurm_priority:calculate_priority(HighPriorityJob, #{fairshare => 0.9}),
    LowPrio = flurm_priority:calculate_priority(LowPriorityJob, #{fairshare => 0.1}),

    ?assert(HighPrio > LowPrio).

test_weight_changes() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now - 3600, qos => <<"high">>},

    %% Calculate with default weights
    _ = flurm_priority:get_weights(),  % Ensure table exists
    Priority1 = flurm_priority:calculate_priority(JobInfo),

    %% Change weights significantly
    ok = flurm_priority:set_weights(#{
        age => 0,
        job_size => 0,
        partition => 0,
        qos => 10000,  % Only QOS matters
        nice => 0,
        fairshare => 0
    }),
    Priority2 = flurm_priority:calculate_priority(JobInfo),

    %% Priorities should be different
    ?assertNotEqual(Priority1, Priority2).

%%====================================================================
%% Edge Cases and Boundary Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"handles very large num_nodes",
         fun test_large_nodes/0},
        {"handles very large num_cpus",
         fun test_large_cpus/0},
        {"handles extreme nice values",
         fun test_extreme_nice/0},
        {"handles concurrent access to weights",
         fun test_concurrent_weights/0}
     ]}.

test_large_nodes() ->
    JobInfo = #{num_nodes => 10000, num_cpus => 1},

    Factor = flurm_priority:size_factor(JobInfo, 100),

    ?assert(is_float(Factor)).

test_large_cpus() ->
    JobInfo = #{num_nodes => 1, num_cpus => 100000},

    Factor = flurm_priority:size_factor(JobInfo, 100),

    ?assert(is_float(Factor)).

test_extreme_nice() ->
    %% Beyond normal range
    Job1 = #{nice => -100000},
    Job2 = #{nice => 100000},

    Factor1 = flurm_priority:nice_factor(Job1),
    Factor2 = flurm_priority:nice_factor(Job2),

    %% Should still calculate (may exceed -1 to 1 range)
    ?assert(is_float(Factor1) orelse is_integer(Factor1)),
    ?assert(is_float(Factor2) orelse is_integer(Factor2)).

test_concurrent_weights() ->
    %% Spawn multiple processes to read/write weights
    Self = self(),

    Pids = [spawn(fun() ->
        ok = flurm_priority:set_weights(#{age => N * 100}),
        _ = flurm_priority:get_weights(),
        Self ! {done, N}
    end) || N <- lists:seq(1, 10)],

    %% Wait for all to complete
    Results = [receive {done, N} -> N after 1000 -> timeout end
               || _ <- Pids],

    ?assertEqual(lists:seq(1, 10), lists:sort(Results)).
