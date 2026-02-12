%%%-------------------------------------------------------------------
%%% @doc FLURM Priority Coverage Tests
%%%
%%% Comprehensive EUnit tests that call real flurm_priority functions
%%% (no mocking) to maximize rebar3 cover results. Exercises every
%%% exported and TEST-exported function with edge cases.
%%%
%%% External dependencies (flurm_fairshare, flurm_partition_registry,
%%% flurm_node_registry) are not running, so functions that call them
%%% will hit the catch/default branches -- which is good for coverage.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_priority_cover_tests).

-include_lib("flurm_core/include/flurm_core.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixture - ensure ETS table exists, clean up after
%%====================================================================

priority_cover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"ensure_weights_table creates table", fun test_ensure_weights_table/0},
        {"ensure_weights_table is idempotent", fun test_ensure_weights_table_idempotent/0},
        {"get_weights returns default map", fun test_get_weights_defaults/0},
        {"set_weights then get_weights", fun test_set_and_get_weights/0},
        {"set_weights partial map", fun test_set_weights_partial/0},
        {"age_factor zero for just-submitted job", fun test_age_factor_zero/0},
        {"age_factor increases with age", fun test_age_factor_increases/0},
        {"age_factor capped for very old job", fun test_age_factor_capped/0},
        {"age_factor handles tuple timestamp", fun test_age_factor_tuple_timestamp/0},
        {"age_factor handles invalid timestamp", fun test_age_factor_invalid_timestamp/0},
        {"age_factor handles future submit time", fun test_age_factor_future/0},
        {"age_factor with missing submit_time", fun test_age_factor_missing_key/0},
        {"size_factor small job high factor", fun test_size_factor_small/0},
        {"size_factor large job low factor", fun test_size_factor_large/0},
        {"size_factor cluster size normalization", fun test_size_factor_cluster_norm/0},
        {"size_factor cpu impact", fun test_size_factor_cpu_impact/0},
        {"size_factor minimal cluster size", fun test_size_factor_min_cluster/0},
        {"size_factor missing keys defaults", fun test_size_factor_defaults/0},
        {"partition_factor defaults to 0.5", fun test_partition_factor_default/0},
        {"partition_factor with missing partition key", fun test_partition_factor_missing/0},
        {"nice_factor zero", fun test_nice_factor_zero/0},
        {"nice_factor negative nice", fun test_nice_factor_negative/0},
        {"nice_factor positive nice", fun test_nice_factor_positive/0},
        {"nice_factor max bounds", fun test_nice_factor_max_bounds/0},
        {"nice_factor default when missing", fun test_nice_factor_missing/0},
        {"qos_factor high", fun test_qos_factor_high/0},
        {"qos_factor normal", fun test_qos_factor_normal/0},
        {"qos_factor low", fun test_qos_factor_low/0},
        {"qos_factor unknown", fun test_qos_factor_unknown/0},
        {"qos_factor missing key", fun test_qos_factor_missing/0},
        {"fairshare_factor with context", fun test_fairshare_factor_context/0},
        {"fairshare_factor default without module", fun test_fairshare_factor_default/0},
        {"calculate_factors returns all keys", fun test_calculate_factors_keys/0},
        {"calculate_factors with empty job", fun test_calculate_factors_empty/0},
        {"calculate_priority returns integer", fun test_calculate_priority_integer/0},
        {"calculate_priority with context", fun test_calculate_priority_context/0},
        {"calculate_priority clamped to min", fun test_calculate_priority_min_clamp/0},
        {"calculate_priority empty job", fun test_calculate_priority_empty_job/0},
        {"calculate_priority high prio job", fun test_calculate_priority_high_prio/0},
        {"calculate_priority low prio job", fun test_calculate_priority_low_prio/0},
        {"get_priority_factors returns map", fun test_get_priority_factors/0},
        {"get_priority_factors all keys present", fun test_get_priority_factors_keys/0},
        {"get_priority_factors empty job", fun test_get_priority_factors_empty/0},
        {"weights affect priority", fun test_weights_affect_priority/0},
        {"zero weights produce min priority", fun test_zero_weights/0}
     ]}.

setup() ->
    %% Clean up any leftover weights table from previous runs
    case ets:whereis(flurm_priority_weights) of
        undefined -> ok;
        _ -> ets:delete(flurm_priority_weights)
    end,
    %% Now create it fresh via the module's own function
    flurm_priority:ensure_weights_table(),
    ok.

cleanup(_) ->
    case ets:whereis(flurm_priority_weights) of
        undefined -> ok;
        _ -> ets:delete(flurm_priority_weights)
    end,
    ok.

%%====================================================================
%% Helper
%%====================================================================

make_job() ->
    make_job(#{}).

make_job(Overrides) ->
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
        submit_time => Now - 300,
        nice => 0,
        qos => <<"normal">>
    },
    maps:merge(Defaults, Overrides).

%%====================================================================
%% ensure_weights_table/0 Tests
%%====================================================================

test_ensure_weights_table() ->
    %% Table was created in setup - verify it exists
    ?assertNotEqual(undefined, ets:whereis(flurm_priority_weights)).

test_ensure_weights_table_idempotent() ->
    %% Calling again should not crash (table already exists)
    ?assertEqual(ok, flurm_priority:ensure_weights_table()),
    ?assertNotEqual(undefined, ets:whereis(flurm_priority_weights)).

%%====================================================================
%% get_weights/0 and set_weights/1 Tests
%%====================================================================

test_get_weights_defaults() ->
    W = flurm_priority:get_weights(),
    ?assert(is_map(W)),
    ?assertEqual(1000, maps:get(age, W)),
    ?assertEqual(200, maps:get(job_size, W)),
    ?assertEqual(100, maps:get(partition, W)),
    ?assertEqual(500, maps:get(qos, W)),
    ?assertEqual(100, maps:get(nice, W)),
    ?assertEqual(5000, maps:get(fairshare, W)).

test_set_and_get_weights() ->
    New = #{age => 2000, job_size => 400, partition => 200,
            qos => 1000, nice => 200, fairshare => 3000},
    ok = flurm_priority:set_weights(New),
    W = flurm_priority:get_weights(),
    ?assertEqual(2000, maps:get(age, W)),
    ?assertEqual(400, maps:get(job_size, W)),
    ?assertEqual(3000, maps:get(fairshare, W)).

test_set_weights_partial() ->
    ok = flurm_priority:set_weights(#{age => 9999}),
    W = flurm_priority:get_weights(),
    ?assertEqual(9999, maps:get(age, W)),
    %% Other keys are gone because set_weights replaces whole map
    ?assertEqual(undefined, maps:get(job_size, W, undefined)).

%%====================================================================
%% age_factor/2 Tests
%%====================================================================

test_age_factor_zero() ->
    Now = erlang:system_time(second),
    Job = make_job(#{submit_time => Now}),
    F = flurm_priority:age_factor(Job, Now),
    ?assert(is_float(F)),
    ?assert(F >= 0.0),
    ?assert(F < 0.001).

test_age_factor_increases() ->
    Now = erlang:system_time(second),
    Job1 = make_job(#{submit_time => Now - 3600}),
    Job2 = make_job(#{submit_time => Now - 86400}),
    F1 = flurm_priority:age_factor(Job1, Now),
    F2 = flurm_priority:age_factor(Job2, Now),
    ?assert(F2 > F1),
    ?assert(F1 > 0.0).

test_age_factor_capped() ->
    Now = erlang:system_time(second),
    %% Job older than MAX_AGE (604800s = 7 days)
    Job = make_job(#{submit_time => Now - 2000000}),
    F = flurm_priority:age_factor(Job, Now),
    ?assert(F < 1.0),
    ?assert(F > 0.99).

test_age_factor_tuple_timestamp() ->
    Now = erlang:system_time(second),
    %% Construct {MegaSecs, Secs, MicroSecs} format
    MegaSecs = (Now - 7200) div 1000000,
    Secs = (Now - 7200) rem 1000000,
    Job = make_job(#{submit_time => {MegaSecs, Secs, 0}}),
    F = flurm_priority:age_factor(Job, Now),
    ?assert(F > 0.0),
    ?assert(F < 1.0).

test_age_factor_invalid_timestamp() ->
    Now = erlang:system_time(second),
    Job = make_job(#{submit_time => not_a_timestamp}),
    F = flurm_priority:age_factor(Job, Now),
    %% Invalid falls through to default = Now, so age = 0
    ?assertEqual(0.0, F).

test_age_factor_future() ->
    Now = erlang:system_time(second),
    Job = make_job(#{submit_time => Now + 10000}),
    F = flurm_priority:age_factor(Job, Now),
    %% max(0, negative) = 0, so factor = 0
    ?assertEqual(0.0, F).

test_age_factor_missing_key() ->
    Now = erlang:system_time(second),
    %% No submit_time key - defaults to Now
    Job = #{},
    F = flurm_priority:age_factor(Job, Now),
    ?assertEqual(0.0, F).

%%====================================================================
%% size_factor/2 Tests
%%====================================================================

test_size_factor_small() ->
    Job = make_job(#{num_nodes => 1, num_cpus => 4}),
    F = flurm_priority:size_factor(Job, 100),
    ?assert(F > 0.5),
    ?assert(F =< 1.0).

test_size_factor_large() ->
    Job = make_job(#{num_nodes => 80, num_cpus => 64}),
    F = flurm_priority:size_factor(Job, 100),
    ?assert(F < 0.3).

test_size_factor_cluster_norm() ->
    Job = make_job(#{num_nodes => 10, num_cpus => 8}),
    F1 = flurm_priority:size_factor(Job, 100),
    F2 = flurm_priority:size_factor(Job, 1000),
    %% Same job is proportionally smaller in bigger cluster
    ?assert(F2 > F1).

test_size_factor_cpu_impact() ->
    Job1 = make_job(#{num_nodes => 1, num_cpus => 1}),
    Job2 = make_job(#{num_nodes => 1, num_cpus => 64}),
    F1 = flurm_priority:size_factor(Job1, 100),
    F2 = flurm_priority:size_factor(Job2, 100),
    %% More CPUs = lower factor
    ?assert(F1 > F2).

test_size_factor_min_cluster() ->
    %% ClusterSize of 0 should be safe (max(1, ClusterSize))
    Job = make_job(#{num_nodes => 1, num_cpus => 1}),
    F = flurm_priority:size_factor(Job, 0),
    ?assert(is_float(F)).

test_size_factor_defaults() ->
    %% Missing num_nodes and num_cpus default to 1
    Job = #{},
    F = flurm_priority:size_factor(Job, 100),
    ?assert(is_float(F)),
    ?assert(F > 0.0).

%%====================================================================
%% partition_factor/1 Tests
%%====================================================================

test_partition_factor_default() ->
    %% flurm_partition_registry is not running, catch returns default 0.5
    Job = make_job(#{partition => <<"compute">>}),
    F = flurm_priority:partition_factor(Job),
    ?assertEqual(0.5, F).

test_partition_factor_missing() ->
    %% No partition key - defaults to <<"default">>
    Job = #{},
    F = flurm_priority:partition_factor(Job),
    ?assertEqual(0.5, F).

%%====================================================================
%% nice_factor/1 Tests
%%====================================================================

test_nice_factor_zero() ->
    Job = make_job(#{nice => 0}),
    F = flurm_priority:nice_factor(Job),
    ?assertEqual(0.0, F).

test_nice_factor_negative() ->
    Job = make_job(#{nice => -5000}),
    F = flurm_priority:nice_factor(Job),
    ?assertEqual(0.5, F).

test_nice_factor_positive() ->
    Job = make_job(#{nice => 5000}),
    F = flurm_priority:nice_factor(Job),
    ?assertEqual(-0.5, F).

test_nice_factor_max_bounds() ->
    Job1 = make_job(#{nice => -10000}),
    Job2 = make_job(#{nice => 10000}),
    F1 = flurm_priority:nice_factor(Job1),
    F2 = flurm_priority:nice_factor(Job2),
    ?assertEqual(1.0, F1),
    ?assertEqual(-1.0, F2).

test_nice_factor_missing() ->
    %% No nice key - defaults to 0
    Job = #{},
    F = flurm_priority:nice_factor(Job),
    ?assertEqual(0.0, F).

%%====================================================================
%% qos_factor/1 Tests (TEST export)
%%====================================================================

test_qos_factor_high() ->
    Job = make_job(#{qos => <<"high">>}),
    ?assertEqual(1.0, flurm_priority:qos_factor(Job)).

test_qos_factor_normal() ->
    Job = make_job(#{qos => <<"normal">>}),
    ?assertEqual(0.5, flurm_priority:qos_factor(Job)).

test_qos_factor_low() ->
    Job = make_job(#{qos => <<"low">>}),
    ?assertEqual(0.2, flurm_priority:qos_factor(Job)).

test_qos_factor_unknown() ->
    Job = make_job(#{qos => <<"premium">>}),
    ?assertEqual(0.5, flurm_priority:qos_factor(Job)).

test_qos_factor_missing() ->
    %% No qos key - defaults to <<"normal">> = 0.5
    Job = #{},
    ?assertEqual(0.5, flurm_priority:qos_factor(Job)).

%%====================================================================
%% fairshare_factor/2 Tests (TEST export)
%%====================================================================

test_fairshare_factor_context() ->
    Job = make_job(),
    Context = #{fairshare => 0.75},
    F = flurm_priority:fairshare_factor(Job, Context),
    ?assertEqual(0.75, F).

test_fairshare_factor_default() ->
    %% No fairshare in context, flurm_fairshare not running => 0.5 default
    Job = make_job(),
    F = flurm_priority:fairshare_factor(Job, #{}),
    ?assertEqual(0.5, F).

%%====================================================================
%% calculate_factors/3 Tests (TEST export)
%%====================================================================

test_calculate_factors_keys() ->
    Job = make_job(),
    W = flurm_priority:get_weights(),
    Factors = flurm_priority:calculate_factors(Job, #{}, W),
    ?assert(is_map(Factors)),
    ?assert(maps:is_key(age, Factors)),
    ?assert(maps:is_key(job_size, Factors)),
    ?assert(maps:is_key(partition, Factors)),
    ?assert(maps:is_key(qos, Factors)),
    ?assert(maps:is_key(nice, Factors)),
    ?assert(maps:is_key(fairshare, Factors)).

test_calculate_factors_empty() ->
    W = flurm_priority:get_weights(),
    Factors = flurm_priority:calculate_factors(#{}, #{}, W),
    ?assert(is_map(Factors)),
    %% All factors should be valid numbers
    maps:foreach(fun(_K, V) ->
        ?assert(is_float(V) orelse is_integer(V))
    end, Factors).

%%====================================================================
%% calculate_priority/1 and /2 Tests
%%====================================================================

test_calculate_priority_integer() ->
    Job = make_job(),
    P = flurm_priority:calculate_priority(Job),
    ?assert(is_integer(P)).

test_calculate_priority_context() ->
    Job = make_job(),
    P = flurm_priority:calculate_priority(Job, #{fairshare => 0.9}),
    ?assert(is_integer(P)),
    ?assert(P >= ?MIN_PRIORITY).

test_calculate_priority_min_clamp() ->
    %% Even with bad factors, priority should not go below MIN_PRIORITY
    Job = make_job(#{nice => 10000, qos => <<"low">>}),
    P = flurm_priority:calculate_priority(Job),
    ?assert(P >= ?MIN_PRIORITY).

test_calculate_priority_empty_job() ->
    P = flurm_priority:calculate_priority(#{}),
    ?assert(is_integer(P)),
    ?assert(P >= ?MIN_PRIORITY).

test_calculate_priority_high_prio() ->
    Now = erlang:system_time(second),
    Job = make_job(#{
        submit_time => Now - 604800,  %% 7 days old
        num_nodes => 1,
        num_cpus => 1,
        qos => <<"high">>,
        nice => -10000
    }),
    P = flurm_priority:calculate_priority(Job, #{fairshare => 1.0}),
    ?assert(is_integer(P)),
    ?assert(P > 1000).

test_calculate_priority_low_prio() ->
    Now = erlang:system_time(second),
    Job = make_job(#{
        submit_time => Now,
        num_nodes => 100,
        num_cpus => 64,
        qos => <<"low">>,
        nice => 10000
    }),
    P = flurm_priority:calculate_priority(Job, #{fairshare => 0.0}),
    ?assert(is_integer(P)),
    ?assert(P >= ?MIN_PRIORITY).

%%====================================================================
%% get_priority_factors/1 Tests
%%====================================================================

test_get_priority_factors() ->
    Job = make_job(),
    Factors = flurm_priority:get_priority_factors(Job),
    ?assert(is_map(Factors)),
    ?assert(maps:size(Factors) >= 6).

test_get_priority_factors_keys() ->
    Job = make_job(#{qos => <<"high">>, nice => -2000}),
    Factors = flurm_priority:get_priority_factors(Job),
    ?assertEqual(1.0, maps:get(qos, Factors)),
    ?assertEqual(0.2, maps:get(nice, Factors)),
    ?assert(maps:get(age, Factors) >= 0.0),
    ?assert(maps:get(job_size, Factors) >= 0.0),
    ?assertEqual(0.5, maps:get(partition, Factors)),
    ?assertEqual(0.5, maps:get(fairshare, Factors)).

test_get_priority_factors_empty() ->
    Factors = flurm_priority:get_priority_factors(#{}),
    ?assert(is_map(Factors)),
    %% With empty job: age=0.0, nice=0.0, qos=0.5, etc.
    ?assertEqual(0.0, maps:get(age, Factors)),
    ?assertEqual(0.0, maps:get(nice, Factors)),
    ?assertEqual(0.5, maps:get(qos, Factors)).

%%====================================================================
%% Priority weight impact tests
%%====================================================================

test_weights_affect_priority() ->
    Job = make_job(),
    %% Calculate with defaults
    P1 = flurm_priority:calculate_priority(Job),

    %% Double the age weight
    W = flurm_priority:get_weights(),
    ok = flurm_priority:set_weights(W#{age := maps:get(age, W) * 10}),
    P2 = flurm_priority:calculate_priority(Job),

    %% Priority should differ (unless age factor is 0, which it shouldn't
    %% be for our test job submitted 300s ago)
    ?assertNotEqual(P1, P2).

test_zero_weights() ->
    ok = flurm_priority:set_weights(#{
        age => 0, job_size => 0, partition => 0,
        qos => 0, nice => 0, fairshare => 0
    }),
    Job = make_job(),
    P = flurm_priority:calculate_priority(Job),
    %% With all weights = 0, priority sum = 0, clamped to MIN_PRIORITY
    ?assertEqual(?MIN_PRIORITY, P).

%%====================================================================
%% Additional edge case tests (standalone, no fixture needed)
%%====================================================================

nice_factor_small_values_test() ->
    %% Small nice value
    Job = #{nice => 1},
    F = flurm_priority:nice_factor(Job),
    ?assert(F < 0.0),
    ?assert(F > -0.001).

nice_factor_large_negative_test() ->
    Job = #{nice => -9999},
    F = flurm_priority:nice_factor(Job),
    ?assert(F > 0.99).

qos_factor_binary_match_test() ->
    %% Ensure exact binary matching
    ?assertEqual(1.0, flurm_priority:qos_factor(#{qos => <<"high">>})),
    ?assertEqual(0.5, flurm_priority:qos_factor(#{qos => <<"normal">>})),
    ?assertEqual(0.2, flurm_priority:qos_factor(#{qos => <<"low">>})),
    ?assertEqual(0.5, flurm_priority:qos_factor(#{qos => <<"HIGH">>})),
    ?assertEqual(0.5, flurm_priority:qos_factor(#{qos => <<>>})).

size_factor_single_node_single_cpu_test() ->
    Job = #{num_nodes => 1, num_cpus => 1},
    F = flurm_priority:size_factor(Job, 1000),
    %% 1 - 0.5*(1/1000) - 0.5*min(1.0, 1/64)
    Expected = 1.0 - (0.5 * (1/1000)) - (0.5 * (1/64)),
    ?assert(abs(F - Expected) < 0.0001).

size_factor_full_cluster_test() ->
    Job = #{num_nodes => 100, num_cpus => 64},
    F = flurm_priority:size_factor(Job, 100),
    %% 1 - 0.5*(100/100) - 0.5*min(1.0, 64/64) = 1 - 0.5 - 0.5 = 0.0
    ?assert(abs(F) < 0.0001).

age_factor_one_second_test() ->
    Now = erlang:system_time(second),
    Job = #{submit_time => Now - 1},
    F = flurm_priority:age_factor(Job, Now),
    %% 1.0 - exp(-1/86400) is very small but > 0
    ?assert(F > 0.0),
    ?assert(F < 0.001).

age_factor_exactly_one_day_test() ->
    Now = erlang:system_time(second),
    Job = #{submit_time => Now - 86400},
    F = flurm_priority:age_factor(Job, Now),
    %% 1.0 - exp(-1) = ~0.6321
    Expected = 1.0 - math:exp(-1.0),
    ?assert(abs(F - Expected) < 0.001).
