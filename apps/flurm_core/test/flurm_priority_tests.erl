%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_priority module
%%%
%%% Tests the multi-factor priority calculation system including:
%%% - Individual priority factors (age, size, partition, QOS, nice, fairshare)
%%% - Weight configuration
%%% - Priority calculation with various job configurations
%%% - Edge cases and boundary conditions
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_priority_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Clean up weights table to ensure fresh state
    catch ets:delete(flurm_priority_weights),
    ok.

cleanup(_) ->
    catch ets:delete(flurm_priority_weights),
    ok.

%%====================================================================
%% Weight Configuration Tests
%%====================================================================

weights_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get default weights", fun test_default_weights/0},
        {"set custom weights", fun test_set_weights/0},
        {"weight persistence", fun test_weight_persistence/0},
        {"partial weight override", fun test_partial_weight_override/0}
     ]}.

test_default_weights() ->
    Weights = flurm_priority:get_weights(),
    ?assert(is_map(Weights)),
    ?assertEqual(1000, maps:get(age, Weights)),
    ?assertEqual(200, maps:get(job_size, Weights)),
    ?assertEqual(100, maps:get(partition, Weights)),
    ?assertEqual(500, maps:get(qos, Weights)),
    ?assertEqual(100, maps:get(nice, Weights)),
    ?assertEqual(5000, maps:get(fairshare, Weights)).

test_set_weights() ->
    NewWeights = #{
        age => 2000,
        job_size => 500,
        partition => 200,
        qos => 1000,
        nice => 50,
        fairshare => 3000
    },
    ok = flurm_priority:set_weights(NewWeights),
    Retrieved = flurm_priority:get_weights(),
    ?assertEqual(2000, maps:get(age, Retrieved)),
    ?assertEqual(500, maps:get(job_size, Retrieved)),
    ?assertEqual(3000, maps:get(fairshare, Retrieved)).

test_weight_persistence() ->
    NewWeights = #{age => 5000, job_size => 100, partition => 50, qos => 250, nice => 25, fairshare => 1000},
    ok = flurm_priority:set_weights(NewWeights),

    %% Get weights multiple times to verify persistence
    W1 = flurm_priority:get_weights(),
    W2 = flurm_priority:get_weights(),
    ?assertEqual(W1, W2),
    ?assertEqual(5000, maps:get(age, W1)).

test_partial_weight_override() ->
    %% Set only some weights
    PartialWeights = #{age => 9999, qos => 9999},
    ok = flurm_priority:set_weights(PartialWeights),
    Retrieved = flurm_priority:get_weights(),
    ?assertEqual(9999, maps:get(age, Retrieved)),
    ?assertEqual(9999, maps:get(qos, Retrieved)),
    %% Other weights should be undefined in the partial map
    ?assertEqual(undefined, maps:get(job_size, Retrieved, undefined)).

%%====================================================================
%% Age Factor Tests
%%====================================================================

age_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"age factor zero for just submitted", fun test_age_factor_zero/0},
        {"age factor increases with time", fun test_age_factor_increases/0},
        {"age factor capped at max", fun test_age_factor_capped/0},
        {"age factor with erlang timestamp format", fun test_age_factor_erlang_timestamp/0},
        {"age factor with missing submit time", fun test_age_factor_missing_time/0},
        {"age factor with invalid submit time", fun test_age_factor_invalid_time/0}
     ]}.

test_age_factor_zero() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now},
    Factor = flurm_priority:age_factor(JobInfo, Now),
    %% Should be very close to 0 for just submitted job
    ?assert(Factor < 0.01).

test_age_factor_increases() ->
    Now = erlang:system_time(second),
    JobOld = #{submit_time => Now - 86400},  % 1 day ago
    JobNew = #{submit_time => Now - 3600},   % 1 hour ago

    OldFactor = flurm_priority:age_factor(JobOld, Now),
    NewFactor = flurm_priority:age_factor(JobNew, Now),

    %% Older job should have higher age factor
    ?assert(OldFactor > NewFactor).

test_age_factor_capped() ->
    Now = erlang:system_time(second),
    %% Job from 30 days ago
    VeryOldJob = #{submit_time => Now - (30 * 86400)},
    %% Job from 7 days ago (max age cap)
    CappedJob = #{submit_time => Now - (7 * 86400)},

    VeryOldFactor = flurm_priority:age_factor(VeryOldJob, Now),
    CappedFactor = flurm_priority:age_factor(CappedJob, Now),

    %% Both should be at or near the cap
    ?assert(VeryOldFactor >= 0.99),
    ?assert(CappedFactor >= 0.99),
    %% They should be approximately equal since both are at the cap
    ?assert(abs(VeryOldFactor - CappedFactor) < 0.01).

test_age_factor_erlang_timestamp() ->
    Now = erlang:system_time(second),
    %% Use erlang timestamp format {MegaSecs, Secs, MicroSecs}
    MegaSecs = (Now - 3600) div 1000000,
    Secs = (Now - 3600) rem 1000000,
    JobInfo = #{submit_time => {MegaSecs, Secs, 0}},
    Factor = flurm_priority:age_factor(JobInfo, Now),
    %% Should be a reasonable value (1 hour old)
    ?assert(Factor > 0.0),
    ?assert(Factor < 0.5).

test_age_factor_missing_time() ->
    Now = erlang:system_time(second),
    %% Job with no submit_time
    JobInfo = #{},
    Factor = flurm_priority:age_factor(JobInfo, Now),
    %% Should default to 0 (as if just submitted)
    ?assert(Factor < 0.01).

test_age_factor_invalid_time() ->
    Now = erlang:system_time(second),
    %% Job with invalid submit_time
    JobInfo = #{submit_time => invalid},
    Factor = flurm_priority:age_factor(JobInfo, Now),
    %% Should default to 0
    ?assert(Factor < 0.01).

%%====================================================================
%% Size Factor Tests
%%====================================================================

size_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"small job gets higher factor", fun test_size_factor_small_job/0},
        {"large job gets lower factor", fun test_size_factor_large_job/0},
        {"size factor with defaults", fun test_size_factor_defaults/0},
        {"size factor normalization", fun test_size_factor_normalization/0}
     ]}.

test_size_factor_small_job() ->
    SmallJob = #{num_nodes => 1, num_cpus => 4},
    LargeJob = #{num_nodes => 50, num_cpus => 400},
    ClusterSize = 100,

    SmallFactor = flurm_priority:size_factor(SmallJob, ClusterSize),
    LargeFactor = flurm_priority:size_factor(LargeJob, ClusterSize),

    %% Smaller jobs should get higher factor (priority boost)
    ?assert(SmallFactor > LargeFactor).

test_size_factor_large_job() ->
    Job = #{num_nodes => 100, num_cpus => 800},
    ClusterSize = 100,
    Factor = flurm_priority:size_factor(Job, ClusterSize),
    %% Large job using full cluster should have low factor
    ?assert(Factor < 0.5).

test_size_factor_defaults() ->
    %% Job with no size specified
    Job = #{},
    ClusterSize = 100,
    Factor = flurm_priority:size_factor(Job, ClusterSize),
    %% Should use defaults (1 node, 1 cpu) and get high factor
    ?assert(Factor > 0.9).

test_size_factor_normalization() ->
    Job = #{num_nodes => 10, num_cpus => 64},
    %% Test with different cluster sizes
    SmallCluster = 10,
    LargeCluster = 1000,

    SmallClusterFactor = flurm_priority:size_factor(Job, SmallCluster),
    LargeClusterFactor = flurm_priority:size_factor(Job, LargeCluster),

    %% Same job should get higher factor in larger cluster
    ?assert(LargeClusterFactor > SmallClusterFactor).

%%====================================================================
%% Partition Factor Tests
%%====================================================================

partition_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"partition factor default", fun test_partition_factor_default/0},
        {"partition factor with missing partition", fun test_partition_factor_missing/0}
     ]}.

test_partition_factor_default() ->
    Job = #{partition => <<"default">>},
    Factor = flurm_priority:partition_factor(Job),
    %% Should return default 0.5 when partition registry not available
    ?assertEqual(0.5, Factor).

test_partition_factor_missing() ->
    %% Job with no partition specified
    Job = #{},
    Factor = flurm_priority:partition_factor(Job),
    %% Should use default partition and return 0.5
    ?assertEqual(0.5, Factor).

%%====================================================================
%% Nice Factor Tests
%%====================================================================

nice_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"nice factor zero", fun test_nice_factor_zero/0},
        {"nice factor negative (high priority)", fun test_nice_factor_negative/0},
        {"nice factor positive (low priority)", fun test_nice_factor_positive/0},
        {"nice factor default", fun test_nice_factor_default/0}
     ]}.

test_nice_factor_zero() ->
    Job = #{nice => 0},
    Factor = flurm_priority:nice_factor(Job),
    ?assertEqual(0.0, Factor).

test_nice_factor_negative() ->
    %% Negative nice = higher priority (like Unix)
    Job = #{nice => -5000},
    Factor = flurm_priority:nice_factor(Job),
    %% Should be positive (inverted)
    ?assertEqual(0.5, Factor).

test_nice_factor_positive() ->
    %% Positive nice = lower priority
    Job = #{nice => 5000},
    Factor = flurm_priority:nice_factor(Job),
    %% Should be negative (inverted)
    ?assertEqual(-0.5, Factor).

test_nice_factor_default() ->
    %% Job with no nice value
    Job = #{},
    Factor = flurm_priority:nice_factor(Job),
    ?assertEqual(0.0, Factor).

%%====================================================================
%% Calculate Priority Tests
%%====================================================================

priority_calculation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"calculate basic priority", fun test_calculate_basic_priority/0},
        {"calculate priority with context", fun test_calculate_priority_with_context/0},
        {"calculate priority clamped to range", fun test_priority_clamped/0},
        {"priority factors returned", fun test_get_priority_factors/0},
        {"high QOS increases priority", fun test_high_qos_priority/0},
        {"low QOS decreases priority", fun test_low_qos_priority/0},
        {"old job has higher priority", fun test_old_job_priority/0}
     ]}.

test_calculate_basic_priority() ->
    Now = erlang:system_time(second),
    JobInfo = #{
        submit_time => Now,
        num_nodes => 1,
        num_cpus => 4,
        partition => <<"default">>,
        qos => <<"normal">>,
        nice => 0
    },
    Priority = flurm_priority:calculate_priority(JobInfo),
    ?assert(is_integer(Priority)),
    ?assert(Priority >= ?MIN_PRIORITY),
    ?assert(Priority =< ?MAX_PRIORITY * 100).

test_calculate_priority_with_context() ->
    Now = erlang:system_time(second),
    JobInfo = #{
        submit_time => Now,
        num_nodes => 1,
        num_cpus => 4,
        qos => <<"normal">>
    },
    Context = #{fairshare => 0.8},  % Good fairshare
    Priority = flurm_priority:calculate_priority(JobInfo, Context),
    ?assert(is_integer(Priority)),
    ?assert(Priority > 0).

test_priority_clamped() ->
    Now = erlang:system_time(second),
    %% Job with extreme values
    JobInfo = #{
        submit_time => Now - (30 * 86400),  % Very old
        num_nodes => 1,
        num_cpus => 1,
        qos => <<"high">>,
        nice => -10000
    },
    Priority = flurm_priority:calculate_priority(JobInfo),
    %% Should be clamped to max
    ?assert(Priority =< ?MAX_PRIORITY * 100).

test_get_priority_factors() ->
    Now = erlang:system_time(second),
    JobInfo = #{
        submit_time => Now,
        num_nodes => 4,
        num_cpus => 32,
        partition => <<"gpu">>,
        qos => <<"high">>,
        nice => -1000
    },
    Factors = flurm_priority:get_priority_factors(JobInfo),
    ?assert(is_map(Factors)),
    ?assert(maps:is_key(age, Factors)),
    ?assert(maps:is_key(job_size, Factors)),
    ?assert(maps:is_key(partition, Factors)),
    ?assert(maps:is_key(qos, Factors)),
    ?assert(maps:is_key(nice, Factors)),
    ?assert(maps:is_key(fairshare, Factors)).

test_high_qos_priority() ->
    Now = erlang:system_time(second),
    HighQOSJob = #{
        submit_time => Now,
        num_nodes => 1,
        num_cpus => 4,
        qos => <<"high">>
    },
    NormalQOSJob = #{
        submit_time => Now,
        num_nodes => 1,
        num_cpus => 4,
        qos => <<"normal">>
    },
    HighPriority = flurm_priority:calculate_priority(HighQOSJob),
    NormalPriority = flurm_priority:calculate_priority(NormalQOSJob),
    ?assert(HighPriority > NormalPriority).

test_low_qos_priority() ->
    Now = erlang:system_time(second),
    NormalQOSJob = #{
        submit_time => Now,
        num_nodes => 1,
        num_cpus => 4,
        qos => <<"normal">>
    },
    LowQOSJob = #{
        submit_time => Now,
        num_nodes => 1,
        num_cpus => 4,
        qos => <<"low">>
    },
    NormalPriority = flurm_priority:calculate_priority(NormalQOSJob),
    LowPriority = flurm_priority:calculate_priority(LowQOSJob),
    ?assert(NormalPriority > LowPriority).

test_old_job_priority() ->
    Now = erlang:system_time(second),
    NewJob = #{
        submit_time => Now,
        num_nodes => 1,
        num_cpus => 4,
        qos => <<"normal">>
    },
    OldJob = #{
        submit_time => Now - 86400,  % 1 day old
        num_nodes => 1,
        num_cpus => 4,
        qos => <<"normal">>
    },
    NewPriority = flurm_priority:calculate_priority(NewJob),
    OldPriority = flurm_priority:calculate_priority(OldJob),
    ?assert(OldPriority > NewPriority).

%%====================================================================
%% QOS Factor Tests
%%====================================================================

qos_factor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"qos factor high", fun test_qos_factor_high/0},
        {"qos factor normal", fun test_qos_factor_normal/0},
        {"qos factor low", fun test_qos_factor_low/0},
        {"qos factor unknown", fun test_qos_factor_unknown/0}
     ]}.

test_qos_factor_high() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now, qos => <<"high">>},
    Factors = flurm_priority:get_priority_factors(JobInfo),
    ?assertEqual(1.0, maps:get(qos, Factors)).

test_qos_factor_normal() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now, qos => <<"normal">>},
    Factors = flurm_priority:get_priority_factors(JobInfo),
    ?assertEqual(0.5, maps:get(qos, Factors)).

test_qos_factor_low() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now, qos => <<"low">>},
    Factors = flurm_priority:get_priority_factors(JobInfo),
    ?assertEqual(0.2, maps:get(qos, Factors)).

test_qos_factor_unknown() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now, qos => <<"unknown_qos">>},
    Factors = flurm_priority:get_priority_factors(JobInfo),
    %% Unknown QOS should default to 0.5
    ?assertEqual(0.5, maps:get(qos, Factors)).

%%====================================================================
%% Edge Cases and Boundary Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"empty job info", fun test_empty_job_info/0},
        {"nil values handled", fun test_nil_values/0},
        {"extreme weight values", fun test_extreme_weights/0},
        {"zero cluster size handled", fun test_zero_cluster_size/0}
     ]}.

test_empty_job_info() ->
    %% Should handle empty job info gracefully
    Priority = flurm_priority:calculate_priority(#{}),
    ?assert(is_integer(Priority)),
    ?assert(Priority >= ?MIN_PRIORITY).

test_nil_values() ->
    %% Test with minimal/empty job info to verify defaults are used
    %% Note: some values like num_nodes/num_cpus need to be numeric
    %% for size calculations, so we test with empty map and explicit defaults
    JobInfo1 = #{},  % Empty map - uses all defaults
    Priority1 = flurm_priority:calculate_priority(JobInfo1),
    ?assert(is_integer(Priority1)),

    %% Test with only some fields defined
    Now = erlang:system_time(second),
    JobInfo2 = #{submit_time => Now, num_nodes => 1, num_cpus => 1},
    Priority2 = flurm_priority:calculate_priority(JobInfo2),
    ?assert(is_integer(Priority2)).

test_extreme_weights() ->
    %% Set very high weights
    ExtremeWeights = #{
        age => 1000000,
        job_size => 1000000,
        partition => 1000000,
        qos => 1000000,
        nice => 1000000,
        fairshare => 1000000
    },
    ok = flurm_priority:set_weights(ExtremeWeights),

    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now, qos => <<"high">>},
    Priority = flurm_priority:calculate_priority(JobInfo),

    %% Should still be clamped
    ?assert(Priority =< ?MAX_PRIORITY * 100).

test_zero_cluster_size() ->
    %% size_factor should handle zero cluster size
    Job = #{num_nodes => 1, num_cpus => 4},
    %% Note: The function uses max(1, ClusterSize) internally
    %% but we can test the internal function directly
    Factor = flurm_priority:size_factor(Job, 0),
    ?assert(is_float(Factor)).

%%====================================================================
%% Fairshare Integration Tests
%%====================================================================

fairshare_integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"fairshare from context", fun test_fairshare_from_context/0},
        {"fairshare default", fun test_fairshare_default/0}
     ]}.

test_fairshare_from_context() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now, qos => <<"normal">>},
    Context = #{fairshare => 1.0},  % Excellent fairshare
    Factors = flurm_priority:calculate_priority(JobInfo, Context),
    ?assert(is_integer(Factors)).

test_fairshare_default() ->
    Now = erlang:system_time(second),
    JobInfo = #{submit_time => Now, user => <<"testuser">>, account => <<"testaccount">>},
    %% Without context, should use default fairshare
    Priority = flurm_priority:calculate_priority(JobInfo),
    ?assert(is_integer(Priority)).

%%====================================================================
%% Weight Impact Tests
%%====================================================================

weight_impact_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"age weight impacts priority", fun test_age_weight_impact/0},
        {"qos weight impacts priority", fun test_qos_weight_impact/0},
        {"nice weight impacts priority", fun test_nice_weight_impact/0}
     ]}.

test_age_weight_impact() ->
    Now = erlang:system_time(second),
    OldJob = #{submit_time => Now - 86400, qos => <<"normal">>},

    %% Low age weight
    ok = flurm_priority:set_weights(#{age => 100, job_size => 200, partition => 100, qos => 500, nice => 100, fairshare => 5000}),
    LowAgePriority = flurm_priority:calculate_priority(OldJob),

    %% High age weight
    ok = flurm_priority:set_weights(#{age => 10000, job_size => 200, partition => 100, qos => 500, nice => 100, fairshare => 5000}),
    HighAgePriority = flurm_priority:calculate_priority(OldJob),

    %% Higher age weight should result in higher priority for old job
    ?assert(HighAgePriority > LowAgePriority).

test_qos_weight_impact() ->
    Now = erlang:system_time(second),
    HighQOSJob = #{submit_time => Now, qos => <<"high">>},
    NormalQOSJob = #{submit_time => Now, qos => <<"normal">>},

    %% High QOS weight
    ok = flurm_priority:set_weights(#{age => 100, job_size => 100, partition => 100, qos => 10000, nice => 100, fairshare => 100}),

    HighPriority = flurm_priority:calculate_priority(HighQOSJob),
    NormalPriority = flurm_priority:calculate_priority(NormalQOSJob),

    %% High QOS should have significantly higher priority
    ?assert(HighPriority > NormalPriority * 1.5).

test_nice_weight_impact() ->
    Now = erlang:system_time(second),
    NiceJob = #{submit_time => Now, qos => <<"normal">>, nice => -5000},  % Higher priority
    MeanJob = #{submit_time => Now, qos => <<"normal">>, nice => 5000},   % Lower priority

    %% With nice weight
    ok = flurm_priority:set_weights(#{age => 100, job_size => 100, partition => 100, qos => 100, nice => 5000, fairshare => 100}),

    NicePriority = flurm_priority:calculate_priority(NiceJob),
    MeanPriority = flurm_priority:calculate_priority(MeanJob),

    %% Nice (negative) job should have higher priority
    ?assert(NicePriority > MeanPriority).

%%====================================================================
%% Recalculate All Tests (with job registry)
%%====================================================================

recalculate_all_test_() ->
    {setup,
     fun() ->
         %% Start the job registry and supervisor
         application:ensure_all_started(sasl),
         {ok, RegistryPid} = flurm_job_registry:start_link(),
         {ok, SupPid} = flurm_job_sup:start_link(),
         #{registry => RegistryPid, supervisor => SupPid}
     end,
     fun(#{registry := RegistryPid, supervisor := SupPid}) ->
         lists:foreach(fun(Pid) ->
             case is_process_alive(Pid) of
                 true ->
                     Ref = monitor(process, Pid),
                     unlink(Pid),
                     catch gen_server:stop(Pid, shutdown, 5000),
                     receive
                         {'DOWN', Ref, process, Pid, _} -> ok
                     after 5000 ->
                         demonitor(Ref, [flush]),
                         catch exit(Pid, kill)
                     end;
                 false ->
                     ok
             end
         end, [SupPid, RegistryPid]),
         catch ets:delete(flurm_priority_weights)
     end,
     fun(_) ->
         [
             {"Recalculate all with pending jobs", fun() ->
                 %% Create a job spec
                 JobSpec = #job_spec{
                     user_id = 1000,
                     group_id = 1000,
                     partition = <<"default">>,
                     num_nodes = 1,
                     num_cpus = 4,
                     time_limit = 3600,
                     script = <<"#!/bin/bash\necho hello">>
                 },
                 %% Submit a job
                 {ok, _Pid, _JobId} = flurm_job:submit(JobSpec),
                 _ = sys:get_state(flurm_job_registry),

                 %% Call recalculate_all
                 ok = flurm_priority:recalculate_all(),

                 %% The job's priority should have been recalculated
                 ok
             end},
             {"Recalculate all with no pending jobs", fun() ->
                 %% Recalculate when no jobs are pending
                 ok = flurm_priority:recalculate_all()
             end}
         ]
     end}.

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

%% Test get_weights when table is empty (returns default)
get_default_weights_test() ->
    %% Delete the table if it exists
    catch ets:delete(flurm_priority_weights),
    %% Get weights should return defaults
    Weights = flurm_priority:get_weights(),
    ?assertEqual(1000, maps:get(age, Weights)),
    ?assertEqual(5000, maps:get(fairshare, Weights)),
    ok.

%% Test cluster size factor calculation
cluster_size_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Size factor with various cluster sizes", fun() ->
            Job = #{num_nodes => 10, num_cpus => 40},
            %% Different cluster sizes should give different factors
            Factor1 = flurm_priority:size_factor(Job, 10),   % Job uses 100% of cluster
            Factor2 = flurm_priority:size_factor(Job, 100),  % Job uses 10% of cluster
            Factor3 = flurm_priority:size_factor(Job, 1000), % Job uses 1% of cluster

            %% Larger cluster = higher factor for same job
            ?assert(Factor3 > Factor2),
            ?assert(Factor2 > Factor1)
        end},
        {"Size factor with single CPU job", fun() ->
            Job = #{num_nodes => 1, num_cpus => 1},
            Factor = flurm_priority:size_factor(Job, 100),
            %% Small job should get high factor (close to 1.0)
            ?assert(Factor > 0.9)
        end}
     ]}.

%% Test fairshare factor with provided context
fairshare_context_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Fairshare from context overrides lookup", fun() ->
            Now = erlang:system_time(second),
            JobInfo = #{submit_time => Now, qos => <<"normal">>},
            %% Provide fairshare in context
            Context1 = #{fairshare => 0.9},  % High fairshare
            Context2 = #{fairshare => 0.1},  % Low fairshare

            Priority1 = flurm_priority:calculate_priority(JobInfo, Context1),
            Priority2 = flurm_priority:calculate_priority(JobInfo, Context2),

            %% Higher fairshare should give higher priority
            ?assert(Priority1 > Priority2)
        end},
        {"Fairshare default when not in context", fun() ->
            Now = erlang:system_time(second),
            JobInfo = #{submit_time => Now, qos => <<"normal">>, user => <<"testuser">>, account => <<"testaccount">>},
            %% Without fairshare in context, should use default
            Priority = flurm_priority:calculate_priority(JobInfo, #{}),
            ?assert(is_integer(Priority))
        end}
     ]}.

%% Test partition factor with mock registry
partition_with_registry_test() ->
    %% Test that partition_factor returns 0.5 when registry is not available
    JobInfo = #{partition => <<"gpu">>},
    Factor = flurm_priority:partition_factor(JobInfo),
    %% Without registry, should return default 0.5
    ?assertEqual(0.5, Factor),
    ok.

%% Test full priority calculation with all factors
full_priority_calculation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Full calculation with all factors", fun() ->
            Now = erlang:system_time(second),
            JobInfo = #{
                submit_time => Now - 86400,  % 1 day old
                num_nodes => 4,
                num_cpus => 32,
                partition => <<"compute">>,
                qos => <<"high">>,
                nice => -1000,
                user => <<"user1">>,
                account => <<"research">>
            },
            Priority = flurm_priority:calculate_priority(JobInfo),
            ?assert(is_integer(Priority)),
            ?assert(Priority > 0)
        end},
        {"Priority factors are all returned", fun() ->
            Now = erlang:system_time(second),
            JobInfo = #{
                submit_time => Now,
                num_nodes => 1,
                num_cpus => 4
            },
            Factors = flurm_priority:get_priority_factors(JobInfo),
            ?assert(maps:is_key(age, Factors)),
            ?assert(maps:is_key(job_size, Factors)),
            ?assert(maps:is_key(partition, Factors)),
            ?assert(maps:is_key(qos, Factors)),
            ?assert(maps:is_key(nice, Factors)),
            ?assert(maps:is_key(fairshare, Factors)),
            %% All factors should be numeric
            ?assert(is_float(maps:get(age, Factors))),
            ?assert(is_float(maps:get(job_size, Factors)))
        end}
     ]}.
