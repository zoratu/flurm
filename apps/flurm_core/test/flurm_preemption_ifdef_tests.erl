%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_preemption internal functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_preemption_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Ensure the config table exists
    flurm_preemption:ensure_config_table(),
    ok.

cleanup(_) ->
    %% Clean up config table if it exists
    case ets:whereis(flurm_preemption_config) of
        undefined -> ok;
        _ -> ets:delete_all_objects(flurm_preemption_config)
    end,
    ok.

preemption_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun ensure_config_table_creates_table/0,
         fun find_preemption_set_empty_candidates/0,
         fun find_preemption_set_sufficient_resources/0,
         fun find_preemption_set_insufficient_resources/0,
         fun find_sufficient_set_accumulates/0,
         fun calculate_freed_resources_empty/0,
         fun calculate_freed_resources_single_job/0,
         fun calculate_freed_resources_multiple_jobs/0
     ]}.

%%====================================================================
%% Ensure Config Table Tests
%%====================================================================

ensure_config_table_creates_table() ->
    %% Clean up first
    case ets:whereis(flurm_preemption_config) of
        undefined -> ok;
        _ -> ets:delete(flurm_preemption_config)
    end,

    flurm_preemption:ensure_config_table(),
    ?assertNotEqual(undefined, ets:whereis(flurm_preemption_config)).

ensure_config_table_idempotent_test() ->
    flurm_preemption:ensure_config_table(),
    flurm_preemption:ensure_config_table(),
    ?assertNotEqual(undefined, ets:whereis(flurm_preemption_config)).

%%====================================================================
%% Find Preemption Set Tests
%%====================================================================

find_preemption_set_empty_candidates() ->
    Result = flurm_preemption:find_preemption_set([], 1, 1, 1024),
    ?assertEqual({error, insufficient_preemptable_resources}, Result).

find_preemption_set_sufficient_resources() ->
    Candidate = #{
        job_id => 1,
        priority => 100,
        num_nodes => 2,
        num_cpus => 4,
        memory_mb => 2048
    },
    {ok, Jobs} = flurm_preemption:find_preemption_set([Candidate], 1, 2, 1024),
    ?assertEqual(1, length(Jobs)).

find_preemption_set_insufficient_resources() ->
    Candidate = #{
        job_id => 1,
        priority => 100,
        num_nodes => 1,
        num_cpus => 2,
        memory_mb => 1024
    },
    Result = flurm_preemption:find_preemption_set([Candidate], 10, 20, 10240),
    ?assertEqual({error, insufficient_preemptable_resources}, Result).

find_preemption_set_multiple_jobs_test() ->
    Candidates = [
        #{job_id => 1, priority => 50, num_nodes => 1, num_cpus => 2, memory_mb => 1024},
        #{job_id => 2, priority => 100, num_nodes => 2, num_cpus => 4, memory_mb => 2048},
        #{job_id => 3, priority => 75, num_nodes => 1, num_cpus => 2, memory_mb => 1024}
    ],
    {ok, Jobs} = flurm_preemption:find_preemption_set(Candidates, 2, 4, 2048),
    %% Should include jobs sorted by priority (lowest first)
    ?assert(length(Jobs) >= 1).

%%====================================================================
%% Find Sufficient Set Tests
%%====================================================================

find_sufficient_set_accumulates() ->
    Jobs = [
        #{job_id => 1, num_nodes => 1, num_cpus => 2, memory_mb => 1024},
        #{job_id => 2, num_nodes => 1, num_cpus => 2, memory_mb => 1024}
    ],
    %% Need 2 nodes, should accumulate both jobs
    {ok, Selected} = flurm_preemption:find_sufficient_set(
        Jobs, 2, 4, 2048, [], 0, 0, 0
    ),
    ?assertEqual(2, length(Selected)).

find_sufficient_set_stops_early_test() ->
    Jobs = [
        #{job_id => 1, num_nodes => 2, num_cpus => 4, memory_mb => 2048},
        #{job_id => 2, num_nodes => 1, num_cpus => 2, memory_mb => 1024}
    ],
    %% Only need 1 node, should stop after first job
    {ok, Selected} = flurm_preemption:find_sufficient_set(
        Jobs, 1, 2, 1024, [], 0, 0, 0
    ),
    ?assertEqual(1, length(Selected)).

find_sufficient_set_already_satisfied_test() ->
    Jobs = [
        #{job_id => 1, num_nodes => 1, num_cpus => 2, memory_mb => 1024}
    ],
    %% Already have 2 nodes accumulated
    {ok, Selected} = flurm_preemption:find_sufficient_set(
        Jobs, 2, 4, 2048, [], 2, 4, 2048
    ),
    ?assertEqual(0, length(Selected)).

%%====================================================================
%% Calculate Freed Resources Tests
%%====================================================================

calculate_freed_resources_empty() ->
    Result = flurm_preemption:calculate_freed_resources([]),
    ?assertEqual(#{nodes => 0, cpus => 0, memory_mb => 0}, Result).

calculate_freed_resources_single_job() ->
    Jobs = [#{num_nodes => 2, num_cpus => 8, memory_mb => 4096}],
    Result = flurm_preemption:calculate_freed_resources(Jobs),
    ?assertEqual(#{nodes => 2, cpus => 8, memory_mb => 4096}, Result).

calculate_freed_resources_multiple_jobs() ->
    Jobs = [
        #{num_nodes => 2, num_cpus => 8, memory_mb => 4096},
        #{num_nodes => 3, num_cpus => 12, memory_mb => 8192}
    ],
    Result = flurm_preemption:calculate_freed_resources(Jobs),
    ?assertEqual(#{nodes => 5, cpus => 20, memory_mb => 12288}, Result).

calculate_freed_resources_defaults_test() ->
    %% Jobs without explicit values use defaults
    Jobs = [#{}],
    Result = flurm_preemption:calculate_freed_resources(Jobs),
    ?assertEqual(#{nodes => 1, cpus => 1, memory_mb => 1024}, Result).

%%====================================================================
%% Preemption Mode Tests
%%====================================================================

get_preemption_mode_default_test() ->
    flurm_preemption:ensure_config_table(),
    Mode = flurm_preemption:get_preemption_mode(),
    ?assertEqual(requeue, Mode).

set_get_preemption_mode_test() ->
    flurm_preemption:ensure_config_table(),
    flurm_preemption:set_preemption_mode(cancel),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode()),
    %% Reset
    flurm_preemption:set_preemption_mode(requeue).

get_preemption_mode_partition_test() ->
    flurm_preemption:ensure_config_table(),
    %% Without partition-specific mode, should return global default
    Mode = flurm_preemption:get_preemption_mode(#{partition => <<"batch">>}),
    ?assertEqual(requeue, Mode).

set_partition_preemption_mode_test() ->
    flurm_preemption:ensure_config_table(),
    flurm_preemption:set_partition_preemption_mode(<<"gpu">>, suspend),
    Mode = flurm_preemption:get_preemption_mode(#{partition => <<"gpu">>}),
    ?assertEqual(suspend, Mode).

%%====================================================================
%% Grace Time Tests
%%====================================================================

get_grace_time_default_test() ->
    flurm_preemption:ensure_config_table(),
    GraceTime = flurm_preemption:get_grace_time(),
    ?assertEqual(60, GraceTime).

set_get_grace_time_test() ->
    flurm_preemption:ensure_config_table(),
    flurm_preemption:set_grace_time(120),
    ?assertEqual(120, flurm_preemption:get_grace_time()),
    %% Reset
    flurm_preemption:set_grace_time(60).

get_grace_time_partition_test() ->
    flurm_preemption:ensure_config_table(),
    %% Without partition-specific grace time, should return global default
    GraceTime = flurm_preemption:get_grace_time(<<"batch">>),
    ?assertEqual(60, GraceTime).

set_partition_grace_time_test() ->
    flurm_preemption:ensure_config_table(),
    flurm_preemption:set_partition_grace_time(<<"interactive">>, 30),
    GraceTime = flurm_preemption:get_grace_time(<<"interactive">>),
    ?assertEqual(30, GraceTime).

%%====================================================================
%% Priority Threshold Tests
%%====================================================================

get_priority_threshold_default_test() ->
    flurm_preemption:ensure_config_table(),
    Threshold = flurm_preemption:get_priority_threshold(),
    ?assertEqual(1000, Threshold).

set_get_priority_threshold_test() ->
    flurm_preemption:ensure_config_table(),
    flurm_preemption:set_priority_threshold(500),
    ?assertEqual(500, flurm_preemption:get_priority_threshold()),
    %% Reset
    flurm_preemption:set_priority_threshold(1000).

%%====================================================================
%% QOS Preemption Rules Tests
%%====================================================================

get_qos_rules_default_test() ->
    flurm_preemption:ensure_config_table(),
    Rules = flurm_preemption:get_qos_preemption_rules(),
    ?assertEqual(#{<<"high">> => 300, <<"normal">> => 200, <<"low">> => 100}, Rules).

set_get_qos_rules_test() ->
    flurm_preemption:ensure_config_table(),
    NewRules = #{<<"critical">> => 400, <<"high">> => 300, <<"normal">> => 200},
    flurm_preemption:set_qos_preemption_rules(NewRules),
    ?assertEqual(NewRules, flurm_preemption:get_qos_preemption_rules()),
    %% Reset
    flurm_preemption:set_qos_preemption_rules(
        #{<<"high">> => 300, <<"normal">> => 200, <<"low">> => 100}
    ).

%%====================================================================
%% Can Preempt Tests
%%====================================================================

can_preempt_higher_qos_test() ->
    flurm_preemption:ensure_config_table(),
    Preemptor = #{qos => <<"high">>, priority => 1000},
    Preemptee = #{qos => <<"low">>, priority => 500},
    ?assert(flurm_preemption:can_preempt(Preemptor, Preemptee)).

can_preempt_lower_qos_test() ->
    flurm_preemption:ensure_config_table(),
    Preemptor = #{qos => <<"low">>, priority => 1000},
    Preemptee = #{qos => <<"high">>, priority => 500},
    ?assertNot(flurm_preemption:can_preempt(Preemptor, Preemptee)).

can_preempt_same_qos_high_priority_diff_test() ->
    flurm_preemption:ensure_config_table(),
    Preemptor = #{qos => <<"normal">>, priority => 5000},
    Preemptee = #{qos => <<"normal">>, priority => 1000},
    %% Requires > 1000 priority difference (threshold)
    ?assert(flurm_preemption:can_preempt(Preemptor, Preemptee)).

can_preempt_same_qos_low_priority_diff_test() ->
    flurm_preemption:ensure_config_table(),
    Preemptor = #{qos => <<"normal">>, priority => 1500},
    Preemptee = #{qos => <<"normal">>, priority => 1000},
    %% Only 500 difference, needs > 1000
    ?assertNot(flurm_preemption:can_preempt(Preemptor, Preemptee)).

%%====================================================================
%% Calculate Preemption Cost Tests
%%====================================================================

calculate_preemption_cost_low_priority_test() ->
    flurm_preemption:ensure_config_table(),
    Job = #{priority => 100, qos => <<"low">>, num_nodes => 1, num_cpus => 2},
    Cost = flurm_preemption:calculate_preemption_cost(Job),
    ?assert(Cost >= 0.0).

calculate_preemption_cost_high_priority_test() ->
    flurm_preemption:ensure_config_table(),
    LowPriorityJob = #{priority => 100, qos => <<"low">>, num_nodes => 1, num_cpus => 2},
    HighPriorityJob = #{priority => 9000, qos => <<"high">>, num_nodes => 1, num_cpus => 2},
    LowCost = flurm_preemption:calculate_preemption_cost(LowPriorityJob),
    HighCost = flurm_preemption:calculate_preemption_cost(HighPriorityJob),
    %% Higher priority should have higher cost (less desirable to preempt)
    ?assert(HighCost > LowCost).

calculate_preemption_cost_with_runtime_test() ->
    flurm_preemption:ensure_config_table(),
    Now = erlang:system_time(second),
    ShortRunJob = #{priority => 500, qos => <<"normal">>, start_time => Now - 60},
    LongRunJob = #{priority => 500, qos => <<"normal">>, start_time => Now - 3600},
    ShortCost = flurm_preemption:calculate_preemption_cost(ShortRunJob),
    LongCost = flurm_preemption:calculate_preemption_cost(LongRunJob),
    %% Longer running job should have higher cost
    ?assert(LongCost > ShortCost).
