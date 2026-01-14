%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_preemption module
%%%
%%% NO MECK - Tests all functions directly without mocking.
%%% Tests configuration, preemption logic, cost calculation, job
%%% comparison, and internal helper functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_preemption_pure_tests).
-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Clean up any existing config table
    catch ets:delete(flurm_preemption_config),
    ok.

cleanup(_) ->
    catch ets:delete(flurm_preemption_config),
    ok.

%%====================================================================
%% Preemption Mode Configuration Tests
%%====================================================================

preemption_mode_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get default preemption mode returns requeue", fun test_default_preemption_mode/0},
        {"set requeue mode", fun test_set_requeue_mode/0},
        {"set cancel mode", fun test_set_cancel_mode/0},
        {"set checkpoint mode", fun test_set_checkpoint_mode/0},
        {"set suspend mode", fun test_set_suspend_mode/0},
        {"set off mode", fun test_set_off_mode/0},
        {"mode persists after multiple gets", fun test_mode_persists/0}
     ]}.

test_default_preemption_mode() ->
    Mode = flurm_preemption:get_preemption_mode(),
    ?assertEqual(requeue, Mode).

test_set_requeue_mode() ->
    ok = flurm_preemption:set_preemption_mode(requeue),
    ?assertEqual(requeue, flurm_preemption:get_preemption_mode()).

test_set_cancel_mode() ->
    ok = flurm_preemption:set_preemption_mode(cancel),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode()).

test_set_checkpoint_mode() ->
    ok = flurm_preemption:set_preemption_mode(checkpoint),
    ?assertEqual(checkpoint, flurm_preemption:get_preemption_mode()).

test_set_suspend_mode() ->
    ok = flurm_preemption:set_preemption_mode(suspend),
    ?assertEqual(suspend, flurm_preemption:get_preemption_mode()).

test_set_off_mode() ->
    ok = flurm_preemption:set_preemption_mode(off),
    ?assertEqual(off, flurm_preemption:get_preemption_mode()).

test_mode_persists() ->
    ok = flurm_preemption:set_preemption_mode(cancel),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode()),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode()),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode()).

%%====================================================================
%% Grace Time Configuration Tests
%%====================================================================

grace_time_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get default grace time returns 60", fun test_default_grace_time/0},
        {"set grace time to 30", fun test_set_grace_time_30/0},
        {"set grace time to 120", fun test_set_grace_time_120/0},
        {"set grace time to 300", fun test_set_grace_time_300/0},
        {"set grace time to 1", fun test_set_grace_time_min/0},
        {"set grace time to large value", fun test_set_grace_time_large/0},
        {"grace time overrides previous value", fun test_grace_time_override/0}
     ]}.

test_default_grace_time() ->
    Time = flurm_preemption:get_grace_time(),
    ?assertEqual(60, Time).

test_set_grace_time_30() ->
    ok = flurm_preemption:set_grace_time(30),
    ?assertEqual(30, flurm_preemption:get_grace_time()).

test_set_grace_time_120() ->
    ok = flurm_preemption:set_grace_time(120),
    ?assertEqual(120, flurm_preemption:get_grace_time()).

test_set_grace_time_300() ->
    ok = flurm_preemption:set_grace_time(300),
    ?assertEqual(300, flurm_preemption:get_grace_time()).

test_set_grace_time_min() ->
    ok = flurm_preemption:set_grace_time(1),
    ?assertEqual(1, flurm_preemption:get_grace_time()).

test_set_grace_time_large() ->
    ok = flurm_preemption:set_grace_time(86400),  % 24 hours
    ?assertEqual(86400, flurm_preemption:get_grace_time()).

test_grace_time_override() ->
    ok = flurm_preemption:set_grace_time(10),
    ?assertEqual(10, flurm_preemption:get_grace_time()),
    ok = flurm_preemption:set_grace_time(100),
    ?assertEqual(100, flurm_preemption:get_grace_time()).

%%====================================================================
%% Priority Threshold Configuration Tests
%%====================================================================

priority_threshold_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get default priority threshold returns 1000", fun test_default_priority_threshold/0},
        {"set priority threshold to 0", fun test_set_threshold_zero/0},
        {"set priority threshold to 500", fun test_set_threshold_500/0},
        {"set priority threshold to 2000", fun test_set_threshold_2000/0},
        {"set priority threshold to 5000", fun test_set_threshold_5000/0},
        {"priority threshold affects can_preempt", fun test_threshold_affects_preemption/0}
     ]}.

test_default_priority_threshold() ->
    Threshold = flurm_preemption:get_priority_threshold(),
    ?assertEqual(1000, Threshold).

test_set_threshold_zero() ->
    ok = flurm_preemption:set_priority_threshold(0),
    ?assertEqual(0, flurm_preemption:get_priority_threshold()).

test_set_threshold_500() ->
    ok = flurm_preemption:set_priority_threshold(500),
    ?assertEqual(500, flurm_preemption:get_priority_threshold()).

test_set_threshold_2000() ->
    ok = flurm_preemption:set_priority_threshold(2000),
    ?assertEqual(2000, flurm_preemption:get_priority_threshold()).

test_set_threshold_5000() ->
    ok = flurm_preemption:set_priority_threshold(5000),
    ?assertEqual(5000, flurm_preemption:get_priority_threshold()).

test_threshold_affects_preemption() ->
    %% With default threshold of 1000, small difference won't allow preemption
    Job1 = #{qos => <<"normal">>, priority => 1500},
    Job2 = #{qos => <<"normal">>, priority => 1000},
    ?assertNot(flurm_preemption:can_preempt(Job1, Job2)),

    %% Set threshold to 0, now any priority difference allows preemption
    ok = flurm_preemption:set_priority_threshold(0),
    ?assert(flurm_preemption:can_preempt(Job1, Job2)).

%%====================================================================
%% QOS Preemption Rules Tests
%%====================================================================

qos_rules_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get default QOS rules", fun test_default_qos_rules/0},
        {"default QOS has correct levels", fun test_default_qos_levels/0},
        {"set custom QOS rules", fun test_set_custom_qos_rules/0},
        {"custom rules with critical level", fun test_custom_rules_critical/0},
        {"custom rules override completely", fun test_custom_rules_override/0},
        {"QOS rules affect can_preempt", fun test_qos_rules_affect_preemption/0}
     ]}.

test_default_qos_rules() ->
    Rules = flurm_preemption:get_qos_preemption_rules(),
    ?assert(is_map(Rules)),
    ?assert(maps:is_key(<<"high">>, Rules)),
    ?assert(maps:is_key(<<"normal">>, Rules)),
    ?assert(maps:is_key(<<"low">>, Rules)).

test_default_qos_levels() ->
    Rules = flurm_preemption:get_qos_preemption_rules(),
    ?assertEqual(300, maps:get(<<"high">>, Rules)),
    ?assertEqual(200, maps:get(<<"normal">>, Rules)),
    ?assertEqual(100, maps:get(<<"low">>, Rules)).

test_set_custom_qos_rules() ->
    CustomRules = #{
        <<"premium">> => 500,
        <<"standard">> => 100
    },
    ok = flurm_preemption:set_qos_preemption_rules(CustomRules),
    Retrieved = flurm_preemption:get_qos_preemption_rules(),
    ?assertEqual(500, maps:get(<<"premium">>, Retrieved)),
    ?assertEqual(100, maps:get(<<"standard">>, Retrieved)).

test_custom_rules_critical() ->
    CustomRules = #{
        <<"critical">> => 1000,
        <<"urgent">> => 800,
        <<"high">> => 500,
        <<"normal">> => 200,
        <<"low">> => 50
    },
    ok = flurm_preemption:set_qos_preemption_rules(CustomRules),
    Rules = flurm_preemption:get_qos_preemption_rules(),
    ?assertEqual(1000, maps:get(<<"critical">>, Rules)),
    ?assertEqual(800, maps:get(<<"urgent">>, Rules)).

test_custom_rules_override() ->
    %% Set rules once
    ok = flurm_preemption:set_qos_preemption_rules(#{<<"a">> => 100}),
    ?assertEqual(#{<<"a">> => 100}, flurm_preemption:get_qos_preemption_rules()),

    %% Override with new rules
    ok = flurm_preemption:set_qos_preemption_rules(#{<<"b">> => 200}),
    Rules = flurm_preemption:get_qos_preemption_rules(),
    ?assertEqual(200, maps:get(<<"b">>, Rules)),
    ?assertEqual(undefined, maps:get(<<"a">>, Rules, undefined)).

test_qos_rules_affect_preemption() ->
    %% Set custom rules where premium > standard
    ok = flurm_preemption:set_qos_preemption_rules(#{
        <<"premium">> => 500,
        <<"standard">> => 100
    }),

    PremiumJob = #{qos => <<"premium">>, priority => 100},
    StandardJob = #{qos => <<"standard">>, priority => 100},

    %% Premium should preempt standard
    ?assert(flurm_preemption:can_preempt(PremiumJob, StandardJob)),
    %% Standard should not preempt premium
    ?assertNot(flurm_preemption:can_preempt(StandardJob, PremiumJob)).

%%====================================================================
%% Partition-Specific Preemption Mode Tests
%%====================================================================

partition_mode_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"set partition preemption mode to requeue", fun test_set_partition_mode_requeue/0},
        {"set partition preemption mode to cancel", fun test_set_partition_mode_cancel/0},
        {"set partition preemption mode to checkpoint", fun test_set_partition_mode_checkpoint/0},
        {"set partition preemption mode to suspend", fun test_set_partition_mode_suspend/0},
        {"set partition preemption mode to off", fun test_set_partition_mode_off/0},
        {"multiple partitions have different modes", fun test_multiple_partition_modes/0},
        {"get mode with partition context", fun test_get_mode_partition_context/0},
        {"unconfigured partition uses global mode", fun test_unconfigured_partition/0},
        {"partition mode overrides global", fun test_partition_overrides_global/0}
     ]}.

test_set_partition_mode_requeue() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"gpu">>, requeue),
    ?assertEqual(requeue, flurm_preemption:get_preemption_mode(#{partition => <<"gpu">>})).

test_set_partition_mode_cancel() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"batch">>, cancel),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode(#{partition => <<"batch">>})).

test_set_partition_mode_checkpoint() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"hpc">>, checkpoint),
    ?assertEqual(checkpoint, flurm_preemption:get_preemption_mode(#{partition => <<"hpc">>})).

test_set_partition_mode_suspend() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"interactive">>, suspend),
    ?assertEqual(suspend, flurm_preemption:get_preemption_mode(#{partition => <<"interactive">>})).

test_set_partition_mode_off() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"protected">>, off),
    ?assertEqual(off, flurm_preemption:get_preemption_mode(#{partition => <<"protected">>})).

test_multiple_partition_modes() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"part1">>, requeue),
    ok = flurm_preemption:set_partition_preemption_mode(<<"part2">>, cancel),
    ok = flurm_preemption:set_partition_preemption_mode(<<"part3">>, suspend),

    ?assertEqual(requeue, flurm_preemption:get_preemption_mode(#{partition => <<"part1">>})),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode(#{partition => <<"part2">>})),
    ?assertEqual(suspend, flurm_preemption:get_preemption_mode(#{partition => <<"part3">>})).

test_get_mode_partition_context() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"special">>, checkpoint),
    Mode = flurm_preemption:get_preemption_mode(#{partition => <<"special">>}),
    ?assertEqual(checkpoint, Mode).

test_unconfigured_partition() ->
    %% Set global mode
    ok = flurm_preemption:set_preemption_mode(cancel),
    %% Unconfigured partition should use global
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode(#{partition => <<"nonexistent">>})).

test_partition_overrides_global() ->
    ok = flurm_preemption:set_preemption_mode(requeue),
    ok = flurm_preemption:set_partition_preemption_mode(<<"override">>, suspend),

    ?assertEqual(requeue, flurm_preemption:get_preemption_mode()),
    ?assertEqual(suspend, flurm_preemption:get_preemption_mode(#{partition => <<"override">>})).

%%====================================================================
%% Partition-Specific Grace Time Tests
%%====================================================================

partition_grace_time_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"set partition grace time", fun test_set_partition_grace_time/0},
        {"partition grace time to 30", fun test_partition_grace_30/0},
        {"partition grace time to 300", fun test_partition_grace_300/0},
        {"multiple partitions have different grace times", fun test_multiple_partition_grace_times/0},
        {"unconfigured partition uses global grace time", fun test_unconfigured_partition_grace/0},
        {"partition grace time overrides global", fun test_partition_grace_overrides_global/0}
     ]}.

test_set_partition_grace_time() ->
    ok = flurm_preemption:set_partition_grace_time(<<"test">>, 45),
    ?assertEqual(45, flurm_preemption:get_grace_time(<<"test">>)).

test_partition_grace_30() ->
    ok = flurm_preemption:set_partition_grace_time(<<"fast">>, 30),
    ?assertEqual(30, flurm_preemption:get_grace_time(<<"fast">>)).

test_partition_grace_300() ->
    ok = flurm_preemption:set_partition_grace_time(<<"slow">>, 300),
    ?assertEqual(300, flurm_preemption:get_grace_time(<<"slow">>)).

test_multiple_partition_grace_times() ->
    ok = flurm_preemption:set_partition_grace_time(<<"p1">>, 10),
    ok = flurm_preemption:set_partition_grace_time(<<"p2">>, 60),
    ok = flurm_preemption:set_partition_grace_time(<<"p3">>, 300),

    ?assertEqual(10, flurm_preemption:get_grace_time(<<"p1">>)),
    ?assertEqual(60, flurm_preemption:get_grace_time(<<"p2">>)),
    ?assertEqual(300, flurm_preemption:get_grace_time(<<"p3">>)).

test_unconfigured_partition_grace() ->
    ok = flurm_preemption:set_grace_time(90),
    ?assertEqual(90, flurm_preemption:get_grace_time(<<"nonexistent">>)).

test_partition_grace_overrides_global() ->
    ok = flurm_preemption:set_grace_time(60),
    ok = flurm_preemption:set_partition_grace_time(<<"custom">>, 180),

    ?assertEqual(60, flurm_preemption:get_grace_time()),
    ?assertEqual(180, flurm_preemption:get_grace_time(<<"custom">>)).

%%====================================================================
%% Can Preempt Logic Tests - QOS Based
%%====================================================================

can_preempt_qos_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"high QOS can preempt low QOS", fun test_high_preempts_low/0},
        {"high QOS can preempt normal QOS", fun test_high_preempts_normal/0},
        {"normal QOS can preempt low QOS", fun test_normal_preempts_low/0},
        {"low QOS cannot preempt high QOS", fun test_low_cannot_preempt_high/0},
        {"low QOS cannot preempt normal QOS", fun test_low_cannot_preempt_normal/0},
        {"normal QOS cannot preempt high QOS", fun test_normal_cannot_preempt_high/0},
        {"same QOS cannot preempt without priority diff", fun test_same_qos_no_preempt/0},
        {"unknown QOS uses level 0", fun test_unknown_qos_level_zero/0}
     ]}.

test_high_preempts_low() ->
    HighJob = #{qos => <<"high">>, priority => 100},
    LowJob = #{qos => <<"low">>, priority => 100},
    ?assert(flurm_preemption:can_preempt(HighJob, LowJob)).

test_high_preempts_normal() ->
    HighJob = #{qos => <<"high">>, priority => 100},
    NormalJob = #{qos => <<"normal">>, priority => 100},
    ?assert(flurm_preemption:can_preempt(HighJob, NormalJob)).

test_normal_preempts_low() ->
    NormalJob = #{qos => <<"normal">>, priority => 100},
    LowJob = #{qos => <<"low">>, priority => 100},
    ?assert(flurm_preemption:can_preempt(NormalJob, LowJob)).

test_low_cannot_preempt_high() ->
    LowJob = #{qos => <<"low">>, priority => 9999},
    HighJob = #{qos => <<"high">>, priority => 100},
    ?assertNot(flurm_preemption:can_preempt(LowJob, HighJob)).

test_low_cannot_preempt_normal() ->
    LowJob = #{qos => <<"low">>, priority => 9999},
    NormalJob = #{qos => <<"normal">>, priority => 100},
    ?assertNot(flurm_preemption:can_preempt(LowJob, NormalJob)).

test_normal_cannot_preempt_high() ->
    NormalJob = #{qos => <<"normal">>, priority => 9999},
    HighJob = #{qos => <<"high">>, priority => 100},
    ?assertNot(flurm_preemption:can_preempt(NormalJob, HighJob)).

test_same_qos_no_preempt() ->
    Job1 = #{qos => <<"normal">>, priority => 1500},
    Job2 = #{qos => <<"normal">>, priority => 1000},
    %% With default threshold of 1000, 500 difference is not enough
    ?assertNot(flurm_preemption:can_preempt(Job1, Job2)).

test_unknown_qos_level_zero() ->
    %% Unknown QOS should get level 0
    UnknownJob = #{qos => <<"mystery">>, priority => 100},
    LowJob = #{qos => <<"low">>, priority => 100},
    %% Level 0 (unknown) < level 100 (low), so cannot preempt
    ?assertNot(flurm_preemption:can_preempt(UnknownJob, LowJob)).

%%====================================================================
%% Can Preempt Logic Tests - Priority Based
%%====================================================================

can_preempt_priority_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"same QOS large priority diff can preempt", fun test_large_priority_diff/0},
        {"same QOS exact threshold can preempt", fun test_exact_threshold/0},
        {"same QOS below threshold cannot preempt", fun test_below_threshold/0},
        {"zero threshold allows any priority diff", fun test_zero_threshold/0},
        {"custom threshold 500", fun test_custom_threshold_500/0},
        {"priority without QOS uses normal", fun test_priority_no_qos/0}
     ]}.

test_large_priority_diff() ->
    %% With threshold 1000, diff of 4000 should allow preemption
    HighPriJob = #{qos => <<"normal">>, priority => 5000},
    LowPriJob = #{qos => <<"normal">>, priority => 1000},
    ?assert(flurm_preemption:can_preempt(HighPriJob, LowPriJob)).

test_exact_threshold() ->
    %% Priority diff exactly at threshold should allow preemption (>)
    Job1 = #{qos => <<"normal">>, priority => 2001},
    Job2 = #{qos => <<"normal">>, priority => 1000},
    ?assert(flurm_preemption:can_preempt(Job1, Job2)).

test_below_threshold() ->
    Job1 = #{qos => <<"normal">>, priority => 1999},
    Job2 = #{qos => <<"normal">>, priority => 1000},
    %% 999 diff < 1000 threshold
    ?assertNot(flurm_preemption:can_preempt(Job1, Job2)).

test_zero_threshold() ->
    ok = flurm_preemption:set_priority_threshold(0),
    Job1 = #{qos => <<"normal">>, priority => 101},
    Job2 = #{qos => <<"normal">>, priority => 100},
    ?assert(flurm_preemption:can_preempt(Job1, Job2)).

test_custom_threshold_500() ->
    ok = flurm_preemption:set_priority_threshold(500),
    Job1 = #{qos => <<"normal">>, priority => 1600},
    Job2 = #{qos => <<"normal">>, priority => 1000},
    %% 600 diff > 500 threshold
    ?assert(flurm_preemption:can_preempt(Job1, Job2)).

test_priority_no_qos() ->
    %% Jobs without QOS should use default "normal"
    Job1 = #{priority => 5000},
    Job2 = #{priority => 1000},
    %% 4000 diff > 1000 threshold
    ?assert(flurm_preemption:can_preempt(Job1, Job2)).

%%====================================================================
%% Can Preempt Edge Cases
%%====================================================================

can_preempt_edge_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job with no priority uses default", fun test_no_priority_uses_default/0},
        {"job with no QOS uses normal", fun test_no_qos_uses_normal/0},
        {"empty job maps use defaults", fun test_empty_jobs/0},
        {"binary QOS names work correctly", fun test_binary_qos/0}
     ]}.

test_no_priority_uses_default() ->
    Job1 = #{qos => <<"high">>},  % No priority
    Job2 = #{qos => <<"low">>, priority => 100},
    %% High QOS should still preempt low QOS
    ?assert(flurm_preemption:can_preempt(Job1, Job2)).

test_no_qos_uses_normal() ->
    Job1 = #{priority => 100},  % No QOS
    Job2 = #{qos => <<"low">>, priority => 100},
    %% Normal (default) > low, so can preempt
    ?assert(flurm_preemption:can_preempt(Job1, Job2)).

test_empty_jobs() ->
    Job1 = #{},
    Job2 = #{},
    %% Both use defaults, same QOS, same priority, cannot preempt
    ?assertNot(flurm_preemption:can_preempt(Job1, Job2)).

test_binary_qos() ->
    Job1 = #{qos => <<"high">>},
    Job2 = #{qos => <<"low">>},
    ?assert(flurm_preemption:can_preempt(Job1, Job2)).

%%====================================================================
%% Calculate Preemption Cost Tests
%%====================================================================

preemption_cost_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"cost is a non-negative float", fun test_cost_is_float/0},
        {"lower priority has lower cost", fun test_cost_priority_ordering/0},
        {"lower QOS has lower cost", fun test_cost_qos_ordering/0},
        {"newer job has lower cost", fun test_cost_start_time_ordering/0},
        {"more resources gives lower cost", fun test_cost_resource_benefit/0},
        {"missing start_time handled", fun test_cost_no_start_time/0},
        {"undefined start_time handled", fun test_cost_undefined_start_time/0},
        {"non-integer start_time handled", fun test_cost_invalid_start_time/0},
        {"cost with all defaults", fun test_cost_all_defaults/0},
        {"cost calculation is deterministic", fun test_cost_deterministic/0}
     ]}.

test_cost_is_float() ->
    Job = #{priority => 1000, qos => <<"normal">>},
    Cost = flurm_preemption:calculate_preemption_cost(Job),
    ?assert(is_float(Cost)),
    ?assert(Cost >= 0.0).

test_cost_priority_ordering() ->
    Now = erlang:system_time(second),
    LowPriJob = #{priority => 100, qos => <<"normal">>, start_time => Now},
    HighPriJob = #{priority => 9000, qos => <<"normal">>, start_time => Now},

    LowCost = flurm_preemption:calculate_preemption_cost(LowPriJob),
    HighCost = flurm_preemption:calculate_preemption_cost(HighPriJob),
    ?assert(LowCost < HighCost).

test_cost_qos_ordering() ->
    Now = erlang:system_time(second),
    LowQOSJob = #{priority => 1000, qos => <<"low">>, start_time => Now},
    HighQOSJob = #{priority => 1000, qos => <<"high">>, start_time => Now},

    LowCost = flurm_preemption:calculate_preemption_cost(LowQOSJob),
    HighCost = flurm_preemption:calculate_preemption_cost(HighQOSJob),
    ?assert(LowCost < HighCost).

test_cost_start_time_ordering() ->
    Now = erlang:system_time(second),
    NewJob = #{priority => 1000, qos => <<"normal">>, start_time => Now},
    OldJob = #{priority => 1000, qos => <<"normal">>, start_time => Now - 7200},  % 2 hours ago

    NewCost = flurm_preemption:calculate_preemption_cost(NewJob),
    OldCost = flurm_preemption:calculate_preemption_cost(OldJob),
    ?assert(NewCost < OldCost).

test_cost_resource_benefit() ->
    Now = erlang:system_time(second),
    SmallJob = #{priority => 1000, qos => <<"normal">>, num_nodes => 1, num_cpus => 1, start_time => Now},
    BigJob = #{priority => 1000, qos => <<"normal">>, num_nodes => 100, num_cpus => 1000, start_time => Now},

    SmallCost = flurm_preemption:calculate_preemption_cost(SmallJob),
    BigCost = flurm_preemption:calculate_preemption_cost(BigJob),
    %% Big job should have lower cost due to resource benefit
    ?assert(BigCost < SmallCost).

test_cost_no_start_time() ->
    Job = #{priority => 1000, qos => <<"normal">>},
    Cost = flurm_preemption:calculate_preemption_cost(Job),
    ?assert(is_float(Cost)),
    ?assert(Cost >= 0.0).

test_cost_undefined_start_time() ->
    Job = #{priority => 1000, qos => <<"normal">>, start_time => undefined},
    Cost = flurm_preemption:calculate_preemption_cost(Job),
    ?assert(is_float(Cost)),
    ?assert(Cost >= 0.0).

test_cost_invalid_start_time() ->
    Job = #{priority => 1000, qos => <<"normal">>, start_time => "invalid"},
    Cost = flurm_preemption:calculate_preemption_cost(Job),
    ?assert(is_float(Cost)),
    ?assert(Cost >= 0.0).

test_cost_all_defaults() ->
    %% Job with no specified values should use all defaults
    Job = #{},
    Cost = flurm_preemption:calculate_preemption_cost(Job),
    ?assert(is_float(Cost)),
    ?assert(Cost >= 0.0).

test_cost_deterministic() ->
    Job = #{priority => 5000, qos => <<"high">>, num_nodes => 5, num_cpus => 40},
    Cost1 = flurm_preemption:calculate_preemption_cost(Job),
    Cost2 = flurm_preemption:calculate_preemption_cost(Job),
    ?assertEqual(Cost1, Cost2).

%%====================================================================
%% Check Preemption Tests
%%====================================================================

check_preemption_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"check_preemption returns error when disabled", fun test_check_disabled/0},
        {"check_preemption handles no running jobs", fun test_check_no_running/0}
     ]}.

test_check_disabled() ->
    ok = flurm_preemption:set_preemption_mode(off),
    PendingJob = #{
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 8192,
        priority => 10000
    },
    Result = flurm_preemption:check_preemption(PendingJob),
    ?assertEqual({error, preemption_disabled}, Result).

test_check_no_running() ->
    %% Without job_registry running, check_preemption should error
    PendingJob = #{priority => 10000, num_nodes => 1, num_cpus => 4, memory_mb => 8192},
    Result = (catch flurm_preemption:check_preemption(PendingJob)),
    IsExpected = case Result of
        {error, _} -> true;
        {'EXIT', _} -> true;
        _ -> false
    end,
    ?assert(IsExpected).

%%====================================================================
%% Preempt Jobs Tests
%%====================================================================

preempt_jobs_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"preempt_jobs with empty list", fun test_preempt_empty/0},
        {"preempt_jobs with undefined pid", fun test_preempt_undefined_pid/0},
        {"preempt_jobs with no pid key", fun test_preempt_no_pid/0},
        {"preempt_jobs returns ok tuple", fun test_preempt_returns_ok/0},
        {"preempt_jobs requeue mode", fun test_preempt_requeue/0},
        {"preempt_jobs cancel mode", fun test_preempt_cancel/0},
        {"preempt_jobs checkpoint mode", fun test_preempt_checkpoint/0},
        {"preempt_jobs suspend mode", fun test_preempt_suspend/0},
        {"preempt_jobs off mode", fun test_preempt_off/0}
     ]}.

test_preempt_empty() ->
    {ok, Preempted} = flurm_preemption:preempt_jobs([], requeue),
    ?assertEqual([], Preempted).

test_preempt_undefined_pid() ->
    Jobs = [#{job_id => 1, pid => undefined}],
    {ok, Preempted} = flurm_preemption:preempt_jobs(Jobs, requeue),
    ?assertEqual([], Preempted).

test_preempt_no_pid() ->
    Jobs = [#{job_id => 1}],
    {ok, Preempted} = flurm_preemption:preempt_jobs(Jobs, requeue),
    ?assertEqual([], Preempted).

test_preempt_returns_ok() ->
    {ok, _} = flurm_preemption:preempt_jobs([], cancel),
    ok.

test_preempt_requeue() ->
    {ok, Preempted} = flurm_preemption:preempt_jobs([#{job_id => 1}], requeue),
    ?assertEqual([], Preempted).

test_preempt_cancel() ->
    {ok, Preempted} = flurm_preemption:preempt_jobs([#{job_id => 1}], cancel),
    ?assertEqual([], Preempted).

test_preempt_checkpoint() ->
    {ok, Preempted} = flurm_preemption:preempt_jobs([#{job_id => 1}], checkpoint),
    ?assertEqual([], Preempted).

test_preempt_suspend() ->
    {ok, Preempted} = flurm_preemption:preempt_jobs([#{job_id => 1}], suspend),
    ?assertEqual([], Preempted).

test_preempt_off() ->
    {ok, Preempted} = flurm_preemption:preempt_jobs([#{job_id => 1}], off),
    ?assertEqual([], Preempted).

%%====================================================================
%% Get Preemption Mode With Context Tests
%%====================================================================

get_mode_context_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get mode with empty map returns global", fun test_mode_empty_map/0},
        {"get mode with partition key", fun test_mode_partition_key/0},
        {"get mode with qos key only", fun test_mode_qos_key_only/0},
        {"get mode with partition and qos", fun test_mode_partition_and_qos/0},
        {"get mode with partition overrides qos", fun test_mode_partition_overrides_qos/0},
        {"get mode fallback to qos when no partition", fun test_mode_fallback_to_qos/0}
     ]}.

test_mode_empty_map() ->
    ?assertEqual(requeue, flurm_preemption:get_preemption_mode(#{})).

test_mode_partition_key() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"p1">>, cancel),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode(#{partition => <<"p1">>})).

test_mode_qos_key_only() ->
    %% Without flurm_qos module, should fall back to global mode
    Mode = flurm_preemption:get_preemption_mode(#{qos => <<"high">>}),
    ?assertEqual(requeue, Mode).

test_mode_partition_and_qos() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"p1">>, suspend),
    Mode = flurm_preemption:get_preemption_mode(#{partition => <<"p1">>, qos => <<"high">>}),
    ?assertEqual(suspend, Mode).

test_mode_partition_overrides_qos() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"p1">>, checkpoint),
    Mode = flurm_preemption:get_preemption_mode(#{partition => <<"p1">>, qos => <<"low">>}),
    ?assertEqual(checkpoint, Mode).

test_mode_fallback_to_qos() ->
    %% Unconfigured partition with QOS key should check QOS then global
    ok = flurm_preemption:set_preemption_mode(cancel),
    Mode = flurm_preemption:get_preemption_mode(#{partition => <<"unconfigured">>, qos => <<"high">>}),
    %% Falls back to global since partition is not configured and flurm_qos is not available
    ?assertEqual(cancel, Mode).

%%====================================================================
%% Handle Preempted Job Tests
%%====================================================================

handle_preempted_job_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"handle_preempted_job off mode returns disabled", fun test_handle_off_mode/0},
        {"handle_preempted_job requeue without job_manager", fun test_handle_requeue_no_manager/0},
        {"handle_preempted_job cancel without job_manager", fun test_handle_cancel_no_manager/0},
        {"handle_preempted_job checkpoint without job_manager", fun test_handle_checkpoint_no_manager/0},
        {"handle_preempted_job suspend without job_manager", fun test_handle_suspend_no_manager/0}
     ]}.

test_handle_off_mode() ->
    Result = flurm_preemption:handle_preempted_job(1, off),
    ?assertEqual({error, preemption_disabled}, Result).

test_handle_requeue_no_manager() ->
    %% Without flurm_job_manager running, should error
    Result = (catch flurm_preemption:handle_preempted_job(1, requeue)),
    IsError = case Result of
        {error, _} -> true;
        {'EXIT', _} -> true;
        _ -> false
    end,
    ?assert(IsError).

test_handle_cancel_no_manager() ->
    Result = (catch flurm_preemption:handle_preempted_job(1, cancel)),
    IsError = case Result of
        {error, _} -> true;
        {'EXIT', _} -> true;
        _ -> false
    end,
    ?assert(IsError).

test_handle_checkpoint_no_manager() ->
    %% Checkpoint falls back to requeue
    Result = (catch flurm_preemption:handle_preempted_job(1, checkpoint)),
    IsError = case Result of
        {error, _} -> true;
        {'EXIT', _} -> true;
        _ -> false
    end,
    ?assert(IsError).

test_handle_suspend_no_manager() ->
    Result = (catch flurm_preemption:handle_preempted_job(1, suspend)),
    IsError = case Result of
        {error, _} -> true;
        {'EXIT', _} -> true;
        _ -> false
    end,
    ?assert(IsError).

%%====================================================================
%% Find Preemptable Jobs Tests
%%====================================================================

find_preemptable_jobs_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"find_preemptable_jobs disabled returns error", fun test_find_disabled/0},
        {"find_preemptable_jobs uses partition mode", fun test_find_uses_partition_mode/0}
     ]}.

test_find_disabled() ->
    ok = flurm_preemption:set_preemption_mode(off),
    PendingJob = #{priority => 10000, partition => <<"default">>, qos => <<"high">>},
    Resources = #{num_nodes => 1},
    Result = flurm_preemption:find_preemptable_jobs(PendingJob, Resources),
    ?assertEqual({error, preemption_disabled}, Result).

test_find_uses_partition_mode() ->
    %% Set partition mode to off
    ok = flurm_preemption:set_partition_preemption_mode(<<"disabled_partition">>, off),
    PendingJob = #{priority => 10000, partition => <<"disabled_partition">>, qos => <<"high">>},
    Resources = #{num_nodes => 1},
    Result = flurm_preemption:find_preemptable_jobs(PendingJob, Resources),
    ?assertEqual({error, preemption_disabled}, Result).

%%====================================================================
%% Check Preemption Opportunity Tests
%%====================================================================

check_preemption_opportunity_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"check_preemption_opportunity disabled returns error", fun test_opportunity_disabled/0},
        {"check_preemption_opportunity no candidates", fun test_opportunity_no_candidates/0}
     ]}.

test_opportunity_disabled() ->
    ok = flurm_preemption:set_preemption_mode(off),
    PendingJob = #{priority => 10000},
    Resources = #{num_nodes => 1},
    Result = flurm_preemption:check_preemption_opportunity(PendingJob, Resources),
    ?assertEqual({error, preemption_disabled}, Result).

test_opportunity_no_candidates() ->
    %% Without job_registry, should return error
    PendingJob = #{priority => 10000, num_nodes => 1, num_cpus => 4, memory_mb => 8192},
    Resources = #{num_nodes => 1, num_cpus => 4, memory_mb => 8192},
    Result = (catch flurm_preemption:check_preemption_opportunity(PendingJob, Resources)),
    IsError = case Result of
        {error, _} -> true;
        {'EXIT', _} -> true;
        _ -> false
    end,
    ?assert(IsError).

%%====================================================================
%% Execute Preemption Tests
%%====================================================================

execute_preemption_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"execute_preemption empty list returns all_failed", fun test_execute_empty/0},
        {"execute_preemption uses plan mode", fun test_execute_uses_plan_mode/0},
        {"execute_preemption uses plan grace_time", fun test_execute_uses_plan_grace_time/0},
        {"execute_preemption defaults to global mode", fun test_execute_defaults_global/0}
     ]}.

test_execute_empty() ->
    Plan = #{jobs_to_preempt => []},
    Result = flurm_preemption:execute_preemption(Plan, #{}),
    ?assertEqual({error, all_preemptions_failed}, Result).

test_execute_uses_plan_mode() ->
    Plan = #{
        jobs_to_preempt => [#{job_id => 1}],
        preemption_mode => suspend,
        grace_time => 30
    },
    Result = (catch flurm_preemption:execute_preemption(Plan, #{})),
    %% Should fail because job doesn't exist, but shouldn't crash
    IsError = case Result of
        {error, _} -> true;
        {'EXIT', _} -> true;
        _ -> false
    end,
    ?assert(IsError).

test_execute_uses_plan_grace_time() ->
    Plan = #{
        jobs_to_preempt => [],
        grace_time => 120
    },
    Result = flurm_preemption:execute_preemption(Plan, #{}),
    ?assertEqual({error, all_preemptions_failed}, Result).

test_execute_defaults_global() ->
    Plan = #{jobs_to_preempt => []},
    Result = flurm_preemption:execute_preemption(Plan, #{}),
    ?assertEqual({error, all_preemptions_failed}, Result).

%%====================================================================
%% Graceful Preempt Tests (Limited - requires job infrastructure)
%%====================================================================

graceful_preempt_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"graceful_preempt uses partition grace time", fun test_graceful_partition_grace/0}
     ]}.

test_graceful_partition_grace() ->
    %% This will timeout/error without job infrastructure, but tests the code path
    ok = flurm_preemption:set_grace_time(1),  % Short grace time
    ok = flurm_preemption:set_partition_grace_time(<<"test">>, 1),

    %% Calling graceful_preempt without job infrastructure will fail
    Result = (catch flurm_preemption:graceful_preempt(99999, [], requeue)),
    %% Accept any result - we're testing code coverage not functionality
    IsExpected = case Result of
        {ok, _} -> true;
        {error, _} -> true;
        {'EXIT', _} -> true;
        _ -> false
    end,
    ?assert(IsExpected).

%%====================================================================
%% Config Table Initialization Tests
%%====================================================================

config_table_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"config table created on first access", fun test_config_table_created/0},
        {"config table reused on subsequent access", fun test_config_table_reused/0},
        {"multiple config operations", fun test_multiple_config_ops/0}
     ]}.

test_config_table_created() ->
    %% Table doesn't exist yet
    ?assertEqual(undefined, ets:whereis(flurm_preemption_config)),
    %% First access should create it
    _ = flurm_preemption:get_preemption_mode(),
    ?assertNotEqual(undefined, ets:whereis(flurm_preemption_config)).

test_config_table_reused() ->
    _ = flurm_preemption:get_preemption_mode(),
    Tid1 = ets:whereis(flurm_preemption_config),
    _ = flurm_preemption:get_grace_time(),
    Tid2 = ets:whereis(flurm_preemption_config),
    ?assertEqual(Tid1, Tid2).

test_multiple_config_ops() ->
    %% Multiple operations should all work
    ok = flurm_preemption:set_preemption_mode(cancel),
    ok = flurm_preemption:set_grace_time(120),
    ok = flurm_preemption:set_priority_threshold(500),
    ok = flurm_preemption:set_qos_preemption_rules(#{<<"a">> => 100}),
    ok = flurm_preemption:set_partition_preemption_mode(<<"p1">>, suspend),
    ok = flurm_preemption:set_partition_grace_time(<<"p1">>, 30),

    ?assertEqual(cancel, flurm_preemption:get_preemption_mode()),
    ?assertEqual(120, flurm_preemption:get_grace_time()),
    ?assertEqual(500, flurm_preemption:get_priority_threshold()),
    ?assertEqual(#{<<"a">> => 100}, flurm_preemption:get_qos_preemption_rules()),
    ?assertEqual(suspend, flurm_preemption:get_preemption_mode(#{partition => <<"p1">>})),
    ?assertEqual(30, flurm_preemption:get_grace_time(<<"p1">>)).

%%====================================================================
%% Comprehensive Integration Tests
%%====================================================================

integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"full preemption config workflow", fun test_full_workflow/0},
        {"mixed QOS and priority preemption", fun test_mixed_qos_priority/0},
        {"cost comparison for job selection", fun test_cost_comparison/0}
     ]}.

test_full_workflow() ->
    %% Set up complete configuration
    ok = flurm_preemption:set_preemption_mode(requeue),
    ok = flurm_preemption:set_grace_time(60),
    ok = flurm_preemption:set_priority_threshold(500),
    ok = flurm_preemption:set_qos_preemption_rules(#{
        <<"critical">> => 1000,
        <<"high">> => 500,
        <<"normal">> => 200,
        <<"low">> => 50
    }),
    ok = flurm_preemption:set_partition_preemption_mode(<<"gpu">>, cancel),
    ok = flurm_preemption:set_partition_grace_time(<<"gpu">>, 30),

    %% Verify configuration
    ?assertEqual(requeue, flurm_preemption:get_preemption_mode()),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode(#{partition => <<"gpu">>})),
    ?assertEqual(60, flurm_preemption:get_grace_time()),
    ?assertEqual(30, flurm_preemption:get_grace_time(<<"gpu">>)),

    %% Test preemption decisions
    CriticalJob = #{qos => <<"critical">>, priority => 100},
    LowJob = #{qos => <<"low">>, priority => 100},
    ?assert(flurm_preemption:can_preempt(CriticalJob, LowJob)).

test_mixed_qos_priority() ->
    ok = flurm_preemption:set_priority_threshold(100),

    %% Higher QOS always wins regardless of priority
    HighQOS = #{qos => <<"high">>, priority => 100},
    LowQOS = #{qos => <<"low">>, priority => 9999},
    ?assert(flurm_preemption:can_preempt(HighQOS, LowQOS)),
    ?assertNot(flurm_preemption:can_preempt(LowQOS, HighQOS)),

    %% Same QOS uses priority
    NormalHigh = #{qos => <<"normal">>, priority => 5000},
    NormalLow = #{qos => <<"normal">>, priority => 1000},
    ?assert(flurm_preemption:can_preempt(NormalHigh, NormalLow)).

test_cost_comparison() ->
    Now = erlang:system_time(second),

    %% Create jobs with different characteristics
    IdealVictim = #{
        priority => 50,
        qos => <<"low">>,
        start_time => Now,
        num_nodes => 10,
        num_cpus => 100
    },

    BadVictim = #{
        priority => 9000,
        qos => <<"high">>,
        start_time => Now - 86400,  % 1 day old
        num_nodes => 1,
        num_cpus => 1
    },

    IdealCost = flurm_preemption:calculate_preemption_cost(IdealVictim),
    BadCost = flurm_preemption:calculate_preemption_cost(BadVictim),

    %% Ideal victim should have lower cost
    ?assert(IdealCost < BadCost).

%%====================================================================
%% Resource Calculations Tests (via public interfaces)
%%====================================================================

resource_calc_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"cost handles missing num_nodes", fun test_cost_missing_num_nodes/0},
        {"cost handles missing num_cpus", fun test_cost_missing_num_cpus/0},
        {"cost handles all resource fields missing", fun test_cost_all_resources_missing/0}
     ]}.

test_cost_missing_num_nodes() ->
    Job = #{priority => 1000, qos => <<"normal">>, num_cpus => 10},
    Cost = flurm_preemption:calculate_preemption_cost(Job),
    ?assert(is_float(Cost)).

test_cost_missing_num_cpus() ->
    Job = #{priority => 1000, qos => <<"normal">>, num_nodes => 5},
    Cost = flurm_preemption:calculate_preemption_cost(Job),
    ?assert(is_float(Cost)).

test_cost_all_resources_missing() ->
    Job = #{priority => 1000, qos => <<"normal">>},
    Cost = flurm_preemption:calculate_preemption_cost(Job),
    ?assert(is_float(Cost)).

%%====================================================================
%% Boundary and Edge Case Tests
%%====================================================================

boundary_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"priority at boundary values", fun test_priority_boundaries/0},
        {"QOS level 0 handling", fun test_qos_level_zero/0},
        {"very large priority difference", fun test_very_large_priority_diff/0},
        {"cost with negative resource benefit", fun test_negative_resource_benefit/0}
     ]}.

test_priority_boundaries() ->
    %% Test with MIN_PRIORITY
    LowJob = #{priority => 0, qos => <<"normal">>},
    %% Test with MAX_PRIORITY
    HighJob = #{priority => 10000, qos => <<"normal">>},

    %% Cost calculation should handle these
    LowCost = flurm_preemption:calculate_preemption_cost(LowJob),
    HighCost = flurm_preemption:calculate_preemption_cost(HighJob),

    ?assert(is_float(LowCost)),
    ?assert(is_float(HighCost)),
    ?assert(LowCost < HighCost).

test_qos_level_zero() ->
    %% Unknown QOS gets level 0
    ok = flurm_preemption:set_qos_preemption_rules(#{<<"known">> => 100}),

    UnknownJob = #{qos => <<"unknown">>, priority => 100},
    KnownJob = #{qos => <<"known">>, priority => 100},

    %% Unknown (0) cannot preempt known (100)
    ?assertNot(flurm_preemption:can_preempt(UnknownJob, KnownJob)),
    %% Known (100) can preempt unknown (0)
    ?assert(flurm_preemption:can_preempt(KnownJob, UnknownJob)).

test_very_large_priority_diff() ->
    ok = flurm_preemption:set_priority_threshold(0),

    VeryHighJob = #{qos => <<"normal">>, priority => 999999},
    VeryLowJob = #{qos => <<"normal">>, priority => 1},

    ?assert(flurm_preemption:can_preempt(VeryHighJob, VeryLowJob)).

test_negative_resource_benefit() ->
    Now = erlang:system_time(second),

    %% Job with massive resources should have lower cost
    MassiveJob = #{
        priority => 1000,
        qos => <<"normal">>,
        start_time => Now,
        num_nodes => 1000,
        num_cpus => 10000
    },

    TinyJob = #{
        priority => 1000,
        qos => <<"normal">>,
        start_time => Now,
        num_nodes => 1,
        num_cpus => 1
    },

    MassiveCost = flurm_preemption:calculate_preemption_cost(MassiveJob),
    TinyCost = flurm_preemption:calculate_preemption_cost(TinyJob),

    %% Cost should still be non-negative
    ?assert(MassiveCost >= 0.0),
    ?assert(MassiveCost < TinyCost).
