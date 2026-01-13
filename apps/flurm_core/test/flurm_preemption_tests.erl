%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_preemption module
%%%
%%% Tests configuration, preemption logic, job comparison, and
%%% scheduler integration functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_preemption_tests).
-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure config table exists and is clean
    catch ets:delete(flurm_preemption_config),
    ok.

cleanup(_) ->
    catch ets:delete(flurm_preemption_config),
    ok.

%%====================================================================
%% Basic Configuration Tests
%%====================================================================

preemption_config_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"default preemption mode", fun test_default_mode/0},
        {"set and get preemption mode", fun test_set_mode/0},
        {"all valid preemption modes", fun test_all_modes/0},
        {"default grace time", fun test_default_grace_time/0},
        {"set and get grace time", fun test_grace_time/0},
        {"default priority threshold", fun test_default_priority_threshold/0},
        {"set and get priority threshold", fun test_priority_threshold/0},
        {"default QOS rules", fun test_default_qos_rules/0},
        {"custom QOS rules", fun test_custom_qos_rules/0}
     ]}.

test_default_mode() ->
    Mode = flurm_preemption:get_preemption_mode(),
    ?assertEqual(requeue, Mode).

test_set_mode() ->
    ok = flurm_preemption:set_preemption_mode(cancel),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode()).

test_all_modes() ->
    %% Test all valid preemption modes
    Modes = [requeue, cancel, checkpoint, suspend, off],
    lists:foreach(fun(Mode) ->
        ok = flurm_preemption:set_preemption_mode(Mode),
        ?assertEqual(Mode, flurm_preemption:get_preemption_mode())
    end, Modes).

test_default_grace_time() ->
    DefaultTime = flurm_preemption:get_grace_time(),
    ?assertEqual(60, DefaultTime).

test_grace_time() ->
    ok = flurm_preemption:set_grace_time(120),
    ?assertEqual(120, flurm_preemption:get_grace_time()),

    %% Test different values
    ok = flurm_preemption:set_grace_time(30),
    ?assertEqual(30, flurm_preemption:get_grace_time()).

test_default_priority_threshold() ->
    Threshold = flurm_preemption:get_priority_threshold(),
    ?assertEqual(1000, Threshold).

test_priority_threshold() ->
    ok = flurm_preemption:set_priority_threshold(500),
    ?assertEqual(500, flurm_preemption:get_priority_threshold()),

    ok = flurm_preemption:set_priority_threshold(0),
    ?assertEqual(0, flurm_preemption:get_priority_threshold()).

test_default_qos_rules() ->
    Rules = flurm_preemption:get_qos_preemption_rules(),
    ?assert(is_map(Rules)),
    ?assertEqual(300, maps:get(<<"high">>, Rules)),
    ?assertEqual(200, maps:get(<<"normal">>, Rules)),
    ?assertEqual(100, maps:get(<<"low">>, Rules)).

test_custom_qos_rules() ->
    CustomRules = #{
        <<"critical">> => 500,
        <<"high">> => 400,
        <<"normal">> => 200,
        <<"low">> => 50
    },
    ok = flurm_preemption:set_qos_preemption_rules(CustomRules),
    Retrieved = flurm_preemption:get_qos_preemption_rules(),
    ?assertEqual(500, maps:get(<<"critical">>, Retrieved)),
    ?assertEqual(50, maps:get(<<"low">>, Retrieved)).

%%====================================================================
%% Partition-Specific Configuration Tests
%%====================================================================

partition_config_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"partition-specific preemption mode", fun test_partition_mode/0},
        {"partition-specific grace time", fun test_partition_grace_time/0},
        {"get mode with partition context", fun test_get_mode_with_partition/0},
        {"get mode with QOS context", fun test_get_mode_with_qos/0}
     ]}.

test_partition_mode() ->
    %% Set different modes for different partitions
    ok = flurm_preemption:set_partition_preemption_mode(<<"gpu">>, cancel),
    ok = flurm_preemption:set_partition_preemption_mode(<<"batch">>, requeue),
    ok = flurm_preemption:set_partition_preemption_mode(<<"interactive">>, off),

    %% Verify partition-specific modes
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode(#{partition => <<"gpu">>})),
    ?assertEqual(requeue, flurm_preemption:get_preemption_mode(#{partition => <<"batch">>})),
    ?assertEqual(off, flurm_preemption:get_preemption_mode(#{partition => <<"interactive">>})).

test_partition_grace_time() ->
    ok = flurm_preemption:set_partition_grace_time(<<"gpu">>, 30),
    ok = flurm_preemption:set_partition_grace_time(<<"batch">>, 300),

    ?assertEqual(30, flurm_preemption:get_grace_time(<<"gpu">>)),
    ?assertEqual(300, flurm_preemption:get_grace_time(<<"batch">>)),
    %% Non-configured partition should fall back to default
    ?assertEqual(60, flurm_preemption:get_grace_time(<<"other">>)).

test_get_mode_with_partition() ->
    %% Set partition-specific mode
    ok = flurm_preemption:set_partition_preemption_mode(<<"special">>, suspend),

    %% Get mode with partition context
    Mode = flurm_preemption:get_preemption_mode(#{partition => <<"special">>}),
    ?assertEqual(suspend, Mode).

test_get_mode_with_qos() ->
    %% Without partition, should use global mode
    GlobalMode = flurm_preemption:get_preemption_mode(#{qos => <<"high">>}),
    ?assertEqual(requeue, GlobalMode).

%%====================================================================
%% Preemption Logic Tests
%%====================================================================

preemption_logic_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"can_preempt by QOS - high preempts low", fun test_can_preempt_qos_high_low/0},
        {"can_preempt by QOS - high preempts normal", fun test_can_preempt_qos_high_normal/0},
        {"can_preempt by QOS - normal preempts low", fun test_can_preempt_qos_normal_low/0},
        {"cannot preempt - low cannot preempt high", fun test_cannot_preempt_low_high/0},
        {"cannot preempt - same QOS without priority diff", fun test_cannot_preempt_same_qos_no_diff/0},
        {"can_preempt by priority - large difference", fun test_can_preempt_priority_large_diff/0},
        {"cannot preempt - small priority difference", fun test_cannot_preempt_priority_small_diff/0},
        {"preemption disabled check", fun test_preemption_disabled/0},
        {"check_preemption when disabled", fun test_check_preemption_disabled/0}
     ]}.

test_can_preempt_qos_high_low() ->
    HighJob = #{qos => <<"high">>, priority => 1000},
    LowJob = #{qos => <<"low">>, priority => 1000},
    ?assert(flurm_preemption:can_preempt(HighJob, LowJob)).

test_can_preempt_qos_high_normal() ->
    HighJob = #{qos => <<"high">>, priority => 1000},
    NormalJob = #{qos => <<"normal">>, priority => 1000},
    ?assert(flurm_preemption:can_preempt(HighJob, NormalJob)).

test_can_preempt_qos_normal_low() ->
    NormalJob = #{qos => <<"normal">>, priority => 1000},
    LowJob = #{qos => <<"low">>, priority => 1000},
    ?assert(flurm_preemption:can_preempt(NormalJob, LowJob)).

test_cannot_preempt_low_high() ->
    LowJob = #{qos => <<"low">>, priority => 1000},
    HighJob = #{qos => <<"high">>, priority => 1000},
    ?assertNot(flurm_preemption:can_preempt(LowJob, HighJob)).

test_cannot_preempt_same_qos_no_diff() ->
    Job1 = #{qos => <<"normal">>, priority => 1000},
    Job2 = #{qos => <<"normal">>, priority => 900},
    %% Needs > 1000 priority difference
    ?assertNot(flurm_preemption:can_preempt(Job1, Job2)).

test_can_preempt_priority_large_diff() ->
    %% Same QOS but large priority difference
    HighPriorityJob = #{qos => <<"normal">>, priority => 5000},
    LowPriorityJob = #{qos => <<"normal">>, priority => 1000},
    %% Difference is 4000 > 1000 threshold
    ?assert(flurm_preemption:can_preempt(HighPriorityJob, LowPriorityJob)).

test_cannot_preempt_priority_small_diff() ->
    Job1 = #{qos => <<"normal">>, priority => 1500},
    Job2 = #{qos => <<"normal">>, priority => 1000},
    %% Difference is 500 < 1000 threshold
    ?assertNot(flurm_preemption:can_preempt(Job1, Job2)).

test_preemption_disabled() ->
    flurm_preemption:set_preemption_mode(off),
    PendingJob = #{
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 8192,
        priority => 10000
    },
    Result = flurm_preemption:check_preemption(PendingJob),
    ?assertEqual({error, preemption_disabled}, Result).

test_check_preemption_disabled() ->
    flurm_preemption:set_preemption_mode(off),
    PendingJob = #{priority => 10000, partition => <<"default">>},
    ResourcesNeeded = #{num_nodes => 1, num_cpus => 4},
    Result = flurm_preemption:check_preemption_opportunity(PendingJob, ResourcesNeeded),
    ?assertEqual({error, preemption_disabled}, Result).

%%====================================================================
%% Preemption Cost Calculation Tests
%%====================================================================

preemption_cost_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"low priority job has lower cost", fun test_cost_low_priority/0},
        {"low QOS job has lower cost", fun test_cost_low_qos/0},
        {"newly started job has lower cost", fun test_cost_new_job/0},
        {"job with more resources has lower cost", fun test_cost_more_resources/0}
     ]}.

test_cost_low_priority() ->
    LowPriorityJob = #{priority => 100, qos => <<"normal">>, start_time => erlang:system_time(second)},
    HighPriorityJob = #{priority => 9000, qos => <<"normal">>, start_time => erlang:system_time(second)},
    LowCost = flurm_preemption:calculate_preemption_cost(LowPriorityJob),
    HighCost = flurm_preemption:calculate_preemption_cost(HighPriorityJob),
    ?assert(LowCost < HighCost).

test_cost_low_qos() ->
    LowQOSJob = #{priority => 1000, qos => <<"low">>, start_time => erlang:system_time(second)},
    HighQOSJob = #{priority => 1000, qos => <<"high">>, start_time => erlang:system_time(second)},
    LowCost = flurm_preemption:calculate_preemption_cost(LowQOSJob),
    HighCost = flurm_preemption:calculate_preemption_cost(HighQOSJob),
    ?assert(LowCost < HighCost).

test_cost_new_job() ->
    Now = erlang:system_time(second),
    NewJob = #{priority => 1000, qos => <<"normal">>, start_time => Now},
    OldJob = #{priority => 1000, qos => <<"normal">>, start_time => Now - 3600}, % 1 hour ago
    NewCost = flurm_preemption:calculate_preemption_cost(NewJob),
    OldCost = flurm_preemption:calculate_preemption_cost(OldJob),
    ?assert(NewCost < OldCost).

test_cost_more_resources() ->
    SmallJob = #{priority => 1000, qos => <<"normal">>, num_nodes => 1, num_cpus => 1,
                 start_time => erlang:system_time(second)},
    BigJob = #{priority => 1000, qos => <<"normal">>, num_nodes => 10, num_cpus => 100,
               start_time => erlang:system_time(second)},
    SmallCost = flurm_preemption:calculate_preemption_cost(SmallJob),
    BigCost = flurm_preemption:calculate_preemption_cost(BigJob),
    %% Big job should have lower cost (negative resource benefit)
    ?assert(BigCost < SmallCost).

%%====================================================================
%% Preempt Jobs Function Tests
%%====================================================================

preempt_jobs_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"preempt_jobs returns job IDs", fun test_preempt_jobs_returns_ids/0}
     ]}.

test_preempt_jobs_returns_ids() ->
    %% Test with empty job list
    {ok, Preempted} = flurm_preemption:preempt_jobs([], requeue),
    ?assertEqual([], Preempted).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job with missing QOS uses default", fun test_missing_qos/0},
        {"job with missing priority uses default", fun test_missing_priority/0},
        {"get mode with empty options", fun test_get_mode_empty_opts/0}
     ]}.

test_missing_qos() ->
    Job1 = #{priority => 5000},  % No QOS specified
    Job2 = #{priority => 1000, qos => <<"low">>},
    %% Job1 should use normal QOS by default
    %% Since normal > low, Job1 can preempt Job2
    ?assert(flurm_preemption:can_preempt(Job1, Job2)).

test_missing_priority() ->
    Job1 = #{qos => <<"high">>},  % No priority specified
    Job2 = #{qos => <<"low">>, priority => 1000},
    %% Should still work with QOS-based preemption
    ?assert(flurm_preemption:can_preempt(Job1, Job2)).

test_get_mode_empty_opts() ->
    Mode = flurm_preemption:get_preemption_mode(#{}),
    ?assertEqual(requeue, Mode).

%%====================================================================
%% Handle Preempted Job Mode Tests
%%====================================================================

handle_preempted_job_mode_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"handle_preempted_job with off mode", fun test_handle_preempted_off/0}
     ]}.

test_handle_preempted_off() ->
    Result = flurm_preemption:handle_preempted_job(1, off),
    ?assertEqual({error, preemption_disabled}, Result).
