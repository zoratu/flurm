%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_preemption module
%%%-------------------------------------------------------------------
-module(flurm_preemption_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure config table exists
    catch ets:delete(flurm_preemption_config),
    ok.

cleanup(_) ->
    catch ets:delete(flurm_preemption_config),
    ok.

preemption_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"default preemption mode", fun test_default_mode/0},
      {"set preemption mode", fun test_set_mode/0},
      {"set grace time", fun test_grace_time/0},
      {"can_preempt by QOS", fun test_can_preempt_qos/0},
      {"can_preempt by priority", fun test_can_preempt_priority/0},
      {"preemption disabled", fun test_preemption_disabled/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_default_mode() ->
    Mode = flurm_preemption:get_preemption_mode(),
    ?assertEqual(requeue, Mode).

test_set_mode() ->
    %% Test all valid modes
    ok = flurm_preemption:set_preemption_mode(cancel),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode()),

    ok = flurm_preemption:set_preemption_mode(checkpoint),
    ?assertEqual(checkpoint, flurm_preemption:get_preemption_mode()),

    ok = flurm_preemption:set_preemption_mode(suspend),
    ?assertEqual(suspend, flurm_preemption:get_preemption_mode()),

    ok = flurm_preemption:set_preemption_mode(off),
    ?assertEqual(off, flurm_preemption:get_preemption_mode()),

    ok = flurm_preemption:set_preemption_mode(requeue),
    ?assertEqual(requeue, flurm_preemption:get_preemption_mode()).

test_grace_time() ->
    %% Default grace time
    DefaultTime = flurm_preemption:get_grace_time(),
    ?assertEqual(60, DefaultTime),

    %% Set custom grace time
    ok = flurm_preemption:set_grace_time(120),
    ?assertEqual(120, flurm_preemption:get_grace_time()).

test_can_preempt_qos() ->
    %% High QOS can preempt low
    HighJob = #{qos => <<"high">>, priority => 1000},
    LowJob = #{qos => <<"low">>, priority => 1000},
    ?assert(flurm_preemption:can_preempt(HighJob, LowJob)),

    %% High QOS can preempt normal
    NormalJob = #{qos => <<"normal">>, priority => 1000},
    ?assert(flurm_preemption:can_preempt(HighJob, NormalJob)),

    %% Normal QOS can preempt low
    ?assert(flurm_preemption:can_preempt(NormalJob, LowJob)),

    %% Low QOS cannot preempt high
    ?assertNot(flurm_preemption:can_preempt(LowJob, HighJob)).

test_can_preempt_priority() ->
    %% Same QOS, need significant priority difference
    Job1 = #{qos => <<"normal">>, priority => 5000},
    Job2 = #{qos => <<"normal">>, priority => 1000},

    %% Job1 has higher priority by 4000, but needs > 1000 difference
    ?assert(flurm_preemption:can_preempt(Job1, Job2)),

    %% Job with only slightly higher priority cannot preempt
    Job3 = #{qos => <<"normal">>, priority => 1500},
    ?assertNot(flurm_preemption:can_preempt(Job3, Job2)).

test_preemption_disabled() ->
    %% Disable preemption
    flurm_preemption:set_preemption_mode(off),

    PendingJob = #{
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 8192,
        priority => 10000
    },

    %% Check should fail when disabled
    Result = flurm_preemption:check_preemption(PendingJob),
    ?assertEqual({error, preemption_disabled}, Result).
