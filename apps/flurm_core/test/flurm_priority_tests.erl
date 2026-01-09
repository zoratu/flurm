%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_priority module
%%%-------------------------------------------------------------------
-module(flurm_priority_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure the weights table is fresh
    catch ets:delete(flurm_priority_weights),
    ok.

cleanup(_) ->
    catch ets:delete(flurm_priority_weights),
    ok.

priority_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"set and get weights", fun test_set_get_weights/0},
      {"default weights", fun test_default_weights/0},
      {"age factor calculation", fun test_age_factor/0},
      {"size factor calculation", fun test_size_factor/0},
      {"nice factor calculation", fun test_nice_factor/0},
      {"partition factor calculation", fun test_partition_factor/0},
      {"full priority calculation", fun test_calculate_priority/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_set_get_weights() ->
    %% Default weights should be returned
    Defaults = flurm_priority:get_weights(),
    ?assert(maps:is_key(age, Defaults)),
    ?assert(maps:is_key(fairshare, Defaults)),

    %% Set custom weights
    CustomWeights = #{age => 500, fairshare => 1000, job_size => 100},
    ok = flurm_priority:set_weights(CustomWeights),

    %% Get should return custom weights
    Retrieved = flurm_priority:get_weights(),
    ?assertEqual(500, maps:get(age, Retrieved)),
    ?assertEqual(1000, maps:get(fairshare, Retrieved)).

test_default_weights() ->
    Weights = flurm_priority:get_weights(),
    %% Check all expected weights exist
    ?assert(maps:is_key(age, Weights)),
    ?assert(maps:is_key(job_size, Weights)),
    ?assert(maps:is_key(partition, Weights)),
    ?assert(maps:is_key(qos, Weights)),
    ?assert(maps:is_key(nice, Weights)),
    ?assert(maps:is_key(fairshare, Weights)).

test_age_factor() ->
    Now = erlang:system_time(second),

    %% Brand new job should have low age factor
    NewJob = #{submit_time => Now},
    AgeFactor0 = flurm_priority:age_factor(NewJob, Now),
    ?assert(AgeFactor0 < 0.01),

    %% Job from 1 day ago should have higher factor
    OneDayAgo = Now - 86400,
    OldJob = #{submit_time => OneDayAgo},
    AgeFactor1 = flurm_priority:age_factor(OldJob, Now),
    ?assert(AgeFactor1 > 0.5),

    %% Very old job approaches 1.0
    OneWeekAgo = Now - 604800,
    VeryOldJob = #{submit_time => OneWeekAgo},
    AgeFactorMax = flurm_priority:age_factor(VeryOldJob, Now),
    ?assert(AgeFactorMax > 0.9).

test_size_factor() ->
    %% Small job should have higher factor (inverted size)
    SmallJob = #{num_nodes => 1, num_cpus => 1},
    SmallFactor = flurm_priority:size_factor(SmallJob, 100),
    ?assert(SmallFactor > 0.9),

    %% Large job should have lower factor
    LargeJob = #{num_nodes => 50, num_cpus => 64},
    LargeFactor = flurm_priority:size_factor(LargeJob, 100),
    ?assert(LargeFactor < SmallFactor).

test_nice_factor() ->
    %% Negative nice (higher priority)
    HighPriorityJob = #{nice => -5000},
    HighFactor = flurm_priority:nice_factor(HighPriorityJob),
    ?assert(HighFactor > 0),

    %% Zero nice (normal priority)
    NormalJob = #{nice => 0},
    NormalFactor = flurm_priority:nice_factor(NormalJob),
    ?assertEqual(0.0, NormalFactor),

    %% Positive nice (lower priority)
    LowPriorityJob = #{nice => 5000},
    LowFactor = flurm_priority:nice_factor(LowPriorityJob),
    ?assert(LowFactor < 0).

test_partition_factor() ->
    %% Without partition registry, should return default 0.5
    Job = #{partition => <<"nonexistent">>},
    Factor = flurm_priority:partition_factor(Job),
    ?assertEqual(0.5, Factor).

test_calculate_priority() ->
    Now = erlang:system_time(second),

    %% Basic job
    Job = #{
        submit_time => Now,
        num_nodes => 1,
        num_cpus => 1,
        partition => <<"default">>,
        qos => <<"normal">>,
        nice => 0,
        user => <<"testuser">>,
        account => <<"testaccount">>
    },

    Priority = flurm_priority:calculate_priority(Job),

    %% Priority should be a positive integer
    ?assert(is_integer(Priority)),
    ?assert(Priority >= 0).
