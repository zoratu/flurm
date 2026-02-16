%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_simulation module
%%% Tests for deterministic simulation testing framework
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_simulation_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Note: These tests call the actual simulation functions.
%% The simulation module is designed to be deterministic and
%% can run without external dependencies.

%%====================================================================
%% API Function Tests
%%====================================================================

scenarios_test() ->
    %% Test that scenarios/0 returns a list of scenario atoms
    Scenarios = flurm_simulation:scenarios(),
    ?assert(is_list(Scenarios)),
    ?assert(length(Scenarios) > 0),
    %% Check that all scenarios are atoms
    lists:foreach(fun(S) -> ?assert(is_atom(S)) end, Scenarios),
    %% Check for expected scenarios
    ?assert(lists:member(submit_job_single_node, Scenarios)),
    ?assert(lists:member(submit_job_multi_node, Scenarios)),
    ?assert(lists:member(job_completion, Scenarios)),
    ?assert(lists:member(leader_election_normal, Scenarios)),
    ?assert(lists:member(partition_minority, Scenarios)),
    ?assert(lists:member(sibling_job_basic, Scenarios)).

run_single_node_test_() ->
    {timeout, 30, fun() ->
        %% Test basic single node simulation
        {ok, Result} = flurm_simulation:run(42, submit_job_single_node),
        ?assert(is_tuple(Result)),
        %% Result is a sim_result record
        ?assertEqual(42, element(2, Result)),  % seed field
        ?assertEqual(submit_job_single_node, element(3, Result))  % scenario field
    end}.

run_multi_node_test_() ->
    {timeout, 30, fun() ->
        %% Test multi-node simulation
        {ok, Result} = flurm_simulation:run(123, submit_job_multi_node),
        ?assert(is_tuple(Result)),
        ?assertEqual(123, element(2, Result)),
        ?assertEqual(submit_job_multi_node, element(3, Result))
    end}.

run_job_completion_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(1, job_completion),
        ?assert(is_tuple(Result)),
        ?assertEqual(job_completion, element(3, Result))
    end}.

run_job_cancellation_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(2, job_cancellation),
        ?assert(is_tuple(Result)),
        ?assertEqual(job_cancellation, element(3, Result))
    end}.

run_leader_election_normal_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(3, leader_election_normal),
        ?assert(is_tuple(Result)),
        ?assertEqual(leader_election_normal, element(3, Result))
    end}.

run_leader_failure_during_submit_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(4, leader_failure_during_submit),
        ?assert(is_tuple(Result)),
        ?assertEqual(leader_failure_during_submit, element(3, Result))
    end}.

run_partition_minority_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(5, partition_minority),
        ?assert(is_tuple(Result)),
        ?assertEqual(partition_minority, element(3, Result))
    end}.

run_partition_majority_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(6, partition_majority),
        ?assert(is_tuple(Result)),
        ?assertEqual(partition_majority, element(3, Result))
    end}.

run_partition_during_job_run_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(7, partition_during_job_run),
        ?assert(is_tuple(Result)),
        ?assertEqual(partition_during_job_run, element(3, Result))
    end}.

run_sibling_job_basic_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(8, sibling_job_basic),
        ?assert(is_tuple(Result)),
        ?assertEqual(sibling_job_basic, element(3, Result))
    end}.

run_sibling_job_race_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(9, sibling_job_race),
        ?assert(is_tuple(Result)),
        ?assertEqual(sibling_job_race, element(3, Result))
    end}.

run_sibling_revocation_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(10, sibling_revocation),
        ?assert(is_tuple(Result)),
        ?assertEqual(sibling_revocation, element(3, Result))
    end}.

run_origin_cluster_failure_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(11, origin_cluster_failure),
        ?assert(is_tuple(Result)),
        ?assertEqual(origin_cluster_failure, element(3, Result))
    end}.

run_cascading_node_failures_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(12, cascading_node_failures),
        ?assert(is_tuple(Result)),
        ?assertEqual(cascading_node_failures, element(3, Result))
    end}.

run_migration_shadow_to_active_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(13, migration_shadow_to_active),
        ?assert(is_tuple(Result)),
        ?assertEqual(migration_shadow_to_active, element(3, Result))
    end}.

run_migration_with_failures_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(14, migration_with_failures),
        ?assert(is_tuple(Result)),
        ?assertEqual(migration_with_failures, element(3, Result))
    end}.

run_unknown_scenario_test_() ->
    {timeout, 30, fun() ->
        %% Test with an unknown scenario (uses default config)
        {ok, Result} = flurm_simulation:run(99, unknown_scenario),
        ?assert(is_tuple(Result)),
        ?assertEqual(unknown_scenario, element(3, Result))
    end}.

%% Test multiple seeds to exercise deterministic behavior
run_deterministic_test_() ->
    {timeout, 30, fun() ->
        %% Running with the same seed should produce the same result
        {ok, Result1} = flurm_simulation:run(42, submit_job_single_node),
        {ok, Result2} = flurm_simulation:run(42, submit_job_single_node),
        %% The results should be identical for the same seed
        ?assertEqual(element(2, Result1), element(2, Result2)),  % seed
        ?assertEqual(element(3, Result1), element(3, Result2)),  % scenario
        ?assertEqual(element(5, Result1), element(5, Result2))   % steps
    end}.

run_different_seeds_test_() ->
    {timeout, 30, fun() ->
        %% Running with different seeds may produce different step counts
        {ok, Result1} = flurm_simulation:run(1, submit_job_multi_node),
        {ok, Result2} = flurm_simulation:run(999, submit_job_multi_node),
        ?assertEqual(1, element(2, Result1)),
        ?assertEqual(999, element(2, Result2))
    end}.

run_all_scenarios_test_() ->
    {timeout, 120, fun() ->
        %% Run all scenarios with just 1 seed each (for speed)
        {ok, Results} = flurm_simulation:run_all_scenarios(1),
        ?assert(is_map(Results)),
        ?assert(maps:is_key(passed, Results)),
        ?assert(maps:is_key(failed, Results)),
        Passed = maps:get(passed, Results),
        Failed = maps:get(failed, Results),
        ?assert(is_integer(Passed)),
        ?assert(is_integer(Failed)),
        ?assert(Passed >= 0),
        ?assert(Failed >= 0)
    end}.

%%====================================================================
%% Internal Function Tests (via -ifdef(TEST) exports)
%%====================================================================

init_simulation_test_() ->
    {timeout, 10, fun() ->
        %% Test init_simulation with various scenarios
        State1 = flurm_simulation:init_simulation(1, submit_job_single_node),
        ?assert(is_tuple(State1)),

        State2 = flurm_simulation:init_simulation(2, leader_election_normal),
        ?assert(is_tuple(State2)),

        State3 = flurm_simulation:init_simulation(3, partition_minority),
        ?assert(is_tuple(State3))
    end}.

step_test_() ->
    {timeout, 10, fun() ->
        %% Initialize and take a step
        State0 = flurm_simulation:init_simulation(1, submit_job_single_node),
        {Result, State1} = flurm_simulation:step(State0),
        ?assert(Result =:= done orelse Result =:= continue),
        ?assert(is_tuple(State1))
    end}.

check_invariants_test_() ->
    {timeout, 10, fun() ->
        %% Test invariant checking
        State0 = flurm_simulation:init_simulation(1, submit_job_multi_node),
        State1 = flurm_simulation:check_invariants(State0),
        ?assert(is_tuple(State1))
    end}.

inject_failures_test_() ->
    {timeout, 10, fun() ->
        %% Test failure injection
        State0 = flurm_simulation:init_simulation(1, submit_job_multi_node),
        State1 = flurm_simulation:inject_failures(State0),
        ?assert(is_tuple(State1))
    end}.

%% Test deliver_message with a constructed message
deliver_message_test_() ->
    {timeout, 10, fun() ->
        %% Initialize state
        State0 = flurm_simulation:init_simulation(1, submit_job_multi_node),
        %% Construct a sim_message record
        %% The record is: sim_message{from, to, type, payload, send_time, delivery_time}
        Msg = {sim_message, <<"node1">>, <<"node2">>, test_msg, #{}, 0, 1},
        State1 = flurm_simulation:deliver_message(Msg, State0),
        ?assert(is_tuple(State1))
    end}.

%%====================================================================
%% Edge Case Tests
%%====================================================================

run_with_zero_seed_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(0, submit_job_single_node),
        ?assert(is_tuple(Result)),
        ?assertEqual(0, element(2, Result))
    end}.

run_with_negative_seed_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(-1, submit_job_single_node),
        ?assert(is_tuple(Result)),
        ?assertEqual(-1, element(2, Result))
    end}.

run_with_large_seed_test_() ->
    {timeout, 30, fun() ->
        {ok, Result} = flurm_simulation:run(1000000000, submit_job_single_node),
        ?assert(is_tuple(Result)),
        ?assertEqual(1000000000, element(2, Result))
    end}.
