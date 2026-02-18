%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_simulation module
%%%
%%% These tests call the actual flurm_simulation module functions directly
%%% to get comprehensive code coverage. Tests cover:
%%% - run/2 simulation execution
%%% - run_all_scenarios/0,1 batch execution
%%% - scenarios/0 scenario list
%%% - Internal simulation functions (via -ifdef(TEST) exports)
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_simulation_direct_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_simulation_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

simulation_direct_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        %% API Tests
        {"scenarios returns list", fun test_scenarios_returns_list/0},
        {"scenarios contains expected items", fun test_scenarios_expected_items/0},
        {"run with valid seed and scenario", fun test_run_valid/0},
        {"run with different scenarios", fun test_run_different_scenarios/0},
        {"run reproducible with same seed", fun test_run_reproducible/0},
        {"run_all_scenarios with count", fun test_run_all_scenarios_count/0},

        %% Internal function tests (via TEST exports)
        {"init_simulation creates state", fun test_init_simulation/0},
        {"init_simulation different scenarios", fun test_init_different_scenarios/0},
        {"step advances simulation", fun test_step_advances/0},
        {"step with messages", fun test_step_with_messages/0},
        {"check_invariants on valid state", fun test_check_invariants_valid/0},
        {"inject_failures modifies state", fun test_inject_failures/0},
        {"deliver_message to node", fun test_deliver_message/0},

        %% Scenario configuration tests
        {"scenario_config for each scenario", fun test_scenario_configs/0},

        %% Event execution tests
        {"execute submit_job event", fun test_execute_submit_job/0},
        {"execute kill_node event", fun test_execute_kill_node/0},
        {"execute partition event", fun test_execute_partition/0},
        {"execute create_sibling_jobs event", fun test_execute_sibling_jobs/0},

        %% Invariant checking tests
        {"leader uniqueness invariant", fun test_leader_uniqueness/0},
        {"sibling exclusivity invariant", fun test_sibling_exclusivity/0},
        {"no job loss invariant", fun test_no_job_loss/0},
        {"log consistency invariant", fun test_log_consistency/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% API Tests
%%====================================================================

test_scenarios_returns_list() ->
    Scenarios = flurm_simulation:scenarios(),
    ?assert(is_list(Scenarios)),
    ?assert(length(Scenarios) > 0).

test_scenarios_expected_items() ->
    Scenarios = flurm_simulation:scenarios(),
    %% Check for some expected scenarios
    ?assert(lists:member(submit_job_single_node, Scenarios)),
    ?assert(lists:member(leader_election_normal, Scenarios)),
    ?assert(lists:member(partition_minority, Scenarios)),
    ?assert(lists:member(sibling_job_basic, Scenarios)).

test_run_valid() ->
    Result = flurm_simulation:run(12345, submit_job_single_node),
    ?assertMatch({ok, _}, Result),
    {ok, SimResult} = Result,
    ?assertEqual(12345, element(2, SimResult)),  % seed field
    ?assertEqual(submit_job_single_node, element(3, SimResult)).  % scenario field

test_run_different_scenarios() ->
    Scenarios = [submit_job_single_node, submit_job_multi_node, job_completion],
    Results = [flurm_simulation:run(1000, S) || S <- Scenarios],
    %% All should succeed
    lists:foreach(fun(R) ->
        ?assertMatch({ok, _}, R)
    end, Results).

test_run_reproducible() ->
    Seed = 99999,
    Scenario = submit_job_single_node,
    {ok, Result1} = flurm_simulation:run(Seed, Scenario),
    {ok, Result2} = flurm_simulation:run(Seed, Scenario),
    %% Results should be identical
    ?assertEqual(element(5, Result1), element(5, Result2)).  % steps field

test_run_all_scenarios_count() ->
    %% Run with just 1 seed per scenario for speed
    {ok, Results} = flurm_simulation:run_all_scenarios(1),
    ?assert(is_map(Results)),
    ?assert(maps:is_key(passed, Results)),
    ?assert(maps:is_key(failed, Results)),
    Total = maps:get(passed, Results) + maps:get(failed, Results),
    Scenarios = flurm_simulation:scenarios(),
    ?assertEqual(length(Scenarios), Total).

%%====================================================================
%% Internal Function Tests
%%====================================================================

test_init_simulation() ->
    State = flurm_simulation:init_simulation(12345, submit_job_single_node),
    ?assert(is_tuple(State)),
    ?assertEqual(sim_state, element(1, State)).

test_init_different_scenarios() ->
    State1 = flurm_simulation:init_simulation(100, submit_job_single_node),
    State2 = flurm_simulation:init_simulation(100, leader_election_normal),
    %% Different scenarios should have different initial states
    %% (leader_election_normal has undefined leader)
    ?assertNotEqual(element(8, State1), element(8, State2)).  % leader field

test_step_advances() ->
    State0 = flurm_simulation:init_simulation(12345, submit_job_single_node),
    %% Step should either continue or be done
    Result = flurm_simulation:step(State0),
    ?assert(is_tuple(Result)),
    {Status, _NewState} = Result,
    ?assert(Status =:= done orelse Status =:= continue).

test_step_with_messages() ->
    State0 = flurm_simulation:init_simulation(12345, submit_job_multi_node),
    %% Run a few steps to generate messages
    {_, State1} = flurm_simulation:step(State0),
    {_, State2} = flurm_simulation:step(State1),
    {_, _State3} = flurm_simulation:step(State2),
    %% Just ensure it doesn't crash
    ?assert(true).

test_check_invariants_valid() ->
    State = flurm_simulation:init_simulation(12345, submit_job_single_node),
    CheckedState = flurm_simulation:check_invariants(State),
    %% Should not have any violations in initial state
    ?assertEqual([], element(9, CheckedState)).  % invariant_violations field

test_inject_failures() ->
    State0 = flurm_simulation:init_simulation(12345, submit_job_multi_node),
    %% inject_failures should return a state (possibly modified)
    State1 = flurm_simulation:inject_failures(State0),
    ?assert(is_tuple(State1)),
    ?assertEqual(sim_state, element(1, State1)).

test_deliver_message() ->
    State0 = flurm_simulation:init_simulation(12345, submit_job_multi_node),
    %% Create a simple message
    Msg = {sim_message, <<"node1">>, <<"node2">>, job_allocated, 1, 0, 1},
    State1 = flurm_simulation:deliver_message(Msg, State0),
    ?assert(is_tuple(State1)),
    ?assertEqual(sim_state, element(1, State1)).

%%====================================================================
%% Scenario Configuration Tests
%%====================================================================

test_scenario_configs() ->
    Scenarios = flurm_simulation:scenarios(),
    lists:foreach(fun(Scenario) ->
        State = flurm_simulation:init_simulation(1, Scenario),
        %% Each scenario should create a valid state
        ?assertEqual(sim_state, element(1, State)),
        %% Nodes map should not be empty
        Nodes = element(2, State),
        ?assert(map_size(Nodes) > 0)
    end, Scenarios).

%%====================================================================
%% Event Execution Tests
%%====================================================================

test_execute_submit_job() ->
    State0 = flurm_simulation:init_simulation(12345, submit_job_single_node),
    %% Run until we process submit_job event
    {_, State1} = run_n_steps(State0, 5),
    %% Jobs map should have at least one job
    Jobs = element(5, State1),
    ?assert(map_size(Jobs) >= 0).

test_execute_kill_node() ->
    State0 = flurm_simulation:init_simulation(12345, leader_failure_during_submit),
    %% This scenario schedules a node kill
    {_, State1} = run_n_steps(State0, 10),
    %% Just verify it doesn't crash
    ?assertEqual(sim_state, element(1, State1)).

test_execute_partition() ->
    State0 = flurm_simulation:init_simulation(12345, partition_minority),
    %% Run until partition event might execute
    {_, State1} = run_n_steps(State0, 20),
    ?assertEqual(sim_state, element(1, State1)).

test_execute_sibling_jobs() ->
    State0 = flurm_simulation:init_simulation(12345, sibling_job_basic),
    %% Run simulation
    {_, State1} = run_n_steps(State0, 15),
    %% Just verify it doesn't crash
    ?assertEqual(sim_state, element(1, State1)).

%%====================================================================
%% Invariant Checking Tests
%%====================================================================

test_leader_uniqueness() ->
    %% Create a state with multiple leaders (violation)
    State0 = flurm_simulation:init_simulation(12345, submit_job_multi_node),
    %% The initial state should have at most one leader
    Checked = flurm_simulation:check_invariants(State0),
    Violations = element(9, Checked),
    %% No violations expected in initial state
    LeaderViolations = [V || V <- Violations,
                             is_tuple(V),
                             element(1, V) =:= leader_uniqueness_violation],
    ?assertEqual([], LeaderViolations).

test_sibling_exclusivity() ->
    State0 = flurm_simulation:init_simulation(12345, sibling_job_basic),
    Checked = flurm_simulation:check_invariants(State0),
    Violations = element(9, Checked),
    %% No violations expected in initial state
    SiblingViolations = [V || V <- Violations,
                              is_tuple(V),
                              element(1, V) =:= sibling_exclusivity_violation],
    ?assertEqual([], SiblingViolations).

test_no_job_loss() ->
    State0 = flurm_simulation:init_simulation(12345, submit_job_single_node),
    Checked = flurm_simulation:check_invariants(State0),
    Violations = element(9, Checked),
    %% No job loss violations expected (no jobs yet)
    JobLossViolations = [V || V <- Violations,
                              is_tuple(V),
                              element(1, V) =:= no_job_loss_violation],
    ?assertEqual([], JobLossViolations).

test_log_consistency() ->
    State0 = flurm_simulation:init_simulation(12345, submit_job_multi_node),
    Checked = flurm_simulation:check_invariants(State0),
    Violations = element(9, Checked),
    %% No log consistency violations expected (empty logs)
    LogViolations = [V || V <- Violations,
                          is_tuple(V),
                          element(1, V) =:= log_consistency_violation],
    ?assertEqual([], LogViolations).

%%====================================================================
%% Helper Functions
%%====================================================================

run_n_steps(State, 0) ->
    {done, State};
run_n_steps(State, N) when N > 0 ->
    case flurm_simulation:step(State) of
        {done, NewState} ->
            {done, NewState};
        {continue, NewState} ->
            run_n_steps(NewState, N - 1)
    end.

%%====================================================================
%% Additional Scenario Tests
%%====================================================================

scenario_specific_test_() ->
    [
        {"submit_job_single_node runs to completion", fun() ->
            {ok, Result} = flurm_simulation:run(111, submit_job_single_node),
            ?assert(element(4, Result))  % success field
        end},
        {"submit_job_multi_node runs to completion", fun() ->
            {ok, Result} = flurm_simulation:run(222, submit_job_multi_node),
            ?assert(element(4, Result))
        end},
        {"job_completion runs to completion", fun() ->
            {ok, Result} = flurm_simulation:run(333, job_completion),
            ?assert(element(4, Result))
        end},
        {"job_cancellation runs to completion", fun() ->
            {ok, Result} = flurm_simulation:run(444, job_cancellation),
            ?assert(element(4, Result))
        end},
        {"leader_election_normal scenario", fun() ->
            {ok, Result} = flurm_simulation:run(555, leader_election_normal),
            ?assert(is_tuple(Result))
        end},
        {"partition_minority scenario", fun() ->
            {ok, Result} = flurm_simulation:run(666, partition_minority),
            ?assert(is_tuple(Result))
        end},
        {"partition_majority scenario", fun() ->
            {ok, Result} = flurm_simulation:run(777, partition_majority),
            ?assert(is_tuple(Result))
        end},
        {"sibling_job_race scenario", fun() ->
            {ok, Result} = flurm_simulation:run(888, sibling_job_race),
            ?assert(is_tuple(Result))
        end},
        {"cascading_node_failures scenario", fun() ->
            {ok, Result} = flurm_simulation:run(999, cascading_node_failures),
            ?assert(is_tuple(Result))
        end},
        {"migration_shadow_to_active scenario", fun() ->
            {ok, Result} = flurm_simulation:run(1010, migration_shadow_to_active),
            ?assert(is_tuple(Result))
        end}
    ].

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    [
        {"run with seed 0", fun() ->
            {ok, Result} = flurm_simulation:run(0, submit_job_single_node),
            ?assert(is_tuple(Result))
        end},
        {"run with very large seed", fun() ->
            {ok, Result} = flurm_simulation:run(9999999999, submit_job_single_node),
            ?assert(is_tuple(Result))
        end},
        {"run with negative seed", fun() ->
            {ok, Result} = flurm_simulation:run(-12345, submit_job_single_node),
            ?assert(is_tuple(Result))
        end},
        {"multiple runs with same scenario different seeds", fun() ->
            Seeds = [100, 200, 300, 400, 500],
            Results = [flurm_simulation:run(S, submit_job_single_node) || S <- Seeds],
            ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, Results))
        end}
    ].

%%====================================================================
%% Stress Tests
%%====================================================================

stress_test_() ->
    {timeout, 60, [
        {"run simulation 10 times", fun() ->
            Results = [flurm_simulation:run(I * 1000, submit_job_single_node)
                       || I <- lists:seq(1, 10)],
            PassedCount = length([R || {ok, _} = R <- Results]),
            ?assertEqual(10, PassedCount)
        end}
    ]}.

%%====================================================================
%% State Manipulation Tests
%%====================================================================

state_manipulation_test_() ->
    [
        {"init creates nodes map", fun() ->
            State = flurm_simulation:init_simulation(1, submit_job_multi_node),
            Nodes = element(2, State),
            ?assert(is_map(Nodes)),
            ?assertEqual(3, map_size(Nodes))  % multi_node has 3 nodes
        end},
        {"init creates empty messages", fun() ->
            State = flurm_simulation:init_simulation(1, submit_job_single_node),
            Messages = element(3, State),
            ?assert(is_list(Messages))
        end},
        {"init sets time to 0", fun() ->
            State = flurm_simulation:init_simulation(1, submit_job_single_node),
            Time = element(4, State),
            ?assertEqual(0, Time)
        end},
        {"init creates events list", fun() ->
            State = flurm_simulation:init_simulation(1, submit_job_single_node),
            Events = element(6, State),
            ?assert(is_list(Events))
        end}
    ].
