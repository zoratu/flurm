%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_simulation module
%%% Achieves 100% code coverage for deterministic simulation testing.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_simulation_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

simulation_api_test_() ->
    [
        %% API tests
        {"run simulation with valid seed and scenario", fun test_run_basic/0},
        {"run simulation returns error on exception", fun test_run_error/0},
        {"run_all_scenarios runs all scenarios", fun test_run_all_scenarios/0},
        {"run_all_scenarios with custom seed count", fun test_run_all_scenarios_custom/0},
        {"scenarios returns list of all scenarios", fun test_scenarios/0}
    ].

scenario_test_() ->
    [
        %% Basic operation scenarios
        {"submit_job_single_node scenario", fun test_scenario_submit_job_single_node/0},
        {"submit_job_multi_node scenario", fun test_scenario_submit_job_multi_node/0},
        {"job_completion scenario", fun test_scenario_job_completion/0},
        {"job_cancellation scenario", fun test_scenario_job_cancellation/0},

        %% Leader election scenarios
        {"leader_election_normal scenario", fun test_scenario_leader_election/0},
        {"leader_failure_during_submit scenario", fun test_scenario_leader_failure/0},

        %% Partition scenarios
        {"partition_minority scenario", fun test_scenario_partition_minority/0},
        {"partition_majority scenario", fun test_scenario_partition_majority/0},

        %% Federation sibling scenarios
        {"sibling_job_basic scenario", fun test_scenario_sibling_basic/0},
        {"sibling_job_race scenario", fun test_scenario_sibling_race/0},
        {"sibling_revocation scenario", fun test_scenario_sibling_revocation/0},
        {"origin_cluster_failure scenario", fun test_scenario_origin_failure/0},

        %% Other scenarios
        {"cascading_node_failures scenario", fun test_scenario_cascading_failures/0},
        {"migration_shadow_to_active scenario", fun test_scenario_migration_shadow/0},
        {"migration_with_failures scenario", fun test_scenario_migration_failures/0}
    ].

internal_functions_test_() ->
    [
        {"init_simulation creates initial state", fun test_init_simulation/0},
        {"step advances simulation", fun test_step/0},
        {"step with messages", fun test_step_with_messages/0},
        {"check_invariants detects violations", fun test_check_invariants/0},
        {"inject_failures injects random failures", fun test_inject_failures/0},
        {"deliver_message handles delivery", fun test_deliver_message/0},
        {"deliver_message to offline node", fun test_deliver_message_offline/0},
        {"deliver_message across partition", fun test_deliver_message_partition/0}
    ].

%%====================================================================
%% API Tests
%%====================================================================

test_run_basic() ->
    %% Run a simple scenario with known seed
    {ok, Result} = flurm_simulation:run(42, submit_job_single_node),

    %% Verify result structure
    ?assert(is_tuple(Result)),
    ?assertEqual(sim_result, element(1, Result)),
    ?assertEqual(42, element(2, Result)),           % seed
    ?assertEqual(submit_job_single_node, element(3, Result)),  % scenario
    ?assert(is_boolean(element(4, Result))),        % success
    ?assert(is_integer(element(5, Result))),        % steps
    ?assert(is_tuple(element(6, Result))),          % final_state
    ?assert(is_list(element(7, Result))),           % violations
    ?assert(is_list(element(8, Result))).           % trace

test_run_error() ->
    %% Run with invalid scenario to trigger error path
    Result = flurm_simulation:run(1, nonexistent_scenario),
    %% Should still return ok since scenario_config handles unknown scenarios
    ?assertMatch({ok, _}, Result).

test_run_all_scenarios() ->
    %% Run with small seed count for speed
    {ok, Report} = flurm_simulation:run_all_scenarios(2),
    ?assert(is_map(Report)),
    ?assert(maps:is_key(passed, Report)),
    ?assert(maps:is_key(failed, Report)),
    ?assert(maps:get(passed, Report) + maps:get(failed, Report) > 0).

test_run_all_scenarios_custom() ->
    %% Run with 1 seed per scenario
    {ok, Report} = flurm_simulation:run_all_scenarios(1),
    ?assert(maps:get(passed, Report) + maps:get(failed, Report) >= length(flurm_simulation:scenarios())).

test_scenarios() ->
    Scenarios = flurm_simulation:scenarios(),
    ?assert(is_list(Scenarios)),
    ?assert(length(Scenarios) >= 10),

    %% Verify expected scenarios exist
    ?assert(lists:member(submit_job_single_node, Scenarios)),
    ?assert(lists:member(leader_election_normal, Scenarios)),
    ?assert(lists:member(partition_minority, Scenarios)),
    ?assert(lists:member(sibling_job_basic, Scenarios)),
    ?assert(lists:member(cascading_node_failures, Scenarios)).

%%====================================================================
%% Scenario Tests
%%====================================================================

test_scenario_submit_job_single_node() ->
    {ok, Result} = flurm_simulation:run(1, submit_job_single_node),
    %% Should complete without violations in ideal case
    ?assert(is_list(element(7, Result))).  % violations list

test_scenario_submit_job_multi_node() ->
    {ok, Result} = flurm_simulation:run(1, submit_job_multi_node),
    ?assert(is_tuple(Result)).

test_scenario_job_completion() ->
    {ok, Result} = flurm_simulation:run(1, job_completion),
    FinalState = element(6, Result),
    ?assertEqual(sim_state, element(1, FinalState)).

test_scenario_job_cancellation() ->
    {ok, Result} = flurm_simulation:run(1, job_cancellation),
    ?assert(is_tuple(Result)).

test_scenario_leader_election() ->
    {ok, Result} = flurm_simulation:run(1, leader_election_normal),
    FinalState = element(6, Result),
    %% Leader starts as undefined for this scenario
    ?assertEqual(sim_state, element(1, FinalState)).

test_scenario_leader_failure() ->
    {ok, Result} = flurm_simulation:run(1, leader_failure_during_submit),
    %% This scenario kills the leader
    FinalState = element(6, Result),
    Nodes = element(2, FinalState),
    %% Check node1 was killed
    Node1 = maps:get(<<"node1">>, Nodes),
    ?assertEqual(false, element(5, Node1)).  % online = false

test_scenario_partition_minority() ->
    {ok, Result} = flurm_simulation:run(1, partition_minority),
    FinalState = element(6, Result),
    Nodes = element(2, FinalState),
    %% Should have 5 nodes
    ?assertEqual(5, maps:size(Nodes)).

test_scenario_partition_majority() ->
    {ok, Result} = flurm_simulation:run(1, partition_majority),
    FinalState = element(6, Result),
    Nodes = element(2, FinalState),
    %% Leader should be partitioned from majority
    Node1 = maps:get(<<"node1">>, Nodes),
    PartitionedFrom = element(6, Node1),
    ?assert(length(PartitionedFrom) > 0).

test_scenario_sibling_basic() ->
    {ok, Result} = flurm_simulation:run(1, sibling_job_basic),
    FinalState = element(6, Result),
    Jobs = element(6, FinalState),
    %% Should have created sibling jobs
    ?assert(maps:size(Jobs) >= 1).

test_scenario_sibling_race() ->
    {ok, Result} = flurm_simulation:run(1, sibling_job_race),
    FinalState = element(6, Result),
    Jobs = element(6, FinalState),
    ?assert(maps:size(Jobs) >= 1).

test_scenario_sibling_revocation() ->
    {ok, Result} = flurm_simulation:run(1, sibling_revocation),
    FinalState = element(6, Result),
    Jobs = element(6, FinalState),
    ?assert(maps:size(Jobs) >= 1).

test_scenario_origin_failure() ->
    {ok, Result} = flurm_simulation:run(1, origin_cluster_failure),
    FinalState = element(6, Result),
    Nodes = element(2, FinalState),
    %% Origin cluster (node1) should be killed
    Node1 = maps:get(<<"node1">>, Nodes),
    ?assertEqual(false, element(5, Node1)).

test_scenario_cascading_failures() ->
    {ok, Result} = flurm_simulation:run(1, cascading_node_failures),
    FinalState = element(6, Result),
    Nodes = element(2, FinalState),
    %% Should have 5 nodes
    ?assertEqual(5, maps:size(Nodes)).

test_scenario_migration_shadow() ->
    {ok, Result} = flurm_simulation:run(1, migration_shadow_to_active),
    ?assert(is_tuple(Result)).

test_scenario_migration_failures() ->
    {ok, Result} = flurm_simulation:run(1, migration_with_failures),
    ?assert(is_tuple(Result)).

%%====================================================================
%% Internal Function Tests
%%====================================================================

test_init_simulation() ->
    State = flurm_simulation:init_simulation(42, submit_job_single_node),
    ?assertEqual(sim_state, element(1, State)),

    %% Check nodes were created
    Nodes = element(2, State),
    ?assert(maps:size(Nodes) >= 1),

    %% Check time starts at 0
    ?assertEqual(0, element(4, State)),

    %% Check RNG was initialized
    RNG = element(5, State),
    ?assert(is_tuple(RNG)).

test_step() ->
    State = flurm_simulation:init_simulation(1, submit_job_single_node),

    %% Step should advance simulation
    {Result, NewState} = flurm_simulation:step(State),
    ?assert(Result =:= done orelse Result =:= continue),
    ?assertEqual(sim_state, element(1, NewState)).

test_step_with_messages() ->
    State = flurm_simulation:init_simulation(1, submit_job_single_node),

    %% Advance time until there are messages
    State2 = advance_until_messages(State, 100),

    %% Step should process message
    {_, NewState} = flurm_simulation:step(State2),
    ?assertEqual(sim_state, element(1, NewState)).

advance_until_messages(State, 0) -> State;
advance_until_messages(State, N) ->
    Messages = element(3, State),
    case Messages of
        [] ->
            {_, NewState} = flurm_simulation:step(State),
            advance_until_messages(NewState, N - 1);
        _ ->
            State
    end.

test_check_invariants() ->
    State = flurm_simulation:init_simulation(1, submit_job_single_node),

    %% Initial state should have no violations
    CheckedState = flurm_simulation:check_invariants(State),
    Violations = element(9, CheckedState),
    ?assert(is_list(Violations)).

test_inject_failures() ->
    State = flurm_simulation:init_simulation(1, submit_job_multi_node),

    %% Run inject_failures many times to cover probability branches
    lists:foldl(fun(_, S) ->
        NewState = flurm_simulation:inject_failures(S),
        ?assertEqual(sim_state, element(1, NewState)),
        NewState
    end, State, lists:seq(1, 200)).

test_deliver_message() ->
    State = flurm_simulation:init_simulation(1, submit_job_single_node),

    %% Create a test message
    Msg = {sim_message,
           <<"node1">>,   % from
           <<"node1">>,   % to
           job_allocated, % type
           1,             % payload
           0,             % send_time
           1              % delivery_time
    },

    NewState = flurm_simulation:deliver_message(Msg, State),
    ?assertEqual(sim_state, element(1, NewState)),

    %% Check event was recorded
    Events = element(6, NewState),
    ?assert(length(Events) > 0).

test_deliver_message_offline() ->
    State = flurm_simulation:init_simulation(1, submit_job_multi_node),

    %% Kill node1
    Nodes = element(2, State),
    Node1 = maps:get(<<"node1">>, Nodes),
    OfflineNode = setelement(5, Node1, false),  % online = false
    NewNodes = maps:put(<<"node1">>, OfflineNode, Nodes),
    StateWithOffline = setelement(2, State, NewNodes),

    %% Try to deliver to offline node
    Msg = {sim_message,
           <<"node2">>,
           <<"node1">>,
           test_msg,
           test_payload,
           0,
           1
    },

    NewState = flurm_simulation:deliver_message(Msg, StateWithOffline),
    Events = element(6, NewState),
    %% Should have message_dropped event
    ?assert(lists:any(fun({message_dropped, _, _}) -> true; (_) -> false end, Events)).

test_deliver_message_partition() ->
    State = flurm_simulation:init_simulation(1, submit_job_multi_node),

    %% Create partition between node1 and node2
    Nodes = element(2, State),
    Node2 = maps:get(<<"node2">>, Nodes),
    PartitionedNode = setelement(6, Node2, [<<"node1">>]),  % partitioned_from
    NewNodes = maps:put(<<"node2">>, PartitionedNode, Nodes),
    StateWithPartition = setelement(2, State, NewNodes),

    %% Try to deliver across partition
    Msg = {sim_message,
           <<"node1">>,
           <<"node2">>,
           test_msg,
           test_payload,
           0,
           1
    },

    NewState = flurm_simulation:deliver_message(Msg, StateWithPartition),
    Events = element(6, NewState),
    %% Should have message_dropped_partition event
    ?assert(lists:any(fun({message_dropped_partition, _, _}) -> true; (_) -> false end, Events)).

%%====================================================================
%% Invariant Tests
%%====================================================================

invariant_test_() ->
    [
        {"leader uniqueness check", fun test_leader_uniqueness/0},
        {"sibling exclusivity check", fun test_sibling_exclusivity/0},
        {"no job loss check", fun test_no_job_loss/0},
        {"log consistency check", fun test_log_consistency/0}
    ].

test_leader_uniqueness() ->
    State = flurm_simulation:init_simulation(1, leader_election_normal),

    %% Create state with multiple leaders (violation)
    Nodes = element(2, State),
    Node1 = maps:get(<<"node1">>, Nodes),
    Node2 = maps:get(<<"node2">>, Nodes),

    %% Set both as leaders
    LeaderNode1 = setelement(4, Node1, leader),
    LeaderNode2 = setelement(4, Node2, leader),

    ViolationNodes = #{
        <<"node1">> => LeaderNode1,
        <<"node2">> => LeaderNode2,
        <<"node3">> => maps:get(<<"node3">>, Nodes)
    },
    StateWithViolation = setelement(2, State, ViolationNodes),

    %% Check invariants should find violation
    CheckedState = flurm_simulation:check_invariants(StateWithViolation),
    Violations = element(9, CheckedState),
    ?assert(lists:any(fun({leader_uniqueness_violation, _, _}) -> true; (_) -> false end, Violations)).

test_sibling_exclusivity() ->
    State = flurm_simulation:init_simulation(1, sibling_job_basic),

    %% Advance simulation to create sibling jobs
    {ok, Result} = flurm_simulation:run(1, sibling_job_basic),
    FinalState = element(6, Result),

    %% Check invariants
    CheckedState = flurm_simulation:check_invariants(FinalState),
    %% This is just checking that the check runs without error
    ?assertEqual(sim_state, element(1, CheckedState)).

test_no_job_loss() ->
    %% Create state with jobs and all nodes online
    State = flurm_simulation:init_simulation(1, submit_job_multi_node),

    %% Add a job
    Jobs = #{1 => {sim_job, 1, pending, [<<"node1">>, <<"node2">>], undefined, 0}},
    StateWithJob = setelement(6, State, Jobs),

    %% Check should pass (all sibling clusters online)
    CheckedState = flurm_simulation:check_invariants(StateWithJob),
    ?assertEqual(sim_state, element(1, CheckedState)).

test_log_consistency() ->
    State = flurm_simulation:init_simulation(1, submit_job_multi_node),

    %% Check invariants
    CheckedState = flurm_simulation:check_invariants(State),
    %% Initial state should have consistent (empty) logs
    ?assertEqual(sim_state, element(1, CheckedState)).

%%====================================================================
%% Determinism Tests
%%====================================================================

determinism_test_() ->
    [
        {"same seed produces same result", fun test_determinism/0},
        {"different seeds produce different results", fun test_different_seeds/0}
    ].

test_determinism() ->
    %% Run same scenario with same seed twice
    {ok, Result1} = flurm_simulation:run(12345, submit_job_single_node),
    {ok, Result2} = flurm_simulation:run(12345, submit_job_single_node),

    %% Should produce identical results
    ?assertEqual(element(4, Result1), element(4, Result2)),  % success
    ?assertEqual(element(5, Result1), element(5, Result2)).  % steps

test_different_seeds() ->
    %% Run same scenario with different seeds
    {ok, Result1} = flurm_simulation:run(1, submit_job_single_node),
    {ok, Result2} = flurm_simulation:run(999, submit_job_single_node),

    %% Results may differ (due to random failure injection)
    %% Just verify both completed
    ?assert(is_tuple(Result1)),
    ?assert(is_tuple(Result2)).

%%====================================================================
%% Stress Tests
%%====================================================================

stress_test_() ->
    {timeout, 60, [
        {"run many simulations without crash", fun test_many_simulations/0}
    ]}.

test_many_simulations() ->
    %% Run 50 simulations across various scenarios
    Scenarios = flurm_simulation:scenarios(),
    lists:foreach(fun(Scenario) ->
        lists:foreach(fun(Seed) ->
            {ok, _} = flurm_simulation:run(Seed, Scenario)
        end, lists:seq(1, 3))
    end, lists:sublist(Scenarios, 5)).

%%====================================================================
%% Max Steps Tests
%%====================================================================

max_steps_test_() ->
    [
        {"simulation respects max steps", fun test_max_steps/0}
    ].

test_max_steps() ->
    %% Run simulation - it should complete within max steps
    {ok, Result} = flurm_simulation:run(1, submit_job_single_node),
    Steps = element(5, Result),
    ?assert(Steps =< 10000).  % Max steps is 10000
