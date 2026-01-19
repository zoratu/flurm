%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_cloud_scaler internal functions
%%%
%%% These tests exercise the internal helper functions exported via
%%% -ifdef(TEST) to achieve coverage of pure logic functions.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaler_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Metrics Collection Tests
%%====================================================================

collect_scaling_metrics_test() ->
    %% This function tries to get metrics from job_registry and node_registry
    %% When those aren't running, it returns safe defaults
    Metrics = flurm_cloud_scaler:collect_scaling_metrics(),
    ?assert(is_map(Metrics)),
    ?assert(maps:is_key(pending_jobs, Metrics)),
    ?assert(maps:is_key(total_nodes, Metrics)),
    ?assert(maps:is_key(up_nodes, Metrics)),
    ?assert(maps:is_key(utilization, Metrics)),
    ?assert(maps:is_key(pending_demand, Metrics)).

collect_resource_utilization_test() ->
    %% When no nodes are registered, returns zeroes
    Result = flurm_cloud_scaler:collect_resource_utilization(),
    case Result of
        {TotalC, AllocC, TotalM, AllocM} ->
            ?assert(is_integer(TotalC)),
            ?assert(is_integer(AllocC)),
            ?assert(is_integer(TotalM)),
            ?assert(is_integer(AllocM));
        _ ->
            %% If nodes aren't running, this is expected
            ok
    end.

estimate_pending_demand_test() ->
    %% When job_registry isn't running, returns 0
    Result = flurm_cloud_scaler:estimate_pending_demand(),
    ?assert(is_integer(Result)),
    ?assert(Result >= 0).

%%====================================================================
%% Scaling Decision Tests
%%====================================================================

determine_scaling_action_no_action_test() ->
    %% Low utilization, no pending jobs, at min nodes
    Metrics = #{
        pending_jobs => 0,
        total_nodes => 5,
        up_nodes => 5,
        utilization => 0.5,
        pending_demand => 0
    },
    State = flurm_cloud_scaler:test_state([{min_nodes, 5}, {max_nodes, 100}]),
    %% Utilization is 50% - between thresholds, at min nodes
    %% Should be none since we can't scale down further
    Result = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    ?assertEqual(none, Result).

determine_scaling_action_scale_up_high_utilization_test() ->
    %% High utilization triggers scale up
    Metrics = #{
        pending_jobs => 5,
        total_nodes => 10,
        up_nodes => 10,
        utilization => 0.85,  % Above 0.8 threshold
        pending_demand => 2
    },
    State = flurm_cloud_scaler:test_state([{min_nodes, 0}, {max_nodes, 100}]),
    Result = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    ?assertMatch({scale_up, _}, Result).

determine_scaling_action_scale_up_many_pending_test() ->
    %% Many pending jobs triggers scale up
    Metrics = #{
        pending_jobs => 15,  % Above 10 threshold
        total_nodes => 10,
        up_nodes => 10,
        utilization => 0.5,
        pending_demand => 5
    },
    State = flurm_cloud_scaler:test_state([{min_nodes, 0}, {max_nodes, 100}]),
    Result = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    ?assertMatch({scale_up, _}, Result).

determine_scaling_action_scale_up_below_min_test() ->
    %% Below min nodes triggers scale up
    Metrics = #{
        pending_jobs => 0,
        total_nodes => 3,
        up_nodes => 3,
        utilization => 0.5,
        pending_demand => 0
    },
    State = flurm_cloud_scaler:test_state([{min_nodes, 5}, {max_nodes, 100}]),
    Result = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    ?assertMatch({scale_up, _}, Result).

determine_scaling_action_scale_down_test() ->
    %% Low utilization, no pending jobs, above min
    Metrics = #{
        pending_jobs => 0,
        total_nodes => 20,
        up_nodes => 20,
        utilization => 0.15,  % Below 0.3 threshold
        pending_demand => 0
    },
    State = flurm_cloud_scaler:test_state([{min_nodes, 5}, {max_nodes, 100}]),
    Result = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    ?assertMatch({scale_down, _}, Result).

determine_scaling_action_at_max_capacity_test() ->
    %% At max capacity, cannot scale up
    Metrics = #{
        pending_jobs => 15,
        total_nodes => 100,
        up_nodes => 100,
        utilization => 0.9,
        pending_demand => 10
    },
    State = flurm_cloud_scaler:test_state([{min_nodes, 0}, {max_nodes, 100}]),
    Result = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    %% Can't scale up since at max
    ?assertEqual(none, Result).

%%====================================================================
%% Scale Count Calculation Tests
%%====================================================================

calculate_scale_up_count_basic_test() ->
    Metrics = #{
        pending_demand => 5,
        total_nodes => 10
    },
    State = flurm_cloud_scaler:test_state([{max_nodes, 100}]),
    Result = flurm_cloud_scaler:calculate_scale_up_count(Metrics, State),
    ?assertEqual(5, Result).

calculate_scale_up_count_zero_demand_test() ->
    Metrics = #{
        pending_demand => 0,
        total_nodes => 10
    },
    State = flurm_cloud_scaler:test_state([{max_nodes, 100}]),
    Result = flurm_cloud_scaler:calculate_scale_up_count(Metrics, State),
    %% Should return at least 1
    ?assertEqual(1, Result).

calculate_scale_up_count_respects_max_test() ->
    Metrics = #{
        pending_demand => 50,
        total_nodes => 90
    },
    State = flurm_cloud_scaler:test_state([{max_nodes, 100}]),
    Result = flurm_cloud_scaler:calculate_scale_up_count(Metrics, State),
    %% Should not exceed max_nodes - total_nodes = 10
    ?assertEqual(10, Result).

calculate_scale_down_count_basic_test() ->
    Metrics = #{
        total_nodes => 20,
        utilization => 0.1  % Very low utilization
    },
    State = flurm_cloud_scaler:test_state([{min_nodes, 5}]),
    Result = flurm_cloud_scaler:calculate_scale_down_count(Metrics, State),
    ?assert(Result >= 1),
    %% Should not go below min_nodes
    ?assert(Result =< 15).

calculate_scale_down_count_respects_min_test() ->
    Metrics = #{
        total_nodes => 6,
        utilization => 0.1
    },
    State = flurm_cloud_scaler:test_state([{min_nodes, 5}]),
    Result = flurm_cloud_scaler:calculate_scale_down_count(Metrics, State),
    %% Can only remove 1 node (6 - 5 = 1)
    ?assertEqual(1, Result).

calculate_scale_down_count_moderate_utilization_test() ->
    Metrics = #{
        total_nodes => 20,
        utilization => 0.2  % Slightly below threshold
    },
    State = flurm_cloud_scaler:test_state([{min_nodes, 5}]),
    Result = flurm_cloud_scaler:calculate_scale_down_count(Metrics, State),
    ?assert(Result >= 1).

%%====================================================================
%% Find Idle Nodes Tests
%%====================================================================

find_idle_nodes_no_registry_test() ->
    %% When node registry isn't running, returns empty list
    Result = flurm_cloud_scaler:find_idle_nodes(5),
    ?assertEqual([], Result).

find_idle_nodes_zero_count_test() ->
    Result = flurm_cloud_scaler:find_idle_nodes(0),
    ?assertEqual([], Result).

%%====================================================================
%% Test State Helper Tests
%%====================================================================

test_state_defaults_test() ->
    State = flurm_cloud_scaler:test_state([]),
    %% Verify default values
    ?assertEqual(false, element(2, State)),  % enabled
    ?assertEqual(undefined, element(3, State)),  % provider
    ?assertEqual(#{}, element(4, State)),  % config
    ?assertEqual(0, element(8, State)),  % min_nodes
    ?assertEqual(100, element(9, State)).  % max_nodes

test_state_custom_values_test() ->
    State = flurm_cloud_scaler:test_state([
        {enabled, true},
        {provider, aws},
        {min_nodes, 10},
        {max_nodes, 50}
    ]),
    ?assertEqual(true, element(2, State)),
    ?assertEqual(aws, element(3, State)),
    ?assertEqual(10, element(8, State)),
    ?assertEqual(50, element(9, State)).
