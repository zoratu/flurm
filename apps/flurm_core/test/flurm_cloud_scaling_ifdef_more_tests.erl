%%%-------------------------------------------------------------------
%%% @doc Extra TEST-export helper coverage for flurm_cloud_scaling.
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaling_ifdef_more_tests).

-include_lib("eunit/include/eunit.hrl").

-record(scaling_policy, {
    min_nodes,
    max_nodes,
    target_pending_jobs,
    target_idle_nodes,
    scale_up_increment,
    scale_down_increment,
    cooldown_seconds,
    instance_types,
    spot_enabled,
    spot_max_price,
    idle_threshold_seconds,
    queue_depth_threshold
}).

-record(scaling_action, {
    id,
    type,
    requested_count,
    actual_count,
    start_time,
    end_time,
    status,
    error,
    instance_ids
}).

-record(cost_tracking, {
    total_instance_hours,
    total_cost,
    budget_limit,
    budget_alert_threshold,
    cost_per_hour,
    daily_costs,
    monthly_costs
}).

-record(cloud_instance, {
    instance_id,
    provider,
    instance_type,
    launch_time,
    state,
    public_ip,
    private_ip,
    node_name,
    last_job_time,
    hourly_cost
}).

-record(state, {
    enabled,
    provider,
    provider_config,
    policy,
    current_cloud_nodes,
    pending_actions,
    action_history,
    last_scale_time,
    check_timer,
    idle_check_timer,
    cloud_instances,
    cost_tracking
}).

update_and_format_policy_test() ->
    P0 = #scaling_policy{
        min_nodes = 1,
        max_nodes = 10,
        target_pending_jobs = 5,
        target_idle_nodes = 1,
        scale_up_increment = 2,
        scale_down_increment = 1,
        cooldown_seconds = 120,
        instance_types = [<<"c5.large">>],
        spot_enabled = false,
        spot_max_price = 0.0,
        idle_threshold_seconds = 300,
        queue_depth_threshold = 6
    },
    P1 = flurm_cloud_scaling:update_policy(P0, #{
        min_nodes => 2,
        max_nodes => 20,
        spot_enabled => true,
        spot_max_price => 0.25
    }),
    Map = flurm_cloud_scaling:format_policy(P1),
    ?assertEqual(2, maps:get(min_nodes, Map)),
    ?assertEqual(20, maps:get(max_nodes, Map)),
    ?assertEqual(true, maps:get(spot_enabled, Map)),
    ?assertEqual(0.25, maps:get(spot_max_price, Map)).

collect_stats_counts_test() ->
    CompletedUp = #scaling_action{
        id = <<"a1">>, type = scale_up, requested_count = 2, actual_count = 2,
        start_time = 1, end_time = 2, status = completed, error = undefined,
        instance_ids = [<<"i-1">>, <<"i-2">>]
    },
    CompletedDown = #scaling_action{
        id = <<"a2">>, type = scale_down, requested_count = 1, actual_count = 1,
        start_time = 3, end_time = 4, status = completed, error = undefined,
        instance_ids = []
    },
    Failed = #scaling_action{
        id = <<"a3">>, type = scale_up, requested_count = 1, actual_count = 0,
        start_time = 5, end_time = 6, status = failed, error = <<"boom">>,
        instance_ids = []
    },
    St = #state{
        enabled = true,
        provider = gcp,
        provider_config = #{},
        policy = undefined,
        current_cloud_nodes = 3,
        pending_actions = [#scaling_action{id = <<"p1">>}],
        action_history = [CompletedUp, CompletedDown, Failed],
        last_scale_time = 0,
        check_timer = undefined,
        idle_check_timer = undefined,
        cloud_instances = #{},
        cost_tracking = undefined
    },
    Stats = flurm_cloud_scaling:collect_stats(St),
    ?assertEqual(true, maps:get(enabled, Stats)),
    ?assertEqual(gcp, maps:get(provider, Stats)),
    ?assertEqual(3, maps:get(current_cloud_nodes, Stats)),
    ?assertEqual(1, maps:get(pending_actions, Stats)),
    ?assertEqual(1, maps:get(completed_scale_ups, Stats)),
    ?assertEqual(1, maps:get(completed_scale_downs, Stats)),
    ?assertEqual(1, maps:get(failed_actions, Stats)),
    ?assertEqual(2, maps:get(total_instances_launched, Stats)),
    ?assertEqual(1, maps:get(total_instances_terminated, Stats)).

cost_and_budget_helpers_test() ->
    CT = #cost_tracking{
        total_instance_hours = 0.0,
        total_cost = 400.0,
        budget_limit = 1000.0,
        budget_alert_threshold = 0.8,
        cost_per_hour = #{
            <<"c5.large">> => 0.085,
            <<"n1-standard-4">> => 0.19
        },
        daily_costs = #{},
        monthly_costs = #{}
    },
    ?assertEqual(0.085, flurm_cloud_scaling:get_instance_cost(<<"c5.large">>, CT)),
    %% Unknown type falls back to a default > 0.
    ?assert(flurm_cloud_scaling:get_instance_cost(<<"unknown-type">>, CT) > 0.0),
    Budget = flurm_cloud_scaling:calculate_budget_status(CT),
    ?assert(is_map(Budget)),
    ?assertEqual(1000.0, maps:get(budget_limit, Budget)),
    ?assertEqual(400.0, maps:get(total_cost, Budget)),
    ?assertEqual(600.0, maps:get(remaining, Budget)).

format_instance_helpers_test() ->
    I1 = #cloud_instance{
        instance_id = <<"i-123">>,
        provider = aws,
        instance_type = <<"c5.large">>,
        launch_time = 1700000000,
        state = running,
        public_ip = <<"1.2.3.4">>,
        private_ip = <<"10.0.0.10">>,
        node_name = <<"node-a">>,
        last_job_time = 1700000001,
        hourly_cost = 0.085
    },
    M1 = flurm_cloud_scaling:format_instance(I1),
    ?assertEqual(<<"i-123">>, maps:get(instance_id, M1)),
    ?assertEqual(aws, maps:get(provider, M1)),
    ?assertEqual(running, maps:get(state, M1)),
    List = flurm_cloud_scaling:format_cloud_instances(#{
        <<"i-123">> => I1
    }),
    ?assertEqual(1, length(List)).
