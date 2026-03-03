%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_cloud_scaler internal functions
%%%
%%% These tests exercise the internal helper functions exported via
%%% -ifdef(TEST) to achieve coverage of pure logic functions.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaler_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

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

%%====================================================================
%% Tail Coverage Tests
%%====================================================================

code_change_passthrough_test() ->
    State = flurm_cloud_scaler:test_state([{enabled, true}, {provider, aws}]),
    ?assertEqual({ok, State}, flurm_cloud_scaler:code_change(old, State, [])).

collect_resource_utilization_non_list_fallback_test() ->
    with_meck([flurm_node_registry], fun() ->
        meck:expect(flurm_node_registry, list_nodes, fun() -> bad_nodes end),
        ?assertEqual({0, 0, 0, 0}, flurm_cloud_scaler:collect_resource_utilization())
    end).

estimate_pending_demand_non_list_fallback_test() ->
    with_meck([flurm_job_registry], fun() ->
        meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> bad_jobs end),
        ?assertEqual(0, flurm_cloud_scaler:estimate_pending_demand())
    end).

find_idle_nodes_non_list_fallback_test() ->
    with_meck([flurm_node_registry], fun() ->
        meck:expect(flurm_node_registry, list_nodes, fun() -> bad_nodes end),
        ?assertEqual([], flurm_cloud_scaler:find_idle_nodes(3))
    end).

provider_missing_config_tail_branches_test() ->
    StateAws = flurm_cloud_scaler:test_state([{provider, aws}, {config, #{}}]),
    {reply, {error, missing_asg_name}, _} =
        flurm_cloud_scaler:handle_call({scale_down, 1}, {self(), make_ref()}, StateAws),

    StateGcp = flurm_cloud_scaler:test_state([{provider, gcp}, {config, #{}}]),
    {reply, {error, missing_mig_name}, _} =
        flurm_cloud_scaler:handle_call({scale_down, 1}, {self(), make_ref()}, StateGcp),

    StateAzure = flurm_cloud_scaler:test_state(
        [{provider, azure}, {config, #{vmss_name => <<"vmss">>}}]),
    {reply, {error, missing_resource_group}, _} =
        flurm_cloud_scaler:handle_call({scale_up, 1}, {self(), make_ref()}, StateAzure).

handle_info_scale_up_cooldown_expired_error_branch_test() ->
    with_scale_up_metrics(fun() ->
        Now = erlang:monotonic_time(millisecond),
        State = flurm_cloud_scaler:test_state([
            {enabled, true},
            {provider, undefined},
            {last_scale_up, Now - 120001}
        ]),
        {noreply, NewState} = flurm_cloud_scaler:handle_info(evaluate_scaling, State),
        cancel_eval_timer(NewState)
    end).

handle_info_scale_up_cooldown_active_branch_test() ->
    with_scale_up_metrics(fun() ->
        Now = erlang:monotonic_time(millisecond),
        State = flurm_cloud_scaler:test_state([
            {enabled, true},
            {provider, undefined},
            {last_scale_up, Now}
        ]),
        {noreply, NewState} = flurm_cloud_scaler:handle_info(evaluate_scaling, State),
        cancel_eval_timer(NewState)
    end).

handle_info_scale_down_cooldown_expired_error_branch_test() ->
    with_scale_down_metrics(fun() ->
        Now = erlang:monotonic_time(millisecond),
        State = flurm_cloud_scaler:test_state([
            {enabled, true},
            {provider, undefined},
            {last_scale_down, Now - 300001},
            {min_nodes, 0},
            {max_nodes, 100}
        ]),
        {noreply, NewState} = flurm_cloud_scaler:handle_info(evaluate_scaling, State),
        cancel_eval_timer(NewState)
    end).

handle_info_scale_down_cooldown_active_branch_test() ->
    with_scale_down_metrics(fun() ->
        Now = erlang:monotonic_time(millisecond),
        State = flurm_cloud_scaler:test_state([
            {enabled, true},
            {provider, undefined},
            {last_scale_down, Now},
            {min_nodes, 0},
            {max_nodes, 100}
        ]),
        {noreply, NewState} = flurm_cloud_scaler:handle_info(evaluate_scaling, State),
        cancel_eval_timer(NewState)
    end).

with_scale_up_metrics(Fun) ->
    with_meck([flurm_job_registry, flurm_node_registry], fun() ->
        meck:expect(flurm_job_registry, count_by_state, fun() -> #{pending => 15} end),
        meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> [] end),
        meck:expect(flurm_node_registry, count_by_state, fun() ->
            #{up => 1, down => 0, drain => 0}
        end),
        meck:expect(flurm_node_registry, list_nodes, fun() -> [] end),
        Fun()
    end).

with_scale_down_metrics(Fun) ->
    with_meck([flurm_job_registry, flurm_node_registry], fun() ->
        meck:expect(flurm_job_registry, count_by_state, fun() -> #{pending => 0} end),
        meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> [] end),
        meck:expect(flurm_node_registry, count_by_state, fun() ->
            #{up => 6, down => 0, drain => 0}
        end),
        meck:expect(flurm_node_registry, list_nodes, fun() -> [{<<"n1">>, self()}] end),
        meck:expect(flurm_node_registry, get_node_entry, fun(<<"n1">>) ->
            {ok, #node_entry{
                name = <<"n1">>,
                pid = self(),
                hostname = <<"host1">>,
                state = up,
                partitions = [],
                cpus_total = 10,
                cpus_avail = 10,
                memory_total = 1000,
                memory_avail = 1000,
                gpus_total = 0,
                gpus_avail = 0
            }}
        end),
        Fun()
    end).

cancel_eval_timer(State) ->
    case element(5, State) of
        Timer when is_reference(Timer) ->
            erlang:cancel_timer(Timer),
            ok;
        _ ->
            ok
    end.

with_meck(Modules, Fun) ->
    lists:foreach(fun(M) ->
        catch meck:unload(M),
        meck:new(M, [non_strict])
    end, Modules),
    try
        Fun()
    after
        lists:foreach(fun(M) -> catch meck:unload(M) end, Modules)
    end.
