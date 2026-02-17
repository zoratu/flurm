%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_cloud_scaler module
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaler_tests).
-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start meck for dependencies
    meck:new(flurm_job_registry, [non_strict]),
    meck:new(flurm_node_registry, [non_strict]),
    meck:new(flurm_job, [non_strict]),

    %% Default mocks
    meck:expect(flurm_job_registry, count_by_state, fun() -> #{pending => 0, running => 0} end),
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> [] end),
    meck:expect(flurm_node_registry, count_by_state, fun() -> #{up => 0, down => 0, drain => 0} end),
    meck:expect(flurm_node_registry, list_nodes, fun() -> [] end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) -> {error, not_found} end),

    %% Start the cloud scaler server
    case whereis(flurm_cloud_scaler) of
        undefined ->
            {ok, Pid} = flurm_cloud_scaler:start_link(),
            %% Unlink to prevent test process crash on shutdown
            unlink(Pid),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, Pid}) ->
    catch ets:delete(flurm_cloud_config),
    case is_process_alive(Pid) of
        true ->
            Ref = monitor(process, Pid),
            catch gen_server:stop(flurm_cloud_scaler, shutdown, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush])
            end;
        false ->
            ok
    end,
    meck:unload();
cleanup({existing, _Pid}) ->
    meck:unload().

cloud_scaler_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"initial state", fun test_initial_state/0},
      {"set provider", fun test_set_provider/0},
      {"enable without provider fails", fun test_enable_without_provider/0},
      {"enable with provider", fun test_enable_with_provider/0},
      {"set and get config", fun test_set_get_config/0},
      {"set and get limits", fun test_set_get_limits/0},
      {"get scaling status", fun test_get_scaling_status/0},
      {"aws scale up", fun test_aws_scale_up/0},
      {"gcp scale up", fun test_gcp_scale_up/0},
      {"azure scale up", fun test_azure_scale_up/0},
      {"aws scale down", fun test_aws_scale_down/0},
      {"gcp scale down", fun test_gcp_scale_down/0},
      {"azure scale down", fun test_azure_scale_down/0},
      {"scale down without provider", fun test_scale_down_no_provider/0},
      {"scale up without provider", fun test_scale_up_no_provider/0},
      {"aws scale up missing config", fun test_aws_scale_up_missing_config/0},
      {"gcp scale up missing config", fun test_gcp_scale_up_missing_config/0},
      {"azure scale up missing config", fun test_azure_scale_up_missing_config/0},
      {"azure scale down missing vmss", fun test_azure_scale_down_missing_vmss/0},
      {"azure scale down missing rg", fun test_azure_scale_down_missing_rg/0},
      {"force evaluation disabled", fun test_force_evaluation_disabled/0},
      {"force evaluation enabled", fun test_force_evaluation_enabled/0},
      {"config merge", fun test_config_merge/0},
      {"limits validation", fun test_limits_validation/0},
      {"scaling status full", fun test_scaling_status_full/0},
      {"unknown request", fun test_unknown_request/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_initial_state() ->
    %% Should be disabled initially
    ?assertNot(flurm_cloud_scaler:is_enabled()),

    %% No provider set
    ?assertEqual(undefined, flurm_cloud_scaler:get_provider()),

    %% Default limits
    {Min, Max} = flurm_cloud_scaler:get_limits(),
    ?assertEqual(0, Min),
    ?assertEqual(100, Max),

    %% Empty config
    ?assertEqual(#{}, flurm_cloud_scaler:get_config()).

test_set_provider() ->
    %% Set AWS provider
    ok = flurm_cloud_scaler:set_provider(aws),
    ?assertEqual(aws, flurm_cloud_scaler:get_provider()),

    %% Change to GCP
    ok = flurm_cloud_scaler:set_provider(gcp),
    ?assertEqual(gcp, flurm_cloud_scaler:get_provider()),

    %% Change to Azure
    ok = flurm_cloud_scaler:set_provider(azure),
    ?assertEqual(azure, flurm_cloud_scaler:get_provider()).

test_enable_without_provider() ->
    %% Ensure no provider
    ?assertEqual(undefined, flurm_cloud_scaler:get_provider()),

    %% Enable should fail
    Result = flurm_cloud_scaler:enable(),
    ?assertEqual({error, no_provider}, Result),

    %% Should still be disabled
    ?assertNot(flurm_cloud_scaler:is_enabled()).

test_enable_with_provider() ->
    %% Set provider first
    ok = flurm_cloud_scaler:set_provider(aws),

    %% Enable should succeed
    ok = flurm_cloud_scaler:enable(),
    ?assert(flurm_cloud_scaler:is_enabled()),

    %% Disable
    ok = flurm_cloud_scaler:disable(),
    ?assertNot(flurm_cloud_scaler:is_enabled()),

    %% Re-enable
    ok = flurm_cloud_scaler:enable(),
    ?assert(flurm_cloud_scaler:is_enabled()),

    %% Double disable should be fine
    ok = flurm_cloud_scaler:disable(),
    ok = flurm_cloud_scaler:disable(),
    ?assertNot(flurm_cloud_scaler:is_enabled()).

test_set_get_config() ->
    %% Set AWS config
    Config = #{
        asg_name => <<"my-asg">>,
        region => <<"us-west-2">>,
        instance_type => <<"c5.xlarge">>
    },
    ok = flurm_cloud_scaler:set_config(Config),

    %% Get config
    Retrieved = flurm_cloud_scaler:get_config(),
    ?assertEqual(<<"my-asg">>, maps:get(asg_name, Retrieved)),
    ?assertEqual(<<"us-west-2">>, maps:get(region, Retrieved)),
    ?assertEqual(<<"c5.xlarge">>, maps:get(instance_type, Retrieved)).

test_set_get_limits() ->
    %% Default limits
    {Min0, Max0} = flurm_cloud_scaler:get_limits(),
    ?assertEqual(0, Min0),
    ?assertEqual(100, Max0),

    %% Set new limits
    ok = flurm_cloud_scaler:set_limits(5, 50),
    {Min1, Max1} = flurm_cloud_scaler:get_limits(),
    ?assertEqual(5, Min1),
    ?assertEqual(50, Max1),

    %% Set min=max
    ok = flurm_cloud_scaler:set_limits(10, 10),
    {Min2, Max2} = flurm_cloud_scaler:get_limits(),
    ?assertEqual(10, Min2),
    ?assertEqual(10, Max2),

    %% Set min=0
    ok = flurm_cloud_scaler:set_limits(0, 1000),
    {Min3, Max3} = flurm_cloud_scaler:get_limits(),
    ?assertEqual(0, Min3),
    ?assertEqual(1000, Max3).

test_get_scaling_status() ->
    Status = flurm_cloud_scaler:get_scaling_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(enabled, Status)),
    ?assert(maps:is_key(provider, Status)),
    ?assert(maps:is_key(min_nodes, Status)),
    ?assert(maps:is_key(max_nodes, Status)),
    ?assert(maps:is_key(last_scale_up, Status)),
    ?assert(maps:is_key(last_scale_down, Status)),
    ?assert(maps:is_key(pending_operations, Status)),

    %% Initial values
    ?assertEqual(false, maps:get(enabled, Status)),
    ?assertEqual(undefined, maps:get(provider, Status)),
    ?assertEqual(0, maps:get(min_nodes, Status)),
    ?assertEqual(100, maps:get(max_nodes, Status)),
    ?assertEqual(undefined, maps:get(last_scale_up, Status)),
    ?assertEqual(undefined, maps:get(last_scale_down, Status)),
    ?assertEqual(0, maps:get(pending_operations, Status)).

test_aws_scale_up() ->
    %% Set AWS provider and config
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{
        asg_name => <<"test-asg">>,
        region => <<"us-east-1">>
    }),

    %% Scale up
    {ok, InstanceIds} = flurm_cloud_scaler:scale_up(3),
    ?assert(is_list(InstanceIds)),
    ?assertEqual(3, length(InstanceIds)),

    %% All instance IDs should be binaries starting with "i-"
    lists:foreach(fun(Id) ->
        ?assert(is_binary(Id)),
        ?assertEqual(<<"i-">>, binary:part(Id, 0, 2))
    end, InstanceIds).

test_gcp_scale_up() ->
    %% Set GCP provider and config
    ok = flurm_cloud_scaler:set_provider(gcp),
    ok = flurm_cloud_scaler:set_config(#{
        mig_name => <<"test-mig">>,
        zone => <<"us-central1-a">>
    }),

    %% Scale up
    {ok, InstanceNames} = flurm_cloud_scaler:scale_up(2),
    ?assert(is_list(InstanceNames)),
    ?assertEqual(2, length(InstanceNames)),

    %% All instance names should be binaries starting with "instance-"
    lists:foreach(fun(Name) ->
        ?assert(is_binary(Name)),
        ?assertEqual(<<"instance-">>, binary:part(Name, 0, 9))
    end, InstanceNames).

test_azure_scale_up() ->
    %% Set Azure provider and config
    ok = flurm_cloud_scaler:set_provider(azure),
    ok = flurm_cloud_scaler:set_config(#{
        vmss_name => <<"test-vmss">>,
        resource_group => <<"test-rg">>
    }),

    %% Scale up
    {ok, VmIds} = flurm_cloud_scaler:scale_up(4),
    ?assert(is_list(VmIds)),
    ?assertEqual(4, length(VmIds)),

    %% All VM IDs should be binaries starting with "vm_"
    lists:foreach(fun(Id) ->
        ?assert(is_binary(Id)),
        ?assertEqual(<<"vm_">>, binary:part(Id, 0, 3))
    end, VmIds).

test_aws_scale_down() ->
    %% Set AWS provider and config
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{
        asg_name => <<"test-asg">>,
        region => <<"us-east-1">>
    }),

    %% Scale down (no idle nodes, returns empty list)
    {ok, Terminated} = flurm_cloud_scaler:scale_down(2),
    ?assert(is_list(Terminated)).

test_gcp_scale_down() ->
    %% Set GCP provider and config
    ok = flurm_cloud_scaler:set_provider(gcp),
    ok = flurm_cloud_scaler:set_config(#{
        mig_name => <<"test-mig">>,
        zone => <<"us-central1-a">>
    }),

    %% Scale down
    {ok, Terminated} = flurm_cloud_scaler:scale_down(2),
    ?assert(is_list(Terminated)).

test_azure_scale_down() ->
    %% Set Azure provider and config
    ok = flurm_cloud_scaler:set_provider(azure),
    ok = flurm_cloud_scaler:set_config(#{
        vmss_name => <<"test-vmss">>,
        resource_group => <<"test-rg">>
    }),

    %% Scale down
    {ok, Terminated} = flurm_cloud_scaler:scale_down(2),
    ?assert(is_list(Terminated)).

test_scale_down_no_provider() ->
    %% Try scale down without provider
    ?assertEqual(undefined, flurm_cloud_scaler:get_provider()),
    Result = flurm_cloud_scaler:scale_down(1),
    ?assertEqual({error, no_provider}, Result).

test_scale_up_no_provider() ->
    %% Try scale up without provider
    ?assertEqual(undefined, flurm_cloud_scaler:get_provider()),
    Result = flurm_cloud_scaler:scale_up(1),
    ?assertEqual({error, no_provider}, Result).

test_aws_scale_up_missing_config() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    %% No asg_name in config
    ok = flurm_cloud_scaler:set_config(#{region => <<"us-east-1">>}),

    Result = flurm_cloud_scaler:scale_up(1),
    ?assertEqual({error, missing_asg_name}, Result).

test_gcp_scale_up_missing_config() ->
    ok = flurm_cloud_scaler:set_provider(gcp),
    %% No mig_name in config
    ok = flurm_cloud_scaler:set_config(#{zone => <<"us-central1-a">>}),

    Result = flurm_cloud_scaler:scale_up(1),
    ?assertEqual({error, missing_mig_name}, Result).

test_azure_scale_up_missing_config() ->
    ok = flurm_cloud_scaler:set_provider(azure),
    %% No vmss_name in config
    ok = flurm_cloud_scaler:set_config(#{resource_group => <<"test-rg">>}),

    Result = flurm_cloud_scaler:scale_up(1),
    ?assertEqual({error, missing_vmss_name}, Result).

test_azure_scale_down_missing_vmss() ->
    ok = flurm_cloud_scaler:set_provider(azure),
    %% Only resource_group, no vmss_name
    ok = flurm_cloud_scaler:set_config(#{resource_group => <<"test-rg">>}),

    Result = flurm_cloud_scaler:scale_down(1),
    ?assertEqual({error, missing_vmss_name}, Result).

test_azure_scale_down_missing_rg() ->
    ok = flurm_cloud_scaler:set_provider(azure),
    %% Only vmss_name, no resource_group
    ok = flurm_cloud_scaler:set_config(#{vmss_name => <<"test-vmss">>}),

    Result = flurm_cloud_scaler:scale_down(1),
    ?assertEqual({error, missing_resource_group}, Result).

test_force_evaluation_disabled() ->
    %% force_evaluation when disabled does nothing
    ?assertNot(flurm_cloud_scaler:is_enabled()),
    ok = flurm_cloud_scaler:force_evaluation(),

    %% Wait for cast to be processed
    _ = sys:get_state(flurm_cloud_scaler),

    %% Should still be disabled
    ?assertNot(flurm_cloud_scaler:is_enabled()).

test_force_evaluation_enabled() ->
    %% Set up provider and enable
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{
        asg_name => <<"test-asg">>,
        region => <<"us-east-1">>
    }),
    ok = flurm_cloud_scaler:enable(),

    %% Force evaluation
    ok = flurm_cloud_scaler:force_evaluation(),

    %% Wait for cast to be processed
    _ = sys:get_state(flurm_cloud_scaler),

    %% Should still be enabled
    ?assert(flurm_cloud_scaler:is_enabled()).

test_config_merge() ->
    %% Set initial config
    ok = flurm_cloud_scaler:set_config(#{
        asg_name => <<"asg-1">>,
        region => <<"us-east-1">>
    }),

    Config1 = flurm_cloud_scaler:get_config(),
    ?assertEqual(<<"asg-1">>, maps:get(asg_name, Config1)),
    ?assertEqual(<<"us-east-1">>, maps:get(region, Config1)),

    %% Merge more config - should keep existing and add new
    ok = flurm_cloud_scaler:set_config(#{
        instance_type => <<"c5.xlarge">>,
        region => <<"us-west-2">>  % Override
    }),

    Config2 = flurm_cloud_scaler:get_config(),
    ?assertEqual(<<"asg-1">>, maps:get(asg_name, Config2)),  % Kept
    ?assertEqual(<<"us-west-2">>, maps:get(region, Config2)),  % Overridden
    ?assertEqual(<<"c5.xlarge">>, maps:get(instance_type, Config2)).  % New

test_limits_validation() ->
    %% Valid limits
    ok = flurm_cloud_scaler:set_limits(0, 100),
    ok = flurm_cloud_scaler:set_limits(10, 10),
    ok = flurm_cloud_scaler:set_limits(0, 0),
    ok = flurm_cloud_scaler:set_limits(50, 100).

test_scaling_status_full() ->
    %% Set up full state
    ok = flurm_cloud_scaler:set_provider(gcp),
    ok = flurm_cloud_scaler:set_config(#{
        mig_name => <<"test-mig">>,
        zone => <<"us-central1-a">>
    }),
    ok = flurm_cloud_scaler:set_limits(5, 50),
    ok = flurm_cloud_scaler:enable(),

    %% Do a scale operation to set timestamps
    {ok, _} = flurm_cloud_scaler:scale_up(1),

    %% Get status
    Status = flurm_cloud_scaler:get_scaling_status(),

    ?assertEqual(true, maps:get(enabled, Status)),
    ?assertEqual(gcp, maps:get(provider, Status)),
    ?assertEqual(5, maps:get(min_nodes, Status)),
    ?assertEqual(50, maps:get(max_nodes, Status)),
    ?assert(is_integer(maps:get(last_scale_up, Status))),
    ?assertEqual(0, maps:get(pending_operations, Status)).

test_unknown_request() ->
    %% Unknown call
    Result = gen_server:call(flurm_cloud_scaler, {unknown_request, test}),
    ?assertEqual({error, unknown_request}, Result).

%%====================================================================
%% Internal Function Tests
%%====================================================================

internal_functions_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"collect_scaling_metrics empty", fun test_collect_metrics_empty/0},
      {"collect_scaling_metrics with jobs", fun test_collect_metrics_with_jobs/0},
      {"collect_resource_utilization empty", fun test_collect_resource_empty/0},
      {"collect_resource_utilization with nodes", fun test_collect_resource_with_nodes/0},
      {"estimate_pending_demand empty", fun test_estimate_demand_empty/0},
      {"estimate_pending_demand with jobs", fun test_estimate_demand_with_jobs/0},
      {"determine_scaling_action none", fun test_determine_action_none/0},
      {"determine_scaling_action scale up high util", fun test_determine_action_scale_up_util/0},
      {"determine_scaling_action scale up pending", fun test_determine_action_scale_up_pending/0},
      {"determine_scaling_action scale up min nodes", fun test_determine_action_scale_up_min/0},
      {"determine_scaling_action scale down", fun test_determine_action_scale_down/0},
      {"calculate_scale_up_count", fun test_calculate_scale_up_count/0},
      {"calculate_scale_down_count", fun test_calculate_scale_down_count/0},
      {"find_idle_nodes empty", fun test_find_idle_nodes_empty/0},
      {"find_idle_nodes with idle", fun test_find_idle_nodes_with_idle/0},
      {"find_idle_nodes partial", fun test_find_idle_nodes_partial/0},
      {"test_state helper", fun test_test_state_helper/0}
     ]}.

test_collect_metrics_empty() ->
    %% With empty mocks already set up
    Metrics = flurm_cloud_scaler:collect_scaling_metrics(),

    ?assert(is_map(Metrics)),
    ?assertEqual(0, maps:get(pending_jobs, Metrics)),
    ?assertEqual(0, maps:get(total_nodes, Metrics)),
    ?assertEqual(0, maps:get(up_nodes, Metrics)),
    ?assert(is_float(maps:get(utilization, Metrics))).

test_collect_metrics_with_jobs() ->
    %% Mock pending jobs
    meck:expect(flurm_job_registry, count_by_state, fun() ->
        #{pending => 15, running => 10, completed => 5}
    end),
    meck:expect(flurm_node_registry, count_by_state, fun() ->
        #{up => 5, down => 1, drain => 0}
    end),

    Metrics = flurm_cloud_scaler:collect_scaling_metrics(),

    ?assertEqual(15, maps:get(pending_jobs, Metrics)),
    ?assertEqual(6, maps:get(total_nodes, Metrics)),  % 5+1+0
    ?assertEqual(5, maps:get(up_nodes, Metrics)).

test_collect_resource_empty() ->
    %% Empty list
    Result = flurm_cloud_scaler:collect_resource_utilization(),
    ?assertEqual({0, 0, 0, 0}, Result).

test_collect_resource_with_nodes() ->
    %% Mock node list with entries
    meck:expect(flurm_node_registry, list_nodes, fun() ->
        [{<<"node1">>, self()}, {<<"node2">>, self()}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun
        (<<"node1">>) ->
            {ok, #node_entry{name = <<"node1">>, pid = self(), hostname = <<"host1">>,
                  state = up, partitions = [], cpus_total = 16, cpus_avail = 8,
                  memory_total = 64*1024, memory_avail = 32*1024, gpus_total = 0,
                  gpus_avail = 0, allocations = #{}}};
        (<<"node2">>) ->
            {ok, #node_entry{name = <<"node2">>, pid = self(), hostname = <<"host2">>,
                  state = up, partitions = [], cpus_total = 32, cpus_avail = 16,
                  memory_total = 128*1024, memory_avail = 64*1024, gpus_total = 0,
                  gpus_avail = 0, allocations = #{}}}
    end),

    Result = flurm_cloud_scaler:collect_resource_utilization(),

    %% Total CPUs: 16+32=48, Allocated: (16-8)+(32-16)=8+16=24
    %% Total Memory: 64+128=192, Allocated: (64-32)+(128-64)=32+64=96
    ?assertEqual({48, 24, 192*1024, 96*1024}, Result).

test_estimate_demand_empty() ->
    %% Empty pending jobs
    Result = flurm_cloud_scaler:estimate_pending_demand(),
    ?assertEqual(0, Result).

test_estimate_demand_with_jobs() ->
    %% Mock pending jobs
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(pending) ->
        [{1001, self()}, {1002, self()}, {1003, self()}]
    end),
    meck:expect(flurm_job, get_info, fun(_Pid) ->
        {ok, #{num_nodes => 2, cpus => 4}}
    end),

    Result = flurm_cloud_scaler:estimate_pending_demand(),

    %% 3 jobs x 2 nodes each = 6
    ?assertEqual(6, Result).

test_determine_action_none() ->
    %% Normal operation - no scaling needed
    Metrics = #{
        pending_jobs => 0,
        total_nodes => 10,
        utilization => 0.5,  % 50% - between thresholds
        pending_demand => 0
    },
    State = flurm_cloud_scaler:test_state([
        {enabled, true},
        {min_nodes, 5},
        {max_nodes, 20}
    ]),

    Action = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    ?assertEqual(none, Action).

test_determine_action_scale_up_util() ->
    %% High utilization - need scale up
    Metrics = #{
        pending_jobs => 0,
        total_nodes => 10,
        utilization => 0.9,  % 90% > 80% threshold
        pending_demand => 0
    },
    State = flurm_cloud_scaler:test_state([
        {enabled, true},
        {min_nodes, 5},
        {max_nodes, 20}
    ]),

    Action = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    ?assertMatch({scale_up, _}, Action).

test_determine_action_scale_up_pending() ->
    %% Many pending jobs - need scale up
    Metrics = #{
        pending_jobs => 15,  % > 10 threshold
        total_nodes => 10,
        utilization => 0.5,
        pending_demand => 5
    },
    State = flurm_cloud_scaler:test_state([
        {enabled, true},
        {min_nodes, 5},
        {max_nodes, 20}
    ]),

    Action = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    ?assertMatch({scale_up, _}, Action).

test_determine_action_scale_up_min() ->
    %% Below minimum nodes
    Metrics = #{
        pending_jobs => 0,
        total_nodes => 3,
        utilization => 0.5,
        pending_demand => 0
    },
    State = flurm_cloud_scaler:test_state([
        {enabled, true},
        {min_nodes, 5},  % Min is 5, but only 3 total
        {max_nodes, 20}
    ]),

    Action = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    ?assertMatch({scale_up, _}, Action).

test_determine_action_scale_down() ->
    %% Low utilization, no pending - can scale down
    Metrics = #{
        pending_jobs => 0,
        total_nodes => 15,
        utilization => 0.1,  % 10% < 30% threshold
        pending_demand => 0
    },
    State = flurm_cloud_scaler:test_state([
        {enabled, true},
        {min_nodes, 5},
        {max_nodes, 20}
    ]),

    Action = flurm_cloud_scaler:determine_scaling_action(Metrics, State),
    ?assertMatch({scale_down, _}, Action).

test_calculate_scale_up_count() ->
    %% With pending demand
    Metrics1 = #{
        pending_demand => 5,
        total_nodes => 10
    },
    State1 = flurm_cloud_scaler:test_state([{max_nodes, 20}]),

    Count1 = flurm_cloud_scaler:calculate_scale_up_count(Metrics1, State1),
    ?assertEqual(5, Count1),

    %% With zero pending demand - minimum 1
    Metrics2 = #{
        pending_demand => 0,
        total_nodes => 10
    },
    State2 = flurm_cloud_scaler:test_state([{max_nodes, 20}]),

    Count2 = flurm_cloud_scaler:calculate_scale_up_count(Metrics2, State2),
    ?assertEqual(1, Count2),

    %% Limited by max
    Metrics3 = #{
        pending_demand => 50,
        total_nodes => 18
    },
    State3 = flurm_cloud_scaler:test_state([{max_nodes, 20}]),

    Count3 = flurm_cloud_scaler:calculate_scale_up_count(Metrics3, State3),
    ?assertEqual(2, Count3).  % Can only add 2 more (20-18)

test_calculate_scale_down_count() ->
    %% Low utilization
    Metrics1 = #{
        total_nodes => 20,
        utilization => 0.1  % Very low
    },
    State1 = flurm_cloud_scaler:test_state([{min_nodes, 5}]),

    Count1 = flurm_cloud_scaler:calculate_scale_down_count(Metrics1, State1),
    ?assert(Count1 >= 1),
    ?assert(Count1 =< 15),  % Can't go below min

    %% Near threshold
    Metrics2 = #{
        total_nodes => 10,
        utilization => 0.25
    },
    State2 = flurm_cloud_scaler:test_state([{min_nodes, 5}]),

    Count2 = flurm_cloud_scaler:calculate_scale_down_count(Metrics2, State2),
    ?assert(Count2 >= 1).

test_find_idle_nodes_empty() ->
    %% No nodes
    Result = flurm_cloud_scaler:find_idle_nodes(5),
    ?assertEqual([], Result).

test_find_idle_nodes_with_idle() ->
    %% Mock nodes - some idle
    meck:expect(flurm_node_registry, list_nodes, fun() ->
        [{<<"node1">>, self()}, {<<"node2">>, self()}, {<<"node3">>, self()}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun
        (<<"node1">>) ->
            %% Idle - all CPUs available
            {ok, #node_entry{name = <<"node1">>, pid = self(), hostname = <<"host1">>,
                  state = up, partitions = [], cpus_total = 16, cpus_avail = 16,
                  memory_total = 64*1024, memory_avail = 64*1024, gpus_total = 0,
                  gpus_avail = 0, allocations = #{}}};
        (<<"node2">>) ->
            %% Busy - some CPUs used
            {ok, #node_entry{name = <<"node2">>, pid = self(), hostname = <<"host2">>,
                  state = up, partitions = [], cpus_total = 16, cpus_avail = 8,
                  memory_total = 64*1024, memory_avail = 32*1024, gpus_total = 0,
                  gpus_avail = 0, allocations = #{}}};
        (<<"node3">>) ->
            %% Idle - all CPUs available
            {ok, #node_entry{name = <<"node3">>, pid = self(), hostname = <<"host3">>,
                  state = up, partitions = [], cpus_total = 16, cpus_avail = 16,
                  memory_total = 64*1024, memory_avail = 64*1024, gpus_total = 0,
                  gpus_avail = 0, allocations = #{}}}
    end),

    Result = flurm_cloud_scaler:find_idle_nodes(5),

    %% Should return node1 and node3 (idle ones)
    ?assertEqual(2, length(Result)),
    ?assert(lists:member(<<"node1">>, Result)),
    ?assert(lists:member(<<"node3">>, Result)).

test_find_idle_nodes_partial() ->
    %% Mock more idle nodes than requested
    meck:expect(flurm_node_registry, list_nodes, fun() ->
        [{<<"node1">>, self()}, {<<"node2">>, self()},
         {<<"node3">>, self()}, {<<"node4">>, self()}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(NodeName) ->
        %% All nodes idle
        {ok, #node_entry{name = NodeName, pid = self(), hostname = <<"host">>,
              state = up, partitions = [], cpus_total = 16, cpus_avail = 16,
              memory_total = 64*1024, memory_avail = 64*1024, gpus_total = 0,
              gpus_avail = 0, allocations = #{}}}
    end),

    %% Request only 2
    Result = flurm_cloud_scaler:find_idle_nodes(2),

    ?assertEqual(2, length(Result)).

test_test_state_helper() ->
    %% Test the test_state helper function
    State1 = flurm_cloud_scaler:test_state([]),
    %% Default values
    ?assertNot(element(2, State1)),  % enabled = false
    ?assertEqual(undefined, element(3, State1)),  % provider
    ?assertEqual(#{}, element(4, State1)),  % config
    ?assertEqual(0, element(8, State1)),  % min_nodes
    ?assertEqual(100, element(9, State1)),  % max_nodes

    %% With custom values
    State2 = flurm_cloud_scaler:test_state([
        {enabled, true},
        {provider, aws},
        {config, #{key => value}},
        {min_nodes, 10},
        {max_nodes, 50}
    ]),
    ?assert(element(2, State2)),  % enabled = true
    ?assertEqual(aws, element(3, State2)),
    ?assertEqual(#{key => value}, element(4, State2)),
    ?assertEqual(10, element(8, State2)),
    ?assertEqual(50, element(9, State2)).

%%====================================================================
%% Scaling Evaluation Tests
%%====================================================================

scaling_evaluation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"evaluate_scaling timer disabled", fun test_eval_timer_disabled/0},
      {"evaluate_scaling timer enabled", fun test_eval_timer_enabled/0},
      {"scale up cooldown", fun test_scale_up_cooldown/0},
      {"scale down cooldown", fun test_scale_down_cooldown/0},
      {"auto scale up on high load", fun test_auto_scale_up_high_load/0},
      {"auto scale down on low load", fun test_auto_scale_down_low_load/0}
     ]}.

test_eval_timer_disabled() ->
    %% When disabled, evaluate_scaling message does nothing
    ?assertNot(flurm_cloud_scaler:is_enabled()),

    %% Send timer message directly
    flurm_cloud_scaler ! evaluate_scaling,
    _ = sys:get_state(flurm_cloud_scaler),

    %% Should still be disabled
    ?assertNot(flurm_cloud_scaler:is_enabled()).

test_eval_timer_enabled() ->
    %% Set up and enable
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),
    ok = flurm_cloud_scaler:enable(),

    %% Trigger evaluation
    flurm_cloud_scaler ! evaluate_scaling,
    _ = sys:get_state(flurm_cloud_scaler),

    %% Should still be enabled
    ?assert(flurm_cloud_scaler:is_enabled()).

test_scale_up_cooldown() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),

    %% First scale up
    {ok, _} = flurm_cloud_scaler:scale_up(1),

    %% Status should show last_scale_up is set
    Status = flurm_cloud_scaler:get_scaling_status(),
    ?assert(is_integer(maps:get(last_scale_up, Status))).

test_scale_down_cooldown() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),

    %% First scale down
    {ok, _} = flurm_cloud_scaler:scale_down(1),

    %% Status should show last_scale_down is set
    Status = flurm_cloud_scaler:get_scaling_status(),
    ?assert(is_integer(maps:get(last_scale_down, Status))).

test_auto_scale_up_high_load() ->
    %% Mock high load
    meck:expect(flurm_job_registry, count_by_state, fun() ->
        #{pending => 20, running => 50}  % Many pending jobs
    end),
    meck:expect(flurm_node_registry, count_by_state, fun() ->
        #{up => 5, down => 0, drain => 0}
    end),

    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),
    ok = flurm_cloud_scaler:set_limits(0, 100),
    ok = flurm_cloud_scaler:enable(),

    %% Force evaluation
    ok = flurm_cloud_scaler:force_evaluation(),
    _ = sys:get_state(flurm_cloud_scaler),

    %% Should have triggered scale up (last_scale_up should be set)
    Status = flurm_cloud_scaler:get_scaling_status(),
    ?assert(is_integer(maps:get(last_scale_up, Status))).

test_auto_scale_down_low_load() ->
    %% Mock low load
    meck:expect(flurm_job_registry, count_by_state, fun() ->
        #{pending => 0, running => 1}  % Almost no jobs
    end),
    meck:expect(flurm_node_registry, count_by_state, fun() ->
        #{up => 10, down => 0, drain => 0}  % Many nodes
    end),
    meck:expect(flurm_node_registry, list_nodes, fun() ->
        [{<<"n1">>, self()}, {<<"n2">>, self()}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        %% All nodes idle
        {ok, #node_entry{name = Name, pid = self(), hostname = <<"host">>,
              state = up, partitions = [], cpus_total = 16, cpus_avail = 16,
              memory_total = 64*1024, memory_avail = 64*1024, gpus_total = 0,
              gpus_avail = 0, allocations = #{}}}
    end),

    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),
    ok = flurm_cloud_scaler:set_limits(0, 100),
    ok = flurm_cloud_scaler:enable(),

    %% Force evaluation
    ok = flurm_cloud_scaler:force_evaluation(),
    _ = sys:get_state(flurm_cloud_scaler),

    %% Status should exist (evaluation happened)
    Status = flurm_cloud_scaler:get_scaling_status(),
    ?assert(maps:get(enabled, Status)).

%%====================================================================
%% Provider Specific Tests
%%====================================================================

provider_specific_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"aws default region", fun test_aws_default_region/0},
      {"gcp default zone", fun test_gcp_default_zone/0},
      {"aws scale down with idle nodes", fun test_aws_scale_down_idle/0},
      {"gcp scale down with idle nodes", fun test_gcp_scale_down_idle/0},
      {"azure scale down with idle nodes", fun test_azure_scale_down_idle/0},
      {"aws scale down no idle", fun test_aws_scale_down_no_idle/0},
      {"provider switch during operation", fun test_provider_switch/0},
      {"multiple scale operations", fun test_multiple_scale_ops/0}
     ]}.

test_aws_default_region() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),
    %% No region specified - should use default us-east-1

    {ok, InstanceIds} = flurm_cloud_scaler:scale_up(1),
    ?assertEqual(1, length(InstanceIds)).

test_gcp_default_zone() ->
    ok = flurm_cloud_scaler:set_provider(gcp),
    ok = flurm_cloud_scaler:set_config(#{mig_name => <<"test-mig">>}),
    %% No zone specified - should use default us-central1-a

    {ok, InstanceNames} = flurm_cloud_scaler:scale_up(1),
    ?assertEqual(1, length(InstanceNames)).

test_aws_scale_down_idle() ->
    %% Mock idle nodes
    meck:expect(flurm_node_registry, list_nodes, fun() ->
        [{<<"i-12345">>, self()}, {<<"i-67890">>, self()}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, pid = self(), hostname = <<"host">>,
              state = up, partitions = [], cpus_total = 16, cpus_avail = 16,
              memory_total = 64*1024, memory_avail = 64*1024, gpus_total = 0,
              gpus_avail = 0, allocations = #{}}}
    end),

    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{
        asg_name => <<"test-asg">>,
        region => <<"us-west-2">>
    }),

    {ok, Terminated} = flurm_cloud_scaler:scale_down(2),
    ?assertEqual(2, length(Terminated)).

test_gcp_scale_down_idle() ->
    %% Mock idle nodes
    meck:expect(flurm_node_registry, list_nodes, fun() ->
        [{<<"instance-1">>, self()}, {<<"instance-2">>, self()}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, pid = self(), hostname = <<"host">>,
              state = up, partitions = [], cpus_total = 8, cpus_avail = 8,
              memory_total = 32*1024, memory_avail = 32*1024, gpus_total = 0,
              gpus_avail = 0, allocations = #{}}}
    end),

    ok = flurm_cloud_scaler:set_provider(gcp),
    ok = flurm_cloud_scaler:set_config(#{
        mig_name => <<"test-mig">>,
        zone => <<"europe-west1-b">>
    }),

    {ok, Terminated} = flurm_cloud_scaler:scale_down(2),
    ?assertEqual(2, length(Terminated)).

test_azure_scale_down_idle() ->
    %% Mock idle nodes
    meck:expect(flurm_node_registry, list_nodes, fun() ->
        [{<<"vm_1">>, self()}, {<<"vm_2">>, self()}, {<<"vm_3">>, self()}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        {ok, #node_entry{name = Name, pid = self(), hostname = <<"host">>,
              state = up, partitions = [], cpus_total = 4, cpus_avail = 4,
              memory_total = 16*1024, memory_avail = 16*1024, gpus_total = 0,
              gpus_avail = 0, allocations = #{}}}
    end),

    ok = flurm_cloud_scaler:set_provider(azure),
    ok = flurm_cloud_scaler:set_config(#{
        vmss_name => <<"test-vmss">>,
        resource_group => <<"test-rg">>
    }),

    {ok, Terminated} = flurm_cloud_scaler:scale_down(2),
    ?assertEqual(2, length(Terminated)).

test_aws_scale_down_no_idle() ->
    %% Mock busy nodes
    meck:expect(flurm_node_registry, list_nodes, fun() ->
        [{<<"i-busy1">>, self()}, {<<"i-busy2">>, self()}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(Name) ->
        %% All nodes busy (cpus_avail < cpus_total)
        {ok, #node_entry{name = Name, pid = self(), hostname = <<"host">>,
              state = up, partitions = [], cpus_total = 16, cpus_avail = 8,
              memory_total = 64*1024, memory_avail = 32*1024, gpus_total = 0,
              gpus_avail = 0, allocations = #{}}}
    end),

    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),

    {ok, Terminated} = flurm_cloud_scaler:scale_down(2),
    %% No idle nodes to terminate
    ?assertEqual(0, length(Terminated)).

test_provider_switch() ->
    %% Start with AWS
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"aws-asg">>}),
    {ok, AwsIds} = flurm_cloud_scaler:scale_up(1),
    ?assert(binary:match(hd(AwsIds), <<"i-">>) =/= nomatch),

    %% Switch to GCP
    ok = flurm_cloud_scaler:set_provider(gcp),
    ok = flurm_cloud_scaler:set_config(#{mig_name => <<"gcp-mig">>}),
    {ok, GcpNames} = flurm_cloud_scaler:scale_up(1),
    ?assert(binary:match(hd(GcpNames), <<"instance-">>) =/= nomatch),

    %% Switch to Azure
    ok = flurm_cloud_scaler:set_provider(azure),
    ok = flurm_cloud_scaler:set_config(#{
        vmss_name => <<"azure-vmss">>,
        resource_group => <<"azure-rg">>
    }),
    {ok, AzureIds} = flurm_cloud_scaler:scale_up(1),
    ?assert(binary:match(hd(AzureIds), <<"vm_">>) =/= nomatch).

test_multiple_scale_ops() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),

    %% Multiple scale up operations
    {ok, Ids1} = flurm_cloud_scaler:scale_up(2),
    {ok, Ids2} = flurm_cloud_scaler:scale_up(3),
    {ok, Ids3} = flurm_cloud_scaler:scale_up(5),

    ?assertEqual(2, length(Ids1)),
    ?assertEqual(3, length(Ids2)),
    ?assertEqual(5, length(Ids3)),

    %% Scale down operations
    {ok, _} = flurm_cloud_scaler:scale_down(1),
    {ok, _} = flurm_cloud_scaler:scale_down(2).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"scale up count 1", fun test_scale_up_one/0},
      {"scale up large count", fun test_scale_up_large/0},
      {"limits at zero", fun test_limits_zero/0},
      {"config empty map", fun test_config_empty/0},
      {"provider undefined after clear", fun test_provider_undefined/0},
      {"handle_cast unknown", fun test_handle_cast_unknown/0},
      {"handle_info unknown", fun test_handle_info_unknown/0},
      {"registry errors", fun test_registry_errors/0},
      {"node entry errors", fun test_node_entry_errors/0},
      {"job info errors", fun test_job_info_errors/0}
     ]}.

test_scale_up_one() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),

    {ok, Ids} = flurm_cloud_scaler:scale_up(1),
    ?assertEqual(1, length(Ids)).

test_scale_up_large() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),

    {ok, Ids} = flurm_cloud_scaler:scale_up(100),
    ?assertEqual(100, length(Ids)).

test_limits_zero() ->
    ok = flurm_cloud_scaler:set_limits(0, 0),
    {Min, Max} = flurm_cloud_scaler:get_limits(),
    ?assertEqual(0, Min),
    ?assertEqual(0, Max).

test_config_empty() ->
    ok = flurm_cloud_scaler:set_config(#{}),
    Config = flurm_cloud_scaler:get_config(),
    ?assertEqual(#{}, Config).

test_provider_undefined() ->
    %% Initially undefined
    ?assertEqual(undefined, flurm_cloud_scaler:get_provider()),

    %% Set and verify
    ok = flurm_cloud_scaler:set_provider(aws),
    ?assertEqual(aws, flurm_cloud_scaler:get_provider()).

test_handle_cast_unknown() ->
    %% Send unknown cast
    gen_server:cast(flurm_cloud_scaler, {unknown_cast, data}),
    _ = sys:get_state(flurm_cloud_scaler),

    %% Server should still be running
    ?assert(is_process_alive(whereis(flurm_cloud_scaler))).

test_handle_info_unknown() ->
    %% Send unknown message
    flurm_cloud_scaler ! {unknown_message, data},
    _ = sys:get_state(flurm_cloud_scaler),

    %% Server should still be running
    ?assert(is_process_alive(whereis(flurm_cloud_scaler))).

test_registry_errors() ->
    %% Mock registry to throw errors
    meck:expect(flurm_job_registry, count_by_state, fun() ->
        throw(registry_error)
    end),
    meck:expect(flurm_node_registry, count_by_state, fun() ->
        throw(node_registry_error)
    end),

    %% Should handle gracefully
    Metrics = flurm_cloud_scaler:collect_scaling_metrics(),
    ?assertEqual(0, maps:get(pending_jobs, Metrics)),
    ?assertEqual(0, maps:get(total_nodes, Metrics)).

test_node_entry_errors() ->
    meck:expect(flurm_node_registry, list_nodes, fun() ->
        [{<<"node1">>, self()}, {<<"node2">>, self()}]
    end),
    meck:expect(flurm_node_registry, get_node_entry, fun(_) ->
        {error, not_found}
    end),

    %% Should return empty resource utilization
    Result = flurm_cloud_scaler:collect_resource_utilization(),
    ?assertEqual({0, 0, 0, 0}, Result).

test_job_info_errors() ->
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(pending) ->
        [{1001, self()}, {1002, self()}]
    end),
    meck:expect(flurm_job, get_info, fun(_) ->
        {error, not_found}
    end),

    %% Should handle gracefully
    Demand = flurm_cloud_scaler:estimate_pending_demand(),
    ?assertEqual(0, Demand).

%%====================================================================
%% State Machine Tests
%%====================================================================

state_machine_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"enable disable cycle", fun test_enable_disable_cycle/0},
      {"provider change while enabled", fun test_provider_change_enabled/0},
      {"config update while scaling", fun test_config_update_scaling/0},
      {"limits change during operation", fun test_limits_change_operation/0}
     ]}.

test_enable_disable_cycle() ->
    ok = flurm_cloud_scaler:set_provider(aws),

    %% Cycle through enable/disable multiple times
    lists:foreach(fun(_) ->
        ok = flurm_cloud_scaler:enable(),
        ?assert(flurm_cloud_scaler:is_enabled()),
        ok = flurm_cloud_scaler:disable(),
        ?assertNot(flurm_cloud_scaler:is_enabled())
    end, lists:seq(1, 5)).

test_provider_change_enabled() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:enable(),

    %% Change provider while enabled
    ok = flurm_cloud_scaler:set_provider(gcp),

    %% Should still be enabled with new provider
    ?assert(flurm_cloud_scaler:is_enabled()),
    ?assertEqual(gcp, flurm_cloud_scaler:get_provider()).

test_config_update_scaling() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"asg-1">>}),
    ok = flurm_cloud_scaler:enable(),

    %% Scale up
    {ok, _} = flurm_cloud_scaler:scale_up(1),

    %% Update config during operation
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"asg-2">>}),

    %% New scale should use new config
    Config = flurm_cloud_scaler:get_config(),
    ?assertEqual(<<"asg-2">>, maps:get(asg_name, Config)).

test_limits_change_operation() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),
    ok = flurm_cloud_scaler:set_limits(0, 100),
    ok = flurm_cloud_scaler:enable(),

    %% Scale up
    {ok, _} = flurm_cloud_scaler:scale_up(10),

    %% Change limits
    ok = flurm_cloud_scaler:set_limits(5, 20),

    %% Verify new limits
    {Min, Max} = flurm_cloud_scaler:get_limits(),
    ?assertEqual(5, Min),
    ?assertEqual(20, Max).

%%====================================================================
%% Concurrent Operations Tests
%%====================================================================

concurrent_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"concurrent scale operations", fun test_concurrent_scale/0},
      {"concurrent config updates", fun test_concurrent_config/0},
      {"rapid enable disable", fun test_rapid_enable_disable/0}
     ]}.

test_concurrent_scale() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),

    %% Spawn multiple processes to scale
    Self = self(),
    Pids = [spawn(fun() ->
        Result = flurm_cloud_scaler:scale_up(1),
        Self ! {done, self(), Result}
    end) || _ <- lists:seq(1, 5)],

    %% Collect results
    Results = [receive {done, Pid, R} -> R end || Pid <- Pids],

    %% All should succeed
    lists:foreach(fun(R) ->
        ?assertMatch({ok, _}, R)
    end, Results).

test_concurrent_config() ->
    ok = flurm_cloud_scaler:set_provider(aws),

    %% Concurrent config updates
    Self = self(),
    Pids = [spawn(fun() ->
        ok = flurm_cloud_scaler:set_config(#{key => N}),
        Self ! {done, self()}
    end) || N <- lists:seq(1, 10)],

    %% Wait for all
    [receive {done, Pid} -> ok end || Pid <- Pids],

    %% Config should have some value
    Config = flurm_cloud_scaler:get_config(),
    ?assert(maps:is_key(key, Config)).

test_rapid_enable_disable() ->
    ok = flurm_cloud_scaler:set_provider(aws),

    %% Rapid toggle
    lists:foreach(fun(_) ->
        flurm_cloud_scaler:enable(),
        flurm_cloud_scaler:disable()
    end, lists:seq(1, 20)),

    %% Should end up disabled
    ?assertNot(flurm_cloud_scaler:is_enabled()).

%%====================================================================
%% Integration Tests
%%====================================================================

integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"full scaling workflow aws", fun test_full_workflow_aws/0},
      {"full scaling workflow gcp", fun test_full_workflow_gcp/0},
      {"full scaling workflow azure", fun test_full_workflow_azure/0},
      {"status reflects operations", fun test_status_reflects_ops/0}
     ]}.

test_full_workflow_aws() ->
    %% Complete AWS workflow
    ok = flurm_cloud_scaler:set_provider(aws),
    ?assertEqual(aws, flurm_cloud_scaler:get_provider()),

    ok = flurm_cloud_scaler:set_config(#{
        asg_name => <<"prod-asg">>,
        region => <<"us-west-2">>,
        instance_type => <<"c5.2xlarge">>
    }),

    ok = flurm_cloud_scaler:set_limits(2, 50),
    ok = flurm_cloud_scaler:enable(),
    ?assert(flurm_cloud_scaler:is_enabled()),

    %% Scale up
    {ok, UpIds} = flurm_cloud_scaler:scale_up(5),
    ?assertEqual(5, length(UpIds)),

    %% Check status
    Status1 = flurm_cloud_scaler:get_scaling_status(),
    ?assert(is_integer(maps:get(last_scale_up, Status1))),

    %% Scale down
    {ok, DownIds} = flurm_cloud_scaler:scale_down(2),
    ?assert(is_list(DownIds)),

    %% Check status again
    Status2 = flurm_cloud_scaler:get_scaling_status(),
    ?assert(is_integer(maps:get(last_scale_down, Status2))),

    %% Disable
    ok = flurm_cloud_scaler:disable(),
    ?assertNot(flurm_cloud_scaler:is_enabled()).

test_full_workflow_gcp() ->
    %% Complete GCP workflow
    ok = flurm_cloud_scaler:set_provider(gcp),
    ok = flurm_cloud_scaler:set_config(#{
        mig_name => <<"prod-mig">>,
        zone => <<"us-central1-a">>,
        machine_type => <<"n2-standard-4">>
    }),

    ok = flurm_cloud_scaler:set_limits(1, 100),
    ok = flurm_cloud_scaler:enable(),

    {ok, UpNames} = flurm_cloud_scaler:scale_up(3),
    ?assertEqual(3, length(UpNames)),

    {ok, DownNames} = flurm_cloud_scaler:scale_down(1),
    ?assert(is_list(DownNames)),

    ok = flurm_cloud_scaler:disable().

test_full_workflow_azure() ->
    %% Complete Azure workflow
    ok = flurm_cloud_scaler:set_provider(azure),
    ok = flurm_cloud_scaler:set_config(#{
        vmss_name => <<"prod-vmss">>,
        resource_group => <<"prod-rg">>,
        vm_size => <<"Standard_D4s_v3">>
    }),

    ok = flurm_cloud_scaler:set_limits(0, 200),
    ok = flurm_cloud_scaler:enable(),

    {ok, UpIds} = flurm_cloud_scaler:scale_up(4),
    ?assertEqual(4, length(UpIds)),

    {ok, DownIds} = flurm_cloud_scaler:scale_down(2),
    ?assert(is_list(DownIds)),

    ok = flurm_cloud_scaler:disable().

test_status_reflects_ops() ->
    ok = flurm_cloud_scaler:set_provider(aws),
    ok = flurm_cloud_scaler:set_config(#{asg_name => <<"test-asg">>}),
    ok = flurm_cloud_scaler:set_limits(5, 50),

    %% Check initial status
    Status1 = flurm_cloud_scaler:get_scaling_status(),
    ?assertEqual(false, maps:get(enabled, Status1)),
    ?assertEqual(aws, maps:get(provider, Status1)),
    ?assertEqual(5, maps:get(min_nodes, Status1)),
    ?assertEqual(50, maps:get(max_nodes, Status1)),

    %% Enable and check
    ok = flurm_cloud_scaler:enable(),
    Status2 = flurm_cloud_scaler:get_scaling_status(),
    ?assertEqual(true, maps:get(enabled, Status2)),

    %% Scale and check
    {ok, _} = flurm_cloud_scaler:scale_up(1),
    Status3 = flurm_cloud_scaler:get_scaling_status(),
    ?assert(is_integer(maps:get(last_scale_up, Status3))),

    %% Change limits and check
    ok = flurm_cloud_scaler:set_limits(10, 100),
    Status4 = flurm_cloud_scaler:get_scaling_status(),
    ?assertEqual(10, maps:get(min_nodes, Status4)),
    ?assertEqual(100, maps:get(max_nodes, Status4)).
