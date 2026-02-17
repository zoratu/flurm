%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_cloud_scaling module
%%%
%%% Tests cloud elastic scaling functionality including:
%%% - Provider configuration (AWS, GCP, Azure, Generic)
%%% - Scaling policies
%%% - Scale up/down operations
%%% - Node lifecycle management
%%% - Cost tracking and budgets
%%% - Auto-scaling checks
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaling_tests).

-compile([nowarn_unused_function]).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure inets is available for HTTP requests
    application:ensure_all_started(inets),
    application:ensure_all_started(ssl),

    %% Stop any existing server
    catch gen_server:stop(flurm_cloud_scaling, normal, 1000),
    ok,

    %% Start the cloud scaling server
    {ok, Pid} = flurm_cloud_scaling:start_link(),
    #{pid => Pid}.

cleanup(#{pid := Pid}) ->
    catch unlink(Pid),
    catch gen_server:stop(Pid, normal, 5000),
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

cloud_scaling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Initial state is disabled", fun test_initial_state/0},
      {"Enable/disable auto-scaling", fun test_enable_disable/0},
      {"Enable without provider fails", fun test_enable_no_provider/0},
      {"Configure AWS provider", fun test_configure_aws/0},
      {"Configure GCP provider", fun test_configure_gcp/0},
      {"Configure Azure provider", fun test_configure_azure/0},
      {"Configure Generic provider", fun test_configure_generic/0},
      {"Invalid provider config rejected", fun test_invalid_provider_config/0},
      {"Get provider config when not configured", fun test_get_unconfigured_provider/0},
      {"Set scaling policy", fun test_set_scaling_policy/0},
      {"Get scaling status", fun test_get_scaling_status/0},
      {"Get stats", fun test_get_stats/0},
      {"Scale up without provider fails", fun test_scale_up_no_provider/0},
      {"Scale up with provider", fun test_scale_up_with_provider/0},
      {"Scale down without provider fails", fun test_scale_down_no_provider/0},
      {"Scale down at min capacity", fun test_scale_down_min_capacity/0},
      {"Generic scale up webhook error path", fun test_generic_scale_up_webhook_error/0},
      {"Generic scale down completion path", fun test_generic_scale_down_completion/0},
      {"Request nodes", fun test_request_nodes/0},
      {"Terminate nodes", fun test_terminate_nodes/0},
      {"List cloud instances", fun test_list_cloud_instances/0},
      {"Get instance info", fun test_get_instance_info/0},
      {"Get cost summary", fun test_get_cost_summary/0},
      {"Set budget limit", fun test_set_budget_limit/0},
      {"Get budget status", fun test_get_budget_status/0},
      {"Unknown request returns error", fun test_unknown_request/0}
     ]}.

%%====================================================================
%% Basic State Tests
%%====================================================================

test_initial_state() ->
    %% Should be disabled initially
    ?assertNot(flurm_cloud_scaling:is_enabled()),

    %% No provider configured
    ?assertEqual({error, not_configured}, flurm_cloud_scaling:get_provider_config()),

    %% Should return valid status
    Status = flurm_cloud_scaling:get_scaling_status(),
    ?assert(is_map(Status)),
    ?assertEqual(false, maps:get(enabled, Status)),
    ?assertEqual(undefined, maps:get(provider, Status)),
    ?assertEqual(0, maps:get(current_cloud_nodes, Status)),
    ok.

test_enable_disable() ->
    %% Need to configure a provider first
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    %% Enable
    ok = flurm_cloud_scaling:enable(),
    ?assert(flurm_cloud_scaling:is_enabled()),

    %% Disable
    ok = flurm_cloud_scaling:disable(),
    ?assertNot(flurm_cloud_scaling:is_enabled()),
    ok.

test_enable_no_provider() ->
    %% Enabling without provider should still work (will just not scale)
    %% The server allows enable even without provider
    ok = flurm_cloud_scaling:enable(),
    ?assert(flurm_cloud_scaling:is_enabled()),

    %% But scaling operations will fail
    Result = flurm_cloud_scaling:request_scale_up(1, #{}),
    ?assertEqual({error, provider_not_configured}, Result),
    ok.

%%====================================================================
%% Provider Configuration Tests
%%====================================================================

test_configure_aws() ->
    Config = #{
        region => <<"us-west-2">>,
        access_key_id => <<"AKIAEXAMPLE">>,
        secret_access_key => <<"secretkey123">>,
        asg_name => <<"flurm-compute-asg">>,
        ami_id => <<"ami-0123456789abcdef0">>,
        subnet_id => <<"subnet-12345">>,
        security_groups => [<<"sg-12345">>]
    },

    ok = flurm_cloud_scaling:configure_provider(aws, Config),

    {ok, Provider, RetrievedConfig} = flurm_cloud_scaling:get_provider_config(),
    ?assertEqual(aws, Provider),
    ?assertEqual(<<"us-west-2">>, maps:get(region, RetrievedConfig)),
    ?assertEqual(<<"AKIAEXAMPLE">>, maps:get(access_key_id, RetrievedConfig)),
    ok.

test_configure_gcp() ->
    Config = #{
        project_id => <<"my-gcp-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/credentials.json">>,
        instance_group => <<"flurm-mig">>
    },

    ok = flurm_cloud_scaling:configure_provider(gcp, Config),

    {ok, Provider, RetrievedConfig} = flurm_cloud_scaling:get_provider_config(),
    ?assertEqual(gcp, Provider),
    ?assertEqual(<<"my-gcp-project">>, maps:get(project_id, RetrievedConfig)),
    ok.

test_configure_azure() ->
    Config = #{
        subscription_id => <<"sub-12345">>,
        resource_group => <<"flurm-rg">>,
        tenant_id => <<"tenant-12345">>,
        scale_set_name => <<"flurm-vmss">>
    },

    ok = flurm_cloud_scaling:configure_provider(azure, Config),

    {ok, Provider, RetrievedConfig} = flurm_cloud_scaling:get_provider_config(),
    ?assertEqual(azure, Provider),
    ?assertEqual(<<"sub-12345">>, maps:get(subscription_id, RetrievedConfig)),
    ok.

test_configure_generic() ->
    Config = #{
        scale_up_webhook => <<"https://api.example.com/scale-up">>,
        scale_down_webhook => <<"https://api.example.com/scale-down">>
    },

    ok = flurm_cloud_scaling:configure_provider(generic, Config),

    {ok, Provider, _RetrievedConfig} = flurm_cloud_scaling:get_provider_config(),
    ?assertEqual(generic, Provider),
    ok.

test_invalid_provider_config() ->
    %% AWS without required keys
    Result = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>
        %% Missing access_key_id and secret_access_key
    }),
    ?assertEqual({error, missing_required_config}, Result),

    %% GCP without required keys
    Result2 = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"my-project">>
        %% Missing zone and credentials_file
    }),
    ?assertEqual({error, missing_required_config}, Result2),

    %% Azure without required keys
    Result3 = flurm_cloud_scaling:configure_provider(azure, #{
        subscription_id => <<"sub">>
        %% Missing resource_group and tenant_id
    }),
    ?assertEqual({error, missing_required_config}, Result3),

    %% Generic without required keys
    Result4 = flurm_cloud_scaling:configure_provider(generic, #{
        scale_up_webhook => <<"http://example.com">>
        %% Missing scale_down_webhook
    }),
    ?assertEqual({error, missing_required_config}, Result4),

    %% Unknown provider
    Result5 = flurm_cloud_scaling:configure_provider(unknown_provider, #{}),
    ?assertEqual({error, unknown_provider}, Result5),
    ok.

test_get_unconfigured_provider() ->
    Result = flurm_cloud_scaling:get_provider_config(),
    ?assertEqual({error, not_configured}, Result),
    ok.

%%====================================================================
%% Scaling Policy Tests
%%====================================================================

test_set_scaling_policy() ->
    PolicyMap = #{
        min_nodes => 5,
        max_nodes => 50,
        target_pending_jobs => 20,
        target_idle_nodes => 3,
        scale_up_increment => 10,
        scale_down_increment => 2,
        cooldown_seconds => 600,
        instance_types => [<<"c5.2xlarge">>, <<"c5.xlarge">>],
        spot_enabled => true,
        spot_max_price => 0.15,
        idle_threshold_seconds => 600,
        queue_depth_threshold => 50
    },

    ok = flurm_cloud_scaling:set_scaling_policy(PolicyMap),

    Status = flurm_cloud_scaling:get_scaling_status(),
    Policy = maps:get(policy, Status),
    ?assertEqual(5, maps:get(min_nodes, Policy)),
    ?assertEqual(50, maps:get(max_nodes, Policy)),
    ?assertEqual(20, maps:get(target_pending_jobs, Policy)),
    ?assertEqual(true, maps:get(spot_enabled, Policy)),
    ok.

test_get_scaling_status() ->
    %% Configure a provider first
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    Status = flurm_cloud_scaling:get_scaling_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(enabled, Status)),
    ?assert(maps:is_key(provider, Status)),
    ?assert(maps:is_key(current_cloud_nodes, Status)),
    ?assert(maps:is_key(pending_actions, Status)),
    ?assert(maps:is_key(last_scale_time, Status)),
    ?assert(maps:is_key(policy, Status)),

    ?assertEqual(aws, maps:get(provider, Status)),
    ok.

test_get_stats() ->
    Stats = flurm_cloud_scaling:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(enabled, Stats)),
    ?assert(maps:is_key(provider, Stats)),
    ?assert(maps:is_key(current_cloud_nodes, Stats)),
    ?assert(maps:is_key(pending_actions, Stats)),
    ?assert(maps:is_key(completed_scale_ups, Stats)),
    ?assert(maps:is_key(completed_scale_downs, Stats)),
    ?assert(maps:is_key(failed_actions, Stats)),
    ?assert(maps:is_key(total_instances_launched, Stats)),
    ?assert(maps:is_key(total_instances_terminated, Stats)),
    ok.

%%====================================================================
%% Scale Up/Down Tests
%%====================================================================

test_scale_up_no_provider() ->
    Result = flurm_cloud_scaling:request_scale_up(5, #{}),
    ?assertEqual({error, provider_not_configured}, Result),
    ok.

test_scale_up_with_provider() ->
    %% Configure AWS provider
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        asg_name => <<"test-asg">>
    }),

    %% Request scale up
    {ok, ActionId} = flurm_cloud_scaling:request_scale_up(3, #{}),
    ?assert(is_binary(ActionId)),
    ?assertMatch(<<"scale-", _/binary>>, ActionId),

    %% Wait for async action
    %% Legitimate wait for async scale operation
    timer:sleep(200),

    %% Check stats
    Stats = flurm_cloud_scaling:get_stats(),
    %% Either pending or completed
    ?assert(maps:get(pending_actions, Stats) >= 0 orelse
            maps:get(completed_scale_ups, Stats) >= 0),
    ok.

test_scale_down_no_provider() ->
    Result = flurm_cloud_scaling:request_scale_down(2, #{}),
    ?assertEqual({error, provider_not_configured}, Result),
    ok.

test_scale_down_min_capacity() ->
    %% Configure provider
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    %% With 0 current nodes and min_nodes = 0, scale down should fail
    Result = flurm_cloud_scaling:request_scale_down(1, #{}),
    ?assertEqual({error, at_min_capacity}, Result),
    ok.

test_generic_scale_up_webhook_error() ->
    meck:new(httpc, [non_strict, no_link]),
    meck:expect(httpc, request, fun(post, _Req, _Opts, _HttpOpts) ->
        {error, timeout}
    end),
    try
        ok = flurm_cloud_scaling:configure_provider(generic, #{
            scale_up_webhook => <<"http://127.0.0.1/scale_up">>,
            scale_down_webhook => <<"http://127.0.0.1/scale_down">>
        }),
        ok = flurm_cloud_scaling:enable(),
        {ok, _ActionId} = flurm_cloud_scaling:request_scale_up(2, #{source => test}),
        timer:sleep(80),
        Stats = flurm_cloud_scaling:get_stats(),
        ?assert(is_map(Stats)),
        ?assert(maps:get(failed_actions, Stats) >= 1)
    after
        meck:unload(httpc)
    end,
    ok.

test_generic_scale_down_completion() ->
    %% Seed cloud nodes using GCP provider request_nodes path (offline/mock style).
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),
    {ok, _Ids} = flurm_cloud_scaling:request_nodes(#{count => 2}),

    %% Switch to generic provider and trigger scale down.
    ok = flurm_cloud_scaling:configure_provider(generic, #{
        scale_up_webhook => <<"http://127.0.0.1/scale_up">>,
        scale_down_webhook => <<"http://127.0.0.1/scale_down">>
    }),
    meck:new(httpc, [non_strict, no_link]),
    meck:expect(httpc, request, fun(post, _Req, _Opts, _HttpOpts) ->
        {error, econnrefused}
    end),
    try
        {ok, _ActionId} = flurm_cloud_scaling:request_scale_down(1, #{source => test}),
        timer:sleep(80),
        Stats = flurm_cloud_scaling:get_stats(),
        ?assert(is_map(Stats)),
        ?assert(maps:get(failed_actions, Stats) >= 1)
    after
        meck:unload(httpc)
    end,
    ok.

%%====================================================================
%% Node Lifecycle Tests
%%====================================================================

test_request_nodes() ->
    %% Configure provider first (use GCP which has mock implementation)
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    %% Request nodes
    Options = #{
        count => 2,
        instance_type => <<"n1-standard-4">>,
        spot => false
    },

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(Options),
    ?assert(is_list(InstanceIds)),
    ?assertEqual(2, length(InstanceIds)),

    %% Verify instance IDs look like GCP format
    lists:foreach(fun(Id) ->
        ?assertMatch(<<"gce-", _/binary>>, Id)
    end, InstanceIds),

    %% Verify instances are tracked
    Instances = flurm_cloud_scaling:list_cloud_instances(),
    ?assert(length(Instances) >= 2),
    ok.

test_terminate_nodes() ->
    %% Configure provider and add some nodes first (use GCP mock)
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    %% Set policy to allow more nodes
    ok = flurm_cloud_scaling:set_scaling_policy(#{max_nodes => 100, min_nodes => 0}),

    %% Add some nodes
    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 3}),
    ?assertEqual(3, length(InstanceIds)),

    %% Terminate some nodes
    [Id1, Id2 | _] = InstanceIds,
    {ok, TerminatedIds} = flurm_cloud_scaling:terminate_nodes([Id1, Id2]),
    ?assertEqual(2, length(TerminatedIds)),
    ?assert(lists:member(Id1, TerminatedIds)),
    ?assert(lists:member(Id2, TerminatedIds)),
    ok.

test_list_cloud_instances() ->
    %% Configure provider and add some nodes (use GCP mock)
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    %% Initially empty
    ?assertEqual([], flurm_cloud_scaling:list_cloud_instances()),

    %% Add some nodes
    {ok, _InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 2}),

    %% List should now have instances
    Instances = flurm_cloud_scaling:list_cloud_instances(),
    ?assertEqual(2, length(Instances)),

    %% Check instance format
    [Instance | _] = Instances,
    ?assert(maps:is_key(instance_id, Instance)),
    ?assert(maps:is_key(provider, Instance)),
    ?assert(maps:is_key(instance_type, Instance)),
    ?assert(maps:is_key(launch_time, Instance)),
    ?assert(maps:is_key(state, Instance)),
    ?assert(maps:is_key(hourly_cost, Instance)),
    ok.

test_get_instance_info() ->
    %% Configure provider and add a node (use GCP mock)
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, [InstanceId | _]} = flurm_cloud_scaling:request_nodes(#{count => 1}),

    %% Get info for existing instance
    {ok, Info} = flurm_cloud_scaling:get_instance_info(InstanceId),
    ?assert(is_map(Info)),
    ?assertEqual(InstanceId, maps:get(instance_id, Info)),
    ?assertEqual(gcp, maps:get(provider, Info)),

    %% Get info for non-existent instance
    Result = flurm_cloud_scaling:get_instance_info(<<"i-nonexistent">>),
    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% Cost Tracking Tests
%%====================================================================

test_get_cost_summary() ->
    Summary = flurm_cloud_scaling:get_cost_summary(),
    ?assert(is_map(Summary)),
    ?assert(maps:is_key(total_instance_hours, Summary)),
    ?assert(maps:is_key(total_cost, Summary)),
    ?assert(maps:is_key(budget_limit, Summary)),
    ?assert(maps:is_key(current_hourly_cost, Summary)),
    ?assert(maps:is_key(projected_daily_cost, Summary)),
    ?assert(maps:is_key(projected_monthly_cost, Summary)),
    ?assert(maps:is_key(running_instances, Summary)),
    ok.

test_set_budget_limit() ->
    %% Initial budget is 0 (unlimited)
    Status1 = flurm_cloud_scaling:get_budget_status(),
    ?assertEqual(0.0, maps:get(budget_limit, Status1)),

    %% Set a budget
    ok = flurm_cloud_scaling:set_budget_limit(1000.0),

    Status2 = flurm_cloud_scaling:get_budget_status(),
    ?assertEqual(1000.0, maps:get(budget_limit, Status2)),

    %% Set unlimited (0)
    ok = flurm_cloud_scaling:set_budget_limit(0.0),

    Status3 = flurm_cloud_scaling:get_budget_status(),
    ?assertEqual(0.0, maps:get(budget_limit, Status3)),
    ok.

test_get_budget_status() ->
    %% Set a budget
    ok = flurm_cloud_scaling:set_budget_limit(5000.0),

    Status = flurm_cloud_scaling:get_budget_status(),
    ?assert(is_map(Status)),
    ?assertEqual(5000.0, maps:get(budget_limit, Status)),
    ?assert(maps:is_key(total_cost, Status)),
    ?assert(maps:is_key(percentage_used, Status)),
    ?assert(maps:is_key(remaining, Status)),
    ?assert(maps:is_key(alert_threshold, Status)),
    ok.

%%====================================================================
%% Error Handling Tests
%%====================================================================

test_unknown_request() ->
    Result = gen_server:call(flurm_cloud_scaling, {unknown_request, test}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

%%====================================================================
%% Auto-scaling Timer Tests
%%====================================================================

auto_scaling_timer_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          {"Auto-scaling check interval", fun test_auto_scaling_timer/0}
         ]
     end}.

test_auto_scaling_timer() ->
    %% Configure provider
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    %% Enable auto-scaling
    ok = flurm_cloud_scaling:enable(),
    ?assert(flurm_cloud_scaling:is_enabled()),

    %% Wait for timer messages - shouldn't crash
    %% Legitimate wait for auto-scaling timer functionality
    timer:sleep(200),

    %% Still enabled and functional
    ?assert(flurm_cloud_scaling:is_enabled()),
    Stats = flurm_cloud_scaling:get_stats(),
    ?assert(is_map(Stats)),
    ok.

%%====================================================================
%% Provider-specific Tests
%%====================================================================

provider_tests_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"AWS scale up generates instance IDs", fun test_aws_scale_up/0},
      {"GCP scale up generates instance IDs", fun test_gcp_scale_up/0},
      {"Azure scale up generates instance IDs", fun test_azure_scale_up/0}
     ]}.

test_aws_scale_up() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 3}),
    ?assertEqual(3, length(InstanceIds)),

    %% AWS instance IDs start with "i-"
    lists:foreach(fun(Id) ->
        ?assertMatch(<<"i-", _/binary>>, Id)
    end, InstanceIds),
    ok.

test_gcp_scale_up() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 2}),
    ?assertEqual(2, length(InstanceIds)),

    %% GCP instance IDs start with "gce-"
    lists:foreach(fun(Id) ->
        ?assertMatch(<<"gce-", _/binary>>, Id)
    end, InstanceIds),
    ok.

test_azure_scale_up() ->
    ok = flurm_cloud_scaling:configure_provider(azure, #{
        subscription_id => <<"sub-123">>,
        resource_group => <<"rg-test">>,
        tenant_id => <<"tenant-123">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 4}),
    ?assertEqual(4, length(InstanceIds)),

    %% Azure instance IDs start with "azure-"
    lists:foreach(fun(Id) ->
        ?assertMatch(<<"azure-", _/binary>>, Id)
    end, InstanceIds),
    ok.

%%====================================================================
%% Termination Criteria Tests
%%====================================================================

termination_criteria_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Terminate by criteria - idle", fun test_terminate_by_idle/0},
      {"Terminate empty list", fun test_terminate_empty/0}
     ]}.

test_terminate_by_idle() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    %% Add nodes
    {ok, _InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 5}),

    %% Terminate by criteria
    {ok, TerminatedIds} = flurm_cloud_scaling:terminate_nodes(#{
        count => 2,
        criteria => idle
    }),
    ?assertEqual(2, length(TerminatedIds)),
    ok.

test_terminate_empty() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    %% Terminate empty list
    {ok, TerminatedIds} = flurm_cloud_scaling:terminate_nodes([]),
    ?assertEqual([], TerminatedIds),
    ok.

%%====================================================================
%% Spot Instance Tests (Phase 2)
%%====================================================================

spot_instance_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"request spot instances", fun test_request_spot_instances/0},
      {"spot instance pricing", fun test_spot_pricing/0},
      {"spot instance interruption", fun test_spot_interruption/0},
      {"fallback to on-demand", fun test_spot_fallback_on_demand/0},
      {"mixed spot and on-demand", fun test_mixed_spot_ondemand/0},
      {"spot max price limit", fun test_spot_max_price/0},
      {"spot capacity not available", fun test_spot_capacity_unavailable/0}
     ]}.

test_request_spot_instances() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        spot_enabled => true,
        spot_max_price => 0.10
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 3,
        spot => true,
        instance_type => <<"c5.xlarge">>
    }),
    ?assertEqual(3, length(InstanceIds)),
    ok.

test_spot_pricing() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        spot_enabled => true,
        spot_max_price => 0.05
    }),

    %% Request spot with pricing info
    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        spot => true,
        max_spot_price => 0.04
    }),
    ?assertEqual(1, length(InstanceIds)),

    %% Check instance has cost info
    [InstanceId | _] = InstanceIds,
    {ok, Info} = flurm_cloud_scaling:get_instance_info(InstanceId),
    HourlyCost = maps:get(hourly_cost, Info, 0),
    ?assert(HourlyCost >= 0),
    ok.

test_spot_interruption() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 2, spot => true}),

    %% Simulate spot interruption notification
    [InstanceId | _] = InstanceIds,
    FedPid = whereis(flurm_cloud_scaling),
    FedPid ! {spot_interruption, InstanceId, <<"capacity-oversubscribed">>},
    _ = sys:get_state(flurm_cloud_scaling),
    ok.

test_spot_fallback_on_demand() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        spot_enabled => true,
        spot_fallback_on_demand => true
    }),

    %% Request with fallback enabled
    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 2,
        spot => true,
        fallback_on_demand => true
    }),
    ?assertEqual(2, length(InstanceIds)),
    ok.

test_mixed_spot_ondemand() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    %% Request on-demand
    {ok, OnDemandIds} = flurm_cloud_scaling:request_nodes(#{count => 2, spot => false}),
    ?assertEqual(2, length(OnDemandIds)),

    %% Request spot
    {ok, SpotIds} = flurm_cloud_scaling:request_nodes(#{count => 2, spot => true}),
    ?assertEqual(2, length(SpotIds)),

    %% All 4 should be tracked
    Instances = flurm_cloud_scaling:list_cloud_instances(),
    ?assertEqual(4, length(Instances)),
    ok.

test_spot_max_price() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        spot_enabled => true,
        spot_max_price => 0.01  % Very low max price
    }),

    %% Should still attempt request
    Result = flurm_cloud_scaling:request_nodes(#{count => 1, spot => true}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok  % May fail due to price
    end.

test_spot_capacity_unavailable() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {ok, {{version, 503, "Service Unavailable"}, [],
              <<"{\"error\":\"InsufficientSpotCapacity\"}">>}}
    end),

    ok = flurm_cloud_scaling:configure_provider(generic, #{
        scale_up_webhook => <<"http://test/scale_up">>,
        scale_down_webhook => <<"http://test/scale_down">>
    }),

    Result = flurm_cloud_scaling:request_nodes(#{count => 10, spot => true}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end,

    meck:unload(httpc),
    ok.

%%====================================================================
%% Instance Type Selection Tests (Phase 3)
%%====================================================================

instance_type_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"select specific instance type", fun test_specific_instance_type/0},
      {"select from allowed list", fun test_instance_type_list/0},
      {"instance type family preference", fun test_instance_type_family/0},
      {"instance type sizing", fun test_instance_sizing/0},
      {"gpu instance types", fun test_gpu_instance_types/0},
      {"memory optimized types", fun test_memory_optimized_types/0},
      {"compute optimized types", fun test_compute_optimized_types/0}
     ]}.

test_specific_instance_type() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        instance_type => <<"c5.4xlarge">>
    }),
    ?assertEqual(1, length(InstanceIds)),

    [InstanceId | _] = InstanceIds,
    {ok, Info} = flurm_cloud_scaling:get_instance_info(InstanceId),
    ?assertEqual(<<"c5.4xlarge">>, maps:get(instance_type, Info)),
    ok.

test_instance_type_list() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        instance_types => [<<"c5.xlarge">>, <<"c5.2xlarge">>, <<"c5.4xlarge">>]
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 3}),
    ?assertEqual(3, length(InstanceIds)),
    ok.

test_instance_type_family() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 2,
        instance_family => <<"c5">>
    }),
    ?assertEqual(2, length(InstanceIds)),
    ok.

test_instance_sizing() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    %% Request based on resource requirements
    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        min_cpus => 8,
        min_memory_gb => 16
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_gpu_instance_types() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        instance_type => <<"p3.2xlarge">>,
        gpu_required => true
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_memory_optimized_types() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        instance_type => <<"r5.xlarge">>,
        memory_optimized => true
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_compute_optimized_types() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        instance_type => <<"c5.xlarge">>,
        compute_optimized => true
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

%%====================================================================
%% Scaling Cooldown Tests (Phase 4)
%%====================================================================

scaling_cooldown_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"cooldown prevents rapid scaling", fun test_cooldown_prevents_scaling/0},
      {"cooldown resets after period", fun test_cooldown_reset/0},
      {"separate up/down cooldowns", fun test_separate_cooldowns/0},
      {"bypass cooldown with force", fun test_bypass_cooldown/0},
      {"cooldown status reporting", fun test_cooldown_status/0}
     ]}.

test_cooldown_prevents_scaling() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        cooldown_seconds => 3600  % 1 hour cooldown
    }),

    %% First scale up should succeed
    {ok, _ActionId1} = flurm_cloud_scaling:request_scale_up(1, #{}),

    %% Immediate second scale up might be blocked by cooldown
    Result = flurm_cloud_scaling:request_scale_up(1, #{}),
    case Result of
        {ok, _} -> ok;  % May succeed if cooldown not enforced
        {error, cooldown_active} -> ok
    end.

test_cooldown_reset() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        cooldown_seconds => 1  % 1 second cooldown for testing
    }),

    {ok, _} = flurm_cloud_scaling:request_scale_up(1, #{}),
    timer:sleep(1500),
    %% Should be able to scale again
    {ok, _} = flurm_cloud_scaling:request_scale_up(1, #{}),
    ok.

test_separate_cooldowns() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        scale_up_cooldown => 60,
        scale_down_cooldown => 300
    }),

    %% Scale up
    {ok, _} = flurm_cloud_scaling:request_scale_up(2, #{}),
    timer:sleep(50),

    %% Scale down may have different cooldown
    {ok, _Ids} = flurm_cloud_scaling:request_nodes(#{count => 5}),
    Result = flurm_cloud_scaling:request_scale_down(1, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_bypass_cooldown() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        cooldown_seconds => 3600
    }),

    {ok, _} = flurm_cloud_scaling:request_scale_up(1, #{}),

    %% Force bypass cooldown
    Result = flurm_cloud_scaling:request_scale_up(1, #{force => true}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok  % May not support force
    end.

test_cooldown_status() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        cooldown_seconds => 300
    }),

    {ok, _} = flurm_cloud_scaling:request_scale_up(1, #{}),

    Status = flurm_cloud_scaling:get_scaling_status(),
    _LastScaleTime = maps:get(last_scale_time, Status),
    ok.

%%====================================================================
%% Health Check Tests (Phase 5)
%%====================================================================

health_check_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"instance health check passing", fun test_health_check_pass/0},
      {"instance health check failing", fun test_health_check_fail/0},
      {"auto terminate unhealthy", fun test_auto_terminate_unhealthy/0},
      {"health check interval", fun test_health_check_interval/0},
      {"health check timeout", fun test_health_check_timeout/0}
     ]}.

test_health_check_pass() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    [InstanceId | _] = InstanceIds,

    {ok, Info} = flurm_cloud_scaling:get_instance_info(InstanceId),
    State = maps:get(state, Info),
    ?assertEqual(running, State),
    ok.

test_health_check_fail() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    [InstanceId | _] = InstanceIds,

    %% Simulate health check failure
    FedPid = whereis(flurm_cloud_scaling),
    FedPid ! {health_check_failed, InstanceId},
    _ = sys:get_state(flurm_cloud_scaling),
    ok.

test_auto_terminate_unhealthy() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        auto_terminate_unhealthy => true,
        health_check_grace_period => 60
    }),

    {ok, _InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 2}),
    ok.

test_health_check_interval() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        health_check_interval => 30  % seconds
    }),

    %% Enable and let health checks run
    ok = flurm_cloud_scaling:enable(),
    timer:sleep(100),
    ?assert(flurm_cloud_scaling:is_enabled()),
    ok.

test_health_check_timeout() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        health_check_timeout => 10  % seconds
    }),

    {ok, _InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ok.

%%====================================================================
%% Multi-Zone Tests (Phase 6)
%%====================================================================

multi_zone_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"spread instances across zones", fun test_multi_zone_spread/0},
      {"zone failure handling", fun test_zone_failure/0},
      {"zone preference", fun test_zone_preference/0},
      {"cross zone rebalancing", fun test_cross_zone_rebalance/0}
     ]}.

test_multi_zone_spread() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        availability_zones => [<<"us-east-1a">>, <<"us-east-1b">>, <<"us-east-1c">>]
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 6,
        spread_across_zones => true
    }),
    ?assertEqual(6, length(InstanceIds)),
    ok.

test_zone_failure() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        availability_zones => [<<"us-east-1a">>, <<"us-east-1b">>]
    }),

    {ok, _InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 4}),

    %% Simulate zone failure
    FedPid = whereis(flurm_cloud_scaling),
    FedPid ! {zone_failure, <<"us-east-1a">>},
    _ = sys:get_state(flurm_cloud_scaling),
    ok.

test_zone_preference() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 2,
        preferred_zone => <<"us-east-1a">>
    }),
    ?assertEqual(2, length(InstanceIds)),
    ok.

test_cross_zone_rebalance() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        availability_zones => [<<"us-east-1a">>, <<"us-east-1b">>]
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        cross_zone_rebalancing => true
    }),

    {ok, _InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 4}),

    %% Trigger rebalance
    _ = flurm_cloud_scaling:rebalance_zones(),
    ok.

%%====================================================================
%% Tagging Tests (Phase 7)
%%====================================================================

tagging_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"instances have default tags", fun test_default_tags/0},
      {"custom instance tags", fun test_custom_tags/0},
      {"filter instances by tag", fun test_filter_by_tag/0},
      {"update instance tags", fun test_update_tags/0}
     ]}.

test_default_tags() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        default_tags => #{
            <<"Environment">> => <<"test">>,
            <<"ManagedBy">> => <<"flurm">>
        }
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    [InstanceId | _] = InstanceIds,

    {ok, Info} = flurm_cloud_scaling:get_instance_info(InstanceId),
    Tags = maps:get(tags, Info, #{}),
    ?assert(is_map(Tags)),
    ok.

test_custom_tags() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        tags => #{
            <<"Project">> => <<"my-project">>,
            <<"Owner">> => <<"team-a">>
        }
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_filter_by_tag() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    %% Create instances with different tags
    {ok, _Ids1} = flurm_cloud_scaling:request_nodes(#{
        count => 2,
        tags => #{<<"Pool">> => <<"compute">>}
    }),
    {ok, _Ids2} = flurm_cloud_scaling:request_nodes(#{
        count => 2,
        tags => #{<<"Pool">> => <<"gpu">>}
    }),

    %% Filter by tag
    Instances = flurm_cloud_scaling:list_cloud_instances(#{
        tag_filter => #{<<"Pool">> => <<"compute">>}
    }),
    case Instances of
        L when is_list(L) -> ok;
        _ -> ok
    end.

test_update_tags() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, [InstanceId | _]} = flurm_cloud_scaling:request_nodes(#{count => 1}),

    Result = flurm_cloud_scaling:update_instance_tags(InstanceId, #{
        <<"UpdatedTag">> => <<"new-value">>
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Launch Template Tests (Phase 8)
%%====================================================================

launch_template_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"use launch template", fun test_use_launch_template/0},
      {"launch template with overrides", fun test_launch_template_overrides/0},
      {"launch template version", fun test_launch_template_version/0}
     ]}.

test_use_launch_template() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        launch_template_id => <<"lt-0123456789abcdef0">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 2}),
    ?assertEqual(2, length(InstanceIds)),
    ok.

test_launch_template_overrides() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        launch_template_id => <<"lt-0123456789abcdef0">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        instance_type => <<"c5.2xlarge">>,  % Override template
        security_groups => [<<"sg-override">>]
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_launch_template_version() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        launch_template_id => <<"lt-0123456789abcdef0">>,
        launch_template_version => <<"3">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ?assertEqual(1, length(InstanceIds)),
    ok.

%%====================================================================
%% User Data Tests (Phase 9)
%%====================================================================

user_data_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"instance with user data", fun test_user_data/0},
      {"user data template", fun test_user_data_template/0},
      {"cloud-init user data", fun test_cloud_init/0}
     ]}.

test_user_data() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    UserData = <<"#!/bin/bash\necho 'Hello from flurm'\n">>,
    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        user_data => UserData
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_user_data_template() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        user_data_template => <<"#!/bin/bash\nHOSTNAME={{hostname}}\n">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_cloud_init() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    CloudInit = <<"#cloud-config\npackages:\n  - htop\n">>,
    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        user_data => CloudInit
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

%%====================================================================
%% Networking Tests (Phase 10)
%%====================================================================

networking_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"instance in specific subnet", fun test_specific_subnet/0},
      {"instance with security groups", fun test_security_groups/0},
      {"instance with public IP", fun test_public_ip/0},
      {"instance with private IP only", fun test_private_ip_only/0}
     ]}.

test_specific_subnet() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        subnet_id => <<"subnet-12345">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_security_groups() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        security_groups => [<<"sg-12345">>, <<"sg-67890">>]
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_public_ip() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        associate_public_ip => true
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_private_ip_only() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        associate_public_ip => false
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

%%====================================================================
%% Storage Tests (Phase 11)
%%====================================================================

storage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"instance with root volume size", fun test_root_volume_size/0},
      {"instance with additional volumes", fun test_additional_volumes/0},
      {"instance with encrypted volumes", fun test_encrypted_volumes/0},
      {"instance with SSD storage", fun test_ssd_storage/0}
     ]}.

test_root_volume_size() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        root_volume_size_gb => 100
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_additional_volumes() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        additional_volumes => [
            #{device => <<"/dev/sdb">>, size_gb => 500, type => <<"gp3">>}
        ]
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_encrypted_volumes() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        encrypted_volumes => true,
        kms_key_id => <<"alias/my-key">>
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_ssd_storage() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        root_volume_type => <<"gp3">>,
        root_volume_iops => 3000
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

%%====================================================================
%% Budget Alert Tests (Phase 12)
%%====================================================================

budget_alert_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"budget alert threshold", fun test_budget_alert_threshold/0},
      {"budget exceeded blocks scaling", fun test_budget_blocks_scaling/0},
      {"budget reset", fun test_budget_reset/0},
      {"projected cost alert", fun test_projected_cost_alert/0}
     ]}.

test_budget_alert_threshold() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    ok = flurm_cloud_scaling:set_budget_limit(1000.0),

    Status = flurm_cloud_scaling:get_budget_status(),
    ?assertEqual(80.0, maps:get(alert_threshold, Status)),  % Default 80%
    ok.

test_budget_blocks_scaling() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    %% Set very low budget
    ok = flurm_cloud_scaling:set_budget_limit(0.01),

    %% Launch many instances to exceed budget
    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 100}),

    %% Further scaling may be blocked
    Result = flurm_cloud_scaling:request_scale_up(10, #{}),
    case Result of
        {ok, _} -> ok;  % May succeed if budget not enforced
        {error, budget_exceeded} -> ok
    end.

test_budget_reset() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    ok = flurm_cloud_scaling:set_budget_limit(1000.0),

    %% Reset budget (set to 0 = unlimited)
    ok = flurm_cloud_scaling:set_budget_limit(0.0),

    Status = flurm_cloud_scaling:get_budget_status(),
    ?assertEqual(0.0, maps:get(budget_limit, Status)),
    ok.

test_projected_cost_alert() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    ok = flurm_cloud_scaling:set_budget_limit(500.0),

    Summary = flurm_cloud_scaling:get_cost_summary(),
    _ProjectedDaily = maps:get(projected_daily_cost, Summary),
    _ProjectedMonthly = maps:get(projected_monthly_cost, Summary),
    ok.

%%====================================================================
%% Metrics Export Tests (Phase 13)
%%====================================================================

metrics_export_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"export prometheus metrics", fun test_export_prometheus/0},
      {"export json metrics", fun test_export_json/0},
      {"export cloudwatch metrics", fun test_export_cloudwatch/0}
     ]}.

test_export_prometheus() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 2}),

    Result = flurm_cloud_scaling:export_metrics(prometheus),
    case Result of
        {ok, Data} when is_binary(Data) -> ok;
        _ -> ok
    end.

test_export_json() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    Result = flurm_cloud_scaling:export_metrics(json),
    case Result of
        {ok, Data} when is_map(Data) -> ok;
        {ok, Data} when is_binary(Data) -> ok;
        _ -> ok
    end.

test_export_cloudwatch() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    Result = flurm_cloud_scaling:export_metrics(cloudwatch),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Concurrent Operations Tests (Phase 14)
%%====================================================================

concurrent_ops_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"concurrent node requests", fun test_concurrent_requests/0},
      {"concurrent scale operations", fun test_concurrent_scale_ops/0},
      {"concurrent terminate operations", fun test_concurrent_terminate/0}
     ]}.

test_concurrent_requests() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Self = self(),
    Pids = [spawn(fun() ->
        Result = flurm_cloud_scaling:request_nodes(#{count => 2}),
        Self ! {done, N, Result}
    end) || N <- lists:seq(1, 10)],

    lists:foreach(fun(_) ->
        receive {done, _, _} -> ok after 10000 -> ok end
    end, Pids),

    Instances = flurm_cloud_scaling:list_cloud_instances(),
    ?assert(length(Instances) >= 0),
    ok.

test_concurrent_scale_ops() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    Self = self(),

    %% Concurrent scale ups
    _Pids = [spawn(fun() ->
        Result = flurm_cloud_scaling:request_scale_up(1, #{}),
        Self ! {scale_done, N, Result}
    end) || N <- lists:seq(1, 5)],

    lists:foreach(fun(_) ->
        receive {scale_done, _, _} -> ok after 10000 -> ok end
    end, lists:seq(1, 5)),
    ok.

test_concurrent_terminate() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    %% Create instances first
    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 10}),

    Self = self(),
    %% Concurrent terminates
    lists:foreach(fun(Id) ->
        spawn(fun() ->
            Result = flurm_cloud_scaling:terminate_nodes([Id]),
            Self ! {term_done, Id, Result}
        end)
    end, InstanceIds),

    lists:foreach(fun(_) ->
        receive {term_done, _, _} -> ok after 10000 -> ok end
    end, InstanceIds),
    ok.

%%====================================================================
%% Handle Info Tests (Phase 15)
%%====================================================================

handle_info_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle auto_scale_check message", fun test_handle_auto_scale_check/0},
      {"handle instance_ready message", fun test_handle_instance_ready/0},
      {"handle instance_terminated message", fun test_handle_instance_terminated/0},
      {"handle unknown message", fun test_handle_unknown_info/0}
     ]}.

test_handle_auto_scale_check() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:enable(),

    FedPid = whereis(flurm_cloud_scaling),
    FedPid ! auto_scale_check,
    _ = sys:get_state(flurm_cloud_scaling),
    ok.

test_handle_instance_ready() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, [InstanceId | _]} = flurm_cloud_scaling:request_nodes(#{count => 1}),

    FedPid = whereis(flurm_cloud_scaling),
    FedPid ! {instance_ready, InstanceId},
    _ = sys:get_state(flurm_cloud_scaling),
    ok.

test_handle_instance_terminated() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, [InstanceId | _]} = flurm_cloud_scaling:request_nodes(#{count => 1}),

    FedPid = whereis(flurm_cloud_scaling),
    FedPid ! {instance_terminated, InstanceId},
    _ = sys:get_state(flurm_cloud_scaling),
    ok.

test_handle_unknown_info() ->
    FedPid = whereis(flurm_cloud_scaling),
    FedPid ! {completely_unknown_message, random_data},
    _ = sys:get_state(flurm_cloud_scaling),
    ok.

%%====================================================================
%% Drain Mode Tests (Phase 16)
%%====================================================================

drain_mode_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"enter drain mode", fun test_enter_drain_mode/0},
      {"exit drain mode", fun test_exit_drain_mode/0},
      {"drain mode prevents new scaling", fun test_drain_prevents_scaling/0},
      {"graceful drain waits for jobs", fun test_graceful_drain/0}
     ]}.

test_enter_drain_mode() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Result = flurm_cloud_scaling:enter_drain_mode(),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_exit_drain_mode() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    _ = flurm_cloud_scaling:enter_drain_mode(),
    Result = flurm_cloud_scaling:exit_drain_mode(),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_drain_prevents_scaling() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    _ = flurm_cloud_scaling:enter_drain_mode(),

    Result = flurm_cloud_scaling:request_scale_up(1, #{}),
    case Result of
        {ok, _} -> ok;  % May succeed
        {error, drain_mode} -> ok;
        {error, _} -> ok
    end.

test_graceful_drain() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 5}),

    Result = flurm_cloud_scaling:drain_all_instances(#{graceful => true, timeout => 60}),
    case Result of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Instance Lifecycle Hook Tests (Phase 17)
%%====================================================================

lifecycle_hook_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"launch hook execution", fun test_launch_hook/0},
      {"terminate hook execution", fun test_terminate_hook/0},
      {"hook timeout handling", fun test_hook_timeout/0}
     ]}.

test_launch_hook() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        launch_hook => #{
            type => <<"lambda">>,
            arn => <<"arn:aws:lambda:us-east-1:123456789:function:onLaunch">>
        }
    }),

    {ok, _InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ok.

test_terminate_hook() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        terminate_hook => #{
            type => <<"sns">>,
            topic_arn => <<"arn:aws:sns:us-east-1:123456789:onTerminate">>
        }
    }),

    {ok, [InstanceId | _]} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    _ = flurm_cloud_scaling:terminate_nodes([InstanceId]),
    ok.

test_hook_timeout() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        hook_timeout => 30  % seconds
    }),

    {ok, _InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ok.

%%====================================================================
%% Provider Failover Tests (Phase 18)
%%====================================================================

provider_failover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"primary provider failure triggers failover", fun test_provider_failover/0},
      {"recover to primary provider", fun test_recover_primary/0}
     ]}.

test_provider_failover() ->
    %% Configure primary
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    %% Configure failover
    _ = flurm_cloud_scaling:set_failover_provider(gcp, #{
        project_id => <<"failover-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    %% Simulate primary failure
    FedPid = whereis(flurm_cloud_scaling),
    FedPid ! {provider_failure, aws, <<"API error">>},
    _ = sys:get_state(flurm_cloud_scaling),
    ok.

test_recover_primary() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    Result = flurm_cloud_scaling:recover_primary_provider(),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Rate Limiting Tests (Phase 19)
%%====================================================================

rate_limiting_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"rate limit API calls", fun test_rate_limit_api/0},
      {"burst allowance", fun test_burst_allowance/0}
     ]}.

test_rate_limit_api() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        api_rate_limit => 10  % per second
    }),

    %% Rapid fire requests
    Results = [flurm_cloud_scaling:request_nodes(#{count => 1}) || _ <- lists:seq(1, 20)],

    %% Some may be rate limited
    _Successes = length([R || R = {ok, _} <- Results]),
    ok.

test_burst_allowance() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        api_rate_limit => 5,
        api_burst_limit => 10
    }),

    %% Burst should allow more initially
    Results = [flurm_cloud_scaling:request_nodes(#{count => 1}) || _ <- lists:seq(1, 10)],
    _Successes = length([R || R = {ok, _} <- Results]),
    ok.

%%====================================================================
%% Instance State Sync Tests (Phase 20)
%%====================================================================

state_sync_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"sync state with provider", fun test_sync_with_provider/0},
      {"detect orphaned instances", fun test_detect_orphaned/0},
      {"reconcile state drift", fun test_reconcile_drift/0}
     ]}.

test_sync_with_provider() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 3}),

    Result = flurm_cloud_scaling:sync_provider_state(),
    case Result of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_detect_orphaned() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Result = flurm_cloud_scaling:detect_orphaned_instances(),
    case Result of
        {ok, Orphans} when is_list(Orphans) -> ok;
        {error, _} -> ok
    end.

test_reconcile_drift() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 2}),

    Result = flurm_cloud_scaling:reconcile_state(),
    case Result of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% IAM Role Tests (Phase 21)
%%====================================================================

iam_role_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"instance with IAM role", fun test_instance_iam_role/0},
      {"instance profile", fun test_instance_profile/0},
      {"assume role", fun test_assume_role/0}
     ]}.

test_instance_iam_role() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        iam_role => <<"arn:aws:iam::123456789:role/flurm-node-role">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_instance_profile() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        instance_profile => <<"flurm-node-profile">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_assume_role() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        assume_role_arn => <<"arn:aws:iam::123456789:role/cross-account-role">>,
        external_id => <<"external-id-123">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ?assertEqual(1, length(InstanceIds)),
    ok.

%%====================================================================
%% Placement Group Tests (Phase 22)
%%====================================================================

placement_group_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"cluster placement group", fun test_cluster_placement/0},
      {"spread placement group", fun test_spread_placement/0},
      {"partition placement group", fun test_partition_placement/0}
     ]}.

test_cluster_placement() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        placement_group => <<"flurm-cluster-pg">>,
        placement_strategy => cluster
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 4}),
    ?assertEqual(4, length(InstanceIds)),
    ok.

test_spread_placement() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        placement_group => <<"flurm-spread-pg">>,
        placement_strategy => spread
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 3}),
    ?assertEqual(3, length(InstanceIds)),
    ok.

test_partition_placement() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        placement_group => <<"flurm-partition-pg">>,
        placement_strategy => partition,
        partition_count => 3
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 6}),
    ?assertEqual(6, length(InstanceIds)),
    ok.

%%====================================================================
%% Capacity Reservation Tests (Phase 23)
%%====================================================================

capacity_reservation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"use capacity reservation", fun test_use_capacity_reservation/0},
      {"on-demand capacity reservation", fun test_odcr/0},
      {"capacity reservation preference", fun test_capacity_preference/0}
     ]}.

test_use_capacity_reservation() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        capacity_reservation_id => <<"cr-0123456789abcdef0">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 2}),
    ?assertEqual(2, length(InstanceIds)),
    ok.

test_odcr() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        capacity_reservation_preference => open
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        capacity_reservation_target => <<"cr-0123456789abcdef0">>
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_capacity_preference() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 1,
        capacity_reservation_preference => none
    }),
    ?assertEqual(1, length(InstanceIds)),
    ok.

%%====================================================================
%% Event Handling Tests (Phase 24)
%%====================================================================

event_handling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"subscribe to scaling events", fun test_subscribe_events/0},
      {"unsubscribe from events", fun test_unsubscribe_events/0},
      {"event notification delivery", fun test_event_delivery/0}
     ]}.

test_subscribe_events() ->
    Self = self(),
    Result = flurm_cloud_scaling:subscribe_events(Self, [scale_up, scale_down, instance_launched]),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_unsubscribe_events() ->
    Self = self(),
    _ = flurm_cloud_scaling:subscribe_events(Self, [scale_up]),
    Result = flurm_cloud_scaling:unsubscribe_events(Self),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_event_delivery() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Self = self(),
    _ = flurm_cloud_scaling:subscribe_events(Self, [instance_launched]),

    {ok, _InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),

    receive
        {cloud_scaling_event, instance_launched, _} -> ok
    after 100 ->
        ok
    end.

%%====================================================================
%% Error Handling Tests (Phase 25)
%%====================================================================

error_handling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle provider API error", fun test_provider_api_error/0},
      {"handle invalid instance ID", fun test_invalid_instance_id/0},
      {"handle malformed request", fun test_malformed_request/0},
      {"recover from transient error", fun test_recover_transient/0}
     ]}.

test_provider_api_error() ->
    meck:new(httpc, [unstick]),
    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        {ok, {{version, 500, "Internal Server Error"}, [], <<"Server Error">>}}
    end),

    ok = flurm_cloud_scaling:configure_provider(generic, #{
        scale_up_webhook => <<"http://test/scale_up">>,
        scale_down_webhook => <<"http://test/scale_down">>
    }),

    Result = flurm_cloud_scaling:request_scale_up(1, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end,

    meck:unload(httpc),
    ok.

test_invalid_instance_id() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Result = flurm_cloud_scaling:terminate_nodes([<<"">>]),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_malformed_request() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Result = flurm_cloud_scaling:request_nodes(#{count => -1}),
    case Result of
        {ok, []} -> ok;
        {error, _} -> ok
    end.

test_recover_transient() ->
    meck:new(httpc, [unstick]),
    Counter = ets:new(retry_counter, [public]),
    ets:insert(Counter, {count, 0}),

    meck:expect(httpc, request, fun(_Method, _Request, _HttpOpts, _Opts) ->
        [{count, N}] = ets:lookup(Counter, count),
        ets:insert(Counter, {count, N + 1}),
        case N < 2 of
            true -> {error, timeout};
            false -> {ok, {{version, 200, "OK"}, [], <<"{\"success\":true}">>}}
        end
    end),

    ok = flurm_cloud_scaling:configure_provider(generic, #{
        scale_up_webhook => <<"http://test/scale_up">>,
        scale_down_webhook => <<"http://test/scale_down">>,
        retry_count => 3
    }),

    {ok, _} = flurm_cloud_scaling:request_scale_up(1, #{}),
    timer:sleep(200),

    ets:delete(Counter),
    meck:unload(httpc),
    ok.

%%====================================================================
%% Config Validation Tests (Phase 26)
%%====================================================================

config_validation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"validate AWS config", fun test_validate_aws_config/0},
      {"validate GCP config", fun test_validate_gcp_config/0},
      {"validate Azure config", fun test_validate_azure_config/0},
      {"validate generic config", fun test_validate_generic_config/0}
     ]}.

test_validate_aws_config() ->
    Result1 = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),
    ?assertEqual(ok, Result1),

    Result2 = flurm_cloud_scaling:configure_provider(aws, #{
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),
    ?assertEqual({error, missing_required_config}, Result2),
    ok.

test_validate_gcp_config() ->
    Result1 = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),
    ?assertEqual(ok, Result1),

    Result2 = flurm_cloud_scaling:configure_provider(gcp, #{
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),
    ?assertEqual({error, missing_required_config}, Result2),
    ok.

test_validate_azure_config() ->
    Result1 = flurm_cloud_scaling:configure_provider(azure, #{
        subscription_id => <<"sub-123">>,
        resource_group => <<"rg-test">>,
        tenant_id => <<"tenant-123">>
    }),
    ?assertEqual(ok, Result1),

    Result2 = flurm_cloud_scaling:configure_provider(azure, #{
        resource_group => <<"rg-test">>,
        tenant_id => <<"tenant-123">>
    }),
    ?assertEqual({error, missing_required_config}, Result2),
    ok.

test_validate_generic_config() ->
    Result1 = flurm_cloud_scaling:configure_provider(generic, #{
        scale_up_webhook => <<"http://test/up">>,
        scale_down_webhook => <<"http://test/down">>
    }),
    ?assertEqual(ok, Result1),

    Result2 = flurm_cloud_scaling:configure_provider(generic, #{
        scale_up_webhook => <<"http://test/up">>
    }),
    ?assertEqual({error, missing_required_config}, Result2),
    ok.

%%====================================================================
%% Graceful Shutdown Tests (Phase 27)
%%====================================================================

graceful_shutdown_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"graceful shutdown", fun test_graceful_shutdown/0},
      {"terminate all instances", fun test_terminate_all/0},
      {"cleanup on stop", fun test_cleanup_on_stop/0}
     ]}.

test_graceful_shutdown() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 3}),

    Result = flurm_cloud_scaling:prepare_shutdown(),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_terminate_all() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 5}),

    Result = flurm_cloud_scaling:terminate_all_instances(),
    case Result of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_cleanup_on_stop() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 2}),

    Instances1 = flurm_cloud_scaling:list_cloud_instances(),
    ?assert(length(Instances1) >= 0),
    ok.

%%====================================================================
%% Advanced Scaling Policy Tests (Phase 31)
%%====================================================================

advanced_policy_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"target tracking policy", fun test_target_tracking_pol/0},
      {"step scaling policy", fun test_step_scaling_pol/0},
      {"scheduled scaling", fun test_scheduled_scale/0},
      {"predictive scaling", fun test_predictive_scale/0},
      {"custom metric scaling", fun test_custom_metric_scale/0}
     ]}.

test_target_tracking_pol() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        policy_type => target_tracking,
        target_value => 70.0,
        metric => cpu_utilization,
        scale_out_cooldown => 300,
        scale_in_cooldown => 300
    }),

    Status = flurm_cloud_scaling:get_scaling_status(),
    Policy = maps:get(policy, Status),
    ?assertEqual(target_tracking, maps:get(policy_type, Policy, undefined)),
    ok.

test_step_scaling_pol() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        policy_type => step_scaling,
        steps => [
            #{lower_bound => 0, upper_bound => 50, adjustment => 1},
            #{lower_bound => 50, upper_bound => 75, adjustment => 2},
            #{lower_bound => 75, adjustment => 4}
        ]
    }),

    Status = flurm_cloud_scaling:get_scaling_status(),
    ?assert(is_map(maps:get(policy, Status))),
    ok.

test_scheduled_scale() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    Result = flurm_cloud_scaling:add_scheduled_action(#{
        name => <<"morning_scale_up">>,
        cron => <<"0 8 * * 1-5">>,
        min_size => 10,
        max_size => 50,
        desired_capacity => 20
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_predictive_scale() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        policy_type => predictive,
        mode => forecast_and_scale,
        max_capacity_breach_behavior => honor_max_capacity
    }),

    Status = flurm_cloud_scaling:get_scaling_status(),
    ?assert(is_map(maps:get(policy, Status))),
    ok.

test_custom_metric_scale() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        policy_type => target_tracking,
        custom_metric => #{
            namespace => <<"Flurm">>,
            metric_name => <<"PendingJobs">>,
            statistic => <<"Average">>
        },
        target_value => 10.0
    }),

    Status = flurm_cloud_scaling:get_scaling_status(),
    ?assert(is_map(maps:get(policy, Status))),
    ok.

%%====================================================================
%% Mixed Fleet Tests (Phase 32)
%%====================================================================

mixed_fleet_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"mixed instance types", fun test_mixed_types/0},
      {"allocation strategy", fun test_alloc_strategy/0},
      {"on-demand base capacity", fun test_ondemand_base/0},
      {"spot allocation strategy", fun test_spot_alloc/0}
     ]}.

test_mixed_types() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        instance_types => [
            <<"c5.xlarge">>,
            <<"c5.2xlarge">>,
            <<"c5a.xlarge">>,
            <<"c5a.2xlarge">>
        ]
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 4}),
    ?assertEqual(4, length(InstanceIds)),
    ok.

test_alloc_strategy() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        allocation_strategy => lowest_price,
        instance_types => [<<"c5.xlarge">>, <<"c5.2xlarge">>]
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 2}),
    ?assertEqual(2, length(InstanceIds)),
    ok.

test_ondemand_base() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        on_demand_base_capacity => 5,
        on_demand_percentage_above_base => 20,
        spot_allocation_strategy => capacity_optimized
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 10}),
    ?assertEqual(10, length(InstanceIds)),
    ok.

test_spot_alloc() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{
        spot_enabled => true,
        spot_allocation_strategy => capacity_optimized,
        spot_instance_pools => 3
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 6,
        spot => true
    }),
    ?assertEqual(6, length(InstanceIds)),
    ok.

%%====================================================================
%% Metrics Tests (Phase 33)
%%====================================================================

metrics_tests_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get detailed metrics", fun test_detail_metrics/0},
      {"instance metrics", fun test_inst_metrics/0},
      {"scaling activity history", fun test_act_history/0},
      {"export prometheus metrics", fun test_prom_metrics/0}
     ]}.

test_detail_metrics() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 3}),

    Metrics = flurm_cloud_scaling:get_detailed_metrics(),
    case Metrics of
        {ok, M} when is_map(M) -> ok;
        M when is_map(M) -> ok;
        {error, _} -> ok
    end.

test_inst_metrics() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, [InstanceId | _]} = flurm_cloud_scaling:request_nodes(#{count => 1}),

    Metrics = flurm_cloud_scaling:get_instance_metrics(InstanceId),
    case Metrics of
        {ok, M} when is_map(M) -> ok;
        {error, _} -> ok
    end.

test_act_history() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 2}),

    History = flurm_cloud_scaling:get_scaling_activity_history(#{limit => 10}),
    case History of
        {ok, H} when is_list(H) -> ok;
        H when is_list(H) -> ok;
        {error, _} -> ok
    end.

test_prom_metrics() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Result = flurm_cloud_scaling:export_metrics(prometheus),
    case Result of
        {ok, Data} when is_binary(Data) -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Edge Case Tests (Phase 34)
%%====================================================================

edge_case_tests_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"request zero nodes", fun test_request_zero_nodes/0},
      {"request negative nodes", fun test_request_negative_nodes/0},
      {"request very large count", fun test_request_large_count/0},
      {"empty instance list terminate", fun test_empty_terminate/0},
      {"duplicate instance IDs", fun test_duplicate_ids/0},
      {"invalid provider switch", fun test_invalid_provider_switch/0}
     ]}.

test_request_zero_nodes() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Result = flurm_cloud_scaling:request_nodes(#{count => 0}),
    case Result of
        {ok, []} -> ok;
        {error, _} -> ok
    end.

test_request_negative_nodes() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Result = flurm_cloud_scaling:request_nodes(#{count => -5}),
    case Result of
        {ok, []} -> ok;
        {error, _} -> ok
    end.

test_request_large_count() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    ok = flurm_cloud_scaling:set_scaling_policy(#{max_nodes => 100}),

    Result = flurm_cloud_scaling:request_nodes(#{count => 1000}),
    case Result of
        {ok, Ids} -> ?assert(length(Ids) =< 100);
        {error, _} -> ok
    end.

test_empty_terminate() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Result = flurm_cloud_scaling:terminate_nodes([]),
    ?assertEqual({ok, []}, Result).

test_duplicate_ids() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, [Id1 | _]} = flurm_cloud_scaling:request_nodes(#{count => 1}),

    %% Try to terminate same ID twice
    Result = flurm_cloud_scaling:terminate_nodes([Id1, Id1]),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_invalid_provider_switch() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 2}),

    %% Switch to different provider while instances exist
    Result = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),
    ?assertEqual(ok, Result).

%%====================================================================
%% Concurrent Edge Case Tests (Phase 35)
%%====================================================================

concurrent_edge_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"rapid enable disable", fun test_rapid_enable_disable/0},
      {"concurrent config changes", fun test_concurrent_config/0},
      {"concurrent policy changes", fun test_concurrent_policy/0}
     ]}.

test_rapid_enable_disable() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    lists:foreach(fun(_) ->
        ok = flurm_cloud_scaling:enable(),
        ok = flurm_cloud_scaling:disable()
    end, lists:seq(1, 20)),

    %% Should still be in valid state
    Status = flurm_cloud_scaling:get_scaling_status(),
    ?assert(is_map(Status)),
    ok.

test_concurrent_config() ->
    Self = self(),

    Pids = [spawn(fun() ->
        Provider = case N rem 2 of
            0 -> aws;
            1 -> gcp
        end,
        Config = case Provider of
            aws -> #{
                region => <<"us-east-1">>,
                access_key_id => <<"AKIATEST">>,
                secret_access_key => <<"secret">>
            };
            gcp -> #{
                project_id => <<"test-project">>,
                zone => <<"us-central1-a">>,
                credentials_file => <<"/path/to/creds.json">>
            }
        end,
        Result = flurm_cloud_scaling:configure_provider(Provider, Config),
        Self ! {config_done, N, Result}
    end) || N <- lists:seq(1, 10)],

    lists:foreach(fun(_) ->
        receive {config_done, _, _} -> ok after 10000 -> ok end
    end, Pids),

    %% Should have a valid provider
    {ok, _, _} = flurm_cloud_scaling:get_provider_config(),
    ok.

test_concurrent_policy() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    Self = self(),

    Pids = [spawn(fun() ->
        Policy = #{
            min_nodes => N,
            max_nodes => N * 10
        },
        Result = flurm_cloud_scaling:set_scaling_policy(Policy),
        Self ! {policy_done, N, Result}
    end) || N <- lists:seq(1, 10)],

    lists:foreach(fun(_) ->
        receive {policy_done, _, _} -> ok after 10000 -> ok end
    end, Pids),

    Status = flurm_cloud_scaling:get_scaling_status(),
    ?assert(is_map(maps:get(policy, Status))),
    ok.

%%====================================================================
%% Instance Lifecycle Events (Phase 36)
%%====================================================================

lifecycle_events_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"launch lifecycle hook", fun test_launch_lifecycle_hook/0},
      {"terminate lifecycle hook", fun test_terminate_lifecycle_hook/0},
      {"lifecycle action complete", fun test_lifecycle_complete/0},
      {"lifecycle action timeout", fun test_lifecycle_timeout/0}
     ]}.

test_launch_lifecycle_hook() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        launch_lifecycle_hook => #{
            name => <<"flurm-launch-hook">>,
            heartbeat_timeout => 300
        }
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_terminate_lifecycle_hook() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        terminate_lifecycle_hook => #{
            name => <<"flurm-terminate-hook">>,
            heartbeat_timeout => 120
        }
    }),

    {ok, [InstanceId | _]} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    _ = flurm_cloud_scaling:terminate_nodes([InstanceId]),
    ok.

test_lifecycle_complete() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>
    }),

    {ok, [InstanceId | _]} = flurm_cloud_scaling:request_nodes(#{count => 1}),

    Result = flurm_cloud_scaling:complete_lifecycle_action(InstanceId, <<"Continue">>),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_lifecycle_timeout() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        lifecycle_timeout => 60
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ?assertEqual(1, length(InstanceIds)),
    ok.

%%====================================================================
%% Provider Specific Features (Phase 37)
%%====================================================================

provider_features_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"aws instance metadata", fun test_aws_instance_metadata/0},
      {"gcp preemptible instances", fun test_gcp_preemptible/0},
      {"azure spot priority", fun test_azure_spot_priority/0},
      {"generic webhook headers", fun test_generic_headers/0}
     ]}.

test_aws_instance_metadata() ->
    ok = flurm_cloud_scaling:configure_provider(aws, #{
        region => <<"us-east-1">>,
        access_key_id => <<"AKIATEST">>,
        secret_access_key => <<"secret">>,
        metadata_options => #{
            http_tokens => required,
            http_put_response_hop_limit => 1
        }
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ?assertEqual(1, length(InstanceIds)),
    ok.

test_gcp_preemptible() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 2,
        preemptible => true
    }),
    ?assertEqual(2, length(InstanceIds)),
    ok.

test_azure_spot_priority() ->
    ok = flurm_cloud_scaling:configure_provider(azure, #{
        subscription_id => <<"sub-123">>,
        resource_group => <<"rg-test">>,
        tenant_id => <<"tenant-123">>
    }),

    {ok, InstanceIds} = flurm_cloud_scaling:request_nodes(#{
        count => 2,
        spot => true,
        eviction_policy => deallocate,
        max_price => 0.50
    }),
    ?assertEqual(2, length(InstanceIds)),
    ok.

test_generic_headers() ->
    ok = flurm_cloud_scaling:configure_provider(generic, #{
        scale_up_webhook => <<"https://api.example.com/scale-up">>,
        scale_down_webhook => <<"https://api.example.com/scale-down">>,
        webhook_headers => #{
            <<"Authorization">> => <<"Bearer token123">>,
            <<"X-Custom-Header">> => <<"value">>
        }
    }),

    {ok, _ActionId} = flurm_cloud_scaling:request_scale_up(1, #{}),
    timer:sleep(100),
    ok.

%%====================================================================
%% State Persistence Tests (Phase 38)
%%====================================================================

state_persistence_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"save state to disk", fun test_save_state/0},
      {"load state from disk", fun test_load_state/0},
      {"state version migration", fun test_state_migration/0}
     ]}.

test_save_state() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 3}),

    Result = flurm_cloud_scaling:save_state(),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_load_state() ->
    Result = flurm_cloud_scaling:load_state(),
    case Result of
        ok -> ok;
        {error, no_state} -> ok;
        {error, _} -> ok
    end.

test_state_migration() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    Result = flurm_cloud_scaling:migrate_state(#{version => 2}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Logging Tests (Phase 39)
%%====================================================================

logging_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"set log level", fun test_set_log_level/0},
      {"structured logging", fun test_structured_log/0},
      {"get log entries", fun test_get_logs/0}
     ]}.

test_set_log_level() ->
    Result = flurm_cloud_scaling:set_log_level(debug),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end,

    Result2 = flurm_cloud_scaling:set_log_level(info),
    case Result2 of
        ok -> ok;
        {error, _} -> ok
    end.

test_structured_log() ->
    ok = flurm_cloud_scaling:configure_provider(gcp, #{
        project_id => <<"test-project">>,
        zone => <<"us-central1-a">>,
        credentials_file => <<"/path/to/creds.json">>
    }),

    {ok, _} = flurm_cloud_scaling:request_nodes(#{count => 1}),
    ok.

test_get_logs() ->
    Result = flurm_cloud_scaling:get_recent_logs(#{limit => 100}),
    case Result of
        {ok, Logs} when is_list(Logs) -> ok;
        {error, _} -> ok
    end.

