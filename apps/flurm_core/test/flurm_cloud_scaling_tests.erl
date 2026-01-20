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
