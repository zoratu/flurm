%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_cloud_scaling
%%%
%%% These tests directly test the gen_server callbacks (init/1, handle_call/3,
%%% handle_cast/2, handle_info/2) without any mocking.
%%%
%%% NO MECK USED - pure unit tests only.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaling_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% Record definitions copied from source for testing
-record(scaling_policy, {
    min_nodes :: non_neg_integer(),
    max_nodes :: non_neg_integer(),
    target_pending_jobs :: non_neg_integer(),
    target_idle_nodes :: non_neg_integer(),
    scale_up_increment :: pos_integer(),
    scale_down_increment :: pos_integer(),
    cooldown_seconds :: pos_integer(),
    instance_types :: [binary()],
    spot_enabled :: boolean(),
    spot_max_price :: float(),
    idle_threshold_seconds :: pos_integer(),
    queue_depth_threshold :: pos_integer()
}).

-record(scaling_action, {
    id :: binary(),
    type :: scale_up | scale_down,
    requested_count :: pos_integer(),
    actual_count :: non_neg_integer(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer() | undefined,
    status :: pending | in_progress | completed | failed,
    error :: binary() | undefined,
    instance_ids :: [binary()]
}).

-record(cloud_instance, {
    instance_id :: binary(),
    provider :: atom(),
    instance_type :: binary(),
    launch_time :: non_neg_integer(),
    state :: pending | running | stopping | terminated,
    public_ip :: binary() | undefined,
    private_ip :: binary() | undefined,
    node_name :: binary() | undefined,
    last_job_time :: non_neg_integer(),
    hourly_cost :: float()
}).

-record(cost_tracking, {
    total_instance_hours :: float(),
    total_cost :: float(),
    budget_limit :: float(),
    budget_alert_threshold :: float(),
    cost_per_hour :: map(),
    daily_costs :: map(),
    monthly_costs :: map()
}).

-record(state, {
    enabled :: boolean(),
    provider :: atom() | undefined,
    provider_config :: map(),
    policy :: #scaling_policy{},
    current_cloud_nodes :: non_neg_integer(),
    pending_actions :: [#scaling_action{}],
    action_history :: [#scaling_action{}],
    last_scale_time :: non_neg_integer(),
    check_timer :: reference() | undefined,
    idle_check_timer :: reference() | undefined,
    cloud_instances :: map(),
    cost_tracking :: #cost_tracking{}
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Create a default test state
default_test_state() ->
    DefaultPolicy = #scaling_policy{
        min_nodes = 0,
        max_nodes = 100,
        target_pending_jobs = 10,
        target_idle_nodes = 2,
        scale_up_increment = 5,
        scale_down_increment = 1,
        cooldown_seconds = 300,
        instance_types = [<<"c5.xlarge">>],
        spot_enabled = false,
        spot_max_price = 0.0,
        idle_threshold_seconds = 300,
        queue_depth_threshold = 10
    },
    DefaultCostTracking = #cost_tracking{
        total_instance_hours = 0.0,
        total_cost = 0.0,
        budget_limit = 0.0,
        budget_alert_threshold = 0.8,
        cost_per_hour = #{
            <<"c5.xlarge">> => 0.17,
            <<"t3.medium">> => 0.0416
        },
        daily_costs = #{},
        monthly_costs = #{}
    },
    #state{
        enabled = false,
        provider = undefined,
        provider_config = #{},
        policy = DefaultPolicy,
        current_cloud_nodes = 0,
        pending_actions = [],
        action_history = [],
        last_scale_time = 0,
        check_timer = undefined,
        idle_check_timer = undefined,
        cloud_instances = #{},
        cost_tracking = DefaultCostTracking
    }.

%% Create a state with AWS provider configured
aws_configured_state() ->
    State = default_test_state(),
    State#state{
        provider = aws,
        provider_config = #{
            region => <<"us-east-1">>,
            access_key_id => <<"test-key">>,
            secret_access_key => <<"test-secret">>
        }
    }.

%% Create a state with cloud instances
state_with_instances() ->
    State = aws_configured_state(),
    Now = erlang:system_time(second),
    Instances = #{
        <<"i-12345">> => #cloud_instance{
            instance_id = <<"i-12345">>,
            provider = aws,
            instance_type = <<"c5.xlarge">>,
            launch_time = Now - 3600,
            state = running,
            public_ip = <<"1.2.3.4">>,
            private_ip = <<"10.0.0.1">>,
            node_name = <<"node1@host">>,
            last_job_time = Now - 100,
            hourly_cost = 0.17
        },
        <<"i-67890">> => #cloud_instance{
            instance_id = <<"i-67890">>,
            provider = aws,
            instance_type = <<"c5.xlarge">>,
            launch_time = Now - 7200,
            state = running,
            public_ip = <<"1.2.3.5">>,
            private_ip = <<"10.0.0.2">>,
            node_name = <<"node2@host">>,
            last_job_time = Now - 1000,  % Idle for longer
            hourly_cost = 0.17
        }
    },
    State#state{
        cloud_instances = Instances,
        current_cloud_nodes = 2
    }.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {"init/1 tests", [
        {"initializes with default state",
         fun() ->
             {ok, State} = flurm_cloud_scaling:init([]),
             ?assertEqual(false, State#state.enabled),
             ?assertEqual(undefined, State#state.provider),
             ?assertEqual(#{}, State#state.provider_config),
             ?assertEqual(0, State#state.current_cloud_nodes),
             ?assertEqual([], State#state.pending_actions),
             ?assertEqual([], State#state.action_history)
         end},

        {"initializes with correct default policy",
         fun() ->
             {ok, State} = flurm_cloud_scaling:init([]),
             Policy = State#state.policy,
             ?assertEqual(0, Policy#scaling_policy.min_nodes),
             ?assertEqual(100, Policy#scaling_policy.max_nodes),
             ?assertEqual(10, Policy#scaling_policy.target_pending_jobs),
             ?assertEqual(2, Policy#scaling_policy.target_idle_nodes),
             ?assertEqual(5, Policy#scaling_policy.scale_up_increment),
             ?assertEqual(1, Policy#scaling_policy.scale_down_increment),
             ?assertEqual(300, Policy#scaling_policy.cooldown_seconds),
             ?assertEqual([<<"c5.xlarge">>], Policy#scaling_policy.instance_types),
             ?assertEqual(false, Policy#scaling_policy.spot_enabled)
         end},

        {"initializes with cost tracking",
         fun() ->
             {ok, State} = flurm_cloud_scaling:init([]),
             CostTracking = State#state.cost_tracking,
             ?assertEqual(0.0, CostTracking#cost_tracking.total_instance_hours),
             ?assertEqual(0.0, CostTracking#cost_tracking.total_cost),
             ?assertEqual(0.0, CostTracking#cost_tracking.budget_limit),
             ?assertEqual(0.8, CostTracking#cost_tracking.budget_alert_threshold)
         end},

        {"initializes cloud instances as empty map",
         fun() ->
             {ok, State} = flurm_cloud_scaling:init([]),
             ?assertEqual(#{}, State#state.cloud_instances)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Provider Configuration Tests
%%====================================================================

configure_provider_test_() ->
    {"configure_provider tests", [
        {"configures AWS provider with valid config",
         fun() ->
             State = default_test_state(),
             Config = #{
                 region => <<"us-east-1">>,
                 access_key_id => <<"test-key">>,
                 secret_access_key => <<"test-secret">>
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {configure_provider, aws, Config}, {self(), make_ref()}, State),
             ?assertEqual(aws, NewState#state.provider),
             ?assertEqual(Config, NewState#state.provider_config)
         end},

        {"configures GCP provider with valid config",
         fun() ->
             State = default_test_state(),
             Config = #{
                 project_id => <<"my-project">>,
                 zone => <<"us-central1-a">>,
                 credentials_file => <<"/path/to/creds.json">>
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {configure_provider, gcp, Config}, {self(), make_ref()}, State),
             ?assertEqual(gcp, NewState#state.provider),
             ?assertEqual(Config, NewState#state.provider_config)
         end},

        {"configures Azure provider with valid config",
         fun() ->
             State = default_test_state(),
             Config = #{
                 subscription_id => <<"sub-123">>,
                 resource_group => <<"my-rg">>,
                 tenant_id => <<"tenant-456">>
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {configure_provider, azure, Config}, {self(), make_ref()}, State),
             ?assertEqual(azure, NewState#state.provider),
             ?assertEqual(Config, NewState#state.provider_config)
         end},

        {"configures generic provider with valid config",
         fun() ->
             State = default_test_state(),
             Config = #{
                 scale_up_webhook => <<"https://api.example.com/scale-up">>,
                 scale_down_webhook => <<"https://api.example.com/scale-down">>
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {configure_provider, generic, Config}, {self(), make_ref()}, State),
             ?assertEqual(generic, NewState#state.provider),
             ?assertEqual(Config, NewState#state.provider_config)
         end},

        {"rejects AWS config missing region",
         fun() ->
             State = default_test_state(),
             Config = #{
                 access_key_id => <<"test-key">>,
                 secret_access_key => <<"test-secret">>
             },
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, aws, Config}, {self(), make_ref()}, State)
         end},

        {"rejects AWS config missing access_key_id",
         fun() ->
             State = default_test_state(),
             Config = #{
                 region => <<"us-east-1">>,
                 secret_access_key => <<"test-secret">>
             },
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, aws, Config}, {self(), make_ref()}, State)
         end},

        {"rejects GCP config missing project_id",
         fun() ->
             State = default_test_state(),
             Config = #{
                 zone => <<"us-central1-a">>,
                 credentials_file => <<"/path/to/creds.json">>
             },
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, gcp, Config}, {self(), make_ref()}, State)
         end},

        {"rejects Azure config missing subscription_id",
         fun() ->
             State = default_test_state(),
             Config = #{
                 resource_group => <<"my-rg">>,
                 tenant_id => <<"tenant-456">>
             },
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, azure, Config}, {self(), make_ref()}, State)
         end},

        {"rejects generic config missing webhooks",
         fun() ->
             State = default_test_state(),
             Config = #{scale_up_webhook => <<"https://api.example.com/scale-up">>},
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, generic, Config}, {self(), make_ref()}, State)
         end},

        {"rejects unknown provider",
         fun() ->
             State = default_test_state(),
             {reply, {error, unknown_provider}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, openstack, #{}}, {self(), make_ref()}, State)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Get Provider Config Tests
%%====================================================================

get_provider_config_test_() ->
    {"get_provider_config tests", [
        {"returns error when provider not configured",
         fun() ->
             State = default_test_state(),
             {reply, {error, not_configured}, _} = flurm_cloud_scaling:handle_call(
                 get_provider_config, {self(), make_ref()}, State)
         end},

        {"returns provider and config when configured",
         fun() ->
             State = aws_configured_state(),
             {reply, {ok, aws, Config}, _} = flurm_cloud_scaling:handle_call(
                 get_provider_config, {self(), make_ref()}, State),
             ?assertEqual(<<"us-east-1">>, maps:get(region, Config))
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Scale Up Tests
%%====================================================================

scale_up_test_() ->
    {"scale_up tests", [
        {"returns error when provider not configured",
         fun() ->
             State = default_test_state(),
             {reply, {error, provider_not_configured}, _} = flurm_cloud_scaling:handle_call(
                 {scale_up, 5, #{}}, {self(), make_ref()}, State)
         end},

        {"returns error at max capacity",
         fun() ->
             State = aws_configured_state(),
             Policy = State#state.policy,
             StateAtMax = State#state{
                 current_cloud_nodes = Policy#scaling_policy.max_nodes
             },
             {reply, {error, at_max_capacity}, _} = flurm_cloud_scaling:handle_call(
                 {scale_up, 5, #{}}, {self(), make_ref()}, StateAtMax)
         end},

        {"initiates scale up and returns action ID",
         fun() ->
             %% Trap exits to handle spawned linked process
             process_flag(trap_exit, true),
             State = aws_configured_state(),
             {reply, {ok, ActionId}, NewState} = flurm_cloud_scaling:handle_call(
                 {scale_up, 3, #{}}, {self(), make_ref()}, State),
             ?assert(is_binary(ActionId)),
             ?assert(binary:match(ActionId, <<"scale-">>) =/= nomatch),
             ?assertEqual(1, length(NewState#state.pending_actions)),
             [Action] = NewState#state.pending_actions,
             ?assertEqual(ActionId, Action#scaling_action.id),
             ?assertEqual(scale_up, Action#scaling_action.type),
             ?assertEqual(3, Action#scaling_action.requested_count),
             ?assertEqual(in_progress, Action#scaling_action.status),
             %% Clean up any exit messages from spawned process
             receive {'EXIT', _, _} -> ok after 100 -> ok end,
             process_flag(trap_exit, false)
         end},

        {"limits scale up to max_nodes",
         fun() ->
             %% Trap exits to handle spawned linked process
             process_flag(trap_exit, true),
             State = aws_configured_state(),
             Policy = State#state.policy,
             StateNearMax = State#state{
                 current_cloud_nodes = Policy#scaling_policy.max_nodes - 2
             },
             {reply, {ok, _ActionId}, NewState} = flurm_cloud_scaling:handle_call(
                 {scale_up, 10, #{}}, {self(), make_ref()}, StateNearMax),
             [Action] = NewState#state.pending_actions,
             ?assertEqual(10, Action#scaling_action.requested_count),
             ?assertEqual(2, Action#scaling_action.actual_count),  % Limited to 2
             %% Clean up any exit messages from spawned process
             receive {'EXIT', _, _} -> ok after 100 -> ok end,
             process_flag(trap_exit, false)
         end},

        {"updates last_scale_time",
         fun() ->
             %% Trap exits to handle spawned linked process
             process_flag(trap_exit, true),
             State = aws_configured_state(),
             Before = erlang:system_time(second),
             {reply, {ok, _}, NewState} = flurm_cloud_scaling:handle_call(
                 {scale_up, 2, #{}}, {self(), make_ref()}, State),
             After = erlang:system_time(second),
             ?assert(NewState#state.last_scale_time >= Before),
             ?assert(NewState#state.last_scale_time =< After),
             %% Clean up any exit messages from spawned process
             receive {'EXIT', _, _} -> ok after 100 -> ok end,
             process_flag(trap_exit, false)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Scale Down Tests
%%====================================================================

scale_down_test_() ->
    {"scale_down tests", [
        {"returns error when provider not configured",
         fun() ->
             State = default_test_state(),
             {reply, {error, provider_not_configured}, _} = flurm_cloud_scaling:handle_call(
                 {scale_down, 2, #{}}, {self(), make_ref()}, State)
         end},

        {"returns error at min capacity",
         fun() ->
             State = aws_configured_state(),
             Policy = State#state.policy,
             StateAtMin = State#state{
                 current_cloud_nodes = Policy#scaling_policy.min_nodes
             },
             {reply, {error, at_min_capacity}, _} = flurm_cloud_scaling:handle_call(
                 {scale_down, 2, #{}}, {self(), make_ref()}, StateAtMin)
         end},

        {"initiates scale down and returns action ID",
         fun() ->
             %% Trap exits to handle spawned linked process
             process_flag(trap_exit, true),
             State = aws_configured_state(),
             StateWithNodes = State#state{current_cloud_nodes = 10},
             {reply, {ok, ActionId}, NewState} = flurm_cloud_scaling:handle_call(
                 {scale_down, 2, #{}}, {self(), make_ref()}, StateWithNodes),
             ?assert(is_binary(ActionId)),
             ?assertEqual(1, length(NewState#state.pending_actions)),
             [Action] = NewState#state.pending_actions,
             ?assertEqual(scale_down, Action#scaling_action.type),
             ?assertEqual(2, Action#scaling_action.requested_count),
             %% Clean up any exit messages from spawned process
             receive {'EXIT', _, _} -> ok after 100 -> ok end,
             process_flag(trap_exit, false)
         end},

        {"limits scale down to min_nodes",
         fun() ->
             %% Trap exits to handle spawned linked process
             process_flag(trap_exit, true),
             State = aws_configured_state(),
             Policy = State#state.policy,
             NewPolicy = Policy#scaling_policy{min_nodes = 3},
             StateNearMin = State#state{
                 policy = NewPolicy,
                 current_cloud_nodes = 5
             },
             {reply, {ok, _}, NewState} = flurm_cloud_scaling:handle_call(
                 {scale_down, 10, #{}}, {self(), make_ref()}, StateNearMin),
             [Action] = NewState#state.pending_actions,
             ?assertEqual(10, Action#scaling_action.requested_count),
             ?assertEqual(2, Action#scaling_action.actual_count),  % Limited to 2
             %% Clean up any exit messages from spawned process
             receive {'EXIT', _, _} -> ok after 100 -> ok end,
             process_flag(trap_exit, false)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Get Scaling Status Tests
%%====================================================================

get_scaling_status_test_() ->
    {"get_scaling_status tests", [
        {"returns correct status for unconfigured state",
         fun() ->
             State = default_test_state(),
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_scaling_status, {self(), make_ref()}, State),
             ?assertEqual(false, maps:get(enabled, Status)),
             ?assertEqual(undefined, maps:get(provider, Status)),
             ?assertEqual(0, maps:get(current_cloud_nodes, Status)),
             ?assertEqual(0, maps:get(pending_actions, Status)),
             ?assert(maps:is_key(policy, Status))
         end},

        {"returns correct status for configured state",
         fun() ->
             State = aws_configured_state(),
             EnabledState = State#state{
                 enabled = true,
                 current_cloud_nodes = 5,
                 last_scale_time = 1234567890
             },
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_scaling_status, {self(), make_ref()}, EnabledState),
             ?assertEqual(true, maps:get(enabled, Status)),
             ?assertEqual(aws, maps:get(provider, Status)),
             ?assertEqual(5, maps:get(current_cloud_nodes, Status)),
             ?assertEqual(1234567890, maps:get(last_scale_time, Status))
         end},

        {"includes formatted policy in status",
         fun() ->
             State = aws_configured_state(),
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_scaling_status, {self(), make_ref()}, State),
             Policy = maps:get(policy, Status),
             ?assert(is_map(Policy)),
             ?assertEqual(0, maps:get(min_nodes, Policy)),
             ?assertEqual(100, maps:get(max_nodes, Policy)),
             ?assertEqual(10, maps:get(target_pending_jobs, Policy))
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Set Policy Tests
%%====================================================================

set_policy_test_() ->
    {"set_policy tests", [
        {"updates min_nodes",
         fun() ->
             State = default_test_state(),
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_policy, #{min_nodes => 5}}, {self(), make_ref()}, State),
             ?assertEqual(5, NewState#state.policy#scaling_policy.min_nodes)
         end},

        {"updates max_nodes",
         fun() ->
             State = default_test_state(),
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_policy, #{max_nodes => 50}}, {self(), make_ref()}, State),
             ?assertEqual(50, NewState#state.policy#scaling_policy.max_nodes)
         end},

        {"updates multiple fields at once",
         fun() ->
             State = default_test_state(),
             PolicyMap = #{
                 min_nodes => 2,
                 max_nodes => 20,
                 target_pending_jobs => 5,
                 scale_up_increment => 3,
                 cooldown_seconds => 600
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_policy, PolicyMap}, {self(), make_ref()}, State),
             Policy = NewState#state.policy,
             ?assertEqual(2, Policy#scaling_policy.min_nodes),
             ?assertEqual(20, Policy#scaling_policy.max_nodes),
             ?assertEqual(5, Policy#scaling_policy.target_pending_jobs),
             ?assertEqual(3, Policy#scaling_policy.scale_up_increment),
             ?assertEqual(600, Policy#scaling_policy.cooldown_seconds)
         end},

        {"updates spot settings",
         fun() ->
             State = default_test_state(),
             PolicyMap = #{
                 spot_enabled => true,
                 spot_max_price => 0.05
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_policy, PolicyMap}, {self(), make_ref()}, State),
             Policy = NewState#state.policy,
             ?assertEqual(true, Policy#scaling_policy.spot_enabled),
             ?assertEqual(0.05, Policy#scaling_policy.spot_max_price)
         end},

        {"updates instance_types",
         fun() ->
             State = default_test_state(),
             PolicyMap = #{instance_types => [<<"m5.large">>, <<"m5.xlarge">>]},
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_policy, PolicyMap}, {self(), make_ref()}, State),
             ?assertEqual([<<"m5.large">>, <<"m5.xlarge">>],
                          NewState#state.policy#scaling_policy.instance_types)
         end},

        {"preserves unspecified fields",
         fun() ->
             State = default_test_state(),
             OriginalPolicy = State#state.policy,
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_policy, #{min_nodes => 5}}, {self(), make_ref()}, State),
             NewPolicy = NewState#state.policy,
             %% Check unchanged fields
             ?assertEqual(OriginalPolicy#scaling_policy.max_nodes,
                          NewPolicy#scaling_policy.max_nodes),
             ?assertEqual(OriginalPolicy#scaling_policy.cooldown_seconds,
                          NewPolicy#scaling_policy.cooldown_seconds)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Enable/Disable Tests
%%====================================================================

enable_disable_test_() ->
    {"enable/disable tests", [
        {"enable sets enabled to true and starts timers",
         fun() ->
             State = default_test_state(),
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 enable, {self(), make_ref()}, State),
             ?assertEqual(true, NewState#state.enabled),
             ?assert(NewState#state.check_timer =/= undefined),
             ?assert(NewState#state.idle_check_timer =/= undefined),
             %% Clean up timers
             erlang:cancel_timer(NewState#state.check_timer),
             erlang:cancel_timer(NewState#state.idle_check_timer)
         end},

        {"disable sets enabled to false and cancels timers",
         fun() ->
             State = default_test_state(),
             Timer1 = erlang:send_after(60000, self(), test),
             Timer2 = erlang:send_after(60000, self(), test2),
             EnabledState = State#state{
                 enabled = true,
                 check_timer = Timer1,
                 idle_check_timer = Timer2
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 disable, {self(), make_ref()}, EnabledState),
             ?assertEqual(false, NewState#state.enabled),
             ?assertEqual(undefined, NewState#state.check_timer),
             ?assertEqual(undefined, NewState#state.idle_check_timer)
         end},

        {"disable handles undefined timers gracefully",
         fun() ->
             State = default_test_state(),
             EnabledState = State#state{enabled = true},
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 disable, {self(), make_ref()}, EnabledState),
             ?assertEqual(false, NewState#state.enabled)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - is_enabled Tests
%%====================================================================

is_enabled_test_() ->
    {"is_enabled tests", [
        {"returns false when disabled",
         fun() ->
             State = default_test_state(),
             {reply, false, _} = flurm_cloud_scaling:handle_call(
                 is_enabled, {self(), make_ref()}, State)
         end},

        {"returns true when enabled",
         fun() ->
             State = default_test_state(),
             EnabledState = State#state{enabled = true},
             {reply, true, _} = flurm_cloud_scaling:handle_call(
                 is_enabled, {self(), make_ref()}, EnabledState)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Get Stats Tests
%%====================================================================

get_stats_test_() ->
    {"get_stats tests", [
        {"returns stats for empty state",
         fun() ->
             State = default_test_state(),
             {reply, Stats, _} = flurm_cloud_scaling:handle_call(
                 get_stats, {self(), make_ref()}, State),
             ?assertEqual(false, maps:get(enabled, Stats)),
             ?assertEqual(undefined, maps:get(provider, Stats)),
             ?assertEqual(0, maps:get(current_cloud_nodes, Stats)),
             ?assertEqual(0, maps:get(pending_actions, Stats)),
             ?assertEqual(0, maps:get(completed_scale_ups, Stats)),
             ?assertEqual(0, maps:get(completed_scale_downs, Stats)),
             ?assertEqual(0, maps:get(failed_actions, Stats)),
             ?assertEqual(0, maps:get(total_instances_launched, Stats)),
             ?assertEqual(0, maps:get(total_instances_terminated, Stats))
         end},

        {"returns stats with action history",
         fun() ->
             State = aws_configured_state(),
             CompletedScaleUp = #scaling_action{
                 id = <<"action-1">>,
                 type = scale_up,
                 requested_count = 5,
                 actual_count = 5,
                 start_time = 1000,
                 end_time = 1100,
                 status = completed,
                 instance_ids = []
             },
             CompletedScaleDown = #scaling_action{
                 id = <<"action-2">>,
                 type = scale_down,
                 requested_count = 2,
                 actual_count = 2,
                 start_time = 2000,
                 end_time = 2100,
                 status = completed,
                 instance_ids = []
             },
             FailedAction = #scaling_action{
                 id = <<"action-3">>,
                 type = scale_up,
                 requested_count = 3,
                 actual_count = 0,
                 start_time = 3000,
                 end_time = 3100,
                 status = failed,
                 error = <<"test error">>,
                 instance_ids = []
             },
             StateWithHistory = State#state{
                 enabled = true,
                 current_cloud_nodes = 3,
                 action_history = [CompletedScaleUp, CompletedScaleDown, FailedAction]
             },
             {reply, Stats, _} = flurm_cloud_scaling:handle_call(
                 get_stats, {self(), make_ref()}, StateWithHistory),
             ?assertEqual(true, maps:get(enabled, Stats)),
             ?assertEqual(aws, maps:get(provider, Stats)),
             ?assertEqual(3, maps:get(current_cloud_nodes, Stats)),
             ?assertEqual(1, maps:get(completed_scale_ups, Stats)),
             ?assertEqual(1, maps:get(completed_scale_downs, Stats)),
             ?assertEqual(1, maps:get(failed_actions, Stats)),
             ?assertEqual(5, maps:get(total_instances_launched, Stats)),
             ?assertEqual(2, maps:get(total_instances_terminated, Stats))
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Request Nodes Tests
%%====================================================================

request_nodes_test_() ->
    {"request_nodes tests", [
        {"returns error when provider not configured",
         fun() ->
             State = default_test_state(),
             {reply, {error, provider_not_configured}, _} = flurm_cloud_scaling:handle_call(
                 {request_nodes, 2, #{}}, {self(), make_ref()}, State)
         end},

        {"returns error at max capacity",
         fun() ->
             State = aws_configured_state(),
             Policy = State#state.policy,
             StateAtMax = State#state{
                 current_cloud_nodes = Policy#scaling_policy.max_nodes
             },
             {reply, {error, at_max_capacity}, _} = flurm_cloud_scaling:handle_call(
                 {request_nodes, 2, #{}}, {self(), make_ref()}, StateAtMax)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Terminate Nodes Tests
%%====================================================================

terminate_nodes_test_() ->
    {"terminate_nodes tests", [
        {"returns error when provider not configured",
         fun() ->
             State = default_test_state(),
             StateWithInstances = State#state{
                 cloud_instances = #{<<"i-123">> => #cloud_instance{}},
                 current_cloud_nodes = 1
             },
             {reply, {error, provider_not_configured}, _} = flurm_cloud_scaling:handle_call(
                 {terminate_nodes, [<<"i-123">>]}, {self(), make_ref()}, StateWithInstances)
         end},

        {"returns error at min capacity with no instances",
         fun() ->
             State = aws_configured_state(),
             {reply, {error, at_min_capacity}, _} = flurm_cloud_scaling:handle_call(
                 {terminate_nodes, [<<"i-nonexistent">>]}, {self(), make_ref()}, State)
         end},

        {"handles empty instance list",
         fun() ->
             State = aws_configured_state(),
             {reply, {ok, []}, _} = flurm_cloud_scaling:handle_call(
                 {terminate_nodes, []}, {self(), make_ref()}, State)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - List Cloud Instances Tests
%%====================================================================

list_cloud_instances_test_() ->
    {"list_cloud_instances tests", [
        {"returns empty list when no instances",
         fun() ->
             State = default_test_state(),
             {reply, [], _} = flurm_cloud_scaling:handle_call(
                 list_cloud_instances, {self(), make_ref()}, State)
         end},

        {"returns formatted instances list",
         fun() ->
             State = state_with_instances(),
             {reply, Instances, _} = flurm_cloud_scaling:handle_call(
                 list_cloud_instances, {self(), make_ref()}, State),
             ?assertEqual(2, length(Instances)),
             [Instance1 | _] = Instances,
             ?assert(is_map(Instance1)),
             ?assert(maps:is_key(instance_id, Instance1)),
             ?assert(maps:is_key(provider, Instance1)),
             ?assert(maps:is_key(instance_type, Instance1)),
             ?assert(maps:is_key(state, Instance1)),
             ?assert(maps:is_key(hourly_cost, Instance1)),
             ?assert(maps:is_key(running_hours, Instance1))
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Get Instance Info Tests
%%====================================================================

get_instance_info_test_() ->
    {"get_instance_info tests", [
        {"returns error for non-existent instance",
         fun() ->
             State = default_test_state(),
             {reply, {error, not_found}, _} = flurm_cloud_scaling:handle_call(
                 {get_instance_info, <<"i-nonexistent">>}, {self(), make_ref()}, State)
         end},

        {"returns instance info for existing instance",
         fun() ->
             State = state_with_instances(),
             {reply, {ok, Info}, _} = flurm_cloud_scaling:handle_call(
                 {get_instance_info, <<"i-12345">>}, {self(), make_ref()}, State),
             ?assert(is_map(Info)),
             ?assertEqual(<<"i-12345">>, maps:get(instance_id, Info)),
             ?assertEqual(aws, maps:get(provider, Info)),
             ?assertEqual(<<"c5.xlarge">>, maps:get(instance_type, Info)),
             ?assertEqual(running, maps:get(state, Info)),
             ?assertEqual(<<"1.2.3.4">>, maps:get(public_ip, Info)),
             ?assertEqual(0.17, maps:get(hourly_cost, Info))
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Cost Summary Tests
%%====================================================================

get_cost_summary_test_() ->
    {"get_cost_summary tests", [
        {"returns cost summary for empty state",
         fun() ->
             State = default_test_state(),
             {reply, Summary, _} = flurm_cloud_scaling:handle_call(
                 get_cost_summary, {self(), make_ref()}, State),
             ?assert(is_map(Summary)),
             ?assertEqual(0.0, maps:get(total_instance_hours, Summary)),
             ?assertEqual(0.0, maps:get(total_cost, Summary)),
             ?assertEqual(0.0, maps:get(current_hourly_cost, Summary)),
             ?assertEqual(0, maps:get(running_instances, Summary))
         end},

        {"returns cost summary with running instances",
         fun() ->
             State = state_with_instances(),
             {reply, Summary, _} = flurm_cloud_scaling:handle_call(
                 get_cost_summary, {self(), make_ref()}, State),
             %% Two instances at $0.17/hr = $0.34/hr
             ?assertEqual(0.34, maps:get(current_hourly_cost, Summary)),
             ?assertEqual(2, maps:get(running_instances, Summary)),
             ?assert(maps:get(projected_daily_cost, Summary) > 0),
             ?assert(maps:get(projected_monthly_cost, Summary) > 0)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Set Budget Limit Tests
%%====================================================================

set_budget_limit_test_() ->
    {"set_budget_limit tests", [
        {"sets budget limit",
         fun() ->
             State = default_test_state(),
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_budget_limit, 1000.0}, {self(), make_ref()}, State),
             ?assertEqual(1000.0, NewState#state.cost_tracking#cost_tracking.budget_limit)
         end},

        {"sets budget limit to zero (unlimited)",
         fun() ->
             State = default_test_state(),
             CostTracking = State#state.cost_tracking,
             StateWithBudget = State#state{
                 cost_tracking = CostTracking#cost_tracking{budget_limit = 500.0}
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_budget_limit, 0.0}, {self(), make_ref()}, StateWithBudget),
             ?assertEqual(0.0, NewState#state.cost_tracking#cost_tracking.budget_limit)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Get Budget Status Tests
%%====================================================================

get_budget_status_test_() ->
    {"get_budget_status tests", [
        {"returns unlimited status when no budget set",
         fun() ->
             State = default_test_state(),
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_budget_status, {self(), make_ref()}, State),
             ?assertEqual(0.0, maps:get(budget_limit, Status)),
             ?assertEqual(0.0, maps:get(percentage_used, Status)),
             ?assertEqual(unlimited, maps:get(remaining, Status))
         end},

        {"returns budget status when budget is set",
         fun() ->
             State = default_test_state(),
             CostTracking = State#state.cost_tracking,
             StateWithBudget = State#state{
                 cost_tracking = CostTracking#cost_tracking{
                     budget_limit = 1000.0,
                     total_cost = 250.0
                 }
             },
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_budget_status, {self(), make_ref()}, StateWithBudget),
             ?assertEqual(1000.0, maps:get(budget_limit, Status)),
             ?assertEqual(250.0, maps:get(total_cost, Status)),
             ?assertEqual(25.0, maps:get(percentage_used, Status)),
             ?assertEqual(750.0, maps:get(remaining, Status)),
             ?assertEqual(false, maps:get(over_budget, Status))
         end},

        {"indicates over budget when exceeded",
         fun() ->
             State = default_test_state(),
             CostTracking = State#state.cost_tracking,
             StateOverBudget = State#state{
                 cost_tracking = CostTracking#cost_tracking{
                     budget_limit = 100.0,
                     total_cost = 150.0
                 }
             },
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_budget_status, {self(), make_ref()}, StateOverBudget),
             ?assertEqual(true, maps:get(over_budget, Status)),
             ?assertEqual(0, maps:get(remaining, Status))  % max(0, -50) = 0
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Terminate Nodes by Criteria Tests
%%====================================================================

terminate_nodes_by_criteria_test_() ->
    {"terminate_nodes_by_criteria tests", [
        {"returns error when provider not configured",
         fun() ->
             State = default_test_state(),
             StateWithInstances = State#state{
                 cloud_instances = #{<<"i-123">> => #cloud_instance{}},
                 current_cloud_nodes = 1
             },
             {reply, {error, provider_not_configured}, _} = flurm_cloud_scaling:handle_call(
                 {terminate_nodes_by_criteria, #{count => 1}}, {self(), make_ref()}, StateWithInstances)
         end}
    ]}.

%%====================================================================
%% handle_call/3 - Unknown Request Tests
%%====================================================================

unknown_request_test_() ->
    {"unknown request tests", [
        {"returns error for unknown request",
         fun() ->
             State = default_test_state(),
             {reply, {error, unknown_request}, _} = flurm_cloud_scaling:handle_call(
                 {unknown_request, some_data}, {self(), make_ref()}, State)
         end}
    ]}.

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_test_() ->
    {"handle_cast tests", [
        {"ignores unknown cast messages",
         fun() ->
             State = default_test_state(),
             {noreply, NewState} = flurm_cloud_scaling:handle_cast(unknown_msg, State),
             ?assertEqual(State, NewState)
         end},

        {"ignores any cast message",
         fun() ->
             State = aws_configured_state(),
             {noreply, _} = flurm_cloud_scaling:handle_cast({some, message}, State)
         end}
    ]}.

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_test_() ->
    {"handle_info tests", [
        {"check_scaling does nothing when disabled",
         fun() ->
             State = default_test_state(),
             {noreply, NewState} = flurm_cloud_scaling:handle_info(check_scaling, State),
             ?assertEqual(State, NewState)
         end},

        {"check_idle_nodes does nothing when disabled",
         fun() ->
             State = default_test_state(),
             {noreply, NewState} = flurm_cloud_scaling:handle_info(check_idle_nodes, State),
             ?assertEqual(State, NewState)
         end},

        {"update_costs does nothing when disabled",
         fun() ->
             State = default_test_state(),
             {noreply, NewState} = flurm_cloud_scaling:handle_info(update_costs, State),
             ?assertEqual(State, NewState)
         end},

        {"handles unknown info messages",
         fun() ->
             State = default_test_state(),
             {noreply, NewState} = flurm_cloud_scaling:handle_info({unknown, message}, State),
             ?assertEqual(State, NewState)
         end},

        {"instance_launched registers new instance",
         fun() ->
             State = aws_configured_state(),
             InstanceInfo = #{
                 instance_type => <<"c5.xlarge">>,
                 provider => aws,
                 public_ip => <<"1.2.3.4">>,
                 private_ip => <<"10.0.0.1">>
             },
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {instance_launched, <<"i-new-instance">>, InstanceInfo}, State),
             ?assertEqual(1, NewState#state.current_cloud_nodes),
             ?assert(maps:is_key(<<"i-new-instance">>, NewState#state.cloud_instances))
         end},

        {"instance_terminated unregisters instance",
         fun() ->
             State = state_with_instances(),
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {instance_terminated, <<"i-12345">>}, State),
             ?assertEqual(1, NewState#state.current_cloud_nodes),
             ?assertNot(maps:is_key(<<"i-12345">>, NewState#state.cloud_instances))
         end},

        {"instance_terminated handles non-existent instance",
         fun() ->
             State = default_test_state(),
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {instance_terminated, <<"i-nonexistent">>}, State),
             ?assertEqual(State, NewState)
         end},

        {"scale_action_complete handles completed action",
         fun() ->
             State = aws_configured_state(),
             ActionId = <<"test-action-123">>,
             Action = #scaling_action{
                 id = ActionId,
                 type = scale_up,
                 requested_count = 3,
                 actual_count = 3,
                 start_time = erlang:system_time(second) - 10,
                 status = in_progress,
                 instance_ids = []
             },
             StateWithPending = State#state{pending_actions = [Action]},
             Result = {ok, #{instance_ids => [<<"i-1">>, <<"i-2">>, <<"i-3">>]}},
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {scale_action_complete, ActionId, Result}, StateWithPending),
             ?assertEqual([], NewState#state.pending_actions),
             ?assertEqual(1, length(NewState#state.action_history)),
             [CompletedAction] = NewState#state.action_history,
             ?assertEqual(completed, CompletedAction#scaling_action.status),
             ?assertEqual(3, NewState#state.current_cloud_nodes)
         end},

        {"scale_action_complete handles failed action",
         fun() ->
             State = aws_configured_state(),
             ActionId = <<"test-action-456">>,
             Action = #scaling_action{
                 id = ActionId,
                 type = scale_up,
                 requested_count = 3,
                 actual_count = 3,
                 start_time = erlang:system_time(second) - 10,
                 status = in_progress,
                 instance_ids = []
             },
             StateWithPending = State#state{pending_actions = [Action]},
             Result = {error, timeout},
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {scale_action_complete, ActionId, Result}, StateWithPending),
             ?assertEqual([], NewState#state.pending_actions),
             [FailedAction] = NewState#state.action_history,
             ?assertEqual(failed, FailedAction#scaling_action.status),
             ?assertEqual(0, NewState#state.current_cloud_nodes)  % No change on failure
         end},

        {"scale_action_complete ignores unknown action",
         fun() ->
             State = aws_configured_state(),
             Result = {ok, #{instance_ids => [<<"i-1">>]}},
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {scale_action_complete, <<"unknown-action">>, Result}, State),
             ?assertEqual(State, NewState)
         end},

        {"scale_action_complete handles scale_down with terminated_count",
         fun() ->
             State = aws_configured_state(),
             StateWithNodes = State#state{current_cloud_nodes = 10},
             ActionId = <<"scale-down-123">>,
             Action = #scaling_action{
                 id = ActionId,
                 type = scale_down,
                 requested_count = 3,
                 actual_count = 3,
                 start_time = erlang:system_time(second) - 10,
                 status = in_progress,
                 instance_ids = []
             },
             StateWithPending = StateWithNodes#state{pending_actions = [Action]},
             Result = {ok, #{terminated_count => 3}},
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {scale_action_complete, ActionId, Result}, StateWithPending),
             ?assertEqual(7, NewState#state.current_cloud_nodes)  % 10 - 3 = 7
         end}
    ]}.

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_test_() ->
    {"terminate tests", [
        {"returns ok",
         fun() ->
             State = default_test_state(),
             ?assertEqual(ok, flurm_cloud_scaling:terminate(normal, State))
         end},

        {"returns ok for any reason",
         fun() ->
             State = default_test_state(),
             ?assertEqual(ok, flurm_cloud_scaling:terminate(shutdown, State)),
             ?assertEqual(ok, flurm_cloud_scaling:terminate({error, test}, State))
         end}
    ]}.

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_test_() ->
    {"code_change tests", [
        {"returns state unchanged",
         fun() ->
             State = default_test_state(),
             ?assertEqual({ok, State}, flurm_cloud_scaling:code_change("1.0", State, extra))
         end}
    ]}.

%%====================================================================
%% Integration-style Tests (still pure, no mocking)
%%====================================================================

integration_test_() ->
    {"integration-style tests", [
        {"full enable/configure/scale workflow state transitions",
         fun() ->
             %% Start with fresh state
             {ok, State0} = flurm_cloud_scaling:init([]),

             %% Configure provider
             Config = #{
                 region => <<"us-west-2">>,
                 access_key_id => <<"AKIATEST">>,
                 secret_access_key => <<"secret123">>
             },
             {reply, ok, State1} = flurm_cloud_scaling:handle_call(
                 {configure_provider, aws, Config}, {self(), make_ref()}, State0),
             ?assertEqual(aws, State1#state.provider),

             %% Set policy
             PolicyMap = #{min_nodes => 1, max_nodes => 10},
             {reply, ok, State2} = flurm_cloud_scaling:handle_call(
                 {set_policy, PolicyMap}, {self(), make_ref()}, State1),
             ?assertEqual(1, State2#state.policy#scaling_policy.min_nodes),

             %% Enable scaling
             {reply, ok, State3} = flurm_cloud_scaling:handle_call(
                 enable, {self(), make_ref()}, State2),
             ?assertEqual(true, State3#state.enabled),

             %% Clean up timers
             erlang:cancel_timer(State3#state.check_timer),
             erlang:cancel_timer(State3#state.idle_check_timer),

             %% Verify we can get status
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_scaling_status, {self(), make_ref()}, State3),
             ?assertEqual(true, maps:get(enabled, Status)),
             ?assertEqual(aws, maps:get(provider, Status))
         end},

        {"budget tracking workflow",
         fun() ->
             {ok, State0} = flurm_cloud_scaling:init([]),

             %% Set budget
             {reply, ok, State1} = flurm_cloud_scaling:handle_call(
                 {set_budget_limit, 500.0}, {self(), make_ref()}, State0),

             %% Check budget status
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_budget_status, {self(), make_ref()}, State1),
             ?assertEqual(500.0, maps:get(budget_limit, Status)),
             ?assertEqual(0.0, maps:get(percentage_used, Status))
         end}
    ]}.

%%====================================================================
%% Additional Policy Tests
%%====================================================================

additional_policy_test_() ->
    {"additional policy tests", [
        {"updates idle_threshold_seconds",
         fun() ->
             State = default_test_state(),
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_policy, #{idle_threshold_seconds => 600}}, {self(), make_ref()}, State),
             ?assertEqual(600, NewState#state.policy#scaling_policy.idle_threshold_seconds)
         end},

        {"updates queue_depth_threshold",
         fun() ->
             State = default_test_state(),
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_policy, #{queue_depth_threshold => 20}}, {self(), make_ref()}, State),
             ?assertEqual(20, NewState#state.policy#scaling_policy.queue_depth_threshold)
         end},

        {"updates target_idle_nodes",
         fun() ->
             State = default_test_state(),
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {set_policy, #{target_idle_nodes => 5}}, {self(), make_ref()}, State),
             ?assertEqual(5, NewState#state.policy#scaling_policy.target_idle_nodes)
         end}
    ]}.

%%====================================================================
%% Additional Provider Config Validation Tests
%%====================================================================

provider_config_validation_test_() ->
    {"provider config validation tests", [
        {"rejects AWS config missing secret_access_key",
         fun() ->
             State = default_test_state(),
             Config = #{
                 region => <<"us-east-1">>,
                 access_key_id => <<"test-key">>
             },
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, aws, Config}, {self(), make_ref()}, State)
         end},

        {"rejects GCP config missing zone",
         fun() ->
             State = default_test_state(),
             Config = #{
                 project_id => <<"my-project">>,
                 credentials_file => <<"/path/to/creds.json">>
             },
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, gcp, Config}, {self(), make_ref()}, State)
         end},

        {"rejects GCP config missing credentials_file",
         fun() ->
             State = default_test_state(),
             Config = #{
                 project_id => <<"my-project">>,
                 zone => <<"us-central1-a">>
             },
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, gcp, Config}, {self(), make_ref()}, State)
         end},

        {"rejects Azure config missing resource_group",
         fun() ->
             State = default_test_state(),
             Config = #{
                 subscription_id => <<"sub-123">>,
                 tenant_id => <<"tenant-456">>
             },
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, azure, Config}, {self(), make_ref()}, State)
         end},

        {"rejects Azure config missing tenant_id",
         fun() ->
             State = default_test_state(),
             Config = #{
                 subscription_id => <<"sub-123">>,
                 resource_group => <<"my-rg">>
             },
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, azure, Config}, {self(), make_ref()}, State)
         end},

        {"rejects generic config with only scale_down_webhook",
         fun() ->
             State = default_test_state(),
             Config = #{scale_down_webhook => <<"https://api.example.com/scale-down">>},
             {reply, {error, missing_required_config}, _} = flurm_cloud_scaling:handle_call(
                 {configure_provider, generic, Config}, {self(), make_ref()}, State)
         end}
    ]}.

%%====================================================================
%% Cost Tracking Edge Cases Tests
%%====================================================================

cost_tracking_edge_cases_test_() ->
    {"cost tracking edge cases tests", [
        {"handles high budget utilization percentage",
         fun() ->
             State = default_test_state(),
             CostTracking = State#state.cost_tracking,
             StateHighUsage = State#state{
                 cost_tracking = CostTracking#cost_tracking{
                     budget_limit = 100.0,
                     total_cost = 95.0  % 95% usage
                 }
             },
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_budget_status, {self(), make_ref()}, StateHighUsage),
             ?assertEqual(95.0, maps:get(percentage_used, Status)),
             ?assertEqual(5.0, maps:get(remaining, Status)),
             ?assertEqual(false, maps:get(over_budget, Status))
         end},

        {"handles exactly at budget limit",
         fun() ->
             State = default_test_state(),
             CostTracking = State#state.cost_tracking,
             StateAtLimit = State#state{
                 cost_tracking = CostTracking#cost_tracking{
                     budget_limit = 100.0,
                     total_cost = 100.0
                 }
             },
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_budget_status, {self(), make_ref()}, StateAtLimit),
             ?assertEqual(100.0, maps:get(percentage_used, Status)),
             %% max(0, 100.0-100.0) = 0 (integer) not 0.0
             ?assertEqual(0, maps:get(remaining, Status)),
             ?assertEqual(false, maps:get(over_budget, Status))
         end},

        {"cost summary includes daily and monthly costs",
         fun() ->
             State = default_test_state(),
             CostTracking = State#state.cost_tracking,
             StateWithHistory = State#state{
                 cost_tracking = CostTracking#cost_tracking{
                     total_instance_hours = 100.5,
                     total_cost = 50.25,
                     daily_costs = #{<<"2026-01-13">> => 10.5},
                     monthly_costs = #{<<"2026-01">> => 50.25}
                 }
             },
             {reply, Summary, _} = flurm_cloud_scaling:handle_call(
                 get_cost_summary, {self(), make_ref()}, StateWithHistory),
             ?assertEqual(100.5, maps:get(total_instance_hours, Summary)),
             ?assertEqual(50.25, maps:get(total_cost, Summary)),
             ?assertEqual(#{<<"2026-01-13">> => 10.5}, maps:get(daily_costs, Summary)),
             ?assertEqual(#{<<"2026-01">> => 50.25}, maps:get(monthly_costs, Summary))
         end}
    ]}.

%%====================================================================
%% Instance Lifecycle Tests
%%====================================================================

instance_lifecycle_test_() ->
    {"instance lifecycle tests", [
        {"instance_launched sets correct initial state",
         fun() ->
             State = aws_configured_state(),
             InstanceInfo = #{
                 instance_type => <<"t3.medium">>,
                 public_ip => <<"52.1.2.3">>,
                 private_ip => <<"172.16.0.1">>,
                 node_name => <<"compute-001@cluster">>
             },
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {instance_launched, <<"i-launched-001">>, InstanceInfo}, State),
             ?assertEqual(1, NewState#state.current_cloud_nodes),
             %% Verify instance was added to cloud_instances map
             ?assert(maps:is_key(<<"i-launched-001">>, NewState#state.cloud_instances))
         end},

        {"multiple instances can be launched",
         fun() ->
             State = aws_configured_state(),
             InstanceInfo1 = #{instance_type => <<"c5.xlarge">>},
             InstanceInfo2 = #{instance_type => <<"c5.xlarge">>},
             {noreply, State1} = flurm_cloud_scaling:handle_info(
                 {instance_launched, <<"i-001">>, InstanceInfo1}, State),
             {noreply, State2} = flurm_cloud_scaling:handle_info(
                 {instance_launched, <<"i-002">>, InstanceInfo2}, State1),
             ?assertEqual(2, State2#state.current_cloud_nodes),
             ?assertEqual(2, maps:size(State2#state.cloud_instances))
         end},

        {"terminating updates cost tracking",
         fun() ->
             State = state_with_instances(),
             %% Terminate one instance
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {instance_terminated, <<"i-12345">>}, State),
             %% Cost tracking should be updated
             CostTracking = NewState#state.cost_tracking,
             ?assert(CostTracking#cost_tracking.total_instance_hours > 0)
         end}
    ]}.

%%====================================================================
%% Pending Actions Tests
%%====================================================================

pending_actions_test_() ->
    {"pending actions tests", [
        {"counts pending actions in status",
         fun() ->
             State = aws_configured_state(),
             Action1 = #scaling_action{
                 id = <<"action-1">>,
                 type = scale_up,
                 requested_count = 3,
                 actual_count = 3,
                 start_time = 1000,
                 status = in_progress,
                 instance_ids = []
             },
             Action2 = #scaling_action{
                 id = <<"action-2">>,
                 type = scale_down,
                 requested_count = 1,
                 actual_count = 1,
                 start_time = 1100,
                 status = in_progress,
                 instance_ids = []
             },
             StateWithPending = State#state{pending_actions = [Action1, Action2]},
             {reply, Status, _} = flurm_cloud_scaling:handle_call(
                 get_scaling_status, {self(), make_ref()}, StateWithPending),
             ?assertEqual(2, maps:get(pending_actions, Status))
         end},

        {"counts pending actions in stats",
         fun() ->
             State = aws_configured_state(),
             Action = #scaling_action{
                 id = <<"action-1">>,
                 type = scale_up,
                 requested_count = 5,
                 actual_count = 5,
                 start_time = 1000,
                 status = in_progress,
                 instance_ids = []
             },
             StateWithPending = State#state{pending_actions = [Action]},
             {reply, Stats, _} = flurm_cloud_scaling:handle_call(
                 get_stats, {self(), make_ref()}, StateWithPending),
             ?assertEqual(1, maps:get(pending_actions, Stats))
         end},

        {"action history keeps last 100 actions",
         fun() ->
             State = aws_configured_state(),
             ActionId = <<"test-action">>,
             Action = #scaling_action{
                 id = ActionId,
                 type = scale_up,
                 requested_count = 1,
                 actual_count = 1,
                 start_time = erlang:system_time(second) - 10,
                 status = in_progress,
                 instance_ids = []
             },
             %% Create state with 99 history items
             OldActions = [#scaling_action{
                 id = iolist_to_binary(["old-", integer_to_binary(N)]),
                 type = scale_up,
                 requested_count = 1,
                 actual_count = 1,
                 start_time = N,
                 end_time = N + 10,
                 status = completed,
                 instance_ids = []
             } || N <- lists:seq(1, 99)],
             StateWithHistory = State#state{
                 pending_actions = [Action],
                 action_history = OldActions
             },
             %% Complete the action
             Result = {ok, #{instance_ids => [<<"i-1">>]}},
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {scale_action_complete, ActionId, Result}, StateWithHistory),
             %% Should have exactly 100 items (new + 99 old truncated)
             ?assertEqual(100, length(NewState#state.action_history))
         end}
    ]}.

%%====================================================================
%% Scale Action Complete - Additional Edge Cases
%%====================================================================

scale_action_complete_edge_cases_test_() ->
    {"scale_action_complete edge cases", [
        {"handles action with count result format",
         fun() ->
             State = aws_configured_state(),
             ActionId = <<"action-with-count">>,
             Action = #scaling_action{
                 id = ActionId,
                 type = scale_up,
                 requested_count = 5,
                 actual_count = 5,
                 start_time = erlang:system_time(second) - 10,
                 status = in_progress,
                 instance_ids = []
             },
             StateWithPending = State#state{pending_actions = [Action]},
             Result = {ok, #{count => 5}},
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {scale_action_complete, ActionId, Result}, StateWithPending),
             [CompletedAction] = NewState#state.action_history,
             ?assertEqual(completed, CompletedAction#scaling_action.status),
             ?assertEqual(5, NewState#state.current_cloud_nodes)
         end},

        {"does not go below zero nodes on scale down failure",
         fun() ->
             State = aws_configured_state(),
             %% Start with 0 nodes
             StateZeroNodes = State#state{current_cloud_nodes = 0},
             ActionId = <<"scale-down-fail">>,
             Action = #scaling_action{
                 id = ActionId,
                 type = scale_down,
                 requested_count = 5,
                 actual_count = 5,
                 start_time = erlang:system_time(second) - 10,
                 status = in_progress,
                 instance_ids = []
             },
             StateWithPending = StateZeroNodes#state{pending_actions = [Action]},
             %% Even if we somehow get a terminated_count result, we should clamp to 0
             Result = {ok, #{terminated_count => 5}},
             {noreply, NewState} = flurm_cloud_scaling:handle_info(
                 {scale_action_complete, ActionId, Result}, StateWithPending),
             ?assertEqual(0, NewState#state.current_cloud_nodes)  % max(0, 0-5) = 0
         end}
    ]}.

%%====================================================================
%% GCP and Azure Provider Tests
%%====================================================================

other_providers_test_() ->
    {"other providers tests", [
        {"GCP provider configuration",
         fun() ->
             State = default_test_state(),
             Config = #{
                 project_id => <<"my-gcp-project">>,
                 zone => <<"us-central1-a">>,
                 credentials_file => <<"/etc/gcp/credentials.json">>
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {configure_provider, gcp, Config}, {self(), make_ref()}, State),
             {reply, {ok, gcp, ReturnedConfig}, _} = flurm_cloud_scaling:handle_call(
                 get_provider_config, {self(), make_ref()}, NewState),
             ?assertEqual(<<"my-gcp-project">>, maps:get(project_id, ReturnedConfig))
         end},

        {"Azure provider configuration",
         fun() ->
             State = default_test_state(),
             Config = #{
                 subscription_id => <<"azure-sub-123">>,
                 resource_group => <<"flurm-rg">>,
                 tenant_id => <<"azure-tenant-456">>
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {configure_provider, azure, Config}, {self(), make_ref()}, State),
             {reply, {ok, azure, ReturnedConfig}, _} = flurm_cloud_scaling:handle_call(
                 get_provider_config, {self(), make_ref()}, NewState),
             ?assertEqual(<<"azure-sub-123">>, maps:get(subscription_id, ReturnedConfig))
         end},

        {"generic webhook provider configuration",
         fun() ->
             State = default_test_state(),
             Config = #{
                 scale_up_webhook => <<"https://hooks.example.com/scale-up">>,
                 scale_down_webhook => <<"https://hooks.example.com/scale-down">>
             },
             {reply, ok, NewState} = flurm_cloud_scaling:handle_call(
                 {configure_provider, generic, Config}, {self(), make_ref()}, State),
             {reply, {ok, generic, ReturnedConfig}, _} = flurm_cloud_scaling:handle_call(
                 get_provider_config, {self(), make_ref()}, NewState),
             ?assertEqual(<<"https://hooks.example.com/scale-up">>,
                          maps:get(scale_up_webhook, ReturnedConfig))
         end}
    ]}.
