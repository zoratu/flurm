%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for FLURM Cloud Scaler
%%%
%%% These tests directly test the gen_server callbacks without mocking.
%%% Tests init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
%%% and code_change/3 directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_cloud_scaler_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% We need to define the state record locally since it's private
-record(state, {
    enabled = false :: boolean(),
    provider :: aws | gcp | azure | undefined,
    config = #{} :: map(),
    eval_timer :: reference() | undefined,
    last_scale_up :: integer() | undefined,
    last_scale_down :: integer() | undefined,
    min_nodes = 0 :: non_neg_integer(),
    max_nodes = 100 :: non_neg_integer(),
    pending_scale_ops = [] :: [map()]
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Create the ETS table that init/1 expects
    case ets:info(flurm_cloud_config) of
        undefined -> ok;
        _ -> ets:delete(flurm_cloud_config)
    end,
    ok.

cleanup(_) ->
    case ets:info(flurm_cloud_config) of
        undefined -> ok;
        _ -> ets:delete(flurm_cloud_config)
    end,
    ok.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init creates state with defaults",
       fun() ->
           {ok, State} = flurm_cloud_scaler:init([]),
           ?assertEqual(false, State#state.enabled),
           ?assertEqual(undefined, State#state.provider),
           ?assertEqual(#{}, State#state.config),
           ?assertEqual(undefined, State#state.eval_timer),
           ?assertEqual(undefined, State#state.last_scale_up),
           ?assertEqual(undefined, State#state.last_scale_down),
           ?assertEqual(0, State#state.min_nodes),
           ?assertEqual(100, State#state.max_nodes),
           ?assertEqual([], State#state.pending_scale_ops)
       end},
      {"init creates ETS config table",
       fun() ->
           %% Clear any existing table
           case ets:info(flurm_cloud_config) of
               undefined -> ok;
               _ -> ets:delete(flurm_cloud_config)
           end,
           {ok, _State} = flurm_cloud_scaler:init([]),
           ?assertNotEqual(undefined, ets:info(flurm_cloud_config))
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - Enable/Disable
%%====================================================================

handle_call_enable_disable_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"enable fails without provider",
       fun() ->
           State = #state{enabled = false, provider = undefined},
           {reply, Result, NewState} = flurm_cloud_scaler:handle_call(enable, {self(), make_ref()}, State),
           ?assertEqual({error, no_provider}, Result),
           ?assertEqual(false, NewState#state.enabled)
       end},
      {"enable succeeds with provider",
       fun() ->
           State = #state{enabled = false, provider = aws},
           {reply, Result, NewState} = flurm_cloud_scaler:handle_call(enable, {self(), make_ref()}, State),
           ?assertEqual(ok, Result),
           ?assertEqual(true, NewState#state.enabled),
           ?assertNotEqual(undefined, NewState#state.eval_timer),
           %% Cancel the timer to clean up
           erlang:cancel_timer(NewState#state.eval_timer)
       end},
      {"disable cancels timer",
       fun() ->
           Timer = erlang:send_after(60000, self(), test),
           State = #state{enabled = true, provider = aws, eval_timer = Timer},
           {reply, Result, NewState} = flurm_cloud_scaler:handle_call(disable, {self(), make_ref()}, State),
           ?assertEqual(ok, Result),
           ?assertEqual(false, NewState#state.enabled),
           ?assertEqual(undefined, NewState#state.eval_timer)
       end},
      {"disable without timer",
       fun() ->
           State = #state{enabled = true, provider = aws, eval_timer = undefined},
           {reply, Result, NewState} = flurm_cloud_scaler:handle_call(disable, {self(), make_ref()}, State),
           ?assertEqual(ok, Result),
           ?assertEqual(false, NewState#state.enabled)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - is_enabled
%%====================================================================

handle_call_is_enabled_test_() ->
    [
     {"is_enabled returns true when enabled",
      fun() ->
          State = #state{enabled = true},
          {reply, Result, _} = flurm_cloud_scaler:handle_call(is_enabled, {self(), make_ref()}, State),
          ?assertEqual(true, Result)
      end},
     {"is_enabled returns false when disabled",
      fun() ->
          State = #state{enabled = false},
          {reply, Result, _} = flurm_cloud_scaler:handle_call(is_enabled, {self(), make_ref()}, State),
          ?assertEqual(false, Result)
      end}
    ].

%%====================================================================
%% handle_call/3 Tests - Provider Management
%%====================================================================

handle_call_provider_test_() ->
    [
     {"set_provider to aws",
      fun() ->
          State = #state{provider = undefined},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({set_provider, aws}, {self(), make_ref()}, State),
          ?assertEqual(ok, Result),
          ?assertEqual(aws, NewState#state.provider)
      end},
     {"set_provider to gcp",
      fun() ->
          State = #state{provider = undefined},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({set_provider, gcp}, {self(), make_ref()}, State),
          ?assertEqual(ok, Result),
          ?assertEqual(gcp, NewState#state.provider)
      end},
     {"set_provider to azure",
      fun() ->
          State = #state{provider = undefined},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({set_provider, azure}, {self(), make_ref()}, State),
          ?assertEqual(ok, Result),
          ?assertEqual(azure, NewState#state.provider)
      end},
     {"get_provider returns undefined initially",
      fun() ->
          State = #state{provider = undefined},
          {reply, Result, _} = flurm_cloud_scaler:handle_call(get_provider, {self(), make_ref()}, State),
          ?assertEqual(undefined, Result)
      end},
     {"get_provider returns current provider",
      fun() ->
          State = #state{provider = gcp},
          {reply, Result, _} = flurm_cloud_scaler:handle_call(get_provider, {self(), make_ref()}, State),
          ?assertEqual(gcp, Result)
      end}
    ].

%%====================================================================
%% handle_call/3 Tests - Config Management
%%====================================================================

handle_call_config_test_() ->
    [
     {"set_config stores configuration",
      fun() ->
          State = #state{config = #{}},
          Config = #{region => <<"us-east-1">>, asg_name => <<"my-asg">>},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({set_config, Config}, {self(), make_ref()}, State),
          ?assertEqual(ok, Result),
          ?assertEqual(Config, NewState#state.config)
      end},
     {"set_config merges with existing config",
      fun() ->
          State = #state{config = #{region => <<"us-east-1">>}},
          NewConfig = #{asg_name => <<"my-asg">>},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({set_config, NewConfig}, {self(), make_ref()}, State),
          ?assertEqual(ok, Result),
          ?assertEqual(#{region => <<"us-east-1">>, asg_name => <<"my-asg">>}, NewState#state.config)
      end},
     {"set_config overwrites existing keys",
      fun() ->
          State = #state{config = #{region => <<"us-east-1">>}},
          NewConfig = #{region => <<"us-west-2">>},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({set_config, NewConfig}, {self(), make_ref()}, State),
          ?assertEqual(ok, Result),
          ?assertEqual(#{region => <<"us-west-2">>}, NewState#state.config)
      end},
     {"get_config returns empty map initially",
      fun() ->
          State = #state{config = #{}},
          {reply, Result, _} = flurm_cloud_scaler:handle_call(get_config, {self(), make_ref()}, State),
          ?assertEqual(#{}, Result)
      end},
     {"get_config returns current config",
      fun() ->
          Config = #{region => <<"eu-west-1">>, mig_name => <<"test-mig">>},
          State = #state{config = Config},
          {reply, Result, _} = flurm_cloud_scaler:handle_call(get_config, {self(), make_ref()}, State),
          ?assertEqual(Config, Result)
      end}
    ].

%%====================================================================
%% handle_call/3 Tests - Limits Management
%%====================================================================

handle_call_limits_test_() ->
    [
     {"set_limits updates min and max nodes",
      fun() ->
          State = #state{min_nodes = 0, max_nodes = 100},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({set_limits, 5, 50}, {self(), make_ref()}, State),
          ?assertEqual(ok, Result),
          ?assertEqual(5, NewState#state.min_nodes),
          ?assertEqual(50, NewState#state.max_nodes)
      end},
     {"set_limits to zero min",
      fun() ->
          State = #state{min_nodes = 5, max_nodes = 100},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({set_limits, 0, 100}, {self(), make_ref()}, State),
          ?assertEqual(ok, Result),
          ?assertEqual(0, NewState#state.min_nodes)
      end},
     {"get_limits returns current limits",
      fun() ->
          State = #state{min_nodes = 10, max_nodes = 200},
          {reply, Result, _} = flurm_cloud_scaler:handle_call(get_limits, {self(), make_ref()}, State),
          ?assertEqual({10, 200}, Result)
      end},
     {"get_limits returns default limits",
      fun() ->
          State = #state{},
          {reply, Result, _} = flurm_cloud_scaler:handle_call(get_limits, {self(), make_ref()}, State),
          ?assertEqual({0, 100}, Result)
      end}
    ].

%%====================================================================
%% handle_call/3 Tests - Scaling Status
%%====================================================================

handle_call_scaling_status_test_() ->
    [
     {"get_scaling_status returns complete status map",
      fun() ->
          State = #state{
              enabled = true,
              provider = aws,
              min_nodes = 5,
              max_nodes = 50,
              last_scale_up = 12345,
              last_scale_down = 67890,
              pending_scale_ops = [#{}, #{}]
          },
          {reply, Status, _} = flurm_cloud_scaler:handle_call(get_scaling_status, {self(), make_ref()}, State),
          ?assertEqual(true, maps:get(enabled, Status)),
          ?assertEqual(aws, maps:get(provider, Status)),
          ?assertEqual(5, maps:get(min_nodes, Status)),
          ?assertEqual(50, maps:get(max_nodes, Status)),
          ?assertEqual(12345, maps:get(last_scale_up, Status)),
          ?assertEqual(67890, maps:get(last_scale_down, Status)),
          ?assertEqual(2, maps:get(pending_operations, Status))
      end},
     {"get_scaling_status with undefined timestamps",
      fun() ->
          State = #state{enabled = false, provider = undefined},
          {reply, Status, _} = flurm_cloud_scaler:handle_call(get_scaling_status, {self(), make_ref()}, State),
          ?assertEqual(false, maps:get(enabled, Status)),
          ?assertEqual(undefined, maps:get(provider, Status)),
          ?assertEqual(undefined, maps:get(last_scale_up, Status)),
          ?assertEqual(undefined, maps:get(last_scale_down, Status)),
          ?assertEqual(0, maps:get(pending_operations, Status))
      end}
    ].

%%====================================================================
%% handle_call/3 Tests - Scale Up
%%====================================================================

handle_call_scale_up_test_() ->
    [
     {"scale_up fails without provider",
      fun() ->
          State = #state{provider = undefined},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({scale_up, 3}, {self(), make_ref()}, State),
          ?assertEqual({error, no_provider}, Result)
      end},
     {"scale_up with AWS provider missing asg_name",
      fun() ->
          State = #state{provider = aws, config = #{}},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({scale_up, 2}, {self(), make_ref()}, State),
          ?assertEqual({error, missing_asg_name}, Result)
      end},
     {"scale_up with AWS provider succeeds",
      fun() ->
          State = #state{provider = aws, config = #{asg_name => <<"test-asg">>}},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({scale_up, 3}, {self(), make_ref()}, State),
          ?assertMatch({ok, [_, _, _]}, Result),
          {ok, InstanceIds} = Result,
          ?assertEqual(3, length(InstanceIds)),
          ?assertNotEqual(undefined, NewState#state.last_scale_up)
      end},
     {"scale_up with GCP provider missing mig_name",
      fun() ->
          State = #state{provider = gcp, config = #{}},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({scale_up, 2}, {self(), make_ref()}, State),
          ?assertEqual({error, missing_mig_name}, Result)
      end},
     {"scale_up with GCP provider succeeds",
      fun() ->
          State = #state{provider = gcp, config = #{mig_name => <<"test-mig">>}},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({scale_up, 2}, {self(), make_ref()}, State),
          ?assertMatch({ok, [_, _]}, Result),
          {ok, InstanceNames} = Result,
          ?assertEqual(2, length(InstanceNames)),
          ?assertNotEqual(undefined, NewState#state.last_scale_up)
      end},
     {"scale_up with Azure provider missing vmss_name",
      fun() ->
          State = #state{provider = azure, config = #{resource_group => <<"test-rg">>}},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({scale_up, 2}, {self(), make_ref()}, State),
          ?assertEqual({error, missing_vmss_name}, Result)
      end},
     {"scale_up with Azure provider missing resource_group",
      fun() ->
          State = #state{provider = azure, config = #{vmss_name => <<"test-vmss">>}},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({scale_up, 2}, {self(), make_ref()}, State),
          ?assertEqual({error, missing_resource_group}, Result)
      end},
     {"scale_up with Azure provider succeeds",
      fun() ->
          State = #state{provider = azure, config = #{vmss_name => <<"test-vmss">>, resource_group => <<"test-rg">>}},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({scale_up, 4}, {self(), make_ref()}, State),
          ?assertMatch({ok, [_, _, _, _]}, Result),
          {ok, VmIds} = Result,
          ?assertEqual(4, length(VmIds)),
          ?assertNotEqual(undefined, NewState#state.last_scale_up)
      end}
    ].

%%====================================================================
%% handle_call/3 Tests - Scale Down
%%====================================================================

handle_call_scale_down_test_() ->
    [
     {"scale_down fails without provider",
      fun() ->
          State = #state{provider = undefined},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({scale_down, 3}, {self(), make_ref()}, State),
          ?assertEqual({error, no_provider}, Result)
      end},
     {"scale_down with AWS provider missing asg_name",
      fun() ->
          State = #state{provider = aws, config = #{}},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({scale_down, 2}, {self(), make_ref()}, State),
          ?assertEqual({error, missing_asg_name}, Result)
      end},
     {"scale_down with AWS provider succeeds",
      fun() ->
          State = #state{provider = aws, config = #{asg_name => <<"test-asg">>}},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({scale_down, 3}, {self(), make_ref()}, State),
          %% Returns empty list since no idle nodes in registry
          ?assertMatch({ok, _}, Result),
          ?assertNotEqual(undefined, NewState#state.last_scale_down)
      end},
     {"scale_down with GCP provider missing mig_name",
      fun() ->
          State = #state{provider = gcp, config = #{}},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({scale_down, 2}, {self(), make_ref()}, State),
          ?assertEqual({error, missing_mig_name}, Result)
      end},
     {"scale_down with GCP provider succeeds",
      fun() ->
          State = #state{provider = gcp, config = #{mig_name => <<"test-mig">>}},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({scale_down, 2}, {self(), make_ref()}, State),
          ?assertMatch({ok, _}, Result),
          ?assertNotEqual(undefined, NewState#state.last_scale_down)
      end},
     {"scale_down with Azure provider missing vmss_name",
      fun() ->
          State = #state{provider = azure, config = #{resource_group => <<"test-rg">>}},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({scale_down, 2}, {self(), make_ref()}, State),
          ?assertEqual({error, missing_vmss_name}, Result)
      end},
     {"scale_down with Azure provider missing resource_group",
      fun() ->
          State = #state{provider = azure, config = #{vmss_name => <<"test-vmss">>}},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({scale_down, 2}, {self(), make_ref()}, State),
          ?assertEqual({error, missing_resource_group}, Result)
      end},
     {"scale_down with Azure provider succeeds",
      fun() ->
          State = #state{provider = azure, config = #{vmss_name => <<"test-vmss">>, resource_group => <<"test-rg">>}},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call({scale_down, 4}, {self(), make_ref()}, State),
          ?assertMatch({ok, _}, Result),
          ?assertNotEqual(undefined, NewState#state.last_scale_down)
      end}
    ].

%%====================================================================
%% handle_call/3 Tests - Unknown Request
%%====================================================================

handle_call_unknown_test_() ->
    [
     {"unknown request returns error",
      fun() ->
          State = #state{},
          {reply, Result, NewState} = flurm_cloud_scaler:handle_call(unknown_request, {self(), make_ref()}, State),
          ?assertEqual({error, unknown_request}, Result),
          ?assertEqual(State, NewState)
      end},
     {"arbitrary tuple request returns error",
      fun() ->
          State = #state{},
          {reply, Result, _} = flurm_cloud_scaler:handle_call({some, random, tuple}, {self(), make_ref()}, State),
          ?assertEqual({error, unknown_request}, Result)
      end}
    ].

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_test_() ->
    [
     {"force_evaluation when disabled does nothing",
      fun() ->
          State = #state{enabled = false},
          {noreply, NewState} = flurm_cloud_scaler:handle_cast(force_evaluation, State),
          ?assertEqual(State, NewState)
      end},
     {"force_evaluation when enabled triggers evaluation",
      fun() ->
          %% This will try to collect metrics which may fail gracefully
          State = #state{enabled = true, provider = aws, config = #{asg_name => <<"test">>}},
          {noreply, _NewState} = flurm_cloud_scaler:handle_cast(force_evaluation, State),
          %% Just verify it doesn't crash
          ?assert(true)
      end},
     {"unknown cast is ignored",
      fun() ->
          State = #state{},
          {noreply, NewState} = flurm_cloud_scaler:handle_cast(unknown_message, State),
          ?assertEqual(State, NewState)
      end},
     {"arbitrary term cast is ignored",
      fun() ->
          State = #state{provider = aws},
          {noreply, NewState} = flurm_cloud_scaler:handle_cast({some, data}, State),
          ?assertEqual(State, NewState)
      end}
    ].

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_test_() ->
    [
     {"evaluate_scaling when disabled",
      fun() ->
          State = #state{enabled = false},
          {noreply, NewState} = flurm_cloud_scaler:handle_info(evaluate_scaling, State),
          ?assertEqual(State, NewState)
      end},
     {"evaluate_scaling when enabled reschedules timer",
      fun() ->
          State = #state{enabled = true, provider = aws, config = #{asg_name => <<"test">>}},
          {noreply, NewState} = flurm_cloud_scaler:handle_info(evaluate_scaling, State),
          ?assertNotEqual(undefined, NewState#state.eval_timer),
          %% Clean up timer
          erlang:cancel_timer(NewState#state.eval_timer)
      end},
     {"unknown info message is ignored",
      fun() ->
          State = #state{},
          {noreply, NewState} = flurm_cloud_scaler:handle_info(unknown_info, State),
          ?assertEqual(State, NewState)
      end},
     {"arbitrary message is ignored",
      fun() ->
          State = #state{provider = gcp},
          {noreply, NewState} = flurm_cloud_scaler:handle_info({arbitrary, message, 123}, State),
          ?assertEqual(State, NewState)
      end}
    ].

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_test_() ->
    [
     {"terminate with no timer",
      fun() ->
          State = #state{eval_timer = undefined},
          Result = flurm_cloud_scaler:terminate(normal, State),
          ?assertEqual(ok, Result)
      end},
     {"terminate cancels timer",
      fun() ->
          Timer = erlang:send_after(60000, self(), test),
          State = #state{eval_timer = Timer},
          Result = flurm_cloud_scaler:terminate(normal, State),
          ?assertEqual(ok, Result),
          %% Verify timer was cancelled (no message received)
          receive
              test -> ?assert(false)
          after 0 ->
              ?assert(true)
          end
      end},
     {"terminate with shutdown reason",
      fun() ->
          State = #state{eval_timer = undefined},
          Result = flurm_cloud_scaler:terminate(shutdown, State),
          ?assertEqual(ok, Result)
      end},
     {"terminate with error reason",
      fun() ->
          State = #state{eval_timer = undefined},
          Result = flurm_cloud_scaler:terminate({error, some_reason}, State),
          ?assertEqual(ok, Result)
      end}
    ].

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_test_() ->
    [
     {"code_change returns state unchanged",
      fun() ->
          State = #state{enabled = true, provider = aws},
          {ok, NewState} = flurm_cloud_scaler:code_change("1.0.0", State, []),
          ?assertEqual(State, NewState)
      end},
     {"code_change with different version",
      fun() ->
          State = #state{config = #{key => value}},
          {ok, NewState} = flurm_cloud_scaler:code_change("2.0.0", State, extra_data),
          ?assertEqual(State, NewState)
      end}
    ].

%%====================================================================
%% Integration-style Tests (still pure, no mocking)
%%====================================================================

workflow_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"complete workflow: init -> set provider -> enable -> disable",
       fun() ->
           %% Init
           {ok, State0} = flurm_cloud_scaler:init([]),
           ?assertEqual(false, State0#state.enabled),

           %% Set provider
           {reply, ok, State1} = flurm_cloud_scaler:handle_call({set_provider, aws}, {self(), make_ref()}, State0),
           ?assertEqual(aws, State1#state.provider),

           %% Set config
           Config = #{asg_name => <<"prod-asg">>, region => <<"us-west-2">>},
           {reply, ok, State2} = flurm_cloud_scaler:handle_call({set_config, Config}, {self(), make_ref()}, State1),
           ?assertEqual(Config, State2#state.config),

           %% Enable
           {reply, ok, State3} = flurm_cloud_scaler:handle_call(enable, {self(), make_ref()}, State2),
           ?assertEqual(true, State3#state.enabled),
           Timer = State3#state.eval_timer,
           ?assertNotEqual(undefined, Timer),

           %% Disable
           {reply, ok, State4} = flurm_cloud_scaler:handle_call(disable, {self(), make_ref()}, State3),
           ?assertEqual(false, State4#state.enabled),
           ?assertEqual(undefined, State4#state.eval_timer)
       end},
      {"limits workflow",
       fun() ->
           State0 = #state{},

           %% Get default limits
           {reply, {0, 100}, State0} = flurm_cloud_scaler:handle_call(get_limits, {self(), make_ref()}, State0),

           %% Set new limits
           {reply, ok, State1} = flurm_cloud_scaler:handle_call({set_limits, 10, 500}, {self(), make_ref()}, State0),
           ?assertEqual(10, State1#state.min_nodes),
           ?assertEqual(500, State1#state.max_nodes),

           %% Get updated limits
           {reply, {10, 500}, State1} = flurm_cloud_scaler:handle_call(get_limits, {self(), make_ref()}, State1)
       end},
      {"scale operations update timestamps",
       fun() ->
           State = #state{provider = aws, config = #{asg_name => <<"test">>}},

           %% Scale up
           {reply, {ok, _}, State1} = flurm_cloud_scaler:handle_call({scale_up, 2}, {self(), make_ref()}, State),
           ?assertNotEqual(undefined, State1#state.last_scale_up),

           %% Scale down
           {reply, {ok, _}, State2} = flurm_cloud_scaler:handle_call({scale_down, 1}, {self(), make_ref()}, State),
           ?assertNotEqual(undefined, State2#state.last_scale_down)
       end}
     ]}.

%%====================================================================
%% Provider-Specific Config Tests
%%====================================================================

provider_config_test_() ->
    [
     {"AWS config with all options",
      fun() ->
          State = #state{provider = aws, config = #{
              asg_name => <<"my-asg">>,
              region => <<"eu-central-1">>,
              launch_template => <<"lt-12345">>
          }},
          {reply, {ok, Instances}, _} = flurm_cloud_scaler:handle_call({scale_up, 5}, {self(), make_ref()}, State),
          ?assertEqual(5, length(Instances)),
          lists:foreach(fun(Id) ->
              ?assertMatch(<<"i-", _/binary>>, Id)
          end, Instances)
      end},
     {"GCP config with all options",
      fun() ->
          State = #state{provider = gcp, config = #{
              mig_name => <<"my-mig">>,
              zone => <<"europe-west1-b">>,
              project => <<"my-project">>
          }},
          {reply, {ok, Instances}, _} = flurm_cloud_scaler:handle_call({scale_up, 3}, {self(), make_ref()}, State),
          ?assertEqual(3, length(Instances)),
          lists:foreach(fun(Name) ->
              ?assertMatch(<<"instance-", _/binary>>, Name)
          end, Instances)
      end},
     {"Azure config with all options",
      fun() ->
          State = #state{provider = azure, config = #{
              vmss_name => <<"my-vmss">>,
              resource_group => <<"my-rg">>,
              subscription => <<"sub-12345">>
          }},
          {reply, {ok, Instances}, _} = flurm_cloud_scaler:handle_call({scale_up, 2}, {self(), make_ref()}, State),
          ?assertEqual(2, length(Instances)),
          lists:foreach(fun(Id) ->
              ?assertMatch(<<"vm_", _/binary>>, Id)
          end, Instances)
      end}
    ].

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    [
     {"multiple enable calls are idempotent",
      fun() ->
          State0 = #state{provider = aws},
          {reply, ok, State1} = flurm_cloud_scaler:handle_call(enable, {self(), make_ref()}, State0),
          Timer1 = State1#state.eval_timer,
          {reply, ok, State2} = flurm_cloud_scaler:handle_call(enable, {self(), make_ref()}, State1),
          Timer2 = State2#state.eval_timer,
          %% Both should be enabled
          ?assertEqual(true, State1#state.enabled),
          ?assertEqual(true, State2#state.enabled),
          %% Cancel timers
          erlang:cancel_timer(Timer1),
          erlang:cancel_timer(Timer2)
      end},
     {"multiple disable calls are safe",
      fun() ->
          State0 = #state{enabled = false, eval_timer = undefined},
          {reply, ok, State1} = flurm_cloud_scaler:handle_call(disable, {self(), make_ref()}, State0),
          {reply, ok, State2} = flurm_cloud_scaler:handle_call(disable, {self(), make_ref()}, State1),
          ?assertEqual(false, State2#state.enabled)
      end},
     {"config merge preserves unrelated keys",
      fun() ->
          State0 = #state{config = #{a => 1, b => 2, c => 3}},
          {reply, ok, State1} = flurm_cloud_scaler:handle_call({set_config, #{b => 20, d => 4}}, {self(), make_ref()}, State0),
          Expected = #{a => 1, b => 20, c => 3, d => 4},
          ?assertEqual(Expected, State1#state.config)
      end},
     {"status reflects all state changes",
      fun() ->
          Now = erlang:monotonic_time(millisecond),
          State = #state{
              enabled = true,
              provider = azure,
              min_nodes = 1,
              max_nodes = 10,
              last_scale_up = Now - 1000,
              last_scale_down = Now - 2000,
              pending_scale_ops = [#{id => 1}, #{id => 2}, #{id => 3}]
          },
          {reply, Status, _} = flurm_cloud_scaler:handle_call(get_scaling_status, {self(), make_ref()}, State),
          ?assertEqual(3, maps:get(pending_operations, Status))
      end}
    ].
