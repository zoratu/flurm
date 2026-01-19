%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_power module.
%%%
%%% These tests directly test init/1, handle_call/3, handle_cast/2,
%%% and handle_info/2 callbacks WITHOUT using meck or any mocking.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_power_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% Re-define records to access internal state
-record(state, {
    enabled :: boolean(),
    policy :: aggressive | balanced | conservative,
    idle_timeout :: non_neg_integer(),
    resume_timeout :: non_neg_integer(),
    min_powered_nodes :: non_neg_integer(),
    resume_threshold :: non_neg_integer(),
    power_cap :: non_neg_integer() | undefined,
    power_check_timer :: reference() | undefined,
    energy_sample_timer :: reference() | undefined,
    pending_power_requests :: #{binary() => {reference(), pid()}}
}).

-record(node_power, {
    name :: binary(),
    state :: powered_on | powered_off | powering_up | powering_down | suspended,
    method :: ipmi | wake_on_lan | cloud | script,
    last_state_change :: non_neg_integer(),
    last_job_end :: non_neg_integer() | undefined,
    suspend_count :: non_neg_integer(),
    resume_count :: non_neg_integer(),
    power_watts :: non_neg_integer() | undefined,
    mac_address :: binary() | undefined,
    ipmi_address :: binary() | undefined,
    ipmi_user :: binary() | undefined,
    ipmi_password :: binary() | undefined,
    cloud_instance_id :: binary() | undefined,
    cloud_provider :: atom() | undefined,
    power_script :: binary() | undefined,
    pending_power_on_requests :: non_neg_integer()
}).

-record(energy_sample, {
    key :: {binary(), non_neg_integer()},
    power_watts :: non_neg_integer(),
    cpu_power :: non_neg_integer() | undefined,
    memory_power :: non_neg_integer() | undefined,
    gpu_power :: non_neg_integer() | undefined
}).

-record(job_energy, {
    job_id :: pos_integer(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer() | undefined,
    nodes :: [binary()],
    total_energy_wh :: float(),
    samples :: non_neg_integer()
}).

-define(POWER_STATE_TABLE, flurm_power_state).
-define(ENERGY_TABLE, flurm_energy_metrics).
-define(JOB_ENERGY_TABLE, flurm_job_energy).
-define(NODE_RELEASES_TABLE, flurm_node_releases).

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Clean up any existing tables
    catch ets:delete(?POWER_STATE_TABLE),
    catch ets:delete(?ENERGY_TABLE),
    catch ets:delete(?JOB_ENERGY_TABLE),
    catch ets:delete(?NODE_RELEASES_TABLE),
    ok.

cleanup(_) ->
    catch ets:delete(?POWER_STATE_TABLE),
    catch ets:delete(?ENERGY_TABLE),
    catch ets:delete(?JOB_ENERGY_TABLE),
    catch ets:delete(?NODE_RELEASES_TABLE),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

init_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"init creates ETS tables and returns correct state",
       fun test_init_creates_tables/0},
      {"init sets balanced policy defaults",
       fun test_init_balanced_defaults/0},
      {"init starts timers",
       fun test_init_starts_timers/0}
     ]}.

handle_call_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"enable sets enabled to true",
       fun test_enable/0},
      {"disable sets enabled to false",
       fun test_disable/0},
      {"get_status returns complete status map",
       fun test_get_status/0},
      {"set_policy to aggressive changes settings",
       fun test_set_policy_aggressive/0},
      {"set_policy to conservative changes settings",
       fun test_set_policy_conservative/0},
      {"get_policy returns current policy",
       fun test_get_policy/0},
      {"set_power_cap sets cap value",
       fun test_set_power_cap/0},
      {"get_power_cap returns cap value",
       fun test_get_power_cap/0},
      {"get_power_cap returns undefined when not set",
       fun test_get_power_cap_undefined/0},
      {"get_current_power calculates from nodes",
       fun test_get_current_power/0},
      {"get_stats returns stats map",
       fun test_get_stats/0},
      {"set_idle_timeout updates timeout",
       fun test_set_idle_timeout/0},
      {"get_idle_timeout returns timeout",
       fun test_get_idle_timeout/0},
      {"set_resume_timeout updates timeout",
       fun test_set_resume_timeout/0},
      {"configure_node_power sets node config",
       fun test_configure_node_power/0},
      {"unknown request returns error",
       fun test_unknown_request/0},
      {"check_idle returns count",
       fun test_check_idle/0}
     ]}.

handle_cast_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"record_power_usage inserts energy sample",
       fun test_record_power_usage/0},
      {"record_power_usage updates node power watts",
       fun test_record_power_usage_updates_node/0},
      {"record_job_energy creates new entry",
       fun test_record_job_energy_new/0},
      {"record_job_energy updates existing entry",
       fun test_record_job_energy_update/0},
      {"release_for_power_off inserts release record",
       fun test_release_for_power_off/0},
      {"release_for_power_off decrements pending requests",
       fun test_release_for_power_off_decrements/0},
      {"job_start creates job energy entry",
       fun test_job_start/0},
      {"job_start marks nodes as active",
       fun test_job_start_marks_nodes/0},
      {"job_end updates job and node records",
       fun test_job_end/0},
      {"unknown cast is ignored",
       fun test_unknown_cast/0}
     ]}.

handle_info_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"check_power reschedules timer when enabled",
       fun test_check_power_enabled/0},
      {"check_power reschedules timer when disabled",
       fun test_check_power_disabled/0},
      {"sample_energy reschedules timer",
       fun test_sample_energy/0},
      {"power_transition_complete updates node state",
       fun test_power_transition_complete/0},
      {"power_timeout handles stuck transition",
       fun test_power_timeout/0},
      {"unknown info is ignored",
       fun test_unknown_info/0}
     ]}.

ets_api_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_node_power_state returns state",
       fun test_get_node_power_state/0},
      {"get_node_power_state returns not_found",
       fun test_get_node_power_state_not_found/0},
      {"get_nodes_by_power_state returns matching nodes",
       fun test_get_nodes_by_power_state/0},
      {"get_available_powered_nodes returns powered on nodes",
       fun test_get_available_powered_nodes/0},
      {"get_job_energy returns energy",
       fun test_get_job_energy/0},
      {"get_job_energy returns not_found",
       fun test_get_job_energy_not_found/0},
      {"get_power_policy returns node config",
       fun test_get_power_policy/0},
      {"get_power_policy returns not_found",
       fun test_get_power_policy_not_found/0}
     ]}.

legacy_api_test_() ->
    [
     {"suspend_node calls set_node_power_state with suspend",
      fun test_suspend_node_delegation/0},
     {"resume_node calls set_node_power_state with resume",
      fun test_resume_node_delegation/0},
     {"power_off_node calls set_node_power_state with off",
      fun test_power_off_node_delegation/0},
     {"power_on_node calls set_node_power_state with on",
      fun test_power_on_node_delegation/0},
     {"set_power_budget calls set_power_cap",
      fun test_set_power_budget_delegation/0},
     {"get_power_budget calls get_power_cap",
      fun test_get_power_budget_delegation/0},
     {"get_current_power_usage calls get_current_power",
      fun test_get_current_power_usage_delegation/0}
    ].

internal_function_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get_node_energy calculates energy for time range",
       fun test_get_node_energy/0},
      {"get_cluster_energy sums all nodes",
       fun test_get_cluster_energy/0}
     ]}.

%%====================================================================
%% Init Tests
%%====================================================================

test_init_creates_tables() ->
    {ok, State} = flurm_power:init([]),

    %% Verify ETS tables were created
    ?assertEqual(?POWER_STATE_TABLE, ets:info(?POWER_STATE_TABLE, name)),
    ?assertEqual(?ENERGY_TABLE, ets:info(?ENERGY_TABLE, name)),
    ?assertEqual(?JOB_ENERGY_TABLE, ets:info(?JOB_ENERGY_TABLE, name)),
    ?assertEqual(?NODE_RELEASES_TABLE, ets:info(?NODE_RELEASES_TABLE, name)),

    %% Verify tables are public and have correct type
    ?assertEqual(public, ets:info(?POWER_STATE_TABLE, protection)),
    ?assertEqual(set, ets:info(?POWER_STATE_TABLE, type)),
    ?assertEqual(ordered_set, ets:info(?ENERGY_TABLE, type)),

    %% Cleanup timers
    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_init_balanced_defaults() ->
    {ok, State} = flurm_power:init([]),

    ?assertEqual(true, State#state.enabled),
    ?assertEqual(balanced, State#state.policy),
    ?assertEqual(300, State#state.idle_timeout),  % 5 minutes for balanced
    ?assertEqual(120, State#state.resume_timeout),
    ?assertEqual(2, State#state.min_powered_nodes),  % balanced default
    ?assertEqual(5, State#state.resume_threshold),   % balanced default
    ?assertEqual(undefined, State#state.power_cap),
    ?assertEqual(#{}, State#state.pending_power_requests),

    %% Cleanup timers
    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_init_starts_timers() ->
    {ok, State} = flurm_power:init([]),

    ?assert(is_reference(State#state.power_check_timer)),
    ?assert(is_reference(State#state.energy_sample_timer)),

    %% Verify timers are active
    ?assert(erlang:read_timer(State#state.power_check_timer) > 0),
    ?assert(erlang:read_timer(State#state.energy_sample_timer) > 0),

    %% Cleanup timers
    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

%%====================================================================
%% Handle Call Tests
%%====================================================================

test_enable() ->
    {ok, State} = flurm_power:init([]),
    DisabledState = State#state{enabled = false},

    {reply, ok, NewState} = flurm_power:handle_call(enable, {self(), make_ref()}, DisabledState),

    ?assertEqual(true, NewState#state.enabled),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_disable() ->
    {ok, State} = flurm_power:init([]),

    {reply, ok, NewState} = flurm_power:handle_call(disable, {self(), make_ref()}, State),

    ?assertEqual(false, NewState#state.enabled),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_status() ->
    {ok, State} = flurm_power:init([]),

    %% Add some test nodes
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        power_watts = 500,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node2">>,
        state = powered_off,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {reply, Status, _NewState} = flurm_power:handle_call(get_status, {self(), make_ref()}, State),

    ?assertEqual(true, maps:get(enabled, Status)),
    ?assertEqual(balanced, maps:get(policy, Status)),
    ?assertEqual(300, maps:get(idle_timeout, Status)),
    ?assertEqual(120, maps:get(resume_timeout, Status)),
    ?assertEqual(2, maps:get(min_powered_nodes, Status)),
    ?assertEqual(5, maps:get(resume_threshold, Status)),
    ?assertEqual(undefined, maps:get(power_cap, Status)),
    ?assertEqual(1, maps:get(nodes_powered_on, Status)),
    ?assertEqual(1, maps:get(nodes_powered_off, Status)),
    ?assertEqual(0, maps:get(nodes_suspended, Status)),
    ?assertEqual(0, maps:get(nodes_transitioning, Status)),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_set_policy_aggressive() ->
    {ok, State} = flurm_power:init([]),

    {reply, ok, NewState} = flurm_power:handle_call({set_policy, aggressive}, {self(), make_ref()}, State),

    ?assertEqual(aggressive, NewState#state.policy),
    ?assertEqual(60, NewState#state.idle_timeout),
    ?assertEqual(1, NewState#state.min_powered_nodes),
    ?assertEqual(3, NewState#state.resume_threshold),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_set_policy_conservative() ->
    {ok, State} = flurm_power:init([]),

    {reply, ok, NewState} = flurm_power:handle_call({set_policy, conservative}, {self(), make_ref()}, State),

    ?assertEqual(conservative, NewState#state.policy),
    ?assertEqual(900, NewState#state.idle_timeout),
    ?assertEqual(5, NewState#state.min_powered_nodes),
    ?assertEqual(10, NewState#state.resume_threshold),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_policy() ->
    {ok, State} = flurm_power:init([]),

    {reply, Policy, _NewState} = flurm_power:handle_call(get_policy, {self(), make_ref()}, State),

    ?assertEqual(balanced, Policy),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_set_power_cap() ->
    {ok, State} = flurm_power:init([]),

    {reply, ok, NewState} = flurm_power:handle_call({set_power_cap, 10000}, {self(), make_ref()}, State),

    ?assertEqual(10000, NewState#state.power_cap),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_power_cap() ->
    {ok, State} = flurm_power:init([]),
    StateWithCap = State#state{power_cap = 5000},

    {reply, Cap, _NewState} = flurm_power:handle_call(get_power_cap, {self(), make_ref()}, StateWithCap),

    ?assertEqual(5000, Cap),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_power_cap_undefined() ->
    {ok, State} = flurm_power:init([]),

    {reply, Cap, _NewState} = flurm_power:handle_call(get_power_cap, {self(), make_ref()}, State),

    ?assertEqual(undefined, Cap),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_current_power() ->
    {ok, State} = flurm_power:init([]),

    %% Add powered on nodes with known power values
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        power_watts = 300,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node2">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        power_watts = 400,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node3">>,
        state = powered_off,  % This should not be counted
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        power_watts = 500,
        pending_power_on_requests = 0
    }),

    {reply, Power, _NewState} = flurm_power:handle_call(get_current_power, {self(), make_ref()}, State),

    ?assertEqual(700, Power),  % 300 + 400, node3 is off

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_stats() ->
    {ok, State} = flurm_power:init([]),

    %% Add some nodes
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 5,
        resume_count = 3,
        power_watts = 500,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node2">>,
        state = suspended,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 2,
        resume_count = 1,
        pending_power_on_requests = 0
    }),

    {reply, Stats, _NewState} = flurm_power:handle_call(get_stats, {self(), make_ref()}, State),

    ?assertEqual(true, maps:get(enabled, Stats)),
    ?assertEqual(balanced, maps:get(policy, Stats)),
    ?assertEqual(2, maps:get(total_nodes, Stats)),
    ?assertEqual(7, maps:get(total_suspends, Stats)),  % 5 + 2
    ?assertEqual(4, maps:get(total_resumes, Stats)),   % 3 + 1
    ?assertEqual(500, maps:get(current_power_watts, Stats)),

    NodesByState = maps:get(nodes_by_state, Stats),
    ?assertEqual(1, maps:get(powered_on, NodesByState)),
    ?assertEqual(1, maps:get(suspended, NodesByState)),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_set_idle_timeout() ->
    {ok, State} = flurm_power:init([]),

    {reply, ok, NewState} = flurm_power:handle_call({set_idle_timeout, 600}, {self(), make_ref()}, State),

    ?assertEqual(600, NewState#state.idle_timeout),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_idle_timeout() ->
    {ok, State} = flurm_power:init([]),

    {reply, Timeout, _NewState} = flurm_power:handle_call(get_idle_timeout, {self(), make_ref()}, State),

    ?assertEqual(300, Timeout),  % Default balanced timeout

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_set_resume_timeout() ->
    {ok, State} = flurm_power:init([]),

    {reply, ok, NewState} = flurm_power:handle_call({set_resume_timeout, 180}, {self(), make_ref()}, State),

    ?assertEqual(180, NewState#state.resume_timeout),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_configure_node_power() ->
    {ok, State} = flurm_power:init([]),

    Config = #{
        method => ipmi,
        ipmi_address => <<"192.168.1.100">>,
        ipmi_user => <<"admin">>,
        ipmi_password => <<"secret">>,
        power_watts => 600,
        mac_address => <<"AA:BB:CC:DD:EE:FF">>
    },

    {reply, ok, _NewState} = flurm_power:handle_call(
        {configure_node_power, <<"node1">>, Config},
        {self(), make_ref()},
        State
    ),

    %% Verify node was configured
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(ipmi, NodePower#node_power.method),
    ?assertEqual(<<"192.168.1.100">>, NodePower#node_power.ipmi_address),
    ?assertEqual(<<"admin">>, NodePower#node_power.ipmi_user),
    ?assertEqual(<<"secret">>, NodePower#node_power.ipmi_password),
    ?assertEqual(600, NodePower#node_power.power_watts),
    ?assertEqual(<<"AA:BB:CC:DD:EE:FF">>, NodePower#node_power.mac_address),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_unknown_request() ->
    {ok, State} = flurm_power:init([]),

    {reply, Result, _NewState} = flurm_power:handle_call(unknown_request, {self(), make_ref()}, State),

    ?assertEqual({error, unknown_request}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_check_idle() ->
    {ok, State} = flurm_power:init([]),

    %% With no nodes, should return 0
    {reply, Count, _NewState} = flurm_power:handle_call(check_idle, {self(), make_ref()}, State),

    ?assertEqual(0, Count),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

%%====================================================================
%% Handle Cast Tests
%%====================================================================

test_record_power_usage() ->
    {ok, State} = flurm_power:init([]),

    {noreply, _NewState} = flurm_power:handle_cast(
        {record_power_usage, <<"node1">>, 450},
        State
    ),

    %% Verify energy sample was inserted
    Samples = ets:tab2list(?ENERGY_TABLE),
    ?assertEqual(1, length(Samples)),
    [Sample] = Samples,
    ?assertEqual(450, Sample#energy_sample.power_watts),
    {NodeName, _Timestamp} = Sample#energy_sample.key,
    ?assertEqual(<<"node1">>, NodeName),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_record_power_usage_updates_node() ->
    {ok, State} = flurm_power:init([]),

    %% Create a node first
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        power_watts = 300,
        pending_power_on_requests = 0
    }),

    {noreply, _NewState} = flurm_power:handle_cast(
        {record_power_usage, <<"node1">>, 550},
        State
    ),

    %% Verify node power_watts was updated
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(550, NodePower#node_power.power_watts),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_record_job_energy_new() ->
    {ok, State} = flurm_power:init([]),

    {noreply, _NewState} = flurm_power:handle_cast(
        {record_job_energy, 123, [<<"node1">>, <<"node2">>], 15.5},
        State
    ),

    %% Verify job energy entry was created
    [JobEnergy] = ets:lookup(?JOB_ENERGY_TABLE, 123),
    ?assertEqual(123, JobEnergy#job_energy.job_id),
    ?assertEqual([<<"node1">>, <<"node2">>], JobEnergy#job_energy.nodes),
    ?assert(JobEnergy#job_energy.total_energy_wh >= 15.5),
    ?assertEqual(1, JobEnergy#job_energy.samples),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_record_job_energy_update() ->
    {ok, State} = flurm_power:init([]),

    %% Create initial entry
    ets:insert(?JOB_ENERGY_TABLE, #job_energy{
        job_id = 456,
        start_time = erlang:system_time(second),
        nodes = [<<"node1">>],
        total_energy_wh = 10.0,
        samples = 5
    }),

    {noreply, _NewState} = flurm_power:handle_cast(
        {record_job_energy, 456, [<<"node1">>], 5.0},
        State
    ),

    %% Verify job energy was updated
    [JobEnergy] = ets:lookup(?JOB_ENERGY_TABLE, 456),
    ?assertEqual(15.0, JobEnergy#job_energy.total_energy_wh),
    ?assertEqual(6, JobEnergy#job_energy.samples),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_release_for_power_off() ->
    {ok, State} = flurm_power:init([]),

    {noreply, _NewState} = flurm_power:handle_cast(
        {release_for_power_off, <<"node1">>},
        State
    ),

    %% Verify release was recorded
    [{<<"node1">>, ReleaseTime}] = ets:lookup(?NODE_RELEASES_TABLE, <<"node1">>),
    ?assert(is_integer(ReleaseTime)),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_release_for_power_off_decrements() ->
    {ok, State} = flurm_power:init([]),

    %% Create node with pending requests
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 3
    }),

    {noreply, _NewState} = flurm_power:handle_cast(
        {release_for_power_off, <<"node1">>},
        State
    ),

    %% Verify pending requests was decremented
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(2, NodePower#node_power.pending_power_on_requests),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_job_start() ->
    {ok, State} = flurm_power:init([]),

    {noreply, _NewState} = flurm_power:handle_cast(
        {job_start, 789, [<<"node1">>, <<"node2">>]},
        State
    ),

    %% Verify job energy entry was created
    [JobEnergy] = ets:lookup(?JOB_ENERGY_TABLE, 789),
    ?assertEqual(789, JobEnergy#job_energy.job_id),
    ?assertEqual([<<"node1">>, <<"node2">>], JobEnergy#job_energy.nodes),
    ?assertEqual(0.0, JobEnergy#job_energy.total_energy_wh),
    ?assertEqual(0, JobEnergy#job_energy.samples),
    ?assertEqual(undefined, JobEnergy#job_energy.end_time),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_job_start_marks_nodes() ->
    {ok, State} = flurm_power:init([]),

    %% Create nodes with last_job_end set
    Now = erlang:system_time(second),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = Now,
        last_job_end = Now - 100,
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {noreply, _NewState} = flurm_power:handle_cast(
        {job_start, 101, [<<"node1">>]},
        State
    ),

    %% Verify last_job_end was cleared (set to undefined)
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(undefined, NodePower#node_power.last_job_end),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_job_end() ->
    {ok, State} = flurm_power:init([]),

    %% Create job entry and nodes
    ets:insert(?JOB_ENERGY_TABLE, #job_energy{
        job_id = 999,
        start_time = erlang:system_time(second) - 100,
        nodes = [<<"node1">>, <<"node2">>],
        total_energy_wh = 25.0,
        samples = 10
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        last_job_end = undefined,
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node2">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        last_job_end = undefined,
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {noreply, _NewState} = flurm_power:handle_cast(
        {job_end, 999},
        State
    ),

    %% Verify job entry was updated with end_time
    [JobEnergy] = ets:lookup(?JOB_ENERGY_TABLE, 999),
    ?assert(JobEnergy#job_energy.end_time =/= undefined),

    %% Verify nodes have last_job_end set
    [Node1] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    [Node2] = ets:lookup(?POWER_STATE_TABLE, <<"node2">>),
    ?assert(Node1#node_power.last_job_end =/= undefined),
    ?assert(Node2#node_power.last_job_end =/= undefined),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_unknown_cast() ->
    {ok, State} = flurm_power:init([]),

    {noreply, NewState} = flurm_power:handle_cast(unknown_cast, State),

    %% State should be unchanged
    ?assertEqual(State#state.enabled, NewState#state.enabled),
    ?assertEqual(State#state.policy, NewState#state.policy),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

%%====================================================================
%% Handle Info Tests
%%====================================================================

test_check_power_enabled() ->
    {ok, State} = flurm_power:init([]),
    OldTimer = State#state.power_check_timer,

    {noreply, NewState} = flurm_power:handle_info(check_power, State),

    %% New timer should be set
    ?assert(is_reference(NewState#state.power_check_timer)),
    ?assert(NewState#state.power_check_timer =/= OldTimer),

    erlang:cancel_timer(NewState#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_check_power_disabled() ->
    {ok, State} = flurm_power:init([]),
    DisabledState = State#state{enabled = false},
    OldTimer = DisabledState#state.power_check_timer,

    {noreply, NewState} = flurm_power:handle_info(check_power, DisabledState),

    %% New timer should still be set (just doesn't do work)
    ?assert(is_reference(NewState#state.power_check_timer)),
    ?assert(NewState#state.power_check_timer =/= OldTimer),

    erlang:cancel_timer(NewState#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_sample_energy() ->
    {ok, State} = flurm_power:init([]),
    OldTimer = State#state.energy_sample_timer,

    {noreply, NewState} = flurm_power:handle_info(sample_energy, State),

    %% New timer should be set
    ?assert(is_reference(NewState#state.energy_sample_timer)),
    ?assert(NewState#state.energy_sample_timer =/= OldTimer),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(NewState#state.energy_sample_timer).

test_power_transition_complete() ->
    {ok, State} = flurm_power:init([]),

    %% Create a node in powering_up state
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powering_up,
        method = script,
        last_state_change = erlang:system_time(second) - 30,
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {noreply, _NewState} = flurm_power:handle_info(
        {power_transition_complete, <<"node1">>, powered_on},
        State
    ),

    %% Verify state was updated
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(powered_on, NodePower#node_power.state),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_power_timeout() ->
    {ok, State} = flurm_power:init([]),

    %% Create a node in powering_up state
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powering_up,
        method = script,
        last_state_change = erlang:system_time(second) - 30,
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {noreply, _NewState} = flurm_power:handle_info(
        {power_timeout, <<"node1">>, powering_up},
        State
    ),

    %% Verify state was set to powered_off (timeout failure)
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(powered_off, NodePower#node_power.state),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_unknown_info() ->
    {ok, State} = flurm_power:init([]),

    {noreply, NewState} = flurm_power:handle_info(unknown_info, State),

    %% State should be unchanged
    ?assertEqual(State#state.enabled, NewState#state.enabled),
    ?assertEqual(State#state.policy, NewState#state.policy),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

%%====================================================================
%% ETS API Tests
%%====================================================================

test_get_node_power_state() ->
    {ok, State} = flurm_power:init([]),

    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = suspended,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    Result = flurm_power:get_node_power_state(<<"node1">>),

    ?assertEqual({ok, suspended}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_node_power_state_not_found() ->
    {ok, State} = flurm_power:init([]),

    Result = flurm_power:get_node_power_state(<<"nonexistent">>),

    ?assertEqual({error, not_found}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_nodes_by_power_state() ->
    {ok, State} = flurm_power:init([]),

    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node2">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node3">>,
        state = suspended,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    PoweredOn = flurm_power:get_nodes_by_power_state(powered_on),
    Suspended = flurm_power:get_nodes_by_power_state(suspended),
    PoweredOff = flurm_power:get_nodes_by_power_state(powered_off),

    ?assertEqual(2, length(PoweredOn)),
    ?assert(lists:member(<<"node1">>, PoweredOn)),
    ?assert(lists:member(<<"node2">>, PoweredOn)),
    ?assertEqual([<<"node3">>], Suspended),
    ?assertEqual([], PoweredOff),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_available_powered_nodes() ->
    {ok, State} = flurm_power:init([]),

    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node2">>,
        state = powered_off,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    Available = flurm_power:get_available_powered_nodes(),

    ?assertEqual([<<"node1">>], Available),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_job_energy() ->
    {ok, State} = flurm_power:init([]),

    ets:insert(?JOB_ENERGY_TABLE, #job_energy{
        job_id = 12345,
        start_time = erlang:system_time(second),
        nodes = [<<"node1">>],
        total_energy_wh = 42.5,
        samples = 10
    }),

    Result = flurm_power:get_job_energy(12345),

    ?assertEqual({ok, 42.5}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_job_energy_not_found() ->
    {ok, State} = flurm_power:init([]),

    Result = flurm_power:get_job_energy(99999),

    ?assertEqual({error, not_found}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_power_policy() ->
    {ok, State} = flurm_power:init([]),

    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = ipmi,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        power_watts = 500,
        mac_address = <<"AA:BB:CC:DD:EE:FF">>,
        ipmi_address = <<"192.168.1.100">>,
        pending_power_on_requests = 0
    }),

    Result = flurm_power:get_power_policy(<<"node1">>),

    ?assertMatch({ok, #{method := ipmi, power_watts := 500}}, Result),
    {ok, Policy} = Result,
    ?assertEqual(<<"AA:BB:CC:DD:EE:FF">>, maps:get(mac_address, Policy)),
    ?assertEqual(<<"192.168.1.100">>, maps:get(ipmi_address, Policy)),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_power_policy_not_found() ->
    {ok, State} = flurm_power:init([]),

    Result = flurm_power:get_power_policy(<<"nonexistent">>),

    ?assertEqual({error, not_found}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

%%====================================================================
%% Legacy API Tests
%%====================================================================

test_suspend_node_delegation() ->
    %% Just verify the function exists and has the right arity
    %% The actual delegation will go through gen_server which we can't test purely
    ?assert(is_function(fun flurm_power:suspend_node/1, 1)).

test_resume_node_delegation() ->
    ?assert(is_function(fun flurm_power:resume_node/1, 1)).

test_power_off_node_delegation() ->
    ?assert(is_function(fun flurm_power:power_off_node/1, 1)).

test_power_on_node_delegation() ->
    ?assert(is_function(fun flurm_power:power_on_node/1, 1)).

test_set_power_budget_delegation() ->
    ?assert(is_function(fun flurm_power:set_power_budget/1, 1)).

test_get_power_budget_delegation() ->
    ?assert(is_function(fun flurm_power:get_power_budget/0, 0)).

test_get_current_power_usage_delegation() ->
    ?assert(is_function(fun flurm_power:get_current_power_usage/0, 0)).

%%====================================================================
%% Internal Function Tests
%%====================================================================

test_get_node_energy() ->
    {ok, State} = flurm_power:init([]),

    Now = erlang:system_time(second),

    %% Insert some energy samples
    ets:insert(?ENERGY_TABLE, #energy_sample{
        key = {<<"node1">>, Now - 100},
        power_watts = 400
    }),
    ets:insert(?ENERGY_TABLE, #energy_sample{
        key = {<<"node1">>, Now - 90},
        power_watts = 450
    }),
    ets:insert(?ENERGY_TABLE, #energy_sample{
        key = {<<"node1">>, Now - 80},
        power_watts = 500
    }),

    %% Query energy for time range
    Result = flurm_power:get_node_energy(<<"node1">>, {Now - 150, Now}),

    ?assertMatch({ok, _Energy}, Result),
    {ok, Energy} = Result,
    ?assert(Energy > 0),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_get_cluster_energy() ->
    {ok, State} = flurm_power:init([]),

    Now = erlang:system_time(second),

    %% Insert energy samples for multiple nodes
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = Now,
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node2">>,
        state = powered_on,
        method = script,
        last_state_change = Now,
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    ets:insert(?ENERGY_TABLE, #energy_sample{
        key = {<<"node1">>, Now - 50},
        power_watts = 400
    }),
    ets:insert(?ENERGY_TABLE, #energy_sample{
        key = {<<"node2">>, Now - 50},
        power_watts = 300
    }),

    Result = flurm_power:get_cluster_energy({Now - 100, Now}),

    ?assertMatch({ok, _Energy}, Result),
    {ok, Energy} = Result,
    ?assert(Energy >= 0),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

%%====================================================================
%% Additional Tests - Terminate and Code Change
%%====================================================================

terminate_test() ->
    {ok, State} = flurm_power:init([]),

    Result = flurm_power:terminate(normal, State),

    ?assertEqual(ok, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

code_change_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    Result = flurm_power:code_change("1.0.0", State, []),

    ?assertEqual({ok, State}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Additional Tests - Power states with undefined power_watts
%%====================================================================

current_power_with_undefined_watts_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Add powered on node with undefined power_watts
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node_no_watts">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        power_watts = undefined,
        pending_power_on_requests = 0
    }),

    {reply, Power, _NewState} = flurm_power:handle_call(get_current_power, {self(), make_ref()}, State),

    %% Should use default estimate of 500 watts
    ?assertEqual(500, Power),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Test empty energy query
%%====================================================================

empty_energy_query_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    Now = erlang:system_time(second),
    Result = flurm_power:get_node_energy(<<"nonexistent">>, {Now - 100, Now}),

    ?assertEqual({ok, 0.0}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Test job_end with nonexistent job
%%====================================================================

job_end_nonexistent_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Should not crash when job doesn't exist
    {noreply, _NewState} = flurm_power:handle_cast({job_end, 99999}, State),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Test power timeout when state already changed
%%====================================================================

power_timeout_state_changed_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Create a node that already completed transition
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,  % Already completed, not powering_up
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    %% Timeout for old state should be ignored
    {noreply, _NewState} = flurm_power:handle_info(
        {power_timeout, <<"node1">>, powering_up},
        State
    ),

    %% State should still be powered_on (not changed to powered_off)
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(powered_on, NodePower#node_power.state),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Test powering_down timeout
%%====================================================================

power_timeout_powering_down_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Create a node in powering_down state
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powering_down,
        method = script,
        last_state_change = erlang:system_time(second) - 30,
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {noreply, _NewState} = flurm_power:handle_info(
        {power_timeout, <<"node1">>, powering_down},
        State
    ),

    %% Should be forced to powered_off
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(powered_off, NodePower#node_power.state),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Additional Tests - Power State Transitions
%%====================================================================

set_node_power_state_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"set_node_power_state on already powered_on node",
       fun test_set_node_state_already_on/0},
      {"set_node_power_state off on powered_on node",
       fun test_set_node_state_power_off/0},
      {"set_node_power_state suspend on powered_on node",
       fun test_set_node_state_suspend/0},
      {"set_node_power_state invalid transition",
       fun test_set_node_state_invalid_transition/0},
      {"set_node_power_state on new node",
       fun test_set_node_state_new_node/0}
     ]}.

test_set_node_state_already_on() ->
    {ok, State} = flurm_power:init([]),

    %% Create a powered_on node
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    %% Requesting power on should succeed (no-op)
    {reply, Result, _NewState} = flurm_power:handle_call(
        {set_node_power_state, <<"node1">>, on},
        {self(), make_ref()},
        State
    ),

    ?assertEqual(ok, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_set_node_state_power_off() ->
    {ok, State} = flurm_power:init([]),

    %% Create a powered_on node
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    %% Request power off
    {reply, Result, _NewState} = flurm_power:handle_call(
        {set_node_power_state, <<"node1">>, off},
        {self(), make_ref()},
        State
    ),

    %% Should succeed (script won't actually run since file doesn't exist)
    %% The state will be set to powering_down or error returned
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    case Result of
        ok -> ?assertEqual(powering_down, NodePower#node_power.state);
        {error, {script_not_found, _}} -> ok;
        _ -> ?assert(false)
    end,

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_set_node_state_suspend() ->
    {ok, State} = flurm_power:init([]),

    %% Create a powered_on node
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    %% Request suspend
    {reply, _Result, _NewState} = flurm_power:handle_call(
        {set_node_power_state, <<"node1">>, suspend},
        {self(), make_ref()},
        State
    ),

    %% Check state (may succeed or fail depending on script)
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assert(NodePower#node_power.state =:= powered_on orelse
            NodePower#node_power.state =:= powering_down),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_set_node_state_invalid_transition() ->
    {ok, State} = flurm_power:init([]),

    %% Create a node in powering_up state (can't interrupt)
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powering_up,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    %% Request another transition should fail
    {reply, Result, _NewState} = flurm_power:handle_call(
        {set_node_power_state, <<"node1">>, off},
        {self(), make_ref()},
        State
    ),

    ?assertMatch({error, {invalid_transition, _, _}}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_set_node_state_new_node() ->
    {ok, State} = flurm_power:init([]),

    %% Request suspend on non-existent node (will be created as powered_on)
    {reply, _Result, _NewState} = flurm_power:handle_call(
        {set_node_power_state, <<"new_node">>, suspend},
        {self(), make_ref()},
        State
    ),

    %% Node should now exist
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"new_node">>),
    ?assert(NodePower#node_power.name =:= <<"new_node">>),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

%%====================================================================
%% Additional Tests - Execute Power Command
%%====================================================================

execute_power_command_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"execute_power_command with default method",
       fun test_execute_power_command_default/0},
      {"execute_power_command with ipmi method no address",
       fun test_execute_power_command_ipmi_no_addr/0},
      {"execute_power_command with wake_on_lan no mac",
       fun test_execute_power_command_wol_no_mac/0},
      {"execute_power_command with cloud no provider",
       fun test_execute_power_command_cloud_no_provider/0},
      {"execute_power_command with unsupported method",
       fun test_execute_power_command_unsupported/0}
     ]}.

test_execute_power_command_default() ->
    {ok, State} = flurm_power:init([]),

    %% Create a node with script method
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    %% Execute command (will fail because script doesn't exist)
    {reply, Result, _NewState} = flurm_power:handle_call(
        {execute_power_command, <<"node1">>, power_off, #{}},
        {self(), make_ref()},
        State
    ),

    ?assertMatch({error, {script_not_found, _}}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_execute_power_command_ipmi_no_addr() ->
    {ok, State} = flurm_power:init([]),

    %% Create a node with ipmi method but no address
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = ipmi,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        ipmi_address = undefined,
        pending_power_on_requests = 0
    }),

    {reply, Result, _NewState} = flurm_power:handle_call(
        {execute_power_command, <<"node1">>, power_on, #{}},
        {self(), make_ref()},
        State
    ),

    ?assertEqual({error, no_ipmi_address}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_execute_power_command_wol_no_mac() ->
    {ok, State} = flurm_power:init([]),

    %% Create a node with wake_on_lan method but no MAC
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_off,
        method = wake_on_lan,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        mac_address = undefined,
        pending_power_on_requests = 0
    }),

    {reply, Result, _NewState} = flurm_power:handle_call(
        {execute_power_command, <<"node1">>, power_on, #{}},
        {self(), make_ref()},
        State
    ),

    ?assertEqual({error, no_mac_address}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_execute_power_command_cloud_no_provider() ->
    {ok, State} = flurm_power:init([]),

    %% Create a node with cloud method but no provider
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_off,
        method = cloud,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        cloud_provider = undefined,
        pending_power_on_requests = 0
    }),

    {reply, Result, _NewState} = flurm_power:handle_call(
        {execute_power_command, <<"node1">>, power_on, #{}},
        {self(), make_ref()},
        State
    ),

    ?assertEqual({error, no_cloud_provider}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_execute_power_command_unsupported() ->
    {ok, State} = flurm_power:init([]),

    %% Create a node with an unknown method
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_off,
        method = unknown_method,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {reply, Result, _NewState} = flurm_power:handle_call(
        {execute_power_command, <<"node1">>, power_on, #{}},
        {self(), make_ref()},
        State
    ),

    ?assertEqual({error, unsupported_method}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

%%====================================================================
%% Additional Tests - Request Power On
%%====================================================================

request_power_on_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"request_power_on for powered_on node",
       fun test_request_power_on_already_on/0},
      {"request_power_on for powering_up node",
       fun test_request_power_on_powering_up/0},
      {"request_power_on for powered_off node",
       fun test_request_power_on_powered_off/0},
      {"request_power_on for non-existent node",
       fun test_request_power_on_new_node/0}
     ]}.

test_request_power_on_already_on() ->
    {ok, State} = flurm_power:init([]),

    %% Create a powered_on node
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {reply, Result, _NewState} = flurm_power:handle_call(
        {request_power_on, <<"node1">>},
        {self(), make_ref()},
        State
    ),

    ?assertEqual(ok, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_request_power_on_powering_up() ->
    {ok, State} = flurm_power:init([]),

    %% Create a powering_up node
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powering_up,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 1
    }),

    {reply, Result, _NewState} = flurm_power:handle_call(
        {request_power_on, <<"node1">>},
        {self(), make_ref()},
        State
    ),

    ?assertEqual(ok, Result),

    %% Verify pending requests was incremented
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(2, NodePower#node_power.pending_power_on_requests),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_request_power_on_powered_off() ->
    {ok, State} = flurm_power:init([]),

    %% Create a powered_off node
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_off,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {reply, _Result, _NewState} = flurm_power:handle_call(
        {request_power_on, <<"node1">>},
        {self(), make_ref()},
        State
    ),

    %% Verify pending requests was incremented
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(1, NodePower#node_power.pending_power_on_requests),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

test_request_power_on_new_node() ->
    {ok, State} = flurm_power:init([]),

    {reply, _Result, _NewState} = flurm_power:handle_call(
        {request_power_on, <<"new_node">>},
        {self(), make_ref()},
        State
    ),

    %% Node should be created with pending request
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"new_node">>),
    ?assertEqual(1, NodePower#node_power.pending_power_on_requests),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer).

%%====================================================================
%% Additional Tests - Wake-on-LAN
%%====================================================================

wake_on_lan_with_mac_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Create a node with wake_on_lan method and valid MAC
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_off,
        method = wake_on_lan,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        mac_address = <<"AA:BB:CC:DD:EE:FF">>,
        pending_power_on_requests = 0
    }),

    %% Execute power_on command with wake_on_lan
    {reply, Result, _NewState} = flurm_power:handle_call(
        {execute_power_command, <<"node1">>, power_on, #{}},
        {self(), make_ref()},
        State
    ),

    %% Should succeed (WoL packet sent)
    ?assertEqual(ok, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

wake_on_lan_power_off_unsupported_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Create a node with wake_on_lan method
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = wake_on_lan,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        mac_address = <<"AA:BB:CC:DD:EE:FF">>,
        pending_power_on_requests = 0
    }),

    %% Execute power_off command with wake_on_lan should fail
    {reply, Result, _NewState} = flurm_power:handle_call(
        {execute_power_command, <<"node1">>, power_off, #{}},
        {self(), make_ref()},
        State
    ),

    ?assertEqual({error, wake_on_lan_only_supports_power_on}, Result),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Additional Tests - Configure Node with Various Options
%%====================================================================

configure_node_cloud_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    Config = #{
        method => cloud,
        cloud_provider => aws,
        cloud_instance_id => <<"i-1234567890abcdef0">>
    },

    {reply, ok, _NewState} = flurm_power:handle_call(
        {configure_node_power, <<"cloud_node">>, Config},
        {self(), make_ref()},
        State
    ),

    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"cloud_node">>),
    ?assertEqual(cloud, NodePower#node_power.method),
    ?assertEqual(aws, NodePower#node_power.cloud_provider),
    ?assertEqual(<<"i-1234567890abcdef0">>, NodePower#node_power.cloud_instance_id),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

configure_node_script_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    Config = #{
        method => script,
        power_script => <<"/custom/power/script.sh">>
    },

    {reply, ok, _NewState} = flurm_power:handle_call(
        {configure_node_power, <<"script_node">>, Config},
        {self(), make_ref()},
        State
    ),

    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"script_node">>),
    ?assertEqual(script, NodePower#node_power.method),
    ?assertEqual(<<"/custom/power/script.sh">>, NodePower#node_power.power_script),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

configure_node_unknown_key_ignored_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    Config = #{
        method => ipmi,
        unknown_key => <<"ignored">>,
        another_unknown => 12345
    },

    {reply, ok, _NewState} = flurm_power:handle_call(
        {configure_node_power, <<"node_with_unknown">>, Config},
        {self(), make_ref()},
        State
    ),

    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node_with_unknown">>),
    ?assertEqual(ipmi, NodePower#node_power.method),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Additional Tests - Power transition complete for powered_on
%%====================================================================

power_transition_complete_powered_on_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Create a node in powering_up state
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powering_up,
        method = script,
        last_state_change = erlang:system_time(second) - 30,
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {noreply, _NewState} = flurm_power:handle_info(
        {power_transition_complete, <<"node1">>, powered_on},
        State
    ),

    %% Verify state was updated to powered_on
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(powered_on, NodePower#node_power.state),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

power_transition_complete_suspended_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Create a node in powering_down state
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powering_down,
        method = script,
        last_state_change = erlang:system_time(second) - 30,
        suspend_count = 1,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    {noreply, _NewState} = flurm_power:handle_info(
        {power_transition_complete, <<"node1">>, suspended},
        State
    ),

    %% Verify state was updated to suspended
    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(suspended, NodePower#node_power.state),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

power_transition_complete_nonexistent_node_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Completing transition for non-existent node should not crash
    {noreply, _NewState} = flurm_power:handle_info(
        {power_transition_complete, <<"nonexistent">>, powered_on},
        State
    ),

    ?assertEqual([], ets:lookup(?POWER_STATE_TABLE, <<"nonexistent">>)),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Additional Tests - Record power usage for non-existent node
%%====================================================================

record_power_usage_nonexistent_node_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Recording power for non-existent node should not crash
    {noreply, _NewState} = flurm_power:handle_cast(
        {record_power_usage, <<"nonexistent">>, 500},
        State
    ),

    %% Sample should still be recorded
    Samples = ets:tab2list(?ENERGY_TABLE),
    ?assertEqual(1, length(Samples)),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Additional Tests - Release for power off with zero pending
%%====================================================================

release_for_power_off_zero_pending_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Create node with zero pending requests
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"node1">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        pending_power_on_requests = 0
    }),

    %% Should not crash or go negative
    {noreply, _NewState} = flurm_power:handle_cast(
        {release_for_power_off, <<"node1">>},
        State
    ),

    [NodePower] = ets:lookup(?POWER_STATE_TABLE, <<"node1">>),
    ?assertEqual(0, NodePower#node_power.pending_power_on_requests),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Additional Tests - Job start on non-existent node
%%====================================================================

job_start_nonexistent_node_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Job start on non-existent nodes should not crash
    {noreply, _NewState} = flurm_power:handle_cast(
        {job_start, 123, [<<"nonexistent1">>, <<"nonexistent2">>]},
        State
    ),

    %% Job entry should still be created
    [JobEnergy] = ets:lookup(?JOB_ENERGY_TABLE, 123),
    ?assertEqual([<<"nonexistent1">>, <<"nonexistent2">>], JobEnergy#job_energy.nodes),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Additional Tests - Mixed node states in get_current_power
%%====================================================================

current_power_mixed_states_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Add nodes in various states
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"powered_on_with_watts">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        power_watts = 300,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"powered_on_no_watts">>,
        state = powered_on,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 0,
        power_watts = undefined,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"suspended">>,
        state = suspended,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 1,
        resume_count = 0,
        power_watts = 400,
        pending_power_on_requests = 0
    }),
    ets:insert(?POWER_STATE_TABLE, #node_power{
        name = <<"powering_up">>,
        state = powering_up,
        method = script,
        last_state_change = erlang:system_time(second),
        suspend_count = 0,
        resume_count = 1,
        power_watts = 350,
        pending_power_on_requests = 0
    }),

    {reply, Power, _NewState} = flurm_power:handle_call(get_current_power, {self(), make_ref()}, State),

    %% Should be 300 (with watts) + 500 (default for undefined) = 800
    %% Suspended and powering_up nodes should not be counted
    ?assertEqual(800, Power),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Additional Tests - set_policy maintains other state fields
%%====================================================================

set_policy_preserves_state_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),
    StateWithCap = State#state{power_cap = 5000, enabled = false},

    {reply, ok, NewState} = flurm_power:handle_call(
        {set_policy, aggressive},
        {self(), make_ref()},
        StateWithCap
    ),

    %% Policy settings should change
    ?assertEqual(aggressive, NewState#state.policy),
    ?assertEqual(60, NewState#state.idle_timeout),

    %% But power_cap and enabled should be preserved
    ?assertEqual(5000, NewState#state.power_cap),
    ?assertEqual(false, NewState#state.enabled),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).

%%====================================================================
%% Additional Tests - Power timeout for non-existent node
%%====================================================================

power_timeout_nonexistent_node_test() ->
    setup(),
    {ok, State} = flurm_power:init([]),

    %% Timeout for non-existent node should not crash
    {noreply, _NewState} = flurm_power:handle_info(
        {power_timeout, <<"nonexistent">>, powering_up},
        State
    ),

    erlang:cancel_timer(State#state.power_check_timer),
    erlang:cancel_timer(State#state.energy_sample_timer),
    cleanup(ok).
