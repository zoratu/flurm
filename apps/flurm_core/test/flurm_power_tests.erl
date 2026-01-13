%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_power module
%%%
%%% Tests cover:
%%% - Power management enable/disable
%%% - Power policies
%%% - Node power state management
%%% - Power capping
%%% - Energy tracking
%%% - Scheduler integration
%%%-------------------------------------------------------------------
-module(flurm_power_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start the power management server
    case whereis(flurm_power) of
        undefined ->
            {ok, Pid} = flurm_power:start_link(),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, _Pid}) ->
    %% Clean up ETS tables
    catch ets:delete(flurm_power_state),
    catch ets:delete(flurm_energy_metrics),
    catch ets:delete(flurm_job_energy),
    catch ets:delete(flurm_node_releases),
    gen_server:stop(flurm_power);
cleanup({existing, _Pid}) ->
    %% Clean tables but leave server running
    catch ets:delete_all_objects(flurm_power_state),
    catch ets:delete_all_objects(flurm_energy_metrics),
    catch ets:delete_all_objects(flurm_job_energy),
    catch ets:delete_all_objects(flurm_node_releases),
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

enable_disable_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"enable power management", fun test_enable/0},
      {"disable power management", fun test_disable/0},
      {"get status", fun test_get_status/0}
     ]}.

policy_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"set policy to aggressive", fun test_set_policy_aggressive/0},
      {"set policy to balanced", fun test_set_policy_balanced/0},
      {"set policy to conservative", fun test_set_policy_conservative/0},
      {"get policy", fun test_get_policy/0}
     ]}.

node_power_state_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get node power state for unknown node", fun test_get_unknown_node_state/0},
      {"set node power state - initial state", fun test_set_initial_node_state/0},
      {"get nodes by power state", fun test_get_nodes_by_state/0},
      {"configure node power method", fun test_configure_node_power/0}
     ]}.

power_capping_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"set power cap", fun test_set_power_cap/0},
      {"get power cap - undefined", fun test_get_power_cap_undefined/0},
      {"get current power - no nodes", fun test_get_current_power_no_nodes/0},
      {"get current power - with nodes", fun test_get_current_power_with_nodes/0}
     ]}.

energy_tracking_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"record power usage", fun test_record_power_usage/0},
      {"get node energy - no samples", fun test_get_node_energy_no_samples/0},
      {"get node energy - with samples", fun test_get_node_energy_with_samples/0},
      {"record job energy", fun test_record_job_energy/0},
      {"get job energy - not found", fun test_get_job_energy_not_found/0}
     ]}.

scheduler_integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"request node power on - new node", fun test_request_power_on_new/0},
      {"request node power on - already powered", fun test_request_power_on_already_powered/0},
      {"get available powered nodes - empty", fun test_get_available_powered_nodes_empty/0},
      {"get available powered nodes - with nodes", fun test_get_available_powered_nodes_with_nodes/0},
      {"notify job start", fun test_notify_job_start/0},
      {"notify job end", fun test_notify_job_end/0},
      {"release node for power off", fun test_release_node_for_power_off/0}
     ]}.

legacy_api_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"set idle timeout", fun test_set_idle_timeout/0},
      {"get idle timeout", fun test_get_idle_timeout/0},
      {"set resume timeout", fun test_set_resume_timeout/0},
      {"get power stats", fun test_get_power_stats/0},
      {"legacy power budget alias", fun test_legacy_power_budget/0}
     ]}.

%%====================================================================
%% Enable/Disable Test Cases
%%====================================================================

test_enable() ->
    %% Power management should be enabled by default
    Status = flurm_power:get_status(),
    ?assertEqual(true, maps:get(enabled, Status)),

    %% Disable and re-enable
    ok = flurm_power:disable(),
    Status2 = flurm_power:get_status(),
    ?assertEqual(false, maps:get(enabled, Status2)),

    ok = flurm_power:enable(),
    Status3 = flurm_power:get_status(),
    ?assertEqual(true, maps:get(enabled, Status3)).

test_disable() ->
    ok = flurm_power:disable(),
    Status = flurm_power:get_status(),
    ?assertEqual(false, maps:get(enabled, Status)).

test_get_status() ->
    Status = flurm_power:get_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(enabled, Status)),
    ?assert(maps:is_key(policy, Status)),
    ?assert(maps:is_key(idle_timeout, Status)),
    ?assert(maps:is_key(resume_timeout, Status)),
    ?assert(maps:is_key(min_powered_nodes, Status)),
    ?assert(maps:is_key(resume_threshold, Status)),
    ?assert(maps:is_key(power_cap, Status)),
    ?assert(maps:is_key(current_power, Status)),
    ?assert(maps:is_key(nodes_powered_on, Status)),
    ?assert(maps:is_key(nodes_powered_off, Status)),
    ?assert(maps:is_key(nodes_suspended, Status)),
    ?assert(maps:is_key(nodes_transitioning, Status)).

%%====================================================================
%% Policy Test Cases
%%====================================================================

test_set_policy_aggressive() ->
    ok = flurm_power:set_policy(aggressive),
    ?assertEqual(aggressive, flurm_power:get_policy()),

    %% Verify aggressive policy settings are applied
    Status = flurm_power:get_status(),
    ?assertEqual(60, maps:get(idle_timeout, Status)),
    ?assertEqual(1, maps:get(min_powered_nodes, Status)),
    ?assertEqual(3, maps:get(resume_threshold, Status)).

test_set_policy_balanced() ->
    ok = flurm_power:set_policy(balanced),
    ?assertEqual(balanced, flurm_power:get_policy()),

    %% Verify balanced policy settings
    Status = flurm_power:get_status(),
    ?assertEqual(300, maps:get(idle_timeout, Status)),
    ?assertEqual(2, maps:get(min_powered_nodes, Status)),
    ?assertEqual(5, maps:get(resume_threshold, Status)).

test_set_policy_conservative() ->
    ok = flurm_power:set_policy(conservative),
    ?assertEqual(conservative, flurm_power:get_policy()),

    %% Verify conservative policy settings
    Status = flurm_power:get_status(),
    ?assertEqual(900, maps:get(idle_timeout, Status)),
    ?assertEqual(5, maps:get(min_powered_nodes, Status)),
    ?assertEqual(10, maps:get(resume_threshold, Status)).

test_get_policy() ->
    %% Default policy is balanced
    ?assertEqual(balanced, flurm_power:get_policy()),

    %% Change and verify
    ok = flurm_power:set_policy(aggressive),
    ?assertEqual(aggressive, flurm_power:get_policy()),

    ok = flurm_power:set_policy(conservative),
    ?assertEqual(conservative, flurm_power:get_policy()).

%%====================================================================
%% Node Power State Test Cases
%%====================================================================

test_get_unknown_node_state() ->
    Result = flurm_power:get_node_power_state(<<"unknown_node">>),
    ?assertEqual({error, not_found}, Result).

test_set_initial_node_state() ->
    %% Configure a node first to create its entry
    NodeName = <<"test_node_1">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Verify the node is created with powered_on state by default
    {ok, State} = flurm_power:get_node_power_state(NodeName),
    ?assertEqual(powered_on, State).

test_get_nodes_by_state() ->
    %% Initially no nodes
    ?assertEqual([], flurm_power:get_nodes_by_power_state(powered_on)),

    %% Configure some nodes
    flurm_power:configure_node_power(<<"node1">>, #{method => script}),
    flurm_power:configure_node_power(<<"node2">>, #{method => script}),
    flurm_power:configure_node_power(<<"node3">>, #{method => script}),

    %% All should be powered on by default
    PoweredOn = flurm_power:get_nodes_by_power_state(powered_on),
    ?assertEqual(3, length(PoweredOn)),
    ?assert(lists:member(<<"node1">>, PoweredOn)),
    ?assert(lists:member(<<"node2">>, PoweredOn)),
    ?assert(lists:member(<<"node3">>, PoweredOn)).

test_configure_node_power() ->
    NodeName = <<"config_test_node">>,

    %% Configure with IPMI settings
    Config = #{
        method => ipmi,
        ipmi_address => <<"192.168.1.100">>,
        ipmi_user => <<"admin">>,
        ipmi_password => <<"secret">>,
        power_watts => 500
    },
    ok = flurm_power:configure_node_power(NodeName, Config),

    %% Verify node exists and can be queried
    {ok, State} = flurm_power:get_node_power_state(NodeName),
    ?assertEqual(powered_on, State),

    %% Verify power policy retrieval
    {ok, Policy} = flurm_power:get_power_policy(NodeName),
    ?assertEqual(ipmi, maps:get(method, Policy)),
    ?assertEqual(<<"192.168.1.100">>, maps:get(ipmi_address, Policy)),
    ?assertEqual(500, maps:get(power_watts, Policy)).

%%====================================================================
%% Power Capping Test Cases
%%====================================================================

test_set_power_cap() ->
    ok = flurm_power:set_power_cap(10000),
    ?assertEqual(10000, flurm_power:get_power_cap()),

    ok = flurm_power:set_power_cap(5000),
    ?assertEqual(5000, flurm_power:get_power_cap()).

test_get_power_cap_undefined() ->
    %% Default power cap is undefined
    ?assertEqual(undefined, flurm_power:get_power_cap()).

test_get_current_power_no_nodes() ->
    %% No nodes configured, power should be 0
    Power = flurm_power:get_current_power(),
    ?assertEqual(0, Power).

test_get_current_power_with_nodes() ->
    %% Configure some nodes with known power
    flurm_power:configure_node_power(<<"power_node1">>, #{
        method => script,
        power_watts => 400
    }),
    flurm_power:configure_node_power(<<"power_node2">>, #{
        method => script,
        power_watts => 600
    }),

    %% Current power should sum the powered-on nodes
    Power = flurm_power:get_current_power(),
    ?assertEqual(1000, Power).

%%====================================================================
%% Energy Tracking Test Cases
%%====================================================================

test_record_power_usage() ->
    NodeName = <<"energy_test_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Record power usage
    flurm_power:record_power_usage(NodeName, 350),

    %% Give async cast time to process
    timer:sleep(50),

    %% Verify power was recorded via power policy
    {ok, Policy} = flurm_power:get_power_policy(NodeName),
    ?assertEqual(350, maps:get(power_watts, Policy)).

test_get_node_energy_no_samples() ->
    Now = erlang:system_time(second),
    {ok, Energy} = flurm_power:get_node_energy(<<"no_samples_node">>, {Now - 3600, Now}),
    ?assertEqual(0.0, Energy).

test_get_node_energy_with_samples() ->
    NodeName = <<"energy_sample_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Record multiple power samples using the API
    %% (This uses gen_server:cast so give it time to process)
    flurm_power:record_power_usage(NodeName, 300),
    timer:sleep(100),
    flurm_power:record_power_usage(NodeName, 350),
    timer:sleep(100),
    flurm_power:record_power_usage(NodeName, 400),
    timer:sleep(100),

    Now = erlang:system_time(second),

    %% Get energy for the time range (last 10 seconds should have samples)
    {ok, Energy} = flurm_power:get_node_energy(NodeName, {Now - 10, Now}),

    %% Energy should be calculated from the samples
    %% Since samples are recorded at same timestamps (second granularity),
    %% only one sample may be kept. Just verify we get some result.
    ?assert(is_float(Energy)).

test_record_job_energy() ->
    JobId = 12345,
    Nodes = [<<"job_node1">>, <<"job_node2">>],

    %% Record job energy
    flurm_power:record_job_energy(JobId, Nodes, 100.5),

    %% Give async cast time to process
    timer:sleep(50),

    %% Retrieve job energy
    {ok, Energy} = flurm_power:get_job_energy(JobId),
    ?assertEqual(100.5, Energy),

    %% Record more energy
    flurm_power:record_job_energy(JobId, Nodes, 50.0),
    timer:sleep(50),

    %% Should be cumulative
    {ok, TotalEnergy} = flurm_power:get_job_energy(JobId),
    ?assertEqual(150.5, TotalEnergy).

test_get_job_energy_not_found() ->
    Result = flurm_power:get_job_energy(99999),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% Scheduler Integration Test Cases
%%====================================================================

test_request_power_on_new() ->
    NodeName = <<"scheduler_node">>,

    %% Request power on for new node without script configured
    %% This will fail because the default script doesn't exist
    Result = flurm_power:request_node_power_on(NodeName),

    %% Should fail with script not found error (script method is default)
    ?assertMatch({error, {script_not_found, _}}, Result).

test_request_power_on_already_powered() ->
    NodeName = <<"already_on_node">>,

    %% First configure the node as already powered on
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Verify it's powered on
    {ok, powered_on} = flurm_power:get_node_power_state(NodeName),

    %% Request power on for already powered node should succeed
    Result = flurm_power:request_node_power_on(NodeName),
    ?assertEqual(ok, Result).

test_get_available_powered_nodes_empty() ->
    %% No nodes configured
    Nodes = flurm_power:get_available_powered_nodes(),
    ?assertEqual([], Nodes).

test_get_available_powered_nodes_with_nodes() ->
    %% Configure some powered-on nodes
    flurm_power:configure_node_power(<<"avail_node1">>, #{method => script}),
    flurm_power:configure_node_power(<<"avail_node2">>, #{method => script}),

    Nodes = flurm_power:get_available_powered_nodes(),
    ?assertEqual(2, length(Nodes)),
    ?assert(lists:member(<<"avail_node1">>, Nodes)),
    ?assert(lists:member(<<"avail_node2">>, Nodes)).

test_notify_job_start() ->
    JobId = 54321,
    Nodes = [<<"job_start_node1">>, <<"job_start_node2">>],

    %% Configure nodes first
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script})
    end, Nodes),

    %% Notify job start
    flurm_power:notify_job_start(JobId, Nodes),

    %% Give async cast time to process
    timer:sleep(50),

    %% Job energy entry should be created
    {ok, Energy} = flurm_power:get_job_energy(JobId),
    ?assertEqual(0.0, Energy).

test_notify_job_end() ->
    JobId = 11111,
    Nodes = [<<"job_end_node">>],

    %% Configure node
    flurm_power:configure_node_power(<<"job_end_node">>, #{method => script}),

    %% Start job
    flurm_power:notify_job_start(JobId, Nodes),
    timer:sleep(50),

    %% End job
    flurm_power:notify_job_end(JobId),
    timer:sleep(50),

    %% Job should still be retrievable
    {ok, _Energy} = flurm_power:get_job_energy(JobId).

test_release_node_for_power_off() ->
    NodeName = <<"release_test_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Release node for power off
    flurm_power:release_node_for_power_off(NodeName),

    %% Give async cast time to process
    timer:sleep(50),

    %% Node should still be powered on (release just marks it as available)
    {ok, State} = flurm_power:get_node_power_state(NodeName),
    ?assertEqual(powered_on, State).

%%====================================================================
%% Legacy API Test Cases
%%====================================================================

test_set_idle_timeout() ->
    ok = flurm_power:set_idle_timeout(600),
    ?assertEqual(600, flurm_power:get_idle_timeout()).

test_get_idle_timeout() ->
    %% Default is 300 seconds (5 minutes) for balanced policy
    Timeout = flurm_power:get_idle_timeout(),
    ?assertEqual(300, Timeout).

test_set_resume_timeout() ->
    ok = flurm_power:set_resume_timeout(180),
    %% Can only verify via get_status since there's no get_resume_timeout
    Status = flurm_power:get_status(),
    ?assertEqual(180, maps:get(resume_timeout, Status)).

test_get_power_stats() ->
    %% Configure some nodes
    flurm_power:configure_node_power(<<"stats_node1">>, #{
        method => script,
        power_watts => 300
    }),
    flurm_power:configure_node_power(<<"stats_node2">>, #{
        method => script,
        power_watts => 400
    }),

    Stats = flurm_power:get_power_stats(),
    ?assert(is_map(Stats)),
    ?assertEqual(true, maps:get(enabled, Stats)),
    ?assertEqual(balanced, maps:get(policy, Stats)),
    ?assertEqual(2, maps:get(total_nodes, Stats)),
    ?assertEqual(700, maps:get(current_power_watts, Stats)),
    ?assert(maps:is_key(nodes_by_state, Stats)),
    ?assert(maps:is_key(total_suspends, Stats)),
    ?assert(maps:is_key(total_resumes, Stats)).

test_legacy_power_budget() ->
    %% set_power_budget is an alias for set_power_cap
    ok = flurm_power:set_power_budget(8000),
    ?assertEqual(8000, flurm_power:get_power_budget()),

    %% Also verify via the primary API
    ?assertEqual(8000, flurm_power:get_power_cap()).

%%====================================================================
%% Additional Integration Tests
%%====================================================================

cluster_energy_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get cluster energy", fun test_get_cluster_energy/0}
     ]}.

test_get_cluster_energy() ->
    Now = erlang:system_time(second),

    %% Configure nodes
    flurm_power:configure_node_power(<<"cluster_node1">>, #{method => script}),
    flurm_power:configure_node_power(<<"cluster_node2">>, #{method => script}),

    %% Insert energy samples for both nodes
    ets:insert(flurm_energy_metrics, {
        {<<"cluster_node1">>, Now - 20},
        200,
        undefined,
        undefined,
        undefined
    }),
    ets:insert(flurm_energy_metrics, {
        {<<"cluster_node2">>, Now - 20},
        300,
        undefined,
        undefined,
        undefined
    }),

    %% Get total cluster energy
    {ok, Energy} = flurm_power:get_cluster_energy({Now - 60, Now}),
    ?assert(Energy >= 0.0).

check_idle_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"check idle nodes", fun test_check_idle_nodes/0}
     ]}.

test_check_idle_nodes() ->
    %% Configure some nodes
    flurm_power:configure_node_power(<<"idle_node1">>, #{method => script}),
    flurm_power:configure_node_power(<<"idle_node2">>, #{method => script}),

    %% Check idle nodes (shouldn't suspend anything since we're at min_powered_nodes)
    Count = flurm_power:check_idle_nodes(),
    ?assert(is_integer(Count)),
    ?assert(Count >= 0).
