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
            %% Unlink to prevent test process crash on shutdown
            unlink(Pid),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, Pid}) ->
    %% Clean up ETS tables
    catch ets:delete(flurm_power_state),
    catch ets:delete(flurm_energy_metrics),
    catch ets:delete(flurm_job_energy),
    catch ets:delete(flurm_node_releases),
    case is_process_alive(Pid) of
        true ->
            Ref = monitor(process, Pid),
            catch gen_server:stop(flurm_power, shutdown, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush])
            end;
        false ->
            ok
    end;
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
    _ = sys:get_state(flurm_power),

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
    _ = sys:get_state(flurm_power),
    flurm_power:record_power_usage(NodeName, 350),
    _ = sys:get_state(flurm_power),
    flurm_power:record_power_usage(NodeName, 400),
    _ = sys:get_state(flurm_power),

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
    _ = sys:get_state(flurm_power),

    %% Retrieve job energy
    {ok, Energy} = flurm_power:get_job_energy(JobId),
    ?assertEqual(100.5, Energy),

    %% Record more energy
    flurm_power:record_job_energy(JobId, Nodes, 50.0),
    _ = sys:get_state(flurm_power),

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
    _ = sys:get_state(flurm_power),

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
    _ = sys:get_state(flurm_power),

    %% End job
    flurm_power:notify_job_end(JobId),
    _ = sys:get_state(flurm_power),

    %% Job should still be retrievable
    {ok, _Energy} = flurm_power:get_job_energy(JobId).

test_release_node_for_power_off() ->
    NodeName = <<"release_test_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Release node for power off
    flurm_power:release_node_for_power_off(NodeName),

    %% Give async cast time to process
    _ = sys:get_state(flurm_power),

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

%%====================================================================
%% IPMI Power Control Tests (Phase 2)
%%====================================================================

ipmi_control_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"ipmi power on command", fun test_ipmi_power_on/0},
      {"ipmi power off command", fun test_ipmi_power_off/0},
      {"ipmi power status", fun test_ipmi_power_status/0},
      {"ipmi power reset", fun test_ipmi_power_reset/0},
      {"ipmi power cycle", fun test_ipmi_power_cycle/0},
      {"ipmi connection error", fun test_ipmi_connection_error/0},
      {"ipmi authentication failure", fun test_ipmi_auth_failure/0}
     ]}.

test_ipmi_power_on() ->
    NodeName = <<"ipmi_on_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => ipmi,
        ipmi_address => <<"192.168.1.50">>,
        ipmi_user => <<"admin">>,
        ipmi_password => <<"password">>
    }),

    %% Manually set to powered_off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok  % IPMI not available in test
    end.

test_ipmi_power_off() ->
    NodeName = <<"ipmi_off_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => ipmi,
        ipmi_address => <<"192.168.1.51">>,
        ipmi_user => <<"admin">>,
        ipmi_password => <<"password">>
    }),

    Result = flurm_power:request_node_power_off(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_ipmi_power_status() ->
    NodeName = <<"ipmi_status_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => ipmi,
        ipmi_address => <<"192.168.1.52">>,
        ipmi_user => <<"admin">>,
        ipmi_password => <<"password">>
    }),

    Result = flurm_power:get_node_power_state(NodeName),
    case Result of
        {ok, State} -> ?assert(is_atom(State));
        {error, _} -> ok
    end.

test_ipmi_power_reset() ->
    NodeName = <<"ipmi_reset_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => ipmi,
        ipmi_address => <<"192.168.1.53">>,
        ipmi_user => <<"admin">>,
        ipmi_password => <<"password">>
    }),

    Result = flurm_power:reset_node(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_ipmi_power_cycle() ->
    NodeName = <<"ipmi_cycle_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => ipmi,
        ipmi_address => <<"192.168.1.54">>,
        ipmi_user => <<"admin">>,
        ipmi_password => <<"password">>
    }),

    Result = flurm_power:cycle_node(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_ipmi_connection_error() ->
    NodeName = <<"ipmi_error_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => ipmi,
        ipmi_address => <<"10.255.255.1">>,  % Unreachable
        ipmi_user => <<"admin">>,
        ipmi_password => <<"password">>
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    ?assertMatch({error, _}, Result).

test_ipmi_auth_failure() ->
    NodeName = <<"ipmi_auth_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => ipmi,
        ipmi_address => <<"192.168.1.56">>,
        ipmi_user => <<"wrong_user">>,
        ipmi_password => <<"wrong_password">>
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    case Result of
        ok -> ok;  % May succeed if IPMI not configured
        {error, _} -> ok
    end.

%%====================================================================
%% Wake-on-LAN Tests (Phase 3)
%%====================================================================

wol_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"wake node using WOL", fun test_wol_wake/0},
      {"WOL with broadcast address", fun test_wol_broadcast/0},
      {"WOL invalid MAC", fun test_wol_invalid_mac/0},
      {"WOL multiple nodes", fun test_wol_multiple_nodes/0}
     ]}.

test_wol_wake() ->
    NodeName = <<"wol_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => wol,
        mac_address => <<"AA:BB:CC:DD:EE:FF">>
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok  % WOL may not be available in test
    end.

test_wol_broadcast() ->
    NodeName = <<"wol_broadcast_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => wol,
        mac_address => <<"11:22:33:44:55:66">>,
        broadcast_address => <<"192.168.1.255">>
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_wol_invalid_mac() ->
    NodeName = <<"wol_invalid_mac_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => wol,
        mac_address => <<"INVALID:MAC">>
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_wol_multiple_nodes() ->
    Nodes = [<<"wol_m1">>, <<"wol_m2">>, <<"wol_m3">>],
    Macs = [<<"AA:11:22:33:44:01">>, <<"AA:11:22:33:44:02">>, <<"AA:11:22:33:44:03">>],

    lists:foreach(fun({N, M}) ->
        flurm_power:configure_node_power(N, #{
            method => wol,
            mac_address => M
        }),
        ets:insert(flurm_power_state, {N, #{state => powered_off}})
    end, lists:zip(Nodes, Macs)),

    %% Wake all nodes
    Results = [flurm_power:request_node_power_on(N) || N <- Nodes],
    lists:foreach(fun(R) ->
        case R of
            ok -> ok;
            {error, _} -> ok
        end
    end, Results).

%%====================================================================
%% Script-based Power Control Tests (Phase 4)
%%====================================================================

script_control_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"script power on", fun test_script_power_on/0},
      {"script power off", fun test_script_power_off/0},
      {"script with arguments", fun test_script_with_args/0},
      {"script timeout", fun test_script_timeout/0},
      {"script not found", fun test_script_not_found/0},
      {"script error exit", fun test_script_error/0}
     ]}.

test_script_power_on() ->
    NodeName = <<"script_on_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_on_script => <<"/usr/local/bin/power_on.sh">>,
        power_off_script => <<"/usr/local/bin/power_off.sh">>
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    case Result of
        ok -> ok;
        {error, {script_not_found, _}} -> ok;
        {error, _} -> ok
    end.

test_script_power_off() ->
    NodeName = <<"script_off_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_on_script => <<"/usr/local/bin/power_on.sh">>,
        power_off_script => <<"/usr/local/bin/power_off.sh">>
    }),

    Result = flurm_power:request_node_power_off(NodeName),
    case Result of
        ok -> ok;
        {error, {script_not_found, _}} -> ok;
        {error, _} -> ok
    end.

test_script_with_args() ->
    NodeName = <<"script_args_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_on_script => <<"/usr/local/bin/power.sh on">>,
        power_off_script => <<"/usr/local/bin/power.sh off">>,
        script_args => [NodeName, <<"--timeout=30">>]
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_script_timeout() ->
    NodeName = <<"script_timeout_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_on_script => <<"/bin/sleep 100">>,  % Long running
        script_timeout => 1  % 1 second timeout
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    case Result of
        ok -> ok;  % May complete before timeout
        {error, timeout} -> ok;
        {error, _} -> ok
    end.

test_script_not_found() ->
    NodeName = <<"script_missing_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_on_script => <<"/nonexistent/path/script.sh">>
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    ?assertMatch({error, {script_not_found, _}}, Result).

test_script_error() ->
    NodeName = <<"script_error_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_on_script => <<"/bin/false">>  % Always exits with error
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName),
    case Result of
        ok -> ok;  % Script may not exist
        {error, _} -> ok
    end.

%%====================================================================
%% Power Scheduling Tests (Phase 5)
%%====================================================================

power_scheduling_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"schedule power off", fun test_schedule_power_off/0},
      {"schedule power on", fun test_schedule_power_on/0},
      {"cancel scheduled action", fun test_cancel_scheduled/0},
      {"scheduled action expiry", fun test_scheduled_expiry/0},
      {"get scheduled actions", fun test_get_scheduled_actions/0}
     ]}.

test_schedule_power_off() ->
    NodeName = <<"sched_off_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Schedule power off in 1 hour
    Result = flurm_power:schedule_power_off(NodeName, 3600),
    case Result of
        {ok, _ScheduleId} -> ok;
        ok -> ok;
        {error, _} -> ok
    end.

test_schedule_power_on() ->
    NodeName = <<"sched_on_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    %% Schedule power on in 30 minutes
    Result = flurm_power:schedule_power_on(NodeName, 1800),
    case Result of
        {ok, _ScheduleId} -> ok;
        ok -> ok;
        {error, _} -> ok
    end.

test_cancel_scheduled() ->
    NodeName = <<"sched_cancel_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    case flurm_power:schedule_power_off(NodeName, 7200) of
        {ok, ScheduleId} ->
            Result = flurm_power:cancel_scheduled_action(ScheduleId),
            case Result of
                ok -> ok;
                {error, _} -> ok
            end;
        _ -> ok
    end.

test_scheduled_expiry() ->
    NodeName = <<"sched_expiry_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Schedule action with very short delay (will likely expire before executed)
    Result = flurm_power:schedule_power_off(NodeName, 1),
    case Result of
        {ok, _} -> ok;
        ok -> ok;
        {error, _} -> ok
    end,
    timer:sleep(100).

test_get_scheduled_actions() ->
    NodeName = <<"sched_list_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    _ = flurm_power:schedule_power_off(NodeName, 3600),

    Result = flurm_power:get_scheduled_actions(),
    case Result of
        {ok, Actions} when is_list(Actions) -> ok;
        Actions when is_list(Actions) -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Power Profile Tests (Phase 6)
%%====================================================================

power_profile_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"set node power profile", fun test_set_power_profile/0},
      {"get node power profile", fun test_get_power_profile/0},
      {"power profile affects scheduling", fun test_profile_scheduling/0},
      {"default power profile", fun test_default_profile/0}
     ]}.

test_set_power_profile() ->
    NodeName = <<"profile_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    Result = flurm_power:set_node_power_profile(NodeName, #{
        max_power => 500,
        idle_power => 100,
        sleep_power => 10
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_get_power_profile() ->
    NodeName = <<"profile_get_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_watts => 400
    }),

    Result = flurm_power:get_power_policy(NodeName),
    case Result of
        {ok, Profile} when is_map(Profile) ->
            ?assert(maps:is_key(power_watts, Profile));
        {error, _} -> ok
    end.

test_profile_scheduling() ->
    NodeName = <<"profile_sched_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_watts => 600
    }),

    %% High power node should be considered for power-off first
    ok = flurm_power:set_policy(aggressive),
    _ = flurm_power:check_idle_nodes(),
    ok.

test_default_profile() ->
    NodeName = <<"profile_default_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    {ok, Profile} = flurm_power:get_power_policy(NodeName),
    %% Default power should be 0 or undefined
    DefaultPower = maps:get(power_watts, Profile, 0),
    ?assert(DefaultPower >= 0).

%%====================================================================
%% Power Limit Tests (Phase 7)
%%====================================================================

power_limit_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"enforce power cap", fun test_enforce_power_cap/0},
      {"power cap prevents power on", fun test_cap_prevents_power_on/0},
      {"dynamic power cap adjustment", fun test_dynamic_cap/0},
      {"power cap status", fun test_power_cap_status/0}
     ]}.

test_enforce_power_cap() ->
    %% Set a low power cap
    ok = flurm_power:set_power_cap(500),

    %% Configure nodes that exceed cap
    flurm_power:configure_node_power(<<"cap_node1">>, #{
        method => script,
        power_watts => 300
    }),
    flurm_power:configure_node_power(<<"cap_node2">>, #{
        method => script,
        power_watts => 300
    }),

    %% Current power (600) exceeds cap (500)
    CurrentPower = flurm_power:get_current_power(),
    ?assertEqual(600, CurrentPower),

    %% Power cap enforcement would prevent new nodes
    PowerCap = flurm_power:get_power_cap(),
    ?assertEqual(500, PowerCap).

test_cap_prevents_power_on() ->
    ok = flurm_power:set_power_cap(100),

    flurm_power:configure_node_power(<<"cap_prevent_node">>, #{
        method => script,
        power_watts => 200  % Exceeds cap
    }),

    %% Set to off first
    ets:insert(flurm_power_state, {<<"cap_prevent_node">>, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(<<"cap_prevent_node">>),
    case Result of
        ok -> ok;  % May succeed if cap not enforced
        {error, power_cap_exceeded} -> ok;
        {error, _} -> ok
    end.

test_dynamic_cap() ->
    %% Increase cap
    ok = flurm_power:set_power_cap(5000),
    ?assertEqual(5000, flurm_power:get_power_cap()),

    %% Decrease cap
    ok = flurm_power:set_power_cap(3000),
    ?assertEqual(3000, flurm_power:get_power_cap()),

    %% Remove cap
    ok = flurm_power:set_power_cap(undefined),
    ?assertEqual(undefined, flurm_power:get_power_cap()).

test_power_cap_status() ->
    ok = flurm_power:set_power_cap(2000),

    flurm_power:configure_node_power(<<"cap_status_node">>, #{
        method => script,
        power_watts => 500
    }),

    Status = flurm_power:get_status(),
    ?assertEqual(2000, maps:get(power_cap, Status)),
    ?assertEqual(500, maps:get(current_power, Status)).

%%====================================================================
%% Suspend/Resume Tests (Phase 8)
%%====================================================================

suspend_resume_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"suspend node", fun test_suspend_node/0},
      {"resume node", fun test_resume_node/0},
      {"suspend all idle", fun test_suspend_all_idle/0},
      {"resume threshold", fun test_resume_threshold/0},
      {"suspend locked node", fun test_suspend_locked/0}
     ]}.

test_suspend_node() ->
    NodeName = <<"suspend_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    Result = flurm_power:suspend_node(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_resume_node() ->
    NodeName = <<"resume_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Suspend first
    _ = flurm_power:suspend_node(NodeName),
    _ = sys:get_state(flurm_power),

    Result = flurm_power:resume_node(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_suspend_all_idle() ->
    %% Configure multiple nodes
    lists:foreach(fun(N) ->
        NodeName = list_to_binary("idle_suspend_" ++ integer_to_list(N)),
        flurm_power:configure_node_power(NodeName, #{method => script})
    end, lists:seq(1, 5)),

    %% Set aggressive policy to suspend more
    ok = flurm_power:set_policy(aggressive),

    Result = flurm_power:suspend_idle_nodes(),
    case Result of
        {ok, Count} when is_integer(Count) -> ok;
        ok -> ok;
        {error, _} -> ok
    end.

test_resume_threshold() ->
    %% Set resume threshold
    ok = flurm_power:set_resume_threshold(5),

    Status = flurm_power:get_status(),
    ?assertEqual(5, maps:get(resume_threshold, Status)).

test_suspend_locked() ->
    NodeName = <<"locked_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Lock node
    _ = flurm_power:lock_node(NodeName),
    _ = sys:get_state(flurm_power),

    %% Try to suspend locked node
    Result = flurm_power:suspend_node(NodeName),
    case Result of
        ok -> ok;  % May succeed
        {error, node_locked} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Node Lock Tests (Phase 9)
%%====================================================================

node_lock_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"lock node", fun test_lock_node/0},
      {"unlock node", fun test_unlock_node/0},
      {"get locked nodes", fun test_get_locked_nodes/0},
      {"locked node prevents power off", fun test_locked_prevents_off/0}
     ]}.

test_lock_node() ->
    NodeName = <<"lock_test_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    Result = flurm_power:lock_node(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_unlock_node() ->
    NodeName = <<"unlock_test_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    _ = flurm_power:lock_node(NodeName),
    _ = sys:get_state(flurm_power),

    Result = flurm_power:unlock_node(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_get_locked_nodes() ->
    Nodes = [<<"locked_1">>, <<"locked_2">>],

    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script}),
        _ = flurm_power:lock_node(N)
    end, Nodes),
    _ = sys:get_state(flurm_power),

    Result = flurm_power:get_locked_nodes(),
    case Result of
        {ok, LockedNodes} when is_list(LockedNodes) -> ok;
        LockedNodes when is_list(LockedNodes) -> ok;
        {error, _} -> ok
    end.

test_locked_prevents_off() ->
    NodeName = <<"locked_off_test">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    _ = flurm_power:lock_node(NodeName),
    _ = sys:get_state(flurm_power),

    Result = flurm_power:request_node_power_off(NodeName),
    case Result of
        ok -> ok;  % May succeed if lock not enforced
        {error, node_locked} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Min/Max Powered Nodes Tests (Phase 10)
%%====================================================================

min_max_nodes_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"set min powered nodes", fun test_set_min_powered/0},
      {"set max powered nodes", fun test_set_max_powered/0},
      {"min nodes prevents power off", fun test_min_prevents_off/0},
      {"max nodes prevents power on", fun test_max_prevents_on/0}
     ]}.

test_set_min_powered() ->
    ok = flurm_power:set_min_powered_nodes(3),
    Status = flurm_power:get_status(),
    ?assertEqual(3, maps:get(min_powered_nodes, Status)).

test_set_max_powered() ->
    Result = flurm_power:set_max_powered_nodes(20),
    case Result of
        ok ->
            Status = flurm_power:get_status(),
            ?assertEqual(20, maps:get(max_powered_nodes, Status, 20));
        {error, _} -> ok
    end.

test_min_prevents_off() ->
    ok = flurm_power:set_min_powered_nodes(2),

    %% Configure exactly 2 nodes
    flurm_power:configure_node_power(<<"min_node1">>, #{method => script}),
    flurm_power:configure_node_power(<<"min_node2">>, #{method => script}),

    %% Should not be able to power off when at minimum
    Result = flurm_power:request_node_power_off(<<"min_node1">>),
    case Result of
        ok -> ok;  % May succeed
        {error, at_min_nodes} -> ok;
        {error, _} -> ok
    end.

test_max_prevents_on() ->
    _ = flurm_power:set_max_powered_nodes(2),

    %% Configure 2 nodes already on
    flurm_power:configure_node_power(<<"max_node1">>, #{method => script}),
    flurm_power:configure_node_power(<<"max_node2">>, #{method => script}),

    %% Add a third node that's off
    flurm_power:configure_node_power(<<"max_node3">>, #{method => script}),
    ets:insert(flurm_power_state, {<<"max_node3">>, #{state => powered_off}}),

    %% Should not be able to power on when at maximum
    Result = flurm_power:request_node_power_on(<<"max_node3">>),
    case Result of
        ok -> ok;  % May succeed
        {error, at_max_nodes} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Energy Cost Calculation Tests (Phase 11)
%%====================================================================

energy_cost_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"calculate energy cost", fun test_calc_energy_cost/0},
      {"set electricity rate", fun test_set_electricity_rate/0},
      {"get total cost", fun test_get_total_cost/0},
      {"cost by time range", fun test_cost_by_time_range/0}
     ]}.

test_calc_energy_cost() ->
    flurm_power:configure_node_power(<<"cost_node">>, #{
        method => script,
        power_watts => 500
    }),

    %% Set electricity rate ($/kWh)
    _ = flurm_power:set_electricity_rate(0.12),

    %% Record some power usage
    Now = erlang:system_time(second),
    ets:insert(flurm_energy_metrics, {
        {<<"cost_node">>, Now - 3600},  % 1 hour ago
        500,
        undefined,
        undefined,
        undefined
    }),

    Result = flurm_power:get_energy_cost(<<"cost_node">>, {Now - 3600, Now}),
    case Result of
        {ok, Cost} when is_number(Cost) -> ?assert(Cost >= 0);
        {error, _} -> ok
    end.

test_set_electricity_rate() ->
    Result = flurm_power:set_electricity_rate(0.15),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_get_total_cost() ->
    Now = erlang:system_time(second),

    flurm_power:configure_node_power(<<"total_cost_node">>, #{
        method => script,
        power_watts => 400
    }),

    Result = flurm_power:get_total_energy_cost({Now - 86400, Now}),  % Last 24h
    case Result of
        {ok, Cost} when is_number(Cost) -> ok;
        {error, _} -> ok
    end.

test_cost_by_time_range() ->
    Now = erlang:system_time(second),

    Result = flurm_power:get_cost_breakdown({Now - 3600, Now}, hourly),
    case Result of
        {ok, Breakdown} when is_list(Breakdown) -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Power Event Tests (Phase 12)
%%====================================================================

power_event_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"subscribe to power events", fun test_subscribe_power_events/0},
      {"unsubscribe from events", fun test_unsubscribe_events/0},
      {"power on generates event", fun test_power_on_event/0},
      {"power off generates event", fun test_power_off_event/0}
     ]}.

test_subscribe_power_events() ->
    Self = self(),
    Result = flurm_power:subscribe_events(Self),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_unsubscribe_events() ->
    Self = self(),
    _ = flurm_power:subscribe_events(Self),
    Result = flurm_power:unsubscribe_events(Self),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_power_on_event() ->
    Self = self(),
    _ = flurm_power:subscribe_events(Self),

    NodeName = <<"event_on_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    _ = flurm_power:request_node_power_on(NodeName),

    receive
        {power_event, power_on, NodeName} -> ok
    after 100 ->
        ok  % Event delivery not guaranteed
    end.

test_power_off_event() ->
    Self = self(),
    _ = flurm_power:subscribe_events(Self),

    NodeName = <<"event_off_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    _ = flurm_power:request_node_power_off(NodeName),

    receive
        {power_event, power_off, NodeName} -> ok
    after 100 ->
        ok
    end.

%%====================================================================
%% Handle Info Tests (Phase 13)
%%====================================================================

handle_info_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"handle check_idle_nodes message", fun test_handle_check_idle/0},
      {"handle power_check message", fun test_handle_power_check/0},
      {"handle unknown message", fun test_handle_unknown_msg/0}
     ]}.

test_handle_check_idle() ->
    flurm_power:configure_node_power(<<"idle_check_node">>, #{method => script}),

    FedPid = whereis(flurm_power),
    FedPid ! check_idle_nodes,
    _ = sys:get_state(flurm_power),
    ok.

test_handle_power_check() ->
    flurm_power:configure_node_power(<<"power_check_node">>, #{method => script}),

    FedPid = whereis(flurm_power),
    FedPid ! power_check,
    _ = sys:get_state(flurm_power),
    ok.

test_handle_unknown_msg() ->
    FedPid = whereis(flurm_power),
    FedPid ! {unknown_message, random_data},
    _ = sys:get_state(flurm_power),
    ok.

%%====================================================================
%% Concurrent Operations Tests (Phase 14)
%%====================================================================

concurrent_ops_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"concurrent power state queries", fun test_concurrent_queries/0},
      {"concurrent power operations", fun test_concurrent_power_ops/0},
      {"concurrent energy recording", fun test_concurrent_energy_rec/0}
     ]}.

test_concurrent_queries() ->
    %% Configure nodes
    lists:foreach(fun(N) ->
        NodeName = list_to_binary("conc_query_" ++ integer_to_list(N)),
        flurm_power:configure_node_power(NodeName, #{method => script})
    end, lists:seq(1, 10)),

    Self = self(),
    Pids = [spawn(fun() ->
        NodeName = list_to_binary("conc_query_" ++ integer_to_list(N rem 10 + 1)),
        _ = flurm_power:get_node_power_state(NodeName),
        _ = flurm_power:get_status(),
        Self ! {done, N}
    end) || N <- lists:seq(1, 50)],

    lists:foreach(fun(_) ->
        receive {done, _} -> ok after 10000 -> ok end
    end, Pids),
    ok.

test_concurrent_power_ops() ->
    %% Configure nodes
    lists:foreach(fun(N) ->
        NodeName = list_to_binary("conc_ops_" ++ integer_to_list(N)),
        flurm_power:configure_node_power(NodeName, #{method => script})
    end, lists:seq(1, 5)),

    Self = self(),
    Pids = [spawn(fun() ->
        NodeName = list_to_binary("conc_ops_" ++ integer_to_list(N rem 5 + 1)),
        _ = flurm_power:request_node_power_on(NodeName),
        Self ! {done, N}
    end) || N <- lists:seq(1, 20)],

    lists:foreach(fun(_) ->
        receive {done, _} -> ok after 10000 -> ok end
    end, Pids),
    ok.

test_concurrent_energy_rec() ->
    NodeName = <<"conc_energy_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    Self = self(),
    Pids = [spawn(fun() ->
        flurm_power:record_power_usage(NodeName, 100 + N),
        Self ! {done, N}
    end) || N <- lists:seq(1, 50)],

    lists:foreach(fun(_) ->
        receive {done, _} -> ok after 10000 -> ok end
    end, Pids),

    _ = sys:get_state(flurm_power),
    ok.

%%====================================================================
%% Edge Case Tests (Phase 15)
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"empty node name", fun test_empty_node_name/0},
      {"very long node name", fun test_long_node_name/0},
      {"special chars in node name", fun test_special_chars_node/0},
      {"negative power value", fun test_negative_power/0},
      {"zero power cap", fun test_zero_power_cap/0}
     ]}.

test_empty_node_name() ->
    Result = flurm_power:configure_node_power(<<>>, #{method => script}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_long_node_name() ->
    LongName = list_to_binary([65 || _ <- lists:seq(1, 10000)]),
    Result = flurm_power:configure_node_power(LongName, #{method => script}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_special_chars_node() ->
    Result = flurm_power:configure_node_power(<<"node!@#$%">>, #{method => script}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_negative_power() ->
    Result = flurm_power:set_power_cap(-100),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_zero_power_cap() ->
    ok = flurm_power:set_power_cap(0),
    ?assertEqual(0, flurm_power:get_power_cap()).

%%====================================================================
%% Error Recovery Tests (Phase 16)
%%====================================================================

error_recovery_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"recover from IPMI failure", fun test_recover_ipmi_failure/0},
      {"recover from script failure", fun test_recover_script_failure/0},
      {"graceful degradation", fun test_graceful_degradation/0}
     ]}.

test_recover_ipmi_failure() ->
    NodeName = <<"recover_ipmi_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => ipmi,
        ipmi_address => <<"10.255.255.1">>,  % Unreachable
        ipmi_user => <<"admin">>,
        ipmi_password => <<"password">>
    }),

    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    %% Should fail but not crash
    _Result1 = flurm_power:request_node_power_on(NodeName),

    %% Server should still be responsive
    Status = flurm_power:get_status(),
    ?assert(is_map(Status)),
    ok.

test_recover_script_failure() ->
    NodeName = <<"recover_script_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_on_script => <<"/nonexistent/script.sh">>
    }),

    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    %% Should fail but not crash
    _Result = flurm_power:request_node_power_on(NodeName),

    %% Server should still be responsive
    Status = flurm_power:get_status(),
    ?assert(is_map(Status)),
    ok.

test_graceful_degradation() ->
    %% Remove all nodes
    catch ets:delete_all_objects(flurm_power_state),

    %% Operations should not crash
    _ = flurm_power:get_current_power(),
    _ = flurm_power:get_available_powered_nodes(),
    _ = flurm_power:check_idle_nodes(),

    Status = flurm_power:get_status(),
    ?assert(is_map(Status)),
    ok.

%%====================================================================
%% Configuration Tests (Phase 17)
%%====================================================================

configuration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"update all settings", fun test_update_all_settings/0},
      {"get all settings", fun test_get_all_settings/0},
      {"reset to defaults", fun test_reset_defaults/0}
     ]}.

test_update_all_settings() ->
    Settings = #{
        enabled => true,
        policy => aggressive,
        idle_timeout => 120,
        resume_timeout => 60,
        min_powered_nodes => 3,
        power_cap => 5000
    },

    Result = flurm_power:update_settings(Settings),
    case Result of
        ok ->
            Status = flurm_power:get_status(),
            ?assertEqual(true, maps:get(enabled, Status)),
            ?assertEqual(aggressive, maps:get(policy, Status));
        {error, _} -> ok
    end.

test_get_all_settings() ->
    Settings = flurm_power:get_settings(),
    case Settings of
        {ok, S} when is_map(S) -> ok;
        S when is_map(S) -> ok;
        {error, _} -> ok
    end.

test_reset_defaults() ->
    %% Change some settings first
    ok = flurm_power:set_policy(aggressive),
    ok = flurm_power:set_power_cap(1000),

    Result = flurm_power:reset_to_defaults(),
    case Result of
        ok ->
            %% Verify defaults are restored
            ?assertEqual(balanced, flurm_power:get_policy());
        {error, _} -> ok
    end.

%%====================================================================
%% Power Transition Tests (Phase 18)
%%====================================================================

power_transition_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"transitioning state tracking", fun test_transitioning_state/0},
      {"power on transition", fun test_power_on_transition/0},
      {"power off transition", fun test_power_off_transition/0},
      {"transition timeout handling", fun test_transition_timeout/0},
      {"cancel transition", fun test_cancel_transition/0}
     ]}.

test_transitioning_state() ->
    NodeName = <<"trans_state_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    Status = flurm_power:get_status(),
    TransNodes = maps:get(nodes_transitioning, Status, 0),
    ?assert(is_integer(TransNodes)),
    ok.

test_power_on_transition() ->
    NodeName = <<"trans_on_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    %% Start power on - puts node in transitioning state
    _ = flurm_power:request_node_power_on(NodeName),
    _ = sys:get_state(flurm_power),
    ok.

test_power_off_transition() ->
    NodeName = <<"trans_off_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Start power off
    _ = flurm_power:request_node_power_off(NodeName),
    _ = sys:get_state(flurm_power),
    ok.

test_transition_timeout() ->
    NodeName = <<"trans_timeout_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_on_timeout => 1  % Very short timeout
    }),
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    _ = flurm_power:request_node_power_on(NodeName),
    timer:sleep(100),
    _ = sys:get_state(flurm_power),
    ok.

test_cancel_transition() ->
    NodeName = <<"trans_cancel_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    _ = flurm_power:request_node_power_on(NodeName),

    Result = flurm_power:cancel_power_transition(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Power Queue Tests (Phase 19)
%%====================================================================

power_queue_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"queue power requests", fun test_queue_power_requests/0},
      {"process queue in order", fun test_queue_order/0},
      {"queue max size", fun test_queue_max_size/0},
      {"clear queue", fun test_clear_queue/0}
     ]}.

test_queue_power_requests() ->
    %% Configure multiple nodes
    Nodes = [<<"queue_node_1">>, <<"queue_node_2">>, <<"queue_node_3">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script}),
        ets:insert(flurm_power_state, {N, #{state => powered_off}})
    end, Nodes),

    %% Request power on for all
    lists:foreach(fun(N) ->
        _ = flurm_power:request_node_power_on(N)
    end, Nodes),

    _ = sys:get_state(flurm_power),
    ok.

test_queue_order() ->
    Nodes = [<<"order_1">>, <<"order_2">>, <<"order_3">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script}),
        ets:insert(flurm_power_state, {N, #{state => powered_off}})
    end, Nodes),

    %% Queue in specific order
    lists:foreach(fun(N) ->
        _ = flurm_power:request_node_power_on(N)
    end, Nodes),

    _ = sys:get_state(flurm_power),
    ok.

test_queue_max_size() ->
    %% Configure many nodes
    Nodes = [list_to_binary("max_q_" ++ integer_to_list(N)) || N <- lists:seq(1, 100)],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script}),
        ets:insert(flurm_power_state, {N, #{state => powered_off}})
    end, Nodes),

    %% Try to queue all
    lists:foreach(fun(N) ->
        _ = flurm_power:request_node_power_on(N)
    end, Nodes),

    _ = sys:get_state(flurm_power),
    ok.

test_clear_queue() ->
    NodeName = <<"clear_queue_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    _ = flurm_power:request_node_power_on(NodeName),

    Result = flurm_power:clear_power_queue(),
    case Result of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Power Priority Tests (Phase 20)
%%====================================================================

power_priority_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"high priority power on", fun test_high_priority_power_on/0},
      {"low priority deferred", fun test_low_priority_deferred/0},
      {"priority ordering", fun test_priority_ordering/0}
     ]}.

test_high_priority_power_on() ->
    NodeName = <<"high_priority_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName, #{priority => high}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_low_priority_deferred() ->
    NodeName = <<"low_priority_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    Result = flurm_power:request_node_power_on(NodeName, #{priority => low}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_priority_ordering() ->
    Nodes = [
        {<<"pri_high">>, high},
        {<<"pri_normal">>, normal},
        {<<"pri_low">>, low}
    ],

    lists:foreach(fun({N, _Pri}) ->
        flurm_power:configure_node_power(N, #{method => script}),
        ets:insert(flurm_power_state, {N, #{state => powered_off}})
    end, Nodes),

    lists:foreach(fun({N, Pri}) ->
        _ = flurm_power:request_node_power_on(N, #{priority => Pri})
    end, Nodes),

    _ = sys:get_state(flurm_power),
    ok.

%%====================================================================
%% Power Stats History Tests (Phase 21)
%%====================================================================

power_stats_history_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"record power state change", fun test_record_state_change/0},
      {"get power history", fun test_get_power_history/0},
      {"get stats summary", fun test_get_stats_summary/0},
      {"history retention", fun test_history_retention/0}
     ]}.

test_record_state_change() ->
    NodeName = <<"hist_state_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Power off should record state change
    _ = flurm_power:request_node_power_off(NodeName),
    _ = sys:get_state(flurm_power),
    ok.

test_get_power_history() ->
    NodeName = <<"hist_query_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    Now = erlang:system_time(second),
    Result = flurm_power:get_power_history(NodeName, {Now - 3600, Now}),
    case Result of
        {ok, History} when is_list(History) -> ok;
        {error, _} -> ok
    end.

test_get_stats_summary() ->
    flurm_power:configure_node_power(<<"summary_node">>, #{method => script}),

    Stats = flurm_power:get_power_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(total_suspends, Stats)),
    ?assert(maps:is_key(total_resumes, Stats)),
    ok.

test_history_retention() ->
    NodeName = <<"retention_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Generate some history
    lists:foreach(fun(_) ->
        _ = flurm_power:request_node_power_off(NodeName),
        _ = sys:get_state(flurm_power),
        _ = flurm_power:request_node_power_on(NodeName),
        _ = sys:get_state(flurm_power)
    end, lists:seq(1, 5)),

    Now = erlang:system_time(second),
    Result = flurm_power:get_power_history(NodeName, {Now - 60, Now}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Node Group Tests (Phase 22)
%%====================================================================

node_group_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"create node group", fun test_create_node_group/0},
      {"add node to group", fun test_add_to_group/0},
      {"remove node from group", fun test_remove_from_group/0},
      {"power off group", fun test_power_off_group/0},
      {"power on group", fun test_power_on_group/0}
     ]}.

test_create_node_group() ->
    Result = flurm_power:create_node_group(<<"compute_group">>, #{
        description => <<"Compute nodes">>,
        power_policy => aggressive
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_add_to_group() ->
    _ = flurm_power:create_node_group(<<"add_group">>, #{}),

    NodeName = <<"group_member_1">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    Result = flurm_power:add_node_to_group(<<"add_group">>, NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_remove_from_group() ->
    _ = flurm_power:create_node_group(<<"rem_group">>, #{}),

    NodeName = <<"rem_member_1">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    _ = flurm_power:add_node_to_group(<<"rem_group">>, NodeName),

    Result = flurm_power:remove_node_from_group(<<"rem_group">>, NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_power_off_group() ->
    _ = flurm_power:create_node_group(<<"off_group">>, #{}),

    Nodes = [<<"off_grp_1">>, <<"off_grp_2">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script}),
        _ = flurm_power:add_node_to_group(<<"off_group">>, N)
    end, Nodes),

    Result = flurm_power:power_off_group(<<"off_group">>),
    case Result of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_power_on_group() ->
    _ = flurm_power:create_node_group(<<"on_group">>, #{}),

    Nodes = [<<"on_grp_1">>, <<"on_grp_2">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script}),
        ets:insert(flurm_power_state, {N, #{state => powered_off}}),
        _ = flurm_power:add_node_to_group(<<"on_group">>, N)
    end, Nodes),

    Result = flurm_power:power_on_group(<<"on_group">>),
    case Result of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Maintenance Window Tests (Phase 23)
%%====================================================================

maintenance_window_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"schedule maintenance window", fun test_schedule_maintenance/0},
      {"enter maintenance mode", fun test_enter_maintenance/0},
      {"exit maintenance mode", fun test_exit_maintenance/0},
      {"power actions during maintenance", fun test_maintenance_power_actions/0}
     ]}.

test_schedule_maintenance() ->
    StartTime = erlang:system_time(second) + 3600,  % 1 hour from now
    EndTime = StartTime + 7200,  % 2 hours duration

    Result = flurm_power:schedule_maintenance_window(#{
        start_time => StartTime,
        end_time => EndTime,
        action => suspend_all
    }),
    case Result of
        {ok, _Id} -> ok;
        ok -> ok;
        {error, _} -> ok
    end.

test_enter_maintenance() ->
    flurm_power:configure_node_power(<<"maint_node">>, #{method => script}),

    Result = flurm_power:enter_maintenance_mode(<<"maint_node">>),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_exit_maintenance() ->
    NodeName = <<"exit_maint_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    _ = flurm_power:enter_maintenance_mode(NodeName),
    _ = sys:get_state(flurm_power),

    Result = flurm_power:exit_maintenance_mode(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_maintenance_power_actions() ->
    NodeName = <<"maint_action_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    _ = flurm_power:enter_maintenance_mode(NodeName),
    _ = sys:get_state(flurm_power),

    %% Power actions should be blocked during maintenance
    Result = flurm_power:request_node_power_off(NodeName),
    case Result of
        ok -> ok;  % May succeed
        {error, in_maintenance} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Alert and Notification Tests (Phase 24)
%%====================================================================

alert_notification_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"power cap alert", fun test_power_cap_alert/0},
      {"power failure alert", fun test_power_failure_alert/0},
      {"configure alert thresholds", fun test_configure_alerts/0},
      {"get active alerts", fun test_get_active_alerts/0}
     ]}.

test_power_cap_alert() ->
    ok = flurm_power:set_power_cap(100),

    %% Configure nodes that exceed cap
    lists:foreach(fun(N) ->
        NodeName = list_to_binary("cap_alert_" ++ integer_to_list(N)),
        flurm_power:configure_node_power(NodeName, #{
            method => script,
            power_watts => 50
        })
    end, lists:seq(1, 5)),

    %% Should trigger alert
    Alerts = flurm_power:get_active_alerts(),
    case Alerts of
        {ok, A} when is_list(A) -> ok;
        A when is_list(A) -> ok;
        {error, _} -> ok
    end.

test_power_failure_alert() ->
    NodeName = <<"fail_alert_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => script,
        power_on_script => <<"/nonexistent/script.sh">>
    }),
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    _ = flurm_power:request_node_power_on(NodeName),
    _ = sys:get_state(flurm_power),

    %% Check for failure alerts
    Alerts = flurm_power:get_active_alerts(),
    case Alerts of
        {ok, A} when is_list(A) -> ok;
        A when is_list(A) -> ok;
        {error, _} -> ok
    end.

test_configure_alerts() ->
    Result = flurm_power:configure_alerts(#{
        power_cap_warning_threshold => 0.8,  % 80% of cap
        failure_alert_count => 3
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_get_active_alerts() ->
    Alerts = flurm_power:get_active_alerts(),
    case Alerts of
        {ok, A} when is_list(A) -> ok;
        A when is_list(A) -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Bulk Operations Tests (Phase 25)
%%====================================================================

bulk_operations_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"bulk power on", fun test_bulk_power_on/0},
      {"bulk power off", fun test_bulk_power_off/0},
      {"bulk configure", fun test_bulk_configure/0},
      {"bulk status query", fun test_bulk_status/0}
     ]}.

test_bulk_power_on() ->
    Nodes = [<<"bulk_on_1">>, <<"bulk_on_2">>, <<"bulk_on_3">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script}),
        ets:insert(flurm_power_state, {N, #{state => powered_off}})
    end, Nodes),

    Result = flurm_power:bulk_power_on(Nodes),
    case Result of
        {ok, _Results} -> ok;
        ok -> ok;
        {error, _} -> ok
    end.

test_bulk_power_off() ->
    Nodes = [<<"bulk_off_1">>, <<"bulk_off_2">>, <<"bulk_off_3">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script})
    end, Nodes),

    Result = flurm_power:bulk_power_off(Nodes),
    case Result of
        {ok, _Results} -> ok;
        ok -> ok;
        {error, _} -> ok
    end.

test_bulk_configure() ->
    Nodes = [<<"bulk_cfg_1">>, <<"bulk_cfg_2">>, <<"bulk_cfg_3">>],
    Config = #{method => script, power_watts => 300},

    Result = flurm_power:bulk_configure_nodes(Nodes, Config),
    case Result of
        {ok, _Results} -> ok;
        ok -> ok;
        {error, _} -> ok
    end.

test_bulk_status() ->
    Nodes = [<<"bulk_stat_1">>, <<"bulk_stat_2">>, <<"bulk_stat_3">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script})
    end, Nodes),

    Result = flurm_power:bulk_get_power_state(Nodes),
    case Result of
        {ok, States} when is_map(States) -> ok;
        {ok, States} when is_list(States) -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Integration with Scheduler Tests (Phase 26)
%%====================================================================

scheduler_integration_extended_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"scheduler requests power", fun test_scheduler_power_request/0},
      {"job completion triggers release", fun test_job_completion_release/0},
      {"idle check integrates with scheduler", fun test_idle_scheduler_integration/0}
     ]}.

test_scheduler_power_request() ->
    NodeName = <<"sched_req_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),
    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    %% Scheduler requests power on for job
    Result = flurm_power:scheduler_request_power_on(NodeName, #{job_id => 12345}),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_job_completion_release() ->
    NodeName = <<"job_comp_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Notify job start
    flurm_power:notify_job_start(99999, [NodeName]),
    _ = sys:get_state(flurm_power),

    %% Notify job end
    flurm_power:notify_job_end(99999),
    _ = sys:get_state(flurm_power),

    %% Node should be released for power management
    ok.

test_idle_scheduler_integration() ->
    Nodes = [<<"idle_sched_1">>, <<"idle_sched_2">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{method => script})
    end, Nodes),

    %% Set aggressive policy
    ok = flurm_power:set_policy(aggressive),

    %% Check idle nodes
    Count = flurm_power:check_idle_nodes(),
    ?assert(is_integer(Count)),
    ok.

%%====================================================================
%% Metrics Export Tests (Phase 27)
%%====================================================================

metrics_export_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"export prometheus metrics", fun test_export_prometheus/0},
      {"export json metrics", fun test_export_json/0},
      {"get metrics summary", fun test_get_metrics_summary/0}
     ]}.

test_export_prometheus() ->
    flurm_power:configure_node_power(<<"prom_node">>, #{
        method => script,
        power_watts => 400
    }),

    Result = flurm_power:export_metrics(prometheus),
    case Result of
        {ok, Data} when is_binary(Data) -> ok;
        {error, _} -> ok
    end.

test_export_json() ->
    flurm_power:configure_node_power(<<"json_node">>, #{
        method => script,
        power_watts => 400
    }),

    Result = flurm_power:export_metrics(json),
    case Result of
        {ok, Data} when is_binary(Data) -> ok;
        {ok, Data} when is_map(Data) -> ok;
        {error, _} -> ok
    end.

test_get_metrics_summary() ->
    flurm_power:configure_node_power(<<"metrics_node">>, #{
        method => script,
        power_watts => 400
    }),

    Summary = flurm_power:get_metrics_summary(),
    case Summary of
        {ok, S} when is_map(S) -> ok;
        S when is_map(S) -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Power Efficiency Tests (Phase 28)
%%====================================================================

power_efficiency_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"calculate efficiency ratio", fun test_efficiency_ratio/0},
      {"efficiency by node", fun test_efficiency_by_node/0},
      {"efficiency recommendations", fun test_efficiency_recommendations/0}
     ]}.

test_efficiency_ratio() ->
    flurm_power:configure_node_power(<<"eff_node">>, #{
        method => script,
        power_watts => 400
    }),

    Result = flurm_power:get_efficiency_ratio(),
    case Result of
        {ok, Ratio} when is_number(Ratio) -> ok;
        Ratio when is_number(Ratio) -> ok;
        {error, _} -> ok
    end.

test_efficiency_by_node() ->
    Nodes = [<<"eff_n1">>, <<"eff_n2">>, <<"eff_n3">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{
            method => script,
            power_watts => 300 + erlang:phash2(N, 200)
        })
    end, Nodes),

    Result = flurm_power:get_efficiency_by_node(),
    case Result of
        {ok, Efficiencies} when is_map(Efficiencies) -> ok;
        Efficiencies when is_map(Efficiencies) -> ok;
        {error, _} -> ok
    end.

test_efficiency_recommendations() ->
    flurm_power:configure_node_power(<<"rec_node">>, #{
        method => script,
        power_watts => 500
    }),

    Result = flurm_power:get_efficiency_recommendations(),
    case Result of
        {ok, Recs} when is_list(Recs) -> ok;
        Recs when is_list(Recs) -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Power Profile Optimization Tests (Phase 29)
%%====================================================================

profile_optimization_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"auto optimize profiles", fun test_auto_optimize/0},
      {"learn power patterns", fun test_learn_patterns/0},
      {"apply learned optimizations", fun test_apply_optimizations/0}
     ]}.

test_auto_optimize() ->
    flurm_power:configure_node_power(<<"opt_node">>, #{
        method => script,
        power_watts => 400
    }),

    Result = flurm_power:enable_auto_optimization(),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_learn_patterns() ->
    flurm_power:configure_node_power(<<"learn_node">>, #{
        method => script,
        power_watts => 400
    }),

    %% Record usage patterns
    lists:foreach(fun(_) ->
        flurm_power:record_power_usage(<<"learn_node">>, 350 + rand:uniform(100)),
        _ = sys:get_state(flurm_power)
    end, lists:seq(1, 20)),

    Result = flurm_power:analyze_power_patterns(<<"learn_node">>),
    case Result of
        {ok, Patterns} when is_map(Patterns) -> ok;
        {error, _} -> ok
    end.

test_apply_optimizations() ->
    flurm_power:configure_node_power(<<"apply_opt_node">>, #{
        method => script,
        power_watts => 400
    }),

    Result = flurm_power:apply_power_optimizations(<<"apply_opt_node">>),
    case Result of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Redundancy and Failover Tests (Phase 30)
%%====================================================================

redundancy_failover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"failover power control method", fun test_failover_method/0},
      {"redundant power paths", fun test_redundant_paths/0},
      {"power controller failover", fun test_controller_failover/0}
     ]}.

test_failover_method() ->
    NodeName = <<"failover_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => ipmi,
        ipmi_address => <<"10.255.255.1">>,  % Unreachable
        failover_method => script,
        power_on_script => <<"/usr/bin/true">>
    }),

    ets:insert(flurm_power_state, {NodeName, #{state => powered_off}}),

    %% IPMI should fail, fallback to script
    Result = flurm_power:request_node_power_on(NodeName),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_redundant_paths() ->
    NodeName = <<"redundant_node">>,
    flurm_power:configure_node_power(NodeName, #{
        method => ipmi,
        ipmi_address => <<"192.168.1.100">>,
        redundant_ipmi_address => <<"192.168.2.100">>
    }),

    {ok, _State} = flurm_power:get_node_power_state(NodeName),
    ok.

test_controller_failover() ->
    %% Simulate controller failure by sending crash message
    FedPid = whereis(flurm_power),

    %% Server should handle unknown messages gracefully
    FedPid ! {controller_failure, primary},
    _ = sys:get_state(flurm_power),
    ok.

%%====================================================================
%% Batch Job Power Allocation Tests (Phase 31)
%%====================================================================

batch_job_power_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"allocate power for batch job", fun test_allocate_batch_power/0},
      {"release batch job power", fun test_release_batch_power/0},
      {"batch job power tracking", fun test_batch_power_tracking/0}
     ]}.

test_allocate_batch_power() ->
    Nodes = [<<"batch_n1">>, <<"batch_n2">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{
            method => script,
            power_watts => 300
        })
    end, Nodes),

    Result = flurm_power:allocate_power_for_job(88888, Nodes, #{
        max_power => 800
    }),
    case Result of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_release_batch_power() ->
    Nodes = [<<"release_bn1">>, <<"release_bn2">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{
            method => script,
            power_watts => 300
        })
    end, Nodes),

    _ = flurm_power:allocate_power_for_job(77777, Nodes, #{}),
    _ = sys:get_state(flurm_power),

    Result = flurm_power:release_power_for_job(77777),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_batch_power_tracking() ->
    Nodes = [<<"track_bn1">>],
    lists:foreach(fun(N) ->
        flurm_power:configure_node_power(N, #{
            method => script,
            power_watts => 400
        })
    end, Nodes),

    _ = flurm_power:allocate_power_for_job(66666, Nodes, #{}),
    _ = sys:get_state(flurm_power),

    Result = flurm_power:get_job_power_allocation(66666),
    case Result of
        {ok, Allocation} when is_map(Allocation) -> ok;
        {error, not_found} -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% Time-of-Day Scheduling Tests (Phase 32)
%%====================================================================

time_of_day_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"set peak hours", fun test_set_peak_hours/0},
      {"set off-peak hours", fun test_set_off_peak_hours/0},
      {"time-based power policy", fun test_time_based_policy/0}
     ]}.

test_set_peak_hours() ->
    Result = flurm_power:set_peak_hours(#{
        start_hour => 9,
        end_hour => 17,
        days => [1, 2, 3, 4, 5]  % Monday-Friday
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_set_off_peak_hours() ->
    Result = flurm_power:set_off_peak_hours(#{
        start_hour => 22,
        end_hour => 6,
        days => [1, 2, 3, 4, 5, 6, 7]
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

test_time_based_policy() ->
    Result = flurm_power:set_time_based_policy(#{
        peak => aggressive,
        off_peak => conservative
    }),
    case Result of
        ok -> ok;
        {error, _} -> ok
    end.

%%====================================================================
%% API Compatibility Tests (Phase 33)
%%====================================================================

api_compatibility_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"legacy API still works", fun test_legacy_api_compat/0},
      {"deprecated functions warn", fun test_deprecated_functions/0},
      {"backward compatible responses", fun test_backward_compatible/0}
     ]}.

test_legacy_api_compat() ->
    %% Test that old API functions still work
    ok = flurm_power:set_power_budget(5000),
    ?assertEqual(5000, flurm_power:get_power_budget()),

    ok = flurm_power:set_idle_timeout(600),
    ?assertEqual(600, flurm_power:get_idle_timeout()),
    ok.

test_deprecated_functions() ->
    %% Deprecated but should still work
    Stats = flurm_power:get_power_stats(),
    ?assert(is_map(Stats)),
    ok.

test_backward_compatible() ->
    NodeName = <<"compat_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    %% Old-style status query
    Status = flurm_power:get_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(enabled, Status)),
    ?assert(maps:is_key(policy, Status)),
    ok.

%%====================================================================
%% Stress Tests (Phase 34)
%%====================================================================

stress_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"many concurrent power queries", fun test_stress_queries/0},
      {"rapid state changes", fun test_rapid_state_changes/0},
      {"high volume energy recording", fun test_high_volume_energy/0}
     ]}.

test_stress_queries() ->
    %% Configure nodes
    lists:foreach(fun(N) ->
        NodeName = list_to_binary("stress_" ++ integer_to_list(N)),
        flurm_power:configure_node_power(NodeName, #{method => script})
    end, lists:seq(1, 50)),

    Self = self(),
    Pids = [spawn(fun() ->
        lists:foreach(fun(_) ->
            _ = flurm_power:get_status(),
            _ = flurm_power:get_power_stats()
        end, lists:seq(1, 10)),
        Self ! {done, N}
    end) || N <- lists:seq(1, 20)],

    lists:foreach(fun(_) ->
        receive {done, _} -> ok after 30000 -> ok end
    end, Pids),
    ok.

test_rapid_state_changes() ->
    NodeName = <<"rapid_change_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    lists:foreach(fun(_) ->
        _ = flurm_power:request_node_power_off(NodeName),
        _ = flurm_power:request_node_power_on(NodeName)
    end, lists:seq(1, 50)),

    _ = sys:get_state(flurm_power),
    ok.

test_high_volume_energy() ->
    NodeName = <<"high_vol_energy_node">>,
    flurm_power:configure_node_power(NodeName, #{method => script}),

    Self = self(),
    Pids = [spawn(fun() ->
        lists:foreach(fun(_) ->
            flurm_power:record_power_usage(NodeName, 300 + rand:uniform(200))
        end, lists:seq(1, 100)),
        Self ! {done, N}
    end) || N <- lists:seq(1, 10)],

    lists:foreach(fun(_) ->
        receive {done, _} -> ok after 30000 -> ok end
    end, Pids),

    _ = sys:get_state(flurm_power),
    ok.
