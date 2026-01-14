%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_node_manager_server
%%%
%%% These tests call the actual module functions directly to get
%%% code coverage. External dependencies are mocked with meck.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_manager_server_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

node_manager_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init starts with empty nodes", fun test_init/0},
      {"register_node adds node", fun test_register_node/0},
      {"update_node modifies node", fun test_update_node/0},
      {"update_node returns error for unknown", fun test_update_node_not_found/0},
      {"get_node returns node", fun test_get_node/0},
      {"get_node returns error for unknown", fun test_get_node_not_found/0},
      {"list_nodes returns all nodes", fun test_list_nodes/0},
      {"get_available_nodes filters by state", fun test_get_available_nodes/0},
      {"get_available_nodes_for_job filters by resources", fun test_get_available_nodes_for_job/0},
      {"allocate_resources allocates to node", fun test_allocate_resources/0},
      {"allocate_resources fails for insufficient resources", fun test_allocate_resources_insufficient/0},
      {"allocate_resources fails for unknown node", fun test_allocate_resources_unknown_node/0},
      {"drain_node sets drain state", fun test_drain_node/0},
      {"drain_node fails for unknown node", fun test_drain_node_not_found/0},
      {"undrain_node resumes node", fun test_undrain_node/0},
      {"undrain_node fails if not draining", fun test_undrain_node_not_draining/0},
      {"undrain_node fails for unknown node", fun test_undrain_node_not_found/0},
      {"get_drain_reason returns reason", fun test_get_drain_reason/0},
      {"get_drain_reason fails if not draining", fun test_get_drain_reason_not_draining/0},
      {"is_node_draining returns true for drained", fun test_is_node_draining_true/0},
      {"is_node_draining returns false for not drained", fun test_is_node_draining_false/0},
      {"handle_cast heartbeat updates node", fun test_heartbeat/0},
      {"handle_cast heartbeat for unknown node", fun test_heartbeat_unknown/0},
      {"handle_cast release_resources releases resources", fun test_release_resources/0},
      {"handle_info check_heartbeats marks stale nodes down", fun test_check_heartbeats/0},
      {"handle_call unknown returns error", fun test_unknown_call/0},
      {"handle_cast unknown is ignored", fun test_unknown_cast/0},
      {"handle_info unknown is ignored", fun test_unknown_info/0},
      {"terminate returns ok", fun test_terminate/0}
     ]}.

setup() ->
    meck:new(flurm_core, [passthrough, non_strict]),
    meck:new(flurm_scheduler, [passthrough, non_strict]),
    meck:new(flurm_config_server, [passthrough, non_strict]),
    meck:new(flurm_gres, [passthrough, non_strict]),

    meck:expect(flurm_core, new_node, fun(Spec) ->
        #node{
            hostname = maps:get(hostname, Spec, <<"unknown">>),
            cpus = maps:get(cpus, Spec, 8),
            memory_mb = maps:get(memory_mb, Spec, 16384),
            state = maps:get(state, Spec, up),
            features = maps:get(features, Spec, []),
            partitions = maps:get(partitions, Spec, [<<"default">>]),
            running_jobs = [],
            load_avg = 0.0,
            free_memory_mb = maps:get(memory_mb, Spec, 16384),
            allocations = #{},
            gres_config = [],
            gres_available = #{},
            gres_total = #{},
            gres_allocations = #{}
        }
    end),
    meck:expect(flurm_core, node_hostname, fun(#node{hostname = H}) -> H end),
    meck:expect(flurm_core, node_state, fun(#node{state = S}) -> S end),
    meck:expect(flurm_core, update_node_state, fun(N, S) -> N#node{state = S} end),
    meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
    meck:expect(flurm_config_server, subscribe_changes, fun(_) -> ok end),
    meck:expect(flurm_gres, parse_gres_string, fun(_) -> {ok, []} end),
    meck:expect(flurm_gres, register_node_gres, fun(_, _) -> ok end),
    meck:expect(flurm_gres, deallocate, fun(_, _) -> ok end),
    ok.

cleanup(_) ->
    meck:unload(flurm_core),
    meck:unload(flurm_scheduler),
    meck:unload(flurm_config_server),
    meck:unload(flurm_gres),
    ok.

%% State record matching the module's internal state
-record(state, {
    nodes = #{} :: #{binary() => #node{}}
}).

%%====================================================================
%% Test Cases
%%====================================================================

test_init() ->
    {ok, State} = flurm_node_manager_server:init([]),
    ?assertMatch(#state{}, State),
    ?assertEqual(#{}, State#state.nodes),
    %% Flush check_heartbeats timer message
    receive check_heartbeats -> ok after 100 -> ok end.

test_register_node() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384},
    {reply, Result, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    ?assertEqual(ok, Result),
    ?assert(maps:is_key(<<"node1">>, State1#state.nodes)).

test_update_node() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, State2} = flurm_node_manager_server:handle_call(
        {update_node, <<"node1">>, #{load_avg => 1.5}}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    #{<<"node1">> := Node} = State2#state.nodes,
    ?assertEqual(1.5, Node#node.load_avg).

test_update_node_not_found() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    {reply, Result, _State1} = flurm_node_manager_server:handle_call(
        {update_node, <<"nonexistent">>, #{load_avg => 1.5}}, {self(), make_ref()}, State0),
    ?assertEqual({error, not_found}, Result).

test_get_node() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_node_manager_server:handle_call(
        {get_node, <<"node1">>}, {self(), make_ref()}, State1),
    ?assertMatch({ok, #node{hostname = <<"node1">>}}, Result).

test_get_node_not_found() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    {reply, Result, _State1} = flurm_node_manager_server:handle_call(
        {get_node, <<"nonexistent">>}, {self(), make_ref()}, State0),
    ?assertEqual({error, not_found}, Result).

test_list_nodes() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec1 = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384},
    NodeSpec2 = #{hostname => <<"node2">>, cpus => 16, memory_mb => 32768},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec1}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec2}, {self(), make_ref()}, State1),
    {reply, Result, _State3} = flurm_node_manager_server:handle_call(
        list_nodes, {self(), make_ref()}, State2),
    ?assert(is_list(Result)),
    ?assertEqual(2, length(Result)).

test_get_available_nodes() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_node_manager_server:handle_call(
        get_available_nodes, {self(), make_ref()}, State1),
    ?assert(is_list(Result)),
    ?assertEqual(1, length(Result)).

test_get_available_nodes_for_job() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_node_manager_server:handle_call(
        {get_available_nodes_for_job, 4, 8192, <<"default">>}, {self(), make_ref()}, State1),
    ?assert(is_list(Result)),
    ?assertEqual(1, length(Result)).

test_allocate_resources() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, State2} = flurm_node_manager_server:handle_call(
        {allocate_resources, <<"node1">>, 1, 4, 8192}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    #{<<"node1">> := Node} = State2#state.nodes,
    ?assert(lists:member(1, Node#node.running_jobs)).

test_allocate_resources_insufficient() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_node_manager_server:handle_call(
        {allocate_resources, <<"node1">>, 1, 100, 8192}, {self(), make_ref()}, State1),
    ?assertEqual({error, insufficient_resources}, Result).

test_allocate_resources_unknown_node() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    {reply, Result, _State1} = flurm_node_manager_server:handle_call(
        {allocate_resources, <<"nonexistent">>, 1, 4, 8192}, {self(), make_ref()}, State0),
    ?assertEqual({error, node_not_found}, Result).

test_drain_node() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, State2} = flurm_node_manager_server:handle_call(
        {drain_node, <<"node1">>, <<"maintenance">>}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    #{<<"node1">> := Node} = State2#state.nodes,
    ?assertEqual(drain, Node#node.state),
    ?assertEqual(<<"maintenance">>, Node#node.drain_reason).

test_drain_node_not_found() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    {reply, Result, _State1} = flurm_node_manager_server:handle_call(
        {drain_node, <<"nonexistent">>, <<"maintenance">>}, {self(), make_ref()}, State0),
    ?assertEqual({error, not_found}, Result).

test_undrain_node() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_node_manager_server:handle_call(
        {drain_node, <<"node1">>, <<"maintenance">>}, {self(), make_ref()}, State1),
    {reply, Result, State3} = flurm_node_manager_server:handle_call(
        {undrain_node, <<"node1">>}, {self(), make_ref()}, State2),
    ?assertEqual(ok, Result),
    #{<<"node1">> := Node} = State3#state.nodes,
    ?assertEqual(idle, Node#node.state).

test_undrain_node_not_draining() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_node_manager_server:handle_call(
        {undrain_node, <<"node1">>}, {self(), make_ref()}, State1),
    ?assertEqual({error, not_draining}, Result).

test_undrain_node_not_found() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    {reply, Result, _State1} = flurm_node_manager_server:handle_call(
        {undrain_node, <<"nonexistent">>}, {self(), make_ref()}, State0),
    ?assertEqual({error, not_found}, Result).

test_get_drain_reason() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_node_manager_server:handle_call(
        {drain_node, <<"node1">>, <<"maintenance">>}, {self(), make_ref()}, State1),
    {reply, Result, _State3} = flurm_node_manager_server:handle_call(
        {get_drain_reason, <<"node1">>}, {self(), make_ref()}, State2),
    ?assertEqual({ok, <<"maintenance">>}, Result).

test_get_drain_reason_not_draining() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_node_manager_server:handle_call(
        {get_drain_reason, <<"node1">>}, {self(), make_ref()}, State1),
    ?assertEqual({error, not_draining}, Result).

test_is_node_draining_true() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_node_manager_server:handle_call(
        {drain_node, <<"node1">>, <<"maintenance">>}, {self(), make_ref()}, State1),
    {reply, Result, _State3} = flurm_node_manager_server:handle_call(
        {is_node_draining, <<"node1">>}, {self(), make_ref()}, State2),
    ?assertEqual(true, Result).

test_is_node_draining_false() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_node_manager_server:handle_call(
        {is_node_draining, <<"node1">>}, {self(), make_ref()}, State1),
    ?assertEqual(false, Result).

test_heartbeat() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    HeartbeatData = #{hostname => <<"node1">>, load_avg => 2.5, free_memory_mb => 8192},
    {noreply, State2} = flurm_node_manager_server:handle_cast(
        {heartbeat, HeartbeatData}, State1),
    #{<<"node1">> := Node} = State2#state.nodes,
    ?assertEqual(2.5, Node#node.load_avg).

test_heartbeat_unknown() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    HeartbeatData = #{hostname => <<"unknown">>, load_avg => 2.5},
    {noreply, State1} = flurm_node_manager_server:handle_cast(
        {heartbeat, HeartbeatData}, State0),
    ?assertEqual(State0, State1).

test_release_resources() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_node_manager_server:handle_call(
        {allocate_resources, <<"node1">>, 1, 4, 8192}, {self(), make_ref()}, State1),
    {noreply, State3} = flurm_node_manager_server:handle_cast(
        {release_resources, <<"node1">>, 1}, State2),
    #{<<"node1">> := Node} = State3#state.nodes,
    ?assertEqual(false, lists:member(1, Node#node.running_jobs)).

test_check_heartbeats() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    %% Create a node with old heartbeat
    OldTime = erlang:system_time(second) - 120,  %% 2 minutes ago
    Node = #node{
        hostname = <<"stale_node">>,
        cpus = 8, memory_mb = 16384,
        state = idle, features = [], partitions = [],
        running_jobs = [], load_avg = 0.0, free_memory_mb = 16384,
        last_heartbeat = OldTime, allocations = #{},
        gres_config = [], gres_available = #{}, gres_total = #{}, gres_allocations = #{}
    },
    State1 = State0#state{nodes = #{<<"stale_node">> => Node}},
    {noreply, State2} = flurm_node_manager_server:handle_info(check_heartbeats, State1),
    #{<<"stale_node">> := UpdatedNode} = State2#state.nodes,
    ?assertEqual(down, UpdatedNode#node.state),
    %% Flush next timer
    receive check_heartbeats -> ok after 100 -> ok end.

test_unknown_call() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    {reply, Result, _State1} = flurm_node_manager_server:handle_call(
        unknown_request, {self(), make_ref()}, State0),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    {noreply, State1} = flurm_node_manager_server:handle_cast(unknown, State0),
    ?assertEqual(State0, State1).

test_unknown_info() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    {noreply, State1} = flurm_node_manager_server:handle_info(unknown, State0),
    ?assertEqual(State0, State1).

test_terminate() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    Result = flurm_node_manager_server:terminate(normal, State0),
    ?assertEqual(ok, Result).

%%====================================================================
%% Dynamic Node Operations Tests
%%====================================================================

dynamic_node_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"add_node adds new node", fun test_add_node/0},
      {"add_node rejects duplicate", fun test_add_node_duplicate/0},
      {"remove_node force removes node", fun test_remove_node_force/0},
      {"update_node_properties updates node", fun test_update_node_properties/0},
      {"get_running_jobs_on_node returns jobs", fun test_get_running_jobs_on_node/0}
     ]}.

test_add_node() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"new_node">>, cpus => 8, memory_mb => 16384},
    {reply, Result, State1} = flurm_node_manager_server:handle_call(
        {add_node, NodeSpec}, {self(), make_ref()}, State0),
    ?assertEqual(ok, Result),
    ?assert(maps:is_key(<<"new_node">>, State1#state.nodes)).

test_add_node_duplicate() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {add_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_node_manager_server:handle_call(
        {add_node, NodeSpec}, {self(), make_ref()}, State1),
    ?assertEqual({error, already_registered}, Result).

test_remove_node_force() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {add_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, State2} = flurm_node_manager_server:handle_call(
        {remove_node, <<"node1">>, force}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    ?assertEqual(false, maps:is_key(<<"node1">>, State2#state.nodes)).

test_update_node_properties() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {add_node, NodeSpec}, {self(), make_ref()}, State0),
    Updates = #{cpus => 16, memory_mb => 32768},
    {reply, Result, State2} = flurm_node_manager_server:handle_call(
        {update_node_properties, <<"node1">>, Updates}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    #{<<"node1">> := Node} = State2#state.nodes,
    ?assertEqual(16, Node#node.cpus),
    ?assertEqual(32768, Node#node.memory_mb).

test_get_running_jobs_on_node() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {add_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_node_manager_server:handle_call(
        {allocate_resources, <<"node1">>, 1, 4, 8192}, {self(), make_ref()}, State1),
    {reply, Result, _State3} = flurm_node_manager_server:handle_call(
        {get_running_jobs_on_node, <<"node1">>}, {self(), make_ref()}, State2),
    ?assertEqual({ok, [1]}, Result).

%%====================================================================
%% GRES Tests
%%====================================================================

gres_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"register_node_gres registers GRES", fun test_register_node_gres/0},
      {"get_node_gres returns GRES info", fun test_get_node_gres/0},
      {"get_available_nodes_with_gres filters by GRES", fun test_get_available_nodes_with_gres/0}
     ]}.

test_register_node_gres() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    GRESList = [#{type => gpu, name => <<"a100">>, count => 4}],
    {reply, Result, State2} = flurm_node_manager_server:handle_call(
        {register_node_gres, <<"node1">>, GRESList}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    #{<<"node1">> := Node} = State2#state.nodes,
    ?assertEqual(GRESList, Node#node.gres_config).

test_get_node_gres() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    GRESList = [#{type => gpu, name => <<"a100">>, count => 4}],
    {reply, ok, State2} = flurm_node_manager_server:handle_call(
        {register_node_gres, <<"node1">>, GRESList}, {self(), make_ref()}, State1),
    {reply, Result, _State3} = flurm_node_manager_server:handle_call(
        {get_node_gres, <<"node1">>}, {self(), make_ref()}, State2),
    ?assertMatch({ok, #{config := _, available := _, total := _}}, Result).

test_get_available_nodes_with_gres() ->
    {ok, State0} = flurm_node_manager_server:init([]),
    NodeSpec = #{hostname => <<"node1">>, cpus => 8, memory_mb => 16384, state => idle},
    {reply, ok, State1} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_node_manager_server:handle_call(
        {get_available_nodes_with_gres, 4, 8192, <<"default">>, <<>>}, {self(), make_ref()}, State1),
    ?assert(is_list(Result)),
    ?assertEqual(1, length(Result)).
