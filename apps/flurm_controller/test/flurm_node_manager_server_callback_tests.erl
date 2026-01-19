%%%-------------------------------------------------------------------
%%% @doc Callback tests for flurm_node_manager_server gen_server
%%%
%%% Tests the gen_server callbacks directly without starting the process.
%%% This allows testing the callback logic in isolation.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_manager_server_callback_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

make_test_node(Hostname) ->
    make_test_node(Hostname, 16, 32768).

make_test_node(Hostname, Cpus, MemoryMb) ->
    #node{
        hostname = Hostname,
        cpus = Cpus,
        memory_mb = MemoryMb,
        state = idle,
        drain_reason = undefined,
        features = [<<"gpu">>],
        partitions = [<<"default">>, <<"compute">>],
        running_jobs = [],
        load_avg = 0.1,
        free_memory_mb = MemoryMb,
        last_heartbeat = erlang:system_time(second),
        allocations = #{}
    }.

make_state() ->
    make_state(#{}).

make_state(Nodes) ->
    {state, Nodes}.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_returns_empty_state_test() ->
    %% init/1 requires lager and sends messages, so we just verify
    %% the pattern - in production the process handles the timer
    {ok, State} = flurm_node_manager_server:init([]),
    ?assertMatch({state, #{}}, State).

%%====================================================================
%% handle_call Tests - register_node
%%====================================================================

handle_call_register_node_test() ->
    State = make_state(),
    NodeSpec = #{
        hostname => <<"node001">>,
        cpus => 16,
        memory_mb => 32768,
        features => [],
        partitions => [<<"default">>]
    },
    {reply, ok, NewState} = flurm_node_manager_server:handle_call(
        {register_node, NodeSpec}, {self(), make_ref()}, State
    ),
    {state, Nodes} = NewState,
    ?assert(maps:is_key(<<"node001">>, Nodes)).

%%====================================================================
%% handle_call Tests - get_node
%%====================================================================

handle_call_get_node_found_test() ->
    Node = make_test_node(<<"node001">>),
    State = make_state(#{<<"node001">> => Node}),
    {reply, {ok, FoundNode}, _NewState} = flurm_node_manager_server:handle_call(
        {get_node, <<"node001">>}, {self(), make_ref()}, State
    ),
    ?assertEqual(<<"node001">>, FoundNode#node.hostname).

handle_call_get_node_not_found_test() ->
    State = make_state(),
    {reply, {error, not_found}, _NewState} = flurm_node_manager_server:handle_call(
        {get_node, <<"unknown">>}, {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - list_nodes
%%====================================================================

handle_call_list_nodes_empty_test() ->
    State = make_state(),
    {reply, NodeList, _NewState} = flurm_node_manager_server:handle_call(
        list_nodes, {self(), make_ref()}, State
    ),
    ?assertEqual([], NodeList).

handle_call_list_nodes_with_nodes_test() ->
    Node1 = make_test_node(<<"node001">>),
    Node2 = make_test_node(<<"node002">>),
    State = make_state(#{<<"node001">> => Node1, <<"node002">> => Node2}),
    {reply, NodeList, _NewState} = flurm_node_manager_server:handle_call(
        list_nodes, {self(), make_ref()}, State
    ),
    ?assertEqual(2, length(NodeList)).

%%====================================================================
%% handle_call Tests - get_available_nodes
%%====================================================================

handle_call_get_available_nodes_idle_test() ->
    Node1 = make_test_node(<<"node001">>),
    Node2 = (make_test_node(<<"node002">>))#node{state = down},
    State = make_state(#{<<"node001">> => Node1, <<"node002">> => Node2}),
    {reply, Available, _NewState} = flurm_node_manager_server:handle_call(
        get_available_nodes, {self(), make_ref()}, State
    ),
    ?assertEqual(1, length(Available)),
    ?assertEqual(<<"node001">>, (hd(Available))#node.hostname).

handle_call_get_available_nodes_mixed_test() ->
    Node = (make_test_node(<<"node001">>))#node{state = mixed},
    State = make_state(#{<<"node001">> => Node}),
    {reply, Available, _NewState} = flurm_node_manager_server:handle_call(
        get_available_nodes, {self(), make_ref()}, State
    ),
    ?assertEqual(1, length(Available)).

%%====================================================================
%% handle_call Tests - get_available_nodes_for_job
%%====================================================================

handle_call_get_available_nodes_for_job_test() ->
    Node1 = make_test_node(<<"node001">>, 16, 32768),
    Node2 = make_test_node(<<"node002">>, 4, 8192),
    State = make_state(#{<<"node001">> => Node1, <<"node002">> => Node2}),
    %% Request 8 CPUs and 16GB - only node001 should match
    {reply, Available, _NewState} = flurm_node_manager_server:handle_call(
        {get_available_nodes_for_job, 8, 16384, <<"default">>},
        {self(), make_ref()}, State
    ),
    ?assertEqual(1, length(Available)),
    ?assertEqual(<<"node001">>, (hd(Available))#node.hostname).

handle_call_get_available_nodes_for_job_partition_filter_test() ->
    Node1 = make_test_node(<<"node001">>),
    Node2 = (make_test_node(<<"node002">>))#node{partitions = [<<"gpu">>]},
    State = make_state(#{<<"node001">> => Node1, <<"node002">> => Node2}),
    {reply, Available, _NewState} = flurm_node_manager_server:handle_call(
        {get_available_nodes_for_job, 1, 1024, <<"gpu">>},
        {self(), make_ref()}, State
    ),
    ?assertEqual(1, length(Available)),
    ?assertEqual(<<"node002">>, (hd(Available))#node.hostname).

handle_call_get_available_nodes_for_job_drain_excluded_test() ->
    Node = (make_test_node(<<"node001">>))#node{state = drain, drain_reason = <<"maintenance">>},
    State = make_state(#{<<"node001">> => Node}),
    {reply, Available, _NewState} = flurm_node_manager_server:handle_call(
        {get_available_nodes_for_job, 1, 1024, <<"default">>},
        {self(), make_ref()}, State
    ),
    ?assertEqual([], Available).

%%====================================================================
%% handle_call Tests - allocate_resources
%%====================================================================

handle_call_allocate_resources_success_test() ->
    Node = make_test_node(<<"node001">>, 16, 32768),
    State = make_state(#{<<"node001">> => Node}),
    {reply, ok, NewState} = flurm_node_manager_server:handle_call(
        {allocate_resources, <<"node001">>, 1001, 4, 8192},
        {self(), make_ref()}, State
    ),
    {state, Nodes} = NewState,
    UpdatedNode = maps:get(<<"node001">>, Nodes),
    ?assertEqual(#{1001 => {4, 8192}}, UpdatedNode#node.allocations),
    ?assertEqual([1001], UpdatedNode#node.running_jobs).

handle_call_allocate_resources_insufficient_test() ->
    Node = make_test_node(<<"node001">>, 4, 8192),
    State = make_state(#{<<"node001">> => Node}),
    {reply, {error, insufficient_resources}, _NewState} = flurm_node_manager_server:handle_call(
        {allocate_resources, <<"node001">>, 1001, 8, 16384},
        {self(), make_ref()}, State
    ).

handle_call_allocate_resources_node_not_found_test() ->
    State = make_state(),
    {reply, {error, node_not_found}, _NewState} = flurm_node_manager_server:handle_call(
        {allocate_resources, <<"unknown">>, 1001, 4, 8192},
        {self(), make_ref()}, State
    ).

handle_call_allocate_resources_fully_allocated_test() ->
    Node = make_test_node(<<"node001">>, 4, 8192),
    State = make_state(#{<<"node001">> => Node}),
    %% Allocate all resources
    {reply, ok, NewState} = flurm_node_manager_server:handle_call(
        {allocate_resources, <<"node001">>, 1001, 4, 8192},
        {self(), make_ref()}, State
    ),
    {state, Nodes} = NewState,
    UpdatedNode = maps:get(<<"node001">>, Nodes),
    ?assertEqual(allocated, UpdatedNode#node.state).

%%====================================================================
%% handle_call Tests - drain_node
%%====================================================================

handle_call_drain_node_test() ->
    Node = make_test_node(<<"node001">>),
    State = make_state(#{<<"node001">> => Node}),
    {reply, ok, NewState} = flurm_node_manager_server:handle_call(
        {drain_node, <<"node001">>, <<"maintenance">>},
        {self(), make_ref()}, State
    ),
    {state, Nodes} = NewState,
    DrainedNode = maps:get(<<"node001">>, Nodes),
    ?assertEqual(drain, DrainedNode#node.state),
    ?assertEqual(<<"maintenance">>, DrainedNode#node.drain_reason).

handle_call_drain_node_not_found_test() ->
    State = make_state(),
    {reply, {error, not_found}, _NewState} = flurm_node_manager_server:handle_call(
        {drain_node, <<"unknown">>, <<"test">>},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - undrain_node
%%====================================================================

handle_call_undrain_node_success_test() ->
    Node = (make_test_node(<<"node001">>))#node{state = drain, drain_reason = <<"test">>},
    State = make_state(#{<<"node001">> => Node}),
    {reply, ok, NewState} = flurm_node_manager_server:handle_call(
        {undrain_node, <<"node001">>},
        {self(), make_ref()}, State
    ),
    {state, Nodes} = NewState,
    UndrainedNode = maps:get(<<"node001">>, Nodes),
    ?assertEqual(idle, UndrainedNode#node.state),
    ?assertEqual(undefined, UndrainedNode#node.drain_reason).

handle_call_undrain_node_not_draining_test() ->
    Node = make_test_node(<<"node001">>),
    State = make_state(#{<<"node001">> => Node}),
    {reply, {error, not_draining}, _NewState} = flurm_node_manager_server:handle_call(
        {undrain_node, <<"node001">>},
        {self(), make_ref()}, State
    ).

handle_call_undrain_node_with_allocations_test() ->
    Node = (make_test_node(<<"node001">>))#node{
        state = drain,
        drain_reason = <<"test">>,
        allocations = #{1001 => {4, 8192}}
    },
    State = make_state(#{<<"node001">> => Node}),
    {reply, ok, NewState} = flurm_node_manager_server:handle_call(
        {undrain_node, <<"node001">>},
        {self(), make_ref()}, State
    ),
    {state, Nodes} = NewState,
    UndrainedNode = maps:get(<<"node001">>, Nodes),
    ?assertEqual(mixed, UndrainedNode#node.state).

%%====================================================================
%% handle_call Tests - get_drain_reason
%%====================================================================

handle_call_get_drain_reason_success_test() ->
    Node = (make_test_node(<<"node001">>))#node{state = drain, drain_reason = <<"test reason">>},
    State = make_state(#{<<"node001">> => Node}),
    {reply, {ok, Reason}, _NewState} = flurm_node_manager_server:handle_call(
        {get_drain_reason, <<"node001">>},
        {self(), make_ref()}, State
    ),
    ?assertEqual(<<"test reason">>, Reason).

handle_call_get_drain_reason_not_draining_test() ->
    Node = make_test_node(<<"node001">>),
    State = make_state(#{<<"node001">> => Node}),
    {reply, {error, not_draining}, _NewState} = flurm_node_manager_server:handle_call(
        {get_drain_reason, <<"node001">>},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - is_node_draining
%%====================================================================

handle_call_is_node_draining_true_test() ->
    Node = (make_test_node(<<"node001">>))#node{state = drain},
    State = make_state(#{<<"node001">> => Node}),
    {reply, true, _NewState} = flurm_node_manager_server:handle_call(
        {is_node_draining, <<"node001">>},
        {self(), make_ref()}, State
    ).

handle_call_is_node_draining_false_test() ->
    Node = make_test_node(<<"node001">>),
    State = make_state(#{<<"node001">> => Node}),
    {reply, false, _NewState} = flurm_node_manager_server:handle_call(
        {is_node_draining, <<"node001">>},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - add_node
%%====================================================================

handle_call_add_node_success_test() ->
    State = make_state(),
    NodeSpec = #{
        hostname => <<"newnode001">>,
        cpus => 32,
        memory_mb => 65536,
        features => [],
        partitions => [<<"default">>]
    },
    {reply, ok, NewState} = flurm_node_manager_server:handle_call(
        {add_node, NodeSpec},
        {self(), make_ref()}, State
    ),
    {state, Nodes} = NewState,
    ?assert(maps:is_key(<<"newnode001">>, Nodes)).

handle_call_add_node_already_exists_test() ->
    Node = make_test_node(<<"node001">>),
    State = make_state(#{<<"node001">> => Node}),
    NodeSpec = #{hostname => <<"node001">>, cpus => 32, memory_mb => 65536, features => [], partitions => []},
    {reply, {error, already_registered}, _NewState} = flurm_node_manager_server:handle_call(
        {add_node, NodeSpec},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - remove_node (force)
%%====================================================================

handle_call_remove_node_force_test() ->
    Node = make_test_node(<<"node001">>),
    State = make_state(#{<<"node001">> => Node}),
    {reply, ok, NewState} = flurm_node_manager_server:handle_call(
        {remove_node, <<"node001">>, force},
        {self(), make_ref()}, State
    ),
    {state, Nodes} = NewState,
    ?assertNot(maps:is_key(<<"node001">>, Nodes)).

handle_call_remove_node_not_found_test() ->
    State = make_state(),
    {reply, {error, not_found}, _NewState} = flurm_node_manager_server:handle_call(
        {remove_node, <<"unknown">>, force},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - update_node_properties
%%====================================================================

handle_call_update_node_properties_test() ->
    Node = make_test_node(<<"node001">>, 16, 32768),
    State = make_state(#{<<"node001">> => Node}),
    Updates = #{cpus => 32, memory_mb => 65536},
    {reply, ok, NewState} = flurm_node_manager_server:handle_call(
        {update_node_properties, <<"node001">>, Updates},
        {self(), make_ref()}, State
    ),
    {state, Nodes} = NewState,
    UpdatedNode = maps:get(<<"node001">>, Nodes),
    ?assertEqual(32, UpdatedNode#node.cpus),
    ?assertEqual(65536, UpdatedNode#node.memory_mb).

handle_call_update_node_properties_not_found_test() ->
    State = make_state(),
    {reply, {error, not_found}, _NewState} = flurm_node_manager_server:handle_call(
        {update_node_properties, <<"unknown">>, #{cpus => 32}},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - get_running_jobs_on_node
%%====================================================================

handle_call_get_running_jobs_on_node_test() ->
    Node = (make_test_node(<<"node001">>))#node{running_jobs = [1001, 1002, 1003]},
    State = make_state(#{<<"node001">> => Node}),
    {reply, {ok, Jobs}, _NewState} = flurm_node_manager_server:handle_call(
        {get_running_jobs_on_node, <<"node001">>},
        {self(), make_ref()}, State
    ),
    ?assertEqual([1001, 1002, 1003], Jobs).

handle_call_get_running_jobs_on_node_not_found_test() ->
    State = make_state(),
    {reply, {error, not_found}, _NewState} = flurm_node_manager_server:handle_call(
        {get_running_jobs_on_node, <<"unknown">>},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - GRES operations
%%====================================================================

handle_call_register_node_gres_test() ->
    Node = make_test_node(<<"node001">>),
    State = make_state(#{<<"node001">> => Node}),
    GRESList = [
        #{type => gpu, name => <<"a100">>, count => 4, memory_mb => 40960}
    ],
    {reply, ok, NewState} = flurm_node_manager_server:handle_call(
        {register_node_gres, <<"node001">>, GRESList},
        {self(), make_ref()}, State
    ),
    {state, Nodes} = NewState,
    UpdatedNode = maps:get(<<"node001">>, Nodes),
    ?assertEqual(GRESList, UpdatedNode#node.gres_config),
    ?assertEqual(4, maps:get(<<"gpu">>, UpdatedNode#node.gres_total)),
    ?assertEqual(4, maps:get(<<"gpu:a100">>, UpdatedNode#node.gres_total)).

handle_call_get_node_gres_test() ->
    Node = (make_test_node(<<"node001">>))#node{
        gres_config = [#{type => gpu, count => 2}],
        gres_total = #{<<"gpu">> => 2},
        gres_available = #{<<"gpu">> => 2},
        gres_allocations = #{}
    },
    State = make_state(#{<<"node001">> => Node}),
    {reply, {ok, Result}, _NewState} = flurm_node_manager_server:handle_call(
        {get_node_gres, <<"node001">>},
        {self(), make_ref()}, State
    ),
    ?assertEqual(#{<<"gpu">> => 2}, maps:get(total, Result)),
    ?assertEqual(#{<<"gpu">> => 2}, maps:get(available, Result)).

handle_call_get_node_gres_not_found_test() ->
    State = make_state(),
    {reply, {error, not_found}, _NewState} = flurm_node_manager_server:handle_call(
        {get_node_gres, <<"unknown">>},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_call Tests - unknown request
%%====================================================================

handle_call_unknown_request_test() ->
    State = make_state(),
    {reply, {error, unknown_request}, _NewState} = flurm_node_manager_server:handle_call(
        {unknown_operation, some_args},
        {self(), make_ref()}, State
    ).

%%====================================================================
%% handle_cast Tests
%%====================================================================

handle_cast_heartbeat_test() ->
    Node = make_test_node(<<"node001">>),
    OldHeartbeat = Node#node.last_heartbeat - 100,
    Node2 = Node#node{last_heartbeat = OldHeartbeat},
    State = make_state(#{<<"node001">> => Node2}),
    HeartbeatData = #{
        hostname => <<"node001">>,
        load_avg => 1.5,
        free_memory_mb => 16384,
        running_jobs => [1001]
    },
    {noreply, NewState} = flurm_node_manager_server:handle_cast(
        {heartbeat, HeartbeatData}, State
    ),
    {state, Nodes} = NewState,
    UpdatedNode = maps:get(<<"node001">>, Nodes),
    ?assert(UpdatedNode#node.last_heartbeat > OldHeartbeat),
    ?assertEqual(1.5, UpdatedNode#node.load_avg),
    ?assertEqual(16384, UpdatedNode#node.free_memory_mb),
    ?assertEqual([1001], UpdatedNode#node.running_jobs).

handle_cast_heartbeat_unknown_node_test() ->
    State = make_state(),
    HeartbeatData = #{hostname => <<"unknown">>},
    {noreply, NewState} = flurm_node_manager_server:handle_cast(
        {heartbeat, HeartbeatData}, State
    ),
    ?assertEqual(State, NewState).

handle_cast_release_resources_test() ->
    Node = (make_test_node(<<"node001">>))#node{
        state = mixed,
        allocations = #{1001 => {4, 8192}},
        running_jobs = [1001]
    },
    State = make_state(#{<<"node001">> => Node}),
    {noreply, NewState} = flurm_node_manager_server:handle_cast(
        {release_resources, <<"node001">>, 1001}, State
    ),
    {state, Nodes} = NewState,
    UpdatedNode = maps:get(<<"node001">>, Nodes),
    ?assertEqual(#{}, UpdatedNode#node.allocations),
    ?assertEqual([], UpdatedNode#node.running_jobs),
    ?assertEqual(idle, UpdatedNode#node.state).

handle_cast_release_resources_unknown_node_test() ->
    State = make_state(),
    {noreply, NewState} = flurm_node_manager_server:handle_cast(
        {release_resources, <<"unknown">>, 1001}, State
    ),
    ?assertEqual(State, NewState).

handle_cast_release_gres_test() ->
    Node = (make_test_node(<<"node001">>))#node{
        gres_available = #{<<"gpu">> => 2},
        gres_allocations = #{1001 => [{gpu, 2, [0, 1]}]}
    },
    State = make_state(#{<<"node001">> => Node}),
    {noreply, NewState} = flurm_node_manager_server:handle_cast(
        {release_gres, <<"node001">>, 1001}, State
    ),
    {state, Nodes} = NewState,
    UpdatedNode = maps:get(<<"node001">>, Nodes),
    ?assertEqual(#{<<"gpu">> => 4}, UpdatedNode#node.gres_available),
    ?assertEqual(#{}, UpdatedNode#node.gres_allocations).

handle_cast_unknown_test() ->
    State = make_state(),
    {noreply, NewState} = flurm_node_manager_server:handle_cast(
        {unknown_cast, args}, State
    ),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info Tests
%%====================================================================

handle_info_check_heartbeats_test() ->
    Now = erlang:system_time(second),
    %% Node with recent heartbeat
    Node1 = (make_test_node(<<"node001">>))#node{last_heartbeat = Now - 10},
    %% Node with old heartbeat (should be marked down)
    Node2 = (make_test_node(<<"node002">>))#node{last_heartbeat = Now - 120},
    State = make_state(#{<<"node001">> => Node1, <<"node002">> => Node2}),
    {noreply, NewState} = flurm_node_manager_server:handle_info(
        check_heartbeats, State
    ),
    {state, Nodes} = NewState,
    ?assertEqual(idle, (maps:get(<<"node001">>, Nodes))#node.state),
    ?assertEqual(down, (maps:get(<<"node002">>, Nodes))#node.state).

handle_info_config_changed_nodes_test() ->
    State = make_state(),
    {noreply, _NewState} = flurm_node_manager_server:handle_info(
        {config_changed, nodes, [], []}, State
    ).

handle_info_config_changed_other_test() ->
    State = make_state(),
    {noreply, NewState} = flurm_node_manager_server:handle_info(
        {config_changed, some_key, old_val, new_val}, State
    ),
    ?assertEqual(State, NewState).

handle_info_unknown_test() ->
    State = make_state(),
    {noreply, NewState} = flurm_node_manager_server:handle_info(
        {unknown_message, args}, State
    ),
    ?assertEqual(State, NewState).

%%====================================================================
%% terminate Tests
%%====================================================================

terminate_test() ->
    State = make_state(),
    ?assertEqual(ok, flurm_node_manager_server:terminate(normal, State)).

%%====================================================================
%% Helper Function Tests
%%====================================================================

get_available_resources_empty_allocations_test() ->
    Node = make_test_node(<<"node001">>, 16, 32768),
    {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
    ?assertEqual(16, CpusAvail),
    ?assertEqual(32768, MemAvail).

get_available_resources_with_allocations_test() ->
    Node = (make_test_node(<<"node001">>, 16, 32768))#node{
        allocations = #{
            1001 => {4, 8192},
            1002 => {2, 4096}
        }
    },
    {CpusAvail, MemAvail} = flurm_node_manager_server:get_available_resources(Node),
    ?assertEqual(10, CpusAvail),  % 16 - 4 - 2
    ?assertEqual(20480, MemAvail). % 32768 - 8192 - 4096

apply_node_updates_state_test() ->
    Node = make_test_node(<<"node001">>),
    Updates = #{state => down},
    UpdatedNode = flurm_node_manager_server:apply_node_updates(Node, Updates),
    ?assertEqual(down, UpdatedNode#node.state).

apply_node_updates_load_avg_test() ->
    Node = make_test_node(<<"node001">>),
    Updates = #{load_avg => 2.5},
    UpdatedNode = flurm_node_manager_server:apply_node_updates(Node, Updates),
    ?assertEqual(2.5, UpdatedNode#node.load_avg).

apply_node_updates_multiple_test() ->
    Node = make_test_node(<<"node001">>),
    Updates = #{load_avg => 3.0, free_memory_mb => 8192, running_jobs => [1001, 1002]},
    UpdatedNode = flurm_node_manager_server:apply_node_updates(Node, Updates),
    ?assertEqual(3.0, UpdatedNode#node.load_avg),
    ?assertEqual(8192, UpdatedNode#node.free_memory_mb),
    ?assertEqual([1001, 1002], UpdatedNode#node.running_jobs).

apply_node_updates_unknown_key_test() ->
    Node = make_test_node(<<"node001">>),
    Updates = #{unknown_key => some_value},
    UpdatedNode = flurm_node_manager_server:apply_node_updates(Node, Updates),
    ?assertEqual(Node#node.load_avg, UpdatedNode#node.load_avg).

config_to_node_spec_basic_test() ->
    NodeConfig = #{
        cpus => 16,
        realmemory => 32768,
        feature => [<<"gpu">>],
        partitions => [<<"default">>],
        state => idle
    },
    NodeSpec = flurm_node_manager_server:config_to_node_spec(<<"node001">>, NodeConfig),
    ?assertEqual(<<"node001">>, maps:get(hostname, NodeSpec)),
    ?assertEqual(16, maps:get(cpus, NodeSpec)),
    ?assertEqual(32768, maps:get(memory_mb, NodeSpec)),
    ?assertEqual([<<"gpu">>], maps:get(features, NodeSpec)),
    ?assertEqual([<<"default">>], maps:get(partitions, NodeSpec)).

config_to_node_spec_computed_cpus_test() ->
    NodeConfig = #{
        sockets => 2,
        corespersocket => 8,
        threadspercore => 2,
        realmemory => 65536
    },
    NodeSpec = flurm_node_manager_server:config_to_node_spec(<<"node001">>, NodeConfig),
    ?assertEqual(32, maps:get(cpus, NodeSpec)).  % 2 * 8 * 2

apply_property_updates_cpus_test() ->
    Node = make_test_node(<<"node001">>, 16, 32768),
    Updates = #{cpus => 32},
    UpdatedNode = flurm_node_manager_server:apply_property_updates(Node, Updates),
    ?assertEqual(32, UpdatedNode#node.cpus).

apply_property_updates_memory_test() ->
    Node = make_test_node(<<"node001">>, 16, 32768),
    Updates = #{memory_mb => 65536},
    UpdatedNode = flurm_node_manager_server:apply_property_updates(Node, Updates),
    ?assertEqual(65536, UpdatedNode#node.memory_mb).

apply_property_updates_features_test() ->
    Node = make_test_node(<<"node001">>),
    Updates = #{features => [<<"gpu">>, <<"nvme">>]},
    UpdatedNode = flurm_node_manager_server:apply_property_updates(Node, Updates),
    ?assertEqual([<<"gpu">>, <<"nvme">>], UpdatedNode#node.features).

apply_property_updates_partitions_test() ->
    Node = make_test_node(<<"node001">>),
    Updates = #{partitions => [<<"gpu">>, <<"compute">>]},
    UpdatedNode = flurm_node_manager_server:apply_property_updates(Node, Updates),
    ?assertEqual([<<"gpu">>, <<"compute">>], UpdatedNode#node.partitions).

apply_property_updates_state_test() ->
    Node = make_test_node(<<"node001">>),
    Updates = #{state => drain},
    UpdatedNode = flurm_node_manager_server:apply_property_updates(Node, Updates),
    ?assertEqual(drain, UpdatedNode#node.state).

build_gres_maps_empty_test() ->
    {Config, Total, Available} = flurm_node_manager_server:build_gres_maps([]),
    ?assertEqual([], Config),
    ?assertEqual(#{}, Total),
    ?assertEqual(#{}, Available).

build_gres_maps_single_gpu_test() ->
    GRESList = [#{type => gpu, count => 4}],
    {Config, Total, Available} = flurm_node_manager_server:build_gres_maps(GRESList),
    ?assertEqual(GRESList, Config),
    ?assertEqual(#{<<"gpu">> => 4}, Total),
    ?assertEqual(#{<<"gpu">> => 4}, Available).

build_gres_maps_named_gpu_test() ->
    GRESList = [#{type => gpu, name => <<"a100">>, count => 4}],
    {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
    ?assertEqual(4, maps:get(<<"gpu">>, Total)),
    ?assertEqual(4, maps:get(<<"gpu:a100">>, Total)).

build_gres_maps_multiple_types_test() ->
    GRESList = [
        #{type => gpu, name => <<"a100">>, count => 4},
        #{type => fpga, count => 2}
    ],
    {_Config, Total, _Available} = flurm_node_manager_server:build_gres_maps(GRESList),
    ?assertEqual(4, maps:get(<<"gpu">>, Total)),
    ?assertEqual(4, maps:get(<<"gpu:a100">>, Total)),
    ?assertEqual(2, maps:get(<<"fpga">>, Total)).

atom_to_gres_key_gpu_test() ->
    ?assertEqual(<<"gpu">>, flurm_node_manager_server:atom_to_gres_key(gpu)).

atom_to_gres_key_fpga_test() ->
    ?assertEqual(<<"fpga">>, flurm_node_manager_server:atom_to_gres_key(fpga)).

atom_to_gres_key_mic_test() ->
    ?assertEqual(<<"mic">>, flurm_node_manager_server:atom_to_gres_key(mic)).

atom_to_gres_key_mps_test() ->
    ?assertEqual(<<"mps">>, flurm_node_manager_server:atom_to_gres_key(mps)).

atom_to_gres_key_shard_test() ->
    ?assertEqual(<<"shard">>, flurm_node_manager_server:atom_to_gres_key(shard)).

atom_to_gres_key_other_atom_test() ->
    ?assertEqual(<<"custom">>, flurm_node_manager_server:atom_to_gres_key(custom)).

atom_to_gres_key_binary_test() ->
    ?assertEqual(<<"already_binary">>, flurm_node_manager_server:atom_to_gres_key(<<"already_binary">>)).

check_node_gres_availability_no_gres_test() ->
    Node = make_test_node(<<"node001">>),
    ?assertEqual(true, flurm_node_manager_server:check_node_gres_availability(Node, <<>>)).

check_node_gres_availability_sufficient_test() ->
    Node = (make_test_node(<<"node001">>))#node{
        gres_available = #{<<"gpu">> => 4, <<"gpu:a100">> => 4}
    },
    ?assertEqual(true, flurm_node_manager_server:check_node_gres_availability(Node, <<"gpu:2">>)).

check_node_gres_availability_insufficient_test() ->
    Node = (make_test_node(<<"node001">>))#node{
        gres_available = #{<<"gpu">> => 2}
    },
    ?assertEqual(false, flurm_node_manager_server:check_node_gres_availability(Node, <<"gpu:4">>)).

build_updates_from_config_no_changes_test() ->
    Node = make_test_node(<<"node001">>, 16, 32768),
    Node2 = Node#node{features = [<<"gpu">>], partitions = [<<"default">>]},
    NodeDef = #{
        cpus => 16,
        realmemory => 32768,
        feature => [<<"gpu">>],
        partitions => [<<"default">>]
    },
    Updates = flurm_node_manager_server:build_updates_from_config(Node2, NodeDef),
    ?assertEqual(#{}, Updates).

build_updates_from_config_cpu_change_test() ->
    Node = make_test_node(<<"node001">>, 16, 32768),
    NodeDef = #{cpus => 32, realmemory => 32768},
    Updates = flurm_node_manager_server:build_updates_from_config(Node, NodeDef),
    ?assertEqual(#{cpus => 32}, Updates).

build_updates_from_config_memory_change_test() ->
    Node = make_test_node(<<"node001">>, 16, 32768),
    NodeDef = #{cpus => 16, realmemory => 65536},
    Updates = flurm_node_manager_server:build_updates_from_config(Node, NodeDef),
    ?assertEqual(#{memory_mb => 65536}, Updates).
