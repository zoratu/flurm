%%%-------------------------------------------------------------------
%%% @doc FLURM Node Registry Tests
%%%
%%% Comprehensive EUnit tests for the flurm_node_registry module.
%%% Tests node registration, lookup, state updates, and ETS table
%%% management.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_registry_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Internal State Record Definition (for test compilation)
%%====================================================================

%% Mirror the internal state record for testing gen_server callbacks
-record(state, {
    monitors = #{} :: #{reference() => binary()}
}).

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start required applications
    application:ensure_all_started(lager),

    %% Stop existing registry if running
    case whereis(flurm_node_registry) of
        undefined -> ok;
        Pid ->
            unlink(Pid),
            exit(Pid, shutdown),
            flurm_test_utils:wait_for_death(Pid)
    end,

    %% Clean up existing ETS tables
    catch ets:delete(flurm_nodes_by_name),
    catch ets:delete(flurm_nodes_by_state),
    catch ets:delete(flurm_nodes_by_partition),

    ok.

cleanup(_) ->
    %% Stop registry if running
    case whereis(flurm_node_registry) of
        undefined -> ok;
        Pid ->
            unlink(Pid),
            exit(Pid, shutdown),
            flurm_test_utils:wait_for_death(Pid)
    end,

    %% Clean up ETS tables
    catch ets:delete(flurm_nodes_by_name),
    catch ets:delete(flurm_nodes_by_state),
    catch ets:delete(flurm_nodes_by_partition),

    ok.

setup_with_registry() ->
    setup(),

    %% Mock flurm_node for get_info calls during registration
    meck:new(flurm_node, [passthrough, non_strict]),
    meck:expect(flurm_node, get_info, fun(_Pid) ->
        {ok, #{
            hostname => <<"test.local">>,
            state => up,
            partitions => [<<"default">>],
            cpus => 8,
            cpus_available => 8,
            memory => 16384,
            memory_available => 16384,
            gpus => 0,
            gpus_available => 0
        }}
    end),

    %% Start registry
    {ok, _Pid} = flurm_node_registry:start_link(),
    ok.

cleanup_with_registry(_) ->
    meck:unload(flurm_node),
    cleanup(ok).

%%====================================================================
%% Module Info Tests
%%====================================================================

module_info_test_() ->
    [
        {"module_info/0 returns module information", fun() ->
            Info = flurm_node_registry:module_info(),
            ?assert(is_list(Info)),
            ?assertMatch({module, flurm_node_registry}, lists:keyfind(module, 1, Info)),
            ?assertMatch({exports, _}, lists:keyfind(exports, 1, Info))
        end},
        {"module_info/1 returns exports", fun() ->
            Exports = flurm_node_registry:module_info(exports),
            ?assert(is_list(Exports)),
            %% Check expected exports
            ?assert(lists:member({start_link, 0}, Exports)),
            ?assert(lists:member({register_node, 2}, Exports)),
            ?assert(lists:member({unregister_node, 1}, Exports)),
            ?assert(lists:member({lookup_node, 1}, Exports)),
            ?assert(lists:member({list_nodes, 0}, Exports)),
            ?assert(lists:member({list_all_nodes, 0}, Exports)),
            ?assert(lists:member({list_nodes_by_state, 1}, Exports)),
            ?assert(lists:member({list_nodes_by_partition, 1}, Exports)),
            ?assert(lists:member({get_available_nodes, 1}, Exports)),
            ?assert(lists:member({update_state, 2}, Exports)),
            ?assert(lists:member({update_entry, 2}, Exports)),
            ?assert(lists:member({get_node_entry, 1}, Exports)),
            ?assert(lists:member({count_by_state, 0}, Exports)),
            ?assert(lists:member({count_nodes, 0}, Exports)),
            ?assert(lists:member({init, 1}, Exports)),
            ?assert(lists:member({handle_call, 3}, Exports)),
            ?assert(lists:member({handle_cast, 2}, Exports)),
            ?assert(lists:member({handle_info, 2}, Exports)),
            ?assert(lists:member({terminate, 2}, Exports)),
            ?assert(lists:member({code_change, 3}, Exports))
        end},
        {"module_info/1 returns attributes", fun() ->
            Attrs = flurm_node_registry:module_info(attributes),
            ?assert(is_list(Attrs)),
            %% Should have behaviour attribute
            BehaviourAttr = proplists:get_value(behaviour, Attrs,
                            proplists:get_value(behavior, Attrs, [])),
            ?assert(lists:member(gen_server, BehaviourAttr))
        end}
    ].

%%====================================================================
%% Gen Server Init Tests
%%====================================================================

init_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"init/1 returns ok with empty state", fun test_init_returns_ok/0},
         {"init/1 creates ETS tables", fun test_init_creates_ets_tables/0}
     ]}.

test_init_returns_ok() ->
    Result = flurm_node_registry:init([]),

    ?assertMatch({ok, _State}, Result),
    %% Clean up ETS tables created by init for next test
    catch ets:delete(flurm_nodes_by_name),
    catch ets:delete(flurm_nodes_by_state),
    catch ets:delete(flurm_nodes_by_partition),
    ok.

test_init_creates_ets_tables() ->
    {ok, _State} = flurm_node_registry:init([]),

    %% Verify ETS tables exist
    ?assertNotEqual(undefined, ets:whereis(flurm_nodes_by_name)),
    ?assertNotEqual(undefined, ets:whereis(flurm_nodes_by_state)),
    ?assertNotEqual(undefined, ets:whereis(flurm_nodes_by_partition)),

    %% Verify table properties
    NameTableInfo = ets:info(flurm_nodes_by_name),
    ?assertEqual(set, proplists:get_value(type, NameTableInfo)),
    ?assertEqual(public, proplists:get_value(protection, NameTableInfo)),

    StateTableInfo = ets:info(flurm_nodes_by_state),
    ?assertEqual(bag, proplists:get_value(type, StateTableInfo)),
    ?assertEqual(public, proplists:get_value(protection, StateTableInfo)),

    PartitionTableInfo = ets:info(flurm_nodes_by_partition),
    ?assertEqual(bag, proplists:get_value(type, PartitionTableInfo)),
    ?assertEqual(public, proplists:get_value(protection, PartitionTableInfo)),
    ok.

%%====================================================================
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"start_link/0 starts registry with correct name", fun test_start_link_name/0},
         {"start_link/0 returns error if already started", fun test_start_link_already_started/0}
     ]}.

test_start_link_name() ->
    Result = flurm_node_registry:start_link(),

    ?assertMatch({ok, _Pid}, Result),
    {ok, Pid} = Result,
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Verify registered name
    ?assertEqual(Pid, whereis(flurm_node_registry)),
    ok.

test_start_link_already_started() ->
    %% Start registry first
    {ok, Pid1} = flurm_node_registry:start_link(),
    ?assert(is_pid(Pid1)),

    %% Try to start again - should return the existing process pid
    %% (graceful handling of already_started for supervisor restarts)
    Result = flurm_node_registry:start_link(),

    ?assertMatch({ok, _}, Result),
    {ok, Pid2} = Result,
    ?assertEqual(Pid1, Pid2),  %% Should be the same pid
    ok.

%%====================================================================
%% Registration Tests
%%====================================================================

registration_test_() ->
    {setup,
     fun setup_with_registry/0,
     fun cleanup_with_registry/1,
     [
         {"register_node/2 registers a node successfully", fun test_register_node/0},
         {"register_node/2 returns error for duplicate", fun test_register_duplicate/0},
         {"unregister_node/1 removes a registered node", fun test_unregister_node/0}
     ]}.

test_register_node() ->
    NodeName = <<"test-node-001">>,
    Pid = spawn_link(fun() -> receive stop -> ok end end),

    Result = flurm_node_registry:register_node(NodeName, Pid),

    ?assertEqual(ok, Result),

    %% Verify node can be looked up
    ?assertEqual({ok, Pid}, flurm_node_registry:lookup_node(NodeName)),

    %% Cleanup
    Pid ! stop,
    ok.

test_register_duplicate() ->
    NodeName = <<"duplicate-test-node">>,
    Pid1 = spawn_link(fun() -> receive stop -> ok end end),
    Pid2 = spawn_link(fun() -> receive stop -> ok end end),

    %% Register first time
    ok = flurm_node_registry:register_node(NodeName, Pid1),

    %% Try to register again
    Result = flurm_node_registry:register_node(NodeName, Pid2),

    ?assertEqual({error, already_registered}, Result),

    %% Cleanup
    Pid1 ! stop,
    Pid2 ! stop,
    ok.

test_unregister_node() ->
    NodeName = <<"unregister-test-node">>,
    Pid = spawn_link(fun() -> receive stop -> ok end end),

    %% Register node
    ok = flurm_node_registry:register_node(NodeName, Pid),
    ?assertEqual({ok, Pid}, flurm_node_registry:lookup_node(NodeName)),

    %% Unregister node
    Result = flurm_node_registry:unregister_node(NodeName),

    ?assertEqual(ok, Result),
    ?assertEqual({error, not_found}, flurm_node_registry:lookup_node(NodeName)),

    %% Cleanup
    Pid ! stop,
    ok.

%%====================================================================
%% Lookup Tests
%%====================================================================

lookup_test_() ->
    {setup,
     fun setup_with_registry/0,
     fun cleanup_with_registry/1,
     [
         {"lookup_node/1 returns pid for existing node", fun test_lookup_existing/0},
         {"lookup_node/1 returns error for non-existent node", fun test_lookup_non_existent/0},
         {"get_node_entry/1 returns full entry", fun test_get_node_entry/0}
     ]}.

test_lookup_existing() ->
    NodeName = <<"lookup-test-node">>,
    Pid = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(NodeName, Pid),

    Result = flurm_node_registry:lookup_node(NodeName),

    ?assertEqual({ok, Pid}, Result),

    Pid ! stop,
    ok.

test_lookup_non_existent() ->
    Result = flurm_node_registry:lookup_node(<<"non-existent-node">>),

    ?assertEqual({error, not_found}, Result),
    ok.

test_get_node_entry() ->
    NodeName = <<"entry-test-node">>,
    Pid = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(NodeName, Pid),

    Result = flurm_node_registry:get_node_entry(NodeName),

    ?assertMatch({ok, #node_entry{}}, Result),
    {ok, Entry} = Result,
    ?assertEqual(NodeName, Entry#node_entry.name),
    ?assertEqual(Pid, Entry#node_entry.pid),

    Pid ! stop,
    ok.

%%====================================================================
%% List Tests
%%====================================================================

list_test_() ->
    {setup,
     fun setup_with_registry/0,
     fun cleanup_with_registry/1,
     [
         {"list_nodes/0 returns all registered nodes", fun test_list_nodes/0},
         {"list_all_nodes/0 is alias for list_nodes/0", fun test_list_all_nodes/0},
         {"list_nodes_by_state/1 filters by state", fun test_list_nodes_by_state/0},
         {"list_nodes_by_partition/1 filters by partition", fun test_list_nodes_by_partition/0}
     ]}.

test_list_nodes() ->
    Pid1 = spawn_link(fun() -> receive stop -> ok end end),
    Pid2 = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(<<"list-node-001">>, Pid1),
    ok = flurm_node_registry:register_node(<<"list-node-002">>, Pid2),

    Result = flurm_node_registry:list_nodes(),

    ?assert(is_list(Result)),
    ?assertEqual(2, length(Result)),
    ?assert(lists:member({<<"list-node-001">>, Pid1}, Result)),
    ?assert(lists:member({<<"list-node-002">>, Pid2}, Result)),

    Pid1 ! stop,
    Pid2 ! stop,
    ok.

test_list_all_nodes() ->
    Pid = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(<<"all-nodes-test">>, Pid),

    ListNodes = flurm_node_registry:list_nodes(),
    ListAllNodes = flurm_node_registry:list_all_nodes(),

    ?assertEqual(ListNodes, ListAllNodes),

    Pid ! stop,
    ok.

test_list_nodes_by_state() ->
    %% Register nodes with different states
    Pid1 = spawn_link(fun() -> receive stop -> ok end end),
    Pid2 = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(<<"state-node-001">>, Pid1),
    ok = flurm_node_registry:register_node(<<"state-node-002">>, Pid2),

    %% Both should be 'up' initially (from mock)
    UpNodes = flurm_node_registry:list_nodes_by_state(up),

    ?assert(is_list(UpNodes)),
    ?assert(length(UpNodes) >= 2),

    %% Down nodes should be empty or not include our test nodes
    DownNodes = flurm_node_registry:list_nodes_by_state(down),
    ?assertNot(lists:keymember(<<"state-node-001">>, 1, DownNodes)),
    ?assertNot(lists:keymember(<<"state-node-002">>, 1, DownNodes)),

    Pid1 ! stop,
    Pid2 ! stop,
    ok.

test_list_nodes_by_partition() ->
    %% Set up mock to return specific partition
    meck:expect(flurm_node, get_info, fun(_Pid) ->
        {ok, #{
            hostname => <<"test.local">>,
            state => up,
            partitions => [<<"compute">>, <<"gpu">>],
            cpus => 8,
            cpus_available => 8,
            memory => 16384,
            memory_available => 16384,
            gpus => 2,
            gpus_available => 2
        }}
    end),

    Pid = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(<<"partition-test-node">>, Pid),

    ComputeNodes = flurm_node_registry:list_nodes_by_partition(<<"compute">>),
    GpuNodes = flurm_node_registry:list_nodes_by_partition(<<"gpu">>),

    ?assert(lists:keymember(<<"partition-test-node">>, 1, ComputeNodes)),
    ?assert(lists:keymember(<<"partition-test-node">>, 1, GpuNodes)),

    Pid ! stop,
    ok.

%%====================================================================
%% Available Nodes Tests
%%====================================================================

available_nodes_test_() ->
    {setup,
     fun setup_with_registry/0,
     fun cleanup_with_registry/1,
     [
         {"get_available_nodes/1 returns nodes with sufficient resources", fun test_available_nodes/0},
         {"get_available_nodes/1 filters by resource requirements", fun test_available_nodes_filter/0}
     ]}.

test_available_nodes() ->
    Pid = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(<<"avail-test-node">>, Pid),

    %% Request 4 CPUs, 8GB memory, 0 GPUs
    Result = flurm_node_registry:get_available_nodes({4, 8192, 0}),

    ?assert(is_list(Result)),
    %% Should find our node (has 8 CPUs, 16GB)
    NodeNames = [E#node_entry.name || E <- Result],
    ?assert(lists:member(<<"avail-test-node">>, NodeNames)),

    Pid ! stop,
    ok.

test_available_nodes_filter() ->
    Pid = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(<<"filter-test-node">>, Pid),

    %% Request more resources than available
    Result = flurm_node_registry:get_available_nodes({100, 100000, 10}),

    %% Should not find any nodes
    ?assertEqual([], Result),

    Pid ! stop,
    ok.

%%====================================================================
%% State Update Tests
%%====================================================================

state_update_test_() ->
    {setup,
     fun setup_with_registry/0,
     fun cleanup_with_registry/1,
     [
         {"update_state/2 updates node state", fun test_update_state/0},
         {"update_state/2 returns error for non-existent node", fun test_update_state_not_found/0},
         {"update_entry/2 updates full node entry", fun test_update_entry/0}
     ]}.

test_update_state() ->
    NodeName = <<"state-update-node">>,
    Pid = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(NodeName, Pid),

    %% Update state to drain
    Result = flurm_node_registry:update_state(NodeName, drain),

    ?assertEqual(ok, Result),

    %% Verify state changed
    {ok, Entry} = flurm_node_registry:get_node_entry(NodeName),
    ?assertEqual(drain, Entry#node_entry.state),

    Pid ! stop,
    ok.

test_update_state_not_found() ->
    Result = flurm_node_registry:update_state(<<"non-existent">>, down),

    ?assertEqual({error, not_found}, Result),
    ok.

test_update_entry() ->
    NodeName = <<"entry-update-node">>,
    Pid = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(NodeName, Pid),

    %% Create updated entry
    NewEntry = #node_entry{
        name = NodeName,
        pid = Pid,
        hostname = <<"updated.local">>,
        state = drain,
        partitions = [<<"updated">>],
        cpus_total = 16,
        cpus_avail = 8,
        memory_total = 32768,
        memory_avail = 16384,
        gpus_total = 4,
        gpus_avail = 2
    },

    Result = flurm_node_registry:update_entry(NodeName, NewEntry),

    ?assertEqual(ok, Result),

    %% Verify entry updated
    {ok, UpdatedEntry} = flurm_node_registry:get_node_entry(NodeName),
    ?assertEqual(16, UpdatedEntry#node_entry.cpus_total),
    ?assertEqual(8, UpdatedEntry#node_entry.cpus_avail),

    Pid ! stop,
    ok.

%%====================================================================
%% Count Tests
%%====================================================================

count_test_() ->
    {setup,
     fun setup_with_registry/0,
     fun cleanup_with_registry/1,
     [
         {"count_nodes/0 returns total node count", fun test_count_nodes/0},
         {"count_by_state/0 returns state breakdown", fun test_count_by_state/0}
     ]}.

test_count_nodes() ->
    Pid1 = spawn_link(fun() -> receive stop -> ok end end),
    Pid2 = spawn_link(fun() -> receive stop -> ok end end),

    %% Get initial count
    InitialCount = flurm_node_registry:count_nodes(),

    ok = flurm_node_registry:register_node(<<"count-node-001">>, Pid1),
    ok = flurm_node_registry:register_node(<<"count-node-002">>, Pid2),

    Result = flurm_node_registry:count_nodes(),

    ?assertEqual(InitialCount + 2, Result),

    Pid1 ! stop,
    Pid2 ! stop,
    ok.

test_count_by_state() ->
    Pid1 = spawn_link(fun() -> receive stop -> ok end end),
    Pid2 = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(<<"count-state-001">>, Pid1),
    ok = flurm_node_registry:register_node(<<"count-state-002">>, Pid2),

    Result = flurm_node_registry:count_by_state(),

    ?assert(is_map(Result)),
    ?assert(maps:is_key(up, Result)),
    ?assert(maps:is_key(down, Result)),
    ?assert(maps:is_key(drain, Result)),
    ?assert(maps:is_key(maint, Result)),

    %% Both nodes should be 'up'
    UpCount = maps:get(up, Result),
    ?assert(UpCount >= 2),

    Pid1 ! stop,
    Pid2 ! stop,
    ok.

%%====================================================================
%% Handle Info Tests
%%====================================================================

handle_info_test_() ->
    {setup,
     fun setup_with_registry/0,
     fun cleanup_with_registry/1,
     [
         {"handles DOWN message for registered node", fun test_handle_down_message/0},
         {"handles config_reload_nodes message", fun test_handle_config_reload/0},
         {"handles config_changed message", fun test_handle_config_changed/0},
         {"ignores unknown messages", fun test_handle_unknown_message/0}
     ]}.

test_handle_down_message() ->
    NodeName = <<"down-test-node">>,
    Pid = spawn_link(fun() -> receive stop -> ok end end),

    ok = flurm_node_registry:register_node(NodeName, Pid),
    ?assertEqual({ok, Pid}, flurm_node_registry:lookup_node(NodeName)),

    %% Kill the process to trigger DOWN message
    Pid ! stop,
    flurm_test_utils:wait_for_death(Pid),
    %% Wait for registry to process DOWN message
    _ = sys:get_state(flurm_node_registry),

    %% Node should be automatically unregistered
    ?assertEqual({error, not_found}, flurm_node_registry:lookup_node(NodeName)),
    ok.

test_handle_config_reload() ->
    %% Mock flurm_config_slurm
    meck:new(flurm_config_slurm, [passthrough, non_strict]),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(Pattern) -> [Pattern] end),

    %% Send config reload message to registry
    NodeDefs = [#{nodename => <<"config-node">>, cpus => 16}],

    whereis(flurm_node_registry) ! {config_reload_nodes, NodeDefs},
    _ = sys:get_state(flurm_node_registry),

    %% No crash should occur
    ?assert(is_process_alive(whereis(flurm_node_registry))),

    meck:unload(flurm_config_slurm),
    ok.

test_handle_config_changed() ->
    %% Mock flurm_config_slurm
    meck:new(flurm_config_slurm, [passthrough, non_strict]),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(Pattern) -> [Pattern] end),

    %% Send config changed message
    NewNodes = [#{nodename => <<"changed-node">>, cpus => 32}],

    whereis(flurm_node_registry) ! {config_changed, nodes, [], NewNodes},
    _ = sys:get_state(flurm_node_registry),

    %% No crash should occur
    ?assert(is_process_alive(whereis(flurm_node_registry))),

    %% Other config changes should be ignored
    whereis(flurm_node_registry) ! {config_changed, partitions, [], []},
    _ = sys:get_state(flurm_node_registry),
    ?assert(is_process_alive(whereis(flurm_node_registry))),

    meck:unload(flurm_config_slurm),
    ok.

test_handle_unknown_message() ->
    whereis(flurm_node_registry) ! {unknown_message, data},
    _ = sys:get_state(flurm_node_registry),

    %% Should not crash
    ?assert(is_process_alive(whereis(flurm_node_registry))),
    ok.

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

callback_test_() ->
    {setup,
     fun setup_with_registry/0,
     fun cleanup_with_registry/1,
     [
         {"handle_call unknown request returns error", fun test_handle_call_unknown/0},
         {"handle_cast is ignored", fun test_handle_cast_ignored/0},
         {"terminate returns ok", fun test_terminate/0},
         {"code_change returns ok with state", fun test_code_change/0}
     ]}.

test_handle_call_unknown() ->
    Result = gen_server:call(flurm_node_registry, {unknown_request, data}),

    ?assertEqual({error, unknown_request}, Result),
    ok.

test_handle_cast_ignored() ->
    %% Cast should not crash the server
    gen_server:cast(flurm_node_registry, {some_cast, data}),
    _ = sys:get_state(flurm_node_registry),

    ?assert(is_process_alive(whereis(flurm_node_registry))),
    ok.

test_terminate() ->
    %% Test terminate callback directly
    State = #state{monitors = #{}},
    Result = flurm_node_registry:terminate(normal, State),

    ?assertEqual(ok, Result),
    ok.

test_code_change() ->
    %% Test code_change callback directly
    State = #state{monitors = #{}},
    Result = flurm_node_registry:code_change(old_vsn, State, extra),

    ?assertEqual({ok, State}, Result),
    ok.

%%====================================================================
%% Behaviour Implementation Tests
%%====================================================================

behaviour_test_() ->
    [
        {"implements gen_server behaviour", fun() ->
            Attrs = flurm_node_registry:module_info(attributes),
            Behaviours = proplists:get_value(behaviour, Attrs, []) ++
                         proplists:get_value(behavior, Attrs, []),
            ?assert(lists:member(gen_server, Behaviours))
        end},
        {"exports required gen_server callbacks", fun() ->
            Exports = flurm_node_registry:module_info(exports),
            ?assert(lists:member({init, 1}, Exports)),
            ?assert(lists:member({handle_call, 3}, Exports)),
            ?assert(lists:member({handle_cast, 2}, Exports)),
            ?assert(lists:member({handle_info, 2}, Exports)),
            ?assert(lists:member({terminate, 2}, Exports)),
            ?assert(lists:member({code_change, 3}, Exports))
        end}
    ].

%%====================================================================
%% ETS Table Tests
%%====================================================================

ets_table_test_() ->
    {setup,
     fun setup_with_registry/0,
     fun cleanup_with_registry/1,
     [
         {"nodes_by_name table is set type", fun() ->
             Info = ets:info(flurm_nodes_by_name),
             ?assertEqual(set, proplists:get_value(type, Info))
         end},
         {"nodes_by_state table is bag type", fun() ->
             Info = ets:info(flurm_nodes_by_state),
             ?assertEqual(bag, proplists:get_value(type, Info))
         end},
         {"nodes_by_partition table is bag type", fun() ->
             Info = ets:info(flurm_nodes_by_partition),
             ?assertEqual(bag, proplists:get_value(type, Info))
         end},
         {"tables are public for read concurrency", fun() ->
             ?assertEqual(public, proplists:get_value(protection, ets:info(flurm_nodes_by_name))),
             ?assertEqual(public, proplists:get_value(protection, ets:info(flurm_nodes_by_state))),
             ?assertEqual(public, proplists:get_value(protection, ets:info(flurm_nodes_by_partition)))
         end}
     ]}.

