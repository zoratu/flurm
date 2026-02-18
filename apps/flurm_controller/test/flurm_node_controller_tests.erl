%%%-------------------------------------------------------------------
%%% @doc FLURM Node Controller Module Tests
%%%
%%% Comprehensive tests for flurm_node_acceptor, flurm_node_connection_manager,
%%% and flurm_node_manager_server modules.
%%% These tests aim for 100% code coverage using meck for mocking.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_controller_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% flurm_node_connection_manager Tests
%%====================================================================

connection_manager_test_() ->
    {setup,
     fun setup_conn_mgr/0,
     fun cleanup_conn_mgr/1,
     fun(_Pid) ->
         [
            {"Connection manager start_link", fun test_conn_mgr_start/0},
            {"Connection manager register_connection", fun test_conn_mgr_register/0},
            {"Connection manager unregister_connection", fun test_conn_mgr_unregister/0},
            {"Connection manager get_connection found", fun test_conn_mgr_get_found/0},
            {"Connection manager get_connection not found", fun test_conn_mgr_get_not_found/0},
            {"Connection manager list_connected_nodes", fun test_conn_mgr_list/0},
            {"Connection manager is_node_connected", fun test_conn_mgr_is_connected/0},
            {"Connection manager send_to_node success", fun test_conn_mgr_send_node/0},
            {"Connection manager send_to_node not connected", fun test_conn_mgr_send_not_connected/0},
            {"Connection manager send_to_nodes", fun test_conn_mgr_send_nodes/0},
            {"Connection manager find_by_socket", fun test_conn_mgr_find_by_socket/0},
            {"Connection manager replace existing connection", fun test_conn_mgr_replace_connection/0},
            {"Connection manager unknown call", fun test_conn_mgr_unknown_call/0},
            {"Connection manager unknown cast", fun test_conn_mgr_unknown_cast/0},
            {"Connection manager unknown info", fun test_conn_mgr_unknown_info/0}
         ]
     end}.

setup_conn_mgr() ->
    catch meck:unload(flurm_node_manager_server),
    meck:new(flurm_node_manager_server, [passthrough, non_strict]),
    meck:expect(flurm_node_manager_server, update_node, fun(_, _) -> ok end),
    {ok, Pid} = flurm_node_connection_manager:start_link(),
    Pid.

cleanup_conn_mgr(Pid) ->
    unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    flurm_test_utils:wait_for_death(Pid),
    catch meck:unload(flurm_node_manager_server),
    ok.

test_conn_mgr_start() ->
    %% Already started in setup
    Nodes = flurm_node_connection_manager:list_connected_nodes(),
    ?assertEqual([], Nodes),
    ok.

test_conn_mgr_register() ->
    TestPid = spawn(fun() -> receive stop -> ok end end),
    ok = flurm_node_connection_manager:register_connection(<<"node1">>, TestPid),
    {ok, TestPid} = flurm_node_connection_manager:get_connection(<<"node1">>),
    TestPid ! stop,
    ok.

test_conn_mgr_unregister() ->
    TestPid = spawn(fun() -> receive stop -> ok end end),
    ok = flurm_node_connection_manager:register_connection(<<"unreg_node">>, TestPid),
    {ok, TestPid} = flurm_node_connection_manager:get_connection(<<"unreg_node">>),
    ok = flurm_node_connection_manager:unregister_connection(<<"unreg_node">>),
    _ = sys:get_state(flurm_node_connection_manager),
    {error, not_connected} = flurm_node_connection_manager:get_connection(<<"unreg_node">>),
    TestPid ! stop,
    ok.

test_conn_mgr_get_found() ->
    TestPid = spawn(fun() -> receive stop -> ok end end),
    ok = flurm_node_connection_manager:register_connection(<<"get_node">>, TestPid),
    {ok, TestPid} = flurm_node_connection_manager:get_connection(<<"get_node">>),
    TestPid ! stop,
    ok.

test_conn_mgr_get_not_found() ->
    {error, not_connected} = flurm_node_connection_manager:get_connection(<<"nonexistent">>),
    ok.

test_conn_mgr_list() ->
    Pid1 = spawn(fun() -> receive stop -> ok end end),
    Pid2 = spawn(fun() -> receive stop -> ok end end),
    ok = flurm_node_connection_manager:register_connection(<<"list1">>, Pid1),
    ok = flurm_node_connection_manager:register_connection(<<"list2">>, Pid2),
    Nodes = flurm_node_connection_manager:list_connected_nodes(),
    ?assertEqual(2, length(Nodes)),
    ?assert(lists:member(<<"list1">>, Nodes)),
    ?assert(lists:member(<<"list2">>, Nodes)),
    Pid1 ! stop,
    Pid2 ! stop,
    ok.

test_conn_mgr_is_connected() ->
    TestPid = spawn(fun() -> receive stop -> ok end end),
    ok = flurm_node_connection_manager:register_connection(<<"conn_check">>, TestPid),
    ?assertEqual(true, flurm_node_connection_manager:is_node_connected(<<"conn_check">>)),
    ?assertEqual(false, flurm_node_connection_manager:is_node_connected(<<"not_connected">>)),
    TestPid ! stop,
    ok.

test_conn_mgr_send_node() ->
    TestPid = spawn(fun() ->
        receive
            {send, _Msg} -> ok
        end
    end),
    ok = flurm_node_connection_manager:register_connection(<<"send_node">>, TestPid),
    Result = flurm_node_connection_manager:send_to_node(<<"send_node">>, #{type => test}),
    ?assertEqual(ok, Result),
    TestPid ! stop,
    ok.

test_conn_mgr_send_not_connected() ->
    Result = flurm_node_connection_manager:send_to_node(<<"not_there">>, #{type => test}),
    ?assertEqual({error, not_connected}, Result),
    ok.

test_conn_mgr_send_nodes() ->
    Pid1 = spawn(fun() -> receive {send, _} -> ok end end),
    Pid2 = spawn(fun() -> receive {send, _} -> ok end end),
    ok = flurm_node_connection_manager:register_connection(<<"multi1">>, Pid1),
    ok = flurm_node_connection_manager:register_connection(<<"multi2">>, Pid2),
    Results = flurm_node_connection_manager:send_to_nodes([<<"multi1">>, <<"multi2">>, <<"not_there">>], #{type => test}),
    ?assertEqual(3, length(Results)),
    %% Check results
    ?assertEqual({<<"multi1">>, ok}, lists:keyfind(<<"multi1">>, 1, Results)),
    ?assertEqual({<<"multi2">>, ok}, lists:keyfind(<<"multi2">>, 1, Results)),
    ?assertEqual({<<"not_there">>, {error, not_connected}}, lists:keyfind(<<"not_there">>, 1, Results)),
    ok.

test_conn_mgr_find_by_socket() ->
    %% find_by_socket uses the caller's pid
    Self = self(),
    ok = flurm_node_connection_manager:register_connection(<<"socket_node">>, Self),
    {ok, <<"socket_node">>} = flurm_node_connection_manager:find_by_socket(fake_socket),
    %% Unregister so we can test error case
    ok = flurm_node_connection_manager:unregister_connection(<<"socket_node">>),
    _ = sys:get_state(flurm_node_connection_manager),
    error = flurm_node_connection_manager:find_by_socket(fake_socket),
    ok.

test_conn_mgr_replace_connection() ->
    Pid1 = spawn(fun() -> receive stop -> ok end end),
    Pid2 = spawn(fun() -> receive stop -> ok end end),
    ok = flurm_node_connection_manager:register_connection(<<"replace_node">>, Pid1),
    {ok, Pid1} = flurm_node_connection_manager:get_connection(<<"replace_node">>),
    %% Replace with new connection
    ok = flurm_node_connection_manager:register_connection(<<"replace_node">>, Pid2),
    {ok, Pid2} = flurm_node_connection_manager:get_connection(<<"replace_node">>),
    Pid1 ! stop,
    Pid2 ! stop,
    ok.

test_conn_mgr_unknown_call() ->
    Result = gen_server:call(flurm_node_connection_manager, {unknown_request}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_conn_mgr_unknown_cast() ->
    ok = gen_server:cast(flurm_node_connection_manager, {unknown_message}),
    _ = sys:get_state(flurm_node_connection_manager),
    %% Should not crash
    Nodes = flurm_node_connection_manager:list_connected_nodes(),
    ?assert(is_list(Nodes)),
    ok.

test_conn_mgr_unknown_info() ->
    flurm_node_connection_manager ! {unknown_info_message},
    _ = sys:get_state(flurm_node_connection_manager),
    %% Should not crash
    Nodes = flurm_node_connection_manager:list_connected_nodes(),
    ?assert(is_list(Nodes)),
    ok.

%%====================================================================
%% flurm_node_manager_server Tests
%%====================================================================

node_manager_server_test_() ->
    {foreach,
     fun setup_node_manager/0,
     fun cleanup_node_manager/1,
     [
        {"Node manager start_link", fun test_node_mgr_start/0},
        {"Node manager register_node", fun test_node_mgr_register/0},
        {"Node manager update_node success", fun test_node_mgr_update/0},
        {"Node manager update_node not found", fun test_node_mgr_update_not_found/0},
        {"Node manager get_node found", fun test_node_mgr_get/0},
        {"Node manager get_node not found", fun test_node_mgr_get_not_found/0},
        {"Node manager list_nodes", fun test_node_mgr_list/0},
        {"Node manager get_available_nodes", fun test_node_mgr_available/0},
        {"Node manager get_available_nodes_for_job", fun test_node_mgr_available_for_job/0},
        {"Node manager allocate_resources success", fun test_node_mgr_allocate/0},
        {"Node manager allocate_resources insufficient", fun test_node_mgr_allocate_insufficient/0},
        {"Node manager allocate_resources not found", fun test_node_mgr_allocate_not_found/0},
        {"Node manager release_resources", fun test_node_mgr_release/0},
        {"Node manager drain_node", fun test_node_mgr_drain/0},
        {"Node manager undrain_node", fun test_node_mgr_undrain/0},
        {"Node manager undrain_node not draining", fun test_node_mgr_undrain_not_draining/0},
        {"Node manager get_drain_reason", fun test_node_mgr_get_drain_reason/0},
        {"Node manager is_node_draining", fun test_node_mgr_is_draining/0},
        {"Node manager heartbeat", fun test_node_mgr_heartbeat/0},
        {"Node manager heartbeat unknown node", fun test_node_mgr_heartbeat_unknown/0},
        {"Node manager add_node", fun test_node_mgr_add/0},
        {"Node manager add_node already registered", fun test_node_mgr_add_already_registered/0},
        {"Node manager remove_node force", fun test_node_mgr_remove_force/0},
        {"Node manager update_node_properties", fun test_node_mgr_update_props/0},
        {"Node manager get_running_jobs_on_node", fun test_node_mgr_running_jobs/0},
        {"Node manager register_node_gres", fun test_node_mgr_register_gres/0},
        {"Node manager get_node_gres", fun test_node_mgr_get_gres/0},
        {"Node manager allocate_gres", fun test_node_mgr_allocate_gres/0},
        {"Node manager release_gres", fun test_node_mgr_release_gres/0},
        {"Node manager get_available_nodes_with_gres", fun test_node_mgr_available_with_gres/0},
        {"Node manager check heartbeats", fun test_node_mgr_check_heartbeats/0},
        {"Node manager unknown call", fun test_node_mgr_unknown_call/0},
        {"Node manager unknown cast", fun test_node_mgr_unknown_cast/0},
        {"Node manager unknown info", fun test_node_mgr_unknown_info/0},
        {"Node manager config_reload_nodes", fun test_node_mgr_config_reload/0},
        {"Node manager config_changed nodes", fun test_node_mgr_config_changed/0},
        {"Node manager config_changed other", fun test_node_mgr_config_changed_other/0}
     ]}.

setup_node_manager() ->
    catch meck:unload(flurm_scheduler),
    meck:new(flurm_scheduler, [passthrough, non_strict]),
    meck:expect(flurm_scheduler, trigger_schedule, fun() -> ok end),
    meck:expect(flurm_scheduler, job_completed, fun(_) -> ok end),
    meck:expect(flurm_scheduler, job_failed, fun(_) -> ok end),
    catch meck:unload(flurm_partition_registry),
    meck:new(flurm_partition_registry, [passthrough, non_strict]),
    meck:expect(flurm_partition_registry, add_node_to_partition, fun(_, _) -> ok end),
    meck:expect(flurm_partition_registry, remove_node_from_partition, fun(_, _) -> ok end),
    catch meck:unload(flurm_config_server),
    meck:new(flurm_config_server, [passthrough, non_strict]),
    meck:expect(flurm_config_server, subscribe_changes, fun(_) -> ok end),
    meck:expect(flurm_config_server, get_nodes, fun() -> [] end),
    meck:expect(flurm_config_server, get_node, fun(_) -> undefined end),
    catch meck:unload(flurm_gres),
    meck:new(flurm_gres, [passthrough, non_strict]),
    meck:expect(flurm_gres, register_node_gres, fun(_, _) -> ok end),
    meck:expect(flurm_gres, deallocate, fun(_, _) -> ok end),
    meck:expect(flurm_gres, parse_gres_string, fun(<<>>) -> {ok, []}; (_) -> {ok, [#{type => gpu, count => 1, name => any}]} end),
    catch meck:unload(lager),
    meck:new(lager, [non_strict, passthrough]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    catch meck:unload(flurm_config_slurm),
    meck:new(flurm_config_slurm, [passthrough, non_strict]),
    meck:expect(flurm_config_slurm, expand_hostlist, fun(Name) -> [Name] end),
    {ok, Pid} = flurm_node_manager_server:start_link(),
    Pid.

cleanup_node_manager(Pid) ->
    unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    meck:unload([flurm_scheduler, flurm_partition_registry, flurm_config_server, flurm_gres, lager, flurm_config_slurm]),
    ok.

make_node_spec(Hostname) ->
    make_node_spec(Hostname, #{}).

make_node_spec(Hostname, Overrides) ->
    Defaults = #{
        hostname => Hostname,
        cpus => 8,
        memory_mb => 16384,
        partitions => [<<"default">>]
    },
    maps:merge(Defaults, Overrides).

test_node_mgr_start() ->
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual([], Nodes),
    ok.

test_node_mgr_register() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"test_node">>)),
    {ok, Node} = flurm_node_manager_server:get_node(<<"test_node">>),
    ?assertEqual(<<"test_node">>, Node#node.hostname),
    ok.

test_node_mgr_update() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"update_node">>)),
    ok = flurm_node_manager_server:update_node(<<"update_node">>, #{state => down}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"update_node">>),
    ?assertEqual(down, Node#node.state),
    ok.

test_node_mgr_update_not_found() ->
    Result = flurm_node_manager_server:update_node(<<"nonexistent">>, #{state => down}),
    ?assertEqual({error, not_found}, Result),
    ok.

test_node_mgr_get() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"get_node">>)),
    {ok, Node} = flurm_node_manager_server:get_node(<<"get_node">>),
    ?assertEqual(<<"get_node">>, Node#node.hostname),
    ok.

test_node_mgr_get_not_found() ->
    Result = flurm_node_manager_server:get_node(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result),
    ok.

test_node_mgr_list() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"list1">>)),
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"list2">>)),
    Nodes = flurm_node_manager_server:list_nodes(),
    ?assertEqual(2, length(Nodes)),
    ok.

test_node_mgr_available() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"avail1">>)),
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"avail2">>)),
    ok = flurm_node_manager_server:update_node(<<"avail2">>, #{state => down}),
    Available = flurm_node_manager_server:get_available_nodes(),
    ?assertEqual(1, length(Available)),
    ok.

test_node_mgr_available_for_job() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"job_node">>, #{cpus => 8, memory_mb => 16384})),
    Nodes = flurm_node_manager_server:get_available_nodes_for_job(4, 8192, <<"default">>),
    ?assertEqual(1, length(Nodes)),
    %% Try to get nodes that require too many resources
    Nodes2 = flurm_node_manager_server:get_available_nodes_for_job(16, 8192, <<"default">>),
    ?assertEqual([], Nodes2),
    ok.

test_node_mgr_allocate() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"alloc_node">>, #{cpus => 8, memory_mb => 16384})),
    ok = flurm_node_manager_server:allocate_resources(<<"alloc_node">>, 1, 4, 8192),
    {ok, Node} = flurm_node_manager_server:get_node(<<"alloc_node">>),
    ?assert(lists:member(1, Node#node.running_jobs)),
    ok.

test_node_mgr_allocate_insufficient() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"insuf_node">>, #{cpus => 4, memory_mb => 4096})),
    Result = flurm_node_manager_server:allocate_resources(<<"insuf_node">>, 1, 8, 8192),
    ?assertEqual({error, insufficient_resources}, Result),
    ok.

test_node_mgr_allocate_not_found() ->
    Result = flurm_node_manager_server:allocate_resources(<<"nonexistent">>, 1, 4, 8192),
    ?assertEqual({error, node_not_found}, Result),
    ok.

test_node_mgr_release() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"release_node">>, #{cpus => 8})),
    ok = flurm_node_manager_server:allocate_resources(<<"release_node">>, 1, 4, 4096),
    {ok, Node1} = flurm_node_manager_server:get_node(<<"release_node">>),
    ?assert(lists:member(1, Node1#node.running_jobs)),
    ok = flurm_node_manager_server:release_resources(<<"release_node">>, 1),
    _ = sys:get_state(flurm_node_manager_server),
    {ok, Node2} = flurm_node_manager_server:get_node(<<"release_node">>),
    ?assertNot(lists:member(1, Node2#node.running_jobs)),
    ok.

test_node_mgr_drain() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"drain_node">>)),
    ok = flurm_node_manager_server:drain_node(<<"drain_node">>, <<"maintenance">>),
    {ok, Node} = flurm_node_manager_server:get_node(<<"drain_node">>),
    ?assertEqual(drain, Node#node.state),
    ?assertEqual(<<"maintenance">>, Node#node.drain_reason),
    ok.

test_node_mgr_undrain() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"undrain_node">>)),
    ok = flurm_node_manager_server:drain_node(<<"undrain_node">>, <<"maintenance">>),
    ok = flurm_node_manager_server:undrain_node(<<"undrain_node">>),
    {ok, Node} = flurm_node_manager_server:get_node(<<"undrain_node">>),
    ?assertEqual(idle, Node#node.state),
    ?assertEqual(undefined, Node#node.drain_reason),
    ok.

test_node_mgr_undrain_not_draining() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"not_drain">>)),
    Result = flurm_node_manager_server:undrain_node(<<"not_drain">>),
    ?assertEqual({error, not_draining}, Result),
    ok.

test_node_mgr_get_drain_reason() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"reason_node">>)),
    ok = flurm_node_manager_server:drain_node(<<"reason_node">>, <<"test reason">>),
    {ok, Reason} = flurm_node_manager_server:get_drain_reason(<<"reason_node">>),
    ?assertEqual(<<"test reason">>, Reason),
    %% Test not draining
    ok = flurm_node_manager_server:undrain_node(<<"reason_node">>),
    Result = flurm_node_manager_server:get_drain_reason(<<"reason_node">>),
    ?assertEqual({error, not_draining}, Result),
    %% Test not found
    Result2 = flurm_node_manager_server:get_drain_reason(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result2),
    ok.

test_node_mgr_is_draining() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"draining_check">>)),
    ?assertEqual(false, flurm_node_manager_server:is_node_draining(<<"draining_check">>)),
    ok = flurm_node_manager_server:drain_node(<<"draining_check">>, <<"test">>),
    ?assertEqual(true, flurm_node_manager_server:is_node_draining(<<"draining_check">>)),
    ?assertEqual({error, not_found}, flurm_node_manager_server:is_node_draining(<<"nonexistent">>)),
    ok.

test_node_mgr_heartbeat() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"hb_node">>)),
    ok = flurm_node_manager_server:heartbeat(#{hostname => <<"hb_node">>, load_avg => 1.5, free_memory_mb => 8192}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"hb_node">>),
    ?assertEqual(1.5, Node#node.load_avg),
    ok.

test_node_mgr_heartbeat_unknown() ->
    ok = flurm_node_manager_server:heartbeat(#{hostname => <<"unknown_node">>}),
    %% Should not crash
    ok.

test_node_mgr_add() ->
    ok = flurm_node_manager_server:add_node(make_node_spec(<<"add_node">>)),
    {ok, Node} = flurm_node_manager_server:get_node(<<"add_node">>),
    ?assertEqual(<<"add_node">>, Node#node.hostname),
    ok.

test_node_mgr_add_already_registered() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"existing_node">>)),
    Result = flurm_node_manager_server:add_node(make_node_spec(<<"existing_node">>)),
    ?assertEqual({error, already_registered}, Result),
    ok.

test_node_mgr_remove_force() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"remove_node">>)),
    ok = flurm_node_manager_server:remove_node(<<"remove_node">>, force),
    Result = flurm_node_manager_server:get_node(<<"remove_node">>),
    ?assertEqual({error, not_found}, Result),
    ok.

test_node_mgr_update_props() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"props_node">>, #{cpus => 4})),
    ok = flurm_node_manager_server:update_node_properties(<<"props_node">>, #{cpus => 8, memory_mb => 32768}),
    {ok, Node} = flurm_node_manager_server:get_node(<<"props_node">>),
    ?assertEqual(8, Node#node.cpus),
    ?assertEqual(32768, Node#node.memory_mb),
    ok.

test_node_mgr_running_jobs() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"jobs_node">>)),
    ok = flurm_node_manager_server:allocate_resources(<<"jobs_node">>, 100, 2, 1024),
    ok = flurm_node_manager_server:allocate_resources(<<"jobs_node">>, 200, 2, 1024),
    {ok, Jobs} = flurm_node_manager_server:get_running_jobs_on_node(<<"jobs_node">>),
    ?assertEqual(2, length(Jobs)),
    ?assert(lists:member(100, Jobs)),
    ?assert(lists:member(200, Jobs)),
    %% Test not found
    Result = flurm_node_manager_server:get_running_jobs_on_node(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result),
    ok.

test_node_mgr_register_gres() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"gres_node">>)),
    GRESList = [#{type => gpu, name => <<"a100">>, count => 4, index => 0}],
    ok = flurm_node_manager_server:register_node_gres(<<"gres_node">>, GRESList),
    {ok, GRESInfo} = flurm_node_manager_server:get_node_gres(<<"gres_node">>),
    ?assertEqual(4, maps:get(<<"gpu">>, maps:get(total, GRESInfo))),
    ok.

test_node_mgr_get_gres() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"get_gres_node">>)),
    {ok, GRESInfo} = flurm_node_manager_server:get_node_gres(<<"get_gres_node">>),
    ?assert(is_map(GRESInfo)),
    %% Test not found
    Result = flurm_node_manager_server:get_node_gres(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result),
    ok.

test_node_mgr_allocate_gres() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"alloc_gres_node">>)),
    GRESList = [#{type => gpu, name => <<"a100">>, count => 4, index => 0}],
    ok = flurm_node_manager_server:register_node_gres(<<"alloc_gres_node">>, GRESList),
    {ok, Allocations} = flurm_node_manager_server:allocate_gres(<<"alloc_gres_node">>, 1, <<"gpu:1">>, false),
    ?assert(is_list(Allocations)),
    ok.

test_node_mgr_release_gres() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"rel_gres_node">>)),
    GRESList = [#{type => gpu, name => <<"a100">>, count => 4, index => 0}],
    ok = flurm_node_manager_server:register_node_gres(<<"rel_gres_node">>, GRESList),
    {ok, _} = flurm_node_manager_server:allocate_gres(<<"rel_gres_node">>, 1, <<"gpu:2">>, false),
    ok = flurm_node_manager_server:release_gres(<<"rel_gres_node">>, 1),
    _ = sys:get_state(flurm_node_manager_server),
    ok.

test_node_mgr_available_with_gres() ->
    ok = flurm_node_manager_server:register_node(make_node_spec(<<"gres_avail_node">>, #{cpus => 8, memory_mb => 16384})),
    GRESList = [#{type => gpu, name => <<"a100">>, count => 4, index => 0}],
    ok = flurm_node_manager_server:register_node_gres(<<"gres_avail_node">>, GRESList),
    Nodes = flurm_node_manager_server:get_available_nodes_with_gres(4, 8192, <<"default">>, <<>>),
    ?assertEqual(1, length(Nodes)),
    ok.

test_node_mgr_check_heartbeats() ->
    %% Trigger heartbeat check manually
    flurm_node_manager_server ! check_heartbeats,
    _ = sys:get_state(flurm_node_manager_server),
    %% Should not crash
    ok.

test_node_mgr_unknown_call() ->
    Result = gen_server:call(flurm_node_manager_server, {unknown_request}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_node_mgr_unknown_cast() ->
    ok = gen_server:cast(flurm_node_manager_server, {unknown_message}),
    _ = sys:get_state(flurm_node_manager_server),
    %% Should not crash
    ok.

test_node_mgr_unknown_info() ->
    flurm_node_manager_server ! {unknown_info_message},
    _ = sys:get_state(flurm_node_manager_server),
    %% Should not crash
    ok.

test_node_mgr_config_reload() ->
    flurm_node_manager_server ! {config_reload_nodes, [#{nodename => <<"config_node">>, cpus => 4}]},
    _ = sys:get_state(flurm_node_manager_server),
    %% Should not crash
    ok.

test_node_mgr_config_changed() ->
    flurm_node_manager_server ! {config_changed, nodes, [], [#{nodename => <<"changed_node">>}]},
    _ = sys:get_state(flurm_node_manager_server),
    %% Should not crash
    ok.

test_node_mgr_config_changed_other() ->
    flurm_node_manager_server ! {config_changed, partitions, [], []},
    _ = sys:get_state(flurm_node_manager_server),
    %% Should not crash (ignored)
    ok.
