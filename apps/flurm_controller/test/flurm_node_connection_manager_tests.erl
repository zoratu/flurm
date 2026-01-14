%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_node_connection_manager Module
%%%
%%% Tests the gen_server that tracks TCP connections from node daemons.
%%% Tests cover all exported API functions and gen_server callbacks.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_connection_manager_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

connection_manager_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link registers server", fun test_start_link/0},
        {"register_connection stores connection", fun test_register_connection/0},
        {"register_connection replaces existing", fun test_register_connection_replace/0},
        {"unregister_connection removes connection", fun test_unregister_connection/0},
        {"unregister_connection handles missing", fun test_unregister_connection_missing/0},
        {"get_connection returns pid", fun test_get_connection_exists/0},
        {"get_connection returns error for missing", fun test_get_connection_missing/0},
        {"find_by_socket returns hostname", fun test_find_by_socket/0},
        {"find_by_socket returns error for unknown", fun test_find_by_socket_unknown/0},
        {"send_to_node sends message", fun test_send_to_node/0},
        {"send_to_node returns error for disconnected", fun test_send_to_node_disconnected/0},
        {"send_to_nodes sends to multiple", fun test_send_to_nodes/0},
        {"send_to_nodes handles mixed connected/disconnected", fun test_send_to_nodes_mixed/0},
        {"list_connected_nodes returns all hostnames", fun test_list_connected_nodes/0},
        {"list_connected_nodes returns empty when none", fun test_list_connected_nodes_empty/0},
        {"is_node_connected returns true", fun test_is_node_connected_true/0},
        {"is_node_connected returns false", fun test_is_node_connected_false/0},
        {"process DOWN removes connection", fun test_process_down/0},
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast is ignored", fun test_unknown_cast/0},
        {"unknown info is ignored", fun test_unknown_info/0},
        {"terminate returns ok", fun test_terminate/0}
     ]}.

setup() ->
    %% Mock flurm_node_manager_server for DOWN handling
    meck:new(flurm_node_manager_server, [non_strict]),
    meck:expect(flurm_node_manager_server, update_node, fun(_, _) -> ok end),

    %% Stop any existing server
    case whereis(flurm_node_connection_manager) of
        undefined -> ok;
        Pid ->
            gen_server:stop(Pid),
            timer:sleep(50)
    end,

    %% Start fresh server
    {ok, ServerPid} = flurm_node_connection_manager:start_link(),
    {server_pid, ServerPid}.

cleanup({server_pid, Pid}) ->
    catch gen_server:stop(Pid),
    catch meck:unload(flurm_node_manager_server),
    timer:sleep(50),
    ok.

%%====================================================================
%% Start Link Tests
%%====================================================================

test_start_link() ->
    %% Server already started in setup
    Pid = whereis(flurm_node_connection_manager),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ok.

%%====================================================================
%% Register Connection Tests
%%====================================================================

test_register_connection() ->
    FakePid = spawn(fun() -> receive stop -> ok end end),
    Hostname = <<"node001">>,

    Result = flurm_node_connection_manager:register_connection(Hostname, FakePid),

    ?assertEqual(ok, Result),

    %% Verify it's stored
    {ok, StoredPid} = flurm_node_connection_manager:get_connection(Hostname),
    ?assertEqual(FakePid, StoredPid),

    FakePid ! stop,
    ok.

test_register_connection_replace() ->
    OldPid = spawn(fun() -> receive stop -> ok end end),
    NewPid = spawn(fun() -> receive stop -> ok end end),
    Hostname = <<"node002">>,

    %% Register first connection
    ok = flurm_node_connection_manager:register_connection(Hostname, OldPid),
    {ok, StoredPid1} = flurm_node_connection_manager:get_connection(Hostname),
    ?assertEqual(OldPid, StoredPid1),

    %% Register replacement
    ok = flurm_node_connection_manager:register_connection(Hostname, NewPid),
    {ok, StoredPid2} = flurm_node_connection_manager:get_connection(Hostname),
    ?assertEqual(NewPid, StoredPid2),

    OldPid ! stop,
    NewPid ! stop,
    ok.

%%====================================================================
%% Unregister Connection Tests
%%====================================================================

test_unregister_connection() ->
    FakePid = spawn(fun() -> receive stop -> ok end end),
    Hostname = <<"node003">>,

    ok = flurm_node_connection_manager:register_connection(Hostname, FakePid),
    ?assert(flurm_node_connection_manager:is_node_connected(Hostname)),

    ok = flurm_node_connection_manager:unregister_connection(Hostname),
    timer:sleep(50),  % Cast is async

    ?assertNot(flurm_node_connection_manager:is_node_connected(Hostname)),
    FakePid ! stop,
    ok.

test_unregister_connection_missing() ->
    %% Should not crash when unregistering non-existent connection
    ok = flurm_node_connection_manager:unregister_connection(<<"nonexistent">>),
    timer:sleep(50),
    ok.

%%====================================================================
%% Get Connection Tests
%%====================================================================

test_get_connection_exists() ->
    FakePid = spawn(fun() -> receive stop -> ok end end),
    Hostname = <<"node004">>,

    ok = flurm_node_connection_manager:register_connection(Hostname, FakePid),

    Result = flurm_node_connection_manager:get_connection(Hostname),

    ?assertEqual({ok, FakePid}, Result),
    FakePid ! stop,
    ok.

test_get_connection_missing() ->
    Result = flurm_node_connection_manager:get_connection(<<"nonexistent_node">>),
    ?assertEqual({error, not_connected}, Result),
    ok.

%%====================================================================
%% Find By Socket Tests
%%====================================================================

test_find_by_socket() ->
    %% The find_by_socket function uses the caller's pid to find hostname
    %% We need to call it from the registered connection process
    Hostname = <<"node005">>,
    Parent = self(),

    ConnPid = spawn(fun() ->
        receive
            do_find ->
                Result = flurm_node_connection_manager:find_by_socket(fake_socket),
                Parent ! {find_result, Result}
        end,
        receive stop -> ok end
    end),

    ok = flurm_node_connection_manager:register_connection(Hostname, ConnPid),
    ConnPid ! do_find,

    receive
        {find_result, {ok, FoundHostname}} ->
            ?assertEqual(Hostname, FoundHostname)
    after 1000 ->
        ?assert(false)
    end,

    ConnPid ! stop,
    ok.

test_find_by_socket_unknown() ->
    %% Call from an unregistered process
    Result = flurm_node_connection_manager:find_by_socket(fake_socket),
    ?assertEqual(error, Result),
    ok.

%%====================================================================
%% Send To Node Tests
%%====================================================================

test_send_to_node() ->
    Parent = self(),
    TestMessage = #{type => test_msg, payload => <<"data">>},

    ConnPid = spawn(fun() ->
        receive
            {send, Msg} ->
                Parent ! {received, Msg}
        end,
        receive stop -> ok end
    end),

    Hostname = <<"node006">>,
    ok = flurm_node_connection_manager:register_connection(Hostname, ConnPid),

    Result = flurm_node_connection_manager:send_to_node(Hostname, TestMessage),

    ?assertEqual(ok, Result),

    receive
        {received, ReceivedMsg} ->
            ?assertEqual(TestMessage, ReceivedMsg)
    after 1000 ->
        ?assert(false)
    end,

    ConnPid ! stop,
    ok.

test_send_to_node_disconnected() ->
    TestMessage = #{type => test_msg, payload => <<"data">>},
    Result = flurm_node_connection_manager:send_to_node(<<"disconnected_node">>, TestMessage),
    ?assertEqual({error, not_connected}, Result),
    ok.

%%====================================================================
%% Send To Nodes Tests
%%====================================================================

test_send_to_nodes() ->
    Parent = self(),
    TestMessage = #{type => broadcast, payload => <<"data">>},

    ConnPid1 = spawn(fun() ->
        receive {send, Msg} -> Parent ! {received, node1, Msg} end,
        receive stop -> ok end
    end),

    ConnPid2 = spawn(fun() ->
        receive {send, Msg} -> Parent ! {received, node2, Msg} end,
        receive stop -> ok end
    end),

    ok = flurm_node_connection_manager:register_connection(<<"node007">>, ConnPid1),
    ok = flurm_node_connection_manager:register_connection(<<"node008">>, ConnPid2),

    Results = flurm_node_connection_manager:send_to_nodes(
        [<<"node007">>, <<"node008">>],
        TestMessage
    ),

    ?assertEqual([{<<"node007">>, ok}, {<<"node008">>, ok}], Results),

    %% Verify both received the message
    receive {received, node1, _} -> ok after 1000 -> ?assert(false) end,
    receive {received, node2, _} -> ok after 1000 -> ?assert(false) end,

    ConnPid1 ! stop,
    ConnPid2 ! stop,
    ok.

test_send_to_nodes_mixed() ->
    Parent = self(),
    TestMessage = #{type => broadcast, payload => <<"data">>},

    ConnPid = spawn(fun() ->
        receive {send, Msg} -> Parent ! {received, Msg} end,
        receive stop -> ok end
    end),

    ok = flurm_node_connection_manager:register_connection(<<"node009">>, ConnPid),

    Results = flurm_node_connection_manager:send_to_nodes(
        [<<"node009">>, <<"disconnected_node">>],
        TestMessage
    ),

    ?assertEqual([{<<"node009">>, ok}, {<<"disconnected_node">>, {error, not_connected}}], Results),

    ConnPid ! stop,
    ok.

%%====================================================================
%% List Connected Nodes Tests
%%====================================================================

test_list_connected_nodes() ->
    FakePid1 = spawn(fun() -> receive stop -> ok end end),
    FakePid2 = spawn(fun() -> receive stop -> ok end end),
    FakePid3 = spawn(fun() -> receive stop -> ok end end),

    ok = flurm_node_connection_manager:register_connection(<<"nodeA">>, FakePid1),
    ok = flurm_node_connection_manager:register_connection(<<"nodeB">>, FakePid2),
    ok = flurm_node_connection_manager:register_connection(<<"nodeC">>, FakePid3),

    ConnectedNodes = flurm_node_connection_manager:list_connected_nodes(),

    ?assert(lists:member(<<"nodeA">>, ConnectedNodes)),
    ?assert(lists:member(<<"nodeB">>, ConnectedNodes)),
    ?assert(lists:member(<<"nodeC">>, ConnectedNodes)),

    FakePid1 ! stop,
    FakePid2 ! stop,
    FakePid3 ! stop,
    ok.

test_list_connected_nodes_empty() ->
    %% Start fresh to ensure no connections
    gen_server:stop(flurm_node_connection_manager),
    timer:sleep(50),
    {ok, _} = flurm_node_connection_manager:start_link(),

    ConnectedNodes = flurm_node_connection_manager:list_connected_nodes(),

    ?assertEqual([], ConnectedNodes),
    ok.

%%====================================================================
%% Is Node Connected Tests
%%====================================================================

test_is_node_connected_true() ->
    FakePid = spawn(fun() -> receive stop -> ok end end),
    Hostname = <<"node010">>,

    ok = flurm_node_connection_manager:register_connection(Hostname, FakePid),

    Result = flurm_node_connection_manager:is_node_connected(Hostname),

    ?assertEqual(true, Result),
    FakePid ! stop,
    ok.

test_is_node_connected_false() ->
    Result = flurm_node_connection_manager:is_node_connected(<<"not_connected_node">>),
    ?assertEqual(false, Result),
    ok.

%%====================================================================
%% Process DOWN Tests
%%====================================================================

test_process_down() ->
    Hostname = <<"node011">>,

    %% Create a process that will die
    ConnPid = spawn(fun() -> receive stop -> ok end end),
    ok = flurm_node_connection_manager:register_connection(Hostname, ConnPid),

    ?assert(flurm_node_connection_manager:is_node_connected(Hostname)),

    %% Kill the connection process
    ConnPid ! stop,
    timer:sleep(100),  % Wait for DOWN message to be processed

    %% Connection should be removed
    ?assertNot(flurm_node_connection_manager:is_node_connected(Hostname)),

    %% Verify update_node was called
    ?assert(meck:called(flurm_node_manager_server, update_node, [Hostname, #{state => down}])),
    ok.

%%====================================================================
%% Unknown Message Tests
%%====================================================================

test_unknown_call() ->
    Result = gen_server:call(flurm_node_connection_manager, {unknown_request, data}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    %% Should not crash
    gen_server:cast(flurm_node_connection_manager, {unknown_cast, data}),
    timer:sleep(50),
    %% Server should still be alive
    ?assert(is_process_alive(whereis(flurm_node_connection_manager))),
    ok.

test_unknown_info() ->
    %% Send unknown info message directly
    whereis(flurm_node_connection_manager) ! {unknown_info, data},
    timer:sleep(50),
    %% Server should still be alive
    ?assert(is_process_alive(whereis(flurm_node_connection_manager))),
    ok.

%%====================================================================
%% Terminate Test
%%====================================================================

test_terminate() ->
    %% Verify server terminates cleanly
    Pid = whereis(flurm_node_connection_manager),
    gen_server:stop(Pid),
    timer:sleep(50),
    ?assertEqual(undefined, whereis(flurm_node_connection_manager)),

    %% Restart for cleanup
    {ok, _} = flurm_node_connection_manager:start_link(),
    ok.

%%====================================================================
%% Multiple Connections Tests
%%====================================================================

multiple_connections_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"handles many concurrent connections", fun test_many_connections/0},
        {"concurrent register/unregister", fun test_concurrent_operations/0}
     ]}.

test_many_connections() ->
    NumConnections = 100,
    Pids = [spawn(fun() -> receive stop -> ok end end) || _ <- lists:seq(1, NumConnections)],
    Hostnames = [list_to_binary("node_" ++ integer_to_list(I)) || I <- lists:seq(1, NumConnections)],

    %% Register all connections
    lists:foreach(fun({Hostname, Pid}) ->
        ok = flurm_node_connection_manager:register_connection(Hostname, Pid)
    end, lists:zip(Hostnames, Pids)),

    %% Verify all are connected
    ConnectedNodes = flurm_node_connection_manager:list_connected_nodes(),
    ?assertEqual(NumConnections, length(ConnectedNodes)),

    %% Cleanup
    lists:foreach(fun(Pid) -> Pid ! stop end, Pids),
    ok.

test_concurrent_operations() ->
    Parent = self(),
    NumOps = 50,

    %% Spawn processes that register and unregister concurrently
    Pids = [spawn(fun() ->
        Hostname = list_to_binary("conc_node_" ++ integer_to_list(I)),
        ConnPid = spawn(fun() -> receive stop -> ok end end),
        ok = flurm_node_connection_manager:register_connection(Hostname, ConnPid),
        _ = flurm_node_connection_manager:is_node_connected(Hostname),
        _ = flurm_node_connection_manager:list_connected_nodes(),
        ok = flurm_node_connection_manager:unregister_connection(Hostname),
        ConnPid ! stop,
        Parent ! {done, self()}
    end) || I <- lists:seq(1, NumOps)],

    %% Wait for all to complete
    lists:foreach(fun(Pid) ->
        receive {done, Pid} -> ok after 5000 -> ?assert(false) end
    end, Pids),

    ok.

%%====================================================================
%% Init Tests
%%====================================================================

init_test_() ->
    [
        {"init returns empty state", fun test_init_state/0}
    ].

test_init_state() ->
    %% Stop existing server
    case whereis(flurm_node_connection_manager) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid), timer:sleep(50)
    end,

    {ok, NewPid} = flurm_node_connection_manager:start_link(),
    ?assert(is_pid(NewPid)),

    %% Fresh server should have no connections
    ?assertEqual([], flurm_node_connection_manager:list_connected_nodes()),

    gen_server:stop(NewPid),
    ok.
