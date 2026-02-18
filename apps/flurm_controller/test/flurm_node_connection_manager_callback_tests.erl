%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_node_connection_manager gen_server callbacks
%%%
%%% Tests the callback functions directly without starting the process.
%%% Uses meck to mock dependencies.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_connection_manager_callback_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% State record copied from module (internal)
-record(state, {
    connections = #{} :: #{binary() => pid()},
    pids_to_nodes = #{} :: #{pid() => binary()}
}).

make_state() ->
    #state{}.

make_state_with_connections(Conns) ->
    P2N = maps:fold(fun(Host, Pid, Acc) -> maps:put(Pid, Host, Acc) end, #{}, Conns),
    #state{connections = Conns, pids_to_nodes = P2N}.

setup() ->
    catch meck:unload(error_logger),
    catch meck:unload(flurm_node_manager_server),
    meck:new(error_logger, [passthrough, unstick, no_link]),
    meck:expect(error_logger, info_msg, fun(_Fmt, _Args) -> ok end),
    meck:expect(error_logger, warning_msg, fun(_Fmt, _Args) -> ok end),
    meck:expect(error_logger, error_msg, fun(_Fmt, _Args) -> ok end),
    meck:new(flurm_node_manager_server, [non_strict, no_link, no_passthrough_cover]),
    meck:expect(flurm_node_manager_server, update_node, fun(_Hostname, _Updates) -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(flurm_node_manager_server),
    meck:unload(error_logger),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

node_connection_manager_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"init creates empty state", fun init_creates_empty_state/0},
      {"register connection success", fun handle_call_register_connection_success/0},
      {"register connection replaces existing", fun handle_call_register_connection_replaces/0},
      {"get_connection success", fun handle_call_get_connection_success/0},
      {"get_connection not connected", fun handle_call_get_connection_not_connected/0},
      {"find_by_socket success", fun handle_call_find_by_socket_success/0},
      {"find_by_socket not found", fun handle_call_find_by_socket_not_found/0},
      {"send_to_node success", fun handle_call_send_to_node_success/0},
      {"send_to_node not connected", fun handle_call_send_to_node_not_connected/0},
      {"send_to_nodes success", fun handle_call_send_to_nodes_success/0},
      {"send_to_nodes mixed results", fun handle_call_send_to_nodes_mixed/0},
      {"list_connected_nodes", fun handle_call_list_connected_nodes/0},
      {"is_connected true", fun handle_call_is_connected_true/0},
      {"is_connected false", fun handle_call_is_connected_false/0},
      {"unknown request", fun handle_call_unknown_request/0},
      {"unregister via cast", fun handle_cast_unregister/0},
      {"unregister nonexistent", fun handle_cast_unregister_nonexistent/0},
      {"handle_cast ignores other messages", fun handle_cast_ignores_messages/0},
      {"handle_info DOWN removes connection", fun handle_info_down_removes_connection/0},
      {"handle_info DOWN unknown pid", fun handle_info_down_unknown_pid/0},
      {"handle_info ignores other messages", fun handle_info_ignores_messages/0},
      {"terminate returns ok", fun terminate_returns_ok/0},
      {"log helper", fun log_helper_test/0}
     ]}.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_creates_empty_state() ->
    {ok, State} = flurm_node_connection_manager:init([]),
    ?assertEqual(#{}, State#state.connections),
    ?assertEqual(#{}, State#state.pids_to_nodes).

%%====================================================================
%% handle_call/3 Tests - register
%%====================================================================

handle_call_register_connection_success() ->
    State = make_state(),
    Pid = self(),
    {reply, ok, NewState} = flurm_node_connection_manager:handle_call({register, <<"node1">>, Pid}, {Pid, make_ref()}, State),
    ?assertEqual(Pid, maps:get(<<"node1">>, NewState#state.connections)),
    ?assertEqual(<<"node1">>, maps:get(Pid, NewState#state.pids_to_nodes)).

handle_call_register_connection_replaces() ->
    OldPid = spawn(fun() -> receive stop -> ok end end),
    State = make_state_with_connections(#{<<"node1">> => OldPid}),
    NewPid = self(),
    {reply, ok, NewState} = flurm_node_connection_manager:handle_call({register, <<"node1">>, NewPid}, {NewPid, make_ref()}, State),
    ?assertEqual(NewPid, maps:get(<<"node1">>, NewState#state.connections)),
    ?assertEqual(<<"node1">>, maps:get(NewPid, NewState#state.pids_to_nodes)),
    ?assertNot(maps:is_key(OldPid, NewState#state.pids_to_nodes)),
    OldPid ! stop.

%%====================================================================
%% handle_call/3 Tests - get_connection
%%====================================================================

handle_call_get_connection_success() ->
    Pid = self(),
    State = make_state_with_connections(#{<<"node1">> => Pid}),
    {reply, {ok, Pid}, NewState} = flurm_node_connection_manager:handle_call({get_connection, <<"node1">>}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

handle_call_get_connection_not_connected() ->
    State = make_state(),
    {reply, {error, not_connected}, NewState} = flurm_node_connection_manager:handle_call({get_connection, <<"node1">>}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - find_by_socket
%%====================================================================

handle_call_find_by_socket_success() ->
    Pid = self(),
    State = make_state_with_connections(#{<<"node1">> => Pid}),
    {reply, {ok, <<"node1">>}, NewState} = flurm_node_connection_manager:handle_call({find_by_socket, some_socket}, {Pid, make_ref()}, State),
    ?assertEqual(State, NewState).

handle_call_find_by_socket_not_found() ->
    State = make_state(),
    {reply, error, NewState} = flurm_node_connection_manager:handle_call({find_by_socket, some_socket}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - send_to_node
%%====================================================================

handle_call_send_to_node_success() ->
    %% Create a process that will receive the message
    TestPid = self(),
    ReceiverPid = spawn(fun() ->
        receive
            {send, Msg} -> TestPid ! {received, Msg}
        end
    end),
    State = make_state_with_connections(#{<<"node1">> => ReceiverPid}),
    Message = #{type => ping},
    {reply, ok, NewState} = flurm_node_connection_manager:handle_call({send_to_node, <<"node1">>, Message}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState),
    receive
        {received, Msg} -> ?assertEqual(Message, Msg)
    after 1000 ->
        ?assert(false)
    end.

handle_call_send_to_node_not_connected() ->
    State = make_state(),
    Message = #{type => ping},
    {reply, {error, not_connected}, NewState} = flurm_node_connection_manager:handle_call({send_to_node, <<"node1">>, Message}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - send_to_nodes
%%====================================================================

handle_call_send_to_nodes_success() ->
    TestPid = self(),
    Receiver1 = spawn(fun() -> receive {send, _} -> TestPid ! received1 end end),
    Receiver2 = spawn(fun() -> receive {send, _} -> TestPid ! received2 end end),
    State = make_state_with_connections(#{<<"node1">> => Receiver1, <<"node2">> => Receiver2}),
    Message = #{type => ping},
    {reply, Results, _NewState} = flurm_node_connection_manager:handle_call({send_to_nodes, [<<"node1">>, <<"node2">>], Message}, {self(), make_ref()}, State),
    ?assertEqual([{<<"node1">>, ok}, {<<"node2">>, ok}], Results),
    receive received1 -> ok after 1000 -> ?assert(false) end,
    receive received2 -> ok after 1000 -> ?assert(false) end.

handle_call_send_to_nodes_mixed() ->
    TestPid = self(),
    Receiver1 = spawn(fun() -> receive {send, _} -> TestPid ! received1 end end),
    State = make_state_with_connections(#{<<"node1">> => Receiver1}),
    Message = #{type => ping},
    {reply, Results, _NewState} = flurm_node_connection_manager:handle_call({send_to_nodes, [<<"node1">>, <<"node2">>], Message}, {self(), make_ref()}, State),
    ?assertEqual([{<<"node1">>, ok}, {<<"node2">>, {error, not_connected}}], Results),
    receive received1 -> ok after 1000 -> ?assert(false) end.

%%====================================================================
%% handle_call/3 Tests - list_connected_nodes
%%====================================================================

handle_call_list_connected_nodes() ->
    Pid1 = spawn(fun() -> receive stop -> ok end end),
    Pid2 = spawn(fun() -> receive stop -> ok end end),
    State = make_state_with_connections(#{<<"node1">> => Pid1, <<"node2">> => Pid2}),
    {reply, Nodes, _NewState} = flurm_node_connection_manager:handle_call(list_connected_nodes, {self(), make_ref()}, State),
    ?assertEqual(2, length(Nodes)),
    ?assert(lists:member(<<"node1">>, Nodes)),
    ?assert(lists:member(<<"node2">>, Nodes)),
    Pid1 ! stop,
    Pid2 ! stop.

%%====================================================================
%% handle_call/3 Tests - is_connected
%%====================================================================

handle_call_is_connected_true() ->
    Pid = self(),
    State = make_state_with_connections(#{<<"node1">> => Pid}),
    {reply, true, _NewState} = flurm_node_connection_manager:handle_call({is_connected, <<"node1">>}, {self(), make_ref()}, State).

handle_call_is_connected_false() ->
    State = make_state(),
    {reply, false, _NewState} = flurm_node_connection_manager:handle_call({is_connected, <<"node1">>}, {self(), make_ref()}, State).

%%====================================================================
%% handle_call/3 Tests - unknown request
%%====================================================================

handle_call_unknown_request() ->
    State = make_state(),
    {reply, {error, unknown_request}, NewState} = flurm_node_connection_manager:handle_call({unknown, request}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_unregister() ->
    Pid = self(),
    State = make_state_with_connections(#{<<"node1">> => Pid}),
    {noreply, NewState} = flurm_node_connection_manager:handle_cast({unregister, <<"node1">>}, State),
    ?assertNot(maps:is_key(<<"node1">>, NewState#state.connections)),
    ?assertNot(maps:is_key(Pid, NewState#state.pids_to_nodes)).

handle_cast_unregister_nonexistent() ->
    State = make_state(),
    {noreply, NewState} = flurm_node_connection_manager:handle_cast({unregister, <<"nonexistent">>}, State),
    ?assertEqual(State, NewState).

handle_cast_ignores_messages() ->
    State = make_state(),
    {noreply, NewState} = flurm_node_connection_manager:handle_cast(any_other_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_down_removes_connection() ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    State = make_state_with_connections(#{<<"node1">> => Pid}),
    DownMsg = {'DOWN', make_ref(), process, Pid, normal},
    {noreply, NewState} = flurm_node_connection_manager:handle_info(DownMsg, State),
    ?assertNot(maps:is_key(<<"node1">>, NewState#state.connections)),
    ?assertNot(maps:is_key(Pid, NewState#state.pids_to_nodes)),
    %% Verify update_node was called
    ?assert(meck:called(flurm_node_manager_server, update_node, [<<"node1">>, #{state => down}])).

handle_info_down_unknown_pid() ->
    UnknownPid = spawn(fun() -> receive stop -> ok end end),
    State = make_state(),
    DownMsg = {'DOWN', make_ref(), process, UnknownPid, normal},
    {noreply, NewState} = flurm_node_connection_manager:handle_info(DownMsg, State),
    ?assertEqual(State, NewState).

handle_info_ignores_messages() ->
    State = make_state(),
    {noreply, NewState} = flurm_node_connection_manager:handle_info(any_info, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_returns_ok() ->
    State = make_state(),
    ?assertEqual(ok, flurm_node_connection_manager:terminate(normal, State)),
    ?assertEqual(ok, flurm_node_connection_manager:terminate(shutdown, State)).

%%====================================================================
%% Internal Helper Tests
%%====================================================================

log_helper_test() ->
    %% Test that log function doesn't crash
    ?assertEqual(ok, flurm_node_connection_manager:log(debug, "test ~p", [1])),
    ?assertEqual(ok, flurm_node_connection_manager:log(info, "test ~p", [2])),
    ?assertEqual(ok, flurm_node_connection_manager:log(warning, "test ~p", [3])),
    ?assertEqual(ok, flurm_node_connection_manager:log(error, "test ~p", [4])).
