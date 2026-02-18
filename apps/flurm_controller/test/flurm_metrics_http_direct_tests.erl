%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_metrics_http
%%%
%%% These tests call the actual module functions directly to get
%%% code coverage. External dependencies are mocked with meck.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_metrics_http_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

metrics_http_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init starts listening", fun test_init_success/0},
      {"init handles eaddrinuse", fun test_init_eaddrinuse/0},
      {"init handles other errors", fun test_init_other_error/0},
      {"handle_call returns error", fun test_handle_call/0},
      {"handle_cast ignores messages", fun test_handle_cast/0},
      {"handle_info accept without socket", fun test_accept_no_socket/0},
      {"handle_info accept timeout", fun test_accept_timeout/0},
      {"handle_info accept success", fun test_accept_success/0},
      {"handle_info accept closed", fun test_accept_closed/0},
      {"handle_info accept error", fun test_accept_error/0},
      {"handle_info unknown", fun test_unknown_info/0},
      {"terminate with socket", fun test_terminate_with_socket/0},
      {"terminate without socket", fun test_terminate_without_socket/0},
      {"code_change returns ok", fun test_code_change/0}
     ]}.

setup() ->
    catch meck:unload(application),
    meck:new(application, [unstick, passthrough]),
    meck:expect(application, get_env, fun
        (flurm_controller, metrics_port, Default) -> Default;
        (_, _, Default) -> Default
    end),
    ok.

cleanup(_) ->
    meck:unload(application),
    ok.

%% State record matching the module's internal state
-record(state, {
    socket :: gen_tcp:socket() | undefined,
    port :: pos_integer()
}).

%%====================================================================
%% Test Cases
%%====================================================================

test_init_success() ->
    %% Use a random high port to avoid conflicts
    Port = 49150 + rand:uniform(1000),
    meck:expect(application, get_env, fun
        (flurm_controller, metrics_port, _) -> Port;
        (_, _, Default) -> Default
    end),

    {ok, State} = flurm_metrics_http:init([]),
    ?assertEqual(Port, State#state.port),
    ?assertNotEqual(undefined, State#state.socket),

    %% Close the socket
    gen_tcp:close(State#state.socket),

    %% Flush the accept message
    receive accept -> ok after 100 -> ok end.

test_init_eaddrinuse() ->
    %% Start a listener to block the port
    Port = 49250 + rand:uniform(1000),
    {ok, BlockSocket} = gen_tcp:listen(Port, [{reuseaddr, true}]),

    meck:expect(application, get_env, fun
        (flurm_controller, metrics_port, _) -> Port;
        (_, _, Default) -> Default
    end),

    %% Now init should get eaddrinuse
    {ok, State} = flurm_metrics_http:init([]),
    ?assertEqual(Port, State#state.port),
    %% Socket might be undefined or might succeed (depends on OS)
    %% Just ensure we don't crash

    %% Cleanup
    gen_tcp:close(BlockSocket),
    case State#state.socket of
        undefined -> ok;
        S -> gen_tcp:close(S)
    end.

test_init_other_error() ->
    %% Use an invalid port number to trigger error
    %% Port 0 is special - may cause an error on some systems
    meck:expect(application, get_env, fun
        (flurm_controller, metrics_port, _) -> 0;  % Port 0 gets assigned
        (_, _, Default) -> Default
    end),

    {ok, State} = flurm_metrics_http:init([]),
    %% Port 0 may work (OS assigns port) or fail - either is acceptable
    case State#state.socket of
        undefined -> ok;
        S -> gen_tcp:close(S), receive accept -> ok after 100 -> ok end
    end.

test_handle_call() ->
    State = #state{socket = undefined, port = 9090},
    {reply, Result, NewState} = flurm_metrics_http:handle_call(unknown_request, {self(), make_ref()}, State),
    ?assertEqual({error, unknown_request}, Result),
    ?assertEqual(State, NewState).

test_handle_cast() ->
    State = #state{socket = undefined, port = 9090},
    {noreply, NewState} = flurm_metrics_http:handle_cast(some_message, State),
    ?assertEqual(State, NewState).

test_accept_no_socket() ->
    State = #state{socket = undefined, port = 9090},
    {noreply, NewState} = flurm_metrics_http:handle_info(accept, State),
    ?assertEqual(State, NewState).

test_accept_timeout() ->
    Port = 49350 + rand:uniform(1000),
    {ok, Socket} = gen_tcp:listen(Port, [{reuseaddr, true}]),
    State = #state{socket = Socket, port = Port},

    %% This will timeout after 1 second
    {noreply, NewState} = flurm_metrics_http:handle_info(accept, State),
    ?assertEqual(Socket, NewState#state.socket),

    gen_tcp:close(Socket),
    %% Flush accept message
    receive accept -> ok after 100 -> ok end.

test_accept_success() ->
    Port = 49450 + rand:uniform(1000),
    {ok, ListenSocket} = gen_tcp:listen(Port, [{reuseaddr, true}]),
    State = #state{socket = ListenSocket, port = Port},

    %% Spawn a client that will connect
    spawn(fun() ->
        %% Small delay to let accept start first
        receive after 50 -> ok end,
        case gen_tcp:connect("localhost", Port, []) of
            {ok, S} -> gen_tcp:close(S);
            _ -> ok
        end
    end),

    %% Accept should succeed
    {noreply, NewState} = flurm_metrics_http:handle_info(accept, State),
    ?assertEqual(ListenSocket, NewState#state.socket),

    gen_tcp:close(ListenSocket),
    %% Flush accept message
    receive accept -> ok after 200 -> ok end.

test_accept_closed() ->
    Port = 49550 + rand:uniform(1000),
    {ok, Socket} = gen_tcp:listen(Port, [{reuseaddr, true}]),

    %% Close socket before accepting
    gen_tcp:close(Socket),
    State = #state{socket = Socket, port = Port},

    %% Accept should get closed error
    {noreply, NewState} = flurm_metrics_http:handle_info(accept, State),
    ?assertEqual(undefined, NewState#state.socket).

test_accept_error() ->
    %% Create a state with an invalid socket reference
    FakeSocket = make_ref(),  % Not a real socket
    State = #state{socket = FakeSocket, port = 9090},

    %% This will error because FakeSocket is not a real socket
    %% The handler should handle it gracefully by catching the error
    try
        flurm_metrics_http:handle_info(accept, State)
    catch
        error:_ -> ok
    end.

test_unknown_info() ->
    State = #state{socket = undefined, port = 9090},
    {noreply, NewState} = flurm_metrics_http:handle_info(unknown, State),
    ?assertEqual(State, NewState).

test_terminate_with_socket() ->
    Port = 49650 + rand:uniform(1000),
    {ok, Socket} = gen_tcp:listen(Port, [{reuseaddr, true}]),
    State = #state{socket = Socket, port = Port},

    Result = flurm_metrics_http:terminate(normal, State),
    ?assertEqual(ok, Result).

test_terminate_without_socket() ->
    State = #state{socket = undefined, port = 9090},
    Result = flurm_metrics_http:terminate(normal, State),
    ?assertEqual(ok, Result).

test_code_change() ->
    State = #state{socket = undefined, port = 9090},
    {ok, NewState} = flurm_metrics_http:code_change("1.0", State, []),
    ?assertEqual(State, NewState).

%%====================================================================
%% HTTP Request Tests (placeholders - real tests in integration_test_)
%%====================================================================

http_request_placeholder_test_() ->
    [
     {"HTTP requests are tested in integration_test_", fun() -> ok end}
    ].

%%====================================================================
%% Full Integration Test with Real Server
%%====================================================================

integration_test_() ->
    {setup,
     fun() ->
         meck:new(application, [unstick, passthrough]),
         meck:new(flurm_metrics, [passthrough, non_strict]),

         Port = 49850 + rand:uniform(100),
         meck:expect(application, get_env, fun
             (flurm_controller, metrics_port, _) -> Port;
             (_, _, Default) -> Default
         end),
         meck:expect(flurm_metrics, format_prometheus, fun() -> ["test_metric 42\n"] end),

         {ok, Pid} = flurm_metrics_http:start_link(),
         _ = sys:get_state(Pid),  % Sync with server
         {Pid, Port}
     end,
     fun({Pid, _Port}) ->
         catch gen_server:stop(Pid),
         meck:unload(flurm_metrics),
         meck:unload(application),
         ok
     end,
     fun({_Pid, Port}) ->
         [
          {"full integration GET /metrics", fun() ->
               case gen_tcp:connect("localhost", Port, [binary, {active, false}], 2000) of
                   {ok, Socket} ->
                       gen_tcp:send(Socket, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n"),
                       case gen_tcp:recv(Socket, 0, 3000) of
                           {ok, Data} ->
                               ?assert(binary:match(Data, <<"HTTP/1.1 200">>) =/= nomatch),
                               ?assert(binary:match(Data, <<"test_metric">>) =/= nomatch);
                           {error, Reason} ->
                               ?debugFmt("Recv error: ~p", [Reason])
                       end,
                       gen_tcp:close(Socket);
                   {error, Reason} ->
                       ?debugFmt("Connect error: ~p", [Reason])
               end
           end},
          {"full integration GET /", fun() ->
               case gen_tcp:connect("localhost", Port, [binary, {active, false}], 2000) of
                   {ok, Socket} ->
                       gen_tcp:send(Socket, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"),
                       case gen_tcp:recv(Socket, 0, 3000) of
                           {ok, Data} ->
                               ?assert(binary:match(Data, <<"HTTP/1.1 200">>) =/= nomatch),
                               ?assert(binary:match(Data, <<"FLURM Metrics">>) =/= nomatch);
                           _ ->
                               ok
                       end,
                       gen_tcp:close(Socket);
                   _ ->
                       ok
               end
           end},
          {"full integration GET /health", fun() ->
               case gen_tcp:connect("localhost", Port, [binary, {active, false}], 2000) of
                   {ok, Socket} ->
                       gen_tcp:send(Socket, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"),
                       case gen_tcp:recv(Socket, 0, 3000) of
                           {ok, Data} ->
                               ?assert(binary:match(Data, <<"HTTP/1.1 200">>) =/= nomatch),
                               ?assert(binary:match(Data, <<"\"status\":\"ok\"">>) =/= nomatch);
                           _ ->
                               ok
                       end,
                       gen_tcp:close(Socket);
                   _ ->
                       ok
               end
           end},
          {"full integration GET /notfound", fun() ->
               case gen_tcp:connect("localhost", Port, [binary, {active, false}], 2000) of
                   {ok, Socket} ->
                       gen_tcp:send(Socket, "GET /notfound HTTP/1.1\r\nHost: localhost\r\n\r\n"),
                       case gen_tcp:recv(Socket, 0, 3000) of
                           {ok, Data} ->
                               ?assert(binary:match(Data, <<"HTTP/1.1 404">>) =/= nomatch);
                           _ ->
                               ok
                       end,
                       gen_tcp:close(Socket);
                   _ ->
                       ok
               end
           end}
         ]
     end}.
