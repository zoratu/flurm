%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_metrics_http module
%%% Tests for HTTP endpoint for Prometheus metrics using pure functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_metrics_http_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Note: These tests focus on the pure helper functions exported
%% under -ifdef(TEST). Socket handling is tested separately.

%%====================================================================
%% method_to_binary Tests
%%====================================================================

method_to_binary_get_test() ->
    ?assertEqual(<<"GET">>, flurm_metrics_http:method_to_binary('GET')).

method_to_binary_post_test() ->
    ?assertEqual(<<"POST">>, flurm_metrics_http:method_to_binary('POST')).

method_to_binary_put_test() ->
    ?assertEqual(<<"PUT">>, flurm_metrics_http:method_to_binary('PUT')).

method_to_binary_delete_test() ->
    ?assertEqual(<<"DELETE">>, flurm_metrics_http:method_to_binary('DELETE')).

method_to_binary_other_test() ->
    Result = flurm_metrics_http:method_to_binary('PATCH'),
    ?assert(is_binary(Result)),
    ?assertEqual(<<"PATCH">>, Result).

method_to_binary_options_test() ->
    Result = flurm_metrics_http:method_to_binary('OPTIONS'),
    ?assertEqual(<<"OPTIONS">>, Result).

method_to_binary_head_test() ->
    Result = flurm_metrics_http:method_to_binary('HEAD'),
    ?assertEqual(<<"HEAD">>, Result).

%%====================================================================
%% drain_headers Tests (conceptual - requires socket)
%%====================================================================

%% Note: drain_headers requires a real socket with HTTP data.
%% We test the return type expectations here.

%%====================================================================
%% drain_headers_with_length Tests (conceptual)
%%====================================================================

%% Note: These require socket connections. The function returns
%% {ContentLength, Headers} tuple.

%%====================================================================
%% read_body Tests
%%====================================================================

read_body_zero_length_test() ->
    %% read_body with length 0 returns empty binary without reading socket
    Result = flurm_metrics_http:read_body(fake_socket, 0),
    ?assertEqual(<<>>, Result).

%%====================================================================
%% Response Building Tests
%%====================================================================

%% These functions require sockets but we can test their behavior
%% by verifying the response format.

%%====================================================================
%% Integration-style Tests (with real socket)
%%====================================================================

metrics_http_server_test_() ->
    {setup,
     fun setup_server/0,
     fun cleanup_server/1,
     fun(Port) ->
         [
             {"GET /metrics returns 200", fun() -> test_get_metrics(Port) end},
             {"GET / returns 200", fun() -> test_get_index(Port) end},
             {"GET /health returns 200", fun() -> test_get_health(Port) end},
             {"GET /unknown returns 404", fun() -> test_get_404(Port) end}
         ]
     end}.

setup_server() ->
    %% Start flurm_metrics if not running
    case whereis(flurm_metrics) of
        undefined ->
            case flurm_metrics:start_link() of
                {ok, MetricsPid} -> unlink(MetricsPid);
                {error, {already_started, MetricsPid}} -> unlink(MetricsPid)
            end;
        _ -> ok
    end,

    %% Use a random high port
    Port = 19100 + rand:uniform(800),
    application:set_env(flurm_controller, metrics_port, Port),

    %% Stop any existing server
    case whereis(flurm_metrics_http) of
        undefined -> ok;
        HttpP ->
            catch unlink(HttpP),
            catch gen_server:stop(HttpP, shutdown, 2000)
    end,

    timer:sleep(100),

    {ok, HttpPid} = flurm_metrics_http:start_link(),
    unlink(HttpPid),

    %% Wait for server to start
    timer:sleep(100),
    Port.

cleanup_server(_Port) ->
    case whereis(flurm_metrics_http) of
        undefined -> ok;
        Pid ->
            catch gen_server:stop(Pid, shutdown, 2000)
    end.

test_get_metrics(Port) ->
    {ok, Socket} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Socket, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n"),
    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),
    gen_tcp:close(Socket),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response).

test_get_index(Port) ->
    {ok, Socket} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Socket, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"),
    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),
    gen_tcp:close(Socket),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response).

test_get_health(Port) ->
    {ok, Socket} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Socket, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"),
    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),
    gen_tcp:close(Socket),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Response).

test_get_404(Port) ->
    {ok, Socket} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Socket, "GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n"),
    {ok, Response} = gen_tcp:recv(Socket, 0, 5000),
    gen_tcp:close(Socket),
    ?assertMatch(<<"HTTP/1.1 404", _/binary>>, Response).

%%====================================================================
%% gen_server Callback Tests
%%====================================================================

code_change_test() ->
    State = {state, undefined, 9090},
    {ok, NewState} = flurm_metrics_http:code_change(old_vsn, State, extra),
    ?assertEqual(State, NewState).

%%====================================================================
%% Edge Cases
%%====================================================================

%% Test multiple rapid connections
rapid_connections_test_() ->
    {setup,
     fun setup_server/0,
     fun cleanup_server/1,
     fun(Port) ->
         [{"rapid connections", fun() ->
             Pids = [spawn_link(fun() ->
                 {ok, S} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
                 gen_tcp:send(S, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"),
                 {ok, _} = gen_tcp:recv(S, 0, 5000),
                 gen_tcp:close(S)
             end) || _ <- lists:seq(1, 5)],
             %% Wait for all connections to complete
             timer:sleep(500),
             %% Check all processes completed
             lists:foreach(fun(Pid) ->
                 ?assertEqual(false, is_process_alive(Pid))
             end, Pids)
         end}]
     end}.
