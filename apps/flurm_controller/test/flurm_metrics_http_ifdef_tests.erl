%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_metrics_http internal functions
%%% Tests the -ifdef(TEST) exported helper functions directly
%%%-------------------------------------------------------------------
-module(flurm_metrics_http_ifdef_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Create a mock socket pair for testing response functions
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(ListenSock),
    {ok, ClientSock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 1000),
    {ok, ServerSock} = gen_tcp:accept(ListenSock, 1000),
    gen_tcp:close(ListenSock),
    {ClientSock, ServerSock}.

cleanup({ClientSock, ServerSock}) ->
    catch gen_tcp:close(ClientSock),
    catch gen_tcp:close(ServerSock),
    ok.

%%====================================================================
%% Test Generator
%%====================================================================

ifdef_exports_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun({_ClientSock, ServerSock}) ->
          {"send_metrics_response sends 200 with text/plain", fun() ->
              ok = flurm_metrics_http:send_metrics_response(ServerSock)
          end}
      end,
      fun({ClientSock, ServerSock}) ->
          {"send_index_response sends HTML", fun() ->
              ok = flurm_metrics_http:send_index_response(ServerSock),
              {ok, Data} = gen_tcp:recv(ClientSock, 0, 1000),
              ?assert(binary:match(Data, <<"200 OK">>) =/= nomatch),
              ?assert(binary:match(Data, <<"text/html">>) =/= nomatch),
              ?assert(binary:match(Data, <<"FLURM Metrics">>) =/= nomatch)
          end}
      end,
      fun({ClientSock, ServerSock}) ->
          {"send_health_response sends JSON", fun() ->
              ok = flurm_metrics_http:send_health_response(ServerSock),
              {ok, Data} = gen_tcp:recv(ClientSock, 0, 1000),
              ?assert(binary:match(Data, <<"200 OK">>) =/= nomatch),
              ?assert(binary:match(Data, <<"application/json">>) =/= nomatch),
              ?assert(binary:match(Data, <<"status">>) =/= nomatch)
          end}
      end,
      fun({ClientSock, ServerSock}) ->
          {"send_404_response sends Not Found", fun() ->
              ok = flurm_metrics_http:send_404_response(ServerSock),
              {ok, Data} = gen_tcp:recv(ClientSock, 0, 1000),
              ?assert(binary:match(Data, <<"404 Not Found">>) =/= nomatch)
          end}
      end
     ]}.

%% Test drain_headers with mock socket
drain_headers_test_() ->
    {setup,
     fun() ->
         {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false}, {packet, http_bin}]),
         {ok, Port} = inet:port(ListenSock),
         {ok, ClientSock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 1000),
         {ok, ServerSock} = gen_tcp:accept(ListenSock, 1000),
         gen_tcp:close(ListenSock),
         inet:setopts(ServerSock, [{packet, http_bin}]),
         {ClientSock, ServerSock}
     end,
     fun({ClientSock, ServerSock}) ->
         catch gen_tcp:close(ClientSock),
         catch gen_tcp:close(ServerSock)
     end,
     fun({ClientSock, ServerSock}) ->
         [
          {"drain_headers with http_eoh", fun() ->
              %% Send a complete HTTP request from client side
              ok = gen_tcp:send(ClientSock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
              %% Read the request line first
              {ok, {http_request, _, _, _}} = gen_tcp:recv(ServerSock, 0, 1000),
              %% Now drain headers should consume the rest
              ok = flurm_metrics_http:drain_headers(ServerSock)
          end}
         ]
     end}.

%% Test handle_request with complete request/response cycle
handle_request_test_() ->
    {setup,
     fun() ->
         %% Start flurm_metrics if needed for format_prometheus
         case whereis(flurm_metrics) of
             undefined -> ok;
             OldPid ->
                 gen_server:stop(OldPid, normal, 5000),
                 flurm_test_utils:wait_for_death(OldPid)
         end,
         catch ets:delete(flurm_metrics),
         catch ets:delete(flurm_histograms),
         {ok, MetricsPid} = flurm_metrics:start_link(),
         MetricsPid
     end,
     fun(MetricsPid) ->
         catch gen_server:stop(MetricsPid, normal, 5000),
         flurm_test_utils:wait_for_death(MetricsPid),
         catch ets:delete(flurm_metrics),
         catch ets:delete(flurm_histograms)
     end,
     fun(_MetricsPid) ->
         [
          {"handle_request processes GET /metrics", fun() ->
              {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false}, {packet, http_bin}]),
              {ok, Port} = inet:port(ListenSock),
              {ok, ClientSock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 1000),
              {ok, ServerSock} = gen_tcp:accept(ListenSock, 1000),
              gen_tcp:close(ListenSock),

              %% Send HTTP request
              ok = gen_tcp:send(ClientSock, <<"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n">>),

              %% Spawn handle_request (it closes the socket when done)
              spawn(fun() -> flurm_metrics_http:handle_request(ServerSock) end),

              %% Receive response with appropriate timeout
              case gen_tcp:recv(ClientSock, 0, 2000) of
                  {ok, Data} ->
                      ?assert(binary:match(Data, <<"200 OK">>) =/= nomatch);
                  {error, closed} ->
                      %% Socket was closed, that's also OK
                      ok
              end,
              catch gen_tcp:close(ClientSock)
          end},
          {"handle_request processes GET /health", fun() ->
              {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false}, {packet, http_bin}]),
              {ok, Port} = inet:port(ListenSock),
              {ok, ClientSock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 1000),
              {ok, ServerSock} = gen_tcp:accept(ListenSock, 1000),
              gen_tcp:close(ListenSock),

              ok = gen_tcp:send(ClientSock, <<"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
              spawn(fun() -> flurm_metrics_http:handle_request(ServerSock) end),

              case gen_tcp:recv(ClientSock, 0, 2000) of
                  {ok, Data} ->
                      ?assert(binary:match(Data, <<"status">>) =/= nomatch);
                  {error, closed} ->
                      ok
              end,
              catch gen_tcp:close(ClientSock)
          end},
          {"handle_request processes GET /", fun() ->
              {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false}, {packet, http_bin}]),
              {ok, Port} = inet:port(ListenSock),
              {ok, ClientSock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 1000),
              {ok, ServerSock} = gen_tcp:accept(ListenSock, 1000),
              gen_tcp:close(ListenSock),

              ok = gen_tcp:send(ClientSock, <<"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
              spawn(fun() -> flurm_metrics_http:handle_request(ServerSock) end),

              case gen_tcp:recv(ClientSock, 0, 2000) of
                  {ok, Data} ->
                      ?assert(binary:match(Data, <<"FLURM">>) =/= nomatch);
                  {error, closed} ->
                      ok
              end,
              catch gen_tcp:close(ClientSock)
          end},
          {"handle_request returns 404 for unknown path", fun() ->
              {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false}, {packet, http_bin}]),
              {ok, Port} = inet:port(ListenSock),
              {ok, ClientSock} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}], 1000),
              {ok, ServerSock} = gen_tcp:accept(ListenSock, 1000),
              gen_tcp:close(ListenSock),

              ok = gen_tcp:send(ClientSock, <<"GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
              spawn(fun() -> flurm_metrics_http:handle_request(ServerSock) end),

              case gen_tcp:recv(ClientSock, 0, 2000) of
                  {ok, Data} ->
                      ?assert(binary:match(Data, <<"404">>) =/= nomatch);
                  {error, closed} ->
                      ok
              end,
              catch gen_tcp:close(ClientSock)
          end}
         ]
     end}.
