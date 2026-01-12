%%%-------------------------------------------------------------------
%%% @doc FLURM Metrics HTTP Server
%%%
%%% Provides an HTTP endpoint for Prometheus to scrape metrics.
%%% Listens on port 9090 by default and exposes /metrics endpoint.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_metrics_http).
-behaviour(gen_server).

%% API
-export([start_link/0, stop/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(DEFAULT_PORT, 9090).

-record(state, {
    socket :: gen_tcp:socket() | undefined,
    port :: pos_integer()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    Port = application:get_env(flurm_controller, metrics_port, ?DEFAULT_PORT),
    case gen_tcp:listen(Port, [
        binary,
        {packet, http_bin},
        {active, false},
        {reuseaddr, true},
        {backlog, 100}
    ]) of
        {ok, Socket} ->
            lager:info("[metrics_http] Metrics HTTP server listening on port ~p", [Port]),
            %% Start accepting connections
            self() ! accept,
            {ok, #state{socket = Socket, port = Port}};
        {error, eaddrinuse} ->
            lager:warning("[metrics_http] Port ~p in use, metrics endpoint disabled", [Port]),
            {ok, #state{socket = undefined, port = Port}};
        {error, Reason} ->
            lager:error("[metrics_http] Failed to start: ~p", [Reason]),
            {ok, #state{socket = undefined, port = Port}}
    end.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(accept, #state{socket = undefined} = State) ->
    %% No socket, don't try to accept
    {noreply, State};
handle_info(accept, #state{socket = ListenSocket} = State) ->
    %% Accept connections with a timeout to allow for clean shutdown
    case gen_tcp:accept(ListenSocket, 1000) of
        {ok, ClientSocket} ->
            spawn(fun() -> handle_request(ClientSocket) end),
            self() ! accept,
            {noreply, State};
        {error, timeout} ->
            self() ! accept,
            {noreply, State};
        {error, closed} ->
            lager:info("[metrics_http] Listen socket closed"),
            {noreply, State#state{socket = undefined}};
        {error, Reason} ->
            lager:error("[metrics_http] Accept error: ~p", [Reason]),
            self() ! accept,
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{socket = Socket}) ->
    case Socket of
        undefined -> ok;
        _ -> gen_tcp:close(Socket)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

handle_request(Socket) ->
    inet:setopts(Socket, [{active, false}]),
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, {http_request, 'GET', {abs_path, <<"/metrics">>}, _}} ->
            %% Read headers until we get http_eoh
            drain_headers(Socket),
            send_metrics_response(Socket);
        {ok, {http_request, 'GET', {abs_path, <<"/">>}, _}} ->
            drain_headers(Socket),
            send_index_response(Socket);
        {ok, {http_request, 'GET', {abs_path, <<"/health">>}, _}} ->
            drain_headers(Socket),
            send_health_response(Socket);
        {ok, _Other} ->
            drain_headers(Socket),
            send_404_response(Socket);
        {error, _} ->
            ok
    end,
    gen_tcp:close(Socket).

drain_headers(Socket) ->
    case gen_tcp:recv(Socket, 0, 1000) of
        {ok, http_eoh} ->
            ok;
        {ok, {http_header, _, _, _, _}} ->
            drain_headers(Socket);
        _ ->
            ok
    end.

send_metrics_response(Socket) ->
    Metrics = case catch flurm_metrics:format_prometheus() of
        Data when is_list(Data) -> iolist_to_binary(Data);
        _ -> <<>>
    end,
    Response = [
        <<"HTTP/1.1 200 OK\r\n">>,
        <<"Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n">>,
        <<"Content-Length: ">>, integer_to_binary(byte_size(Metrics)), <<"\r\n">>,
        <<"\r\n">>,
        Metrics
    ],
    inet:setopts(Socket, [{packet, raw}]),
    gen_tcp:send(Socket, Response).

send_index_response(Socket) ->
    Body = <<"<html><head><title>FLURM Metrics</title></head><body>",
             "<h1>FLURM Metrics</h1>",
             "<p><a href=\"/metrics\">Metrics</a></p>",
             "<p><a href=\"/health\">Health</a></p>",
             "</body></html>">>,
    Response = [
        <<"HTTP/1.1 200 OK\r\n">>,
        <<"Content-Type: text/html\r\n">>,
        <<"Content-Length: ">>, integer_to_binary(byte_size(Body)), <<"\r\n">>,
        <<"\r\n">>,
        Body
    ],
    inet:setopts(Socket, [{packet, raw}]),
    gen_tcp:send(Socket, Response).

send_health_response(Socket) ->
    Body = <<"{\"status\":\"ok\"}">>,
    Response = [
        <<"HTTP/1.1 200 OK\r\n">>,
        <<"Content-Type: application/json\r\n">>,
        <<"Content-Length: ">>, integer_to_binary(byte_size(Body)), <<"\r\n">>,
        <<"\r\n">>,
        Body
    ],
    inet:setopts(Socket, [{packet, raw}]),
    gen_tcp:send(Socket, Response).

send_404_response(Socket) ->
    Body = <<"Not Found">>,
    Response = [
        <<"HTTP/1.1 404 Not Found\r\n">>,
        <<"Content-Type: text/plain\r\n">>,
        <<"Content-Length: ">>, integer_to_binary(byte_size(Body)), <<"\r\n">>,
        <<"\r\n">>,
        Body
    ],
    inet:setopts(Socket, [{packet, raw}]),
    gen_tcp:send(Socket, Response).
