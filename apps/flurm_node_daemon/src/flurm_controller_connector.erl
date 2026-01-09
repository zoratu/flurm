%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Connector
%%%
%%% Maintains the connection to the controller daemon and handles
%%% registration, heartbeats, and job assignments.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_connector).

-behaviour(gen_server).

-export([start_link/0]).
-export([send_message/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(RECONNECT_INTERVAL, 5000). % 5 seconds

-record(state, {
    socket :: gen_tcp:socket() | undefined,
    host :: string(),
    port :: pos_integer(),
    heartbeat_interval :: pos_integer(),
    connected :: boolean()
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Send a message to the controller
-spec send_message(map()) -> ok | {error, term()}.
send_message(Message) ->
    gen_server:call(?MODULE, {send_message, Message}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("Controller Connector started"),

    {ok, Host} = application:get_env(flurm_node_daemon, controller_host),
    {ok, Port} = application:get_env(flurm_node_daemon, controller_port),
    {ok, HeartbeatInterval} = application:get_env(flurm_node_daemon, heartbeat_interval),

    %% Try to connect immediately
    self() ! connect,

    {ok, #state{
        socket = undefined,
        host = Host,
        port = Port,
        heartbeat_interval = HeartbeatInterval,
        connected = false
    }}.

handle_call({send_message, Message}, _From, #state{socket = Socket, connected = true} = State) ->
    case flurm_protocol:encode(Message) of
        {ok, Binary} ->
            case gen_tcp:send(Socket, Binary) of
                ok ->
                    {reply, ok, State};
                {error, Reason} ->
                    lager:error("Failed to send message: ~p", [Reason]),
                    {reply, {error, Reason}, State#state{connected = false}}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({send_message, _Message}, _From, #state{connected = false} = State) ->
    {reply, {error, not_connected}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(connect, #state{host = Host, port = Port} = State) ->
    case gen_tcp:connect(Host, Port, [binary, {active, true}, {packet, raw}], 5000) of
        {ok, Socket} ->
            lager:info("Connected to controller at ~s:~p", [Host, Port]),
            %% Register with controller
            register_with_controller(Socket),
            %% Start heartbeat timer
            erlang:send_after(State#state.heartbeat_interval, self(), heartbeat),
            {noreply, State#state{socket = Socket, connected = true}};
        {error, Reason} ->
            lager:warning("Failed to connect to controller: ~p, retrying in ~pms",
                         [Reason, ?RECONNECT_INTERVAL]),
            erlang:send_after(?RECONNECT_INTERVAL, self(), connect),
            {noreply, State#state{connected = false}}
    end;

handle_info(heartbeat, #state{socket = Socket, connected = true, heartbeat_interval = Interval} = State) ->
    Metrics = flurm_system_monitor:get_metrics(),
    RunningJobs = [], % TODO: Get from job executor
    HeartbeatMsg = #{
        type => node_heartbeat,
        payload => #{
            hostname => maps:get(hostname, Metrics),
            load_avg => maps:get(load_avg, Metrics),
            free_memory_mb => maps:get(free_memory_mb, Metrics),
            running_jobs => RunningJobs
        }
    },
    case flurm_protocol:encode(HeartbeatMsg) of
        {ok, Binary} ->
            gen_tcp:send(Socket, Binary);
        _ ->
            ok
    end,
    erlang:send_after(Interval, self(), heartbeat),
    {noreply, State};

handle_info(heartbeat, #state{connected = false} = State) ->
    %% Skip heartbeat if not connected
    {noreply, State};

handle_info({tcp, _Socket, Data}, State) ->
    case flurm_protocol:decode(Data) of
        {ok, Message} ->
            handle_controller_message(Message);
        {error, Reason} ->
            lager:warning("Failed to decode message from controller: ~p", [Reason])
    end,
    {noreply, State};

handle_info({tcp_closed, _Socket}, #state{host = Host, port = Port} = State) ->
    lager:warning("Connection to controller ~s:~p closed, reconnecting...", [Host, Port]),
    erlang:send_after(?RECONNECT_INTERVAL, self(), connect),
    {noreply, State#state{socket = undefined, connected = false}};

handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:error("TCP error: ~p", [Reason]),
    erlang:send_after(?RECONNECT_INTERVAL, self(), connect),
    {noreply, State#state{socket = undefined, connected = false}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{socket = Socket}) when Socket =/= undefined ->
    gen_tcp:close(Socket),
    ok;
terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

register_with_controller(Socket) ->
    Metrics = flurm_system_monitor:get_metrics(),
    RegisterMsg = #{
        type => node_register,
        payload => #{
            hostname => maps:get(hostname, Metrics),
            cpus => maps:get(cpus, Metrics),
            memory_mb => maps:get(total_memory_mb, Metrics),
            features => [],
            partitions => [<<"default">>]
        }
    },
    case flurm_protocol:encode(RegisterMsg) of
        {ok, Binary} ->
            gen_tcp:send(Socket, Binary),
            lager:info("Sent registration to controller");
        {error, Reason} ->
            lager:error("Failed to encode registration: ~p", [Reason])
    end.

handle_controller_message(#{type := ack, payload := Payload}) ->
    lager:debug("Received ack from controller: ~p", [Payload]);

handle_controller_message(#{type := error, payload := Payload}) ->
    lager:warning("Received error from controller: ~p", [Payload]);

handle_controller_message(#{type := Type, payload := Payload}) ->
    lager:info("Received message from controller: ~p ~p", [Type, Payload]).
