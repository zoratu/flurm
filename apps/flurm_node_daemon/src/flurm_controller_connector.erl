%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Connector
%%%
%%% Maintains the connection to the controller daemon and handles
%%% registration, heartbeats, job assignments, and status reporting.
%%%
%%% Features:
%%% - Automatic reconnection with exponential backoff
%%% - Node registration with system capabilities
%%% - Periodic heartbeats with system metrics
%%% - Job launch and completion handling
%%% - Message framing for TCP stream
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_connector).

-behaviour(gen_server).

-export([start_link/0]).
-export([send_message/1, report_job_complete/3, report_job_failed/3]).
-export([get_state/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(INITIAL_RECONNECT_INTERVAL, 1000).  % 1 second
-define(MAX_RECONNECT_INTERVAL, 60000).     % 60 seconds
-define(BACKOFF_MULTIPLIER, 2).

-record(state, {
    socket :: gen_tcp:socket() | undefined,
    host :: string(),
    port :: pos_integer(),
    heartbeat_interval :: pos_integer(),
    heartbeat_timer :: reference() | undefined,
    connected :: boolean(),
    registered :: boolean(),
    node_id :: binary() | undefined,
    reconnect_interval :: pos_integer(),
    buffer :: binary(),  % Buffer for TCP message framing
    running_jobs :: #{pos_integer() => pid()}  % Job ID -> Executor PID
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

%% @doc Report job completion to controller
-spec report_job_complete(pos_integer(), integer(), binary()) -> ok | {error, term()}.
report_job_complete(JobId, ExitCode, Output) ->
    gen_server:cast(?MODULE, {job_complete, JobId, ExitCode, Output}).

%% @doc Report job failure to controller
-spec report_job_failed(pos_integer(), term(), binary()) -> ok | {error, term()}.
report_job_failed(JobId, Reason, Output) ->
    gen_server:cast(?MODULE, {job_failed, JobId, Reason, Output}).

%% @doc Get current connection state
-spec get_state() -> map().
get_state() ->
    gen_server:call(?MODULE, get_state).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("Controller Connector starting"),

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
        heartbeat_timer = undefined,
        connected = false,
        registered = false,
        node_id = undefined,
        reconnect_interval = ?INITIAL_RECONNECT_INTERVAL,
        buffer = <<>>,
        running_jobs = #{}
    }}.

handle_call({send_message, Message}, _From, #state{socket = Socket, connected = true} = State) ->
    Result = send_protocol_message(Socket, Message),
    {reply, Result, State};

handle_call({send_message, _Message}, _From, #state{connected = false} = State) ->
    {reply, {error, not_connected}, State};

handle_call(get_state, _From, State) ->
    Info = #{
        connected => State#state.connected,
        registered => State#state.registered,
        node_id => State#state.node_id,
        host => State#state.host,
        port => State#state.port,
        running_jobs => maps:size(State#state.running_jobs)
    },
    {reply, Info, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({job_complete, JobId, ExitCode, Output}, State) ->
    NewState = handle_job_completion(JobId, ExitCode, Output, completed, State),
    {noreply, NewState};

handle_cast({job_failed, JobId, Reason, Output}, State) ->
    NewState = handle_job_completion(JobId, -1, Output, {failed, Reason}, State),
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(connect, #state{host = Host, port = Port, reconnect_interval = Interval} = State) ->
    case gen_tcp:connect(Host, Port, [binary, {active, true}, {packet, raw}], 5000) of
        {ok, Socket} ->
            lager:info("Connected to controller at ~s:~p", [Host, Port]),
            %% Register with controller
            ok = register_with_controller(Socket),
            %% Start heartbeat timer
            TimerRef = erlang:send_after(State#state.heartbeat_interval, self(), heartbeat),
            {noreply, State#state{
                socket = Socket,
                connected = true,
                heartbeat_timer = TimerRef,
                reconnect_interval = ?INITIAL_RECONNECT_INTERVAL,
                buffer = <<>>
            }};
        {error, Reason} ->
            lager:warning("Failed to connect to controller: ~p, retrying in ~pms",
                         [Reason, Interval]),
            erlang:send_after(Interval, self(), connect),
            %% Exponential backoff
            NewInterval = min(Interval * ?BACKOFF_MULTIPLIER, ?MAX_RECONNECT_INTERVAL),
            {noreply, State#state{connected = false, reconnect_interval = NewInterval}}
    end;

handle_info(heartbeat, #state{socket = Socket, connected = true, heartbeat_interval = Interval} = State) ->
    Metrics = flurm_system_monitor:get_metrics(),
    RunningJobIds = maps:keys(State#state.running_jobs),

    HeartbeatMsg = #{
        type => node_heartbeat,
        payload => #{
            hostname => maps:get(hostname, Metrics),
            cpus => maps:get(cpus, Metrics),
            total_memory_mb => maps:get(total_memory_mb, Metrics),
            free_memory_mb => maps:get(free_memory_mb, Metrics),
            load_avg => maps:get(load_avg, Metrics),
            running_jobs => RunningJobIds,
            job_count => length(RunningJobIds)
        }
    },

    case send_protocol_message(Socket, HeartbeatMsg) of
        ok ->
            ok;
        {error, Reason} ->
            lager:warning("Failed to send heartbeat: ~p", [Reason])
    end,

    TimerRef = erlang:send_after(Interval, self(), heartbeat),
    {noreply, State#state{heartbeat_timer = TimerRef}};

handle_info(heartbeat, #state{connected = false} = State) ->
    %% Skip heartbeat if not connected
    {noreply, State};

handle_info({tcp, Socket, Data}, #state{socket = Socket, buffer = Buffer} = State) ->
    %% Accumulate data and try to decode messages
    NewBuffer = <<Buffer/binary, Data/binary>>,
    {Messages, RemainingBuffer} = decode_messages(NewBuffer, []),

    %% Process each complete message
    NewState = lists:foldl(fun(Msg, S) ->
        handle_controller_message(Msg, S)
    end, State#state{buffer = RemainingBuffer}, Messages),

    {noreply, NewState};

handle_info({tcp_closed, _Socket}, #state{host = Host, port = Port, heartbeat_timer = Timer} = State) ->
    lager:warning("Connection to controller ~s:~p closed, reconnecting...", [Host, Port]),
    cancel_timer(Timer),
    erlang:send_after(State#state.reconnect_interval, self(), connect),
    {noreply, State#state{
        socket = undefined,
        connected = false,
        registered = false,
        heartbeat_timer = undefined,
        buffer = <<>>
    }};

handle_info({tcp_error, _Socket, Reason}, #state{heartbeat_timer = Timer} = State) ->
    lager:error("TCP error: ~p", [Reason]),
    cancel_timer(Timer),
    erlang:send_after(State#state.reconnect_interval, self(), connect),
    {noreply, State#state{
        socket = undefined,
        connected = false,
        registered = false,
        heartbeat_timer = undefined,
        buffer = <<>>
    }};

handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
    %% Job executor process died
    case find_job_by_pid(Pid, State#state.running_jobs) of
        {ok, JobId} ->
            lager:warning("Job ~p executor died: ~p", [JobId, Reason]),
            NewJobs = maps:remove(JobId, State#state.running_jobs),
            %% Report failure to controller
            report_job_to_controller(State#state.socket, JobId, failed, -1, <<"Executor crashed">>),
            {noreply, State#state{running_jobs = NewJobs}};
        error ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{socket = Socket, heartbeat_timer = Timer}) ->
    cancel_timer(Timer),
    case Socket of
        undefined -> ok;
        _ -> gen_tcp:close(Socket)
    end,
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

send_protocol_message(Socket, Message) ->
    case flurm_protocol:encode(Message) of
        {ok, Binary} ->
            %% Add length prefix for framing
            Len = byte_size(Binary),
            gen_tcp:send(Socket, <<Len:32, Binary/binary>>);
        {error, Reason} ->
            {error, Reason}
    end.

register_with_controller(Socket) ->
    Metrics = flurm_system_monitor:get_metrics(),
    GPUs = flurm_system_monitor:get_gpus(),

    RegisterMsg = #{
        type => node_register,
        payload => #{
            hostname => maps:get(hostname, Metrics),
            cpus => maps:get(cpus, Metrics),
            memory_mb => maps:get(total_memory_mb, Metrics),
            gpus => GPUs,
            features => detect_features(),
            partitions => [<<"default">>],
            version => <<"0.1.0">>
        }
    },

    case send_protocol_message(Socket, RegisterMsg) of
        ok ->
            lager:info("Sent registration to controller"),
            ok;
        {error, Reason} ->
            lager:error("Failed to send registration: ~p", [Reason]),
            {error, Reason}
    end.

%% Decode multiple messages from buffer
decode_messages(<<Len:32, Data/binary>>, Acc) when byte_size(Data) >= Len ->
    <<MsgData:Len/binary, Rest/binary>> = Data,
    case flurm_protocol:decode(MsgData) of
        {ok, Message} ->
            decode_messages(Rest, [Message | Acc]);
        {error, Reason} ->
            lager:warning("Failed to decode message: ~p", [Reason]),
            decode_messages(Rest, Acc)
    end;
decode_messages(Buffer, Acc) ->
    {lists:reverse(Acc), Buffer}.

handle_controller_message(#{type := node_register_ack, payload := Payload}, State) ->
    NodeId = maps:get(<<"node_id">>, Payload, undefined),
    lager:info("Registration acknowledged, node_id: ~p", [NodeId]),
    State#state{registered = true, node_id = NodeId};

handle_controller_message(#{type := node_heartbeat_ack}, State) ->
    %% Heartbeat acknowledged - controller is alive
    State;

handle_controller_message(#{type := job_launch, payload := Payload}, State) ->
    JobId = maps:get(<<"job_id">>, Payload),
    TimeLimit = maps:get(<<"time_limit">>, Payload, undefined),
    lager:info("Received job launch request for job ~p (time_limit: ~p)", [JobId, TimeLimit]),

    JobSpec = #{
        job_id => JobId,
        script => maps:get(<<"script">>, Payload, <<>>),
        working_dir => maps:get(<<"working_dir">>, Payload, <<"/tmp">>),
        environment => maps:get(<<"environment">>, Payload, #{}),
        num_cpus => maps:get(<<"num_cpus">>, Payload, 1),
        memory_mb => maps:get(<<"memory_mb">>, Payload, 1024),
        time_limit => TimeLimit
    },

    case flurm_job_executor_sup:start_job(JobSpec) of
        {ok, Pid} ->
            %% Monitor the job executor
            erlang:monitor(process, Pid),
            NewJobs = maps:put(JobId, Pid, State#state.running_jobs),
            lager:info("Started job ~p, executor: ~p", [JobId, Pid]),
            State#state{running_jobs = NewJobs};
        {error, Reason} ->
            lager:error("Failed to start job ~p: ~p", [JobId, Reason]),
            report_job_to_controller(State#state.socket, JobId, failed, -1,
                                    iolist_to_binary(io_lib:format("~p", [Reason]))),
            State
    end;

handle_controller_message(#{type := job_cancel, payload := Payload}, State) ->
    JobId = maps:get(<<"job_id">>, Payload),
    lager:info("Received job cancel request for job ~p", [JobId]),

    case maps:get(JobId, State#state.running_jobs, undefined) of
        undefined ->
            lager:warning("Job ~p not found for cancellation", [JobId]),
            State;
        Pid ->
            flurm_job_executor:cancel(Pid),
            NewJobs = maps:remove(JobId, State#state.running_jobs),
            State#state{running_jobs = NewJobs}
    end;

handle_controller_message(#{type := node_drain}, State) ->
    lager:info("Received drain request - stopping new job acceptance"),
    %% TODO: Mark node as draining, don't accept new jobs
    State;

handle_controller_message(#{type := node_resume}, State) ->
    lager:info("Received resume request"),
    %% TODO: Resume accepting new jobs
    State;

handle_controller_message(#{type := ack, payload := Payload}, State) ->
    lager:debug("Received ack from controller: ~p", [Payload]),
    State;

handle_controller_message(#{type := error, payload := Payload}, State) ->
    lager:warning("Received error from controller: ~p", [Payload]),
    State;

handle_controller_message(#{type := Type, payload := Payload}, State) ->
    lager:info("Received unknown message from controller: ~p ~p", [Type, Payload]),
    State.

handle_job_completion(JobId, ExitCode, Output, Status, #state{running_jobs = Jobs} = State) ->
    case maps:is_key(JobId, Jobs) of
        true ->
            report_job_to_controller(State#state.socket, JobId, Status, ExitCode, Output),
            State#state{running_jobs = maps:remove(JobId, Jobs)};
        false ->
            lager:warning("Job ~p not found in running jobs", [JobId]),
            State
    end.

report_job_to_controller(undefined, _JobId, _Status, _ExitCode, _Output) ->
    {error, not_connected};
report_job_to_controller(Socket, JobId, Status, ExitCode, Output) ->
    {MsgType, Reason} = case Status of
        completed -> {job_complete, <<"completed">>};
        {failed, timeout} -> {job_failed, <<"timeout">>};
        {failed, cancelled} -> {job_failed, <<"cancelled">>};
        {failed, R} when is_atom(R) -> {job_failed, atom_to_binary(R, utf8)};
        {failed, R} when is_binary(R) -> {job_failed, R};
        {failed, R} -> {job_failed, iolist_to_binary(io_lib:format("~p", [R]))};
        _ -> {job_failed, <<"unknown">>}
    end,

    Msg = #{
        type => MsgType,
        payload => #{
            job_id => JobId,
            exit_code => ExitCode,
            output => Output,
            reason => Reason,
            timestamp => erlang:system_time(millisecond)
        }
    },

    send_protocol_message(Socket, Msg).

find_job_by_pid(Pid, Jobs) ->
    case [JobId || {JobId, P} <- maps:to_list(Jobs), P =:= Pid] of
        [JobId] -> {ok, JobId};
        [] -> error
    end.

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

%% Detect hardware features available on this node
detect_features() ->
    Features = [],
    %% Check for GPU support
    Features1 = case filelib:is_file("/dev/nvidia0") of
        true -> [<<"gpu">> | Features];
        false -> Features
    end,
    %% Check for InfiniBand
    Features2 = case filelib:is_dir("/sys/class/infiniband") of
        true -> [<<"ib">> | Features1];
        false -> Features1
    end,
    %% Check for AVX support
    Features3 = case check_cpu_flag("avx") of
        true -> [<<"avx">> | Features2];
        false -> Features2
    end,
    Features3.

check_cpu_flag(Flag) ->
    case file:read_file("/proc/cpuinfo") of
        {ok, Content} ->
            binary:match(Content, list_to_binary(Flag)) =/= nomatch;
        _ ->
            false
    end.
