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
-export([send_message/1, report_job_complete/3, report_job_complete/4,
         report_job_failed/3, report_job_failed/4]).
-export([get_state/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(TEST).
-export([decode_messages/2, find_job_by_pid/2, cancel_timer/1,
         detect_features/0, check_cpu_flag/1]).
-endif.

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
    running_jobs :: #{pos_integer() => pid()},  % Job ID -> Executor PID
    draining :: boolean(),  % Whether node is draining (not accepting new jobs)
    drain_reason :: binary() | undefined  % Reason for draining
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
    report_job_complete(JobId, ExitCode, Output, 0).

%% @doc Report job completion to controller with energy data
-spec report_job_complete(pos_integer(), integer(), binary(), non_neg_integer()) -> ok | {error, term()}.
report_job_complete(JobId, ExitCode, Output, EnergyUsed) ->
    gen_server:cast(?MODULE, {job_complete, JobId, ExitCode, Output, EnergyUsed}).

%% @doc Report job failure to controller
-spec report_job_failed(pos_integer(), term(), binary()) -> ok | {error, term()}.
report_job_failed(JobId, Reason, Output) ->
    report_job_failed(JobId, Reason, Output, 0).

%% @doc Report job failure to controller with energy data
-spec report_job_failed(pos_integer(), term(), binary(), non_neg_integer()) -> ok | {error, term()}.
report_job_failed(JobId, Reason, Output, EnergyUsed) ->
    gen_server:cast(?MODULE, {job_failed, JobId, Reason, Output, EnergyUsed}).

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
        running_jobs = #{},
        draining = false,
        drain_reason = undefined
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
        running_jobs => maps:size(State#state.running_jobs),
        draining => State#state.draining,
        drain_reason => State#state.drain_reason
    },
    {reply, Info, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({job_complete, JobId, ExitCode, Output, EnergyUsed}, State) ->
    NewState = handle_job_completion(JobId, ExitCode, Output, completed, EnergyUsed, State),
    {noreply, NewState};

handle_cast({job_failed, JobId, Reason, Output, EnergyUsed}, State) ->
    NewState = handle_job_completion(JobId, -1, Output, {failed, Reason}, EnergyUsed, State),
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
    GPUInfo = flurm_system_monitor:get_gpus(),
    GPUAllocation = flurm_system_monitor:get_gpu_allocation(),

    %% Get current power consumption from job executor (if available)
    PowerWatts = try
        flurm_job_executor:get_current_power()
    catch
        _:_ -> 0.0
    end,

    HeartbeatMsg = #{
        type => node_heartbeat,
        payload => #{
            hostname => maps:get(hostname, Metrics),
            cpus => maps:get(cpus, Metrics),
            total_memory_mb => maps:get(total_memory_mb, Metrics),
            free_memory_mb => maps:get(free_memory_mb, Metrics),
            available_memory_mb => maps:get(available_memory_mb, Metrics, 0),
            load_avg => maps:get(load_avg, Metrics),
            load_avg_5 => maps:get(load_avg_5, Metrics, 0.0),
            load_avg_15 => maps:get(load_avg_15, Metrics, 0.0),
            running_jobs => RunningJobIds,
            job_count => length(RunningJobIds),
            %% GPU tracking
            gpus => GPUInfo,
            gpu_count => length(GPUInfo),
            gpu_allocation => GPUAllocation,
            %% Energy/power monitoring
            power_watts => PowerWatts,
            %% Node status
            draining => State#state.draining,
            drain_reason => State#state.drain_reason,
            timestamp => erlang:system_time(millisecond)
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
            report_job_to_controller(State#state.socket, JobId, failed, -1, <<"Executor crashed">>, 0),
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

    %% Read partitions from environment variable or config
    Partitions = get_node_partitions(),

    RegisterMsg = #{
        type => node_register,
        payload => #{
            hostname => maps:get(hostname, Metrics),
            cpus => maps:get(cpus, Metrics),
            memory_mb => maps:get(total_memory_mb, Metrics),
            gpus => GPUs,
            features => detect_features(),
            partitions => Partitions,
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
    StdOut = maps:get(<<"std_out">>, Payload, undefined),
    StdErr = maps:get(<<"std_err">>, Payload, undefined),
    lager:info("Received job launch request for job ~p (time_limit: ~p, std_out: ~p)",
               [JobId, TimeLimit, StdOut]),

    %% Check if node is draining - reject new jobs if so
    case State#state.draining of
        true ->
            lager:warning("Rejecting job ~p - node is draining: ~s",
                         [JobId, State#state.drain_reason]),
            report_job_to_controller(State#state.socket, JobId, failed, -1,
                                    <<"Node is draining, cannot accept new jobs">>, 0),
            State;
        false ->
            JobSpec = #{
                job_id => JobId,
                script => maps:get(<<"script">>, Payload, <<>>),
                working_dir => maps:get(<<"working_dir">>, Payload, <<"/tmp">>),
                environment => maps:get(<<"environment">>, Payload, #{}),
                num_cpus => maps:get(<<"num_cpus">>, Payload, 1),
                memory_mb => maps:get(<<"memory_mb">>, Payload, 1024),
                time_limit => TimeLimit,
                std_out => StdOut,
                std_err => StdErr
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
                                            iolist_to_binary(io_lib:format("~p", [Reason])), 0),
                    State
            end
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

handle_controller_message(#{type := step_launch, payload := Payload}, State) ->
    JobId = maps:get(<<"job_id">>, Payload),
    StepId = maps:get(<<"step_id">>, Payload),
    Command = maps:get(<<"command">>, Payload, <<"hostname">>),
    lager:info("Received step launch request for job ~p step ~p: ~s", [JobId, StepId, Command]),

    %% Execute the step command synchronously (for srun)
    %% The output needs to be sent back to controller -> srun callback
    WorkDir = binary_to_list(maps:get(<<"working_dir">>, Payload, <<"/tmp">>)),
    Env = maps:get(<<"environment">>, Payload, #{}),

    %% Build environment list for port
    EnvList = [{binary_to_list(K), binary_to_list(V)} || {K, V} <- maps:to_list(Env)],

    %% Execute the command
    try
        {Output, ExitCode} = execute_step_command(Command, WorkDir, EnvList),
        lager:info("Step ~p.~p completed with exit code ~p, output: ~p bytes",
                   [JobId, StepId, ExitCode, byte_size(Output)]),
        %% Report output back to controller
        report_step_complete(State#state.socket, JobId, StepId, ExitCode, Output)
    catch
        Type:Error ->
            lager:error("Step ~p.~p execution failed: ~p:~p", [JobId, StepId, Type, Error]),
            ErrorMsg = iolist_to_binary(io_lib:format("~p:~p", [Type, Error])),
            report_step_complete(State#state.socket, JobId, StepId, 1, ErrorMsg)
    end,
    State;

handle_controller_message(#{type := node_drain} = Msg, State) ->
    Payload = maps:get(payload, Msg, #{}),
    Reason = maps:get(<<"reason">>, Payload, <<"controller request">>),
    lager:info("Received drain request - stopping new job acceptance, reason: ~s", [Reason]),
    %% Mark node as draining - existing jobs continue, new jobs rejected
    NewState = State#state{
        draining = true,
        drain_reason = Reason
    },
    %% Send acknowledgement to controller
    send_drain_ack(State#state.socket, true, Reason, maps:size(State#state.running_jobs)),
    NewState;

handle_controller_message(#{type := node_resume}, State) ->
    lager:info("Received resume request - accepting new jobs again"),
    %% Resume accepting new jobs
    NewState = State#state{
        draining = false,
        drain_reason = undefined
    },
    %% Send acknowledgement to controller
    send_drain_ack(State#state.socket, false, undefined, maps:size(State#state.running_jobs)),
    NewState;

handle_controller_message(#{type := ack, payload := Payload}, State) ->
    lager:debug("Received ack from controller: ~p", [Payload]),
    State;

handle_controller_message(#{type := error, payload := Payload}, State) ->
    lager:warning("Received error from controller: ~p", [Payload]),
    State;

handle_controller_message(#{type := Type, payload := Payload}, State) ->
    lager:info("Received unknown message from controller: ~p ~p", [Type, Payload]),
    State.

handle_job_completion(JobId, ExitCode, Output, Status, EnergyUsed, #state{running_jobs = Jobs} = State) ->
    case maps:is_key(JobId, Jobs) of
        true ->
            report_job_to_controller(State#state.socket, JobId, Status, ExitCode, Output, EnergyUsed),
            State#state{running_jobs = maps:remove(JobId, Jobs)};
        false ->
            lager:warning("Job ~p not found in running jobs", [JobId]),
            State
    end.

%% Report job status to controller with energy tracking
report_job_to_controller(undefined, _JobId, _Status, _ExitCode, _Output, _EnergyUsed) ->
    {error, not_connected};
report_job_to_controller(Socket, JobId, Status, ExitCode, Output, EnergyUsed) ->
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
            energy_used_uj => EnergyUsed,  % Energy in microjoules
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

%% Send drain acknowledgement to controller
send_drain_ack(undefined, _Draining, _Reason, _RunningJobCount) ->
    {error, not_connected};
send_drain_ack(Socket, Draining, Reason, RunningJobCount) ->
    Msg = #{
        type => drain_ack,
        payload => #{
            draining => Draining,
            reason => Reason,
            running_jobs => RunningJobCount,
            timestamp => erlang:system_time(millisecond)
        }
    },
    send_protocol_message(Socket, Msg).

%% Get partitions this node should join from environment or config
%% Reads FLURM_NODE_PARTITIONS env var (comma-separated) or app env
get_node_partitions() ->
    case os:getenv("FLURM_NODE_PARTITIONS") of
        false ->
            %% Try application environment
            case application:get_env(flurm_node_daemon, partitions) of
                {ok, Partitions} when is_list(Partitions) ->
                    [ensure_binary(P) || P <- Partitions];
                _ ->
                    [<<"default">>]
            end;
        PartitionsStr ->
            %% Parse comma-separated partition names
            Parts = string:tokens(PartitionsStr, ","),
            case Parts of
                [] -> [<<"default">>];
                _ -> [list_to_binary(string:trim(P)) || P <- Parts]
            end
    end.

%% Ensure a value is a binary
ensure_binary(V) when is_binary(V) -> V;
ensure_binary(V) when is_list(V) -> list_to_binary(V);
ensure_binary(V) when is_atom(V) -> atom_to_binary(V, utf8).

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

%%====================================================================
%% Step Execution Helpers
%%====================================================================

%% @doc Execute a step command and return output and exit code.
-spec execute_step_command(binary(), string(), [{string(), string()}]) -> {binary(), integer()}.
execute_step_command(Command, WorkDir, Env) ->
    %% Parse the command - it might be a single binary or have args
    CmdStr = binary_to_list(Command),
    lager:debug("Executing step command: ~s in ~s", [CmdStr, WorkDir]),

    %% Use os:cmd for simple command execution
    %% For more complex cases, we'd use open_port
    FullEnv = [{"HOME", "/tmp"}, {"PATH", "/usr/local/bin:/usr/bin:/bin"} | Env],

    %% Build the command with environment
    EnvStr = string:join([K ++ "=" ++ V || {K, V} <- FullEnv], " "),
    FullCmd = "cd " ++ WorkDir ++ " && " ++ EnvStr ++ " " ++ CmdStr ++ " 2>&1",

    Output = os:cmd(FullCmd),
    %% os:cmd always returns a string, exit code is not directly available
    %% For proper exit code handling, we'd need to use open_port
    ExitCode = 0,  % Assume success for now
    {list_to_binary(Output), ExitCode}.

%% @doc Report step completion to the controller.
-spec report_step_complete(gen_tcp:socket(), non_neg_integer(), non_neg_integer(), integer(), binary()) -> ok | {error, term()}.
report_step_complete(Socket, JobId, StepId, ExitCode, Output) ->
    Message = #{
        type => step_complete,
        payload => #{
            <<"job_id">> => JobId,
            <<"step_id">> => StepId,
            <<"exit_code">> => ExitCode,
            <<"output">> => Output
        }
    },
    send_protocol_message(Socket, Message).
