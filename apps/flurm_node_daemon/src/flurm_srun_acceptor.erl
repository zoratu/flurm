%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon srun Acceptor
%%%
%%% Ranch protocol handler for incoming srun connections.
%%% When srun gets a job allocation from slurmctld, it connects directly
%%% to slurmd (flurmnd) on the allocated nodes to launch tasks.
%%%
%%% This module handles (SLURM 22.05 message types):
%%% - REQUEST_LAUNCH_TASKS (6001) - Launch job step tasks
%%% - REQUEST_SIGNAL_TASKS (6004) - Signal running tasks
%%% - REQUEST_TERMINATE_TASKS (6006) - Terminate tasks
%%% - REQUEST_REATTACH_TASKS (6007) - Reattach to running tasks
%%%
%%% Wire format:
%%%   <<Length:32/big, Header:10/binary, Body/binary>>
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_srun_acceptor).

-behaviour(ranch_protocol).

-export([start_link/3]).
-export([init/3]).
%% Exported to suppress unused warning when I/O forwarding is disabled
-export([connect_io_socket/3]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% Minimum data needed to determine message length
-define(LENGTH_PREFIX_SIZE, 4).
%% Receive timeout (60 seconds for task execution)
-define(RECV_TIMEOUT, 60000).

%%====================================================================
%% Ranch Protocol Callbacks
%%====================================================================

%% @doc Start a new connection handler process.
%% Ranch calls this for each new srun connection.
-spec start_link(ranch:ref(), module(), map()) -> {ok, pid()}.
start_link(Ref, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Transport, Opts]),
    {ok, Pid}.

%% @doc Initialize the connection handler.
%% Performs Ranch handshake and enters the receive loop.
-spec init(ranch:ref(), module(), map()) -> ok | {error, term()}.
init(Ref, Transport, Opts) ->
    case ranch:handshake(Ref) of
        {ok, Socket} ->
            lager:info("New srun connection accepted on node daemon"),
            %% Get peer info for logging
            PeerInfo = case Transport:peername(Socket) of
                {ok, {Addr, Port}} ->
                    io_lib:format("~p:~p", [Addr, Port]);
                _ ->
                    "unknown"
            end,
            lager:debug("srun connection from ~s", [PeerInfo]),
            %% Set socket options
            ok = Transport:setopts(Socket, [
                {nodelay, true},      % TCP_NODELAY for low latency
                {keepalive, true},    % Enable TCP keepalive
                {active, false}       % Passive mode for controlled receives
            ]),
            %% Initialize connection state
            State = #{
                socket => Socket,
                transport => Transport,
                opts => Opts,
                buffer => <<>>,
                request_count => 0,
                start_time => erlang:system_time(millisecond),
                tasks => #{}  % Track running tasks {TaskId => Pid}
            },
            loop(State);
        {error, Reason} ->
            lager:warning("srun handshake failed: ~p", [Reason]),
            {error, Reason}
    end.

%%====================================================================
%% Main Receive Loop
%%====================================================================

%% @doc Main receive loop with buffer handling.
%% Uses active mode to receive both socket data and task messages.
-spec loop(map()) -> ok.
loop(#{socket := Socket, transport := Transport, buffer := Buffer} = State) ->
    %% Switch to active mode for non-blocking receive
    %% Handle case where socket might already be closed
    SocketClosed = maps:get(socket_closed, State, false),
    case SocketClosed of
        false ->
            case Transport:setopts(Socket, [{active, once}]) of
                ok -> ok;
                {error, _} ->
                    lager:debug("Socket already closed during setopts"),
                    ok
            end;
        true ->
            ok  % Socket already closed, skip setopts
    end,
    receive
        {tcp, Socket, Data} ->
            lager:info("srun acceptor received ~p bytes: hex=~s",
                       [byte_size(Data), binary_to_hex(Data)]),
            NewBuffer = <<Buffer/binary, Data/binary>>,
            case process_buffer(NewBuffer, State) of
                {continue, UpdatedState} ->
                    loop(UpdatedState);
                {close, _Reason} ->
                    close_connection(State)
            end;
        {tcp_closed, Socket} ->
            %% Socket closed, but continue processing task messages
            lager:info("srun connection closed, continuing to wait for task completion"),
            Tasks = maps:get(tasks, State, #{}),
            case maps:size(Tasks) of
                0 ->
                    %% No tasks running, we can exit
                    lager:info("No tasks running, exiting loop"),
                    ok;
                _ ->
                    %% Still have tasks, continue loop to receive task_complete messages
                    loop(State#{socket_closed => true})
            end;
        {tcp_error, Socket, Reason} ->
            lager:warning("srun connection error: ~p", [Reason]),
            cleanup_tasks(State),
            close_connection(State);

        %% Task output - forward to srun via I/O socket
        {task_output, TaskId, Output} ->
            lager:debug("Task ~p output: ~s", [TaskId, Output]),
            %% Forward output via I/O socket using SLURM I/O protocol
            IoSocket = maps:get(io_socket, State, undefined),
            {_JobId, _StepId, Gtid} = TaskId,
            forward_io_output(IoSocket, Gtid, Output),
            loop(State);

        %% Task completed - send EOF on I/O socket, then MESSAGE_TASK_EXIT
        {task_complete, TaskId, ExitStatus, Output} ->
            lager:info("Task ~p completed with status ~p", [TaskId, ExitStatus]),
            %% Get task info for the exit message
            {JobId, StepId, Gtid} = TaskId,
            Tasks = maps:get(tasks, State, #{}),
            IoSocket = maps:get(io_socket, State, undefined),

            %% Forward any remaining output
            case Output of
                <<>> -> ok;
                _ ->
                    lager:info("Task final output: ~s", [Output]),
                    forward_io_output(IoSocket, Gtid, Output)
            end,

            %% Send EOF on I/O socket (zero-length message for both stdout and stderr)
            send_io_eof(IoSocket, Gtid),

            %% Give srun time to process the EOF before we close and send MESSAGE_TASK_EXIT
            %% srun needs to process all I/O before receiving task exit notification
            timer:sleep(50),

            %% Close I/O socket after EOF - use shutdown to ensure data is flushed
            shutdown_io_socket(IoSocket),

            %% Send MESSAGE_TASK_EXIT to notify srun that task has exited
            %% Use step_het_comp from state (saved from REQUEST_LAUNCH_TASKS)
            StepHetComp = maps:get(step_het_comp, State, 16#FFFFFFFE),
            %% Convert Erlang exit status (already right-shifted by 8) to raw waitpid format
            %% SLURM expects: normal exit = (exit_code << 8), signal = signal_number
            %% Erlang gives us: (waitpid_status >> 8) for normal exits
            WaitpidStatus = ExitStatus bsl 8,
            TaskExitMsg = #{
                job_id => JobId,
                step_id => StepId,
                step_het_comp => StepHetComp,
                task_ids => [Gtid],
                return_codes => [WaitpidStatus]
            },
            lager:info("Sending MESSAGE_TASK_EXIT for job ~p step ~p (exit code ~p, waitpid status ~p)",
                       [JobId, StepId, ExitStatus, WaitpidStatus]),
            %% Try sending on original socket first, fall back to srun's port if closed
            Socket = maps:get(socket, State),
            Transport = maps:get(transport, State),
            case send_response(Socket, Transport, ?MESSAGE_TASK_EXIT, TaskExitMsg) of
                ok ->
                    lager:info("MESSAGE_TASK_EXIT sent on original socket");
                {error, closed} ->
                    %% Socket was closed after RESPONSE_LAUNCH_TASKS, try srun's resp_port
                    SrunAddr = maps:get(srun_addr, State, {127,0,0,1}),
                    SrunRespPorts = maps:get(resp_ports, State, []),
                    case SrunRespPorts of
                        [Port | _] when Port > 0 ->
                            lager:info("Original socket closed, connecting to srun resp_port ~p:~p", [SrunAddr, Port]),
                            %% Use linger to ensure data is sent before close
                            case gen_tcp:connect(SrunAddr, Port, [binary, {active, false}, {nodelay, true}, {linger, {true, 10}}, {send_timeout, 5000}], 5000) of
                                {ok, NewSock} ->
                                    send_response_tcp(NewSock, ?MESSAGE_TASK_EXIT, TaskExitMsg),
                                    %% Wait for srun to process the message
                                    timer:sleep(500),
                                    %% Try to read any response (to keep connection alive)
                                    case gen_tcp:recv(NewSock, 0, 1000) of
                                        {ok, _Data} -> lager:info("Got response from srun");
                                        {error, timeout} -> lager:info("No response from srun (timeout)");
                                        {error, closed} -> lager:info("srun closed connection after MESSAGE_TASK_EXIT");
                                        {error, Other} -> lager:info("Error waiting for srun: ~p", [Other])
                                    end,
                                    gen_tcp:close(NewSock);
                                {error, ConnErr} ->
                                    lager:error("Failed to connect to srun port: ~p", [ConnErr])
                            end;
                        _ ->
                            lager:error("Socket closed and no srun resp_port to retry")
                    end;
                {error, Reason} ->
                    lager:error("Failed to send MESSAGE_TASK_EXIT: ~p", [Reason])
            end,

            %% Update task state (remove io_socket since we closed it)
            NewTasks = maps:remove(TaskId, Tasks),
            NewState = State#{tasks => NewTasks, io_socket => undefined},
            %% If no more tasks and socket is closed, exit; otherwise continue loop
            SocketClosed = maps:get(socket_closed, State, false),
            case {maps:size(NewTasks), SocketClosed} of
                {0, true} ->
                    lager:info("All tasks complete and socket closed, exiting"),
                    ok;
                {0, false} ->
                    lager:info("All tasks complete, waiting for client to close connection"),
                    loop(NewState);
                _ ->
                    loop(NewState)
            end;

        %% Task error
        {task_error, TaskId, Reason} ->
            lager:error("Task ~p error: ~p", [TaskId, Reason]),
            NewTasks = maps:remove(TaskId, maps:get(tasks, State, #{})),
            send_task_exit(Socket, Transport, 1),  % Non-zero exit
            loop(State#{tasks => NewTasks});

        %% Task timeout
        {task_timeout, TaskId} ->
            lager:warning("Task ~p timed out", [TaskId]),
            NewTasks = maps:remove(TaskId, maps:get(tasks, State, #{})),
            send_task_exit(Socket, Transport, 124),  % Timeout exit code
            loop(State#{tasks => NewTasks})

    after ?RECV_TIMEOUT ->
        %% Check if we have running tasks
        Tasks = maps:get(tasks, State, #{}),
        case maps:size(Tasks) of
            0 ->
                lager:debug("srun connection idle timeout"),
                close_connection(State);
            _ ->
                %% Still have running tasks, keep waiting
                loop(State)
        end
    end.

%% @doc Send task exit status to srun.
-spec send_task_exit(inet:socket(), module(), integer()) -> ok | {error, term()}.
send_task_exit(Socket, Transport, ExitStatus) ->
    lager:debug("Sending task exit status ~p to srun", [ExitStatus]),
    Response = #slurm_rc_response{return_code = ExitStatus},
    send_response(Socket, Transport, ?RESPONSE_SLURM_RC, Response).

%%====================================================================
%% Buffer Processing
%%====================================================================

%% @doc Process accumulated data in the buffer.
-spec process_buffer(binary(), map()) -> {continue, map()} | {close, term()}.
process_buffer(Buffer, State) when byte_size(Buffer) < ?LENGTH_PREFIX_SIZE ->
    {continue, State#{buffer => Buffer}};
process_buffer(<<Length:32/big, Rest/binary>> = Buffer, State)
  when byte_size(Rest) < Length ->
    {continue, State#{buffer => Buffer}};
process_buffer(<<Length:32/big, _Rest/binary>> = _Buffer, _State)
  when Length < ?SLURM_HEADER_SIZE ->
    lager:warning("Invalid srun message length: ~p", [Length]),
    {close, invalid_message_length};
process_buffer(<<Length:32/big, Rest/binary>>, State)
  when byte_size(Rest) >= Length ->
    <<MessageData:Length/binary, Remaining/binary>> = Rest,
    FullMessage = <<Length:32/big, MessageData/binary>>,
    case handle_message(FullMessage, State) of
        {ok, UpdatedState} ->
            process_buffer(Remaining, UpdatedState#{buffer => <<>>});
        {error, Reason} ->
            lager:warning("srun message handling failed: ~p", [Reason]),
            process_buffer(Remaining, State#{buffer => <<>>})
    end;
process_buffer(Buffer, State) ->
    {continue, State#{buffer => Buffer}}.

%%====================================================================
%% Message Handling
%%====================================================================

%% @doc Handle a complete message from srun.
-spec handle_message(binary(), map()) -> {ok, map()} | {error, term()}.
handle_message(MessageBin, #{socket := Socket, transport := Transport,
                             request_count := Count} = State) ->
    lager:info("Handling srun message, size=~p bytes", [byte_size(MessageBin)]),
    DecodeResult = case flurm_protocol_codec:decode_with_extra(MessageBin) of
        {ok, Msg, ExtraInfo, Rest} ->
            lager:info("Decoded message with auth: msg_type=~p",
                       [Msg#slurm_msg.header#slurm_header.msg_type]),
            {ok, Msg, ExtraInfo, Rest};
        {error, AuthErr} ->
            lager:info("Auth decode failed (~p), trying plain decode", [AuthErr]),
            case flurm_protocol_codec:decode(MessageBin) of
                {ok, Msg, Rest} ->
                    lager:info("Plain decode succeeded: msg_type=~p",
                               [Msg#slurm_msg.header#slurm_header.msg_type]),
                    {ok, Msg, #{}, Rest};
                PlainErr ->
                    lager:warning("Plain decode also failed: ~p", [PlainErr]),
                    PlainErr
            end
    end,
    case DecodeResult of
        {ok, #slurm_msg{header = Header, body = Body}, _ExtraInfo, _Rest} ->
            MsgType = Header#slurm_header.msg_type,
            lager:info("srun message type: ~p (~s)",
                       [MsgType, flurm_protocol_codec:message_type_name(MsgType)]),
            case handle_srun_request(MsgType, Body, State) of
                {ok, ResponseType, ResponseBody, NewState} ->
                    send_response(Socket, Transport, ResponseType, ResponseBody),
                    {ok, NewState#{request_count => Count + 1}};
                {ok, NewState} ->
                    %% No response needed
                    {ok, NewState#{request_count => Count + 1}};
                {error, Reason} ->
                    ErrorResponse = #slurm_rc_response{return_code = -1},
                    send_response(Socket, Transport, ?RESPONSE_SLURM_RC, ErrorResponse),
                    lager:warning("srun handler error: ~p", [Reason]),
                    {ok, State#{request_count => Count + 1}}
            end;
        {error, Reason} ->
            lager:warning("Failed to decode srun message: ~p", [Reason]),
            ErrorResponse = #slurm_rc_response{return_code = -1},
            send_response(Socket, Transport, ?RESPONSE_SLURM_RC, ErrorResponse),
            {error, Reason}
    end.

%%====================================================================
%% srun Request Handlers
%%====================================================================

%% @doc Handle specific srun request types.
-spec handle_srun_request(non_neg_integer(), term(), map()) ->
    {ok, non_neg_integer(), term(), map()} | {ok, map()} | {error, term()}.

%% REQUEST_LAUNCH_TASKS - srun wants to launch tasks on this node
handle_srun_request(?REQUEST_LAUNCH_TASKS, Body, #{socket := Socket, transport := Transport} = State) ->
    lager:info("REQUEST_LAUNCH_TASKS received: ~p", [Body]),
    %% Extract job_id, step_id, step_het_comp, io_port, resp_port, and io_key from the request body
    {JobId, StepId, StepHetComp, IoPorts, RespPorts, IoKey} = case Body of
        #launch_tasks_request{job_id = JId, step_id = SId, step_het_comp = SHC, io_port = IOPorts, resp_port = RPorts, io_key = Key} ->
            {JId, SId, SHC, IOPorts, RPorts, Key};
        #{job_id := JId, step_id := SId} ->
            {JId, SId, maps:get(step_het_comp, Body, 16#FFFFFFFE), maps:get(io_port, Body, []), maps:get(resp_port, Body, []), <<>>};
        _ ->
            {0, 0, 16#FFFFFFFE, [], [], <<>>}
    end,

    %% Get srun's IP address from the socket connection
    SrunAddr = case Transport:peername(Socket) of
        {ok, {Addr, _Port}} -> Addr;
        _ -> {127, 0, 0, 1}
    end,
    lager:info("srun address: ~p, I/O ports: ~p, resp_ports: ~p, io_key size: ~p",
               [SrunAddr, IoPorts, RespPorts, byte_size(IoKey)]),

    %% Connect to I/O port BEFORE sending RESPONSE_LAUNCH_TASKS
    %% According to SLURM protocol, slurmstepd connects to srun's I/O port
    %% and sends io_init_msg BEFORE slurmd sends RESPONSE_LAUNCH_TASKS.
    %% This keeps the original socket open for MESSAGE_TASK_EXIT.
    IoSocket = case IoPorts of
        [IoPort | _] when IoPort > 0 ->
            lager:info("Connecting to srun I/O port ~p before RESPONSE_LAUNCH_TASKS", [IoPort]),
            connect_io_socket(SrunAddr, IoPort, IoKey);
        _ ->
            lager:warning("No I/O port available, task output won't be forwarded"),
            undefined
    end,

    %% Store io_socket, ports, and step_het_comp in state for I/O forwarding and MESSAGE_TASK_EXIT
    StateWithIo = State#{io_socket => IoSocket, srun_addr => SrunAddr, resp_ports => RespPorts, io_ports => IoPorts, step_het_comp => StepHetComp},

    %% Extract task info from body
    %% Body should contain: job_id, step_id, task info, command, environment, etc.
    case launch_tasks(Body, StateWithIo) of
        {ok, TaskInfo, NewState} ->
            %% Send success response with task PIDs
            %% Format response using launch_tasks_response record
            %% IMPORTANT: step_id must come FIRST in the response body
            LocalPids = maps:get(local_pids, TaskInfo, []),
            Gtids = maps:get(gtids, TaskInfo, [0]),  % Global task ID
            %% Use step_het_comp from request - srun needs to match this for task state lookup
            Response = #launch_tasks_response{
                job_id = JobId,
                step_id = StepId,
                step_het_comp = StepHetComp,  %% Must match request value
                return_code = 0,
                node_name = get_hostname(),
                count_of_pids = length(LocalPids),
                local_pids = LocalPids,
                gtids = Gtids
            },
            %% RESPONSE_LAUNCH_TASKS must go on original socket (srun is waiting there)
            lager:info("Sending RESPONSE_LAUNCH_TASKS on original socket"),
            {ok, ?RESPONSE_LAUNCH_TASKS, Response, NewState};
        {error, Reason} ->
            lager:error("Failed to launch tasks: ~p", [Reason]),
            %% Close io_socket on failure
            close_io_socket(IoSocket),
            {error, Reason}
    end;

%% REQUEST_SIGNAL_TASKS - Signal running tasks
handle_srun_request(?REQUEST_SIGNAL_TASKS, Body, State) ->
    lager:info("REQUEST_SIGNAL_TASKS received: ~p", [Body]),
    Signal = maps:get(signal, Body, 15),  % Default SIGTERM
    TaskIds = maps:get(task_ids, Body, all),
    signal_tasks(TaskIds, Signal, State),
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response, State};

%% REQUEST_TERMINATE_TASKS - Terminate tasks
handle_srun_request(?REQUEST_TERMINATE_TASKS, Body, State) ->
    lager:info("REQUEST_TERMINATE_TASKS received: ~p", [Body]),
    TaskIds = maps:get(task_ids, Body, all),
    terminate_tasks(TaskIds, State),
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response, State};

%% REQUEST_REATTACH_TASKS - Reattach to running tasks
handle_srun_request(?REQUEST_REATTACH_TASKS, Body, State) ->
    lager:info("REQUEST_REATTACH_TASKS received: ~p", [Body]),
    JobId = maps:get(job_id, Body, 0),
    StepId = maps:get(step_id, Body, 0),
    case find_tasks(JobId, StepId, State) of
        {ok, TaskInfo} ->
            Response = #{
                return_code => 0,
                task_info => TaskInfo
            },
            {ok, ?RESPONSE_REATTACH_TASKS, Response, State};
        {error, not_found} ->
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response, State}
    end;

%% REQUEST_JOB_READY (4016) - srun checks if node is ready for job
handle_srun_request(?REQUEST_JOB_READY, Body, State) ->
    lager:info("REQUEST_JOB_READY received: ~p", [Body]),
    %% Extract job_id from body (typically: job_id:32, step_id:32)
    JobId = case Body of
        <<JId:32/big, _/binary>> -> JId;
        #{job_id := JId} -> JId;
        _ -> 0
    end,
    lager:info("Job ready check for job_id=~p - reporting READY", [JobId]),
    %% RESPONSE_JOB_READY format: return_code(32) where 0=ready, non-zero=not ready
    %% SLURM_SUCCESS (0) = ready
    Response = #{return_code => 0, prolog_running => 0},
    {ok, ?RESPONSE_JOB_READY, Response, State};

%% Unknown message type
handle_srun_request(MsgType, Body, _State) ->
    lager:warning("Unknown srun message type ~p: ~p", [MsgType, Body]),
    {error, {unknown_message_type, MsgType}}.

%%====================================================================
%% Task Management
%%====================================================================

%% @doc Launch tasks for a job step.
-spec launch_tasks(term(), map()) -> {ok, map(), map()} | {error, term()}.
launch_tasks(#launch_tasks_request{} = Req, #{tasks := Tasks} = State) ->
    %% Extract command and environment from the request record
    #launch_tasks_request{
        job_id = JobId,
        step_id = StepId,
        uid = _Uid,
        gid = _Gid,
        user_name = UserName,
        argv = Argv,
        env = Env0,
        cwd = Cwd0,
        ntasks = _Ntasks,
        global_task_ids = GlobalTaskIds0
    } = Req,

    %% Get command and args from argv
    {Command, Args} = case Argv of
        [Cmd | Rest] when is_binary(Cmd) -> {Cmd, Rest};
        _ -> {<<"/bin/echo">>, [<<"Hello from FLURM">>]}
    end,

    %% Convert env list to key-value pairs if needed
    Env = convert_env_to_pairs(Env0),

    %% Normalize cwd (strip null terminator from SLURM protocol)
    Cwd = case strip_null(Cwd0) of
        <<>> -> <<"/tmp">>;
        CleanCwd -> CleanCwd
    end,

    lager:info("Launching task: user=~s job=~p step=~p cmd=~s args=~p cwd=~s",
               [UserName, JobId, StepId, Command, Args, Cwd]),

    %% Extract global task IDs from request (srun tells us which task IDs to use)
    %% Format: [[TaskIds for node 0], [TaskIds for node 1], ...]
    %% For single node single task: [[0]]
    Gtid = case GlobalTaskIds0 of
        [[FirstGtid | _] | _] when is_integer(FirstGtid) -> FirstGtid;
        _ -> 0  % Default to task 0
    end,
    lager:info("Using global task ID (gtid) = ~p from request", [Gtid]),

    %% Create a task process to execute the command
    %% Internal TaskId includes gtid for tracking
    TaskId = {JobId, StepId, Gtid},
    Parent = self(),

    %% Spawn task executor
    TaskPid = spawn_link(fun() ->
        execute_task(Command, Args, Env, Parent, TaskId)
    end),

    %% Use gtid for response and a reasonable local PID (pseudo-PID for now)
    LocalPid = erlang:unique_integer([positive]) rem 65536,

    TaskInfo = #{
        task_id => TaskId,
        pid => TaskPid,
        job_id => JobId,
        step_id => StepId,
        state => running,
        local_pids => [LocalPid],
        gtids => [Gtid]
    },

    NewTasks = maps:put(TaskId, TaskInfo, Tasks),
    {ok, TaskInfo, State#{tasks => NewTasks}};

%% Fallback for raw binary bodies (decode failed)
launch_tasks(Body, State) when is_binary(Body) ->
    lager:warning("launch_tasks received raw binary body (~p bytes), using fallback", [byte_size(Body)]),
    %% Create a minimal request record with defaults
    FallbackReq = #launch_tasks_request{
        job_id = 0,
        step_id = 0,
        user_name = <<"unknown">>,
        argv = [<<"/bin/echo">>, <<"FLURM fallback task">>],
        env = [],
        cwd = <<"/tmp">>
    },
    launch_tasks(FallbackReq, State);

%% Fallback for map bodies (legacy)
launch_tasks(Body, State) when is_map(Body) ->
    lager:info("launch_tasks received map body, converting to record"),
    %% Extract from map
    JobId = maps:get(job_id, Body, 0),
    StepId = maps:get(step_id, Body, 0),
    Argv = case maps:get(argv, Body, undefined) of
        undefined ->
            Cmd = maps:get(command, Body, <<"/bin/echo">>),
            Args = maps:get(args, Body, [<<"Hello from FLURM">>]),
            [Cmd | Args];
        A -> A
    end,
    Env = maps:get(environment, Body, maps:get(env, Body, [])),
    Cwd = maps:get(cwd, Body, <<"/tmp">>),
    Req = #launch_tasks_request{
        job_id = JobId,
        step_id = StepId,
        user_name = maps:get(user_name, Body, <<"unknown">>),
        argv = Argv,
        env = Env,
        cwd = Cwd
    },
    launch_tasks(Req, State).

%% @doc Execute a task command.
-spec execute_task(binary(), list(), list(), pid(), term()) -> ok.
execute_task(Command, Args, Env, Parent, TaskId) ->
    %% Strip null terminators from command and args (SLURM protocol includes them)
    CleanCommand = strip_null(Command),
    CleanArgs = [strip_null(A) || A <- Args, is_binary(A)],

    lager:info("Task ~p executing: ~s ~p", [TaskId, CleanCommand, CleanArgs]),

    %% Build command string
    CmdStr = case CleanArgs of
        [] -> binary_to_list(CleanCommand);
        _ ->
            ArgStrs = [binary_to_list(A) || A <- CleanArgs],
            string:join([binary_to_list(CleanCommand) | ArgStrs], " ")
    end,

    %% Set up environment
    EnvOpts = case Env of
        L when is_list(L), length(L) > 0 ->
            [{env, [{binary_to_list(K), binary_to_list(V)} || {K, V} <- L]}];
        _ ->
            []
    end,

    %% Execute command
    PortOpts = [
        stream,
        exit_status,
        use_stdio,
        stderr_to_stdout,
        binary
        | EnvOpts
    ],

    try
        Port = open_port({spawn, CmdStr}, PortOpts),
        collect_output(Port, Parent, TaskId, <<>>)
    catch
        error:Reason ->
            lager:error("Task ~p failed to start: ~p", [TaskId, Reason]),
            Parent ! {task_error, TaskId, Reason}
    end.

%% @doc Collect output from a task port.
-spec collect_output(port(), pid(), term(), binary()) -> ok.
collect_output(Port, Parent, TaskId, Acc) ->
    receive
        {Port, {data, Data}} ->
            lager:debug("Task ~p output: ~s", [TaskId, Data]),
            %% Send output to parent for streaming to srun
            Parent ! {task_output, TaskId, Data},
            %% Accumulate only for logging, don't send again on completion
            collect_output(Port, Parent, TaskId, <<Acc/binary, Data/binary>>);
        {Port, {exit_status, Status}} ->
            lager:info("Task ~p exited with status ~p", [TaskId, Status]),
            %% Don't send Acc - output was already streamed via task_output messages
            Parent ! {task_complete, TaskId, Status, <<>>};
        {'EXIT', Port, Reason} ->
            lager:warning("Task ~p port exited: ~p", [TaskId, Reason]),
            Parent ! {task_error, TaskId, Reason}
    after 300000 ->  % 5 minute timeout
        lager:warning("Task ~p timed out", [TaskId]),
        catch port_close(Port),
        Parent ! {task_timeout, TaskId}
    end.

%% @doc Signal specific tasks.
-spec signal_tasks(list() | all, integer(), map()) -> ok.
signal_tasks(all, Signal, #{tasks := Tasks}) ->
    maps:foreach(fun(_TaskId, #{pid := Pid}) ->
        Pid ! {signal, Signal}
    end, Tasks);
signal_tasks(TaskIds, Signal, #{tasks := Tasks}) ->
    lists:foreach(fun(TaskId) ->
        case maps:get(TaskId, Tasks, undefined) of
            #{pid := Pid} -> Pid ! {signal, Signal};
            undefined -> ok
        end
    end, TaskIds).

%% @doc Terminate specific tasks.
-spec terminate_tasks(list() | all, map()) -> ok.
terminate_tasks(all, #{tasks := Tasks}) ->
    maps:foreach(fun(_TaskId, #{pid := Pid}) ->
        exit(Pid, kill)
    end, Tasks);
terminate_tasks(TaskIds, #{tasks := Tasks}) ->
    lists:foreach(fun(TaskId) ->
        case maps:get(TaskId, Tasks, undefined) of
            #{pid := Pid} -> exit(Pid, kill);
            undefined -> ok
        end
    end, TaskIds).

%% @doc Find tasks for a job/step.
-spec find_tasks(integer(), integer(), map()) -> {ok, map()} | {error, not_found}.
find_tasks(JobId, StepId, #{tasks := Tasks}) ->
    MatchingTasks = maps:filter(fun(_TaskId, #{job_id := J, step_id := S}) ->
        J == JobId andalso S == StepId
    end, Tasks),
    case maps:size(MatchingTasks) of
        0 -> {error, not_found};
        _ -> {ok, MatchingTasks}
    end.

%% @doc Clean up all tasks on connection close.
-spec cleanup_tasks(map()) -> ok.
cleanup_tasks(#{tasks := Tasks}) ->
    maps:foreach(fun(TaskId, #{pid := Pid}) ->
        lager:debug("Cleaning up task ~p", [TaskId]),
        exit(Pid, shutdown)
    end, Tasks),
    ok;
cleanup_tasks(_) ->
    ok.

%%====================================================================
%% Response Handling
%%====================================================================

%% @doc Send a response message back to srun.
-spec send_response(inet:socket(), module(), non_neg_integer(), term()) -> ok | {error, term()}.
send_response(Socket, Transport, MsgType, Body) ->
    %% Use regular encoding with MUNGE auth
    case flurm_protocol_codec:encode_response(MsgType, Body) of
        {ok, ResponseBin} ->
            Size = byte_size(ResponseBin),
            lager:info("SENDING response type=~p size=~p bytes", [MsgType, Size]),
            %% Debug: show first 64 bytes of response
            First64 = case Size > 64 of
                true -> binary:part(ResponseBin, 0, 64);
                false -> ResponseBin
            end,
            lager:info("SENDING first 64 bytes: ~s", [binary_to_hex(First64)]),
            case Transport:send(Socket, ResponseBin) of
                ok ->
                    lager:info("SENT ~p bytes OK", [Size]),
                    ok;
                {error, Reason} ->
                    lager:warning("Failed to send srun response: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            lager:warning("Failed to encode srun response: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Send a response message on a TCP socket (gen_tcp).
%% Uses full SLURM message format with length prefix and auth.
-spec send_response_tcp(gen_tcp:socket(), non_neg_integer(), term()) -> ok | {error, term()}.
send_response_tcp(Socket, MsgType, Body) ->
    %% Use proper auth (srun requires it)
    case flurm_protocol_codec:encode_response(MsgType, Body) of
        {ok, ResponseBin} ->
            Size = byte_size(ResponseBin),
            lager:info("SENDING response type=~p size=~p bytes on resp_socket", [MsgType, Size]),
            First64 = case Size > 64 of
                true -> binary:part(ResponseBin, 0, 64);
                false -> ResponseBin
            end,
            lager:info("SENDING first 64 bytes: ~s", [binary_to_hex(First64)]),
            %% Log FULL message in hex for debugging
            lager:info("FULL HEX (~p bytes): ~s", [Size, binary_to_hex_full(ResponseBin)]),
            case gen_tcp:send(Socket, ResponseBin) of
                ok ->
                    lager:info("SENT ~p bytes on resp_socket OK", [Size]),
                    ok;
                {error, Reason} ->
                    lager:warning("Failed to send on resp_socket: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            lager:warning("Failed to encode response for resp_socket: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Convert full binary to hex (no truncation).
-spec binary_to_hex_full(binary()) -> binary().
binary_to_hex_full(Bin) when is_binary(Bin) ->
    list_to_binary([[io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(Bin)]]).

%%====================================================================
%% Helper Functions
%%====================================================================

%% @doc Get the hostname of this node.
-spec get_hostname() -> binary().
get_hostname() ->
    case inet:gethostname() of
        {ok, Name} -> list_to_binary(Name);
        _ -> <<"unknown">>
    end.

%% @doc Strip trailing null bytes from binary (SLURM strings include null terminators).
-spec strip_null(binary()) -> binary().
strip_null(<<>>) -> <<>>;
strip_null(Bin) ->
    case binary:last(Bin) of
        0 -> strip_null(binary:part(Bin, 0, byte_size(Bin) - 1));
        _ -> Bin
    end.

%% @doc Close the connection gracefully.
-spec close_connection(map()) -> ok.
close_connection(#{socket := Socket, transport := Transport,
                   request_count := Count, start_time := StartTime} = State) ->
    Duration = erlang:system_time(millisecond) - StartTime,
    lager:info("Closing srun connection after ~p requests, duration: ~p ms",
                [Count, Duration]),
    %% Close io_socket if open
    case maps:get(io_socket, State, undefined) of
        Sock when is_port(Sock) -> gen_tcp:close(Sock);
        _ -> ok
    end,
    Transport:close(Socket),
    ok.

%%====================================================================
%% I/O Socket Handling (SLURM I/O Forwarding Protocol)
%%====================================================================

%% @doc Connect to srun's I/O port and send initialization message.
%% Returns the connected socket or undefined on failure.
%% IoKey is the credential signature from REQUEST_LAUNCH_TASKS.
-spec connect_io_socket(tuple(), non_neg_integer(), binary()) -> gen_tcp:socket() | undefined.
connect_io_socket(SrunAddr, IoPort, IoKey) ->
    lager:info("Connecting to srun I/O port ~p:~p (io_key size: ~p)", [SrunAddr, IoPort, byte_size(IoKey)]),
    case gen_tcp:connect(SrunAddr, IoPort, [binary, {active, false}, {nodelay, true}], 5000) of
        {ok, Socket} ->
            lager:info("Connected to srun I/O port successfully"),
            %% Send I/O init message
            %% NodeId=0 for single node, stdout/stderr objects = 1 each
            %% CRITICAL: io_init_msg_unpack REJECTS IO_PROTOCOL_VERSION (0xb001)!
            %% The check is: version != IO_PROTOCOL_VERSION && version >= SLURM_MIN_PROTOCOL_VERSION
            %% So we MUST use SLURM_PROTOCOL_VERSION (0x2600) instead.
            %% Format: length:32 + version:16 + nodeid:32 + stdout_objs:32 + stderr_objs:32 + io_key_len:32 + io_key
            %% IoKey is the credential signature that srun will validate
            InitMsg = flurm_io_protocol:encode_io_init_msg(
                ?SLURM_PROTOCOL_VERSION,  % Must use SLURM protocol version (0x2600), NOT IO_PROTOCOL_VERSION!
                0,        % NodeId
                1,        % StdoutObjs
                1,        % StderrObjs
                IoKey     % Credential signature for I/O authentication
            ),
            lager:info("Sending io_init_msg (~p bytes): hex=~s",
                       [byte_size(InitMsg), binary_to_hex(InitMsg)]),
            case gen_tcp:send(Socket, InitMsg) of
                ok ->
                    lager:info("Sent io_init_msg to srun"),
                    Socket;
                {error, Reason} ->
                    lager:error("Failed to send io_init_msg: ~p", [Reason]),
                    gen_tcp:close(Socket),
                    undefined
            end;
        {error, Reason} ->
            lager:error("Failed to connect to srun I/O port ~p:~p: ~p",
                        [SrunAddr, IoPort, Reason]),
            undefined
    end.

%% @doc Close the I/O socket if it exists.
-spec close_io_socket(gen_tcp:socket() | undefined) -> ok.
close_io_socket(undefined) -> ok;
close_io_socket(Socket) ->
    lager:debug("Closing I/O socket"),
    gen_tcp:close(Socket),
    ok.

%% @doc Shutdown the I/O socket gracefully.
%% Uses shutdown to ensure data is flushed before close.
-spec shutdown_io_socket(gen_tcp:socket() | undefined) -> ok.
shutdown_io_socket(undefined) -> ok;
shutdown_io_socket(Socket) ->
    lager:debug("Shutting down I/O socket"),
    %% Shutdown write direction first to signal EOF to peer
    gen_tcp:shutdown(Socket, write),
    %% Give peer time to read remaining data
    timer:sleep(10),
    %% Then close the socket
    gen_tcp:close(Socket),
    ok.

%% @doc Forward task output via I/O socket.
%% Encodes output as SLURM I/O protocol stdout message.
-spec forward_io_output(gen_tcp:socket() | undefined, non_neg_integer(), binary()) -> ok.
forward_io_output(undefined, _Gtid, _Output) ->
    %% No I/O socket, output not forwarded
    ok;
forward_io_output(Socket, Gtid, Output) when byte_size(Output) > 0 ->
    %% Encode as stdout message: io_hdr + data
    %% Gtid = global task ID, Ltid = local task ID (same for single task)
    Ltid = Gtid rem 256,
    Msg = flurm_io_protocol:encode_stdout(Gtid, Ltid, Output),
    case gen_tcp:send(Socket, Msg) of
        ok ->
            lager:debug("Forwarded ~p bytes of output to srun", [byte_size(Output)]);
        {error, Reason} ->
            lager:warning("Failed to forward output to srun: ~p", [Reason])
    end,
    ok;
forward_io_output(_Socket, _Gtid, _Output) ->
    ok.

%% @doc Send EOF marker on I/O socket.
%% Signals end of output stream to srun.
%% We must send EOF for both stdout and stderr since we declared
%% stdout_objs=1 and stderr_objs=1 in io_init_msg.
-spec send_io_eof(gen_tcp:socket() | undefined, non_neg_integer()) -> ok.
send_io_eof(undefined, _Gtid) -> ok;
send_io_eof(Socket, Gtid) ->
    Ltid = Gtid rem 256,
    %% Send EOF for stdout (type=1)
    StdoutEof = flurm_io_protocol:encode_io_hdr(?SLURM_IO_STDOUT, Gtid, Ltid, 0),
    %% Send EOF for stderr (type=2)
    StderrEof = flurm_io_protocol:encode_io_hdr(?SLURM_IO_STDERR, Gtid, Ltid, 0),
    case gen_tcp:send(Socket, <<StdoutEof/binary, StderrEof/binary>>) of
        ok ->
            lager:info("Sent EOF for stdout and stderr to srun I/O socket");
        {error, Reason} ->
            lager:warning("Failed to send EOF to srun: ~p", [Reason])
    end,
    ok.

%% @doc Convert binary to hex string for debugging
-spec binary_to_hex(binary()) -> binary().
binary_to_hex(Bin) when is_binary(Bin) ->
    %% Limit to first 64 bytes for readability
    TruncatedBin = case byte_size(Bin) > 64 of
        true -> binary:part(Bin, 0, 64);
        false -> Bin
    end,
    list_to_binary([[io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(TruncatedBin)]]).

%% @doc Convert environment list to key-value pairs.
%% Environment comes as list of "KEY=VALUE\0" binaries from SLURM.
%% We need to strip null terminators to use with open_port.
-spec convert_env_to_pairs([binary()]) -> [{binary(), binary()}].
convert_env_to_pairs(EnvList) when is_list(EnvList) ->
    lists:filtermap(fun(EnvVar) when is_binary(EnvVar) ->
        %% Strip null terminator first
        CleanVar = strip_null(EnvVar),
        case binary:split(CleanVar, <<"=">>) of
            [Key, Value] -> {true, {strip_null(Key), strip_null(Value)}};
            [Key] -> {true, {strip_null(Key), <<>>}};
            _ -> false
        end;
    (_) -> false
    end, EnvList);
convert_env_to_pairs(_) ->
    [].
