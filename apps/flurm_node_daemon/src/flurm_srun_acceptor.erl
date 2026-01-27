%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon srun Acceptor
%%%
%%% Ranch protocol handler for incoming srun connections.
%%% When srun gets a job allocation from slurmctld, it connects directly
%%% to slurmd (flurmnd) on the allocated nodes to launch tasks.
%%%
%%% This module handles:
%%% - REQUEST_LAUNCH_TASKS (5008) - Launch job step tasks
%%% - REQUEST_SIGNAL_TASKS (5010) - Signal running tasks
%%% - REQUEST_TERMINATE_TASKS (5011) - Terminate tasks
%%% - REQUEST_REATTACH_TASKS (5012) - Reattach to running tasks
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
    ok = Transport:setopts(Socket, [{active, once}]),
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
            lager:debug("srun connection closed normally"),
            cleanup_tasks(State),
            ok;
        {tcp_error, Socket, Reason} ->
            lager:warning("srun connection error: ~p", [Reason]),
            cleanup_tasks(State),
            close_connection(State);

        %% Task output - forward to srun
        {task_output, TaskId, Output} ->
            lager:debug("Task ~p output: ~s", [TaskId, Output]),
            %% For now, just log it. Full implementation would stream to srun.
            %% TODO: Implement I/O streaming protocol
            loop(State);

        %% Task completed - send exit status
        {task_complete, TaskId, ExitStatus, Output} ->
            lager:info("Task ~p completed with status ~p", [TaskId, ExitStatus]),
            %% Log the output
            case Output of
                <<>> -> ok;
                _ -> lager:info("Task output: ~s", [Output])
            end,
            %% Update task state
            NewTasks = maps:remove(TaskId, maps:get(tasks, State, #{})),
            %% If no more tasks, we can send completion
            case maps:size(NewTasks) of
                0 ->
                    %% Send exit status to srun
                    send_task_exit(Socket, Transport, ExitStatus),
                    close_connection(State#{tasks => NewTasks});
                _ ->
                    loop(State#{tasks => NewTasks})
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
handle_srun_request(?REQUEST_LAUNCH_TASKS, Body, State) ->
    lager:info("REQUEST_LAUNCH_TASKS received: ~p", [Body]),
    %% Extract task info from body
    %% Body should contain: job_id, step_id, task info, command, environment, etc.
    case launch_tasks(Body, State) of
        {ok, TaskInfo, NewState} ->
            %% Send success response with task PIDs
            %% Format response using launch_tasks_response record
            LocalPids = maps:get(local_pids, TaskInfo, []),
            Gtids = maps:get(gtids, TaskInfo, [0]),  % Global task ID
            Response = #launch_tasks_response{
                return_code = 0,
                node_name = get_hostname(),
                srun_node_id = 0,
                count_of_pids = length(LocalPids),
                local_pids = LocalPids,
                gtids = Gtids
            },
            {ok, ?RESPONSE_LAUNCH_TASKS, Response, NewState};
        {error, Reason} ->
            lager:error("Failed to launch tasks: ~p", [Reason]),
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
launch_tasks(Body, #{tasks := Tasks} = State) ->
    %% Extract command and environment from body
    Command = case Body of
        #{command := Cmd} when is_binary(Cmd) -> Cmd;
        #{command := Cmd} when is_list(Cmd) -> list_to_binary(Cmd);
        #{argv := [Cmd | _]} when is_binary(Cmd) -> Cmd;
        _ -> <<"/bin/echo">>
    end,
    Args = case Body of
        #{argv := [_ | A]} -> A;
        #{args := A} -> A;
        _ -> [<<"Hello from FLURM">>]
    end,
    Env = maps:get(environment, Body, maps:get(env, Body, [])),
    JobId = maps:get(job_id, Body, 0),
    StepId = maps:get(step_id, Body, 0),

    lager:info("Launching task: job=~p step=~p cmd=~s args=~p",
               [JobId, StepId, Command, Args]),

    %% Create a task process to execute the command
    TaskId = {JobId, StepId, erlang:unique_integer([positive])},
    Parent = self(),

    %% Spawn task executor
    TaskPid = spawn_link(fun() ->
        execute_task(Command, Args, Env, Parent, TaskId)
    end),

    %% Generate a local PID placeholder (SLURM expects OS PIDs)
    %% We use the unique_integer part of TaskId as a pseudo-PID
    {_, _, UniqueId} = TaskId,
    LocalPid = UniqueId rem 65536,  % Keep it in a reasonable range
    Gtid = UniqueId rem 256,        % Global task ID (0-255 for most cases)

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
    {ok, TaskInfo, State#{tasks => NewTasks}}.

%% @doc Execute a task command.
-spec execute_task(binary(), list(), list(), pid(), term()) -> ok.
execute_task(Command, Args, Env, Parent, TaskId) ->
    lager:info("Task ~p executing: ~s ~p", [TaskId, Command, Args]),

    %% Build command string
    CmdStr = case Args of
        [] -> binary_to_list(Command);
        _ ->
            ArgStrs = [binary_to_list(A) || A <- Args, is_binary(A)],
            string:join([binary_to_list(Command) | ArgStrs], " ")
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
            collect_output(Port, Parent, TaskId, <<Acc/binary, Data/binary>>);
        {Port, {exit_status, Status}} ->
            lager:info("Task ~p exited with status ~p", [TaskId, Status]),
            Parent ! {task_complete, TaskId, Status, Acc};
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
    case flurm_protocol_codec:encode_response(MsgType, Body) of
        {ok, ResponseBin} ->
            lager:debug("Sending srun response type=~p size=~p",
                        [MsgType, byte_size(ResponseBin)]),
            case Transport:send(Socket, ResponseBin) of
                ok -> ok;
                {error, Reason} ->
                    lager:warning("Failed to send srun response: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            lager:warning("Failed to encode srun response: ~p", [Reason]),
            {error, Reason}
    end.

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

%% @doc Close the connection gracefully.
-spec close_connection(map()) -> ok.
close_connection(#{socket := Socket, transport := Transport,
                   request_count := Count, start_time := StartTime}) ->
    Duration = erlang:system_time(millisecond) - StartTime,
    lager:info("Closing srun connection after ~p requests, duration: ~p ms",
                [Count, Duration]),
    Transport:close(Socket),
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
