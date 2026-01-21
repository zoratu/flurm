%%%-------------------------------------------------------------------
%%% @doc FLURM Node Connection Acceptor
%%%
%%% Ranch protocol handler for accepting connections from node daemons.
%%% Uses the internal FLURM protocol (not SLURM binary protocol).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_acceptor).

-behaviour(ranch_protocol).

-include_lib("flurm_core/include/flurm_core.hrl").

-export([start_link/3]).
-export([init/3]).

-ifdef(TEST).
-export([
    build_node_spec/1,
    build_heartbeat_data/1,
    exit_code_to_state/1,
    failure_reason_to_state/1,
    build_register_ack_payload/1,
    extract_message_from_buffer/1,
    frame_message/1
]).
-endif.

%%====================================================================
%% API
%%====================================================================

start_link(Ref, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Transport, Opts]),
    {ok, Pid}.

init(Ref, Transport, _Opts) ->
    {ok, Socket} = ranch:handshake(Ref),
    Transport:setopts(Socket, [{active, once}, {packet, raw}, binary]),
    loop(Socket, Transport, <<>>).

%%====================================================================
%% Internal functions
%%====================================================================

loop(Socket, Transport, Buffer) ->
    receive
        {tcp, Socket, Data} ->
            NewBuffer = <<Buffer/binary, Data/binary>>,
            case process_messages(Socket, Transport, NewBuffer) of
                {ok, RemainingBuffer} ->
                    Transport:setopts(Socket, [{active, once}]),
                    loop(Socket, Transport, RemainingBuffer);
                {error, Reason} ->
                    log(warning, "Connection error: ~p", [Reason]),
                    Transport:close(Socket)
            end;
        {tcp_closed, Socket} ->
            handle_disconnect(Socket),
            ok;
        {tcp_error, Socket, Reason} ->
            log(warning, "TCP error: ~p", [Reason]),
            handle_disconnect(Socket),
            Transport:close(Socket);
        {send, Message} ->
            case send_message(Socket, Transport, Message) of
                ok -> loop(Socket, Transport, Buffer);
                {error, _} -> Transport:close(Socket)
            end;
        stop ->
            Transport:close(Socket)
    end.

%% Process complete messages from buffer
process_messages(Socket, Transport, <<Len:32, Data/binary>>) when byte_size(Data) >= Len ->
    <<MsgData:Len/binary, Rest/binary>> = Data,
    case flurm_protocol:decode(MsgData) of
        {ok, Message} ->
            case handle_message(Socket, Transport, Message) of
                ok ->
                    process_messages(Socket, Transport, Rest);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            log(warning, "Failed to decode message: ~p", [Reason]),
            process_messages(Socket, Transport, Rest)
    end;
process_messages(_Socket, _Transport, Buffer) ->
    {ok, Buffer}.

%% Handle incoming messages from node daemons
handle_message(Socket, Transport, #{type := node_register, payload := Payload}) ->
    Hostname = maps:get(<<"hostname">>, Payload),
    log(info, "Node ~s registering", [Hostname]),

    %% Register with node manager
    NodeSpec = #{
        hostname => Hostname,
        cpus => maps:get(<<"cpus">>, Payload, 1),
        memory_mb => maps:get(<<"memory_mb">>, Payload, 1024),
        state => idle,
        partitions => [<<"default">>]
    },
    log(info, "Node ~s calling node_manager:register_node", [Hostname]),
    case flurm_node_manager_server:register_node(NodeSpec) of
        ok ->
            log(info, "Node ~s registered with node_manager", [Hostname]),
            %% Also register with node_registry for scheduler access
            RegistrySpec = #{
                hostname => Hostname,
                cpus => maps:get(cpus, NodeSpec, 1),
                memory_mb => maps:get(memory_mb, NodeSpec, 1024),
                state => up,
                partitions => maps:get(partitions, NodeSpec, [<<"default">>])
            },
            case flurm_node_registry:register_node_direct(RegistrySpec) of
                ok ->
                    log(info, "Node ~s registered with node_registry", [Hostname]);
                {error, already_registered} ->
                    log(debug, "Node ~s already in registry", [Hostname]);
                RegistryErr ->
                    log(warning, "Node ~s registry error: ~p", [Hostname, RegistryErr])
            end,
            %% Register socket with connection manager
            case flurm_node_connection_manager:register_connection(Hostname, self()) of
                ok ->
                    log(info, "Node ~s registered with connection_manager", [Hostname]),
                    %% Send acknowledgment
                    AckPayload = #{
                        <<"node_id">> => Hostname,
                        <<"status">> => <<"accepted">>
                    },
                    Result = send_message(Socket, Transport, #{type => node_register_ack, payload => AckPayload}),
                    log(info, "Node ~s ack send result: ~p", [Hostname, Result]),
                    Result;
                ConnErr ->
                    log(error, "Node ~s connection_manager error: ~p", [Hostname, ConnErr]),
                    ConnErr
            end;
        {error, Reason} ->
            log(error, "Failed to register node ~s: ~p", [Hostname, Reason]),
            send_message(Socket, Transport, #{type => error, payload => #{
                <<"reason">> => iolist_to_binary(io_lib:format("~p", [Reason]))
            }})
    end;

handle_message(Socket, Transport, #{type := node_heartbeat, payload := Payload}) ->
    Hostname = maps:get(<<"hostname">>, Payload),

    %% Update node manager with heartbeat data
    HeartbeatData = #{
        hostname => Hostname,
        load_avg => maps:get(<<"load_avg">>, Payload, 0.0),
        free_memory_mb => maps:get(<<"free_memory_mb">>, Payload, 0),
        running_jobs => maps:get(<<"running_jobs">>, Payload, [])
    },
    flurm_node_manager_server:heartbeat(HeartbeatData),

    %% Send ack
    send_message(Socket, Transport, #{type => node_heartbeat_ack, payload => #{}});

handle_message(_Socket, _Transport, #{type := job_complete, payload := Payload}) ->
    JobId = maps:get(<<"job_id">>, Payload),
    ExitCode = maps:get(<<"exit_code">>, Payload, 0),

    log(info, "Job ~p completed with exit code ~p", [JobId, ExitCode]),

    %% Update job state in job_manager
    NewState = case ExitCode of
        0 -> completed;
        _ -> failed
    end,
    flurm_job_manager:update_job(JobId, #{
        state => NewState,
        exit_code => ExitCode,
        end_time => erlang:system_time(second)
    }),

    %% Notify scheduler to release resources and schedule more jobs
    case ExitCode of
        0 -> flurm_scheduler:job_completed(JobId);
        _ -> flurm_scheduler:job_failed(JobId)
    end,
    ok;

handle_message(_Socket, _Transport, #{type := job_failed, payload := Payload}) ->
    JobId = maps:get(<<"job_id">>, Payload),
    Reason = maps:get(<<"reason">>, Payload, <<"unknown">>),
    ExitCode = maps:get(<<"exit_code">>, Payload, -1),

    %% Check if job was already cancelled by the controller
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} when Job#job.state =:= cancelled ->
            %% Job was cancelled, ignore the failed report from node
            log(info, "Job ~p: ignoring failed report (already cancelled)", [JobId]),
            ok;
        _ ->
            %% Determine job state based on reason
            State = case Reason of
                <<"timeout">> ->
                    log(warning, "Job ~p timed out", [JobId]),
                    timeout;
                _ ->
                    log(warning, "Job ~p failed: ~s", [JobId, Reason]),
                    failed
            end,

            %% Update job state in job_manager
            flurm_job_manager:update_job(JobId, #{
                state => State,
                exit_code => ExitCode,
                end_time => erlang:system_time(second)
            }),

            %% Notify scheduler to release resources
            flurm_scheduler:job_failed(JobId)
    end,
    ok;

handle_message(_Socket, _Transport, #{type := Type, payload := _Payload}) ->
    log(debug, "Received unhandled message type: ~p", [Type]),
    ok.

handle_disconnect(Socket) ->
    %% Find and unregister this connection
    case flurm_node_connection_manager:find_by_socket(Socket) of
        {ok, Hostname} ->
            log(info, "Node ~s disconnected", [Hostname]),
            %% Mark node as down
            flurm_node_manager_server:update_node(Hostname, #{state => down}),
            %% Fail all jobs running on this node
            fail_jobs_on_node(Hostname),
            %% Unregister the connection
            flurm_node_connection_manager:unregister_connection(Hostname);
        error ->
            ok
    end.

%% Fail all jobs that were running on a node that went down
fail_jobs_on_node(Hostname) ->
    %% Find all jobs that were allocated to this node and are still running
    AllJobs = flurm_job_manager:list_jobs(),
    RunningOnNode = lists:filter(fun(Job) ->
        State = element(6, Job),  % #job.state
        AllocatedNodes = element(16, Job),  % #job.allocated_nodes
        (State =:= running orelse State =:= configuring) andalso
        lists:member(Hostname, AllocatedNodes)
    end, AllJobs),

    log(info, "Node ~s disconnect: found ~p jobs still running on node",
        [Hostname, length(RunningOnNode)]),

    lists:foreach(fun(Job) ->
        JobId = element(2, Job),  % #job.id
        log(warning, "Job ~p failed due to node ~s failure", [JobId, Hostname]),
        flurm_job_manager:update_job(JobId, #{
            state => node_fail,
            exit_code => -1,
            end_time => erlang:system_time(second)
        }),
        flurm_scheduler:job_failed(JobId)
    end, RunningOnNode).

send_message(Socket, Transport, Message) ->
    case flurm_protocol:encode(Message) of
        {ok, Binary} ->
            Len = byte_size(Binary),
            Transport:send(Socket, <<Len:32, Binary/binary>>);
        {error, Reason} ->
            {error, Reason}
    end.

-ifdef(TEST).
%%====================================================================
%% Pure Helper Functions (exported for testing)
%%====================================================================

%% @doc Build a node specification map from registration payload.
%% Extracts hostname, cpus, memory with defaults.
-spec build_node_spec(map()) -> map().
build_node_spec(Payload) ->
    #{
        hostname => maps:get(<<"hostname">>, Payload),
        cpus => maps:get(<<"cpus">>, Payload, 1),
        memory_mb => maps:get(<<"memory_mb">>, Payload, 1024),
        state => idle,
        partitions => [<<"default">>]
    }.

%% @doc Build heartbeat data map from payload.
%% Extracts hostname, load_avg, free_memory_mb, running_jobs with defaults.
-spec build_heartbeat_data(map()) -> map().
build_heartbeat_data(Payload) ->
    #{
        hostname => maps:get(<<"hostname">>, Payload),
        load_avg => maps:get(<<"load_avg">>, Payload, 0.0),
        free_memory_mb => maps:get(<<"free_memory_mb">>, Payload, 0),
        running_jobs => maps:get(<<"running_jobs">>, Payload, [])
    }.

%% @doc Convert exit code to job state.
%% Exit code 0 means completed, anything else means failed.
-spec exit_code_to_state(integer()) -> completed | failed.
exit_code_to_state(0) -> completed;
exit_code_to_state(_) -> failed.

%% @doc Convert failure reason to job state.
%% Timeout reason results in timeout state, others result in failed.
-spec failure_reason_to_state(binary()) -> timeout | failed.
failure_reason_to_state(<<"timeout">>) -> timeout;
failure_reason_to_state(_) -> failed.

%% @doc Build a node registration acknowledgment payload.
-spec build_register_ack_payload(binary()) -> map().
build_register_ack_payload(Hostname) ->
    #{
        <<"node_id">> => Hostname,
        <<"status">> => <<"accepted">>
    }.

%% @doc Extract a complete message from a buffer if available.
%% Returns {ok, MessageData, Remaining} or {incomplete, Buffer}.
-spec extract_message_from_buffer(binary()) ->
    {ok, binary(), binary()} | {incomplete, binary()}.
extract_message_from_buffer(<<Len:32, Data/binary>> = Buffer) when byte_size(Data) >= Len ->
    <<MsgData:Len/binary, Rest/binary>> = Data,
    {ok, MsgData, Rest};
extract_message_from_buffer(Buffer) ->
    {incomplete, Buffer}.

%% @doc Frame a message with length prefix for sending.
-spec frame_message(binary()) -> binary().
frame_message(Binary) ->
    Len = byte_size(Binary),
    <<Len:32, Binary/binary>>.
-endif.

%% Logging helpers
log(Level, Fmt, Args) ->
    Msg = io_lib:format(Fmt, Args),
    case Level of
        debug -> ok;
        info -> error_logger:info_msg("[node_acceptor] ~s~n", [Msg]);
        warning -> error_logger:warning_msg("[node_acceptor] ~s~n", [Msg]);
        error -> error_logger:error_msg("[node_acceptor] ~s~n", [Msg])
    end.
