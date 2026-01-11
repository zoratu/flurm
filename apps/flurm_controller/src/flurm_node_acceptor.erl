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
    case flurm_node_manager:register_node(NodeSpec) of
        ok ->
            log(info, "Node ~s registered with node_manager", [Hostname]),
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
    flurm_node_manager:heartbeat(HeartbeatData),

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

    %% Check if job was already cancelled by the controller
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} when Job#job.state =:= cancelled ->
            %% Job was cancelled, ignore the failed report from node
            log(info, "Job ~p: ignoring failed report (already cancelled)", [JobId]),
            ok;
        _ ->
            log(warning, "Job ~p failed: ~s", [JobId, Reason]),

            %% Update job state in job_manager
            flurm_job_manager:update_job(JobId, #{
                state => failed,
                exit_code => -1,
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
            flurm_node_connection_manager:unregister_connection(Hostname);
        error ->
            ok
    end.

send_message(Socket, Transport, Message) ->
    case flurm_protocol:encode(Message) of
        {ok, Binary} ->
            Len = byte_size(Binary),
            Transport:send(Socket, <<Len:32, Binary/binary>>);
        {error, Reason} ->
            {error, Reason}
    end.

%% Logging helpers
log(Level, Fmt, Args) ->
    Msg = io_lib:format(Fmt, Args),
    case Level of
        debug -> ok;
        info -> error_logger:info_msg("[node_acceptor] ~s~n", [Msg]);
        warning -> error_logger:warning_msg("[node_acceptor] ~s~n", [Msg]);
        error -> error_logger:error_msg("[node_acceptor] ~s~n", [Msg])
    end.
