%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Protocol Handler
%%%
%%% Ranch protocol handler for incoming client connections.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_protocol).

-behaviour(ranch_protocol).

-export([start_link/3]).
-export([init/3]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% API
%%====================================================================

start_link(Ref, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Transport, Opts]),
    {ok, Pid}.

init(Ref, Transport, _Opts) ->
    {ok, Socket} = ranch:handshake(Ref),
    lager:debug("New connection accepted"),
    loop(Socket, Transport, <<>>).

%%====================================================================
%% Internal functions
%%====================================================================

loop(Socket, Transport, Buffer) ->
    case Transport:recv(Socket, 0, 30000) of
        {ok, Data} ->
            NewBuffer = <<Buffer/binary, Data/binary>>,
            case process_buffer(NewBuffer) of
                {ok, Message, Rest} ->
                    Response = handle_message(Message),
                    case flurm_protocol:encode(Response) of
                        {ok, ResponseBin} ->
                            Transport:send(Socket, ResponseBin);
                        {error, _} ->
                            ok
                    end,
                    loop(Socket, Transport, Rest);
                {incomplete, Buffer2} ->
                    loop(Socket, Transport, Buffer2)
            end;
        {error, closed} ->
            lager:debug("Connection closed"),
            ok;
        {error, timeout} ->
            lager:debug("Connection timeout"),
            Transport:close(Socket);
        {error, Reason} ->
            lager:warning("Connection error: ~p", [Reason]),
            Transport:close(Socket)
    end.

process_buffer(Buffer) when byte_size(Buffer) < ?HEADER_SIZE ->
    {incomplete, Buffer};
process_buffer(<<_Type:16, PayloadSize:32, Rest/binary>> = Buffer) ->
    case byte_size(Rest) >= PayloadSize of
        true ->
            <<MessageBin:(PayloadSize + ?HEADER_SIZE)/binary, Remaining/binary>> = Buffer,
            case flurm_protocol:decode(MessageBin) of
                {ok, Message} ->
                    {ok, Message, Remaining};
                {error, _} ->
                    {incomplete, Buffer}
            end;
        false ->
            {incomplete, Buffer}
    end.

handle_message(#{type := job_submit, payload := Payload}) ->
    case flurm_job_manager:submit_job(Payload) of
        {ok, JobId} ->
            #{type => ack, payload => #{job_id => JobId}};
        {error, Reason} ->
            #{type => error, payload => #{reason => Reason}}
    end;

handle_message(#{type := job_cancel, payload := #{job_id := JobId}}) ->
    case flurm_job_manager:cancel_job(JobId) of
        ok ->
            #{type => ack, payload => #{status => <<"cancelled">>}};
        {error, Reason} ->
            #{type => error, payload => #{reason => Reason}}
    end;

handle_message(#{type := job_status, payload := #{job_id := JobId}}) ->
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            #{type => ack, payload => job_to_map(Job)};
        {error, not_found} ->
            #{type => error, payload => #{reason => <<"not_found">>}}
    end;

handle_message(#{type := node_register, payload := Payload}) ->
    case flurm_node_manager:register_node(Payload) of
        ok ->
            #{type => ack, payload => #{status => <<"registered">>}};
        {error, Reason} ->
            #{type => error, payload => #{reason => Reason}}
    end;

handle_message(#{type := node_heartbeat, payload := Payload}) ->
    flurm_node_manager:heartbeat(Payload),
    #{type => ack, payload => #{status => <<"ok">>}};

handle_message(#{type := partition_create, payload := Payload}) ->
    case flurm_partition_manager:create_partition(Payload) of
        ok ->
            #{type => ack, payload => #{status => <<"created">>}};
        {error, Reason} ->
            #{type => error, payload => #{reason => Reason}}
    end;

handle_message(#{type := Type, payload := _Payload}) ->
    lager:warning("Unknown message type: ~p", [Type]),
    #{type => error, payload => #{reason => <<"unknown_message_type">>}}.

job_to_map(Job) ->
    #{
        job_id => element(2, Job),     % #job.id
        name => element(3, Job),       % #job.name
        state => element(6, Job),      % #job.state
        partition => element(5, Job)   % #job.partition
    }.
