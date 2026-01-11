%%%-------------------------------------------------------------------
%%% @doc FLURM Controller TCP Acceptor
%%%
%%% Ranch protocol handler for incoming SLURM client connections.
%%% Handles TCP connections from SLURM clients (sbatch, squeue, etc.),
%%% parses messages using flurm_protocol_codec, routes to handlers,
%%% and sends responses back.
%%%
%%% Wire format:
%%%   <<Length:32/big, Header:10/binary, Body/binary>>
%%%
%%% Where Length = byte_size(Header) + byte_size(Body) = 10 + body_size
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_acceptor).

-behaviour(ranch_protocol).

-export([start_link/3]).
-export([init/3]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% Minimum data needed to determine message length
-define(LENGTH_PREFIX_SIZE, 4).
%% Receive timeout (30 seconds)
-define(RECV_TIMEOUT, 30000).

%%====================================================================
%% Ranch Protocol Callbacks
%%====================================================================

%% @doc Start a new connection handler process.
%% Ranch calls this for each new connection.
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
            lager:debug("New SLURM client connection accepted"),
            %% Set socket options for low latency
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
                start_time => erlang:system_time(millisecond)
            },
            loop(State);
        {error, Reason} ->
            lager:warning("Ranch handshake failed: ~p", [Reason]),
            {error, Reason}
    end.

%%====================================================================
%% Main Receive Loop
%%====================================================================

%% @doc Main receive loop with buffer handling.
%% Handles partial reads as messages may span multiple TCP packets.
-spec loop(map()) -> ok.
loop(#{socket := Socket, transport := Transport, buffer := Buffer} = State) ->
    case Transport:recv(Socket, 0, ?RECV_TIMEOUT) of
        {ok, Data} ->
            NewBuffer = <<Buffer/binary, Data/binary>>,
            case process_buffer(NewBuffer, State) of
                {continue, UpdatedState} ->
                    loop(UpdatedState);
                {close, _Reason} ->
                    close_connection(State)
            end;
        {error, closed} ->
            lager:debug("Client connection closed normally"),
            ok;
        {error, timeout} ->
            lager:debug("Client connection timeout after ~p ms", [?RECV_TIMEOUT]),
            close_connection(State);
        {error, Reason} ->
            lager:warning("Connection error: ~p", [Reason]),
            close_connection(State)
    end.

%%====================================================================
%% Buffer Processing
%%====================================================================

%% @doc Process accumulated data in the buffer.
%% Handles partial reads and multiple messages in a single buffer.
-spec process_buffer(binary(), map()) -> {continue, map()} | {close, term()}.
process_buffer(Buffer, State) when byte_size(Buffer) < ?LENGTH_PREFIX_SIZE ->
    %% Not enough data for length prefix, wait for more
    {continue, State#{buffer => Buffer}};
process_buffer(<<Length:32/big, Rest/binary>> = Buffer, State)
  when byte_size(Rest) < Length ->
    %% Have length but incomplete message, wait for more
    {continue, State#{buffer => Buffer}};
process_buffer(<<Length:32/big, _Rest/binary>> = _Buffer, _State)
  when Length < ?SLURM_HEADER_SIZE ->
    %% Invalid message length (too small for header)
    lager:warning("Invalid message length: ~p (minimum is ~p)",
                  [Length, ?SLURM_HEADER_SIZE]),
    {close, invalid_message_length};
process_buffer(<<Length:32/big, Rest/binary>>, State)
  when byte_size(Rest) >= Length ->
    %% Complete message available
    <<MessageData:Length/binary, Remaining/binary>> = Rest,
    %% Reconstruct the full message with length prefix for codec
    FullMessage = <<Length:32/big, MessageData/binary>>,
    case handle_message(FullMessage, State) of
        {ok, UpdatedState} ->
            %% Process remaining data (may contain more messages)
            process_buffer(Remaining, UpdatedState#{buffer => <<>>});
        {error, Reason} ->
            lager:warning("Message handling failed: ~p", [Reason]),
            %% Continue processing on non-fatal errors
            process_buffer(Remaining, State#{buffer => <<>>})
    end;
process_buffer(Buffer, State) ->
    %% Fallback: wait for more data
    {continue, State#{buffer => Buffer}}.

%%====================================================================
%% Message Handling
%%====================================================================

%% @doc Handle a complete message.
%% Tries to decode with auth credentials first, falls back to plain decode
%% for test clients that don't send auth.
-spec handle_message(binary(), map()) -> {ok, map()} | {error, term()}.
handle_message(MessageBin, #{socket := Socket, transport := Transport,
                             request_count := Count} = State) ->
    %% Try decode with auth first (for real SLURM clients)
    %% Fall back to plain decode (for test clients)
    DecodeResult = case flurm_protocol_codec:decode_with_extra(MessageBin) of
        {ok, Msg, ExtraInfo, Rest} ->
            {ok, Msg, ExtraInfo, Rest};
        {error, _AuthErr} ->
            %% Try plain decode without auth
            case flurm_protocol_codec:decode(MessageBin) of
                {ok, Msg, Rest} ->
                    {ok, Msg, #{}, Rest};
                PlainErr ->
                    PlainErr
            end
    end,
    case DecodeResult of
        {ok, #slurm_msg{header = Header, body = Body}, ExtraInfo2, _Rest} ->
            lager:debug("Received message type: ~p (~p), from: ~p",
                        [Header#slurm_header.msg_type,
                         flurm_protocol_codec:message_type_name(Header#slurm_header.msg_type),
                         maps:get(hostname, ExtraInfo2, <<"unknown">>)]),
            %% Route to handler
            case flurm_controller_handler:handle(Header, Body) of
                {ok, ResponseType, ResponseBody} ->
                    send_response(Socket, Transport, ResponseType, ResponseBody),
                    {ok, State#{request_count => Count + 1}};
                {error, Reason} ->
                    %% Send error response
                    ErrorResponse = #slurm_rc_response{return_code = -1},
                    send_response(Socket, Transport, ?RESPONSE_SLURM_RC, ErrorResponse),
                    lager:warning("Handler error: ~p", [Reason]),
                    {ok, State#{request_count => Count + 1}}
            end;
        {error, {incomplete_message, _, _}} ->
            %% This shouldn't happen here as we check completeness above
            {error, incomplete_message};
        {error, Reason} ->
            lager:warning("Failed to decode message: ~p", [Reason]),
            %% Send error response for malformed messages
            ErrorResponse = #slurm_rc_response{return_code = -1},
            send_response(Socket, Transport, ?RESPONSE_SLURM_RC, ErrorResponse),
            {error, Reason}
    end.

%% @doc Send a response message back to the client.
%% Uses encode_response which includes auth section at beginning (required by real SLURM clients).
-spec send_response(inet:socket(), module(), non_neg_integer(), term()) -> ok | {error, term()}.
send_response(Socket, Transport, MsgType, Body) ->
    %% Use encode_response - includes auth section (auth/none) at beginning of body
    case flurm_protocol_codec:encode_response(MsgType, Body) of
        {ok, ResponseBin} ->
            case Transport:send(Socket, ResponseBin) of
                ok ->
                    lager:debug("Sent response type: ~p (~p) with ~p bytes",
                                [MsgType, flurm_protocol_codec:message_type_name(MsgType),
                                 byte_size(ResponseBin)]),
                    ok;
                {error, Reason} ->
                    lager:warning("Failed to send response: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            lager:warning("Failed to encode response: ~p", [Reason]),
            {error, Reason}
    end.

%%====================================================================
%% Connection Management
%%====================================================================

%% @doc Close the connection gracefully.
-spec close_connection(map()) -> ok.
close_connection(#{socket := Socket, transport := Transport,
                   request_count := Count, start_time := StartTime}) ->
    Duration = erlang:system_time(millisecond) - StartTime,
    lager:debug("Closing connection after ~p requests, duration: ~p ms",
                [Count, Duration]),
    Transport:close(Socket),
    ok.
