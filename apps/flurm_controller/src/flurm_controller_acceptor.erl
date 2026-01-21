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

-ifdef(TEST).
-export([
    binary_to_hex/1,
    get_hex_prefix/1,
    create_initial_state/3,
    check_buffer_status/1,
    extract_message/1,
    calculate_duration/2
]).
-endif.

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
            %% Log incoming data hex prefix for debugging
            InHex = case byte_size(Data) of
                N when N >= 24 ->
                    <<First24:24/binary, _/binary>> = Data,
                    binary_to_hex(First24);
                _ ->
                    binary_to_hex(Data)
            end,
            lager:info("Received ~p bytes, hex=~s", [byte_size(Data), InHex]),
            %% Log full header info for debugging (skip 4-byte outer length)
            case byte_size(Data) >= 14 of
                true ->
                    <<_OL:32/big, V:16/big, F:16/big, MT:16/big, BL:32/big, _/binary>> = Data,
                    lager:info("Header: version=~.16B flags=~.16B msg_type=~p body_len=~p",
                              [V, F, MT, BL]);
                false -> ok
            end,
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
                {ok, ResponseType, ResponseBody, CallbackInfo} ->
                    %% Handler returned callback info for srun allocation
                    %% IMPORTANT: Connect to callback BEFORE sending response
                    %% srun may close its listener after receiving the allocation response
                    JobId = maps:get(job_id, CallbackInfo, 0),
                    Port = maps:get(port, CallbackInfo, 0),
                    lager:info("srun allocation for job ~p, callback port ~p", [JobId, Port]),

                    %% Try callback connection FIRST (before response)
                    CallbackResult = case Port of
                        P when P > 0 ->
                            %% Get client IP from socket
                            case inet:peername(Socket) of
                                {ok, {IpAddr, _}} ->
                                    Host = format_ip(IpAddr),
                                    lager:info("Attempting callback connection to ~s:~p BEFORE response", [Host, P]),
                                    try_callback_connection(Host, P, JobId);
                                _ ->
                                    lager:warning("Could not get peer address for callback"),
                                    {error, no_peer_address}
                            end;
                        _ ->
                            lager:warning("No callback port specified for job ~p", [JobId]),
                            {error, no_port}
                    end,
                    lager:info("Callback result: ~p, now sending allocation response", [CallbackResult]),

                    %% Send the allocation response after callback attempt
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
%% All responses use encode_response with MUNGE auth section.
-spec send_response(inet:socket(), module(), non_neg_integer(), term()) -> ok | {error, term()}.
send_response(Socket, Transport, MsgType, Body) ->
    case flurm_protocol_codec:encode_response(MsgType, Body) of
        {ok, ResponseBin} ->
            %% Log hex of first 24 bytes for debugging
            HexPrefix = case byte_size(ResponseBin) of
                N when N >= 24 ->
                    <<First24:24/binary, _/binary>> = ResponseBin,
                    binary_to_hex(First24);
                _ ->
                    binary_to_hex(ResponseBin)
            end,
            lager:info("Sending response type=~p (~s) size=~p hex=~s",
                       [MsgType, flurm_protocol_codec:message_type_name(MsgType),
                        byte_size(ResponseBin), HexPrefix]),
            case Transport:send(Socket, ResponseBin) of
                ok ->
                    lager:info("Response sent successfully"),
                    ok;
                {error, Reason} ->
                    lager:warning("Failed to send response: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            lager:warning("Failed to encode response: ~p", [Reason]),
            {error, Reason}
    end.

%% Helper to convert binary to hex string
binary_to_hex(Bin) ->
    list_to_binary([[io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(Bin)]]).

-ifdef(TEST).
%%====================================================================
%% Pure Helper Functions (exported for testing)
%%====================================================================

%% @doc Get hex prefix of data (first 24 bytes or entire data if smaller).
-spec get_hex_prefix(binary()) -> binary().
get_hex_prefix(Data) when byte_size(Data) >= 24 ->
    <<First24:24/binary, _/binary>> = Data,
    binary_to_hex(First24);
get_hex_prefix(Data) ->
    binary_to_hex(Data).

%% @doc Create initial connection state.
-spec create_initial_state(inet:socket(), module(), map()) -> map().
create_initial_state(Socket, Transport, Opts) ->
    #{
        socket => Socket,
        transport => Transport,
        opts => Opts,
        buffer => <<>>,
        request_count => 0,
        start_time => erlang:system_time(millisecond)
    }.

%% @doc Check the status of a buffer for message completeness.
%% Returns: need_more_data | invalid_length | {complete, Length}
-spec check_buffer_status(binary()) -> need_more_data | invalid_length | {complete, non_neg_integer()}.
check_buffer_status(Buffer) when byte_size(Buffer) < ?LENGTH_PREFIX_SIZE ->
    need_more_data;
check_buffer_status(<<Length:32/big, _Rest/binary>>) when Length < ?SLURM_HEADER_SIZE ->
    invalid_length;
check_buffer_status(<<Length:32/big, Rest/binary>>) when byte_size(Rest) < Length ->
    need_more_data;
check_buffer_status(<<Length:32/big, _Rest/binary>>) ->
    {complete, Length}.

%% @doc Extract a complete message from buffer.
%% Returns {ok, FullMessage, Remaining} or {incomplete, Buffer}.
-spec extract_message(binary()) -> {ok, binary(), binary()} | {incomplete, binary()}.
extract_message(<<Length:32/big, Rest/binary>> = _Buffer) when byte_size(Rest) >= Length ->
    <<MessageData:Length/binary, Remaining/binary>> = Rest,
    FullMessage = <<Length:32/big, MessageData/binary>>,
    {ok, FullMessage, Remaining};
extract_message(Buffer) ->
    {incomplete, Buffer}.

%% @doc Calculate connection duration from start time.
-spec calculate_duration(integer(), integer()) -> integer().
calculate_duration(EndTime, StartTime) ->
    EndTime - StartTime.
-endif.

%%====================================================================
%% Connection Management
%%====================================================================

%% @doc Format an IP address tuple as a string.
-spec format_ip(inet:ip_address()) -> string().
format_ip({A, B, C, D}) ->
    lists:flatten(io_lib:format("~p.~p.~p.~p", [A, B, C, D]));
format_ip({A, B, C, D, E, F, G, H}) ->
    lists:flatten(io_lib:format("~.16B:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B",
                                [A, B, C, D, E, F, G, H])).

%% @doc Try to connect to srun's callback port and send job ready message.
%% This is attempted after sending the allocation response.
%% Uses retry logic since srun may take time to be ready for callback.
-spec try_callback_connection(string(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
try_callback_connection(Host, Port, JobId) ->
    try_callback_connection(Host, Port, JobId, 5).  %% 5 retries

try_callback_connection(_Host, _Port, _JobId, 0) ->
    lager:warning("Callback connection failed after all retries"),
    {error, max_retries};
try_callback_connection(Host, Port, JobId, Retries) ->
    lager:info("Attempting callback to ~s:~p for job ~p (retries left: ~p)", [Host, Port, JobId, Retries]),
    Options = [binary, {packet, raw}, {active, false}, {nodelay, true}],
    case gen_tcp:connect(Host, Port, Options, 1000) of
        {error, econnrefused} when Retries > 1 ->
            lager:debug("Connection refused, retrying in 200ms"),
            timer:sleep(200),
            try_callback_connection(Host, Port, JobId, Retries - 1);
        {ok, Socket} ->
            lager:info("Connected to srun callback at ~s:~p", [Host, Port]),
            %% Send SRUN_PING (7001) to acknowledge the connection
            Ping = #srun_ping{job_id = JobId, step_id = 0},
            case flurm_protocol_codec:encode_response(?SRUN_PING, Ping) of
                {ok, MessageBin} ->
                    case gen_tcp:send(Socket, MessageBin) of
                        ok ->
                            lager:info("Sent SRUN_PING to callback for job ~p", [JobId]),
                            %% Wait for srun's response and keep connection open
                            case gen_tcp:recv(Socket, 0, 2000) of
                                {ok, RespData} ->
                                    lager:info("Got callback response: ~p bytes", [byte_size(RespData)]);
                                {error, timeout} ->
                                    lager:debug("No immediate callback response (ok)");
                                {error, RecvErr} ->
                                    lager:warning("Callback recv error: ~p", [RecvErr])
                            end,
                            %% Close the callback socket after processing
                            %% srun should have received our response by now
                            gen_tcp:close(Socket),
                            ok;
                        {error, Reason} ->
                            lager:warning("Failed to send to callback: ~p", [Reason]),
                            gen_tcp:close(Socket),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    lager:warning("Failed to encode callback message: ~p", [Reason]),
                    gen_tcp:close(Socket),
                    {error, Reason}
            end;
        {error, Reason} ->
            lager:warning("Failed to connect to srun callback at ~s:~p: ~p",
                          [Host, Port, Reason]),
            {error, Reason}
    end.

%% @doc Close the connection gracefully.
-spec close_connection(map()) -> ok.
close_connection(#{socket := Socket, transport := Transport,
                   request_count := Count, start_time := StartTime}) ->
    Duration = erlang:system_time(millisecond) - StartTime,
    lager:debug("Closing connection after ~p requests, duration: ~p ms",
                [Count, Duration]),
    Transport:close(Socket),
    ok.
