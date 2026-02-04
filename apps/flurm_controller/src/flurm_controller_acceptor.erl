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

%% Temporarily suppress warnings for callback functions during debugging
-compile([{nowarn_unused_function, [{format_ip, 1},
                                    {try_callback_connection, 3},
                                    {try_callback_connection, 4},
                                    {try_callback_silent, 3},
                                    {try_callback_silent_sync, 3},
                                    {callback_socket_loop, 2},
                                    {callback_handler_loop, 2},
                                    {spawn_callback_handler, 2},
                                    {parse_callback_response, 2},
                                    {send_srun_ping_message, 2},
                                    {get_peer_host, 1}]}]).

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
    calculate_duration/2,
    get_munge_auth_mode/0,
    verify_munge_credential/1
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
            %% Verify MUNGE credential if present
            case verify_munge_credential(ExtraInfo2) of
                {ok, AuthResult} ->
                    lager:debug("Received message type: ~p (~p), from: ~p, auth: ~p",
                                [Header#slurm_header.msg_type,
                                 flurm_protocol_codec:message_type_name(Header#slurm_header.msg_type),
                                 maps:get(hostname, ExtraInfo2, <<"unknown">>),
                                 AuthResult]),
                    %% Route to handler
            case flurm_controller_handler:handle(Header, Body) of
                {ok, ResponseType, ResponseBody} ->
                    send_response(Socket, Transport, ResponseType, ResponseBody),
                    {ok, State#{request_count => Count + 1}};
                {ok, ResponseType, ResponseBody, CallbackInfo} ->
                    %% Handler returned callback info for srun allocation
                    JobId = maps:get(job_id, CallbackInfo, 0),
                    Port = maps:get(port, CallbackInfo, 0),
                    lager:info("srun allocation for job ~p, callback port ~p", [JobId, Port]),

                    %% Send the allocation response FIRST before callback
                    send_response(Socket, Transport, ResponseType, ResponseBody),

                    %% Establish callback connection to srun and send SRUN_PING
                    %% This notifies srun that controller is ready for notifications
                    case Port > 0 of
                        true ->
                            ClientHost = case inet:peername(Socket) of
                                {ok, {IP, _}} -> inet:ntoa(IP);
                                _ -> "127.0.0.1"
                            end,
                            lager:info("Initiating callback to ~s:~p for job ~p", [ClientHost, Port, JobId]),
                            %% Spawn callback in separate process to not block main handler
                            spawn(fun() ->
                                try_callback_silent_sync(ClientHost, Port, JobId)
                            end);
                        false ->
                            lager:info("No callback port specified, skipping callback")
                    end,
                    {ok, State#{request_count => Count + 1}};
                {error, Reason} ->
                    %% Send error response
                    ErrorResponse = #slurm_rc_response{return_code = -1},
                    send_response(Socket, Transport, ?RESPONSE_SLURM_RC, ErrorResponse),
                    lager:warning("Handler error: ~p", [Reason]),
                    {ok, State#{request_count => Count + 1}}
            end;
                {error, AuthReason} ->
                    %% MUNGE credential verification failed
                    lager:warning("MUNGE authentication failed: ~p", [AuthReason]),
                    ErrorResponse = #slurm_rc_response{return_code = -1},
                    send_response(Socket, Transport, ?RESPONSE_SLURM_RC, ErrorResponse),
                    {error, {auth_failed, AuthReason}}
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
%% All responses include MUNGE auth section (required by SLURM clients).
-spec send_response(inet:socket(), module(), non_neg_integer(), term()) -> ok | {error, term()}.
send_response(Socket, Transport, MsgType, Body) ->
    EncodeResult = flurm_protocol_codec:encode_response(MsgType, Body),
    case EncodeResult of
        {ok, ResponseBin} ->
            lager:info("Sending response type=~p (~s) size=~p hex=~s",
                       [MsgType, flurm_protocol_codec:message_type_name(MsgType),
                        byte_size(ResponseBin), binary_to_hex(ResponseBin)]),
            %% DEBUG: Parse our own message to verify format
            <<OuterLen:32/big, Ver:16/big, Flags:16/big, MT:16/big, BodyLen:32/big, _Rest/binary>> = ResponseBin,
            lager:info("DEBUG VERIFY: outer_len=~p ver=~.16B flags=~p msg_type=~p body_len=~p",
                       [OuterLen, Ver, Flags, MT, BodyLen]),
            lager:info("DEBUG VERIFY: header_size=16, check: ~p + 16 = ~p <= ~p ? ~p",
                       [BodyLen, BodyLen + 16, OuterLen, (BodyLen + 16) =< OuterLen]),
            %% DEBUG: Log exactly what's being sent
            lager:info("SOCKET SEND: ~p bytes starting with ~s",
                       [byte_size(ResponseBin), binary_to_hex(binary:part(ResponseBin, 0, min(byte_size(ResponseBin), 40)))]),
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

%% @doc Verify MUNGE credential from auth info.
%% Behavior is controlled by application environment:
%%   - munge_auth_enabled: true | false | strict
%%     - false: Skip all MUNGE verification (default for development)
%%     - true: Verify MUNGE but allow failures (graceful degradation)
%%     - strict: Require valid MUNGE credentials (production mode)
%%
%% Returns {ok, verified} if credential is valid or MUNGE verification is not available,
%% returns {ok, skipped} if no MUNGE credential present (test clients),
%% returns {error, Reason} if credential verification fails and strict mode is enabled.
-spec verify_munge_credential(map()) -> {ok, atom()} | {ok, {atom(), term()}} | {error, term()}.
verify_munge_credential(AuthInfo) ->
    MungeMode = get_munge_auth_mode(),
    case MungeMode of
        false ->
            %% MUNGE verification disabled
            {ok, disabled};
        _ ->
            do_verify_munge_credential(AuthInfo, MungeMode)
    end.

%% @doc Get MUNGE authentication mode from application config.
%% Returns false (disabled), true (permissive), or strict.
-spec get_munge_auth_mode() -> false | true | strict.
get_munge_auth_mode() ->
    application:get_env(flurm_controller, munge_auth_enabled, true).

%% @doc Internal function to verify MUNGE credential.
-spec do_verify_munge_credential(map(), true | strict) -> {ok, atom()} | {ok, {atom(), term()}} | {error, term()}.
do_verify_munge_credential(AuthInfo, Mode) ->
    case maps:get(auth_type, AuthInfo, unknown) of
        munge ->
            %% MUNGE credential present - verify it if MUNGE is available
            case maps:get(credential, AuthInfo, undefined) of
                undefined ->
                    %% No credential in auth info, skip verification
                    case Mode of
                        strict -> {error, no_credential};
                        _ -> {ok, no_credential}
                    end;
                Credential when is_binary(Credential), byte_size(Credential) > 0 ->
                    verify_munge_credential_binary(Credential, Mode);
                _ ->
                    case Mode of
                        strict -> {error, empty_credential};
                        _ -> {ok, empty_credential}
                    end
            end;
        _ ->
            %% Non-MUNGE auth or test client without auth
            case Mode of
                strict -> {error, no_munge_auth};
                _ -> {ok, skipped}
            end
    end.

%% @doc Verify a binary MUNGE credential.
-spec verify_munge_credential_binary(binary(), true | strict) -> {ok, atom()} | {ok, {atom(), term()}} | {error, term()}.
verify_munge_credential_binary(Credential, Mode) ->
    case flurm_munge:is_available() of
        true ->
            %% MUNGE available - verify the credential
            case flurm_munge:verify(Credential) of
                ok ->
                    lager:debug("MUNGE credential verified successfully"),
                    {ok, verified};
                {error, Reason} ->
                    lager:warning("MUNGE credential verification failed: ~p", [Reason]),
                    case Mode of
                        strict ->
                            %% Strict mode - reject invalid credentials
                            {error, Reason};
                        _ ->
                            %% Permissive mode - log but allow connection (graceful degradation)
                            {ok, {verification_failed, Reason}}
                    end
            end;
        false ->
            %% MUNGE not available
            case Mode of
                strict ->
                    lager:warning("MUNGE not available but strict mode enabled"),
                    {error, munge_unavailable};
                _ ->
                    lager:debug("MUNGE not available, skipping credential verification"),
                    {ok, munge_unavailable}
            end
    end.

%% @doc Get the peer host from a socket (for callback connections)
-spec get_peer_host(inet:socket()) -> {ok, string()} | {error, term()}.
get_peer_host(Socket) ->
    case inet:peername(Socket) of
        {ok, {IP, _Port}} ->
            case IP of
                {A, B, C, D} ->
                    {ok, inet:ntoa({A, B, C, D})};
                _ ->
                    {ok, inet:ntoa(IP)}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

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

%% @doc Try callback connection with SRUN_PING message.
%% Connects, sends SRUN_PING, waits for response, then keeps socket open.
%% Returns {ok, Pid} where Pid is the process maintaining the callback socket.
-spec try_callback_silent_sync(string(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
try_callback_silent_sync(Host, Port, JobId) ->
    lager:info("Callback sync to ~s:~p for job ~p", [Host, Port, JobId]),
    Options = [binary, {packet, raw}, {active, false}, {nodelay, true}],
    case gen_tcp:connect(Host, Port, Options, 1000) of
        {ok, Socket} ->
            lager:info("Callback connected to ~s:~p for job ~p", [Host, Port, JobId]),
            %% Send SRUN_PING - srun expects a valid SLURM message after accepting
            %% This prevents the "Socket timed out on send/recv" error
            case send_srun_ping_message(Socket, JobId) of
                ok ->
                    lager:info("SRUN_PING sent to callback for job ~p", [JobId]),
                    %% Wait for srun's response
                    case gen_tcp:recv(Socket, 0, 5000) of
                        {ok, ResponseData} ->
                            lager:info("Callback ping response: ~p bytes", [byte_size(ResponseData)]),
                            parse_callback_response(ResponseData, JobId);
                        {error, timeout} ->
                            lager:warning("Callback ping response timeout for job ~p", [JobId]);
                        {error, RecvErr} ->
                            lager:warning("Callback ping recv error: ~p", [RecvErr])
                    end,
                    %% Keep socket alive
                    spawn_callback_handler(Socket, JobId),
                    ok;
                {error, PingErr} ->
                    lager:warning("Failed to send SRUN_PING: ~p", [PingErr]),
                    gen_tcp:close(Socket),
                    {error, PingErr}
            end;
        {error, econnrefused} ->
            lager:debug("Callback connection refused to ~s:~p", [Host, Port]),
            {error, econnrefused};
        {error, Reason} ->
            lager:warning("Callback connection failed to ~s:~p: ~p", [Host, Port, Reason]),
            {error, Reason}
    end.

%% Spawn a dedicated handler process for the callback socket
spawn_callback_handler(Socket, JobId) ->
    %% Spawn a linked process to handle the socket
    Pid = spawn(fun() ->
        %% This process now owns the socket (after controlling_process)
        receive
            {take_socket, S} ->
                lager:info("Callback handler ~p received socket for job ~p", [self(), JobId]),
                inet:setopts(S, [{active, true}]),
                callback_handler_loop(S, JobId)
        after 5000 ->
            lager:warning("Callback handler timeout waiting for socket for job ~p", [JobId])
        end
    end),
    %% Transfer socket ownership
    case gen_tcp:controlling_process(Socket, Pid) of
        ok ->
            lager:info("Transferred socket ownership to ~p for job ~p", [Pid, JobId]),
            Pid ! {take_socket, Socket};
        {error, Reason} ->
            lager:warning("Failed to transfer socket ownership: ~p", [Reason]),
            exit(Pid, socket_transfer_failed),
            gen_tcp:close(Socket)
    end.

%% Long-running handler loop for callback socket
callback_handler_loop(Socket, JobId) ->
    receive
        {tcp, Socket, Data} ->
            Hex = binary_to_hex(Data),
            lager:info("Callback handler received from job ~p: ~p bytes, hex=~s",
                       [JobId, byte_size(Data), Hex]),
            callback_handler_loop(Socket, JobId);
        {tcp_closed, Socket} ->
            lager:info("Callback handler: socket closed for job ~p", [JobId]);
        {tcp_error, Socket, Reason} ->
            lager:warning("Callback handler: socket error for job ~p: ~p", [JobId, Reason])
    after 300000 ->  %% 5 minute timeout
        lager:debug("Callback handler timeout for job ~p", [JobId]),
        gen_tcp:close(Socket)
    end.

%% Parse and log the callback response from srun
parse_callback_response(Data, JobId) when byte_size(Data) >= 14 ->
    <<OL:32/big, V:16/big, F:16/big, MT:16/big, BL:32/big, Rest/binary>> = Data,
    lager:info("Callback response for job ~p: outer_len=~p ver=~.16B flags=~p msg_type=~p body_len=~p",
               [JobId, OL, V, F, MT, BL]),
    %% Try to extract the return code if this is RESPONSE_SLURM_RC (8001)
    case {MT, BL, Rest} of
        {8001, _, _} when byte_size(Rest) >= BL ->
            %% Skip auth section to get body
            lager:info("Callback response is RESPONSE_SLURM_RC (~p)", [MT]);
        _ ->
            ok
    end;
parse_callback_response(Data, JobId) ->
    lager:info("Callback response for job ~p: ~p bytes (too small to parse)",
               [JobId, byte_size(Data)]).

%% @doc Try callback connection silently - just connect, don't send anything.
%% This establishes the message socket that srun's message thread expects.
-spec try_callback_silent(string(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
try_callback_silent(Host, Port, JobId) ->
    lager:info("Silent callback attempt to ~s:~p for job ~p", [Host, Port, JobId]),
    Options = [binary, {packet, raw}, {active, true}, {nodelay, true}],
    case gen_tcp:connect(Host, Port, Options, 2000) of
        {ok, Socket} ->
            lager:info("Silent callback connected to ~s:~p for job ~p", [Host, Port, JobId]),
            %% Just keep the socket open - don't send anything
            %% The message thread expects the connection to stay open
            callback_handler_loop(Socket, JobId);
        {error, econnrefused} ->
            lager:debug("Callback connection refused to ~s:~p", [Host, Port]),
            {error, econnrefused};
        {error, Reason} ->
            lager:warning("Callback connection failed to ~s:~p: ~p", [Host, Port, Reason]),
            {error, Reason}
    end.

%% @doc Try to connect to srun's callback port for out-of-band notifications.
%% This is attempted BEFORE sending the allocation response.
%% The callback socket is used for SRUN_PING, SRUN_JOB_COMPLETE, etc.
%% srun will connect DIRECTLY to slurmd (node daemon) for task execution.
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
            %% Send SRUN_PING immediately before srun times out the connection
            %% srun expects a valid SLURM message after accepting connection
            case send_srun_ping_message(Socket, JobId) of
                ok ->
                    lager:info("SRUN_PING sent successfully, setting up handler");
                {error, PingErr} ->
                    lager:warning("Failed to send SRUN_PING: ~p", [PingErr])
            end,
            %% Set active mode so we can receive messages from srun
            inet:setopts(Socket, [{active, true}]),
            %% Spawn handler to keep connection open and handle messages
            HandlerPid = spawn(fun() ->
                lager:info("Callback handler started for job ~p (after PING)", [JobId]),
                callback_socket_loop(Socket, JobId)
            end),
            %% Transfer socket ownership to handler process
            gen_tcp:controlling_process(Socket, HandlerPid),
            ok;
        {error, ConnectErr} ->
            lager:warning("Failed to connect to srun callback at ~s:~p: ~p",
                          [Host, Port, ConnectErr]),
            {error, ConnectErr}
    end.

%% @doc Handle callback socket - keep it alive for job event notifications.
callback_socket_loop(Socket, JobId) ->
    receive
        {tcp, Socket, Data} ->
            Hex = binary_to_hex(Data),
            lager:info("Callback data from srun for job ~p: ~p bytes, hex=~s", [JobId, byte_size(Data), Hex]),
            %% Try to parse as SLURM message
            case byte_size(Data) >= 14 of
                true ->
                    <<OL:32/big, V:16/big, F:16/big, MT:16/big, BL:32/big, _/binary>> = Data,
                    lager:info("Callback msg: outer_len=~p ver=~.16B flags=~.16B msg_type=~p body_len=~p",
                               [OL, V, F, MT, BL]);
                false -> ok
            end,
            callback_socket_loop(Socket, JobId);
        {tcp_closed, Socket} ->
            lager:info("Callback socket closed for job ~p", [JobId]),
            ok;
        {tcp_error, Socket, Reason} ->
            lager:warning("Callback socket error for job ~p: ~p", [JobId, Reason]),
            ok;
        {send_job_complete, ExitCode} ->
            %% Send SRUN_JOB_COMPLETE to srun
            lager:info("Sending job complete to srun for job ~p, exit=~p", [JobId, ExitCode]),
            Complete = #srun_job_complete{job_id = JobId, step_id = 0},
            case flurm_protocol_codec:encode_response(?SRUN_JOB_COMPLETE, Complete) of
                {ok, Bin} -> gen_tcp:send(Socket, Bin);
                _ -> ok
            end,
            gen_tcp:close(Socket),
            ok;
        stop ->
            gen_tcp:close(Socket),
            ok
    after 300000 ->  %% 5 minute timeout
        lager:debug("Callback socket timeout for job ~p", [JobId]),
        gen_tcp:close(Socket),
        ok
    end.

%% @doc Send SRUN_PING to callback to notify srun that callback is active.
-spec send_srun_ping_message(inet:socket(), non_neg_integer()) -> ok | {error, term()}.
send_srun_ping_message(Socket, JobId) ->
    lager:info("Sending SRUN_PING to callback for job ~p", [JobId]),
    Ping = #srun_ping{job_id = JobId, step_id = 0},
    case flurm_protocol_codec:encode_response(?SRUN_PING, Ping) of
        {ok, MessageBin} ->
            Result = gen_tcp:send(Socket, MessageBin),
            lager:info("Sent SRUN_PING to callback, result: ~p, size: ~p", [Result, byte_size(MessageBin)]),
            Result;
        {error, Reason} ->
            lager:warning("Failed to encode SRUN_PING: ~p", [Reason]),
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
