%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Connection Acceptor
%%%
%%% Ranch protocol handler for accepting DBD client connections.
%%% Handles connections from slurmctld, sacctmgr, sacct, and other tools.
%%%
%%% The SLURM DBD protocol uses a persist connection handshake:
%%% 1. Client sends REQUEST_PERSIST_INIT (6500) with DBD framing
%%% 2. Server responds with PERSIST_RC (1433) using simple persist format
%%% 3. Subsequent messages use DBD framing: [u16 msg_type][body]
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_acceptor).

-behaviour(ranch_protocol).

-export([start_link/3]).
-export([init/3]).

-ifdef(TEST).
-export([
    loop/1,
    process_buffer/2,
    handle_message/2,
    handle_dbd_message/2,
    handle_dbd_request/2,
    send_persist_rc/3,
    send_dbd_rc/2,
    peername/2
]).
-endif.

-include_lib("flurm_protocol/include/flurm_protocol.hrl").

-record(conn_state, {
    socket :: ranch_transport:socket(),
    transport :: module(),
    buffer = <<>> :: binary(),
    authenticated = false :: boolean(),
    client_version = 0 :: non_neg_integer(),
    client_info = #{} :: map()
}).

%%====================================================================
%% API
%%====================================================================

start_link(Ref, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Transport, Opts]),
    {ok, Pid}.

init(Ref, Transport, _Opts) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, raw}, binary]),

    lager:debug("DBD connection accepted from ~p", [peername(Socket, Transport)]),

    State = #conn_state{
        socket = Socket,
        transport = Transport
    },
    loop(State).

%%====================================================================
%% Internal functions
%%====================================================================

loop(#conn_state{socket = Socket, transport = Transport, buffer = Buffer} = State) ->
    receive
        {tcp, Socket, Data} ->
            NewBuffer = <<Buffer/binary, Data/binary>>,
            case process_buffer(NewBuffer, State) of
                {ok, Remaining, NewState} ->
                    ok = Transport:setopts(Socket, [{active, once}]),
                    loop(NewState#conn_state{buffer = Remaining});
                {error, Reason} ->
                    lager:warning("DBD connection error: ~p", [Reason]),
                    Transport:close(Socket)
            end;
        {tcp_closed, Socket} ->
            lager:debug("DBD connection closed"),
            ok;
        {tcp_error, Socket, Reason} ->
            lager:warning("DBD connection error: ~p", [Reason]),
            Transport:close(Socket);
        _Other ->
            ok = Transport:setopts(Socket, [{active, once}]),
            loop(State)
    after 300000 ->
        %% 5 minute timeout
        lager:debug("DBD connection timeout"),
        Transport:close(Socket)
    end.

process_buffer(Buffer, State) when byte_size(Buffer) < 4 ->
    %% Need more data for length prefix
    {ok, Buffer, State};
process_buffer(<<Length:32/big, Rest/binary>> = Buffer, State) when byte_size(Rest) < Length ->
    %% Need more data for complete message
    {ok, Buffer, State};
process_buffer(<<Length:32/big, Rest/binary>>, State) ->
    <<MsgData:Length/binary, Remaining/binary>> = Rest,
    case handle_message(MsgData, State) of
        {ok, NewState} ->
            %% Process any remaining data
            process_buffer(Remaining, NewState);
        {error, _} = Error ->
            Error
    end.

%% @doc Handle a complete message payload (after length prefix is stripped).
%%
%% The SLURM DBD protocol uses two framing modes:
%% 1. Initial REQUEST_PERSIST_INIT: DBD prefix (2-byte msg_type) + standard SLURM header + auth + body
%% 2. After handshake: DBD framing with 2-byte msg_type + type-specific body
%%
%% For the initial connection, sacct/sacctmgr sends REQUEST_PERSIST_INIT
%% using the standard SLURM RPC format with a DBD prefix. The slurmdbd
%% reads the first 2 bytes to get the msg_type, then dispatches.
handle_message(MsgData, _State) when byte_size(MsgData) < 2 ->
    lager:warning("DBD message too short: ~p bytes", [byte_size(MsgData)]),
    {error, message_too_short};
handle_message(MsgData, #conn_state{authenticated = false} = State) ->
    %% Not yet authenticated - expect REQUEST_PERSIST_INIT
    %% DBD framing: first 2 bytes = msg_type
    <<DbdMsgType:16/big, _Rest/binary>> = MsgData,
    lager:info("DBD pre-auth message: dbd_msg_type=~p, size=~p bytes",
               [DbdMsgType, byte_size(MsgData)]),
    case DbdMsgType of
        ?REQUEST_PERSIST_INIT ->
            handle_persist_init(MsgData, State);
        _ ->
            lager:warning("DBD: expected PERSIST_INIT (6500), got msg_type=~p", [DbdMsgType]),
            {error, {expected_persist_init, DbdMsgType}}
    end;
handle_message(MsgData, #conn_state{authenticated = true} = State) ->
    %% Authenticated - use DBD framing for subsequent messages
    handle_dbd_message(MsgData, State).

%% @doc Handle the initial PERSIST_INIT handshake.
%%
%% The incoming message format is:
%%   [u16 dbd_msg_type=6500][standard SLURM header][auth credential][persist_init body]
%%
%% We need to extract the client's protocol version from the persist_init body
%% and respond with PERSIST_RC.
handle_persist_init(MsgData, State) ->
    <<_DbdMsgType:16/big, RestAfterDbdPrefix/binary>> = MsgData,
    %% Parse the standard SLURM header that follows the DBD prefix
    %% Header format: version(u16) + flags(u16) + msg_type(u16) + body_length(u32)
    %%                + forward_cnt(u16) + ret_cnt(u16) + orig_addr(variable)
    case parse_persist_init_header(RestAfterDbdPrefix) of
        {ok, ClientVersion, _PersistBody} ->
            lager:info("DBD: PERSIST_INIT received, client version=~.16B",
                       [ClientVersion]),
            %% Negotiate protocol version - use the client's version or our own,
            %% whichever is lower
            OurVersion = ?SLURM_PROTOCOL_VERSION,
            NegotiatedVersion = min(ClientVersion, OurVersion),
            lager:info("DBD: negotiated protocol version=~.16B (client=~.16B, ours=~.16B)",
                       [NegotiatedVersion, ClientVersion, OurVersion]),
            %% Send PERSIST_RC response
            send_persist_rc(0, NegotiatedVersion, State);
        {error, Reason} ->
            lager:warning("DBD: failed to parse PERSIST_INIT: ~p", [Reason]),
            %% Try simplified parsing - just extract version from header
            case RestAfterDbdPrefix of
                <<ClientVersion:16/big, _/binary>> when ClientVersion > 16#2000 ->
                    lager:info("DBD: fallback version extraction: ~.16B", [ClientVersion]),
                    NegotiatedVersion = min(ClientVersion, ?SLURM_PROTOCOL_VERSION),
                    send_persist_rc(0, NegotiatedVersion, State);
                _ ->
                    %% Last resort - use our version
                    lager:info("DBD: using our version as fallback"),
                    send_persist_rc(0, ?SLURM_PROTOCOL_VERSION, State)
            end
    end.

%% @doc Parse the standard SLURM header from a PERSIST_INIT message.
%% Returns {ok, ClientVersion, PersistBody} or {error, Reason}.
parse_persist_init_header(Data) when byte_size(Data) < 14 ->
    {error, header_too_short};
parse_persist_init_header(<<Version:16/big, Flags:16/big, MsgType:16/big,
                            BodyLength:32/big, FwdCnt:16/big, RetCnt:16/big,
                            Rest/binary>>) ->
    lager:debug("DBD header: version=~.16B flags=~.16B msg_type=~p body_len=~p fwd=~p ret=~p",
                [Version, Flags, MsgType, BodyLength, FwdCnt, RetCnt]),
    %% Skip orig_addr - it's at least 2 bytes (AF_UNSPEC = 0x0000)
    %% For AF_INET it's 16 bytes, for AF_INET6 it's 28 bytes
    case skip_orig_addr(Rest) of
        {ok, AfterAddr} ->
            %% Skip auth credential section
            %% Auth is: plugin_id(u32) + token(packstr) + ...
            %% For auth/none it may be minimal
            case skip_auth_section(AfterAddr) of
                {ok, PersistBody} ->
                    {ok, Version, PersistBody};
                {error, _} ->
                    %% Even if we can't parse auth, we have the version
                    {ok, Version, <<>>}
            end;
        {error, _} ->
            %% Even if we can't parse addr, we have the version
            {ok, Version, <<>>}
    end.

%% Skip the orig_addr field (slurm_addr_t) in the header
skip_orig_addr(<<0:16/big, Rest/binary>>) ->
    %% AF_UNSPEC (0) - just the 2-byte family
    {ok, Rest};
skip_orig_addr(<<2:16/big, Rest/binary>>) when byte_size(Rest) >= 14 ->
    %% AF_INET (2) - 2 bytes family + 14 bytes data
    <<_:14/binary, Remaining/binary>> = Rest,
    {ok, Remaining};
skip_orig_addr(<<10:16/big, Rest/binary>>) when byte_size(Rest) >= 26 ->
    %% AF_INET6 (10) - 2 bytes family + 26 bytes data
    <<_:26/binary, Remaining/binary>> = Rest,
    {ok, Remaining};
skip_orig_addr(<<Family:16/big, _/binary>>) ->
    {error, {unknown_addr_family, Family}};
skip_orig_addr(_) ->
    {error, addr_too_short}.

%% Skip the auth credential section
skip_auth_section(<<_PluginId:32/big, Rest/binary>>) ->
    %% Skip the token string (packstr format: u32 length + data)
    case Rest of
        <<0:32/big, Rest2/binary>> ->
            %% NULL string (length 0)
            {ok, Rest2};
        <<Len:32/big, Rest2/binary>> when byte_size(Rest2) >= Len ->
            <<_Token:Len/binary, Rest3/binary>> = Rest2,
            {ok, Rest3};
        _ ->
            {error, auth_parse_failed}
    end;
skip_auth_section(_) ->
    {error, auth_too_short}.

%% @doc Send PERSIST_RC response.
%%
%% The client reads this with PERSIST_FLAG_DBD cleared, so the format is
%% the simple persist format (NOT DBD framing, NOT full RPC header):
%%
%%   [4 bytes: total payload length, big-endian]
%%   [2 bytes: msg_type = 1433 (PERSIST_RC), big-endian]
%%   [packstr: comment (u32 length + string bytes, 0 for NULL/empty)]
%%   [2 bytes: flags (u16), big-endian]
%%   [4 bytes: rc (u32, return code, 0=success), big-endian]
%%   [2 bytes: ret_info (u16, negotiated protocol version), big-endian]
%%
send_persist_rc(ReturnCode, NegotiatedVersion,
                #conn_state{socket = Socket, transport = Transport} = State) ->
    MsgType = ?PERSIST_RC,  %% 1433
    %% Pack comment as NULL string (u32 0 = no string data)
    CommentPacked = <<0:32/big>>,
    Flags = 0,
    %% Build payload: msg_type + comment + flags + rc + ret_info
    Payload = <<MsgType:16/big,
                CommentPacked/binary,
                Flags:16/big,
                ReturnCode:32/big,
                NegotiatedVersion:16/big>>,
    %% Send with 4-byte length prefix
    PayloadLen = byte_size(Payload),
    Msg = <<PayloadLen:32/big, Payload/binary>>,
    lager:info("DBD: sending PERSIST_RC (rc=~p, version=~.16B, ~p bytes)",
               [ReturnCode, NegotiatedVersion, PayloadLen]),
    case Transport:send(Socket, Msg) of
        ok ->
            lager:info("DBD: PERSIST_RC sent successfully"),
            {ok, State#conn_state{
                authenticated = true,
                client_version = NegotiatedVersion
            }};
        {error, Reason} ->
            lager:warning("DBD: failed to send PERSIST_RC: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Handle a DBD-framed message after authentication.
%%
%% DBD framing: [u16 msg_type][type-specific body]
%% (The 4-byte length prefix has already been consumed by process_buffer)
handle_dbd_message(<<DbdMsgType:16/big, Body/binary>>, State) ->
    lager:debug("DBD message: type=~p, body_size=~p", [DbdMsgType, byte_size(Body)]),
    Response = handle_dbd_request(DbdMsgType, Body),
    case Response of
        {rc, RC} ->
            send_dbd_rc(RC, State);
        {rc, RC, Comment} ->
            send_dbd_rc(RC, Comment, State);
        none ->
            {ok, State}
    end.

%% @doc Handle specific DBD request types.
%%
%% SLURM DBD message type enum (from slurmdbd_defs.h):
%%   1400 = DEFUNCT_DBD_INIT     1401 = DBD_FINI
%%   1402 = DBD_ADD_ACCOUNTS     1403 = DBD_ADD_ACCOUNT_COORDS
%%   1404 = DBD_ADD_ASSOCS       1405 = DBD_ADD_CLUSTERS
%%   1406 = DBD_ADD_USERS        1407 = DBD_CLUSTER_TRES
%%   1408 = DBD_FLUSH_JOBS       1409 = DBD_GET_ACCOUNTS
%%   1410 = DBD_GET_ASSOCS       1411 = DBD_GET_ASSOC_USAGE
%%   1412 = DBD_GET_CLUSTERS     1413 = DBD_GET_CLUSTER_USAGE
%%   1414 = DBD_RECONFIG         1415 = DBD_GET_USERS
%%   1424 = DBD_JOB_COMPLETE     1425 = DBD_JOB_START
%%   1432 = DBD_NODE_STATE       1434 = DBD_REGISTER_CTLD
%%   1444 = DBD_GET_JOBS_COND    1466 = DBD_GET_CONFIG

handle_dbd_request(?REQUEST_PERSIST_INIT, _Body) ->
    %% Duplicate persist init after auth - just ack it
    lager:info("DBD: duplicate PERSIST_INIT after auth"),
    {rc, 0};

handle_dbd_request(1401, _Body) ->
    %% DBD_FINI - connection finalize
    lager:info("DBD: received DBD_FINI"),
    {rc, 0};

handle_dbd_request(1407, _Body) ->
    %% DBD_CLUSTER_TRES - cluster TRES registration
    lager:info("DBD: received DBD_CLUSTER_TRES"),
    {rc, 0};

handle_dbd_request(1444, _Body) ->
    %% DBD_GET_JOBS_COND - sacct job query
    lager:info("DBD: received DBD_GET_JOBS_COND (sacct query)"),
    {rc, 0};

handle_dbd_request(1410, _Body) ->
    %% DBD_GET_ASSOCS - get associations
    lager:info("DBD: received DBD_GET_ASSOCS"),
    {rc, 0};

handle_dbd_request(1412, _Body) ->
    %% DBD_GET_CLUSTERS - get clusters
    lager:info("DBD: received DBD_GET_CLUSTERS"),
    {rc, 0};

handle_dbd_request(1415, _Body) ->
    %% DBD_GET_USERS - get users
    lager:info("DBD: received DBD_GET_USERS"),
    {rc, 0};

handle_dbd_request(1432, _Body) ->
    %% DBD_NODE_STATE - node state update
    lager:info("DBD: received DBD_NODE_STATE"),
    {rc, 0};

handle_dbd_request(1434, _Body) ->
    %% DBD_REGISTER_CTLD - controller registration
    lager:info("DBD: received DBD_REGISTER_CTLD"),
    {rc, 0};

handle_dbd_request(1425, _Body) ->
    %% DBD_JOB_START - job start notification
    lager:info("DBD: received DBD_JOB_START"),
    {rc, 0};

handle_dbd_request(1424, _Body) ->
    %% DBD_JOB_COMPLETE - job completion notification
    lager:info("DBD: received DBD_JOB_COMPLETE"),
    {rc, 0};

handle_dbd_request(1466, _Body) ->
    %% DBD_GET_CONFIG - get configuration
    lager:info("DBD: received DBD_GET_CONFIG"),
    {rc, 0};

handle_dbd_request(1409, _Body) ->
    %% DBD_GET_ACCOUNTS - get accounts
    lager:info("DBD: received DBD_GET_ACCOUNTS"),
    {rc, 0};

handle_dbd_request(MsgType, _Body) ->
    lager:warning("DBD: unsupported message type: ~p", [MsgType]),
    {rc, 0}.

%% @doc Send a DBD return code response using PERSIST_RC format.
%%
%% After the initial handshake, the client reads responses using
%% the DBD persist format. For PERSIST_RC responses:
%%   [4 bytes: payload length]
%%   [2 bytes: msg_type = 1433 (PERSIST_RC)]
%%   [packstr: comment]
%%   [2 bytes: flags (u16)]
%%   [4 bytes: rc (u32)]
%%   [2 bytes: ret_info (u16)]
send_dbd_rc(ReturnCode, State) ->
    send_dbd_rc(ReturnCode, null, State).

send_dbd_rc(ReturnCode, Comment,
            #conn_state{socket = Socket, transport = Transport,
                        client_version = Version} = State) ->
    MsgType = ?PERSIST_RC,
    CommentPacked = case Comment of
        null -> <<0:32/big>>;
        Bin when is_binary(Bin), byte_size(Bin) > 0 ->
            WithNull = <<Bin/binary, 0>>,
            <<(byte_size(WithNull)):32/big, WithNull/binary>>;
        _ -> <<0:32/big>>
    end,
    Flags = 0,
    RetInfo = Version,
    Payload = <<MsgType:16/big,
                CommentPacked/binary,
                Flags:16/big,
                ReturnCode:32/big,
                RetInfo:16/big>>,
    PayloadLen = byte_size(Payload),
    Msg = <<PayloadLen:32/big, Payload/binary>>,
    case Transport:send(Socket, Msg) of
        ok -> {ok, State};
        {error, _} = Error -> Error
    end.

peername(Socket, Transport) ->
    case Transport:peername(Socket) of
        {ok, {IP, Port}} ->
            io_lib:format("~p:~p", [IP, Port]);
        _ ->
            "unknown"
    end.
