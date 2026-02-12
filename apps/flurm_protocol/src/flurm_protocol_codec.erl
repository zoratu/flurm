%%%-------------------------------------------------------------------
%%% @doc FLURM Protocol Codec - Main SLURM Binary Protocol Codec
%%%
%%% This module provides the main encode/decode interface for SLURM
%%% protocol messages. It handles the complete message wire format:
%%%
%%% Wire format:
%%%   <<Length:32/big, Header:22/binary, Body/binary>>
%%%
%%% Where Length = byte_size(Header) + byte_size(Body) = 22 + body_size
%%%
%%% The codec supports the following priority message types:
%%% Requests:
%%%   - REQUEST_SUBMIT_BATCH_JOB (4003)
%%%   - REQUEST_JOB_INFO (2003)
%%%   - REQUEST_NODE_REGISTRATION_STATUS (1001)
%%%   - REQUEST_PING (1008)
%%%   - REQUEST_CANCEL_JOB (4006)
%%%
%%% Responses:
%%%   - RESPONSE_SUBMIT_BATCH_JOB (4004)
%%%   - RESPONSE_JOB_INFO (2004)
%%%   - RESPONSE_SLURM_RC (8001)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_codec).

-export([
    %% Main API
    decode/1,
    encode/2,

    %% Full wire format with extra data (auth credentials)
    decode_with_extra/1,
    encode_with_extra/2,
    encode_with_extra/3,

    %% Response encoding/decoding (with auth section)
    encode_response/2,
    encode_response_no_auth/2,
    encode_response_proper_auth/2,
    decode_response/1,

    %% Body encode/decode
    decode_body/2,
    encode_body/2,

    %% Message type helpers
    message_type_name/1,
    is_request/1,
    is_response/1,

    %% Job description extraction
    extract_full_job_desc/1,
    extract_resources_from_protocol/1,

    %% Reconfigure response encoding
    encode_reconfigure_response/1
]).

-include("flurm_protocol.hrl").

-ifdef(TEST).
-export([
    extract_resources/1,
    strip_auth_section/1
]).
-endif.

%%%===================================================================
%%% Main API
%%%===================================================================

%% @doc Decode a complete message from wire format.
%%
%% Expects: <<Length:32/big, Header/binary, Body:BodySize/binary>>
%% Header is variable length (16-34 bytes depending on orig_addr family)
%%
%% Returns {ok, Message, Rest} on success, where Message is a #slurm_msg{}
%% record with the body decoded to an appropriate record type.
-spec decode(binary()) -> {ok, #slurm_msg{}, binary()} | {error, term()}.
decode(<<Length:32/big, Rest/binary>> = _Data)
  when byte_size(Rest) >= Length, Length >= ?SLURM_HEADER_SIZE_MIN ->
    <<MsgData:Length/binary, Remaining/binary>> = Rest,
    %% Parse header with variable-length orig_addr support
    case flurm_protocol_header:parse_header(MsgData) of
        {ok, Header, BodyBin} ->
            MsgType = Header#slurm_header.msg_type,
            case decode_body(MsgType, BodyBin) of
                {ok, Body} ->
                    Msg = #slurm_msg{header = Header, body = Body},
                    {ok, Msg, Remaining};
                {error, _} = BodyError ->
                    BodyError
            end;
        {error, _} = HeaderError ->
            HeaderError
    end;
decode(<<Length:32/big, Rest/binary>>)
  when byte_size(Rest) < Length ->
    {error, {incomplete_message, Length, byte_size(Rest)}};
decode(<<Length:32/big, _/binary>>)
  when Length < ?SLURM_HEADER_SIZE_MIN ->
    {error, {invalid_message_length, Length}};
decode(Binary) when byte_size(Binary) < 4 ->
    {error, {incomplete_length_prefix, byte_size(Binary)}};
decode(_) ->
    {error, invalid_message_data}.

%% @doc Encode a message to wire format.
%%
%% Takes a message type and body record, produces wire-format binary.
%% Returns {ok, Binary} on success.
-spec encode(non_neg_integer(), term()) -> {ok, binary()} | {error, term()}.
encode(MsgType, Body) ->
    case encode_body(MsgType, Body) of
        {ok, BodyBin} ->
            Header = #slurm_header{
                version = flurm_protocol_header:protocol_version(),
                flags = 0,
                msg_index = 0,
                msg_type = MsgType,
                body_length = byte_size(BodyBin)
            },
            case flurm_protocol_header:encode_header(Header) of
                {ok, HeaderBin} ->
                    Length = byte_size(HeaderBin) + byte_size(BodyBin),
                    {ok, <<Length:32/big, HeaderBin/binary, BodyBin/binary>>};
                {error, _} = HeaderError ->
                    HeaderError
            end;
        {error, _} = BodyError ->
            BodyError
    end.

%% @doc Decode a complete message with auth credentials from wire format.
%%
%% SLURM wire format for requests (client to server):
%%   <<Length:32/big, Header:22/binary, AuthSection/binary, MsgBody/binary>>
%%
%% Auth section format (from slurm_auth.c):
%%   <<PluginId:32/big, CredLen:32/big, Credential:CredLen/binary>>
%%
%% Returns {ok, Message, ExtraInfo, Rest} on success.
-spec decode_with_extra(binary()) -> {ok, #slurm_msg{}, map(), binary()} | {error, term()}.
decode_with_extra(<<Length:32/big, Rest/binary>> = _Data)
  when byte_size(Rest) >= Length, Length >= ?SLURM_HEADER_SIZE_MIN ->
    <<MsgData:Length/binary, Remaining/binary>> = Rest,
    %% Parse header with variable-length orig_addr support
    case flurm_protocol_header:parse_header(MsgData) of
        {ok, Header, BodyWithAuth} ->
            MsgType = Header#slurm_header.msg_type,
            %% Strip auth section from beginning of body
            case strip_auth_section(BodyWithAuth) of
                {ok, ActualBody, AuthInfo} ->
                    case decode_body(MsgType, ActualBody) of
                        {ok, Body} ->
                            Msg = #slurm_msg{header = Header, body = Body},
                            {ok, Msg, AuthInfo, Remaining};
                        {error, _} = BodyError ->
                            BodyError
                    end;
                {error, _} = AuthError ->
                    AuthError
            end;
        {error, _} = HeaderError ->
            HeaderError
    end;
decode_with_extra(<<Length:32/big, Rest/binary>>)
  when byte_size(Rest) < Length ->
    {error, {incomplete_message, Length, byte_size(Rest)}};
decode_with_extra(Binary) when byte_size(Binary) < 4 ->
    {error, {incomplete_length_prefix, byte_size(Binary)}};
decode_with_extra(_) ->
    {error, invalid_message_data}.

%% @doc Strip the auth section from the beginning of a message body.
%% Auth section format from slurm_auth.c:
%%   plugin_id:32 (4 bytes)
%%   credential packstr: length:32 + data (variable length)
%% Note: The packstr includes length + data (no null terminator in SLURM packstr for auth)
-spec strip_auth_section(binary()) -> {ok, binary(), map()} | {error, term()}.
strip_auth_section(<<PluginId:32/big, CredLen:32/big, Rest/binary>> = Full) when CredLen =< byte_size(Rest) ->
    lager:debug("strip_auth: plugin_id=~p, cred_len=~p, rest_size=~p, full_size=~p",
                [PluginId, CredLen, byte_size(Rest), byte_size(Full)]),
    <<Credential:CredLen/binary, ActualBody/binary>> = Rest,
    lager:debug("strip_auth: stripped ~p bytes auth, body_size=~p", [8 + CredLen, byte_size(ActualBody)]),
    %% Check if credential is MUNGE (plugin_id 101)
    AuthType = case PluginId of
        101 -> munge;
        _ -> unknown
    end,
    AuthInfo = #{auth_type => AuthType, plugin_id => PluginId, cred_len => CredLen, credential => Credential},
    {ok, ActualBody, AuthInfo};
strip_auth_section(<<PluginId:32/big, CredLen:32/big, Rest/binary>>) ->
    lager:warning("strip_auth FAIL: plugin_id=~p, cred_len=~p, rest_size=~p (too short)",
                  [PluginId, CredLen, byte_size(Rest)]),
    {error, {auth_cred_too_short, CredLen}};
strip_auth_section(Binary) when byte_size(Binary) < 8 ->
    lager:warning("strip_auth FAIL: binary too short (~p bytes)", [byte_size(Binary)]),
    {error, {auth_section_too_short, byte_size(Binary)}};
strip_auth_section(_) ->
    {error, invalid_auth_section}.

%% @doc Encode a message with extra data (auth credentials) to wire format.
%%
%% Uses default hostname for extra data.
%% Wire format: <<Length:32/big, Header:10/binary, Body/binary, ExtraData:39/binary>>
-spec encode_with_extra(non_neg_integer(), term()) -> {ok, binary()} | {error, term()}.
encode_with_extra(MsgType, Body) ->
    encode_with_extra(MsgType, Body, flurm_protocol_auth:default_hostname()).

%% @doc Encode a message with extra data (auth credentials) to wire format.
%%
%% Takes message type, body, and hostname for extra data.
%% Wire format: <<Length:32/big, Header:10/binary, Body/binary, ExtraData:39/binary>>
-spec encode_with_extra(non_neg_integer(), term(), binary()) -> {ok, binary()} | {error, term()}.
encode_with_extra(MsgType, Body, Hostname) ->
    case encode_body(MsgType, Body) of
        {ok, BodyBin} ->
            Header = #slurm_header{
                version = flurm_protocol_header:protocol_version(),
                flags = 0,
                msg_index = 0,
                msg_type = MsgType,
                body_length = byte_size(BodyBin)
            },
            case flurm_protocol_header:encode_header(Header) of
                {ok, HeaderBin} ->
                    ExtraData = flurm_protocol_auth:encode_extra(MsgType, Hostname),
                    %% Length includes header + body + extra data
                    Length = byte_size(HeaderBin) + byte_size(BodyBin) + byte_size(ExtraData),
                    {ok, <<Length:32/big, HeaderBin/binary, BodyBin/binary, ExtraData/binary>>};
                {error, _} = HeaderError ->
                    HeaderError
            end;
        {error, _} = BodyError ->
            BodyError
    end.

%% @doc Encode a response message.
%%
%% SLURM 24.11 response wire format (server to client):
%%   <<OuterLength:32, Header:14, AuthSection/binary, Body/binary>>
%% Where:
%%   - Header = <<Version:16, Flags:16, MsgType:16, BodyLen:32, ForwardCnt:16, RetCnt:16>>
%%   - AuthSection = <<PluginId:32, MungeMagic:32, Errno:32, CredPackstr/binary>>
%%   - Header.body_length = BodySize ONLY (auth is NOT counted in body_length)
%%   - ForwardCnt and RetCnt are 0 for simple responses
%%
-spec encode_response(non_neg_integer(), term()) -> {ok, binary()} | {error, term()}.
encode_response(MsgType, Body) ->
    case encode_body(MsgType, Body) of
        {ok, BodyBin} ->
            BodySize = byte_size(BodyBin),
            %% Log body hex for debugging (first 64 bytes)
            BodyHex = case BodySize > 64 of
                true ->
                    <<First64:64/binary, _/binary>> = BodyBin,
                    binary_to_hex(First64);
                false ->
                    binary_to_hex(BodyBin)
            end,
            lager:info("Body for msg_type=~p: size=~p hex64=~s", [MsgType, BodySize, BodyHex]),

            %% Create auth section WITH proper message hash embedded in MUNGE credential
            AuthSection = create_proper_auth_section(MsgType),
            AuthSize = byte_size(AuthSection),

            lager:info("encode_response: auth=~p, body=~p (msg_type=~p)",
                       [AuthSize, BodySize, MsgType]),

            %% body_length = just the message body
            %% The hash is embedded in MUNGE credential, no separate hash section needed
            TotalBodyLen = BodySize,
            Header = #slurm_header{
                version = flurm_protocol_header:protocol_version(),
                flags = 0,
                msg_index = 0,
                msg_type = MsgType,
                body_length = TotalBodyLen  %% hash + body
            },
            case flurm_protocol_header:encode_header(Header) of
                {ok, HeaderBin} ->
                    %% Debug: show each section in detail
                    HeaderHex = binary_to_hex(HeaderBin),
                    AuthHex = binary_to_hex(AuthSection),
                    FullBodyHex = binary_to_hex(BodyBin),
                    lager:info("DEBUG header(~p): ~s", [byte_size(HeaderBin), HeaderHex]),
                    lager:info("DEBUG auth(~p): ~s", [AuthSize, AuthHex]),
                    lager:info("DEBUG body(~p): ~s", [BodySize, FullBodyHex]),
                    %% Wire format: outer_length, header, auth_section, body
                    %% Hash is embedded in MUNGE credential, no separate hash section
                    TotalPayload = <<HeaderBin/binary, AuthSection/binary, BodyBin/binary>>,
                    OuterLength = byte_size(TotalPayload),
                    FullMessage = <<OuterLength:32/big, TotalPayload/binary>>,
                    lager:info("FULL MESSAGE (~p bytes): ~s",
                               [byte_size(FullMessage), binary_to_hex(FullMessage)]),
                    {ok, FullMessage};
                {error, _} = HeaderError ->
                    HeaderError
            end;
        {error, _} = BodyError ->
            BodyError
    end.

%% @doc Encode a response message WITHOUT auth section.
%% Wire format: <<OuterLength:32, Header:14, Body>>
%% Sets SLURM_NO_AUTH_CRED flag to tell client to skip auth parsing.
-spec encode_response_no_auth(non_neg_integer(), term()) -> {ok, binary()} | {error, term()}.
encode_response_no_auth(MsgType, Body) ->
    case encode_body(MsgType, Body) of
        {ok, BodyBin} ->
            BodySize = byte_size(BodyBin),
            Header = #slurm_header{
                version = flurm_protocol_header:protocol_version(),
                flags = ?SLURM_NO_AUTH_CRED,  %% Flag: no auth credential present
                msg_index = 0,
                msg_type = MsgType,
                body_length = BodySize
            },
            case flurm_protocol_header:encode_header(Header) of
                {ok, HeaderBin} ->
                    TotalPayload = <<HeaderBin/binary, BodyBin/binary>>,
                    OuterLength = byte_size(TotalPayload),
                    lager:debug("Response (no auth): header=~p body=~p total=~p",
                               [byte_size(HeaderBin), BodySize, OuterLength]),
                    {ok, <<OuterLength:32/big, TotalPayload/binary>>};
                {error, _} = HeaderError ->
                    HeaderError
            end;
        {error, _} = BodyError ->
            BodyError
    end.

%% @doc Encode a response with proper SLURM MUNGE auth format.
%% For srun RESPONSE_RESOURCE_ALLOCATION compatibility.
%% Wire format: <<OuterLength:32, Header:22, AuthSection, Body>>
%% Where AuthSection uses proper MUNGE format: plugin_id, magic, errno, credential_packstr
-spec encode_response_proper_auth(non_neg_integer(), term()) -> {ok, binary()} | {error, term()}.
encode_response_proper_auth(MsgType, Body) ->
    case encode_body(MsgType, Body) of
        {ok, BodyBin} ->
            BodySize = byte_size(BodyBin),
            AuthSection = create_proper_auth_section(MsgType),
            AuthSize = byte_size(AuthSection),
            %% body_length = BODY ONLY (auth size determined separately)
            Header = #slurm_header{
                version = flurm_protocol_header:protocol_version(),
                flags = 0,
                msg_index = 0,
                msg_type = MsgType,
                body_length = BodySize  %% Body ONLY
            },
            case flurm_protocol_header:encode_header(Header) of
                {ok, HeaderBin} ->
                    TotalPayload = <<HeaderBin/binary, AuthSection/binary, BodyBin/binary>>,
                    OuterLength = byte_size(TotalPayload),
                    lager:info("Response (proper auth): header=~p auth=~p body=~p body_length=~p total=~p",
                               [byte_size(HeaderBin), AuthSize, BodySize, BodySize, OuterLength]),
                    {ok, <<OuterLength:32/big, TotalPayload/binary>>};
                {error, _} = HeaderError ->
                    HeaderError
            end;
        {error, _} = BodyError ->
            BodyError
    end.

%% @doc Decode a response message (with auth section stripped).
%%
%% Response wire format (server to client):
%%   <<OuterLength:32, Header:14, AuthSection/binary, Body/binary>>
%% Where Header = <<Version:16, Flags:16, MsgType:16, BodyLen:32, ForwardCnt:16, RetCnt:16>>
%%
%% This decoder strips the auth section and returns just the message.
-spec decode_response(binary()) -> {ok, #slurm_msg{}, binary()} | {error, term()}.
decode_response(<<Length:32/big, Rest/binary>> = _Data)
  when byte_size(Rest) >= Length, Length >= ?SLURM_HEADER_SIZE_MIN ->
    <<MsgData:Length/binary, Remaining/binary>> = Rest,
    %% Parse header with variable-length orig_addr support
    case flurm_protocol_header:parse_header(MsgData) of
        {ok, Header, BodyWithAuth} ->
            MsgType = Header#slurm_header.msg_type,
            BodyLen = Header#slurm_header.body_length,
            %% Auth section is between header and body
            case strip_auth_section_from_response(BodyWithAuth, BodyLen) of
                {ok, ActualBody} ->
                    case decode_body(MsgType, ActualBody) of
                        {ok, Body} ->
                            Msg = #slurm_msg{header = Header, body = Body},
                            {ok, Msg, Remaining};
                        {error, _} = BodyError ->
                            BodyError
                    end;
                {error, _} = AuthError ->
                    AuthError
            end;
        {error, _} = HeaderError ->
            HeaderError
    end;
decode_response(<<Length:32/big, Rest/binary>>)
  when byte_size(Rest) < Length ->
    {error, {incomplete_message, Length, byte_size(Rest)}};
decode_response(Binary) when byte_size(Binary) < 4 ->
    {error, {incomplete_length_prefix, byte_size(Binary)}};
decode_response(_) ->
    {error, invalid_message_data}.

%% @doc Strip auth section from response to get actual body.
%% The auth section is at the start, and body is the last BodyLen bytes.
-spec strip_auth_section_from_response(binary(), non_neg_integer()) -> {ok, binary()} | {error, term()}.
strip_auth_section_from_response(BodyWithAuth, BodyLen) when byte_size(BodyWithAuth) >= BodyLen ->
    %% Body is at the END of BodyWithAuth
    AuthLen = byte_size(BodyWithAuth) - BodyLen,
    <<_AuthSection:AuthLen/binary, ActualBody:BodyLen/binary>> = BodyWithAuth,
    {ok, ActualBody};
strip_auth_section_from_response(BodyWithAuth, BodyLen) ->
    {error, {body_too_short, byte_size(BodyWithAuth), BodyLen}}.

%% @doc Get MUNGE credential by calling munge command with optional payload
%% If MsgType is provided, embeds a HASH_PLUGIN_NONE hash (plugin_type + msg_type bytes)
-dialyzer({nowarn_function, get_munge_credential/1}).
-compile({nowarn_unused_function, get_munge_credential/1}).
-spec get_munge_credential(non_neg_integer() | undefined) -> {ok, binary()} | {error, term()}.
get_munge_credential(undefined) ->
    %% No hash - create credential with no payload
    case os:cmd("munge -n 2>/dev/null") of
        [] -> {error, munge_not_available};
        Result ->
            Cred = string:trim(Result, trailing, "\n"),
            {ok, list_to_binary(Cred)}
    end;
get_munge_credential(MsgType) when is_integer(MsgType) ->
    %% Create MUNGE credential WITH hash payload embedded
    %% SLURM's _check_hash() expects EXACTLY 3 bytes for HASH_PLUGIN_NONE:
    %%   byte 0: 0x01 (HASH_PLUGIN_NONE - NOTE: SLURM enum starts at DEFAULT=0, NONE=1)
    %%   byte 1: msg_type high byte (network byte order)
    %%   byte 2: msg_type low byte (network byte order)
    %% The msg_type is checked: (cred_hash[1] == type[0]) && (cred_hash[2] == type[1])
    %% SLURM hash_plugin_type enum: DEFAULT=0, NONE=1, K12=2, SHA256=3
    MsgTypeHigh = (MsgType bsr 8) band 16#FF,
    MsgTypeLow = MsgType band 16#FF,
    %% Use printf with OCTAL escapes for binary payload
    %% Format: \NNN where NNN is 3 octal digits
    %% Build the command string directly with single quotes to avoid shell escaping issues
    %% Single quotes prevent the shell from interpreting escapes, but printf interprets them
    Cmd = lists:flatten(io_lib:format(
        "printf '\\~3.8.0B\\~3.8.0B\\~3.8.0B' | munge 2>/dev/null",
        [1, MsgTypeHigh, MsgTypeLow])),  %% 1 = HASH_PLUGIN_NONE
    lager:info("Creating MUNGE credential with 3-byte hash (type=NONE=1, msg_type=0x~4.16.0B)", [MsgType]),
    lager:debug("MUNGE command: ~s", [Cmd]),
    case os:cmd(Cmd) of
        [] ->
            lager:warning("MUNGE command returned empty, trying without payload"),
            %% Fallback to no payload
            case os:cmd("munge -n 2>/dev/null") of
                [] -> {error, munge_not_available};
                FallbackResult ->
                    Cred = string:trim(FallbackResult, trailing, "\n"),
                    lager:info("MUNGE credential obtained (no hash fallback), length=~p", [length(Cred)]),
                    {ok, list_to_binary(Cred)}
            end;
        Result ->
            Cred = string:trim(Result, trailing, "\n"),
            lager:info("MUNGE credential obtained (hash=NONE), length=~p", [length(Cred)]),
            {ok, list_to_binary(Cred)}
    end.

%% Old auth section format removed - using create_proper_auth_section only

%% @doc Create auth section with proper SLURM format
%% Format for auth_g_unpack (reading order):
%%   plugin_id:32 (101 = AUTH_PLUGIN_MUNGE) - read FIRST by auth_g_unpack
%%   credential_packstr (length:32, credential, null:8) - read by auth_p_unpack
%%
%% The MUNGE credential contains a 3-byte hash payload:
%%   [HASH_PLUGIN_NONE=1, msg_type_hi, msg_type_lo]
%% SLURM hash_plugin_type enum: DEFAULT=0, NONE=1, K12=2, SHA256=3
%% This is verified by SLURM's _check_hash() function.
%%
%% Auth section with MUNGE credential including embedded msg_type hash
-spec create_proper_auth_section(non_neg_integer() | undefined) -> binary().
create_proper_auth_section(MsgType) ->
    PluginId = 101,  %% AUTH_PLUGIN_MUNGE
    %% Create MUNGE credential WITH 3-byte hash payload
    %% SLURM's _check_hash() expects: [HASH_PLUGIN_NONE=1, msg_type_hi, msg_type_lo]
    %%
    %% SLURM's auth_munge.c uses packstr (length includes null, data ends with null).
    %% See _pack_cred() which calls packstr(cred->signature, buffer).
    case get_munge_credential(MsgType) of
        {ok, Credential} ->
            CredLen = byte_size(Credential),
            %% SLURM auth uses packstr format (includes null terminator)
            CredPackstr = flurm_protocol_pack:pack_string(Credential),
            AuthBin = <<PluginId:32/big, CredPackstr/binary>>,
            lager:info("AUTH: plugin_id=~p cred_len=~p packstr_len=~p total=~p msg_type=~p",
                       [PluginId, CredLen, byte_size(CredPackstr), byte_size(AuthBin), MsgType]),
            AuthBin;
        {error, _} ->
            %% No MUNGE - use empty credential (will likely fail verification)
            EmptyPackstr = flurm_protocol_pack:pack_string(<<>>),
            AuthBin = <<PluginId:32/big, EmptyPackstr/binary>>,
            lager:info("AUTH (no munge): plugin_id=~p total=~p", [PluginId, byte_size(AuthBin)]),
            AuthBin
    end.

%%%===================================================================
%%% Body Encoding/Decoding
%%%===================================================================

%% @doc Decode message body based on message type.
-spec decode_body(non_neg_integer(), binary()) -> {ok, term()} | {error, term()}.

%% REQUEST_PING (1008) - Empty body
decode_body(?REQUEST_PING, <<>>) ->
    {ok, #ping_request{}};
decode_body(?REQUEST_PING, _Binary) ->
    %% Accept any body for ping, often empty
    {ok, #ping_request{}};

%% REQUEST_NODE_REGISTRATION_STATUS (1001)
decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Binary) ->
    decode_node_registration_request(Binary);

%% REQUEST_JOB_INFO (2003)
decode_body(?REQUEST_JOB_INFO, Binary) ->
    decode_job_info_request(Binary);

%% REQUEST_RESOURCE_ALLOCATION (4001) - srun
decode_body(?REQUEST_RESOURCE_ALLOCATION, Binary) ->
    decode_resource_allocation_request(Binary);

%% REQUEST_SUBMIT_BATCH_JOB (4003)
decode_body(?REQUEST_SUBMIT_BATCH_JOB, Binary) ->
    decode_batch_job_request(Binary);

%% REQUEST_CANCEL_JOB (4006)
decode_body(?REQUEST_CANCEL_JOB, Binary) ->
    decode_cancel_job_request(Binary);

%% REQUEST_KILL_JOB (5032) - used by scancel
decode_body(?REQUEST_KILL_JOB, Binary) ->
    decode_kill_job_request(Binary);

%% REQUEST_SUSPEND (5014) - scontrol suspend/resume
decode_body(?REQUEST_SUSPEND, Binary) ->
    decode_suspend_request(Binary);

%% REQUEST_SIGNAL_JOB (5018) - scancel -s SIGNAL, scontrol signal
decode_body(?REQUEST_SIGNAL_JOB, Binary) ->
    decode_signal_job_request(Binary);

%% REQUEST_COMPLETE_PROLOG (5019) - slurmd reports prolog completion
decode_body(?REQUEST_COMPLETE_PROLOG, Binary) ->
    decode_complete_prolog_request(Binary);

%% MESSAGE_EPILOG_COMPLETE (6012) - slurmd reports epilog completion
decode_body(?MESSAGE_EPILOG_COMPLETE, Binary) ->
    decode_epilog_complete_msg(Binary);

%% MESSAGE_TASK_EXIT (6003) - slurmd reports task exit
decode_body(?MESSAGE_TASK_EXIT, Binary) ->
    decode_task_exit_msg(Binary);

%% REQUEST_UPDATE_JOB (4014) - scontrol update job, hold, release
decode_body(?REQUEST_UPDATE_JOB, Binary) ->
    decode_update_job_request(Binary);

%% REQUEST_JOB_WILL_RUN (4012) - sbatch --test-only
decode_body(?REQUEST_JOB_WILL_RUN, Binary) ->
    decode_job_will_run_request(Binary);

%% REQUEST_JOB_STEP_CREATE (5001)
decode_body(?REQUEST_JOB_STEP_CREATE, Binary) ->
    decode_job_step_create_request(Binary);

%% REQUEST_JOB_STEP_INFO (5003)
decode_body(?REQUEST_JOB_STEP_INFO, Binary) ->
    decode_job_step_info_request(Binary);

%% REQUEST_RESERVATION_INFO (2012)
decode_body(?REQUEST_RESERVATION_INFO, Binary) ->
    decode_reservation_info_request(Binary);

%% REQUEST_LICENSE_INFO (1017)
decode_body(?REQUEST_LICENSE_INFO, Binary) ->
    decode_license_info_request(Binary);

%% REQUEST_TOPO_INFO (2018)
decode_body(?REQUEST_TOPO_INFO, Binary) ->
    decode_topo_info_request(Binary);

%% REQUEST_FRONT_END_INFO (2028)
decode_body(?REQUEST_FRONT_END_INFO, Binary) ->
    decode_front_end_info_request(Binary);

%% REQUEST_BURST_BUFFER_INFO (2020)
decode_body(?REQUEST_BURST_BUFFER_INFO, Binary) ->
    decode_burst_buffer_info_request(Binary);

%% REQUEST_RECONFIGURE (1003) - scontrol reconfigure
decode_body(?REQUEST_RECONFIGURE, Binary) ->
    decode_reconfigure_request(Binary);

%% REQUEST_RECONFIGURE_WITH_CONFIG (1004) - scontrol reconfigure with specific settings
decode_body(?REQUEST_RECONFIGURE_WITH_CONFIG, Binary) ->
    decode_reconfigure_with_config_request(Binary);

%% REQUEST_SHUTDOWN (1005) - scontrol shutdown
decode_body(?REQUEST_SHUTDOWN, Binary) ->
    decode_shutdown_request(Binary);

%% REQUEST_BUILD_INFO (2001) - scontrol show config (build info)
decode_body(?REQUEST_BUILD_INFO, _Binary) ->
    {ok, #{}};

%% REQUEST_CONFIG_INFO (2016) - configuration dump
decode_body(?REQUEST_CONFIG_INFO, _Binary) ->
    {ok, #{}};

%% REQUEST_STATS_INFO (2026) - sdiag statistics
decode_body(?REQUEST_STATS_INFO, Binary) ->
    decode_stats_info_request(Binary);

%% RESPONSE_SLURM_RC (8001)
decode_body(?RESPONSE_SLURM_RC, Binary) ->
    decode_slurm_rc_response(Binary);

%% RESPONSE_SUBMIT_BATCH_JOB (4004)
decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary) ->
    decode_batch_job_response(Binary);

%% RESPONSE_JOB_INFO (2004)
decode_body(?RESPONSE_JOB_INFO, Binary) ->
    decode_job_info_response(Binary);

%% RESPONSE_PARTITION_INFO (2010)
decode_body(?RESPONSE_PARTITION_INFO, Binary) ->
    decode_partition_info_response(Binary);

%% REQUEST_LAUNCH_TASKS (6001) - srun launching tasks on node
decode_body(?REQUEST_LAUNCH_TASKS, Binary) ->
    decode_launch_tasks_request(Binary);

%% REQUEST_FED_INFO (2049) - Federation info request
decode_body(?REQUEST_FED_INFO, Binary) ->
    decode_fed_info_request(Binary);

%% REQUEST_FEDERATION_SUBMIT (2032) - Cross-cluster job submission
decode_body(?REQUEST_FEDERATION_SUBMIT, Binary) ->
    decode_federation_submit_request(Binary);

%% REQUEST_FEDERATION_JOB_STATUS (2034) - Query job on remote cluster
decode_body(?REQUEST_FEDERATION_JOB_STATUS, Binary) ->
    decode_federation_job_status_request(Binary);

%% REQUEST_FEDERATION_JOB_CANCEL (2036) - Cancel job on remote cluster
decode_body(?REQUEST_FEDERATION_JOB_CANCEL, Binary) ->
    decode_federation_job_cancel_request(Binary);

%% RESPONSE_FED_INFO (2050) - Federation info response
decode_body(?RESPONSE_FED_INFO, Binary) ->
    decode_fed_info_response(Binary);

%% RESPONSE_FEDERATION_SUBMIT (2033) - Cross-cluster job response
decode_body(?RESPONSE_FEDERATION_SUBMIT, Binary) ->
    decode_federation_submit_response(Binary);

%% RESPONSE_FEDERATION_JOB_STATUS (2035) - Remote job status
decode_body(?RESPONSE_FEDERATION_JOB_STATUS, Binary) ->
    decode_federation_job_status_response(Binary);

%% RESPONSE_FEDERATION_JOB_CANCEL (2037) - Remote cancel response
decode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Binary) ->
    decode_federation_job_cancel_response(Binary);

%% REQUEST_UPDATE_FEDERATION (2064) - Federation update request
decode_body(?REQUEST_UPDATE_FEDERATION, Binary) ->
    decode_update_federation_request(Binary);

%% RESPONSE_UPDATE_FEDERATION (2065) - Federation update response
decode_body(?RESPONSE_UPDATE_FEDERATION, Binary) ->
    decode_update_federation_response(Binary);

%% MSG_FED_JOB_SUBMIT (2070) - Sibling job creation
decode_body(?MSG_FED_JOB_SUBMIT, Binary) ->
    decode_fed_job_submit_msg(Binary);

%% MSG_FED_JOB_STARTED (2071) - Job started notification
decode_body(?MSG_FED_JOB_STARTED, Binary) ->
    decode_fed_job_started_msg(Binary);

%% MSG_FED_SIBLING_REVOKE (2072) - Sibling revocation
decode_body(?MSG_FED_SIBLING_REVOKE, Binary) ->
    decode_fed_sibling_revoke_msg(Binary);

%% MSG_FED_JOB_COMPLETED (2073) - Job completed notification
decode_body(?MSG_FED_JOB_COMPLETED, Binary) ->
    decode_fed_job_completed_msg(Binary);

%% MSG_FED_JOB_FAILED (2074) - Job failed notification
decode_body(?MSG_FED_JOB_FAILED, Binary) ->
    decode_fed_job_failed_msg(Binary);

%% Unknown message type - return raw body
decode_body(_MsgType, Binary) ->
    {ok, Binary}.

%% @doc Encode message body based on message type.
-spec encode_body(non_neg_integer(), term()) -> {ok, binary()} | {error, term()}.

%% REQUEST_PING (1008)
encode_body(?REQUEST_PING, #ping_request{}) ->
    {ok, <<>>};

%% REQUEST_NODE_REGISTRATION_STATUS (1001)
encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req) ->
    encode_node_registration_request(Req);

%% REQUEST_JOB_INFO (2003)
encode_body(?REQUEST_JOB_INFO, Req) ->
    encode_job_info_request(Req);

%% REQUEST_SUBMIT_BATCH_JOB (4003)
encode_body(?REQUEST_SUBMIT_BATCH_JOB, Req) ->
    encode_batch_job_request(Req);

%% REQUEST_CANCEL_JOB (4006)
encode_body(?REQUEST_CANCEL_JOB, Req) ->
    encode_cancel_job_request(Req);

%% REQUEST_KILL_JOB (5032)
encode_body(?REQUEST_KILL_JOB, Req) ->
    encode_kill_job_request(Req);

%% RESPONSE_JOB_WILL_RUN (4013)
encode_body(?RESPONSE_JOB_WILL_RUN, Resp) ->
    encode_job_will_run_response(Resp);

%% RESPONSE_JOB_READY (4020) - srun checks if nodes are ready for job
%% Format: return_code(32) - same as RESPONSE_SLURM_RC
encode_body(?RESPONSE_JOB_READY, Resp) ->
    encode_job_ready_response(Resp);

%% RESPONSE_SLURM_RC (8001)
encode_body(?RESPONSE_SLURM_RC, Resp) ->
    encode_slurm_rc_response(Resp);

%% SRUN_JOB_COMPLETE (7004)
encode_body(?SRUN_JOB_COMPLETE, Resp) ->
    encode_srun_job_complete(Resp);

%% SRUN_PING (7001)
encode_body(?SRUN_PING, Resp) ->
    encode_srun_ping(Resp);

%% RESPONSE_SUBMIT_BATCH_JOB (4004)
encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Resp) ->
    encode_batch_job_response(Resp);

%% RESPONSE_JOB_INFO (2004)
encode_body(?RESPONSE_JOB_INFO, Resp) ->
    encode_job_info_response(Resp);

%% REQUEST_NODE_INFO (2007)
encode_body(?REQUEST_NODE_INFO, #node_info_request{} = Req) ->
    encode_node_info_request(Req);
encode_body(?REQUEST_NODE_INFO, _) ->
    {ok, <<0:32/big, 0:32/big>>};  % show_flags=0, empty node name

%% RESPONSE_NODE_INFO (2008)
encode_body(?RESPONSE_NODE_INFO, Resp) ->
    Result = encode_node_info_response(Resp),
    case Result of
        {ok, Bin} ->
            lager:info("RESPONSE_NODE_INFO: encoded ~p nodes, ~p bytes",
                      [Resp#node_info_response.node_count, byte_size(Bin)]);
        _ -> ok
    end,
    Result;

%% REQUEST_PARTITION_INFO (2009)
encode_body(?REQUEST_PARTITION_INFO, #partition_info_request{} = Req) ->
    encode_partition_info_request(Req);
encode_body(?REQUEST_PARTITION_INFO, _) ->
    {ok, <<0:32/big, 0:32/big>>};  % show_flags=0, empty partition name

%% RESPONSE_PARTITION_INFO (2010)
encode_body(?RESPONSE_PARTITION_INFO, Resp) ->
    encode_partition_info_response(Resp);

%% RESPONSE_RESOURCE_ALLOCATION (4002)
encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp) ->
    encode_resource_allocation_response(Resp);

%% RESPONSE_JOB_ALLOCATION_INFO (4020) - same format as RESPONSE_RESOURCE_ALLOCATION
encode_body(?RESPONSE_JOB_ALLOCATION_INFO, Resp) ->
    encode_resource_allocation_response(Resp);

%% RESPONSE_JOB_STEP_CREATE (5002)
encode_body(?RESPONSE_JOB_STEP_CREATE, Resp) ->
    encode_job_step_create_response(Resp);

%% RESPONSE_JOB_STEP_INFO (5004)
encode_body(?RESPONSE_JOB_STEP_INFO, Resp) ->
    encode_job_step_info_response(Resp);

%% RESPONSE_RESERVATION_INFO (2013)
encode_body(?RESPONSE_RESERVATION_INFO, Resp) ->
    encode_reservation_info_response(Resp);

%% RESPONSE_LICENSE_INFO (1018)
encode_body(?RESPONSE_LICENSE_INFO, Resp) ->
    encode_license_info_response(Resp);

%% RESPONSE_TOPO_INFO (2019)
encode_body(?RESPONSE_TOPO_INFO, Resp) ->
    encode_topo_info_response(Resp);

%% RESPONSE_FRONT_END_INFO (2029)
encode_body(?RESPONSE_FRONT_END_INFO, Resp) ->
    encode_front_end_info_response(Resp);

%% RESPONSE_BURST_BUFFER_INFO (2021)
encode_body(?RESPONSE_BURST_BUFFER_INFO, Resp) ->
    encode_burst_buffer_info_response(Resp);

%% RESPONSE_BUILD_INFO (2002)
encode_body(?RESPONSE_BUILD_INFO, Resp) ->
    encode_build_info_response(Resp);

%% RESPONSE_CONFIG_INFO (2017)
encode_body(?RESPONSE_CONFIG_INFO, Resp) ->
    encode_config_info_response(Resp);

%% RESPONSE_STATS_INFO (2027)
encode_body(?RESPONSE_STATS_INFO, Resp) ->
    encode_stats_info_response(Resp);

%% RESPONSE_LAUNCH_TASKS (5009) - Response to srun task launch
encode_body(?RESPONSE_LAUNCH_TASKS, Resp) ->
    encode_launch_tasks_response(Resp);

%% RESPONSE_REATTACH_TASKS (5013)
encode_body(?RESPONSE_REATTACH_TASKS, Resp) ->
    encode_reattach_tasks_response(Resp);

%% MESSAGE_TASK_EXIT (6003) - Task exit notification
encode_body(?MESSAGE_TASK_EXIT, Resp) ->
    encode_task_exit_msg(Resp);

%% REQUEST_FED_INFO (2049) - Federation info request
encode_body(?REQUEST_FED_INFO, Req) ->
    encode_fed_info_request(Req);

%% REQUEST_FEDERATION_SUBMIT (2032) - Cross-cluster job submission
encode_body(?REQUEST_FEDERATION_SUBMIT, Req) ->
    encode_federation_submit_request(Req);

%% REQUEST_FEDERATION_JOB_STATUS (2034) - Query job on remote cluster
encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req) ->
    encode_federation_job_status_request(Req);

%% REQUEST_FEDERATION_JOB_CANCEL (2036) - Cancel job on remote cluster
encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req) ->
    encode_federation_job_cancel_request(Req);

%% RESPONSE_FED_INFO (2050) - Federation info response
encode_body(?RESPONSE_FED_INFO, Resp) ->
    encode_fed_info_response(Resp);

%% RESPONSE_FEDERATION_SUBMIT (2033) - Cross-cluster job response
encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp) ->
    encode_federation_submit_response(Resp);

%% RESPONSE_FEDERATION_JOB_STATUS (2035) - Remote job status
encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp) ->
    encode_federation_job_status_response(Resp);

%% RESPONSE_FEDERATION_JOB_CANCEL (2037) - Remote cancel response
encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp) ->
    encode_federation_job_cancel_response(Resp);

%% REQUEST_UPDATE_FEDERATION (2064) - Federation update request
encode_body(?REQUEST_UPDATE_FEDERATION, Req) ->
    encode_update_federation_request(Req);

%% RESPONSE_UPDATE_FEDERATION (2065) - Federation update response
encode_body(?RESPONSE_UPDATE_FEDERATION, Resp) ->
    encode_update_federation_response(Resp);

%% MSG_FED_JOB_SUBMIT (2070) - Sibling job creation
encode_body(?MSG_FED_JOB_SUBMIT, Msg) ->
    encode_fed_job_submit_msg(Msg);

%% MSG_FED_JOB_STARTED (2071) - Job started notification
encode_body(?MSG_FED_JOB_STARTED, Msg) ->
    encode_fed_job_started_msg(Msg);

%% MSG_FED_SIBLING_REVOKE (2072) - Sibling revocation
encode_body(?MSG_FED_SIBLING_REVOKE, Msg) ->
    encode_fed_sibling_revoke_msg(Msg);

%% MSG_FED_JOB_COMPLETED (2073) - Job completed notification
encode_body(?MSG_FED_JOB_COMPLETED, Msg) ->
    encode_fed_job_completed_msg(Msg);

%% MSG_FED_JOB_FAILED (2074) - Job failed notification
encode_body(?MSG_FED_JOB_FAILED, Msg) ->
    encode_fed_job_failed_msg(Msg);

%% Raw binary passthrough
encode_body(_MsgType, Binary) when is_binary(Binary) ->
    {ok, Binary};

encode_body(MsgType, Body) ->
    {error, {unsupported_message_type, MsgType, Body}}.

%%%===================================================================
%%% Message Type Helpers
%%%===================================================================

%% @doc Return human-readable name for message type.
-spec message_type_name(non_neg_integer()) -> atom().
message_type_name(?REQUEST_NODE_REGISTRATION_STATUS) -> request_node_registration_status;
message_type_name(?MESSAGE_NODE_REGISTRATION_STATUS) -> message_node_registration_status;
message_type_name(?REQUEST_RECONFIGURE) -> request_reconfigure;
message_type_name(?REQUEST_RECONFIGURE_WITH_CONFIG) -> request_reconfigure_with_config;
message_type_name(?REQUEST_SHUTDOWN) -> request_shutdown;
message_type_name(?REQUEST_PING) -> request_ping;
message_type_name(?REQUEST_BUILD_INFO) -> request_build_info;
message_type_name(?REQUEST_JOB_INFO) -> request_job_info;
message_type_name(?RESPONSE_JOB_INFO) -> response_job_info;
message_type_name(?REQUEST_NODE_INFO) -> request_node_info;
message_type_name(?RESPONSE_NODE_INFO) -> response_node_info;
message_type_name(?REQUEST_PARTITION_INFO) -> request_partition_info;
message_type_name(?RESPONSE_PARTITION_INFO) -> response_partition_info;
message_type_name(?REQUEST_RESOURCE_ALLOCATION) -> request_resource_allocation;
message_type_name(?RESPONSE_RESOURCE_ALLOCATION) -> response_resource_allocation;
message_type_name(?REQUEST_JOB_ALLOCATION_INFO) -> request_job_allocation_info;
message_type_name(?RESPONSE_JOB_ALLOCATION_INFO) -> response_job_allocation_info;
message_type_name(?REQUEST_KILL_TIMELIMIT) -> request_kill_timelimit;
message_type_name(?REQUEST_SUBMIT_BATCH_JOB) -> request_submit_batch_job;
message_type_name(?RESPONSE_SUBMIT_BATCH_JOB) -> response_submit_batch_job;
message_type_name(?REQUEST_CANCEL_JOB) -> request_cancel_job;
message_type_name(?REQUEST_KILL_JOB) -> request_kill_job;
message_type_name(?REQUEST_UPDATE_JOB) -> request_update_job;
message_type_name(?REQUEST_JOB_WILL_RUN) -> request_job_will_run;
message_type_name(?RESPONSE_JOB_WILL_RUN) -> response_job_will_run;
message_type_name(?REQUEST_JOB_READY) -> request_job_ready;
message_type_name(?RESPONSE_JOB_READY) -> response_job_ready;
message_type_name(?REQUEST_JOB_STEP_CREATE) -> request_job_step_create;
message_type_name(?RESPONSE_JOB_STEP_CREATE) -> response_job_step_create;
message_type_name(?RESPONSE_SLURM_RC) -> response_slurm_rc;
message_type_name(?REQUEST_RESERVATION_INFO) -> request_reservation_info;
message_type_name(?RESPONSE_RESERVATION_INFO) -> response_reservation_info;
message_type_name(?REQUEST_LICENSE_INFO) -> request_license_info;
message_type_name(?RESPONSE_LICENSE_INFO) -> response_license_info;
message_type_name(?REQUEST_TOPO_INFO) -> request_topo_info;
message_type_name(?RESPONSE_TOPO_INFO) -> response_topo_info;
message_type_name(?REQUEST_FRONT_END_INFO) -> request_front_end_info;
message_type_name(?RESPONSE_FRONT_END_INFO) -> response_front_end_info;
message_type_name(?REQUEST_BURST_BUFFER_INFO) -> request_burst_buffer_info;
message_type_name(?RESPONSE_BURST_BUFFER_INFO) -> response_burst_buffer_info;
message_type_name(?RESPONSE_BUILD_INFO) -> response_build_info;
message_type_name(?REQUEST_CONFIG_INFO) -> request_config_info;
message_type_name(?RESPONSE_CONFIG_INFO) -> response_config_info;
message_type_name(?REQUEST_STATS_INFO) -> request_stats_info;
message_type_name(?RESPONSE_STATS_INFO) -> response_stats_info;
message_type_name(?REQUEST_FED_INFO) -> request_fed_info;
message_type_name(?RESPONSE_FED_INFO) -> response_fed_info;
message_type_name(?REQUEST_FEDERATION_SUBMIT) -> request_federation_submit;
message_type_name(?RESPONSE_FEDERATION_SUBMIT) -> response_federation_submit;
message_type_name(?REQUEST_FEDERATION_JOB_STATUS) -> request_federation_job_status;
message_type_name(?RESPONSE_FEDERATION_JOB_STATUS) -> response_federation_job_status;
message_type_name(?REQUEST_FEDERATION_JOB_CANCEL) -> request_federation_job_cancel;
message_type_name(?RESPONSE_FEDERATION_JOB_CANCEL) -> response_federation_job_cancel;
message_type_name(?REQUEST_UPDATE_FEDERATION) -> request_update_federation;
message_type_name(?RESPONSE_UPDATE_FEDERATION) -> response_update_federation;
message_type_name(Type) -> {unknown, Type}.

%% @doc Check if message type is a request.
%%
%% SLURM uses specific message type codes for requests. These are not
%% strictly odd/even - the pattern varies by category. We check against
%% known request types.
-spec is_request(non_neg_integer()) -> boolean().
is_request(?REQUEST_NODE_REGISTRATION_STATUS) -> true;
is_request(?REQUEST_RECONFIGURE) -> true;
is_request(?REQUEST_RECONFIGURE_WITH_CONFIG) -> true;
is_request(?REQUEST_SHUTDOWN) -> true;
is_request(?REQUEST_PING) -> true;
is_request(?REQUEST_BUILD_INFO) -> true;
is_request(?REQUEST_JOB_INFO) -> true;
is_request(?REQUEST_JOB_INFO_SINGLE) -> true;
is_request(?REQUEST_NODE_INFO) -> true;
is_request(?REQUEST_PARTITION_INFO) -> true;
is_request(?REQUEST_RESOURCE_ALLOCATION) -> true;
is_request(?REQUEST_SUBMIT_BATCH_JOB) -> true;
is_request(?REQUEST_BATCH_JOB_LAUNCH) -> true;
is_request(?REQUEST_CANCEL_JOB) -> true;
is_request(?REQUEST_UPDATE_JOB) -> true;
is_request(?REQUEST_JOB_STEP_CREATE) -> true;
is_request(?REQUEST_JOB_STEP_INFO) -> true;
is_request(?REQUEST_STEP_COMPLETE) -> true;
is_request(?REQUEST_LAUNCH_TASKS) -> true;
is_request(?REQUEST_SIGNAL_TASKS) -> true;
is_request(?REQUEST_TERMINATE_TASKS) -> true;
is_request(?REQUEST_KILL_JOB) -> true;
is_request(?REQUEST_RESERVATION_INFO) -> true;
is_request(?REQUEST_LICENSE_INFO) -> true;
is_request(?REQUEST_TOPO_INFO) -> true;
is_request(?REQUEST_FRONT_END_INFO) -> true;
is_request(?REQUEST_BURST_BUFFER_INFO) -> true;
is_request(?REQUEST_CONFIG_INFO) -> true;
is_request(?REQUEST_STATS_INFO) -> true;
is_request(?REQUEST_FED_INFO) -> true;
is_request(?REQUEST_FEDERATION_SUBMIT) -> true;
is_request(?REQUEST_FEDERATION_JOB_STATUS) -> true;
is_request(?REQUEST_FEDERATION_JOB_CANCEL) -> true;
is_request(?REQUEST_UPDATE_FEDERATION) -> true;
%% Fallback: check if it starts with REQUEST_ pattern (1xxx, 2xxx odd, 4xxx, 5xxx)
is_request(Type) when Type >= 1001, Type =< 1029 -> true;
is_request(_) -> false.

%% @doc Check if message type is a response.
%%
%% SLURM uses specific message type codes for responses. These are typically
%% paired with request types (request + 1 = response in many cases).
-spec is_response(non_neg_integer()) -> boolean().
is_response(?MESSAGE_NODE_REGISTRATION_STATUS) -> true;
is_response(?RESPONSE_BUILD_INFO) -> true;
is_response(?RESPONSE_JOB_INFO) -> true;
is_response(?RESPONSE_NODE_INFO) -> true;
is_response(?RESPONSE_PARTITION_INFO) -> true;
is_response(?RESPONSE_RESOURCE_ALLOCATION) -> true;
is_response(?RESPONSE_JOB_ALLOCATION_INFO) -> true;
is_response(?RESPONSE_SUBMIT_BATCH_JOB) -> true;
is_response(?RESPONSE_CANCEL_JOB_STEP) -> true;
is_response(?RESPONSE_JOB_STEP_CREATE) -> true;
is_response(?RESPONSE_JOB_STEP_INFO) -> true;
is_response(?RESPONSE_STEP_LAYOUT) -> true;
is_response(?RESPONSE_LAUNCH_TASKS) -> true;
is_response(?RESPONSE_JOB_READY) -> true;
is_response(?RESPONSE_SLURM_RC) -> true;
is_response(?RESPONSE_SLURM_RC_MSG) -> true;
is_response(?RESPONSE_RESERVATION_INFO) -> true;
is_response(?RESPONSE_LICENSE_INFO) -> true;
is_response(?RESPONSE_TOPO_INFO) -> true;
is_response(?RESPONSE_FRONT_END_INFO) -> true;
is_response(?RESPONSE_BURST_BUFFER_INFO) -> true;
is_response(?RESPONSE_CONFIG_INFO) -> true;
is_response(?RESPONSE_STATS_INFO) -> true;
is_response(?RESPONSE_FED_INFO) -> true;
is_response(?RESPONSE_FEDERATION_SUBMIT) -> true;
is_response(?RESPONSE_FEDERATION_JOB_STATUS) -> true;
is_response(?RESPONSE_FEDERATION_JOB_CANCEL) -> true;
is_response(?RESPONSE_UPDATE_FEDERATION) -> true;
is_response(_) -> false.

%%%===================================================================
%%% Request Decoders
%%%===================================================================

%% Decode REQUEST_NODE_REGISTRATION_STATUS (1001)
decode_node_registration_request(<<>>) ->
    {ok, #node_registration_request{status_only = false}};
decode_node_registration_request(<<StatusOnly:8, _Rest/binary>>) ->
    {ok, #node_registration_request{status_only = StatusOnly =/= 0}};
decode_node_registration_request(_) ->
    {ok, #node_registration_request{status_only = false}}.

%% Decode REQUEST_JOB_INFO (2003)
decode_job_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, JobId:32/big, UserId:32/big, _Rest/binary>> ->
            {ok, #job_info_request{
                show_flags = ShowFlags,
                job_id = JobId,
                user_id = UserId
            }};
        <<ShowFlags:32/big, JobId:32/big>> ->
            {ok, #job_info_request{
                show_flags = ShowFlags,
                job_id = JobId,
                user_id = 0
            }};
        <<ShowFlags:32/big>> ->
            {ok, #job_info_request{
                show_flags = ShowFlags,
                job_id = 0,
                user_id = 0
            }};
        <<>> ->
            {ok, #job_info_request{}};
        _ ->
            {error, invalid_job_info_request}
    end.

%% Decode REQUEST_RESOURCE_ALLOCATION (4001) - srun interactive job
%% Similar to batch job but for interactive execution
%% Includes callback port where srun listens for job updates
decode_resource_allocation_request(Binary) ->
    try
        %% Log first 200 bytes for debugging callback port extraction
        DumpSize = min(200, byte_size(Binary)),
        <<DumpBytes:DumpSize/binary, _/binary>> = Binary,
        HexDump = list_to_binary([[io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(DumpBytes)]]),
        lager:info("Resource allocation request first ~p bytes: ~s", [DumpSize, HexDump]),

        %% Search for ALL uint16 values in ephemeral port range (30000-65000)
        PortCandidates = find_all_port_candidates(Binary, 0, []),
        %% Filter to likely callback ports (high ephemeral range used by srun)
        HighPorts = [{O, P} || {O, P} <- PortCandidates, P >= 40000, P =< 65000],
        lager:info("High port candidates (40000-65000): ~p", [HighPorts]),
        lager:info("Message body size: ~p bytes", [byte_size(Binary)]),

        %% Resource allocation is similar to batch job in format
        %% Reuse similar scanning approach
        Name = find_job_name(Binary),
        WorkDir = find_work_dir(Binary),
        {UserId, GroupId} = extract_uid_gid(Binary),

        %% Extract resource requirements
        {ProtoNodes, ProtoCpus} = extract_resources_from_protocol(Binary),
        MinNodes = case ProtoNodes of
            N when N > 0, N < 16#FFFFFFFE -> N;
            _ -> 1
        end,
        MinCpus = case ProtoCpus of
            C when C > 0, C < 16#FFFFFFFE -> C;
            _ -> 1
        end,

        %% Extract srun callback info - port is typically at a known offset
        %% In SLURM protocol, alloc_resp_port is a uint16 early in the message
        {AllocRespPort, OtherPort, SrunPid} = extract_srun_callback_info(Binary),

        lager:debug("srun callback info: port=~p, other_port=~p, pid=~p",
                    [AllocRespPort, OtherPort, SrunPid]),

        Req = #resource_allocation_request{
            name = Name,
            partition = <<>>,
            work_dir = WorkDir,
            min_nodes = MinNodes,
            max_nodes = MinNodes,
            min_cpus = MinCpus,
            num_tasks = MinCpus,
            cpus_per_task = 1,
            time_limit = 3600,  % Default 1 hour
            priority = 0,
            user_id = UserId,
            group_id = GroupId,
            alloc_resp_port = AllocRespPort,
            other_port = OtherPort,
            srun_pid = SrunPid,
            resp_host = <<>>  % Will be filled from connection info
        },
        {ok, Req}
    catch
        _:Reason ->
            {error, {resource_allocation_decode_failed, Reason}}
    end.

%% Extract srun callback information from the request binary
%% The callback port is where srun listens for job state updates
%%
%% SLURM job_desc_msg_t packing order (from slurm_pack_job_desc_msg):
%% Protocol version 0x2600 (SLURM 23.11):
%% 1. site_factor (uint32) - SLURM 21.08+
%% 2. account (packstr - uint32 len + string)
%% 3. acctg_freq (packstr)
%% 4. admin_comment (packstr)
%% 5. alloc_node (packstr)
%% 6. alloc_resp_port (uint16) <-- THIS IS WHAT WE WANT
%% 7. alloc_sid (uint32)
%% ...
%%
%% Note: Looking at actual traffic, there appear to be 5 packstrs before the port,
%% not 4 as documented. This may be due to version differences.
extract_srun_callback_info(Binary) ->
    try
        %% Skip site_factor (4 bytes)
        <<SiteFactor:32/big, Rest1/binary>> = Binary,
        lager:debug("Callback info: site_factor=~p (~.16B)", [SiteFactor, SiteFactor]),

        %% Skip packstrs until we find a non-packstr pattern
        %% In SLURM 23.11, there are 5 packstrs before the port:
        %% account, acctg_freq, admin_comment, alloc_node, + one more
        {Rest2, Str1} = skip_packstr(Rest1),
        {Rest3, Str2} = skip_packstr(Rest2),
        {Rest4, Str3} = skip_packstr(Rest3),
        {Rest5, Str4} = skip_packstr(Rest4),
        {Rest6, Str5} = skip_packstr(Rest5),

        lager:debug("Skipped 5 packstrs: ~p, ~p, ~p, ~p, ~p",
                    [Str1, Str2, Str3, Str4, Str5]),

        %% Now we should be at alloc_resp_port (uint16) and alloc_sid (uint32)
        <<AllocRespPort:16/big, AllocSid:32/big, _/binary>> = Rest6,

        lager:info("Parsed srun callback: port=~p (0x~.16B), sid=~p",
                   [AllocRespPort, AllocRespPort, AllocSid]),

        %% Check for NO_VAL16 (0xFFFE = 65534) which means no callback port
        ActualPort = case AllocRespPort of
            16#FFFE -> 0;  % NO_VAL16 means no port
            16#FFFF -> 0;  % Also treat 0xFFFF as no port
            P when P =< 1024 -> 0;  % Invalid low port
            P -> P
        end,

        {ActualPort, 0, AllocSid}
    catch
        _:Reason ->
            lager:warning("Failed to parse srun callback info: ~p, trying 4 packstrs",
                          [Reason]),
            %% Try with 4 packstrs (older format)
            try_parse_4_packstrs(Binary)
    end.

%% Try to find callback port using direct offset reading
%% Based on empirical observation, the alloc_resp_port field is at offset 341
%% in the job_desc_msg for SLURM 23.11/24.11 protocol version 0x2600
try_parse_4_packstrs(Binary) when byte_size(Binary) >= 348 ->
    %% Scan for callback port at common offsets
    %% The exact offset varies based on variable-length fields before it (packstrs)
    %% Body sizes range from ~470 to ~490 bytes, so port is at offsets 340-360
    Candidates = scan_for_callback_port_at_offsets(Binary, lists:seq(335, 370)),
    lager:info("Port candidates: ~p", [Candidates]),
    %% Filter to ephemeral ports (32768-60999) which are used for the callback listener
    EphemeralCandidates = [{O, P} || {O, P} <- Candidates, P >= 32768, P =< 60999],
    lager:info("Ephemeral port candidates (32768-60999): ~p", [EphemeralCandidates]),
    %% Further filter: reject ports that look like structure padding
    %% Real ephemeral ports are randomly assigned by the OS and don't have regular patterns
    %% Suspicious patterns: low byte is 0x00 or port divisible by 256
    RealPortCandidates = [{O, P} || {O, P} <- EphemeralCandidates,
                                    (P rem 256) =/= 0,        % Not divisible by 256
                                    (P rem 256) =/= 192],     % Not 0xC0 low byte
    lager:info("Real port candidates (filtered): ~p", [RealPortCandidates]),
    %% Prefer the candidate at the HIGHEST offset from the filtered list
    %% The port field comes after variable-length strings, so higher offset = correct position
    case RealPortCandidates of
        [_ | _] ->
            %% Sort by offset descending and take the highest
            Sorted = lists:reverse(lists:keysort(1, RealPortCandidates)),
            {Offset, Port} = hd(Sorted),
            lager:info("Found callback port at offset ~p: ~p (highest filtered)", [Offset, Port]),
            {Port, 0, 0};
        [] ->
            %% Fall back to any ephemeral port (highest offset)
            case EphemeralCandidates of
                [_ | _] ->
                    Sorted2 = lists:reverse(lists:keysort(1, EphemeralCandidates)),
                    {Offset2, Port2} = hd(Sorted2),
                    lager:info("Found callback port at offset ~p: ~p (fallback highest)", [Offset2, Port2]),
                    {Port2, 0, 0};
                [] ->
                    lager:warning("No callback port found in known offsets"),
                    scan_for_callback_port(Binary)
            end
    end;
try_parse_4_packstrs(Binary) ->
    %% Message too short, try scanning
    lager:warning("Message too short (~p bytes), trying scan", [byte_size(Binary)]),
    scan_for_callback_port(Binary).


%% Check specific offsets for callback port
scan_for_callback_port_at_offsets(Binary, Offsets) ->
    [{O, P} || O <- Offsets,
               byte_size(Binary) > O + 2,
               begin
                   <<_:O/binary, P:16/big, _/binary>> = Binary,
                   P >= 30000 andalso P =< 60000
               end].

%% Scan for callback port as fallback
scan_for_callback_port(Binary) ->
    Candidates = find_all_port_candidates(Binary, 0, []),
    %% Filter to likely callback ports (ephemeral range, offset 300-400)
    EphemeralPorts = [{O, P} || {O, P} <- Candidates,
                                P >= 32000, P =< 60000,
                                O >= 300, O =< 400],
    case EphemeralPorts of
        [{_, Port} | _] ->
            lager:info("Scan found callback port: ~p", [Port]),
            {Port, 0, 0};
        [] ->
            lager:warning("No callback port found"),
            {0, 0, 0}
    end.

%% Skip a length-prefixed string (packstr format: uint32 length + string bytes)
%% Returns {RemainingBinary, StringValue}
skip_packstr(<<16#FFFFFFFF:32/big, Rest/binary>>) ->
    %% NO_VAL / null string
    {Rest, <<>>};
skip_packstr(<<Len:32/big, Rest/binary>>) when Len < 16#FFFFFFFE ->
    case Rest of
        <<Str:Len/binary, Remaining/binary>> ->
            {Remaining, Str};
        _ ->
            %% Not enough data
            throw({incomplete_packstr, Len, byte_size(Rest)})
    end;
skip_packstr(<<Len:32/big, _/binary>>) ->
    %% Invalid length (NO_VAL or similar)
    throw({invalid_packstr_len, Len});
skip_packstr(_) ->
    throw(insufficient_data).

%% Find all potential port values in binary for debugging
%% Returns list of {Offset, Port} tuples for ports in typical ephemeral range
find_all_port_candidates(Binary, Offset, Acc) when Offset + 2 =< byte_size(Binary) ->
    <<_:Offset/binary, Port:16/big, _/binary>> = Binary,
    NewAcc = case Port of
        P when P >= 30000, P =< 60000 -> [{Offset, P} | Acc];  % Ephemeral port range
        _ -> Acc
    end,
    find_all_port_candidates(Binary, Offset + 1, NewAcc);
find_all_port_candidates(_Binary, _Offset, Acc) ->
    lists:reverse(Acc).

%% Decode REQUEST_SUBMIT_BATCH_JOB (4003) - Pattern-based version
%% SLURM batch job format has many optional fields with SLURM_NO_VAL (0xFFFFFFFE)
%% sentinels. Rather than decode every field, we scan for key patterns.
decode_batch_job_request(Binary) ->
    try
        decode_batch_job_request_scan(Binary)
    catch
        _:Reason ->
            {error, {batch_job_decode_failed, Reason}}
    end.

%% Pattern-based decoder that scans for key fields in the message
decode_batch_job_request_scan(Binary) ->
    %% Find job name - try protocol binary first, then SBATCH directive, then env var
    ProtoName = find_job_name(Binary),

    %% Find the script by looking for "#!/" shebang
    Script = find_script(Binary),

    %% Try to extract job name from #SBATCH --job-name directive in script
    ScriptName = extract_job_name_from_script(Script),
    %% Also try SBATCH_JOB_NAME environment variable in the binary
    EnvName = extract_env_str(Binary, <<"SBATCH_JOB_NAME=">>),
    %% Also try --job-name= in command line args embedded in binary
    CmdLineName = extract_cmdline_str(Binary, <<"--job-name=">>),
    %% Priority: env var > cmd line > script directive > protocol heuristic
    %% (Protocol binary heuristic scanning is unreliable, use as last resort)
    Name = first_nonempty([EnvName, CmdLineName, ScriptName, ProtoName, <<"unknown">>]),

    %% Debug: Log extracted script info
    ScriptLen = byte_size(Script),
    ScriptPreview = case ScriptLen > 80 of
        true -> <<(binary:part(Script, 0, 80))/binary, "...">>;
        false -> Script
    end,
    lager:info("Protocol decode: job=~s, script_size=~p, script_preview=~p",
               [Name, ScriptLen, ScriptPreview]),

    %% Find working directory - look for path patterns
    WorkDir = find_work_dir(Binary),

    %% Extract user/group IDs from known offset patterns
    {UserId, GroupId} = extract_uid_gid(Binary),

    %% Extract time limit: script directive > command line > env var > binary flag > default
    ScriptTimeLimit = extract_time_limit(Script),
    TimeLimit = case ScriptTimeLimit of
        300 ->
            %% Default value - try other sources
            CmdLineTimeLimit = extract_time_limit_from_cmdline(Binary),
            EnvTimeLimit = extract_time_from_env(Binary),
            DirectTimeLimit = extract_time_from_binary_flags(Binary),
            first_nonzero([CmdLineTimeLimit, EnvTimeLimit, DirectTimeLimit, 300]);
        _ -> ScriptTimeLimit
    end,

    %% Extract node/task/cpu/memory counts from script SBATCH directives (most reliable)
    {ScriptNodes, ScriptTasks, ScriptCpus, ScriptMemMb} = extract_resources(Script),

    %% Also extract from SLURM environment variables in the protocol binary.
    %% When users pass flags on the command line (e.g., sbatch --cpus-per-task=16 --wrap),
    %% the values are NOT in the script but ARE in SLURM_* env vars within the binary.
    {EnvNodes, EnvTasks, EnvCpus, EnvMemMb} = extract_resources_from_env(Binary),

    %% Prefer script values, then env values, then protocol scanning, then defaults.
    %% Protocol binary scanning (last resort, unreliable)
    {ProtoNodes, ProtoCpus} = extract_resources_from_protocol(Binary),

    %% Also search the embedded command line for resource flags
    {CmdNodes, CmdTasks, CmdCpus, CmdMemMb} = extract_resources_from_cmdline(Binary),
    lager:debug("Resource resolution: script={~p,~p,~p,~p} env={~p,~p,~p,~p} cmd={~p,~p,~p,~p} proto={~p,~p}",
               [ScriptNodes, ScriptTasks, ScriptCpus, ScriptMemMb,
                EnvNodes, EnvTasks, EnvCpus, EnvMemMb,
                CmdNodes, CmdTasks, CmdCpus, CmdMemMb,
                ProtoNodes, ProtoCpus]),

    MinNodes = first_nonzero([ScriptNodes, EnvNodes, CmdNodes, 1]),

    %% CpusPerTask from explicit --cpus-per-task / -c flags
    CpusPerTask = first_nonzero([ScriptCpus, EnvCpus, CmdCpus]),
    NumTasks = first_nonzero([ScriptTasks, EnvTasks, CmdTasks, 1]),

    %% Calculate total CPUs needed:
    %% - If explicit --cpus-per-task given: total = num_tasks * cpus_per_task
    %% - If only --ntasks given: total = num_tasks (1 CPU per task default)
    %% - If neither: fall back to protocol scanner (unreliable) or 1
    MinCpus = case CpusPerTask of
        0 when NumTasks > 1 ->
            %% --ntasks specified but no --cpus-per-task: 1 CPU per task
            NumTasks;
        0 ->
            %% Neither ntasks nor cpus-per-task specified: try protocol scanner
            case ProtoCpus of
                PC when PC > 0, PC < 16#FFFFFFFE, PC =< 64 -> PC;
                _ -> 1
            end;
        _ ->
            %% Explicit --cpus-per-task: total = tasks * cpus_per_task
            max(NumTasks * CpusPerTask, CpusPerTask)
    end,

    %% Memory: use script value if specified, then env/cmd, then binary scan, otherwise 0
    BinaryMemMb = case first_nonzero([ScriptMemMb, EnvMemMb, CmdMemMb]) of
        0 -> extract_mem_from_binary_flags(Binary);
        _ -> 0
    end,
    MinMemPerNode = first_nonzero([ScriptMemMb, EnvMemMb, CmdMemMb, BinaryMemMb]),

    lager:debug("Final resource values: min_nodes=~p min_cpus=~p num_tasks=~p cpus_per_task=~p min_mem=~p",
               [MinNodes, MinCpus, NumTasks, CpusPerTask, MinMemPerNode]),

    %% Find stdout/stderr paths from the protocol message or script
    StdOut = find_std_out(Binary, Script, WorkDir),
    StdErr = find_std_err(Binary, Script),

    %% Extract partition from env var, script directive, or command line
    Partition = first_nonempty([
        extract_env_str(Binary, <<"SBATCH_PARTITION=">>),
        extract_sbatch_directive_value(Script, <<"--partition=">>),
        extract_sbatch_directive_value(Script, <<"-p ">>),
        extract_cmdline_str(Binary, <<"--partition=">>)
    ]),

    Req = #batch_job_request{
        account = <<>>,
        acctg_freq = <<>>,
        admin_comment = <<>>,
        alloc_node = <<>>,
        alloc_resp_port = 0,
        alloc_sid = 0,
        argc = 0,
        argv = [],
        name = Name,
        partition = Partition,
        script = Script,
        work_dir = WorkDir,
        min_nodes = MinNodes,
        max_nodes = MinNodes,
        min_cpus = MinCpus,
        min_mem_per_node = MinMemPerNode,
        num_tasks = NumTasks,
        cpus_per_task = max(1, CpusPerTask),
        time_limit = TimeLimit,
        priority = extract_priority_from_binary(Binary),
        dependency = extract_dependency_from_binary(Binary),
        array_inx = extract_array_spec(Binary, Script),
        user_id = UserId,
        group_id = GroupId,
        std_out = StdOut,
        std_err = StdErr
    },
    {ok, Req}.

%% Find job name by scanning for length-prefixed string in the expected region
find_job_name(Binary) when byte_size(Binary) > 70 ->
    %% Scan bytes 50-70 for a reasonable length field followed by printable chars
    find_job_name_scan(Binary, 50);
find_job_name(_) ->
    <<"unknown">>.

find_job_name_scan(_Binary, Offset) when Offset > 70 ->
    <<"unknown">>;
find_job_name_scan(Binary, Offset) when byte_size(Binary) > Offset + 4 ->
    <<_:Offset/binary, Len:32/big, Rest/binary>> = Binary,
    if
        Len > 0, Len < 256, byte_size(Rest) >= Len ->
            <<Str:Len/binary, _/binary>> = Rest,
            case is_printable_name(Str) of
                true ->
                    %% Strip trailing null if present
                    strip_null(Str);
                false ->
                    find_job_name_scan(Binary, Offset + 1)
            end;
        true ->
            find_job_name_scan(Binary, Offset + 1)
    end;
find_job_name_scan(_, _) ->
    <<"unknown">>.

%% Find script by looking for shebang pattern
find_script(Binary) ->
    case binary:match(Binary, <<"#!/">>) of
        {Start, _} ->
            %% Script runs from shebang to some delimiter
            %% Check for length prefix 4 bytes before
            extract_script_content(Binary, Start);
        nomatch ->
            <<>>
    end.

extract_script_content(Binary, ShebangOffset) ->
    %% The script might have a 4-byte length prefix before it
    %% Or it might just run to the end of a section
    <<_:ShebangOffset/binary, ScriptRest/binary>> = Binary,
    %% Find the end of the script - typically null terminated or ends with a pattern
    case binary:match(ScriptRest, <<0, 0, 0>>) of
        {EndOffset, _} when EndOffset > 10 ->
            <<Script:EndOffset/binary, _/binary>> = ScriptRest,
            Script;
        _ ->
            %% Take reasonable chunk as script
            ScriptLen = min(2048, byte_size(ScriptRest)),
            <<Script:ScriptLen/binary, _/binary>> = ScriptRest,
            %% Trim at first triple-null if present
            trim_script(Script)
    end.

trim_script(Script) ->
    case binary:match(Script, <<0, 0>>) of
        {Pos, _} when Pos > 10 ->
            <<Trimmed:Pos/binary, _/binary>> = Script,
            Trimmed;
        _ ->
            Script
    end.

%% Find working directory
find_work_dir(Binary) ->
    %% Look for PWD= in environment or /jobs or similar path
    case binary:match(Binary, <<"PWD=">>) of
        {Start, 4} ->
            <<_:Start/binary, "PWD=", PathRest/binary>> = Binary,
            extract_path(PathRest);
        nomatch ->
            <<"/tmp">>
    end.

extract_path(Binary) ->
    %% Extract until null or non-path character
    extract_path(Binary, 0).

extract_path(Binary, Pos) when Pos < byte_size(Binary) ->
    case binary:at(Binary, Pos) of
        0 -> <<Path:Pos/binary, _/binary>> = Binary, Path;
        C when C < 32 -> <<Path:Pos/binary, _/binary>> = Binary, Path;
        _ -> extract_path(Binary, Pos + 1)
    end;
extract_path(Binary, Pos) ->
    <<Path:Pos/binary, _/binary>> = Binary,
    Path.

%% Extract priority from the SLURM job_desc_msg_t binary.
%% Walks the packed structure to skip variable-length strings, then reads
%% priority at a fixed offset (51 bytes) after the name field.
%%
%% Field order: site_factor:32, batch_features:str, cluster_features:str,
%%   clusters:str, contiguous:16, container:str, core_spec:16, task_dist:32,
%%   kill_on_node_fail:16, features:str, fed_siblings_active:64,
%%   fed_siblings_viable:64, job_id:32, job_id_str:str, name:str,
%%   [51 bytes of fixed fields], priority:32
extract_priority_from_binary(Binary) when byte_size(Binary) >= 200 ->
    try
        %% Walk to end of name field (same as update decoder)
        <<_SiteFactor:32/big, R0/binary>> = Binary,
        {R1, _} = skip_packstr(R0),   % batch_features
        {R2, _} = skip_packstr(R1),   % cluster_features
        {R3, _} = skip_packstr(R2),   % clusters
        <<_Contiguous:16/big, R4/binary>> = R3,
        {R5, _} = skip_packstr(R4),   % container
        <<_CoreSpec:16/big, _TaskDist:32/big, _KillOnFail:16/big, R6/binary>> = R5,
        {R7, _} = skip_packstr(R6),   % features
        <<_FedActive:64/big, _FedViable:64/big, R8/binary>> = R7,
        <<_JobId:32/big, R9/binary>> = R8,
        {R10, _} = skip_packstr(R9),  % job_id_str
        {R11, _} = skip_packstr(R10), % name
        %% After name: 51 bytes of fixed fields, then priority:32
        case R11 of
            <<_FixedFields:51/binary, Priority:32/big, _/binary>> ->
                Priority;
            _ ->
                ?SLURM_NO_VAL
        end
    catch
        _:_ -> ?SLURM_NO_VAL
    end;
extract_priority_from_binary(_) ->
    ?SLURM_NO_VAL.

%% Extract dependency string from the SLURM job_desc_msg_t binary.
%% Scans for known dependency prefixes (afterok:, afterany:, afternotok:,
%% aftercorr:, singleton) as packed strings in the binary.
extract_dependency_from_binary(Binary) ->
    %% Try to find dependency patterns in the binary
    %% Dependencies are packstr (4-byte length + content) containing text like
    %% "afterok:1", "afterany:2", "singleton", "afterok:1,afternotok:2"
    Patterns = [<<"afterok:">>, <<"afterany:">>, <<"afternotok:">>,
                <<"aftercorr:">>, <<"singleton">>],
    extract_dependency_scan(Binary, Patterns).

extract_dependency_scan(_Binary, []) -> <<>>;
extract_dependency_scan(Binary, [Pattern | Rest]) ->
    case binary:match(Binary, Pattern) of
        {Start, _} when Start >= 4 ->
            %% Check if this is a packstr by reading the 4-byte length prefix
            LenOffset = Start - 4,
            <<_:LenOffset/binary, Len:32/big, StrStart/binary>> = Binary,
            if
                Len > 0, Len < 256, byte_size(StrStart) >= Len ->
                    <<DepStr:Len/binary, _/binary>> = StrStart,
                    %% Validate it looks like a dependency string
                    Cleaned = strip_null(DepStr),
                    case Cleaned of
                        <<>> -> extract_dependency_scan(Binary, Rest);
                        _ -> Cleaned
                    end;
                true ->
                    extract_dependency_scan(Binary, Rest)
            end;
        _ ->
            extract_dependency_scan(Binary, Rest)
    end.

%% Extract array spec from script directives, command line, or env vars
extract_array_spec(Binary, Script) ->
    %% Try #SBATCH --array= directive in script
    case extract_sbatch_directive_value(Script, <<"--array=">>) of
        <<>> ->
            %% Try --array= in the embedded command line within the binary
            case extract_cmdline_flag_value(Binary, <<"--array=">>) of
                <<>> ->
                    %% Try SBATCH_ARRAY_INX= env var
                    extract_env_str(Binary, <<"SBATCH_ARRAY_INX=">>);
                Val -> Val
            end;
        Val -> Val
    end.

%% Extract a value from #SBATCH directives in a script.
%% Parses line by line, stops at first non-comment/non-blank line.
%% First directive wins (SLURM behavior).
extract_sbatch_directive_value(Script, Flag) when byte_size(Script) > 0 ->
    Lines = binary:split(Script, <<"\n">>, [global]),
    extract_sbatch_directive_from_lines(Lines, Flag);
extract_sbatch_directive_value(_, _) -> <<>>.

extract_sbatch_directive_from_lines([], _Flag) -> <<>>;
extract_sbatch_directive_from_lines([Line | Rest], Flag) ->
    Trimmed = trim_leading_spaces(Line),
    case Trimmed of
        <<"#SBATCH ", Args/binary>> ->
            TrimmedArgs = trim_leading_spaces(Args),
            FlagLen = byte_size(Flag),
            case TrimmedArgs of
                <<Flag:FlagLen/binary, Val/binary>> ->
                    extract_value_until_whitespace(Val);  % First match wins
                _ ->
                    extract_sbatch_directive_from_lines(Rest, Flag)
            end;
        <<"#!", _/binary>> -> extract_sbatch_directive_from_lines(Rest, Flag);
        <<"#", _/binary>> -> extract_sbatch_directive_from_lines(Rest, Flag);
        <<>> -> extract_sbatch_directive_from_lines(Rest, Flag);
        _ -> <<>>  % Stop at first non-comment line
    end.

%% Extract a value from command-line flags in the binary (e.g., --array=0-3)
extract_cmdline_flag_value(Binary, Flag) ->
    case binary:match(Binary, Flag) of
        {Start, PLen} ->
            <<_:Start/binary, _:PLen/binary, Rest/binary>> = Binary,
            Val = extract_value_until_whitespace(Rest),
            %% Validate it looks like an array spec (digits, dashes, commas, percent)
            case is_valid_array_spec(Val) of
                true -> Val;
                false -> <<>>
            end;
        nomatch -> <<>>
    end.

%% Extract characters until whitespace or null
extract_value_until_whitespace(Binary) ->
    extract_value_until_whitespace(Binary, <<>>).
extract_value_until_whitespace(<<>>, Acc) -> Acc;
extract_value_until_whitespace(<<C, _/binary>>, Acc)
  when C =:= $\s; C =:= $\n; C =:= $\r; C =:= $\t; C =:= 0 -> Acc;
extract_value_until_whitespace(<<C, Rest/binary>>, Acc) ->
    extract_value_until_whitespace(Rest, <<Acc/binary, C>>).

%% Validate array spec contains only digits, dashes, commas, percent, underscores
is_valid_array_spec(<<>>) -> false;
is_valid_array_spec(Bin) -> is_valid_array_spec_chars(Bin).
is_valid_array_spec_chars(<<>>) -> true;
is_valid_array_spec_chars(<<C, Rest/binary>>)
  when C >= $0, C =< $9; C =:= $-; C =:= $,; C =:= $%; C =:= $_ ->
    is_valid_array_spec_chars(Rest);
is_valid_array_spec_chars(_) -> false.

%% Extract a string value from command-line flags in the binary (e.g., --partition=gpu)
extract_cmdline_str(Binary, Flag) ->
    case binary:matches(Binary, Flag) of
        [] -> <<>>;
        Matches ->
            %% Find a match that looks like a command-line arg (preceded by null or space)
            %% rather than a script directive (preceded by #SBATCH)
            extract_cmdline_str_from_matches(Binary, Flag, Matches)
    end.

extract_cmdline_str_from_matches(_Binary, _Flag, []) -> <<>>;
extract_cmdline_str_from_matches(Binary, Flag, [{Start, PLen} | Rest]) ->
    <<_:Start/binary, _:PLen/binary, ValRest/binary>> = Binary,
    Value = extract_value_until_whitespace(ValRest),
    %% Check if this match is inside a #SBATCH directive (skip those)
    IsSbatchDirective = case Start >= 8 of
        true ->
            %% Look backwards for #SBATCH
            CheckStart = max(0, Start - 20),
            CheckLen = Start - CheckStart,
            <<_:CheckStart/binary, Context:CheckLen/binary, _/binary>> = Binary,
            case binary:match(Context, <<"#SBATCH">>) of
                nomatch -> false;
                _ -> true
            end;
        false -> false
    end,
    case IsSbatchDirective of
        true -> extract_cmdline_str_from_matches(Binary, Flag, Rest);
        false -> Value
    end.

%% Extract UID/GID from the message - these are typically near the end in fixed positions
extract_uid_gid(Binary) when byte_size(Binary) > 100 ->
    %% Default to root if we can't find them
    {0, 0};
extract_uid_gid(_) ->
    {0, 0}.

%% Find stdout path - check script directives and protocol message
find_std_out(Binary, Script, WorkDir) ->
    %% First check for --output= or -o in script directives
    case find_sbatch_output_directive(Script) of
        <<>> ->
            %% Check for --output= in the binary message (command-line args)
            case find_output_flag_in_binary(Binary) of
                <<>> ->
                    %% Fall back to scanning for .out path pattern in binary
                    find_output_path_in_binary(Binary, WorkDir);
                Path ->
                    Path
            end;
        Path ->
            Path
    end.

%% Scan binary for std_out packstr field from job_desc_msg_t.
%% The SLURM client packs the --output= path directly as the std_out field.
%% It appears as a packstr: [u32 length][path bytes][null terminator].
%% We look for packstr values containing the path (starting with / and ending
%% with patterns like .out, %j.out, etc) that aren't the default slurm- pattern.
find_output_flag_in_binary(Binary) ->
    find_custom_out_path(Binary, 0).

find_custom_out_path(Binary, Offset) when Offset + 8 < byte_size(Binary) ->
    case Binary of
        <<_:Offset/binary, Len:32/big, Rest/binary>>
                when Len > 4, Len < 512, byte_size(Rest) >= Len ->
            <<Str:Len/binary, _/binary>> = Rest,
            %% Check if this looks like a user-specified output path
            %% Must start with / and NOT be a default slurm- pattern
            case Str of
                <<$/, _/binary>> ->
                    Cleaned = strip_null(Str),
                    IsDefault = case binary:match(Cleaned, <<"slurm-">>) of
                        {_, _} -> true;
                        nomatch -> false
                    end,
                    case IsDefault of
                        true ->
                            find_custom_out_path(Binary, Offset + 1);
                        false ->
                            %% Check it looks like an output path (contains .out or %j)
                            HasOut = binary:match(Cleaned, <<".out">>) =/= nomatch,
                            HasPercent = binary:match(Cleaned, <<"%">>) =/= nomatch,
                            if HasOut orelse HasPercent ->
                                   Cleaned;
                               true ->
                                   find_custom_out_path(Binary, Offset + 1)
                            end
                    end;
                _ ->
                    find_custom_out_path(Binary, Offset + 1)
            end;
        _ ->
            find_custom_out_path(Binary, Offset + 1)
    end;
find_custom_out_path(_, _) ->
    <<>>.


%% Find stderr path - check script directives
find_std_err(Binary, Script) ->
    %% Check for --error= or -e in script directives
    case find_sbatch_error_directive(Script) of
        <<>> ->
            %% Check for .err path pattern in binary
            find_error_path_in_binary(Binary);
        Path ->
            Path
    end.

%% Look for #SBATCH --output= or #SBATCH -o in the script
find_sbatch_output_directive(Script) ->
    case binary:match(Script, <<"--output=">>) of
        {Start, 9} ->
            <<_:Start/binary, "--output=", PathRest/binary>> = Script,
            extract_directive_path(PathRest);
        nomatch ->
            case binary:match(Script, <<"-o ">>) of
                {Start2, 3} ->
                    %% Make sure it's an SBATCH directive, not something else
                    <<Before:Start2/binary, "-o ", PathRest2/binary>> = Script,
                    case binary:match(Before, <<"#SBATCH">>) of
                        nomatch -> <<>>;
                        _ -> extract_directive_path(PathRest2)
                    end;
                nomatch ->
                    <<>>
            end
    end.

%% Look for #SBATCH --error= or #SBATCH -e in the script
find_sbatch_error_directive(Script) ->
    case binary:match(Script, <<"--error=">>) of
        {Start, 8} ->
            <<_:Start/binary, "--error=", PathRest/binary>> = Script,
            extract_directive_path(PathRest);
        nomatch ->
            case binary:match(Script, <<"-e ">>) of
                {Start2, 3} ->
                    <<Before:Start2/binary, "-e ", PathRest2/binary>> = Script,
                    case binary:match(Before, <<"#SBATCH">>) of
                        nomatch -> <<>>;
                        _ -> extract_directive_path(PathRest2)
                    end;
                nomatch ->
                    <<>>
            end
    end.

%% Extract path from directive until newline or space
extract_directive_path(Binary) ->
    extract_directive_path(Binary, 0).

extract_directive_path(Binary, Pos) when Pos < byte_size(Binary) ->
    case binary:at(Binary, Pos) of
        $\n -> <<Path:Pos/binary, _/binary>> = Binary, Path;
        $\r -> <<Path:Pos/binary, _/binary>> = Binary, Path;
        $  -> <<Path:Pos/binary, _/binary>> = Binary, Path;  % space
        0 -> <<Path:Pos/binary, _/binary>> = Binary, Path;
        _ -> extract_directive_path(Binary, Pos + 1)
    end;
extract_directive_path(Binary, Pos) ->
    binary:part(Binary, 0, min(Pos, byte_size(Binary))).

%% Scan binary message for output file path (patterns like /path/to/file.out)
find_output_path_in_binary(Binary, _WorkDir) ->
    %% Look for .out file paths in the protocol message
    %% SLURM protocol puts std_out as a length-prefixed string
    case scan_for_out_path(Binary, 0) of
        <<>> -> <<>>;
        Path -> Path
    end.

%% Scan for .out file path pattern
scan_for_out_path(Binary, Offset) when Offset + 10 < byte_size(Binary) ->
    case Binary of
        <<_:Offset/binary, Len:32/big, Rest/binary>> when Len > 4, Len < 512 ->
            case Rest of
                <<$/, _/binary>> = StrWithRest ->
                    <<OutStr:Len/binary, _/binary>> = StrWithRest,
                    case binary:match(OutStr, <<".out">>) of
                        {_, _} ->
                            %% Found a path starting with / and containing .out
                            strip_null(OutStr);
                        _ ->
                            scan_for_out_path(Binary, Offset + 1)
                    end;
                <<_:Len/binary, _/binary>> ->
                    scan_for_out_path(Binary, Offset + 1);
                _ ->
                    scan_for_out_path(Binary, Offset + 1)
            end;
        _ ->
            scan_for_out_path(Binary, Offset + 1)
    end;
scan_for_out_path(_, _) ->
    <<>>.

%% Scan binary for error file path
find_error_path_in_binary(Binary) ->
    case scan_for_err_path(Binary, 0) of
        <<>> -> <<>>;
        Path -> Path
    end.

%% Scan for .err file path pattern
scan_for_err_path(Binary, Offset) when Offset + 10 < byte_size(Binary) ->
    case Binary of
        <<_:Offset/binary, Len:32/big, Rest/binary>> when Len > 4, Len < 512 ->
            case Rest of
                <<$/, _/binary>> = StrWithRest ->
                    <<ErrStr:Len/binary, _/binary>> = StrWithRest,
                    case binary:match(ErrStr, <<".err">>) of
                        {_, _} ->
                            strip_null(ErrStr);
                        _ ->
                            scan_for_err_path(Binary, Offset + 1)
                    end;
                <<_:Len/binary, _/binary>> ->
                    scan_for_err_path(Binary, Offset + 1);
                _ ->
                    scan_for_err_path(Binary, Offset + 1)
            end;
        _ ->
            scan_for_err_path(Binary, Offset + 1)
    end;
scan_for_err_path(_, _) ->
    <<>>.

%% Extract time limit from embedded sbatch command line in the protocol binary
extract_time_limit_from_cmdline(Binary) ->
    case binary:matches(Binary, <<"sbatch ">>) of
        [] -> 0;
        Matches -> extract_time_from_cmdline_matches(Binary, Matches)
    end.

extract_time_from_cmdline_matches(_Binary, []) -> 0;
extract_time_from_cmdline_matches(Binary, [{Start, _} | Rest]) ->
    <<_:Start/binary, CmdRest/binary>> = Binary,
    CmdLine = extract_until_null(CmdRest, 0),
    case extract_time_from_cmdline_string(CmdLine) of
        0 -> extract_time_from_cmdline_matches(Binary, Rest);
        Found -> Found
    end.

%% Scan a command line string for --time= or -t flags
extract_time_from_cmdline_string(CmdLine) ->
    case binary:match(CmdLine, <<"--time=">>) of
        {Pos, Len} ->
            <<_:Pos/binary, _:Len/binary, TimeRest/binary>> = CmdLine,
            TimeStr = extract_value_until_whitespace(TimeRest),
            parse_time_string(TimeStr);
        nomatch ->
            case binary:match(CmdLine, <<"-t ">>) of
                {Pos2, Len2} ->
                    <<_:Pos2/binary, _:Len2/binary, TimeRest2/binary>> = CmdLine,
                    Trimmed = trim_leading_spaces(TimeRest2),
                    TimeStr2 = extract_value_until_whitespace(Trimmed),
                    parse_time_string(TimeStr2);
                nomatch -> 0
            end
    end.

%% Extract time limit from SBATCH_TIMELIMIT or SLURM_TIMELIMIT env vars
extract_time_from_env(Binary) ->
    case extract_env_str(Binary, <<"SBATCH_TIMELIMIT=">>) of
        <<>> ->
            case extract_env_str(Binary, <<"SLURM_TIMELIMIT=">>) of
                <<>> -> 0;
                Val -> parse_time_string(Val)
            end;
        Val -> parse_time_string(Val)
    end.

%% Scan the raw binary for --time= flag (null-terminated strings in protocol)
extract_time_from_binary_flags(Binary) ->
    case binary:match(Binary, <<"--time=">>) of
        {Pos, Len} ->
            <<_:Pos/binary, _:Len/binary, Rest/binary>> = Binary,
            TimeStr = extract_value_until_null_or_whitespace(Rest),
            case byte_size(TimeStr) of
                0 -> 0;
                _ -> parse_time_string(TimeStr)
            end;
        nomatch -> 0
    end.

%% Scan the raw binary for --mem= flag and extract memory value in MB.
extract_mem_from_binary_flags(Binary) ->
    case binary:match(Binary, <<"--mem=">>) of
        {Pos, Len} ->
            <<_:Pos/binary, _:Len/binary, Rest/binary>> = Binary,
            MemStr = extract_value_until_null_or_whitespace(Rest),
            case byte_size(MemStr) of
                0 -> 0;
                _ -> parse_memory_value(MemStr)
            end;
        nomatch -> 0
    end.

%% Extract value until null byte, whitespace, or end of string
extract_value_until_null_or_whitespace(Binary) ->
    extract_value_until_null_or_whitespace(Binary, <<>>).
extract_value_until_null_or_whitespace(<<>>, Acc) -> Acc;
extract_value_until_null_or_whitespace(<<0, _/binary>>, Acc) -> Acc;
extract_value_until_null_or_whitespace(<<$\s, _/binary>>, Acc) -> Acc;
extract_value_until_null_or_whitespace(<<$\t, _/binary>>, Acc) -> Acc;
extract_value_until_null_or_whitespace(<<$\n, _/binary>>, Acc) -> Acc;
extract_value_until_null_or_whitespace(<<C, Rest/binary>>, Acc) ->
    extract_value_until_null_or_whitespace(Rest, <<Acc/binary, C>>).

%% Extract time limit from #SBATCH --time directive.
%% Parses line by line, stops at first non-comment/non-blank line.
%% First directive wins (SLURM behavior).
extract_time_limit(Script) ->
    Lines = binary:split(Script, <<"\n">>, [global]),
    extract_time_limit_from_lines(Lines).

extract_time_limit_from_lines([]) -> 300;
extract_time_limit_from_lines([Line | Rest]) ->
    Trimmed = trim_leading_spaces(Line),
    case Trimmed of
        <<"#SBATCH ", Args/binary>> ->
            case extract_time_from_sbatch_args(trim_leading_spaces(Args)) of
                0 -> extract_time_limit_from_lines(Rest);
                Time -> Time  % First occurrence wins
            end;
        <<"#!", _/binary>> -> extract_time_limit_from_lines(Rest);
        <<"#", _/binary>> -> extract_time_limit_from_lines(Rest);
        <<>> -> extract_time_limit_from_lines(Rest);
        _ -> 300  % Stop at first non-comment line
    end.

extract_time_from_sbatch_args(<<"--time=", Rest/binary>>) ->
    parse_time_value(Rest);
extract_time_from_sbatch_args(<<"-t ", Rest/binary>>) ->
    parse_time_value(trim_leading_spaces(Rest));
extract_time_from_sbatch_args(_) -> 0.

parse_time_value(Binary) ->
    %% Parse HH:MM:SS or MM:SS or just minutes
    TimeStr = extract_until_newline(Binary),
    parse_time_string(TimeStr).

extract_until_newline(Binary) ->
    case binary:match(Binary, <<"\n">>) of
        {Pos, _} ->
            <<Str:Pos/binary, _/binary>> = Binary,
            Str;
        nomatch ->
            binary:part(Binary, 0, min(20, byte_size(Binary)))
    end.

parse_time_string(Binary) ->
    %% Try D-HH:MM:SS format first (e.g., "1-00:00:00", "7-12:30:00")
    case parse_day_time(Binary) of
        0 -> parse_time_string_nodash(Binary);
        Seconds -> Seconds
    end.

%% Parse D-HH:MM:SS format (days-hours:minutes:seconds)
parse_day_time(Binary) ->
    case binary:match(Binary, <<"-">>) of
        {DashPos, 1} when DashPos > 0, DashPos =< 3 ->
            <<DayPart:DashPos/binary, "-", Rest/binary>> = Binary,
            Days = parse_plain_minutes(DayPart),  % reuse digit parser for day count
            case Rest of
                <<H1, H2, ":", M1, M2, ":", S1, S2, _/binary>>
                  when H1 >= $0, H1 =< $9, H2 >= $0, H2 =< $9,
                       M1 >= $0, M1 =< $9, M2 >= $0, M2 =< $9,
                       S1 >= $0, S1 =< $9, S2 >= $0, S2 =< $9 ->
                    Hours = (H1 - $0) * 10 + (H2 - $0),
                    Minutes = (M1 - $0) * 10 + (M2 - $0),
                    Seconds = (S1 - $0) * 10 + (S2 - $0),
                    Days * 86400 + Hours * 3600 + Minutes * 60 + Seconds;
                <<H1, H2, ":", M1, M2, _/binary>>
                  when H1 >= $0, H1 =< $9, H2 >= $0, H2 =< $9,
                       M1 >= $0, M1 =< $9, M2 >= $0, M2 =< $9 ->
                    Hours = (H1 - $0) * 10 + (H2 - $0),
                    Minutes = (M1 - $0) * 10 + (M2 - $0),
                    Days * 86400 + Hours * 3600 + Minutes * 60;
                _ -> 0
            end;
        _ -> 0
    end.

parse_time_string_nodash(<<"00:", Rest/binary>>) ->
    %% HH:MM:SS format with 00 hours
    parse_minutes_seconds(Rest);
parse_time_string_nodash(<<H1, H2, ":", M1, M2, ":", S1, S2, _/binary>>)
  when H1 >= $0, H1 =< $9, H2 >= $0, H2 =< $9,
       M1 >= $0, M1 =< $9, M2 >= $0, M2 =< $9,
       S1 >= $0, S1 =< $9, S2 >= $0, S2 =< $9 ->
    Hours = (H1 - $0) * 10 + (H2 - $0),
    Minutes = (M1 - $0) * 10 + (M2 - $0),
    Seconds = (S1 - $0) * 10 + (S2 - $0),
    Hours * 3600 + Minutes * 60 + Seconds;
parse_time_string_nodash(<<H1, H2, ":", M1, M2, ":", S1, S2>>)
  when H1 >= $0, H1 =< $9, H2 >= $0, H2 =< $9,
       M1 >= $0, M1 =< $9, M2 >= $0, M2 =< $9,
       S1 >= $0, S1 =< $9, S2 >= $0, S2 =< $9 ->
    Hours = (H1 - $0) * 10 + (H2 - $0),
    Minutes = (M1 - $0) * 10 + (M2 - $0),
    Seconds = (S1 - $0) * 10 + (S2 - $0),
    Hours * 3600 + Minutes * 60 + Seconds;
parse_time_string_nodash(<<M1, M2, ":", S1, S2, _/binary>>)
  when M1 >= $0, M1 =< $9, M2 >= $0, M2 =< $9,
       S1 >= $0, S1 =< $9, S2 >= $0, S2 =< $9 ->
    Minutes = (M1 - $0) * 10 + (M2 - $0),
    Seconds = (S1 - $0) * 10 + (S2 - $0),
    Minutes * 60 + Seconds;
parse_time_string_nodash(<<M1, M2, ":", S1, S2>>)
  when M1 >= $0, M1 =< $9, M2 >= $0, M2 =< $9,
       S1 >= $0, S1 =< $9, S2 >= $0, S2 =< $9 ->
    Minutes = (M1 - $0) * 10 + (M2 - $0),
    Seconds = (S1 - $0) * 10 + (S2 - $0),
    Minutes * 60 + Seconds;
parse_time_string_nodash(Binary) ->
    %% Try to parse as plain minutes (e.g., "60", "1", "120")
    case parse_plain_minutes(Binary) of
        0 -> 300;  % Default 5 minutes
        Minutes -> Minutes * 60
    end.

parse_plain_minutes(Binary) ->
    parse_plain_minutes(Binary, 0).
parse_plain_minutes(<<D, Rest/binary>>, Acc) when D >= $0, D =< $9 ->
    parse_plain_minutes(Rest, Acc * 10 + (D - $0));
parse_plain_minutes(_, Acc) ->
    Acc.

parse_minutes_seconds(<<M1, M2, ":", S1, S2, _/binary>>)
  when M1 >= $0, M1 =< $9, M2 >= $0, M2 =< $9,
       S1 >= $0, S1 =< $9, S2 >= $0, S2 =< $9 ->
    Minutes = (M1 - $0) * 10 + (M2 - $0),
    Seconds = (S1 - $0) * 10 + (S2 - $0),
    Minutes * 60 + Seconds;
parse_minutes_seconds(<<M1, M2, ":", S1, S2>>)
  when M1 >= $0, M1 =< $9, M2 >= $0, M2 =< $9,
       S1 >= $0, S1 =< $9, S2 >= $0, S2 =< $9 ->
    Minutes = (M1 - $0) * 10 + (M2 - $0),
    Seconds = (S1 - $0) * 10 + (S2 - $0),
    Minutes * 60 + Seconds;
parse_minutes_seconds(_) ->
    300.

%% Extract resources from protocol binary (min_nodes, min_cpus)
%%
%% SLURM job_desc_msg_t structure (network byte order / big-endian):
%% The structure is complex and version-dependent, but the key resource
%% fields appear in a relatively fixed location early in the message.
%%
%% Key fields we extract:
%% - min_nodes / max_nodes
%% - min_cpus / cpus_per_task
%% - pn_min_memory (memory per node)
%% - time_limit
%% - priority
%% - partition name (length-prefixed string)
%% - job name (length-prefixed string)
%% - user_id / group_id
%%
%% Returns {MinNodes, MinCpus} or {0, 0} on failure (fallback to script).
extract_resources_from_protocol(Binary) when byte_size(Binary) < 100 ->
    %% Too small to contain meaningful job_desc_msg_t data
    {0, 0};
extract_resources_from_protocol(Binary) ->
    try
        extract_job_desc_resources(Binary)
    catch
        _:_ ->
            %% Fallback to script extraction on any error
            {0, 0}
    end.

%% Try to extract resources from job_desc_msg_t binary
%% Uses multiple strategies since protocol varies by SLURM version
extract_job_desc_resources(Binary) ->
    %% Strategy 1: Try fixed offset decoding for common SLURM versions
    case try_fixed_offset_decode(Binary) of
        {N, C} when N > 0; C > 0 ->
            {N, C};
        _ ->
            %% Strategy 2: Scan for resource patterns
            case scan_for_resources(Binary) of
                {N, C} when N > 0; C > 0 ->
                    {N, C};
                _ ->
                    {0, 0}
            end
    end.

%% Try decoding at known fixed offsets for SLURM 22.05/23.x protocol
%% job_desc_msg_t typical layout (bytes):
%%   0-3:   job_id (uint32)
%%   4-7:   user_id (uint32)
%%   8-11:  group_id (uint32)
%%  12-15:  alloc_node string length
%%  After alloc_node string:
%%   - contiguous (uint16)
%%   - core_spec (uint16)
%%   - cpus_per_task (uint16)
%%   - delay_boot (uint32)
%%   ... more fields ...
%%   - min_cpus (uint32)
%%   - min_nodes (uint32)
%%   - max_nodes (uint32)
try_fixed_offset_decode(Binary) when byte_size(Binary) >= 40 ->
    %% Skip initial header fields, look for typical resource field patterns
    %% In SLURM protocol, many uint32 fields use SLURM_NO_VAL (0xFFFFFFFE)
    %% as sentinel for "not set". Valid resource counts are typically 1-65535.

    %% Scan first 200 bytes for plausible min_nodes/min_cpus pairs
    %% They appear as consecutive uint32 values where:
    %%   - min_nodes: 1-10000 (reasonable cluster size)
    %%   - min_cpus: 1-100000 (reasonable CPU count)
    scan_uint32_pairs(Binary, 0, byte_size(Binary) - 8);
try_fixed_offset_decode(_) ->
    {0, 0}.

%% Scan binary for consecutive uint32 pairs that look like resource counts
scan_uint32_pairs(_Binary, Offset, MaxOffset) when Offset > MaxOffset ->
    {0, 0};
scan_uint32_pairs(Binary, Offset, MaxOffset) ->
    <<_:Offset/binary, V1:32/big, V2:32/big, _/binary>> = Binary,
    %% Check if these look like valid min_nodes, min_cpus values
    %% Valid: 1-65534 (SLURM_NO_VAL is 0xFFFFFFFE)
    case is_valid_resource_value(V1) andalso is_valid_resource_value(V2) of
        true when V1 =< 10000, V2 =< 100000 ->
            %% Plausible node/cpu counts found
            %% But we need more validation - check if next values also reasonable
            case validate_resource_context(Binary, Offset) of
                true -> {V1, V2};
                false -> scan_uint32_pairs(Binary, Offset + 4, MaxOffset)
            end;
        _ ->
            scan_uint32_pairs(Binary, Offset + 4, MaxOffset)
    end.

%% Check if value is a valid resource count (not NO_VAL, not 0, reasonable size)
is_valid_resource_value(V) when V > 0, V < 16#FFFFFFFE, V =< 1000000 ->
    true;
is_valid_resource_value(_) ->
    false.

%% Validate that the context around potential resource values looks right
%% Resource fields are often followed by more uint32 values in similar range
validate_resource_context(Binary, Offset) when byte_size(Binary) >= Offset + 16 ->
    <<_:Offset/binary, _V1:32/big, _V2:32/big, V3:32/big, _/binary>> = Binary,
    %% max_nodes typically follows min_nodes
    %% Check if V3 could be max_nodes (should be >= V1 if set)
    case V3 of
        X when X == 16#FFFFFFFE -> true;  % NO_VAL is valid for max_nodes
        X when X >= 1, X =< 100000 -> true;
        _ -> false
    end;
validate_resource_context(_, _) ->
    false.

%% Strategy 2: Scan for resource-related patterns in the binary
%% Look for strings like "nodes=", "--cpus", or known field boundaries
scan_for_resources(Binary) ->
    MinNodes = scan_for_node_count(Binary),
    MinCpus = scan_for_cpu_count(Binary),
    {MinNodes, MinCpus}.

%% Scan for node count by looking for uint32 values preceded by string patterns
scan_for_node_count(Binary) ->
    %% Look for patterns indicating node count fields
    %% In SLURM, min_nodes is often near strings containing "node"
    case find_field_near_pattern(Binary, <<"node">>, 50) of
        V when V > 0, V =< 10000 -> V;
        _ -> 0
    end.

%% Scan for CPU count
scan_for_cpu_count(Binary) ->
    case find_field_near_pattern(Binary, <<"cpu">>, 50) of
        V when V > 0, V =< 100000 -> V;
        _ ->
            %% Also try looking for ntasks pattern
            case find_field_near_pattern(Binary, <<"task">>, 50) of
                V when V > 0, V =< 100000 -> V;
                _ -> 0
            end
    end.

%% Find a uint32 value near a pattern string
find_field_near_pattern(Binary, Pattern, SearchRadius) ->
    case binary:match(Binary, Pattern) of
        {Start, _} ->
            %% Search nearby for valid uint32 values
            SearchStart = max(0, Start - SearchRadius),
            SearchEnd = min(byte_size(Binary) - 4, Start + SearchRadius),
            find_best_uint32_in_range(Binary, SearchStart, SearchEnd);
        nomatch ->
            0
    end.

%% Find a plausible resource value in a byte range
find_best_uint32_in_range(Binary, Start, End) when Start =< End - 4 ->
    <<_:Start/binary, V:32/big, _/binary>> = Binary,
    case is_valid_resource_value(V) andalso V =< 10000 of
        true -> V;
        false -> find_best_uint32_in_range(Binary, Start + 4, End)
    end;
find_best_uint32_in_range(_, _, _) ->
    0.

%% Extended resource extraction returning full job_desc info
%% This can be called when more detailed resource info is needed
-spec extract_full_job_desc(binary()) ->
    {ok, #{min_nodes => non_neg_integer(),
           max_nodes => non_neg_integer(),
           min_cpus => non_neg_integer(),
           cpus_per_task => non_neg_integer(),
           time_limit => non_neg_integer(),
           priority => non_neg_integer(),
           partition => binary(),
           job_name => binary(),
           user_id => non_neg_integer(),
           group_id => non_neg_integer()}} |
    {error, term()}.
extract_full_job_desc(Binary) when byte_size(Binary) < 50 ->
    {error, binary_too_small};
extract_full_job_desc(Binary) ->
    try
        %% Extract what we can from the binary
        {MinNodes, MinCpus} = extract_resources_from_protocol(Binary),
        {UserId, GroupId} = extract_uid_gid_from_header(Binary),
        TimeLimit = extract_time_limit_from_protocol(Binary),
        Priority = extract_priority_from_protocol(Binary),
        Partition = extract_partition_from_protocol(Binary),
        JobName = find_job_name(Binary),

        {ok, #{
            min_nodes => MinNodes,
            max_nodes => MinNodes,  % Conservative default
            min_cpus => MinCpus,
            cpus_per_task => max(1, MinCpus),
            time_limit => TimeLimit,
            priority => Priority,
            partition => Partition,
            job_name => JobName,
            user_id => UserId,
            group_id => GroupId
        }}
    catch
        _:Reason ->
            {error, Reason}
    end.

%% Extract user_id and group_id from protocol header
%% In SLURM job_desc_msg_t, these are at fixed offsets 4-7 and 8-11
extract_uid_gid_from_header(<<_JobId:32/big, UserId:32/big, GroupId:32/big, _/binary>>)
  when UserId < 65536, GroupId < 65536 ->
    {UserId, GroupId};
extract_uid_gid_from_header(_) ->
    {0, 0}.

%% Extract time limit from protocol binary
%% Time limit is a uint32 in minutes, look for it in typical locations
extract_time_limit_from_protocol(Binary) when byte_size(Binary) >= 100 ->
    %% Scan for plausible time limit values (1-525600 minutes = 1 year)
    case scan_for_time_limit(Binary, 20, min(200, byte_size(Binary) - 4)) of
        T when T > 0 -> T;
        _ -> 0
    end;
extract_time_limit_from_protocol(_) ->
    0.

scan_for_time_limit(_Binary, Offset, MaxOffset) when Offset > MaxOffset ->
    0;
scan_for_time_limit(Binary, Offset, MaxOffset) ->
    <<_:Offset/binary, V:32/big, _/binary>> = Binary,
    %% Valid time limits: 1-525600 minutes (up to 1 year)
    %% Common values: 60, 120, 480, 1440 (1h, 2h, 8h, 24h)
    case V of
        T when T >= 1, T =< 525600 -> T;
        _ -> scan_for_time_limit(Binary, Offset + 4, MaxOffset)
    end.

%% Extract priority from protocol binary
extract_priority_from_protocol(Binary) when byte_size(Binary) >= 100 ->
    %% Priority is uint32, typically 0-4294967294
    %% Normal priority is often around 4294901760 (0xFFFF0000 - nice offset)
    0;  % Return 0 for default priority
extract_priority_from_protocol(_) ->
    0.

%% Extract partition name from protocol binary
extract_partition_from_protocol(Binary) ->
    %% Look for partition string - typically a short identifier
    %% Common patterns: "batch", "compute", "gpu", etc.
    case binary:match(Binary, <<"partition">>) of
        {Start, _} when Start + 15 < byte_size(Binary) ->
            %% Try to find the partition value after the field name
            <<_:Start/binary, _:72, Rest/binary>> = Binary,
            extract_short_string(Rest, 32);
        _ ->
            <<>>
    end.

%% Extract a short string from binary (for partition names, etc.)
extract_short_string(<<Len:32/big, Str:Len/binary, _/binary>>, MaxLen)
  when Len > 0, Len =< MaxLen ->
    strip_null(Str);
extract_short_string(_, _) ->
    <<>>.

%% Extract resources from SLURM environment variables in the protocol binary.
%% When users pass flags on the command line (e.g., sbatch --cpus-per-task=16 --wrap "sleep 30"),
%% the SLURM client sets environment variables like SLURM_CPUS_PER_TASK=16 in the job
%% environment embedded in the protocol binary. This is more reliable than protocol binary
%% scanning for command-line flags that don't appear in #SBATCH script directives.
%% Returns {Nodes, Tasks, Cpus, MemoryMb} - 0 means not found.
extract_resources_from_env(Binary) ->
    %% Try multiple env var patterns that sbatch may set
    Nodes = first_nonzero([
        extract_env_int(Binary, <<"SLURM_NNODES=">>),
        extract_env_int(Binary, <<"SLURM_JOB_NUM_NODES=">>),
        extract_env_int(Binary, <<"SBATCH_NODES=">>)
    ]),
    Tasks = first_nonzero([
        extract_env_int(Binary, <<"SLURM_NTASKS=">>),
        extract_env_int(Binary, <<"SBATCH_NTASKS=">>)
    ]),
    Cpus = first_nonzero([
        extract_env_int(Binary, <<"SLURM_CPUS_PER_TASK=">>),
        extract_env_int(Binary, <<"SBATCH_CPUS_PER_TASK=">>),
        extract_env_int(Binary, <<"SLURM_CPUS_ON_NODE=">>)
    ]),
    MemMb = first_nonzero([
        extract_env_int(Binary, <<"SLURM_MEM_PER_NODE=">>),
        extract_env_int(Binary, <<"SBATCH_MEM_PER_NODE=">>)
    ]),
    lager:debug("extract_resources_from_env: nodes=~p tasks=~p cpus=~p mem=~p",
               [Nodes, Tasks, Cpus, MemMb]),
    {Nodes, Tasks, Cpus, MemMb}.

first_nonzero([]) -> 0;
first_nonzero([V | _]) when V > 0 -> V;
first_nonzero([_ | Rest]) -> first_nonzero(Rest).

%% Extract resources from the embedded sbatch command line in the protocol binary.
%% SLURM embeds the full command line (e.g., "sbatch -N3 --cpus-per-task=16 --wrap sleep 10")
%% as a packstr in the batch job request binary.
extract_resources_from_cmdline(Binary) ->
    %% Find all occurrences of "sbatch " in the binary
    case binary:matches(Binary, <<"sbatch ">>) of
        [] ->
            {0, 0, 0, 0};
        Matches ->
            %% Try each match, use the first one that contains resource flags
            extract_resources_from_cmdline_matches(Binary, Matches)
    end.

extract_resources_from_cmdline_matches(_Binary, []) ->
    {0, 0, 0, 0};
extract_resources_from_cmdline_matches(Binary, [{Start, _} | Rest]) ->
    <<_:Start/binary, CmdRest/binary>> = Binary,
    CmdLine = extract_until_null(CmdRest, 0),
    lager:debug("Checking sbatch command line at offset ~p: ~s", [Start, CmdLine]),
    %% First try #SBATCH directive parsing (for scripts embedded in cmdline)
    Result1 = extract_resources(CmdLine),
    %% Also try direct command-line flag parsing (for --mem=, --nodes=, etc.)
    Result2 = extract_resources_from_cmdline_string(CmdLine),
    %% Merge: prefer non-zero values from either source
    Result = merge_resources(Result1, Result2),
    case Result of
        {0, 0, 0, 0} ->
            %% No resources found in this match, try next
            extract_resources_from_cmdline_matches(Binary, Rest);
        _ ->
            Result
    end.

%% Parse resource flags directly from a command line string (not #SBATCH directives).
%% Handles: --mem=, --nodes=/-N, --ntasks=/-n, --cpus-per-task=/-c
extract_resources_from_cmdline_string(CmdLine) ->
    Nodes = case binary:match(CmdLine, <<"--nodes=">>) of
        {NPos, NLen} ->
            <<_:NPos/binary, _:NLen/binary, NRest/binary>> = CmdLine,
            parse_int_value(NRest);
        nomatch ->
            case binary:match(CmdLine, <<"-N ">>) of
                {NPos2, NLen2} ->
                    <<_:NPos2/binary, _:NLen2/binary, NRest2/binary>> = CmdLine,
                    parse_int_value(trim_leading_spaces(NRest2));
                nomatch -> 0
            end
    end,
    Tasks = case binary:match(CmdLine, <<"--ntasks=">>) of
        {TPos, TLen} ->
            <<_:TPos/binary, _:TLen/binary, TRest/binary>> = CmdLine,
            parse_int_value(TRest);
        nomatch ->
            case binary:match(CmdLine, <<"-n ">>) of
                {TPos2, TLen2} ->
                    <<_:TPos2/binary, _:TLen2/binary, TRest2/binary>> = CmdLine,
                    parse_int_value(trim_leading_spaces(TRest2));
                nomatch -> 0
            end
    end,
    Cpus = case binary:match(CmdLine, <<"--cpus-per-task=">>) of
        {CPos, CLen} ->
            <<_:CPos/binary, _:CLen/binary, CRest/binary>> = CmdLine,
            parse_int_value(CRest);
        nomatch ->
            case binary:match(CmdLine, <<"-c ">>) of
                {CPos2, CLen2} ->
                    <<_:CPos2/binary, _:CLen2/binary, CRest2/binary>> = CmdLine,
                    parse_int_value(trim_leading_spaces(CRest2));
                nomatch -> 0
            end
    end,
    MemMb = case binary:match(CmdLine, <<"--mem=">>) of
        {MPos, MLen} ->
            <<_:MPos/binary, _:MLen/binary, MRest/binary>> = CmdLine,
            parse_memory_value(MRest);
        nomatch -> 0
    end,
    {Nodes, Tasks, Cpus, MemMb}.

%% Merge two resource tuples, preferring non-zero values from either.
merge_resources({N1, T1, C1, M1}, {N2, T2, C2, M2}) ->
    {first_nonzero([N1, N2]),
     first_nonzero([T1, T2]),
     first_nonzero([C1, C2]),
     first_nonzero([M1, M2])}.

extract_until_null(Binary, Pos) when Pos < byte_size(Binary) ->
    case binary:at(Binary, Pos) of
        0 -> binary:part(Binary, 0, Pos);
        _ -> extract_until_null(Binary, Pos + 1)
    end;
extract_until_null(Binary, _Pos) ->
    Binary.

%% Search for an environment variable pattern in binary and extract its integer value.
extract_env_int(Binary, Pattern) ->
    case binary:match(Binary, Pattern) of
        {Start, PLen} ->
            <<_:Start/binary, _:PLen/binary, Rest/binary>> = Binary,
            parse_env_int_value(Rest);
        nomatch ->
            0
    end.

%% Parse an integer from the start of a binary, stopping at null or non-digit.
parse_env_int_value(<<D, Rest/binary>>) when D >= $0, D =< $9 ->
    parse_env_int_value(Rest, D - $0);
parse_env_int_value(_) ->
    0.

parse_env_int_value(<<D, Rest/binary>>, Acc) when D >= $0, D =< $9 ->
    parse_env_int_value(Rest, Acc * 10 + (D - $0));
parse_env_int_value(_, Acc) ->
    Acc.

%% Extract a string value from an environment variable in the binary.
extract_env_str(Binary, Pattern) ->
    case binary:match(Binary, Pattern) of
        {Start, PLen} ->
            <<_:Start/binary, _:PLen/binary, Rest/binary>> = Binary,
            parse_env_str_value(Rest);
        nomatch ->
            <<>>
    end.

%% Parse a string from the start of a binary, stopping at null, newline, or non-printable.
parse_env_str_value(Binary) ->
    parse_env_str_value(Binary, <<>>).

parse_env_str_value(<<>>, Acc) -> Acc;
parse_env_str_value(<<0, _/binary>>, Acc) -> Acc;
parse_env_str_value(<<$\n, _/binary>>, Acc) -> Acc;
parse_env_str_value(<<C, Rest/binary>>, Acc) when C >= 32, C < 127 ->
    parse_env_str_value(Rest, <<Acc/binary, C>>);
parse_env_str_value(_, Acc) -> Acc.

%% Extract --job-name from #SBATCH directives in script.
%% Parses line by line, stops at first non-comment/non-blank line (SLURM behavior).
%% For duplicate directives, first one wins (SLURM behavior).
extract_job_name_from_script(Script) ->
    Lines = binary:split(Script, <<"\n">>, [global]),
    extract_job_name_from_lines(Lines).

extract_job_name_from_lines([]) -> <<>>;
extract_job_name_from_lines([Line | Rest]) ->
    Trimmed = trim_leading_spaces(Line),
    case Trimmed of
        <<"#SBATCH ", Args/binary>> ->
            case extract_name_from_sbatch_args(Args) of
                <<>> -> extract_job_name_from_lines(Rest);
                Name -> Name  % First occurrence wins
            end;
        <<"#!", _/binary>> ->
            %% Shebang line, continue
            extract_job_name_from_lines(Rest);
        <<"#", _/binary>> ->
            %% Other comment, continue
            extract_job_name_from_lines(Rest);
        <<>> ->
            %% Blank line, continue
            extract_job_name_from_lines(Rest);
        _ ->
            %% Non-comment line: stop parsing directives
            <<>>
    end.

extract_name_from_sbatch_args(Args) ->
    Trimmed = trim_leading_spaces(Args),
    case Trimmed of
        <<"--job-name=", Rest/binary>> ->
            extract_until_whitespace(Rest);
        <<"-J ", Rest/binary>> ->
            extract_until_whitespace(trim_leading_spaces(Rest));
        _ -> <<>>
    end.

trim_leading_spaces(<<$\s, Rest/binary>>) -> trim_leading_spaces(Rest);
trim_leading_spaces(<<$\t, Rest/binary>>) -> trim_leading_spaces(Rest);
trim_leading_spaces(Bin) -> Bin.

%% Extract text until whitespace or newline
extract_until_whitespace(Binary) ->
    extract_until_whitespace(Binary, <<>>).

extract_until_whitespace(<<>>, Acc) -> Acc;
extract_until_whitespace(<<C, _/binary>>, Acc)
  when C =:= $\n; C =:= $\r; C =:= $\s; C =:= $\t; C =:= 0 -> Acc;
extract_until_whitespace(<<C, Rest/binary>>, Acc) ->
    extract_until_whitespace(Rest, <<Acc/binary, C>>).

%% Return first non-empty binary from list
first_nonempty([]) -> <<>>;
first_nonempty([<<>> | Rest]) -> first_nonempty(Rest);
first_nonempty([V | _]) when is_binary(V), byte_size(V) > 0 -> V;
first_nonempty([_ | Rest]) -> first_nonempty(Rest).

%% Extract resources from #SBATCH directives
%% Returns {Nodes, Tasks, Cpus, MemoryMb} - 0 means not specified in script.
%% Parses line by line, stops at first non-comment/non-blank line (SLURM behavior).
%% For duplicate directives, first one wins (SLURM behavior).
extract_resources(Script) ->
    Lines = binary:split(Script, <<"\n">>, [global]),
    extract_resources_from_lines(Lines, {0, 0, 0, 0}).

extract_resources_from_lines([], Acc) -> Acc;
extract_resources_from_lines([Line | Rest], {_Nodes, _Tasks, _Cpus, _Mem} = Acc) ->
    Trimmed = trim_leading_spaces(Line),
    case Trimmed of
        <<"#SBATCH ", Args/binary>> ->
            NewAcc = parse_resource_from_sbatch_args(trim_leading_spaces(Args), Acc),
            extract_resources_from_lines(Rest, NewAcc);
        <<"#!", _/binary>> ->
            extract_resources_from_lines(Rest, Acc);
        <<"#", _/binary>> ->
            extract_resources_from_lines(Rest, Acc);
        <<>> ->
            extract_resources_from_lines(Rest, Acc);
        _ ->
            Acc  % Stop at first non-comment line
    end.

parse_resource_from_sbatch_args(Args, {Nodes, Tasks, Cpus, Mem}) ->
    case Args of
        <<"--nodes=", Rest/binary>> ->
            V = parse_int_value(Rest),
            {first_nonzero_val(Nodes, V), Tasks, Cpus, Mem};
        <<"-N ", Rest/binary>> ->
            V = parse_int_value(trim_leading_spaces(Rest)),
            {first_nonzero_val(Nodes, V), Tasks, Cpus, Mem};
        <<"-N", D, _/binary>> = Bin when D >= $0, D =< $9 ->
            <<"-N", Rest/binary>> = Bin,
            V = parse_int_value(Rest),
            {first_nonzero_val(Nodes, V), Tasks, Cpus, Mem};
        <<"--ntasks=", Rest/binary>> ->
            V = parse_int_value(Rest),
            {Nodes, first_nonzero_val(Tasks, V), Cpus, Mem};
        <<"-n ", Rest/binary>> ->
            V = parse_int_value(trim_leading_spaces(Rest)),
            {Nodes, first_nonzero_val(Tasks, V), Cpus, Mem};
        <<"-n", D, _/binary>> = Bin when D >= $0, D =< $9 ->
            <<"-n", Rest/binary>> = Bin,
            V = parse_int_value(Rest),
            {Nodes, first_nonzero_val(Tasks, V), Cpus, Mem};
        <<"--cpus-per-task=", Rest/binary>> ->
            V = parse_int_value(Rest),
            {Nodes, Tasks, first_nonzero_val(Cpus, V), Mem};
        <<"-c ", Rest/binary>> ->
            V = parse_int_value(trim_leading_spaces(Rest)),
            {Nodes, Tasks, first_nonzero_val(Cpus, V), Mem};
        <<"-c", D, _/binary>> = Bin when D >= $0, D =< $9 ->
            <<"-c", Rest/binary>> = Bin,
            V = parse_int_value(Rest),
            {Nodes, Tasks, first_nonzero_val(Cpus, V), Mem};
        <<"--mem=", Rest/binary>> ->
            V = parse_memory_value(Rest),
            {Nodes, Tasks, Cpus, first_nonzero_val(Mem, V)};
        _ ->
            {Nodes, Tasks, Cpus, Mem}
    end.

%% Keep existing value if already set (first wins); otherwise use new value
first_nonzero_val(0, New) -> New;
first_nonzero_val(Existing, _) -> Existing.

%% Parse memory value with optional suffix (G, M, K)
%% Returns value in MB
parse_memory_value(Bin) ->
    RawVal = parse_int_value(Bin),
    %% Check for suffix after the digits
    Suffix = skip_digits(Bin),
    case Suffix of
        <<$G, _/binary>> -> RawVal * 1024;
        <<$g, _/binary>> -> RawVal * 1024;
        <<$K, _/binary>> -> max(1, RawVal div 1024);
        <<$k, _/binary>> -> max(1, RawVal div 1024);
        <<$M, _/binary>> -> RawVal;
        <<$m, _/binary>> -> RawVal;
        _ -> RawVal  % Default: MB
    end.

skip_digits(<<D, Rest/binary>>) when D >= $0, D =< $9 ->
    skip_digits(Rest);
skip_digits(Rest) ->
    Rest.

parse_int_value(<<D, Rest/binary>>) when D >= $0, D =< $9 ->
    parse_int_value(Rest, D - $0);
parse_int_value(_) ->
    1.

parse_int_value(<<D, Rest/binary>>, Acc) when D >= $0, D =< $9 ->
    parse_int_value(Rest, Acc * 10 + (D - $0));
parse_int_value(_, Acc) ->
    Acc.

%% Check if binary contains a printable job name
is_printable_name(<<>>) -> false;
is_printable_name(Bin) ->
    is_printable_name_chars(Bin).

is_printable_name_chars(<<>>) -> true;
is_printable_name_chars(<<0>>) -> true;  % Null terminator OK
is_printable_name_chars(<<C, Rest/binary>>) when C >= 32, C < 127 ->
    is_printable_name_chars(Rest);
is_printable_name_chars(_) -> false.

%% Strip trailing nulls from string
strip_null(Bin) ->
    case binary:match(Bin, <<0>>) of
        {Pos, _} -> <<Stripped:Pos/binary, _/binary>> = Bin, Stripped;
        nomatch -> Bin
    end.

%% Decode REQUEST_CANCEL_JOB (4006)
decode_cancel_job_request(Binary) ->
    case Binary of
        <<JobId:32/big, StepId:32/big, Signal:32/big, Flags:32/big, Rest/binary>> when byte_size(Rest) > 0 ->
            %% Try to unpack the optional job_id_str, default to undefined on error
            JobIdStr = case flurm_protocol_pack:unpack_string(Rest) of
                {ok, Str, _} -> ensure_binary(Str);
                {error, _} -> undefined
            end,
            {ok, #cancel_job_request{
                job_id = JobId,
                job_id_str = JobIdStr,
                step_id = StepId,
                signal = Signal,
                flags = Flags
            }};
        <<JobId:32/big, StepId:32/big, Signal:32/big, Flags:32/big>> ->
            {ok, #cancel_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = Signal,
                flags = Flags
            }};
        <<JobId:32/big, StepId:32/big, Signal:32/big>> ->
            {ok, #cancel_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = Signal
            }};
        <<JobId:32/big>> ->
            {ok, #cancel_job_request{job_id = JobId}};
        <<>> ->
            {ok, #cancel_job_request{}};
        _ ->
            {error, invalid_cancel_job_request}
    end.

%% Decode REQUEST_KILL_JOB (5032)
%% SLURM 24.x format observed:
%%   <<StepId:32, StepHetComp:32, ???:32, JobIdStr, ???:32, Signal:16, Flags:16>>
%% Where StepId/StepHetComp may be SLURM_NO_VAL (0xFFFFFFFE)
decode_kill_job_request(Binary) ->
    case Binary of
        %% Format with 3 leading uint32 values, then string
        <<StepId:32/big-signed, _StepHetComp:32/big, _Field3:32/big,
          Rest1/binary>> when byte_size(Rest1) >= 4 ->
            case flurm_protocol_pack:unpack_string(Rest1) of
                {ok, JobIdStr, Rest2} when byte_size(Rest2) >= 8 ->
                    <<_Field4:32/big, Signal:16/big, Flags:16/big, Rest3/binary>> = Rest2,
                    {ok, Sibling, _Rest4} = safe_unpack_string(Rest3),
                    JobId = parse_job_id(JobIdStr),
                    {ok, #kill_job_request{
                        job_id = JobId,
                        job_id_str = ensure_binary(JobIdStr),
                        step_id = normalize_step_id(StepId),
                        signal = Signal,
                        flags = Flags,
                        sibling = ensure_binary(Sibling)
                    }};
                {ok, JobIdStr, Rest2} ->
                    %% Shorter format - just string and maybe signal
                    JobId = parse_job_id(JobIdStr),
                    {Signal, Flags} = case Rest2 of
                        <<S:16/big, F:16/big, _/binary>> -> {S, F};
                        _ -> {9, 0}  % Default to SIGKILL
                    end,
                    {ok, #kill_job_request{
                        job_id = JobId,
                        job_id_str = ensure_binary(JobIdStr),
                        step_id = normalize_step_id(StepId),
                        signal = Signal,
                        flags = Flags
                    }};
                _ ->
                    %% No string found - try numeric format
                    decode_kill_job_numeric_format(Binary)
            end;
        %% Try simpler numeric format
        _ ->
            decode_kill_job_numeric_format(Binary)
    end.

decode_kill_job_numeric_format(Binary) ->
    case Binary of
        <<JobId:32/big, StepId:32/big-signed, Signal:16/big, Flags:16/big>> ->
            {ok, #kill_job_request{
                job_id = JobId,
                step_id = normalize_step_id(StepId),
                signal = Signal,
                flags = Flags
            }};
        <<JobId:32/big, StepId:32/big-signed>> ->
            {ok, #kill_job_request{
                job_id = JobId,
                step_id = normalize_step_id(StepId)
            }};
        _ ->
            {error, invalid_kill_job_request}
    end.

%% Decode REQUEST_SUSPEND (5014) - scontrol suspend/resume
%% SLURM format: job_id (32), op (16: 1=suspend, 0=resume), [job_id_str]
decode_suspend_request(Binary) ->
    case Binary of
        <<JobId:32/big, Op:16/big, Rest/binary>> ->
            Suspend = Op =/= 0,
            {ok, JobIdStr, _} = safe_unpack_string(Rest),
            {ok, #suspend_request{
                job_id = JobId,
                job_id_str = ensure_binary(JobIdStr),
                suspend = Suspend
            }};
        <<JobId:32/big, Op:16/big>> ->
            Suspend = Op =/= 0,
            {ok, #suspend_request{
                job_id = JobId,
                suspend = Suspend
            }};
        _ ->
            {error, invalid_suspend_request}
    end.

%% Decode REQUEST_SIGNAL_JOB (5018) - scancel -s SIGNAL, scontrol signal
%% SLURM format: job_id (32), step_id (32 signed), signal (16), flags (16), [job_id_str]
decode_signal_job_request(Binary) ->
    case Binary of
        <<JobId:32/big, StepId:32/big-signed, Signal:16/big, Flags:16/big, Rest/binary>> ->
            {ok, JobIdStr, _} = safe_unpack_string(Rest),
            {ok, #signal_job_request{
                job_id = JobId,
                job_id_str = ensure_binary(JobIdStr),
                step_id = normalize_step_id(StepId),
                signal = Signal,
                flags = Flags
            }};
        <<JobId:32/big, StepId:32/big-signed, Signal:16/big, Flags:16/big>> ->
            {ok, #signal_job_request{
                job_id = JobId,
                step_id = normalize_step_id(StepId),
                signal = Signal,
                flags = Flags
            }};
        <<JobId:32/big, Signal:16/big, Flags:16/big>> ->
            {ok, #signal_job_request{
                job_id = JobId,
                signal = Signal,
                flags = Flags
            }};
        _ ->
            {error, invalid_signal_job_request}
    end.

%% Decode REQUEST_COMPLETE_PROLOG (5019)
%% SLURM format: job_id (32), prolog_rc (32), [node_name]
decode_complete_prolog_request(Binary) ->
    case Binary of
        <<JobId:32/big, PrologRc:32/big-signed, Rest/binary>> ->
            {ok, NodeName, _} = safe_unpack_string(Rest),
            {ok, #complete_prolog_request{
                job_id = JobId,
                prolog_rc = PrologRc,
                node_name = ensure_binary(NodeName)
            }};
        <<JobId:32/big, PrologRc:32/big-signed>> ->
            {ok, #complete_prolog_request{
                job_id = JobId,
                prolog_rc = PrologRc
            }};
        _ ->
            {error, invalid_complete_prolog_request}
    end.

%% Decode MESSAGE_EPILOG_COMPLETE (6012)
%% SLURM format: job_id (32), epilog_rc (32), [node_name]
decode_epilog_complete_msg(Binary) ->
    case Binary of
        <<JobId:32/big, EpilogRc:32/big-signed, Rest/binary>> ->
            {ok, NodeName, _} = safe_unpack_string(Rest),
            {ok, #epilog_complete_msg{
                job_id = JobId,
                epilog_rc = EpilogRc,
                node_name = ensure_binary(NodeName)
            }};
        <<JobId:32/big, EpilogRc:32/big-signed>> ->
            {ok, #epilog_complete_msg{
                job_id = JobId,
                epilog_rc = EpilogRc
            }};
        _ ->
            {error, invalid_epilog_complete_msg}
    end.

%% Decode MESSAGE_TASK_EXIT (6003)
%% Wire format from _pack_task_exit_msg / _unpack_task_exit_msg:
%%   return_code: uint32
%%   num_tasks: uint32 (count of task_id_list)
%%   task_id_list: pack32_array format = count:32 + elements:32 each
%%   step_id: slurm_step_id_t (job_id:32, step_id:32, step_het_comp:32)
decode_task_exit_msg(Binary) ->
    case Binary of
        <<ReturnCode:32/big-signed, NumTasks:32/big, Rest/binary>> ->
            %% Parse task_id_list (pack32_array: count then values)
            case parse_task_id_array(NumTasks, Rest) of
                {ok, TaskIds, Rest2} ->
                    case Rest2 of
                        <<JobId:32/big, StepId:32/big, StepHetComp:32/big, Rest3/binary>> ->
                            %% Try to get node name if present
                            NodeName = case safe_unpack_string(Rest3) of
                                {ok, N, _} -> ensure_binary(N);
                                _ -> <<>>
                            end,
                            {ok, #task_exit_msg{
                                job_id = JobId,
                                step_id = StepId,
                                step_het_comp = StepHetComp,
                                task_ids = TaskIds,
                                return_code = ReturnCode,
                                node_name = NodeName
                            }};
                        <<JobId:32/big, StepId:32/big>> ->
                            {ok, #task_exit_msg{
                                job_id = JobId,
                                step_id = StepId,
                                task_ids = TaskIds,
                                return_code = ReturnCode
                            }};
                        _ ->
                            %% Fallback - minimal info
                            {ok, #task_exit_msg{
                                task_ids = TaskIds,
                                return_code = ReturnCode
                            }}
                    end;
                {error, _} ->
                    {error, invalid_task_exit_msg}
            end;
        _ ->
            {error, invalid_task_exit_msg}
    end.

%% Parse task ID array (pack32_array format)
parse_task_id_array(0, Rest) ->
    {ok, [], Rest};
parse_task_id_array(Count, Binary) ->
    parse_task_ids(Count, Binary, []).

parse_task_ids(0, Rest, Acc) ->
    {ok, lists:reverse(Acc), Rest};
parse_task_ids(N, <<TaskId:32/big, Rest/binary>>, Acc) when N > 0 ->
    parse_task_ids(N - 1, Rest, [TaskId | Acc]);
parse_task_ids(_, _, _) ->
    {error, truncated_task_id_list}.

%% Parse job ID from string (may contain array indices like "123_4")
parse_job_id(undefined) -> 0;
parse_job_id(<<>>) -> 0;
parse_job_id(JobIdStr) when is_binary(JobIdStr) ->
    %% Strip null terminator if present
    Stripped = case binary:match(JobIdStr, <<0>>) of
        {Pos, _} -> binary:part(JobIdStr, 0, Pos);
        nomatch -> JobIdStr
    end,
    %% Extract numeric part (handle "123_4" format for array jobs)
    case binary:split(Stripped, <<"_">>) of
        [BaseId | _] -> safe_binary_to_integer(BaseId);
        _ -> safe_binary_to_integer(Stripped)
    end.

safe_binary_to_integer(Bin) ->
    try binary_to_integer(Bin)
    catch _:_ -> 0
    end.

%% Normalize step_id (SLURM_NO_VAL means no specific step)
normalize_step_id(StepId) when StepId < 0 -> -1;
normalize_step_id(StepId) when StepId >= 16#FFFFFFFE -> -1;
normalize_step_id(StepId) -> StepId.

%% Safe string unpack that returns empty binary on failure
safe_unpack_string(Binary) ->
    case flurm_protocol_pack:unpack_string(Binary) of
        {ok, Str, Rest} -> {ok, Str, Rest};
        _ -> {ok, <<>>, Binary}
    end.

%%%===================================================================
%%% Response Decoders
%%%===================================================================

%% Decode RESPONSE_SLURM_RC (8001)
decode_slurm_rc_response(<<ReturnCode:32/big-signed, _Rest/binary>>) ->
    {ok, #slurm_rc_response{return_code = ReturnCode}};
decode_slurm_rc_response(<<ReturnCode:32/big-signed>>) ->
    {ok, #slurm_rc_response{return_code = ReturnCode}};
decode_slurm_rc_response(<<>>) ->
    {ok, #slurm_rc_response{return_code = 0}};
decode_slurm_rc_response(_) ->
    {error, invalid_slurm_rc_response}.

%% Decode RESPONSE_SUBMIT_BATCH_JOB (4004)
decode_batch_job_response(Binary) ->
    case Binary of
        <<JobId:32/big, StepId:32/big, ErrorCode:32/big, Rest/binary>> when byte_size(Rest) > 0 ->
            %% Try to unpack the optional user message, default to undefined on error
            UserMsg = case flurm_protocol_pack:unpack_string(Rest) of
                {ok, Msg, _} -> ensure_binary(Msg);
                {error, _} -> undefined
            end,
            {ok, #batch_job_response{
                job_id = JobId,
                step_id = StepId,
                error_code = ErrorCode,
                job_submit_user_msg = UserMsg
            }};
        <<JobId:32/big, StepId:32/big, ErrorCode:32/big>> ->
            {ok, #batch_job_response{
                job_id = JobId,
                step_id = StepId,
                error_code = ErrorCode
            }};
        <<JobId:32/big, StepId:32/big>> ->
            {ok, #batch_job_response{
                job_id = JobId,
                step_id = StepId
            }};
        <<JobId:32/big>> ->
            {ok, #batch_job_response{job_id = JobId}};
        <<>> ->
            {ok, #batch_job_response{}};
        _ ->
            {error, invalid_batch_job_response}
    end.

%% Decode RESPONSE_JOB_INFO (2004) - Simplified
decode_job_info_response(Binary) ->
    case Binary of
        <<LastUpdate:64/big, JobCount:32/big, Rest/binary>> ->
            Jobs = decode_job_info_list(JobCount, Rest, []),
            {ok, #job_info_response{
                last_update = LastUpdate,
                job_count = JobCount,
                jobs = Jobs
            }};
        <<LastUpdate:64/big, JobCount:32/big>> ->
            {ok, #job_info_response{
                last_update = LastUpdate,
                job_count = JobCount,
                jobs = []
            }};
        <<>> ->
            {ok, #job_info_response{}};
        _ ->
            {error, invalid_job_info_response}
    end.

decode_job_info_list(0, _Binary, Acc) ->
    lists:reverse(Acc);
decode_job_info_list(Count, Binary, Acc) when Count > 0 ->
    case decode_single_job_info(Binary) of
        {ok, JobInfo, Rest} ->
            decode_job_info_list(Count - 1, Rest, [JobInfo | Acc]);
        {error, _} ->
            lists:reverse(Acc)
    end.

%% Decode a single job_info record (simplified)
decode_single_job_info(Binary) ->
    try
        {ok, Account, R1} = flurm_protocol_pack:unpack_string(Binary),
        {ok, AccrueTime, R2} = flurm_protocol_pack:unpack_time(R1),
        {ok, AdminComment, R3} = flurm_protocol_pack:unpack_string(R2),
        {ok, AllocNode, R4} = flurm_protocol_pack:unpack_string(R3),
        {ok, AllocSid, R5} = flurm_protocol_pack:unpack_uint32(R4),
        {ok, JobId, R6} = flurm_protocol_pack:unpack_uint32(R5),
        {ok, JobState, R7} = flurm_protocol_pack:unpack_uint32(R6),
        {ok, Name, R8} = flurm_protocol_pack:unpack_string(R7),
        {ok, Partition, R9} = flurm_protocol_pack:unpack_string(R8),
        {ok, Nodes, R10} = flurm_protocol_pack:unpack_string(R9),
        {ok, UserId, R11} = flurm_protocol_pack:unpack_uint32(R10),
        {ok, GroupId, R12} = flurm_protocol_pack:unpack_uint32(R11),
        {ok, NumNodes, R13} = flurm_protocol_pack:unpack_uint32(R12),
        {ok, NumCpus, R14} = flurm_protocol_pack:unpack_uint32(R13),
        {ok, NumTasks, R15} = flurm_protocol_pack:unpack_uint32(R14),
        {ok, Priority, R16} = flurm_protocol_pack:unpack_uint32(R15),
        {ok, TimeLimit, R17} = flurm_protocol_pack:unpack_uint32(R16),
        {ok, StartTime, R18} = flurm_protocol_pack:unpack_time(R17),
        {ok, EndTime, R19} = flurm_protocol_pack:unpack_time(R18),
        {ok, SubmitTime, Rest} = flurm_protocol_pack:unpack_time(R19),

        JobInfo = #job_info{
            account = ensure_binary(Account),
            accrue_time = ensure_integer(AccrueTime),
            admin_comment = ensure_binary(AdminComment),
            alloc_node = ensure_binary(AllocNode),
            alloc_sid = ensure_integer(AllocSid),
            job_id = ensure_integer(JobId),
            job_state = ensure_integer(JobState),
            name = ensure_binary(Name),
            partition = ensure_binary(Partition),
            nodes = ensure_binary(Nodes),
            user_id = ensure_integer(UserId),
            group_id = ensure_integer(GroupId),
            num_nodes = ensure_integer(NumNodes),
            num_cpus = ensure_integer(NumCpus),
            num_tasks = ensure_integer(NumTasks),
            priority = ensure_integer(Priority),
            time_limit = ensure_integer(TimeLimit),
            start_time = ensure_integer(StartTime),
            end_time = ensure_integer(EndTime),
            submit_time = ensure_integer(SubmitTime)
        },
        {ok, JobInfo, Rest}
    catch
        _:_ ->
            {error, invalid_job_info}
    end.

%% Decode RESPONSE_PARTITION_INFO (2010)
decode_partition_info_response(Binary) ->
    case Binary of
        <<PartCount:32/big, LastUpdate:64/big, Rest/binary>> ->
            Partitions = decode_partition_list(PartCount, Rest, []),
            {ok, #partition_info_response{
                last_update = LastUpdate,
                partition_count = PartCount,
                partitions = Partitions
            }};
        <<PartCount:32/big, LastUpdate:64/big>> ->
            {ok, #partition_info_response{
                last_update = LastUpdate,
                partition_count = PartCount,
                partitions = []
            }};
        <<>> ->
            {ok, #partition_info_response{}};
        _ ->
            {error, invalid_partition_info_response}
    end.

decode_partition_list(0, _Binary, Acc) ->
    lists:reverse(Acc);
decode_partition_list(Count, Binary, Acc) when Count > 0 ->
    case decode_single_partition_info(Binary) of
        {ok, PartInfo, Rest} ->
            decode_partition_list(Count - 1, Rest, [PartInfo | Acc]);
        {error, _} ->
            lists:reverse(Acc)
    end.

%% Decode a single partition_info record (simplified)
%% Matches the format from encode_single_partition_info
decode_single_partition_info(Binary) ->
    try
        %% Field 1: name
        {ok, Name, R1} = flurm_protocol_pack:unpack_string(Binary),
        %% Fields 2-9: cpu_bind, grace_time, max_time, default_time, max_nodes, min_nodes, total_nodes, total_cpus
        {ok, _CpuBind, R2} = flurm_protocol_pack:unpack_uint32(R1),
        {ok, _GraceTime, R3} = flurm_protocol_pack:unpack_uint32(R2),
        {ok, MaxTime, R4} = flurm_protocol_pack:unpack_uint32(R3),
        {ok, DefaultTime, R5} = flurm_protocol_pack:unpack_uint32(R4),
        {ok, MaxNodes, R6} = flurm_protocol_pack:unpack_uint32(R5),
        {ok, MinNodes, R7} = flurm_protocol_pack:unpack_uint32(R6),
        {ok, TotalNodes, R8} = flurm_protocol_pack:unpack_uint32(R7),
        {ok, TotalCpus, R9} = flurm_protocol_pack:unpack_uint32(R8),
        %% Fields 10-12: def_mem_per_cpu (uint64), max_cpus_per_node, max_mem_per_cpu (uint64)
        {ok, _DefMemPerCpu, R10} = flurm_protocol_pack:unpack_uint64(R9),
        {ok, _MaxCpusPerNode, R11} = flurm_protocol_pack:unpack_uint32(R10),
        {ok, _MaxMemPerCpu, R12} = flurm_protocol_pack:unpack_uint64(R11),
        %% Fields 13-22: 10 uint16 values
        {ok, _Flags, R13} = flurm_protocol_pack:unpack_uint16(R12),
        {ok, _MaxShare, R14} = flurm_protocol_pack:unpack_uint16(R13),
        {ok, _OverTimeLimit, R15} = flurm_protocol_pack:unpack_uint16(R14),
        {ok, _PreemptMode, R16} = flurm_protocol_pack:unpack_uint16(R15),
        {ok, PriorityJobFactor, R17} = flurm_protocol_pack:unpack_uint16(R16),
        {ok, PriorityTier, R18} = flurm_protocol_pack:unpack_uint16(R17),
        {ok, StateUp, R19} = flurm_protocol_pack:unpack_uint16(R18),
        {ok, _CrType, R20} = flurm_protocol_pack:unpack_uint16(R19),
        {ok, _ResumeTimeout, R21} = flurm_protocol_pack:unpack_uint16(R20),
        {ok, _SuspendTimeout, R22} = flurm_protocol_pack:unpack_uint16(R21),
        %% Field 23: suspend_time (uint32)
        {ok, _SuspendTime, R23} = flurm_protocol_pack:unpack_uint32(R22),
        %% Fields 24-33: 10 strings
        {ok, _AllowAccounts, R24} = flurm_protocol_pack:unpack_string(R23),
        {ok, _AllowGroups, R25} = flurm_protocol_pack:unpack_string(R24),
        {ok, _AllowAllocNodes, R26} = flurm_protocol_pack:unpack_string(R25),
        {ok, _AllowQos, R27} = flurm_protocol_pack:unpack_string(R26),
        {ok, _QosChar, R28} = flurm_protocol_pack:unpack_string(R27),
        {ok, _Alternate, R29} = flurm_protocol_pack:unpack_string(R28),
        {ok, _DenyAccounts, R30} = flurm_protocol_pack:unpack_string(R29),
        {ok, _DenyQos, R31} = flurm_protocol_pack:unpack_string(R30),
        {ok, Nodes, R32} = flurm_protocol_pack:unpack_string(R31),
        {ok, _Nodesets, R33} = flurm_protocol_pack:unpack_string(R32),
        %% Field 34: node_inx (NO_VAL marker)
        <<_NoVal:32/big, R34/binary>> = R33,
        %% Fields 35-36: 2 strings
        {ok, _BillingWeights, R35} = flurm_protocol_pack:unpack_string(R34),
        {ok, _TresFmt, R36} = flurm_protocol_pack:unpack_string(R35),
        %% Field 37: job_defaults_list count
        <<_JobDefaultsCount:32/big, Rest/binary>> = R36,

        PartInfo = #partition_info{
            name = ensure_binary(Name),
            max_time = ensure_integer(MaxTime),
            default_time = ensure_integer(DefaultTime),
            max_nodes = ensure_integer(MaxNodes),
            min_nodes = ensure_integer(MinNodes),
            total_nodes = ensure_integer(TotalNodes),
            total_cpus = ensure_integer(TotalCpus),
            priority_job_factor = ensure_integer(PriorityJobFactor),
            priority_tier = ensure_integer(PriorityTier),
            state_up = ensure_integer(StateUp),
            nodes = ensure_binary(Nodes)
        },
        {ok, PartInfo, Rest}
    catch
        _:_ ->
            {error, invalid_partition_info}
    end.

%%%===================================================================
%%% Request Encoders
%%%===================================================================

%% Encode REQUEST_NODE_REGISTRATION_STATUS (1001)
encode_node_registration_request(#node_registration_request{status_only = StatusOnly}) ->
    Flag = case StatusOnly of true -> 1; false -> 0 end,
    {ok, <<Flag:8>>}.

%% Encode REQUEST_JOB_INFO (2003)
encode_job_info_request(#job_info_request{
    show_flags = ShowFlags,
    job_id = JobId,
    user_id = UserId
}) ->
    {ok, <<ShowFlags:32/big, JobId:32/big, UserId:32/big>>}.

%% Encode REQUEST_SUBMIT_BATCH_JOB (4003) - Simplified
%% Handle legacy job_submit_req record by converting to batch_job_request
encode_batch_job_request(#job_submit_req{} = Req) ->
    BatchReq = #batch_job_request{
        name = Req#job_submit_req.name,
        script = Req#job_submit_req.script,
        partition = Req#job_submit_req.partition,
        min_nodes = Req#job_submit_req.num_nodes,
        max_nodes = Req#job_submit_req.num_nodes,
        min_cpus = Req#job_submit_req.num_cpus,
        cpus_per_task = 1,
        num_tasks = Req#job_submit_req.num_cpus,
        min_mem_per_node = Req#job_submit_req.memory_mb,
        time_limit = Req#job_submit_req.time_limit,
        priority = Req#job_submit_req.priority,
        work_dir = Req#job_submit_req.working_dir,
        environment = maps:fold(fun(K, V, Acc) ->
            [<<(atom_to_binary(K, utf8))/binary, "=", V/binary>> | Acc]
        end, [], Req#job_submit_req.env)
    },
    encode_batch_job_request(BatchReq);

encode_batch_job_request(#batch_job_request{} = Req) ->
    Parts = [
        flurm_protocol_pack:pack_string(Req#batch_job_request.account),
        flurm_protocol_pack:pack_string(Req#batch_job_request.acctg_freq),
        flurm_protocol_pack:pack_string(Req#batch_job_request.admin_comment),
        flurm_protocol_pack:pack_string(Req#batch_job_request.alloc_node),
        flurm_protocol_pack:pack_uint16(Req#batch_job_request.alloc_resp_port),
        flurm_protocol_pack:pack_uint32(Req#batch_job_request.alloc_sid),
        flurm_protocol_pack:pack_uint32(length(Req#batch_job_request.argv)),
        encode_string_list(Req#batch_job_request.argv),
        flurm_protocol_pack:pack_string(Req#batch_job_request.name),
        flurm_protocol_pack:pack_string(Req#batch_job_request.partition),
        flurm_protocol_pack:pack_string(Req#batch_job_request.script),
        flurm_protocol_pack:pack_string(Req#batch_job_request.work_dir),
        flurm_protocol_pack:pack_uint32(Req#batch_job_request.min_nodes),
        flurm_protocol_pack:pack_uint32(Req#batch_job_request.max_nodes),
        flurm_protocol_pack:pack_uint32(Req#batch_job_request.min_cpus),
        flurm_protocol_pack:pack_uint32(Req#batch_job_request.num_tasks),
        flurm_protocol_pack:pack_uint32(Req#batch_job_request.cpus_per_task),
        flurm_protocol_pack:pack_uint32(Req#batch_job_request.time_limit),
        flurm_protocol_pack:pack_uint32(Req#batch_job_request.priority),
        flurm_protocol_pack:pack_uint32(Req#batch_job_request.user_id),
        flurm_protocol_pack:pack_uint32(Req#batch_job_request.group_id)
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode REQUEST_CANCEL_JOB (4006)
encode_cancel_job_request(#cancel_job_request{
    job_id = JobId,
    job_id_str = JobIdStr,
    step_id = StepId,
    signal = Signal,
    flags = Flags
}) ->
    Parts = [
        <<JobId:32/big, StepId:32/big, Signal:32/big, Flags:32/big>>,
        flurm_protocol_pack:pack_string(JobIdStr)
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode REQUEST_KILL_JOB (5032)
encode_kill_job_request(#kill_job_request{
    job_id_str = JobIdStr,
    step_id = StepId,
    signal = Signal,
    flags = Flags,
    sibling = Sibling
}) ->
    Parts = [
        flurm_protocol_pack:pack_string(JobIdStr),
        <<StepId:32/big-signed, Signal:16/big, Flags:16/big>>,
        flurm_protocol_pack:pack_string(Sibling)
    ],
    {ok, iolist_to_binary(Parts)}.

%%%===================================================================
%%% Response Encoders
%%%===================================================================

%% Encode RESPONSE_SLURM_RC (8001)
encode_slurm_rc_response(#slurm_rc_response{return_code = RC}) ->
    {ok, <<RC:32/big-signed>>}.

%% Encode RESPONSE_JOB_READY (4020)
%% srun sends REQUEST_JOB_READY (4019) to check if nodes are ready
%% SLURM uses return_code_msg_t format - same as RESPONSE_SLURM_RC
%% Format: just return_code(32) - 0 = ready, non-zero = error
encode_job_ready_response(#{return_code := RC}) ->
    {ok, <<RC:32/big-signed>>};
encode_job_ready_response(RC) when is_integer(RC) ->
    {ok, <<RC:32/big-signed>>}.

%% Encode SRUN_JOB_COMPLETE (7004)
%% Sent to srun to indicate job is ready/complete
%% SLURM 22.05 format: slurm_step_id_t has job_id, step_id, step_het_comp
encode_srun_job_complete(#srun_job_complete{job_id = JobId, step_id = StepId}) ->
    %% step_het_comp is 0 for non-heterogeneous jobs
    StepHetComp = 0,
    {ok, <<JobId:32/big, StepId:32/big, StepHetComp:32/big>>}.

%% Encode SRUN_PING (7001)
%% According to SLURM source (slurm_protocol_pack.c), SRUN_PING has NO BODY.
%% The message type alone indicates a ping request.
encode_srun_ping(#srun_ping{}) ->
    %% SRUN_PING contains no body/information - just an empty body
    {ok, <<>>}.

%% Encode RESPONSE_SUBMIT_BATCH_JOB (4004)
encode_batch_job_response(#batch_job_response{
    job_id = JobId,
    step_id = StepId,
    error_code = ErrorCode,
    job_submit_user_msg = UserMsg
}) ->
    Parts = [
        <<JobId:32/big, StepId:32/big, ErrorCode:32/big>>,
        flurm_protocol_pack:pack_string(UserMsg)
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode RESPONSE_RESOURCE_ALLOCATION (4002)
%% For srun: this returns job allocation info
%% SLURM 22.05 _unpack_resource_allocation_response_msg format:
%% From src/common/slurm_protocol_pack.c (SLURM_22_05_PROTOCOL_VERSION):
%%  1. account: string
%%  2. alias_list: string
%%  3. batch_host: string
%%  4. environment: string array
%%  5. error_code: uint32
%%  6. job_submit_user_msg: string
%%  7. job_id: uint32
%%  8. node_cnt: uint32
%%  9. node_addr_present: uint8 (0 = no, 1 = yes)
%% 10. (node_addr array if present)
%% 11. node_list: string
%% 12. ntasks_per_board: uint16
%% 13. ntasks_per_core: uint16
%% 14. ntasks_per_tres: uint16
%% 15. ntasks_per_socket: uint16
%% 16. num_cpu_groups: uint32
%% 17. (cpus_per_node array if num_cpu_groups > 0)
%% 18. (cpu_count_reps array if num_cpu_groups > 0)
%% 19. partition: string
%% 20. pn_min_memory: uint64
%% 21. qos: string
%% 22. resv_name: string
%% 23. select_jobinfo (via select_g_select_jobinfo_unpack)
%% 24. cluster_rec_present: uint8 (0 = NULL, 1 = present)
%% 25. (working_cluster_rec if present)

%% Main encoding function - handles all cases including errors
%% (The main clause properly handles undefined fields with defaults)
encode_resource_allocation_response(#resource_allocation_response{
    job_id = JobId,
    node_list = NodeList,
    num_nodes = NumNodes,
    partition = Partition,
    error_code = ErrorCode,
    job_submit_user_msg = UserMsg,
    cpus_per_node = CpusPerNode,
    num_cpu_groups = NumCpuGroups,
    node_addrs = NodeAddrs
}) ->
    %% Use actual NodeList or empty string
    NodeListBin = case NodeList of
        undefined -> <<>>;
        _ when is_binary(NodeList) -> NodeList;
        _ when is_list(NodeList) -> list_to_binary(NodeList);
        _ -> <<>>
    end,

    %% Use actual NumNodes or 0
    NodeCnt = case NumNodes of
        undefined -> 0;
        N when is_integer(N), N >= 0 -> N;
        _ -> 0
    end,

    %% Pack node addresses if present
    %% SLURM format:
    %%   node_addr_present:8 (0 or 1)
    %%   if present: count:32, then for each: slurm_addr_t
    %%   slurm_addr_t for IPv4: <<AF_INET:16/big, IP:32/big, Port:16/big>>
    NodeAddrParts = case NodeAddrs of
        [] ->
            %% No addresses - node_addr_present = 0
            [<<0:8>>];
        Addrs when is_list(Addrs) ->
            %% Pack addresses - node_addr_present = 1, then count, then addresses
            AddrCount = length(Addrs),
            AddrBins = [pack_slurm_addr(IP, Port) || {IP, Port} <- Addrs],
            [<<1:8, AddrCount:32/big>> | AddrBins]
    end,

    Parts = [
        %% 1. account
        flurm_protocol_pack:pack_string(<<>>),
        %% 2. alias_list (deprecated, pack null)
        flurm_protocol_pack:pack_string(<<>>),
        %% 3. batch_host
        flurm_protocol_pack:pack_string(<<>>),
        %% 4. environment - empty string array (count=0)
        <<0:32/big>>,
        %% 5. error_code
        <<ErrorCode:32/big>>,
        %% 6. job_submit_user_msg
        flurm_protocol_pack:pack_string(UserMsg),
        %% 7. job_id
        <<JobId:32/big>>,
        %% 8. node_cnt
        <<NodeCnt:32/big>>,
        %% 9-10. node_addr_present + node_addr (if present)
        NodeAddrParts,
        %% 11. node_list
        flurm_protocol_pack:pack_string(NodeListBin),
        %% 12. ntasks_per_board = NO_VAL16
        <<16#FFFF:16/big>>,
        %% 13. ntasks_per_core = NO_VAL16
        <<16#FFFF:16/big>>,
        %% 14. ntasks_per_tres = NO_VAL16
        <<16#FFFF:16/big>>,
        %% 15. ntasks_per_socket = NO_VAL16
        <<16#FFFF:16/big>>,
        %% 16. num_cpu_groups
        %% 17. cpus_per_node array (uint16 per group)
        %% 18. cpu_count_reps array (uint32 per group - how many nodes have this CPU count)
        encode_cpu_groups(NumCpuGroups, CpusPerNode, NodeCnt),
        %% 19. partition
        flurm_protocol_pack:pack_string(Partition),
        %% 20. pn_min_memory = 0
        <<0:64/big>>,
        %% 21. qos
        flurm_protocol_pack:pack_string(<<>>),
        %% 22. resv_name
        flurm_protocol_pack:pack_string(<<>>),
        %% 23. select_jobinfo via select_g_select_jobinfo_unpack:
        %%     First: plugin_id: uint32 (109 = SELECT_PLUGIN_CONS_TRES)
        %%     Then plugin-specific data: NONE for cons_tres/cons_common
        %%     (select_p_select_jobinfo_pack in cons_common.c is empty - packs nothing)
        <<109:32/big>>,
        %% 24. cluster_rec_present = 0 (NULL)
        <<0:8>>
        %% 25. (skip working_cluster_rec - not present)
    ],
    {ok, iolist_to_binary(Parts)}.

%% Pack a SLURM network address for address arrays (slurm_pack_addr format)
%% Wire format from slurm_protocol_socket.c slurm_pack_addr():
%%   For AF_INET:
%%     - family: uint16 (big-endian via pack16)
%%     - addr: uint32 (big-endian via pack32)
%%     - port: uint16 (big-endian via pack16)
%% Total: 2 + 4 + 2 = 8 bytes per address
%% NOTE: This is different from packmem format which includes raw sockaddr_in with padding!
%% IP is a tuple like {172, 19, 0, 3}
pack_slurm_addr({A, B, C, D}, Port) when is_integer(A), is_integer(B),
                                         is_integer(C), is_integer(D),
                                         is_integer(Port) ->
    %% AF_INET = 2 for IPv4
    %% SLURM's slurm_pack_addr() packs the address directly (NO ntohl):
    %%   pack32(in->sin_addr.s_addr, buffer);
    %% sin_addr.s_addr is already in network byte order (big-endian).
    %% So for IP 172.18.0.3, wire format is AC 12 00 03 (network order).
    IP = (A bsl 24) bor (B bsl 16) bor (C bsl 8) bor D,
    <<2:16/big,       % family = AF_INET
      IP:32/big,      % addr (network order, same as sin_addr.s_addr)
      Port:16/big>>;  % port (network order)
pack_slurm_addr(IPBin, Port) when is_binary(IPBin), is_integer(Port) ->
    %% Parse "172.19.0.3" format
    case inet:parse_address(binary_to_list(IPBin)) of
        {ok, {A, B, C, D}} ->
            pack_slurm_addr({A, B, C, D}, Port);
        _ ->
            %% Fallback to localhost
            pack_slurm_addr({127, 0, 0, 1}, Port)
    end.

%% Encode CPU groups for resource allocation response
%% SLURM wire format (from slurm_protocol_pack.c _pack_resource_allocation_response_msg):
%%   - num_cpu_groups: uint32
%%   - If num_cpu_groups > 0:
%%     - cpus_per_node: pack16_array (array_count:32, elements:16 each)
%%     - cpu_count_reps: pack32_array (array_count:32, elements:32 each)
%%
%% Example: 1 group with 1 CPU per node, 1 node total:
%%   num_cpu_groups=1
%%   cpus_per_node_count=1, cpus_per_node[0]=1
%%   cpu_count_reps_count=1, cpu_count_reps[0]=1
encode_cpu_groups(0, _CpusPerNode, _NodeCnt) ->
    %% No CPU groups - just the count
    <<0:32/big>>;
encode_cpu_groups(NumCpuGroups, CpusPerNode, NodeCnt) when is_integer(NumCpuGroups), NumCpuGroups > 0 ->
    %% Ensure CpusPerNode is a list
    CpusList = case CpusPerNode of
        L when is_list(L) -> L;
        N when is_integer(N) -> [N];
        undefined -> [1];
        _ -> [1]
    end,
    %% Pad or truncate to match NumCpuGroups
    PaddedCpus = pad_list(CpusList, NumCpuGroups, 1),
    %% Calculate node counts per group (distribute evenly, last group gets remainder)
    %% For simplicity, if 1 group, all nodes go to that group
    NodeCounts = distribute_nodes(NumCpuGroups, NodeCnt),
    %% Encode using SLURM's pack16_array and pack32_array format:
    %% pack16_array: array_count:32, then uint16 elements
    %% pack32_array: array_count:32, then uint32 elements
    CpusBin = << <<C:16/big>> || C <- PaddedCpus >>,
    CountsBin = << <<N:32/big>> || N <- NodeCounts >>,
    <<NumCpuGroups:32/big,
      NumCpuGroups:32/big, CpusBin/binary,      %% pack16_array for cpus_per_node
      NumCpuGroups:32/big, CountsBin/binary>>;  %% pack32_array for cpu_count_reps
encode_cpu_groups(_, _, _) ->
    %% Fallback - no groups
    <<0:32/big>>.

%% Pad a list to exactly N elements, filling with Default
pad_list(List, N, Default) ->
    Len = length(List),
    if
        Len >= N -> lists:sublist(List, N);
        true -> List ++ lists:duplicate(N - Len, Default)
    end.

%% Distribute NodeCnt nodes across NumGroups groups
%% Returns a list of node counts per group
distribute_nodes(1, NodeCnt) ->
    [NodeCnt];
distribute_nodes(NumGroups, NodeCnt) when NumGroups > 0 ->
    Base = NodeCnt div NumGroups,
    Remainder = NodeCnt rem NumGroups,
    %% First Remainder groups get Base+1, rest get Base
    [if I =< Remainder -> Base + 1; true -> Base end || I <- lists:seq(1, NumGroups)].

%% Encode RESPONSE_JOB_INFO (2004)
%% Note: For SLURM 21.08+ (protocol version >= 0x2500), the format is:
%%   record_count:32, last_update:time_t, job_data...
%% The record_count comes FIRST, then the timestamp.
encode_job_info_response(#job_info_response{
    last_update = LastUpdate,
    job_count = JobCount,
    jobs = Jobs
}) ->
    JobsBin = [encode_single_job_info(J) || J <- Jobs],
    %% From SLURM source slurm_protocol_pack.c _pack_job_info_msg:
    %% 1. record_count: 32-bit
    %% 2. last_update: time_t (64-bit)
    %% 3. last_backfill: time_t (64-bit)
    LastBackfill = 0,  % No backfill scheduler yet
    Parts = [
        <<JobCount:32/big, LastUpdate:64/big, LastBackfill:64/big>>,
        JobsBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single job_info record - SLURM 22.05 format
%% Fields MUST be in exact order matching SLURM's _unpack_job_info_members()
%% From src/common/slurm_protocol_pack.c
encode_single_job_info(#job_info{} = J) ->
    %% SLURM 22.05 unpacks ALL fields unconditionally - no conditional packing!
    [
        %% Fields 1-10: array_job_id, array_task_id, array_task_str, array_max_tasks,
        %% assoc_id, container, delay_boot, job_id, user_id, group_id
        flurm_protocol_pack:pack_uint32(J#job_info.array_job_id),
        flurm_protocol_pack:pack_uint32(J#job_info.array_task_id),
        flurm_protocol_pack:pack_string(J#job_info.array_task_str),  % Always pack
        flurm_protocol_pack:pack_uint32(J#job_info.array_max_tasks),
        flurm_protocol_pack:pack_uint32(J#job_info.assoc_id),
        flurm_protocol_pack:pack_string(J#job_info.container),
        flurm_protocol_pack:pack_uint32(J#job_info.delay_boot),
        flurm_protocol_pack:pack_uint32(J#job_info.job_id),
        flurm_protocol_pack:pack_uint32(J#job_info.user_id),
        flurm_protocol_pack:pack_uint32(J#job_info.group_id),
        %% Fields 11-20: het_job_id, het_job_id_set, het_job_offset, profile,
        %% job_state, batch_flag, state_reason, power_flags, reboot, restart_cnt
        flurm_protocol_pack:pack_uint32(J#job_info.het_job_id),
        flurm_protocol_pack:pack_string(J#job_info.het_job_id_set),  % Always pack
        flurm_protocol_pack:pack_uint32(J#job_info.het_job_offset),
        flurm_protocol_pack:pack_uint32(J#job_info.profile),
        flurm_protocol_pack:pack_uint32(J#job_info.job_state),
        flurm_protocol_pack:pack_uint16(J#job_info.batch_flag),
        flurm_protocol_pack:pack_uint16(J#job_info.state_reason),
        flurm_protocol_pack:pack_uint8(J#job_info.power_flags),
        flurm_protocol_pack:pack_uint8(J#job_info.reboot),
        flurm_protocol_pack:pack_uint16(J#job_info.restart_cnt),
        %% Fields 21-30: show_flags, deadline, alloc_sid, time_limit, time_min,
        %% nice, submit_time, eligible_time, accrue_time, start_time
        flurm_protocol_pack:pack_uint16(J#job_info.show_flags),
        flurm_protocol_pack:pack_time(J#job_info.deadline),
        flurm_protocol_pack:pack_uint32(J#job_info.alloc_sid),
        flurm_protocol_pack:pack_uint32(J#job_info.time_limit),
        flurm_protocol_pack:pack_uint32(J#job_info.time_min),
        flurm_protocol_pack:pack_uint32(J#job_info.nice),
        flurm_protocol_pack:pack_time(J#job_info.submit_time),
        flurm_protocol_pack:pack_time(J#job_info.eligible_time),
        flurm_protocol_pack:pack_time(J#job_info.accrue_time),
        flurm_protocol_pack:pack_time(J#job_info.start_time),
        %% Fields 31-40: end_time, suspend_time, pre_sus_time, resize_time,
        %% last_sched_eval, preempt_time, priority, billable_tres, cluster, nodes
        flurm_protocol_pack:pack_time(J#job_info.end_time),
        flurm_protocol_pack:pack_time(J#job_info.suspend_time),
        flurm_protocol_pack:pack_time(J#job_info.pre_sus_time),
        flurm_protocol_pack:pack_time(J#job_info.resize_time),
        flurm_protocol_pack:pack_time(J#job_info.last_sched_eval),
        flurm_protocol_pack:pack_time(J#job_info.preempt_time),
        flurm_protocol_pack:pack_uint32(J#job_info.priority),
        flurm_protocol_pack:pack_double(J#job_info.billable_tres),
        flurm_protocol_pack:pack_string(J#job_info.cluster),
        flurm_protocol_pack:pack_string(J#job_info.nodes),
        %% Fields 41-50: sched_nodes, partition, account, admin_comment,
        %% site_factor, network, comment, batch_features, batch_host, burst_buffer
        flurm_protocol_pack:pack_string(J#job_info.sched_nodes),
        flurm_protocol_pack:pack_string(J#job_info.partition),
        flurm_protocol_pack:pack_string(J#job_info.account),
        flurm_protocol_pack:pack_string(J#job_info.admin_comment),
        flurm_protocol_pack:pack_uint32(J#job_info.site_factor),
        flurm_protocol_pack:pack_string(J#job_info.network),
        flurm_protocol_pack:pack_string(J#job_info.comment),
        flurm_protocol_pack:pack_string(J#job_info.container),  % container packed TWICE in SLURM 22.05!
        flurm_protocol_pack:pack_string(<<>>),  % batch_features
        flurm_protocol_pack:pack_string(J#job_info.batch_host),
        flurm_protocol_pack:pack_string(J#job_info.burst_buffer),
        %% Fields 51-60: burst_buffer_state, system_comment, qos, preemptable_time,
        %% licenses, state_desc, resv_name, mcs_label, exit_code, derived_ec
        flurm_protocol_pack:pack_string(J#job_info.burst_buffer_state),
        flurm_protocol_pack:pack_string(J#job_info.system_comment),
        flurm_protocol_pack:pack_string(J#job_info.qos),
        flurm_protocol_pack:pack_time(J#job_info.preemptable_time),
        flurm_protocol_pack:pack_string(J#job_info.licenses),
        flurm_protocol_pack:pack_string(J#job_info.state_desc),
        flurm_protocol_pack:pack_string(J#job_info.resv_name),
        flurm_protocol_pack:pack_string(J#job_info.mcs_label),
        flurm_protocol_pack:pack_uint32(J#job_info.exit_code),
        flurm_protocol_pack:pack_uint32(J#job_info.derived_ec),
        %% Fields 61-70: gres_total (string), job_resrcs (struct), gres_detail_str/cnt,
        %% name, user_name, wckey, req_switch, wait4switch, alloc_node
        flurm_protocol_pack:pack_string(<<>>),  % gres_total
        encode_job_resources_empty(),           % job_resrcs (empty struct)
        %% gres_detail: pack count then strings (pack_string_array includes count)
        flurm_protocol_pack:pack_string_array(J#job_info.gres_detail_str),
        flurm_protocol_pack:pack_string(J#job_info.name),
        flurm_protocol_pack:pack_string(J#job_info.user_name),
        flurm_protocol_pack:pack_string(J#job_info.wckey),
        flurm_protocol_pack:pack_uint32(J#job_info.req_switch),
        flurm_protocol_pack:pack_uint32(J#job_info.wait4switch),
        flurm_protocol_pack:pack_string(J#job_info.alloc_node),
        %% Fields 71-80: node_inx (bitstring), select_jobinfo (struct), features,
        %% prefer, cluster_features, work_dir, dependency, command, num_cpus, max_cpus
        encode_bitstring_empty(),               % node_inx (empty bitstring)
        encode_select_jobinfo_empty(),          % select_jobinfo (empty struct)
        flurm_protocol_pack:pack_string(J#job_info.features),
        flurm_protocol_pack:pack_string(<<>>),  % prefer
        flurm_protocol_pack:pack_string(J#job_info.cluster_features),
        flurm_protocol_pack:pack_string(J#job_info.work_dir),
        flurm_protocol_pack:pack_string(J#job_info.dependency),
        flurm_protocol_pack:pack_string(J#job_info.command),
        flurm_protocol_pack:pack_uint32(J#job_info.num_cpus),
        flurm_protocol_pack:pack_uint32(J#job_info.max_cpus),
        %% Fields 81-90: num_nodes, max_nodes, requeue, ntasks_per_node,
        %% ntasks_per_tres, num_tasks, shared, cpu_freq_min, cpu_freq_max, cpu_freq_gov
        flurm_protocol_pack:pack_uint32(J#job_info.num_nodes),
        flurm_protocol_pack:pack_uint32(J#job_info.max_nodes),
        flurm_protocol_pack:pack_uint16(J#job_info.requeue),
        flurm_protocol_pack:pack_uint16(J#job_info.ntasks_per_node),
        flurm_protocol_pack:pack_uint16(J#job_info.ntasks_per_tres),
        flurm_protocol_pack:pack_uint32(J#job_info.num_tasks),
        flurm_protocol_pack:pack_uint16(J#job_info.shared),
        flurm_protocol_pack:pack_uint32(0),     % cpu_freq_min
        flurm_protocol_pack:pack_uint32(0),     % cpu_freq_max
        flurm_protocol_pack:pack_uint32(0),     % cpu_freq_gov
        %% Fields 91-100: cronspec, contiguous, core_spec, cpus_per_task,
        %% pn_min_cpus, pn_min_memory, pn_min_tmp_disk, req_nodes, req_node_inx, exc_nodes
        flurm_protocol_pack:pack_string(<<>>),  % cronspec
        flurm_protocol_pack:pack_uint16(J#job_info.contiguous),
        flurm_protocol_pack:pack_uint16(J#job_info.core_spec),
        flurm_protocol_pack:pack_uint16(J#job_info.cpus_per_task),
        flurm_protocol_pack:pack_uint16(J#job_info.pn_min_cpus),
        flurm_protocol_pack:pack_uint64(J#job_info.pn_min_memory),
        flurm_protocol_pack:pack_uint32(J#job_info.pn_min_tmp_disk),
        flurm_protocol_pack:pack_string(J#job_info.req_nodes),
        encode_bitstring_empty(),               % req_node_inx
        flurm_protocol_pack:pack_string(J#job_info.exc_nodes),
        %% Fields 101-110: exc_node_inx, std_err, std_in, std_out,
        %% multi_core_data, bitflags, tres_alloc_str, tres_req_str, start_protocol_ver, fed_origin_str
        encode_bitstring_empty(),               % exc_node_inx
        flurm_protocol_pack:pack_string(J#job_info.std_err),
        flurm_protocol_pack:pack_string(J#job_info.std_in),
        flurm_protocol_pack:pack_string(J#job_info.std_out),
        encode_multi_core_data_empty(),         % multi_core_data
        flurm_protocol_pack:pack_uint64(J#job_info.bitflags),  % bitflags is uint64!
        flurm_protocol_pack:pack_string(J#job_info.tres_alloc_str),
        flurm_protocol_pack:pack_string(J#job_info.tres_req_str),
        flurm_protocol_pack:pack_uint16(0),     % start_protocol_ver
        flurm_protocol_pack:pack_string(J#job_info.fed_origin_str),
        %% Fields 111-120: fed_siblings_active, fed_siblings_active_str,
        %% fed_siblings_viable, fed_siblings_viable_str, cpus_per_tres, mem_per_tres,
        %% tres_bind, tres_freq, tres_per_job, tres_per_node
        flurm_protocol_pack:pack_uint64(J#job_info.fed_siblings_active),
        flurm_protocol_pack:pack_string(J#job_info.fed_siblings_active_str),
        flurm_protocol_pack:pack_uint64(J#job_info.fed_siblings_viable),
        flurm_protocol_pack:pack_string(J#job_info.fed_siblings_viable_str),
        flurm_protocol_pack:pack_string(J#job_info.cpus_per_tres),
        flurm_protocol_pack:pack_string(J#job_info.mem_per_tres),
        flurm_protocol_pack:pack_string(J#job_info.tres_bind),
        flurm_protocol_pack:pack_string(J#job_info.tres_freq),
        flurm_protocol_pack:pack_string(J#job_info.tres_per_job),
        flurm_protocol_pack:pack_string(J#job_info.tres_per_node),
        %% Fields 121-126: tres_per_socket, tres_per_task, mail_type, mail_user, selinux_context
        flurm_protocol_pack:pack_string(J#job_info.tres_per_socket),
        flurm_protocol_pack:pack_string(J#job_info.tres_per_task),
        flurm_protocol_pack:pack_uint16(J#job_info.mail_type),
        flurm_protocol_pack:pack_string(J#job_info.mail_user),
        flurm_protocol_pack:pack_string(<<>>)   % selinux_context
    ].

%% Empty job_resources structure - indicates no allocated resources
%% Format: NO_VAL (0xFFFFFFFF) indicates null job_resources
encode_job_resources_empty() ->
    <<?SLURM_NO_VAL:32/big>>.

%% Empty bitstring index - indicates no nodes
%% Format: NO_VAL (0xFFFFFFFE) indicates NULL bitstring
%% From SLURM pack.h: pack_bit_str_hex packs NO_VAL for NULL bitmap
encode_bitstring_empty() ->
    <<?SLURM_NO_VAL:32/big>>.

%% Encode node_inx as a SLURM pack_bit_str_hex format.
%% Wire format: uint32 bit_count (or NO_VAL for NULL), then packstr(hex_mask).
%% The hex mask is a string like "0x07" representing which node indices are set.
%% node_inx is a list of indices (0-based) into the global node list.
encode_node_inx([]) ->
    <<?SLURM_NO_VAL:32/big>>;
encode_node_inx(Indices) when is_list(Indices) ->
    %% Find the highest index to determine bitmap size
    MaxIdx = lists:max(Indices),
    BitCount = MaxIdx + 1,
    %% Build a bitmap integer with bits set at each index
    BitmapInt = lists:foldl(fun(Idx, Acc) -> Acc bor (1 bsl Idx) end, 0, Indices),
    %% Convert to hex string (SLURM format: "0x" prefix, MSB left, LSB right)
    HexStr = iolist_to_binary(io_lib:format("0x~.16B", [BitmapInt])),
    [<<BitCount:32/big>>, flurm_protocol_pack:pack_string(HexStr)].

%% Empty select_jobinfo structure
%% From SLURM select.c: select_g_select_jobinfo_pack packs plugin_id first,
%% then the plugin's data. For cons_tres (plugin_id=109), the data is empty.
-define(SELECT_PLUGIN_CONS_TRES, 109).
encode_select_jobinfo_empty() ->
    %% Pack plugin_id first (uint32), then empty data for cons_tres
    <<?SELECT_PLUGIN_CONS_TRES:32/big>>.

%% Empty multi_core_data structure
%% Format: single byte 0 to indicate null multi_core_data
%% From SLURM slurm_protocol_pack.c: pack8((uint8_t) 0, buffer) for NULL
encode_multi_core_data_empty() ->
    <<0:8>>.

%% Encode REQUEST_NODE_INFO (2007)
encode_node_info_request(#node_info_request{
    show_flags = ShowFlags,
    node_name = NodeName
}) ->
    Parts = [
        <<ShowFlags:32/big>>,
        flurm_protocol_pack:pack_string(NodeName)
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode RESPONSE_NODE_INFO (2008)
%% From SLURM source slurm_protocol_pack.c _pack_node_info_msg:
%% 1. record_count: 32-bit
%% 2. last_update: time_t (64-bit)
encode_node_info_response(#node_info_response{
    last_update = LastUpdate,
    node_count = NodeCount,
    nodes = Nodes
}) ->
    NodesBin = [encode_single_node_info(N) || N <- Nodes],
    Parts = [
        <<NodeCount:32/big, LastUpdate:64/big>>,
        NodesBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single node_info record - SLURM 22.05 format
%% Fields MUST be in exact order matching SLURM's _unpack_node_info_members()
encode_single_node_info(#node_info{} = N) ->
    [
        %% Fields 1-5: name, node_hostname, node_addr, bcast_address, port
        flurm_protocol_pack:pack_string(N#node_info.name),
        flurm_protocol_pack:pack_string(N#node_info.node_hostname),
        flurm_protocol_pack:pack_string(N#node_info.node_addr),
        flurm_protocol_pack:pack_string(<<>>),  % bcast_address
        flurm_protocol_pack:pack_uint16(N#node_info.port),
        %% Fields 6-8: next_state, node_state, version
        flurm_protocol_pack:pack_uint32(0),  % next_state
        flurm_protocol_pack:pack_uint32(N#node_info.node_state),
        flurm_protocol_pack:pack_string(N#node_info.version),
        %% Fields 9-13: cpus, boards, sockets, cores, threads (all uint16)
        flurm_protocol_pack:pack_uint16(N#node_info.cpus),
        flurm_protocol_pack:pack_uint16(N#node_info.boards),
        flurm_protocol_pack:pack_uint16(N#node_info.sockets),
        flurm_protocol_pack:pack_uint16(N#node_info.cores),
        flurm_protocol_pack:pack_uint16(N#node_info.threads),
        %% Fields 14-15: real_memory (uint64), tmp_disk (uint32)
        flurm_protocol_pack:pack_uint64(N#node_info.real_memory),
        flurm_protocol_pack:pack_uint32(N#node_info.tmp_disk),
        %% Fields 16-22: mcs_label, owner, core_spec_cnt, cpu_bind, mem_spec_limit, cpu_spec_list, cpus_efctv
        flurm_protocol_pack:pack_string(<<>>),  % mcs_label
        flurm_protocol_pack:pack_uint32(N#node_info.owner),
        flurm_protocol_pack:pack_uint16(0),  % core_spec_cnt
        flurm_protocol_pack:pack_uint32(0),  % cpu_bind
        flurm_protocol_pack:pack_uint64(0),  % mem_spec_limit
        flurm_protocol_pack:pack_string(<<>>),  % cpu_spec_list
        flurm_protocol_pack:pack_uint16(N#node_info.cpus),  % cpus_efctv = cpus
        %% Fields 23-25: cpu_load, free_mem (uint64), weight
        flurm_protocol_pack:pack_uint32(N#node_info.cpu_load),
        flurm_protocol_pack:pack_uint64(N#node_info.free_mem),
        flurm_protocol_pack:pack_uint32(N#node_info.weight),
        %% Fields 26-30: reason_uid, boot_time, last_busy, reason_time, slurmd_start_time
        flurm_protocol_pack:pack_uint32(N#node_info.reason_uid),
        flurm_protocol_pack:pack_time(N#node_info.boot_time),
        flurm_protocol_pack:pack_time(N#node_info.last_busy),
        flurm_protocol_pack:pack_time(N#node_info.reason_time),
        flurm_protocol_pack:pack_time(N#node_info.slurmd_start_time),
        %% Field 31: select_nodeinfo - plugin_id + cons_tres data
        <<?SELECT_PLUGIN_CONS_TRES:32/big>>,
        %% cons_tres packs: alloc_cpus(16) + alloc_memory(64) + tres_alloc_fmt_str + tres_alloc_weighted(double)
        <<(N#node_info.alloc_cpus):16/big>>,  % alloc_cpus
        <<(N#node_info.alloc_memory):64/big>>,  % alloc_memory
        flurm_protocol_pack:pack_string(<<>>),  % tres_alloc_fmt_str
        flurm_protocol_pack:pack_double(0.0),   % tres_alloc_weighted
        %% Fields 32-41: arch, features, features_act, gres, gres_drain, gres_used, os, comment, extra, reason
        flurm_protocol_pack:pack_string(N#node_info.arch),
        flurm_protocol_pack:pack_string(N#node_info.features),
        flurm_protocol_pack:pack_string(N#node_info.features_act),
        flurm_protocol_pack:pack_string(N#node_info.gres),
        flurm_protocol_pack:pack_string(N#node_info.gres_drain),
        flurm_protocol_pack:pack_string(N#node_info.gres_used),
        flurm_protocol_pack:pack_string(N#node_info.os),
        flurm_protocol_pack:pack_string(<<>>),  % comment
        flurm_protocol_pack:pack_string(<<>>),  % extra
        flurm_protocol_pack:pack_string(N#node_info.reason),
        %% Field 42: acct_gather_energy - full structure even for NULL:
        %% base_consumed_energy(64) + ave_watts(32) + consumed_energy(64) +
        %% current_watts(32) + previous_consumed_energy(64) + poll_time(time)
        <<0:64/big, 0:32/big, 0:64/big, 0:32/big, 0:64/big>>,
        flurm_protocol_pack:pack_time(0),  % poll_time
        %% Field 43: ext_sensors_data - full structure for NULL:
        %% consumed_energy(64) + temperature(32) + energy_update_time(time) + current_watts(32)
        <<0:64/big, 0:32/big>>,
        flurm_protocol_pack:pack_time(0),
        <<0:32/big>>,
        %% Field 44: power_mgmt_data - NO_VAL for NULL
        <<?SLURM_NO_VAL:32/big>>,
        %% Field 45: tres_fmt_str
        flurm_protocol_pack:pack_string(<<>>)
    ].

%% Encode REQUEST_PARTITION_INFO (2009)
encode_partition_info_request(#partition_info_request{
    show_flags = ShowFlags,
    partition_name = PartitionName
}) ->
    Parts = [
        <<ShowFlags:32/big>>,
        flurm_protocol_pack:pack_string(PartitionName)
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode RESPONSE_PARTITION_INFO (2010)
%% From SLURM source slurm_protocol_pack.c _pack_partition_info_msg:
%% 1. record_count: 32-bit
%% 2. last_update: time_t (64-bit)
encode_partition_info_response(#partition_info_response{
    last_update = LastUpdate,
    partition_count = PartCount,
    partitions = Partitions
}) ->
    PartsBin = [encode_single_partition_info(P) || P <- Partitions],
    Parts = [
        <<PartCount:32/big, LastUpdate:64/big>>,
        PartsBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single partition_info record - SLURM 22.05 format
%% Fields MUST be in exact order matching SLURM's _unpack_partition_info_members()
encode_single_partition_info(#partition_info{} = P) ->
    [
        %% Field 1: name
        flurm_protocol_pack:pack_string(P#partition_info.name),
        %% Fields 2-9: cpu_bind, grace_time, max_time, default_time, max_nodes, min_nodes, total_nodes, total_cpus
        flurm_protocol_pack:pack_uint32(0),  % cpu_bind
        flurm_protocol_pack:pack_uint32(0),  % grace_time
        flurm_protocol_pack:pack_uint32(P#partition_info.max_time),
        flurm_protocol_pack:pack_uint32(P#partition_info.default_time),
        flurm_protocol_pack:pack_uint32(P#partition_info.max_nodes),
        flurm_protocol_pack:pack_uint32(P#partition_info.min_nodes),
        flurm_protocol_pack:pack_uint32(P#partition_info.total_nodes),
        flurm_protocol_pack:pack_uint32(P#partition_info.total_cpus),
        %% Fields 10-12: def_mem_per_cpu (uint64), max_cpus_per_node, max_mem_per_cpu (uint64)
        flurm_protocol_pack:pack_uint64(0),  % def_mem_per_cpu
        flurm_protocol_pack:pack_uint32(16#FFFFFFFF),  % max_cpus_per_node (INFINITE)
        flurm_protocol_pack:pack_uint64(0),  % max_mem_per_cpu
        %% Fields 13-22: flags, max_share, over_time_limit, preempt_mode, priority_job_factor, priority_tier, state_up, cr_type, resume_timeout, suspend_timeout (all uint16)
        flurm_protocol_pack:pack_uint16(0),  % flags
        flurm_protocol_pack:pack_uint16(1),  % max_share
        flurm_protocol_pack:pack_uint16(0),  % over_time_limit
        flurm_protocol_pack:pack_uint16(0),  % preempt_mode
        flurm_protocol_pack:pack_uint16(P#partition_info.priority_job_factor),
        flurm_protocol_pack:pack_uint16(P#partition_info.priority_tier),
        flurm_protocol_pack:pack_uint16(P#partition_info.state_up),
        flurm_protocol_pack:pack_uint16(0),  % cr_type
        flurm_protocol_pack:pack_uint16(0),  % resume_timeout
        flurm_protocol_pack:pack_uint16(0),  % suspend_timeout
        %% Field 23: suspend_time (uint32)
        flurm_protocol_pack:pack_uint32(0),  % suspend_time
        %% Fields 24-33: allow_accounts, allow_groups, allow_alloc_nodes, allow_qos, qos_char, alternate, deny_accounts, deny_qos, nodes, nodesets
        flurm_protocol_pack:pack_string(<<>>),  % allow_accounts
        flurm_protocol_pack:pack_string(<<>>),  % allow_groups
        flurm_protocol_pack:pack_string(<<>>),  % allow_alloc_nodes
        flurm_protocol_pack:pack_string(<<>>),  % allow_qos
        flurm_protocol_pack:pack_string(<<>>),  % qos_char
        flurm_protocol_pack:pack_string(<<>>),  % alternate
        flurm_protocol_pack:pack_string(<<>>),  % deny_accounts
        flurm_protocol_pack:pack_string(<<>>),  % deny_qos
        flurm_protocol_pack:pack_string(P#partition_info.nodes),
        flurm_protocol_pack:pack_string(<<>>),  % nodesets
        %% Field 34: node_inx (bitstring) - array of int32 range pairs
        encode_node_inx(P#partition_info.node_inx),
        %% Fields 35-36: billing_weights_str, tres_fmt_str
        flurm_protocol_pack:pack_string(<<>>),  % billing_weights_str
        flurm_protocol_pack:pack_string(<<>>),  % tres_fmt_str
        %% Field 37: job_defaults_list - pack count 0 for empty list
        <<0:32/big>>
    ].

%%%===================================================================
%%% Internal Helpers
%%%===================================================================

%% Encode a list of strings
encode_string_list(Strings) ->
    [flurm_protocol_pack:pack_string(S) || S <- Strings].

%% Ensure value is binary
ensure_binary(undefined) -> <<>>;
ensure_binary(null) -> <<>>;
ensure_binary(Bin) when is_binary(Bin) -> Bin;
ensure_binary(List) when is_list(List) -> list_to_binary(List);
ensure_binary(_) -> <<>>.

%% Ensure value is integer
ensure_integer(undefined) -> 0;
ensure_integer(null) -> 0;
ensure_integer(Int) when is_integer(Int) -> Int;
ensure_integer(_) -> 0.

%%%===================================================================
%%% Job Step Request/Response Encoders and Decoders
%%%===================================================================

%% Decode REQUEST_JOB_STEP_CREATE (5001)
%% SLURM format varies by version, basic fields:
%%   job_id, step_id, user_id, min_nodes, max_nodes, num_tasks, cpus_per_task
%%   time_limit, flags, immediate, host, node_list, features, etc.
decode_job_step_create_request(Binary) ->
    try
        %% Try to extract key fields from the binary
        %% The format is complex and version-dependent, so we extract what we can
        case Binary of
            <<JobId:32/big, StepId:32/big, UserId:32/big, MinNodes:32/big,
              MaxNodes:32/big, NumTasks:32/big, CpusPerTask:32/big, TimeLimit:32/big,
              Flags:32/big, Immediate:32/big, Rest/binary>> ->
                %% Try to unpack name string
                {Name, _Rest2} = case flurm_protocol_pack:unpack_string(Rest) of
                    {ok, N, R} -> {ensure_binary(N), R};
                    _ -> {<<>>, Rest}
                end,
                {ok, #job_step_create_request{
                    job_id = JobId,
                    step_id = normalize_step_id(StepId),
                    user_id = UserId,
                    min_nodes = max(1, MinNodes),
                    max_nodes = max(1, MaxNodes),
                    num_tasks = max(1, NumTasks),
                    cpus_per_task = max(1, CpusPerTask),
                    time_limit = TimeLimit,
                    flags = Flags,
                    immediate = Immediate,
                    name = Name
                }};
            <<JobId:32/big, StepId:32/big, Rest/binary>> ->
                %% Minimal format
                {ok, #job_step_create_request{
                    job_id = JobId,
                    step_id = normalize_step_id(StepId),
                    name = extract_name_from_binary(Rest)
                }};
            <<JobId:32/big>> ->
                {ok, #job_step_create_request{job_id = JobId}};
            _ ->
                {error, invalid_job_step_create_request}
        end
    catch
        _:Reason ->
            {error, {job_step_create_decode_failed, Reason}}
    end.

%% Decode REQUEST_JOB_STEP_INFO (5003)
%% Format: show_flags(32), job_id(32), step_id(32)
decode_job_step_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, JobId:32/big, StepId:32/big, _Rest/binary>> ->
            {ok, #job_step_info_request{
                show_flags = ShowFlags,
                job_id = JobId,
                step_id = normalize_step_id(StepId)
            }};
        <<ShowFlags:32/big, JobId:32/big>> ->
            {ok, #job_step_info_request{
                show_flags = ShowFlags,
                job_id = JobId
            }};
        <<ShowFlags:32/big>> ->
            {ok, #job_step_info_request{
                show_flags = ShowFlags
            }};
        <<>> ->
            {ok, #job_step_info_request{}};
        _ ->
            {error, invalid_job_step_info_request}
    end.

%% Decode REQUEST_LAUNCH_TASKS (6001) - srun launching tasks on node
%% SLURM 22.05 wire format from _pack_launch_tasks_request_msg:
%%   1. step_id (pack_step_id): job_id(32), step_id(32), step_het_comp(32)
%%   2. uid: uint32
%%   3. gid: uint32
%%   4. user_name: string
%%   5. gids: uint32 array with count
%%   6. het_job_* fields (various)
%%   7. mpi_plugin_id: uint32
%%   8. ntasks, nnodes, etc.
%%   9. cred_version: uint16
%%  10. cred: slurm_cred
%%  11. task arrays and ports
%%  12. env, argv, cwd, etc.
decode_launch_tasks_request(Binary) ->
    try
        %% Parse step_id structure
        <<JobId:32/big, StepId:32/big, StepHetComp:32/big, Rest0/binary>> = Binary,
        lager:info("Decoded step_id: job=~p step=~p het_comp=~p", [JobId, StepId, StepHetComp]),

        %% Parse uid, gid
        <<Uid:32/big, Gid:32/big, Rest1/binary>> = Rest0,
        lager:info("Decoded uid=~p gid=~p", [Uid, Gid]),

        %% Parse user_name string
        {UserName, Rest2} = unpack_string_safe(Rest1),
        lager:info("Decoded user_name=~p", [UserName]),

        %% Parse gids array (count + array)
        <<NgIds:32/big, Rest3/binary>> = Rest2,
        {GIds, Rest4} = unpack_uint32_array(NgIds, Rest3),
        lager:info("Decoded ~p gids, Rest4 size=~p", [NgIds, byte_size(Rest4)]),

        %% Parse het_job fields
        lager:info("About to parse het_job, Rest4 first 20 bytes: ~p", [binary:part(Rest4, 0, min(20, byte_size(Rest4)))]),
        <<HetJobNodeOffset:32/big, HetJobId:32/big, HetJobNnodes:32/big, Rest5/binary>> = Rest4,
        lager:info("het_job: node_offset=~p id=~p nnodes=~p, Rest5 size=~p", [HetJobNodeOffset, HetJobId, HetJobNnodes, byte_size(Rest5)]),

        %% Skip het_job_tids arrays if HetJobNnodes > 0
        Rest6 = skip_het_job_tids(HetJobNnodes, Rest5),
        lager:info("After skip_het_job_tids, Rest6 size=~p", [byte_size(Rest6)]),

        %% Parse more het_job fields
        <<_HetJobNtasks:32/big, Rest7/binary>> = Rest6,

        %% Skip het_job_tid_offsets if HetJobNnodes > 0
        Rest8 = skip_het_job_tid_offsets(HetJobNnodes, Rest7),

        %% Continue with remaining het_job fields
        <<_HetJobOffset:32/big, _HetJobStepCnt:32/big, _HetJobTaskOffset:32/big, Rest9/binary>> = Rest8,

        %% Parse het_job_node_list string
        {_HetJobNodeList, Rest10} = unpack_string_safe(Rest9),

        %% Parse mpi_plugin_id
        <<_MpiPluginId:32/big, Rest11/binary>> = Rest10,

        %% Parse task/node counts
        <<Ntasks:32/big, _NtasksPerBoard:16/big, _NtasksPerCore:16/big,
          _NtasksPerTres:16/big, _NtasksPerSocket:16/big, Rest12/binary>> = Rest11,
        lager:info("Decoded ntasks=~p", [Ntasks]),

        %% Parse memory limits
        <<JobMemLim:64/big, StepMemLim:64/big, Rest13/binary>> = Rest12,
        lager:debug("mem_lim: job=~p step=~p", [JobMemLim, StepMemLim]),

        %% Parse nnodes
        <<Nnodes:32/big, Rest14/binary>> = Rest13,
        lager:info("Decoded nnodes=~p", [Nnodes]),

        %% Parse cpus_per_task
        <<CpusPerTask:16/big, Rest15/binary>> = Rest14,
        lager:info("cpus_per_task=~p, Rest15 first 20 bytes: ~p", [CpusPerTask, binary:part(Rest15, 0, min(20, byte_size(Rest15)))]),

        %% Parse tres_per_task string
        {TresPerTask, Rest16} = unpack_string_safe(Rest15),
        lager:info("tres_per_task=~p, Rest16 first 20 bytes: ~p", [TresPerTask, binary:part(Rest16, 0, min(20, byte_size(Rest16)))]),

        %% Parse threads_per_core, task_dist, node_cpus, job_core_spec, accel_bind_type
        <<ThreadsPerCore:16/big, TaskDist:32/big, NodeCpus:16/big,
          JobCoreSpec:16/big, AccelBindType:16/big, Rest17/binary>> = Rest16,
        lager:info("threads_per_core=~p task_dist=~p node_cpus=~p job_core_spec=~p accel_bind_type=~p",
                   [ThreadsPerCore, TaskDist, NodeCpus, JobCoreSpec, AccelBindType]),
        lager:info("Rest17 first 20 bytes: ~p", [binary:part(Rest17, 0, min(20, byte_size(Rest17)))]),

        %% The SLURM REQUEST_LAUNCH_TASKS format is complex. Instead of parsing every field,
        %% let's find the env vars and argv by pattern matching, then extract what we need.
        %%
        %% Strategy: Search for the env array, which starts with a reasonable count (20-60)
        %% followed by packstr entries starting with "HOME=" or "SLURM_"

        %% Find env vars position in the ENTIRE binary, not just Rest17
        %% The REQUEST_LAUNCH_TASKS format is complex and our field-by-field parsing
        %% may get misaligned. Search the original body for env vars.
        case find_env_vars_position(Binary) of
            {ok, EnvOffset} ->
                lager:debug("Found env at offset ~p in full binary", [EnvOffset]),
                <<_Skip:EnvOffset/binary, RestFromEnv/binary>> = Binary,

                %% Parse env array
                <<Envc:32/big, RestEnvStrings/binary>> = RestFromEnv,
                {Env, RestAfterEnv} = unpack_string_array(Envc, RestEnvStrings),
                lager:info("Decoded ~p env vars", [Envc]),

                %% Parse spank_job_env array
                <<SpankEnvCount:32/big, RestSpank/binary>> = RestAfterEnv,
                {_SpankEnv, RestAfterSpank} = unpack_string_array(SpankEnvCount, RestSpank),

                %% Parse container string
                {_Container, RestAfterContainer} = unpack_string_safe(RestAfterSpank),

                %% Parse cwd
                {Cwd, RestAfterCwd} = unpack_string_safe(RestAfterContainer),
                lager:info("Decoded cwd=~p", [Cwd]),

                %% Parse cpu_bind_type and cpu_bind
                <<CpuBindType:16/big, RestCpuBind/binary>> = RestAfterCwd,
                {CpuBind, RestAfterCpuBind} = unpack_string_safe(RestCpuBind),

                %% Parse mem_bind_type and mem_bind
                <<_MemBindType:16/big, RestMemBind/binary>> = RestAfterCpuBind,
                {_MemBind, RestAfterMemBind} = unpack_string_safe(RestMemBind),

                %% Parse argv array
                <<Argc:32/big, RestArgvStrings/binary>> = RestAfterMemBind,
                {Argv, RestAfterArgv} = unpack_string_array(Argc, RestArgvStrings),
                lager:info("Decoded argv (~p): ~p", [Argc, Argv]),

                %% Calculate offset for RestAfterArgv in the full binary
                %% This is where io_port should be located (AFTER env, AFTER argv)
                EnvEndOffset = byte_size(Binary) - byte_size(RestAfterArgv),
                lager:info("RestAfterArgv offset: ~p, first 30 bytes hex: ~s",
                          [EnvEndOffset, binary_to_hex_local(binary:part(RestAfterArgv, 0, min(30, byte_size(RestAfterArgv))))]),

                %% We've got the essential fields we need (env, cwd, argv)
                %% SLURM wire format: resp_port comes BEFORE env, io_port comes AFTER argv
                RespPort = extract_resp_port_before_env(Binary, EnvOffset),
                lager:info("Extracted resp_port (before env): ~p", [RespPort]),

                %% io_port comes AFTER env/argv section - search in RestAfterArgv
                IoPort = extract_io_port_after_argv(RestAfterArgv),
                lager:info("Extracted io_port (after argv): ~p", [IoPort]),

                %% Use defaults for the rest
                Flags = 0,
                Ofname = <<>>,
                Efname = <<>>,
                Ifname = <<>>,
                TasksToLaunch = [Ntasks],
                GlobalTaskIds = [[0]],
                CompleteNodelist = <<>>;

            not_found ->
                lager:warning("Could not find env vars in message, using defaults"),
                Env = [],
                Cwd = <<"/tmp">>,
                CpuBindType = 0,
                CpuBind = <<>>,
                Argv = [<<"/bin/hostname">>],
                Flags = 0,
                Ofname = <<>>,
                Efname = <<>>,
                Ifname = <<>>,
                IoPort = [],
                TasksToLaunch = [Ntasks],
                GlobalTaskIds = [[0]],
                RespPort = [],
                CompleteNodelist = <<>>
        end,

        %% Extract partition from env if available
        Partition = extract_partition_from_env(Env),

        %% Extract io_key (credential signature) for I/O authentication
        IoKey = extract_io_key(Binary),

        %% Build the record
        Result = #launch_tasks_request{
            job_id = JobId,
            step_id = normalize_step_id(StepId),
            step_het_comp = StepHetComp,
            uid = Uid,
            gid = Gid,
            user_name = UserName,
            gids = GIds,
            ntasks = Ntasks,
            nnodes = Nnodes,
            argc = length(Argv),
            argv = Argv,
            envc = length(Env),
            env = Env,
            cwd = Cwd,
            cpu_bind_type = CpuBindType,
            cpu_bind = CpuBind,
            task_dist = TaskDist,
            flags = Flags,
            tasks_to_launch = TasksToLaunch,
            global_task_ids = GlobalTaskIds,
            resp_port = RespPort,
            io_port = IoPort,
            ofname = Ofname,
            efname = Efname,
            ifname = Ifname,
            complete_nodelist = CompleteNodelist,
            partition = Partition,
            job_mem_lim = JobMemLim,
            step_mem_lim = StepMemLim,
            io_key = IoKey
        },
        {ok, Result}
    catch
        error:{badmatch, _} = E ->
            lager:warning("launch_tasks_request decode badmatch: ~p", [E]),
            {error, {launch_tasks_request_decode_failed, E}};
        Class:Reason:Stack ->
            lager:warning("launch_tasks_request decode error: ~p:~p~n~p", [Class, Reason, Stack]),
            {error, {launch_tasks_request_decode_failed, {Class, Reason}}}
    end.

%% Helper: unpack a string safely (returns empty binary on failure)
unpack_string_safe(<<Len:32/big, Rest/binary>>) when Len =< byte_size(Rest), Len < 16#FFFFFFFE ->
    <<Str:Len/binary, Rest2/binary>> = Rest,
    {Str, Rest2};
unpack_string_safe(<<16#FFFFFFFF:32, Rest/binary>>) ->
    %% NULL string (0xFFFFFFFF = NO_VAL)
    {<<>>, Rest};
unpack_string_safe(<<Len:32/big, _/binary>> = Full) ->
    lager:warning("unpack_string_safe: len=~p too large for buffer (~p bytes)", [Len, byte_size(Full) - 4]),
    {<<>>, <<>>};
unpack_string_safe(Binary) ->
    lager:warning("unpack_string_safe: insufficient data (~p bytes)", [byte_size(Binary)]),
    {<<>>, <<>>}.

%% Helper: unpack uint32 array
unpack_uint32_array(0, Rest) ->
    {[], Rest};
unpack_uint32_array(Count, Binary) ->
    unpack_uint32_array(Count, Binary, []).

unpack_uint32_array(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
unpack_uint32_array(N, <<Val:32/big, Rest/binary>>, Acc) ->
    unpack_uint32_array(N - 1, Rest, [Val | Acc]);
unpack_uint32_array(_, Rest, Acc) ->
    {lists:reverse(Acc), Rest}.

%% Helper: unpack string array
unpack_string_array(0, Rest) ->
    {[], Rest};
unpack_string_array(Count, Binary) ->
    unpack_string_array(Count, Binary, []).

unpack_string_array(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
unpack_string_array(N, Binary, Acc) ->
    {Str, Rest} = unpack_string_safe(Binary),
    unpack_string_array(N - 1, Rest, [Str | Acc]).

%% Helper: skip het_job_tids arrays (conditional on HetJobNnodes > 0 and not SLURM_NO_VAL)
skip_het_job_tids(0, Rest) ->
    Rest;
skip_het_job_tids(HetJobNnodes, Rest) when HetJobNnodes >= 16#FFFFFFFE ->
    %% SLURM_NO_VAL (0xFFFFFFFE) or SLURM_NO_VAL_UINT32 (0xFFFFFFFF) - skip nothing
    Rest;
skip_het_job_tids(HetJobNnodes, Binary) ->
    %% Each node has: count(32) + array of uint32
    skip_het_job_tids_loop(HetJobNnodes, Binary).

skip_het_job_tids_loop(0, Rest) ->
    Rest;
skip_het_job_tids_loop(N, <<Count:32/big, Rest/binary>>) when Count < 16#FFFFFFFE ->
    SkipBytes = Count * 4,
    <<_:SkipBytes/binary, Rest2/binary>> = Rest,
    skip_het_job_tids_loop(N - 1, Rest2);
skip_het_job_tids_loop(N, <<_Count:32/big, Rest/binary>>) ->
    %% Count is NO_VAL, skip nothing for this node
    skip_het_job_tids_loop(N - 1, Rest);
skip_het_job_tids_loop(_, Rest) ->
    Rest.

%% Helper: skip het_job_tid_offsets (conditional on HetJobNnodes > 0 and not SLURM_NO_VAL)
skip_het_job_tid_offsets(0, Rest) ->
    Rest;
skip_het_job_tid_offsets(HetJobNnodes, Rest) when HetJobNnodes >= 16#FFFFFFFE ->
    %% SLURM_NO_VAL - skip nothing
    Rest;
skip_het_job_tid_offsets(HetJobNnodes, Binary) ->
    SkipBytes = HetJobNnodes * 4,
    <<_:SkipBytes/binary, Rest/binary>> = Binary,
    Rest.

%% Helper: find env vars position in binary by scanning for packstr_array pattern
%% Look for: count (30-60 range) followed by packstr starting with "HOME=" or "SLURM_"
find_env_vars_position(Binary) ->
    %% Also try searching for "SLURM_" directly (6 bytes) in the binary
    case binary:match(Binary, <<"SLURM_">>) of
        {StrPos, _} when StrPos >= 8 ->
            %% Found SLURM_ at position StrPos. The count should be a few bytes before.
            %% Packstr format: <<Len:32, String:Len>> so string starts at Len+4.
            %% env array format: <<Count:32, Str1Len:32, Str1:Str1Len, Str2Len:32, ...>>
            %% So count is 8 bytes before the first string content
            %% But first check if this is part of env vars by checking the prefix
            PossibleCountPos = StrPos - 8,
            case PossibleCountPos >= 0 of
                true ->
                    <<_:PossibleCountPos/binary, PossibleCount:32/big, PossibleLen:32/big, _/binary>> = Binary,
                    lager:info("Found SLURM_ at ~p, possible count at ~p: ~p, strlen: ~p",
                               [StrPos, PossibleCountPos, PossibleCount, PossibleLen]),
                    if
                        PossibleCount >= 10 andalso PossibleCount =< 100 andalso
                        PossibleLen >= 10 andalso PossibleLen < 200 ->
                            {ok, PossibleCountPos};
                        true ->
                            %% Fall back to original search
                            find_env_vars_position_scan(Binary, 0)
                    end;
                false ->
                    find_env_vars_position_scan(Binary, 0)
            end;
        _ ->
            find_env_vars_position_scan(Binary, 0)
    end.

find_env_vars_position_scan(Binary, Offset) when Offset + 50 < byte_size(Binary) ->
    <<_:Offset/binary, MaybeCount:32/big, Rest/binary>> = Binary,
    %% Check if count is reasonable for env vars (typically 30-60)
    if
        MaybeCount >= 20 andalso MaybeCount =< 100 ->
            %% Check if next packstr starts with "HOME=" or "SLURM_" or "PATH="
            case Rest of
                <<StrLen:32/big, _/binary>> when StrLen >= 5, StrLen < 200 ->
                    %% Extract first few bytes of string
                    case Rest of
                        <<_:32, FirstChars:5/binary, _/binary>> ->
                            case FirstChars of
                                <<"HOME=">> ->
                                    lager:info("Found HOME= at offset ~p (count=~p)", [Offset, MaybeCount]),
                                    {ok, Offset};
                                <<"PATH=">> ->
                                    lager:info("Found PATH= at offset ~p (count=~p)", [Offset, MaybeCount]),
                                    {ok, Offset};
                                <<"SLURM">> ->
                                    lager:info("Found SLURM at offset ~p (count=~p)", [Offset, MaybeCount]),
                                    {ok, Offset};
                                <<"HOSTN">> ->
                                    %% HOSTNAME=
                                    lager:info("Found HOSTNAME at offset ~p (count=~p)", [Offset, MaybeCount]),
                                    {ok, Offset};
                                _ ->
                                    find_env_vars_position_scan(Binary, Offset + 1)
                            end;
                        _ ->
                            find_env_vars_position_scan(Binary, Offset + 1)
                    end;
                _ ->
                    find_env_vars_position_scan(Binary, Offset + 1)
            end;
        true ->
            find_env_vars_position_scan(Binary, Offset + 1)
    end;
find_env_vars_position_scan(_Binary, _Offset) ->
    not_found.

%% Helper: extract partition from environment variables
extract_partition_from_env(EnvList) when is_list(EnvList) ->
    case lists:search(fun(EnvVar) ->
        case EnvVar of
            <<"SLURM_JOB_PARTITION=", _/binary>> -> true;
            _ -> false
        end
    end, EnvList) of
        {value, <<"SLURM_JOB_PARTITION=", Partition/binary>>} -> Partition;
        _ -> <<>>
    end;
extract_partition_from_env(_) ->
    <<>>.

%% Extract resp_port from binary BEFORE env offset
%% resp_port uses 16-bit count + 16-bit values (SLURM pack16 calls)
extract_resp_port_before_env(Binary, EnvOffset) when EnvOffset > 50 ->
    %% Search from start of binary to just before env vars
    SearchEnd = min(EnvOffset + 50, byte_size(Binary)),
    SearchBin = binary:part(Binary, 0, SearchEnd),

    %% Find port arrays (using existing heuristic with 32-bit count)
    AllArrays = find_all_port_arrays(SearchBin, 0, []),
    case AllArrays of
        [First | _] -> First;
        [] -> []
    end;
extract_resp_port_before_env(_, _) ->
    [].

%% Extract io_port from binary AFTER argv section
%% io_port uses 16-bit count + 16-bit values (SLURM pack16 calls)
%% Format: num_io_port(16), io_port[](16 each), followed by alloc_tls_cert(packstr)
extract_io_port_after_argv(Binary) when byte_size(Binary) >= 4 ->
    %% io_port should be near the start of this section
    %% Search for 16-bit count format: count(16) + port(16) where port is ephemeral
    lager:info("Searching for io_port in ~p bytes after argv", [byte_size(Binary)]),
    find_io_port_16bit(Binary, 0);
extract_io_port_after_argv(_) ->
    [].

%% Find io_port using 16-bit count format (SLURM pack16 for count)
find_io_port_16bit(Binary, Offset) when Offset + 4 =< byte_size(Binary) ->
    <<_:Offset/binary, NumPorts:16/big, Port1:16/big, _/binary>> = Binary,
    if
        NumPorts >= 1 andalso NumPorts =< 4 andalso
        Port1 >= 30000 andalso Port1 < 60000 ->
            %% Looks like a valid io_port array with 16-bit count
            Ports = extract_port_array_16(Binary, Offset + 2, NumPorts),
            ValidPorts = [P || P <- Ports, P >= 30000, P < 60000],
            if
                length(ValidPorts) =:= NumPorts ->
                    lager:info("Found io_port (16-bit count) at offset ~p: count=~p, ports=~p",
                               [Offset, NumPorts, Ports]),
                    Ports;
                true ->
                    find_io_port_16bit(Binary, Offset + 1)
            end;
        true ->
            find_io_port_16bit(Binary, Offset + 1)
    end;
find_io_port_16bit(_, _) ->
    [].

%% Extract array of uint16 ports with 16-bit elements starting at Offset
extract_port_array_16(Binary, Offset, Count) ->
    extract_port_array_16(Binary, Offset, Count, []).

extract_port_array_16(_, _, 0, Acc) ->
    lists:reverse(Acc);
extract_port_array_16(Binary, Offset, Count, Acc) when Offset + 2 =< byte_size(Binary) ->
    <<_:Offset/binary, Port:16/big, _/binary>> = Binary,
    extract_port_array_16(Binary, Offset + 2, Count - 1, [Port | Acc]);
extract_port_array_16(_, _, _, Acc) ->
    lists:reverse(Acc).

%% Find all port arrays in binary (returns list of port lists)
%% SLURM pack16_array format: count:32/big + ports:16/big each
%% Note: count is 32-bit, ports are 16-bit!
find_all_port_arrays(Binary, Offset, Acc) when Offset + 6 < byte_size(Binary) ->
    <<_:Offset/binary, NumPorts:32/big, Port1:16/big, _/binary>> = Binary,
    %% Check if this looks like a port array: small count (1-4) and ephemeral port
    if
        NumPorts >= 1 andalso NumPorts =< 4 andalso
        Port1 >= 30000 andalso Port1 < 60000 ->
            %% Potential match - verify all ports are valid
            Ports = extract_port_array(Binary, Offset + 4, NumPorts),
            ValidPorts = [P || P <- Ports, P >= 30000, P < 60000],
            if
                length(ValidPorts) =:= NumPorts ->
                    lager:info("Found port16 array at offset ~p: count=~p, ports=~p",
                               [Offset, NumPorts, Ports]),
                    %% Skip past this array (4 bytes count + 2 bytes per port) and continue searching
                    NextOffset = Offset + 4 + (NumPorts * 2),
                    %% DEBUG: Show bytes right after this array to understand what follows
                    NextBytes = case byte_size(Binary) > NextOffset + 20 of
                        true -> binary:part(Binary, NextOffset, 20);
                        false -> binary:part(Binary, NextOffset, max(0, byte_size(Binary) - NextOffset))
                    end,
                    lager:info("DEBUG: Next 20 bytes after port array at offset ~p: hex=~s (dec: ~w)",
                               [NextOffset, binary_to_hex_local(NextBytes), binary_to_list(NextBytes)]),
                    find_all_port_arrays(Binary, NextOffset, [Ports | Acc]);
                true ->
                    %% Some ports invalid, keep scanning
                    find_all_port_arrays(Binary, Offset + 1, Acc)
            end;
        true ->
            find_all_port_arrays(Binary, Offset + 1, Acc)
    end;
find_all_port_arrays(_, _, Acc) ->
    lists:reverse(Acc).

%% Helper for debug hex output (local to avoid dependency)
binary_to_hex_local(Bin) ->
    lists:flatten([io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(Bin)]).

%% Extract array of uint16 ports
extract_port_array(Binary, Offset, Count) ->
    extract_port_array(Binary, Offset, Count, []).

extract_port_array(_, _, 0, Acc) ->
    lists:reverse(Acc);
extract_port_array(Binary, Offset, Count, Acc) when Offset + 2 =< byte_size(Binary) ->
    <<_:Offset/binary, Port:16/big, _/binary>> = Binary,
    extract_port_array(Binary, Offset + 2, Count - 1, [Port | Acc]);
extract_port_array(_, _, _, Acc) ->
    lists:reverse(Acc).

%% Extract io_key (credential signature) from REQUEST_LAUNCH_TASKS binary
%% The credential signature is used for I/O authentication with srun
%% Format: The signature is packed using packmem (4-byte length + data)
%%
%% The credential in REQUEST_LAUNCH_TASKS follows this order:
%% 1. cred_version: uint16 (2 bytes)
%% 2. cred: slurm_cred structure with signature at the end
%%
%% The signature is binary (not base64), packed with packmem.
%% MUNGE binary signatures are typically 100-160 bytes.
%%
%% We search backwards from the end for a packmem pattern that looks like
%% a signature: 4-byte length (80-200) followed by that many bytes of data.
-spec extract_io_key(binary()) -> binary().
extract_io_key(Binary) when byte_size(Binary) < 100 ->
    %% Binary too small to contain a credential with signature
    <<>>;
extract_io_key(Binary) ->
    %% Search for signature pattern from the end of the binary
    %% MUNGE signatures are typically 100-160 bytes, packed as packmem
    case find_signature_pattern(Binary) of
        {ok, Signature} ->
            %% Use first 32 bytes as io_key (SLURM_IO_KEY_SIZE = 32)
            case byte_size(Signature) >= 32 of
                true ->
                    <<IoKey:32/binary, _/binary>> = Signature,
                    IoKey;
                false ->
                    %% Pad short signatures to 32 bytes
                    Padding = 32 - byte_size(Signature),
                    <<Signature/binary, 0:(Padding*8)>>
            end;
        not_found ->
            %% Generate deterministic io_key from binary hash as fallback
            %% This allows I/O to work even without proper signature extraction
            Hash = crypto:hash(sha256, Binary),
            <<IoKey:32/binary, _/binary>> = Hash,
            IoKey
    end.

%% Search for a packmem pattern that looks like a MUNGE signature
%% Searches backwards from the end of the binary
-spec find_signature_pattern(binary()) -> {ok, binary()} | not_found.
find_signature_pattern(Binary) ->
    Size = byte_size(Binary),
    find_signature_pattern(Binary, Size - 4).

find_signature_pattern(_Binary, Offset) when Offset < 100 ->
    %% Searched enough of the binary, no signature found
    not_found;
find_signature_pattern(Binary, Offset) ->
    %% Look for packmem pattern: 4-byte length (80-200) followed by data
    case Binary of
        <<_:Offset/binary, Len:32/big, Rest/binary>>
          when Len >= 80, Len =< 200, byte_size(Rest) >= Len ->
            %% Found a potential signature pattern
            <<Signature:Len/binary, _/binary>> = Rest,
            %% Verify this looks like a MUNGE signature (mostly printable/binary)
            case is_likely_signature(Signature) of
                true ->
                    {ok, Signature};
                false ->
                    find_signature_pattern(Binary, Offset - 1)
            end;
        _ ->
            find_signature_pattern(Binary, Offset - 1)
    end.

%% Check if binary looks like a MUNGE signature
%% MUNGE signatures are base64-ish with some binary prefix
-spec is_likely_signature(binary()) -> boolean().
is_likely_signature(<<>>) ->
    false;
is_likely_signature(Bin) ->
    %% MUNGE signatures start with "MUNGE:" or have high entropy
    case Bin of
        <<"MUNGE:", _/binary>> ->
            true;
        _ ->
            %% Check if it looks like base64 (mostly printable ASCII)
            PrintableCount = count_printable_bytes(Bin),
            PrintableRatio = PrintableCount / byte_size(Bin),
            PrintableRatio > 0.7
    end.

%% Count printable ASCII bytes in binary
-spec count_printable_bytes(binary()) -> non_neg_integer().
count_printable_bytes(Bin) ->
    count_printable_bytes(Bin, 0).

count_printable_bytes(<<>>, Count) ->
    Count;
count_printable_bytes(<<B, Rest/binary>>, Count) when B >= 32, B =< 126 ->
    count_printable_bytes(Rest, Count + 1);
count_printable_bytes(<<_, Rest/binary>>, Count) ->
    count_printable_bytes(Rest, Count).

%% Encode RESPONSE_JOB_STEP_CREATE (5002)
%% SLURM 22.05 wire format from _pack_job_step_create_response_msg:
%%   1. def_cpu_bind_type: uint32
%%   2. resv_ports: string (NULL is ok)
%%   3. job_step_id: uint32 (step ID within the job)
%%   4. step_layout: via pack_slurm_step_layout()
%%   5. cred: via slurm_cred_pack()
%%   6. select_jobinfo: via select_g_select_jobinfo_pack()
%%   7. switch_job: via switch_g_pack_jobinfo()
%%   8. use_protocol_ver: uint16
encode_job_step_create_response(#job_step_create_response{
    job_step_id = StepId,
    job_id = JobId,
    user_id = Uid,
    group_id = Gid,
    user_name = UserName0,
    node_list = NodeList0,
    num_tasks = NumTasks0,
    error_code = _ErrorCode,
    error_msg = _ErrorMsg
} = _Resp) ->
    %% Get node_list with fallback
    NodeList = case NodeList0 of
        <<>> -> <<"flurm-node1">>;
        _ -> NodeList0
    end,
    UserName = case UserName0 of
        <<>> -> <<"root">>;
        _ -> UserName0
    end,
    NumTasks = max(1, NumTasks0),

    Parts = [
        %% 1. def_cpu_bind_type (0 = default)
        <<0:32/big>>,
        %% 2. resv_ports (NULL string - use undefined for NULL, not empty string)
        flurm_protocol_pack:pack_string(undefined),
        %% 3. job_step_id - single uint32 (NOT slurm_step_id_t, just step ID)
        <<StepId:32/big>>,
        %% 4. step_layout - minimal valid structure
        encode_step_layout(NodeList, 1, NumTasks),
        %% 5. cred - slurm_cred_pack format with job info
        encode_minimal_cred(JobId, StepId, Uid, Gid, NumTasks, UserName, NodeList),
        %% 6. select_jobinfo - plugin_id then plugin data
        encode_select_jobinfo(),
        %% 7. switch_job (NULL) - plugin_id = SWITCH_PLUGIN_NONE = 100
        <<100:32/big>>,  %% switch_g_pack_jobinfo packs SWITCH_PLUGIN_NONE for NULL
        %% 8. use_protocol_ver
        <<16#2600:16/big>>  %% SLURM 22.05 protocol version
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a minimal step_layout for SLURM 22.05
%% Format: presence(16) + front_end(str) + node_list(str) + node_cnt(32) +
%%         start_protocol_ver(16) + task_cnt(32) + task_dist(32) +
%%         for each node: pack32_array of task IDs
encode_step_layout(NodeList, NodeCnt, TaskCnt) ->
    %% Existence flag: 1 = valid step_layout, 0 = NULL
    ExistsFlag = 1,
    %% Task distribution: 1 = SLURM_DIST_BLOCK (default)
    TaskDist = 1,
    %% For each node, pack an array of task IDs using pack32_array format
    %% pack32_array: count(32) + uint32 elements
    %% For 1 task on 1 node: count=1, task_id[0]=0
    TaskArrays = lists:duplicate(NodeCnt,
        <<1:32/big,   %% array count = 1 task per node
          0:32/big>>  %% task_id = 0
    ),
    iolist_to_binary([
        <<ExistsFlag:16/big>>,  %% pack16 existence flag (1 = valid)
        flurm_protocol_pack:pack_string(undefined),  %% front_end (NULL)
        flurm_protocol_pack:pack_string(NodeList),  %% node_list
        <<NodeCnt:32/big>>,  %% node_cnt
        <<16#2600:16/big>>,  %% start_protocol_ver (22.05)
        <<TaskCnt:32/big>>,  %% task_cnt
        <<TaskDist:32/big>>,  %% task_dist
        %% For each node, pack tids array (pack32_array format)
        TaskArrays
    ]).

%% Encode a minimal valid credential for SLURM 22.05
%% slurm_cred_unpack reads credential fields directly WITHOUT a packbuf size prefix!
%% Wire format: credential_fields + packmem(signature, siglen)
%% Note: This is counter-intuitive since slurm_cred_pack uses packbuf, but
%% slurm_cred_unpack reads fields directly. Testing shows no size prefix needed.
%%
%% Encode a credential with specific job/step/user info
encode_minimal_cred(JobId, StepId, Uid, Gid, NumTasks, UserName, NodeList) ->
    %% _pack_cred format for SLURM_22_05_PROTOCOL_VERSION:
    CredBuffer = encode_cred_buffer(JobId, StepId, Uid, Gid, NumTasks, UserName, NodeList),

    BufferSize = byte_size(CredBuffer),
    lager:info("Credential buffer: size=~p user=~s nodes=~s",
               [BufferSize, UserName, NodeList]),

    %% For signature, we use an empty signature (flurmnd will skip verification)
    %% packmem format: size(32) + data
    Signature = <<>>,
    SigLen = 0,

    %% NO BufferSize prefix - slurm_cred_unpack reads fields directly
    <<CredBuffer/binary, SigLen:32/big, Signature/binary>>.

%% Encode the credential buffer (output of _pack_cred)
%% This follows SLURM 22.05 _pack_cred format exactly
encode_cred_buffer(JobId, StepId, Uid, Gid, NumTasks, UserName, NodeList) ->
    Now = erlang:system_time(second),

    iolist_to_binary([
        %% 1. step_id: job_id(32), step_id(32), step_het_comp(32)
        <<JobId:32/big, StepId:32/big, 0:32/big>>,

        %% 2. uid(32), gid(32)
        <<Uid:32/big, Gid:32/big>>,

        %% 3. pw_name, pw_gecos, pw_dir, pw_shell (all packstr - NULL uses length=0)
        flurm_protocol_pack:pack_string(UserName),
        flurm_protocol_pack:pack_string(undefined),  %% pw_gecos (NULL)
        flurm_protocol_pack:pack_string(<<"/root">>),  %% pw_dir
        flurm_protocol_pack:pack_string(<<"/bin/bash">>),  %% pw_shell

        %% 4. gid_array: pack32_array format = count(32) + elements
        <<1:32/big, Gid:32/big>>,  %% 1 group

        %% 5. gr_names: packstr_array format = count(32) + packstr elements
        <<1:32/big>>,
        flurm_protocol_pack:pack_string(<<"root">>),

        %% 6. gres_job_list (empty list = pack16 count = 0)
        <<0:16/big>>,

        %% 7. gres_step_list (empty list = pack16 count = 0)
        <<0:16/big>>,

        %% 8. job_core_spec (16-bit)
        <<16#FFFF:16/big>>,  %% NO_VAL16

        %% 9. job_account, job_alias_list, job_comment, job_constraints, job_partition, job_reservation
        flurm_protocol_pack:pack_string(undefined),  %% account (NULL)
        flurm_protocol_pack:pack_string(undefined),  %% alias_list (NULL)
        flurm_protocol_pack:pack_string(undefined),  %% comment (NULL)
        flurm_protocol_pack:pack_string(undefined),  %% constraints (NULL)
        flurm_protocol_pack:pack_string(<<"default">>),  %% partition
        flurm_protocol_pack:pack_string(undefined),  %% reservation (NULL)

        %% 10. job_restart_cnt (16-bit)
        <<0:16/big>>,

        %% 11. std_err, std_in, std_out (packstr - NULL)
        flurm_protocol_pack:pack_string(undefined),
        flurm_protocol_pack:pack_string(undefined),
        flurm_protocol_pack:pack_string(undefined),

        %% 12. step_hostlist
        flurm_protocol_pack:pack_string(NodeList),

        %% 13. x11 (16-bit)
        <<0:16/big>>,

        %% 14. ctime (pack_time format: 64-bit timestamp)
        <<Now:64/big>>,

        %% 15. tot_core_cnt (32-bit)
        <<1:32/big>>,

        %% 16. job_core_bitmap (pack_bit_str_hex: NO_VAL = NULL bitmap)
        <<16#FFFFFFFE:32/big>>,

        %% 17. step_core_bitmap (pack_bit_str_hex: NO_VAL = NULL bitmap)
        <<16#FFFFFFFE:32/big>>,

        %% 18. core_array_size (16-bit) - 0 means no core array
        <<0:16/big>>,

        %% 19. cpu_array_count (32-bit!) - 0 means no cpu array
        <<0:32/big>>,

        %% 20. job_nhosts (32), job_ntasks (32), job_hostlist (packstr)
        <<1:32/big>>,  %% nhosts
        <<NumTasks:32/big>>,  %% ntasks
        flurm_protocol_pack:pack_string(NodeList),

        %% 21. job_mem_alloc_size (32-bit) - 0 means no mem info
        <<0:32/big>>,

        %% 22. step_mem_alloc_size (32-bit)
        <<0:32/big>>,

        %% 23. selinux_context (packstr - NULL)
        flurm_protocol_pack:pack_string(undefined)
    ]).

%% Encode select_jobinfo for cons_tres plugin
encode_select_jobinfo() ->
    %% select_g_select_jobinfo_pack: plugin_id(32) + plugin-specific data
    %% For cons_tres (plugin_id=109), the pack function is empty (packs nothing)
    <<109:32/big>>.

%% Encode RESPONSE_JOB_STEP_INFO (5004)
%% Format: last_update(time), step_count(32), then step info records
encode_job_step_info_response(#job_step_info_response{
    last_update = LastUpdate,
    step_count = StepCount,
    steps = Steps
}) ->
    StepsBin = [encode_single_job_step_info(S) || S <- Steps],
    Parts = [
        flurm_protocol_pack:pack_time(LastUpdate),
        <<StepCount:32/big>>,
        StepsBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single job_step_info record
encode_single_job_step_info(#job_step_info{} = S) ->
    [
        %% job_id, step_id
        <<(S#job_step_info.job_id):32/big>>,
        <<(S#job_step_info.step_id):32/big>>,
        %% step_name
        flurm_protocol_pack:pack_string(S#job_step_info.step_name),
        %% partition
        flurm_protocol_pack:pack_string(S#job_step_info.partition),
        %% user_id, state
        <<(S#job_step_info.user_id):32/big>>,
        <<(S#job_step_info.state):32/big>>,
        %% num_tasks, num_cpus, time_limit
        <<(S#job_step_info.num_tasks):32/big>>,
        <<(S#job_step_info.num_cpus):32/big>>,
        <<(S#job_step_info.time_limit):32/big>>,
        %% start_time, run_time
        flurm_protocol_pack:pack_time(S#job_step_info.start_time),
        <<(S#job_step_info.run_time):32/big>>,
        %% nodes, node_cnt
        flurm_protocol_pack:pack_string(S#job_step_info.nodes),
        <<(S#job_step_info.node_cnt):32/big>>,
        %% tres_alloc_str, exit_code
        flurm_protocol_pack:pack_string(S#job_step_info.tres_alloc_str),
        <<(S#job_step_info.exit_code):32/big>>
    ].

%% Encode RESPONSE_LAUNCH_TASKS (6002)
%% SLURM wire format from _unpack_launch_tasks_response_msg:
%%   step_id: slurm_step_id_t via unpack_step_id_members:
%%     - sluid: 64-bit (database ID, can be 0)
%%     - job_id: 32-bit
%%     - step_id: 32-bit
%%     - step_het_comp: 32-bit
%%   return_code: 32
%%   node_name: packstr
%%   count_of_pids: 32 (first read, then overwritten by safe_unpack32_array)
%%   local_pids array: count(32) + data(32 each) via safe_unpack32_array
%%   task_ids array: count(32) + data(32 each) via safe_unpack32_array
%%
%% NOTE: safe_unpack32_array reads its own count from the buffer, so each
%% array has its own count prefix. The first count_of_pids is read but
%% immediately overwritten by the array unpacking.
encode_launch_tasks_response(#launch_tasks_response{
    job_id = JobId,
    step_id = StepId,
    step_het_comp = StepHetComp,
    return_code = ReturnCode,
    node_name = NodeName,
    count_of_pids = CountOfPids,
    local_pids = LocalPids,
    gtids = Gtids
}) ->
    PidsBin = [<<P:32/big>> || P <- LocalPids],
    GtidsBin = [<<G:32/big>> || G <- Gtids],
    Parts = [
        %% step_id: job_id(32) + step_id(32) + step_het_comp(32) - NO sluid for SLURM 22.05
        <<JobId:32/big, StepId:32/big, StepHetComp:32/big>>,
        <<ReturnCode:32/signed-big>>,
        flurm_protocol_pack:pack_string(ensure_binary(NodeName)),
        %% count_of_pids (read first, then overwritten by safe_unpack32_array)
        <<CountOfPids:32/big>>,
        %% local_pids array: count prefix (read by safe_unpack32_array)
        <<CountOfPids:32/big>>,
        PidsBin,
        %% task_ids/gtids array: count prefix (read by safe_unpack32_array)
        <<CountOfPids:32/big>>,
        GtidsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_launch_tasks_response(#{return_code := ReturnCode} = Map) ->
    %% Support map format as well as record
    JobId = maps:get(job_id, Map, 0),
    StepId = maps:get(step_id, Map, 0),
    StepHetComp = maps:get(step_het_comp, Map, 0),
    NodeName = maps:get(node_name, Map, <<>>),
    LocalPids = maps:get(local_pids, Map, []),
    Gtids = maps:get(gtids, Map, []),
    CountOfPids = length(LocalPids),
    encode_launch_tasks_response(#launch_tasks_response{
        job_id = JobId,
        step_id = StepId,
        step_het_comp = StepHetComp,
        return_code = ReturnCode,
        node_name = NodeName,
        count_of_pids = CountOfPids,
        local_pids = LocalPids,
        gtids = Gtids
    });
encode_launch_tasks_response(_) ->
    %% Default empty response
    encode_launch_tasks_response(#launch_tasks_response{}).

%% Encode MESSAGE_TASK_EXIT (6003)
%% SLURM wire format from _pack_task_exit_msg / _unpack_task_exit_msg:
%%   return_code: uint32 (single value, NOT array)
%%   num_tasks: uint32 (count of task_id_list)
%%   task_id_list: pack32_array format = count:32 + elements:32 each
%%   step_id: slurm_step_id_t via pack_step_id (SLURM 22.05 - NO sluid):
%%     - job_id: 32-bit
%%     - step_id: 32-bit
%%     - step_het_comp: 32-bit
%%
%% IMPORTANT: pack32_array packs count AGAIN before array elements!
%% The packing order is: return_code, num_tasks, array_count, task_ids[], step_id
encode_task_exit_msg(#{job_id := JobId, step_id := StepId} = Map) ->
    StepHetComp = maps:get(step_het_comp, Map, 16#FFFFFFFE),
    TaskIds = maps:get(task_ids, Map, [0]),
    ReturnCodes = maps:get(return_codes, Map, [0]),
    %% Use first return code (SLURM uses single return_code, not array)
    ReturnCode = case ReturnCodes of
        [RC | _] -> RC;
        _ -> 0
    end,
    NumTasks = length(TaskIds),
    TaskIdsBin = [<<T:32/big>> || T <- TaskIds],
    %% Correct SLURM order: return_code, num_tasks, pack32_array(task_ids), step_id
    %% pack32_array format: count:32 followed by elements
    Parts = [
        %% return_code: single uint32
        <<ReturnCode:32/signed-big>>,
        %% num_tasks: count of task_ids
        <<NumTasks:32/big>>,
        %% task_id_list as pack32_array: count + elements
        <<NumTasks:32/big>>,  %% pack32_array count (same as num_tasks)
        TaskIdsBin,           %% array elements
        %% step_id: job_id(32) + step_id(32) + step_het_comp(32) - NO sluid for SLURM 22.05
        <<JobId:32/big, StepId:32/big, StepHetComp:32/big>>
    ],
    {ok, iolist_to_binary(Parts)};
encode_task_exit_msg(_) ->
    %% Default empty response
    encode_task_exit_msg(#{job_id => 0, step_id => 0}).

%% Encode RESPONSE_REATTACH_TASKS (5013)
%% Format: return_code(32), node_name(string), count_of_pids(32),
%%         local_pids(32 each), gtids(32 each), executable_names(strings)
encode_reattach_tasks_response(#reattach_tasks_response{
    return_code = ReturnCode,
    node_name = NodeName,
    count_of_pids = CountOfPids,
    local_pids = LocalPids,
    gtids = Gtids,
    executable_names = ExecNames
}) ->
    PidsBin = [<<P:32/big>> || P <- LocalPids],
    GtidsBin = [<<G:32/big>> || G <- Gtids],
    ExecNamesBin = [flurm_protocol_pack:pack_string(ensure_binary(N)) || N <- ExecNames],
    Parts = [
        <<ReturnCode:32/signed-big>>,
        flurm_protocol_pack:pack_string(ensure_binary(NodeName)),
        <<CountOfPids:32/big>>,
        PidsBin,
        GtidsBin,
        ExecNamesBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_reattach_tasks_response(#{return_code := ReturnCode} = Map) ->
    NodeName = maps:get(node_name, Map, <<>>),
    LocalPids = maps:get(local_pids, Map, []),
    Gtids = maps:get(gtids, Map, []),
    ExecNames = maps:get(executable_names, Map, []),
    CountOfPids = length(LocalPids),
    encode_reattach_tasks_response(#reattach_tasks_response{
        return_code = ReturnCode,
        node_name = NodeName,
        count_of_pids = CountOfPids,
        local_pids = LocalPids,
        gtids = Gtids,
        executable_names = ExecNames
    });
encode_reattach_tasks_response(_) ->
    %% Default empty response
    encode_reattach_tasks_response(#reattach_tasks_response{}).

%% Helper to extract name from binary
extract_name_from_binary(Binary) ->
    case flurm_protocol_pack:unpack_string(Binary) of
        {ok, Name, _Rest} -> ensure_binary(Name);
        _ -> <<>>
    end.

%%====================================================================
%% Job Control Request/Response Encoders and Decoders
%%====================================================================

%% Decode REQUEST_UPDATE_JOB (4014) - scontrol update job
%% scontrol update sends a full job_desc_msg_t packed via _pack_job_desc_msg.
%% We decode the fields we care about using known field positions from the
%% SLURM 22.05 wire format.
%%
%% The SLURM 22.05 pack order starts with:
%%   site_factor:32, batch_features:str, cluster_features:str, clusters:str,
%%   contiguous:16, container:str, core_spec:16, task_dist:32,
%%   kill_on_node_fail:16, features:str, fed_siblings_active:64,
%%   fed_siblings_viable:64, job_id:32, job_id_str:str, name:str, ...
%%
%% Priority is at offset (binary_size - 358) from start.
%% TimeLimit is at offset (binary_size - 179) from start.
%% These offsets from end are stable because all fields after the variable-
%% length strings near the beginning have fixed sizes.
decode_update_job_request(Binary) ->
    try
        decode_update_job_request_walk(Binary)
    catch
        _:Reason ->
            lager:error("update_job decode failed: ~p", [Reason]),
            {error, {update_job_decode_failed, Reason}}
    end.

decode_update_job_request_walk(Binary) when byte_size(Binary) < 64 ->
    %% Too small to be a valid job_desc_msg_t, try string-based fallback
    case flurm_protocol_pack:unpack_string(Binary) of
        {ok, JobIdStr, _Rest} when byte_size(JobIdStr) > 0 ->
            JobId = parse_job_id(JobIdStr),
            {ok, #update_job_request{
                job_id = JobId,
                job_id_str = ensure_binary(JobIdStr)
            }};
        _ ->
            {ok, #update_job_request{}}
    end;
decode_update_job_request_walk(Binary) ->
    Size = byte_size(Binary),

    %% Walk the beginning of the packed structure to extract
    %% job_id_str and name (variable-length string fields)
    %%
    %% Field order: site_factor:32, batch_features:str,
    %%   cluster_features:str, clusters:str, contiguous:16,
    %%   container:str, core_spec:16, task_dist:32,
    %%   kill_on_node_fail:16, features:str,
    %%   fed_siblings_active:64, fed_siblings_viable:64,
    %%   job_id:32, job_id_str:str, name:str

    %% Skip: site_factor(4) = 4 bytes
    <<_SiteFactor:32/big, Rest0/binary>> = Binary,

    %% Skip: batch_features(str), cluster_features(str), clusters(str)
    {Rest1, _} = skip_packstr(Rest0),
    {Rest2, _} = skip_packstr(Rest1),
    {Rest3, _} = skip_packstr(Rest2),

    %% Skip: contiguous(2)
    <<_Contiguous:16/big, Rest4/binary>> = Rest3,

    %% Skip: container(str)
    {Rest5, _} = skip_packstr(Rest4),

    %% Skip: core_spec(2), task_dist(4), kill_on_node_fail(2)
    <<_CoreSpec:16/big, _TaskDist:32/big, _KillOnFail:16/big, Rest6/binary>> = Rest5,

    %% Skip: features(str)
    {Rest7, _} = skip_packstr(Rest6),

    %% Skip: fed_siblings_active(8), fed_siblings_viable(8)
    <<_FedActive:64/big, _FedViable:64/big, Rest8/binary>> = Rest7,

    %% Read: job_id(4)
    <<PackedJobId:32/big, Rest9/binary>> = Rest8,

    %% Read: job_id_str(str)
    {Rest10, JobIdStr} = skip_packstr(Rest9),

    %% Read: name(str)
    {_Rest11, Name} = skip_packstr(Rest10),

    %% Determine actual job_id
    JobId = case PackedJobId of
        16#FFFFFFFE ->
            %% NO_VAL - get from job_id_str
            case JobIdStr of
                <<>> -> 0;
                _ -> parse_job_id(JobIdStr)
            end;
        _ -> PackedJobId
    end,

    %% Extract priority and time_limit from fixed offsets from end
    %% These are stable because all fields after the variable-length
    %% strings at the beginning have fixed total size.
    PriorityOffset = Size - 358,
    TimeLimitOffset = Size - 179,

    Priority = case PriorityOffset >= 0 andalso PriorityOffset + 4 =< Size of
        true ->
            <<_:PriorityOffset/binary, P:32/big, _/binary>> = Binary,
            P;
        false -> ?SLURM_NO_VAL
    end,

    TimeLimit = case TimeLimitOffset >= 0 andalso TimeLimitOffset + 4 =< Size of
        true ->
            <<_:TimeLimitOffset/binary, T:32/big, _/binary>> = Binary,
            T;
        false -> ?SLURM_NO_VAL
    end,

    %% Strip null terminators from strings
    CleanName = strip_null(Name),
    CleanJobIdStr = strip_null(JobIdStr),

    lager:info("UPDATE_JOB decoded: job_id=~p, job_id_str=~s, name=~s, "
               "priority=~p, time_limit=~p",
               [JobId, CleanJobIdStr, CleanName, Priority, TimeLimit]),

    {ok, #update_job_request{
        job_id = JobId,
        job_id_str = CleanJobIdStr,
        priority = Priority,
        time_limit = TimeLimit,
        name = CleanName
    }}.


%% Decode REQUEST_JOB_WILL_RUN (4012) - sbatch --test-only
decode_job_will_run_request(Binary) ->
    try
        case Binary of
            <<JobId:32/big, Rest/binary>> when JobId > 0 ->
                Partition = find_partition_in_binary(Rest),
                MinNodes = find_nodes_in_binary(Rest),
                {ok, #job_will_run_request{
                    job_id = JobId,
                    partition = Partition,
                    min_nodes = MinNodes
                }};
            _ ->
                Partition = find_partition_in_binary(Binary),
                MinNodes = find_nodes_in_binary(Binary),
                MinCpus = find_cpus_in_binary(Binary),
                {ok, #job_will_run_request{
                    partition = Partition,
                    min_nodes = MinNodes,
                    min_cpus = MinCpus
                }}
        end
    catch
        _:Reason ->
            {error, {job_will_run_decode_failed, Reason}}
    end.

%% Encode RESPONSE_JOB_WILL_RUN (4013)
encode_job_will_run_response(#job_will_run_response{
    job_id = JobId,
    start_time = StartTime,
    node_list = NodeList,
    proc_cnt = ProcCnt,
    error_code = ErrorCode
}) ->
    Parts = [
        <<JobId:32/big>>,
        flurm_protocol_pack:pack_time(StartTime),
        flurm_protocol_pack:pack_string(ensure_binary(NodeList)),
        <<ProcCnt:32/big>>,
        <<ErrorCode:32/big>>
    ],
    {ok, iolist_to_binary(Parts)}.

find_partition_in_binary(Binary) ->
    case binary:match(Binary, <<"partition=">>) of
        {Start, Len} ->
            <<_:Start/binary, _:Len/binary, Rest/binary>> = Binary,
            extract_until_delimiter(Rest);
        nomatch ->
            <<>>
    end.

find_nodes_in_binary(_Binary) -> 1.
find_cpus_in_binary(_Binary) -> 1.

extract_until_delimiter(Binary) ->
    extract_until_delimiter(Binary, 0).

extract_until_delimiter(Binary, Pos) when Pos < byte_size(Binary) ->
    case binary:at(Binary, Pos) of
        C when C =:= $\s; C =:= $\n; C =:= $\r; C =:= 0 ->
            <<Result:Pos/binary, _/binary>> = Binary,
            Result;
        _ ->
            extract_until_delimiter(Binary, Pos + 1)
    end;
extract_until_delimiter(Binary, Pos) ->
    binary:part(Binary, 0, min(Pos, byte_size(Binary))).
%%%===================================================================
%%% Info/Query Message Decoders (Batch 3)
%%%===================================================================

%% Decode REQUEST_RESERVATION_INFO (2012)
%% Format: show_flags(32), reservation_name(string)
decode_reservation_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, Rest/binary>> ->
            {ResvName, _Rest2} = case flurm_protocol_pack:unpack_string(Rest) of
                {ok, N, R} -> {ensure_binary(N), R};
                _ -> {<<>>, Rest}
            end,
            {ok, #reservation_info_request{
                show_flags = ShowFlags,
                reservation_name = ResvName
            }};
        <<>> ->
            {ok, #reservation_info_request{}};
        _ ->
            {ok, #reservation_info_request{}}
    end.

%% Decode REQUEST_LICENSE_INFO (1017)
%% Format: show_flags(32)
decode_license_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, _Rest/binary>> ->
            {ok, #license_info_request{show_flags = ShowFlags}};
        <<>> ->
            {ok, #license_info_request{}};
        _ ->
            {ok, #license_info_request{}}
    end.

%% Decode REQUEST_TOPO_INFO (2018)
%% Format: typically empty or minimal
decode_topo_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, _Rest/binary>> ->
            {ok, #topo_info_request{show_flags = ShowFlags}};
        <<>> ->
            {ok, #topo_info_request{}};
        _ ->
            {ok, #topo_info_request{}}
    end.

%% Decode REQUEST_FRONT_END_INFO (2028)
%% Format: show_flags(32)
decode_front_end_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, _Rest/binary>> ->
            {ok, #front_end_info_request{show_flags = ShowFlags}};
        <<>> ->
            {ok, #front_end_info_request{}};
        _ ->
            {ok, #front_end_info_request{}}
    end.

%% Decode REQUEST_BURST_BUFFER_INFO (2020)
%% Format: show_flags(32)
decode_burst_buffer_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, _Rest/binary>> ->
            {ok, #burst_buffer_info_request{show_flags = ShowFlags}};
        <<>> ->
            {ok, #burst_buffer_info_request{}};
        _ ->
            {ok, #burst_buffer_info_request{}}
    end.

%%%===================================================================
%%% Info/Query Message Encoders (Batch 3)
%%%===================================================================

%% Encode RESPONSE_RESERVATION_INFO (2013)
%% From SLURM source slurm_protocol_pack.c _pack_reserve_info_msg:
%% 1. record_count: 32-bit
%% 2. last_update: time_t (64-bit)
encode_reservation_info_response(#reservation_info_response{
    last_update = LastUpdate,
    reservation_count = ResvCount,
    reservations = Reservations
}) ->
    ResvsBin = [encode_single_reservation_info(R) || R <- Reservations],
    Parts = [
        <<ResvCount:32/big, LastUpdate:64/big>>,
        ResvsBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single reservation_info record - SLURM 22.05 format
%% Fields based on SLURM's reserve_info_t and _pack_reserve_info_members
encode_single_reservation_info(#reservation_info{} = R) ->
    [
        %% accounts, burst_buffer, core_cnt, core_spec_cnt, end_time
        flurm_protocol_pack:pack_string(R#reservation_info.accounts),
        flurm_protocol_pack:pack_string(R#reservation_info.burst_buffer),
        flurm_protocol_pack:pack_uint32(R#reservation_info.core_cnt),
        flurm_protocol_pack:pack_uint32(R#reservation_info.core_spec_cnt),
        flurm_protocol_pack:pack_time(R#reservation_info.end_time),
        %% features, flags
        flurm_protocol_pack:pack_string(R#reservation_info.features),
        flurm_protocol_pack:pack_uint64(R#reservation_info.flags),
        %% groups, licenses, max_start_delay
        flurm_protocol_pack:pack_string(R#reservation_info.groups),
        flurm_protocol_pack:pack_string(R#reservation_info.licenses),
        flurm_protocol_pack:pack_uint32(R#reservation_info.max_start_delay),
        %% name, node_cnt, node_list
        flurm_protocol_pack:pack_string(R#reservation_info.name),
        flurm_protocol_pack:pack_uint32(R#reservation_info.node_cnt),
        flurm_protocol_pack:pack_string(R#reservation_info.node_list),
        %% node_inx - pack NO_VAL for empty bitstring
        <<?SLURM_NO_VAL:32/big>>,
        %% partition, purge_comp_time, resv_watts, start_time
        flurm_protocol_pack:pack_string(R#reservation_info.partition),
        flurm_protocol_pack:pack_uint32(R#reservation_info.purge_comp_time),
        flurm_protocol_pack:pack_uint32(R#reservation_info.resv_watts),
        flurm_protocol_pack:pack_time(R#reservation_info.start_time),
        %% tres_str, users
        flurm_protocol_pack:pack_string(R#reservation_info.tres_str),
        flurm_protocol_pack:pack_string(R#reservation_info.users)
    ].

%% Encode RESPONSE_LICENSE_INFO (1018)
%% From SLURM source slurm_protocol_pack.c _pack_license_info_msg:
%% 1. last_update: time_t (64-bit)
%% 2. num_lic: 32-bit
encode_license_info_response(#license_info_response{
    last_update = LastUpdate,
    license_count = LicCount,
    licenses = Licenses
}) ->
    LicsBin = [encode_single_license_info(L) || L <- Licenses],
    Parts = [
        flurm_protocol_pack:pack_time(LastUpdate),
        <<LicCount:32/big>>,
        LicsBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single license_info record
%% Fields based on SLURM's slurm_license_info_t
encode_single_license_info(#license_info{} = L) ->
    [
        flurm_protocol_pack:pack_string(L#license_info.name),
        flurm_protocol_pack:pack_uint32(L#license_info.total),
        flurm_protocol_pack:pack_uint32(L#license_info.in_use),
        flurm_protocol_pack:pack_uint32(L#license_info.available),
        flurm_protocol_pack:pack_uint32(L#license_info.reserved),
        flurm_protocol_pack:pack_uint8(L#license_info.remote)
    ].

%% Encode RESPONSE_TOPO_INFO (2019)
%% From SLURM source slurm_protocol_pack.c _pack_topo_info_msg:
%% 1. record_count: 32-bit (no timestamp for topo)
encode_topo_info_response(#topo_info_response{
    topo_count = TopoCount,
    topos = Topos
}) ->
    ToposBin = [encode_single_topo_info(T) || T <- Topos],
    Parts = [
        <<TopoCount:32/big>>,
        ToposBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single topo_info record
%% Fields based on SLURM's topo_info_t
encode_single_topo_info(#topo_info{} = T) ->
    [
        flurm_protocol_pack:pack_uint16(T#topo_info.level),
        flurm_protocol_pack:pack_uint32(T#topo_info.link_speed),
        flurm_protocol_pack:pack_string(T#topo_info.name),
        flurm_protocol_pack:pack_string(T#topo_info.nodes),
        flurm_protocol_pack:pack_string(T#topo_info.switches)
    ].

%% Encode RESPONSE_FRONT_END_INFO (2029)
%% From SLURM source slurm_protocol_pack.c _pack_front_end_info_msg:
%% 1. record_count: 32-bit
%% 2. last_update: time_t (64-bit)
encode_front_end_info_response(#front_end_info_response{
    last_update = LastUpdate,
    front_end_count = FECount,
    front_ends = FrontEnds
}) ->
    FEsBin = [encode_single_front_end_info(FE) || FE <- FrontEnds],
    Parts = [
        <<FECount:32/big, LastUpdate:64/big>>,
        FEsBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single front_end_info record
%% Fields based on SLURM's front_end_info_t
encode_single_front_end_info(#front_end_info{} = FE) ->
    [
        flurm_protocol_pack:pack_string(FE#front_end_info.allow_groups),
        flurm_protocol_pack:pack_string(FE#front_end_info.allow_users),
        flurm_protocol_pack:pack_time(FE#front_end_info.boot_time),
        flurm_protocol_pack:pack_string(FE#front_end_info.deny_groups),
        flurm_protocol_pack:pack_string(FE#front_end_info.deny_users),
        flurm_protocol_pack:pack_string(FE#front_end_info.name),
        flurm_protocol_pack:pack_uint32(FE#front_end_info.node_state),
        flurm_protocol_pack:pack_string(FE#front_end_info.reason),
        flurm_protocol_pack:pack_time(FE#front_end_info.reason_time),
        flurm_protocol_pack:pack_uint32(FE#front_end_info.reason_uid),
        flurm_protocol_pack:pack_time(FE#front_end_info.slurmd_start_time),
        flurm_protocol_pack:pack_string(FE#front_end_info.version)
    ].

%% Encode RESPONSE_BURST_BUFFER_INFO (2021)
%% From SLURM source slurm_protocol_pack.c _pack_burst_buffer_info_msg:
%% 1. record_count: 32-bit
%% 2. last_update: time_t (64-bit)
encode_burst_buffer_info_response(#burst_buffer_info_response{
    last_update = LastUpdate,
    burst_buffer_count = BBCount,
    burst_buffers = BurstBuffers
}) ->
    BBsBin = [encode_single_burst_buffer_info(BB) || BB <- BurstBuffers],
    Parts = [
        <<BBCount:32/big, LastUpdate:64/big>>,
        BBsBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single burst_buffer_info record
%% Fields based on SLURM's burst_buffer_info_t
encode_single_burst_buffer_info(#burst_buffer_info{} = BB) ->
    PoolsBin = [encode_single_bb_pool(P) || P <- BB#burst_buffer_info.pools],
    [
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.name),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.allow_users),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.create_buffer),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.default_pool),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.deny_users),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.destroy_buffer),
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.flags),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.get_sys_state),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.get_sys_status),
        flurm_protocol_pack:pack_uint64(BB#burst_buffer_info.granularity),
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.pool_cnt),
        PoolsBin,
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.other_timeout),
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.stage_in_timeout),
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.stage_out_timeout),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.start_stage_in),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.start_stage_out),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.stop_stage_in),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.stop_stage_out),
        flurm_protocol_pack:pack_uint64(BB#burst_buffer_info.total_space),
        flurm_protocol_pack:pack_uint64(BB#burst_buffer_info.unfree_space),
        flurm_protocol_pack:pack_uint64(BB#burst_buffer_info.used_space),
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.validate_timeout)
    ].

%% Encode a single burst_buffer_pool record
encode_single_bb_pool(#burst_buffer_pool{} = P) ->
    [
        flurm_protocol_pack:pack_uint64(P#burst_buffer_pool.granularity),
        flurm_protocol_pack:pack_string(P#burst_buffer_pool.name),
        flurm_protocol_pack:pack_uint64(P#burst_buffer_pool.total_space),
        flurm_protocol_pack:pack_uint64(P#burst_buffer_pool.unfree_space),
        flurm_protocol_pack:pack_uint64(P#burst_buffer_pool.used_space)
    ].

%% Encode RESPONSE_BUILD_INFO (2002) - scontrol show config
%% Complete SLURM 22.05 _pack_slurm_ctl_conf_msg format.
%% Fields must be in exact order matching the SLURM client's unpack function.
encode_build_info_response(#build_info_response{} = R) ->
    S = fun flurm_protocol_pack:pack_string/1,
    U16 = fun flurm_protocol_pack:pack_uint16/1,
    U32 = fun flurm_protocol_pack:pack_uint32/1,
    U64 = fun flurm_protocol_pack:pack_uint64/1,
    T = fun flurm_protocol_pack:pack_time/1,
    SA = fun flurm_protocol_pack:pack_string_array/1,
    %% Empty key_pair_list / plugin_params_list / slurm_pack_list = count 0
    EmptyList = <<0:32/big>>,
    ControlMachine = R#build_info_response.control_machine,
    Parts = [
        T(erlang:system_time(second)),                  % last_update
        U16(0),                                          % accounting_storage_enforce
        S(<<>>),                                         % accounting_storage_backup_host
        S(<<>>),                                         % accounting_storage_host
        S(<<>>),                                         % accounting_storage_ext_host
        S(<<>>),                                         % accounting_storage_params
        U16(0),                                          % accounting_storage_port
        S(<<>>),                                         % accounting_storage_tres
        S(R#build_info_response.accounting_storage_type),% accounting_storage_type
        S(<<>>),                                         % accounting_storage_user
        EmptyList,                                       % acct_gather_conf (key_pair_list)
        S(<<>>),                                         % acct_gather_energy_type
        S(<<>>),                                         % acct_gather_filesystem_type
        S(<<>>),                                         % acct_gather_interconnect_type
        U16(0),                                          % acct_gather_node_freq
        S(<<>>),                                         % acct_gather_profile_type
        S(<<>>),                                         % authalttypes
        S(<<>>),                                         % authalt_params
        S(<<>>),                                         % authinfo
        S(R#build_info_response.auth_type),              % authtype
        U16(120),                                        % batch_start_timeout
        T(erlang:system_time(second)),                   % boot_time
        S(<<>>),                                         % bb_type
        S(<<>>),                                         % bcast_exclude
        S(<<>>),                                         % bcast_parameters
        EmptyList,                                       % cgroup_conf (key_pair_list)
        S(<<>>),                                         % cli_filter_plugins
        S(R#build_info_response.cluster_name),           % cluster_name
        S(<<>>),                                         % comm_params
        U16(0),                                          % complete_wait
        U32(0),                                          % conf_flags
        SA([ControlMachine]),                            % control_addr (string array)
        SA([ControlMachine]),                            % control_machine (string array)
        S(<<>>),                                         % core_spec_plugin
        U32(0),                                          % cpu_freq_def
        U32(0),                                          % cpu_freq_govs
        S(<<"cred/none">>),                              % cred_type
        U64(0),                                          % def_mem_per_cpu
        U64(0),                                          % debug_flags
        S(<<>>),                                         % dependency_params
        U16(0),                                          % eio_timeout
        U16(0),                                          % enforce_part_limits
        S(<<>>),                                         % epilog
        U32(0),                                          % epilog_msg_time
        S(<<>>),                                         % epilog_slurmctld
        EmptyList,                                       % ext_sensors_conf (key_pair_list)
        S(<<>>),                                         % ext_sensors_type
        U16(0),                                          % ext_sensors_freq
        S(<<>>),                                         % fed_params
        U32(1),                                          % first_job_id
        U16(1),                                          % fs_dampening_factor
        U16(2),                                          % get_env_timeout
        S(<<>>),                                         % gres_plugins
        U16(600),                                        % group_time
        U16(1),                                          % group_force
        S(<<>>),                                         % gpu_freq_def
        U32(0),                                          % hash_val
        U16(0),                                          % health_check_interval
        U16(0),                                          % health_check_node_state
        S(<<>>),                                         % health_check_program
        U16(0),                                          % inactive_limit
        S(<<>>),                                         % interactive_step_opts
        S(<<>>),                                         % job_acct_gather_freq
        S(<<"jobacct_gather/none">>),                    % job_acct_gather_type
        S(<<>>),                                         % job_acct_gather_params
        S(<<>>),                                         % job_comp_host
        S(<<>>),                                         % job_comp_loc
        S(<<>>),                                         % job_comp_params
        U32(0),                                          % job_comp_port
        S(R#build_info_response.job_comp_type),          % job_comp_type
        S(<<>>),                                         % job_comp_user
        S(<<>>),                                         % job_container_plugin
        S(<<>>),                                         % job_credential_private_key
        S(<<>>),                                         % job_credential_public_certificate
        EmptyList,                                       % job_defaults_list (slurm_pack_list)
        U16(0),                                          % job_file_append
        U16(1),                                          % job_requeue
        S(<<>>),                                         % job_submit_plugins
        U16(0),                                          % kill_on_bad_exit
        U16(60),                                         % kill_wait
        S(<<>>),                                         % launch_params
        S(<<"launch/slurm">>),                           % launch_type
        S(<<>>),                                         % licenses
        U16(0),                                          % log_fmt
        U32(1001),                                       % max_array_sz
        U32(0),                                          % max_dbd_msgs
        S(<<>>),                                         % mail_domain
        S(<<"/bin/mail">>),                              % mail_prog
        U32(10000),                                      % max_job_cnt
        U32(16#03ffffff),                                % max_job_id
        U64(0),                                          % max_mem_per_cpu
        U32(65536),                                      % max_node_cnt
        U32(40000),                                      % max_step_cnt
        U16(512),                                        % max_tasks_per_node
        S(<<>>),                                         % mcs_plugin
        S(<<>>),                                         % mcs_plugin_params
        U32(300),                                        % min_job_age
        EmptyList,                                       % mpi_conf (key_pair_list)
        S(<<"mpi/none">>),                               % mpi_default
        S(<<>>),                                         % mpi_params
        U16(10),                                         % msg_timeout
        U32(1),                                          % next_job_id
        EmptyList,                                       % node_features_conf (config_plugin_params_list)
        S(<<>>),                                         % node_features_plugins
        S(<<>>),                                         % node_prefix
        U16(0),                                          % over_time_limit
        S(R#build_info_response.plugin_dir),             % plugindir
        S(<<>>),                                         % plugstack
        S(<<>>),                                         % power_parameters
        S(<<>>),                                         % power_plugin
        U16(0),                                          % preempt_mode
        S(<<"preempt/none">>),                           % preempt_type
        U32(0),                                          % preempt_exempt_time
        S(<<>>),                                         % prep_params
        S(<<>>),                                         % prep_plugins
        U32(0),                                          % priority_decay_hl
        U32(300),                                        % priority_calc_period
        U16(0),                                          % priority_favor_small
        U16(0),                                          % priority_flags
        U32(0),                                          % priority_max_age
        S(<<>>),                                         % priority_params
        U16(0),                                          % priority_reset_period
        S(R#build_info_response.priority_type),          % priority_type
        U32(0),                                          % priority_weight_age
        U32(0),                                          % priority_weight_assoc
        U32(0),                                          % priority_weight_fs
        U32(0),                                          % priority_weight_js
        U32(0),                                          % priority_weight_part
        U32(0),                                          % priority_weight_qos
        S(<<>>),                                         % priority_weight_tres
        U16(0),                                          % private_data
        S(<<"proctrack/linuxproc">>),                    % proctrack_type
        S(<<>>),                                         % prolog
        U16(0),                                          % prolog_epilog_timeout
        S(<<>>),                                         % prolog_slurmctld
        U16(0),                                          % prolog_flags
        U16(0),                                          % propagate_prio_process
        S(<<>>),                                         % propagate_rlimits
        S(<<>>),                                         % propagate_rlimits_except
        S(<<>>),                                         % reboot_program
        U16(0),                                          % reconfig_flags
        S(<<>>),                                         % requeue_exit
        S(<<>>),                                         % requeue_exit_hold
        S(<<>>),                                         % resume_fail_program
        S(<<>>),                                         % resume_program
        U16(300),                                        % resume_rate
        U16(60),                                         % resume_timeout
        S(<<>>),                                         % resv_epilog
        U16(0),                                          % resv_over_run
        S(<<>>),                                         % resv_prolog
        U16(0),                                          % ret2service
        S(<<>>),                                         % route_plugin
        S(<<>>),                                         % sched_params
        S(<<>>),                                         % sched_logfile
        U16(0),                                          % sched_log_level
        U16(30),                                         % sched_time_slice
        S(R#build_info_response.scheduler_type),         % schedtype
        S(<<>>),                                         % scron_params
        S(R#build_info_response.select_type),            % select_type
        EmptyList,                                       % select_conf_key_pairs (key_pair_list)
        U16(0),                                          % select_type_param
        S(<<"/etc/slurm/slurm.conf">>),                 % slurm_conf
        U32(0),                                          % slurm_user_id
        S(R#build_info_response.slurm_user_name),        % slurm_user_name
        U32(0),                                          % slurmd_user_id
        S(R#build_info_response.slurmd_user_name),       % slurmd_user_name
        S(<<>>),                                         % slurmctld_addr
        U16(7),                                          % slurmctld_debug
        S(<<"/var/log/slurmctld.log">>),                 % slurmctld_logfile
        S(<<>>),                                         % slurmctld_params
        S(<<"/var/run/slurmctld.pid">>),                 % slurmctld_pidfile
        S(<<>>),                                         % slurmctld_plugstack
        EmptyList,                                       % slurmctld_plugstack_conf (config_plugin_params_list)
        U32(R#build_info_response.slurmctld_port),       % slurmctld_port
        U16(1),                                          % slurmctld_port_count
        S(<<>>),                                         % slurmctld_primary_off_prog
        S(<<>>),                                         % slurmctld_primary_on_prog
        U16(0),                                          % slurmctld_syslog_debug
        U16(120),                                        % slurmctld_timeout
        U16(7),                                          % slurmd_debug
        S(<<"/var/log/slurmd.log">>),                    % slurmd_logfile
        S(<<>>),                                         % slurmd_params
        S(<<"/var/run/slurmd.pid">>),                    % slurmd_pidfile
        U32(R#build_info_response.slurmd_port),          % slurmd_port
        S(R#build_info_response.spool_dir),              % slurmd_spooldir
        U16(0),                                          % slurmd_syslog_debug
        U16(300),                                        % slurmd_timeout
        S(<<>>),                                         % srun_epilog
        U16(0),                                          % srun_port_range[0]
        U16(0),                                          % srun_port_range[1]
        S(<<>>),                                         % srun_prolog
        S(R#build_info_response.state_save_location),    % state_save_location
        S(<<>>),                                         % suspend_exc_nodes
        S(<<>>),                                         % suspend_exc_parts
        S(<<>>),                                         % suspend_program
        U16(0),                                          % suspend_rate
        U32(0),                                          % suspend_time
        U16(0),                                          % suspend_timeout
        S(<<>>),                                         % switch_param
        S(<<"switch/none">>),                            % switch_type
        S(<<>>),                                         % task_epilog
        S(<<>>),                                         % task_prolog
        S(<<"task/none">>),                              % task_plugin
        U32(0),                                          % task_plugin_param
        U16(2),                                          % tcp_timeout
        S(<<"/tmp">>),                                   % tmp_fs
        S(<<>>),                                         % topology_param
        S(<<"topology/none">>),                          % topology_plugin
        U16(50),                                         % tree_width
        S(<<>>),                                         % unkillable_program
        U16(60),                                         % unkillable_timeout
        S(R#build_info_response.version),                % version
        U16(0),                                          % vsize_factor
        U16(0),                                          % wait_time
        S(<<>>)                                          % x11_params
    ],
    {ok, iolist_to_binary(Parts)};
encode_build_info_response(_Response) ->
    %% Fallback for non-record responses
    {ok, <<>>}.

%% Encode RESPONSE_CONFIG_INFO (2017) - configuration dump
%% Returns configuration as SLURM config info format
encode_config_info_response(#config_info_response{last_update = LastUpdate, config = Config}) ->
    %% Config is returned as a list of key=value strings
    ConfigList = maps:fold(fun(K, V, Acc) ->
        KeyBin = if
            is_atom(K) -> atom_to_binary(K, utf8);
            is_binary(K) -> K;
            is_list(K) -> list_to_binary(K);
            true -> iolist_to_binary(io_lib:format("~p", [K]))
        end,
        ValBin = if
            is_binary(V) -> V;
            is_list(V) -> list_to_binary(V);
            is_integer(V) -> integer_to_binary(V);
            is_atom(V) -> atom_to_binary(V, utf8);
            true -> iolist_to_binary(io_lib:format("~p", [V]))
        end,
        [<<KeyBin/binary, "=", ValBin/binary>> | Acc]
    end, [], Config),

    %% Pack as: last_update, count, then each config string
    ConfigCount = length(ConfigList),
    ConfigBins = [flurm_protocol_pack:pack_string(S) || S <- ConfigList],
    Parts = [
        flurm_protocol_pack:pack_time(LastUpdate),
        <<ConfigCount:32/big>>,
        ConfigBins
    ],
    {ok, iolist_to_binary(Parts)};
encode_config_info_response(_Response) ->
    {ok, <<>>}.

%% Encode RESPONSE_STATS_INFO (2027) - sdiag statistics
%% Returns scheduler statistics in SLURM format
encode_stats_info_response(#stats_info_response{} = S) ->
    %% SLURM stats_info_response_msg format (simplified)
    Parts = [
        flurm_protocol_pack:pack_uint32(S#stats_info_response.parts_packed),
        flurm_protocol_pack:pack_time(S#stats_info_response.req_time),
        flurm_protocol_pack:pack_time(S#stats_info_response.req_time_start),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.server_thread_count),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.agent_queue_size),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.agent_count),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.agent_thread_count),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.dbd_agent_queue_size),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_submitted),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_started),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_completed),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_canceled),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_failed),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_pending),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_running),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.schedule_cycle_max),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.schedule_cycle_last),
        flurm_protocol_pack:pack_uint64(S#stats_info_response.schedule_cycle_sum),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.schedule_cycle_counter),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.schedule_cycle_depth),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.schedule_queue_len),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_backfilled_jobs),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_last_backfilled_jobs),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_cycle_counter),
        flurm_protocol_pack:pack_uint64(S#stats_info_response.bf_cycle_sum),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_cycle_last),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_cycle_max),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_depth_sum),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_depth_try_sum),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_queue_len),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_queue_len_sum),
        flurm_protocol_pack:pack_time(S#stats_info_response.bf_when_last_cycle),
        flurm_protocol_pack:pack_uint8(case S#stats_info_response.bf_active of true -> 1; false -> 0 end),
        %% RPC type stats (empty for now)
        <<0:32/big>>,  % rpc_type_cnt
        %% RPC user stats (empty for now)
        <<0:32/big>>   % rpc_user_cnt
    ],
    {ok, iolist_to_binary(Parts)};
encode_stats_info_response(_Response) ->
    {ok, <<>>}.

%% NOTE: decode_update_job_request, decode_job_will_run_request, and
%% encode_job_will_run_response are defined earlier in this file with full
%% implementations.

%% Decode REQUEST_SHUTDOWN (1004) - scontrol shutdown
decode_shutdown_request(Binary) ->
    {ok, Binary}.

%% Decode REQUEST_STATS_INFO (1009) - sdiag
decode_stats_info_request(Binary) ->
    {ok, Binary}.

%% Decode REQUEST_RECONFIGURE (1003) - scontrol reconfigure
%% This is typically an empty request (no body) for full reconfiguration
decode_reconfigure_request(<<>>) ->
    {ok, #reconfigure_request{}};
decode_reconfigure_request(<<Flags:32/big, _Rest/binary>>) ->
    {ok, #reconfigure_request{flags = Flags}};
decode_reconfigure_request(_Binary) ->
    {ok, #reconfigure_request{}}.

%% Decode REQUEST_RECONFIGURE_WITH_CONFIG (1004) - scontrol reconfigure with specific settings
%% Wire format: ConfigFileLen:32, ConfigFile/binary, SettingsCount:32, Settings...
decode_reconfigure_with_config_request(<<>>) ->
    {ok, #reconfigure_with_config_request{}};
decode_reconfigure_with_config_request(Binary) ->
    try
        %% Parse config file path
        {ConfigFile, Rest1} = flurm_protocol_pack:unpack_string(Binary),

        %% Parse flags (force, notify_nodes)
        <<Flags:32/big, Rest2/binary>> = Rest1,
        Force = (Flags band 1) =/= 0,
        NotifyNodes = (Flags band 2) =/= 0,

        %% Parse settings count and key-value pairs
        <<SettingsCount:32/big, Rest3/binary>> = Rest2,
        {Settings, _Remaining} = decode_settings(SettingsCount, Rest3, #{}),

        {ok, #reconfigure_with_config_request{
            config_file = ConfigFile,
            settings = Settings,
            force = Force,
            notify_nodes = NotifyNodes
        }}
    catch
        _:_ ->
            %% If parsing fails, return empty request (will trigger full reconfigure)
            {ok, #reconfigure_with_config_request{}}
    end.

%% Decode key-value settings from binary
decode_settings(0, Binary, Acc) ->
    {Acc, Binary};
decode_settings(Count, Binary, Acc) when Count > 0 ->
    try
        {Key, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {Value, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        KeyAtom = try binary_to_existing_atom(Key, utf8)
                  catch _:_ -> binary_to_atom(Key, utf8)
                  end,
        decode_settings(Count - 1, Rest2, Acc#{KeyAtom => Value})
    catch
        _:_ ->
            {Acc, <<>>}
    end.

%% Encode RESPONSE for reconfigure (using reconfigure_response record)
encode_reconfigure_response(#reconfigure_response{} = R) ->
    %% Encode changed keys as comma-separated string
    ChangedKeysStr = iolist_to_binary(lists:join(<<",">>,
        [atom_to_binary(K, utf8) || K <- R#reconfigure_response.changed_keys])),
    Parts = [
        flurm_protocol_pack:pack_int32(R#reconfigure_response.return_code),
        flurm_protocol_pack:pack_string(R#reconfigure_response.message),
        flurm_protocol_pack:pack_string(ChangedKeysStr),
        flurm_protocol_pack:pack_uint32(R#reconfigure_response.version)
    ],
    {ok, iolist_to_binary(Parts)};
encode_reconfigure_response(_) ->
    %% Fallback - success with no details
    {ok, <<0:32/big-signed, 0:32, 0:32, 0:32>>}.

%% Convert binary to hex string for debugging
binary_to_hex(Bin) when is_binary(Bin) ->
    list_to_binary([[io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(Bin)]]).

%%%===================================================================
%%% Federation Message Encoding/Decoding
%%%===================================================================

%% Decode REQUEST_FED_INFO (2049)
decode_fed_info_request(<<>>) ->
    {ok, #fed_info_request{}};
decode_fed_info_request(<<ShowFlags:32/big, _Rest/binary>>) ->
    {ok, #fed_info_request{show_flags = ShowFlags}};
decode_fed_info_request(_) ->
    {ok, #fed_info_request{}}.

%% Encode REQUEST_FED_INFO (2049)
encode_fed_info_request(#fed_info_request{show_flags = ShowFlags}) ->
    {ok, <<ShowFlags:32/big>>};
encode_fed_info_request(_) ->
    {ok, <<0:32/big>>}.

%% Decode RESPONSE_FED_INFO (2050)
decode_fed_info_response(Binary) ->
    try
        {FedName, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {LocalCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<ClusterCount:32/big, Rest3/binary>> = Rest2,
        {Clusters, _} = decode_fed_clusters(ClusterCount, Rest3, []),
        {ok, #fed_info_response{
            federation_name = FedName,
            local_cluster = LocalCluster,
            cluster_count = ClusterCount,
            clusters = Clusters
        }}
    catch
        _:_ ->
            {ok, #fed_info_response{}}
    end.

%% Encode RESPONSE_FED_INFO (2050)
encode_fed_info_response(#fed_info_response{} = R) ->
    ClustersBin = encode_fed_clusters(R#fed_info_response.clusters),
    Parts = [
        flurm_protocol_pack:pack_string(R#fed_info_response.federation_name),
        flurm_protocol_pack:pack_string(R#fed_info_response.local_cluster),
        <<(R#fed_info_response.cluster_count):32/big>>,
        ClustersBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_info_response(_) ->
    {ok, <<0:32, 0:32, 0:32>>}.

%% Decode federation cluster list
decode_fed_clusters(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_fed_clusters(Count, Binary, Acc) ->
    try
        {Name, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {Host, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<Port:32/big, Rest3/binary>> = Rest2,
        {State, Rest4} = flurm_protocol_pack:unpack_string(Rest3),
        <<Weight:32/big, FeatureCount:32/big, Rest5/binary>> = Rest4,
        {Features, Rest6} = decode_string_list(FeatureCount, Rest5, []),
        <<PartitionCount:32/big, Rest7/binary>> = Rest6,
        {Partitions, Rest8} = decode_string_list(PartitionCount, Rest7, []),
        Cluster = #fed_cluster_info{
            name = Name,
            host = Host,
            port = Port,
            state = State,
            weight = Weight,
            features = Features,
            partitions = Partitions
        },
        decode_fed_clusters(Count - 1, Rest8, [Cluster | Acc])
    catch
        _:_ ->
            {lists:reverse(Acc), <<>>}
    end.

%% Encode federation cluster list
encode_fed_clusters(Clusters) ->
    lists:map(fun(#fed_cluster_info{} = C) ->
        FeaturesBin = encode_string_list(C#fed_cluster_info.features),
        PartitionsBin = encode_string_list(C#fed_cluster_info.partitions),
        [
            flurm_protocol_pack:pack_string(C#fed_cluster_info.name),
            flurm_protocol_pack:pack_string(C#fed_cluster_info.host),
            <<(C#fed_cluster_info.port):32/big>>,
            flurm_protocol_pack:pack_string(C#fed_cluster_info.state),
            <<(C#fed_cluster_info.weight):32/big>>,
            <<(length(C#fed_cluster_info.features)):32/big>>,
            FeaturesBin,
            <<(length(C#fed_cluster_info.partitions)):32/big>>,
            PartitionsBin
        ]
    end, Clusters).

%% Decode REQUEST_FEDERATION_SUBMIT (2032)
decode_federation_submit_request(Binary) ->
    try
        {SourceCluster, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {TargetCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<JobId:32/big, Rest3/binary>> = Rest2,
        {Name, Rest4} = flurm_protocol_pack:unpack_string(Rest3),
        {Script, Rest5} = flurm_protocol_pack:unpack_string(Rest4),
        {Partition, Rest6} = flurm_protocol_pack:unpack_string(Rest5),
        <<NumCpus:32/big, NumNodes:32/big, MemoryMb:32/big,
          TimeLimit:32/big, UserId:32/big, GroupId:32/big,
          Priority:32/big, Rest7/binary>> = Rest6,
        {WorkDir, Rest8} = flurm_protocol_pack:unpack_string(Rest7),
        {StdOut, Rest9} = flurm_protocol_pack:unpack_string(Rest8),
        {StdErr, Rest10} = flurm_protocol_pack:unpack_string(Rest9),
        <<EnvCount:32/big, Rest11/binary>> = Rest10,
        {Environment, Rest12} = decode_string_list(EnvCount, Rest11, []),
        {Features, _} = flurm_protocol_pack:unpack_string(Rest12),
        {ok, #federation_submit_request{
            source_cluster = SourceCluster,
            target_cluster = TargetCluster,
            job_id = JobId,
            name = Name,
            script = Script,
            partition = Partition,
            num_cpus = NumCpus,
            num_nodes = NumNodes,
            memory_mb = MemoryMb,
            time_limit = TimeLimit,
            user_id = UserId,
            group_id = GroupId,
            priority = Priority,
            work_dir = WorkDir,
            std_out = StdOut,
            std_err = StdErr,
            environment = Environment,
            features = Features
        }}
    catch
        _:_ ->
            {ok, #federation_submit_request{}}
    end.

%% Encode REQUEST_FEDERATION_SUBMIT (2032)
encode_federation_submit_request(#federation_submit_request{} = R) ->
    EnvBin = encode_string_list(R#federation_submit_request.environment),
    Parts = [
        flurm_protocol_pack:pack_string(R#federation_submit_request.source_cluster),
        flurm_protocol_pack:pack_string(R#federation_submit_request.target_cluster),
        <<(R#federation_submit_request.job_id):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_submit_request.name),
        flurm_protocol_pack:pack_string(R#federation_submit_request.script),
        flurm_protocol_pack:pack_string(R#federation_submit_request.partition),
        <<(R#federation_submit_request.num_cpus):32/big,
          (R#federation_submit_request.num_nodes):32/big,
          (R#federation_submit_request.memory_mb):32/big,
          (R#federation_submit_request.time_limit):32/big,
          (R#federation_submit_request.user_id):32/big,
          (R#federation_submit_request.group_id):32/big,
          (R#federation_submit_request.priority):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_submit_request.work_dir),
        flurm_protocol_pack:pack_string(R#federation_submit_request.std_out),
        flurm_protocol_pack:pack_string(R#federation_submit_request.std_err),
        <<(length(R#federation_submit_request.environment)):32/big>>,
        EnvBin,
        flurm_protocol_pack:pack_string(R#federation_submit_request.features)
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_submit_request(_) ->
    {ok, <<>>}.

%% Decode RESPONSE_FEDERATION_SUBMIT (2033)
decode_federation_submit_response(Binary) ->
    try
        {SourceCluster, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        <<JobId:32/big, ErrorCode:32/big, Rest2/binary>> = Rest1,
        {ErrorMsg, _} = flurm_protocol_pack:unpack_string(Rest2),
        {ok, #federation_submit_response{
            source_cluster = SourceCluster,
            job_id = JobId,
            error_code = ErrorCode,
            error_msg = ErrorMsg
        }}
    catch
        _:_ ->
            {ok, #federation_submit_response{}}
    end.

%% Encode RESPONSE_FEDERATION_SUBMIT (2033)
encode_federation_submit_response(#federation_submit_response{} = R) ->
    Parts = [
        flurm_protocol_pack:pack_string(R#federation_submit_response.source_cluster),
        <<(R#federation_submit_response.job_id):32/big,
          (R#federation_submit_response.error_code):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_submit_response.error_msg)
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_submit_response(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:32>>}.

%% Decode REQUEST_FEDERATION_JOB_STATUS (2034)
decode_federation_job_status_request(Binary) ->
    try
        {SourceCluster, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        <<JobId:32/big, Rest2/binary>> = Rest1,
        {JobIdStr, _} = flurm_protocol_pack:unpack_string(Rest2),
        {ok, #federation_job_status_request{
            source_cluster = SourceCluster,
            job_id = JobId,
            job_id_str = JobIdStr
        }}
    catch
        _:_ ->
            {ok, #federation_job_status_request{}}
    end.

%% Encode REQUEST_FEDERATION_JOB_STATUS (2034)
encode_federation_job_status_request(#federation_job_status_request{} = R) ->
    Parts = [
        flurm_protocol_pack:pack_string(R#federation_job_status_request.source_cluster),
        <<(R#federation_job_status_request.job_id):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_job_status_request.job_id_str)
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_job_status_request(_) ->
    {ok, <<>>}.

%% Decode RESPONSE_FEDERATION_JOB_STATUS (2035)
decode_federation_job_status_response(Binary) ->
    try
        <<JobId:32/big, JobState:32/big, StateReason:32/big,
          ExitCode:32/big, StartTime:64/big, EndTime:64/big, Rest/binary>> = Binary,
        {Nodes, _} = flurm_protocol_pack:unpack_string(Rest),
        {ok, #federation_job_status_response{
            job_id = JobId,
            job_state = JobState,
            state_reason = StateReason,
            exit_code = ExitCode,
            start_time = StartTime,
            end_time = EndTime,
            nodes = Nodes
        }}
    catch
        _:_ ->
            {ok, #federation_job_status_response{}}
    end.

%% Encode RESPONSE_FEDERATION_JOB_STATUS (2035)
encode_federation_job_status_response(#federation_job_status_response{} = R) ->
    Parts = [
        <<(R#federation_job_status_response.job_id):32/big,
          (R#federation_job_status_response.job_state):32/big,
          (R#federation_job_status_response.state_reason):32/big,
          (R#federation_job_status_response.exit_code):32/big,
          (R#federation_job_status_response.start_time):64/big,
          (R#federation_job_status_response.end_time):64/big>>,
        flurm_protocol_pack:pack_string(R#federation_job_status_response.nodes)
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_job_status_response(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:32, 0:64, 0:64, 0:32>>}.

%% Decode REQUEST_FEDERATION_JOB_CANCEL (2036)
decode_federation_job_cancel_request(Binary) ->
    try
        {SourceCluster, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        <<JobId:32/big, Rest2/binary>> = Rest1,
        {JobIdStr, Rest3} = flurm_protocol_pack:unpack_string(Rest2),
        <<Signal:32/big, _/binary>> = Rest3,
        {ok, #federation_job_cancel_request{
            source_cluster = SourceCluster,
            job_id = JobId,
            job_id_str = JobIdStr,
            signal = Signal
        }}
    catch
        _:_ ->
            {ok, #federation_job_cancel_request{}}
    end.

%% Encode REQUEST_FEDERATION_JOB_CANCEL (2036)
encode_federation_job_cancel_request(#federation_job_cancel_request{} = R) ->
    Parts = [
        flurm_protocol_pack:pack_string(R#federation_job_cancel_request.source_cluster),
        <<(R#federation_job_cancel_request.job_id):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_job_cancel_request.job_id_str),
        <<(R#federation_job_cancel_request.signal):32/big>>
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_job_cancel_request(_) ->
    {ok, <<>>}.

%% Decode RESPONSE_FEDERATION_JOB_CANCEL (2037)
decode_federation_job_cancel_response(Binary) ->
    try
        <<JobId:32/big, ErrorCode:32/big, Rest/binary>> = Binary,
        {ErrorMsg, _} = flurm_protocol_pack:unpack_string(Rest),
        {ok, #federation_job_cancel_response{
            job_id = JobId,
            error_code = ErrorCode,
            error_msg = ErrorMsg
        }}
    catch
        _:_ ->
            {ok, #federation_job_cancel_response{}}
    end.

%% Encode RESPONSE_FEDERATION_JOB_CANCEL (2037)
encode_federation_job_cancel_response(#federation_job_cancel_response{} = R) ->
    Parts = [
        <<(R#federation_job_cancel_response.job_id):32/big,
          (R#federation_job_cancel_response.error_code):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_job_cancel_response.error_msg)
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_job_cancel_response(_) ->
    {ok, <<0:32, 0:32, 0:32>>}.

%% Decode REQUEST_UPDATE_FEDERATION (2064)
decode_update_federation_request(Binary) ->
    try
        %% Format: action:32, cluster_name:packstr, host:packstr, port:32, settings_count:32, [key:packstr, value:packstr]*
        <<ActionCode:32/big, Rest1/binary>> = Binary,
        Action = case ActionCode of
            1 -> add_cluster;
            2 -> remove_cluster;
            3 -> update_settings;
            _ -> unknown
        end,
        {ClusterName, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        {Host, Rest3} = flurm_protocol_pack:unpack_string(Rest2),
        <<Port:32/big, SettingsCount:32/big, Rest4/binary>> = Rest3,
        Settings = decode_settings_map(SettingsCount, Rest4, #{}),
        {ok, #update_federation_request{
            action = Action,
            cluster_name = ClusterName,
            host = Host,
            port = Port,
            settings = Settings
        }}
    catch
        _:_ ->
            {ok, #update_federation_request{action = unknown}}
    end.

%% Decode RESPONSE_UPDATE_FEDERATION (2065)
decode_update_federation_response(Binary) ->
    try
        <<ErrorCode:32/big, Rest/binary>> = Binary,
        {ErrorMsg, _} = flurm_protocol_pack:unpack_string(Rest),
        {ok, #update_federation_response{
            error_code = ErrorCode,
            error_msg = ErrorMsg
        }}
    catch
        _:_ ->
            {ok, #update_federation_response{error_code = 1, error_msg = <<"decode error">>}}
    end.

%% Encode REQUEST_UPDATE_FEDERATION (2064)
encode_update_federation_request(#update_federation_request{} = R) ->
    ActionCode = case R#update_federation_request.action of
        add_cluster -> 1;
        remove_cluster -> 2;
        update_settings -> 3;
        _ -> 0
    end,
    SettingsList = maps:to_list(R#update_federation_request.settings),
    SettingsBin = encode_settings_map(SettingsList),
    Parts = [
        <<ActionCode:32/big>>,
        flurm_protocol_pack:pack_string(R#update_federation_request.cluster_name),
        flurm_protocol_pack:pack_string(R#update_federation_request.host),
        <<(R#update_federation_request.port):32/big,
          (length(SettingsList)):32/big>>,
        SettingsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_update_federation_request(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:32, 0:32>>}.

%% Encode RESPONSE_UPDATE_FEDERATION (2065)
encode_update_federation_response(#update_federation_response{} = R) ->
    Parts = [
        <<(R#update_federation_response.error_code):32/big>>,
        flurm_protocol_pack:pack_string(R#update_federation_response.error_msg)
    ],
    {ok, iolist_to_binary(Parts)};
encode_update_federation_response(_) ->
    {ok, <<0:32, 0:32>>}.

%% Helper: Decode settings map from binary
decode_settings_map(0, _Rest, Acc) ->
    Acc;
decode_settings_map(Count, Binary, Acc) ->
    try
        {Key, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {Value, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        decode_settings_map(Count - 1, Rest2, Acc#{Key => Value})
    catch
        _:_ ->
            Acc
    end.

%% Helper: Encode settings map to binary
encode_settings_map(SettingsList) ->
    lists:map(fun({Key, Value}) ->
        KeyBin = if is_binary(Key) -> Key;
                    is_atom(Key) -> atom_to_binary(Key, utf8);
                    true -> term_to_binary(Key)
                 end,
        ValueBin = if is_binary(Value) -> Value;
                      is_atom(Value) -> atom_to_binary(Value, utf8);
                      is_integer(Value) -> integer_to_binary(Value);
                      true -> term_to_binary(Value)
                   end,
        [flurm_protocol_pack:pack_string(KeyBin),
         flurm_protocol_pack:pack_string(ValueBin)]
    end, SettingsList).

%% Helper: Decode a list of strings for federation messages
decode_string_list(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_string_list(Count, Binary, Acc) ->
    try
        {Str, Rest} = flurm_protocol_pack:unpack_string(Binary),
        decode_string_list(Count - 1, Rest, [Str | Acc])
    catch
        _:_ ->
            {lists:reverse(Acc), <<>>}
    end.

%%%===================================================================
%%% Sibling Job Coordination Message Encoding/Decoding (Phase 7D)
%%%
%%% TLA+ Safety Invariants:
%%%   - SiblingExclusivity: At most one sibling runs at any time
%%%   - OriginAwareness: Origin cluster tracks which sibling is running
%%%   - NoJobLoss: Jobs must have at least one active sibling or be terminal
%%%===================================================================

%% Decode MSG_FED_JOB_SUBMIT (2070) - Origin -> All: Create sibling job
decode_fed_job_submit_msg(Binary) ->
    try
        {FedJobId, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {OriginCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        {TargetCluster, Rest3} = flurm_protocol_pack:unpack_string(Rest2),
        <<SubmitTime:64/big, JobSpecLen:32/big, Rest4/binary>> = Rest3,
        <<JobSpecBin:JobSpecLen/binary, _/binary>> = Rest4,
        JobSpec = binary_to_term(JobSpecBin),
        {ok, #fed_job_submit_msg{
            federation_job_id = FedJobId,
            origin_cluster = OriginCluster,
            target_cluster = TargetCluster,
            job_spec = JobSpec,
            submit_time = SubmitTime
        }}
    catch
        _:_ ->
            {ok, #fed_job_submit_msg{
                federation_job_id = <<>>,
                origin_cluster = <<>>,
                target_cluster = <<>>,
                job_spec = #{}
            }}
    end.

%% Encode MSG_FED_JOB_SUBMIT (2070)
encode_fed_job_submit_msg(#fed_job_submit_msg{} = M) ->
    JobSpecBin = term_to_binary(M#fed_job_submit_msg.job_spec),
    Parts = [
        flurm_protocol_pack:pack_string(M#fed_job_submit_msg.federation_job_id),
        flurm_protocol_pack:pack_string(M#fed_job_submit_msg.origin_cluster),
        flurm_protocol_pack:pack_string(M#fed_job_submit_msg.target_cluster),
        <<(M#fed_job_submit_msg.submit_time):64/big>>,
        <<(byte_size(JobSpecBin)):32/big>>,
        JobSpecBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_job_submit_msg(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:64, 0:32>>}.

%% Decode MSG_FED_JOB_STARTED (2071) - Running -> Origin: Job started
decode_fed_job_started_msg(Binary) ->
    try
        {FedJobId, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {RunningCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<LocalJobId:32/big, StartTime:64/big, _/binary>> = Rest2,
        {ok, #fed_job_started_msg{
            federation_job_id = FedJobId,
            running_cluster = RunningCluster,
            local_job_id = LocalJobId,
            start_time = StartTime
        }}
    catch
        _:_ ->
            {ok, #fed_job_started_msg{
                federation_job_id = <<>>,
                running_cluster = <<>>
            }}
    end.

%% Encode MSG_FED_JOB_STARTED (2071)
encode_fed_job_started_msg(#fed_job_started_msg{} = M) ->
    Parts = [
        flurm_protocol_pack:pack_string(M#fed_job_started_msg.federation_job_id),
        flurm_protocol_pack:pack_string(M#fed_job_started_msg.running_cluster),
        <<(M#fed_job_started_msg.local_job_id):32/big,
          (M#fed_job_started_msg.start_time):64/big>>
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_job_started_msg(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:64>>}.

%% Decode MSG_FED_SIBLING_REVOKE (2072) - Origin -> All: Cancel siblings
decode_fed_sibling_revoke_msg(Binary) ->
    try
        {FedJobId, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {RunningCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        {RevokeReason, _} = flurm_protocol_pack:unpack_string(Rest2),
        {ok, #fed_sibling_revoke_msg{
            federation_job_id = FedJobId,
            running_cluster = RunningCluster,
            revoke_reason = RevokeReason
        }}
    catch
        _:_ ->
            {ok, #fed_sibling_revoke_msg{
                federation_job_id = <<>>,
                running_cluster = <<>>
            }}
    end.

%% Encode MSG_FED_SIBLING_REVOKE (2072)
encode_fed_sibling_revoke_msg(#fed_sibling_revoke_msg{} = M) ->
    Parts = [
        flurm_protocol_pack:pack_string(M#fed_sibling_revoke_msg.federation_job_id),
        flurm_protocol_pack:pack_string(M#fed_sibling_revoke_msg.running_cluster),
        flurm_protocol_pack:pack_string(M#fed_sibling_revoke_msg.revoke_reason)
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_sibling_revoke_msg(_) ->
    {ok, <<0:32, 0:32, 0:32>>}.

%% Decode MSG_FED_JOB_COMPLETED (2073) - Running -> Origin: Job completed
decode_fed_job_completed_msg(Binary) ->
    try
        {FedJobId, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {RunningCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<LocalJobId:32/big, EndTime:64/big, ExitCode:32/signed-big, _/binary>> = Rest2,
        {ok, #fed_job_completed_msg{
            federation_job_id = FedJobId,
            running_cluster = RunningCluster,
            local_job_id = LocalJobId,
            end_time = EndTime,
            exit_code = ExitCode
        }}
    catch
        _:_ ->
            {ok, #fed_job_completed_msg{
                federation_job_id = <<>>,
                running_cluster = <<>>
            }}
    end.

%% Encode MSG_FED_JOB_COMPLETED (2073)
encode_fed_job_completed_msg(#fed_job_completed_msg{} = M) ->
    Parts = [
        flurm_protocol_pack:pack_string(M#fed_job_completed_msg.federation_job_id),
        flurm_protocol_pack:pack_string(M#fed_job_completed_msg.running_cluster),
        <<(M#fed_job_completed_msg.local_job_id):32/big,
          (M#fed_job_completed_msg.end_time):64/big,
          (M#fed_job_completed_msg.exit_code):32/signed-big>>
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_job_completed_msg(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:64, 0:32>>}.

%% Decode MSG_FED_JOB_FAILED (2074) - Running -> Origin: Job failed
decode_fed_job_failed_msg(Binary) ->
    try
        {FedJobId, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {RunningCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<LocalJobId:32/big, EndTime:64/big, ExitCode:32/signed-big, Rest3/binary>> = Rest2,
        {ErrorMsg, _} = flurm_protocol_pack:unpack_string(Rest3),
        {ok, #fed_job_failed_msg{
            federation_job_id = FedJobId,
            running_cluster = RunningCluster,
            local_job_id = LocalJobId,
            end_time = EndTime,
            exit_code = ExitCode,
            error_msg = ErrorMsg
        }}
    catch
        _:_ ->
            {ok, #fed_job_failed_msg{
                federation_job_id = <<>>,
                running_cluster = <<>>
            }}
    end.

%% Encode MSG_FED_JOB_FAILED (2074)
encode_fed_job_failed_msg(#fed_job_failed_msg{} = M) ->
    Parts = [
        flurm_protocol_pack:pack_string(M#fed_job_failed_msg.federation_job_id),
        flurm_protocol_pack:pack_string(M#fed_job_failed_msg.running_cluster),
        <<(M#fed_job_failed_msg.local_job_id):32/big,
          (M#fed_job_failed_msg.end_time):64/big,
          (M#fed_job_failed_msg.exit_code):32/signed-big>>,
        flurm_protocol_pack:pack_string(M#fed_job_failed_msg.error_msg)
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_job_failed_msg(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:64, 0:32, 0:32>>}.

