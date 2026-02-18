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
%%% The actual encoding/decoding of message bodies is delegated to
%%% specialized codec modules:
%%%   - flurm_codec_job: Job-related messages
%%%   - flurm_codec_step: Job step messages
%%%   - flurm_codec_node: Node and partition messages
%%%   - flurm_codec_system: System/admin messages
%%%   - flurm_codec_federation: Federation messages
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

%% Suppress warnings for functions only used when TEST is defined
-compile({nowarn_unused_function, [
    extract_resources/1,
    extract_resources_from_lines/2,
    parse_resource_from_sbatch_args/2,
    first_nonzero_val/2,
    parse_memory_value/1,
    skip_digits/1,
    parse_int_value/1,
    parse_int_value/2,
    trim_leading_spaces/1
]}).

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
%%% Body Encoding/Decoding - Delegated to Codec Modules
%%%===================================================================

%% @doc Decode message body based on message type.
%% Delegates to specialized codec modules in order of likelihood.
-spec decode_body(non_neg_integer(), binary()) -> {ok, term()} | {error, term()}.
decode_body(MsgType, Binary) ->
    %% Try each codec module in order of message frequency
    case flurm_codec_job:decode_body(MsgType, Binary) of
        unsupported ->
            case flurm_codec_step:decode_body(MsgType, Binary) of
                unsupported ->
                    case flurm_codec_node:decode_body(MsgType, Binary) of
                        unsupported ->
                            case flurm_codec_system:decode_body(MsgType, Binary) of
                                unsupported ->
                                    case flurm_codec_federation:decode_body(MsgType, Binary) of
                                        unsupported ->
                                            %% Unknown message type - return raw body
                                            {ok, Binary};
                                        Result -> Result
                                    end;
                                Result -> Result
                            end;
                        Result -> Result
                    end;
                Result -> Result
            end;
        Result -> Result
    end.

%% @doc Encode message body based on message type.
%% Delegates to specialized codec modules in order of likelihood.
-spec encode_body(non_neg_integer(), term()) -> {ok, binary()} | {error, term()}.
encode_body(MsgType, Body) ->
    %% Try each codec module in order of message frequency
    case flurm_codec_job:encode_body(MsgType, Body) of
        unsupported ->
            case flurm_codec_step:encode_body(MsgType, Body) of
                unsupported ->
                    case flurm_codec_node:encode_body(MsgType, Body) of
                        unsupported ->
                            case flurm_codec_system:encode_body(MsgType, Body) of
                                unsupported ->
                                    case flurm_codec_federation:encode_body(MsgType, Body) of
                                        unsupported ->
                                            %% Raw binary passthrough
                                            case Body of
                                                Bin when is_binary(Bin) ->
                                                    {ok, Bin};
                                                _ ->
                                                    {error, {unsupported_message_type, MsgType, Body}}
                                            end;
                                        Result -> Result
                                    end;
                                Result -> Result
                            end;
                        Result -> Result
                    end;
                Result -> Result
            end;
        Result -> Result
    end.

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
%%% Resource Extraction Functions (exported)
%%%===================================================================

%% @doc Extract resources from job_desc_msg_t binary in SLURM protocol.
%% This attempts to find min_nodes and min_cpus values encoded in the
%% binary protocol format used by sbatch/salloc/srun.
%%
%% Returns {MinNodes, MinCpus} or {0, 0} on failure (fallback to script).
-spec extract_resources_from_protocol(binary()) -> {non_neg_integer(), non_neg_integer()}.
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

%% @doc Extended resource extraction returning full job_desc info
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

%% Encode RESPONSE for reconfigure (using reconfigure_response record)
-spec encode_reconfigure_response(#reconfigure_response{}) -> {ok, binary()}.
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
-spec binary_to_hex(binary()) -> binary().
binary_to_hex(Bin) when is_binary(Bin) ->
    list_to_binary([[io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(Bin)]]).

%%%===================================================================
%%% Internal Helper Functions for Resource Extraction
%%%===================================================================

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
try_fixed_offset_decode(Binary) when byte_size(Binary) >= 40 ->
    %% Scan first 200 bytes for plausible min_nodes/min_cpus pairs
    scan_uint32_pairs(Binary, 0, byte_size(Binary) - 8);
try_fixed_offset_decode(_) ->
    {0, 0}.

%% Scan binary for consecutive uint32 pairs that look like resource counts
scan_uint32_pairs(_Binary, Offset, MaxOffset) when Offset > MaxOffset ->
    {0, 0};
scan_uint32_pairs(Binary, Offset, MaxOffset) ->
    <<_:Offset/binary, V1:32/big, V2:32/big, _/binary>> = Binary,
    case is_valid_resource_value(V1) andalso is_valid_resource_value(V2) of
        true when V1 =< 10000, V2 =< 100000 ->
            case validate_resource_context(Binary, Offset) of
                true -> {V1, V2};
                false -> scan_uint32_pairs(Binary, Offset + 4, MaxOffset)
            end;
        _ ->
            scan_uint32_pairs(Binary, Offset + 4, MaxOffset)
    end.

%% Check if value is a valid resource count
is_valid_resource_value(V) when V > 0, V < 16#FFFFFFFE, V =< 1000000 ->
    true;
is_valid_resource_value(_) ->
    false.

%% Validate that the context around potential resource values looks right
validate_resource_context(Binary, Offset) when byte_size(Binary) >= Offset + 16 ->
    <<_:Offset/binary, _V1:32/big, _V2:32/big, V3:32/big, _/binary>> = Binary,
    case V3 of
        X when X == 16#FFFFFFFE -> true;
        X when X >= 1, X =< 100000 -> true;
        _ -> false
    end;
validate_resource_context(_, _) ->
    false.

%% Strategy 2: Scan for resource-related patterns in the binary
scan_for_resources(Binary) ->
    MinNodes = scan_for_node_count(Binary),
    MinCpus = scan_for_cpu_count(Binary),
    {MinNodes, MinCpus}.

%% Scan for node count by looking for uint32 values preceded by string patterns
scan_for_node_count(Binary) ->
    case find_field_near_pattern(Binary, <<"node">>, 50) of
        V when V > 0, V =< 10000 -> V;
        _ -> 0
    end.

%% Scan for CPU count
scan_for_cpu_count(Binary) ->
    case find_field_near_pattern(Binary, <<"cpu">>, 50) of
        V when V > 0, V =< 100000 -> V;
        _ ->
            case find_field_near_pattern(Binary, <<"task">>, 50) of
                V when V > 0, V =< 100000 -> V;
                _ -> 0
            end
    end.

%% Find a uint32 value near a pattern string
find_field_near_pattern(Binary, Pattern, SearchRadius) ->
    case binary:match(Binary, Pattern) of
        {Start, _} ->
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

%% Extract user_id and group_id from protocol header
extract_uid_gid_from_header(<<_JobId:32/big, UserId:32/big, GroupId:32/big, _/binary>>)
  when UserId < 65536, GroupId < 65536 ->
    {UserId, GroupId};
extract_uid_gid_from_header(_) ->
    {0, 0}.

%% Extract time limit from protocol binary
extract_time_limit_from_protocol(Binary) when byte_size(Binary) >= 100 ->
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
    case V of
        T when T >= 1, T =< 525600 -> T;
        _ -> scan_for_time_limit(Binary, Offset + 4, MaxOffset)
    end.

%% Extract priority from protocol binary
extract_priority_from_protocol(_Binary) ->
    0.

%% Extract partition name from protocol binary
extract_partition_from_protocol(Binary) ->
    case binary:match(Binary, <<"partition">>) of
        {Start, _} when Start + 15 < byte_size(Binary) ->
            <<_:Start/binary, _:72, Rest/binary>> = Binary,
            extract_short_string(Rest, 32);
        _ ->
            <<>>
    end.

%% Extract a short string from binary
extract_short_string(<<Len:32/big, Str:Len/binary, _/binary>>, MaxLen)
  when Len > 0, Len =< MaxLen ->
    strip_null(Str);
extract_short_string(_, _) ->
    <<>>.

%% Find job name by scanning for length-prefixed string
find_job_name(Binary) when byte_size(Binary) > 70 ->
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
                    strip_null(Str);
                false ->
                    find_job_name_scan(Binary, Offset + 1)
            end;
        true ->
            find_job_name_scan(Binary, Offset + 1)
    end;
find_job_name_scan(_, _) ->
    <<"unknown">>.

%% Check if binary contains a printable job name
is_printable_name(<<>>) -> false;
is_printable_name(Bin) ->
    is_printable_name_chars(Bin).

is_printable_name_chars(<<>>) -> true;
is_printable_name_chars(<<0>>) -> true;
is_printable_name_chars(<<C, Rest/binary>>) when C >= 32, C < 127 ->
    is_printable_name_chars(Rest);
is_printable_name_chars(_) -> false.

%% Strip trailing nulls from string
strip_null(Bin) ->
    case binary:match(Bin, <<0>>) of
        {Pos, _} -> <<Stripped:Pos/binary, _/binary>> = Bin, Stripped;
        nomatch -> Bin
    end.

%% Extract resources from #SBATCH directives (exported for tests)
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
            Acc
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

first_nonzero_val(0, New) -> New;
first_nonzero_val(Existing, _) -> Existing.

parse_memory_value(Bin) ->
    RawVal = parse_int_value(Bin),
    Suffix = skip_digits(Bin),
    case Suffix of
        <<$G, _/binary>> -> RawVal * 1024;
        <<$g, _/binary>> -> RawVal * 1024;
        <<$K, _/binary>> -> max(1, RawVal div 1024);
        <<$k, _/binary>> -> max(1, RawVal div 1024);
        <<$M, _/binary>> -> RawVal;
        <<$m, _/binary>> -> RawVal;
        _ -> RawVal
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

trim_leading_spaces(<<$\s, Rest/binary>>) -> trim_leading_spaces(Rest);
trim_leading_spaces(<<$\t, Rest/binary>>) -> trim_leading_spaces(Rest);
trim_leading_spaces(Bin) -> Bin.
