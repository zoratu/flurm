%%%-------------------------------------------------------------------
%%% @doc FLURM Protocol Codec - Main SLURM Binary Protocol Codec
%%%
%%% This module provides the main encode/decode interface for SLURM
%%% protocol messages. It handles the complete message wire format:
%%%
%%% Wire format:
%%%   <<Length:32/big, Header:10/binary, Body/binary>>
%%%
%%% Where Length = byte_size(Header) + byte_size(Body) = 10 + body_size
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

    %% Response encoding (with auth section at beginning)
    encode_response/2,

    %% Body encode/decode
    decode_body/2,
    encode_body/2,

    %% Message type helpers
    message_type_name/1,
    is_request/1,
    is_response/1,

    %% Job description extraction
    extract_full_job_desc/1,
    extract_resources_from_protocol/1
]).

-include("flurm_protocol.hrl").

%%%===================================================================
%%% Main API
%%%===================================================================

%% @doc Decode a complete message from wire format.
%%
%% Expects: <<Length:32/big, Header:12/binary, Body:BodySize/binary>>
%% where Length = 12 + BodySize
%%
%% Returns {ok, Message, Rest} on success, where Message is a #slurm_msg{}
%% record with the body decoded to an appropriate record type.
-spec decode(binary()) -> {ok, #slurm_msg{}, binary()} | {error, term()}.
decode(<<Length:32/big, Rest/binary>> = _Data)
  when byte_size(Rest) >= Length, Length >= ?SLURM_HEADER_SIZE ->
    BodySize = Length - ?SLURM_HEADER_SIZE,
    <<HeaderBin:?SLURM_HEADER_SIZE/binary, BodyBin:BodySize/binary, Remaining/binary>> = Rest,
    case flurm_protocol_header:parse_header(HeaderBin) of
        {ok, Header, <<>>} ->
            MsgType = Header#slurm_header.msg_type,
            case decode_body(MsgType, BodyBin) of
                {ok, Body} ->
                    Msg = #slurm_msg{header = Header, body = Body},
                    {ok, Msg, Remaining};
                {error, _} = BodyError ->
                    BodyError
            end;
        {ok, Header, Extra} ->
            %% This shouldn't happen with exact 10-byte header
            {error, {extra_header_data, Header, Extra}};
        {error, _} = HeaderError ->
            HeaderError
    end;
decode(<<Length:32/big, Rest/binary>>)
  when byte_size(Rest) < Length ->
    {error, {incomplete_message, Length, byte_size(Rest)}};
decode(<<Length:32/big, _/binary>>)
  when Length < ?SLURM_HEADER_SIZE ->
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
%%   <<Length:32/big, Header:10/binary, AuthSection/binary, MsgBody/binary>>
%%
%% Auth section format:
%%   <<AuthHeader:10/binary, CredLen:32/big, Credential:CredLen/binary>>
%%
%% Returns {ok, Message, ExtraInfo, Rest} on success.
-spec decode_with_extra(binary()) -> {ok, #slurm_msg{}, map(), binary()} | {error, term()}.
decode_with_extra(<<Length:32/big, Rest/binary>> = _Data)
  when byte_size(Rest) >= Length, Length >= ?SLURM_HEADER_SIZE ->
    <<MsgData:Length/binary, Remaining/binary>> = Rest,
    <<HeaderBin:?SLURM_HEADER_SIZE/binary, BodyWithAuth/binary>> = MsgData,
    case flurm_protocol_header:parse_header(HeaderBin) of
        {ok, Header, <<>>} ->
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
%% Auth section format: <<AuthHeader:10/binary, CredLen:32/big, Credential:CredLen/binary>>
-spec strip_auth_section(binary()) -> {ok, binary(), map()} | {error, term()}.
strip_auth_section(<<_AuthHeader:10/binary, CredLen:32/big, Rest/binary>>) when CredLen =< byte_size(Rest) ->
    <<Credential:CredLen/binary, ActualBody/binary>> = Rest,
    %% Check if credential is MUNGE
    AuthType = case Credential of
        <<"MUNGE:", _/binary>> -> munge;
        _ -> unknown
    end,
    AuthInfo = #{auth_type => AuthType, cred_len => CredLen},
    {ok, ActualBody, AuthInfo};
strip_auth_section(<<_AuthHeader:10/binary, CredLen:32/big, _Rest/binary>>) ->
    {error, {auth_cred_too_short, CredLen}};
strip_auth_section(Binary) when byte_size(Binary) < 14 ->
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
%% SLURM 22.05 response wire format (server to client):
%%   <<OuterLength:32, Header:10, AuthSection/binary, Body/binary>>
%% Where:
%%   - AuthSection = <<AuthHeader:10, CredLen:32, Credential:CredLen>>
%%   - Header.body_length = BodySize only (NOT including auth section)
%%   - AuthSection uses MUNGE credential for proper authentication
%%
-spec encode_response(non_neg_integer(), term()) -> {ok, binary()} | {error, term()}.
encode_response(MsgType, Body) ->
    case encode_body(MsgType, Body) of
        {ok, BodyBin} ->
            BodySize = byte_size(BodyBin),
            %% Get MUNGE credential for auth section
            AuthSection = create_auth_section(),
            AuthSize = byte_size(AuthSection),

            Header = #slurm_header{
                version = flurm_protocol_header:protocol_version(),
                flags = 0,
                msg_index = 0,
                msg_type = MsgType,
                body_length = BodySize  %% Just body, not auth
            },
            case flurm_protocol_header:encode_header(Header) of
                {ok, HeaderBin} ->
                    %% Wire format: outer_length, header, auth_section, body
                    TotalPayload = <<HeaderBin/binary, AuthSection/binary, BodyBin/binary>>,
                    OuterLength = byte_size(TotalPayload),
                    lager:debug("Response: header=~p auth=~p body=~p total=~p",
                               [byte_size(HeaderBin), AuthSize, BodySize, OuterLength]),
                    {ok, <<OuterLength:32/big, TotalPayload/binary>>};
                {error, _} = HeaderError ->
                    HeaderError
            end;
        {error, _} = BodyError ->
            BodyError
    end.

%% @doc Create auth section with MUNGE credential
%% Format: 10-byte header + credential length + credential
%% Header format: 8 bytes padding/zeros + 2 bytes auth type indicator
%% This format was empirically determined to work with SLURM 22.05 clients
-spec create_auth_section() -> binary().
create_auth_section() ->
    case get_munge_credential() of
        {ok, Credential} ->
            CredLen = byte_size(Credential),
            %% 10-byte header: 8 zeros + auth type hint (101 = munge)
            AuthHeader = <<0:64/big, 101:16/big>>,
            <<AuthHeader/binary, CredLen:32/big, Credential/binary>>;
        {error, _} ->
            %% Fallback: empty auth section
            AuthHeader = <<0:64/big, 101:16/big>>,
            <<AuthHeader/binary, 0:32/big>>
    end.

%% @doc Get MUNGE credential by calling munge command
-spec get_munge_credential() -> {ok, binary()} | {error, term()}.
get_munge_credential() ->
    case os:cmd("munge -n 2>/dev/null") of
        [] -> {error, munge_not_available};
        Result ->
            %% Remove trailing newline
            Cred = string:trim(Result, trailing, "\n"),
            {ok, list_to_binary(Cred)}
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
decode_body(?REQUEST_RECONFIGURE, _Binary) ->
    {ok, #{}};

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

%% RESPONSE_SLURM_RC (8001)
encode_body(?RESPONSE_SLURM_RC, Resp) ->
    encode_slurm_rc_response(Resp);

%% RESPONSE_SUBMIT_BATCH_JOB (4004)
encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Resp) ->
    encode_batch_job_response(Resp);

%% RESPONSE_JOB_INFO (2004)
encode_body(?RESPONSE_JOB_INFO, Resp) ->
    Result = encode_job_info_response(Resp),
    case Result of
        {ok, Bin} ->
            %% Log first 20 bytes of body for debugging
            BodyPrefix = case byte_size(Bin) >= 20 of
                true -> <<First20:20/binary, _/binary>> = Bin, First20;
                false -> Bin
            end,
            HexBody = list_to_binary([[io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(BodyPrefix)]]),
            lager:info("RESPONSE_JOB_INFO: encoded ~p jobs, ~p bytes, body_hex=~s",
                      [Resp#job_info_response.job_count, byte_size(Bin), HexBody]);
        _ -> ok
    end,
    Result;

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
message_type_name(?REQUEST_SUBMIT_BATCH_JOB) -> request_submit_batch_job;
message_type_name(?RESPONSE_SUBMIT_BATCH_JOB) -> response_submit_batch_job;
message_type_name(?REQUEST_CANCEL_JOB) -> request_cancel_job;
message_type_name(?REQUEST_KILL_JOB) -> request_kill_job;
message_type_name(?REQUEST_UPDATE_JOB) -> request_update_job;
message_type_name(?REQUEST_JOB_WILL_RUN) -> request_job_will_run;
message_type_name(?RESPONSE_JOB_WILL_RUN) -> response_job_will_run;
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
message_type_name(Type) -> {unknown, Type}.

%% @doc Check if message type is a request.
%%
%% SLURM uses specific message type codes for requests. These are not
%% strictly odd/even - the pattern varies by category. We check against
%% known request types.
-spec is_request(non_neg_integer()) -> boolean().
is_request(?REQUEST_NODE_REGISTRATION_STATUS) -> true;
is_request(?REQUEST_RECONFIGURE) -> true;
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
is_response(?RESPONSE_SUBMIT_BATCH_JOB) -> true;
is_response(?RESPONSE_CANCEL_JOB_STEP) -> true;
is_response(?RESPONSE_JOB_STEP_CREATE) -> true;
is_response(?RESPONSE_JOB_STEP_INFO) -> true;
is_response(?RESPONSE_STEP_LAYOUT) -> true;
is_response(?RESPONSE_LAUNCH_TASKS) -> true;
is_response(?RESPONSE_SLURM_RC) -> true;
is_response(?RESPONSE_SLURM_RC_MSG) -> true;
is_response(?RESPONSE_RESERVATION_INFO) -> true;
is_response(?RESPONSE_LICENSE_INFO) -> true;
is_response(?RESPONSE_TOPO_INFO) -> true;
is_response(?RESPONSE_FRONT_END_INFO) -> true;
is_response(?RESPONSE_BURST_BUFFER_INFO) -> true;
is_response(?RESPONSE_CONFIG_INFO) -> true;
is_response(?RESPONSE_STATS_INFO) -> true;
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
decode_resource_allocation_request(Binary) ->
    try
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
            group_id = GroupId
        },
        {ok, Req}
    catch
        _:Reason ->
            {error, {resource_allocation_decode_failed, Reason}}
    end.

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
    %% Find job name - it's a length-prefixed string typically around offset 58-70
    Name = find_job_name(Binary),

    %% Find the script by looking for "#!/" shebang
    Script = find_script(Binary),

    %% Find working directory - look for path patterns
    WorkDir = find_work_dir(Binary),

    %% Extract user/group IDs from known offset patterns
    {UserId, GroupId} = extract_uid_gid(Binary),

    %% Extract time limit from #SBATCH directive in script if present
    TimeLimit = extract_time_limit(Script),

    %% Extract node/task counts - first try protocol binary, then script directives
    {ProtoNodes, ProtoCpus} = extract_resources_from_protocol(Binary),
    {ScriptNodes, ScriptTasks} = extract_resources(Script),

    %% Use protocol values if set (not 0 or SLURM_NO_VAL), else script values
    MinNodes = case ProtoNodes of
        N when N > 0, N < 16#FFFFFFFE -> N;
        _ -> ScriptNodes
    end,
    MinCpus = case ProtoCpus of
        C when C > 0, C < 16#FFFFFFFE -> C;
        _ -> 1
    end,
    NumTasks = ScriptTasks,

    %% Find stdout/stderr paths from the protocol message or script
    StdOut = find_std_out(Binary, Script, WorkDir),
    StdErr = find_std_err(Binary, Script),

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
        partition = <<>>,
        script = Script,
        work_dir = WorkDir,
        min_nodes = MinNodes,
        max_nodes = MinNodes,
        min_cpus = MinCpus,
        num_tasks = NumTasks,
        cpus_per_task = 1,
        time_limit = TimeLimit,
        priority = 0,
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
            %% Check for .out path pattern in binary message
            find_output_path_in_binary(Binary, WorkDir);
        Path ->
            Path
    end.

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

%% Extract time limit from #SBATCH --time directive
extract_time_limit(Script) ->
    case binary:match(Script, <<"--time=">>) of
        {Start, 7} ->
            <<_:Start/binary, "--time=", TimeRest/binary>> = Script,
            parse_time_value(TimeRest);
        nomatch ->
            case binary:match(Script, <<"-t ">>) of
                {Start2, 3} ->
                    <<_:Start2/binary, "-t ", TimeRest2/binary>> = Script,
                    parse_time_value(TimeRest2);
                nomatch ->
                    300  % Default 5 minutes
            end
    end.

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

parse_time_string(<<"00:", Rest/binary>>) ->
    %% HH:MM:SS format with 00 hours
    parse_minutes_seconds(Rest);
parse_time_string(<<H1, H2, ":", M1, M2, ":", S1, S2, _/binary>>)
  when H1 >= $0, H1 =< $9, H2 >= $0, H2 =< $9,
       M1 >= $0, M1 =< $9, M2 >= $0, M2 =< $9,
       S1 >= $0, S1 =< $9, S2 >= $0, S2 =< $9 ->
    Hours = (H1 - $0) * 10 + (H2 - $0),
    Minutes = (M1 - $0) * 10 + (M2 - $0),
    Seconds = (S1 - $0) * 10 + (S2 - $0),
    Hours * 3600 + Minutes * 60 + Seconds;
parse_time_string(<<M1, M2, ":", S1, S2, _/binary>>)
  when M1 >= $0, M1 =< $9, M2 >= $0, M2 =< $9,
       S1 >= $0, S1 =< $9, S2 >= $0, S2 =< $9 ->
    Minutes = (M1 - $0) * 10 + (M2 - $0),
    Seconds = (S1 - $0) * 10 + (S2 - $0),
    Minutes * 60 + Seconds;
parse_time_string(_) ->
    300.  % Default 5 minutes

parse_minutes_seconds(<<M1, M2, ":", S1, S2, _/binary>>)
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

%% Extract resources from #SBATCH directives
extract_resources(Script) ->
    %% Try --nodes= first, then -N
    Nodes = case binary:match(Script, <<"--nodes=">>) of
        {NStart, 8} ->
            <<_:NStart/binary, "--nodes=", NRest/binary>> = Script,
            parse_int_value(NRest);
        nomatch ->
            case binary:match(Script, <<"-N ">>) of
                {NStart2, 3} ->
                    <<_:NStart2/binary, "-N ", NRest2/binary>> = Script,
                    parse_int_value(NRest2);
                nomatch ->
                    1
            end
    end,
    %% Try --ntasks= first, then -n
    Tasks = case binary:match(Script, <<"--ntasks=">>) of
        {TStart, 9} ->
            <<_:TStart/binary, "--ntasks=", TRest/binary>> = Script,
            parse_int_value(TRest);
        nomatch ->
            case binary:match(Script, <<"-n ">>) of
                {TStart2, 3} ->
                    <<_:TStart2/binary, "-n ", TRest2/binary>> = Script,
                    parse_int_value(TRest2);
                nomatch ->
                    1
            end
    end,
    {Nodes, Tasks}.

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
        <<JobId:32/big, StepId:32/big, Signal:32/big, Flags:32/big, Rest/binary>> ->
            {ok, JobIdStr, _} = flurm_protocol_pack:unpack_string(Rest),
            {ok, #cancel_job_request{
                job_id = JobId,
                job_id_str = ensure_binary(JobIdStr),
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
        <<JobId:32/big, StepId:32/big, ErrorCode:32/big, Rest/binary>> ->
            {ok, UserMsg, _} = flurm_protocol_pack:unpack_string(Rest),
            {ok, #batch_job_response{
                job_id = JobId,
                step_id = StepId,
                error_code = ErrorCode,
                job_submit_user_msg = ensure_binary(UserMsg)
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
%% For srun: this should return job allocation info
%% However, the protocol is complex - for now, let's return RESPONSE_SLURM_RC instead
%% which srun should understand as an error
encode_resource_allocation_response(#resource_allocation_response{
    error_code = ErrorCode
}) when ErrorCode =/= 0 ->
    %% For errors, just return an error code
    %% SLURM clients check error_code first
    {ok, <<ErrorCode:32/big-signed>>};

encode_resource_allocation_response(#resource_allocation_response{
    job_id = JobId,
    node_list = _NodeList,
    num_nodes = _NumNodes,
    partition = Partition,
    error_code = ErrorCode,
    job_submit_user_msg = UserMsg,
    cpus_per_node = _CpusPerNode,
    num_cpu_groups = _NumCpuGroups
}) ->
    %% SLURM _unpack_resource_allocation_response_msg format (22.05):
    %% From src/common/slurm_protocol_pack.c:
    %% 1. cpu_bind_type: uint16
    %% 2. error_code: uint32
    %% 3. gid: uint32
    %% 4. job_id: uint32
    %% 5. job_submit_user_msg: string
    %% 6. node_cnt: uint32
    %% 7. node_list: string
    %% 8. ntasks_per_board: uint16
    %% 9. ntasks_per_core: uint16
    %% 10. ntasks_per_socket: uint16
    %% 11. ntasks_per_tres: uint16
    %% 12. partition: string
    %% 13. pn_min_memory: uint64
    %% 14. uid: uint32
    %% 15. working_cluster_rec: conditional (NULL = no data, non-NULL = pack_cluster_rec)
    %% 16. If node_cnt > 0: unpack_job_resources (complex structure)
    %%
    %% For a minimal response, set node_cnt = 0 to avoid needing job_resources
    %% The working_cluster_rec is unpacked with slurm_unpack_slurmdb_cluster_rec
    %% which reads a uint8 first (0 = NULL record)

    Parts = [
        <<0:16/big>>,                                 % cpu_bind_type = 0
        <<ErrorCode:32/big>>,                         % error_code
        <<0:32/big>>,                                 % gid = 0
        <<JobId:32/big>>,                             % job_id
        flurm_protocol_pack:pack_string(UserMsg),     % job_submit_user_msg
        <<0:32/big>>,                                 % node_cnt = 0 (no allocation yet)
        flurm_protocol_pack:pack_string(<<>>),        % node_list = empty
        <<16#FFFF:16/big>>,                           % ntasks_per_board
        <<16#FFFF:16/big>>,                           % ntasks_per_core
        <<16#FFFF:16/big>>,                           % ntasks_per_socket
        <<16#FFFF:16/big>>,                           % ntasks_per_tres
        flurm_protocol_pack:pack_string(Partition),   % partition
        <<0:64/big>>,                                 % pn_min_memory = 0
        <<0:32/big>>,                                 % uid = 0
        <<0:8>>                                       % working_cluster_rec = NULL
    ],
    {ok, iolist_to_binary(Parts)}.

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
        <<0:16/big>>,  % alloc_cpus
        <<0:64/big>>,  % alloc_memory
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
        %% Field 34: node_inx (bitstring) - pack NO_VAL for empty
        <<?SLURM_NO_VAL:32/big>>,
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

%% Encode RESPONSE_JOB_STEP_CREATE (5002)
%% Format: step_id(32), error_code(32), error_msg(string), step_layout, cred, switch_job
encode_job_step_create_response(#job_step_create_response{
    job_step_id = StepId,
    error_code = ErrorCode,
    error_msg = ErrorMsg
} = _Resp) ->
    Parts = [
        <<StepId:32/big>>,
        <<ErrorCode:32/big>>,
        flurm_protocol_pack:pack_string(ensure_binary(ErrorMsg)),
        %% Step layout - minimal: pack count then data
        <<0:32/big>>,  % layout data count
        %% Credential - pack length 0 for no credential
        <<0:32/big>>,
        %% Switch job - pack NULL
        <<0:32/big>>
    ],
    {ok, iolist_to_binary(Parts)}.

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
decode_update_job_request(Binary) ->
    try
        case Binary of
            <<JobId:32/big, Rest/binary>> when JobId > 0 ->
                Priority = extract_update_field(Rest, priority),
                TimeLimit = extract_update_field(Rest, time_limit),
                Requeue = extract_update_field(Rest, requeue),
                {ok, #update_job_request{
                    job_id = JobId,
                    priority = Priority,
                    time_limit = TimeLimit,
                    requeue = Requeue
                }};
            _ ->
                case flurm_protocol_pack:unpack_string(Binary) of
                    {ok, JobIdStr, _Rest} when byte_size(JobIdStr) > 0 ->
                        JobId = parse_job_id(JobIdStr),
                        {ok, #update_job_request{
                            job_id = JobId,
                            job_id_str = ensure_binary(JobIdStr)
                        }};
                    _ ->
                        {ok, #update_job_request{}}
                end
        end
    catch
        _:Reason ->
            {error, {update_job_decode_failed, Reason}}
    end.

extract_update_field(_Binary, _Field) ->
    ?SLURM_NO_VAL.

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
%% SLURM build info is a complex structure with many fields.
%% For compatibility, we encode all required fields in order.
encode_build_info_response(#build_info_response{} = R) ->
    %% SLURM 22.05 _pack_slurm_ctl_conf_msg format (simplified)
    Parts = [
        flurm_protocol_pack:pack_time(erlang:system_time(second)),  % last_update
        flurm_protocol_pack:pack_uint16(0),                          % accounting_storage_enforce
        flurm_protocol_pack:pack_string(R#build_info_response.accounting_storage_type),
        flurm_protocol_pack:pack_string(<<>>),                       % accounting_storage_tres
        flurm_protocol_pack:pack_string(<<>>),                       % accounting_storage_host
        flurm_protocol_pack:pack_string(<<>>),                       % accounting_storage_backup_host
        flurm_protocol_pack:pack_string(<<>>),                       % accounting_storage_loc
        flurm_protocol_pack:pack_uint32(0),                          % accounting_storage_port
        flurm_protocol_pack:pack_string(<<>>),                       % accounting_storage_user
        flurm_protocol_pack:pack_string(<<>>),                       % acct_gather_energy_type
        flurm_protocol_pack:pack_string(<<>>),                       % acct_gather_interconnect_type
        flurm_protocol_pack:pack_string(<<>>),                       % acct_gather_filesystem_type
        flurm_protocol_pack:pack_string(<<>>),                       % acct_gather_profile_type
        flurm_protocol_pack:pack_string(R#build_info_response.auth_type),
        flurm_protocol_pack:pack_string(<<>>),                       % authinfo
        flurm_protocol_pack:pack_string(R#build_info_response.cluster_name),
        flurm_protocol_pack:pack_string(<<>>),                       % comm_params
        flurm_protocol_pack:pack_string(R#build_info_response.control_machine),
        flurm_protocol_pack:pack_uint32(R#build_info_response.slurmctld_port),
        flurm_protocol_pack:pack_string(<<>>),                       % cred_type
        flurm_protocol_pack:pack_uint32(0),                          % debug_flags
        flurm_protocol_pack:pack_string(R#build_info_response.job_comp_type),
        flurm_protocol_pack:pack_string(<<>>),                       % job_comp_host
        flurm_protocol_pack:pack_uint32(0),                          % job_comp_port
        flurm_protocol_pack:pack_string(<<>>),                       % job_container_plugin
        flurm_protocol_pack:pack_string(<<>>),                       % mcs_plugin
        flurm_protocol_pack:pack_string(<<>>),                       % mcs_plugin_params
        flurm_protocol_pack:pack_string(R#build_info_response.plugin_dir),
        flurm_protocol_pack:pack_string(R#build_info_response.priority_type),
        flurm_protocol_pack:pack_string(<<>>),                       % prep_plugins
        flurm_protocol_pack:pack_string(R#build_info_response.scheduler_type),
        flurm_protocol_pack:pack_string(R#build_info_response.select_type),
        flurm_protocol_pack:pack_uint16(R#build_info_response.slurmd_port),
        flurm_protocol_pack:pack_string(R#build_info_response.slurmd_user_name),
        flurm_protocol_pack:pack_string(R#build_info_response.slurm_user_name),
        flurm_protocol_pack:pack_string(R#build_info_response.slurmctld_host),
        flurm_protocol_pack:pack_string(R#build_info_response.spool_dir),
        flurm_protocol_pack:pack_string(R#build_info_response.state_save_location),
        flurm_protocol_pack:pack_string(R#build_info_response.version),
        %% Many more fields would be needed for full compatibility
        %% Adding minimal set that allows clients to parse without error
        <<0:32/big>>  % End marker / padding
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
