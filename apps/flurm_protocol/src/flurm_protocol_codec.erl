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
    is_response/1
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

%% @doc Encode a response message with auth section at the BEGINNING.
%%
%% SLURM response format: <<Length:32/big, Header:10/binary, AuthSection, MsgBody>>
%% Auth section: <<AuthHeader:10/binary, CredLen:32/big, Credential:CredLen/binary>>
%% For responses, use auth_type=101 (MUNGE) with a real MUNGE credential.
%% Note: SLURM clients verify the MUNGE credential from responses.
-spec encode_response(non_neg_integer(), term()) -> {ok, binary()} | {error, term()}.
encode_response(MsgType, Body) ->
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
                    %% Generate a real MUNGE credential for the response
                    %% AuthHeader: 8 bytes zero + 2 bytes auth_type (101 = MUNGE)
                    AuthType = 101,  % MUNGE
                    AuthHeader = <<0:64, AuthType:16/big>>,
                    %% Try to get real MUNGE credential
                    Credential = case flurm_munge:encode() of
                        {ok, MungeCred} -> MungeCred;
                        {error, _} -> <<>>  % Fall back to empty
                    end,
                    CredLen = byte_size(Credential),
                    AuthSection = <<AuthHeader/binary, CredLen:32/big, Credential/binary>>,
                    %% Wire format: length, header, auth section, body
                    TotalPayload = <<HeaderBin/binary, AuthSection/binary, BodyBin/binary>>,
                    Length = byte_size(TotalPayload),
                    {ok, <<Length:32/big, TotalPayload/binary>>};
                {error, _} = HeaderError ->
                    HeaderError
            end;
        {error, _} = BodyError ->
            BodyError
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

%% REQUEST_SUBMIT_BATCH_JOB (4003)
decode_body(?REQUEST_SUBMIT_BATCH_JOB, Binary) ->
    decode_batch_job_request(Binary);

%% REQUEST_CANCEL_JOB (4006)
decode_body(?REQUEST_CANCEL_JOB, Binary) ->
    decode_cancel_job_request(Binary);

%% REQUEST_KILL_JOB (5032) - used by scancel
decode_body(?REQUEST_KILL_JOB, Binary) ->
    decode_kill_job_request(Binary);

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

%% RESPONSE_SLURM_RC (8001)
encode_body(?RESPONSE_SLURM_RC, Resp) ->
    encode_slurm_rc_response(Resp);

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
    encode_node_info_response(Resp);

%% REQUEST_PARTITION_INFO (2009)
encode_body(?REQUEST_PARTITION_INFO, #partition_info_request{} = Req) ->
    encode_partition_info_request(Req);
encode_body(?REQUEST_PARTITION_INFO, _) ->
    {ok, <<0:32/big, 0:32/big>>};  % show_flags=0, empty partition name

%% RESPONSE_PARTITION_INFO (2010)
encode_body(?RESPONSE_PARTITION_INFO, Resp) ->
    encode_partition_info_response(Resp);

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
message_type_name(?REQUEST_JOB_STEP_CREATE) -> request_job_step_create;
message_type_name(?RESPONSE_JOB_STEP_CREATE) -> response_job_step_create;
message_type_name(?RESPONSE_SLURM_RC) -> response_slurm_rc;
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

    %% Extract node/task counts from #SBATCH directives
    {MinNodes, NumTasks} = extract_resources(Script),

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
        min_cpus = 1,
        num_tasks = NumTasks,
        cpus_per_task = 1,
        time_limit = TimeLimit,
        priority = 0,
        user_id = UserId,
        group_id = GroupId
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

%% Extract resources from #SBATCH directives
extract_resources(Script) ->
    Nodes = case binary:match(Script, <<"--nodes=">>) of
        {NStart, 8} ->
            <<_:NStart/binary, "--nodes=", NRest/binary>> = Script,
            parse_int_value(NRest);
        nomatch ->
            1
    end,
    Tasks = case binary:match(Script, <<"--ntasks=">>) of
        {TStart, 9} ->
            <<_:TStart/binary, "--ntasks=", TRest/binary>> = Script,
            parse_int_value(TRest);
        nomatch ->
            1
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
%% Based on observed wire format from scancel: 22 bytes body
decode_kill_job_request(Binary) ->
    %% Try string-based format first (SLURM 19.05+)
    case decode_kill_job_string_format(Binary) of
        {ok, _} = Result -> Result;
        {error, _} ->
            %% Fallback: try numeric format
            decode_kill_job_numeric_format(Binary)
    end.

decode_kill_job_string_format(Binary) ->
    case flurm_protocol_pack:unpack_string(Binary) of
        {ok, JobIdStr, Rest1} when byte_size(Rest1) >= 8 ->
            <<StepId:32/big-signed, Signal:16/big, Flags:16/big, Rest2/binary>> = Rest1,
            {ok, Sibling, _Rest3} = flurm_protocol_pack:unpack_string(Rest2),
            {ok, #kill_job_request{
                job_id = 0,
                job_id_str = ensure_binary(JobIdStr),
                step_id = StepId,
                signal = Signal,
                flags = Flags,
                sibling = ensure_binary(Sibling)
            }};
        _ ->
            {error, not_string_format}
    end.

decode_kill_job_numeric_format(Binary) ->
    case Binary of
        <<JobId:32/big, StepId:32/big-signed, Signal:16/big, Flags:16/big>> ->
            {ok, #kill_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = Signal,
                flags = Flags
            }};
        <<JobId:32/big, StepId:32/big-signed>> ->
            {ok, #kill_job_request{
                job_id = JobId,
                step_id = StepId
            }};
        _ ->
            {error, invalid_kill_job_request}
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

%% Encode RESPONSE_JOB_INFO (2004)
encode_job_info_response(#job_info_response{
    last_update = LastUpdate,
    job_count = JobCount,
    jobs = Jobs
}) ->
    JobsBin = [encode_single_job_info(J) || J <- Jobs],
    Parts = [
        <<LastUpdate:64/big, JobCount:32/big>>,
        JobsBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single job_info record
encode_single_job_info(#job_info{} = J) ->
    [
        flurm_protocol_pack:pack_string(J#job_info.account),
        flurm_protocol_pack:pack_time(J#job_info.accrue_time),
        flurm_protocol_pack:pack_string(J#job_info.admin_comment),
        flurm_protocol_pack:pack_string(J#job_info.alloc_node),
        flurm_protocol_pack:pack_uint32(J#job_info.alloc_sid),
        flurm_protocol_pack:pack_uint32(J#job_info.job_id),
        flurm_protocol_pack:pack_uint32(J#job_info.job_state),
        flurm_protocol_pack:pack_string(J#job_info.name),
        flurm_protocol_pack:pack_string(J#job_info.partition),
        flurm_protocol_pack:pack_string(J#job_info.nodes),
        flurm_protocol_pack:pack_uint32(J#job_info.user_id),
        flurm_protocol_pack:pack_uint32(J#job_info.group_id),
        flurm_protocol_pack:pack_uint32(J#job_info.num_nodes),
        flurm_protocol_pack:pack_uint32(J#job_info.num_cpus),
        flurm_protocol_pack:pack_uint32(J#job_info.num_tasks),
        flurm_protocol_pack:pack_uint32(J#job_info.priority),
        flurm_protocol_pack:pack_uint32(J#job_info.time_limit),
        flurm_protocol_pack:pack_time(J#job_info.start_time),
        flurm_protocol_pack:pack_time(J#job_info.end_time),
        flurm_protocol_pack:pack_time(J#job_info.submit_time)
    ].

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
encode_node_info_response(#node_info_response{
    last_update = LastUpdate,
    node_count = NodeCount,
    nodes = Nodes
}) ->
    NodesBin = [encode_single_node_info(N) || N <- Nodes],
    Parts = [
        <<LastUpdate:64/big, NodeCount:32/big>>,
        NodesBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single node_info record
encode_single_node_info(#node_info{} = N) ->
    [
        flurm_protocol_pack:pack_string(N#node_info.name),
        flurm_protocol_pack:pack_string(N#node_info.node_hostname),
        flurm_protocol_pack:pack_string(N#node_info.node_addr),
        flurm_protocol_pack:pack_uint16(N#node_info.port),
        flurm_protocol_pack:pack_uint32(N#node_info.node_state),
        flurm_protocol_pack:pack_string(N#node_info.version),
        flurm_protocol_pack:pack_string(N#node_info.arch),
        flurm_protocol_pack:pack_string(N#node_info.os),
        flurm_protocol_pack:pack_uint32(N#node_info.cpus),
        flurm_protocol_pack:pack_uint32(N#node_info.real_memory),
        flurm_protocol_pack:pack_uint32(N#node_info.free_mem),
        flurm_protocol_pack:pack_uint32(N#node_info.cpu_load),
        flurm_protocol_pack:pack_string(N#node_info.features),
        flurm_protocol_pack:pack_string(N#node_info.partitions)
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
encode_partition_info_response(#partition_info_response{
    last_update = LastUpdate,
    partition_count = PartCount,
    partitions = Partitions
}) ->
    PartsBin = [encode_single_partition_info(P) || P <- Partitions],
    Parts = [
        <<LastUpdate:64/big, PartCount:32/big>>,
        PartsBin
    ],
    {ok, iolist_to_binary(Parts)}.

%% Encode a single partition_info record
encode_single_partition_info(#partition_info{} = P) ->
    [
        flurm_protocol_pack:pack_string(P#partition_info.name),
        flurm_protocol_pack:pack_uint32(P#partition_info.state_up),
        flurm_protocol_pack:pack_uint32(P#partition_info.max_time),
        flurm_protocol_pack:pack_uint32(P#partition_info.default_time),
        flurm_protocol_pack:pack_uint32(P#partition_info.max_nodes),
        flurm_protocol_pack:pack_uint32(P#partition_info.min_nodes),
        flurm_protocol_pack:pack_uint32(P#partition_info.total_nodes),
        flurm_protocol_pack:pack_uint32(P#partition_info.total_cpus),
        flurm_protocol_pack:pack_string(P#partition_info.nodes),
        flurm_protocol_pack:pack_uint32(P#partition_info.priority_tier),
        flurm_protocol_pack:pack_uint32(P#partition_info.priority_job_factor)
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
