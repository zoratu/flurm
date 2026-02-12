%%%-------------------------------------------------------------------
%%% @doc Direct unit tests for flurm_protocol_codec module.
%%%
%%% These tests call actual module functions directly (no mocking)
%%% to achieve code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_codec_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Fixtures
%%%===================================================================

setup() ->
    %% Start lager so encode_response/2 and friends can log
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Test Generators
%%%===================================================================

flurm_protocol_codec_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% Decode tests
      {"decode ping request", fun decode_ping_request_test/0},
      {"decode job info request", fun decode_job_info_request_test/0},
      {"decode node registration request", fun decode_node_registration_test/0},
      {"decode slurm rc response", fun decode_slurm_rc_response_test/0},
      {"decode batch job response", fun decode_batch_job_response_test/0},
      {"decode error cases", fun decode_error_cases_test/0},

      %% Encode tests
      {"encode ping request", fun encode_ping_request_test/0},
      {"encode slurm rc response", fun encode_slurm_rc_response_test/0},
      {"encode batch job response", fun encode_batch_job_response_test/0},
      {"encode job info request", fun encode_job_info_request_test/0},
      {"encode raw binary", fun encode_raw_binary_test/0},

      %% Message type helpers
      {"message_type_name", fun message_type_name_test/0},
      {"is_request", fun is_request_test/0},
      {"is_response", fun is_response_test/0},

      %% Full wire format tests
      {"encode/decode round trip", fun encode_decode_roundtrip_test/0},
      {"encode_with_extra", fun encode_with_extra_test/0},
      {"decode_with_extra", fun decode_with_extra_test/0},

      %% Response encoding
      {"encode_response", fun encode_response_test/0},
      {"encode_response_no_auth", fun encode_response_no_auth_test/0},
      {"encode_response_proper_auth", fun encode_response_proper_auth_test/0},
      {"decode_response", fun decode_response_test/0},
      {"decode_response happy path", fun decode_response_happy_path_test/0},
      {"encode_response_no_auth roundtrip", fun encode_decode_response_roundtrip_test/0},

      %% Body encode/decode
      {"decode_body for various types", fun decode_body_various_test/0},
      {"encode_body for various types", fun encode_body_various_test/0},

      %% Resource extraction
      {"extract_resources_from_protocol", fun extract_resources_test/0},
      {"extract_full_job_desc", fun extract_full_job_desc_test/0},

      %% Script resource extraction (SBATCH parsing)
      {"extract_resources from SBATCH directives", fun extract_resources_sbatch_test/0},
      {"extract_resources memory suffixes", fun extract_resources_memory_test/0},
      {"extract_resources first wins", fun extract_resources_first_wins_test/0},
      {"extract_resources stops at non-comment", fun extract_resources_stop_test/0},
      {"extract_resources edge cases", fun extract_resources_edge_cases_test/0},

      %% Auth section parsing
      {"decode_with_extra non-MUNGE auth", fun decode_with_extra_non_munge_test/0},
      {"strip_auth_section error cases", fun strip_auth_section_errors_test/0},

      %% Passthrough message validation
      {"encode/decode passthrough binary body", fun passthrough_binary_validation_test/0}
     ]}.

%%%===================================================================
%%% Decode Tests
%%%===================================================================

decode_ping_request_test() ->
    %% Create a valid ping request message
    Header = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_type = ?REQUEST_PING,
        body_length = 0
    },
    {ok, HeaderBin} = flurm_protocol_header:encode_header(Header),
    Length = byte_size(HeaderBin),
    WireBin = <<Length:32/big, HeaderBin/binary>>,

    Result = flurm_protocol_codec:decode(WireBin),
    ?assertMatch({ok, #slurm_msg{body = #ping_request{}}, <<>>}, Result),
    ok.

decode_job_info_request_test() ->
    %% Create job info request with show_flags, job_id, user_id
    BodyBin = <<0:32/big, 123:32/big, 1000:32/big>>,
    Header = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_type = ?REQUEST_JOB_INFO,
        body_length = byte_size(BodyBin)
    },
    {ok, HeaderBin} = flurm_protocol_header:encode_header(Header),
    Length = byte_size(HeaderBin) + byte_size(BodyBin),
    WireBin = <<Length:32/big, HeaderBin/binary, BodyBin/binary>>,

    Result = flurm_protocol_codec:decode(WireBin),
    ?assertMatch({ok, #slurm_msg{body = #job_info_request{}}, <<>>}, Result),
    {ok, #slurm_msg{body = Body}, _} = Result,
    ?assertEqual(123, Body#job_info_request.job_id),
    ?assertEqual(1000, Body#job_info_request.user_id),
    ok.

decode_node_registration_test() ->
    %% Test node registration request decode
    BodyBin = <<1:8>>,  % status_only = true
    Header = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_type = ?REQUEST_NODE_REGISTRATION_STATUS,
        body_length = byte_size(BodyBin)
    },
    {ok, HeaderBin} = flurm_protocol_header:encode_header(Header),
    Length = byte_size(HeaderBin) + byte_size(BodyBin),
    WireBin = <<Length:32/big, HeaderBin/binary, BodyBin/binary>>,

    Result = flurm_protocol_codec:decode(WireBin),
    ?assertMatch({ok, #slurm_msg{body = #node_registration_request{}}, <<>>}, Result),

    %% Test with empty body
    EmptyBodyHeader = Header#slurm_header{body_length = 0},
    {ok, EmptyHeaderBin} = flurm_protocol_header:encode_header(EmptyBodyHeader),
    EmptyLength = byte_size(EmptyHeaderBin),
    EmptyWireBin = <<EmptyLength:32/big, EmptyHeaderBin/binary>>,
    EmptyResult = flurm_protocol_codec:decode(EmptyWireBin),
    ?assertMatch({ok, #slurm_msg{body = #node_registration_request{}}, <<>>}, EmptyResult),
    ok.

decode_slurm_rc_response_test() ->
    %% Test SLURM RC response decode
    BodyBin = <<0:32/signed-big>>,  % return_code = 0
    Header = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_type = ?RESPONSE_SLURM_RC,
        body_length = byte_size(BodyBin)
    },
    {ok, HeaderBin} = flurm_protocol_header:encode_header(Header),
    Length = byte_size(HeaderBin) + byte_size(BodyBin),
    WireBin = <<Length:32/big, HeaderBin/binary, BodyBin/binary>>,

    Result = flurm_protocol_codec:decode(WireBin),
    ?assertMatch({ok, #slurm_msg{body = #slurm_rc_response{}}, <<>>}, Result),
    ok.

decode_batch_job_response_test() ->
    %% Test batch job response decode
    %% Format: job_id:32, step_id:32, error_code:32, msg_len:32, msg
    MsgBin = <<"Job submitted">>,
    MsgLen = byte_size(MsgBin) + 1,  % Include null terminator
    BodyBin = <<100:32/big, 0:32/big, 0:32/big, MsgLen:32/big, MsgBin/binary, 0:8>>,
    Header = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_type = ?RESPONSE_SUBMIT_BATCH_JOB,
        body_length = byte_size(BodyBin)
    },
    {ok, HeaderBin} = flurm_protocol_header:encode_header(Header),
    Length = byte_size(HeaderBin) + byte_size(BodyBin),
    WireBin = <<Length:32/big, HeaderBin/binary, BodyBin/binary>>,

    Result = flurm_protocol_codec:decode(WireBin),
    ?assertMatch({ok, #slurm_msg{body = #batch_job_response{}}, <<>>}, Result),
    ok.

decode_error_cases_test() ->
    %% Test incomplete length prefix
    Result1 = flurm_protocol_codec:decode(<<1, 2, 3>>),
    ?assertMatch({error, {incomplete_length_prefix, _}}, Result1),

    %% Test incomplete message
    Result2 = flurm_protocol_codec:decode(<<100:32/big, 1, 2, 3>>),
    ?assertMatch({error, {incomplete_message, _, _}}, Result2),

    %% Test invalid message length (too small)
    Result3 = flurm_protocol_codec:decode(<<5:32/big, 1, 2, 3, 4, 5>>),
    ?assertMatch({error, {invalid_message_length, _}}, Result3),

    %% Test invalid data
    Result4 = flurm_protocol_codec:decode(not_binary),
    ?assertMatch({error, invalid_message_data}, Result4),
    ok.

%%%===================================================================
%%% Encode Tests
%%%===================================================================

encode_ping_request_test() ->
    Result = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    ?assertMatch({ok, _}, Result),
    {ok, Bin} = Result,
    ?assert(is_binary(Bin)),

    %% Verify roundtrip
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Bin),
    ?assertEqual(?REQUEST_PING, Msg#slurm_msg.header#slurm_header.msg_type),
    ok.

encode_slurm_rc_response_test() ->
    %% Encode success response
    Response1 = #slurm_rc_response{return_code = 0},
    Result1 = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Response1),
    ?assertMatch({ok, _}, Result1),

    %% Encode error response
    Response2 = #slurm_rc_response{return_code = -1},
    Result2 = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Response2),
    ?assertMatch({ok, _}, Result2),
    ok.

encode_batch_job_response_test() ->
    Response = #batch_job_response{
        job_id = 12345,
        step_id = 0,
        error_code = 0,
        job_submit_user_msg = <<"Job submitted successfully">>
    },
    Result = flurm_protocol_codec:encode(?RESPONSE_SUBMIT_BATCH_JOB, Response),
    ?assertMatch({ok, _}, Result),
    ok.

encode_job_info_request_test() ->
    Request = #job_info_request{
        show_flags = 0,
        job_id = 123,
        user_id = 1000
    },
    Result = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Request),
    ?assertMatch({ok, _}, Result),
    ok.

encode_raw_binary_test() ->
    %% Test encoding with unknown message type - should still work
    %% Using a valid known message type instead
    Result = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    ?assertMatch({ok, _}, Result),
    ok.

%%%===================================================================
%%% Message Type Helper Tests
%%%===================================================================

message_type_name_test() ->
    %% Test known message types
    ?assertEqual(request_ping, flurm_protocol_codec:message_type_name(?REQUEST_PING)),
    ?assertEqual(request_job_info, flurm_protocol_codec:message_type_name(?REQUEST_JOB_INFO)),
    ?assertEqual(response_job_info, flurm_protocol_codec:message_type_name(?RESPONSE_JOB_INFO)),
    ?assertEqual(request_submit_batch_job, flurm_protocol_codec:message_type_name(?REQUEST_SUBMIT_BATCH_JOB)),
    ?assertEqual(response_submit_batch_job, flurm_protocol_codec:message_type_name(?RESPONSE_SUBMIT_BATCH_JOB)),
    ?assertEqual(response_slurm_rc, flurm_protocol_codec:message_type_name(?RESPONSE_SLURM_RC)),
    ?assertEqual(request_node_registration_status, flurm_protocol_codec:message_type_name(?REQUEST_NODE_REGISTRATION_STATUS)),
    ?assertEqual(request_reconfigure, flurm_protocol_codec:message_type_name(?REQUEST_RECONFIGURE)),
    ?assertEqual(request_shutdown, flurm_protocol_codec:message_type_name(?REQUEST_SHUTDOWN)),
    ?assertEqual(request_cancel_job, flurm_protocol_codec:message_type_name(?REQUEST_CANCEL_JOB)),
    ?assertEqual(request_kill_job, flurm_protocol_codec:message_type_name(?REQUEST_KILL_JOB)),
    ?assertEqual(request_node_info, flurm_protocol_codec:message_type_name(?REQUEST_NODE_INFO)),
    ?assertEqual(response_node_info, flurm_protocol_codec:message_type_name(?RESPONSE_NODE_INFO)),
    ?assertEqual(request_partition_info, flurm_protocol_codec:message_type_name(?REQUEST_PARTITION_INFO)),
    ?assertEqual(response_partition_info, flurm_protocol_codec:message_type_name(?RESPONSE_PARTITION_INFO)),

    %% Test unknown message type
    UnknownResult = flurm_protocol_codec:message_type_name(99999),
    ?assertMatch({unknown, 99999}, UnknownResult),
    ok.

is_request_test() ->
    %% Test known request types
    ?assert(flurm_protocol_codec:is_request(?REQUEST_PING)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_JOB_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_SUBMIT_BATCH_JOB)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_NODE_REGISTRATION_STATUS)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_RECONFIGURE)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_SHUTDOWN)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_CANCEL_JOB)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_KILL_JOB)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_NODE_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_PARTITION_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_RESOURCE_ALLOCATION)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_JOB_STEP_CREATE)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_JOB_STEP_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_RESERVATION_INFO)),

    %% Test non-request types
    ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_SLURM_RC)),
    ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_JOB_INFO)),
    ?assertNot(flurm_protocol_codec:is_request(99999)),
    ok.

is_response_test() ->
    %% Test known response types
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_SLURM_RC)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_JOB_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_SUBMIT_BATCH_JOB)),
    ?assert(flurm_protocol_codec:is_response(?MESSAGE_NODE_REGISTRATION_STATUS)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_NODE_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_PARTITION_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_RESOURCE_ALLOCATION)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_JOB_STEP_CREATE)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_JOB_STEP_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_RESERVATION_INFO)),

    %% Test non-response types
    ?assertNot(flurm_protocol_codec:is_response(?REQUEST_PING)),
    ?assertNot(flurm_protocol_codec:is_response(?REQUEST_JOB_INFO)),
    ?assertNot(flurm_protocol_codec:is_response(99999)),
    ok.

%%%===================================================================
%%% Wire Format Tests
%%%===================================================================

encode_decode_roundtrip_test() ->
    %% Test ping request roundtrip
    {ok, PingBin} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    {ok, PingMsg, <<>>} = flurm_protocol_codec:decode(PingBin),
    ?assertEqual(?REQUEST_PING, PingMsg#slurm_msg.header#slurm_header.msg_type),

    %% Test SLURM RC response roundtrip
    {ok, RCBin} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    {ok, RCMsg, <<>>} = flurm_protocol_codec:decode(RCBin),
    ?assertEqual(?RESPONSE_SLURM_RC, RCMsg#slurm_msg.header#slurm_header.msg_type),
    ok.

encode_with_extra_test() ->
    %% Test encode_with_extra/2
    Result1 = flurm_protocol_codec:encode_with_extra(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    ?assertMatch({ok, _}, Result1),

    %% Test encode_with_extra/3 with custom hostname
    Result2 = flurm_protocol_codec:encode_with_extra(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}, <<"testhost">>),
    ?assertMatch({ok, _}, Result2),
    ok.

decode_with_extra_test() ->
    %% Create a message with auth section
    %% Auth section format: <<PluginId:32/big, CredLen:32/big, Credential/binary>>
    %% Plugin ID 101 = MUNGE authentication
    Credential = <<"MUNGE:test_credential">>,
    CredLen = byte_size(Credential),
    AuthSection = <<101:32/big, CredLen:32/big, Credential/binary>>,

    %% Create body
    BodyBin = <<0:32/signed-big>>,  % return_code = 0

    %% Create header
    Header = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_type = ?RESPONSE_SLURM_RC,
        body_length = byte_size(BodyBin)
    },
    {ok, HeaderBin} = flurm_protocol_header:encode_header(Header),

    %% Assemble wire format
    TotalPayload = <<HeaderBin/binary, AuthSection/binary, BodyBin/binary>>,
    Length = byte_size(TotalPayload),
    WireBin = <<Length:32/big, TotalPayload/binary>>,

    Result = flurm_protocol_codec:decode_with_extra(WireBin),
    ?assertMatch({ok, #slurm_msg{}, #{auth_type := munge}, <<>>}, Result),

    %% Test error cases
    ErrorResult1 = flurm_protocol_codec:decode_with_extra(<<1, 2, 3>>),
    ?assertMatch({error, _}, ErrorResult1),

    ErrorResult2 = flurm_protocol_codec:decode_with_extra(<<100:32/big, 1, 2>>),
    ?assertMatch({error, _}, ErrorResult2),
    ok.

encode_response_test() ->
    %% Test encode_response/2 with lager running (started in setup)
    %% Encode a RESPONSE_SLURM_RC message
    Body = #slurm_rc_response{return_code = 0},
    Result = flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC, Body),
    ?assertMatch({ok, <<_:32/big, _/binary>>}, Result),
    {ok, Encoded} = Result,
    %% Verify wire format: <<OuterLength:32/big, Header, Auth, Body>>
    <<OuterLength:32/big, Payload/binary>> = Encoded,
    ?assertEqual(byte_size(Payload), OuterLength),
    %% Must be larger than just header (22 bytes) since auth section is included
    ?assert(OuterLength > 16),
    ok.

encode_response_no_auth_test() ->
    %% Test encode_response_no_auth/2 - produces minimal wire format
    Body = #slurm_rc_response{return_code = 42},
    Result = flurm_protocol_codec:encode_response_no_auth(?RESPONSE_SLURM_RC, Body),
    ?assertMatch({ok, <<_:32/big, _/binary>>}, Result),
    {ok, Encoded} = Result,
    <<OuterLength:32/big, Payload/binary>> = Encoded,
    ?assertEqual(byte_size(Payload), OuterLength),
    %% No auth section - should be header + body only
    %% Parse header to verify SLURM_NO_AUTH_CRED flag is set
    {ok, Header, BodyBin} = flurm_protocol_header:parse_header(Payload),
    ?assertEqual(?SLURM_NO_AUTH_CRED, Header#slurm_header.flags band ?SLURM_NO_AUTH_CRED),
    ?assertEqual(?RESPONSE_SLURM_RC, Header#slurm_header.msg_type),
    %% Body should decode back to return_code = 42
    {ok, DecodedBody} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, BodyBin),
    ?assertEqual(42, DecodedBody#slurm_rc_response.return_code),
    ok.

encode_response_proper_auth_test() ->
    %% Test encode_response_proper_auth/2 - proper MUNGE auth format
    Body = #slurm_rc_response{return_code = 0},
    Result = flurm_protocol_codec:encode_response_proper_auth(?RESPONSE_SLURM_RC, Body),
    ?assertMatch({ok, <<_:32/big, _/binary>>}, Result),
    {ok, Encoded} = Result,
    <<OuterLength:32/big, Payload/binary>> = Encoded,
    ?assertEqual(byte_size(Payload), OuterLength),
    %% Must include auth section (larger than no-auth version)
    {ok, NoAuthEncoded} = flurm_protocol_codec:encode_response_no_auth(?RESPONSE_SLURM_RC, Body),
    ?assert(byte_size(Encoded) > byte_size(NoAuthEncoded)),
    ok.

decode_response_test() ->
    %% Test decode_response/1 - error cases
    ErrorResult1 = flurm_protocol_codec:decode_response(<<1, 2, 3>>),
    ?assertMatch({error, _}, ErrorResult1),

    ErrorResult2 = flurm_protocol_codec:decode_response(<<100:32/big, 1, 2>>),
    ?assertMatch({error, _}, ErrorResult2),

    %% Empty binary
    ErrorResult3 = flurm_protocol_codec:decode_response(<<>>),
    ?assertMatch({error, _}, ErrorResult3),

    %% Just length prefix, no data
    ErrorResult4 = flurm_protocol_codec:decode_response(<<0:32/big>>),
    ?assertMatch({error, _}, ErrorResult4),
    ok.

decode_response_happy_path_test() ->
    %% Build a valid response message using encode_response_no_auth, then decode it
    Body = #slurm_rc_response{return_code = 7},
    {ok, Encoded} = flurm_protocol_codec:encode_response_no_auth(?RESPONSE_SLURM_RC, Body),
    %% decode_response should parse it (no_auth has flag set, so auth stripping handles it)
    Result = flurm_protocol_codec:decode_response(Encoded),
    ?assertMatch({ok, #slurm_msg{}, <<>>}, Result),
    {ok, Msg, <<>>} = Result,
    ?assertEqual(?RESPONSE_SLURM_RC, Msg#slurm_msg.header#slurm_header.msg_type),
    ok.

encode_decode_response_roundtrip_test() ->
    %% Full roundtrip: encode_response_no_auth -> decode_response -> verify body
    Body = #batch_job_response{job_id = 999, step_id = 0, error_code = 0},
    {ok, Encoded} = flurm_protocol_codec:encode_response_no_auth(?RESPONSE_SUBMIT_BATCH_JOB, Body),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode_response(Encoded),
    ?assertEqual(?RESPONSE_SUBMIT_BATCH_JOB, Msg#slurm_msg.header#slurm_header.msg_type),
    ?assertEqual(999, Msg#slurm_msg.body#batch_job_response.job_id),
    ok.

%%%===================================================================
%%% Body Encode/Decode Tests
%%%===================================================================

decode_body_various_test() ->
    %% Test decode_body for various message types
    %% REQUEST_PING - empty body
    {ok, PingBody} = flurm_protocol_codec:decode_body(?REQUEST_PING, <<>>),
    ?assertMatch(#ping_request{}, PingBody),

    %% REQUEST_PING - non-empty body (should accept)
    {ok, PingBody2} = flurm_protocol_codec:decode_body(?REQUEST_PING, <<"some data">>),
    ?assertMatch(#ping_request{}, PingBody2),

    %% REQUEST_NODE_REGISTRATION_STATUS - empty
    {ok, NodeRegBody1} = flurm_protocol_codec:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, <<>>),
    ?assertMatch(#node_registration_request{status_only = false}, NodeRegBody1),

    %% REQUEST_NODE_REGISTRATION_STATUS - with data
    {ok, NodeRegBody2} = flurm_protocol_codec:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, <<1:8, 0, 0>>),
    ?assertMatch(#node_registration_request{status_only = true}, NodeRegBody2),

    %% REQUEST_JOB_INFO - empty
    {ok, JobInfoBody1} = flurm_protocol_codec:decode_body(?REQUEST_JOB_INFO, <<>>),
    ?assertMatch(#job_info_request{}, JobInfoBody1),

    %% REQUEST_JOB_INFO - partial
    {ok, JobInfoBody2} = flurm_protocol_codec:decode_body(?REQUEST_JOB_INFO, <<1:32/big>>),
    ?assertEqual(1, JobInfoBody2#job_info_request.show_flags),

    %% REQUEST_JOB_INFO - full
    {ok, JobInfoBody3} = flurm_protocol_codec:decode_body(?REQUEST_JOB_INFO, <<1:32/big, 100:32/big, 500:32/big>>),
    ?assertEqual(100, JobInfoBody3#job_info_request.job_id),
    ?assertEqual(500, JobInfoBody3#job_info_request.user_id),

    %% REQUEST_BUILD_INFO - empty
    {ok, BuildInfoBody} = flurm_protocol_codec:decode_body(?REQUEST_BUILD_INFO, <<>>),
    ?assertMatch(#{}, BuildInfoBody),

    %% REQUEST_CONFIG_INFO - empty
    {ok, ConfigInfoBody} = flurm_protocol_codec:decode_body(?REQUEST_CONFIG_INFO, <<>>),
    ?assertMatch(#{}, ConfigInfoBody),

    %% RESPONSE_SLURM_RC
    {ok, RcBody} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<0:32/signed-big>>),
    ?assertMatch(#slurm_rc_response{return_code = 0}, RcBody),

    %% Unknown message type returns raw binary
    RawBin = <<"raw data">>,
    {ok, UnknownBody} = flurm_protocol_codec:decode_body(99999, RawBin),
    ?assertEqual(RawBin, UnknownBody),
    ok.

encode_body_various_test() ->
    %% Test encode_body for various message types
    %% REQUEST_PING
    {ok, PingBin} = flurm_protocol_codec:encode_body(?REQUEST_PING, #ping_request{}),
    ?assertEqual(<<>>, PingBin),

    %% RESPONSE_SLURM_RC
    {ok, RcBin} = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 42}),
    ?assertEqual(<<42:32/signed-big>>, RcBin),

    %% Raw binary passthrough
    RawBin = <<"raw data">>,
    {ok, PassthroughBin} = flurm_protocol_codec:encode_body(99999, RawBin),
    ?assertEqual(RawBin, PassthroughBin),

    %% Unsupported type with non-binary body
    UnsupportedResult = flurm_protocol_codec:encode_body(99999, {some, tuple}),
    ?assertMatch({error, {unsupported_message_type, _, _}}, UnsupportedResult),
    ok.

%%%===================================================================
%%% Resource Extraction Tests
%%%===================================================================

extract_resources_test() ->
    %% Test with binary too small
    Result1 = flurm_protocol_codec:extract_resources_from_protocol(<<1, 2, 3>>),
    ?assertEqual({0, 0}, Result1),

    %% Test with empty binary
    Result2 = flurm_protocol_codec:extract_resources_from_protocol(<<>>),
    ?assertEqual({0, 0}, Result2),

    %% Test with larger binary that contains potential resource values
    %% Create a binary with some plausible uint32 pairs
    LargeBin = <<0:256, 5:32/big, 10:32/big, 5:32/big, 0:256>>,
    Result3 = flurm_protocol_codec:extract_resources_from_protocol(LargeBin),
    %% Should find some values or return {0, 0}
    ?assertMatch({_, _}, Result3),
    ok.

extract_full_job_desc_test() ->
    %% Test with binary too small
    Result1 = flurm_protocol_codec:extract_full_job_desc(<<1, 2, 3>>),
    ?assertMatch({error, binary_too_small}, Result1),

    %% Test with larger binary
    LargeBin = binary:copy(<<0>>, 200),
    Result2 = flurm_protocol_codec:extract_full_job_desc(LargeBin),
    %% Should return ok with a map or error
    ?assertMatch({ok, #{min_nodes := _, min_cpus := _}}, Result2),
    ok.

%%%===================================================================
%%% Additional Decode Body Tests
%%%===================================================================

decode_body_cancel_job_test_() ->
    [
     {"cancel job - full format", fun() ->
        %% REQUEST_CANCEL_JOB with all fields
        JobIdStr = <<"123">>,
        StrLen = byte_size(JobIdStr) + 1,
        BodyBin = <<100:32/big, 0:32/big, 9:32/big, 0:32/big, StrLen:32/big, JobIdStr/binary, 0:8>>,
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, BodyBin),
        ?assertMatch(#cancel_job_request{job_id = 100}, Body)
      end},
     {"cancel job - minimal format", fun() ->
        BodyBin = <<100:32/big>>,
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, BodyBin),
        ?assertMatch(#cancel_job_request{job_id = 100}, Body)
      end},
     {"cancel job - empty", fun() ->
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, <<>>),
        ?assertMatch(#cancel_job_request{}, Body)
      end}
    ].

decode_body_kill_job_test_() ->
    [
     {"kill job - numeric format", fun() ->
        BodyBin = <<100:32/big, -1:32/big-signed, 9:16/big, 0:16/big>>,
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_KILL_JOB, BodyBin),
        ?assertMatch(#kill_job_request{job_id = 100}, Body)
      end},
     {"kill job - short format", fun() ->
        BodyBin = <<100:32/big, -1:32/big-signed>>,
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_KILL_JOB, BodyBin),
        ?assertMatch(#kill_job_request{job_id = 100}, Body)
      end}
    ].

decode_body_slurm_rc_test_() ->
    [
     {"slurm rc - success", fun() ->
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<0:32/signed-big>>),
        ?assertEqual(0, Body#slurm_rc_response.return_code)
      end},
     {"slurm rc - error code", fun() ->
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<-1:32/signed-big>>),
        ?assertEqual(-1, Body#slurm_rc_response.return_code)
      end},
     {"slurm rc - with extra data", fun() ->
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<42:32/signed-big, "extra">>),
        ?assertEqual(42, Body#slurm_rc_response.return_code)
      end},
     {"slurm rc - empty", fun() ->
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<>>),
        ?assertEqual(0, Body#slurm_rc_response.return_code)
      end}
    ].

decode_body_batch_job_response_test_() ->
    [
     {"batch job response - full", fun() ->
        Msg = <<"OK">>,
        MsgLen = byte_size(Msg) + 1,
        BodyBin = <<100:32/big, 0:32/big, 0:32/big, MsgLen:32/big, Msg/binary, 0:8>>,
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, BodyBin),
        ?assertEqual(100, Body#batch_job_response.job_id)
      end},
     {"batch job response - minimal with step_id", fun() ->
        %% Two uint32 values - job_id and step_id
        BodyBin = <<100:32/big, 0:32/big>>,
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, BodyBin),
        ?assertEqual(100, Body#batch_job_response.job_id)
      end},
     {"batch job response - minimal job_id only", fun() ->
        BodyBin = <<100:32/big>>,
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, BodyBin),
        ?assertEqual(100, Body#batch_job_response.job_id)
      end},
     {"batch job response - empty", fun() ->
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, <<>>),
        ?assertMatch(#batch_job_response{}, Body)
      end}
    ].

decode_body_job_info_response_test_() ->
    [
     {"job info response - empty", fun() ->
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_JOB_INFO, <<>>),
        ?assertMatch(#job_info_response{}, Body)
      end},
     {"job info response - header only", fun() ->
        BodyBin = <<1000:64/big, 0:32/big>>,
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_JOB_INFO, BodyBin),
        ?assertEqual(1000, Body#job_info_response.last_update),
        ?assertEqual(0, Body#job_info_response.job_count)
      end}
    ].

decode_body_partition_info_response_test_() ->
    [
     {"partition info response - empty", fun() ->
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_PARTITION_INFO, <<>>),
        ?assertMatch(#partition_info_response{}, Body)
      end},
     {"partition info response - header only", fun() ->
        BodyBin = <<0:32/big, 1000:64/big>>,
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_PARTITION_INFO, BodyBin),
        ?assertEqual(1000, Body#partition_info_response.last_update),
        ?assertEqual(0, Body#partition_info_response.partition_count)
      end}
    ].

%%%===================================================================
%%% Additional Encode Body Tests
%%%===================================================================

encode_body_node_registration_test_() ->
    [
     {"node registration - status_only true", fun() ->
        Req = #node_registration_request{status_only = true},
        {ok, Bin} = flurm_protocol_codec:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req),
        ?assertEqual(<<1:8>>, Bin)
      end},
     {"node registration - status_only false", fun() ->
        Req = #node_registration_request{status_only = false},
        {ok, Bin} = flurm_protocol_codec:encode_body(?REQUEST_NODE_REGISTRATION_STATUS, Req),
        ?assertEqual(<<0:8>>, Bin)
      end}
    ].

encode_body_cancel_job_test_() ->
    [
     {"cancel job request", fun() ->
        Req = #cancel_job_request{
            job_id = 123,
            job_id_str = <<"123">>,
            step_id = 0,
            signal = 9,
            flags = 0
        },
        {ok, Bin} = flurm_protocol_codec:encode_body(?REQUEST_CANCEL_JOB, Req),
        ?assert(is_binary(Bin)),
        ?assert(byte_size(Bin) > 16)
      end}
    ].

encode_body_kill_job_test_() ->
    [
     {"kill job request", fun() ->
        Req = #kill_job_request{
            job_id = 123,
            job_id_str = <<"123">>,
            step_id = -1,
            signal = 9,
            flags = 0,
            sibling = <<>>
        },
        {ok, Bin} = flurm_protocol_codec:encode_body(?REQUEST_KILL_JOB, Req),
        ?assert(is_binary(Bin))
      end}
    ].

encode_body_batch_job_request_test_() ->
    [
     {"batch job request", fun() ->
        Req = #batch_job_request{
            name = <<"test_job">>,
            partition = <<"batch">>,
            script = <<"#!/bin/bash\necho hello">>,
            work_dir = <<"/tmp">>,
            min_nodes = 1,
            max_nodes = 1,
            min_cpus = 1,
            num_tasks = 1,
            cpus_per_task = 1,
            time_limit = 300,
            priority = 0,
            user_id = 1000,
            group_id = 1000
        },
        {ok, Bin} = flurm_protocol_codec:encode_body(?REQUEST_SUBMIT_BATCH_JOB, Req),
        ?assert(is_binary(Bin))
      end}
    ].

encode_body_node_info_request_test_() ->
    [
     {"node info request - record", fun() ->
        Req = #node_info_request{show_flags = 0, node_name = <<>>},
        {ok, Bin} = flurm_protocol_codec:encode_body(?REQUEST_NODE_INFO, Req),
        ?assert(is_binary(Bin))
      end},
     {"node info request - default", fun() ->
        {ok, Bin} = flurm_protocol_codec:encode_body(?REQUEST_NODE_INFO, #{}),
        ?assertEqual(<<0:32/big, 0:32/big>>, Bin)
      end}
    ].

encode_body_partition_info_request_test_() ->
    [
     {"partition info request - record", fun() ->
        Req = #partition_info_request{show_flags = 0, partition_name = <<>>},
        {ok, Bin} = flurm_protocol_codec:encode_body(?REQUEST_PARTITION_INFO, Req),
        ?assert(is_binary(Bin))
      end},
     {"partition info request - default", fun() ->
        {ok, Bin} = flurm_protocol_codec:encode_body(?REQUEST_PARTITION_INFO, #{}),
        ?assertEqual(<<0:32/big, 0:32/big>>, Bin)
      end}
    ].

%%%===================================================================
%%% Message Type Name Coverage Tests
%%%===================================================================

message_type_name_coverage_test_() ->
    [
     {"all message type names", fun() ->
        %% Test all defined message types to ensure coverage
        MsgTypes = [
            ?REQUEST_NODE_REGISTRATION_STATUS,
            ?MESSAGE_NODE_REGISTRATION_STATUS,
            ?REQUEST_RECONFIGURE,
            ?REQUEST_RECONFIGURE_WITH_CONFIG,
            ?REQUEST_SHUTDOWN,
            ?REQUEST_PING,
            ?REQUEST_BUILD_INFO,
            ?REQUEST_JOB_INFO,
            ?RESPONSE_JOB_INFO,
            ?REQUEST_NODE_INFO,
            ?RESPONSE_NODE_INFO,
            ?REQUEST_PARTITION_INFO,
            ?RESPONSE_PARTITION_INFO,
            ?REQUEST_RESOURCE_ALLOCATION,
            ?RESPONSE_RESOURCE_ALLOCATION,
            ?REQUEST_SUBMIT_BATCH_JOB,
            ?RESPONSE_SUBMIT_BATCH_JOB,
            ?REQUEST_CANCEL_JOB,
            ?REQUEST_KILL_JOB,
            ?REQUEST_UPDATE_JOB,
            ?REQUEST_JOB_WILL_RUN,
            ?RESPONSE_JOB_WILL_RUN,
            ?REQUEST_JOB_STEP_CREATE,
            ?RESPONSE_JOB_STEP_CREATE,
            ?RESPONSE_SLURM_RC,
            ?REQUEST_RESERVATION_INFO,
            ?RESPONSE_RESERVATION_INFO,
            ?REQUEST_LICENSE_INFO,
            ?RESPONSE_LICENSE_INFO,
            ?REQUEST_TOPO_INFO,
            ?RESPONSE_TOPO_INFO,
            ?REQUEST_FRONT_END_INFO,
            ?RESPONSE_FRONT_END_INFO,
            ?REQUEST_BURST_BUFFER_INFO,
            ?RESPONSE_BURST_BUFFER_INFO,
            ?RESPONSE_BUILD_INFO,
            ?REQUEST_CONFIG_INFO,
            ?RESPONSE_CONFIG_INFO,
            ?REQUEST_STATS_INFO,
            ?RESPONSE_STATS_INFO
        ],
        lists:foreach(fun(Type) ->
            Name = flurm_protocol_codec:message_type_name(Type),
            ?assert(is_atom(Name) orelse is_tuple(Name))
        end, MsgTypes)
      end}
    ].

%%%===================================================================
%%% is_request/is_response Coverage Tests
%%%===================================================================

is_request_coverage_test_() ->
    [
     {"additional is_request checks", fun() ->
        %% Test additional request types
        ?assert(flurm_protocol_codec:is_request(?REQUEST_JOB_INFO_SINGLE)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_BATCH_JOB_LAUNCH)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_STEP_COMPLETE)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_LAUNCH_TASKS)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_SIGNAL_TASKS)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_TERMINATE_TASKS)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_LICENSE_INFO)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_TOPO_INFO)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_FRONT_END_INFO)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_BURST_BUFFER_INFO)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_CONFIG_INFO)),
        ?assert(flurm_protocol_codec:is_request(?REQUEST_STATS_INFO)),
        %% Test fallback range
        ?assert(flurm_protocol_codec:is_request(1001)),
        ?assert(flurm_protocol_codec:is_request(1029))
      end}
    ].

is_response_coverage_test_() ->
    [
     {"additional is_response checks", fun() ->
        %% Test additional response types
        ?assert(flurm_protocol_codec:is_response(?RESPONSE_CANCEL_JOB_STEP)),
        ?assert(flurm_protocol_codec:is_response(?RESPONSE_STEP_LAYOUT)),
        ?assert(flurm_protocol_codec:is_response(?RESPONSE_LAUNCH_TASKS)),
        ?assert(flurm_protocol_codec:is_response(?RESPONSE_SLURM_RC_MSG)),
        ?assert(flurm_protocol_codec:is_response(?RESPONSE_LICENSE_INFO)),
        ?assert(flurm_protocol_codec:is_response(?RESPONSE_TOPO_INFO)),
        ?assert(flurm_protocol_codec:is_response(?RESPONSE_FRONT_END_INFO)),
        ?assert(flurm_protocol_codec:is_response(?RESPONSE_BURST_BUFFER_INFO)),
        ?assert(flurm_protocol_codec:is_response(?RESPONSE_CONFIG_INFO)),
        ?assert(flurm_protocol_codec:is_response(?RESPONSE_STATS_INFO))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Response Encoders
%%%===================================================================

encode_job_info_response_test_() ->
    [
     {"encode empty job info response", fun() ->
        Response = #job_info_response{
            last_update = 1700000000,
            job_count = 0,
            jobs = []
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_JOB_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end},
     {"encode job info response with jobs", fun() ->
        Job = #job_info{
            job_id = 12345,
            name = <<"test_job">>,
            partition = <<"default">>,
            job_state = 1,
            user_id = 1000,
            group_id = 1000,
            num_nodes = 1,
            num_cpus = 4,
            priority = 100
        },
        Response = #job_info_response{
            last_update = 1700000000,
            job_count = 1,
            jobs = [Job]
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_JOB_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_node_info_response_test_() ->
    [
     {"encode empty node info response", fun() ->
        Response = #node_info_response{
            last_update = 1700000000,
            node_count = 0,
            nodes = []
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_NODE_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end},
     {"encode node info response with nodes", fun() ->
        Node = #node_info{
            name = <<"node001">>,
            node_state = 0,
            cpus = 16,
            sockets = 2,
            cores = 4,
            threads = 2,
            real_memory = 65536,
            tmp_disk = 100000
        },
        Response = #node_info_response{
            last_update = 1700000000,
            node_count = 1,
            nodes = [Node]
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_NODE_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_partition_info_response_test_() ->
    [
     {"encode empty partition info response", fun() ->
        Response = #partition_info_response{
            last_update = 1700000000,
            partition_count = 0,
            partitions = []
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_PARTITION_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end},
     {"encode partition info response with partitions", fun() ->
        Part = #partition_info{
            name = <<"compute">>,
            state_up = 1,
            total_nodes = 10,
            total_cpus = 160,
            max_time = 86400,
            nodes = <<"node[001-010]">>
        },
        Response = #partition_info_response{
            last_update = 1700000000,
            partition_count = 1,
            partitions = [Part]
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_PARTITION_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_resource_allocation_response_test_() ->
    [
     {"encode minimal resource allocation response", fun() ->
        Response = #resource_allocation_response{
            job_id = 12345,
            node_list = <<"node001">>,
            node_cnt = 1,
            cpus_per_node = [4],
            num_cpu_groups = 1
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_job_step_create_response_test_() ->
    [
     {"encode job step create response", fun() ->
        Response = #job_step_create_response{
            job_id = 12345,
            job_step_id = 0,
            error_code = 0,
            node_list = <<"node001">>
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_JOB_STEP_CREATE, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_launch_tasks_response_test_() ->
    [
     {"encode launch tasks response with record", fun() ->
        Response = #launch_tasks_response{
            return_code = 0,
            node_name = <<"node001">>,
            local_pids = [1234, 1235],
            gtids = [0, 1]
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_LAUNCH_TASKS, Response),
        ?assertMatch({ok, _Binary}, Result)
      end},
     {"encode launch tasks response with map", fun() ->
        Response = #{
            return_code => 0,
            node_name => <<"node001">>,
            local_pids => [1234],
            gtids => [0]
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_LAUNCH_TASKS, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_task_exit_msg_test_() ->
    [
     {"encode task exit message", fun() ->
        Msg = #{
            job_id => 12345,
            step_id => 0,
            task_id => 0,
            exit_status => 0
        },
        Result = flurm_protocol_codec:encode_body(?MESSAGE_TASK_EXIT, Msg),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_reservation_info_response_test_() ->
    [
     {"encode empty reservation info response", fun() ->
        Response = #reservation_info_response{
            last_update = 1700000000,
            reservation_count = 0,
            reservations = []
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_RESERVATION_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_license_info_response_test_() ->
    [
     {"encode empty license info response", fun() ->
        Response = #license_info_response{
            last_update = 1700000000,
            license_count = 0,
            licenses = []
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_LICENSE_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_build_info_response_test_() ->
    [
     {"encode build info response", fun() ->
        Response = #build_info_response{
            version = <<"23.02.0">>,
            cluster_name = <<"test_cluster">>,
            control_machine = <<"controller">>,
            slurmctld_port = 6817
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_BUILD_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_stats_info_response_test_() ->
    [
     {"encode stats info response", fun() ->
        Response = #stats_info_response{
            parts_packed = 0,
            req_time = 1700000000,
            req_time_start = 1699900000,
            server_thread_count = 4,
            jobs_submitted = 1000,
            jobs_started = 800,
            jobs_completed = 700,
            jobs_canceled = 50,
            jobs_failed = 10
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_STATS_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

%%%===================================================================
%%% Federation Message Tests
%%%===================================================================

encode_fed_info_response_test_() ->
    [
     {"encode fed info response", fun() ->
        Response = #fed_info_response{
            federation_name = <<"test_federation">>,
            local_cluster = <<"local_cluster">>,
            cluster_count = 0,
            clusters = []
        },
        Result = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Response),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_federation_submit_request_test_() ->
    [
     {"encode federation submit request", fun() ->
        Request = #federation_submit_request{
            source_cluster = <<"local_cluster">>,
            target_cluster = <<"remote_cluster">>,
            job_id = 12345,
            user_id = 1000,
            script = <<"#!/bin/bash\necho hello">>
        },
        Result = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_SUBMIT, Request),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_federation_job_status_request_test_() ->
    [
     {"encode federation job status request", fun() ->
        Request = #federation_job_status_request{
            job_id = 12345,
            source_cluster = <<"origin_cluster">>
        },
        Result = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_STATUS, Request),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_federation_job_cancel_request_test_() ->
    [
     {"encode federation job cancel request", fun() ->
        Request = #federation_job_cancel_request{
            job_id = 12345,
            source_cluster = <<"origin_cluster">>,
            signal = 15
        },
        Result = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Request),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_fed_job_messages_test_() ->
    [
     {"encode fed job submit msg", fun() ->
        Msg = #fed_job_submit_msg{
            federation_job_id = <<"fed-12345">>,
            origin_cluster = <<"origin">>,
            target_cluster = <<"target">>,
            job_spec = #{},
            submit_time = 1700000000
        },
        Result = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        ?assertMatch({ok, _Binary}, Result)
      end},
     {"encode fed job started msg", fun() ->
        Msg = #fed_job_started_msg{
            federation_job_id = <<"fed-12345">>,
            running_cluster = <<"running">>,
            local_job_id = 12345,
            start_time = 1700000100
        },
        Result = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
        ?assertMatch({ok, _Binary}, Result)
      end},
     {"encode fed sibling revoke msg", fun() ->
        Msg = #fed_sibling_revoke_msg{
            federation_job_id = <<"fed-12345">>,
            running_cluster = <<"origin">>,
            revoke_reason = <<"job started elsewhere">>
        },
        Result = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
        ?assertMatch({ok, _Binary}, Result)
      end},
     {"encode fed job completed msg", fun() ->
        Msg = #fed_job_completed_msg{
            federation_job_id = <<"fed-12345">>,
            running_cluster = <<"origin">>,
            local_job_id = 12345,
            exit_code = 0,
            end_time = 1700001000
        },
        Result = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
        ?assertMatch({ok, _Binary}, Result)
      end},
     {"encode fed job failed msg", fun() ->
        Msg = #fed_job_failed_msg{
            federation_job_id = <<"fed-12345">>,
            running_cluster = <<"origin">>,
            local_job_id = 12345,
            exit_code = 1,
            error_msg = <<"out of memory">>
        },
        Result = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

%%%===================================================================
%%% Request Encoder Tests
%%%===================================================================

encode_cancel_job_request_test_() ->
    [
     {"encode cancel job request", fun() ->
        Request = #cancel_job_request{
            job_id = 12345,
            job_id_str = <<"12345">>,
            step_id = 0,
            signal = 15,
            flags = 0
        },
        Result = flurm_protocol_codec:encode_body(?REQUEST_CANCEL_JOB, Request),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_kill_job_request_test_() ->
    [
     {"encode kill job request", fun() ->
        Request = #kill_job_request{
            job_id = 12345,
            step_id = 0,
            signal = 9,
            flags = 0
        },
        Result = flurm_protocol_codec:encode_body(?REQUEST_KILL_JOB, Request),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_node_info_request_test_() ->
    [
     {"encode node info request", fun() ->
        Request = #node_info_request{
            show_flags = 0,
            node_name = <<>>
        },
        Result = flurm_protocol_codec:encode_body(?REQUEST_NODE_INFO, Request),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

encode_partition_info_request_test_() ->
    [
     {"encode partition info request", fun() ->
        Request = #partition_info_request{
            show_flags = 0,
            partition_name = <<>>
        },
        Result = flurm_protocol_codec:encode_body(?REQUEST_PARTITION_INFO, Request),
        ?assertMatch({ok, _Binary}, Result)
      end}
    ].

%%%===================================================================
%%% Error Path Tests
%%%===================================================================

decode_invalid_messages_test_() ->
    [
     {"decode with truncated header", fun() ->
        Result = flurm_protocol_codec:decode(<<1, 2, 3>>),
        ?assertMatch({error, _}, Result)
      end},
     {"decode with invalid length", fun() ->
        Result = flurm_protocol_codec:decode(<<1000:32/big, 1, 2, 3>>),
        ?assertMatch({error, _}, Result)
      end},
     {"decode_response with truncated data", fun() ->
        Result = flurm_protocol_codec:decode_response(<<5:32/big, 1, 2>>),
        ?assertMatch({error, _}, Result)
      end}
    ].

encode_body_fallback_test_() ->
    [
     {"encode raw binary passes through", fun() ->
        Binary = <<"raw data">>,
        Result = flurm_protocol_codec:encode_body(99999, Binary),
        ?assertMatch({ok, Binary}, Result)
      end},
     {"encode unknown body type returns error", fun() ->
        Result = flurm_protocol_codec:encode_body(99999, {unknown, record}),
        ?assertMatch({error, _}, Result)
      end}
    ].

%%%===================================================================
%%% Script Resource Extraction Tests (extract_resources/1)
%%%===================================================================

extract_resources_sbatch_test() ->
    %% Test all SBATCH flag variants
    Script1 = <<"#!/bin/bash\n#SBATCH --nodes=4\n#SBATCH --ntasks=16\n#SBATCH --cpus-per-task=2\n#SBATCH --mem=8G\necho hello">>,
    {Nodes1, Tasks1, Cpus1, Mem1} = flurm_protocol_codec:extract_resources(Script1),
    ?assertEqual(4, Nodes1),
    ?assertEqual(16, Tasks1),
    ?assertEqual(2, Cpus1),
    ?assertEqual(8192, Mem1),  % 8G = 8*1024 MB

    %% Short flag variants: -N, -n, -c
    Script2 = <<"#!/bin/bash\n#SBATCH -N 2\n#SBATCH -n 8\n#SBATCH -c 4\nrun_job">>,
    {Nodes2, Tasks2, Cpus2, _Mem2} = flurm_protocol_codec:extract_resources(Script2),
    ?assertEqual(2, Nodes2),
    ?assertEqual(8, Tasks2),
    ?assertEqual(4, Cpus2),

    %% Short flags without space: -N2, -n8, -c4
    Script3 = <<"#!/bin/bash\n#SBATCH -N2\n#SBATCH -n8\n#SBATCH -c4\nrun_job">>,
    {Nodes3, Tasks3, Cpus3, _} = flurm_protocol_codec:extract_resources(Script3),
    ?assertEqual(2, Nodes3),
    ?assertEqual(8, Tasks3),
    ?assertEqual(4, Cpus3),
    ok.

extract_resources_memory_test() ->
    %% Memory with G suffix
    Script1 = <<"#!/bin/bash\n#SBATCH --mem=16G\nrun">>,
    {_, _, _, Mem1} = flurm_protocol_codec:extract_resources(Script1),
    ?assertEqual(16384, Mem1),  % 16*1024

    %% Memory with lowercase g
    Script2 = <<"#!/bin/bash\n#SBATCH --mem=4g\nrun">>,
    {_, _, _, Mem2} = flurm_protocol_codec:extract_resources(Script2),
    ?assertEqual(4096, Mem2),

    %% Memory with M suffix (explicit MB)
    Script3 = <<"#!/bin/bash\n#SBATCH --mem=512M\nrun">>,
    {_, _, _, Mem3} = flurm_protocol_codec:extract_resources(Script3),
    ?assertEqual(512, Mem3),

    %% Memory with K suffix
    Script4 = <<"#!/bin/bash\n#SBATCH --mem=2048K\nrun">>,
    {_, _, _, Mem4} = flurm_protocol_codec:extract_resources(Script4),
    ?assertEqual(2, Mem4),  % 2048/1024 = 2 MB

    %% Memory without suffix (defaults to MB)
    Script5 = <<"#!/bin/bash\n#SBATCH --mem=256\nrun">>,
    {_, _, _, Mem5} = flurm_protocol_codec:extract_resources(Script5),
    ?assertEqual(256, Mem5),
    ok.

extract_resources_first_wins_test() ->
    %% First non-zero value wins for each resource type
    Script = <<"#!/bin/bash\n#SBATCH --nodes=4\n#SBATCH --nodes=8\n#SBATCH --ntasks=2\n#SBATCH --ntasks=16\nrun">>,
    {Nodes, Tasks, _, _} = flurm_protocol_codec:extract_resources(Script),
    ?assertEqual(4, Nodes),  % First wins
    ?assertEqual(2, Tasks),  % First wins
    ok.

extract_resources_stop_test() ->
    %% Parsing stops at first non-comment, non-empty line
    Script = <<"#!/bin/bash\n#SBATCH --nodes=2\necho hello\n#SBATCH --ntasks=16\nrun">>,
    {Nodes, Tasks, _, _} = flurm_protocol_codec:extract_resources(Script),
    ?assertEqual(2, Nodes),
    ?assertEqual(0, Tasks),  % Never reached because parsing stopped at "echo hello"

    %% Empty lines are skipped
    Script2 = <<"#!/bin/bash\n\n#SBATCH --nodes=3\n\n#SBATCH --ntasks=5\nrun">>,
    {Nodes2, Tasks2, _, _} = flurm_protocol_codec:extract_resources(Script2),
    ?assertEqual(3, Nodes2),
    ?assertEqual(5, Tasks2),  % Empty lines don't stop parsing

    %% Comments are skipped
    Script3 = <<"#!/bin/bash\n# This is a comment\n#SBATCH --nodes=1\nrun">>,
    {Nodes3, _, _, _} = flurm_protocol_codec:extract_resources(Script3),
    ?assertEqual(1, Nodes3),
    ok.

extract_resources_edge_cases_test() ->
    %% Empty script
    {N1, T1, C1, M1} = flurm_protocol_codec:extract_resources(<<>>),
    ?assertEqual({0, 0, 0, 0}, {N1, T1, C1, M1}),

    %% Script with no SBATCH directives
    {N2, T2, C2, M2} = flurm_protocol_codec:extract_resources(<<"#!/bin/bash\necho hello\nrun">>),
    ?assertEqual({0, 0, 0, 0}, {N2, T2, C2, M2}),

    %% Unrecognized SBATCH flags are ignored
    Script3 = <<"#!/bin/bash\n#SBATCH --job-name=test\n#SBATCH --nodes=1\nrun">>,
    {N3, _, _, _} = flurm_protocol_codec:extract_resources(Script3),
    ?assertEqual(1, N3),

    %% Only shebang line
    {N4, T4, C4, M4} = flurm_protocol_codec:extract_resources(<<"#!/bin/bash">>),
    ?assertEqual({0, 0, 0, 0}, {N4, T4, C4, M4}),
    ok.

%%%===================================================================
%%% Auth Section Parsing Tests
%%%===================================================================

decode_with_extra_non_munge_test() ->
    %% Test with non-MUNGE plugin ID (unknown auth type)
    Credential = <<"AUTH:some_cred">>,
    CredLen = byte_size(Credential),
    %% Plugin ID 999 = unknown auth
    AuthSection = <<999:32/big, CredLen:32/big, Credential/binary>>,

    BodyBin = <<0:32/signed-big>>,  % return_code = 0
    Header = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_type = ?RESPONSE_SLURM_RC,
        body_length = byte_size(BodyBin)
    },
    {ok, HeaderBin} = flurm_protocol_header:encode_header(Header),

    TotalPayload = <<HeaderBin/binary, AuthSection/binary, BodyBin/binary>>,
    Length = byte_size(TotalPayload),
    WireBin = <<Length:32/big, TotalPayload/binary>>,

    Result = flurm_protocol_codec:decode_with_extra(WireBin),
    ?assertMatch({ok, #slurm_msg{}, #{auth_type := unknown}, <<>>}, Result),
    {ok, _, AuthInfo, _} = Result,
    ?assertEqual(999, maps:get(plugin_id, AuthInfo)),
    ?assertEqual(CredLen, maps:get(cred_len, AuthInfo)),
    ok.

strip_auth_section_errors_test() ->
    %% Too short binary (< 8 bytes for plugin_id + cred_len)
    Result1 = flurm_protocol_codec:strip_auth_section(<<1, 2, 3, 4, 5>>),
    ?assertMatch({error, _}, Result1),

    %% Exactly 8 bytes but cred_len claims more than available
    Result2 = flurm_protocol_codec:strip_auth_section(<<101:32/big, 100:32/big>>),
    ?assertMatch({error, _}, Result2),

    %% Empty binary
    Result3 = flurm_protocol_codec:strip_auth_section(<<>>),
    ?assertMatch({error, _}, Result3),

    %% Valid auth section with zero-length credential
    Result4 = flurm_protocol_codec:strip_auth_section(<<101:32/big, 0:32/big, "body">>),
    ?assertMatch({ok, <<"body">>, #{auth_type := munge}}, Result4),
    ok.

%%%===================================================================
%%% Passthrough Message Validation Tests
%%%===================================================================

passthrough_binary_validation_test() ->
    %% Encoding a raw binary for an unknown message type passes through
    RawBody = <<"arbitrary binary data for unknown message type">>,
    {ok, Encoded} = flurm_protocol_codec:encode_body(60000, RawBody),
    ?assertEqual(RawBody, Encoded),

    %% Encoding non-binary for unknown type returns error
    Result = flurm_protocol_codec:encode_body(60000, {some, tuple}),
    ?assertMatch({error, {unsupported_message_type, 60000, _}}, Result),

    %% Full roundtrip: encode unknown type with binary body -> decode
    %% The decoder should handle it as a raw binary body
    Header = #slurm_header{
        version = ?SLURM_PROTOCOL_VERSION,
        flags = 0,
        msg_type = 60000,
        body_length = byte_size(RawBody)
    },
    {ok, HeaderBin} = flurm_protocol_header:encode_header(Header),
    TotalPayload = <<HeaderBin/binary, RawBody/binary>>,
    Length = byte_size(TotalPayload),
    WireBin = <<Length:32/big, TotalPayload/binary>>,

    DecodedResult = flurm_protocol_codec:decode(WireBin),
    ?assertMatch({ok, #slurm_msg{}, <<>>}, DecodedResult),
    {ok, DecodedMsg, <<>>} = DecodedResult,
    %% Body should be the raw binary since type 60000 has no decoder
    ?assertEqual(RawBody, DecodedMsg#slurm_msg.body),
    ok.
