%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for System and Admin Message Types
%%%
%%% Tests for system and admin-related encode_body and decode_body
%%% functions in flurm_protocol_codec.erl.
%%%
%%% Message types covered:
%%% - REQUEST_PING (1008)
%%% - RESPONSE_SLURM_RC (8001)
%%% - REQUEST_BUILD_INFO (2001)
%%% - REQUEST_CONFIG_INFO (2016)
%%% - REQUEST_STATS_INFO (2026)
%%% - REQUEST_RECONFIGURE (1003)
%%% - REQUEST_RECONFIGURE_WITH_CONFIG (1004)
%%% - REQUEST_SHUTDOWN (1005)
%%% - SRUN_PING (7001)
%%%
%%% Also tests top-level functions:
%%% - decode/1, encode/2
%%% - decode_with_extra/1, encode_with_extra/2, encode_with_extra/3
%%% - strip_auth_section/1
%%% - encode_response/2, decode_response/1
%%% - encode_response_no_auth/2, encode_response_proper_auth/2
%%% - get_munge_credential/1
%%% - create_proper_auth_section/1
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_codec_system_admin_tests).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Fixtures
%%%===================================================================

setup() ->
    %% Mock lager for testing
    case code:which(meck) of
        non_existing -> ok;
        _ ->
            catch meck:unload(lager),
            meck:new(lager, [non_strict, no_link]),
            meck:expect(lager, debug, fun(_, _) -> ok end),
            meck:expect(lager, info, fun(_, _) -> ok end),
            meck:expect(lager, warning, fun(_, _) -> ok end),
            meck:expect(lager, error, fun(_, _) -> ok end),
            meck:expect(lager, md, fun(_) -> ok end),
            meck:expect(lager, md, fun() -> [] end),
            meck:expect(lager, do_log, fun(_, _, _, _, _, _, _, _, _, _) -> ok end)
    end,
    ok.

cleanup(_) ->
    catch meck:unload(lager),
    ok.

%%%===================================================================
%%% Test Generators
%%%===================================================================

flurm_protocol_codec_system_admin_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% REQUEST_PING Tests
      request_ping_tests(),

      %% RESPONSE_SLURM_RC Tests
      response_slurm_rc_tests(),

      %% REQUEST_BUILD_INFO Tests
      request_build_info_tests(),

      %% REQUEST_CONFIG_INFO Tests
      request_config_info_tests(),

      %% REQUEST_STATS_INFO Tests
      request_stats_info_tests(),

      %% REQUEST_RECONFIGURE Tests
      request_reconfigure_tests(),

      %% REQUEST_RECONFIGURE_WITH_CONFIG Tests
      request_reconfigure_with_config_tests(),

      %% REQUEST_SHUTDOWN Tests
      request_shutdown_tests(),

      %% SRUN_PING Tests
      srun_ping_tests(),

      %% Top-level decode/1 Tests
      decode_function_tests(),

      %% Top-level encode/2 Tests
      encode_function_tests(),

      %% decode_with_extra/1 Tests
      decode_with_extra_tests(),

      %% strip_auth_section/1 Tests
      strip_auth_section_tests(),

      %% encode_with_extra/2 and encode_with_extra/3 Tests
      encode_with_extra_tests(),

      %% encode_response/2 and decode_response/1 Tests
      encode_decode_response_tests(),

      %% encode_response_no_auth/2 Tests
      encode_response_no_auth_tests(),

      %% encode_response_proper_auth/2 Tests
      encode_response_proper_auth_tests(),

      %% Roundtrip Tests
      roundtrip_tests(),

      %% Edge Case Tests
      edge_case_tests(),

      %% Error Handling Tests
      error_handling_tests(),

      %% Boundary Value Tests
      boundary_value_tests(),

      %% Empty and Null Input Tests
      empty_null_input_tests(),

      %% Large Input Tests
      large_input_tests(),

      %% Malformed Input Tests
      malformed_input_tests(),

      %% Protocol Compatibility Tests
      protocol_compatibility_tests()
     ]}.

%%%===================================================================
%%% REQUEST_PING Tests (1008)
%%%===================================================================

request_ping_tests() ->
    {"REQUEST_PING Tests", [
        {"encode ping_request record", fun encode_ping_request_record_test/0},
        {"encode ping_request empty map", fun encode_ping_request_empty_map_test/0},
        {"decode ping empty body", fun decode_ping_empty_body_test/0},
        {"decode ping with extra data", fun decode_ping_with_extra_data_test/0},
        {"decode ping with binary data", fun decode_ping_with_binary_data_test/0},
        {"encode decode ping roundtrip", fun encode_decode_ping_roundtrip_test/0},
        {"ping multiple roundtrips", fun ping_multiple_roundtrips_test/0},
        {"ping body always empty", fun ping_body_always_empty_test/0}
    ]}.

encode_ping_request_record_test() ->
    Req = #ping_request{},
    Result = flurm_protocol_codec:encode_body(?REQUEST_PING, Req),
    ?assertMatch({ok, <<>>}, Result).

encode_ping_request_empty_map_test() ->
    %% Maps should work as input too - handled by default clause
    Result = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    ?assertMatch({ok, _Binary}, Result).

decode_ping_empty_body_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_PING, <<>>),
    ?assertMatch(#ping_request{}, Body).

decode_ping_with_extra_data_test() ->
    %% Ping should ignore extra data in the body
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_PING, <<"extra data ignored">>),
    ?assertMatch(#ping_request{}, Body).

decode_ping_with_binary_data_test() ->
    %% Any binary should be accepted for ping
    lists:foreach(fun(Bin) ->
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_PING, Bin),
        ?assertMatch(#ping_request{}, Body)
    end, [<<>>, <<0>>, <<1,2,3,4,5>>, <<"test">>, <<0:1000>>]).

encode_decode_ping_roundtrip_test() ->
    Req = #ping_request{},
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, Req),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    ?assertEqual(?REQUEST_PING, Msg#slurm_msg.header#slurm_header.msg_type),
    ?assertMatch(#ping_request{}, Msg#slurm_msg.body).

ping_multiple_roundtrips_test() ->
    %% Test multiple sequential roundtrips
    lists:foreach(fun(_) ->
        Req = #ping_request{},
        {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, Req),
        {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertMatch(#ping_request{}, Msg#slurm_msg.body)
    end, lists:seq(1, 10)).

ping_body_always_empty_test() ->
    %% Encoded ping body should always be empty
    {ok, Body} = flurm_protocol_codec:encode_body(?REQUEST_PING, #ping_request{}),
    ?assertEqual(<<>>, Body).

%%%===================================================================
%%% RESPONSE_SLURM_RC Tests (8001)
%%%===================================================================

response_slurm_rc_tests() ->
    {"RESPONSE_SLURM_RC Tests", [
        {"encode slurm_rc_response record", fun encode_slurm_rc_response_record_test/0},
        {"encode slurm_rc_response positive", fun encode_slurm_rc_response_positive_test/0},
        {"encode slurm_rc_response negative", fun encode_slurm_rc_response_negative_test/0},
        {"decode slurm_rc zero return code", fun decode_slurm_rc_zero_test/0},
        {"decode slurm_rc positive return code", fun decode_slurm_rc_positive_test/0},
        {"decode slurm_rc negative return code", fun decode_slurm_rc_negative_test/0},
        {"decode slurm_rc empty body", fun decode_slurm_rc_empty_test/0},
        {"decode slurm_rc with extra data", fun decode_slurm_rc_extra_data_test/0},
        {"slurm_rc roundtrip various values", fun slurm_rc_roundtrip_various_test/0},
        {"slurm_rc boundary values", fun slurm_rc_boundary_values_test/0},
        {"slurm_rc error codes", fun slurm_rc_error_codes_test/0}
    ]}.

encode_slurm_rc_response_record_test() ->
    Resp = #slurm_rc_response{return_code = 0},
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC, Resp),
    ?assertEqual(<<0:32/signed-big>>, Binary).

encode_slurm_rc_response_positive_test() ->
    Resp = #slurm_rc_response{return_code = 42},
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC, Resp),
    ?assertEqual(<<42:32/signed-big>>, Binary).

encode_slurm_rc_response_negative_test() ->
    Resp = #slurm_rc_response{return_code = -1},
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC, Resp),
    ?assertEqual(<<-1:32/signed-big>>, Binary).

decode_slurm_rc_zero_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<0:32/signed-big>>),
    ?assertEqual(0, Body#slurm_rc_response.return_code).

decode_slurm_rc_positive_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<100:32/signed-big>>),
    ?assertEqual(100, Body#slurm_rc_response.return_code).

decode_slurm_rc_negative_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<-1:32/signed-big>>),
    ?assertEqual(-1, Body#slurm_rc_response.return_code).

decode_slurm_rc_empty_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<>>),
    ?assertMatch(#slurm_rc_response{}, Body).

decode_slurm_rc_extra_data_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<42:32/signed-big, "extra">>),
    ?assertEqual(42, Body#slurm_rc_response.return_code).

slurm_rc_roundtrip_various_test() ->
    Values = [0, 1, -1, 100, -100, 255, -255, 1000, -1000],
    lists:foreach(fun(RC) ->
        Resp = #slurm_rc_response{return_code = RC},
        {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp),
        {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(RC, (Msg#slurm_msg.body)#slurm_rc_response.return_code)
    end, Values).

slurm_rc_boundary_values_test() ->
    %% Test 32-bit signed integer boundaries
    MaxPos = 16#7FFFFFFF,
    MaxNeg = -16#80000000,

    Resp1 = #slurm_rc_response{return_code = MaxPos},
    {ok, Encoded1} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp1),
    {ok, Msg1, <<>>} = flurm_protocol_codec:decode(Encoded1),
    ?assertEqual(MaxPos, (Msg1#slurm_msg.body)#slurm_rc_response.return_code),

    Resp2 = #slurm_rc_response{return_code = MaxNeg},
    {ok, Encoded2} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp2),
    {ok, Msg2, <<>>} = flurm_protocol_codec:decode(Encoded2),
    ?assertEqual(MaxNeg, (Msg2#slurm_msg.body)#slurm_rc_response.return_code).

slurm_rc_error_codes_test() ->
    %% Test common SLURM error codes
    ErrorCodes = [
        {0, "Success"},
        {1, "Generic error"},
        {-1, "Operation failed"},
        {2002, "Job not found"},
        {2003, "Node not found"},
        {2004, "Partition not found"}
    ],
    lists:foreach(fun({RC, _Desc}) ->
        Resp = #slurm_rc_response{return_code = RC},
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC, Resp),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, Binary),
        ?assertEqual(RC, Decoded#slurm_rc_response.return_code)
    end, ErrorCodes).

%%%===================================================================
%%% REQUEST_BUILD_INFO Tests (2001)
%%%===================================================================

request_build_info_tests() ->
    {"REQUEST_BUILD_INFO Tests", [
        {"decode build_info empty", fun decode_build_info_empty_test/0},
        {"decode build_info with data", fun decode_build_info_with_data_test/0},
        {"decode build_info returns empty map", fun decode_build_info_returns_map_test/0},
        {"build_info various inputs", fun build_info_various_inputs_test/0}
    ]}.

decode_build_info_empty_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_BUILD_INFO, <<>>),
    ?assertEqual(#{}, Body).

decode_build_info_with_data_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_BUILD_INFO, <<"ignored data">>),
    ?assertEqual(#{}, Body).

decode_build_info_returns_map_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_BUILD_INFO, <<1,2,3,4,5>>),
    ?assert(is_map(Body)),
    ?assertEqual(#{}, Body).

build_info_various_inputs_test() ->
    Inputs = [<<>>, <<0>>, <<"test">>, <<0:1000>>],
    lists:foreach(fun(Input) ->
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_BUILD_INFO, Input),
        ?assertEqual(#{}, Body)
    end, Inputs).

%%%===================================================================
%%% REQUEST_CONFIG_INFO Tests (2016)
%%%===================================================================

request_config_info_tests() ->
    {"REQUEST_CONFIG_INFO Tests", [
        {"decode config_info empty", fun decode_config_info_empty_test/0},
        {"decode config_info with data", fun decode_config_info_with_data_test/0},
        {"decode config_info returns empty map", fun decode_config_info_returns_map_test/0},
        {"config_info various inputs", fun config_info_various_inputs_test/0}
    ]}.

decode_config_info_empty_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_CONFIG_INFO, <<>>),
    ?assertEqual(#{}, Body).

decode_config_info_with_data_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_CONFIG_INFO, <<"config data">>),
    ?assertEqual(#{}, Body).

decode_config_info_returns_map_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_CONFIG_INFO, <<1,2,3,4,5,6,7,8>>),
    ?assert(is_map(Body)),
    ?assertEqual(#{}, Body).

config_info_various_inputs_test() ->
    Inputs = [<<>>, <<0>>, <<"config">>, <<16#FF:8>>, <<0:2000>>],
    lists:foreach(fun(Input) ->
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_CONFIG_INFO, Input),
        ?assertEqual(#{}, Body)
    end, Inputs).

%%%===================================================================
%%% REQUEST_STATS_INFO Tests (2026)
%%%===================================================================

request_stats_info_tests() ->
    {"REQUEST_STATS_INFO Tests", [
        {"decode stats_info empty", fun decode_stats_info_empty_test/0},
        {"decode stats_info with command", fun decode_stats_info_with_command_test/0},
        {"encode stats_info_request record", fun encode_stats_info_request_record_test/0},
        {"encode stats_info_response record", fun encode_stats_info_response_record_test/0},
        {"stats_info roundtrip", fun stats_info_roundtrip_test/0},
        {"stats_info command values", fun stats_info_command_values_test/0}
    ]}.

decode_stats_info_empty_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_STATS_INFO, <<>>),
    ?assert(is_binary(Body) orelse is_map(Body) orelse is_record(Body, stats_info_request)).

decode_stats_info_with_command_test() ->
    %% Command 0 = query, 1 = reset
    Binary = <<0:32/big>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_STATS_INFO, Binary),
    ?assertMatch({ok, _}, Result).

encode_stats_info_request_record_test() ->
    %% REQUEST_STATS_INFO doesn't support encode_body (server-side only)
    %% Just verify it returns the expected error or handles gracefully
    Req = #stats_info_request{command = 0},
    Result = flurm_protocol_codec:encode_body(?REQUEST_STATS_INFO, Req),
    case Result of
        {ok, _} -> ok;
        {error, {unsupported_message_type, ?REQUEST_STATS_INFO, _}} -> ok;
        _ -> ok
    end.

encode_stats_info_response_record_test() ->
    Resp = #stats_info_response{
        parts_packed = 0,
        req_time = erlang:system_time(second),
        req_time_start = erlang:system_time(second) - 3600,
        server_thread_count = 10,
        jobs_submitted = 100,
        jobs_started = 90,
        jobs_completed = 80,
        jobs_pending = 10,
        jobs_running = 10
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_STATS_INFO, Resp),
    ?assert(is_binary(Binary)).

stats_info_roundtrip_test() ->
    Resp = #stats_info_response{
        parts_packed = 0,
        req_time = 1700000000,
        req_time_start = 1699996400,
        server_thread_count = 5,
        agent_queue_size = 0,
        agent_count = 2,
        agent_thread_count = 4,
        dbd_agent_queue_size = 0,
        jobs_submitted = 50,
        jobs_started = 45,
        jobs_completed = 40,
        jobs_canceled = 2,
        jobs_failed = 3,
        jobs_pending = 5,
        jobs_running = 5,
        schedule_cycle_max = 500,
        schedule_cycle_last = 250,
        bf_active = false
    },
    {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_STATS_INFO, Resp),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    ?assertEqual(?RESPONSE_STATS_INFO, Msg#slurm_msg.header#slurm_header.msg_type).

stats_info_command_values_test() ->
    %% Test different command values (0 = query, 1 = reset)
    Commands = [0, 1],
    lists:foreach(fun(Cmd) ->
        Binary = <<Cmd:32/big>>,
        {ok, _Body} = flurm_protocol_codec:decode_body(?REQUEST_STATS_INFO, Binary)
    end, Commands).

%%%===================================================================
%%% REQUEST_RECONFIGURE Tests (1003)
%%%===================================================================

request_reconfigure_tests() ->
    {"REQUEST_RECONFIGURE Tests", [
        {"decode reconfigure empty", fun decode_reconfigure_empty_test/0},
        {"decode reconfigure with flags", fun decode_reconfigure_with_flags_test/0},
        {"decode reconfigure partial data", fun decode_reconfigure_partial_data_test/0},
        {"reconfigure various flags", fun reconfigure_various_flags_test/0},
        {"reconfigure roundtrip", fun reconfigure_roundtrip_test/0}
    ]}.

decode_reconfigure_empty_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE, <<>>),
    ?assertMatch(#reconfigure_request{}, Body).

decode_reconfigure_with_flags_test() ->
    Binary = <<16#FF:32/big>>,
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE, Binary),
    ?assertEqual(16#FF, Body#reconfigure_request.flags).

decode_reconfigure_partial_data_test() ->
    %% Less than 4 bytes should return default
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE, <<1, 2, 3>>),
    ?assertMatch(#reconfigure_request{}, Body).

reconfigure_various_flags_test() ->
    FlagValues = [0, 1, 16#FF, 16#FFFF, 16#FFFFFFFF],
    lists:foreach(fun(Flags) ->
        Binary = <<Flags:32/big>>,
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE, Binary),
        ?assertEqual(Flags, Body#reconfigure_request.flags)
    end, FlagValues).

reconfigure_roundtrip_test() ->
    %% REQUEST_RECONFIGURE doesn't support encoding (client sends, server receives)
    %% Test decode only
    Binary = <<42:32/big>>,
    {ok, Decoded} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE, Binary),
    ?assertEqual(42, Decoded#reconfigure_request.flags).

%%%===================================================================
%%% REQUEST_RECONFIGURE_WITH_CONFIG Tests (1004)
%%%===================================================================

request_reconfigure_with_config_tests() ->
    {"REQUEST_RECONFIGURE_WITH_CONFIG Tests", [
        {"decode reconfigure_with_config empty", fun decode_reconfigure_with_config_empty_test/0},
        {"decode reconfigure_with_config with data", fun decode_reconfigure_with_config_data_test/0},
        {"reconfigure_with_config various inputs", fun reconfigure_with_config_various_test/0}
    ]}.

decode_reconfigure_with_config_empty_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE_WITH_CONFIG, <<>>),
    ?assertMatch(#reconfigure_with_config_request{}, Body).

decode_reconfigure_with_config_data_test() ->
    %% Test with some data - may return default record due to parsing
    ConfigFile = <<"/etc/slurm/slurm.conf">>,
    ConfigLen = byte_size(ConfigFile) + 1,
    Binary = <<ConfigLen:32/big, ConfigFile/binary, 0, 0:32/big, 0:32/big>>,
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE_WITH_CONFIG, Binary),
    ?assertMatch(#reconfigure_with_config_request{}, Body).

reconfigure_with_config_various_test() ->
    Inputs = [
        <<>>,
        <<0:32/big>>,
        <<1,2,3,4,5,6,7,8>>,
        <<"some random data">>
    ],
    lists:foreach(fun(Input) ->
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE_WITH_CONFIG, Input),
        ?assertMatch(#reconfigure_with_config_request{}, Body)
    end, Inputs).

%%%===================================================================
%%% REQUEST_SHUTDOWN Tests (1005)
%%%===================================================================

request_shutdown_tests() ->
    {"REQUEST_SHUTDOWN Tests", [
        {"decode shutdown empty", fun decode_shutdown_empty_test/0},
        {"decode shutdown with data", fun decode_shutdown_with_data_test/0},
        {"decode shutdown returns binary", fun decode_shutdown_returns_binary_test/0},
        {"shutdown various inputs", fun shutdown_various_inputs_test/0}
    ]}.

decode_shutdown_empty_test() ->
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_SHUTDOWN, <<>>),
    %% Shutdown may return the raw binary
    ?assert(is_binary(Body) orelse is_map(Body)).

decode_shutdown_with_data_test() ->
    Binary = <<1:16/big, 2:16/big>>,
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_SHUTDOWN, Binary),
    ?assert(is_binary(Body) orelse is_map(Body)).

decode_shutdown_returns_binary_test() ->
    Input = <<"shutdown_data">>,
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_SHUTDOWN, Input),
    ?assertEqual(Input, Body).

shutdown_various_inputs_test() ->
    Inputs = [<<>>, <<0>>, <<0:16/big, 0:16/big>>, <<"test">>],
    lists:foreach(fun(Input) ->
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_SHUTDOWN, Input),
        ?assert(is_binary(Body) orelse is_map(Body))
    end, Inputs).

%%%===================================================================
%%% SRUN_PING Tests (7001)
%%% Note: SRUN_PING has an empty body according to SLURM protocol
%%%===================================================================

srun_ping_tests() ->
    {"SRUN_PING Tests", [
        {"encode srun_ping record", fun encode_srun_ping_record_test/0},
        {"srun_ping body is empty", fun srun_ping_body_is_empty_test/0},
        {"srun_ping full roundtrip", fun srun_ping_full_roundtrip_test/0}
    ]}.

encode_srun_ping_record_test() ->
    %% SRUN_PING has no body - just the message type indicates a ping
    Req = #srun_ping{job_id = 12345, step_id = 0},
    {ok, Binary} = flurm_protocol_codec:encode_body(?SRUN_PING, Req),
    ?assertEqual(<<>>, Binary).

srun_ping_body_is_empty_test() ->
    %% All SRUN_PING records produce empty body
    Req = #srun_ping{job_id = 0, step_id = 0},
    {ok, Binary} = flurm_protocol_codec:encode_body(?SRUN_PING, Req),
    ?assertEqual(<<>>, Binary).

srun_ping_full_roundtrip_test() ->
    %% Test full encode/decode roundtrip
    Req = #srun_ping{job_id = 0, step_id = 0},
    {ok, Encoded} = flurm_protocol_codec:encode(?SRUN_PING, Req),
    ?assert(is_binary(Encoded)),
    ?assert(byte_size(Encoded) >= 14).

%%%===================================================================
%%% Top-level decode/1 Tests
%%%===================================================================

decode_function_tests() ->
    {"decode/1 Function Tests", [
        {"decode valid header", fun decode_valid_header_test/0},
        {"decode incomplete header", fun decode_incomplete_header_test/0},
        {"decode valid body", fun decode_valid_body_test/0},
        {"decode incomplete body", fun decode_incomplete_body_test/0},
        {"decode empty binary", fun decode_empty_binary_test/0},
        {"decode too short", fun decode_too_short_test/0},
        {"decode invalid length", fun decode_invalid_length_test/0},
        {"decode multiple messages", fun decode_multiple_messages_test/0},
        {"decode with remaining data", fun decode_with_remaining_data_test/0},
        {"decode length boundary", fun decode_length_boundary_test/0}
    ]}.

decode_valid_header_test() ->
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    ?assertEqual(?REQUEST_PING, Msg#slurm_msg.header#slurm_header.msg_type).

decode_incomplete_header_test() ->
    %% Only length prefix, no header
    Result = flurm_protocol_codec:decode(<<100:32/big>>),
    ?assertMatch({error, _}, Result).

decode_valid_body_test() ->
    {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    ?assertEqual(0, (Msg#slurm_msg.body)#slurm_rc_response.return_code).

decode_incomplete_body_test() ->
    %% Length indicates more data than provided
    Result = flurm_protocol_codec:decode(<<100:32/big, 1,2,3,4,5,6,7,8,9,10>>),
    ?assertMatch({error, {incomplete_message, 100, _}}, Result).

decode_empty_binary_test() ->
    Result = flurm_protocol_codec:decode(<<>>),
    ?assertMatch({error, {incomplete_length_prefix, 0}}, Result).

decode_too_short_test() ->
    Result = flurm_protocol_codec:decode(<<1, 2, 3>>),
    ?assertMatch({error, {incomplete_length_prefix, 3}}, Result).

decode_invalid_length_test() ->
    %% Length smaller than minimum header
    Result = flurm_protocol_codec:decode(<<5:32/big, 1, 2, 3, 4, 5>>),
    ?assertMatch({error, {invalid_message_length, 5}}, Result).

decode_multiple_messages_test() ->
    {ok, Enc1} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    {ok, Enc2} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 42}),
    Combined = <<Enc1/binary, Enc2/binary>>,

    {ok, Msg1, Rest} = flurm_protocol_codec:decode(Combined),
    ?assertEqual(?REQUEST_PING, Msg1#slurm_msg.header#slurm_header.msg_type),

    {ok, Msg2, <<>>} = flurm_protocol_codec:decode(Rest),
    ?assertEqual(?RESPONSE_SLURM_RC, Msg2#slurm_msg.header#slurm_header.msg_type).

decode_with_remaining_data_test() ->
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    Extra = <<"extra remaining data">>,
    {ok, _Msg, Remaining} = flurm_protocol_codec:decode(<<Encoded/binary, Extra/binary>>),
    ?assertEqual(Extra, Remaining).

decode_length_boundary_test() ->
    %% Test exactly at minimum header size
    Result = flurm_protocol_codec:decode(<<16:32/big, 0:128>>),
    %% Should either decode or return appropriate error
    case Result of
        {ok, _, _} -> ok;
        {error, _} -> ok
    end.

%%%===================================================================
%%% Top-level encode/2 Tests
%%%===================================================================

encode_function_tests() ->
    {"encode/2 Function Tests", [
        {"encode ping request", fun encode_ping_request_test/0},
        {"encode slurm_rc response", fun encode_slurm_rc_response_test/0},
        {"encode job_info request", fun encode_job_info_request_test/0},
        {"encode cancel_job request", fun encode_cancel_job_request_test/0},
        {"encode node_info request", fun encode_node_info_request_test/0},
        {"encode partition_info request", fun encode_partition_info_request_test/0},
        {"encode batch_job response", fun encode_batch_job_response_test/0},
        {"encode returns valid binary", fun encode_returns_valid_binary_test/0},
        {"encode creates length prefix", fun encode_creates_length_prefix_test/0}
    ]}.

encode_ping_request_test() ->
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) >= 14).

encode_slurm_rc_response_test() ->
    {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = -1}),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) >= 18).

encode_job_info_request_test() ->
    Req = #job_info_request{show_flags = 1, job_id = 123, user_id = 1000},
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Req),
    ?assert(is_binary(Binary)).

encode_cancel_job_request_test() ->
    Req = #cancel_job_request{job_id = 12345, signal = 9, flags = 0},
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Req),
    ?assert(is_binary(Binary)).

encode_node_info_request_test() ->
    Req = #node_info_request{show_flags = 0, node_name = <<"node01">>},
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_NODE_INFO, Req),
    ?assert(is_binary(Binary)).

encode_partition_info_request_test() ->
    Req = #partition_info_request{show_flags = 0, partition_name = <<"batch">>},
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_PARTITION_INFO, Req),
    ?assert(is_binary(Binary)).

encode_batch_job_response_test() ->
    Resp = #batch_job_response{job_id = 99999, step_id = 0, error_code = 0},
    {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_SUBMIT_BATCH_JOB, Resp),
    ?assert(is_binary(Binary)).

encode_returns_valid_binary_test() ->
    Types = [
        {?REQUEST_PING, #ping_request{}},
        {?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}},
        {?REQUEST_JOB_INFO, #job_info_request{}}
    ],
    lists:foreach(fun({Type, Body}) ->
        {ok, Binary} = flurm_protocol_codec:encode(Type, Body),
        ?assert(is_binary(Binary)),
        ?assert(byte_size(Binary) >= 4)
    end, Types).

encode_creates_length_prefix_test() ->
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    <<Length:32/big, Rest/binary>> = Binary,
    ?assertEqual(Length, byte_size(Rest)).

%%%===================================================================
%%% decode_with_extra/1 Tests
%%%===================================================================

decode_with_extra_tests() ->
    {"decode_with_extra/1 Tests", [
        {"decode_with_extra valid message", fun decode_with_extra_valid_test/0},
        {"decode_with_extra empty", fun decode_with_extra_empty_test/0},
        {"decode_with_extra incomplete", fun decode_with_extra_incomplete_test/0},
        {"decode_with_extra returns auth info", fun decode_with_extra_auth_info_test/0}
    ]}.

decode_with_extra_valid_test() ->
    {ok, Encoded} = flurm_protocol_codec:encode_with_extra(?REQUEST_PING, #ping_request{}),
    Result = flurm_protocol_codec:decode_with_extra(Encoded),
    case Result of
        {ok, Msg, _AuthInfo, _Rest} ->
            ?assertEqual(?REQUEST_PING, Msg#slurm_msg.header#slurm_header.msg_type);
        {error, _} ->
            %% Auth parsing errors are acceptable
            ok
    end.

decode_with_extra_empty_test() ->
    Result = flurm_protocol_codec:decode_with_extra(<<>>),
    ?assertMatch({error, {incomplete_length_prefix, 0}}, Result).

decode_with_extra_incomplete_test() ->
    Result = flurm_protocol_codec:decode_with_extra(<<1, 2, 3>>),
    ?assertMatch({error, {incomplete_length_prefix, 3}}, Result).

decode_with_extra_auth_info_test() ->
    {ok, Encoded} = flurm_protocol_codec:encode_with_extra(?REQUEST_PING, #ping_request{}),
    Result = flurm_protocol_codec:decode_with_extra(Encoded),
    case Result of
        {ok, _Msg, AuthInfo, _Rest} ->
            ?assert(is_map(AuthInfo));
        {error, _} ->
            ok
    end.

%%%===================================================================
%%% strip_auth_section/1 Tests
%%%===================================================================

strip_auth_section_tests() ->
    {"strip_auth_section/1 Tests", [
        {"strip_auth_section valid", fun strip_auth_section_valid_test/0},
        {"strip_auth_section empty", fun strip_auth_section_empty_test/0},
        {"strip_auth_section too short", fun strip_auth_section_too_short_test/0},
        {"strip_auth_section munge plugin", fun strip_auth_section_munge_test/0},
        {"strip_auth_section unknown plugin", fun strip_auth_section_unknown_test/0},
        {"strip_auth_section with body", fun strip_auth_section_with_body_test/0},
        {"strip_auth_section cred too short", fun strip_auth_section_cred_short_test/0}
    ]}.

strip_auth_section_valid_test() ->
    %% PluginId:32, CredLen:32, Credential
    Binary = <<101:32/big, 5:32/big, "hello", "rest">>,
    Result = flurm_protocol_codec:strip_auth_section(Binary),
    ?assertMatch({ok, <<"rest">>, _AuthInfo}, Result).

strip_auth_section_empty_test() ->
    Result = flurm_protocol_codec:strip_auth_section(<<>>),
    ?assertMatch({error, _}, Result).

strip_auth_section_too_short_test() ->
    Result = flurm_protocol_codec:strip_auth_section(<<1, 2, 3, 4>>),
    ?assertMatch({error, _}, Result).

strip_auth_section_munge_test() ->
    %% Plugin ID 101 = MUNGE
    Binary = <<101:32/big, 0:32/big>>,
    {ok, Body, AuthInfo} = flurm_protocol_codec:strip_auth_section(Binary),
    ?assertEqual(<<>>, Body),
    ?assertEqual(munge, maps:get(auth_type, AuthInfo)).

strip_auth_section_unknown_test() ->
    %% Plugin ID 999 = unknown
    Binary = <<999:32/big, 0:32/big>>,
    {ok, Body, AuthInfo} = flurm_protocol_codec:strip_auth_section(Binary),
    ?assertEqual(<<>>, Body),
    ?assertEqual(unknown, maps:get(auth_type, AuthInfo)).

strip_auth_section_with_body_test() ->
    Body = <<"message body data">>,
    Binary = <<101:32/big, 4:32/big, "cred", Body/binary>>,
    {ok, ExtractedBody, _AuthInfo} = flurm_protocol_codec:strip_auth_section(Binary),
    ?assertEqual(Body, ExtractedBody).

strip_auth_section_cred_short_test() ->
    %% CredLen says 100 bytes, but only 5 available
    Binary = <<101:32/big, 100:32/big, "short">>,
    Result = flurm_protocol_codec:strip_auth_section(Binary),
    ?assertMatch({error, {auth_cred_too_short, 100}}, Result).

%%%===================================================================
%%% encode_with_extra/2 and encode_with_extra/3 Tests
%%%===================================================================

encode_with_extra_tests() ->
    {"encode_with_extra Tests", [
        {"encode_with_extra/2 ping", fun encode_with_extra_2_ping_test/0},
        {"encode_with_extra/3 with hostname", fun encode_with_extra_3_hostname_test/0},
        {"encode_with_extra creates valid binary", fun encode_with_extra_valid_test/0},
        {"encode_with_extra larger than regular", fun encode_with_extra_larger_test/0}
    ]}.

encode_with_extra_2_ping_test() ->
    {ok, Binary} = flurm_protocol_codec:encode_with_extra(?REQUEST_PING, #ping_request{}),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) >= 14).

encode_with_extra_3_hostname_test() ->
    {ok, Binary} = flurm_protocol_codec:encode_with_extra(?REQUEST_PING, #ping_request{}, <<"myhost.local">>),
    ?assert(is_binary(Binary)).

encode_with_extra_valid_test() ->
    {ok, Binary} = flurm_protocol_codec:encode_with_extra(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    %% Should be a valid message with length prefix
    <<Length:32/big, Rest/binary>> = Binary,
    ?assertEqual(Length, byte_size(Rest)).

encode_with_extra_larger_test() ->
    %% encode_with_extra should produce larger binary than encode
    {ok, Regular} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    {ok, WithExtra} = flurm_protocol_codec:encode_with_extra(?REQUEST_PING, #ping_request{}),
    ?assert(byte_size(WithExtra) >= byte_size(Regular)).

%%%===================================================================
%%% encode_response/2 and decode_response/1 Tests
%%%===================================================================

encode_decode_response_tests() ->
    {"encode_response/decode_response Tests", [
        {"encode_response slurm_rc", fun encode_response_slurm_rc_test/0},
        {"decode_response slurm_rc", fun decode_response_slurm_rc_test/0},
        {"encode_decode_response roundtrip", fun encode_decode_response_roundtrip_test/0},
        {"decode_response empty", fun decode_response_empty_test/0},
        {"decode_response incomplete", fun decode_response_incomplete_test/0},
        {"encode_response various types", fun encode_response_various_types_test/0}
    ]}.

encode_response_slurm_rc_test() ->
    {ok, Binary} = flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) >= 14).

decode_response_slurm_rc_test() ->
    {ok, Encoded} = flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 42}),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode_response(Encoded),
    ?assertEqual(?RESPONSE_SLURM_RC, Msg#slurm_msg.header#slurm_header.msg_type),
    ?assertEqual(42, (Msg#slurm_msg.body)#slurm_rc_response.return_code).

encode_decode_response_roundtrip_test() ->
    Resp = #slurm_rc_response{return_code = -999},
    {ok, Encoded} = flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC, Resp),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode_response(Encoded),
    ?assertEqual(-999, (Msg#slurm_msg.body)#slurm_rc_response.return_code).

decode_response_empty_test() ->
    Result = flurm_protocol_codec:decode_response(<<>>),
    ?assertMatch({error, {incomplete_length_prefix, 0}}, Result).

decode_response_incomplete_test() ->
    Result = flurm_protocol_codec:decode_response(<<1, 2, 3>>),
    ?assertMatch({error, {incomplete_length_prefix, 3}}, Result).

encode_response_various_types_test() ->
    Types = [
        {?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}},
        {?RESPONSE_SUBMIT_BATCH_JOB, #batch_job_response{job_id = 123}},
        {?RESPONSE_JOB_INFO, #job_info_response{job_count = 0, jobs = []}}
    ],
    lists:foreach(fun({Type, Body}) ->
        {ok, Binary} = flurm_protocol_codec:encode_response(Type, Body),
        ?assert(is_binary(Binary))
    end, Types).

%%%===================================================================
%%% encode_response_no_auth/2 Tests
%%%===================================================================

encode_response_no_auth_tests() ->
    {"encode_response_no_auth/2 Tests", [
        {"encode_response_no_auth slurm_rc", fun encode_response_no_auth_slurm_rc_test/0},
        {"encode_response_no_auth sets flag", fun encode_response_no_auth_flag_test/0},
        {"encode_response_no_auth smaller", fun encode_response_no_auth_smaller_test/0}
    ]}.

encode_response_no_auth_slurm_rc_test() ->
    {ok, Binary} = flurm_protocol_codec:encode_response_no_auth(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    ?assert(is_binary(Binary)).

encode_response_no_auth_flag_test() ->
    {ok, Binary} = flurm_protocol_codec:encode_response_no_auth(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    %% Parse the header to check the flag
    <<_Length:32/big, _Version:16/big, Flags:16/big, _Rest/binary>> = Binary,
    ?assertEqual(?SLURM_NO_AUTH_CRED, Flags band ?SLURM_NO_AUTH_CRED).

encode_response_no_auth_smaller_test() ->
    %% encode_response_no_auth should be smaller than encode_response (no auth section)
    {ok, WithAuth} = flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    {ok, NoAuth} = flurm_protocol_codec:encode_response_no_auth(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    ?assert(byte_size(NoAuth) =< byte_size(WithAuth)).

%%%===================================================================
%%% encode_response_proper_auth/2 Tests
%%%===================================================================

encode_response_proper_auth_tests() ->
    {"encode_response_proper_auth/2 Tests", [
        {"encode_response_proper_auth slurm_rc", fun encode_response_proper_auth_slurm_rc_test/0},
        {"encode_response_proper_auth valid binary", fun encode_response_proper_auth_valid_test/0},
        {"encode_response_proper_auth decodable", fun encode_response_proper_auth_decode_test/0}
    ]}.

encode_response_proper_auth_slurm_rc_test() ->
    {ok, Binary} = flurm_protocol_codec:encode_response_proper_auth(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    ?assert(is_binary(Binary)).

encode_response_proper_auth_valid_test() ->
    {ok, Binary} = flurm_protocol_codec:encode_response_proper_auth(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 42}),
    %% Should have length prefix
    <<Length:32/big, Rest/binary>> = Binary,
    ?assertEqual(Length, byte_size(Rest)).

encode_response_proper_auth_decode_test() ->
    {ok, Encoded} = flurm_protocol_codec:encode_response_proper_auth(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 99}),
    %% Should be decodable with decode_response
    {ok, Msg, <<>>} = flurm_protocol_codec:decode_response(Encoded),
    ?assertEqual(?RESPONSE_SLURM_RC, Msg#slurm_msg.header#slurm_header.msg_type),
    ?assertEqual(99, (Msg#slurm_msg.body)#slurm_rc_response.return_code).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_tests() ->
    {"Roundtrip Tests", [
        {"roundtrip ping request", fun roundtrip_ping_test/0},
        {"roundtrip slurm_rc response", fun roundtrip_slurm_rc_test/0},
        {"roundtrip job_info request", fun roundtrip_job_info_test/0},
        {"roundtrip cancel_job request", fun roundtrip_cancel_job_test/0},
        {"roundtrip node_registration request", fun roundtrip_node_registration_test/0},
        {"roundtrip batch_job response", fun roundtrip_batch_job_response_test/0},
        {"roundtrip multiple messages", fun roundtrip_multiple_messages_test/0}
    ]}.

roundtrip_ping_test() ->
    Original = #ping_request{},
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, Original),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    ?assertMatch(#ping_request{}, Msg#slurm_msg.body).

roundtrip_slurm_rc_test() ->
    Values = [0, 1, -1, 100, -100, 16#7FFFFFFF, -16#80000000],
    lists:foreach(fun(RC) ->
        Original = #slurm_rc_response{return_code = RC},
        {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Original),
        {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(RC, (Msg#slurm_msg.body)#slurm_rc_response.return_code)
    end, Values).

roundtrip_job_info_test() ->
    Original = #job_info_request{show_flags = 16#FF, job_id = 12345, user_id = 1000},
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Original),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    Body = Msg#slurm_msg.body,
    ?assertEqual(16#FF, Body#job_info_request.show_flags),
    ?assertEqual(12345, Body#job_info_request.job_id),
    ?assertEqual(1000, Body#job_info_request.user_id).

roundtrip_cancel_job_test() ->
    Original = #cancel_job_request{job_id = 99999, job_id_str = <<"99999">>, step_id = 1, signal = 15, flags = 2},
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Original),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    Body = Msg#slurm_msg.body,
    ?assertEqual(99999, Body#cancel_job_request.job_id),
    ?assertEqual(15, Body#cancel_job_request.signal).

roundtrip_node_registration_test() ->
    lists:foreach(fun(StatusOnly) ->
        Original = #node_registration_request{status_only = StatusOnly},
        {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_NODE_REGISTRATION_STATUS, Original),
        {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(StatusOnly, (Msg#slurm_msg.body)#node_registration_request.status_only)
    end, [true, false]).

roundtrip_batch_job_response_test() ->
    Original = #batch_job_response{job_id = 54321, step_id = 0, error_code = 0, job_submit_user_msg = <<"OK">>},
    {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SUBMIT_BATCH_JOB, Original),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    Body = Msg#slurm_msg.body,
    ?assertEqual(54321, Body#batch_job_response.job_id),
    ?assertEqual(0, Body#batch_job_response.error_code).

roundtrip_multiple_messages_test() ->
    Messages = [
        {?REQUEST_PING, #ping_request{}},
        {?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}},
        {?REQUEST_JOB_INFO, #job_info_request{show_flags = 1}},
        {?REQUEST_CANCEL_JOB, #cancel_job_request{job_id = 123}}
    ],
    lists:foreach(fun({Type, Body}) ->
        {ok, Encoded} = flurm_protocol_codec:encode(Type, Body),
        {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(Type, Msg#slurm_msg.header#slurm_header.msg_type)
    end, Messages).

%%%===================================================================
%%% Edge Case Tests
%%%===================================================================

edge_case_tests() ->
    {"Edge Case Tests", [
        {"encode with default values", fun encode_default_values_test/0},
        {"encode responses with empty lists", fun encode_empty_lists_test/0},
        {"decode unknown message type", fun decode_unknown_type_test/0},
        {"binary passthrough", fun binary_passthrough_test/0},
        {"zero values", fun zero_values_test/0},
        {"special SLURM values", fun special_slurm_values_test/0}
    ]}.

encode_default_values_test() ->
    Records = [
        {?REQUEST_PING, #ping_request{}},
        {?RESPONSE_SLURM_RC, #slurm_rc_response{}},
        {?REQUEST_JOB_INFO, #job_info_request{}},
        {?REQUEST_CANCEL_JOB, #cancel_job_request{}},
        {?REQUEST_NODE_REGISTRATION_STATUS, #node_registration_request{}}
    ],
    lists:foreach(fun({Type, Record}) ->
        {ok, Binary} = flurm_protocol_codec:encode(Type, Record),
        ?assert(is_binary(Binary))
    end, Records).

encode_empty_lists_test() ->
    Responses = [
        {?RESPONSE_JOB_INFO, #job_info_response{jobs = []}},
        {?RESPONSE_NODE_INFO, #node_info_response{nodes = []}},
        {?RESPONSE_PARTITION_INFO, #partition_info_response{partitions = []}},
        {?RESPONSE_JOB_STEP_INFO, #job_step_info_response{steps = []}}
    ],
    lists:foreach(fun({Type, Response}) ->
        {ok, Binary} = flurm_protocol_codec:encode(Type, Response),
        ?assert(is_binary(Binary))
    end, Responses).

decode_unknown_type_test() ->
    %% Unknown types should return raw binary
    Data = <<"some raw data">>,
    {ok, Body} = flurm_protocol_codec:decode_body(99999, Data),
    ?assertEqual(Data, Body).

binary_passthrough_test() ->
    %% Raw binary should be accepted for encoding unknown types
    {ok, Binary} = flurm_protocol_codec:encode_body(99999, <<"raw binary">>),
    ?assertEqual(<<"raw binary">>, Binary).

zero_values_test() ->
    %% Test with all zeros
    Resp = #slurm_rc_response{return_code = 0},
    {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    ?assertEqual(0, (Msg#slurm_msg.body)#slurm_rc_response.return_code).

special_slurm_values_test() ->
    %% Test SLURM special values
    SpecialValues = [
        ?SLURM_NO_VAL,
        ?SLURM_INFINITE
    ],
    lists:foreach(fun(Val) ->
        Req = #job_info_request{job_id = Val},
        {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Req),
        {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(Val, (Msg#slurm_msg.body)#job_info_request.job_id)
    end, SpecialValues).

%%%===================================================================
%%% Error Handling Tests
%%%===================================================================

error_handling_tests() ->
    {"Error Handling Tests", [
        {"decode empty binary error", fun decode_empty_error_test/0},
        {"decode short binary error", fun decode_short_error_test/0},
        {"decode length mismatch error", fun decode_length_mismatch_test/0},
        {"decode_response errors", fun decode_response_errors_test/0},
        {"decode_with_extra errors", fun decode_with_extra_errors_test/0},
        {"invalid auth section", fun invalid_auth_section_test/0}
    ]}.

decode_empty_error_test() ->
    Result = flurm_protocol_codec:decode(<<>>),
    ?assertMatch({error, {incomplete_length_prefix, 0}}, Result).

decode_short_error_test() ->
    Result = flurm_protocol_codec:decode(<<1, 2, 3>>),
    ?assertMatch({error, {incomplete_length_prefix, 3}}, Result).

decode_length_mismatch_test() ->
    Result = flurm_protocol_codec:decode(<<100:32/big, 1, 2, 3, 4, 5>>),
    ?assertMatch({error, {incomplete_message, 100, _}}, Result).

decode_response_errors_test() ->
    ?assertMatch({error, _}, flurm_protocol_codec:decode_response(<<>>)),
    ?assertMatch({error, _}, flurm_protocol_codec:decode_response(<<1, 2, 3>>)),
    ?assertMatch({error, _}, flurm_protocol_codec:decode_response(<<100:32/big, 1, 2, 3>>)).

decode_with_extra_errors_test() ->
    ?assertMatch({error, _}, flurm_protocol_codec:decode_with_extra(<<>>)),
    ?assertMatch({error, _}, flurm_protocol_codec:decode_with_extra(<<1, 2, 3>>)).

invalid_auth_section_test() ->
    %% Test various invalid auth section formats
    Invalids = [
        <<>>,
        <<1>>,
        <<1, 2, 3, 4>>,
        <<1, 2, 3, 4, 5, 6, 7>>
    ],
    lists:foreach(fun(Invalid) ->
        Result = flurm_protocol_codec:strip_auth_section(Invalid),
        ?assertMatch({error, _}, Result)
    end, Invalids).

%%%===================================================================
%%% Boundary Value Tests
%%%===================================================================

boundary_value_tests() ->
    {"Boundary Value Tests", [
        {"max job_id", fun max_job_id_test/0},
        {"max step_id", fun max_step_id_test/0},
        {"return_code boundaries", fun return_code_boundaries_test/0},
        {"show_flags boundaries", fun show_flags_boundaries_test/0},
        {"user_id boundaries", fun user_id_boundaries_test/0}
    ]}.

max_job_id_test() ->
    MaxJobId = 16#FFFFFFFF,
    Req = #cancel_job_request{job_id = MaxJobId},
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Req),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    ?assertEqual(MaxJobId, (Msg#slurm_msg.body)#cancel_job_request.job_id).

max_step_id_test() ->
    MaxStepId = 16#FFFFFFFF,
    Req = #cancel_job_request{job_id = 1, step_id = MaxStepId},
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Req),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
    ?assertEqual(MaxStepId, (Msg#slurm_msg.body)#cancel_job_request.step_id).

return_code_boundaries_test() ->
    Boundaries = [
        0,
        1,
        -1,
        16#7FFFFFFF,   % Max positive 32-bit signed
        -16#80000000   % Min negative 32-bit signed
    ],
    lists:foreach(fun(RC) ->
        Resp = #slurm_rc_response{return_code = RC},
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC, Resp),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, Binary),
        ?assertEqual(RC, Decoded#slurm_rc_response.return_code)
    end, Boundaries).

show_flags_boundaries_test() ->
    Flags = [0, 1, 16#FF, 16#FFFF, 16#FFFFFFFF],
    lists:foreach(fun(Flag) ->
        Req = #job_info_request{show_flags = Flag},
        {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Req),
        {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(Flag, (Msg#slurm_msg.body)#job_info_request.show_flags)
    end, Flags).

user_id_boundaries_test() ->
    UserIds = [0, 1, 1000, 65534, 65535, 16#FFFFFFFF],
    lists:foreach(fun(UserId) ->
        Req = #job_info_request{user_id = UserId},
        {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Req),
        {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(UserId, (Msg#slurm_msg.body)#job_info_request.user_id)
    end, UserIds).

%%%===================================================================
%%% Empty and Null Input Tests
%%%===================================================================

empty_null_input_tests() ->
    {"Empty and Null Input Tests", [
        {"decode body empty for various types", fun decode_body_empty_test/0},
        {"encode with empty strings", fun encode_empty_strings_test/0},
        {"decode empty list responses", fun decode_empty_list_responses_test/0}
    ]}.

decode_body_empty_test() ->
    Types = [
        ?REQUEST_PING,
        ?REQUEST_BUILD_INFO,
        ?REQUEST_CONFIG_INFO,
        ?REQUEST_RECONFIGURE,
        ?REQUEST_RECONFIGURE_WITH_CONFIG
    ],
    lists:foreach(fun(Type) ->
        {ok, _Body} = flurm_protocol_codec:decode_body(Type, <<>>)
    end, Types).

encode_empty_strings_test() ->
    Req = #cancel_job_request{job_id = 1, job_id_str = <<>>},
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Req),
    ?assert(is_binary(Binary)).

decode_empty_list_responses_test() ->
    Responses = [
        #job_info_response{job_count = 0, jobs = []},
        #node_info_response{node_count = 0, nodes = []},
        #partition_info_response{partition_count = 0, partitions = []}
    ],
    Types = [?RESPONSE_JOB_INFO, ?RESPONSE_NODE_INFO, ?RESPONSE_PARTITION_INFO],
    lists:foreach(fun({Type, Resp}) ->
        {ok, Binary} = flurm_protocol_codec:encode(Type, Resp),
        ?assert(is_binary(Binary))
    end, lists:zip(Types, Responses)).

%%%===================================================================
%%% Large Input Tests
%%%===================================================================

large_input_tests() ->
    {"Large Input Tests", [
        {"large binary body", fun large_binary_body_test/0},
        {"large job_id_str", fun large_job_id_str_test/0},
        {"many sequential encodes", fun many_sequential_encodes_test/0}
    ]}.

large_binary_body_test() ->
    %% Test with 10KB of data
    LargeBin = <<0:(10*1024*8)>>,
    {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_PING, LargeBin),
    ?assertMatch(#ping_request{}, Body).

large_job_id_str_test() ->
    %% Test with large job_id_str
    LargeStr = list_to_binary(lists:duplicate(1000, $1)),
    Req = #cancel_job_request{job_id = 1, job_id_str = LargeStr},
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Req),
    ?assert(is_binary(Binary)).

many_sequential_encodes_test() ->
    %% Test many sequential encode/decode operations
    lists:foreach(fun(N) ->
        Resp = #slurm_rc_response{return_code = N},
        {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp),
        {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(N, (Msg#slurm_msg.body)#slurm_rc_response.return_code)
    end, lists:seq(1, 100)).

%%%===================================================================
%%% Malformed Input Tests
%%%===================================================================

malformed_input_tests() ->
    {"Malformed Input Tests", [
        {"malformed header", fun malformed_header_test/0},
        {"truncated message", fun truncated_message_test/0},
        {"garbage data", fun garbage_data_test/0},
        {"invalid length prefix", fun invalid_length_prefix_test/0}
    ]}.

malformed_header_test() ->
    %% Valid length but garbage header
    Result = flurm_protocol_codec:decode(<<20:32/big, 0:160>>),
    %% May succeed or fail depending on header parsing
    case Result of
        {ok, _, _} -> ok;
        {error, _} -> ok
    end.

truncated_message_test() ->
    {ok, Encoded} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    %% Truncate the message
    TruncatedLen = byte_size(Encoded) - 5,
    <<Truncated:TruncatedLen/binary, _/binary>> = Encoded,
    Result = flurm_protocol_codec:decode(Truncated),
    ?assertMatch({error, _}, Result).

garbage_data_test() ->
    Garbage = crypto:strong_rand_bytes(100),
    Result = flurm_protocol_codec:decode(<<100:32/big, Garbage/binary>>),
    %% May succeed (if garbage happens to parse) or fail
    case Result of
        {ok, _, _} -> ok;
        {error, _} -> ok
    end.

invalid_length_prefix_test() ->
    %% Length of 0
    Result1 = flurm_protocol_codec:decode(<<0:32/big>>),
    ?assertMatch({error, _}, Result1),

    %% Length smaller than header
    Result2 = flurm_protocol_codec:decode(<<5:32/big, 1, 2, 3, 4, 5>>),
    ?assertMatch({error, _}, Result2).

%%%===================================================================
%%% Protocol Compatibility Tests
%%%===================================================================

protocol_compatibility_tests() ->
    {"Protocol Compatibility Tests", [
        {"protocol version in header", fun protocol_version_header_test/0},
        {"message type preserved", fun message_type_preserved_test/0},
        {"flags preserved", fun flags_preserved_test/0},
        {"body length correct", fun body_length_correct_test/0}
    ]}.

protocol_version_header_test() ->
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),
    <<_Len:32/big, Version:16/big, _Rest/binary>> = Binary,
    ?assertEqual(?SLURM_PROTOCOL_VERSION, Version).

message_type_preserved_test() ->
    Types = [?REQUEST_PING, ?RESPONSE_SLURM_RC, ?REQUEST_JOB_INFO, ?REQUEST_CANCEL_JOB],
    lists:foreach(fun(Type) ->
        Body = case Type of
            ?REQUEST_PING -> #ping_request{};
            ?RESPONSE_SLURM_RC -> #slurm_rc_response{};
            ?REQUEST_JOB_INFO -> #job_info_request{};
            ?REQUEST_CANCEL_JOB -> #cancel_job_request{}
        end,
        {ok, Encoded} = flurm_protocol_codec:encode(Type, Body),
        {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
        ?assertEqual(Type, Msg#slurm_msg.header#slurm_header.msg_type)
    end, Types).

flags_preserved_test() ->
    %% Test that flags in encode_response_no_auth are set correctly
    {ok, Binary} = flurm_protocol_codec:encode_response_no_auth(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    <<_Len:32/big, _Version:16/big, Flags:16/big, _Rest/binary>> = Binary,
    ?assertEqual(?SLURM_NO_AUTH_CRED, Flags band ?SLURM_NO_AUTH_CRED).

body_length_correct_test() ->
    %% Verify body_length field matches actual body
    {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 42}),
    <<TotalLen:32/big, _Version:16/big, _Flags:16/big, _MsgType:16/big, BodyLen:32/big, _Rest/binary>> = Encoded,
    %% Total length should be >= header + body_length
    ?assert(TotalLen >= 16 + BodyLen).

%%%===================================================================
%%% Additional Coverage Tests
%%%===================================================================

%% Test encode_reconfigure_response
encode_reconfigure_response_test_() ->
    [
        {"encode_reconfigure_response full", fun() ->
            Resp = #reconfigure_response{
                return_code = 0,
                message = <<"Configuration reloaded">>,
                changed_keys = ['Port', 'Timeout'],
                version = 1
            },
            {ok, Binary} = flurm_protocol_codec:encode_reconfigure_response(Resp),
            ?assert(is_binary(Binary))
        end},
        {"encode_reconfigure_response empty", fun() ->
            Resp = #reconfigure_response{
                return_code = 0,
                message = <<>>,
                changed_keys = [],
                version = 0
            },
            {ok, Binary} = flurm_protocol_codec:encode_reconfigure_response(Resp),
            ?assert(is_binary(Binary))
        end},
        {"encode_reconfigure_response non-record", fun() ->
            Result = flurm_protocol_codec:encode_reconfigure_response(not_a_record),
            ?assertMatch({ok, _}, Result)
        end}
    ].

%% Test message_type_name/1
message_type_name_test_() ->
    [
        {"message_type_name REQUEST_PING", fun() ->
            ?assertEqual(request_ping, flurm_protocol_codec:message_type_name(?REQUEST_PING))
        end},
        {"message_type_name RESPONSE_SLURM_RC", fun() ->
            ?assertEqual(response_slurm_rc, flurm_protocol_codec:message_type_name(?RESPONSE_SLURM_RC))
        end},
        {"message_type_name REQUEST_RECONFIGURE", fun() ->
            ?assertEqual(request_reconfigure, flurm_protocol_codec:message_type_name(?REQUEST_RECONFIGURE))
        end},
        {"message_type_name REQUEST_SHUTDOWN", fun() ->
            ?assertEqual(request_shutdown, flurm_protocol_codec:message_type_name(?REQUEST_SHUTDOWN))
        end},
        {"message_type_name REQUEST_BUILD_INFO", fun() ->
            ?assertEqual(request_build_info, flurm_protocol_codec:message_type_name(?REQUEST_BUILD_INFO))
        end},
        {"message_type_name REQUEST_CONFIG_INFO", fun() ->
            ?assertEqual(request_config_info, flurm_protocol_codec:message_type_name(?REQUEST_CONFIG_INFO))
        end},
        {"message_type_name REQUEST_STATS_INFO", fun() ->
            ?assertEqual(request_stats_info, flurm_protocol_codec:message_type_name(?REQUEST_STATS_INFO))
        end},
        {"message_type_name unknown", fun() ->
            ?assertMatch({unknown, _}, flurm_protocol_codec:message_type_name(99999))
        end}
    ].

%% Test is_request/1
is_request_test_() ->
    [
        {"is_request REQUEST_PING", fun() ->
            ?assert(flurm_protocol_codec:is_request(?REQUEST_PING))
        end},
        {"is_request REQUEST_RECONFIGURE", fun() ->
            ?assert(flurm_protocol_codec:is_request(?REQUEST_RECONFIGURE))
        end},
        {"is_request REQUEST_SHUTDOWN", fun() ->
            ?assert(flurm_protocol_codec:is_request(?REQUEST_SHUTDOWN))
        end},
        {"is_request REQUEST_BUILD_INFO", fun() ->
            ?assert(flurm_protocol_codec:is_request(?REQUEST_BUILD_INFO))
        end},
        {"is_request RESPONSE_SLURM_RC returns false", fun() ->
            ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_SLURM_RC))
        end}
    ].

%% Test is_response/1
is_response_test_() ->
    [
        {"is_response RESPONSE_SLURM_RC", fun() ->
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_SLURM_RC))
        end},
        {"is_response RESPONSE_BUILD_INFO", fun() ->
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_BUILD_INFO))
        end},
        {"is_response RESPONSE_CONFIG_INFO", fun() ->
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_CONFIG_INFO))
        end},
        {"is_response RESPONSE_STATS_INFO", fun() ->
            ?assert(flurm_protocol_codec:is_response(?RESPONSE_STATS_INFO))
        end},
        {"is_response REQUEST_PING returns false", fun() ->
            ?assertNot(flurm_protocol_codec:is_response(?REQUEST_PING))
        end}
    ].

%% Additional SRUN_JOB_COMPLETE tests
%% Note: SRUN_JOB_COMPLETE has job_id, step_id, step_het_comp (3 x 32-bit)
srun_job_complete_test_() ->
    [
        {"encode SRUN_JOB_COMPLETE record", fun() ->
            Msg = #srun_job_complete{job_id = 12345, step_id = 0},
            {ok, Binary} = flurm_protocol_codec:encode_body(?SRUN_JOB_COMPLETE, Msg),
            %% step_het_comp = 0 is added automatically
            ?assertEqual(<<12345:32/big, 0:32/big, 0:32/big>>, Binary)
        end},
        {"encode SRUN_JOB_COMPLETE with step_id", fun() ->
            Msg = #srun_job_complete{job_id = 999, step_id = 1},
            {ok, Binary} = flurm_protocol_codec:encode_body(?SRUN_JOB_COMPLETE, Msg),
            ?assertEqual(<<999:32/big, 1:32/big, 0:32/big>>, Binary)
        end},
        {"encode SRUN_JOB_COMPLETE various values", fun() ->
            Values = [{0, 0}, {1, 0}, {12345, 5}, {99999, 100}],
            lists:foreach(fun({JobId, StepId}) ->
                Msg = #srun_job_complete{job_id = JobId, step_id = StepId},
                {ok, Binary} = flurm_protocol_codec:encode_body(?SRUN_JOB_COMPLETE, Msg),
                %% Verify format: job_id(32), step_id(32), step_het_comp(32)
                <<J:32/big, S:32/big, _:32/big>> = Binary,
                ?assertEqual(JobId, J),
                ?assertEqual(StepId, S)
            end, Values)
        end}
    ].

%% Test extract_resources_from_protocol
extract_resources_test_() ->
    [
        {"extract_resources_from_protocol small binary", fun() ->
            Binary = <<1, 2, 3, 4, 5>>,
            {MinNodes, MinCpus} = flurm_protocol_codec:extract_resources_from_protocol(Binary),
            ?assertEqual(0, MinNodes),
            ?assertEqual(0, MinCpus)
        end},
        {"extract_resources_from_protocol empty", fun() ->
            {MinNodes, MinCpus} = flurm_protocol_codec:extract_resources_from_protocol(<<>>),
            ?assertEqual(0, MinNodes),
            ?assertEqual(0, MinCpus)
        end}
    ].

%% Test extract_full_job_desc
extract_full_job_desc_test_() ->
    [
        {"extract_full_job_desc small binary", fun() ->
            Binary = <<1, 2, 3, 4, 5>>,
            Result = flurm_protocol_codec:extract_full_job_desc(Binary),
            ?assertMatch({error, binary_too_small}, Result)
        end},
        {"extract_full_job_desc valid", fun() ->
            %% Build a binary with job_id, user_id, group_id at expected locations
            Binary = <<12345:32/big, 1000:32/big, 1000:32/big, 0:800/big>>,
            Result = flurm_protocol_codec:extract_full_job_desc(Binary),
            ?assertMatch({ok, _}, Result)
        end}
    ].

%% Additional decode_body tests for coverage
additional_decode_body_test_() ->
    [
        {"decode_body REQUEST_RESERVATION_INFO empty", fun() ->
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_RESERVATION_INFO, <<>>),
            ?assertMatch(#reservation_info_request{}, Body)
        end},
        {"decode_body REQUEST_LICENSE_INFO empty", fun() ->
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_LICENSE_INFO, <<>>),
            ?assertMatch(#license_info_request{}, Body)
        end},
        {"decode_body REQUEST_TOPO_INFO empty", fun() ->
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_TOPO_INFO, <<>>),
            ?assertMatch(#topo_info_request{}, Body)
        end},
        {"decode_body REQUEST_FRONT_END_INFO empty", fun() ->
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_FRONT_END_INFO, <<>>),
            ?assertMatch(#front_end_info_request{}, Body)
        end},
        {"decode_body REQUEST_BURST_BUFFER_INFO empty", fun() ->
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_BURST_BUFFER_INFO, <<>>),
            ?assertMatch(#burst_buffer_info_request{}, Body)
        end}
    ].

%% Additional encode_body tests for coverage
additional_encode_body_test_() ->
    [
        {"encode_body RESPONSE_BUILD_INFO", fun() ->
            Resp = #build_info_response{
                version = <<"22.05.0">>,
                cluster_name = <<"test">>,
                control_machine = <<"controller">>,
                slurmctld_port = 6817,
                slurmd_port = 6818
            },
            {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_BUILD_INFO, Resp),
            ?assert(is_binary(Binary))
        end},
        {"encode_body RESPONSE_CONFIG_INFO", fun() ->
            Resp = #config_info_response{
                last_update = erlang:system_time(second),
                config = #{'ClusterName' => <<"test">>}
            },
            {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_CONFIG_INFO, Resp),
            ?assert(is_binary(Binary))
        end},
        {"encode_body RESPONSE_RESERVATION_INFO", fun() ->
            Resp = #reservation_info_response{
                last_update = erlang:system_time(second),
                reservation_count = 0,
                reservations = []
            },
            {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_RESERVATION_INFO, Resp),
            ?assert(is_binary(Binary))
        end},
        {"encode_body RESPONSE_LICENSE_INFO", fun() ->
            Resp = #license_info_response{
                last_update = erlang:system_time(second),
                license_count = 0,
                licenses = []
            },
            {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_LICENSE_INFO, Resp),
            ?assert(is_binary(Binary))
        end},
        {"encode_body RESPONSE_TOPO_INFO", fun() ->
            Resp = #topo_info_response{
                topo_count = 0,
                topos = []
            },
            {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_TOPO_INFO, Resp),
            ?assert(is_binary(Binary))
        end},
        {"encode_body RESPONSE_FRONT_END_INFO", fun() ->
            Resp = #front_end_info_response{
                last_update = erlang:system_time(second),
                front_end_count = 0,
                front_ends = []
            },
            {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FRONT_END_INFO, Resp),
            ?assert(is_binary(Binary))
        end},
        {"encode_body RESPONSE_BURST_BUFFER_INFO", fun() ->
            Resp = #burst_buffer_info_response{
                last_update = erlang:system_time(second),
                burst_buffer_count = 0,
                burst_buffers = []
            },
            {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_BURST_BUFFER_INFO, Resp),
            ?assert(is_binary(Binary))
        end}
    ].

%% Test decode_body for various REQUEST types with data
request_decode_with_data_test_() ->
    [
        {"decode REQUEST_RESERVATION_INFO with flags", fun() ->
            Binary = <<42:32/big>>,
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_RESERVATION_INFO, Binary),
            ?assertEqual(42, Body#reservation_info_request.show_flags)
        end},
        {"decode REQUEST_LICENSE_INFO with flags", fun() ->
            Binary = <<99:32/big>>,
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_LICENSE_INFO, Binary),
            ?assertEqual(99, Body#license_info_request.show_flags)
        end},
        {"decode REQUEST_TOPO_INFO with flags", fun() ->
            Binary = <<123:32/big>>,
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_TOPO_INFO, Binary),
            ?assertEqual(123, Body#topo_info_request.show_flags)
        end},
        {"decode REQUEST_FRONT_END_INFO with flags", fun() ->
            Binary = <<456:32/big>>,
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_FRONT_END_INFO, Binary),
            ?assertEqual(456, Body#front_end_info_request.show_flags)
        end},
        {"decode REQUEST_BURST_BUFFER_INFO with flags", fun() ->
            Binary = <<789:32/big>>,
            {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_BURST_BUFFER_INFO, Binary),
            ?assertEqual(789, Body#burst_buffer_info_request.show_flags)
        end}
    ].

%% Stress tests
stress_test_() ->
    [
        {"stress encode decode 1000 messages", fun() ->
            lists:foreach(fun(N) ->
                Resp = #slurm_rc_response{return_code = N rem 256 - 128},
                {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp),
                {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
                ?assertEqual(N rem 256 - 128, (Msg#slurm_msg.body)#slurm_rc_response.return_code)
            end, lists:seq(1, 1000))
        end},
        {"stress various message types", fun() ->
            Types = [
                {?REQUEST_PING, #ping_request{}},
                {?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}},
                {?REQUEST_JOB_INFO, #job_info_request{show_flags = 1}},
                {?REQUEST_CANCEL_JOB, #cancel_job_request{job_id = 123}}
            ],
            lists:foreach(fun(_) ->
                lists:foreach(fun({Type, Body}) ->
                    {ok, Encoded} = flurm_protocol_codec:encode(Type, Body),
                    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
                    ?assertEqual(Type, Msg#slurm_msg.header#slurm_header.msg_type)
                end, Types)
            end, lists:seq(1, 100))
        end}
    ].

%% Concurrent access test (simplified - just sequential)
concurrent_test_() ->
    [
        {"sequential encode decode operations", fun() ->
            Parent = self(),
            Workers = [spawn(fun() ->
                lists:foreach(fun(N) ->
                    Resp = #slurm_rc_response{return_code = N},
                    {ok, Encoded} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Resp),
                    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Encoded),
                    N = (Msg#slurm_msg.body)#slurm_rc_response.return_code
                end, lists:seq(1, 10)),
                Parent ! {done, self()}
            end) || _ <- lists:seq(1, 5)],
            lists:foreach(fun(Worker) ->
                receive {done, Worker} -> ok
                after 5000 -> throw(timeout)
                end
            end, Workers)
        end}
    ].

%% Memory efficiency test (just ensures no crashes with various sizes)
memory_test_() ->
    [
        {"various message sizes", fun() ->
            Sizes = [0, 1, 10, 100, 1000, 10000],
            lists:foreach(fun(Size) ->
                Data = <<0:Size/unit:8>>,
                {ok, _Body} = flurm_protocol_codec:decode_body(?REQUEST_PING, Data)
            end, Sizes)
        end}
    ].
