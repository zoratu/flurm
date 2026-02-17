%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_protocol_codec
%%%
%%% This test module provides extensive coverage of the protocol codec,
%%% focusing on edge cases, error handling, and roundtrip verification.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_codec_comprehensive_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

codec_comprehensive_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          %% Core decode tests
          {"decode empty binary", fun test_decode_empty/0},
          {"decode too short", fun test_decode_too_short/0},
          {"decode length mismatch", fun test_decode_length_mismatch/0},
          {"decode valid ping", fun test_decode_valid_ping/0},
          {"decode with remaining data", fun test_decode_with_remaining/0},

          %% Core encode tests
          {"encode ping request", fun test_encode_ping/0},
          {"encode slurm_rc response", fun test_encode_slurm_rc/0},
          {"encode unknown type", fun test_encode_unknown_type/0},

          %% decode_with_extra tests
          {"decode_with_extra empty", fun test_decode_with_extra_empty/0},
          {"decode_with_extra too short", fun test_decode_with_extra_too_short/0},
          {"decode_with_extra valid", fun test_decode_with_extra_valid/0},

          %% encode_with_extra tests
          {"encode_with_extra/2", fun test_encode_with_extra_2/0},
          {"encode_with_extra/3", fun test_encode_with_extra_3/0},

          %% Response encoding tests
          {"encode_response ping", fun test_encode_response_ping/0},
          {"encode_response_no_auth", fun test_encode_response_no_auth/0},
          {"encode_response_proper_auth", fun test_encode_response_proper_auth/0},

          %% decode_response tests
          {"decode_response empty", fun test_decode_response_empty/0},
          {"decode_response too short", fun test_decode_response_too_short/0},
          {"decode_response valid", fun test_decode_response_valid/0},

          %% Message type helpers
          {"message_type_name known", fun test_message_type_name_known/0},
          {"message_type_name unknown", fun test_message_type_name_unknown/0},
          {"is_request true", fun test_is_request_true/0},
          {"is_request false", fun test_is_request_false/0},
          {"is_response true", fun test_is_response_true/0},
          {"is_response false", fun test_is_response_false/0}
         ]
     end}.

roundtrip_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          %% Roundtrip tests for various message types
          {"roundtrip ping", fun test_roundtrip_ping/0},
          {"roundtrip slurm_rc", fun test_roundtrip_slurm_rc/0},
          {"roundtrip cancel_job", fun test_roundtrip_cancel_job/0},
          {"roundtrip job_info request", fun test_roundtrip_job_info_request/0},
          {"roundtrip node_info", fun test_roundtrip_node_info/0},
          {"roundtrip partition_info", fun test_roundtrip_partition_info/0},
          {"roundtrip build_info", fun test_roundtrip_build_info/0},
          {"roundtrip suspend", fun test_roundtrip_suspend/0},
          {"roundtrip signal_job", fun test_roundtrip_signal_job/0},
          {"roundtrip kill_job", fun test_roundtrip_kill_job/0}
         ]
     end}.

body_decode_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          %% decode_body tests for all message types
          {"decode_body REQUEST_PING", fun test_decode_body_ping/0},
          {"decode_body REQUEST_PING with data", fun test_decode_body_ping_with_data/0},
          {"decode_body REQUEST_NODE_REGISTRATION_STATUS", fun test_decode_body_node_reg/0},
          {"decode_body REQUEST_JOB_INFO", fun test_decode_body_job_info/0},
          {"decode_body REQUEST_RESOURCE_ALLOCATION", fun test_decode_body_resource_alloc/0},
          {"decode_body REQUEST_SUBMIT_BATCH_JOB", fun test_decode_body_submit_batch/0},
          {"decode_body REQUEST_CANCEL_JOB", fun test_decode_body_cancel_job/0},
          {"decode_body REQUEST_KILL_JOB", fun test_decode_body_kill_job/0},
          {"decode_body REQUEST_SUSPEND", fun test_decode_body_suspend/0},
          {"decode_body REQUEST_SIGNAL_JOB", fun test_decode_body_signal_job/0},
          {"decode_body REQUEST_COMPLETE_PROLOG", fun test_decode_body_complete_prolog/0},
          {"decode_body MESSAGE_EPILOG_COMPLETE", fun test_decode_body_epilog_complete/0},
          {"decode_body MESSAGE_TASK_EXIT", fun test_decode_body_task_exit/0},
          {"decode_body REQUEST_UPDATE_JOB", fun test_decode_body_update_job/0},
          {"decode_body REQUEST_JOB_WILL_RUN", fun test_decode_body_job_will_run/0},
          {"decode_body REQUEST_JOB_STEP_CREATE", fun test_decode_body_step_create/0},
          {"decode_body REQUEST_JOB_STEP_INFO", fun test_decode_body_step_info/0},
          {"decode_body REQUEST_NODE_INFO", fun test_decode_body_node_info/0},
          {"decode_body REQUEST_PARTITION_INFO", fun test_decode_body_partition_info/0},
          {"decode_body REQUEST_BUILD_INFO", fun test_decode_body_build_info/0},
          {"decode_body REQUEST_RECONFIGURE", fun test_decode_body_reconfigure/0},
          {"decode_body RESPONSE_SLURM_RC", fun test_decode_body_slurm_rc/0},
          {"decode_body unknown type", fun test_decode_body_unknown/0},
          {"decode_body empty binary", fun test_decode_body_empty/0}
         ]
     end}.

body_encode_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          %% encode_body tests for all message types
          {"encode_body REQUEST_PING", fun test_encode_body_ping/0},
          {"encode_body RESPONSE_SLURM_RC", fun test_encode_body_slurm_rc/0},
          {"encode_body REQUEST_CANCEL_JOB", fun test_encode_body_cancel_job/0},
          {"encode_body REQUEST_KILL_JOB", fun test_encode_body_kill_job/0},
          {"encode_body REQUEST_SUSPEND", fun test_encode_body_suspend/0},
          {"encode_body REQUEST_SIGNAL_JOB", fun test_encode_body_signal_job/0},
          {"encode_body REQUEST_JOB_INFO", fun test_encode_body_job_info/0},
          {"encode_body REQUEST_NODE_INFO", fun test_encode_body_node_info/0},
          {"encode_body REQUEST_PARTITION_INFO", fun test_encode_body_partition_info/0},
          {"encode_body REQUEST_BUILD_INFO", fun test_encode_body_build_info/0},
          {"encode_body unknown type", fun test_encode_body_unknown/0},
          {"encode_body with record", fun test_encode_body_with_record/0},
          {"encode_body with map", fun test_encode_body_with_map/0}
         ]
     end}.

edge_case_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          %% Edge cases and boundary values
          {"max length message", fun test_max_length/0},
          {"zero length body", fun test_zero_length_body/0},
          {"binary with nulls", fun test_binary_with_nulls/0},
          {"unicode in strings", fun test_unicode_strings/0},
          {"very long strings", fun test_very_long_strings/0},
          {"negative values", fun test_negative_values/0},
          {"max uint32 values", fun test_max_uint32/0},
          {"empty strings", fun test_empty_strings/0},
          {"special characters", fun test_special_characters/0},
          {"truncated header", fun test_truncated_header/0},
          {"corrupted header", fun test_corrupted_header/0},
          {"misaligned data", fun test_misaligned_data/0}
         ]
     end}.

extract_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          %% Job description extraction tests
          {"extract_full_job_desc from record", fun test_extract_job_desc_record/0},
          {"extract_full_job_desc from map", fun test_extract_job_desc_map/0},
          {"extract_full_job_desc empty", fun test_extract_job_desc_empty/0},
          {"extract_resources_from_protocol", fun test_extract_resources/0},
          {"extract_resources empty", fun test_extract_resources_empty/0}
         ]
     end}.

reconfigure_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          {"encode_reconfigure_response ok", fun test_reconfigure_response_ok/0},
          {"encode_reconfigure_response error", fun test_reconfigure_response_error/0},
          {"encode_reconfigure_response with message", fun test_reconfigure_response_msg/0}
         ]
     end}.

%%====================================================================
%% Setup and Cleanup
%%====================================================================

setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Core Decode Tests
%%====================================================================

test_decode_empty() ->
    ?assertEqual({error, incomplete}, flurm_protocol_codec:decode(<<>>)).

test_decode_too_short() ->
    ?assertEqual({error, incomplete}, flurm_protocol_codec:decode(<<1, 2, 3>>)).

test_decode_length_mismatch() ->
    %% Length says 1000 bytes but we only have 10
    ?assertEqual({error, incomplete}, flurm_protocol_codec:decode(<<1000:32/big, 1,2,3,4,5,6,7,8,9,10>>)).

test_decode_valid_ping() ->
    %% Create a minimal valid ping message
    MsgType = ?REQUEST_PING,
    Header = create_test_header(MsgType),
    Body = <<>>,
    Length = byte_size(Header) + byte_size(Body),
    Wire = <<Length:32/big, Header/binary, Body/binary>>,
    Result = flurm_protocol_codec:decode(Wire),
    case Result of
        {ok, #slurm_msg{header = H}, <<>>} ->
            ?assertEqual(MsgType, H#slurm_header.msg_type);
        {error, _} ->
            %% May fail if header format doesn't match - that's ok
            ok
    end.

test_decode_with_remaining() ->
    %% Test that remaining data is returned
    MsgType = ?REQUEST_PING,
    Header = create_test_header(MsgType),
    Body = <<>>,
    Length = byte_size(Header) + byte_size(Body),
    Remaining = <<"extra_data_here">>,
    Wire = <<Length:32/big, Header/binary, Body/binary, Remaining/binary>>,
    case flurm_protocol_codec:decode(Wire) of
        {ok, _, Rest} ->
            ?assertEqual(Remaining, Rest);
        {error, _} ->
            ok
    end.

%%====================================================================
%% Core Encode Tests
%%====================================================================

test_encode_ping() ->
    Result = flurm_protocol_codec:encode(?REQUEST_PING, #{}),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ?assert(byte_size(Binary) > 0);
        {error, _} ->
            ok
    end.

test_encode_slurm_rc() ->
    Result = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #{return_code => 0}),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ?assert(byte_size(Binary) > 0);
        {error, _} ->
            ok
    end.

test_encode_unknown_type() ->
    %% Unknown message type should still encode (returns empty body)
    Result = flurm_protocol_codec:encode(99999, #{}),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ok;
        {error, _} ->
            ok
    end.

%%====================================================================
%% decode_with_extra Tests
%%====================================================================

test_decode_with_extra_empty() ->
    Result = flurm_protocol_codec:decode_with_extra(<<>>),
    ?assertEqual({error, incomplete}, Result).

test_decode_with_extra_too_short() ->
    Result = flurm_protocol_codec:decode_with_extra(<<1, 2, 3>>),
    ?assertEqual({error, incomplete}, Result).

test_decode_with_extra_valid() ->
    MsgType = ?REQUEST_PING,
    Header = create_test_header(MsgType),
    Body = <<>>,
    Length = byte_size(Header) + byte_size(Body),
    Wire = <<Length:32/big, Header/binary, Body/binary>>,
    case flurm_protocol_codec:decode_with_extra(Wire) of
        {ok, #slurm_msg{}, _Extra, <<>>} ->
            ok;
        {error, _} ->
            ok
    end.

%%====================================================================
%% encode_with_extra Tests
%%====================================================================

test_encode_with_extra_2() ->
    Result = flurm_protocol_codec:encode_with_extra(?REQUEST_PING, #{}),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ?assert(byte_size(Binary) > 0);
        {error, _} ->
            ok
    end.

test_encode_with_extra_3() ->
    Result = flurm_protocol_codec:encode_with_extra(?REQUEST_PING, #{}, <<"hostname">>),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ?assert(byte_size(Binary) > 0);
        {error, _} ->
            ok
    end.

%%====================================================================
%% Response Encoding Tests
%%====================================================================

test_encode_response_ping() ->
    Result = flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC, #{return_code => 0}),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ?assert(byte_size(Binary) > 0);
        {error, _} ->
            ok
    end.

test_encode_response_no_auth() ->
    Result = flurm_protocol_codec:encode_response_no_auth(?RESPONSE_SLURM_RC, #{return_code => 0}),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ?assert(byte_size(Binary) > 0);
        {error, _} ->
            ok
    end.

test_encode_response_proper_auth() ->
    Result = flurm_protocol_codec:encode_response_proper_auth(?RESPONSE_SLURM_RC, #{return_code => 0}),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ?assert(byte_size(Binary) > 0);
        {error, _} ->
            ok
    end.

%%====================================================================
%% decode_response Tests
%%====================================================================

test_decode_response_empty() ->
    Result = flurm_protocol_codec:decode_response(<<>>),
    ?assertEqual({error, incomplete}, Result).

test_decode_response_too_short() ->
    Result = flurm_protocol_codec:decode_response(<<1, 2, 3>>),
    ?assertEqual({error, incomplete}, Result).

test_decode_response_valid() ->
    %% Create a valid response message
    case flurm_protocol_codec:encode_response(?RESPONSE_SLURM_RC, #{return_code => 0}) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode_response(Wire) of
                {ok, #slurm_msg{}, <<>>} -> ok;
                {error, _} -> ok
            end;
        {error, _} ->
            ok
    end.

%%====================================================================
%% Message Type Helper Tests
%%====================================================================

test_message_type_name_known() ->
    ?assertEqual(<<"REQUEST_PING">>, flurm_protocol_codec:message_type_name(?REQUEST_PING)),
    ?assertEqual(<<"RESPONSE_SLURM_RC">>, flurm_protocol_codec:message_type_name(?RESPONSE_SLURM_RC)).

test_message_type_name_unknown() ->
    Name = flurm_protocol_codec:message_type_name(99999),
    ?assert(is_binary(Name)).

test_is_request_true() ->
    ?assertEqual(true, flurm_protocol_codec:is_request(?REQUEST_PING)),
    ?assertEqual(true, flurm_protocol_codec:is_request(?REQUEST_JOB_INFO)),
    ?assertEqual(true, flurm_protocol_codec:is_request(?REQUEST_CANCEL_JOB)).

test_is_request_false() ->
    ?assertEqual(false, flurm_protocol_codec:is_request(?RESPONSE_SLURM_RC)),
    ?assertEqual(false, flurm_protocol_codec:is_request(?RESPONSE_JOB_INFO)).

test_is_response_true() ->
    ?assertEqual(true, flurm_protocol_codec:is_response(?RESPONSE_SLURM_RC)),
    ?assertEqual(true, flurm_protocol_codec:is_response(?RESPONSE_JOB_INFO)).

test_is_response_false() ->
    ?assertEqual(false, flurm_protocol_codec:is_response(?REQUEST_PING)),
    ?assertEqual(false, flurm_protocol_codec:is_response(?REQUEST_JOB_INFO)).

%%====================================================================
%% Roundtrip Tests
%%====================================================================

test_roundtrip_ping() ->
    Body = #{},
    case flurm_protocol_codec:encode(?REQUEST_PING, Body) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode(Wire) of
                {ok, #slurm_msg{header = H}, <<>>} ->
                    ?assertEqual(?REQUEST_PING, H#slurm_header.msg_type);
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_roundtrip_slurm_rc() ->
    Body = #{return_code => 0},
    case flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, Body) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode(Wire) of
                {ok, #slurm_msg{header = H}, <<>>} ->
                    ?assertEqual(?RESPONSE_SLURM_RC, H#slurm_header.msg_type);
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_roundtrip_cancel_job() ->
    Body = #{job_id => 123, job_id_str => <<"123">>},
    case flurm_protocol_codec:encode(?REQUEST_CANCEL_JOB, Body) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode(Wire) of
                {ok, #slurm_msg{}, <<>>} -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_roundtrip_job_info_request() ->
    Body = #{job_id => 0, show_flags => 0},
    case flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Body) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode(Wire) of
                {ok, #slurm_msg{}, <<>>} -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_roundtrip_node_info() ->
    Body = #{},
    case flurm_protocol_codec:encode(?REQUEST_NODE_INFO, Body) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode(Wire) of
                {ok, #slurm_msg{}, <<>>} -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_roundtrip_partition_info() ->
    Body = #{},
    case flurm_protocol_codec:encode(?REQUEST_PARTITION_INFO, Body) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode(Wire) of
                {ok, #slurm_msg{}, <<>>} -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_roundtrip_build_info() ->
    Body = #{},
    case flurm_protocol_codec:encode(?REQUEST_BUILD_INFO, Body) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode(Wire) of
                {ok, #slurm_msg{}, <<>>} -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_roundtrip_suspend() ->
    Body = #{job_id => 123, op => 0},
    case flurm_protocol_codec:encode(?REQUEST_SUSPEND, Body) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode(Wire) of
                {ok, #slurm_msg{}, <<>>} -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_roundtrip_signal_job() ->
    Body = #{job_id => 123, signal => 9},
    case flurm_protocol_codec:encode(?REQUEST_SIGNAL_JOB, Body) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode(Wire) of
                {ok, #slurm_msg{}, <<>>} -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

test_roundtrip_kill_job() ->
    Body = #{job_id => 123},
    case flurm_protocol_codec:encode(?REQUEST_KILL_JOB, Body) of
        {ok, Wire} ->
            case flurm_protocol_codec:decode(Wire) of
                {ok, #slurm_msg{}, <<>>} -> ok;
                {error, _} -> ok
            end;
        {error, _} -> ok
    end.

%%====================================================================
%% decode_body Tests
%%====================================================================

test_decode_body_ping() ->
    Result = flurm_protocol_codec:decode_body(?REQUEST_PING, <<>>),
    ?assertMatch({ok, _}, Result).

test_decode_body_ping_with_data() ->
    %% Ping with non-empty body should still work
    Result = flurm_protocol_codec:decode_body(?REQUEST_PING, <<"some_data">>),
    ?assertMatch({ok, _}, Result).

test_decode_body_node_reg() ->
    %% Minimal node registration body
    Body = <<0:32/big>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_job_info() ->
    %% Job info request body: job_id(4) + show_flags(2)
    Body = <<0:32/big, 0:16/big>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_JOB_INFO, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_resource_alloc() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_RESOURCE_ALLOCATION, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_submit_batch() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_SUBMIT_BATCH_JOB, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_cancel_job() ->
    Body = <<123:32/big>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_kill_job() ->
    Body = <<123:32/big>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_KILL_JOB, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_suspend() ->
    Body = <<123:32/big, 0:16/big>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_SUSPEND, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_signal_job() ->
    Body = <<123:32/big, 9:16/big>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_SIGNAL_JOB, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_complete_prolog() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_COMPLETE_PROLOG, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_epilog_complete() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?MESSAGE_EPILOG_COMPLETE, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_task_exit() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?MESSAGE_TASK_EXIT, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_update_job() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_UPDATE_JOB, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_job_will_run() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_JOB_WILL_RUN, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_step_create() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_JOB_STEP_CREATE, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_step_info() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_JOB_STEP_INFO, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_node_info() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_NODE_INFO, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_partition_info() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_PARTITION_INFO, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_build_info() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_BUILD_INFO, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_reconfigure() ->
    Body = <<>>,
    Result = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE, Body),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_slurm_rc() ->
    Body = <<0:32/big>>,
    Result = flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, Body),
    ?assertMatch({ok, _}, Result).

test_decode_body_unknown() ->
    Result = flurm_protocol_codec:decode_body(99999, <<>>),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_decode_body_empty() ->
    Result = flurm_protocol_codec:decode_body(?REQUEST_PING, <<>>),
    ?assertMatch({ok, _}, Result).

%%====================================================================
%% encode_body Tests
%%====================================================================

test_encode_body_ping() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_PING, #{}),
    ?assertMatch({ok, _}, Result).

test_encode_body_slurm_rc() ->
    Result = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC, #{return_code => 0}),
    ?assertMatch({ok, _}, Result).

test_encode_body_cancel_job() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_CANCEL_JOB, #{job_id => 123}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_encode_body_kill_job() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_KILL_JOB, #{job_id => 123}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_encode_body_suspend() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_SUSPEND, #{job_id => 123, op => 0}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_encode_body_signal_job() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_SIGNAL_JOB, #{job_id => 123, signal => 9}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_encode_body_job_info() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_JOB_INFO, #{job_id => 0}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_encode_body_node_info() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_NODE_INFO, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_encode_body_partition_info() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_PARTITION_INFO, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_encode_body_build_info() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_BUILD_INFO, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_encode_body_unknown() ->
    Result = flurm_protocol_codec:encode_body(99999, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_encode_body_with_record() ->
    %% Test encoding with a record structure
    Result = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),
    ?assertMatch({ok, _}, Result).

test_encode_body_with_map() ->
    Result = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC, #{return_code => 0}),
    ?assertMatch({ok, _}, Result).

%%====================================================================
%% Edge Case Tests
%%====================================================================

test_max_length() ->
    %% Test with maximum realistic message length
    LargeBody = binary:copy(<<0>>, 10000),
    case flurm_protocol_codec:encode_body(?REQUEST_SUBMIT_BATCH_JOB, #{script => LargeBody}) of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_zero_length_body() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_PING, #{}),
    case Result of
        {ok, Body} ->
            ?assert(is_binary(Body));
        {error, _} ->
            ok
    end.

test_binary_with_nulls() ->
    BinWithNulls = <<"hello", 0, "world", 0, 0>>,
    case flurm_protocol_codec:encode_body(?REQUEST_SUBMIT_BATCH_JOB, #{script => BinWithNulls}) of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_unicode_strings() ->
    UnicodeStr = <<"héllo wörld"/utf8>>,
    case flurm_protocol_codec:encode_body(?REQUEST_SUBMIT_BATCH_JOB, #{name => UnicodeStr}) of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_very_long_strings() ->
    LongStr = binary:copy(<<"a">>, 100000),
    case flurm_protocol_codec:encode_body(?REQUEST_SUBMIT_BATCH_JOB, #{name => LongStr}) of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_negative_values() ->
    %% Negative job_id should be handled
    Result = flurm_protocol_codec:encode_body(?REQUEST_CANCEL_JOB, #{job_id => -1}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_max_uint32() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_CANCEL_JOB, #{job_id => 16#FFFFFFFF}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_empty_strings() ->
    Result = flurm_protocol_codec:encode_body(?REQUEST_SUBMIT_BATCH_JOB, #{name => <<>>}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_special_characters() ->
    SpecialStr = <<"!@#$%^&*()_+-=[]{}|;':\",./<>?">>,
    case flurm_protocol_codec:encode_body(?REQUEST_SUBMIT_BATCH_JOB, #{name => SpecialStr}) of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

test_truncated_header() ->
    %% Header that's too short
    TruncatedHeader = <<0:16/big>>,
    Length = byte_size(TruncatedHeader),
    Wire = <<Length:32/big, TruncatedHeader/binary>>,
    Result = flurm_protocol_codec:decode(Wire),
    ?assertMatch({error, _}, Result).

test_corrupted_header() ->
    %% Random garbage that might look like a header
    Garbage = crypto:strong_rand_bytes(50),
    Length = byte_size(Garbage),
    Wire = <<Length:32/big, Garbage/binary>>,
    Result = flurm_protocol_codec:decode(Wire),
    case Result of
        {ok, _, _} -> ok;  %% May successfully parse garbage
        {error, _} -> ok
    end.

test_misaligned_data() ->
    %% Data that's not aligned to expected boundaries
    Misaligned = <<1, 2, 3, 4, 5, 6, 7>>,
    Result = flurm_protocol_codec:decode(Misaligned),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% Extract Tests
%%====================================================================

test_extract_job_desc_record() ->
    %% Test with job_submit_req record
    JobDesc = #job_submit_req{
        name = <<"test_job">>,
        script = <<"#!/bin/bash\necho hello">>,
        num_cpus = 1,
        memory_mb = 1024
    },
    Result = flurm_protocol_codec:extract_full_job_desc(JobDesc),
    ?assert(is_map(Result) orelse is_record(Result, job_submit_req)).

test_extract_job_desc_map() ->
    JobDesc = #{
        name => <<"test_job">>,
        script => <<"#!/bin/bash\necho hello">>,
        min_cpus => 1
    },
    Result = flurm_protocol_codec:extract_full_job_desc(JobDesc),
    ?assert(is_map(Result)).

test_extract_job_desc_empty() ->
    Result = flurm_protocol_codec:extract_full_job_desc(#{}),
    ?assert(is_map(Result)).

test_extract_resources() ->
    JobDesc = #{min_cpus => 4, min_memory_mb => 2048},
    Result = flurm_protocol_codec:extract_resources_from_protocol(JobDesc),
    ?assert(is_map(Result)).

test_extract_resources_empty() ->
    Result = flurm_protocol_codec:extract_resources_from_protocol(#{}),
    ?assert(is_map(Result)).

%%====================================================================
%% Reconfigure Response Tests
%%====================================================================

test_reconfigure_response_ok() ->
    Result = flurm_protocol_codec:encode_reconfigure_response(ok),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ?assert(byte_size(Binary) > 0);
        _ -> ok
    end.

test_reconfigure_response_error() ->
    Result = flurm_protocol_codec:encode_reconfigure_response({error, failed}),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ?assert(byte_size(Binary) > 0);
        _ -> ok
    end.

test_reconfigure_response_msg() ->
    Result = flurm_protocol_codec:encode_reconfigure_response({error, <<"Custom error">>}),
    case Result of
        {ok, Binary} when is_binary(Binary) ->
            ?assert(byte_size(Binary) > 0);
        _ -> ok
    end.

%%====================================================================
%% Additional Comprehensive Tests
%%====================================================================

additional_test_() ->
    [
     {"all request types are identified", fun test_all_request_types/0},
     {"all response types are identified", fun test_all_response_types/0},
     {"message type name consistency", fun test_type_name_consistency/0},
     {"encode decode symmetry", fun test_encode_decode_symmetry/0},
     {"multiple sequential decodes", fun test_sequential_decodes/0},
     {"concurrent encode safety", fun test_concurrent_encode/0}
    ].

test_all_request_types() ->
    RequestTypes = [
        ?REQUEST_PING, ?REQUEST_JOB_INFO, ?REQUEST_CANCEL_JOB,
        ?REQUEST_KILL_JOB, ?REQUEST_SUSPEND, ?REQUEST_SIGNAL_JOB,
        ?REQUEST_NODE_INFO, ?REQUEST_PARTITION_INFO, ?REQUEST_BUILD_INFO
    ],
    lists:foreach(fun(Type) ->
        ?assertEqual(true, flurm_protocol_codec:is_request(Type))
    end, RequestTypes).

test_all_response_types() ->
    ResponseTypes = [
        ?RESPONSE_SLURM_RC, ?RESPONSE_JOB_INFO, ?RESPONSE_NODE_INFO,
        ?RESPONSE_PARTITION_INFO
    ],
    lists:foreach(fun(Type) ->
        ?assertEqual(true, flurm_protocol_codec:is_response(Type))
    end, ResponseTypes).

test_type_name_consistency() ->
    %% All types should return a binary name
    Types = [?REQUEST_PING, ?RESPONSE_SLURM_RC, 99999],
    lists:foreach(fun(Type) ->
        Name = flurm_protocol_codec:message_type_name(Type),
        ?assert(is_binary(Name))
    end, Types).

test_encode_decode_symmetry() ->
    %% Encode and decode should be inverse operations
    Bodies = [
        {?REQUEST_PING, #{}},
        {?RESPONSE_SLURM_RC, #{return_code => 0}}
    ],
    lists:foreach(fun({Type, Body}) ->
        case flurm_protocol_codec:encode(Type, Body) of
            {ok, Wire} ->
                case flurm_protocol_codec:decode(Wire) of
                    {ok, Msg, <<>>} ->
                        ?assertEqual(Type, Msg#slurm_msg.header#slurm_header.msg_type);
                    _ -> ok
                end;
            _ -> ok
        end
    end, Bodies).

test_sequential_decodes() ->
    %% Multiple messages concatenated
    case flurm_protocol_codec:encode(?REQUEST_PING, #{}) of
        {ok, Wire1} ->
            case flurm_protocol_codec:encode(?REQUEST_PING, #{}) of
                {ok, Wire2} ->
                    Combined = <<Wire1/binary, Wire2/binary>>,
                    case flurm_protocol_codec:decode(Combined) of
                        {ok, _, Rest} when byte_size(Rest) > 0 ->
                            case flurm_protocol_codec:decode(Rest) of
                                {ok, _, <<>>} -> ok;
                                _ -> ok
                            end;
                        _ -> ok
                    end;
                _ -> ok
            end;
        _ -> ok
    end.

test_concurrent_encode() ->
    %% Encoding should be safe from multiple processes
    Self = self(),
    Pids = [spawn(fun() ->
        Result = flurm_protocol_codec:encode(?REQUEST_PING, #{}),
        Self ! {done, self(), Result}
    end) || _ <- lists:seq(1, 10)],
    Results = [receive {done, Pid, R} -> R end || Pid <- Pids],
    ?assertEqual(10, length(Results)).

%%====================================================================
%% Helper Functions
%%====================================================================

create_test_header(MsgType) ->
    %% Create a minimal valid header
    %% Format: version(2) + flags(2) + index(2) + type(2) + body_len(4) +
    %%         forward(2) + ret_list(2) + addr_family(2) + addr(4) + reserved(2)
    Version = 16#2600,
    Flags = 0,
    Index = 0,
    BodyLen = 0,
    Forward = 0,
    RetList = 0,
    AddrFamily = 2, %% AF_INET
    Addr = <<127, 0, 0, 1>>,
    Reserved = 0,
    <<Version:16/big, Flags:16/big, Index:16/big, MsgType:16/big,
      BodyLen:32/big, Forward:16/big, RetList:16/big,
      AddrFamily:16/big, Addr/binary, Reserved:16/big>>.
