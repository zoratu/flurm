%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_protocol_codec module
%%%
%%% These tests do NOT use meck - they test the module directly with
%%% various inputs to achieve high coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_codec_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test: message_type_name/1
%%%===================================================================

message_type_name_known_types_test() ->
    %% Test all known message types
    ?assertEqual(request_node_registration_status, flurm_protocol_codec:message_type_name(?REQUEST_NODE_REGISTRATION_STATUS)),
    ?assertEqual(message_node_registration_status, flurm_protocol_codec:message_type_name(?MESSAGE_NODE_REGISTRATION_STATUS)),
    ?assertEqual(request_reconfigure, flurm_protocol_codec:message_type_name(?REQUEST_RECONFIGURE)),
    ?assertEqual(request_reconfigure_with_config, flurm_protocol_codec:message_type_name(?REQUEST_RECONFIGURE_WITH_CONFIG)),
    ?assertEqual(request_shutdown, flurm_protocol_codec:message_type_name(?REQUEST_SHUTDOWN)),
    ?assertEqual(request_ping, flurm_protocol_codec:message_type_name(?REQUEST_PING)),
    ?assertEqual(request_build_info, flurm_protocol_codec:message_type_name(?REQUEST_BUILD_INFO)),
    ?assertEqual(request_job_info, flurm_protocol_codec:message_type_name(?REQUEST_JOB_INFO)),
    ?assertEqual(response_job_info, flurm_protocol_codec:message_type_name(?RESPONSE_JOB_INFO)),
    ?assertEqual(request_node_info, flurm_protocol_codec:message_type_name(?REQUEST_NODE_INFO)),
    ?assertEqual(response_node_info, flurm_protocol_codec:message_type_name(?RESPONSE_NODE_INFO)),
    ?assertEqual(request_partition_info, flurm_protocol_codec:message_type_name(?REQUEST_PARTITION_INFO)),
    ?assertEqual(response_partition_info, flurm_protocol_codec:message_type_name(?RESPONSE_PARTITION_INFO)),
    ?assertEqual(request_resource_allocation, flurm_protocol_codec:message_type_name(?REQUEST_RESOURCE_ALLOCATION)),
    ?assertEqual(response_resource_allocation, flurm_protocol_codec:message_type_name(?RESPONSE_RESOURCE_ALLOCATION)),
    ?assertEqual(request_submit_batch_job, flurm_protocol_codec:message_type_name(?REQUEST_SUBMIT_BATCH_JOB)),
    ?assertEqual(response_submit_batch_job, flurm_protocol_codec:message_type_name(?RESPONSE_SUBMIT_BATCH_JOB)),
    ?assertEqual(request_cancel_job, flurm_protocol_codec:message_type_name(?REQUEST_CANCEL_JOB)),
    ?assertEqual(request_kill_job, flurm_protocol_codec:message_type_name(?REQUEST_KILL_JOB)),
    ?assertEqual(request_update_job, flurm_protocol_codec:message_type_name(?REQUEST_UPDATE_JOB)),
    ?assertEqual(request_job_will_run, flurm_protocol_codec:message_type_name(?REQUEST_JOB_WILL_RUN)),
    ?assertEqual(response_job_will_run, flurm_protocol_codec:message_type_name(?RESPONSE_JOB_WILL_RUN)),
    ?assertEqual(request_job_step_create, flurm_protocol_codec:message_type_name(?REQUEST_JOB_STEP_CREATE)),
    ?assertEqual(response_job_step_create, flurm_protocol_codec:message_type_name(?RESPONSE_JOB_STEP_CREATE)),
    ?assertEqual(response_slurm_rc, flurm_protocol_codec:message_type_name(?RESPONSE_SLURM_RC)),
    ?assertEqual(request_reservation_info, flurm_protocol_codec:message_type_name(?REQUEST_RESERVATION_INFO)),
    ?assertEqual(response_reservation_info, flurm_protocol_codec:message_type_name(?RESPONSE_RESERVATION_INFO)),
    ?assertEqual(request_license_info, flurm_protocol_codec:message_type_name(?REQUEST_LICENSE_INFO)),
    ?assertEqual(response_license_info, flurm_protocol_codec:message_type_name(?RESPONSE_LICENSE_INFO)),
    ?assertEqual(request_topo_info, flurm_protocol_codec:message_type_name(?REQUEST_TOPO_INFO)),
    ?assertEqual(response_topo_info, flurm_protocol_codec:message_type_name(?RESPONSE_TOPO_INFO)),
    ?assertEqual(request_front_end_info, flurm_protocol_codec:message_type_name(?REQUEST_FRONT_END_INFO)),
    ?assertEqual(response_front_end_info, flurm_protocol_codec:message_type_name(?RESPONSE_FRONT_END_INFO)),
    ?assertEqual(request_burst_buffer_info, flurm_protocol_codec:message_type_name(?REQUEST_BURST_BUFFER_INFO)),
    ?assertEqual(response_burst_buffer_info, flurm_protocol_codec:message_type_name(?RESPONSE_BURST_BUFFER_INFO)),
    ?assertEqual(response_build_info, flurm_protocol_codec:message_type_name(?RESPONSE_BUILD_INFO)),
    ?assertEqual(request_config_info, flurm_protocol_codec:message_type_name(?REQUEST_CONFIG_INFO)),
    ?assertEqual(response_config_info, flurm_protocol_codec:message_type_name(?RESPONSE_CONFIG_INFO)),
    ?assertEqual(request_stats_info, flurm_protocol_codec:message_type_name(?REQUEST_STATS_INFO)),
    ?assertEqual(response_stats_info, flurm_protocol_codec:message_type_name(?RESPONSE_STATS_INFO)).

message_type_name_unknown_test() ->
    %% Unknown message types should return tuple
    ?assertEqual({unknown, 0}, flurm_protocol_codec:message_type_name(0)),
    ?assertEqual({unknown, 9999}, flurm_protocol_codec:message_type_name(9999)),
    ?assertEqual({unknown, 12345}, flurm_protocol_codec:message_type_name(12345)).

%%%===================================================================
%%% Test: is_request/1
%%%===================================================================

is_request_true_test() ->
    %% Test known request types
    ?assert(flurm_protocol_codec:is_request(?REQUEST_NODE_REGISTRATION_STATUS)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_RECONFIGURE)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_RECONFIGURE_WITH_CONFIG)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_SHUTDOWN)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_PING)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_BUILD_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_JOB_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_JOB_INFO_SINGLE)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_NODE_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_PARTITION_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_RESOURCE_ALLOCATION)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_SUBMIT_BATCH_JOB)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_BATCH_JOB_LAUNCH)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_CANCEL_JOB)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_UPDATE_JOB)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_JOB_STEP_CREATE)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_JOB_STEP_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_STEP_COMPLETE)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_LAUNCH_TASKS)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_SIGNAL_TASKS)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_TERMINATE_TASKS)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_KILL_JOB)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_RESERVATION_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_LICENSE_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_TOPO_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_FRONT_END_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_BURST_BUFFER_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_CONFIG_INFO)),
    ?assert(flurm_protocol_codec:is_request(?REQUEST_STATS_INFO)).

is_request_false_test() ->
    %% Test non-request types
    ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_SLURM_RC)),
    ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_JOB_INFO)),
    ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_NODE_INFO)),
    ?assertNot(flurm_protocol_codec:is_request(?RESPONSE_SUBMIT_BATCH_JOB)),
    ?assertNot(flurm_protocol_codec:is_request(0)),
    ?assertNot(flurm_protocol_codec:is_request(9999)).

is_request_range_test() ->
    %% Test the fallback range check (1001-1029)
    ?assert(flurm_protocol_codec:is_request(1001)),
    ?assert(flurm_protocol_codec:is_request(1010)),
    ?assert(flurm_protocol_codec:is_request(1029)),
    ?assertNot(flurm_protocol_codec:is_request(1030)),
    ?assertNot(flurm_protocol_codec:is_request(1000)).

%%%===================================================================
%%% Test: is_response/1
%%%===================================================================

is_response_true_test() ->
    %% Test known response types
    ?assert(flurm_protocol_codec:is_response(?MESSAGE_NODE_REGISTRATION_STATUS)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_BUILD_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_JOB_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_NODE_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_PARTITION_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_RESOURCE_ALLOCATION)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_SUBMIT_BATCH_JOB)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_CANCEL_JOB_STEP)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_JOB_STEP_CREATE)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_JOB_STEP_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_STEP_LAYOUT)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_LAUNCH_TASKS)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_SLURM_RC)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_SLURM_RC_MSG)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_RESERVATION_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_LICENSE_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_TOPO_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_FRONT_END_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_BURST_BUFFER_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_CONFIG_INFO)),
    ?assert(flurm_protocol_codec:is_response(?RESPONSE_STATS_INFO)).

is_response_false_test() ->
    %% Test non-response types
    ?assertNot(flurm_protocol_codec:is_response(?REQUEST_PING)),
    ?assertNot(flurm_protocol_codec:is_response(?REQUEST_JOB_INFO)),
    ?assertNot(flurm_protocol_codec:is_response(0)),
    ?assertNot(flurm_protocol_codec:is_response(9999)).

%%%===================================================================
%%% Test: decode_body/2 - Various message types
%%%===================================================================

decode_body_ping_test() ->
    %% REQUEST_PING with empty body
    {ok, #ping_request{}} = flurm_protocol_codec:decode_body(?REQUEST_PING, <<>>),

    %% REQUEST_PING with non-empty body (should still work)
    {ok, #ping_request{}} = flurm_protocol_codec:decode_body(?REQUEST_PING, <<"anything">>).

decode_body_node_registration_test() ->
    %% Empty body
    {ok, #node_registration_request{status_only = false}} =
        flurm_protocol_codec:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, <<>>),

    %% Status only = true (non-zero byte)
    {ok, #node_registration_request{status_only = true}} =
        flurm_protocol_codec:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, <<1, 0, 0>>),

    %% Status only = false (zero byte)
    {ok, #node_registration_request{status_only = false}} =
        flurm_protocol_codec:decode_body(?REQUEST_NODE_REGISTRATION_STATUS, <<0, 1, 2>>).

decode_body_job_info_request_test() ->
    %% Full format
    Binary1 = <<100:32/big, 123:32/big, 1000:32/big, "extra">>,
    {ok, Req1} = flurm_protocol_codec:decode_body(?REQUEST_JOB_INFO, Binary1),
    ?assertEqual(100, Req1#job_info_request.show_flags),
    ?assertEqual(123, Req1#job_info_request.job_id),
    ?assertEqual(1000, Req1#job_info_request.user_id),

    %% Shorter formats
    {ok, Req2} = flurm_protocol_codec:decode_body(?REQUEST_JOB_INFO, <<50:32/big, 456:32/big>>),
    ?assertEqual(50, Req2#job_info_request.show_flags),
    ?assertEqual(456, Req2#job_info_request.job_id),

    {ok, Req3} = flurm_protocol_codec:decode_body(?REQUEST_JOB_INFO, <<75:32/big>>),
    ?assertEqual(75, Req3#job_info_request.show_flags),

    %% Empty
    {ok, #job_info_request{}} = flurm_protocol_codec:decode_body(?REQUEST_JOB_INFO, <<>>).

decode_body_cancel_job_test() ->
    %% Format with job_id_str (string is packed after the 4 uint32 values)
    %% Note: The implementation requires a properly packed string when 16+ bytes
    JobIdStr = <<"12345">>,
    StrLen = byte_size(JobIdStr) + 1,
    Binary1 = <<100:32/big, 0:32/big, 9:32/big, 0:32/big, StrLen:32/big, JobIdStr/binary, 0:8>>,
    {ok, Req1} = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, Binary1),
    ?assertEqual(100, Req1#cancel_job_request.job_id),
    ?assertEqual(0, Req1#cancel_job_request.step_id),
    ?assertEqual(9, Req1#cancel_job_request.signal),
    ?assertEqual(0, Req1#cancel_job_request.flags),
    ?assertEqual(JobIdStr, Req1#cancel_job_request.job_id_str),

    %% Format with 4 uint32 values and empty string (NULL string encoded as 0 length)
    Binary2 = <<200:32/big, 1:32/big, 15:32/big, 4:32/big, 0:32/big>>,  % 0:32/big is empty string
    {ok, Req2} = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, Binary2),
    ?assertEqual(200, Req2#cancel_job_request.job_id),
    ?assertEqual(1, Req2#cancel_job_request.step_id),
    ?assertEqual(15, Req2#cancel_job_request.signal),
    ?assertEqual(4, Req2#cancel_job_request.flags),

    %% Format with 3 uint32 values
    Binary3 = <<250:32/big, 2:32/big, 11:32/big>>,
    {ok, Req3} = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, Binary3),
    ?assertEqual(250, Req3#cancel_job_request.job_id),
    ?assertEqual(2, Req3#cancel_job_request.step_id),
    ?assertEqual(11, Req3#cancel_job_request.signal),

    %% Minimal format (just job_id)
    {ok, Req4} = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, <<300:32/big>>),
    ?assertEqual(300, Req4#cancel_job_request.job_id),

    %% Empty
    {ok, #cancel_job_request{}} = flurm_protocol_codec:decode_body(?REQUEST_CANCEL_JOB, <<>>).

decode_body_slurm_rc_response_test() ->
    %% Positive return code
    {ok, #slurm_rc_response{return_code = 0}} =
        flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<0:32/big-signed>>),

    %% Negative return code
    {ok, #slurm_rc_response{return_code = -1}} =
        flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<-1:32/big-signed>>),

    %% With extra data
    {ok, #slurm_rc_response{return_code = 42}} =
        flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<42:32/big-signed, "extra">>),

    %% Empty
    {ok, #slurm_rc_response{return_code = 0}} =
        flurm_protocol_codec:decode_body(?RESPONSE_SLURM_RC, <<>>).

decode_body_batch_job_response_test() ->
    %% Full format
    UserMsg = <<"Job submitted">>,
    MsgLen = byte_size(UserMsg) + 1,
    Binary1 = <<123:32/big, 0:32/big, 0:32/big, MsgLen:32/big, UserMsg/binary, 0:8>>,
    {ok, Resp1} = flurm_protocol_codec:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary1),
    ?assertEqual(123, Resp1#batch_job_response.job_id),
    ?assertEqual(0, Resp1#batch_job_response.step_id),
    ?assertEqual(0, Resp1#batch_job_response.error_code),
    ?assertEqual(UserMsg, Resp1#batch_job_response.job_submit_user_msg),

    %% Shorter formats
    {ok, Resp2} = flurm_protocol_codec:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, <<456:32/big, 1:32/big>>),
    ?assertEqual(456, Resp2#batch_job_response.job_id),
    ?assertEqual(1, Resp2#batch_job_response.step_id),

    {ok, Resp3} = flurm_protocol_codec:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, <<789:32/big>>),
    ?assertEqual(789, Resp3#batch_job_response.job_id),

    %% Empty
    {ok, #batch_job_response{}} = flurm_protocol_codec:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, <<>>).

decode_body_job_info_response_test() ->
    %% Empty response
    {ok, #job_info_response{}} = flurm_protocol_codec:decode_body(?RESPONSE_JOB_INFO, <<>>),

    %% Response with header only (no jobs)
    {ok, Resp} = flurm_protocol_codec:decode_body(?RESPONSE_JOB_INFO, <<1234567890:64/big, 0:32/big>>),
    ?assertEqual(1234567890, Resp#job_info_response.last_update),
    ?assertEqual(0, Resp#job_info_response.job_count),
    ?assertEqual([], Resp#job_info_response.jobs).

decode_body_unknown_type_test() ->
    %% Unknown message types return raw binary
    {ok, <<"raw data">>} = flurm_protocol_codec:decode_body(99999, <<"raw data">>),
    {ok, <<>>} = flurm_protocol_codec:decode_body(88888, <<>>).

decode_body_build_info_test() ->
    %% REQUEST_BUILD_INFO returns empty map
    {ok, #{}} = flurm_protocol_codec:decode_body(?REQUEST_BUILD_INFO, <<"anything">>),
    {ok, #{}} = flurm_protocol_codec:decode_body(?REQUEST_BUILD_INFO, <<>>).

decode_body_config_info_test() ->
    %% REQUEST_CONFIG_INFO returns empty map
    {ok, #{}} = flurm_protocol_codec:decode_body(?REQUEST_CONFIG_INFO, <<"anything">>).

decode_body_license_info_request_test() ->
    %% With show_flags
    {ok, #license_info_request{show_flags = 123}} =
        flurm_protocol_codec:decode_body(?REQUEST_LICENSE_INFO, <<123:32/big, "extra">>),

    %% Empty
    {ok, #license_info_request{}} = flurm_protocol_codec:decode_body(?REQUEST_LICENSE_INFO, <<>>).

decode_body_topo_info_request_test() ->
    %% With show_flags
    {ok, #topo_info_request{show_flags = 456}} =
        flurm_protocol_codec:decode_body(?REQUEST_TOPO_INFO, <<456:32/big, "extra">>),

    %% Empty
    {ok, #topo_info_request{}} = flurm_protocol_codec:decode_body(?REQUEST_TOPO_INFO, <<>>).

decode_body_front_end_info_request_test() ->
    %% With show_flags
    {ok, #front_end_info_request{show_flags = 789}} =
        flurm_protocol_codec:decode_body(?REQUEST_FRONT_END_INFO, <<789:32/big, "extra">>),

    %% Empty
    {ok, #front_end_info_request{}} = flurm_protocol_codec:decode_body(?REQUEST_FRONT_END_INFO, <<>>).

decode_body_burst_buffer_info_request_test() ->
    %% With show_flags
    {ok, #burst_buffer_info_request{show_flags = 111}} =
        flurm_protocol_codec:decode_body(?REQUEST_BURST_BUFFER_INFO, <<111:32/big, "extra">>),

    %% Empty
    {ok, #burst_buffer_info_request{}} = flurm_protocol_codec:decode_body(?REQUEST_BURST_BUFFER_INFO, <<>>).

decode_body_reservation_info_request_test() ->
    %% With show_flags and name
    Name = <<"resv1">>,
    NameLen = byte_size(Name) + 1,
    Binary = <<222:32/big, NameLen:32/big, Name/binary, 0:8>>,
    {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_RESERVATION_INFO, Binary),
    ?assertEqual(222, Req#reservation_info_request.show_flags),
    ?assertEqual(Name, Req#reservation_info_request.reservation_name),

    %% Empty
    {ok, #reservation_info_request{}} = flurm_protocol_codec:decode_body(?REQUEST_RESERVATION_INFO, <<>>).

decode_body_job_step_info_request_test() ->
    %% Full format
    {ok, Req1} = flurm_protocol_codec:decode_body(?REQUEST_JOB_STEP_INFO, <<1:32/big, 100:32/big, 5:32/big, "extra">>),
    ?assertEqual(1, Req1#job_step_info_request.show_flags),
    ?assertEqual(100, Req1#job_step_info_request.job_id),
    ?assertEqual(5, Req1#job_step_info_request.step_id),

    %% Without step_id
    {ok, Req2} = flurm_protocol_codec:decode_body(?REQUEST_JOB_STEP_INFO, <<2:32/big, 200:32/big>>),
    ?assertEqual(2, Req2#job_step_info_request.show_flags),
    ?assertEqual(200, Req2#job_step_info_request.job_id),

    %% Just show_flags
    {ok, Req3} = flurm_protocol_codec:decode_body(?REQUEST_JOB_STEP_INFO, <<3:32/big>>),
    ?assertEqual(3, Req3#job_step_info_request.show_flags),

    %% Empty
    {ok, #job_step_info_request{}} = flurm_protocol_codec:decode_body(?REQUEST_JOB_STEP_INFO, <<>>).

decode_body_reconfigure_test() ->
    %% Empty
    {ok, #reconfigure_request{}} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE, <<>>),

    %% With flags
    {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE, <<123:32/big, "extra">>),
    ?assertEqual(123, Req#reconfigure_request.flags),

    %% Other binary
    {ok, #reconfigure_request{}} = flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE, <<"abc">>).

decode_body_shutdown_test() ->
    %% Returns the raw binary
    {ok, <<"shutdown data">>} = flurm_protocol_codec:decode_body(?REQUEST_SHUTDOWN, <<"shutdown data">>).

decode_body_stats_info_request_test() ->
    %% Returns the raw binary
    {ok, <<"stats data">>} = flurm_protocol_codec:decode_body(?REQUEST_STATS_INFO, <<"stats data">>).

%%%===================================================================
%%% Test: encode_body/2 - Various message types
%%%===================================================================

encode_body_ping_test() ->
    %% Ping request encodes to empty binary
    {ok, <<>>} = flurm_protocol_codec:encode_body(?REQUEST_PING, #ping_request{}).

encode_body_node_registration_test() ->
    %% Status only = false
    {ok, <<0:8>>} = flurm_protocol_codec:encode_body(?REQUEST_NODE_REGISTRATION_STATUS,
                                                      #node_registration_request{status_only = false}),
    %% Status only = true
    {ok, <<1:8>>} = flurm_protocol_codec:encode_body(?REQUEST_NODE_REGISTRATION_STATUS,
                                                      #node_registration_request{status_only = true}).

encode_body_job_info_request_test() ->
    Req = #job_info_request{
        show_flags = 100,
        job_id = 123,
        user_id = 1000
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_JOB_INFO, Req),
    ?assertEqual(<<100:32/big, 123:32/big, 1000:32/big>>, Binary).

encode_body_slurm_rc_response_test() ->
    %% Positive return code
    {ok, <<0:32/big-signed>>} = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC,
                                                                  #slurm_rc_response{return_code = 0}),
    %% Negative return code
    {ok, <<-1:32/big-signed>>} = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC,
                                                                   #slurm_rc_response{return_code = -1}),
    %% Positive non-zero
    {ok, <<42:32/big-signed>>} = flurm_protocol_codec:encode_body(?RESPONSE_SLURM_RC,
                                                                   #slurm_rc_response{return_code = 42}).

encode_body_batch_job_response_test() ->
    Resp = #batch_job_response{
        job_id = 123,
        step_id = 0,
        error_code = 0,
        job_submit_user_msg = <<"Success">>
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Resp),
    ?assert(is_binary(Binary)),
    %% Check job_id is at the start
    <<123:32/big, _/binary>> = Binary.

encode_body_cancel_job_request_test() ->
    Req = #cancel_job_request{
        job_id = 100,
        job_id_str = <<"100">>,
        step_id = 0,
        signal = 9,
        flags = 0
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_CANCEL_JOB, Req),
    ?assert(is_binary(Binary)),
    %% Check job_id is at the start
    <<100:32/big, _/binary>> = Binary.

encode_body_kill_job_request_test() ->
    Req = #kill_job_request{
        job_id_str = <<"12345">>,
        step_id = -1,
        signal = 15,
        flags = 1,
        sibling = <<>>
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_KILL_JOB, Req),
    ?assert(is_binary(Binary)).

encode_body_batch_job_request_test() ->
    Req = #batch_job_request{
        account = <<>>,
        acctg_freq = <<>>,
        admin_comment = <<>>,
        alloc_node = <<>>,
        alloc_resp_port = 0,
        alloc_sid = 0,
        argc = 0,
        argv = [],
        name = <<"test_job">>,
        partition = <<"batch">>,
        script = <<"#!/bin/bash\necho hello">>,
        work_dir = <<"/tmp">>,
        min_nodes = 1,
        max_nodes = 1,
        min_cpus = 1,
        num_tasks = 1,
        cpus_per_task = 1,
        time_limit = 3600,
        priority = 0,
        user_id = 1000,
        group_id = 1000
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_SUBMIT_BATCH_JOB, Req),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 0).

encode_body_raw_binary_test() ->
    %% Unknown message types with binary body should pass through
    {ok, <<"raw data">>} = flurm_protocol_codec:encode_body(99999, <<"raw data">>),
    {ok, <<>>} = flurm_protocol_codec:encode_body(88888, <<>>).

encode_body_unsupported_test() ->
    %% Non-binary, non-record body for unknown type
    {error, {unsupported_message_type, 77777, not_a_record}} =
        flurm_protocol_codec:encode_body(77777, not_a_record).

encode_body_node_info_request_test() ->
    Req = #node_info_request{
        show_flags = 123,
        node_name = <<"node1">>
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_NODE_INFO, Req),
    ?assert(is_binary(Binary)),
    <<123:32/big, _/binary>> = Binary.

encode_body_node_info_request_default_test() ->
    %% Non-record defaults to show_flags=0, empty name
    {ok, <<0:32/big, 0:32/big>>} = flurm_protocol_codec:encode_body(?REQUEST_NODE_INFO, default).

encode_body_partition_info_request_test() ->
    Req = #partition_info_request{
        show_flags = 456,
        partition_name = <<"compute">>
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_PARTITION_INFO, Req),
    ?assert(is_binary(Binary)),
    <<456:32/big, _/binary>> = Binary.

encode_body_partition_info_request_default_test() ->
    {ok, <<0:32/big, 0:32/big>>} = flurm_protocol_codec:encode_body(?REQUEST_PARTITION_INFO, default).

encode_body_job_will_run_response_test() ->
    Resp = #job_will_run_response{
        job_id = 123,
        start_time = 1700000000,
        node_list = <<"node[1-4]">>,
        proc_cnt = 16,
        error_code = 0
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_JOB_WILL_RUN, Resp),
    ?assert(is_binary(Binary)),
    <<123:32/big, _/binary>> = Binary.

encode_body_job_step_create_response_test() ->
    Resp = #job_step_create_response{
        job_step_id = 5,
        error_code = 0,
        error_msg = <<>>
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_JOB_STEP_CREATE, Resp),
    ?assert(is_binary(Binary)),
    <<5:32/big, _/binary>> = Binary.

encode_body_job_step_info_response_test() ->
    Resp = #job_step_info_response{
        last_update = 1700000000,
        step_count = 0,
        steps = []
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_body_resource_allocation_response_error_test() ->
    Resp = #resource_allocation_response{
        error_code = -1
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp),
    ?assertEqual(<<-1:32/big-signed>>, Binary).

encode_body_resource_allocation_response_success_test() ->
    Resp = #resource_allocation_response{
        job_id = 123,
        node_list = <<"node1">>,
        num_nodes = 1,
        partition = <<"batch">>,
        error_code = 0,
        job_submit_user_msg = <<>>,
        cpus_per_node = [4],
        num_cpu_groups = 1
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Test: encode/2 and decode/1 - Round trip
%%%===================================================================

encode_decode_ping_roundtrip_test() ->
    %% Encode a ping request
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_PING, #ping_request{}),

    %% Verify it has the right structure
    <<Length:32/big, _Header:10/binary, _Body/binary>> = Binary,
    ?assertEqual(10, Length),  % Header only, no body

    %% Decode it back
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Binary),
    ?assertEqual(?REQUEST_PING, Msg#slurm_msg.header#slurm_header.msg_type),
    ?assert(is_record(Msg#slurm_msg.body, ping_request)).

encode_decode_slurm_rc_roundtrip_test() ->
    %% Encode a SLURM RC response
    {ok, Binary} = flurm_protocol_codec:encode(?RESPONSE_SLURM_RC, #slurm_rc_response{return_code = 0}),

    %% Decode it back
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Binary),
    ?assertEqual(?RESPONSE_SLURM_RC, Msg#slurm_msg.header#slurm_header.msg_type),
    ?assertEqual(0, (Msg#slurm_msg.body)#slurm_rc_response.return_code).

encode_decode_job_info_request_roundtrip_test() ->
    Req = #job_info_request{
        show_flags = 100,
        job_id = 123,
        user_id = 1000
    },
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_JOB_INFO, Req),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Binary),

    ?assertEqual(?REQUEST_JOB_INFO, Msg#slurm_msg.header#slurm_header.msg_type),
    DecodedReq = Msg#slurm_msg.body,
    ?assertEqual(100, DecodedReq#job_info_request.show_flags),
    ?assertEqual(123, DecodedReq#job_info_request.job_id),
    ?assertEqual(1000, DecodedReq#job_info_request.user_id).

encode_decode_node_registration_roundtrip_test() ->
    Req = #node_registration_request{status_only = true},
    {ok, Binary} = flurm_protocol_codec:encode(?REQUEST_NODE_REGISTRATION_STATUS, Req),
    {ok, Msg, <<>>} = flurm_protocol_codec:decode(Binary),

    ?assertEqual(?REQUEST_NODE_REGISTRATION_STATUS, Msg#slurm_msg.header#slurm_header.msg_type),
    ?assert((Msg#slurm_msg.body)#node_registration_request.status_only).

%%%===================================================================
%%% Test: decode/1 - Error cases
%%%===================================================================

decode_incomplete_length_prefix_test() ->
    %% Less than 4 bytes
    {error, {incomplete_length_prefix, 0}} = flurm_protocol_codec:decode(<<>>),
    {error, {incomplete_length_prefix, 1}} = flurm_protocol_codec:decode(<<1>>),
    {error, {incomplete_length_prefix, 2}} = flurm_protocol_codec:decode(<<1, 2>>),
    {error, {incomplete_length_prefix, 3}} = flurm_protocol_codec:decode(<<1, 2, 3>>).

decode_incomplete_message_test() ->
    %% Length says 100 bytes but only 50 available
    {error, {incomplete_message, 100, 50}} =
        flurm_protocol_codec:decode(<<100:32/big, (binary:copy(<<0>>, 50))/binary>>).

decode_invalid_message_length_test() ->
    %% Length less than header size (10)
    {error, {invalid_message_length, 5}} =
        flurm_protocol_codec:decode(<<5:32/big, (binary:copy(<<0>>, 20))/binary>>),
    {error, {invalid_message_length, 0}} =
        flurm_protocol_codec:decode(<<0:32/big, (binary:copy(<<0>>, 20))/binary>>).

decode_invalid_data_test() ->
    {error, invalid_message_data} = flurm_protocol_codec:decode(not_binary).

%%%===================================================================
%%% Test: extract_resources_from_protocol/1
%%%===================================================================

extract_resources_small_binary_test() ->
    %% Binary too small - returns {0, 0}
    ?assertEqual({0, 0}, flurm_protocol_codec:extract_resources_from_protocol(<<>>)),
    ?assertEqual({0, 0}, flurm_protocol_codec:extract_resources_from_protocol(<<1,2,3>>)),
    ?assertEqual({0, 0}, flurm_protocol_codec:extract_resources_from_protocol(binary:copy(<<0>>, 50))).

extract_resources_larger_binary_test() ->
    %% Larger binary but with invalid patterns
    Binary = binary:copy(<<16#FF>>, 200),
    Result = flurm_protocol_codec:extract_resources_from_protocol(Binary),
    ?assert(is_tuple(Result)),
    {Nodes, Cpus} = Result,
    ?assert(is_integer(Nodes)),
    ?assert(is_integer(Cpus)).

%%%===================================================================
%%% Test: extract_full_job_desc/1
%%%===================================================================

extract_full_job_desc_small_binary_test() ->
    %% Binary too small
    {error, binary_too_small} = flurm_protocol_codec:extract_full_job_desc(<<>>),
    {error, binary_too_small} = flurm_protocol_codec:extract_full_job_desc(<<1,2,3>>).

extract_full_job_desc_larger_binary_test() ->
    %% Larger binary - should return a map
    Binary = binary:copy(<<0>>, 200),
    {ok, Result} = flurm_protocol_codec:extract_full_job_desc(Binary),
    ?assert(is_map(Result)),
    ?assert(maps:is_key(min_nodes, Result)),
    ?assert(maps:is_key(min_cpus, Result)),
    ?assert(maps:is_key(time_limit, Result)),
    ?assert(maps:is_key(job_name, Result)).

%%%===================================================================
%%% Test: Response encoding/decoding functions
%%%===================================================================

encode_reservation_info_response_test() ->
    Resp = #reservation_info_response{
        last_update = 1700000000,
        reservation_count = 1,
        reservations = [
            #reservation_info{
                name = <<"maintenance">>,
                accounts = <<>>,
                burst_buffer = <<>>,
                core_cnt = 16,
                core_spec_cnt = 0,
                end_time = 1700003600,
                features = <<>>,
                flags = 0,
                groups = <<>>,
                licenses = <<>>,
                max_start_delay = 0,
                node_cnt = 2,
                node_list = <<"node[1-2]">>,
                partition = <<"batch">>,
                purge_comp_time = 0,
                resv_watts = 0,
                start_time = 1700000000,
                tres_str = <<>>,
                users = <<"admin">>
            }
        ]
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_RESERVATION_INFO, Resp),
    ?assert(is_binary(Binary)),
    <<1:32/big, _/binary>> = Binary.

encode_license_info_response_test() ->
    Resp = #license_info_response{
        last_update = 1700000000,
        license_count = 1,
        licenses = [
            #license_info{
                name = <<"matlab">>,
                total = 10,
                in_use = 3,
                available = 7,
                reserved = 0,
                remote = 0
            }
        ]
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_LICENSE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_topo_info_response_test() ->
    Resp = #topo_info_response{
        topo_count = 1,
        topos = [
            #topo_info{
                level = 0,
                link_speed = 100000,
                name = <<"switch1">>,
                nodes = <<"node[1-4]">>,
                switches = <<>>
            }
        ]
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_TOPO_INFO, Resp),
    ?assert(is_binary(Binary)),
    <<1:32/big, _/binary>> = Binary.

encode_front_end_info_response_test() ->
    Resp = #front_end_info_response{
        last_update = 1700000000,
        front_end_count = 1,
        front_ends = [
            #front_end_info{
                allow_groups = <<>>,
                allow_users = <<>>,
                boot_time = 1699900000,
                deny_groups = <<>>,
                deny_users = <<>>,
                name = <<"frontend1">>,
                node_state = 2,
                reason = <<>>,
                reason_time = 0,
                reason_uid = 0,
                slurmd_start_time = 1699900000,
                version = <<"22.05.0">>
            }
        ]
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FRONT_END_INFO, Resp),
    ?assert(is_binary(Binary)),
    <<1:32/big, _/binary>> = Binary.

encode_burst_buffer_info_response_test() ->
    Resp = #burst_buffer_info_response{
        last_update = 1700000000,
        burst_buffer_count = 1,
        burst_buffers = [
            #burst_buffer_info{
                name = <<"datawarp">>,
                default_pool = <<"default">>,
                allow_users = <<>>,
                create_buffer = <<>>,
                deny_users = <<>>,
                destroy_buffer = <<>>,
                flags = 0,
                get_sys_state = <<>>,
                get_sys_status = <<>>,
                granularity = 1048576,
                pool_cnt = 1,
                pools = [
                    #burst_buffer_pool{
                        name = <<"default">>,
                        total_space = 1099511627776,
                        granularity = 1048576,
                        unfree_space = 0,
                        used_space = 0
                    }
                ],
                other_timeout = 300,
                stage_in_timeout = 300,
                stage_out_timeout = 300,
                start_stage_in = <<>>,
                start_stage_out = <<>>,
                stop_stage_in = <<>>,
                stop_stage_out = <<>>,
                total_space = 1099511627776,
                unfree_space = 0,
                used_space = 0,
                validate_timeout = 60
            }
        ]
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_BURST_BUFFER_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_build_info_response_test() ->
    Resp = #build_info_response{
        version = <<"22.05.0">>,
        release = <<"flurm-0.1.0">>,
        cluster_name = <<"testcluster">>,
        control_machine = <<"controller1">>,
        auth_type = <<"auth/munge">>,
        accounting_storage_type = <<"accounting_storage/none">>,
        slurmctld_host = <<"controller1">>,
        slurmctld_port = 6817,
        slurmd_port = 6818,
        slurmd_user_name = <<"root">>,
        slurm_user_name = <<"slurm">>,
        spool_dir = <<"/var/spool/flurm">>,
        state_save_location = <<"/var/spool/flurm/state">>,
        plugin_dir = <<"/usr/lib64/slurm">>,
        priority_type = <<"priority/multifactor">>,
        select_type = <<"select/cons_tres">>,
        scheduler_type = <<"sched/backfill">>,
        job_comp_type = <<"jobcomp/none">>
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_BUILD_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_build_info_response_fallback_test() ->
    %% Non-record input
    {ok, <<>>} = flurm_protocol_codec:encode_body(?RESPONSE_BUILD_INFO, not_a_record).

encode_config_info_response_test() ->
    Resp = #config_info_response{
        last_update = 1700000000,
        config = #{
            cluster_name => <<"testcluster">>,
            port => 6817,
            debug_level => <<"info">>
        }
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_CONFIG_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_config_info_response_fallback_test() ->
    {ok, <<>>} = flurm_protocol_codec:encode_body(?RESPONSE_CONFIG_INFO, not_a_record).

encode_stats_info_response_test() ->
    Resp = #stats_info_response{
        parts_packed = 1,
        req_time = 1700000000,
        req_time_start = 1699900000,
        server_thread_count = 10,
        agent_queue_size = 0,
        agent_count = 2,
        agent_thread_count = 4,
        dbd_agent_queue_size = 0,
        jobs_submitted = 100,
        jobs_started = 95,
        jobs_completed = 90,
        jobs_canceled = 3,
        jobs_failed = 2,
        jobs_pending = 5,
        jobs_running = 5,
        schedule_cycle_max = 1000,
        schedule_cycle_last = 500,
        schedule_cycle_sum = 50000,
        schedule_cycle_counter = 100,
        schedule_cycle_depth = 50,
        schedule_queue_len = 10,
        bf_backfilled_jobs = 20,
        bf_last_backfilled_jobs = 2,
        bf_cycle_counter = 50,
        bf_cycle_sum = 25000,
        bf_cycle_last = 500,
        bf_cycle_max = 1000,
        bf_depth_sum = 500,
        bf_depth_try_sum = 600,
        bf_queue_len = 10,
        bf_queue_len_sum = 500,
        bf_when_last_cycle = 1700000000,
        bf_active = true
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_STATS_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_stats_info_response_fallback_test() ->
    {ok, <<>>} = flurm_protocol_codec:encode_body(?RESPONSE_STATS_INFO, not_a_record).

encode_reconfigure_response_test() ->
    Resp = #reconfigure_response{
        return_code = 0,
        message = <<"Configuration reloaded">>,
        changed_keys = [port, debug_level],
        version = 2
    },
    {ok, Binary} = flurm_protocol_codec:encode_reconfigure_response(Resp),
    ?assert(is_binary(Binary)).

encode_reconfigure_response_fallback_test() ->
    {ok, <<0:32/big-signed, 0:32, 0:32, 0:32>>} = flurm_protocol_codec:encode_reconfigure_response(not_a_record).

%%%===================================================================
%%% Test: Job info response encoding
%%% Note: These tests need lager started or they use try/catch to handle
%%% the lager:info calls in the codec
%%%===================================================================

encode_job_info_response_test() ->
    %% Try to start lager, but continue even if it fails
    catch application:start(lager),
    Resp = #job_info_response{
        last_update = 1700000000,
        job_count = 0,
        jobs = []
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_JOB_INFO, Resp),
        ?assert(is_binary(Binary)),
        <<0:32/big, _/binary>> = Binary  % job_count first
    catch
        error:undef -> ok  % lager not available, skip this test
    end.

encode_job_info_response_with_job_test() ->
    catch application:start(lager),
    Job = #job_info{
        job_id = 123,
        name = <<"test_job">>,
        partition = <<"batch">>,
        user_id = 1000,
        group_id = 1000,
        job_state = ?JOB_PENDING,
        num_nodes = 1,
        num_cpus = 4,
        num_tasks = 4,
        priority = 100,
        time_limit = 3600,
        submit_time = 1700000000,
        start_time = 0,
        end_time = 0,
        nodes = <<>>,
        account = <<>>,
        admin_comment = <<>>,
        alloc_node = <<>>,
        alloc_sid = 0,
        accrue_time = 0
    },
    Resp = #job_info_response{
        last_update = 1700000000,
        job_count = 1,
        jobs = [Job]
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_JOB_INFO, Resp),
        ?assert(is_binary(Binary)),
        ?assert(byte_size(Binary) > 100)
    catch
        error:undef -> ok  % lager not available, skip this test
    end.

%%%===================================================================
%%% Test: Node info response encoding
%%%===================================================================

encode_node_info_response_test() ->
    catch application:start(lager),
    Node = #node_info{
        name = <<"node1">>,
        node_hostname = <<"node1.cluster">>,
        node_addr = <<"192.168.1.1">>,
        port = 6818,
        node_state = ?NODE_STATE_IDLE,
        version = <<"22.05.0">>,
        arch = <<"x86_64">>,
        os = <<"Linux">>,
        cpus = 16,
        boards = 1,
        sockets = 2,
        cores = 8,
        threads = 1,
        real_memory = 64000,
        tmp_disk = 100000,
        weight = 1,
        cpu_load = 100,
        free_mem = 60000,
        features = <<"gpu">>,
        features_act = <<"gpu">>,
        gres = <<"gpu:2">>,
        gres_drain = <<>>,
        gres_used = <<>>,
        reason = <<>>,
        boot_time = 1699900000,
        slurmd_start_time = 1699900000
    },
    Resp = #node_info_response{
        last_update = 1700000000,
        node_count = 1,
        nodes = [Node]
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_NODE_INFO, Resp),
        ?assert(is_binary(Binary)),
        <<1:32/big, _/binary>> = Binary
    catch
        error:undef -> ok  % lager not available, skip this test
    end.

%%%===================================================================
%%% Test: Partition info response encoding
%%%===================================================================

encode_partition_info_response_test() ->
    Part = #partition_info{
        name = <<"batch">>,
        max_time = 86400,
        default_time = 3600,
        max_nodes = 100,
        min_nodes = 1,
        total_nodes = 10,
        total_cpus = 160,
        priority_job_factor = 1,
        priority_tier = 1,
        state_up = 1,
        nodes = <<"node[1-10]">>
    },
    Resp = #partition_info_response{
        last_update = 1700000000,
        partition_count = 1,
        partitions = [Part]
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_PARTITION_INFO, Resp),
    ?assert(is_binary(Binary)),
    <<1:32/big, _/binary>> = Binary.

%%%===================================================================
%%% Test: Partition info response decoding
%%%===================================================================

decode_partition_info_response_empty_test() ->
    {ok, #partition_info_response{}} = flurm_protocol_codec:decode_body(?RESPONSE_PARTITION_INFO, <<>>).

decode_partition_info_response_header_only_test() ->
    Binary = <<1:32/big, 1700000000:64/big>>,
    {ok, Resp} = flurm_protocol_codec:decode_body(?RESPONSE_PARTITION_INFO, Binary),
    ?assertEqual(1, Resp#partition_info_response.partition_count),
    ?assertEqual(1700000000, Resp#partition_info_response.last_update).

%%%===================================================================
%%% Test: Job step info encoding
%%%===================================================================

encode_job_step_info_response_with_step_test() ->
    Step = #job_step_info{
        job_id = 123,
        step_id = 0,
        step_name = <<"step0">>,
        partition = <<"batch">>,
        user_id = 1000,
        state = ?JOB_RUNNING,
        num_tasks = 4,
        num_cpus = 4,
        time_limit = 3600,
        start_time = 1700000000,
        run_time = 100,
        nodes = <<"node1">>,
        node_cnt = 1,
        tres_alloc_str = <<"cpu=4">>,
        exit_code = 0
    },
    Resp = #job_step_info_response{
        last_update = 1700000000,
        step_count = 1,
        steps = [Step]
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Test: Additional decode_body coverage
%%%===================================================================

decode_body_kill_job_numeric_format_test() ->
    %% Numeric format
    Binary = <<100:32/big, -1:32/big-signed, 15:16/big, 1:16/big>>,
    {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_KILL_JOB, Binary),
    ?assertEqual(100, Req#kill_job_request.job_id),
    ?assertEqual(15, Req#kill_job_request.signal),
    ?assertEqual(1, Req#kill_job_request.flags).

decode_body_kill_job_shorter_format_test() ->
    %% Just job_id and step_id
    Binary = <<200:32/big, 5:32/big-signed>>,
    {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_KILL_JOB, Binary),
    ?assertEqual(200, Req#kill_job_request.job_id),
    ?assertEqual(5, Req#kill_job_request.step_id).

decode_body_update_job_with_job_id_test() ->
    %% With numeric job_id
    Binary = <<123:32/big, "rest">>,
    {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_UPDATE_JOB, Binary),
    ?assertEqual(123, Req#update_job_request.job_id).

decode_body_job_will_run_with_job_id_test() ->
    %% With numeric job_id
    Binary = <<456:32/big, "rest">>,
    {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_JOB_WILL_RUN, Binary),
    ?assertEqual(456, Req#job_will_run_request.job_id).

decode_body_job_will_run_no_job_id_test() ->
    %% Without job_id (starts with 0)
    Binary = <<0:32/big, "data">>,
    {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_JOB_WILL_RUN, Binary),
    ?assertEqual(0, Req#job_will_run_request.job_id).

decode_body_job_step_create_full_test() ->
    %% Full format
    Binary = <<100:32/big, 0:32/big, 1000:32/big, 1:32/big, 4:32/big, 8:32/big, 2:32/big, 3600:32/big, 0:32/big, 1:32/big>>,
    {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
    ?assertEqual(100, Req#job_step_create_request.job_id),
    ?assertEqual(1, Req#job_step_create_request.min_nodes),
    ?assertEqual(4, Req#job_step_create_request.max_nodes),
    ?assertEqual(8, Req#job_step_create_request.num_tasks).

decode_body_job_step_create_minimal_test() ->
    %% Just job_id and step_id
    Binary = <<200:32/big, 5:32/big>>,
    {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_JOB_STEP_CREATE, Binary),
    ?assertEqual(200, Req#job_step_create_request.job_id),
    ?assertEqual(5, Req#job_step_create_request.step_id).

decode_body_job_step_create_only_job_id_test() ->
    %% Just job_id
    {ok, Req} = flurm_protocol_codec:decode_body(?REQUEST_JOB_STEP_CREATE, <<300:32/big>>),
    ?assertEqual(300, Req#job_step_create_request.job_id).

decode_body_reconfigure_with_config_empty_test() ->
    {ok, #reconfigure_with_config_request{}} =
        flurm_protocol_codec:decode_body(?REQUEST_RECONFIGURE_WITH_CONFIG, <<>>).

%%%===================================================================
%%% Test: decode_with_extra/1
%%%===================================================================

decode_with_extra_incomplete_test() ->
    %% Less than 4 bytes
    {error, {incomplete_length_prefix, 2}} = flurm_protocol_codec:decode_with_extra(<<1, 2>>),

    %% Length says 100 but not enough data
    {error, {incomplete_message, 100, 50}} =
        flurm_protocol_codec:decode_with_extra(<<100:32/big, (binary:copy(<<0>>, 50))/binary>>).

decode_with_extra_invalid_test() ->
    {error, invalid_message_data} = flurm_protocol_codec:decode_with_extra(not_binary).

%%%===================================================================
%%% Test: decode_response/1
%%%===================================================================

decode_response_incomplete_test() ->
    %% Less than 4 bytes
    {error, {incomplete_length_prefix, 2}} = flurm_protocol_codec:decode_response(<<1, 2>>),

    %% Length says 100 but not enough data
    {error, {incomplete_message, 100, 50}} =
        flurm_protocol_codec:decode_response(<<100:32/big, (binary:copy(<<0>>, 50))/binary>>).

decode_response_invalid_test() ->
    {error, invalid_message_data} = flurm_protocol_codec:decode_response(not_binary).

%%%===================================================================
%%% Test: Legacy job_submit_req encoding
%%%===================================================================

encode_legacy_job_submit_req_test() ->
    %% Test the legacy job_submit_req record conversion
    Req = #job_submit_req{
        name = <<"legacy_job">>,
        script = <<"#!/bin/bash\necho test">>,
        partition = <<"batch">>,
        num_nodes = 2,
        num_cpus = 8,
        memory_mb = 4096,
        time_limit = 3600,
        priority = 100,
        env = #{home => <<"/home/user">>},
        working_dir = <<"/tmp">>
    },
    {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_SUBMIT_BATCH_JOB, Req),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 0).
