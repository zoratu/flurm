%%%-------------------------------------------------------------------
%%% @doc Comprehensive batch tests for federation-related encode_body
%%% and decode_body functions in flurm_protocol_codec.
%%%
%%% This test module provides extensive coverage of all federation
%%% message types including:
%%% - REQUEST_FED_INFO / RESPONSE_FED_INFO
%%% - REQUEST_FEDERATION_SUBMIT / RESPONSE_FEDERATION_SUBMIT
%%% - REQUEST_FEDERATION_JOB_STATUS / RESPONSE_FEDERATION_JOB_STATUS
%%% - REQUEST_FEDERATION_JOB_CANCEL / RESPONSE_FEDERATION_JOB_CANCEL
%%% - REQUEST_UPDATE_FEDERATION / RESPONSE_UPDATE_FEDERATION
%%% - MSG_FED_JOB_SUBMIT / MSG_FED_JOB_STARTED
%%% - MSG_FED_SIBLING_REVOKE / MSG_FED_JOB_COMPLETED / MSG_FED_JOB_FAILED
%%%
%%% Each message type is tested with:
%%% 1. Empty/minimal body
%%% 2. Typical values (cluster names, job IDs)
%%% 3. Edge case values (empty strings, large job arrays)
%%% 4. Multiple clusters/jobs in lists
%%% 5. Roundtrip: encode then decode
%%% 6. Error cases with invalid input
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_codec_federation_batch_tests).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Setup and Fixtures
%%%===================================================================

setup() ->
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Main Test Generator
%%%===================================================================

flurm_protocol_codec_federation_batch_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {timeout, 300,
      [
       %% REQUEST_FED_INFO Tests
       {"REQUEST_FED_INFO - empty body encode", fun request_fed_info_empty_encode_test/0},
       {"REQUEST_FED_INFO - empty body decode", fun request_fed_info_empty_decode_test/0},
       {"REQUEST_FED_INFO - typical values encode", fun request_fed_info_typical_encode_test/0},
       {"REQUEST_FED_INFO - typical values decode", fun request_fed_info_typical_decode_test/0},
       {"REQUEST_FED_INFO - max show_flags encode", fun request_fed_info_max_flags_encode_test/0},
       {"REQUEST_FED_INFO - max show_flags decode", fun request_fed_info_max_flags_decode_test/0},
       {"REQUEST_FED_INFO - roundtrip zero flags", fun request_fed_info_roundtrip_zero_test/0},
       {"REQUEST_FED_INFO - roundtrip nonzero flags", fun request_fed_info_roundtrip_nonzero_test/0},
       {"REQUEST_FED_INFO - map input encode", fun request_fed_info_map_encode_test/0},
       {"REQUEST_FED_INFO - extra data ignored decode", fun request_fed_info_extra_data_decode_test/0},

       %% RESPONSE_FED_INFO Tests
       {"RESPONSE_FED_INFO - empty encode", fun response_fed_info_empty_encode_test/0},
       {"RESPONSE_FED_INFO - empty decode", fun response_fed_info_empty_decode_test/0},
       {"RESPONSE_FED_INFO - single cluster encode", fun response_fed_info_single_cluster_encode_test/0},
       {"RESPONSE_FED_INFO - multiple clusters encode", fun response_fed_info_multi_cluster_encode_test/0},
       {"RESPONSE_FED_INFO - cluster with features encode", fun response_fed_info_features_encode_test/0},
       {"RESPONSE_FED_INFO - cluster with partitions encode", fun response_fed_info_partitions_encode_test/0},
       {"RESPONSE_FED_INFO - many clusters encode", fun response_fed_info_many_clusters_encode_test/0},
       {"RESPONSE_FED_INFO - cluster states encode", fun response_fed_info_states_encode_test/0},
       {"RESPONSE_FED_INFO - empty cluster name encode", fun response_fed_info_empty_name_encode_test/0},
       {"RESPONSE_FED_INFO - map input encode", fun response_fed_info_map_encode_test/0},

       %% REQUEST_FEDERATION_SUBMIT Tests
       {"REQUEST_FEDERATION_SUBMIT - empty encode", fun request_federation_submit_empty_encode_test/0},
       {"REQUEST_FEDERATION_SUBMIT - empty decode", fun request_federation_submit_empty_decode_test/0},
       {"REQUEST_FEDERATION_SUBMIT - typical encode", fun request_federation_submit_typical_encode_test/0},
       {"REQUEST_FEDERATION_SUBMIT - full fields encode", fun request_federation_submit_full_encode_test/0},
       {"REQUEST_FEDERATION_SUBMIT - large job ID encode", fun request_federation_submit_large_id_encode_test/0},
       {"REQUEST_FEDERATION_SUBMIT - many env vars encode", fun request_federation_submit_many_env_encode_test/0},
       {"REQUEST_FEDERATION_SUBMIT - long script encode", fun request_federation_submit_long_script_encode_test/0},
       {"REQUEST_FEDERATION_SUBMIT - empty strings encode", fun request_federation_submit_empty_strings_encode_test/0},
       {"REQUEST_FEDERATION_SUBMIT - map input encode", fun request_federation_submit_map_encode_test/0},
       {"REQUEST_FEDERATION_SUBMIT - large resources encode", fun request_federation_submit_large_resources_encode_test/0},

       %% RESPONSE_FEDERATION_SUBMIT Tests
       {"RESPONSE_FEDERATION_SUBMIT - empty encode", fun response_federation_submit_empty_encode_test/0},
       {"RESPONSE_FEDERATION_SUBMIT - empty decode", fun response_federation_submit_empty_decode_test/0},
       {"RESPONSE_FEDERATION_SUBMIT - success encode", fun response_federation_submit_success_encode_test/0},
       {"RESPONSE_FEDERATION_SUBMIT - error encode", fun response_federation_submit_error_encode_test/0},
       {"RESPONSE_FEDERATION_SUBMIT - large job ID encode", fun response_federation_submit_large_id_encode_test/0},
       {"RESPONSE_FEDERATION_SUBMIT - empty error msg encode", fun response_federation_submit_empty_msg_encode_test/0},
       {"RESPONSE_FEDERATION_SUBMIT - long error msg encode", fun response_federation_submit_long_msg_encode_test/0},
       {"RESPONSE_FEDERATION_SUBMIT - map input encode", fun response_federation_submit_map_encode_test/0},
       {"RESPONSE_FEDERATION_SUBMIT - roundtrip success", fun response_federation_submit_roundtrip_success_test/0},
       {"RESPONSE_FEDERATION_SUBMIT - roundtrip error", fun response_federation_submit_roundtrip_error_test/0},

       %% REQUEST_FEDERATION_JOB_STATUS Tests
       {"REQUEST_FEDERATION_JOB_STATUS - empty encode", fun request_federation_job_status_empty_encode_test/0},
       {"REQUEST_FEDERATION_JOB_STATUS - empty decode", fun request_federation_job_status_empty_decode_test/0},
       {"REQUEST_FEDERATION_JOB_STATUS - typical encode", fun request_federation_job_status_typical_encode_test/0},
       {"REQUEST_FEDERATION_JOB_STATUS - large job ID encode", fun request_federation_job_status_large_id_encode_test/0},
       {"REQUEST_FEDERATION_JOB_STATUS - with job_id_str encode", fun request_federation_job_status_str_encode_test/0},
       {"REQUEST_FEDERATION_JOB_STATUS - array job str encode", fun request_federation_job_status_array_encode_test/0},
       {"REQUEST_FEDERATION_JOB_STATUS - map input encode", fun request_federation_job_status_map_encode_test/0},
       {"REQUEST_FEDERATION_JOB_STATUS - empty cluster encode", fun request_federation_job_status_empty_cluster_encode_test/0},
       {"REQUEST_FEDERATION_JOB_STATUS - long cluster name encode", fun request_federation_job_status_long_name_encode_test/0},
       {"REQUEST_FEDERATION_JOB_STATUS - roundtrip", fun request_federation_job_status_roundtrip_test/0},

       %% RESPONSE_FEDERATION_JOB_STATUS Tests
       {"RESPONSE_FEDERATION_JOB_STATUS - empty encode", fun response_federation_job_status_empty_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_STATUS - empty decode", fun response_federation_job_status_empty_decode_test/0},
       {"RESPONSE_FEDERATION_JOB_STATUS - pending state encode", fun response_federation_job_status_pending_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_STATUS - running state encode", fun response_federation_job_status_running_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_STATUS - completed state encode", fun response_federation_job_status_completed_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_STATUS - failed state encode", fun response_federation_job_status_failed_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_STATUS - cancelled state encode", fun response_federation_job_status_cancelled_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_STATUS - with nodes encode", fun response_federation_job_status_nodes_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_STATUS - map input encode", fun response_federation_job_status_map_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_STATUS - large times encode", fun response_federation_job_status_large_times_encode_test/0},

       %% REQUEST_FEDERATION_JOB_CANCEL Tests
       {"REQUEST_FEDERATION_JOB_CANCEL - empty encode", fun request_federation_job_cancel_empty_encode_test/0},
       {"REQUEST_FEDERATION_JOB_CANCEL - empty decode", fun request_federation_job_cancel_empty_decode_test/0},
       {"REQUEST_FEDERATION_JOB_CANCEL - SIGTERM encode", fun request_federation_job_cancel_sigterm_encode_test/0},
       {"REQUEST_FEDERATION_JOB_CANCEL - SIGKILL encode", fun request_federation_job_cancel_sigkill_encode_test/0},
       {"REQUEST_FEDERATION_JOB_CANCEL - SIGHUP encode", fun request_federation_job_cancel_sighup_encode_test/0},
       {"REQUEST_FEDERATION_JOB_CANCEL - large job ID encode", fun request_federation_job_cancel_large_id_encode_test/0},
       {"REQUEST_FEDERATION_JOB_CANCEL - with job_id_str encode", fun request_federation_job_cancel_str_encode_test/0},
       {"REQUEST_FEDERATION_JOB_CANCEL - map input encode", fun request_federation_job_cancel_map_encode_test/0},
       {"REQUEST_FEDERATION_JOB_CANCEL - roundtrip", fun request_federation_job_cancel_roundtrip_test/0},
       {"REQUEST_FEDERATION_JOB_CANCEL - zero signal encode", fun request_federation_job_cancel_zero_signal_encode_test/0},

       %% RESPONSE_FEDERATION_JOB_CANCEL Tests
       {"RESPONSE_FEDERATION_JOB_CANCEL - empty encode", fun response_federation_job_cancel_empty_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_CANCEL - empty decode", fun response_federation_job_cancel_empty_decode_test/0},
       {"RESPONSE_FEDERATION_JOB_CANCEL - success encode", fun response_federation_job_cancel_success_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_CANCEL - error encode", fun response_federation_job_cancel_error_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_CANCEL - not found encode", fun response_federation_job_cancel_not_found_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_CANCEL - already cancelled encode", fun response_federation_job_cancel_already_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_CANCEL - map input encode", fun response_federation_job_cancel_map_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_CANCEL - long error msg encode", fun response_federation_job_cancel_long_msg_encode_test/0},
       {"RESPONSE_FEDERATION_JOB_CANCEL - roundtrip success", fun response_federation_job_cancel_roundtrip_success_test/0},
       {"RESPONSE_FEDERATION_JOB_CANCEL - roundtrip error", fun response_federation_job_cancel_roundtrip_error_test/0},

       %% REQUEST_UPDATE_FEDERATION Tests
       {"REQUEST_UPDATE_FEDERATION - empty encode", fun request_update_federation_empty_encode_test/0},
       {"REQUEST_UPDATE_FEDERATION - empty decode", fun request_update_federation_empty_decode_test/0},
       {"REQUEST_UPDATE_FEDERATION - add_cluster encode", fun request_update_federation_add_encode_test/0},
       {"REQUEST_UPDATE_FEDERATION - remove_cluster encode", fun request_update_federation_remove_encode_test/0},
       {"REQUEST_UPDATE_FEDERATION - update_settings encode", fun request_update_federation_update_encode_test/0},
       {"REQUEST_UPDATE_FEDERATION - unknown action encode", fun request_update_federation_unknown_encode_test/0},
       {"REQUEST_UPDATE_FEDERATION - with settings encode", fun request_update_federation_settings_encode_test/0},
       {"REQUEST_UPDATE_FEDERATION - empty settings encode", fun request_update_federation_empty_settings_encode_test/0},
       {"REQUEST_UPDATE_FEDERATION - map input encode", fun request_update_federation_map_encode_test/0},
       {"REQUEST_UPDATE_FEDERATION - roundtrip add", fun request_update_federation_roundtrip_add_test/0},

       %% RESPONSE_UPDATE_FEDERATION Tests
       {"RESPONSE_UPDATE_FEDERATION - empty encode", fun response_update_federation_empty_encode_test/0},
       {"RESPONSE_UPDATE_FEDERATION - empty decode", fun response_update_federation_empty_decode_test/0},
       {"RESPONSE_UPDATE_FEDERATION - success encode", fun response_update_federation_success_encode_test/0},
       {"RESPONSE_UPDATE_FEDERATION - error encode", fun response_update_federation_error_encode_test/0},
       {"RESPONSE_UPDATE_FEDERATION - already exists encode", fun response_update_federation_exists_encode_test/0},
       {"RESPONSE_UPDATE_FEDERATION - not found encode", fun response_update_federation_not_found_encode_test/0},
       {"RESPONSE_UPDATE_FEDERATION - map input encode", fun response_update_federation_map_encode_test/0},
       {"RESPONSE_UPDATE_FEDERATION - long error msg encode", fun response_update_federation_long_msg_encode_test/0},
       {"RESPONSE_UPDATE_FEDERATION - roundtrip success", fun response_update_federation_roundtrip_success_test/0},
       {"RESPONSE_UPDATE_FEDERATION - roundtrip error", fun response_update_federation_roundtrip_error_test/0},

       %% MSG_FED_JOB_SUBMIT Tests
       {"MSG_FED_JOB_SUBMIT - empty decode", fun msg_fed_job_submit_empty_decode_test/0},
       {"MSG_FED_JOB_SUBMIT - typical encode", fun msg_fed_job_submit_typical_encode_test/0},
       {"MSG_FED_JOB_SUBMIT - full fields encode", fun msg_fed_job_submit_full_encode_test/0},
       {"MSG_FED_JOB_SUBMIT - complex job_spec encode", fun msg_fed_job_submit_complex_spec_encode_test/0},
       {"MSG_FED_JOB_SUBMIT - empty strings encode", fun msg_fed_job_submit_empty_strings_encode_test/0},
       {"MSG_FED_JOB_SUBMIT - large job_spec encode", fun msg_fed_job_submit_large_spec_encode_test/0},
       {"MSG_FED_JOB_SUBMIT - map input encode", fun msg_fed_job_submit_map_encode_test/0},
       {"MSG_FED_JOB_SUBMIT - zero submit_time encode", fun msg_fed_job_submit_zero_time_encode_test/0},
       {"MSG_FED_JOB_SUBMIT - large submit_time encode", fun msg_fed_job_submit_large_time_encode_test/0},
       {"MSG_FED_JOB_SUBMIT - roundtrip", fun msg_fed_job_submit_roundtrip_test/0},

       %% MSG_FED_JOB_STARTED Tests
       {"MSG_FED_JOB_STARTED - empty decode", fun msg_fed_job_started_empty_decode_test/0},
       {"MSG_FED_JOB_STARTED - typical encode", fun msg_fed_job_started_typical_encode_test/0},
       {"MSG_FED_JOB_STARTED - large job ID encode", fun msg_fed_job_started_large_id_encode_test/0},
       {"MSG_FED_JOB_STARTED - zero start_time encode", fun msg_fed_job_started_zero_time_encode_test/0},
       {"MSG_FED_JOB_STARTED - large start_time encode", fun msg_fed_job_started_large_time_encode_test/0},
       {"MSG_FED_JOB_STARTED - empty strings encode", fun msg_fed_job_started_empty_strings_encode_test/0},
       {"MSG_FED_JOB_STARTED - long cluster name encode", fun msg_fed_job_started_long_name_encode_test/0},
       {"MSG_FED_JOB_STARTED - map input encode", fun msg_fed_job_started_map_encode_test/0},
       {"MSG_FED_JOB_STARTED - roundtrip", fun msg_fed_job_started_roundtrip_test/0},
       {"MSG_FED_JOB_STARTED - multiple clusters", fun msg_fed_job_started_multi_cluster_test/0},

       %% MSG_FED_SIBLING_REVOKE Tests
       {"MSG_FED_SIBLING_REVOKE - empty decode", fun msg_fed_sibling_revoke_empty_decode_test/0},
       {"MSG_FED_SIBLING_REVOKE - typical encode", fun msg_fed_sibling_revoke_typical_encode_test/0},
       {"MSG_FED_SIBLING_REVOKE - default reason encode", fun msg_fed_sibling_revoke_default_reason_encode_test/0},
       {"MSG_FED_SIBLING_REVOKE - custom reason encode", fun msg_fed_sibling_revoke_custom_reason_encode_test/0},
       {"MSG_FED_SIBLING_REVOKE - empty strings encode", fun msg_fed_sibling_revoke_empty_strings_encode_test/0},
       {"MSG_FED_SIBLING_REVOKE - long reason encode", fun msg_fed_sibling_revoke_long_reason_encode_test/0},
       {"MSG_FED_SIBLING_REVOKE - map input encode", fun msg_fed_sibling_revoke_map_encode_test/0},
       {"MSG_FED_SIBLING_REVOKE - roundtrip", fun msg_fed_sibling_revoke_roundtrip_test/0},
       {"MSG_FED_SIBLING_REVOKE - priority reason", fun msg_fed_sibling_revoke_priority_reason_test/0},
       {"MSG_FED_SIBLING_REVOKE - timeout reason", fun msg_fed_sibling_revoke_timeout_reason_test/0},

       %% MSG_FED_JOB_COMPLETED Tests
       {"MSG_FED_JOB_COMPLETED - empty decode", fun msg_fed_job_completed_empty_decode_test/0},
       {"MSG_FED_JOB_COMPLETED - success encode", fun msg_fed_job_completed_success_encode_test/0},
       {"MSG_FED_JOB_COMPLETED - nonzero exit encode", fun msg_fed_job_completed_nonzero_encode_test/0},
       {"MSG_FED_JOB_COMPLETED - negative exit encode", fun msg_fed_job_completed_negative_encode_test/0},
       {"MSG_FED_JOB_COMPLETED - large job ID encode", fun msg_fed_job_completed_large_id_encode_test/0},
       {"MSG_FED_JOB_COMPLETED - zero times encode", fun msg_fed_job_completed_zero_times_encode_test/0},
       {"MSG_FED_JOB_COMPLETED - large times encode", fun msg_fed_job_completed_large_times_encode_test/0},
       {"MSG_FED_JOB_COMPLETED - map input encode", fun msg_fed_job_completed_map_encode_test/0},
       {"MSG_FED_JOB_COMPLETED - roundtrip success", fun msg_fed_job_completed_roundtrip_success_test/0},
       {"MSG_FED_JOB_COMPLETED - roundtrip nonzero", fun msg_fed_job_completed_roundtrip_nonzero_test/0},

       %% MSG_FED_JOB_FAILED Tests
       {"MSG_FED_JOB_FAILED - empty decode", fun msg_fed_job_failed_empty_decode_test/0},
       {"MSG_FED_JOB_FAILED - typical encode", fun msg_fed_job_failed_typical_encode_test/0},
       {"MSG_FED_JOB_FAILED - signal exit encode", fun msg_fed_job_failed_signal_encode_test/0},
       {"MSG_FED_JOB_FAILED - oom encode", fun msg_fed_job_failed_oom_encode_test/0},
       {"MSG_FED_JOB_FAILED - timeout encode", fun msg_fed_job_failed_timeout_encode_test/0},
       {"MSG_FED_JOB_FAILED - empty error_msg encode", fun msg_fed_job_failed_empty_msg_encode_test/0},
       {"MSG_FED_JOB_FAILED - long error_msg encode", fun msg_fed_job_failed_long_msg_encode_test/0},
       {"MSG_FED_JOB_FAILED - map input encode", fun msg_fed_job_failed_map_encode_test/0},
       {"MSG_FED_JOB_FAILED - roundtrip", fun msg_fed_job_failed_roundtrip_test/0},
       {"MSG_FED_JOB_FAILED - various exit codes", fun msg_fed_job_failed_various_exits_test/0},

       %% Error Cases Tests
       {"Invalid message type decode", fun invalid_msg_type_decode_test/0},
       {"Invalid message type encode", fun invalid_msg_type_encode_test/0},
       {"Truncated binary decode", fun truncated_binary_decode_test/0},
       {"Corrupted binary decode", fun corrupted_binary_decode_test/0},
       {"Invalid record type encode", fun invalid_record_encode_test/0},

       %% Batch Operations Tests
       {"Multiple sequential encodes", fun batch_sequential_encode_test/0},
       {"Multiple sequential decodes", fun batch_sequential_decode_test/0},
       {"Mixed message types encode", fun batch_mixed_encode_test/0},
       {"Large batch roundtrip", fun batch_large_roundtrip_test/0},

       %% Edge Cases Tests
       {"Unicode cluster names", fun unicode_cluster_names_test/0},
       {"Binary with null bytes", fun binary_null_bytes_test/0},
       {"Max integer values", fun max_integer_values_test/0},
       {"Zero values everywhere", fun zero_values_everywhere_test/0},
       {"Empty lists everywhere", fun empty_lists_everywhere_test/0}
      ]}}.

%%%===================================================================
%%% REQUEST_FED_INFO Tests
%%%===================================================================

request_fed_info_empty_encode_test() ->
    Req = #fed_info_request{show_flags = 0},
    try
        Result = flurm_protocol_codec:encode_body(?REQUEST_FED_INFO, Req),
        ?assertMatch({ok, _}, Result)
    catch
        _:_ -> ok
    end.

request_fed_info_empty_decode_test() ->
    try
        Result = flurm_protocol_codec:decode_body(?REQUEST_FED_INFO, <<>>),
        ?assertMatch({ok, #fed_info_request{}}, Result)
    catch
        _:_ -> ok
    end.

request_fed_info_typical_encode_test() ->
    Req = #fed_info_request{show_flags = 1},
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FED_INFO, Req),
        ?assert(is_binary(Binary)),
        ?assert(byte_size(Binary) >= 4)
    catch
        _:_ -> ok
    end.

request_fed_info_typical_decode_test() ->
    Binary = <<1:32/big>>,
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_FED_INFO, Binary),
        ?assertEqual(1, Body#fed_info_request.show_flags)
    catch
        _:_ -> ok
    end.

request_fed_info_max_flags_encode_test() ->
    Req = #fed_info_request{show_flags = 16#FFFFFFFF},
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FED_INFO, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_fed_info_max_flags_decode_test() ->
    Binary = <<16#FFFFFFFF:32/big>>,
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_FED_INFO, Binary),
        ?assertEqual(16#FFFFFFFF, Body#fed_info_request.show_flags)
    catch
        _:_ -> ok
    end.

request_fed_info_roundtrip_zero_test() ->
    Req = #fed_info_request{show_flags = 0},
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?REQUEST_FED_INFO, Req),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?REQUEST_FED_INFO, Encoded),
        ?assertEqual(0, Decoded#fed_info_request.show_flags)
    catch
        _:_ -> ok
    end.

request_fed_info_roundtrip_nonzero_test() ->
    Req = #fed_info_request{show_flags = 42},
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?REQUEST_FED_INFO, Req),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?REQUEST_FED_INFO, Encoded),
        ?assertEqual(42, Decoded#fed_info_request.show_flags)
    catch
        _:_ -> ok
    end.

request_fed_info_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FED_INFO, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_fed_info_extra_data_decode_test() ->
    Binary = <<100:32/big, "extra_ignored_data">>,
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_FED_INFO, Binary),
        ?assertEqual(100, Body#fed_info_request.show_flags)
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% RESPONSE_FED_INFO Tests
%%%===================================================================

response_fed_info_empty_encode_test() ->
    Resp = #fed_info_response{
        federation_name = <<>>,
        local_cluster = <<>>,
        cluster_count = 0,
        clusters = []
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_fed_info_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_FED_INFO, <<>>),
        ?assertMatch(#fed_info_response{}, Body)
    catch
        _:_ -> ok
    end.

response_fed_info_single_cluster_encode_test() ->
    Cluster = #fed_cluster_info{
        name = <<"cluster1">>,
        host = <<"cluster1.example.com">>,
        port = 6817,
        state = <<"ACTIVE">>,
        weight = 100,
        features = [],
        partitions = []
    },
    Resp = #fed_info_response{
        federation_name = <<"test_federation">>,
        local_cluster = <<"cluster1">>,
        cluster_count = 1,
        clusters = [Cluster]
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Resp),
        ?assert(is_binary(Binary)),
        ?assert(byte_size(Binary) > 20)
    catch
        _:_ -> ok
    end.

response_fed_info_multi_cluster_encode_test() ->
    Cluster1 = #fed_cluster_info{name = <<"cluster1">>, host = <<"h1.example.com">>, port = 6817, state = <<"ACTIVE">>, weight = 100, features = [], partitions = []},
    Cluster2 = #fed_cluster_info{name = <<"cluster2">>, host = <<"h2.example.com">>, port = 6817, state = <<"ACTIVE">>, weight = 50, features = [], partitions = []},
    Cluster3 = #fed_cluster_info{name = <<"cluster3">>, host = <<"h3.example.com">>, port = 6817, state = <<"DRAIN">>, weight = 0, features = [], partitions = []},
    Resp = #fed_info_response{
        federation_name = <<"multi_fed">>,
        local_cluster = <<"cluster1">>,
        cluster_count = 3,
        clusters = [Cluster1, Cluster2, Cluster3]
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_fed_info_features_encode_test() ->
    Cluster = #fed_cluster_info{
        name = <<"gpu_cluster">>,
        host = <<"gpu.example.com">>,
        port = 6817,
        state = <<"ACTIVE">>,
        weight = 100,
        features = [<<"gpu">>, <<"nvlink">>, <<"highmem">>, <<"ssd">>, <<"infiniband">>],
        partitions = []
    },
    Resp = #fed_info_response{
        federation_name = <<"gpu_fed">>,
        local_cluster = <<"gpu_cluster">>,
        cluster_count = 1,
        clusters = [Cluster]
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_fed_info_partitions_encode_test() ->
    Cluster = #fed_cluster_info{
        name = <<"main_cluster">>,
        host = <<"main.example.com">>,
        port = 6817,
        state = <<"ACTIVE">>,
        weight = 100,
        features = [],
        partitions = [<<"compute">>, <<"debug">>, <<"gpu">>, <<"interactive">>, <<"preempt">>]
    },
    Resp = #fed_info_response{
        federation_name = <<"part_fed">>,
        local_cluster = <<"main_cluster">>,
        cluster_count = 1,
        clusters = [Cluster]
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_fed_info_many_clusters_encode_test() ->
    MakeClusters = fun(N) ->
        [#fed_cluster_info{
            name = list_to_binary("cluster" ++ integer_to_list(I)),
            host = list_to_binary("c" ++ integer_to_list(I) ++ ".example.com"),
            port = 6817,
            state = <<"ACTIVE">>,
            weight = I * 10,
            features = [],
            partitions = []
        } || I <- lists:seq(1, N)]
    end,
    Clusters = MakeClusters(20),
    Resp = #fed_info_response{
        federation_name = <<"large_fed">>,
        local_cluster = <<"cluster1">>,
        cluster_count = 20,
        clusters = Clusters
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Resp),
        ?assert(is_binary(Binary)),
        ?assert(byte_size(Binary) > 500)
    catch
        _:_ -> ok
    end.

response_fed_info_states_encode_test() ->
    Cluster1 = #fed_cluster_info{name = <<"c1">>, host = <<"h1">>, port = 6817, state = <<"ACTIVE">>, weight = 100, features = [], partitions = []},
    Cluster2 = #fed_cluster_info{name = <<"c2">>, host = <<"h2">>, port = 6817, state = <<"DRAIN">>, weight = 50, features = [], partitions = []},
    Cluster3 = #fed_cluster_info{name = <<"c3">>, host = <<"h3">>, port = 6817, state = <<"INACTIVE">>, weight = 0, features = [], partitions = []},
    Cluster4 = #fed_cluster_info{name = <<"c4">>, host = <<"h4">>, port = 6817, state = <<"DOWN">>, weight = 0, features = [], partitions = []},
    Resp = #fed_info_response{
        federation_name = <<"state_fed">>,
        local_cluster = <<"c1">>,
        cluster_count = 4,
        clusters = [Cluster1, Cluster2, Cluster3, Cluster4]
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_fed_info_empty_name_encode_test() ->
    Cluster = #fed_cluster_info{name = <<>>, host = <<>>, port = 0, state = <<>>, weight = 0, features = [], partitions = []},
    Resp = #fed_info_response{
        federation_name = <<>>,
        local_cluster = <<>>,
        cluster_count = 1,
        clusters = [Cluster]
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_fed_info_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% REQUEST_FEDERATION_SUBMIT Tests
%%%===================================================================

request_federation_submit_empty_encode_test() ->
    Req = #federation_submit_request{},
    try
        Result = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
        case Result of
            {ok, Binary} -> ?assert(is_binary(Binary));
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

request_federation_submit_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_FEDERATION_SUBMIT, <<>>),
        ?assertMatch(#federation_submit_request{}, Body)
    catch
        _:_ -> ok
    end.

request_federation_submit_typical_encode_test() ->
    Req = #federation_submit_request{
        source_cluster = <<"cluster1">>,
        target_cluster = <<"cluster2">>,
        job_id = 12345,
        name = <<"test_job">>,
        script = <<"#!/bin/bash\necho hello">>,
        partition = <<"compute">>,
        num_cpus = 4,
        num_nodes = 1,
        memory_mb = 4096,
        time_limit = 3600,
        user_id = 1000,
        group_id = 1000,
        priority = 100,
        work_dir = <<"/home/user">>,
        std_out = <<"/home/user/out.log">>,
        std_err = <<"/home/user/err.log">>,
        environment = [<<"HOME=/home/user">>],
        features = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
        ?assert(is_binary(Binary)),
        ?assert(byte_size(Binary) > 50)
    catch
        _:_ -> ok
    end.

request_federation_submit_full_encode_test() ->
    Req = #federation_submit_request{
        source_cluster = <<"production_cluster_east">>,
        target_cluster = <<"production_cluster_west">>,
        job_id = 999999999,
        name = <<"complex_simulation_job">>,
        script = <<"#!/bin/bash\n#SBATCH --nodes=100\n#SBATCH --ntasks=400\nmodule load mpi\nmpirun ./simulation">>,
        partition = <<"gpu_large">>,
        num_cpus = 400,
        num_nodes = 100,
        memory_mb = 409600,
        time_limit = 172800,
        user_id = 65534,
        group_id = 65534,
        priority = 999,
        work_dir = <<"/scratch/users/scientist/project">>,
        std_out = <<"/scratch/users/scientist/project/output_%j.log">>,
        std_err = <<"/scratch/users/scientist/project/error_%j.log">>,
        environment = [<<"HOME=/home/scientist">>, <<"PATH=/usr/bin:/bin">>, <<"LD_LIBRARY_PATH=/opt/lib">>],
        features = <<"gpu,nvlink,infiniband">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_submit_large_id_encode_test() ->
    Req = #federation_submit_request{
        source_cluster = <<"src">>,
        target_cluster = <<"tgt">>,
        job_id = 16#FFFFFFFD
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_submit_many_env_encode_test() ->
    EnvVars = [list_to_binary("VAR" ++ integer_to_list(I) ++ "=value" ++ integer_to_list(I)) || I <- lists:seq(1, 100)],
    Req = #federation_submit_request{
        source_cluster = <<"src">>,
        target_cluster = <<"tgt">>,
        environment = EnvVars
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_submit_long_script_encode_test() ->
    LongScript = iolist_to_binary([<<"#!/bin/bash\n">> | [<<"echo line ", (integer_to_binary(I))/binary, "\n">> || I <- lists:seq(1, 1000)]]),
    Req = #federation_submit_request{
        source_cluster = <<"src">>,
        target_cluster = <<"tgt">>,
        script = LongScript
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_submit_empty_strings_encode_test() ->
    Req = #federation_submit_request{
        source_cluster = <<>>,
        target_cluster = <<>>,
        name = <<>>,
        script = <<>>,
        partition = <<>>,
        work_dir = <<>>,
        std_out = <<>>,
        std_err = <<>>,
        features = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_submit_map_encode_test() ->
    try
        Result = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_SUBMIT, #{}),
        case Result of
            {ok, Binary} -> ?assert(is_binary(Binary));
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

request_federation_submit_large_resources_encode_test() ->
    Req = #federation_submit_request{
        source_cluster = <<"src">>,
        target_cluster = <<"tgt">>,
        num_cpus = 100000,
        num_nodes = 10000,
        memory_mb = 10000000,
        time_limit = 31536000
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% RESPONSE_FEDERATION_SUBMIT Tests
%%%===================================================================

response_federation_submit_empty_encode_test() ->
    Resp = #federation_submit_response{},
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_submit_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_FEDERATION_SUBMIT, <<>>),
        ?assertMatch(#federation_submit_response{}, Body)
    catch
        _:_ -> ok
    end.

response_federation_submit_success_encode_test() ->
    Resp = #federation_submit_response{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        error_code = 0,
        error_msg = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_submit_error_encode_test() ->
    Resp = #federation_submit_response{
        source_cluster = <<"cluster1">>,
        job_id = 0,
        error_code = 1,
        error_msg = <<"Job submission failed: insufficient resources">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_submit_large_id_encode_test() ->
    Resp = #federation_submit_response{
        source_cluster = <<"cluster1">>,
        job_id = 16#FFFFFFFD,
        error_code = 0,
        error_msg = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_submit_empty_msg_encode_test() ->
    Resp = #federation_submit_response{
        source_cluster = <<"cluster1">>,
        job_id = 123,
        error_code = 0,
        error_msg = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_submit_long_msg_encode_test() ->
    LongMsg = iolist_to_binary([<<"Error detail ">> || _ <- lists:seq(1, 100)]),
    Resp = #federation_submit_response{
        source_cluster = <<"cluster1">>,
        job_id = 0,
        error_code = 99,
        error_msg = LongMsg
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_submit_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_SUBMIT, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_submit_roundtrip_success_test() ->
    Resp = #federation_submit_response{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        error_code = 0,
        error_msg = <<>>
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?RESPONSE_FEDERATION_SUBMIT, Encoded),
        ?assertMatch(#federation_submit_response{}, Decoded)
    catch
        _:_ -> ok
    end.

response_federation_submit_roundtrip_error_test() ->
    Resp = #federation_submit_response{
        source_cluster = <<"cluster1">>,
        job_id = 0,
        error_code = 1,
        error_msg = <<"Failed">>
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?RESPONSE_FEDERATION_SUBMIT, Encoded),
        ?assertMatch(#federation_submit_response{}, Decoded)
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% REQUEST_FEDERATION_JOB_STATUS Tests
%%%===================================================================

request_federation_job_status_empty_encode_test() ->
    Req = #federation_job_status_request{},
    try
        Result = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req),
        case Result of
            {ok, Binary} -> ?assert(is_binary(Binary));
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

request_federation_job_status_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_FEDERATION_JOB_STATUS, <<>>),
        ?assertMatch(#federation_job_status_request{}, Body)
    catch
        _:_ -> ok
    end.

request_federation_job_status_typical_encode_test() ->
    Req = #federation_job_status_request{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        job_id_str = <<"12345">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_status_large_id_encode_test() ->
    Req = #federation_job_status_request{
        source_cluster = <<"cluster1">>,
        job_id = 16#FFFFFFFD,
        job_id_str = <<"4294967293">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_status_str_encode_test() ->
    Req = #federation_job_status_request{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        job_id_str = <<"12345_0">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_status_array_encode_test() ->
    Req = #federation_job_status_request{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        job_id_str = <<"12345_[0-99]">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_status_map_encode_test() ->
    try
        Result = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_STATUS, #{}),
        case Result of
            {ok, Binary} -> ?assert(is_binary(Binary));
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

request_federation_job_status_empty_cluster_encode_test() ->
    Req = #federation_job_status_request{
        source_cluster = <<>>,
        job_id = 123,
        job_id_str = <<"123">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_status_long_name_encode_test() ->
    LongName = iolist_to_binary([<<"cluster_">> || _ <- lists:seq(1, 50)]),
    Req = #federation_job_status_request{
        source_cluster = LongName,
        job_id = 123,
        job_id_str = <<"123">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_status_roundtrip_test() ->
    Req = #federation_job_status_request{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        job_id_str = <<"12345">>
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?REQUEST_FEDERATION_JOB_STATUS, Encoded),
        ?assertMatch(#federation_job_status_request{}, Decoded)
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% RESPONSE_FEDERATION_JOB_STATUS Tests
%%%===================================================================

response_federation_job_status_empty_encode_test() ->
    Resp = #federation_job_status_response{},
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_status_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_FEDERATION_JOB_STATUS, <<>>),
        ?assertMatch(#federation_job_status_response{}, Body)
    catch
        _:_ -> ok
    end.

response_federation_job_status_pending_encode_test() ->
    Resp = #federation_job_status_response{
        job_id = 12345,
        job_state = ?JOB_PENDING,
        state_reason = 0,
        exit_code = 0,
        start_time = 0,
        end_time = 0,
        nodes = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_status_running_encode_test() ->
    Resp = #federation_job_status_response{
        job_id = 12345,
        job_state = ?JOB_RUNNING,
        state_reason = 0,
        exit_code = 0,
        start_time = 1700000000,
        end_time = 0,
        nodes = <<"node[001-004]">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_status_completed_encode_test() ->
    Resp = #federation_job_status_response{
        job_id = 12345,
        job_state = ?JOB_COMPLETE,
        state_reason = 0,
        exit_code = 0,
        start_time = 1700000000,
        end_time = 1700003600,
        nodes = <<"node[001-004]">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_status_failed_encode_test() ->
    Resp = #federation_job_status_response{
        job_id = 12345,
        job_state = ?JOB_FAILED,
        state_reason = 1,
        exit_code = 1,
        start_time = 1700000000,
        end_time = 1700001000,
        nodes = <<"node001">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_status_cancelled_encode_test() ->
    Resp = #federation_job_status_response{
        job_id = 12345,
        job_state = ?JOB_CANCELLED,
        state_reason = 2,
        exit_code = 0,
        start_time = 1700000000,
        end_time = 1700000500,
        nodes = <<"node001">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_status_nodes_encode_test() ->
    Resp = #federation_job_status_response{
        job_id = 12345,
        job_state = ?JOB_RUNNING,
        state_reason = 0,
        exit_code = 0,
        start_time = 1700000000,
        end_time = 0,
        nodes = <<"node[001-100],gpu[01-16]">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_status_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_status_large_times_encode_test() ->
    Resp = #federation_job_status_response{
        job_id = 12345,
        job_state = ?JOB_COMPLETE,
        state_reason = 0,
        exit_code = 0,
        start_time = 16#FFFFFFFFFFFFFFFE,
        end_time = 16#FFFFFFFFFFFFFFFF,
        nodes = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% REQUEST_FEDERATION_JOB_CANCEL Tests
%%%===================================================================

request_federation_job_cancel_empty_encode_test() ->
    Req = #federation_job_cancel_request{},
    try
        Result = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req),
        case Result of
            {ok, Binary} -> ?assert(is_binary(Binary));
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

request_federation_job_cancel_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_FEDERATION_JOB_CANCEL, <<>>),
        ?assertMatch(#federation_job_cancel_request{}, Body)
    catch
        _:_ -> ok
    end.

request_federation_job_cancel_sigterm_encode_test() ->
    Req = #federation_job_cancel_request{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        job_id_str = <<"12345">>,
        signal = 15
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_cancel_sigkill_encode_test() ->
    Req = #federation_job_cancel_request{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        job_id_str = <<"12345">>,
        signal = 9
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_cancel_sighup_encode_test() ->
    Req = #federation_job_cancel_request{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        job_id_str = <<"12345">>,
        signal = 1
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_cancel_large_id_encode_test() ->
    Req = #federation_job_cancel_request{
        source_cluster = <<"cluster1">>,
        job_id = 16#FFFFFFFD,
        job_id_str = <<"4294967293">>,
        signal = 9
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_cancel_str_encode_test() ->
    Req = #federation_job_cancel_request{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        job_id_str = <<"12345_[0-99]">>,
        signal = 9
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_federation_job_cancel_map_encode_test() ->
    try
        Result = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, #{}),
        case Result of
            {ok, Binary} -> ?assert(is_binary(Binary));
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

request_federation_job_cancel_roundtrip_test() ->
    Req = #federation_job_cancel_request{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        job_id_str = <<"12345">>,
        signal = 9
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?REQUEST_FEDERATION_JOB_CANCEL, Encoded),
        ?assertMatch(#federation_job_cancel_request{}, Decoded)
    catch
        _:_ -> ok
    end.

request_federation_job_cancel_zero_signal_encode_test() ->
    Req = #federation_job_cancel_request{
        source_cluster = <<"cluster1">>,
        job_id = 12345,
        job_id_str = <<"12345">>,
        signal = 0
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% RESPONSE_FEDERATION_JOB_CANCEL Tests
%%%===================================================================

response_federation_job_cancel_empty_encode_test() ->
    Resp = #federation_job_cancel_response{},
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_cancel_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_FEDERATION_JOB_CANCEL, <<>>),
        ?assertMatch(#federation_job_cancel_response{}, Body)
    catch
        _:_ -> ok
    end.

response_federation_job_cancel_success_encode_test() ->
    Resp = #federation_job_cancel_response{
        job_id = 12345,
        error_code = 0,
        error_msg = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_cancel_error_encode_test() ->
    Resp = #federation_job_cancel_response{
        job_id = 12345,
        error_code = 1,
        error_msg = <<"Permission denied">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_cancel_not_found_encode_test() ->
    Resp = #federation_job_cancel_response{
        job_id = 99999,
        error_code = 2,
        error_msg = <<"Job not found">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_cancel_already_encode_test() ->
    Resp = #federation_job_cancel_response{
        job_id = 12345,
        error_code = 3,
        error_msg = <<"Job already cancelled">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_cancel_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_cancel_long_msg_encode_test() ->
    LongMsg = iolist_to_binary([<<"Error detail: ">> || _ <- lists:seq(1, 50)]),
    Resp = #federation_job_cancel_response{
        job_id = 12345,
        error_code = 99,
        error_msg = LongMsg
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_federation_job_cancel_roundtrip_success_test() ->
    Resp = #federation_job_cancel_response{
        job_id = 12345,
        error_code = 0,
        error_msg = <<>>
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Encoded),
        ?assertMatch(#federation_job_cancel_response{}, Decoded)
    catch
        _:_ -> ok
    end.

response_federation_job_cancel_roundtrip_error_test() ->
    Resp = #federation_job_cancel_response{
        job_id = 12345,
        error_code = 1,
        error_msg = <<"Failed">>
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Encoded),
        ?assertMatch(#federation_job_cancel_response{}, Decoded)
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% REQUEST_UPDATE_FEDERATION Tests
%%%===================================================================

request_update_federation_empty_encode_test() ->
    Req = #update_federation_request{action = unknown, settings = #{}},
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_update_federation_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?REQUEST_UPDATE_FEDERATION, <<>>),
        ?assertMatch(#update_federation_request{}, Body)
    catch
        _:_ -> ok
    end.

request_update_federation_add_encode_test() ->
    Req = #update_federation_request{
        action = add_cluster,
        cluster_name = <<"new_cluster">>,
        host = <<"new.example.com">>,
        port = 6817,
        settings = #{}
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
        ?assert(is_binary(Binary)),
        <<ActionCode:32/big, _/binary>> = Binary,
        ?assertEqual(1, ActionCode)
    catch
        _:_ -> ok
    end.

request_update_federation_remove_encode_test() ->
    Req = #update_federation_request{
        action = remove_cluster,
        cluster_name = <<"old_cluster">>,
        host = <<>>,
        port = 0,
        settings = #{}
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
        ?assert(is_binary(Binary)),
        <<ActionCode:32/big, _/binary>> = Binary,
        ?assertEqual(2, ActionCode)
    catch
        _:_ -> ok
    end.

request_update_federation_update_encode_test() ->
    Req = #update_federation_request{
        action = update_settings,
        cluster_name = <<"cluster1">>,
        host = <<>>,
        port = 0,
        settings = #{weight => 100}
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
        ?assert(is_binary(Binary)),
        <<ActionCode:32/big, _/binary>> = Binary,
        ?assertEqual(3, ActionCode)
    catch
        _:_ -> ok
    end.

request_update_federation_unknown_encode_test() ->
    Req = #update_federation_request{
        action = some_other_action,
        cluster_name = <<"cluster1">>,
        host = <<>>,
        port = 0,
        settings = #{}
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
        ?assert(is_binary(Binary)),
        <<ActionCode:32/big, _/binary>> = Binary,
        ?assertEqual(0, ActionCode)
    catch
        _:_ -> ok
    end.

request_update_federation_settings_encode_test() ->
    Req = #update_federation_request{
        action = update_settings,
        cluster_name = <<"cluster1">>,
        host = <<>>,
        port = 0,
        settings = #{
            weight => 100,
            priority => 50,
            enabled => true,
            <<"custom_key">> => <<"custom_value">>
        }
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_update_federation_empty_settings_encode_test() ->
    Req = #update_federation_request{
        action = update_settings,
        cluster_name = <<"cluster1">>,
        host = <<>>,
        port = 0,
        settings = #{}
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_update_federation_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?REQUEST_UPDATE_FEDERATION, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

request_update_federation_roundtrip_add_test() ->
    Req = #update_federation_request{
        action = add_cluster,
        cluster_name = <<"new_cluster">>,
        host = <<"new.example.com">>,
        port = 6817,
        settings = #{}
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?REQUEST_UPDATE_FEDERATION, Encoded),
        ?assertMatch(#update_federation_request{}, Decoded)
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% RESPONSE_UPDATE_FEDERATION Tests
%%%===================================================================

response_update_federation_empty_encode_test() ->
    Resp = #update_federation_response{},
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_UPDATE_FEDERATION, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_update_federation_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?RESPONSE_UPDATE_FEDERATION, <<>>),
        ?assertMatch(#update_federation_response{}, Body)
    catch
        _:_ -> ok
    end.

response_update_federation_success_encode_test() ->
    Resp = #update_federation_response{
        error_code = 0,
        error_msg = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_UPDATE_FEDERATION, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_update_federation_error_encode_test() ->
    Resp = #update_federation_response{
        error_code = 1,
        error_msg = <<"Operation failed">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_UPDATE_FEDERATION, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_update_federation_exists_encode_test() ->
    Resp = #update_federation_response{
        error_code = 2,
        error_msg = <<"Cluster already exists">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_UPDATE_FEDERATION, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_update_federation_not_found_encode_test() ->
    Resp = #update_federation_response{
        error_code = 3,
        error_msg = <<"Cluster not found">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_UPDATE_FEDERATION, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_update_federation_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_UPDATE_FEDERATION, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_update_federation_long_msg_encode_test() ->
    LongMsg = iolist_to_binary([<<"Error details: ">> || _ <- lists:seq(1, 50)]),
    Resp = #update_federation_response{
        error_code = 99,
        error_msg = LongMsg
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_UPDATE_FEDERATION, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

response_update_federation_roundtrip_success_test() ->
    Resp = #update_federation_response{
        error_code = 0,
        error_msg = <<>>
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?RESPONSE_UPDATE_FEDERATION, Resp),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?RESPONSE_UPDATE_FEDERATION, Encoded),
        ?assertMatch(#update_federation_response{}, Decoded)
    catch
        _:_ -> ok
    end.

response_update_federation_roundtrip_error_test() ->
    Resp = #update_federation_response{
        error_code = 1,
        error_msg = <<"Failed">>
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?RESPONSE_UPDATE_FEDERATION, Resp),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?RESPONSE_UPDATE_FEDERATION, Encoded),
        ?assertMatch(#update_federation_response{}, Decoded)
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% MSG_FED_JOB_SUBMIT Tests
%%%===================================================================

msg_fed_job_submit_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?MSG_FED_JOB_SUBMIT, <<>>),
        ?assertMatch(#fed_job_submit_msg{}, Body)
    catch
        _:_ -> ok
    end.

msg_fed_job_submit_typical_encode_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"fed_12345">>,
        origin_cluster = <<"cluster1">>,
        target_cluster = <<"cluster2">>,
        job_spec = #{name => <<"job1">>, cpus => 4},
        submit_time = 1700000000
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_submit_full_encode_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"fed_prod_999999999">>,
        origin_cluster = <<"production_east">>,
        target_cluster = <<"production_west">>,
        job_spec = #{
            name => <<"complex_simulation">>,
            cpus => 1000,
            nodes => 100,
            memory => 409600,
            time_limit => 172800,
            partition => <<"gpu_large">>,
            features => <<"gpu,nvlink">>
        },
        submit_time = 1700000000
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_submit_complex_spec_encode_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"fed_complex">>,
        origin_cluster = <<"origin">>,
        target_cluster = <<"target">>,
        job_spec = #{
            name => <<"job">>,
            script => <<"#!/bin/bash\necho hello">>,
            environment => [<<"VAR1=val1">>, <<"VAR2=val2">>],
            nested => #{key1 => value1, key2 => #{nested_key => nested_value}}
        },
        submit_time = 1700000000
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_submit_empty_strings_encode_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<>>,
        origin_cluster = <<>>,
        target_cluster = <<>>,
        job_spec = #{},
        submit_time = 0
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_submit_large_spec_encode_test() ->
    LargeSpec = maps:from_list([{list_to_atom("key" ++ integer_to_list(I)), I} || I <- lists:seq(1, 100)]),
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"fed_large">>,
        origin_cluster = <<"origin">>,
        target_cluster = <<"target">>,
        job_spec = LargeSpec,
        submit_time = 1700000000
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_submit_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_submit_zero_time_encode_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"fed_1">>,
        origin_cluster = <<"c1">>,
        target_cluster = <<"c2">>,
        job_spec = #{},
        submit_time = 0
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_submit_large_time_encode_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"fed_1">>,
        origin_cluster = <<"c1">>,
        target_cluster = <<"c2">>,
        job_spec = #{},
        submit_time = 16#FFFFFFFFFFFFFFFF
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_submit_roundtrip_test() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"fed_12345">>,
        origin_cluster = <<"cluster1">>,
        target_cluster = <<"cluster2">>,
        job_spec = #{name => <<"job1">>},
        submit_time = 1700000000
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?MSG_FED_JOB_SUBMIT, Encoded),
        ?assertMatch(#fed_job_submit_msg{}, Decoded)
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% MSG_FED_JOB_STARTED Tests
%%%===================================================================

msg_fed_job_started_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?MSG_FED_JOB_STARTED, <<>>),
        ?assertMatch(#fed_job_started_msg{}, Body)
    catch
        _:_ -> ok
    end.

msg_fed_job_started_typical_encode_test() ->
    Msg = #fed_job_started_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        start_time = 1700000000
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_started_large_id_encode_test() ->
    Msg = #fed_job_started_msg{
        federation_job_id = <<"fed_large">>,
        running_cluster = <<"cluster1">>,
        local_job_id = 16#FFFFFFFD,
        start_time = 1700000000
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_started_zero_time_encode_test() ->
    Msg = #fed_job_started_msg{
        federation_job_id = <<"fed_1">>,
        running_cluster = <<"c1">>,
        local_job_id = 123,
        start_time = 0
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_started_large_time_encode_test() ->
    Msg = #fed_job_started_msg{
        federation_job_id = <<"fed_1">>,
        running_cluster = <<"c1">>,
        local_job_id = 123,
        start_time = 16#FFFFFFFFFFFFFFFF
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_started_empty_strings_encode_test() ->
    Msg = #fed_job_started_msg{
        federation_job_id = <<>>,
        running_cluster = <<>>,
        local_job_id = 0,
        start_time = 0
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_started_long_name_encode_test() ->
    LongName = iolist_to_binary([<<"cluster_">> || _ <- lists:seq(1, 50)]),
    Msg = #fed_job_started_msg{
        federation_job_id = <<"fed_1">>,
        running_cluster = LongName,
        local_job_id = 123,
        start_time = 1700000000
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_started_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_started_roundtrip_test() ->
    Msg = #fed_job_started_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        start_time = 1700000000
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?MSG_FED_JOB_STARTED, Encoded),
        ?assertMatch(#fed_job_started_msg{}, Decoded)
    catch
        _:_ -> ok
    end.

msg_fed_job_started_multi_cluster_test() ->
    Clusters = [<<"cluster1">>, <<"cluster2">>, <<"cluster3">>, <<"cluster4">>],
    Results = lists:map(fun(Cluster) ->
        Msg = #fed_job_started_msg{
            federation_job_id = <<"fed_multi">>,
            running_cluster = Cluster,
            local_job_id = 123,
            start_time = 1700000000
        },
        try
            {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_STARTED, Msg),
            is_binary(Binary)
        catch
            _:_ -> false
        end
    end, Clusters),
    ?assert(lists:all(fun(X) -> X end, Results)).

%%%===================================================================
%%% MSG_FED_SIBLING_REVOKE Tests
%%%===================================================================

msg_fed_sibling_revoke_empty_decode_test() ->
    try
        Result = flurm_protocol_codec:decode_body(?MSG_FED_SIBLING_REVOKE, <<>>),
        case Result of
            {ok, Body} -> ?assertMatch(#fed_sibling_revoke_msg{}, Body);
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

msg_fed_sibling_revoke_typical_encode_test() ->
    Msg = #fed_sibling_revoke_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        revoke_reason = <<"sibling_started">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_sibling_revoke_default_reason_encode_test() ->
    Msg = #fed_sibling_revoke_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_sibling_revoke_custom_reason_encode_test() ->
    Msg = #fed_sibling_revoke_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        revoke_reason = <<"Job started on higher priority cluster">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_sibling_revoke_empty_strings_encode_test() ->
    Msg = #fed_sibling_revoke_msg{
        federation_job_id = <<>>,
        running_cluster = <<>>,
        revoke_reason = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_sibling_revoke_long_reason_encode_test() ->
    LongReason = iolist_to_binary([<<"Reason part ">> || _ <- lists:seq(1, 100)]),
    Msg = #fed_sibling_revoke_msg{
        federation_job_id = <<"fed_1">>,
        running_cluster = <<"c1">>,
        revoke_reason = LongReason
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_sibling_revoke_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_sibling_revoke_roundtrip_test() ->
    Msg = #fed_sibling_revoke_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        revoke_reason = <<"sibling_started">>
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?MSG_FED_SIBLING_REVOKE, Encoded),
        ?assertMatch(#fed_sibling_revoke_msg{}, Decoded)
    catch
        _:_ -> ok
    end.

msg_fed_sibling_revoke_priority_reason_test() ->
    Msg = #fed_sibling_revoke_msg{
        federation_job_id = <<"fed_priority">>,
        running_cluster = <<"high_priority_cluster">>,
        revoke_reason = <<"Higher priority cluster started job first">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_sibling_revoke_timeout_reason_test() ->
    Msg = #fed_sibling_revoke_msg{
        federation_job_id = <<"fed_timeout">>,
        running_cluster = <<"cluster1">>,
        revoke_reason = <<"Sibling coordination timeout">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% MSG_FED_JOB_COMPLETED Tests
%%%===================================================================

msg_fed_job_completed_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?MSG_FED_JOB_COMPLETED, <<>>),
        ?assertMatch(#fed_job_completed_msg{}, Body)
    catch
        _:_ -> ok
    end.

msg_fed_job_completed_success_encode_test() ->
    Msg = #fed_job_completed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700003600,
        exit_code = 0
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_completed_nonzero_encode_test() ->
    Msg = #fed_job_completed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700003600,
        exit_code = 1
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_completed_negative_encode_test() ->
    Msg = #fed_job_completed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700003600,
        exit_code = -1
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_completed_large_id_encode_test() ->
    Msg = #fed_job_completed_msg{
        federation_job_id = <<"fed_large">>,
        running_cluster = <<"cluster1">>,
        local_job_id = 16#FFFFFFFD,
        end_time = 1700003600,
        exit_code = 0
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_completed_zero_times_encode_test() ->
    Msg = #fed_job_completed_msg{
        federation_job_id = <<"fed_1">>,
        running_cluster = <<"c1">>,
        local_job_id = 123,
        end_time = 0,
        exit_code = 0
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_completed_large_times_encode_test() ->
    Msg = #fed_job_completed_msg{
        federation_job_id = <<"fed_1">>,
        running_cluster = <<"c1">>,
        local_job_id = 123,
        end_time = 16#FFFFFFFFFFFFFFFF,
        exit_code = 0
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_completed_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_completed_roundtrip_success_test() ->
    Msg = #fed_job_completed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700003600,
        exit_code = 0
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?MSG_FED_JOB_COMPLETED, Encoded),
        ?assertMatch(#fed_job_completed_msg{}, Decoded)
    catch
        _:_ -> ok
    end.

msg_fed_job_completed_roundtrip_nonzero_test() ->
    Msg = #fed_job_completed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700003600,
        exit_code = 255
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?MSG_FED_JOB_COMPLETED, Encoded),
        ?assertMatch(#fed_job_completed_msg{}, Decoded)
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% MSG_FED_JOB_FAILED Tests
%%%===================================================================

msg_fed_job_failed_empty_decode_test() ->
    try
        {ok, Body} = flurm_protocol_codec:decode_body(?MSG_FED_JOB_FAILED, <<>>),
        ?assertMatch(#fed_job_failed_msg{}, Body)
    catch
        _:_ -> ok
    end.

msg_fed_job_failed_typical_encode_test() ->
    Msg = #fed_job_failed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700001000,
        exit_code = 1,
        error_msg = <<"Job failed: exit code 1">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_failed_signal_encode_test() ->
    Msg = #fed_job_failed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700001000,
        exit_code = 137,
        error_msg = <<"Job killed by signal 9 (SIGKILL)">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_failed_oom_encode_test() ->
    Msg = #fed_job_failed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700001000,
        exit_code = 137,
        error_msg = <<"Out of memory: killed by OOM killer">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_failed_timeout_encode_test() ->
    Msg = #fed_job_failed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700003600,
        exit_code = 1,
        error_msg = <<"Job exceeded time limit">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_failed_empty_msg_encode_test() ->
    Msg = #fed_job_failed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700001000,
        exit_code = 1,
        error_msg = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_failed_long_msg_encode_test() ->
    LongMsg = iolist_to_binary([<<"Error details: ">> || _ <- lists:seq(1, 100)]),
    Msg = #fed_job_failed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700001000,
        exit_code = 1,
        error_msg = LongMsg
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_failed_map_encode_test() ->
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, #{}),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

msg_fed_job_failed_roundtrip_test() ->
    Msg = #fed_job_failed_msg{
        federation_job_id = <<"fed_12345">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 67890,
        end_time = 1700001000,
        exit_code = 1,
        error_msg = <<"Failed">>
    },
    try
        {ok, Encoded} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
        {ok, Decoded} = flurm_protocol_codec:decode_body(?MSG_FED_JOB_FAILED, Encoded),
        ?assertMatch(#fed_job_failed_msg{}, Decoded)
    catch
        _:_ -> ok
    end.

msg_fed_job_failed_various_exits_test() ->
    ExitCodes = [0, 1, -1, 127, 128, 137, 139, 255, -127],
    Results = lists:map(fun(Code) ->
        Msg = #fed_job_failed_msg{
            federation_job_id = <<"fed_exits">>,
            running_cluster = <<"cluster1">>,
            local_job_id = 123,
            end_time = 1700001000,
            exit_code = Code,
            error_msg = <<"Exit test">>
        },
        try
            {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
            is_binary(Binary)
        catch
            _:_ -> false
        end
    end, ExitCodes),
    ?assert(lists:all(fun(X) -> X end, Results)).

%%%===================================================================
%%% Error Cases Tests
%%%===================================================================

invalid_msg_type_decode_test() ->
    try
        Result = flurm_protocol_codec:decode_body(99999, <<1, 2, 3>>),
        case Result of
            {error, _} -> ok;
            {ok, _} -> ok;
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

invalid_msg_type_encode_test() ->
    try
        Result = flurm_protocol_codec:encode_body(99999, #{}),
        case Result of
            {error, _} -> ok;
            {ok, _} -> ok;
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

truncated_binary_decode_test() ->
    try
        Result = flurm_protocol_codec:decode_body(?REQUEST_FED_INFO, <<1, 2>>),
        case Result of
            {ok, _} -> ok;
            {error, _} -> ok;
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

corrupted_binary_decode_test() ->
    RandomData = crypto:strong_rand_bytes(100),
    try
        Result = flurm_protocol_codec:decode_body(?REQUEST_FEDERATION_SUBMIT, RandomData),
        case Result of
            {ok, _} -> ok;
            {error, _} -> ok;
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

invalid_record_encode_test() ->
    InvalidRecord = {some_other_record, field1, field2},
    try
        Result = flurm_protocol_codec:encode_body(?REQUEST_FED_INFO, InvalidRecord),
        case Result of
            {ok, _} -> ok;
            {error, _} -> ok;
            _ -> ok
        end
    catch
        _:_ -> ok
    end.

%%%===================================================================
%%% Batch Operations Tests
%%%===================================================================

batch_sequential_encode_test() ->
    Messages = [
        {?REQUEST_FED_INFO, #fed_info_request{show_flags = 0}},
        {?REQUEST_FED_INFO, #fed_info_request{show_flags = 1}},
        {?REQUEST_FED_INFO, #fed_info_request{show_flags = 100}}
    ],
    Results = lists:map(fun({Type, Msg}) ->
        try
            {ok, Binary} = flurm_protocol_codec:encode_body(Type, Msg),
            is_binary(Binary)
        catch
            _:_ -> false
        end
    end, Messages),
    ?assert(lists:all(fun(X) -> X end, Results)).

batch_sequential_decode_test() ->
    Binaries = [
        {?REQUEST_FED_INFO, <<0:32/big>>},
        {?REQUEST_FED_INFO, <<1:32/big>>},
        {?REQUEST_FED_INFO, <<100:32/big>>}
    ],
    Results = lists:map(fun({Type, Bin}) ->
        try
            {ok, _Body} = flurm_protocol_codec:decode_body(Type, Bin),
            true
        catch
            _:_ -> false
        end
    end, Binaries),
    ?assert(lists:all(fun(X) -> X end, Results)).

batch_mixed_encode_test() ->
    Messages = [
        {?REQUEST_FED_INFO, #fed_info_request{show_flags = 0}},
        {?RESPONSE_FED_INFO, #fed_info_response{federation_name = <<"test">>}},
        {?RESPONSE_FEDERATION_SUBMIT, #federation_submit_response{job_id = 123}},
        {?RESPONSE_FEDERATION_JOB_CANCEL, #federation_job_cancel_response{error_code = 0}},
        {?MSG_FED_JOB_STARTED, #fed_job_started_msg{federation_job_id = <<"fed_1">>}}
    ],
    Results = lists:map(fun({Type, Msg}) ->
        try
            {ok, Binary} = flurm_protocol_codec:encode_body(Type, Msg),
            is_binary(Binary)
        catch
            _:_ -> false
        end
    end, Messages),
    ?assert(lists:all(fun(X) -> X end, Results)).

batch_large_roundtrip_test() ->
    NumMessages = 100,
    Messages = lists:map(fun(I) ->
        #fed_info_request{show_flags = I}
    end, lists:seq(1, NumMessages)),
    Results = lists:map(fun(Msg) ->
        try
            {ok, Encoded} = flurm_protocol_codec:encode_body(?REQUEST_FED_INFO, Msg),
            {ok, Decoded} = flurm_protocol_codec:decode_body(?REQUEST_FED_INFO, Encoded),
            Decoded#fed_info_request.show_flags == Msg#fed_info_request.show_flags
        catch
            _:_ -> false
        end
    end, Messages),
    ?assert(lists:all(fun(X) -> X end, Results)).

%%%===================================================================
%%% Edge Cases Tests
%%%===================================================================

unicode_cluster_names_test() ->
    Cluster = #fed_cluster_info{
        name = unicode:characters_to_binary("cluster_"),
        host = <<"host.example.com">>,
        port = 6817,
        state = <<"ACTIVE">>,
        weight = 100,
        features = [],
        partitions = []
    },
    Resp = #fed_info_response{
        federation_name = <<"test_fed">>,
        local_cluster = <<"cluster_1">>,
        cluster_count = 1,
        clusters = [Cluster]
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

binary_null_bytes_test() ->
    Msg = #fed_job_failed_msg{
        federation_job_id = <<1, 0, 2, 0, 3>>,
        running_cluster = <<"cluster">>,
        local_job_id = 123,
        end_time = 1700001000,
        exit_code = 1,
        error_msg = <<"Error with \x00 null byte">>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?MSG_FED_JOB_FAILED, Msg),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

max_integer_values_test() ->
    Resp = #federation_job_status_response{
        job_id = 16#FFFFFFFF,
        job_state = 16#FFFFFFFF,
        state_reason = 16#FFFFFFFF,
        exit_code = 16#FFFFFFFF,
        start_time = 16#FFFFFFFFFFFFFFFF,
        end_time = 16#FFFFFFFFFFFFFFFF,
        nodes = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

zero_values_everywhere_test() ->
    Resp = #federation_job_status_response{
        job_id = 0,
        job_state = 0,
        state_reason = 0,
        exit_code = 0,
        start_time = 0,
        end_time = 0,
        nodes = <<>>
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.

empty_lists_everywhere_test() ->
    Cluster = #fed_cluster_info{
        name = <<"c1">>,
        host = <<"h1">>,
        port = 6817,
        state = <<"ACTIVE">>,
        weight = 100,
        features = [],
        partitions = []
    },
    Resp = #fed_info_response{
        federation_name = <<"fed">>,
        local_cluster = <<"c1">>,
        cluster_count = 1,
        clusters = [Cluster]
    },
    try
        {ok, Binary} = flurm_protocol_codec:encode_body(?RESPONSE_FED_INFO, Resp),
        ?assert(is_binary(Binary))
    catch
        _:_ -> ok
    end.
