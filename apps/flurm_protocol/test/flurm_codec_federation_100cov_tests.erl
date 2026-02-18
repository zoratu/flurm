%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_codec_federation module
%%% Coverage target: 100% of all functions and branches
%%%-------------------------------------------------------------------
-module(flurm_codec_federation_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Generator
%%%===================================================================

flurm_codec_federation_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% Decode REQUEST_FED_INFO tests
      {"decode REQUEST_FED_INFO empty",
       fun decode_fed_info_request_empty/0},
      {"decode REQUEST_FED_INFO with show_flags",
       fun decode_fed_info_request_with_flags/0},
      {"decode REQUEST_FED_INFO other",
       fun decode_fed_info_request_other/0},

      %% Decode RESPONSE_FED_INFO tests
      {"decode RESPONSE_FED_INFO full",
       fun decode_fed_info_response_full/0},
      {"decode RESPONSE_FED_INFO empty",
       fun decode_fed_info_response_empty/0},

      %% Decode REQUEST_FEDERATION_SUBMIT tests
      {"decode REQUEST_FEDERATION_SUBMIT full",
       fun decode_federation_submit_full/0},
      {"decode REQUEST_FEDERATION_SUBMIT minimal",
       fun decode_federation_submit_minimal/0},

      %% Decode RESPONSE_FEDERATION_SUBMIT tests
      {"decode RESPONSE_FEDERATION_SUBMIT full",
       fun decode_federation_submit_response_full/0},
      {"decode RESPONSE_FEDERATION_SUBMIT empty",
       fun decode_federation_submit_response_empty/0},

      %% Decode REQUEST_FEDERATION_JOB_STATUS tests
      {"decode REQUEST_FEDERATION_JOB_STATUS full",
       fun decode_federation_job_status_full/0},
      {"decode REQUEST_FEDERATION_JOB_STATUS empty",
       fun decode_federation_job_status_empty/0},

      %% Decode RESPONSE_FEDERATION_JOB_STATUS tests
      {"decode RESPONSE_FEDERATION_JOB_STATUS full",
       fun decode_federation_job_status_response_full/0},
      {"decode RESPONSE_FEDERATION_JOB_STATUS empty",
       fun decode_federation_job_status_response_empty/0},

      %% Decode REQUEST_FEDERATION_JOB_CANCEL tests
      {"decode REQUEST_FEDERATION_JOB_CANCEL full",
       fun decode_federation_job_cancel_full/0},
      {"decode REQUEST_FEDERATION_JOB_CANCEL empty",
       fun decode_federation_job_cancel_empty/0},

      %% Decode RESPONSE_FEDERATION_JOB_CANCEL tests
      {"decode RESPONSE_FEDERATION_JOB_CANCEL full",
       fun decode_federation_job_cancel_response_full/0},
      {"decode RESPONSE_FEDERATION_JOB_CANCEL empty",
       fun decode_federation_job_cancel_response_empty/0},

      %% Decode REQUEST_UPDATE_FEDERATION tests
      {"decode REQUEST_UPDATE_FEDERATION add_cluster",
       fun decode_update_federation_add_cluster/0},
      {"decode REQUEST_UPDATE_FEDERATION remove_cluster",
       fun decode_update_federation_remove_cluster/0},
      {"decode REQUEST_UPDATE_FEDERATION update_settings",
       fun decode_update_federation_update_settings/0},
      {"decode REQUEST_UPDATE_FEDERATION unknown action",
       fun decode_update_federation_unknown/0},
      {"decode REQUEST_UPDATE_FEDERATION empty",
       fun decode_update_federation_empty/0},

      %% Decode RESPONSE_UPDATE_FEDERATION tests
      {"decode RESPONSE_UPDATE_FEDERATION full",
       fun decode_update_federation_response_full/0},
      {"decode RESPONSE_UPDATE_FEDERATION empty",
       fun decode_update_federation_response_empty/0},

      %% Decode MSG_FED_JOB_SUBMIT tests
      {"decode MSG_FED_JOB_SUBMIT full",
       fun decode_fed_job_submit_full/0},
      {"decode MSG_FED_JOB_SUBMIT empty",
       fun decode_fed_job_submit_empty/0},

      %% Decode MSG_FED_JOB_STARTED tests
      {"decode MSG_FED_JOB_STARTED full",
       fun decode_fed_job_started_full/0},
      {"decode MSG_FED_JOB_STARTED empty",
       fun decode_fed_job_started_empty/0},

      %% Decode MSG_FED_SIBLING_REVOKE tests
      {"decode MSG_FED_SIBLING_REVOKE full",
       fun decode_fed_sibling_revoke_full/0},
      {"decode MSG_FED_SIBLING_REVOKE empty",
       fun decode_fed_sibling_revoke_empty/0},

      %% Decode MSG_FED_JOB_COMPLETED tests
      {"decode MSG_FED_JOB_COMPLETED full",
       fun decode_fed_job_completed_full/0},
      {"decode MSG_FED_JOB_COMPLETED empty",
       fun decode_fed_job_completed_empty/0},

      %% Decode MSG_FED_JOB_FAILED tests
      {"decode MSG_FED_JOB_FAILED full",
       fun decode_fed_job_failed_full/0},
      {"decode MSG_FED_JOB_FAILED empty",
       fun decode_fed_job_failed_empty/0},

      %% Decode unsupported tests
      {"decode unsupported message type",
       fun decode_unsupported/0},

      %% Encode REQUEST_FED_INFO tests
      {"encode REQUEST_FED_INFO record",
       fun encode_fed_info_request_record/0},
      {"encode REQUEST_FED_INFO non-record",
       fun encode_fed_info_request_non_record/0},

      %% Encode RESPONSE_FED_INFO tests
      {"encode RESPONSE_FED_INFO record with clusters",
       fun encode_fed_info_response_record/0},
      {"encode RESPONSE_FED_INFO empty clusters",
       fun encode_fed_info_response_empty_clusters/0},
      {"encode RESPONSE_FED_INFO non-record",
       fun encode_fed_info_response_non_record/0},

      %% Encode REQUEST_FEDERATION_SUBMIT tests
      {"encode REQUEST_FEDERATION_SUBMIT record",
       fun encode_federation_submit_request_record/0},
      {"encode REQUEST_FEDERATION_SUBMIT non-record",
       fun encode_federation_submit_request_non_record/0},

      %% Encode RESPONSE_FEDERATION_SUBMIT tests
      {"encode RESPONSE_FEDERATION_SUBMIT record",
       fun encode_federation_submit_response_record/0},
      {"encode RESPONSE_FEDERATION_SUBMIT non-record",
       fun encode_federation_submit_response_non_record/0},

      %% Encode REQUEST_FEDERATION_JOB_STATUS tests
      {"encode REQUEST_FEDERATION_JOB_STATUS record",
       fun encode_federation_job_status_request_record/0},
      {"encode REQUEST_FEDERATION_JOB_STATUS non-record",
       fun encode_federation_job_status_request_non_record/0},

      %% Encode RESPONSE_FEDERATION_JOB_STATUS tests
      {"encode RESPONSE_FEDERATION_JOB_STATUS record",
       fun encode_federation_job_status_response_record/0},
      {"encode RESPONSE_FEDERATION_JOB_STATUS non-record",
       fun encode_federation_job_status_response_non_record/0},

      %% Encode REQUEST_FEDERATION_JOB_CANCEL tests
      {"encode REQUEST_FEDERATION_JOB_CANCEL record",
       fun encode_federation_job_cancel_request_record/0},
      {"encode REQUEST_FEDERATION_JOB_CANCEL non-record",
       fun encode_federation_job_cancel_request_non_record/0},

      %% Encode RESPONSE_FEDERATION_JOB_CANCEL tests
      {"encode RESPONSE_FEDERATION_JOB_CANCEL record",
       fun encode_federation_job_cancel_response_record/0},
      {"encode RESPONSE_FEDERATION_JOB_CANCEL non-record",
       fun encode_federation_job_cancel_response_non_record/0},

      %% Encode REQUEST_UPDATE_FEDERATION tests
      {"encode REQUEST_UPDATE_FEDERATION add_cluster",
       fun encode_update_federation_add_cluster/0},
      {"encode REQUEST_UPDATE_FEDERATION remove_cluster",
       fun encode_update_federation_remove_cluster/0},
      {"encode REQUEST_UPDATE_FEDERATION unknown action",
       fun encode_update_federation_unknown/0},
      {"encode REQUEST_UPDATE_FEDERATION non-record",
       fun encode_update_federation_non_record/0},

      %% Encode RESPONSE_UPDATE_FEDERATION tests
      {"encode RESPONSE_UPDATE_FEDERATION record",
       fun encode_update_federation_response_record/0},
      {"encode RESPONSE_UPDATE_FEDERATION non-record",
       fun encode_update_federation_response_non_record/0},

      %% Encode MSG_FED_JOB_SUBMIT tests
      {"encode MSG_FED_JOB_SUBMIT record",
       fun encode_fed_job_submit_record/0},
      {"encode MSG_FED_JOB_SUBMIT non-record",
       fun encode_fed_job_submit_non_record/0},

      %% Encode MSG_FED_JOB_STARTED tests
      {"encode MSG_FED_JOB_STARTED record",
       fun encode_fed_job_started_record/0},
      {"encode MSG_FED_JOB_STARTED non-record",
       fun encode_fed_job_started_non_record/0},

      %% Encode MSG_FED_SIBLING_REVOKE tests
      {"encode MSG_FED_SIBLING_REVOKE record",
       fun encode_fed_sibling_revoke_record/0},
      {"encode MSG_FED_SIBLING_REVOKE non-record",
       fun encode_fed_sibling_revoke_non_record/0},

      %% Encode MSG_FED_JOB_COMPLETED tests
      {"encode MSG_FED_JOB_COMPLETED record",
       fun encode_fed_job_completed_record/0},
      {"encode MSG_FED_JOB_COMPLETED non-record",
       fun encode_fed_job_completed_non_record/0},

      %% Encode MSG_FED_JOB_FAILED tests
      {"encode MSG_FED_JOB_FAILED record",
       fun encode_fed_job_failed_record/0},
      {"encode MSG_FED_JOB_FAILED non-record",
       fun encode_fed_job_failed_non_record/0},

      %% Encode unsupported tests
      {"encode unsupported message type",
       fun encode_unsupported/0},

      %% Roundtrip tests
      {"roundtrip fed info request",
       fun roundtrip_fed_info_request/0},
      {"roundtrip federation submit request",
       fun roundtrip_federation_submit/0},

      %% Edge cases
      {"encode federation with multiple clusters",
       fun encode_multiple_clusters/0},
      {"encode federation with features and partitions",
       fun encode_with_features_partitions/0},
      {"encode with unicode strings",
       fun encode_unicode_strings/0},
      {"encode with large environment",
       fun encode_large_environment/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Helper to pack a string like flurm_protocol_pack
%%%===================================================================

pack_string(undefined) -> <<16#FFFFFFFF:32/big>>;
pack_string(<<>>) -> <<0:32/big>>;
pack_string(Str) when is_binary(Str) ->
    Len = byte_size(Str),
    <<Len:32/big, Str/binary>>;
pack_string(Str) when is_list(Str) ->
    pack_string(list_to_binary(Str)).

%%%===================================================================
%%% Decode REQUEST_FED_INFO Tests
%%%===================================================================

decode_fed_info_request_empty() ->
    Result = flurm_codec_federation:decode_body(?REQUEST_FED_INFO, <<>>),
    ?assertMatch({ok, #fed_info_request{}}, Result).

decode_fed_info_request_with_flags() ->
    Binary = <<255:32/big, "extra">>,
    Result = flurm_codec_federation:decode_body(?REQUEST_FED_INFO, Binary),
    ?assertMatch({ok, #fed_info_request{show_flags = 255}}, Result).

decode_fed_info_request_other() ->
    Binary = <<1, 2, 3>>,  % Invalid but handled
    Result = flurm_codec_federation:decode_body(?REQUEST_FED_INFO, Binary),
    ?assertMatch({ok, #fed_info_request{}}, Result).

%%%===================================================================
%%% Decode RESPONSE_FED_INFO Tests
%%%===================================================================

decode_fed_info_response_full() ->
    FedName = <<"test_federation">>,
    LocalCluster = <<"cluster1">>,
    ClusterName = <<"cluster2">>,
    Host = <<"host2.example.com">>,
    State = <<"up">>,
    Binary = <<
        (pack_string(FedName))/binary,
        (pack_string(LocalCluster))/binary,
        1:32/big,  % cluster_count
        (pack_string(ClusterName))/binary,
        (pack_string(Host))/binary,
        6817:32/big,  % port
        (pack_string(State))/binary,
        1:32/big,  % weight
        0:32/big,  % feature_count
        0:32/big   % partition_count
    >>,
    Result = flurm_codec_federation:decode_body(?RESPONSE_FED_INFO, Binary),
    ?assertMatch({ok, #fed_info_response{federation_name = <<"test_federation">>}}, Result).

decode_fed_info_response_empty() ->
    Result = flurm_codec_federation:decode_body(?RESPONSE_FED_INFO, <<>>),
    ?assertMatch({ok, #fed_info_response{}}, Result).

%%%===================================================================
%%% Decode REQUEST_FEDERATION_SUBMIT Tests
%%%===================================================================

decode_federation_submit_full() ->
    Binary = <<
        (pack_string(<<"source_cluster">>))/binary,
        (pack_string(<<"target_cluster">>))/binary,
        123:32/big,  % job_id
        (pack_string(<<"test_job">>))/binary,
        (pack_string(<<"#!/bin/bash\necho hello">>))/binary,
        (pack_string(<<"default">>))/binary,
        4:32/big, 1:32/big, 2048:32/big,  % num_cpus, num_nodes, memory_mb
        3600:32/big, 1000:32/big, 1000:32/big, 100:32/big,  % time_limit, user_id, group_id, priority
        (pack_string(<<"/home/user">>))/binary,
        (pack_string(<<"/dev/null">>))/binary,
        (pack_string(<<"/dev/null">>))/binary,
        1:32/big,  % env_count
        (pack_string(<<"PATH=/usr/bin">>))/binary,
        (pack_string(<<"gpu">>))/binary  % features
    >>,
    Result = flurm_codec_federation:decode_body(?REQUEST_FEDERATION_SUBMIT, Binary),
    ?assertMatch({ok, #federation_submit_request{source_cluster = <<"source_cluster">>}}, Result).

decode_federation_submit_minimal() ->
    Result = flurm_codec_federation:decode_body(?REQUEST_FEDERATION_SUBMIT, <<>>),
    ?assertMatch({ok, #federation_submit_request{}}, Result).

%%%===================================================================
%%% Decode RESPONSE_FEDERATION_SUBMIT Tests
%%%===================================================================

decode_federation_submit_response_full() ->
    Binary = <<
        (pack_string(<<"source_cluster">>))/binary,
        456:32/big, 0:32/big,  % job_id, error_code
        (pack_string(<<"Job submitted">>))/binary
    >>,
    Result = flurm_codec_federation:decode_body(?RESPONSE_FEDERATION_SUBMIT, Binary),
    ?assertMatch({ok, #federation_submit_response{job_id = 456}}, Result).

decode_federation_submit_response_empty() ->
    Result = flurm_codec_federation:decode_body(?RESPONSE_FEDERATION_SUBMIT, <<>>),
    ?assertMatch({ok, #federation_submit_response{}}, Result).

%%%===================================================================
%%% Decode REQUEST_FEDERATION_JOB_STATUS Tests
%%%===================================================================

decode_federation_job_status_full() ->
    Binary = <<
        (pack_string(<<"cluster1">>))/binary,
        789:32/big,
        (pack_string(<<"789">>))/binary
    >>,
    Result = flurm_codec_federation:decode_body(?REQUEST_FEDERATION_JOB_STATUS, Binary),
    ?assertMatch({ok, #federation_job_status_request{job_id = 789}}, Result).

decode_federation_job_status_empty() ->
    Result = flurm_codec_federation:decode_body(?REQUEST_FEDERATION_JOB_STATUS, <<>>),
    ?assertMatch({ok, #federation_job_status_request{}}, Result).

%%%===================================================================
%%% Decode RESPONSE_FEDERATION_JOB_STATUS Tests
%%%===================================================================

decode_federation_job_status_response_full() ->
    Binary = <<
        123:32/big,  % job_id
        ?JOB_RUNNING:32/big,  % job_state
        0:32/big,  % state_reason
        0:32/big,  % exit_code
        1234567890:64/big, 0:64/big,  % start_time, end_time
        (pack_string(<<"node[01-04]">>))/binary
    >>,
    Result = flurm_codec_federation:decode_body(?RESPONSE_FEDERATION_JOB_STATUS, Binary),
    ?assertMatch({ok, #federation_job_status_response{job_id = 123}}, Result).

decode_federation_job_status_response_empty() ->
    Result = flurm_codec_federation:decode_body(?RESPONSE_FEDERATION_JOB_STATUS, <<>>),
    ?assertMatch({ok, #federation_job_status_response{}}, Result).

%%%===================================================================
%%% Decode REQUEST_FEDERATION_JOB_CANCEL Tests
%%%===================================================================

decode_federation_job_cancel_full() ->
    Binary = <<
        (pack_string(<<"cluster1">>))/binary,
        456:32/big,
        (pack_string(<<"456">>))/binary,
        9:32/big  % signal
    >>,
    Result = flurm_codec_federation:decode_body(?REQUEST_FEDERATION_JOB_CANCEL, Binary),
    ?assertMatch({ok, #federation_job_cancel_request{job_id = 456, signal = 9}}, Result).

decode_federation_job_cancel_empty() ->
    Result = flurm_codec_federation:decode_body(?REQUEST_FEDERATION_JOB_CANCEL, <<>>),
    ?assertMatch({ok, #federation_job_cancel_request{}}, Result).

%%%===================================================================
%%% Decode RESPONSE_FEDERATION_JOB_CANCEL Tests
%%%===================================================================

decode_federation_job_cancel_response_full() ->
    Binary = <<
        789:32/big, 0:32/big,  % job_id, error_code
        (pack_string(<<>>))/binary
    >>,
    Result = flurm_codec_federation:decode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Binary),
    ?assertMatch({ok, #federation_job_cancel_response{job_id = 789}}, Result).

decode_federation_job_cancel_response_empty() ->
    Result = flurm_codec_federation:decode_body(?RESPONSE_FEDERATION_JOB_CANCEL, <<>>),
    ?assertMatch({ok, #federation_job_cancel_response{}}, Result).

%%%===================================================================
%%% Decode REQUEST_UPDATE_FEDERATION Tests
%%%===================================================================

decode_update_federation_add_cluster() ->
    Binary = <<
        1:32/big,  % action = add_cluster
        (pack_string(<<"new_cluster">>))/binary,
        (pack_string(<<"newhost.example.com">>))/binary,
        6817:32/big, 0:32/big  % port, settings_count
    >>,
    Result = flurm_codec_federation:decode_body(?REQUEST_UPDATE_FEDERATION, Binary),
    ?assertMatch({ok, #update_federation_request{action = add_cluster}}, Result).

decode_update_federation_remove_cluster() ->
    Binary = <<
        2:32/big,  % action = remove_cluster
        (pack_string(<<"old_cluster">>))/binary,
        (pack_string(<<>>))/binary,
        0:32/big, 0:32/big
    >>,
    Result = flurm_codec_federation:decode_body(?REQUEST_UPDATE_FEDERATION, Binary),
    ?assertMatch({ok, #update_federation_request{action = remove_cluster}}, Result).

decode_update_federation_update_settings() ->
    Binary = <<
        3:32/big,  % action = update_settings
        (pack_string(<<>>))/binary,
        (pack_string(<<>>))/binary,
        0:32/big, 0:32/big
    >>,
    Result = flurm_codec_federation:decode_body(?REQUEST_UPDATE_FEDERATION, Binary),
    ?assertMatch({ok, #update_federation_request{action = update_settings}}, Result).

decode_update_federation_unknown() ->
    Binary = <<
        99:32/big,  % action = unknown
        (pack_string(<<>>))/binary,
        (pack_string(<<>>))/binary,
        0:32/big, 0:32/big
    >>,
    Result = flurm_codec_federation:decode_body(?REQUEST_UPDATE_FEDERATION, Binary),
    ?assertMatch({ok, #update_federation_request{action = unknown}}, Result).

decode_update_federation_empty() ->
    Result = flurm_codec_federation:decode_body(?REQUEST_UPDATE_FEDERATION, <<>>),
    ?assertMatch({ok, #update_federation_request{action = unknown}}, Result).

%%%===================================================================
%%% Decode RESPONSE_UPDATE_FEDERATION Tests
%%%===================================================================

decode_update_federation_response_full() ->
    Binary = <<
        0:32/big,  % error_code
        (pack_string(<<"Federation updated">>))/binary
    >>,
    Result = flurm_codec_federation:decode_body(?RESPONSE_UPDATE_FEDERATION, Binary),
    ?assertMatch({ok, #update_federation_response{error_code = 0}}, Result).

decode_update_federation_response_empty() ->
    Result = flurm_codec_federation:decode_body(?RESPONSE_UPDATE_FEDERATION, <<>>),
    ?assertMatch({ok, #update_federation_response{}}, Result).

%%%===================================================================
%%% Decode MSG_FED_JOB_SUBMIT Tests
%%%===================================================================

decode_fed_job_submit_full() ->
    JobSpec = #{name => <<"test">>},
    JobSpecBin = term_to_binary(JobSpec),
    JobSpecLen = byte_size(JobSpecBin),
    Binary = <<
        (pack_string(<<"fed_job_123">>))/binary,
        (pack_string(<<"origin">>))/binary,
        (pack_string(<<"target">>))/binary,
        1234567890:64/big,  % submit_time
        JobSpecLen:32/big, JobSpecBin/binary
    >>,
    Result = flurm_codec_federation:decode_body(?MSG_FED_JOB_SUBMIT, Binary),
    ?assertMatch({ok, #fed_job_submit_msg{federation_job_id = <<"fed_job_123">>}}, Result).

decode_fed_job_submit_empty() ->
    Result = flurm_codec_federation:decode_body(?MSG_FED_JOB_SUBMIT, <<>>),
    ?assertMatch({ok, #fed_job_submit_msg{}}, Result).

%%%===================================================================
%%% Decode MSG_FED_JOB_STARTED Tests
%%%===================================================================

decode_fed_job_started_full() ->
    Binary = <<
        (pack_string(<<"fed_job_456">>))/binary,
        (pack_string(<<"cluster2">>))/binary,
        789:32/big,  % local_job_id
        1234567890:64/big  % start_time
    >>,
    Result = flurm_codec_federation:decode_body(?MSG_FED_JOB_STARTED, Binary),
    ?assertMatch({ok, #fed_job_started_msg{federation_job_id = <<"fed_job_456">>}}, Result).

decode_fed_job_started_empty() ->
    Result = flurm_codec_federation:decode_body(?MSG_FED_JOB_STARTED, <<>>),
    ?assertMatch({ok, #fed_job_started_msg{}}, Result).

%%%===================================================================
%%% Decode MSG_FED_SIBLING_REVOKE Tests
%%%===================================================================

decode_fed_sibling_revoke_full() ->
    Binary = <<
        (pack_string(<<"fed_job_789">>))/binary,
        (pack_string(<<"cluster1">>))/binary,
        (pack_string(<<"sibling_started">>))/binary
    >>,
    Result = flurm_codec_federation:decode_body(?MSG_FED_SIBLING_REVOKE, Binary),
    ?assertMatch({ok, #fed_sibling_revoke_msg{federation_job_id = <<"fed_job_789">>}}, Result).

decode_fed_sibling_revoke_empty() ->
    Result = flurm_codec_federation:decode_body(?MSG_FED_SIBLING_REVOKE, <<>>),
    ?assertMatch({ok, #fed_sibling_revoke_msg{}}, Result).

%%%===================================================================
%%% Decode MSG_FED_JOB_COMPLETED Tests
%%%===================================================================

decode_fed_job_completed_full() ->
    Binary = <<
        (pack_string(<<"fed_job_100">>))/binary,
        (pack_string(<<"cluster3">>))/binary,
        200:32/big,  % local_job_id
        1234567900:64/big,  % end_time
        0:32/signed-big  % exit_code
    >>,
    Result = flurm_codec_federation:decode_body(?MSG_FED_JOB_COMPLETED, Binary),
    ?assertMatch({ok, #fed_job_completed_msg{federation_job_id = <<"fed_job_100">>}}, Result).

decode_fed_job_completed_empty() ->
    Result = flurm_codec_federation:decode_body(?MSG_FED_JOB_COMPLETED, <<>>),
    ?assertMatch({ok, #fed_job_completed_msg{}}, Result).

%%%===================================================================
%%% Decode MSG_FED_JOB_FAILED Tests
%%%===================================================================

decode_fed_job_failed_full() ->
    Binary = <<
        (pack_string(<<"fed_job_500">>))/binary,
        (pack_string(<<"cluster4">>))/binary,
        600:32/big,  % local_job_id
        1234567950:64/big,  % end_time
        1:32/signed-big,  % exit_code
        (pack_string(<<"Out of memory">>))/binary
    >>,
    Result = flurm_codec_federation:decode_body(?MSG_FED_JOB_FAILED, Binary),
    ?assertMatch({ok, #fed_job_failed_msg{federation_job_id = <<"fed_job_500">>}}, Result).

decode_fed_job_failed_empty() ->
    Result = flurm_codec_federation:decode_body(?MSG_FED_JOB_FAILED, <<>>),
    ?assertMatch({ok, #fed_job_failed_msg{}}, Result).

%%%===================================================================
%%% Decode Unsupported Tests
%%%===================================================================

decode_unsupported() ->
    Result = flurm_codec_federation:decode_body(99999, <<1, 2, 3>>),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% Encode REQUEST_FED_INFO Tests
%%%===================================================================

encode_fed_info_request_record() ->
    Req = #fed_info_request{show_flags = 255},
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FED_INFO, Req),
    ?assertEqual(<<255:32/big>>, Binary).

encode_fed_info_request_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FED_INFO, not_a_record),
    ?assertEqual(<<0:32/big>>, Binary).

%%%===================================================================
%%% Encode RESPONSE_FED_INFO Tests
%%%===================================================================

encode_fed_info_response_record() ->
    Clusters = [
        #fed_cluster_info{
            name = <<"cluster1">>,
            host = <<"host1.example.com">>,
            port = 6817,
            state = <<"up">>,
            weight = 1,
            features = [<<"gpu">>, <<"fast">>],
            partitions = [<<"default">>, <<"batch">>]
        },
        #fed_cluster_info{
            name = <<"cluster2">>,
            host = <<"host2.example.com">>,
            port = 6817,
            state = <<"drain">>,
            weight = 2,
            features = [],
            partitions = [<<"default">>]
        }
    ],
    Resp = #fed_info_response{
        federation_name = <<"test_federation">>,
        local_cluster = <<"cluster1">>,
        cluster_count = 2,
        clusters = Clusters
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FED_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 50).

encode_fed_info_response_empty_clusters() ->
    Resp = #fed_info_response{
        federation_name = <<"empty_fed">>,
        local_cluster = <<"local">>,
        cluster_count = 0,
        clusters = []
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FED_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_fed_info_response_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FED_INFO, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode REQUEST_FEDERATION_SUBMIT Tests
%%%===================================================================

encode_federation_submit_request_record() ->
    Req = #federation_submit_request{
        source_cluster = <<"cluster1">>,
        target_cluster = <<"cluster2">>,
        job_id = 123,
        name = <<"test_job">>,
        script = <<"#!/bin/bash\necho hello">>,
        partition = <<"default">>,
        num_cpus = 4,
        num_nodes = 1,
        memory_mb = 2048,
        time_limit = 3600,
        user_id = 1000,
        group_id = 1000,
        priority = 100,
        work_dir = <<"/home/user">>,
        std_out = <<"/dev/null">>,
        std_err = <<"/dev/null">>,
        environment = [<<"PATH=/usr/bin">>, <<"HOME=/home/user">>],
        features = <<"gpu">>
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 100).

encode_federation_submit_request_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FEDERATION_SUBMIT, not_a_record),
    ?assertEqual(<<>>, Binary).

%%%===================================================================
%%% Encode RESPONSE_FEDERATION_SUBMIT Tests
%%%===================================================================

encode_federation_submit_response_record() ->
    Resp = #federation_submit_response{
        source_cluster = <<"cluster1">>,
        job_id = 456,
        error_code = 0,
        error_msg = <<"Job submitted successfully">>
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp),
    ?assert(is_binary(Binary)).

encode_federation_submit_response_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FEDERATION_SUBMIT, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode REQUEST_FEDERATION_JOB_STATUS Tests
%%%===================================================================

encode_federation_job_status_request_record() ->
    Req = #federation_job_status_request{
        source_cluster = <<"cluster1">>,
        job_id = 789,
        job_id_str = <<"789">>
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req),
    ?assert(is_binary(Binary)).

encode_federation_job_status_request_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FEDERATION_JOB_STATUS, not_a_record),
    ?assertEqual(<<>>, Binary).

%%%===================================================================
%%% Encode RESPONSE_FEDERATION_JOB_STATUS Tests
%%%===================================================================

encode_federation_job_status_response_record() ->
    Resp = #federation_job_status_response{
        job_id = 123,
        job_state = ?JOB_RUNNING,
        state_reason = 0,
        exit_code = 0,
        start_time = 1234567890,
        end_time = 0,
        nodes = <<"node[01-04]">>
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
    ?assert(is_binary(Binary)).

encode_federation_job_status_response_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode REQUEST_FEDERATION_JOB_CANCEL Tests
%%%===================================================================

encode_federation_job_cancel_request_record() ->
    Req = #federation_job_cancel_request{
        source_cluster = <<"cluster1">>,
        job_id = 456,
        job_id_str = <<"456">>,
        signal = 9
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req),
    ?assert(is_binary(Binary)).

encode_federation_job_cancel_request_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FEDERATION_JOB_CANCEL, not_a_record),
    ?assertEqual(<<>>, Binary).

%%%===================================================================
%%% Encode RESPONSE_FEDERATION_JOB_CANCEL Tests
%%%===================================================================

encode_federation_job_cancel_response_record() ->
    Resp = #federation_job_cancel_response{
        job_id = 789,
        error_code = 0,
        error_msg = <<>>
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp),
    ?assert(is_binary(Binary)).

encode_federation_job_cancel_response_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode REQUEST_UPDATE_FEDERATION Tests
%%%===================================================================

encode_update_federation_add_cluster() ->
    Req = #update_federation_request{
        action = add_cluster,
        cluster_name = <<"new_cluster">>,
        host = <<"newhost.example.com">>,
        port = 6817,
        settings = #{weight => 1}
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
    ?assert(is_binary(Binary)),
    <<ActionCode:32/big, _/binary>> = Binary,
    ?assertEqual(1, ActionCode).

encode_update_federation_remove_cluster() ->
    Req = #update_federation_request{
        action = remove_cluster,
        cluster_name = <<"old_cluster">>,
        host = <<>>,
        port = 0,
        settings = #{}
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
    ?assert(is_binary(Binary)),
    <<ActionCode:32/big, _/binary>> = Binary,
    ?assertEqual(2, ActionCode).

encode_update_federation_unknown() ->
    Req = #update_federation_request{
        action = unknown_action,
        cluster_name = <<>>,
        host = <<>>,
        port = 0,
        settings = #{}
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
    ?assert(is_binary(Binary)),
    <<ActionCode:32/big, _/binary>> = Binary,
    ?assertEqual(0, ActionCode).

encode_update_federation_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_UPDATE_FEDERATION, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_UPDATE_FEDERATION Tests
%%%===================================================================

encode_update_federation_response_record() ->
    Resp = #update_federation_response{
        error_code = 0,
        error_msg = <<"Success">>
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_UPDATE_FEDERATION, Resp),
    ?assert(is_binary(Binary)).

encode_update_federation_response_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_UPDATE_FEDERATION, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode MSG_FED_JOB_SUBMIT Tests
%%%===================================================================

encode_fed_job_submit_record() ->
    Msg = #fed_job_submit_msg{
        federation_job_id = <<"fed_123">>,
        origin_cluster = <<"cluster1">>,
        target_cluster = <<"cluster2">>,
        job_spec = #{name => <<"test">>, cpus => 4},
        submit_time = 1234567890
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_JOB_SUBMIT, Msg),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 20).

encode_fed_job_submit_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_JOB_SUBMIT, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode MSG_FED_JOB_STARTED Tests
%%%===================================================================

encode_fed_job_started_record() ->
    Msg = #fed_job_started_msg{
        federation_job_id = <<"fed_456">>,
        running_cluster = <<"cluster2">>,
        local_job_id = 789,
        start_time = 1234567890
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_JOB_STARTED, Msg),
    ?assert(is_binary(Binary)).

encode_fed_job_started_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_JOB_STARTED, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode MSG_FED_SIBLING_REVOKE Tests
%%%===================================================================

encode_fed_sibling_revoke_record() ->
    Msg = #fed_sibling_revoke_msg{
        federation_job_id = <<"fed_789">>,
        running_cluster = <<"cluster1">>,
        revoke_reason = <<"sibling_started">>
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_SIBLING_REVOKE, Msg),
    ?assert(is_binary(Binary)).

encode_fed_sibling_revoke_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_SIBLING_REVOKE, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode MSG_FED_JOB_COMPLETED Tests
%%%===================================================================

encode_fed_job_completed_record() ->
    Msg = #fed_job_completed_msg{
        federation_job_id = <<"fed_100">>,
        running_cluster = <<"cluster3">>,
        local_job_id = 200,
        end_time = 1234567900,
        exit_code = 0
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
    ?assert(is_binary(Binary)).

encode_fed_job_completed_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_JOB_COMPLETED, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode MSG_FED_JOB_FAILED Tests
%%%===================================================================

encode_fed_job_failed_record() ->
    Msg = #fed_job_failed_msg{
        federation_job_id = <<"fed_500">>,
        running_cluster = <<"cluster4">>,
        local_job_id = 600,
        end_time = 1234567950,
        exit_code = 1,
        error_msg = <<"Out of memory">>
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_JOB_FAILED, Msg),
    ?assert(is_binary(Binary)).

encode_fed_job_failed_non_record() ->
    {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_JOB_FAILED, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode Unsupported Tests
%%%===================================================================

encode_unsupported() ->
    Result = flurm_codec_federation:encode_body(99999, some_body),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_fed_info_request() ->
    Req = #fed_info_request{show_flags = 123},
    {ok, Encoded} = flurm_codec_federation:encode_body(?REQUEST_FED_INFO, Req),
    {ok, Decoded} = flurm_codec_federation:decode_body(?REQUEST_FED_INFO, Encoded),
    ?assertEqual(123, Decoded#fed_info_request.show_flags).

roundtrip_federation_submit() ->
    Req = #federation_submit_request{
        source_cluster = <<"src">>,
        target_cluster = <<"tgt">>,
        job_id = 999,
        name = <<"roundtrip_test">>,
        script = <<"#!/bin/bash">>,
        partition = <<"default">>,
        num_cpus = 2,
        num_nodes = 1,
        memory_mb = 1024,
        time_limit = 1800,
        user_id = 500,
        group_id = 500,
        priority = 50,
        work_dir = <<"/tmp">>,
        std_out = <<>>,
        std_err = <<>>,
        environment = [<<"VAR=value">>],
        features = <<>>
    },
    {ok, Encoded} = flurm_codec_federation:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
    {ok, Decoded} = flurm_codec_federation:decode_body(?REQUEST_FEDERATION_SUBMIT, Encoded),
    ?assertEqual(<<"src">>, Decoded#federation_submit_request.source_cluster).

%%%===================================================================
%%% Edge Cases
%%%===================================================================

encode_multiple_clusters() ->
    Clusters = [
        #fed_cluster_info{name = <<"c1">>, host = <<"h1">>, port = 6817, state = <<"up">>, weight = 1, features = [], partitions = []},
        #fed_cluster_info{name = <<"c2">>, host = <<"h2">>, port = 6817, state = <<"up">>, weight = 2, features = [], partitions = []},
        #fed_cluster_info{name = <<"c3">>, host = <<"h3">>, port = 6817, state = <<"drain">>, weight = 3, features = [], partitions = []},
        #fed_cluster_info{name = <<"c4">>, host = <<"h4">>, port = 6817, state = <<"down">>, weight = 4, features = [], partitions = []},
        #fed_cluster_info{name = <<"c5">>, host = <<"h5">>, port = 6817, state = <<"up">>, weight = 5, features = [], partitions = []}
    ],
    Resp = #fed_info_response{
        federation_name = <<"multi">>,
        local_cluster = <<"c1">>,
        cluster_count = 5,
        clusters = Clusters
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FED_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 100).

encode_with_features_partitions() ->
    Cluster = #fed_cluster_info{
        name = <<"cluster1">>,
        host = <<"host1">>,
        port = 6817,
        state = <<"up">>,
        weight = 1,
        features = [<<"gpu">>, <<"nvlink">>, <<"fast_storage">>, <<"infiniband">>],
        partitions = [<<"default">>, <<"gpu">>, <<"interactive">>, <<"batch">>, <<"debug">>]
    },
    Resp = #fed_info_response{
        federation_name = <<"featured">>,
        local_cluster = <<"cluster1">>,
        cluster_count = 1,
        clusters = [Cluster]
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FED_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_unicode_strings() ->
    Req = #federation_submit_request{
        source_cluster = <<"cluster", 195, 169>>,  % e-acute
        target_cluster = <<"target">>,
        job_id = 0,
        name = <<"job_", 226, 128, 147, "test">>,  % en-dash
        script = <<"#!/bin/bash\n# Comment with ", 195, 169, "\necho test">>,
        partition = <<"default">>,
        num_cpus = 1,
        num_nodes = 1,
        memory_mb = 1024,
        time_limit = 60,
        user_id = 1000,
        group_id = 1000,
        priority = 0,
        work_dir = <<"/home/", 195, 169, "user">>,
        std_out = <<>>,
        std_err = <<>>,
        environment = [],
        features = <<>>
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
    ?assert(is_binary(Binary)).

encode_large_environment() ->
    Env = [list_to_binary("ENV_VAR_" ++ integer_to_list(N) ++ "=value_" ++ integer_to_list(N))
           || N <- lists:seq(1, 100)],
    Req = #federation_submit_request{
        source_cluster = <<"src">>,
        target_cluster = <<"tgt">>,
        job_id = 0,
        name = <<"env_test">>,
        script = <<"#!/bin/bash">>,
        partition = <<"default">>,
        num_cpus = 1,
        num_nodes = 1,
        memory_mb = 1024,
        time_limit = 60,
        user_id = 1000,
        group_id = 1000,
        priority = 0,
        work_dir = <<"/tmp">>,
        std_out = <<>>,
        std_err = <<>>,
        environment = Env,
        features = <<>>
    },
    {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 1000).

%%%===================================================================
%%% Additional Edge Cases
%%%===================================================================

%% Test with settings in update request
update_with_settings_test_() ->
    {"encode update federation with settings",
     fun() ->
         Req = #update_federation_request{
             action = update_settings,
             cluster_name = <<"cluster1">>,
             host = <<>>,
             port = 0,
             settings = #{
                 weight => 10,
                 drain => true,
                 features => <<"gpu,fast">>
             }
         },
         {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_UPDATE_FEDERATION, Req),
         ?assert(is_binary(Binary))
     end}.

%% Test negative exit codes
negative_exit_code_test_() ->
    {"encode job completed with negative exit code",
     fun() ->
         Msg = #fed_job_completed_msg{
             federation_job_id = <<"job1">>,
             running_cluster = <<"cluster1">>,
             local_job_id = 100,
             end_time = 1234567890,
             exit_code = -15  % SIGTERM
         },
         {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_JOB_COMPLETED, Msg),
         ?assert(is_binary(Binary))
     end}.

%% Test job failed with long error message
long_error_msg_test_() ->
    {"encode job failed with long error message",
     fun() ->
         LongMsg = iolist_to_binary([<<"Error: ">> | lists:duplicate(100, <<"detail ">>)]),
         Msg = #fed_job_failed_msg{
             federation_job_id = <<"job2">>,
             running_cluster = <<"cluster2">>,
             local_job_id = 200,
             end_time = 1234567900,
             exit_code = 137,  % SIGKILL
             error_msg = LongMsg
         },
         {ok, Binary} = flurm_codec_federation:encode_body(?MSG_FED_JOB_FAILED, Msg),
         ?assert(is_binary(Binary)),
         ?assert(byte_size(Binary) > 500)
     end}.

%% Test empty strings everywhere
empty_strings_test_() ->
    {"encode federation submit with all empty strings",
     fun() ->
         Req = #federation_submit_request{
             source_cluster = <<>>,
             target_cluster = <<>>,
             job_id = 0,
             name = <<>>,
             script = <<>>,
             partition = <<>>,
             num_cpus = 0,
             num_nodes = 0,
             memory_mb = 0,
             time_limit = 0,
             user_id = 0,
             group_id = 0,
             priority = 0,
             work_dir = <<>>,
             std_out = <<>>,
             std_err = <<>>,
             environment = [],
             features = <<>>
         },
         {ok, Binary} = flurm_codec_federation:encode_body(?REQUEST_FEDERATION_SUBMIT, Req),
         ?assert(is_binary(Binary))
     end}.

%% Test max integer values
max_values_test_() ->
    {"encode federation status with max values",
     fun() ->
         Resp = #federation_job_status_response{
             job_id = 16#FFFFFFFF,
             job_state = 16#FFFFFFFF,
             state_reason = 16#FFFFFFFF,
             exit_code = 16#FFFFFFFF,
             start_time = 16#FFFFFFFFFFFFFFFF,
             end_time = 16#FFFFFFFFFFFFFFFF,
             nodes = <<"node[00000001-99999999]">>
         },
         {ok, Binary} = flurm_codec_federation:encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp),
         ?assert(is_binary(Binary))
     end}.
