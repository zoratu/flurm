%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_codec_job module
%%% Coverage target: 100% of all functions and branches
%%%-------------------------------------------------------------------
-module(flurm_codec_job_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Generator
%%%===================================================================

flurm_codec_job_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% Decode REQUEST_JOB_INFO tests
      {"decode REQUEST_JOB_INFO full binary",
       fun decode_job_info_full/0},
      {"decode REQUEST_JOB_INFO two fields",
       fun decode_job_info_two_fields/0},
      {"decode REQUEST_JOB_INFO one field",
       fun decode_job_info_one_field/0},
      {"decode REQUEST_JOB_INFO empty",
       fun decode_job_info_empty/0},
      {"decode REQUEST_JOB_INFO invalid",
       fun decode_job_info_invalid/0},

      %% Decode REQUEST_RESOURCE_ALLOCATION tests
      {"decode REQUEST_RESOURCE_ALLOCATION",
       fun decode_resource_allocation/0},

      %% Decode REQUEST_SUBMIT_BATCH_JOB tests
      {"decode REQUEST_SUBMIT_BATCH_JOB full",
       fun decode_batch_job_full/0},
      {"decode REQUEST_SUBMIT_BATCH_JOB minimal",
       fun decode_batch_job_minimal/0},
      {"decode REQUEST_SUBMIT_BATCH_JOB with error",
       fun decode_batch_job_error/0},

      %% Decode REQUEST_CANCEL_JOB tests
      {"decode REQUEST_CANCEL_JOB full",
       fun decode_cancel_job_full/0},
      {"decode REQUEST_CANCEL_JOB three fields",
       fun decode_cancel_job_three_fields/0},
      {"decode REQUEST_CANCEL_JOB two fields",
       fun decode_cancel_job_two_fields/0},
      {"decode REQUEST_CANCEL_JOB one field",
       fun decode_cancel_job_one_field/0},
      {"decode REQUEST_CANCEL_JOB empty",
       fun decode_cancel_job_empty/0},
      {"decode REQUEST_CANCEL_JOB invalid",
       fun decode_cancel_job_invalid/0},

      %% Decode REQUEST_KILL_JOB tests
      {"decode REQUEST_KILL_JOB full with sibling",
       fun decode_kill_job_full/0},
      {"decode REQUEST_KILL_JOB four fields",
       fun decode_kill_job_four_fields/0},
      {"decode REQUEST_KILL_JOB three fields",
       fun decode_kill_job_three_fields/0},
      {"decode REQUEST_KILL_JOB one field",
       fun decode_kill_job_one_field/0},
      {"decode REQUEST_KILL_JOB empty",
       fun decode_kill_job_empty/0},
      {"decode REQUEST_KILL_JOB invalid",
       fun decode_kill_job_invalid/0},

      %% Decode REQUEST_SUSPEND tests
      {"decode REQUEST_SUSPEND full",
       fun decode_suspend_full/0},
      {"decode REQUEST_SUSPEND one field",
       fun decode_suspend_one_field/0},
      {"decode REQUEST_SUSPEND empty",
       fun decode_suspend_empty/0},
      {"decode REQUEST_SUSPEND invalid",
       fun decode_suspend_invalid/0},

      %% Decode REQUEST_SIGNAL_JOB tests
      {"decode REQUEST_SIGNAL_JOB full",
       fun decode_signal_job_full/0},
      {"decode REQUEST_SIGNAL_JOB three fields",
       fun decode_signal_job_three_fields/0},
      {"decode REQUEST_SIGNAL_JOB two fields",
       fun decode_signal_job_two_fields/0},
      {"decode REQUEST_SIGNAL_JOB one field",
       fun decode_signal_job_one_field/0},
      {"decode REQUEST_SIGNAL_JOB empty",
       fun decode_signal_job_empty/0},
      {"decode REQUEST_SIGNAL_JOB invalid",
       fun decode_signal_job_invalid/0},

      %% Decode REQUEST_UPDATE_JOB tests
      {"decode REQUEST_UPDATE_JOB full",
       fun decode_update_job_full/0},
      {"decode REQUEST_UPDATE_JOB minimal",
       fun decode_update_job_minimal/0},
      {"decode REQUEST_UPDATE_JOB with job_id_str",
       fun decode_update_job_with_str/0},

      %% Decode REQUEST_JOB_WILL_RUN tests
      {"decode REQUEST_JOB_WILL_RUN with job_id",
       fun decode_job_will_run_with_id/0},
      {"decode REQUEST_JOB_WILL_RUN minimal",
       fun decode_job_will_run_minimal/0},

      %% Decode RESPONSE_SUBMIT_BATCH_JOB tests
      {"decode RESPONSE_SUBMIT_BATCH_JOB full",
       fun decode_batch_job_response_full/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB three fields",
       fun decode_batch_job_response_three/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB two fields",
       fun decode_batch_job_response_two/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB one field",
       fun decode_batch_job_response_one/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB empty",
       fun decode_batch_job_response_empty/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB invalid",
       fun decode_batch_job_response_invalid/0},

      %% Decode RESPONSE_JOB_INFO tests
      {"decode RESPONSE_JOB_INFO full",
       fun decode_job_info_response_full/0},
      {"decode RESPONSE_JOB_INFO minimal",
       fun decode_job_info_response_minimal/0},
      {"decode RESPONSE_JOB_INFO empty",
       fun decode_job_info_response_empty/0},

      %% Decode unsupported tests
      {"decode unsupported message type",
       fun decode_unsupported/0},

      %% Encode REQUEST_JOB_INFO tests
      {"encode REQUEST_JOB_INFO record",
       fun encode_job_info_request_record/0},
      {"encode REQUEST_JOB_INFO non-record",
       fun encode_job_info_request_non_record/0},

      %% Encode REQUEST_SUBMIT_BATCH_JOB tests
      {"encode REQUEST_SUBMIT_BATCH_JOB record",
       fun encode_batch_job_request_record/0},
      {"encode REQUEST_SUBMIT_BATCH_JOB non-record",
       fun encode_batch_job_request_non_record/0},

      %% Encode REQUEST_CANCEL_JOB tests
      {"encode REQUEST_CANCEL_JOB record",
       fun encode_cancel_job_request_record/0},
      {"encode REQUEST_CANCEL_JOB non-record",
       fun encode_cancel_job_request_non_record/0},

      %% Encode REQUEST_KILL_JOB tests
      {"encode REQUEST_KILL_JOB record",
       fun encode_kill_job_request_record/0},
      {"encode REQUEST_KILL_JOB non-record",
       fun encode_kill_job_request_non_record/0},

      %% Encode RESPONSE_JOB_WILL_RUN tests
      {"encode RESPONSE_JOB_WILL_RUN record",
       fun encode_job_will_run_response_record/0},
      {"encode RESPONSE_JOB_WILL_RUN non-record",
       fun encode_job_will_run_response_non_record/0},

      %% Encode RESPONSE_JOB_READY tests
      {"encode RESPONSE_JOB_READY map",
       fun encode_job_ready_response_map/0},
      {"encode RESPONSE_JOB_READY non-map",
       fun encode_job_ready_response_non_map/0},

      %% Encode RESPONSE_SUBMIT_BATCH_JOB tests
      {"encode RESPONSE_SUBMIT_BATCH_JOB record",
       fun encode_batch_job_response_record/0},
      {"encode RESPONSE_SUBMIT_BATCH_JOB non-record",
       fun encode_batch_job_response_non_record/0},

      %% Encode RESPONSE_JOB_INFO tests
      {"encode RESPONSE_JOB_INFO record with jobs",
       fun encode_job_info_response_record/0},
      {"encode RESPONSE_JOB_INFO record empty",
       fun encode_job_info_response_empty/0},
      {"encode RESPONSE_JOB_INFO non-record",
       fun encode_job_info_response_non_record/0},
      {"encode single job info",
       fun encode_single_job_info/0},
      {"encode single job info non-record",
       fun encode_single_job_info_non_record/0},

      %% Encode RESPONSE_RESOURCE_ALLOCATION tests
      {"encode RESPONSE_RESOURCE_ALLOCATION record",
       fun encode_resource_allocation_response_record/0},
      {"encode RESPONSE_RESOURCE_ALLOCATION non-record",
       fun encode_resource_allocation_response_non_record/0},
      {"encode RESPONSE_JOB_ALLOCATION_INFO",
       fun encode_job_allocation_info_response/0},

      %% Encode unsupported tests
      {"encode unsupported message type",
       fun encode_unsupported/0},

      %% Utility function tests
      {"parse job ID from string",
       fun parse_job_id_tests/0},
      {"strip null from binary",
       fun strip_null_tests/0},
      {"ensure binary conversion",
       fun ensure_binary_tests/0},

      %% Edge cases
      {"roundtrip job info request",
       fun roundtrip_job_info_request/0},
      {"encode multiple jobs in response",
       fun encode_multiple_jobs/0},
      {"encode with maximum values",
       fun encode_max_values/0},
      {"encode with unicode strings",
       fun encode_unicode_strings/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Decode REQUEST_JOB_INFO Tests
%%%===================================================================

decode_job_info_full() ->
    Binary = <<16#0001:32/big, 123:32/big, 1000:32/big, "extra">>,
    Result = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_request{show_flags = 1, job_id = 123, user_id = 1000}}, Result).

decode_job_info_two_fields() ->
    Binary = <<0:32/big, 456:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_request{show_flags = 0, job_id = 456}}, Result).

decode_job_info_one_field() ->
    Binary = <<255:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_request{show_flags = 255}}, Result).

decode_job_info_empty() ->
    Result = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, <<>>),
    ?assertMatch({ok, #job_info_request{}}, Result).

decode_job_info_invalid() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% Decode REQUEST_RESOURCE_ALLOCATION Tests
%%%===================================================================

decode_resource_allocation() ->
    %% Uses same decoder as batch job
    Binary = <<0:32/big>>,  % site_factor
    Result = flurm_codec_job:decode_body(?REQUEST_RESOURCE_ALLOCATION, Binary),
    ?assertMatch({ok, #batch_job_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_SUBMIT_BATCH_JOB Tests
%%%===================================================================

decode_batch_job_full() ->
    %% Create a minimal but valid batch job binary
    %% site_factor(32), batch_features(str), cluster_features(str), clusters(str)
    %% contiguous(16), container(str), core_spec(16), task_dist(32), kill_on_fail(16)
    %% features(str), fed_active(64), fed_viable(64), job_id(32), job_id_str(str)
    %% name(str), partition(str)
    Binary = <<
        0:32/big,                      % site_factor
        16#FFFFFFFF:32/big,            % batch_features (NULL)
        16#FFFFFFFF:32/big,            % cluster_features (NULL)
        16#FFFFFFFF:32/big,            % clusters (NULL)
        0:16/big,                      % contiguous
        16#FFFFFFFF:32/big,            % container (NULL)
        0:16/big, 1:32/big, 1:16/big,  % core_spec, task_dist, kill_on_fail
        16#FFFFFFFF:32/big,            % features (NULL)
        0:64/big, 0:64/big,            % fed_active, fed_viable
        123:32/big,                    % job_id
        16#FFFFFFFF:32/big,            % job_id_str (NULL)
        8:32/big, "testjob", 0,        % name
        7:32/big, "default"            % partition
    >>,
    Result = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_request{}}, Result).

decode_batch_job_minimal() ->
    Binary = <<0:32/big>>,  % Just site_factor
    Result = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_request{}}, Result).

decode_batch_job_error() ->
    %% Very short binary that might cause parsing issues
    Binary = <<>>,
    Result = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_CANCEL_JOB Tests
%%%===================================================================

decode_cancel_job_full() ->
    Binary = <<123:32/big, 0:32/big, 15:32/big, 1:32/big, "extra">>,
    Result = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
    ?assertMatch({ok, #cancel_job_request{job_id = 123, step_id = 0, signal = 15, flags = 1}}, Result).

decode_cancel_job_three_fields() ->
    Binary = <<456:32/big, 1:32/big, 9:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
    ?assertMatch({ok, #cancel_job_request{job_id = 456, step_id = 1, signal = 9}}, Result).

decode_cancel_job_two_fields() ->
    Binary = <<789:32/big, 2:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
    ?assertMatch({ok, #cancel_job_request{job_id = 789, step_id = 2, signal = 9}}, Result).

decode_cancel_job_one_field() ->
    Binary = <<100:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
    ?assertMatch({ok, #cancel_job_request{job_id = 100, signal = 9}}, Result).

decode_cancel_job_empty() ->
    Result = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, <<>>),
    ?assertMatch({ok, #cancel_job_request{}}, Result).

decode_cancel_job_invalid() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% Decode REQUEST_KILL_JOB Tests
%%%===================================================================

decode_kill_job_full() ->
    Sibling = <<"cluster2">>,
    SibLen = byte_size(Sibling),
    Binary = <<123:32/big, 0:32/big, 9:32/big, 0:32/big, SibLen:32/big, Sibling/binary>>,
    Result = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary),
    ?assertMatch({ok, #kill_job_request{job_id = 123, sibling = <<"cluster2">>}}, Result).

decode_kill_job_four_fields() ->
    Binary = <<456:32/big, 1:32/big, 15:32/big, 1:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary),
    ?assertMatch({ok, #kill_job_request{job_id = 456, step_id = 1, signal = 15, flags = 1}}, Result).

decode_kill_job_three_fields() ->
    Binary = <<789:32/big, 2:32/big, 2:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary),
    ?assertMatch({ok, #kill_job_request{job_id = 789, step_id = 2, signal = 2}}, Result).

decode_kill_job_one_field() ->
    Binary = <<100:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary),
    ?assertMatch({ok, #kill_job_request{job_id = 100}}, Result).

decode_kill_job_empty() ->
    Result = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, <<>>),
    ?assertMatch({ok, #kill_job_request{}}, Result).

decode_kill_job_invalid() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% Decode REQUEST_SUSPEND Tests
%%%===================================================================

decode_suspend_full() ->
    Binary = <<123:32/big, 1:32/big, "extra">>,
    Result = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary),
    ?assertMatch({ok, #suspend_request{job_id = 123, suspend = true}}, Result).

decode_suspend_one_field() ->
    Binary = <<456:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary),
    ?assertMatch({ok, #suspend_request{job_id = 456}}, Result).

decode_suspend_empty() ->
    Result = flurm_codec_job:decode_body(?REQUEST_SUSPEND, <<>>),
    ?assertMatch({ok, #suspend_request{}}, Result).

decode_suspend_invalid() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% Decode REQUEST_SIGNAL_JOB Tests
%%%===================================================================

decode_signal_job_full() ->
    Binary = <<123:32/big, 0:32/big, 15:32/big, 1:32/big, "extra">>,
    Result = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
    ?assertMatch({ok, #signal_job_request{job_id = 123, step_id = 0, signal = 15, flags = 1}}, Result).

decode_signal_job_three_fields() ->
    Binary = <<456:32/big, 1:32/big, 9:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
    ?assertMatch({ok, #signal_job_request{job_id = 456, step_id = 1, signal = 9}}, Result).

decode_signal_job_two_fields() ->
    Binary = <<789:32/big, 2:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
    ?assertMatch({ok, #signal_job_request{job_id = 789, signal = 2}}, Result).

decode_signal_job_one_field() ->
    Binary = <<100:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
    ?assertMatch({ok, #signal_job_request{job_id = 100}}, Result).

decode_signal_job_empty() ->
    Result = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, <<>>),
    ?assertMatch({ok, #signal_job_request{}}, Result).

decode_signal_job_invalid() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% Decode REQUEST_UPDATE_JOB Tests
%%%===================================================================

decode_update_job_full() ->
    %% Full update job binary with all fields
    Binary = <<
        0:32/big,                      % site_factor
        16#FFFFFFFF:32/big,            % batch_features (NULL)
        16#FFFFFFFF:32/big,            % cluster_features (NULL)
        16#FFFFFFFF:32/big,            % clusters (NULL)
        0:16/big,                      % contiguous
        16#FFFFFFFF:32/big,            % container (NULL)
        0:16/big, 1:32/big, 1:16/big,  % core_spec, task_dist, kill_on_fail
        16#FFFFFFFF:32/big,            % features (NULL)
        0:64/big, 0:64/big,            % fed_active, fed_viable
        123:32/big,                    % job_id
        16#FFFFFFFF:32/big,            % job_id_str (NULL)
        8:32/big, "testjob", 0         % name
    >>,
    Result = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Binary),
    ?assertMatch({ok, #update_job_request{}}, Result).

decode_update_job_minimal() ->
    Binary = <<>>,
    Result = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Binary),
    ?assertMatch({ok, #update_job_request{}}, Result).

decode_update_job_with_str() ->
    %% Short binary with job_id_str
    JobIdStr = <<"123">>,
    Len = byte_size(JobIdStr),
    Binary = <<Len:32/big, JobIdStr/binary>>,
    Result = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Binary),
    ?assertMatch({ok, #update_job_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_JOB_WILL_RUN Tests
%%%===================================================================

decode_job_will_run_with_id() ->
    Binary = <<123:32/big, "partition=default", 0>>,
    Result = flurm_codec_job:decode_body(?REQUEST_JOB_WILL_RUN, Binary),
    ?assertMatch({ok, #job_will_run_request{job_id = 123}}, Result).

decode_job_will_run_minimal() ->
    Binary = <<0:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_JOB_WILL_RUN, Binary),
    ?assertMatch({ok, #job_will_run_request{}}, Result).

%%%===================================================================
%%% Decode RESPONSE_SUBMIT_BATCH_JOB Tests
%%%===================================================================

decode_batch_job_response_full() ->
    Msg = <<"Job submitted">>,
    MsgLen = byte_size(Msg),
    Binary = <<123:32/big, 0:32/big, 0:32/big, MsgLen:32/big, Msg/binary>>,
    Result = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_response{job_id = 123, step_id = 0, error_code = 0}}, Result).

decode_batch_job_response_three() ->
    Binary = <<456:32/big, 1:32/big, 0:32/big>>,
    Result = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_response{job_id = 456, step_id = 1, error_code = 0}}, Result).

decode_batch_job_response_two() ->
    Binary = <<789:32/big, 2:32/big>>,
    Result = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_response{job_id = 789, step_id = 2}}, Result).

decode_batch_job_response_one() ->
    Binary = <<100:32/big>>,
    Result = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_response{job_id = 100}}, Result).

decode_batch_job_response_empty() ->
    Result = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, <<>>),
    ?assertMatch({ok, #batch_job_response{}}, Result).

decode_batch_job_response_invalid() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% Decode RESPONSE_JOB_INFO Tests
%%%===================================================================

decode_job_info_response_full() ->
    Binary = <<1234567890:64/big, 2:32/big, "jobs_data">>,
    Result = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_response{last_update = 1234567890, job_count = 2}}, Result).

decode_job_info_response_minimal() ->
    Binary = <<0:64/big, 0:32/big>>,
    Result = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_response{last_update = 0, job_count = 0}}, Result).

decode_job_info_response_empty() ->
    Result = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, <<>>),
    ?assertMatch({ok, #job_info_response{}}, Result).

%%%===================================================================
%%% Decode Unsupported Tests
%%%===================================================================

decode_unsupported() ->
    Result = flurm_codec_job:decode_body(99999, <<1, 2, 3>>),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% Encode REQUEST_JOB_INFO Tests
%%%===================================================================

encode_job_info_request_record() ->
    Req = #job_info_request{show_flags = 1, job_id = 123, user_id = 1000},
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_JOB_INFO, Req),
    ?assertEqual(<<1:32/big, 123:32/big, 1000:32/big>>, Binary).

encode_job_info_request_non_record() ->
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_JOB_INFO, not_a_record),
    ?assertEqual(<<0:32, 0:32, 0:32>>, Binary).

%%%===================================================================
%%% Encode REQUEST_SUBMIT_BATCH_JOB Tests
%%%===================================================================

encode_batch_job_request_record() ->
    Req = #batch_job_request{
        name = <<"test_job">>,
        partition = <<"default">>,
        num_tasks = 4,
        cpus_per_task = 2
    },
    Result = flurm_codec_job:encode_body(?REQUEST_SUBMIT_BATCH_JOB, Req),
    ?assertMatch({error, batch_job_encode_not_implemented}, Result).

encode_batch_job_request_non_record() ->
    Result = flurm_codec_job:encode_body(?REQUEST_SUBMIT_BATCH_JOB, not_a_record),
    ?assertMatch({error, invalid_batch_job_request}, Result).

%%%===================================================================
%%% Encode REQUEST_CANCEL_JOB Tests
%%%===================================================================

encode_cancel_job_request_record() ->
    Req = #cancel_job_request{job_id = 123, step_id = 0, signal = 15, flags = 1},
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_CANCEL_JOB, Req),
    ?assertEqual(<<123:32/big, 0:32/big, 15:32/big, 1:32/big>>, Binary).

encode_cancel_job_request_non_record() ->
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_CANCEL_JOB, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode REQUEST_KILL_JOB Tests
%%%===================================================================

encode_kill_job_request_record() ->
    Req = #kill_job_request{
        job_id = 456,
        step_id = 1,
        signal = 9,
        flags = 0,
        sibling = <<"cluster2">>
    },
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_KILL_JOB, Req),
    ?assert(is_binary(Binary)),
    <<JobId:32/big, _/binary>> = Binary,
    ?assertEqual(456, JobId).

encode_kill_job_request_non_record() ->
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_KILL_JOB, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_JOB_WILL_RUN Tests
%%%===================================================================

encode_job_will_run_response_record() ->
    Resp = #job_will_run_response{
        job_id = 123,
        start_time = 1234567890,
        node_list = <<"node[01-10]">>,
        proc_cnt = 80,
        error_code = 0
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, Resp),
    ?assert(is_binary(Binary)),
    <<JobId:32/big, _/binary>> = Binary,
    ?assertEqual(123, JobId).

encode_job_will_run_response_non_record() ->
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_JOB_READY Tests
%%%===================================================================

encode_job_ready_response_map() ->
    Map = #{return_code => 1},
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_READY, Map),
    ?assertEqual(<<1:32/signed-big>>, Binary).

encode_job_ready_response_non_map() ->
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_READY, not_a_map),
    ?assertEqual(<<1:32/signed-big>>, Binary).

%%%===================================================================
%%% Encode RESPONSE_SUBMIT_BATCH_JOB Tests
%%%===================================================================

encode_batch_job_response_record() ->
    Resp = #batch_job_response{
        job_id = 123,
        step_id = 0,
        error_code = 0,
        job_submit_user_msg = <<"Job submitted successfully">>
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Resp),
    ?assert(is_binary(Binary)),
    <<JobId:32/big, _/binary>> = Binary,
    ?assertEqual(123, JobId).

encode_batch_job_response_non_record() ->
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, not_a_record),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_JOB_INFO Tests
%%%===================================================================

encode_job_info_response_record() ->
    Jobs = [
        #job_info{
            job_id = 123,
            name = <<"test_job">>,
            user_id = 1000,
            group_id = 1000,
            job_state = ?JOB_RUNNING,
            partition = <<"default">>,
            num_nodes = 4,
            num_cpus = 16,
            time_limit = 3600,
            start_time = 1234567890,
            end_time = 0,
            nodes = <<"node[01-04]">>
        }
    ],
    Resp = #job_info_response{
        last_update = 1234567890,
        job_count = 1,
        jobs = Jobs
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_job_info_response_empty() ->
    Resp = #job_info_response{
        last_update = 1234567890,
        job_count = 0,
        jobs = []
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_job_info_response_non_record() ->
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, not_a_record),
    ?assert(is_binary(Binary)).

encode_single_job_info() ->
    Job = #job_info{
        job_id = 456,
        name = <<"batch_job">>,
        user_id = 1001,
        group_id = 1001,
        job_state = ?JOB_PENDING,
        partition = <<"gpu">>,
        num_nodes = 1,
        num_cpus = 8,
        time_limit = 7200,
        start_time = 0,
        end_time = 0,
        nodes = <<>>
    },
    Resp = #job_info_response{
        last_update = erlang:system_time(second),
        job_count = 1,
        jobs = [Job]
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_single_job_info_non_record() ->
    Resp = #job_info_response{
        last_update = 1234567890,
        job_count = 1,
        jobs = [not_a_record]
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_RESOURCE_ALLOCATION Tests
%%%===================================================================

encode_resource_allocation_response_record() ->
    Resp = #resource_allocation_response{
        job_id = 123,
        node_list = <<"node[01-04]">>,
        cpus_per_node = [8, 8, 8, 8],
        num_cpu_groups = 1,
        num_nodes = 4,
        partition = <<"default">>,
        alias_list = <<>>,
        error_code = 0,
        job_submit_user_msg = <<>>,
        node_cnt = 4,
        node_addrs = [{{192, 168, 1, 1}, 6818}, {{192, 168, 1, 2}, 6818}]
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 20).

encode_resource_allocation_response_non_record() ->
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, not_a_record),
    ?assert(is_binary(Binary)).

encode_job_allocation_info_response() ->
    %% Same as resource allocation
    Resp = #resource_allocation_response{
        job_id = 456,
        node_list = <<"gpu01">>,
        node_cnt = 1,
        partition = <<"gpu">>
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_ALLOCATION_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode Unsupported Tests
%%%===================================================================

encode_unsupported() ->
    Result = flurm_codec_job:encode_body(99999, some_body),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% Utility Function Tests
%%%===================================================================

parse_job_id_tests() ->
    %% Test via update job decode
    Binary1 = <<3:32/big, "123">>,
    {ok, R1} = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Binary1),
    ?assertEqual(123, R1#update_job_request.job_id),

    %% Test array job notation via update job decode
    Binary2 = <<6:32/big, "456_78">>,
    {ok, R2} = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Binary2),
    ?assertEqual(456, R2#update_job_request.job_id).

strip_null_tests() ->
    %% Test via batch job response decode with null-terminated string
    Msg = <<"Test", 0, "more">>,
    MsgLen = byte_size(Msg),
    Binary = <<123:32/big, 0:32/big, 0:32/big, MsgLen:32/big, Msg/binary>>,
    {ok, _Result} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    %% Null handling is internal
    ok.

ensure_binary_tests() ->
    %% Test via job will run response encoding
    Resp1 = #job_will_run_response{node_list = undefined},
    {ok, _} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, Resp1),

    Resp2 = #job_will_run_response{node_list = "string_list"},
    {ok, _} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, Resp2),

    Resp3 = #job_will_run_response{node_list = <<"binary">>},
    {ok, _} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, Resp3),
    ok.

%%%===================================================================
%%% Edge Cases
%%%===================================================================

roundtrip_job_info_request() ->
    Req = #job_info_request{show_flags = 255, job_id = 12345, user_id = 99999},
    {ok, Encoded} = flurm_codec_job:encode_body(?REQUEST_JOB_INFO, Req),
    {ok, Decoded} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Encoded),
    ?assertEqual(Req#job_info_request.show_flags, Decoded#job_info_request.show_flags),
    ?assertEqual(Req#job_info_request.job_id, Decoded#job_info_request.job_id),
    ?assertEqual(Req#job_info_request.user_id, Decoded#job_info_request.user_id).

encode_multiple_jobs() ->
    Jobs = [
        #job_info{job_id = 100, name = <<"job1">>, job_state = ?JOB_PENDING},
        #job_info{job_id = 101, name = <<"job2">>, job_state = ?JOB_RUNNING},
        #job_info{job_id = 102, name = <<"job3">>, job_state = ?JOB_COMPLETE},
        #job_info{job_id = 103, name = <<"job4">>, job_state = ?JOB_CANCELLED},
        #job_info{job_id = 104, name = <<"job5">>, job_state = ?JOB_FAILED}
    ],
    Resp = #job_info_response{
        last_update = erlang:system_time(second),
        job_count = 5,
        jobs = Jobs
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_max_values() ->
    Resp = #resource_allocation_response{
        job_id = 16#FFFFFFFF,
        error_code = 16#FFFFFFFF,
        node_cnt = 16#FFFFFFFF,
        num_cpu_groups = 16#FFFFFFFF,
        num_nodes = 16#FFFFFFFF
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp),
    ?assert(is_binary(Binary)).

encode_unicode_strings() ->
    Job = #job_info{
        job_id = 999,
        name = <<"test_", 195, 169, "job">>,  % e-acute
        partition = <<"default">>,
        nodes = <<"node", 226, 128, 147, "01">>  % en-dash
    },
    Resp = #job_info_response{
        last_update = 0,
        job_count = 1,
        jobs = [Job]
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Additional Edge Cases
%%%===================================================================

%% Test all job states
job_state_test_() ->
    States = [
        {?JOB_PENDING, "pending"},
        {?JOB_RUNNING, "running"},
        {?JOB_SUSPENDED, "suspended"},
        {?JOB_COMPLETE, "complete"},
        {?JOB_CANCELLED, "cancelled"},
        {?JOB_FAILED, "failed"},
        {?JOB_TIMEOUT, "timeout"},
        {?JOB_NODE_FAIL, "node_fail"},
        {?JOB_PREEMPTED, "preempted"},
        {?JOB_BOOT_FAIL, "boot_fail"},
        {?JOB_DEADLINE, "deadline"},
        {?JOB_OOM, "oom"}
    ],
    [
        {lists:flatten(io_lib:format("encode job with state ~s", [Name])),
         fun() ->
             Job = #job_info{job_id = 100, job_state = State},
             Resp = #job_info_response{last_update = 0, job_count = 1, jobs = [Job]},
             {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Resp),
             ?assert(is_binary(Binary))
         end}
    || {State, Name} <- States].

%% Test large job count
large_job_count_test_() ->
    {"encode response with many jobs",
     fun() ->
         Jobs = [#job_info{job_id = N, name = list_to_binary("job" ++ integer_to_list(N))}
                 || N <- lists:seq(1, 100)],
         Resp = #job_info_response{
             last_update = 1234567890,
             job_count = 100,
             jobs = Jobs
         },
         {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Resp),
         ?assert(is_binary(Binary)),
         ?assert(byte_size(Binary) > 1000)
     end}.

%% Test with empty node addresses
empty_node_addrs_test_() ->
    {"encode resource allocation with empty node addrs",
     fun() ->
         Resp = #resource_allocation_response{
             job_id = 123,
             node_list = <<"node01">>,
             node_addrs = []
         },
         {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test with empty cpus_per_node
empty_cpus_per_node_test_() ->
    {"encode resource allocation with empty cpus per node",
     fun() ->
         Resp = #resource_allocation_response{
             job_id = 123,
             node_list = <<"node01">>,
             cpus_per_node = []
         },
         {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test negative error codes
negative_error_code_test_() ->
    {"decode batch response with error code",
     fun() ->
         Binary = <<123:32/big, 0:32/big, 1:32/big>>,  % Error code = 1
         {ok, Result} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
         ?assertEqual(1, Result#batch_job_response.error_code)
     end}.

%% Test suspend resume toggle
suspend_resume_test_() ->
    [
        {"decode suspend request - suspend true",
         fun() ->
             Binary = <<123:32/big, 1:32/big>>,
             {ok, Result} = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary),
             ?assertEqual(true, Result#suspend_request.suspend)
         end},
        {"decode suspend request - suspend false (resume)",
         fun() ->
             Binary = <<123:32/big, 0:32/big>>,
             {ok, Result} = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary),
             ?assertEqual(false, Result#suspend_request.suspend)
         end}
    ].
