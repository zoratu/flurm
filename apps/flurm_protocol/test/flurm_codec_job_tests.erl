%%%-------------------------------------------------------------------
%%% @doc Comprehensive unit tests for flurm_codec_job module.
%%%
%%% Tests for all job-related message encoding and decoding functions.
%%% Coverage target: ~90% of flurm_codec_job.erl (783 lines)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_codec_job_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Fixtures
%%%===================================================================

setup() ->
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Test Generators
%%%===================================================================

flurm_codec_job_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% decode_body/2 tests
      {"decode REQUEST_JOB_INFO - full", fun decode_job_info_request_full_test/0},
      {"decode REQUEST_JOB_INFO - partial", fun decode_job_info_request_partial_test/0},
      {"decode REQUEST_JOB_INFO - empty", fun decode_job_info_request_empty_test/0},
      {"decode REQUEST_RESOURCE_ALLOCATION", fun decode_resource_allocation_request_test/0},
      {"decode REQUEST_SUBMIT_BATCH_JOB - small binary", fun decode_batch_job_request_small_test/0},
      {"decode REQUEST_SUBMIT_BATCH_JOB - large binary", fun decode_batch_job_request_large_test/0},
      {"decode REQUEST_CANCEL_JOB - full format", fun decode_cancel_job_request_full_test/0},
      {"decode REQUEST_CANCEL_JOB - partial formats", fun decode_cancel_job_request_partial_test/0},
      {"decode REQUEST_CANCEL_JOB - empty", fun decode_cancel_job_request_empty_test/0},
      {"decode REQUEST_KILL_JOB - full format", fun decode_kill_job_request_full_test/0},
      {"decode REQUEST_KILL_JOB - partial formats", fun decode_kill_job_request_partial_test/0},
      {"decode REQUEST_KILL_JOB - empty", fun decode_kill_job_request_empty_test/0},
      {"decode REQUEST_SUSPEND - full format", fun decode_suspend_request_full_test/0},
      {"decode REQUEST_SUSPEND - partial formats", fun decode_suspend_request_partial_test/0},
      {"decode REQUEST_SIGNAL_JOB - full format", fun decode_signal_job_request_full_test/0},
      {"decode REQUEST_SIGNAL_JOB - partial formats", fun decode_signal_job_request_partial_test/0},
      {"decode REQUEST_UPDATE_JOB - small binary", fun decode_update_job_request_small_test/0},
      {"decode REQUEST_UPDATE_JOB - large binary", fun decode_update_job_request_large_test/0},
      {"decode REQUEST_JOB_WILL_RUN - with job_id", fun decode_job_will_run_request_with_job_id_test/0},
      {"decode REQUEST_JOB_WILL_RUN - without job_id", fun decode_job_will_run_request_without_job_id_test/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB - full", fun decode_batch_job_response_full_test/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB - partial", fun decode_batch_job_response_partial_test/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB - empty", fun decode_batch_job_response_empty_test/0},
      {"decode RESPONSE_JOB_INFO - full", fun decode_job_info_response_full_test/0},
      {"decode RESPONSE_JOB_INFO - partial", fun decode_job_info_response_partial_test/0},
      {"decode RESPONSE_JOB_INFO - empty", fun decode_job_info_response_empty_test/0},
      {"decode unsupported message type", fun decode_unsupported_test/0},

      %% encode_body/2 tests
      {"encode REQUEST_JOB_INFO", fun encode_job_info_request_test/0},
      {"encode REQUEST_JOB_INFO - default", fun encode_job_info_request_default_test/0},
      {"encode REQUEST_SUBMIT_BATCH_JOB", fun encode_batch_job_request_test/0},
      {"encode REQUEST_SUBMIT_BATCH_JOB - invalid", fun encode_batch_job_request_invalid_test/0},
      {"encode REQUEST_CANCEL_JOB", fun encode_cancel_job_request_test/0},
      {"encode REQUEST_CANCEL_JOB - default", fun encode_cancel_job_request_default_test/0},
      {"encode REQUEST_KILL_JOB", fun encode_kill_job_request_test/0},
      {"encode REQUEST_KILL_JOB - default", fun encode_kill_job_request_default_test/0},
      {"encode RESPONSE_JOB_WILL_RUN", fun encode_job_will_run_response_test/0},
      {"encode RESPONSE_JOB_WILL_RUN - default", fun encode_job_will_run_response_default_test/0},
      {"encode RESPONSE_JOB_READY", fun encode_job_ready_response_test/0},
      {"encode RESPONSE_JOB_READY - default", fun encode_job_ready_response_default_test/0},
      {"encode RESPONSE_SUBMIT_BATCH_JOB", fun encode_batch_job_response_test/0},
      {"encode RESPONSE_SUBMIT_BATCH_JOB - default", fun encode_batch_job_response_default_test/0},
      {"encode RESPONSE_JOB_INFO", fun encode_job_info_response_test/0},
      {"encode RESPONSE_JOB_INFO - default", fun encode_job_info_response_default_test/0},
      {"encode RESPONSE_JOB_INFO - with jobs", fun encode_job_info_response_with_jobs_test/0},
      {"encode RESPONSE_RESOURCE_ALLOCATION", fun encode_resource_allocation_response_test/0},
      {"encode RESPONSE_RESOURCE_ALLOCATION - default", fun encode_resource_allocation_response_default_test/0},
      {"encode RESPONSE_JOB_ALLOCATION_INFO", fun encode_job_allocation_info_response_test/0},
      {"encode unsupported message type", fun encode_unsupported_test/0},

      %% Edge case tests
      {"decode with malformed binary", fun decode_malformed_binary_test/0},
      {"encode/decode roundtrip - cancel job", fun roundtrip_cancel_job_test/0},
      {"encode/decode roundtrip - kill job", fun roundtrip_kill_job_test/0},
      {"encode/decode roundtrip - job info request", fun roundtrip_job_info_request_test/0},
      {"encode/decode roundtrip - batch job response", fun roundtrip_batch_job_response_test/0},

      %% Internal helper tests
      {"extract_job_resources - small binary", fun extract_job_resources_small_test/0},
      {"extract_job_resources - large binary", fun extract_job_resources_large_test/0},
      {"skip_packstr - NULL string", fun skip_packstr_null_test/0},
      {"skip_packstr - valid string", fun skip_packstr_valid_test/0},
      {"skip_packstr - empty", fun skip_packstr_empty_test/0},
      {"strip_null - with null", fun strip_null_with_null_test/0},
      {"strip_null - without null", fun strip_null_without_null_test/0},
      {"strip_null - empty", fun strip_null_empty_test/0},
      {"ensure_binary - various types", fun ensure_binary_various_test/0},
      {"parse_job_id - various formats", fun parse_job_id_various_test/0},

      %% Boundary value tests
      {"decode with NO_VAL values", fun decode_no_val_test/0},
      {"decode with max uint32 values", fun decode_max_uint32_test/0},
      {"encode with empty strings", fun encode_empty_strings_test/0},
      {"encode with large strings", fun encode_large_strings_test/0},

      %% Array encoding tests
      {"encode uint16 array - empty", fun encode_uint16_array_empty_test/0},
      {"encode uint16 array - with values", fun encode_uint16_array_with_values_test/0},
      {"encode uint32 array - empty", fun encode_uint32_array_empty_test/0},
      {"encode uint32 array - with values", fun encode_uint32_array_with_values_test/0},
      {"encode node_addrs - empty", fun encode_node_addrs_empty_test/0},
      {"encode node_addrs - with values", fun encode_node_addrs_with_values_test/0}
     ]}.

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_JOB_INFO
%%%===================================================================

decode_job_info_request_full_test() ->
    Binary = <<0:32/big, 123:32/big, 1000:32/big>>,
    Result = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_request{}}, Result),
    {ok, Body} = Result,
    ?assertEqual(0, Body#job_info_request.show_flags),
    ?assertEqual(123, Body#job_info_request.job_id),
    ?assertEqual(1000, Body#job_info_request.user_id).

decode_job_info_request_partial_test() ->
    %% With show_flags and job_id only
    Binary1 = <<1:32/big, 456:32/big>>,
    {ok, Body1} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary1),
    ?assertEqual(1, Body1#job_info_request.show_flags),
    ?assertEqual(456, Body1#job_info_request.job_id),

    %% With show_flags only
    Binary2 = <<2:32/big>>,
    {ok, Body2} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary2),
    ?assertEqual(2, Body2#job_info_request.show_flags).

decode_job_info_request_empty_test() ->
    Result = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, <<>>),
    ?assertMatch({ok, #job_info_request{}}, Result).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_RESOURCE_ALLOCATION
%%%===================================================================

decode_resource_allocation_request_test() ->
    %% Resource allocation uses same format as batch job
    Binary = <<0:256>>,  % Small binary
    Result = flurm_codec_job:decode_body(?REQUEST_RESOURCE_ALLOCATION, Binary),
    ?assertMatch({ok, #batch_job_request{}}, Result).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_SUBMIT_BATCH_JOB
%%%===================================================================

decode_batch_job_request_small_test() ->
    Binary = <<0:128>>,  % Small binary < 32 bytes
    Result = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_request{}}, Result).

decode_batch_job_request_large_test() ->
    %% Create a larger binary with valid SLURM structure
    SiteFactor = <<0:32/big>>,
    BatchFeatures = <<16#FFFFFFFF:32/big>>,  % NULL string
    ClusterFeatures = <<16#FFFFFFFF:32/big>>,
    Clusters = <<16#FFFFFFFF:32/big>>,
    Contiguous = <<0:16/big>>,
    Container = <<16#FFFFFFFF:32/big>>,
    CoreSpec = <<0:16/big>>,
    TaskDist = <<0:32/big>>,
    KillOnFail = <<0:16/big>>,
    Features = <<16#FFFFFFFF:32/big>>,
    FedActive = <<0:64/big>>,
    FedViable = <<0:64/big>>,
    JobId = <<12345:32/big>>,
    JobIdStr = <<16#FFFFFFFF:32/big>>,
    Name = <<4:32/big, "test">>,
    Partition = <<7:32/big, "default">>,
    Padding = binary:copy(<<0>>, 300),

    Binary = <<SiteFactor/binary, BatchFeatures/binary, ClusterFeatures/binary,
               Clusters/binary, Contiguous/binary, Container/binary, CoreSpec/binary,
               TaskDist/binary, KillOnFail/binary, Features/binary, FedActive/binary,
               FedViable/binary, JobId/binary, JobIdStr/binary, Name/binary,
               Partition/binary, Padding/binary>>,

    Result = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_request{}}, Result).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_CANCEL_JOB
%%%===================================================================

decode_cancel_job_request_full_test() ->
    Binary = <<100:32/big, 0:32/big, 9:32/big, 1:32/big, "extra">>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
    ?assertEqual(100, Body#cancel_job_request.job_id),
    ?assertEqual(0, Body#cancel_job_request.step_id),
    ?assertEqual(9, Body#cancel_job_request.signal),
    ?assertEqual(1, Body#cancel_job_request.flags).

decode_cancel_job_request_partial_test() ->
    %% 3 fields
    Binary1 = <<100:32/big, 1:32/big, 15:32/big>>,
    {ok, Body1} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary1),
    ?assertEqual(100, Body1#cancel_job_request.job_id),
    ?assertEqual(1, Body1#cancel_job_request.step_id),
    ?assertEqual(15, Body1#cancel_job_request.signal),

    %% 2 fields
    Binary2 = <<200:32/big, 2:32/big>>,
    {ok, Body2} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary2),
    ?assertEqual(200, Body2#cancel_job_request.job_id),
    ?assertEqual(2, Body2#cancel_job_request.step_id),
    ?assertEqual(9, Body2#cancel_job_request.signal),  % Default SIGKILL

    %% 1 field
    Binary3 = <<300:32/big>>,
    {ok, Body3} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary3),
    ?assertEqual(300, Body3#cancel_job_request.job_id),
    ?assertEqual(9, Body3#cancel_job_request.signal).

decode_cancel_job_request_empty_test() ->
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, <<>>),
    ?assertMatch(#cancel_job_request{}, Body).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_KILL_JOB
%%%===================================================================

decode_kill_job_request_full_test() ->
    %% With sibling string
    SiblingStr = <<"sibling">>,
    SibLen = byte_size(SiblingStr),
    Binary = <<100:32/big, 0:32/big, 9:32/big, 1:32/big, SibLen:32/big, SiblingStr/binary>>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary),
    ?assertEqual(100, Body#kill_job_request.job_id),
    ?assertEqual(0, Body#kill_job_request.step_id),
    ?assertEqual(9, Body#kill_job_request.signal),
    ?assertEqual(1, Body#kill_job_request.flags),
    ?assertEqual(SiblingStr, Body#kill_job_request.sibling).

decode_kill_job_request_partial_test() ->
    %% 4 fields without sibling
    Binary1 = <<100:32/big, 0:32/big, 9:32/big, 1:32/big>>,
    {ok, Body1} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary1),
    ?assertEqual(100, Body1#kill_job_request.job_id),
    ?assertEqual(0, Body1#kill_job_request.step_id),
    ?assertEqual(9, Body1#kill_job_request.signal),
    ?assertEqual(1, Body1#kill_job_request.flags),

    %% 3 fields
    Binary2 = <<200:32/big, 1:32/big, 15:32/big>>,
    {ok, Body2} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary2),
    ?assertEqual(200, Body2#kill_job_request.job_id),
    ?assertEqual(1, Body2#kill_job_request.step_id),
    ?assertEqual(15, Body2#kill_job_request.signal),

    %% 1 field
    Binary3 = <<300:32/big>>,
    {ok, Body3} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary3),
    ?assertEqual(300, Body3#kill_job_request.job_id).

decode_kill_job_request_empty_test() ->
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, <<>>),
    ?assertMatch(#kill_job_request{}, Body).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_SUSPEND
%%%===================================================================

decode_suspend_request_full_test() ->
    %% Suspend = true (non-zero)
    Binary1 = <<100:32/big, 1:32/big, "extra">>,
    {ok, Body1} = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary1),
    ?assertEqual(100, Body1#suspend_request.job_id),
    ?assertEqual(true, Body1#suspend_request.suspend),

    %% Suspend = false (zero)
    Binary2 = <<200:32/big, 0:32/big>>,
    {ok, Body2} = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary2),
    ?assertEqual(200, Body2#suspend_request.job_id),
    ?assertEqual(false, Body2#suspend_request.suspend).

decode_suspend_request_partial_test() ->
    %% Only job_id
    Binary = <<100:32/big>>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary),
    ?assertEqual(100, Body#suspend_request.job_id),

    %% Empty
    {ok, Body2} = flurm_codec_job:decode_body(?REQUEST_SUSPEND, <<>>),
    ?assertMatch(#suspend_request{}, Body2).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_SIGNAL_JOB
%%%===================================================================

decode_signal_job_request_full_test() ->
    Binary = <<100:32/big, 0:32/big, 15:32/big, 1:32/big, "extra">>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
    ?assertEqual(100, Body#signal_job_request.job_id),
    ?assertEqual(0, Body#signal_job_request.step_id),
    ?assertEqual(15, Body#signal_job_request.signal),
    ?assertEqual(1, Body#signal_job_request.flags).

decode_signal_job_request_partial_test() ->
    %% 3 fields
    Binary1 = <<100:32/big, 0:32/big, 15:32/big>>,
    {ok, Body1} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary1),
    ?assertEqual(100, Body1#signal_job_request.job_id),
    ?assertEqual(0, Body1#signal_job_request.step_id),
    ?assertEqual(15, Body1#signal_job_request.signal),

    %% 2 fields (job_id + signal without step_id)
    Binary2 = <<200:32/big, 9:32/big>>,
    {ok, Body2} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary2),
    ?assertEqual(200, Body2#signal_job_request.job_id),
    ?assertEqual(9, Body2#signal_job_request.signal),

    %% 1 field
    Binary3 = <<300:32/big>>,
    {ok, Body3} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary3),
    ?assertEqual(300, Body3#signal_job_request.job_id),

    %% Empty
    {ok, Body4} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, <<>>),
    ?assertMatch(#signal_job_request{}, Body4).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_UPDATE_JOB
%%%===================================================================

decode_update_job_request_small_test() ->
    %% Small binary with job_id_str
    %% unpack_string expects: <<Length:32/big, Data:Length/binary>> where Length includes null
    JobIdStr = <<"12345">>,
    StrLen = byte_size(JobIdStr) + 1,  %% +1 for null terminator
    Binary = <<StrLen:32/big, JobIdStr/binary, 0>>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Binary),
    ?assertEqual(12345, Body#update_job_request.job_id),
    ?assertEqual(JobIdStr, Body#update_job_request.job_id_str).

decode_update_job_request_large_test() ->
    %% Create a larger binary with valid structure
    SiteFactor = <<0:32/big>>,
    BatchFeatures = <<16#FFFFFFFF:32/big>>,
    ClusterFeatures = <<16#FFFFFFFF:32/big>>,
    Clusters = <<16#FFFFFFFF:32/big>>,
    Contiguous = <<0:16/big>>,
    Container = <<16#FFFFFFFF:32/big>>,
    CoreSpec = <<0:16/big>>,
    TaskDist = <<0:32/big>>,
    KillOnFail = <<0:16/big>>,
    Features = <<16#FFFFFFFF:32/big>>,
    FedActive = <<0:64/big>>,
    FedViable = <<0:64/big>>,
    JobId = <<12345:32/big>>,
    JobIdStr = <<5:32/big, "12345">>,
    Name = <<7:32/big, "jobname">>,
    Padding = binary:copy(<<0>>, 400),

    Binary = <<SiteFactor/binary, BatchFeatures/binary, ClusterFeatures/binary,
               Clusters/binary, Contiguous/binary, Container/binary, CoreSpec/binary,
               TaskDist/binary, KillOnFail/binary, Features/binary, FedActive/binary,
               FedViable/binary, JobId/binary, JobIdStr/binary, Name/binary,
               Padding/binary>>,

    Result = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Binary),
    ?assertMatch({ok, #update_job_request{}}, Result).

%%%===================================================================
%%% decode_body/2 Tests - REQUEST_JOB_WILL_RUN
%%%===================================================================

decode_job_will_run_request_with_job_id_test() ->
    Binary = <<12345:32/big, "partition=batch", 0>>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_WILL_RUN, Binary),
    ?assertEqual(12345, Body#job_will_run_request.job_id),
    ?assertEqual(<<"batch">>, Body#job_will_run_request.partition).

decode_job_will_run_request_without_job_id_test() ->
    %% Binary that doesn't start with a valid job_id (first 32-bit is 0)
    %% The find_partition_in_binary function looks for "partition=" literal
    Binary = <<0:32/big, "partition=compute", 0>>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_WILL_RUN, Binary),
    ?assertEqual(<<"compute">>, Body#job_will_run_request.partition).

%%%===================================================================
%%% decode_body/2 Tests - RESPONSE_SUBMIT_BATCH_JOB
%%%===================================================================

decode_batch_job_response_full_test() ->
    Msg = <<"Job submitted">>,
    MsgLen = byte_size(Msg),
    Binary = <<100:32/big, 0:32/big, 0:32/big, MsgLen:32/big, Msg/binary>>,
    {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertEqual(100, Body#batch_job_response.job_id),
    ?assertEqual(0, Body#batch_job_response.step_id),
    ?assertEqual(0, Body#batch_job_response.error_code),
    ?assertEqual(Msg, Body#batch_job_response.job_submit_user_msg).

decode_batch_job_response_partial_test() ->
    %% 3 fields
    Binary1 = <<100:32/big, 0:32/big, 0:32/big>>,
    {ok, Body1} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary1),
    ?assertEqual(100, Body1#batch_job_response.job_id),
    ?assertEqual(0, Body1#batch_job_response.step_id),
    ?assertEqual(0, Body1#batch_job_response.error_code),

    %% 2 fields
    Binary2 = <<200:32/big, 1:32/big>>,
    {ok, Body2} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary2),
    ?assertEqual(200, Body2#batch_job_response.job_id),
    ?assertEqual(1, Body2#batch_job_response.step_id),

    %% 1 field
    Binary3 = <<300:32/big>>,
    {ok, Body3} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary3),
    ?assertEqual(300, Body3#batch_job_response.job_id).

decode_batch_job_response_empty_test() ->
    {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, <<>>),
    ?assertMatch(#batch_job_response{}, Body).

%%%===================================================================
%%% decode_body/2 Tests - RESPONSE_JOB_INFO
%%%===================================================================

decode_job_info_response_full_test() ->
    Binary = <<1700000000:64/big, 5:32/big, "jobdata">>,
    {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, Binary),
    ?assertEqual(1700000000, Body#job_info_response.last_update),
    ?assertEqual(5, Body#job_info_response.job_count),
    ?assertEqual([], Body#job_info_response.jobs).

decode_job_info_response_partial_test() ->
    Binary = <<1700000000:64/big, 0:32/big>>,
    {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, Binary),
    ?assertEqual(1700000000, Body#job_info_response.last_update),
    ?assertEqual(0, Body#job_info_response.job_count).

decode_job_info_response_empty_test() ->
    {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, <<>>),
    ?assertMatch(#job_info_response{}, Body).

%%%===================================================================
%%% decode_body/2 Tests - Unsupported
%%%===================================================================

decode_unsupported_test() ->
    Result = flurm_codec_job:decode_body(99999, <<"data">>),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% encode_body/2 Tests - REQUEST_JOB_INFO
%%%===================================================================

encode_job_info_request_test() ->
    Request = #job_info_request{
        show_flags = 1,
        job_id = 12345,
        user_id = 1000
    },
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_JOB_INFO, Request),
    ?assertEqual(<<1:32/big, 12345:32/big, 1000:32/big>>, Binary).

encode_job_info_request_default_test() ->
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_JOB_INFO, #{}),
    ?assertEqual(<<0:32, 0:32, 0:32>>, Binary).

%%%===================================================================
%%% encode_body/2 Tests - REQUEST_SUBMIT_BATCH_JOB
%%%===================================================================

encode_batch_job_request_test() ->
    Request = #batch_job_request{
        name = <<"test_job">>,
        partition = <<"default">>,
        script = <<"#!/bin/bash\necho hello">>
    },
    Result = flurm_codec_job:encode_body(?REQUEST_SUBMIT_BATCH_JOB, Request),
    ?assertMatch({error, batch_job_encode_not_implemented}, Result).

encode_batch_job_request_invalid_test() ->
    Result = flurm_codec_job:encode_body(?REQUEST_SUBMIT_BATCH_JOB, invalid),
    ?assertMatch({error, invalid_batch_job_request}, Result).

%%%===================================================================
%%% encode_body/2 Tests - REQUEST_CANCEL_JOB
%%%===================================================================

encode_cancel_job_request_test() ->
    Request = #cancel_job_request{
        job_id = 12345,
        step_id = 0,
        signal = 15,
        flags = 1
    },
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_CANCEL_JOB, Request),
    ?assertEqual(<<12345:32/big, 0:32/big, 15:32/big, 1:32/big>>, Binary).

encode_cancel_job_request_default_test() ->
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_CANCEL_JOB, #{}),
    ?assertEqual(<<0:32, ?SLURM_NO_VAL:32, 9:32, 0:32>>, Binary).

%%%===================================================================
%%% encode_body/2 Tests - REQUEST_KILL_JOB
%%%===================================================================

encode_kill_job_request_test() ->
    Request = #kill_job_request{
        job_id = 12345,
        step_id = 0,
        signal = 9,
        flags = 0,
        sibling = <<"sibling">>
    },
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_KILL_JOB, Request),
    ?assert(is_binary(Binary)),
    %% Check job_id at beginning
    <<JobId:32/big, _/binary>> = Binary,
    ?assertEqual(12345, JobId).

encode_kill_job_request_default_test() ->
    {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_KILL_JOB, #{}),
    ?assert(is_binary(Binary)),
    <<JobId:32/big, _/binary>> = Binary,
    ?assertEqual(0, JobId).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_JOB_WILL_RUN
%%%===================================================================

encode_job_will_run_response_test() ->
    Response = #job_will_run_response{
        job_id = 12345,
        start_time = 1700000000,
        node_list = <<"node001">>,
        proc_cnt = 4,
        error_code = 0
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, Response),
    ?assert(is_binary(Binary)),
    <<JobId:32/big, _/binary>> = Binary,
    ?assertEqual(12345, JobId).

encode_job_will_run_response_default_test() ->
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, #{}),
    ?assertEqual(<<0:32, 0:64, 0:32, 0:32, 0:32>>, Binary).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_JOB_READY
%%%===================================================================

encode_job_ready_response_test() ->
    Response = #{return_code => 0},
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_READY, Response),
    ?assertEqual(<<0:32/signed-big>>, Binary).

encode_job_ready_response_default_test() ->
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_READY, #{}),
    ?assertEqual(<<1:32/signed-big>>, Binary).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_SUBMIT_BATCH_JOB
%%%===================================================================

encode_batch_job_response_test() ->
    Response = #batch_job_response{
        job_id = 12345,
        step_id = 0,
        error_code = 0,
        job_submit_user_msg = <<"OK">>
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Response),
    ?assert(is_binary(Binary)),
    <<JobId:32/big, StepId:32/big, ErrorCode:32/big, _/binary>> = Binary,
    ?assertEqual(12345, JobId),
    ?assertEqual(0, StepId),
    ?assertEqual(0, ErrorCode).

encode_batch_job_response_default_test() ->
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, #{}),
    ?assertEqual(<<0:32, 0:32, 0:32, 0:32>>, Binary).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_JOB_INFO
%%%===================================================================

encode_job_info_response_test() ->
    Response = #job_info_response{
        last_update = 1700000000,
        job_count = 0,
        jobs = []
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Response),
    ?assert(is_binary(Binary)).

encode_job_info_response_default_test() ->
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, #{}),
    ?assertEqual(<<0:64, 0:32>>, Binary).

encode_job_info_response_with_jobs_test() ->
    Job = #job_info{
        job_id = 12345,
        name = <<"test_job">>,
        user_id = 1000,
        group_id = 1000,
        job_state = 1,
        partition = <<"default">>,
        num_nodes = 1,
        num_cpus = 4,
        time_limit = 3600,
        start_time = 1700000000,
        end_time = 0,
        nodes = <<"node001">>
    },
    Response = #job_info_response{
        last_update = 1700000000,
        job_count = 1,
        jobs = [Job]
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Response),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 12).

%%%===================================================================
%%% encode_body/2 Tests - RESPONSE_RESOURCE_ALLOCATION
%%%===================================================================

encode_resource_allocation_response_test() ->
    Response = #resource_allocation_response{
        job_id = 12345,
        node_list = <<"node001">>,
        cpus_per_node = [4],
        num_cpu_groups = 1,
        node_cnt = 1,
        partition = <<"default">>,
        alias_list = <<>>,
        error_code = 0,
        job_submit_user_msg = <<>>,
        node_addrs = [{{172, 19, 0, 3}, 6818}]
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
    ?assert(is_binary(Binary)),
    <<ErrorCode:32/big, JobId:32/big, _/binary>> = Binary,
    ?assertEqual(0, ErrorCode),
    ?assertEqual(12345, JobId).

encode_resource_allocation_response_default_test() ->
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, #{}),
    ?assert(is_binary(Binary)).

encode_job_allocation_info_response_test() ->
    Response = #resource_allocation_response{
        job_id = 12345,
        node_list = <<"node001">>,
        cpus_per_node = [4],
        num_cpu_groups = 1,
        node_cnt = 1
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_ALLOCATION_INFO, Response),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% encode_body/2 Tests - Unsupported
%%%===================================================================

encode_unsupported_test() ->
    Result = flurm_codec_job:encode_body(99999, #{}),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% Malformed Binary Tests
%%%===================================================================

decode_malformed_binary_test() ->
    %% Invalid job info request (partial byte)
    Result1 = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, <<1, 2, 3>>),
    ?assertMatch({error, invalid_job_info_request}, Result1),

    %% Invalid cancel job request
    Result2 = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, <<1, 2>>),
    ?assertMatch({error, invalid_cancel_job_request}, Result2),

    %% Invalid kill job request
    Result3 = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, <<1, 2, 3>>),
    ?assertMatch({error, invalid_kill_job_request}, Result3),

    %% Invalid suspend request
    Result4 = flurm_codec_job:decode_body(?REQUEST_SUSPEND, <<1, 2>>),
    ?assertMatch({error, invalid_suspend_request}, Result4),

    %% Invalid signal job request
    Result5 = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, <<1, 2>>),
    ?assertMatch({error, invalid_signal_job_request}, Result5),

    %% Invalid batch job response
    Result6 = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, <<1, 2>>),
    ?assertMatch({error, invalid_batch_job_response}, Result6).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_cancel_job_test() ->
    Request = #cancel_job_request{
        job_id = 12345,
        step_id = 0,
        signal = 15,
        flags = 1
    },
    {ok, Encoded} = flurm_codec_job:encode_body(?REQUEST_CANCEL_JOB, Request),
    {ok, Decoded} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Encoded),
    ?assertEqual(Request#cancel_job_request.job_id, Decoded#cancel_job_request.job_id),
    ?assertEqual(Request#cancel_job_request.step_id, Decoded#cancel_job_request.step_id),
    ?assertEqual(Request#cancel_job_request.signal, Decoded#cancel_job_request.signal),
    ?assertEqual(Request#cancel_job_request.flags, Decoded#cancel_job_request.flags).

roundtrip_kill_job_test() ->
    Request = #kill_job_request{
        job_id = 12345,
        step_id = 0,
        signal = 9,
        flags = 0,
        sibling = <<"cluster1">>
    },
    {ok, Encoded} = flurm_codec_job:encode_body(?REQUEST_KILL_JOB, Request),
    {ok, Decoded} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Encoded),
    ?assertEqual(Request#kill_job_request.job_id, Decoded#kill_job_request.job_id),
    ?assertEqual(Request#kill_job_request.step_id, Decoded#kill_job_request.step_id),
    ?assertEqual(Request#kill_job_request.signal, Decoded#kill_job_request.signal),
    ?assertEqual(Request#kill_job_request.flags, Decoded#kill_job_request.flags).

roundtrip_job_info_request_test() ->
    Request = #job_info_request{
        show_flags = 1,
        job_id = 12345,
        user_id = 1000
    },
    {ok, Encoded} = flurm_codec_job:encode_body(?REQUEST_JOB_INFO, Request),
    {ok, Decoded} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Encoded),
    ?assertEqual(Request#job_info_request.show_flags, Decoded#job_info_request.show_flags),
    ?assertEqual(Request#job_info_request.job_id, Decoded#job_info_request.job_id),
    ?assertEqual(Request#job_info_request.user_id, Decoded#job_info_request.user_id).

roundtrip_batch_job_response_test() ->
    Response = #batch_job_response{
        job_id = 12345,
        step_id = 0,
        error_code = 0,
        job_submit_user_msg = <<"Job submitted">>
    },
    {ok, Encoded} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Response),
    {ok, Decoded} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Encoded),
    ?assertEqual(Response#batch_job_response.job_id, Decoded#batch_job_response.job_id),
    ?assertEqual(Response#batch_job_response.step_id, Decoded#batch_job_response.step_id),
    ?assertEqual(Response#batch_job_response.error_code, Decoded#batch_job_response.error_code).

%%%===================================================================
%%% Internal Helper Tests
%%%===================================================================

extract_job_resources_small_test() ->
    Binary = <<0:64>>,
    %% This is an internal function test - we call decode_body which uses it
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch(#batch_job_request{}, Body).

extract_job_resources_large_test() ->
    %% Create a binary large enough to extract resources from
    Binary = binary:copy(<<0>>, 250),
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch(#batch_job_request{}, Body).

skip_packstr_null_test() ->
    %% NULL string (NO_VAL marker)
    Binary = <<16#FFFFFFFF:32/big, "rest">>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, <<0:32/big, Binary/binary>>),
    ?assertMatch(#batch_job_request{}, Body).

skip_packstr_valid_test() ->
    %% Valid string
    Str = <<"hello">>,
    Len = byte_size(Str),
    Binary = <<Len:32/big, Str/binary, "rest">>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, <<0:32/big, Binary/binary>>),
    ?assertMatch(#batch_job_request{}, Body).

skip_packstr_empty_test() ->
    %% Empty string (length = 0)
    Binary = <<0:32/big, "rest">>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, <<0:32/big, Binary/binary>>),
    ?assertMatch(#batch_job_request{}, Body).

strip_null_with_null_test() ->
    %% Test via batch job response which strips nulls from message
    Msg = <<"test", 0, "extra">>,
    MsgLen = byte_size(Msg),
    Binary = <<100:32/big, 0:32/big, 0:32/big, MsgLen:32/big, Msg/binary>>,
    {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch(#batch_job_response{}, Body).

strip_null_without_null_test() ->
    Msg = <<"test_message">>,
    MsgLen = byte_size(Msg),
    Binary = <<100:32/big, 0:32/big, 0:32/big, MsgLen:32/big, Msg/binary>>,
    {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch(#batch_job_response{}, Body).

strip_null_empty_test() ->
    Binary = <<100:32/big, 0:32/big, 0:32/big, 0:32/big>>,
    {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch(#batch_job_response{}, Body).

ensure_binary_various_test() ->
    %% Test via encoding which uses ensure_binary
    Response1 = #batch_job_response{
        job_id = 1,
        job_submit_user_msg = undefined
    },
    {ok, _} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Response1),

    Response2 = #batch_job_response{
        job_id = 2,
        job_submit_user_msg = <<"binary">>
    },
    {ok, _} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Response2),

    Response3 = #batch_job_response{
        job_id = 3,
        job_submit_user_msg = "list"
    },
    {ok, _} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Response3).

parse_job_id_various_test() ->
    %% Test via update_job_request which parses job IDs
    %% unpack_string expects: <<Length:32/big, Data:Length/binary>> where Data includes null terminator

    %% Simple numeric - "12345" + null = 6 bytes
    Binary1 = <<6:32/big, "12345", 0>>,
    {ok, Body1} = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Binary1),
    ?assertEqual(12345, Body1#update_job_request.job_id),

    %% Array notation - "12345_67" + null = 9 bytes
    Binary2 = <<9:32/big, "12345_67", 0>>,
    {ok, Body2} = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Binary2),
    ?assertEqual(12345, Body2#update_job_request.job_id),

    %% Empty (zero length means undefined) - but record defaults job_id to 0
    Binary3 = <<0:32/big>>,
    {ok, Body3} = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Binary3),
    ?assertEqual(0, Body3#update_job_request.job_id).

%%%===================================================================
%%% Boundary Value Tests
%%%===================================================================

decode_no_val_test() ->
    %% NO_VAL in job info request
    Binary = <<?SLURM_NO_VAL:32/big, ?SLURM_NO_VAL:32/big, ?SLURM_NO_VAL:32/big>>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
    ?assertEqual(?SLURM_NO_VAL, Body#job_info_request.show_flags),
    ?assertEqual(?SLURM_NO_VAL, Body#job_info_request.job_id),
    ?assertEqual(?SLURM_NO_VAL, Body#job_info_request.user_id).

decode_max_uint32_test() ->
    %% Max uint32 values
    Binary = <<16#FFFFFFFF:32/big, 16#FFFFFFFF:32/big, 16#FFFFFFFF:32/big>>,
    {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
    ?assertEqual(16#FFFFFFFF, Body#job_info_request.show_flags).

encode_empty_strings_test() ->
    Response = #resource_allocation_response{
        job_id = 1,
        node_list = <<>>,
        partition = <<>>,
        alias_list = <<>>,
        job_submit_user_msg = <<>>,
        cpus_per_node = [],
        num_cpu_groups = 0,
        node_cnt = 0,
        node_addrs = []
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
    ?assert(is_binary(Binary)).

encode_large_strings_test() ->
    LargeString = binary:copy(<<"A">>, 1000),
    Response = #resource_allocation_response{
        job_id = 1,
        node_list = LargeString,
        partition = LargeString,
        alias_list = <<>>,
        job_submit_user_msg = LargeString,
        cpus_per_node = [],
        num_cpu_groups = 0,
        node_cnt = 0,
        node_addrs = []
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 3000).

%%%===================================================================
%%% Array Encoding Tests
%%%===================================================================

encode_uint16_array_empty_test() ->
    Response = #resource_allocation_response{
        job_id = 1,
        cpus_per_node = [],
        num_cpu_groups = 0,
        node_cnt = 0,
        node_addrs = []
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
    ?assert(is_binary(Binary)).

encode_uint16_array_with_values_test() ->
    Response = #resource_allocation_response{
        job_id = 1,
        cpus_per_node = [4, 8, 16],
        num_cpu_groups = 3,
        node_cnt = 3,
        node_addrs = []
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
    ?assert(is_binary(Binary)).

encode_uint32_array_empty_test() ->
    Response = #resource_allocation_response{
        job_id = 1,
        cpus_per_node = [],
        num_cpu_groups = 0,
        node_cnt = 0,
        node_addrs = []
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
    ?assert(is_binary(Binary)).

encode_uint32_array_with_values_test() ->
    Response = #resource_allocation_response{
        job_id = 1,
        cpus_per_node = [4, 8],
        num_cpu_groups = 2,
        node_cnt = 2,
        node_addrs = []
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
    ?assert(is_binary(Binary)).

encode_node_addrs_empty_test() ->
    Response = #resource_allocation_response{
        job_id = 1,
        node_addrs = [],
        cpus_per_node = [],
        num_cpu_groups = 0,
        node_cnt = 0
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
    ?assert(is_binary(Binary)).

encode_node_addrs_with_values_test() ->
    Response = #resource_allocation_response{
        job_id = 1,
        node_addrs = [
            {{192, 168, 1, 1}, 6818},
            {{192, 168, 1, 2}, 6818},
            {{192, 168, 1, 3}, 6818}
        ],
        cpus_per_node = [4, 4, 4],
        num_cpu_groups = 3,
        node_cnt = 3
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Additional Coverage Tests
%%%===================================================================

%% Test decode with extra trailing data
decode_with_trailing_data_test_() ->
    [
     {"job info request with trailing data", fun() ->
        Binary = <<1:32/big, 123:32/big, 1000:32/big, "extra trailing data">>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
        ?assertEqual(123, Body#job_info_request.job_id)
      end},
     {"cancel job with trailing data", fun() ->
        Binary = <<100:32/big, 0:32/big, 9:32/big, 0:32/big, "extra">>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(100, Body#cancel_job_request.job_id)
      end}
    ].

%% Test various error code values
error_code_values_test_() ->
    [
     {"batch response with error code 0", fun() ->
        Binary = <<100:32/big, 0:32/big, 0:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
        ?assertEqual(0, Body#batch_job_response.error_code)
      end},
     {"batch response with error code 1", fun() ->
        Binary = <<100:32/big, 0:32/big, 1:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
        ?assertEqual(1, Body#batch_job_response.error_code)
      end},
     {"batch response with large error code", fun() ->
        Binary = <<100:32/big, 0:32/big, 16#FFFF:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
        ?assertEqual(16#FFFF, Body#batch_job_response.error_code)
      end}
    ].

%% Test job_will_run with various partition formats
job_will_run_partition_test_() ->
    [
     {"partition with equals sign", fun() ->
        Binary = <<0:32/big, "partition=batch", 0, "more">>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_WILL_RUN, Binary),
        ?assertEqual(<<"batch">>, Body#job_will_run_request.partition)
      end},
     {"partition with space delimiter", fun() ->
        Binary = <<0:32/big, "partition=compute other=stuff">>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_WILL_RUN, Binary),
        ?assertEqual(<<"compute">>, Body#job_will_run_request.partition)
      end},
     {"partition with newline delimiter", fun() ->
        Binary = <<0:32/big, "partition=gpu\nother=stuff">>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_WILL_RUN, Binary),
        ?assertEqual(<<"gpu">>, Body#job_will_run_request.partition)
      end},
     {"no partition field", fun() ->
        Binary = <<0:32/big, "other=stuff">>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_WILL_RUN, Binary),
        ?assertEqual(<<>>, Body#job_will_run_request.partition)
      end}
    ].

%% Test job info response with various job counts
job_info_response_counts_test_() ->
    [
     {"zero jobs", fun() ->
        Binary = <<1700000000:64/big, 0:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, Binary),
        ?assertEqual(0, Body#job_info_response.job_count),
        ?assertEqual([], Body#job_info_response.jobs)
      end},
     {"multiple jobs count", fun() ->
        Binary = <<1700000000:64/big, 100:32/big, "jobs data">>,
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, Binary),
        ?assertEqual(100, Body#job_info_response.job_count)
      end}
    ].

%% Test signal values
signal_values_test_() ->
    [
     {"SIGTERM (15)", fun() ->
        Binary = <<100:32/big, 0:32/big, 15:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
        ?assertEqual(15, Body#signal_job_request.signal)
      end},
     {"SIGKILL (9)", fun() ->
        Binary = <<100:32/big, 0:32/big, 9:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
        ?assertEqual(9, Body#signal_job_request.signal)
      end},
     {"SIGHUP (1)", fun() ->
        Binary = <<100:32/big, 0:32/big, 1:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
        ?assertEqual(1, Body#signal_job_request.signal)
      end},
     {"Custom signal (100)", fun() ->
        Binary = <<100:32/big, 0:32/big, 100:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
        ?assertEqual(100, Body#signal_job_request.signal)
      end}
    ].

%% Test step_id values
step_id_values_test_() ->
    [
     {"step_id = 0", fun() ->
        Binary = <<100:32/big, 0:32/big, 9:32/big, 0:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(0, Body#cancel_job_request.step_id)
      end},
     {"step_id = 1", fun() ->
        Binary = <<100:32/big, 1:32/big, 9:32/big, 0:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(1, Body#cancel_job_request.step_id)
      end},
     {"step_id = NO_VAL", fun() ->
        Binary = <<100:32/big, ?SLURM_NO_VAL:32/big, 9:32/big, 0:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(?SLURM_NO_VAL, Body#cancel_job_request.step_id)
      end}
    ].

%% Test flags values
flags_values_test_() ->
    [
     {"flags = 0", fun() ->
        Binary = <<100:32/big, 0:32/big, 9:32/big, 0:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(0, Body#cancel_job_request.flags)
      end},
     {"flags = 1", fun() ->
        Binary = <<100:32/big, 0:32/big, 9:32/big, 1:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(1, Body#cancel_job_request.flags)
      end},
     {"flags = 0xFFFF", fun() ->
        Binary = <<100:32/big, 0:32/big, 9:32/big, 16#FFFF:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(16#FFFF, Body#cancel_job_request.flags)
      end}
    ].

%% Test resource allocation response with various node configurations
resource_allocation_node_configs_test_() ->
    [
     {"single node", fun() ->
        Response = #resource_allocation_response{
            job_id = 1,
            node_list = <<"node001">>,
            cpus_per_node = [4],
            num_cpu_groups = 1,
            node_cnt = 1,
            node_addrs = [{{192, 168, 1, 1}, 6818}]
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
        ?assert(is_binary(Binary))
      end},
     {"multiple nodes", fun() ->
        Response = #resource_allocation_response{
            job_id = 2,
            node_list = <<"node[001-004]">>,
            cpus_per_node = [4, 4, 4, 4],
            num_cpu_groups = 4,
            node_cnt = 4,
            node_addrs = [
                {{192, 168, 1, 1}, 6818},
                {{192, 168, 1, 2}, 6818},
                {{192, 168, 1, 3}, 6818},
                {{192, 168, 1, 4}, 6818}
            ]
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
        ?assert(is_binary(Binary))
      end},
     {"heterogeneous cpus", fun() ->
        Response = #resource_allocation_response{
            job_id = 3,
            node_list = <<"node[001-003]">>,
            cpus_per_node = [4, 8, 16],
            num_cpu_groups = 3,
            node_cnt = 3,
            node_addrs = []
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Response),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test encode_single_job_info with various job states
encode_job_states_test_() ->
    [
     {"job state PENDING", fun() ->
        Job = #job_info{job_id = 1, job_state = ?JOB_PENDING},
        Response = #job_info_response{last_update = 1, job_count = 1, jobs = [Job]},
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Response),
        ?assert(is_binary(Binary))
      end},
     {"job state RUNNING", fun() ->
        Job = #job_info{job_id = 2, job_state = ?JOB_RUNNING},
        Response = #job_info_response{last_update = 1, job_count = 1, jobs = [Job]},
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Response),
        ?assert(is_binary(Binary))
      end},
     {"job state COMPLETE", fun() ->
        Job = #job_info{job_id = 3, job_state = ?JOB_COMPLETE},
        Response = #job_info_response{last_update = 1, job_count = 1, jobs = [Job]},
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Response),
        ?assert(is_binary(Binary))
      end},
     {"job state CANCELLED", fun() ->
        Job = #job_info{job_id = 4, job_state = ?JOB_CANCELLED},
        Response = #job_info_response{last_update = 1, job_count = 1, jobs = [Job]},
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Response),
        ?assert(is_binary(Binary))
      end},
     {"job state FAILED", fun() ->
        Job = #job_info{job_id = 5, job_state = ?JOB_FAILED},
        Response = #job_info_response{last_update = 1, job_count = 1, jobs = [Job]},
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Response),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test encode with null/undefined values
encode_null_values_test_() ->
    [
     {"null job_submit_user_msg", fun() ->
        Response = #batch_job_response{
            job_id = 1,
            step_id = 0,
            error_code = 0,
            job_submit_user_msg = null
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Response),
        ?assert(is_binary(Binary))
      end},
     {"undefined sibling", fun() ->
        Request = #kill_job_request{
            job_id = 1,
            sibling = undefined
        },
        {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_KILL_JOB, Request),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test multiple jobs in response
multiple_jobs_test_() ->
    [
     {"two jobs", fun() ->
        Job1 = #job_info{job_id = 1, name = <<"job1">>},
        Job2 = #job_info{job_id = 2, name = <<"job2">>},
        Response = #job_info_response{
            last_update = 1700000000,
            job_count = 2,
            jobs = [Job1, Job2]
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Response),
        ?assert(is_binary(Binary))
      end},
     {"five jobs", fun() ->
        Jobs = [#job_info{job_id = I, name = iolist_to_binary(["job", integer_to_list(I)])} || I <- lists:seq(1, 5)],
        Response = #job_info_response{
            last_update = 1700000000,
            job_count = 5,
            jobs = Jobs
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Response),
        ?assert(is_binary(Binary))
      end}
    ].

%% Test with non-record input for encode_single_job_info
encode_invalid_job_info_test() ->
    Response = #job_info_response{
        last_update = 1,
        job_count = 1,
        jobs = [invalid_job]  % Not a #job_info record
    },
    {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, Response),
    ?assert(is_binary(Binary)).  % Should handle gracefully

%%%===================================================================
%%% Additional Coverage Tests - Job Info Request
%%%===================================================================

job_info_request_extended_test_() ->
    [
     {"decode with full fields", fun() ->
        Binary = <<42:32/big, 12345:32/big, 1000:32/big, "extra">>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
        ?assertEqual(42, Body#job_info_request.show_flags),
        ?assertEqual(12345, Body#job_info_request.job_id),
        ?assertEqual(1000, Body#job_info_request.user_id)
      end},
     {"decode with two fields", fun() ->
        Binary = <<1:32/big, 999:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
        ?assertEqual(1, Body#job_info_request.show_flags),
        ?assertEqual(999, Body#job_info_request.job_id)
      end},
     {"decode with one field", fun() ->
        Binary = <<100:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
        ?assertEqual(100, Body#job_info_request.show_flags)
      end},
     {"decode empty", fun() ->
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, <<>>),
        ?assertMatch(#job_info_request{}, Body)
      end},
     {"encode record", fun() ->
        Req = #job_info_request{show_flags = 7, job_id = 123, user_id = 456},
        {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_JOB_INFO, Req),
        ?assertEqual(<<7:32/big, 123:32/big, 456:32/big>>, Binary)
      end},
     {"encode invalid", fun() ->
        {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_JOB_INFO, invalid),
        ?assertEqual(<<0:32, 0:32, 0:32>>, Binary)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Cancel Job Request
%%%===================================================================

cancel_job_request_extended_test_() ->
    [
     {"decode with full fields", fun() ->
        Binary = <<12345:32/big, 0:32/big, 15:32/big, 1:32/big, "extra">>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(12345, Body#cancel_job_request.job_id),
        ?assertEqual(0, Body#cancel_job_request.step_id),
        ?assertEqual(15, Body#cancel_job_request.signal),
        ?assertEqual(1, Body#cancel_job_request.flags)
      end},
     {"decode with three fields", fun() ->
        Binary = <<999:32/big, 1:32/big, 9:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(999, Body#cancel_job_request.job_id),
        ?assertEqual(1, Body#cancel_job_request.step_id),
        ?assertEqual(9, Body#cancel_job_request.signal)
      end},
     {"decode with two fields uses default signal", fun() ->
        Binary = <<777:32/big, 2:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(777, Body#cancel_job_request.job_id),
        ?assertEqual(9, Body#cancel_job_request.signal)
      end},
     {"decode with one field", fun() ->
        Binary = <<555:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(555, Body#cancel_job_request.job_id)
      end},
     {"decode empty", fun() ->
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, <<>>),
        ?assertMatch(#cancel_job_request{}, Body)
      end},
     {"encode record", fun() ->
        Req = #cancel_job_request{job_id = 123, step_id = 0, signal = 15, flags = 2},
        {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_CANCEL_JOB, Req),
        ?assertEqual(<<123:32/big, 0:32/big, 15:32/big, 2:32/big>>, Binary)
      end},
     {"encode invalid", fun() ->
        {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_CANCEL_JOB, invalid),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Kill Job Request
%%%===================================================================

kill_job_request_extended_test_() ->
    [
     {"decode with sibling", fun() ->
        SiblingStr = <<"sibling_cluster">>,
        SibLen = byte_size(SiblingStr),
        Binary = <<12345:32/big, 0:32/big, 9:32/big, 3:32/big, SibLen:32/big, SiblingStr/binary>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary),
        ?assertEqual(12345, Body#kill_job_request.job_id),
        ?assertEqual(SiblingStr, Body#kill_job_request.sibling)
      end},
     {"decode with four fields", fun() ->
        Binary = <<999:32/big, 1:32/big, 15:32/big, 7:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary),
        ?assertEqual(999, Body#kill_job_request.job_id),
        ?assertEqual(15, Body#kill_job_request.signal),
        ?assertEqual(7, Body#kill_job_request.flags)
      end},
     {"decode with three fields", fun() ->
        Binary = <<777:32/big, 2:32/big, 9:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary),
        ?assertEqual(777, Body#kill_job_request.job_id)
      end},
     {"decode with one field", fun() ->
        Binary = <<555:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, Binary),
        ?assertEqual(555, Body#kill_job_request.job_id)
      end},
     {"decode empty", fun() ->
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_KILL_JOB, <<>>),
        ?assertMatch(#kill_job_request{}, Body)
      end},
     {"encode record", fun() ->
        Req = #kill_job_request{job_id = 123, step_id = 0, signal = 9, flags = 1, sibling = <<"test">>},
        {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_KILL_JOB, Req),
        ?assert(byte_size(Binary) > 16)
      end},
     {"encode invalid", fun() ->
        {ok, Binary} = flurm_codec_job:encode_body(?REQUEST_KILL_JOB, invalid),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Suspend Request
%%%===================================================================

suspend_request_extended_test_() ->
    [
     {"decode with suspend = 1", fun() ->
        Binary = <<12345:32/big, 1:32/big, "extra">>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary),
        ?assertEqual(12345, Body#suspend_request.job_id),
        ?assertEqual(true, Body#suspend_request.suspend)
      end},
     {"decode with suspend = 0", fun() ->
        Binary = <<12345:32/big, 0:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary),
        ?assertEqual(false, Body#suspend_request.suspend)
      end},
     {"decode with one field", fun() ->
        Binary = <<555:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUSPEND, Binary),
        ?assertEqual(555, Body#suspend_request.job_id)
      end},
     {"decode empty", fun() ->
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUSPEND, <<>>),
        ?assertMatch(#suspend_request{}, Body)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Signal Job Request
%%%===================================================================

signal_job_request_extended_test_() ->
    [
     {"decode with full fields", fun() ->
        Binary = <<12345:32/big, 0:32/big, 15:32/big, 1:32/big, "extra">>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
        ?assertEqual(12345, Body#signal_job_request.job_id),
        ?assertEqual(0, Body#signal_job_request.step_id),
        ?assertEqual(15, Body#signal_job_request.signal),
        ?assertEqual(1, Body#signal_job_request.flags)
      end},
     {"decode with three fields", fun() ->
        Binary = <<999:32/big, 1:32/big, 9:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
        ?assertEqual(9, Body#signal_job_request.signal)
      end},
     {"decode with two fields", fun() ->
        Binary = <<777:32/big, 2:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
        ?assertEqual(777, Body#signal_job_request.job_id),
        ?assertEqual(2, Body#signal_job_request.signal)
      end},
     {"decode with one field", fun() ->
        Binary = <<555:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, Binary),
        ?assertEqual(555, Body#signal_job_request.job_id)
      end},
     {"decode empty", fun() ->
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SIGNAL_JOB, <<>>),
        ?assertMatch(#signal_job_request{}, Body)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Batch Job Response
%%%===================================================================

batch_job_response_extended_test_() ->
    [
     {"decode with message", fun() ->
        MsgStr = <<"Job submitted successfully">>,
        MsgLen = byte_size(MsgStr),
        Binary = <<12345:32/big, 0:32/big, 0:32/big, MsgLen:32/big, MsgStr/binary>>,
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
        ?assertEqual(12345, Body#batch_job_response.job_id),
        ?assertEqual(0, Body#batch_job_response.error_code),
        ?assertEqual(MsgStr, Body#batch_job_response.job_submit_user_msg)
      end},
     {"decode with error code", fun() ->
        Binary = <<0:32/big, 0:32/big, 1:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
        ?assertEqual(1, Body#batch_job_response.error_code)
      end},
     {"decode with two fields", fun() ->
        Binary = <<999:32/big, 5:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
        ?assertEqual(999, Body#batch_job_response.job_id),
        ?assertEqual(5, Body#batch_job_response.step_id)
      end},
     {"decode with one field", fun() ->
        Binary = <<777:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
        ?assertEqual(777, Body#batch_job_response.job_id)
      end},
     {"decode empty", fun() ->
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, <<>>),
        ?assertMatch(#batch_job_response{}, Body)
      end},
     {"encode record", fun() ->
        Resp = #batch_job_response{job_id = 123, step_id = 0, error_code = 0, job_submit_user_msg = <<"OK">>},
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Resp),
        ?assert(byte_size(Binary) > 12)
      end},
     {"encode invalid", fun() ->
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_SUBMIT_BATCH_JOB, invalid),
        ?assertEqual(<<0:32, 0:32, 0:32, 0:32>>, Binary)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Job Will Run Response
%%%===================================================================

job_will_run_response_extended_test_() ->
    [
     {"encode full response", fun() ->
        Resp = #job_will_run_response{
            job_id = 12345,
            start_time = 1700000000,
            node_list = <<"node[001-004]">>,
            proc_cnt = 64,
            error_code = 0
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, Resp),
        ?assert(byte_size(Binary) > 20)
      end},
     {"encode error response", fun() ->
        Resp = #job_will_run_response{
            job_id = 0,
            start_time = 0,
            node_list = <<>>,
            proc_cnt = 0,
            error_code = 1
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, Resp),
        ?assert(is_binary(Binary))
      end},
     {"encode invalid", fun() ->
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, invalid),
        ?assertEqual(<<0:32, 0:64, 0:32, 0:32, 0:32>>, Binary)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Job Ready Response
%%%===================================================================

job_ready_response_extended_test_() ->
    [
     {"encode map with return_code", fun() ->
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_READY, #{return_code => 0}),
        ?assertEqual(<<0:32/signed-big>>, Binary)
      end},
     {"encode map with negative return_code", fun() ->
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_READY, #{return_code => -1}),
        ?assertEqual(<<-1:32/signed-big>>, Binary)
      end},
     {"encode invalid", fun() ->
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_READY, invalid),
        ?assertEqual(<<1:32/signed-big>>, Binary)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Resource Allocation Response
%%%===================================================================

resource_allocation_response_extended_test_() ->
    [
     {"encode full response", fun() ->
        Resp = #resource_allocation_response{
            error_code = 0,
            job_id = 12345,
            node_list = <<"node[001-004]">>,
            num_cpu_groups = 1,
            cpus_per_node = [16],
            node_cnt = 4,
            partition = <<"compute">>,
            alias_list = <<>>,
            node_addrs = [{{192,168,1,1}, 6818}, {{192,168,1,2}, 6818}],
            job_submit_user_msg = <<>>
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp),
        ?assert(byte_size(Binary) > 50)
      end},
     {"encode with empty node addrs", fun() ->
        Resp = #resource_allocation_response{
            error_code = 0,
            job_id = 1,
            node_list = <<"node1">>,
            num_cpu_groups = 1,
            cpus_per_node = [4],
            node_cnt = 1,
            partition = <<"default">>,
            alias_list = <<>>,
            node_addrs = [],
            job_submit_user_msg = <<>>
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp),
        ?assert(is_binary(Binary))
      end},
     {"encode for job_allocation_info", fun() ->
        Resp = #resource_allocation_response{
            error_code = 0,
            job_id = 999,
            node_list = <<"compute001">>,
            num_cpu_groups = 1,
            cpus_per_node = [8],
            node_cnt = 1,
            partition = <<"debug">>,
            alias_list = <<>>,
            node_addrs = [],
            job_submit_user_msg = <<>>
        },
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_ALLOCATION_INFO, Resp),
        ?assert(is_binary(Binary))
      end},
     {"encode invalid", fun() ->
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, invalid),
        ?assert(is_binary(Binary))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Unsupported Types
%%%===================================================================

unsupported_job_types_test_() ->
    [
     {"decode unsupported type 0", fun() ->
        ?assertEqual(unsupported, flurm_codec_job:decode_body(0, <<>>))
      end},
     {"decode unsupported type 9999", fun() ->
        ?assertEqual(unsupported, flurm_codec_job:decode_body(9999, <<"data">>))
      end},
     {"encode unsupported type 0", fun() ->
        ?assertEqual(unsupported, flurm_codec_job:encode_body(0, #{}))
      end},
     {"encode unsupported type 9999", fun() ->
        ?assertEqual(unsupported, flurm_codec_job:encode_body(9999, #{}))
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Batch Job Request
%%%===================================================================

batch_job_request_extended_test_() ->
    [
     {"decode small binary", fun() ->
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_SUBMIT_BATCH_JOB, <<"small">>),
        ?assertMatch(#batch_job_request{}, Body)
      end},
     {"encode record returns not implemented", fun() ->
        Req = #batch_job_request{job_id = 1, name = <<"test">>},
        Result = flurm_codec_job:encode_body(?REQUEST_SUBMIT_BATCH_JOB, Req),
        ?assertEqual({error, batch_job_encode_not_implemented}, Result)
      end},
     {"encode invalid", fun() ->
        Result = flurm_codec_job:encode_body(?REQUEST_SUBMIT_BATCH_JOB, invalid),
        ?assertEqual({error, invalid_batch_job_request}, Result)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Job Info Response
%%%===================================================================

job_info_response_extended_test_() ->
    [
     {"decode with two fields", fun() ->
        Binary = <<1700000000:64/big, 5:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, Binary),
        ?assertEqual(1700000000, Body#job_info_response.last_update),
        ?assertEqual(5, Body#job_info_response.job_count)
      end},
     {"decode empty", fun() ->
        {ok, Body} = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, <<>>),
        ?assertMatch(#job_info_response{}, Body)
      end},
     {"encode non-record", fun() ->
        {ok, Binary} = flurm_codec_job:encode_body(?RESPONSE_JOB_INFO, invalid),
        ?assertEqual(<<0:64, 0:32>>, Binary)
      end}
    ].

%%%===================================================================
%%% Additional Coverage Tests - Large Values
%%%===================================================================

large_values_job_test_() ->
    [
     {"large job_id in cancel", fun() ->
        Binary = <<16#FFFFFFFD:32/big, 0:32/big, 9:32/big, 0:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_CANCEL_JOB, Binary),
        ?assertEqual(16#FFFFFFFD, Body#cancel_job_request.job_id)
      end},
     {"large show_flags in job_info", fun() ->
        Binary = <<16#FFFFFFFF:32/big>>,
        {ok, Body} = flurm_codec_job:decode_body(?REQUEST_JOB_INFO, Binary),
        ?assertEqual(16#FFFFFFFF, Body#job_info_request.show_flags)
      end}
    ].
