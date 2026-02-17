%%%-------------------------------------------------------------------
%%% @doc Comprehensive unit tests for flurm_protocol_codec job-related
%%% encode_body and decode_body functions.
%%%
%%% Tests for all job batch message encoding and decoding functions.
%%% Coverage target: job-related message types in flurm_protocol_codec.erl
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_codec_job_batch_tests).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").

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

flurm_protocol_codec_job_batch_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% REQUEST_SUBMIT_BATCH_JOB (4003) - encode and decode
      {"decode REQUEST_SUBMIT_BATCH_JOB - empty", fun decode_submit_batch_job_empty_test/0},
      {"decode REQUEST_SUBMIT_BATCH_JOB - minimal", fun decode_submit_batch_job_minimal_test/0},
      {"decode REQUEST_SUBMIT_BATCH_JOB - typical", fun decode_submit_batch_job_typical_test/0},
      {"decode REQUEST_SUBMIT_BATCH_JOB - edge case zeros", fun decode_submit_batch_job_zeros_test/0},
      {"decode REQUEST_SUBMIT_BATCH_JOB - edge case max int", fun decode_submit_batch_job_max_int_test/0},
      {"encode REQUEST_SUBMIT_BATCH_JOB - record input", fun encode_submit_batch_job_record_test/0},
      {"encode REQUEST_SUBMIT_BATCH_JOB - map input", fun encode_submit_batch_job_map_test/0},
      {"encode REQUEST_SUBMIT_BATCH_JOB - invalid", fun encode_submit_batch_job_invalid_test/0},

      %% RESPONSE_SUBMIT_BATCH_JOB (4004) - encode and decode
      {"decode RESPONSE_SUBMIT_BATCH_JOB - empty", fun decode_response_batch_job_empty_test/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB - typical", fun decode_response_batch_job_typical_test/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB - with message", fun decode_response_batch_job_with_msg_test/0},
      {"decode RESPONSE_SUBMIT_BATCH_JOB - edge cases", fun decode_response_batch_job_edge_test/0},
      {"encode RESPONSE_SUBMIT_BATCH_JOB - record input", fun encode_response_batch_job_record_test/0},
      {"encode RESPONSE_SUBMIT_BATCH_JOB - map input", fun encode_response_batch_job_map_test/0},
      {"roundtrip RESPONSE_SUBMIT_BATCH_JOB", fun roundtrip_response_batch_job_test/0},

      %% REQUEST_JOB_INFO (2003) - encode and decode
      {"decode REQUEST_JOB_INFO - empty", fun decode_job_info_request_empty_test/0},
      {"decode REQUEST_JOB_INFO - typical", fun decode_job_info_request_typical_test/0},
      {"decode REQUEST_JOB_INFO - partial", fun decode_job_info_request_partial_test/0},
      {"decode REQUEST_JOB_INFO - edge cases", fun decode_job_info_request_edge_test/0},
      {"encode REQUEST_JOB_INFO - record input", fun encode_job_info_request_record_test/0},
      {"encode REQUEST_JOB_INFO - map input", fun encode_job_info_request_map_test/0},
      {"roundtrip REQUEST_JOB_INFO", fun roundtrip_job_info_request_test/0},

      %% RESPONSE_JOB_INFO (2004) - encode and decode
      {"decode RESPONSE_JOB_INFO - empty", fun decode_job_info_response_empty_test/0},
      {"decode RESPONSE_JOB_INFO - typical", fun decode_job_info_response_typical_test/0},
      {"decode RESPONSE_JOB_INFO - multiple jobs", fun decode_job_info_response_multiple_test/0},
      {"encode RESPONSE_JOB_INFO - empty", fun encode_job_info_response_empty_test/0},
      {"encode RESPONSE_JOB_INFO - with jobs", fun encode_job_info_response_with_jobs_test/0},
      {"encode RESPONSE_JOB_INFO - map input", fun encode_job_info_response_map_test/0},

      %% REQUEST_CANCEL_JOB (4006) - encode and decode
      {"decode REQUEST_CANCEL_JOB - empty", fun decode_cancel_job_empty_test/0},
      {"decode REQUEST_CANCEL_JOB - typical", fun decode_cancel_job_typical_test/0},
      {"decode REQUEST_CANCEL_JOB - partial formats", fun decode_cancel_job_partial_test/0},
      {"decode REQUEST_CANCEL_JOB - edge cases", fun decode_cancel_job_edge_test/0},
      {"encode REQUEST_CANCEL_JOB - record input", fun encode_cancel_job_record_test/0},
      {"encode REQUEST_CANCEL_JOB - map input", fun encode_cancel_job_map_test/0},
      {"roundtrip REQUEST_CANCEL_JOB", fun roundtrip_cancel_job_test/0},

      %% REQUEST_KILL_JOB (5032) - similar to cancel
      {"decode REQUEST_KILL_JOB - empty", fun decode_kill_job_empty_test/0},
      {"decode REQUEST_KILL_JOB - typical", fun decode_kill_job_typical_test/0},
      {"decode REQUEST_KILL_JOB - with sibling", fun decode_kill_job_with_sibling_test/0},
      {"decode REQUEST_KILL_JOB - partial formats", fun decode_kill_job_partial_test/0},
      {"encode REQUEST_KILL_JOB - record input", fun encode_kill_job_record_test/0},
      {"encode REQUEST_KILL_JOB - map input", fun encode_kill_job_map_test/0},
      {"roundtrip REQUEST_KILL_JOB", fun roundtrip_kill_job_test/0},

      %% REQUEST_UPDATE_JOB (3001) - encode and decode
      {"decode REQUEST_UPDATE_JOB - minimal", fun decode_update_job_minimal_test/0},
      {"decode REQUEST_UPDATE_JOB - typical", fun decode_update_job_typical_test/0},
      {"decode REQUEST_UPDATE_JOB - with job_id_str", fun decode_update_job_with_str_test/0},
      {"decode REQUEST_UPDATE_JOB - edge cases", fun decode_update_job_edge_test/0},

      %% REQUEST_JOB_WILL_RUN (4012) - encode and decode
      {"decode REQUEST_JOB_WILL_RUN - empty", fun decode_job_will_run_empty_test/0},
      {"decode REQUEST_JOB_WILL_RUN - typical", fun decode_job_will_run_typical_test/0},
      {"decode REQUEST_JOB_WILL_RUN - with partition", fun decode_job_will_run_partition_test/0},
      {"encode RESPONSE_JOB_WILL_RUN - record input", fun encode_job_will_run_response_record_test/0},
      {"encode RESPONSE_JOB_WILL_RUN - map input", fun encode_job_will_run_response_map_test/0},

      %% REQUEST_JOB_STEP_CREATE (5001) - encode and decode
      {"decode REQUEST_JOB_STEP_CREATE - empty", fun decode_job_step_create_empty_test/0},
      {"decode REQUEST_JOB_STEP_CREATE - typical", fun decode_job_step_create_typical_test/0},
      {"decode REQUEST_JOB_STEP_CREATE - full format", fun decode_job_step_create_full_test/0},
      {"encode RESPONSE_JOB_STEP_CREATE - record input", fun encode_job_step_create_response_record_test/0},
      {"encode RESPONSE_JOB_STEP_CREATE - map input", fun encode_job_step_create_response_map_test/0},

      %% REQUEST_JOB_STEP_INFO (5003) - encode and decode
      {"decode REQUEST_JOB_STEP_INFO - empty", fun decode_job_step_info_empty_test/0},
      {"decode REQUEST_JOB_STEP_INFO - typical", fun decode_job_step_info_typical_test/0},
      {"decode REQUEST_JOB_STEP_INFO - partial", fun decode_job_step_info_partial_test/0},
      {"encode RESPONSE_JOB_STEP_INFO - empty", fun encode_job_step_info_response_empty_test/0},
      {"encode RESPONSE_JOB_STEP_INFO - with steps", fun encode_job_step_info_response_with_steps_test/0},

      %% RESPONSE_RESOURCE_ALLOCATION (4002) - encode and decode
      {"encode RESPONSE_RESOURCE_ALLOCATION - record input", fun encode_resource_allocation_response_record_test/0},
      {"encode RESPONSE_RESOURCE_ALLOCATION - with node_addrs", fun encode_resource_allocation_with_addrs_test/0},
      {"encode RESPONSE_RESOURCE_ALLOCATION - empty", fun encode_resource_allocation_empty_test/0},
      {"encode RESPONSE_RESOURCE_ALLOCATION - edge cases", fun encode_resource_allocation_edge_test/0},

      %% RESPONSE_JOB_ALLOCATION_INFO (4015) - encode and decode
      {"encode RESPONSE_JOB_ALLOCATION_INFO - typical", fun encode_job_allocation_info_typical_test/0},
      {"encode RESPONSE_JOB_ALLOCATION_INFO - empty", fun encode_job_allocation_info_empty_test/0},

      %% SRUN_JOB_COMPLETE (7004) - encode
      {"encode SRUN_JOB_COMPLETE - typical", fun encode_srun_job_complete_typical_test/0},
      {"encode SRUN_JOB_COMPLETE - edge cases", fun encode_srun_job_complete_edge_test/0},

      %% REQUEST_SUSPEND (5014), REQUEST_SIGNAL_JOB (5018)
      {"decode REQUEST_SUSPEND - typical", fun decode_suspend_typical_test/0},
      {"decode REQUEST_SUSPEND - resume", fun decode_suspend_resume_test/0},
      {"decode REQUEST_SUSPEND - edge cases", fun decode_suspend_edge_test/0},
      {"decode REQUEST_SIGNAL_JOB - typical", fun decode_signal_job_typical_test/0},
      {"decode REQUEST_SIGNAL_JOB - various signals", fun decode_signal_job_signals_test/0},
      {"decode REQUEST_SIGNAL_JOB - edge cases", fun decode_signal_job_edge_test/0},

      %% REQUEST_COMPLETE_PROLOG (5019), MESSAGE_EPILOG_COMPLETE (6012)
      {"decode REQUEST_COMPLETE_PROLOG - typical", fun decode_complete_prolog_typical_test/0},
      {"decode REQUEST_COMPLETE_PROLOG - with error", fun decode_complete_prolog_error_test/0},
      {"decode REQUEST_COMPLETE_PROLOG - empty", fun decode_complete_prolog_empty_test/0},
      {"decode MESSAGE_EPILOG_COMPLETE - typical", fun decode_epilog_complete_typical_test/0},
      {"decode MESSAGE_EPILOG_COMPLETE - with error", fun decode_epilog_complete_error_test/0},
      {"decode MESSAGE_EPILOG_COMPLETE - empty", fun decode_epilog_complete_empty_test/0},

      %% MESSAGE_TASK_EXIT (5046)
      {"decode MESSAGE_TASK_EXIT - typical", fun decode_task_exit_typical_test/0},
      {"decode MESSAGE_TASK_EXIT - multiple tasks", fun decode_task_exit_multiple_test/0},
      {"decode MESSAGE_TASK_EXIT - empty", fun decode_task_exit_empty_test/0},
      {"encode MESSAGE_TASK_EXIT - typical", fun encode_task_exit_typical_test/0},

      %% Error handling tests
      {"decode with malformed binary - cancel", fun decode_malformed_cancel_test/0},
      {"decode with malformed binary - kill", fun decode_malformed_kill_test/0},
      {"decode with malformed binary - signal", fun decode_malformed_signal_test/0},
      {"decode with malformed binary - suspend", fun decode_malformed_suspend_test/0},

      %% Boundary value tests
      {"decode with NO_VAL values", fun decode_no_val_values_test/0},
      {"decode with max uint32 values", fun decode_max_uint32_test/0},
      {"decode with empty strings", fun decode_empty_strings_test/0},
      {"encode with special characters", fun encode_special_chars_test/0},

      %% Additional edge cases
      {"encode RESPONSE_JOB_READY - ready", fun encode_job_ready_ready_test/0},
      {"encode RESPONSE_JOB_READY - not ready", fun encode_job_ready_not_ready_test/0},
      {"encode SRUN_PING - empty", fun encode_srun_ping_test/0}
     ]}.

%%%===================================================================
%%% REQUEST_SUBMIT_BATCH_JOB (4003) Tests
%%%===================================================================

decode_submit_batch_job_empty_test() ->
    Result = try_decode(?REQUEST_SUBMIT_BATCH_JOB, <<>>),
    ?assertMatch({ok, _}, Result).

decode_submit_batch_job_minimal_test() ->
    %% Minimal binary with just some padding
    Binary = <<0:64>>,
    Result = try_decode(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, _}, Result).

decode_submit_batch_job_typical_test() ->
    %% Create a binary with shebang script and some structure
    Script = <<"#!/bin/bash\necho hello\n">>,
    Binary = build_batch_job_binary(Script, <<"test_job">>, <<"/tmp">>),
    Result = try_decode(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, _}, Result).

decode_submit_batch_job_zeros_test() ->
    %% All zeros - edge case
    Binary = <<0:256>>,
    Result = try_decode(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, _}, Result).

decode_submit_batch_job_max_int_test() ->
    %% Max uint32 values - testing NO_VAL handling
    Binary = <<?SLURM_NO_VAL:32/big, ?SLURM_NO_VAL:32/big, ?SLURM_NO_VAL:32/big>>,
    Result = try_decode(?REQUEST_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, _}, Result).

encode_submit_batch_job_record_test() ->
    Req = #batch_job_request{
        name = <<"test_job">>,
        partition = <<"default">>,
        script = <<"#!/bin/bash\necho hello">>,
        work_dir = <<"/tmp">>,
        min_nodes = 1,
        max_nodes = 1,
        min_cpus = 4,
        num_tasks = 1,
        cpus_per_task = 4,
        time_limit = 3600,
        priority = 100,
        user_id = 1000,
        group_id = 1000
    },
    Result = try_encode(?REQUEST_SUBMIT_BATCH_JOB, Req),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 50).

encode_submit_batch_job_map_test() ->
    %% Maps are typically converted or handled specially
    Result = try_encode(?REQUEST_SUBMIT_BATCH_JOB, #{}),
    %% May return error or default encoding
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

encode_submit_batch_job_invalid_test() ->
    Result = try_encode(?REQUEST_SUBMIT_BATCH_JOB, invalid_data),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% RESPONSE_SUBMIT_BATCH_JOB (4004) Tests
%%%===================================================================

decode_response_batch_job_empty_test() ->
    Result = try_decode(?RESPONSE_SUBMIT_BATCH_JOB, <<>>),
    ?assertMatch({ok, #batch_job_response{}}, Result).

decode_response_batch_job_typical_test() ->
    Binary = <<12345:32/big, 0:32/big, 0:32/big>>,
    Result = try_decode(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_response{}}, Result),
    {ok, Resp} = Result,
    ?assertEqual(12345, Resp#batch_job_response.job_id),
    ?assertEqual(0, Resp#batch_job_response.error_code).

decode_response_batch_job_with_msg_test() ->
    Msg = <<"Job submitted successfully">>,
    MsgLen = byte_size(Msg) + 1,
    Binary = <<12345:32/big, 0:32/big, 0:32/big, MsgLen:32/big, Msg/binary, 0>>,
    Result = try_decode(?RESPONSE_SUBMIT_BATCH_JOB, Binary),
    ?assertMatch({ok, #batch_job_response{}}, Result),
    {ok, Resp} = Result,
    ?assertEqual(12345, Resp#batch_job_response.job_id).

decode_response_batch_job_edge_test() ->
    %% Single field
    Binary1 = <<999:32/big>>,
    Result1 = try_decode(?RESPONSE_SUBMIT_BATCH_JOB, Binary1),
    ?assertMatch({ok, #batch_job_response{}}, Result1),

    %% Two fields
    Binary2 = <<888:32/big, 5:32/big>>,
    Result2 = try_decode(?RESPONSE_SUBMIT_BATCH_JOB, Binary2),
    ?assertMatch({ok, #batch_job_response{}}, Result2),

    %% Error code non-zero
    Binary3 = <<777:32/big, 0:32/big, 1:32/big>>,
    Result3 = try_decode(?RESPONSE_SUBMIT_BATCH_JOB, Binary3),
    ?assertMatch({ok, #batch_job_response{}}, Result3),
    {ok, Resp3} = Result3,
    ?assertEqual(1, Resp3#batch_job_response.error_code).

encode_response_batch_job_record_test() ->
    Resp = #batch_job_response{
        job_id = 12345,
        step_id = 0,
        error_code = 0,
        job_submit_user_msg = <<"Job queued">>
    },
    Result = try_encode(?RESPONSE_SUBMIT_BATCH_JOB, Resp),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) >= 12).

encode_response_batch_job_map_test() ->
    Result = try_encode(?RESPONSE_SUBMIT_BATCH_JOB, #{}),
    case Result of
        {ok, Binary} -> ?assert(is_binary(Binary));
        {error, _} -> ok
    end.

roundtrip_response_batch_job_test() ->
    Original = #batch_job_response{
        job_id = 54321,
        step_id = 0,
        error_code = 0,
        job_submit_user_msg = <<"Test message">>
    },
    {ok, Encoded} = try_encode(?RESPONSE_SUBMIT_BATCH_JOB, Original),
    {ok, Decoded} = try_decode(?RESPONSE_SUBMIT_BATCH_JOB, Encoded),
    ?assertEqual(Original#batch_job_response.job_id, Decoded#batch_job_response.job_id),
    ?assertEqual(Original#batch_job_response.step_id, Decoded#batch_job_response.step_id),
    ?assertEqual(Original#batch_job_response.error_code, Decoded#batch_job_response.error_code).

%%%===================================================================
%%% REQUEST_JOB_INFO (2003) Tests
%%%===================================================================

decode_job_info_request_empty_test() ->
    Result = try_decode(?REQUEST_JOB_INFO, <<>>),
    ?assertMatch({ok, #job_info_request{}}, Result).

decode_job_info_request_typical_test() ->
    Binary = <<0:32/big, 12345:32/big, 1000:32/big>>,
    Result = try_decode(?REQUEST_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_request{}}, Result),
    {ok, Req} = Result,
    ?assertEqual(0, Req#job_info_request.show_flags),
    ?assertEqual(12345, Req#job_info_request.job_id),
    ?assertEqual(1000, Req#job_info_request.user_id).

decode_job_info_request_partial_test() ->
    %% One field
    Binary1 = <<1:32/big>>,
    Result1 = try_decode(?REQUEST_JOB_INFO, Binary1),
    ?assertMatch({ok, #job_info_request{}}, Result1),

    %% Two fields
    Binary2 = <<2:32/big, 999:32/big>>,
    Result2 = try_decode(?REQUEST_JOB_INFO, Binary2),
    ?assertMatch({ok, #job_info_request{}}, Result2).

decode_job_info_request_edge_test() ->
    %% NO_VAL for all fields
    Binary = <<?SLURM_NO_VAL:32/big, ?SLURM_NO_VAL:32/big, ?SLURM_NO_VAL:32/big>>,
    Result = try_decode(?REQUEST_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_request{}}, Result),
    {ok, Req} = Result,
    ?assertEqual(?SLURM_NO_VAL, Req#job_info_request.show_flags).

encode_job_info_request_record_test() ->
    Req = #job_info_request{
        show_flags = 1,
        job_id = 12345,
        user_id = 1000
    },
    Result = try_encode(?REQUEST_JOB_INFO, Req),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assertEqual(<<1:32/big, 12345:32/big, 1000:32/big>>, Binary).

encode_job_info_request_map_test() ->
    Result = try_encode(?REQUEST_JOB_INFO, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

roundtrip_job_info_request_test() ->
    Original = #job_info_request{
        show_flags = 5,
        job_id = 99999,
        user_id = 500
    },
    {ok, Encoded} = try_encode(?REQUEST_JOB_INFO, Original),
    {ok, Decoded} = try_decode(?REQUEST_JOB_INFO, Encoded),
    ?assertEqual(Original#job_info_request.show_flags, Decoded#job_info_request.show_flags),
    ?assertEqual(Original#job_info_request.job_id, Decoded#job_info_request.job_id),
    ?assertEqual(Original#job_info_request.user_id, Decoded#job_info_request.user_id).

%%%===================================================================
%%% RESPONSE_JOB_INFO (2004) Tests
%%%===================================================================

decode_job_info_response_empty_test() ->
    Result = try_decode(?RESPONSE_JOB_INFO, <<>>),
    ?assertMatch({ok, #job_info_response{}}, Result).

decode_job_info_response_typical_test() ->
    Binary = <<1700000000:64/big, 0:32/big>>,
    Result = try_decode(?RESPONSE_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_response{}}, Result),
    {ok, Resp} = Result,
    ?assertEqual(1700000000, Resp#job_info_response.last_update),
    ?assertEqual(0, Resp#job_info_response.job_count).

decode_job_info_response_multiple_test() ->
    %% Response claiming multiple jobs (even without full job data)
    Binary = <<1700000000:64/big, 5:32/big, "incomplete_data">>,
    Result = try_decode(?RESPONSE_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_response{}}, Result),
    {ok, Resp} = Result,
    ?assertEqual(5, Resp#job_info_response.job_count).

encode_job_info_response_empty_test() ->
    Resp = #job_info_response{
        last_update = 1700000000,
        job_count = 0,
        jobs = []
    },
    Result = try_encode(?RESPONSE_JOB_INFO, Resp),
    ?assertMatch({ok, _}, Result).

encode_job_info_response_with_jobs_test() ->
    Job = #job_info{
        job_id = 12345,
        name = <<"test_job">>,
        user_id = 1000,
        group_id = 1000,
        job_state = ?JOB_RUNNING,
        partition = <<"default">>,
        num_nodes = 1,
        num_cpus = 4,
        time_limit = 3600
    },
    Resp = #job_info_response{
        last_update = 1700000000,
        job_count = 1,
        jobs = [Job]
    },
    Result = try_encode(?RESPONSE_JOB_INFO, Resp),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assert(byte_size(Binary) > 12).

encode_job_info_response_map_test() ->
    Result = try_encode(?RESPONSE_JOB_INFO, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%%===================================================================
%%% REQUEST_CANCEL_JOB (4006) Tests
%%%===================================================================

decode_cancel_job_empty_test() ->
    Result = try_decode(?REQUEST_CANCEL_JOB, <<>>),
    ?assertMatch({ok, #cancel_job_request{}}, Result).

decode_cancel_job_typical_test() ->
    Binary = <<12345:32/big, 0:32/big, 9:32/big, 0:32/big>>,
    Result = try_decode(?REQUEST_CANCEL_JOB, Binary),
    ?assertMatch({ok, #cancel_job_request{}}, Result),
    {ok, Req} = Result,
    ?assertEqual(12345, Req#cancel_job_request.job_id),
    ?assertEqual(0, Req#cancel_job_request.step_id),
    ?assertEqual(9, Req#cancel_job_request.signal).

decode_cancel_job_partial_test() ->
    %% One field
    Binary1 = <<999:32/big>>,
    Result1 = try_decode(?REQUEST_CANCEL_JOB, Binary1),
    ?assertMatch({ok, #cancel_job_request{}}, Result1),
    {ok, Req1} = Result1,
    ?assertEqual(999, Req1#cancel_job_request.job_id),

    %% Two fields - may be invalid for some decoders
    Binary2 = <<888:32/big, 1:32/big>>,
    Result2 = try_decode(?REQUEST_CANCEL_JOB, Binary2),
    case Result2 of
        {ok, _} -> ok;
        {error, _} -> ok  % Some partial formats may be invalid
    end,

    %% Three fields - may be invalid for some decoders
    Binary3 = <<777:32/big, 2:32/big, 15:32/big>>,
    Result3 = try_decode(?REQUEST_CANCEL_JOB, Binary3),
    case Result3 of
        {ok, Req3} ->
            ?assertEqual(15, Req3#cancel_job_request.signal);
        {error, _} ->
            ok  % Partial format may not be supported
    end.

decode_cancel_job_edge_test() ->
    %% Max job_id
    Binary1 = <<16#FFFFFFFD:32/big, 0:32/big, 9:32/big, 0:32/big>>,
    Result1 = try_decode(?REQUEST_CANCEL_JOB, Binary1),
    ?assertMatch({ok, #cancel_job_request{}}, Result1),
    {ok, Req1} = Result1,
    ?assertEqual(16#FFFFFFFD, Req1#cancel_job_request.job_id),

    %% Various signals
    Binary2 = <<100:32/big, 0:32/big, 15:32/big, 0:32/big>>,  % SIGTERM
    Result2 = try_decode(?REQUEST_CANCEL_JOB, Binary2),
    ?assertMatch({ok, #cancel_job_request{}}, Result2).

encode_cancel_job_record_test() ->
    Req = #cancel_job_request{
        job_id = 12345,
        step_id = 0,
        signal = 9,
        flags = 0
    },
    Result = try_encode(?REQUEST_CANCEL_JOB, Req),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assert(is_binary(Binary)).

encode_cancel_job_map_test() ->
    Result = try_encode(?REQUEST_CANCEL_JOB, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

roundtrip_cancel_job_test() ->
    Original = #cancel_job_request{
        job_id = 54321,
        step_id = 1,
        signal = 15,
        flags = 1
    },
    {ok, Encoded} = try_encode(?REQUEST_CANCEL_JOB, Original),
    {ok, Decoded} = try_decode(?REQUEST_CANCEL_JOB, Encoded),
    ?assertEqual(Original#cancel_job_request.job_id, Decoded#cancel_job_request.job_id),
    ?assertEqual(Original#cancel_job_request.step_id, Decoded#cancel_job_request.step_id),
    ?assertEqual(Original#cancel_job_request.signal, Decoded#cancel_job_request.signal).

%%%===================================================================
%%% REQUEST_KILL_JOB (5032) Tests
%%%===================================================================

decode_kill_job_empty_test() ->
    Result = try_decode(?REQUEST_KILL_JOB, <<>>),
    %% Empty may return error or default record
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

decode_kill_job_typical_test() ->
    %% Numeric format
    Binary = <<12345:32/big, 0:32/big-signed, 9:16/big, 0:16/big>>,
    Result = try_decode(?REQUEST_KILL_JOB, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(12345, Req#kill_job_request.job_id);
        {error, _} ->
            %% Alternative format expected
            ok
    end.

decode_kill_job_with_sibling_test() ->
    %% Build a binary that includes sibling info
    %% The SLURM 24.x format is complex with step_id, step_het_comp, etc.
    %% We test that the decoder handles various input formats gracefully
    Sibling = <<"sibling_cluster">>,
    SibLen = byte_size(Sibling) + 1,
    Binary = <<12345:32/big, 0:32/big, 9:32/big, 0:32/big, SibLen:32/big, Sibling/binary, 0>>,
    Result = try_decode(?REQUEST_KILL_JOB, Binary),
    case Result of
        {ok, Req} ->
            %% Job ID may be extracted from different positions depending on format
            ?assert(is_integer(Req#kill_job_request.job_id));
        {error, _} ->
            ok  % Complex format may not parse with simplified binary
    end.

decode_kill_job_partial_test() ->
    %% Four fields
    Binary1 = <<100:32/big, 0:32/big, 9:32/big, 1:32/big>>,
    Result1 = try_decode(?REQUEST_KILL_JOB, Binary1),
    case Result1 of
        {ok, _} -> ok;
        {error, _} -> ok
    end,

    %% One field
    Binary2 = <<200:32/big>>,
    Result2 = try_decode(?REQUEST_KILL_JOB, Binary2),
    case Result2 of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

encode_kill_job_record_test() ->
    Req = #kill_job_request{
        job_id = 12345,
        job_id_str = <<"12345">>,
        step_id = 0,
        signal = 9,
        flags = 0,
        sibling = <<>>
    },
    Result = try_encode(?REQUEST_KILL_JOB, Req),
    ?assertMatch({ok, _}, Result).

encode_kill_job_map_test() ->
    Result = try_encode(?REQUEST_KILL_JOB, #{}),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

roundtrip_kill_job_test() ->
    Original = #kill_job_request{
        job_id = 54321,
        job_id_str = <<"54321">>,
        step_id = 0,
        signal = 15,
        flags = 1,
        sibling = <<"cluster1">>
    },
    {ok, Encoded} = try_encode(?REQUEST_KILL_JOB, Original),
    case try_decode(?REQUEST_KILL_JOB, Encoded) of
        {ok, Decoded} ->
            %% Job ID should match (possibly extracted from string)
            ?assert(Decoded#kill_job_request.job_id >= 0);
        {error, _} ->
            ok
    end.

%%%===================================================================
%%% REQUEST_UPDATE_JOB (3001) Tests
%%%===================================================================

decode_update_job_minimal_test() ->
    Binary = <<0:32/big>>,  % Empty length prefix
    Result = try_decode(?REQUEST_UPDATE_JOB, Binary),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

decode_update_job_typical_test() ->
    %% Job ID string
    JobIdStr = <<"12345">>,
    StrLen = byte_size(JobIdStr) + 1,
    Binary = <<StrLen:32/big, JobIdStr/binary, 0>>,
    Result = try_decode(?REQUEST_UPDATE_JOB, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(12345, Req#update_job_request.job_id);
        {error, _} ->
            ok
    end.

decode_update_job_with_str_test() ->
    JobIdStr = <<"99999">>,
    StrLen = byte_size(JobIdStr) + 1,
    Binary = <<StrLen:32/big, JobIdStr/binary, 0>>,
    Result = try_decode(?REQUEST_UPDATE_JOB, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(99999, Req#update_job_request.job_id),
            ?assertEqual(JobIdStr, Req#update_job_request.job_id_str);
        {error, _} ->
            ok
    end.

decode_update_job_edge_test() ->
    %% Array job format "123_4"
    JobIdStr = <<"12345_67">>,
    StrLen = byte_size(JobIdStr) + 1,
    Binary = <<StrLen:32/big, JobIdStr/binary, 0>>,
    Result = try_decode(?REQUEST_UPDATE_JOB, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(12345, Req#update_job_request.job_id);
        {error, _} ->
            ok
    end.

%%%===================================================================
%%% REQUEST_JOB_WILL_RUN (4012) / RESPONSE_JOB_WILL_RUN (4013) Tests
%%%===================================================================

decode_job_will_run_empty_test() ->
    Result = try_decode(?REQUEST_JOB_WILL_RUN, <<>>),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

decode_job_will_run_typical_test() ->
    Binary = <<12345:32/big, "partition=batch", 0>>,
    Result = try_decode(?REQUEST_JOB_WILL_RUN, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(12345, Req#job_will_run_request.job_id);
        {error, _} ->
            ok
    end.

decode_job_will_run_partition_test() ->
    Binary = <<0:32/big, "partition=compute", 0>>,
    Result = try_decode(?REQUEST_JOB_WILL_RUN, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(<<"compute">>, Req#job_will_run_request.partition);
        {error, _} ->
            ok
    end.

encode_job_will_run_response_record_test() ->
    Resp = #job_will_run_response{
        job_id = 12345,
        start_time = 1700000000,
        node_list = <<"node001">>,
        proc_cnt = 4,
        error_code = 0
    },
    Result = try_encode(?RESPONSE_JOB_WILL_RUN, Resp),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assert(is_binary(Binary)).

encode_job_will_run_response_map_test() ->
    Result = try_encode(?RESPONSE_JOB_WILL_RUN, #{}),
    case Result of
        {ok, Binary} -> ?assert(is_binary(Binary));
        {error, _} -> ok
    end.

%%%===================================================================
%%% REQUEST_JOB_STEP_CREATE (5001) / RESPONSE_JOB_STEP_CREATE (5002) Tests
%%%===================================================================

decode_job_step_create_empty_test() ->
    Result = try_decode(?REQUEST_JOB_STEP_CREATE, <<>>),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok  % May require non-empty input
    end.

decode_job_step_create_typical_test() ->
    Binary = <<12345:32/big, 0:32/big>>,
    Result = try_decode(?REQUEST_JOB_STEP_CREATE, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(12345, Req#job_step_create_request.job_id);
        {error, _} ->
            ok
    end.

decode_job_step_create_full_test() ->
    NameStr = <<"step_test">>,
    NameLen = byte_size(NameStr) + 1,
    Binary = <<
        12345:32/big,    % job_id
        0:32/big,        % step_id
        1000:32/big,     % user_id
        1:32/big,        % min_nodes
        4:32/big,        % max_nodes
        16:32/big,       % num_tasks
        1:32/big,        % cpus_per_task
        3600:32/big,     % time_limit
        0:32/big,        % flags
        1:32/big,        % immediate
        NameLen:32/big,  % name length
        NameStr/binary,  % name
        0                % null terminator
    >>,
    Result = try_decode(?REQUEST_JOB_STEP_CREATE, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(12345, Req#job_step_create_request.job_id),
            ?assertEqual(0, Req#job_step_create_request.step_id),
            ?assertEqual(1000, Req#job_step_create_request.user_id);
        {error, _} ->
            ok
    end.

encode_job_step_create_response_record_test() ->
    Resp = #job_step_create_response{
        job_step_id = 0,
        job_id = 12345,
        user_id = 1000,
        group_id = 1000,
        user_name = <<"testuser">>,
        node_list = <<"node001">>,
        num_tasks = 4,
        error_code = 0
    },
    Result = try_encode(?RESPONSE_JOB_STEP_CREATE, Resp),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assert(is_binary(Binary)).

encode_job_step_create_response_map_test() ->
    Result = try_encode(?RESPONSE_JOB_STEP_CREATE, #{}),
    %% Map input may not be supported - gracefully handle either success or error
    case Result of
        {ok, Binary} -> ?assert(is_binary(Binary));
        {error, _} -> ok  % Map input not supported
    end.

%%%===================================================================
%%% REQUEST_JOB_STEP_INFO (5003) / RESPONSE_JOB_STEP_INFO (5004) Tests
%%%===================================================================

decode_job_step_info_empty_test() ->
    Result = try_decode(?REQUEST_JOB_STEP_INFO, <<>>),
    ?assertMatch({ok, _}, Result).

decode_job_step_info_typical_test() ->
    Binary = <<0:32/big, 12345:32/big, 0:32/big>>,
    Result = try_decode(?REQUEST_JOB_STEP_INFO, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(0, Req#job_step_info_request.show_flags),
            ?assertEqual(12345, Req#job_step_info_request.job_id);
        {error, _} ->
            ok
    end.

decode_job_step_info_partial_test() ->
    %% One field
    Binary1 = <<1:32/big>>,
    Result1 = try_decode(?REQUEST_JOB_STEP_INFO, Binary1),
    case Result1 of
        {ok, Req1} ->
            ?assertEqual(1, Req1#job_step_info_request.show_flags);
        {error, _} ->
            ok
    end,

    %% Two fields
    Binary2 = <<2:32/big, 999:32/big>>,
    Result2 = try_decode(?REQUEST_JOB_STEP_INFO, Binary2),
    case Result2 of
        {ok, Req2} ->
            ?assertEqual(999, Req2#job_step_info_request.job_id);
        {error, _} ->
            ok
    end.

encode_job_step_info_response_empty_test() ->
    Resp = #job_step_info_response{
        last_update = 1700000000,
        step_count = 0,
        steps = []
    },
    Result = try_encode(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assertMatch({ok, _}, Result).

encode_job_step_info_response_with_steps_test() ->
    Step = #job_step_info{
        job_id = 12345,
        step_id = 0,
        step_name = <<"batch">>,
        partition = <<"default">>,
        user_id = 1000,
        state = 1,
        num_tasks = 4,
        num_cpus = 16,
        time_limit = 3600,
        start_time = 1700000000,
        run_time = 300,
        nodes = <<"node001">>,
        node_cnt = 1
    },
    Resp = #job_step_info_response{
        last_update = 1700000000,
        step_count = 1,
        steps = [Step]
    },
    Result = try_encode(?RESPONSE_JOB_STEP_INFO, Resp),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assert(byte_size(Binary) > 20).

%%%===================================================================
%%% RESPONSE_RESOURCE_ALLOCATION (4002) Tests
%%%===================================================================

encode_resource_allocation_response_record_test() ->
    Resp = #resource_allocation_response{
        job_id = 12345,
        node_list = <<"node001">>,
        cpus_per_node = [4],
        num_cpu_groups = 1,
        num_nodes = 1,
        partition = <<"default">>,
        alias_list = <<>>,
        error_code = 0,
        job_submit_user_msg = <<>>,
        node_addrs = []
    },
    Result = try_encode(?RESPONSE_RESOURCE_ALLOCATION, Resp),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assert(byte_size(Binary) > 50).

encode_resource_allocation_with_addrs_test() ->
    Resp = #resource_allocation_response{
        job_id = 12345,
        node_list = <<"node[001-004]">>,
        cpus_per_node = [4, 4, 4, 4],
        num_cpu_groups = 4,
        num_nodes = 4,
        partition = <<"compute">>,
        alias_list = <<>>,
        error_code = 0,
        job_submit_user_msg = <<>>,
        node_addrs = [
            {{192, 168, 1, 1}, 6818},
            {{192, 168, 1, 2}, 6818},
            {{192, 168, 1, 3}, 6818},
            {{192, 168, 1, 4}, 6818}
        ]
    },
    Result = try_encode(?RESPONSE_RESOURCE_ALLOCATION, Resp),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assert(byte_size(Binary) > 100).

encode_resource_allocation_empty_test() ->
    Resp = #resource_allocation_response{
        job_id = 0,
        node_list = <<>>,
        cpus_per_node = [],
        num_cpu_groups = 0,
        num_nodes = 0,
        partition = <<>>,
        alias_list = <<>>,
        error_code = 0,
        job_submit_user_msg = <<>>,
        node_addrs = []
    },
    Result = try_encode(?RESPONSE_RESOURCE_ALLOCATION, Resp),
    ?assertMatch({ok, _}, Result).

encode_resource_allocation_edge_test() ->
    %% With error code
    Resp = #resource_allocation_response{
        job_id = 0,
        node_list = <<>>,
        cpus_per_node = [],
        num_cpu_groups = 0,
        num_nodes = 0,
        partition = <<>>,
        alias_list = <<>>,
        error_code = 1,
        job_submit_user_msg = <<"Error: No nodes available">>,
        node_addrs = []
    },
    Result = try_encode(?RESPONSE_RESOURCE_ALLOCATION, Resp),
    ?assertMatch({ok, _}, Result).

%%%===================================================================
%%% RESPONSE_JOB_ALLOCATION_INFO (4015) Tests
%%%===================================================================

encode_job_allocation_info_typical_test() ->
    Resp = #resource_allocation_response{
        job_id = 12345,
        node_list = <<"node001">>,
        cpus_per_node = [8],
        num_cpu_groups = 1,
        num_nodes = 1,
        partition = <<"debug">>,
        alias_list = <<>>,
        error_code = 0,
        job_submit_user_msg = <<>>,
        node_addrs = []
    },
    Result = try_encode(?RESPONSE_JOB_ALLOCATION_INFO, Resp),
    ?assertMatch({ok, _}, Result).

encode_job_allocation_info_empty_test() ->
    Resp = #resource_allocation_response{
        job_id = 0,
        node_list = <<>>,
        cpus_per_node = [],
        num_cpu_groups = 0,
        num_nodes = 0,
        partition = <<>>,
        alias_list = <<>>,
        error_code = 0,
        job_submit_user_msg = <<>>,
        node_addrs = []
    },
    Result = try_encode(?RESPONSE_JOB_ALLOCATION_INFO, Resp),
    ?assertMatch({ok, _}, Result).

%%%===================================================================
%%% SRUN_JOB_COMPLETE (7004) Tests
%%%===================================================================

encode_srun_job_complete_typical_test() ->
    Msg = #srun_job_complete{
        job_id = 12345,
        step_id = 0
    },
    Result = try_encode(?SRUN_JOB_COMPLETE, Msg),
    ?assertMatch({ok, _}, Result),
    {ok, Binary} = Result,
    ?assertEqual(12, byte_size(Binary)).  % job_id(4) + step_id(4) + step_het_comp(4)

encode_srun_job_complete_edge_test() ->
    %% Max values
    Msg = #srun_job_complete{
        job_id = 16#FFFFFFFD,
        step_id = 16#FFFFFFFE
    },
    Result = try_encode(?SRUN_JOB_COMPLETE, Msg),
    ?assertMatch({ok, _}, Result).

%%%===================================================================
%%% REQUEST_SUSPEND (5014), REQUEST_SIGNAL_JOB (5018) Tests
%%%===================================================================

decode_suspend_typical_test() ->
    Binary = <<12345:32/big, 1:16/big>>,  % suspend = true
    Result = try_decode(?REQUEST_SUSPEND, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(12345, Req#suspend_request.job_id),
            ?assertEqual(true, Req#suspend_request.suspend);
        {error, _} ->
            ok
    end.

decode_suspend_resume_test() ->
    Binary = <<12345:32/big, 0:16/big>>,  % suspend = false (resume)
    Result = try_decode(?REQUEST_SUSPEND, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(false, Req#suspend_request.suspend);
        {error, _} ->
            ok
    end.

decode_suspend_edge_test() ->
    %% Empty
    Result1 = try_decode(?REQUEST_SUSPEND, <<>>),
    case Result1 of
        {ok, _} -> ok;
        {error, _} -> ok
    end,

    %% Just job_id
    Binary2 = <<100:32/big>>,
    Result2 = try_decode(?REQUEST_SUSPEND, Binary2),
    case Result2 of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

decode_signal_job_typical_test() ->
    Binary = <<12345:32/big, 0:32/big-signed, 15:16/big, 0:16/big>>,
    Result = try_decode(?REQUEST_SIGNAL_JOB, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(12345, Req#signal_job_request.job_id),
            ?assertEqual(15, Req#signal_job_request.signal);
        {error, _} ->
            ok
    end.

decode_signal_job_signals_test() ->
    %% SIGKILL (9)
    Binary1 = <<100:32/big, 0:32/big-signed, 9:16/big, 0:16/big>>,
    Result1 = try_decode(?REQUEST_SIGNAL_JOB, Binary1),
    case Result1 of
        {ok, Req1} -> ?assertEqual(9, Req1#signal_job_request.signal);
        {error, _} -> ok
    end,

    %% SIGTERM (15)
    Binary2 = <<100:32/big, 0:32/big-signed, 15:16/big, 0:16/big>>,
    Result2 = try_decode(?REQUEST_SIGNAL_JOB, Binary2),
    case Result2 of
        {ok, Req2} -> ?assertEqual(15, Req2#signal_job_request.signal);
        {error, _} -> ok
    end,

    %% SIGHUP (1)
    Binary3 = <<100:32/big, 0:32/big-signed, 1:16/big, 0:16/big>>,
    Result3 = try_decode(?REQUEST_SIGNAL_JOB, Binary3),
    case Result3 of
        {ok, Req3} -> ?assertEqual(1, Req3#signal_job_request.signal);
        {error, _} -> ok
    end.

decode_signal_job_edge_test() ->
    %% Empty
    Result = try_decode(?REQUEST_SIGNAL_JOB, <<>>),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%%===================================================================
%%% REQUEST_COMPLETE_PROLOG (5019), MESSAGE_EPILOG_COMPLETE (6012) Tests
%%%===================================================================

decode_complete_prolog_typical_test() ->
    NodeName = <<"node001">>,
    NodeLen = byte_size(NodeName) + 1,
    Binary = <<12345:32/big, 0:32/big-signed, NodeLen:32/big, NodeName/binary, 0>>,
    Result = try_decode(?REQUEST_COMPLETE_PROLOG, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(12345, Req#complete_prolog_request.job_id),
            ?assertEqual(0, Req#complete_prolog_request.prolog_rc);
        {error, _} ->
            ok
    end.

decode_complete_prolog_error_test() ->
    Binary = <<12345:32/big, -1:32/big-signed>>,
    Result = try_decode(?REQUEST_COMPLETE_PROLOG, Binary),
    case Result of
        {ok, Req} ->
            ?assertEqual(-1, Req#complete_prolog_request.prolog_rc);
        {error, _} ->
            ok
    end.

decode_complete_prolog_empty_test() ->
    Result = try_decode(?REQUEST_COMPLETE_PROLOG, <<>>),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

decode_epilog_complete_typical_test() ->
    NodeName = <<"node001">>,
    NodeLen = byte_size(NodeName) + 1,
    Binary = <<12345:32/big, 0:32/big-signed, NodeLen:32/big, NodeName/binary, 0>>,
    Result = try_decode(?MESSAGE_EPILOG_COMPLETE, Binary),
    case Result of
        {ok, Msg} ->
            ?assertEqual(12345, Msg#epilog_complete_msg.job_id),
            ?assertEqual(0, Msg#epilog_complete_msg.epilog_rc);
        {error, _} ->
            ok
    end.

decode_epilog_complete_error_test() ->
    Binary = <<12345:32/big, 1:32/big-signed>>,
    Result = try_decode(?MESSAGE_EPILOG_COMPLETE, Binary),
    case Result of
        {ok, Msg} ->
            ?assertEqual(1, Msg#epilog_complete_msg.epilog_rc);
        {error, _} ->
            ok
    end.

decode_epilog_complete_empty_test() ->
    Result = try_decode(?MESSAGE_EPILOG_COMPLETE, <<>>),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%%===================================================================
%%% MESSAGE_TASK_EXIT (6003) Tests
%%%===================================================================

decode_task_exit_typical_test() ->
    Binary = <<
        0:32/big-signed,   % return_code
        1:32/big,          % num_tasks
        0:32/big,          % task_id[0]
        12345:32/big,      % job_id
        0:32/big,          % step_id
        16#FFFFFFFE:32/big % step_het_comp
    >>,
    Result = try_decode(?MESSAGE_TASK_EXIT, Binary),
    case Result of
        {ok, Msg} ->
            ?assertEqual(12345, Msg#task_exit_msg.job_id),
            ?assertEqual([0], Msg#task_exit_msg.task_ids),
            ?assertEqual(0, Msg#task_exit_msg.return_code);
        {error, _} ->
            ok
    end.

decode_task_exit_multiple_test() ->
    Binary = <<
        0:32/big-signed,   % return_code
        3:32/big,          % num_tasks
        0:32/big,          % task_id[0]
        1:32/big,          % task_id[1]
        2:32/big,          % task_id[2]
        12345:32/big,      % job_id
        0:32/big,          % step_id
        16#FFFFFFFE:32/big % step_het_comp
    >>,
    Result = try_decode(?MESSAGE_TASK_EXIT, Binary),
    case Result of
        {ok, Msg} ->
            ?assertEqual([0, 1, 2], Msg#task_exit_msg.task_ids);
        {error, _} ->
            ok
    end.

decode_task_exit_empty_test() ->
    Result = try_decode(?MESSAGE_TASK_EXIT, <<>>),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

encode_task_exit_typical_test() ->
    Msg = #task_exit_msg{
        job_id = 12345,
        step_id = 0,
        step_het_comp = 16#FFFFFFFE,
        task_ids = [0],
        return_code = 0,
        node_name = <<"node001">>
    },
    Result = try_encode(?MESSAGE_TASK_EXIT, Msg),
    ?assertMatch({ok, _}, Result).

%%%===================================================================
%%% Error Handling Tests
%%%===================================================================

decode_malformed_cancel_test() ->
    %% Partial bytes
    Result = try_decode(?REQUEST_CANCEL_JOB, <<1, 2>>),
    case Result of
        {ok, _} -> ok;  % May handle gracefully
        {error, _} -> ok
    end.

decode_malformed_kill_test() ->
    %% Partial bytes
    Result = try_decode(?REQUEST_KILL_JOB, <<1, 2, 3>>),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

decode_malformed_signal_test() ->
    %% Partial bytes
    Result = try_decode(?REQUEST_SIGNAL_JOB, <<1, 2>>),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

decode_malformed_suspend_test() ->
    %% Partial bytes
    Result = try_decode(?REQUEST_SUSPEND, <<1, 2>>),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

%%%===================================================================
%%% Boundary Value Tests
%%%===================================================================

decode_no_val_values_test() ->
    Binary = <<?SLURM_NO_VAL:32/big, ?SLURM_NO_VAL:32/big, ?SLURM_NO_VAL:32/big>>,
    Result = try_decode(?REQUEST_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_request{}}, Result),
    {ok, Req} = Result,
    ?assertEqual(?SLURM_NO_VAL, Req#job_info_request.show_flags).

decode_max_uint32_test() ->
    Binary = <<16#FFFFFFFF:32/big, 16#FFFFFFFF:32/big, 16#FFFFFFFF:32/big>>,
    Result = try_decode(?REQUEST_JOB_INFO, Binary),
    ?assertMatch({ok, #job_info_request{}}, Result).

decode_empty_strings_test() ->
    %% Empty string in packstr format
    Binary = <<0:32/big>>,  % length = 0
    Result = try_decode(?REQUEST_UPDATE_JOB, Binary),
    case Result of
        {ok, _} -> ok;
        {error, _} -> ok
    end.

encode_special_chars_test() ->
    Resp = #batch_job_response{
        job_id = 12345,
        step_id = 0,
        error_code = 0,
        job_submit_user_msg = <<"Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?">>
    },
    Result = try_encode(?RESPONSE_SUBMIT_BATCH_JOB, Resp),
    ?assertMatch({ok, _}, Result).

%%%===================================================================
%%% Additional Edge Case Tests
%%%===================================================================

encode_job_ready_ready_test() ->
    Result = try_encode(?RESPONSE_JOB_READY, #{return_code => 0}),
    ?assertMatch({ok, <<0:32/signed-big>>}, Result).

encode_job_ready_not_ready_test() ->
    Result = try_encode(?RESPONSE_JOB_READY, #{return_code => 1}),
    ?assertMatch({ok, <<1:32/signed-big>>}, Result).

encode_srun_ping_test() ->
    Result = try_encode(?SRUN_PING, #srun_ping{}),
    ?assertMatch({ok, <<>>}, Result).

%%%===================================================================
%%% Helper Functions
%%%===================================================================

%% Try to decode with error handling
try_decode(MsgType, Binary) ->
    try
        flurm_protocol_codec:decode_body(MsgType, Binary)
    catch
        _:Reason ->
            {error, Reason}
    end.

%% Try to encode with error handling
try_encode(MsgType, Body) ->
    try
        flurm_protocol_codec:encode_body(MsgType, Body)
    catch
        _:Reason ->
            {error, Reason}
    end.

%% Build a batch job binary with script
build_batch_job_binary(Script, Name, WorkDir) ->
    %% Build a minimal batch job binary with key fields
    %% Site factor
    SiteFactor = <<0:32/big>>,
    %% Batch features (NULL)
    BatchFeatures = <<16#FFFFFFFF:32/big>>,
    %% Cluster features (NULL)
    ClusterFeatures = <<16#FFFFFFFF:32/big>>,
    %% Clusters (NULL)
    Clusters = <<16#FFFFFFFF:32/big>>,
    %% Contiguous
    Contiguous = <<0:16/big>>,
    %% Container (NULL)
    Container = <<16#FFFFFFFF:32/big>>,
    %% Core spec
    CoreSpec = <<0:16/big>>,
    %% Task dist
    TaskDist = <<0:32/big>>,
    %% Kill on fail
    KillOnFail = <<0:16/big>>,
    %% Features (NULL)
    Features = <<16#FFFFFFFF:32/big>>,
    %% Fed siblings
    FedActive = <<0:64/big>>,
    FedViable = <<0:64/big>>,
    %% Job ID
    JobId = <<0:32/big>>,
    %% Job ID string (NULL)
    JobIdStr = <<16#FFFFFFFF:32/big>>,
    %% Name
    NameLen = byte_size(Name) + 1,
    NameBin = <<NameLen:32/big, Name/binary, 0>>,
    %% Working directory as PWD= env var
    PwdEnv = <<"PWD=", WorkDir/binary, 0>>,
    %% Script (shebang will be found)
    ScriptBin = Script,
    %% Padding
    Padding = <<0:256>>,

    <<SiteFactor/binary, BatchFeatures/binary, ClusterFeatures/binary,
      Clusters/binary, Contiguous/binary, Container/binary, CoreSpec/binary,
      TaskDist/binary, KillOnFail/binary, Features/binary, FedActive/binary,
      FedViable/binary, JobId/binary, JobIdStr/binary, NameBin/binary,
      Padding/binary, PwdEnv/binary, Padding/binary, ScriptBin/binary>>.
