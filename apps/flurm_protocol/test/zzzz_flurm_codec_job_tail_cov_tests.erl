%%%-------------------------------------------------------------------
%%% @doc Tail coverage tests for flurm_codec_job edge branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_codec_job_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

codec_job_tail_cov_test_() ->
    [
     {"decode batch request non-binary error branch",
      fun decode_batch_request_non_binary_error_test/0},
     {"extract job resources short-binary defaults",
      fun extract_job_resources_short_binary_defaults_test/0},
     {"extract job resources catch defaults",
      fun extract_job_resources_catch_defaults_test/0},
     {"extract_uint32 NO_VAL variants and fallback",
      fun extract_uint32_no_val_variants_test/0},
     {"decode kill request 4-field branch",
      fun decode_kill_four_field_branch_test/0},
     {"decode kill request sibling unpack fallback",
      fun decode_kill_unpack_fallback_branch_test/0},
     {"decode update request NO_VAL + empty id string",
      fun decode_update_no_val_empty_id_test/0},
     {"decode update request NO_VAL + parsed id string",
      fun decode_update_no_val_parse_id_test/0},
     {"decode update request NO_VAL + invalid id string",
      fun decode_update_no_val_invalid_id_test/0},
     {"decode update request non-binary catch branch",
      fun decode_update_non_binary_catch_branch_test/0},
     {"decode job_will_run non-binary catch branch",
      fun decode_job_will_run_non_binary_catch_test/0},
     {"decode batch response 3-field branch",
      fun decode_batch_response_three_fields_branch_test/0},
     {"decode job_info response 12-byte branch",
      fun decode_job_info_response_twelve_byte_branch_test/0},
     {"decode job_info response fallback branch",
      fun decode_job_info_response_fallback_branch_test/0},
     {"decode job_info list zero-count branch",
      fun decode_job_info_zero_count_list_branch_test/0},
     {"decode batch response null-string branch",
      fun decode_batch_response_null_string_branch_test/0},
     {"encode resource allocation invalid addr branch",
      fun encode_resource_allocation_invalid_addr_branch_test/0},
     {"encode ensure_binary fallback branch",
      fun ensure_binary_fallback_branch_test/0},
     {"partition parsing without delimiter tail branch",
      fun partition_parse_no_delimiter_tail_branch_test/0},
     {"strip_null null-terminator branch",
      fun strip_null_terminator_branch_test/0},
     {"strip_null non-binary branch",
      fun strip_null_non_binary_branch_test/0},
     {"parse_job_id empty binary branch",
      fun parse_job_id_empty_branch_test/0},
     {"parse_job_id non-binary branch",
      fun parse_job_id_non_binary_branch_test/0}
    ].

decode_batch_request_non_binary_error_test() ->
    Result = flurm_codec_job:decode_batch_job_request(not_a_binary),
    ?assertEqual({error, invalid_batch_job_request}, Result).

extract_job_resources_short_binary_defaults_test() ->
    ?assertEqual({1, 1, 1, 1, 0, 0, 0},
                 flurm_codec_job:extract_job_resources(<<0:96>>)).

extract_job_resources_catch_defaults_test() ->
    ?assertEqual({1, 1, 1, 1, 0, 0, 0},
                 flurm_codec_job:extract_job_resources(not_a_binary)).

extract_uint32_no_val_variants_test() ->
    BinNoVal1 = <<16#FFFFFFFE:32/big>>,
    BinNoVal2 = <<16#FFFFFFFF:32/big>>,
    ?assertEqual(7, flurm_codec_job:extract_uint32_at(BinNoVal1, 0, 7)),
    ?assertEqual(9, flurm_codec_job:extract_uint32_at(BinNoVal2, 0, 9)),
    ?assertEqual(11, flurm_codec_job:extract_uint32_at(<<1, 2, 3>>, 4, 11)).

decode_kill_four_field_branch_test() ->
    Bin = <<11:32/big, 22:32/big, 33:32/big, 44:32/big>>,
    {ok, Req} = flurm_codec_job:decode_kill_job_request(Bin),
    ?assertEqual(11, Req#kill_job_request.job_id),
    ?assertEqual(22, Req#kill_job_request.step_id),
    ?assertEqual(33, Req#kill_job_request.signal),
    ?assertEqual(44, Req#kill_job_request.flags).

decode_kill_unpack_fallback_branch_test() ->
    %% Declared string length (5) exceeds remaining bytes (2), forcing fallback path.
    Bin = <<11:32/big, 22:32/big, 33:32/big, 44:32/big, 5:32/big, 1, 2>>,
    {ok, Req} = flurm_codec_job:decode_kill_job_request(Bin),
    ?assertEqual(<<>>, Req#kill_job_request.sibling).

decode_update_no_val_empty_id_test() ->
    Bin = build_update_binary(16#FFFFFFFE, <<>>, <<"ab">>),
    {ok, Req} = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Bin),
    ?assertEqual(0, Req#update_job_request.job_id),
    ?assertEqual(?SLURM_NO_VAL, Req#update_job_request.priority),
    ?assertEqual(?SLURM_NO_VAL, Req#update_job_request.time_limit).

decode_update_no_val_parse_id_test() ->
    Bin = build_update_binary(16#FFFFFFFE, <<"123_9">>, <<"ab">>),
    {ok, Req} = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Bin),
    ?assertEqual(123, Req#update_job_request.job_id),
    ?assertEqual(?SLURM_NO_VAL, Req#update_job_request.priority),
    ?assertEqual(?SLURM_NO_VAL, Req#update_job_request.time_limit).

decode_update_no_val_invalid_id_test() ->
    Bin = build_update_binary(16#FFFFFFFE, <<"abc">>, <<"ab">>),
    {ok, Req} = flurm_codec_job:decode_body(?REQUEST_UPDATE_JOB, Bin),
    ?assertEqual(0, Req#update_job_request.job_id).

decode_update_non_binary_catch_branch_test() ->
    Result = flurm_codec_job:decode_update_job_request(not_a_binary),
    ?assertMatch({error, {update_job_decode_failed, _}}, Result).

decode_job_will_run_non_binary_catch_test() ->
    Result = flurm_codec_job:decode_body(?REQUEST_JOB_WILL_RUN, not_a_binary),
    ?assertMatch({error, {job_will_run_decode_failed, _}}, Result).

decode_batch_response_three_fields_branch_test() ->
    Bin = <<7:32/big, 8:32/big, 9:32/big>>,
    {ok, Resp} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Bin),
    ?assertEqual(7, Resp#batch_job_response.job_id),
    ?assertEqual(8, Resp#batch_job_response.step_id),
    ?assertEqual(9, Resp#batch_job_response.error_code).

decode_job_info_response_twelve_byte_branch_test() ->
    Bin = <<111:64/big, 3:32/big>>,
    {ok, Resp} = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, Bin),
    ?assertEqual(111, Resp#job_info_response.last_update),
    ?assertEqual(3, Resp#job_info_response.job_count).

decode_job_info_response_fallback_branch_test() ->
    {ok, Resp} = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, <<1, 2, 3, 4, 5>>),
    ?assertEqual(0, Resp#job_info_response.job_count).

decode_job_info_zero_count_list_branch_test() ->
    Bin = <<123:64/big, 0:32/big, 1, 2, 3>>,
    {ok, Resp} = flurm_codec_job:decode_body(?RESPONSE_JOB_INFO, Bin),
    ?assertEqual(123, Resp#job_info_response.last_update),
    ?assertEqual(0, Resp#job_info_response.job_count),
    ?assertEqual([], Resp#job_info_response.jobs).

decode_batch_response_null_string_branch_test() ->
    Bin = <<1:32/big, 2:32/big, 3:32/big, 16#FFFFFFFF:32, "tail">>,
    {ok, Resp} = flurm_codec_job:decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Bin),
    ?assertEqual(<<>>, Resp#batch_job_response.job_submit_user_msg).

encode_resource_allocation_invalid_addr_branch_test() ->
    Resp = #resource_allocation_response{
        error_code = 0,
        job_id = 1,
        node_list = <<"n1">>,
        num_cpu_groups = 1,
        cpus_per_node = [1],
        node_cnt = 1,
        partition = <<"p">>,
        alias_list = <<>>,
        node_addrs = [invalid_addr],
        job_submit_user_msg = <<>>
    },
    {ok, Bin} = flurm_codec_job:encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp),
    ?assert(is_binary(Bin)),
    %% Encoded default sockaddr for invalid addresses.
    ?assertNotEqual(nomatch, binary:match(Bin, <<0:16, 0:16, 0:32, 0:64>>)).

ensure_binary_fallback_branch_test() ->
    Resp = #job_will_run_response{
        job_id = 10,
        start_time = 0,
        node_list = 42,
        proc_cnt = 0,
        error_code = 0
    },
    {ok, Bin} = flurm_codec_job:encode_body(?RESPONSE_JOB_WILL_RUN, Resp),
    ?assert(is_binary(Bin)).

partition_parse_no_delimiter_tail_branch_test() ->
    Bin = <<123:32/big, "partition=alpha">>,
    {ok, Req} = flurm_codec_job:decode_body(?REQUEST_JOB_WILL_RUN, Bin),
    ?assertEqual(<<"alpha">>, Req#job_will_run_request.partition).

strip_null_terminator_branch_test() ->
    ?assertEqual(<<"abc">>, flurm_codec_job:strip_null(<<"abc", 0, "tail">>)).

strip_null_non_binary_branch_test() ->
    ?assertEqual(<<>>, flurm_codec_job:strip_null(42)).

parse_job_id_empty_branch_test() ->
    ?assertEqual(0, flurm_codec_job:parse_job_id(<<>>)).

parse_job_id_non_binary_branch_test() ->
    ?assertEqual(0, flurm_codec_job:parse_job_id(42)).

build_update_binary(PackedJobId, JobIdStr, Name) ->
    <<0:32/big,                 % site_factor
      16#FFFFFFFF:32/big,       % batch_features
      16#FFFFFFFF:32/big,       % cluster_features
      16#FFFFFFFF:32/big,       % clusters
      0:16/big,                 % contiguous
      16#FFFFFFFF:32/big,       % container
      0:16/big,                 % core_spec
      1:32/big,                 % task_dist
      1:16/big,                 % kill_on_fail
      16#FFFFFFFF:32/big,       % features
      0:64/big,                 % fed_active
      0:64/big,                 % fed_viable
      PackedJobId:32/big,
      (pack_str(JobIdStr))/binary,
      (pack_str(Name))/binary>>.

pack_str(Bin) when is_binary(Bin) ->
    <<(byte_size(Bin)):32/big, Bin/binary>>.
