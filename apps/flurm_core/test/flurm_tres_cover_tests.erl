%%%-------------------------------------------------------------------
%%% @doc FLURM TRES Coverage Tests
%%%
%%% Comprehensive EUnit tests that call real flurm_tres functions
%%% (no mocking) to maximize rebar3 cover results. Exercises every
%%% exported function and targets internal code paths including
%%% billing calculation, format_key/1, parse_key/1, and edge cases.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_tres_cover_tests).

-include_lib("flurm_core/include/flurm_core.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% keys/0 Tests
%%====================================================================

keys_returns_list_test() ->
    Keys = flurm_tres:keys(),
    ?assert(is_list(Keys)),
    ?assert(length(Keys) >= 7).

keys_contains_all_standard_keys_test() ->
    Keys = flurm_tres:keys(),
    ?assert(lists:member(cpu_seconds, Keys)),
    ?assert(lists:member(mem_seconds, Keys)),
    ?assert(lists:member(node_seconds, Keys)),
    ?assert(lists:member(gpu_seconds, Keys)),
    ?assert(lists:member(job_count, Keys)),
    ?assert(lists:member(job_time, Keys)),
    ?assert(lists:member(billing, Keys)).

%%====================================================================
%% zero/0 Tests
%%====================================================================

zero_returns_map_with_all_standard_keys_test() ->
    Z = flurm_tres:zero(),
    ?assert(is_map(Z)),
    lists:foreach(fun(K) ->
        ?assertEqual(0, maps:get(K, Z))
    end, flurm_tres:keys()).

zero_is_empty_test() ->
    ?assert(flurm_tres:is_empty(flurm_tres:zero())).

%%====================================================================
%% add/2 Tests
%%====================================================================

add_two_empty_maps_test() ->
    ?assertEqual(#{}, flurm_tres:add(#{}, #{})).

add_zero_to_zero_test() ->
    Z = flurm_tres:zero(),
    Result = flurm_tres:add(Z, Z),
    ?assert(flurm_tres:is_empty(Result)).

add_disjoint_keys_test() ->
    A = #{cpu_seconds => 10},
    B = #{gpu_seconds => 20},
    Result = flurm_tres:add(A, B),
    ?assertEqual(10, maps:get(cpu_seconds, Result)),
    ?assertEqual(20, maps:get(gpu_seconds, Result)).

add_overlapping_keys_test() ->
    A = #{cpu_seconds => 100, mem_seconds => 200, billing => 50},
    B = #{cpu_seconds => 50, mem_seconds => 100, gpu_seconds => 10},
    Result = flurm_tres:add(A, B),
    ?assertEqual(150, maps:get(cpu_seconds, Result)),
    ?assertEqual(300, maps:get(mem_seconds, Result)),
    ?assertEqual(50, maps:get(billing, Result)),
    ?assertEqual(10, maps:get(gpu_seconds, Result)).

add_large_values_test() ->
    A = #{cpu_seconds => 999999999, mem_seconds => 999999999},
    B = #{cpu_seconds => 1, mem_seconds => 1},
    Result = flurm_tres:add(A, B),
    ?assertEqual(1000000000, maps:get(cpu_seconds, Result)),
    ?assertEqual(1000000000, maps:get(mem_seconds, Result)).

add_from_job_results_test() ->
    T1 = flurm_tres:from_job(100, 4, 1024, 1),
    T2 = flurm_tres:from_job(200, 2, 2048, 1),
    Result = flurm_tres:add(T1, T2),
    ?assertEqual(100*4 + 200*2, maps:get(cpu_seconds, Result)),
    ?assertEqual(100*1024 + 200*2048, maps:get(mem_seconds, Result)),
    ?assertEqual(2, maps:get(job_count, Result)),
    ?assertEqual(300, maps:get(job_time, Result)).

add_custom_atom_keys_test() ->
    A = #{my_custom_tres => 42},
    B = #{my_custom_tres => 8},
    Result = flurm_tres:add(A, B),
    ?assertEqual(50, maps:get(my_custom_tres, Result)).

%%====================================================================
%% subtract/2 Tests
%%====================================================================

subtract_empty_maps_test() ->
    ?assertEqual(#{}, flurm_tres:subtract(#{}, #{})).

subtract_equal_values_test() ->
    M = #{cpu_seconds => 100, mem_seconds => 200},
    Result = flurm_tres:subtract(M, M),
    ?assertEqual(0, maps:get(cpu_seconds, Result)),
    ?assertEqual(0, maps:get(mem_seconds, Result)).

subtract_floors_at_zero_test() ->
    A = #{cpu_seconds => 50},
    B = #{cpu_seconds => 200},
    Result = flurm_tres:subtract(A, B),
    ?assertEqual(0, maps:get(cpu_seconds, Result)).

subtract_key_not_in_first_map_test() ->
    A = #{},
    B = #{cpu_seconds => 100},
    Result = flurm_tres:subtract(A, B),
    ?assertEqual(0, maps:get(cpu_seconds, Result)).

subtract_key_only_in_first_map_test() ->
    A = #{cpu_seconds => 100, gpu_seconds => 50},
    B = #{cpu_seconds => 30},
    Result = flurm_tres:subtract(A, B),
    ?assertEqual(70, maps:get(cpu_seconds, Result)),
    ?assertEqual(50, maps:get(gpu_seconds, Result)).

subtract_large_overflow_floor_test() ->
    A = #{cpu_seconds => 1},
    B = #{cpu_seconds => 999999999999},
    Result = flurm_tres:subtract(A, B),
    ?assertEqual(0, maps:get(cpu_seconds, Result)).

subtract_all_keys_floor_test() ->
    A = flurm_tres:zero(),
    B = #{cpu_seconds => 10, mem_seconds => 20, node_seconds => 30,
          gpu_seconds => 40, job_count => 5, job_time => 60, billing => 70},
    Result = flurm_tres:subtract(A, B),
    lists:foreach(fun(K) ->
        ?assertEqual(0, maps:get(K, Result))
    end, flurm_tres:keys()).

%%====================================================================
%% from_job/1 Tests
%%====================================================================

from_job_map_basic_test() ->
    JobInfo = #{elapsed => 3600, num_cpus => 8, req_mem => 4096,
                num_nodes => 2, tres_alloc => #{gpu => 2}},
    Result = flurm_tres:from_job(JobInfo),
    ?assertEqual(3600 * 8, maps:get(cpu_seconds, Result)),
    ?assertEqual(3600 * 4096, maps:get(mem_seconds, Result)),
    ?assertEqual(3600 * 2, maps:get(node_seconds, Result)),
    ?assertEqual(3600 * 2, maps:get(gpu_seconds, Result)),
    ?assertEqual(1, maps:get(job_count, Result)),
    ?assertEqual(3600, maps:get(job_time, Result)).

from_job_map_empty_test() ->
    %% All defaults: elapsed=0, num_cpus=0, req_mem=0, num_nodes=1, no gpu
    Result = flurm_tres:from_job(#{}),
    ?assertEqual(0, maps:get(cpu_seconds, Result)),
    ?assertEqual(0, maps:get(mem_seconds, Result)),
    ?assertEqual(0, maps:get(node_seconds, Result)),
    ?assertEqual(0, maps:get(gpu_seconds, Result)),
    ?assertEqual(1, maps:get(job_count, Result)),
    ?assertEqual(0, maps:get(job_time, Result)),
    ?assertEqual(0, maps:get(billing, Result)).

from_job_map_no_tres_alloc_test() ->
    %% No tres_alloc key at all - gpu_seconds should be 0
    JobInfo = #{elapsed => 60, num_cpus => 1, req_mem => 512, num_nodes => 1},
    Result = flurm_tres:from_job(JobInfo),
    ?assertEqual(0, maps:get(gpu_seconds, Result)).

from_job_map_no_gpu_in_tres_alloc_test() ->
    %% tres_alloc exists but no gpu key
    JobInfo = #{elapsed => 60, num_cpus => 2, req_mem => 1024,
                num_nodes => 1, tres_alloc => #{fpga => 1}},
    Result = flurm_tres:from_job(JobInfo),
    ?assertEqual(0, maps:get(gpu_seconds, Result)).

from_job_map_zero_elapsed_test() ->
    JobInfo = #{elapsed => 0, num_cpus => 32, req_mem => 65536,
                num_nodes => 4, tres_alloc => #{gpu => 8}},
    Result = flurm_tres:from_job(JobInfo),
    ?assertEqual(0, maps:get(cpu_seconds, Result)),
    ?assertEqual(0, maps:get(mem_seconds, Result)),
    ?assertEqual(0, maps:get(node_seconds, Result)),
    ?assertEqual(0, maps:get(gpu_seconds, Result)),
    ?assertEqual(0, maps:get(billing, Result)).

from_job_map_large_values_test() ->
    JobInfo = #{elapsed => 86400, num_cpus => 1024, req_mem => 1048576,
                num_nodes => 128, tres_alloc => #{gpu => 512}},
    Result = flurm_tres:from_job(JobInfo),
    ?assertEqual(86400 * 1024, maps:get(cpu_seconds, Result)),
    ?assertEqual(86400 * 1048576, maps:get(mem_seconds, Result)),
    ?assertEqual(86400 * 128, maps:get(node_seconds, Result)),
    ?assertEqual(86400 * 512, maps:get(gpu_seconds, Result)).

%%====================================================================
%% from_job/4 Tests
%%====================================================================

from_job_explicit_basic_test() ->
    Result = flurm_tres:from_job(3600, 4, 2048, 2),
    ?assertEqual(14400, maps:get(cpu_seconds, Result)),
    ?assertEqual(3600 * 2048, maps:get(mem_seconds, Result)),
    ?assertEqual(7200, maps:get(node_seconds, Result)),
    ?assertEqual(0, maps:get(gpu_seconds, Result)),
    ?assertEqual(1, maps:get(job_count, Result)),
    ?assertEqual(3600, maps:get(job_time, Result)).

from_job_explicit_zero_elapsed_test() ->
    Result = flurm_tres:from_job(0, 16, 8192, 4),
    ?assertEqual(0, maps:get(cpu_seconds, Result)),
    ?assertEqual(0, maps:get(mem_seconds, Result)),
    ?assertEqual(0, maps:get(node_seconds, Result)),
    ?assertEqual(0, maps:get(billing, Result)).

from_job_explicit_zero_cpus_test() ->
    Result = flurm_tres:from_job(100, 0, 1024, 1),
    ?assertEqual(0, maps:get(cpu_seconds, Result)),
    ?assertEqual(100 * 1024, maps:get(mem_seconds, Result)),
    ?assertEqual(100, maps:get(node_seconds, Result)).

from_job_explicit_single_node_test() ->
    Result = flurm_tres:from_job(60, 1, 512, 1),
    ?assertEqual(60, maps:get(cpu_seconds, Result)),
    ?assertEqual(60 * 512, maps:get(mem_seconds, Result)),
    ?assertEqual(60, maps:get(node_seconds, Result)).

%%====================================================================
%% Billing calculation coverage
%%====================================================================

billing_basic_test() ->
    %% Default billing: CPU*1.0 + (MemMB/1024)*0.25 + GPU*2.0, times elapsed
    %% from_job/4: NumCpus=4, ReqMem=1024, GpuCount=0, Elapsed=100
    %% BillingUnits = 4*1.0 + (1024/1024)*0.25 + 0*2.0 = 4.25
    %% Billing = round(4.25 * 100) = 425
    Result = flurm_tres:from_job(100, 4, 1024, 1),
    ?assertEqual(425, maps:get(billing, Result)).

billing_with_gpu_test() ->
    %% from_job/1 with GPU
    %% NumCpus=2, ReqMem=2048, GpuCount=1, Elapsed=100
    %% BillingUnits = 2*1.0 + (2048/1024)*0.25 + 1*2.0 = 2 + 0.5 + 2.0 = 4.5
    %% Billing = round(4.5 * 100) = 450
    JobInfo = #{elapsed => 100, num_cpus => 2, req_mem => 2048,
                num_nodes => 1, tres_alloc => #{gpu => 1}},
    Result = flurm_tres:from_job(JobInfo),
    ?assertEqual(450, maps:get(billing, Result)).

billing_zero_mem_test() ->
    %% NumCpus=1, ReqMem=0, GpuCount=0, Elapsed=10
    %% BillingUnits = 1*1.0 + 0/1024*0.25 + 0 = 1.0
    %% Billing = round(1.0 * 10) = 10
    Result = flurm_tres:from_job(10, 1, 0, 1),
    ?assertEqual(10, maps:get(billing, Result)).

billing_high_gpu_count_test() ->
    %% NumCpus=1, ReqMem=0, GpuCount=8, Elapsed=1
    %% BillingUnits = 1*1.0 + 0 + 8*2.0 = 17.0
    %% Billing = round(17.0 * 1) = 17
    JobInfo = #{elapsed => 1, num_cpus => 1, req_mem => 0,
                num_nodes => 1, tres_alloc => #{gpu => 8}},
    Result = flurm_tres:from_job(JobInfo),
    ?assertEqual(17, maps:get(billing, Result)).

%%====================================================================
%% format/1 Tests
%%====================================================================

format_empty_map_test() ->
    ?assertEqual(<<>>, flurm_tres:format(#{})).

format_zero_map_test() ->
    %% All zeros should produce empty string (values > 0 filter)
    ?assertEqual(<<>>, flurm_tres:format(flurm_tres:zero())).

format_single_cpu_test() ->
    Result = flurm_tres:format(#{cpu_seconds => 500}),
    ?assertEqual(<<"cpu=500">>, Result).

format_single_mem_test() ->
    Result = flurm_tres:format(#{mem_seconds => 1024}),
    ?assertEqual(<<"mem=1024">>, Result).

format_single_node_test() ->
    Result = flurm_tres:format(#{node_seconds => 7200}),
    ?assertEqual(<<"node=7200">>, Result).

format_single_gpu_test() ->
    Result = flurm_tres:format(#{gpu_seconds => 100}),
    ?assertEqual(<<"gres/gpu=100">>, Result).

format_single_billing_test() ->
    Result = flurm_tres:format(#{billing => 425}),
    ?assertEqual(<<"billing=425">>, Result).

format_skips_job_count_test() ->
    Result = flurm_tres:format(#{job_count => 10}),
    ?assertEqual(<<>>, Result).

format_skips_job_time_test() ->
    Result = flurm_tres:format(#{job_time => 3600}),
    ?assertEqual(<<>>, Result).

format_custom_atom_key_test() ->
    %% Non-standard keys go through atom_to_list fallback
    Result = flurm_tres:format(#{license_matlab => 5}),
    ?assertEqual(<<"license_matlab=5">>, Result).

format_mixed_zero_and_nonzero_test() ->
    Input = #{cpu_seconds => 100, mem_seconds => 0, gpu_seconds => 50,
              job_count => 3, billing => 0},
    Result = flurm_tres:format(Input),
    %% Only cpu and gpu should appear (billing=0 filtered, job_count=skip)
    ?assert(binary:match(Result, <<"cpu=100">>) =/= nomatch),
    ?assert(binary:match(Result, <<"gres/gpu=50">>) =/= nomatch),
    ?assertEqual(nomatch, binary:match(Result, <<"billing">>)),
    ?assertEqual(nomatch, binary:match(Result, <<"job_count">>)).

format_from_job_roundtrip_coverage_test() ->
    %% Generate a TRES map from a job and format it - exercises full path
    T = flurm_tres:from_job(60, 2, 2048, 1),
    Result = flurm_tres:format(T),
    ?assert(is_binary(Result)),
    ?assert(byte_size(Result) > 0),
    %% Should contain cpu and mem at minimum
    ?assert(binary:match(Result, <<"cpu=">>) =/= nomatch),
    ?assert(binary:match(Result, <<"mem=">>) =/= nomatch).

%%====================================================================
%% parse/1 Tests
%%====================================================================

parse_empty_binary_test() ->
    Result = flurm_tres:parse(<<>>),
    ?assert(flurm_tres:is_empty(Result)).

parse_empty_string_returns_zero_map_test() ->
    %% parse(<<>>) returns zero(), so all standard keys should be present
    Result = flurm_tres:parse(<<>>),
    ?assertEqual(0, maps:get(cpu_seconds, Result)),
    ?assertEqual(0, maps:get(billing, Result)).

parse_cpu_key_test() ->
    Result = flurm_tres:parse("cpu=500"),
    ?assertEqual(500, maps:get(cpu_seconds, Result)).

parse_mem_key_test() ->
    Result = flurm_tres:parse("mem=2048"),
    ?assertEqual(2048, maps:get(mem_seconds, Result)).

parse_node_key_test() ->
    Result = flurm_tres:parse("node=100"),
    ?assertEqual(100, maps:get(node_seconds, Result)).

parse_gres_gpu_key_test() ->
    Result = flurm_tres:parse("gres/gpu=4"),
    ?assertEqual(4, maps:get(gpu_seconds, Result)).

parse_gpu_shorthand_test() ->
    Result = flurm_tres:parse("gpu=8"),
    ?assertEqual(8, maps:get(gpu_seconds, Result)).

parse_billing_key_test() ->
    Result = flurm_tres:parse("billing=999"),
    ?assertEqual(999, maps:get(billing, Result)).

parse_job_count_key_test() ->
    Result = flurm_tres:parse("job_count=42"),
    ?assertEqual(42, maps:get(job_count, Result)).

parse_job_time_key_test() ->
    Result = flurm_tres:parse("job_time=3600"),
    ?assertEqual(3600, maps:get(job_time, Result)).

parse_custom_key_test() ->
    %% Unknown keys go through list_to_atom fallback
    Result = flurm_tres:parse("fpga=2"),
    ?assertEqual(2, maps:get(fpga, Result)).

parse_binary_input_test() ->
    Result = flurm_tres:parse(<<"cpu=100,mem=200">>),
    ?assertEqual(100, maps:get(cpu_seconds, Result)),
    ?assertEqual(200, maps:get(mem_seconds, Result)).

parse_string_input_test() ->
    Result = flurm_tres:parse("cpu=100,mem=200"),
    ?assertEqual(100, maps:get(cpu_seconds, Result)),
    ?assertEqual(200, maps:get(mem_seconds, Result)).

parse_multiple_keys_test() ->
    Result = flurm_tres:parse("cpu=100,mem=200,node=50,billing=425"),
    ?assertEqual(100, maps:get(cpu_seconds, Result)),
    ?assertEqual(200, maps:get(mem_seconds, Result)),
    ?assertEqual(50, maps:get(node_seconds, Result)),
    ?assertEqual(425, maps:get(billing, Result)).

parse_with_spaces_test() ->
    %% parse_key and value use string:trim
    Result = flurm_tres:parse(" cpu = 100 , mem = 200 "),
    ?assertEqual(100, maps:get(cpu_seconds, Result)),
    ?assertEqual(200, maps:get(mem_seconds, Result)).

parse_ignores_malformed_parts_test() ->
    %% Parts without "=" should be skipped
    Result = flurm_tres:parse("cpu=100,badpart,mem=200"),
    ?assertEqual(100, maps:get(cpu_seconds, Result)),
    ?assertEqual(200, maps:get(mem_seconds, Result)).

parse_overrides_zero_defaults_test() ->
    %% parse starts from zero(), so non-mentioned keys stay 0
    Result = flurm_tres:parse("cpu=500"),
    ?assertEqual(500, maps:get(cpu_seconds, Result)),
    ?assertEqual(0, maps:get(mem_seconds, Result)),
    ?assertEqual(0, maps:get(gpu_seconds, Result)).

%%====================================================================
%% multiply/2 Tests
%%====================================================================

multiply_by_zero_test() ->
    Input = #{cpu_seconds => 100, mem_seconds => 200},
    Result = flurm_tres:multiply(Input, 0),
    ?assertEqual(0, maps:get(cpu_seconds, Result)),
    ?assertEqual(0, maps:get(mem_seconds, Result)).

multiply_by_one_test() ->
    Input = #{cpu_seconds => 100, mem_seconds => 200},
    Result = flurm_tres:multiply(Input, 1),
    ?assertEqual(100, maps:get(cpu_seconds, Result)),
    ?assertEqual(200, maps:get(mem_seconds, Result)).

multiply_by_integer_test() ->
    Input = #{cpu_seconds => 100, mem_seconds => 50, billing => 25},
    Result = flurm_tres:multiply(Input, 3),
    ?assertEqual(300, maps:get(cpu_seconds, Result)),
    ?assertEqual(150, maps:get(mem_seconds, Result)),
    ?assertEqual(75, maps:get(billing, Result)).

multiply_by_float_test() ->
    Input = #{cpu_seconds => 100},
    Result = flurm_tres:multiply(Input, 0.5),
    ?assertEqual(50, maps:get(cpu_seconds, Result)).

multiply_rounds_test() ->
    Input = #{cpu_seconds => 3},
    Result = flurm_tres:multiply(Input, 0.5),
    %% round(3 * 0.5) = round(1.5) = 2
    ?assertEqual(2, maps:get(cpu_seconds, Result)).

multiply_large_factor_test() ->
    Input = #{cpu_seconds => 1000},
    Result = flurm_tres:multiply(Input, 1000),
    ?assertEqual(1000000, maps:get(cpu_seconds, Result)).

multiply_empty_map_test() ->
    Result = flurm_tres:multiply(#{}, 5),
    ?assertEqual(#{}, Result).

multiply_zero_map_test() ->
    Result = flurm_tres:multiply(flurm_tres:zero(), 100),
    ?assert(flurm_tres:is_empty(Result)).

%%====================================================================
%% is_empty/1 Tests
%%====================================================================

is_empty_truly_empty_test() ->
    ?assert(flurm_tres:is_empty(#{})).

is_empty_zero_map_test() ->
    ?assert(flurm_tres:is_empty(flurm_tres:zero())).

is_empty_single_zero_test() ->
    ?assert(flurm_tres:is_empty(#{cpu_seconds => 0})).

is_empty_multiple_zeros_test() ->
    ?assert(flurm_tres:is_empty(#{cpu_seconds => 0, mem_seconds => 0, gpu_seconds => 0})).

is_empty_one_nonzero_test() ->
    ?assertNot(flurm_tres:is_empty(#{cpu_seconds => 0, mem_seconds => 1})).

is_empty_all_nonzero_test() ->
    ?assertNot(flurm_tres:is_empty(#{cpu_seconds => 1, mem_seconds => 2})).

is_empty_after_subtract_to_zero_test() ->
    A = #{cpu_seconds => 100},
    B = #{cpu_seconds => 100},
    Result = flurm_tres:subtract(A, B),
    ?assert(flurm_tres:is_empty(Result)).

is_empty_after_multiply_by_zero_test() ->
    Input = #{cpu_seconds => 100, mem_seconds => 200},
    Result = flurm_tres:multiply(Input, 0),
    ?assert(flurm_tres:is_empty(Result)).

%%====================================================================
%% exceeds/2 Tests
%%====================================================================

exceeds_nothing_exceeded_test() ->
    Current = #{cpu_seconds => 50, mem_seconds => 100},
    Limits = #{cpu_seconds => 500, mem_seconds => 500},
    ?assertEqual([], flurm_tres:exceeds(Current, Limits)).

exceeds_one_limit_test() ->
    Current = #{cpu_seconds => 600},
    Limits = #{cpu_seconds => 500},
    Result = flurm_tres:exceeds(Current, Limits),
    ?assertEqual([{cpu_seconds, 600, 500}], Result).

exceeds_multiple_limits_test() ->
    Current = #{cpu_seconds => 600, mem_seconds => 1000},
    Limits = #{cpu_seconds => 500, mem_seconds => 800},
    Result = flurm_tres:exceeds(Current, Limits),
    ?assertEqual(2, length(Result)),
    ?assert(lists:member({cpu_seconds, 600, 500}, Result)),
    ?assert(lists:member({mem_seconds, 1000, 800}, Result)).

exceeds_zero_limit_is_unlimited_test() ->
    %% Limit of 0 means unlimited - should never exceed
    Current = #{cpu_seconds => 999999999},
    Limits = #{cpu_seconds => 0},
    ?assertEqual([], flurm_tres:exceeds(Current, Limits)).

exceeds_equal_values_not_exceeded_test() ->
    Current = #{cpu_seconds => 500},
    Limits = #{cpu_seconds => 500},
    ?assertEqual([], flurm_tres:exceeds(Current, Limits)).

exceeds_missing_key_in_current_test() ->
    %% Current does not have the key - defaults to 0 via maps:get/3
    Current = #{},
    Limits = #{cpu_seconds => 500},
    ?assertEqual([], flurm_tres:exceeds(Current, Limits)).

exceeds_empty_limits_test() ->
    Current = #{cpu_seconds => 1000000},
    Limits = #{},
    ?assertEqual([], flurm_tres:exceeds(Current, Limits)).

exceeds_both_empty_test() ->
    ?assertEqual([], flurm_tres:exceeds(#{}, #{})).

exceeds_barely_over_test() ->
    Current = #{cpu_seconds => 501},
    Limits = #{cpu_seconds => 500},
    Result = flurm_tres:exceeds(Current, Limits),
    ?assertEqual([{cpu_seconds, 501, 500}], Result).

%%====================================================================
%% Composition / Integration Tests
%%====================================================================

add_then_subtract_identity_test() ->
    A = #{cpu_seconds => 100, mem_seconds => 200},
    B = #{cpu_seconds => 50, mem_seconds => 100},
    Added = flurm_tres:add(A, B),
    Result = flurm_tres:subtract(Added, B),
    ?assertEqual(100, maps:get(cpu_seconds, Result)),
    ?assertEqual(200, maps:get(mem_seconds, Result)).

multiply_then_add_test() ->
    Base = #{cpu_seconds => 100},
    Doubled = flurm_tres:multiply(Base, 2),
    Result = flurm_tres:add(Base, Doubled),
    ?assertEqual(300, maps:get(cpu_seconds, Result)).

from_job_then_format_then_parse_test() ->
    %% from_job/4 -> format -> parse roundtrip
    %% Note: job_count and job_time are skipped in format, so they
    %% won't roundtrip. cpu, mem, node, billing should.
    T = flurm_tres:from_job(100, 2, 1024, 1),
    Formatted = flurm_tres:format(T),
    Parsed = flurm_tres:parse(Formatted),
    ?assertEqual(maps:get(cpu_seconds, T), maps:get(cpu_seconds, Parsed)),
    ?assertEqual(maps:get(mem_seconds, T), maps:get(mem_seconds, Parsed)),
    ?assertEqual(maps:get(node_seconds, T), maps:get(node_seconds, Parsed)),
    ?assertEqual(maps:get(billing, T), maps:get(billing, Parsed)).

exceeds_after_add_test() ->
    A = #{cpu_seconds => 400},
    B = #{cpu_seconds => 200},
    Combined = flurm_tres:add(A, B),
    Limits = #{cpu_seconds => 500},
    Result = flurm_tres:exceeds(Combined, Limits),
    ?assertEqual([{cpu_seconds, 600, 500}], Result).

subtract_then_is_empty_test() ->
    A = #{cpu_seconds => 100, mem_seconds => 200},
    Result = flurm_tres:subtract(A, #{cpu_seconds => 100, mem_seconds => 200}),
    ?assert(flurm_tres:is_empty(Result)).

from_job_map_and_explicit_same_result_test() ->
    %% Without GPU, from_job/1 and from_job/4 should produce same result
    JobInfo = #{elapsed => 60, num_cpus => 4, req_mem => 2048, num_nodes => 2},
    Result1 = flurm_tres:from_job(JobInfo),
    Result2 = flurm_tres:from_job(60, 4, 2048, 2),
    ?assertEqual(maps:get(cpu_seconds, Result1), maps:get(cpu_seconds, Result2)),
    ?assertEqual(maps:get(mem_seconds, Result1), maps:get(mem_seconds, Result2)),
    ?assertEqual(maps:get(node_seconds, Result1), maps:get(node_seconds, Result2)),
    ?assertEqual(maps:get(gpu_seconds, Result1), maps:get(gpu_seconds, Result2)),
    ?assertEqual(maps:get(billing, Result1), maps:get(billing, Result2)).

%%====================================================================
%% Edge Cases
%%====================================================================

add_with_negative_values_test() ->
    %% Module doesn't enforce non_neg on input, should still add correctly
    A = #{cpu_seconds => 100},
    B = #{cpu_seconds => -50},
    Result = flurm_tres:add(A, B),
    ?assertEqual(50, maps:get(cpu_seconds, Result)).

multiply_by_negative_test() ->
    %% Negative factor - module doesn't guard, should work via round/1
    Input = #{cpu_seconds => 100},
    Result = flurm_tres:multiply(Input, -1),
    ?assertEqual(-100, maps:get(cpu_seconds, Result)).

parse_single_very_large_value_test() ->
    Result = flurm_tres:parse("cpu=999999999"),
    ?assertEqual(999999999, maps:get(cpu_seconds, Result)).

format_very_large_value_test() ->
    Result = flurm_tres:format(#{cpu_seconds => 999999999}),
    ?assertEqual(<<"cpu=999999999">>, Result).
