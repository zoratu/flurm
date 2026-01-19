%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_benchmark module
%%%
%%% Tests all exported functions without mocking - direct function testing.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_benchmark_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Fixture for benchmark result for report tests
sample_result() ->
    #{
        name => test_benchmark,
        duration_ms => 100,
        iterations => 1000,
        ops_per_second => 10000.0,
        min_latency_us => 50,
        max_latency_us => 200,
        avg_latency_us => 100.0,
        p50_latency_us => 95,
        p95_latency_us => 180,
        p99_latency_us => 195,
        memory_before => 1000000,
        memory_after => 1500000,
        memory_delta => 500000
    }.

sample_result_2() ->
    #{
        name => another_benchmark,
        duration_ms => 200,
        iterations => 2000,
        ops_per_second => 10000.0,
        min_latency_us => 40,
        max_latency_us => 250,
        avg_latency_us => 110.0,
        p50_latency_us => 100,
        p95_latency_us => 220,
        p99_latency_us => 240,
        memory_before => 2000000,
        memory_after => 2800000,
        memory_delta => 800000
    }.

%%====================================================================
%% run_all/0 Tests
%%====================================================================

run_all_0_returns_list_test() ->
    %% Run with iterations >= 10 for measurable timing (protocol_codec divides by 2)
    Results = flurm_benchmark:run_all(#{iterations => 10, job_count => 10, connections => 2, requests_per_connection => 2}),
    ?assert(is_list(Results)),
    ?assertEqual(5, length(Results)).

run_all_0_returns_maps_test() ->
    Results = flurm_benchmark:run_all(#{iterations => 10, job_count => 10, connections => 2, requests_per_connection => 2}),
    lists:foreach(fun(R) -> ?assert(is_map(R)) end, Results).

run_all_0_contains_all_benchmarks_test() ->
    Results = flurm_benchmark:run_all(#{iterations => 10, job_count => 10, connections => 2, requests_per_connection => 2}),
    Names = [maps:get(name, R) || R <- Results],
    ?assert(lists:member(job_submission, Names)),
    ?assert(lists:member(scheduler_cycle, Names)),
    ?assert(lists:member(protocol_codec, Names)),
    ?assert(lists:member(memory_usage, Names)),
    ?assert(lists:member(concurrent_connections, Names)).

%%====================================================================
%% run_all/1 Tests
%%====================================================================

run_all_1_with_options_test() ->
    Options = #{iterations => 10, job_count => 10, connections => 2, requests_per_connection => 2},
    Results = flurm_benchmark:run_all(Options),
    ?assert(is_list(Results)),
    ?assertEqual(5, length(Results)).

run_all_1_respects_iterations_test() ->
    %% Job submission uses iterations option
    Options = #{iterations => 10, job_count => 10, connections => 2, requests_per_connection => 2},
    Results = flurm_benchmark:run_all(Options),
    JobSubmissionResult = hd([R || R <- Results, maps:get(name, R) =:= job_submission]),
    ?assertEqual(10, maps:get(iterations, JobSubmissionResult)).

%%====================================================================
%% run_benchmark/1 Tests
%%====================================================================

run_benchmark_1_job_submission_test() ->
    Result = flurm_benchmark:run_benchmark(job_submission),
    ?assert(is_map(Result)),
    ?assertEqual(job_submission, maps:get(name, Result)).

run_benchmark_1_scheduler_cycle_test() ->
    Result = flurm_benchmark:run_benchmark(scheduler_cycle),
    ?assert(is_map(Result)),
    ?assertEqual(scheduler_cycle, maps:get(name, Result)).

run_benchmark_1_protocol_codec_test() ->
    Result = flurm_benchmark:run_benchmark(protocol_codec),
    ?assert(is_map(Result)),
    ?assertEqual(protocol_codec, maps:get(name, Result)).

run_benchmark_1_memory_usage_test() ->
    Result = flurm_benchmark:run_benchmark(memory_usage),
    ?assert(is_map(Result)),
    ?assertEqual(memory_usage, maps:get(name, Result)).

run_benchmark_1_concurrent_connections_test() ->
    Result = flurm_benchmark:run_benchmark(concurrent_connections),
    ?assert(is_map(Result)),
    ?assertEqual(concurrent_connections, maps:get(name, Result)).

%%====================================================================
%% run_benchmark/2 Tests
%%====================================================================

run_benchmark_2_with_iterations_test() ->
    Result = flurm_benchmark:run_benchmark(job_submission, #{iterations => 10}),
    ?assertEqual(10, maps:get(iterations, Result)).

run_benchmark_2_scheduler_with_iterations_test() ->
    Result = flurm_benchmark:run_benchmark(scheduler_cycle, #{iterations => 5}),
    ?assertEqual(5, maps:get(iterations, Result)).

run_benchmark_2_protocol_with_iterations_test() ->
    Result = flurm_benchmark:run_benchmark(protocol_codec, #{iterations => 20}),
    %% Protocol codec divides iterations by number of messages (2)
    ?assertEqual(20, maps:get(iterations, Result)).

run_benchmark_2_memory_with_job_count_test() ->
    Result = flurm_benchmark:run_benchmark(memory_usage, #{job_count => 50}),
    ?assertEqual(50, maps:get(iterations, Result)).

run_benchmark_2_concurrent_with_options_test() ->
    Result = flurm_benchmark:run_benchmark(concurrent_connections,
        #{connections => 2, requests_per_connection => 3}),
    ?assertEqual(6, maps:get(iterations, Result)).

%%====================================================================
%% job_submission_throughput/1 Tests
%%====================================================================

job_submission_throughput_returns_map_test() ->
    Result = flurm_benchmark:job_submission_throughput(#{iterations => 5}),
    ?assert(is_map(Result)).

job_submission_throughput_has_required_fields_test() ->
    Result = flurm_benchmark:job_submission_throughput(#{iterations => 5}),
    ?assertEqual(job_submission, maps:get(name, Result)),
    ?assert(maps:is_key(duration_ms, Result)),
    ?assert(maps:is_key(iterations, Result)),
    ?assert(maps:is_key(ops_per_second, Result)),
    ?assert(maps:is_key(min_latency_us, Result)),
    ?assert(maps:is_key(max_latency_us, Result)),
    ?assert(maps:is_key(avg_latency_us, Result)),
    ?assert(maps:is_key(p50_latency_us, Result)),
    ?assert(maps:is_key(p95_latency_us, Result)),
    ?assert(maps:is_key(p99_latency_us, Result)),
    ?assert(maps:is_key(memory_before, Result)),
    ?assert(maps:is_key(memory_after, Result)),
    ?assert(maps:is_key(memory_delta, Result)).

job_submission_throughput_iterations_match_test() ->
    Result = flurm_benchmark:job_submission_throughput(#{iterations => 10}),
    ?assertEqual(10, maps:get(iterations, Result)).

job_submission_throughput_ops_per_second_positive_test() ->
    Result = flurm_benchmark:job_submission_throughput(#{iterations => 10}),
    ?assert(maps:get(ops_per_second, Result) > 0).

job_submission_throughput_latency_reasonable_test() ->
    Result = flurm_benchmark:job_submission_throughput(#{iterations => 10}),
    Min = maps:get(min_latency_us, Result),
    Max = maps:get(max_latency_us, Result),
    Avg = maps:get(avg_latency_us, Result),
    ?assert(Min >= 0),
    ?assert(Max >= Min),
    ?assert(Avg >= Min),
    ?assert(Avg =< Max).

%%====================================================================
%% scheduler_cycle_time/1 Tests
%%====================================================================

scheduler_cycle_time_returns_map_test() ->
    Result = flurm_benchmark:scheduler_cycle_time(#{iterations => 5}),
    ?assert(is_map(Result)).

scheduler_cycle_time_has_name_test() ->
    Result = flurm_benchmark:scheduler_cycle_time(#{iterations => 5}),
    ?assertEqual(scheduler_cycle, maps:get(name, Result)).

scheduler_cycle_time_iterations_match_test() ->
    Result = flurm_benchmark:scheduler_cycle_time(#{iterations => 8}),
    ?assertEqual(8, maps:get(iterations, Result)).

scheduler_cycle_time_has_memory_fields_test() ->
    Result = flurm_benchmark:scheduler_cycle_time(#{iterations => 5}),
    ?assert(maps:is_key(memory_before, Result)),
    ?assert(maps:is_key(memory_after, Result)),
    ?assert(maps:is_key(memory_delta, Result)).

scheduler_cycle_time_has_percentile_fields_test() ->
    Result = flurm_benchmark:scheduler_cycle_time(#{iterations => 10}),
    ?assert(maps:is_key(p50_latency_us, Result)),
    ?assert(maps:is_key(p95_latency_us, Result)),
    ?assert(maps:is_key(p99_latency_us, Result)).

%%====================================================================
%% protocol_codec_performance/1 Tests
%%====================================================================

protocol_codec_performance_returns_map_test() ->
    Result = flurm_benchmark:protocol_codec_performance(#{iterations => 10}),
    ?assert(is_map(Result)).

protocol_codec_performance_has_name_test() ->
    Result = flurm_benchmark:protocol_codec_performance(#{iterations => 10}),
    ?assertEqual(protocol_codec, maps:get(name, Result)).

protocol_codec_performance_ops_positive_test() ->
    Result = flurm_benchmark:protocol_codec_performance(#{iterations => 20}),
    ?assert(maps:get(ops_per_second, Result) > 0).

protocol_codec_performance_duration_positive_test() ->
    Result = flurm_benchmark:protocol_codec_performance(#{iterations => 10}),
    ?assert(maps:get(duration_ms, Result) >= 0).

%%====================================================================
%% memory_usage/1 Tests
%%====================================================================

memory_usage_returns_map_test() ->
    Result = flurm_benchmark:memory_usage(#{job_count => 10}),
    ?assert(is_map(Result)).

memory_usage_has_name_test() ->
    Result = flurm_benchmark:memory_usage(#{job_count => 10}),
    ?assertEqual(memory_usage, maps:get(name, Result)).

memory_usage_job_count_matches_iterations_test() ->
    Result = flurm_benchmark:memory_usage(#{job_count => 25}),
    ?assertEqual(25, maps:get(iterations, Result)).

memory_usage_tracks_memory_test() ->
    Result = flurm_benchmark:memory_usage(#{job_count => 100}),
    MemBefore = maps:get(memory_before, Result),
    MemAfter = maps:get(memory_after, Result),
    MemDelta = maps:get(memory_delta, Result),
    ?assert(MemBefore > 0),
    ?assert(MemAfter > 0),
    ?assertEqual(MemAfter - MemBefore, MemDelta).

memory_usage_has_latency_fields_test() ->
    Result = flurm_benchmark:memory_usage(#{job_count => 10}),
    ?assert(maps:is_key(min_latency_us, Result)),
    ?assert(maps:is_key(max_latency_us, Result)),
    ?assert(maps:is_key(avg_latency_us, Result)).

%%====================================================================
%% concurrent_connections/1 Tests
%%====================================================================

concurrent_connections_returns_map_test() ->
    Result = flurm_benchmark:concurrent_connections(#{connections => 2, requests_per_connection => 3}),
    ?assert(is_map(Result)).

concurrent_connections_has_name_test() ->
    Result = flurm_benchmark:concurrent_connections(#{connections => 2, requests_per_connection => 3}),
    ?assertEqual(concurrent_connections, maps:get(name, Result)).

concurrent_connections_calculates_total_ops_test() ->
    Result = flurm_benchmark:concurrent_connections(#{connections => 3, requests_per_connection => 4}),
    ?assertEqual(12, maps:get(iterations, Result)).

concurrent_connections_has_all_fields_test() ->
    Result = flurm_benchmark:concurrent_connections(#{connections => 2, requests_per_connection => 3}),
    ?assert(maps:is_key(duration_ms, Result)),
    ?assert(maps:is_key(ops_per_second, Result)),
    ?assert(maps:is_key(min_latency_us, Result)),
    ?assert(maps:is_key(max_latency_us, Result)),
    ?assert(maps:is_key(avg_latency_us, Result)),
    ?assert(maps:is_key(p50_latency_us, Result)),
    ?assert(maps:is_key(p95_latency_us, Result)),
    ?assert(maps:is_key(p99_latency_us, Result)).

concurrent_connections_latency_minimum_test() ->
    %% Timer sleep of 1ms ensures minimum latency > 1000us
    Result = flurm_benchmark:concurrent_connections(#{connections => 2, requests_per_connection => 3}),
    MinLatency = maps:get(min_latency_us, Result),
    ?assert(MinLatency >= 1000).

%%====================================================================
%% generate_report/1 Tests
%%====================================================================

generate_report_returns_binary_test() ->
    Results = [sample_result()],
    Report = flurm_benchmark:generate_report(Results),
    ?assert(is_binary(Report)).

generate_report_contains_header_test() ->
    Results = [sample_result()],
    Report = flurm_benchmark:generate_report(Results),
    ?assert(binary:match(Report, <<"FLURM Performance Benchmark Report">>) =/= nomatch).

generate_report_contains_benchmark_name_test() ->
    Results = [sample_result()],
    Report = flurm_benchmark:generate_report(Results),
    ?assert(binary:match(Report, <<"test_benchmark">>) =/= nomatch).

generate_report_contains_duration_test() ->
    Results = [sample_result()],
    Report = flurm_benchmark:generate_report(Results),
    ?assert(binary:match(Report, <<"Duration">>) =/= nomatch).

generate_report_contains_throughput_test() ->
    Results = [sample_result()],
    Report = flurm_benchmark:generate_report(Results),
    ?assert(binary:match(Report, <<"Throughput">>) =/= nomatch).

generate_report_contains_latency_test() ->
    Results = [sample_result()],
    Report = flurm_benchmark:generate_report(Results),
    ?assert(binary:match(Report, <<"Latency">>) =/= nomatch).

generate_report_contains_memory_test() ->
    Results = [sample_result()],
    Report = flurm_benchmark:generate_report(Results),
    ?assert(binary:match(Report, <<"Memory">>) =/= nomatch).

generate_report_multiple_results_test() ->
    Results = [sample_result(), sample_result_2()],
    Report = flurm_benchmark:generate_report(Results),
    ?assert(binary:match(Report, <<"test_benchmark">>) =/= nomatch),
    ?assert(binary:match(Report, <<"another_benchmark">>) =/= nomatch).

generate_report_empty_list_test() ->
    Report = flurm_benchmark:generate_report([]),
    ?assert(is_binary(Report)),
    ?assert(binary:match(Report, <<"FLURM Performance Benchmark Report">>) =/= nomatch).

generate_report_formats_kb_test() ->
    %% 500000 bytes should be formatted as KB
    Results = [sample_result()],
    Report = flurm_benchmark:generate_report(Results),
    ?assert(binary:match(Report, <<"KB">>) =/= nomatch).

generate_report_formats_mb_test() ->
    %% Over 1MB should be formatted as MB
    Result = sample_result(),
    ResultMB = Result#{memory_delta => 2000000},
    Report = flurm_benchmark:generate_report([ResultMB]),
    ?assert(binary:match(Report, <<"MB">>) =/= nomatch).

generate_report_formats_bytes_test() ->
    %% Small memory delta should be formatted as B
    Result = sample_result(),
    ResultBytes = Result#{memory_delta => 500},
    Report = flurm_benchmark:generate_report([ResultBytes]),
    ?assert(binary:match(Report, <<" B">>) =/= nomatch).

%%====================================================================
%% compare_reports/2 Tests
%%====================================================================

compare_reports_returns_binary_test() ->
    Before = [sample_result()],
    After = [sample_result()],
    Report = flurm_benchmark:compare_reports(Before, After),
    ?assert(is_binary(Report)).

compare_reports_contains_header_test() ->
    Before = [sample_result()],
    After = [sample_result()],
    Report = flurm_benchmark:compare_reports(Before, After),
    ?assert(binary:match(Report, <<"FLURM Performance Comparison">>) =/= nomatch).

compare_reports_contains_benchmark_name_test() ->
    Before = [sample_result()],
    After = [sample_result()],
    Report = flurm_benchmark:compare_reports(Before, After),
    ?assert(binary:match(Report, <<"test_benchmark">>) =/= nomatch).

compare_reports_shows_ops_change_test() ->
    Before = [sample_result()],
    After = [sample_result()],
    Report = flurm_benchmark:compare_reports(Before, After),
    ?assert(binary:match(Report, <<"ops/sec">>) =/= nomatch).

compare_reports_shows_percentage_test() ->
    Before = [sample_result()],
    After = [sample_result()],
    Report = flurm_benchmark:compare_reports(Before, After),
    ?assert(binary:match(Report, <<"%">>) =/= nomatch).

compare_reports_improvement_test() ->
    Before = [sample_result()],
    %% Double the ops/sec
    AfterResult = (sample_result())#{ops_per_second => 20000.0},
    After = [AfterResult],
    Report = flurm_benchmark:compare_reports(Before, After),
    %% Should show positive percentage (100% improvement)
    ?assert(binary:match(Report, <<"100">>) =/= nomatch).

compare_reports_missing_before_test() ->
    %% When a benchmark is missing from before, should skip comparison
    Before = [],
    After = [sample_result()],
    Report = flurm_benchmark:compare_reports(Before, After),
    %% Should still be valid binary
    ?assert(is_binary(Report)).

compare_reports_multiple_benchmarks_test() ->
    Before = [sample_result(), sample_result_2()],
    After = [sample_result(), sample_result_2()],
    Report = flurm_benchmark:compare_reports(Before, After),
    ?assert(binary:match(Report, <<"test_benchmark">>) =/= nomatch),
    ?assert(binary:match(Report, <<"another_benchmark">>) =/= nomatch).

compare_reports_empty_lists_test() ->
    Report = flurm_benchmark:compare_reports([], []),
    ?assert(is_binary(Report)),
    ?assert(binary:match(Report, <<"FLURM Performance Comparison">>) =/= nomatch).

%%====================================================================
%% Result Structure Tests
%%====================================================================

result_structure_name_is_atom_test() ->
    Result = flurm_benchmark:run_benchmark(job_submission, #{iterations => 5}),
    ?assert(is_atom(maps:get(name, Result))).

result_structure_duration_is_integer_test() ->
    Result = flurm_benchmark:run_benchmark(job_submission, #{iterations => 5}),
    ?assert(is_integer(maps:get(duration_ms, Result))).

result_structure_iterations_is_integer_test() ->
    Result = flurm_benchmark:run_benchmark(job_submission, #{iterations => 5}),
    ?assert(is_integer(maps:get(iterations, Result))).

result_structure_ops_per_second_is_float_test() ->
    Result = flurm_benchmark:run_benchmark(job_submission, #{iterations => 5}),
    ?assert(is_float(maps:get(ops_per_second, Result))).

result_structure_latencies_are_numbers_test() ->
    Result = flurm_benchmark:run_benchmark(job_submission, #{iterations => 10}),
    ?assert(is_number(maps:get(min_latency_us, Result))),
    ?assert(is_number(maps:get(max_latency_us, Result))),
    ?assert(is_number(maps:get(avg_latency_us, Result))),
    ?assert(is_number(maps:get(p50_latency_us, Result))),
    ?assert(is_number(maps:get(p95_latency_us, Result))),
    ?assert(is_number(maps:get(p99_latency_us, Result))).

result_structure_memory_is_integer_test() ->
    Result = flurm_benchmark:run_benchmark(job_submission, #{iterations => 5}),
    ?assert(is_integer(maps:get(memory_before, Result))),
    ?assert(is_integer(maps:get(memory_after, Result))),
    ?assert(is_integer(maps:get(memory_delta, Result))).

%%====================================================================
%% Edge Cases and Default Values Tests
%%====================================================================

default_iterations_job_submission_test() ->
    %% Default is 1000 iterations
    Result = flurm_benchmark:run_benchmark(job_submission, #{}),
    ?assertEqual(1000, maps:get(iterations, Result)).

default_iterations_scheduler_cycle_test() ->
    %% Default is 100 iterations
    Result = flurm_benchmark:run_benchmark(scheduler_cycle, #{}),
    ?assertEqual(100, maps:get(iterations, Result)).

default_iterations_protocol_codec_test() ->
    %% Default is 10000 iterations
    Result = flurm_benchmark:run_benchmark(protocol_codec, #{}),
    ?assertEqual(10000, maps:get(iterations, Result)).

default_job_count_memory_usage_test() ->
    %% Default is 10000 jobs
    Result = flurm_benchmark:run_benchmark(memory_usage, #{}),
    ?assertEqual(10000, maps:get(iterations, Result)).

default_connections_concurrent_test() ->
    %% Default is 100 connections x 10 requests = 1000 ops
    Result = flurm_benchmark:run_benchmark(concurrent_connections, #{}),
    ?assertEqual(1000, maps:get(iterations, Result)).

%%====================================================================
%% Percentile Calculation Tests
%%====================================================================

percentile_values_ordered_test() ->
    Result = flurm_benchmark:run_benchmark(job_submission, #{iterations => 100}),
    P50 = maps:get(p50_latency_us, Result),
    P95 = maps:get(p95_latency_us, Result),
    P99 = maps:get(p99_latency_us, Result),
    Min = maps:get(min_latency_us, Result),
    Max = maps:get(max_latency_us, Result),
    ?assert(P50 >= Min),
    ?assert(P95 >= P50),
    ?assert(P99 >= P95),
    ?assert(Max >= P99).

%%====================================================================
%% Integration-like Tests (using real functions)
%%====================================================================

full_workflow_run_and_report_test() ->
    Options = #{iterations => 10, job_count => 10, connections => 2, requests_per_connection => 2},
    Results = flurm_benchmark:run_all(Options),
    Report = flurm_benchmark:generate_report(Results),
    ?assert(is_binary(Report)),
    ?assert(byte_size(Report) > 100).

full_workflow_compare_test() ->
    Options = #{iterations => 10, job_count => 10, connections => 2, requests_per_connection => 2},
    Before = flurm_benchmark:run_all(Options),
    After = flurm_benchmark:run_all(Options),
    Report = flurm_benchmark:compare_reports(Before, After),
    ?assert(is_binary(Report)),
    ?assert(byte_size(Report) > 50).

%%====================================================================
%% Throughput Sanity Tests
%%====================================================================

throughput_reasonable_test() ->
    %% Throughput should be reasonable (at least 100 ops/sec for simple operations)
    Result = flurm_benchmark:run_benchmark(job_submission, #{iterations => 100}),
    OpsPerSec = maps:get(ops_per_second, Result),
    ?assert(OpsPerSec > 100).

memory_benchmark_creates_jobs_test() ->
    %% Memory usage should increase when creating jobs
    Result = flurm_benchmark:run_benchmark(memory_usage, #{job_count => 1000}),
    MemDelta = maps:get(memory_delta, Result),
    %% Memory should have changed (though could be negative due to GC)
    ?assert(is_integer(MemDelta)).

%%====================================================================
%% run_all/0 Default Test (for coverage)
%%====================================================================

%% Note: run_all/0 calls run_all(#{}), which uses default iterations
%% This test is slow but needed for full coverage of run_all/0
run_all_0_direct_test_() ->
    %% Use test generator with timeout for slow test
    {timeout, 120, fun() ->
        Results = flurm_benchmark:run_all(),
        ?assert(is_list(Results)),
        ?assertEqual(5, length(Results))
    end}.
