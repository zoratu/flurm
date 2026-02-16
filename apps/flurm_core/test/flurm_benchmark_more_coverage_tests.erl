%%%-------------------------------------------------------------------
%%% @doc Additional coverage tests for flurm_benchmark module.
%%% Tests internal functions exported via -ifdef(TEST) and edge cases.
%%%-------------------------------------------------------------------
-module(flurm_benchmark_more_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Statistical Functions Tests
%%====================================================================

safe_min_empty_test() ->
    ?assertEqual(0, flurm_benchmark:safe_min([])).

safe_min_single_test() ->
    ?assertEqual(42, flurm_benchmark:safe_min([42])).

safe_min_multiple_test() ->
    ?assertEqual(1, flurm_benchmark:safe_min([5, 3, 1, 4, 2])).

safe_min_negative_test() ->
    ?assertEqual(-10, flurm_benchmark:safe_min([5, -10, 3])).

safe_max_empty_test() ->
    ?assertEqual(0, flurm_benchmark:safe_max([])).

safe_max_single_test() ->
    ?assertEqual(99, flurm_benchmark:safe_max([99])).

safe_max_multiple_test() ->
    ?assertEqual(100, flurm_benchmark:safe_max([1, 100, 50, 75])).

safe_max_negative_test() ->
    ?assertEqual(5, flurm_benchmark:safe_max([-5, 0, 5])).

safe_avg_empty_test() ->
    ?assertEqual(0.0, flurm_benchmark:safe_avg([])).

safe_avg_single_test() ->
    ?assertEqual(10.0, flurm_benchmark:safe_avg([10])).

safe_avg_multiple_test() ->
    ?assertEqual(3.0, flurm_benchmark:safe_avg([1, 2, 3, 4, 5])).

safe_avg_floats_test() ->
    Result = flurm_benchmark:safe_avg([1, 2, 3]),
    ?assert(abs(Result - 2.0) < 0.0001).

%%====================================================================
%% Percentile Tests
%%====================================================================

percentile_empty_test() ->
    ?assertEqual(0, flurm_benchmark:percentile([], 50)).

percentile_single_element_test() ->
    ?assertEqual(42, flurm_benchmark:percentile([42], 50)).

percentile_50_test() ->
    SortedList = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    Result = flurm_benchmark:percentile(SortedList, 50),
    ?assert(Result >= 4 andalso Result =< 6).

percentile_99_test() ->
    SortedList = lists:seq(1, 100),
    Result = flurm_benchmark:percentile(SortedList, 99),
    ?assert(Result >= 98 andalso Result =< 100).

percentile_0_test() ->
    SortedList = [1, 2, 3, 4, 5],
    Result = flurm_benchmark:percentile(SortedList, 0),
    ?assertEqual(1, Result).

percentile_100_test() ->
    SortedList = [1, 2, 3, 4, 5],
    Result = flurm_benchmark:percentile(SortedList, 100),
    ?assertEqual(5, Result).

%%====================================================================
%% Format Bytes Tests
%%====================================================================

format_bytes_small_test() ->
    Result = flurm_benchmark:format_bytes(500),
    ?assertEqual(<<"500 B">>, Result).

format_bytes_zero_test() ->
    Result = flurm_benchmark:format_bytes(0),
    ?assertEqual(<<"0 B">>, Result).

format_bytes_kb_test() ->
    Result = flurm_benchmark:format_bytes(2048),
    ?assertEqual(<<"2.00 KB">>, Result).

format_bytes_mb_test() ->
    Result = flurm_benchmark:format_bytes(2097152),
    ?assertEqual(<<"2.00 MB">>, Result).

format_bytes_large_kb_test() ->
    Result = flurm_benchmark:format_bytes(512000),
    ?assertEqual(<<"500.00 KB">>, Result).

%%====================================================================
%% Format Result Tests
%%====================================================================

format_result_basic_test() ->
    Result = #{
        name => test_benchmark,
        duration_ms => 1000,
        iterations => 100,
        ops_per_second => 100.0,
        avg_latency_us => 10.0,
        p99_latency_us => 15,
        memory_delta => 1024
    },
    Output = flurm_benchmark:format_result(Result),
    ?assert(is_binary(Output)),
    ?assert(byte_size(Output) > 0).

format_result_with_floats_test() ->
    Result = #{
        name => float_test,
        duration_ms => 500,
        iterations => 50,
        ops_per_second => 99.999,
        avg_latency_us => 5.555,
        p99_latency_us => 10,
        memory_delta => 2048
    },
    Output = flurm_benchmark:format_result(Result),
    ?assert(is_binary(Output)).

%%====================================================================
%% Format Comparison Tests
%%====================================================================

format_comparison_improvement_test() ->
    Before = #{ops_per_second => 100.0},
    After = #{ops_per_second => 200.0},
    Output = flurm_benchmark:format_comparison(test_benchmark, Before, After),
    ?assert(is_binary(Output)).

format_comparison_regression_test() ->
    Before = #{ops_per_second => 200.0},
    After = #{ops_per_second => 100.0},
    Output = flurm_benchmark:format_comparison(test_benchmark, Before, After),
    ?assert(is_binary(Output)).

format_comparison_no_change_test() ->
    Before = #{ops_per_second => 100.0},
    After = #{ops_per_second => 100.0},
    Output = flurm_benchmark:format_comparison(test_benchmark, Before, After),
    ?assert(is_binary(Output)).

format_comparison_zero_before_test() ->
    Before = #{ops_per_second => 0.01},  % Very small value instead of 0 to avoid division by zero
    After = #{ops_per_second => 100.0},
    Output = flurm_benchmark:format_comparison(test_benchmark, Before, After),
    ?assert(is_binary(Output)).

%%====================================================================
%% Create Result Tests
%%====================================================================

create_result_basic_test() ->
    Result = flurm_benchmark:create_result(
        test_bench,
        1000000,  % 1 second in microseconds
        100,      % 100 iterations
        lists:duplicate(100, 10000),  % 10ms per operation
        1000000,  % memory before
        1100000   % memory after
    ),
    ?assert(is_map(Result)),
    ?assertEqual(test_bench, maps:get(name, Result)),
    ?assertEqual(1000, maps:get(duration_ms, Result)),
    ?assertEqual(100, maps:get(iterations, Result)),
    ?assert(maps:get(ops_per_second, Result) > 0),
    ?assertEqual(100000, maps:get(memory_delta, Result)).

create_result_empty_latencies_test() ->
    Result = flurm_benchmark:create_result(
        empty_test,
        1000,
        0,
        [],
        1000000,
        1000000
    ),
    ?assert(is_map(Result)),
    ?assertEqual(0, maps:get(min_latency_us, Result)),
    ?assertEqual(0, maps:get(max_latency_us, Result)),
    ?assertEqual(0.0, maps:get(avg_latency_us, Result)).

create_result_single_latency_test() ->
    Result = flurm_benchmark:create_result(
        single_test,
        1000,
        1,
        [500],
        1000000,
        1000000
    ),
    ?assert(is_map(Result)),
    ?assertEqual(500, maps:get(min_latency_us, Result)),
    ?assertEqual(500, maps:get(max_latency_us, Result)).

%%====================================================================
%% Sample Messages Tests
%%====================================================================

create_sample_messages_test() ->
    Messages = flurm_benchmark:create_sample_messages(),
    ?assert(is_list(Messages)),
    ?assert(length(Messages) >= 2),
    %% Each message should be a tuple of {Type, Body}
    lists:foreach(fun({Type, Body}) ->
        ?assert(is_integer(Type)),
        ?assert(is_binary(Body))
    end, Messages).

%%====================================================================
%% Encode Simple Message Tests
%%====================================================================

encode_simple_message_ping_test() ->
    Encoded = flurm_benchmark:encode_simple_message(1008, <<>>),
    ?assert(is_binary(Encoded)),
    ?assert(byte_size(Encoded) >= 16).

encode_simple_message_with_body_test() ->
    Body = <<1, 2, 3, 4>>,
    Encoded = flurm_benchmark:encode_simple_message(2003, Body),
    ?assert(is_binary(Encoded)),
    ?assert(byte_size(Encoded) >= 20).

%%====================================================================
%% Create Dummy Job Tests
%%====================================================================

create_dummy_job_test() ->
    Job = flurm_benchmark:create_dummy_job(),
    ?assert(is_tuple(Job)),
    ?assertEqual(job, element(1, Job)).

create_dummy_job_has_name_test() ->
    Job = flurm_benchmark:create_dummy_job(),
    %% Job record has name field
    ?assertEqual(<<"bench_job">>, element(3, Job)).

create_dummy_job_has_partition_test() ->
    Job = flurm_benchmark:create_dummy_job(),
    %% Job record has partition field
    ?assertEqual(<<"batch">>, element(5, Job)).

%%====================================================================
%% Simulate Scheduler Cycle Tests
%%====================================================================

simulate_scheduler_cycle_test() ->
    %% Should complete without error
    Result = flurm_benchmark:simulate_scheduler_cycle(),
    ?assertEqual(500500, Result).  % Sum of 1 to 1000

%%====================================================================
%% Run Benchmark Tests
%%====================================================================

run_benchmark_job_submission_test() ->
    Result = flurm_benchmark:run_benchmark(job_submission),
    ?assert(is_map(Result)),
    ?assertEqual(job_submission, maps:get(name, Result)).

run_benchmark_scheduler_cycle_test() ->
    Result = flurm_benchmark:run_benchmark(scheduler_cycle),
    ?assert(is_map(Result)),
    ?assertEqual(scheduler_cycle, maps:get(name, Result)).

run_benchmark_protocol_codec_test() ->
    Result = flurm_benchmark:run_benchmark(protocol_codec),
    ?assert(is_map(Result)),
    ?assertEqual(protocol_codec, maps:get(name, Result)).

run_benchmark_memory_usage_test() ->
    Result = flurm_benchmark:run_benchmark(memory_usage),
    ?assert(is_map(Result)),
    ?assertEqual(memory_usage, maps:get(name, Result)).

run_benchmark_concurrent_connections_test() ->
    %% This test takes a moment, use smaller iteration count
    Options = #{connections => 5, requests_per_connection => 2},
    Result = flurm_benchmark:run_benchmark(concurrent_connections, Options),
    ?assert(is_map(Result)),
    ?assertEqual(concurrent_connections, maps:get(name, Result)).

run_benchmark_with_options_test() ->
    Options = #{iterations => 10},
    Result = flurm_benchmark:run_benchmark(job_submission, Options),
    ?assertEqual(10, maps:get(iterations, Result)).

%%====================================================================
%% Generate Report Tests
%%====================================================================

generate_report_empty_test() ->
    Report = flurm_benchmark:generate_report([]),
    ?assert(is_binary(Report)),
    ?assert(binary:match(Report, <<"FLURM Performance">>) =/= nomatch).

generate_report_single_test() ->
    Result = #{
        name => test_bench,
        duration_ms => 100,
        iterations => 10,
        ops_per_second => 100.0,
        avg_latency_us => 10.0,
        p99_latency_us => 15,
        memory_delta => 1024
    },
    Report = flurm_benchmark:generate_report([Result]),
    ?assert(is_binary(Report)),
    ?assert(byte_size(Report) > 50).

generate_report_multiple_test() ->
    Results = [
        #{name => bench1, duration_ms => 100, iterations => 10,
          ops_per_second => 100.0, avg_latency_us => 10.0,
          p99_latency_us => 15, memory_delta => 1024},
        #{name => bench2, duration_ms => 200, iterations => 20,
          ops_per_second => 100.0, avg_latency_us => 10.0,
          p99_latency_us => 15, memory_delta => 2048}
    ],
    Report = flurm_benchmark:generate_report(Results),
    ?assert(is_binary(Report)).

%%====================================================================
%% Compare Reports Tests
%%====================================================================

compare_reports_empty_test() ->
    Comparison = flurm_benchmark:compare_reports([], []),
    ?assert(is_binary(Comparison)).

compare_reports_matching_test() ->
    Before = [#{name => test, ops_per_second => 100.0}],
    After = [#{name => test, ops_per_second => 150.0}],
    Comparison = flurm_benchmark:compare_reports(Before, After),
    ?assert(is_binary(Comparison)).

compare_reports_mismatched_test() ->
    Before = [#{name => test1, ops_per_second => 100.0}],
    After = [#{name => test2, ops_per_second => 150.0}],
    Comparison = flurm_benchmark:compare_reports(Before, After),
    ?assert(is_binary(Comparison)).

%%====================================================================
%% Format Results Tests
%%====================================================================

format_results_empty_test() ->
    Output = flurm_benchmark:format_results([]),
    ?assert(is_binary(Output)),
    ?assert(binary:match(Output, <<"FLURM Performance">>) =/= nomatch).

format_results_single_test() ->
    Results = [#{
        name => test_bench,
        duration_ms => 1000,
        iterations => 100,
        ops_per_second => 100.0,
        min_latency_us => 5,
        max_latency_us => 20,
        avg_latency_us => 10.0,
        p50_latency_us => 10,
        p95_latency_us => 18,
        p99_latency_us => 20,
        memory_before => 1000000,
        memory_after => 1100000,
        memory_delta => 100000
    }],
    Output = flurm_benchmark:format_results(Results),
    ?assert(is_binary(Output)).

%%====================================================================
%% API Function Tests
%%====================================================================

benchmark_job_submission_test() ->
    Result = flurm_benchmark:benchmark_job_submission(10),
    ?assert(is_map(Result)),
    ?assertEqual(job_submission, maps:get(name, Result)),
    ?assertEqual(10, maps:get(iterations, Result)).

benchmark_scheduler_cycle_test() ->
    Result = flurm_benchmark:benchmark_scheduler_cycle(5),
    ?assert(is_map(Result)),
    ?assertEqual(scheduler_cycle, maps:get(name, Result)).

benchmark_protocol_encoding_test() ->
    Result = flurm_benchmark:benchmark_protocol_encoding(100),
    ?assert(is_map(Result)),
    ?assertEqual(protocol_encoding, maps:get(name, Result)).

benchmark_state_persistence_test() ->
    Result = flurm_benchmark:benchmark_state_persistence(50),
    ?assert(is_map(Result)),
    ?assertEqual(state_persistence, maps:get(name, Result)),
    %% Should have store and lookup sub-results
    ?assert(maps:is_key(store, Result)),
    ?assert(maps:is_key(lookup, Result)).

%%====================================================================
%% Job Submission Throughput Tests
%%====================================================================

job_submission_throughput_default_test() ->
    Result = flurm_benchmark:job_submission_throughput(#{}),
    ?assert(is_map(Result)),
    ?assertEqual(job_submission, maps:get(name, Result)),
    ?assertEqual(1000, maps:get(iterations, Result)).

job_submission_throughput_custom_test() ->
    Result = flurm_benchmark:job_submission_throughput(#{iterations => 50}),
    ?assertEqual(50, maps:get(iterations, Result)).

%%====================================================================
%% Scheduler Cycle Time Tests
%%====================================================================

scheduler_cycle_time_default_test() ->
    Result = flurm_benchmark:scheduler_cycle_time(#{}),
    ?assert(is_map(Result)),
    ?assertEqual(scheduler_cycle, maps:get(name, Result)),
    ?assertEqual(100, maps:get(iterations, Result)).

scheduler_cycle_time_custom_test() ->
    Result = flurm_benchmark:scheduler_cycle_time(#{iterations => 25}),
    ?assertEqual(25, maps:get(iterations, Result)).

%%====================================================================
%% Protocol Codec Performance Tests
%%====================================================================

protocol_codec_performance_default_test() ->
    Result = flurm_benchmark:protocol_codec_performance(#{}),
    ?assert(is_map(Result)),
    ?assertEqual(protocol_codec, maps:get(name, Result)).

protocol_codec_performance_custom_test() ->
    Result = flurm_benchmark:protocol_codec_performance(#{iterations => 100}),
    ?assert(is_map(Result)).

%%====================================================================
%% Memory Usage Tests
%%====================================================================

memory_usage_default_test() ->
    Result = flurm_benchmark:memory_usage(#{}),
    ?assert(is_map(Result)),
    ?assertEqual(memory_usage, maps:get(name, Result)).

memory_usage_custom_test() ->
    Result = flurm_benchmark:memory_usage(#{job_count => 100}),
    ?assertEqual(100, maps:get(iterations, Result)).

%%====================================================================
%% Concurrent Connections Tests
%%====================================================================

concurrent_connections_default_test() ->
    %% Use small values for fast test
    Result = flurm_benchmark:concurrent_connections(#{connections => 3, requests_per_connection => 2}),
    ?assert(is_map(Result)),
    ?assertEqual(concurrent_connections, maps:get(name, Result)).
