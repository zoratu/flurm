%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_benchmark module
%%% Achieves 100% code coverage for the benchmarking functionality.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_benchmark_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Mock dependencies
    meck:new(flurm_protocol_codec, [passthrough, non_strict]),
    meck:expect(flurm_protocol_codec, decode, fun(_Binary) ->
        {ok, {request, ping, #{}}}
    end),
    meck:expect(flurm_protocol_codec, encode, fun(_Msg) ->
        <<1,2,3,4,5>>
    end),
    ok.

cleanup(_) ->
    meck:unload(flurm_protocol_codec),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

benchmark_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Primary API tests
        {"run_all_benchmarks returns list of results", fun test_run_all_benchmarks/0},
        {"benchmark_job_submission with various counts", fun test_benchmark_job_submission/0},
        {"benchmark_scheduler_cycle with various counts", fun test_benchmark_scheduler_cycle/0},
        {"benchmark_protocol_encoding with various counts", fun test_benchmark_protocol_encoding/0},
        {"benchmark_state_persistence with various counts", fun test_benchmark_state_persistence/0},
        {"format_results formats benchmark output", fun test_format_results/0},

        %% Extended API tests
        {"run_all with default options", fun test_run_all_default/0},
        {"run_all with custom options", fun test_run_all_custom/0},
        {"run_benchmark dispatches correctly", fun test_run_benchmark_dispatch/0},
        {"job_submission_throughput benchmark", fun test_job_submission_throughput/0},
        {"scheduler_cycle_time benchmark", fun test_scheduler_cycle_time/0},
        {"protocol_codec_performance benchmark", fun test_protocol_codec_performance/0},
        {"memory_usage benchmark", fun test_memory_usage/0},
        {"concurrent_connections benchmark", fun test_concurrent_connections/0},
        {"generate_report formats results", fun test_generate_report/0},
        {"compare_reports compares benchmarks", fun test_compare_reports/0}
     ]}.

internal_functions_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Internal function tests
        {"safe_min handles empty list", fun test_safe_min_empty/0},
        {"safe_min handles non-empty list", fun test_safe_min_nonempty/0},
        {"safe_max handles empty list", fun test_safe_max_empty/0},
        {"safe_max handles non-empty list", fun test_safe_max_nonempty/0},
        {"safe_avg handles empty list", fun test_safe_avg_empty/0},
        {"safe_avg handles non-empty list", fun test_safe_avg_nonempty/0},
        {"percentile handles empty list", fun test_percentile_empty/0},
        {"percentile handles various percentiles", fun test_percentile_various/0},
        {"format_bytes formats various sizes", fun test_format_bytes/0},
        {"format_result formats single result", fun test_format_result/0},
        {"format_comparison compares results", fun test_format_comparison/0},
        {"create_result builds proper map", fun test_create_result/0},
        {"create_sample_messages returns valid messages", fun test_create_sample_messages/0},
        {"encode_simple_message builds protocol frame", fun test_encode_simple_message/0},
        {"create_dummy_job creates valid job record", fun test_create_dummy_job/0},
        {"simulate_scheduler_cycle executes", fun test_simulate_scheduler_cycle/0},
        {"create_batch_job_request creates valid record", fun test_create_batch_job_request/0}
     ]}.

%%====================================================================
%% Primary API Tests
%%====================================================================

test_run_all_benchmarks() ->
    Results = flurm_benchmark:run_all_benchmarks(),
    ?assert(is_list(Results)),
    ?assertEqual(4, length(Results)),

    %% Verify each result has required fields
    lists:foreach(fun(Result) ->
        ?assert(is_map(Result)),
        ?assert(maps:is_key(name, Result))
    end, Results).

test_benchmark_job_submission() ->
    %% Small number for fast test
    Result = flurm_benchmark:benchmark_job_submission(10),
    ?assert(is_map(Result)),
    ?assertEqual(job_submission, maps:get(name, Result)),
    ?assertEqual(10, maps:get(iterations, Result)),
    ?assert(maps:get(duration_ms, Result) >= 0),
    ?assert(maps:get(ops_per_second, Result) >= 0),
    ?assert(maps:get(min_latency_us, Result) >= 0),
    ?assert(maps:get(max_latency_us, Result) >= 0),
    ?assert(is_float(maps:get(avg_latency_us, Result))),
    ?assert(maps:get(p50_latency_us, Result) >= 0),
    ?assert(maps:get(p95_latency_us, Result) >= 0),
    ?assert(maps:get(p99_latency_us, Result) >= 0),
    ?assert(is_integer(maps:get(memory_before, Result))),
    ?assert(is_integer(maps:get(memory_after, Result))).

test_benchmark_scheduler_cycle() ->
    Result = flurm_benchmark:benchmark_scheduler_cycle(10),
    ?assert(is_map(Result)),
    ?assertEqual(scheduler_cycle, maps:get(name, Result)),
    ?assertEqual(10, maps:get(iterations, Result)),
    ?assert(maps:is_key(pending_jobs, Result)),
    ?assert(maps:get(pending_jobs, Result) =< 10).

test_benchmark_protocol_encoding() ->
    Result = flurm_benchmark:benchmark_protocol_encoding(100),
    ?assert(is_map(Result)),
    ?assertEqual(protocol_encoding, maps:get(name, Result)),
    ?assert(maps:is_key(message_types, Result)),
    ?assert(maps:get(message_types, Result) > 0).

test_benchmark_state_persistence() ->
    Result = flurm_benchmark:benchmark_state_persistence(50),
    ?assert(is_map(Result)),
    ?assertEqual(state_persistence, maps:get(name, Result)),
    ?assert(maps:is_key(store, Result)),
    ?assert(maps:is_key(lookup, Result)),
    ?assert(maps:is_key(store_ops_per_second, Result)),
    ?assert(maps:is_key(lookup_ops_per_second, Result)),

    %% Check nested store result
    StoreResult = maps:get(store, Result),
    ?assert(is_map(StoreResult)),
    ?assertEqual(state_store, maps:get(name, StoreResult)),

    %% Check nested lookup result
    LookupResult = maps:get(lookup, Result),
    ?assert(is_map(LookupResult)),
    ?assertEqual(state_lookup, maps:get(name, LookupResult)).

test_format_results() ->
    Results = [
        #{name => job_submission, duration_ms => 100, iterations => 1000,
          ops_per_second => 10000.0, min_latency_us => 1, max_latency_us => 100,
          avg_latency_us => 10.0, p50_latency_us => 9, p95_latency_us => 50,
          p99_latency_us => 90, memory_before => 1000000, memory_after => 1100000,
          memory_delta => 100000},
        #{name => state_persistence, duration_ms => 200, iterations => 2000,
          store_ops_per_second => 5000.0, lookup_ops_per_second => 10000.0,
          store => #{name => state_store, avg_latency_us => 5.0, p99_latency_us => 20},
          lookup => #{name => state_lookup, avg_latency_us => 2.0, p99_latency_us => 10},
          memory_before => 1000000, memory_after => 1100000, memory_delta => 100000}
    ],

    Formatted = flurm_benchmark:format_results(Results),
    ?assert(is_binary(Formatted)),
    ?assert(byte_size(Formatted) > 0),
    %% Check it contains expected content
    ?assert(binary:match(Formatted, <<"FLURM Performance Benchmark Results">>) =/= nomatch),
    ?assert(binary:match(Formatted, <<"Summary">>) =/= nomatch).

%%====================================================================
%% Extended API Tests
%%====================================================================

test_run_all_default() ->
    Results = flurm_benchmark:run_all(),
    ?assert(is_list(Results)),
    ?assertEqual(5, length(Results)).

test_run_all_custom() ->
    Results = flurm_benchmark:run_all(#{iterations => 5}),
    ?assert(is_list(Results)),
    ?assertEqual(5, length(Results)).

test_run_benchmark_dispatch() ->
    %% Test each benchmark type
    R1 = flurm_benchmark:run_benchmark(job_submission),
    ?assertEqual(job_submission, maps:get(name, R1)),

    R2 = flurm_benchmark:run_benchmark(scheduler_cycle),
    ?assertEqual(scheduler_cycle, maps:get(name, R2)),

    R3 = flurm_benchmark:run_benchmark(protocol_codec),
    ?assertEqual(protocol_codec, maps:get(name, R3)),

    R4 = flurm_benchmark:run_benchmark(memory_usage),
    ?assertEqual(memory_usage, maps:get(name, R4)),

    R5 = flurm_benchmark:run_benchmark(concurrent_connections),
    ?assertEqual(concurrent_connections, maps:get(name, R5)),

    %% Test with options
    R6 = flurm_benchmark:run_benchmark(job_submission, #{iterations => 5}),
    ?assertEqual(5, maps:get(iterations, R6)).

test_job_submission_throughput() ->
    Result = flurm_benchmark:job_submission_throughput(#{iterations => 10}),
    ?assert(is_map(Result)),
    ?assertEqual(job_submission, maps:get(name, Result)),
    ?assertEqual(10, maps:get(iterations, Result)).

test_scheduler_cycle_time() ->
    Result = flurm_benchmark:scheduler_cycle_time(#{iterations => 10}),
    ?assert(is_map(Result)),
    ?assertEqual(scheduler_cycle, maps:get(name, Result)),
    ?assertEqual(10, maps:get(iterations, Result)).

test_protocol_codec_performance() ->
    Result = flurm_benchmark:protocol_codec_performance(#{iterations => 50}),
    ?assert(is_map(Result)),
    ?assertEqual(protocol_codec, maps:get(name, Result)).

test_memory_usage() ->
    Result = flurm_benchmark:memory_usage(#{job_count => 10}),
    ?assert(is_map(Result)),
    ?assertEqual(memory_usage, maps:get(name, Result)),
    ?assertEqual(10, maps:get(iterations, Result)).

test_concurrent_connections() ->
    Result = flurm_benchmark:concurrent_connections(#{connections => 5, requests_per_connection => 2}),
    ?assert(is_map(Result)),
    ?assertEqual(concurrent_connections, maps:get(name, Result)),
    ?assertEqual(10, maps:get(iterations, Result)).  % 5 * 2

test_generate_report() ->
    Results = [
        #{name => test_benchmark, duration_ms => 100, iterations => 100,
          ops_per_second => 1000.0, avg_latency_us => 100.0, p99_latency_us => 500,
          memory_delta => 10000}
    ],

    Report = flurm_benchmark:generate_report(Results),
    ?assert(is_binary(Report)),
    ?assert(binary:match(Report, <<"FLURM Performance Benchmark Report">>) =/= nomatch).

test_compare_reports() ->
    Before = [
        #{name => job_submission, ops_per_second => 1000.0}
    ],
    After = [
        #{name => job_submission, ops_per_second => 1500.0}
    ],

    Comparison = flurm_benchmark:compare_reports(Before, After),
    ?assert(is_binary(Comparison)),
    ?assert(binary:match(Comparison, <<"Performance Comparison">>) =/= nomatch),
    ?assert(binary:match(Comparison, <<"job_submission">>) =/= nomatch),

    %% Test with mismatched names
    AfterMismatch = [
        #{name => other_benchmark, ops_per_second => 2000.0}
    ],
    Comparison2 = flurm_benchmark:compare_reports(Before, AfterMismatch),
    ?assert(is_binary(Comparison2)).

%%====================================================================
%% Internal Function Tests
%%====================================================================

test_safe_min_empty() ->
    ?assertEqual(0, flurm_benchmark:safe_min([])).

test_safe_min_nonempty() ->
    ?assertEqual(1, flurm_benchmark:safe_min([5, 3, 1, 4, 2])),
    ?assertEqual(-10, flurm_benchmark:safe_min([0, -10, 5])),
    ?assertEqual(42, flurm_benchmark:safe_min([42])).

test_safe_max_empty() ->
    ?assertEqual(0, flurm_benchmark:safe_max([])).

test_safe_max_nonempty() ->
    ?assertEqual(5, flurm_benchmark:safe_max([5, 3, 1, 4, 2])),
    ?assertEqual(100, flurm_benchmark:safe_max([0, -10, 100])),
    ?assertEqual(42, flurm_benchmark:safe_max([42])).

test_safe_avg_empty() ->
    ?assertEqual(0.0, flurm_benchmark:safe_avg([])).

test_safe_avg_nonempty() ->
    ?assertEqual(3.0, flurm_benchmark:safe_avg([1, 2, 3, 4, 5])),
    ?assertEqual(10.0, flurm_benchmark:safe_avg([10])),
    ?assertEqual(0.0, flurm_benchmark:safe_avg([0, 0, 0])).

test_percentile_empty() ->
    ?assertEqual(0, flurm_benchmark:percentile([], 50)).

test_percentile_various() ->
    %% Sorted list for percentile
    Sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],

    %% p50 should be around middle
    P50 = flurm_benchmark:percentile(Sorted, 50),
    ?assert(P50 >= 4 andalso P50 =< 6),

    %% p99 should be near end
    P99 = flurm_benchmark:percentile(Sorted, 99),
    ?assert(P99 >= 9),

    %% p1 should be near start
    P1 = flurm_benchmark:percentile(Sorted, 1),
    ?assert(P1 =< 2),

    %% Single element
    ?assertEqual(42, flurm_benchmark:percentile([42], 50)).

test_format_bytes() ->
    %% Bytes
    B100 = flurm_benchmark:format_bytes(100),
    ?assert(binary:match(B100, <<"B">>) =/= nomatch),

    %% Kilobytes
    KB = flurm_benchmark:format_bytes(2048),
    ?assert(binary:match(KB, <<"KB">>) =/= nomatch),

    %% Megabytes
    MB = flurm_benchmark:format_bytes(2 * 1024 * 1024),
    ?assert(binary:match(MB, <<"MB">>) =/= nomatch),

    %% Zero
    Zero = flurm_benchmark:format_bytes(0),
    ?assert(binary:match(Zero, <<"0">>) =/= nomatch).

test_format_result() ->
    Result = #{
        name => test_benchmark,
        duration_ms => 100,
        iterations => 1000,
        ops_per_second => 10000.0,
        avg_latency_us => 100.0,
        p99_latency_us => 500,
        memory_delta => 10000
    },

    Formatted = flurm_benchmark:format_result(Result),
    ?assert(is_binary(Formatted)),
    ?assert(binary:match(Formatted, <<"test_benchmark">>) =/= nomatch),
    ?assert(binary:match(Formatted, <<"Duration">>) =/= nomatch).

test_format_comparison() ->
    Before = #{name => test, ops_per_second => 1000.0},
    After = #{name => test, ops_per_second => 1500.0},

    Comparison = flurm_benchmark:format_comparison(test, Before, After),
    ?assert(is_binary(Comparison)),
    %% Should show 50% improvement
    ?assert(binary:match(Comparison, <<"test">>) =/= nomatch),

    %% Test with zero ops before
    BeforeZero = #{name => test, ops_per_second => 0.0},
    Comparison2 = flurm_benchmark:format_comparison(test, BeforeZero, After),
    ?assert(is_binary(Comparison2)).

test_create_result() ->
    Latencies = [10, 20, 30, 40, 50],
    Result = flurm_benchmark:create_result(test_name, 1000, 5, Latencies, 1000000, 1100000),

    ?assertEqual(test_name, maps:get(name, Result)),
    ?assertEqual(1, maps:get(duration_ms, Result)),  % 1000 us = 1 ms
    ?assertEqual(5, maps:get(iterations, Result)),
    ?assert(is_number(maps:get(ops_per_second, Result))),
    ?assertEqual(10, maps:get(min_latency_us, Result)),
    ?assertEqual(50, maps:get(max_latency_us, Result)),
    ?assertEqual(30.0, maps:get(avg_latency_us, Result)),
    ?assertEqual(1000000, maps:get(memory_before, Result)),
    ?assertEqual(1100000, maps:get(memory_after, Result)),
    ?assertEqual(100000, maps:get(memory_delta, Result)).

test_create_sample_messages() ->
    Messages = flurm_benchmark:create_sample_messages(),
    ?assert(is_list(Messages)),
    ?assert(length(Messages) >= 2),

    %% Each message should be {Type, Body}
    lists:foreach(fun({Type, Body}) ->
        ?assert(is_integer(Type)),
        ?assert(is_binary(Body))
    end, Messages).

test_encode_simple_message() ->
    Encoded = flurm_benchmark:encode_simple_message(1001, <<1, 2, 3>>),
    ?assert(is_binary(Encoded)),

    %% Check structure: 4 bytes length, 2 version, 2 flags, 2 unused, 2 type, 4 body_len, body
    ?assert(byte_size(Encoded) >= 16),

    %% Parse header
    <<TotalLen:32/big, _Version:16/big, _Flags:16/big, _:16/big, MsgType:16/big,
      BodyLen:32/big, Body/binary>> = Encoded,
    ?assertEqual(16, TotalLen),
    ?assertEqual(1001, MsgType),
    ?assertEqual(3, BodyLen),
    ?assertEqual(<<1, 2, 3>>, Body).

test_create_dummy_job() ->
    Job = flurm_benchmark:create_dummy_job(),
    ?assert(is_tuple(Job)),
    ?assertEqual(job, element(1, Job)),
    %% Verify required fields exist and have reasonable values
    ?assert(element(2, Job) >= 1),  % id
    ?assertEqual(<<"bench_job">>, element(3, Job)),  % name
    ?assertEqual(<<"benchuser">>, element(4, Job)),  % user
    ?assertEqual(<<"batch">>, element(5, Job)).  % partition

test_simulate_scheduler_cycle() ->
    %% Should not crash
    ?assertEqual(ok, catch flurm_benchmark:simulate_scheduler_cycle()).

test_create_batch_job_request() ->
    Request = flurm_benchmark:create_batch_job_request(),
    ?assert(is_tuple(Request)),
    ?assertEqual(batch_job_request, element(1, Request)),
    ?assertEqual(<<"benchmark_job">>, element(2, Request)).  % name field

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"benchmark with 1 iteration", fun test_single_iteration/0},
        {"format_results with empty list", fun test_format_empty_results/0},
        {"state persistence cleans up ETS table", fun test_state_persistence_cleanup/0}
     ]}.

test_single_iteration() ->
    Result = flurm_benchmark:benchmark_job_submission(1),
    ?assertEqual(1, maps:get(iterations, Result)),
    ?assert(maps:get(duration_ms, Result) >= 0).

test_format_empty_results() ->
    Formatted = flurm_benchmark:format_results([]),
    ?assert(is_binary(Formatted)),
    ?assert(binary:match(Formatted, <<"Summary">>) =/= nomatch).

test_state_persistence_cleanup() ->
    %% Run state persistence benchmark
    _Result = flurm_benchmark:benchmark_state_persistence(10),

    %% Verify temp table was deleted
    ?assertEqual(undefined, ets:whereis(flurm_benchmark_jobs_temp)).

%%====================================================================
%% Concurrent Benchmark Tests
%%====================================================================

concurrent_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"concurrent benchmarks don't interfere", fun test_concurrent_benchmarks/0}
     ]}.

test_concurrent_benchmarks() ->
    Parent = self(),

    %% Run multiple benchmarks concurrently
    spawn(fun() ->
        R = flurm_benchmark:benchmark_job_submission(5),
        Parent ! {result, 1, R}
    end),
    spawn(fun() ->
        R = flurm_benchmark:benchmark_scheduler_cycle(5),
        Parent ! {result, 2, R}
    end),

    %% Collect results
    R1 = receive {result, 1, Result1} -> Result1 after 30000 -> error end,
    R2 = receive {result, 2, Result2} -> Result2 after 30000 -> error end,

    ?assert(is_map(R1)),
    ?assert(is_map(R2)),
    ?assertEqual(job_submission, maps:get(name, R1)),
    ?assertEqual(scheduler_cycle, maps:get(name, R2)).

%%====================================================================
%% Integration Tests
%%====================================================================

integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"full benchmark workflow", fun test_full_workflow/0}
     ]}.

test_full_workflow() ->
    %% Run all benchmarks
    Results = flurm_benchmark:run_all(#{iterations => 5}),

    %% Generate report
    Report = flurm_benchmark:generate_report(Results),
    ?assert(is_binary(Report)),
    ?assert(byte_size(Report) > 100),

    %% Run again for comparison
    Results2 = flurm_benchmark:run_all(#{iterations => 5}),

    %% Compare
    Comparison = flurm_benchmark:compare_reports(Results, Results2),
    ?assert(is_binary(Comparison)).
