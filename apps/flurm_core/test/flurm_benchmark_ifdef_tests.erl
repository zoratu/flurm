%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_benchmark internal functions
%%%
%%% Tests the internal pure functions exposed via -ifdef(TEST).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_benchmark_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test: safe_min/1
%%====================================================================

safe_min_empty_list_test() ->
    ?assertEqual(0, flurm_benchmark:safe_min([])).

safe_min_single_element_test() ->
    ?assertEqual(5, flurm_benchmark:safe_min([5])).

safe_min_multiple_elements_test() ->
    ?assertEqual(1, flurm_benchmark:safe_min([5, 3, 1, 4, 2])).

safe_min_negative_numbers_test() ->
    ?assertEqual(-10, flurm_benchmark:safe_min([-5, -10, -1, 0, 5])).

safe_min_all_same_test() ->
    ?assertEqual(7, flurm_benchmark:safe_min([7, 7, 7, 7])).

%%====================================================================
%% Test: safe_max/1
%%====================================================================

safe_max_empty_list_test() ->
    ?assertEqual(0, flurm_benchmark:safe_max([])).

safe_max_single_element_test() ->
    ?assertEqual(5, flurm_benchmark:safe_max([5])).

safe_max_multiple_elements_test() ->
    ?assertEqual(5, flurm_benchmark:safe_max([5, 3, 1, 4, 2])).

safe_max_negative_numbers_test() ->
    ?assertEqual(5, flurm_benchmark:safe_max([-5, -10, -1, 0, 5])).

safe_max_all_same_test() ->
    ?assertEqual(7, flurm_benchmark:safe_max([7, 7, 7, 7])).

%%====================================================================
%% Test: safe_avg/1
%%====================================================================

safe_avg_empty_list_test() ->
    ?assertEqual(0.0, flurm_benchmark:safe_avg([])).

safe_avg_single_element_test() ->
    ?assertEqual(5.0, flurm_benchmark:safe_avg([5])).

safe_avg_multiple_elements_test() ->
    %% (1+2+3+4+5) / 5 = 15 / 5 = 3.0
    ?assertEqual(3.0, flurm_benchmark:safe_avg([1, 2, 3, 4, 5])).

safe_avg_negative_numbers_test() ->
    %% (-5 + 5) / 2 = 0.0
    ?assertEqual(0.0, flurm_benchmark:safe_avg([-5, 5])).

safe_avg_fractional_result_test() ->
    %% (1+2) / 2 = 1.5
    ?assertEqual(1.5, flurm_benchmark:safe_avg([1, 2])).

%%====================================================================
%% Test: percentile/2
%%====================================================================

percentile_empty_list_test() ->
    ?assertEqual(0, flurm_benchmark:percentile([], 50)).

percentile_p50_odd_list_test() ->
    %% Sorted list: [1, 2, 3, 4, 5]
    %% p50 index: round(50/100 * 5) = round(2.5) = 3 -> value 3
    ?assertEqual(3, flurm_benchmark:percentile([1, 2, 3, 4, 5], 50)).

percentile_p50_even_list_test() ->
    %% Sorted list: [1, 2, 3, 4]
    %% p50 index: round(50/100 * 4) = round(2) = 2 -> value 2
    ?assertEqual(2, flurm_benchmark:percentile([1, 2, 3, 4], 50)).

percentile_p95_test() ->
    %% List of 100 elements (1 to 100)
    List = lists:seq(1, 100),
    %% p95 index: round(95/100 * 100) = 95 -> value 95
    ?assertEqual(95, flurm_benchmark:percentile(List, 95)).

percentile_p99_test() ->
    %% List of 100 elements (1 to 100)
    List = lists:seq(1, 100),
    %% p99 index: round(99/100 * 100) = 99 -> value 99
    ?assertEqual(99, flurm_benchmark:percentile(List, 99)).

percentile_p0_test() ->
    %% p0 should return first element (index clamped to 1)
    ?assertEqual(1, flurm_benchmark:percentile([1, 2, 3, 4, 5], 0)).

percentile_p100_test() ->
    %% p100 should return last element
    ?assertEqual(5, flurm_benchmark:percentile([1, 2, 3, 4, 5], 100)).

percentile_single_element_test() ->
    ?assertEqual(42, flurm_benchmark:percentile([42], 50)).

%%====================================================================
%% Test: format_bytes/1
%%====================================================================

format_bytes_bytes_test() ->
    %% Under 1024 should show bytes
    Result = flurm_benchmark:format_bytes(500),
    ?assertEqual(<<"500 B">>, Result).

format_bytes_zero_test() ->
    Result = flurm_benchmark:format_bytes(0),
    ?assertEqual(<<"0 B">>, Result).

format_bytes_kilobytes_test() ->
    %% 1024 bytes = 1.00 KB
    Result = flurm_benchmark:format_bytes(1024),
    ?assertEqual(<<"1.00 KB">>, Result).

format_bytes_kilobytes_fractional_test() ->
    %% 2560 bytes = 2.50 KB
    Result = flurm_benchmark:format_bytes(2560),
    ?assertEqual(<<"2.50 KB">>, Result).

format_bytes_megabytes_test() ->
    %% 1048576 bytes = 1.00 MB
    Result = flurm_benchmark:format_bytes(1048576),
    ?assertEqual(<<"1.00 MB">>, Result).

format_bytes_megabytes_fractional_test() ->
    %% 5242880 bytes = 5.00 MB
    Result = flurm_benchmark:format_bytes(5242880),
    ?assertEqual(<<"5.00 MB">>, Result).

format_bytes_large_megabytes_test() ->
    %% 104857600 bytes = 100.00 MB
    Result = flurm_benchmark:format_bytes(104857600),
    ?assertEqual(<<"100.00 MB">>, Result).

%%====================================================================
%% Test: create_result/6
%%====================================================================

create_result_basic_test() ->
    Result = flurm_benchmark:create_result(
        test_benchmark,
        1000000,  % 1 second in microseconds
        100,      % 100 iterations
        [10, 20, 30, 40, 50],  % Latencies
        1000000,  % Memory before
        1100000   % Memory after
    ),
    ?assertEqual(test_benchmark, maps:get(name, Result)),
    ?assertEqual(1000, maps:get(duration_ms, Result)),
    ?assertEqual(100, maps:get(iterations, Result)),
    ?assertEqual(100.0, maps:get(ops_per_second, Result)),
    ?assertEqual(10, maps:get(min_latency_us, Result)),
    ?assertEqual(50, maps:get(max_latency_us, Result)),
    ?assertEqual(30.0, maps:get(avg_latency_us, Result)),
    ?assertEqual(1000000, maps:get(memory_before, Result)),
    ?assertEqual(1100000, maps:get(memory_after, Result)),
    ?assertEqual(100000, maps:get(memory_delta, Result)).

create_result_empty_latencies_test() ->
    Result = flurm_benchmark:create_result(
        empty_test,
        1000,
        0,
        [],
        1000,
        1000
    ),
    ?assertEqual(0, maps:get(min_latency_us, Result)),
    ?assertEqual(0, maps:get(max_latency_us, Result)),
    ?assertEqual(0.0, maps:get(avg_latency_us, Result)),
    ?assertEqual(0, maps:get(p50_latency_us, Result)),
    ?assertEqual(0, maps:get(p95_latency_us, Result)),
    ?assertEqual(0, maps:get(p99_latency_us, Result)).

create_result_has_all_fields_test() ->
    Result = flurm_benchmark:create_result(test, 1000, 10, [1, 2, 3], 100, 200),
    ExpectedKeys = [name, duration_ms, iterations, ops_per_second,
                    min_latency_us, max_latency_us, avg_latency_us,
                    p50_latency_us, p95_latency_us, p99_latency_us,
                    memory_before, memory_after, memory_delta],
    lists:foreach(fun(Key) ->
        ?assert(maps:is_key(Key, Result), {missing_key, Key})
    end, ExpectedKeys).

%%====================================================================
%% Test: create_sample_messages/0
%%====================================================================

create_sample_messages_test() ->
    Messages = flurm_benchmark:create_sample_messages(),
    ?assert(is_list(Messages)),
    ?assert(length(Messages) >= 1),
    %% Each message should be a tuple
    lists:foreach(fun(Msg) ->
        ?assert(is_tuple(Msg)),
        ?assertEqual(2, tuple_size(Msg))
    end, Messages).

%%====================================================================
%% Test: encode_simple_message/2
%%====================================================================

encode_simple_message_empty_body_test() ->
    Result = flurm_benchmark:encode_simple_message(1, <<>>),
    ?assert(is_binary(Result)),
    %% Header should be 16 bytes + 0 byte body
    ?assertEqual(16, byte_size(Result)).

encode_simple_message_with_body_test() ->
    Body = <<"test">>,
    Result = flurm_benchmark:encode_simple_message(1, Body),
    ?assert(is_binary(Result)),
    %% Header 16 bytes + 4 byte body
    ?assertEqual(20, byte_size(Result)).

encode_simple_message_type_in_header_test() ->
    MsgType = 100,
    Result = flurm_benchmark:encode_simple_message(MsgType, <<>>),
    %% Message type is at offset 10 (16-bit big-endian)
    <<_:10/binary, EncodedType:16/big, _/binary>> = Result,
    ?assertEqual(MsgType, EncodedType).

%%====================================================================
%% Test: create_dummy_job/0
%%====================================================================

create_dummy_job_returns_job_record_test() ->
    Job = flurm_benchmark:create_dummy_job(),
    ?assert(is_record(Job, job)).

create_dummy_job_has_valid_fields_test() ->
    Job = flurm_benchmark:create_dummy_job(),
    ?assert(is_integer(Job#job.id)),
    ?assertEqual(<<"bench_job">>, Job#job.name),
    ?assertEqual(<<"benchuser">>, Job#job.user),
    ?assertEqual(<<"batch">>, Job#job.partition),
    ?assertEqual(pending, Job#job.state),
    ?assert(is_binary(Job#job.script)),
    ?assertEqual(1, Job#job.num_nodes),
    ?assertEqual(1, Job#job.num_cpus),
    ?assertEqual(1024, Job#job.memory_mb),
    ?assertEqual(60, Job#job.time_limit),
    ?assertEqual(100, Job#job.priority),
    ?assert(is_integer(Job#job.submit_time)),
    ?assertEqual([], Job#job.allocated_nodes).

create_dummy_job_unique_ids_test() ->
    Job1 = flurm_benchmark:create_dummy_job(),
    Job2 = flurm_benchmark:create_dummy_job(),
    Job3 = flurm_benchmark:create_dummy_job(),
    Ids = [Job1#job.id, Job2#job.id, Job3#job.id],
    %% All IDs should be unique (very high probability with random)
    UniqueIds = lists:usort(Ids),
    ?assertEqual(length(Ids), length(UniqueIds)).

%%====================================================================
%% Test: simulate_scheduler_cycle/0
%%====================================================================

simulate_scheduler_cycle_test() ->
    %% Just verify it returns and doesn't crash
    Result = flurm_benchmark:simulate_scheduler_cycle(),
    %% It returns the sum of 1..1000 = 500500
    ?assertEqual(500500, Result).

%%====================================================================
%% Test: format_result/1
%%====================================================================

format_result_test() ->
    Result = #{
        name => test_benchmark,
        duration_ms => 1000,
        iterations => 100,
        ops_per_second => 100.0,
        avg_latency_us => 10.0,
        p99_latency_us => 50,
        memory_delta => 1024
    },
    Formatted = flurm_benchmark:format_result(Result),
    ?assert(is_binary(Formatted)),
    %% Should contain benchmark name
    ?assertMatch({match, _}, re:run(Formatted, <<"test_benchmark">>)).

%%====================================================================
%% Test: format_comparison/3
%%====================================================================

format_comparison_test() ->
    Before = #{name => test, ops_per_second => 100.0},
    After = #{name => test, ops_per_second => 150.0},
    Result = flurm_benchmark:format_comparison(test, Before, After),
    ?assert(is_binary(Result)),
    %% Should contain the benchmark name
    ?assertMatch({match, _}, re:run(Result, <<"test">>)).

format_comparison_improvement_test() ->
    Before = #{name => test, ops_per_second => 100.0},
    After = #{name => test, ops_per_second => 200.0},
    Result = flurm_benchmark:format_comparison(test, Before, After),
    ?assert(is_binary(Result)),
    %% Should show 100% improvement
    ?assertMatch({match, _}, re:run(Result, <<"100">>)).

format_comparison_regression_test() ->
    Before = #{name => test, ops_per_second => 100.0},
    After = #{name => test, ops_per_second => 50.0},
    Result = flurm_benchmark:format_comparison(test, Before, After),
    ?assert(is_binary(Result)),
    %% Should show negative percentage
    ?assertMatch({match, _}, re:run(Result, <<"-">>)).
