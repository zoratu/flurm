%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_metrics module
%%%
%%% Tests metrics collection functionality including:
%%% - Counter operations (increment/decrement)
%%% - Gauge operations
%%% - Histogram operations
%%% - Labeled metrics (TRES)
%%% - Prometheus format output
%%% - Metric collection
%%% - Reset functionality
%%% - Internal helper functions
%%% - Edge cases and error handling
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_metrics_tests).
-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start the metrics server
    case whereis(flurm_metrics) of
        undefined ->
            {ok, Pid} = flurm_metrics:start_link(),
            %% Unlink to prevent test process crash on shutdown
            unlink(Pid),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, Pid}) ->
    catch ets:delete(flurm_metrics),
    catch ets:delete(flurm_histograms),
    catch ets:delete(flurm_labeled_metrics),
    case is_process_alive(Pid) of
        true ->
            Ref = monitor(process, Pid),
            catch gen_server:stop(flurm_metrics, shutdown, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush])
            end;
        false ->
            ok
    end;
cleanup({existing, _Pid}) ->
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

metrics_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"increment counter", fun test_increment/0},
      {"decrement counter", fun test_decrement/0},
      {"set gauge", fun test_gauge/0},
      {"histogram observation", fun test_histogram/0},
      {"get all metrics", fun test_get_all_metrics/0},
      {"prometheus format", fun test_prometheus_format/0},
      {"reset metrics", fun test_reset/0}
     ]}.

%%====================================================================
%% Basic Counter Tests
%%====================================================================

test_increment() ->
    %% Initial value should be 0 or set in init
    flurm_metrics:increment(flurm_jobs_submitted_total),
    _ = sys:get_state(flurm_metrics),  % Allow async cast to complete
    {ok, Value} = flurm_metrics:get_metric(flurm_jobs_submitted_total),
    ?assert(Value >= 1),

    %% Increment by specific amount
    flurm_metrics:increment(flurm_jobs_submitted_total, 5),
    _ = sys:get_state(flurm_metrics),
    {ok, Value2} = flurm_metrics:get_metric(flurm_jobs_submitted_total),
    ?assertEqual(Value + 5, Value2).

test_decrement() ->
    %% Set a starting value
    flurm_metrics:gauge(test_counter, 10),
    _ = sys:get_state(flurm_metrics),

    %% Decrement
    flurm_metrics:decrement(test_counter),
    _ = sys:get_state(flurm_metrics),
    {ok, Value} = flurm_metrics:get_metric(test_counter),
    ?assertEqual(9, Value),

    %% Should not go below 0
    flurm_metrics:decrement(test_counter, 100),
    _ = sys:get_state(flurm_metrics),
    {ok, Value2} = flurm_metrics:get_metric(test_counter),
    ?assertEqual(0, Value2).

test_gauge() ->
    %% Set gauge value
    flurm_metrics:gauge(flurm_jobs_running, 42),
    _ = sys:get_state(flurm_metrics),
    {ok, Value} = flurm_metrics:get_metric(flurm_jobs_running),
    ?assertEqual(42, Value),

    %% Update gauge
    flurm_metrics:gauge(flurm_jobs_running, 100),
    _ = sys:get_state(flurm_metrics),
    {ok, Value2} = flurm_metrics:get_metric(flurm_jobs_running),
    ?assertEqual(100, Value2).

test_histogram() ->
    %% Record observations
    flurm_metrics:histogram(flurm_request_duration_ms, 10),
    flurm_metrics:histogram(flurm_request_duration_ms, 50),
    flurm_metrics:histogram(flurm_request_duration_ms, 100),
    flurm_metrics:histogram(flurm_request_duration_ms, 200),
    _ = sys:get_state(flurm_metrics),

    %% Get all metrics should include histogram
    AllMetrics = flurm_metrics:get_all_metrics(),
    HistData = maps:get(flurm_request_duration_ms, AllMetrics),
    ?assertEqual(histogram, maps:get(type, HistData)),
    ?assertEqual(4, maps:get(count, HistData)),
    ?assertEqual(360.0, maps:get(sum, HistData)).

test_get_all_metrics() ->
    AllMetrics = flurm_metrics:get_all_metrics(),
    ?assert(is_map(AllMetrics)),

    %% Should have standard metrics initialized
    ?assert(maps:is_key(flurm_jobs_submitted_total, AllMetrics)),
    ?assert(maps:is_key(flurm_jobs_running, AllMetrics)),
    ?assert(maps:is_key(flurm_nodes_total, AllMetrics)).

test_prometheus_format() ->
    %% Set some values
    flurm_metrics:gauge(flurm_jobs_pending, 5),
    flurm_metrics:gauge(flurm_jobs_running, 10),

    %% Get Prometheus format
    Output = flurm_metrics:format_prometheus(),
    ?assert(is_list(Output)),

    %% Flatten for easier checking
    OutputStr = lists:flatten(Output),

    %% Should contain metric names
    ?assert(string:find(OutputStr, "flurm_jobs_pending") =/= nomatch),
    ?assert(string:find(OutputStr, "flurm_jobs_running") =/= nomatch),

    %% Should have TYPE declarations
    ?assert(string:find(OutputStr, "# TYPE") =/= nomatch),

    %% Should have HELP text
    ?assert(string:find(OutputStr, "# HELP") =/= nomatch).

test_reset() ->
    %% Set some values
    flurm_metrics:gauge(flurm_jobs_pending, 100),
    flurm_metrics:increment(flurm_jobs_submitted_total, 50),

    %% Reset
    ok = flurm_metrics:reset(),

    %% Counters should be back to 0
    {ok, SubmittedValue} = flurm_metrics:get_metric(flurm_jobs_submitted_total),
    ?assertEqual(0, SubmittedValue),

    %% Gauges should be 0
    {ok, PendingValue} = flurm_metrics:get_metric(flurm_jobs_pending),
    ?assertEqual(0, PendingValue).

%%====================================================================
%% Advanced Counter Tests
%%====================================================================

advanced_counter_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"increment new counter", fun test_increment_new_counter/0},
      {"decrement new counter", fun test_decrement_new_counter/0},
      {"increment by zero", fun test_increment_by_zero/0},
      {"increment by negative (wraps to increment)", fun test_increment_by_negative/0},
      {"multiple counters independent", fun test_multiple_counters/0}
     ]}.

test_increment_new_counter() ->
    %% Incrementing a non-existent counter should create it
    flurm_metrics:increment(new_test_counter),
    _ = sys:get_state(flurm_metrics),
    {ok, Value} = flurm_metrics:get_metric(new_test_counter),
    ?assertEqual(1, Value).

test_decrement_new_counter() ->
    %% Decrementing a non-existent counter should create it at 0
    flurm_metrics:decrement(another_new_counter),
    _ = sys:get_state(flurm_metrics),
    {ok, Value} = flurm_metrics:get_metric(another_new_counter),
    ?assertEqual(0, Value).

test_increment_by_zero() ->
    %% Set initial value using increment (not gauge, as increment can only work on counters)
    flurm_metrics:increment(zero_test_counter, 10),
    _ = sys:get_state(flurm_metrics),

    %% Increment by 0
    flurm_metrics:increment(zero_test_counter, 0),
    _ = sys:get_state(flurm_metrics),

    {ok, Value} = flurm_metrics:get_metric(zero_test_counter),
    ?assertEqual(10, Value).

test_increment_by_negative() ->
    %% Set initial value
    flurm_metrics:increment(neg_test_counter, 10),
    _ = sys:get_state(flurm_metrics),

    %% Increment by negative (behavior varies - may add or subtract)
    flurm_metrics:increment(neg_test_counter, -5),
    _ = sys:get_state(flurm_metrics),

    {ok, Value} = flurm_metrics:get_metric(neg_test_counter),
    %% Value should be 10 + (-5) = 5
    ?assertEqual(5, Value).

test_multiple_counters() ->
    %% Create multiple counters
    flurm_metrics:increment(counter_a, 10),
    flurm_metrics:increment(counter_b, 20),
    flurm_metrics:increment(counter_c, 30),
    _ = sys:get_state(flurm_metrics),

    %% Each should be independent
    {ok, A} = flurm_metrics:get_metric(counter_a),
    {ok, B} = flurm_metrics:get_metric(counter_b),
    {ok, C} = flurm_metrics:get_metric(counter_c),

    ?assertEqual(10, A),
    ?assertEqual(20, B),
    ?assertEqual(30, C).

%%====================================================================
%% Advanced Gauge Tests
%%====================================================================

advanced_gauge_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"gauge can be negative", fun test_gauge_negative/0},
      {"gauge can be float", fun test_gauge_float/0},
      {"gauge overwrites previous value", fun test_gauge_overwrite/0},
      {"decrement gauge", fun test_decrement_gauge/0}
     ]}.

test_gauge_negative() ->
    flurm_metrics:gauge(negative_gauge, -50),
    _ = sys:get_state(flurm_metrics),
    {ok, Value} = flurm_metrics:get_metric(negative_gauge),
    ?assertEqual(-50, Value).

test_gauge_float() ->
    flurm_metrics:gauge(float_gauge, 3.14159),
    _ = sys:get_state(flurm_metrics),
    {ok, Value} = flurm_metrics:get_metric(float_gauge),
    ?assertEqual(3.14159, Value).

test_gauge_overwrite() ->
    flurm_metrics:gauge(overwrite_gauge, 100),
    _ = sys:get_state(flurm_metrics),

    flurm_metrics:gauge(overwrite_gauge, 200),
    _ = sys:get_state(flurm_metrics),

    flurm_metrics:gauge(overwrite_gauge, 50),
    _ = sys:get_state(flurm_metrics),

    {ok, Value} = flurm_metrics:get_metric(overwrite_gauge),
    ?assertEqual(50, Value).

test_decrement_gauge() ->
    %% Set as gauge
    flurm_metrics:gauge(decrement_gauge_test, 100),
    _ = sys:get_state(flurm_metrics),

    %% Decrement should work on gauges too
    flurm_metrics:decrement(decrement_gauge_test, 30),
    _ = sys:get_state(flurm_metrics),

    {ok, Value} = flurm_metrics:get_metric(decrement_gauge_test),
    ?assertEqual(70, Value).

%%====================================================================
%% Advanced Histogram Tests
%%====================================================================

advanced_histogram_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"histogram bucket distribution", fun test_histogram_buckets/0},
      {"histogram with large values", fun test_histogram_large_values/0},
      {"histogram observe alias", fun test_histogram_observe/0},
      {"multiple histograms independent", fun test_multiple_histograms/0},
      {"histogram with zero value", fun test_histogram_zero/0}
     ]}.

test_histogram_buckets() ->
    %% Record values that fall into different buckets
    %% Buckets: 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, infinity
    flurm_metrics:histogram(bucket_test_hist, 3),    % Falls into 5 bucket
    flurm_metrics:histogram(bucket_test_hist, 15),   % Falls into 25 bucket
    flurm_metrics:histogram(bucket_test_hist, 75),   % Falls into 100 bucket
    flurm_metrics:histogram(bucket_test_hist, 300),  % Falls into 500 bucket
    _ = sys:get_state(flurm_metrics),

    AllMetrics = flurm_metrics:get_all_metrics(),
    HistData = maps:get(bucket_test_hist, AllMetrics),

    ?assertEqual(4, maps:get(count, HistData)),
    ?assertEqual(393.0, maps:get(sum, HistData)).

test_histogram_large_values() ->
    %% Record values larger than all buckets
    flurm_metrics:histogram(large_hist, 50000),
    flurm_metrics:histogram(large_hist, 100000),
    _ = sys:get_state(flurm_metrics),

    AllMetrics = flurm_metrics:get_all_metrics(),
    HistData = maps:get(large_hist, AllMetrics),

    ?assertEqual(2, maps:get(count, HistData)),
    ?assertEqual(150000.0, maps:get(sum, HistData)).

test_histogram_observe() ->
    %% observe is an alias for histogram
    flurm_metrics:observe(observe_test_hist, 100),
    flurm_metrics:observe(observe_test_hist, 200),
    _ = sys:get_state(flurm_metrics),

    AllMetrics = flurm_metrics:get_all_metrics(),
    HistData = maps:get(observe_test_hist, AllMetrics),

    ?assertEqual(2, maps:get(count, HistData)),
    ?assertEqual(300.0, maps:get(sum, HistData)).

test_multiple_histograms() ->
    flurm_metrics:histogram(hist_a, 10),
    flurm_metrics:histogram(hist_b, 20),
    flurm_metrics:histogram(hist_a, 30),
    _ = sys:get_state(flurm_metrics),

    AllMetrics = flurm_metrics:get_all_metrics(),

    HistA = maps:get(hist_a, AllMetrics),
    HistB = maps:get(hist_b, AllMetrics),

    ?assertEqual(2, maps:get(count, HistA)),
    ?assertEqual(40.0, maps:get(sum, HistA)),

    ?assertEqual(1, maps:get(count, HistB)),
    ?assertEqual(20.0, maps:get(sum, HistB)).

test_histogram_zero() ->
    flurm_metrics:histogram(zero_hist, 0),
    flurm_metrics:histogram(zero_hist, 0),
    flurm_metrics:histogram(zero_hist, 0),
    _ = sys:get_state(flurm_metrics),

    AllMetrics = flurm_metrics:get_all_metrics(),
    HistData = maps:get(zero_hist, AllMetrics),

    ?assertEqual(3, maps:get(count, HistData)),
    ?assertEqual(0.0, maps:get(sum, HistData)).

%%====================================================================
%% Labeled Metrics Tests (TRES)
%%====================================================================

labeled_metrics_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"labeled gauge", fun test_labeled_gauge/0},
      {"labeled counter", fun test_labeled_counter/0},
      {"get labeled metric", fun test_get_labeled_metric/0},
      {"labeled metrics in prometheus format", fun test_labeled_prometheus/0},
      {"multiple labels", fun test_multiple_labels/0}
     ]}.

test_labeled_gauge() ->
    %% Set labeled gauge
    flurm_metrics:labeled_gauge(flurm_tres_total, #{type => <<"cpu">>}, 100),
    _ = sys:get_state(flurm_metrics),

    %% Get labeled metric
    {ok, Value} = flurm_metrics:get_labeled_metric(flurm_tres_total, #{type => <<"cpu">>}),
    ?assertEqual(100, Value).

test_labeled_counter() ->
    %% Increment labeled counter
    flurm_metrics:labeled_counter(flurm_tres_allocated, #{type => <<"cpu">>}, 10),
    _ = sys:get_state(flurm_metrics),

    {ok, Value1} = flurm_metrics:get_labeled_metric(flurm_tres_allocated, #{type => <<"cpu">>}),
    ?assertEqual(10, Value1),

    %% Increment again
    flurm_metrics:labeled_counter(flurm_tres_allocated, #{type => <<"cpu">>}, 5),
    _ = sys:get_state(flurm_metrics),

    {ok, Value2} = flurm_metrics:get_labeled_metric(flurm_tres_allocated, #{type => <<"cpu">>}),
    ?assertEqual(15, Value2).

test_get_labeled_metric() ->
    %% Set values for different labels
    flurm_metrics:labeled_gauge(tres_test, #{type => <<"cpu">>}, 100),
    flurm_metrics:labeled_gauge(tres_test, #{type => <<"mem">>}, 200),
    flurm_metrics:labeled_gauge(tres_test, #{type => <<"gpu">>}, 300),
    _ = sys:get_state(flurm_metrics),

    %% Get each
    {ok, Cpu} = flurm_metrics:get_labeled_metric(tres_test, #{type => <<"cpu">>}),
    {ok, Mem} = flurm_metrics:get_labeled_metric(tres_test, #{type => <<"mem">>}),
    {ok, Gpu} = flurm_metrics:get_labeled_metric(tres_test, #{type => <<"gpu">>}),

    ?assertEqual(100, Cpu),
    ?assertEqual(200, Mem),
    ?assertEqual(300, Gpu),

    %% Non-existent label
    Result = flurm_metrics:get_labeled_metric(tres_test, #{type => <<"nonexistent">>}),
    ?assertEqual({error, not_found}, Result).

test_labeled_prometheus() ->
    %% Set labeled metrics
    flurm_metrics:labeled_gauge(prom_labeled_test, #{type => <<"cpu">>}, 100),
    flurm_metrics:labeled_gauge(prom_labeled_test, #{type => <<"mem">>}, 200),
    _ = sys:get_state(flurm_metrics),

    %% Get Prometheus format
    Output = flurm_metrics:format_prometheus(),
    OutputStr = lists:flatten(Output),

    %% Should contain labeled format with curly braces
    ?assert(string:find(OutputStr, "prom_labeled_test") =/= nomatch).

test_multiple_labels() ->
    %% Set with multiple labels
    flurm_metrics:labeled_gauge(multi_label_test, #{type => <<"gpu">>, name => <<"a100">>}, 8),
    flurm_metrics:labeled_gauge(multi_label_test, #{type => <<"gpu">>, name => <<"v100">>}, 4),
    _ = sys:get_state(flurm_metrics),

    {ok, A100} = flurm_metrics:get_labeled_metric(multi_label_test, #{type => <<"gpu">>, name => <<"a100">>}),
    {ok, V100} = flurm_metrics:get_labeled_metric(multi_label_test, #{type => <<"gpu">>, name => <<"v100">>}),

    ?assertEqual(8, A100),
    ?assertEqual(4, V100).

%%====================================================================
%% Prometheus Format Tests
%%====================================================================

prometheus_format_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"histogram in prometheus format", fun test_histogram_prometheus/0},
      {"counter in prometheus format", fun test_counter_prometheus/0},
      {"gauge in prometheus format", fun test_gauge_prometheus/0},
      {"float values formatted correctly", fun test_float_prometheus/0}
     ]}.

test_histogram_prometheus() ->
    %% Create histogram
    flurm_metrics:histogram(prom_hist_test, 10),
    flurm_metrics:histogram(prom_hist_test, 100),
    _ = sys:get_state(flurm_metrics),

    Output = flurm_metrics:format_prometheus(),
    OutputStr = lists:flatten(Output),

    %% Should have bucket format
    ?assert(string:find(OutputStr, "_bucket{le=") =/= nomatch),

    %% Should have _sum and _count
    ?assert(string:find(OutputStr, "_sum") =/= nomatch),
    ?assert(string:find(OutputStr, "_count") =/= nomatch).

test_counter_prometheus() ->
    flurm_metrics:increment(prom_counter_test, 42),
    _ = sys:get_state(flurm_metrics),

    Output = flurm_metrics:format_prometheus(),
    OutputStr = lists:flatten(Output),

    ?assert(string:find(OutputStr, "prom_counter_test") =/= nomatch),
    ?assert(string:find(OutputStr, "# TYPE prom_counter_test counter") =/= nomatch).

test_gauge_prometheus() ->
    flurm_metrics:gauge(prom_gauge_test, 99),
    _ = sys:get_state(flurm_metrics),

    Output = flurm_metrics:format_prometheus(),
    OutputStr = lists:flatten(Output),

    ?assert(string:find(OutputStr, "prom_gauge_test") =/= nomatch),
    ?assert(string:find(OutputStr, "# TYPE prom_gauge_test gauge") =/= nomatch).

test_float_prometheus() ->
    flurm_metrics:gauge(float_prom_test, 3.14159),
    _ = sys:get_state(flurm_metrics),

    Output = flurm_metrics:format_prometheus(),
    OutputStr = lists:flatten(Output),

    %% Should contain the float value
    ?assert(string:find(OutputStr, "3.14") =/= nomatch).

%%====================================================================
%% Collect Metrics Tests
%%====================================================================

collect_metrics_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"collect_tres_metrics", fun test_collect_tres_metrics/0}
     ]}.

test_collect_tres_metrics() ->
    %% Call collect_tres_metrics (this may require mocked dependencies)
    Result = flurm_metrics:collect_tres_metrics(),

    %% Should return ok (even if no data available)
    ?assertEqual(ok, Result).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"get non-existent metric", fun test_get_nonexistent/0},
      {"unknown call handled", fun test_unknown_call/0},
      {"unknown cast handled", fun test_unknown_cast/0},
      {"unknown info handled", fun test_unknown_info/0},
      {"code change handled", fun test_code_change/0}
     ]}.

test_get_nonexistent() ->
    Result = flurm_metrics:get_metric(nonexistent_metric_12345),
    ?assertEqual({error, not_found}, Result).

test_unknown_call() ->
    Result = gen_server:call(flurm_metrics, {unknown_request, data}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    gen_server:cast(flurm_metrics, {unknown_cast, data}),
    _ = sys:get_state(flurm_metrics),

    %% Server should still be running
    ?assert(is_pid(whereis(flurm_metrics))).

test_unknown_info() ->
    flurm_metrics ! {unknown_info, data},
    _ = sys:get_state(flurm_metrics),

    %% Server should still be running
    ?assert(is_pid(whereis(flurm_metrics))).

test_code_change() ->
    %% Suspend server
    ok = sys:suspend(flurm_metrics),

    %% Trigger code change
    Result = sys:change_code(flurm_metrics, flurm_metrics, old_vsn, extra),
    ?assertEqual(ok, Result),

    %% Resume server
    ok = sys:resume(flurm_metrics),

    %% Server should still work
    flurm_metrics:gauge(code_change_test, 123),
    _ = sys:get_state(flurm_metrics),
    {ok, Value} = flurm_metrics:get_metric(code_change_test),
    ?assertEqual(123, Value).

%%====================================================================
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    {"start_link handles already_started",
     {setup,
      fun() ->
          %% Ensure metrics is not running
          catch gen_server:stop(flurm_metrics, shutdown, 1000),
          timer:sleep(100),
          %% Start fresh
          {ok, Pid} = flurm_metrics:start_link(),
          Pid
      end,
      fun(Pid) ->
          catch gen_server:stop(Pid, shutdown, 5000)
      end,
      fun(Pid) ->
          [
           {"returns ok with existing pid", fun() ->
               %% Try to start another - should return the existing one
               Result = flurm_metrics:start_link(),
               ?assertEqual({ok, Pid}, Result)
           end}
          ]
      end}}.

%%====================================================================
%% Terminate Tests
%%====================================================================

terminate_test_() ->
    {"Terminate cancels timer",
     {setup,
      fun() ->
          catch gen_server:stop(flurm_metrics, shutdown, 1000),
          timer:sleep(100),
          {ok, Pid} = flurm_metrics:start_link(),
          unlink(Pid),
          Pid
      end,
      fun(_Pid) -> ok end,
      fun(Pid) ->
          [
           {"terminate cleans up", fun() ->
               gen_server:stop(Pid, normal, 5000),
               ?assertEqual(undefined, whereis(flurm_metrics))
           end}
          ]
      end}}.

%%====================================================================
%% Help Text Tests
%%====================================================================

help_text_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"all standard metrics have help text", fun test_all_help_text/0}
     ]}.

test_all_help_text() ->
    Output = flurm_metrics:format_prometheus(),
    OutputStr = lists:flatten(Output),

    %% All standard metrics should have HELP text
    StandardMetrics = [
        "flurm_jobs_submitted_total",
        "flurm_jobs_completed_total",
        "flurm_jobs_failed_total",
        "flurm_jobs_pending",
        "flurm_jobs_running",
        "flurm_nodes_total",
        "flurm_cpus_total"
    ],

    lists:foreach(fun(Metric) ->
        HelpLine = "# HELP " ++ Metric,
        ?assert(string:find(OutputStr, HelpLine) =/= nomatch, Metric)
    end, StandardMetrics).

%%====================================================================
%% Collect Timer Tests
%%====================================================================

collect_timer_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"collect_metrics message triggers collection", fun test_collect_message/0}
     ]}.

test_collect_message() ->
    %% Send collect_metrics message directly
    flurm_metrics ! collect_metrics,
    _ = sys:get_state(flurm_metrics),

    %% Server should still be running
    ?assert(is_pid(whereis(flurm_metrics))).

%%====================================================================
%% Format Label Tests
%%====================================================================

format_label_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"empty labels format correctly", fun test_empty_labels/0},
      {"atom labels format correctly", fun test_atom_labels/0},
      {"integer labels format correctly", fun test_integer_labels/0}
     ]}.

test_empty_labels() ->
    %% Empty labels should produce no label string
    flurm_metrics:labeled_gauge(empty_label_test, #{}, 100),
    _ = sys:get_state(flurm_metrics),

    Output = flurm_metrics:format_prometheus(),
    OutputStr = lists:flatten(Output),

    %% Should have the metric but without label braces (or empty braces)
    ?assert(string:find(OutputStr, "empty_label_test") =/= nomatch).

test_atom_labels() ->
    %% Atom label values
    flurm_metrics:labeled_gauge(atom_label_test, #{type => cpu}, 50),
    _ = sys:get_state(flurm_metrics),

    {ok, Value} = flurm_metrics:get_labeled_metric(atom_label_test, #{type => cpu}),
    ?assertEqual(50, Value).

test_integer_labels() ->
    %% Integer label values
    flurm_metrics:labeled_gauge(int_label_test, #{node_id => 42}, 75),
    _ = sys:get_state(flurm_metrics),

    {ok, Value} = flurm_metrics:get_labeled_metric(int_label_test, #{node_id => 42}),
    ?assertEqual(75, Value).

%%====================================================================
%% Federation Metrics Tests
%%====================================================================

federation_metrics_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"federation metrics initialized", fun test_federation_metrics_init/0}
     ]}.

test_federation_metrics_init() ->
    %% Federation metrics should be initialized
    AllMetrics = flurm_metrics:get_all_metrics(),

    ?assert(maps:is_key(flurm_federation_clusters_total, AllMetrics)),
    ?assert(maps:is_key(flurm_federation_jobs_submitted_total, AllMetrics)),
    ?assert(maps:is_key(flurm_federation_routing_decisions_total, AllMetrics)).

%%====================================================================
%% Rate Limiter Metrics Tests
%%====================================================================

rate_limiter_metrics_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"rate limiter metrics initialized", fun test_rate_limiter_metrics_init/0}
     ]}.

test_rate_limiter_metrics_init() ->
    AllMetrics = flurm_metrics:get_all_metrics(),

    ?assert(maps:is_key(flurm_requests_total, AllMetrics)),
    ?assert(maps:is_key(flurm_requests_rejected_total, AllMetrics)),
    ?assert(maps:is_key(flurm_backpressure_events_total, AllMetrics)).
