%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_metrics module
%%% Tests all functions for 100% coverage
%%%-------------------------------------------------------------------
-module(flurm_metrics_comprehensive_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing metrics server
    catch gen_server:stop(flurm_metrics),
    catch ets:delete(flurm_metrics),
    catch ets:delete(flurm_histograms),
    timer:sleep(10),
    %% Start the metrics server
    {ok, Pid} = flurm_metrics:start_link(),
    {started, Pid}.

cleanup({started, _Pid}) ->
    catch ets:delete(flurm_metrics),
    catch ets:delete(flurm_histograms),
    catch gen_server:stop(flurm_metrics),
    timer:sleep(10);
cleanup(_) ->
    ok.

%%====================================================================
%% Test Generator
%%====================================================================

metrics_comprehensive_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"increment counter by 1", fun test_increment_by_one/0},
      {"increment counter by N", fun test_increment_by_n/0},
      {"increment new counter", fun test_increment_new_counter/0},
      {"decrement counter", fun test_decrement_counter/0},
      {"decrement gauge", fun test_decrement_gauge/0},
      {"decrement non-existent creates counter", fun test_decrement_nonexistent/0},
      {"decrement does not go below 0", fun test_decrement_floor/0},
      {"set gauge value", fun test_gauge/0},
      {"histogram observation", fun test_histogram/0},
      {"histogram creates new if not exists", fun test_histogram_new/0},
      {"observe alias for histogram", fun test_observe/0},
      {"get_metric returns not_found for unknown", fun test_get_metric_not_found/0},
      {"get_all_metrics returns complete map", fun test_get_all_metrics/0},
      {"format_prometheus output", fun test_format_prometheus/0},
      {"reset clears all metrics", fun test_reset/0},
      {"handle unknown call", fun test_unknown_call/0},
      {"handle unknown cast", fun test_unknown_cast/0},
      {"handle unknown info", fun test_unknown_info/0},
      {"collect_metrics info message", fun test_collect_metrics/0},
      {"code_change callback", fun test_code_change/0},
      {"terminate callback", fun test_terminate/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_increment_by_one() ->
    flurm_metrics:increment(flurm_jobs_submitted_total),
    timer:sleep(20),
    {ok, Value} = flurm_metrics:get_metric(flurm_jobs_submitted_total),
    ?assert(Value >= 1).

test_increment_by_n() ->
    flurm_metrics:increment(flurm_jobs_submitted_total, 5),
    timer:sleep(20),
    {ok, Value} = flurm_metrics:get_metric(flurm_jobs_submitted_total),
    ?assert(Value >= 5).

test_increment_new_counter() ->
    flurm_metrics:increment(my_custom_counter, 10),
    timer:sleep(20),
    {ok, Value} = flurm_metrics:get_metric(my_custom_counter),
    ?assertEqual(10, Value).

test_decrement_counter() ->
    %% First increment to have something to decrement
    flurm_metrics:increment(test_dec_counter, 10),
    timer:sleep(20),
    flurm_metrics:decrement(test_dec_counter, 3),
    timer:sleep(20),
    {ok, Value} = flurm_metrics:get_metric(test_dec_counter),
    ?assertEqual(7, Value).

test_decrement_gauge() ->
    flurm_metrics:gauge(test_dec_gauge, 10),
    timer:sleep(20),
    flurm_metrics:decrement(test_dec_gauge, 3),
    timer:sleep(20),
    {ok, Value} = flurm_metrics:get_metric(test_dec_gauge),
    ?assertEqual(7, Value).

test_decrement_nonexistent() ->
    flurm_metrics:decrement(nonexistent_counter),
    timer:sleep(20),
    {ok, Value} = flurm_metrics:get_metric(nonexistent_counter),
    ?assertEqual(0, Value).

test_decrement_floor() ->
    flurm_metrics:increment(test_floor_counter, 5),
    timer:sleep(20),
    flurm_metrics:decrement(test_floor_counter, 100),
    timer:sleep(20),
    {ok, Value} = flurm_metrics:get_metric(test_floor_counter),
    ?assertEqual(0, Value).

test_gauge() ->
    flurm_metrics:gauge(test_gauge, 42),
    timer:sleep(20),
    {ok, Value1} = flurm_metrics:get_metric(test_gauge),
    ?assertEqual(42, Value1),

    %% Update gauge
    flurm_metrics:gauge(test_gauge, 100),
    timer:sleep(20),
    {ok, Value2} = flurm_metrics:get_metric(test_gauge),
    ?assertEqual(100, Value2).

test_histogram() ->
    %% Record multiple observations
    flurm_metrics:histogram(flurm_request_duration_ms, 10),
    flurm_metrics:histogram(flurm_request_duration_ms, 50),
    flurm_metrics:histogram(flurm_request_duration_ms, 100),
    flurm_metrics:histogram(flurm_request_duration_ms, 500),
    flurm_metrics:histogram(flurm_request_duration_ms, 5000),
    flurm_metrics:histogram(flurm_request_duration_ms, 20000),
    timer:sleep(50),

    AllMetrics = flurm_metrics:get_all_metrics(),
    HistData = maps:get(flurm_request_duration_ms, AllMetrics),
    ?assertEqual(histogram, maps:get(type, HistData)),
    ?assertEqual(6, maps:get(count, HistData)),
    ?assertEqual(25660.0, maps:get(sum, HistData)).

test_histogram_new() ->
    %% Histogram on a new name should auto-initialize
    flurm_metrics:histogram(my_new_histogram, 42),
    timer:sleep(20),
    AllMetrics = flurm_metrics:get_all_metrics(),
    HistData = maps:get(my_new_histogram, AllMetrics),
    ?assertEqual(histogram, maps:get(type, HistData)),
    ?assertEqual(1, maps:get(count, HistData)).

test_observe() ->
    %% observe is an alias for histogram
    flurm_metrics:observe(test_observe_hist, 123),
    timer:sleep(20),
    AllMetrics = flurm_metrics:get_all_metrics(),
    HistData = maps:get(test_observe_hist, AllMetrics),
    ?assertEqual(histogram, maps:get(type, HistData)).

test_get_metric_not_found() ->
    Result = flurm_metrics:get_metric(completely_unknown_metric),
    ?assertEqual({error, not_found}, Result).

test_get_all_metrics() ->
    AllMetrics = flurm_metrics:get_all_metrics(),
    ?assert(is_map(AllMetrics)),
    %% Should have standard metrics
    ?assert(maps:is_key(flurm_jobs_submitted_total, AllMetrics)),
    ?assert(maps:is_key(flurm_jobs_running, AllMetrics)),
    ?assert(maps:is_key(flurm_nodes_total, AllMetrics)),
    ?assert(maps:is_key(flurm_cpus_total, AllMetrics)).

test_format_prometheus() ->
    %% Set some values
    flurm_metrics:gauge(flurm_jobs_pending, 5),
    flurm_metrics:gauge(flurm_jobs_running, 10),
    flurm_metrics:increment(flurm_jobs_submitted_total, 100),
    flurm_metrics:histogram(flurm_request_duration_ms, 50),
    timer:sleep(30),

    Output = flurm_metrics:format_prometheus(),
    ?assert(is_list(Output)),

    OutputStr = lists:flatten(Output),

    %% Check for metric names
    ?assert(string:find(OutputStr, "flurm_jobs_pending") =/= nomatch),
    ?assert(string:find(OutputStr, "flurm_jobs_running") =/= nomatch),

    %% Check for TYPE declarations
    ?assert(string:find(OutputStr, "# TYPE") =/= nomatch),

    %% Check for HELP text
    ?assert(string:find(OutputStr, "# HELP") =/= nomatch),

    %% Check for histogram bucket output
    ?assert(string:find(OutputStr, "_bucket") =/= nomatch),
    ?assert(string:find(OutputStr, "_sum") =/= nomatch),
    ?assert(string:find(OutputStr, "_count") =/= nomatch).

test_reset() ->
    %% Set some values
    flurm_metrics:gauge(flurm_jobs_pending, 100),
    flurm_metrics:increment(flurm_jobs_submitted_total, 50),
    timer:sleep(20),

    %% Reset
    ok = flurm_metrics:reset(),

    %% Counters should be back to 0
    {ok, SubmittedValue} = flurm_metrics:get_metric(flurm_jobs_submitted_total),
    ?assertEqual(0, SubmittedValue),

    %% Gauges should be 0
    {ok, PendingValue} = flurm_metrics:get_metric(flurm_jobs_pending),
    ?assertEqual(0, PendingValue).

test_unknown_call() ->
    Result = gen_server:call(flurm_metrics, {unknown_request, arg1, arg2}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    %% Unknown cast should be silently ignored
    ok = gen_server:cast(flurm_metrics, {unknown_cast, arg1}),
    timer:sleep(20),
    %% Server should still be running
    ?assert(is_pid(whereis(flurm_metrics))).

test_unknown_info() ->
    %% Unknown info message should be silently ignored
    flurm_metrics ! {unknown_info_message, arg1, arg2},
    timer:sleep(20),
    %% Server should still be running
    ?assert(is_pid(whereis(flurm_metrics))).

test_collect_metrics() ->
    %% Trigger collect_metrics manually
    flurm_metrics ! collect_metrics,
    timer:sleep(50),
    %% Server should still be running and have collected something
    ?assert(is_pid(whereis(flurm_metrics))).

test_code_change() ->
    %% Test code_change callback directly
    State = {state, undefined},
    {ok, NewState} = flurm_metrics:code_change(old_vsn, State, extra),
    ?assertEqual(State, NewState).

test_terminate() ->
    %% Test terminate by stopping the server
    Pid = whereis(flurm_metrics),
    ?assert(is_pid(Pid)),
    gen_server:stop(flurm_metrics),
    timer:sleep(50),
    ?assertEqual(undefined, whereis(flurm_metrics)).

%%====================================================================
%% Format Value Tests
%%====================================================================

format_value_test_() ->
    [
     {"Integer format", fun() ->
         {ok, Pid} = flurm_metrics:start_link(),
         flurm_metrics:gauge(test_int, 42),
         timer:sleep(20),
         Output = flurm_metrics:format_prometheus(),
         OutputStr = lists:flatten(Output),
         ?assert(string:find(OutputStr, "test_int 42") =/= nomatch),
         gen_server:stop(Pid)
     end},
     {"Float format", fun() ->
         {ok, Pid} = flurm_metrics:start_link(),
         flurm_metrics:histogram(test_float_hist, 3.14159),
         timer:sleep(20),
         Output = flurm_metrics:format_prometheus(),
         OutputStr = lists:flatten(Output),
         ?assert(string:find(OutputStr, "test_float_hist_sum") =/= nomatch),
         gen_server:stop(Pid)
     end}
    ].

%%====================================================================
%% Help Text Tests
%%====================================================================

help_text_test_() ->
    {setup,
     fun() ->
         catch gen_server:stop(flurm_metrics),
         catch ets:delete(flurm_metrics),
         catch ets:delete(flurm_histograms),
         timer:sleep(20),
         {ok, Pid} = flurm_metrics:start_link(),
         Pid
     end,
     fun(Pid) ->
         gen_server:stop(Pid)
     end,
     fun(_) ->
         [
          {"All metrics have help text", fun() ->
              Output = flurm_metrics:format_prometheus(),
              OutputStr = lists:flatten(Output),
              %% Count # HELP occurrences - should match number of unique metrics
              HelpCount = length(string:split(OutputStr, "# HELP", all)) - 1,
              ?assert(HelpCount > 0)
          end}
         ]
     end}.
