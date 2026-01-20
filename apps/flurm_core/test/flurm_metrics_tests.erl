%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_metrics module
%%%-------------------------------------------------------------------
-module(flurm_metrics_tests).
-include_lib("eunit/include/eunit.hrl").

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
%% Test Cases
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
