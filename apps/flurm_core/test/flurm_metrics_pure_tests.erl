%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_metrics module
%%%
%%% Tests all exported functions directly without mocking.
%%% Tests gen_server callbacks (init/1, handle_call/3, handle_cast/2,
%%% handle_info/2, terminate/2, code_change/3) directly.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_metrics_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% Define the state record to match the module
-record(state, {
    collect_timer :: reference() | undefined
}).

%% Define ETS table names
-define(METRICS_TABLE, flurm_metrics).
-define(HISTOGRAM_TABLE, flurm_histograms).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup function - creates ETS tables needed for testing
setup() ->
    %% Clean up any existing tables first
    catch ets:delete(?METRICS_TABLE),
    catch ets:delete(?HISTOGRAM_TABLE),
    ok.

%% Cleanup function
cleanup(_) ->
    catch ets:delete(?METRICS_TABLE),
    catch ets:delete(?HISTOGRAM_TABLE),
    ok.

%% Initialize and return state for tests that need it
init_state() ->
    setup(),
    {ok, State} = flurm_metrics:init([]),
    erlang:cancel_timer(State#state.collect_timer),
    State#state{collect_timer = undefined}.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"init creates ETS tables and returns proper state",
       fun() ->
           {ok, State} = flurm_metrics:init([]),
           %% Verify tables exist
           ?assertEqual(?METRICS_TABLE, ets:info(?METRICS_TABLE, name)),
           ?assertEqual(?HISTOGRAM_TABLE, ets:info(?HISTOGRAM_TABLE, name)),
           %% Verify state has timer
           ?assert(is_reference(State#state.collect_timer)),
           %% Cancel the timer to avoid side effects
           erlang:cancel_timer(State#state.collect_timer)
       end},
      {"init initializes standard metrics",
       fun() ->
           {ok, State} = flurm_metrics:init([]),
           %% Check some standard job counters
           [{flurm_jobs_submitted_total, counter, 0}] =
               ets:lookup(?METRICS_TABLE, flurm_jobs_submitted_total),
           [{flurm_jobs_completed_total, counter, 0}] =
               ets:lookup(?METRICS_TABLE, flurm_jobs_completed_total),
           [{flurm_jobs_failed_total, counter, 0}] =
               ets:lookup(?METRICS_TABLE, flurm_jobs_failed_total),
           %% Check some standard gauges
           [{flurm_jobs_pending, gauge, 0}] =
               ets:lookup(?METRICS_TABLE, flurm_jobs_pending),
           [{flurm_nodes_total, gauge, 0}] =
               ets:lookup(?METRICS_TABLE, flurm_nodes_total),
           %% Cleanup timer
           erlang:cancel_timer(State#state.collect_timer)
       end},
      {"init starts collect timer",
       fun() ->
           {ok, State} = flurm_metrics:init([]),
           Timer = State#state.collect_timer,
           ?assert(is_reference(Timer)),
           %% Timer should be active
           ?assertNotEqual(false, erlang:cancel_timer(Timer))
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests
%%====================================================================

handle_call_format_prometheus_test() ->
    State = init_state(),
    %% Add some test data
    ets:insert(?METRICS_TABLE, {test_counter, counter, 42}),
    ets:insert(?METRICS_TABLE, {test_gauge, gauge, 100}),

    {reply, Output, NewState} = flurm_metrics:handle_call(format_prometheus, {self(), make_ref()}, State),

    ?assertEqual(State, NewState),
    ?assert(is_list(Output)),
    %% Output should contain our test metrics
    FlatOutput = lists:flatten(Output),
    ?assert(string:find(FlatOutput, "test_counter") =/= nomatch),
    ?assert(string:find(FlatOutput, "42") =/= nomatch),
    cleanup(ok).

handle_call_reset_test() ->
    State = init_state(),
    %% Add a custom metric
    ets:insert(?METRICS_TABLE, {custom_metric, counter, 999}),

    {reply, ok, NewState} = flurm_metrics:handle_call(reset, {self(), make_ref()}, State),

    ?assertEqual(State, NewState),
    %% Custom metric should be gone
    ?assertEqual([], ets:lookup(?METRICS_TABLE, custom_metric)),
    %% Standard metrics should be reinitialized
    [{flurm_jobs_submitted_total, counter, 0}] =
        ets:lookup(?METRICS_TABLE, flurm_jobs_submitted_total),
    cleanup(ok).

handle_call_unknown_test() ->
    State = init_state(),
    {reply, Reply, NewState} = flurm_metrics:handle_call(unknown_request, {self(), make_ref()}, State),
    ?assertEqual({error, unknown_request}, Reply),
    ?assertEqual(State, NewState),
    cleanup(ok).

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_increment_new_test() ->
    State = init_state(),
    {noreply, NewState} = flurm_metrics:handle_cast({increment, new_counter, 5}, State),
    ?assertEqual(State, NewState),
    [{new_counter, counter, 5}] = ets:lookup(?METRICS_TABLE, new_counter),
    cleanup(ok).

handle_cast_increment_existing_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {existing_counter, counter, 10}),
    {noreply, _} = flurm_metrics:handle_cast({increment, existing_counter, 7}, State),
    [{existing_counter, counter, 17}] = ets:lookup(?METRICS_TABLE, existing_counter),
    cleanup(ok).

handle_cast_decrement_counter_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {dec_counter, counter, 10}),
    {noreply, _} = flurm_metrics:handle_cast({decrement, dec_counter, 3}, State),
    [{dec_counter, counter, 7}] = ets:lookup(?METRICS_TABLE, dec_counter),
    cleanup(ok).

handle_cast_decrement_gauge_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {dec_gauge, gauge, 100}),
    {noreply, _} = flurm_metrics:handle_cast({decrement, dec_gauge, 25}, State),
    [{dec_gauge, gauge, 75}] = ets:lookup(?METRICS_TABLE, dec_gauge),
    cleanup(ok).

handle_cast_decrement_nonexistent_test() ->
    State = init_state(),
    {noreply, _} = flurm_metrics:handle_cast({decrement, nonexistent, 5}, State),
    [{nonexistent, counter, 0}] = ets:lookup(?METRICS_TABLE, nonexistent),
    cleanup(ok).

handle_cast_decrement_floor_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {floor_counter, counter, 3}),
    {noreply, _} = flurm_metrics:handle_cast({decrement, floor_counter, 10}, State),
    [{floor_counter, counter, 0}] = ets:lookup(?METRICS_TABLE, floor_counter),
    cleanup(ok).

handle_cast_gauge_test() ->
    State = init_state(),
    {noreply, _} = flurm_metrics:handle_cast({gauge, my_gauge, 42.5}, State),
    [{my_gauge, gauge, 42.5}] = ets:lookup(?METRICS_TABLE, my_gauge),
    cleanup(ok).

handle_cast_histogram_test() ->
    State = init_state(),
    %% First observation
    {noreply, _} = flurm_metrics:handle_cast({histogram, flurm_scheduler_duration_ms, 50}, State),
    [{flurm_scheduler_duration_ms, Buckets1, Sum1, Count1}] =
        ets:lookup(?HISTOGRAM_TABLE, flurm_scheduler_duration_ms),
    ?assertEqual(50.0, Sum1),
    ?assertEqual(1, Count1),
    %% Buckets >= 50 should have count 1
    ?assertEqual(1, maps:get(50, Buckets1)),
    ?assertEqual(1, maps:get(100, Buckets1)),
    ?assertEqual(1, maps:get(infinity, Buckets1)),
    %% Buckets < 50 should have count 0
    ?assertEqual(0, maps:get(25, Buckets1)),
    ?assertEqual(0, maps:get(10, Buckets1)),
    cleanup(ok).

handle_cast_histogram_new_test() ->
    State = init_state(),
    %% Create a new histogram that doesn't exist
    {noreply, _} = flurm_metrics:handle_cast({histogram, brand_new_histogram, 100}, State),
    [{brand_new_histogram, Buckets, Sum, Count}] =
        ets:lookup(?HISTOGRAM_TABLE, brand_new_histogram),
    ?assertEqual(100.0, Sum),
    ?assertEqual(1, Count),
    ?assert(maps:is_key(infinity, Buckets)),
    cleanup(ok).

handle_cast_unknown_test() ->
    State = init_state(),
    {noreply, NewState} = flurm_metrics:handle_cast(unknown_message, State),
    ?assertEqual(State, NewState),
    cleanup(ok).

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_collect_metrics_test() ->
    State = init_state(),
    {noreply, NewState} = flurm_metrics:handle_info(collect_metrics, State),
    %% A new timer should be scheduled
    ?assert(is_reference(NewState#state.collect_timer)),
    %% Cancel the new timer
    erlang:cancel_timer(NewState#state.collect_timer),
    cleanup(ok).

handle_info_unknown_test() ->
    State = init_state(),
    {noreply, NewState} = flurm_metrics:handle_info(unknown_info, State),
    ?assertEqual(State, NewState),
    cleanup(ok).

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_with_timer_test() ->
    Timer = erlang:send_after(60000, self(), test),
    State = #state{collect_timer = Timer},
    ?assertEqual(ok, flurm_metrics:terminate(normal, State)),
    %% Timer should be cancelled
    ?assertEqual(false, erlang:cancel_timer(Timer)).

terminate_without_timer_test() ->
    State = #state{collect_timer = undefined},
    ?assertEqual(ok, flurm_metrics:terminate(normal, State)).

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_test() ->
    State = #state{collect_timer = undefined},
    ?assertEqual({ok, State}, flurm_metrics:code_change("1.0.0", State, [])).

%%====================================================================
%% get_metric/1 Tests (Direct ETS access)
%%====================================================================

get_metric_existing_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {test_metric, counter, 123}),
    ?assertEqual({ok, 123}, flurm_metrics:get_metric(test_metric)),
    cleanup(ok).

get_metric_nonexistent_test() ->
    State = init_state(),
    _ = State, %% Avoid unused variable warning
    ?assertEqual({error, not_found}, flurm_metrics:get_metric(does_not_exist)),
    cleanup(ok).

%%====================================================================
%% get_all_metrics/0 Tests
%%====================================================================

get_all_metrics_test() ->
    State = init_state(),
    _ = State, %% Avoid unused variable warning
    %% Add a custom counter
    ets:insert(?METRICS_TABLE, {custom_counter, counter, 42}),

    AllMetrics = flurm_metrics:get_all_metrics(),

    %% Should contain custom counter
    ?assertEqual(#{type => counter, value => 42}, maps:get(custom_counter, AllMetrics)),
    %% Should contain standard metrics
    ?assert(maps:is_key(flurm_jobs_submitted_total, AllMetrics)),
    %% Should contain histograms
    HistogramData = maps:get(flurm_scheduler_duration_ms, AllMetrics),
    ?assertEqual(histogram, maps:get(type, HistogramData)),
    ?assert(maps:is_key(buckets, HistogramData)),
    ?assert(maps:is_key(sum, HistogramData)),
    ?assert(maps:is_key(count, HistogramData)),
    cleanup(ok).

%%====================================================================
%% observe/2 Tests (alias for histogram)
%%====================================================================

observe_alias_test() ->
    State = init_state(),
    %% observe should work the same as histogram
    %% Since both send a cast, we test via handle_cast directly
    {noreply, _} = flurm_metrics:handle_cast({histogram, observe_test_metric, 75}, State),
    [{observe_test_metric, Buckets, Sum, Count}] =
        ets:lookup(?HISTOGRAM_TABLE, observe_test_metric),
    ?assertEqual(75.0, Sum),
    ?assertEqual(1, Count),
    ?assert(maps:is_key(100, Buckets)),
    cleanup(ok).

%%====================================================================
%% Histogram Bucket Distribution Tests
%%====================================================================

histogram_distribution_test() ->
    State = init_state(),
    %% Value of 5 should go into buckets 5, 10, 25, 50, 100, ... infinity
    {noreply, _} = flurm_metrics:handle_cast({histogram, dist_test, 5}, State),
    [{dist_test, Buckets, _, _}] = ets:lookup(?HISTOGRAM_TABLE, dist_test),

    %% Value 5 is <= 5, so bucket 5 should be 1
    ?assertEqual(1, maps:get(5, Buckets)),
    %% Value 5 is <= 10, so bucket 10 should be 1
    ?assertEqual(1, maps:get(10, Buckets)),
    %% Value 5 is > 1, so bucket 1 should be 0
    ?assertEqual(0, maps:get(1, Buckets)),
    cleanup(ok).

histogram_multiple_observations_test() ->
    State = init_state(),
    %% Add multiple observations
    {noreply, _} = flurm_metrics:handle_cast({histogram, multi_test, 10}, State),
    {noreply, _} = flurm_metrics:handle_cast({histogram, multi_test, 20}, State),
    {noreply, _} = flurm_metrics:handle_cast({histogram, multi_test, 100}, State),

    [{multi_test, Buckets, Sum, Count}] = ets:lookup(?HISTOGRAM_TABLE, multi_test),

    ?assertEqual(130.0, Sum),
    ?assertEqual(3, Count),
    %% Bucket 10: only value 10 fits
    ?assertEqual(1, maps:get(10, Buckets)),
    %% Bucket 25: values 10, 20 fit
    ?assertEqual(2, maps:get(25, Buckets)),
    %% Bucket 100: all values fit
    ?assertEqual(3, maps:get(100, Buckets)),
    %% Bucket infinity: all values fit
    ?assertEqual(3, maps:get(infinity, Buckets)),
    cleanup(ok).

%%====================================================================
%% Prometheus Format Tests
%%====================================================================

prometheus_format_structure_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {prom_test_counter, counter, 42}),

    {reply, Output, _} = flurm_metrics:handle_call(format_prometheus, {self(), make_ref()}, State),
    FlatOutput = lists:flatten(Output),

    %% Should contain HELP line
    ?assert(string:find(FlatOutput, "# HELP prom_test_counter") =/= nomatch),
    %% Should contain TYPE line
    ?assert(string:find(FlatOutput, "# TYPE prom_test_counter counter") =/= nomatch),
    %% Should contain the value
    ?assert(string:find(FlatOutput, "prom_test_counter 42") =/= nomatch),
    cleanup(ok).

prometheus_format_histogram_test() ->
    State = init_state(),
    %% Add a histogram observation
    {noreply, _} = flurm_metrics:handle_cast({histogram, flurm_scheduler_duration_ms, 50}, State),

    {reply, Output, _} = flurm_metrics:handle_call(format_prometheus, {self(), make_ref()}, State),
    FlatOutput = lists:flatten(Output),

    %% Should contain histogram type
    ?assert(string:find(FlatOutput, "# TYPE flurm_scheduler_duration_ms histogram") =/= nomatch),
    %% Should contain bucket lines
    ?assert(string:find(FlatOutput, "_bucket{le=") =/= nomatch),
    %% Should contain +Inf bucket
    ?assert(string:find(FlatOutput, "le=\"+Inf\"") =/= nomatch),
    %% Should contain _sum and _count
    ?assert(string:find(FlatOutput, "_sum") =/= nomatch),
    ?assert(string:find(FlatOutput, "_count") =/= nomatch),
    cleanup(ok).

prometheus_format_float_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {float_metric, gauge, 3.14159}),

    {reply, Output, _} = flurm_metrics:handle_call(format_prometheus, {self(), make_ref()}, State),
    FlatOutput = lists:flatten(Output),

    %% Float should be formatted with 6 decimal places
    ?assert(string:find(FlatOutput, "3.141590") =/= nomatch),
    cleanup(ok).

%%====================================================================
%% Edge Cases and Error Handling Tests
%%====================================================================

increment_zero_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {zero_test, counter, 10}),
    {noreply, _} = flurm_metrics:handle_cast({increment, zero_test, 0}, State),
    [{zero_test, counter, 10}] = ets:lookup(?METRICS_TABLE, zero_test),
    cleanup(ok).

increment_negative_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {neg_test, counter, 10}),
    {noreply, _} = flurm_metrics:handle_cast({increment, neg_test, -5}, State),
    %% This effectively decrements
    [{neg_test, counter, 5}] = ets:lookup(?METRICS_TABLE, neg_test),
    cleanup(ok).

gauge_negative_test() ->
    State = init_state(),
    {noreply, _} = flurm_metrics:handle_cast({gauge, neg_gauge, -100}, State),
    [{neg_gauge, gauge, -100}] = ets:lookup(?METRICS_TABLE, neg_gauge),
    cleanup(ok).

gauge_overwrite_test() ->
    State = init_state(),
    {noreply, _} = flurm_metrics:handle_cast({gauge, overwrite_gauge, 100}, State),
    {noreply, _} = flurm_metrics:handle_cast({gauge, overwrite_gauge, 200}, State),
    [{overwrite_gauge, gauge, 200}] = ets:lookup(?METRICS_TABLE, overwrite_gauge),
    cleanup(ok).

histogram_zero_test() ->
    State = init_state(),
    {noreply, _} = flurm_metrics:handle_cast({histogram, zero_hist, 0}, State),
    [{zero_hist, Buckets, Sum, Count}] = ets:lookup(?HISTOGRAM_TABLE, zero_hist),
    ?assertEqual(0.0, Sum),
    ?assertEqual(1, Count),
    %% All buckets should contain this value
    ?assertEqual(1, maps:get(1, Buckets)),
    ?assertEqual(1, maps:get(infinity, Buckets)),
    cleanup(ok).

histogram_large_value_test() ->
    State = init_state(),
    {noreply, _} = flurm_metrics:handle_cast({histogram, large_hist, 999999}, State),
    [{large_hist, Buckets, Sum, Count}] = ets:lookup(?HISTOGRAM_TABLE, large_hist),
    ?assertEqual(999999.0, Sum),
    ?assertEqual(1, Count),
    %% Only infinity bucket should contain this value
    ?assertEqual(0, maps:get(10000, Buckets)),
    ?assertEqual(1, maps:get(infinity, Buckets)),
    cleanup(ok).

%%====================================================================
%% Standard Metrics Initialization Tests
%%====================================================================

all_job_counters_initialized_test() ->
    State = init_state(),
    _ = State, %% Avoid unused variable warning
    Counters = [
        flurm_jobs_submitted_total,
        flurm_jobs_completed_total,
        flurm_jobs_failed_total,
        flurm_jobs_cancelled_total,
        flurm_jobs_preempted_total
    ],
    lists:foreach(fun(Name) ->
        [{Name, counter, 0}] = ets:lookup(?METRICS_TABLE, Name)
    end, Counters),
    cleanup(ok).

all_job_gauges_initialized_test() ->
    State = init_state(),
    _ = State, %% Avoid unused variable warning
    Gauges = [
        flurm_jobs_pending,
        flurm_jobs_running,
        flurm_jobs_suspended
    ],
    lists:foreach(fun(Name) ->
        [{Name, gauge, 0}] = ets:lookup(?METRICS_TABLE, Name)
    end, Gauges),
    cleanup(ok).

all_node_gauges_initialized_test() ->
    State = init_state(),
    _ = State, %% Avoid unused variable warning
    Gauges = [
        flurm_nodes_total,
        flurm_nodes_up,
        flurm_nodes_down,
        flurm_nodes_drain
    ],
    lists:foreach(fun(Name) ->
        [{Name, gauge, 0}] = ets:lookup(?METRICS_TABLE, Name)
    end, Gauges),
    cleanup(ok).

all_resource_gauges_initialized_test() ->
    State = init_state(),
    _ = State, %% Avoid unused variable warning
    Gauges = [
        flurm_cpus_total,
        flurm_cpus_allocated,
        flurm_cpus_idle,
        flurm_memory_total_mb,
        flurm_memory_allocated_mb
    ],
    lists:foreach(fun(Name) ->
        [{Name, gauge, 0}] = ets:lookup(?METRICS_TABLE, Name)
    end, Gauges),
    cleanup(ok).

all_scheduler_metrics_initialized_test() ->
    State = init_state(),
    _ = State, %% Avoid unused variable warning
    Counters = [
        flurm_scheduler_cycles_total,
        flurm_scheduler_backfill_jobs
    ],
    lists:foreach(fun(Name) ->
        [{Name, counter, 0}] = ets:lookup(?METRICS_TABLE, Name)
    end, Counters),
    cleanup(ok).

all_rate_limiting_metrics_initialized_test() ->
    State = init_state(),
    _ = State, %% Avoid unused variable warning
    Counters = [
        flurm_requests_total,
        flurm_requests_rejected_total,
        flurm_backpressure_events_total
    ],
    lists:foreach(fun(Name) ->
        [{Name, counter, 0}] = ets:lookup(?METRICS_TABLE, Name)
    end, Counters),
    cleanup(ok).

all_histograms_initialized_test() ->
    State = init_state(),
    _ = State, %% Avoid unused variable warning
    Histograms = [
        flurm_scheduler_duration_ms,
        flurm_job_wait_time_seconds,
        flurm_job_run_time_seconds,
        flurm_request_duration_ms
    ],
    lists:foreach(fun(Name) ->
        [{Name, Buckets, Sum, Count}] = ets:lookup(?HISTOGRAM_TABLE, Name),
        ?assertEqual(0.0, Sum),
        ?assertEqual(0, Count),
        ?assert(maps:is_key(infinity, Buckets)),
        ?assert(maps:is_key(1, Buckets)),
        ?assert(maps:is_key(10000, Buckets))
    end, Histograms),
    cleanup(ok).

%%====================================================================
%% Concurrent Access Tests (ETS is thread-safe)
%%====================================================================

concurrent_increments_test() ->
    State = init_state(),
    %% Simulate multiple sequential increments (would be concurrent in real usage)
    lists:foreach(fun(I) ->
        {noreply, _} = flurm_metrics:handle_cast({increment, concurrent_counter, I}, State)
    end, lists:seq(1, 10)),

    [{concurrent_counter, counter, Total}] = ets:lookup(?METRICS_TABLE, concurrent_counter),
    %% Sum of 1 to 10 = 55
    ?assertEqual(55, Total),
    cleanup(ok).

%%====================================================================
%% Reset Behavior Tests
%%====================================================================

reset_clears_custom_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {custom1, counter, 100}),
    ets:insert(?METRICS_TABLE, {custom2, gauge, 200}),

    {reply, ok, _} = flurm_metrics:handle_call(reset, {self(), make_ref()}, State),

    ?assertEqual([], ets:lookup(?METRICS_TABLE, custom1)),
    ?assertEqual([], ets:lookup(?METRICS_TABLE, custom2)),
    cleanup(ok).

reset_reinitializes_standard_test() ->
    State = init_state(),
    %% Modify a standard metric
    ets:insert(?METRICS_TABLE, {flurm_jobs_submitted_total, counter, 999}),

    {reply, ok, _} = flurm_metrics:handle_call(reset, {self(), make_ref()}, State),

    %% Should be back to 0
    [{flurm_jobs_submitted_total, counter, 0}] =
        ets:lookup(?METRICS_TABLE, flurm_jobs_submitted_total),
    cleanup(ok).

reset_clears_histograms_test() ->
    State = init_state(),
    %% Add observations to a histogram
    {noreply, _} = flurm_metrics:handle_cast({histogram, flurm_scheduler_duration_ms, 100}, State),
    {noreply, _} = flurm_metrics:handle_cast({histogram, flurm_scheduler_duration_ms, 200}, State),

    %% Reset
    {reply, ok, _} = flurm_metrics:handle_call(reset, {self(), make_ref()}, State),

    %% Histogram should be reinitialized
    [{flurm_scheduler_duration_ms, _, Sum, Count}] =
        ets:lookup(?HISTOGRAM_TABLE, flurm_scheduler_duration_ms),
    ?assertEqual(0.0, Sum),
    ?assertEqual(0, Count),
    cleanup(ok).

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

%% Test decrement on gauge that goes to floor
decrement_gauge_floor_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {floor_gauge, gauge, 5}),
    {noreply, _} = flurm_metrics:handle_cast({decrement, floor_gauge, 20}, State),
    [{floor_gauge, gauge, 0}] = ets:lookup(?METRICS_TABLE, floor_gauge),
    cleanup(ok).

%% Test increment with float value
increment_float_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {float_counter, counter, 10.5}),
    {noreply, _} = flurm_metrics:handle_cast({increment, float_counter, 2.5}, State),
    [{float_counter, counter, 13.0}] = ets:lookup(?METRICS_TABLE, float_counter),
    cleanup(ok).

%% Test multiple gauge overwrites
multiple_gauge_overwrites_test() ->
    State = init_state(),
    lists:foreach(fun(V) ->
        {noreply, _} = flurm_metrics:handle_cast({gauge, multi_overwrite, V}, State)
    end, [1, 2, 3, 4, 5]),
    [{multi_overwrite, gauge, 5}] = ets:lookup(?METRICS_TABLE, multi_overwrite),
    cleanup(ok).

%% Test histogram with fractional value
histogram_fractional_test() ->
    State = init_state(),
    {noreply, _} = flurm_metrics:handle_cast({histogram, frac_hist, 7.5}, State),
    [{frac_hist, Buckets, Sum, Count}] = ets:lookup(?HISTOGRAM_TABLE, frac_hist),
    ?assertEqual(7.5, Sum),
    ?assertEqual(1, Count),
    %% 7.5 should be in bucket 10 but not 5
    ?assertEqual(0, maps:get(5, Buckets)),
    ?assertEqual(1, maps:get(10, Buckets)),
    cleanup(ok).

%% Test prometheus format with gauge type
prometheus_format_gauge_type_test() ->
    State = init_state(),
    ets:insert(?METRICS_TABLE, {prom_gauge_type, gauge, 999}),

    {reply, Output, _} = flurm_metrics:handle_call(format_prometheus, {self(), make_ref()}, State),
    FlatOutput = lists:flatten(Output),

    ?assert(string:find(FlatOutput, "# TYPE prom_gauge_type gauge") =/= nomatch),
    ?assert(string:find(FlatOutput, "prom_gauge_type 999") =/= nomatch),
    cleanup(ok).

%% Test terminate with different reasons
terminate_shutdown_test() ->
    State = #state{collect_timer = undefined},
    ?assertEqual(ok, flurm_metrics:terminate(shutdown, State)).

terminate_abnormal_test() ->
    Timer = erlang:send_after(60000, self(), test),
    State = #state{collect_timer = Timer},
    ?assertEqual(ok, flurm_metrics:terminate({error, some_reason}, State)),
    %% Timer should be cancelled
    ?assertEqual(false, erlang:cancel_timer(Timer)).

%% Test code_change with different versions
code_change_upgrade_test() ->
    State = #state{collect_timer = make_ref()},
    ?assertEqual({ok, State}, flurm_metrics:code_change("0.9.0", State, {upgrade, "1.0.0"})).

code_change_downgrade_test() ->
    State = #state{collect_timer = undefined},
    ?assertEqual({ok, State}, flurm_metrics:code_change("2.0.0", State, {downgrade, "1.0.0"})).
