%%%-------------------------------------------------------------------
%%% @doc FLURM Metrics Collection
%%%
%%% Collects and exposes metrics in Prometheus format for monitoring.
%%%
%%% Metrics categories:
%%% - Job metrics: submitted, running, completed, failed, cancelled
%%% - Node metrics: total, up, down, drain, allocated CPUs/memory
%%% - Scheduler metrics: cycles, duration, backfill count
%%% - Rate limiting metrics: requests, rejections, backpressure
%%% - Fairshare metrics: usage by user/account
%%%
%%% Metrics are exposed via HTTP endpoint for Prometheus scraping.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_metrics).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    increment/1,
    increment/2,
    decrement/1,
    decrement/2,
    gauge/2,
    histogram/2,
    observe/2,
    get_metric/1,
    get_all_metrics/0,
    format_prometheus/0,
    reset/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(METRICS_TABLE, flurm_metrics).
-define(HISTOGRAM_TABLE, flurm_histograms).

%% Collection interval for gauge metrics
-define(COLLECT_INTERVAL, 15000).  % 15 seconds

%% Histogram buckets for timing metrics (milliseconds)
-define(DURATION_BUCKETS, [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]).

-record(state, {
    collect_timer :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the metrics server.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    case gen_server:start_link({local, ?SERVER}, ?MODULE, [], []) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            %% Process already running - return existing pid
            %% This handles race conditions during startup and restarts
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Increment a counter metric.
-spec increment(atom()) -> ok.
increment(Name) ->
    increment(Name, 1).

%% @doc Increment a counter metric by N.
-spec increment(atom(), number()) -> ok.
increment(Name, N) ->
    gen_server:cast(?SERVER, {increment, Name, N}).

%% @doc Decrement a counter metric.
-spec decrement(atom()) -> ok.
decrement(Name) ->
    decrement(Name, 1).

%% @doc Decrement a counter metric by N.
-spec decrement(atom(), number()) -> ok.
decrement(Name, N) ->
    gen_server:cast(?SERVER, {decrement, Name, N}).

%% @doc Set a gauge metric to a specific value.
-spec gauge(atom(), number()) -> ok.
gauge(Name, Value) ->
    gen_server:cast(?SERVER, {gauge, Name, Value}).

%% @doc Record a histogram observation.
-spec histogram(atom(), number()) -> ok.
histogram(Name, Value) ->
    gen_server:cast(?SERVER, {histogram, Name, Value}).

%% @doc Alias for histogram (for timing observations).
-spec observe(atom(), number()) -> ok.
observe(Name, Value) ->
    histogram(Name, Value).

%% @doc Get a specific metric value.
-spec get_metric(atom()) -> {ok, number()} | {error, not_found}.
get_metric(Name) ->
    case ets:lookup(?METRICS_TABLE, Name) of
        [{Name, _Type, Value}] -> {ok, Value};
        [] -> {error, not_found}
    end.

%% @doc Get all metrics as a map.
-spec get_all_metrics() -> map().
get_all_metrics() ->
    Metrics = ets:foldl(
        fun({Name, Type, Value}, Acc) ->
            maps:put(Name, #{type => Type, value => Value}, Acc)
        end,
        #{},
        ?METRICS_TABLE
    ),
    Histograms = ets:foldl(
        fun({Name, Buckets, Sum, Count}, Acc) ->
            maps:put(Name, #{type => histogram, buckets => Buckets, sum => Sum, count => Count}, Acc)
        end,
        #{},
        ?HISTOGRAM_TABLE
    ),
    maps:merge(Metrics, Histograms).

%% @doc Format all metrics in Prometheus text format.
-spec format_prometheus() -> iolist().
format_prometheus() ->
    gen_server:call(?SERVER, format_prometheus).

%% @doc Reset all metrics.
-spec reset() -> ok.
reset() ->
    gen_server:call(?SERVER, reset).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables
    ets:new(?METRICS_TABLE, [named_table, public, set]),
    ets:new(?HISTOGRAM_TABLE, [named_table, public, set]),

    %% Initialize standard metrics
    init_standard_metrics(),

    %% Start collection timer
    Timer = erlang:send_after(?COLLECT_INTERVAL, self(), collect_metrics),

    {ok, #state{collect_timer = Timer}}.

handle_call(format_prometheus, _From, State) ->
    Output = do_format_prometheus(),
    {reply, Output, State};

handle_call(reset, _From, State) ->
    ets:delete_all_objects(?METRICS_TABLE),
    ets:delete_all_objects(?HISTOGRAM_TABLE),
    init_standard_metrics(),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({increment, Name, N}, State) ->
    case ets:lookup(?METRICS_TABLE, Name) of
        [{Name, counter, Value}] ->
            ets:insert(?METRICS_TABLE, {Name, counter, Value + N});
        [] ->
            ets:insert(?METRICS_TABLE, {Name, counter, N})
    end,
    {noreply, State};

handle_cast({decrement, Name, N}, State) ->
    case ets:lookup(?METRICS_TABLE, Name) of
        [{Name, counter, Value}] ->
            ets:insert(?METRICS_TABLE, {Name, counter, max(0, Value - N)});
        [{Name, gauge, Value}] ->
            ets:insert(?METRICS_TABLE, {Name, gauge, max(0, Value - N)});
        [] ->
            ets:insert(?METRICS_TABLE, {Name, counter, 0})
    end,
    {noreply, State};

handle_cast({gauge, Name, Value}, State) ->
    ets:insert(?METRICS_TABLE, {Name, gauge, Value}),
    {noreply, State};

handle_cast({histogram, Name, Value}, State) ->
    update_histogram(Name, Value),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(collect_metrics, State) ->
    collect_system_metrics(),
    Timer = erlang:send_after(?COLLECT_INTERVAL, self(), collect_metrics),
    {noreply, State#state{collect_timer = Timer}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.collect_timer of
        undefined -> ok;
        Timer -> erlang:cancel_timer(Timer)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Initialize standard metrics
init_standard_metrics() ->
    %% Job counters
    ets:insert(?METRICS_TABLE, {flurm_jobs_submitted_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_jobs_completed_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_jobs_failed_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_jobs_cancelled_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_jobs_preempted_total, counter, 0}),

    %% Job gauges
    ets:insert(?METRICS_TABLE, {flurm_jobs_pending, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_jobs_running, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_jobs_suspended, gauge, 0}),

    %% Node gauges
    ets:insert(?METRICS_TABLE, {flurm_nodes_total, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_nodes_up, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_nodes_down, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_nodes_drain, gauge, 0}),

    %% Resource gauges
    ets:insert(?METRICS_TABLE, {flurm_cpus_total, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_cpus_allocated, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_cpus_idle, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_memory_total_mb, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_memory_allocated_mb, gauge, 0}),

    %% Scheduler counters
    ets:insert(?METRICS_TABLE, {flurm_scheduler_cycles_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_scheduler_backfill_jobs, counter, 0}),

    %% Rate limiting counters
    ets:insert(?METRICS_TABLE, {flurm_requests_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_requests_rejected_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_backpressure_events_total, counter, 0}),

    %% Initialize histograms
    init_histogram(flurm_scheduler_duration_ms),
    init_histogram(flurm_job_wait_time_seconds),
    init_histogram(flurm_job_run_time_seconds),
    init_histogram(flurm_request_duration_ms).

%% @private Initialize a histogram
init_histogram(Name) ->
    Buckets = lists:foldl(
        fun(B, Acc) -> maps:put(B, 0, Acc) end,
        #{},
        ?DURATION_BUCKETS ++ [infinity]
    ),
    ets:insert(?HISTOGRAM_TABLE, {Name, Buckets, 0.0, 0}).

%% @private Update histogram with new observation
update_histogram(Name, Value) ->
    case ets:lookup(?HISTOGRAM_TABLE, Name) of
        [{Name, Buckets, Sum, Count}] ->
            NewBuckets = maps:map(
                fun(Boundary, BucketCount) ->
                    case Value =< Boundary of
                        true -> BucketCount + 1;
                        false -> BucketCount
                    end
                end,
                Buckets
            ),
            ets:insert(?HISTOGRAM_TABLE, {Name, NewBuckets, Sum + Value, Count + 1});
        [] ->
            init_histogram(Name),
            update_histogram(Name, Value)
    end.

%% @private Collect system metrics periodically
collect_system_metrics() ->
    %% Collect job metrics
    collect_job_metrics(),
    %% Collect node metrics
    collect_node_metrics(),
    %% Collect rate limiter metrics
    collect_rate_limiter_metrics().

%% @private Collect job-related metrics
collect_job_metrics() ->
    case catch flurm_job_registry:count_by_state() of
        Counts when is_map(Counts) ->
            gauge(flurm_jobs_pending, maps:get(pending, Counts, 0)),
            gauge(flurm_jobs_running, maps:get(running, Counts, 0)),
            gauge(flurm_jobs_suspended, maps:get(suspended, Counts, 0));
        _ ->
            ok
    end.

%% @private Collect node-related metrics
collect_node_metrics() ->
    case catch flurm_node_registry:count_by_state() of
        Counts when is_map(Counts) ->
            TotalNodes = maps:fold(fun(_, V, Acc) -> V + Acc end, 0, Counts),
            gauge(flurm_nodes_total, TotalNodes),
            gauge(flurm_nodes_up, maps:get(up, Counts, 0)),
            gauge(flurm_nodes_down, maps:get(down, Counts, 0)),
            gauge(flurm_nodes_drain, maps:get(drain, Counts, 0));
        _ ->
            ok
    end,
    %% Collect resource utilization
    case catch collect_resource_utilization() of
        {TotalCpus, AllocCpus, TotalMem, AllocMem} ->
            gauge(flurm_cpus_total, TotalCpus),
            gauge(flurm_cpus_allocated, AllocCpus),
            gauge(flurm_cpus_idle, TotalCpus - AllocCpus),
            gauge(flurm_memory_total_mb, TotalMem),
            gauge(flurm_memory_allocated_mb, AllocMem);
        _ ->
            ok
    end.

%% @private Collect resource utilization from node registry
collect_resource_utilization() ->
    Nodes = case catch flurm_node_registry:list_nodes() of
        L when is_list(L) -> L;
        _ -> []
    end,
    lists:foldl(
        fun({NodeName, _Pid}, {TotalC, AllocC, TotalM, AllocM}) ->
            case catch flurm_node_registry:get_node_entry(NodeName) of
                {ok, #node_entry{cpus_total = CT, cpus_avail = CA,
                                 memory_total = MT, memory_avail = MA}} ->
                    {TotalC + CT, AllocC + (CT - CA), TotalM + MT, AllocM + (MT - MA)};
                _ ->
                    {TotalC, AllocC, TotalM, AllocM}
            end
        end,
        {0, 0, 0, 0},
        Nodes
    ).

%% @private Collect rate limiter metrics
collect_rate_limiter_metrics() ->
    case catch flurm_rate_limiter:get_stats() of
        Stats when is_map(Stats) ->
            gauge(flurm_rate_limiter_load, maps:get(current_load, Stats, 0.0)),
            case maps:get(backpressure_active, Stats, false) of
                true -> gauge(flurm_backpressure_active, 1);
                false -> gauge(flurm_backpressure_active, 0)
            end;
        _ ->
            ok
    end.

%% @private Format metrics in Prometheus text format
do_format_prometheus() ->
    MetricLines = ets:foldl(
        fun({Name, Type, Value}, Acc) ->
            TypeStr = case Type of
                counter -> "counter";
                gauge -> "gauge"
            end,
            [format_metric(Name, TypeStr, Value) | Acc]
        end,
        [],
        ?METRICS_TABLE
    ),
    HistogramLines = ets:foldl(
        fun({Name, Buckets, Sum, Count}, Acc) ->
            [format_histogram(Name, Buckets, Sum, Count) | Acc]
        end,
        [],
        ?HISTOGRAM_TABLE
    ),
    lists:flatten(MetricLines ++ HistogramLines).

%% @private Format a single metric
format_metric(Name, Type, Value) ->
    NameStr = atom_to_list(Name),
    [
        "# HELP ", NameStr, " ", get_metric_help(Name), "\n",
        "# TYPE ", NameStr, " ", Type, "\n",
        NameStr, " ", format_value(Value), "\n"
    ].

%% @private Format a histogram
format_histogram(Name, Buckets, Sum, Count) ->
    NameStr = atom_to_list(Name),
    BucketLines = lists:map(
        fun({Boundary, BucketCount}) ->
            BoundaryStr = case Boundary of
                infinity -> "+Inf";
                N -> integer_to_list(N)
            end,
            [NameStr, "_bucket{le=\"", BoundaryStr, "\"} ", integer_to_list(BucketCount), "\n"]
        end,
        lists:sort(maps:to_list(Buckets))
    ),
    [
        "# HELP ", NameStr, " ", get_metric_help(Name), "\n",
        "# TYPE ", NameStr, " histogram\n",
        BucketLines,
        NameStr, "_sum ", format_value(Sum), "\n",
        NameStr, "_count ", integer_to_list(Count), "\n"
    ].

%% @private Format a numeric value
format_value(Value) when is_integer(Value) ->
    integer_to_list(Value);
format_value(Value) when is_float(Value) ->
    io_lib:format("~.6f", [Value]).

%% @private Get help text for a metric
get_metric_help(flurm_jobs_submitted_total) -> "Total number of jobs submitted";
get_metric_help(flurm_jobs_completed_total) -> "Total number of jobs completed successfully";
get_metric_help(flurm_jobs_failed_total) -> "Total number of jobs that failed";
get_metric_help(flurm_jobs_cancelled_total) -> "Total number of jobs cancelled";
get_metric_help(flurm_jobs_preempted_total) -> "Total number of jobs preempted";
get_metric_help(flurm_jobs_pending) -> "Current number of pending jobs";
get_metric_help(flurm_jobs_running) -> "Current number of running jobs";
get_metric_help(flurm_jobs_suspended) -> "Current number of suspended jobs";
get_metric_help(flurm_nodes_total) -> "Total number of nodes";
get_metric_help(flurm_nodes_up) -> "Number of nodes in up state";
get_metric_help(flurm_nodes_down) -> "Number of nodes in down state";
get_metric_help(flurm_nodes_drain) -> "Number of nodes in drain state";
get_metric_help(flurm_cpus_total) -> "Total CPUs in cluster";
get_metric_help(flurm_cpus_allocated) -> "CPUs currently allocated to jobs";
get_metric_help(flurm_cpus_idle) -> "CPUs currently idle";
get_metric_help(flurm_memory_total_mb) -> "Total memory in cluster (MB)";
get_metric_help(flurm_memory_allocated_mb) -> "Memory allocated to jobs (MB)";
get_metric_help(flurm_scheduler_cycles_total) -> "Total scheduler cycles";
get_metric_help(flurm_scheduler_backfill_jobs) -> "Jobs scheduled via backfill";
get_metric_help(flurm_scheduler_duration_ms) -> "Scheduler cycle duration";
get_metric_help(flurm_job_wait_time_seconds) -> "Job wait time in queue";
get_metric_help(flurm_job_run_time_seconds) -> "Job run time";
get_metric_help(flurm_request_duration_ms) -> "Request processing duration";
get_metric_help(flurm_requests_total) -> "Total requests processed";
get_metric_help(flurm_requests_rejected_total) -> "Requests rejected by rate limiter";
get_metric_help(flurm_backpressure_events_total) -> "Backpressure activation events";
get_metric_help(flurm_rate_limiter_load) -> "Current rate limiter load (0-1)";
get_metric_help(flurm_backpressure_active) -> "Whether backpressure is active (0/1)";
get_metric_help(_) -> "FLURM metric".
