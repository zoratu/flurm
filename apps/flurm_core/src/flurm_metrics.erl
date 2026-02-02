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
    reset/0,
    %% Labeled metrics API (for TRES)
    labeled_gauge/3,
    labeled_counter/3,
    get_labeled_metric/2,
    collect_tres_metrics/0
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
-define(LABELED_TABLE, flurm_labeled_metrics).

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

%% @doc Set a labeled gauge metric (e.g., for TRES metrics).
%% Labels is a map of label name -> value, e.g., #{type => <<"cpu">>}
-spec labeled_gauge(atom(), map(), number()) -> ok.
labeled_gauge(Name, Labels, Value) ->
    gen_server:cast(?SERVER, {labeled_gauge, Name, Labels, Value}).

%% @doc Increment a labeled counter metric.
-spec labeled_counter(atom(), map(), number()) -> ok.
labeled_counter(Name, Labels, N) ->
    gen_server:cast(?SERVER, {labeled_counter, Name, Labels, N}).

%% @doc Get a labeled metric value.
-spec get_labeled_metric(atom(), map()) -> {ok, number()} | {error, not_found}.
get_labeled_metric(Name, Labels) ->
    Key = {Name, Labels},
    case ets:lookup(?LABELED_TABLE, Key) of
        [{Key, _Type, Value}] -> {ok, Value};
        [] -> {error, not_found}
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables
    ets:new(?METRICS_TABLE, [named_table, public, set]),
    ets:new(?HISTOGRAM_TABLE, [named_table, public, set]),
    ets:new(?LABELED_TABLE, [named_table, public, set]),

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
    ets:delete_all_objects(?LABELED_TABLE),
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

handle_cast({labeled_gauge, Name, Labels, Value}, State) ->
    Key = {Name, Labels},
    ets:insert(?LABELED_TABLE, {Key, gauge, Value}),
    {noreply, State};

handle_cast({labeled_counter, Name, Labels, N}, State) ->
    Key = {Name, Labels},
    case ets:lookup(?LABELED_TABLE, Key) of
        [{Key, counter, Value}] ->
            ets:insert(?LABELED_TABLE, {Key, counter, Value + N});
        [] ->
            ets:insert(?LABELED_TABLE, {Key, counter, N})
    end,
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

    %% Federation gauges
    ets:insert(?METRICS_TABLE, {flurm_federation_clusters_total, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_federation_clusters_healthy, gauge, 0}),
    ets:insert(?METRICS_TABLE, {flurm_federation_clusters_unhealthy, gauge, 0}),

    %% Federation counters
    ets:insert(?METRICS_TABLE, {flurm_federation_jobs_submitted_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_federation_jobs_received_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_federation_routing_decisions_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_federation_routing_local_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_federation_routing_remote_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_federation_health_checks_total, counter, 0}),
    ets:insert(?METRICS_TABLE, {flurm_federation_health_check_failures_total, counter, 0}),

    %% Initialize histograms
    init_histogram(flurm_scheduler_duration_ms),
    init_histogram(flurm_job_wait_time_seconds),
    init_histogram(flurm_job_run_time_seconds),
    init_histogram(flurm_request_duration_ms),
    init_histogram(flurm_federation_routing_duration_ms),
    init_histogram(flurm_federation_remote_submit_duration_ms).

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
    collect_rate_limiter_metrics(),
    %% Collect federation metrics
    collect_federation_metrics(),
    %% Collect TRES metrics
    collect_tres_metrics().

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

%% @private Collect federation metrics
collect_federation_metrics() ->
    case catch flurm_federation:get_federation_stats() of
        Stats when is_map(Stats) ->
            gauge(flurm_federation_clusters_total, maps:get(clusters_total, Stats, 0)),
            gauge(flurm_federation_clusters_healthy, maps:get(clusters_healthy, Stats, 0)),
            gauge(flurm_federation_clusters_unhealthy, maps:get(clusters_unhealthy, Stats, 0));
        _ ->
            %% Federation module may not be running
            ok
    end.

%% @private Collect TRES (Trackable Resources) metrics
%% Emits labeled metrics for each TRES type (cpu, mem, gpu, node, energy, etc.)
collect_tres_metrics() ->
    %% Collect TRES totals and allocated from node registry
    try
        Nodes = case catch flurm_node_registry:list_nodes() of
            L when is_list(L) -> L;
            _ -> []
        end,

        %% Aggregate TRES data from all nodes
        TresData = lists:foldl(
            fun({NodeName, _Pid}, Acc) ->
                case catch flurm_node_registry:get_node_entry(NodeName) of
                    {ok, #node_entry{
                        cpus_total = CT, cpus_avail = CA,
                        memory_total = MT, memory_avail = MA,
                        gpus_total = GT, gpus_avail = GA}} ->
                        %% Accumulate TRES totals and allocated
                        #{
                            cpu_total => maps:get(cpu_total, Acc, 0) + CT,
                            cpu_alloc => maps:get(cpu_alloc, Acc, 0) + (CT - CA),
                            mem_total => maps:get(mem_total, Acc, 0) + MT,
                            mem_alloc => maps:get(mem_alloc, Acc, 0) + (MT - MA),
                            gpu_total => maps:get(gpu_total, Acc, 0) + GT,
                            gpu_alloc => maps:get(gpu_alloc, Acc, 0) + (GT - GA),
                            node_total => maps:get(node_total, Acc, 0) + 1,
                            node_alloc => maps:get(node_alloc, Acc, 0)  % Updated below
                        };
                    _ ->
                        Acc
                end
            end,
            #{cpu_total => 0, cpu_alloc => 0, mem_total => 0, mem_alloc => 0,
              gpu_total => 0, gpu_alloc => 0, node_total => 0, node_alloc => 0},
            Nodes
        ),

        %% Emit labeled metrics for standard TRES types
        %% flurm_tres_total{type="cpu"} 100
        %% flurm_tres_allocated{type="cpu"} 50
        emit_tres_metric(<<"cpu">>, maps:get(cpu_total, TresData, 0), maps:get(cpu_alloc, TresData, 0)),
        emit_tres_metric(<<"mem">>, maps:get(mem_total, TresData, 0), maps:get(mem_alloc, TresData, 0)),
        emit_tres_metric(<<"gpu">>, maps:get(gpu_total, TresData, 0), maps:get(gpu_alloc, TresData, 0)),
        emit_tres_metric(<<"node">>, maps:get(node_total, TresData, 0), maps:get(node_alloc, TresData, 0)),

        %% Get configured TRES from account manager and emit for each
        collect_configured_tres_metrics()
    catch
        _:_ ->
            %% Node registry may not be running
            ok
    end.

%% @private Emit TRES metrics for a specific type
emit_tres_metric(TresType, Total, Allocated) ->
    Labels = #{type => TresType},
    labeled_gauge(flurm_tres_total, Labels, Total),
    labeled_gauge(flurm_tres_allocated, Labels, Allocated),
    labeled_gauge(flurm_tres_idle, Labels, max(0, Total - Allocated)).

%% @private Collect metrics for configured TRES types from account manager
collect_configured_tres_metrics() ->
    case catch flurm_account_manager:list_tres() of
        TresList when is_list(TresList) ->
            %% Emit a metric showing each configured TRES type
            lists:foreach(
                fun(#tres{type = Type, name = Name}) ->
                    %% For GRES types, include the name in the label
                    Labels = case Name of
                        <<>> -> #{type => Type};
                        _ -> #{type => Type, name => Name}
                    end,
                    %% Emit configured TRES indicator (1 = configured)
                    labeled_gauge(flurm_tres_configured, Labels, 1)
                end,
                TresList
            );
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
    LabeledLines = format_labeled_metrics(),
    lists:flatten(MetricLines ++ HistogramLines ++ LabeledLines).

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

%% @private Format labeled metrics (TRES, etc.)
%% Groups metrics by name, outputs HELP/TYPE once, then all labeled values
format_labeled_metrics() ->
    %% Collect all labeled metrics and group by name
    AllLabeled = ets:tab2list(?LABELED_TABLE),

    %% Group by metric name
    Grouped = lists:foldl(
        fun({{Name, Labels}, Type, Value}, Acc) ->
            Existing = maps:get(Name, Acc, {Type, []}),
            {ExistingType, ExistingValues} = Existing,
            maps:put(Name, {ExistingType, [{Labels, Value} | ExistingValues]}, Acc)
        end,
        #{},
        AllLabeled
    ),

    %% Format each group
    maps:fold(
        fun(Name, {Type, LabeledValues}, Acc) ->
            [format_labeled_metric_group(Name, Type, LabeledValues) | Acc]
        end,
        [],
        Grouped
    ).

%% @private Format a group of labeled metrics with the same name
format_labeled_metric_group(Name, Type, LabeledValues) ->
    NameStr = atom_to_list(Name),
    TypeStr = case Type of
        counter -> "counter";
        gauge -> "gauge"
    end,
    ValueLines = lists:map(
        fun({Labels, Value}) ->
            LabelStr = format_labels(Labels),
            [NameStr, LabelStr, " ", format_value(Value), "\n"]
        end,
        LabeledValues
    ),
    [
        "# HELP ", NameStr, " ", get_metric_help(Name), "\n",
        "# TYPE ", NameStr, " ", TypeStr, "\n",
        ValueLines
    ].

%% @private Format labels map to Prometheus label string
%% #{type => <<"cpu">>, node => <<"node1">>} -> {type="cpu",node="node1"}
format_labels(Labels) when map_size(Labels) =:= 0 ->
    "";
format_labels(Labels) ->
    LabelList = maps:fold(
        fun(K, V, Acc) ->
            KeyStr = format_label_key(K),
            ValStr = format_label_value(V),
            [[KeyStr, "=\"", ValStr, "\""] | Acc]
        end,
        [],
        Labels
    ),
    ["{", lists:join(",", lists:reverse(LabelList)), "}"].

format_label_key(K) when is_atom(K) -> atom_to_list(K);
format_label_key(K) when is_binary(K) -> binary_to_list(K);
format_label_key(K) when is_list(K) -> K.

format_label_value(V) when is_binary(V) -> binary_to_list(V);
format_label_value(V) when is_atom(V) -> atom_to_list(V);
format_label_value(V) when is_integer(V) -> integer_to_list(V);
format_label_value(V) when is_list(V) -> V.

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
%% Federation metrics
get_metric_help(flurm_federation_clusters_total) -> "Total number of federated clusters";
get_metric_help(flurm_federation_clusters_healthy) -> "Number of healthy federated clusters";
get_metric_help(flurm_federation_clusters_unhealthy) -> "Number of unhealthy federated clusters";
get_metric_help(flurm_federation_jobs_submitted_total) -> "Total jobs submitted to remote clusters";
get_metric_help(flurm_federation_jobs_received_total) -> "Total jobs received from remote clusters";
get_metric_help(flurm_federation_routing_decisions_total) -> "Total federation routing decisions made";
get_metric_help(flurm_federation_routing_local_total) -> "Jobs routed to local cluster";
get_metric_help(flurm_federation_routing_remote_total) -> "Jobs routed to remote clusters";
get_metric_help(flurm_federation_health_checks_total) -> "Total federation health checks performed";
get_metric_help(flurm_federation_health_check_failures_total) -> "Federation health check failures";
get_metric_help(flurm_federation_routing_duration_ms) -> "Federation routing decision duration";
get_metric_help(flurm_federation_remote_submit_duration_ms) -> "Remote job submission duration";
%% TRES metrics
get_metric_help(flurm_tres_total) -> "Total TRES (Trackable Resources) in cluster by type";
get_metric_help(flurm_tres_allocated) -> "TRES allocated to jobs by type";
get_metric_help(flurm_tres_idle) -> "TRES available (total - allocated) by type";
get_metric_help(flurm_tres_configured) -> "Configured TRES types (1 = configured)";
get_metric_help(_) -> "FLURM metric".
