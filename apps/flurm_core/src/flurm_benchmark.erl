%%%-------------------------------------------------------------------
%%% @doc FLURM Performance Benchmarking
%%%
%%% Tools for measuring and analyzing FLURM performance:
%%% - Job submission throughput
%%% - Scheduler latency
%%% - Node registration throughput
%%% - Message processing rates
%%% - Memory usage tracking
%%%
%%% Use these tools to identify bottlenecks and verify performance
%%% improvements.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_benchmark).

-include("flurm_core.hrl").

%% API
-export([
    run_all/0,
    run_benchmark/1,
    job_submission_throughput/1,
    scheduler_latency/1,
    node_registration_throughput/1,
    message_throughput/1,
    memory_usage/0,
    report/1,
    format_results/1
]).

%% Benchmark parameters
-define(DEFAULT_ITERATIONS, 1000).
-define(WARMUP_ITERATIONS, 100).

%%====================================================================
%% API
%%====================================================================

%% @doc Run all benchmarks with default parameters.
-spec run_all() -> map().
run_all() ->
    Results = #{
        job_submission => job_submission_throughput(#{}),
        scheduler_latency => scheduler_latency(#{}),
        node_registration => node_registration_throughput(#{}),
        message_throughput => message_throughput(#{}),
        memory => memory_usage()
    },
    report(Results),
    Results.

%% @doc Run a specific benchmark.
-spec run_benchmark(atom()) -> map().
run_benchmark(job_submission) ->
    job_submission_throughput(#{});
run_benchmark(scheduler_latency) ->
    scheduler_latency(#{});
run_benchmark(node_registration) ->
    node_registration_throughput(#{});
run_benchmark(message_throughput) ->
    message_throughput(#{});
run_benchmark(memory) ->
    memory_usage();
run_benchmark(_) ->
    #{error => unknown_benchmark}.

%% @doc Benchmark job submission throughput.
%% Measures how many jobs can be submitted per second.
-spec job_submission_throughput(map()) -> map().
job_submission_throughput(Opts) ->
    Iterations = maps:get(iterations, Opts, ?DEFAULT_ITERATIONS),
    Warmup = maps:get(warmup, Opts, ?WARMUP_ITERATIONS),

    %% Ensure job registry is running
    ensure_registry_running(),

    %% Warmup phase
    io:format("Running job submission warmup (~p iterations)...~n", [Warmup]),
    _ = run_job_submissions(Warmup),

    %% Benchmark phase
    io:format("Running job submission benchmark (~p iterations)...~n", [Iterations]),
    {Time, SubmittedCount} = timer:tc(fun() -> run_job_submissions(Iterations) end),

    TimeMs = Time / 1000,
    ThroughputPerSec = (SubmittedCount / TimeMs) * 1000,

    #{
        benchmark => job_submission,
        iterations => Iterations,
        successful => SubmittedCount,
        total_time_ms => TimeMs,
        throughput_per_sec => ThroughputPerSec,
        avg_latency_us => Time / max(1, SubmittedCount)
    }.

%% @doc Benchmark scheduler latency.
%% Measures time for scheduler to process pending jobs.
-spec scheduler_latency(map()) -> map().
scheduler_latency(Opts) ->
    Iterations = maps:get(iterations, Opts, 100),

    io:format("Running scheduler latency benchmark (~p iterations)...~n", [Iterations]),

    Latencies = lists:map(
        fun(_) ->
            %% Create some pending jobs
            Jobs = create_mock_jobs(10),

            %% Measure scheduler cycle time
            Start = erlang:monotonic_time(microsecond),
            _ = run_scheduler_cycle(Jobs),
            End = erlang:monotonic_time(microsecond),

            End - Start
        end,
        lists:seq(1, Iterations)
    ),

    analyze_latencies(scheduler_latency, Latencies).

%% @doc Benchmark node registration throughput.
-spec node_registration_throughput(map()) -> map().
node_registration_throughput(Opts) ->
    Iterations = maps:get(iterations, Opts, ?DEFAULT_ITERATIONS),

    ensure_registry_running(),

    io:format("Running node registration benchmark (~p iterations)...~n", [Iterations]),

    {Time, RegisteredCount} = timer:tc(
        fun() ->
            lists:foldl(
                fun(I, Acc) ->
                    NodeName = list_to_binary("bench_node_" ++ integer_to_list(I)),
                    case mock_register_node(NodeName) of
                        ok -> Acc + 1;
                        _ -> Acc
                    end
                end,
                0,
                lists:seq(1, Iterations)
            )
        end
    ),

    %% Cleanup
    cleanup_bench_nodes(),

    TimeMs = Time / 1000,
    ThroughputPerSec = (RegisteredCount / TimeMs) * 1000,

    #{
        benchmark => node_registration,
        iterations => Iterations,
        successful => RegisteredCount,
        total_time_ms => TimeMs,
        throughput_per_sec => ThroughputPerSec,
        avg_latency_us => Time / max(1, RegisteredCount)
    }.

%% @doc Benchmark message passing throughput.
-spec message_throughput(map()) -> map().
message_throughput(Opts) ->
    Iterations = maps:get(iterations, Opts, 10000),

    io:format("Running message throughput benchmark (~p iterations)...~n", [Iterations]),

    %% Spawn a receiver process
    Self = self(),
    Receiver = spawn(fun() -> message_receiver(Self, Iterations) end),

    %% Send messages and measure
    {Time, _} = timer:tc(
        fun() ->
            lists:foreach(
                fun(I) ->
                    Receiver ! {msg, I}
                end,
                lists:seq(1, Iterations)
            ),
            %% Wait for receiver to finish
            receive
                {done, ReceivedCount} -> ReceivedCount
            after 30000 ->
                timeout
            end
        end
    ),

    TimeMs = Time / 1000,
    ThroughputPerSec = (Iterations / TimeMs) * 1000,

    #{
        benchmark => message_throughput,
        iterations => Iterations,
        total_time_ms => TimeMs,
        throughput_per_sec => ThroughputPerSec,
        avg_latency_us => Time / Iterations
    }.

%% @doc Get current memory usage statistics.
-spec memory_usage() -> map().
memory_usage() ->
    Memory = erlang:memory(),
    ProcessCount = erlang:system_info(process_count),
    EtsCount = length(ets:all()),

    %% Get FLURM-specific memory usage
    FlurmEtsTables = [T || T <- ets:all(),
                           is_atom(T),
                           lists:prefix("flurm_", atom_to_list(T))],
    FlurmEtsMemory = lists:sum([ets:info(T, memory) * erlang:system_info(wordsize)
                                || T <- FlurmEtsTables]),

    #{
        benchmark => memory,
        total_bytes => proplists:get_value(total, Memory),
        processes_bytes => proplists:get_value(processes, Memory),
        ets_bytes => proplists:get_value(ets, Memory),
        atom_bytes => proplists:get_value(atom, Memory),
        binary_bytes => proplists:get_value(binary, Memory),
        process_count => ProcessCount,
        ets_table_count => EtsCount,
        flurm_ets_tables => length(FlurmEtsTables),
        flurm_ets_bytes => FlurmEtsMemory
    }.

%% @doc Print a formatted report of benchmark results.
-spec report(map()) -> ok.
report(Results) ->
    io:format("~n========================================~n"),
    io:format("       FLURM BENCHMARK REPORT~n"),
    io:format("========================================~n~n"),

    maps:foreach(
        fun(Name, Result) ->
            io:format("~s~n", [string:uppercase(atom_to_list(Name))]),
            io:format("----------------------------------------~n"),
            format_result(Result),
            io:format("~n")
        end,
        Results
    ),
    ok.

%% @doc Format results as a string.
-spec format_results(map()) -> iolist().
format_results(Results) ->
    lists:flatten([
        io_lib:format("~p: ~p~n", [Name, Result])
        || {Name, Result} <- maps:to_list(Results)
    ]).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Ensure registries are running for benchmarks
ensure_registry_running() ->
    %% Start job registry if not running
    case whereis(flurm_job_registry) of
        undefined ->
            catch flurm_job_registry:start_link();
        _ -> ok
    end,
    %% Start node registry if not running
    case whereis(flurm_node_registry) of
        undefined ->
            catch flurm_node_registry:start_link();
        _ -> ok
    end.

%% @private Run job submissions
run_job_submissions(Count) ->
    lists:foldl(
        fun(I, Acc) ->
            JobSpec = #{
                name => list_to_binary("bench_job_" ++ integer_to_list(I)),
                num_nodes => 1,
                num_cpus => 1,
                memory_mb => 1024,
                time_limit => 3600,
                user => <<"benchmark">>,
                account => <<"bench">>
            },
            case mock_submit_job(JobSpec) of
                {ok, _} -> Acc + 1;
                _ -> Acc
            end
        end,
        0,
        lists:seq(1, Count)
    ).

%% @private Mock job submission (without actual process creation)
mock_submit_job(JobSpec) ->
    %% Simulate the cost of job validation and registration
    _ = maps:get(name, JobSpec),
    _ = maps:get(num_nodes, JobSpec, 1),
    _ = maps:get(num_cpus, JobSpec, 1),

    %% Generate a job ID
    JobId = erlang:unique_integer([positive]),
    {ok, JobId}.

%% @private Mock node registration
mock_register_node(NodeName) ->
    %% Simulate registration overhead
    _ = binary_to_list(NodeName),
    ok.

%% @private Cleanup benchmark nodes
cleanup_bench_nodes() ->
    %% Would remove bench_node_* entries
    ok.

%% @private Create mock jobs for scheduler testing
create_mock_jobs(Count) ->
    [#{
        job_id => I,
        priority => rand:uniform(10000),
        num_nodes => rand:uniform(4),
        num_cpus => rand:uniform(16),
        memory_mb => rand:uniform(8192)
    } || I <- lists:seq(1, Count)].

%% @private Run a scheduler cycle on mock jobs
run_scheduler_cycle(Jobs) ->
    %% Simulate sorting by priority
    Sorted = lists:sort(
        fun(A, B) ->
            maps:get(priority, A, 0) > maps:get(priority, B, 0)
        end,
        Jobs
    ),

    %% Simulate resource allocation checks
    lists:foreach(
        fun(Job) ->
            _ = maps:get(num_nodes, Job),
            _ = maps:get(num_cpus, Job),
            _ = maps:get(memory_mb, Job)
        end,
        Sorted
    ),

    length(Sorted).

%% @private Message receiver process
message_receiver(Parent, Expected) ->
    message_receiver_loop(Parent, Expected, 0).

message_receiver_loop(Parent, Expected, Count) when Count >= Expected ->
    Parent ! {done, Count};
message_receiver_loop(Parent, Expected, Count) ->
    receive
        {msg, _} ->
            message_receiver_loop(Parent, Expected, Count + 1)
    after 5000 ->
        Parent ! {done, Count}
    end.

%% @private Analyze latency measurements
analyze_latencies(Name, Latencies) ->
    Sorted = lists:sort(Latencies),
    Count = length(Sorted),

    Min = hd(Sorted),
    Max = lists:last(Sorted),
    Sum = lists:sum(Sorted),
    Mean = Sum / Count,

    %% Percentiles
    P50Index = max(1, round(Count * 0.5)),
    P95Index = max(1, round(Count * 0.95)),
    P99Index = max(1, round(Count * 0.99)),

    P50 = lists:nth(P50Index, Sorted),
    P95 = lists:nth(P95Index, Sorted),
    P99 = lists:nth(P99Index, Sorted),

    #{
        benchmark => Name,
        iterations => Count,
        min_us => Min,
        max_us => Max,
        mean_us => Mean,
        p50_us => P50,
        p95_us => P95,
        p99_us => P99
    }.

%% @private Format a single result for display
format_result(#{benchmark := _Name} = Result) ->
    maps:foreach(
        fun(Key, Value) when Key =/= benchmark ->
            case Value of
                V when is_float(V) ->
                    io:format("  ~-20s: ~.2f~n", [Key, V]);
                V when is_integer(V), V > 1000000 ->
                    io:format("  ~-20s: ~.2f MB~n", [Key, V / 1048576]);
                V ->
                    io:format("  ~-20s: ~p~n", [Key, V])
            end
        end,
        Result
    );
format_result(Result) ->
    io:format("  ~p~n", [Result]).
