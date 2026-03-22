%%%-------------------------------------------------------------------
%%% @doc FLURM Job Journal
%%%
%%% Append-only JSON log of all job lifecycle events. Each line is a
%%% self-contained JSON object with timestamp, event type, and details.
%%%
%%% Journal file: /var/log/flurm/journal.jsonl
%%%
%%% Events recorded:
%%%   job_submitted  - New job entered the system
%%%   job_scheduled  - Job allocated to node(s)
%%%   job_started    - Job began executing
%%%   job_completed  - Job finished (exit code 0)
%%%   job_failed     - Job finished (exit code != 0)
%%%   job_cancelled  - Job was cancelled
%%%   job_timeout    - Job exceeded time limit
%%%   node_joined    - Compute node registered
%%%   node_left      - Compute node disconnected
%%%   scale_up       - Cloud auto-scaler launched instance(s)
%%%   scale_down     - Cloud auto-scaler terminated instance(s)
%%%   metrics        - Periodic metrics snapshot
%%%
%%% Query examples:
%%%   grep job_completed /var/log/flurm/journal.jsonl | jq .
%%%   grep job_submitted /var/log/flurm/journal.jsonl | jq -s 'length'
%%%   cat journal.jsonl | jq 'select(.event=="metrics")' | tail -1
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_journal).
-behaviour(gen_server).

-export([
    start_link/0,
    %% Event recording
    record/2,
    record/3,
    %% Convenience
    job_submitted/2,
    job_scheduled/3,
    job_completed/2,
    job_failed/3,
    job_cancelled/1,
    node_joined/2,
    node_left/2,
    scale_event/2,
    metrics_snapshot/0
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_PATH, "/var/log/flurm/journal.jsonl").
-define(METRICS_INTERVAL, 60000).  % Snapshot every 60s

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec record(atom(), map()) -> ok.
record(Event, Details) ->
    gen_server:cast(?SERVER, {record, Event, Details}).

-spec record(atom(), map(), map()) -> ok.
record(Event, Details, Extra) ->
    gen_server:cast(?SERVER, {record, Event, maps:merge(Details, Extra)}).

job_submitted(JobId, JobSpec) ->
    record(job_submitted, #{
        job_id => JobId,
        name => maps:get(name, JobSpec, <<>>),
        user => maps:get(user, JobSpec, <<>>),
        partition => maps:get(partition, JobSpec, <<>>),
        num_nodes => maps:get(num_nodes, JobSpec, 1),
        num_cpus => maps:get(num_cpus, JobSpec, 1),
        priority => maps:get(priority, JobSpec, 0)
    }).

job_scheduled(JobId, Nodes, Licenses) ->
    record(job_scheduled, #{
        job_id => JobId,
        nodes => Nodes,
        licenses => Licenses
    }).

job_completed(JobId, ExitCode) ->
    record(job_completed, #{job_id => JobId, exit_code => ExitCode}).

job_failed(JobId, ExitCode, Reason) ->
    record(job_failed, #{job_id => JobId, exit_code => ExitCode, reason => Reason}).

job_cancelled(JobId) ->
    record(job_cancelled, #{job_id => JobId}).

node_joined(NodeName, Info) ->
    record(node_joined, #{
        node => NodeName,
        cpus => maps:get(cpus, Info, 0),
        memory_mb => maps:get(memory_mb, Info, 0)
    }).

node_left(NodeName, Reason) ->
    record(node_left, #{node => NodeName, reason => Reason}).

scale_event(Direction, Details) ->
    record(Direction, Details).

metrics_snapshot() ->
    gen_server:cast(?SERVER, metrics_snapshot).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Path = application:get_env(flurm_core, journal_path, ?DEFAULT_PATH),
    case file:open(Path, [append, {encoding, utf8}]) of
        {ok, Fd} ->
            lager:info("[journal] Recording to ~s", [Path]),
            %% Schedule periodic metrics snapshots
            erlang:send_after(?METRICS_INTERVAL, self(), metrics_tick),
            %% Record startup
            write_event(Fd, controller_started, #{
                node => atom_to_binary(node(), utf8),
                pid => list_to_binary(os:getpid())
            }),
            {ok, #{fd => Fd, path => Path}};
        {error, Reason} ->
            lager:warning("[journal] Cannot open ~s: ~p (journaling disabled)", [Path, Reason]),
            {ok, #{fd => undefined, path => Path}}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({record, Event, Details}, #{fd := Fd} = State) ->
    write_event(Fd, Event, Details),
    {noreply, State};

handle_cast(metrics_snapshot, #{fd := Fd} = State) ->
    Metrics = collect_metrics(),
    write_event(Fd, metrics, Metrics),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(metrics_tick, #{fd := Fd} = State) ->
    Metrics = collect_metrics(),
    write_event(Fd, metrics, Metrics),
    erlang:send_after(?METRICS_INTERVAL, self(), metrics_tick),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{fd := Fd}) ->
    case Fd of
        undefined -> ok;
        _ ->
            write_event(Fd, controller_stopped, #{}),
            file:close(Fd)
    end;
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal
%%%===================================================================

write_event(undefined, _Event, _Details) ->
    ok;
write_event(Fd, Event, Details) ->
    Now = calendar:system_time_to_rfc3339(
        erlang:system_time(second), [{offset, "Z"}]),
    Line = jsx:encode(#{
        t => list_to_binary(Now),
        event => Event,
        d => ensure_binary_values(Details)
    }),
    file:write(Fd, [Line, "\n"]).

collect_metrics() ->
    %% Collect current cluster state
    Submitted = prom_value(flurm_jobs_submitted_total),
    Completed = prom_value(flurm_jobs_completed_total),
    Failed = prom_value(flurm_jobs_failed_total),
    Cancelled = prom_value(flurm_jobs_cancelled_total),
    Running = prom_value(flurm_jobs_running),
    Pending = prom_value(flurm_jobs_pending),
    NodesUp = prom_value(flurm_nodes_up),
    CpusTotal = prom_value(flurm_cpus_total),
    CpusAlloc = prom_value(flurm_cpus_allocated),
    Cycles = prom_value(flurm_scheduler_cycles_total),
    #{
        jobs_submitted => Submitted,
        jobs_completed => Completed,
        jobs_failed => Failed,
        jobs_cancelled => Cancelled,
        jobs_running => Running,
        jobs_pending => Pending,
        nodes_up => NodesUp,
        cpus_total => CpusTotal,
        cpus_allocated => CpusAlloc,
        scheduler_cycles => Cycles
    }.

prom_value(Name) ->
    try prometheus_gauge:value(Name)
    catch _:_ ->
        try prometheus_counter:value(Name)
        catch _:_ -> 0
        end
    end.

%% Ensure all map values are JSON-encodable
ensure_binary_values(Map) when is_map(Map) ->
    maps:map(fun(_K, V) -> ensure_binary(V) end, Map);
ensure_binary_values(Other) ->
    Other.

ensure_binary(V) when is_binary(V) -> V;
ensure_binary(V) when is_atom(V) -> atom_to_binary(V, utf8);
ensure_binary(V) when is_integer(V) -> V;
ensure_binary(V) when is_float(V) -> V;
ensure_binary(V) when is_list(V) ->
    case io_lib:printable_list(V) of
        true -> list_to_binary(V);
        false -> [ensure_binary(E) || E <- V]
    end;
ensure_binary(V) when is_pid(V) -> list_to_binary(pid_to_list(V));
ensure_binary(V) when is_map(V) -> ensure_binary_values(V);
ensure_binary(V) -> iolist_to_binary(io_lib:format("~p", [V])).
