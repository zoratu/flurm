%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon - slurmdbd Sync Manager
%%%
%%% MIGRATION-ONLY SYNC MANAGER
%%% ===========================
%%% This module manages batched synchronization of job records from
%%% FLURM to an existing slurmdbd MySQL database during migration.
%%%
%%% Features:
%%% - Queued job record sync with configurable batch sizes
%%% - Automatic retry logic for transient connection failures
%%% - Enable/disable sync without losing queued data
%%% - Periodic flush of pending queue
%%% - Statistics and monitoring for sync operations
%%%
%%% This is NOT intended for long-term use. Once migration is complete,
%%% disable sync and transition fully to FLURM's native storage.
%%%
%%% USAGE:
%%% 1. Enable sync: flurm_dbd_sync:enable_sync()
%%% 2. Queue jobs: flurm_dbd_sync:queue_job(JobRecord)
%%% 3. Monitor: flurm_dbd_sync:get_sync_status()
%%% 4. Disable when done: flurm_dbd_sync:disable_sync()
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sync).

-behaviour(gen_server).

%% API
-export([start_link/0, start_link/1]).
-export([
    enable_sync/0,
    disable_sync/0,
    is_sync_enabled/0,
    queue_job/1,
    queue_jobs/1,
    flush_queue/0,
    clear_queue/0,
    get_sync_status/0,
    get_pending_count/0,
    get_failed_jobs/0,
    retry_failed_jobs/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Test exports
-ifdef(TEST).
-export([
    should_retry/2,
    calculate_backoff/1
]).
-endif.

-define(SERVER, ?MODULE).

%% Default configuration
-define(DEFAULT_BATCH_SIZE, 100).
-define(DEFAULT_FLUSH_INTERVAL, 5000).  % 5 seconds
-define(DEFAULT_MAX_RETRIES, 3).
-define(DEFAULT_RETRY_DELAY, 1000).     % 1 second
-define(DEFAULT_MAX_BACKOFF, 60000).    % 60 seconds
-define(DEFAULT_MAX_QUEUE_SIZE, 10000).

-record(state, {
    enabled :: boolean(),
    pending_queue :: queue:queue(map()),
    failed_jobs :: [map()],
    batch_size :: pos_integer(),
    flush_interval :: pos_integer(),
    max_retries :: pos_integer(),
    retry_delay :: pos_integer(),
    max_queue_size :: pos_integer(),
    flush_timer :: reference() | undefined,
    retry_count :: non_neg_integer(),
    stats :: map()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the sync manager with default config from application env
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    Config = get_sync_config(),
    start_link(Config).

%% @doc Start the sync manager with explicit config
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

%% @doc Enable synchronization to slurmdbd
%% When enabled, queued jobs will be periodically flushed to MySQL
-spec enable_sync() -> ok.
enable_sync() ->
    gen_server:call(?SERVER, enable_sync).

%% @doc Disable synchronization
%% Jobs in the queue are preserved but not synced
-spec disable_sync() -> ok.
disable_sync() ->
    gen_server:call(?SERVER, disable_sync).

%% @doc Check if sync is currently enabled
-spec is_sync_enabled() -> boolean().
is_sync_enabled() ->
    gen_server:call(?SERVER, is_sync_enabled).

%% @doc Queue a job record for sync to slurmdbd
%% Jobs are batched and synced periodically or when batch is full
-spec queue_job(map()) -> ok | {error, queue_full}.
queue_job(JobRecord) ->
    gen_server:call(?SERVER, {queue_job, JobRecord}).

%% @doc Queue multiple job records for sync
-spec queue_jobs([map()]) -> {ok, non_neg_integer()} | {error, term()}.
queue_jobs(JobRecords) ->
    gen_server:call(?SERVER, {queue_jobs, JobRecords}).

%% @doc Immediately flush all queued jobs to slurmdbd
%% Useful before shutdown or when immediate sync is needed
-spec flush_queue() -> {ok, non_neg_integer()} | {error, term()}.
flush_queue() ->
    gen_server:call(?SERVER, flush_queue, 60000).

%% @doc Clear the pending queue without syncing
%% WARNING: This will lose all queued job data
-spec clear_queue() -> {ok, non_neg_integer()}.
clear_queue() ->
    gen_server:call(?SERVER, clear_queue).

%% @doc Get sync status including pending count and statistics
-spec get_sync_status() -> map().
get_sync_status() ->
    gen_server:call(?SERVER, get_sync_status).

%% @doc Get count of pending jobs in queue
-spec get_pending_count() -> non_neg_integer().
get_pending_count() ->
    gen_server:call(?SERVER, get_pending_count).

%% @doc Get list of jobs that failed to sync
-spec get_failed_jobs() -> [map()].
get_failed_jobs() ->
    gen_server:call(?SERVER, get_failed_jobs).

%% @doc Retry syncing failed jobs
-spec retry_failed_jobs() -> {ok, non_neg_integer()} | {error, term()}.
retry_failed_jobs() ->
    gen_server:call(?SERVER, retry_failed_jobs, 60000).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    lager:info("FLURM DBD Sync Manager starting (MIGRATION-ONLY module)"),

    State = #state{
        enabled = maps:get(enabled, Config, false),
        pending_queue = queue:new(),
        failed_jobs = [],
        batch_size = maps:get(batch_size, Config, ?DEFAULT_BATCH_SIZE),
        flush_interval = maps:get(flush_interval, Config, ?DEFAULT_FLUSH_INTERVAL),
        max_retries = maps:get(max_retries, Config, ?DEFAULT_MAX_RETRIES),
        retry_delay = maps:get(retry_delay, Config, ?DEFAULT_RETRY_DELAY),
        max_queue_size = maps:get(max_queue_size, Config, ?DEFAULT_MAX_QUEUE_SIZE),
        flush_timer = undefined,
        retry_count = 0,
        stats = #{
            jobs_queued => 0,
            jobs_synced => 0,
            jobs_failed => 0,
            batches_synced => 0,
            sync_errors => 0,
            last_sync_time => undefined,
            last_error => undefined
        }
    },

    %% Start flush timer if enabled
    NewState = case State#state.enabled of
        true -> start_flush_timer(State);
        false -> State
    end,

    {ok, NewState}.

handle_call(enable_sync, _From, State) ->
    lager:info("Enabling slurmdbd sync"),
    NewState = start_flush_timer(State#state{enabled = true}),
    {reply, ok, NewState};

handle_call(disable_sync, _From, State) ->
    lager:info("Disabling slurmdbd sync (queue preserved: ~p jobs)",
               [queue:len(State#state.pending_queue)]),
    NewState = cancel_flush_timer(State#state{enabled = false}),
    {reply, ok, NewState};

handle_call(is_sync_enabled, _From, #state{enabled = Enabled} = State) ->
    {reply, Enabled, State};

handle_call({queue_job, JobRecord}, _From, State) ->
    QueueLen = queue:len(State#state.pending_queue),
    case QueueLen >= State#state.max_queue_size of
        true ->
            lager:warning("Sync queue full (~p jobs), dropping job ~p",
                         [QueueLen, maps:get(job_id, JobRecord, unknown)]),
            {reply, {error, queue_full}, State};
        false ->
            NewQueue = queue:in(JobRecord, State#state.pending_queue),
            Stats = maps:update_with(jobs_queued, fun(V) -> V + 1 end, 1, State#state.stats),
            NewState = State#state{pending_queue = NewQueue, stats = Stats},
            %% Check if we should flush due to batch size
            FinalState = maybe_trigger_flush(NewState),
            {reply, ok, FinalState}
    end;

handle_call({queue_jobs, JobRecords}, _From, State) ->
    QueueLen = queue:len(State#state.pending_queue),
    Available = State#state.max_queue_size - QueueLen,
    ToQueue = lists:sublist(JobRecords, Available),
    Queued = length(ToQueue),

    NewQueue = lists:foldl(fun(Job, Q) ->
        queue:in(Job, Q)
    end, State#state.pending_queue, ToQueue),

    Stats = maps:update_with(jobs_queued, fun(V) -> V + Queued end, Queued, State#state.stats),
    NewState = State#state{pending_queue = NewQueue, stats = Stats},

    case Queued < length(JobRecords) of
        true ->
            lager:warning("Sync queue full, queued ~p of ~p jobs", [Queued, length(JobRecords)]),
            FinalState = maybe_trigger_flush(NewState),
            {reply, {ok, Queued}, FinalState};
        false ->
            FinalState = maybe_trigger_flush(NewState),
            {reply, {ok, Queued}, FinalState}
    end;

handle_call(flush_queue, _From, State) ->
    case do_flush(State) of
        {ok, Count, NewState} ->
            {reply, {ok, Count}, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;

handle_call(clear_queue, _From, State) ->
    Count = queue:len(State#state.pending_queue),
    lager:warning("Clearing sync queue (~p jobs discarded)", [Count]),
    {reply, {ok, Count}, State#state{pending_queue = queue:new()}};

handle_call(get_sync_status, _From, State) ->
    Status = #{
        enabled => State#state.enabled,
        pending_count => queue:len(State#state.pending_queue),
        failed_count => length(State#state.failed_jobs),
        batch_size => State#state.batch_size,
        flush_interval => State#state.flush_interval,
        max_queue_size => State#state.max_queue_size,
        retry_count => State#state.retry_count,
        stats => State#state.stats
    },
    {reply, Status, State};

handle_call(get_pending_count, _From, State) ->
    {reply, queue:len(State#state.pending_queue), State};

handle_call(get_failed_jobs, _From, State) ->
    {reply, State#state.failed_jobs, State};

handle_call(retry_failed_jobs, _From, #state{failed_jobs = []} = State) ->
    {reply, {ok, 0}, State};

handle_call(retry_failed_jobs, _From, State) ->
    FailedJobs = State#state.failed_jobs,
    %% Move failed jobs back to queue
    NewQueue = lists:foldl(fun(Job, Q) ->
        queue:in(Job, Q)
    end, State#state.pending_queue, FailedJobs),
    NewState = State#state{pending_queue = NewQueue, failed_jobs = []},
    %% Trigger flush
    case do_flush(NewState) of
        {ok, Count, FinalState} ->
            {reply, {ok, Count}, FinalState};
        {error, Reason, FinalState} ->
            {reply, {error, Reason}, FinalState}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush_timer, #state{enabled = false} = State) ->
    %% Sync disabled, don't flush
    {noreply, State};

handle_info(flush_timer, State) ->
    case do_flush(State) of
        {ok, _Count, NewState} ->
            FinalState = start_flush_timer(NewState),
            {noreply, FinalState};
        {error, _Reason, NewState} ->
            %% On error, use exponential backoff for next retry
            Backoff = calculate_backoff(NewState#state.retry_count),
            FinalState = start_flush_timer(NewState#state{
                retry_count = NewState#state.retry_count + 1
            }, Backoff),
            {noreply, FinalState}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Try to flush remaining jobs on shutdown
    QueueLen = queue:len(State#state.pending_queue),
    case QueueLen > 0 of
        true ->
            lager:warning("Sync manager terminating with ~p queued jobs", [QueueLen]),
            %% Best effort flush
            catch do_flush(State);
        false ->
            ok
    end,
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Get sync config from application environment
get_sync_config() ->
    #{
        enabled => application:get_env(flurm_dbd, sync_enabled, false),
        batch_size => application:get_env(flurm_dbd, sync_batch_size, ?DEFAULT_BATCH_SIZE),
        flush_interval => application:get_env(flurm_dbd, sync_flush_interval, ?DEFAULT_FLUSH_INTERVAL),
        max_retries => application:get_env(flurm_dbd, sync_max_retries, ?DEFAULT_MAX_RETRIES),
        retry_delay => application:get_env(flurm_dbd, sync_retry_delay, ?DEFAULT_RETRY_DELAY),
        max_queue_size => application:get_env(flurm_dbd, sync_max_queue_size, ?DEFAULT_MAX_QUEUE_SIZE)
    }.

%% @private Start the periodic flush timer
start_flush_timer(State) ->
    start_flush_timer(State, State#state.flush_interval).

start_flush_timer(State, Interval) ->
    NewState = cancel_flush_timer(State),
    Timer = erlang:send_after(Interval, self(), flush_timer),
    NewState#state{flush_timer = Timer}.

%% @private Cancel the flush timer
cancel_flush_timer(#state{flush_timer = undefined} = State) ->
    State;
cancel_flush_timer(#state{flush_timer = Timer} = State) ->
    erlang:cancel_timer(Timer),
    State#state{flush_timer = undefined}.

%% @private Maybe trigger flush if batch is full
maybe_trigger_flush(State) ->
    case queue:len(State#state.pending_queue) >= State#state.batch_size of
        true when State#state.enabled ->
            %% Batch full, trigger immediate flush
            self() ! flush_timer,
            State;
        _ ->
            State
    end.

%% @private Perform the actual flush operation
do_flush(#state{pending_queue = Queue} = State) ->
    case queue:is_empty(Queue) of
        true ->
            {ok, 0, State};
        false ->
            do_flush_non_empty(State)
    end.

do_flush_non_empty(State) ->
    %% Extract batch from queue
    {Batch, RemainingQueue} = extract_batch(State#state.pending_queue, State#state.batch_size),

    %% Check if MySQL connector is connected
    case flurm_dbd_mysql:is_connected() of
        false ->
            %% Not connected, put batch back and report error
            lager:warning("Cannot flush: MySQL not connected"),
            Stats = maps:update_with(sync_errors, fun(V) -> V + 1 end, 1, State#state.stats),
            NewStats = Stats#{last_error => not_connected},
            {error, not_connected, State#state{stats = NewStats}};
        true ->
            %% Try to sync the batch
            case do_sync_batch(Batch, State#state.max_retries) of
                {ok, Count} ->
                    Stats = State#state.stats,
                    NewStats = Stats#{
                        jobs_synced => maps:get(jobs_synced, Stats, 0) + Count,
                        batches_synced => maps:get(batches_synced, Stats, 0) + 1,
                        last_sync_time => erlang:system_time(second)
                    },
                    NewState = State#state{
                        pending_queue = RemainingQueue,
                        retry_count = 0,
                        stats = NewStats
                    },
                    lager:debug("Synced ~p jobs to slurmdbd", [Count]),
                    {ok, Count, NewState};
                {partial, Synced, Failed} ->
                    %% Some jobs synced, some failed
                    Stats = State#state.stats,
                    NewStats = Stats#{
                        jobs_synced => maps:get(jobs_synced, Stats, 0) + length(Synced),
                        jobs_failed => maps:get(jobs_failed, Stats, 0) + length(Failed),
                        batches_synced => maps:get(batches_synced, Stats, 0) + 1,
                        last_sync_time => erlang:system_time(second)
                    },
                    NewState = State#state{
                        pending_queue = RemainingQueue,
                        failed_jobs = State#state.failed_jobs ++ Failed,
                        retry_count = 0,
                        stats = NewStats
                    },
                    lager:warning("Partial sync: ~p succeeded, ~p failed",
                                 [length(Synced), length(Failed)]),
                    {ok, length(Synced), NewState};
                {error, Reason} ->
                    %% Complete failure, move batch to failed jobs
                    Stats = maps:update_with(sync_errors, fun(V) -> V + 1 end, 1, State#state.stats),
                    NewStats = Stats#{
                        jobs_failed => maps:get(jobs_failed, Stats, 0) + length(Batch),
                        last_error => Reason
                    },
                    NewState = State#state{
                        pending_queue = RemainingQueue,
                        failed_jobs = State#state.failed_jobs ++ Batch,
                        stats = NewStats
                    },
                    lager:error("Batch sync failed: ~p (~p jobs moved to failed)",
                               [Reason, length(Batch)]),
                    {error, Reason, NewState}
            end
    end.

%% @private Extract a batch of items from the queue
extract_batch(Queue, BatchSize) ->
    extract_batch(Queue, BatchSize, []).

extract_batch(Queue, 0, Acc) ->
    {lists:reverse(Acc), Queue};
extract_batch(Queue, N, Acc) ->
    case queue:out(Queue) of
        {{value, Item}, NewQueue} ->
            extract_batch(NewQueue, N - 1, [Item | Acc]);
        {empty, _} ->
            {lists:reverse(Acc), Queue}
    end.

%% @private Sync a batch of jobs with retry logic
do_sync_batch(Jobs, MaxRetries) ->
    do_sync_batch(Jobs, MaxRetries, 0).

do_sync_batch(Jobs, MaxRetries, Attempt) when Attempt >= MaxRetries ->
    %% Max retries exceeded, try individual sync to identify failures
    do_sync_individual(Jobs);

do_sync_batch(Jobs, MaxRetries, Attempt) ->
    case flurm_dbd_mysql:sync_job_records(Jobs) of
        {ok, Count} ->
            {ok, Count};
        {error, Reason} ->
            case should_retry(Reason, Attempt) of
                true ->
                    Delay = calculate_backoff(Attempt),
                    lager:debug("Batch sync retry ~p/~p after ~pms: ~p",
                               [Attempt + 1, MaxRetries, Delay, Reason]),
                    timer:sleep(Delay),
                    do_sync_batch(Jobs, MaxRetries, Attempt + 1);
                false ->
                    %% Non-retryable error, try individual sync
                    do_sync_individual(Jobs)
            end
    end.

%% @private Sync jobs individually to identify which ones fail
do_sync_individual(Jobs) ->
    {Synced, Failed} = lists:foldl(fun(Job, {S, F}) ->
        case flurm_dbd_mysql:sync_job_record(Job) of
            ok -> {[Job | S], F};
            {error, _} -> {S, [Job | F]}
        end
    end, {[], []}, Jobs),

    case {Synced, Failed} of
        {[], _} -> {error, all_jobs_failed};
        {_, []} -> {ok, length(Synced)};
        _ -> {partial, Synced, Failed}
    end.

%% @doc Determine if an error is retryable
should_retry(timeout, _Attempt) -> true;
should_retry(closed, _Attempt) -> true;
should_retry(econnrefused, _Attempt) -> true;
should_retry(enotconn, _Attempt) -> true;
should_retry({error, timeout}, _Attempt) -> true;
should_retry({error, closed}, _Attempt) -> true;
should_retry(_, _) -> false.

%% @doc Calculate exponential backoff delay
calculate_backoff(Attempt) ->
    BaseDelay = ?DEFAULT_RETRY_DELAY,
    %% Exponential backoff: delay * 2^attempt, capped at max
    Delay = min(BaseDelay * (1 bsl Attempt), ?DEFAULT_MAX_BACKOFF),
    %% Add jitter (0-25%)
    Jitter = rand:uniform(Delay div 4),
    Delay + Jitter.
