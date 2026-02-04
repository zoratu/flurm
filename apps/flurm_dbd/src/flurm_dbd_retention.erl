%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Data Retention Module
%%%
%%% Provides data pruning and retention management for accounting data.
%%% Supports configurable retention periods and automatic cleanup.
%%%
%%% Default retention periods:
%%% - Job records: 365 days (1 year)
%%% - Usage records: 730 days (2 years)
%%% - Archive records: 1095 days (3 years)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_retention).

-export([
    prune_old_records/1,
    get_retention_config/0,
    set_retention_config/1
]).

%% Additional retention functions
-export([
    prune_job_records/1,
    prune_usage_records/1,
    archive_records/1,
    get_archive_stats/0,
    schedule_prune/1,
    cancel_scheduled_prune/0
]).

%% Default retention periods (days)
-define(DEFAULT_JOB_RETENTION_DAYS, 365).
-define(DEFAULT_USAGE_RETENTION_DAYS, 730).
-define(DEFAULT_ARCHIVE_RETENTION_DAYS, 1095).
-define(SECONDS_PER_DAY, 86400).

%%====================================================================
%% Types
%%====================================================================

-type retention_config() :: #{
    job_retention_days => pos_integer(),
    usage_retention_days => pos_integer(),
    archive_retention_days => pos_integer(),
    auto_prune_enabled => boolean(),
    auto_prune_interval_hours => pos_integer()
}.

-type prune_result() :: #{
    jobs_pruned => non_neg_integer(),
    usage_pruned => non_neg_integer(),
    archived => non_neg_integer(),
    errors => [term()]
}.

%%====================================================================
%% API Functions
%%====================================================================

%% @doc Prune records older than the specified retention period.
%% RetentionDays specifies how many days of records to keep.
-spec prune_old_records(pos_integer()) -> {ok, prune_result()} | {error, term()}.
prune_old_records(RetentionDays) when is_integer(RetentionDays), RetentionDays > 0 ->
    Cutoff = erlang:system_time(second) - (RetentionDays * ?SECONDS_PER_DAY),

    %% Prune jobs, then usage records
    JobResult = prune_job_records(Cutoff),
    UsageResult = prune_usage_records(Cutoff),

    %% Combine results
    CombinedResult = #{
        jobs_pruned => maps:get(count, JobResult, 0),
        usage_pruned => maps:get(count, UsageResult, 0),
        archived => 0,
        errors => maps:get(errors, JobResult, []) ++ maps:get(errors, UsageResult, [])
    },

    case maps:get(errors, CombinedResult) of
        [] -> {ok, CombinedResult};
        _ -> {ok, CombinedResult}  %% Return ok with errors included
    end.

%% @doc Get the current retention configuration.
-spec get_retention_config() -> retention_config().
get_retention_config() ->
    #{
        job_retention_days => get_env(job_retention_days, ?DEFAULT_JOB_RETENTION_DAYS),
        usage_retention_days => get_env(usage_retention_days, ?DEFAULT_USAGE_RETENTION_DAYS),
        archive_retention_days => get_env(archive_retention_days, ?DEFAULT_ARCHIVE_RETENTION_DAYS),
        auto_prune_enabled => get_env(auto_prune_enabled, false),
        auto_prune_interval_hours => get_env(auto_prune_interval_hours, 24)
    }.

%% @doc Set the retention configuration.
%% Validates and applies the provided configuration.
-spec set_retention_config(retention_config()) -> ok | {error, term()}.
set_retention_config(Config) when is_map(Config) ->
    %% Validate configuration
    case validate_config(Config) of
        ok ->
            %% Apply each setting
            maps:foreach(fun(Key, Value) ->
                application:set_env(flurm_dbd, Key, Value)
            end, Config),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Prune job records older than the cutoff timestamp.
-spec prune_job_records(non_neg_integer()) -> #{count => non_neg_integer(), errors => [term()]}.
prune_job_records(CutoffTime) when is_integer(CutoffTime) ->
    try
        %% First, archive old records
        {ok, ArchivedCount} = flurm_dbd_server:archive_old_records(cutoff_to_days(CutoffTime)),

        %% Then purge from main tables
        {ok, PurgedCount} = flurm_dbd_server:purge_old_records(cutoff_to_days(CutoffTime)),

        #{count => ArchivedCount + PurgedCount, errors => []}
    catch
        _:Reason ->
            #{count => 0, errors => [{job_prune_error, Reason}]}
    end.

%% @doc Prune usage records older than the cutoff timestamp.
-spec prune_usage_records(non_neg_integer()) -> #{count => non_neg_integer(), errors => [term()]}.
prune_usage_records(CutoffTime) when is_integer(CutoffTime) ->
    %% Usage records are keyed by period (YYYY-MM)
    %% Calculate the cutoff period
    CutoffPeriod = timestamp_to_period(CutoffTime),

    try
        %% For now, usage pruning would need to be implemented in flurm_dbd_server
        %% This is a placeholder that returns 0 pruned
        lager:info("Would prune usage records older than period ~s", [CutoffPeriod]),
        #{count => 0, errors => []}
    catch
        _:Reason ->
            #{count => 0, errors => [{usage_prune_error, Reason}]}
    end.

%% @doc Archive records older than the specified days.
%% Moves records from active storage to archive storage.
-spec archive_records(pos_integer()) -> {ok, non_neg_integer()} | {error, term()}.
archive_records(DaysOld) when is_integer(DaysOld), DaysOld > 0 ->
    try
        flurm_dbd_server:archive_old_jobs(DaysOld)
    catch
        _:Reason ->
            {error, Reason}
    end.

%% @doc Get statistics about archived records.
-spec get_archive_stats() -> map().
get_archive_stats() ->
    try
        ArchivedJobs = flurm_dbd_server:get_archived_jobs(),
        Stats = flurm_dbd_server:get_stats(),
        #{
            archived_job_count => length(ArchivedJobs),
            total_archived => maps:get(archived_jobs, Stats, 0),
            oldest_archive => find_oldest_timestamp(ArchivedJobs),
            newest_archive => find_newest_timestamp(ArchivedJobs)
        }
    catch
        _:_ ->
            #{
                archived_job_count => 0,
                total_archived => 0,
                oldest_archive => 0,
                newest_archive => 0
            }
    end.

%% @doc Schedule automatic pruning at the specified interval.
%% IntervalHours specifies how often to run the pruning (in hours).
-spec schedule_prune(pos_integer()) -> {ok, reference()} | {error, term()}.
schedule_prune(IntervalHours) when is_integer(IntervalHours), IntervalHours > 0 ->
    IntervalMs = IntervalHours * 3600 * 1000,
    Config = get_retention_config(),
    RetentionDays = maps:get(job_retention_days, Config, ?DEFAULT_JOB_RETENTION_DAYS),

    %% Schedule periodic pruning
    TimerRef = erlang:send_after(IntervalMs, self(), {scheduled_prune, RetentionDays, IntervalMs}),

    %% Store the timer reference in application env for cancellation
    application:set_env(flurm_dbd, prune_timer_ref, TimerRef),

    lager:info("Scheduled automatic pruning every ~p hours", [IntervalHours]),
    {ok, TimerRef}.

%% @doc Cancel any scheduled pruning.
-spec cancel_scheduled_prune() -> ok.
cancel_scheduled_prune() ->
    case application:get_env(flurm_dbd, prune_timer_ref) of
        {ok, TimerRef} ->
            erlang:cancel_timer(TimerRef),
            application:unset_env(flurm_dbd, prune_timer_ref),
            lager:info("Cancelled scheduled pruning"),
            ok;
        undefined ->
            ok
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Get environment variable with default.
get_env(Key, Default) ->
    application:get_env(flurm_dbd, Key, Default).

%% @private Validate retention configuration.
validate_config(Config) when is_map(Config) ->
    Validators = [
        {job_retention_days, fun is_positive_integer/1},
        {usage_retention_days, fun is_positive_integer/1},
        {archive_retention_days, fun is_positive_integer/1},
        {auto_prune_enabled, fun is_boolean/1},
        {auto_prune_interval_hours, fun is_positive_integer/1}
    ],

    Errors = lists:filtermap(fun({Key, Validator}) ->
        case maps:find(Key, Config) of
            {ok, Value} ->
                case Validator(Value) of
                    true -> false;
                    false -> {true, {invalid_value, Key, Value}}
                end;
            error ->
                false  %% Key not present, that's ok
        end
    end, Validators),

    case Errors of
        [] -> ok;
        _ -> {error, {validation_failed, Errors}}
    end.

%% @private Check if value is a positive integer.
is_positive_integer(Value) when is_integer(Value), Value > 0 -> true;
is_positive_integer(_) -> false.

%% @private Convert cutoff timestamp to days from now.
cutoff_to_days(CutoffTime) ->
    Now = erlang:system_time(second),
    max(1, (Now - CutoffTime) div ?SECONDS_PER_DAY).

%% @private Convert timestamp to period string (YYYY-MM).
timestamp_to_period(Timestamp) ->
    {{Year, Month, _Day}, _Time} = calendar:gregorian_seconds_to_datetime(
        Timestamp + 62167219200  %% Convert from Unix epoch to Gregorian
    ),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

%% @private Find the oldest timestamp in a list of jobs.
find_oldest_timestamp([]) -> 0;
find_oldest_timestamp(Jobs) ->
    lists:foldl(fun(Job, Oldest) ->
        EndTime = maps:get(end_time, Job, 0),
        case EndTime of
            0 -> Oldest;
            _ when Oldest =:= 0 -> EndTime;
            _ -> min(EndTime, Oldest)
        end
    end, 0, Jobs).

%% @private Find the newest timestamp in a list of jobs.
find_newest_timestamp([]) -> 0;
find_newest_timestamp(Jobs) ->
    lists:foldl(fun(Job, Newest) ->
        EndTime = maps:get(end_time, Job, 0),
        max(EndTime, Newest)
    end, 0, Jobs).
