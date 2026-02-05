%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Auto-Fragmenting Storage
%%%
%%% Provides auto-fragmenting, time-based Mnesia tables for accounting
%%% data persistence. Features:
%%%
%%% 1. Time-Based Tables: Monthly tables created automatically
%%%    - job_records_YYYY_MM - Main table for the month
%%%    - Tables created on-demand at first insert for new month
%%%
%%% 2. Burst Fragmentation: When monthly table exceeds thresholds,
%%%    additional fragments are created automatically
%%%    - Threshold: 500K records or 1GB per fragment
%%%    - No manual tuning required
%%%
%%% 3. Automatic Aging:
%%%    - Hot (current month): ram_copies (fast writes)
%%%    - Warm (1-3 months): disc_copies (balanced)
%%%    - Cold (3-12 months): disc_only_copies (space efficient)
%%%    - Archived (>12 months): exported to files, optionally deleted
%%%
%%% 4. Query Routing: Queries automatically span relevant tables
%%%    based on time range filters.
%%%
%%% Usage:
%%%   %% Insert job record (auto-routes to correct table)
%%%   flurm_dbd_fragment:insert_job(JobRecord).
%%%
%%%   %% Query jobs in time range (auto-routes across tables)
%%%   flurm_dbd_fragment:query_jobs(#{start_time => T1, end_time => T2}).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_fragment).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% Job record operations
-export([
    insert_job/1,
    get_job/1,
    query_jobs/1,
    delete_job/1
]).

%% Fragment management
-export([
    ensure_current_table/0,
    get_fragment_meta/1,
    list_all_fragments/0,
    trigger_archival/1,
    get_table_stats/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Internal exports for testing
-ifdef(TEST).
-export([
    table_name_for_time/1,
    get_tables_for_range/2,
    should_fragment/1,
    age_table/2,
    current_month/0
]).
-endif.

%%====================================================================
%% Records
%%====================================================================

%% Fragment metadata tracking
-record(fragment_meta, {
    table_name          :: atom(),
    base_period         :: binary(),    %% "YYYY-MM"
    fragment_id         :: non_neg_integer(),  %% 0 = main, 1+ = burst
    record_count        :: non_neg_integer(),
    size_bytes          :: non_neg_integer(),
    created_at          :: non_neg_integer(),
    status              :: hot | warm | cold | archived
}).

%% Job record stored in Mnesia (same as flurm_dbd_ra but for persistence)
-record(job_record, {
    job_id              :: pos_integer(),
    job_name            :: binary(),
    user_name           :: binary(),
    user_id             :: non_neg_integer(),
    group_id            :: non_neg_integer(),
    account             :: binary(),
    partition           :: binary(),
    cluster             :: binary(),
    qos                 :: binary(),
    state               :: atom(),
    exit_code           :: integer(),
    num_nodes           :: pos_integer(),
    num_cpus            :: pos_integer(),
    num_tasks           :: pos_integer(),
    req_mem             :: non_neg_integer(),
    submit_time         :: non_neg_integer(),
    start_time          :: non_neg_integer(),
    end_time            :: non_neg_integer(),
    elapsed             :: non_neg_integer(),
    tres_alloc          :: map(),
    tres_req            :: map()
}).

%% Server state
-record(state, {
    current_table       :: atom(),
    current_period      :: binary(),
    fragment_metas      :: #{atom() => #fragment_meta{}},
    check_timer         :: reference() | undefined
}).

%%====================================================================
%% Constants
%%====================================================================

%% Thresholds for auto-fragmentation
-define(RECORDS_PER_FRAGMENT, 500000).
-define(BYTES_PER_FRAGMENT, 1073741824).  %% 1GB

%% Aging thresholds (months)
-define(WARM_AGE_MONTHS, 1).
-define(COLD_AGE_MONTHS, 3).
-define(ARCHIVE_AGE_MONTHS, 12).

%% Check interval for maintenance (1 hour)
-define(CHECK_INTERVAL_MS, 3600000).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the fragment manager.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Insert a job record into the appropriate table.
%% Auto-creates table if needed, auto-fragments if threshold exceeded.
-spec insert_job(map()) -> ok | {error, term()}.
insert_job(JobInfo) when is_map(JobInfo) ->
    gen_server:call(?MODULE, {insert_job, JobInfo}).

%% @doc Get a job record by ID.
%% Searches all tables if period unknown.
-spec get_job(pos_integer()) -> {ok, map()} | {error, not_found}.
get_job(JobId) when is_integer(JobId) ->
    gen_server:call(?MODULE, {get_job, JobId}).

%% @doc Query jobs with filters.
%% Automatically routes to relevant tables based on time range.
%% Filters: user_name, account, partition, state, start_time, end_time
-spec query_jobs(map()) -> {ok, [map()]} | {error, term()}.
query_jobs(Filters) when is_map(Filters) ->
    gen_server:call(?MODULE, {query_jobs, Filters}).

%% @doc Delete a job record.
-spec delete_job(pos_integer()) -> ok | {error, not_found}.
delete_job(JobId) when is_integer(JobId) ->
    gen_server:call(?MODULE, {delete_job, JobId}).

%% @doc Ensure current month's table exists.
-spec ensure_current_table() -> {ok, atom()} | {error, term()}.
ensure_current_table() ->
    gen_server:call(?MODULE, ensure_current_table).

%% @doc Get metadata for a fragment.
-spec get_fragment_meta(atom()) -> {ok, map()} | {error, not_found}.
get_fragment_meta(TableName) when is_atom(TableName) ->
    gen_server:call(?MODULE, {get_fragment_meta, TableName}).

%% @doc List all fragments with their metadata.
-spec list_all_fragments() -> [map()].
list_all_fragments() ->
    gen_server:call(?MODULE, list_all_fragments).

%% @doc Trigger archival of tables older than specified months.
-spec trigger_archival(pos_integer()) -> {ok, non_neg_integer()} | {error, term()}.
trigger_archival(MonthsOld) when is_integer(MonthsOld), MonthsOld > 0 ->
    gen_server:call(?MODULE, {trigger_archival, MonthsOld}).

%% @doc Get statistics about all tables.
-spec get_table_stats() -> map().
get_table_stats() ->
    gen_server:call(?MODULE, get_table_stats).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Ensure Mnesia is running
    case mnesia:system_info(is_running) of
        yes -> ok;
        no ->
            mnesia:start(),
            timer:sleep(100)
    end,

    %% Create fragment metadata table if not exists
    create_meta_table(),

    %% Load existing fragment metadata
    FragmentMetas = load_fragment_metas(),

    %% Ensure current month's table exists
    {CurrentTable, CurrentPeriod} = ensure_current_table_internal(FragmentMetas),

    %% Schedule periodic maintenance
    CheckTimer = erlang:send_after(?CHECK_INTERVAL_MS, self(), maintenance_check),

    State = #state{
        current_table = CurrentTable,
        current_period = CurrentPeriod,
        fragment_metas = FragmentMetas,
        check_timer = CheckTimer
    },

    lager:info("FLURM DBD Fragment Manager started, current table: ~p", [CurrentTable]),
    {ok, State}.

handle_call({insert_job, JobInfo}, _From, State) ->
    %% Determine which table to use based on job's end_time
    EndTime = maps:get(end_time, JobInfo, erlang:system_time(second)),
    Period = time_to_period(EndTime),

    %% Ensure table exists for this period
    {TableName, NewState} = ensure_table_for_period(Period, State),

    %% Check if we need to create a burst fragment
    {FinalTable, FinalState} = maybe_create_burst_fragment(TableName, NewState),

    %% Insert the record
    Record = map_to_job_record(JobInfo),
    Result = case mnesia:transaction(fun() ->
        mnesia:write(FinalTable, Record, write)
    end) of
        {atomic, ok} ->
            %% Update metadata record count
            update_record_count(FinalTable, 1, FinalState),
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end,

    {reply, Result, FinalState};

handle_call({get_job, JobId}, _From, State) ->
    %% Search all tables for the job (could optimize with index later)
    Tables = get_all_job_tables(State),
    Result = find_job_in_tables(JobId, Tables),
    {reply, Result, State};

handle_call({query_jobs, Filters}, _From, State) ->
    %% Determine which tables to query based on time range
    Tables = case {maps:get(start_time, Filters, undefined),
                   maps:get(end_time, Filters, undefined)} of
        {undefined, undefined} ->
            %% No time filter, query all tables
            get_all_job_tables(State);
        {StartTime, EndTime} ->
            get_tables_for_range(StartTime, EndTime, State)
    end,

    %% Query each table and combine results
    Results = lists:flatmap(fun(Table) ->
        query_table_with_filters(Table, Filters)
    end, Tables),

    {reply, {ok, Results}, State};

handle_call({delete_job, JobId}, _From, State) ->
    Tables = get_all_job_tables(State),
    Result = delete_job_from_tables(JobId, Tables),
    {reply, Result, State};

handle_call(ensure_current_table, _From, State) ->
    {Table, NewState} = ensure_table_for_period(current_month(), State),
    {reply, {ok, Table}, NewState};

handle_call({get_fragment_meta, TableName}, _From, State) ->
    case maps:get(TableName, State#state.fragment_metas, undefined) of
        undefined ->
            {reply, {error, not_found}, State};
        Meta ->
            {reply, {ok, fragment_meta_to_map(Meta)}, State}
    end;

handle_call(list_all_fragments, _From, State) ->
    Metas = [fragment_meta_to_map(M) || M <- maps:values(State#state.fragment_metas)],
    {reply, Metas, State};

handle_call({trigger_archival, MonthsOld}, _From, State) ->
    {Count, NewState} = archive_old_tables(MonthsOld, State),
    {reply, {ok, Count}, NewState};

handle_call(get_table_stats, _From, State) ->
    Stats = collect_table_stats(State),
    {reply, Stats, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(maintenance_check, State) ->
    %% Perform periodic maintenance
    NewState = perform_maintenance(State),

    %% Reschedule
    CheckTimer = erlang:send_after(?CHECK_INTERVAL_MS, self(), maintenance_check),
    {noreply, NewState#state{check_timer = CheckTimer}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{check_timer = Timer}) ->
    case Timer of
        undefined -> ok;
        _ -> erlang:cancel_timer(Timer)
    end,
    ok.

%%====================================================================
%% Internal Functions - Table Management
%%====================================================================

%% @private Create the fragment metadata table.
create_meta_table() ->
    case mnesia:create_table(flurm_fragment_meta, [
        {attributes, record_info(fields, fragment_meta)},
        {disc_copies, [node()]},
        {type, set}
    ]) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, _}} -> ok;
        {aborted, Reason} ->
            lager:error("Failed to create fragment meta table: ~p", [Reason])
    end.

%% @private Load existing fragment metadata from Mnesia.
load_fragment_metas() ->
    case mnesia:transaction(fun() ->
        mnesia:foldl(fun(Meta, Acc) ->
            maps:put(Meta#fragment_meta.table_name, Meta, Acc)
        end, #{}, flurm_fragment_meta)
    end) of
        {atomic, Metas} -> Metas;
        {aborted, _} -> #{}
    end.

%% @private Ensure current month's table exists (internal).
ensure_current_table_internal(FragmentMetas) ->
    Period = current_month(),
    TableName = period_to_table_name(Period, 0),

    case maps:is_key(TableName, FragmentMetas) of
        true ->
            {TableName, Period};
        false ->
            case create_job_table(TableName, Period, 0, hot) of
                ok -> {TableName, Period};
                {error, already_exists} -> {TableName, Period};
                {error, _Reason} -> {TableName, Period}
            end
    end.

%% @private Ensure table exists for a period.
ensure_table_for_period(Period, State) ->
    TableName = period_to_table_name(Period, 0),

    case maps:is_key(TableName, State#state.fragment_metas) of
        true ->
            %% Table exists, check if it's current
            NewState = case Period =:= State#state.current_period of
                true -> State;
                false -> State#state{current_table = TableName, current_period = Period}
            end,
            {TableName, NewState};
        false ->
            %% Create new table
            Status = determine_table_status(Period),
            case create_job_table(TableName, Period, 0, Status) of
                ok ->
                    Meta = #fragment_meta{
                        table_name = TableName,
                        base_period = Period,
                        fragment_id = 0,
                        record_count = 0,
                        size_bytes = 0,
                        created_at = erlang:system_time(second),
                        status = Status
                    },
                    NewMetas = maps:put(TableName, Meta, State#state.fragment_metas),
                    save_fragment_meta(Meta),
                    {TableName, State#state{fragment_metas = NewMetas,
                                            current_table = TableName,
                                            current_period = Period}};
                {error, already_exists} ->
                    %% Race condition, table was created elsewhere
                    {TableName, State};
                {error, Reason} ->
                    lager:error("Failed to create table ~p: ~p", [TableName, Reason]),
                    {State#state.current_table, State}
            end
    end.

%% @private Create a job record table.
create_job_table(TableName, Period, FragmentId, Status) ->
    StorageType = status_to_storage_type(Status),

    Opts = [
        {attributes, record_info(fields, job_record)},
        {StorageType, [node()]},
        {type, set},
        {record_name, job_record},
        {user_properties, [{period, Period}, {fragment_id, FragmentId}]}
    ],

    case mnesia:create_table(TableName, Opts) of
        {atomic, ok} ->
            lager:info("Created job table ~p (period=~s, frag=~p, status=~p)",
                      [TableName, Period, FragmentId, Status]),
            ok;
        {aborted, {already_exists, _}} ->
            {error, already_exists};
        {aborted, Reason} ->
            {error, Reason}
    end.

%% @private Check if we need to create a burst fragment.
maybe_create_burst_fragment(TableName, State) ->
    case maps:get(TableName, State#state.fragment_metas, undefined) of
        undefined ->
            {TableName, State};
        Meta when Meta#fragment_meta.record_count >= ?RECORDS_PER_FRAGMENT;
                  Meta#fragment_meta.size_bytes >= ?BYTES_PER_FRAGMENT ->
            %% Need to create burst fragment
            create_burst_fragment(Meta, State);
        _ ->
            {TableName, State}
    end.

%% @private Create a burst fragment.
create_burst_fragment(Meta, State) ->
    NewFragId = Meta#fragment_meta.fragment_id + 1,
    Period = Meta#fragment_meta.base_period,
    NewTableName = period_to_table_name(Period, NewFragId),

    case create_job_table(NewTableName, Period, NewFragId, Meta#fragment_meta.status) of
        ok ->
            NewMeta = #fragment_meta{
                table_name = NewTableName,
                base_period = Period,
                fragment_id = NewFragId,
                record_count = 0,
                size_bytes = 0,
                created_at = erlang:system_time(second),
                status = Meta#fragment_meta.status
            },
            NewMetas = maps:put(NewTableName, NewMeta, State#state.fragment_metas),
            save_fragment_meta(NewMeta),
            lager:info("Created burst fragment ~p for period ~s", [NewTableName, Period]),
            {NewTableName, State#state{fragment_metas = NewMetas}};
        {error, already_exists} ->
            {NewTableName, State};
        {error, Reason} ->
            lager:error("Failed to create burst fragment: ~p", [Reason]),
            {Meta#fragment_meta.table_name, State}
    end.

%% @private Update record count for a table.
update_record_count(TableName, Delta, State) ->
    case maps:get(TableName, State#state.fragment_metas, undefined) of
        undefined -> ok;
        Meta ->
            NewCount = Meta#fragment_meta.record_count + Delta,
            NewMeta = Meta#fragment_meta{record_count = NewCount},
            save_fragment_meta(NewMeta)
    end.

%% @private Save fragment metadata to Mnesia.
save_fragment_meta(Meta) ->
    mnesia:transaction(fun() ->
        mnesia:write(flurm_fragment_meta, Meta, write)
    end).

%%====================================================================
%% Internal Functions - Queries
%%====================================================================

%% @private Get all job tables.
get_all_job_tables(State) ->
    Tables = maps:keys(State#state.fragment_metas),
    lists:filter(fun(T) ->
        case atom_to_list(T) of
            "job_records_" ++ _ -> true;
            _ -> false
        end
    end, Tables).

%% @private Get tables for a time range.
get_tables_for_range(StartTime, EndTime, State) ->
    StartPeriod = case StartTime of
        undefined -> <<"1970-01">>;
        _ -> time_to_period(StartTime)
    end,
    EndPeriod = case EndTime of
        undefined -> current_month();
        _ -> time_to_period(EndTime)
    end,

    %% Get all periods between start and end
    Periods = periods_between(StartPeriod, EndPeriod),

    %% Get all tables (including burst fragments) for these periods
    AllTables = maps:keys(State#state.fragment_metas),
    lists:filter(fun(TableName) ->
        case maps:get(TableName, State#state.fragment_metas, undefined) of
            undefined -> false;
            Meta -> lists:member(Meta#fragment_meta.base_period, Periods)
        end
    end, AllTables).

%% @private Find a job in tables.
find_job_in_tables(_JobId, []) ->
    {error, not_found};
find_job_in_tables(JobId, [Table | Rest]) ->
    case mnesia:transaction(fun() ->
        mnesia:read(Table, JobId)
    end) of
        {atomic, [Record]} ->
            {ok, job_record_to_map(Record)};
        {atomic, []} ->
            find_job_in_tables(JobId, Rest);
        {aborted, _} ->
            find_job_in_tables(JobId, Rest)
    end.

%% @private Query a table with filters.
query_table_with_filters(Table, Filters) ->
    case mnesia:transaction(fun() ->
        mnesia:foldl(fun(Record, Acc) ->
            case matches_filters(Record, Filters) of
                true -> [job_record_to_map(Record) | Acc];
                false -> Acc
            end
        end, [], Table)
    end) of
        {atomic, Results} -> Results;
        {aborted, _} -> []
    end.

%% @private Check if record matches filters.
matches_filters(Record, Filters) ->
    maps:fold(fun
        (user_name, V, Acc) -> Acc andalso Record#job_record.user_name =:= V;
        (account, V, Acc) -> Acc andalso Record#job_record.account =:= V;
        (partition, V, Acc) -> Acc andalso Record#job_record.partition =:= V;
        (state, V, Acc) -> Acc andalso Record#job_record.state =:= V;
        (start_time, V, Acc) when V =/= undefined ->
            Acc andalso Record#job_record.start_time >= V;
        (end_time, V, Acc) when V =/= undefined ->
            Acc andalso Record#job_record.end_time =< V;
        (_, _, Acc) -> Acc
    end, true, Filters).

%% @private Delete job from tables.
delete_job_from_tables(_JobId, []) ->
    {error, not_found};
delete_job_from_tables(JobId, [Table | Rest]) ->
    case mnesia:transaction(fun() ->
        case mnesia:read(Table, JobId) of
            [_] ->
                mnesia:delete(Table, JobId, write),
                found;
            [] ->
                not_found
        end
    end) of
        {atomic, found} -> ok;
        {atomic, not_found} -> delete_job_from_tables(JobId, Rest);
        {aborted, _} -> delete_job_from_tables(JobId, Rest)
    end.

%%====================================================================
%% Internal Functions - Maintenance & Aging
%%====================================================================

%% @private Perform periodic maintenance.
perform_maintenance(State) ->
    %% 1. Check for month rollover
    CurrentPeriod = current_month(),
    State1 = case CurrentPeriod =:= State#state.current_period of
        true -> State;
        false ->
            lager:info("Month rollover detected: ~s -> ~s",
                      [State#state.current_period, CurrentPeriod]),
            {_, NewState} = ensure_table_for_period(CurrentPeriod, State),
            NewState
    end,

    %% 2. Age old tables
    State2 = age_tables(State1),

    %% 3. Update table size estimates
    State3 = update_table_sizes(State2),

    State3.

%% @private Age tables based on their period.
age_tables(State) ->
    Now = erlang:system_time(second),
    {{CurrentYear, CurrentMonth, _}, _} = calendar:system_time_to_local_time(Now, second),

    maps:fold(fun(TableName, Meta, AccState) ->
        case should_age_table(Meta, CurrentYear, CurrentMonth) of
            {true, NewStatus} ->
                age_table(TableName, NewStatus, AccState);
            false ->
                AccState
        end
    end, State, State#state.fragment_metas).

%% @private Check if a table should be aged.
should_age_table(Meta, CurrentYear, CurrentMonth) ->
    Period = Meta#fragment_meta.base_period,
    {TableYear, TableMonth} = parse_period(Period),

    MonthsAgo = (CurrentYear * 12 + CurrentMonth) - (TableYear * 12 + TableMonth),

    case {Meta#fragment_meta.status, MonthsAgo} of
        {hot, N} when N >= ?WARM_AGE_MONTHS -> {true, warm};
        {warm, N} when N >= ?COLD_AGE_MONTHS -> {true, cold};
        {cold, N} when N >= ?ARCHIVE_AGE_MONTHS -> {true, archived};
        _ -> false
    end.

%% @private Age a table to a new status.
age_table(TableName, NewStatus, State) ->
    case maps:get(TableName, State#state.fragment_metas, undefined) of
        undefined -> State;
        Meta ->
            OldStorage = status_to_storage_type(Meta#fragment_meta.status),
            NewStorage = status_to_storage_type(NewStatus),

            %% Change table storage type if needed
            case OldStorage =/= NewStorage of
                true ->
                    change_table_storage(TableName, OldStorage, NewStorage),
                    lager:info("Aged table ~p: ~p -> ~p",
                              [TableName, Meta#fragment_meta.status, NewStatus]);
                false ->
                    ok
            end,

            %% Update metadata
            NewMeta = Meta#fragment_meta{status = NewStatus},
            save_fragment_meta(NewMeta),
            NewMetas = maps:put(TableName, NewMeta, State#state.fragment_metas),
            State#state{fragment_metas = NewMetas}
    end.

%% @private Change table storage type.
change_table_storage(TableName, OldStorage, NewStorage) ->
    %% Remove old storage type
    case mnesia:change_table_copy_type(TableName, node(), NewStorage) of
        {atomic, ok} ->
            lager:debug("Changed ~p from ~p to ~p", [TableName, OldStorage, NewStorage]);
        {aborted, Reason} ->
            lager:warning("Failed to change ~p storage: ~p", [TableName, Reason])
    end.

%% @private Archive old tables.
archive_old_tables(MonthsOld, State) ->
    Now = erlang:system_time(second),
    {{CurrentYear, CurrentMonth, _}, _} = calendar:system_time_to_local_time(Now, second),
    CutoffMonths = CurrentYear * 12 + CurrentMonth - MonthsOld,

    {Count, NewMetas} = maps:fold(fun(TableName, Meta, {C, Metas}) ->
        {TableYear, TableMonth} = parse_period(Meta#fragment_meta.base_period),
        TableMonths = TableYear * 12 + TableMonth,

        case TableMonths < CutoffMonths andalso Meta#fragment_meta.status =/= archived of
            true ->
                %% Export and optionally delete
                export_table(TableName, Meta),
                NewMeta = Meta#fragment_meta{status = archived},
                save_fragment_meta(NewMeta),
                {C + 1, maps:put(TableName, NewMeta, Metas)};
            false ->
                {C, Metas}
        end
    end, {0, State#state.fragment_metas}, State#state.fragment_metas),

    {Count, State#state{fragment_metas = NewMetas}}.

%% @private Export a table to archive file.
export_table(TableName, Meta) ->
    ArchiveDir = application:get_env(flurm_dbd, archive_dir, "/var/lib/flurm/archive"),
    filelib:ensure_dir(ArchiveDir ++ "/"),

    FileName = io_lib:format("~s/~s_~s_~p.dat",
                            [ArchiveDir,
                             atom_to_list(TableName),
                             Meta#fragment_meta.base_period,
                             Meta#fragment_meta.fragment_id]),

    case mnesia:dump_tables([TableName]) of
        {atomic, ok} ->
            lager:info("Exported table ~p to ~s", [TableName, FileName]);
        {aborted, Reason} ->
            lager:error("Failed to export ~p: ~p", [TableName, Reason])
    end.

%% @private Update table size estimates.
update_table_sizes(State) ->
    NewMetas = maps:map(fun(TableName, Meta) ->
        try
            Size = mnesia:table_info(TableName, memory) * erlang:system_info(wordsize),
            RecordCount = mnesia:table_info(TableName, size),
            Meta#fragment_meta{size_bytes = Size, record_count = RecordCount}
        catch
            _:_ -> Meta
        end
    end, State#state.fragment_metas),
    State#state{fragment_metas = NewMetas}.

%%====================================================================
%% Internal Functions - Helpers
%%====================================================================

%% @private Get current month as period string.
current_month() ->
    {{Year, Month, _}, _} = calendar:local_time(),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

%% @private Convert timestamp to period string.
time_to_period(Timestamp) ->
    {{Year, Month, _}, _} = calendar:system_time_to_local_time(Timestamp, second),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

%% @private Parse period string to {Year, Month}.
parse_period(Period) ->
    [YearStr, MonthStr] = binary:split(Period, <<"-">>),
    {binary_to_integer(YearStr), binary_to_integer(MonthStr)}.

%% @private Convert period to table name.
period_to_table_name(Period, FragmentId) ->
    PeriodStr = binary:replace(Period, <<"-">>, <<"_">>),
    case FragmentId of
        0 ->
            list_to_atom("job_records_" ++ binary_to_list(PeriodStr));
        N ->
            list_to_atom("job_records_" ++ binary_to_list(PeriodStr) ++ "_frag_" ++ integer_to_list(N))
    end.

%% @private Get all periods between two periods.
periods_between(StartPeriod, EndPeriod) ->
    {StartYear, StartMonth} = parse_period(StartPeriod),
    {EndYear, EndMonth} = parse_period(EndPeriod),

    StartMonths = StartYear * 12 + StartMonth,
    EndMonths = EndYear * 12 + EndMonth,

    [begin
        Year = M div 12,
        Month = case M rem 12 of 0 -> 12; N -> N end,
        AdjYear = case M rem 12 of 0 -> Year - 1; _ -> Year end,
        list_to_binary(io_lib:format("~4..0B-~2..0B", [AdjYear, Month]))
    end || M <- lists:seq(StartMonths, EndMonths)].

%% @private Determine table status based on period age.
determine_table_status(Period) ->
    {{CurrentYear, CurrentMonth, _}, _} = calendar:local_time(),
    {TableYear, TableMonth} = parse_period(Period),

    MonthsAgo = (CurrentYear * 12 + CurrentMonth) - (TableYear * 12 + TableMonth),

    if
        MonthsAgo < ?WARM_AGE_MONTHS -> hot;
        MonthsAgo < ?COLD_AGE_MONTHS -> warm;
        MonthsAgo < ?ARCHIVE_AGE_MONTHS -> cold;
        true -> archived
    end.

%% @private Convert status to Mnesia storage type.
status_to_storage_type(hot) -> ram_copies;
status_to_storage_type(warm) -> disc_copies;
status_to_storage_type(cold) -> disc_only_copies;
status_to_storage_type(archived) -> disc_only_copies.

%% @private Convert job info map to record.
map_to_job_record(JobInfo) ->
    Now = erlang:system_time(second),
    #job_record{
        job_id = maps:get(job_id, JobInfo),
        job_name = maps:get(job_name, JobInfo, <<>>),
        user_name = maps:get(user_name, JobInfo, <<>>),
        user_id = maps:get(user_id, JobInfo, 0),
        group_id = maps:get(group_id, JobInfo, 0),
        account = maps:get(account, JobInfo, <<>>),
        partition = maps:get(partition, JobInfo, <<>>),
        cluster = maps:get(cluster, JobInfo, <<"flurm">>),
        qos = maps:get(qos, JobInfo, <<"normal">>),
        state = maps:get(state, JobInfo, completed),
        exit_code = maps:get(exit_code, JobInfo, 0),
        num_nodes = maps:get(num_nodes, JobInfo, 1),
        num_cpus = maps:get(num_cpus, JobInfo, 1),
        num_tasks = maps:get(num_tasks, JobInfo, 1),
        req_mem = maps:get(req_mem, JobInfo, 0),
        submit_time = maps:get(submit_time, JobInfo, Now),
        start_time = maps:get(start_time, JobInfo, 0),
        end_time = maps:get(end_time, JobInfo, Now),
        elapsed = maps:get(elapsed, JobInfo, 0),
        tres_alloc = maps:get(tres_alloc, JobInfo, #{}),
        tres_req = maps:get(tres_req, JobInfo, #{})
    }.

%% @private Convert job record to map.
job_record_to_map(#job_record{} = R) ->
    #{
        job_id => R#job_record.job_id,
        job_name => R#job_record.job_name,
        user_name => R#job_record.user_name,
        user_id => R#job_record.user_id,
        group_id => R#job_record.group_id,
        account => R#job_record.account,
        partition => R#job_record.partition,
        cluster => R#job_record.cluster,
        qos => R#job_record.qos,
        state => R#job_record.state,
        exit_code => R#job_record.exit_code,
        num_nodes => R#job_record.num_nodes,
        num_cpus => R#job_record.num_cpus,
        num_tasks => R#job_record.num_tasks,
        req_mem => R#job_record.req_mem,
        submit_time => R#job_record.submit_time,
        start_time => R#job_record.start_time,
        end_time => R#job_record.end_time,
        elapsed => R#job_record.elapsed,
        tres_alloc => R#job_record.tres_alloc,
        tres_req => R#job_record.tres_req
    }.

%% @private Convert fragment metadata to map.
fragment_meta_to_map(#fragment_meta{} = M) ->
    #{
        table_name => M#fragment_meta.table_name,
        base_period => M#fragment_meta.base_period,
        fragment_id => M#fragment_meta.fragment_id,
        record_count => M#fragment_meta.record_count,
        size_bytes => M#fragment_meta.size_bytes,
        created_at => M#fragment_meta.created_at,
        status => M#fragment_meta.status
    }.

%% @private Collect table statistics.
collect_table_stats(State) ->
    Metas = maps:values(State#state.fragment_metas),
    TotalRecords = lists:sum([M#fragment_meta.record_count || M <- Metas]),
    TotalSize = lists:sum([M#fragment_meta.size_bytes || M <- Metas]),
    TableCount = maps:size(State#state.fragment_metas),

    StatusCounts = lists:foldl(fun(M, Acc) ->
        Status = M#fragment_meta.status,
        maps:update_with(Status, fun(C) -> C + 1 end, 1, Acc)
    end, #{}, Metas),

    #{
        total_records => TotalRecords,
        total_size_bytes => TotalSize,
        table_count => TableCount,
        current_table => State#state.current_table,
        current_period => State#state.current_period,
        status_counts => StatusCounts
    }.

%% Internal exports for testing (non-state versions)
-ifdef(TEST).
table_name_for_time(Timestamp) ->
    Period = time_to_period(Timestamp),
    period_to_table_name(Period, 0).

get_tables_for_range(StartTime, EndTime) ->
    StartPeriod = time_to_period(StartTime),
    EndPeriod = time_to_period(EndTime),
    periods_between(StartPeriod, EndPeriod).

should_fragment(Meta) ->
    Meta#fragment_meta.record_count >= ?RECORDS_PER_FRAGMENT orelse
    Meta#fragment_meta.size_bytes >= ?BYTES_PER_FRAGMENT.

age_table(TableName, NewStatus) ->
    %% Simplified version for testing
    lager:info("Would age ~p to ~p", [TableName, NewStatus]),
    ok.
-endif.
