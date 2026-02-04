%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Query Engine
%%%
%%% Phase 7C-1: Query engine for accounting data.
%%%
%%% Provides query functionality for accounting data stored in the
%%% Ra-based distributed accounting system. Supports:
%%% - Query jobs by user with time range
%%% - Query jobs by account with time range
%%% - Query TRES usage by entity type
%%% - Format TRES as sacct-compatible string
%%% - Pagination support for large result sets
%%%
%%% Pagination:
%%% All list queries support pagination via Options map:
%%% - limit: maximum number of results to return
%%% - offset: number of results to skip
%%% - sort_by: field to sort by (default: submit_time)
%%% - sort_order: asc | desc (default: desc)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_query).

%% Phase 7C-1 Required Functions
-export([
    jobs_by_user/3,            %% jobs_by_user(User, StartTime, EndTime)
    jobs_by_account/3,         %% jobs_by_account(Account, StartTime, EndTime)
    tres_usage_by_user/2,      %% tres_usage_by_user(User, Period)
    tres_usage_by_account/2    %% tres_usage_by_account(Account, Period)
]).

%% Paginated versions
-export([
    jobs_by_user/4,            %% With pagination options
    jobs_by_account/4,         %% With pagination options
    query_jobs_paginated/2     %% Generic paginated query
]).

%% Original exports (backward compatibility)
-export([
    query_jobs_by_user/3,
    query_jobs_by_account/3,
    query_tres_usage/3,
    format_tres_string/1
]).

%% Additional query functions
-export([
    query_jobs/1,
    query_jobs_in_range/2,
    aggregate_user_tres/2,
    aggregate_account_tres/2,
    format_sacct_output/1
]).

%%====================================================================
%% Phase 7C-1 Required Functions
%%====================================================================

%% @doc Query jobs by user within a time range.
%% Phase 7C-1: Primary function for user job queries.
%% Returns all jobs for the specified user between StartTime and EndTime.
%% Times are unix timestamps (seconds since epoch).
-spec jobs_by_user(binary(), non_neg_integer(), non_neg_integer()) ->
    {ok, [map()]} | {error, term()}.
jobs_by_user(User, StartTime, EndTime) when is_binary(User),
                                             is_integer(StartTime),
                                             is_integer(EndTime) ->
    jobs_by_user(User, StartTime, EndTime, #{}).

%% @doc Query jobs by user with pagination options.
%% Options can include:
%% - limit: maximum results to return
%% - offset: skip first N results
%% - sort_by: field to sort by (submit_time, start_time, end_time, job_id)
%% - sort_order: asc | desc
-spec jobs_by_user(binary(), non_neg_integer(), non_neg_integer(), map()) ->
    {ok, [map()], map()} | {error, term()}.
jobs_by_user(User, StartTime, EndTime, Options) when is_binary(User),
                                                      is_integer(StartTime),
                                                      is_integer(EndTime),
                                                      is_map(Options) ->
    Filters = #{
        user_name => User,
        start_time => StartTime,
        end_time => EndTime
    },
    query_jobs_paginated(Filters, Options).

%% @doc Query jobs by account within a time range.
%% Phase 7C-1: Primary function for account job queries.
-spec jobs_by_account(binary(), non_neg_integer(), non_neg_integer()) ->
    {ok, [map()]} | {error, term()}.
jobs_by_account(Account, StartTime, EndTime) when is_binary(Account),
                                                   is_integer(StartTime),
                                                   is_integer(EndTime) ->
    jobs_by_account(Account, StartTime, EndTime, #{}).

%% @doc Query jobs by account with pagination options.
-spec jobs_by_account(binary(), non_neg_integer(), non_neg_integer(), map()) ->
    {ok, [map()], map()} | {error, term()}.
jobs_by_account(Account, StartTime, EndTime, Options) when is_binary(Account),
                                                            is_integer(StartTime),
                                                            is_integer(EndTime),
                                                            is_map(Options) ->
    Filters = #{
        account => Account,
        start_time => StartTime,
        end_time => EndTime
    },
    query_jobs_paginated(Filters, Options).

%% @doc Get TRES usage for a user over a specific period.
%% Phase 7C-1: Primary function for user TRES queries.
%% Period is in "YYYY-MM" format (e.g., "2026-02") or "YYYY-MM-DD".
-spec tres_usage_by_user(binary(), binary()) -> {ok, map()} | {error, term()}.
tres_usage_by_user(User, Period) when is_binary(User), is_binary(Period) ->
    query_tres_usage(user, User, Period).

%% @doc Get TRES usage for an account over a specific period.
%% Phase 7C-1: Primary function for account TRES queries.
-spec tres_usage_by_account(binary(), binary()) -> {ok, map()} | {error, term()}.
tres_usage_by_account(Account, Period) when is_binary(Account), is_binary(Period) ->
    query_tres_usage(account, Account, Period).

%% @doc Query jobs with pagination support.
%% Returns {ok, Jobs, Pagination} where Pagination contains:
%% - total: total number of matching records
%% - limit: limit used
%% - offset: offset used
%% - has_more: boolean indicating if more results exist
-spec query_jobs_paginated(map(), map()) ->
    {ok, [map()], map()} | {error, term()}.
query_jobs_paginated(Filters, Options) when is_map(Filters), is_map(Options) ->
    %% Get all matching jobs first
    case query_jobs(Filters) of
        {ok, AllJobs} ->
            %% Apply sorting
            SortBy = maps:get(sort_by, Options, submit_time),
            SortOrder = maps:get(sort_order, Options, desc),
            Sorted = sort_jobs(AllJobs, SortBy, SortOrder),

            %% Apply pagination
            Total = length(Sorted),
            Limit = maps:get(limit, Options, Total),
            Offset = maps:get(offset, Options, 0),

            %% Extract the page
            Paginated = paginate(Sorted, Offset, Limit),
            HasMore = (Offset + length(Paginated)) < Total,

            Pagination = #{
                total => Total,
                limit => Limit,
                offset => Offset,
                has_more => HasMore,
                returned => length(Paginated)
            },
            {ok, Paginated, Pagination};
        {error, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% Original API Functions (Backward Compatibility)
%%====================================================================

%% @doc Query jobs by user within a time range (original API).
-spec query_jobs_by_user(binary(), non_neg_integer(), non_neg_integer()) ->
    {ok, [map()]} | {error, term()}.
query_jobs_by_user(User, StartTime, EndTime) when is_binary(User),
                                                  is_integer(StartTime),
                                                  is_integer(EndTime) ->
    Filters = #{
        user_name => User,
        start_time => StartTime,
        end_time => EndTime
    },
    query_jobs(Filters).

%% @doc Query jobs by account within a time range.
%% Returns all jobs for the specified account between StartTime and EndTime.
%% Times are unix timestamps (seconds since epoch).
-spec query_jobs_by_account(binary(), non_neg_integer(), non_neg_integer()) ->
    {ok, [map()]} | {error, term()}.
query_jobs_by_account(Account, StartTime, EndTime) when is_binary(Account),
                                                        is_integer(StartTime),
                                                        is_integer(EndTime) ->
    Filters = #{
        account => Account,
        start_time => StartTime,
        end_time => EndTime
    },
    query_jobs(Filters).

%% @doc Query TRES usage for an entity.
%% EntityType is 'user', 'account', or 'cluster'.
%% EntityId is the user name, account name, or cluster name.
%% Period is in "YYYY-MM" format.
-spec query_tres_usage(user | account | cluster, binary(), binary()) ->
    {ok, map()} | {error, term()}.
query_tres_usage(EntityType, EntityId, Period) when is_atom(EntityType),
                                                    is_binary(EntityId),
                                                    is_binary(Period) ->
    %% Try Ra first, fall back to local server
    case whereis(flurm_dbd_ra) of
        undefined ->
            %% Ra not running, query from local server
            query_tres_from_server(EntityType, EntityId, Period);
        _ ->
            flurm_dbd_ra:get_tres_usage(EntityType, EntityId, Period)
    end.

%% @doc Format a TRES map as a sacct-compatible string.
%% Returns a string like "cpu=123,mem=456M,node=7,gres/gpu=2"
-spec format_tres_string(map()) -> binary().
format_tres_string(TresMap) when is_map(TresMap) ->
    Parts = lists:filtermap(fun({Key, Value}) ->
        format_tres_part(Key, Value)
    end, maps:to_list(TresMap)),
    case Parts of
        [] -> <<>>;
        _ -> list_to_binary(lists:join(",", Parts))
    end.

%% @doc Generic job query with filters.
%% Filters can include: user_name, account, partition, state, start_time, end_time
-spec query_jobs(map()) -> {ok, [map()]} | {error, term()}.
query_jobs(Filters) when is_map(Filters) ->
    %% Try Ra first for consistent reads, fall back to local server
    case whereis(flurm_dbd_ra) of
        undefined ->
            %% Ra not running, query from local server
            query_jobs_from_server(Filters);
        _ ->
            flurm_dbd_ra:query_jobs(Filters)
    end.

%% @doc Query jobs within a time range regardless of user/account.
-spec query_jobs_in_range(non_neg_integer(), non_neg_integer()) ->
    {ok, [map()]} | {error, term()}.
query_jobs_in_range(StartTime, EndTime) when is_integer(StartTime),
                                             is_integer(EndTime) ->
    Filters = #{
        start_time => StartTime,
        end_time => EndTime
    },
    query_jobs(Filters).

%% @doc Aggregate TRES usage for a user across all periods.
%% Returns total TRES usage for the user.
-spec aggregate_user_tres(binary(), [binary()]) -> map().
aggregate_user_tres(User, Periods) when is_binary(User), is_list(Periods) ->
    aggregate_entity_tres(user, User, Periods).

%% @doc Aggregate TRES usage for an account across all periods.
%% Returns total TRES usage for the account.
-spec aggregate_account_tres(binary(), [binary()]) -> map().
aggregate_account_tres(Account, Periods) when is_binary(Account), is_list(Periods) ->
    aggregate_entity_tres(account, Account, Periods).

%% @doc Format job records for sacct-style output.
%% Returns an iolist suitable for output.
-spec format_sacct_output([map()]) -> iolist().
format_sacct_output(Jobs) when is_list(Jobs) ->
    Header = io_lib:format("~-12s ~-10s ~-10s ~-12s ~-12s ~-10s ~-8s ~-30s~n",
                           ["JobID", "User", "Account", "Partition", "State", "Elapsed", "ExitCode", "AllocTRES"]),
    Separator = lists:duplicate(110, $-) ++ "\n",
    Rows = [format_sacct_row(Job) || Job <- Jobs],
    [Header, Separator | Rows].

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Format a single TRES component.
format_tres_part(cpu_seconds, Value) when Value > 0 ->
    {true, io_lib:format("cpu=~B", [Value])};
format_tres_part(cpu, Value) when Value > 0 ->
    {true, io_lib:format("cpu=~B", [Value])};
format_tres_part(mem_seconds, Value) when Value > 0 ->
    %% Convert to megabytes if large
    {true, format_memory(Value)};
format_tres_part(mem, Value) when Value > 0 ->
    {true, format_memory(Value)};
format_tres_part(node_seconds, Value) when Value > 0 ->
    {true, io_lib:format("node=~B", [Value])};
format_tres_part(node, Value) when Value > 0 ->
    {true, io_lib:format("node=~B", [Value])};
format_tres_part(gpu_seconds, Value) when Value > 0 ->
    {true, io_lib:format("gres/gpu=~B", [Value])};
format_tres_part(gpu, Value) when Value > 0 ->
    {true, io_lib:format("gres/gpu=~B", [Value])};
format_tres_part(billing, Value) when Value > 0 ->
    {true, io_lib:format("billing=~B", [Value])};
format_tres_part(energy, Value) when Value > 0 ->
    {true, io_lib:format("energy=~B", [Value])};
format_tres_part(_Key, _Value) ->
    false.

%% @private Format memory value with appropriate suffix.
format_memory(Value) when Value >= 1073741824 ->  %% >= 1TB
    io_lib:format("mem=~.2fT", [Value / 1073741824]);
format_memory(Value) when Value >= 1048576 ->     %% >= 1GB
    io_lib:format("mem=~.2fG", [Value / 1048576]);
format_memory(Value) when Value >= 1024 ->        %% >= 1MB
    io_lib:format("mem=~.2fM", [Value / 1024]);
format_memory(Value) ->
    io_lib:format("mem=~BK", [Value]).

%% @private Query jobs from the local server (fallback).
query_jobs_from_server(Filters) ->
    try
        %% Use the existing flurm_dbd_server for queries
        AllJobs = flurm_dbd_server:list_job_records(Filters),
        %% Apply additional time filtering if needed
        FilteredJobs = apply_time_filters(AllJobs, Filters),
        {ok, FilteredJobs}
    catch
        _:Reason ->
            {error, Reason}
    end.

%% @private Apply time-based filters to job list.
apply_time_filters(Jobs, Filters) ->
    StartTime = maps:get(start_time, Filters, 0),
    EndTime = maps:get(end_time, Filters, erlang:system_time(second) + 86400),
    lists:filter(fun(Job) ->
        JobStart = maps:get(start_time, Job, 0),
        JobEnd = maps:get(end_time, Job, erlang:system_time(second)),
        JobStart >= StartTime andalso JobEnd =< EndTime
    end, Jobs).

%% @private Query TRES from local server (fallback).
query_tres_from_server(EntityType, EntityId, _Period) ->
    try
        case EntityType of
            user ->
                Usage = flurm_dbd_server:calculate_user_tres_usage(EntityId),
                {ok, Usage};
            account ->
                Usage = flurm_dbd_server:calculate_account_tres_usage(EntityId),
                {ok, Usage};
            cluster ->
                Usage = flurm_dbd_server:calculate_cluster_tres_usage(),
                {ok, Usage}
        end
    catch
        _:Reason ->
            {error, Reason}
    end.

%% @private Aggregate TRES for an entity across periods.
aggregate_entity_tres(EntityType, EntityId, Periods) ->
    Results = lists:filtermap(fun(Period) ->
        case query_tres_usage(EntityType, EntityId, Period) of
            {ok, Usage} -> {true, Usage};
            {error, _} -> false
        end
    end, Periods),
    %% Aggregate all results
    lists:foldl(fun(Usage, Acc) ->
        merge_tres_maps(Usage, Acc)
    end, empty_tres_map(), Results).

%% @private Merge two TRES maps by summing values.
merge_tres_maps(Map1, Map2) ->
    maps:fold(fun(Key, Value, Acc) ->
        maps:update_with(Key, fun(V) -> V + Value end, Value, Acc)
    end, Map2, Map1).

%% @private Return an empty TRES map.
empty_tres_map() ->
    #{
        cpu_seconds => 0,
        mem_seconds => 0,
        gpu_seconds => 0,
        node_seconds => 0,
        job_count => 0,
        job_time => 0
    }.

%% @private Format a single job record as a sacct output row.
format_sacct_row(Job) when is_map(Job) ->
    JobId = integer_to_list(maps:get(job_id, Job, 0)),
    User = binary_to_list(maps:get(user_name, Job, <<>>)),
    Account = binary_to_list(maps:get(account, Job, <<>>)),
    Partition = binary_to_list(maps:get(partition, Job, <<>>)),
    State = atom_to_list(maps:get(state, Job, unknown)),
    Elapsed = format_elapsed(maps:get(elapsed, Job, 0)),
    ExitCode = format_exit_code(maps:get(exit_code, Job, 0)),
    TresAlloc = format_tres_string(maps:get(tres_alloc, Job, #{})),
    io_lib:format("~-12s ~-10s ~-10s ~-12s ~-12s ~-10s ~-8s ~-30s~n",
                  [JobId, User, Account, Partition, State, Elapsed, ExitCode, TresAlloc]).

%% @private Format elapsed time as HH:MM:SS.
format_elapsed(Seconds) when is_integer(Seconds), Seconds >= 0 ->
    Hours = Seconds div 3600,
    Minutes = (Seconds rem 3600) div 60,
    Secs = Seconds rem 60,
    io_lib:format("~2..0B:~2..0B:~2..0B", [Hours, Minutes, Secs]);
format_elapsed(_) ->
    "00:00:00".

%% @private Format exit code as ExitCode:Signal.
format_exit_code(Code) when is_integer(Code) ->
    io_lib:format("~B:0", [Code]);
format_exit_code(_) ->
    "0:0".

%%====================================================================
%% Pagination Helpers
%%====================================================================

%% @private Sort jobs by specified field and order.
sort_jobs(Jobs, SortBy, SortOrder) ->
    Comparator = make_comparator(SortBy, SortOrder),
    lists:sort(Comparator, Jobs).

%% @private Create a comparator function for sorting.
make_comparator(SortBy, asc) ->
    fun(A, B) ->
        maps:get(SortBy, A, 0) =< maps:get(SortBy, B, 0)
    end;
make_comparator(SortBy, desc) ->
    fun(A, B) ->
        maps:get(SortBy, A, 0) >= maps:get(SortBy, B, 0)
    end.

%% @private Paginate a list with offset and limit.
paginate(List, Offset, Limit) ->
    %% Skip offset elements, then take limit elements
    AfterOffset = lists:nthtail(min(Offset, length(List)), List),
    lists:sublist(AfterOffset, Limit).
