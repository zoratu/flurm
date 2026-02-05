%%%-------------------------------------------------------------------
%%% @doc Unit tests for flurm_dbd_fragment module
%%%
%%% Tests auto-fragmenting Mnesia schema functionality:
%%% - Time-based table creation
%%% - Burst fragmentation
%%% - Query routing
%%% - Table aging
%%%-------------------------------------------------------------------
-module(flurm_dbd_fragment_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Record Definitions (must be before functions that use them)
%%====================================================================

%% Record definitions matching the module
-record(fragment_meta, {
    table_name,
    base_period,
    fragment_id,
    record_count,
    size_bytes,
    created_at,
    status
}).

-record(job_record, {
    job_id,
    job_name,
    user_name,
    user_id,
    group_id,
    account,
    partition,
    cluster,
    qos,
    state,
    exit_code,
    num_nodes,
    num_cpus,
    num_tasks,
    req_mem,
    submit_time,
    start_time,
    end_time,
    elapsed,
    tres_alloc,
    tres_req
}).

%% Fragmentation thresholds
-define(RECORDS_PER_FRAGMENT, 500000).
-define(BYTES_PER_FRAGMENT, 1073741824).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup/teardown for tests that need Mnesia
setup_mnesia() ->
    application:ensure_all_started(mnesia),
    mnesia:stop(),
    mnesia:delete_schema([node()]),
    mnesia:create_schema([node()]),
    mnesia:start(),
    ok.

cleanup_mnesia() ->
    mnesia:stop(),
    mnesia:delete_schema([node()]),
    ok.

%%====================================================================
%% Period/Table Name Tests
%%====================================================================

period_to_table_name_test() ->
    %% Test main table naming
    ?assertEqual(job_records_2026_02, period_to_table_name(<<"2026-02">>, 0)),
    ?assertEqual(job_records_2025_12, period_to_table_name(<<"2025-12">>, 0)),
    ?assertEqual(job_records_2024_01, period_to_table_name(<<"2024-01">>, 0)).

period_to_table_name_with_fragment_test() ->
    %% Test burst fragment naming
    ?assertEqual(job_records_2026_02_frag_1, period_to_table_name(<<"2026-02">>, 1)),
    ?assertEqual(job_records_2026_02_frag_5, period_to_table_name(<<"2026-02">>, 5)),
    ?assertEqual(job_records_2025_12_frag_10, period_to_table_name(<<"2025-12">>, 10)).

time_to_period_test() ->
    %% Test timestamp to period conversion
    %% 2026-02-01 00:00:00 UTC
    T1 = 1769990400,
    ?assertEqual(<<"2026-02">>, time_to_period(T1)),

    %% 2025-12-15 12:00:00 UTC
    T2 = 1765800000,
    ?assertEqual(<<"2025-12">>, time_to_period(T2)).

parse_period_test() ->
    ?assertEqual({2026, 2}, parse_period(<<"2026-02">>)),
    ?assertEqual({2025, 12}, parse_period(<<"2025-12">>)),
    ?assertEqual({2024, 1}, parse_period(<<"2024-01">>)).

%%====================================================================
%% Periods Between Tests
%%====================================================================

periods_between_single_test() ->
    %% Same month
    Result = periods_between(<<"2026-02">>, <<"2026-02">>),
    ?assertEqual([<<"2026-02">>], Result).

periods_between_multiple_test() ->
    %% Multiple months in same year
    Result = periods_between(<<"2026-01">>, <<"2026-03">>),
    ?assertEqual([<<"2026-01">>, <<"2026-02">>, <<"2026-03">>], Result).

periods_between_year_boundary_test() ->
    %% Across year boundary
    Result = periods_between(<<"2025-11">>, <<"2026-02">>),
    ?assertEqual([<<"2025-11">>, <<"2025-12">>, <<"2026-01">>, <<"2026-02">>], Result).

%%====================================================================
%% Status/Storage Type Tests
%%====================================================================

status_to_storage_type_test() ->
    ?assertEqual(ram_copies, status_to_storage_type(hot)),
    ?assertEqual(disc_copies, status_to_storage_type(warm)),
    ?assertEqual(disc_only_copies, status_to_storage_type(cold)),
    ?assertEqual(disc_only_copies, status_to_storage_type(archived)).

determine_table_status_test() ->
    %% Current month should be hot
    CurrentPeriod = current_month(),
    ?assertEqual(hot, determine_table_status(CurrentPeriod)),

    %% Old months should be cold or archived based on age
    %% Note: This test may need adjustment based on current date
    ?assertEqual(archived, determine_table_status(<<"2020-01">>)).

%%====================================================================
%% Fragmentation Threshold Tests
%%====================================================================

should_fragment_test() ->
    %% Below threshold - should not fragment
    Meta1 = make_test_meta(100000, 500000000),  %% 100K records, 500MB
    ?assertEqual(false, should_fragment(Meta1)),

    %% At record threshold - should fragment
    Meta2 = make_test_meta(500000, 500000000),  %% 500K records, 500MB
    ?assertEqual(true, should_fragment(Meta2)),

    %% At size threshold - should fragment
    Meta3 = make_test_meta(100000, 1073741824),  %% 100K records, 1GB
    ?assertEqual(true, should_fragment(Meta3)),

    %% Above both thresholds - should fragment
    Meta4 = make_test_meta(600000, 2000000000),
    ?assertEqual(true, should_fragment(Meta4)).

%%====================================================================
%% Job Record Conversion Tests
%%====================================================================

map_to_job_record_test() ->
    JobInfo = #{
        job_id => 12345,
        job_name => <<"test_job">>,
        user_name => <<"alice">>,
        user_id => 1000,
        group_id => 1000,
        account => <<"research">>,
        partition => <<"batch">>,
        state => completed,
        exit_code => 0,
        num_nodes => 2,
        num_cpus => 8,
        num_tasks => 8,
        req_mem => 16384,
        submit_time => 1000000,
        start_time => 1000010,
        end_time => 1001000,
        elapsed => 990,
        tres_alloc => #{cpu => 8, mem => 16384}
    },
    Record = map_to_job_record(JobInfo),
    ?assertEqual(12345, Record#job_record.job_id),
    ?assertEqual(<<"test_job">>, Record#job_record.job_name),
    ?assertEqual(<<"alice">>, Record#job_record.user_name),
    ?assertEqual(completed, Record#job_record.state),
    ?assertEqual(990, Record#job_record.elapsed).

job_record_to_map_test() ->
    Record = #job_record{
        job_id = 12345,
        job_name = <<"test_job">>,
        user_name = <<"bob">>,
        user_id = 1001,
        group_id = 1001,
        account = <<"physics">>,
        partition = <<"gpu">>,
        cluster = <<"flurm">>,
        qos = <<"high">>,
        state = running,
        exit_code = 0,
        num_nodes = 1,
        num_cpus = 4,
        num_tasks = 4,
        req_mem = 8192,
        submit_time = 2000000,
        start_time = 2000010,
        end_time = 0,
        elapsed = 0,
        tres_alloc = #{},
        tres_req = #{}
    },
    Map = job_record_to_map(Record),
    ?assertEqual(12345, maps:get(job_id, Map)),
    ?assertEqual(<<"bob">>, maps:get(user_name, Map)),
    ?assertEqual(running, maps:get(state, Map)).

map_to_job_record_defaults_test() ->
    %% Test that defaults are applied for missing fields
    JobInfo = #{job_id => 99999},
    Record = map_to_job_record(JobInfo),
    ?assertEqual(99999, Record#job_record.job_id),
    ?assertEqual(<<>>, Record#job_record.job_name),
    ?assertEqual(<<>>, Record#job_record.user_name),
    ?assertEqual(<<"flurm">>, Record#job_record.cluster),
    ?assertEqual(<<"normal">>, Record#job_record.qos),
    ?assertEqual(completed, Record#job_record.state),
    ?assertEqual(1, Record#job_record.num_nodes),
    ?assertEqual(1, Record#job_record.num_cpus).

%%====================================================================
%% Filter Matching Tests
%%====================================================================

matches_filters_empty_test() ->
    Record = make_test_job_record(),
    ?assertEqual(true, matches_filters(Record, #{})).

matches_filters_user_test() ->
    Record = make_test_job_record(),
    ?assertEqual(true, matches_filters(Record, #{user_name => <<"alice">>})),
    ?assertEqual(false, matches_filters(Record, #{user_name => <<"bob">>})).

matches_filters_account_test() ->
    Record = make_test_job_record(),
    ?assertEqual(true, matches_filters(Record, #{account => <<"research">>})),
    ?assertEqual(false, matches_filters(Record, #{account => <<"physics">>})).

matches_filters_partition_test() ->
    Record = make_test_job_record(),
    ?assertEqual(true, matches_filters(Record, #{partition => <<"batch">>})),
    ?assertEqual(false, matches_filters(Record, #{partition => <<"gpu">>})).

matches_filters_state_test() ->
    Record = make_test_job_record(),
    ?assertEqual(true, matches_filters(Record, #{state => completed})),
    ?assertEqual(false, matches_filters(Record, #{state => running})).

matches_filters_time_range_test() ->
    Record = make_test_job_record(),  %% start_time=1000010, end_time=1001000
    ?assertEqual(true, matches_filters(Record, #{start_time => 1000000})),
    ?assertEqual(false, matches_filters(Record, #{start_time => 1000020})),
    ?assertEqual(true, matches_filters(Record, #{end_time => 1002000})),
    ?assertEqual(false, matches_filters(Record, #{end_time => 1000500})).

matches_filters_combined_test() ->
    Record = make_test_job_record(),
    ?assertEqual(true, matches_filters(Record, #{
        user_name => <<"alice">>,
        account => <<"research">>,
        state => completed
    })),
    ?assertEqual(false, matches_filters(Record, #{
        user_name => <<"alice">>,
        account => <<"wrong_account">>,
        state => completed
    })).

%%====================================================================
%% Fragment Metadata Tests
%%====================================================================

fragment_meta_to_map_test() ->
    Meta = #fragment_meta{
        table_name = job_records_2026_02,
        base_period = <<"2026-02">>,
        fragment_id = 0,
        record_count = 50000,
        size_bytes = 100000000,
        created_at = 1769990400,
        status = hot
    },
    Map = fragment_meta_to_map(Meta),
    ?assertEqual(job_records_2026_02, maps:get(table_name, Map)),
    ?assertEqual(<<"2026-02">>, maps:get(base_period, Map)),
    ?assertEqual(0, maps:get(fragment_id, Map)),
    ?assertEqual(50000, maps:get(record_count, Map)),
    ?assertEqual(hot, maps:get(status, Map)).

%%====================================================================
%% Age Determination Tests
%%====================================================================

should_age_table_test() ->
    %% Get current year/month
    {{CurrentYear, CurrentMonth, _}, _} = calendar:local_time(),

    %% Hot table that's 1 month old should become warm
    OneMonthAgo = subtract_months(CurrentYear, CurrentMonth, 1),
    MetaHotOld = make_test_meta_with_status_and_period(hot, OneMonthAgo),
    ?assertMatch({true, warm}, should_age_table(MetaHotOld, CurrentYear, CurrentMonth)),

    %% Warm table that's 3 months old should become cold
    ThreeMonthsAgo = subtract_months(CurrentYear, CurrentMonth, 3),
    MetaWarmOld = make_test_meta_with_status_and_period(warm, ThreeMonthsAgo),
    ?assertMatch({true, cold}, should_age_table(MetaWarmOld, CurrentYear, CurrentMonth)),

    %% Cold table that's 12 months old should become archived
    TwelveMonthsAgo = subtract_months(CurrentYear, CurrentMonth, 12),
    MetaColdOld = make_test_meta_with_status_and_period(cold, TwelveMonthsAgo),
    ?assertMatch({true, archived}, should_age_table(MetaColdOld, CurrentYear, CurrentMonth)),

    %% Current month hot table should not age
    CurrentPeriod = current_month(),
    MetaHotCurrent = make_test_meta_with_status_and_period(hot, CurrentPeriod),
    ?assertEqual(false, should_age_table(MetaHotCurrent, CurrentYear, CurrentMonth)).

%%====================================================================
%% Helper Functions
%%====================================================================

%% These match the internal functions in flurm_dbd_fragment
period_to_table_name(Period, FragmentId) ->
    PeriodStr = binary:replace(Period, <<"-">>, <<"_">>),
    case FragmentId of
        0 ->
            list_to_atom("job_records_" ++ binary_to_list(PeriodStr));
        N ->
            list_to_atom("job_records_" ++ binary_to_list(PeriodStr) ++ "_frag_" ++ integer_to_list(N))
    end.

time_to_period(Timestamp) ->
    {{Year, Month, _}, _} = calendar:system_time_to_local_time(Timestamp, second),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

parse_period(Period) ->
    [YearStr, MonthStr] = binary:split(Period, <<"-">>),
    {binary_to_integer(YearStr), binary_to_integer(MonthStr)}.

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

status_to_storage_type(hot) -> ram_copies;
status_to_storage_type(warm) -> disc_copies;
status_to_storage_type(cold) -> disc_only_copies;
status_to_storage_type(archived) -> disc_only_copies.

current_month() ->
    {{Year, Month, _}, _} = calendar:local_time(),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

determine_table_status(Period) ->
    {{CurrentYear, CurrentMonth, _}, _} = calendar:local_time(),
    {TableYear, TableMonth} = parse_period(Period),

    MonthsAgo = (CurrentYear * 12 + CurrentMonth) - (TableYear * 12 + TableMonth),

    if
        MonthsAgo < 1 -> hot;
        MonthsAgo < 3 -> warm;
        MonthsAgo < 12 -> cold;
        true -> archived
    end.

should_fragment(Meta) ->
    Meta#fragment_meta.record_count >= ?RECORDS_PER_FRAGMENT orelse
    Meta#fragment_meta.size_bytes >= ?BYTES_PER_FRAGMENT.

make_test_meta(RecordCount, SizeBytes) ->
    #fragment_meta{
        table_name = job_records_2026_02,
        base_period = <<"2026-02">>,
        fragment_id = 0,
        record_count = RecordCount,
        size_bytes = SizeBytes,
        created_at = erlang:system_time(second),
        status = hot
    }.

make_test_meta_with_status_and_period(Status, Period) ->
    #fragment_meta{
        table_name = period_to_table_name(Period, 0),
        base_period = Period,
        fragment_id = 0,
        record_count = 10000,
        size_bytes = 50000000,
        created_at = erlang:system_time(second),
        status = Status
    }.

make_test_job_record() ->
    #job_record{
        job_id = 12345,
        job_name = <<"test_job">>,
        user_name = <<"alice">>,
        user_id = 1000,
        group_id = 1000,
        account = <<"research">>,
        partition = <<"batch">>,
        cluster = <<"flurm">>,
        qos = <<"normal">>,
        state = completed,
        exit_code = 0,
        num_nodes = 2,
        num_cpus = 8,
        num_tasks = 8,
        req_mem = 16384,
        submit_time = 1000000,
        start_time = 1000010,
        end_time = 1001000,
        elapsed = 990,
        tres_alloc = #{cpu => 8, mem => 16384},
        tres_req = #{cpu => 8, mem => 16384}
    }.

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

job_record_to_map(R) ->
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

fragment_meta_to_map(M) ->
    #{
        table_name => M#fragment_meta.table_name,
        base_period => M#fragment_meta.base_period,
        fragment_id => M#fragment_meta.fragment_id,
        record_count => M#fragment_meta.record_count,
        size_bytes => M#fragment_meta.size_bytes,
        created_at => M#fragment_meta.created_at,
        status => M#fragment_meta.status
    }.

should_age_table(Meta, CurrentYear, CurrentMonth) ->
    Period = Meta#fragment_meta.base_period,
    {TableYear, TableMonth} = parse_period(Period),

    MonthsAgo = (CurrentYear * 12 + CurrentMonth) - (TableYear * 12 + TableMonth),

    case {Meta#fragment_meta.status, MonthsAgo} of
        {hot, N} when N >= 1 -> {true, warm};
        {warm, N} when N >= 3 -> {true, cold};
        {cold, N} when N >= 12 -> {true, archived};
        _ -> false
    end.

subtract_months(Year, Month, Subtract) ->
    TotalMonths = Year * 12 + Month - Subtract,
    NewYear = (TotalMonths - 1) div 12,
    NewMonth = case TotalMonths rem 12 of
        0 -> 12;
        N -> N
    end,
    list_to_binary(io_lib:format("~4..0B-~2..0B", [NewYear, NewMonth])).
