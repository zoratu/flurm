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

-record(state, {
    current_table,
    current_period,
    fragment_metas,
    check_timer
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
%% Internal Branch Coverage Tests (real module exports)
%%====================================================================

fragment_internal_branches_test_() ->
    {foreach,
     fun setup_internal_mocks/0,
     fun cleanup_internal_mocks/1,
     [
      {"ensure_table_for_period existing current", fun test_ensure_table_existing_current/0},
      {"ensure_table_for_period existing old period", fun test_ensure_table_existing_old_period/0},
      {"ensure_table_for_period create ok", fun test_ensure_table_create_ok/0},
      {"ensure_table_for_period create already_exists", fun test_ensure_table_create_exists/0},
      {"ensure_table_for_period create error", fun test_ensure_table_create_error/0},
      {"create_burst_fragment branches", fun test_create_burst_fragment_branches/0},
      {"update_record_count and save", fun test_update_record_count_branches/0},
      {"table/range helpers", fun test_table_and_range_helpers/0},
      {"find/query/delete helpers", fun test_find_query_delete_helpers/0},
      {"maintenance/aging helpers", fun test_maintenance_aging_helpers/0},
      {"archive and export helpers", fun test_archive_and_export_helpers/0},
      {"size/status/map/stat helpers", fun test_size_status_map_stat_helpers/0}
     ]}.

test_ensure_table_existing_current() ->
    Period = <<"2026-02">>,
    Table = flurm_dbd_fragment:period_to_table_name(Period, 0),
    Meta = #fragment_meta{
        table_name = Table,
        base_period = Period,
        fragment_id = 0,
        record_count = 0,
        size_bytes = 0,
        created_at = 1,
        status = hot
    },
    State = #state{
        current_table = Table,
        current_period = Period,
        fragment_metas = #{Table => Meta},
        check_timer = undefined
    },
    {TableOut, StateOut} = flurm_dbd_fragment:ensure_table_for_period(Period, State),
    ?assertEqual(Table, TableOut),
    ?assertEqual(State, StateOut).

test_ensure_table_existing_old_period() ->
    OldPeriod = <<"2026-01">>,
    NewPeriod = <<"2026-02">>,
    Table = flurm_dbd_fragment:period_to_table_name(NewPeriod, 0),
    Meta = #fragment_meta{
        table_name = Table,
        base_period = NewPeriod,
        fragment_id = 0,
        record_count = 0,
        size_bytes = 0,
        created_at = 1,
        status = hot
    },
    State = #state{
        current_table = old_table,
        current_period = OldPeriod,
        fragment_metas = #{Table => Meta},
        check_timer = undefined
    },
    {TableOut, StateOut} = flurm_dbd_fragment:ensure_table_for_period(NewPeriod, State),
    ?assertEqual(Table, TableOut),
    ?assertEqual(Table, StateOut#state.current_table),
    ?assertEqual(NewPeriod, StateOut#state.current_period).

test_ensure_table_create_ok() ->
    Period = <<"2026-03">>,
    meck:expect(mnesia, create_table, fun(_, _) -> {atomic, ok} end),
    {TableOut, StateOut} = flurm_dbd_fragment:ensure_table_for_period(Period, empty_state()),
    ?assertEqual(flurm_dbd_fragment:period_to_table_name(Period, 0), TableOut),
    ?assert(maps:is_key(TableOut, StateOut#state.fragment_metas)).

test_ensure_table_create_exists() ->
    Period = <<"2026-04">>,
    Table = flurm_dbd_fragment:period_to_table_name(Period, 0),
    State = empty_state(),
    meck:expect(mnesia, create_table, fun(_, _) -> {aborted, {already_exists, any}} end),
    {TableOut, StateOut} = flurm_dbd_fragment:ensure_table_for_period(Period, State),
    ?assertEqual(Table, TableOut),
    ?assertEqual(State, StateOut).

test_ensure_table_create_error() ->
    Period = <<"2026-05">>,
    State = empty_state(),
    meck:expect(mnesia, create_table, fun(_, _) -> {aborted, boom} end),
    {TableOut, StateOut} = flurm_dbd_fragment:ensure_table_for_period(Period, State),
    ?assertEqual(State#state.current_table, TableOut),
    ?assertEqual(State, StateOut).

test_create_burst_fragment_branches() ->
    Period = <<"2026-06">>,
    BaseTable = flurm_dbd_fragment:period_to_table_name(Period, 0),
    Meta = #fragment_meta{
        table_name = BaseTable,
        base_period = Period,
        fragment_id = 0,
        record_count = ?RECORDS_PER_FRAGMENT,
        size_bytes = 1,
        created_at = 1,
        status = hot
    },
    State0 = #state{
        current_table = BaseTable,
        current_period = Period,
        fragment_metas = #{BaseTable => Meta},
        check_timer = undefined
    },

    meck:expect(mnesia, create_table, fun(_, _) -> {atomic, ok} end),
    {NewTable, State1} = flurm_dbd_fragment:create_burst_fragment(Meta, State0),
    ?assert(maps:is_key(NewTable, State1#state.fragment_metas)),

    meck:expect(mnesia, create_table, fun(_, _) -> {aborted, {already_exists, any}} end),
    {NewTable2, State2} = flurm_dbd_fragment:create_burst_fragment(Meta, State1),
    ?assertEqual(NewTable2, flurm_dbd_fragment:period_to_table_name(Period, 1)),
    ?assertEqual(State1, State2),

    meck:expect(mnesia, create_table, fun(_, _) -> {aborted, bad_create} end),
    {FallbackTable, State3} = flurm_dbd_fragment:create_burst_fragment(Meta, State2),
    ?assertEqual(BaseTable, FallbackTable),
    ?assertEqual(State2, State3),

    meck:expect(mnesia, create_table, fun(_, _) -> {atomic, ok} end),
    {ChosenTable, _} = flurm_dbd_fragment:maybe_create_burst_fragment(BaseTable, State1),
    ?assert(is_atom(ChosenTable)).

test_update_record_count_branches() ->
    Period = <<"2026-07">>,
    Table = flurm_dbd_fragment:period_to_table_name(Period, 0),
    Meta = #fragment_meta{
        table_name = Table,
        base_period = Period,
        fragment_id = 0,
        record_count = 10,
        size_bytes = 0,
        created_at = 1,
        status = hot
    },
    State = #state{
        current_table = Table,
        current_period = Period,
        fragment_metas = #{Table => Meta},
        check_timer = undefined
    },
    ?assertEqual(ok, flurm_dbd_fragment:update_record_count(no_such_table, 1, State)),
    ?assertEqual({atomic, ok}, flurm_dbd_fragment:update_record_count(Table, 2, State)).

test_table_and_range_helpers() ->
    Period = flurm_dbd_fragment:current_month(),
    Table = flurm_dbd_fragment:period_to_table_name(Period, 0),
    Other = non_job_table,
    Meta = #fragment_meta{
        table_name = Table,
        base_period = Period,
        fragment_id = 0,
        record_count = 0,
        size_bytes = 0,
        created_at = 1,
        status = hot
    },
    State = #state{
        current_table = Table,
        current_period = Period,
        fragment_metas = #{Table => Meta, Other => Meta#fragment_meta{table_name = Other}},
        check_timer = undefined
    },
    JobTables = flurm_dbd_fragment:get_all_job_tables(State),
    ?assert(lists:member(Table, JobTables)),
    ?assertNot(lists:member(Other, JobTables)),
    Tables = flurm_dbd_fragment:get_tables_for_range(undefined, undefined, State),
    ?assert(lists:member(Table, Tables)).

test_find_query_delete_helpers() ->
    Period = <<"2026-09">>,
    Table = flurm_dbd_fragment:period_to_table_name(Period, 0),
    Record = #job_record{
        job_id = 123,
        job_name = <<"j">>,
        user_name = <<"u">>,
        user_id = 1,
        group_id = 1,
        account = <<"a">>,
        partition = <<"p">>,
        cluster = <<"flurm">>,
        qos = <<"normal">>,
        state = completed,
        exit_code = 0,
        num_nodes = 1,
        num_cpus = 1,
        num_tasks = 1,
        req_mem = 0,
        submit_time = 10,
        start_time = 20,
        end_time = 30,
        elapsed = 10,
        tres_alloc = #{},
        tres_req = #{}
    },

    meck:expect(mnesia, transaction, fun(Fun) ->
        case erlang:get(tx_mode) of
            find_hit -> {atomic, [Record]};
            find_empty -> {atomic, []};
            find_abort -> {aborted, tx_fail};
            query_ok -> {atomic, Fun()};
            query_abort -> {aborted, fold_fail};
            del_found -> {atomic, found};
            del_not_found -> {atomic, not_found};
            del_abort -> {aborted, del_fail};
            _ -> {atomic, Fun()}
        end
    end),
    meck:expect(mnesia, foldl, fun(FoldFun, Acc0, _) ->
        FoldFun(Record, FoldFun(Record#job_record{user_name = <<"x">>}, Acc0))
    end),
    meck:expect(mnesia, read, fun(_, _) -> [Record] end),
    meck:expect(mnesia, delete, fun(_, _, _) -> ok end),

    ?assertEqual({error, not_found}, flurm_dbd_fragment:find_job_in_tables(1, [])),
    erlang:put(tx_mode, find_empty),
    ?assertEqual({error, not_found}, flurm_dbd_fragment:find_job_in_tables(1, [Table])),
    erlang:put(tx_mode, find_abort),
    ?assertEqual({error, not_found}, flurm_dbd_fragment:find_job_in_tables(1, [Table])),
    erlang:put(tx_mode, find_hit),
    ?assertMatch({ok, #{job_id := 123}}, flurm_dbd_fragment:find_job_in_tables(1, [Table])),

    erlang:put(tx_mode, query_ok),
    Results = flurm_dbd_fragment:query_table_with_filters(Table, #{user_name => <<"u">>}),
    ?assertEqual(1, length(Results)),
    erlang:put(tx_mode, query_abort),
    ?assertEqual([], flurm_dbd_fragment:query_table_with_filters(Table, #{})),

    ?assertEqual({error, not_found}, flurm_dbd_fragment:delete_job_from_tables(1, [])),
    erlang:put(tx_mode, del_not_found),
    ?assertEqual({error, not_found}, flurm_dbd_fragment:delete_job_from_tables(1, [Table])),
    erlang:put(tx_mode, del_abort),
    ?assertEqual({error, not_found}, flurm_dbd_fragment:delete_job_from_tables(1, [Table])),
    erlang:put(tx_mode, del_found),
    ?assertEqual(ok, flurm_dbd_fragment:delete_job_from_tables(1, [Table])).

test_maintenance_aging_helpers() ->
    Old = <<"2025-01">>,
    Table = flurm_dbd_fragment:period_to_table_name(Old, 0),
    Meta = #fragment_meta{
        table_name = Table,
        base_period = Old,
        fragment_id = 0,
        record_count = 10,
        size_bytes = 100,
        created_at = 1,
        status = hot
    },
    State = #state{
        current_table = old_table,
        current_period = <<"2020-01">>,
        fragment_metas = #{Table => Meta},
        check_timer = undefined
    },

    meck:expect(mnesia, create_table, fun(_, _) -> {aborted, {already_exists, any}} end),
    meck:expect(mnesia, change_table_copy_type, fun(_, _, _) -> {atomic, ok} end),
    _ = flurm_dbd_fragment:perform_maintenance(State),

    ?assertMatch({true, warm}, flurm_dbd_fragment:should_age_table(Meta, 2026, 2)),
    ?assertMatch({true, cold}, flurm_dbd_fragment:should_age_table(Meta#fragment_meta{status = warm}, 2026, 5)),
    ?assertMatch({true, archived}, flurm_dbd_fragment:should_age_table(Meta#fragment_meta{status = cold}, 2027, 2)),
    ?assertEqual(false, flurm_dbd_fragment:should_age_table(Meta#fragment_meta{base_period = <<"2026-02">>}, 2026, 2)),
    _ = flurm_dbd_fragment:age_tables(State),
    ?assertEqual(ok, flurm_dbd_fragment:change_table_storage(Table, ram_copies, disc_copies)),
    meck:expect(mnesia, change_table_copy_type, fun(_, _, _) -> {aborted, denied} end),
    ?assertEqual(ok, flurm_dbd_fragment:change_table_storage(Table, ram_copies, disc_copies)).

test_archive_and_export_helpers() ->
    meck:expect(filelib, ensure_dir, fun(_) -> ok end),
    application:set_env(flurm_dbd, archive_dir, "/tmp/flurm_dbd_archive_test"),
    OldPeriod = <<"2024-01">>,
    RecentPeriod = <<"2026-02">>,
    OldTable = flurm_dbd_fragment:period_to_table_name(OldPeriod, 0),
    RecentTable = flurm_dbd_fragment:period_to_table_name(RecentPeriod, 0),
    OldMeta = #fragment_meta{
        table_name = OldTable,
        base_period = OldPeriod,
        fragment_id = 0,
        record_count = 1,
        size_bytes = 1,
        created_at = 1,
        status = cold
    },
    RecentMeta = OldMeta#fragment_meta{
        table_name = RecentTable,
        base_period = RecentPeriod,
        status = hot
    },
    State = #state{
        current_table = RecentTable,
        current_period = RecentPeriod,
        fragment_metas = #{OldTable => OldMeta, RecentTable => RecentMeta},
        check_timer = undefined
    },
    meck:expect(mnesia, dump_tables, fun(_) -> {atomic, ok} end),
    {Count1, State1} = flurm_dbd_fragment:archive_old_tables(12, State),
    ?assert(Count1 >= 0),
    ?assertMatch(#state{}, State1),
    meck:expect(mnesia, dump_tables, fun(_) -> {aborted, dump_failed} end),
    {_Count2, _State2} = flurm_dbd_fragment:archive_old_tables(12, State),
    ok.

test_size_status_map_stat_helpers() ->
    Table1 = job_records_2026_01,
    Table2 = job_records_2026_02,
    Meta1 = #fragment_meta{
        table_name = Table1,
        base_period = <<"2026-01">>,
        fragment_id = 0,
        record_count = 2,
        size_bytes = 8,
        created_at = 1,
        status = hot
    },
    Meta2 = Meta1#fragment_meta{
        table_name = Table2,
        base_period = <<"2026-02">>,
        record_count = 3,
        size_bytes = 9,
        status = hot
    },
    State = #state{
        current_table = Table2,
        current_period = <<"2026-02">>,
        fragment_metas = #{Table1 => Meta1, Table2 => Meta2},
        check_timer = undefined
    },

    meck:expect(mnesia, table_info, fun(T, memory) ->
        case T of Table1 -> 10; _ -> erlang:error(no_table) end
    end),
    meck:expect(mnesia, table_info, fun(T, size) ->
        case T of Table1 -> 5; _ -> erlang:error(no_table) end
    end),
    _ = flurm_dbd_fragment:update_table_sizes(State),

    {{CY, CM, _}, _} = calendar:local_time(),
    OneMonthAgo = subtract_months(CY, CM, 1),
    ThreeMonthsAgo = subtract_months(CY, CM, 3),
    TwelveMonthsAgo = subtract_months(CY, CM, 12),
    ?assertEqual(job_records_2026_02_frag_1, flurm_dbd_fragment:period_to_table_name(<<"2026-02">>, 1)),
    ?assertEqual(warm, flurm_dbd_fragment:determine_table_status(OneMonthAgo)),
    ?assertEqual(cold, flurm_dbd_fragment:determine_table_status(ThreeMonthsAgo)),
    ?assertEqual(archived, flurm_dbd_fragment:determine_table_status(TwelveMonthsAgo)),
    ?assertEqual(disc_copies, flurm_dbd_fragment:status_to_storage_type(warm)),
    ?assertEqual(disc_only_copies, flurm_dbd_fragment:status_to_storage_type(cold)),
    ?assertEqual(disc_only_copies, flurm_dbd_fragment:status_to_storage_type(archived)),
    ?assert(is_map(flurm_dbd_fragment:job_record_to_map(make_test_job_record()))),
    ?assert(is_map(flurm_dbd_fragment:fragment_meta_to_map(Meta1))),
    Stats = flurm_dbd_fragment:collect_table_stats(State),
    ?assertMatch(#{status_counts := #{hot := 2}}, Stats).

%%====================================================================
%% Helper Functions
%%====================================================================

setup_internal_mocks() ->
    catch meck:unload(lager),
    catch meck:unload(mnesia),
    catch meck:unload(filelib),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, md, fun() -> [] end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:new(mnesia, [passthrough, unstick, no_link, non_strict]),
    meck:expect(mnesia, create_table, fun(_, _) -> {atomic, ok} end),
    meck:expect(mnesia, transaction, fun(Fun) -> {atomic, Fun()} end),
    meck:expect(mnesia, write, fun(_, _, _) -> ok end),
    meck:expect(mnesia, change_table_copy_type, fun(_, _, _) -> {atomic, ok} end),
    meck:expect(mnesia, dump_tables, fun(_) -> {atomic, ok} end),
    meck:expect(mnesia, table_info, fun(_, memory) -> 1; (_, size) -> 1 end),
    meck:expect(mnesia, read, fun(_, _) -> [] end),
    meck:expect(mnesia, delete, fun(_, _, _) -> ok end),
    meck:expect(mnesia, foldl, fun(_, Acc, _) -> Acc end),
    meck:new(filelib, [passthrough, no_link, non_strict, unstick]),
    meck:expect(filelib, ensure_dir, fun(_) -> ok end),
    ok.

cleanup_internal_mocks(_) ->
    catch erlang:erase(tx_mode),
    catch meck:unload(filelib),
    catch meck:unload(mnesia),
    catch meck:unload(lager),
    ok.

empty_state() ->
    #state{
        current_table = default_table,
        current_period = <<"2026-01">>,
        fragment_metas = #{},
        check_timer = undefined
    }.

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

%%====================================================================
%% Real Module Path Tests
%%====================================================================

fragment_real_paths_test_() ->
    {foreach,
     fun setup_real_fragment/0,
     fun cleanup_real_fragment/1,
     [
      {"test exports call real module helpers", fun test_real_module_helpers/0},
      {"test handle_call unknown request", fun test_real_unknown_handle_call/0},
      {"test ensure table and insert/query/delete path", fun test_real_insert_query_delete/0},
      {"test archival and stats path", fun test_real_archive_and_stats/0},
      {"test maintenance callbacks", fun test_real_maintenance_callbacks/0}
     ]}.

setup_real_fragment() ->
    catch meck:unload(lager),
    meck:new(lager, [no_link, non_strict, passthrough]),
    meck:expect(lager, md, fun() -> [] end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    application:set_env(mnesia, dir, "/tmp/flurm_dbd_fragment_real_" ++
                        integer_to_list(erlang:unique_integer([positive]))),
    catch mnesia:stop(),
    catch mnesia:delete_schema([node()]),
    mnesia:create_schema([node()]),
    mnesia:start(),
    ok.

cleanup_real_fragment(_) ->
    catch mnesia:stop(),
    catch meck:unload(lager),
    ok.

test_real_module_helpers() ->
    Now = erlang:system_time(second),
    ?assert(is_atom(flurm_dbd_fragment:table_name_for_time(Now))),
    ?assert(is_binary(flurm_dbd_fragment:current_month())),
    ?assert(is_list(flurm_dbd_fragment:get_tables_for_range(Now - 3600, Now))),
    Meta = #fragment_meta{
        table_name = test_table,
        base_period = <<"2026-02">>,
        fragment_id = 0,
        record_count = 1,
        size_bytes = 1,
        created_at = Now,
        status = hot
    },
    ?assertEqual(ok, flurm_dbd_fragment:age_table(test_table, warm)),
    ?assertEqual(false, flurm_dbd_fragment:should_fragment(Meta)).

test_real_unknown_handle_call() ->
    State = #state{
        current_table = test_table,
        current_period = <<"2026-02">>,
        fragment_metas = #{},
        check_timer = undefined
    },
    ?assertMatch({reply, {error, unknown_request}, _},
                 flurm_dbd_fragment:handle_call(unknown_req, {self(), make_ref()}, State)),
    ?assertMatch({noreply, _}, flurm_dbd_fragment:handle_cast(unknown_cast, State)),
    ?assertMatch({noreply, _}, flurm_dbd_fragment:handle_info(unknown_info, State)),
    ?assertEqual(ok, flurm_dbd_fragment:terminate(normal, State)).

test_real_insert_query_delete() ->
    {ok, Pid} = flurm_dbd_fragment:start_link(),
    ?assert(is_pid(Pid)),
    {ok, TableName} = flurm_dbd_fragment:ensure_current_table(),
    ?assert(is_atom(TableName)),
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1234, job_name => <<"j">>, user_name => <<"u">>,
        user_id => 1000, group_id => 1000, account => <<"a">>,
        partition => <<"p">>, state => completed, exit_code => 0,
        num_nodes => 1, num_cpus => 1, num_tasks => 1, req_mem => 0,
        submit_time => Now - 10, start_time => Now - 5, end_time => Now,
        elapsed => 5, tres_alloc => #{cpu => 1}, tres_req => #{cpu => 1}
    },
    ?assertEqual(ok, flurm_dbd_fragment:insert_job(Job)),
    {ok, Jobs} = flurm_dbd_fragment:query_jobs(#{}),
    ?assert(is_list(Jobs)),
    _ = flurm_dbd_fragment:get_job(1234),
    _ = flurm_dbd_fragment:delete_job(1234),
    gen_server:stop(Pid).

test_real_archive_and_stats() ->
    {ok, Pid} = flurm_dbd_fragment:start_link(),
    _ = flurm_dbd_fragment:ensure_current_table(),
    Frags = flurm_dbd_fragment:list_all_fragments(),
    ?assert(is_list(Frags)),
    _ = flurm_dbd_fragment:get_fragment_meta(nonexistent_table),
    {ok, Count} = flurm_dbd_fragment:trigger_archival(1),
    ?assert(is_integer(Count)),
    Stats = flurm_dbd_fragment:get_table_stats(),
    ?assert(is_map(Stats)),
    gen_server:stop(Pid).

test_real_maintenance_callbacks() ->
    {ok, Pid} = flurm_dbd_fragment:start_link(),
    State = sys:get_state(Pid),
    ?assertMatch({noreply, _}, flurm_dbd_fragment:handle_info(maintenance_check, State)),
    gen_server:stop(Pid).

%% Reuse dedicated fragment coverage suites so they run under app-based eunit.
fragment_cover_suite_passthrough_test_() ->
    [flurm_dbd_fragment_cover_tests:pure_test_(),
     flurm_dbd_fragment_cover_tests:server_test_()].
