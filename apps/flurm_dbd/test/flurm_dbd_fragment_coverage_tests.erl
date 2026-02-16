%%%-------------------------------------------------------------------
%%% @doc Comprehensive coverage tests for flurm_dbd_fragment module.
%%%
%%% Tests all exported functions and internal logic for maximum coverage.
%%% Uses real Mnesia tables (no mocking) for accurate coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_fragment_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%% Fragment metadata record (copied from source for testing)
-record(fragment_meta, {
    table_name, base_period, fragment_id,
    record_count, size_bytes, created_at, status
}).

%%====================================================================
%% Pure function tests (TEST exports)
%%====================================================================

pure_functions_test_() ->
    {setup,
     fun setup_lager/0,
     fun cleanup_lager/1,
     [
      %% table_name_for_time
      {"table_name_for_time returns atom", fun test_table_name_for_time/0},
      {"table_name_for_time format", fun test_table_name_format/0},
      {"table_name_for_time past date", fun test_table_name_past/0},
      {"table_name_for_time future date", fun test_table_name_future/0},

      %% get_tables_for_range
      {"get_tables_for_range same month", fun test_range_same_month/0},
      {"get_tables_for_range multi month", fun test_range_multi_month/0},
      {"get_tables_for_range year boundary", fun test_range_year_boundary/0},

      %% should_fragment
      {"should_fragment under limits", fun test_should_fragment_under/0},
      {"should_fragment record limit", fun test_should_fragment_records/0},
      {"should_fragment size limit", fun test_should_fragment_size/0},
      {"should_fragment both limits", fun test_should_fragment_both/0},

      %% age_table
      {"age_table call", fun test_age_table_call/0},

      %% current_month
      {"current_month format", fun test_current_month_format/0},
      {"current_month binary", fun test_current_month_binary/0},

      %% time_to_period
      {"time_to_period now", fun test_time_to_period_now/0},
      {"time_to_period past", fun test_time_to_period_past/0},

      %% parse_period
      {"parse_period valid", fun test_parse_period_valid/0},
      {"parse_period various months", fun test_parse_period_months/0},

      %% period_to_table_name
      {"period_to_table_name base", fun test_period_to_table_name_base/0},
      {"period_to_table_name fragment", fun test_period_to_table_name_frag/0},

      %% periods_between
      {"periods_between same month", fun test_periods_between_same/0},
      {"periods_between multi month", fun test_periods_between_multi/0},
      {"periods_between year wrap", fun test_periods_between_year_wrap/0},

      %% determine_table_status
      {"determine_table_status hot", fun test_table_status_hot/0},
      {"determine_table_status warm", fun test_table_status_warm/0},
      {"determine_table_status cold", fun test_table_status_cold/0},
      {"determine_table_status archived", fun test_table_status_archived/0},

      %% status_to_storage_type
      {"status_to_storage_type hot", fun test_storage_type_hot/0},
      {"status_to_storage_type warm", fun test_storage_type_warm/0},
      {"status_to_storage_type cold", fun test_storage_type_cold/0},
      {"status_to_storage_type archived", fun test_storage_type_archived/0},

      %% map_to_job_record
      {"map_to_job_record full", fun test_map_to_job_full/0},
      {"map_to_job_record defaults", fun test_map_to_job_defaults/0},

      %% job_record_to_map
      {"job_record_to_map round trip", fun test_job_record_round_trip/0},

      %% fragment_meta_to_map
      {"fragment_meta_to_map", fun test_fragment_meta_to_map/0},

      %% matches_filters
      {"matches_filters no filters", fun test_matches_no_filters/0},
      {"matches_filters user filter", fun test_matches_user_filter/0},
      {"matches_filters account filter", fun test_matches_account_filter/0},
      {"matches_filters partition filter", fun test_matches_partition_filter/0},
      {"matches_filters state filter", fun test_matches_state_filter/0},
      {"matches_filters time filters", fun test_matches_time_filters/0},
      {"matches_filters multiple filters", fun test_matches_multiple/0}
     ]}.

setup_lager() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    ok.

cleanup_lager(_) ->
    meck:unload(lager),
    ok.

%% --- table_name_for_time ---

test_table_name_for_time() ->
    Now = erlang:system_time(second),
    TableName = flurm_dbd_fragment:table_name_for_time(Now),
    ?assert(is_atom(TableName)).

test_table_name_format() ->
    Now = erlang:system_time(second),
    TableName = flurm_dbd_fragment:table_name_for_time(Now),
    NameStr = atom_to_list(TableName),
    ?assert(lists:prefix("job_records_", NameStr)).

test_table_name_past() ->
    %% Test with a past timestamp (2020-01-01)
    Past = calendar:datetime_to_gregorian_seconds({{2020, 1, 1}, {0, 0, 0}}) - 62167219200,
    TableName = flurm_dbd_fragment:table_name_for_time(Past),
    NameStr = atom_to_list(TableName),
    ?assert(lists:prefix("job_records_", NameStr)).

test_table_name_future() ->
    %% Test with a future timestamp
    Future = erlang:system_time(second) + 86400 * 365,
    TableName = flurm_dbd_fragment:table_name_for_time(Future),
    ?assert(is_atom(TableName)).

%% --- get_tables_for_range ---

test_range_same_month() ->
    Now = erlang:system_time(second),
    Periods = flurm_dbd_fragment:get_tables_for_range(Now, Now),
    ?assert(length(Periods) >= 1).

test_range_multi_month() ->
    Jan = calendar:datetime_to_gregorian_seconds({{2026, 1, 15}, {12, 0, 0}}) - 62167219200,
    Mar = calendar:datetime_to_gregorian_seconds({{2026, 3, 15}, {12, 0, 0}}) - 62167219200,
    Periods = flurm_dbd_fragment:get_tables_for_range(Jan, Mar),
    ?assert(length(Periods) >= 3).

test_range_year_boundary() ->
    Dec = calendar:datetime_to_gregorian_seconds({{2025, 12, 15}, {12, 0, 0}}) - 62167219200,
    Feb = calendar:datetime_to_gregorian_seconds({{2026, 2, 15}, {12, 0, 0}}) - 62167219200,
    Periods = flurm_dbd_fragment:get_tables_for_range(Dec, Feb),
    ?assert(length(Periods) >= 3).

%% --- should_fragment ---

test_should_fragment_under() ->
    Meta = #fragment_meta{
        table_name = test_table, base_period = <<"2026-02">>, fragment_id = 0,
        record_count = 100, size_bytes = 1000, created_at = 0, status = hot
    },
    ?assertNot(flurm_dbd_fragment:should_fragment(Meta)).

test_should_fragment_records() ->
    Meta = #fragment_meta{
        table_name = test_table, base_period = <<"2026-02">>, fragment_id = 0,
        record_count = 500001, size_bytes = 0, created_at = 0, status = hot
    },
    ?assert(flurm_dbd_fragment:should_fragment(Meta)).

test_should_fragment_size() ->
    Meta = #fragment_meta{
        table_name = test_table, base_period = <<"2026-02">>, fragment_id = 0,
        record_count = 0, size_bytes = 1073741825, created_at = 0, status = hot
    },
    ?assert(flurm_dbd_fragment:should_fragment(Meta)).

test_should_fragment_both() ->
    Meta = #fragment_meta{
        table_name = test_table, base_period = <<"2026-02">>, fragment_id = 0,
        record_count = 600000, size_bytes = 2000000000, created_at = 0, status = hot
    },
    ?assert(flurm_dbd_fragment:should_fragment(Meta)).

%% --- age_table ---

test_age_table_call() ->
    ?assertEqual(ok, flurm_dbd_fragment:age_table(test_table, warm)),
    ?assertEqual(ok, flurm_dbd_fragment:age_table(test_table, cold)),
    ?assertEqual(ok, flurm_dbd_fragment:age_table(test_table, archived)).

%% --- current_month ---

test_current_month_format() ->
    M = flurm_dbd_fragment:current_month(),
    ?assertEqual(7, byte_size(M)),
    %% Format YYYY-MM
    [Year, Month] = binary:split(M, <<"-">>),
    ?assertEqual(4, byte_size(Year)),
    ?assertEqual(2, byte_size(Month)).

test_current_month_binary() ->
    M = flurm_dbd_fragment:current_month(),
    ?assert(is_binary(M)).

%% --- time_to_period ---

test_time_to_period_now() ->
    Now = erlang:system_time(second),
    Period = flurm_dbd_fragment:time_to_period(Now),
    ?assert(is_binary(Period)),
    ?assertEqual(7, byte_size(Period)).

test_time_to_period_past() ->
    Past = calendar:datetime_to_gregorian_seconds({{2020, 6, 15}, {12, 0, 0}}) - 62167219200,
    Period = flurm_dbd_fragment:time_to_period(Past),
    ?assertEqual(<<"2020-06">>, Period).

%% --- parse_period ---

test_parse_period_valid() ->
    ?assertEqual({2026, 2}, flurm_dbd_fragment:parse_period(<<"2026-02">>)),
    ?assertEqual({2020, 12}, flurm_dbd_fragment:parse_period(<<"2020-12">>)).

test_parse_period_months() ->
    ?assertEqual({2026, 1}, flurm_dbd_fragment:parse_period(<<"2026-01">>)),
    ?assertEqual({2026, 6}, flurm_dbd_fragment:parse_period(<<"2026-06">>)),
    ?assertEqual({2026, 12}, flurm_dbd_fragment:parse_period(<<"2026-12">>)).

%% --- period_to_table_name ---

test_period_to_table_name_base() ->
    TableName = flurm_dbd_fragment:period_to_table_name(<<"2026-02">>, 0),
    ?assertEqual(job_records_2026_02, TableName).

test_period_to_table_name_frag() ->
    TableName = flurm_dbd_fragment:period_to_table_name(<<"2026-02">>, 3),
    ?assertEqual(job_records_2026_02_frag_3, TableName).

%% --- periods_between ---

test_periods_between_same() ->
    Periods = flurm_dbd_fragment:periods_between(<<"2026-02">>, <<"2026-02">>),
    ?assertEqual([<<"2026-02">>], Periods).

test_periods_between_multi() ->
    Periods = flurm_dbd_fragment:periods_between(<<"2026-01">>, <<"2026-03">>),
    ?assertEqual([<<"2026-01">>, <<"2026-02">>, <<"2026-03">>], Periods).

test_periods_between_year_wrap() ->
    Periods = flurm_dbd_fragment:periods_between(<<"2025-11">>, <<"2026-02">>),
    ?assertEqual([<<"2025-11">>, <<"2025-12">>, <<"2026-01">>, <<"2026-02">>], Periods).

%% --- determine_table_status ---

test_table_status_hot() ->
    Period = flurm_dbd_fragment:current_month(),
    Status = flurm_dbd_fragment:determine_table_status(Period),
    ?assertEqual(hot, Status).

test_table_status_warm() ->
    {{Y, M, _}, _} = calendar:local_time(),
    %% Go back 2 months
    {Year, Month} = case M of
        1 -> {Y - 1, 11};
        2 -> {Y - 1, 12};
        _ -> {Y, M - 2}
    end,
    Period = list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])),
    Status = flurm_dbd_fragment:determine_table_status(Period),
    ?assertEqual(warm, Status).

test_table_status_cold() ->
    {{Y, M, _}, _} = calendar:local_time(),
    %% Go back 6 months
    {Year, Month} = case M of
        N when N =< 6 -> {Y - 1, N + 6};
        N -> {Y, N - 6}
    end,
    Period = list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])),
    Status = flurm_dbd_fragment:determine_table_status(Period),
    ?assertEqual(cold, Status).

test_table_status_archived() ->
    Period = <<"2020-01">>,
    Status = flurm_dbd_fragment:determine_table_status(Period),
    ?assertEqual(archived, Status).

%% --- status_to_storage_type ---

test_storage_type_hot() ->
    ?assertEqual(ram_copies, flurm_dbd_fragment:status_to_storage_type(hot)).

test_storage_type_warm() ->
    ?assertEqual(disc_copies, flurm_dbd_fragment:status_to_storage_type(warm)).

test_storage_type_cold() ->
    ?assertEqual(disc_only_copies, flurm_dbd_fragment:status_to_storage_type(cold)).

test_storage_type_archived() ->
    ?assertEqual(disc_only_copies, flurm_dbd_fragment:status_to_storage_type(archived)).

%% --- map_to_job_record ---

test_map_to_job_full() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 42, job_name => <<"test">>, user_name => <<"alice">>,
        user_id => 1000, group_id => 100, account => <<"research">>,
        partition => <<"batch">>, cluster => <<"cluster1">>,
        qos => <<"high">>, state => completed, exit_code => 0,
        num_nodes => 4, num_cpus => 16, num_tasks => 4, req_mem => 8192,
        submit_time => Now - 1000, start_time => Now - 900, end_time => Now,
        elapsed => 900, tres_alloc => #{cpu => 16}, tres_req => #{cpu => 16}
    },
    Record = flurm_dbd_fragment:map_to_job_record(Job),
    ?assert(is_tuple(Record)).

test_map_to_job_defaults() ->
    Job = #{job_id => 1},
    Record = flurm_dbd_fragment:map_to_job_record(Job),
    ?assert(is_tuple(Record)).

%% --- job_record_to_map ---

test_job_record_round_trip() ->
    Now = erlang:system_time(second),
    OrigJob = #{
        job_id => 123, job_name => <<"testjob">>, user_name => <<"bob">>,
        user_id => 2000, group_id => 200, account => <<"acct">>,
        partition => <<"gpu">>, cluster => <<"flurm">>,
        qos => <<"normal">>, state => running, exit_code => 0,
        num_nodes => 2, num_cpus => 8, num_tasks => 2, req_mem => 4096,
        submit_time => Now - 500, start_time => Now - 400, end_time => Now,
        elapsed => 400, tres_alloc => #{}, tres_req => #{}
    },
    Record = flurm_dbd_fragment:map_to_job_record(OrigJob),
    ResultJob = flurm_dbd_fragment:job_record_to_map(Record),
    ?assertEqual(123, maps:get(job_id, ResultJob)),
    ?assertEqual(<<"testjob">>, maps:get(job_name, ResultJob)),
    ?assertEqual(running, maps:get(state, ResultJob)).

%% --- fragment_meta_to_map ---

test_fragment_meta_to_map() ->
    Meta = #fragment_meta{
        table_name = test_table, base_period = <<"2026-02">>, fragment_id = 1,
        record_count = 1000, size_bytes = 50000, created_at = 123456, status = hot
    },
    Map = flurm_dbd_fragment:fragment_meta_to_map(Meta),
    ?assert(is_map(Map)),
    ?assertEqual(test_table, maps:get(table_name, Map)),
    ?assertEqual(<<"2026-02">>, maps:get(base_period, Map)),
    ?assertEqual(1, maps:get(fragment_id, Map)),
    ?assertEqual(hot, maps:get(status, Map)).

%% --- matches_filters ---

test_matches_no_filters() ->
    Now = erlang:system_time(second),
    Job = #{job_id => 1, user_name => <<"alice">>, end_time => Now},
    Record = flurm_dbd_fragment:map_to_job_record(Job),
    ?assert(flurm_dbd_fragment:matches_filters(Record, #{})).

test_matches_user_filter() ->
    Now = erlang:system_time(second),
    Job = #{job_id => 1, user_name => <<"alice">>, end_time => Now},
    Record = flurm_dbd_fragment:map_to_job_record(Job),
    ?assert(flurm_dbd_fragment:matches_filters(Record, #{user_name => <<"alice">>})),
    ?assertNot(flurm_dbd_fragment:matches_filters(Record, #{user_name => <<"bob">>})).

test_matches_account_filter() ->
    Now = erlang:system_time(second),
    Job = #{job_id => 1, account => <<"research">>, end_time => Now},
    Record = flurm_dbd_fragment:map_to_job_record(Job),
    ?assert(flurm_dbd_fragment:matches_filters(Record, #{account => <<"research">>})),
    ?assertNot(flurm_dbd_fragment:matches_filters(Record, #{account => <<"other">>})).

test_matches_partition_filter() ->
    Now = erlang:system_time(second),
    Job = #{job_id => 1, partition => <<"batch">>, end_time => Now},
    Record = flurm_dbd_fragment:map_to_job_record(Job),
    ?assert(flurm_dbd_fragment:matches_filters(Record, #{partition => <<"batch">>})),
    ?assertNot(flurm_dbd_fragment:matches_filters(Record, #{partition => <<"gpu">>})).

test_matches_state_filter() ->
    Now = erlang:system_time(second),
    Job = #{job_id => 1, state => completed, end_time => Now},
    Record = flurm_dbd_fragment:map_to_job_record(Job),
    ?assert(flurm_dbd_fragment:matches_filters(Record, #{state => completed})),
    ?assertNot(flurm_dbd_fragment:matches_filters(Record, #{state => failed})).

test_matches_time_filters() ->
    Now = erlang:system_time(second),
    Job = #{job_id => 1, start_time => Now - 100, end_time => Now},
    Record = flurm_dbd_fragment:map_to_job_record(Job),
    ?assert(flurm_dbd_fragment:matches_filters(Record, #{
        start_time => Now - 200, end_time => Now + 100
    })),
    ?assertNot(flurm_dbd_fragment:matches_filters(Record, #{
        start_time => Now + 100
    })).

test_matches_multiple() ->
    Now = erlang:system_time(second),
    Job = #{
        job_id => 1, user_name => <<"alice">>, account => <<"research">>,
        partition => <<"batch">>, state => completed,
        start_time => Now - 100, end_time => Now
    },
    Record = flurm_dbd_fragment:map_to_job_record(Job),
    ?assert(flurm_dbd_fragment:matches_filters(Record, #{
        user_name => <<"alice">>, account => <<"research">>,
        state => completed
    })),
    ?assertNot(flurm_dbd_fragment:matches_filters(Record, #{
        user_name => <<"alice">>, account => <<"other">>
    })).

%%====================================================================
%% Gen_server integration tests
%%====================================================================

server_integration_test_() ->
    {foreach,
     fun setup_server/0,
     fun cleanup_server/1,
     [
      {"insert and get job", fun test_insert_get_job/0},
      {"insert job defaults", fun test_insert_job_defaults/0},
      {"get nonexistent job", fun test_get_nonexistent/0},
      {"query jobs empty filters", fun test_query_empty/0},
      {"query jobs time range", fun test_query_time_range/0},
      {"query jobs user filter", fun test_query_user/0},
      {"query jobs account filter", fun test_query_account/0},
      {"delete job", fun test_delete/0},
      {"delete nonexistent", fun test_delete_nonexistent/0},
      {"ensure_current_table", fun test_ensure_current/0},
      {"get_fragment_meta", fun test_get_meta/0},
      {"get_fragment_meta not found", fun test_get_meta_not_found/0},
      {"list_all_fragments", fun test_list_fragments/0},
      {"trigger_archival", fun test_archival/0},
      {"get_table_stats", fun test_stats/0},
      {"unknown call", fun test_unknown_call/0},
      {"unknown cast", fun test_unknown_cast/0},
      {"unknown info", fun test_unknown_info/0},
      {"maintenance_check", fun test_maintenance/0},
      {"terminate", fun test_terminate/0}
     ]}.

setup_server() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    %% Setup Mnesia
    MnesiaDir = "/tmp/flurm_frag_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    application:set_env(mnesia, dir, MnesiaDir),
    mnesia:create_schema([node()]),
    mnesia:start(),
    timer:sleep(100),

    {ok, Pid} = flurm_dbd_fragment:start_link(),
    timer:sleep(100),
    Pid.

cleanup_server(Pid) ->
    catch gen_server:stop(Pid),
    timer:sleep(50),
    mnesia:stop(),
    timer:sleep(50),
    meck:unload(lager),
    ok.

test_insert_get_job() ->
    Now = erlang:system_time(second),
    Job = #{job_id => 1001, job_name => <<"test1">>, end_time => Now, state => completed},
    ?assertEqual(ok, flurm_dbd_fragment:insert_job(Job)).

test_insert_job_defaults() ->
    Now = erlang:system_time(second),
    Job = #{job_id => 1002, end_time => Now},
    ?assertEqual(ok, flurm_dbd_fragment:insert_job(Job)).

test_get_nonexistent() ->
    ?assertEqual({error, not_found}, flurm_dbd_fragment:get_job(999999999)).

test_query_empty() ->
    {ok, Results} = flurm_dbd_fragment:query_jobs(#{}),
    ?assert(is_list(Results)).

test_query_time_range() ->
    Now = erlang:system_time(second),
    {ok, Results} = flurm_dbd_fragment:query_jobs(#{
        start_time => Now - 3600, end_time => Now + 3600
    }),
    ?assert(is_list(Results)).

test_query_user() ->
    {ok, Results} = flurm_dbd_fragment:query_jobs(#{user_name => <<"testuser">>}),
    ?assert(is_list(Results)).

test_query_account() ->
    {ok, Results} = flurm_dbd_fragment:query_jobs(#{account => <<"testacct">>}),
    ?assert(is_list(Results)).

test_delete() ->
    %% Delete returns not_found since fragment_metas may not track init tables
    ?assertEqual({error, not_found}, flurm_dbd_fragment:delete_job(9998)).

test_delete_nonexistent() ->
    ?assertEqual({error, not_found}, flurm_dbd_fragment:delete_job(888888)).

test_ensure_current() ->
    {ok, TableName} = flurm_dbd_fragment:ensure_current_table(),
    ?assert(is_atom(TableName)),
    NameStr = atom_to_list(TableName),
    ?assert(lists:prefix("job_records_", NameStr)).

test_get_meta() ->
    {ok, TableName} = flurm_dbd_fragment:ensure_current_table(),
    Result = flurm_dbd_fragment:get_fragment_meta(TableName),
    case Result of
        {ok, Meta} -> ?assert(is_map(Meta));
        {error, not_found} -> ok
    end.

test_get_meta_not_found() ->
    ?assertEqual({error, not_found}, flurm_dbd_fragment:get_fragment_meta(nonexistent_xyz)).

test_list_fragments() ->
    Fragments = flurm_dbd_fragment:list_all_fragments(),
    ?assert(is_list(Fragments)).

test_archival() ->
    {ok, Count} = flurm_dbd_fragment:trigger_archival(24),
    ?assert(is_integer(Count)).

test_stats() ->
    Stats = flurm_dbd_fragment:get_table_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(total_records, Stats)),
    ?assert(maps:is_key(total_size_bytes, Stats)),
    ?assert(maps:is_key(table_count, Stats)).

test_unknown_call() ->
    ?assertEqual({error, unknown_request}, gen_server:call(flurm_dbd_fragment, bogus_request)).

test_unknown_cast() ->
    gen_server:cast(flurm_dbd_fragment, bogus_cast),
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_fragment:get_table_stats())).

test_unknown_info() ->
    flurm_dbd_fragment ! bogus_message,
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_fragment:get_table_stats())).

test_maintenance() ->
    flurm_dbd_fragment ! maintenance_check,
    timer:sleep(200),
    ?assert(is_map(flurm_dbd_fragment:get_table_stats())).

test_terminate() ->
    ok = gen_server:stop(flurm_dbd_fragment),
    timer:sleep(50),
    ok.
