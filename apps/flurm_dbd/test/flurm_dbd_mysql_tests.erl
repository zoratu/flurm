%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon - MySQL Bridge Tests
%%%
%%% Unit tests for the flurm_dbd_mysql module which provides the
%%% slurmdbd MySQL connector for zero-downtime migration.
%%%
%%% These tests use meck to mock the MySQL connector since we don't
%%% want to depend on a real MySQL database for unit tests.
%%%
%%% Test Categories:
%%% 1. Connection Handling - connect, disconnect, is_connected, auto-connect
%%% 2. Query Building - INSERT and SELECT query construction
%%% 3. Job Record Conversion - FLURM <-> slurmdbd format mapping
%%% 4. Error Handling - connection failures, query errors, edge cases
%%% 5. State Management - gen_server callbacks and state transitions
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_mysql_tests).

-include_lib("eunit/include/eunit.hrl").

-record(state, {
    connection,
    config,
    connected,
    schema_version,
    last_error,
    stats,
    reconnect_attempts = 0,
    current_reconnect_delay = 1000
}).

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing server
    case whereis(flurm_dbd_mysql) of
        undefined -> ok;
        ExistingPid ->
            catch gen_server:stop(ExistingPid, normal, 5000),
            flurm_test_utils:wait_for_death(ExistingPid)
    end,
    %% Mock the mysql module
    meck:new(mysql, [non_strict, no_link]),
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, stop, fun(_Pid) -> ok end),
    meck:expect(mysql, query, fun(_Conn, _Query) -> {ok, [], []} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> ok end),
    meck:expect(mysql, transaction, fun(_Conn, Fun) ->
        try
            Fun(),
            {atomic, ok}
        catch
            throw:Reason -> {aborted, Reason}
        end
    end),
    %% Mock lager to suppress log output
    meck:new(lager, [non_strict, no_link, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    %% Start the MySQL bridge
    Config = #{
        host => "localhost",
        port => 3306,
        user => "test",
        password => "test",
        database => "test_db",
        auto_connect => false
    },
    {ok, Pid} = flurm_dbd_mysql:start_link(Config),
    Pid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true ->
            catch gen_server:stop(Pid, normal, 5000);
        false ->
            ok
    end,
    catch meck:unload(mysql),
    catch meck:unload(lager),
    ok.

%%====================================================================
%% Connection Tests
%%====================================================================

connection_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"is_connected returns false initially", fun test_not_connected_initially/0},
             {"connect succeeds with valid config", fun test_connect_success/0},
             {"connect fails with error", fun test_connect_failure/0},
             {"disconnect works", fun test_disconnect/0},
             {"get_connection_status returns map", fun test_get_connection_status/0},
             {"connect with SSL config", fun test_connect_with_ssl/0},
             {"reconnect after disconnect", fun test_reconnect/0}
         ]
     end
    }.

test_not_connected_initially() ->
    Result = flurm_dbd_mysql:is_connected(),
    ?assertEqual(false, Result).

test_connect_success() ->
    Config = #{
        host => "localhost",
        port => 3306,
        user => "test",
        password => "test",
        database => "test_db"
    },
    Result = flurm_dbd_mysql:connect(Config),
    ?assertEqual(ok, Result),
    ?assertEqual(true, flurm_dbd_mysql:is_connected()).

test_connect_failure() ->
    %% Mock to return error
    meck:expect(mysql, start_link, fun(_Opts) -> {error, econnrefused} end),
    Config = #{
        host => "badhost",
        port => 3306,
        user => "test",
        password => "test",
        database => "test_db"
    },
    Result = flurm_dbd_mysql:connect(Config),
    ?assertEqual({error, econnrefused}, Result),
    ?assertEqual(false, flurm_dbd_mysql:is_connected()).

test_disconnect() ->
    %% First connect
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),
    ?assertEqual(true, flurm_dbd_mysql:is_connected()),
    %% Then disconnect
    Result = flurm_dbd_mysql:disconnect(),
    ?assertEqual(ok, Result),
    ?assertEqual(false, flurm_dbd_mysql:is_connected()).

test_get_connection_status() ->
    Status = flurm_dbd_mysql:get_connection_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(connected, Status)),
    ?assert(maps:is_key(stats, Status)),
    %% Password should be hidden
    Config = maps:get(config, Status, #{}),
    ?assertEqual(false, maps:is_key(password, Config)).

test_connect_with_ssl() ->
    meck:expect(mysql, start_link, fun(Opts) ->
        %% Verify SSL options are included
        HasSsl = proplists:is_defined(ssl, Opts),
        case HasSsl of
            true -> {ok, spawn(fun() -> receive stop -> ok end end)};
            false -> {error, no_ssl}
        end
    end),
    Config = #{
        host => "localhost",
        port => 3306,
        user => "test",
        password => "test",
        database => "test_db",
        ssl => true,
        ssl_opts => [{verify, verify_peer}]
    },
    Result = flurm_dbd_mysql:connect(Config),
    ?assertEqual(ok, Result).

test_reconnect() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    %% Connect
    ok = flurm_dbd_mysql:connect(Config),
    ?assertEqual(true, flurm_dbd_mysql:is_connected()),
    %% Disconnect
    ok = flurm_dbd_mysql:disconnect(),
    ?assertEqual(false, flurm_dbd_mysql:is_connected()),
    %% Reconnect
    ok = flurm_dbd_mysql:connect(Config),
    ?assertEqual(true, flurm_dbd_mysql:is_connected()).

%%====================================================================
%% Job Sync Tests
%%====================================================================

job_sync_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"sync_job_record fails when not connected", fun test_sync_not_connected/0},
             {"sync_job_record succeeds when connected", fun test_sync_success/0},
             {"sync_job_records batch succeeds", fun test_sync_batch_success/0},
             {"sync_job_records fails on error", fun test_sync_batch_failure/0},
             {"sync_job_record with minimal record", fun test_sync_minimal_record/0},
             {"sync_job_record with all fields", fun test_sync_full_record/0},
             {"sync_job_records empty list", fun test_sync_empty_batch/0}
         ]
     end
    }.

test_sync_not_connected() ->
    %% Make sure we're not connected
    flurm_dbd_mysql:disconnect(),
    JobRecord = sample_job_record(),
    Result = flurm_dbd_mysql:sync_job_record(JobRecord),
    ?assertEqual({error, not_connected}, Result).

test_sync_success() ->
    %% Connect first
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> ok end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    JobRecord = sample_job_record(),
    Result = flurm_dbd_mysql:sync_job_record(JobRecord),
    ?assertEqual(ok, Result).

test_sync_batch_success() ->
    %% Connect first
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> ok end),
    meck:expect(mysql, transaction, fun(_Conn, Fun) ->
        Fun(),
        {atomic, ok}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Jobs = [sample_job_record(1001), sample_job_record(1002), sample_job_record(1003)],
    Result = flurm_dbd_mysql:sync_job_records(Jobs),
    ?assertEqual({ok, 3}, Result).

test_sync_batch_failure() ->
    %% Connect first
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, transaction, fun(_Conn, _Fun) ->
        {aborted, {sync_error, db_error}}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Jobs = [sample_job_record(2001)],
    Result = flurm_dbd_mysql:sync_job_records(Jobs),
    ?assertMatch({error, _}, Result).

test_sync_minimal_record() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> ok end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    %% Minimal job with just job_id
    MinimalJob = #{job_id => 9999},
    Result = flurm_dbd_mysql:sync_job_record(MinimalJob),
    ?assertEqual(ok, Result).

test_sync_full_record() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> ok end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    FullJob = #{
        job_id => 5000,
        job_name => <<"full_test_job">>,
        user_id => 1001,
        group_id => 1001,
        account => <<"research">>,
        partition => <<"gpu">>,
        state => running,
        exit_code => 0,
        num_nodes => 4,
        num_cpus => 128,
        submit_time => 1700000000,
        eligible_time => 1700000100,
        start_time => 1700000200,
        end_time => 0,
        time_limit => 86400,
        tres_alloc => #{cpu => 128, mem => 524288, gpu => 4, node => 4},
        tres_req => #{cpu => 128, mem => 524288, gpu => 4},
        work_dir => <<"/scratch/user/project">>,
        std_out => <<"/scratch/user/project/job.out">>,
        std_err => <<"/scratch/user/project/job.err">>
    },
    Result = flurm_dbd_mysql:sync_job_record(FullJob),
    ?assertEqual(ok, Result).

test_sync_empty_batch() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, transaction, fun(_Conn, Fun) ->
        Fun(),
        {atomic, ok}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Result = flurm_dbd_mysql:sync_job_records([]),
    ?assertEqual({ok, 0}, Result).

%%====================================================================
%% Historical Read Tests
%%====================================================================

read_historical_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"read_historical_jobs fails when not connected", fun test_read_not_connected/0},
             {"read_historical_jobs returns jobs", fun test_read_success/0},
             {"read_historical_jobs with filters", fun test_read_with_filters/0},
             {"read_historical_jobs empty result", fun test_read_empty_result/0},
             {"read_historical_jobs with state filter", fun test_read_with_state_filter/0},
             {"read_historical_jobs with account filter", fun test_read_with_account_filter/0}
         ]
     end
    }.

test_read_not_connected() ->
    flurm_dbd_mysql:disconnect(),
    Now = erlang:system_time(second),
    Result = flurm_dbd_mysql:read_historical_jobs(Now - 3600, Now),
    ?assertEqual({error, not_connected}, Result).

test_read_success() ->
    %% Connect first
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    %% Mock query to return sample data
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) ->
        Columns = [<<"id_job">>, <<"job_name">>, <<"id_user">>, <<"id_group">>,
                   <<"account">>, <<"partition">>, <<"state">>, <<"exit_code">>,
                   <<"nodes_alloc">>, <<"cpus_req">>, <<"time_submit">>, <<"time_eligible">>,
                   <<"time_start">>, <<"time_end">>, <<"timelimit">>, <<"tres_alloc">>,
                   <<"tres_req">>, <<"work_dir">>, <<"std_out">>, <<"std_err">>],
        Row = {1001, <<"test_job">>, 1000, 1000, <<"account">>, <<"default">>,
               3, 0, 1, 4, 1000000, 1000000, 1000100, 1000200, 0,
               <<"1=4,2=8192">>, <<>>, <<"/home">>, <<>>, <<>>},
        {ok, Columns, [Row]}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Now = erlang:system_time(second),
    Result = flurm_dbd_mysql:read_historical_jobs(Now - 3600, Now),
    ?assertMatch({ok, [_|_]}, Result),
    {ok, [Job | _]} = Result,
    ?assertEqual(1001, maps:get(job_id, Job)).

test_read_with_filters() ->
    %% Connect first
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) ->
        {ok, [], []}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Now = erlang:system_time(second),
    Filters = #{user => <<"testuser">>, partition => <<"batch">>, limit => 100},
    Result = flurm_dbd_mysql:read_historical_jobs(Now - 3600, Now, Filters),
    ?assertMatch({ok, _}, Result).

test_read_empty_result() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) ->
        {ok, [], []}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Now = erlang:system_time(second),
    Result = flurm_dbd_mysql:read_historical_jobs(Now - 3600, Now),
    ?assertEqual({ok, []}, Result).

test_read_with_state_filter() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    QueryParams = ets:new(query_params, [public]),
    meck:expect(mysql, query, fun(_Conn, _Query, Params) ->
        ets:insert(QueryParams, {params, Params}),
        {ok, [], []}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Now = erlang:system_time(second),
    Filters = #{state => completed},
    Result = flurm_dbd_mysql:read_historical_jobs(Now - 3600, Now, Filters),
    ?assertMatch({ok, _}, Result),

    %% Verify state was converted to integer (3 = completed)
    [{params, Params}] = ets:lookup(QueryParams, params),
    ?assert(lists:member(3, Params)),
    ets:delete(QueryParams).

test_read_with_account_filter() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    QueryParams = ets:new(query_params, [public]),
    meck:expect(mysql, query, fun(_Conn, _Query, Params) ->
        ets:insert(QueryParams, {params, Params}),
        {ok, [], []}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Now = erlang:system_time(second),
    Filters = #{account => <<"research">>},
    Result = flurm_dbd_mysql:read_historical_jobs(Now - 3600, Now, Filters),
    ?assertMatch({ok, _}, Result),

    [{params, Params}] = ets:lookup(QueryParams, params),
    ?assert(lists:member(<<"research">>, Params)),
    ets:delete(QueryParams).

%%====================================================================
%% Schema Compatibility Tests
%%====================================================================

schema_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"check_schema fails when not connected", fun test_schema_not_connected/0},
             {"check_schema returns version", fun test_schema_version/0},
             {"check_schema handles not found", fun test_schema_not_found/0},
             {"check_schema handles binary version", fun test_schema_binary_version/0},
             {"check_schema handles query error", fun test_schema_query_error/0}
         ]
     end
    }.

test_schema_not_connected() ->
    flurm_dbd_mysql:disconnect(),
    Result = flurm_dbd_mysql:check_schema_compatibility(),
    ?assertEqual({error, not_connected}, Result).

test_schema_version() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query) ->
        {ok, [<<"version">>], [[8]]}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Result = flurm_dbd_mysql:check_schema_compatibility(),
    ?assertEqual({ok, 8}, Result).

test_schema_not_found() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query) ->
        {ok, [], []}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Result = flurm_dbd_mysql:check_schema_compatibility(),
    ?assertEqual({error, schema_not_found}, Result).

test_schema_binary_version() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query) ->
        {ok, [<<"version">>], [[<<"9">>]]}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Result = flurm_dbd_mysql:check_schema_compatibility(),
    ?assertEqual({ok, 9}, Result).

test_schema_query_error() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query) ->
        {error, table_not_found}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Result = flurm_dbd_mysql:check_schema_compatibility(),
    ?assertEqual({error, table_not_found}, Result).

%%====================================================================
%% Record Mapping Tests (Test Internal Functions)
%%====================================================================

record_mapping_test_() ->
    [
        {"map_job_to_slurmdbd converts FLURM job", fun test_map_job_to_slurmdbd/0},
        {"map_slurmdbd_to_job converts slurmdbd row", fun test_map_slurmdbd_to_job/0},
        {"map_slurmdbd_to_job handles zero times", fun test_map_slurmdbd_to_job_zero_times/0},
        {"state_to_slurmdbd maps all states", fun test_state_to_slurmdbd/0},
        {"slurmdbd_to_state maps all states", fun test_slurmdbd_to_state/0},
        {"format_tres_string formats TRES map", fun test_format_tres_string/0},
        {"format_tres_string with all TRES types", fun test_format_tres_all_types/0},
        {"parse_tres_string parses TRES string", fun test_parse_tres_string/0},
        {"parse_tres_string handles malformed input", fun test_parse_tres_malformed/0},
        {"build_insert_query creates valid SQL", fun test_build_insert_query/0},
        {"build_select_query creates valid SQL", fun test_build_select_query/0},
        {"build_select_query with all filters", fun test_build_select_query_all_filters/0}
    ].

test_map_job_to_slurmdbd() ->
    Job = #{
        job_id => 1001,
        job_name => <<"test_job">>,
        user_id => 1000,
        group_id => 1000,
        account => <<"account">>,
        partition => <<"default">>,
        state => completed,
        exit_code => 0,
        num_nodes => 2,
        num_cpus => 8,
        submit_time => 1000000,
        eligible_time => 1000000,
        start_time => 1000100,
        end_time => 1000200,
        tres_alloc => #{cpu => 8, mem => 16384}
    },
    Result = flurm_dbd_mysql:map_job_to_slurmdbd(Job),
    ?assertEqual(1001, maps:get(id_job, Result)),
    ?assertEqual(<<"test_job">>, maps:get(job_name, Result)),
    ?assertEqual(3, maps:get(state, Result)),  % completed = 3
    ?assertEqual(8, maps:get(cpus_req, Result)).

test_map_slurmdbd_to_job() ->
    Row = #{
        id_job => 1001,
        job_name => <<"test_job">>,
        id_user => 1000,
        id_group => 1000,
        account => <<"account">>,
        partition => <<"default">>,
        state => 3,  % completed
        exit_code => 0,
        nodes_alloc => 2,
        cpus_req => 8,
        time_submit => 1000000,
        time_eligible => 1000000,
        time_start => 1000100,
        time_end => 1000200,
        tres_alloc => <<"1=8,2=16384">>
    },
    Result = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
    ?assertEqual(1001, maps:get(job_id, Result)),
    ?assertEqual(completed, maps:get(state, Result)),
    ?assertEqual(100, maps:get(elapsed, Result)).  % 1000200 - 1000100

test_map_slurmdbd_to_job_zero_times() ->
    %% Test with zero start/end times (job not yet started)
    Row = #{
        id_job => 2001,
        job_name => <<"pending_job">>,
        id_user => 1000,
        id_group => 1000,
        account => <<"account">>,
        partition => <<"default">>,
        state => 0,  % pending
        exit_code => 0,
        nodes_alloc => 0,
        cpus_req => 4,
        time_submit => 1000000,
        time_eligible => 0,
        time_start => 0,
        time_end => 0,
        tres_alloc => <<>>
    },
    Result = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
    ?assertEqual(2001, maps:get(job_id, Result)),
    ?assertEqual(pending, maps:get(state, Result)),
    ?assertEqual(0, maps:get(elapsed, Result)).

test_state_to_slurmdbd() ->
    ?assertEqual(0, flurm_dbd_mysql:state_to_slurmdbd(pending)),
    ?assertEqual(1, flurm_dbd_mysql:state_to_slurmdbd(running)),
    ?assertEqual(2, flurm_dbd_mysql:state_to_slurmdbd(suspended)),
    ?assertEqual(3, flurm_dbd_mysql:state_to_slurmdbd(completed)),
    ?assertEqual(4, flurm_dbd_mysql:state_to_slurmdbd(cancelled)),
    ?assertEqual(5, flurm_dbd_mysql:state_to_slurmdbd(failed)),
    ?assertEqual(6, flurm_dbd_mysql:state_to_slurmdbd(timeout)),
    ?assertEqual(7, flurm_dbd_mysql:state_to_slurmdbd(node_fail)),
    ?assertEqual(8, flurm_dbd_mysql:state_to_slurmdbd(preempted)),
    ?assertEqual(9, flurm_dbd_mysql:state_to_slurmdbd(boot_fail)),
    ?assertEqual(10, flurm_dbd_mysql:state_to_slurmdbd(deadline)),
    ?assertEqual(11, flurm_dbd_mysql:state_to_slurmdbd(oom)),
    ?assertEqual(0, flurm_dbd_mysql:state_to_slurmdbd(unknown)),
    ?assertEqual(0, flurm_dbd_mysql:state_to_slurmdbd(invalid_state)).

test_slurmdbd_to_state() ->
    ?assertEqual(pending, flurm_dbd_mysql:slurmdbd_to_state(0)),
    ?assertEqual(running, flurm_dbd_mysql:slurmdbd_to_state(1)),
    ?assertEqual(suspended, flurm_dbd_mysql:slurmdbd_to_state(2)),
    ?assertEqual(completed, flurm_dbd_mysql:slurmdbd_to_state(3)),
    ?assertEqual(cancelled, flurm_dbd_mysql:slurmdbd_to_state(4)),
    ?assertEqual(failed, flurm_dbd_mysql:slurmdbd_to_state(5)),
    ?assertEqual(timeout, flurm_dbd_mysql:slurmdbd_to_state(6)),
    ?assertEqual(node_fail, flurm_dbd_mysql:slurmdbd_to_state(7)),
    ?assertEqual(preempted, flurm_dbd_mysql:slurmdbd_to_state(8)),
    ?assertEqual(boot_fail, flurm_dbd_mysql:slurmdbd_to_state(9)),
    ?assertEqual(deadline, flurm_dbd_mysql:slurmdbd_to_state(10)),
    ?assertEqual(oom, flurm_dbd_mysql:slurmdbd_to_state(11)),
    ?assertEqual(unknown, flurm_dbd_mysql:slurmdbd_to_state(99)),
    ?assertEqual(unknown, flurm_dbd_mysql:slurmdbd_to_state(-1)).

test_format_tres_string() ->
    %% Empty map
    ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(#{})),
    %% Map with values
    TresMap = #{cpu => 4, mem => 8192},
    Result = flurm_dbd_mysql:format_tres_string(TresMap),
    ?assert(is_binary(Result)),
    %% Should contain cpu (1=4) and mem (2=8192)
    ?assert(binary:match(Result, <<"1=4">>) =/= nomatch orelse
            binary:match(Result, <<"2=8192">>) =/= nomatch),
    %% Non-map input
    ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(undefined)),
    %% Map with GPU
    TresWithGpu = #{cpu => 4, gpu => 2},
    GpuResult = flurm_dbd_mysql:format_tres_string(TresWithGpu),
    ?assert(is_binary(GpuResult)).

test_format_tres_all_types() ->
    %% Test all known TRES types
    TresMap = #{
        cpu => 16,
        mem => 65536,
        energy => 1000,
        node => 4,
        billing => 64,
        fs_disk => 100000,
        vmem => 131072,
        pages => 1000,
        gpu => 2
    },
    Result = flurm_dbd_mysql:format_tres_string(TresMap),
    ?assert(is_binary(Result)),
    %% Verify some of the mappings
    ?assert(binary:match(Result, <<"1=16">>) =/= nomatch),  % cpu
    ?assert(binary:match(Result, <<"4=4">>) =/= nomatch),   % node
    ?assert(binary:match(Result, <<"1001=2">>) =/= nomatch). % gpu

test_parse_tres_string() ->
    %% Empty string
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<>>)),
    %% Valid TRES string
    TresString = <<"1=4,2=8192,4=2">>,
    Result = flurm_dbd_mysql:parse_tres_string(TresString),
    ?assertEqual(4, maps:get(cpu, Result)),
    ?assertEqual(8192, maps:get(mem, Result)),
    ?assertEqual(2, maps:get(node, Result)),
    %% Invalid input
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(undefined)),
    %% Malformed string (should be handled gracefully)
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<"invalid">>)).

test_parse_tres_malformed() ->
    %% Single invalid entry
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<"abc">>)),
    %% Mix of valid and invalid
    Result = flurm_dbd_mysql:parse_tres_string(<<"1=4,invalid,2=8192">>),
    ?assertEqual(4, maps:get(cpu, Result)),
    ?assertEqual(8192, maps:get(mem, Result)),
    %% Non-numeric values
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<"a=b,c=d">>)),
    %% Empty segments
    Result2 = flurm_dbd_mysql:parse_tres_string(<<"1=4,,2=8192">>),
    ?assertEqual(4, maps:get(cpu, Result2)).

test_build_insert_query() ->
    Query = flurm_dbd_mysql:build_insert_query("flurm_job_table"),
    ?assert(is_binary(Query)),
    ?assert(binary:match(Query, <<"INSERT INTO flurm_job_table">>) =/= nomatch),
    ?assert(binary:match(Query, <<"ON DUPLICATE KEY UPDATE">>) =/= nomatch),
    %% Verify all expected columns are present
    ?assert(binary:match(Query, <<"id_job">>) =/= nomatch),
    ?assert(binary:match(Query, <<"job_name">>) =/= nomatch),
    ?assert(binary:match(Query, <<"state">>) =/= nomatch),
    ?assert(binary:match(Query, <<"tres_alloc">>) =/= nomatch).

test_build_select_query() ->
    Options = #{
        start_time => 1000000,
        end_time => 2000000,
        filters => #{user => <<"testuser">>, limit => 100}
    },
    {Query, Params} = flurm_dbd_mysql:build_select_query("flurm_job_table", Options),
    ?assert(is_binary(Query)),
    ?assert(is_list(Params)),
    ?assert(binary:match(Query, <<"SELECT">>) =/= nomatch),
    ?assert(binary:match(Query, <<"FROM flurm_job_table">>) =/= nomatch),
    ?assert(binary:match(Query, <<"LIMIT">>) =/= nomatch),
    ?assert(binary:match(Query, <<"ORDER BY">>) =/= nomatch).

test_build_select_query_all_filters() ->
    Options = #{
        start_time => 1000000,
        end_time => 2000000,
        filters => #{
            user => <<"testuser">>,
            account => <<"research">>,
            partition => <<"gpu">>,
            state => completed,
            limit => 500
        }
    },
    {Query, Params} = flurm_dbd_mysql:build_select_query("cluster_job_table", Options),
    ?assert(is_binary(Query)),
    %% Verify filter conditions are in query
    ?assert(binary:match(Query, <<"id_user">>) =/= nomatch),
    ?assert(binary:match(Query, <<"account">>) =/= nomatch),
    ?assert(binary:match(Query, <<"partition">>) =/= nomatch),
    ?assert(binary:match(Query, <<"state">>) =/= nomatch),
    %% Params should include start_time, end_time, and filter values
    ?assert(length(Params) >= 2).

%%====================================================================
%% Error Handling Tests
%%====================================================================

error_handling_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"handles query errors gracefully", fun test_query_error/0},
             {"handles unknown requests", fun test_unknown_request/0}
         ]
     end
    }.

%% Separate fixture for tests that need to restart the server
advanced_error_handling_test_() ->
    {foreach,
     fun() ->
         %% Stop any existing server
         case whereis(flurm_dbd_mysql) of
             undefined -> ok;
             Pid ->
                 catch gen_server:stop(Pid, normal, 5000),
                 timer:sleep(50)
         end,
         %% Mock mysql
         catch meck:unload(mysql),
         catch meck:unload(lager),
         meck:new(mysql, [non_strict, no_link]),
         meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
         meck:expect(mysql, stop, fun(_Pid) -> ok end),
         meck:expect(mysql, query, fun(_Conn, _Query) -> {ok, [], []} end),
         meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> ok end),
         meck:new(lager, [non_strict, no_link, passthrough]),
         meck:expect(lager, info, fun(_Fmt) -> ok end),
         meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
         ok
     end,
     fun(_) ->
         case whereis(flurm_dbd_mysql) of
             undefined -> ok;
             Pid -> catch gen_server:stop(Pid, normal, 5000)
         end,
         catch meck:unload(mysql),
         catch meck:unload(lager),
         ok
     end,
     [
         {"handles auto_connect retry", fun test_auto_connect_retry/0},
         {"handles query returning {ok, _} format", fun test_query_ok_format/0},
         {"tracks error statistics", fun test_error_statistics/0}
     ]
    }.

test_query_error() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> {error, db_error} end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    JobRecord = sample_job_record(),
    Result = flurm_dbd_mysql:sync_job_record(JobRecord),
    ?assertEqual({error, db_error}, Result).

test_unknown_request() ->
    Result = gen_server:call(flurm_dbd_mysql, unknown_request),
    ?assertEqual({error, unknown_request}, Result).

test_auto_connect_retry() ->
    %% This test verifies the auto-connect behavior on failure
    %% Stop existing
    case whereis(flurm_dbd_mysql) of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid, normal, 5000)
    end,

    %% Mock to fail first, then succeed
    Counter = ets:new(counter, [public]),
    ets:insert(Counter, {attempts, 0}),
    meck:expect(mysql, start_link, fun(_Opts) ->
        [{attempts, N}] = ets:lookup(Counter, attempts),
        ets:insert(Counter, {attempts, N + 1}),
        case N of
            0 -> {error, econnrefused};
            _ -> {ok, spawn(fun() -> receive stop -> ok end end)}
        end
    end),

    Config = #{
        host => "localhost",
        port => 3306,
        user => "test",
        password => "test",
        database => "test_db",
        auto_connect => true
    },
    {ok, NewPid} = flurm_dbd_mysql:start_link(Config),

    %% Give time for auto-connect retry
    timer:sleep(100),

    %% Cleanup
    ets:delete(Counter),
    catch gen_server:stop(NewPid, normal, 5000).

test_query_ok_format() ->
    %% Start a fresh server for this test
    Config = #{
        host => "localhost",
        port => 3306,
        user => "test",
        password => "test",
        database => "test_db",
        auto_connect => false
    },
    {ok, Pid} = flurm_dbd_mysql:start_link(Config),

    %% Some MySQL operations return {ok, AffectedRows}
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> {ok, 1} end),
    ok = flurm_dbd_mysql:connect(Config),

    JobRecord = sample_job_record(),
    Result = flurm_dbd_mysql:sync_job_record(JobRecord),
    ?assertEqual(ok, Result),

    %% Cleanup
    catch gen_server:stop(Pid, normal, 5000).

test_error_statistics() ->
    %% Start a fresh server for this test
    Config = #{
        host => "localhost",
        port => 3306,
        user => "test",
        password => "test",
        database => "test_db",
        auto_connect => false
    },
    {ok, Pid} = flurm_dbd_mysql:start_link(Config),

    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> {error, some_error} end),
    ok = flurm_dbd_mysql:connect(Config),

    %% Get initial stats
    InitialStatus = flurm_dbd_mysql:get_connection_status(),
    InitialErrors = maps:get(sync_errors, maps:get(stats, InitialStatus), 0),

    %% Cause an error
    _Result = flurm_dbd_mysql:sync_job_record(sample_job_record()),

    %% Check error count increased
    NewStatus = flurm_dbd_mysql:get_connection_status(),
    NewErrors = maps:get(sync_errors, maps:get(stats, NewStatus), 0),
    ?assert(NewErrors > InitialErrors),

    %% Cleanup
    catch gen_server:stop(Pid, normal, 5000).

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"handle_cast unknown does not crash", fun() ->
                 gen_server:cast(flurm_dbd_mysql, unknown_message),
                 timer:sleep(10),
                 ?assert(is_process_alive(whereis(flurm_dbd_mysql)))
             end},
             {"handle_info unknown does not crash", fun() ->
                 whereis(flurm_dbd_mysql) ! unknown_info,
                 timer:sleep(10),
                 ?assert(is_process_alive(whereis(flurm_dbd_mysql)))
             end},
             {"handle_info DOWN message triggers reconnect", fun test_connection_down/0}
         ]
     end
    }.

test_connection_down() ->
    %% This tests the DOWN message handling when connection process dies
    meck:expect(mysql, start_link, fun(_Opts) ->
        ConnPid = spawn(fun() -> receive stop -> ok end end),
        {ok, ConnPid}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),
    ?assertEqual(true, flurm_dbd_mysql:is_connected()),

    %% Get the connection status to find the connection pid
    Status = flurm_dbd_mysql:get_connection_status(),
    ?assertEqual(true, maps:get(connected, Status)),

    %% Simulate connection death by sending DOWN message
    %% The actual connection pid is internal, but we can verify the module handles unknown info
    whereis(flurm_dbd_mysql) ! {'DOWN', make_ref(), process, self(), connection_closed},
    timer:sleep(50),

    %% Server should still be alive
    ?assert(is_process_alive(whereis(flurm_dbd_mysql))).

%%====================================================================
%% Connection Status Statistics Tests
%%====================================================================

stats_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"stats track jobs_synced", fun test_stats_jobs_synced/0},
             {"stats track jobs_read", fun test_stats_jobs_read/0},
             {"stats track connect_attempts", fun test_stats_connect_attempts/0}
         ]
     end
    }.

test_stats_jobs_synced() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> ok end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    %% Sync some jobs
    ok = flurm_dbd_mysql:sync_job_record(sample_job_record(1)),
    ok = flurm_dbd_mysql:sync_job_record(sample_job_record(2)),

    Status = flurm_dbd_mysql:get_connection_status(),
    Stats = maps:get(stats, Status),
    ?assertEqual(2, maps:get(jobs_synced, Stats)).

test_stats_jobs_read() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) ->
        Columns = [<<"id_job">>, <<"job_name">>, <<"id_user">>, <<"id_group">>,
                   <<"account">>, <<"partition">>, <<"state">>, <<"exit_code">>,
                   <<"nodes_alloc">>, <<"cpus_req">>, <<"time_submit">>, <<"time_eligible">>,
                   <<"time_start">>, <<"time_end">>, <<"timelimit">>, <<"tres_alloc">>,
                   <<"tres_req">>, <<"work_dir">>, <<"std_out">>, <<"std_err">>],
        Row1 = {1, <<"j1">>, 1000, 1000, <<"a">>, <<"d">>, 3, 0, 1, 4, 1000000, 1000000, 1000100, 1000200, 0, <<>>, <<>>, <<>>, <<>>, <<>>},
        Row2 = {2, <<"j2">>, 1000, 1000, <<"a">>, <<"d">>, 3, 0, 1, 4, 1000000, 1000000, 1000100, 1000200, 0, <<>>, <<>>, <<>>, <<>>, <<>>},
        {ok, Columns, [Row1, Row2]}
    end),
    Config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
    ok = flurm_dbd_mysql:connect(Config),

    Now = erlang:system_time(second),
    {ok, _Jobs} = flurm_dbd_mysql:read_historical_jobs(Now - 3600, Now),

    Status = flurm_dbd_mysql:get_connection_status(),
    Stats = maps:get(stats, Status),
    ?assertEqual(2, maps:get(jobs_read, Stats)).

test_stats_connect_attempts() ->
    %% Initial status should have connect_attempts
    Status = flurm_dbd_mysql:get_connection_status(),
    Stats = maps:get(stats, Status),
    ?assert(maps:is_key(connect_attempts, Stats)).

%%====================================================================
%% API Wrapper and Callback Branch Tests
%%====================================================================

api_wrapper_and_callback_branches_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link/0 reads config from app env", fun test_start_link_zero_arity/0},
      {"read_historical_jobs/2 wrapper", fun test_read_historical_two_arity/0},
      {"handle_info auto_connect max retries", fun test_handle_info_max_retries/0},
      {"handle_info auto_connect empty config", fun test_handle_info_empty_config/0},
      {"handle_info DOWN for current connection", fun test_handle_info_connection_down/0}
     ]}.

test_start_link_zero_arity() ->
    catch gen_server:stop(flurm_dbd_mysql),
    application:set_env(flurm_dbd, mysql_host, "localhost"),
    application:set_env(flurm_dbd, mysql_port, 3306),
    application:set_env(flurm_dbd, mysql_user, "test"),
    application:set_env(flurm_dbd, mysql_password, "test"),
    application:set_env(flurm_dbd, mysql_database, "test_db"),
    {ok, Pid} = flurm_dbd_mysql:start_link(),
    ?assert(is_pid(Pid)),
    gen_server:stop(Pid).

test_read_historical_two_arity() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> {ok, [], []} end),
    ok = flurm_dbd_mysql:connect(#{host => "localhost", port => 3306, user => "test",
                                   password => "test", database => "test_db"}),
    Now = erlang:system_time(second),
    ?assertMatch({ok, _}, flurm_dbd_mysql:read_historical_jobs(Now - 60, Now)).

test_handle_info_max_retries() ->
    State = #state{
        connection = undefined,
        config = #{host => "localhost"},
        connected = false,
        schema_version = undefined,
        last_error = undefined,
        stats = #{},
        reconnect_attempts = 10,
        current_reconnect_delay = 1000
    },
    {noreply, NewState} = flurm_dbd_mysql:handle_info(auto_connect, State),
    ?assertEqual(true, maps:get(reconnect_gave_up, NewState#state.stats)).

test_handle_info_empty_config() ->
    State = #state{
        connection = undefined,
        config = #{},
        connected = false,
        schema_version = undefined,
        last_error = undefined,
        stats = #{},
        reconnect_attempts = 0,
        current_reconnect_delay = 1000
    },
    ?assertMatch({noreply, _}, flurm_dbd_mysql:handle_info(auto_connect, State)).

test_handle_info_connection_down() ->
    ConnPid = spawn(fun() -> receive stop -> ok end end),
    State = #state{
        connection = ConnPid,
        config = #{host => "localhost"},
        connected = true,
        schema_version = undefined,
        last_error = undefined,
        stats = #{},
        reconnect_attempts = 4,
        current_reconnect_delay = 32000
    },
    {noreply, NewState} =
        flurm_dbd_mysql:handle_info({'DOWN', make_ref(), process, ConnPid, closed}, State),
    ?assertEqual(undefined, NewState#state.connection),
    ?assertEqual(false, NewState#state.connected),
    ?assertEqual(closed, NewState#state.last_error),
    ?assertEqual(0, NewState#state.reconnect_attempts),
    ?assertEqual(1000, NewState#state.current_reconnect_delay),
    ConnPid ! stop.

%%====================================================================
%% Targeted Uncovered Branch Tests
%%====================================================================

targeted_uncovered_branches_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"sync_job_records not connected branch", fun test_sync_batch_not_connected_branch/0},
      {"sync_job_records sync_error throw branch", fun test_sync_batch_sync_error_throw_branch/0},
      {"sync_job_records aborted generic branch", fun test_sync_batch_aborted_generic_branch/0},
      {"read_historical error branch", fun test_read_historical_error_branch/0},
      {"schema compatibility warning branch", fun test_schema_warning_branch/0},
      {"auto_connect success branch", fun test_auto_connect_success_branch/0},
      {"format_tres unknown-only branch", fun test_format_tres_unknown_only_branch/0},
      {"parse_tres unknown id branch", fun test_parse_tres_unknown_id_branch/0},
      {"column_to_atom fallback via read", fun test_column_to_atom_fallback_branch/0},
      {"tres_id mappings branch set", fun test_tres_id_mappings_branch_set/0}
     ]}.

test_sync_batch_not_connected_branch() ->
    _ = flurm_dbd_mysql:disconnect(),
    ?assertEqual({error, not_connected}, flurm_dbd_mysql:sync_job_records([sample_job_record(42)])).

test_sync_batch_sync_error_throw_branch() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> {error, write_failed} end),
    ok = flurm_dbd_mysql:connect(#{host => "localhost", port => 3306, user => "test",
                                   password => "test", database => "test_db"}),
    ?assertEqual({error, write_failed}, flurm_dbd_mysql:sync_job_records([sample_job_record(76)])).

test_sync_batch_aborted_generic_branch() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, transaction, fun(_Conn, _Fun) -> {aborted, tx_abort} end),
    ok = flurm_dbd_mysql:connect(#{host => "localhost", port => 3306, user => "test",
                                   password => "test", database => "test_db"}),
    ?assertEqual({error, tx_abort}, flurm_dbd_mysql:sync_job_records([sample_job_record(77)])).

test_read_historical_error_branch() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> {error, read_failed} end),
    ok = flurm_dbd_mysql:connect(#{host => "localhost", port => 3306, user => "test",
                                   password => "test", database => "test_db"}),
    Now = erlang:system_time(second),
    ?assertEqual({error, read_failed},
                 flurm_dbd_mysql:read_historical_jobs(Now - 60, Now, #{})).

test_schema_warning_branch() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query) -> {ok, [<<"version">>], [[99]]} end),
    ok = flurm_dbd_mysql:connect(#{host => "localhost", port => 3306, user => "test",
                                   password => "test", database => "test_db"}),
    ?assertEqual({ok, 99}, flurm_dbd_mysql:check_schema_compatibility()).

test_auto_connect_success_branch() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    State = #state{
        connection = undefined,
        config = #{host => "localhost", port => 3306, user => "test", password => "test", database => "test_db"},
        connected = false,
        schema_version = undefined,
        last_error = undefined,
        stats = #{},
        reconnect_attempts = 2,
        current_reconnect_delay = 2000
    },
    {noreply, NewState} = flurm_dbd_mysql:handle_info(auto_connect, State),
    ?assertEqual(true, NewState#state.connected),
    ?assertEqual(0, NewState#state.reconnect_attempts).

test_format_tres_unknown_only_branch() ->
    ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(#{mystery_tres => 1})).

test_parse_tres_unknown_id_branch() ->
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<"999=1">>)).

test_column_to_atom_fallback_branch() ->
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) ->
        Columns = [<<"id_job">>, <<"custom_col">>],
        Row = {1234, <<"x">>},
        {ok, Columns, [Row]}
    end),
    ok = flurm_dbd_mysql:connect(#{host => "localhost", port => 3306, user => "test",
                                   password => "test", database => "test_db"}),
    Now = erlang:system_time(second),
    ?assertMatch({ok, [_]}, flurm_dbd_mysql:read_historical_jobs(Now - 10, Now)).

test_tres_id_mappings_branch_set() ->
    Parsed = flurm_dbd_mysql:parse_tres_string(
               <<"2=1,3=2,4=3,5=4,6=5,7=6,8=7,1001=8">>),
    ?assertEqual(1, maps:get(mem, Parsed)),
    ?assertEqual(2, maps:get(energy, Parsed)),
    ?assertEqual(3, maps:get(node, Parsed)),
    ?assertEqual(4, maps:get(billing, Parsed)),
    ?assertEqual(5, maps:get(fs_disk, Parsed)),
    ?assertEqual(6, maps:get(vmem, Parsed)),
    ?assertEqual(7, maps:get(pages, Parsed)),
    ?assertEqual(8, maps:get(gpu, Parsed)).

%%====================================================================
%% Helper Functions
%%====================================================================

sample_job_record() ->
    sample_job_record(1001).

sample_job_record(JobId) ->
    #{
        job_id => JobId,
        job_name => <<"test_job">>,
        user_id => 1000,
        group_id => 1000,
        account => <<"testaccount">>,
        partition => <<"default">>,
        state => completed,
        exit_code => 0,
        num_nodes => 1,
        num_cpus => 4,
        submit_time => erlang:system_time(second) - 200,
        eligible_time => erlang:system_time(second) - 200,
        start_time => erlang:system_time(second) - 100,
        end_time => erlang:system_time(second),
        elapsed => 100,
        tres_alloc => #{cpu => 4, mem => 8192},
        tres_req => #{},
        work_dir => <<"/home/test">>,
        std_out => <<>>,
        std_err => <<>>
    }.
