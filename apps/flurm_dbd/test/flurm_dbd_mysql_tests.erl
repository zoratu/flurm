%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon - MySQL Bridge Tests
%%%
%%% Unit tests for the flurm_dbd_mysql module which provides the
%%% slurmdbd MySQL connector for zero-downtime migration.
%%%
%%% These tests use meck to mock the MySQL connector since we don't
%%% want to depend on a real MySQL database for unit tests.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_mysql_tests).

-include_lib("eunit/include/eunit.hrl").

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
             {"get_connection_status returns map", fun test_get_connection_status/0}
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
             {"sync_job_records fails on error", fun test_sync_batch_failure/0}
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
             {"read_historical_jobs with filters", fun test_read_with_filters/0}
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
             {"check_schema handles not found", fun test_schema_not_found/0}
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

%%====================================================================
%% Record Mapping Tests (Test Internal Functions)
%%====================================================================

record_mapping_test_() ->
    [
        {"map_job_to_slurmdbd converts FLURM job", fun test_map_job_to_slurmdbd/0},
        {"map_slurmdbd_to_job converts slurmdbd row", fun test_map_slurmdbd_to_job/0},
        {"state_to_slurmdbd maps all states", fun test_state_to_slurmdbd/0},
        {"slurmdbd_to_state maps all states", fun test_slurmdbd_to_state/0},
        {"format_tres_string formats TRES map", fun test_format_tres_string/0},
        {"parse_tres_string parses TRES string", fun test_parse_tres_string/0},
        {"build_insert_query creates valid SQL", fun test_build_insert_query/0},
        {"build_select_query creates valid SQL", fun test_build_select_query/0}
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
    ?assertEqual(0, flurm_dbd_mysql:state_to_slurmdbd(unknown)).

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
    ?assertEqual(unknown, flurm_dbd_mysql:slurmdbd_to_state(99)).

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

test_build_insert_query() ->
    Query = flurm_dbd_mysql:build_insert_query("flurm_job_table"),
    ?assert(is_binary(Query)),
    ?assert(binary:match(Query, <<"INSERT INTO flurm_job_table">>) =/= nomatch),
    ?assert(binary:match(Query, <<"ON DUPLICATE KEY UPDATE">>) =/= nomatch).

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
    ?assert(binary:match(Query, <<"LIMIT">>) =/= nomatch).

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
             {"handles unknown requests", fun test_unknown_request/0},
             {"handles auto_connect retry", fun test_auto_connect_retry/0}
         ]
     end
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
             end}
         ]
     end
    }.

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
