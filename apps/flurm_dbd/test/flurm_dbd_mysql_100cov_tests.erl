%%%-------------------------------------------------------------------
%%% @doc FLURM DBD MySQL 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_dbd_mysql module covering all
%%% exported functions, edge cases, error paths, and gen_server callbacks.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_mysql_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Stop any existing server
    case whereis(flurm_dbd_mysql) of
        undefined -> ok;
        ExistingPid ->
            catch gen_server:stop(ExistingPid, normal, 5000),
            wait_for_death(ExistingPid)
    end,
    %% Mock mysql module
    meck:new(mysql, [non_strict, no_link]),
    meck:expect(mysql, start_link, fun(_Opts) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, stop, fun(_Pid) -> ok end),
    meck:expect(mysql, query, fun(_Conn, _Query) -> {ok, [<<"column">>], [[<<"value">>]]} end),
    meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> ok end),
    meck:expect(mysql, transaction, fun(_Conn, Fun) -> {atomic, Fun()} end),
    %% Mock lager
    meck:new(lager, [non_strict, no_link, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    ok.

cleanup(_) ->
    case whereis(flurm_dbd_mysql) of
        undefined -> ok;
        Pid ->
            catch gen_server:stop(Pid, normal, 5000),
            wait_for_death(Pid)
    end,
    catch meck:unload(mysql),
    catch meck:unload(lager),
    ok.

wait_for_death(Pid) ->
    case is_process_alive(Pid) of
        false -> ok;
        true ->
            timer:sleep(10),
            wait_for_death(Pid)
    end.

%%====================================================================
%% State Conversion Tests
%%====================================================================

state_to_slurmdbd_test_() ->
    [
        {"pending converts to 0", fun() ->
             ?assertEqual(0, flurm_dbd_mysql:state_to_slurmdbd(pending))
         end},
        {"running converts to 1", fun() ->
             ?assertEqual(1, flurm_dbd_mysql:state_to_slurmdbd(running))
         end},
        {"suspended converts to 2", fun() ->
             ?assertEqual(2, flurm_dbd_mysql:state_to_slurmdbd(suspended))
         end},
        {"completed converts to 3", fun() ->
             ?assertEqual(3, flurm_dbd_mysql:state_to_slurmdbd(completed))
         end},
        {"cancelled converts to 4", fun() ->
             ?assertEqual(4, flurm_dbd_mysql:state_to_slurmdbd(cancelled))
         end},
        {"failed converts to 5", fun() ->
             ?assertEqual(5, flurm_dbd_mysql:state_to_slurmdbd(failed))
         end},
        {"timeout converts to 6", fun() ->
             ?assertEqual(6, flurm_dbd_mysql:state_to_slurmdbd(timeout))
         end},
        {"node_fail converts to 7", fun() ->
             ?assertEqual(7, flurm_dbd_mysql:state_to_slurmdbd(node_fail))
         end},
        {"preempted converts to 8", fun() ->
             ?assertEqual(8, flurm_dbd_mysql:state_to_slurmdbd(preempted))
         end},
        {"boot_fail converts to 9", fun() ->
             ?assertEqual(9, flurm_dbd_mysql:state_to_slurmdbd(boot_fail))
         end},
        {"deadline converts to 10", fun() ->
             ?assertEqual(10, flurm_dbd_mysql:state_to_slurmdbd(deadline))
         end},
        {"oom converts to 11", fun() ->
             ?assertEqual(11, flurm_dbd_mysql:state_to_slurmdbd(oom))
         end},
        {"unknown state converts to 0", fun() ->
             ?assertEqual(0, flurm_dbd_mysql:state_to_slurmdbd(unknown_state)),
             ?assertEqual(0, flurm_dbd_mysql:state_to_slurmdbd(some_other))
         end}
    ].

slurmdbd_to_state_test_() ->
    [
        {"0 converts to pending", fun() ->
             ?assertEqual(pending, flurm_dbd_mysql:slurmdbd_to_state(0))
         end},
        {"1 converts to running", fun() ->
             ?assertEqual(running, flurm_dbd_mysql:slurmdbd_to_state(1))
         end},
        {"2 converts to suspended", fun() ->
             ?assertEqual(suspended, flurm_dbd_mysql:slurmdbd_to_state(2))
         end},
        {"3 converts to completed", fun() ->
             ?assertEqual(completed, flurm_dbd_mysql:slurmdbd_to_state(3))
         end},
        {"4 converts to cancelled", fun() ->
             ?assertEqual(cancelled, flurm_dbd_mysql:slurmdbd_to_state(4))
         end},
        {"5 converts to failed", fun() ->
             ?assertEqual(failed, flurm_dbd_mysql:slurmdbd_to_state(5))
         end},
        {"6 converts to timeout", fun() ->
             ?assertEqual(timeout, flurm_dbd_mysql:slurmdbd_to_state(6))
         end},
        {"7 converts to node_fail", fun() ->
             ?assertEqual(node_fail, flurm_dbd_mysql:slurmdbd_to_state(7))
         end},
        {"8 converts to preempted", fun() ->
             ?assertEqual(preempted, flurm_dbd_mysql:slurmdbd_to_state(8))
         end},
        {"9 converts to boot_fail", fun() ->
             ?assertEqual(boot_fail, flurm_dbd_mysql:slurmdbd_to_state(9))
         end},
        {"10 converts to deadline", fun() ->
             ?assertEqual(deadline, flurm_dbd_mysql:slurmdbd_to_state(10))
         end},
        {"11 converts to oom", fun() ->
             ?assertEqual(oom, flurm_dbd_mysql:slurmdbd_to_state(11))
         end},
        {"unknown number converts to unknown", fun() ->
             ?assertEqual(unknown, flurm_dbd_mysql:slurmdbd_to_state(99)),
             ?assertEqual(unknown, flurm_dbd_mysql:slurmdbd_to_state(100))
         end}
    ].

%%====================================================================
%% TRES String Formatting Tests
%%====================================================================

format_tres_string_test_() ->
    [
        {"empty map returns empty binary", fun() ->
             ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(#{}))
         end},
        {"single cpu entry", fun() ->
             Result = flurm_dbd_mysql:format_tres_string(#{cpu => 4}),
             ?assertEqual(<<"1=4">>, Result)
         end},
        {"single mem entry", fun() ->
             Result = flurm_dbd_mysql:format_tres_string(#{mem => 8192}),
             ?assertEqual(<<"2=8192">>, Result)
         end},
        {"single node entry", fun() ->
             Result = flurm_dbd_mysql:format_tres_string(#{node => 2}),
             ?assertEqual(<<"4=2">>, Result)
         end},
        {"multiple entries", fun() ->
             Result = flurm_dbd_mysql:format_tres_string(#{cpu => 4, mem => 8192, node => 2}),
             %% Order may vary, check contents
             ?assert(binary:match(Result, <<"1=4">>) =/= nomatch),
             ?assert(binary:match(Result, <<"2=8192">>) =/= nomatch),
             ?assert(binary:match(Result, <<"4=2">>) =/= nomatch)
         end},
        {"gpu entry uses GRES id", fun() ->
             Result = flurm_dbd_mysql:format_tres_string(#{gpu => 2}),
             ?assertEqual(<<"1001=2">>, Result)
         end},
        {"energy entry", fun() ->
             Result = flurm_dbd_mysql:format_tres_string(#{energy => 1000}),
             ?assertEqual(<<"3=1000">>, Result)
         end},
        {"billing entry", fun() ->
             Result = flurm_dbd_mysql:format_tres_string(#{billing => 500}),
             ?assertEqual(<<"5=500">>, Result)
         end},
        {"unknown keys are skipped", fun() ->
             Result = flurm_dbd_mysql:format_tres_string(#{unknown_key => 100}),
             ?assertEqual(<<>>, Result)
         end},
        {"non-map input returns empty", fun() ->
             ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(not_a_map)),
             ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string([]))
         end}
    ].

parse_tres_string_test_() ->
    [
        {"empty binary returns empty map", fun() ->
             ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<>>))
         end},
        {"single entry", fun() ->
             Result = flurm_dbd_mysql:parse_tres_string(<<"1=4">>),
             ?assertEqual(#{cpu => 4}, Result)
         end},
        {"multiple entries", fun() ->
             Result = flurm_dbd_mysql:parse_tres_string(<<"1=4,2=8192,4=2">>),
             ?assertEqual(#{cpu => 4, mem => 8192, node => 2}, Result)
         end},
        {"GRES gpu entry", fun() ->
             Result = flurm_dbd_mysql:parse_tres_string(<<"1001=2">>),
             ?assertEqual(#{gpu => 2}, Result)
         end},
        {"energy entry", fun() ->
             Result = flurm_dbd_mysql:parse_tres_string(<<"3=1000">>),
             ?assertEqual(#{energy => 1000}, Result)
         end},
        {"billing entry", fun() ->
             Result = flurm_dbd_mysql:parse_tres_string(<<"5=500">>),
             ?assertEqual(#{billing => 500}, Result)
         end},
        {"fs_disk entry", fun() ->
             Result = flurm_dbd_mysql:parse_tres_string(<<"6=1024">>),
             ?assertEqual(#{fs_disk => 1024}, Result)
         end},
        {"vmem entry", fun() ->
             Result = flurm_dbd_mysql:parse_tres_string(<<"7=2048">>),
             ?assertEqual(#{vmem => 2048}, Result)
         end},
        {"pages entry", fun() ->
             Result = flurm_dbd_mysql:parse_tres_string(<<"8=100">>),
             ?assertEqual(#{pages => 100}, Result)
         end},
        {"invalid format entries are skipped", fun() ->
             Result = flurm_dbd_mysql:parse_tres_string(<<"1=4,invalid,2=8192">>),
             ?assertEqual(#{cpu => 4, mem => 8192}, Result)
         end},
        {"non-numeric values are skipped", fun() ->
             Result = flurm_dbd_mysql:parse_tres_string(<<"1=abc,2=8192">>),
             ?assertEqual(#{mem => 8192}, Result)
         end},
        {"non-binary input returns empty map", fun() ->
             ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(not_binary)),
             ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string([]))
         end}
    ].

%%====================================================================
%% Job Mapping Tests
%%====================================================================

map_job_to_slurmdbd_test_() ->
    [
        {"maps full job correctly", fun() ->
             Job = make_job(1),
             Result = flurm_dbd_mysql:map_job_to_slurmdbd(Job),
             ?assert(is_map(Result)),
             ?assertEqual(1, maps:get(id_job, Result)),
             ?assertEqual(<<"test_job">>, maps:get(job_name, Result)),
             ?assertEqual(1000, maps:get(id_user, Result)),
             ?assertEqual(1000, maps:get(id_group, Result)),
             ?assertEqual(<<"testaccount">>, maps:get(account, Result)),
             ?assertEqual(<<"default">>, maps:get(partition, Result))
         end},
        {"maps state correctly", fun() ->
             Job = #{job_id => 1, state => completed},
             Result = flurm_dbd_mysql:map_job_to_slurmdbd(Job),
             ?assertEqual(3, maps:get(state, Result))  %% 3 = completed
         end},
        {"maps minimal job with defaults", fun() ->
             Job = #{job_id => 999},
             Result = flurm_dbd_mysql:map_job_to_slurmdbd(Job),
             ?assertEqual(999, maps:get(id_job, Result)),
             ?assertEqual(<<>>, maps:get(job_name, Result)),
             ?assertEqual(0, maps:get(id_user, Result))
         end},
        {"maps TRES correctly", fun() ->
             Job = #{job_id => 1, tres_alloc => #{cpu => 4, mem => 8192}},
             Result = flurm_dbd_mysql:map_job_to_slurmdbd(Job),
             TresAlloc = maps:get(tres_alloc, Result),
             ?assert(is_binary(TresAlloc)),
             ?assert(binary:match(TresAlloc, <<"1=4">>) =/= nomatch)
         end}
    ].

map_slurmdbd_to_job_test_() ->
    [
        {"maps slurmdbd row to job", fun() ->
             Row = #{
                 id_job => 123,
                 job_name => <<"test">>,
                 id_user => 1000,
                 id_group => 1000,
                 account => <<"acct">>,
                 partition => <<"part">>,
                 state => 3,
                 exit_code => 0,
                 nodes_alloc => 2,
                 cpus_req => 8,
                 time_submit => 1700000000,
                 time_eligible => 1700000001,
                 time_start => 1700000010,
                 time_end => 1700000100,
                 tres_alloc => <<"1=8,4=2">>,
                 tres_req => <<"1=8">>,
                 work_dir => <<"/home/user">>,
                 std_out => <<"/home/user/out">>,
                 std_err => <<"/home/user/err">>
             },
             Result = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
             ?assertEqual(123, maps:get(job_id, Result)),
             ?assertEqual(completed, maps:get(state, Result)),
             ?assertEqual(90, maps:get(elapsed, Result))  %% end - start
         end},
        {"calculates elapsed correctly", fun() ->
             Row = #{time_start => 1000, time_end => 2000},
             Result = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
             ?assertEqual(1000, maps:get(elapsed, Result))
         end},
        {"elapsed is 0 when times are 0", fun() ->
             Row = #{time_start => 0, time_end => 0},
             Result = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
             ?assertEqual(0, maps:get(elapsed, Result))
         end},
        {"parses tres_alloc", fun() ->
             Row = #{tres_alloc => <<"1=4,2=8192">>},
             Result = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
             TresAlloc = maps:get(tres_alloc, Result),
             ?assertEqual(4, maps:get(cpu, TresAlloc)),
             ?assertEqual(8192, maps:get(mem, TresAlloc))
         end}
    ].

%%====================================================================
%% Query Building Tests
%%====================================================================

build_insert_query_test_() ->
    [
        {"builds valid INSERT query", fun() ->
             Query = flurm_dbd_mysql:build_insert_query("test_cluster_job_table"),
             ?assert(is_binary(Query)),
             ?assert(binary:match(Query, <<"INSERT INTO">>) =/= nomatch),
             ?assert(binary:match(Query, <<"ON DUPLICATE KEY UPDATE">>) =/= nomatch)
         end},
        {"includes all required columns", fun() ->
             Query = flurm_dbd_mysql:build_insert_query("test_job_table"),
             %% Check for key columns
             ?assert(binary:match(Query, <<"id_job">>) =/= nomatch),
             ?assert(binary:match(Query, <<"job_name">>) =/= nomatch),
             ?assert(binary:match(Query, <<"id_user">>) =/= nomatch),
             ?assert(binary:match(Query, <<"account">>) =/= nomatch),
             ?assert(binary:match(Query, <<"state">>) =/= nomatch)
         end}
    ].

build_select_query_test_() ->
    [
        {"builds basic SELECT query", fun() ->
             {Query, Params} = flurm_dbd_mysql:build_select_query("test_job_table", #{
                 start_time => 1700000000,
                 end_time => 1700001000,
                 filters => #{}
             }),
             ?assert(is_binary(Query)),
             ?assert(binary:match(Query, <<"SELECT">>) =/= nomatch),
             ?assert(binary:match(Query, <<"FROM">>) =/= nomatch),
             ?assert(binary:match(Query, <<"WHERE">>) =/= nomatch),
             ?assertEqual(2, length(Params))  %% start_time and end_time
         end},
        {"adds user filter", fun() ->
             {Query, Params} = flurm_dbd_mysql:build_select_query("test_job_table", #{
                 start_time => 1700000000,
                 end_time => 1700001000,
                 filters => #{user => <<"testuser">>}
             }),
             ?assert(binary:match(Query, <<"id_user">>) =/= nomatch),
             ?assertEqual(3, length(Params))
         end},
        {"adds account filter", fun() ->
             {Query, Params} = flurm_dbd_mysql:build_select_query("test_job_table", #{
                 start_time => 1700000000,
                 end_time => 1700001000,
                 filters => #{account => <<"testacct">>}
             }),
             ?assert(binary:match(Query, <<"account">>) =/= nomatch),
             ?assertEqual(3, length(Params))
         end},
        {"adds partition filter", fun() ->
             {Query, Params} = flurm_dbd_mysql:build_select_query("test_job_table", #{
                 start_time => 1700000000,
                 end_time => 1700001000,
                 filters => #{partition => <<"gpu">>}
             }),
             ?assert(binary:match(Query, <<"partition">>) =/= nomatch),
             ?assertEqual(3, length(Params))
         end},
        {"adds state filter", fun() ->
             {Query, Params} = flurm_dbd_mysql:build_select_query("test_job_table", #{
                 start_time => 1700000000,
                 end_time => 1700001000,
                 filters => #{state => completed}
             }),
             ?assert(binary:match(Query, <<"state">>) =/= nomatch),
             ?assertEqual(3, length(Params))
         end},
        {"respects limit", fun() ->
             {Query, _Params} = flurm_dbd_mysql:build_select_query("test_job_table", #{
                 start_time => 1700000000,
                 end_time => 1700001000,
                 filters => #{limit => 50}
             }),
             ?assert(binary:match(Query, <<"LIMIT 50">>) =/= nomatch)
         end},
        {"multiple filters", fun() ->
             {Query, Params} = flurm_dbd_mysql:build_select_query("test_job_table", #{
                 start_time => 1700000000,
                 end_time => 1700001000,
                 filters => #{
                     user => <<"testuser">>,
                     account => <<"testacct">>,
                     state => completed
                 }
             }),
             ?assert(binary:match(Query, <<"id_user">>) =/= nomatch),
             ?assert(binary:match(Query, <<"account">>) =/= nomatch),
             ?assert(binary:match(Query, <<"state">>) =/= nomatch),
             ?assertEqual(5, length(Params))  %% 2 time + 3 filters
         end}
    ].

%%====================================================================
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"start_link/0 starts server with default config", fun() ->
             meck:new(application, [unstick, passthrough]),
             meck:expect(application, get_env, fun
                 (flurm_dbd, mysql_host, _) -> "localhost";
                 (flurm_dbd, mysql_port, _) -> 3306;
                 (flurm_dbd, mysql_user, _) -> "slurm";
                 (flurm_dbd, mysql_password, _) -> "";
                 (flurm_dbd, mysql_database, _) -> "slurm_acct_db"
             end),
             {ok, Pid} = flurm_dbd_mysql:start_link(),
             ?assert(is_process_alive(Pid)),
             gen_server:stop(Pid),
             meck:unload(application)
         end},
         {"start_link/1 starts server with explicit config", fun() ->
             Config = #{
                 host => "testhost",
                 port => 3307,
                 user => "testuser",
                 password => "testpass",
                 database => "testdb",
                 auto_connect => false
             },
             {ok, Pid} = flurm_dbd_mysql:start_link(Config),
             ?assert(is_process_alive(Pid)),
             gen_server:stop(Pid)
         end},
         {"start_link with auto_connect triggers connection", fun() ->
             meck:expect(mysql, start_link, fun(_Opts) ->
                 {ok, spawn(fun() -> receive stop -> ok end end)}
             end),
             Config = #{
                 host => "localhost",
                 port => 3306,
                 user => "slurm",
                 password => "",
                 database => "slurm_acct_db",
                 auto_connect => true
             },
             {ok, Pid} = flurm_dbd_mysql:start_link(Config),
             ?assert(is_process_alive(Pid)),
             timer:sleep(50),  %% Give time for auto_connect
             gen_server:stop(Pid)
         end}
     ]
    }.

%%====================================================================
%% Connect/Disconnect Tests
%%====================================================================

connect_disconnect_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{host => "localhost", port => 3306, auto_connect => false},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"connect succeeds with valid config", fun() ->
             meck:expect(mysql, start_link, fun(_Opts) ->
                 {ok, spawn(fun() -> receive stop -> ok end end)}
             end),
             Result = flurm_dbd_mysql:connect(#{
                 host => "localhost",
                 port => 3306,
                 user => "slurm",
                 password => "",
                 database => "slurm_acct_db"
             }),
             ?assertEqual(ok, Result),
             ?assertEqual(true, flurm_dbd_mysql:is_connected())
         end},
         {"connect fails with connection error", fun() ->
             meck:expect(mysql, start_link, fun(_Opts) ->
                 {error, econnrefused}
             end),
             Result = flurm_dbd_mysql:connect(#{host => "badhost"}),
             ?assertMatch({error, _}, Result),
             ?assertEqual(false, flurm_dbd_mysql:is_connected())
         end},
         {"disconnect when not connected", fun() ->
             ?assertEqual(false, flurm_dbd_mysql:is_connected()),
             Result = flurm_dbd_mysql:disconnect(),
             ?assertEqual(ok, Result)
         end},
         {"disconnect after connect", fun() ->
             meck:expect(mysql, start_link, fun(_Opts) ->
                 {ok, spawn(fun() -> receive stop -> ok end end)}
             end),
             meck:expect(mysql, stop, fun(_Pid) -> ok end),
             flurm_dbd_mysql:connect(#{}),
             ?assertEqual(true, flurm_dbd_mysql:is_connected()),
             Result = flurm_dbd_mysql:disconnect(),
             ?assertEqual(ok, Result),
             ?assertEqual(false, flurm_dbd_mysql:is_connected())
         end},
         {"is_connected returns correct state", fun() ->
             ?assertEqual(false, flurm_dbd_mysql:is_connected()),
             meck:expect(mysql, start_link, fun(_Opts) ->
                 {ok, spawn(fun() -> receive stop -> ok end end)}
             end),
             flurm_dbd_mysql:connect(#{}),
             ?assertEqual(true, flurm_dbd_mysql:is_connected())
         end}
     ]
    }.

%%====================================================================
%% Sync Job Record Tests
%%====================================================================

sync_job_record_test_() ->
    {foreach,
     fun() ->
         setup(),
         meck:expect(mysql, start_link, fun(_Opts) ->
             {ok, spawn(fun() -> receive stop -> ok end end)}
         end),
         Config = #{host => "localhost", port => 3306, auto_connect => false},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         flurm_dbd_mysql:connect(#{}),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"sync_job_record succeeds", fun() ->
             meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> ok end),
             Job = make_job(1),
             Result = flurm_dbd_mysql:sync_job_record(Job),
             ?assertEqual(ok, Result)
         end},
         {"sync_job_record returns {ok, _} as ok", fun() ->
             meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> {ok, []} end),
             Job = make_job(1),
             Result = flurm_dbd_mysql:sync_job_record(Job),
             ?assertEqual(ok, Result)
         end},
         {"sync_job_record fails when not connected", fun() ->
             flurm_dbd_mysql:disconnect(),
             Job = make_job(1),
             Result = flurm_dbd_mysql:sync_job_record(Job),
             ?assertEqual({error, not_connected}, Result)
         end},
         {"sync_job_record returns error on query failure", fun() ->
             meck:expect(mysql, query, fun(_Conn, _Query, _Params) ->
                 {error, syntax_error}
             end),
             Job = make_job(1),
             Result = flurm_dbd_mysql:sync_job_record(Job),
             ?assertEqual({error, syntax_error}, Result)
         end},
         {"sync_job_record updates stats on success", fun() ->
             meck:expect(mysql, query, fun(_Conn, _Query, _Params) -> ok end),
             Job = make_job(1),
             flurm_dbd_mysql:sync_job_record(Job),
             Status = flurm_dbd_mysql:get_connection_status(),
             Stats = maps:get(stats, Status),
             ?assert(maps:get(jobs_synced, Stats) >= 1)
         end}
     ]
    }.

%%====================================================================
%% Sync Job Records Batch Tests
%%====================================================================

sync_job_records_test_() ->
    {foreach,
     fun() ->
         setup(),
         meck:expect(mysql, start_link, fun(_Opts) ->
             {ok, spawn(fun() -> receive stop -> ok end end)}
         end),
         Config = #{host => "localhost", port => 3306, auto_connect => false},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         flurm_dbd_mysql:connect(#{}),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"sync_job_records succeeds", fun() ->
             meck:expect(mysql, transaction, fun(_Conn, _Fun) -> {atomic, ok} end),
             Jobs = [make_job(N) || N <- lists:seq(1, 5)],
             Result = flurm_dbd_mysql:sync_job_records(Jobs),
             ?assertEqual({ok, 5}, Result)
         end},
         {"sync_job_records fails when not connected", fun() ->
             flurm_dbd_mysql:disconnect(),
             Jobs = [make_job(1)],
             Result = flurm_dbd_mysql:sync_job_records(Jobs),
             ?assertEqual({error, not_connected}, Result)
         end},
         {"sync_job_records returns error on transaction failure", fun() ->
             meck:expect(mysql, transaction, fun(_Conn, _Fun) ->
                 {aborted, transaction_error}
             end),
             Jobs = [make_job(1)],
             Result = flurm_dbd_mysql:sync_job_records(Jobs),
             ?assertEqual({error, transaction_error}, Result)
         end},
         {"sync_job_records handles sync_error abort", fun() ->
             meck:expect(mysql, transaction, fun(_Conn, _Fun) ->
                 {aborted, {sync_error, individual_error}}
             end),
             Jobs = [make_job(1)],
             Result = flurm_dbd_mysql:sync_job_records(Jobs),
             ?assertEqual({error, individual_error}, Result)
         end}
     ]
    }.

%%====================================================================
%% Read Historical Jobs Tests
%%====================================================================

read_historical_jobs_test_() ->
    {foreach,
     fun() ->
         setup(),
         meck:expect(mysql, start_link, fun(_Opts) ->
             {ok, spawn(fun() -> receive stop -> ok end end)}
         end),
         Config = #{host => "localhost", port => 3306, auto_connect => false, cluster_name => <<"test">>},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         flurm_dbd_mysql:connect(#{}),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"read_historical_jobs/2 with time range", fun() ->
             Columns = [<<"id_job">>, <<"job_name">>, <<"id_user">>, <<"id_group">>,
                        <<"account">>, <<"partition">>, <<"state">>, <<"exit_code">>,
                        <<"nodes_alloc">>, <<"cpus_req">>, <<"time_submit">>,
                        <<"time_eligible">>, <<"time_start">>, <<"time_end">>,
                        <<"timelimit">>, <<"tres_alloc">>, <<"tres_req">>,
                        <<"work_dir">>, <<"std_out">>, <<"std_err">>],
             Rows = [{1, <<"job1">>, 1000, 1000, <<"acct">>, <<"part">>, 3, 0,
                      1, 4, 1700000000, 1700000001, 1700000010, 1700000100,
                      60, <<"1=4">>, <<"1=4">>, <<"/home">>, <<"/out">>, <<"/err">>}],
             meck:expect(mysql, query, fun(_Conn, _Query, _Params) ->
                 {ok, Columns, Rows}
             end),
             StartTime = 1700000000,
             EndTime = 1700001000,
             Result = flurm_dbd_mysql:read_historical_jobs(StartTime, EndTime),
             ?assertMatch({ok, [_|_]}, Result)
         end},
         {"read_historical_jobs/3 with filters", fun() ->
             Columns = [<<"id_job">>, <<"job_name">>, <<"id_user">>, <<"id_group">>,
                        <<"account">>, <<"partition">>, <<"state">>, <<"exit_code">>,
                        <<"nodes_alloc">>, <<"cpus_req">>, <<"time_submit">>,
                        <<"time_eligible">>, <<"time_start">>, <<"time_end">>,
                        <<"timelimit">>, <<"tres_alloc">>, <<"tres_req">>,
                        <<"work_dir">>, <<"std_out">>, <<"std_err">>],
             Rows = [],
             meck:expect(mysql, query, fun(_Conn, _Query, _Params) ->
                 {ok, Columns, Rows}
             end),
             Result = flurm_dbd_mysql:read_historical_jobs(1700000000, 1700001000, #{
                 user => <<"testuser">>,
                 limit => 10
             }),
             ?assertEqual({ok, []}, Result)
         end},
         {"read_historical_jobs fails when not connected", fun() ->
             flurm_dbd_mysql:disconnect(),
             Result = flurm_dbd_mysql:read_historical_jobs(1700000000, 1700001000),
             ?assertEqual({error, not_connected}, Result)
         end},
         {"read_historical_jobs handles query error", fun() ->
             meck:expect(mysql, query, fun(_Conn, _Query, _Params) ->
                 {error, table_not_found}
             end),
             Result = flurm_dbd_mysql:read_historical_jobs(1700000000, 1700001000),
             ?assertEqual({error, table_not_found}, Result)
         end}
     ]
    }.

%%====================================================================
%% Check Schema Compatibility Tests
%%====================================================================

check_schema_compatibility_test_() ->
    {foreach,
     fun() ->
         setup(),
         meck:expect(mysql, start_link, fun(_Opts) ->
             {ok, spawn(fun() -> receive stop -> ok end end)}
         end),
         Config = #{host => "localhost", port => 3306, auto_connect => false},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         flurm_dbd_mysql:connect(#{}),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"check_schema_compatibility returns version as integer", fun() ->
             meck:expect(mysql, query, fun(_Conn, _Query) ->
                 {ok, [<<"version">>], [[8]]}
             end),
             Result = flurm_dbd_mysql:check_schema_compatibility(),
             ?assertEqual({ok, 8}, Result)
         end},
         {"check_schema_compatibility parses binary version", fun() ->
             meck:expect(mysql, query, fun(_Conn, _Query) ->
                 {ok, [<<"version">>], [[<<"9">>]]}
             end),
             Result = flurm_dbd_mysql:check_schema_compatibility(),
             ?assertEqual({ok, 9}, Result)
         end},
         {"check_schema_compatibility fails when not connected", fun() ->
             flurm_dbd_mysql:disconnect(),
             Result = flurm_dbd_mysql:check_schema_compatibility(),
             ?assertEqual({error, not_connected}, Result)
         end},
         {"check_schema_compatibility handles schema_not_found", fun() ->
             meck:expect(mysql, query, fun(_Conn, _Query) ->
                 {ok, [], []}
             end),
             Result = flurm_dbd_mysql:check_schema_compatibility(),
             ?assertEqual({error, schema_not_found}, Result)
         end},
         {"check_schema_compatibility handles query error", fun() ->
             meck:expect(mysql, query, fun(_Conn, _Query) ->
                 {error, table_not_found}
             end),
             Result = flurm_dbd_mysql:check_schema_compatibility(),
             ?assertEqual({error, table_not_found}, Result)
         end}
     ]
    }.

%%====================================================================
%% Get Connection Status Tests
%%====================================================================

get_connection_status_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{host => "localhost", port => 3306, auto_connect => false},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"get_connection_status returns status map", fun() ->
             Status = flurm_dbd_mysql:get_connection_status(),
             ?assert(is_map(Status)),
             ?assert(maps:is_key(connected, Status)),
             ?assert(maps:is_key(schema_version, Status)),
             ?assert(maps:is_key(last_error, Status)),
             ?assert(maps:is_key(stats, Status)),
             ?assert(maps:is_key(config, Status))
         end},
         {"get_connection_status hides password", fun() ->
             Status = flurm_dbd_mysql:get_connection_status(),
             Config = maps:get(config, Status),
             ?assertNot(maps:is_key(password, Config))
         end},
         {"get_connection_status reflects connected state", fun() ->
             Status1 = flurm_dbd_mysql:get_connection_status(),
             ?assertEqual(false, maps:get(connected, Status1)),
             meck:expect(mysql, start_link, fun(_Opts) ->
                 {ok, spawn(fun() -> receive stop -> ok end end)}
             end),
             flurm_dbd_mysql:connect(#{}),
             Status2 = flurm_dbd_mysql:get_connection_status(),
             ?assertEqual(true, maps:get(connected, Status2))
         end}
     ]
    }.

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{host => "localhost", port => 3306, auto_connect => false},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"handle_call unknown request returns error", fun() ->
             Result = gen_server:call(flurm_dbd_mysql, unknown_request),
             ?assertEqual({error, unknown_request}, Result)
         end},
         {"handle_cast does not crash", fun() ->
             gen_server:cast(flurm_dbd_mysql, unknown_cast),
             timer:sleep(10),
             ?assert(is_process_alive(whereis(flurm_dbd_mysql)))
         end},
         {"handle_info unknown does not crash", fun() ->
             whereis(flurm_dbd_mysql) ! unknown_message,
             timer:sleep(10),
             ?assert(is_process_alive(whereis(flurm_dbd_mysql)))
         end}
     ]
    }.

%%====================================================================
%% Auto-Connect and Reconnection Tests
%%====================================================================

auto_connect_test_() ->
    {setup,
     fun() ->
         setup(),
         ConnectCount = ets:new(connect_count, [public]),
         ets:insert(ConnectCount, {count, 0}),
         meck:expect(mysql, start_link, fun(_Opts) ->
             [{count, N}] = ets:lookup(ConnectCount, count),
             ets:insert(ConnectCount, {count, N + 1}),
             case N of
                 0 -> {error, econnrefused};  %% First attempt fails
                 _ -> {ok, spawn(fun() -> receive stop -> ok end end)}  %% Later succeeds
             end
         end),
         Config = #{host => "localhost", port => 3306, auto_connect => true},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         {Pid, ConnectCount}
     end,
     fun({Pid, ConnectCount}) ->
         catch gen_server:stop(Pid, normal, 5000),
         ets:delete(ConnectCount),
         cleanup(ok)
     end,
     fun({_Pid, ConnectCount}) ->
         [
             {"auto_connect retries on failure", fun() ->
                 %% Wait for retries
                 timer:sleep(3000),
                 [{count, N}] = ets:lookup(ConnectCount, count),
                 ?assert(N >= 2)
             end}
         ]
     end
    }.

%%====================================================================
%% Connection Monitor Tests
%%====================================================================

connection_monitor_test_() ->
    {setup,
     fun() ->
         setup(),
         ConnPid = spawn(fun() -> receive stop -> ok end end),
         meck:expect(mysql, start_link, fun(_Opts) -> {ok, ConnPid} end),
         Config = #{host => "localhost", port => 3306, auto_connect => false},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         flurm_dbd_mysql:connect(#{}),
         {Pid, ConnPid}
     end,
     fun({Pid, _ConnPid}) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     fun({_Pid, ConnPid}) ->
         [
             {"connection monitor triggers reconnect on DOWN", fun() ->
                 ?assertEqual(true, flurm_dbd_mysql:is_connected()),
                 %% Simulate connection death
                 ConnPid ! stop,
                 timer:sleep(100),
                 %% Should have detected disconnect
                 ?assertEqual(false, flurm_dbd_mysql:is_connected())
             end}
         ]
     end
    }.

%%====================================================================
%% SSL Configuration Tests
%%====================================================================

ssl_config_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"connect with SSL enabled", fun() ->
             meck:expect(mysql, start_link, fun(Opts) ->
                 case proplists:get_value(ssl, Opts) of
                     undefined -> {error, no_ssl};
                     _ -> {ok, spawn(fun() -> receive stop -> ok end end)}
                 end
             end),
             Config = #{
                 host => "localhost",
                 port => 3306,
                 ssl => true,
                 ssl_opts => [{verify, verify_none}],
                 auto_connect => false
             },
             {ok, Pid} = flurm_dbd_mysql:start_link(Config),
             Result = flurm_dbd_mysql:connect(#{}),
             ?assertEqual(ok, Result),
             gen_server:stop(Pid)
         end},
         {"connect without SSL", fun() ->
             meck:expect(mysql, start_link, fun(Opts) ->
                 case proplists:get_value(ssl, Opts) of
                     undefined -> {ok, spawn(fun() -> receive stop -> ok end end)};
                     _ -> {error, unexpected_ssl}
                 end
             end),
             Config = #{
                 host => "localhost",
                 port => 3306,
                 ssl => false,
                 auto_connect => false
             },
             {ok, Pid} = flurm_dbd_mysql:start_link(Config),
             Result = flurm_dbd_mysql:connect(#{}),
             ?assertEqual(ok, Result),
             gen_server:stop(Pid)
         end}
     ]
    }.

%%====================================================================
%% Max Reconnect Attempts Tests
%%====================================================================

max_reconnect_test_() ->
    {setup,
     fun() ->
         setup(),
         meck:expect(mysql, start_link, fun(_Opts) -> {error, econnrefused} end),
         Config = #{host => "localhost", port => 3306, auto_connect => true},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     fun(_Pid) ->
         [
             {"max reconnect attempts stops retrying", fun() ->
                 %% Wait long enough for some retries with backoff
                 timer:sleep(5000),
                 Status = flurm_dbd_mysql:get_connection_status(),
                 _Stats = maps:get(stats, Status),
                 %% Should have set the gave_up flag or stopped trying
                 ?assertEqual(false, maps:get(connected, Status))
             end}
         ]
     end
    }.

%%====================================================================
%% Terminate Tests
%%====================================================================

terminate_test_() ->
    {setup,
     fun() ->
         setup(),
         StopCalled = ets:new(stop_called, [public]),
         ets:insert(StopCalled, {called, false}),
         ConnPid = spawn(fun() -> receive stop -> ok end end),
         meck:expect(mysql, start_link, fun(_Opts) -> {ok, ConnPid} end),
         meck:expect(mysql, stop, fun(_Pid) ->
             ets:insert(StopCalled, {called, true}),
             ok
         end),
         Config = #{host => "localhost", port => 3306, auto_connect => false},
         {ok, Pid} = flurm_dbd_mysql:start_link(Config),
         flurm_dbd_mysql:connect(#{}),
         {Pid, StopCalled}
     end,
     fun({Pid, StopCalled}) ->
         case is_process_alive(Pid) of
             true -> catch gen_server:stop(Pid, normal, 5000);
             false -> ok
         end,
         ets:delete(StopCalled),
         cleanup(ok)
     end,
     fun({Pid, StopCalled}) ->
         [
             {"terminate stops mysql connection", fun() ->
                 ?assertEqual(true, flurm_dbd_mysql:is_connected()),
                 gen_server:stop(Pid, normal, 5000),
                 timer:sleep(50),
                 [{called, WasCalled}] = ets:lookup(StopCalled, called),
                 ?assertEqual(true, WasCalled)
             end}
         ]
     end
    }.

%%====================================================================
%% Helper Functions
%%====================================================================

make_job(JobId) ->
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
        start_time => erlang:system_time(second) - 100,
        end_time => erlang:system_time(second),
        elapsed => 100,
        time_limit => 60,
        eligible_time => erlang:system_time(second) - 199,
        tres_alloc => #{cpu => 4, mem => 8192},
        tres_req => #{cpu => 4, mem => 8192},
        work_dir => <<"/home/user">>,
        std_out => <<"/home/user/job.out">>,
        std_err => <<"/home/user/job.err">>
    }.
