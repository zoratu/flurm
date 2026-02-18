%%%-------------------------------------------------------------------
%%% @doc Comprehensive coverage tests for flurm_dbd_mysql module.
%%%
%%% Tests all exported functions including:
%%% - Job record mapping (FLURM <-> slurmdbd)
%%% - State conversions
%%% - TRES string formatting and parsing
%%% - SQL query generation
%%% - Gen_server API with mocked MySQL connection
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_mysql_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Pure function tests (TEST exports)
%%====================================================================

pure_functions_test_() ->
    {setup,
     fun setup_lager/0,
     fun cleanup_lager/1,
     [
      %% map_job_to_slurmdbd
      {"map_job_to_slurmdbd full job", fun test_map_job_full/0},
      {"map_job_to_slurmdbd minimal", fun test_map_job_minimal/0},
      {"map_job_to_slurmdbd with tres", fun test_map_job_with_tres/0},
      {"map_job_to_slurmdbd all states", fun test_map_job_all_states/0},

      %% map_slurmdbd_to_job
      {"map_slurmdbd_to_job full", fun test_map_slurmdbd_full/0},
      {"map_slurmdbd_to_job minimal", fun test_map_slurmdbd_minimal/0},
      {"map_slurmdbd_to_job elapsed calc", fun test_map_slurmdbd_elapsed/0},
      {"map_slurmdbd_to_job elapsed zero start", fun test_map_slurmdbd_zero_start/0},
      {"map_slurmdbd_to_job elapsed zero end", fun test_map_slurmdbd_zero_end/0},

      %% state_to_slurmdbd
      {"state_to_slurmdbd pending", fun test_state_to_pending/0},
      {"state_to_slurmdbd running", fun test_state_to_running/0},
      {"state_to_slurmdbd suspended", fun test_state_to_suspended/0},
      {"state_to_slurmdbd completed", fun test_state_to_completed/0},
      {"state_to_slurmdbd cancelled", fun test_state_to_cancelled/0},
      {"state_to_slurmdbd failed", fun test_state_to_failed/0},
      {"state_to_slurmdbd timeout", fun test_state_to_timeout/0},
      {"state_to_slurmdbd node_fail", fun test_state_to_node_fail/0},
      {"state_to_slurmdbd preempted", fun test_state_to_preempted/0},
      {"state_to_slurmdbd boot_fail", fun test_state_to_boot_fail/0},
      {"state_to_slurmdbd deadline", fun test_state_to_deadline/0},
      {"state_to_slurmdbd oom", fun test_state_to_oom/0},
      {"state_to_slurmdbd unknown", fun test_state_to_unknown/0},

      %% slurmdbd_to_state
      {"slurmdbd_to_state 0-11", fun test_slurmdbd_to_state_all/0},
      {"slurmdbd_to_state unknown", fun test_slurmdbd_to_state_unknown/0},

      %% format_tres_string
      {"format_tres_string empty map", fun test_format_tres_empty/0},
      {"format_tres_string single value", fun test_format_tres_single/0},
      {"format_tres_string multiple values", fun test_format_tres_multiple/0},
      {"format_tres_string all types", fun test_format_tres_all_types/0},
      {"format_tres_string unknown key", fun test_format_tres_unknown_key/0},
      {"format_tres_string non-map", fun test_format_tres_non_map/0},
      {"format_tres_string zero-size map", fun test_format_tres_zero_size/0},

      %% parse_tres_string
      {"parse_tres_string empty", fun test_parse_tres_empty/0},
      {"parse_tres_string single", fun test_parse_tres_single/0},
      {"parse_tres_string multiple", fun test_parse_tres_multiple/0},
      {"parse_tres_string all ids", fun test_parse_tres_all_ids/0},
      {"parse_tres_string gpu", fun test_parse_tres_gpu/0},
      {"parse_tres_string malformed", fun test_parse_tres_malformed/0},
      {"parse_tres_string bad int", fun test_parse_tres_bad_int/0},
      {"parse_tres_string non-binary", fun test_parse_tres_non_binary/0},
      {"parse_tres_string unknown id", fun test_parse_tres_unknown_id/0},

      %% build_insert_query
      {"build_insert_query format", fun test_build_insert_format/0},
      {"build_insert_query upsert", fun test_build_insert_upsert/0},

      %% build_select_query
      {"build_select_query basic", fun test_build_select_basic/0},
      {"build_select_query with filters", fun test_build_select_filters/0},
      {"build_select_query user filter", fun test_build_select_user/0},
      {"build_select_query account filter", fun test_build_select_account/0},
      {"build_select_query partition filter", fun test_build_select_partition/0},
      {"build_select_query state filter", fun test_build_select_state/0},
      {"build_select_query limit", fun test_build_select_limit/0}
     ]}.

setup_lager() ->
    catch meck:unload(lager),
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

%% --- map_job_to_slurmdbd ---

test_map_job_full() ->
    Job = #{
        job_id => 42, job_name => <<"myjob">>, user_id => 1000, group_id => 100,
        account => <<"myacct">>, partition => <<"batch">>, state => running,
        exit_code => 0, num_nodes => 4, num_cpus => 16,
        submit_time => 1000, eligible_time => 1001, start_time => 1010, end_time => 1100,
        time_limit => 3600, tres_alloc => #{cpu => 16}, tres_req => #{cpu => 16},
        work_dir => <<"/home/user">>, std_out => <<"out.log">>, std_err => <<"err.log">>
    },
    R = flurm_dbd_mysql:map_job_to_slurmdbd(Job),
    ?assertEqual(42, maps:get(id_job, R)),
    ?assertEqual(<<"myjob">>, maps:get(job_name, R)),
    ?assertEqual(1, maps:get(state, R)).

test_map_job_minimal() ->
    R = flurm_dbd_mysql:map_job_to_slurmdbd(#{}),
    ?assertEqual(0, maps:get(id_job, R)),
    ?assertEqual(<<>>, maps:get(job_name, R)),
    ?assertEqual(0, maps:get(state, R)).

test_map_job_with_tres() ->
    Job = #{job_id => 1, tres_alloc => #{cpu => 8, mem => 4096, node => 2, gpu => 1}},
    R = flurm_dbd_mysql:map_job_to_slurmdbd(Job),
    TresStr = maps:get(tres_alloc, R),
    ?assert(is_binary(TresStr)).

test_map_job_all_states() ->
    States = [pending, running, suspended, completed, cancelled, failed, timeout,
              node_fail, preempted, boot_fail, deadline, oom, unknown_state],
    lists:foreach(fun(S) ->
        R = flurm_dbd_mysql:map_job_to_slurmdbd(#{job_id => 1, state => S}),
        ?assert(is_integer(maps:get(state, R)))
    end, States).

%% --- map_slurmdbd_to_job ---

test_map_slurmdbd_full() ->
    Row = #{
        id_job => 42, job_name => <<"test">>, id_user => 1000, id_group => 100,
        account => <<"acct">>, partition => <<"batch">>, state => 3,
        exit_code => 0, nodes_alloc => 2, cpus_req => 4,
        time_submit => 1000, time_eligible => 1001, time_start => 1010, time_end => 1100,
        tres_alloc => <<"1=4,4=2">>, tres_req => <<"1=4">>,
        work_dir => <<"/home">>, std_out => <<"out">>, std_err => <<"err">>
    },
    J = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
    ?assertEqual(42, maps:get(job_id, J)),
    ?assertEqual(completed, maps:get(state, J)),
    ?assertEqual(90, maps:get(elapsed, J)).

test_map_slurmdbd_minimal() ->
    J = flurm_dbd_mysql:map_slurmdbd_to_job(#{}),
    ?assertEqual(0, maps:get(job_id, J)),
    ?assertEqual(pending, maps:get(state, J)).

test_map_slurmdbd_elapsed() ->
    Row = #{time_start => 1000, time_end => 1500},
    J = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
    ?assertEqual(500, maps:get(elapsed, J)).

test_map_slurmdbd_zero_start() ->
    Row = #{time_start => 0, time_end => 1000},
    J = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
    ?assertEqual(0, maps:get(elapsed, J)).

test_map_slurmdbd_zero_end() ->
    Row = #{time_start => 1000, time_end => 0},
    J = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
    ?assertEqual(0, maps:get(elapsed, J)).

%% --- state_to_slurmdbd ---

test_state_to_pending() -> ?assertEqual(0, flurm_dbd_mysql:state_to_slurmdbd(pending)).
test_state_to_running() -> ?assertEqual(1, flurm_dbd_mysql:state_to_slurmdbd(running)).
test_state_to_suspended() -> ?assertEqual(2, flurm_dbd_mysql:state_to_slurmdbd(suspended)).
test_state_to_completed() -> ?assertEqual(3, flurm_dbd_mysql:state_to_slurmdbd(completed)).
test_state_to_cancelled() -> ?assertEqual(4, flurm_dbd_mysql:state_to_slurmdbd(cancelled)).
test_state_to_failed() -> ?assertEqual(5, flurm_dbd_mysql:state_to_slurmdbd(failed)).
test_state_to_timeout() -> ?assertEqual(6, flurm_dbd_mysql:state_to_slurmdbd(timeout)).
test_state_to_node_fail() -> ?assertEqual(7, flurm_dbd_mysql:state_to_slurmdbd(node_fail)).
test_state_to_preempted() -> ?assertEqual(8, flurm_dbd_mysql:state_to_slurmdbd(preempted)).
test_state_to_boot_fail() -> ?assertEqual(9, flurm_dbd_mysql:state_to_slurmdbd(boot_fail)).
test_state_to_deadline() -> ?assertEqual(10, flurm_dbd_mysql:state_to_slurmdbd(deadline)).
test_state_to_oom() -> ?assertEqual(11, flurm_dbd_mysql:state_to_slurmdbd(oom)).
test_state_to_unknown() -> ?assertEqual(0, flurm_dbd_mysql:state_to_slurmdbd(bogus_state)).

%% --- slurmdbd_to_state ---

test_slurmdbd_to_state_all() ->
    Expected = [pending, running, suspended, completed, cancelled, failed,
                timeout, node_fail, preempted, boot_fail, deadline, oom],
    lists:foreach(fun(I) ->
        Expected_State = lists:nth(I + 1, Expected),
        ?assertEqual(Expected_State, flurm_dbd_mysql:slurmdbd_to_state(I))
    end, lists:seq(0, 11)).

test_slurmdbd_to_state_unknown() ->
    ?assertEqual(unknown, flurm_dbd_mysql:slurmdbd_to_state(99)),
    ?assertEqual(unknown, flurm_dbd_mysql:slurmdbd_to_state(-1)).

%% --- format_tres_string ---

test_format_tres_empty() ->
    ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(#{})).

test_format_tres_single() ->
    R = flurm_dbd_mysql:format_tres_string(#{cpu => 4}),
    ?assertEqual(<<"1=4">>, R).

test_format_tres_multiple() ->
    R = flurm_dbd_mysql:format_tres_string(#{cpu => 4, node => 2}),
    ?assert(is_binary(R)),
    ?assertMatch({match, _}, re:run(R, "1=4")),
    ?assertMatch({match, _}, re:run(R, "4=2")).

test_format_tres_all_types() ->
    R = flurm_dbd_mysql:format_tres_string(#{
        cpu => 1, mem => 2, energy => 3, node => 4,
        billing => 5, fs_disk => 6, vmem => 7, pages => 8, gpu => 9
    }),
    ?assert(is_binary(R)),
    %% Should have entries for known types
    ?assert(byte_size(R) > 10).

test_format_tres_unknown_key() ->
    R = flurm_dbd_mysql:format_tres_string(#{unknown_type => 100}),
    ?assertEqual(<<>>, R).

test_format_tres_non_map() ->
    ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(not_a_map)),
    ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string([1, 2, 3])).

test_format_tres_zero_size() ->
    ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(#{})).

%% --- parse_tres_string ---

test_parse_tres_empty() ->
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<>>)).

test_parse_tres_single() ->
    R = flurm_dbd_mysql:parse_tres_string(<<"1=8">>),
    ?assertEqual(8, maps:get(cpu, R)).

test_parse_tres_multiple() ->
    R = flurm_dbd_mysql:parse_tres_string(<<"1=4,2=8192,4=2">>),
    ?assertEqual(4, maps:get(cpu, R)),
    ?assertEqual(8192, maps:get(mem, R)),
    ?assertEqual(2, maps:get(node, R)).

test_parse_tres_all_ids() ->
    R = flurm_dbd_mysql:parse_tres_string(<<"1=1,2=2,3=3,4=4,5=5,6=6,7=7,8=8">>),
    ?assertEqual(1, maps:get(cpu, R)),
    ?assertEqual(2, maps:get(mem, R)),
    ?assertEqual(3, maps:get(energy, R)),
    ?assertEqual(4, maps:get(node, R)),
    ?assertEqual(5, maps:get(billing, R)),
    ?assertEqual(6, maps:get(fs_disk, R)),
    ?assertEqual(7, maps:get(vmem, R)),
    ?assertEqual(8, maps:get(pages, R)).

test_parse_tres_gpu() ->
    R = flurm_dbd_mysql:parse_tres_string(<<"1001=4">>),
    ?assertEqual(4, maps:get(gpu, R)).

test_parse_tres_malformed() ->
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<"abc">>)),
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<"no_equals">>)).

test_parse_tres_bad_int() ->
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<"abc=def">>)),
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<"1=abc">>)).

test_parse_tres_non_binary() ->
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(not_binary)),
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(123)).

test_parse_tres_unknown_id() ->
    R = flurm_dbd_mysql:parse_tres_string(<<"999=100">>),
    ?assertNot(maps:is_key(999, R)).

%% --- build_insert_query ---

test_build_insert_format() ->
    Q = flurm_dbd_mysql:build_insert_query("test_job_table"),
    ?assert(is_binary(Q)),
    ?assertMatch({match, _}, re:run(Q, "INSERT INTO")),
    ?assertMatch({match, _}, re:run(Q, "test_job_table")).

test_build_insert_upsert() ->
    Q = flurm_dbd_mysql:build_insert_query("my_table"),
    ?assertMatch({match, _}, re:run(Q, "ON DUPLICATE KEY UPDATE")).

%% --- build_select_query ---

test_build_select_basic() ->
    {Q, Params} = flurm_dbd_mysql:build_select_query("tbl", #{
        start_time => 1000, end_time => 2000, filters => #{}
    }),
    ?assert(is_binary(Q)),
    ?assertMatch({match, _}, re:run(Q, "SELECT")),
    ?assertEqual(2, length(Params)).

test_build_select_filters() ->
    {Q, Params} = flurm_dbd_mysql:build_select_query("tbl", #{
        start_time => 0, end_time => 9999,
        filters => #{user => <<"alice">>, account => <<"acct">>}
    }),
    ?assertMatch({match, _}, re:run(Q, "id_user")),
    ?assertMatch({match, _}, re:run(Q, "account")),
    ?assertEqual(4, length(Params)).

test_build_select_user() ->
    {Q, Params} = flurm_dbd_mysql:build_select_query("tbl", #{
        start_time => 0, end_time => 9999, filters => #{user => <<"bob">>}
    }),
    ?assertMatch({match, _}, re:run(Q, "id_user")),
    ?assertEqual(3, length(Params)).

test_build_select_account() ->
    {Q, Params} = flurm_dbd_mysql:build_select_query("tbl", #{
        start_time => 0, end_time => 9999, filters => #{account => <<"research">>}
    }),
    ?assertMatch({match, _}, re:run(Q, "account")),
    ?assertEqual(3, length(Params)).

test_build_select_partition() ->
    {Q, Params} = flurm_dbd_mysql:build_select_query("tbl", #{
        start_time => 0, end_time => 9999, filters => #{partition => <<"batch">>}
    }),
    ?assertMatch({match, _}, re:run(Q, "`partition`")),
    ?assertEqual(3, length(Params)).

test_build_select_state() ->
    {Q, Params} = flurm_dbd_mysql:build_select_query("tbl", #{
        start_time => 0, end_time => 9999, filters => #{state => completed}
    }),
    ?assertMatch({match, _}, re:run(Q, "state")),
    ?assertEqual(3, length(Params)),
    ?assertEqual(3, lists:last(Params)).

test_build_select_limit() ->
    {Q, _Params} = flurm_dbd_mysql:build_select_query("tbl", #{
        start_time => 0, end_time => 9999, filters => #{limit => 500}
    }),
    ?assertMatch({match, _}, re:run(Q, "LIMIT 500")).

%%====================================================================
%% Gen_server tests with mocked MySQL
%%====================================================================

server_test_() ->
    {foreach,
     fun setup_server/0,
     fun cleanup_server/1,
     [
      {"init no auto_connect", fun test_init_no_auto/0},
      {"connect success", fun test_connect_success/0},
      {"connect failure", fun test_connect_failure/0},
      {"disconnect", fun test_disconnect/0},
      {"is_connected", fun test_is_connected/0},
      {"sync_job_record not connected", fun test_sync_not_connected/0},
      {"sync_job_record success", fun test_sync_success/0},
      {"sync_job_record failure", fun test_sync_failure/0},
      {"sync_job_records not connected", fun test_batch_not_connected/0},
      {"sync_job_records success", fun test_batch_success/0},
      {"sync_job_records failure", fun test_batch_failure/0},
      {"read_historical not connected", fun test_read_not_connected/0},
      {"check_schema not connected", fun test_schema_not_connected/0},
      {"check_schema success", fun test_schema_success/0},
      {"check_schema outside range", fun test_schema_outside_range/0},
      {"check_schema not found", fun test_schema_not_found/0},
      {"get_connection_status", fun test_connection_status/0},
      {"unknown call", fun test_unknown_call/0},
      {"unknown cast", fun test_unknown_cast/0},
      {"unknown info", fun test_unknown_info/0},
      {"auto_connect max attempts", fun test_auto_connect_max/0},
      {"auto_connect empty config", fun test_auto_connect_empty/0},
      {"auto_connect success", fun test_auto_connect_success/0},
      {"auto_connect retry", fun test_auto_connect_retry/0},
      {"DOWN message", fun test_down_message/0},
      {"terminate with connection", fun test_terminate_conn/0}
     ]}.

setup_server() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    meck:new(mysql, [non_strict, no_link]),
    meck:expect(mysql, start_link, fun(_) -> {ok, spawn(fun() -> receive stop -> ok end end)} end),
    meck:expect(mysql, stop, fun(_) -> ok end),
    meck:expect(mysql, query, fun(_, _) -> ok end),
    meck:expect(mysql, query, fun(_, _, _) -> ok end),
    meck:expect(mysql, transaction, fun(_, Fun) -> {atomic, Fun()} end),

    Config = #{host => "localhost", port => 3306, auto_connect => false},
    {ok, Pid} = flurm_dbd_mysql:start_link(Config),
    Pid.

cleanup_server(Pid) ->
    catch gen_server:stop(Pid),
    timer:sleep(10),
    catch meck:unload(mysql),
    catch meck:unload(lager),
    ok.

test_init_no_auto() ->
    ?assertNot(flurm_dbd_mysql:is_connected()).

test_connect_success() ->
    ?assertEqual(ok, flurm_dbd_mysql:connect(#{})),
    ?assert(flurm_dbd_mysql:is_connected()).

test_connect_failure() ->
    meck:expect(mysql, start_link, fun(_) -> {error, econnrefused} end),
    ?assertMatch({error, _}, flurm_dbd_mysql:connect(#{})).

test_disconnect() ->
    flurm_dbd_mysql:connect(#{}),
    ?assertEqual(ok, flurm_dbd_mysql:disconnect()),
    ?assertNot(flurm_dbd_mysql:is_connected()).

test_is_connected() ->
    ?assertNot(flurm_dbd_mysql:is_connected()),
    flurm_dbd_mysql:connect(#{}),
    ?assert(flurm_dbd_mysql:is_connected()).

test_sync_not_connected() ->
    ?assertEqual({error, not_connected}, flurm_dbd_mysql:sync_job_record(#{})).

test_sync_success() ->
    flurm_dbd_mysql:connect(#{}),
    ?assertEqual(ok, flurm_dbd_mysql:sync_job_record(#{job_id => 1})).

test_sync_failure() ->
    flurm_dbd_mysql:connect(#{}),
    meck:expect(mysql, query, fun(_, _, _) -> {error, timeout} end),
    ?assertMatch({error, _}, flurm_dbd_mysql:sync_job_record(#{job_id => 1})).

test_batch_not_connected() ->
    ?assertEqual({error, not_connected}, flurm_dbd_mysql:sync_job_records([#{}])).

test_batch_success() ->
    flurm_dbd_mysql:connect(#{}),
    meck:expect(mysql, query, fun(_, _, _) -> ok end),
    ?assertMatch({ok, _}, flurm_dbd_mysql:sync_job_records([#{job_id => 1}, #{job_id => 2}])).

test_batch_failure() ->
    flurm_dbd_mysql:connect(#{}),
    meck:expect(mysql, transaction, fun(_, _) -> {aborted, some_error} end),
    ?assertMatch({error, _}, flurm_dbd_mysql:sync_job_records([#{}])).

test_read_not_connected() ->
    ?assertEqual({error, not_connected}, flurm_dbd_mysql:read_historical_jobs(0, 9999)).

test_schema_not_connected() ->
    ?assertEqual({error, not_connected}, flurm_dbd_mysql:check_schema_compatibility()).

test_schema_success() ->
    flurm_dbd_mysql:connect(#{}),
    meck:expect(mysql, query, fun(_, _) -> {ok, [<<"version">>], [[8]]} end),
    ?assertEqual({ok, 8}, flurm_dbd_mysql:check_schema_compatibility()).

test_schema_outside_range() ->
    flurm_dbd_mysql:connect(#{}),
    meck:expect(mysql, query, fun(_, _) -> {ok, [<<"version">>], [[15]]} end),
    ?assertEqual({ok, 15}, flurm_dbd_mysql:check_schema_compatibility()).

test_schema_not_found() ->
    flurm_dbd_mysql:connect(#{}),
    meck:expect(mysql, query, fun(_, _) -> {ok, [<<"version">>], []} end),
    ?assertEqual({error, schema_not_found}, flurm_dbd_mysql:check_schema_compatibility()).

test_connection_status() ->
    Status = flurm_dbd_mysql:get_connection_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(connected, Status)),
    ?assert(maps:is_key(stats, Status)),
    Config = maps:get(config, Status, #{}),
    ?assertNot(maps:is_key(password, Config)).

test_unknown_call() ->
    ?assertEqual({error, unknown_request}, gen_server:call(flurm_dbd_mysql, bogus)).

test_unknown_cast() ->
    gen_server:cast(flurm_dbd_mysql, bogus),
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_mysql:get_connection_status())).

test_unknown_info() ->
    flurm_dbd_mysql ! bogus,
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_mysql:get_connection_status())).

test_auto_connect_max() ->
    sys:replace_state(flurm_dbd_mysql, fun(S) -> setelement(8, S, 10) end),
    flurm_dbd_mysql ! auto_connect,
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_mysql:get_connection_status())).

test_auto_connect_empty() ->
    sys:replace_state(flurm_dbd_mysql, fun(S) ->
        S1 = setelement(3, S, #{}),
        setelement(8, S1, 0)
    end),
    flurm_dbd_mysql ! auto_connect,
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_mysql:get_connection_status())).

test_auto_connect_success() ->
    sys:replace_state(flurm_dbd_mysql, fun(S) ->
        S1 = setelement(3, S, #{host => "localhost"}),
        setelement(8, S1, 0)
    end),
    flurm_dbd_mysql ! auto_connect,
    timer:sleep(100),
    Status = flurm_dbd_mysql:get_connection_status(),
    ?assert(maps:get(connected, Status)).

test_auto_connect_retry() ->
    meck:expect(mysql, start_link, fun(_) -> {error, econnrefused} end),
    sys:replace_state(flurm_dbd_mysql, fun(S) ->
        S1 = setelement(3, S, #{host => "localhost"}),
        setelement(8, S1, 0)
    end),
    flurm_dbd_mysql ! auto_connect,
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_mysql:get_connection_status())).

test_down_message() ->
    flurm_dbd_mysql:connect(#{}),
    FakePid = spawn(fun() -> ok end),
    timer:sleep(10),
    sys:replace_state(flurm_dbd_mysql, fun(S) -> setelement(2, S, FakePid) end),
    flurm_dbd_mysql ! {'DOWN', make_ref(), process, FakePid, normal},
    timer:sleep(100),
    Status = flurm_dbd_mysql:get_connection_status(),
    ?assertNot(maps:get(connected, Status)).

test_terminate_conn() ->
    flurm_dbd_mysql:connect(#{}),
    ok = gen_server:stop(flurm_dbd_mysql),
    timer:sleep(50),
    ok.
