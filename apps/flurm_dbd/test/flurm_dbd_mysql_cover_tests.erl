%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_dbd_mysql (792 lines).
%%%
%%% Tests TEST-exported pure functions (mapping, state conversion,
%%% TRES formatting, SQL generation) and gen_server API with mocked
%%% mysql connections.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_mysql_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Pure function tests (TEST exports, no gen_server needed)
%%====================================================================

pure_test_() ->
    {setup,
     fun setup_pure/0,
     fun cleanup_pure/1,
     [
      %% map_job_to_slurmdbd
      {"map_job_to_slurmdbd full",              fun test_map_job_full/0},
      {"map_job_to_slurmdbd defaults",          fun test_map_job_defaults/0},

      %% map_slurmdbd_to_job
      {"map_slurmdbd_to_job full",              fun test_map_slurmdbd_full/0},
      {"map_slurmdbd_to_job elapsed calc",      fun test_map_slurmdbd_elapsed/0},
      {"map_slurmdbd_to_job elapsed zero",      fun test_map_slurmdbd_elapsed_zero/0},

      %% state conversions
      {"state_to_slurmdbd all states",          fun test_state_to_slurmdbd/0},
      {"state_to_slurmdbd unknown",             fun test_state_to_slurmdbd_unknown/0},
      {"slurmdbd_to_state all states",          fun test_slurmdbd_to_state/0},
      {"slurmdbd_to_state unknown",             fun test_slurmdbd_to_state_unknown/0},

      %% TRES string formatting
      {"format_tres_string empty map",          fun test_format_tres_empty/0},
      {"format_tres_string with values",        fun test_format_tres_values/0},
      {"format_tres_string zero-size map",      fun test_format_tres_zero_size/0},
      {"format_tres_string non-map",            fun test_format_tres_non_map/0},
      {"format_tres_string unknown key",        fun test_format_tres_unknown_key/0},

      %% TRES string parsing
      {"parse_tres_string empty",               fun test_parse_tres_empty/0},
      {"parse_tres_string valid",               fun test_parse_tres_valid/0},
      {"parse_tres_string gpu id",              fun test_parse_tres_gpu/0},
      {"parse_tres_string non-binary",          fun test_parse_tres_non_binary/0},
      {"parse_tres_string malformed",           fun test_parse_tres_malformed/0},
      {"parse_tres_string bad int",             fun test_parse_tres_bad_int/0},
      {"parse_tres_string unknown id",          fun test_parse_tres_unknown_id/0},

      %% SQL generation
      {"build_insert_query",                    fun test_build_insert_query/0},
      {"build_select_query basic",              fun test_build_select_basic/0},
      {"build_select_query with filters",       fun test_build_select_filters/0},
      {"build_select_query state filter",       fun test_build_select_state_filter/0}
     ]}.

setup_pure() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),
    ok.

cleanup_pure(_) ->
    meck:unload(lager),
    ok.

%% --- map_job_to_slurmdbd ---

test_map_job_full() ->
    Job = #{
        job_id => 42, job_name => <<"test">>, user_id => 1000,
        group_id => 100, account => <<"acct">>, partition => <<"batch">>,
        state => running, exit_code => 0, num_nodes => 2, num_cpus => 4,
        submit_time => 1000, eligible_time => 1001,
        start_time => 1010, end_time => 1100, time_limit => 600,
        tres_alloc => #{cpu => 4}, tres_req => #{cpu => 4},
        work_dir => <<"/home">>, std_out => <<"out">>, std_err => <<"err">>
    },
    R = flurm_dbd_mysql:map_job_to_slurmdbd(Job),
    ?assertEqual(42, maps:get(id_job, R)),
    ?assertEqual(1, maps:get(state, R)),  %% running = 1
    ?assertEqual(4, maps:get(cpus_req, R)).

test_map_job_defaults() ->
    R = flurm_dbd_mysql:map_job_to_slurmdbd(#{}),
    ?assertEqual(0, maps:get(id_job, R)),
    ?assertEqual(0, maps:get(state, R)),  %% pending = 0
    ?assertEqual(<<>>, maps:get(job_name, R)).

%% --- map_slurmdbd_to_job ---

test_map_slurmdbd_full() ->
    Row = #{
        id_job => 42, job_name => <<"test">>, id_user => 1000,
        id_group => 100, account => <<"acct">>, partition => <<"batch">>,
        state => 3, exit_code => 0, nodes_alloc => 2, cpus_req => 4,
        time_submit => 1000, time_eligible => 1001,
        time_start => 1010, time_end => 1100,
        tres_alloc => <<"1=4,4=2">>, tres_req => <<"1=4">>,
        work_dir => <<"/home">>, std_out => <<"out">>, std_err => <<"err">>
    },
    J = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
    ?assertEqual(42, maps:get(job_id, J)),
    ?assertEqual(completed, maps:get(state, J)),  %% 3 = completed
    ?assertEqual(90, maps:get(elapsed, J)).  %% 1100 - 1010

test_map_slurmdbd_elapsed() ->
    Row = #{time_start => 100, time_end => 200},
    J = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
    ?assertEqual(100, maps:get(elapsed, J)).

test_map_slurmdbd_elapsed_zero() ->
    Row = #{time_start => 0, time_end => 0},
    J = flurm_dbd_mysql:map_slurmdbd_to_job(Row),
    ?assertEqual(0, maps:get(elapsed, J)).

%% --- state conversions ---

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
    ?assertEqual(11, flurm_dbd_mysql:state_to_slurmdbd(oom)).

test_state_to_slurmdbd_unknown() ->
    ?assertEqual(0, flurm_dbd_mysql:state_to_slurmdbd(bogus_state)).

test_slurmdbd_to_state() ->
    ?assertEqual(pending,   flurm_dbd_mysql:slurmdbd_to_state(0)),
    ?assertEqual(running,   flurm_dbd_mysql:slurmdbd_to_state(1)),
    ?assertEqual(suspended, flurm_dbd_mysql:slurmdbd_to_state(2)),
    ?assertEqual(completed, flurm_dbd_mysql:slurmdbd_to_state(3)),
    ?assertEqual(cancelled, flurm_dbd_mysql:slurmdbd_to_state(4)),
    ?assertEqual(failed,    flurm_dbd_mysql:slurmdbd_to_state(5)),
    ?assertEqual(timeout,   flurm_dbd_mysql:slurmdbd_to_state(6)),
    ?assertEqual(node_fail, flurm_dbd_mysql:slurmdbd_to_state(7)),
    ?assertEqual(preempted, flurm_dbd_mysql:slurmdbd_to_state(8)),
    ?assertEqual(boot_fail, flurm_dbd_mysql:slurmdbd_to_state(9)),
    ?assertEqual(deadline,  flurm_dbd_mysql:slurmdbd_to_state(10)),
    ?assertEqual(oom,       flurm_dbd_mysql:slurmdbd_to_state(11)).

test_slurmdbd_to_state_unknown() ->
    ?assertEqual(unknown, flurm_dbd_mysql:slurmdbd_to_state(99)).

%% --- TRES string formatting ---

test_format_tres_empty() ->
    ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(#{})).

test_format_tres_values() ->
    R = flurm_dbd_mysql:format_tres_string(#{cpu => 4, node => 2}),
    ?assert(is_binary(R)),
    ?assertMatch({match, _}, re:run(R, <<"1=4">>)),
    ?assertMatch({match, _}, re:run(R, <<"4=2">>)).

test_format_tres_zero_size() ->
    ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(#{})).

test_format_tres_non_map() ->
    ?assertEqual(<<>>, flurm_dbd_mysql:format_tres_string(not_a_map)).

test_format_tres_unknown_key() ->
    %% Unknown keys should be filtered out (undefined type id)
    R = flurm_dbd_mysql:format_tres_string(#{unknown_key => 5}),
    ?assertEqual(<<>>, R).

%% --- TRES string parsing ---

test_parse_tres_empty() ->
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(<<>>)).

test_parse_tres_valid() ->
    R = flurm_dbd_mysql:parse_tres_string(<<"1=4,2=8192,4=2">>),
    ?assertEqual(4, maps:get(cpu, R)),
    ?assertEqual(8192, maps:get(mem, R)),
    ?assertEqual(2, maps:get(node, R)).

test_parse_tres_gpu() ->
    R = flurm_dbd_mysql:parse_tres_string(<<"1001=2">>),
    ?assertEqual(2, maps:get(gpu, R)).

test_parse_tres_non_binary() ->
    ?assertEqual(#{}, flurm_dbd_mysql:parse_tres_string(not_a_binary)).

test_parse_tres_malformed() ->
    %% No = sign
    R = flurm_dbd_mysql:parse_tres_string(<<"abc">>),
    ?assertEqual(#{}, R).

test_parse_tres_bad_int() ->
    %% Non-integer values
    R = flurm_dbd_mysql:parse_tres_string(<<"abc=def">>),
    ?assertEqual(#{}, R).

test_parse_tres_unknown_id() ->
    %% Known-range but unmapped id (e.g., 999)
    R = flurm_dbd_mysql:parse_tres_string(<<"999=5">>),
    %% 999 maps to undefined, so should be skipped
    ?assertNot(maps:is_key(999, R)).

%% --- SQL generation ---

test_build_insert_query() ->
    Q = flurm_dbd_mysql:build_insert_query("flurm_job_table"),
    ?assert(is_binary(Q)),
    ?assertMatch({match, _}, re:run(Q, "INSERT INTO")),
    ?assertMatch({match, _}, re:run(Q, "ON DUPLICATE KEY UPDATE")).

test_build_select_basic() ->
    {Q, Params} = flurm_dbd_mysql:build_select_query("flurm_job_table", #{
        start_time => 1000, end_time => 2000, filters => #{}
    }),
    ?assert(is_binary(Q)),
    ?assertMatch({match, _}, re:run(Q, "SELECT")),
    ?assertEqual(2, length(Params)).  %% start_time, end_time

test_build_select_filters() ->
    {Q, Params} = flurm_dbd_mysql:build_select_query("flurm_job_table", #{
        start_time => 1000, end_time => 2000,
        filters => #{user => <<"alice">>, account => <<"acct">>,
                     partition => <<"batch">>, limit => 500}
    }),
    ?assert(is_binary(Q)),
    ?assertMatch({match, _}, re:run(Q, "id_user")),
    ?assertMatch({match, _}, re:run(Q, "account")),
    ?assertMatch({match, _}, re:run(Q, "`partition`")),
    ?assertEqual(5, length(Params)).  %% start_time, end_time, user, account, partition

test_build_select_state_filter() ->
    {Q, Params} = flurm_dbd_mysql:build_select_query("t", #{
        start_time => 0, end_time => 9999,
        filters => #{state => completed}
    }),
    ?assertMatch({match, _}, re:run(Q, "state")),
    %% Params: start_time, end_time, state_int
    ?assertEqual(3, length(Params)),
    ?assertEqual(3, lists:last(Params)).  %% completed = 3

%%====================================================================
%% Gen_server tests (mock mysql module)
%%====================================================================

server_test_() ->
    {foreach,
     fun setup_server/0,
     fun cleanup_server/1,
     [
      {"init with no auto_connect",          fun test_init_no_autoconnect/0},
      {"connect success",                    fun test_connect_success/0},
      {"connect failure",                    fun test_connect_failure/0},
      {"disconnect",                         fun test_disconnect/0},
      {"is_connected",                       fun test_is_connected/0},
      {"sync_job_record not connected",      fun test_sync_not_connected/0},
      {"sync_job_record success",            fun test_sync_success/0},
      {"sync_job_record failure",            fun test_sync_failure/0},
      {"sync_job_records not connected",     fun test_batch_sync_not_connected/0},
      {"sync_job_records success",           fun test_batch_sync_success/0},
      {"sync_job_records failure",           fun test_batch_sync_failure/0},
      {"read_historical not connected",      fun test_read_not_connected/0},
      {"check_schema not connected",         fun test_schema_not_connected/0},
      {"get_connection_status",              fun test_connection_status/0},
      {"unknown call",                       fun test_unknown_call/0},
      {"unknown cast",                       fun test_unknown_cast/0},
      {"unknown info",                       fun test_unknown_info/0},
      {"auto_connect max attempts",          fun test_auto_connect_max/0},
      {"auto_connect empty config",          fun test_auto_connect_empty_config/0},
      {"DOWN message",                       fun test_down_message/0},
      {"terminate with connection",          fun test_terminate_with_conn/0}
     ]}.

setup_server() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),

    catch meck:unload(mysql),
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

test_init_no_autoconnect() ->
    %% Already started in setup with auto_connect => false
    ?assertNot(flurm_dbd_mysql:is_connected()).

test_connect_success() ->
    ?assertEqual(ok, flurm_dbd_mysql:connect(#{host => "localhost"})),
    ?assert(flurm_dbd_mysql:is_connected()).

test_connect_failure() ->
    meck:expect(mysql, start_link, fun(_) -> {error, econnrefused} end),
    ?assertMatch({error, _}, flurm_dbd_mysql:connect(#{host => "bad"})).

test_disconnect() ->
    flurm_dbd_mysql:connect(#{}),
    ?assertEqual(ok, flurm_dbd_mysql:disconnect()),
    ?assertNot(flurm_dbd_mysql:is_connected()).

test_is_connected() ->
    ?assertNot(flurm_dbd_mysql:is_connected()),
    flurm_dbd_mysql:connect(#{}),
    ?assert(flurm_dbd_mysql:is_connected()).

test_sync_not_connected() ->
    ?assertMatch({error, not_connected}, flurm_dbd_mysql:sync_job_record(#{})).

test_sync_success() ->
    flurm_dbd_mysql:connect(#{}),
    ?assertEqual(ok, flurm_dbd_mysql:sync_job_record(#{job_id => 1, state => completed})).

test_sync_failure() ->
    flurm_dbd_mysql:connect(#{}),
    meck:expect(mysql, query, fun(_, _, _) -> {error, timeout} end),
    ?assertMatch({error, _}, flurm_dbd_mysql:sync_job_record(#{job_id => 1})).

test_batch_sync_not_connected() ->
    ?assertMatch({error, not_connected}, flurm_dbd_mysql:sync_job_records([#{}])).

test_batch_sync_success() ->
    flurm_dbd_mysql:connect(#{}),
    meck:expect(mysql, query, fun(_, _, _) -> ok end),
    meck:expect(mysql, transaction, fun(_, Fun) -> {atomic, Fun()} end),
    ?assertMatch({ok, _}, flurm_dbd_mysql:sync_job_records([#{job_id => 1}, #{job_id => 2}])).

test_batch_sync_failure() ->
    flurm_dbd_mysql:connect(#{}),
    meck:expect(mysql, transaction, fun(_, _Fun) -> {aborted, timeout} end),
    ?assertMatch({error, _}, flurm_dbd_mysql:sync_job_records([#{}])).

test_read_not_connected() ->
    ?assertMatch({error, not_connected},
                 flurm_dbd_mysql:read_historical_jobs(0, 9999)).

test_schema_not_connected() ->
    ?assertMatch({error, not_connected},
                 flurm_dbd_mysql:check_schema_compatibility()).

test_connection_status() ->
    Status = flurm_dbd_mysql:get_connection_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(connected, Status)),
    ?assert(maps:is_key(stats, Status)),
    %% Password should be hidden
    Config = maps:get(config, Status, #{}),
    ?assertNot(maps:is_key(password, Config)).

test_unknown_call() ->
    ?assertEqual({error, unknown_request},
                 gen_server:call(flurm_dbd_mysql, bogus)).

test_unknown_cast() ->
    gen_server:cast(flurm_dbd_mysql, bogus),
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_mysql:get_connection_status())).

test_unknown_info() ->
    flurm_dbd_mysql ! bogus,
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_mysql:get_connection_status())).

test_auto_connect_max() ->
    %% Simulate max reconnect attempts reached
    %% Send auto_connect when reconnect_attempts is at max (10)
    %% We do this by setting state via sys:replace_state
    sys:replace_state(flurm_dbd_mysql, fun(S) ->
        %% S is a record - set reconnect_attempts field via setelement
        %% Record layout: state is the 1st field (tag), then fields in order
        %% reconnect_attempts is field 8 (1-based: tag=1, connection=2, config=3,
        %% connected=4, schema_version=5, last_error=6, stats=7, reconnect_attempts=8)
        setelement(8, S, 10)
    end),
    flurm_dbd_mysql ! auto_connect,
    timer:sleep(50),
    %% Should not crash
    ?assert(is_map(flurm_dbd_mysql:get_connection_status())).

test_auto_connect_empty_config() ->
    sys:replace_state(flurm_dbd_mysql, fun(S) ->
        %% Set config (field 3) to empty map and reconnect_attempts (field 8) to 0
        S1 = setelement(3, S, #{}),
        setelement(8, S1, 0)
    end),
    flurm_dbd_mysql ! auto_connect,
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_mysql:get_connection_status())).

test_down_message() ->
    %% Connect first
    flurm_dbd_mysql:connect(#{}),
    %% Get the connection pid
    Status = flurm_dbd_mysql:get_connection_status(),
    ?assert(maps:get(connected, Status)),
    %% Send a DOWN message for the connection
    FakePid = spawn(fun() -> ok end),
    timer:sleep(10),
    sys:replace_state(flurm_dbd_mysql, fun(S) ->
        setelement(2, S, FakePid)
    end),
    flurm_dbd_mysql ! {'DOWN', make_ref(), process, FakePid, normal},
    timer:sleep(100),
    %% Should now be disconnected
    Status2 = flurm_dbd_mysql:get_connection_status(),
    ?assertNot(maps:get(connected, Status2)).

test_terminate_with_conn() ->
    flurm_dbd_mysql:connect(#{}),
    ok = gen_server:stop(flurm_dbd_mysql),
    timer:sleep(50),
    ok.
