%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_dbd_server (1188 lines).
%%%
%%% Tests both the TEST-exported internal pure functions (no
%%% gen_server needed) and the full gen_server API (start a real
%%% server instance registered locally).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_server_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%% ---------- Records (copied from source for test access) ----------

-record(job_record, {
    job_id, job_name, user_name, user_id, group_id,
    account, partition, cluster, qos, state, exit_code,
    num_nodes, num_cpus, num_tasks, req_mem,
    submit_time, eligible_time, start_time, end_time, elapsed,
    tres_alloc, tres_req, work_dir, std_out, std_err
}).

-record(step_record, {
    job_id, step_id, step_name, state, exit_code,
    num_tasks, num_nodes, start_time, end_time, elapsed, tres_alloc
}).

-record(assoc_record, {
    id, cluster, account, user,
    partition = <<>>, shares = 1, max_jobs = 0, max_submit = 0,
    max_wall = 0, grp_tres = #{}, max_tres_per_job = #{},
    qos = [], default_qos = <<>>, usage = #{}
}).

%%====================================================================
%% Pure-function tests (TEST exports, no gen_server)
%%====================================================================

pure_functions_test_() ->
    {setup,
     fun setup_lager/0,
     fun cleanup_lager/1,
     [
      {"job_info_to_record full map",           fun test_job_info_to_record_full/0},
      {"job_info_to_record partial map",         fun test_job_info_to_record_partial/0},
      {"update_job_record applies updates",      fun test_update_job_record/0},
      {"update_job_record start_time=0 branch",  fun test_update_job_record_zero_start/0},
      {"step_info_to_record",                    fun test_step_info_to_record/0},
      {"step_info_to_record defaults",           fun test_step_info_to_record_defaults/0},
      {"job_record_to_map round-trip",           fun test_job_record_to_map/0},
      {"assoc_record_to_map",                    fun test_assoc_record_to_map/0},
      {"update_assoc_record",                    fun test_update_assoc_record/0},
      {"update_assoc_record empty updates",      fun test_update_assoc_record_empty/0},
      {"filter_job_records",                     fun test_filter_job_records/0},
      {"matches_job_filters all criteria",       fun test_matches_job_filters/0},
      {"matches_job_filters unknown key",        fun test_matches_unknown_filter/0},
      {"format_sacct_row",                       fun test_format_sacct_row/0},
      {"format_elapsed zero",                    fun test_format_elapsed_zero/0},
      {"format_elapsed 59s",                     fun test_format_elapsed_59/0},
      {"format_elapsed 1h5m3s",                  fun test_format_elapsed_3903/0},
      {"format_elapsed negative/invalid",        fun test_format_elapsed_invalid/0},
      {"format_exit_code normal",                fun test_format_exit_code_normal/0},
      {"format_exit_code non-integer",           fun test_format_exit_code_non_int/0},
      {"current_period format",                  fun test_current_period/0}
     ]}.

setup_lager() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),

    %% Mock flurm_tres since calculate_tres_usage calls it
    catch meck:unload(flurm_tres),
    meck:new(flurm_tres, [non_strict, no_link]),
    meck:expect(flurm_tres, from_job, fun(_) ->
        #{cpu_seconds => 0, mem_seconds => 0, gpu_seconds => 0,
          node_seconds => 0, job_count => 1, job_time => 0}
    end),
    meck:expect(flurm_tres, zero, fun() ->
        #{cpu_seconds => 0, mem_seconds => 0, gpu_seconds => 0,
          node_seconds => 0, job_count => 0, job_time => 0}
    end),
    meck:expect(flurm_tres, add, fun(A, B) ->
        maps:fold(fun(K, V, Acc) ->
            maps:update_with(K, fun(Old) -> Old + V end, V, Acc)
        end, A, B)
    end),
    ok.

cleanup_lager(_) ->
    meck:unload(flurm_tres),
    meck:unload(lager),
    ok.

test_job_info_to_record_full() ->
    Now = erlang:system_time(second),
    Info = #{
        job_id => 42, name => <<"test">>, user_name => <<"alice">>,
        user_id => 1000, group_id => 100, account => <<"acct">>,
        partition => <<"batch">>, cluster => <<"mycluster">>,
        qos => <<"high">>, state => running, exit_code => 0,
        num_nodes => 2, num_cpus => 4, num_tasks => 4, req_mem => 8192,
        submit_time => Now, eligible_time => Now,
        start_time => Now, end_time => Now + 100, elapsed => 100,
        tres_alloc => #{cpu => 4}, tres_req => #{cpu => 4},
        work_dir => <<"/home">>, std_out => <<"out">>, std_err => <<"err">>
    },
    R = flurm_dbd_server:job_info_to_record(Info),
    ?assertEqual(42, R#job_record.job_id),
    ?assertEqual(<<"test">>, R#job_record.job_name),
    ?assertEqual(running, R#job_record.state),
    ?assertEqual(4, R#job_record.num_cpus).

test_job_info_to_record_partial() ->
    R = flurm_dbd_server:job_info_to_record(#{job_id => 1}),
    ?assertEqual(1, R#job_record.job_id),
    ?assertEqual(<<>>, R#job_record.job_name),
    ?assertEqual(pending, R#job_record.state),
    ?assertEqual(#{}, R#job_record.tres_alloc).

test_update_job_record() ->
    Now = erlang:system_time(second),
    Existing = #job_record{
        job_id = 1, job_name = <<"j">>, user_name = <<"u">>,
        user_id = 0, group_id = 0, account = <<>>, partition = <<>>,
        cluster = <<"flurm">>, qos = <<"normal">>, state = running,
        exit_code = 0, num_nodes = 1, num_cpus = 1, num_tasks = 1,
        req_mem = 0, submit_time = Now, eligible_time = Now,
        start_time = Now, end_time = 0, elapsed = 0,
        tres_alloc = #{}, tres_req = #{}, work_dir = <<>>,
        std_out = <<>>, std_err = <<>>
    },
    Updates = #{state => completed, exit_code => 0, end_time => Now + 60},
    Updated = flurm_dbd_server:update_job_record(Existing, Updates),
    ?assertEqual(completed, Updated#job_record.state),
    ?assertEqual(60, Updated#job_record.elapsed).

test_update_job_record_zero_start() ->
    Now = erlang:system_time(second),
    Existing = #job_record{
        job_id = 1, job_name = <<>>, user_name = <<>>,
        user_id = 0, group_id = 0, account = <<>>, partition = <<>>,
        cluster = <<"flurm">>, qos = <<"normal">>, state = pending,
        exit_code = 0, num_nodes = 1, num_cpus = 1, num_tasks = 1,
        req_mem = 0, submit_time = Now, eligible_time = Now,
        start_time = 0, end_time = 0, elapsed = 0,
        tres_alloc = #{}, tres_req = #{}, work_dir = <<>>,
        std_out = <<>>, std_err = <<>>
    },
    Updates = #{state => completed, start_time => Now, end_time => Now + 10},
    Updated = flurm_dbd_server:update_job_record(Existing, Updates),
    ?assertEqual(10, Updated#job_record.elapsed).

test_step_info_to_record() ->
    Info = #{
        job_id => 1, step_id => 0, name => <<"step0">>,
        state => completed, exit_code => 0,
        num_tasks => 2, num_nodes => 1,
        start_time => 100, end_time => 200, elapsed => 100,
        tres_alloc => #{cpu => 2}
    },
    R = flurm_dbd_server:step_info_to_record(Info),
    ?assertEqual(1, R#step_record.job_id),
    ?assertEqual(0, R#step_record.step_id),
    ?assertEqual(<<"step0">>, R#step_record.step_name).

test_step_info_to_record_defaults() ->
    R = flurm_dbd_server:step_info_to_record(#{job_id => 1, step_id => 0}),
    ?assertEqual(<<>>, R#step_record.step_name),
    ?assertEqual(pending, R#step_record.state),
    ?assertEqual(#{}, R#step_record.tres_alloc).

test_job_record_to_map() ->
    Now = erlang:system_time(second),
    R = #job_record{
        job_id = 1, job_name = <<"j">>, user_name = <<"u">>,
        user_id = 0, group_id = 0, account = <<"a">>, partition = <<"p">>,
        cluster = <<"c">>, qos = <<"q">>, state = completed,
        exit_code = 0, num_nodes = 1, num_cpus = 2, num_tasks = 1,
        req_mem = 1024, submit_time = Now, eligible_time = Now,
        start_time = Now, end_time = Now + 10, elapsed = 10,
        tres_alloc = #{}, tres_req = #{}, work_dir = <<"/w">>,
        std_out = <<>>, std_err = <<>>
    },
    M = flurm_dbd_server:job_record_to_map(R),
    ?assertEqual(1, maps:get(job_id, M)),
    ?assertEqual(completed, maps:get(state, M)),
    ?assertEqual(10, maps:get(elapsed, M)).

test_assoc_record_to_map() ->
    R = #assoc_record{
        id = 1, cluster = <<"c">>, account = <<"a">>, user = <<"u">>
    },
    M = flurm_dbd_server:assoc_record_to_map(R),
    ?assertEqual(1, maps:get(id, M)),
    ?assertEqual(<<"c">>, maps:get(cluster, M)),
    ?assertEqual(1, maps:get(shares, M)).

test_update_assoc_record() ->
    R = #assoc_record{
        id = 1, cluster = <<"c">>, account = <<"a">>, user = <<"u">>
    },
    Updates = #{shares => 10, max_jobs => 50, qos => [<<"high">>]},
    U = flurm_dbd_server:update_assoc_record(R, Updates),
    ?assertEqual(10, U#assoc_record.shares),
    ?assertEqual(50, U#assoc_record.max_jobs),
    ?assertEqual([<<"high">>], U#assoc_record.qos).

test_update_assoc_record_empty() ->
    R = #assoc_record{
        id = 1, cluster = <<"c">>, account = <<"a">>, user = <<"u">>
    },
    U = flurm_dbd_server:update_assoc_record(R, #{}),
    ?assertEqual(R, U).

test_filter_job_records() ->
    Now = erlang:system_time(second),
    R1 = #job_record{
        job_id = 1, job_name = <<>>, user_name = <<"alice">>,
        user_id = 0, group_id = 0, account = <<"a">>, partition = <<"p">>,
        cluster = <<"c">>, qos = <<"q">>, state = completed,
        exit_code = 0, num_nodes = 1, num_cpus = 1, num_tasks = 1,
        req_mem = 0, submit_time = Now, eligible_time = Now,
        start_time = Now, end_time = Now + 10, elapsed = 10,
        tres_alloc = #{}, tres_req = #{}, work_dir = <<>>,
        std_out = <<>>, std_err = <<>>
    },
    R2 = R1#job_record{job_id = 2, user_name = <<"bob">>},
    Filtered = flurm_dbd_server:filter_job_records([R1, R2], #{user => <<"alice">>}),
    ?assertEqual(1, length(Filtered)),
    [Match] = Filtered,
    ?assertEqual(1, Match#job_record.job_id).

test_matches_job_filters() ->
    Now = erlang:system_time(second),
    R = #job_record{
        job_id = 1, job_name = <<>>, user_name = <<"alice">>,
        user_id = 0, group_id = 0, account = <<"a">>, partition = <<"p">>,
        cluster = <<"c">>, qos = <<"q">>, state = completed,
        exit_code = 0, num_nodes = 1, num_cpus = 1, num_tasks = 1,
        req_mem = 0, submit_time = Now, eligible_time = Now,
        start_time = Now, end_time = Now + 10, elapsed = 10,
        tres_alloc = #{}, tres_req = #{}, work_dir = <<>>,
        std_out = <<>>, std_err = <<>>
    },
    %% All filters match
    ?assert(flurm_dbd_server:matches_job_filters(R, #{
        user => <<"alice">>, account => <<"a">>,
        partition => <<"p">>, state => completed,
        start_time_after => Now - 1, start_time_before => Now + 1,
        end_time_after => Now, end_time_before => Now + 20
    })),
    %% User mismatch
    ?assertNot(flurm_dbd_server:matches_job_filters(R, #{user => <<"bob">>})),
    %% Account mismatch
    ?assertNot(flurm_dbd_server:matches_job_filters(R, #{account => <<"x">>})),
    %% Partition mismatch
    ?assertNot(flurm_dbd_server:matches_job_filters(R, #{partition => <<"x">>})),
    %% State mismatch
    ?assertNot(flurm_dbd_server:matches_job_filters(R, #{state => running})),
    %% Time filter mismatch
    ?assertNot(flurm_dbd_server:matches_job_filters(R, #{start_time_after => Now + 999})),
    ?assertNot(flurm_dbd_server:matches_job_filters(R, #{start_time_before => Now - 999})),
    ?assertNot(flurm_dbd_server:matches_job_filters(R, #{end_time_after => Now + 999})),
    ?assertNot(flurm_dbd_server:matches_job_filters(R, #{end_time_before => Now - 999})).

test_matches_unknown_filter() ->
    Now = erlang:system_time(second),
    R = #job_record{
        job_id = 1, job_name = <<>>, user_name = <<"alice">>,
        user_id = 0, group_id = 0, account = <<>>, partition = <<>>,
        cluster = <<>>, qos = <<>>, state = pending,
        exit_code = 0, num_nodes = 1, num_cpus = 1, num_tasks = 1,
        req_mem = 0, submit_time = Now, eligible_time = Now,
        start_time = 0, end_time = 0, elapsed = 0,
        tres_alloc = #{}, tres_req = #{}, work_dir = <<>>,
        std_out = <<>>, std_err = <<>>
    },
    %% Unknown filter key should be ignored (passes through)
    ?assert(flurm_dbd_server:matches_job_filters(R, #{unknown_key => anything})).

test_format_sacct_row() ->
    Job = #{job_id => 1, user_name => <<"alice">>, account => <<"acct">>,
            partition => <<"batch">>, state => completed,
            elapsed => 3661, exit_code => 0},
    Row = flurm_dbd_server:format_sacct_row(Job),
    Flat = lists:flatten(Row),
    ?assert(lists:prefix("1", Flat)).

test_format_elapsed_zero() ->
    ?assertEqual("00:00:00", lists:flatten(flurm_dbd_server:format_elapsed(0))).

test_format_elapsed_59() ->
    ?assertEqual("00:00:59", lists:flatten(flurm_dbd_server:format_elapsed(59))).

test_format_elapsed_3903() ->
    %% 1h5m3s = 3600 + 300 + 3 = 3903
    ?assertEqual("01:05:03", lists:flatten(flurm_dbd_server:format_elapsed(3903))).

test_format_elapsed_invalid() ->
    ?assertEqual("00:00:00", flurm_dbd_server:format_elapsed(not_an_int)).

test_format_exit_code_normal() ->
    ?assertEqual("0:0", lists:flatten(flurm_dbd_server:format_exit_code(0))),
    ?assertEqual("1:0", lists:flatten(flurm_dbd_server:format_exit_code(1))).

test_format_exit_code_non_int() ->
    ?assertEqual("0:0", flurm_dbd_server:format_exit_code(not_int)).

test_current_period() ->
    P = flurm_dbd_server:current_period(),
    ?assert(is_binary(P)),
    ?assertEqual(7, byte_size(P)).  %% "YYYY-MM"

%%====================================================================
%% Gen_server integration tests
%%====================================================================

server_test_() ->
    {foreach,
     fun setup_server/0,
     fun cleanup_server/1,
     [
      {"record_job_submit and get_job",      fun test_server_submit_and_get/0},
      {"record_job_start/2 existing job",    fun test_server_start_event_existing/0},
      {"record_job_start/2 new job",         fun test_server_start_event_new/0},
      {"record_job_end/3 existing job",      fun test_server_end_event_existing/0},
      {"record_job_end/3 unknown job",       fun test_server_end_event_unknown/0},
      {"record_job_cancelled existing",      fun test_server_cancel_existing/0},
      {"record_job_cancelled unknown",       fun test_server_cancel_unknown/0},
      {"record_job_start/1 (map) and end",   fun test_server_map_start_end/0},
      {"record_job_end/1 no existing rec",   fun test_server_map_end_no_existing/0},
      {"record_job_step",                    fun test_server_step/0},
      {"list_job_records",                   fun test_server_list/0},
      {"list_job_records with filters",      fun test_server_list_filters/0},
      {"get_job_record not_found",           fun test_server_get_not_found/0},
      {"get_jobs_by_user",                   fun test_server_jobs_by_user/0},
      {"get_jobs_by_account",                fun test_server_jobs_by_account/0},
      {"get_jobs_in_range",                  fun test_server_jobs_in_range/0},
      {"format_sacct_output",               fun test_server_sacct_output/0},
      {"usage tracking",                    fun test_server_usage/0},
      {"reset_usage",                       fun test_server_reset_usage/0},
      {"association CRUD",                  fun test_server_assoc_crud/0},
      {"archive and purge jobs",            fun test_server_archive_purge/0},
      {"archive_old_records",               fun test_server_archive_old_records/0},
      {"purge_old_records",                 fun test_server_purge_old_records/0},
      {"get_stats",                         fun test_server_stats/0},
      {"TRES usage calculations",           fun test_server_tres_usage/0},
      {"get_archived_jobs with filters",    fun test_server_archived_filters/0},
      {"unknown call returns error",        fun test_server_unknown_call/0},
      {"unknown cast is ignored",           fun test_server_unknown_cast/0},
      {"unknown info is ignored",           fun test_server_unknown_info/0},
      {"submit_time megasecs branch",       fun test_server_submit_megasecs/0},
      {"submit_time integer branch",        fun test_server_submit_integer_time/0},
      {"end event with start_time=0",       fun test_server_end_event_start_zero/0},
      {"cancel with start_time=0",          fun test_server_cancel_start_zero/0},
      {"terminate returns ok",              fun test_server_terminate/0}
     ]}.

setup_server() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),

    catch meck:unload(flurm_tres),
    meck:new(flurm_tres, [non_strict, no_link]),
    meck:expect(flurm_tres, from_job, fun(_) ->
        #{cpu_seconds => 10, mem_seconds => 0, gpu_seconds => 0,
          node_seconds => 10, job_count => 1, job_time => 10}
    end),
    meck:expect(flurm_tres, zero, fun() ->
        #{cpu_seconds => 0, mem_seconds => 0, gpu_seconds => 0,
          node_seconds => 0, job_count => 0, job_time => 0}
    end),
    meck:expect(flurm_tres, add, fun(A, B) ->
        maps:fold(fun(K, V, Acc) ->
            maps:update_with(K, fun(Old) -> Old + V end, V, Acc)
        end, A, B)
    end),

    %% Mock start_listener since init sends start_listener message
    catch meck:unload(flurm_dbd_sup),
    meck:new(flurm_dbd_sup, [non_strict, no_link]),
    meck:expect(flurm_dbd_sup, start_listener, fun() -> {ok, self()} end),

    {ok, Pid} = flurm_dbd_server:start_link(),
    %% Drain the start_listener message
    timer:sleep(50),
    Pid.

cleanup_server(Pid) ->
    catch gen_server:stop(Pid),
    timer:sleep(10),
    catch meck:unload(flurm_dbd_sup),
    catch meck:unload(flurm_tres),
    catch meck:unload(lager),
    ok.

test_server_submit_and_get() ->
    flurm_dbd_server:record_job_submit(#{job_id => 100, user_id => 1000}),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(100),
    ?assertEqual(100, maps:get(job_id, Job)),
    ?assertEqual(pending, maps:get(state, Job)).

test_server_start_event_existing() ->
    flurm_dbd_server:record_job_submit(#{job_id => 200}),
    timer:sleep(50),
    flurm_dbd_server:record_job_start(200, [<<"node1">>, <<"node2">>]),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(200),
    ?assertEqual(running, maps:get(state, Job)),
    ?assertEqual(2, maps:get(num_nodes, Job)).

test_server_start_event_new() ->
    flurm_dbd_server:record_job_start(300, [<<"node1">>]),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(300),
    ?assertEqual(running, maps:get(state, Job)).

test_server_end_event_existing() ->
    flurm_dbd_server:record_job_submit(#{job_id => 400}),
    timer:sleep(50),
    flurm_dbd_server:record_job_start(400, [<<"n1">>]),
    timer:sleep(50),
    flurm_dbd_server:record_job_end(400, 0, completed),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(400),
    ?assertEqual(completed, maps:get(state, Job)),
    ?assertEqual(0, maps:get(exit_code, Job)).

test_server_end_event_unknown() ->
    flurm_dbd_server:record_job_end(999, 1, failed),
    timer:sleep(100),
    %% Should just log warning, not crash
    ?assertEqual({error, not_found}, flurm_dbd_server:get_job_record(999)).

test_server_cancel_existing() ->
    flurm_dbd_server:record_job_submit(#{job_id => 500}),
    timer:sleep(50),
    flurm_dbd_server:record_job_cancelled(500, user_request),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(500),
    ?assertEqual(cancelled, maps:get(state, Job)),
    ?assertEqual(-1, maps:get(exit_code, Job)).

test_server_cancel_unknown() ->
    flurm_dbd_server:record_job_cancelled(9999, user_request),
    timer:sleep(100),
    %% Should just log warning
    ?assertEqual({error, not_found}, flurm_dbd_server:get_job_record(9999)).

test_server_map_start_end() ->
    Now = erlang:system_time(second),
    flurm_dbd_server:record_job_start(#{
        job_id => 600, name => <<"j">>, user_name => <<"alice">>,
        state => running, start_time => Now
    }),
    timer:sleep(50),
    flurm_dbd_server:record_job_end(#{
        job_id => 600, state => completed, exit_code => 0, end_time => Now + 60
    }),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(600),
    ?assertEqual(completed, maps:get(state, Job)).

test_server_map_end_no_existing() ->
    Now = erlang:system_time(second),
    flurm_dbd_server:record_job_end(#{
        job_id => 700, state => completed, exit_code => 0, end_time => Now
    }),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(700),
    ?assertEqual(700, maps:get(job_id, Job)).

test_server_step() ->
    flurm_dbd_server:record_job_step(#{job_id => 1, step_id => 0}),
    timer:sleep(100),
    Stats = flurm_dbd_server:get_stats(),
    ?assert(maps:get(steps_recorded, Stats) >= 1).

test_server_list() ->
    flurm_dbd_server:record_job_submit(#{job_id => 801}),
    timer:sleep(50),
    Records = flurm_dbd_server:list_job_records(),
    ?assert(length(Records) >= 1).

test_server_list_filters() ->
    flurm_dbd_server:record_job_submit(#{job_id => 802}),
    timer:sleep(50),
    Records = flurm_dbd_server:list_job_records(#{state => pending}),
    ?assert(length(Records) >= 1).

test_server_get_not_found() ->
    ?assertEqual({error, not_found}, flurm_dbd_server:get_job_record(123456789)).

test_server_jobs_by_user() ->
    Now = erlang:system_time(second),
    flurm_dbd_server:record_job_start(#{
        job_id => 901, user_name => <<"testuser">>, start_time => Now
    }),
    timer:sleep(100),
    Jobs = flurm_dbd_server:get_jobs_by_user(<<"testuser">>),
    ?assert(length(Jobs) >= 1).

test_server_jobs_by_account() ->
    Now = erlang:system_time(second),
    flurm_dbd_server:record_job_start(#{
        job_id => 902, account => <<"testacct">>, start_time => Now
    }),
    timer:sleep(100),
    Jobs = flurm_dbd_server:get_jobs_by_account(<<"testacct">>),
    ?assert(length(Jobs) >= 1).

test_server_jobs_in_range() ->
    Now = erlang:system_time(second),
    flurm_dbd_server:record_job_start(#{
        job_id => 903, start_time => Now
    }),
    timer:sleep(100),
    Jobs = flurm_dbd_server:get_jobs_in_range(Now - 10, Now + 10),
    ?assert(length(Jobs) >= 1).

test_server_sacct_output() ->
    Result = flurm_dbd_server:format_sacct_output([
        #{job_id => 1, user_name => <<"u">>, account => <<"a">>,
          partition => <<"p">>, state => completed, elapsed => 10,
          exit_code => 0}
    ]),
    Flat = lists:flatten(Result),
    ?assert(length(Flat) > 0).

test_server_usage() ->
    UserUsage = flurm_dbd_server:get_user_usage(<<"nonexistent">>),
    ?assertEqual(0, maps:get(cpu_seconds, UserUsage)),
    AcctUsage = flurm_dbd_server:get_account_usage(<<"nonexistent">>),
    ?assertEqual(0, maps:get(cpu_seconds, AcctUsage)),
    ClusterUsage = flurm_dbd_server:get_cluster_usage(),
    ?assert(is_map(ClusterUsage)).

test_server_reset_usage() ->
    ?assertEqual(ok, flurm_dbd_server:reset_usage()).

test_server_assoc_crud() ->
    {ok, Id} = flurm_dbd_server:add_association(<<"c">>, <<"a">>, <<"u">>),
    ?assert(is_integer(Id)),

    {ok, Id2} = flurm_dbd_server:add_association(<<"c">>, <<"a">>, <<"u2">>,
                                                   #{shares => 5, max_jobs => 10}),
    ?assert(Id2 > Id),

    {ok, Assoc} = flurm_dbd_server:get_association(Id),
    ?assertEqual(<<"u">>, maps:get(user, Assoc)),

    ?assertEqual({error, not_found}, flurm_dbd_server:get_association(999999)),

    Assocs = flurm_dbd_server:get_associations(<<"c">>),
    ?assert(length(Assocs) >= 2),

    ?assertEqual(ok, flurm_dbd_server:update_association(Id, #{shares => 20})),
    {ok, Updated} = flurm_dbd_server:get_association(Id),
    ?assertEqual(20, maps:get(shares, Updated)),

    ?assertEqual({error, not_found}, flurm_dbd_server:update_association(999999, #{})),

    UserAssocs = flurm_dbd_server:get_user_associations(<<"u">>),
    ?assert(length(UserAssocs) >= 1),

    AcctAssocs = flurm_dbd_server:get_account_associations(<<"a">>),
    ?assert(length(AcctAssocs) >= 2),

    ?assertEqual(ok, flurm_dbd_server:remove_association(Id)),
    ?assertEqual({error, not_found}, flurm_dbd_server:get_association(Id)),

    ?assertEqual({error, not_found}, flurm_dbd_server:remove_association(999999)).

test_server_archive_purge() ->
    %% Create an old job
    OldTime = erlang:system_time(second) - 200 * 86400,
    flurm_dbd_server:record_job_start(#{
        job_id => 1001, start_time => OldTime,
        state => completed, end_time => OldTime + 100
    }),
    timer:sleep(50),
    %% Also need to set state to completed and end_time
    flurm_dbd_server:record_job_end(#{
        job_id => 1001, state => completed, exit_code => 0,
        end_time => OldTime + 100
    }),
    timer:sleep(100),

    {ok, ArchiveCount} = flurm_dbd_server:archive_old_jobs(30),
    ?assert(ArchiveCount >= 0),

    Archived = flurm_dbd_server:get_archived_jobs(),
    ?assert(is_list(Archived)),

    {ok, PurgeCount} = flurm_dbd_server:purge_old_jobs(1),
    ?assert(PurgeCount >= 0).

test_server_archive_old_records() ->
    {ok, Count} = flurm_dbd_server:archive_old_records(1),
    ?assert(is_integer(Count)).

test_server_purge_old_records() ->
    {ok, Count} = flurm_dbd_server:purge_old_records(1),
    ?assert(is_integer(Count)).

test_server_stats() ->
    Stats = flurm_dbd_server:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(total_job_records, Stats)),
    ?assert(maps:is_key(uptime_seconds, Stats)).

test_server_tres_usage() ->
    UserTres = flurm_dbd_server:calculate_user_tres_usage(<<"alice">>),
    ?assert(is_map(UserTres)),
    AcctTres = flurm_dbd_server:calculate_account_tres_usage(<<"acct">>),
    ?assert(is_map(AcctTres)),
    ClusterTres = flurm_dbd_server:calculate_cluster_tres_usage(),
    ?assert(is_map(ClusterTres)).

test_server_archived_filters() ->
    Records = flurm_dbd_server:get_archived_jobs(#{state => completed}),
    ?assert(is_list(Records)).

test_server_unknown_call() ->
    ?assertEqual({error, unknown_request},
                 gen_server:call(flurm_dbd_server, totally_bogus)).

test_server_unknown_cast() ->
    gen_server:cast(flurm_dbd_server, totally_bogus),
    timer:sleep(50),
    %% Should not crash
    ?assertMatch(#{}, flurm_dbd_server:get_stats()).

test_server_unknown_info() ->
    flurm_dbd_server ! totally_bogus,
    timer:sleep(50),
    ?assertMatch(#{}, flurm_dbd_server:get_stats()).

test_server_submit_megasecs() ->
    %% Test the {MegaSecs, Secs, MicroSecs} branch
    flurm_dbd_server:record_job_submit(#{
        job_id => 1100, submit_time => {1, 500000, 0}
    }),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(1100),
    ?assertEqual(1500000, maps:get(submit_time, Job)).

test_server_submit_integer_time() ->
    Now = erlang:system_time(second),
    flurm_dbd_server:record_job_submit(#{
        job_id => 1101, submit_time => Now
    }),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(1101),
    ?assertEqual(Now, maps:get(submit_time, Job)).

test_server_end_event_start_zero() ->
    %% Job with start_time = 0 should use Now
    flurm_dbd_server:record_job_submit(#{job_id => 1200}),
    timer:sleep(50),
    flurm_dbd_server:record_job_end(1200, 0, completed),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(1200),
    ?assertEqual(completed, maps:get(state, Job)).

test_server_cancel_start_zero() ->
    flurm_dbd_server:record_job_submit(#{job_id => 1300}),
    timer:sleep(50),
    flurm_dbd_server:record_job_cancelled(1300, timeout),
    timer:sleep(100),
    {ok, Job} = flurm_dbd_server:get_job_record(1300),
    ?assertEqual(cancelled, maps:get(state, Job)).

test_server_terminate() ->
    %% Just verify terminate doesn't crash
    ok = gen_server:stop(flurm_dbd_server),
    timer:sleep(50),
    %% Restart for other tests to clean up properly
    ok.
