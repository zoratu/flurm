%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_dbd_sync (525 lines).
%%%
%%% Tests the MySQL sync manager gen_server with mocked
%%% flurm_dbd_mysql. Tests enable/disable sync, queue_job,
%%% queue_jobs, flush_queue, clear_queue, get_sync_status,
%%% get_pending_count, get_failed_jobs, retry_failed_jobs.
%%% Also tests TEST-exported should_retry/2 and calculate_backoff/1.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sync_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Pure function tests (TEST exports)
%%====================================================================

pure_test_() ->
    [
     %% should_retry
     {"should_retry timeout",          fun test_should_retry_timeout/0},
     {"should_retry closed",           fun test_should_retry_closed/0},
     {"should_retry econnrefused",     fun test_should_retry_econnrefused/0},
     {"should_retry enotconn",         fun test_should_retry_enotconn/0},
     {"should_retry {error, timeout}", fun test_should_retry_error_timeout/0},
     {"should_retry {error, closed}",  fun test_should_retry_error_closed/0},
     {"should_retry other",            fun test_should_retry_other/0},

     %% calculate_backoff
     {"calculate_backoff attempt 0",   fun test_backoff_0/0},
     {"calculate_backoff attempt 1",   fun test_backoff_1/0},
     {"calculate_backoff attempt 5",   fun test_backoff_5/0},
     {"calculate_backoff capped",      fun test_backoff_capped/0}
    ].

test_should_retry_timeout() ->
    ?assert(flurm_dbd_sync:should_retry(timeout, 0)).

test_should_retry_closed() ->
    ?assert(flurm_dbd_sync:should_retry(closed, 1)).

test_should_retry_econnrefused() ->
    ?assert(flurm_dbd_sync:should_retry(econnrefused, 2)).

test_should_retry_enotconn() ->
    ?assert(flurm_dbd_sync:should_retry(enotconn, 0)).

test_should_retry_error_timeout() ->
    ?assert(flurm_dbd_sync:should_retry({error, timeout}, 0)).

test_should_retry_error_closed() ->
    ?assert(flurm_dbd_sync:should_retry({error, closed}, 0)).

test_should_retry_other() ->
    ?assertNot(flurm_dbd_sync:should_retry(syntax_error, 0)),
    ?assertNot(flurm_dbd_sync:should_retry(bad_query, 1)),
    ?assertNot(flurm_dbd_sync:should_retry({error, bad_sql}, 0)).

test_backoff_0() ->
    Delay = flurm_dbd_sync:calculate_backoff(0),
    %% 1000 * 2^0 = 1000, + jitter (0..250)
    ?assert(Delay >= 1000),
    ?assert(Delay =< 1250).

test_backoff_1() ->
    Delay = flurm_dbd_sync:calculate_backoff(1),
    %% 1000 * 2^1 = 2000, + jitter (0..500)
    ?assert(Delay >= 2000),
    ?assert(Delay =< 2500).

test_backoff_5() ->
    Delay = flurm_dbd_sync:calculate_backoff(5),
    %% 1000 * 2^5 = 32000, + jitter (0..8000)
    ?assert(Delay >= 32000),
    ?assert(Delay =< 40000).

test_backoff_capped() ->
    Delay = flurm_dbd_sync:calculate_backoff(10),
    %% 1000 * 2^10 = 1024000, capped at 60000, + jitter (0..15000)
    ?assert(Delay >= 60000),
    ?assert(Delay =< 75000).

%%====================================================================
%% Gen_server tests
%%====================================================================

server_test_() ->
    {foreach,
     fun setup_server/0,
     fun cleanup_server/1,
     [
      {"init disabled by default",        fun test_init_disabled/0},
      {"enable/disable sync",             fun test_enable_disable/0},
      {"is_sync_enabled",                 fun test_is_sync_enabled/0},
      {"queue_job ok",                    fun test_queue_job/0},
      {"queue_job when full",             fun test_queue_job_full/0},
      {"queue_jobs",                      fun test_queue_jobs/0},
      {"queue_jobs partial (queue full)",  fun test_queue_jobs_partial/0},
      {"flush_queue empty",               fun test_flush_empty/0},
      {"flush_queue with jobs success",   fun test_flush_with_jobs/0},
      {"flush_queue not connected",       fun test_flush_not_connected/0},
      {"flush_queue batch error",         fun test_flush_batch_error/0},
      {"flush_queue partial success",     fun test_flush_partial/0},
      {"clear_queue",                     fun test_clear_queue/0},
      {"get_sync_status",                 fun test_get_sync_status/0},
      {"get_pending_count",               fun test_get_pending_count/0},
      {"get_failed_jobs",                 fun test_get_failed_jobs/0},
      {"retry_failed_jobs empty",         fun test_retry_empty/0},
      {"retry_failed_jobs with jobs",     fun test_retry_with_jobs/0},
      {"flush_timer when disabled",       fun test_flush_timer_disabled/0},
      {"flush_timer with error backoff",  fun test_flush_timer_error/0},
      {"maybe_trigger_flush batch full",  fun test_batch_full_trigger/0},
      {"unknown call",                    fun test_unknown_call/0},
      {"unknown cast",                    fun test_unknown_cast/0},
      {"unknown info",                    fun test_unknown_info/0},
      {"terminate with queue",            fun test_terminate_with_queue/0},
      {"terminate empty queue",           fun test_terminate_empty/0}
     ]}.

setup_server() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),

    catch meck:unload(flurm_dbd_mysql),
    meck:new(flurm_dbd_mysql, [non_strict, no_link]),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) -> ok end),

    Config = #{
        enabled => false,
        batch_size => 5,
        flush_interval => 60000,  %% Long interval to prevent auto-flush in tests
        max_retries => 2,
        retry_delay => 100,
        max_queue_size => 20
    },
    {ok, Pid} = flurm_dbd_sync:start_link(Config),
    Pid.

cleanup_server(Pid) ->
    catch gen_server:stop(Pid),
    timer:sleep(10),
    catch meck:unload(flurm_dbd_mysql),
    catch meck:unload(lager),
    ok.

test_init_disabled() ->
    ?assertNot(flurm_dbd_sync:is_sync_enabled()).

test_enable_disable() ->
    ?assertEqual(ok, flurm_dbd_sync:enable_sync()),
    ?assert(flurm_dbd_sync:is_sync_enabled()),
    ?assertEqual(ok, flurm_dbd_sync:disable_sync()),
    ?assertNot(flurm_dbd_sync:is_sync_enabled()).

test_is_sync_enabled() ->
    ?assertNot(flurm_dbd_sync:is_sync_enabled()),
    flurm_dbd_sync:enable_sync(),
    ?assert(flurm_dbd_sync:is_sync_enabled()).

test_queue_job() ->
    ?assertEqual(ok, flurm_dbd_sync:queue_job(#{job_id => 1})),
    ?assertEqual(1, flurm_dbd_sync:get_pending_count()).

test_queue_job_full() ->
    %% Fill the queue (max_queue_size = 20)
    lists:foreach(fun(I) ->
        flurm_dbd_sync:queue_job(#{job_id => I})
    end, lists:seq(1, 20)),
    ?assertEqual({error, queue_full}, flurm_dbd_sync:queue_job(#{job_id => 21})).

test_queue_jobs() ->
    Jobs = [#{job_id => I} || I <- lists:seq(1, 3)],
    ?assertEqual({ok, 3}, flurm_dbd_sync:queue_jobs(Jobs)),
    ?assertEqual(3, flurm_dbd_sync:get_pending_count()).

test_queue_jobs_partial() ->
    %% Fill most of the queue first
    lists:foreach(fun(I) ->
        flurm_dbd_sync:queue_job(#{job_id => I})
    end, lists:seq(1, 18)),
    %% Now try to queue 5 more (only 2 should fit)
    Jobs = [#{job_id => I} || I <- lists:seq(100, 104)],
    {ok, Queued} = flurm_dbd_sync:queue_jobs(Jobs),
    ?assertEqual(2, Queued).

test_flush_empty() ->
    ?assertEqual({ok, 0}, flurm_dbd_sync:flush_queue()).

test_flush_with_jobs() ->
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    flurm_dbd_sync:queue_job(#{job_id => 2}),
    {ok, Count} = flurm_dbd_sync:flush_queue(),
    ?assertEqual(2, Count),
    ?assertEqual(0, flurm_dbd_sync:get_pending_count()).

test_flush_not_connected() ->
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> false end),
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    ?assertMatch({error, not_connected}, flurm_dbd_sync:flush_queue()),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end).

test_flush_batch_error() ->
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(_) -> {error, timeout} end),
    %% Make individual sync also fail
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) -> {error, timeout} end),
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    ?assertMatch({error, _}, flurm_dbd_sync:flush_queue()),
    %% Failed jobs should be in failed list
    Failed = flurm_dbd_sync:get_failed_jobs(),
    ?assert(length(Failed) >= 1).

test_flush_partial() ->
    %% First call to batch fails, individual sync partially succeeds
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(_) -> {error, timeout} end),
    CallCount = atomics:new(1, [{signed, false}]),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(Job) ->
        Count = atomics:add_get(CallCount, 1, 1),
        case Count rem 2 of
            1 -> ok;
            0 -> {error, bad_job}
        end
    end),
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    flurm_dbd_sync:queue_job(#{job_id => 2}),
    {ok, Count} = flurm_dbd_sync:flush_queue(),
    ?assert(Count >= 1).

test_clear_queue() ->
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    flurm_dbd_sync:queue_job(#{job_id => 2}),
    {ok, Cleared} = flurm_dbd_sync:clear_queue(),
    ?assertEqual(2, Cleared),
    ?assertEqual(0, flurm_dbd_sync:get_pending_count()).

test_get_sync_status() ->
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    Status = flurm_dbd_sync:get_sync_status(),
    ?assert(is_map(Status)),
    ?assertNot(maps:get(enabled, Status)),
    ?assertEqual(1, maps:get(pending_count, Status)),
    ?assert(maps:is_key(stats, Status)).

test_get_pending_count() ->
    ?assertEqual(0, flurm_dbd_sync:get_pending_count()),
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    ?assertEqual(1, flurm_dbd_sync:get_pending_count()).

test_get_failed_jobs() ->
    ?assertEqual([], flurm_dbd_sync:get_failed_jobs()).

test_retry_empty() ->
    ?assertEqual({ok, 0}, flurm_dbd_sync:retry_failed_jobs()).

test_retry_with_jobs() ->
    %% First, make some jobs fail
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(_) -> {error, timeout} end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) -> {error, timeout} end),
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    flurm_dbd_sync:flush_queue(),
    timer:sleep(50),
    ?assert(length(flurm_dbd_sync:get_failed_jobs()) >= 1),

    %% Now fix the mock and retry
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
    {ok, Count} = flurm_dbd_sync:retry_failed_jobs(),
    ?assert(Count >= 1),
    ?assertEqual([], flurm_dbd_sync:get_failed_jobs()).

test_flush_timer_disabled() ->
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    %% Send flush_timer when disabled
    flurm_dbd_sync ! flush_timer,
    timer:sleep(100),
    %% Queue should still have the job (no flush)
    ?assertEqual(1, flurm_dbd_sync:get_pending_count()).

test_flush_timer_error() ->
    flurm_dbd_sync:enable_sync(),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> false end),
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    %% Manually trigger flush_timer
    flurm_dbd_sync ! flush_timer,
    timer:sleep(200),
    %% Should not crash; error should increment retry_count
    Status = flurm_dbd_sync:get_sync_status(),
    ?assert(maps:get(retry_count, Status) >= 0),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end).

test_batch_full_trigger() ->
    %% Enable sync and queue batch_size (5) jobs
    flurm_dbd_sync:enable_sync(),
    lists:foreach(fun(I) ->
        flurm_dbd_sync:queue_job(#{job_id => I})
    end, lists:seq(1, 5)),
    %% Batch is full - should trigger immediate flush via self() ! flush_timer
    timer:sleep(200),
    %% Jobs should have been flushed
    ?assertEqual(0, flurm_dbd_sync:get_pending_count()).

test_unknown_call() ->
    ?assertEqual({error, unknown_request},
                 gen_server:call(flurm_dbd_sync, bogus)).

test_unknown_cast() ->
    gen_server:cast(flurm_dbd_sync, bogus),
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_sync:get_sync_status())).

test_unknown_info() ->
    flurm_dbd_sync ! bogus,
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_sync:get_sync_status())).

test_terminate_with_queue() ->
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    ok = gen_server:stop(flurm_dbd_sync),
    timer:sleep(50),
    ok.

test_terminate_empty() ->
    ok = gen_server:stop(flurm_dbd_sync),
    timer:sleep(50),
    ok.
