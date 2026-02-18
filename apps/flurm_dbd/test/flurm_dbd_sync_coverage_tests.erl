%%%-------------------------------------------------------------------
%%% @doc Comprehensive coverage tests for flurm_dbd_sync module.
%%%
%%% Tests the sync manager for batched synchronization to slurmdbd.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sync_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Tests
%%====================================================================

sync_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Pure functions
      {"should_retry timeout", fun test_should_retry_timeout/0},
      {"should_retry closed", fun test_should_retry_closed/0},
      {"should_retry econnrefused", fun test_should_retry_econnrefused/0},
      {"should_retry enotconn", fun test_should_retry_enotconn/0},
      {"should_retry error tuple", fun test_should_retry_error_tuple/0},
      {"should_retry other", fun test_should_retry_other/0},
      {"calculate_backoff 0", fun test_backoff_0/0},
      {"calculate_backoff 1", fun test_backoff_1/0},
      {"calculate_backoff 5", fun test_backoff_5/0},

      %% Gen_server
      {"init with config", fun test_init/0},
      {"enable_sync", fun test_enable_sync/0},
      {"disable_sync", fun test_disable_sync/0},
      {"is_sync_enabled", fun test_is_sync_enabled/0},
      {"queue_job success", fun test_queue_job/0},
      {"queue_job queue full", fun test_queue_job_full/0},
      {"queue_jobs success", fun test_queue_jobs/0},
      {"queue_jobs partial", fun test_queue_jobs_partial/0},
      {"flush_queue empty", fun test_flush_empty/0},
      {"flush_queue not connected", fun test_flush_not_connected/0},
      {"clear_queue", fun test_clear_queue/0},
      {"get_sync_status", fun test_get_sync_status/0},
      {"get_pending_count", fun test_get_pending_count/0},
      {"get_failed_jobs", fun test_get_failed_jobs/0},
      {"retry_failed_jobs empty", fun test_retry_empty/0},
      {"unknown call", fun test_unknown_call/0},
      {"unknown cast", fun test_unknown_cast/0},
      {"unknown info", fun test_unknown_info/0},
      {"flush_timer disabled", fun test_flush_timer_disabled/0},
      {"flush_timer enabled", fun test_flush_timer_enabled/0},
      {"terminate", fun test_terminate/0}
     ]}.

setup() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    catch meck:unload(flurm_dbd_mysql),
    meck:new(flurm_dbd_mysql, [non_strict, no_link]),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> false end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) -> ok end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),

    Config = #{enabled => false, batch_size => 10, max_queue_size => 100},
    {ok, Pid} = flurm_dbd_sync:start_link(Config),
    Pid.

cleanup(Pid) ->
    catch gen_server:stop(Pid),
    timer:sleep(10),
    catch meck:unload(flurm_dbd_mysql),
    catch meck:unload(lager),
    ok.

%% === Pure functions ===

test_should_retry_timeout() ->
    ?assert(flurm_dbd_sync:should_retry(timeout, 0)).

test_should_retry_closed() ->
    ?assert(flurm_dbd_sync:should_retry(closed, 0)).

test_should_retry_econnrefused() ->
    ?assert(flurm_dbd_sync:should_retry(econnrefused, 0)).

test_should_retry_enotconn() ->
    ?assert(flurm_dbd_sync:should_retry(enotconn, 0)).

test_should_retry_error_tuple() ->
    ?assert(flurm_dbd_sync:should_retry({error, timeout}, 0)),
    ?assert(flurm_dbd_sync:should_retry({error, closed}, 0)).

test_should_retry_other() ->
    ?assertNot(flurm_dbd_sync:should_retry(syntax_error, 0)),
    ?assertNot(flurm_dbd_sync:should_retry({error, badarg}, 0)).

test_backoff_0() ->
    Result = flurm_dbd_sync:calculate_backoff(0),
    ?assert(Result >= 1000),
    ?assert(Result =< 1250).

test_backoff_1() ->
    Result = flurm_dbd_sync:calculate_backoff(1),
    ?assert(Result >= 2000),
    ?assert(Result =< 2500).

test_backoff_5() ->
    Result = flurm_dbd_sync:calculate_backoff(5),
    ?assert(Result >= 32000),
    ?assert(Result =< 40000).

%% === Gen_server ===

test_init() ->
    Status = flurm_dbd_sync:get_sync_status(),
    ?assert(is_map(Status)),
    ?assertEqual(false, maps:get(enabled, Status)).

test_enable_sync() ->
    ?assertEqual(ok, flurm_dbd_sync:enable_sync()),
    ?assert(flurm_dbd_sync:is_sync_enabled()).

test_disable_sync() ->
    flurm_dbd_sync:enable_sync(),
    ?assertEqual(ok, flurm_dbd_sync:disable_sync()),
    ?assertNot(flurm_dbd_sync:is_sync_enabled()).

test_is_sync_enabled() ->
    ?assertNot(flurm_dbd_sync:is_sync_enabled()),
    flurm_dbd_sync:enable_sync(),
    ?assert(flurm_dbd_sync:is_sync_enabled()).

test_queue_job() ->
    Job = #{job_id => 1, state => completed},
    ?assertEqual(ok, flurm_dbd_sync:queue_job(Job)),
    ?assertEqual(1, flurm_dbd_sync:get_pending_count()).

test_queue_job_full() ->
    %% Fill the queue
    lists:foreach(fun(I) ->
        flurm_dbd_sync:queue_job(#{job_id => I})
    end, lists:seq(1, 100)),
    %% Queue should be full
    ?assertEqual({error, queue_full}, flurm_dbd_sync:queue_job(#{job_id => 101})).

test_queue_jobs() ->
    Jobs = [#{job_id => I} || I <- lists:seq(1, 5)],
    ?assertEqual({ok, 5}, flurm_dbd_sync:queue_jobs(Jobs)).

test_queue_jobs_partial() ->
    %% Fill partially
    lists:foreach(fun(I) ->
        flurm_dbd_sync:queue_job(#{job_id => I})
    end, lists:seq(1, 95)),
    %% Try to queue 10 more (only 5 will fit)
    Jobs = [#{job_id => I} || I <- lists:seq(96, 105)],
    ?assertEqual({ok, 5}, flurm_dbd_sync:queue_jobs(Jobs)).

test_flush_empty() ->
    ?assertEqual({ok, 0}, flurm_dbd_sync:flush_queue()).

test_flush_not_connected() ->
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    ?assertMatch({error, _}, flurm_dbd_sync:flush_queue()).

test_clear_queue() ->
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    {ok, Count} = flurm_dbd_sync:clear_queue(),
    ?assertEqual(1, Count),
    ?assertEqual(0, flurm_dbd_sync:get_pending_count()).

test_get_sync_status() ->
    Status = flurm_dbd_sync:get_sync_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(enabled, Status)),
    ?assert(maps:is_key(pending_count, Status)),
    ?assert(maps:is_key(failed_count, Status)),
    ?assert(maps:is_key(stats, Status)).

test_get_pending_count() ->
    ?assertEqual(0, flurm_dbd_sync:get_pending_count()),
    flurm_dbd_sync:queue_job(#{job_id => 1}),
    ?assertEqual(1, flurm_dbd_sync:get_pending_count()).

test_get_failed_jobs() ->
    Failed = flurm_dbd_sync:get_failed_jobs(),
    ?assertEqual([], Failed).

test_retry_empty() ->
    ?assertEqual({ok, 0}, flurm_dbd_sync:retry_failed_jobs()).

test_unknown_call() ->
    ?assertEqual({error, unknown_request}, gen_server:call(flurm_dbd_sync, bogus)).

test_unknown_cast() ->
    gen_server:cast(flurm_dbd_sync, bogus),
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_sync:get_sync_status())).

test_unknown_info() ->
    flurm_dbd_sync ! bogus,
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_sync:get_sync_status())).

test_flush_timer_disabled() ->
    flurm_dbd_sync:disable_sync(),
    flurm_dbd_sync ! flush_timer,
    timer:sleep(50),
    ?assert(is_map(flurm_dbd_sync:get_sync_status())).

test_flush_timer_enabled() ->
    flurm_dbd_sync:enable_sync(),
    flurm_dbd_sync ! flush_timer,
    timer:sleep(100),
    ?assert(is_map(flurm_dbd_sync:get_sync_status())).

test_terminate() ->
    ok = gen_server:stop(flurm_dbd_sync),
    timer:sleep(50),
    ok.
