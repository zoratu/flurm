%%%-------------------------------------------------------------------
%%% @doc Tail coverage tests for flurm_dbd_sync edge branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_dbd_sync_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").

sync_tail_cov_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link/0 reads application env config",
       fun start_link_reads_env_config_test/0},
      {"retry_failed_jobs replies error branch",
       fun retry_failed_jobs_error_reply_branch_test/0},
      {"retry_failed_jobs all synced branch",
       fun retry_failed_jobs_all_synced_branch_test/0}
     ]}.

setup() ->
    stop_sync(),

    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_passthrough_cover, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    catch meck:unload(flurm_dbd_mysql),
    meck:new(flurm_dbd_mysql, [no_link, non_strict]),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> ok end),

    unset_sync_env(),
    ok.

cleanup(_) ->
    stop_sync(),
    unset_sync_env(),
    catch meck:unload(flurm_dbd_mysql),
    catch meck:unload(lager),
    ok.

start_link_reads_env_config_test() ->
    application:set_env(flurm_dbd, sync_enabled, true),
    application:set_env(flurm_dbd, sync_batch_size, 7),
    application:set_env(flurm_dbd, sync_flush_interval, 2222),
    application:set_env(flurm_dbd, sync_max_retries, 4),
    application:set_env(flurm_dbd, sync_retry_delay, 33),
    application:set_env(flurm_dbd, sync_max_queue_size, 55),
    {ok, Pid} = flurm_dbd_sync:start_link(),
    Status = flurm_dbd_sync:get_sync_status(),
    ?assertEqual(true, maps:get(enabled, Status)),
    ?assertEqual(7, maps:get(batch_size, Status)),
    ?assertEqual(2222, maps:get(flush_interval, Status)),
    ?assertEqual(55, maps:get(max_queue_size, Status)),
    gen_server:stop(Pid).

retry_failed_jobs_error_reply_branch_test() ->
    Config = #{
        enabled => false,
        batch_size => 10,
        flush_interval => 1000,
        max_retries => 1,
        retry_delay => 1,
        max_queue_size => 100
    },
    {ok, Pid} = flurm_dbd_sync:start_link(Config),

    %% Force flush failure so a job lands in failed_jobs.
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(_Jobs) -> {error, permanent_error} end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> {error, bad_job} end),
    ok = flurm_dbd_sync:queue_job(#{job_id => 1}),
    ?assertEqual({error, all_jobs_failed}, flurm_dbd_sync:flush_queue()),

    %% Retry path should go through handle_call retry_failed_jobs error reply branch.
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> false end),
    ?assertEqual({error, not_connected}, flurm_dbd_sync:retry_failed_jobs()),
    gen_server:stop(Pid).

retry_failed_jobs_all_synced_branch_test() ->
    Config = #{
        enabled => false,
        batch_size => 10,
        flush_interval => 1000,
        max_retries => 1,
        retry_delay => 1,
        max_queue_size => 100
    },
    {ok, Pid} = flurm_dbd_sync:start_link(Config),

    %% First create a failed job batch.
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(_Jobs) -> {error, permanent_error} end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> {error, bad_job} end),
    ok = flurm_dbd_sync:queue_job(#{job_id => 2}),
    ?assertEqual({error, all_jobs_failed}, flurm_dbd_sync:flush_queue()),

    %% Then retry with individual sync succeeding for all jobs.
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(_Jobs) -> {error, permanent_error} end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> ok end),
    ?assertEqual({ok, 1}, flurm_dbd_sync:retry_failed_jobs()),
    gen_server:stop(Pid).

stop_sync() ->
    case whereis(flurm_dbd_sync) of
        undefined ->
            ok;
        Pid ->
            exit(Pid, kill),
            timer:sleep(10),
            ok
    end.

unset_sync_env() ->
    application:unset_env(flurm_dbd, sync_enabled),
    application:unset_env(flurm_dbd, sync_batch_size),
    application:unset_env(flurm_dbd, sync_flush_interval),
    application:unset_env(flurm_dbd, sync_max_retries),
    application:unset_env(flurm_dbd, sync_retry_delay),
    application:unset_env(flurm_dbd, sync_max_queue_size).

