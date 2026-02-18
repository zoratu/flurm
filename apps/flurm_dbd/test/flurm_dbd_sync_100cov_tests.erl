%%%-------------------------------------------------------------------
%%% @doc FLURM DBD Sync 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_dbd_sync module covering all exported
%%% functions, edge cases, error paths, and gen_server callbacks.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sync_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Stop any existing server
    case whereis(flurm_dbd_sync) of
        undefined -> ok;
        ExistingPid ->
            catch gen_server:stop(ExistingPid, normal, 5000),
            wait_for_death(ExistingPid)
    end,
    %% Mock dependencies
    meck:new(flurm_dbd_mysql, [passthrough, non_strict, no_link]),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> ok end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
    %% Mock lager
    meck:new(lager, [non_strict, no_link, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    ok.

cleanup(_) ->
    case whereis(flurm_dbd_sync) of
        undefined -> ok;
        Pid ->
            catch gen_server:stop(Pid, normal, 5000),
            wait_for_death(Pid)
    end,
    catch meck:unload(flurm_dbd_mysql),
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
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"start_link/0 starts server with default config", fun() ->
             %% Mock application:get_env
             meck:new(application, [unstick, passthrough]),
             meck:expect(application, get_env, fun
                 (flurm_dbd, sync_enabled, _) -> false;
                 (flurm_dbd, sync_batch_size, _) -> 100;
                 (flurm_dbd, sync_flush_interval, _) -> 5000;
                 (flurm_dbd, sync_max_retries, _) -> 3;
                 (flurm_dbd, sync_retry_delay, _) -> 1000;
                 (flurm_dbd, sync_max_queue_size, _) -> 10000
             end),
             {ok, Pid} = flurm_dbd_sync:start_link(),
             ?assert(is_process_alive(Pid)),
             gen_server:stop(Pid),
             meck:unload(application)
         end},
         {"start_link/1 starts server with explicit config", fun() ->
             Config = #{
                 enabled => true,
                 batch_size => 50,
                 flush_interval => 1000,
                 max_retries => 5,
                 retry_delay => 500,
                 max_queue_size => 500
             },
             {ok, Pid} = flurm_dbd_sync:start_link(Config),
             ?assert(is_process_alive(Pid)),
             Status = flurm_dbd_sync:get_sync_status(),
             ?assertEqual(50, maps:get(batch_size, Status)),
             ?assertEqual(500, maps:get(max_queue_size, Status)),
             gen_server:stop(Pid)
         end},
         {"start_link with enabled=true starts flush timer", fun() ->
             Config = #{enabled => true, batch_size => 10, flush_interval => 100000, max_queue_size => 100},
             {ok, Pid} = flurm_dbd_sync:start_link(Config),
             ?assert(is_process_alive(Pid)),
             Status = flurm_dbd_sync:get_sync_status(),
             ?assertEqual(true, maps:get(enabled, Status)),
             gen_server:stop(Pid)
         end},
         {"start_link with enabled=false does not start timer", fun() ->
             Config = #{enabled => false, batch_size => 10, flush_interval => 100000, max_queue_size => 100},
             {ok, Pid} = flurm_dbd_sync:start_link(Config),
             ?assert(is_process_alive(Pid)),
             Status = flurm_dbd_sync:get_sync_status(),
             ?assertEqual(false, maps:get(enabled, Status)),
             gen_server:stop(Pid)
         end}
     ]
    }.

%%====================================================================
%% Enable/Disable Sync Tests - Extended
%%====================================================================

enable_disable_extended_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{enabled => false, batch_size => 10, flush_interval => 50, max_queue_size => 100},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"enable_sync multiple times is idempotent", fun() ->
             ok = flurm_dbd_sync:enable_sync(),
             ?assertEqual(true, flurm_dbd_sync:is_sync_enabled()),
             ok = flurm_dbd_sync:enable_sync(),
             ?assertEqual(true, flurm_dbd_sync:is_sync_enabled()),
             ok = flurm_dbd_sync:enable_sync(),
             ?assertEqual(true, flurm_dbd_sync:is_sync_enabled())
         end},
         {"disable_sync multiple times is idempotent", fun() ->
             ok = flurm_dbd_sync:enable_sync(),
             ok = flurm_dbd_sync:disable_sync(),
             ?assertEqual(false, flurm_dbd_sync:is_sync_enabled()),
             ok = flurm_dbd_sync:disable_sync(),
             ?assertEqual(false, flurm_dbd_sync:is_sync_enabled())
         end},
         {"toggle enable/disable preserves queue", fun() ->
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             ?assertEqual(2, flurm_dbd_sync:get_pending_count()),
             ok = flurm_dbd_sync:enable_sync(),
             ?assertEqual(2, flurm_dbd_sync:get_pending_count()),
             ok = flurm_dbd_sync:disable_sync(),
             ?assertEqual(2, flurm_dbd_sync:get_pending_count()),
             ok = flurm_dbd_sync:enable_sync(),
             ?assertEqual(2, flurm_dbd_sync:get_pending_count())
         end},
         {"disable_sync cancels flush timer", fun() ->
             ok = flurm_dbd_sync:enable_sync(),
             timer:sleep(10),
             ok = flurm_dbd_sync:disable_sync(),
             %% Timer should be cancelled - no automatic flush
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             timer:sleep(100),
             ?assertEqual(1, flurm_dbd_sync:get_pending_count())
         end}
     ]
    }.

%%====================================================================
%% Queue Job Tests - Extended
%%====================================================================

queue_job_extended_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{enabled => false, batch_size => 5, flush_interval => 100000, max_queue_size => 10},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"queue_job with various job fields", fun() ->
             Job1 = #{job_id => 1, job_name => <<"test1">>, user_id => 1000},
             Job2 = #{job_id => 2, account => <<"account1">>, partition => <<"gpu">>},
             Job3 = #{job_id => 3, state => completed, exit_code => 0},
             ok = flurm_dbd_sync:queue_job(Job1),
             ok = flurm_dbd_sync:queue_job(Job2),
             ok = flurm_dbd_sync:queue_job(Job3),
             ?assertEqual(3, flurm_dbd_sync:get_pending_count())
         end},
         {"queue_job increments jobs_queued stat", fun() ->
             Status1 = flurm_dbd_sync:get_sync_status(),
             Initial = maps:get(jobs_queued, maps:get(stats, Status1), 0),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             Status2 = flurm_dbd_sync:get_sync_status(),
             Final = maps:get(jobs_queued, maps:get(stats, Status2), 0),
             ?assertEqual(Initial + 2, Final)
         end},
         {"queue_job returns queue_full when at limit", fun() ->
             %% Fill the queue (max is 10)
             lists:foreach(fun(N) ->
                 ok = flurm_dbd_sync:queue_job(make_job(N))
             end, lists:seq(1, 10)),
             ?assertEqual(10, flurm_dbd_sync:get_pending_count()),
             Result = flurm_dbd_sync:queue_job(make_job(11)),
             ?assertEqual({error, queue_full}, Result)
         end},
         {"queue_job without job_id field", fun() ->
             Job = #{job_name => <<"noJobId">>, user_id => 1000},
             ok = flurm_dbd_sync:queue_job(Job),
             ?assertEqual(1, flurm_dbd_sync:get_pending_count())
         end},
         {"queue_job with empty map", fun() ->
             Job = #{},
             ok = flurm_dbd_sync:queue_job(Job),
             ?assertEqual(1, flurm_dbd_sync:get_pending_count())
         end}
     ]
    }.

%%====================================================================
%% Queue Jobs Batch Tests
%%====================================================================

queue_jobs_batch_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{enabled => false, batch_size => 5, flush_interval => 100000, max_queue_size => 10},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"queue_jobs empty list", fun() ->
             Result = flurm_dbd_sync:queue_jobs([]),
             ?assertEqual({ok, 0}, Result),
             ?assertEqual(0, flurm_dbd_sync:get_pending_count())
         end},
         {"queue_jobs single job", fun() ->
             Jobs = [make_job(1)],
             Result = flurm_dbd_sync:queue_jobs(Jobs),
             ?assertEqual({ok, 1}, Result),
             ?assertEqual(1, flurm_dbd_sync:get_pending_count())
         end},
         {"queue_jobs multiple jobs", fun() ->
             Jobs = [make_job(N) || N <- lists:seq(1, 5)],
             Result = flurm_dbd_sync:queue_jobs(Jobs),
             ?assertEqual({ok, 5}, Result),
             ?assertEqual(5, flurm_dbd_sync:get_pending_count())
         end},
         {"queue_jobs partial fit when queue almost full", fun() ->
             %% Fill queue with 8 jobs (max 10)
             lists:foreach(fun(N) ->
                 ok = flurm_dbd_sync:queue_job(make_job(N))
             end, lists:seq(1, 8)),
             ?assertEqual(8, flurm_dbd_sync:get_pending_count()),
             %% Try to add 5 more - only 2 should fit
             Jobs = [make_job(N) || N <- lists:seq(9, 13)],
             Result = flurm_dbd_sync:queue_jobs(Jobs),
             ?assertEqual({ok, 2}, Result),
             ?assertEqual(10, flurm_dbd_sync:get_pending_count())
         end},
         {"queue_jobs none fit when queue is full", fun() ->
             %% Fill queue completely
             lists:foreach(fun(N) ->
                 ok = flurm_dbd_sync:queue_job(make_job(N))
             end, lists:seq(1, 10)),
             ?assertEqual(10, flurm_dbd_sync:get_pending_count()),
             %% Try to add more
             Jobs = [make_job(N) || N <- lists:seq(11, 15)],
             Result = flurm_dbd_sync:queue_jobs(Jobs),
             ?assertEqual({ok, 0}, Result)
         end}
     ]
    }.

%%====================================================================
%% Flush Queue Tests - Extended
%%====================================================================

flush_queue_extended_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{enabled => true, batch_size => 5, flush_interval => 100000, max_queue_size => 100, max_retries => 1},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"flush_queue on empty queue returns 0", fun() ->
             Result = flurm_dbd_sync:flush_queue(),
             ?assertEqual({ok, 0}, Result)
         end},
         {"flush_queue with single job", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             Result = flurm_dbd_sync:flush_queue(),
             ?assertEqual({ok, 1}, Result),
             ?assertEqual(0, flurm_dbd_sync:get_pending_count())
         end},
         {"flush_queue respects batch_size", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             BatchSizes = ets:new(batch_sizes, [public, bag]),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) ->
                 ets:insert(BatchSizes, {batch, length(Jobs)}),
                 {ok, length(Jobs)}
             end),
             %% Queue 12 jobs (batch size is 5)
             lists:foreach(fun(N) ->
                 ok = flurm_dbd_sync:queue_job(make_job(N))
             end, lists:seq(1, 12)),
             %% First flush gets batch of 5
             {ok, 5} = flurm_dbd_sync:flush_queue(),
             %% Second flush gets batch of 5
             {ok, 5} = flurm_dbd_sync:flush_queue(),
             %% Third flush gets remaining 2
             {ok, 2} = flurm_dbd_sync:flush_queue(),
             Batches = ets:tab2list(BatchSizes),
             ?assertEqual([{batch, 5}, {batch, 5}, {batch, 2}], Batches),
             ets:delete(BatchSizes)
         end},
         {"flush_queue when mysql disconnected", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> false end),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             Result = flurm_dbd_sync:flush_queue(),
             ?assertEqual({error, not_connected}, Result),
             %% Job stays in queue
             ?assertEqual(1, flurm_dbd_sync:get_pending_count())
         end},
         {"flush_queue increments sync_errors on mysql disconnect", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> false end),
             Status1 = flurm_dbd_sync:get_sync_status(),
             InitialErrors = maps:get(sync_errors, maps:get(stats, Status1), 0),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             _Result = flurm_dbd_sync:flush_queue(),
             Status2 = flurm_dbd_sync:get_sync_status(),
             FinalErrors = maps:get(sync_errors, maps:get(stats, Status2), 0),
             ?assertEqual(InitialErrors + 1, FinalErrors)
         end},
         {"flush_queue sets last_error on failure", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> false end),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             _Result = flurm_dbd_sync:flush_queue(),
             Status = flurm_dbd_sync:get_sync_status(),
             LastError = maps:get(last_error, maps:get(stats, Status)),
             ?assertEqual(not_connected, LastError)
         end},
         {"flush_queue updates jobs_synced stat", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
             Status1 = flurm_dbd_sync:get_sync_status(),
             InitialSynced = maps:get(jobs_synced, maps:get(stats, Status1), 0),
             lists:foreach(fun(N) ->
                 ok = flurm_dbd_sync:queue_job(make_job(N))
             end, lists:seq(1, 3)),
             {ok, 3} = flurm_dbd_sync:flush_queue(),
             Status2 = flurm_dbd_sync:get_sync_status(),
             FinalSynced = maps:get(jobs_synced, maps:get(stats, Status2), 0),
             ?assertEqual(InitialSynced + 3, FinalSynced)
         end},
         {"flush_queue updates batches_synced stat", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
             Status1 = flurm_dbd_sync:get_sync_status(),
             InitialBatches = maps:get(batches_synced, maps:get(stats, Status1), 0),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             {ok, 1} = flurm_dbd_sync:flush_queue(),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             {ok, 1} = flurm_dbd_sync:flush_queue(),
             Status2 = flurm_dbd_sync:get_sync_status(),
             FinalBatches = maps:get(batches_synced, maps:get(stats, Status2), 0),
             ?assertEqual(InitialBatches + 2, FinalBatches)
         end},
         {"flush_queue sets last_sync_time", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             {ok, 1} = flurm_dbd_sync:flush_queue(),
             Status = flurm_dbd_sync:get_sync_status(),
             LastSyncTime = maps:get(last_sync_time, maps:get(stats, Status)),
             ?assertNotEqual(undefined, LastSyncTime),
             ?assert(is_integer(LastSyncTime))
         end}
     ]
    }.

%%====================================================================
%% Failed Jobs and Retry Tests
%%====================================================================

failed_jobs_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{enabled => true, batch_size => 10, flush_interval => 100000, max_queue_size => 100, max_retries => 1},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"get_failed_jobs initially empty", fun() ->
             ?assertEqual([], flurm_dbd_sync:get_failed_jobs())
         end},
         {"failed batch moves jobs to failed list", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(_) -> {error, db_error} end),
             meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) -> {error, db_error} end),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             _Result = flurm_dbd_sync:flush_queue(),
             Failed = flurm_dbd_sync:get_failed_jobs(),
             ?assertEqual(2, length(Failed))
         end},
         {"retry_failed_jobs moves jobs back to queue", fun() ->
             %% First fail some jobs
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(_) -> {error, db_error} end),
             meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) -> {error, db_error} end),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             _Result = flurm_dbd_sync:flush_queue(),
             ?assertEqual(2, length(flurm_dbd_sync:get_failed_jobs())),
             %% Now make sync succeed
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
             {ok, Count} = flurm_dbd_sync:retry_failed_jobs(),
             ?assertEqual(2, Count),
             ?assertEqual([], flurm_dbd_sync:get_failed_jobs())
         end},
         {"retry_failed_jobs with empty list returns 0", fun() ->
             ?assertEqual([], flurm_dbd_sync:get_failed_jobs()),
             Result = flurm_dbd_sync:retry_failed_jobs(),
             ?assertEqual({ok, 0}, Result)
         end},
         {"retry_failed_jobs can fail again", fun() ->
             %% Fail some jobs
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(_) -> {error, db_error} end),
             meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) -> {error, db_error} end),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             _Result = flurm_dbd_sync:flush_queue(),
             ?assertEqual(1, length(flurm_dbd_sync:get_failed_jobs())),
             %% Retry but still fail
             {error, _} = flurm_dbd_sync:retry_failed_jobs(),
             %% Jobs should be back in failed list
             ?assertEqual(1, length(flurm_dbd_sync:get_failed_jobs()))
         end},
         {"partial sync - some succeed some fail", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(_) -> {error, batch_error} end),
             Counter = ets:new(counter, [public]),
             ets:insert(Counter, {count, 0}),
             meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) ->
                 [{count, N}] = ets:lookup(Counter, count),
                 ets:insert(Counter, {count, N + 1}),
                 case N rem 2 of
                     0 -> ok;
                     1 -> {error, individual_error}
                 end
             end),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             ok = flurm_dbd_sync:queue_job(make_job(3)),
             ok = flurm_dbd_sync:queue_job(make_job(4)),
             {ok, Synced} = flurm_dbd_sync:flush_queue(),
             Failed = flurm_dbd_sync:get_failed_jobs(),
             ?assertEqual(2, Synced),
             ?assertEqual(2, length(Failed)),
             ets:delete(Counter)
         end},
         {"all jobs fail in individual sync", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(_) -> {error, batch_error} end),
             meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) -> {error, individual_error} end),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             Result = flurm_dbd_sync:flush_queue(),
             ?assertMatch({error, _}, Result),
             ?assertEqual(2, length(flurm_dbd_sync:get_failed_jobs()))
         end}
     ]
    }.

%%====================================================================
%% Clear Queue Tests
%%====================================================================

clear_queue_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{enabled => false, batch_size => 10, flush_interval => 100000, max_queue_size => 100},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"clear_queue on empty queue returns 0", fun() ->
             Result = flurm_dbd_sync:clear_queue(),
             ?assertEqual({ok, 0}, Result)
         end},
         {"clear_queue removes all pending jobs", fun() ->
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             ok = flurm_dbd_sync:queue_job(make_job(3)),
             ?assertEqual(3, flurm_dbd_sync:get_pending_count()),
             {ok, Count} = flurm_dbd_sync:clear_queue(),
             ?assertEqual(3, Count),
             ?assertEqual(0, flurm_dbd_sync:get_pending_count())
         end},
         {"clear_queue after partial flush", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
             lists:foreach(fun(N) ->
                 ok = flurm_dbd_sync:queue_job(make_job(N))
             end, lists:seq(1, 10)),
             ok = flurm_dbd_sync:enable_sync(),
             {ok, 10} = flurm_dbd_sync:flush_queue(),
             %% Add more
             ok = flurm_dbd_sync:queue_job(make_job(11)),
             ok = flurm_dbd_sync:queue_job(make_job(12)),
             {ok, Count} = flurm_dbd_sync:clear_queue(),
             ?assertEqual(2, Count),
             ?assertEqual(0, flurm_dbd_sync:get_pending_count())
         end}
     ]
    }.

%%====================================================================
%% Get Sync Status Tests
%%====================================================================

get_sync_status_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{
             enabled => true,
             batch_size => 25,
             flush_interval => 2000,
             max_queue_size => 500,
             max_retries => 5,
             retry_delay => 100
         },
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"status contains all required fields", fun() ->
             Status = flurm_dbd_sync:get_sync_status(),
             ?assert(maps:is_key(enabled, Status)),
             ?assert(maps:is_key(pending_count, Status)),
             ?assert(maps:is_key(failed_count, Status)),
             ?assert(maps:is_key(batch_size, Status)),
             ?assert(maps:is_key(flush_interval, Status)),
             ?assert(maps:is_key(max_queue_size, Status)),
             ?assert(maps:is_key(retry_count, Status)),
             ?assert(maps:is_key(stats, Status))
         end},
         {"status reflects configured values", fun() ->
             Status = flurm_dbd_sync:get_sync_status(),
             ?assertEqual(true, maps:get(enabled, Status)),
             ?assertEqual(25, maps:get(batch_size, Status)),
             ?assertEqual(2000, maps:get(flush_interval, Status)),
             ?assertEqual(500, maps:get(max_queue_size, Status))
         end},
         {"status.stats contains all stat fields", fun() ->
             Status = flurm_dbd_sync:get_sync_status(),
             Stats = maps:get(stats, Status),
             ?assert(maps:is_key(jobs_queued, Stats)),
             ?assert(maps:is_key(jobs_synced, Stats)),
             ?assert(maps:is_key(jobs_failed, Stats)),
             ?assert(maps:is_key(batches_synced, Stats)),
             ?assert(maps:is_key(sync_errors, Stats)),
             ?assert(maps:is_key(last_sync_time, Stats)),
             ?assert(maps:is_key(last_error, Stats))
         end},
         {"pending_count reflects queue state", fun() ->
             Status1 = flurm_dbd_sync:get_sync_status(),
             ?assertEqual(0, maps:get(pending_count, Status1)),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             Status2 = flurm_dbd_sync:get_sync_status(),
             ?assertEqual(1, maps:get(pending_count, Status2)),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             Status3 = flurm_dbd_sync:get_sync_status(),
             ?assertEqual(2, maps:get(pending_count, Status3))
         end},
         {"failed_count reflects failed jobs", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(_) -> {error, db_error} end),
             meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) -> {error, db_error} end),
             Status1 = flurm_dbd_sync:get_sync_status(),
             ?assertEqual(0, maps:get(failed_count, Status1)),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             _Result = flurm_dbd_sync:flush_queue(),
             Status2 = flurm_dbd_sync:get_sync_status(),
             ?assertEqual(1, maps:get(failed_count, Status2))
         end}
     ]
    }.

%%====================================================================
%% Get Pending Count Tests
%%====================================================================

get_pending_count_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{enabled => false, batch_size => 10, flush_interval => 100000, max_queue_size => 100},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"get_pending_count returns 0 for empty queue", fun() ->
             ?assertEqual(0, flurm_dbd_sync:get_pending_count())
         end},
         {"get_pending_count accurate after queue_job", fun() ->
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             ?assertEqual(1, flurm_dbd_sync:get_pending_count()),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             ?assertEqual(2, flurm_dbd_sync:get_pending_count())
         end},
         {"get_pending_count accurate after queue_jobs", fun() ->
             {ok, 5} = flurm_dbd_sync:queue_jobs([make_job(N) || N <- lists:seq(1, 5)]),
             ?assertEqual(5, flurm_dbd_sync:get_pending_count())
         end},
         {"get_pending_count accurate after flush", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
             ok = flurm_dbd_sync:enable_sync(),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             ok = flurm_dbd_sync:queue_job(make_job(2)),
             ?assertEqual(2, flurm_dbd_sync:get_pending_count()),
             {ok, 2} = flurm_dbd_sync:flush_queue(),
             ?assertEqual(0, flurm_dbd_sync:get_pending_count())
         end}
     ]
    }.

%%====================================================================
%% Should Retry Tests
%%====================================================================

should_retry_test_() ->
    [
         {"timeout is retryable", fun() ->
             ?assertEqual(true, flurm_dbd_sync:should_retry(timeout, 0)),
             ?assertEqual(true, flurm_dbd_sync:should_retry(timeout, 5))
         end},
         {"closed is retryable", fun() ->
             ?assertEqual(true, flurm_dbd_sync:should_retry(closed, 0))
         end},
         {"econnrefused is retryable", fun() ->
             ?assertEqual(true, flurm_dbd_sync:should_retry(econnrefused, 0))
         end},
         {"enotconn is retryable", fun() ->
             ?assertEqual(true, flurm_dbd_sync:should_retry(enotconn, 0))
         end},
         {"{error, timeout} is retryable", fun() ->
             ?assertEqual(true, flurm_dbd_sync:should_retry({error, timeout}, 0))
         end},
         {"{error, closed} is retryable", fun() ->
             ?assertEqual(true, flurm_dbd_sync:should_retry({error, closed}, 0))
         end},
         {"db_error is not retryable", fun() ->
             ?assertEqual(false, flurm_dbd_sync:should_retry(db_error, 0))
         end},
         {"syntax_error is not retryable", fun() ->
             ?assertEqual(false, flurm_dbd_sync:should_retry(syntax_error, 0))
         end},
         {"{error, duplicate_key} is not retryable", fun() ->
             ?assertEqual(false, flurm_dbd_sync:should_retry({error, duplicate_key}, 0))
         end},
         {"unknown_error is not retryable", fun() ->
             ?assertEqual(false, flurm_dbd_sync:should_retry(unknown_error, 0))
         end},
         {"atom reason is not retryable", fun() ->
             ?assertEqual(false, flurm_dbd_sync:should_retry(some_other_reason, 0))
         end}
    ].

%%====================================================================
%% Calculate Backoff Tests
%%====================================================================

calculate_backoff_test_() ->
    [
         {"attempt 0 gives base delay with jitter", fun() ->
             Delay = flurm_dbd_sync:calculate_backoff(0),
             %% Base is 1000ms, jitter up to 25%
             ?assert(Delay >= 1000),
             ?assert(Delay =< 1250)
         end},
         {"attempt 1 doubles delay", fun() ->
             Delay = flurm_dbd_sync:calculate_backoff(1),
             %% 2000 base + up to 25% jitter
             ?assert(Delay >= 2000),
             ?assert(Delay =< 2500)
         end},
         {"attempt 2 quadruples delay", fun() ->
             Delay = flurm_dbd_sync:calculate_backoff(2),
             %% 4000 base + up to 25% jitter
             ?assert(Delay >= 4000),
             ?assert(Delay =< 5000)
         end},
         {"high attempt is capped at max", fun() ->
             Delay = flurm_dbd_sync:calculate_backoff(10),
             %% Max is 60000ms + 25% jitter
             ?assert(Delay =< 75000)
         end},
         {"very high attempt is still capped", fun() ->
             Delay = flurm_dbd_sync:calculate_backoff(100),
             ?assert(Delay =< 75000)
         end},
         {"jitter provides variation", fun() ->
             Delays = [flurm_dbd_sync:calculate_backoff(2) || _ <- lists:seq(1, 20)],
             UniqueDelays = lists:usort(Delays),
             %% With jitter, should have some variation
             ?assert(length(UniqueDelays) > 1)
         end}
    ].

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{enabled => false, batch_size => 10, flush_interval => 100000, max_queue_size => 100},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"handle_call unknown request returns error", fun() ->
             Result = gen_server:call(flurm_dbd_sync, some_unknown_request),
             ?assertEqual({error, unknown_request}, Result)
         end},
         {"handle_cast does not crash", fun() ->
             gen_server:cast(flurm_dbd_sync, some_unknown_cast),
             timer:sleep(10),
             ?assert(is_process_alive(whereis(flurm_dbd_sync)))
         end},
         {"handle_info unknown does not crash", fun() ->
             whereis(flurm_dbd_sync) ! some_unknown_message,
             timer:sleep(10),
             ?assert(is_process_alive(whereis(flurm_dbd_sync)))
         end},
         {"handle_info flush_timer when disabled is ignored", fun() ->
             ok = flurm_dbd_sync:disable_sync(),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             InitialCount = flurm_dbd_sync:get_pending_count(),
             whereis(flurm_dbd_sync) ! flush_timer,
             timer:sleep(50),
             %% No flush should occur when disabled
             ?assertEqual(InitialCount, flurm_dbd_sync:get_pending_count())
         end},
         {"handle_info flush_timer when enabled triggers flush", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
             ok = flurm_dbd_sync:enable_sync(),
             ok = flurm_dbd_sync:queue_job(make_job(1)),
             ?assertEqual(1, flurm_dbd_sync:get_pending_count()),
             whereis(flurm_dbd_sync) ! flush_timer,
             timer:sleep(50),
             ?assertEqual(0, flurm_dbd_sync:get_pending_count())
         end}
     ]
    }.

%%====================================================================
%% Flush Timer Backoff Tests
%%====================================================================

flush_timer_backoff_test_() ->
    {setup,
     fun() ->
         setup(),
         meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
         meck:expect(flurm_dbd_mysql, sync_job_records, fun(_) -> {error, timeout} end),
         meck:expect(flurm_dbd_mysql, sync_job_record, fun(_) -> {error, timeout} end),
         Config = #{enabled => true, batch_size => 10, flush_interval => 50, max_queue_size => 100, max_retries => 0},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     fun(_Pid) ->
         [
             {"flush error increments retry_count", fun() ->
                 ok = flurm_dbd_sync:queue_job(make_job(1)),
                 %% Wait for a few timer-triggered flushes (they fail)
                 timer:sleep(200),
                 Status = flurm_dbd_sync:get_sync_status(),
                 RetryCount = maps:get(retry_count, Status, 0),
                 ?assert(RetryCount > 0)
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
         FlushCalled = ets:new(flush_called, [public]),
         ets:insert(FlushCalled, {called, false}),
         meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
         meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) ->
             ets:insert(FlushCalled, {called, true}),
             {ok, length(Jobs)}
         end),
         Config = #{enabled => true, batch_size => 10, flush_interval => 100000, max_queue_size => 100},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         {Pid, FlushCalled}
     end,
     fun({Pid, FlushCalled}) ->
         case is_process_alive(Pid) of
             true -> catch gen_server:stop(Pid, normal, 5000);
             false -> ok
         end,
         ets:delete(FlushCalled),
         cleanup(ok)
     end,
     fun({Pid, FlushCalled}) ->
         [
             {"terminate flushes remaining jobs", fun() ->
                 ok = flurm_dbd_sync:queue_job(make_job(1)),
                 ok = flurm_dbd_sync:queue_job(make_job(2)),
                 ?assertEqual(2, flurm_dbd_sync:get_pending_count()),
                 gen_server:stop(Pid, normal, 5000),
                 timer:sleep(50),
                 [{called, WasCalled}] = ets:lookup(FlushCalled, called),
                 ?assertEqual(true, WasCalled)
             end}
         ]
     end
    }.

%%====================================================================
%% Batch Trigger Tests
%%====================================================================

batch_trigger_test_() ->
    {setup,
     fun() ->
         setup(),
         meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
         meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
         Config = #{enabled => true, batch_size => 3, flush_interval => 100000, max_queue_size => 100},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     fun(_Pid) ->
         [
             {"reaching batch_size triggers auto flush", fun() ->
                 ok = flurm_dbd_sync:queue_job(make_job(1)),
                 ok = flurm_dbd_sync:queue_job(make_job(2)),
                 ?assertEqual(2, flurm_dbd_sync:get_pending_count()),
                 %% Third job reaches batch size
                 ok = flurm_dbd_sync:queue_job(make_job(3)),
                 %% Give time for message processing
                 timer:sleep(50),
                 %% Should have been flushed
                 ?assertEqual(0, flurm_dbd_sync:get_pending_count())
             end}
         ]
     end
    }.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun() ->
         setup(),
         Config = #{enabled => true, batch_size => 10, flush_interval => 100000, max_queue_size => 100, max_retries => 3},
         {ok, Pid} = flurm_dbd_sync:start_link(Config),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid, normal, 5000),
         cleanup(ok)
     end,
     [
         {"queue_job with minimal job data", fun() ->
             Job = #{job_id => 1},
             ok = flurm_dbd_sync:queue_job(Job),
             ?assertEqual(1, flurm_dbd_sync:get_pending_count())
         end},
         {"queue_jobs with mixed job formats", fun() ->
             Jobs = [
                 #{job_id => 1},
                 #{job_id => 2, job_name => <<"test">>},
                 #{job_id => 3, state => completed, exit_code => 0}
             ],
             {ok, 3} = flurm_dbd_sync:queue_jobs(Jobs),
             ?assertEqual(3, flurm_dbd_sync:get_pending_count())
         end},
         {"multiple rapid enable/disable cycles", fun() ->
             lists:foreach(fun(_) ->
                 ok = flurm_dbd_sync:enable_sync(),
                 ok = flurm_dbd_sync:disable_sync()
             end, lists:seq(1, 10)),
             ?assert(is_process_alive(whereis(flurm_dbd_sync)))
         end},
         {"rapid queue and flush cycles", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
             lists:foreach(fun(N) ->
                 ok = flurm_dbd_sync:queue_job(make_job(N)),
                 {ok, 1} = flurm_dbd_sync:flush_queue()
             end, lists:seq(1, 10)),
             ?assertEqual(0, flurm_dbd_sync:get_pending_count())
         end},
         {"concurrent operations via multiple calls", fun() ->
             meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
             meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
             %% Queue many jobs
             lists:foreach(fun(N) ->
                 ok = flurm_dbd_sync:queue_job(make_job(N))
             end, lists:seq(1, 20)),
             %% Multiple flush calls
             {ok, _} = flurm_dbd_sync:flush_queue(),
             {ok, _} = flurm_dbd_sync:flush_queue(),
             {ok, _} = flurm_dbd_sync:flush_queue(),
             ?assertEqual(0, flurm_dbd_sync:get_pending_count())
         end}
     ]
    }.

%%====================================================================
%% Helper Functions
%%====================================================================

make_job(JobId) ->
    #{
        job_id => JobId,
        job_name => list_to_binary(io_lib:format("job_~p", [JobId])),
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
        elapsed => 100
    }.
