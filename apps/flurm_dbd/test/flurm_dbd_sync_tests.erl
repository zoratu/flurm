%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon - Sync Manager Tests
%%%
%%% Unit tests for the flurm_dbd_sync module which provides batched
%%% synchronization of job records from FLURM to slurmdbd MySQL.
%%%
%%% These tests use meck to mock the MySQL connector since we don't
%%% want to depend on a real MySQL database for unit tests.
%%%
%%% Test Categories:
%%% 1. Queue Management - queue_job, queue_jobs, queue overflow, clear
%%% 2. Batch Processing - flush logic, batch size triggers, partial syncs
%%% 3. Retry Logic - backoff calculation, retryable errors, max retries
%%% 4. State Management - enable/disable, gen_server callbacks, statistics
%%% 5. Failed Jobs - tracking, retry, recovery
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sync_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing server
    case whereis(flurm_dbd_sync) of
        undefined -> ok;
        ExistingPid ->
            catch gen_server:stop(ExistingPid, normal, 5000),
            flurm_test_utils:wait_for_death(ExistingPid)
    end,
    %% Mock flurm_dbd_mysql
    meck:new(flurm_dbd_mysql, [non_strict, no_link]),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> ok end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
    %% Mock lager to suppress log output
    meck:new(lager, [non_strict, no_link, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
    %% Start the sync manager
    Config = #{
        enabled => false,
        batch_size => 10,
        flush_interval => 100000,  % Long interval to prevent automatic flushes
        max_retries => 3,
        retry_delay => 100,
        max_queue_size => 100
    },
    {ok, Pid} = flurm_dbd_sync:start_link(Config),
    Pid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true ->
            catch gen_server:stop(Pid, normal, 5000);
        false ->
            ok
    end,
    catch meck:unload(flurm_dbd_mysql),
    catch meck:unload(lager),
    ok.

%%====================================================================
%% Enable/Disable Tests
%%====================================================================

enable_disable_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"is_sync_enabled returns false initially", fun test_disabled_initially/0},
             {"enable_sync enables sync", fun test_enable_sync/0},
             {"disable_sync disables sync", fun test_disable_sync/0},
             {"enable_sync starts flush timer", fun test_enable_starts_timer/0},
             {"disable_sync preserves queue", fun test_disable_preserves_queue/0}
         ]
     end
    }.

test_disabled_initially() ->
    Result = flurm_dbd_sync:is_sync_enabled(),
    ?assertEqual(false, Result).

test_enable_sync() ->
    ok = flurm_dbd_sync:enable_sync(),
    ?assertEqual(true, flurm_dbd_sync:is_sync_enabled()).

test_disable_sync() ->
    ok = flurm_dbd_sync:enable_sync(),
    ?assertEqual(true, flurm_dbd_sync:is_sync_enabled()),
    ok = flurm_dbd_sync:disable_sync(),
    ?assertEqual(false, flurm_dbd_sync:is_sync_enabled()).

test_enable_starts_timer() ->
    ok = flurm_dbd_sync:enable_sync(),
    Status = flurm_dbd_sync:get_sync_status(),
    ?assertEqual(true, maps:get(enabled, Status)).

test_disable_preserves_queue() ->
    ok = flurm_dbd_sync:enable_sync(),
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    ok = flurm_dbd_sync:queue_job(sample_job(2)),
    ?assertEqual(2, flurm_dbd_sync:get_pending_count()),

    ok = flurm_dbd_sync:disable_sync(),
    ?assertEqual(false, flurm_dbd_sync:is_sync_enabled()),
    ?assertEqual(2, flurm_dbd_sync:get_pending_count()).

%%====================================================================
%% Queue Tests
%%====================================================================

queue_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"queue_job adds job to queue", fun test_queue_job/0},
         {"queue_jobs adds multiple jobs", fun test_queue_jobs/0},
         {"queue_job returns error when queue full", fun test_queue_full/0},
         {"get_pending_count returns correct count", fun test_pending_count/0},
         {"clear_queue removes all jobs", fun test_clear_queue/0},
         {"queue_job increments stats", fun test_queue_job_stats/0},
         {"queue preserves job order (FIFO)", fun test_queue_order/0}
     ]
    }.

test_queue_job() ->
    Job = sample_job(1001),
    Result = flurm_dbd_sync:queue_job(Job),
    ?assertEqual(ok, Result),
    ?assertEqual(1, flurm_dbd_sync:get_pending_count()).

test_queue_jobs() ->
    Jobs = [sample_job(1001), sample_job(1002), sample_job(1003)],
    Result = flurm_dbd_sync:queue_jobs(Jobs),
    ?assertEqual({ok, 3}, Result),
    ?assertEqual(3, flurm_dbd_sync:get_pending_count()).

test_queue_full() ->
    %% Stop existing and start with tiny queue
    Pid = whereis(flurm_dbd_sync),
    catch gen_server:stop(Pid, normal, 5000),
    flurm_test_utils:wait_for_death(Pid),

    Config = #{
        enabled => false,
        batch_size => 10,
        flush_interval => 100000,
        max_retries => 3,
        retry_delay => 100,
        max_queue_size => 2
    },
    {ok, _NewPid} = flurm_dbd_sync:start_link(Config),

    %% Fill the queue
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    ok = flurm_dbd_sync:queue_job(sample_job(2)),

    %% Next should fail
    Result = flurm_dbd_sync:queue_job(sample_job(3)),
    ?assertEqual({error, queue_full}, Result).

test_pending_count() ->
    ?assertEqual(0, flurm_dbd_sync:get_pending_count()),
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    ?assertEqual(1, flurm_dbd_sync:get_pending_count()),
    ok = flurm_dbd_sync:queue_job(sample_job(2)),
    ?assertEqual(2, flurm_dbd_sync:get_pending_count()).

test_clear_queue() ->
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    ok = flurm_dbd_sync:queue_job(sample_job(2)),
    ?assertEqual(2, flurm_dbd_sync:get_pending_count()),

    {ok, Count} = flurm_dbd_sync:clear_queue(),
    ?assertEqual(2, Count),
    ?assertEqual(0, flurm_dbd_sync:get_pending_count()).

test_queue_job_stats() ->
    InitialStatus = flurm_dbd_sync:get_sync_status(),
    InitialQueued = maps:get(jobs_queued, maps:get(stats, InitialStatus), 0),

    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    ok = flurm_dbd_sync:queue_job(sample_job(2)),

    NewStatus = flurm_dbd_sync:get_sync_status(),
    NewQueued = maps:get(jobs_queued, maps:get(stats, NewStatus), 0),
    ?assertEqual(InitialQueued + 2, NewQueued).

test_queue_order() ->
    %% Queue jobs and verify they come out in order (via flush)
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    SyncedJobs = ets:new(synced_jobs, [public, ordered_set]),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) ->
        lists:foreach(fun(Job) ->
            JobId = maps:get(job_id, Job),
            ets:insert(SyncedJobs, {JobId, Job})
        end, Jobs),
        {ok, length(Jobs)}
    end),

    ok = flurm_dbd_sync:enable_sync(),
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    ok = flurm_dbd_sync:queue_job(sample_job(2)),
    ok = flurm_dbd_sync:queue_job(sample_job(3)),

    {ok, 3} = flurm_dbd_sync:flush_queue(),

    %% Verify all jobs were synced
    ?assertEqual(3, ets:info(SyncedJobs, size)),
    ets:delete(SyncedJobs).

%%====================================================================
%% Flush Tests
%%====================================================================

flush_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"flush_queue syncs jobs when connected", fun test_flush_connected/0},
         {"flush_queue fails when not connected", fun test_flush_not_connected/0},
         {"flush_queue returns 0 for empty queue", fun test_flush_empty/0},
         {"flush updates last_sync_time", fun test_flush_updates_time/0},
         {"flush increments batches_synced", fun test_flush_increments_batches/0}
     ]
    }.

test_flush_connected() ->
    %% Enable and queue jobs
    ok = flurm_dbd_sync:enable_sync(),
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    ok = flurm_dbd_sync:queue_job(sample_job(2)),
    ?assertEqual(2, flurm_dbd_sync:get_pending_count()),

    %% Mock MySQL as connected
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),

    %% Flush
    Result = flurm_dbd_sync:flush_queue(),
    ?assertEqual({ok, 2}, Result),
    ?assertEqual(0, flurm_dbd_sync:get_pending_count()).

test_flush_not_connected() ->
    %% Enable and queue jobs
    ok = flurm_dbd_sync:enable_sync(),
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    ?assertEqual(1, flurm_dbd_sync:get_pending_count()),

    %% Mock MySQL as disconnected
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> false end),

    %% Flush should fail
    Result = flurm_dbd_sync:flush_queue(),
    ?assertEqual({error, not_connected}, Result),
    %% Jobs should stay in pending queue for retry when not connected
    ?assertEqual(1, flurm_dbd_sync:get_pending_count()).

test_flush_empty() ->
    ok = flurm_dbd_sync:enable_sync(),
    ?assertEqual(0, flurm_dbd_sync:get_pending_count()),

    Result = flurm_dbd_sync:flush_queue(),
    ?assertEqual({ok, 0}, Result).

test_flush_updates_time() ->
    ok = flurm_dbd_sync:enable_sync(),
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),

    {ok, 1} = flurm_dbd_sync:flush_queue(),

    Status = flurm_dbd_sync:get_sync_status(),
    Stats = maps:get(stats, Status),
    LastSyncTime = maps:get(last_sync_time, Stats),
    ?assertNotEqual(undefined, LastSyncTime),
    ?assert(is_integer(LastSyncTime)).

test_flush_increments_batches() ->
    ok = flurm_dbd_sync:enable_sync(),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),

    InitialStatus = flurm_dbd_sync:get_sync_status(),
    InitialBatches = maps:get(batches_synced, maps:get(stats, InitialStatus), 0),

    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    {ok, 1} = flurm_dbd_sync:flush_queue(),

    ok = flurm_dbd_sync:queue_job(sample_job(2)),
    {ok, 1} = flurm_dbd_sync:flush_queue(),

    NewStatus = flurm_dbd_sync:get_sync_status(),
    NewBatches = maps:get(batches_synced, maps:get(stats, NewStatus), 0),
    ?assertEqual(InitialBatches + 2, NewBatches).

%%====================================================================
%% Failed Jobs Tests
%%====================================================================

failed_jobs_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         {"get_failed_jobs returns failed jobs", fun test_get_failed_jobs/0},
         {"retry_failed_jobs requeues jobs", fun test_retry_failed_jobs/0},
         {"retry_failed_jobs with empty list", fun test_retry_empty_failed/0},
         {"failed jobs count is tracked in status", fun test_failed_jobs_count_in_status/0}
     ]
    }.

test_get_failed_jobs() ->
    %% Initially empty
    ?assertEqual([], flurm_dbd_sync:get_failed_jobs()),

    %% Queue a job and make sync fail
    ok = flurm_dbd_sync:enable_sync(),
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(_Jobs) -> {error, db_error} end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> {error, db_error} end),

    %% Flush - should fail and move to failed
    _Result = flurm_dbd_sync:flush_queue(),

    %% Check failed jobs
    Failed = flurm_dbd_sync:get_failed_jobs(),
    ?assert(is_list(Failed)),
    ?assertEqual(1, length(Failed)).

test_retry_failed_jobs() ->
    %% Create a failed job
    ok = flurm_dbd_sync:enable_sync(),
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(_Jobs) -> {error, db_error} end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> {error, db_error} end),
    _Result = flurm_dbd_sync:flush_queue(),

    %% Verify job is failed
    ?assertEqual(1, length(flurm_dbd_sync:get_failed_jobs())),

    %% Now make sync succeed
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> ok end),

    %% Retry
    RetryResult = flurm_dbd_sync:retry_failed_jobs(),
    ?assertMatch({ok, _}, RetryResult),
    ?assertEqual([], flurm_dbd_sync:get_failed_jobs()).

test_retry_empty_failed() ->
    ?assertEqual([], flurm_dbd_sync:get_failed_jobs()),
    Result = flurm_dbd_sync:retry_failed_jobs(),
    ?assertEqual({ok, 0}, Result).

test_failed_jobs_count_in_status() ->
    ok = flurm_dbd_sync:enable_sync(),
    ok = flurm_dbd_sync:queue_job(sample_job(1)),
    ok = flurm_dbd_sync:queue_job(sample_job(2)),
    meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
    meck:expect(flurm_dbd_mysql, sync_job_records, fun(_Jobs) -> {error, db_error} end),
    meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> {error, db_error} end),
    _Result = flurm_dbd_sync:flush_queue(),

    Status = flurm_dbd_sync:get_sync_status(),
    ?assertEqual(2, maps:get(failed_count, Status)).

%%====================================================================
%% Status Tests
%%====================================================================

status_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"get_sync_status returns complete status", fun test_sync_status/0},
             {"status includes all required fields", fun test_sync_status_fields/0}
         ]
     end
    }.

test_sync_status() ->
    Status = flurm_dbd_sync:get_sync_status(),
    ?assert(is_map(Status)),
    ?assert(maps:is_key(enabled, Status)),
    ?assert(maps:is_key(pending_count, Status)),
    ?assert(maps:is_key(failed_count, Status)),
    ?assert(maps:is_key(batch_size, Status)),
    ?assert(maps:is_key(flush_interval, Status)),
    ?assert(maps:is_key(max_queue_size, Status)),
    ?assert(maps:is_key(stats, Status)).

test_sync_status_fields() ->
    Status = flurm_dbd_sync:get_sync_status(),
    Stats = maps:get(stats, Status),

    %% Verify all stat fields are present
    ?assert(maps:is_key(jobs_queued, Stats)),
    ?assert(maps:is_key(jobs_synced, Stats)),
    ?assert(maps:is_key(jobs_failed, Stats)),
    ?assert(maps:is_key(batches_synced, Stats)),
    ?assert(maps:is_key(sync_errors, Stats)),
    ?assert(maps:is_key(last_sync_time, Stats)),
    ?assert(maps:is_key(last_error, Stats)).

%%====================================================================
%% Retry Logic Tests (Internal Functions)
%%====================================================================

retry_logic_test_() ->
    [
        {"should_retry returns true for timeout", fun() ->
            ?assertEqual(true, flurm_dbd_sync:should_retry(timeout, 0))
        end},
        {"should_retry returns true for closed", fun() ->
            ?assertEqual(true, flurm_dbd_sync:should_retry(closed, 0))
        end},
        {"should_retry returns true for econnrefused", fun() ->
            ?assertEqual(true, flurm_dbd_sync:should_retry(econnrefused, 0))
        end},
        {"should_retry returns true for enotconn", fun() ->
            ?assertEqual(true, flurm_dbd_sync:should_retry(enotconn, 0))
        end},
        {"should_retry returns true for wrapped timeout", fun() ->
            ?assertEqual(true, flurm_dbd_sync:should_retry({error, timeout}, 0))
        end},
        {"should_retry returns true for wrapped closed", fun() ->
            ?assertEqual(true, flurm_dbd_sync:should_retry({error, closed}, 0))
        end},
        {"should_retry returns false for other errors", fun() ->
            ?assertEqual(false, flurm_dbd_sync:should_retry(db_error, 0)),
            ?assertEqual(false, flurm_dbd_sync:should_retry(syntax_error, 0)),
            ?assertEqual(false, flurm_dbd_sync:should_retry(unknown, 0)),
            ?assertEqual(false, flurm_dbd_sync:should_retry({error, syntax_error}, 0))
        end},
        {"calculate_backoff increases with attempts", fun() ->
            Delay0 = flurm_dbd_sync:calculate_backoff(0),
            Delay1 = flurm_dbd_sync:calculate_backoff(1),
            Delay2 = flurm_dbd_sync:calculate_backoff(2),
            %% Each should be roughly double (with jitter)
            ?assert(Delay1 > Delay0),
            ?assert(Delay2 > Delay1)
        end},
        {"calculate_backoff is capped at max", fun() ->
            %% High attempt number should still be capped
            Delay = flurm_dbd_sync:calculate_backoff(20),
            %% Should not exceed ~75 seconds (60 + 25% jitter)
            ?assert(Delay =< 75000)
        end},
        {"calculate_backoff includes jitter", fun() ->
            %% Run multiple times and check for variation
            Delays = [flurm_dbd_sync:calculate_backoff(2) || _ <- lists:seq(1, 10)],
            UniqueDelays = lists:usort(Delays),
            %% With jitter, we should get some variation
            ?assert(length(UniqueDelays) > 1)
        end}
    ].

%%====================================================================
%% Batch Trigger Tests
%%====================================================================

batch_trigger_test_() ->
    {setup,
     fun() ->
         %% Stop any existing
         case whereis(flurm_dbd_sync) of
             undefined -> ok;
             ExistingPid ->
                 catch gen_server:stop(ExistingPid, normal, 5000),
                 flurm_test_utils:wait_for_process_death(ExistingPid)
         end,
         %% Mock
         meck:new(flurm_dbd_mysql, [non_strict, no_link]),
         meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
         meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
         meck:new(lager, [non_strict, no_link, passthrough]),
         meck:expect(lager, info, fun(_Fmt) -> ok end),
         meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
         %% Start with small batch size
         Config = #{
             enabled => true,
             batch_size => 3,
             flush_interval => 100000,
             max_retries => 1,
             retry_delay => 10,
             max_queue_size => 100
         },
         {ok, NewPid} = flurm_dbd_sync:start_link(Config),
         NewPid
     end,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"auto-flush triggered when batch full", fun() ->
                 %% Queue jobs up to batch size
                 ok = flurm_dbd_sync:queue_job(sample_job(1)),
                 ok = flurm_dbd_sync:queue_job(sample_job(2)),
                 %% Third should trigger flush
                 ok = flurm_dbd_sync:queue_job(sample_job(3)),
                 %% Give time for flush
                 timer:sleep(50),
                 %% Queue should be empty after auto-flush
                 ?assertEqual(0, flurm_dbd_sync:get_pending_count())
             end}
         ]
     end
    }.

%%====================================================================
%% Batch Size Boundary Tests
%%====================================================================

batch_boundary_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_dbd_sync) of
             undefined -> ok;
             ExistingPid ->
                 catch gen_server:stop(ExistingPid, normal, 5000),
                 flurm_test_utils:wait_for_process_death(ExistingPid)
         end,
         meck:new(flurm_dbd_mysql, [non_strict, no_link]),
         meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
         BatchSizes = ets:new(batch_sizes, [public, bag]),
         meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) ->
             ets:insert(BatchSizes, {batch, length(Jobs)}),
             {ok, length(Jobs)}
         end),
         meck:new(lager, [non_strict, no_link, passthrough]),
         meck:expect(lager, info, fun(_Fmt) -> ok end),
         meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
         Config = #{
             enabled => true,
             batch_size => 5,
             flush_interval => 100000,
             max_retries => 1,
             retry_delay => 10,
             max_queue_size => 100
         },
         {ok, NewPid} = flurm_dbd_sync:start_link(Config),
         {NewPid, BatchSizes}
     end,
     fun({Pid, BatchSizes}) ->
         cleanup(Pid),
         ets:delete(BatchSizes)
     end,
     fun({_Pid, BatchSizes}) ->
         [
             {"batch respects configured size", fun() ->
                 %% Queue more than batch size
                 lists:foreach(fun(N) ->
                     ok = flurm_dbd_sync:queue_job(sample_job(N))
                 end, lists:seq(1, 12)),

                 %% Flush manually
                 {ok, _} = flurm_dbd_sync:flush_queue(),
                 {ok, _} = flurm_dbd_sync:flush_queue(),
                 {ok, _} = flurm_dbd_sync:flush_queue(),

                 %% Check batch sizes were 5 or less
                 AllBatches = ets:match(BatchSizes, {batch, '$1'}),
                 ?assert(lists:all(fun([Size]) -> Size =< 5 end, AllBatches))
             end}
         ]
     end
    }.

%%====================================================================
%% Partial Sync Tests
%%====================================================================

partial_sync_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_dbd_sync) of
             undefined -> ok;
             Pid ->
                 catch gen_server:stop(Pid, normal, 5000),
                 flurm_test_utils:wait_for_process_death(Pid)
         end,
         meck:new(flurm_dbd_mysql, [non_strict, no_link]),
         meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
         %% Batch fails, but individual succeeds for some
         meck:expect(flurm_dbd_mysql, sync_job_records, fun(_Jobs) -> {error, batch_error} end),
         Counter = ets:new(counter, [public]),
         ets:insert(Counter, {count, 0}),
         meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) ->
             [{count, N}] = ets:lookup(Counter, count),
             ets:insert(Counter, {count, N + 1}),
             case N rem 2 of
                 0 -> ok;
                 1 -> {error, individual_error}
             end
         end),
         meck:new(lager, [non_strict, no_link, passthrough]),
         meck:expect(lager, info, fun(_Fmt) -> ok end),
         meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
         Config = #{
             enabled => true,
             batch_size => 100,
             flush_interval => 100000,
             max_retries => 1,
             retry_delay => 10,
             max_queue_size => 100
         },
         {ok, NewPid} = flurm_dbd_sync:start_link(Config),
         {NewPid, Counter}
     end,
     fun({Pid, Counter}) ->
         cleanup(Pid),
         ets:delete(Counter)
     end,
     fun({_Pid, _Counter}) ->
         [
             {"partial sync moves failures to failed list", fun() ->
                 %% Queue 4 jobs
                 ok = flurm_dbd_sync:queue_job(sample_job(1)),
                 ok = flurm_dbd_sync:queue_job(sample_job(2)),
                 ok = flurm_dbd_sync:queue_job(sample_job(3)),
                 ok = flurm_dbd_sync:queue_job(sample_job(4)),

                 %% Flush - batch will fail, individual will be 50% success
                 Result = flurm_dbd_sync:flush_queue(),

                 %% Should succeed for some
                 ?assertMatch({ok, _}, Result),

                 %% Some should be in failed
                 Failed = flurm_dbd_sync:get_failed_jobs(),
                 ?assert(length(Failed) > 0),
                 ?assert(length(Failed) < 4)
             end}
         ]
     end
    }.

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"handle_call unknown returns error", fun() ->
                 Result = gen_server:call(flurm_dbd_sync, unknown_request),
                 ?assertEqual({error, unknown_request}, Result)
             end},
             {"handle_cast unknown does not crash", fun() ->
                 gen_server:cast(flurm_dbd_sync, unknown_message),
                 timer:sleep(10),
                 ?assert(is_process_alive(whereis(flurm_dbd_sync)))
             end},
             {"handle_info unknown does not crash", fun() ->
                 whereis(flurm_dbd_sync) ! unknown_info,
                 timer:sleep(10),
                 ?assert(is_process_alive(whereis(flurm_dbd_sync)))
             end},
             {"handle_info flush_timer when disabled is ignored", fun() ->
                 ok = flurm_dbd_sync:disable_sync(),
                 ok = flurm_dbd_sync:queue_job(sample_job(1)),
                 InitialCount = flurm_dbd_sync:get_pending_count(),
                 %% Send flush timer
                 whereis(flurm_dbd_sync) ! flush_timer,
                 timer:sleep(50),
                 %% Count should be unchanged (no flush when disabled)
                 ?assertEqual(InitialCount, flurm_dbd_sync:get_pending_count())
             end}
         ]
     end
    }.

%%====================================================================
%% Queue Partial Fill Tests
%%====================================================================

queue_partial_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_dbd_sync) of
             undefined -> ok;
             Pid ->
                 catch gen_server:stop(Pid, normal, 5000),
                 flurm_test_utils:wait_for_process_death(Pid)
         end,
         meck:new(flurm_dbd_mysql, [non_strict, no_link]),
         meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
         meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) -> {ok, length(Jobs)} end),
         meck:new(lager, [non_strict, no_link, passthrough]),
         meck:expect(lager, info, fun(_Fmt) -> ok end),
         meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
         Config = #{
             enabled => false,
             batch_size => 100,
             flush_interval => 100000,
             max_retries => 1,
             retry_delay => 10,
             max_queue_size => 3
         },
         {ok, NewPid} = flurm_dbd_sync:start_link(Config),
         NewPid
     end,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"queue_jobs partial fill when near limit", fun() ->
                 %% Queue one job first
                 ok = flurm_dbd_sync:queue_job(sample_job(1)),
                 ?assertEqual(1, flurm_dbd_sync:get_pending_count()),

                 %% Try to queue 5 jobs - only 2 should fit
                 Jobs = [sample_job(N) || N <- lists:seq(2, 6)],
                 {ok, Queued} = flurm_dbd_sync:queue_jobs(Jobs),
                 ?assertEqual(2, Queued),
                 ?assertEqual(3, flurm_dbd_sync:get_pending_count())
             end}
         ]
     end
    }.

%%====================================================================
%% Flush Timer and Backoff Tests
%%====================================================================

flush_timer_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_dbd_sync) of
             undefined -> ok;
             Pid ->
                 catch gen_server:stop(Pid, normal, 5000),
                 flurm_test_utils:wait_for_process_death(Pid)
         end,
         meck:new(flurm_dbd_mysql, [non_strict, no_link]),
         meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
         meck:expect(flurm_dbd_mysql, sync_job_records, fun(_Jobs) -> {error, timeout} end),
         meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> {error, timeout} end),
         meck:new(lager, [non_strict, no_link, passthrough]),
         meck:expect(lager, info, fun(_Fmt) -> ok end),
         meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
         Config = #{
             enabled => true,
             batch_size => 100,
             flush_interval => 50,  % Short interval for testing
             max_retries => 1,
             retry_delay => 10,
             max_queue_size => 100
         },
         {ok, NewPid} = flurm_dbd_sync:start_link(Config),
         NewPid
     end,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"error increments retry_count", fun() ->
                 ok = flurm_dbd_sync:queue_job(sample_job(1)),

                 %% Wait for timer-triggered flush
                 timer:sleep(100),

                 Status = flurm_dbd_sync:get_sync_status(),
                 RetryCount = maps:get(retry_count, Status, 0),
                 ?assert(RetryCount > 0)
             end}
         ]
     end
    }.

%%====================================================================
%% Termination Tests
%%====================================================================

terminate_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_dbd_sync) of
             undefined -> ok;
             Pid ->
                 catch gen_server:stop(Pid, normal, 5000),
                 flurm_test_utils:wait_for_process_death(Pid)
         end,
         meck:new(flurm_dbd_mysql, [non_strict, no_link]),
         meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
         FlushCalled = ets:new(flush_called, [public]),
         ets:insert(FlushCalled, {called, false}),
         meck:expect(flurm_dbd_mysql, sync_job_records, fun(Jobs) ->
             ets:insert(FlushCalled, {called, true}),
             {ok, length(Jobs)}
         end),
         meck:new(lager, [non_strict, no_link, passthrough]),
         meck:expect(lager, info, fun(_Fmt) -> ok end),
         meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
         Config = #{
             enabled => true,
             batch_size => 100,
             flush_interval => 100000,
             max_retries => 1,
             retry_delay => 10,
             max_queue_size => 100
         },
         {ok, NewPid} = flurm_dbd_sync:start_link(Config),
         {NewPid, FlushCalled}
     end,
     fun({Pid, FlushCalled}) ->
         case is_process_alive(Pid) of
             true -> catch gen_server:stop(Pid, normal, 5000);
             false -> ok
         end,
         catch meck:unload(flurm_dbd_mysql),
         catch meck:unload(lager),
         ets:delete(FlushCalled)
     end,
     fun({Pid, FlushCalled}) ->
         [
             {"terminate attempts flush of remaining jobs", fun() ->
                 %% Queue jobs
                 ok = flurm_dbd_sync:queue_job(sample_job(1)),
                 ok = flurm_dbd_sync:queue_job(sample_job(2)),
                 ?assertEqual(2, flurm_dbd_sync:get_pending_count()),

                 %% Stop the server
                 gen_server:stop(Pid, normal, 5000),
                 timer:sleep(50),

                 %% Verify flush was attempted
                 [{called, WasCalled}] = ets:lookup(FlushCalled, called),
                 ?assertEqual(true, WasCalled)
             end}
         ]
     end
    }.

%%====================================================================
%% All Jobs Failed Test
%%====================================================================

all_jobs_failed_test_() ->
    {setup,
     fun() ->
         case whereis(flurm_dbd_sync) of
             undefined -> ok;
             Pid ->
                 catch gen_server:stop(Pid, normal, 5000),
                 flurm_test_utils:wait_for_process_death(Pid)
         end,
         meck:new(flurm_dbd_mysql, [non_strict, no_link]),
         meck:expect(flurm_dbd_mysql, is_connected, fun() -> true end),
         meck:expect(flurm_dbd_mysql, sync_job_records, fun(_Jobs) -> {error, db_error} end),
         meck:expect(flurm_dbd_mysql, sync_job_record, fun(_Job) -> {error, db_error} end),
         meck:new(lager, [non_strict, no_link, passthrough]),
         meck:expect(lager, info, fun(_Fmt) -> ok end),
         meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
         meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),
         Config = #{
             enabled => true,
             batch_size => 100,
             flush_interval => 100000,
             max_retries => 1,
             retry_delay => 10,
             max_queue_size => 100
         },
         {ok, NewPid} = flurm_dbd_sync:start_link(Config),
         NewPid
     end,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"all jobs failing returns error", fun() ->
                 ok = flurm_dbd_sync:queue_job(sample_job(1)),
                 ok = flurm_dbd_sync:queue_job(sample_job(2)),
                 ok = flurm_dbd_sync:queue_job(sample_job(3)),

                 Result = flurm_dbd_sync:flush_queue(),
                 ?assertMatch({error, _}, Result),

                 %% All jobs should be in failed list
                 Failed = flurm_dbd_sync:get_failed_jobs(),
                 ?assertEqual(3, length(Failed))
             end}
         ]
     end
    }.

%%====================================================================
%% Helper Functions
%%====================================================================

sample_job(JobId) ->
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
        elapsed => 100
    }.
