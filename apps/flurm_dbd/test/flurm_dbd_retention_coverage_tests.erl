%%%-------------------------------------------------------------------
%%% @doc Comprehensive coverage tests for flurm_dbd_retention module.
%%%
%%% Tests the data retention and pruning functionality.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_retention_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Tests
%%====================================================================

retention_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% prune_old_records
      {"prune_old_records success", fun test_prune_success/0},
      {"prune_old_records with errors", fun test_prune_with_errors/0},

      %% get_retention_config
      {"get_retention_config defaults", fun test_get_config_defaults/0},
      {"get_retention_config custom", fun test_get_config_custom/0},

      %% set_retention_config
      {"set_retention_config valid", fun test_set_config_valid/0},
      {"set_retention_config invalid", fun test_set_config_invalid/0},
      {"set_retention_config partial", fun test_set_config_partial/0},

      %% prune_job_records
      {"prune_job_records success", fun test_prune_jobs_success/0},
      {"prune_job_records error", fun test_prune_jobs_error/0},

      %% prune_usage_records
      {"prune_usage_records", fun test_prune_usage/0},

      %% archive_records
      {"archive_records success", fun test_archive_success/0},
      {"archive_records error", fun test_archive_error/0},

      %% get_archive_stats
      {"get_archive_stats", fun test_archive_stats/0},
      {"get_archive_stats empty", fun test_archive_stats_empty/0},

      %% schedule_prune
      {"schedule_prune", fun test_schedule_prune/0},

      %% cancel_scheduled_prune
      {"cancel_scheduled_prune", fun test_cancel_prune/0},
      {"cancel_scheduled_prune none", fun test_cancel_prune_none/0}
     ]}.

setup() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    meck:new(flurm_dbd_server, [non_strict, no_link]),
    meck:expect(flurm_dbd_server, archive_old_records, fun(_) -> {ok, 10} end),
    meck:expect(flurm_dbd_server, purge_old_records, fun(_) -> {ok, 5} end),
    meck:expect(flurm_dbd_server, archive_old_jobs, fun(_) -> {ok, 20} end),
    meck:expect(flurm_dbd_server, get_archived_jobs, fun() -> sample_archived_jobs() end),
    meck:expect(flurm_dbd_server, get_stats, fun() -> #{archived_jobs => 50} end),
    ok.

cleanup(_) ->
    application:unset_env(flurm_dbd, prune_timer_ref),
    application:unset_env(flurm_dbd, job_retention_days),
    application:unset_env(flurm_dbd, usage_retention_days),
    application:unset_env(flurm_dbd, archive_retention_days),
    application:unset_env(flurm_dbd, auto_prune_enabled),
    application:unset_env(flurm_dbd, auto_prune_interval_hours),
    catch meck:unload(flurm_dbd_server),
    catch meck:unload(lager),
    ok.

sample_archived_jobs() ->
    Now = erlang:system_time(second),
    [
        #{job_id => 1, end_time => Now - 86400 * 100},
        #{job_id => 2, end_time => Now - 86400 * 200},
        #{job_id => 3, end_time => Now - 86400 * 50}
    ].

%% === prune_old_records ===

test_prune_success() ->
    {ok, Result} = flurm_dbd_retention:prune_old_records(365),
    ?assert(is_map(Result)),
    ?assertEqual(15, maps:get(jobs_pruned, Result)),
    ?assertEqual([], maps:get(errors, Result)).

test_prune_with_errors() ->
    meck:expect(flurm_dbd_server, archive_old_records, fun(_) -> erlang:error(test_error) end),
    {ok, Result} = flurm_dbd_retention:prune_old_records(365),
    ?assert(length(maps:get(errors, Result)) > 0).

%% === get_retention_config ===

test_get_config_defaults() ->
    Config = flurm_dbd_retention:get_retention_config(),
    ?assertEqual(365, maps:get(job_retention_days, Config)),
    ?assertEqual(730, maps:get(usage_retention_days, Config)),
    ?assertEqual(1095, maps:get(archive_retention_days, Config)),
    ?assertEqual(false, maps:get(auto_prune_enabled, Config)),
    ?assertEqual(24, maps:get(auto_prune_interval_hours, Config)).

test_get_config_custom() ->
    application:set_env(flurm_dbd, job_retention_days, 180),
    application:set_env(flurm_dbd, auto_prune_enabled, true),
    Config = flurm_dbd_retention:get_retention_config(),
    ?assertEqual(180, maps:get(job_retention_days, Config)),
    ?assertEqual(true, maps:get(auto_prune_enabled, Config)).

%% === set_retention_config ===

test_set_config_valid() ->
    Config = #{
        job_retention_days => 90,
        usage_retention_days => 180,
        archive_retention_days => 365,
        auto_prune_enabled => true,
        auto_prune_interval_hours => 12
    },
    ?assertEqual(ok, flurm_dbd_retention:set_retention_config(Config)),
    NewConfig = flurm_dbd_retention:get_retention_config(),
    ?assertEqual(90, maps:get(job_retention_days, NewConfig)).

test_set_config_invalid() ->
    Config = #{job_retention_days => -1},
    ?assertMatch({error, _}, flurm_dbd_retention:set_retention_config(Config)).

test_set_config_partial() ->
    Config = #{job_retention_days => 60},
    ?assertEqual(ok, flurm_dbd_retention:set_retention_config(Config)),
    NewConfig = flurm_dbd_retention:get_retention_config(),
    ?assertEqual(60, maps:get(job_retention_days, NewConfig)).

%% === prune_job_records ===

test_prune_jobs_success() ->
    Now = erlang:system_time(second),
    Cutoff = Now - 86400 * 30,
    Result = flurm_dbd_retention:prune_job_records(Cutoff),
    ?assert(is_map(Result)),
    ?assert(maps:get(count, Result) >= 0),
    ?assertEqual([], maps:get(errors, Result)).

test_prune_jobs_error() ->
    meck:expect(flurm_dbd_server, archive_old_records, fun(_) -> erlang:error(db_error) end),
    Now = erlang:system_time(second),
    Result = flurm_dbd_retention:prune_job_records(Now - 86400),
    ?assertEqual(0, maps:get(count, Result)),
    ?assert(length(maps:get(errors, Result)) > 0).

%% === prune_usage_records ===

test_prune_usage() ->
    Now = erlang:system_time(second),
    Result = flurm_dbd_retention:prune_usage_records(Now - 86400 * 365),
    ?assert(is_map(Result)),
    ?assertEqual(0, maps:get(count, Result)).

%% === archive_records ===

test_archive_success() ->
    {ok, Count} = flurm_dbd_retention:archive_records(90),
    ?assertEqual(20, Count).

test_archive_error() ->
    meck:expect(flurm_dbd_server, archive_old_jobs, fun(_) -> erlang:error(archive_error) end),
    ?assertMatch({error, _}, flurm_dbd_retention:archive_records(90)).

%% === get_archive_stats ===

test_archive_stats() ->
    Stats = flurm_dbd_retention:get_archive_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(archived_job_count, Stats)),
    ?assert(maps:is_key(total_archived, Stats)),
    ?assert(maps:is_key(oldest_archive, Stats)),
    ?assert(maps:is_key(newest_archive, Stats)),
    ?assertEqual(3, maps:get(archived_job_count, Stats)).

test_archive_stats_empty() ->
    meck:expect(flurm_dbd_server, get_archived_jobs, fun() -> [] end),
    Stats = flurm_dbd_retention:get_archive_stats(),
    ?assertEqual(0, maps:get(archived_job_count, Stats)),
    ?assertEqual(0, maps:get(oldest_archive, Stats)).

%% === schedule_prune ===

test_schedule_prune() ->
    {ok, TimerRef} = flurm_dbd_retention:schedule_prune(24),
    ?assert(is_reference(TimerRef)),
    %% Clean up
    erlang:cancel_timer(TimerRef).

%% === cancel_scheduled_prune ===

test_cancel_prune() ->
    {ok, _} = flurm_dbd_retention:schedule_prune(24),
    ?assertEqual(ok, flurm_dbd_retention:cancel_scheduled_prune()).

test_cancel_prune_none() ->
    application:unset_env(flurm_dbd, prune_timer_ref),
    ?assertEqual(ok, flurm_dbd_retention:cancel_scheduled_prune()).
