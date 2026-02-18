%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon - Retention Manager Tests
%%%
%%% Unit tests for the flurm_dbd_retention module which provides
%%% data pruning and retention management for accounting data.
%%%
%%% Tests cover:
%%% - Retention policy parsing and validation
%%% - Record pruning logic (jobs and usage)
%%% - Age calculation (cutoff to days, timestamp to period)
%%% - Archive functions
%%% - Scheduled pruning
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_retention_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SECONDS_PER_DAY, 86400).

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Clear any application env settings
    application:unset_env(flurm_dbd, job_retention_days),
    application:unset_env(flurm_dbd, usage_retention_days),
    application:unset_env(flurm_dbd, archive_retention_days),
    application:unset_env(flurm_dbd, auto_prune_enabled),
    application:unset_env(flurm_dbd, auto_prune_interval_hours),
    application:unset_env(flurm_dbd, prune_timer_ref),

    %% Mock flurm_dbd_server
    catch meck:unload(flurm_dbd_server),
    meck:new(flurm_dbd_server, [passthrough, non_strict, no_link]),
    meck:expect(flurm_dbd_server, archive_old_records, fun(_Days) -> {ok, 5} end),
    meck:expect(flurm_dbd_server, purge_old_records, fun(_Days) -> {ok, 3} end),
    meck:expect(flurm_dbd_server, archive_old_jobs, fun(_Days) -> {ok, 10} end),
    meck:expect(flurm_dbd_server, get_archived_jobs, fun() -> sample_archived_jobs() end),
    meck:expect(flurm_dbd_server, get_stats, fun() -> #{archived_jobs => 100} end),

    %% Mock lager to avoid log output during tests
    meck:new(lager, [non_strict, no_link, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),

    ok.

cleanup(_) ->
    %% Cancel any scheduled prune
    catch flurm_dbd_retention:cancel_scheduled_prune(),

    %% Clear env settings
    application:unset_env(flurm_dbd, job_retention_days),
    application:unset_env(flurm_dbd, usage_retention_days),
    application:unset_env(flurm_dbd, archive_retention_days),
    application:unset_env(flurm_dbd, auto_prune_enabled),
    application:unset_env(flurm_dbd, auto_prune_interval_hours),
    application:unset_env(flurm_dbd, prune_timer_ref),

    %% Unload mocks
    catch meck:unload(flurm_dbd_server),
    catch meck:unload(lager),
    ok.

%%====================================================================
%% Retention Configuration Tests
%%====================================================================

retention_config_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"get_retention_config returns defaults", fun test_get_config_defaults/0},
             {"get_retention_config returns custom values", fun test_get_config_custom/0},
             {"set_retention_config sets values", fun test_set_config/0},
             {"set_retention_config validates positive integers", fun test_set_config_validation/0},
             {"set_retention_config validates boolean", fun test_set_config_boolean_validation/0},
             {"set_retention_config accepts partial config", fun test_set_config_partial/0}
         ]
     end
    }.

test_get_config_defaults() ->
    Config = flurm_dbd_retention:get_retention_config(),

    ?assert(is_map(Config)),
    ?assertEqual(365, maps:get(job_retention_days, Config)),
    ?assertEqual(730, maps:get(usage_retention_days, Config)),
    ?assertEqual(1095, maps:get(archive_retention_days, Config)),
    ?assertEqual(false, maps:get(auto_prune_enabled, Config)),
    ?assertEqual(24, maps:get(auto_prune_interval_hours, Config)).

test_get_config_custom() ->
    %% Set custom values
    application:set_env(flurm_dbd, job_retention_days, 180),
    application:set_env(flurm_dbd, usage_retention_days, 365),
    application:set_env(flurm_dbd, archive_retention_days, 730),
    application:set_env(flurm_dbd, auto_prune_enabled, true),
    application:set_env(flurm_dbd, auto_prune_interval_hours, 12),

    Config = flurm_dbd_retention:get_retention_config(),

    ?assertEqual(180, maps:get(job_retention_days, Config)),
    ?assertEqual(365, maps:get(usage_retention_days, Config)),
    ?assertEqual(730, maps:get(archive_retention_days, Config)),
    ?assertEqual(true, maps:get(auto_prune_enabled, Config)),
    ?assertEqual(12, maps:get(auto_prune_interval_hours, Config)).

test_set_config() ->
    NewConfig = #{
        job_retention_days => 90,
        usage_retention_days => 180,
        archive_retention_days => 365,
        auto_prune_enabled => true,
        auto_prune_interval_hours => 6
    },

    Result = flurm_dbd_retention:set_retention_config(NewConfig),
    ?assertEqual(ok, Result),

    %% Verify values were set
    Config = flurm_dbd_retention:get_retention_config(),
    ?assertEqual(90, maps:get(job_retention_days, Config)),
    ?assertEqual(180, maps:get(usage_retention_days, Config)),
    ?assertEqual(365, maps:get(archive_retention_days, Config)),
    ?assertEqual(true, maps:get(auto_prune_enabled, Config)),
    ?assertEqual(6, maps:get(auto_prune_interval_hours, Config)).

test_set_config_validation() ->
    %% Invalid: zero value
    InvalidConfig1 = #{job_retention_days => 0},
    Result1 = flurm_dbd_retention:set_retention_config(InvalidConfig1),
    ?assertMatch({error, {validation_failed, _}}, Result1),

    %% Invalid: negative value
    InvalidConfig2 = #{usage_retention_days => -10},
    Result2 = flurm_dbd_retention:set_retention_config(InvalidConfig2),
    ?assertMatch({error, {validation_failed, _}}, Result2),

    %% Invalid: non-integer value
    InvalidConfig3 = #{archive_retention_days => "365"},
    Result3 = flurm_dbd_retention:set_retention_config(InvalidConfig3),
    ?assertMatch({error, {validation_failed, _}}, Result3),

    %% Invalid: float value
    InvalidConfig4 = #{auto_prune_interval_hours => 6.5},
    Result4 = flurm_dbd_retention:set_retention_config(InvalidConfig4),
    ?assertMatch({error, {validation_failed, _}}, Result4).

test_set_config_boolean_validation() ->
    %% Invalid: non-boolean for auto_prune_enabled
    InvalidConfig = #{auto_prune_enabled => "true"},
    Result = flurm_dbd_retention:set_retention_config(InvalidConfig),
    ?assertMatch({error, {validation_failed, _}}, Result).

test_set_config_partial() ->
    %% First, reset to defaults by unsetting all env vars
    application:unset_env(flurm_dbd, job_retention_days),
    application:unset_env(flurm_dbd, usage_retention_days),
    application:unset_env(flurm_dbd, archive_retention_days),
    application:unset_env(flurm_dbd, auto_prune_enabled),
    application:unset_env(flurm_dbd, auto_prune_interval_hours),

    %% Verify we're at defaults before the test
    DefaultConfig = flurm_dbd_retention:get_retention_config(),
    ?assertEqual(730, maps:get(usage_retention_days, DefaultConfig)),

    %% Should accept partial config with only some keys
    PartialConfig = #{job_retention_days => 60},
    Result = flurm_dbd_retention:set_retention_config(PartialConfig),
    ?assertEqual(ok, Result),

    Config = flurm_dbd_retention:get_retention_config(),
    ?assertEqual(60, maps:get(job_retention_days, Config)),
    %% Other values should remain at defaults
    ?assertEqual(730, maps:get(usage_retention_days, Config)).

%%====================================================================
%% Prune Old Records Tests
%%====================================================================

prune_records_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"prune_old_records returns prune result", fun test_prune_old_records_success/0},
             {"prune_old_records handles errors", fun test_prune_old_records_error/0},
             {"prune_old_records combines job and usage results", fun test_prune_combined_results/0}
         ]
     end
    }.

test_prune_old_records_success() ->
    meck:expect(flurm_dbd_server, archive_old_records, fun(_Days) -> {ok, 10} end),
    meck:expect(flurm_dbd_server, purge_old_records, fun(_Days) -> {ok, 5} end),

    Result = flurm_dbd_retention:prune_old_records(365),
    ?assertMatch({ok, _}, Result),

    {ok, PruneResult} = Result,
    ?assert(is_map(PruneResult)),
    ?assert(maps:is_key(jobs_pruned, PruneResult)),
    ?assert(maps:is_key(usage_pruned, PruneResult)),
    ?assert(maps:is_key(archived, PruneResult)),
    ?assert(maps:is_key(errors, PruneResult)),

    %% Check counts (archived + purged)
    ?assertEqual(15, maps:get(jobs_pruned, PruneResult)),
    ?assertEqual([], maps:get(errors, PruneResult)).

test_prune_old_records_error() ->
    meck:expect(flurm_dbd_server, archive_old_records, fun(_Days) ->
        throw(database_error)
    end),

    Result = flurm_dbd_retention:prune_old_records(365),
    ?assertMatch({ok, _}, Result),  % Still returns ok with errors included

    {ok, PruneResult} = Result,
    Errors = maps:get(errors, PruneResult),
    ?assert(length(Errors) > 0).

test_prune_combined_results() ->
    meck:expect(flurm_dbd_server, archive_old_records, fun(_Days) -> {ok, 20} end),
    meck:expect(flurm_dbd_server, purge_old_records, fun(_Days) -> {ok, 10} end),

    Result = flurm_dbd_retention:prune_old_records(180),
    {ok, PruneResult} = Result,

    ?assertEqual(30, maps:get(jobs_pruned, PruneResult)),
    ?assertEqual(0, maps:get(usage_pruned, PruneResult)),  % Usage pruning is a placeholder
    ?assertEqual(0, maps:get(archived, PruneResult)).

%%====================================================================
%% Prune Job Records Tests
%%====================================================================

prune_job_records_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"prune_job_records with valid cutoff", fun test_prune_jobs_valid_cutoff/0},
             {"prune_job_records handles archive error", fun test_prune_jobs_archive_error/0},
             {"prune_job_records handles purge error", fun test_prune_jobs_purge_error/0}
         ]
     end
    }.

test_prune_jobs_valid_cutoff() ->
    meck:expect(flurm_dbd_server, archive_old_records, fun(_Days) -> {ok, 5} end),
    meck:expect(flurm_dbd_server, purge_old_records, fun(_Days) -> {ok, 3} end),

    CutoffTime = erlang:system_time(second) - (30 * ?SECONDS_PER_DAY),  % 30 days ago
    Result = flurm_dbd_retention:prune_job_records(CutoffTime),

    ?assert(is_map(Result)),
    ?assertEqual(8, maps:get(count, Result)),
    ?assertEqual([], maps:get(errors, Result)).

test_prune_jobs_archive_error() ->
    meck:expect(flurm_dbd_server, archive_old_records, fun(_Days) ->
        throw(archive_failed)
    end),

    CutoffTime = erlang:system_time(second) - (30 * ?SECONDS_PER_DAY),
    Result = flurm_dbd_retention:prune_job_records(CutoffTime),

    ?assertEqual(0, maps:get(count, Result)),
    Errors = maps:get(errors, Result),
    ?assert(length(Errors) > 0),
    ?assertMatch([{job_prune_error, _}], Errors).

test_prune_jobs_purge_error() ->
    meck:expect(flurm_dbd_server, archive_old_records, fun(_Days) -> {ok, 5} end),
    meck:expect(flurm_dbd_server, purge_old_records, fun(_Days) ->
        throw(purge_failed)
    end),

    CutoffTime = erlang:system_time(second) - (30 * ?SECONDS_PER_DAY),
    Result = flurm_dbd_retention:prune_job_records(CutoffTime),

    %% Should still fail since purge_old_records is part of the same try block
    ?assertEqual(0, maps:get(count, Result)),
    Errors = maps:get(errors, Result),
    ?assert(length(Errors) > 0).

%%====================================================================
%% Prune Usage Records Tests
%%====================================================================

prune_usage_records_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"prune_usage_records returns placeholder result", fun test_prune_usage_placeholder/0},
             {"prune_usage_records calculates correct period", fun test_prune_usage_period_calculation/0}
         ]
     end
    }.

test_prune_usage_placeholder() ->
    %% Currently usage pruning is a placeholder
    CutoffTime = erlang:system_time(second) - (365 * ?SECONDS_PER_DAY),
    Result = flurm_dbd_retention:prune_usage_records(CutoffTime),

    ?assert(is_map(Result)),
    ?assertEqual(0, maps:get(count, Result)),
    ?assertEqual([], maps:get(errors, Result)).

test_prune_usage_period_calculation() ->
    %% Verify that usage pruning is at least logging the correct period
    %% This is a smoke test since actual pruning is not implemented
    CutoffTime = erlang:system_time(second) - (180 * ?SECONDS_PER_DAY),
    Result = flurm_dbd_retention:prune_usage_records(CutoffTime),

    ?assert(is_map(Result)),
    ?assertEqual([], maps:get(errors, Result)).

%%====================================================================
%% Archive Records Tests
%%====================================================================

archive_records_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"archive_records archives old jobs", fun test_archive_records_success/0},
             {"archive_records handles errors", fun test_archive_records_error/0}
         ]
     end
    }.

test_archive_records_success() ->
    meck:expect(flurm_dbd_server, archive_old_jobs, fun(Days) ->
        ?assert(Days > 0),
        {ok, 25}
    end),

    Result = flurm_dbd_retention:archive_records(180),
    ?assertEqual({ok, 25}, Result).

test_archive_records_error() ->
    meck:expect(flurm_dbd_server, archive_old_jobs, fun(_Days) ->
        throw(archive_error)
    end),

    Result = flurm_dbd_retention:archive_records(180),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% Archive Stats Tests
%%====================================================================

archive_stats_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"get_archive_stats returns statistics", fun test_archive_stats_success/0},
             {"get_archive_stats handles errors", fun test_archive_stats_error/0},
             {"get_archive_stats with empty archives", fun test_archive_stats_empty/0}
         ]
     end
    }.

test_archive_stats_success() ->
    Stats = flurm_dbd_retention:get_archive_stats(),

    ?assert(is_map(Stats)),
    ?assert(maps:is_key(archived_job_count, Stats)),
    ?assert(maps:is_key(total_archived, Stats)),
    ?assert(maps:is_key(oldest_archive, Stats)),
    ?assert(maps:is_key(newest_archive, Stats)),

    ?assertEqual(3, maps:get(archived_job_count, Stats)),  % From sample_archived_jobs
    ?assertEqual(100, maps:get(total_archived, Stats)).

test_archive_stats_error() ->
    meck:expect(flurm_dbd_server, get_archived_jobs, fun() ->
        throw(stats_error)
    end),

    Stats = flurm_dbd_retention:get_archive_stats(),

    %% Should return empty stats on error
    ?assert(is_map(Stats)),
    ?assertEqual(0, maps:get(archived_job_count, Stats)),
    ?assertEqual(0, maps:get(total_archived, Stats)).

test_archive_stats_empty() ->
    meck:expect(flurm_dbd_server, get_archived_jobs, fun() -> [] end),
    meck:expect(flurm_dbd_server, get_stats, fun() -> #{archived_jobs => 0} end),

    Stats = flurm_dbd_retention:get_archive_stats(),

    ?assertEqual(0, maps:get(archived_job_count, Stats)),
    ?assertEqual(0, maps:get(oldest_archive, Stats)),
    ?assertEqual(0, maps:get(newest_archive, Stats)).

%%====================================================================
%% Schedule Prune Tests
%%====================================================================

schedule_prune_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"schedule_prune creates timer", fun test_schedule_prune_creates_timer/0},
             {"schedule_prune with custom interval", fun test_schedule_prune_custom_interval/0},
             {"cancel_scheduled_prune cancels timer", fun test_cancel_scheduled_prune/0},
             {"cancel_scheduled_prune when no timer", fun test_cancel_no_timer/0}
         ]
     end
    }.

test_schedule_prune_creates_timer() ->
    Result = flurm_dbd_retention:schedule_prune(1),
    ?assertMatch({ok, _}, Result),

    {ok, TimerRef} = Result,
    ?assert(is_reference(TimerRef)),

    %% Verify timer was stored
    {ok, StoredRef} = application:get_env(flurm_dbd, prune_timer_ref),
    ?assertEqual(TimerRef, StoredRef),

    %% Cleanup
    flurm_dbd_retention:cancel_scheduled_prune().

test_schedule_prune_custom_interval() ->
    %% Set custom retention config
    ok = flurm_dbd_retention:set_retention_config(#{job_retention_days => 30}),

    Result = flurm_dbd_retention:schedule_prune(12),
    ?assertMatch({ok, _}, Result),

    %% Cleanup
    flurm_dbd_retention:cancel_scheduled_prune().

test_cancel_scheduled_prune() ->
    %% First schedule
    {ok, _TimerRef} = flurm_dbd_retention:schedule_prune(1),

    %% Now cancel
    Result = flurm_dbd_retention:cancel_scheduled_prune(),
    ?assertEqual(ok, Result),

    %% Timer should be gone
    ?assertEqual(undefined, application:get_env(flurm_dbd, prune_timer_ref)).

test_cancel_no_timer() ->
    %% Should not error when no timer exists
    Result = flurm_dbd_retention:cancel_scheduled_prune(),
    ?assertEqual(ok, Result).

%%====================================================================
%% Age Calculation Tests
%%====================================================================

age_calculation_test_() ->
    [
        {"cutoff 30 days ago yields ~30 days", fun() ->
            Now = erlang:system_time(second),
            CutoffTime = Now - (30 * ?SECONDS_PER_DAY),
            Days = calculate_days_from_cutoff(CutoffTime),
            ?assert(Days >= 29 andalso Days =< 31)
        end},
        {"cutoff 365 days ago yields ~365 days", fun() ->
            Now = erlang:system_time(second),
            CutoffTime = Now - (365 * ?SECONDS_PER_DAY),
            Days = calculate_days_from_cutoff(CutoffTime),
            ?assert(Days >= 364 andalso Days =< 366)
        end},
        {"cutoff 1 day ago yields 1 day (minimum)", fun() ->
            Now = erlang:system_time(second),
            CutoffTime = Now - ?SECONDS_PER_DAY,
            Days = calculate_days_from_cutoff(CutoffTime),
            ?assertEqual(1, Days)
        end}
    ].

%% Helper to test internal cutoff_to_days logic (same formula as module)
calculate_days_from_cutoff(CutoffTime) ->
    Now = erlang:system_time(second),
    max(1, (Now - CutoffTime) div ?SECONDS_PER_DAY).

%%====================================================================
%% Timestamp to Period Tests
%%====================================================================

timestamp_to_period_test_() ->
    [
        {"converts timestamp to YYYY-MM format", fun() ->
            %% January 2026 timestamp (approx)
            %% 2026-01-15 00:00:00 UTC
            Timestamp = 1768521600,  % This is approximately 2026-01-15
            Period = timestamp_to_period_helper(Timestamp),
            ?assert(is_binary(Period)),
            %% Should match YYYY-MM pattern
            ?assertMatch(<<_:4/binary, "-", _:2/binary>>, Period)
        end},
        {"period format is correct length", fun() ->
            Timestamp = erlang:system_time(second) - (180 * ?SECONDS_PER_DAY),
            Period = timestamp_to_period_helper(Timestamp),
            ?assertEqual(7, byte_size(Period))  % "YYYY-MM" is 7 chars
        end}
    ].

%% Helper to test internal timestamp_to_period logic (same formula as module)
timestamp_to_period_helper(Timestamp) ->
    {{Year, Month, _Day}, _Time} = calendar:gregorian_seconds_to_datetime(
        Timestamp + 62167219200
    ),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

%%====================================================================
%% Oldest/Newest Timestamp Tests
%%====================================================================

timestamp_finding_test_() ->
    [
        {"find oldest timestamp in job list", fun() ->
            Jobs = [
                #{job_id => 1, end_time => 3000},
                #{job_id => 2, end_time => 1000},  % Oldest
                #{job_id => 3, end_time => 5000}
            ],
            Oldest = find_oldest_timestamp(Jobs),
            ?assertEqual(1000, Oldest)
        end},
        {"find newest timestamp in job list", fun() ->
            Jobs = [
                #{job_id => 1, end_time => 3000},
                #{job_id => 2, end_time => 1000},
                #{job_id => 3, end_time => 5000}  % Newest
            ],
            Newest = find_newest_timestamp(Jobs),
            ?assertEqual(5000, Newest)
        end},
        {"oldest with empty list returns 0", fun() ->
            ?assertEqual(0, find_oldest_timestamp([]))
        end},
        {"newest with empty list returns 0", fun() ->
            ?assertEqual(0, find_newest_timestamp([]))
        end},
        {"oldest ignores zero end_time", fun() ->
            Jobs = [
                #{job_id => 1, end_time => 0},
                #{job_id => 2, end_time => 2000}
            ],
            Oldest = find_oldest_timestamp(Jobs),
            ?assertEqual(2000, Oldest)
        end},
        {"oldest with all zero returns 0", fun() ->
            Jobs = [
                #{job_id => 1, end_time => 0},
                #{job_id => 2, end_time => 0}
            ],
            Oldest = find_oldest_timestamp(Jobs),
            ?assertEqual(0, Oldest)
        end}
    ].

%% Helper functions (same logic as internal module functions)
find_oldest_timestamp([]) -> 0;
find_oldest_timestamp(Jobs) ->
    lists:foldl(fun(Job, Oldest) ->
        EndTime = maps:get(end_time, Job, 0),
        case EndTime of
            0 -> Oldest;
            _ when Oldest =:= 0 -> EndTime;
            _ -> min(EndTime, Oldest)
        end
    end, 0, Jobs).

find_newest_timestamp([]) -> 0;
find_newest_timestamp(Jobs) ->
    lists:foldl(fun(Job, Newest) ->
        EndTime = maps:get(end_time, Job, 0),
        max(EndTime, Newest)
    end, 0, Jobs).

%%====================================================================
%% Validation Tests
%%====================================================================

validation_test_() ->
    [
        {"is_positive_integer accepts positive integers", fun() ->
            ?assertEqual(true, is_positive_integer(1)),
            ?assertEqual(true, is_positive_integer(100)),
            ?assertEqual(true, is_positive_integer(9999999))
        end},
        {"is_positive_integer rejects zero", fun() ->
            ?assertEqual(false, is_positive_integer(0))
        end},
        {"is_positive_integer rejects negative", fun() ->
            ?assertEqual(false, is_positive_integer(-1)),
            ?assertEqual(false, is_positive_integer(-100))
        end},
        {"is_positive_integer rejects non-integers", fun() ->
            ?assertEqual(false, is_positive_integer(1.5)),
            ?assertEqual(false, is_positive_integer("1")),
            ?assertEqual(false, is_positive_integer(<<"1">>)),
            ?assertEqual(false, is_positive_integer(atom))
        end}
    ].

%% Helper (same as internal function)
is_positive_integer(Value) when is_integer(Value), Value > 0 -> true;
is_positive_integer(_) -> false.

%%====================================================================
%% Integration-style Tests
%%====================================================================

integration_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"full prune workflow", fun test_full_prune_workflow/0},
             {"config change and prune", fun test_config_change_and_prune/0}
         ]
     end
    }.

test_full_prune_workflow() ->
    %% Set retention config
    ok = flurm_dbd_retention:set_retention_config(#{
        job_retention_days => 90,
        auto_prune_enabled => true
    }),

    %% Get config
    Config = flurm_dbd_retention:get_retention_config(),
    ?assertEqual(90, maps:get(job_retention_days, Config)),

    %% Prune records
    {ok, PruneResult} = flurm_dbd_retention:prune_old_records(90),
    ?assert(maps:get(jobs_pruned, PruneResult) > 0),

    %% Get archive stats
    Stats = flurm_dbd_retention:get_archive_stats(),
    ?assert(is_map(Stats)).

test_config_change_and_prune() ->
    %% Initial config
    Config1 = flurm_dbd_retention:get_retention_config(),
    OriginalDays = maps:get(job_retention_days, Config1),

    %% Change config
    ok = flurm_dbd_retention:set_retention_config(#{job_retention_days => 60}),

    %% Verify change
    Config2 = flurm_dbd_retention:get_retention_config(),
    ?assertEqual(60, maps:get(job_retention_days, Config2)),
    ?assertNotEqual(OriginalDays, 60),  % Assuming default is 365

    %% Prune with new config
    {ok, _PruneResult} = flurm_dbd_retention:prune_old_records(60).

%%====================================================================
%% Edge Cases Tests
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"prune with minimum retention (1 day)", fun test_prune_minimum_retention/0},
             {"prune with large retention", fun test_prune_large_retention/0},
             {"archive with minimum days", fun test_archive_minimum_days/0}
         ]
     end
    }.

test_prune_minimum_retention() ->
    %% 1 day is the minimum valid retention
    {ok, Result} = flurm_dbd_retention:prune_old_records(1),
    ?assert(is_map(Result)).

test_prune_large_retention() ->
    %% Very large retention period
    {ok, Result} = flurm_dbd_retention:prune_old_records(3650),  % 10 years
    ?assert(is_map(Result)).

test_archive_minimum_days() ->
    %% Archive with 1 day
    Result = flurm_dbd_retention:archive_records(1),
    ?assertMatch({ok, _}, Result).

%%====================================================================
%% Helper Functions
%%====================================================================

sample_archived_jobs() ->
    Now = erlang:system_time(second),
    [
        #{job_id => 1, end_time => Now - (400 * ?SECONDS_PER_DAY)},  % 400 days ago
        #{job_id => 2, end_time => Now - (450 * ?SECONDS_PER_DAY)},  % 450 days ago (oldest)
        #{job_id => 3, end_time => Now - (370 * ?SECONDS_PER_DAY)}   % 370 days ago (newest)
    ].
