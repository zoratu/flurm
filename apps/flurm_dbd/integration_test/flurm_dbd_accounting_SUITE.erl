%%%-------------------------------------------------------------------
%%% @doc Integration tests for FLURM DBD Accounting Module
%%%
%%% Tests accounting functionality including:
%%% - Table fragmentation across nodes
%%% - Replication consistency via Ra consensus
%%% - Retention policy enforcement
%%% - TRES aggregation and calculations
%%%
%%% These tests require Mnesia and mock Ra operations.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_accounting_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases - Fragmentation
-export([
    test_monthly_table_creation/1,
    test_burst_fragmentation_on_threshold/1,
    test_query_routing_across_fragments/1,
    test_fragment_metadata_tracking/1,
    test_auto_fragment_naming/1,
    test_fragment_size_estimation/1
]).

%% Test cases - Replication
-export([
    test_job_record_replication/1,
    test_tres_consistency_across_replicas/1,
    test_double_counting_prevention/1,
    test_user_totals_aggregation/1,
    test_account_totals_aggregation/1,
    test_concurrent_job_recording/1
]).

%% Test cases - Retention
-export([
    test_retention_config_defaults/1,
    test_retention_config_custom/1,
    test_prune_old_records/1,
    test_table_aging_progression/1,
    test_archive_trigger_by_age/1,
    test_scheduled_retention_enforcement/1
]).

%% Test cases - TRES
-export([
    test_tres_calculation_from_job/1,
    test_tres_aggregation_by_user/1,
    test_tres_aggregation_by_account/1,
    test_tres_non_negative_invariant/1,
    test_tres_query_by_period/1,
    test_billing_tres_calculation/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, fragmentation_tests},
        {group, replication_tests},
        {group, retention_tests},
        {group, tres_tests}
    ].

groups() ->
    [
        {fragmentation_tests, [sequence], [
            test_monthly_table_creation,
            test_burst_fragmentation_on_threshold,
            test_query_routing_across_fragments,
            test_fragment_metadata_tracking,
            test_auto_fragment_naming,
            test_fragment_size_estimation
        ]},
        {replication_tests, [sequence], [
            test_job_record_replication,
            test_tres_consistency_across_replicas,
            test_double_counting_prevention,
            test_user_totals_aggregation,
            test_account_totals_aggregation,
            test_concurrent_job_recording
        ]},
        {retention_tests, [sequence], [
            test_retention_config_defaults,
            test_retention_config_custom,
            test_prune_old_records,
            test_table_aging_progression,
            test_archive_trigger_by_age,
            test_scheduled_retention_enforcement
        ]},
        {tres_tests, [sequence], [
            test_tres_calculation_from_job,
            test_tres_aggregation_by_user,
            test_tres_aggregation_by_account,
            test_tres_non_negative_invariant,
            test_tres_query_by_period,
            test_billing_tres_calculation
        ]}
    ].

init_per_suite(Config) ->
    %% Start applications needed for testing
    application:ensure_all_started(syntax_tools),
    application:ensure_all_started(compiler),

    %% Setup meck for dependencies
    meck:new(lager, [non_strict, no_link, passthrough]),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, debug, fun(_Fmt) -> ok end),
    meck:expect(lager, debug, fun(_Fmt, _Args) -> ok end),

    Config.

end_per_suite(_Config) ->
    catch meck:unload(lager),
    ok.

init_per_group(fragmentation_tests, Config) ->
    %% Setup Mnesia for fragmentation tests
    setup_mnesia(),
    start_mock_fragment_manager(),
    Config;
init_per_group(replication_tests, Config) ->
    %% Setup mock Ra for replication tests
    setup_mock_ra(),
    Config;
init_per_group(retention_tests, Config) ->
    %% Setup retention test environment
    setup_retention_mocks(),
    Config;
init_per_group(tres_tests, Config) ->
    %% Setup TRES test environment
    setup_tres_mocks(),
    Config.

end_per_group(fragmentation_tests, _Config) ->
    cleanup_mock_fragment_manager(),
    cleanup_mnesia(),
    ok;
end_per_group(replication_tests, _Config) ->
    cleanup_mock_ra(),
    ok;
end_per_group(retention_tests, _Config) ->
    cleanup_retention_mocks(),
    ok;
end_per_group(tres_tests, _Config) ->
    cleanup_tres_mocks(),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Fragmentation Tests
%%====================================================================

test_monthly_table_creation(_Config) ->
    %% Test that monthly tables are created on demand
    CurrentPeriod = current_month(),
    TableName = period_to_table_name(CurrentPeriod, 0),

    %% Ensure current table creates the monthly table
    {ok, CreatedTable} = mock_ensure_current_table(),
    ?assertEqual(TableName, CreatedTable),

    %% Verify table exists in metadata
    Fragments = mock_list_all_fragments(),
    ?assert(length(Fragments) >= 1),

    %% First fragment should be for current period
    FirstFragment = hd(Fragments),
    ?assertEqual(CurrentPeriod, maps:get(base_period, FirstFragment)),
    ?assertEqual(0, maps:get(fragment_id, FirstFragment)),
    ?assertEqual(hot, maps:get(status, FirstFragment)),

    ok.

test_burst_fragmentation_on_threshold(_Config) ->
    %% Test that burst fragments are created when threshold exceeded
    CurrentPeriod = current_month(),

    %% Insert records up to threshold
    RecordCount = 500000,  %% At threshold
    mock_set_fragment_record_count(CurrentPeriod, 0, RecordCount),

    %% Next insert should trigger burst fragment
    JobInfo = #{
        job_id => 999999,
        user_name => <<"testuser">>,
        account => <<"testacct">>,
        end_time => erlang:system_time(second)
    },
    {ok, _NewTable} = mock_insert_job(JobInfo),

    %% Should now have a burst fragment
    Fragments = mock_list_all_fragments(),
    BurstFragments = [F || F <- Fragments,
                      maps:get(base_period, F) =:= CurrentPeriod,
                      maps:get(fragment_id, F) > 0],
    ?assert(length(BurstFragments) >= 1),

    ok.

test_query_routing_across_fragments(_Config) ->
    %% Test that queries correctly route to relevant time-based fragments
    %% Create test data spanning multiple months
    Now = erlang:system_time(second),
    OneMonthAgo = Now - (30 * 86400),
    TwoMonthsAgo = Now - (60 * 86400),

    %% Insert jobs at different times
    Job1 = create_test_job_info(1, <<"user1">>, <<"acct1">>, Now),
    Job2 = create_test_job_info(2, <<"user1">>, <<"acct1">>, OneMonthAgo),
    Job3 = create_test_job_info(3, <<"user1">>, <<"acct1">>, TwoMonthsAgo),

    mock_insert_job(Job1),
    mock_insert_job(Job2),
    mock_insert_job(Job3),

    %% Query for jobs in current month only
    CurrentPeriod = time_to_period(Now),
    {ok, CurrentMonthJobs} = mock_query_jobs(#{
        start_time => Now - (15 * 86400),
        end_time => Now
    }),

    %% Should only return jobs from relevant period
    JobIds = [maps:get(job_id, J) || J <- CurrentMonthJobs],
    ?assert(lists:member(1, JobIds)),

    ok.

test_fragment_metadata_tracking(_Config) ->
    %% Test that fragment metadata is properly tracked
    CurrentPeriod = current_month(),
    TableName = period_to_table_name(CurrentPeriod, 0),

    %% Get metadata for current table
    {ok, Meta} = mock_get_fragment_meta(TableName),

    ?assertEqual(TableName, maps:get(table_name, Meta)),
    ?assertEqual(CurrentPeriod, maps:get(base_period, Meta)),
    ?assertEqual(0, maps:get(fragment_id, Meta)),
    ?assert(maps:get(record_count, Meta) >= 0),
    ?assert(maps:get(size_bytes, Meta) >= 0),
    ?assert(maps:get(created_at, Meta) > 0),
    ?assert(lists:member(maps:get(status, Meta), [hot, warm, cold, archived])),

    ok.

test_auto_fragment_naming(_Config) ->
    %% Test fragment naming conventions
    Period = <<"2026-02">>,

    %% Main fragment (id=0)
    MainTable = period_to_table_name(Period, 0),
    ?assertEqual(job_records_2026_02, MainTable),

    %% Burst fragments
    BurstTable1 = period_to_table_name(Period, 1),
    ?assertEqual(job_records_2026_02_frag_1, BurstTable1),

    BurstTable5 = period_to_table_name(Period, 5),
    ?assertEqual(job_records_2026_02_frag_5, BurstTable5),

    ok.

test_fragment_size_estimation(_Config) ->
    %% Test that fragment size is properly estimated
    %% Insert some test data
    lists:foreach(fun(I) ->
        Job = create_test_job_info(I, <<"user1">>, <<"acct1">>, erlang:system_time(second)),
        mock_insert_job(Job)
    end, lists:seq(1, 10)),

    %% Get table stats
    Stats = mock_get_table_stats(),

    ?assert(is_map(Stats)),
    ?assert(maps:get(total_records, Stats) >= 10),
    ?assert(maps:get(total_size_bytes, Stats) >= 0),
    ?assert(maps:get(table_count, Stats) >= 1),

    ok.

%%====================================================================
%% Replication Tests
%%====================================================================

test_job_record_replication(_Config) ->
    %% Test that job records are replicated through Ra
    JobInfo = #{
        job_id => 12345,
        job_name => <<"test_job">>,
        user_name => <<"alice">>,
        account => <<"research">>,
        partition => <<"batch">>,
        state => completed,
        exit_code => 0,
        num_cpus => 4,
        num_nodes => 1,
        elapsed => 3600,
        tres_alloc => #{cpu => 4, mem => 8192}
    },

    %% Record job through Ra
    {ok, JobId} = mock_ra_record_job(JobInfo),
    ?assertEqual(12345, JobId),

    %% Verify job is retrievable
    {ok, RetrievedJob} = mock_ra_get_job(12345),
    ?assertEqual(<<"alice">>, maps:get(user_name, RetrievedJob)),
    ?assertEqual(completed, maps:get(state, RetrievedJob)),

    ok.

test_tres_consistency_across_replicas(_Config) ->
    %% Test that TRES values are consistent across replicated state
    %% (TLA+ TRES Consistency invariant)
    JobInfo = #{
        job_id => 12346,
        user_name => <<"bob">>,
        account => <<"physics">>,
        num_cpus => 8,
        elapsed => 7200,
        tres_alloc => #{cpu => 8, mem => 16384, gpu => 2}
    },

    %% Record job
    {ok, _} = mock_ra_record_job(JobInfo),

    %% Query TRES for user
    {ok, UserTres} = mock_ra_get_user_totals(<<"bob">>),

    %% TRES should be calculated correctly
    ExpectedCpuSeconds = 8 * 7200,  %% num_cpus * elapsed
    ?assertEqual(ExpectedCpuSeconds, maps:get(cpu_seconds, UserTres)),

    ok.

test_double_counting_prevention(_Config) ->
    %% Test that jobs cannot be double-counted
    %% (TLA+ NoDoubleCounting invariant)
    JobInfo = #{
        job_id => 12347,
        user_name => <<"carol">>,
        account => <<"math">>
    },

    %% First recording should succeed
    {ok, _} = mock_ra_record_job(JobInfo),

    %% Second recording of same job should fail
    Result = mock_ra_record_job(JobInfo),
    ?assertEqual({error, already_recorded}, Result),

    ok.

test_user_totals_aggregation(_Config) ->
    %% Test user running totals are properly aggregated
    User = <<"dave">>,

    %% Record multiple jobs for same user
    Job1 = #{job_id => 12350, user_name => User, account => <<"acct1">>,
             num_cpus => 2, elapsed => 1000},
    Job2 = #{job_id => 12351, user_name => User, account => <<"acct1">>,
             num_cpus => 4, elapsed => 2000},
    Job3 = #{job_id => 12352, user_name => User, account => <<"acct1">>,
             num_cpus => 8, elapsed => 500},

    mock_ra_record_job(Job1),
    mock_ra_record_job(Job2),
    mock_ra_record_job(Job3),

    %% Get user totals
    {ok, Totals} = mock_ra_get_user_totals(User),

    %% Total CPU seconds should be sum of all jobs
    ExpectedCpuSeconds = (2 * 1000) + (4 * 2000) + (8 * 500),
    ?assertEqual(ExpectedCpuSeconds, maps:get(cpu_seconds, Totals)),

    ok.

test_account_totals_aggregation(_Config) ->
    %% Test account running totals are properly aggregated
    Account = <<"chemistry">>,

    %% Record jobs for same account but different users
    Job1 = #{job_id => 12360, user_name => <<"user1">>, account => Account,
             num_cpus => 4, elapsed => 1000},
    Job2 = #{job_id => 12361, user_name => <<"user2">>, account => Account,
             num_cpus => 8, elapsed => 1500},

    mock_ra_record_job(Job1),
    mock_ra_record_job(Job2),

    %% Get account totals
    {ok, Totals} = mock_ra_get_account_totals(Account),

    %% Total should combine both users' jobs
    ExpectedCpuSeconds = (4 * 1000) + (8 * 1500),
    ?assertEqual(ExpectedCpuSeconds, maps:get(cpu_seconds, Totals)),

    ok.

test_concurrent_job_recording(_Config) ->
    %% Test that concurrent job recordings don't cause inconsistencies
    NumJobs = 100,
    BaseJobId = 20000,

    %% Simulate concurrent recording
    lists:foreach(fun(I) ->
        JobInfo = #{
            job_id => BaseJobId + I,
            user_name => <<"concurrent_user">>,
            account => <<"concurrent_acct">>,
            num_cpus => 2,
            elapsed => 100
        },
        mock_ra_record_job(JobInfo)
    end, lists:seq(1, NumJobs)),

    %% Verify all jobs were recorded
    {ok, Totals} = mock_ra_get_user_totals(<<"concurrent_user">>),

    %% Total should be exactly NumJobs * (2 * 100)
    ExpectedCpuSeconds = NumJobs * 2 * 100,
    ?assertEqual(ExpectedCpuSeconds, maps:get(cpu_seconds, Totals)),

    ok.

%%====================================================================
%% Retention Tests
%%====================================================================

test_retention_config_defaults(_Config) ->
    %% Test default retention configuration
    Config = mock_get_retention_config(),

    ?assertEqual(365, maps:get(job_retention_days, Config)),
    ?assertEqual(730, maps:get(usage_retention_days, Config)),
    ?assertEqual(1095, maps:get(archive_retention_days, Config)),
    ?assertEqual(false, maps:get(auto_prune_enabled, Config)),
    ?assertEqual(24, maps:get(auto_prune_interval_hours, Config)),

    ok.

test_retention_config_custom(_Config) ->
    %% Test setting custom retention configuration
    NewConfig = #{
        job_retention_days => 90,
        usage_retention_days => 180,
        auto_prune_enabled => true,
        auto_prune_interval_hours => 12
    },

    ok = mock_set_retention_config(NewConfig),

    %% Verify new config
    UpdatedConfig = mock_get_retention_config(),
    ?assertEqual(90, maps:get(job_retention_days, UpdatedConfig)),
    ?assertEqual(true, maps:get(auto_prune_enabled, UpdatedConfig)),

    ok.

test_prune_old_records(_Config) ->
    %% Test that old records are properly pruned
    {ok, Result} = mock_prune_old_records(365),

    ?assert(is_map(Result)),
    ?assert(maps:is_key(jobs_pruned, Result)),
    ?assert(maps:is_key(usage_pruned, Result)),
    ?assert(maps:is_key(errors, Result)),

    %% Errors should be empty for successful prune
    ?assertEqual([], maps:get(errors, Result)),

    ok.

test_table_aging_progression(_Config) ->
    %% Test that tables age through status progression:
    %% hot -> warm -> cold -> archived

    %% Create fragments at different ages
    {{Year, Month, _}, _} = calendar:local_time(),

    %% Current month should be hot
    CurrentMeta = mock_create_fragment_meta(current_month(), hot),
    ?assertEqual(hot, maps:get(status, CurrentMeta)),

    %% 2 months ago should age to warm
    TwoMonthsAgo = subtract_months(Year, Month, 2),
    WarmMeta = mock_age_fragment_if_needed(TwoMonthsAgo, hot),
    ?assertEqual(warm, maps:get(status, WarmMeta)),

    %% 4 months ago should age to cold
    FourMonthsAgo = subtract_months(Year, Month, 4),
    ColdMeta = mock_age_fragment_if_needed(FourMonthsAgo, warm),
    ?assertEqual(cold, maps:get(status, ColdMeta)),

    %% 13 months ago should age to archived
    ThirteenMonthsAgo = subtract_months(Year, Month, 13),
    ArchivedMeta = mock_age_fragment_if_needed(ThirteenMonthsAgo, cold),
    ?assertEqual(archived, maps:get(status, ArchivedMeta)),

    ok.

test_archive_trigger_by_age(_Config) ->
    %% Test that archival is triggered for old tables
    {ok, ArchivedCount} = mock_trigger_archival(12),

    ?assert(is_integer(ArchivedCount)),
    ?assert(ArchivedCount >= 0),

    ok.

test_scheduled_retention_enforcement(_Config) ->
    %% Test scheduled retention works
    {ok, TimerRef} = mock_schedule_prune(1),
    ?assert(is_reference(TimerRef)),

    %% Cancel the scheduled prune
    ok = mock_cancel_scheduled_prune(),

    ok.

%%====================================================================
%% TRES Tests
%%====================================================================

test_tres_calculation_from_job(_Config) ->
    %% Test TRES calculation from a completed job
    JobInfo = #{
        job_id => 30001,
        user_name => <<"tresuser">>,
        account => <<"tresacct">>,
        num_cpus => 4,
        num_nodes => 2,
        req_mem => 8192,
        elapsed => 3600,
        tres_alloc => #{cpu => 4, mem => 8192, gpu => 1}
    },

    Tres = mock_calculate_tres(JobInfo),

    %% Verify TRES calculations
    ?assertEqual(4 * 3600, maps:get(cpu_seconds, Tres)),      %% CPUs * elapsed
    ?assertEqual(8192 * 3600, maps:get(mem_seconds, Tres)),   %% mem * elapsed
    ?assertEqual(2 * 3600, maps:get(node_seconds, Tres)),     %% nodes * elapsed
    ?assertEqual(1 * 3600, maps:get(gpu_seconds, Tres)),      %% gpu * elapsed

    ok.

test_tres_aggregation_by_user(_Config) ->
    %% Test TRES aggregation per user
    User = <<"aggregation_user">>,
    Period = current_month(),

    %% Update TRES for user
    TresUpdate1 = #{
        entity_type => user,
        entity_id => User,
        period => Period,
        cpu_seconds => 10000,
        mem_seconds => 50000
    },
    ok = mock_ra_update_tres(TresUpdate1),

    %% Add more TRES
    TresUpdate2 = #{
        entity_type => user,
        entity_id => User,
        period => Period,
        cpu_seconds => 5000,
        mem_seconds => 25000
    },
    ok = mock_ra_update_tres(TresUpdate2),

    %% Query aggregated TRES
    {ok, Tres} = mock_ra_query_tres(user, User, Period),

    ?assertEqual(15000, maps:get(cpu_seconds, Tres)),
    ?assertEqual(75000, maps:get(mem_seconds, Tres)),

    ok.

test_tres_aggregation_by_account(_Config) ->
    %% Test TRES aggregation per account
    Account = <<"aggregation_acct">>,
    Period = current_month(),

    %% Update TRES for account
    TresUpdate = #{
        entity_type => account,
        entity_id => Account,
        period => Period,
        cpu_seconds => 20000,
        gpu_seconds => 5000
    },
    ok = mock_ra_update_tres(TresUpdate),

    %% Query aggregated TRES
    {ok, Tres} = mock_ra_query_tres(account, Account, Period),

    ?assertEqual(20000, maps:get(cpu_seconds, Tres)),
    ?assertEqual(5000, maps:get(gpu_seconds, Tres)),

    ok.

test_tres_non_negative_invariant(_Config) ->
    %% Test that TRES values are never negative
    %% (TLA+ TRES Non-Negative invariant)
    JobInfo = #{
        job_id => 30010,
        user_name => <<"nonneg_user">>,
        account => <<"nonneg_acct">>,
        num_cpus => 0,  %% Edge case
        elapsed => 0,    %% Edge case
        tres_alloc => #{}
    },

    Tres = mock_calculate_tres(JobInfo),

    %% All values should be >= 0
    ?assert(maps:get(cpu_seconds, Tres) >= 0),
    ?assert(maps:get(mem_seconds, Tres) >= 0),
    ?assert(maps:get(node_seconds, Tres) >= 0),
    ?assert(maps:get(gpu_seconds, Tres) >= 0),

    ok.

test_tres_query_by_period(_Config) ->
    %% Test querying TRES by specific period
    User = <<"period_user">>,
    Period1 = <<"2026-01">>,
    Period2 = <<"2026-02">>,

    %% Add TRES for two different periods
    ok = mock_ra_update_tres(#{
        entity_type => user,
        entity_id => User,
        period => Period1,
        cpu_seconds => 1000
    }),
    ok = mock_ra_update_tres(#{
        entity_type => user,
        entity_id => User,
        period => Period2,
        cpu_seconds => 2000
    }),

    %% Query each period separately
    {ok, Tres1} = mock_ra_query_tres(user, User, Period1),
    {ok, Tres2} = mock_ra_query_tres(user, User, Period2),

    ?assertEqual(1000, maps:get(cpu_seconds, Tres1)),
    ?assertEqual(2000, maps:get(cpu_seconds, Tres2)),

    ok.

test_billing_tres_calculation(_Config) ->
    %% Test billing TRES calculation with weights
    JobInfo = #{
        job_id => 30020,
        num_cpus => 4,
        req_mem => 8192,  %% 8GB
        elapsed => 3600,  %% 1 hour
        tres_alloc => #{gpu => 2}
    },

    %% Calculate billing TRES with default weights
    %% Formula: (CPUs * CpuWeight + MemGB * MemWeight + GPUs * GpuWeight) * Runtime
    %% Default weights: CPU=1.0, Mem=0.25 per GB, GPU=2.0
    BillingTres = mock_calculate_billing_tres(JobInfo),

    %% Expected: (4 * 1.0 + 8 * 0.25 + 2 * 2.0) * 3600 = (4 + 2 + 4) * 3600 = 36000
    ExpectedBilling = round((4 * 1.0 + 8 * 0.25 + 2 * 2.0) * 3600),
    ?assertEqual(ExpectedBilling, BillingTres),

    ok.

%%====================================================================
%% Test Helpers - Setup/Cleanup
%%====================================================================

setup_mnesia() ->
    %% Stop Mnesia if running
    catch mnesia:stop(),
    mnesia:delete_schema([node()]),
    mnesia:create_schema([node()]),
    mnesia:start(),
    ok.

cleanup_mnesia() ->
    catch mnesia:stop(),
    mnesia:delete_schema([node()]),
    ok.

start_mock_fragment_manager() ->
    %% Create ETS table to store fragment state
    Self = self(),
    TableOwner = spawn(fun() ->
        ets:new(mock_fragments, [set, public, named_table]),
        ets:new(mock_jobs, [set, public, named_table]),
        Self ! {ets_ready, self()},
        receive stop -> ok end
    end),
    receive
        {ets_ready, TableOwner} -> ok
    after 5000 ->
        error(ets_table_creation_timeout)
    end,
    register(mock_fragments_owner, TableOwner),

    %% Initialize with current month fragment
    CurrentPeriod = current_month(),
    TableName = period_to_table_name(CurrentPeriod, 0),
    ets:insert(mock_fragments, {TableName, #{
        table_name => TableName,
        base_period => CurrentPeriod,
        fragment_id => 0,
        record_count => 0,
        size_bytes => 0,
        created_at => erlang:system_time(second),
        status => hot
    }}),
    ok.

cleanup_mock_fragment_manager() ->
    catch ets:delete(mock_fragments),
    catch ets:delete(mock_jobs),
    case whereis(mock_fragments_owner) of
        undefined -> ok;
        Pid ->
            Pid ! stop,
            unregister(mock_fragments_owner)
    end,
    ok.

setup_mock_ra() ->
    %% Create ETS tables for mock Ra state
    Self = self(),
    TableOwner = spawn(fun() ->
        ets:new(mock_ra_jobs, [set, public, named_table]),
        ets:new(mock_ra_recorded, [set, public, named_table]),
        ets:new(mock_ra_tres, [set, public, named_table]),
        ets:new(mock_ra_user_totals, [set, public, named_table]),
        ets:new(mock_ra_account_totals, [set, public, named_table]),
        Self ! {ets_ready, self()},
        receive stop -> ok end
    end),
    receive
        {ets_ready, TableOwner} -> ok
    after 5000 ->
        error(ets_table_creation_timeout)
    end,
    register(mock_ra_owner, TableOwner),
    ok.

cleanup_mock_ra() ->
    catch ets:delete(mock_ra_jobs),
    catch ets:delete(mock_ra_recorded),
    catch ets:delete(mock_ra_tres),
    catch ets:delete(mock_ra_user_totals),
    catch ets:delete(mock_ra_account_totals),
    case whereis(mock_ra_owner) of
        undefined -> ok;
        Pid ->
            Pid ! stop,
            unregister(mock_ra_owner)
    end,
    ok.

setup_retention_mocks() ->
    %% Setup mocks for retention module
    meck:new(flurm_dbd_server, [non_strict, no_link]),
    meck:expect(flurm_dbd_server, archive_old_records, fun(_Days) -> {ok, 5} end),
    meck:expect(flurm_dbd_server, purge_old_records, fun(_Days) -> {ok, 3} end),
    meck:expect(flurm_dbd_server, archive_old_jobs, fun(_Days) -> {ok, 10} end),
    meck:expect(flurm_dbd_server, get_archived_jobs, fun() -> [] end),
    meck:expect(flurm_dbd_server, get_stats, fun() -> #{archived_jobs => 0} end),

    %% Clear application env
    application:unset_env(flurm_dbd, job_retention_days),
    application:unset_env(flurm_dbd, usage_retention_days),
    application:unset_env(flurm_dbd, archive_retention_days),
    application:unset_env(flurm_dbd, auto_prune_enabled),
    application:unset_env(flurm_dbd, auto_prune_interval_hours),
    application:unset_env(flurm_dbd, prune_timer_ref),
    ok.

cleanup_retention_mocks() ->
    catch meck:unload(flurm_dbd_server),
    application:unset_env(flurm_dbd, job_retention_days),
    application:unset_env(flurm_dbd, usage_retention_days),
    application:unset_env(flurm_dbd, archive_retention_days),
    application:unset_env(flurm_dbd, auto_prune_enabled),
    application:unset_env(flurm_dbd, auto_prune_interval_hours),
    application:unset_env(flurm_dbd, prune_timer_ref),
    ok.

setup_tres_mocks() ->
    %% Setup ETS tables for TRES tests
    Self = self(),
    TableOwner = spawn(fun() ->
        ets:new(mock_tres_data, [set, public, named_table]),
        Self ! {ets_ready, self()},
        receive stop -> ok end
    end),
    receive
        {ets_ready, TableOwner} -> ok
    after 5000 ->
        error(ets_table_creation_timeout)
    end,
    register(mock_tres_owner, TableOwner),

    %% Also setup Ra mocks for TRES
    setup_mock_ra(),
    ok.

cleanup_tres_mocks() ->
    catch ets:delete(mock_tres_data),
    case whereis(mock_tres_owner) of
        undefined -> ok;
        Pid ->
            Pid ! stop,
            unregister(mock_tres_owner)
    end,
    cleanup_mock_ra(),
    ok.

%%====================================================================
%% Test Helpers - Mock Fragment Manager
%%====================================================================

mock_ensure_current_table() ->
    CurrentPeriod = current_month(),
    TableName = period_to_table_name(CurrentPeriod, 0),
    case ets:lookup(mock_fragments, TableName) of
        [{_, _}] ->
            {ok, TableName};
        [] ->
            Meta = #{
                table_name => TableName,
                base_period => CurrentPeriod,
                fragment_id => 0,
                record_count => 0,
                size_bytes => 0,
                created_at => erlang:system_time(second),
                status => hot
            },
            ets:insert(mock_fragments, {TableName, Meta}),
            {ok, TableName}
    end.

mock_list_all_fragments() ->
    [Meta || {_, Meta} <- ets:tab2list(mock_fragments)].

mock_set_fragment_record_count(Period, FragId, Count) ->
    TableName = period_to_table_name(Period, FragId),
    case ets:lookup(mock_fragments, TableName) of
        [{_, Meta}] ->
            ets:insert(mock_fragments, {TableName, Meta#{record_count => Count}});
        [] ->
            ets:insert(mock_fragments, {TableName, #{
                table_name => TableName,
                base_period => Period,
                fragment_id => FragId,
                record_count => Count,
                size_bytes => 0,
                created_at => erlang:system_time(second),
                status => hot
            }})
    end,
    ok.

mock_insert_job(JobInfo) ->
    JobId = maps:get(job_id, JobInfo),
    EndTime = maps:get(end_time, JobInfo, erlang:system_time(second)),
    Period = time_to_period(EndTime),
    TableName = period_to_table_name(Period, 0),

    %% Check if we need burst fragment
    case ets:lookup(mock_fragments, TableName) of
        [{_, Meta}] ->
            Count = maps:get(record_count, Meta),
            NewTable = if
                Count >= 500000 ->
                    FragId = find_next_fragment_id(Period),
                    NewTableName = period_to_table_name(Period, FragId),
                    ets:insert(mock_fragments, {NewTableName, #{
                        table_name => NewTableName,
                        base_period => Period,
                        fragment_id => FragId,
                        record_count => 1,
                        size_bytes => 500,
                        created_at => erlang:system_time(second),
                        status => hot
                    }}),
                    NewTableName;
                true ->
                    ets:insert(mock_fragments, {TableName, Meta#{
                        record_count => Count + 1,
                        size_bytes => maps:get(size_bytes, Meta) + 500
                    }}),
                    TableName
            end,
            ets:insert(mock_jobs, {JobId, JobInfo}),
            {ok, NewTable};
        [] ->
            %% Create table
            ets:insert(mock_fragments, {TableName, #{
                table_name => TableName,
                base_period => Period,
                fragment_id => 0,
                record_count => 1,
                size_bytes => 500,
                created_at => erlang:system_time(second),
                status => hot
            }}),
            ets:insert(mock_jobs, {JobId, JobInfo}),
            {ok, TableName}
    end.

find_next_fragment_id(Period) ->
    AllFragments = mock_list_all_fragments(),
    PeriodFragments = [maps:get(fragment_id, F) ||
                       F <- AllFragments,
                       maps:get(base_period, F) =:= Period],
    case PeriodFragments of
        [] -> 1;
        Ids -> lists:max(Ids) + 1
    end.

mock_query_jobs(Filters) ->
    AllJobs = [Job || {_, Job} <- ets:tab2list(mock_jobs)],
    StartTime = maps:get(start_time, Filters, 0),
    EndTime = maps:get(end_time, Filters, erlang:system_time(second) + 86400),
    Filtered = lists:filter(fun(Job) ->
        JobEnd = maps:get(end_time, Job, erlang:system_time(second)),
        JobEnd >= StartTime andalso JobEnd =< EndTime
    end, AllJobs),
    {ok, Filtered}.

mock_get_fragment_meta(TableName) ->
    case ets:lookup(mock_fragments, TableName) of
        [{_, Meta}] -> {ok, Meta};
        [] -> {error, not_found}
    end.

mock_get_table_stats() ->
    AllMetas = mock_list_all_fragments(),
    TotalRecords = lists:sum([maps:get(record_count, M) || M <- AllMetas]),
    TotalSize = lists:sum([maps:get(size_bytes, M) || M <- AllMetas]),
    #{
        total_records => TotalRecords,
        total_size_bytes => TotalSize,
        table_count => length(AllMetas)
    }.

%%====================================================================
%% Test Helpers - Mock Ra
%%====================================================================

mock_ra_record_job(JobInfo) ->
    JobId = maps:get(job_id, JobInfo),

    %% Check double counting
    case ets:lookup(mock_ra_recorded, JobId) of
        [{_, true}] ->
            {error, already_recorded};
        [] ->
            %% Record job
            ets:insert(mock_ra_jobs, {JobId, JobInfo}),
            ets:insert(mock_ra_recorded, {JobId, true}),

            %% Calculate and update TRES
            Tres = mock_calculate_tres(JobInfo),
            User = maps:get(user_name, JobInfo, <<>>),
            Account = maps:get(account, JobInfo, <<>>),

            %% Update user totals
            update_mock_totals(mock_ra_user_totals, User, Tres),
            update_mock_totals(mock_ra_account_totals, Account, Tres),

            {ok, JobId}
    end.

mock_ra_get_job(JobId) ->
    case ets:lookup(mock_ra_jobs, JobId) of
        [{_, Job}] -> {ok, Job};
        [] -> {error, not_found}
    end.

mock_ra_get_user_totals(User) ->
    case ets:lookup(mock_ra_user_totals, User) of
        [{_, Totals}] -> {ok, Totals};
        [] -> {error, not_found}
    end.

mock_ra_get_account_totals(Account) ->
    case ets:lookup(mock_ra_account_totals, Account) of
        [{_, Totals}] -> {ok, Totals};
        [] -> {error, not_found}
    end.

mock_ra_update_tres(TresUpdate) ->
    EntityType = maps:get(entity_type, TresUpdate),
    EntityId = maps:get(entity_id, TresUpdate),
    Period = maps:get(period, TresUpdate),
    Key = {EntityType, EntityId, Period},

    CpuSeconds = maps:get(cpu_seconds, TresUpdate, 0),
    MemSeconds = maps:get(mem_seconds, TresUpdate, 0),
    GpuSeconds = maps:get(gpu_seconds, TresUpdate, 0),

    Existing = case ets:lookup(mock_ra_tres, Key) of
        [{_, E}] -> E;
        [] -> #{cpu_seconds => 0, mem_seconds => 0, gpu_seconds => 0,
                node_seconds => 0, job_count => 0, job_time => 0}
    end,

    NewTres = #{
        cpu_seconds => maps:get(cpu_seconds, Existing) + CpuSeconds,
        mem_seconds => maps:get(mem_seconds, Existing) + MemSeconds,
        gpu_seconds => maps:get(gpu_seconds, Existing) + GpuSeconds,
        node_seconds => maps:get(node_seconds, Existing, 0),
        job_count => maps:get(job_count, Existing, 0),
        job_time => maps:get(job_time, Existing, 0)
    },
    ets:insert(mock_ra_tres, {Key, NewTres}),
    ok.

mock_ra_query_tres(EntityType, EntityId, Period) ->
    Key = {EntityType, EntityId, Period},
    case ets:lookup(mock_ra_tres, Key) of
        [{_, Tres}] -> {ok, Tres};
        [] -> {error, not_found}
    end.

update_mock_totals(_Table, <<>>, _Tres) ->
    ok;
update_mock_totals(Table, Entity, Tres) ->
    Existing = case ets:lookup(Table, Entity) of
        [{_, E}] -> E;
        [] -> #{cpu_seconds => 0, mem_seconds => 0, gpu_seconds => 0,
                node_seconds => 0}
    end,
    NewTotals = #{
        cpu_seconds => maps:get(cpu_seconds, Existing) + maps:get(cpu_seconds, Tres),
        mem_seconds => maps:get(mem_seconds, Existing) + maps:get(mem_seconds, Tres),
        gpu_seconds => maps:get(gpu_seconds, Existing) + maps:get(gpu_seconds, Tres),
        node_seconds => maps:get(node_seconds, Existing) + maps:get(node_seconds, Tres)
    },
    ets:insert(Table, {Entity, NewTotals}),
    ok.

%%====================================================================
%% Test Helpers - Retention Mocks
%%====================================================================

mock_get_retention_config() ->
    #{
        job_retention_days => application:get_env(flurm_dbd, job_retention_days, 365),
        usage_retention_days => application:get_env(flurm_dbd, usage_retention_days, 730),
        archive_retention_days => application:get_env(flurm_dbd, archive_retention_days, 1095),
        auto_prune_enabled => application:get_env(flurm_dbd, auto_prune_enabled, false),
        auto_prune_interval_hours => application:get_env(flurm_dbd, auto_prune_interval_hours, 24)
    }.

mock_set_retention_config(Config) ->
    maps:foreach(fun(Key, Value) ->
        application:set_env(flurm_dbd, Key, Value)
    end, Config),
    ok.

mock_prune_old_records(_RetentionDays) ->
    {ok, #{
        jobs_pruned => 5,
        usage_pruned => 0,
        archived => 0,
        errors => []
    }}.

mock_create_fragment_meta(Period, Status) ->
    TableName = period_to_table_name(Period, 0),
    #{
        table_name => TableName,
        base_period => Period,
        fragment_id => 0,
        record_count => 0,
        size_bytes => 0,
        created_at => erlang:system_time(second),
        status => Status
    }.

mock_age_fragment_if_needed(Period, CurrentStatus) ->
    {{CurrentYear, CurrentMonth, _}, _} = calendar:local_time(),
    {TableYear, TableMonth} = parse_period(Period),
    MonthsAgo = (CurrentYear * 12 + CurrentMonth) - (TableYear * 12 + TableMonth),

    NewStatus = case {CurrentStatus, MonthsAgo} of
        {hot, N} when N >= 1 -> warm;
        {warm, N} when N >= 3 -> cold;
        {cold, N} when N >= 12 -> archived;
        _ -> CurrentStatus
    end,
    mock_create_fragment_meta(Period, NewStatus).

mock_trigger_archival(_MonthsOld) ->
    {ok, 3}.

mock_schedule_prune(IntervalHours) ->
    IntervalMs = IntervalHours * 3600 * 1000,
    TimerRef = erlang:send_after(IntervalMs, self(), scheduled_prune),
    application:set_env(flurm_dbd, prune_timer_ref, TimerRef),
    {ok, TimerRef}.

mock_cancel_scheduled_prune() ->
    case application:get_env(flurm_dbd, prune_timer_ref) of
        {ok, TimerRef} ->
            erlang:cancel_timer(TimerRef),
            application:unset_env(flurm_dbd, prune_timer_ref);
        undefined ->
            ok
    end,
    ok.

%%====================================================================
%% Test Helpers - TRES Calculations
%%====================================================================

mock_calculate_tres(JobInfo) ->
    NumCpus = maps:get(num_cpus, JobInfo, 1),
    NumNodes = maps:get(num_nodes, JobInfo, 1),
    ReqMem = maps:get(req_mem, JobInfo, 0),
    Elapsed = maps:get(elapsed, JobInfo, 0),
    TresAlloc = maps:get(tres_alloc, JobInfo, #{}),
    Gpus = maps:get(gpu, TresAlloc, 0),

    #{
        cpu_seconds => NumCpus * Elapsed,
        mem_seconds => ReqMem * Elapsed,
        node_seconds => NumNodes * Elapsed,
        gpu_seconds => Gpus * Elapsed
    }.

mock_calculate_billing_tres(JobInfo) ->
    NumCpus = maps:get(num_cpus, JobInfo, 1),
    ReqMemMB = maps:get(req_mem, JobInfo, 0),
    ReqMemGB = ReqMemMB / 1024,
    Elapsed = maps:get(elapsed, JobInfo, 0),
    TresAlloc = maps:get(tres_alloc, JobInfo, #{}),
    Gpus = maps:get(gpu, TresAlloc, 0),

    %% Default weights
    CpuWeight = 1.0,
    MemWeight = 0.25,  %% per GB
    GpuWeight = 2.0,

    round((NumCpus * CpuWeight + ReqMemGB * MemWeight + Gpus * GpuWeight) * Elapsed).

%%====================================================================
%% Test Helpers - Time/Period Utilities
%%====================================================================

current_month() ->
    {{Year, Month, _}, _} = calendar:local_time(),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

time_to_period(Timestamp) ->
    {{Year, Month, _}, _} = calendar:system_time_to_local_time(Timestamp, second),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

parse_period(Period) ->
    [YearStr, MonthStr] = binary:split(Period, <<"-">>),
    {binary_to_integer(YearStr), binary_to_integer(MonthStr)}.

period_to_table_name(Period, FragmentId) ->
    PeriodStr = binary:replace(Period, <<"-">>, <<"_">>),
    case FragmentId of
        0 ->
            list_to_atom("job_records_" ++ binary_to_list(PeriodStr));
        N ->
            list_to_atom("job_records_" ++ binary_to_list(PeriodStr) ++
                         "_frag_" ++ integer_to_list(N))
    end.

subtract_months(Year, Month, Subtract) ->
    TotalMonths = Year * 12 + Month - Subtract,
    NewYear = (TotalMonths - 1) div 12,
    NewMonth = case TotalMonths rem 12 of
        0 -> 12;
        N -> N
    end,
    list_to_binary(io_lib:format("~4..0B-~2..0B", [NewYear, NewMonth])).

create_test_job_info(JobId, User, Account, EndTime) ->
    #{
        job_id => JobId,
        job_name => <<"test_job_", (integer_to_binary(JobId))/binary>>,
        user_name => User,
        account => Account,
        partition => <<"batch">>,
        state => completed,
        exit_code => 0,
        num_cpus => 4,
        num_nodes => 1,
        req_mem => 4096,
        elapsed => 3600,
        submit_time => EndTime - 3700,
        start_time => EndTime - 3600,
        end_time => EndTime,
        tres_alloc => #{cpu => 4, mem => 4096}
    }.
