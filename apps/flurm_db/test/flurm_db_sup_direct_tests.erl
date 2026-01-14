%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_db_sup module
%%%
%%% These tests call flurm_db_sup functions directly to get code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_sup_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

supervisor_callbacks_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Init returns supervisor spec", fun init_test/0},
      {"Init creates ETS tables", fun init_ets_tables_test/0},
      {"Init opens DETS tables", fun init_dets_tables_test/0}
     ]}.

data_loading_test_() ->
    {setup,
     fun setup_with_dets/0,
     fun cleanup_with_dets/1,
     [
      {"Load jobs from DETS", fun load_jobs_from_dets_test/0},
      {"Init job counter - from DETS", fun init_job_counter_from_dets_test/0},
      {"Init job counter - from jobs", fun init_job_counter_from_jobs_test/0},
      {"Init job counter - empty", fun init_job_counter_empty_test/0}
     ]}.

get_data_dir_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"Get data dir - configured", fun get_data_dir_configured_test/0},
      {"Get data dir - fallback", fun get_data_dir_fallback_test/0}
     ]}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup() ->
    meck:new(lager, [passthrough]),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    %% Clean up any existing tables
    cleanup_tables(),
    %% Set a test data directory
    TestDir = filename:join(["/tmp", "flurm_db_test_" ++ integer_to_list(erlang:unique_integer([positive]))]),
    application:set_env(flurm_db, data_dir, TestDir),
    TestDir.

cleanup(TestDir) ->
    meck:unload(lager),
    cleanup_tables(),
    application:unset_env(flurm_db, data_dir),
    %% Clean up test directory
    catch file:del_dir_r(TestDir),
    ok.

setup_with_dets() ->
    TestDir = setup(),
    %% Create the test directory
    ok = filelib:ensure_dir(filename:join(TestDir, "dummy")),
    TestDir.

cleanup_with_dets(TestDir) ->
    cleanup(TestDir).

cleanup_tables() ->
    lists:foreach(fun(Table) ->
        case ets:whereis(Table) of
            undefined -> ok;
            _ -> catch ets:delete(Table)
        end
    end, [flurm_db_jobs_ets, flurm_db_nodes_ets, flurm_db_partitions_ets,
          flurm_db_job_counter_ets]),
    catch dets:close(flurm_db_jobs_dets),
    catch dets:close(flurm_db_counter_dets).

%%====================================================================
%% Supervisor Callback Tests
%%====================================================================

init_test() ->
    {ok, {SupFlags, Children}} = flurm_db_sup:init(#{}),

    %% Check supervisor flags
    ?assertMatch(#{strategy := one_for_one, intensity := 5, period := 10}, SupFlags),

    %% Check children - should have ra_starter
    ?assertEqual(1, length(Children)),
    [RaStarter] = Children,
    ?assertEqual(flurm_db_ra_starter, maps:get(id, RaStarter)),
    ?assertEqual(transient, maps:get(restart, RaStarter)).

init_ets_tables_test() ->
    %% Clean up first
    cleanup_tables(),

    %% Init should create ETS tables
    {ok, _} = flurm_db_sup:init(#{}),

    %% Verify tables exist
    ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs_ets)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_nodes_ets)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_partitions_ets)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_job_counter_ets)).

init_dets_tables_test() ->
    %% Clean up first
    cleanup_tables(),

    %% Init should open DETS tables
    {ok, _} = flurm_db_sup:init(#{}),

    %% Verify DETS tables are open
    ?assertNotEqual(undefined, dets:info(flurm_db_jobs_dets)),
    ?assertNotEqual(undefined, dets:info(flurm_db_counter_dets)).

%%====================================================================
%% Data Loading Tests
%%====================================================================

load_jobs_from_dets_test() ->
    %% Clean up first
    cleanup_tables(),

    %% Create and populate DETS with test jobs
    TestDir = application:get_env(flurm_db, data_dir, "/tmp/flurm_db_test"),
    ok = filelib:ensure_dir(filename:join(TestDir, "dummy")),
    JobsDetsFile = filename:join(TestDir, "flurm_jobs.dets"),

    {ok, _} = dets:open_file(flurm_db_jobs_dets, [{file, JobsDetsFile}, {type, set}]),
    dets:insert(flurm_db_jobs_dets, {1, make_test_job(1)}),
    dets:insert(flurm_db_jobs_dets, {2, make_test_job(2)}),
    dets:sync(flurm_db_jobs_dets),
    dets:close(flurm_db_jobs_dets),

    %% Init should load jobs from DETS
    {ok, _} = flurm_db_sup:init(#{}),

    %% Verify jobs are in ETS
    ?assertEqual(2, length(ets:tab2list(flurm_db_jobs_ets))).

init_job_counter_from_dets_test() ->
    %% Clean up first
    cleanup_tables(),

    %% Create and populate DETS with counter
    TestDir = application:get_env(flurm_db, data_dir, "/tmp/flurm_db_test"),
    ok = filelib:ensure_dir(filename:join(TestDir, "dummy")),
    CounterDetsFile = filename:join(TestDir, "flurm_counter.dets"),

    {ok, _} = dets:open_file(flurm_db_counter_dets, [{file, CounterDetsFile}, {type, set}]),
    dets:insert(flurm_db_counter_dets, {counter, 42}),
    dets:sync(flurm_db_counter_dets),
    dets:close(flurm_db_counter_dets),

    %% Init should restore counter from DETS
    {ok, _} = flurm_db_sup:init(#{}),

    %% Verify counter is restored
    [{counter, Counter}] = ets:lookup(flurm_db_job_counter_ets, counter),
    ?assertEqual(42, Counter).

init_job_counter_from_jobs_test() ->
    %% Clean up first
    cleanup_tables(),

    %% Create ETS with jobs but no DETS counter
    TestDir = application:get_env(flurm_db, data_dir, "/tmp/flurm_db_test"),
    ok = filelib:ensure_dir(filename:join(TestDir, "dummy")),

    %% Delete counter DETS file if exists
    CounterDetsFile = filename:join(TestDir, "flurm_counter.dets"),
    catch file:delete(CounterDetsFile),

    %% Create jobs DETS with jobs
    JobsDetsFile = filename:join(TestDir, "flurm_jobs.dets"),
    {ok, _} = dets:open_file(flurm_db_jobs_dets, [{file, JobsDetsFile}, {type, set}]),
    dets:insert(flurm_db_jobs_dets, {10, make_test_job(10)}),
    dets:insert(flurm_db_jobs_dets, {20, make_test_job(20)}),
    dets:insert(flurm_db_jobs_dets, {5, make_test_job(5)}),
    dets:sync(flurm_db_jobs_dets),
    dets:close(flurm_db_jobs_dets),

    %% Init should calculate counter from max job ID
    {ok, _} = flurm_db_sup:init(#{}),

    %% Verify counter is max job ID
    [{counter, Counter}] = ets:lookup(flurm_db_job_counter_ets, counter),
    ?assertEqual(20, Counter).

init_job_counter_empty_test() ->
    %% Clean up first
    cleanup_tables(),

    %% Create empty DETS tables
    TestDir = application:get_env(flurm_db, data_dir, "/tmp/flurm_db_test"),
    ok = filelib:ensure_dir(filename:join(TestDir, "dummy")),

    %% Delete existing DETS files
    catch file:delete(filename:join(TestDir, "flurm_jobs.dets")),
    catch file:delete(filename:join(TestDir, "flurm_counter.dets")),

    %% Init with empty tables
    {ok, _} = flurm_db_sup:init(#{}),

    %% Counter should be 0
    [{counter, Counter}] = ets:lookup(flurm_db_job_counter_ets, counter),
    ?assertEqual(0, Counter).

%%====================================================================
%% Get Data Dir Tests
%%====================================================================

get_data_dir_configured_test() ->
    TestDir = "/tmp/configured_dir_test",
    application:set_env(flurm_db, data_dir, TestDir),

    %% Init will use configured dir
    cleanup_tables(),
    {ok, _} = flurm_db_sup:init(#{}),

    %% Directory should be created
    ?assert(filelib:is_dir(TestDir)),

    %% Clean up
    file:del_dir_r(TestDir).

get_data_dir_fallback_test() ->
    %% Unset data_dir to test fallback
    application:unset_env(flurm_db, data_dir),

    %% Init will use fallback dir
    cleanup_tables(),
    {ok, _} = flurm_db_sup:init(#{}),

    %% Should have created some directory (either /var/lib/flurm or ./data)
    ok.

%%====================================================================
%% Test Helpers
%%====================================================================

make_test_job(Id) ->
    #job{
        id = Id,
        name = <<"test_job">>,
        user = <<"user1">>,
        partition = <<"batch">>,
        state = pending,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined
    }.
