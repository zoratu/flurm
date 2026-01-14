%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_db_persist module
%%%
%%% These tests call flurm_db_persist functions directly to get code coverage.
%%% We test both the ETS fallback path and (mocked) Ra path.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_persist_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

ets_backend_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     [
      {"Is Ra available - nonode", fun is_ra_available_nonode_test/0},
      {"Persistence mode - ETS", fun persistence_mode_ets_test/0},
      {"Persistence mode - none", fun persistence_mode_none_test/0},
      {"Store job ETS", fun store_job_ets_test/0},
      {"Get job ETS - found", fun get_job_ets_found_test/0},
      {"Get job ETS - not found", fun get_job_ets_not_found_test/0},
      {"Update job ETS - found", fun update_job_ets_found_test/0},
      {"Update job ETS - not found", fun update_job_ets_not_found_test/0},
      {"Update job ETS - state transitions", fun update_job_ets_state_transitions_test/0},
      {"Delete job ETS", fun delete_job_ets_test/0},
      {"List jobs ETS - empty", fun list_jobs_ets_empty_test/0},
      {"List jobs ETS - with jobs", fun list_jobs_ets_with_jobs_test/0},
      {"Next job ID ETS", fun next_job_id_ets_test/0}
     ]}.

ra_backend_test_() ->
    {setup,
     fun setup_ra_mock/0,
     fun cleanup_ra_mock/1,
     [
      {"Store job Ra", fun store_job_ra_test/0},
      {"Update job Ra - state update", fun update_job_ra_state_test/0},
      {"Update job Ra - exit code update", fun update_job_ra_exit_code_test/0},
      {"Update job Ra - allocated nodes update", fun update_job_ra_allocated_nodes_test/0},
      {"Get job Ra", fun get_job_ra_test/0},
      {"Delete job Ra", fun delete_job_ra_test/0},
      {"List jobs Ra - success", fun list_jobs_ra_success_test/0},
      {"List jobs Ra - error", fun list_jobs_ra_error_test/0},
      {"Next job ID Ra - success", fun next_job_id_ra_success_test/0},
      {"Next job ID Ra - fallback to ETS", fun next_job_id_ra_fallback_test/0}
     ]}.

ra_job_conversion_test_() ->
    {setup,
     fun setup_ra_mock/0,
     fun cleanup_ra_mock/1,
     [
      {"Ra job to job conversion", fun ra_job_to_job_test/0}
     ]}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup_ets() ->
    %% Create ETS tables for testing
    ets:new(flurm_db_jobs_ets, [named_table, public, set, {read_concurrency, true}]),
    ets:new(flurm_db_job_counter_ets, [named_table, public, set]),
    ets:insert(flurm_db_job_counter_ets, {counter, 0}),
    ok.

cleanup_ets(_) ->
    catch ets:delete(flurm_db_jobs_ets),
    catch ets:delete(flurm_db_job_counter_ets),
    catch dets:close(flurm_db_jobs_dets),
    catch dets:close(flurm_db_counter_dets),
    ok.

setup_ra_mock() ->
    setup_ets(),
    meck:new(flurm_db_ra, [passthrough]),
    %% Mock ra:members to return success so is_ra_available returns true
    meck:new(ra, [passthrough]),
    meck:expect(ra, members, fun(_) -> {ok, [{flurm_db_ra, node()}], {flurm_db_ra, node()}} end),
    ok.

cleanup_ra_mock(_) ->
    meck:unload(flurm_db_ra),
    meck:unload(ra),
    cleanup_ets(ok).

%%====================================================================
%% ETS Backend Tests
%%====================================================================

is_ra_available_nonode_test() ->
    %% When running as nonode@nohost, Ra should not be available
    case node() of
        nonode@nohost ->
            ?assertEqual(false, flurm_db_persist:is_ra_available());
        _ ->
            %% Named node without Ra cluster - should return false too
            ok
    end.

persistence_mode_ets_test() ->
    ?assertEqual(ets, flurm_db_persist:persistence_mode()).

persistence_mode_none_test() ->
    %% Delete the ETS table to simulate no persistence
    ets:delete(flurm_db_jobs_ets),
    ?assertEqual(none, flurm_db_persist:persistence_mode()),
    %% Recreate for other tests
    ets:new(flurm_db_jobs_ets, [named_table, public, set, {read_concurrency, true}]).

store_job_ets_test() ->
    Job = make_test_job(1),
    ok = flurm_db_persist:store_job(Job),

    %% Verify it's in ETS
    [{1, StoredJob}] = ets:lookup(flurm_db_jobs_ets, 1),
    ?assertEqual(1, StoredJob#job.id),
    ?assertEqual(<<"test_job">>, StoredJob#job.name).

get_job_ets_found_test() ->
    Job = make_test_job(2),
    ets:insert(flurm_db_jobs_ets, {2, Job}),

    {ok, Retrieved} = flurm_db_persist:get_job(2),
    ?assertEqual(2, Retrieved#job.id).

get_job_ets_not_found_test() ->
    ?assertEqual({error, not_found}, flurm_db_persist:get_job(999)).

update_job_ets_found_test() ->
    Job = make_test_job(3),
    ets:insert(flurm_db_jobs_ets, {3, Job}),

    %% Update with new state
    ok = flurm_db_persist:update_job(3, #{state => running}),

    [{3, UpdatedJob}] = ets:lookup(flurm_db_jobs_ets, 3),
    ?assertEqual(running, UpdatedJob#job.state).

update_job_ets_not_found_test() ->
    ?assertEqual({error, not_found}, flurm_db_persist:update_job(999, #{state => running})).

update_job_ets_state_transitions_test() ->
    Job = make_test_job(4),
    ets:insert(flurm_db_jobs_ets, {4, Job}),

    %% Update to running - should set start_time
    ok = flurm_db_persist:update_job(4, #{state => running}),
    [{4, RunningJob}] = ets:lookup(flurm_db_jobs_ets, 4),
    ?assertEqual(running, RunningJob#job.state),
    ?assertNotEqual(undefined, RunningJob#job.start_time),

    %% Update to completed - should set end_time
    ok = flurm_db_persist:update_job(4, #{state => completed}),
    [{4, CompletedJob}] = ets:lookup(flurm_db_jobs_ets, 4),
    ?assertEqual(completed, CompletedJob#job.state),
    ?assertNotEqual(undefined, CompletedJob#job.end_time),

    %% Test other terminal states
    ets:insert(flurm_db_jobs_ets, {5, make_test_job(5)}),
    ok = flurm_db_persist:update_job(5, #{state => failed}),
    [{5, FailedJob}] = ets:lookup(flurm_db_jobs_ets, 5),
    ?assertEqual(failed, FailedJob#job.state),

    ets:insert(flurm_db_jobs_ets, {6, make_test_job(6)}),
    ok = flurm_db_persist:update_job(6, #{state => cancelled}),
    [{6, CancelledJob}] = ets:lookup(flurm_db_jobs_ets, 6),
    ?assertEqual(cancelled, CancelledJob#job.state),

    ets:insert(flurm_db_jobs_ets, {7, make_test_job(7)}),
    ok = flurm_db_persist:update_job(7, #{state => timeout}),
    [{7, TimeoutJob}] = ets:lookup(flurm_db_jobs_ets, 7),
    ?assertEqual(timeout, TimeoutJob#job.state),

    ets:insert(flurm_db_jobs_ets, {8, make_test_job(8)}),
    ok = flurm_db_persist:update_job(8, #{state => node_fail}),
    [{8, NodeFailJob}] = ets:lookup(flurm_db_jobs_ets, 8),
    ?assertEqual(node_fail, NodeFailJob#job.state),

    %% Test allocated_nodes update
    ets:insert(flurm_db_jobs_ets, {9, make_test_job(9)}),
    ok = flurm_db_persist:update_job(9, #{allocated_nodes => [<<"node1">>]}),
    [{9, AllocJob}] = ets:lookup(flurm_db_jobs_ets, 9),
    ?assertEqual([<<"node1">>], AllocJob#job.allocated_nodes),

    %% Test exit_code update
    ets:insert(flurm_db_jobs_ets, {10, make_test_job(10)}),
    ok = flurm_db_persist:update_job(10, #{exit_code => 0}),
    [{10, ExitJob}] = ets:lookup(flurm_db_jobs_ets, 10),
    ?assertEqual(0, ExitJob#job.exit_code),

    %% Test start_time and end_time updates
    ets:insert(flurm_db_jobs_ets, {11, make_test_job(11)}),
    Now = erlang:system_time(second),
    ok = flurm_db_persist:update_job(11, #{start_time => Now, end_time => Now + 100}),
    [{11, TimeJob}] = ets:lookup(flurm_db_jobs_ets, 11),
    ?assertEqual(Now, TimeJob#job.start_time),
    ?assertEqual(Now + 100, TimeJob#job.end_time),

    %% Test unknown field update (should be ignored)
    ets:insert(flurm_db_jobs_ets, {12, make_test_job(12)}),
    ok = flurm_db_persist:update_job(12, #{unknown_field => value}),
    [{12, UnchangedJob}] = ets:lookup(flurm_db_jobs_ets, 12),
    ?assertEqual(pending, UnchangedJob#job.state).

delete_job_ets_test() ->
    Job = make_test_job(100),
    ets:insert(flurm_db_jobs_ets, {100, Job}),

    ok = flurm_db_persist:delete_job(100),
    ?assertEqual([], ets:lookup(flurm_db_jobs_ets, 100)).

list_jobs_ets_empty_test() ->
    %% Clear all jobs
    ets:delete_all_objects(flurm_db_jobs_ets),
    ?assertEqual([], flurm_db_persist:list_jobs()).

list_jobs_ets_with_jobs_test() ->
    ets:delete_all_objects(flurm_db_jobs_ets),
    ets:insert(flurm_db_jobs_ets, {1, make_test_job(1)}),
    ets:insert(flurm_db_jobs_ets, {2, make_test_job(2)}),

    Jobs = flurm_db_persist:list_jobs(),
    ?assertEqual(2, length(Jobs)).

next_job_id_ets_test() ->
    ets:insert(flurm_db_job_counter_ets, {counter, 0}),

    ?assertEqual(1, flurm_db_persist:next_job_id()),
    ?assertEqual(2, flurm_db_persist:next_job_id()),
    ?assertEqual(3, flurm_db_persist:next_job_id()).

%%====================================================================
%% Ra Backend Tests
%%====================================================================

store_job_ra_test() ->
    meck:expect(flurm_db_ra, submit_job, fun(_) -> {ok, 1} end),

    Job = make_test_job(1),
    ok = flurm_db_persist:store_job(Job),

    ?assert(meck:called(flurm_db_ra, submit_job, '_')).

update_job_ra_state_test() ->
    meck:expect(flurm_db_ra, update_job_state, fun(1, running) -> ok end),

    ok = flurm_db_persist:update_job(1, #{state => running}),
    ?assert(meck:called(flurm_db_ra, update_job_state, [1, running])).

update_job_ra_exit_code_test() ->
    meck:expect(flurm_db_ra, set_job_exit_code, fun(1, 0) -> ok end),

    ok = flurm_db_persist:update_job(1, #{exit_code => 0}),
    ?assert(meck:called(flurm_db_ra, set_job_exit_code, [1, 0])).

update_job_ra_allocated_nodes_test() ->
    meck:expect(flurm_db_ra, allocate_job, fun(1, [<<"node1">>]) -> ok end),

    ok = flurm_db_persist:update_job(1, #{allocated_nodes => [<<"node1">>]}),
    ?assert(meck:called(flurm_db_ra, allocate_job, [1, [<<"node1">>]])).

get_job_ra_test() ->
    RaJob = make_test_ra_job(1),
    meck:expect(flurm_db_ra, get_job, fun(1) -> {ok, RaJob} end),

    {ok, Job} = flurm_db_persist:get_job(1),
    ?assertEqual(1, Job#job.id),
    ?assert(meck:called(flurm_db_ra, get_job, [1])).

delete_job_ra_test() ->
    %% Ra doesn't support deletion, just returns ok
    ok = flurm_db_persist:delete_job(1).

list_jobs_ra_success_test() ->
    RaJobs = [make_test_ra_job(1), make_test_ra_job(2)],
    meck:expect(flurm_db_ra, list_jobs, fun() -> {ok, RaJobs} end),

    Jobs = flurm_db_persist:list_jobs(),
    ?assertEqual(2, length(Jobs)).

list_jobs_ra_error_test() ->
    meck:expect(flurm_db_ra, list_jobs, fun() -> {error, timeout} end),

    Jobs = flurm_db_persist:list_jobs(),
    ?assertEqual([], Jobs).

next_job_id_ra_success_test() ->
    meck:expect(flurm_db_ra, allocate_job_id, fun() -> {ok, 42} end),

    ?assertEqual(42, flurm_db_persist:next_job_id()).

next_job_id_ra_fallback_test() ->
    %% When Ra fails, should fall back to ETS
    meck:expect(flurm_db_ra, allocate_job_id, fun() -> {error, timeout} end),
    ets:insert(flurm_db_job_counter_ets, {counter, 99}),

    ?assertEqual(100, flurm_db_persist:next_job_id()).

%%====================================================================
%% Conversion Tests
%%====================================================================

ra_job_to_job_test() ->
    RaJob = make_test_ra_job(1),
    meck:expect(flurm_db_ra, get_job, fun(1) -> {ok, RaJob} end),

    {ok, Job} = flurm_db_persist:get_job(1),

    ?assertEqual(1, Job#job.id),
    ?assertEqual(<<"test_job">>, Job#job.name),
    ?assertEqual(<<"user1">>, Job#job.user),
    ?assertEqual(<<"batch">>, Job#job.partition),
    ?assertEqual(pending, Job#job.state),
    ?assertEqual(<<"#!/bin/bash">>, Job#job.script),
    ?assertEqual(1, Job#job.num_nodes),
    ?assertEqual(4, Job#job.num_cpus),
    ?assertEqual(1024, Job#job.memory_mb),
    ?assertEqual(3600, Job#job.time_limit),
    ?assertEqual(100, Job#job.priority),
    %% Default values for fields not in ra_job
    ?assertEqual(<<>>, Job#job.account),
    ?assertEqual(<<"normal">>, Job#job.qos).

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

make_test_ra_job(Id) ->
    #ra_job{
        id = Id,
        name = <<"test_job">>,
        user = <<"user1">>,
        group = <<"users">>,
        partition = <<"batch">>,
        state = pending,
        script = <<"#!/bin/bash">>,
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
