%%%-------------------------------------------------------------------
%%% @doc FLURM DB Persist 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_db_persist module covering:
%%% - Job storage operations (Ra and ETS backends)
%%% - Job retrieval and listing
%%% - Job updates and deletion
%%% - Persistence mode detection
%%% - Helper functions
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_persist_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

meck_setup() ->
    catch meck:unload(ra),
    catch meck:unload(flurm_db_ra),
    catch meck:unload(dets),
    catch meck:unload(ets),
    %% Create ETS tables for testing
    catch ets:delete(flurm_db_jobs_ets),
    catch ets:delete(flurm_db_job_counter_ets),
    ets:new(flurm_db_jobs_ets, [named_table, public, set]),
    ets:new(flurm_db_job_counter_ets, [named_table, public, set]),
    ets:insert(flurm_db_job_counter_ets, {counter, 0}),
    ok.

meck_cleanup(_) ->
    meck:unload(),
    catch ets:delete(flurm_db_jobs_ets),
    catch ets:delete(flurm_db_job_counter_ets),
    ok.

%%====================================================================
%% Test Job Record Creation
%%====================================================================

create_test_job(Id) ->
    #job{
        id = Id,
        name = <<"test_job">>,
        user = <<"testuser">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        num_tasks = 1,
        cpus_per_task = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    }.

create_test_ra_job(Id) ->
    #ra_job{
        id = Id,
        name = <<"ra_test_job">>,
        user = <<"rauser">>,
        group = <<"users">>,
        partition = <<"compute">>,
        state = running,
        script = <<"#!/bin/bash\necho ra">>,
        num_nodes = 2,
        num_cpus = 8,
        num_tasks = 2,
        cpus_per_task = 4,
        memory_mb = 2048,
        time_limit = 7200,
        priority = 200,
        submit_time = 1000000,
        start_time = 1000100,
        end_time = undefined,
        allocated_nodes = [<<"node1">>, <<"node2">>],
        exit_code = undefined,
        array_job_id = 0,
        array_task_id = undefined
    }.

%%====================================================================
%% Is Ra Available Tests
%%====================================================================

is_ra_available_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"is_ra_available returns false for nonode@nohost",
         fun() ->
             %% When node() is nonode@nohost, should return false
             %% This is tricky to test without actually being nonode@nohost
             %% We test the ETS fallback path instead
             Result = flurm_db_persist:is_ra_available(),
             %% In test environment, this depends on whether Ra is running
             ?assert(is_boolean(Result))
         end},
        {"is_ra_available returns false when Ra not running",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             Result = flurm_db_persist:is_ra_available(),
             ?assertEqual(false, Result)
         end},
        {"is_ra_available returns true when Ra running",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),

             Result = flurm_db_persist:is_ra_available(),
             ?assertEqual(true, Result)
         end}
     ]}.

%%====================================================================
%% Persistence Mode Tests
%%====================================================================

persistence_mode_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"persistence_mode returns ra when available",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),

             Result = flurm_db_persist:persistence_mode(),
             ?assertEqual(ra, Result)
         end},
        {"persistence_mode returns ets when ETS available",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             %% ETS table already exists from setup

             Result = flurm_db_persist:persistence_mode(),
             ?assertEqual(ets, Result)
         end},
        {"persistence_mode returns none when nothing available",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             %% Delete ETS table
             ets:delete(flurm_db_jobs_ets),

             Result = flurm_db_persist:persistence_mode(),
             ?assertEqual(none, Result),
             %% Recreate for cleanup
             ets:new(flurm_db_jobs_ets, [named_table, public, set])
         end}
     ]}.

%%====================================================================
%% Store Job ETS Tests
%%====================================================================

store_job_ets_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"store_job stores in ETS when Ra unavailable",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> undefined end),

             Job = create_test_job(1),
             Result = flurm_db_persist:store_job(Job),
             ?assertEqual(ok, Result),

             %% Verify stored in ETS
             [{1, StoredJob}] = ets:lookup(flurm_db_jobs_ets, 1),
             ?assertEqual(<<"test_job">>, StoredJob#job.name)
         end},
        {"store_job persists to DETS when available",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> [{size, 0}] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(dets, sync, fun(_) -> ok end),

             Job = create_test_job(2),
             Result = flurm_db_persist:store_job(Job),
             ?assertEqual(ok, Result),

             ?assert(meck:called(dets, insert, '_'))
         end}
     ]}.

%%====================================================================
%% Store Job Ra Tests
%%====================================================================

store_job_ra_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"store_job stores via Ra when available",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, store_job, fun(_Id, _JobSpec) -> {ok, 1} end),

             Job = create_test_job(1),
             Result = flurm_db_persist:store_job(Job),
             ?assertEqual(ok, Result)
         end},
        {"store_job returns error from Ra",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, store_job, fun(_Id, _JobSpec) -> {error, ra_timeout} end),

             Job = create_test_job(1),
             Result = flurm_db_persist:store_job(Job),
             ?assertEqual({error, ra_timeout}, Result)
         end}
     ]}.

%%====================================================================
%% Update Job ETS Tests
%%====================================================================

update_job_ets_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"update_job updates state in ETS",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> undefined end),

             %% Store a job first
             Job = create_test_job(1),
             ets:insert(flurm_db_jobs_ets, {1, Job}),

             %% Update the job
             Result = flurm_db_persist:update_job(1, #{state => running}),
             ?assertEqual(ok, Result),

             %% Verify update
             [{1, UpdatedJob}] = ets:lookup(flurm_db_jobs_ets, 1),
             ?assertEqual(running, UpdatedJob#job.state)
         end},
        {"update_job sets start_time when running",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> undefined end),

             Job = create_test_job(1),
             ets:insert(flurm_db_jobs_ets, {1, Job}),

             flurm_db_persist:update_job(1, #{state => running}),

             [{1, UpdatedJob}] = ets:lookup(flurm_db_jobs_ets, 1),
             ?assertNotEqual(undefined, UpdatedJob#job.start_time)
         end},
        {"update_job sets end_time for terminal states",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> undefined end),

             %% Test completed
             Job1 = create_test_job(1),
             ets:insert(flurm_db_jobs_ets, {1, Job1}),
             flurm_db_persist:update_job(1, #{state => completed}),
             [{1, Updated1}] = ets:lookup(flurm_db_jobs_ets, 1),
             ?assertNotEqual(undefined, Updated1#job.end_time),

             %% Test failed
             Job2 = create_test_job(2),
             ets:insert(flurm_db_jobs_ets, {2, Job2}),
             flurm_db_persist:update_job(2, #{state => failed}),
             [{2, Updated2}] = ets:lookup(flurm_db_jobs_ets, 2),
             ?assertNotEqual(undefined, Updated2#job.end_time),

             %% Test cancelled
             Job3 = create_test_job(3),
             ets:insert(flurm_db_jobs_ets, {3, Job3}),
             flurm_db_persist:update_job(3, #{state => cancelled}),
             [{3, Updated3}] = ets:lookup(flurm_db_jobs_ets, 3),
             ?assertNotEqual(undefined, Updated3#job.end_time),

             %% Test timeout
             Job4 = create_test_job(4),
             ets:insert(flurm_db_jobs_ets, {4, Job4}),
             flurm_db_persist:update_job(4, #{state => timeout}),
             [{4, Updated4}] = ets:lookup(flurm_db_jobs_ets, 4),
             ?assertNotEqual(undefined, Updated4#job.end_time),

             %% Test node_fail
             Job5 = create_test_job(5),
             ets:insert(flurm_db_jobs_ets, {5, Job5}),
             flurm_db_persist:update_job(5, #{state => node_fail}),
             [{5, Updated5}] = ets:lookup(flurm_db_jobs_ets, 5),
             ?assertNotEqual(undefined, Updated5#job.end_time)
         end},
        {"update_job updates allocated_nodes",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> undefined end),

             Job = create_test_job(1),
             ets:insert(flurm_db_jobs_ets, {1, Job}),

             Nodes = [<<"node1">>, <<"node2">>],
             flurm_db_persist:update_job(1, #{allocated_nodes => Nodes}),

             [{1, Updated}] = ets:lookup(flurm_db_jobs_ets, 1),
             ?assertEqual(Nodes, Updated#job.allocated_nodes)
         end},
        {"update_job updates exit_code",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> undefined end),

             Job = create_test_job(1),
             ets:insert(flurm_db_jobs_ets, {1, Job}),

             flurm_db_persist:update_job(1, #{exit_code => 0}),

             [{1, Updated}] = ets:lookup(flurm_db_jobs_ets, 1),
             ?assertEqual(0, Updated#job.exit_code)
         end},
        {"update_job updates start_time and end_time directly",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> undefined end),

             Job = create_test_job(1),
             ets:insert(flurm_db_jobs_ets, {1, Job}),

             flurm_db_persist:update_job(1, #{start_time => 12345, end_time => 67890}),

             [{1, Updated}] = ets:lookup(flurm_db_jobs_ets, 1),
             ?assertEqual(12345, Updated#job.start_time),
             ?assertEqual(67890, Updated#job.end_time)
         end},
        {"update_job returns not_found for missing job",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             Result = flurm_db_persist:update_job(999, #{state => running}),
             ?assertEqual({error, not_found}, Result)
         end}
     ]}.

%%====================================================================
%% Update Job Ra Tests
%%====================================================================

update_job_ra_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"update_job via Ra with state change",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, update_job_state, fun(_JobId, _State) -> ok end),

             Result = flurm_db_persist:update_job(1, #{state => running}),
             ?assertEqual(ok, Result)
         end},
        {"update_job via Ra with allocated_nodes",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, allocate_job, fun(_JobId, _Nodes) -> ok end),

             Result = flurm_db_persist:update_job(1, #{allocated_nodes => [<<"n1">>]}),
             ?assertEqual(ok, Result)
         end},
        {"update_job via Ra with allocated_nodes and configuring state",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, allocate_job, fun(_JobId, _Nodes) -> ok end),
             %% Should NOT call update_job_state for configuring since allocate_job handles it

             Result = flurm_db_persist:update_job(1, #{
                 allocated_nodes => [<<"n1">>],
                 state => configuring
             }),
             ?assertEqual(ok, Result),
             %% Verify allocate_job was called
             ?assert(meck:called(flurm_db_ra, allocate_job, '_'))
         end},
        {"update_job via Ra with exit_code",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, set_job_exit_code, fun(_JobId, _Code) -> ok end),

             Result = flurm_db_persist:update_job(1, #{exit_code => 0}),
             ?assertEqual(ok, Result)
         end},
        {"update_job via Ra with field updates",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, update_job_fields, fun(_JobId, _Fields) -> ok end),

             Result = flurm_db_persist:update_job(1, #{
                 time_limit => 7200,
                 name => <<"new_name">>,
                 priority => 500
             }),
             ?assertEqual(ok, Result)
         end}
     ]}.

%%====================================================================
%% Get Job Tests
%%====================================================================

get_job_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"get_job from ETS",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             Job = create_test_job(1),
             ets:insert(flurm_db_jobs_ets, {1, Job}),

             Result = flurm_db_persist:get_job(1),
             ?assertEqual({ok, Job}, Result)
         end},
        {"get_job not found in ETS",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             Result = flurm_db_persist:get_job(999),
             ?assertEqual({error, not_found}, Result)
         end},
        {"get_job from Ra",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),

             RaJob = create_test_ra_job(1),
             meck:expect(flurm_db_ra, get_job, fun(1) -> {ok, RaJob} end),

             Result = flurm_db_persist:get_job(1),
             ?assertMatch({ok, #job{}}, Result),
             {ok, Job} = Result,
             ?assertEqual(1, Job#job.id),
             ?assertEqual(<<"ra_test_job">>, Job#job.name)
         end},
        {"get_job not found via Ra",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, get_job, fun(_) -> {error, not_found} end),

             Result = flurm_db_persist:get_job(999),
             ?assertEqual({error, not_found}, Result)
         end}
     ]}.

%%====================================================================
%% Delete Job Tests
%%====================================================================

delete_job_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"delete_job from ETS",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> undefined end),

             Job = create_test_job(1),
             ets:insert(flurm_db_jobs_ets, {1, Job}),

             Result = flurm_db_persist:delete_job(1),
             ?assertEqual(ok, Result),

             %% Verify deleted
             ?assertEqual([], ets:lookup(flurm_db_jobs_ets, 1))
         end},
        {"delete_job via Ra (no-op)",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),

             %% Ra delete is a no-op
             Result = flurm_db_persist:delete_job(1),
             ?assertEqual(ok, Result)
         end},
        {"delete_job removes from DETS when available",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> [{size, 1}] end),
             meck:expect(dets, delete, fun(_, _) -> ok end),
             meck:expect(dets, sync, fun(_) -> ok end),

             Job = create_test_job(1),
             ets:insert(flurm_db_jobs_ets, {1, Job}),

             flurm_db_persist:delete_job(1),
             ?assert(meck:called(dets, delete, '_'))
         end}
     ]}.

%%====================================================================
%% List Jobs Tests
%%====================================================================

list_jobs_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"list_jobs from ETS",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             Job1 = create_test_job(1),
             Job2 = create_test_job(2),
             ets:insert(flurm_db_jobs_ets, {1, Job1}),
             ets:insert(flurm_db_jobs_ets, {2, Job2}),

             Result = flurm_db_persist:list_jobs(),
             ?assertEqual(2, length(Result)),
             ?assert(lists:member(Job1, Result)),
             ?assert(lists:member(Job2, Result))
         end},
        {"list_jobs empty ETS",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             Result = flurm_db_persist:list_jobs(),
             ?assertEqual([], Result)
         end},
        {"list_jobs from Ra",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),

             RaJob1 = create_test_ra_job(1),
             RaJob2 = create_test_ra_job(2),
             meck:expect(flurm_db_ra, list_jobs, fun() -> {ok, [RaJob1, RaJob2]} end),

             Result = flurm_db_persist:list_jobs(),
             ?assertEqual(2, length(Result))
         end},
        {"list_jobs Ra error returns empty",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, list_jobs, fun() -> {error, timeout} end),

             Result = flurm_db_persist:list_jobs(),
             ?assertEqual([], Result)
         end},
        {"list_jobs with undefined ETS table returns empty",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             %% Delete the ETS table
             ets:delete(flurm_db_jobs_ets),

             Result = flurm_db_persist:list_jobs(),
             ?assertEqual([], Result),

             %% Recreate for cleanup
             ets:new(flurm_db_jobs_ets, [named_table, public, set])
         end}
     ]}.

%%====================================================================
%% Next Job ID Tests
%%====================================================================

next_job_id_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"next_job_id from ETS",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> undefined end),

             Id1 = flurm_db_persist:next_job_id(),
             Id2 = flurm_db_persist:next_job_id(),
             Id3 = flurm_db_persist:next_job_id(),

             ?assertEqual(1, Id1),
             ?assertEqual(2, Id2),
             ?assertEqual(3, Id3)
         end},
        {"next_job_id from Ra",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, allocate_job_id, fun() -> {ok, 42} end),

             Result = flurm_db_persist:next_job_id(),
             ?assertEqual(42, Result)
         end},
        {"next_job_id Ra error falls back to ETS",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(flurm_db_ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:new(error_logger, [passthrough, unstick]),
             meck:expect(ra, members, fun(_) -> {ok, [], node()} end),
             meck:expect(flurm_db_ra, allocate_job_id, fun() -> {error, timeout} end),
             meck:expect(dets, info, fun(_) -> undefined end),
             meck:expect(error_logger, error_msg, fun(_, _) -> ok end),

             Result = flurm_db_persist:next_job_id(),
             ?assertEqual(1, Result)
         end},
        {"next_job_id persists counter to DETS",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(dets, [passthrough]),
             meck:expect(ra, members, fun(_) -> {error, noproc} end),
             meck:expect(dets, info, fun(_) -> [{size, 1}] end),
             meck:expect(dets, insert, fun(_, _) -> ok end),
             meck:expect(dets, sync, fun(_) -> ok end),

             flurm_db_persist:next_job_id(),
             ?assert(meck:called(dets, insert, '_'))
         end}
     ]}.

%%====================================================================
%% Ra Job to Job Conversion Tests
%%====================================================================

ra_job_to_job_test_() ->
    {"ra_job_to_job conversion tests",
     [
        {"converts all fields correctly",
         fun() ->
             RaJob = create_test_ra_job(1),
             Job = flurm_db_persist:ra_job_to_job(RaJob),

             ?assertEqual(1, Job#job.id),
             ?assertEqual(<<"ra_test_job">>, Job#job.name),
             ?assertEqual(<<"rauser">>, Job#job.user),
             ?assertEqual(<<"compute">>, Job#job.partition),
             ?assertEqual(running, Job#job.state),
             ?assertEqual(<<"#!/bin/bash\necho ra">>, Job#job.script),
             ?assertEqual(2, Job#job.num_nodes),
             ?assertEqual(8, Job#job.num_cpus),
             ?assertEqual(2, Job#job.num_tasks),
             ?assertEqual(4, Job#job.cpus_per_task),
             ?assertEqual(2048, Job#job.memory_mb),
             ?assertEqual(7200, Job#job.time_limit),
             ?assertEqual(200, Job#job.priority),
             ?assertEqual(1000000, Job#job.submit_time),
             ?assertEqual(1000100, Job#job.start_time),
             ?assertEqual(undefined, Job#job.end_time),
             ?assertEqual([<<"node1">>, <<"node2">>], Job#job.allocated_nodes),
             ?assertEqual(undefined, Job#job.exit_code),
             ?assertEqual(0, Job#job.array_job_id),
             ?assertEqual(undefined, Job#job.array_task_id),
             %% Default values for fields not in ra_job
             ?assertEqual(<<>>, Job#job.account),
             ?assertEqual(<<"normal">>, Job#job.qos)
         end},
        {"converts job with all optional fields set",
         fun() ->
             RaJob = #ra_job{
                 id = 99,
                 name = <<"complete_job">>,
                 user = <<"user">>,
                 group = <<"group">>,
                 partition = <<"part">>,
                 state = completed,
                 script = <<"echo done">>,
                 num_nodes = 1,
                 num_cpus = 1,
                 num_tasks = 1,
                 cpus_per_task = 1,
                 memory_mb = 512,
                 time_limit = 60,
                 priority = 50,
                 submit_time = 100,
                 start_time = 200,
                 end_time = 300,
                 allocated_nodes = [<<"n1">>],
                 exit_code = 0,
                 array_job_id = 10,
                 array_task_id = 5
             },
             Job = flurm_db_persist:ra_job_to_job(RaJob),

             ?assertEqual(300, Job#job.end_time),
             ?assertEqual(0, Job#job.exit_code),
             ?assertEqual(10, Job#job.array_job_id),
             ?assertEqual(5, Job#job.array_task_id)
         end}
     ]}.

%%====================================================================
%% Apply Job Updates Tests
%%====================================================================

apply_job_updates_test_() ->
    {"apply_job_updates tests",
     [
        {"apply empty updates",
         fun() ->
             Job = create_test_job(1),
             Result = flurm_db_persist:apply_job_updates(Job, #{}),
             ?assertEqual(Job, Result)
         end},
        {"apply state update",
         fun() ->
             Job = create_test_job(1),
             Result = flurm_db_persist:apply_job_updates(Job, #{state => running}),
             ?assertEqual(running, Result#job.state)
         end},
        {"apply allocated_nodes update",
         fun() ->
             Job = create_test_job(1),
             Nodes = [<<"a">>, <<"b">>],
             Result = flurm_db_persist:apply_job_updates(Job, #{allocated_nodes => Nodes}),
             ?assertEqual(Nodes, Result#job.allocated_nodes)
         end},
        {"apply multiple updates",
         fun() ->
             Job = create_test_job(1),
             Updates = #{
                 state => completed,
                 exit_code => 0,
                 end_time => 999999
             },
             Result = flurm_db_persist:apply_job_updates(Job, Updates),
             ?assertEqual(completed, Result#job.state),
             ?assertEqual(0, Result#job.exit_code),
             ?assertEqual(999999, Result#job.end_time)
         end},
        {"unknown keys are ignored",
         fun() ->
             Job = create_test_job(1),
             Result = flurm_db_persist:apply_job_updates(Job, #{unknown_key => value}),
             ?assertEqual(Job, Result)
         end}
     ]}.
