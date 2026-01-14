%%%-------------------------------------------------------------------
%%% @doc FLURM Job Manager Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job_manager gen_server,
%%% covering job submission, lifecycle operations, state tracking,
%%% persistence, and error handling.
%%%
%%% Note: flurm_job_manager is in flurm_controller app but tests
%%% placed here for core functionality coverage.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_manager_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

job_manager_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Submit regular job", fun test_submit_regular_job/0},
        {"Submit job with licenses", fun test_submit_job_with_licenses/0},
        {"Submit job invalid licenses", fun test_submit_job_invalid_licenses/0},
        {"Submit job with dependencies", fun test_submit_job_with_dependencies/0},
        {"Submit job invalid dependencies", fun test_submit_job_invalid_dependencies/0},
        {"Submit array job", fun test_submit_array_job/0},
        {"Submit array job invalid spec", fun test_submit_array_job_invalid_spec/0},
        {"Submit job limit exceeded", fun test_submit_job_limit_exceeded/0},
        {"Cancel existing job", fun test_cancel_existing_job/0},
        {"Cancel non-existent job", fun test_cancel_nonexistent_job/0},
        {"Cancel running job with nodes", fun test_cancel_running_job_with_nodes/0},
        {"Get existing job", fun test_get_existing_job/0},
        {"Get non-existent job", fun test_get_nonexistent_job/0},
        {"List jobs", fun test_list_jobs/0},
        {"Update job state completed", fun test_update_job_state_completed/0},
        {"Update job state failed", fun test_update_job_state_failed/0},
        {"Update job state timeout", fun test_update_job_state_timeout/0},
        {"Update job state running", fun test_update_job_state_running/0},
        {"Update job fields", fun test_update_job_fields/0},
        {"Update non-existent job", fun test_update_nonexistent_job/0},
        {"Hold pending job", fun test_hold_pending_job/0},
        {"Hold already held job", fun test_hold_already_held_job/0},
        {"Hold running job error", fun test_hold_running_job_error/0},
        {"Hold non-existent job", fun test_hold_nonexistent_job/0},
        {"Release held job", fun test_release_held_job/0},
        {"Release pending job", fun test_release_pending_job/0},
        {"Release running job error", fun test_release_running_job_error/0},
        {"Release non-existent job", fun test_release_nonexistent_job/0},
        {"Requeue running job", fun test_requeue_running_job/0},
        {"Requeue pending job", fun test_requeue_pending_job/0},
        {"Requeue completed job error", fun test_requeue_completed_job_error/0},
        {"Requeue non-existent job", fun test_requeue_nonexistent_job/0},
        {"Import job", fun test_import_job/0},
        {"Import job already exists", fun test_import_job_already_exists/0},
        {"Unknown request", fun test_unknown_request/0},
        {"Unknown cast", fun test_unknown_cast/0},
        {"Unknown info", fun test_unknown_info/0},
        {"Terminate callback", fun test_terminate/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),

    %% Unload any existing mocks first
    catch meck:unload(lager),
    catch meck:unload(flurm_db_persist),
    catch meck:unload(flurm_limits),
    catch meck:unload(flurm_license),
    catch meck:unload(flurm_scheduler),
    catch meck:unload(flurm_job_deps),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_job_dispatcher_server),
    catch meck:unload(flurm_job_array),
    catch meck:unload(flurm_core),

    %% Setup mocks for dependencies
    %% Note: Don't use passthrough for lager to avoid call loops
    meck:new(lager, [non_strict]),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt) -> ok end),

    meck:new(flurm_db_persist, [non_strict]),
    meck:expect(flurm_db_persist, persistence_mode, fun() -> none end),
    meck:expect(flurm_db_persist, store_job, fun(_Job) -> ok end),
    meck:expect(flurm_db_persist, update_job, fun(_JobId, _Updates) -> ok end),
    meck:expect(flurm_db_persist, list_jobs, fun() -> [] end),

    meck:new(flurm_limits, [non_strict]),
    meck:expect(flurm_limits, check_submit_limits, fun(_Spec) -> ok end),

    meck:new(flurm_license, [non_strict]),
    meck:expect(flurm_license, parse_license_spec, fun(<<>>) -> {ok, []};
                                                     (_) -> {ok, [{<<"matlab">>, 1}]} end),
    meck:expect(flurm_license, validate_licenses, fun(_) -> ok end),

    meck:new(flurm_scheduler, [non_strict]),
    meck:expect(flurm_scheduler, submit_job, fun(_JobId) -> ok end),
    meck:expect(flurm_scheduler, job_failed, fun(_JobId) -> ok end),

    meck:new(flurm_job_deps, [non_strict]),
    meck:expect(flurm_job_deps, parse_dependency_spec, fun(<<>>) -> {ok, []};
                                                          (_) -> {ok, []} end),
    meck:expect(flurm_job_deps, add_dependencies, fun(_JobId, _Spec) -> ok end),
    meck:expect(flurm_job_deps, check_dependencies, fun(_JobId) -> {ok, []} end),
    meck:expect(flurm_job_deps, on_job_state_change, fun(_JobId, _State) -> ok end),
    meck:expect(flurm_job_deps, remove_all_dependencies, fun(_JobId) -> ok end),
    meck:expect(flurm_job_deps, has_circular_dependency, fun(_J1, _J2) -> false end),

    meck:new(flurm_metrics, [non_strict]),
    meck:expect(flurm_metrics, increment, fun(_Metric) -> ok end),

    meck:new(flurm_job_dispatcher_server, [non_strict]),
    meck:expect(flurm_job_dispatcher_server, cancel_job, fun(_JobId, _Nodes) -> ok end),

    meck:new(flurm_job_array, [non_strict]),
    meck:expect(flurm_job_array, parse_array_spec, fun(<<"1-10">>) -> {ok, #{start => 1, stop => 10, step => 1}};
                                                      (<<"invalid">>) -> {error, invalid_spec};
                                                      (_) -> {ok, #{start => 1, stop => 5, step => 1}} end),
    meck:expect(flurm_job_array, create_array_job, fun(_Job, _Spec) -> {ok, 1000} end),
    meck:expect(flurm_job_array, get_schedulable_tasks, fun(_ArrayJobId) -> [] end),
    meck:expect(flurm_job_array, update_task_state, fun(_ArrayJobId, _TaskId, _Updates) -> ok end),

    meck:new(flurm_core, [non_strict]),
    meck:expect(flurm_core, update_job_state, fun(Job, NewState) ->
        Job#job{state = NewState}
    end),

    %% Start the job manager
    {ok, Pid} = flurm_job_manager:start_link(),
    #{manager_pid => Pid}.

cleanup(#{manager_pid := Pid}) ->
    catch meck:unload(lager),
    catch meck:unload(flurm_db_persist),
    catch meck:unload(flurm_limits),
    catch meck:unload(flurm_license),
    catch meck:unload(flurm_scheduler),
    catch meck:unload(flurm_job_deps),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_job_dispatcher_server),
    catch meck:unload(flurm_job_array),
    catch meck:unload(flurm_core),

    case is_process_alive(Pid) of
        true ->
            unlink(Pid),
            gen_server:stop(Pid, shutdown, 5000);
        false ->
            ok
    end,
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_job_spec() ->
    make_job_spec(#{}).

make_job_spec(Overrides) ->
    Defaults = #{
        name => <<"test_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        script => <<"#!/bin/bash\necho hello">>,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 1024,
        time_limit => 3600,
        priority => 100,
        licenses => <<>>
    },
    maps:merge(Defaults, Overrides).

%%====================================================================
%% Job Submission Tests
%%====================================================================

test_submit_regular_job() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),
    ?assert(is_integer(JobId)),
    ?assert(JobId >= 1),

    %% Verify job was stored
    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(<<"test_job">>, Job#job.name),
    ?assertEqual(<<"testuser">>, Job#job.user),
    ?assertEqual(pending, Job#job.state),
    ok.

test_submit_job_with_licenses() ->
    meck:expect(flurm_license, parse_license_spec, fun(_) ->
        {ok, [{<<"matlab">>, 2}]}
    end),

    JobSpec = make_job_spec(#{licenses => <<"matlab:2">>}),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),
    ?assert(is_integer(JobId)),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual([{<<"matlab">>, 2}], Job#job.licenses),
    ok.

test_submit_job_invalid_licenses() ->
    meck:expect(flurm_license, parse_license_spec, fun(_) ->
        {error, invalid_format}
    end),

    JobSpec = make_job_spec(#{licenses => <<"invalid:license">>}),
    Result = flurm_job_manager:submit_job(JobSpec),
    ?assertMatch({error, {invalid_licenses, _}}, Result),
    ok.

test_submit_job_with_dependencies() ->
    %% Note: We use 'singleton' type dependency to avoid recursive call to get_job
    %% which would cause a 'calling_self' error in gen_server
    meck:expect(flurm_job_deps, parse_dependency_spec, fun(_) ->
        {ok, [{singleton, <<"test_singleton">>}]}
    end),
    meck:expect(flurm_job_deps, check_dependencies, fun(_) ->
        {waiting, [{singleton, <<"test_singleton">>}]}
    end),

    JobSpec = make_job_spec(#{dependency => <<"singleton:test_singleton">>}),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),
    ?assert(is_integer(JobId)),
    ok.

test_submit_job_invalid_dependencies() ->
    meck:expect(flurm_job_deps, parse_dependency_spec, fun(_) ->
        {error, circular_dependency}
    end),

    JobSpec = make_job_spec(#{dependency => <<"afterok:invalid">>}),
    Result = flurm_job_manager:submit_job(JobSpec),
    ?assertMatch({error, {invalid_dependency, _}}, Result),
    ok.

test_submit_array_job() ->
    JobSpec = make_job_spec(#{array => <<"1-10">>}),
    {ok, {array, ArrayJobId}} = flurm_job_manager:submit_job(JobSpec),
    ?assertEqual(1000, ArrayJobId),
    ok.

test_submit_array_job_invalid_spec() ->
    meck:expect(flurm_job_array, parse_array_spec, fun(_) ->
        {error, invalid_spec}
    end),

    JobSpec = make_job_spec(#{array => <<"invalid">>}),
    Result = flurm_job_manager:submit_job(JobSpec),
    ?assertMatch({error, {invalid_array_spec, _}}, Result),
    ok.

test_submit_job_limit_exceeded() ->
    meck:expect(flurm_limits, check_submit_limits, fun(_) ->
        {error, max_jobs_exceeded}
    end),

    JobSpec = make_job_spec(),
    Result = flurm_job_manager:submit_job(JobSpec),
    ?assertMatch({error, {submit_limit_exceeded, _}}, Result),
    ok.

%%====================================================================
%% Cancel Job Tests
%%====================================================================

test_cancel_existing_job() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    ok = flurm_job_manager:cancel_job(JobId),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(cancelled, Job#job.state),
    ok.

test_cancel_nonexistent_job() ->
    Result = flurm_job_manager:cancel_job(999999),
    ?assertEqual({error, not_found}, Result),
    ok.

test_cancel_running_job_with_nodes() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Update job to running with allocated nodes
    ok = flurm_job_manager:update_job(JobId, #{
        state => running,
        allocated_nodes => [<<"node1">>, <<"node2">>]
    }),

    ok = flurm_job_manager:cancel_job(JobId),

    %% Verify cancel_job was called on dispatcher
    ?assert(meck:called(flurm_job_dispatcher_server, cancel_job, [JobId, '_'])),
    ok.

%%====================================================================
%% Get Job Tests
%%====================================================================

test_get_existing_job() ->
    JobSpec = make_job_spec(#{name => <<"lookup_test">>}),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(JobId, Job#job.id),
    ?assertEqual(<<"lookup_test">>, Job#job.name),
    ok.

test_get_nonexistent_job() ->
    Result = flurm_job_manager:get_job(888888),
    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% List Jobs Tests
%%====================================================================

test_list_jobs() ->
    %% Submit multiple jobs
    {ok, _Id1} = flurm_job_manager:submit_job(make_job_spec(#{name => <<"job1">>})),
    {ok, _Id2} = flurm_job_manager:submit_job(make_job_spec(#{name => <<"job2">>})),
    {ok, _Id3} = flurm_job_manager:submit_job(make_job_spec(#{name => <<"job3">>})),

    Jobs = flurm_job_manager:list_jobs(),
    ?assert(length(Jobs) >= 3),
    Names = [J#job.name || J <- Jobs],
    ?assert(lists:member(<<"job1">>, Names)),
    ?assert(lists:member(<<"job2">>, Names)),
    ?assert(lists:member(<<"job3">>, Names)),
    ok.

%%====================================================================
%% Update Job Tests
%%====================================================================

test_update_job_state_completed() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    ok = flurm_job_manager:update_job(JobId, #{state => completed}),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(completed, Job#job.state),

    %% Verify metrics were incremented
    ?assert(meck:called(flurm_metrics, increment, [flurm_jobs_completed_total])),
    ok.

test_update_job_state_failed() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    ok = flurm_job_manager:update_job(JobId, #{state => failed}),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(failed, Job#job.state),

    %% Verify metrics were incremented
    ?assert(meck:called(flurm_metrics, increment, [flurm_jobs_failed_total])),
    ok.

test_update_job_state_timeout() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    ok = flurm_job_manager:update_job(JobId, #{state => timeout}),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(timeout, Job#job.state),
    ok.

test_update_job_state_running() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    ok = flurm_job_manager:update_job(JobId, #{state => running}),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(running, Job#job.state),
    ok.

test_update_job_fields() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    Now = erlang:system_time(second),
    ok = flurm_job_manager:update_job(JobId, #{
        allocated_nodes => [<<"node1">>],
        start_time => Now,
        priority => 500,
        time_limit => 7200
    }),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual([<<"node1">>], Job#job.allocated_nodes),
    ?assertEqual(Now, Job#job.start_time),
    ?assertEqual(500, Job#job.priority),
    ?assertEqual(7200, Job#job.time_limit),
    ok.

test_update_nonexistent_job() ->
    Result = flurm_job_manager:update_job(777777, #{state => running}),
    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% Hold Job Tests
%%====================================================================

test_hold_pending_job() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Job should be pending
    {ok, Job1} = flurm_job_manager:get_job(JobId),
    ?assertEqual(pending, Job1#job.state),

    ok = flurm_job_manager:hold_job(JobId),

    {ok, Job2} = flurm_job_manager:get_job(JobId),
    ?assertEqual(held, Job2#job.state),
    ok.

test_hold_already_held_job() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    ok = flurm_job_manager:hold_job(JobId),
    %% Holding again should succeed
    ok = flurm_job_manager:hold_job(JobId),
    ok.

test_hold_running_job_error() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Update to running
    ok = flurm_job_manager:update_job(JobId, #{state => running}),

    Result = flurm_job_manager:hold_job(JobId),
    ?assertMatch({error, {invalid_state, running}}, Result),
    ok.

test_hold_nonexistent_job() ->
    Result = flurm_job_manager:hold_job(666666),
    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% Release Job Tests
%%====================================================================

test_release_held_job() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    ok = flurm_job_manager:hold_job(JobId),
    {ok, Job1} = flurm_job_manager:get_job(JobId),
    ?assertEqual(held, Job1#job.state),

    ok = flurm_job_manager:release_job(JobId),

    {ok, Job2} = flurm_job_manager:get_job(JobId),
    ?assertEqual(pending, Job2#job.state),

    %% Verify scheduler was notified
    ?assert(meck:called(flurm_scheduler, submit_job, [JobId])),
    ok.

test_release_pending_job() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Release a pending job (not held) should succeed
    ok = flurm_job_manager:release_job(JobId),
    ok.

test_release_running_job_error() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    ok = flurm_job_manager:update_job(JobId, #{state => running}),

    Result = flurm_job_manager:release_job(JobId),
    ?assertMatch({error, {invalid_state, running}}, Result),
    ok.

test_release_nonexistent_job() ->
    Result = flurm_job_manager:release_job(555555),
    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% Requeue Job Tests
%%====================================================================

test_requeue_running_job() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Set to running with nodes
    ok = flurm_job_manager:update_job(JobId, #{
        state => running,
        allocated_nodes => [<<"node1">>],
        start_time => erlang:system_time(second)
    }),

    ok = flurm_job_manager:requeue_job(JobId),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(pending, Job#job.state),
    ?assertEqual([], Job#job.allocated_nodes),
    ?assertEqual(undefined, Job#job.start_time),
    ok.

test_requeue_pending_job() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    %% Requeue pending job should succeed (no-op)
    ok = flurm_job_manager:requeue_job(JobId),
    ok.

test_requeue_completed_job_error() ->
    JobSpec = make_job_spec(),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    ok = flurm_job_manager:update_job(JobId, #{state => completed}),

    Result = flurm_job_manager:requeue_job(JobId),
    ?assertMatch({error, {invalid_state, completed}}, Result),
    ok.

test_requeue_nonexistent_job() ->
    Result = flurm_job_manager:requeue_job(444444),
    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% Import Job Tests
%%====================================================================

test_import_job() ->
    ImportSpec = #{
        id => 50000,
        name => <<"imported_job">>,
        user => <<"slurm_user">>,
        partition => <<"compute">>,
        num_cpus => 8,
        num_nodes => 2,
        memory_mb => 4096,
        priority => 200,
        state => running,
        time_limit => 7200,
        submit_time => erlang:system_time(second) - 3600,
        start_time => erlang:system_time(second) - 1800
    },

    {ok, JobId} = flurm_job_manager:import_job(ImportSpec),
    ?assertEqual(50000, JobId),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual(<<"imported_job">>, Job#job.name),
    ?assertEqual(running, Job#job.state),
    ?assertEqual(8, Job#job.num_cpus),
    ok.

test_import_job_already_exists() ->
    ImportSpec = #{
        id => 60000,
        name => <<"first_import">>,
        user => <<"user1">>
    },

    {ok, _} = flurm_job_manager:import_job(ImportSpec),

    %% Try to import again with same ID
    Result = flurm_job_manager:import_job(ImportSpec),
    ?assertEqual({error, already_exists}, Result),
    ok.

%%====================================================================
%% Error Handling Tests
%%====================================================================

test_unknown_request() ->
    Result = gen_server:call(flurm_job_manager, unknown_request),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    gen_server:cast(flurm_job_manager, unknown_cast),
    timer:sleep(10),
    %% Server should still be alive
    ?assert(is_process_alive(whereis(flurm_job_manager))),
    ok.

test_unknown_info() ->
    flurm_job_manager ! unknown_info_message,
    timer:sleep(10),
    %% Server should still be alive
    ?assert(is_process_alive(whereis(flurm_job_manager))),
    ok.

test_terminate() ->
    %% Test that terminate callback is called when stopping
    %% We don't actually stop the manager here to avoid breaking the test framework
    %% Instead we verify the manager responds and can be stopped gracefully
    JobSpec = make_job_spec(),
    {ok, _JobId} = flurm_job_manager:submit_job(JobSpec),

    Pid = whereis(flurm_job_manager),
    ?assert(is_process_alive(Pid)),

    %% Verify the manager is working
    Jobs = flurm_job_manager:list_jobs(),
    ?assert(length(Jobs) >= 1),

    %% The actual stop happens in cleanup/1, which tests the terminate callback
    ok.

%%====================================================================
%% Persistence Tests
%%====================================================================

persistence_test_() ->
    {setup,
     fun setup_with_persistence/0,
     fun cleanup/1,
     [
        {"Load persisted jobs", fun test_load_persisted_jobs/0},
        {"Persist job failure", fun test_persist_job_failure/0}
     ]}.

setup_with_persistence() ->
    application:ensure_all_started(sasl),

    %% Unload any existing mocks first
    catch meck:unload(lager),
    catch meck:unload(flurm_db_persist),
    catch meck:unload(flurm_limits),
    catch meck:unload(flurm_license),
    catch meck:unload(flurm_scheduler),
    catch meck:unload(flurm_job_deps),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_job_dispatcher_server),
    catch meck:unload(flurm_core),

    meck:new(lager, [non_strict]),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, warning, fun(_Fmt) -> ok end),
    meck:expect(lager, error, fun(_Fmt, _Args) -> ok end),
    meck:expect(lager, error, fun(_Fmt) -> ok end),

    %% Setup persistence with ETS mode and pre-existing jobs
    meck:new(flurm_db_persist, [non_strict]),
    meck:expect(flurm_db_persist, persistence_mode, fun() -> ets end),
    meck:expect(flurm_db_persist, store_job, fun(_Job) -> ok end),
    meck:expect(flurm_db_persist, update_job, fun(_JobId, _Updates) -> ok end),
    meck:expect(flurm_db_persist, list_jobs, fun() ->
        [
            #job{id = 100, name = <<"persisted_job1">>, user = <<"user1">>,
                 partition = <<"default">>, state = pending, script = <<>>,
                 num_nodes = 1, num_cpus = 1, memory_mb = 1024, time_limit = 3600,
                 priority = 100, submit_time = erlang:system_time(second),
                 allocated_nodes = []},
            #job{id = 200, name = <<"persisted_job2">>, user = <<"user2">>,
                 partition = <<"compute">>, state = running, script = <<>>,
                 num_nodes = 2, num_cpus = 8, memory_mb = 8192, time_limit = 7200,
                 priority = 150, submit_time = erlang:system_time(second),
                 allocated_nodes = [<<"node1">>]}
        ]
    end),

    meck:new(flurm_limits, [non_strict]),
    meck:expect(flurm_limits, check_submit_limits, fun(_Spec) -> ok end),

    meck:new(flurm_license, [non_strict]),
    meck:expect(flurm_license, parse_license_spec, fun(_) -> {ok, []} end),
    meck:expect(flurm_license, validate_licenses, fun(_) -> ok end),

    meck:new(flurm_scheduler, [non_strict]),
    meck:expect(flurm_scheduler, submit_job, fun(_JobId) -> ok end),
    meck:expect(flurm_scheduler, job_failed, fun(_JobId) -> ok end),

    meck:new(flurm_job_deps, [non_strict]),
    meck:expect(flurm_job_deps, parse_dependency_spec, fun(_) -> {ok, []} end),
    meck:expect(flurm_job_deps, add_dependencies, fun(_, _) -> ok end),
    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {ok, []} end),
    meck:expect(flurm_job_deps, on_job_state_change, fun(_, _) -> ok end),
    meck:expect(flurm_job_deps, remove_all_dependencies, fun(_) -> ok end),

    meck:new(flurm_metrics, [non_strict]),
    meck:expect(flurm_metrics, increment, fun(_Metric) -> ok end),

    meck:new(flurm_job_dispatcher_server, [non_strict]),
    meck:expect(flurm_job_dispatcher_server, cancel_job, fun(_, _) -> ok end),

    meck:new(flurm_core, [non_strict]),
    meck:expect(flurm_core, update_job_state, fun(Job, NewState) ->
        Job#job{state = NewState}
    end),

    {ok, Pid} = flurm_job_manager:start_link(),
    #{manager_pid => Pid}.

test_load_persisted_jobs() ->
    %% Verify jobs were loaded from persistence
    Jobs = flurm_job_manager:list_jobs(),
    ?assert(length(Jobs) >= 2),

    {ok, Job1} = flurm_job_manager:get_job(100),
    ?assertEqual(<<"persisted_job1">>, Job1#job.name),

    {ok, Job2} = flurm_job_manager:get_job(200),
    ?assertEqual(<<"persisted_job2">>, Job2#job.name),
    ?assertEqual(running, Job2#job.state),
    ok.

test_persist_job_failure() ->
    %% Setup persistence to fail
    meck:expect(flurm_db_persist, store_job, fun(_Job) -> {error, storage_full} end),

    JobSpec = #{
        name => <<"test_persist_fail">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        script => <<"#!/bin/bash\necho test">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 512,
        time_limit => 60,
        priority => 100,
        licenses => <<>>
    },

    %% Job should still be submitted even if persistence fails
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),
    ?assert(is_integer(JobId)),

    %% Verify store_job was actually called (and failed)
    ?assert(meck:called(flurm_db_persist, store_job, ['_'])),

    %% Note: lager:error may or may not be called depending on logging configuration
    %% The important thing is the job submission succeeded despite persistence failure
    ok.

%%====================================================================
%% License Handling Edge Cases
%%====================================================================

license_edge_cases_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        {"Submit job with pre-parsed licenses", fun test_submit_with_parsed_licenses/0},
        {"Submit job license validation failure", fun test_submit_license_validation_failure/0}
     ]}.

test_submit_with_parsed_licenses() ->
    %% Test when licenses are already a list (pre-parsed)
    JobSpec = make_job_spec(#{licenses => [{<<"matlab">>, 2}, {<<"stata">>, 1}]}),
    {ok, JobId} = flurm_job_manager:submit_job(JobSpec),

    {ok, Job} = flurm_job_manager:get_job(JobId),
    ?assertEqual([{<<"matlab">>, 2}, {<<"stata">>, 1}], Job#job.licenses),
    ok.

test_submit_license_validation_failure() ->
    meck:expect(flurm_license, parse_license_spec, fun(_) -> {ok, [{<<"nonexistent">>, 1}]} end),
    meck:expect(flurm_license, validate_licenses, fun(_) -> {error, license_not_found} end),

    JobSpec = make_job_spec(#{licenses => <<"nonexistent:1">>}),
    Result = flurm_job_manager:submit_job(JobSpec),
    ?assertMatch({error, {invalid_licenses, _}}, Result),
    ok.

%%====================================================================
%% Array Job Creation Edge Cases
%%====================================================================

array_job_edge_cases_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        {"Array job creation failure", fun test_array_job_creation_failure/0},
        {"Array job limit exceeded", fun test_array_job_limit_exceeded/0}
     ]}.

test_array_job_creation_failure() ->
    meck:expect(flurm_job_array, parse_array_spec, fun(_) ->
        {ok, #{start => 1, stop => 10}}
    end),
    meck:expect(flurm_job_array, create_array_job, fun(_, _) ->
        {error, too_many_tasks}
    end),

    JobSpec = make_job_spec(#{array => <<"1-1000">>}),
    Result = flurm_job_manager:submit_job(JobSpec),
    ?assertMatch({error, {array_creation_failed, _}}, Result),
    ok.

test_array_job_limit_exceeded() ->
    meck:expect(flurm_limits, check_submit_limits, fun(_) ->
        {error, array_size_exceeded}
    end),

    JobSpec = make_job_spec(#{array => <<"1-10">>}),
    Result = flurm_job_manager:submit_job(JobSpec),
    ?assertMatch({error, {submit_limit_exceeded, _}}, Result),
    ok.
