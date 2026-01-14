%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_job_manager
%%%
%%% These tests call the actual module functions directly to get
%%% code coverage. External dependencies are mocked with meck.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_manager_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

job_manager_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"init loads persisted jobs", fun test_init/0},
      {"submit_job creates new job", fun test_submit_job/0},
      {"submit_job with array creates array job", fun test_submit_array_job/0},
      {"cancel_job marks job cancelled", fun test_cancel_job/0},
      {"cancel_job returns error for unknown", fun test_cancel_job_not_found/0},
      {"get_job returns job", fun test_get_job/0},
      {"get_job returns error for unknown", fun test_get_job_not_found/0},
      {"list_jobs returns all jobs", fun test_list_jobs/0},
      {"update_job applies updates", fun test_update_job/0},
      {"update_job returns error for unknown", fun test_update_job_not_found/0},
      {"hold_job holds pending job", fun test_hold_job/0},
      {"hold_job fails for non-pending", fun test_hold_job_invalid_state/0},
      {"release_job releases held job", fun test_release_job/0},
      {"release_job fails for non-held", fun test_release_job_invalid_state/0},
      {"requeue_job requeues running job", fun test_requeue_job/0},
      {"requeue_job fails for non-running", fun test_requeue_job_invalid_state/0},
      {"import_job imports with preserved ID", fun test_import_job/0},
      {"import_job rejects duplicate", fun test_import_job_duplicate/0},
      {"handle_call unknown returns error", fun test_unknown_call/0},
      {"handle_cast ignores messages", fun test_handle_cast/0},
      {"handle_info ignores messages", fun test_handle_info/0},
      {"terminate returns ok", fun test_terminate/0}
     ]}.

setup() ->
    meck:new(flurm_db_persist, [passthrough, non_strict]),
    meck:new(flurm_scheduler, [passthrough, non_strict]),
    meck:new(flurm_limits, [passthrough, non_strict]),
    meck:new(flurm_license, [passthrough, non_strict]),
    meck:new(flurm_metrics, [passthrough, non_strict]),
    meck:new(flurm_job_dispatcher_server, [passthrough, non_strict]),
    meck:new(flurm_job_deps, [passthrough, non_strict]),
    meck:new(flurm_job_array, [passthrough, non_strict]),
    meck:new(flurm_core, [passthrough, non_strict]),

    %% Default mocks
    meck:expect(flurm_db_persist, persistence_mode, fun() -> none end),
    meck:expect(flurm_db_persist, list_jobs, fun() -> [] end),
    meck:expect(flurm_db_persist, store_job, fun(_) -> ok end),
    meck:expect(flurm_db_persist, update_job, fun(_, _) -> ok end),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> ok end),
    meck:expect(flurm_scheduler, job_failed, fun(_) -> ok end),
    meck:expect(flurm_limits, check_submit_limits, fun(_) -> ok end),
    meck:expect(flurm_license, parse_license_spec, fun(_) -> {ok, []} end),
    meck:expect(flurm_license, validate_licenses, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_job_dispatcher_server, cancel_job, fun(_, _) -> ok end),
    meck:expect(flurm_job_deps, add_dependencies, fun(_, _) -> ok end),
    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {ok, []} end),
    meck:expect(flurm_job_deps, on_job_state_change, fun(_, _) -> ok end),
    meck:expect(flurm_job_deps, remove_all_dependencies, fun(_) -> ok end),
    meck:expect(flurm_job_deps, parse_dependency_spec, fun(_) -> {ok, []} end),
    meck:expect(flurm_job_array, parse_array_spec, fun(_) -> {ok, #{indices => [0,1,2]}} end),
    meck:expect(flurm_job_array, create_array_job, fun(_, _) -> {ok, 1} end),
    meck:expect(flurm_job_array, get_schedulable_tasks, fun(_) -> [] end),
    meck:expect(flurm_core, update_job_state, fun(J, S) -> J#job{state = S} end),
    ok.

cleanup(_) ->
    meck:unload(flurm_db_persist),
    meck:unload(flurm_scheduler),
    meck:unload(flurm_limits),
    meck:unload(flurm_license),
    meck:unload(flurm_metrics),
    meck:unload(flurm_job_dispatcher_server),
    meck:unload(flurm_job_deps),
    meck:unload(flurm_job_array),
    meck:unload(flurm_core),
    ok.

%% State record matching the module's internal state
-record(state, {
    jobs = #{} :: #{integer() => #job{}},
    job_counter = 1 :: pos_integer(),
    persistence_mode = none :: ra | ets | none
}).

%%====================================================================
%% Test Cases
%%====================================================================

test_init() ->
    {ok, State} = flurm_job_manager:init([]),
    ?assertMatch(#state{}, State),
    ?assertEqual(#{}, State#state.jobs),
    ?assertEqual(1, State#state.job_counter),
    ?assertEqual(none, State#state.persistence_mode).

test_submit_job() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, Result, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    ?assertMatch({ok, 1}, Result),
    ?assertEqual(2, State1#state.job_counter),
    ?assert(maps:is_key(1, State1#state.jobs)).

test_submit_array_job() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"array_job">>, user => <<"user1">>, array => <<"0-2">>},
    {reply, Result, _State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    ?assertMatch({ok, {array, _}}, Result).

test_cancel_job() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    {reply, Result, State2} = flurm_job_manager:handle_call(
        {cancel_job, JobId}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    #{JobId := Job} = State2#state.jobs,
    ?assertEqual(cancelled, Job#job.state).

test_cancel_job_not_found() ->
    {ok, State0} = flurm_job_manager:init([]),
    {reply, Result, _State1} = flurm_job_manager:handle_call(
        {cancel_job, 9999}, {self(), make_ref()}, State0),
    ?assertEqual({error, not_found}, Result).

test_get_job() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_job_manager:handle_call(
        {get_job, JobId}, {self(), make_ref()}, State1),
    ?assertMatch({ok, #job{id = 1, name = <<"test_job">>}}, Result).

test_get_job_not_found() ->
    {ok, State0} = flurm_job_manager:init([]),
    {reply, Result, _State1} = flurm_job_manager:handle_call(
        {get_job, 9999}, {self(), make_ref()}, State0),
    ?assertEqual({error, not_found}, Result).

test_list_jobs() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec1 = #{name => <<"job1">>, user => <<"user1">>},
    JobSpec2 = #{name => <<"job2">>, user => <<"user2">>},
    {reply, {ok, _}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec1}, {self(), make_ref()}, State0),
    {reply, {ok, _}, State2} = flurm_job_manager:handle_call(
        {submit_job, JobSpec2}, {self(), make_ref()}, State1),
    {reply, Result, _State3} = flurm_job_manager:handle_call(
        list_jobs, {self(), make_ref()}, State2),
    ?assert(is_list(Result)),
    ?assertEqual(2, length(Result)).

test_update_job() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    Updates = #{priority => 200, state => running},
    {reply, Result, State2} = flurm_job_manager:handle_call(
        {update_job, JobId, Updates}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    #{JobId := Job} = State2#state.jobs,
    ?assertEqual(200, Job#job.priority),
    ?assertEqual(running, Job#job.state).

test_update_job_not_found() ->
    {ok, State0} = flurm_job_manager:init([]),
    {reply, Result, _State1} = flurm_job_manager:handle_call(
        {update_job, 9999, #{priority => 200}}, {self(), make_ref()}, State0),
    ?assertEqual({error, not_found}, Result).

test_hold_job() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    {reply, Result, State2} = flurm_job_manager:handle_call(
        {hold_job, JobId}, {self(), make_ref()}, State1),
    ?assertEqual(ok, Result),
    #{JobId := Job} = State2#state.jobs,
    ?assertEqual(held, Job#job.state).

test_hold_job_invalid_state() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    %% Set job to running state
    {reply, ok, State2} = flurm_job_manager:handle_call(
        {update_job, JobId, #{state => running}}, {self(), make_ref()}, State1),
    {reply, Result, _State3} = flurm_job_manager:handle_call(
        {hold_job, JobId}, {self(), make_ref()}, State2),
    ?assertMatch({error, {invalid_state, running}}, Result).

test_release_job() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_job_manager:handle_call(
        {hold_job, JobId}, {self(), make_ref()}, State1),
    {reply, Result, State3} = flurm_job_manager:handle_call(
        {release_job, JobId}, {self(), make_ref()}, State2),
    ?assertEqual(ok, Result),
    #{JobId := Job} = State3#state.jobs,
    ?assertEqual(pending, Job#job.state).

test_release_job_invalid_state() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_job_manager:handle_call(
        {update_job, JobId, #{state => running}}, {self(), make_ref()}, State1),
    {reply, Result, _State3} = flurm_job_manager:handle_call(
        {release_job, JobId}, {self(), make_ref()}, State2),
    ?assertMatch({error, {invalid_state, running}}, Result).

test_requeue_job() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    %% Set to running
    {reply, ok, State2} = flurm_job_manager:handle_call(
        {update_job, JobId, #{state => running, allocated_nodes => [<<"node1">>]}}, {self(), make_ref()}, State1),
    {reply, Result, State3} = flurm_job_manager:handle_call(
        {requeue_job, JobId}, {self(), make_ref()}, State2),
    ?assertEqual(ok, Result),
    #{JobId := Job} = State3#state.jobs,
    ?assertEqual(pending, Job#job.state),
    ?assertEqual([], Job#job.allocated_nodes).

test_requeue_job_invalid_state() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    %% Job is pending, can't requeue
    {reply, ok, State2} = flurm_job_manager:handle_call(
        {update_job, JobId, #{state => completed}}, {self(), make_ref()}, State1),
    {reply, Result, _State3} = flurm_job_manager:handle_call(
        {requeue_job, JobId}, {self(), make_ref()}, State2),
    ?assertMatch({error, {invalid_state, completed}}, Result).

test_import_job() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{id => 1000, name => <<"imported_job">>, user => <<"user1">>, state => pending},
    {reply, Result, State1} = flurm_job_manager:handle_call(
        {import_job, JobSpec}, {self(), make_ref()}, State0),
    ?assertEqual({ok, 1000}, Result),
    ?assert(maps:is_key(1000, State1#state.jobs)),
    %% Counter should be updated to be after imported ID
    ?assertEqual(1001, State1#state.job_counter).

test_import_job_duplicate() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{id => 1000, name => <<"imported_job">>, user => <<"user1">>},
    {reply, {ok, 1000}, State1} = flurm_job_manager:handle_call(
        {import_job, JobSpec}, {self(), make_ref()}, State0),
    {reply, Result, _State2} = flurm_job_manager:handle_call(
        {import_job, JobSpec}, {self(), make_ref()}, State1),
    ?assertEqual({error, already_exists}, Result).

test_unknown_call() ->
    {ok, State0} = flurm_job_manager:init([]),
    {reply, Result, _State1} = flurm_job_manager:handle_call(
        unknown_request, {self(), make_ref()}, State0),
    ?assertEqual({error, unknown_request}, Result).

test_handle_cast() ->
    {ok, State0} = flurm_job_manager:init([]),
    {noreply, State1} = flurm_job_manager:handle_cast(some_message, State0),
    ?assertEqual(State0, State1).

test_handle_info() ->
    {ok, State0} = flurm_job_manager:init([]),
    {noreply, State1} = flurm_job_manager:handle_info(some_message, State0),
    ?assertEqual(State0, State1).

test_terminate() ->
    {ok, State0} = flurm_job_manager:init([]),
    Result = flurm_job_manager:terminate(normal, State0),
    ?assertEqual(ok, Result).

%%====================================================================
%% Update State Tests
%%====================================================================

update_state_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"update job to completed", fun test_update_job_completed/0},
      {"update job to failed", fun test_update_job_failed/0},
      {"update job to timeout", fun test_update_job_timeout/0},
      {"update job with various fields", fun test_update_job_various_fields/0}
     ]}.

test_update_job_completed() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_job_manager:handle_call(
        {update_job, JobId, #{state => completed}}, {self(), make_ref()}, State1),
    #{JobId := Job} = State2#state.jobs,
    ?assertEqual(completed, Job#job.state).

test_update_job_failed() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_job_manager:handle_call(
        {update_job, JobId, #{state => failed}}, {self(), make_ref()}, State1),
    #{JobId := Job} = State2#state.jobs,
    ?assertEqual(failed, Job#job.state).

test_update_job_timeout() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    {reply, ok, State2} = flurm_job_manager:handle_call(
        {update_job, JobId, #{state => timeout}}, {self(), make_ref()}, State1),
    #{JobId := Job} = State2#state.jobs,
    ?assertEqual(timeout, Job#job.state).

test_update_job_various_fields() ->
    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, {ok, JobId}, State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    Now = erlang:system_time(second),
    Updates = #{
        allocated_nodes => [<<"node1">>, <<"node2">>],
        start_time => Now,
        end_time => Now + 100,
        exit_code => 0,
        time_limit => 7200
    },
    {reply, ok, State2} = flurm_job_manager:handle_call(
        {update_job, JobId, Updates}, {self(), make_ref()}, State1),
    #{JobId := Job} = State2#state.jobs,
    ?assertEqual([<<"node1">>, <<"node2">>], Job#job.allocated_nodes),
    ?assertEqual(0, Job#job.exit_code),
    ?assertEqual(7200, Job#job.time_limit).

%%====================================================================
%% Persistence Mode Tests
%%====================================================================

persistence_test_() ->
    {setup,
     fun setup_persistence/0,
     fun cleanup/1,
     [
      {"init loads jobs from ets persistence", fun test_init_with_persistence/0}
     ]}.

setup_persistence() ->
    setup(),
    Job = #job{id = 100, name = <<"persisted">>, user = <<"user">>, partition = <<"default">>,
               state = pending, script = <<>>, num_nodes = 1, num_cpus = 1, memory_mb = 1024,
               time_limit = 3600, priority = 100, submit_time = 0, allocated_nodes = []},
    meck:expect(flurm_db_persist, persistence_mode, fun() -> ets end),
    meck:expect(flurm_db_persist, list_jobs, fun() -> [Job] end),
    ok.

test_init_with_persistence() ->
    {ok, State} = flurm_job_manager:init([]),
    ?assertEqual(ets, State#state.persistence_mode),
    ?assert(maps:is_key(100, State#state.jobs)),
    ?assertEqual(101, State#state.job_counter).

%%====================================================================
%% Limit Rejection Tests
%%====================================================================

limit_rejection_test_() ->
    {setup,
     fun setup_limits/0,
     fun cleanup/1,
     [
      {"submit_job rejected by limits", fun test_submit_job_limit_exceeded/0},
      {"submit_job rejected by invalid license", fun test_submit_job_invalid_license/0}
     ]}.

setup_limits() ->
    setup(),
    ok.

test_submit_job_limit_exceeded() ->
    meck:expect(flurm_limits, check_submit_limits, fun(_) -> {error, max_jobs_per_user_exceeded} end),

    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>},
    {reply, Result, _State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    ?assertMatch({error, {submit_limit_exceeded, _}}, Result).

test_submit_job_invalid_license() ->
    meck:expect(flurm_license, validate_licenses, fun(_) -> {error, license_not_found} end),
    meck:expect(flurm_license, parse_license_spec, fun(_) -> {ok, [{<<"matlab">>, 1}]} end),

    {ok, State0} = flurm_job_manager:init([]),
    JobSpec = #{name => <<"test_job">>, user => <<"user1">>, licenses => <<"matlab:1">>},
    {reply, Result, _State1} = flurm_job_manager:handle_call(
        {submit_job, JobSpec}, {self(), make_ref()}, State0),
    ?assertMatch({error, {invalid_licenses, _}}, Result).
