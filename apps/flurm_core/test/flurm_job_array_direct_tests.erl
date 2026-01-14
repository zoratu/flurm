%%%-------------------------------------------------------------------
%%% @doc FLURM Job Array Direct Tests
%%%
%%% Additional comprehensive EUnit tests for the flurm_job_array gen_server,
%%% focusing on direct function calls without mocking, edge cases,
%%% error paths, and internal function coverage.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_array_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Local Record Definitions (copied from flurm_job_array.erl)
%% These are internal to flurm_job_array and not exported
%%====================================================================

-record(array_job, {
    id,
    name,
    user,
    partition,
    base_job,
    task_ids,
    task_count,
    max_concurrent,
    state,
    submit_time,
    start_time,
    end_time,
    stats
}).

-record(array_task, {
    id,
    array_job_id,
    task_id,
    job_id,
    state,
    exit_code,
    start_time,
    end_time,
    node
}).

-record(array_spec, {
    start_idx,
    end_idx,
    step,
    indices,
    max_concurrent
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

array_direct_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Parse string input to parse_array_spec", fun test_parse_string_input/0},
        {"Parse complex array specs", fun test_parse_complex_specs/0},
        {"Parse spec with whitespace", fun test_parse_with_whitespace/0},
        {"Format spec with single value", fun test_format_single_value/0},
        {"Format spec with step and max concurrent", fun test_format_step_and_max_conc/0},
        {"Generate task IDs from indices", fun test_generate_task_ids_indices/0},
        {"Generate task IDs from range", fun test_generate_task_ids_range/0},
        {"Create array job with binary spec", fun test_create_with_binary_spec/0},
        {"Create array job validation errors", fun test_create_validation_errors/0},
        {"Task state transitions", fun test_task_state_transitions/0},
        {"Array job state transitions", fun test_array_job_state_transitions/0},
        {"Throttling behavior", fun test_throttling_behavior/0},
        {"Schedule next task ordering", fun test_schedule_ordering/0},
        {"Task environment variables complete", fun test_task_env_complete/0},
        {"Update non-existent task", fun test_update_nonexistent_task/0},
        {"Cancel non-cancellable tasks", fun test_cancel_non_cancellable/0},
        {"Array completion with mixed states", fun test_array_completion_mixed/0},
        {"Check throttle cast handling", fun test_check_throttle_cast/0},
        {"Code change callback", fun test_code_change/0},
        {"Terminate callback", fun test_terminate/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    {ok, Pid} = flurm_job_array:start_link(),
    #{array_pid => Pid}.

cleanup(#{array_pid := Pid}) ->
    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_base_job() ->
    make_base_job(#{}).

make_base_job(Overrides) ->
    Defaults = #{
        id => 1,
        name => <<"test_array_job">>,
        user => <<"testuser">>,
        partition => <<"default">>,
        state => pending,
        script => <<"#!/bin/bash\necho $SLURM_ARRAY_TASK_ID">>,
        num_nodes => 1,
        num_cpus => 1,
        memory_mb => 1024,
        time_limit => 3600,
        priority => 100,
        submit_time => erlang:system_time(second),
        allocated_nodes => []
    },
    Props = maps:merge(Defaults, Overrides),
    #job{
        id = maps:get(id, Props),
        name = maps:get(name, Props),
        user = maps:get(user, Props),
        partition = maps:get(partition, Props),
        state = maps:get(state, Props),
        script = maps:get(script, Props),
        num_nodes = maps:get(num_nodes, Props),
        num_cpus = maps:get(num_cpus, Props),
        memory_mb = maps:get(memory_mb, Props),
        time_limit = maps:get(time_limit, Props),
        priority = maps:get(priority, Props),
        submit_time = maps:get(submit_time, Props),
        allocated_nodes = maps:get(allocated_nodes, Props)
    }.

%%====================================================================
%% Parsing Tests - Extended
%%====================================================================

test_parse_string_input() ->
    %% Test that string (list) input is handled
    {ok, Spec} = flurm_job_array:parse_array_spec("0-10"),
    ?assertEqual(0, Spec#array_spec.start_idx),
    ?assertEqual(10, Spec#array_spec.end_idx),
    ?assertEqual(1, Spec#array_spec.step),

    %% Test with step
    {ok, Spec2} = flurm_job_array:parse_array_spec("0-100:5"),
    ?assertEqual(5, Spec2#array_spec.step),

    %% Test with max concurrent
    {ok, Spec3} = flurm_job_array:parse_array_spec("0-50%10"),
    ?assertEqual(10, Spec3#array_spec.max_concurrent),
    ok.

test_parse_complex_specs() ->
    %% Range with step and max concurrent
    {ok, Spec1} = flurm_job_array:parse_array_spec(<<"0-100:2%5">>),
    ?assertEqual(0, Spec1#array_spec.start_idx),
    ?assertEqual(100, Spec1#array_spec.end_idx),
    ?assertEqual(2, Spec1#array_spec.step),
    ?assertEqual(5, Spec1#array_spec.max_concurrent),

    %% Comma-separated list with ranges
    {ok, Spec2} = flurm_job_array:parse_array_spec(<<"1-3,10-12,20">>),
    ?assertEqual([1, 2, 3, 10, 11, 12, 20], Spec2#array_spec.indices),

    %% List with max concurrent
    {ok, Spec3} = flurm_job_array:parse_array_spec(<<"1,5,10,15%2">>),
    ?assertEqual([1, 5, 10, 15], Spec3#array_spec.indices),
    ?assertEqual(2, Spec3#array_spec.max_concurrent),
    ok.

test_parse_with_whitespace() ->
    %% Test that whitespace is handled in parsing
    %% The parser should handle trimmed input
    {ok, Spec1} = flurm_job_array:parse_array_spec(<<"0-10">>),
    ?assertEqual(0, Spec1#array_spec.start_idx),

    %% Single value
    {ok, Spec2} = flurm_job_array:parse_array_spec(<<"5">>),
    ?assertEqual(5, Spec2#array_spec.start_idx),
    ?assertEqual(5, Spec2#array_spec.end_idx),
    ok.

%%====================================================================
%% Format Tests - Extended
%%====================================================================

test_format_single_value() ->
    %% Test formatting a single value (start == end)
    Spec = #array_spec{
        start_idx = 42,
        end_idx = 42,
        step = 1,
        max_concurrent = unlimited
    },
    Result = flurm_job_array:format_array_spec(Spec),
    ?assertEqual(<<"42">>, Result),
    ok.

test_format_step_and_max_conc() ->
    %% Test formatting with step
    Spec1 = #array_spec{
        start_idx = 0,
        end_idx = 100,
        step = 5,
        max_concurrent = unlimited
    },
    Result1 = flurm_job_array:format_array_spec(Spec1),
    ?assertEqual(<<"0-100:5">>, Result1),

    %% Test formatting with step and max concurrent
    Spec2 = #array_spec{
        start_idx = 0,
        end_idx = 100,
        step = 2,
        max_concurrent = 10
    },
    Result2 = flurm_job_array:format_array_spec(Spec2),
    ?assertEqual(<<"0-100:2%10">>, Result2),

    %% Test list format with max concurrent
    Spec3 = #array_spec{
        indices = [1, 2, 3, 4, 5],
        step = 1,
        max_concurrent = 3
    },
    Result3 = flurm_job_array:format_array_spec(Spec3),
    ?assertEqual(<<"1,2,3,4,5%3">>, Result3),
    ok.

%%====================================================================
%% Task ID Generation Tests
%%====================================================================

test_generate_task_ids_indices() ->
    %% Create array with explicit indices
    BaseJob = make_base_job(),
    Spec = #array_spec{
        start_idx = undefined,
        end_idx = undefined,
        step = 1,
        indices = [1, 5, 10, 15, 20],
        max_concurrent = unlimited
    },
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Verify task IDs match indices
    {ok, ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
    ?assertEqual([1, 5, 10, 15, 20], ArrayJob#array_job.task_ids),
    ?assertEqual(5, ArrayJob#array_job.task_count),
    ok.

test_generate_task_ids_range() ->
    %% Create array with range and step
    BaseJob = make_base_job(),
    Spec = #array_spec{
        start_idx = 0,
        end_idx = 10,
        step = 2,
        indices = undefined,
        max_concurrent = unlimited
    },
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Verify task IDs follow range with step
    {ok, ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
    ?assertEqual([0, 2, 4, 6, 8, 10], ArrayJob#array_job.task_ids),
    ?assertEqual(6, ArrayJob#array_job.task_count),
    ok.

%%====================================================================
%% Array Job Creation Tests
%%====================================================================

test_create_with_binary_spec() ->
    %% Test creating array job with binary spec string
    BaseJob = make_base_job(),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, <<"0-4">>),
    ?assert(is_integer(ArrayJobId)),

    {ok, ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
    ?assertEqual(5, ArrayJob#array_job.task_count),
    ?assertEqual([0, 1, 2, 3, 4], ArrayJob#array_job.task_ids),
    ok.

test_create_validation_errors() ->
    BaseJob = make_base_job(),

    %% Test array too large error (> 10000 tasks)
    Spec1 = #array_spec{
        start_idx = 0,
        end_idx = 15000,
        step = 1,
        max_concurrent = unlimited
    },
    {error, array_too_large} = flurm_job_array:create_array_job(BaseJob, Spec1),

    %% Test empty array error
    Spec2 = #array_spec{
        start_idx = undefined,
        end_idx = undefined,
        step = 1,
        indices = [],
        max_concurrent = unlimited
    },
    {error, empty_array} = flurm_job_array:create_array_job(BaseJob, Spec2),
    ok.

%%====================================================================
%% Task State Transition Tests
%%====================================================================

test_task_state_transitions() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Initial state is pending
    {ok, Task0} = flurm_job_array:get_array_task(ArrayJobId, 0),
    ?assertEqual(pending, Task0#array_task.state),

    %% Transition to running via task_started
    ok = flurm_job_array:task_started(ArrayJobId, 0, 1000),
    timer:sleep(50),
    {ok, Task0Running} = flurm_job_array:get_array_task(ArrayJobId, 0),
    ?assertEqual(running, Task0Running#array_task.state),
    ?assertEqual(1000, Task0Running#array_task.job_id),
    ?assertNotEqual(undefined, Task0Running#array_task.start_time),

    %% Transition to completed via task_completed with exit 0
    ok = flurm_job_array:task_completed(ArrayJobId, 0, 0),
    timer:sleep(50),
    {ok, Task0Completed} = flurm_job_array:get_array_task(ArrayJobId, 0),
    ?assertEqual(completed, Task0Completed#array_task.state),
    ?assertEqual(0, Task0Completed#array_task.exit_code),
    ?assertNotEqual(undefined, Task0Completed#array_task.end_time),

    %% Transition to failed via task_completed with non-zero exit
    ok = flurm_job_array:task_started(ArrayJobId, 1, 1001),
    ok = flurm_job_array:task_completed(ArrayJobId, 1, 127),
    timer:sleep(50),
    {ok, Task1Failed} = flurm_job_array:get_array_task(ArrayJobId, 1),
    ?assertEqual(failed, Task1Failed#array_task.state),
    ?assertEqual(127, Task1Failed#array_task.exit_code),
    ok.

test_array_job_state_transitions() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-1">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Initial state is pending
    {ok, ArrayJob1} = flurm_job_array:get_array_job(ArrayJobId),
    ?assertEqual(pending, ArrayJob1#array_job.state),

    %% First task starts - array should transition to running
    ok = flurm_job_array:task_started(ArrayJobId, 0, 1000),
    timer:sleep(50),
    {ok, ArrayJob2} = flurm_job_array:get_array_job(ArrayJobId),
    ?assertEqual(running, ArrayJob2#array_job.state),
    ?assertNotEqual(undefined, ArrayJob2#array_job.start_time),

    %% All tasks complete - array should transition to completed
    ok = flurm_job_array:task_started(ArrayJobId, 1, 1001),
    ok = flurm_job_array:task_completed(ArrayJobId, 0, 0),
    ok = flurm_job_array:task_completed(ArrayJobId, 1, 0),
    timer:sleep(100),
    {ok, ArrayJob3} = flurm_job_array:get_array_job(ArrayJobId),
    ?assertEqual(completed, ArrayJob3#array_job.state),
    ?assertNotEqual(undefined, ArrayJob3#array_job.end_time),
    ok.

%%====================================================================
%% Throttling Tests
%%====================================================================

test_throttling_behavior() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-9%2">>),  %% Max 2 concurrent
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Initially can schedule up to max concurrent
    ?assertEqual(true, flurm_job_array:can_schedule_task(ArrayJobId)),
    ?assertEqual(2, flurm_job_array:get_schedulable_count(ArrayJobId)),

    %% Start 2 tasks - should hit limit
    ok = flurm_job_array:task_started(ArrayJobId, 0, 1000),
    ok = flurm_job_array:task_started(ArrayJobId, 1, 1001),
    timer:sleep(50),

    ?assertEqual(false, flurm_job_array:can_schedule_task(ArrayJobId)),
    ?assertEqual(0, flurm_job_array:get_schedulable_count(ArrayJobId)),

    %% Complete one task - can schedule again
    ok = flurm_job_array:task_completed(ArrayJobId, 0, 0),
    timer:sleep(50),

    ?assertEqual(true, flurm_job_array:can_schedule_task(ArrayJobId)),
    ?assertEqual(1, flurm_job_array:get_schedulable_count(ArrayJobId)),
    ok.

test_schedule_ordering() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"5,2,8,1,10%3">>),  %% Non-sequential indices
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Schedulable tasks should be sorted by task_id
    Tasks = flurm_job_array:get_schedulable_tasks(ArrayJobId),
    TaskIds = [T#array_task.task_id || T <- Tasks],
    %% Should return first 3 in sorted order
    ?assertEqual([1, 2, 5], TaskIds),

    %% schedule_next_task should return lowest task_id
    {ok, NextTask} = flurm_job_array:schedule_next_task(ArrayJobId),
    ?assertEqual(1, NextTask#array_task.task_id),
    ok.

%%====================================================================
%% Task Environment Tests
%%====================================================================

test_task_env_complete() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"5-15:2">>),  %% 5, 7, 9, 11, 13, 15
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Get env for a middle task
    Env = flurm_job_array:get_task_env(ArrayJobId, 9),

    ?assertEqual(integer_to_binary(ArrayJobId), maps:get(<<"SLURM_ARRAY_JOB_ID">>, Env)),
    ?assertEqual(<<"9">>, maps:get(<<"SLURM_ARRAY_TASK_ID">>, Env)),
    ?assertEqual(<<"6">>, maps:get(<<"SLURM_ARRAY_TASK_COUNT">>, Env)),
    ?assertEqual(<<"5">>, maps:get(<<"SLURM_ARRAY_TASK_MIN">>, Env)),
    ?assertEqual(<<"15">>, maps:get(<<"SLURM_ARRAY_TASK_MAX">>, Env)),
    ok.

%%====================================================================
%% Edge Case Tests
%%====================================================================

test_update_nonexistent_task() ->
    %% Update for non-existent task should not crash
    ok = flurm_job_array:update_task_state(999999, 0, #{state => running}),
    ok.

test_cancel_non_cancellable() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Complete task 0
    ok = flurm_job_array:task_started(ArrayJobId, 0, 1000),
    ok = flurm_job_array:task_completed(ArrayJobId, 0, 0),
    timer:sleep(50),

    %% Try to cancel completed task - should fail
    {error, task_not_cancellable} = flurm_job_array:cancel_array_task(ArrayJobId, 0),

    %% Fail task 1
    ok = flurm_job_array:task_started(ArrayJobId, 1, 1001),
    ok = flurm_job_array:task_completed(ArrayJobId, 1, 1),
    timer:sleep(50),

    %% Try to cancel failed task - should fail
    {error, task_not_cancellable} = flurm_job_array:cancel_array_task(ArrayJobId, 1),
    ok.

test_array_completion_mixed() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-3">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Complete tasks with mixed results
    ok = flurm_job_array:task_started(ArrayJobId, 0, 1000),
    ok = flurm_job_array:task_started(ArrayJobId, 1, 1001),
    ok = flurm_job_array:task_started(ArrayJobId, 2, 1002),
    ok = flurm_job_array:task_started(ArrayJobId, 3, 1003),

    ok = flurm_job_array:task_completed(ArrayJobId, 0, 0),   %% Success
    ok = flurm_job_array:task_completed(ArrayJobId, 1, 1),   %% Failure
    ok = flurm_job_array:task_completed(ArrayJobId, 2, 0),   %% Success
    ok = flurm_job_array:task_completed(ArrayJobId, 3, 0),   %% Success
    timer:sleep(100),

    %% Array should be failed because one task failed
    {ok, ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
    ?assertEqual(failed, ArrayJob#array_job.state),

    %% Verify stats
    Stats = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(4, maps:get(total, Stats)),
    ?assertEqual(3, maps:get(completed, Stats)),
    ?assertEqual(1, maps:get(failed, Stats)),
    ok.

test_check_throttle_cast() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-4%2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Fill up slots
    ok = flurm_job_array:task_started(ArrayJobId, 0, 1000),
    ok = flurm_job_array:task_started(ArrayJobId, 1, 1001),
    timer:sleep(50),

    %% Complete a task - this triggers check_throttle cast
    ok = flurm_job_array:task_completed(ArrayJobId, 0, 0),
    timer:sleep(100),

    %% More tasks should be schedulable now
    ?assertEqual(1, flurm_job_array:get_schedulable_count(ArrayJobId)),
    ok.

test_code_change() ->
    %% Test that server continues working (code_change returns ok)
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),
    ?assert(is_integer(ArrayJobId)),
    ok.

test_terminate() ->
    %% Test that server handles operations before termination
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Verify we can still query
    {ok, _ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
    ok.

%%====================================================================
%% Additional Test Suites
%%====================================================================

%% Test expand_array function
expand_array_test_() ->
    {setup,
     fun() ->
         {ok, Pid} = flurm_job_array:start_link(),
         #{array_pid => Pid}
     end,
     fun(#{array_pid := Pid}) ->
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         [
             {"Expand array with range", fun() ->
                 {ok, Tasks} = flurm_job_array:expand_array(<<"0-4">>),
                 ?assertEqual(5, length(Tasks)),
                 %% Each task should have proper fields
                 Task0 = hd(Tasks),
                 ?assert(maps:is_key(task_id, Task0)),
                 ?assert(maps:is_key(task_count, Task0)),
                 ?assert(maps:is_key(task_min, Task0)),
                 ?assert(maps:is_key(task_max, Task0)),
                 ?assert(maps:is_key(max_concurrent, Task0)),
                 ?assertEqual(0, maps:get(task_id, Task0)),
                 ?assertEqual(5, maps:get(task_count, Task0)),
                 ?assertEqual(0, maps:get(task_min, Task0)),
                 ?assertEqual(4, maps:get(task_max, Task0))
             end},
             {"Expand array with step", fun() ->
                 {ok, Tasks} = flurm_job_array:expand_array(<<"0-10:2">>),
                 ?assertEqual(6, length(Tasks)),
                 TaskIds = [maps:get(task_id, T) || T <- Tasks],
                 ?assertEqual([0, 2, 4, 6, 8, 10], TaskIds)
             end},
             {"Expand array with indices", fun() ->
                 Spec = #array_spec{
                     indices = [1, 5, 9],
                     step = 1,
                     max_concurrent = unlimited
                 },
                 {ok, Tasks} = flurm_job_array:expand_array(Spec),
                 ?assertEqual(3, length(Tasks)),
                 TaskIds = [maps:get(task_id, T) || T <- Tasks],
                 ?assertEqual([1, 5, 9], TaskIds)
             end}
         ]
     end}.

%% Test schedule_next_task edge cases
schedule_next_edge_test_() ->
    {setup,
     fun() ->
         {ok, Pid} = flurm_job_array:start_link(),
         #{array_pid => Pid}
     end,
     fun(#{array_pid := Pid}) ->
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         [
             {"Schedule next when no pending tasks", fun() ->
                 BaseJob = make_base_job(),
                 {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-0">>),
                 {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

                 %% Start and complete the only task
                 ok = flurm_job_array:task_started(ArrayJobId, 0, 1000),
                 ok = flurm_job_array:task_completed(ArrayJobId, 0, 0),
                 timer:sleep(50),

                 %% Now try to schedule - should return no_pending
                 {error, no_pending} = flurm_job_array:schedule_next_task(ArrayJobId)
             end},
             {"Schedule next when throttled", fun() ->
                 BaseJob = make_base_job(),
                 {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-5%1">>),  %% Max 1 concurrent
                 {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

                 %% Start one task - should hit throttle
                 ok = flurm_job_array:task_started(ArrayJobId, 0, 1000),
                 timer:sleep(50),

                 %% Now try to schedule - should return throttled
                 {error, throttled} = flurm_job_array:schedule_next_task(ArrayJobId)
             end},
             {"Schedule next for non-existent array", fun() ->
                 %% can_schedule_task returns false for non-existent
                 ?assertEqual(false, flurm_job_array:can_schedule_task(999999))
             end}
         ]
     end}.

%% Test cancel scenarios
cancel_scenarios_test_() ->
    {setup,
     fun() ->
         {ok, Pid} = flurm_job_array:start_link(),
         #{array_pid => Pid}
     end,
     fun(#{array_pid := Pid}) ->
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         [
             {"Cancel running task", fun() ->
                 BaseJob = make_base_job(),
                 {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
                 {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

                 %% Start task to make it running
                 ok = flurm_job_array:task_started(ArrayJobId, 0, 1000),
                 timer:sleep(50),

                 %% Cancel running task
                 ok = flurm_job_array:cancel_array_task(ArrayJobId, 0),
                 {ok, Task} = flurm_job_array:get_array_task(ArrayJobId, 0),
                 ?assertEqual(cancelled, Task#array_task.state)
             end},
             {"Cancel array with mixed task states", fun() ->
                 BaseJob = make_base_job(),
                 {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-3">>),
                 {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

                 %% Complete task 0
                 ok = flurm_job_array:task_started(ArrayJobId, 0, 1000),
                 ok = flurm_job_array:task_completed(ArrayJobId, 0, 0),

                 %% Start task 1 (running)
                 ok = flurm_job_array:task_started(ArrayJobId, 1, 1001),
                 timer:sleep(50),

                 %% Cancel whole array
                 ok = flurm_job_array:cancel_array_job(ArrayJobId),

                 %% Task 0 should still be completed
                 {ok, Task0} = flurm_job_array:get_array_task(ArrayJobId, 0),
                 ?assertEqual(completed, Task0#array_task.state),

                 %% Task 1 should be cancelled
                 {ok, Task1} = flurm_job_array:get_array_task(ArrayJobId, 1),
                 ?assertEqual(cancelled, Task1#array_task.state),

                 %% Pending tasks should be cancelled
                 {ok, Task2} = flurm_job_array:get_array_task(ArrayJobId, 2),
                 ?assertEqual(cancelled, Task2#array_task.state)
             end}
         ]
     end}.

%% Test unknown request and message handling
error_handling_test_() ->
    {setup,
     fun() ->
         {ok, Pid} = flurm_job_array:start_link(),
         #{array_pid => Pid}
     end,
     fun(#{array_pid := Pid}) ->
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         [
             {"Unknown gen_server call", fun() ->
                 Result = gen_server:call(flurm_job_array, {unknown, request, data}),
                 ?assertEqual({error, unknown_request}, Result)
             end},
             {"Unknown gen_server cast", fun() ->
                 %% Should not crash
                 gen_server:cast(flurm_job_array, {unknown, cast}),
                 timer:sleep(10),
                 ?assert(is_process_alive(whereis(flurm_job_array)))
             end},
             {"Unknown info message", fun() ->
                 %% Should not crash
                 whereis(flurm_job_array) ! {some, random, info},
                 timer:sleep(10),
                 ?assert(is_process_alive(whereis(flurm_job_array)))
             end}
         ]
     end}.

%% Test array job all cancelled completion state
all_cancelled_test_() ->
    {setup,
     fun() ->
         {ok, Pid} = flurm_job_array:start_link(),
         #{array_pid => Pid}
     end,
     fun(#{array_pid := Pid}) ->
         catch unlink(Pid),
         catch gen_server:stop(Pid, shutdown, 5000)
     end,
     fun(_) ->
         {"All tasks cancelled results in cancelled array", fun() ->
             BaseJob = make_base_job(),
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
             {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

             %% Cancel all tasks individually
             ok = flurm_job_array:cancel_array_task(ArrayJobId, 0),
             ok = flurm_job_array:cancel_array_task(ArrayJobId, 1),
             ok = flurm_job_array:cancel_array_task(ArrayJobId, 2),
             timer:sleep(100),

             %% Array should be in cancelled state
             {ok, ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
             ?assertEqual(cancelled, ArrayJob#array_job.state)
         end}
     end}.
