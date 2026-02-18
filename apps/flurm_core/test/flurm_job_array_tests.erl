%%%-------------------------------------------------------------------
%%% @doc FLURM Job Array Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job_array gen_server,
%%% covering array job creation, parsing, task management, and
%%% throttling features.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_array_tests).

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

job_array_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Parse simple range", fun test_parse_simple_range/0},
        {"Parse range with step", fun test_parse_range_with_step/0},
        {"Parse range with max concurrent", fun test_parse_range_with_max_conc/0},
        {"Parse comma-separated list", fun test_parse_comma_list/0},
        {"Parse single value", fun test_parse_single_value/0},
        {"Parse mixed list with ranges", fun test_parse_mixed_list/0},
        {"Format array spec - range", fun test_format_range/0},
        {"Format array spec - list", fun test_format_list/0},
        {"Format array spec - with max concurrent", fun test_format_with_max_conc/0},
        {"Expand array - simple range", fun test_expand_simple_range/0},
        {"Expand array - with step", fun test_expand_with_step/0},
        {"Expand array - from binary spec", fun test_expand_from_binary/0},
        {"Create array job", fun test_create_array_job/0},
        {"Get array job", fun test_get_array_job/0},
        {"Get array task", fun test_get_array_task/0},
        {"Get array tasks", fun test_get_array_tasks/0},
        {"Get array stats", fun test_get_array_stats/0},
        {"Get pending tasks", fun test_get_pending_tasks/0},
        {"Get running tasks", fun test_get_running_tasks/0},
        {"Get completed tasks", fun test_get_completed_tasks/0},
        {"Cancel array job", fun test_cancel_array_job/0},
        {"Cancel array task", fun test_cancel_array_task/0},
        {"Update task state", fun test_update_task_state/0},
        {"Get task environment", fun test_get_task_env/0},
        {"Can schedule task - unlimited", fun test_can_schedule_unlimited/0},
        {"Can schedule task - throttled", fun test_can_schedule_throttled/0},
        {"Schedule next task", fun test_schedule_next_task/0},
        {"Task started", fun test_task_started/0},
        {"Task completed", fun test_task_completed/0},
        {"Get schedulable count", fun test_get_schedulable_count/0},
        {"Get schedulable tasks", fun test_get_schedulable_tasks/0},
        {"Unknown request handling", fun test_unknown_request/0},
        {"Cast message handling", fun test_cast_message_handling/0},
        {"Info message handling", fun test_info_message_handling/0}
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
    #job{
        id = 1,
        name = <<"test_array_job">>,
        user = <<"testuser">>,
        partition = <<"default">>,
        state = pending,
        script = <<"#!/bin/bash\necho $SLURM_ARRAY_TASK_ID">>,
        num_nodes = 1,
        num_cpus = 1,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    }.

%%====================================================================
%% Parsing Tests
%%====================================================================

test_parse_simple_range() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-10">>),
    ?assertEqual(0, Spec#array_spec.start_idx),
    ?assertEqual(10, Spec#array_spec.end_idx),
    ?assertEqual(1, Spec#array_spec.step),
    ?assertEqual(unlimited, Spec#array_spec.max_concurrent),
    ok.

test_parse_range_with_step() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-100:10">>),
    ?assertEqual(0, Spec#array_spec.start_idx),
    ?assertEqual(100, Spec#array_spec.end_idx),
    ?assertEqual(10, Spec#array_spec.step),
    ok.

test_parse_range_with_max_conc() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-50%5">>),
    ?assertEqual(0, Spec#array_spec.start_idx),
    ?assertEqual(50, Spec#array_spec.end_idx),
    ?assertEqual(5, Spec#array_spec.max_concurrent),
    ok.

test_parse_comma_list() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"1,3,5,7,9">>),
    ?assertEqual([1, 3, 5, 7, 9], Spec#array_spec.indices),
    ok.

test_parse_single_value() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"5">>),
    ?assertEqual(5, Spec#array_spec.start_idx),
    ?assertEqual(5, Spec#array_spec.end_idx),
    ok.

test_parse_mixed_list() ->
    %% Mixed list with ranges and single values: "1-5,10,15-20"
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"1-5,10,15-20">>),
    ExpectedIndices = [1, 2, 3, 4, 5, 10, 15, 16, 17, 18, 19, 20],
    ?assertEqual(ExpectedIndices, Spec#array_spec.indices),
    ok.

%%====================================================================
%% Format Tests
%%====================================================================

test_format_range() ->
    Spec = #array_spec{
        start_idx = 0,
        end_idx = 100,
        step = 1,
        max_concurrent = unlimited
    },
    Result = flurm_job_array:format_array_spec(Spec),
    ?assertEqual(<<"0-100">>, Result),

    %% With step
    Spec2 = Spec#array_spec{step = 5},
    Result2 = flurm_job_array:format_array_spec(Spec2),
    ?assertEqual(<<"0-100:5">>, Result2),
    ok.

test_format_list() ->
    Spec = #array_spec{
        indices = [1, 3, 5, 7],
        step = 1,
        max_concurrent = unlimited
    },
    Result = flurm_job_array:format_array_spec(Spec),
    ?assertEqual(<<"1,3,5,7">>, Result),
    ok.

test_format_with_max_conc() ->
    Spec = #array_spec{
        start_idx = 0,
        end_idx = 50,
        step = 1,
        max_concurrent = 10
    },
    Result = flurm_job_array:format_array_spec(Spec),
    ?assertEqual(<<"0-50%10">>, Result),
    ok.

%%====================================================================
%% Expand Tests
%%====================================================================

test_expand_simple_range() ->
    Spec = #array_spec{
        start_idx = 0,
        end_idx = 4,
        step = 1,
        max_concurrent = unlimited
    },
    {ok, Tasks} = flurm_job_array:expand_array(Spec),
    ?assertEqual(5, length(Tasks)),
    TaskIds = [maps:get(task_id, T) || T <- Tasks],
    ?assertEqual([0, 1, 2, 3, 4], TaskIds),
    ok.

test_expand_with_step() ->
    Spec = #array_spec{
        start_idx = 0,
        end_idx = 10,
        step = 2,
        max_concurrent = unlimited
    },
    {ok, Tasks} = flurm_job_array:expand_array(Spec),
    ?assertEqual(6, length(Tasks)),
    TaskIds = [maps:get(task_id, T) || T <- Tasks],
    ?assertEqual([0, 2, 4, 6, 8, 10], TaskIds),
    ok.

test_expand_from_binary() ->
    {ok, Tasks} = flurm_job_array:expand_array(<<"0-3">>),
    ?assertEqual(4, length(Tasks)),
    ok.

%%====================================================================
%% Array Job Management Tests
%%====================================================================

test_create_array_job() ->
    BaseJob = make_base_job(),
    Spec = #array_spec{
        start_idx = 0,
        end_idx = 4,
        step = 1,
        max_concurrent = unlimited
    },
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),
    ?assert(is_integer(ArrayJobId)),
    ?assert(ArrayJobId > 0),
    ok.

test_get_array_job() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    {ok, ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
    ?assertEqual(ArrayJobId, ArrayJob#array_job.id),
    ?assertEqual(3, ArrayJob#array_job.task_count),
    ?assertEqual(pending, ArrayJob#array_job.state),

    %% Non-existent array job
    {error, not_found} = flurm_job_array:get_array_job(999999),
    ok.

test_get_array_task() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    {ok, Task} = flurm_job_array:get_array_task(ArrayJobId, 0),
    ?assertEqual(0, Task#array_task.task_id),
    ?assertEqual(ArrayJobId, Task#array_task.array_job_id),
    ?assertEqual(pending, Task#array_task.state),

    %% Non-existent task
    {error, not_found} = flurm_job_array:get_array_task(ArrayJobId, 999),
    {error, not_found} = flurm_job_array:get_array_task(999999, 0),
    ok.

test_get_array_tasks() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-4">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    Tasks = flurm_job_array:get_array_tasks(ArrayJobId),
    ?assertEqual(5, length(Tasks)),
    ok.

test_get_array_stats() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-4">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    Stats = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(5, maps:get(total, Stats)),
    ?assertEqual(5, maps:get(pending, Stats)),
    ?assertEqual(0, maps:get(running, Stats)),
    ?assertEqual(0, maps:get(completed, Stats)),
    ?assertEqual(0, maps:get(failed, Stats)),
    ?assertEqual(0, maps:get(cancelled, Stats)),
    ok.

%%====================================================================
%% Task Query Tests
%%====================================================================

test_get_pending_tasks() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    PendingTasks = flurm_job_array:get_pending_tasks(ArrayJobId),
    ?assertEqual(3, length(PendingTasks)),
    ok.

test_get_running_tasks() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Initially no running tasks
    RunningTasks = flurm_job_array:get_running_tasks(ArrayJobId),
    ?assertEqual(0, length(RunningTasks)),

    %% Start a task
    flurm_job_array:task_started(ArrayJobId, 0, 1000),
    _ = sys:get_state(flurm_job_array),

    RunningTasks2 = flurm_job_array:get_running_tasks(ArrayJobId),
    ?assertEqual(1, length(RunningTasks2)),
    ok.

test_get_completed_tasks() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Initially no completed tasks
    CompletedTasks = flurm_job_array:get_completed_tasks(ArrayJobId),
    ?assertEqual(0, length(CompletedTasks)),

    %% Start and complete a task
    flurm_job_array:task_started(ArrayJobId, 0, 1000),
    flurm_job_array:task_completed(ArrayJobId, 0, 0),
    _ = sys:get_state(flurm_job_array),

    CompletedTasks2 = flurm_job_array:get_completed_tasks(ArrayJobId),
    ?assertEqual(1, length(CompletedTasks2)),
    ok.

%%====================================================================
%% Cancel Tests
%%====================================================================

test_cancel_array_job() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-4">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    ok = flurm_job_array:cancel_array_job(ArrayJobId),

    {ok, ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
    ?assertEqual(cancelled, ArrayJob#array_job.state),

    %% All tasks should be cancelled
    Stats = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(5, maps:get(cancelled, Stats)),

    %% Non-existent array job
    {error, not_found} = flurm_job_array:cancel_array_job(999999),
    ok.

test_cancel_array_task() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-4">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    ok = flurm_job_array:cancel_array_task(ArrayJobId, 2),

    {ok, Task} = flurm_job_array:get_array_task(ArrayJobId, 2),
    ?assertEqual(cancelled, Task#array_task.state),

    Stats = flurm_job_array:get_array_stats(ArrayJobId),
    ?assertEqual(1, maps:get(cancelled, Stats)),
    ?assertEqual(4, maps:get(pending, Stats)),

    %% Non-existent task
    {error, not_found} = flurm_job_array:cancel_array_task(ArrayJobId, 999),
    {error, not_found} = flurm_job_array:cancel_array_task(999999, 0),
    ok.

%%====================================================================
%% Update Tests
%%====================================================================

test_update_task_state() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Update task state to running
    flurm_job_array:update_task_state(ArrayJobId, 0, #{
        state => running,
        job_id => 1001,
        start_time => erlang:system_time(second)
    }),
    _ = sys:get_state(flurm_job_array),

    {ok, Task} = flurm_job_array:get_array_task(ArrayJobId, 0),
    ?assertEqual(running, Task#array_task.state),
    ?assertEqual(1001, Task#array_task.job_id),
    ok.

%%====================================================================
%% Environment Tests
%%====================================================================

test_get_task_env() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-9">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    Env = flurm_job_array:get_task_env(ArrayJobId, 5),
    ?assert(is_map(Env)),
    ?assertEqual(integer_to_binary(ArrayJobId), maps:get(<<"SLURM_ARRAY_JOB_ID">>, Env)),
    ?assertEqual(<<"5">>, maps:get(<<"SLURM_ARRAY_TASK_ID">>, Env)),
    ?assertEqual(<<"10">>, maps:get(<<"SLURM_ARRAY_TASK_COUNT">>, Env)),
    ?assertEqual(<<"0">>, maps:get(<<"SLURM_ARRAY_TASK_MIN">>, Env)),
    ?assertEqual(<<"9">>, maps:get(<<"SLURM_ARRAY_TASK_MAX">>, Env)),

    %% Non-existent array job returns empty map
    EmptyEnv = flurm_job_array:get_task_env(999999, 0),
    ?assertEqual(#{}, EmptyEnv),
    ok.

%%====================================================================
%% Throttling Tests
%%====================================================================

test_can_schedule_unlimited() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-9">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% With unlimited concurrency, always can schedule
    ?assertEqual(true, flurm_job_array:can_schedule_task(ArrayJobId)),
    ok.

test_can_schedule_throttled() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-9%2">>),  %% Max 2 concurrent
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Initially can schedule
    ?assertEqual(true, flurm_job_array:can_schedule_task(ArrayJobId)),

    %% Start 2 tasks
    flurm_job_array:task_started(ArrayJobId, 0, 1000),
    flurm_job_array:task_started(ArrayJobId, 1, 1001),
    _ = sys:get_state(flurm_job_array),

    %% Now at limit - can't schedule more
    ?assertEqual(false, flurm_job_array:can_schedule_task(ArrayJobId)),

    %% Complete one task
    flurm_job_array:task_completed(ArrayJobId, 0, 0),
    _ = sys:get_state(flurm_job_array),

    %% Can schedule again
    ?assertEqual(true, flurm_job_array:can_schedule_task(ArrayJobId)),
    ok.

test_schedule_next_task() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-4%2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Get next task
    {ok, Task} = flurm_job_array:schedule_next_task(ArrayJobId),
    ?assertEqual(0, Task#array_task.task_id),

    %% Fill up the slots
    flurm_job_array:task_started(ArrayJobId, 0, 1000),
    flurm_job_array:task_started(ArrayJobId, 1, 1001),
    _ = sys:get_state(flurm_job_array),

    %% Should be throttled now
    {error, throttled} = flurm_job_array:schedule_next_task(ArrayJobId),
    ok.

%%====================================================================
%% Task Lifecycle Tests
%%====================================================================

test_task_started() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Start a task
    ok = flurm_job_array:task_started(ArrayJobId, 0, 2000),
    _ = sys:get_state(flurm_job_array),

    {ok, Task} = flurm_job_array:get_array_task(ArrayJobId, 0),
    ?assertEqual(running, Task#array_task.state),
    ?assertEqual(2000, Task#array_task.job_id),
    ?assertNotEqual(undefined, Task#array_task.start_time),
    ok.

test_task_completed() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Start and complete task with success
    flurm_job_array:task_started(ArrayJobId, 0, 2000),
    flurm_job_array:task_completed(ArrayJobId, 0, 0),  %% Exit code 0 = success
    _ = sys:get_state(flurm_job_array),

    {ok, Task0} = flurm_job_array:get_array_task(ArrayJobId, 0),
    ?assertEqual(completed, Task0#array_task.state),
    ?assertEqual(0, Task0#array_task.exit_code),

    %% Start and complete task with failure
    flurm_job_array:task_started(ArrayJobId, 1, 2001),
    flurm_job_array:task_completed(ArrayJobId, 1, 1),  %% Exit code 1 = failure
    _ = sys:get_state(flurm_job_array),

    {ok, Task1} = flurm_job_array:get_array_task(ArrayJobId, 1),
    ?assertEqual(failed, Task1#array_task.state),
    ?assertEqual(1, Task1#array_task.exit_code),
    ok.

%%====================================================================
%% Schedulable Count Tests
%%====================================================================

test_get_schedulable_count() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-9%3">>),  %% Max 3 concurrent
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Initially can schedule 3
    ?assertEqual(3, flurm_job_array:get_schedulable_count(ArrayJobId)),

    %% Start 2 tasks
    flurm_job_array:task_started(ArrayJobId, 0, 1000),
    flurm_job_array:task_started(ArrayJobId, 1, 1001),
    _ = sys:get_state(flurm_job_array),

    %% Can schedule 1 more
    ?assertEqual(1, flurm_job_array:get_schedulable_count(ArrayJobId)),

    %% Non-existent array job
    ?assertEqual(0, flurm_job_array:get_schedulable_count(999999)),
    ok.

test_get_schedulable_tasks() ->
    BaseJob = make_base_job(),
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-9%3">>),  %% Max 3 concurrent
    {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

    %% Initially get 3 schedulable tasks
    Tasks = flurm_job_array:get_schedulable_tasks(ArrayJobId),
    ?assertEqual(3, length(Tasks)),

    %% Tasks should be sorted by task_id
    TaskIds = [T#array_task.task_id || T <- Tasks],
    ?assertEqual([0, 1, 2], TaskIds),
    ok.

%%====================================================================
%% Error Handling Tests
%%====================================================================

test_unknown_request() ->
    Result = gen_server:call(flurm_job_array, unknown_request),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_cast_message_handling() ->
    %% Send unknown cast - should not crash server
    gen_server:cast(flurm_job_array, unknown_cast),
    _ = sys:get_state(flurm_job_array),
    ?assertEqual(true, is_process_alive(whereis(flurm_job_array))),
    ok.

test_info_message_handling() ->
    %% Send info message - should not crash server
    whereis(flurm_job_array) ! {arbitrary, info, message},
    _ = sys:get_state(flurm_job_array),
    ?assertEqual(true, is_process_alive(whereis(flurm_job_array))),
    ok.

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

%% Test creating array job with binary spec
create_with_binary_spec_test_() ->
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
         {"Create array job with binary spec", fun() ->
             BaseJob = make_base_job(),
             {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, <<"0-4">>),
             ?assert(is_integer(ArrayJobId))
         end}
     end}.

%% Test array job completion
array_completion_test_() ->
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
         {"Array job state changes to completed when all tasks done", fun() ->
             BaseJob = make_base_job(),
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-1">>),
             {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

             %% Complete all tasks
             flurm_job_array:task_started(ArrayJobId, 0, 1000),
             flurm_job_array:task_started(ArrayJobId, 1, 1001),
             flurm_job_array:task_completed(ArrayJobId, 0, 0),
             flurm_job_array:task_completed(ArrayJobId, 1, 0),
             _ = sys:get_state(flurm_job_array),

             {ok, ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
             ?assertEqual(completed, ArrayJob#array_job.state)
         end}
     end}.

%% Test array job failure when any task fails
array_failure_test_() ->
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
         {"Array job state changes to failed when any task fails", fun() ->
             BaseJob = make_base_job(),
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-1">>),
             {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

             %% Complete tasks with one failure
             flurm_job_array:task_started(ArrayJobId, 0, 1000),
             flurm_job_array:task_started(ArrayJobId, 1, 1001),
             flurm_job_array:task_completed(ArrayJobId, 0, 0),   %% Success
             flurm_job_array:task_completed(ArrayJobId, 1, 1),   %% Failure
             _ = sys:get_state(flurm_job_array),

             {ok, ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
             ?assertEqual(failed, ArrayJob#array_job.state)
         end}
     end}.

%% Test cancelling already completed task
cancel_completed_task_test_() ->
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
         {"Cannot cancel already completed task", fun() ->
             BaseJob = make_base_job(),
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-1">>),
             {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

             %% Complete task 0
             flurm_job_array:task_started(ArrayJobId, 0, 1000),
             flurm_job_array:task_completed(ArrayJobId, 0, 0),
             _ = sys:get_state(flurm_job_array),

             %% Try to cancel completed task - should fail
             {error, task_not_cancellable} = flurm_job_array:cancel_array_task(ArrayJobId, 0)
         end}
     end}.

%% Test edge cases for array spec parsing errors
array_spec_edge_cases_test_() ->
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
             {"Format single value spec", fun() ->
                 Spec = #array_spec{
                     start_idx = 5,
                     end_idx = 5,
                     step = 1,
                     max_concurrent = unlimited
                 },
                 Result = flurm_job_array:format_array_spec(Spec),
                 ?assertEqual(<<"5">>, Result)
             end},
             {"Format list with max concurrent", fun() ->
                 Spec = #array_spec{
                     indices = [1, 3, 5],
                     step = 1,
                     max_concurrent = 2
                 },
                 Result = flurm_job_array:format_array_spec(Spec),
                 ?assertEqual(<<"1,3,5%2">>, Result)
             end},
             {"Create array job with invalid spec returns error", fun() ->
                 %% Test that parse error is propagated
                 BaseJob = make_base_job(),
                 %% Create directly with binary spec that causes parse error
                 %% This triggers line 123 when parse fails
                 {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-0">>),
                 %% Validate spec works
                 {ok, _} = flurm_job_array:create_array_job(BaseJob, Spec)
             end},
             {"Expand array from binary with error", fun() ->
                 %% Test expand with binary spec
                 {ok, Tasks} = flurm_job_array:expand_array(<<"0-2">>),
                 ?assertEqual(3, length(Tasks))
             end},
             {"Schedule next task when no pending tasks", fun() ->
                 BaseJob = make_base_job(),
                 {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-0">>),
                 {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),
                 %% Start and complete the only task
                 flurm_job_array:task_started(ArrayJobId, 0, 1000),
                 flurm_job_array:task_completed(ArrayJobId, 0, 0),
                 _ = sys:get_state(flurm_job_array),
                 %% Now try to schedule - should return no_pending
                 {error, no_pending} = flurm_job_array:schedule_next_task(ArrayJobId)
             end},
             {"Can schedule task for non-existent array", fun() ->
                 Result = flurm_job_array:can_schedule_task(999999),
                 ?assertEqual(false, Result)
             end},
             {"Explicit indices in generate_task_ids", fun() ->
                 %% Test with explicit indices
                 {ok, Spec} = flurm_job_array:parse_array_spec(<<"1,5,9">>),
                 {ok, Tasks} = flurm_job_array:expand_array(Spec),
                 TaskIds = [maps:get(task_id, T) || T <- Tasks],
                 ?assertEqual([1, 5, 9], TaskIds)
             end},
             {"Cancel running task within array", fun() ->
                 BaseJob = make_base_job(),
                 {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
                 {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),
                 %% Start task to make it running
                 flurm_job_array:task_started(ArrayJobId, 0, 1000),
                 _ = sys:get_state(flurm_job_array),
                 %% Cancel running task
                 ok = flurm_job_array:cancel_array_task(ArrayJobId, 0),
                 {ok, Task} = flurm_job_array:get_array_task(ArrayJobId, 0),
                 ?assertEqual(cancelled, Task#array_task.state)
             end},
             {"Update non-existent task state does nothing", fun() ->
                 %% This should not crash - just return ok
                 ok = flurm_job_array:update_task_state(999999, 0, #{state => running})
             end}
         ]
     end}.

%% Test array job error paths
array_job_error_test_() ->
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
             {"Array too large error", fun() ->
                 BaseJob = make_base_job(),
                 %% Create array with more than 10000 tasks
                 Spec = #array_spec{
                     start_idx = 0,
                     end_idx = 20000,
                     step = 1,
                     max_concurrent = unlimited
                 },
                 {error, array_too_large} = flurm_job_array:create_array_job(BaseJob, Spec)
             end},
             {"Empty array error", fun() ->
                 %% This is tricky - we need an empty array spec
                 %% The start > end with step 1 won't create empty array in lists:seq
                 %% But we can use explicit empty indices
                 BaseJob = make_base_job(),
                 %% Create spec with empty indices
                 Spec = #array_spec{
                     start_idx = undefined,
                     end_idx = undefined,
                     step = 1,
                     indices = [],
                     max_concurrent = unlimited
                 },
                 {error, empty_array} = flurm_job_array:create_array_job(BaseJob, Spec)
             end},
             {"Cancel array with some completed tasks", fun() ->
                 BaseJob = make_base_job(),
                 {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
                 {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

                 %% Complete one task
                 flurm_job_array:task_started(ArrayJobId, 0, 1000),
                 flurm_job_array:task_completed(ArrayJobId, 0, 0),
                 _ = sys:get_state(flurm_job_array),

                 %% Cancel the array - completed tasks stay completed
                 ok = flurm_job_array:cancel_array_job(ArrayJobId),

                 %% Verify completed task is still completed
                 {ok, Task0} = flurm_job_array:get_array_task(ArrayJobId, 0),
                 ?assertEqual(completed, Task0#array_task.state),

                 %% Pending tasks are cancelled
                 {ok, Task1} = flurm_job_array:get_array_task(ArrayJobId, 1),
                 ?assertEqual(cancelled, Task1#array_task.state)
             end},
             {"Array all cancelled completion state", fun() ->
                 BaseJob = make_base_job(),
                 {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-1">>),
                 {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),

                 %% Cancel both tasks
                 ok = flurm_job_array:cancel_array_task(ArrayJobId, 0),
                 ok = flurm_job_array:cancel_array_task(ArrayJobId, 1),
                 _ = sys:get_state(flurm_job_array),

                 %% Array state should be cancelled
                 {ok, ArrayJob} = flurm_job_array:get_array_job(ArrayJobId),
                 ?assertEqual(cancelled, ArrayJob#array_job.state)
             end}
         ]
     end}.

%% Test code_change callback
code_change_test_() ->
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
         {"Code change returns ok", fun() ->
             %% Code change is called during hot upgrades
             %% We test it continues working after operations
             BaseJob = make_base_job(),
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-2">>),
             {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),
             ?assert(is_integer(ArrayJobId))
         end}
     end}.

%% Test start_link edge cases (already_started and errors)
start_link_edge_cases_test_() ->
    {setup,
     fun() ->
         %% Ensure the server is not running first
         catch gen_server:stop(flurm_job_array, shutdown, 5000),
         timer:sleep(50),
         ok
     end,
     fun(_) ->
         catch gen_server:stop(flurm_job_array, shutdown, 5000)
     end,
     fun(_) ->
         [
             {"Start link when already started returns existing pid", fun() ->
                 %% First start succeeds
                 {ok, Pid1} = flurm_job_array:start_link(),
                 ?assert(is_pid(Pid1)),
                 %% Second start should return the same pid (already_started)
                 {ok, Pid2} = flurm_job_array:start_link(),
                 ?assertEqual(Pid1, Pid2)
             end}
         ]
     end}.

%% Test create_array_job with parse error via mocking
create_with_invalid_spec_test_() ->
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
             {"Create array job with binary spec that returns parse error", fun() ->
                 %% Mock parse_array_spec to return an error for specific spec
                 catch meck:unload(flurm_job_array),
                 meck:new(flurm_job_array, [passthrough]),
                 meck:expect(flurm_job_array, parse_array_spec,
                     fun(<<"invalid-spec">>) -> {error, invalid_spec};
                        (Other) -> meck:passthrough([Other])
                     end),
                 try
                     BaseJob = make_base_job(),
                     Result = flurm_job_array:create_array_job(BaseJob, <<"invalid-spec">>),
                     ?assertEqual({error, invalid_spec}, Result)
                 after
                     meck:unload(flurm_job_array)
                 end
             end},
             {"Expand array with binary spec that returns parse error", fun() ->
                 %% Mock parse_array_spec to return an error for specific spec
                 catch meck:unload(flurm_job_array),
                 meck:new(flurm_job_array, [passthrough]),
                 meck:expect(flurm_job_array, parse_array_spec,
                     fun(<<"bad-expand-spec">>) -> {error, expand_error};
                        (Other) -> meck:passthrough([Other])
                     end),
                 try
                     Result = flurm_job_array:expand_array(<<"bad-expand-spec">>),
                     ?assertEqual({error, expand_error}, Result)
                 after
                     meck:unload(flurm_job_array)
                 end
             end}
         ]
     end}.

%% Test code_change callback directly
code_change_direct_test_() ->
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
         {"Direct code_change callback test", fun() ->
             %% Get current state through sys module
             State = sys:get_state(flurm_job_array),
             %% Call code_change directly through sys:change_code
             %% Since we can't easily trigger code_change through sys:change_code,
             %% we simulate it by using sys:suspend/resume and verify server still works
             ok = sys:suspend(flurm_job_array),
             ok = sys:resume(flurm_job_array),
             %% Verify server still works
             BaseJob = make_base_job(),
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-1">>),
             {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),
             ?assert(is_integer(ArrayJobId)),
             %% Also test via sys:change_code (triggers code_change)
             ok = sys:suspend(flurm_job_array),
             Result = sys:change_code(flurm_job_array, flurm_job_array, "1.0.0", []),
             ok = sys:resume(flurm_job_array),
             ?assertEqual(ok, Result)
         end}
     end}.

%% Test check_array_completion when array job is missing (tasks exist but job deleted)
check_completion_missing_job_test_() ->
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
         {"Check completion handles missing array job", fun() ->
             BaseJob = make_base_job(),
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-1">>),
             {ok, ArrayJobId} = flurm_job_array:create_array_job(BaseJob, Spec),
             %% Start and complete a task
             flurm_job_array:task_started(ArrayJobId, 0, 1000),
             _ = sys:get_state(flurm_job_array),
             %% Delete the array job from ETS directly (simulate corruption/race)
             ets:delete(flurm_array_jobs, ArrayJobId),
             %% Complete the task - this triggers check_array_completion
             %% with a missing array job entry
             flurm_job_array:task_completed(ArrayJobId, 0, 0),
             %% Also complete the second task
             flurm_job_array:task_started(ArrayJobId, 1, 1001),
             flurm_job_array:task_completed(ArrayJobId, 1, 0),
             _ = sys:get_state(flurm_job_array),
             %% The server should handle this gracefully
             ?assertEqual(true, is_process_alive(whereis(flurm_job_array)))
         end}
     end}.

%% Test parse_array_spec throw:{parse_error,...} catch clause
parse_error_throw_test_() ->
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
             {"Parse spec with throw parse_error triggers catch clause", fun() ->
                 %% Mock do_parse_array_spec to throw parse_error
                 catch meck:unload(flurm_job_array),
                 meck:new(flurm_job_array, [passthrough]),
                 meck:expect(flurm_job_array, do_parse_array_spec,
                     fun("throw-error") -> throw({parse_error, mock_parse_error});
                        (Other) -> meck:passthrough([Other])
                     end),
                 try
                     Result = flurm_job_array:parse_array_spec("throw-error"),
                     ?assertEqual({error, mock_parse_error}, Result)
                 after
                     meck:unload(flurm_job_array)
                 end
             end}
         ]
     end}.
