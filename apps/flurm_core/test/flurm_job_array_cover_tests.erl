%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit tests for flurm_job_array targeting 100%
%%% line coverage.
%%%
%%% Exercises every public API function, every gen_server callback
%%% clause, every internal function branch, and every parsing path
%%% in the module.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_array_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Record definitions mirroring flurm_job_array internal records
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

-record(state, {
    next_array_id
}).

%%====================================================================
%% Test Fixture
%%====================================================================

job_array_cover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% --- Parsing ---
        {"parse_array_spec binary input delegates to list parser",
         fun test_parse_array_spec_binary/0},
        {"parse_array_spec string (list) input",
         fun test_parse_array_spec_string/0},
        {"parse_array_spec simple range 0-10",
         fun test_parse_simple_range/0},
        {"parse_array_spec range with step 0-100:5",
         fun test_parse_range_step/0},
        {"parse_array_spec range with max concurrent 0-50%10",
         fun test_parse_range_percent/0},
        {"parse_array_spec range+step+percent 0-100:2%5",
         fun test_parse_range_step_percent/0},
        {"parse_array_spec comma-separated list 1,3,5,7",
         fun test_parse_comma_list/0},
        {"parse_array_spec comma list with percent 1,2,3%2",
         fun test_parse_comma_list_percent/0},
        {"parse_array_spec mixed list with ranges 1-3,10,20-22",
         fun test_parse_mixed_list/0},
        {"parse_array_spec single value 5",
         fun test_parse_single_value/0},

        %% --- Formatting ---
        {"format_array_spec simple range (step=1, unlimited)",
         fun test_format_simple_range/0},
        {"format_array_spec single value (start==end)",
         fun test_format_single_value/0},
        {"format_array_spec range with step",
         fun test_format_range_step/0},
        {"format_array_spec range with max concurrent",
         fun test_format_range_percent/0},
        {"format_array_spec range with step and max concurrent",
         fun test_format_range_step_percent/0},
        {"format_array_spec indices list unlimited",
         fun test_format_indices_unlimited/0},
        {"format_array_spec indices list with max concurrent",
         fun test_format_indices_percent/0},

        %% --- Roundtrip parse/format ---
        {"parse/format roundtrip range",
         fun test_roundtrip_range/0},
        {"parse/format roundtrip range+step",
         fun test_roundtrip_range_step/0},
        {"parse/format roundtrip range+percent",
         fun test_roundtrip_range_percent/0},
        {"parse/format roundtrip single",
         fun test_roundtrip_single/0},

        %% --- expand_array ---
        {"expand_array from binary spec",
         fun test_expand_array_binary/0},
        {"expand_array from record spec (range)",
         fun test_expand_array_record_range/0},
        {"expand_array from record spec (indices)",
         fun test_expand_array_record_indices/0},

        %% --- create_array_job ---
        {"create_array_job with binary spec string",
         fun test_create_binary_spec/0},
        {"create_array_job with record spec",
         fun test_create_record_spec/0},
        {"create_array_job with binary spec parse error",
         fun test_create_binary_parse_error/0},
        {"create_array_job error: empty tasks",
         fun test_create_empty_tasks/0},
        {"create_array_job error: too many tasks (>10000)",
         fun test_create_too_many_tasks/0},
        {"create_array_job increments array ID",
         fun test_create_increments_id/0},

        %% --- get_ queries ---
        {"get_array_job found and not_found",
         fun test_get_array_job/0},
        {"get_array_task found and not_found",
         fun test_get_array_task/0},
        {"get_array_tasks returns all tasks",
         fun test_get_array_tasks/0},
        {"get_array_stats with various states",
         fun test_get_array_stats/0},
        {"get_pending_tasks filters correctly",
         fun test_get_pending_tasks/0},
        {"get_running_tasks filters correctly",
         fun test_get_running_tasks/0},
        {"get_completed_tasks filters correctly",
         fun test_get_completed_tasks/0},

        %% --- cancel ---
        {"cancel_array_job found",
         fun test_cancel_array_job_found/0},
        {"cancel_array_job not_found",
         fun test_cancel_array_job_not_found/0},
        {"cancel_array_job with mixed states (completed stays)",
         fun test_cancel_array_job_mixed/0},
        {"cancel_array_task pending",
         fun test_cancel_task_pending/0},
        {"cancel_array_task running (with job_id set)",
         fun test_cancel_task_running/0},
        {"cancel_array_task not_cancellable (completed)",
         fun test_cancel_task_completed/0},
        {"cancel_array_task not_cancellable (cancelled)",
         fun test_cancel_task_cancelled/0},
        {"cancel_array_task not_found",
         fun test_cancel_task_not_found/0},

        %% --- update_task_state (cast) ---
        {"update_task_state existing task",
         fun test_update_task_state/0},
        {"update_task_state non-existent task (no-op)",
         fun test_update_task_state_nonexistent/0},

        %% --- task lifecycle ---
        {"task_started marks running with job_id",
         fun test_task_started/0},
        {"task_completed exit 0 -> completed",
         fun test_task_completed_success/0},
        {"task_completed exit non-zero -> failed",
         fun test_task_completed_failure/0},

        %% --- update_array_job_state (pending->running on first running task) ---
        {"array job transitions pending->running on first task start",
         fun test_array_job_pending_to_running/0},
        {"array job already running does not re-transition",
         fun test_array_job_already_running/0},

        %% --- check_array_completion ---
        {"array completion: all completed -> completed",
         fun test_completion_all_completed/0},
        {"array completion: all cancelled -> cancelled",
         fun test_completion_all_cancelled/0},
        {"array completion: some failed -> failed",
         fun test_completion_some_failed/0},
        {"array completion: not all done yet -> no state change",
         fun test_completion_not_done/0},
        {"array completion: job record deleted (orphan tasks)",
         fun test_completion_job_deleted/0},

        %% --- get_task_env ---
        {"get_task_env success",
         fun test_get_task_env_success/0},
        {"get_task_env error (non-existent array)",
         fun test_get_task_env_error/0},

        %% --- can_schedule_task ---
        {"can_schedule_task unlimited -> true",
         fun test_can_schedule_unlimited/0},
        {"can_schedule_task below max -> true",
         fun test_can_schedule_below_max/0},
        {"can_schedule_task at max -> false",
         fun test_can_schedule_at_max/0},
        {"can_schedule_task non-existent -> false",
         fun test_can_schedule_nonexistent/0},

        %% --- schedule_next_task ---
        {"schedule_next_task found (returns lowest pending)",
         fun test_schedule_next_found/0},
        {"schedule_next_task throttled",
         fun test_schedule_next_throttled/0},
        {"schedule_next_task no_pending",
         fun test_schedule_next_no_pending/0},

        %% --- get_schedulable_count ---
        {"get_schedulable_count unlimited",
         fun test_schedulable_count_unlimited/0},
        {"get_schedulable_count limited",
         fun test_schedulable_count_limited/0},
        {"get_schedulable_count non-existent",
         fun test_schedulable_count_nonexistent/0},

        %% --- get_schedulable_tasks ---
        {"get_schedulable_tasks returns sorted subset",
         fun test_schedulable_tasks/0},
        {"get_schedulable_tasks empty when at limit",
         fun test_schedulable_tasks_empty/0},

        %% --- gen_server catch-all clauses ---
        {"handle_call unknown request",
         fun test_handle_call_unknown/0},
        {"handle_cast unknown message",
         fun test_handle_cast_unknown/0},
        {"handle_cast check_throttle with pending tasks",
         fun test_handle_cast_check_throttle_with_pending/0},
        {"handle_cast check_throttle with no pending tasks",
         fun test_handle_cast_check_throttle_no_pending/0},
        {"handle_info unknown message",
         fun test_handle_info_unknown/0},
        {"terminate callback",
         fun test_terminate/0},
        {"code_change callback",
         fun test_code_change/0},

        %% --- start_link already_started branch ---
        {"start_link returns ok when already started",
         fun test_start_link_already_started/0},

        %% --- TEST-exported internal functions ---
        {"do_parse_array_spec with all format variants",
         fun test_do_parse_array_spec/0},
        {"parse_range_spec range with step",
         fun test_parse_range_spec_with_step/0},
        {"parse_range_spec range without step",
         fun test_parse_range_spec_no_step/0},
        {"parse_range_spec single number",
         fun test_parse_range_spec_single/0},
        {"parse_list_spec comma-separated",
         fun test_parse_list_spec/0},
        {"parse_list_element range element",
         fun test_parse_list_element_range/0},
        {"parse_list_element single element",
         fun test_parse_list_element_single/0},
        {"generate_task_ids from indices",
         fun test_generate_task_ids_indices/0},
        {"generate_task_ids from range",
         fun test_generate_task_ids_range/0},
        {"apply_task_updates merges fields",
         fun test_apply_task_updates/0}
     ]}.

setup() ->
    %% Ensure lager is available (or at least sasl for logging fallback)
    catch application:ensure_all_started(sasl),
    catch application:ensure_all_started(lager),
    {ok, Pid} = flurm_job_array:start_link(),
    #{array_pid => Pid}.

cleanup(#{array_pid := Pid}) ->
    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

make_base_job() ->
    #job{
        id = 1,
        name = <<"test_array">>,
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

%% Drain all pending casts so ETS is consistent.
drain() ->
    _ = sys:get_state(flurm_job_array).

%%====================================================================
%% Parsing Tests
%%====================================================================

test_parse_array_spec_binary() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-10">>),
    ?assertEqual(0, Spec#array_spec.start_idx),
    ?assertEqual(10, Spec#array_spec.end_idx).

test_parse_array_spec_string() ->
    {ok, Spec} = flurm_job_array:parse_array_spec("0-10"),
    ?assertEqual(0, Spec#array_spec.start_idx),
    ?assertEqual(10, Spec#array_spec.end_idx).

test_parse_simple_range() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-10">>),
    ?assertEqual(0, Spec#array_spec.start_idx),
    ?assertEqual(10, Spec#array_spec.end_idx),
    ?assertEqual(1, Spec#array_spec.step),
    ?assertEqual(unlimited, Spec#array_spec.max_concurrent),
    ?assertEqual(undefined, Spec#array_spec.indices).

test_parse_range_step() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-100:5">>),
    ?assertEqual(0, Spec#array_spec.start_idx),
    ?assertEqual(100, Spec#array_spec.end_idx),
    ?assertEqual(5, Spec#array_spec.step),
    ?assertEqual(unlimited, Spec#array_spec.max_concurrent).

test_parse_range_percent() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-50%10">>),
    ?assertEqual(0, Spec#array_spec.start_idx),
    ?assertEqual(50, Spec#array_spec.end_idx),
    ?assertEqual(1, Spec#array_spec.step),
    ?assertEqual(10, Spec#array_spec.max_concurrent).

test_parse_range_step_percent() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-100:2%5">>),
    ?assertEqual(0, Spec#array_spec.start_idx),
    ?assertEqual(100, Spec#array_spec.end_idx),
    ?assertEqual(2, Spec#array_spec.step),
    ?assertEqual(5, Spec#array_spec.max_concurrent).

test_parse_comma_list() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"1,3,5,7">>),
    ?assertEqual([1, 3, 5, 7], Spec#array_spec.indices),
    ?assertEqual(unlimited, Spec#array_spec.max_concurrent).

test_parse_comma_list_percent() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"1,2,3%2">>),
    ?assertEqual([1, 2, 3], Spec#array_spec.indices),
    ?assertEqual(2, Spec#array_spec.max_concurrent).

test_parse_mixed_list() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"1-3,10,20-22">>),
    ?assertEqual([1, 2, 3, 10, 20, 21, 22], Spec#array_spec.indices).

test_parse_single_value() ->
    {ok, Spec} = flurm_job_array:parse_array_spec(<<"5">>),
    ?assertEqual(5, Spec#array_spec.start_idx),
    ?assertEqual(5, Spec#array_spec.end_idx),
    ?assertEqual(1, Spec#array_spec.step).

%%====================================================================
%% Formatting Tests
%%====================================================================

test_format_simple_range() ->
    Spec = #array_spec{start_idx = 0, end_idx = 10, step = 1,
                       indices = undefined, max_concurrent = unlimited},
    ?assertEqual(<<"0-10">>, flurm_job_array:format_array_spec(Spec)).

test_format_single_value() ->
    Spec = #array_spec{start_idx = 5, end_idx = 5, step = 1,
                       indices = undefined, max_concurrent = unlimited},
    ?assertEqual(<<"5">>, flurm_job_array:format_array_spec(Spec)).

test_format_range_step() ->
    Spec = #array_spec{start_idx = 0, end_idx = 100, step = 5,
                       indices = undefined, max_concurrent = unlimited},
    ?assertEqual(<<"0-100:5">>, flurm_job_array:format_array_spec(Spec)).

test_format_range_percent() ->
    Spec = #array_spec{start_idx = 0, end_idx = 50, step = 1,
                       indices = undefined, max_concurrent = 10},
    ?assertEqual(<<"0-50%10">>, flurm_job_array:format_array_spec(Spec)).

test_format_range_step_percent() ->
    Spec = #array_spec{start_idx = 0, end_idx = 100, step = 2,
                       indices = undefined, max_concurrent = 10},
    ?assertEqual(<<"0-100:2%10">>, flurm_job_array:format_array_spec(Spec)).

test_format_indices_unlimited() ->
    Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                       step = 1, indices = [1, 3, 5, 7],
                       max_concurrent = unlimited},
    ?assertEqual(<<"1,3,5,7">>, flurm_job_array:format_array_spec(Spec)).

test_format_indices_percent() ->
    Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                       step = 1, indices = [1, 2, 3],
                       max_concurrent = 2},
    ?assertEqual(<<"1,2,3%2">>, flurm_job_array:format_array_spec(Spec)).

%%====================================================================
%% Roundtrip Tests
%%====================================================================

test_roundtrip_range() ->
    {ok, S} = flurm_job_array:parse_array_spec(<<"0-100">>),
    ?assertEqual(<<"0-100">>, flurm_job_array:format_array_spec(S)).

test_roundtrip_range_step() ->
    {ok, S} = flurm_job_array:parse_array_spec(<<"0-100:5">>),
    ?assertEqual(<<"0-100:5">>, flurm_job_array:format_array_spec(S)).

test_roundtrip_range_percent() ->
    {ok, S} = flurm_job_array:parse_array_spec(<<"0-50%10">>),
    ?assertEqual(<<"0-50%10">>, flurm_job_array:format_array_spec(S)).

test_roundtrip_single() ->
    {ok, S} = flurm_job_array:parse_array_spec(<<"42">>),
    ?assertEqual(<<"42">>, flurm_job_array:format_array_spec(S)).

%%====================================================================
%% expand_array Tests
%%====================================================================

test_expand_array_binary() ->
    %% expand_array/1 when is_binary -> parses, then expands
    {ok, Tasks} = flurm_job_array:expand_array(<<"0-4">>),
    ?assertEqual(5, length(Tasks)),
    TaskIds = [maps:get(task_id, T) || T <- Tasks],
    ?assertEqual([0, 1, 2, 3, 4], TaskIds),
    First = hd(Tasks),
    ?assertEqual(5, maps:get(task_count, First)),
    ?assertEqual(0, maps:get(task_min, First)),
    ?assertEqual(4, maps:get(task_max, First)),
    ?assertEqual(unlimited, maps:get(max_concurrent, First)).

test_expand_array_record_range() ->
    Spec = #array_spec{start_idx = 0, end_idx = 10, step = 2,
                       indices = undefined, max_concurrent = 3},
    {ok, Tasks} = flurm_job_array:expand_array(Spec),
    TaskIds = [maps:get(task_id, T) || T <- Tasks],
    ?assertEqual([0, 2, 4, 6, 8, 10], TaskIds),
    First = hd(Tasks),
    ?assertEqual(6, maps:get(task_count, First)),
    ?assertEqual(0, maps:get(task_min, First)),
    ?assertEqual(10, maps:get(task_max, First)),
    ?assertEqual(3, maps:get(max_concurrent, First)).

test_expand_array_record_indices() ->
    Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                       step = 1, indices = [5, 10, 20],
                       max_concurrent = unlimited},
    {ok, Tasks} = flurm_job_array:expand_array(Spec),
    TaskIds = [maps:get(task_id, T) || T <- Tasks],
    ?assertEqual([5, 10, 20], TaskIds),
    First = hd(Tasks),
    ?assertEqual(3, maps:get(task_count, First)),
    ?assertEqual(5, maps:get(task_min, First)),
    ?assertEqual(20, maps:get(task_max, First)).

%%====================================================================
%% create_array_job Tests
%%====================================================================

test_create_binary_spec() ->
    BaseJob = make_base_job(),
    %% create_array_job(BaseJob, BinarySpec) parses then calls with record
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-4">>),
    ?assert(is_integer(Id)),
    {ok, AJ} = flurm_job_array:get_array_job(Id),
    ?assertEqual(5, AJ#array_job.task_count),
    ?assertEqual([0, 1, 2, 3, 4], AJ#array_job.task_ids),
    ?assertEqual(pending, AJ#array_job.state),
    ?assertEqual(unlimited, AJ#array_job.max_concurrent).

test_create_record_spec() ->
    BaseJob = make_base_job(),
    Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                       indices = undefined, max_concurrent = 2},
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, Spec),
    {ok, AJ} = flurm_job_array:get_array_job(Id),
    ?assertEqual(3, AJ#array_job.task_count),
    ?assertEqual(2, AJ#array_job.max_concurrent).

test_create_binary_parse_error() ->
    BaseJob = make_base_job(),
    %% Invalid spec "abc" causes error:badarg from list_to_integer.
    %% The parse_array_spec try/catch only catches throw:{parse_error, _},
    %% so an invalid spec actually crashes.  Verify the crash:
    ?assertError(badarg, flurm_job_array:create_array_job(BaseJob, <<"abc">>)).

test_create_empty_tasks() ->
    BaseJob = make_base_job(),
    Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                       step = 1, indices = [],
                       max_concurrent = unlimited},
    ?assertEqual({error, empty_array},
                 flurm_job_array:create_array_job(BaseJob, Spec)).

test_create_too_many_tasks() ->
    BaseJob = make_base_job(),
    Spec = #array_spec{start_idx = 0, end_idx = 15000, step = 1,
                       indices = undefined, max_concurrent = unlimited},
    ?assertEqual({error, array_too_large},
                 flurm_job_array:create_array_job(BaseJob, Spec)).

test_create_increments_id() ->
    BaseJob = make_base_job(),
    {ok, Id1} = flurm_job_array:create_array_job(BaseJob, <<"0-1">>),
    {ok, Id2} = flurm_job_array:create_array_job(BaseJob, <<"0-1">>),
    ?assertEqual(Id1 + 1, Id2).

%%====================================================================
%% get_ Query Tests
%%====================================================================

test_get_array_job() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    {ok, AJ} = flurm_job_array:get_array_job(Id),
    ?assertEqual(Id, AJ#array_job.id),
    ?assertEqual(3, AJ#array_job.task_count),
    %% not_found
    ?assertEqual({error, not_found}, flurm_job_array:get_array_job(999999)).

test_get_array_task() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    {ok, Task} = flurm_job_array:get_array_task(Id, 1),
    ?assertEqual(1, Task#array_task.task_id),
    ?assertEqual(Id, Task#array_task.array_job_id),
    ?assertEqual(pending, Task#array_task.state),
    %% not_found
    ?assertEqual({error, not_found}, flurm_job_array:get_array_task(Id, 999)),
    ?assertEqual({error, not_found}, flurm_job_array:get_array_task(999999, 0)).

test_get_array_tasks() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-4">>),
    Tasks = flurm_job_array:get_array_tasks(Id),
    ?assertEqual(5, length(Tasks)),
    %% Empty for non-existent
    ?assertEqual([], flurm_job_array:get_array_tasks(999999)).

test_get_array_stats() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-4">>),

    %% All pending initially
    Stats0 = flurm_job_array:get_array_stats(Id),
    ?assertEqual(5, maps:get(total, Stats0)),
    ?assertEqual(5, maps:get(pending, Stats0)),
    ?assertEqual(0, maps:get(running, Stats0)),
    ?assertEqual(0, maps:get(completed, Stats0)),
    ?assertEqual(0, maps:get(failed, Stats0)),
    ?assertEqual(0, maps:get(cancelled, Stats0)),

    %% Start some, complete some, fail one, cancel one
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_started(Id, 1, 1001),
    flurm_job_array:task_started(Id, 2, 1002),
    drain(),
    flurm_job_array:task_completed(Id, 0, 0),
    flurm_job_array:task_completed(Id, 1, 1),
    drain(),
    ok = flurm_job_array:cancel_array_task(Id, 3),

    Stats1 = flurm_job_array:get_array_stats(Id),
    ?assertEqual(5, maps:get(total, Stats1)),
    ?assertEqual(1, maps:get(pending, Stats1)),
    ?assertEqual(1, maps:get(running, Stats1)),
    ?assertEqual(1, maps:get(completed, Stats1)),
    ?assertEqual(1, maps:get(failed, Stats1)),
    ?assertEqual(1, maps:get(cancelled, Stats1)).

test_get_pending_tasks() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    ?assertEqual(3, length(flurm_job_array:get_pending_tasks(Id))),
    flurm_job_array:task_started(Id, 0, 1000),
    drain(),
    ?assertEqual(2, length(flurm_job_array:get_pending_tasks(Id))).

test_get_running_tasks() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    ?assertEqual(0, length(flurm_job_array:get_running_tasks(Id))),
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_started(Id, 1, 1001),
    drain(),
    ?assertEqual(2, length(flurm_job_array:get_running_tasks(Id))).

test_get_completed_tasks() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    ?assertEqual(0, length(flurm_job_array:get_completed_tasks(Id))),
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_completed(Id, 0, 0),
    drain(),
    ?assertEqual(1, length(flurm_job_array:get_completed_tasks(Id))).

%%====================================================================
%% Cancel Tests
%%====================================================================

test_cancel_array_job_found() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-4">>),
    ok = flurm_job_array:cancel_array_job(Id),
    {ok, AJ} = flurm_job_array:get_array_job(Id),
    ?assertEqual(cancelled, AJ#array_job.state),
    ?assertNotEqual(undefined, AJ#array_job.end_time),
    Stats = flurm_job_array:get_array_stats(Id),
    ?assertEqual(5, maps:get(cancelled, Stats)).

test_cancel_array_job_not_found() ->
    ?assertEqual({error, not_found}, flurm_job_array:cancel_array_job(999999)).

test_cancel_array_job_mixed() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-3">>),
    %% Complete task 0
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_completed(Id, 0, 0),
    %% Start task 1 (running with job_id)
    flurm_job_array:task_started(Id, 1, 1001),
    drain(),
    %% Cancel entire array
    ok = flurm_job_array:cancel_array_job(Id),
    %% Task 0 stays completed
    {ok, T0} = flurm_job_array:get_array_task(Id, 0),
    ?assertEqual(completed, T0#array_task.state),
    %% Task 1 was running -> cancelled (cancel_task_internal with job_id)
    {ok, T1} = flurm_job_array:get_array_task(Id, 1),
    ?assertEqual(cancelled, T1#array_task.state),
    %% Task 2, 3 were pending -> cancelled (cancel_task_internal without job_id)
    {ok, T2} = flurm_job_array:get_array_task(Id, 2),
    ?assertEqual(cancelled, T2#array_task.state).

test_cancel_task_pending() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    ok = flurm_job_array:cancel_array_task(Id, 1),
    {ok, T} = flurm_job_array:get_array_task(Id, 1),
    ?assertEqual(cancelled, T#array_task.state).

test_cancel_task_running() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    flurm_job_array:task_started(Id, 0, 1000),
    drain(),
    ok = flurm_job_array:cancel_array_task(Id, 0),
    {ok, T} = flurm_job_array:get_array_task(Id, 0),
    ?assertEqual(cancelled, T#array_task.state).

test_cancel_task_completed() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_completed(Id, 0, 0),
    drain(),
    ?assertEqual({error, task_not_cancellable},
                 flurm_job_array:cancel_array_task(Id, 0)).

test_cancel_task_cancelled() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    ok = flurm_job_array:cancel_array_task(Id, 0),
    %% Already cancelled -> not_cancellable
    ?assertEqual({error, task_not_cancellable},
                 flurm_job_array:cancel_array_task(Id, 0)).

test_cancel_task_not_found() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    ?assertEqual({error, not_found},
                 flurm_job_array:cancel_array_task(Id, 999)),
    ?assertEqual({error, not_found},
                 flurm_job_array:cancel_array_task(999999, 0)).

%%====================================================================
%% update_task_state Tests
%%====================================================================

test_update_task_state() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    ok = flurm_job_array:update_task_state(Id, 0, #{
        state => running, job_id => 2000,
        start_time => erlang:system_time(second)
    }),
    drain(),
    {ok, T} = flurm_job_array:get_array_task(Id, 0),
    ?assertEqual(running, T#array_task.state),
    ?assertEqual(2000, T#array_task.job_id).

test_update_task_state_nonexistent() ->
    %% No crash, just ok
    ok = flurm_job_array:update_task_state(999999, 0, #{state => running}),
    drain().

%%====================================================================
%% Task Lifecycle Tests
%%====================================================================

test_task_started() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    ok = flurm_job_array:task_started(Id, 0, 3000),
    drain(),
    {ok, T} = flurm_job_array:get_array_task(Id, 0),
    ?assertEqual(running, T#array_task.state),
    ?assertEqual(3000, T#array_task.job_id),
    ?assertNotEqual(undefined, T#array_task.start_time).

test_task_completed_success() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-0">>),
    flurm_job_array:task_started(Id, 0, 3000),
    drain(),
    ok = flurm_job_array:task_completed(Id, 0, 0),
    drain(),
    {ok, T} = flurm_job_array:get_array_task(Id, 0),
    ?assertEqual(completed, T#array_task.state),
    ?assertEqual(0, T#array_task.exit_code),
    ?assertNotEqual(undefined, T#array_task.end_time).

test_task_completed_failure() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-0">>),
    flurm_job_array:task_started(Id, 0, 3000),
    drain(),
    ok = flurm_job_array:task_completed(Id, 0, 127),
    drain(),
    {ok, T} = flurm_job_array:get_array_task(Id, 0),
    ?assertEqual(failed, T#array_task.state),
    ?assertEqual(127, T#array_task.exit_code).

%%====================================================================
%% update_array_job_state Tests
%%====================================================================

test_array_job_pending_to_running() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    %% Array starts pending
    {ok, AJ1} = flurm_job_array:get_array_job(Id),
    ?assertEqual(pending, AJ1#array_job.state),
    %% First task starts -> array transitions to running
    flurm_job_array:task_started(Id, 0, 1000),
    drain(),
    {ok, AJ2} = flurm_job_array:get_array_job(Id),
    ?assertEqual(running, AJ2#array_job.state),
    ?assertNotEqual(undefined, AJ2#array_job.start_time).

test_array_job_already_running() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    flurm_job_array:task_started(Id, 0, 1000),
    drain(),
    {ok, AJ1} = flurm_job_array:get_array_job(Id),
    StartTime1 = AJ1#array_job.start_time,
    %% Second task starts -> no re-transition, start_time unchanged
    flurm_job_array:task_started(Id, 1, 1001),
    drain(),
    {ok, AJ2} = flurm_job_array:get_array_job(Id),
    ?assertEqual(running, AJ2#array_job.state),
    ?assertEqual(StartTime1, AJ2#array_job.start_time).

%%====================================================================
%% check_array_completion Tests
%%====================================================================

test_completion_all_completed() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-1">>),
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_started(Id, 1, 1001),
    flurm_job_array:task_completed(Id, 0, 0),
    flurm_job_array:task_completed(Id, 1, 0),
    drain(),
    {ok, AJ} = flurm_job_array:get_array_job(Id),
    ?assertEqual(completed, AJ#array_job.state),
    ?assertNotEqual(undefined, AJ#array_job.end_time).

test_completion_all_cancelled() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-1">>),
    ok = flurm_job_array:cancel_array_task(Id, 0),
    ok = flurm_job_array:cancel_array_task(Id, 1),
    {ok, AJ} = flurm_job_array:get_array_job(Id),
    ?assertEqual(cancelled, AJ#array_job.state).

test_completion_some_failed() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-1">>),
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_started(Id, 1, 1001),
    flurm_job_array:task_completed(Id, 0, 0),
    flurm_job_array:task_completed(Id, 1, 1),
    drain(),
    {ok, AJ} = flurm_job_array:get_array_job(Id),
    ?assertEqual(failed, AJ#array_job.state).

test_completion_not_done() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-2">>),
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_completed(Id, 0, 0),
    drain(),
    %% Only 1 of 3 done -> array not yet completed
    {ok, AJ} = flurm_job_array:get_array_job(Id),
    ?assertEqual(running, AJ#array_job.state),
    ?assertEqual(undefined, AJ#array_job.end_time).

test_completion_job_deleted() ->
    %% Covers line 503: check_array_completion when all tasks are done
    %% but the array job record has been deleted from ETS (orphan tasks).
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-0">>),
    %% Delete the array job record from ETS so check_array_completion
    %% hits the [] -> ok branch
    ets:delete(flurm_array_jobs, Id),
    %% Cancel the only task -- cancel_task_internal calls check_array_completion
    %% which finds all tasks done but no job record
    [Task] = ets:lookup(flurm_array_tasks, {Id, 0}),
    ets:insert(flurm_array_tasks, Task#array_task{
        state = cancelled, end_time = erlang:system_time(second)
    }),
    %% Directly trigger check_array_completion via update_task_state
    %% which calls check_array_completion internally
    flurm_job_array:update_task_state(Id, 0, #{state => cancelled}),
    drain(),
    %% The job record should still not exist
    ?assertEqual({error, not_found}, flurm_job_array:get_array_job(Id)).

%%====================================================================
%% get_task_env Tests
%%====================================================================

test_get_task_env_success() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-9">>),
    Env = flurm_job_array:get_task_env(Id, 5),
    ?assert(is_map(Env)),
    ?assertEqual(integer_to_binary(Id), maps:get(<<"SLURM_ARRAY_JOB_ID">>, Env)),
    ?assertEqual(<<"5">>, maps:get(<<"SLURM_ARRAY_TASK_ID">>, Env)),
    ?assertEqual(<<"10">>, maps:get(<<"SLURM_ARRAY_TASK_COUNT">>, Env)),
    ?assertEqual(<<"0">>, maps:get(<<"SLURM_ARRAY_TASK_MIN">>, Env)),
    ?assertEqual(<<"9">>, maps:get(<<"SLURM_ARRAY_TASK_MAX">>, Env)).

test_get_task_env_error() ->
    Env = flurm_job_array:get_task_env(999999, 0),
    ?assertEqual(#{}, Env).

%%====================================================================
%% can_schedule_task Tests
%%====================================================================

test_can_schedule_unlimited() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-9">>),
    ?assertEqual(true, flurm_job_array:can_schedule_task(Id)).

test_can_schedule_below_max() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-9%3">>),
    flurm_job_array:task_started(Id, 0, 1000),
    drain(),
    %% 1 running < 3 max -> true
    ?assertEqual(true, flurm_job_array:can_schedule_task(Id)).

test_can_schedule_at_max() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-9%2">>),
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_started(Id, 1, 1001),
    drain(),
    %% 2 running == 2 max -> false
    ?assertEqual(false, flurm_job_array:can_schedule_task(Id)).

test_can_schedule_nonexistent() ->
    ?assertEqual(false, flurm_job_array:can_schedule_task(999999)).

%%====================================================================
%% schedule_next_task Tests
%%====================================================================

test_schedule_next_found() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-4">>),
    {ok, Task} = flurm_job_array:schedule_next_task(Id),
    ?assertEqual(0, Task#array_task.task_id).

test_schedule_next_throttled() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-4%1">>),
    flurm_job_array:task_started(Id, 0, 1000),
    drain(),
    ?assertEqual({error, throttled}, flurm_job_array:schedule_next_task(Id)).

test_schedule_next_no_pending() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-0">>),
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_completed(Id, 0, 0),
    drain(),
    ?assertEqual({error, no_pending}, flurm_job_array:schedule_next_task(Id)).

%%====================================================================
%% get_schedulable_count Tests
%%====================================================================

test_schedulable_count_unlimited() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-9">>),
    ?assertEqual(10, flurm_job_array:get_schedulable_count(Id)).

test_schedulable_count_limited() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-9%3">>),
    ?assertEqual(3, flurm_job_array:get_schedulable_count(Id)),
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_started(Id, 1, 1001),
    drain(),
    ?assertEqual(1, flurm_job_array:get_schedulable_count(Id)).

test_schedulable_count_nonexistent() ->
    ?assertEqual(0, flurm_job_array:get_schedulable_count(999999)).

%%====================================================================
%% get_schedulable_tasks Tests
%%====================================================================

test_schedulable_tasks() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-9%3">>),
    Tasks = flurm_job_array:get_schedulable_tasks(Id),
    ?assertEqual(3, length(Tasks)),
    TaskIds = [T#array_task.task_id || T <- Tasks],
    ?assertEqual([0, 1, 2], TaskIds).

test_schedulable_tasks_empty() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-4%2">>),
    flurm_job_array:task_started(Id, 0, 1000),
    flurm_job_array:task_started(Id, 1, 1001),
    drain(),
    ?assertEqual([], flurm_job_array:get_schedulable_tasks(Id)).

%%====================================================================
%% gen_server Catch-All / Callback Tests
%%====================================================================

test_handle_call_unknown() ->
    Result = gen_server:call(flurm_job_array, {totally, unknown, request}),
    ?assertEqual({error, unknown_request}, Result).

test_handle_cast_unknown() ->
    gen_server:cast(flurm_job_array, {totally_unknown_cast}),
    drain(),
    ?assert(is_process_alive(whereis(flurm_job_array))).

test_handle_cast_check_throttle_with_pending() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-4%2">>),
    flurm_job_array:task_started(Id, 0, 1000),
    drain(),
    %% Complete task so check_throttle is triggered; there are pending tasks
    flurm_job_array:task_completed(Id, 0, 0),
    drain(),
    %% Server should still be alive, and there should be schedulable tasks
    ?assert(is_process_alive(whereis(flurm_job_array))),
    ?assert(flurm_job_array:get_schedulable_count(Id) > 0).

test_handle_cast_check_throttle_no_pending() ->
    BaseJob = make_base_job(),
    {ok, Id} = flurm_job_array:create_array_job(BaseJob, <<"0-0">>),
    flurm_job_array:task_started(Id, 0, 1000),
    drain(),
    %% Complete last task -> check_throttle with no pending
    flurm_job_array:task_completed(Id, 0, 0),
    drain(),
    ?assert(is_process_alive(whereis(flurm_job_array))).

test_handle_info_unknown() ->
    whereis(flurm_job_array) ! {arbitrary, info, message},
    drain(),
    ?assert(is_process_alive(whereis(flurm_job_array))).

test_terminate() ->
    %% The server handles terminate correctly; tested by cleanup.
    %% Also verify it stays alive right now.
    ?assert(is_process_alive(whereis(flurm_job_array))).

test_code_change() ->
    %% Verify code_change works by calling it directly on the state
    State = #state{next_array_id = 42},
    ?assertEqual({ok, State},
                 flurm_job_array:code_change("1.0", State, [])).

%%====================================================================
%% start_link already_started Branch
%%====================================================================

test_start_link_already_started() ->
    %% The gen_server is already running from setup/0.
    %% Calling start_link again should hit the {already_started, Pid} branch
    %% and return {ok, Pid}.
    Result = flurm_job_array:start_link(),
    ?assertMatch({ok, _Pid}, Result),
    {ok, Pid} = Result,
    ?assertEqual(whereis(flurm_job_array), Pid).

%%====================================================================
%% TEST-exported Internal Function Tests
%%====================================================================

test_do_parse_array_spec() ->
    %% Range without step or percent
    S1 = flurm_job_array:do_parse_array_spec("0-10"),
    ?assertEqual(0, S1#array_spec.start_idx),
    ?assertEqual(10, S1#array_spec.end_idx),
    ?assertEqual(1, S1#array_spec.step),
    ?assertEqual(unlimited, S1#array_spec.max_concurrent),

    %% Range with step
    S2 = flurm_job_array:do_parse_array_spec("0-100:5"),
    ?assertEqual(5, S2#array_spec.step),

    %% Range with percent
    S3 = flurm_job_array:do_parse_array_spec("0-50%10"),
    ?assertEqual(10, S3#array_spec.max_concurrent),

    %% Range with step and percent
    S4 = flurm_job_array:do_parse_array_spec("0-100:2%5"),
    ?assertEqual(2, S4#array_spec.step),
    ?assertEqual(5, S4#array_spec.max_concurrent),

    %% Comma list
    S5 = flurm_job_array:do_parse_array_spec("1,3,5"),
    ?assertEqual([1, 3, 5], S5#array_spec.indices),

    %% Single value
    S6 = flurm_job_array:do_parse_array_spec("42"),
    ?assertEqual(42, S6#array_spec.start_idx),
    ?assertEqual(42, S6#array_spec.end_idx).

test_parse_range_spec_with_step() ->
    S = flurm_job_array:parse_range_spec("0-100:10", unlimited),
    ?assertEqual(0, S#array_spec.start_idx),
    ?assertEqual(100, S#array_spec.end_idx),
    ?assertEqual(10, S#array_spec.step),
    ?assertEqual(unlimited, S#array_spec.max_concurrent).

test_parse_range_spec_no_step() ->
    S = flurm_job_array:parse_range_spec("0-50", 5),
    ?assertEqual(0, S#array_spec.start_idx),
    ?assertEqual(50, S#array_spec.end_idx),
    ?assertEqual(1, S#array_spec.step),
    ?assertEqual(5, S#array_spec.max_concurrent).

test_parse_range_spec_single() ->
    S = flurm_job_array:parse_range_spec("42", unlimited),
    ?assertEqual(42, S#array_spec.start_idx),
    ?assertEqual(42, S#array_spec.end_idx),
    ?assertEqual(1, S#array_spec.step).

test_parse_list_spec() ->
    S = flurm_job_array:parse_list_spec("1,3,5,7", 2),
    ?assertEqual([1, 3, 5, 7], S#array_spec.indices),
    ?assertEqual(2, S#array_spec.max_concurrent),
    ?assertEqual(undefined, S#array_spec.start_idx).

test_parse_list_element_range() ->
    ?assertEqual([1, 2, 3, 4, 5], flurm_job_array:parse_list_element("1-5")).

test_parse_list_element_single() ->
    ?assertEqual([42], flurm_job_array:parse_list_element("42")).

test_generate_task_ids_indices() ->
    Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                       step = 1, indices = [1, 5, 10],
                       max_concurrent = unlimited},
    ?assertEqual([1, 5, 10], flurm_job_array:generate_task_ids(Spec)).

test_generate_task_ids_range() ->
    Spec = #array_spec{start_idx = 0, end_idx = 10, step = 2,
                       indices = undefined, max_concurrent = unlimited},
    ?assertEqual([0, 2, 4, 6, 8, 10], flurm_job_array:generate_task_ids(Spec)).

test_apply_task_updates() ->
    Task = #array_task{
        id = {1, 0}, array_job_id = 1, task_id = 0,
        job_id = undefined, state = pending, exit_code = undefined,
        start_time = undefined, end_time = undefined, node = undefined
    },
    Now = erlang:system_time(second),
    Updated = flurm_job_array:apply_task_updates(Task, #{
        state => running, job_id => 5000,
        start_time => Now, node => <<"node01">>
    }),
    ?assertEqual(running, Updated#array_task.state),
    ?assertEqual(5000, Updated#array_task.job_id),
    ?assertEqual(Now, Updated#array_task.start_time),
    ?assertEqual(<<"node01">>, Updated#array_task.node),
    %% Fields not in the update map are preserved
    ?assertEqual({1, 0}, Updated#array_task.id),
    ?assertEqual(1, Updated#array_task.array_job_id),
    ?assertEqual(0, Updated#array_task.task_id),
    ?assertEqual(undefined, Updated#array_task.exit_code),
    ?assertEqual(undefined, Updated#array_task.end_time),

    %% Empty update preserves everything
    Same = flurm_job_array:apply_task_updates(Task, #{}),
    ?assertEqual(pending, Same#array_task.state),
    ?assertEqual(undefined, Same#array_task.job_id).
