%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_job_array module
%%%
%%% These tests exercise the flurm_job_array module WITHOUT any mocking.
%%% We test gen_server callbacks directly and pure functions.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_array_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% Record definitions copied from flurm_job_array for testing
-record(array_spec, {
    start_idx :: non_neg_integer() | undefined,
    end_idx :: non_neg_integer() | undefined,
    step :: pos_integer(),
    indices :: [non_neg_integer()] | undefined,
    max_concurrent :: pos_integer() | unlimited
}).

-record(array_job, {
    id :: pos_integer(),
    name :: binary(),
    user :: binary(),
    partition :: binary(),
    base_job :: #job{},
    task_ids :: [non_neg_integer()],
    task_count :: pos_integer(),
    max_concurrent :: pos_integer() | unlimited,
    state :: pending | running | completed | failed | cancelled,
    submit_time :: non_neg_integer(),
    start_time :: non_neg_integer() | undefined,
    end_time :: non_neg_integer() | undefined,
    stats :: map()
}).

-record(array_task, {
    id :: {pos_integer(), non_neg_integer()},
    array_job_id :: pos_integer(),
    task_id :: non_neg_integer(),
    job_id :: pos_integer() | undefined,
    state :: pending | running | completed | failed | cancelled,
    exit_code :: integer() | undefined,
    start_time :: non_neg_integer() | undefined,
    end_time :: non_neg_integer() | undefined,
    node :: binary() | undefined
}).

-record(state, {
    next_array_id :: pos_integer()
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup and teardown for tests that need ETS tables
setup_ets() ->
    %% Create the ETS tables that flurm_job_array uses
    case ets:info(flurm_array_jobs) of
        undefined ->
            ets:new(flurm_array_jobs, [
                named_table, public, set,
                {keypos, #array_job.id}
            ]);
        _ ->
            ets:delete_all_objects(flurm_array_jobs)
    end,
    case ets:info(flurm_array_tasks) of
        undefined ->
            ets:new(flurm_array_tasks, [
                named_table, public, set,
                {keypos, #array_task.id}
            ]);
        _ ->
            ets:delete_all_objects(flurm_array_tasks)
    end,
    ok.

cleanup_ets(_) ->
    catch ets:delete_all_objects(flurm_array_jobs),
    catch ets:delete_all_objects(flurm_array_tasks),
    ok.

%% Helper to create a base job for testing
make_base_job() ->
    #job{
        id = 1000,
        name = <<"test_array_job">>,
        user = <<"testuser">>,
        partition = <<"normal">>,
        state = pending,
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 1024,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second),
        allocated_nodes = []
    }.

%%====================================================================
%% Parse Array Spec Tests
%%====================================================================

parse_array_spec_test_() ->
    {"Array specification parsing tests", [
        {"Parse simple range",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-10">>),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(10, Spec#array_spec.end_idx),
             ?assertEqual(1, Spec#array_spec.step),
             ?assertEqual(unlimited, Spec#array_spec.max_concurrent),
             ?assertEqual(undefined, Spec#array_spec.indices)
         end},

        {"Parse range with step",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-100:5">>),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(100, Spec#array_spec.end_idx),
             ?assertEqual(5, Spec#array_spec.step),
             ?assertEqual(unlimited, Spec#array_spec.max_concurrent)
         end},

        {"Parse range with max concurrent",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-50%10">>),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(50, Spec#array_spec.end_idx),
             ?assertEqual(1, Spec#array_spec.step),
             ?assertEqual(10, Spec#array_spec.max_concurrent)
         end},

        {"Parse range with step and max concurrent",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"0-100:2%5">>),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(100, Spec#array_spec.end_idx),
             ?assertEqual(2, Spec#array_spec.step),
             ?assertEqual(5, Spec#array_spec.max_concurrent)
         end},

        {"Parse comma-separated list",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"1,3,5,7,9">>),
             ?assertEqual([1,3,5,7,9], Spec#array_spec.indices),
             ?assertEqual(unlimited, Spec#array_spec.max_concurrent),
             ?assertEqual(undefined, Spec#array_spec.start_idx),
             ?assertEqual(undefined, Spec#array_spec.end_idx)
         end},

        {"Parse comma-separated list with max concurrent",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"1,2,3,4,5%2">>),
             ?assertEqual([1,2,3,4,5], Spec#array_spec.indices),
             ?assertEqual(2, Spec#array_spec.max_concurrent)
         end},

        {"Parse single value",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"42">>),
             ?assertEqual(42, Spec#array_spec.start_idx),
             ?assertEqual(42, Spec#array_spec.end_idx),
             ?assertEqual(1, Spec#array_spec.step)
         end},

        {"Parse string list (not binary)",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec("0-5"),
             ?assertEqual(0, Spec#array_spec.start_idx),
             ?assertEqual(5, Spec#array_spec.end_idx)
         end},

        {"Parse mixed list with ranges",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"1-3,10,20-22">>),
             ?assertEqual([1,2,3,10,20,21,22], Spec#array_spec.indices)
         end},

        {"Parse list with duplicate handling",
         fun() ->
             {ok, Spec} = flurm_job_array:parse_array_spec(<<"1,2,2,3,1">>),
             %% lists:usort removes duplicates and sorts
             ?assertEqual([1,2,3], Spec#array_spec.indices)
         end}
    ]}.

%%====================================================================
%% Format Array Spec Tests
%%====================================================================

format_array_spec_test_() ->
    {"Array specification formatting tests", [
        {"Format simple range",
         fun() ->
             Spec = #array_spec{start_idx = 0, end_idx = 10, step = 1,
                                indices = undefined, max_concurrent = unlimited},
             Result = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(<<"0-10">>, Result)
         end},

        {"Format single value",
         fun() ->
             Spec = #array_spec{start_idx = 5, end_idx = 5, step = 1,
                                indices = undefined, max_concurrent = unlimited},
             Result = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(<<"5">>, Result)
         end},

        {"Format range with step",
         fun() ->
             Spec = #array_spec{start_idx = 0, end_idx = 100, step = 10,
                                indices = undefined, max_concurrent = unlimited},
             Result = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(<<"0-100:10">>, Result)
         end},

        {"Format range with max concurrent",
         fun() ->
             Spec = #array_spec{start_idx = 0, end_idx = 50, step = 1,
                                indices = undefined, max_concurrent = 5},
             Result = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(<<"0-50%5">>, Result)
         end},

        {"Format range with step and max concurrent",
         fun() ->
             Spec = #array_spec{start_idx = 0, end_idx = 100, step = 2,
                                indices = undefined, max_concurrent = 10},
             Result = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(<<"0-100:2%10">>, Result)
         end},

        {"Format indices list",
         fun() ->
             Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                                step = 1, indices = [1, 3, 5, 7],
                                max_concurrent = unlimited},
             Result = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(<<"1,3,5,7">>, Result)
         end},

        {"Format indices list with max concurrent",
         fun() ->
             Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                                step = 1, indices = [1, 2, 3],
                                max_concurrent = 2},
             Result = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(<<"1,2,3%2">>, Result)
         end}
    ]}.

%%====================================================================
%% Expand Array Tests
%%====================================================================

expand_array_test_() ->
    {"Array expansion tests", [
        {"Expand simple range",
         fun() ->
             {ok, Tasks} = flurm_job_array:expand_array(<<"0-4">>),
             ?assertEqual(5, length(Tasks)),
             TaskIds = [maps:get(task_id, T) || T <- Tasks],
             ?assertEqual([0,1,2,3,4], TaskIds),
             %% Check common fields
             [First | _] = Tasks,
             ?assertEqual(5, maps:get(task_count, First)),
             ?assertEqual(0, maps:get(task_min, First)),
             ?assertEqual(4, maps:get(task_max, First)),
             ?assertEqual(unlimited, maps:get(max_concurrent, First))
         end},

        {"Expand range with step",
         fun() ->
             {ok, Tasks} = flurm_job_array:expand_array(<<"0-10:2">>),
             TaskIds = [maps:get(task_id, T) || T <- Tasks],
             ?assertEqual([0,2,4,6,8,10], TaskIds),
             ?assertEqual(6, length(Tasks))
         end},

        {"Expand range with max concurrent",
         fun() ->
             {ok, Tasks} = flurm_job_array:expand_array(<<"0-5%3">>),
             [First | _] = Tasks,
             ?assertEqual(3, maps:get(max_concurrent, First))
         end},

        {"Expand list spec",
         fun() ->
             {ok, Tasks} = flurm_job_array:expand_array(<<"1,5,10,20">>),
             TaskIds = [maps:get(task_id, T) || T <- Tasks],
             ?assertEqual([1,5,10,20], TaskIds),
             [First | _] = Tasks,
             ?assertEqual(4, maps:get(task_count, First)),
             ?assertEqual(1, maps:get(task_min, First)),
             ?assertEqual(20, maps:get(task_max, First))
         end},

        {"Expand array spec record directly",
         fun() ->
             Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                indices = undefined, max_concurrent = unlimited},
             {ok, Tasks} = flurm_job_array:expand_array(Spec),
             TaskIds = [maps:get(task_id, T) || T <- Tasks],
             ?assertEqual([0,1,2], TaskIds)
         end},

        {"Expand indices list directly",
         fun() ->
             Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                                step = 1, indices = [10, 20, 30],
                                max_concurrent = 2},
             {ok, Tasks} = flurm_job_array:expand_array(Spec),
             TaskIds = [maps:get(task_id, T) || T <- Tasks],
             ?assertEqual([10, 20, 30], TaskIds),
             [First | _] = Tasks,
             ?assertEqual(2, maps:get(max_concurrent, First))
         end}
    ]}.

%%====================================================================
%% gen_server init/1 Tests
%%====================================================================

init_test_() ->
    {"init/1 callback tests",
     {setup,
      fun() -> ok end,
      fun(_) ->
          catch ets:delete(flurm_array_jobs),
          catch ets:delete(flurm_array_tasks),
          ok
      end,
      [
          {"init creates ETS tables and initial state",
           fun() ->
               %% Ensure tables don't exist
               catch ets:delete(flurm_array_jobs),
               catch ets:delete(flurm_array_tasks),

               {ok, State} = flurm_job_array:init([]),

               %% Check state
               ?assertEqual(1, State#state.next_array_id),

               %% Check tables were created
               ?assertNotEqual(undefined, ets:info(flurm_array_jobs)),
               ?assertNotEqual(undefined, ets:info(flurm_array_tasks)),

               %% Check table options - should be named, public, set
               JobsInfo = ets:info(flurm_array_jobs),
               TasksInfo = ets:info(flurm_array_tasks),

               ?assertEqual(set, proplists:get_value(type, JobsInfo)),
               ?assertEqual(set, proplists:get_value(type, TasksInfo)),
               ?assertEqual(public, proplists:get_value(protection, JobsInfo)),
               ?assertEqual(public, proplists:get_value(protection, TasksInfo)),

               %% Cleanup
               ets:delete(flurm_array_jobs),
               ets:delete(flurm_array_tasks)
           end}
      ]}}.

%%====================================================================
%% gen_server handle_call/3 Tests - Create Array Job
%%====================================================================

handle_call_create_array_job_test_() ->
    {"handle_call create_array_job tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Create array job with range spec",
           fun() ->
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = unlimited},

               {reply, {ok, 1}, NewState} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               ?assertEqual(2, NewState#state.next_array_id),

               %% Verify array job was created in ETS
               [ArrayJob] = ets:lookup(flurm_array_jobs, 1),
               ?assertEqual(1, ArrayJob#array_job.id),
               ?assertEqual(<<"test_array_job">>, ArrayJob#array_job.name),
               ?assertEqual(<<"testuser">>, ArrayJob#array_job.user),
               ?assertEqual(<<"normal">>, ArrayJob#array_job.partition),
               ?assertEqual([0,1,2,3,4], ArrayJob#array_job.task_ids),
               ?assertEqual(5, ArrayJob#array_job.task_count),
               ?assertEqual(unlimited, ArrayJob#array_job.max_concurrent),
               ?assertEqual(pending, ArrayJob#array_job.state),

               %% Verify tasks were created
               Tasks = ets:tab2list(flurm_array_tasks),
               ?assertEqual(5, length(Tasks)),
               TaskIds = lists:sort([T#array_task.task_id || T <- Tasks]),
               ?assertEqual([0,1,2,3,4], TaskIds),

               %% Check individual task fields
               [{1, 0}] = [T#array_task.id || T <- Tasks, T#array_task.task_id =:= 0],
               [FirstTask | _] = [T || T <- Tasks, T#array_task.task_id =:= 0],
               ?assertEqual(1, FirstTask#array_task.array_job_id),
               ?assertEqual(pending, FirstTask#array_task.state),
               ?assertEqual(undefined, FirstTask#array_task.job_id),
               ?assertEqual(undefined, FirstTask#array_task.exit_code)
           end},

          {"Create array job with list spec",
           fun() ->
               State = #state{next_array_id = 5},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                                  step = 1, indices = [1, 5, 10],
                                  max_concurrent = 2},

               {reply, {ok, 5}, NewState} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               ?assertEqual(6, NewState#state.next_array_id),

               [ArrayJob] = ets:lookup(flurm_array_jobs, 5),
               ?assertEqual([1, 5, 10], ArrayJob#array_job.task_ids),
               ?assertEqual(3, ArrayJob#array_job.task_count),
               ?assertEqual(2, ArrayJob#array_job.max_concurrent)
           end},

          {"Create array job with step",
           fun() ->
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 10, step = 2,
                                  indices = undefined, max_concurrent = unlimited},

               {reply, {ok, 1}, _NewState} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               [ArrayJob] = ets:lookup(flurm_array_jobs, 1),
               ?assertEqual([0,2,4,6,8,10], ArrayJob#array_job.task_ids),
               ?assertEqual(6, ArrayJob#array_job.task_count)
           end},

          {"Reject empty array",
           fun() ->
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               %% Empty indices list
               Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                                  step = 1, indices = [],
                                  max_concurrent = unlimited},

               {reply, {error, empty_array}, SameState} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               ?assertEqual(1, SameState#state.next_array_id),
               ?assertEqual([], ets:tab2list(flurm_array_jobs))
           end},

          {"Reject array too large",
           fun() ->
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               %% More than 10000 tasks
               Spec = #array_spec{start_idx = 0, end_idx = 15000, step = 1,
                                  indices = undefined, max_concurrent = unlimited},

               {reply, {error, array_too_large}, SameState} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               ?assertEqual(1, SameState#state.next_array_id),
               ?assertEqual([], ets:tab2list(flurm_array_jobs))
           end},

          {"Multiple array job creation increments ID",
           fun() ->
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},

               {reply, {ok, 1}, State2} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),
               ?assertEqual(2, State2#state.next_array_id),

               {reply, {ok, 2}, State3} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State2),
               ?assertEqual(3, State3#state.next_array_id),

               {reply, {ok, 3}, State4} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State3),
               ?assertEqual(4, State4#state.next_array_id),

               %% Verify all three array jobs exist
               ?assertEqual(3, length(ets:tab2list(flurm_array_jobs)))
           end}
      ]}}.

%%====================================================================
%% gen_server handle_call/3 Tests - Cancel Array Job
%%====================================================================

handle_call_cancel_array_job_test_() ->
    {"handle_call cancel_array_job tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Cancel array job updates state",
           fun() ->
               %% First create an array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, State2} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Now cancel it
               {reply, ok, _State3} =
                   flurm_job_array:handle_call({cancel_array_job, 1}, {self(), ref}, State2),

               %% Verify array job state is cancelled
               [ArrayJob] = ets:lookup(flurm_array_jobs, 1),
               ?assertEqual(cancelled, ArrayJob#array_job.state),
               ?assertNotEqual(undefined, ArrayJob#array_job.end_time),

               %% Verify all tasks are cancelled
               Tasks = ets:tab2list(flurm_array_tasks),
               lists:foreach(fun(T) ->
                   ?assertEqual(cancelled, T#array_task.state)
               end, Tasks)
           end},

          {"Cancel non-existent array job returns error",
           fun() ->
               State = #state{next_array_id = 1},

               {reply, {error, not_found}, _} =
                   flurm_job_array:handle_call({cancel_array_job, 999}, {self(), ref}, State)
           end},

          {"Cancel only affects pending and running tasks",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, State2} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Manually set one task to completed
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = completed}),

               %% Cancel the array job
               {reply, ok, _} =
                   flurm_job_array:handle_call({cancel_array_job, 1}, {self(), ref}, State2),

               %% Task 0 should still be completed (not cancelled)
               [Task0After] = ets:lookup(flurm_array_tasks, {1, 0}),
               ?assertEqual(completed, Task0After#array_task.state),

               %% Other tasks should be cancelled
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               [Task2] = ets:lookup(flurm_array_tasks, {1, 2}),
               ?assertEqual(cancelled, Task1#array_task.state),
               ?assertEqual(cancelled, Task2#array_task.state)
           end}
      ]}}.

%%====================================================================
%% gen_server handle_call/3 Tests - Cancel Array Task
%%====================================================================

handle_call_cancel_array_task_test_() ->
    {"handle_call cancel_array_task tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Cancel single pending task",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, State2} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Cancel task 2
               {reply, ok, _} =
                   flurm_job_array:handle_call({cancel_array_task, 1, 2}, {self(), ref}, State2),

               %% Task 2 should be cancelled
               [Task2] = ets:lookup(flurm_array_tasks, {1, 2}),
               ?assertEqual(cancelled, Task2#array_task.state),

               %% Other tasks should still be pending
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ?assertEqual(pending, Task0#array_task.state),
               ?assertEqual(pending, Task1#array_task.state)
           end},

          {"Cancel running task",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, State2} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Mark task 1 as running
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = running, job_id = 5001}),

               %% Cancel running task
               {reply, ok, _} =
                   flurm_job_array:handle_call({cancel_array_task, 1, 1}, {self(), ref}, State2),

               [Task1After] = ets:lookup(flurm_array_tasks, {1, 1}),
               ?assertEqual(cancelled, Task1After#array_task.state)
           end},

          {"Cannot cancel completed task",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, State2} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Mark task 1 as completed
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = completed}),

               %% Try to cancel completed task
               {reply, {error, task_not_cancellable}, _} =
                   flurm_job_array:handle_call({cancel_array_task, 1, 1}, {self(), ref}, State2)
           end},

          {"Cancel non-existent task returns error",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, State2} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Try to cancel non-existent task
               {reply, {error, not_found}, _} =
                   flurm_job_array:handle_call({cancel_array_task, 1, 999}, {self(), ref}, State2)
           end},

          {"Cancel task from non-existent array returns error",
           fun() ->
               State = #state{next_array_id = 1},

               {reply, {error, not_found}, _} =
                   flurm_job_array:handle_call({cancel_array_task, 999, 0}, {self(), ref}, State)
           end}
      ]}}.

%%====================================================================
%% gen_server handle_call/3 Tests - Unknown Request
%%====================================================================

handle_call_unknown_test_() ->
    {"handle_call unknown request tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Unknown request returns error",
           fun() ->
               State = #state{next_array_id = 1},

               {reply, {error, unknown_request}, SameState} =
                   flurm_job_array:handle_call({some_unknown_request, foo}, {self(), ref}, State),

               ?assertEqual(State, SameState)
           end}
      ]}}.

%%====================================================================
%% gen_server handle_cast/2 Tests
%%====================================================================

handle_cast_update_task_state_test_() ->
    {"handle_cast update_task_state tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Update task state to running",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _State2} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Update task 0 to running
               Updates = #{state => running, job_id => 5001, start_time => 1234567890},
               {noreply, _} =
                   flurm_job_array:handle_cast({update_task_state, 1, 0, Updates}, State),

               %% Verify task was updated
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               ?assertEqual(running, Task0#array_task.state),
               ?assertEqual(5001, Task0#array_task.job_id),
               ?assertEqual(1234567890, Task0#array_task.start_time),

               %% Verify array job state was updated to running
               [ArrayJob] = ets:lookup(flurm_array_jobs, 1),
               ?assertEqual(running, ArrayJob#array_job.state)
           end},

          {"Update task state to completed",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 0, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _State2} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% First set to running
               {noreply, _} =
                   flurm_job_array:handle_cast({update_task_state, 1, 0, #{state => running}}, State),

               %% Then complete it
               Updates = #{state => completed, exit_code => 0, end_time => 1234567900},
               {noreply, _} =
                   flurm_job_array:handle_cast({update_task_state, 1, 0, Updates}, State),

               %% Verify task was updated
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               ?assertEqual(completed, Task0#array_task.state),
               ?assertEqual(0, Task0#array_task.exit_code),

               %% Array job should now be completed (only task is done)
               [ArrayJob] = ets:lookup(flurm_array_jobs, 1),
               ?assertEqual(completed, ArrayJob#array_job.state)
           end},

          {"Update task with node assignment",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               Updates = #{state => running, node => <<"node001">>},
               {noreply, _} =
                   flurm_job_array:handle_cast({update_task_state, 1, 1, Updates}, State),

               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ?assertEqual(<<"node001">>, Task1#array_task.node)
           end},

          {"Update non-existent task does nothing",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% This should not crash
               {noreply, _} =
                   flurm_job_array:handle_cast({update_task_state, 1, 999, #{state => running}}, State)
           end},

          {"Array completion when all tasks done with failures",
           fun() ->
               %% Create array job with 3 tasks
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Complete task 0 successfully
               {noreply, _} = flurm_job_array:handle_cast({update_task_state, 1, 0,
                   #{state => completed, exit_code => 0}}, State),

               %% Complete task 1 successfully
               {noreply, _} = flurm_job_array:handle_cast({update_task_state, 1, 1,
                   #{state => completed, exit_code => 0}}, State),

               %% Fail task 2
               {noreply, _} = flurm_job_array:handle_cast({update_task_state, 1, 2,
                   #{state => failed, exit_code => 1}}, State),

               %% Array job should be marked as failed (has failed tasks)
               [ArrayJob] = ets:lookup(flurm_array_jobs, 1),
               ?assertEqual(failed, ArrayJob#array_job.state)
           end}
      ]}}.

handle_cast_check_throttle_test_() ->
    {"handle_cast check_throttle tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"check_throttle with no pending tasks",
           fun() ->
               State = #state{next_array_id = 1},
               %% Should not crash even without any array jobs
               {noreply, SameState} =
                   flurm_job_array:handle_cast({check_throttle, 999}, State),
               ?assertEqual(State, SameState)
           end},

          {"check_throttle returns noreply",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               {noreply, _} =
                   flurm_job_array:handle_cast({check_throttle, 1}, State)
           end}
      ]}}.

handle_cast_unknown_test_() ->
    {"handle_cast unknown message tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Unknown cast returns noreply",
           fun() ->
               State = #state{next_array_id = 1},

               {noreply, SameState} =
                   flurm_job_array:handle_cast({unknown_cast, data}, State),

               ?assertEqual(State, SameState)
           end}
      ]}}.

%%====================================================================
%% gen_server handle_info/2, terminate/2, code_change/3 Tests
%%====================================================================

handle_info_test_() ->
    {"handle_info tests", [
        {"handle_info ignores unknown messages",
         fun() ->
             State = #state{next_array_id = 1},
             {noreply, SameState} = flurm_job_array:handle_info(some_message, State),
             ?assertEqual(State, SameState)
         end}
    ]}.

terminate_test_() ->
    {"terminate tests", [
        {"terminate returns ok",
         fun() ->
             State = #state{next_array_id = 1},
             ?assertEqual(ok, flurm_job_array:terminate(normal, State)),
             ?assertEqual(ok, flurm_job_array:terminate(shutdown, State)),
             ?assertEqual(ok, flurm_job_array:terminate({error, some_reason}, State))
         end}
    ]}.

code_change_test_() ->
    {"code_change tests", [
        {"code_change returns ok with state",
         fun() ->
             State = #state{next_array_id = 42},
             ?assertEqual({ok, State}, flurm_job_array:code_change("1.0", State, [])),
             ?assertEqual({ok, State}, flurm_job_array:code_change("2.0", State, extra))
         end}
    ]}.

%%====================================================================
%% ETS-based Function Tests (get_array_job, get_array_task, etc.)
%%====================================================================

get_array_job_test_() ->
    {"get_array_job tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Get existing array job",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               {ok, ArrayJob} = flurm_job_array:get_array_job(1),
               ?assertEqual(1, ArrayJob#array_job.id),
               ?assertEqual(<<"test_array_job">>, ArrayJob#array_job.name)
           end},

          {"Get non-existent array job returns error",
           fun() ->
               ?assertEqual({error, not_found}, flurm_job_array:get_array_job(999))
           end}
      ]}}.

get_array_task_test_() ->
    {"get_array_task tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Get existing task",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               {ok, Task} = flurm_job_array:get_array_task(1, 2),
               ?assertEqual({1, 2}, Task#array_task.id),
               ?assertEqual(2, Task#array_task.task_id),
               ?assertEqual(1, Task#array_task.array_job_id)
           end},

          {"Get non-existent task returns error",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 2, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               ?assertEqual({error, not_found}, flurm_job_array:get_array_task(1, 999))
           end},

          {"Get task from non-existent array returns error",
           fun() ->
               ?assertEqual({error, not_found}, flurm_job_array:get_array_task(999, 0))
           end}
      ]}}.

get_array_tasks_test_() ->
    {"get_array_tasks tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Get all tasks for array",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               Tasks = flurm_job_array:get_array_tasks(1),
               ?assertEqual(5, length(Tasks)),
               TaskIds = lists:sort([T#array_task.task_id || T <- Tasks]),
               ?assertEqual([0,1,2,3,4], TaskIds)
           end},

          {"Get tasks for non-existent array returns empty list",
           fun() ->
               ?assertEqual([], flurm_job_array:get_array_tasks(999))
           end}
      ]}}.

get_pending_tasks_test_() ->
    {"get_pending_tasks tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Get pending tasks only",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set some tasks to different states
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = running}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = completed}),

               PendingTasks = flurm_job_array:get_pending_tasks(1),
               ?assertEqual(3, length(PendingTasks)),
               TaskIds = lists:sort([T#array_task.task_id || T <- PendingTasks]),
               ?assertEqual([2,3,4], TaskIds)
           end}
      ]}}.

get_running_tasks_test_() ->
    {"get_running_tasks tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Get running tasks only",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set some tasks to running
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task2] = ets:lookup(flurm_array_tasks, {1, 2}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = running}),
               ets:insert(flurm_array_tasks, Task2#array_task{state = running}),

               RunningTasks = flurm_job_array:get_running_tasks(1),
               ?assertEqual(2, length(RunningTasks)),
               TaskIds = lists:sort([T#array_task.task_id || T <- RunningTasks]),
               ?assertEqual([0,2], TaskIds)
           end}
      ]}}.

get_completed_tasks_test_() ->
    {"get_completed_tasks tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Get completed tasks only",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set some tasks to completed
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               [Task3] = ets:lookup(flurm_array_tasks, {1, 3}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = completed}),
               ets:insert(flurm_array_tasks, Task3#array_task{state = completed}),

               CompletedTasks = flurm_job_array:get_completed_tasks(1),
               ?assertEqual(2, length(CompletedTasks)),
               TaskIds = lists:sort([T#array_task.task_id || T <- CompletedTasks]),
               ?assertEqual([1,3], TaskIds)
           end}
      ]}}.

get_array_stats_test_() ->
    {"get_array_stats tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Get stats for array with mixed states",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 9, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set tasks to various states:
               %% 0,1 - running (2)
               %% 2,3,4 - completed (3)
               %% 5 - failed (1)
               %% 6 - cancelled (1)
               %% 7,8,9 - pending (3)
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               [Task2] = ets:lookup(flurm_array_tasks, {1, 2}),
               [Task3] = ets:lookup(flurm_array_tasks, {1, 3}),
               [Task4] = ets:lookup(flurm_array_tasks, {1, 4}),
               [Task5] = ets:lookup(flurm_array_tasks, {1, 5}),
               [Task6] = ets:lookup(flurm_array_tasks, {1, 6}),

               ets:insert(flurm_array_tasks, Task0#array_task{state = running}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = running}),
               ets:insert(flurm_array_tasks, Task2#array_task{state = completed}),
               ets:insert(flurm_array_tasks, Task3#array_task{state = completed}),
               ets:insert(flurm_array_tasks, Task4#array_task{state = completed}),
               ets:insert(flurm_array_tasks, Task5#array_task{state = failed}),
               ets:insert(flurm_array_tasks, Task6#array_task{state = cancelled}),

               Stats = flurm_job_array:get_array_stats(1),
               ?assertEqual(10, maps:get(total, Stats)),
               ?assertEqual(3, maps:get(pending, Stats)),
               ?assertEqual(2, maps:get(running, Stats)),
               ?assertEqual(3, maps:get(completed, Stats)),
               ?assertEqual(1, maps:get(failed, Stats)),
               ?assertEqual(1, maps:get(cancelled, Stats))
           end},

          {"Get stats for non-existent array",
           fun() ->
               Stats = flurm_job_array:get_array_stats(999),
               ?assertEqual(0, maps:get(total, Stats)),
               ?assertEqual(0, maps:get(pending, Stats)),
               ?assertEqual(0, maps:get(running, Stats)),
               ?assertEqual(0, maps:get(completed, Stats)),
               ?assertEqual(0, maps:get(failed, Stats)),
               ?assertEqual(0, maps:get(cancelled, Stats))
           end}
      ]}}.

%%====================================================================
%% Task Environment Tests
%%====================================================================

get_task_env_test_() ->
    {"get_task_env tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Get environment variables for task",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 9, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               Env = flurm_job_array:get_task_env(1, 5),
               ?assertEqual(<<"1">>, maps:get(<<"SLURM_ARRAY_JOB_ID">>, Env)),
               ?assertEqual(<<"5">>, maps:get(<<"SLURM_ARRAY_TASK_ID">>, Env)),
               ?assertEqual(<<"10">>, maps:get(<<"SLURM_ARRAY_TASK_COUNT">>, Env)),
               ?assertEqual(<<"0">>, maps:get(<<"SLURM_ARRAY_TASK_MIN">>, Env)),
               ?assertEqual(<<"9">>, maps:get(<<"SLURM_ARRAY_TASK_MAX">>, Env))
           end},

          {"Get environment variables with indices list",
           fun() ->
               %% Create array job with specific indices
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = undefined, end_idx = undefined,
                                  step = 1, indices = [5, 10, 20, 50],
                                  max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               Env = flurm_job_array:get_task_env(1, 10),
               ?assertEqual(<<"1">>, maps:get(<<"SLURM_ARRAY_JOB_ID">>, Env)),
               ?assertEqual(<<"10">>, maps:get(<<"SLURM_ARRAY_TASK_ID">>, Env)),
               ?assertEqual(<<"4">>, maps:get(<<"SLURM_ARRAY_TASK_COUNT">>, Env)),
               ?assertEqual(<<"5">>, maps:get(<<"SLURM_ARRAY_TASK_MIN">>, Env)),
               ?assertEqual(<<"50">>, maps:get(<<"SLURM_ARRAY_TASK_MAX">>, Env))
           end},

          {"Get environment for non-existent array returns empty map",
           fun() ->
               Env = flurm_job_array:get_task_env(999, 0),
               ?assertEqual(#{}, Env)
           end}
      ]}}.

%%====================================================================
%% Throttling Tests
%%====================================================================

can_schedule_task_test_() ->
    {"can_schedule_task tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Can schedule when unlimited",
           fun() ->
               %% Create array job with unlimited concurrent
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               ?assertEqual(true, flurm_job_array:can_schedule_task(1))
           end},

          {"Can schedule when below limit",
           fun() ->
               %% Create array job with max 3 concurrent
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 9, step = 1,
                                  indices = undefined, max_concurrent = 3},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set 2 tasks to running
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = running}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = running}),

               ?assertEqual(true, flurm_job_array:can_schedule_task(1))
           end},

          {"Cannot schedule when at limit",
           fun() ->
               %% Create array job with max 2 concurrent
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 9, step = 1,
                                  indices = undefined, max_concurrent = 2},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set 2 tasks to running (at limit)
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = running}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = running}),

               ?assertEqual(false, flurm_job_array:can_schedule_task(1))
           end},

          {"Cannot schedule non-existent array",
           fun() ->
               ?assertEqual(false, flurm_job_array:can_schedule_task(999))
           end}
      ]}}.

schedule_next_task_test_() ->
    {"schedule_next_task tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Schedule next pending task",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               {ok, Task} = flurm_job_array:schedule_next_task(1),
               %% Should return task with lowest task_id
               ?assertEqual(0, Task#array_task.task_id)
           end},

          {"Schedule returns first pending by task_id",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set tasks 0, 1 to running
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = running}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = running}),

               {ok, Task} = flurm_job_array:schedule_next_task(1),
               ?assertEqual(2, Task#array_task.task_id)
           end},

          {"Schedule returns throttled when at limit",
           fun() ->
               %% Create array job with max 2 concurrent
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 4, step = 1,
                                  indices = undefined, max_concurrent = 2},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set 2 tasks to running
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = running}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = running}),

               ?assertEqual({error, throttled}, flurm_job_array:schedule_next_task(1))
           end},

          {"Schedule returns no_pending when all done",
           fun() ->
               %% Create array job
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 1, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set all tasks to completed
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = completed}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = completed}),

               ?assertEqual({error, no_pending}, flurm_job_array:schedule_next_task(1))
           end}
      ]}}.

get_schedulable_count_test_() ->
    {"get_schedulable_count tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Schedulable count with unlimited",
           fun() ->
               %% Create array job with unlimited concurrent
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 9, step = 1,
                                  indices = undefined, max_concurrent = unlimited},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               ?assertEqual(10, flurm_job_array:get_schedulable_count(1))
           end},

          {"Schedulable count respects limit",
           fun() ->
               %% Create array job with max 3 concurrent
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 9, step = 1,
                                  indices = undefined, max_concurrent = 3},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set 1 task to running
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = running}),

               %% 3 - 1 = 2 slots available, 9 pending
               ?assertEqual(2, flurm_job_array:get_schedulable_count(1))
           end},

          {"Schedulable count when fewer pending than slots",
           fun() ->
               %% Create array job with 2 tasks and max 5 concurrent
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 1, step = 1,
                                  indices = undefined, max_concurrent = 5},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               ?assertEqual(2, flurm_job_array:get_schedulable_count(1))
           end},

          {"Schedulable count is 0 for non-existent array",
           fun() ->
               ?assertEqual(0, flurm_job_array:get_schedulable_count(999))
           end}
      ]}}.

get_schedulable_tasks_test_() ->
    {"get_schedulable_tasks tests",
     {foreach,
      fun setup_ets/0,
      fun cleanup_ets/1,
      [
          {"Get schedulable tasks respects limit",
           fun() ->
               %% Create array job with max 3 concurrent
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 9, step = 1,
                                  indices = undefined, max_concurrent = 3},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               Tasks = flurm_job_array:get_schedulable_tasks(1),
               ?assertEqual(3, length(Tasks)),
               %% Should be sorted by task_id
               TaskIds = [T#array_task.task_id || T <- Tasks],
               ?assertEqual([0,1,2], TaskIds)
           end},

          {"Get schedulable tasks with some running",
           fun() ->
               %% Create array job with max 5 concurrent
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 9, step = 1,
                                  indices = undefined, max_concurrent = 5},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set 2 tasks to running
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = running}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = running}),

               Tasks = flurm_job_array:get_schedulable_tasks(1),
               ?assertEqual(3, length(Tasks)),
               TaskIds = [T#array_task.task_id || T <- Tasks],
               ?assertEqual([2,3,4], TaskIds)
           end},

          {"Get empty list when at limit",
           fun() ->
               %% Create array job with max 2 concurrent
               State = #state{next_array_id = 1},
               BaseJob = make_base_job(),
               Spec = #array_spec{start_idx = 0, end_idx = 9, step = 1,
                                  indices = undefined, max_concurrent = 2},
               {reply, {ok, 1}, _} =
                   flurm_job_array:handle_call({create_array_job, BaseJob, Spec}, {self(), ref}, State),

               %% Set 2 tasks to running (at limit)
               [Task0] = ets:lookup(flurm_array_tasks, {1, 0}),
               [Task1] = ets:lookup(flurm_array_tasks, {1, 1}),
               ets:insert(flurm_array_tasks, Task0#array_task{state = running}),
               ets:insert(flurm_array_tasks, Task1#array_task{state = running}),

               ?assertEqual([], flurm_job_array:get_schedulable_tasks(1))
           end}
      ]}}.

%%====================================================================
%% Round-trip Parse/Format Tests
%%====================================================================

parse_format_roundtrip_test_() ->
    {"Parse and format round-trip tests", [
        {"Simple range round-trip",
         fun() ->
             Original = <<"0-100">>,
             {ok, Spec} = flurm_job_array:parse_array_spec(Original),
             Formatted = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(Original, Formatted)
         end},

        {"Range with step round-trip",
         fun() ->
             Original = <<"0-100:5">>,
             {ok, Spec} = flurm_job_array:parse_array_spec(Original),
             Formatted = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(Original, Formatted)
         end},

        {"Range with concurrent round-trip",
         fun() ->
             Original = <<"0-50%10">>,
             {ok, Spec} = flurm_job_array:parse_array_spec(Original),
             Formatted = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(Original, Formatted)
         end},

        {"Single value round-trip",
         fun() ->
             Original = <<"42">>,
             {ok, Spec} = flurm_job_array:parse_array_spec(Original),
             Formatted = flurm_job_array:format_array_spec(Spec),
             ?assertEqual(Original, Formatted)
         end}
    ]}.
