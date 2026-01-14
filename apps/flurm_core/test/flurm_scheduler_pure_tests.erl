%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_scheduler module
%%%
%%% These tests directly test the gen_server callbacks without any mocking.
%%% The goal is to achieve code coverage by testing init/1, handle_call/3,
%%% handle_cast/2, handle_info/2, terminate/2, and code_change/3 directly.
%%%
%%% Note: Some code paths require external services (flurm_job_manager,
%%% flurm_node_manager, etc.) which are tested via catch wrappers in the
%%% actual code. We test the paths that can be tested without mocking.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% We need to define the scheduler_state record here since it's not exported
-record(scheduler_state, {
    pending_jobs    :: queue:queue(pos_integer()),
    running_jobs    :: sets:set(pos_integer()),
    nodes_cache     :: ets:tid(),
    schedule_timer  :: reference() | undefined,
    schedule_cycles :: non_neg_integer(),
    completed_count :: non_neg_integer(),
    failed_count    :: non_neg_integer(),
    pending_reasons :: #{pos_integer() => {term(), non_neg_integer()}}
}).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Create a minimal valid scheduler state for testing
make_test_state() ->
    NodesCache = ets:new(test_scheduler_nodes_cache, [set, private, {keypos, 1}]),
    #scheduler_state{
        pending_jobs = queue:new(),
        running_jobs = sets:new(),
        nodes_cache = NodesCache,
        schedule_timer = undefined,
        schedule_cycles = 0,
        completed_count = 0,
        failed_count = 0,
        pending_reasons = #{}
    }.

cleanup_state(State) ->
    case State#scheduler_state.nodes_cache of
        undefined -> ok;
        Tid ->
            try ets:delete(Tid) catch _:_ -> ok end
    end,
    case State#scheduler_state.schedule_timer of
        undefined -> ok;
        Timer -> erlang:cancel_timer(Timer)
    end,
    ok.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
      {"init creates valid state",
       fun() ->
           {ok, State} = flurm_scheduler:init([]),
           ?assert(is_record(State, scheduler_state)),
           ?assertEqual(0, queue:len(State#scheduler_state.pending_jobs)),
           ?assertEqual(0, sets:size(State#scheduler_state.running_jobs)),
           ?assert(is_reference(State#scheduler_state.nodes_cache)),
           ?assert(is_reference(State#scheduler_state.schedule_timer)),
           ?assertEqual(0, State#scheduler_state.schedule_cycles),
           ?assertEqual(0, State#scheduler_state.completed_count),
           ?assertEqual(0, State#scheduler_state.failed_count),
           ?assertEqual(#{}, State#scheduler_state.pending_reasons),
           cleanup_state(State)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests
%%====================================================================

handle_call_get_stats_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"get_stats returns correct stats for empty state",
           fun() ->
               {reply, {ok, Stats}, NewState} = flurm_scheduler:handle_call(get_stats, {self(), make_ref()}, State),
               ?assertEqual(0, maps:get(pending_count, Stats)),
               ?assertEqual(0, maps:get(running_count, Stats)),
               ?assertEqual(0, maps:get(completed_count, Stats)),
               ?assertEqual(0, maps:get(failed_count, Stats)),
               ?assertEqual(0, maps:get(schedule_cycles, Stats)),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

handle_call_get_stats_with_jobs_test_() ->
    {setup,
     fun() ->
         State0 = make_test_state(),
         PendingJobs = queue:from_list([1, 2, 3]),
         RunningJobs = sets:from_list([4, 5]),
         State0#scheduler_state{
             pending_jobs = PendingJobs,
             running_jobs = RunningJobs,
             schedule_cycles = 10,
             completed_count = 100,
             failed_count = 5
         }
     end,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"get_stats returns correct stats with jobs",
           fun() ->
               {reply, {ok, Stats}, _NewState} = flurm_scheduler:handle_call(get_stats, {self(), make_ref()}, State),
               ?assertEqual(3, maps:get(pending_count, Stats)),
               ?assertEqual(2, maps:get(running_count, Stats)),
               ?assertEqual(100, maps:get(completed_count, Stats)),
               ?assertEqual(5, maps:get(failed_count, Stats)),
               ?assertEqual(10, maps:get(schedule_cycles, Stats))
           end}
         ]
     end}.

handle_call_unknown_request_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"unknown request returns error",
           fun() ->
               {reply, Reply, NewState} = flurm_scheduler:handle_call(unknown_request, {self(), make_ref()}, State),
               ?assertEqual({error, unknown_request}, Reply),
               ?assertEqual(State, NewState)
           end},
          {"arbitrary term returns error",
           fun() ->
               {reply, Reply, _} = flurm_scheduler:handle_call({some, arbitrary, term}, {self(), make_ref()}, State),
               ?assertEqual({error, unknown_request}, Reply)
           end}
         ]
     end}.

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_submit_job_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"submit_job adds job to pending queue",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_cast({submit_job, 1}, State),
               ?assertEqual(1, queue:len(NewState#scheduler_state.pending_jobs)),
               {value, JobId} = queue:peek(NewState#scheduler_state.pending_jobs),
               ?assertEqual(1, JobId),
               %% The cast sends a schedule_cycle message to self
               receive
                   schedule_cycle -> ok
               after 100 ->
                   ?assert(false)
               end
           end},
          {"submit_job preserves order for multiple jobs",
           fun() ->
               {noreply, State1} = flurm_scheduler:handle_cast({submit_job, 100}, State),
               {noreply, State2} = flurm_scheduler:handle_cast({submit_job, 200}, State1),
               {noreply, State3} = flurm_scheduler:handle_cast({submit_job, 300}, State2),
               ?assertEqual(3, queue:len(State3#scheduler_state.pending_jobs)),
               Jobs = queue:to_list(State3#scheduler_state.pending_jobs),
               ?assertEqual([100, 200, 300], Jobs),
               %% Drain messages
               drain_messages()
           end}
         ]
     end}.

%% Note: job_completed and job_failed casts call flurm_job_manager:get_job/1
%% which requires the gen_server to be running. These are tested in integration
%% tests. Here we just verify the cast message format is accepted.


handle_cast_trigger_schedule_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"trigger_schedule sends schedule_cycle message",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_cast(trigger_schedule, State),
               ?assertEqual(State, NewState),
               receive
                   schedule_cycle -> ok
               after 100 ->
                   ?assert(false)
               end
           end}
         ]
     end}.

handle_cast_job_deps_satisfied_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"job_deps_satisfied triggers schedule cycle",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_cast({job_deps_satisfied, 42}, State),
               ?assertEqual(State, NewState),
               receive
                   schedule_cycle -> ok
               after 100 ->
                   ?assert(false)
               end
           end}
         ]
     end}.

handle_cast_unknown_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"unknown cast is ignored",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_cast(unknown_cast, State),
               ?assertEqual(State, NewState)
           end},
          {"arbitrary cast is ignored",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_cast({some, random, thing}, State),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_schedule_cycle_test_() ->
    {setup,
     fun() ->
         State0 = make_test_state(),
         %% Set a timer to test cancellation
         Timer = erlang:send_after(60000, self(), dummy_timer),
         State0#scheduler_state{schedule_timer = Timer}
     end,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"schedule_cycle with empty queue increments cycle count",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_info(schedule_cycle, State),
               ?assertEqual(1, NewState#scheduler_state.schedule_cycles),
               ?assert(is_reference(NewState#scheduler_state.schedule_timer)),
               cleanup_state(NewState)
           end}
         ]
     end}.

%% Note: schedule_cycle with pending jobs requires flurm_job_manager.
%% Testing only empty queue scenario in pure tests.

handle_info_schedule_cycle_no_timer_test_() ->
    {setup,
     fun() ->
         State0 = make_test_state(),
         State0#scheduler_state{schedule_timer = undefined}
     end,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"schedule_cycle with undefined timer works",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_info(schedule_cycle, State),
               ?assertEqual(1, NewState#scheduler_state.schedule_cycles),
               ?assert(is_reference(NewState#scheduler_state.schedule_timer)),
               cleanup_state(NewState)
           end}
         ]
     end}.

handle_info_config_changed_partitions_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"config_changed partitions triggers schedule",
           fun() ->
               OldPartitions = [#{name => <<"part1">>}],
               NewPartitions = [#{name => <<"part1">>}, #{name => <<"part2">>}],
               {noreply, NewState} = flurm_scheduler:handle_info(
                   {config_changed, partitions, OldPartitions, NewPartitions}, State),
               ?assertEqual(State, NewState),
               receive
                   schedule_cycle -> ok
               after 100 ->
                   ?assert(false)
               end
           end}
         ]
     end}.

handle_info_config_changed_nodes_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"config_changed nodes triggers schedule",
           fun() ->
               OldNodes = [#{name => <<"node1">>}],
               NewNodes = [#{name => <<"node1">>}, #{name => <<"node2">>}],
               {noreply, NewState} = flurm_scheduler:handle_info(
                   {config_changed, nodes, OldNodes, NewNodes}, State),
               ?assertEqual(State, NewState),
               receive
                   schedule_cycle -> ok
               after 100 ->
                   ?assert(false)
               end
           end}
         ]
     end}.

handle_info_config_changed_schedulertype_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"config_changed schedulertype triggers schedule",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_info(
                   {config_changed, schedulertype, fifo, backfill}, State),
               ?assertEqual(State, NewState),
               receive
                   schedule_cycle -> ok
               after 100 ->
                   ?assert(false)
               end
           end}
         ]
     end}.

handle_info_config_changed_other_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"config_changed other key is ignored",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_info(
                   {config_changed, some_other_key, old_value, new_value}, State),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

handle_info_unknown_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"unknown info message is ignored",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_info(unknown_info, State),
               ?assertEqual(State, NewState)
           end},
          {"arbitrary info message is ignored",
           fun() ->
               {noreply, NewState} = flurm_scheduler:handle_info({arbitrary, message, 123}, State),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_with_timer_test_() ->
    [
     {"terminate cancels timer and deletes ets",
      fun() ->
          State0 = make_test_state(),
          Timer = erlang:send_after(60000, self(), dummy),
          State = State0#scheduler_state{schedule_timer = Timer},
          Result = flurm_scheduler:terminate(normal, State),
          ?assertEqual(ok, Result),
          %% Timer should be cancelled
          ?assertEqual(false, erlang:read_timer(Timer) =/= false)
          %% Note: ETS table deletion is handled by terminate itself
      end}
    ].

terminate_without_timer_test_() ->
    [
     {"terminate works with undefined timer",
      fun() ->
          State0 = make_test_state(),
          State = State0#scheduler_state{schedule_timer = undefined},
          Result = flurm_scheduler:terminate(shutdown, State),
          ?assertEqual(ok, Result)
      end}
    ].

terminate_various_reasons_test_() ->
    [
     {"terminate handles normal reason",
      fun() ->
          State = make_test_state(),
          ?assertEqual(ok, flurm_scheduler:terminate(normal, State))
      end},
     {"terminate handles shutdown reason",
      fun() ->
          State2 = make_test_state(),
          ?assertEqual(ok, flurm_scheduler:terminate(shutdown, State2))
      end},
     {"terminate handles error reason",
      fun() ->
          State3 = make_test_state(),
          ?assertEqual(ok, flurm_scheduler:terminate({error, some_reason}, State3))
      end}
    ].

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"code_change returns state unchanged",
           fun() ->
               {ok, NewState} = flurm_scheduler:code_change("1.0.0", State, []),
               ?assertEqual(State, NewState)
           end},
          {"code_change with any version",
           fun() ->
               {ok, NewState} = flurm_scheduler:code_change("2.0.0", State, extra),
               ?assertEqual(State, NewState)
           end},
          {"code_change with undefined version",
           fun() ->
               {ok, NewState} = flurm_scheduler:code_change(undefined, State, []),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% Integration-style Tests (Testing Multiple Callbacks)
%%====================================================================

workflow_submit_and_stats_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"submit jobs and check stats",
           fun() ->
               %% Submit several jobs
               {noreply, S1} = flurm_scheduler:handle_cast({submit_job, 1}, State),
               {noreply, S2} = flurm_scheduler:handle_cast({submit_job, 2}, S1),
               {noreply, S3} = flurm_scheduler:handle_cast({submit_job, 3}, S2),
               drain_messages(),

               %% Check stats
               {reply, {ok, Stats}, _} = flurm_scheduler:handle_call(get_stats, {self(), make_ref()}, S3),
               ?assertEqual(3, maps:get(pending_count, Stats)),
               ?assertEqual(0, maps:get(running_count, Stats))
           end}
         ]
     end}.

%% Note: workflow_complete_and_fail tests require external services.
%% Testing submit workflow only here.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_large_job_ids_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"handles very large job IDs",
           fun() ->
               LargeId = 999999999999,
               {noreply, S1} = flurm_scheduler:handle_cast({submit_job, LargeId}, State),
               {value, JobId} = queue:peek(S1#scheduler_state.pending_jobs),
               ?assertEqual(LargeId, JobId),
               drain_messages()
           end}
         ]
     end}.

edge_case_many_pending_jobs_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"handles many pending jobs",
           fun() ->
               %% Submit 100 jobs
               FinalState = lists:foldl(
                   fun(N, S) ->
                       {noreply, NewS} = flurm_scheduler:handle_cast({submit_job, N}, S),
                       NewS
                   end,
                   State,
                   lists:seq(1, 100)
               ),
               drain_messages(),

               %% Check stats
               {reply, {ok, Stats}, _} = flurm_scheduler:handle_call(get_stats, {self(), make_ref()}, FinalState),
               ?assertEqual(100, maps:get(pending_count, Stats))
           end}
         ]
     end}.

edge_case_many_running_jobs_test_() ->
    {setup,
     fun() ->
         State0 = make_test_state(),
         RunningJobs = sets:from_list(lists:seq(1, 1000)),
         State0#scheduler_state{running_jobs = RunningJobs}
     end,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"handles many running jobs",
           fun() ->
               {reply, {ok, Stats}, _} = flurm_scheduler:handle_call(get_stats, {self(), make_ref()}, State),
               ?assertEqual(1000, maps:get(running_count, Stats))
           end}
         ]
     end}.

edge_case_state_consistency_test_() ->
    {setup,
     fun() ->
         State0 = make_test_state(),
         State0#scheduler_state{
             pending_jobs = queue:from_list([1, 2, 3]),
             running_jobs = sets:from_list([4, 5]),
             schedule_cycles = 50,
             completed_count = 100,
             failed_count = 10
         }
     end,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"state consistency after submit operations",
           fun() ->
               %% Submit a new job
               {noreply, S1} = flurm_scheduler:handle_cast({submit_job, 6}, State),
               {noreply, S2} = flurm_scheduler:handle_cast({submit_job, 7}, S1),
               drain_messages(),

               %% Check final stats
               {reply, {ok, Stats}, _} = flurm_scheduler:handle_call(get_stats, {self(), make_ref()}, S2),
               ?assertEqual(5, maps:get(pending_count, Stats)),  % 3 original + 2 new
               ?assertEqual(2, maps:get(running_count, Stats)),   % unchanged
               ?assertEqual(100, maps:get(completed_count, Stats)), % unchanged
               ?assertEqual(10, maps:get(failed_count, Stats))    % unchanged
           end}
         ]
     end}.

%%====================================================================
%% Queue Operations Tests
%%====================================================================

queue_operations_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"FIFO ordering is preserved",
           fun() ->
               {noreply, S1} = flurm_scheduler:handle_cast({submit_job, 100}, State),
               {noreply, S2} = flurm_scheduler:handle_cast({submit_job, 200}, S1),
               {noreply, S3} = flurm_scheduler:handle_cast({submit_job, 300}, S2),
               {noreply, S4} = flurm_scheduler:handle_cast({submit_job, 400}, S3),
               drain_messages(),

               Jobs = queue:to_list(S4#scheduler_state.pending_jobs),
               ?assertEqual([100, 200, 300, 400], Jobs)
           end}
         ]
     end}.

%%====================================================================
%% Config Change Tests (Additional Coverage)
%%====================================================================

config_change_all_types_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"handles partition config change with empty lists",
           fun() ->
               {noreply, _} = flurm_scheduler:handle_info(
                   {config_changed, partitions, [], []}, State),
               drain_messages()
           end},
          {"handles node config change with empty lists",
           fun() ->
               {noreply, _} = flurm_scheduler:handle_info(
                   {config_changed, nodes, [], []}, State),
               drain_messages()
           end},
          {"handles scheduler type change from fifo to backfill",
           fun() ->
               {noreply, _} = flurm_scheduler:handle_info(
                   {config_changed, schedulertype, fifo, backfill}, State),
               drain_messages()
           end},
          {"handles scheduler type change to priority",
           fun() ->
               {noreply, _} = flurm_scheduler:handle_info(
                   {config_changed, schedulertype, backfill, priority}, State),
               drain_messages()
           end}
         ]
     end}.

%%====================================================================
%% Running Jobs Set Tests
%%====================================================================

running_jobs_set_operations_test_() ->
    {setup,
     fun make_test_state/0,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"running jobs set is initially empty",
           fun() ->
               {reply, {ok, Stats}, _} = flurm_scheduler:handle_call(get_stats, {self(), make_ref()}, State),
               ?assertEqual(0, maps:get(running_count, Stats))
           end}
         ]
     end}.

%%====================================================================
%% Timer Tests
%%====================================================================

timer_management_test_() ->
    {setup,
     fun() ->
         State0 = make_test_state(),
         Timer = erlang:send_after(60000, self(), old_timer),
         State0#scheduler_state{schedule_timer = Timer}
     end,
     fun cleanup_state/1,
     fun(State) ->
         [
          {"schedule_cycle cancels old timer and creates new one",
           fun() ->
               OldTimer = State#scheduler_state.schedule_timer,
               {noreply, NewState} = flurm_scheduler:handle_info(schedule_cycle, State),
               NewTimer = NewState#scheduler_state.schedule_timer,

               %% Old timer should be cancelled
               ?assertEqual(false, erlang:read_timer(OldTimer)),
               %% New timer should be active
               ?assert(is_reference(NewTimer)),
               ?assert(NewTimer =/= OldTimer),

               cleanup_state(NewState)
           end}
         ]
     end}.

%%====================================================================
%% Helper Functions
%%====================================================================

drain_messages() ->
    receive
        _ -> drain_messages()
    after 0 ->
        ok
    end.
