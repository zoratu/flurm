%%%-------------------------------------------------------------------
%%% @doc Pure unit tests for flurm_job_registry module
%%%
%%% These tests directly test the gen_server callbacks without any mocking.
%%% Tests focus on testing init/1, handle_call/3, handle_cast/2, handle_info/2,
%%% terminate/2, and code_change/3 directly with controlled ETS tables and state.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_registry_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% We need to define the state record to match flurm_job_registry
-record(state, {
    monitors = #{} :: #{reference() => job_id()}
}).

%% ETS table names (must match flurm_job_registry)
-define(JOBS_BY_ID, flurm_jobs_by_id).
-define(JOBS_BY_USER, flurm_jobs_by_user).
-define(JOBS_BY_STATE, flurm_jobs_by_state).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup function that creates ETS tables
setup_ets() ->
    %% Delete any existing tables first
    catch ets:delete(?JOBS_BY_ID),
    catch ets:delete(?JOBS_BY_USER),
    catch ets:delete(?JOBS_BY_STATE),

    %% Create ETS tables matching what init/1 creates
    ets:new(?JOBS_BY_ID, [
        named_table,
        set,
        public,
        {keypos, #job_entry.job_id},
        {read_concurrency, true}
    ]),
    ets:new(?JOBS_BY_USER, [
        named_table,
        bag,
        public,
        {read_concurrency, true}
    ]),
    ets:new(?JOBS_BY_STATE, [
        named_table,
        bag,
        public,
        {read_concurrency, true}
    ]),
    #state{monitors = #{}}.

%% Cleanup function
cleanup_ets(_State) ->
    catch ets:delete(?JOBS_BY_ID),
    catch ets:delete(?JOBS_BY_USER),
    catch ets:delete(?JOBS_BY_STATE),
    ok.

%% Create a test state with some job entries
setup_ets_with_entries() ->
    State = setup_ets(),

    %% Insert test entries directly into ETS (bypassing handle_call)
    Entry1 = #job_entry{
        job_id = 1,
        pid = self(),
        user_id = 1000,
        state = pending,
        partition = <<"default">>,
        submit_time = erlang:timestamp()
    },
    Entry2 = #job_entry{
        job_id = 2,
        pid = self(),
        user_id = 1000,
        state = running,
        partition = <<"compute">>,
        submit_time = erlang:timestamp()
    },
    Entry3 = #job_entry{
        job_id = 3,
        pid = self(),
        user_id = 2000,
        state = pending,
        partition = <<"default">>,
        submit_time = erlang:timestamp()
    },

    ets:insert(?JOBS_BY_ID, Entry1),
    ets:insert(?JOBS_BY_ID, Entry2),
    ets:insert(?JOBS_BY_ID, Entry3),

    ets:insert(?JOBS_BY_USER, {1000, 1, self()}),
    ets:insert(?JOBS_BY_USER, {1000, 2, self()}),
    ets:insert(?JOBS_BY_USER, {2000, 3, self()}),

    ets:insert(?JOBS_BY_STATE, {pending, 1, self()}),
    ets:insert(?JOBS_BY_STATE, {running, 2, self()}),
    ets:insert(?JOBS_BY_STATE, {pending, 3, self()}),

    State.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {setup,
     fun() ->
         %% Clean up any existing tables
         catch ets:delete(?JOBS_BY_ID),
         catch ets:delete(?JOBS_BY_USER),
         catch ets:delete(?JOBS_BY_STATE),
         ok
     end,
     fun(_) ->
         catch ets:delete(?JOBS_BY_ID),
         catch ets:delete(?JOBS_BY_USER),
         catch ets:delete(?JOBS_BY_STATE),
         ok
     end,
     [
      {"init creates all ETS tables",
       fun() ->
           {ok, State} = flurm_job_registry:init([]),
           ?assert(is_record(State, state)),
           ?assertEqual(#{}, State#state.monitors),

           %% Verify tables exist
           ?assertMatch([_|_], ets:info(?JOBS_BY_ID)),
           ?assertMatch([_|_], ets:info(?JOBS_BY_USER)),
           ?assertMatch([_|_], ets:info(?JOBS_BY_STATE)),

           %% Verify table types
           ?assertEqual(set, proplists:get_value(type, ets:info(?JOBS_BY_ID))),
           ?assertEqual(bag, proplists:get_value(type, ets:info(?JOBS_BY_USER))),
           ?assertEqual(bag, proplists:get_value(type, ets:info(?JOBS_BY_STATE))),

           %% Cleanup
           catch ets:delete(?JOBS_BY_ID),
           catch ets:delete(?JOBS_BY_USER),
           catch ets:delete(?JOBS_BY_STATE)
       end}
     ]}.

%%====================================================================
%% handle_call - unregister Tests
%%====================================================================

handle_call_unregister_existing_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"unregister existing job removes from all tables",
           fun() ->
               %% Verify job exists first
               ?assertEqual(1, length(ets:lookup(?JOBS_BY_ID, 1))),

               {reply, ok, NewState} =
                   flurm_job_registry:handle_call({unregister, 1}, {self(), make_ref()}, State),

               %% Verify job is removed from primary table
               ?assertEqual([], ets:lookup(?JOBS_BY_ID, 1)),

               %% State should be unchanged (no monitor to remove)
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

handle_call_unregister_nonexistent_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"unregister non-existent job returns ok",
           fun() ->
               {reply, ok, NewState} =
                   flurm_job_registry:handle_call({unregister, 9999}, {self(), make_ref()}, State),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% handle_call - update_state Tests
%%====================================================================

handle_call_update_state_existing_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"update_state changes job state in primary table",
           fun() ->
               %% Verify initial state
               [Entry1] = ets:lookup(?JOBS_BY_ID, 1),
               ?assertEqual(pending, Entry1#job_entry.state),

               {reply, ok, NewState} =
                   flurm_job_registry:handle_call({update_state, 1, running}, {self(), make_ref()}, State),

               %% Verify state changed
               [Entry2] = ets:lookup(?JOBS_BY_ID, 1),
               ?assertEqual(running, Entry2#job_entry.state),

               %% State record should be unchanged
               ?assertEqual(State, NewState)
           end},
          {"update_state updates state index",
           fun() ->
               %% Initial state - job 2 is running
               [Entry1] = ets:lookup(?JOBS_BY_ID, 2),
               ?assertEqual(running, Entry1#job_entry.state),

               %% Update to completed
               {reply, ok, _} =
                   flurm_job_registry:handle_call({update_state, 2, completed}, {self(), make_ref()}, State),

               %% Verify old state removed, new state added
               RunningJobs = ets:lookup(?JOBS_BY_STATE, running),
               CompletedJobs = ets:lookup(?JOBS_BY_STATE, completed),

               %% Job 2 should no longer be in running
               RunningJobIds = [Id || {_, Id, _} <- RunningJobs],
               ?assertNot(lists:member(2, RunningJobIds)),

               %% Job 2 should be in completed
               CompletedJobIds = [Id || {_, Id, _} <- CompletedJobs],
               ?assert(lists:member(2, CompletedJobIds))
           end}
         ]
     end}.

handle_call_update_state_not_found_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"update_state on non-existent job returns not_found",
           fun() ->
               {reply, Result, NewState} =
                   flurm_job_registry:handle_call({update_state, 9999, running}, {self(), make_ref()}, State),
               ?assertEqual({error, not_found}, Result),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% handle_call - unknown_request Tests
%%====================================================================

handle_call_unknown_request_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"unknown request returns error",
           fun() ->
               {reply, Result, NewState} =
                   flurm_job_registry:handle_call(unknown_request, {self(), make_ref()}, State),
               ?assertEqual({error, unknown_request}, Result),
               ?assertEqual(State, NewState)
           end},
          {"arbitrary tuple request returns error",
           fun() ->
               {reply, Result, _} =
                   flurm_job_registry:handle_call({some, random, request}, {self(), make_ref()}, State),
               ?assertEqual({error, unknown_request}, Result)
           end}
         ]
     end}.

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_unknown_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"unknown cast is ignored",
           fun() ->
               {noreply, NewState} = flurm_job_registry:handle_cast(unknown_cast, State),
               ?assertEqual(State, NewState)
           end},
          {"arbitrary cast is ignored",
           fun() ->
               {noreply, NewState} = flurm_job_registry:handle_cast({some, message}, State),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% handle_info - DOWN message Tests
%%====================================================================

handle_info_down_with_monitor_test_() ->
    {setup,
     fun() ->
         State = setup_ets_with_entries(),
         %% Add a monitor reference to state
         MonRef = make_ref(),
         State#state{monitors = #{MonRef => 1}}
     end,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"DOWN message for monitored job removes job",
           fun() ->
               [MonRef] = maps:keys(State#state.monitors),

               {noreply, NewState} =
                   flurm_job_registry:handle_info({'DOWN', MonRef, process, self(), normal}, State),

               %% Job should be removed from primary table
               ?assertEqual([], ets:lookup(?JOBS_BY_ID, 1)),

               %% Monitor should be removed from state
               ?assertEqual(#{}, NewState#state.monitors)
           end}
         ]
     end}.

handle_info_down_unknown_monitor_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"DOWN message for unknown monitor is ignored",
           fun() ->
               UnknownRef = make_ref(),

               {noreply, NewState} =
                   flurm_job_registry:handle_info({'DOWN', UnknownRef, process, self(), normal}, State),

               ?assertEqual(State, NewState)
           end}
         ]
     end}.

handle_info_unknown_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"unknown info message is ignored",
           fun() ->
               {noreply, NewState} = flurm_job_registry:handle_info(unknown_info, State),
               ?assertEqual(State, NewState)
           end},
          {"arbitrary info message is ignored",
           fun() ->
               {noreply, NewState} = flurm_job_registry:handle_info({random, message, 123}, State),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"terminate returns ok for normal reason",
           fun() ->
               Result = flurm_job_registry:terminate(normal, State),
               ?assertEqual(ok, Result)
           end},
          {"terminate returns ok for shutdown reason",
           fun() ->
               Result = flurm_job_registry:terminate(shutdown, State),
               ?assertEqual(ok, Result)
           end},
          {"terminate returns ok for error reason",
           fun() ->
               Result = flurm_job_registry:terminate({error, some_error}, State),
               ?assertEqual(ok, Result)
           end}
         ]
     end}.

terminate_with_monitors_test_() ->
    {setup,
     fun() ->
         State = setup_ets(),
         MonRef1 = make_ref(),
         MonRef2 = make_ref(),
         State#state{monitors = #{MonRef1 => 1, MonRef2 => 2}}
     end,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"terminate with monitors returns ok",
           fun() ->
               Result = flurm_job_registry:terminate(normal, State),
               ?assertEqual(ok, Result)
           end}
         ]
     end}.

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"code_change returns state unchanged",
           fun() ->
               {ok, NewState} = flurm_job_registry:code_change("1.0.0", State, []),
               ?assertEqual(State, NewState)
           end},
          {"code_change with any version",
           fun() ->
               {ok, NewState} = flurm_job_registry:code_change("2.0.0", State, extra),
               ?assertEqual(State, NewState)
           end},
          {"code_change with undefined version",
           fun() ->
               {ok, NewState} = flurm_job_registry:code_change(undefined, State, []),
               ?assertEqual(State, NewState)
           end}
         ]
     end}.

%%====================================================================
%% Direct ETS Function Tests - lookup_job
%%====================================================================

lookup_job_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"lookup_job finds existing job",
           fun() ->
               {ok, Pid} = flurm_job_registry:lookup_job(1),
               ?assert(is_pid(Pid))
           end},
          {"lookup_job returns not_found for missing job",
           fun() ->
               Result = flurm_job_registry:lookup_job(9999),
               ?assertEqual({error, not_found}, Result)
           end}
         ]
     end}.

%%====================================================================
%% Direct ETS Function Tests - get_job_entry
%%====================================================================

get_job_entry_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"get_job_entry returns full entry",
           fun() ->
               {ok, Entry} = flurm_job_registry:get_job_entry(1),
               ?assertEqual(1, Entry#job_entry.job_id),
               ?assertEqual(1000, Entry#job_entry.user_id),
               ?assertEqual(pending, Entry#job_entry.state),
               ?assertEqual(<<"default">>, Entry#job_entry.partition)
           end},
          {"get_job_entry returns not_found for missing job",
           fun() ->
               Result = flurm_job_registry:get_job_entry(9999),
               ?assertEqual({error, not_found}, Result)
           end}
         ]
     end}.

%%====================================================================
%% Direct ETS Function Tests - list_jobs
%%====================================================================

list_jobs_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"list_jobs returns all jobs",
           fun() ->
               Jobs = flurm_job_registry:list_jobs(),
               ?assertEqual(3, length(Jobs)),
               JobIds = [Id || {Id, _} <- Jobs],
               ?assert(lists:member(1, JobIds)),
               ?assert(lists:member(2, JobIds)),
               ?assert(lists:member(3, JobIds))
           end}
         ]
     end}.

list_jobs_empty_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"list_jobs returns empty list when no jobs",
           fun() ->
               Jobs = flurm_job_registry:list_jobs(),
               ?assertEqual([], Jobs)
           end}
         ]
     end}.

%%====================================================================
%% Direct ETS Function Tests - list_jobs_by_state
%%====================================================================

list_jobs_by_state_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"list_jobs_by_state returns pending jobs",
           fun() ->
               Jobs = flurm_job_registry:list_jobs_by_state(pending),
               ?assertEqual(2, length(Jobs)),
               JobIds = [Id || {Id, _} <- Jobs],
               ?assert(lists:member(1, JobIds)),
               ?assert(lists:member(3, JobIds))
           end},
          {"list_jobs_by_state returns running jobs",
           fun() ->
               Jobs = flurm_job_registry:list_jobs_by_state(running),
               ?assertEqual(1, length(Jobs)),
               [{JobId, _}] = Jobs,
               ?assertEqual(2, JobId)
           end},
          {"list_jobs_by_state returns empty for unused state",
           fun() ->
               Jobs = flurm_job_registry:list_jobs_by_state(completed),
               ?assertEqual([], Jobs)
           end}
         ]
     end}.

%%====================================================================
%% Direct ETS Function Tests - list_jobs_by_user
%%====================================================================

list_jobs_by_user_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"list_jobs_by_user returns user's jobs",
           fun() ->
               Jobs = flurm_job_registry:list_jobs_by_user(1000),
               ?assertEqual(2, length(Jobs)),
               JobIds = [Id || {Id, _} <- Jobs],
               ?assert(lists:member(1, JobIds)),
               ?assert(lists:member(2, JobIds))
           end},
          {"list_jobs_by_user returns single job for user",
           fun() ->
               Jobs = flurm_job_registry:list_jobs_by_user(2000),
               ?assertEqual(1, length(Jobs)),
               [{JobId, _}] = Jobs,
               ?assertEqual(3, JobId)
           end},
          {"list_jobs_by_user returns empty for unknown user",
           fun() ->
               Jobs = flurm_job_registry:list_jobs_by_user(9999),
               ?assertEqual([], Jobs)
           end}
         ]
     end}.

%%====================================================================
%% Direct ETS Function Tests - count_by_state
%%====================================================================

count_by_state_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"count_by_state returns correct counts",
           fun() ->
               Counts = flurm_job_registry:count_by_state(),
               ?assertEqual(2, maps:get(pending, Counts)),
               ?assertEqual(1, maps:get(running, Counts)),
               ?assertEqual(0, maps:get(completed, Counts)),
               ?assertEqual(0, maps:get(failed, Counts))
           end}
         ]
     end}.

count_by_state_empty_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"count_by_state returns all zeros when empty",
           fun() ->
               Counts = flurm_job_registry:count_by_state(),
               ?assertEqual(0, maps:get(pending, Counts)),
               ?assertEqual(0, maps:get(running, Counts)),
               ?assertEqual(0, maps:get(completed, Counts))
           end}
         ]
     end}.

%%====================================================================
%% Direct ETS Function Tests - count_by_user
%%====================================================================

count_by_user_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"count_by_user returns correct counts",
           fun() ->
               Counts = flurm_job_registry:count_by_user(),
               ?assertEqual(2, maps:get(1000, Counts)),
               ?assertEqual(1, maps:get(2000, Counts))
           end}
         ]
     end}.

count_by_user_empty_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"count_by_user returns empty map when no jobs",
           fun() ->
               Counts = flurm_job_registry:count_by_user(),
               ?assertEqual(#{}, Counts)
           end}
         ]
     end}.

%%====================================================================
%% State Record Tests
%%====================================================================

state_record_test_() ->
    [
     {"empty state has empty monitors map",
      fun() ->
          State = #state{},
          ?assertEqual(#{}, State#state.monitors)
      end},
     {"state with monitors",
      fun() ->
          MonRef = make_ref(),
          State = #state{monitors = #{MonRef => 1}},
          ?assertEqual(1, maps:get(MonRef, State#state.monitors))
      end}
    ].

%%====================================================================
%% Job Entry Record Tests
%%====================================================================

job_entry_record_test_() ->
    [
     {"job_entry record creation",
      fun() ->
          Entry = #job_entry{
              job_id = 1,
              pid = self(),
              user_id = 1000,
              state = pending,
              partition = <<"default">>,
              submit_time = erlang:timestamp()
          },
          ?assertEqual(1, Entry#job_entry.job_id),
          ?assertEqual(1000, Entry#job_entry.user_id),
          ?assertEqual(pending, Entry#job_entry.state),
          ?assertEqual(<<"default">>, Entry#job_entry.partition),
          ?assert(is_pid(Entry#job_entry.pid)),
          ?assert(is_tuple(Entry#job_entry.submit_time))
      end}
    ].

%%====================================================================
%% Edge Cases - Large Job IDs
%%====================================================================

large_job_ids_test_() ->
    {setup,
     fun() ->
         State = setup_ets(),
         %% Insert job with large ID
         Entry = #job_entry{
             job_id = 999999999,
             pid = self(),
             user_id = 1000,
             state = pending,
             partition = <<"default">>,
             submit_time = erlang:timestamp()
         },
         ets:insert(?JOBS_BY_ID, Entry),
         ets:insert(?JOBS_BY_USER, {1000, 999999999, self()}),
         ets:insert(?JOBS_BY_STATE, {pending, 999999999, self()}),
         State
     end,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"lookup works with large job ID",
           fun() ->
               {ok, Pid} = flurm_job_registry:lookup_job(999999999),
               ?assert(is_pid(Pid))
           end},
          {"list includes large job ID",
           fun() ->
               Jobs = flurm_job_registry:list_jobs(),
               JobIds = [Id || {Id, _} <- Jobs],
               ?assert(lists:member(999999999, JobIds))
           end}
         ]
     end}.

%%====================================================================
%% Edge Cases - Many Jobs
%%====================================================================

many_jobs_test_() ->
    {setup,
     fun() ->
         State = setup_ets(),
         %% Insert 100 jobs
         lists:foreach(
             fun(N) ->
                 Entry = #job_entry{
                     job_id = N,
                     pid = self(),
                     user_id = 1000 + (N rem 10),
                     state = case N rem 3 of
                         0 -> pending;
                         1 -> running;
                         2 -> completed
                     end,
                     partition = <<"default">>,
                     submit_time = erlang:timestamp()
                 },
                 ets:insert(?JOBS_BY_ID, Entry),
                 ets:insert(?JOBS_BY_USER, {Entry#job_entry.user_id, N, self()}),
                 ets:insert(?JOBS_BY_STATE, {Entry#job_entry.state, N, self()})
             end,
             lists:seq(1, 100)
         ),
         State
     end,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"list returns all 100 jobs",
           fun() ->
               Jobs = flurm_job_registry:list_jobs(),
               ?assertEqual(100, length(Jobs))
           end},
          {"count by state is correct for many jobs",
           fun() ->
               Counts = flurm_job_registry:count_by_state(),
               %% 100 jobs divided by 3 states
               PendingCount = maps:get(pending, Counts),
               RunningCount = maps:get(running, Counts),
               CompletedCount = maps:get(completed, Counts),
               ?assertEqual(100, PendingCount + RunningCount + CompletedCount)
           end},
          {"count by user is correct for many jobs",
           fun() ->
               Counts = flurm_job_registry:count_by_user(),
               TotalCount = lists:sum(maps:values(Counts)),
               ?assertEqual(100, TotalCount)
           end}
         ]
     end}.

%%====================================================================
%% Edge Cases - Various States
%%====================================================================

various_states_test_() ->
    {setup,
     fun() ->
         State = setup_ets(),
         States = [pending, configuring, running, completing,
                   completed, cancelled, failed, timeout, node_fail],
         lists:foreach(
             fun({N, JobState}) ->
                 Entry = #job_entry{
                     job_id = N,
                     pid = self(),
                     user_id = 1000,
                     state = JobState,
                     partition = <<"default">>,
                     submit_time = erlang:timestamp()
                 },
                 ets:insert(?JOBS_BY_ID, Entry),
                 ets:insert(?JOBS_BY_USER, {1000, N, self()}),
                 ets:insert(?JOBS_BY_STATE, {JobState, N, self()})
             end,
             lists:zip(lists:seq(1, length(States)), States)
         ),
         State
     end,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"all state types can be queried",
           fun() ->
               States = [pending, configuring, running, completing,
                         completed, cancelled, failed, timeout, node_fail],
               lists:foreach(
                   fun(JobState) ->
                       Jobs = flurm_job_registry:list_jobs_by_state(JobState),
                       ?assertEqual(1, length(Jobs))
                   end,
                   States
               )
           end}
         ]
     end}.

%%====================================================================
%% Monitors Map Operations Tests
%%====================================================================

monitors_map_test_() ->
    [
     {"monitors map supports multiple entries",
      fun() ->
          Ref1 = make_ref(),
          Ref2 = make_ref(),
          Ref3 = make_ref(),
          State = #state{monitors = #{Ref1 => 1, Ref2 => 2, Ref3 => 3}},
          ?assertEqual(3, maps:size(State#state.monitors)),
          ?assertEqual(1, maps:get(Ref1, State#state.monitors)),
          ?assertEqual(2, maps:get(Ref2, State#state.monitors)),
          ?assertEqual(3, maps:get(Ref3, State#state.monitors))
      end},
     {"monitors map can be updated",
      fun() ->
          Ref1 = make_ref(),
          State1 = #state{monitors = #{Ref1 => 1}},
          Ref2 = make_ref(),
          State2 = State1#state{monitors = maps:put(Ref2, 2, State1#state.monitors)},
          ?assertEqual(2, maps:size(State2#state.monitors))
      end}
    ].

%%====================================================================
%% ETS Table Properties Tests
%%====================================================================

ets_table_properties_test_() ->
    {setup,
     fun setup_ets/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"JOBS_BY_ID is a set table",
           fun() ->
               ?assertEqual(set, proplists:get_value(type, ets:info(?JOBS_BY_ID)))
           end},
          {"JOBS_BY_USER is a bag table",
           fun() ->
               ?assertEqual(bag, proplists:get_value(type, ets:info(?JOBS_BY_USER)))
           end},
          {"JOBS_BY_STATE is a bag table",
           fun() ->
               ?assertEqual(bag, proplists:get_value(type, ets:info(?JOBS_BY_STATE)))
           end},
          {"tables are public",
           fun() ->
               ?assertEqual(public, proplists:get_value(protection, ets:info(?JOBS_BY_ID))),
               ?assertEqual(public, proplists:get_value(protection, ets:info(?JOBS_BY_USER))),
               ?assertEqual(public, proplists:get_value(protection, ets:info(?JOBS_BY_STATE)))
           end},
          {"JOBS_BY_ID has read_concurrency",
           fun() ->
               ?assertEqual(true, proplists:get_value(read_concurrency, ets:info(?JOBS_BY_ID)))
           end}
         ]
     end}.

%%====================================================================
%% Partition Field Tests
%%====================================================================

partition_field_test_() ->
    {setup,
     fun() ->
         State = setup_ets(),
         %% Insert jobs with different partitions
         lists:foreach(
             fun({N, Part}) ->
                 Entry = #job_entry{
                     job_id = N,
                     pid = self(),
                     user_id = 1000,
                     state = pending,
                     partition = Part,
                     submit_time = erlang:timestamp()
                 },
                 ets:insert(?JOBS_BY_ID, Entry),
                 ets:insert(?JOBS_BY_USER, {1000, N, self()}),
                 ets:insert(?JOBS_BY_STATE, {pending, N, self()})
             end,
             [{1, <<"default">>}, {2, <<"compute">>}, {3, <<"gpu">>}, {4, <<"highmem">>}]
         ),
         State
     end,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"jobs with different partitions stored correctly",
           fun() ->
               {ok, Entry1} = flurm_job_registry:get_job_entry(1),
               ?assertEqual(<<"default">>, Entry1#job_entry.partition),

               {ok, Entry2} = flurm_job_registry:get_job_entry(2),
               ?assertEqual(<<"compute">>, Entry2#job_entry.partition),

               {ok, Entry3} = flurm_job_registry:get_job_entry(3),
               ?assertEqual(<<"gpu">>, Entry3#job_entry.partition),

               {ok, Entry4} = flurm_job_registry:get_job_entry(4),
               ?assertEqual(<<"highmem">>, Entry4#job_entry.partition)
           end}
         ]
     end}.

%%====================================================================
%% Submit Time Tests
%%====================================================================

submit_time_test_() ->
    {setup,
     fun() ->
         State = setup_ets(),
         Now = erlang:timestamp(),
         Entry = #job_entry{
             job_id = 1,
             pid = self(),
             user_id = 1000,
             state = pending,
             partition = <<"default">>,
             submit_time = Now
         },
         ets:insert(?JOBS_BY_ID, Entry),
         {State, Now}
     end,
     fun({State, _Now}) -> cleanup_ets(State) end,
     fun({_State, ExpectedTime}) ->
         [
          {"submit_time is preserved correctly",
           fun() ->
               {ok, Entry} = flurm_job_registry:get_job_entry(1),
               ?assertEqual(ExpectedTime, Entry#job_entry.submit_time)
           end}
         ]
     end}.

%%====================================================================
%% State Transition Tests (via update_state)
%%====================================================================

state_transitions_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(State) ->
         [
          {"transition pending to running",
           fun() ->
               %% Job 1 is pending
               {ok, Entry1} = flurm_job_registry:get_job_entry(1),
               ?assertEqual(pending, Entry1#job_entry.state),

               {reply, ok, _} =
                   flurm_job_registry:handle_call({update_state, 1, running}, {self(), make_ref()}, State),

               {ok, Entry2} = flurm_job_registry:get_job_entry(1),
               ?assertEqual(running, Entry2#job_entry.state)
           end},
          {"transition running to completed",
           fun() ->
               %% First set to running
               {reply, ok, State2} =
                   flurm_job_registry:handle_call({update_state, 1, running}, {self(), make_ref()}, State),

               %% Then complete
               {reply, ok, _} =
                   flurm_job_registry:handle_call({update_state, 1, completed}, {self(), make_ref()}, State2),

               {ok, Entry} = flurm_job_registry:get_job_entry(1),
               ?assertEqual(completed, Entry#job_entry.state)
           end}
         ]
     end}.

%%====================================================================
%% Consistency Tests
%%====================================================================

consistency_test_() ->
    {setup,
     fun setup_ets_with_entries/0,
     fun cleanup_ets/1,
     fun(_State) ->
         [
          {"primary and secondary indices are consistent",
           fun() ->
               %% Get all jobs from primary table
               AllJobs = flurm_job_registry:list_jobs(),

               %% Get all jobs from state index
               PendingJobs = flurm_job_registry:list_jobs_by_state(pending),
               RunningJobs = flurm_job_registry:list_jobs_by_state(running),

               %% Totals should match
               StateTotal = length(PendingJobs) + length(RunningJobs),
               ?assertEqual(length(AllJobs), StateTotal)
           end},
          {"user index matches primary table",
           fun() ->
               AllJobs = flurm_job_registry:list_jobs(),

               User1000Jobs = flurm_job_registry:list_jobs_by_user(1000),
               User2000Jobs = flurm_job_registry:list_jobs_by_user(2000),

               UserTotal = length(User1000Jobs) + length(User2000Jobs),
               ?assertEqual(length(AllJobs), UserTotal)
           end}
         ]
     end}.
