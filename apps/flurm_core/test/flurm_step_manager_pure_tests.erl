%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_step_manager (Gen Server)
%%%
%%% Tests the step manager gen_server by directly calling
%%% init/1, handle_call/3, handle_cast/2, handle_info/2, and terminate/2.
%%%
%%% NO MECK - Pure unit tests only.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_step_manager_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% Record definition matching the module's internal state
-record(step, {
    job_id :: pos_integer(),
    step_id :: non_neg_integer(),
    name :: binary(),
    state :: pending | running | completing | completed | failed | cancelled,
    num_tasks :: pos_integer(),
    num_nodes :: pos_integer(),
    allocated_nodes :: [binary()],
    start_time :: integer() | undefined,
    end_time :: integer() | undefined,
    exit_code :: integer() | undefined,
    command :: binary()
}).

-record(state, {
    steps = #{} :: #{job_step_key() => #step{}},
    step_counters = #{} :: #{pos_integer() => non_neg_integer()}
}).

-type job_step_key() :: {pos_integer(), non_neg_integer()}.

%%====================================================================
%% Test Descriptions
%%====================================================================

flurm_step_manager_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% init/1 tests
      {"init returns ok with empty state", fun init_test/0},

      %% create_step tests
      {"create_step creates first step with id 0", fun create_step_first_test/0},
      {"create_step increments step id", fun create_step_increment_test/0},
      {"create_step with custom spec", fun create_step_custom_spec_test/0},
      {"create_step for different jobs", fun create_step_different_jobs_test/0},

      %% get_step tests
      {"get_step returns step data", fun get_step_found_test/0},
      {"get_step returns not_found for missing", fun get_step_not_found_test/0},

      %% list_steps tests
      {"list_steps returns empty for no steps", fun list_steps_empty_test/0},
      {"list_steps returns all job steps", fun list_steps_test/0},
      {"list_steps filters by job id", fun list_steps_filter_test/0},

      %% update_step tests
      {"update_step updates state", fun update_step_state_test/0},
      {"update_step updates multiple fields", fun update_step_multiple_test/0},
      {"update_step returns not_found for missing", fun update_step_not_found_test/0},
      {"update_step ignores unknown fields", fun update_step_unknown_fields_test/0},

      %% complete_step tests
      {"complete_step marks completed with exit code", fun complete_step_test/0},
      {"complete_step returns not_found for missing", fun complete_step_not_found_test/0},
      {"complete_step with various exit codes", fun complete_step_exit_codes_test/0},

      %% cancel_step tests
      {"cancel_step marks cancelled", fun cancel_step_test/0},
      {"cancel_step returns not_found for missing", fun cancel_step_not_found_test/0},

      %% handle_call unknown request
      {"handle_call unknown request returns error", fun handle_call_unknown_test/0},

      %% handle_cast tests
      {"handle_cast ignores unknown messages", fun handle_cast_test/0},

      %% handle_info tests
      {"handle_info ignores unknown messages", fun handle_info_test/0},

      %% terminate tests
      {"terminate returns ok", fun terminate_test/0}
     ]}.

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup() ->
    %% Ensure lager is started for logging
    case application:ensure_all_started(lager) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        _ -> ok
    end,
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test() ->
    {ok, State} = flurm_step_manager:init([]),
    ?assertEqual(#{}, State#state.steps),
    ?assertEqual(#{}, State#state.step_counters).

%%====================================================================
%% handle_call create_step Tests
%%====================================================================

create_step_first_test() ->
    %% First step for a job should have step_id 0
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 1001,
    StepSpec = #{name => <<"test_step">>, command => <<"/bin/echo hello">>},

    {reply, {ok, StepId}, NewState} =
        flurm_step_manager:handle_call({create_step, JobId, StepSpec}, {self(), ref}, InitState),

    ?assertEqual(0, StepId),
    ?assertEqual(1, maps:get(JobId, NewState#state.step_counters)),
    ?assert(maps:is_key({JobId, 0}, NewState#state.steps)).

create_step_increment_test() ->
    %% Subsequent steps should have incrementing step_ids
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 1002,

    %% Create first step
    {reply, {ok, 0}, State1} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, InitState),

    %% Create second step
    {reply, {ok, 1}, State2} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, State1),

    %% Create third step
    {reply, {ok, 2}, State3} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, State2),

    ?assertEqual(3, maps:get(JobId, State3#state.step_counters)),
    ?assertEqual(3, maps:size(State3#state.steps)).

create_step_custom_spec_test() ->
    %% Test step creation with custom spec values
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 1003,
    StepSpec = #{
        name => <<"custom_step">>,
        num_tasks => 4,
        num_nodes => 2,
        command => <<"/path/to/script.sh">>
    },

    {reply, {ok, StepId}, NewState} =
        flurm_step_manager:handle_call({create_step, JobId, StepSpec}, {self(), ref}, InitState),

    ?assertEqual(0, StepId),
    Step = maps:get({JobId, 0}, NewState#state.steps),
    ?assertEqual(<<"custom_step">>, Step#step.name),
    ?assertEqual(4, Step#step.num_tasks),
    ?assertEqual(2, Step#step.num_nodes),
    ?assertEqual(<<"/path/to/script.sh">>, Step#step.command),
    ?assertEqual(pending, Step#step.state).

create_step_different_jobs_test() ->
    %% Steps for different jobs should have independent counters
    {ok, InitState} = flurm_step_manager:init([]),

    %% Create step for job 1
    {reply, {ok, 0}, State1} =
        flurm_step_manager:handle_call({create_step, 100, #{}}, {self(), ref}, InitState),

    %% Create step for job 2
    {reply, {ok, 0}, State2} =
        flurm_step_manager:handle_call({create_step, 200, #{}}, {self(), ref}, State1),

    %% Create another step for job 1
    {reply, {ok, 1}, State3} =
        flurm_step_manager:handle_call({create_step, 100, #{}}, {self(), ref}, State2),

    ?assertEqual(2, maps:get(100, State3#state.step_counters)),
    ?assertEqual(1, maps:get(200, State3#state.step_counters)).

%%====================================================================
%% handle_call get_step Tests
%%====================================================================

get_step_found_test() ->
    %% Get an existing step
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 2001,
    StepSpec = #{name => <<"my_step">>, num_tasks => 2},

    %% Create step
    {reply, {ok, StepId}, State1} =
        flurm_step_manager:handle_call({create_step, JobId, StepSpec}, {self(), ref}, InitState),

    %% Get step
    {reply, {ok, StepMap}, _} =
        flurm_step_manager:handle_call({get_step, JobId, StepId}, {self(), ref}, State1),

    ?assertEqual(JobId, maps:get(job_id, StepMap)),
    ?assertEqual(0, maps:get(step_id, StepMap)),
    ?assertEqual(<<"my_step">>, maps:get(name, StepMap)),
    ?assertEqual(pending, maps:get(state, StepMap)),
    ?assertEqual(2, maps:get(num_tasks, StepMap)).

get_step_not_found_test() ->
    %% Get a non-existent step
    {ok, InitState} = flurm_step_manager:init([]),

    {reply, Result, _} =
        flurm_step_manager:handle_call({get_step, 9999, 0}, {self(), ref}, InitState),

    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% handle_call list_steps Tests
%%====================================================================

list_steps_empty_test() ->
    %% List steps for a job with no steps
    {ok, InitState} = flurm_step_manager:init([]),

    {reply, Steps, _} =
        flurm_step_manager:handle_call({list_steps, 9999}, {self(), ref}, InitState),

    ?assertEqual([], Steps).

list_steps_test() ->
    %% List all steps for a job
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 3001,

    %% Create multiple steps
    {reply, {ok, _}, State1} =
        flurm_step_manager:handle_call({create_step, JobId, #{name => <<"step0">>}}, {self(), ref}, InitState),
    {reply, {ok, _}, State2} =
        flurm_step_manager:handle_call({create_step, JobId, #{name => <<"step1">>}}, {self(), ref}, State1),
    {reply, {ok, _}, State3} =
        flurm_step_manager:handle_call({create_step, JobId, #{name => <<"step2">>}}, {self(), ref}, State2),

    %% List steps
    {reply, Steps, _} =
        flurm_step_manager:handle_call({list_steps, JobId}, {self(), ref}, State3),

    ?assertEqual(3, length(Steps)).

list_steps_filter_test() ->
    %% List steps should only return steps for the specified job
    {ok, InitState} = flurm_step_manager:init([]),

    %% Create steps for different jobs
    {reply, {ok, _}, State1} =
        flurm_step_manager:handle_call({create_step, 100, #{}}, {self(), ref}, InitState),
    {reply, {ok, _}, State2} =
        flurm_step_manager:handle_call({create_step, 100, #{}}, {self(), ref}, State1),
    {reply, {ok, _}, State3} =
        flurm_step_manager:handle_call({create_step, 200, #{}}, {self(), ref}, State2),

    %% List steps for job 100
    {reply, Steps100, _} =
        flurm_step_manager:handle_call({list_steps, 100}, {self(), ref}, State3),
    ?assertEqual(2, length(Steps100)),

    %% List steps for job 200
    {reply, Steps200, _} =
        flurm_step_manager:handle_call({list_steps, 200}, {self(), ref}, State3),
    ?assertEqual(1, length(Steps200)).

%%====================================================================
%% handle_call update_step Tests
%%====================================================================

update_step_state_test() ->
    %% Update step state to running
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 4001,

    %% Create step
    {reply, {ok, StepId}, State1} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, InitState),

    %% Update state
    Now = erlang:system_time(second),
    {reply, ok, State2} =
        flurm_step_manager:handle_call(
            {update_step, JobId, StepId, #{state => running, start_time => Now}},
            {self(), ref}, State1),

    %% Verify update
    Step = maps:get({JobId, StepId}, State2#state.steps),
    ?assertEqual(running, Step#step.state),
    ?assertEqual(Now, Step#step.start_time).

update_step_multiple_test() ->
    %% Update multiple fields at once
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 4002,

    %% Create step
    {reply, {ok, StepId}, State1} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, InitState),

    %% Update multiple fields
    {reply, ok, State2} =
        flurm_step_manager:handle_call(
            {update_step, JobId, StepId, #{
                state => running,
                allocated_nodes => [<<"node1">>, <<"node2">>],
                start_time => 1234567890
            }},
            {self(), ref}, State1),

    %% Verify updates
    Step = maps:get({JobId, StepId}, State2#state.steps),
    ?assertEqual(running, Step#step.state),
    ?assertEqual([<<"node1">>, <<"node2">>], Step#step.allocated_nodes),
    ?assertEqual(1234567890, Step#step.start_time).

update_step_not_found_test() ->
    %% Update a non-existent step
    {ok, InitState} = flurm_step_manager:init([]),

    {reply, Result, _} =
        flurm_step_manager:handle_call(
            {update_step, 9999, 0, #{state => running}},
            {self(), ref}, InitState),

    ?assertEqual({error, not_found}, Result).

update_step_unknown_fields_test() ->
    %% Unknown fields should be ignored
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 4003,

    %% Create step
    {reply, {ok, StepId}, State1} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, InitState),

    %% Update with unknown field
    {reply, ok, State2} =
        flurm_step_manager:handle_call(
            {update_step, JobId, StepId, #{
                state => running,
                unknown_field => <<"should be ignored">>
            }},
            {self(), ref}, State1),

    %% Verify only valid field was updated
    Step = maps:get({JobId, StepId}, State2#state.steps),
    ?assertEqual(running, Step#step.state).

%%====================================================================
%% handle_call complete_step Tests
%%====================================================================

complete_step_test() ->
    %% Complete a step with exit code
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 5001,

    %% Create step
    {reply, {ok, StepId}, State1} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, InitState),

    %% Complete step
    {reply, ok, State2} =
        flurm_step_manager:handle_call(
            {complete_step, JobId, StepId, 0},
            {self(), ref}, State1),

    %% Verify completion
    Step = maps:get({JobId, StepId}, State2#state.steps),
    ?assertEqual(completed, Step#step.state),
    ?assertEqual(0, Step#step.exit_code),
    ?assertNotEqual(undefined, Step#step.end_time).

complete_step_not_found_test() ->
    %% Complete a non-existent step
    {ok, InitState} = flurm_step_manager:init([]),

    {reply, Result, _} =
        flurm_step_manager:handle_call(
            {complete_step, 9999, 0, 0},
            {self(), ref}, InitState),

    ?assertEqual({error, not_found}, Result).

complete_step_exit_codes_test() ->
    %% Test various exit codes
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 5002,

    %% Create multiple steps
    {reply, {ok, 0}, State1} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, InitState),
    {reply, {ok, 1}, State2} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, State1),
    {reply, {ok, 2}, State3} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, State2),

    %% Complete with different exit codes
    {reply, ok, State4} =
        flurm_step_manager:handle_call({complete_step, JobId, 0, 0}, {self(), ref}, State3),
    {reply, ok, State5} =
        flurm_step_manager:handle_call({complete_step, JobId, 1, 1}, {self(), ref}, State4),
    {reply, ok, State6} =
        flurm_step_manager:handle_call({complete_step, JobId, 2, 127}, {self(), ref}, State5),

    %% Verify exit codes
    ?assertEqual(0, (maps:get({JobId, 0}, State6#state.steps))#step.exit_code),
    ?assertEqual(1, (maps:get({JobId, 1}, State6#state.steps))#step.exit_code),
    ?assertEqual(127, (maps:get({JobId, 2}, State6#state.steps))#step.exit_code).

%%====================================================================
%% handle_call cancel_step Tests
%%====================================================================

cancel_step_test() ->
    %% Cancel a step
    {ok, InitState} = flurm_step_manager:init([]),
    JobId = 6001,

    %% Create step
    {reply, {ok, StepId}, State1} =
        flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, InitState),

    %% Cancel step
    {reply, ok, State2} =
        flurm_step_manager:handle_call(
            {cancel_step, JobId, StepId},
            {self(), ref}, State1),

    %% Verify cancellation
    Step = maps:get({JobId, StepId}, State2#state.steps),
    ?assertEqual(cancelled, Step#step.state),
    ?assertNotEqual(undefined, Step#step.end_time).

cancel_step_not_found_test() ->
    %% Cancel a non-existent step
    {ok, InitState} = flurm_step_manager:init([]),

    {reply, Result, _} =
        flurm_step_manager:handle_call(
            {cancel_step, 9999, 0},
            {self(), ref}, InitState),

    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% handle_call unknown request Test
%%====================================================================

handle_call_unknown_test() ->
    %% Unknown request should return error
    {ok, InitState} = flurm_step_manager:init([]),

    {reply, Result, _} =
        flurm_step_manager:handle_call({unknown_request, arg1}, {self(), ref}, InitState),

    ?assertEqual({error, unknown_request}, Result).

%%====================================================================
%% handle_cast Tests
%%====================================================================

handle_cast_test() ->
    %% handle_cast should ignore messages and return noreply
    {ok, InitState} = flurm_step_manager:init([]),

    {noreply, State1} = flurm_step_manager:handle_cast(unknown_message, InitState),
    ?assertEqual(InitState, State1),

    {noreply, State2} = flurm_step_manager:handle_cast({some, complex, message}, InitState),
    ?assertEqual(InitState, State2).

%%====================================================================
%% handle_info Tests
%%====================================================================

handle_info_test() ->
    %% handle_info should ignore messages and return noreply
    {ok, InitState} = flurm_step_manager:init([]),

    {noreply, State1} = flurm_step_manager:handle_info(unknown_message, InitState),
    ?assertEqual(InitState, State1),

    {noreply, State2} = flurm_step_manager:handle_info({some, info, message}, InitState),
    ?assertEqual(InitState, State2).

%%====================================================================
%% terminate Tests
%%====================================================================

terminate_test() ->
    %% terminate should return ok
    {ok, InitState} = flurm_step_manager:init([]),

    Result1 = flurm_step_manager:terminate(normal, InitState),
    ?assertEqual(ok, Result1),

    Result2 = flurm_step_manager:terminate(shutdown, InitState),
    ?assertEqual(ok, Result2),

    Result3 = flurm_step_manager:terminate({shutdown, term}, InitState),
    ?assertEqual(ok, Result3).

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

step_lifecycle_test_() ->
    %% Test a complete step lifecycle
    {setup,
     fun() ->
         {ok, InitState} = flurm_step_manager:init([]),
         InitState
     end,
     fun(_) -> ok end,
     fun(InitState) ->
         JobId = 7001,

         %% Create step
         {reply, {ok, StepId}, State1} =
             flurm_step_manager:handle_call({create_step, JobId, #{name => <<"lifecycle_step">>}},
                                            {self(), ref}, InitState),

         %% Update to running
         {reply, ok, State2} =
             flurm_step_manager:handle_call(
                 {update_step, JobId, StepId, #{state => running, start_time => 1000}},
                 {self(), ref}, State1),

         %% Complete step
         {reply, ok, State3} =
             flurm_step_manager:handle_call({complete_step, JobId, StepId, 0},
                                            {self(), ref}, State2),

         %% Get final step state
         {reply, {ok, FinalStep}, _} =
             flurm_step_manager:handle_call({get_step, JobId, StepId}, {self(), ref}, State3),

         [
          ?_assertEqual(0, StepId),
          ?_assertEqual(completed, maps:get(state, FinalStep)),
          ?_assertEqual(0, maps:get(exit_code, FinalStep))
         ]
     end}.

many_steps_test_() ->
    %% Test creating many steps
    {setup,
     fun() ->
         {ok, InitState} = flurm_step_manager:init([]),
         InitState
     end,
     fun(_) -> ok end,
     fun(InitState) ->
         JobId = 8001,
         NumSteps = 100,

         %% Create many steps
         FinalState = lists:foldl(
             fun(_, State) ->
                 {reply, {ok, _}, NewState} =
                     flurm_step_manager:handle_call({create_step, JobId, #{}},
                                                    {self(), ref}, State),
                 NewState
             end,
             InitState,
             lists:seq(1, NumSteps)),

         %% List steps
         {reply, Steps, _} =
             flurm_step_manager:handle_call({list_steps, JobId}, {self(), ref}, FinalState),

         [
          ?_assertEqual(NumSteps, length(Steps)),
          ?_assertEqual(NumSteps, maps:get(JobId, FinalState#state.step_counters))
         ]
     end}.

default_values_test_() ->
    %% Test that default values are set correctly
    {setup,
     fun() ->
         {ok, InitState} = flurm_step_manager:init([]),
         InitState
     end,
     fun(_) -> ok end,
     fun(InitState) ->
         JobId = 9001,

         %% Create step with empty spec
         {reply, {ok, _}, State1} =
             flurm_step_manager:handle_call({create_step, JobId, #{}}, {self(), ref}, InitState),

         Step = maps:get({JobId, 0}, State1#state.steps),

         [
          ?_assertEqual(<<"step">>, Step#step.name),
          ?_assertEqual(pending, Step#step.state),
          ?_assertEqual(1, Step#step.num_tasks),
          ?_assertEqual(1, Step#step.num_nodes),
          ?_assertEqual([], Step#step.allocated_nodes),
          ?_assertEqual(undefined, Step#step.start_time),
          ?_assertEqual(undefined, Step#step.end_time),
          ?_assertEqual(undefined, Step#step.exit_code),
          ?_assertEqual(<<>>, Step#step.command)
         ]
     end}.
