%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_step_manager gen_server callbacks
%%%
%%% Tests the callback functions directly without starting the process.
%%% Uses meck to mock lager dependency.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_step_manager_callback_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Records copied from module (internal)
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
    steps = #{} :: #{{pos_integer(), non_neg_integer()} => #step{}},
    step_counters = #{} :: #{pos_integer() => non_neg_integer()}
}).

make_state() ->
    #state{}.

make_step(JobId, StepId) ->
    #step{
        job_id = JobId,
        step_id = StepId,
        name = <<"step">>,
        state = pending,
        num_tasks = 1,
        num_nodes = 1,
        allocated_nodes = [],
        start_time = undefined,
        end_time = undefined,
        exit_code = undefined,
        command = <<"hostname">>
    }.

make_state_with_steps(Steps) ->
    StepMap = maps:from_list([{{S#step.job_id, S#step.step_id}, S} || S <- Steps]),
    Counters = lists:foldl(fun(S, Acc) ->
        JobId = S#step.job_id,
        StepId = S#step.step_id,
        Current = maps:get(JobId, Acc, 0),
        maps:put(JobId, max(Current, StepId + 1), Acc)
    end, #{}, Steps),
    #state{steps = StepMap, step_counters = Counters}.

setup() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, md, fun(_) -> ok end),    meck:expect(lager, info, fun(_Fmt) -> ok end),
    meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
    ok.

cleanup(_) ->
    meck:unload(lager),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

step_manager_callback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"init creates empty state", fun init_creates_empty_state/0},
      {"create_step success", fun handle_call_create_step_success/0},
      {"create_step with spec", fun handle_call_create_step_with_spec/0},
      {"create_step increments counter", fun handle_call_create_step_increments_counter/0},
      {"get_step success", fun handle_call_get_step_success/0},
      {"get_step not found", fun handle_call_get_step_not_found/0},
      {"list_steps returns job steps", fun handle_call_list_steps/0},
      {"list_steps empty", fun handle_call_list_steps_empty/0},
      {"update_step success", fun handle_call_update_step_success/0},
      {"update_step not found", fun handle_call_update_step_not_found/0},
      {"complete_step success", fun handle_call_complete_step_success/0},
      {"complete_step not found", fun handle_call_complete_step_not_found/0},
      {"cancel_step success", fun handle_call_cancel_step_success/0},
      {"cancel_step not found", fun handle_call_cancel_step_not_found/0},
      {"unknown request", fun handle_call_unknown_request/0},
      {"handle_cast ignores messages", fun handle_cast_ignores_messages/0},
      {"handle_info ignores messages", fun handle_info_ignores_messages/0},
      {"terminate returns ok", fun terminate_returns_ok/0},
      {"step_to_map", fun step_to_map_test/0},
      {"apply_step_updates", fun apply_step_updates_test/0}
     ]}.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_creates_empty_state() ->
    {ok, State} = flurm_step_manager:init([]),
    ?assertEqual(#{}, State#state.steps),
    ?assertEqual(#{}, State#state.step_counters).

%%====================================================================
%% handle_call/3 Tests - create_step
%%====================================================================

handle_call_create_step_success() ->
    State = make_state(),
    StepSpec = #{},
    {reply, {ok, StepId}, NewState} = flurm_step_manager:handle_call({create_step, 1, StepSpec}, {self(), make_ref()}, State),
    ?assertEqual(0, StepId),
    ?assert(maps:is_key({1, 0}, NewState#state.steps)),
    ?assertEqual(1, maps:get(1, NewState#state.step_counters)).

handle_call_create_step_with_spec() ->
    State = make_state(),
    StepSpec = #{
        name => <<"my_step">>,
        num_tasks => 4,
        num_nodes => 2,
        command => <<"mpirun -np 4 ./app">>
    },
    {reply, {ok, 0}, NewState} = flurm_step_manager:handle_call({create_step, 1, StepSpec}, {self(), make_ref()}, State),
    Step = maps:get({1, 0}, NewState#state.steps),
    ?assertEqual(<<"my_step">>, Step#step.name),
    ?assertEqual(4, Step#step.num_tasks),
    ?assertEqual(2, Step#step.num_nodes),
    ?assertEqual(<<"mpirun -np 4 ./app">>, Step#step.command),
    ?assertEqual(pending, Step#step.state).

handle_call_create_step_increments_counter() ->
    %% Create first step
    State0 = make_state(),
    {reply, {ok, 0}, State1} = flurm_step_manager:handle_call({create_step, 1, #{}}, {self(), make_ref()}, State0),
    %% Create second step for same job
    {reply, {ok, 1}, State2} = flurm_step_manager:handle_call({create_step, 1, #{}}, {self(), make_ref()}, State1),
    %% Create step for different job
    {reply, {ok, 0}, State3} = flurm_step_manager:handle_call({create_step, 2, #{}}, {self(), make_ref()}, State2),

    ?assertEqual(2, maps:get(1, State3#state.step_counters)),
    ?assertEqual(1, maps:get(2, State3#state.step_counters)),
    ?assert(maps:is_key({1, 0}, State3#state.steps)),
    ?assert(maps:is_key({1, 1}, State3#state.steps)),
    ?assert(maps:is_key({2, 0}, State3#state.steps)).

%%====================================================================
%% handle_call/3 Tests - get_step
%%====================================================================

handle_call_get_step_success() ->
    Step = make_step(1, 0),
    State = make_state_with_steps([Step]),
    {reply, {ok, StepMap}, NewState} = flurm_step_manager:handle_call({get_step, 1, 0}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState),
    ?assertEqual(1, maps:get(job_id, StepMap)),
    ?assertEqual(0, maps:get(step_id, StepMap)),
    ?assertEqual(pending, maps:get(state, StepMap)).

handle_call_get_step_not_found() ->
    State = make_state(),
    {reply, {error, not_found}, NewState} = flurm_step_manager:handle_call({get_step, 1, 0}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - list_steps
%%====================================================================

handle_call_list_steps() ->
    Step1 = make_step(1, 0),
    Step2 = make_step(1, 1),
    Step3 = make_step(2, 0),  % Different job
    State = make_state_with_steps([Step1, Step2, Step3]),
    {reply, Steps, _NewState} = flurm_step_manager:handle_call({list_steps, 1}, {self(), make_ref()}, State),
    ?assertEqual(2, length(Steps)),
    StepIds = [maps:get(step_id, S) || S <- Steps],
    ?assert(lists:member(0, StepIds)),
    ?assert(lists:member(1, StepIds)).

handle_call_list_steps_empty() ->
    State = make_state(),
    {reply, Steps, _NewState} = flurm_step_manager:handle_call({list_steps, 1}, {self(), make_ref()}, State),
    ?assertEqual([], Steps).

%%====================================================================
%% handle_call/3 Tests - update_step
%%====================================================================

handle_call_update_step_success() ->
    Step = make_step(1, 0),
    State = make_state_with_steps([Step]),
    Updates = #{
        state => running,
        allocated_nodes => [<<"node1">>, <<"node2">>],
        start_time => 1234567890
    },
    {reply, ok, NewState} = flurm_step_manager:handle_call({update_step, 1, 0, Updates}, {self(), make_ref()}, State),
    UpdatedStep = maps:get({1, 0}, NewState#state.steps),
    ?assertEqual(running, UpdatedStep#step.state),
    ?assertEqual([<<"node1">>, <<"node2">>], UpdatedStep#step.allocated_nodes),
    ?assertEqual(1234567890, UpdatedStep#step.start_time).

handle_call_update_step_not_found() ->
    State = make_state(),
    Updates = #{state => running},
    {reply, {error, not_found}, NewState} = flurm_step_manager:handle_call({update_step, 1, 0, Updates}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - complete_step
%%====================================================================

handle_call_complete_step_success() ->
    Step = (make_step(1, 0))#step{state = running, start_time = 1234567890},
    State = make_state_with_steps([Step]),
    {reply, ok, NewState} = flurm_step_manager:handle_call({complete_step, 1, 0, 0}, {self(), make_ref()}, State),
    UpdatedStep = maps:get({1, 0}, NewState#state.steps),
    ?assertEqual(completed, UpdatedStep#step.state),
    ?assertEqual(0, UpdatedStep#step.exit_code),
    ?assertNotEqual(undefined, UpdatedStep#step.end_time).

handle_call_complete_step_not_found() ->
    State = make_state(),
    {reply, {error, not_found}, NewState} = flurm_step_manager:handle_call({complete_step, 1, 0, 0}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - cancel_step
%%====================================================================

handle_call_cancel_step_success() ->
    Step = make_step(1, 0),
    State = make_state_with_steps([Step]),
    {reply, ok, NewState} = flurm_step_manager:handle_call({cancel_step, 1, 0}, {self(), make_ref()}, State),
    UpdatedStep = maps:get({1, 0}, NewState#state.steps),
    ?assertEqual(cancelled, UpdatedStep#step.state),
    ?assertNotEqual(undefined, UpdatedStep#step.end_time).

handle_call_cancel_step_not_found() ->
    State = make_state(),
    {reply, {error, not_found}, NewState} = flurm_step_manager:handle_call({cancel_step, 1, 0}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_call/3 Tests - unknown request
%%====================================================================

handle_call_unknown_request() ->
    State = make_state(),
    {reply, {error, unknown_request}, NewState} = flurm_step_manager:handle_call({unknown, request}, {self(), make_ref()}, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_ignores_messages() ->
    State = make_state(),
    {noreply, NewState} = flurm_step_manager:handle_cast(any_message, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_ignores_messages() ->
    State = make_state(),
    {noreply, NewState} = flurm_step_manager:handle_info(any_info, State),
    ?assertEqual(State, NewState).

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_returns_ok() ->
    State = make_state(),
    ?assertEqual(ok, flurm_step_manager:terminate(normal, State)),
    ?assertEqual(ok, flurm_step_manager:terminate(shutdown, State)).

%%====================================================================
%% Internal Helper Tests
%%====================================================================

step_to_map_test() ->
    Step = #step{
        job_id = 1,
        step_id = 0,
        name = <<"test_step">>,
        state = running,
        num_tasks = 4,
        num_nodes = 2,
        allocated_nodes = [<<"node1">>, <<"node2">>],
        start_time = 1234567890,
        end_time = undefined,
        exit_code = undefined,
        command = <<"./test">>
    },
    Map = flurm_step_manager:step_to_map(Step),
    ?assertEqual(1, maps:get(job_id, Map)),
    ?assertEqual(0, maps:get(step_id, Map)),
    ?assertEqual(<<"test_step">>, maps:get(name, Map)),
    ?assertEqual(running, maps:get(state, Map)),
    ?assertEqual(4, maps:get(num_tasks, Map)),
    ?assertEqual(2, maps:get(num_nodes, Map)),
    ?assertEqual([<<"node1">>, <<"node2">>], maps:get(allocated_nodes, Map)),
    ?assertEqual(1234567890, maps:get(start_time, Map)),
    ?assertEqual(undefined, maps:get(end_time, Map)),
    ?assertEqual(undefined, maps:get(exit_code, Map)),
    ?assertEqual(<<"./test">>, maps:get(command, Map)).

apply_step_updates_test() ->
    Step = make_step(1, 0),
    Updates = #{
        state => running,
        allocated_nodes => [<<"node1">>],
        start_time => 1234567890,
        end_time => 1234567900,
        exit_code => 0,
        unknown_field => ignored
    },
    Updated = flurm_step_manager:apply_step_updates(Step, Updates),
    ?assertEqual(running, Updated#step.state),
    ?assertEqual([<<"node1">>], Updated#step.allocated_nodes),
    ?assertEqual(1234567890, Updated#step.start_time),
    ?assertEqual(1234567900, Updated#step.end_time),
    ?assertEqual(0, Updated#step.exit_code),
    %% Name and command should remain unchanged
    ?assertEqual(<<"step">>, Updated#step.name),
    ?assertEqual(<<"hostname">>, Updated#step.command).
