%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_step_manager module
%%% Tests job step tracking and lifecycle management
%%%-------------------------------------------------------------------
-module(flurm_step_manager_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing step manager
    catch gen_server:stop(flurm_step_manager),
    timer:sleep(20),
    %% Start lager for logging
    application:ensure_all_started(lager),
    %% Start fresh
    {ok, Pid} = flurm_step_manager:start_link(),
    {started, Pid}.

cleanup({started, _Pid}) ->
    catch gen_server:stop(flurm_step_manager),
    timer:sleep(20);
cleanup(_) ->
    ok.

%%====================================================================
%% Test Generator
%%====================================================================

step_manager_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"Server starts correctly", fun test_server_starts/0},
      {"Create step", fun test_create_step/0},
      {"Create step with defaults", fun test_create_step_defaults/0},
      {"Create multiple steps for same job", fun test_create_multiple_steps/0},
      {"Get step", fun test_get_step/0},
      {"Get nonexistent step", fun test_get_step_not_found/0},
      {"List steps for job", fun test_list_steps/0},
      {"List steps for job with no steps", fun test_list_steps_empty/0},
      {"Update step state", fun test_update_step/0},
      {"Update step allocated_nodes", fun test_update_step_nodes/0},
      {"Update step start_time", fun test_update_step_start_time/0},
      {"Update step end_time", fun test_update_step_end_time/0},
      {"Update step exit_code", fun test_update_step_exit_code/0},
      {"Update nonexistent step", fun test_update_step_not_found/0},
      {"Complete step", fun test_complete_step/0},
      {"Complete nonexistent step", fun test_complete_step_not_found/0},
      {"Cancel step", fun test_cancel_step/0},
      {"Cancel nonexistent step", fun test_cancel_step_not_found/0},
      {"Handle unknown call", fun test_unknown_call/0},
      {"Handle unknown cast", fun test_unknown_cast/0},
      {"Handle unknown info", fun test_unknown_info/0},
      {"Terminate callback", fun test_terminate/0}
     ]}.

%%====================================================================
%% Test Cases
%%====================================================================

test_server_starts() ->
    Pid = whereis(flurm_step_manager),
    ?assert(is_pid(Pid)).

test_create_step() ->
    JobId = 1001,
    StepSpec = #{
        name => <<"my_step">>,
        num_tasks => 4,
        num_nodes => 2,
        command => <<"srun hostname">>
    },
    {ok, StepId} = flurm_step_manager:create_step(JobId, StepSpec),
    ?assertEqual(0, StepId),

    %% Verify step was created
    {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
    ?assertEqual(JobId, maps:get(job_id, Step)),
    ?assertEqual(0, maps:get(step_id, Step)),
    ?assertEqual(<<"my_step">>, maps:get(name, Step)),
    ?assertEqual(pending, maps:get(state, Step)),
    ?assertEqual(4, maps:get(num_tasks, Step)),
    ?assertEqual(2, maps:get(num_nodes, Step)),
    ?assertEqual(<<"srun hostname">>, maps:get(command, Step)).

test_create_step_defaults() ->
    JobId = 1002,
    StepSpec = #{},  % Empty spec should use defaults
    {ok, StepId} = flurm_step_manager:create_step(JobId, StepSpec),

    {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
    ?assertEqual(<<"step">>, maps:get(name, Step)),
    ?assertEqual(1, maps:get(num_tasks, Step)),
    ?assertEqual(1, maps:get(num_nodes, Step)),
    ?assertEqual(<<>>, maps:get(command, Step)),
    ?assertEqual([], maps:get(allocated_nodes, Step)),
    ?assertEqual(undefined, maps:get(start_time, Step)),
    ?assertEqual(undefined, maps:get(end_time, Step)),
    ?assertEqual(undefined, maps:get(exit_code, Step)).

test_create_multiple_steps() ->
    JobId = 1003,

    %% Create first step
    {ok, StepId1} = flurm_step_manager:create_step(JobId, #{name => <<"step1">>}),
    ?assertEqual(0, StepId1),

    %% Create second step
    {ok, StepId2} = flurm_step_manager:create_step(JobId, #{name => <<"step2">>}),
    ?assertEqual(1, StepId2),

    %% Create third step
    {ok, StepId3} = flurm_step_manager:create_step(JobId, #{name => <<"step3">>}),
    ?assertEqual(2, StepId3),

    %% Verify all steps exist
    Steps = flurm_step_manager:list_steps(JobId),
    ?assertEqual(3, length(Steps)).

test_get_step() ->
    JobId = 1004,
    {ok, StepId} = flurm_step_manager:create_step(JobId, #{name => <<"test_step">>}),
    {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
    ?assertEqual(<<"test_step">>, maps:get(name, Step)).

test_get_step_not_found() ->
    Result = flurm_step_manager:get_step(9999, 0),
    ?assertEqual({error, not_found}, Result).

test_list_steps() ->
    JobId = 1005,
    %% Create multiple steps
    flurm_step_manager:create_step(JobId, #{name => <<"step_a">>}),
    flurm_step_manager:create_step(JobId, #{name => <<"step_b">>}),
    flurm_step_manager:create_step(JobId, #{name => <<"step_c">>}),

    Steps = flurm_step_manager:list_steps(JobId),
    ?assertEqual(3, length(Steps)),

    %% Verify all steps belong to the job
    lists:foreach(fun(Step) ->
        ?assertEqual(JobId, maps:get(job_id, Step))
    end, Steps).

test_list_steps_empty() ->
    Steps = flurm_step_manager:list_steps(8888),
    ?assertEqual([], Steps).

test_update_step() ->
    JobId = 1006,
    {ok, StepId} = flurm_step_manager:create_step(JobId, #{}),

    %% Update state to running
    ok = flurm_step_manager:update_step(JobId, StepId, #{state => running}),

    {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
    ?assertEqual(running, maps:get(state, Step)).

test_update_step_nodes() ->
    JobId = 1007,
    {ok, StepId} = flurm_step_manager:create_step(JobId, #{}),

    Nodes = [<<"node01">>, <<"node02">>],
    ok = flurm_step_manager:update_step(JobId, StepId, #{allocated_nodes => Nodes}),

    {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
    ?assertEqual(Nodes, maps:get(allocated_nodes, Step)).

test_update_step_start_time() ->
    JobId = 1008,
    {ok, StepId} = flurm_step_manager:create_step(JobId, #{}),

    StartTime = erlang:system_time(second),
    ok = flurm_step_manager:update_step(JobId, StepId, #{start_time => StartTime}),

    {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
    ?assertEqual(StartTime, maps:get(start_time, Step)).

test_update_step_end_time() ->
    JobId = 1009,
    {ok, StepId} = flurm_step_manager:create_step(JobId, #{}),

    EndTime = erlang:system_time(second),
    ok = flurm_step_manager:update_step(JobId, StepId, #{end_time => EndTime}),

    {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
    ?assertEqual(EndTime, maps:get(end_time, Step)).

test_update_step_exit_code() ->
    JobId = 1010,
    {ok, StepId} = flurm_step_manager:create_step(JobId, #{}),

    ok = flurm_step_manager:update_step(JobId, StepId, #{exit_code => 42}),

    {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
    ?assertEqual(42, maps:get(exit_code, Step)).

test_update_step_not_found() ->
    Result = flurm_step_manager:update_step(7777, 0, #{state => running}),
    ?assertEqual({error, not_found}, Result).

test_complete_step() ->
    JobId = 1011,
    {ok, StepId} = flurm_step_manager:create_step(JobId, #{}),

    %% Complete the step with exit code 0
    ok = flurm_step_manager:complete_step(JobId, StepId, 0),

    {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
    ?assertEqual(completed, maps:get(state, Step)),
    ?assertEqual(0, maps:get(exit_code, Step)),
    ?assert(maps:get(end_time, Step) =/= undefined).

test_complete_step_not_found() ->
    Result = flurm_step_manager:complete_step(6666, 0, 0),
    ?assertEqual({error, not_found}, Result).

test_cancel_step() ->
    JobId = 1012,
    {ok, StepId} = flurm_step_manager:create_step(JobId, #{}),

    %% Cancel the step
    ok = flurm_step_manager:cancel_step(JobId, StepId),

    {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
    ?assertEqual(cancelled, maps:get(state, Step)),
    ?assert(maps:get(end_time, Step) =/= undefined).

test_cancel_step_not_found() ->
    Result = flurm_step_manager:cancel_step(5555, 0),
    ?assertEqual({error, not_found}, Result).

test_unknown_call() ->
    Result = gen_server:call(flurm_step_manager, {unknown_request, arg}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    ok = gen_server:cast(flurm_step_manager, {unknown_cast, arg}),
    timer:sleep(20),
    ?assert(is_pid(whereis(flurm_step_manager))).

test_unknown_info() ->
    flurm_step_manager ! {unknown_info, arg},
    timer:sleep(20),
    ?assert(is_pid(whereis(flurm_step_manager))).

test_terminate() ->
    Pid = whereis(flurm_step_manager),
    ?assert(is_pid(Pid)),
    gen_server:stop(flurm_step_manager),
    timer:sleep(50),
    ?assertEqual(undefined, whereis(flurm_step_manager)).

%%====================================================================
%% Multi-Job Tests
%%====================================================================

multi_job_test_() ->
    {setup,
     fun() ->
         catch gen_server:stop(flurm_step_manager),
         timer:sleep(20),
         application:ensure_all_started(lager),
         {ok, Pid} = flurm_step_manager:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid),
         timer:sleep(20)
     end,
     [
      {"Steps isolated per job", fun() ->
          %% Create steps for job 1
          {ok, _} = flurm_step_manager:create_step(1, #{name => <<"j1s0">>}),
          {ok, _} = flurm_step_manager:create_step(1, #{name => <<"j1s1">>}),

          %% Create steps for job 2
          {ok, _} = flurm_step_manager:create_step(2, #{name => <<"j2s0">>}),

          %% List should be isolated
          Steps1 = flurm_step_manager:list_steps(1),
          Steps2 = flurm_step_manager:list_steps(2),

          ?assertEqual(2, length(Steps1)),
          ?assertEqual(1, length(Steps2))
      end},
      {"Step IDs independent per job", fun() ->
          {ok, S1} = flurm_step_manager:create_step(100, #{}),
          {ok, S2} = flurm_step_manager:create_step(200, #{}),

          %% Both should start at 0
          ?assertEqual(0, S1),
          ?assertEqual(0, S2)
      end}
     ]}.

%%====================================================================
%% Update with Unknown Keys Test
%%====================================================================

update_unknown_keys_test_() ->
    {setup,
     fun() ->
         catch gen_server:stop(flurm_step_manager),
         timer:sleep(20),
         application:ensure_all_started(lager),
         {ok, Pid} = flurm_step_manager:start_link(),
         Pid
     end,
     fun(Pid) ->
         catch gen_server:stop(Pid),
         timer:sleep(20)
     end,
     [
      {"Unknown update keys are ignored", fun() ->
          JobId = 3001,
          {ok, StepId} = flurm_step_manager:create_step(JobId, #{}),

          %% Update with unknown key - should succeed and ignore unknown
          ok = flurm_step_manager:update_step(JobId, StepId, #{
              state => running,
              unknown_key => <<"ignored">>
          }),

          {ok, Step} = flurm_step_manager:get_step(JobId, StepId),
          ?assertEqual(running, maps:get(state, Step)),
          ?assertNot(maps:is_key(unknown_key, Step))
      end}
     ]}.
