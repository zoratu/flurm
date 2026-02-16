%%%-------------------------------------------------------------------
%%% @doc Real EUnit tests for flurm_job_executor module
%%%
%%% Tests the job executor functions that don't require external
%%% dependencies like running jobs. Focuses on internal helpers
%%% and state management.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor_real_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

%%====================================================================
%% Test Helper Functions
%%====================================================================

%% Test build_env_vars function
build_env_vars_test_() ->
    [
     {"build_env_vars with minimal job spec",
      fun() ->
          JobSpec = #{
              job_id => 123,
              num_cpus => 4,
              num_nodes => 1,
              time_limit => 3600
          },
          Env = flurm_job_executor:build_env_vars(JobSpec),
          ?assert(is_list(Env)),
          %% Check SLURM_JOB_ID is set
          ?assert(lists:any(fun({"SLURM_JOB_ID", "123"}) -> true; (_) -> false end, Env))
      end},
     {"build_env_vars includes all standard variables",
      fun() ->
          JobSpec = #{
              job_id => 42,
              num_cpus => 8,
              num_tasks => 4,
              num_nodes => 2,
              time_limit => 7200,
              partition => <<"compute">>,
              working_dir => <<"/home/user/work">>,
              user_id => 1000,
              group_id => 1000
          },
          Env = flurm_job_executor:build_env_vars(JobSpec),
          EnvMap = maps:from_list(Env),
          ?assertEqual("42", maps:get("SLURM_JOB_ID", EnvMap)),
          ?assertEqual("8", maps:get("SLURM_CPUS_PER_TASK", EnvMap, undefined)),
          ?assertEqual("4", maps:get("SLURM_NTASKS", EnvMap)),
          ?assertEqual("2", maps:get("SLURM_NNODES", EnvMap))
      end},
     {"build_env_vars preserves existing environment",
      fun() ->
          JobSpec = #{
              job_id => 1,
              num_cpus => 1,
              environment => #{
                  <<"MY_VAR">> => <<"my_value">>,
                  <<"OTHER_VAR">> => <<"other">>
              }
          },
          Env = flurm_job_executor:build_env_vars(JobSpec),
          EnvMap = maps:from_list(Env),
          ?assertEqual("my_value", maps:get("MY_VAR", EnvMap, undefined)),
          ?assertEqual("other", maps:get("OTHER_VAR", EnvMap, undefined))
      end}
    ].

%% Test script_to_command function
script_to_command_test_() ->
    [
     {"script_to_command with simple script",
      fun() ->
          Script = <<"#!/bin/bash\necho hello">>,
          {Cmd, TempFile} = flurm_job_executor:script_to_command(Script, 123),
          ?assert(is_list(Cmd)),
          ?assert(is_list(TempFile) orelse is_binary(TempFile)),
          %% Clean up temp file
          file:delete(TempFile)
      end},
     {"script_to_command creates executable temp file",
      fun() ->
          Script = <<"#!/bin/bash\necho test">>,
          {_Cmd, TempFile} = flurm_job_executor:script_to_command(Script, 456),
          ?assert(filelib:is_regular(TempFile)),
          %% Check it's executable
          {ok, #file_info{mode = Mode}} = file:read_file_info(TempFile),
          ?assert((Mode band 8#111) =/= 0),  %% Has execute permission
          file:delete(TempFile)
      end}
    ].

%% Test build_output_file function
build_output_file_test_() ->
    [
     {"build_output_file with default pattern",
      fun() ->
          Result = flurm_job_executor:build_output_file(123, <<>>, <<"/tmp">>),
          ?assertEqual(<<"/tmp/slurm-123.out">>, Result)
      end},
     {"build_output_file with explicit path",
      fun() ->
          Result = flurm_job_executor:build_output_file(123, <<"/var/log/job.out">>, <<"/tmp">>),
          ?assertEqual(<<"/var/log/job.out">>, Result)
      end},
     {"build_output_file with pattern substitution",
      fun() ->
          Result = flurm_job_executor:build_output_file(123, <<"job-%j.out">>, <<"/work">>),
          ?assertEqual(<<"/work/job-123.out">>, Result)
      end},
     {"build_output_file with relative path",
      fun() ->
          Result = flurm_job_executor:build_output_file(42, <<"output.log">>, <<"/home/user">>),
          ?assertEqual(<<"/home/user/output.log">>, Result)
      end}
    ].

%% Test parse_time_limit function
parse_time_limit_test_() ->
    [
     {"parse_time_limit with integer seconds",
      fun() ->
          ?assertEqual(3600, flurm_job_executor:parse_time_limit(3600))
      end},
     {"parse_time_limit with HH:MM:SS string",
      fun() ->
          ?assertEqual(3661, flurm_job_executor:parse_time_limit(<<"01:01:01">>))
      end},
     {"parse_time_limit with MM:SS string",
      fun() ->
          ?assertEqual(125, flurm_job_executor:parse_time_limit(<<"02:05">>))
      end},
     {"parse_time_limit with just seconds string",
      fun() ->
          ?assertEqual(300, flurm_job_executor:parse_time_limit(<<"300">>))
      end}
    ].

%% Test calculate_elapsed_time function
calculate_elapsed_time_test_() ->
    [
     {"calculate_elapsed_time returns seconds",
      fun() ->
          StartTime = erlang:system_time(second) - 100,
          Elapsed = flurm_job_executor:calculate_elapsed_time(StartTime),
          ?assert(Elapsed >= 100),
          ?assert(Elapsed < 110)  %% Allow some tolerance
      end},
     {"calculate_elapsed_time with recent start",
      fun() ->
          StartTime = erlang:system_time(second),
          Elapsed = flurm_job_executor:calculate_elapsed_time(StartTime),
          ?assert(Elapsed >= 0),
          ?assert(Elapsed < 5)
      end}
    ].

%% Test signal_name_to_number function
signal_name_to_number_test_() ->
    [
     {"signal_name_to_number for SIGTERM",
      ?_assertEqual(15, flurm_job_executor:signal_name_to_number(<<"SIGTERM">>))},
     {"signal_name_to_number for SIGKILL",
      ?_assertEqual(9, flurm_job_executor:signal_name_to_number(<<"SIGKILL">>))},
     {"signal_name_to_number for SIGHUP",
      ?_assertEqual(1, flurm_job_executor:signal_name_to_number(<<"SIGHUP">>))},
     {"signal_name_to_number for SIGINT",
      ?_assertEqual(2, flurm_job_executor:signal_name_to_number(<<"SIGINT">>))},
     {"signal_name_to_number for SIGSTOP",
      ?_assertEqual(19, flurm_job_executor:signal_name_to_number(<<"SIGSTOP">>))},
     {"signal_name_to_number for SIGCONT",
      ?_assertEqual(18, flurm_job_executor:signal_name_to_number(<<"SIGCONT">>))},
     {"signal_name_to_number for SIGUSR1",
      ?_assertEqual(10, flurm_job_executor:signal_name_to_number(<<"SIGUSR1">>))},
     {"signal_name_to_number for SIGUSR2",
      ?_assertEqual(12, flurm_job_executor:signal_name_to_number(<<"SIGUSR2">>))},
     {"signal_name_to_number for numeric string",
      ?_assertEqual(15, flurm_job_executor:signal_name_to_number(<<"15">>))},
     {"signal_name_to_number for unknown signal",
      ?_assertEqual(15, flurm_job_executor:signal_name_to_number(<<"UNKNOWN">>))}
    ].

%% Test merge_environments function
merge_environments_test_() ->
    [
     {"merge empty environments",
      fun() ->
          Result = flurm_job_executor:merge_environments(#{}, #{}),
          ?assertEqual(#{}, Result)
      end},
     {"merge with base only",
      fun() ->
          Base = #{<<"A">> => <<"1">>},
          Result = flurm_job_executor:merge_environments(Base, #{}),
          ?assertEqual(Base, Result)
      end},
     {"merge with override only",
      fun() ->
          Override = #{<<"B">> => <<"2">>},
          Result = flurm_job_executor:merge_environments(#{}, Override),
          ?assertEqual(Override, Result)
      end},
     {"override takes precedence",
      fun() ->
          Base = #{<<"A">> => <<"1">>, <<"B">> => <<"2">>},
          Override = #{<<"A">> => <<"override">>},
          Result = flurm_job_executor:merge_environments(Base, Override),
          ?assertEqual(<<"override">>, maps:get(<<"A">>, Result)),
          ?assertEqual(<<"2">>, maps:get(<<"B">>, Result))
      end}
    ].

%%====================================================================
%% State Record Tests
%%====================================================================

%% Test initial state creation
initial_state_test_() ->
    [
     {"create initial state with minimal job spec",
      fun() ->
          JobSpec = #{
              job_id => 1,
              script => <<"#!/bin/bash\necho test">>,
              num_cpus => 1
          },
          State = flurm_job_executor:create_initial_state(JobSpec),
          ?assert(is_map(State)),
          ?assertEqual(1, maps:get(job_id, State)),
          ?assertEqual(pending, maps:get(state, State))
      end}
    ].

%% Test state transitions
state_transition_test_() ->
    [
     {"transition from pending to running",
      fun() ->
          State = #{state => pending, start_time => undefined},
          NewState = flurm_job_executor:transition_state(State, running),
          ?assertEqual(running, maps:get(state, NewState)),
          ?assert(maps:get(start_time, NewState) =/= undefined)
      end},
     {"transition from running to completed",
      fun() ->
          State = #{state => running, exit_code => undefined},
          NewState = flurm_job_executor:transition_state(State, {completed, 0}),
          ?assertEqual(completed, maps:get(state, NewState)),
          ?assertEqual(0, maps:get(exit_code, NewState))
      end},
     {"transition from running to failed",
      fun() ->
          State = #{state => running, exit_code => undefined},
          NewState = flurm_job_executor:transition_state(State, {failed, 1}),
          ?assertEqual(failed, maps:get(state, NewState)),
          ?assertEqual(1, maps:get(exit_code, NewState))
      end}
    ].

%%====================================================================
%% Cleanup Function Tests
%%====================================================================

cleanup_temp_files_test_() ->
    [
     {"cleanup_temp_files with no files",
      fun() ->
          State = #{temp_files => []},
          ?assertEqual(ok, flurm_job_executor:cleanup_temp_files(State))
      end},
     {"cleanup_temp_files with actual temp file",
      fun() ->
          TempFile = "/tmp/flurm_test_" ++ integer_to_list(erlang:unique_integer([positive])),
          ok = file:write_file(TempFile, <<"test">>),
          State = #{temp_files => [TempFile]},
          ?assertEqual(ok, flurm_job_executor:cleanup_temp_files(State)),
          ?assertNot(filelib:is_regular(TempFile))
      end},
     {"cleanup_temp_files ignores missing files",
      fun() ->
          State = #{temp_files => ["/tmp/nonexistent_file_xyz123"]},
          ?assertEqual(ok, flurm_job_executor:cleanup_temp_files(State))
      end}
    ].
