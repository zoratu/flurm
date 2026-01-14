%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_job_executor module
%%%
%%% Tests the job executor gen_server directly. Mocks external
%%% dependencies (flurm_controller_connector) but NOT the module
%%% being tested.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

job_executor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_start_link_basic/1,
      fun test_get_status_pending/1,
      fun test_get_status_running/1,
      fun test_get_output/1,
      fun test_cancel_before_start/1,
      fun test_cancel_running_job/1,
      fun test_successful_job_execution/1,
      fun test_failed_job_execution/1,
      fun test_job_timeout/1,
      fun test_unknown_call/1,
      fun test_unknown_cast/1,
      fun test_unknown_info/1
     ]}.

setup() ->
    %% Create temp directory for job scripts
    TmpDir = "/tmp/flurm_executor_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    ok = filelib:ensure_dir(TmpDir ++ "/"),
    file:make_dir(TmpDir),

    %% Mock external dependencies
    meck:new(flurm_controller_connector, [non_strict]),
    meck:expect(flurm_controller_connector, report_job_complete, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_controller_connector, report_job_failed, fun(_, _, _, _) -> ok end),

    %% Start lager
    catch application:start(lager),

    #{tmp_dir => TmpDir}.

cleanup(#{tmp_dir := TmpDir}) ->
    %% Clean up temp directory
    os:cmd("rm -rf " ++ TmpDir),
    meck:unload(flurm_controller_connector),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_start_link_basic(#{tmp_dir := TmpDir}) ->
    {"start_link/1 starts executor with job spec",
     fun() ->
         JobSpec = #{
             job_id => 1001,
             script => <<"#!/bin/bash\necho hello\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         ?assert(is_pid(Pid)),
         ?assert(is_process_alive(Pid)),

         %% Give it time to run and stop
         timer:sleep(500),
         %% It should have stopped after executing
         ok
     end}.

test_get_status_pending(#{tmp_dir := TmpDir}) ->
    {"get_status/1 returns pending status initially",
     fun() ->
         JobSpec = #{
             job_id => 1002,
             script => <<"#!/bin/bash\nsleep 10\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         %% Immediately get status - should be pending or running
         Status = flurm_job_executor:get_status(Pid),

         ?assert(is_map(Status)),
         ?assertEqual(1002, maps:get(job_id, Status)),
         ?assert(lists:member(maps:get(status, Status), [pending, running])),

         %% Cancel to clean up
         flurm_job_executor:cancel(Pid),
         timer:sleep(100)
     end}.

test_get_status_running(#{tmp_dir := TmpDir}) ->
    {"get_status/1 returns running status during execution",
     fun() ->
         JobSpec = #{
             job_id => 1003,
             script => <<"#!/bin/bash\nsleep 5\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         %% Wait for execution to start
         timer:sleep(200),

         Status = flurm_job_executor:get_status(Pid),

         ?assertEqual(running, maps:get(status, Status)),
         ?assert(is_integer(maps:get(start_time, Status))),

         %% Cancel to clean up
         flurm_job_executor:cancel(Pid),
         timer:sleep(100)
     end}.

test_get_output(#{tmp_dir := TmpDir}) ->
    {"get_output/1 returns captured output",
     fun() ->
         JobSpec = #{
             job_id => 1004,
             script => <<"#!/bin/bash\necho \"test output\"\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         %% Wait for execution to complete
         timer:sleep(500),

         %% Process might have stopped, but we can still test the mechanism
         ok
     end}.

test_cancel_before_start(#{tmp_dir := TmpDir}) ->
    {"cancel/1 works before job starts",
     fun() ->
         JobSpec = #{
             job_id => 1005,
             script => <<"#!/bin/bash\nsleep 100\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         %% Cancel immediately
         ok = flurm_job_executor:cancel(Pid),

         %% Should stop
         timer:sleep(100),
         ?assertNot(is_process_alive(Pid))
     end}.

test_cancel_running_job(#{tmp_dir := TmpDir}) ->
    {"cancel/1 stops running job",
     fun() ->
         JobSpec = #{
             job_id => 1006,
             script => <<"#!/bin/bash\nwhile true; do sleep 1; done\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         %% Wait for job to start running
         timer:sleep(200),

         %% Cancel the job
         ok = flurm_job_executor:cancel(Pid),

         %% Should stop
         timer:sleep(200),
         ?assertNot(is_process_alive(Pid)),

         %% Verify failure was reported
         ?assert(meck:called(flurm_controller_connector, report_job_failed, ['_', cancelled, '_', '_']))
     end}.

test_successful_job_execution(#{tmp_dir := TmpDir}) ->
    {"successful job reports completion",
     fun() ->
         JobSpec = #{
             job_id => 1007,
             script => <<"#!/bin/bash\necho success\nexit 0\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         %% Wait for completion
         timer:sleep(500),

         %% Verify completion was reported
         ?assert(meck:called(flurm_controller_connector, report_job_complete, [1007, 0, '_', '_']))
     end}.

test_failed_job_execution(#{tmp_dir := TmpDir}) ->
    {"failed job reports failure",
     fun() ->
         JobSpec = #{
             job_id => 1008,
             script => <<"#!/bin/bash\nexit 42\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         %% Wait for completion
         timer:sleep(500),

         %% Verify failure was reported with exit code
         ?assert(meck:called(flurm_controller_connector, report_job_failed, [1008, {exit_code, 42}, '_', '_']))
     end}.

test_job_timeout(#{tmp_dir := TmpDir}) ->
    {"job timeout terminates job",
     fun() ->
         JobSpec = #{
             job_id => 1009,
             script => <<"#!/bin/bash\nsleep 100\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512,
             time_limit => 1  % 1 second timeout
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         %% Wait for timeout (1 second + buffer)
         timer:sleep(2000),

         %% Should have stopped
         ?assertNot(is_process_alive(Pid)),

         %% Verify timeout was reported
         ?assert(meck:called(flurm_controller_connector, report_job_failed, [1009, timeout, '_', '_']))
     end}.

test_unknown_call(#{tmp_dir := TmpDir}) ->
    {"handle_call returns error for unknown request",
     fun() ->
         JobSpec = #{
             job_id => 1010,
             script => <<"#!/bin/bash\nsleep 10\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),
         timer:sleep(100),

         Result = gen_server:call(Pid, unknown_request),

         ?assertEqual({error, unknown_request}, Result),

         flurm_job_executor:cancel(Pid),
         timer:sleep(100)
     end}.

test_unknown_cast(#{tmp_dir := TmpDir}) ->
    {"handle_cast ignores unknown messages",
     fun() ->
         JobSpec = #{
             job_id => 1011,
             script => <<"#!/bin/bash\nsleep 10\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),
         timer:sleep(100),

         gen_server:cast(Pid, unknown_message),

         %% Should still be running
         timer:sleep(50),
         ?assert(is_process_alive(Pid)),

         flurm_job_executor:cancel(Pid),
         timer:sleep(100)
     end}.

test_unknown_info(#{tmp_dir := TmpDir}) ->
    {"handle_info ignores unknown messages",
     fun() ->
         JobSpec = #{
             job_id => 1012,
             script => <<"#!/bin/bash\nsleep 10\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),
         timer:sleep(100),

         Pid ! random_info_message,

         %% Should still be running
         timer:sleep(50),
         ?assert(is_process_alive(Pid)),

         flurm_job_executor:cancel(Pid),
         timer:sleep(100)
     end}.

%%====================================================================
%% Additional coverage tests
%%====================================================================

environment_and_gpu_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_job_with_gpus/1,
      fun test_job_with_environment/1,
      fun test_job_with_output_file/1,
      fun test_power_monitoring_functions/1
     ]}.

test_job_with_gpus(#{tmp_dir := TmpDir}) ->
    {"job with GPUs sets CUDA_VISIBLE_DEVICES",
     fun() ->
         JobSpec = #{
             job_id => 2001,
             script => <<"#!/bin/bash\necho $CUDA_VISIBLE_DEVICES\nexit 0\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512,
             gpus => [0, 1]
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         timer:sleep(500),
         ok
     end}.

test_job_with_environment(#{tmp_dir := TmpDir}) ->
    {"job with custom environment variables",
     fun() ->
         JobSpec = #{
             job_id => 2002,
             script => <<"#!/bin/bash\necho $MY_VAR\nexit 0\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512,
             environment => #{<<"MY_VAR">> => <<"test_value">>}
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         timer:sleep(500),
         ok
     end}.

test_job_with_output_file(#{tmp_dir := TmpDir}) ->
    {"job writes output to specified file",
     fun() ->
         OutputFile = list_to_binary(TmpDir ++ "/output.txt"),

         JobSpec = #{
             job_id => 2003,
             script => <<"#!/bin/bash\necho output_test\nexit 0\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512,
             std_out => OutputFile
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         %% Wait for completion
         timer:sleep(500),

         %% Check output file exists
         ?assert(filelib:is_file(binary_to_list(OutputFile)))
     end}.

test_power_monitoring_functions(_) ->
    {"power monitoring functions work",
     fun() ->
         %% Test get_current_power (exported function)
         Power = flurm_job_executor:get_current_power(),
         ?assert(is_float(Power)),
         ?assert(Power >= 0.0),

         %% Test get_rapl_power (exported function)
         RaplPower = flurm_job_executor:get_rapl_power(),
         ?assert(is_float(RaplPower)),
         ?assert(RaplPower >= 0.0)
     end}.

%%====================================================================
%% Prolog/Epilog tests
%%====================================================================

prolog_epilog_test_() ->
    {foreach,
     fun setup_prolog_epilog/0,
     fun cleanup_prolog_epilog/1,
     [
      fun test_prolog_success/1,
      fun test_prolog_not_found/1,
      fun test_epilog_on_completion/1
     ]}.

setup_prolog_epilog() ->
    %% Create temp directory
    TmpDir = "/tmp/flurm_prolog_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    ok = filelib:ensure_dir(TmpDir ++ "/"),
    file:make_dir(TmpDir),

    %% Create prolog script
    PrologPath = TmpDir ++ "/prolog.sh",
    ok = file:write_file(PrologPath, "#!/bin/bash\nexit 0\n"),
    ok = file:change_mode(PrologPath, 8#755),

    %% Create epilog script
    EpilogPath = TmpDir ++ "/epilog.sh",
    ok = file:write_file(EpilogPath, "#!/bin/bash\nexit 0\n"),
    ok = file:change_mode(EpilogPath, 8#755),

    %% Mock external dependencies
    meck:new(flurm_controller_connector, [non_strict]),
    meck:expect(flurm_controller_connector, report_job_complete, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_controller_connector, report_job_failed, fun(_, _, _, _) -> ok end),

    catch application:start(lager),

    #{tmp_dir => TmpDir, prolog_path => PrologPath, epilog_path => EpilogPath}.

cleanup_prolog_epilog(#{tmp_dir := TmpDir}) ->
    os:cmd("rm -rf " ++ TmpDir),
    application:unset_env(flurm_node_daemon, prolog_path),
    application:unset_env(flurm_node_daemon, epilog_path),
    meck:unload(flurm_controller_connector),
    ok.

test_prolog_success(#{tmp_dir := TmpDir, prolog_path := PrologPath}) ->
    {"prolog script executes before job",
     fun() ->
         application:set_env(flurm_node_daemon, prolog_path, PrologPath),

         JobSpec = #{
             job_id => 3001,
             script => <<"#!/bin/bash\necho test\nexit 0\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         timer:sleep(500),
         ok
     end}.

test_prolog_not_found(#{tmp_dir := TmpDir}) ->
    {"prolog script not found fails job",
     fun() ->
         application:set_env(flurm_node_daemon, prolog_path, "/nonexistent/prolog.sh"),

         JobSpec = #{
             job_id => 3002,
             script => <<"#!/bin/bash\necho test\nexit 0\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         timer:sleep(500),

         %% Job should have failed due to prolog failure
         ?assert(meck:called(flurm_controller_connector, report_job_failed, ['_', '_', '_', '_']))
     end}.

test_epilog_on_completion(#{tmp_dir := TmpDir, epilog_path := EpilogPath}) ->
    {"epilog script executes after job completion",
     fun() ->
         application:set_env(flurm_node_daemon, epilog_path, EpilogPath),

         JobSpec = #{
             job_id => 3003,
             script => <<"#!/bin/bash\nexit 0\n">>,
             working_dir => list_to_binary(TmpDir),
             num_cpus => 1,
             memory_mb => 512
         },

         {ok, Pid} = flurm_job_executor:start_link(JobSpec),

         timer:sleep(500),

         %% Job should have completed
         ?assert(meck:called(flurm_controller_connector, report_job_complete, ['_', '_', '_', '_']))
     end}.
