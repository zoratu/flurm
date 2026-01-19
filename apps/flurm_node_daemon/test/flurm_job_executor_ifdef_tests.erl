%%%-------------------------------------------------------------------
%%% @doc Test suite for flurm_job_executor internal functions
%%%
%%% These tests exercise the internal helper functions exported via
%%% -ifdef(TEST) to achieve code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup
%%====================================================================

setup() ->
    %% Ensure lager is available for logging
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

job_executor_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"now_ms returns milliseconds", fun now_ms_test/0},
      {"cancel_timeout with undefined", fun cancel_timeout_undefined_test/0},
      {"cancel_timeout with timer ref", fun cancel_timeout_ref_test/0},
      {"build_environment basic", fun build_environment_basic_test/0},
      {"build_environment with GPUs", fun build_environment_gpu_test/0},
      {"create_script_file creates and makes executable", fun create_script_file_test/0},
      {"cleanup_cgroup undefined", fun cleanup_cgroup_undefined_test/0},
      {"read_current_energy returns integer", fun read_current_energy_test/0},
      {"read_rapl_energy returns integer", fun read_rapl_energy_test/0},
      {"sum_rapl_energies empty list", fun sum_rapl_energies_empty_test/0},
      {"sum_rapl_energies with nonexistent paths", fun sum_rapl_energies_nonexistent_test/0},
      {"get_current_power returns float", fun get_current_power_test/0},
      {"get_rapl_power returns float", fun get_rapl_power_test/0},
      {"write_output_files basic", fun write_output_files_test/0},
      {"execute_prolog without config", fun execute_prolog_no_config_test/0},
      {"execute_epilog without config", fun execute_epilog_no_config_test/0},
      {"execute_script with nonexistent script", fun execute_script_not_found_test/0},
      {"setup_cgroup on non-linux", fun setup_cgroup_non_linux_test/0}
     ]}.

%%====================================================================
%% Tests
%%====================================================================

now_ms_test() ->
    T1 = flurm_job_executor:now_ms(),
    ?assert(is_integer(T1)),
    ?assert(T1 > 0),
    timer:sleep(10),
    T2 = flurm_job_executor:now_ms(),
    ?assert(T2 >= T1).

cancel_timeout_undefined_test() ->
    ?assertEqual(ok, flurm_job_executor:cancel_timeout(undefined)).

cancel_timeout_ref_test() ->
    %% Create a timer that won't fire yet
    Ref = erlang:send_after(60000, self(), job_timeout),
    ?assertEqual(ok, flurm_job_executor:cancel_timeout(Ref)).

build_environment_basic_test() ->
    %% Create a minimal state record for testing
    %% We need to access the record definition, so use a map-like approach
    State = make_test_state(123, #{}, 4, 2048, []),
    EnvList = flurm_job_executor:build_environment(State),

    %% Check for expected environment variables
    ?assert(is_list(EnvList)),
    ?assertMatch({_, "123"}, lists:keyfind("FLURM_JOB_ID", 1, EnvList)),
    ?assertMatch({_, "4"}, lists:keyfind("FLURM_JOB_CPUS", 1, EnvList)),
    ?assertMatch({_, "2048"}, lists:keyfind("FLURM_JOB_MEMORY_MB", 1, EnvList)),
    %% SLURM compatibility vars
    ?assertMatch({_, "123"}, lists:keyfind("SLURM_JOB_ID", 1, EnvList)),
    ?assertMatch({_, "4"}, lists:keyfind("SLURM_CPUS_ON_NODE", 1, EnvList)).

build_environment_gpu_test() ->
    State = make_test_state(456, #{}, 2, 1024, [0, 1, 2]),
    EnvList = flurm_job_executor:build_environment(State),

    %% Check GPU environment variables
    ?assertMatch({_, "0,1,2"}, lists:keyfind("FLURM_GPUS", 1, EnvList)),
    ?assertMatch({_, "0,1,2"}, lists:keyfind("CUDA_VISIBLE_DEVICES", 1, EnvList)),
    ?assertMatch({_, "0,1,2"}, lists:keyfind("SLURM_JOB_GPUS", 1, EnvList)).

create_script_file_test() ->
    JobId = erlang:unique_integer([positive]),
    Script = <<"#!/bin/bash\necho 'test'\n">>,

    Filename = flurm_job_executor:create_script_file(JobId, Script),

    ?assert(filelib:is_regular(Filename)),
    {ok, FileInfo} = file:read_file_info(Filename),
    Mode = element(8, FileInfo),
    ?assert((Mode band 8#111) =/= 0),  % Check executable bits

    %% Cleanup
    file:delete(Filename).

cleanup_cgroup_undefined_test() ->
    ?assertEqual(ok, flurm_job_executor:cleanup_cgroup(undefined)).

read_current_energy_test() ->
    Energy = flurm_job_executor:read_current_energy(),
    ?assert(is_integer(Energy)),
    ?assert(Energy >= 0).

read_rapl_energy_test() ->
    Energy = flurm_job_executor:read_rapl_energy(),
    ?assert(is_integer(Energy)),
    ?assert(Energy >= 0).

sum_rapl_energies_empty_test() ->
    Result = flurm_job_executor:sum_rapl_energies("/nonexistent", [], 0),
    ?assertEqual(0, Result).

sum_rapl_energies_nonexistent_test() ->
    Result = flurm_job_executor:sum_rapl_energies("/nonexistent", ["domain1"], 0),
    ?assertEqual(0, Result).

get_current_power_test() ->
    Power = flurm_job_executor:get_current_power(),
    ?assert(is_float(Power)),
    ?assert(Power >= 0.0).

get_rapl_power_test() ->
    Power = flurm_job_executor:get_rapl_power(),
    ?assert(is_float(Power)),
    ?assert(Power >= 0.0).

write_output_files_test() ->
    JobId = erlang:unique_integer([positive]),
    Output = <<"Test output content">>,
    TmpFile = "/tmp/flurm_test_output_" ++ integer_to_list(JobId) ++ ".out",

    %% Call write_output_files - it doesn't return a meaningful value
    flurm_job_executor:write_output_files(JobId, Output, list_to_binary(TmpFile), undefined),

    %% Check file was created
    ?assert(filelib:is_regular(TmpFile)),
    {ok, Content} = file:read_file(TmpFile),
    ?assertEqual(Output, Content),

    %% Cleanup
    file:delete(TmpFile).

execute_prolog_no_config_test() ->
    %% Without prolog_path configured, should return ok
    application:unset_env(flurm_node_daemon, prolog_path),
    Result = flurm_job_executor:execute_prolog(999, []),
    ?assertEqual(ok, Result).

execute_epilog_no_config_test() ->
    %% Without epilog_path configured, should return ok
    application:unset_env(flurm_node_daemon, epilog_path),
    Result = flurm_job_executor:execute_epilog(999, 0, []),
    ?assertEqual(ok, Result).

execute_script_not_found_test() ->
    Result = flurm_job_executor:execute_script("/nonexistent/script.sh", 1, [], "test"),
    ?assertMatch({error, {script_not_found, _}}, Result).

setup_cgroup_non_linux_test() ->
    %% On non-Linux (like macOS), should return undefined
    case os:type() of
        {unix, linux} ->
            %% On Linux, result depends on cgroup availability
            Result = flurm_job_executor:setup_cgroup(99999, 1, 256),
            ?assert(Result =:= undefined orelse is_list(Result));
        _ ->
            Result = flurm_job_executor:setup_cgroup(99999, 1, 256),
            ?assertEqual(undefined, Result)
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

%% Create a test state record (must match the -record(state, ...) definition)
make_test_state(JobId, Environment, NumCpus, MemoryMB, GPUs) ->
    {state,
     JobId,           % job_id
     <<>>,            % script
     <<"/tmp">>,      % working_dir
     Environment,     % environment
     NumCpus,         % num_cpus
     MemoryMB,        % memory_mb
     undefined,       % time_limit
     undefined,       % port
     pending,         % status
     undefined,       % exit_code
     <<>>,            % output
     undefined,       % start_time
     undefined,       % end_time
     undefined,       % cgroup_path
     undefined,       % script_path
     undefined,       % timeout_ref
     undefined,       % std_out
     undefined,       % std_err
     GPUs,            % gpus
     0,               % energy_start
     undefined,       % prolog_status
     undefined        % epilog_status
    }.
