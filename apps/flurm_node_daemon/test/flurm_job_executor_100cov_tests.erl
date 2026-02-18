%%%-------------------------------------------------------------------
%%% @doc FLURM Job Executor 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_job_executor module covering:
%%% - Job execution lifecycle
%%% - Cgroup setup and cleanup
%%% - GPU isolation
%%% - Environment building
%%% - Output handling
%%% - Prolog/Epilog execution
%%% - Energy monitoring
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_executor_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

meck_setup() ->
    catch meck:unload(flurm_controller_connector),
    catch meck:unload(file),
    catch meck:unload(filelib),
    catch meck:unload(lager),
    catch meck:unload(application),
    catch meck:unload(os),
    catch meck:unload(inet),
    ok.

meck_cleanup(_) ->
    meck:unload(),
    ok.

%%====================================================================
%% Create Script File Tests
%%====================================================================

create_script_file_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"create_script_file creates executable file",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             JobId = 12345,
             Script = <<"#!/bin/bash\necho hello">>,

             meck:expect(file, write_file, fun(Filename, Content) ->
                 ?assertEqual("/tmp/flurm_job_12345.sh", Filename),
                 ?assertEqual(Script, Content),
                 ok
             end),
             meck:expect(file, change_mode, fun(_, Mode) ->
                 ?assertEqual(8#755, Mode),
                 ok
             end),
             meck:expect(file, read_file_info, fun(_) ->
                 {ok, {file_info, 100, regular, read_write, 0, 0, 0, 0, 8#755, 0, 0, 0, 0, 0}}
             end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             Result = flurm_job_executor:create_script_file(JobId, Script),
             ?assertEqual("/tmp/flurm_job_12345.sh", Result)
         end},
        {"create_script_file logs error on read_file_info failure",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(file, write_file, fun(_, _) -> ok end),
             meck:expect(file, change_mode, fun(_, _) -> ok end),
             meck:expect(file, read_file_info, fun(_) -> {error, enoent} end),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, error, fun(_, _) -> ok end),

             flurm_job_executor:create_script_file(1, <<"test">>),
             ?assert(meck:called(lager, error, '_'))
         end}
     ]}.

%%====================================================================
%% Build Environment Tests
%%====================================================================

build_environment_test_() ->
    {"build_environment tests",
     [
        {"build_environment with integer CPUs",
         fun() ->
             State = {state, 1, <<>>, <<"/tmp">>, #{}, 4, 1024, 3600, undefined, pending,
                      undefined, <<>>, undefined, undefined, undefined, undefined, undefined, [],
                      0, undefined, undefined},
             Result = flurm_job_executor:build_environment(State),

             ?assert(lists:member({"FLURM_JOB_ID", "1"}, Result)),
             ?assert(lists:member({"FLURM_JOB_CPUS", "4"}, Result)),
             ?assert(lists:member({"FLURM_JOB_MEMORY_MB", "1024"}, Result)),
             ?assert(lists:member({"SLURM_JOB_ID", "1"}, Result)),
             ?assert(lists:member({"SLURM_CPUS_ON_NODE", "4"}, Result))
         end},
        {"build_environment with fractional CPUs",
         fun() ->
             State = {state, 2, <<>>, <<"/tmp">>, #{}, 0.5, 512, 1800, undefined, pending,
                      undefined, <<>>, undefined, undefined, undefined, undefined, undefined, [],
                      0, undefined, undefined},
             Result = flurm_job_executor:build_environment(State),

             %% Should have both exact and integer versions
             ?assert(lists:member({"FLURM_JOB_CPUS", "0.50"}, Result)),
             ?assert(lists:member({"FLURM_JOB_CPUS_INT", "1"}, Result))
         end},
        {"build_environment with GPUs",
         fun() ->
             State = {state, 3, <<>>, <<"/tmp">>, #{}, 2, 2048, 7200, undefined, pending,
                      undefined, <<>>, undefined, undefined, undefined, undefined, undefined,
                      [0, 1], 0, undefined, undefined},
             Result = flurm_job_executor:build_environment(State),

             ?assert(lists:member({"FLURM_GPUS", "0,1"}, Result)),
             ?assert(lists:member({"CUDA_VISIBLE_DEVICES", "0,1"}, Result)),
             ?assert(lists:member({"GPU_DEVICE_ORDINAL", "0,1"}, Result)),
             ?assert(lists:member({"SLURM_JOB_GPUS", "0,1"}, Result))
         end},
        {"build_environment with user environment",
         fun() ->
             UserEnv = #{<<"MY_VAR">> => <<"my_value">>, <<"ANOTHER">> => <<"test">>},
             State = {state, 4, <<>>, <<"/tmp">>, UserEnv, 1, 1024, 3600, undefined, pending,
                      undefined, <<>>, undefined, undefined, undefined, undefined, undefined, [],
                      0, undefined, undefined},
             Result = flurm_job_executor:build_environment(State),

             ?assert(lists:member({"MY_VAR", "my_value"}, Result)),
             ?assert(lists:member({"ANOTHER", "test"}, Result))
         end}
     ]}.

%%====================================================================
%% Normalize CPU Count Tests
%%====================================================================

normalize_cpu_count_test_() ->
    {"normalize_cpu_count tests",
     [
        {"integer CPU count",
         fun() ->
             ?assertEqual(4, flurm_job_executor:normalize_cpu_count(4)),
             ?assertEqual(1, flurm_job_executor:normalize_cpu_count(1)),
             ?assertEqual(128, flurm_job_executor:normalize_cpu_count(128))
         end},
        {"float CPU count",
         fun() ->
             ?assertEqual(0.5, flurm_job_executor:normalize_cpu_count(0.5)),
             ?assertEqual(1.5, flurm_job_executor:normalize_cpu_count(1.5))
         end},
        {"binary CPU count",
         fun() ->
             ?assertEqual(0.5, flurm_job_executor:normalize_cpu_count(<<"0.5">>)),
             ?assertEqual(4, flurm_job_executor:normalize_cpu_count(<<"4">>))
         end},
        {"string CPU count",
         fun() ->
             ?assertEqual(0.5, flurm_job_executor:normalize_cpu_count("0.5")),
             ?assertEqual(8, flurm_job_executor:normalize_cpu_count("8"))
         end},
        {"invalid CPU count defaults to 1",
         fun() ->
             ?assertEqual(1, flurm_job_executor:normalize_cpu_count(-1)),
             ?assertEqual(1, flurm_job_executor:normalize_cpu_count(0)),
             ?assertEqual(1, flurm_job_executor:normalize_cpu_count("invalid")),
             ?assertEqual(1, flurm_job_executor:normalize_cpu_count(undefined))
         end}
     ]}.

%%====================================================================
%% Format CPU Count Tests
%%====================================================================

format_cpu_count_test_() ->
    {"format_cpu_count tests",
     [
        {"format integer",
         fun() ->
             ?assertEqual("4", flurm_job_executor:format_cpu_count(4)),
             ?assertEqual("1", flurm_job_executor:format_cpu_count(1))
         end},
        {"format float",
         fun() ->
             ?assertEqual("0.50", flurm_job_executor:format_cpu_count(0.5)),
             ?assertEqual("1.25", flurm_job_executor:format_cpu_count(1.25))
         end},
        {"format whole number float",
         fun() ->
             ?assertEqual("2", flurm_job_executor:format_cpu_count(2.0))
         end}
     ]}.

%%====================================================================
%% Expand Output Path Tests
%%====================================================================

expand_output_path_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"expand_output_path expands %j",
         fun() ->
             meck:new(inet, [passthrough, unstick]),
             meck:expect(inet, gethostname, fun() -> {ok, "testhost"} end),

             Result = flurm_job_executor:expand_output_path("/output/job-%j.out", 42),
             ?assertEqual("/output/job-42.out", Result)
         end},
        {"expand_output_path expands %J",
         fun() ->
             meck:new(inet, [passthrough, unstick]),
             meck:expect(inet, gethostname, fun() -> {ok, "testhost"} end),

             Result = flurm_job_executor:expand_output_path("/output/job-%J.out", 100),
             ?assertEqual("/output/job-100.out", Result)
         end},
        {"expand_output_path expands %N",
         fun() ->
             meck:new(inet, [passthrough, unstick]),
             meck:expect(inet, gethostname, fun() -> {ok, "compute01"} end),

             Result = flurm_job_executor:expand_output_path("/output/%N/job.out", 1),
             ?assertEqual("/output/compute01/job.out", Result)
         end},
        {"expand_output_path expands %n",
         fun() ->
             meck:new(inet, [passthrough, unstick]),
             meck:expect(inet, gethostname, fun() -> {ok, "testhost"} end),

             Result = flurm_job_executor:expand_output_path("/output/node%n.out", 1),
             ?assertEqual("/output/node0.out", Result)
         end},
        {"expand_output_path expands %t",
         fun() ->
             meck:new(inet, [passthrough, unstick]),
             meck:expect(inet, gethostname, fun() -> {ok, "testhost"} end),

             Result = flurm_job_executor:expand_output_path("/output/task%t.out", 1),
             ?assertEqual("/output/task0.out", Result)
         end},
        {"expand_output_path expands %%",
         fun() ->
             meck:new(inet, [passthrough, unstick]),
             meck:expect(inet, gethostname, fun() -> {ok, "testhost"} end),

             Result = flurm_job_executor:expand_output_path("/output/file%%1.out", 1),
             ?assertEqual("/output/file%1.out", Result)
         end},
        {"expand_output_path handles multiple placeholders",
         fun() ->
             meck:new(inet, [passthrough, unstick]),
             meck:expect(inet, gethostname, fun() -> {ok, "node1"} end),

             Result = flurm_job_executor:expand_output_path("/data/%N/job%j_task%t.out", 99),
             ?assertEqual("/data/node1/job99_task0.out", Result)
         end}
     ]}.

%%====================================================================
%% Ensure Working Dir Tests
%%====================================================================

ensure_working_dir_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"ensure_working_dir returns existing dir",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:expect(filelib, is_dir, fun("/existing/path") -> true end),

             Result = flurm_job_executor:ensure_working_dir(<<"/existing/path">>, 1),
             ?assertEqual("/existing/path", Result)
         end},
        {"ensure_working_dir creates missing dir",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(filelib, is_dir, fun(_) -> false end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(file, make_dir, fun("/new/path") -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             Result = flurm_job_executor:ensure_working_dir(<<"/new/path">>, 1),
             ?assertEqual("/new/path", Result)
         end},
        {"ensure_working_dir handles eexist",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(filelib, is_dir, fun(_) -> false end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(file, make_dir, fun(_) -> {error, eexist} end),

             Result = flurm_job_executor:ensure_working_dir(<<"/race/condition">>, 1),
             ?assertEqual("/race/condition", Result)
         end},
        {"ensure_working_dir falls back to /tmp on make_dir error",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(filelib, is_dir, fun(_) -> false end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(file, make_dir, fun(_) -> {error, eacces} end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             Result = flurm_job_executor:ensure_working_dir(<<"/forbidden">>, 1),
             ?assertEqual("/tmp", Result)
         end},
        {"ensure_working_dir falls back to /tmp on ensure_dir error",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(filelib, is_dir, fun(_) -> false end),
             meck:expect(filelib, ensure_dir, fun(_) -> {error, enoent} end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             Result = flurm_job_executor:ensure_working_dir(<<"/no/parent">>, 1),
             ?assertEqual("/tmp", Result)
         end}
     ]}.

%%====================================================================
%% Setup Cgroup Tests
%%====================================================================

setup_cgroup_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"setup_cgroup returns undefined on non-Linux",
         fun() ->
             meck:new(os, [passthrough, unstick]),
             meck:expect(os, type, fun() -> {unix, darwin} end),

             Result = flurm_job_executor:setup_cgroup(1, 4, 1024),
             ?assertEqual(undefined, Result)
         end},
        {"setup_cgroup_v2 success",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(filelib, is_dir, fun("/sys/fs/cgroup/cgroup.controllers") -> true;
                                             (_) -> false end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(file, make_dir, fun(_) -> ok end),
             meck:expect(file, write_file, fun(_, _) -> ok end),

             Result = flurm_job_executor:setup_cgroup_v2("flurm_1", 4, 2048),
             ?assertMatch({ok, _}, Result)
         end},
        {"setup_cgroup_v2 with fractional CPUs",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(filelib, is_dir, fun("/sys/fs/cgroup/cgroup.controllers") -> true;
                                             (_) -> false end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(file, make_dir, fun(_) -> ok end),
             meck:expect(file, write_file, fun(Path, Content) ->
                 case Path of
                     Path when is_list(Path) ->
                         case string:find(Path, "cpu.max") of
                             nomatch -> ok;
                             _ ->
                                 %% For 0.5 CPUs: 50000 100000
                                 ?assert(string:find(binary_to_list(iolist_to_binary(Content)), "50000") =/= nomatch)
                         end,
                         ok
                 end
             end),

             Result = flurm_job_executor:setup_cgroup_v2("flurm_1", 0.5, 512),
             ?assertMatch({ok, _}, Result)
         end},
        {"setup_cgroup_v2 not available",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:expect(filelib, is_dir, fun("/sys/fs/cgroup/cgroup.controllers") -> false end),

             Result = flurm_job_executor:setup_cgroup_v2("flurm_1", 4, 1024),
             ?assertEqual({error, cgroup_v2_not_available}, Result)
         end},
        {"setup_cgroup_v1 success",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(filelib, is_dir, fun("/sys/fs/cgroup/memory") -> true;
                                             (_) -> false end),
             meck:expect(file, make_dir, fun(_) -> ok end),
             meck:expect(file, write_file, fun(_, _) -> ok end),

             Result = flurm_job_executor:setup_cgroup_v1("flurm_1", 4, 2048),
             ?assertMatch({ok, _}, Result)
         end},
        {"setup_cgroup_v1 with fractional CPUs",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(filelib, is_dir, fun("/sys/fs/cgroup/memory") -> true;
                                             (_) -> false end),
             meck:expect(file, make_dir, fun(_) -> ok end),
             meck:expect(file, write_file, fun(_, _) -> ok end),

             Result = flurm_job_executor:setup_cgroup_v1("flurm_1", 0.25, 512),
             ?assertMatch({ok, _}, Result)
         end},
        {"setup_cgroup_v1 not available",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:expect(filelib, is_dir, fun("/sys/fs/cgroup/memory") -> false end),

             Result = flurm_job_executor:setup_cgroup_v1("flurm_1", 4, 1024),
             ?assertEqual({error, cgroup_v1_not_available}, Result)
         end}
     ]}.

%%====================================================================
%% Cleanup Cgroup Tests
%%====================================================================

cleanup_cgroup_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"cleanup_cgroup with undefined",
         fun() ->
             Result = flurm_job_executor:cleanup_cgroup(undefined),
             ?assertEqual(ok, Result)
         end},
        {"cleanup_cgroup kills processes and removes dir",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:new(timer, [passthrough, unstick]),
             meck:new(os, [passthrough, unstick]),

             meck:expect(file, read_file, fun(_) -> {ok, <<"123\n456\n">>} end),
             meck:expect(file, del_dir, fun(_) -> ok end),
             meck:expect(os, cmd, fun(_) -> "" end),
             meck:expect(timer, sleep, fun(_) -> ok end),

             Result = flurm_job_executor:cleanup_cgroup("/sys/fs/cgroup/flurm_1"),
             ?assertEqual(ok, Result)
         end},
        {"cleanup_cgroup handles read error",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:new(timer, [passthrough, unstick]),

             meck:expect(file, read_file, fun(_) -> {error, enoent} end),
             meck:expect(file, del_dir, fun(_) -> ok end),
             meck:expect(timer, sleep, fun(_) -> ok end),

             Result = flurm_job_executor:cleanup_cgroup("/sys/fs/cgroup/flurm_1"),
             ?assertEqual(ok, Result)
         end}
     ]}.

%%====================================================================
%% GPU Isolation Tests
%%====================================================================

setup_gpu_isolation_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"setup_gpu_isolation with undefined cgroup",
         fun() ->
             Result = flurm_job_executor:setup_gpu_isolation(undefined, [0, 1]),
             ?assertEqual(ok, Result)
         end},
        {"setup_gpu_isolation with empty GPUs",
         fun() ->
             Result = flurm_job_executor:setup_gpu_isolation("/sys/fs/cgroup/test", []),
             ?assertEqual(ok, Result)
         end},
        {"setup_gpu_isolation on non-Linux",
         fun() ->
             meck:new(os, [passthrough, unstick]),
             meck:expect(os, type, fun() -> {unix, darwin} end),

             Result = flurm_job_executor:setup_gpu_isolation("/sys/fs/cgroup/test", [0]),
             ?assertEqual(ok, Result)
         end},
        {"setup_gpu_isolation_v2 with BPF not supported",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),

             meck:expect(filelib, is_dir, fun(_) -> true end),
             meck:expect(filelib, is_file, fun(_) -> false end),

             Result = flurm_job_executor:setup_gpu_isolation_v2("/sys/fs/cgroup/test", [0]),
             ?assertEqual({error, bpf_not_supported}, Result)
         end},
        {"setup_gpu_isolation_v2 cgroup not found",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:expect(filelib, is_dir, fun(_) -> false end),

             Result = flurm_job_executor:setup_gpu_isolation_v2("/nonexistent", [0]),
             ?assertEqual({error, cgroup_not_found}, Result)
         end},
        {"setup_gpu_isolation_v1 success",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(filelib, is_dir, fun("/sys/fs/cgroup/devices") -> true;
                                             (_) -> false end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(file, make_dir, fun(_) -> ok end),
             meck:expect(file, write_file, fun(_, _) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             Result = flurm_job_executor:setup_gpu_isolation_v1("/sys/fs/cgroup/memory/flurm_1", [0, 1]),
             ?assertEqual(ok, Result)
         end},
        {"setup_gpu_isolation_v1 no device cgroup",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:expect(filelib, is_dir, fun("/sys/fs/cgroup/devices") -> false end),

             Result = flurm_job_executor:setup_gpu_isolation_v1("/path", [0]),
             ?assertEqual({error, device_cgroup_not_available}, Result)
         end}
     ]}.

%%====================================================================
%% Allow Basic Devices Tests
%%====================================================================

allow_basic_devices_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"allow_basic_devices writes device rules",
         fun() ->
             meck:new(file, [passthrough, unstick]),

             DeviceRules = [],
             meck:expect(file, write_file, fun(_, Rule) ->
                 %% All rules should be for character devices
                 ?assert(binary:match(Rule, <<"c ">>) =/= nomatch),
                 ok
             end),

             Result = flurm_job_executor:allow_basic_devices("/sys/fs/cgroup/devices/test"),
             ?assertEqual(ok, Result)
         end}
     ]}.

%%====================================================================
%% Allow Nvidia Devices Tests
%%====================================================================

allow_nvidia_devices_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"allow_nvidia_devices writes GPU rules",
         fun() ->
             meck:new(file, [passthrough, unstick]),

             WriteCount = ets:new(write_count, [public]),
             ets:insert(WriteCount, {count, 0}),

             meck:expect(file, write_file, fun(_, _) ->
                 [{count, C}] = ets:lookup(WriteCount, count),
                 ets:insert(WriteCount, {count, C + 1}),
                 ok
             end),

             Result = flurm_job_executor:allow_nvidia_devices("/sys/fs/cgroup/devices/test", [0, 1, 2]),

             %% Should write: nvidia-uvm, nvidia-uvm-tools, nvidiactl, nvidia-modeset, + 3 GPU devices
             [{count, Count}] = ets:lookup(WriteCount, count),
             ?assertEqual(7, Count),
             ets:delete(WriteCount),
             ?assertEqual(ok, Result)
         end}
     ]}.

%%====================================================================
%% Write Output Files Tests
%%====================================================================

write_output_files_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"write_output_files with default path",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),
             meck:new(inet, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(inet, gethostname, fun() -> {ok, "host"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(file, write_file, fun(Path, Content) ->
                 ?assertEqual("/tmp/slurm-42.out", Path),
                 ?assertEqual(<<"test output">>, Content),
                 ok
             end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             flurm_job_executor:write_output_files(42, <<"test output">>, undefined, undefined)
         end},
        {"write_output_files with custom path",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),
             meck:new(inet, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(inet, gethostname, fun() -> {ok, "node1"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(file, write_file, fun(Path, _) ->
                 ?assertEqual("/data/job-42.out", Path),
                 ok
             end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             flurm_job_executor:write_output_files(42, <<"output">>, <<"/data/job-%j.out">>, undefined)
         end},
        {"write_output_files handles write error",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),
             meck:new(inet, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(inet, gethostname, fun() -> {ok, "host"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(file, write_file, fun(_, _) -> {error, eacces} end),
             meck:expect(lager, error, fun(_, _) -> ok end),

             flurm_job_executor:write_output_files(1, <<"output">>, undefined, undefined),
             ?assert(meck:called(lager, error, '_'))
         end},
        {"write_output_files handles ensure_dir error",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(inet, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(inet, gethostname, fun() -> {ok, "host"} end),
             meck:expect(filelib, ensure_dir, fun(_) -> {error, enoent} end),
             meck:expect(lager, error, fun(_, _) -> ok end),

             flurm_job_executor:write_output_files(1, <<"output">>, undefined, undefined),
             ?assert(meck:called(lager, error, '_'))
         end}
     ]}.

%%====================================================================
%% Report Completion Tests
%%====================================================================

report_completion_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"report_completion for completed job",
         fun() ->
             meck:new(flurm_controller_connector, [passthrough]),
             meck:expect(flurm_controller_connector, report_job_complete, fun(JobId, ExitCode, Output, Energy) ->
                 ?assertEqual(1, JobId),
                 ?assertEqual(0, ExitCode),
                 ?assertEqual(<<"output">>, Output),
                 ?assertEqual(1000, Energy),
                 ok
             end),

             State = {state, 1, <<>>, <<>>, #{}, 1, 1024, 3600, undefined, completed,
                      0, <<"output">>, undefined, undefined, undefined, undefined, undefined, [],
                      0, undefined, undefined},
             flurm_job_executor:report_completion(completed, 0, State, 1000)
         end},
        {"report_completion for cancelled job",
         fun() ->
             meck:new(flurm_controller_connector, [passthrough]),
             meck:expect(flurm_controller_connector, report_job_failed, fun(JobId, Reason, Output, Energy) ->
                 ?assertEqual(2, JobId),
                 ?assertEqual(cancelled, Reason),
                 ?assertEqual(<<>>, Output),
                 ?assertEqual(0, Energy),
                 ok
             end),

             State = {state, 2, <<>>, <<>>, #{}, 1, 1024, 3600, undefined, cancelled,
                      -15, <<>>, undefined, undefined, undefined, undefined, undefined, [],
                      0, undefined, undefined},
             flurm_job_executor:report_completion(cancelled, -15, State, 0)
         end},
        {"report_completion for timeout",
         fun() ->
             meck:new(flurm_controller_connector, [passthrough]),
             meck:expect(flurm_controller_connector, report_job_failed, fun(JobId, Reason, _, _) ->
                 ?assertEqual(3, JobId),
                 ?assertEqual(timeout, Reason),
                 ok
             end),

             State = {state, 3, <<>>, <<>>, #{}, 1, 1024, 60, undefined, timeout,
                      -14, <<>>, undefined, undefined, undefined, undefined, undefined, [],
                      0, undefined, undefined},
             flurm_job_executor:report_completion(timeout, -14, State, 500)
         end},
        {"report_completion for failed job",
         fun() ->
             meck:new(flurm_controller_connector, [passthrough]),
             meck:expect(flurm_controller_connector, report_job_failed, fun(JobId, Reason, _, _) ->
                 ?assertEqual(4, JobId),
                 ?assertMatch({exit_code, 1}, Reason),
                 ok
             end),

             State = {state, 4, <<>>, <<>>, #{}, 1, 1024, 3600, undefined, failed,
                      1, <<"error">>, undefined, undefined, undefined, undefined, undefined, [],
                      0, undefined, undefined},
             flurm_job_executor:report_completion(failed, 1, State, 100)
         end},
        {"report_completion/3 calls report_completion/4 with 0 energy",
         fun() ->
             meck:new(flurm_controller_connector, [passthrough]),
             meck:expect(flurm_controller_connector, report_job_complete, fun(_, _, _, Energy) ->
                 ?assertEqual(0, Energy),
                 ok
             end),

             State = {state, 1, <<>>, <<>>, #{}, 1, 1024, 3600, undefined, completed,
                      0, <<>>, undefined, undefined, undefined, undefined, undefined, [],
                      0, undefined, undefined},
             flurm_job_executor:report_completion(completed, 0, State)
         end}
     ]}.

%%====================================================================
%% Cancel Timeout Tests
%%====================================================================

cancel_timeout_test_() ->
    {"cancel_timeout tests",
     [
        {"cancel_timeout with undefined",
         fun() ->
             ?assertEqual(ok, flurm_job_executor:cancel_timeout(undefined))
         end},
        {"cancel_timeout with valid reference",
         fun() ->
             Ref = erlang:send_after(10000, self(), job_timeout),
             ?assertEqual(ok, flurm_job_executor:cancel_timeout(Ref)),
             %% Verify no timeout message
             receive
                 job_timeout -> error(should_not_receive)
             after 10 ->
                 ok
             end
         end},
        {"cancel_timeout flushes pending message",
         fun() ->
             self() ! job_timeout,
             Ref = make_ref(),
             ?assertEqual(ok, flurm_job_executor:cancel_timeout(Ref)),
             %% Message should be flushed
             receive
                 job_timeout -> error(should_not_receive)
             after 10 ->
                 ok
             end
         end}
     ]}.

%%====================================================================
%% Now MS Tests
%%====================================================================

now_ms_test_() ->
    {"now_ms tests",
     [
        {"now_ms returns milliseconds",
         fun() ->
             T1 = flurm_job_executor:now_ms(),
             timer:sleep(10),
             T2 = flurm_job_executor:now_ms(),
             ?assert(T2 > T1),
             ?assert(T2 - T1 >= 10)
         end}
     ]}.

%%====================================================================
%% Execute Prolog Tests
%%====================================================================

execute_prolog_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"execute_prolog with no prolog configured",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:expect(application, get_env, fun(flurm_node_daemon, prolog_path) -> undefined end),

             Result = flurm_job_executor:execute_prolog(1, []),
             ?assertEqual(ok, Result)
         end},
        {"execute_prolog with empty path",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:expect(application, get_env, fun(flurm_node_daemon, prolog_path) -> {ok, ""} end),

             Result = flurm_job_executor:execute_prolog(1, []),
             ?assertEqual(ok, Result)
         end}
     ]}.

%%====================================================================
%% Execute Epilog Tests
%%====================================================================

execute_epilog_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"execute_epilog with no epilog configured",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:expect(application, get_env, fun(flurm_node_daemon, epilog_path) -> undefined end),

             Result = flurm_job_executor:execute_epilog(1, 0, []),
             ?assertEqual(ok, Result)
         end}
     ]}.

%%====================================================================
%% Execute Script Tests
%%====================================================================

execute_script_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"execute_script with non-existent script",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(filelib, is_regular, fun(_) -> false end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             Result = flurm_job_executor:execute_script("/nonexistent", 1, [], "prolog"),
             ?assertMatch({error, {script_not_found, _}}, Result)
         end}
     ]}.

%%====================================================================
%% Read Current Energy Tests
%%====================================================================

read_current_energy_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"read_current_energy on non-Linux",
         fun() ->
             meck:new(os, [passthrough, unstick]),
             meck:expect(os, type, fun() -> {unix, darwin} end),

             Result = flurm_job_executor:read_current_energy(),
             ?assertEqual(0, Result)
         end}
     ]}.

%%====================================================================
%% Read RAPL Energy Tests
%%====================================================================

read_rapl_energy_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"read_rapl_energy with no powercap",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:expect(filelib, is_dir, fun("/sys/class/powercap") -> false end),

             Result = flurm_job_executor:read_rapl_energy(),
             ?assertEqual(0, Result)
         end},
        {"read_rapl_energy with powercap",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:new(file, [passthrough, unstick]),

             meck:expect(filelib, is_dir, fun("/sys/class/powercap") -> true end),
             meck:expect(file, list_dir, fun("/sys/class/powercap") ->
                 {ok, ["intel-rapl:0", "intel-rapl:1", "other"]}
             end),
             meck:expect(file, read_file, fun(Path) ->
                 case string:find(Path, "energy_uj") of
                     nomatch -> {error, enoent};
                     _ -> {ok, <<"1000000\n">>}
                 end
             end),

             Result = flurm_job_executor:read_rapl_energy(),
             ?assertEqual(2000000, Result)  %% Sum of 2 domains
         end}
     ]}.

%%====================================================================
%% Sum RAPL Energies Tests
%%====================================================================

sum_rapl_energies_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"sum_rapl_energies with empty list",
         fun() ->
             Result = flurm_job_executor:sum_rapl_energies("/sys/class/powercap", [], 0),
             ?assertEqual(0, Result)
         end},
        {"sum_rapl_energies with valid domains",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun(_) -> {ok, <<"500000\n">>} end),

             Result = flurm_job_executor:sum_rapl_energies("/sys/class/powercap",
                                                            ["intel-rapl:0", "intel-rapl:1"], 0),
             ?assertEqual(1000000, Result)
         end},
        {"sum_rapl_energies handles read errors",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun(_) -> {error, enoent} end),

             Result = flurm_job_executor:sum_rapl_energies("/sys/class/powercap",
                                                            ["intel-rapl:0"], 100),
             ?assertEqual(100, Result)
         end},
        {"sum_rapl_energies handles parse errors",
         fun() ->
             meck:new(file, [passthrough, unstick]),
             meck:expect(file, read_file, fun(_) -> {ok, <<"invalid\n">>} end),

             Result = flurm_job_executor:sum_rapl_energies("/sys/class/powercap",
                                                            ["intel-rapl:0"], 50),
             ?assertEqual(50, Result)
         end}
     ]}.

%%====================================================================
%% Get Current Power Tests
%%====================================================================

get_current_power_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"get_current_power on non-Linux",
         fun() ->
             meck:new(os, [passthrough, unstick]),
             meck:expect(os, type, fun() -> {win32, nt} end),

             Result = flurm_job_executor:get_current_power(),
             ?assertEqual(0.0, Result)
         end}
     ]}.

%%====================================================================
%% Get RAPL Power Tests
%%====================================================================

get_rapl_power_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"get_rapl_power with no powercap",
         fun() ->
             meck:new(filelib, [passthrough, unstick]),
             meck:expect(filelib, is_dir, fun("/sys/class/powercap") -> false end),

             Result = flurm_job_executor:get_rapl_power(),
             ?assertEqual(0.0, Result)
         end}
     ]}.

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"get_status returns job info",
         fun() ->
             meck:new(lager, [passthrough]),
             meck:expect(lager, info, fun(_, _) -> ok end),

             JobSpec = #{
                 job_id => 42,
                 script => <<"#!/bin/bash\necho test">>,
                 working_dir => <<"/tmp">>,
                 num_cpus => 4,
                 memory_mb => 1024
             },

             {ok, Pid} = gen_server:start(flurm_job_executor, JobSpec, []),
             Status = flurm_job_executor:get_status(Pid),

             ?assertEqual(42, maps:get(job_id, Status)),
             ?assertEqual(pending, maps:get(status, Status)),

             %% Stop the executor
             exit(Pid, kill),
             timer:sleep(50)
         end},
        {"get_output returns accumulated output",
         fun() ->
             meck:new(lager, [passthrough]),
             meck:expect(lager, info, fun(_, _) -> ok end),

             JobSpec = #{
                 job_id => 43,
                 script => <<"echo test">>,
                 num_cpus => 1,
                 memory_mb => 512
             },

             {ok, Pid} = gen_server:start(flurm_job_executor, JobSpec, []),
             Output = flurm_job_executor:get_output(Pid),

             ?assertEqual(<<>>, Output),

             exit(Pid, kill),
             timer:sleep(50)
         end},
        {"cancel sends cancel cast",
         fun() ->
             meck:new(lager, [passthrough]),
             meck:new(flurm_controller_connector, [passthrough]),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(flurm_controller_connector, report_job_failed, fun(_, _, _, _) -> ok end),

             JobSpec = #{
                 job_id => 44,
                 script => <<"sleep 100">>,
                 num_cpus => 1,
                 memory_mb => 512
             },

             {ok, Pid} = gen_server:start(flurm_job_executor, JobSpec, []),

             %% Give it time to start
             timer:sleep(50),

             %% Cancel should not crash
             flurm_job_executor:cancel(Pid),

             timer:sleep(100),
             ?assertNot(is_process_alive(Pid))
         end}
     ]}.
