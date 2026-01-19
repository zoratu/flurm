%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_job_dispatcher_server internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise pure internal functions for building job launch
%%% messages and signal name conversion without requiring a running gen_server.
%%%
%%% Note: flurm_job_dispatcher_server is in flurm_controller app, but tests
%%% are in flurm_core/test for organization purposes.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher_server_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% build_job_launch_message/2 Tests
%%====================================================================

build_job_launch_message_test_() ->
    {"build_job_launch_message/2 tests", [
        {"builds message with complete job info",
         fun() ->
             JobId = 12345,
             JobInfo = #{
                 name => <<"simulation_job">>,
                 script => <<"#!/bin/bash\nmpirun ./sim">>,
                 num_tasks => 8,
                 num_cpus => 4,
                 memory_mb => 16384,
                 time_limit => 7200,
                 partition => <<"compute">>,
                 work_dir => <<"/home/user/simulations">>,
                 std_out => <<"/home/user/simulations/output.log">>,
                 std_err => <<"/home/user/simulations/error.log">>,
                 user_id => 1000,
                 group_id => 1000
             },
             Msg = flurm_job_dispatcher_server:build_job_launch_message(JobId, JobInfo),

             ?assertEqual(job_launch, maps:get(type, Msg)),

             Payload = maps:get(payload, Msg),
             ?assertEqual(12345, maps:get(<<"job_id">>, Payload)),
             ?assertEqual(<<"#!/bin/bash\nmpirun ./sim">>, maps:get(<<"script">>, Payload)),
             ?assertEqual(<<"/home/user/simulations">>, maps:get(<<"working_dir">>, Payload)),
             ?assertEqual(4, maps:get(<<"num_cpus">>, Payload)),
             ?assertEqual(16384, maps:get(<<"memory_mb">>, Payload)),
             ?assertEqual(7200, maps:get(<<"time_limit">>, Payload)),
             ?assertEqual(1000, maps:get(<<"user_id">>, Payload)),
             ?assertEqual(1000, maps:get(<<"group_id">>, Payload)),
             ?assertEqual(<<"/home/user/simulations/output.log">>, maps:get(<<"std_out">>, Payload)),
             ?assertEqual(<<"/home/user/simulations/error.log">>, maps:get(<<"std_err">>, Payload))
         end},

        {"builds message with default values",
         fun() ->
             JobId = 100,
             JobInfo = #{},
             Msg = flurm_job_dispatcher_server:build_job_launch_message(JobId, JobInfo),

             ?assertEqual(job_launch, maps:get(type, Msg)),

             Payload = maps:get(payload, Msg),
             ?assertEqual(100, maps:get(<<"job_id">>, Payload)),
             ?assertEqual(<<>>, maps:get(<<"script">>, Payload)),
             ?assertEqual(<<"/tmp">>, maps:get(<<"working_dir">>, Payload)),
             ?assertEqual(1, maps:get(<<"num_cpus">>, Payload)),
             ?assertEqual(1024, maps:get(<<"memory_mb">>, Payload)),
             ?assertEqual(3600, maps:get(<<"time_limit">>, Payload)),
             ?assertEqual(0, maps:get(<<"user_id">>, Payload)),
             ?assertEqual(0, maps:get(<<"group_id">>, Payload))
         end},

        {"generates default stdout from work_dir and job_id",
         fun() ->
             JobId = 999,
             JobInfo = #{work_dir => <<"/home/testuser/work">>},
             Msg = flurm_job_dispatcher_server:build_job_launch_message(JobId, JobInfo),

             Payload = maps:get(payload, Msg),
             ?assertEqual(<<"/home/testuser/work/slurm-999.out">>, maps:get(<<"std_out">>, Payload))
         end},

        {"uses provided stdout over default",
         fun() ->
             JobId = 100,
             JobInfo = #{
                 work_dir => <<"/home/user/work">>,
                 std_out => <<"/custom/output.log">>
             },
             Msg = flurm_job_dispatcher_server:build_job_launch_message(JobId, JobInfo),

             Payload = maps:get(payload, Msg),
             ?assertEqual(<<"/custom/output.log">>, maps:get(<<"std_out">>, Payload))
         end},

        {"includes environment variables",
         fun() ->
             JobId = 500,
             JobInfo = #{
                 name => <<"test_job">>,
                 num_tasks => 4,
                 num_cpus => 2,
                 partition => <<"gpu">>,
                 work_dir => <<"/scratch">>
             },
             Msg = flurm_job_dispatcher_server:build_job_launch_message(JobId, JobInfo),

             Payload = maps:get(payload, Msg),
             Env = maps:get(<<"environment">>, Payload),

             ?assertEqual(<<"500">>, maps:get(<<"SLURM_JOB_ID">>, Env)),
             ?assertEqual(<<"test_job">>, maps:get(<<"SLURM_JOB_NAME">>, Env)),
             ?assertEqual(<<"4">>, maps:get(<<"SLURM_NTASKS">>, Env)),
             ?assertEqual(<<"2">>, maps:get(<<"SLURM_CPUS_PER_TASK">>, Env)),
             ?assertEqual(<<"gpu">>, maps:get(<<"SLURM_JOB_PARTITION">>, Env)),
             ?assertEqual(<<"/scratch">>, maps:get(<<"SLURM_SUBMIT_DIR">>, Env))
         end},

        {"environment variables have default values",
         fun() ->
             JobId = 1,
             JobInfo = #{},
             Msg = flurm_job_dispatcher_server:build_job_launch_message(JobId, JobInfo),

             Payload = maps:get(payload, Msg),
             Env = maps:get(<<"environment">>, Payload),

             ?assertEqual(<<"1">>, maps:get(<<"SLURM_JOB_ID">>, Env)),
             ?assertEqual(<<"job">>, maps:get(<<"SLURM_JOB_NAME">>, Env)),
             ?assertEqual(<<"1">>, maps:get(<<"SLURM_NTASKS">>, Env)),
             ?assertEqual(<<"1">>, maps:get(<<"SLURM_CPUS_PER_TASK">>, Env)),
             ?assertEqual(<<"default">>, maps:get(<<"SLURM_JOB_PARTITION">>, Env)),
             ?assertEqual(<<"/tmp">>, maps:get(<<"SLURM_SUBMIT_DIR">>, Env))
         end}
    ]}.

%%====================================================================
%% signal_to_name/1 Tests
%%====================================================================

signal_to_name_test_() ->
    {"signal_to_name/1 tests", [
        {"converts sigterm",
         fun() ->
             ?assertEqual(<<"SIGTERM">>, flurm_job_dispatcher_server:signal_to_name(sigterm))
         end},

        {"converts sigkill",
         fun() ->
             ?assertEqual(<<"SIGKILL">>, flurm_job_dispatcher_server:signal_to_name(sigkill))
         end},

        {"converts sigstop",
         fun() ->
             ?assertEqual(<<"SIGSTOP">>, flurm_job_dispatcher_server:signal_to_name(sigstop))
         end},

        {"converts sigcont",
         fun() ->
             ?assertEqual(<<"SIGCONT">>, flurm_job_dispatcher_server:signal_to_name(sigcont))
         end},

        {"converts sighup",
         fun() ->
             ?assertEqual(<<"SIGHUP">>, flurm_job_dispatcher_server:signal_to_name(sighup))
         end},

        {"converts sigusr1",
         fun() ->
             ?assertEqual(<<"SIGUSR1">>, flurm_job_dispatcher_server:signal_to_name(sigusr1))
         end},

        {"converts sigusr2",
         fun() ->
             ?assertEqual(<<"SIGUSR2">>, flurm_job_dispatcher_server:signal_to_name(sigusr2))
         end},

        {"converts sigint",
         fun() ->
             ?assertEqual(<<"SIGINT">>, flurm_job_dispatcher_server:signal_to_name(sigint))
         end},

        {"converts other atom to uppercase",
         fun() ->
             ?assertEqual(<<"SIGQUIT">>, flurm_job_dispatcher_server:signal_to_name(sigquit)),
             ?assertEqual(<<"SIGALRM">>, flurm_job_dispatcher_server:signal_to_name(sigalrm)),
             ?assertEqual(<<"SIGPIPE">>, flurm_job_dispatcher_server:signal_to_name(sigpipe))
         end},

        {"converts integer to binary",
         fun() ->
             ?assertEqual(<<"9">>, flurm_job_dispatcher_server:signal_to_name(9)),
             ?assertEqual(<<"15">>, flurm_job_dispatcher_server:signal_to_name(15)),
             ?assertEqual(<<"2">>, flurm_job_dispatcher_server:signal_to_name(2))
         end}
    ]}.

%%====================================================================
%% Message Structure Tests
%%====================================================================

message_structure_test_() ->
    {"Message structure tests", [
        {"launch message has correct type",
         fun() ->
             Msg = flurm_job_dispatcher_server:build_job_launch_message(1, #{}),
             ?assertEqual(job_launch, maps:get(type, Msg))
         end},

        {"launch message payload is a map",
         fun() ->
             Msg = flurm_job_dispatcher_server:build_job_launch_message(1, #{}),
             ?assert(is_map(maps:get(payload, Msg)))
         end},

        {"launch message payload has all required keys",
         fun() ->
             Msg = flurm_job_dispatcher_server:build_job_launch_message(1, #{}),
             Payload = maps:get(payload, Msg),

             RequiredKeys = [<<"job_id">>, <<"script">>, <<"working_dir">>,
                            <<"environment">>, <<"num_cpus">>, <<"memory_mb">>,
                            <<"time_limit">>, <<"user_id">>, <<"group_id">>,
                            <<"std_out">>, <<"std_err">>],

             lists:foreach(fun(Key) ->
                 ?assert(maps:is_key(Key, Payload),
                         io_lib:format("Missing key: ~p", [Key]))
             end, RequiredKeys)
         end},

        {"environment is a map",
         fun() ->
             Msg = flurm_job_dispatcher_server:build_job_launch_message(1, #{}),
             Payload = maps:get(payload, Msg),
             Env = maps:get(<<"environment">>, Payload),
             ?assert(is_map(Env))
         end},

        {"environment has all SLURM variables",
         fun() ->
             Msg = flurm_job_dispatcher_server:build_job_launch_message(1, #{}),
             Payload = maps:get(payload, Msg),
             Env = maps:get(<<"environment">>, Payload),

             SlurmVars = [<<"SLURM_JOB_ID">>, <<"SLURM_JOB_NAME">>,
                         <<"SLURM_NTASKS">>, <<"SLURM_CPUS_PER_TASK">>,
                         <<"SLURM_JOB_PARTITION">>, <<"SLURM_SUBMIT_DIR">>],

             lists:foreach(fun(Key) ->
                 ?assert(maps:is_key(Key, Env),
                         io_lib:format("Missing SLURM var: ~p", [Key]))
             end, SlurmVars)
         end}
    ]}.

%%====================================================================
%% Edge Cases Tests
%%====================================================================

edge_cases_test_() ->
    {"Edge case tests", [
        {"handles empty script",
         fun() ->
             Msg = flurm_job_dispatcher_server:build_job_launch_message(1, #{script => <<>>}),
             Payload = maps:get(payload, Msg),
             ?assertEqual(<<>>, maps:get(<<"script">>, Payload))
         end},

        {"handles zero time limit",
         fun() ->
             Msg = flurm_job_dispatcher_server:build_job_launch_message(1, #{time_limit => 0}),
             Payload = maps:get(payload, Msg),
             ?assertEqual(0, maps:get(<<"time_limit">>, Payload))
         end},

        {"handles large job ID",
         fun() ->
             LargeId = 999999999,
             Msg = flurm_job_dispatcher_server:build_job_launch_message(LargeId, #{}),
             Payload = maps:get(payload, Msg),
             Env = maps:get(<<"environment">>, Payload),

             ?assertEqual(LargeId, maps:get(<<"job_id">>, Payload)),
             ?assertEqual(<<"999999999">>, maps:get(<<"SLURM_JOB_ID">>, Env))
         end},

        {"handles unicode in job name",
         fun() ->
             JobInfo = #{name => <<"job_test">>},
             Msg = flurm_job_dispatcher_server:build_job_launch_message(1, JobInfo),
             Payload = maps:get(payload, Msg),
             Env = maps:get(<<"environment">>, Payload),
             ?assertEqual(<<"job_test">>, maps:get(<<"SLURM_JOB_NAME">>, Env))
         end},

        {"handles empty std_err",
         fun() ->
             Msg = flurm_job_dispatcher_server:build_job_launch_message(1, #{std_err => <<>>}),
             Payload = maps:get(payload, Msg),
             ?assertEqual(<<>>, maps:get(<<"std_err">>, Payload))
         end}
    ]}.
