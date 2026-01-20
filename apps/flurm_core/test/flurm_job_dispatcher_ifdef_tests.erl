%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_job_dispatcher internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise the facade module's internal functions and
%%% stub mode behavior when the server is not running.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% server_running/0 Tests
%%====================================================================

server_running_test_() ->
    {"server_running/0 tests", [
        {"returns false when server not registered",
         fun() ->
             %% Ensure server is not running
             case whereis(flurm_job_dispatcher_server) of
                 undefined -> ok;
                 Pid ->
                     %% If running, unregister for test
                     exit(Pid, kill),
                     flurm_test_utils:wait_for_death(Pid)
             end,
             ?assertEqual(false, flurm_job_dispatcher:server_running())
         end},

        {"returns false for undefined process",
         fun() ->
             %% This is the typical test scenario
             ?assertEqual(false, flurm_job_dispatcher:server_running())
         end}
    ]}.

%%====================================================================
%% Stub Mode Tests (API functions when server not running)
%%====================================================================

stub_mode_test_() ->
    {"Stub mode tests (server not running)", [
        {"dispatch_job returns ok in stub mode",
         fun() ->
             %% Make sure server is not running
             ?assertEqual(false, flurm_job_dispatcher:server_running()),

             %% Test dispatch_job in stub mode
             JobInfo = #{
                 allocated_nodes => [<<"node1">>, <<"node2">>],
                 name => <<"test_job">>,
                 script => <<"#!/bin/bash\necho test">>
             },
             ?assertEqual(ok, flurm_job_dispatcher:dispatch_job(1, JobInfo))
         end},

        {"cancel_job returns ok in stub mode",
         fun() ->
             ?assertEqual(false, flurm_job_dispatcher:server_running()),
             ?assertEqual(ok, flurm_job_dispatcher:cancel_job(1, [<<"node1">>]))
         end},

        {"preempt_job returns ok in stub mode",
         fun() ->
             ?assertEqual(false, flurm_job_dispatcher:server_running()),
             ?assertEqual(ok, flurm_job_dispatcher:preempt_job(1, #{signal => sigterm}))
         end},

        {"requeue_job returns ok in stub mode",
         fun() ->
             ?assertEqual(false, flurm_job_dispatcher:server_running()),
             ?assertEqual(ok, flurm_job_dispatcher:requeue_job(1))
         end},

        {"drain_node returns ok in stub mode",
         fun() ->
             ?assertEqual(false, flurm_job_dispatcher:server_running()),
             ?assertEqual(ok, flurm_job_dispatcher:drain_node(<<"node1">>))
         end},

        {"resume_node returns ok in stub mode",
         fun() ->
             ?assertEqual(false, flurm_job_dispatcher:server_running()),
             ?assertEqual(ok, flurm_job_dispatcher:resume_node(<<"node1">>))
         end}
    ]}.

%%====================================================================
%% API Type Tests
%%====================================================================

api_types_test_() ->
    {"API type validation tests", [
        {"dispatch_job accepts integer job_id and map",
         fun() ->
             ?assertEqual(ok, flurm_job_dispatcher:dispatch_job(123, #{}))
         end},

        {"cancel_job accepts integer and node list",
         fun() ->
             ?assertEqual(ok, flurm_job_dispatcher:cancel_job(456, []))
         end},

        {"preempt_job accepts integer and options map",
         fun() ->
             ?assertEqual(ok, flurm_job_dispatcher:preempt_job(789, #{}))
         end},

        {"drain_node accepts binary hostname",
         fun() ->
             ?assertEqual(ok, flurm_job_dispatcher:drain_node(<<"compute001">>))
         end},

        {"resume_node accepts binary hostname",
         fun() ->
             ?assertEqual(ok, flurm_job_dispatcher:resume_node(<<"compute001">>))
         end}
    ]}.

%%====================================================================
%% Preempt Options Tests
%%====================================================================

preempt_options_test_() ->
    {"preempt_job options tests", [
        {"handles sigterm signal option",
         fun() ->
             ?assertEqual(ok, flurm_job_dispatcher:preempt_job(1, #{signal => sigterm}))
         end},

        {"handles sigkill signal option",
         fun() ->
             ?assertEqual(ok, flurm_job_dispatcher:preempt_job(1, #{signal => sigkill}))
         end},

        {"handles sigstop signal option",
         fun() ->
             ?assertEqual(ok, flurm_job_dispatcher:preempt_job(1, #{signal => sigstop}))
         end},

        {"handles sigcont signal option",
         fun() ->
             ?assertEqual(ok, flurm_job_dispatcher:preempt_job(1, #{signal => sigcont}))
         end},

        {"handles empty options (default signal)",
         fun() ->
             ?assertEqual(ok, flurm_job_dispatcher:preempt_job(1, #{}))
         end}
    ]}.

%%====================================================================
%% Dispatch Job Info Tests
%%====================================================================

dispatch_job_info_test_() ->
    {"dispatch_job job info tests", [
        {"handles complete job info",
         fun() ->
             JobInfo = #{
                 allocated_nodes => [<<"node1">>, <<"node2">>],
                 name => <<"simulation_job">>,
                 script => <<"#!/bin/bash\nmpirun ./sim">>,
                 num_cpus => 16,
                 memory_mb => 8192,
                 time_limit => 7200,
                 work_dir => <<"/home/user/simulations">>,
                 std_out => <<"/home/user/simulations/output.log">>
             },
             ?assertEqual(ok, flurm_job_dispatcher:dispatch_job(100, JobInfo))
         end},

        {"handles minimal job info",
         fun() ->
             JobInfo = #{},
             ?assertEqual(ok, flurm_job_dispatcher:dispatch_job(101, JobInfo))
         end},

        {"handles job info with no allocated nodes",
         fun() ->
             JobInfo = #{allocated_nodes => []},
             ?assertEqual(ok, flurm_job_dispatcher:dispatch_job(102, JobInfo))
         end}
    ]}.
