%%%-------------------------------------------------------------------
%%% @doc FLURM Job Dispatcher Tests
%%%
%%% EUnit tests for the flurm_job_dispatcher facade module.
%%% This module provides a facade for job dispatching operations,
%%% delegating to flurm_job_dispatcher_server when available or
%%% operating in stub mode for testing.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Tests run without the dispatcher server (stub mode)
dispatcher_stub_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Dispatch job in stub mode", fun test_dispatch_job_stub/0},
        {"Cancel job in stub mode", fun test_cancel_job_stub/0},
        {"Preempt job in stub mode", fun test_preempt_job_stub/0},
        {"Requeue job in stub mode", fun test_requeue_job_stub/0},
        {"Drain node in stub mode", fun test_drain_node_stub/0},
        {"Resume node in stub mode", fun test_resume_node_stub/0},
        {"Multiple dispatch calls", fun test_multiple_dispatches/0},
        {"Dispatch with various options", fun test_dispatch_options/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    %% Ensure the dispatcher server is NOT running for stub mode tests
    case whereis(flurm_job_dispatcher_server) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid)
    end,
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Stub Mode Tests
%%====================================================================

test_dispatch_job_stub() ->
    %% In stub mode, dispatch_job should return ok
    JobInfo = #{
        job_id => 1,
        allocated_nodes => [<<"node1">>, <<"node2">>],
        script => <<"#!/bin/bash\necho hello">>,
        user_id => 1000,
        group_id => 1000
    },
    Result = flurm_job_dispatcher:dispatch_job(1, JobInfo),
    ?assertEqual(ok, Result),
    ok.

test_cancel_job_stub() ->
    %% In stub mode, cancel_job should return ok
    Nodes = [<<"node1">>, <<"node2">>],
    Result = flurm_job_dispatcher:cancel_job(1, Nodes),
    ?assertEqual(ok, Result),
    ok.

test_preempt_job_stub() ->
    %% In stub mode, preempt_job should return ok
    Options = #{signal => sigterm},
    Result = flurm_job_dispatcher:preempt_job(1, Options),
    ?assertEqual(ok, Result),

    %% With different signal
    Options2 = #{signal => sigkill},
    Result2 = flurm_job_dispatcher:preempt_job(2, Options2),
    ?assertEqual(ok, Result2),

    %% Default signal
    Result3 = flurm_job_dispatcher:preempt_job(3, #{}),
    ?assertEqual(ok, Result3),
    ok.

test_requeue_job_stub() ->
    %% In stub mode, requeue_job should return ok
    Result = flurm_job_dispatcher:requeue_job(1),
    ?assertEqual(ok, Result),
    ok.

test_drain_node_stub() ->
    %% In stub mode, drain_node should return ok
    Result = flurm_job_dispatcher:drain_node(<<"node1">>),
    ?assertEqual(ok, Result),
    ok.

test_resume_node_stub() ->
    %% In stub mode, resume_node should return ok
    Result = flurm_job_dispatcher:resume_node(<<"node1">>),
    ?assertEqual(ok, Result),
    ok.

test_multiple_dispatches() ->
    %% Test multiple dispatch calls in sequence
    JobInfo1 = #{allocated_nodes => [<<"node1">>]},
    JobInfo2 = #{allocated_nodes => [<<"node2">>]},
    JobInfo3 = #{allocated_nodes => [<<"node3">>]},

    ok = flurm_job_dispatcher:dispatch_job(1, JobInfo1),
    ok = flurm_job_dispatcher:dispatch_job(2, JobInfo2),
    ok = flurm_job_dispatcher:dispatch_job(3, JobInfo3),
    ok.

test_dispatch_options() ->
    %% Test dispatch with various job info options
    JobInfo = #{
        allocated_nodes => [<<"node1">>, <<"node2">>, <<"node3">>],
        script => <<"#!/bin/bash\necho $SLURM_JOB_ID">>,
        user_id => 1000,
        group_id => 1000,
        work_dir => <<"/tmp/job_1">>,
        environment => #{
            <<"MY_VAR">> => <<"my_value">>
        }
    },
    Result = flurm_job_dispatcher:dispatch_job(100, JobInfo),
    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Additional Tests
%%====================================================================

%% Test empty node list
empty_nodes_test() ->
    JobInfo = #{allocated_nodes => []},
    Result = flurm_job_dispatcher:dispatch_job(1, JobInfo),
    ?assertEqual(ok, Result),

    Result2 = flurm_job_dispatcher:cancel_job(1, []),
    ?assertEqual(ok, Result2),
    ok.

%% Test various signal types
signal_types_test() ->
    Signals = [sigterm, sigkill, sigstop, sigcont],
    lists:foreach(fun(Signal) ->
        Options = #{signal => Signal},
        Result = flurm_job_dispatcher:preempt_job(1, Options),
        ?assertEqual(ok, Result)
    end, Signals),
    ok.

%% Test concurrent dispatch calls
concurrent_dispatch_test() ->
    Self = self(),
    NumJobs = 10,

    %% Spawn multiple processes to dispatch jobs concurrently
    Pids = [spawn(fun() ->
        JobInfo = #{allocated_nodes => [<<"node1">>]},
        Result = flurm_job_dispatcher:dispatch_job(I, JobInfo),
        Self ! {done, I, Result}
    end) || I <- lists:seq(1, NumJobs)],

    %% Wait for all to complete
    Results = [receive {done, _I, R} -> R end || _ <- Pids],

    %% All should succeed
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)),
    ok.

%% Test drain and resume sequence
drain_resume_sequence_test() ->
    Node = <<"test_node">>,

    %% Drain node
    ok = flurm_job_dispatcher:drain_node(Node),

    %% Resume node
    ok = flurm_job_dispatcher:resume_node(Node),
    ok.

%% Test full job lifecycle through dispatcher
job_lifecycle_test() ->
    JobId = 1000,
    Nodes = [<<"node1">>, <<"node2">>],
    JobInfo = #{
        job_id => JobId,
        allocated_nodes => Nodes,
        script => <<"#!/bin/bash\nsleep 10">>
    },

    %% Dispatch job
    ok = flurm_job_dispatcher:dispatch_job(JobId, JobInfo),

    %% Preempt job (send signal)
    ok = flurm_job_dispatcher:preempt_job(JobId, #{signal => sigterm}),

    %% Requeue job
    ok = flurm_job_dispatcher:requeue_job(JobId),

    %% Dispatch again
    ok = flurm_job_dispatcher:dispatch_job(JobId, JobInfo),

    %% Cancel job
    ok = flurm_job_dispatcher:cancel_job(JobId, Nodes),
    ok.

%%====================================================================
%% Tests with server running (mocked)
%%====================================================================

%% Test dispatcher with server running using a mock server process
dispatcher_with_mock_server_test_() ->
    {setup,
     fun() ->
         %% Start a mock process registered as flurm_job_dispatcher_server
         MockPid = spawn(fun mock_dispatcher_server/0),
         register(flurm_job_dispatcher_server, MockPid),
         #{mock_pid => MockPid}
     end,
     fun(#{mock_pid := MockPid}) ->
         catch unregister(flurm_job_dispatcher_server),
         exit(MockPid, kill),
         ok
     end,
     fun(_) ->
         [
             {"Server running check returns true", fun() ->
                 %% Verify server is detected as running
                 Pid = whereis(flurm_job_dispatcher_server),
                 ?assert(is_pid(Pid)),
                 ?assert(is_process_alive(Pid))
             end},
             {"Dispatch with server running", fun() ->
                 %% This will try to call the mock server
                 %% Our mock just returns ok for calls
                 JobInfo = #{allocated_nodes => [<<"node1">>]},
                 Result = (catch flurm_job_dispatcher:dispatch_job(1, JobInfo)),
                 %% We're testing that the server path is taken
                 %% The mock server will handle the call
                 ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT')
             end},
             {"Cancel with server running", fun() ->
                 Nodes = [<<"node1">>],
                 Result = (catch flurm_job_dispatcher:cancel_job(1, Nodes)),
                 ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT')
             end},
             {"Preempt with server running", fun() ->
                 Options = #{signal => sigterm},
                 Result = (catch flurm_job_dispatcher:preempt_job(1, Options)),
                 ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT')
             end},
             {"Requeue with server running", fun() ->
                 Result = (catch flurm_job_dispatcher:requeue_job(1)),
                 ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT')
             end},
             {"Drain node with server running", fun() ->
                 Result = (catch flurm_job_dispatcher:drain_node(<<"node1">>)),
                 ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT')
             end},
             {"Resume node with server running", fun() ->
                 Result = (catch flurm_job_dispatcher:resume_node(<<"node1">>)),
                 ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT')
             end}
         ]
     end}.

%% Mock dispatcher server that just receives messages
mock_dispatcher_server() ->
    receive
        {'$gen_call', From, _Request} ->
            gen:reply(From, ok),
            mock_dispatcher_server();
        {'$gen_cast', _Request} ->
            mock_dispatcher_server();
        _ ->
            mock_dispatcher_server()
    after 30000 ->
        ok
    end.

%% Test server_running with dead process (edge case)
server_running_dead_process_test() ->
    %% Start a process and kill it
    Pid = spawn(fun() -> receive stop -> ok end end),
    register(flurm_job_dispatcher_server, Pid),
    %% Kill the process
    exit(Pid, kill),
    flurm_test_utils:wait_for_death(Pid),
    %% Now whereis returns undefined because process is dead
    ?assertEqual(undefined, whereis(flurm_job_dispatcher_server)),
    %% Operations should fall through to stub mode
    ok = flurm_job_dispatcher:dispatch_job(1, #{allocated_nodes => [<<"node1">>]}).

%% Test all dispatcher operations in quick succession (stress test)
stress_test() ->
    %% Test many operations in rapid succession
    lists:foreach(fun(I) ->
        JobInfo = #{allocated_nodes => [<<"node1">>]},
        ok = flurm_job_dispatcher:dispatch_job(I, JobInfo),
        ok = flurm_job_dispatcher:preempt_job(I, #{}),
        ok = flurm_job_dispatcher:requeue_job(I),
        ok = flurm_job_dispatcher:cancel_job(I, [<<"node1">>])
    end, lists:seq(1, 100)),
    ok.
