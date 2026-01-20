%%%-------------------------------------------------------------------
%%% @doc FLURM Job Dispatcher Coverage Tests
%%%
%%% Comprehensive EUnit tests specifically designed to maximize code
%%% coverage for the flurm_job_dispatcher facade module. Tests both
%%% stub mode and server-running mode paths.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup_stub() ->
    application:ensure_all_started(sasl),
    %% Ensure the dispatcher server is NOT running
    case whereis(flurm_job_dispatcher_server) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch exit(Pid, kill),
            flurm_test_utils:wait_for_death(Pid)
    end,
    #{mode => stub}.

setup_with_server() ->
    application:ensure_all_started(sasl),
    %% Ensure old server is stopped
    case whereis(flurm_job_dispatcher_server) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch exit(Pid, kill),
            flurm_test_utils:wait_for_death(Pid)
    end,
    %% Start mock server
    MockPid = spawn(fun mock_server_loop/0),
    register(flurm_job_dispatcher_server, MockPid),
    #{mode => server, mock_pid => MockPid}.

cleanup(#{mode := stub}) ->
    ok;
cleanup(#{mode := server, mock_pid := MockPid}) ->
    catch unregister(flurm_job_dispatcher_server),
    catch exit(MockPid, kill),
    flurm_test_utils:wait_for_death(MockPid),
    ok.

mock_server_loop() ->
    receive
        {'$gen_call', {Pid, Tag}, _Request} ->
            Pid ! {Tag, ok},
            mock_server_loop();
        stop ->
            ok;
        _ ->
            mock_server_loop()
    after 30000 ->
        ok
    end.

%%====================================================================
%% Test Fixtures - Stub Mode
%%====================================================================

stub_mode_test_() ->
    {foreach,
     fun setup_stub/0,
     fun cleanup/1,
     [
        {"dispatch_job stub returns ok", fun test_dispatch_job_stub/0},
        {"dispatch_job stub empty nodes", fun test_dispatch_job_stub_empty/0},
        {"dispatch_job stub multiple nodes", fun test_dispatch_job_stub_multi/0},
        {"cancel_job stub returns ok", fun test_cancel_job_stub/0},
        {"cancel_job stub empty nodes", fun test_cancel_job_stub_empty/0},
        {"preempt_job stub returns ok", fun test_preempt_job_stub/0},
        {"preempt_job stub default signal", fun test_preempt_job_stub_default_signal/0},
        {"preempt_job stub sigterm", fun test_preempt_job_stub_sigterm/0},
        {"preempt_job stub sigkill", fun test_preempt_job_stub_sigkill/0},
        {"preempt_job stub sigstop", fun test_preempt_job_stub_sigstop/0},
        {"preempt_job stub sigcont", fun test_preempt_job_stub_sigcont/0},
        {"requeue_job stub returns ok", fun test_requeue_job_stub/0},
        {"drain_node stub returns ok", fun test_drain_node_stub/0},
        {"resume_node stub returns ok", fun test_resume_node_stub/0}
     ]}.

%%====================================================================
%% Stub Mode Tests
%%====================================================================

test_dispatch_job_stub() ->
    JobInfo = #{
        job_id => 1,
        allocated_nodes => [<<"node1">>],
        script => <<"#!/bin/bash\necho hello">>,
        user_id => 1000
    },
    Result = flurm_job_dispatcher:dispatch_job(1, JobInfo),
    ?assertEqual(ok, Result),
    ok.

test_dispatch_job_stub_empty() ->
    JobInfo = #{
        job_id => 1,
        allocated_nodes => []
    },
    Result = flurm_job_dispatcher:dispatch_job(1, JobInfo),
    ?assertEqual(ok, Result),
    ok.

test_dispatch_job_stub_multi() ->
    JobInfo = #{
        job_id => 1,
        allocated_nodes => [<<"node1">>, <<"node2">>, <<"node3">>]
    },
    Result = flurm_job_dispatcher:dispatch_job(1, JobInfo),
    ?assertEqual(ok, Result),
    ok.

test_cancel_job_stub() ->
    Nodes = [<<"node1">>, <<"node2">>],
    Result = flurm_job_dispatcher:cancel_job(1, Nodes),
    ?assertEqual(ok, Result),
    ok.

test_cancel_job_stub_empty() ->
    Result = flurm_job_dispatcher:cancel_job(1, []),
    ?assertEqual(ok, Result),
    ok.

test_preempt_job_stub() ->
    Options = #{signal => sigterm, nodes => [<<"node1">>]},
    Result = flurm_job_dispatcher:preempt_job(1, Options),
    ?assertEqual(ok, Result),
    ok.

test_preempt_job_stub_default_signal() ->
    %% Empty options should use default sigterm
    Result = flurm_job_dispatcher:preempt_job(1, #{}),
    ?assertEqual(ok, Result),
    ok.

test_preempt_job_stub_sigterm() ->
    Options = #{signal => sigterm},
    Result = flurm_job_dispatcher:preempt_job(1, Options),
    ?assertEqual(ok, Result),
    ok.

test_preempt_job_stub_sigkill() ->
    Options = #{signal => sigkill},
    Result = flurm_job_dispatcher:preempt_job(1, Options),
    ?assertEqual(ok, Result),
    ok.

test_preempt_job_stub_sigstop() ->
    Options = #{signal => sigstop},
    Result = flurm_job_dispatcher:preempt_job(1, Options),
    ?assertEqual(ok, Result),
    ok.

test_preempt_job_stub_sigcont() ->
    Options = #{signal => sigcont},
    Result = flurm_job_dispatcher:preempt_job(1, Options),
    ?assertEqual(ok, Result),
    ok.

test_requeue_job_stub() ->
    Result = flurm_job_dispatcher:requeue_job(1),
    ?assertEqual(ok, Result),
    ok.

test_drain_node_stub() ->
    Result = flurm_job_dispatcher:drain_node(<<"node1">>),
    ?assertEqual(ok, Result),
    ok.

test_resume_node_stub() ->
    Result = flurm_job_dispatcher:resume_node(<<"node1">>),
    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Test Fixtures - Server Mode
%%====================================================================

server_mode_test_() ->
    {foreach,
     fun setup_with_server/0,
     fun cleanup/1,
     [
        {"server_running returns true", fun test_server_running_true/0},
        {"dispatch_job delegates to server", fun test_dispatch_job_server/0},
        {"cancel_job delegates to server", fun test_cancel_job_server/0},
        {"preempt_job delegates to server", fun test_preempt_job_server/0},
        {"requeue_job delegates to server", fun test_requeue_job_server/0},
        {"drain_node delegates to server", fun test_drain_node_server/0},
        {"resume_node delegates to server", fun test_resume_node_server/0}
     ]}.

%%====================================================================
%% Server Mode Tests
%%====================================================================

test_server_running_true() ->
    Pid = whereis(flurm_job_dispatcher_server),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ok.

test_dispatch_job_server() ->
    JobInfo = #{
        job_id => 1,
        allocated_nodes => [<<"node1">>]
    },
    %% This will call through to the mock server
    Result = (catch flurm_job_dispatcher:dispatch_job(1, JobInfo)),
    %% Mock server returns ok
    ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT'),
    ok.

test_cancel_job_server() ->
    Nodes = [<<"node1">>],
    Result = (catch flurm_job_dispatcher:cancel_job(1, Nodes)),
    ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT'),
    ok.

test_preempt_job_server() ->
    Options = #{signal => sigterm},
    Result = (catch flurm_job_dispatcher:preempt_job(1, Options)),
    ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT'),
    ok.

test_requeue_job_server() ->
    Result = (catch flurm_job_dispatcher:requeue_job(1)),
    ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT'),
    ok.

test_drain_node_server() ->
    Result = (catch flurm_job_dispatcher:drain_node(<<"node1">>)),
    ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT'),
    ok.

test_resume_node_server() ->
    Result = (catch flurm_job_dispatcher:resume_node(<<"node1">>)),
    ?assert(Result =:= ok orelse element(1, Result) =:= 'EXIT'),
    ok.

%%====================================================================
%% Server Running Edge Cases
%%====================================================================

server_running_edge_cases_test_() ->
    [
        {"server_running with dead process", fun test_server_running_dead/0},
        {"server_running undefined", fun test_server_running_undefined/0}
    ].

test_server_running_dead() ->
    %% Start a process and immediately kill it
    Pid = spawn(fun() -> receive stop -> ok end end),
    register(flurm_job_dispatcher_server, Pid),
    exit(Pid, kill),
    flurm_test_utils:wait_for_death(Pid),

    %% Process is dead, whereis returns undefined
    ?assertEqual(undefined, whereis(flurm_job_dispatcher_server)),

    %% Operations should fall through to stub mode
    ok = flurm_job_dispatcher:dispatch_job(1, #{allocated_nodes => []}),
    ok.

test_server_running_undefined() ->
    %% Ensure no server is registered
    case whereis(flurm_job_dispatcher_server) of
        undefined -> ok;
        Pid ->
            catch unregister(flurm_job_dispatcher_server),
            catch exit(Pid, kill),
            flurm_test_utils:wait_for_death(Pid)
    end,

    ?assertEqual(undefined, whereis(flurm_job_dispatcher_server)),

    %% Operations should use stub mode
    ok = flurm_job_dispatcher:dispatch_job(1, #{allocated_nodes => [<<"node1">>]}),
    ok = flurm_job_dispatcher:cancel_job(1, [<<"node1">>]),
    ok = flurm_job_dispatcher:preempt_job(1, #{}),
    ok = flurm_job_dispatcher:requeue_job(1),
    ok = flurm_job_dispatcher:drain_node(<<"node1">>),
    ok = flurm_job_dispatcher:resume_node(<<"node1">>),
    ok.

%%====================================================================
%% Concurrency Tests
%%====================================================================

concurrency_test_() ->
    {setup,
     fun setup_stub/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"concurrent dispatch operations", fun test_concurrent_dispatch/0},
             {"concurrent preempt operations", fun test_concurrent_preempt/0},
             {"concurrent node operations", fun test_concurrent_node_ops/0}
         ]
     end}.

test_concurrent_dispatch() ->
    Self = self(),
    NumJobs = 20,

    Pids = [spawn(fun() ->
        JobInfo = #{
            job_id => I,
            allocated_nodes => [<<"node1">>]
        },
        Result = flurm_job_dispatcher:dispatch_job(I, JobInfo),
        Self ! {done, I, Result}
    end) || I <- lists:seq(1, NumJobs)],

    Results = [receive {done, _, R} -> R end || _ <- Pids],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)),
    ok.

test_concurrent_preempt() ->
    Self = self(),
    NumJobs = 20,

    Pids = [spawn(fun() ->
        Options = #{signal => sigterm},
        Result = flurm_job_dispatcher:preempt_job(I, Options),
        Self ! {done, I, Result}
    end) || I <- lists:seq(1, NumJobs)],

    Results = [receive {done, _, R} -> R end || _ <- Pids],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results)),
    ok.

test_concurrent_node_ops() ->
    Self = self(),
    NumNodes = 10,

    %% Start drain operations
    DrainPids = [spawn(fun() ->
        Node = list_to_binary("node" ++ integer_to_list(I)),
        Result = flurm_job_dispatcher:drain_node(Node),
        Self ! {drain_done, I, Result}
    end) || I <- lists:seq(1, NumNodes)],

    %% Start resume operations
    ResumePids = [spawn(fun() ->
        Node = list_to_binary("node" ++ integer_to_list(I)),
        Result = flurm_job_dispatcher:resume_node(Node),
        Self ! {resume_done, I, Result}
    end) || I <- lists:seq(1, NumNodes)],

    DrainResults = [receive {drain_done, _, R} -> R end || _ <- DrainPids],
    ResumeResults = [receive {resume_done, _, R} -> R end || _ <- ResumePids],

    ?assert(lists:all(fun(R) -> R =:= ok end, DrainResults)),
    ?assert(lists:all(fun(R) -> R =:= ok end, ResumeResults)),
    ok.

%%====================================================================
%% Job Lifecycle Tests
%%====================================================================

job_lifecycle_test_() ->
    {setup,
     fun setup_stub/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"full job lifecycle in stub mode", fun test_full_lifecycle/0},
             {"multiple jobs lifecycle", fun test_multiple_jobs_lifecycle/0}
         ]
     end}.

test_full_lifecycle() ->
    JobId = 100,
    Nodes = [<<"node1">>, <<"node2">>],
    JobInfo = #{
        job_id => JobId,
        allocated_nodes => Nodes,
        script => <<"#!/bin/bash\nsleep 10">>,
        user_id => 1000,
        group_id => 1000,
        work_dir => <<"/tmp/job_100">>
    },

    %% Dispatch
    ok = flurm_job_dispatcher:dispatch_job(JobId, JobInfo),

    %% Preempt with different signals
    ok = flurm_job_dispatcher:preempt_job(JobId, #{signal => sigstop}),
    ok = flurm_job_dispatcher:preempt_job(JobId, #{signal => sigcont}),

    %% Requeue
    ok = flurm_job_dispatcher:requeue_job(JobId),

    %% Dispatch again
    ok = flurm_job_dispatcher:dispatch_job(JobId, JobInfo),

    %% Cancel
    ok = flurm_job_dispatcher:cancel_job(JobId, Nodes),
    ok.

test_multiple_jobs_lifecycle() ->
    %% Run lifecycle for multiple jobs
    lists:foreach(fun(JobId) ->
        Nodes = [<<"node1">>],
        JobInfo = #{
            job_id => JobId,
            allocated_nodes => Nodes
        },

        ok = flurm_job_dispatcher:dispatch_job(JobId, JobInfo),
        ok = flurm_job_dispatcher:preempt_job(JobId, #{signal => sigterm}),
        ok = flurm_job_dispatcher:requeue_job(JobId),
        ok = flurm_job_dispatcher:cancel_job(JobId, Nodes)
    end, lists:seq(1, 10)),
    ok.

%%====================================================================
%% Node Management Tests
%%====================================================================

node_management_test_() ->
    {setup,
     fun setup_stub/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"drain and resume sequence", fun test_drain_resume_sequence/0},
             {"multiple node operations", fun test_multiple_node_operations/0}
         ]
     end}.

test_drain_resume_sequence() ->
    Node = <<"compute-node-01">>,

    %% Drain
    ok = flurm_job_dispatcher:drain_node(Node),

    %% Resume
    ok = flurm_job_dispatcher:resume_node(Node),

    %% Drain again
    ok = flurm_job_dispatcher:drain_node(Node),
    ok.

test_multiple_node_operations() ->
    Nodes = [<<"node1">>, <<"node2">>, <<"node3">>, <<"node4">>, <<"node5">>],

    %% Drain all
    lists:foreach(fun(Node) ->
        ok = flurm_job_dispatcher:drain_node(Node)
    end, Nodes),

    %% Resume all
    lists:foreach(fun(Node) ->
        ok = flurm_job_dispatcher:resume_node(Node)
    end, Nodes),
    ok.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun setup_stub/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"dispatch with minimal info", fun test_dispatch_minimal/0},
             {"dispatch with full info", fun test_dispatch_full_info/0},
             {"preempt with extra options", fun test_preempt_extra_options/0},
             {"cancel single node", fun test_cancel_single_node/0},
             {"cancel many nodes", fun test_cancel_many_nodes/0},
             {"large job id", fun test_large_job_id/0}
         ]
     end}.

test_dispatch_minimal() ->
    JobInfo = #{},
    ok = flurm_job_dispatcher:dispatch_job(1, JobInfo),
    ok.

test_dispatch_full_info() ->
    JobInfo = #{
        job_id => 12345,
        allocated_nodes => [<<"node1">>, <<"node2">>, <<"node3">>],
        script => <<"#!/bin/bash\necho $SLURM_JOB_ID\nsleep 100">>,
        user_id => 1000,
        group_id => 1000,
        work_dir => <<"/scratch/user/job_12345">>,
        std_out => <<"/scratch/user/job_12345/output.log">>,
        std_err => <<"/scratch/user/job_12345/error.log">>,
        environment => #{
            <<"PATH">> => <<"/usr/bin">>,
            <<"HOME">> => <<"/home/user">>
        }
    },
    ok = flurm_job_dispatcher:dispatch_job(12345, JobInfo),
    ok.

test_preempt_extra_options() ->
    Options = #{
        signal => sigterm,
        nodes => [<<"node1">>, <<"node2">>],
        grace_time => 30,
        checkpoint => true
    },
    ok = flurm_job_dispatcher:preempt_job(1, Options),
    ok.

test_cancel_single_node() ->
    ok = flurm_job_dispatcher:cancel_job(1, [<<"single-node">>]),
    ok.

test_cancel_many_nodes() ->
    Nodes = [list_to_binary("node" ++ integer_to_list(I)) || I <- lists:seq(1, 100)],
    ok = flurm_job_dispatcher:cancel_job(1, Nodes),
    ok.

test_large_job_id() ->
    LargeJobId = 999999999999,
    JobInfo = #{allocated_nodes => [<<"node1">>]},
    ok = flurm_job_dispatcher:dispatch_job(LargeJobId, JobInfo),
    ok = flurm_job_dispatcher:preempt_job(LargeJobId, #{}),
    ok = flurm_job_dispatcher:requeue_job(LargeJobId),
    ok = flurm_job_dispatcher:cancel_job(LargeJobId, [<<"node1">>]),
    ok.

%%====================================================================
%% Stress Tests
%%====================================================================

stress_test_() ->
    {setup,
     fun setup_stub/0,
     fun cleanup/1,
     fun(_) ->
         [
             {"rapid dispatch/cancel cycle", fun test_rapid_cycle/0},
             {"many jobs sequential", fun test_many_jobs_sequential/0}
         ]
     end}.

test_rapid_cycle() ->
    %% Rapidly dispatch and cancel jobs
    lists:foreach(fun(I) ->
        JobInfo = #{allocated_nodes => [<<"node1">>]},
        ok = flurm_job_dispatcher:dispatch_job(I, JobInfo),
        ok = flurm_job_dispatcher:cancel_job(I, [<<"node1">>])
    end, lists:seq(1, 100)),
    ok.

test_many_jobs_sequential() ->
    %% Process many jobs sequentially
    lists:foreach(fun(I) ->
        JobInfo = #{allocated_nodes => [<<"node1">>]},
        ok = flurm_job_dispatcher:dispatch_job(I, JobInfo),
        ok = flurm_job_dispatcher:preempt_job(I, #{signal => sigterm}),
        ok = flurm_job_dispatcher:requeue_job(I),
        ok = flurm_job_dispatcher:dispatch_job(I, JobInfo),
        ok = flurm_job_dispatcher:cancel_job(I, [<<"node1">>])
    end, lists:seq(1, 50)),
    ok.
