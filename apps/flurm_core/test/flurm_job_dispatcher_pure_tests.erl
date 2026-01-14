%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_job_dispatcher (Facade Module)
%%%
%%% Tests the job dispatcher facade without mocking.
%%% Since this is a facade that delegates to flurm_job_dispatcher_server
%%% when running, and operates in stub mode when the server is not running,
%%% we test the stub mode behavior (server not running).
%%%
%%% NO MECK - Pure unit tests only.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Descriptions
%%====================================================================

flurm_job_dispatcher_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"dispatch_job returns ok in stub mode", fun dispatch_job_stub_test/0},
      {"dispatch_job with empty job info", fun dispatch_job_empty_info_test/0},
      {"dispatch_job with allocated nodes", fun dispatch_job_with_nodes_test/0},
      {"cancel_job returns ok in stub mode", fun cancel_job_stub_test/0},
      {"cancel_job with empty node list", fun cancel_job_empty_nodes_test/0},
      {"cancel_job with multiple nodes", fun cancel_job_multiple_nodes_test/0},
      {"preempt_job returns ok in stub mode", fun preempt_job_stub_test/0},
      {"preempt_job with default signal", fun preempt_job_default_signal_test/0},
      {"preempt_job with sigkill", fun preempt_job_sigkill_test/0},
      {"preempt_job with sigstop", fun preempt_job_sigstop_test/0},
      {"preempt_job with sigcont", fun preempt_job_sigcont_test/0},
      {"requeue_job returns ok in stub mode", fun requeue_job_stub_test/0},
      {"drain_node returns ok in stub mode", fun drain_node_stub_test/0},
      {"drain_node with different hostnames", fun drain_node_various_test/0},
      {"resume_node returns ok in stub mode", fun resume_node_stub_test/0},
      {"resume_node with different hostnames", fun resume_node_various_test/0}
     ]}.

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup() ->
    %% Ensure lager is started for logging
    case application:ensure_all_started(lager) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        _ -> ok  %% Ignore errors in test environment
    end,
    %% Ensure flurm_job_dispatcher_server is NOT running for stub mode tests
    %% (it shouldn't be running in unit tests anyway)
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% dispatch_job/2 Tests
%%====================================================================

dispatch_job_stub_test() ->
    %% In stub mode (server not running), dispatch_job should return ok
    JobId = 1001,
    JobInfo = #{name => <<"test_job">>, allocated_nodes => [<<"node1">>]},
    Result = flurm_job_dispatcher:dispatch_job(JobId, JobInfo),
    ?assertEqual(ok, Result).

dispatch_job_empty_info_test() ->
    %% dispatch_job with empty job info should still work
    JobId = 1002,
    JobInfo = #{},
    Result = flurm_job_dispatcher:dispatch_job(JobId, JobInfo),
    ?assertEqual(ok, Result).

dispatch_job_with_nodes_test() ->
    %% dispatch_job with multiple allocated nodes
    JobId = 1003,
    JobInfo = #{
        name => <<"multi_node_job">>,
        allocated_nodes => [<<"node1">>, <<"node2">>, <<"node3">>],
        partition => <<"compute">>,
        num_cpus => 16
    },
    Result = flurm_job_dispatcher:dispatch_job(JobId, JobInfo),
    ?assertEqual(ok, Result).

%%====================================================================
%% cancel_job/2 Tests
%%====================================================================

cancel_job_stub_test() ->
    %% In stub mode, cancel_job should return ok
    JobId = 2001,
    Nodes = [<<"node1">>],
    Result = flurm_job_dispatcher:cancel_job(JobId, Nodes),
    ?assertEqual(ok, Result).

cancel_job_empty_nodes_test() ->
    %% cancel_job with empty node list should still work
    JobId = 2002,
    Nodes = [],
    Result = flurm_job_dispatcher:cancel_job(JobId, Nodes),
    ?assertEqual(ok, Result).

cancel_job_multiple_nodes_test() ->
    %% cancel_job with multiple nodes
    JobId = 2003,
    Nodes = [<<"node1">>, <<"node2">>, <<"node3">>, <<"node4">>],
    Result = flurm_job_dispatcher:cancel_job(JobId, Nodes),
    ?assertEqual(ok, Result).

%%====================================================================
%% preempt_job/2 Tests
%%====================================================================

preempt_job_stub_test() ->
    %% In stub mode, preempt_job should return ok
    JobId = 3001,
    Options = #{signal => sigterm},
    Result = flurm_job_dispatcher:preempt_job(JobId, Options),
    ?assertEqual(ok, Result).

preempt_job_default_signal_test() ->
    %% preempt_job with empty options uses default sigterm
    JobId = 3002,
    Options = #{},
    Result = flurm_job_dispatcher:preempt_job(JobId, Options),
    ?assertEqual(ok, Result).

preempt_job_sigkill_test() ->
    %% preempt_job with sigkill signal
    JobId = 3003,
    Options = #{signal => sigkill},
    Result = flurm_job_dispatcher:preempt_job(JobId, Options),
    ?assertEqual(ok, Result).

preempt_job_sigstop_test() ->
    %% preempt_job with sigstop signal
    JobId = 3004,
    Options = #{signal => sigstop},
    Result = flurm_job_dispatcher:preempt_job(JobId, Options),
    ?assertEqual(ok, Result).

preempt_job_sigcont_test() ->
    %% preempt_job with sigcont signal
    JobId = 3005,
    Options = #{signal => sigcont, nodes => [<<"node1">>]},
    Result = flurm_job_dispatcher:preempt_job(JobId, Options),
    ?assertEqual(ok, Result).

%%====================================================================
%% requeue_job/1 Tests
%%====================================================================

requeue_job_stub_test() ->
    %% In stub mode, requeue_job should return ok
    JobId = 4001,
    Result = flurm_job_dispatcher:requeue_job(JobId),
    ?assertEqual(ok, Result).

%%====================================================================
%% drain_node/1 Tests
%%====================================================================

drain_node_stub_test() ->
    %% In stub mode, drain_node should return ok
    Hostname = <<"node1">>,
    Result = flurm_job_dispatcher:drain_node(Hostname),
    ?assertEqual(ok, Result).

drain_node_various_test() ->
    %% Test drain_node with various hostname formats
    ?assertEqual(ok, flurm_job_dispatcher:drain_node(<<"compute-001">>)),
    ?assertEqual(ok, flurm_job_dispatcher:drain_node(<<"gpu-node-01">>)),
    ?assertEqual(ok, flurm_job_dispatcher:drain_node(<<"login.cluster.local">>)),
    ?assertEqual(ok, flurm_job_dispatcher:drain_node(<<"">>)).

%%====================================================================
%% resume_node/1 Tests
%%====================================================================

resume_node_stub_test() ->
    %% In stub mode, resume_node should return ok
    Hostname = <<"node1">>,
    Result = flurm_job_dispatcher:resume_node(Hostname),
    ?assertEqual(ok, Result).

resume_node_various_test() ->
    %% Test resume_node with various hostname formats
    ?assertEqual(ok, flurm_job_dispatcher:resume_node(<<"compute-001">>)),
    ?assertEqual(ok, flurm_job_dispatcher:resume_node(<<"gpu-node-01">>)),
    ?assertEqual(ok, flurm_job_dispatcher:resume_node(<<"login.cluster.local">>)),
    ?assertEqual(ok, flurm_job_dispatcher:resume_node(<<"">>)).

%%====================================================================
%% Edge Case Tests
%%====================================================================

large_job_id_test_() ->
    %% Test with very large job IDs
    [
     ?_assertEqual(ok, flurm_job_dispatcher:dispatch_job(999999999, #{})),
     ?_assertEqual(ok, flurm_job_dispatcher:cancel_job(999999999, [])),
     ?_assertEqual(ok, flurm_job_dispatcher:preempt_job(999999999, #{})),
     ?_assertEqual(ok, flurm_job_dispatcher:requeue_job(999999999))
    ].

complex_job_info_test_() ->
    %% Test with complex job info maps
    ComplexJobInfo = #{
        name => <<"complex_job">>,
        allocated_nodes => [<<"n1">>, <<"n2">>, <<"n3">>],
        partition => <<"gpu">>,
        qos => <<"high">>,
        num_cpus => 32,
        memory => 65536,
        time_limit => 3600,
        command => <<"/path/to/script.sh">>,
        environment => #{<<"PATH">> => <<"/usr/bin">>}
    },
    [
     ?_assertEqual(ok, flurm_job_dispatcher:dispatch_job(12345, ComplexJobInfo))
    ].
