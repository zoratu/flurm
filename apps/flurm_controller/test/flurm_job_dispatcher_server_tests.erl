%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_job_dispatcher_server
%%%
%%% Covers job dispatch, cancellation, signal/preemption, requeue,
%%% drain/resume, build_job_launch_message/2, and signal_to_name/1.
%%%
%%% Uses meck to mock flurm_node_connection_manager.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_job_dispatcher_server_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher_server_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

dispatcher_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Lifecycle
        {"process starts and registers",
         fun test_starts_and_registers/0},

        %% dispatch_job
        {"dispatch_job success to all nodes",
         fun test_dispatch_job_success/0},
        {"dispatch_job no nodes returns error",
         fun test_dispatch_job_no_nodes/0},
        {"dispatch_job partial failure still ok",
         fun test_dispatch_job_partial_failure/0},
        {"dispatch_job all nodes fail returns error",
         fun test_dispatch_job_all_fail/0},
        {"dispatch_job tracks dispatched nodes",
         fun test_dispatch_job_tracks_nodes/0},

        %% cancel_job
        {"cancel_job sends cancel to nodes",
         fun test_cancel_job/0},
        {"cancel_job removes from tracking",
         fun test_cancel_job_removes_tracking/0},

        %% preempt_job
        {"preempt_job sends signal to tracked nodes",
         fun test_preempt_job_tracked/0},
        {"preempt_job with explicit nodes",
         fun test_preempt_job_explicit_nodes/0},
        {"preempt_job no nodes returns error",
         fun test_preempt_job_no_nodes/0},
        {"preempt_job default signal is sigterm",
         fun test_preempt_job_default_signal/0},

        %% signal_job
        {"signal_job delegates to preempt_job",
         fun test_signal_job/0},

        %% requeue_job
        {"requeue_job removes from dispatch tracking",
         fun test_requeue_job/0},

        %% drain_node / resume_node
        {"drain_node sends drain message",
         fun test_drain_node/0},
        {"resume_node sends resume message",
         fun test_resume_node/0},

        %% Unknown messages
        {"unknown call returns error",
         fun test_unknown_call/0},
        {"unknown cast no crash",
         fun test_unknown_cast/0},
        {"unknown info no crash",
         fun test_unknown_info/0}
     ]}.

%%====================================================================
%% Test-exported internal functions
%%====================================================================

internal_functions_test_() ->
    [
        %% build_job_launch_message
        {"build_job_launch_message with defaults",
         fun test_build_launch_msg_defaults/0},
        {"build_job_launch_message with custom fields",
         fun test_build_launch_msg_custom/0},
        {"build_job_launch_message default stdout path",
         fun test_build_launch_msg_stdout_default/0},
        {"build_job_launch_message custom stdout path",
         fun test_build_launch_msg_stdout_custom/0},

        %% signal_to_name
        {"signal_to_name known signals",
         fun test_signal_to_name_known/0},
        {"signal_to_name unknown atom",
         fun test_signal_to_name_unknown_atom/0},
        {"signal_to_name integer",
         fun test_signal_to_name_integer/0}
    ].

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),
    catch meck:unload(flurm_node_connection_manager),
    meck:new(flurm_node_connection_manager, [non_strict]),

    %% Default: send_to_nodes returns all ok
    meck:expect(flurm_node_connection_manager, send_to_nodes,
        fun(Nodes, _Msg) ->
            [{N, ok} || N <- Nodes]
        end),
    meck:expect(flurm_node_connection_manager, send_to_node,
        fun(_Node, _Msg) -> ok end),

    case whereis(flurm_job_dispatcher_server) of
        undefined -> ok;
        Pid ->
            gen_server:stop(Pid, normal, 5000),
            timer:sleep(50)
    end,
    {ok, DispPid} = flurm_job_dispatcher_server:start_link(),
    unlink(DispPid),
    DispPid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true -> gen_server:stop(Pid, normal, 5000);
        false -> ok
    end,
    catch meck:unload(flurm_node_connection_manager),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

make_job_info(Nodes) ->
    #{
        allocated_nodes => Nodes,
        script => <<"#!/bin/bash\necho hello">>,
        name => <<"test_job">>,
        work_dir => <<"/tmp/test">>,
        num_tasks => 1,
        num_cpus => 4,
        partition => <<"default">>,
        memory_mb => 1024,
        time_limit => 3600,
        user_id => 1000,
        group_id => 1000
    }.

%%====================================================================
%% Tests - Lifecycle
%%====================================================================

test_starts_and_registers() ->
    ?assertNotEqual(undefined, whereis(flurm_job_dispatcher_server)).

%%====================================================================
%% Tests - dispatch_job
%%====================================================================

test_dispatch_job_success() ->
    Nodes = [<<"node1">>, <<"node2">>],
    Result = flurm_job_dispatcher_server:dispatch_job(1, make_job_info(Nodes)),
    ?assertEqual(ok, Result).

test_dispatch_job_no_nodes() ->
    Result = flurm_job_dispatcher_server:dispatch_job(2, make_job_info([])),
    ?assertEqual({error, no_nodes}, Result).

test_dispatch_job_partial_failure() ->
    meck:expect(flurm_node_connection_manager, send_to_nodes,
        fun(Nodes, _Msg) ->
            [{hd(Nodes), ok} | [{N, {error, econnrefused}} || N <- tl(Nodes)]]
        end),
    Nodes = [<<"ok_node">>, <<"fail_node">>],
    Result = flurm_job_dispatcher_server:dispatch_job(3, make_job_info(Nodes)),
    %% At least one succeeded, so ok
    ?assertEqual(ok, Result).

test_dispatch_job_all_fail() ->
    meck:expect(flurm_node_connection_manager, send_to_nodes,
        fun(Nodes, _Msg) ->
            [{N, {error, econnrefused}} || N <- Nodes]
        end),
    Nodes = [<<"fail1">>, <<"fail2">>],
    Result = flurm_job_dispatcher_server:dispatch_job(4, make_job_info(Nodes)),
    ?assertEqual({error, all_nodes_failed}, Result).

test_dispatch_job_tracks_nodes() ->
    Nodes = [<<"track1">>, <<"track2">>],
    ok = flurm_job_dispatcher_server:dispatch_job(10, make_job_info(Nodes)),
    %% Preempt should work on the tracked nodes
    Result = flurm_job_dispatcher_server:preempt_job(10, #{}),
    ?assertEqual(ok, Result),
    %% Verify send_to_nodes was called with the tracked nodes
    ?assert(meck:num_calls(flurm_node_connection_manager, send_to_nodes, '_') >= 2).

%%====================================================================
%% Tests - cancel_job
%%====================================================================

test_cancel_job() ->
    Nodes = [<<"cancel1">>, <<"cancel2">>],
    flurm_job_dispatcher_server:cancel_job(20, Nodes),
    timer:sleep(50),
    %% Verify send_to_nodes was called
    ?assert(meck:called(flurm_node_connection_manager, send_to_nodes,
                        [Nodes, '_'])).

test_cancel_job_removes_tracking() ->
    %% First dispatch a job to track it
    Nodes = [<<"rm1">>],
    ok = flurm_job_dispatcher_server:dispatch_job(21, make_job_info(Nodes)),
    %% Cancel it
    flurm_job_dispatcher_server:cancel_job(21, Nodes),
    timer:sleep(50),
    %% Preempt should now fail (no tracked nodes)
    Result = flurm_job_dispatcher_server:preempt_job(21, #{}),
    ?assertEqual({error, no_nodes_for_job}, Result).

%%====================================================================
%% Tests - preempt_job
%%====================================================================

test_preempt_job_tracked() ->
    Nodes = [<<"p1">>, <<"p2">>],
    ok = flurm_job_dispatcher_server:dispatch_job(30, make_job_info(Nodes)),
    Result = flurm_job_dispatcher_server:preempt_job(30, #{signal => sigkill}),
    ?assertEqual(ok, Result).

test_preempt_job_explicit_nodes() ->
    ExplicitNodes = [<<"explicit1">>],
    Result = flurm_job_dispatcher_server:preempt_job(31, #{nodes => ExplicitNodes}),
    ?assertEqual(ok, Result).

test_preempt_job_no_nodes() ->
    Result = flurm_job_dispatcher_server:preempt_job(99, #{}),
    ?assertEqual({error, no_nodes_for_job}, Result).

test_preempt_job_default_signal() ->
    ok = flurm_job_dispatcher_server:dispatch_job(32, make_job_info([<<"sig_node">>])),
    ok = flurm_job_dispatcher_server:preempt_job(32, #{}),
    %% Check meck history for the preempt call (second send_to_nodes call)
    History = meck:history(flurm_node_connection_manager),
    %% Find the call with job_signal type
    SignalCalls = [Msg || {_Pid, {flurm_node_connection_manager, send_to_nodes, [_Nodes, Msg]}, _Ret} <- History,
                          maps:get(type, Msg, undefined) =:= job_signal],
    ?assert(length(SignalCalls) > 0),
    [LastSignalMsg | _] = lists:reverse(SignalCalls),
    Payload = maps:get(payload, LastSignalMsg),
    ?assertEqual(<<"SIGTERM">>, maps:get(<<"signal">>, Payload)).

%%====================================================================
%% Tests - signal_job
%%====================================================================

test_signal_job() ->
    %% signal_job delegates to preempt_job with nodes=[Node]
    ok = flurm_job_dispatcher_server:dispatch_job(40, make_job_info([<<"sig_target">>])),
    Result = flurm_job_dispatcher_server:signal_job(40, sigterm, <<"sig_target">>),
    ?assertEqual(ok, Result).

%%====================================================================
%% Tests - requeue_job
%%====================================================================

test_requeue_job() ->
    Nodes = [<<"rq1">>],
    ok = flurm_job_dispatcher_server:dispatch_job(50, make_job_info(Nodes)),
    flurm_job_dispatcher_server:requeue_job(50),
    timer:sleep(50),
    %% After requeue, tracked nodes should be gone
    Result = flurm_job_dispatcher_server:preempt_job(50, #{}),
    ?assertEqual({error, no_nodes_for_job}, Result).

%%====================================================================
%% Tests - drain_node / resume_node
%%====================================================================

test_drain_node() ->
    Result = flurm_job_dispatcher_server:drain_node(<<"drain_host">>),
    ?assertEqual(ok, Result),
    ?assert(meck:called(flurm_node_connection_manager, send_to_node,
                        [<<"drain_host">>, '_'])).

test_resume_node() ->
    Result = flurm_job_dispatcher_server:resume_node(<<"resume_host">>),
    ?assertEqual(ok, Result),
    ?assert(meck:called(flurm_node_connection_manager, send_to_node,
                        [<<"resume_host">>, '_'])).

%%====================================================================
%% Tests - Unknown messages
%%====================================================================

test_unknown_call() ->
    ?assertEqual({error, unknown_request},
                 gen_server:call(flurm_job_dispatcher_server, bogus)).

test_unknown_cast() ->
    gen_server:cast(flurm_job_dispatcher_server, bogus),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_job_dispatcher_server))).

test_unknown_info() ->
    whereis(flurm_job_dispatcher_server) ! bogus,
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_job_dispatcher_server))).

%%====================================================================
%% Tests - build_job_launch_message (TEST export)
%%====================================================================

test_build_launch_msg_defaults() ->
    Msg = flurm_job_dispatcher_server:build_job_launch_message(1, #{}),
    ?assertEqual(job_launch, maps:get(type, Msg)),
    Payload = maps:get(payload, Msg),
    ?assertEqual(1, maps:get(<<"job_id">>, Payload)),
    ?assertEqual(<<>>, maps:get(<<"script">>, Payload)),
    ?assertEqual(<<"/tmp">>, maps:get(<<"working_dir">>, Payload)),
    ?assertEqual(1024, maps:get(<<"memory_mb">>, Payload)),
    ?assertEqual(3600, maps:get(<<"time_limit">>, Payload)).

test_build_launch_msg_custom() ->
    Info = #{
        script => <<"#!/bin/bash\necho test">>,
        work_dir => <<"/home/user">>,
        name => <<"my_job">>,
        num_tasks => 4,
        num_cpus => 8,
        partition => <<"gpu">>,
        memory_mb => 4096,
        time_limit => 7200,
        user_id => 500,
        group_id => 500,
        std_out => <<"/tmp/out.log">>,
        std_err => <<"/tmp/err.log">>,
        prolog => <<"/etc/prolog.sh">>,
        epilog => <<"/etc/epilog.sh">>
    },
    Msg = flurm_job_dispatcher_server:build_job_launch_message(42, Info),
    Payload = maps:get(payload, Msg),
    ?assertEqual(42, maps:get(<<"job_id">>, Payload)),
    ?assertEqual(<<"#!/bin/bash\necho test">>, maps:get(<<"script">>, Payload)),
    ?assertEqual(<<"/home/user">>, maps:get(<<"working_dir">>, Payload)),
    ?assertEqual(8, maps:get(<<"num_cpus">>, Payload)),
    ?assertEqual(4096, maps:get(<<"memory_mb">>, Payload)),
    ?assertEqual(7200, maps:get(<<"time_limit">>, Payload)),
    ?assertEqual(500, maps:get(<<"user_id">>, Payload)),
    ?assertEqual(500, maps:get(<<"group_id">>, Payload)),
    ?assertEqual(<<"/tmp/out.log">>, maps:get(<<"std_out">>, Payload)),
    ?assertEqual(<<"/tmp/err.log">>, maps:get(<<"std_err">>, Payload)),
    ?assertEqual(<<"/etc/prolog.sh">>, maps:get(<<"prolog">>, Payload)),
    ?assertEqual(<<"/etc/epilog.sh">>, maps:get(<<"epilog">>, Payload)),
    %% Verify environment
    Env = maps:get(<<"environment">>, Payload),
    ?assertEqual(<<"42">>, maps:get(<<"SLURM_JOB_ID">>, Env)),
    ?assertEqual(<<"my_job">>, maps:get(<<"SLURM_JOB_NAME">>, Env)),
    ?assertEqual(<<"4">>, maps:get(<<"SLURM_NTASKS">>, Env)),
    ?assertEqual(<<"8">>, maps:get(<<"SLURM_CPUS_PER_TASK">>, Env)),
    ?assertEqual(<<"gpu">>, maps:get(<<"SLURM_JOB_PARTITION">>, Env)),
    ?assertEqual(<<"/home/user">>, maps:get(<<"SLURM_SUBMIT_DIR">>, Env)).

test_build_launch_msg_stdout_default() ->
    Msg = flurm_job_dispatcher_server:build_job_launch_message(5, #{work_dir => <<"/work">>}),
    Payload = maps:get(payload, Msg),
    ?assertEqual(<<"/work/slurm-5.out">>, maps:get(<<"std_out">>, Payload)).

test_build_launch_msg_stdout_custom() ->
    Msg = flurm_job_dispatcher_server:build_job_launch_message(5,
        #{work_dir => <<"/work">>, std_out => <<"/custom/output.log">>}),
    Payload = maps:get(payload, Msg),
    ?assertEqual(<<"/custom/output.log">>, maps:get(<<"std_out">>, Payload)).

%%====================================================================
%% Tests - signal_to_name (TEST export)
%%====================================================================

test_signal_to_name_known() ->
    ?assertEqual(<<"SIGTERM">>, flurm_job_dispatcher_server:signal_to_name(sigterm)),
    ?assertEqual(<<"SIGKILL">>, flurm_job_dispatcher_server:signal_to_name(sigkill)),
    ?assertEqual(<<"SIGSTOP">>, flurm_job_dispatcher_server:signal_to_name(sigstop)),
    ?assertEqual(<<"SIGCONT">>, flurm_job_dispatcher_server:signal_to_name(sigcont)),
    ?assertEqual(<<"SIGHUP">>, flurm_job_dispatcher_server:signal_to_name(sighup)),
    ?assertEqual(<<"SIGUSR1">>, flurm_job_dispatcher_server:signal_to_name(sigusr1)),
    ?assertEqual(<<"SIGUSR2">>, flurm_job_dispatcher_server:signal_to_name(sigusr2)),
    ?assertEqual(<<"SIGINT">>, flurm_job_dispatcher_server:signal_to_name(sigint)).

test_signal_to_name_unknown_atom() ->
    ?assertEqual(<<"SIGFOO">>, flurm_job_dispatcher_server:signal_to_name(sigfoo)).

test_signal_to_name_integer() ->
    ?assertEqual(<<"9">>, flurm_job_dispatcher_server:signal_to_name(9)),
    ?assertEqual(<<"15">>, flurm_job_dispatcher_server:signal_to_name(15)).
