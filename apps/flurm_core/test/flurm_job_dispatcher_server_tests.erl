%%%-------------------------------------------------------------------
%%% @doc FLURM Job Dispatcher Server Tests
%%%
%%% Comprehensive EUnit tests for the flurm_job_dispatcher_server module.
%%% Tests job dispatching, cancellation, preemption, node drain/resume,
%%% and signal handling.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher_server_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Main test fixture
dispatcher_server_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init/1 initializes state correctly", fun test_init/0},
        {"dispatch_job with no nodes returns error", fun test_dispatch_no_nodes/0},
        {"dispatch_job to nodes succeeds", fun test_dispatch_success/0},
        {"dispatch_job with partial failure", fun test_dispatch_partial_failure/0},
        {"dispatch_job with all nodes failed", fun test_dispatch_all_failed/0},
        {"cancel_job sends cancel to nodes", fun test_cancel_job/0},
        {"drain_node sends drain message", fun test_drain_node/0},
        {"resume_node sends resume message", fun test_resume_node/0},
        {"preempt_job with default signal", fun test_preempt_default_signal/0},
        {"preempt_job with sigkill", fun test_preempt_sigkill/0},
        {"preempt_job with no nodes for job", fun test_preempt_no_nodes/0},
        {"requeue_job removes from tracking", fun test_requeue_job/0},
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast is ignored", fun test_unknown_cast/0},
        {"unknown info is ignored", fun test_unknown_info/0},
        {"terminate returns ok", fun test_terminate/0}
     ]}.

setup() ->
    %% Mock the node connection manager
    meck:new(flurm_node_connection_manager, [passthrough, non_strict]),
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(Nodes, _Msg) ->
                    [{N, ok} || N <- Nodes]
                end),
    meck:expect(flurm_node_connection_manager, send_to_node,
                fun(_Node, _Msg) -> ok end),
    ok.

cleanup(_) ->
    %% Stop dispatcher server if running
    case whereis(flurm_job_dispatcher_server) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            Ref = monitor(process, Pid),
            catch gen_server:stop(Pid, shutdown, 2000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 2000 ->
                demonitor(Ref, [flush])
            end
    end,

    meck:unload(flurm_node_connection_manager),
    ok.

%%====================================================================
%% Init Tests
%%====================================================================

test_init() ->
    {ok, State} = flurm_job_dispatcher_server:init([]),

    %% State is a record with dispatched_jobs map
    ?assertEqual({state, #{}}, State),
    ok.

%%====================================================================
%% Dispatch Job Tests
%%====================================================================

test_dispatch_no_nodes() ->
    {ok, State} = flurm_job_dispatcher_server:init([]),

    JobInfo = #{allocated_nodes => []},

    {reply, Result, _NewState} = flurm_job_dispatcher_server:handle_call(
        {dispatch_job, 1, JobInfo}, {self(), ref}, State),

    ?assertEqual({error, no_nodes}, Result),
    ok.

test_dispatch_success() ->
    {ok, State} = flurm_job_dispatcher_server:init([]),

    JobInfo = #{
        allocated_nodes => [<<"node1">>, <<"node2">>],
        script => <<"#!/bin/bash\necho hello">>,
        num_cpus => 2,
        memory_mb => 4096,
        time_limit => 3600,
        work_dir => <<"/tmp">>
    },

    {reply, Result, NewState} = flurm_job_dispatcher_server:handle_call(
        {dispatch_job, 100, JobInfo}, {self(), ref}, State),

    ?assertEqual(ok, Result),

    %% Verify job is tracked
    {state, Jobs} = NewState,
    ?assert(maps:is_key(100, Jobs)),
    ?assertEqual([<<"node1">>, <<"node2">>], maps:get(100, Jobs)),
    ok.

test_dispatch_partial_failure() ->
    %% Mock partial failure - node2 fails
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(Nodes, _Msg) ->
                    lists:map(fun(N) ->
                        case N of
                            <<"node2">> -> {N, {error, not_connected}};
                            _ -> {N, ok}
                        end
                    end, Nodes)
                end),

    {ok, State} = flurm_job_dispatcher_server:init([]),

    JobInfo = #{
        allocated_nodes => [<<"node1">>, <<"node2">>],
        script => <<"#!/bin/bash\necho hello">>
    },

    {reply, Result, NewState} = flurm_job_dispatcher_server:handle_call(
        {dispatch_job, 101, JobInfo}, {self(), ref}, State),

    %% Should still succeed (partial success is ok)
    ?assertEqual(ok, Result),

    %% Only successful nodes should be tracked
    {state, Jobs} = NewState,
    ?assertEqual([<<"node1">>], maps:get(101, Jobs)),
    ok.

test_dispatch_all_failed() ->
    %% Mock complete failure
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(Nodes, _Msg) ->
                    [{N, {error, not_connected}} || N <- Nodes]
                end),

    {ok, State} = flurm_job_dispatcher_server:init([]),

    JobInfo = #{
        allocated_nodes => [<<"node1">>, <<"node2">>],
        script => <<"#!/bin/bash\necho hello">>
    },

    {reply, Result, _NewState} = flurm_job_dispatcher_server:handle_call(
        {dispatch_job, 102, JobInfo}, {self(), ref}, State),

    ?assertEqual({error, all_nodes_failed}, Result),
    ok.

%%====================================================================
%% Cancel Job Tests
%%====================================================================

test_cancel_job() ->
    %% Track the cancel message
    Self = self(),
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(Nodes, Msg) ->
                    Self ! {cancel_sent, Nodes, Msg},
                    [{N, ok} || N <- Nodes]
                end),

    {ok, _State} = flurm_job_dispatcher_server:init([]),

    %% Pre-populate dispatched jobs
    State1 = {state, #{200 => [<<"node1">>, <<"node2">>]}},

    Nodes = [<<"node1">>, <<"node2">>],
    {noreply, NewState} = flurm_job_dispatcher_server:handle_cast(
        {cancel_job, 200, Nodes}, State1),

    %% Verify cancel message was sent
    receive
        {cancel_sent, SentNodes, Msg} ->
            ?assertEqual(Nodes, SentNodes),
            ?assertEqual(job_cancel, maps:get(type, Msg)),
            Payload = maps:get(payload, Msg),
            ?assertEqual(200, maps:get(<<"job_id">>, Payload))
    after 1000 ->
        ?assert(false)
    end,

    %% Job should be removed from tracking
    {state, Jobs} = NewState,
    ?assertNot(maps:is_key(200, Jobs)),
    ok.

%%====================================================================
%% Drain/Resume Node Tests
%%====================================================================

test_drain_node() ->
    Self = self(),
    meck:expect(flurm_node_connection_manager, send_to_node,
                fun(Node, Msg) ->
                    Self ! {drain_sent, Node, Msg},
                    ok
                end),

    {ok, State} = flurm_job_dispatcher_server:init([]),

    {reply, Result, _NewState} = flurm_job_dispatcher_server:handle_call(
        {drain_node, <<"node1">>}, {self(), ref}, State),

    ?assertEqual(ok, Result),

    %% Verify drain message was sent
    receive
        {drain_sent, Node, Msg} ->
            ?assertEqual(<<"node1">>, Node),
            ?assertEqual(node_drain, maps:get(type, Msg))
    after 1000 ->
        ?assert(false)
    end,
    ok.

test_resume_node() ->
    Self = self(),
    meck:expect(flurm_node_connection_manager, send_to_node,
                fun(Node, Msg) ->
                    Self ! {resume_sent, Node, Msg},
                    ok
                end),

    {ok, State} = flurm_job_dispatcher_server:init([]),

    {reply, Result, _NewState} = flurm_job_dispatcher_server:handle_call(
        {resume_node, <<"node1">>}, {self(), ref}, State),

    ?assertEqual(ok, Result),

    %% Verify resume message was sent
    receive
        {resume_sent, Node, Msg} ->
            ?assertEqual(<<"node1">>, Node),
            ?assertEqual(node_resume, maps:get(type, Msg))
    after 1000 ->
        ?assert(false)
    end,
    ok.

%%====================================================================
%% Preempt Job Tests
%%====================================================================

test_preempt_default_signal() ->
    Self = self(),
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(Nodes, Msg) ->
                    Self ! {preempt_sent, Nodes, Msg},
                    [{N, ok} || N <- Nodes]
                end),

    %% Pre-populate dispatched jobs
    State = {state, #{300 => [<<"node1">>, <<"node2">>]}},

    %% Preempt with default signal (sigterm)
    {reply, Result, _NewState} = flurm_job_dispatcher_server:handle_call(
        {preempt_job, 300, #{}}, {self(), ref}, State),

    ?assertEqual(ok, Result),

    %% Verify signal message was sent
    receive
        {preempt_sent, Nodes, Msg} ->
            ?assertEqual([<<"node1">>, <<"node2">>], Nodes),
            ?assertEqual(job_signal, maps:get(type, Msg)),
            Payload = maps:get(payload, Msg),
            ?assertEqual(300, maps:get(<<"job_id">>, Payload)),
            ?assertEqual(<<"SIGTERM">>, maps:get(<<"signal">>, Payload))
    after 1000 ->
        ?assert(false)
    end,
    ok.

test_preempt_sigkill() ->
    Self = self(),
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(Nodes, Msg) ->
                    Self ! {preempt_sent, Nodes, Msg},
                    [{N, ok} || N <- Nodes]
                end),

    State = {state, #{301 => [<<"node1">>]}},

    %% Preempt with sigkill
    Options = #{signal => sigkill},
    {reply, Result, _NewState} = flurm_job_dispatcher_server:handle_call(
        {preempt_job, 301, Options}, {self(), ref}, State),

    ?assertEqual(ok, Result),

    receive
        {preempt_sent, _Nodes, Msg} ->
            Payload = maps:get(payload, Msg),
            ?assertEqual(<<"SIGKILL">>, maps:get(<<"signal">>, Payload))
    after 1000 ->
        ?assert(false)
    end,
    ok.

test_preempt_no_nodes() ->
    State = {state, #{}},  % No jobs tracked

    {reply, Result, _NewState} = flurm_job_dispatcher_server:handle_call(
        {preempt_job, 999, #{}}, {self(), ref}, State),

    ?assertEqual({error, no_nodes_for_job}, Result),
    ok.

%%====================================================================
%% Requeue Job Tests
%%====================================================================

test_requeue_job() ->
    %% Pre-populate dispatched jobs
    State = {state, #{400 => [<<"node1">>, <<"node2">>]}},

    {noreply, NewState} = flurm_job_dispatcher_server:handle_cast(
        {requeue_job, 400}, State),

    %% Job should be removed from tracking
    {state, Jobs} = NewState,
    ?assertNot(maps:is_key(400, Jobs)),
    ok.

%%====================================================================
%% Unknown Message Tests
%%====================================================================

test_unknown_call() ->
    {ok, State} = flurm_job_dispatcher_server:init([]),

    {reply, Result, SameState} = flurm_job_dispatcher_server:handle_call(
        unknown_request, {self(), ref}, State),

    ?assertEqual({error, unknown_request}, Result),
    ?assertEqual(State, SameState),
    ok.

test_unknown_cast() ->
    {ok, State} = flurm_job_dispatcher_server:init([]),

    {noreply, SameState} = flurm_job_dispatcher_server:handle_cast(
        unknown_message, State),

    ?assertEqual(State, SameState),
    ok.

test_unknown_info() ->
    {ok, State} = flurm_job_dispatcher_server:init([]),

    {noreply, SameState} = flurm_job_dispatcher_server:handle_info(
        unknown_info, State),

    ?assertEqual(State, SameState),
    ok.

test_terminate() ->
    {ok, State} = flurm_job_dispatcher_server:init([]),

    Result = flurm_job_dispatcher_server:terminate(normal, State),

    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Signal Conversion Tests
%%====================================================================

signal_test_() ->
    {"Signal to name conversion",
     [
        {"All signal types convert correctly", fun test_signal_conversions/0}
     ]}.

test_signal_conversions() ->
    Self = self(),
    meck:new(flurm_node_connection_manager, [passthrough, non_strict]),
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(Nodes, Msg) ->
                    Self ! {signal_msg, Msg},
                    [{N, ok} || N <- Nodes]
                end),

    Signals = [
        {sigterm, <<"SIGTERM">>},
        {sigkill, <<"SIGKILL">>},
        {sigstop, <<"SIGSTOP">>},
        {sigcont, <<"SIGCONT">>},
        {sighup, <<"SIGHUP">>},
        {sigusr1, <<"SIGUSR1">>},
        {sigusr2, <<"SIGUSR2">>},
        {sigint, <<"SIGINT">>}
    ],

    State = {state, #{500 => [<<"node1">>]}},

    lists:foreach(fun({Signal, Expected}) ->
        Options = #{signal => Signal},
        {reply, ok, _} = flurm_job_dispatcher_server:handle_call(
            {preempt_job, 500, Options}, {self(), ref}, State),

        receive
            {signal_msg, Msg} ->
                Payload = maps:get(payload, Msg),
                Actual = maps:get(<<"signal">>, Payload),
                ?assertEqual(Expected, Actual)
        after 1000 ->
            ?assert(false)
        end
    end, Signals),

    meck:unload(flurm_node_connection_manager),
    ok.

%%====================================================================
%% Job Launch Message Building Tests
%%====================================================================

job_launch_message_test_() ->
    {foreach,
     fun setup_launch/0,
     fun cleanup_launch/1,
     [
        {"Build message with minimal info", fun test_build_message_minimal/0},
        {"Build message with all fields", fun test_build_message_full/0},
        {"Default output file is set", fun test_build_message_default_output/0}
     ]}.

setup_launch() ->
    meck:new(flurm_node_connection_manager, [passthrough, non_strict]),
    ok.

cleanup_launch(_) ->
    meck:unload(flurm_node_connection_manager),
    ok.

test_build_message_minimal() ->
    Self = self(),
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(_Nodes, Msg) ->
                    Self ! {launch_msg, Msg},
                    [{<<"node1">>, ok}]
                end),

    {ok, State} = flurm_job_dispatcher_server:init([]),

    JobInfo = #{
        allocated_nodes => [<<"node1">>]
    },

    {reply, ok, _} = flurm_job_dispatcher_server:handle_call(
        {dispatch_job, 600, JobInfo}, {self(), ref}, State),

    receive
        {launch_msg, Msg} ->
            ?assertEqual(job_launch, maps:get(type, Msg)),
            Payload = maps:get(payload, Msg),
            ?assertEqual(600, maps:get(<<"job_id">>, Payload)),
            ?assertEqual(<<>>, maps:get(<<"script">>, Payload)),
            ?assertEqual(1, maps:get(<<"num_cpus">>, Payload)),
            ?assertEqual(1024, maps:get(<<"memory_mb">>, Payload)),
            ?assertEqual(3600, maps:get(<<"time_limit">>, Payload))
    after 1000 ->
        ?assert(false)
    end,
    ok.

test_build_message_full() ->
    Self = self(),
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(_Nodes, Msg) ->
                    Self ! {launch_msg, Msg},
                    [{<<"node1">>, ok}]
                end),

    {ok, State} = flurm_job_dispatcher_server:init([]),

    JobInfo = #{
        allocated_nodes => [<<"node1">>],
        script => <<"#!/bin/bash\necho test">>,
        name => <<"test_job">>,
        num_tasks => 4,
        num_cpus => 8,
        memory_mb => 16384,
        time_limit => 7200,
        work_dir => <<"/home/user/work">>,
        partition => <<"gpu">>,
        user_id => 1000,
        group_id => 1000,
        std_out => <<"/home/user/output.log">>,
        std_err => <<"/home/user/error.log">>
    },

    {reply, ok, _} = flurm_job_dispatcher_server:handle_call(
        {dispatch_job, 601, JobInfo}, {self(), ref}, State),

    receive
        {launch_msg, Msg} ->
            Payload = maps:get(payload, Msg),
            ?assertEqual(601, maps:get(<<"job_id">>, Payload)),
            ?assertEqual(<<"#!/bin/bash\necho test">>, maps:get(<<"script">>, Payload)),
            ?assertEqual(8, maps:get(<<"num_cpus">>, Payload)),
            ?assertEqual(16384, maps:get(<<"memory_mb">>, Payload)),
            ?assertEqual(7200, maps:get(<<"time_limit">>, Payload)),
            ?assertEqual(<<"/home/user/work">>, maps:get(<<"working_dir">>, Payload)),
            ?assertEqual(1000, maps:get(<<"user_id">>, Payload)),
            ?assertEqual(1000, maps:get(<<"group_id">>, Payload)),
            ?assertEqual(<<"/home/user/output.log">>, maps:get(<<"std_out">>, Payload)),
            ?assertEqual(<<"/home/user/error.log">>, maps:get(<<"std_err">>, Payload)),

            %% Check environment variables
            Env = maps:get(<<"environment">>, Payload),
            ?assertEqual(<<"601">>, maps:get(<<"SLURM_JOB_ID">>, Env)),
            ?assertEqual(<<"test_job">>, maps:get(<<"SLURM_JOB_NAME">>, Env)),
            ?assertEqual(<<"4">>, maps:get(<<"SLURM_NTASKS">>, Env)),
            ?assertEqual(<<"8">>, maps:get(<<"SLURM_CPUS_PER_TASK">>, Env)),
            ?assertEqual(<<"gpu">>, maps:get(<<"SLURM_JOB_PARTITION">>, Env))
    after 1000 ->
        ?assert(false)
    end,
    ok.

test_build_message_default_output() ->
    Self = self(),
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(_Nodes, Msg) ->
                    Self ! {launch_msg, Msg},
                    [{<<"node1">>, ok}]
                end),

    {ok, State} = flurm_job_dispatcher_server:init([]),

    JobInfo = #{
        allocated_nodes => [<<"node1">>],
        work_dir => <<"/home/user">>,
        std_out => <<>>  % Empty std_out should get default
    },

    {reply, ok, _} = flurm_job_dispatcher_server:handle_call(
        {dispatch_job, 602, JobInfo}, {self(), ref}, State),

    receive
        {launch_msg, Msg} ->
            Payload = maps:get(payload, Msg),
            %% Default output path should be work_dir/slurm-<jobid>.out
            ?assertEqual(<<"/home/user/slurm-602.out">>, maps:get(<<"std_out">>, Payload))
    after 1000 ->
        ?assert(false)
    end,
    ok.

%%====================================================================
%% Live Server Tests
%%====================================================================

live_server_test_() ->
    {foreach,
     fun setup_live/0,
     fun cleanup_live/1,
     [
        {"start_link starts server", fun test_live_start_link/0},
        {"dispatch_job API works", fun test_live_dispatch_job/0},
        {"cancel_job API works", fun test_live_cancel_job/0},
        {"drain_node API works", fun test_live_drain_node/0},
        {"resume_node API works", fun test_live_resume_node/0},
        {"preempt_job API works", fun test_live_preempt_job/0},
        {"requeue_job API works", fun test_live_requeue_job/0}
     ]}.

setup_live() ->
    meck:new(flurm_node_connection_manager, [passthrough, non_strict]),
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(Nodes, _Msg) -> [{N, ok} || N <- Nodes] end),
    meck:expect(flurm_node_connection_manager, send_to_node,
                fun(_Node, _Msg) -> ok end),

    %% Stop any existing server
    case whereis(flurm_job_dispatcher_server) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            Ref = monitor(process, Pid),
            catch gen_server:stop(Pid, shutdown, 2000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 2000 ->
                demonitor(Ref, [flush])
            end
    end,
    ok.

cleanup_live(_) ->
    case whereis(flurm_job_dispatcher_server) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            Ref = monitor(process, Pid),
            catch gen_server:stop(Pid, shutdown, 2000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 2000 ->
                demonitor(Ref, [flush])
            end
    end,

    meck:unload(flurm_node_connection_manager),
    ok.

test_live_start_link() ->
    {ok, Pid} = flurm_job_dispatcher_server:start_link(),

    ?assert(is_pid(Pid)),
    ?assertEqual(Pid, whereis(flurm_job_dispatcher_server)),
    ok.

test_live_dispatch_job() ->
    {ok, _Pid} = flurm_job_dispatcher_server:start_link(),

    JobInfo = #{
        allocated_nodes => [<<"node1">>],
        script => <<"#!/bin/bash\necho test">>
    },

    Result = flurm_job_dispatcher_server:dispatch_job(700, JobInfo),

    ?assertEqual(ok, Result),
    ok.

test_live_cancel_job() ->
    {ok, _Pid} = flurm_job_dispatcher_server:start_link(),

    %% First dispatch a job
    JobInfo = #{allocated_nodes => [<<"node1">>]},
    ok = flurm_job_dispatcher_server:dispatch_job(701, JobInfo),

    %% Then cancel it
    Result = flurm_job_dispatcher_server:cancel_job(701, [<<"node1">>]),

    ?assertEqual(ok, Result),
    ok.

test_live_drain_node() ->
    {ok, _Pid} = flurm_job_dispatcher_server:start_link(),

    Result = flurm_job_dispatcher_server:drain_node(<<"node1">>),

    ?assertEqual(ok, Result),
    ok.

test_live_resume_node() ->
    {ok, _Pid} = flurm_job_dispatcher_server:start_link(),

    Result = flurm_job_dispatcher_server:resume_node(<<"node1">>),

    ?assertEqual(ok, Result),
    ok.

test_live_preempt_job() ->
    {ok, _Pid} = flurm_job_dispatcher_server:start_link(),

    %% First dispatch a job
    JobInfo = #{allocated_nodes => [<<"node1">>]},
    ok = flurm_job_dispatcher_server:dispatch_job(702, JobInfo),

    %% Then preempt it
    Result = flurm_job_dispatcher_server:preempt_job(702, #{signal => sigterm}),

    ?assertEqual(ok, Result),
    ok.

test_live_requeue_job() ->
    {ok, _Pid} = flurm_job_dispatcher_server:start_link(),

    %% First dispatch a job
    JobInfo = #{allocated_nodes => [<<"node1">>]},
    ok = flurm_job_dispatcher_server:dispatch_job(703, JobInfo),

    %% Then requeue it
    Result = flurm_job_dispatcher_server:requeue_job(703),

    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Preempt with Explicit Nodes Tests
%%====================================================================

preempt_explicit_nodes_test_() ->
    {foreach,
     fun setup_explicit/0,
     fun cleanup_explicit/1,
     [
        {"Preempt with explicit node list", fun test_preempt_explicit_nodes/0},
        {"Preempt with partial node failure", fun test_preempt_partial_failure/0}
     ]}.

setup_explicit() ->
    meck:new(flurm_node_connection_manager, [passthrough, non_strict]),
    ok.

cleanup_explicit(_) ->
    meck:unload(flurm_node_connection_manager),
    ok.

test_preempt_explicit_nodes() ->
    Self = self(),
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(Nodes, Msg) ->
                    Self ! {preempt_explicit, Nodes, Msg},
                    [{N, ok} || N <- Nodes]
                end),

    %% State has job 800 on node1, but we want to preempt only on node2
    State = {state, #{800 => [<<"node1">>]}},

    Options = #{
        signal => sigterm,
        nodes => [<<"node2">>, <<"node3">>]  % Explicit nodes
    },

    {reply, ok, _} = flurm_job_dispatcher_server:handle_call(
        {preempt_job, 800, Options}, {self(), ref}, State),

    receive
        {preempt_explicit, Nodes, _Msg} ->
            %% Should use explicit nodes, not tracked nodes
            ?assertEqual([<<"node2">>, <<"node3">>], Nodes)
    after 1000 ->
        ?assert(false)
    end,
    ok.

test_preempt_partial_failure() ->
    meck:expect(flurm_node_connection_manager, send_to_nodes,
                fun(Nodes, _Msg) ->
                    %% First node succeeds, second fails
                    lists:map(fun(N) ->
                        case N of
                            <<"node2">> -> {N, {error, not_connected}};
                            _ -> {N, ok}
                        end
                    end, Nodes)
                end),

    State = {state, #{801 => [<<"node1">>, <<"node2">>]}},

    {reply, Result, _} = flurm_job_dispatcher_server:handle_call(
        {preempt_job, 801, #{}}, {self(), ref}, State),

    %% Partial success should still return ok
    ?assertEqual(ok, Result),
    ok.
