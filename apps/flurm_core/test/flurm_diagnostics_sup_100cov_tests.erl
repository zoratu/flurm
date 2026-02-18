%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_diagnostics_sup module
%%% Achieves 100% code coverage for the diagnostics supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_diagnostics_sup_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing supervisor
    catch supervisor:terminate_child(flurm_diagnostics_sup, flurm_leak_detector),
    catch exit(whereis(flurm_diagnostics_sup), kill),
    catch exit(whereis(flurm_leak_detector), kill),
    timer:sleep(50),

    %% Mock flurm_diagnostics to avoid starting real leak detector
    meck:new(flurm_diagnostics, [passthrough, non_strict]),
    meck:expect(flurm_diagnostics, start_link, fun([_Interval]) ->
        Pid = spawn_link(fun() ->
            register(flurm_leak_detector, self()),
            receive stop -> ok end
        end),
        {ok, Pid}
    end),
    ok.

cleanup(_) ->
    catch exit(whereis(flurm_diagnostics_sup), kill),
    catch exit(whereis(flurm_leak_detector), kill),
    meck:unload(flurm_diagnostics),
    timer:sleep(50),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

diagnostics_sup_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link with no args", fun test_start_link_no_args/0},
        {"start_link with empty options", fun test_start_link_empty_opts/0},
        {"start_link with custom interval", fun test_start_link_custom_interval/0},
        {"start_link with leak detector disabled", fun test_start_link_disabled/0},
        {"start_link with all options", fun test_start_link_all_opts/0},
        {"supervisor restarts child", fun test_supervisor_restarts/0},
        {"init returns correct spec", fun test_init_spec/0},
        {"init with disabled returns empty children", fun test_init_disabled/0}
     ]}.

%%====================================================================
%% Start Link Tests
%%====================================================================

test_start_link_no_args() ->
    {ok, Pid} = flurm_diagnostics_sup:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Should have registered supervisor
    ?assertEqual(Pid, whereis(flurm_diagnostics_sup)),

    %% Check children
    Children = supervisor:which_children(flurm_diagnostics_sup),
    ?assertEqual(1, length(Children)),
    [{flurm_leak_detector, ChildPid, worker, [flurm_diagnostics]}] = Children,
    ?assert(is_pid(ChildPid)).

test_start_link_empty_opts() ->
    {ok, Pid} = flurm_diagnostics_sup:start_link(#{}),
    ?assert(is_pid(Pid)),
    Children = supervisor:which_children(flurm_diagnostics_sup),
    ?assertEqual(1, length(Children)).

test_start_link_custom_interval() ->
    {ok, Pid} = flurm_diagnostics_sup:start_link(#{interval => 30000}),
    ?assert(is_pid(Pid)),

    %% Verify the start_link was called with our interval
    ?assert(meck:called(flurm_diagnostics, start_link, [[30000]])).

test_start_link_disabled() ->
    {ok, Pid} = flurm_diagnostics_sup:start_link(#{enable_leak_detector => false}),
    ?assert(is_pid(Pid)),

    %% Should have no children when disabled
    Children = supervisor:which_children(flurm_diagnostics_sup),
    ?assertEqual([], Children).

test_start_link_all_opts() ->
    {ok, Pid} = flurm_diagnostics_sup:start_link(#{
        interval => 15000,
        enable_leak_detector => true
    }),
    ?assert(is_pid(Pid)),

    Children = supervisor:which_children(flurm_diagnostics_sup),
    ?assertEqual(1, length(Children)),
    ?assert(meck:called(flurm_diagnostics, start_link, [[15000]])).

%%====================================================================
%% Supervisor Behavior Tests
%%====================================================================

test_supervisor_restarts() ->
    {ok, _Pid} = flurm_diagnostics_sup:start_link(#{}),

    %% Get child pid
    [{flurm_leak_detector, ChildPid1, _, _}] = supervisor:which_children(flurm_diagnostics_sup),

    %% Kill the child
    exit(ChildPid1, kill),
    timer:sleep(100),

    %% Child should be restarted
    [{flurm_leak_detector, ChildPid2, _, _}] = supervisor:which_children(flurm_diagnostics_sup),
    ?assert(is_pid(ChildPid2)),
    ?assertNotEqual(ChildPid1, ChildPid2).

test_init_spec() ->
    %% Test init directly
    {ok, {SupFlags, Children}} = flurm_diagnostics_sup:init(#{}),

    %% Check supervisor flags
    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
    ?assertEqual(5, maps:get(intensity, SupFlags)),
    ?assertEqual(60, maps:get(period, SupFlags)),

    %% Check children spec
    ?assertEqual(1, length(Children)),
    [ChildSpec] = Children,
    ?assertEqual(flurm_leak_detector, maps:get(id, ChildSpec)),
    ?assertEqual({flurm_diagnostics, start_link, [[60000]]}, maps:get(start, ChildSpec)),
    ?assertEqual(permanent, maps:get(restart, ChildSpec)),
    ?assertEqual(5000, maps:get(shutdown, ChildSpec)),
    ?assertEqual(worker, maps:get(type, ChildSpec)),
    ?assertEqual([flurm_diagnostics], maps:get(modules, ChildSpec)).

test_init_disabled() ->
    {ok, {SupFlags, Children}} = flurm_diagnostics_sup:init(#{enable_leak_detector => false}),

    %% Check supervisor flags
    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),

    %% No children when disabled
    ?assertEqual([], Children).

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

diagnostics_sup_edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"multiple starts fail", fun test_multiple_starts/0},
        {"custom interval in init", fun test_custom_interval_init/0}
     ]}.

test_multiple_starts() ->
    {ok, Pid1} = flurm_diagnostics_sup:start_link(),
    ?assert(is_pid(Pid1)),

    %% Second start should fail because name is registered
    Result = flurm_diagnostics_sup:start_link(),
    ?assertMatch({error, {already_started, _}}, Result).

test_custom_interval_init() ->
    {ok, {_SupFlags, Children}} = flurm_diagnostics_sup:init(#{interval => 5000}),

    ?assertEqual(1, length(Children)),
    [ChildSpec] = Children,
    ?assertEqual({flurm_diagnostics, start_link, [[5000]]}, maps:get(start, ChildSpec)).
