%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_node_daemon_sup module
%%%
%%% Tests the supervisor directly without mocking it.
%%% Child modules are mocked to avoid starting real services.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_sup_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

supervisor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_init_returns_valid_spec/1,
      fun test_init_has_correct_strategy/1,
      fun test_init_has_correct_children/1,
      fun test_start_link_registers_name/1
     ]}.

setup() ->
    %% Mock child modules to prevent actual startup
    meck:new(flurm_system_monitor, [passthrough, non_strict]),
    meck:new(flurm_controller_connector, [passthrough, non_strict]),
    meck:new(flurm_job_executor_sup, [passthrough, non_strict]),

    meck:expect(flurm_system_monitor, start_link, fun() ->
        {ok, spawn(fun() -> receive stop -> ok end end)}
    end),
    meck:expect(flurm_controller_connector, start_link, fun() ->
        {ok, spawn(fun() -> receive stop -> ok end end)}
    end),
    meck:expect(flurm_job_executor_sup, start_link, fun() ->
        {ok, spawn(fun() -> receive stop -> ok end end)}
    end),
    ok.

cleanup(_) ->
    %% Stop supervisor if running
    catch gen_server:stop(flurm_node_daemon_sup),
    meck:unload(flurm_system_monitor),
    meck:unload(flurm_controller_connector),
    meck:unload(flurm_job_executor_sup),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_init_returns_valid_spec(_) ->
    {"init/1 returns valid supervisor spec",
     fun() ->
         {ok, {SupFlags, Children}} = flurm_node_daemon_sup:init([]),

         %% Verify it's a valid supervisor return
         ?assert(is_map(SupFlags)),
         ?assert(is_list(Children)),
         ?assertEqual(3, length(Children))
     end}.

test_init_has_correct_strategy(_) ->
    {"init/1 uses one_for_one strategy",
     fun() ->
         {ok, {SupFlags, _Children}} = flurm_node_daemon_sup:init([]),

         ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
         ?assertEqual(5, maps:get(intensity, SupFlags)),
         ?assertEqual(10, maps:get(period, SupFlags))
     end}.

test_init_has_correct_children(_) ->
    {"init/1 defines correct child specs",
     fun() ->
         {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

         %% Extract child IDs
         ChildIds = [maps:get(id, C) || C <- Children],

         ?assert(lists:member(flurm_system_monitor, ChildIds)),
         ?assert(lists:member(flurm_controller_connector, ChildIds)),
         ?assert(lists:member(flurm_job_executor_sup, ChildIds)),

         %% Verify each child spec has required fields
         lists:foreach(fun(ChildSpec) ->
             ?assert(maps:is_key(id, ChildSpec)),
             ?assert(maps:is_key(start, ChildSpec)),
             ?assert(maps:is_key(restart, ChildSpec)),
             ?assert(maps:is_key(shutdown, ChildSpec)),
             ?assert(maps:is_key(type, ChildSpec)),
             ?assert(maps:is_key(modules, ChildSpec))
         end, Children)
     end}.

test_start_link_registers_name(_) ->
    {"start_link/0 registers supervisor with local name",
     fun() ->
         {ok, Pid} = flurm_node_daemon_sup:start_link(),

         ?assert(is_pid(Pid)),
         ?assertEqual(Pid, whereis(flurm_node_daemon_sup)),

         %% Stop it
         gen_server:stop(Pid)
     end}.

%%====================================================================
%% Child spec validation tests
%%====================================================================

child_spec_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
      {"system_monitor child spec is valid", fun test_system_monitor_spec/0},
      {"controller_connector child spec is valid", fun test_controller_connector_spec/0},
      {"job_executor_sup child spec is valid", fun test_job_executor_sup_spec/0}
     ]}.

test_system_monitor_spec() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),
    [MonitorSpec] = [C || C <- Children, maps:get(id, C) =:= flurm_system_monitor],

    ?assertEqual({flurm_system_monitor, start_link, []}, maps:get(start, MonitorSpec)),
    ?assertEqual(permanent, maps:get(restart, MonitorSpec)),
    ?assertEqual(5000, maps:get(shutdown, MonitorSpec)),
    ?assertEqual(worker, maps:get(type, MonitorSpec)),
    ?assertEqual([flurm_system_monitor], maps:get(modules, MonitorSpec)).

test_controller_connector_spec() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),
    [ConnectorSpec] = [C || C <- Children, maps:get(id, C) =:= flurm_controller_connector],

    ?assertEqual({flurm_controller_connector, start_link, []}, maps:get(start, ConnectorSpec)),
    ?assertEqual(permanent, maps:get(restart, ConnectorSpec)),
    ?assertEqual(5000, maps:get(shutdown, ConnectorSpec)),
    ?assertEqual(worker, maps:get(type, ConnectorSpec)),
    ?assertEqual([flurm_controller_connector], maps:get(modules, ConnectorSpec)).

test_job_executor_sup_spec() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),
    [ExecutorSupSpec] = [C || C <- Children, maps:get(id, C) =:= flurm_job_executor_sup],

    ?assertEqual({flurm_job_executor_sup, start_link, []}, maps:get(start, ExecutorSupSpec)),
    ?assertEqual(permanent, maps:get(restart, ExecutorSupSpec)),
    ?assertEqual(infinity, maps:get(shutdown, ExecutorSupSpec)),
    ?assertEqual(supervisor, maps:get(type, ExecutorSupSpec)),
    ?assertEqual([flurm_job_executor_sup], maps:get(modules, ExecutorSupSpec)).
