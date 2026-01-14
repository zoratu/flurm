%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_node_daemon_sup Module
%%%
%%% Tests the supervisor's init/1 callback to verify correct child
%%% specifications and supervisor flags.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_sup_comprehensive_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Supervisor Init Tests
%%====================================================================

sup_init_test_() ->
    [
        {"init returns valid supervisor spec", fun test_init_returns_valid_spec/0},
        {"init sets one_for_one strategy", fun test_init_strategy/0},
        {"init configures correct intensity", fun test_init_intensity/0},
        {"init configures correct period", fun test_init_period/0},
        {"init creates system_monitor child", fun test_child_system_monitor/0},
        {"init creates controller_connector child", fun test_child_controller_connector/0},
        {"init creates job_executor_sup child", fun test_child_job_executor_sup/0},
        {"init child order is correct", fun test_child_order/0},
        {"all children have required fields", fun test_child_required_fields/0},
        {"workers have correct shutdown", fun test_worker_shutdown/0},
        {"supervisor has correct shutdown", fun test_supervisor_shutdown/0}
    ].

test_init_returns_valid_spec() ->
    Result = flurm_node_daemon_sup:init([]),
    ?assertMatch({ok, {_SupFlags, _Children}}, Result),
    {ok, {SupFlags, Children}} = Result,
    ?assert(is_map(SupFlags)),
    ?assert(is_list(Children)),
    ?assertEqual(3, length(Children)),
    ok.

test_init_strategy() ->
    {ok, {SupFlags, _Children}} = flurm_node_daemon_sup:init([]),
    Strategy = maps:get(strategy, SupFlags),
    ?assertEqual(one_for_one, Strategy),
    ok.

test_init_intensity() ->
    {ok, {SupFlags, _Children}} = flurm_node_daemon_sup:init([]),
    Intensity = maps:get(intensity, SupFlags),
    ?assertEqual(5, Intensity),
    ok.

test_init_period() ->
    {ok, {SupFlags, _Children}} = flurm_node_daemon_sup:init([]),
    Period = maps:get(period, SupFlags),
    ?assertEqual(10, Period),
    ok.

test_child_system_monitor() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),
    MonitorChild = find_child_by_id(flurm_system_monitor, Children),

    ?assertNotEqual(undefined, MonitorChild),
    ?assertEqual({flurm_system_monitor, start_link, []}, maps:get(start, MonitorChild)),
    ?assertEqual(permanent, maps:get(restart, MonitorChild)),
    ?assertEqual(worker, maps:get(type, MonitorChild)),
    ?assertEqual([flurm_system_monitor], maps:get(modules, MonitorChild)),
    ok.

test_child_controller_connector() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),
    ConnectorChild = find_child_by_id(flurm_controller_connector, Children),

    ?assertNotEqual(undefined, ConnectorChild),
    ?assertEqual({flurm_controller_connector, start_link, []}, maps:get(start, ConnectorChild)),
    ?assertEqual(permanent, maps:get(restart, ConnectorChild)),
    ?assertEqual(worker, maps:get(type, ConnectorChild)),
    ?assertEqual([flurm_controller_connector], maps:get(modules, ConnectorChild)),
    ok.

test_child_job_executor_sup() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),
    ExecutorSupChild = find_child_by_id(flurm_job_executor_sup, Children),

    ?assertNotEqual(undefined, ExecutorSupChild),
    ?assertEqual({flurm_job_executor_sup, start_link, []}, maps:get(start, ExecutorSupChild)),
    ?assertEqual(permanent, maps:get(restart, ExecutorSupChild)),
    ?assertEqual(supervisor, maps:get(type, ExecutorSupChild)),
    ?assertEqual([flurm_job_executor_sup], maps:get(modules, ExecutorSupChild)),
    ok.

test_child_order() ->
    %% Verify children are started in correct order:
    %% 1. system_monitor - must start first to collect metrics
    %% 2. controller_connector - connects to controller
    %% 3. job_executor_sup - supervises job execution
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

    ChildIds = [maps:get(id, Child) || Child <- Children],
    ?assertEqual([flurm_system_monitor, flurm_controller_connector, flurm_job_executor_sup], ChildIds),
    ok.

test_child_required_fields() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

    lists:foreach(fun(Child) ->
        %% Each child must have these fields
        ?assert(maps:is_key(id, Child), "Missing id field"),
        ?assert(maps:is_key(start, Child), "Missing start field"),
        ?assert(maps:is_key(restart, Child), "Missing restart field"),
        ?assert(maps:is_key(shutdown, Child), "Missing shutdown field"),
        ?assert(maps:is_key(type, Child), "Missing type field"),
        ?assert(maps:is_key(modules, Child), "Missing modules field"),

        %% Validate types
        ?assert(is_atom(maps:get(id, Child))),
        ?assert(is_tuple(maps:get(start, Child))),
        ?assert(lists:member(maps:get(restart, Child), [permanent, transient, temporary])),
        Shutdown = maps:get(shutdown, Child),
        ?assert(is_integer(Shutdown) orelse Shutdown =:= infinity orelse Shutdown =:= brutal_kill),
        ?assert(lists:member(maps:get(type, Child), [worker, supervisor])),
        ?assert(is_list(maps:get(modules, Child)))
    end, Children),
    ok.

test_worker_shutdown() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

    Workers = [C || C <- Children, maps:get(type, C) =:= worker],
    lists:foreach(fun(Worker) ->
        Shutdown = maps:get(shutdown, Worker),
        ?assertEqual(5000, Shutdown)
    end, Workers),
    ok.

test_supervisor_shutdown() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

    Supervisors = [C || C <- Children, maps:get(type, C) =:= supervisor],
    lists:foreach(fun(Sup) ->
        Shutdown = maps:get(shutdown, Sup),
        ?assertEqual(infinity, Shutdown)
    end, Supervisors),
    ok.

%%====================================================================
%% Supervisor Start Link Tests
%%====================================================================

%% Note: start_link tests are inherently fragile because they actually
%% start a supervisor with real children. The core functionality (init/1)
%% is thoroughly tested above.

%%====================================================================
%% Helper Functions
%%====================================================================

find_child_by_id(Id, Children) ->
    case lists:filter(fun(Child) -> maps:get(id, Child) =:= Id end, Children) of
        [Child] -> Child;
        [] -> undefined
    end.

%%====================================================================
%% Supervisor Flag Validation Tests
%%====================================================================

sup_flags_validation_test_() ->
    [
        {"sup flags is a map", fun test_sup_flags_is_map/0},
        {"sup flags contains all required keys", fun test_sup_flags_required_keys/0}
    ].

test_sup_flags_is_map() ->
    {ok, {SupFlags, _Children}} = flurm_node_daemon_sup:init([]),
    ?assert(is_map(SupFlags)),
    ok.

test_sup_flags_required_keys() ->
    {ok, {SupFlags, _Children}} = flurm_node_daemon_sup:init([]),
    ?assert(maps:is_key(strategy, SupFlags)),
    ?assert(maps:is_key(intensity, SupFlags)),
    ?assert(maps:is_key(period, SupFlags)),
    ok.
