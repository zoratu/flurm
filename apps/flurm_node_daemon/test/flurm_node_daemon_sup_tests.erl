%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon Supervisor Tests
%%%
%%% Comprehensive EUnit tests for the flurm_node_daemon_sup module.
%%% Tests supervisor initialization, child specifications,
%%% and supervision strategy.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_sup_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start required applications
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    %% Stop supervisor if running
    case whereis(flurm_node_daemon_sup) of
        undefined -> ok;
        Pid ->
            unlink(Pid),
            exit(Pid, shutdown),
            timer:sleep(100)
    end,
    ok.

%%====================================================================
%% Module Info Tests
%%====================================================================

module_info_test_() ->
    [
        {"module_info/0 returns module information", fun() ->
            Info = flurm_node_daemon_sup:module_info(),
            ?assert(is_list(Info)),
            ?assertMatch({module, flurm_node_daemon_sup}, lists:keyfind(module, 1, Info)),
            ?assertMatch({exports, _}, lists:keyfind(exports, 1, Info))
        end},
        {"module_info/1 returns exports", fun() ->
            Exports = flurm_node_daemon_sup:module_info(exports),
            ?assert(is_list(Exports)),
            %% Check expected exports
            ?assert(lists:member({start_link, 0}, Exports)),
            ?assert(lists:member({init, 1}, Exports))
        end},
        {"module_info/1 returns attributes", fun() ->
            Attrs = flurm_node_daemon_sup:module_info(attributes),
            ?assert(is_list(Attrs)),
            %% Should have behaviour attribute
            BehaviourAttr = proplists:get_value(behaviour, Attrs,
                            proplists:get_value(behavior, Attrs, [])),
            ?assert(lists:member(supervisor, BehaviourAttr))
        end}
    ].

%%====================================================================
%% Supervisor Init Tests
%%====================================================================

init_test_() ->
    [
        {"init/1 returns valid supervisor specification", fun test_init_returns_valid_spec/0},
        {"init/1 uses one_for_one strategy", fun test_init_strategy/0},
        {"init/1 returns correct restart intensity", fun test_init_intensity/0},
        {"init/1 returns correct restart period", fun test_init_period/0},
        {"init/1 includes all required children", fun test_init_children/0}
    ].

test_init_returns_valid_spec() ->
    Result = flurm_node_daemon_sup:init([]),

    ?assertMatch({ok, {_, _}}, Result),

    {ok, {SupFlags, Children}} = Result,

    %% Verify SupFlags is a map
    ?assert(is_map(SupFlags)),

    %% Verify Children is a list
    ?assert(is_list(Children)),
    ok.

test_init_strategy() ->
    {ok, {SupFlags, _Children}} = flurm_node_daemon_sup:init([]),

    ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
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

test_init_children() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

    %% Should have 3 children
    ?assertEqual(3, length(Children)),

    %% Extract child IDs
    ChildIds = [maps:get(id, C) || C <- Children],

    %% Verify required children are present
    ?assert(lists:member(flurm_system_monitor, ChildIds)),
    ?assert(lists:member(flurm_controller_connector, ChildIds)),
    ?assert(lists:member(flurm_job_executor_sup, ChildIds)),
    ok.

%%====================================================================
%% Child Specification Tests
%%====================================================================

child_spec_test_() ->
    [
        {"system_monitor child spec is valid", fun test_system_monitor_child_spec/0},
        {"controller_connector child spec is valid", fun test_controller_connector_child_spec/0},
        {"job_executor_sup child spec is valid", fun test_job_executor_sup_child_spec/0}
    ].

test_system_monitor_child_spec() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

    %% Find system_monitor child
    [SystemMonitorSpec] = [C || C <- Children, maps:get(id, C) =:= flurm_system_monitor],

    %% Verify child spec structure
    ?assertEqual(flurm_system_monitor, maps:get(id, SystemMonitorSpec)),
    ?assertEqual({flurm_system_monitor, start_link, []}, maps:get(start, SystemMonitorSpec)),
    ?assertEqual(permanent, maps:get(restart, SystemMonitorSpec)),
    ?assertEqual(5000, maps:get(shutdown, SystemMonitorSpec)),
    ?assertEqual(worker, maps:get(type, SystemMonitorSpec)),
    ?assertEqual([flurm_system_monitor], maps:get(modules, SystemMonitorSpec)),
    ok.

test_controller_connector_child_spec() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

    %% Find controller_connector child
    [ConnectorSpec] = [C || C <- Children, maps:get(id, C) =:= flurm_controller_connector],

    %% Verify child spec structure
    ?assertEqual(flurm_controller_connector, maps:get(id, ConnectorSpec)),
    ?assertEqual({flurm_controller_connector, start_link, []}, maps:get(start, ConnectorSpec)),
    ?assertEqual(permanent, maps:get(restart, ConnectorSpec)),
    ?assertEqual(5000, maps:get(shutdown, ConnectorSpec)),
    ?assertEqual(worker, maps:get(type, ConnectorSpec)),
    ?assertEqual([flurm_controller_connector], maps:get(modules, ConnectorSpec)),
    ok.

test_job_executor_sup_child_spec() ->
    {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

    %% Find job_executor_sup child
    [ExecutorSupSpec] = [C || C <- Children, maps:get(id, C) =:= flurm_job_executor_sup],

    %% Verify child spec structure
    ?assertEqual(flurm_job_executor_sup, maps:get(id, ExecutorSupSpec)),
    ?assertEqual({flurm_job_executor_sup, start_link, []}, maps:get(start, ExecutorSupSpec)),
    ?assertEqual(permanent, maps:get(restart, ExecutorSupSpec)),
    ?assertEqual(infinity, maps:get(shutdown, ExecutorSupSpec)),  % Supervisor should have infinity shutdown
    ?assertEqual(supervisor, maps:get(type, ExecutorSupSpec)),  % This is a supervisor, not a worker
    ?assertEqual([flurm_job_executor_sup], maps:get(modules, ExecutorSupSpec)),
    ok.

%%====================================================================
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"start_link/0 starts supervisor with correct name", fun test_start_link_name/0}
     ]}.

test_start_link_name() ->
    %% Mock the child modules to prevent actual startup errors
    meck:new(flurm_system_monitor, [non_strict]),
    meck:new(flurm_controller_connector, [non_strict]),
    meck:new(flurm_job_executor_sup, [non_strict]),

    meck:expect(flurm_system_monitor, start_link, fun() ->
        {ok, spawn_link(fun() -> receive stop -> ok end end)}
    end),
    meck:expect(flurm_controller_connector, start_link, fun() ->
        {ok, spawn_link(fun() -> receive stop -> ok end end)}
    end),
    meck:expect(flurm_job_executor_sup, start_link, fun() ->
        {ok, spawn_link(fun() -> receive stop -> ok end end)}
    end),

    try
        %% Stop if already running
        case whereis(flurm_node_daemon_sup) of
            undefined -> ok;
            OldPid ->
                unlink(OldPid),
                exit(OldPid, shutdown),
                timer:sleep(100)
        end,

        Result = flurm_node_daemon_sup:start_link(),

        ?assertMatch({ok, _Pid}, Result),
        {ok, Pid} = Result,
        ?assert(is_pid(Pid)),
        ?assert(is_process_alive(Pid)),

        %% Verify registered name
        ?assertEqual(Pid, whereis(flurm_node_daemon_sup)),

        %% Cleanup
        exit(Pid, shutdown),
        timer:sleep(100)
    after
        meck:unload(flurm_system_monitor),
        meck:unload(flurm_controller_connector),
        meck:unload(flurm_job_executor_sup)
    end,
    ok.

%%====================================================================
%% Behaviour Implementation Tests
%%====================================================================

behaviour_test_() ->
    [
        {"implements supervisor behaviour", fun() ->
            Attrs = flurm_node_daemon_sup:module_info(attributes),
            Behaviours = proplists:get_value(behaviour, Attrs, []) ++
                         proplists:get_value(behavior, Attrs, []),
            ?assert(lists:member(supervisor, Behaviours))
        end},
        {"exports required supervisor callbacks", fun() ->
            Exports = flurm_node_daemon_sup:module_info(exports),
            ?assert(lists:member({init, 1}, Exports))
        end},
        {"exports start_link/0", fun() ->
            Exports = flurm_node_daemon_sup:module_info(exports),
            ?assert(lists:member({start_link, 0}, Exports))
        end}
    ].

%%====================================================================
%% Child Order Tests
%%====================================================================

child_order_test_() ->
    [
        {"children start in correct order", fun() ->
            {ok, {_SupFlags, Children}} = flurm_node_daemon_sup:init([]),

            ChildIds = [maps:get(id, C) || C <- Children],

            %% System monitor should start first (before connector)
            SystemMonitorIdx = find_index(flurm_system_monitor, ChildIds),
            ConnectorIdx = find_index(flurm_controller_connector, ChildIds),
            ExecutorIdx = find_index(flurm_job_executor_sup, ChildIds),

            %% System monitor should be first
            ?assertEqual(1, SystemMonitorIdx),
            %% Connector should be second
            ?assertEqual(2, ConnectorIdx),
            %% Executor sup should be third
            ?assertEqual(3, ExecutorIdx)
        end}
    ].

find_index(Element, List) ->
    find_index(Element, List, 1).

find_index(_, [], _) -> 0;
find_index(Element, [Element|_], Index) -> Index;
find_index(Element, [_|Rest], Index) -> find_index(Element, Rest, Index + 1).

%%====================================================================
%% Supervisor Flags Validation Tests
%%====================================================================

sup_flags_validation_test_() ->
    [
        {"supervisor flags contain all required keys", fun() ->
            {ok, {SupFlags, _}} = flurm_node_daemon_sup:init([]),

            ?assert(maps:is_key(strategy, SupFlags)),
            ?assert(maps:is_key(intensity, SupFlags)),
            ?assert(maps:is_key(period, SupFlags))
        end},
        {"intensity is non-negative integer", fun() ->
            {ok, {SupFlags, _}} = flurm_node_daemon_sup:init([]),

            Intensity = maps:get(intensity, SupFlags),
            ?assert(is_integer(Intensity)),
            ?assert(Intensity >= 0)
        end},
        {"period is positive integer", fun() ->
            {ok, {SupFlags, _}} = flurm_node_daemon_sup:init([]),

            Period = maps:get(period, SupFlags),
            ?assert(is_integer(Period)),
            ?assert(Period > 0)
        end}
    ].

%%====================================================================
%% Child Spec Validation Tests
%%====================================================================

child_spec_validation_test_() ->
    [
        {"all child specs have required keys", fun() ->
            {ok, {_, Children}} = flurm_node_daemon_sup:init([]),

            RequiredKeys = [id, start, restart, shutdown, type, modules],

            lists:foreach(fun(ChildSpec) ->
                lists:foreach(fun(Key) ->
                    ?assert(maps:is_key(Key, ChildSpec),
                           lists:flatten(io_lib:format(
                               "Child ~p missing key ~p",
                               [maps:get(id, ChildSpec), Key])))
                end, RequiredKeys)
            end, Children)
        end},
        {"all child restart values are valid", fun() ->
            {ok, {_, Children}} = flurm_node_daemon_sup:init([]),

            ValidRestartValues = [permanent, transient, temporary],

            lists:foreach(fun(ChildSpec) ->
                Restart = maps:get(restart, ChildSpec),
                ?assert(lists:member(Restart, ValidRestartValues),
                       lists:flatten(io_lib:format(
                           "Child ~p has invalid restart: ~p",
                           [maps:get(id, ChildSpec), Restart])))
            end, Children)
        end},
        {"all child type values are valid", fun() ->
            {ok, {_, Children}} = flurm_node_daemon_sup:init([]),

            ValidTypeValues = [worker, supervisor],

            lists:foreach(fun(ChildSpec) ->
                Type = maps:get(type, ChildSpec),
                ?assert(lists:member(Type, ValidTypeValues),
                       lists:flatten(io_lib:format(
                           "Child ~p has invalid type: ~p",
                           [maps:get(id, ChildSpec), Type])))
            end, Children)
        end},
        {"all child start tuples are valid MFA", fun() ->
            {ok, {_, Children}} = flurm_node_daemon_sup:init([]),

            lists:foreach(fun(ChildSpec) ->
                Start = maps:get(start, ChildSpec),
                ?assertMatch({_, _, _}, Start),
                {M, F, A} = Start,
                ?assert(is_atom(M)),
                ?assert(is_atom(F)),
                ?assert(is_list(A))
            end, Children)
        end},
        {"all child shutdown values are valid", fun() ->
            {ok, {_, Children}} = flurm_node_daemon_sup:init([]),

            lists:foreach(fun(ChildSpec) ->
                Shutdown = maps:get(shutdown, ChildSpec),
                Type = maps:get(type, ChildSpec),
                case Type of
                    supervisor ->
                        %% Supervisors should have infinity or positive integer
                        ?assert(Shutdown =:= infinity orelse
                                (is_integer(Shutdown) andalso Shutdown > 0));
                    worker ->
                        %% Workers should have positive integer or brutal_kill
                        ?assert(Shutdown =:= brutal_kill orelse
                                (is_integer(Shutdown) andalso Shutdown > 0))
                end
            end, Children)
        end}
    ].
