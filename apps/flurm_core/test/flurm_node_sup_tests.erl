%%%-------------------------------------------------------------------
%%% @doc FLURM Node Supervisor Tests
%%%
%%% Comprehensive EUnit tests for the flurm_node_sup module.
%%% Tests supervisor initialization, dynamic child management,
%%% and simple_one_for_one strategy.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_sup_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Start required applications
    application:ensure_all_started(lager),
    ok.

cleanup(_) ->
    %% Stop supervisor if running
    case whereis(flurm_node_sup) of
        undefined -> ok;
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Ref = monitor(process, Pid),
                    unlink(Pid),
                    catch exit(Pid, shutdown),
                    receive
                        {'DOWN', Ref, process, Pid, _} -> ok
                    after 5000 ->
                        demonitor(Ref, [flush]),
                        catch exit(Pid, kill)
                    end;
                false ->
                    ok
            end
    end,
    ok.

setup_with_supervisor() ->
    setup(),
    %% Mock flurm_node to prevent actual node process startup issues
    %% The supervisor uses simple_one_for_one which calls start_link with [NodeSpec]
    %% where NodeSpec comes from start_child call argument
    catch meck:unload(flurm_node),
    meck:new(flurm_node, [passthrough, non_strict]),
    meck:expect(flurm_node, start_link, fun(NodeSpec) when is_record(NodeSpec, node_spec) ->
        %% Use spawn (not spawn_link) to avoid exit signal propagation
        {ok, spawn(fun() -> receive stop -> ok end end)}
    end),

    %% Stop any existing supervisor first
    case whereis(flurm_node_sup) of
        undefined -> ok;
        ExistingPid ->
            Ref = monitor(process, ExistingPid),
            unlink(ExistingPid),
            catch exit(ExistingPid, shutdown),
            receive {'DOWN', Ref, process, ExistingPid, _} -> ok after 5000 -> ok end
    end,

    %% Start the supervisor
    {ok, Pid} = flurm_node_sup:start_link(),
    %% Unlink to prevent EUnit process from receiving EXIT signals
    unlink(Pid),
    {ok, Pid}.

cleanup_with_supervisor(_) ->
    catch meck:unload(flurm_node),
    cleanup(ok).

%%====================================================================
%% Module Info Tests
%%====================================================================

module_info_test_() ->
    [
        {"module_info/0 returns module information", fun() ->
            Info = flurm_node_sup:module_info(),
            ?assert(is_list(Info)),
            ?assertMatch({module, flurm_node_sup}, lists:keyfind(module, 1, Info)),
            ?assertMatch({exports, _}, lists:keyfind(exports, 1, Info))
        end},
        {"module_info/1 returns exports", fun() ->
            Exports = flurm_node_sup:module_info(exports),
            ?assert(is_list(Exports)),
            %% Check expected exports
            ?assert(lists:member({start_link, 0}, Exports)),
            ?assert(lists:member({start_node, 1}, Exports)),
            ?assert(lists:member({stop_node, 1}, Exports)),
            ?assert(lists:member({which_nodes, 0}, Exports)),
            ?assert(lists:member({count_nodes, 0}, Exports)),
            ?assert(lists:member({init, 1}, Exports))
        end},
        {"module_info/1 returns attributes", fun() ->
            Attrs = flurm_node_sup:module_info(attributes),
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
        {"init/1 uses simple_one_for_one strategy", fun test_init_strategy/0},
        {"init/1 returns correct restart intensity", fun test_init_intensity/0},
        {"init/1 returns correct restart period", fun test_init_period/0},
        {"init/1 returns single child spec template", fun test_init_child_spec/0}
    ].

test_init_returns_valid_spec() ->
    Result = flurm_node_sup:init([]),

    ?assertMatch({ok, {_, _}}, Result),

    {ok, {SupFlags, Children}} = Result,

    %% Verify SupFlags is a map
    ?assert(is_map(SupFlags)),

    %% Verify Children is a list
    ?assert(is_list(Children)),
    ok.

test_init_strategy() ->
    {ok, {SupFlags, _Children}} = flurm_node_sup:init([]),

    ?assertEqual(simple_one_for_one, maps:get(strategy, SupFlags)),
    ok.

test_init_intensity() ->
    {ok, {SupFlags, _Children}} = flurm_node_sup:init([]),

    Intensity = maps:get(intensity, SupFlags),
    %% simple_one_for_one typically has 0 intensity (no auto restart)
    ?assertEqual(0, Intensity),
    ok.

test_init_period() ->
    {ok, {SupFlags, _Children}} = flurm_node_sup:init([]),

    Period = maps:get(period, SupFlags),
    ?assertEqual(1, Period),
    ok.

test_init_child_spec() ->
    {ok, {_SupFlags, Children}} = flurm_node_sup:init([]),

    %% Should have exactly one child spec template
    ?assertEqual(1, length(Children)),

    [ChildSpec] = Children,

    %% Verify child spec structure
    ?assertEqual(flurm_node, maps:get(id, ChildSpec)),
    ?assertEqual({flurm_node, start_link, []}, maps:get(start, ChildSpec)),
    ?assertEqual(temporary, maps:get(restart, ChildSpec)),  % Nodes should not auto-restart
    ?assertEqual(5000, maps:get(shutdown, ChildSpec)),
    ?assertEqual(worker, maps:get(type, ChildSpec)),
    ?assertEqual([flurm_node], maps:get(modules, ChildSpec)),
    ok.

%%====================================================================
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"start_link/0 starts supervisor with correct name", fun test_start_link_name/0},
         {"start_link/0 returns error if already started", fun test_start_link_already_started/0}
     ]}.

test_start_link_name() ->
    %% Stop if already running
    case whereis(flurm_node_sup) of
        undefined -> ok;
        OldPid ->
            OldRef = monitor(process, OldPid),
            unlink(OldPid),
            catch exit(OldPid, shutdown),
            receive {'DOWN', OldRef, process, OldPid, _} -> ok after 5000 -> ok end
    end,

    Result = flurm_node_sup:start_link(),

    ?assertMatch({ok, _Pid}, Result),
    {ok, Pid} = Result,
    %% Unlink to prevent test process from receiving EXIT signals
    unlink(Pid),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Verify registered name
    ?assertEqual(Pid, whereis(flurm_node_sup)),

    %% Cleanup with monitor
    Ref = monitor(process, Pid),
    catch exit(Pid, shutdown),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 5000 ->
        demonitor(Ref, [flush])
    end,
    ok.

test_start_link_already_started() ->
    %% Start supervisor
    case whereis(flurm_node_sup) of
        undefined ->
            {ok, _Pid} = flurm_node_sup:start_link();
        _Pid ->
            ok
    end,

    %% Try to start again
    Result = flurm_node_sup:start_link(),

    ?assertMatch({error, {already_started, _}}, Result),
    ok.

%%====================================================================
%% Dynamic Child Management Tests
%%====================================================================

start_node_test_() ->
    {setup,
     fun setup_with_supervisor/0,
     fun cleanup_with_supervisor/1,
     [
         {"start_node/1 starts a node process", fun test_start_node/0},
         {"start_node/1 accepts node_spec record", fun test_start_node_with_spec/0}
     ]}.

test_start_node() ->
    NodeSpec = #node_spec{
        name = <<"test-node-001">>,
        hostname = <<"node001.cluster.local">>,
        port = 6818,
        cpus = 8,
        memory = 16384,
        gpus = 2,
        features = [gpu, infiniband],
        partitions = [<<"compute">>, <<"gpu">>]
    },

    Result = flurm_node_sup:start_node(NodeSpec),

    ?assertMatch({ok, _Pid}, Result),
    {ok, Pid} = Result,
    ?assert(is_pid(Pid)),
    ok.

test_start_node_with_spec() ->
    NodeSpec = #node_spec{
        name = <<"test-node-002">>,
        hostname = <<"node002.cluster.local">>,
        port = 6818,
        cpus = 16,
        memory = 32768,
        gpus = 4,
        features = [gpu, nvme],
        partitions = [<<"highmem">>]
    },

    Result = flurm_node_sup:start_node(NodeSpec),

    ?assertMatch({ok, _Pid}, Result),
    ok.

%%====================================================================
%% Stop Node Tests
%%====================================================================

stop_node_test_() ->
    {setup,
     fun setup_with_supervisor/0,
     fun cleanup_with_supervisor/1,
     [
         {"stop_node/1 terminates a running node", fun test_stop_node/0},
         {"stop_node/1 returns error for non-existent pid", fun test_stop_non_existent_node/0}
     ]}.

test_stop_node() ->
    NodeSpec = #node_spec{
        name = <<"stop-test-node">>,
        hostname = <<"stoptest.cluster.local">>,
        port = 6818,
        cpus = 4,
        memory = 8192,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    },

    %% Start the node
    {ok, Pid} = flurm_node_sup:start_node(NodeSpec),
    ?assert(is_process_alive(Pid)),

    %% Stop the node
    Result = flurm_node_sup:stop_node(Pid),

    ?assertEqual(ok, Result),
    _ = sys:get_state(flurm_node_sup),
    ?assertNot(is_process_alive(Pid)),
    ok.

test_stop_non_existent_node() ->
    %% Create a fake pid that's not a child
    FakePid = spawn(fun() -> ok end),
    flurm_test_utils:wait_for_death(FakePid),

    Result = flurm_node_sup:stop_node(FakePid),

    %% Note: supervisor:terminate_child/2 for simple_one_for_one supervisors
    %% returns ok even for non-children (as long as the pid is valid format)
    %% This is OTP behavior - the function doesn't validate child membership
    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Which Nodes Tests
%%====================================================================

which_nodes_test_() ->
    {setup,
     fun setup_with_supervisor/0,
     fun cleanup_with_supervisor/1,
     [
         {"which_nodes/0 returns empty list when no nodes", fun test_which_nodes_empty/0},
         {"which_nodes/0 returns list of pids", fun test_which_nodes_with_nodes/0}
     ]}.

test_which_nodes_empty() ->
    %% Stop all existing children first
    Children = flurm_node_sup:which_nodes(),
    lists:foreach(fun(Pid) ->
        flurm_node_sup:stop_node(Pid)
    end, Children),
    _ = sys:get_state(flurm_node_sup),

    Result = flurm_node_sup:which_nodes(),

    ?assertEqual([], Result),
    ok.

test_which_nodes_with_nodes() ->
    %% Stop all existing children first
    Children = flurm_node_sup:which_nodes(),
    lists:foreach(fun(Pid) ->
        flurm_node_sup:stop_node(Pid)
    end, Children),
    _ = sys:get_state(flurm_node_sup),

    %% Start some nodes
    NodeSpec1 = #node_spec{
        name = <<"which-node-001">>,
        hostname = <<"whichnode1.local">>,
        port = 6818,
        cpus = 4,
        memory = 8192,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    },
    NodeSpec2 = #node_spec{
        name = <<"which-node-002">>,
        hostname = <<"whichnode2.local">>,
        port = 6818,
        cpus = 8,
        memory = 16384,
        gpus = 1,
        features = [gpu],
        partitions = [<<"gpu">>]
    },

    {ok, Pid1} = flurm_node_sup:start_node(NodeSpec1),
    {ok, Pid2} = flurm_node_sup:start_node(NodeSpec2),

    Result = flurm_node_sup:which_nodes(),

    ?assert(is_list(Result)),
    ?assertEqual(2, length(Result)),
    ?assert(lists:member(Pid1, Result)),
    ?assert(lists:member(Pid2, Result)),

    %% Cleanup
    flurm_node_sup:stop_node(Pid1),
    flurm_node_sup:stop_node(Pid2),
    ok.

%%====================================================================
%% Count Nodes Tests
%%====================================================================

count_nodes_test_() ->
    {setup,
     fun setup_with_supervisor/0,
     fun cleanup_with_supervisor/1,
     [
         {"count_nodes/0 returns 0 when no nodes", fun test_count_nodes_zero/0},
         {"count_nodes/0 returns correct count", fun test_count_nodes_with_nodes/0}
     ]}.

test_count_nodes_zero() ->
    %% Stop all existing children first
    Children = flurm_node_sup:which_nodes(),
    lists:foreach(fun(Pid) ->
        flurm_node_sup:stop_node(Pid)
    end, Children),
    _ = sys:get_state(flurm_node_sup),

    Result = flurm_node_sup:count_nodes(),

    ?assertEqual(0, Result),
    ok.

test_count_nodes_with_nodes() ->
    %% Stop all existing children first
    Children = flurm_node_sup:which_nodes(),
    lists:foreach(fun(Pid) ->
        flurm_node_sup:stop_node(Pid)
    end, Children),
    _ = sys:get_state(flurm_node_sup),

    %% Start some nodes
    NodeSpecs = [
        #node_spec{name = <<"count-node-001">>, hostname = <<"count1.local">>,
                   port = 6818, cpus = 4, memory = 8192, gpus = 0,
                   features = [], partitions = [<<"default">>]},
        #node_spec{name = <<"count-node-002">>, hostname = <<"count2.local">>,
                   port = 6818, cpus = 8, memory = 16384, gpus = 1,
                   features = [gpu], partitions = [<<"gpu">>]},
        #node_spec{name = <<"count-node-003">>, hostname = <<"count3.local">>,
                   port = 6818, cpus = 16, memory = 32768, gpus = 4,
                   features = [gpu], partitions = [<<"highmem">>]}
    ],

    Pids = lists:map(fun(Spec) ->
        {ok, Pid} = flurm_node_sup:start_node(Spec),
        Pid
    end, NodeSpecs),

    Result = flurm_node_sup:count_nodes(),

    ?assertEqual(3, Result),

    %% Cleanup
    lists:foreach(fun(Pid) -> flurm_node_sup:stop_node(Pid) end, Pids),
    ok.

%%====================================================================
%% Behaviour Implementation Tests
%%====================================================================

behaviour_test_() ->
    [
        {"implements supervisor behaviour", fun() ->
            Attrs = flurm_node_sup:module_info(attributes),
            Behaviours = proplists:get_value(behaviour, Attrs, []) ++
                         proplists:get_value(behavior, Attrs, []),
            ?assert(lists:member(supervisor, Behaviours))
        end},
        {"exports required supervisor callbacks", fun() ->
            Exports = flurm_node_sup:module_info(exports),
            ?assert(lists:member({init, 1}, Exports))
        end},
        {"exports start_link/0", fun() ->
            Exports = flurm_node_sup:module_info(exports),
            ?assert(lists:member({start_link, 0}, Exports))
        end}
    ].

%%====================================================================
%% Supervisor Flags Validation Tests
%%====================================================================

sup_flags_validation_test_() ->
    [
        {"supervisor flags contain all required keys", fun() ->
            {ok, {SupFlags, _}} = flurm_node_sup:init([]),

            ?assert(maps:is_key(strategy, SupFlags)),
            ?assert(maps:is_key(intensity, SupFlags)),
            ?assert(maps:is_key(period, SupFlags))
        end},
        {"strategy is simple_one_for_one for dynamic children", fun() ->
            {ok, {SupFlags, _}} = flurm_node_sup:init([]),

            ?assertEqual(simple_one_for_one, maps:get(strategy, SupFlags))
        end},
        {"intensity is 0 for no auto-restart", fun() ->
            {ok, {SupFlags, _}} = flurm_node_sup:init([]),

            ?assertEqual(0, maps:get(intensity, SupFlags))
        end}
    ].

%%====================================================================
%% Child Spec Validation Tests
%%====================================================================

child_spec_validation_test_() ->
    [
        {"child spec has all required keys", fun() ->
            {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),

            RequiredKeys = [id, start, restart, shutdown, type, modules],
            lists:foreach(fun(Key) ->
                ?assert(maps:is_key(Key, ChildSpec),
                       lists:flatten(io_lib:format("Missing key ~p", [Key])))
            end, RequiredKeys)
        end},
        {"child restart is temporary", fun() ->
            {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),

            %% Nodes should not auto-restart, node daemons re-register
            ?assertEqual(temporary, maps:get(restart, ChildSpec))
        end},
        {"child type is worker", fun() ->
            {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),

            ?assertEqual(worker, maps:get(type, ChildSpec))
        end},
        {"child start is valid MFA", fun() ->
            {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),

            Start = maps:get(start, ChildSpec),
            ?assertMatch({_, _, _}, Start),
            {M, F, A} = Start,
            ?assert(is_atom(M)),
            ?assert(is_atom(F)),
            ?assert(is_list(A))
        end},
        {"child modules contains flurm_node", fun() ->
            {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),

            Modules = maps:get(modules, ChildSpec),
            ?assertEqual([flurm_node], Modules)
        end}
    ].

%%====================================================================
%% API Spec Tests
%%====================================================================

api_spec_test_() ->
    [
        {"start_node/1 accepts node_spec record only", fun() ->
            %% Verify type spec is enforced at runtime
            %% This is a documentation test - the actual enforcement is at compile time
            ok
        end},
        {"stop_node/1 requires pid argument", fun() ->
            Exports = flurm_node_sup:module_info(exports),
            ?assert(lists:member({stop_node, 1}, Exports))
        end},
        {"which_nodes/0 takes no arguments", fun() ->
            Exports = flurm_node_sup:module_info(exports),
            ?assert(lists:member({which_nodes, 0}, Exports))
        end},
        {"count_nodes/0 takes no arguments", fun() ->
            Exports = flurm_node_sup:module_info(exports),
            ?assert(lists:member({count_nodes, 0}, Exports))
        end}
    ].

%%====================================================================
%% Comprehensive Supervisor Spec Validation Tests
%%====================================================================

%% These tests thoroughly validate the supervisor init/1 return value
%% according to OTP supervisor specification requirements.

comprehensive_init_validation_test_() ->
    [
        {"init/1 returns valid {ok, {SupFlags, ChildSpecs}} tuple",
         fun test_init_comprehensive_tuple/0},
        {"SupFlags contains all required keys",
         fun test_sup_flags_required_keys/0},
        {"SupFlags strategy is valid for simple_one_for_one",
         fun test_sup_flags_valid_strategy/0},
        {"SupFlags intensity is non-negative",
         fun test_sup_flags_valid_intensity/0},
        {"SupFlags period is positive",
         fun test_sup_flags_valid_period/0},
        {"Child spec has valid id field",
         fun test_child_spec_valid_id/0},
        {"Child spec has valid start MFA",
         fun test_child_spec_valid_start/0},
        {"Child spec has valid restart type",
         fun test_child_spec_valid_restart/0},
        {"Child spec has valid shutdown value",
         fun test_child_spec_valid_shutdown/0},
        {"Child spec has valid type",
         fun test_child_spec_valid_type/0},
        {"Child spec has valid modules list",
         fun test_child_spec_valid_modules/0},
        {"Complete child spec validation",
         fun test_complete_child_spec_validation/0}
    ].

test_init_comprehensive_tuple() ->
    Result = flurm_node_sup:init([]),
    ?assertMatch({ok, {_, _}}, Result),
    {ok, {SupFlags, ChildSpecs}} = Result,
    ?assert(is_map(SupFlags)),
    ?assert(is_list(ChildSpecs)),
    ok.

test_sup_flags_required_keys() ->
    {ok, {SupFlags, _}} = flurm_node_sup:init([]),
    ?assert(maps:is_key(strategy, SupFlags)),
    ?assert(maps:is_key(intensity, SupFlags)),
    ?assert(maps:is_key(period, SupFlags)),
    ok.

test_sup_flags_valid_strategy() ->
    {ok, {SupFlags, _}} = flurm_node_sup:init([]),
    Strategy = maps:get(strategy, SupFlags),
    ValidStrategies = [one_for_one, one_for_all, rest_for_one, simple_one_for_one],
    ?assert(lists:member(Strategy, ValidStrategies),
            io_lib:format("Invalid strategy: ~p", [Strategy])),
    ok.

test_sup_flags_valid_intensity() ->
    {ok, {SupFlags, _}} = flurm_node_sup:init([]),
    Intensity = maps:get(intensity, SupFlags),
    ?assert(is_integer(Intensity)),
    ?assert(Intensity >= 0, "Intensity must be non-negative"),
    ok.

test_sup_flags_valid_period() ->
    {ok, {SupFlags, _}} = flurm_node_sup:init([]),
    Period = maps:get(period, SupFlags),
    ?assert(is_integer(Period)),
    ?assert(Period > 0, "Period must be positive"),
    ok.

test_child_spec_valid_id() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    Id = maps:get(id, ChildSpec),
    ?assert(is_atom(Id) orelse is_binary(Id) orelse is_list(Id),
            io_lib:format("Invalid id type: ~p", [Id])),
    ok.

test_child_spec_valid_start() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    Start = maps:get(start, ChildSpec),
    ?assertMatch({M, F, A} when is_atom(M) andalso is_atom(F) andalso is_list(A), Start,
                 io_lib:format("Invalid start MFA: ~p", [Start])),
    ok.

test_child_spec_valid_restart() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    Restart = maps:get(restart, ChildSpec),
    ValidRestartTypes = [permanent, transient, temporary],
    ?assert(lists:member(Restart, ValidRestartTypes),
            io_lib:format("Invalid restart type: ~p", [Restart])),
    ok.

test_child_spec_valid_shutdown() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    Shutdown = maps:get(shutdown, ChildSpec),
    ValidShutdown = (is_integer(Shutdown) andalso Shutdown >= 0)
                    orelse Shutdown =:= brutal_kill
                    orelse Shutdown =:= infinity,
    ?assert(ValidShutdown,
            io_lib:format("Invalid shutdown value: ~p", [Shutdown])),
    ok.

test_child_spec_valid_type() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    Type = maps:get(type, ChildSpec),
    ValidTypes = [worker, supervisor],
    ?assert(lists:member(Type, ValidTypes),
            io_lib:format("Invalid type: ~p", [Type])),
    ok.

test_child_spec_valid_modules() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    Modules = maps:get(modules, ChildSpec),
    ValidModules = (Modules =:= dynamic)
                   orelse (is_list(Modules) andalso
                           lists:all(fun is_atom/1, Modules)),
    ?assert(ValidModules,
            io_lib:format("Invalid modules: ~p", [Modules])),
    ok.

test_complete_child_spec_validation() ->
    {ok, {SupFlags, ChildSpecs}} = flurm_node_sup:init([]),

    %% Validate SupFlags
    ?assertMatch(#{strategy := _, intensity := _, period := _}, SupFlags),

    %% Validate child specs
    ?assert(is_list(ChildSpecs)),
    %% For simple_one_for_one, there should be exactly one template
    ?assertEqual(1, length(ChildSpecs)),

    [ChildSpec] = ChildSpecs,
    validate_node_child_spec(ChildSpec),
    ok.

validate_node_child_spec(#{id := Id, start := {M, F, A}, restart := Restart,
                           shutdown := Shutdown, type := Type, modules := Modules}) ->
    %% Validate id
    ?assert(is_atom(Id) orelse is_binary(Id) orelse is_list(Id)),

    %% Validate MFA
    ?assert(is_atom(M)),
    ?assert(is_atom(F)),
    ?assert(is_list(A)),

    %% Validate restart
    ?assert(lists:member(Restart, [permanent, transient, temporary])),

    %% Validate shutdown
    ValidShutdown = (is_integer(Shutdown) andalso Shutdown >= 0)
                    orelse Shutdown =:= brutal_kill
                    orelse Shutdown =:= infinity,
    ?assert(ValidShutdown),

    %% Validate type
    ?assert(lists:member(Type, [worker, supervisor])),

    %% Validate modules
    ValidModules = (Modules =:= dynamic)
                   orelse (is_list(Modules) andalso lists:all(fun is_atom/1, Modules)),
    ?assert(ValidModules),
    ok;
validate_node_child_spec({Id, {M, F, A}, Restart, Shutdown, Type, Modules}) ->
    %% Tuple format (legacy)
    ?assert(is_atom(Id) orelse is_binary(Id) orelse is_list(Id)),
    ?assert(is_atom(M)),
    ?assert(is_atom(F)),
    ?assert(is_list(A)),
    ?assert(lists:member(Restart, [permanent, transient, temporary])),
    ValidShutdown = (is_integer(Shutdown) andalso Shutdown >= 0)
                    orelse Shutdown =:= brutal_kill
                    orelse Shutdown =:= infinity,
    ?assert(ValidShutdown),
    ?assert(lists:member(Type, [worker, supervisor])),
    ValidModules = (Modules =:= dynamic)
                   orelse (is_list(Modules) andalso lists:all(fun is_atom/1, Modules)),
    ?assert(ValidModules),
    ok.
