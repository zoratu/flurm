%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_node_sup module
%%%
%%% Tests the simple_one_for_one supervisor for node processes.
%%% Validates init/1 returns valid specs, and tests dynamic child
%%% management with mocked node processes.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_sup_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% init/1 Tests
%%====================================================================

init_returns_valid_spec_test() ->
    {ok, {SupFlags, ChildSpecs}} = flurm_node_sup:init([]),
    %% For simple_one_for_one, should have exactly one child template
    ?assertMatch(#{strategy := simple_one_for_one}, SupFlags),
    ?assertEqual(1, length(ChildSpecs)).

init_sup_flags_test() ->
    {ok, {SupFlags, _}} = flurm_node_sup:init([]),
    %% Verify all expected supervisor flags
    ?assertMatch(#{strategy := simple_one_for_one,
                   intensity := 0,
                   period := 1}, SupFlags).

child_spec_valid_test() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    %% Validate child spec fields
    ?assertMatch(#{id := flurm_node,
                   start := {flurm_node, start_link, []},
                   restart := temporary,
                   shutdown := 5000,
                   type := worker,
                   modules := [flurm_node]}, ChildSpec).

child_spec_id_test() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    ?assertEqual(flurm_node, maps:get(id, ChildSpec)).

child_spec_start_test() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    ?assertEqual({flurm_node, start_link, []}, maps:get(start, ChildSpec)).

child_spec_restart_temporary_test() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    %% Nodes use temporary restart - not restarted automatically
    ?assertEqual(temporary, maps:get(restart, ChildSpec)).

child_spec_shutdown_test() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    %% 5 second graceful shutdown
    ?assertEqual(5000, maps:get(shutdown, ChildSpec)).

child_spec_type_worker_test() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    ?assertEqual(worker, maps:get(type, ChildSpec)).

child_spec_modules_test() ->
    {ok, {_, [ChildSpec]}} = flurm_node_sup:init([]),
    ?assertEqual([flurm_node], maps:get(modules, ChildSpec)).

%%====================================================================
%% Dynamic Child Management Tests (with mocking)
%%====================================================================

start_child_test_() ->
    {setup,
     fun setup_mocked_supervisor/0,
     fun cleanup_mocked_supervisor/1,
     fun(_Pid) ->
         [
             {"can start child with node spec", fun test_start_child/0},
             {"can start multiple children", fun test_start_multiple_children/0},
             {"which_nodes returns running nodes", fun test_which_nodes/0},
             {"count_nodes returns correct count", fun test_count_nodes/0},
             {"stop_node terminates child", fun test_stop_node/0}
         ]
     end}.

setup_mocked_supervisor() ->
    %% Unload any existing mocks to prevent conflicts in parallel tests
    catch meck:unload(flurm_node),
    %% Mock the flurm_node module to avoid starting real processes
    meck:new(flurm_node, [passthrough, non_strict]),
    meck:expect(flurm_node, start_link, fun(_NodeSpec) ->
        %% Use spawn (not spawn_link) to avoid linked exits affecting test runner
        Pid = spawn(fun() -> mock_node_loop() end),
        {ok, Pid}
    end),

    %% Start the supervisor (unlinked from test process)
    {ok, Pid} = flurm_node_sup:start_link(),
    unlink(Pid),
    Pid.

cleanup_mocked_supervisor(Pid) ->
    %% Unload meck first to prevent issues
    catch meck:unload(flurm_node),

    %% Stop all children first
    case erlang:is_process_alive(Pid) of
        true ->
            lists:foreach(fun(ChildPid) ->
                catch flurm_node_sup:stop_node(ChildPid)
            end, catch flurm_node_sup:which_nodes()),
            %% Stop the supervisor gracefully
            catch gen_server:stop(Pid, shutdown, 1000);
        false ->
            ok
    end,
    ok.

mock_node_loop() ->
    receive
        stop -> ok;
        _ -> mock_node_loop()
    after
        60000 -> ok  %% Auto-terminate after 60s to prevent test hangs
    end.

test_start_child() ->
    NodeSpec = #node_spec{
        name = <<"node1">>,
        hostname = <<"node1.example.com">>,
        port = 6818,
        cpus = 8,
        memory = 16384,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    },
    {ok, Child} = flurm_node_sup:start_node(NodeSpec),
    ?assert(is_pid(Child)),
    ?assert(is_process_alive(Child)),
    %% Clean up
    flurm_node_sup:stop_node(Child).

test_start_multiple_children() ->
    NodeSpec1 = #node_spec{
        name = <<"multi_node1">>,
        hostname = <<"multi_node1.example.com">>,
        port = 6818,
        cpus = 4,
        memory = 8192,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    },
    NodeSpec2 = #node_spec{
        name = <<"multi_node2">>,
        hostname = <<"multi_node2.example.com">>,
        port = 6818,
        cpus = 8,
        memory = 16384,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    },
    {ok, Child1} = flurm_node_sup:start_node(NodeSpec1),
    {ok, Child2} = flurm_node_sup:start_node(NodeSpec2),
    ?assert(is_pid(Child1)),
    ?assert(is_pid(Child2)),
    ?assertNotEqual(Child1, Child2),
    %% Clean up
    flurm_node_sup:stop_node(Child1),
    flurm_node_sup:stop_node(Child2).

test_which_nodes() ->
    %% Start a few nodes
    NodeSpec = #node_spec{
        name = <<"which_node">>,
        hostname = <<"which_node.example.com">>,
        port = 6818,
        cpus = 4,
        memory = 8192,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    },
    {ok, Child} = flurm_node_sup:start_node(NodeSpec),

    Nodes = flurm_node_sup:which_nodes(),
    ?assert(is_list(Nodes)),
    ?assert(lists:member(Child, Nodes)),

    %% Clean up
    flurm_node_sup:stop_node(Child).

test_count_nodes() ->
    InitialCount = flurm_node_sup:count_nodes(),

    NodeSpec = #node_spec{
        name = <<"count_node">>,
        hostname = <<"count_node.example.com">>,
        port = 6818,
        cpus = 4,
        memory = 8192,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    },
    {ok, Child} = flurm_node_sup:start_node(NodeSpec),

    NewCount = flurm_node_sup:count_nodes(),
    ?assertEqual(InitialCount + 1, NewCount),

    %% Clean up
    flurm_node_sup:stop_node(Child).

test_stop_node() ->
    NodeSpec = #node_spec{
        name = <<"stop_node">>,
        hostname = <<"stop_node.example.com">>,
        port = 6818,
        cpus = 4,
        memory = 8192,
        gpus = 0,
        features = [],
        partitions = [<<"default">>]
    },
    {ok, Child} = flurm_node_sup:start_node(NodeSpec),
    ?assert(is_process_alive(Child)),

    ok = flurm_node_sup:stop_node(Child),
    flurm_test_utils:wait_for_death(Child),
    ?assertNot(is_process_alive(Child)).

%%====================================================================
%% Supervisor Spec Validation Tests
%%====================================================================

validate_supervisor_spec_test_() ->
    [
        {"init returns {ok, {SupFlags, ChildSpecs}}", fun() ->
            Result = flurm_node_sup:init([]),
            ?assertMatch({ok, {_, _}}, Result),
            {ok, {SupFlags, ChildSpecs}} = Result,
            ?assert(is_map(SupFlags)),
            ?assert(is_list(ChildSpecs))
        end},
        {"strategy is valid OTP strategy", fun() ->
            {ok, {SupFlags, _}} = flurm_node_sup:init([]),
            Strategy = maps:get(strategy, SupFlags),
            ValidStrategies = [one_for_one, one_for_all, rest_for_one, simple_one_for_one],
            ?assert(lists:member(Strategy, ValidStrategies))
        end},
        {"intensity is non-negative integer", fun() ->
            {ok, {SupFlags, _}} = flurm_node_sup:init([]),
            Intensity = maps:get(intensity, SupFlags),
            ?assert(is_integer(Intensity)),
            ?assert(Intensity >= 0)
        end},
        {"period is positive integer", fun() ->
            {ok, {SupFlags, _}} = flurm_node_sup:init([]),
            Period = maps:get(period, SupFlags),
            ?assert(is_integer(Period)),
            ?assert(Period > 0)
        end},
        {"child spec has valid id", fun() ->
            {ok, {_, [Spec]}} = flurm_node_sup:init([]),
            Id = maps:get(id, Spec),
            ?assert(is_atom(Id))
        end},
        {"child spec has valid MFA", fun() ->
            {ok, {_, [Spec]}} = flurm_node_sup:init([]),
            {M, F, A} = maps:get(start, Spec),
            ?assert(is_atom(M)),
            ?assert(is_atom(F)),
            ?assert(is_list(A))
        end},
        {"child spec restart is valid", fun() ->
            {ok, {_, [Spec]}} = flurm_node_sup:init([]),
            Restart = maps:get(restart, Spec),
            ?assert(lists:member(Restart, [permanent, transient, temporary]))
        end},
        {"child spec shutdown is valid", fun() ->
            {ok, {_, [Spec]}} = flurm_node_sup:init([]),
            Shutdown = maps:get(shutdown, Spec),
            ValidShutdown = (is_integer(Shutdown) andalso Shutdown >= 0)
                            orelse Shutdown =:= brutal_kill
                            orelse Shutdown =:= infinity,
            ?assert(ValidShutdown)
        end},
        {"child spec type is valid", fun() ->
            {ok, {_, [Spec]}} = flurm_node_sup:init([]),
            Type = maps:get(type, Spec),
            ?assert(lists:member(Type, [worker, supervisor]))
        end},
        {"child spec modules is valid", fun() ->
            {ok, {_, [Spec]}} = flurm_node_sup:init([]),
            Modules = maps:get(modules, Spec),
            ValidModules = (Modules =:= dynamic)
                           orelse (is_list(Modules) andalso lists:all(fun is_atom/1, Modules)),
            ?assert(ValidModules)
        end}
    ].
