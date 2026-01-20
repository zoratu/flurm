%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_controller_sup module
%%%
%%% Tests the supervisor init/1, listener management, and configuration
%%% parsing functions.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_sup_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

sup_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"init/1 returns valid supervisor config", fun test_init/0},
         {"start_link/0 starts supervisor", fun test_start_link/0},
         {"listener functions work correctly", fun test_listener_functions/0},
         {"node_listener functions work correctly", fun test_node_listener_functions/0}
     ]}.

setup() ->
    %% Ensure required applications are started
    application:ensure_all_started(lager),
    application:ensure_all_started(ranch),

    %% Disable cluster mode for tests
    application:set_env(flurm_controller, enable_cluster, false),
    application:unset_env(flurm_controller, cluster_nodes),

    %% Set test configuration
    TestPort = 28817 + (erlang:unique_integer([positive]) rem 1000),
    NodePort = 28818 + (erlang:unique_integer([positive]) rem 1000),

    application:set_env(flurm_controller, listen_port, TestPort),
    application:set_env(flurm_controller, node_listen_port, NodePort),
    application:set_env(flurm_controller, listen_address, "127.0.0.1"),
    application:set_env(flurm_controller, num_acceptors, 2),
    application:set_env(flurm_controller, max_connections, 50),
    application:set_env(flurm_controller, max_node_connections, 25),

    #{test_port => TestPort, node_port => NodePort}.

cleanup(_) ->
    %% Stop supervisor if running
    case whereis(flurm_controller_sup) of
        undefined -> ok;
        Pid ->
            %% Unlink first to prevent test process from being killed
            catch unlink(Pid),
            %% Stop listeners first
            catch flurm_controller_sup:stop_listener(),
            catch flurm_controller_sup:stop_node_listener(),
            Ref = monitor(process, Pid),
            catch exit(Pid, shutdown),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 2000 ->
                demonitor(Ref, [flush])
            end
    end,
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_init() ->
    %% Call init/1 directly to test supervisor config
    {ok, {SupFlags, Children}} = flurm_controller_sup:init([]),

    %% Verify supervisor flags
    ?assertMatch(#{strategy := one_for_one}, SupFlags),
    ?assertMatch(#{intensity := _, period := _}, SupFlags),

    %% Verify we have children
    ?assert(is_list(Children)),
    ?assert(length(Children) > 0),

    %% Verify each child spec is valid
    lists:foreach(fun(ChildSpec) ->
        ?assert(maps:is_key(id, ChildSpec)),
        ?assert(maps:is_key(start, ChildSpec)),
        ?assert(maps:is_key(restart, ChildSpec)),
        ?assert(maps:is_key(type, ChildSpec))
    end, Children),

    ok.

test_start_link() ->
    %% Check if supervisor already running
    case whereis(flurm_controller_sup) of
        undefined ->
            %% Start the supervisor
            {ok, Pid} = flurm_controller_sup:start_link(),
            ?assert(is_pid(Pid)),
            ?assertEqual(Pid, whereis(flurm_controller_sup));
        Pid ->
            %% Already running, just verify
            ?assert(is_pid(Pid)),
            ?assertEqual(Pid, whereis(flurm_controller_sup))
    end,
    ok.

test_listener_functions() ->
    %% Ensure supervisor is running
    ensure_supervisor_running(),

    %% Stop any existing listener
    catch flurm_controller_sup:stop_listener(),
    flurm_test_utils:wait_for_unregistered(flurm_controller_listener),

    %% Start listener
    {ok, ListenerPid} = flurm_controller_sup:start_listener(),
    ?assert(is_pid(ListenerPid)),

    %% Get listener info
    Info = flurm_controller_sup:listener_info(),
    ?assert(is_map(Info)),
    ?assert(maps:is_key(port, Info)),
    ?assertEqual(running, maps:get(status, Info)),

    %% Stop listener
    ok = flurm_controller_sup:stop_listener(),
    flurm_test_utils:wait_for_unregistered(flurm_controller_listener),

    %% Info should return error now
    ?assertEqual({error, not_found}, flurm_controller_sup:listener_info()),

    ok.

test_node_listener_functions() ->
    %% Ensure supervisor is running
    ensure_supervisor_running(),

    %% Stop any existing node listener
    catch flurm_controller_sup:stop_node_listener(),
    flurm_test_utils:wait_for_unregistered(flurm_node_listener),

    %% Start node listener
    {ok, ListenerPid} = flurm_controller_sup:start_node_listener(),
    ?assert(is_pid(ListenerPid)),

    %% Get node listener info
    Info = flurm_controller_sup:node_listener_info(),
    ?assert(is_map(Info)),
    ?assert(maps:is_key(port, Info)),
    ?assertEqual(running, maps:get(status, Info)),

    %% Stop node listener
    ok = flurm_controller_sup:stop_node_listener(),
    flurm_test_utils:wait_for_unregistered(flurm_node_listener),

    %% Info should return error now
    ?assertEqual({error, not_found}, flurm_controller_sup:node_listener_info()),

    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

ensure_supervisor_running() ->
    case whereis(flurm_controller_sup) of
        undefined ->
            {ok, Pid} = flurm_controller_sup:start_link(),
            unlink(Pid),
            ok;
        _Pid ->
            ok
    end.

%%====================================================================
%% Additional Unit Tests
%%====================================================================

%% Test address parsing
parse_address_test_() ->
    [
        {"Parse 0.0.0.0", fun() ->
            ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("0.0.0.0"))
        end},
        {"Parse :: (IPv6)", fun() ->
            ?assertEqual({0, 0, 0, 0, 0, 0, 0, 0}, flurm_controller_sup:parse_address("::"))
        end},
        {"Parse IPv4 address", fun() ->
            ?assertEqual({127, 0, 0, 1}, flurm_controller_sup:parse_address("127.0.0.1"))
        end},
        {"Parse tuple passthrough", fun() ->
            ?assertEqual({192, 168, 1, 1}, flurm_controller_sup:parse_address({192, 168, 1, 1}))
        end},
        {"Invalid address falls back to 0.0.0.0", fun() ->
            ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("invalid"))
        end}
    ].

%% Test cluster enabled detection
cluster_enabled_test_() ->
    {setup,
     fun() ->
         application:set_env(flurm_controller, enable_cluster, false)
     end,
     fun(_) ->
         application:unset_env(flurm_controller, enable_cluster)
     end,
     fun(_) ->
         {"Cluster disabled returns false in init children", fun() ->
             {ok, {_, Children}} = flurm_controller_sup:init([]),
             %% When cluster disabled, should not have cluster children
             ClusterChild = lists:filter(fun(C) ->
                 maps:get(id, C) =:= flurm_controller_cluster
             end, Children),
             ?assertEqual([], ClusterChild)
         end}
     end}.

%%====================================================================
%% Tests for Internal Helper Functions (exported via -ifdef(TEST))
%%====================================================================

helper_functions_test_() ->
    {setup,
     fun() ->
         %% Set up test configuration
         application:set_env(flurm_controller, listen_port, 16817),
         application:set_env(flurm_controller, listen_address, "192.168.1.100"),
         application:set_env(flurm_controller, num_acceptors, 8),
         application:set_env(flurm_controller, max_connections, 500),
         application:set_env(flurm_controller, node_listen_port, 16818),
         application:set_env(flurm_controller, max_node_connections, 250)
     end,
     fun(_) ->
         %% Clean up
         application:unset_env(flurm_controller, listen_port),
         application:unset_env(flurm_controller, listen_address),
         application:unset_env(flurm_controller, num_acceptors),
         application:unset_env(flurm_controller, max_connections),
         application:unset_env(flurm_controller, node_listen_port),
         application:unset_env(flurm_controller, max_node_connections)
     end,
     [
         {"get_listener_config/0 returns configured values", fun test_get_listener_config/0},
         {"get_node_listener_config/0 returns configured values", fun test_get_node_listener_config/0},
         {"is_cluster_enabled/0 returns false when disabled", fun test_is_cluster_disabled/0}
     ]}.

test_get_listener_config() ->
    {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_listener_config(),

    ?assertEqual(16817, Port),
    ?assertEqual("192.168.1.100", Address),
    ?assertEqual(8, NumAcceptors),
    ?assertEqual(500, MaxConns),
    ok.

test_get_node_listener_config() ->
    {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_node_listener_config(),

    ?assertEqual(16818, Port),
    ?assertEqual("192.168.1.100", Address),  %% Same as listen_address
    ?assertEqual(8, NumAcceptors),           %% Same as num_acceptors
    ?assertEqual(250, MaxConns),
    ok.

test_is_cluster_disabled() ->
    %% Test with cluster explicitly disabled
    application:set_env(flurm_controller, enable_cluster, false),
    ?assertEqual(false, flurm_controller_sup:is_cluster_enabled()),

    %% Clean up
    application:unset_env(flurm_controller, enable_cluster),
    ok.

%% Test get_listener_config with defaults
listener_config_defaults_test_() ->
    {setup,
     fun() ->
         %% Clear all config to test defaults
         application:unset_env(flurm_controller, listen_port),
         application:unset_env(flurm_controller, listen_address),
         application:unset_env(flurm_controller, num_acceptors),
         application:unset_env(flurm_controller, max_connections)
     end,
     fun(_) -> ok end,
     [
         {"get_listener_config/0 uses defaults", fun() ->
             {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_listener_config(),
             %% Check defaults: 6817, "0.0.0.0", 10, 1000
             ?assertEqual(6817, Port),
             ?assertEqual("0.0.0.0", Address),
             ?assertEqual(10, NumAcceptors),
             ?assertEqual(1000, MaxConns)
         end}
     ]}.

%% Test get_node_listener_config with defaults
node_listener_config_defaults_test_() ->
    {setup,
     fun() ->
         %% Clear all config to test defaults
         application:unset_env(flurm_controller, node_listen_port),
         application:unset_env(flurm_controller, listen_address),
         application:unset_env(flurm_controller, num_acceptors),
         application:unset_env(flurm_controller, max_node_connections)
     end,
     fun(_) -> ok end,
     [
         {"get_node_listener_config/0 uses defaults", fun() ->
             {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_node_listener_config(),
             %% Check defaults: 6818, "0.0.0.0", 10, 500
             ?assertEqual(6818, Port),
             ?assertEqual("0.0.0.0", Address),
             ?assertEqual(10, NumAcceptors),
             ?assertEqual(500, MaxConns)
         end}
     ]}.

%% Test is_cluster_enabled with different configurations
cluster_enabled_detailed_test_() ->
    [
        {"is_cluster_enabled with enable_cluster=true and distributed node",
         {setup,
          fun() ->
              application:set_env(flurm_controller, enable_cluster, true)
          end,
          fun(_) ->
              application:unset_env(flurm_controller, enable_cluster)
          end,
          fun(_) ->
              [{"check cluster enabled result", fun() ->
                  %% On nonode@nohost, should return false even if enabled
                  Result = flurm_controller_sup:is_cluster_enabled(),
                  case node() of
                      'nonode@nohost' ->
                          ?assertEqual(false, Result);
                      _ ->
                          ?assertEqual(true, Result)
                  end
              end}]
          end}},

        {"is_cluster_enabled with enable_cluster=false",
         {setup,
          fun() ->
              application:set_env(flurm_controller, enable_cluster, false)
          end,
          fun(_) ->
              application:unset_env(flurm_controller, enable_cluster)
          end,
          fun(_) ->
              [{"returns false when disabled", fun() ->
                  ?assertEqual(false, flurm_controller_sup:is_cluster_enabled())
              end}]
          end}}
    ].

%%====================================================================
%% Comprehensive Supervisor Spec Validation Tests
%%====================================================================

%% These tests thoroughly validate the supervisor init/1 return value
%% according to OTP supervisor specification requirements.

init_spec_validation_test_() ->
    {setup,
     fun() ->
         application:set_env(flurm_controller, enable_cluster, false)
     end,
     fun(_) ->
         application:unset_env(flurm_controller, enable_cluster)
     end,
     [
         {"init/1 returns valid {ok, {SupFlags, ChildSpecs}} tuple",
          fun test_init_returns_valid_tuple/0},
         {"SupFlags contains valid strategy",
          fun test_sup_flags_strategy/0},
         {"SupFlags contains valid intensity",
          fun test_sup_flags_intensity/0},
         {"SupFlags contains valid period",
          fun test_sup_flags_period/0},
         {"All child specs have valid id",
          fun test_child_specs_id/0},
         {"All child specs have valid start MFA",
          fun test_child_specs_start/0},
         {"All child specs have valid restart",
          fun test_child_specs_restart/0},
         {"All child specs have valid shutdown",
          fun test_child_specs_shutdown/0},
         {"All child specs have valid type",
          fun test_child_specs_type/0},
         {"All child specs have valid modules",
          fun test_child_specs_modules/0}
     ]}.

test_init_returns_valid_tuple() ->
    Result = flurm_controller_sup:init([]),
    ?assertMatch({ok, {_, _}}, Result),
    {ok, {SupFlags, ChildSpecs}} = Result,
    ?assert(is_map(SupFlags)),
    ?assert(is_list(ChildSpecs)),
    ok.

test_sup_flags_strategy() ->
    {ok, {SupFlags, _}} = flurm_controller_sup:init([]),
    Strategy = maps:get(strategy, SupFlags),
    ValidStrategies = [one_for_one, one_for_all, rest_for_one, simple_one_for_one],
    ?assert(lists:member(Strategy, ValidStrategies),
            io_lib:format("Invalid strategy: ~p", [Strategy])),
    ok.

test_sup_flags_intensity() ->
    {ok, {SupFlags, _}} = flurm_controller_sup:init([]),
    Intensity = maps:get(intensity, SupFlags),
    ?assert(is_integer(Intensity)),
    ?assert(Intensity >= 0),
    ok.

test_sup_flags_period() ->
    {ok, {SupFlags, _}} = flurm_controller_sup:init([]),
    Period = maps:get(period, SupFlags),
    ?assert(is_integer(Period)),
    ?assert(Period > 0),
    ok.

test_child_specs_id() ->
    {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
    lists:foreach(fun(Spec) ->
        Id = maps:get(id, Spec),
        ?assert(is_atom(Id) orelse is_binary(Id) orelse is_list(Id),
                io_lib:format("Invalid id: ~p", [Id]))
    end, ChildSpecs),
    ok.

test_child_specs_start() ->
    {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
    lists:foreach(fun(Spec) ->
        Start = maps:get(start, Spec),
        ?assertMatch({M, F, A} when is_atom(M) andalso is_atom(F) andalso is_list(A), Start,
                     io_lib:format("Invalid start MFA: ~p", [Start]))
    end, ChildSpecs),
    ok.

test_child_specs_restart() ->
    {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
    ValidRestart = [permanent, transient, temporary],
    lists:foreach(fun(Spec) ->
        Restart = maps:get(restart, Spec),
        ?assert(lists:member(Restart, ValidRestart),
                io_lib:format("Invalid restart: ~p", [Restart]))
    end, ChildSpecs),
    ok.

test_child_specs_shutdown() ->
    {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
    lists:foreach(fun(Spec) ->
        Shutdown = maps:get(shutdown, Spec),
        ValidShutdown = is_integer(Shutdown) andalso Shutdown >= 0
                        orelse Shutdown =:= brutal_kill
                        orelse Shutdown =:= infinity,
        ?assert(ValidShutdown,
                io_lib:format("Invalid shutdown: ~p", [Shutdown]))
    end, ChildSpecs),
    ok.

test_child_specs_type() ->
    {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
    ValidTypes = [worker, supervisor],
    lists:foreach(fun(Spec) ->
        Type = maps:get(type, Spec),
        ?assert(lists:member(Type, ValidTypes),
                io_lib:format("Invalid type: ~p", [Type]))
    end, ChildSpecs),
    ok.

test_child_specs_modules() ->
    {ok, {_, ChildSpecs}} = flurm_controller_sup:init([]),
    lists:foreach(fun(Spec) ->
        Modules = maps:get(modules, Spec),
        ValidModules = (Modules =:= dynamic)
                       orelse (is_list(Modules) andalso
                               lists:all(fun is_atom/1, Modules)),
        ?assert(ValidModules,
                io_lib:format("Invalid modules: ~p", [Modules]))
    end, ChildSpecs),
    ok.

%%====================================================================
%% Validate All Child Specs Helper
%%====================================================================

validate_child_spec_test_() ->
    {setup,
     fun() ->
         application:set_env(flurm_controller, enable_cluster, false)
     end,
     fun(_) ->
         application:unset_env(flurm_controller, enable_cluster)
     end,
     [
         {"Each child spec passes comprehensive validation",
          fun test_all_child_specs_valid/0}
     ]}.

test_all_child_specs_valid() ->
    {ok, {SupFlags, ChildSpecs}} = flurm_controller_sup:init([]),

    %% Validate SupFlags
    ?assertMatch(#{strategy := _, intensity := _, period := _}, SupFlags),

    %% Validate each child spec
    ?assert(is_list(ChildSpecs)),
    ?assert(length(ChildSpecs) > 0),

    lists:foreach(fun(Spec) ->
        validate_child_spec(Spec)
    end, ChildSpecs),
    ok.

validate_child_spec(#{id := Id, start := {M, F, A}, restart := Restart,
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
validate_child_spec({Id, {M, F, A}, Restart, Shutdown, Type, Modules}) ->
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
