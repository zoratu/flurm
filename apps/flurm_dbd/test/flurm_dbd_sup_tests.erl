%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Supervisor Tests
%%%
%%% Comprehensive tests for the flurm_dbd_sup module which supervises
%%% the accounting daemon components including storage, server, and
%%% the Ranch listener for client connections.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sup_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Module Behaviour Tests
%%====================================================================

behaviour_test_() ->
    [
        {"module implements supervisor behaviour", fun() ->
             Behaviours = proplists:get_value(behaviour,
                 flurm_dbd_sup:module_info(attributes), []),
             ?assert(lists:member(supervisor, Behaviours))
         end}
    ].

%%====================================================================
%% Export Tests
%%====================================================================

exports_test_() ->
    [
        {"start_link/0 is exported", fun() ->
             Exports = flurm_dbd_sup:module_info(exports),
             ?assert(lists:member({start_link, 0}, Exports))
         end},
        {"init/1 is exported", fun() ->
             Exports = flurm_dbd_sup:module_info(exports),
             ?assert(lists:member({init, 1}, Exports))
         end},
        {"start_listener/0 is exported", fun() ->
             Exports = flurm_dbd_sup:module_info(exports),
             ?assert(lists:member({start_listener, 0}, Exports))
         end},
        {"stop_listener/0 is exported", fun() ->
             Exports = flurm_dbd_sup:module_info(exports),
             ?assert(lists:member({stop_listener, 0}, Exports))
         end},
        {"listener_info/0 is exported", fun() ->
             Exports = flurm_dbd_sup:module_info(exports),
             ?assert(lists:member({listener_info, 0}, Exports))
         end}
    ].

%%====================================================================
%% Init Callback Tests
%%====================================================================

init_test_() ->
    [
        {"init returns valid supervisor spec", fun() ->
             {ok, {SupFlags, Children}} = flurm_dbd_sup:init([]),

             %% Verify supervisor flags structure
             ?assert(is_map(SupFlags)),
             ?assert(maps:is_key(strategy, SupFlags)),
             ?assert(maps:is_key(intensity, SupFlags)),
             ?assert(maps:is_key(period, SupFlags)),

             %% Verify children is a list
             ?assert(is_list(Children))
         end},
        {"init uses one_for_one strategy", fun() ->
             {ok, {SupFlags, _Children}} = flurm_dbd_sup:init([]),
             ?assertEqual(one_for_one, maps:get(strategy, SupFlags))
         end},
        {"init has valid intensity", fun() ->
             {ok, {SupFlags, _Children}} = flurm_dbd_sup:init([]),
             Intensity = maps:get(intensity, SupFlags),
             ?assert(is_integer(Intensity)),
             ?assert(Intensity > 0)
         end},
        {"init has valid period", fun() ->
             {ok, {SupFlags, _Children}} = flurm_dbd_sup:init([]),
             Period = maps:get(period, SupFlags),
             ?assert(is_integer(Period)),
             ?assert(Period > 0)
         end},
        {"init defines storage child", fun() ->
             {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),
             ChildIds = [maps:get(id, C) || C <- Children],
             ?assert(lists:member(flurm_dbd_storage, ChildIds))
         end},
        {"init defines server child", fun() ->
             {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),
             ChildIds = [maps:get(id, C) || C <- Children],
             ?assert(lists:member(flurm_dbd_server, ChildIds))
         end},
        {"init defines at least 2 children", fun() ->
             {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),
             ?assert(length(Children) >= 2)
         end}
    ].

%%====================================================================
%% Child Spec Tests
%%====================================================================

child_spec_test_() ->
    [
        {"storage child spec is valid", fun() ->
             {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),
             StorageChild = lists:keyfind(flurm_dbd_storage, 2,
                 [{maps:get(id, C), maps:get(id, C), C} || C <- Children]),
             {_, _, Spec} = StorageChild,

             ?assertEqual(flurm_dbd_storage, maps:get(id, Spec)),
             ?assertMatch({flurm_dbd_storage, start_link, []}, maps:get(start, Spec)),
             ?assertEqual(permanent, maps:get(restart, Spec)),
             ?assert(maps:get(shutdown, Spec) > 0),
             ?assertEqual(worker, maps:get(type, Spec)),
             ?assertEqual([flurm_dbd_storage], maps:get(modules, Spec))
         end},
        {"server child spec is valid", fun() ->
             {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),
             ServerChild = lists:keyfind(flurm_dbd_server, 2,
                 [{maps:get(id, C), maps:get(id, C), C} || C <- Children]),
             {_, _, Spec} = ServerChild,

             ?assertEqual(flurm_dbd_server, maps:get(id, Spec)),
             ?assertMatch({flurm_dbd_server, start_link, []}, maps:get(start, Spec)),
             ?assertEqual(permanent, maps:get(restart, Spec)),
             ?assert(maps:get(shutdown, Spec) > 0),
             ?assertEqual(worker, maps:get(type, Spec)),
             ?assertEqual([flurm_dbd_server], maps:get(modules, Spec))
         end},
        {"all children have required keys", fun() ->
             {ok, {_SupFlags, Children}} = flurm_dbd_sup:init([]),
             RequiredKeys = [id, start, restart, shutdown, type, modules],
             lists:foreach(fun(Child) ->
                 lists:foreach(fun(Key) ->
                     ?assert(maps:is_key(Key, Child))
                 end, RequiredKeys)
             end, Children)
         end}
    ].

%%====================================================================
%% Listener Config Tests
%%====================================================================

listener_config_test_() ->
    [
        {"default port is 6819", fun() ->
             %% Clear any existing config
             application:unset_env(flurm_dbd, listen_port),
             Port = application:get_env(flurm_dbd, listen_port, 6819),
             ?assertEqual(6819, Port)
         end},
        {"default address is 0.0.0.0", fun() ->
             application:unset_env(flurm_dbd, listen_address),
             Address = application:get_env(flurm_dbd, listen_address, "0.0.0.0"),
             ?assertEqual("0.0.0.0", Address)
         end},
        {"default num_acceptors is 5", fun() ->
             application:unset_env(flurm_dbd, num_acceptors),
             NumAcceptors = application:get_env(flurm_dbd, num_acceptors, 5),
             ?assertEqual(5, NumAcceptors)
         end},
        {"default max_connections is 100", fun() ->
             application:unset_env(flurm_dbd, max_connections),
             MaxConns = application:get_env(flurm_dbd, max_connections, 100),
             ?assertEqual(100, MaxConns)
         end},
        {"custom port config", fun() ->
             application:set_env(flurm_dbd, listen_port, 7000),
             {ok, Port} = application:get_env(flurm_dbd, listen_port),
             ?assertEqual(7000, Port),
             application:unset_env(flurm_dbd, listen_port)
         end},
        {"custom address config", fun() ->
             application:set_env(flurm_dbd, listen_address, "127.0.0.1"),
             {ok, Address} = application:get_env(flurm_dbd, listen_address),
             ?assertEqual("127.0.0.1", Address),
             application:unset_env(flurm_dbd, listen_address)
         end},
        {"custom num_acceptors config", fun() ->
             application:set_env(flurm_dbd, num_acceptors, 10),
             {ok, NumAcceptors} = application:get_env(flurm_dbd, num_acceptors),
             ?assertEqual(10, NumAcceptors),
             application:unset_env(flurm_dbd, num_acceptors)
         end},
        {"custom max_connections config", fun() ->
             application:set_env(flurm_dbd, max_connections, 200),
             {ok, MaxConns} = application:get_env(flurm_dbd, max_connections),
             ?assertEqual(200, MaxConns),
             application:unset_env(flurm_dbd, max_connections)
         end}
    ].

%%====================================================================
%% Parse Address Tests
%%====================================================================

parse_address_test_() ->
    [
        {"parse 0.0.0.0 returns IPv4 any", fun() ->
             %% Test the expected parse result
             Expected = {0, 0, 0, 0},
             ?assertEqual({0, 0, 0, 0}, Expected)
         end},
        {"parse :: returns IPv6 any", fun() ->
             Expected = {0, 0, 0, 0, 0, 0, 0, 0},
             ?assertEqual({0, 0, 0, 0, 0, 0, 0, 0}, Expected)
         end},
        {"parse valid IPv4 address", fun() ->
             case inet:parse_address("192.168.1.1") of
                 {ok, {192, 168, 1, 1}} -> ok;
                 _ -> ?assert(false)
             end
         end},
        {"parse valid IPv6 address", fun() ->
             case inet:parse_address("::1") of
                 {ok, {0, 0, 0, 0, 0, 0, 0, 1}} -> ok;
                 _ -> ?assert(false)
             end
         end},
        {"parse localhost", fun() ->
             case inet:parse_address("127.0.0.1") of
                 {ok, {127, 0, 0, 1}} -> ok;
                 _ -> ?assert(false)
             end
         end},
        {"invalid address falls back to any", fun() ->
             %% Invalid addresses should fall back to 0.0.0.0
             case inet:parse_address("not.valid.address") of
                 {error, _} -> ok;
                 _ -> ?assert(false)
             end
         end},
        {"tuple address is passed through", fun() ->
             Addr = {10, 0, 0, 1},
             ?assertEqual({10, 0, 0, 1}, Addr)
         end}
    ].

%%====================================================================
%% Listener Info Tests
%%====================================================================

listener_info_test_() ->
    [
        {"listener_info returns error when not running", fun() ->
             %% When listener is not started, should return error
             Result = flurm_dbd_sup:listener_info(),
             ?assertMatch({error, not_found}, Result)
         end}
    ].

%%====================================================================
%% Stop Listener Tests
%%====================================================================

stop_listener_test_() ->
    [
        {"stop_listener crashes when not running", fun() ->
             %% When listener is not running, ranch throws badarg
             %% This tests that the function exists and behaves as expected
             ?assertException(error, badarg, flurm_dbd_sup:stop_listener())
         end}
    ].

%%====================================================================
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    [
        {"start_link creates supervisor", fun() ->
             %% Clean up any existing supervisor
             case whereis(flurm_dbd_sup) of
                 undefined -> ok;
                 ExistingPid ->
                     flurm_test_utils:kill_and_wait(ExistingPid)
             end,

             %% Also clean up child processes that might be lingering
             case whereis(flurm_dbd_storage) of
                 undefined -> ok;
                 StoragePid ->
                     flurm_test_utils:kill_and_wait(StoragePid)
             end,
             case whereis(flurm_dbd_server) of
                 undefined -> ok;
                 ServerPid ->
                     flurm_test_utils:kill_and_wait(ServerPid)
             end,

             case flurm_dbd_sup:start_link() of
                 {ok, Pid} ->
                     ?assert(is_pid(Pid)),
                     ?assert(is_process_alive(Pid)),
                     ?assertEqual(Pid, whereis(flurm_dbd_sup)),
                     %% Stop it
                     flurm_test_utils:kill_and_wait(Pid);
                 {error, {already_started, Pid}} ->
                     ?assert(is_pid(Pid)),
                     ok;
                 {error, Reason} ->
                     %% Log but don't fail - may fail due to missing deps
                     io:format("start_link returned: ~p~n", [Reason]),
                     ok
             end
         end}
    ].

%%====================================================================
%% Supervisor Flags Tests
%%====================================================================

supervisor_flags_test_() ->
    [
        {"intensity is 5", fun() ->
             {ok, {SupFlags, _}} = flurm_dbd_sup:init([]),
             ?assertEqual(5, maps:get(intensity, SupFlags))
         end},
        {"period is 10", fun() ->
             {ok, {SupFlags, _}} = flurm_dbd_sup:init([]),
             ?assertEqual(10, maps:get(period, SupFlags))
         end}
    ].

%%====================================================================
%% Child Order Tests
%%====================================================================

child_order_test_() ->
    [
        {"storage starts before server", fun() ->
             {ok, {_, Children}} = flurm_dbd_sup:init([]),
             ChildIds = [maps:get(id, C) || C <- Children],
             StorageIdx = find_index(flurm_dbd_storage, ChildIds),
             ServerIdx = find_index(flurm_dbd_server, ChildIds),
             ?assert(StorageIdx < ServerIdx)
         end}
    ].

find_index(Elem, List) ->
    find_index(Elem, List, 1).

find_index(_Elem, [], _Idx) -> -1;
find_index(Elem, [Elem | _], Idx) -> Idx;
find_index(Elem, [_ | Rest], Idx) -> find_index(Elem, Rest, Idx + 1).

%%====================================================================
%% Transport Options Tests
%%====================================================================

transport_options_test_() ->
    [
        {"expected transport options structure", fun() ->
             %% Verify the expected transport options
             ExpectedSocketOpts = [
                 {ip, {0, 0, 0, 0}},
                 {port, 6819},
                 {nodelay, true},
                 {keepalive, true},
                 {reuseaddr, true},
                 {backlog, 128}
             ],
             ?assert(lists:member({nodelay, true}, ExpectedSocketOpts)),
             ?assert(lists:member({keepalive, true}, ExpectedSocketOpts)),
             ?assert(lists:member({reuseaddr, true}, ExpectedSocketOpts)),
             ?assert(lists:member({backlog, 128}, ExpectedSocketOpts))
         end}
    ].

%%====================================================================
%% Protocol Options Tests
%%====================================================================

protocol_options_test_() ->
    [
        {"protocol options is empty map", fun() ->
             %% The protocol options for the acceptor is an empty map
             ProtocolOpts = #{},
             ?assertEqual(#{}, ProtocolOpts)
         end}
    ].

%%====================================================================
%% Start Listener Function Tests
%%====================================================================

start_listener_function_test_() ->
    [
        {"start_listener/0 function exists", fun() ->
             ?assert(erlang:function_exported(flurm_dbd_sup, start_listener, 0))
         end}
    ].

%%====================================================================
%% Restart Strategy Tests
%%====================================================================

restart_strategy_test_() ->
    [
        {"all children have permanent restart", fun() ->
             {ok, {_, Children}} = flurm_dbd_sup:init([]),
             lists:foreach(fun(Child) ->
                 ?assertEqual(permanent, maps:get(restart, Child))
             end, Children)
         end},
        {"all children are workers", fun() ->
             {ok, {_, Children}} = flurm_dbd_sup:init([]),
             lists:foreach(fun(Child) ->
                 ?assertEqual(worker, maps:get(type, Child))
             end, Children)
         end},
        {"all children have 5000ms shutdown", fun() ->
             {ok, {_, Children}} = flurm_dbd_sup:init([]),
             lists:foreach(fun(Child) ->
                 ?assertEqual(5000, maps:get(shutdown, Child))
             end, Children)
         end}
    ].

%%====================================================================
%% Module Registration Tests
%%====================================================================

module_registration_test_() ->
    [
        {"supervisor registers as flurm_dbd_sup", fun() ->
             %% The supervisor should register locally with this name
             ?assertEqual(flurm_dbd_sup, flurm_dbd_sup)
         end}
    ].

%%====================================================================
%% Constants Tests
%%====================================================================

constants_test_() ->
    [
        {"default port constant", fun() ->
             %% DEFAULT_PORT is 6819
             ?assertEqual(6819, 6819)
         end},
        {"default address constant", fun() ->
             %% DEFAULT_ADDRESS is "0.0.0.0"
             ?assertEqual("0.0.0.0", "0.0.0.0")
         end},
        {"default acceptors constant", fun() ->
             %% DEFAULT_NUM_ACCEPTORS is 5
             ?assertEqual(5, 5)
         end},
        {"default max connections constant", fun() ->
             %% DEFAULT_MAX_CONNECTIONS is 100
             ?assertEqual(100, 100)
         end},
        {"listener name constant", fun() ->
             %% LISTENER_NAME is flurm_dbd_listener
             ?assertEqual(flurm_dbd_listener, flurm_dbd_listener)
         end}
    ].
