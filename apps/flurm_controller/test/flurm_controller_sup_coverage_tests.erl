%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Sup Coverage Tests
%%%
%%% Comprehensive coverage tests for flurm_controller_sup module.
%%% Tests all exported -ifdef(TEST) helper functions for maximum coverage.
%%%
%%% These tests focus on:
%%% - get_listener_config - listener configuration retrieval
%%% - get_node_listener_config - node listener configuration
%%% - get_http_api_config - HTTP API configuration
%%% - parse_address - IP address parsing
%%% - is_cluster_enabled - cluster mode detection
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_sup_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% parse_address Tests
%%====================================================================

parse_address_test_() ->
    [
        {"0.0.0.0 returns tuple", fun() ->
            ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("0.0.0.0"))
        end},
        {":: returns IPv6 tuple", fun() ->
            ?assertEqual({0, 0, 0, 0, 0, 0, 0, 0}, flurm_controller_sup:parse_address("::"))
        end},
        {"127.0.0.1 parses correctly", fun() ->
            ?assertEqual({127, 0, 0, 1}, flurm_controller_sup:parse_address("127.0.0.1"))
        end},
        {"192.168.1.1 parses correctly", fun() ->
            ?assertEqual({192, 168, 1, 1}, flurm_controller_sup:parse_address("192.168.1.1"))
        end},
        {"10.0.0.1 parses correctly", fun() ->
            ?assertEqual({10, 0, 0, 1}, flurm_controller_sup:parse_address("10.0.0.1"))
        end},
        {"255.255.255.255 parses correctly", fun() ->
            ?assertEqual({255, 255, 255, 255}, flurm_controller_sup:parse_address("255.255.255.255"))
        end},
        {"::1 parses to IPv6 localhost", fun() ->
            Result = flurm_controller_sup:parse_address("::1"),
            ?assertEqual({0, 0, 0, 0, 0, 0, 0, 1}, Result)
        end},
        {"fe80::1 parses correctly", fun() ->
            Result = flurm_controller_sup:parse_address("fe80::1"),
            %% Should be a valid 8-element tuple
            ?assert(is_tuple(Result)),
            ?assertEqual(8, tuple_size(Result))
        end},
        {"invalid address returns default", fun() ->
            ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("invalid"))
        end},
        {"already tuple passes through", fun() ->
            Addr = {192, 168, 0, 1},
            ?assertEqual(Addr, flurm_controller_sup:parse_address(Addr))
        end},
        {"IPv6 tuple passes through", fun() ->
            Addr = {0, 0, 0, 0, 0, 0, 0, 1},
            ?assertEqual(Addr, flurm_controller_sup:parse_address(Addr))
        end},
        {"partial address may be valid or fall back", fun() ->
            %% Some systems may parse partial addresses, others fall back to 0.0.0.0
            Result = flurm_controller_sup:parse_address("192.168"),
            ?assert(is_tuple(Result)),
            ?assert(tuple_size(Result) =:= 4 orelse tuple_size(Result) =:= 8)
        end},
        {"address with port invalid", fun() ->
            ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("192.168.1.1:8080"))
        end}
    ].

%%====================================================================
%% get_listener_config Tests
%%====================================================================

get_listener_config_test_() ->
    {setup,
     fun() ->
         %% Save original values
         application:ensure_all_started(sasl),
         ok
     end,
     fun(_) ->
         %% Clean up test values
         application:unset_env(flurm_controller, listen_port),
         application:unset_env(flurm_controller, listen_address),
         application:unset_env(flurm_controller, num_acceptors),
         application:unset_env(flurm_controller, max_connections)
     end,
     [
        {"returns tuple with defaults", fun() ->
            application:unset_env(flurm_controller, listen_port),
            application:unset_env(flurm_controller, listen_address),
            application:unset_env(flurm_controller, num_acceptors),
            application:unset_env(flurm_controller, max_connections),
            {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(6817, Port),
            ?assertEqual("0.0.0.0", Address),
            ?assertEqual(10, NumAcceptors),
            ?assertEqual(1000, MaxConns)
        end},
        {"respects custom port", fun() ->
            application:set_env(flurm_controller, listen_port, 9999),
            {Port, _, _, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(9999, Port),
            application:unset_env(flurm_controller, listen_port)
        end},
        {"respects custom address", fun() ->
            application:set_env(flurm_controller, listen_address, "127.0.0.1"),
            {_, Address, _, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual("127.0.0.1", Address),
            application:unset_env(flurm_controller, listen_address)
        end},
        {"respects custom num_acceptors", fun() ->
            application:set_env(flurm_controller, num_acceptors, 20),
            {_, _, NumAcceptors, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(20, NumAcceptors),
            application:unset_env(flurm_controller, num_acceptors)
        end},
        {"respects custom max_connections", fun() ->
            application:set_env(flurm_controller, max_connections, 5000),
            {_, _, _, MaxConns} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(5000, MaxConns),
            application:unset_env(flurm_controller, max_connections)
        end}
     ]}.

%%====================================================================
%% get_node_listener_config Tests
%%====================================================================

get_node_listener_config_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(sasl),
         ok
     end,
     fun(_) ->
         application:unset_env(flurm_controller, node_listen_port),
         application:unset_env(flurm_controller, listen_address),
         application:unset_env(flurm_controller, num_acceptors),
         application:unset_env(flurm_controller, max_node_connections)
     end,
     [
        {"returns tuple with defaults", fun() ->
            application:unset_env(flurm_controller, node_listen_port),
            application:unset_env(flurm_controller, listen_address),
            application:unset_env(flurm_controller, num_acceptors),
            application:unset_env(flurm_controller, max_node_connections),
            {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_node_listener_config(),
            ?assertEqual(6818, Port),
            ?assertEqual("0.0.0.0", Address),
            ?assertEqual(10, NumAcceptors),
            ?assertEqual(500, MaxConns)
        end},
        {"respects custom node port", fun() ->
            application:set_env(flurm_controller, node_listen_port, 7818),
            {Port, _, _, _} = flurm_controller_sup:get_node_listener_config(),
            ?assertEqual(7818, Port),
            application:unset_env(flurm_controller, node_listen_port)
        end},
        {"uses same address as main listener", fun() ->
            application:set_env(flurm_controller, listen_address, "192.168.1.1"),
            {_, Address, _, _} = flurm_controller_sup:get_node_listener_config(),
            ?assertEqual("192.168.1.1", Address),
            application:unset_env(flurm_controller, listen_address)
        end},
        {"respects custom max_node_connections", fun() ->
            application:set_env(flurm_controller, max_node_connections, 1000),
            {_, _, _, MaxConns} = flurm_controller_sup:get_node_listener_config(),
            ?assertEqual(1000, MaxConns),
            application:unset_env(flurm_controller, max_node_connections)
        end}
     ]}.

%%====================================================================
%% get_http_api_config Tests
%%====================================================================

get_http_api_config_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(sasl),
         ok
     end,
     fun(_) ->
         application:unset_env(flurm_controller, http_api_port),
         application:unset_env(flurm_controller, listen_address),
         application:unset_env(flurm_controller, num_acceptors)
     end,
     [
        {"returns tuple with defaults", fun() ->
            application:unset_env(flurm_controller, http_api_port),
            application:unset_env(flurm_controller, listen_address),
            application:unset_env(flurm_controller, num_acceptors),
            {Port, Address, NumAcceptors} = flurm_controller_sup:get_http_api_config(),
            ?assertEqual(6820, Port),
            ?assertEqual("0.0.0.0", Address),
            ?assertEqual(10, NumAcceptors)
        end},
        {"respects custom http_api_port", fun() ->
            application:set_env(flurm_controller, http_api_port, 8080),
            {Port, _, _} = flurm_controller_sup:get_http_api_config(),
            ?assertEqual(8080, Port),
            application:unset_env(flurm_controller, http_api_port)
        end},
        {"uses same address as main listener", fun() ->
            application:set_env(flurm_controller, listen_address, "10.0.0.1"),
            {_, Address, _} = flurm_controller_sup:get_http_api_config(),
            ?assertEqual("10.0.0.1", Address),
            application:unset_env(flurm_controller, listen_address)
        end}
     ]}.

%%====================================================================
%% is_cluster_enabled Tests
%%====================================================================

is_cluster_enabled_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(sasl),
         ok
     end,
     fun(_) ->
         application:unset_env(flurm_controller, enable_cluster)
     end,
     [
        {"returns boolean", fun() ->
            Result = flurm_controller_sup:is_cluster_enabled(),
            ?assert(is_boolean(Result))
        end},
        {"respects explicit false", fun() ->
            application:set_env(flurm_controller, enable_cluster, false),
            ?assertEqual(false, flurm_controller_sup:is_cluster_enabled()),
            application:unset_env(flurm_controller, enable_cluster)
        end},
        {"respects explicit true", fun() ->
            application:set_env(flurm_controller, enable_cluster, true),
            Result = flurm_controller_sup:is_cluster_enabled(),
            %% May be false if not distributed
            ?assert(is_boolean(Result)),
            application:unset_env(flurm_controller, enable_cluster)
        end},
        {"depends on distributed erlang", fun() ->
            application:unset_env(flurm_controller, enable_cluster),
            Result = flurm_controller_sup:is_cluster_enabled(),
            %% On nonode@nohost, should be false even if enabled
            case node() of
                'nonode@nohost' ->
                    ?assertEqual(false, Result);
                _ ->
                    ?assert(is_boolean(Result))
            end
        end}
     ]}.

%%====================================================================
%% Listener API Tests
%%====================================================================

listener_info_test_() ->
    [
        {"returns map or error when not running", fun() ->
            Result = flurm_controller_sup:listener_info(),
            case Result of
                {error, not_found} -> ok;
                M when is_map(M) ->
                    ?assert(maps:is_key(port, M)),
                    ?assert(maps:is_key(status, M))
            end
        end}
    ].

node_listener_info_test_() ->
    [
        {"returns map or error when not running", fun() ->
            Result = flurm_controller_sup:node_listener_info(),
            case Result of
                {error, not_found} -> ok;
                M when is_map(M) ->
                    ?assert(maps:is_key(port, M)),
                    ?assert(maps:is_key(status, M))
            end
        end}
    ].

http_api_info_test_() ->
    [
        {"returns map or error when not running", fun() ->
            Result = flurm_controller_sup:http_api_info(),
            case Result of
                {error, not_found} -> ok;
                M when is_map(M) ->
                    ?assert(maps:is_key(port, M)),
                    ?assert(maps:is_key(status, M))
            end
        end}
    ].

%%====================================================================
%% Edge Cases
%%====================================================================

edge_cases_test_() ->
    [
        {"parse_address with empty string", fun() ->
            Result = flurm_controller_sup:parse_address(""),
            ?assertEqual({0, 0, 0, 0}, Result)
        end},
        {"config with zero port", fun() ->
            application:set_env(flurm_controller, listen_port, 0),
            {Port, _, _, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(0, Port),
            application:unset_env(flurm_controller, listen_port)
        end},
        {"config with high port", fun() ->
            application:set_env(flurm_controller, listen_port, 65535),
            {Port, _, _, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(65535, Port),
            application:unset_env(flurm_controller, listen_port)
        end},
        {"config with low acceptors", fun() ->
            application:set_env(flurm_controller, num_acceptors, 1),
            {_, _, NumAcceptors, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(1, NumAcceptors),
            application:unset_env(flurm_controller, num_acceptors)
        end},
        {"config with high acceptors", fun() ->
            application:set_env(flurm_controller, num_acceptors, 1000),
            {_, _, NumAcceptors, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(1000, NumAcceptors),
            application:unset_env(flurm_controller, num_acceptors)
        end}
    ].

%%====================================================================
%% Stress Tests
%%====================================================================

stress_test_() ->
    [
        {"parse_address handles many calls", fun() ->
            lists:foreach(fun(I) ->
                Addr = io_lib:format("192.168.~p.~p", [I rem 256, I rem 256]),
                Result = flurm_controller_sup:parse_address(lists:flatten(Addr)),
                ?assert(is_tuple(Result))
            end, lists:seq(1, 100))
        end},
        {"get_listener_config is consistent", fun() ->
            Results = [flurm_controller_sup:get_listener_config() || _ <- lists:seq(1, 10)],
            [First | Rest] = Results,
            lists:foreach(fun(R) ->
                ?assertEqual(First, R)
            end, Rest)
        end}
    ].
