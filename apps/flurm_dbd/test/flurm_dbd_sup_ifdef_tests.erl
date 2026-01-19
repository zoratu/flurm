%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_dbd_sup internal functions
%%% Tests the -ifdef(TEST) exported helper functions directly
%%%-------------------------------------------------------------------
-module(flurm_dbd_sup_ifdef_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Cases for parse_address/1
%%====================================================================

parse_address_test_() ->
    [
     {"parse_address with 0.0.0.0", fun() ->
         ?assertEqual({0, 0, 0, 0}, flurm_dbd_sup:parse_address("0.0.0.0"))
     end},
     {"parse_address with IPv6 ::", fun() ->
         ?assertEqual({0, 0, 0, 0, 0, 0, 0, 0}, flurm_dbd_sup:parse_address("::"))
     end},
     {"parse_address with valid IPv4", fun() ->
         ?assertEqual({127, 0, 0, 1}, flurm_dbd_sup:parse_address("127.0.0.1"))
     end},
     {"parse_address with valid IPv4 tuple", fun() ->
         ?assertEqual({192, 168, 1, 1}, flurm_dbd_sup:parse_address({192, 168, 1, 1}))
     end},
     {"parse_address with valid IPv6", fun() ->
         ?assertEqual({0, 0, 0, 0, 0, 0, 0, 1}, flurm_dbd_sup:parse_address("::1"))
     end},
     {"parse_address with invalid address returns default", fun() ->
         ?assertEqual({0, 0, 0, 0}, flurm_dbd_sup:parse_address("invalid"))
     end},
     {"parse_address with specific address", fun() ->
         ?assertEqual({10, 0, 0, 1}, flurm_dbd_sup:parse_address("10.0.0.1"))
     end}
    ].

%%====================================================================
%% Test Cases for get_listener_config/0
%%====================================================================

get_listener_config_test_() ->
    {setup,
     fun() ->
         %% Store original values
         OrigPort = application:get_env(flurm_dbd, listen_port),
         OrigAddr = application:get_env(flurm_dbd, listen_address),
         OrigAcceptors = application:get_env(flurm_dbd, num_acceptors),
         OrigMaxConns = application:get_env(flurm_dbd, max_connections),
         {OrigPort, OrigAddr, OrigAcceptors, OrigMaxConns}
     end,
     fun({OrigPort, OrigAddr, OrigAcceptors, OrigMaxConns}) ->
         %% Restore original values
         case OrigPort of
             undefined -> application:unset_env(flurm_dbd, listen_port);
             {ok, V1} -> application:set_env(flurm_dbd, listen_port, V1)
         end,
         case OrigAddr of
             undefined -> application:unset_env(flurm_dbd, listen_address);
             {ok, V2} -> application:set_env(flurm_dbd, listen_address, V2)
         end,
         case OrigAcceptors of
             undefined -> application:unset_env(flurm_dbd, num_acceptors);
             {ok, V3} -> application:set_env(flurm_dbd, num_acceptors, V3)
         end,
         case OrigMaxConns of
             undefined -> application:unset_env(flurm_dbd, max_connections);
             {ok, V4} -> application:set_env(flurm_dbd, max_connections, V4)
         end
     end,
     fun(_) ->
         [
          {"get_listener_config returns defaults", fun() ->
              application:unset_env(flurm_dbd, listen_port),
              application:unset_env(flurm_dbd, listen_address),
              application:unset_env(flurm_dbd, num_acceptors),
              application:unset_env(flurm_dbd, max_connections),

              {Port, Address, NumAcceptors, MaxConns} = flurm_dbd_sup:get_listener_config(),
              ?assertEqual(6819, Port),
              ?assertEqual("0.0.0.0", Address),
              ?assertEqual(5, NumAcceptors),
              ?assertEqual(100, MaxConns)
          end},
          {"get_listener_config with custom values", fun() ->
              application:set_env(flurm_dbd, listen_port, 7000),
              application:set_env(flurm_dbd, listen_address, "127.0.0.1"),
              application:set_env(flurm_dbd, num_acceptors, 10),
              application:set_env(flurm_dbd, max_connections, 200),

              {Port, Address, NumAcceptors, MaxConns} = flurm_dbd_sup:get_listener_config(),
              ?assertEqual(7000, Port),
              ?assertEqual("127.0.0.1", Address),
              ?assertEqual(10, NumAcceptors),
              ?assertEqual(200, MaxConns)
          end}
         ]
     end}.

%%====================================================================
%% Test Cases for init/1
%%====================================================================

init_test_() ->
    [
     {"init returns supervisor spec", fun() ->
         {ok, {SupFlags, Children}} = flurm_dbd_sup:init([]),

         %% Check supervisor flags
         ?assertEqual(one_for_one, maps:get(strategy, SupFlags)),
         ?assertEqual(5, maps:get(intensity, SupFlags)),
         ?assertEqual(10, maps:get(period, SupFlags)),

         %% Check we have 2 children
         ?assertEqual(2, length(Children)),

         %% Check child IDs
         ChildIds = [maps:get(id, Child) || Child <- Children],
         ?assert(lists:member(flurm_dbd_storage, ChildIds)),
         ?assert(lists:member(flurm_dbd_server, ChildIds))
     end}
    ].

%%====================================================================
%% Test listener_info when not running
%%====================================================================

listener_info_not_running_test_() ->
    [
     {"listener_info returns error when not found", fun() ->
         %% Since no listener is running, should return error
         Result = flurm_dbd_sup:listener_info(),
         ?assertEqual({error, not_found}, Result)
     end}
    ].
