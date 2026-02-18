%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Sup 100% Coverage Tests
%%%
%%% Comprehensive tests targeting all code paths in flurm_controller_sup
%%% including configuration parsing, address parsing edge cases, and
%%% supervisor initialization.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_sup_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

config_test_() ->
    {foreach,
     fun setup_config/0,
     fun cleanup_config/1,
     [
        fun test_get_listener_config_defaults/1,
        fun test_get_listener_config_custom/1,
        fun test_get_node_listener_config_defaults/1,
        fun test_get_node_listener_config_custom/1,
        fun test_get_http_api_config_defaults/1,
        fun test_get_http_api_config_custom/1
     ]}.

setup_config() ->
    application:ensure_all_started(lager),
    %% Clear all config
    ConfigKeys = [listen_port, listen_address, num_acceptors, max_connections,
                  node_listen_port, max_node_connections, http_api_port],
    lists:foreach(fun(Key) ->
        application:unset_env(flurm_controller, Key)
    end, ConfigKeys),
    ok.

cleanup_config(_) ->
    ok.

%%====================================================================
%% Listener Configuration Tests
%%====================================================================

test_get_listener_config_defaults(_) ->
    {"get_listener_config returns defaults", fun() ->
        {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_listener_config(),
        ?assertEqual(6817, Port),
        ?assertEqual("0.0.0.0", Address),
        ?assertEqual(10, NumAcceptors),
        ?assertEqual(1000, MaxConns)
    end}.

test_get_listener_config_custom(_) ->
    {"get_listener_config respects custom values", fun() ->
        application:set_env(flurm_controller, listen_port, 16817),
        application:set_env(flurm_controller, listen_address, "192.168.1.1"),
        application:set_env(flurm_controller, num_acceptors, 20),
        application:set_env(flurm_controller, max_connections, 5000),

        {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_listener_config(),
        ?assertEqual(16817, Port),
        ?assertEqual("192.168.1.1", Address),
        ?assertEqual(20, NumAcceptors),
        ?assertEqual(5000, MaxConns),

        %% Cleanup
        application:unset_env(flurm_controller, listen_port),
        application:unset_env(flurm_controller, listen_address),
        application:unset_env(flurm_controller, num_acceptors),
        application:unset_env(flurm_controller, max_connections)
    end}.

test_get_node_listener_config_defaults(_) ->
    {"get_node_listener_config returns defaults", fun() ->
        {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_node_listener_config(),
        ?assertEqual(6818, Port),
        ?assertEqual("0.0.0.0", Address),
        ?assertEqual(10, NumAcceptors),
        ?assertEqual(500, MaxConns)
    end}.

test_get_node_listener_config_custom(_) ->
    {"get_node_listener_config respects custom values", fun() ->
        application:set_env(flurm_controller, node_listen_port, 16818),
        application:set_env(flurm_controller, listen_address, "10.0.0.1"),
        application:set_env(flurm_controller, num_acceptors, 15),
        application:set_env(flurm_controller, max_node_connections, 250),

        {Port, Address, NumAcceptors, MaxConns} = flurm_controller_sup:get_node_listener_config(),
        ?assertEqual(16818, Port),
        ?assertEqual("10.0.0.1", Address),
        ?assertEqual(15, NumAcceptors),
        ?assertEqual(250, MaxConns),

        %% Cleanup
        application:unset_env(flurm_controller, node_listen_port),
        application:unset_env(flurm_controller, listen_address),
        application:unset_env(flurm_controller, num_acceptors),
        application:unset_env(flurm_controller, max_node_connections)
    end}.

test_get_http_api_config_defaults(_) ->
    {"get_http_api_config returns defaults", fun() ->
        {Port, Address, NumAcceptors} = flurm_controller_sup:get_http_api_config(),
        ?assertEqual(6820, Port),
        ?assertEqual("0.0.0.0", Address),
        ?assertEqual(10, NumAcceptors)
    end}.

test_get_http_api_config_custom(_) ->
    {"get_http_api_config respects custom values", fun() ->
        application:set_env(flurm_controller, http_api_port, 8080),
        application:set_env(flurm_controller, listen_address, "127.0.0.1"),
        application:set_env(flurm_controller, num_acceptors, 5),

        {Port, Address, NumAcceptors} = flurm_controller_sup:get_http_api_config(),
        ?assertEqual(8080, Port),
        ?assertEqual("127.0.0.1", Address),
        ?assertEqual(5, NumAcceptors),

        %% Cleanup
        application:unset_env(flurm_controller, http_api_port),
        application:unset_env(flurm_controller, listen_address),
        application:unset_env(flurm_controller, num_acceptors)
    end}.

%%====================================================================
%% parse_address Tests
%%====================================================================

parse_address_test_() ->
    [
        {"0.0.0.0 shortcut", fun() ->
            ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("0.0.0.0"))
        end},
        {":: shortcut (IPv6 any)", fun() ->
            ?assertEqual({0, 0, 0, 0, 0, 0, 0, 0}, flurm_controller_sup:parse_address("::"))
        end},
        {"localhost IPv4", fun() ->
            ?assertEqual({127, 0, 0, 1}, flurm_controller_sup:parse_address("127.0.0.1"))
        end},
        {"private network address", fun() ->
            ?assertEqual({192, 168, 1, 100}, flurm_controller_sup:parse_address("192.168.1.100"))
        end},
        {"10.x network", fun() ->
            ?assertEqual({10, 0, 0, 1}, flurm_controller_sup:parse_address("10.0.0.1"))
        end},
        {"172.16.x network", fun() ->
            ?assertEqual({172, 16, 0, 1}, flurm_controller_sup:parse_address("172.16.0.1"))
        end},
        {"broadcast address", fun() ->
            ?assertEqual({255, 255, 255, 255}, flurm_controller_sup:parse_address("255.255.255.255"))
        end},
        {"IPv6 localhost", fun() ->
            ?assertEqual({0, 0, 0, 0, 0, 0, 0, 1}, flurm_controller_sup:parse_address("::1"))
        end},
        {"IPv6 link local", fun() ->
            Result = flurm_controller_sup:parse_address("fe80::1"),
            ?assert(is_tuple(Result)),
            ?assertEqual(8, tuple_size(Result))
        end},
        {"invalid address fallback", fun() ->
            ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("invalid"))
        end},
        {"empty string fallback", fun() ->
            ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address(""))
        end},
        {"address with port fallback", fun() ->
            ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("192.168.1.1:8080"))
        end},
        {"hostname fallback", fun() ->
            ?assertEqual({0, 0, 0, 0}, flurm_controller_sup:parse_address("localhost"))
        end},
        {"tuple passthrough IPv4", fun() ->
            Addr = {10, 20, 30, 40},
            ?assertEqual(Addr, flurm_controller_sup:parse_address(Addr))
        end},
        {"tuple passthrough IPv6", fun() ->
            Addr = {0, 0, 0, 0, 0, 0, 0, 1},
            ?assertEqual(Addr, flurm_controller_sup:parse_address(Addr))
        end},
        {"various valid IPv4 addresses", fun() ->
            Addresses = [
                {"1.2.3.4", {1, 2, 3, 4}},
                {"192.0.2.1", {192, 0, 2, 1}},
                {"198.51.100.1", {198, 51, 100, 1}},
                {"203.0.113.1", {203, 0, 113, 1}}
            ],
            lists:foreach(fun({Str, Expected}) ->
                ?assertEqual(Expected, flurm_controller_sup:parse_address(Str))
            end, Addresses)
        end}
    ].

%%====================================================================
%% is_cluster_enabled Tests
%%====================================================================

is_cluster_enabled_test_() ->
    {foreach,
     fun() ->
         application:ensure_all_started(lager),
         ok
     end,
     fun(_) ->
         application:unset_env(flurm_controller, enable_cluster),
         ok
     end,
     [
        fun test_cluster_explicitly_disabled/0,
        fun test_cluster_explicitly_enabled/0,
        fun test_cluster_default_behavior/0
     ]}.

test_cluster_explicitly_disabled() ->
    application:set_env(flurm_controller, enable_cluster, false),
    ?assertEqual(false, flurm_controller_sup:is_cluster_enabled()).

test_cluster_explicitly_enabled() ->
    application:set_env(flurm_controller, enable_cluster, true),
    Result = flurm_controller_sup:is_cluster_enabled(),
    %% On nonode@nohost, should return false even if enabled
    case node() of
        'nonode@nohost' ->
            ?assertEqual(false, Result);
        _ ->
            ?assertEqual(true, Result)
    end.

test_cluster_default_behavior() ->
    application:unset_env(flurm_controller, enable_cluster),
    Result = flurm_controller_sup:is_cluster_enabled(),
    %% Default is true, but depends on distributed Erlang
    ?assert(is_boolean(Result)).

%%====================================================================
%% Supervisor Init Tests
%%====================================================================

supervisor_init_test_() ->
    {setup,
     fun() ->
         application:ensure_all_started(lager),
         application:set_env(flurm_controller, enable_cluster, false),
         ok
     end,
     fun(_) ->
         application:unset_env(flurm_controller, enable_cluster),
         ok
     end,
     [
        {"init returns valid supervisor spec", fun() ->
            {ok, {SupFlags, Children}} = flurm_controller_sup:init([]),

            %% Verify SupFlags
            ?assertMatch(#{strategy := one_for_one}, SupFlags),
            ?assertMatch(#{intensity := _}, SupFlags),
            ?assertMatch(#{period := _}, SupFlags),

            %% Verify Children is a list
            ?assert(is_list(Children)),
            ?assert(length(Children) > 0)
        end},
        {"all child specs are valid maps", fun() ->
            {ok, {_, Children}} = flurm_controller_sup:init([]),
            lists:foreach(fun(Child) ->
                ?assert(is_map(Child)),
                ?assert(maps:is_key(id, Child)),
                ?assert(maps:is_key(start, Child)),
                ?assert(maps:is_key(restart, Child)),
                ?assert(maps:is_key(shutdown, Child)),
                ?assert(maps:is_key(type, Child)),
                ?assert(maps:is_key(modules, Child))
            end, Children)
        end},
        {"child specs have valid start MFA", fun() ->
            {ok, {_, Children}} = flurm_controller_sup:init([]),
            lists:foreach(fun(#{start := {M, F, A}}) ->
                ?assert(is_atom(M)),
                ?assert(is_atom(F)),
                ?assert(is_list(A))
            end, Children)
        end},
        {"child specs have valid restart strategy", fun() ->
            {ok, {_, Children}} = flurm_controller_sup:init([]),
            ValidRestart = [permanent, transient, temporary],
            lists:foreach(fun(#{restart := Restart}) ->
                ?assert(lists:member(Restart, ValidRestart))
            end, Children)
        end},
        {"child specs have valid type", fun() ->
            {ok, {_, Children}} = flurm_controller_sup:init([]),
            ValidTypes = [worker, supervisor],
            lists:foreach(fun(#{type := Type}) ->
                ?assert(lists:member(Type, ValidTypes))
            end, Children)
        end},
        {"cluster disabled excludes cluster children", fun() ->
            application:set_env(flurm_controller, enable_cluster, false),
            {ok, {_, Children}} = flurm_controller_sup:init([]),
            ClusterChildren = [C || C <- Children,
                                   maps:get(id, C) =:= flurm_controller_cluster orelse
                                   maps:get(id, C) =:= flurm_controller_failover],
            ?assertEqual([], ClusterChildren)
        end},
        {"required children are present", fun() ->
            {ok, {_, Children}} = flurm_controller_sup:init([]),
            Ids = [maps:get(id, C) || C <- Children],
            RequiredIds = [flurm_connection_limiter, flurm_metrics, flurm_job_manager,
                          flurm_node_manager_server, flurm_scheduler, flurm_partition_manager],
            lists:foreach(fun(Id) ->
                ?assert(lists:member(Id, Ids),
                       io_lib:format("Missing required child: ~p", [Id]))
            end, RequiredIds)
        end}
     ]}.

%%====================================================================
%% Listener Info Tests
%%====================================================================

listener_info_test_() ->
    [
        {"listener_info when not running", fun() ->
            Result = flurm_controller_sup:listener_info(),
            case Result of
                {error, not_found} -> ok;
                M when is_map(M) -> ?assert(maps:is_key(status, M))
            end
        end},
        {"node_listener_info when not running", fun() ->
            Result = flurm_controller_sup:node_listener_info(),
            case Result of
                {error, not_found} -> ok;
                M when is_map(M) -> ?assert(maps:is_key(status, M))
            end
        end},
        {"http_api_info when not running", fun() ->
            Result = flurm_controller_sup:http_api_info(),
            case Result of
                {error, not_found} -> ok;
                M when is_map(M) -> ?assert(maps:is_key(status, M))
            end
        end}
    ].

%%====================================================================
%% Stop Listener Tests
%%====================================================================

stop_listener_test_() ->
    [
        {"stop_listener when not running returns error", fun() ->
            Result = flurm_controller_sup:stop_listener(),
            ?assertEqual({error, not_found}, Result)
        end},
        {"stop_node_listener when not running returns error", fun() ->
            Result = flurm_controller_sup:stop_node_listener(),
            ?assertEqual({error, not_found}, Result)
        end},
        {"stop_http_api when not running returns error", fun() ->
            Result = flurm_controller_sup:stop_http_api(),
            ?assertEqual({error, not_found}, Result)
        end}
    ].

%%====================================================================
%% Configuration Boundary Tests
%%====================================================================

config_boundary_test_() ->
    [
        {"port 0 accepted", fun() ->
            application:set_env(flurm_controller, listen_port, 0),
            {Port, _, _, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(0, Port),
            application:unset_env(flurm_controller, listen_port)
        end},
        {"port 65535 accepted", fun() ->
            application:set_env(flurm_controller, listen_port, 65535),
            {Port, _, _, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(65535, Port),
            application:unset_env(flurm_controller, listen_port)
        end},
        {"1 acceptor accepted", fun() ->
            application:set_env(flurm_controller, num_acceptors, 1),
            {_, _, NumAcceptors, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(1, NumAcceptors),
            application:unset_env(flurm_controller, num_acceptors)
        end},
        {"many acceptors accepted", fun() ->
            application:set_env(flurm_controller, num_acceptors, 1000),
            {_, _, NumAcceptors, _} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(1000, NumAcceptors),
            application:unset_env(flurm_controller, num_acceptors)
        end},
        {"max_connections boundary", fun() ->
            application:set_env(flurm_controller, max_connections, 100000),
            {_, _, _, MaxConns} = flurm_controller_sup:get_listener_config(),
            ?assertEqual(100000, MaxConns),
            application:unset_env(flurm_controller, max_connections)
        end}
    ].

%%====================================================================
%% Consistency Tests
%%====================================================================

consistency_test_() ->
    [
        {"get_listener_config is consistent", fun() ->
            Results = [flurm_controller_sup:get_listener_config() || _ <- lists:seq(1, 10)],
            [First | Rest] = Results,
            lists:foreach(fun(R) ->
                ?assertEqual(First, R)
            end, Rest)
        end},
        {"get_node_listener_config is consistent", fun() ->
            Results = [flurm_controller_sup:get_node_listener_config() || _ <- lists:seq(1, 10)],
            [First | Rest] = Results,
            lists:foreach(fun(R) ->
                ?assertEqual(First, R)
            end, Rest)
        end},
        {"get_http_api_config is consistent", fun() ->
            Results = [flurm_controller_sup:get_http_api_config() || _ <- lists:seq(1, 10)],
            [First | Rest] = Results,
            lists:foreach(fun(R) ->
                ?assertEqual(First, R)
            end, Rest)
        end},
        {"parse_address is consistent", fun() ->
            Addresses = ["0.0.0.0", "127.0.0.1", "192.168.1.1", "::"],
            lists:foreach(fun(Addr) ->
                Results = [flurm_controller_sup:parse_address(Addr) || _ <- lists:seq(1, 10)],
                [First | Rest] = Results,
                lists:foreach(fun(R) ->
                    ?assertEqual(First, R)
                end, Rest)
            end, Addresses)
        end}
    ].
