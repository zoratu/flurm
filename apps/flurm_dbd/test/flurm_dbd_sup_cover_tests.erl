%%%-------------------------------------------------------------------
%%% @doc Coverage tests for flurm_dbd_sup (230 lines) +
%%% flurm_dbd_storage (212 lines).
%%%
%%% flurm_dbd_sup: Tests parse_address, get_listener_config,
%%% get_ra_cluster_nodes, init/1 (supervisor), start_listener,
%%% stop_listener, listener_info, start_ra_cluster, ra_cluster_info.
%%%
%%% flurm_dbd_storage: Start the real gen_server with ETS backend.
%%% Test store/fetch/delete/list/list-pattern/sync operations.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_sup_cover_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% flurm_dbd_sup pure function tests (TEST exports)
%%====================================================================

sup_pure_test_() ->
    {setup,
     fun setup_sup_pure/0,
     fun cleanup_sup_pure/1,
     [
      %% parse_address
      {"parse_address 0.0.0.0",            fun test_parse_0000/0},
      {"parse_address ::",                 fun test_parse_ipv6_any/0},
      {"parse_address valid IPv4",         fun test_parse_valid_ipv4/0},
      {"parse_address invalid string",     fun test_parse_invalid/0},
      {"parse_address tuple passthrough",  fun test_parse_tuple/0},

      %% get_listener_config
      {"get_listener_config defaults",     fun test_listener_config_defaults/0},
      {"get_listener_config custom",       fun test_listener_config_custom/0},

      %% get_ra_cluster_nodes
      {"get_ra_cluster_nodes default",     fun test_ra_nodes_default/0},
      {"get_ra_cluster_nodes configured",  fun test_ra_nodes_configured/0},

      %% init/1
      {"init returns supervisor spec",     fun test_sup_init/0}
     ]}.

setup_sup_pure() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),
    %% Clear env
    application:unset_env(flurm_dbd, listen_port),
    application:unset_env(flurm_dbd, listen_address),
    application:unset_env(flurm_dbd, num_acceptors),
    application:unset_env(flurm_dbd, max_connections),
    application:unset_env(flurm_dbd, ra_cluster_nodes),
    ok.

cleanup_sup_pure(_) ->
    application:unset_env(flurm_dbd, listen_port),
    application:unset_env(flurm_dbd, listen_address),
    application:unset_env(flurm_dbd, num_acceptors),
    application:unset_env(flurm_dbd, max_connections),
    application:unset_env(flurm_dbd, ra_cluster_nodes),
    meck:unload(lager),
    ok.

%% --- parse_address ---

test_parse_0000() ->
    ?assertEqual({0, 0, 0, 0}, flurm_dbd_sup:parse_address("0.0.0.0")).

test_parse_ipv6_any() ->
    ?assertEqual({0, 0, 0, 0, 0, 0, 0, 0}, flurm_dbd_sup:parse_address("::")).

test_parse_valid_ipv4() ->
    ?assertEqual({192, 168, 1, 1}, flurm_dbd_sup:parse_address("192.168.1.1")).

test_parse_invalid() ->
    ?assertEqual({0, 0, 0, 0}, flurm_dbd_sup:parse_address("not.an.ip.addr.ess")).

test_parse_tuple() ->
    ?assertEqual({127, 0, 0, 1}, flurm_dbd_sup:parse_address({127, 0, 0, 1})).

%% --- get_listener_config ---

test_listener_config_defaults() ->
    {Port, Address, NumAcceptors, MaxConns} = flurm_dbd_sup:get_listener_config(),
    ?assertEqual(6819, Port),
    ?assertEqual("0.0.0.0", Address),
    ?assertEqual(5, NumAcceptors),
    ?assertEqual(100, MaxConns).

test_listener_config_custom() ->
    application:set_env(flurm_dbd, listen_port, 7777),
    application:set_env(flurm_dbd, listen_address, "10.0.0.1"),
    application:set_env(flurm_dbd, num_acceptors, 10),
    application:set_env(flurm_dbd, max_connections, 200),
    {Port, Address, NumAcceptors, MaxConns} = flurm_dbd_sup:get_listener_config(),
    ?assertEqual(7777, Port),
    ?assertEqual("10.0.0.1", Address),
    ?assertEqual(10, NumAcceptors),
    ?assertEqual(200, MaxConns).

%% --- get_ra_cluster_nodes ---

test_ra_nodes_default() ->
    application:unset_env(flurm_dbd, ra_cluster_nodes),
    Nodes = flurm_dbd_sup:get_ra_cluster_nodes(),
    ?assertEqual([node()], Nodes).

test_ra_nodes_configured() ->
    application:set_env(flurm_dbd, ra_cluster_nodes, ['node1@host', 'node2@host']),
    Nodes = flurm_dbd_sup:get_ra_cluster_nodes(),
    ?assertEqual(['node1@host', 'node2@host'], Nodes).

%% --- init/1 ---

test_sup_init() ->
    {ok, {SupFlags, Children}} = flurm_dbd_sup:init([]),
    ?assertMatch(#{strategy := one_for_one}, SupFlags),
    ?assert(length(Children) >= 3),
    Ids = [maps:get(id, C) || C <- Children],
    ?assert(lists:member(flurm_dbd_storage, Ids)),
    ?assert(lists:member(flurm_dbd_fragment, Ids)),
    ?assert(lists:member(flurm_dbd_server, Ids)).

%%====================================================================
%% flurm_dbd_sup dynamic function tests (mock ranch and ra)
%%====================================================================

sup_dynamic_test_() ->
    {foreach,
     fun setup_sup_dynamic/0,
     fun cleanup_sup_dynamic/1,
     [
      {"start_listener success",         fun test_start_listener/0},
      {"start_listener already_started", fun test_start_listener_already/0},
      {"start_listener error",           fun test_start_listener_error/0},
      {"stop_listener",                  fun test_stop_listener/0},
      {"listener_info running",          fun test_listener_info_running/0},
      {"listener_info not_found",        fun test_listener_info_not_found/0},
      {"start_ra_cluster/0",             fun test_start_ra_0/0},
      {"start_ra_cluster/1 success",     fun test_start_ra_1/0},
      {"start_ra_cluster/1 error",       fun test_start_ra_error/0},
      {"start_ra_cluster configured nodes", fun test_start_ra_configured/0},
      {"ra_cluster_info success",        fun test_ra_info_success/0},
      {"ra_cluster_info error",          fun test_ra_info_error/0}
     ]}.

setup_sup_dynamic() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),

    meck:new(ranch, [non_strict, no_link]),
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) -> {ok, self()} end),
    meck:expect(ranch, stop_listener, fun(_) -> ok end),
    meck:expect(ranch, get_port, fun(_) -> 6819 end),
    meck:expect(ranch, get_max_connections, fun(_) -> 100 end),
    meck:expect(ranch, procs, fun(_, _) -> [] end),

    meck:new(flurm_dbd_ra, [non_strict, no_link]),
    meck:expect(flurm_dbd_ra, start_cluster, fun(_) -> ok end),

    meck:new(ra, [non_strict, no_link]),
    meck:expect(ra, members, fun(_) -> {ok, [{flurm_dbd_ra, node()}], {flurm_dbd_ra, node()}} end),

    application:unset_env(flurm_dbd, ra_cluster_nodes),
    ok.

cleanup_sup_dynamic(_) ->
    application:unset_env(flurm_dbd, ra_cluster_nodes),
    catch meck:unload(ra),
    catch meck:unload(flurm_dbd_ra),
    catch meck:unload(ranch),
    catch meck:unload(lager),
    ok.

test_start_listener() ->
    ?assertMatch({ok, _Pid}, flurm_dbd_sup:start_listener()).

test_start_listener_already() ->
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) ->
        {error, {already_started, self()}}
    end),
    ?assertMatch({ok, _Pid}, flurm_dbd_sup:start_listener()).

test_start_listener_error() ->
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) ->
        {error, eaddrinuse}
    end),
    ?assertMatch({error, eaddrinuse}, flurm_dbd_sup:start_listener()).

test_stop_listener() ->
    ?assertEqual(ok, flurm_dbd_sup:stop_listener()).

test_listener_info_running() ->
    Info = flurm_dbd_sup:listener_info(),
    ?assertMatch(#{port := 6819}, Info),
    ?assertEqual(running, maps:get(status, Info)).

test_listener_info_not_found() ->
    meck:expect(ranch, get_port, fun(_) -> error(badarg) end),
    ?assertEqual({error, not_found}, flurm_dbd_sup:listener_info()).

test_start_ra_0() ->
    ?assertEqual(ok, flurm_dbd_sup:start_ra_cluster()).

test_start_ra_1() ->
    ?assertEqual(ok, flurm_dbd_sup:start_ra_cluster([node()])).

test_start_ra_error() ->
    meck:expect(flurm_dbd_ra, start_cluster, fun(_) -> {error, already_started} end),
    ?assertMatch({error, _}, flurm_dbd_sup:start_ra_cluster([node()])).

test_start_ra_configured() ->
    application:set_env(flurm_dbd, ra_cluster_nodes, ['n1@h', 'n2@h']),
    ?assertEqual(ok, flurm_dbd_sup:start_ra_cluster([node()])),
    ?assert(meck:called(flurm_dbd_ra, start_cluster, [['n1@h', 'n2@h']])).

test_ra_info_success() ->
    Info = flurm_dbd_sup:ra_cluster_info(),
    ?assertMatch(#{cluster_name := _, members := _, leader := _}, Info),
    ?assertEqual(running, maps:get(status, Info)).

test_ra_info_error() ->
    meck:expect(ra, members, fun(_) -> {error, noproc} end),
    ?assertMatch({error, _}, flurm_dbd_sup:ra_cluster_info()).

%%====================================================================
%% flurm_dbd_storage gen_server tests
%%====================================================================

storage_test_() ->
    {foreach,
     fun setup_storage/0,
     fun cleanup_storage/1,
     [
      {"store and fetch",                fun test_store_fetch/0},
      {"fetch not found",                fun test_fetch_not_found/0},
      {"fetch unknown table",            fun test_fetch_unknown_table/0},
      {"store unknown table",            fun test_store_unknown_table/0},
      {"delete and verify",              fun test_delete/0},
      {"delete unknown table",           fun test_delete_unknown_table/0},
      {"list all",                       fun test_list_all/0},
      {"list empty table",               fun test_list_empty/0},
      {"list unknown table",             fun test_list_unknown_table/0},
      {"list with pattern",              fun test_list_pattern/0},
      {"list pattern unknown table",     fun test_list_pattern_unknown/0},
      {"sync ets (noop)",                fun test_sync_ets/0},
      {"overwrite existing key",         fun test_overwrite/0},
      {"unknown call",                   fun test_storage_unknown_call/0},
      {"unknown cast",                   fun test_storage_unknown_cast/0},
      {"unknown info",                   fun test_storage_unknown_info/0},
      {"terminate",                      fun test_storage_terminate/0}
     ]}.

setup_storage() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info,    fun(_) -> ok end),
    meck:expect(lager, info,    fun(_, _) -> ok end),
    meck:expect(lager, debug,   fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error,   fun(_, _) -> ok end),

    %% Ensure ETS backend
    application:set_env(flurm_dbd, storage_backend, ets),
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Pid.

cleanup_storage(Pid) ->
    catch gen_server:stop(Pid),
    timer:sleep(10),
    meck:unload(lager),
    ok.

test_store_fetch() ->
    ?assertEqual(ok, flurm_dbd_storage:store(jobs, 1, #{name => <<"test">>})),
    ?assertEqual({ok, #{name => <<"test">>}}, flurm_dbd_storage:fetch(jobs, 1)).

test_fetch_not_found() ->
    ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(jobs, 999)).

test_fetch_unknown_table() ->
    ?assertEqual({error, unknown_table}, flurm_dbd_storage:fetch(nonexistent, 1)).

test_store_unknown_table() ->
    ?assertEqual({error, unknown_table}, flurm_dbd_storage:store(nonexistent, 1, val)).

test_delete() ->
    flurm_dbd_storage:store(jobs, 10, val),
    ?assertEqual(ok, flurm_dbd_storage:delete(jobs, 10)),
    ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(jobs, 10)).

test_delete_unknown_table() ->
    ?assertEqual(ok, flurm_dbd_storage:delete(nonexistent, 1)).

test_list_all() ->
    flurm_dbd_storage:store(accounts, <<"a1">>, #{name => <<"a1">>}),
    flurm_dbd_storage:store(accounts, <<"a2">>, #{name => <<"a2">>}),
    Values = flurm_dbd_storage:list(accounts),
    ?assert(length(Values) >= 2).

test_list_empty() ->
    Values = flurm_dbd_storage:list(qos),
    ?assertEqual([], Values).

test_list_unknown_table() ->
    ?assertEqual([], flurm_dbd_storage:list(nonexistent)).

test_list_pattern() ->
    flurm_dbd_storage:store(users, <<"u1">>, #{role => admin}),
    flurm_dbd_storage:store(users, <<"u2">>, #{role => user}),
    Values = flurm_dbd_storage:list(users, {'_', '_'}),
    ?assert(length(Values) >= 2).

test_list_pattern_unknown() ->
    ?assertEqual([], flurm_dbd_storage:list(nonexistent, {'_', '_'})).

test_sync_ets() ->
    ?assertEqual(ok, flurm_dbd_storage:sync()).

test_overwrite() ->
    flurm_dbd_storage:store(jobs, 100, #{v => 1}),
    flurm_dbd_storage:store(jobs, 100, #{v => 2}),
    ?assertEqual({ok, #{v => 2}}, flurm_dbd_storage:fetch(jobs, 100)).

test_storage_unknown_call() ->
    ?assertEqual({error, unknown_request},
                 gen_server:call(flurm_dbd_storage, bogus)).

test_storage_unknown_cast() ->
    gen_server:cast(flurm_dbd_storage, bogus),
    timer:sleep(50),
    ?assertEqual(ok, flurm_dbd_storage:sync()).

test_storage_unknown_info() ->
    flurm_dbd_storage ! bogus,
    timer:sleep(50),
    ?assertEqual(ok, flurm_dbd_storage:sync()).

test_storage_terminate() ->
    ok = gen_server:stop(flurm_dbd_storage),
    timer:sleep(50),
    ok.
