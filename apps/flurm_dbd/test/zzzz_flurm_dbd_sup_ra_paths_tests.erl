%%%-------------------------------------------------------------------
%%% @doc Focused coverage tests for flurm_dbd_sup RA/listener branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_dbd_sup_ra_paths_tests).

-include_lib("eunit/include/eunit.hrl").

sup_ra_paths_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_listener branches", fun test_start_listener_branches/0},
      {"ra cluster start/get nodes branches", fun test_ra_cluster_start_and_nodes/0},
      {"ra_cluster_info branches", fun test_ra_cluster_info_branches/0},
      {"parse_address branches", fun test_parse_address_branches/0}
     ]}.

setup() ->
    catch meck:unload(flurm_dbd_sup),
    catch meck:unload(lager),
    catch meck:unload(ranch),
    catch meck:unload(flurm_dbd_ra),
    catch meck:unload(ra),

    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    meck:new(ranch, [non_strict, no_link]),
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) -> {ok, self()} end),
    meck:expect(ranch, stop_listener, fun(_) -> ok end),
    meck:expect(ranch, get_port, fun(_) -> 6819 end),
    meck:expect(ranch, get_max_connections, fun(_) -> 100 end),
    meck:expect(ranch, procs, fun(_, _) -> [] end),

    meck:new(flurm_dbd_ra, [non_strict, no_link]),
    meck:expect(flurm_dbd_ra, start_cluster, fun(_) -> ok end),

    meck:new(ra, [non_strict, no_link]),
    meck:expect(ra, members, fun(_) ->
        {ok, [{flurm_dbd_ra, node()}], {flurm_dbd_ra, node()}}
    end),

    application:unset_env(flurm_dbd, ra_cluster_nodes),
    application:unset_env(flurm_dbd, listen_address),
    ok.

cleanup(_) ->
    application:unset_env(flurm_dbd, ra_cluster_nodes),
    application:unset_env(flurm_dbd, listen_address),
    catch meck:unload(ra),
    catch meck:unload(flurm_dbd_ra),
    catch meck:unload(ranch),
    catch meck:unload(lager),
    ok.

test_start_listener_branches() ->
    Pid = spawn(fun() -> receive stop -> ok end end),
    meck:expect(ranch, start_listener, fun(_, _, _, _, _) -> {ok, Pid} end),
    ?assertEqual({ok, Pid}, flurm_dbd_sup:start_listener()),

    meck:expect(ranch, start_listener, fun(_, _, _, _, _) ->
        {error, {already_started, Pid}}
    end),
    ?assertEqual({ok, Pid}, flurm_dbd_sup:start_listener()),

    meck:expect(ranch, start_listener, fun(_, _, _, _, _) ->
        {error, eaddrinuse}
    end),
    ?assertEqual({error, eaddrinuse}, flurm_dbd_sup:start_listener()),

    Pid ! stop,
    ok.

test_ra_cluster_start_and_nodes() ->
    application:unset_env(flurm_dbd, ra_cluster_nodes),
    ?assertEqual([node()], flurm_dbd_sup:get_ra_cluster_nodes()),

    ?assertEqual(ok, flurm_dbd_sup:start_ra_cluster()),
    ?assert(meck:called(flurm_dbd_ra, start_cluster, [[node()]])),

    application:set_env(flurm_dbd, ra_cluster_nodes, ['n1@host', 'n2@host']),
    ?assertEqual(['n1@host', 'n2@host'], flurm_dbd_sup:get_ra_cluster_nodes()),

    ?assertEqual(ok, flurm_dbd_sup:start_ra_cluster([node()])),
    ?assert(meck:called(flurm_dbd_ra, start_cluster, [['n1@host', 'n2@host']])),

    meck:expect(flurm_dbd_ra, start_cluster, fun(_) -> {error, no_quorum} end),
    ?assertEqual({error, no_quorum}, flurm_dbd_sup:start_ra_cluster([node()])).

test_ra_cluster_info_branches() ->
    Info = flurm_dbd_sup:ra_cluster_info(),
    ?assertMatch(#{cluster_name := flurm_dbd_ra, members := _, leader := _}, Info),
    ?assertEqual(running, maps:get(status, Info)),

    meck:expect(ra, members, fun(_) -> {error, noproc} end),
    ?assertEqual({error, noproc}, flurm_dbd_sup:ra_cluster_info()),

    meck:expect(ra, members, fun(_) -> erlang:error(bad_state) end),
    ?assertEqual({error, bad_state}, flurm_dbd_sup:ra_cluster_info()).

test_parse_address_branches() ->
    ?assertEqual({0, 0, 0, 0, 0, 0, 0, 0}, flurm_dbd_sup:parse_address("::")),
    ?assertEqual({127, 0, 0, 1}, flurm_dbd_sup:parse_address("127.0.0.1")),
    ?assertEqual({0, 0, 0, 0}, flurm_dbd_sup:parse_address("not-an-ip")),
    ?assertEqual({10, 0, 0, 1}, flurm_dbd_sup:parse_address({10, 0, 0, 1})).
