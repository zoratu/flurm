%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_db_cluster module
%%%
%%% These tests call flurm_db_cluster functions directly to get code coverage.
%%% Ra is mocked to avoid needing a real cluster.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_cluster_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

server_id_test_() ->
    [
     {"Server ID for local node", fun server_id_local_test/0},
     {"Server ID for specific node", fun server_id_specific_test/0}
    ].

format_status_test_() ->
    [
     {"Format status from map", fun format_status_map_test/0},
     {"Format status from proplist", fun format_status_proplist_test/0}
    ].

cluster_operations_test_() ->
    {setup,
     fun setup_ra_mock/0,
     fun cleanup_ra_mock/1,
     [
      {"Start cluster - success", fun start_cluster_success_test/0},
      {"Start cluster - partial", fun start_cluster_partial_test/0},
      {"Start cluster - error", fun start_cluster_error_test/0},
      {"Join cluster - success", fun join_cluster_success_test/0},
      {"Join cluster - already member", fun join_cluster_already_member_test/0},
      {"Join cluster - server already started", fun join_cluster_server_started_test/0},
      {"Join cluster - timeout", fun join_cluster_timeout_test/0},
      {"Leave cluster - as follower", fun leave_cluster_follower_test/0},
      {"Leave cluster - as leader with others", fun leave_cluster_leader_test/0},
      {"Leave cluster - as only member", fun leave_cluster_only_member_test/0},
      {"Get leader - success", fun get_leader_success_test/0},
      {"Get leader - no leader", fun get_leader_no_leader_test/0},
      {"Get leader - timeout", fun get_leader_timeout_test/0},
      {"Get leader - error", fun get_leader_error_test/0},
      {"Get members - success", fun get_members_success_test/0},
      {"Get members - timeout", fun get_members_timeout_test/0},
      {"Get members - error", fun get_members_error_test/0},
      {"Is leader - true", fun is_leader_true_test/0},
      {"Is leader - false", fun is_leader_false_test/0},
      {"Status - success map", fun status_success_map_test/0},
      {"Status - error", fun status_error_test/0},
      {"Status - not started", fun status_not_started_test/0},
      {"Force election - success", fun force_election_success_test/0},
      {"Force election - error", fun force_election_error_test/0}
     ]}.

init_ra_test_() ->
    {setup,
     fun setup_for_init_ra/0,
     fun cleanup_for_init_ra/1,
     [
      {"Init Ra - success", fun init_ra_success_test/0}
     ]}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup_ra_mock() ->
    meck:new(ra, [passthrough]),
    meck:new(ra_system, [passthrough]),
    meck:new(lager, [passthrough]),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    ok.

cleanup_ra_mock(_) ->
    meck:unload(ra),
    meck:unload(ra_system),
    meck:unload(lager),
    ok.

setup_for_init_ra() ->
    meck:new(ra, [passthrough]),
    meck:new(ra_system, [passthrough]),
    meck:new(lager, [passthrough]),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    ok.

cleanup_for_init_ra(_) ->
    meck:unload(ra),
    meck:unload(ra_system),
    meck:unload(lager),
    ok.

%%====================================================================
%% Server ID Tests
%%====================================================================

server_id_local_test() ->
    ServerId = flurm_db_cluster:server_id(),
    ?assertEqual({flurm_db_ra, node()}, ServerId).

server_id_specific_test() ->
    ServerId = flurm_db_cluster:server_id('other@node'),
    ?assertEqual({flurm_db_ra, 'other@node'}, ServerId).

%%====================================================================
%% Format Status Tests
%%====================================================================

format_status_map_test() ->
    Overview = #{
        state => leader,
        leader => {flurm_db_ra, node()},
        current_term => 5,
        commit_index => 100,
        last_applied => 99,
        cluster => [{flurm_db_ra, node()}]
    },
    Status = flurm_db_cluster:format_status(Overview),

    ?assertEqual(leader, maps:get(state, Status)),
    ?assertEqual({flurm_db_ra, node()}, maps:get(leader, Status)),
    ?assertEqual(5, maps:get(current_term, Status)),
    ?assertEqual(100, maps:get(commit_index, Status)),
    ?assertEqual(99, maps:get(last_applied, Status)),
    ?assertEqual([{flurm_db_ra, node()}], maps:get(cluster_members, Status)).

format_status_proplist_test() ->
    Overview = [
        {state, follower},
        {leader, {flurm_db_ra, 'other@node'}},
        {current_term, 3}
    ],
    Status = flurm_db_cluster:format_status(Overview),

    ?assertEqual(follower, maps:get(state, Status)),
    ?assertEqual({flurm_db_ra, 'other@node'}, maps:get(leader, Status)),
    ?assertEqual(3, maps:get(current_term, Status)).

%%====================================================================
%% Cluster Operation Tests
%%====================================================================

start_cluster_success_test() ->
    meck:expect(ra, start_cluster, fun(_, _, _, _) ->
        {ok, [{flurm_db_ra, node()}], []}
    end),
    meck:expect(ra_system, fetch, fun(_) -> some_config end),

    ok = flurm_db_cluster:start_cluster([node()]).

start_cluster_partial_test() ->
    meck:expect(ra, start_cluster, fun(_, _, _, _) ->
        {ok, [{flurm_db_ra, node()}], [{flurm_db_ra, 'other@node'}]}
    end),
    meck:expect(ra_system, fetch, fun(_) -> some_config end),

    ok = flurm_db_cluster:start_cluster([node(), 'other@node']).

start_cluster_error_test() ->
    meck:expect(ra, start_cluster, fun(_, _, _, _) ->
        {error, some_reason}
    end),
    meck:expect(ra_system, fetch, fun(_) -> some_config end),

    {error, some_reason} = flurm_db_cluster:start_cluster([node()]).

join_cluster_success_test() ->
    meck:expect(ra, start_server, fun(_, _, _, _, _) -> ok end),
    meck:expect(ra, add_member, fun(_, _) ->
        {ok, [], {flurm_db_ra, 'other@node'}}
    end),
    meck:expect(ra_system, fetch, fun(_) -> some_config end),

    ok = flurm_db_cluster:join_cluster('other@node').

join_cluster_already_member_test() ->
    meck:expect(ra, start_server, fun(_, _, _, _, _) -> ok end),
    meck:expect(ra, add_member, fun(_, _) -> {error, already_member} end),
    meck:expect(ra_system, fetch, fun(_) -> some_config end),

    ok = flurm_db_cluster:join_cluster('other@node').

join_cluster_server_started_test() ->
    meck:expect(ra, start_server, fun(_, _, _, _, _) ->
        {error, {already_started, some_pid}}
    end),
    meck:expect(ra, add_member, fun(_, _) ->
        {ok, [], {flurm_db_ra, 'other@node'}}
    end),
    meck:expect(ra_system, fetch, fun(_) -> some_config end),

    ok = flurm_db_cluster:join_cluster('other@node').

join_cluster_timeout_test() ->
    meck:expect(ra, start_server, fun(_, _, _, _, _) -> ok end),
    meck:expect(ra, add_member, fun(_, _) -> {timeout, some_ref} end),
    meck:expect(ra, stop_server, fun(_, _) -> ok end),
    meck:expect(ra_system, fetch, fun(_) -> some_config end),

    {error, timeout} = flurm_db_cluster:join_cluster('other@node').

leave_cluster_follower_test() ->
    ServerId = {flurm_db_ra, node()},
    LeaderId = {flurm_db_ra, 'leader@node'},

    meck:expect(ra, members, fun(_) -> {ok, [ServerId, LeaderId], LeaderId} end),
    meck:expect(ra, remove_member, fun(_, _) -> {ok, [], LeaderId} end),
    meck:expect(ra, stop_server, fun(_, _) -> ok end),

    ok = flurm_db_cluster:leave_cluster().

leave_cluster_leader_test() ->
    ServerId = {flurm_db_ra, node()},
    OtherId = {flurm_db_ra, 'other@node'},

    %% First call: we are leader
    %% After transfer: we are not leader anymore
    meck:sequence(ra, members, 1, [
        {ok, [ServerId, OtherId], ServerId},
        {ok, [ServerId, OtherId], OtherId}
    ]),
    meck:expect(ra, transfer_leadership, fun(_, _) -> ok end),
    meck:expect(ra, remove_member, fun(_, _) -> {ok, [], OtherId} end),
    meck:expect(ra, stop_server, fun(_, _) -> ok end),

    ok = flurm_db_cluster:leave_cluster().

leave_cluster_only_member_test() ->
    ServerId = {flurm_db_ra, node()},

    meck:expect(ra, members, fun(_) -> {ok, [ServerId], ServerId} end),
    meck:expect(ra, stop_server, fun(_, _) -> ok end),

    ok = flurm_db_cluster:leave_cluster().

get_leader_success_test() ->
    LeaderId = {flurm_db_ra, node()},
    meck:expect(ra, members, fun(_) -> {ok, [LeaderId], LeaderId} end),

    {ok, LeaderId} = flurm_db_cluster:get_leader().

get_leader_no_leader_test() ->
    meck:expect(ra, members, fun(_) -> {ok, [{flurm_db_ra, node()}], undefined} end),

    {error, no_leader} = flurm_db_cluster:get_leader().

get_leader_timeout_test() ->
    meck:expect(ra, members, fun(_) -> {timeout, some_ref} end),

    {error, timeout} = flurm_db_cluster:get_leader().

get_leader_error_test() ->
    meck:expect(ra, members, fun(_) -> {error, some_reason} end),

    {error, some_reason} = flurm_db_cluster:get_leader().

get_members_success_test() ->
    Members = [{flurm_db_ra, node()}, {flurm_db_ra, 'other@node'}],
    meck:expect(ra, members, fun(_) -> {ok, Members, {flurm_db_ra, node()}} end),

    {ok, Members} = flurm_db_cluster:get_members().

get_members_timeout_test() ->
    meck:expect(ra, members, fun(_) -> {timeout, some_ref} end),

    {error, timeout} = flurm_db_cluster:get_members().

get_members_error_test() ->
    meck:expect(ra, members, fun(_) -> {error, not_available} end),

    {error, not_available} = flurm_db_cluster:get_members().

is_leader_true_test() ->
    ServerId = {flurm_db_ra, node()},
    meck:expect(ra, members, fun(_) -> {ok, [ServerId], ServerId} end),

    ?assertEqual(true, flurm_db_cluster:is_leader()).

is_leader_false_test() ->
    OtherId = {flurm_db_ra, 'other@node'},
    meck:expect(ra, members, fun(_) -> {ok, [{flurm_db_ra, node()}, OtherId], OtherId} end),

    ?assertEqual(false, flurm_db_cluster:is_leader()).

status_success_map_test() ->
    Overview = #{
        state => leader,
        leader => {flurm_db_ra, node()},
        current_term => 5
    },
    meck:expect(ra, member_overview, fun(_) -> {ok, Overview, undefined} end),

    {ok, Status} = flurm_db_cluster:status(),
    ?assertEqual(leader, maps:get(state, Status)).

status_error_test() ->
    meck:expect(ra, member_overview, fun(_) -> {error, some_reason} end),

    {error, some_reason} = flurm_db_cluster:status().

status_not_started_test() ->
    meck:expect(ra, member_overview, fun(_) -> error(badarg) end),

    {error, not_started} = flurm_db_cluster:status().

force_election_success_test() ->
    meck:expect(ra, trigger_election, fun(_) -> ok end),

    ok = flurm_db_cluster:force_election().

force_election_error_test() ->
    meck:expect(ra, trigger_election, fun(_) -> {error, not_leader} end),

    {error, not_leader} = flurm_db_cluster:force_election().

%%====================================================================
%% Init Ra Tests
%%====================================================================

init_ra_success_test() ->
    meck:expect(application, ensure_all_started, fun(ra) -> {ok, [ra]} end),
    meck:expect(ra_system, fetch, fun(_) -> some_config end),

    ok = flurm_db_cluster:init_ra().
