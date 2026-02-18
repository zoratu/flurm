%%%-------------------------------------------------------------------
%%% @doc FLURM DB Cluster 100% Coverage Tests
%%%
%%% Comprehensive tests for flurm_db_cluster module covering:
%%% - Cluster startup and initialization
%%% - Cluster join and leave operations
%%% - Leader election and status queries
%%% - Error handling and edge cases
%%% - Internal helper functions
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_cluster_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup/teardown for meck-based tests
meck_setup() ->
    %% Unload any existing mocks first
    catch meck:unload(ra),
    catch meck:unload(ra_system),
    catch meck:unload(application),
    catch meck:unload(filelib),
    catch meck:unload(file),
    catch meck:unload(lager),
    catch meck:unload(net_adm),
    catch meck:unload(error_logger),
    ok.

meck_cleanup(_) ->
    meck:unload(),
    ok.

%%====================================================================
%% Server ID Tests
%%====================================================================

server_id_test_() ->
    {"Server ID generation tests",
     [
        {"server_id/0 returns tuple with current node",
         fun() ->
             Expected = {?RA_CLUSTER_NAME, node()},
             Result = flurm_db_cluster:server_id(),
             ?assertEqual(Expected, Result)
         end},
        {"server_id/1 returns tuple with specified node",
         fun() ->
             Expected = {?RA_CLUSTER_NAME, 'other@host'},
             Result = flurm_db_cluster:server_id('other@host'),
             ?assertEqual(Expected, Result)
         end},
        {"server_id/1 with different node names",
         fun() ->
             ?assertEqual({?RA_CLUSTER_NAME, foo@bar}, flurm_db_cluster:server_id(foo@bar)),
             ?assertEqual({?RA_CLUSTER_NAME, 'node1@localhost'}, flurm_db_cluster:server_id('node1@localhost')),
             ?assertEqual({?RA_CLUSTER_NAME, 'test@192.168.1.1'}, flurm_db_cluster:server_id('test@192.168.1.1'))
         end}
     ]}.

%%====================================================================
%% Format Status Tests
%%====================================================================

format_status_test_() ->
    {"Format status tests",
     [
        {"format_status with map overview",
         fun() ->
             Overview = #{
                 state => leader,
                 leader => {cluster, node()},
                 current_term => 5,
                 commit_index => 100,
                 last_applied => 99,
                 cluster => [{cluster, node()}]
             },
             Result = flurm_db_cluster:format_status(Overview),
             ?assertEqual(leader, maps:get(state, Result)),
             ?assertEqual({cluster, node()}, maps:get(leader, Result)),
             ?assertEqual(5, maps:get(current_term, Result)),
             ?assertEqual(100, maps:get(commit_index, Result)),
             ?assertEqual(99, maps:get(last_applied, Result)),
             ?assertEqual([{cluster, node()}], maps:get(cluster_members, Result))
         end},
        {"format_status with empty map",
         fun() ->
             Overview = #{},
             Result = flurm_db_cluster:format_status(Overview),
             ?assertEqual(unknown, maps:get(state, Result)),
             ?assertEqual(undefined, maps:get(leader, Result)),
             ?assertEqual(0, maps:get(current_term, Result)),
             ?assertEqual(0, maps:get(commit_index, Result)),
             ?assertEqual(0, maps:get(last_applied, Result)),
             ?assertEqual([], maps:get(cluster_members, Result))
         end},
        {"format_status with proplist overview",
         fun() ->
             Overview = [
                 {state, follower},
                 {leader, {cluster, 'other@node'}},
                 {current_term, 3}
             ],
             Result = flurm_db_cluster:format_status(Overview),
             ?assertEqual(follower, maps:get(state, Result)),
             ?assertEqual({cluster, 'other@node'}, maps:get(leader, Result)),
             ?assertEqual(3, maps:get(current_term, Result))
         end},
        {"format_status with empty proplist",
         fun() ->
             Overview = [],
             Result = flurm_db_cluster:format_status(Overview),
             ?assertEqual(unknown, maps:get(state, Result)),
             ?assertEqual(undefined, maps:get(leader, Result)),
             ?assertEqual(0, maps:get(current_term, Result))
         end}
     ]}.

%%====================================================================
%% Start Cluster Tests with Meck
%%====================================================================

start_cluster_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"start_cluster/0 calls start_cluster/1 with current node",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(application, get_env, fun(flurm_db, data_dir, _Default) -> "/tmp/test_data";
                                                  (ra, data_dir, _D) -> undefined;
                                                  (_App, _Key, Default) -> Default end),
             meck:expect(application, set_env, fun(_App, _Key, _Val) -> ok end),
             meck:expect(application, ensure_all_started, fun(ra) -> {ok, [ra]} end),
             meck:expect(filelib, ensure_dir, fun(_Path) -> ok end),
             meck:expect(ra_system, fetch, fun(default) -> undefined end),
             meck:expect(ra_system, start_default, fun() -> {ok, self()} end),
             meck:expect(lager, info, fun(_Fmt, _Args) -> ok end),
             meck:expect(lager, warning, fun(_Fmt, _Args) -> ok end),

             meck:expect(ra, start_cluster, fun(_System, _Name, _Machine, ServerIds) ->
                 ?assertEqual(1, length(ServerIds)),
                 {ok, ServerIds, []}
             end),

             Result = flurm_db_cluster:start_cluster(),
             ?assertEqual(ok, Result)
         end},
        {"start_cluster/1 success with all nodes started",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),
             meck:new(error_logger, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir, _) -> "/tmp/test";
                                                  (ra, data_dir, _D) -> undefined;
                                                  (_, _, D) -> D end),
             meck:expect(application, set_env, fun(_, _, _) -> ok end),
             meck:expect(application, ensure_all_started, fun(ra) -> {ok, [ra]} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(ra_system, fetch, fun(default) -> some_config end),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, warning, fun(_, _) -> ok end),
             meck:expect(error_logger, info_msg, fun(_, _) -> ok end),

             Nodes = [node(), 'node2@host', 'node3@host'],
             meck:expect(ra, start_cluster, fun(_System, _Name, _Machine, ServerIds) ->
                 ?assertEqual(3, length(ServerIds)),
                 {ok, ServerIds, []}
             end),

             Result = flurm_db_cluster:start_cluster(Nodes),
             ?assertEqual(ok, Result)
         end},
        {"start_cluster/1 success with partial failure",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),
             meck:new(error_logger, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir, _) -> "/tmp/test";
                                                  (ra, data_dir, _D) -> undefined;
                                                  (_, _, D) -> D end),
             meck:expect(application, set_env, fun(_, _, _) -> ok end),
             meck:expect(application, ensure_all_started, fun(ra) -> {ok, [ra]} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(ra_system, fetch, fun(default) -> some_config end),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, warning, fun(_, _) -> ok end),
             meck:expect(error_logger, info_msg, fun(_, _) -> ok end),
             meck:expect(error_logger, warning_msg, fun(_, _) -> ok end),

             Nodes = [node(), 'node2@host'],
             meck:expect(ra, start_cluster, fun(_System, _Name, _Machine, _ServerIds) ->
                 {ok, [{?RA_CLUSTER_NAME, node()}], [{?RA_CLUSTER_NAME, 'node2@host'}]}
             end),

             Result = flurm_db_cluster:start_cluster(Nodes),
             ?assertEqual(ok, Result)
         end},
        {"start_cluster/1 failure returns error",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),
             meck:new(error_logger, [passthrough, unstick]),

             meck:expect(application, get_env, fun(flurm_db, data_dir, _) -> "/tmp/test";
                                                  (ra, data_dir, _D) -> undefined;
                                                  (_, _, D) -> D end),
             meck:expect(application, set_env, fun(_, _, _) -> ok end),
             meck:expect(application, ensure_all_started, fun(ra) -> {ok, [ra]} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(ra_system, fetch, fun(default) -> some_config end),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, warning, fun(_, _) -> ok end),
             meck:expect(lager, error, fun(_, _) -> ok end),
             meck:expect(error_logger, error_msg, fun(_, _) -> ok end),

             meck:expect(ra, start_cluster, fun(_, _, _, _) -> {error, cluster_start_failed} end),

             Result = flurm_db_cluster:start_cluster([node()]),
             ?assertEqual({error, cluster_start_failed}, Result)
         end},
        {"start_cluster/1 fails when init_ra fails",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(application, get_env, fun(flurm_db, data_dir, _) -> "/nonexistent/path";
                                                  (_, _, D) -> D end),
             meck:expect(filelib, ensure_dir, fun(_) -> {error, enoent} end),
             meck:expect(lager, warning, fun(_, _) -> ok end),
             meck:expect(lager, error, fun(_, _) -> ok end),

             %% This should throw an error due to data_dir_error
             ?assertError({data_dir_error, _}, flurm_db_cluster:start_cluster([node()]))
         end}
     ]}.

%%====================================================================
%% Init Ra Tests
%%====================================================================

init_ra_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"init_ra with configured data_dir success",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(ra_system, [passthrough]),
             meck:new(lager, [passthrough]),

             meck:expect(application, get_env, fun(flurm_db, data_dir, _) -> "/tmp/flurm_test";
                                                  (ra, data_dir, _D) -> undefined;
                                                  (_, _, D) -> D end),
             meck:expect(application, set_env, fun(_, _, _) -> ok end),
             meck:expect(application, ensure_all_started, fun(ra) -> {ok, [ra]} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(ra_system, fetch, fun(default) -> some_config end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             Result = flurm_db_cluster:init_ra(),
             ?assertEqual(ok, Result)
         end},
        {"init_ra with ra already started",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(ra_system, [passthrough]),
             meck:new(lager, [passthrough]),

             meck:expect(application, get_env, fun(flurm_db, data_dir, _) -> "/tmp/test";
                                                  (ra, data_dir, _D) -> {ok, "/existing"};
                                                  (_, _, D) -> D end),
             meck:expect(application, set_env, fun(_, _, _) -> ok end),
             meck:expect(application, ensure_all_started, fun(ra) -> {error, {already_started, ra}} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(ra_system, fetch, fun(default) -> some_config end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             Result = flurm_db_cluster:init_ra(),
             ?assertEqual(ok, Result)
         end},
        {"init_ra with ra start failure",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(application, get_env, fun(flurm_db, data_dir, _) -> "/tmp/test";
                                                  (ra, data_dir, _D) -> undefined;
                                                  (_, _, D) -> D end),
             meck:expect(application, set_env, fun(_, _, _) -> ok end),
             meck:expect(application, ensure_all_started, fun(ra) -> {error, some_error} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             Result = flurm_db_cluster:init_ra(),
             ?assertEqual({error, some_error}, Result)
         end},
        {"init_ra fallback to temp directory",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(ra_system, [passthrough]),
             meck:new(lager, [passthrough]),

             %% First call fails, second succeeds (fallback to /tmp/flurm_db)
             meck:expect(application, get_env, fun(flurm_db, data_dir, _) -> "/nonexistent/path";
                                                  (ra, data_dir, _D) -> undefined;
                                                  (_, _, D) -> D end),
             meck:expect(application, set_env, fun(_, _, _) -> ok end),
             meck:expect(application, ensure_all_started, fun(ra) -> {ok, [ra]} end),
             meck:expect(filelib, ensure_dir, fun("/nonexistent/path/") -> {error, enoent};
                                                 ("/tmp/flurm_db/") -> ok;
                                                 (_) -> ok end),
             meck:expect(ra_system, fetch, fun(default) -> some_config end),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             Result = flurm_db_cluster:init_ra(),
             ?assertEqual(ok, Result)
         end}
     ]}.

%%====================================================================
%% Start Default System Tests
%%====================================================================

start_default_system_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"start_default_system when not running",
         fun() ->
             meck:new(ra_system, [passthrough]),
             meck:new(lager, [passthrough]),

             meck:expect(ra_system, fetch, fun(default) -> undefined end),
             meck:expect(ra_system, start_default, fun() -> {ok, self()} end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             Result = flurm_db_cluster:start_default_system(),
             ?assertEqual(ok, Result)
         end},
        {"start_default_system when already running",
         fun() ->
             meck:new(ra_system, [passthrough]),
             meck:new(lager, [passthrough]),

             meck:expect(ra_system, fetch, fun(default) -> some_config end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             Result = flurm_db_cluster:start_default_system(),
             ?assertEqual(ok, Result)
         end},
        {"start_default_system already_started error",
         fun() ->
             meck:new(ra_system, [passthrough]),
             meck:new(lager, [passthrough]),

             meck:expect(ra_system, fetch, fun(default) -> undefined end),
             meck:expect(ra_system, start_default, fun() -> {error, {already_started, self()}} end),
             meck:expect(lager, info, fun(_, _) -> ok end),

             Result = flurm_db_cluster:start_default_system(),
             ?assertEqual(ok, Result)
         end},
        {"start_default_system other error",
         fun() ->
             meck:new(ra_system, [passthrough]),
             meck:new(lager, [passthrough]),

             meck:expect(ra_system, fetch, fun(default) -> undefined end),
             meck:expect(ra_system, start_default, fun() -> {error, some_error} end),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, error, fun(_, _) -> ok end),

             Result = flurm_db_cluster:start_default_system(),
             ?assertEqual({error, some_error}, Result)
         end}
     ]}.

%%====================================================================
%% Join Cluster Tests
%%====================================================================

join_cluster_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"join_cluster success",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),
             meck:new(error_logger, [passthrough, unstick]),

             setup_init_ra_mocks(),

             meck:expect(ra, start_server, fun(_, _, _, _, _) -> ok end),
             meck:expect(ra, add_member, fun(_, _) -> {ok, [], {?RA_CLUSTER_NAME, 'existing@node'}} end),
             meck:expect(error_logger, info_msg, fun(_, _) -> ok end),

             Result = flurm_db_cluster:join_cluster('existing@node'),
             ?assertEqual(ok, Result)
         end},
        {"join_cluster already_member",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),
             meck:new(error_logger, [passthrough, unstick]),

             setup_init_ra_mocks(),

             meck:expect(ra, start_server, fun(_, _, _, _, _) -> ok end),
             meck:expect(ra, add_member, fun(_, _) -> {error, already_member} end),
             meck:expect(error_logger, info_msg, fun(_, _) -> ok end),

             Result = flurm_db_cluster:join_cluster('existing@node'),
             ?assertEqual(ok, Result)
         end},
        {"join_cluster timeout",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             setup_init_ra_mocks(),

             meck:expect(ra, start_server, fun(_, _, _, _, _) -> ok end),
             meck:expect(ra, add_member, fun(_, _) -> {timeout, some_state} end),
             meck:expect(ra, stop_server, fun(_, _) -> ok end),

             Result = flurm_db_cluster:join_cluster('existing@node'),
             ?assertEqual({error, timeout}, Result)
         end},
        {"join_cluster server already started, then add_member succeeds",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             setup_init_ra_mocks(),

             meck:expect(ra, start_server, fun(_, _, _, _, _) -> {error, {already_started, self()}} end),
             meck:expect(ra, add_member, fun(_, _) -> {ok, [], {?RA_CLUSTER_NAME, node()}} end),

             Result = flurm_db_cluster:join_cluster('existing@node'),
             ?assertEqual(ok, Result)
         end},
        {"join_cluster server already started, already_member",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             setup_init_ra_mocks(),

             meck:expect(ra, start_server, fun(_, _, _, _, _) -> {error, {already_started, self()}} end),
             meck:expect(ra, add_member, fun(_, _) -> {error, already_member} end),

             Result = flurm_db_cluster:join_cluster('existing@node'),
             ?assertEqual(ok, Result)
         end},
        {"join_cluster server start fails",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             setup_init_ra_mocks(),

             meck:expect(ra, start_server, fun(_, _, _, _, _) -> {error, server_start_failed} end),

             Result = flurm_db_cluster:join_cluster('existing@node'),
             ?assertEqual({error, server_start_failed}, Result)
         end},
        {"join_cluster add_member fails with error",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(ra_system, [passthrough]),
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             setup_init_ra_mocks(),

             meck:expect(ra, start_server, fun(_, _, _, _, _) -> ok end),
             meck:expect(ra, add_member, fun(_, _) -> {error, some_error} end),
             meck:expect(ra, stop_server, fun(_, _) -> ok end),

             Result = flurm_db_cluster:join_cluster('existing@node'),
             ?assertEqual({error, some_error}, Result)
         end},
        {"join_cluster init_ra fails",
         fun() ->
             meck:new(application, [passthrough, unstick]),
             meck:new(filelib, [passthrough, unstick]),
             meck:new(lager, [passthrough]),

             meck:expect(application, get_env, fun(flurm_db, data_dir, _) -> "/tmp/test";
                                                  (ra, data_dir, _D) -> undefined;
                                                  (_, _, D) -> D end),
             meck:expect(application, set_env, fun(_, _, _) -> ok end),
             meck:expect(application, ensure_all_started, fun(ra) -> {error, ra_failed} end),
             meck:expect(filelib, ensure_dir, fun(_) -> ok end),
             meck:expect(lager, info, fun(_, _) -> ok end),
             meck:expect(lager, warning, fun(_, _) -> ok end),

             Result = flurm_db_cluster:join_cluster('existing@node'),
             ?assertEqual({error, ra_failed}, Result)
         end}
     ]}.

%%====================================================================
%% Leave Cluster Tests
%%====================================================================

leave_cluster_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"leave_cluster success (not leader)",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(error_logger, [passthrough, unstick]),

             LocalServerId = {?RA_CLUSTER_NAME, node()},
             LeaderId = {?RA_CLUSTER_NAME, 'other@node'},

             meck:expect(ra, members, fun(ServerId) when ServerId =:= LocalServerId ->
                 {ok, [LocalServerId, LeaderId], LeaderId}
             end),
             meck:expect(ra, remove_member, fun(Leader, Member)
                 when Leader =:= LeaderId, Member =:= LocalServerId ->
                 {ok, [], LeaderId}
             end),
             meck:expect(ra, stop_server, fun(default, ServerId)
                 when ServerId =:= LocalServerId -> ok end),
             meck:expect(error_logger, info_msg, fun(_, _) -> ok end),

             Result = flurm_db_cluster:leave_cluster(),
             ?assertEqual(ok, Result)
         end},
        {"leave_cluster as leader with multiple members",
         fun() ->
             meck:new(ra, [passthrough]),
             meck:new(timer, [passthrough, unstick]),
             meck:new(error_logger, [passthrough, unstick]),

             LocalServerId = {?RA_CLUSTER_NAME, node()},
             OtherId = {?RA_CLUSTER_NAME, 'other@node'},

             %% First call: we are leader
             %% After transfer and sleep, we are no longer leader
             meck:expect(ra, members, fun(ServerId) when ServerId =:= LocalServerId ->
                 {ok, [LocalServerId, OtherId], OtherId}  %% Now other is leader
             end),
             meck:expect(ra, transfer_leadership, fun(_, _) -> ok end),
             meck:expect(ra, remove_member, fun(_, _) -> {ok, [], OtherId} end),
             meck:expect(ra, stop_server, fun(default, _) -> ok end),
             meck:expect(timer, sleep, fun(_) -> ok end),
             meck:expect(error_logger, info_msg, fun(_, _) -> ok end),

             Result = flurm_db_cluster:leave_cluster(),
             ?assertEqual(ok, Result)
         end},
        {"leave_cluster as only member",
         fun() ->
             meck:new(ra, [passthrough]),

             LocalServerId = {?RA_CLUSTER_NAME, node()},

             meck:expect(ra, members, fun(ServerId) when ServerId =:= LocalServerId ->
                 {ok, [LocalServerId], LocalServerId}  %% We are leader and only member
             end),
             meck:expect(ra, stop_server, fun(default, _) -> ok end),

             Result = flurm_db_cluster:leave_cluster(),
             ?assertEqual(ok, Result)
         end},
        {"leave_cluster remove_member timeout",
         fun() ->
             meck:new(ra, [passthrough]),

             LocalServerId = {?RA_CLUSTER_NAME, node()},
             LeaderId = {?RA_CLUSTER_NAME, 'other@node'},

             meck:expect(ra, members, fun(_) -> {ok, [LocalServerId, LeaderId], LeaderId} end),
             meck:expect(ra, remove_member, fun(_, _) -> {timeout, some_state} end),

             Result = flurm_db_cluster:leave_cluster(),
             ?assertEqual({error, timeout}, Result)
         end},
        {"leave_cluster get_leader fails",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             Result = flurm_db_cluster:leave_cluster(),
             ?assertEqual({error, noproc}, Result)
         end},
        {"leave_cluster transfer_leadership fails",
         fun() ->
             meck:new(ra, [passthrough]),

             LocalServerId = {?RA_CLUSTER_NAME, node()},
             OtherId = {?RA_CLUSTER_NAME, 'other@node'},

             meck:expect(ra, members, fun(_) ->
                 {ok, [LocalServerId, OtherId], LocalServerId}  %% We are leader
             end),
             meck:expect(ra, transfer_leadership, fun(_, _) -> {error, transfer_failed} end),

             Result = flurm_db_cluster:leave_cluster(),
             ?assertEqual({error, transfer_failed}, Result)
         end},
        {"leave_cluster transfer_leadership already_leader",
         fun() ->
             meck:new(ra, [passthrough]),

             LocalServerId = {?RA_CLUSTER_NAME, node()},
             OtherId = {?RA_CLUSTER_NAME, 'other@node'},

             meck:expect(ra, members, fun(_) ->
                 {ok, [LocalServerId, OtherId], LocalServerId}
             end),
             meck:expect(ra, transfer_leadership, fun(_, _) -> already_leader end),

             Result = flurm_db_cluster:leave_cluster(),
             ?assertEqual({error, cannot_transfer_leadership}, Result)
         end},
        {"leave_cluster get_members fails when leader",
         fun() ->
             meck:new(ra, [passthrough]),

             LocalServerId = {?RA_CLUSTER_NAME, node()},

             %% First call to get_leader returns us as leader
             %% Call to get_members fails
             CallCount = ets:new(call_count, [public]),
             ets:insert(CallCount, {count, 0}),

             meck:expect(ra, members, fun(_) ->
                 [{count, C}] = ets:lookup(CallCount, count),
                 ets:insert(CallCount, {count, C + 1}),
                 case C of
                     0 -> {ok, [LocalServerId], LocalServerId};  %% get_leader
                     _ -> {error, member_query_failed}  %% get_members
                 end
             end),

             Result = flurm_db_cluster:leave_cluster(),
             ets:delete(CallCount),
             ?assertEqual({error, member_query_failed}, Result)
         end}
     ]}.

%%====================================================================
%% Get Leader Tests
%%====================================================================

get_leader_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"get_leader success",
         fun() ->
             meck:new(ra, [passthrough]),

             LeaderId = {?RA_CLUSTER_NAME, 'leader@node'},
             meck:expect(ra, members, fun(_) -> {ok, [LeaderId], LeaderId} end),

             Result = flurm_db_cluster:get_leader(),
             ?assertEqual({ok, LeaderId}, Result)
         end},
        {"get_leader no leader",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, members, fun(_) -> {ok, [], undefined} end),

             Result = flurm_db_cluster:get_leader(),
             ?assertEqual({error, no_leader}, Result)
         end},
        {"get_leader timeout",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, members, fun(_) -> {timeout, some_state} end),

             Result = flurm_db_cluster:get_leader(),
             ?assertEqual({error, timeout}, Result)
         end},
        {"get_leader error",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             Result = flurm_db_cluster:get_leader(),
             ?assertEqual({error, noproc}, Result)
         end}
     ]}.

%%====================================================================
%% Get Members Tests
%%====================================================================

get_members_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"get_members success",
         fun() ->
             meck:new(ra, [passthrough]),

             Members = [{?RA_CLUSTER_NAME, node()}, {?RA_CLUSTER_NAME, 'other@node'}],
             meck:expect(ra, members, fun(_) -> {ok, Members, {?RA_CLUSTER_NAME, node()}} end),

             Result = flurm_db_cluster:get_members(),
             ?assertEqual({ok, Members}, Result)
         end},
        {"get_members timeout",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, members, fun(_) -> {timeout, some_state} end),

             Result = flurm_db_cluster:get_members(),
             ?assertEqual({error, timeout}, Result)
         end},
        {"get_members error",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, members, fun(_) -> {error, not_found} end),

             Result = flurm_db_cluster:get_members(),
             ?assertEqual({error, not_found}, Result)
         end}
     ]}.

%%====================================================================
%% Is Leader Tests
%%====================================================================

is_leader_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"is_leader returns true when leader",
         fun() ->
             meck:new(ra, [passthrough]),

             LocalServerId = {?RA_CLUSTER_NAME, node()},
             meck:expect(ra, members, fun(_) -> {ok, [LocalServerId], LocalServerId} end),

             Result = flurm_db_cluster:is_leader(),
             ?assertEqual(true, Result)
         end},
        {"is_leader returns false when not leader",
         fun() ->
             meck:new(ra, [passthrough]),

             LocalServerId = {?RA_CLUSTER_NAME, node()},
             OtherId = {?RA_CLUSTER_NAME, 'other@node'},
             meck:expect(ra, members, fun(_) -> {ok, [LocalServerId, OtherId], OtherId} end),

             Result = flurm_db_cluster:is_leader(),
             ?assertEqual(false, Result)
         end},
        {"is_leader returns false on error",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, members, fun(_) -> {error, noproc} end),

             Result = flurm_db_cluster:is_leader(),
             ?assertEqual(false, Result)
         end}
     ]}.

%%====================================================================
%% Status Tests
%%====================================================================

status_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"status success",
         fun() ->
             meck:new(ra, [passthrough]),

             Overview = #{
                 state => leader,
                 leader => {?RA_CLUSTER_NAME, node()},
                 current_term => 5
             },
             meck:expect(ra, member_overview, fun(_) -> {ok, Overview, node()} end),

             Result = flurm_db_cluster:status(),
             ?assertMatch({ok, _}, Result),
             {ok, Status} = Result,
             ?assertEqual(leader, maps:get(state, Status))
         end},
        {"status error",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, member_overview, fun(_) -> {error, noproc} end),

             Result = flurm_db_cluster:status(),
             ?assertEqual({error, noproc}, Result)
         end},
        {"status exception returns not_started",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, member_overview, fun(_) -> error(badarg) end),

             Result = flurm_db_cluster:status(),
             ?assertEqual({error, not_started}, Result)
         end}
     ]}.

%%====================================================================
%% Force Election Tests
%%====================================================================

force_election_test_() ->
    {foreach,
     fun meck_setup/0,
     fun meck_cleanup/1,
     [
        {"force_election success",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, trigger_election, fun(_) -> ok end),

             Result = flurm_db_cluster:force_election(),
             ?assertEqual(ok, Result)
         end},
        {"force_election error",
         fun() ->
             meck:new(ra, [passthrough]),

             meck:expect(ra, trigger_election, fun(_) -> {error, not_leader} end),

             Result = flurm_db_cluster:force_election(),
             ?assertEqual({error, not_leader}, Result)
         end}
     ]}.

%%====================================================================
%% Helper Functions
%%====================================================================

setup_init_ra_mocks() ->
    meck:expect(application, get_env, fun(flurm_db, data_dir, _) -> "/tmp/test";
                                         (ra, data_dir, _D) -> {ok, "/tmp/test"};
                                         (_, _, D) -> D end),
    meck:expect(application, set_env, fun(_, _, _) -> ok end),
    meck:expect(application, ensure_all_started, fun(ra) -> {ok, [ra]} end),
    meck:expect(filelib, ensure_dir, fun(_) -> ok end),
    meck:expect(ra_system, fetch, fun(default) -> some_config end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    ok.
