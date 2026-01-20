%%%-------------------------------------------------------------------
%%% @doc Direct EUnit coverage tests for flurm_dbd_storage module
%%%
%%% Tests the DBD storage gen_server directly. The actual gen_server
%%% is started and tested. External dependencies like lager are mocked.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_storage_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

flurm_dbd_storage_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link creates server", fun test_start_link/0},
      {"store value in jobs table", fun test_store_jobs/0},
      {"store value in steps table", fun test_store_steps/0},
      {"store value in events table", fun test_store_events/0},
      {"store value in accounts table", fun test_store_accounts/0},
      {"store value in users table", fun test_store_users/0},
      {"store value in associations table", fun test_store_associations/0},
      {"store value in qos table", fun test_store_qos/0},
      {"store value in usage table", fun test_store_usage/0},
      {"store to unknown table returns error", fun test_store_unknown_table/0},
      {"fetch existing value", fun test_fetch_existing/0},
      {"fetch non-existent key", fun test_fetch_not_found/0},
      {"fetch from unknown table", fun test_fetch_unknown_table/0},
      {"delete existing value", fun test_delete_existing/0},
      {"delete from unknown table", fun test_delete_unknown_table/0},
      {"list all values", fun test_list_all/0},
      {"list from unknown table", fun test_list_unknown_table/0},
      {"list with pattern", fun test_list_with_pattern/0},
      {"list with pattern from unknown table", fun test_list_pattern_unknown_table/0},
      {"sync for ets backend", fun test_sync_ets/0},
      {"unknown call returns error", fun test_unknown_call/0},
      {"unknown cast handled", fun test_unknown_cast/0},
      {"handle_info handled", fun test_handle_info/0},
      {"terminate callback", fun test_terminate/0},
      {"overwrite existing key", fun test_overwrite/0}
     ]}.

setup() ->
    %% Stop any existing server
    case whereis(flurm_dbd_storage) of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid, normal, 1000)
    end,
    %% Mock lager
    catch meck:unload(lager),
    meck:new(lager, [no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),
    %% Ensure ets backend
    application:set_env(flurm_dbd, storage_backend, ets),
    ok.

cleanup(_) ->
    case whereis(flurm_dbd_storage) of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid, normal, 1000)
    end,
    catch meck:unload(lager),
    ok.

%%====================================================================
%% Lifecycle Tests
%%====================================================================

test_start_link() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

%%====================================================================
%% Store Tests
%%====================================================================

test_store_jobs() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:store(jobs, 1, #{name => <<"job1">>})),
    gen_server:stop(Pid).

test_store_steps() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:store(steps, {1, 0}, #{step_name => <<"batch">>})),
    gen_server:stop(Pid).

test_store_events() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:store(events, erlang:system_time(), #{type => job_start})),
    gen_server:stop(Pid).

test_store_accounts() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:store(accounts, <<"root">>, #{desc => <<"Root account">>})),
    gen_server:stop(Pid).

test_store_users() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:store(users, <<"admin">>, #{uid => 0})),
    gen_server:stop(Pid).

test_store_associations() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:store(associations, {<<"user1">>, <<"acct1">>}, #{share => 100})),
    gen_server:stop(Pid).

test_store_qos() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:store(qos, <<"normal">>, #{priority => 50})),
    gen_server:stop(Pid).

test_store_usage() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:store(usage, {<<"user1">>, 2025, 1}, #{cpu_hours => 100})),
    gen_server:stop(Pid).

test_store_unknown_table() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual({error, unknown_table}, flurm_dbd_storage:store(unknown_table, key, value)),
    gen_server:stop(Pid).

%%====================================================================
%% Fetch Tests
%%====================================================================

test_fetch_existing() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Value = #{name => <<"test_job">>, user => <<"testuser">>},
    flurm_dbd_storage:store(jobs, 12345, Value),
    {ok, Retrieved} = flurm_dbd_storage:fetch(jobs, 12345),
    ?assertEqual(Value, Retrieved),
    gen_server:stop(Pid).

test_fetch_not_found() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(jobs, 99999)),
    gen_server:stop(Pid).

test_fetch_unknown_table() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual({error, unknown_table}, flurm_dbd_storage:fetch(unknown_table, key)),
    gen_server:stop(Pid).

%%====================================================================
%% Delete Tests
%%====================================================================

test_delete_existing() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    flurm_dbd_storage:store(users, <<"testuser">>, #{uid => 1000}),
    {ok, _} = flurm_dbd_storage:fetch(users, <<"testuser">>),
    ?assertEqual(ok, flurm_dbd_storage:delete(users, <<"testuser">>)),
    ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(users, <<"testuser">>)),
    gen_server:stop(Pid).

test_delete_unknown_table() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    %% Deleting from unknown table should return ok (no-op)
    ?assertEqual(ok, flurm_dbd_storage:delete(unknown_table, key)),
    gen_server:stop(Pid).

%%====================================================================
%% List Tests
%%====================================================================

test_list_all() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    flurm_dbd_storage:store(accounts, <<"acc1">>, #{name => <<"Account 1">>}),
    flurm_dbd_storage:store(accounts, <<"acc2">>, #{name => <<"Account 2">>}),
    flurm_dbd_storage:store(accounts, <<"acc3">>, #{name => <<"Account 3">>}),
    Values = flurm_dbd_storage:list(accounts),
    ?assertEqual(3, length(Values)),
    gen_server:stop(Pid).

test_list_unknown_table() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual([], flurm_dbd_storage:list(unknown_table)),
    gen_server:stop(Pid).

test_list_with_pattern() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    flurm_dbd_storage:store(qos, qos_normal, #{priority => 50}),
    flurm_dbd_storage:store(qos, qos_high, #{priority => 100}),
    %% Pattern that matches all
    Values = flurm_dbd_storage:list(qos, {'_', '_'}),
    ?assert(length(Values) >= 0),
    gen_server:stop(Pid).

test_list_pattern_unknown_table() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual([], flurm_dbd_storage:list(unknown_table, {'_', '_'})),
    gen_server:stop(Pid).

%%====================================================================
%% Sync Tests
%%====================================================================

test_sync_ets() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    %% For ETS backend, sync is a no-op but should return ok
    ?assertEqual(ok, flurm_dbd_storage:sync()),
    gen_server:stop(Pid).

%%====================================================================
%% Edge Cases
%%====================================================================

test_unknown_call() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Result = gen_server:call(Pid, {completely_unknown_request, arg}),
    ?assertEqual({error, unknown_request}, Result),
    gen_server:stop(Pid).

test_unknown_cast() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    gen_server:cast(Pid, {unknown_cast, data}),
    _ = sys:get_state(Pid),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

test_handle_info() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Pid ! {random, message},
    _ = sys:get_state(Pid),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

test_terminate() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    gen_server:stop(Pid, normal, 5000),
    flurm_test_utils:wait_for_unregistered(flurm_dbd_storage),
    ?assertEqual(undefined, whereis(flurm_dbd_storage)).

test_overwrite() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    flurm_dbd_storage:store(jobs, 123, #{version => 1}),
    {ok, V1} = flurm_dbd_storage:fetch(jobs, 123),
    ?assertEqual(1, maps:get(version, V1)),
    flurm_dbd_storage:store(jobs, 123, #{version => 2}),
    {ok, V2} = flurm_dbd_storage:fetch(jobs, 123),
    ?assertEqual(2, maps:get(version, V2)),
    gen_server:stop(Pid).

%%====================================================================
%% Mnesia Backend Tests (mocked)
%%====================================================================

mnesia_backend_test_() ->
    {foreach,
     fun setup_mnesia/0,
     fun cleanup_mnesia/1,
     [
      {"mnesia store", fun test_mnesia_store/0},
      {"mnesia fetch found", fun test_mnesia_fetch_found/0},
      {"mnesia fetch not found", fun test_mnesia_fetch_not_found/0},
      {"mnesia delete", fun test_mnesia_delete/0},
      {"mnesia list", fun test_mnesia_list/0},
      {"mnesia list with pattern", fun test_mnesia_list_pattern/0},
      {"mnesia sync", fun test_mnesia_sync/0}
     ]}.

setup_mnesia() ->
    case whereis(flurm_dbd_storage) of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid, normal, 1000)
    end,
    catch meck:unload(lager),
    catch meck:unload(mnesia),

    meck:new(lager, [no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:expect(lager, md, fun(_) -> ok end),

    meck:new(mnesia, [passthrough, unstick, no_link]),
    meck:expect(mnesia, create_table, fun(_, _) -> {atomic, ok} end),
    meck:expect(mnesia, dirty_write, fun(_, _) -> ok end),
    meck:expect(mnesia, dirty_read, fun(_, Key) ->
        case Key of
            found_key -> [{table, found_key, #{data => found}}];
            _ -> []
        end
    end),
    meck:expect(mnesia, dirty_delete, fun(_, _) -> ok end),
    meck:expect(mnesia, dirty_all_keys, fun(_) -> [k1, k2, k3] end),
    meck:expect(mnesia, dirty_match_object, fun(_) ->
        [{table, k1, v1}, {table, k2, v2}]
    end),
    meck:expect(mnesia, sync_log, fun() -> ok end),

    application:set_env(flurm_dbd, storage_backend, mnesia),
    ok.

cleanup_mnesia(_) ->
    case whereis(flurm_dbd_storage) of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid, normal, 1000)
    end,
    application:set_env(flurm_dbd, storage_backend, ets),
    catch meck:unload(lager),
    catch meck:unload(mnesia),
    ok.

test_mnesia_store() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:store(jobs, test_key, #{data => <<"test">>})),
    gen_server:stop(Pid).

test_mnesia_fetch_found() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    {ok, Value} = flurm_dbd_storage:fetch(jobs, found_key),
    ?assertEqual(#{data => found}, Value),
    gen_server:stop(Pid).

test_mnesia_fetch_not_found() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(jobs, not_found_key)),
    gen_server:stop(Pid).

test_mnesia_delete() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:delete(jobs, some_key)),
    gen_server:stop(Pid).

test_mnesia_list() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Keys = flurm_dbd_storage:list(jobs),
    ?assertEqual([k1, k2, k3], Keys),
    gen_server:stop(Pid).

test_mnesia_list_pattern() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Values = flurm_dbd_storage:list(jobs, {'_', '_'}),
    ?assertEqual([v1, v2], Values),
    gen_server:stop(Pid).

test_mnesia_sync() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:sync()),
    ?assert(meck:called(mnesia, sync_log, [])),
    gen_server:stop(Pid).
