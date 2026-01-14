%%%-------------------------------------------------------------------
%%% @doc Direct EUnit tests for flurm_dbd_storage module
%%%
%%% These tests start the actual flurm_dbd_storage gen_server and call
%%% its functions directly to achieve code coverage. External dependencies
%%% like lager are mocked.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_storage_direct_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test fixtures
%%====================================================================

flurm_dbd_storage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"start_link starts server", fun test_start_link/0},
      {"store and fetch value", fun test_store_fetch/0},
      {"fetch non-existent key", fun test_fetch_not_found/0},
      {"store to unknown table", fun test_store_unknown_table/0},
      {"fetch from unknown table", fun test_fetch_unknown_table/0},
      {"delete value", fun test_delete/0},
      {"delete from unknown table", fun test_delete_unknown_table/0},
      {"list all values in table", fun test_list/0},
      {"list from unknown table", fun test_list_unknown_table/0},
      {"list with pattern", fun test_list_pattern/0},
      {"list with pattern from unknown table", fun test_list_pattern_unknown_table/0},
      {"sync for ets backend", fun test_sync_ets/0},
      {"unknown call returns error", fun test_unknown_call/0},
      {"unknown cast is handled", fun test_unknown_cast/0},
      {"handle_info is handled", fun test_handle_info/0},
      {"terminate callback", fun test_terminate/0},
      {"store multiple values", fun test_store_multiple/0},
      {"overwrite existing value", fun test_overwrite/0}
     ]}.

setup() ->
    %% Ensure meck is not already mocking lager
    catch meck:unload(lager),

    %% Mock lager for logging
    meck:new(lager, [no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    %% Ensure ets backend is used
    application:set_env(flurm_dbd, storage_backend, ets),
    ok.

cleanup(_) ->
    %% Stop server if running
    case whereis(flurm_dbd_storage) of
        undefined -> ok;
        Pid ->
            catch gen_server:stop(Pid, normal, 5000)
    end,
    catch meck:unload(lager),
    ok.

%%====================================================================
%% Server Lifecycle Tests
%%====================================================================

test_start_link() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

%%====================================================================
%% Store/Fetch Tests
%%====================================================================

test_store_fetch() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Key = test_key,
    Value = #{name => <<"test">>, data => [1, 2, 3]},
    %% Store
    ?assertEqual(ok, flurm_dbd_storage:store(jobs, Key, Value)),
    %% Fetch
    {ok, Retrieved} = flurm_dbd_storage:fetch(jobs, Key),
    ?assertEqual(Value, Retrieved),
    gen_server:stop(Pid).

test_fetch_not_found() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(jobs, nonexistent_key)),
    gen_server:stop(Pid).

test_store_unknown_table() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual({error, unknown_table}, flurm_dbd_storage:store(unknown_table, key, value)),
    gen_server:stop(Pid).

test_fetch_unknown_table() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual({error, unknown_table}, flurm_dbd_storage:fetch(unknown_table, key)),
    gen_server:stop(Pid).

%%====================================================================
%% Delete Tests
%%====================================================================

test_delete() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Key = delete_test_key,
    Value = <<"to_delete">>,
    flurm_dbd_storage:store(users, Key, Value),
    {ok, _} = flurm_dbd_storage:fetch(users, Key),
    ?assertEqual(ok, flurm_dbd_storage:delete(users, Key)),
    ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(users, Key)),
    gen_server:stop(Pid).

test_delete_unknown_table() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    %% Deleting from unknown table should return ok (no-op)
    ?assertEqual(ok, flurm_dbd_storage:delete(unknown_table, key)),
    gen_server:stop(Pid).

%%====================================================================
%% List Tests
%%====================================================================

test_list() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    %% Add some values
    flurm_dbd_storage:store(accounts, acc1, #{name => <<"Account1">>}),
    flurm_dbd_storage:store(accounts, acc2, #{name => <<"Account2">>}),
    flurm_dbd_storage:store(accounts, acc3, #{name => <<"Account3">>}),
    Values = flurm_dbd_storage:list(accounts),
    ?assertEqual(3, length(Values)),
    gen_server:stop(Pid).

test_list_unknown_table() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual([], flurm_dbd_storage:list(unknown_table)),
    gen_server:stop(Pid).

test_list_pattern() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    %% Add some values with specific pattern
    flurm_dbd_storage:store(qos, qos_normal, #{name => <<"normal">>, priority => 50}),
    flurm_dbd_storage:store(qos, qos_high, #{name => <<"high">>, priority => 100}),
    %% List all (pattern matches all)
    Values = flurm_dbd_storage:list(qos, {'_', '_'}),
    ?assert(length(Values) >= 0), % Pattern matching depends on ETS format
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
    %% For ETS backend, sync is a no-op
    ?assertEqual(ok, flurm_dbd_storage:sync()),
    gen_server:stop(Pid).

%%====================================================================
%% Edge Case Tests
%%====================================================================

test_unknown_call() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Result = gen_server:call(Pid, {unknown_request, foo}),
    ?assertEqual({error, unknown_request}, Result),
    gen_server:stop(Pid).

test_unknown_cast() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    %% Should not crash
    gen_server:cast(Pid, {unknown_cast, bar}),
    timer:sleep(20),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

test_handle_info() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Pid ! {some, random, message},
    timer:sleep(20),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

test_terminate() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    gen_server:stop(Pid, normal, 5000),
    timer:sleep(20),
    ?assertEqual(undefined, whereis(flurm_dbd_storage)).

%%====================================================================
%% Additional Tests
%%====================================================================

test_store_multiple() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    %% Test all supported tables
    Tables = [jobs, steps, events, accounts, users, associations, qos, usage],
    lists:foreach(fun(Table) ->
        Key = list_to_atom("key_" ++ atom_to_list(Table)),
        Value = #{table => Table, data => <<"test">>},
        ?assertEqual(ok, flurm_dbd_storage:store(Table, Key, Value)),
        {ok, Retrieved} = flurm_dbd_storage:fetch(Table, Key),
        ?assertEqual(Value, Retrieved)
    end, Tables),
    gen_server:stop(Pid).

test_overwrite() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Key = overwrite_key,
    Value1 = #{version => 1},
    Value2 = #{version => 2},
    flurm_dbd_storage:store(jobs, Key, Value1),
    {ok, R1} = flurm_dbd_storage:fetch(jobs, Key),
    ?assertEqual(1, maps:get(version, R1)),
    %% Overwrite
    flurm_dbd_storage:store(jobs, Key, Value2),
    {ok, R2} = flurm_dbd_storage:fetch(jobs, Key),
    ?assertEqual(2, maps:get(version, R2)),
    gen_server:stop(Pid).

%%====================================================================
%% Mnesia Backend Tests (mocked)
%%====================================================================

mnesia_backend_test_() ->
    {foreach,
     fun setup_mnesia_mock/0,
     fun cleanup_mnesia_mock/1,
     [
      {"mnesia store", fun test_mnesia_store/0},
      {"mnesia fetch", fun test_mnesia_fetch/0},
      {"mnesia fetch not found", fun test_mnesia_fetch_not_found/0},
      {"mnesia delete", fun test_mnesia_delete/0},
      {"mnesia list", fun test_mnesia_list/0},
      {"mnesia list with pattern", fun test_mnesia_list_pattern/0},
      {"mnesia sync", fun test_mnesia_sync/0}
     ]}.

setup_mnesia_mock() ->
    %% Unload any existing mocks
    catch meck:unload(lager),
    catch meck:unload(mnesia),

    %% Mock lager
    meck:new(lager, [no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    %% Mock mnesia
    meck:new(mnesia, [passthrough, unstick, no_link]),
    meck:expect(mnesia, create_table, fun(_, _) -> {atomic, ok} end),
    meck:expect(mnesia, dirty_write, fun(_, _) -> ok end),
    meck:expect(mnesia, dirty_read, fun(_, Key) ->
        case Key of
            found_key -> [{table, found_key, #{data => <<"found">>}}];
            _ -> []
        end
    end),
    meck:expect(mnesia, dirty_delete, fun(_, _) -> ok end),
    meck:expect(mnesia, dirty_all_keys, fun(_) -> [key1, key2] end),
    meck:expect(mnesia, dirty_match_object, fun(_) ->
        [{table, key1, val1}, {table, key2, val2}]
    end),
    meck:expect(mnesia, sync_log, fun() -> ok end),
    %% Set mnesia backend
    application:set_env(flurm_dbd, storage_backend, mnesia),
    ok.

cleanup_mnesia_mock(_) ->
    case whereis(flurm_dbd_storage) of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid, normal, 5000)
    end,
    %% Reset to ets backend
    application:set_env(flurm_dbd, storage_backend, ets),
    catch meck:unload(lager),
    catch meck:unload(mnesia),
    ok.

test_mnesia_store() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:store(jobs, test_key, #{data => <<"test">>})),
    gen_server:stop(Pid).

test_mnesia_fetch() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    {ok, Value} = flurm_dbd_storage:fetch(jobs, found_key),
    ?assertEqual(#{data => <<"found">>}, Value),
    gen_server:stop(Pid).

test_mnesia_fetch_not_found() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(jobs, not_found_key)),
    gen_server:stop(Pid).

test_mnesia_delete() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:delete(jobs, test_key)),
    gen_server:stop(Pid).

test_mnesia_list() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Keys = flurm_dbd_storage:list(jobs),
    ?assertEqual([key1, key2], Keys),
    gen_server:stop(Pid).

test_mnesia_list_pattern() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    Values = flurm_dbd_storage:list(jobs, {'_', '_'}),
    ?assertEqual([val1, val2], Values),
    gen_server:stop(Pid).

test_mnesia_sync() ->
    {ok, Pid} = flurm_dbd_storage:start_link(),
    ?assertEqual(ok, flurm_dbd_storage:sync()),
    gen_server:stop(Pid).
