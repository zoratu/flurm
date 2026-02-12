%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Storage Backend Tests
%%%
%%% Comprehensive tests for the flurm_dbd_storage module which provides
%%% persistence for accounting data using ETS backend.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_storage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing server
    case whereis(flurm_dbd_storage) of
        undefined -> ok;
        ExistingPid ->
            flurm_test_utils:kill_and_wait(ExistingPid)
    end,
    %% Set backend to ETS (default)
    application:set_env(flurm_dbd, storage_backend, ets),
    %% Start fresh server
    {ok, NewPid} = flurm_dbd_storage:start_link(),
    NewPid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true ->
            gen_server:stop(Pid, normal, 5000);
        false ->
            ok
    end,
    ok.

%%====================================================================
%% ETS Backend Tests
%%====================================================================

ets_backend_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"store and fetch value", fun test_store_fetch/0},
             {"store overwrites existing value", fun test_store_overwrite/0},
             {"delete value", fun test_delete/0},
             {"delete nonexistent key", fun test_delete_nonexistent/0},
             {"list values", fun test_list/0},
             {"list with pattern", fun test_list_pattern/0},
             {"sync returns ok", fun test_sync/0},
             {"fetch not found", fun test_fetch_not_found/0},
             {"store in unknown table", fun test_store_unknown_table/0},
             {"fetch from unknown table", fun test_fetch_unknown_table/0},
             {"store complex value types", fun test_store_complex_values/0},
             {"store with binary keys", fun test_binary_keys/0},
             {"store with tuple keys", fun test_tuple_keys/0},
             {"store with integer keys", fun test_integer_keys/0}
         ]
     end
    }.

test_store_fetch() ->
    %% Store a value
    Result1 = flurm_dbd_storage:store(jobs, key1, #{id => 1, name => <<"test">>}),
    ?assertEqual(ok, Result1),

    %% Fetch it back
    {ok, Value} = flurm_dbd_storage:fetch(jobs, key1),
    ?assertEqual(#{id => 1, name => <<"test">>}, Value).

test_store_overwrite() ->
    %% Store initial value
    ok = flurm_dbd_storage:store(jobs, overwrite_key, #{version => 1}),
    {ok, V1} = flurm_dbd_storage:fetch(jobs, overwrite_key),
    ?assertEqual(#{version => 1}, V1),

    %% Overwrite with new value
    ok = flurm_dbd_storage:store(jobs, overwrite_key, #{version => 2, extra => data}),
    {ok, V2} = flurm_dbd_storage:fetch(jobs, overwrite_key),
    ?assertEqual(#{version => 2, extra => data}, V2).

test_delete() ->
    %% Store a value
    ok = flurm_dbd_storage:store(jobs, key2, #{id => 2}),

    %% Verify it exists
    {ok, _} = flurm_dbd_storage:fetch(jobs, key2),

    %% Delete it
    Result = flurm_dbd_storage:delete(jobs, key2),
    ?assertEqual(ok, Result),

    %% Verify it's gone
    ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(jobs, key2)).

test_delete_nonexistent() ->
    %% Delete a key that doesn't exist should still return ok
    Result = flurm_dbd_storage:delete(jobs, nonexistent_key_12345),
    ?assertEqual(ok, Result).

test_list() ->
    %% Store multiple values
    ok = flurm_dbd_storage:store(users, user1, #{name => <<"alice">>}),
    ok = flurm_dbd_storage:store(users, user2, #{name => <<"bob">>}),

    %% List all values
    Values = flurm_dbd_storage:list(users),
    ?assert(is_list(Values)),
    ?assert(length(Values) >= 2),

    %% Verify values are in the list
    ?assert(lists:member(#{name => <<"alice">>}, Values)),
    ?assert(lists:member(#{name => <<"bob">>}, Values)).

test_list_pattern() ->
    %% Store values
    ok = flurm_dbd_storage:store(accounts, acc1, #{name => <<"account1">>}),
    ok = flurm_dbd_storage:store(accounts, acc2, #{name => <<"account2">>}),

    %% List with pattern (match all)
    Values = flurm_dbd_storage:list(accounts, {'_', '_'}),
    ?assert(is_list(Values)),

    %% List with specific key pattern
    Values2 = flurm_dbd_storage:list(accounts, {acc1, '_'}),
    ?assert(is_list(Values2)).

test_sync() ->
    Result = flurm_dbd_storage:sync(),
    ?assertEqual(ok, Result).

test_fetch_not_found() ->
    Result = flurm_dbd_storage:fetch(jobs, nonexistent_key),
    ?assertEqual({error, not_found}, Result).

test_store_unknown_table() ->
    Result = flurm_dbd_storage:store(unknown_table, key, value),
    ?assertEqual({error, unknown_table}, Result).

test_fetch_unknown_table() ->
    Result = flurm_dbd_storage:fetch(unknown_table, key),
    ?assertEqual({error, unknown_table}, Result).

test_store_complex_values() ->
    %% Store a complex nested map
    ComplexValue = #{
        id => 1,
        name => <<"complex_job">>,
        resources => #{cpu => 4, memory => 8192, gpu => 2},
        nodes => [<<"node1">>, <<"node2">>],
        metadata => #{
            submit_time => erlang:system_time(second),
            tags => [production, high_priority]
        }
    },
    ok = flurm_dbd_storage:store(jobs, complex_key, ComplexValue),
    {ok, Retrieved} = flurm_dbd_storage:fetch(jobs, complex_key),
    ?assertEqual(ComplexValue, Retrieved).

test_binary_keys() ->
    BinaryKey = <<"binary_key_12345">>,
    ok = flurm_dbd_storage:store(jobs, BinaryKey, #{data => test}),
    {ok, Value} = flurm_dbd_storage:fetch(jobs, BinaryKey),
    ?assertEqual(#{data => test}, Value),
    ok = flurm_dbd_storage:delete(jobs, BinaryKey),
    ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(jobs, BinaryKey)).

test_tuple_keys() ->
    TupleKey = {user, <<"alice">>, account, <<"default">>},
    ok = flurm_dbd_storage:store(associations, TupleKey, #{shares => 100}),
    {ok, Value} = flurm_dbd_storage:fetch(associations, TupleKey),
    ?assertEqual(#{shares => 100}, Value).

test_integer_keys() ->
    IntKey = 12345,
    ok = flurm_dbd_storage:store(jobs, IntKey, #{job_id => IntKey}),
    {ok, Value} = flurm_dbd_storage:fetch(jobs, IntKey),
    ?assertEqual(#{job_id => IntKey}, Value).

%%====================================================================
%% Gen Server Callback Tests
%%====================================================================

gen_server_callbacks_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"handle_call unknown returns error", fun() ->
                 Result = gen_server:call(flurm_dbd_storage, unknown_request),
                 ?assertEqual({error, unknown_request}, Result)
             end},
             {"handle_call with custom tuple returns error", fun() ->
                 Result = gen_server:call(flurm_dbd_storage, {custom, request, args}),
                 ?assertEqual({error, unknown_request}, Result)
             end},
             {"handle_cast does not crash", fun() ->
                 gen_server:cast(flurm_dbd_storage, some_message),
                 _ = sys:get_state(flurm_dbd_storage),
                 ?assert(is_process_alive(whereis(flurm_dbd_storage)))
             end},
             {"handle_cast with complex message does not crash", fun() ->
                 gen_server:cast(flurm_dbd_storage, {complex, #{data => test}, [1,2,3]}),
                 _ = sys:get_state(flurm_dbd_storage),
                 ?assert(is_process_alive(whereis(flurm_dbd_storage)))
             end},
             {"handle_info does not crash", fun() ->
                 whereis(flurm_dbd_storage) ! some_message,
                 _ = sys:get_state(flurm_dbd_storage),
                 ?assert(is_process_alive(whereis(flurm_dbd_storage)))
             end},
             {"handle_info with timeout message does not crash", fun() ->
                 whereis(flurm_dbd_storage) ! timeout,
                 _ = sys:get_state(flurm_dbd_storage),
                 ?assert(is_process_alive(whereis(flurm_dbd_storage)))
             end},
             {"handle_info with EXIT message does not crash", fun() ->
                 whereis(flurm_dbd_storage) ! {'EXIT', self(), normal},
                 _ = sys:get_state(flurm_dbd_storage),
                 ?assert(is_process_alive(whereis(flurm_dbd_storage)))
             end}
         ]
     end
    }.

%%====================================================================
%% All Table Operations Tests
%%====================================================================

table_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"jobs table operations", fun() ->
                 ok = flurm_dbd_storage:store(jobs, j1, #{id => 1}),
                 {ok, #{id := 1}} = flurm_dbd_storage:fetch(jobs, j1),
                 Values = flurm_dbd_storage:list(jobs),
                 ?assert(length(Values) >= 1),
                 ok = flurm_dbd_storage:delete(jobs, j1),
                 ?assertEqual({error, not_found}, flurm_dbd_storage:fetch(jobs, j1))
             end},
             {"steps table operations", fun() ->
                 ok = flurm_dbd_storage:store(steps, s1, #{step_id => 0}),
                 {ok, #{step_id := 0}} = flurm_dbd_storage:fetch(steps, s1),
                 ok = flurm_dbd_storage:store(steps, s2, #{step_id => 1}),
                 Values = flurm_dbd_storage:list(steps),
                 ?assert(length(Values) >= 2)
             end},
             {"events table operations", fun() ->
                 ok = flurm_dbd_storage:store(events, e1, #{type => submit, time => 100}),
                 {ok, #{type := submit}} = flurm_dbd_storage:fetch(events, e1),
                 ok = flurm_dbd_storage:store(events, e2, #{type => start, time => 200}),
                 ok = flurm_dbd_storage:store(events, e3, #{type => end_event, time => 300}),
                 Values = flurm_dbd_storage:list(events),
                 ?assert(length(Values) >= 3)
             end},
             {"accounts table operations", fun() ->
                 ok = flurm_dbd_storage:store(accounts, a1, #{name => <<"acc">>}),
                 {ok, #{name := <<"acc">>}} = flurm_dbd_storage:fetch(accounts, a1),
                 ok = flurm_dbd_storage:store(accounts, a2, #{name => <<"acc2">>, parent => <<"root">>}),
                 Values = flurm_dbd_storage:list(accounts),
                 ?assert(length(Values) >= 2)
             end},
             {"users table operations", fun() ->
                 ok = flurm_dbd_storage:store(users, u1, #{name => <<"user1">>, uid => 1000}),
                 {ok, #{name := <<"user1">>}} = flurm_dbd_storage:fetch(users, u1),
                 ok = flurm_dbd_storage:store(users, u2, #{name => <<"user2">>, uid => 1001}),
                 Values = flurm_dbd_storage:list(users),
                 ?assert(length(Values) >= 2)
             end},
             {"associations table operations", fun() ->
                 ok = flurm_dbd_storage:store(associations, as1, #{id => 1, user => <<"u1">>, account => <<"a1">>}),
                 {ok, #{id := 1}} = flurm_dbd_storage:fetch(associations, as1),
                 ok = flurm_dbd_storage:store(associations, as2, #{id => 2, user => <<"u2">>, account => <<"a1">>}),
                 Values = flurm_dbd_storage:list(associations),
                 ?assert(length(Values) >= 2)
             end},
             {"qos table operations", fun() ->
                 ok = flurm_dbd_storage:store(qos, q1, #{name => <<"normal">>, priority => 100}),
                 {ok, #{name := <<"normal">>}} = flurm_dbd_storage:fetch(qos, q1),
                 ok = flurm_dbd_storage:store(qos, q2, #{name => <<"high">>, priority => 200}),
                 ok = flurm_dbd_storage:store(qos, q3, #{name => <<"low">>, priority => 50}),
                 Values = flurm_dbd_storage:list(qos),
                 ?assert(length(Values) >= 3)
             end},
             {"usage table operations", fun() ->
                 ok = flurm_dbd_storage:store(usage, usage1, #{cpu_seconds => 1000, user => <<"u1">>}),
                 {ok, #{cpu_seconds := 1000}} = flurm_dbd_storage:fetch(usage, usage1),
                 ok = flurm_dbd_storage:store(usage, usage2, #{cpu_seconds => 2000, user => <<"u2">>}),
                 Values = flurm_dbd_storage:list(usage),
                 ?assert(length(Values) >= 2)
             end}
         ]
     end
    }.

%%====================================================================
%% Mnesia Backend Branch Tests
%%====================================================================

mnesia_backend_test_() ->
    {foreach,
     fun setup_mnesia_branches/0,
     fun cleanup_mnesia_branches/1,
     [
      {"init with mnesia backend", fun test_mnesia_init_branch/0},
      {"mnesia handle_call branches", fun test_mnesia_handle_call_branches/0},
      {"init_mnesia_tables handles already_exists", fun test_init_mnesia_already_exists/0},
      {"init_mnesia_tables handles create error", fun test_init_mnesia_error/0}
     ]}.

setup_mnesia_branches() ->
    catch meck:unload(mnesia),
    catch meck:unload(lager),
    meck:new(mnesia, [non_strict, no_link]),
    meck:new(lager, [non_strict, no_link, passthrough]),
    meck:expect(lager, md, fun() -> [] end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    ok.

cleanup_mnesia_branches(_) ->
    catch meck:unload(lager),
    catch meck:unload(mnesia),
    ok.

test_mnesia_init_branch() ->
    application:set_env(flurm_dbd, storage_backend, mnesia),
    meck:expect(mnesia, create_table, fun(_Name, _Opts) -> {atomic, ok} end),
    {ok, State} = flurm_dbd_storage:init([]),
    ?assertEqual(mnesia, element(2, State)).

test_mnesia_handle_call_branches() ->
    application:set_env(flurm_dbd, storage_backend, mnesia),
    meck:expect(mnesia, create_table, fun(_Name, _Opts) -> {atomic, ok} end),
    {ok, State} = flurm_dbd_storage:init([]),
    meck:expect(mnesia, dirty_write, fun(_T, _V) -> ok end),
    meck:expect(mnesia, dirty_read, fun(_T, _K) -> [{dbd_jobs, k1, v1}] end),
    meck:expect(mnesia, dirty_delete, fun(_T, _K) -> ok end),
    meck:expect(mnesia, dirty_all_keys, fun(_T) -> [k1, k2] end),
    meck:expect(mnesia, dirty_match_object, fun(_P) -> [{dbd_jobs, k1, v1}] end),
    meck:expect(mnesia, sync_log, fun() -> ok end),
    ?assertMatch({reply, ok, _},
                 flurm_dbd_storage:handle_call({store, dbd_jobs, k1, v1}, {self(), make_ref()}, State)),
    ?assertMatch({reply, {ok, v1}, _},
                 flurm_dbd_storage:handle_call({fetch, dbd_jobs, k1}, {self(), make_ref()}, State)),
    meck:expect(mnesia, dirty_read, fun(_T, _K) -> [] end),
    ?assertMatch({reply, {error, not_found}, _},
                 flurm_dbd_storage:handle_call({fetch, dbd_jobs, missing}, {self(), make_ref()}, State)),
    ?assertMatch({reply, ok, _},
                 flurm_dbd_storage:handle_call({delete, dbd_jobs, k1}, {self(), make_ref()}, State)),
    ?assertMatch({reply, [k1, k2], _},
                 flurm_dbd_storage:handle_call({list, dbd_jobs}, {self(), make_ref()}, State)),
    ?assertMatch({reply, [v1], _},
                 flurm_dbd_storage:handle_call({list, dbd_jobs, k1}, {self(), make_ref()}, State)),
    ?assertMatch({reply, ok, _},
                 flurm_dbd_storage:handle_call(sync, {self(), make_ref()}, State)).

test_init_mnesia_already_exists() ->
    application:set_env(flurm_dbd, storage_backend, mnesia),
    meck:expect(mnesia, create_table, fun(_Name, _Opts) -> {aborted, {already_exists, dbd_jobs}} end),
    {ok, _} = flurm_dbd_storage:init([]).

test_init_mnesia_error() ->
    application:set_env(flurm_dbd, storage_backend, mnesia),
    meck:expect(mnesia, create_table, fun(_Name, _Opts) -> {aborted, boom} end),
    {ok, _} = flurm_dbd_storage:init([]).

%%====================================================================
%% Start Link Tests
%%====================================================================

start_link_test_() ->
    [
        {"start_link creates server", fun() ->
             %% Cleanup existing
             case whereis(flurm_dbd_storage) of
                 undefined -> ok;
                 OldPid ->
                     flurm_test_utils:kill_and_wait(OldPid)
             end,
             application:set_env(flurm_dbd, storage_backend, ets),
             {ok, NewPid} = flurm_dbd_storage:start_link(),
             ?assert(is_pid(NewPid)),
             ?assert(is_process_alive(NewPid)),
             ?assertEqual(NewPid, whereis(flurm_dbd_storage)),
             gen_server:stop(NewPid, normal, 5000)
         end},
        {"start_link registers with correct name", fun() ->
             case whereis(flurm_dbd_storage) of
                 undefined -> ok;
                 OldPid ->
                     flurm_test_utils:kill_and_wait(OldPid)
             end,
             application:set_env(flurm_dbd, storage_backend, ets),
             {ok, Pid} = flurm_dbd_storage:start_link(),
             ?assertEqual(Pid, whereis(flurm_dbd_storage)),
             gen_server:stop(Pid, normal, 5000)
         end}
    ].

%%====================================================================
%% List Empty Table Tests
%%====================================================================

list_empty_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"list unknown table returns empty list", fun() ->
                 Values = flurm_dbd_storage:list(nonexistent_table),
                 ?assertEqual([], Values)
             end},
             {"list with pattern on unknown table returns empty list", fun() ->
                 Values = flurm_dbd_storage:list(nonexistent_table, {'_', '_'}),
                 ?assertEqual([], Values)
             end},
             {"list with complex pattern on unknown table returns empty list", fun() ->
                 Values = flurm_dbd_storage:list(nonexistent_table, {key, #{nested => '_'}}),
                 ?assertEqual([], Values)
             end}
         ]
     end
    }.

%%====================================================================
%% Delete from Unknown Table Tests
%%====================================================================

delete_unknown_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"delete from unknown table returns ok", fun() ->
                 Result = flurm_dbd_storage:delete(nonexistent_table, key),
                 ?assertEqual(ok, Result)
             end},
             {"delete with complex key from unknown table returns ok", fun() ->
                 Result = flurm_dbd_storage:delete(nonexistent_table, {complex, key, 123}),
                 ?assertEqual(ok, Result)
             end}
         ]
     end
    }.

%%====================================================================
%% Concurrent Access Tests
%%====================================================================

concurrent_access_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"concurrent stores do not crash", fun() ->
                 Parent = self(),
                 %% Spawn multiple processes to store concurrently
                 Pids = [spawn(fun() ->
                     Results = [flurm_dbd_storage:store(jobs, {concurrent, N, I}, #{n => N, i => I})
                                || I <- lists:seq(1, 10)],
                     Parent ! {done, self(), Results}
                 end) || N <- lists:seq(1, 5)],

                 %% Wait for all to complete
                 AllResults = [receive {done, Pid, R} -> R end || Pid <- Pids],
                 ?assert(lists:all(fun(Rs) -> lists:all(fun(R) -> R =:= ok end, Rs) end, AllResults))
             end},
             {"concurrent reads after writes", fun() ->
                 %% Store some values first
                 [ok = flurm_dbd_storage:store(jobs, {read_test, I}, #{value => I})
                  || I <- lists:seq(1, 20)],

                 Parent = self(),
                 %% Spawn readers
                 Pids = [spawn(fun() ->
                     Results = [flurm_dbd_storage:fetch(jobs, {read_test, I})
                                || I <- lists:seq(1, 20)],
                     Parent ! {done, self(), Results}
                 end) || _ <- lists:seq(1, 5)],

                 %% All should succeed
                 AllResults = [receive {done, Pid, R} -> R end || Pid <- Pids],
                 ?assert(lists:all(fun(Rs) ->
                     lists:all(fun({ok, _}) -> true; (_) -> false end, Rs)
                 end, AllResults))
             end}
         ]
     end
    }.

%%====================================================================
%% Edge Cases Tests
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
         [
             {"store empty map", fun() ->
                 ok = flurm_dbd_storage:store(jobs, empty_map_key, #{}),
                 {ok, Value} = flurm_dbd_storage:fetch(jobs, empty_map_key),
                 ?assertEqual(#{}, Value)
             end},
             {"store empty binary", fun() ->
                 ok = flurm_dbd_storage:store(jobs, empty_binary_key, <<>>),
                 {ok, Value} = flurm_dbd_storage:fetch(jobs, empty_binary_key),
                 ?assertEqual(<<>>, Value)
             end},
             {"store empty list", fun() ->
                 ok = flurm_dbd_storage:store(jobs, empty_list_key, []),
                 {ok, Value} = flurm_dbd_storage:fetch(jobs, empty_list_key),
                 ?assertEqual([], Value)
             end},
             {"store atom value", fun() ->
                 ok = flurm_dbd_storage:store(jobs, atom_key, some_atom),
                 {ok, Value} = flurm_dbd_storage:fetch(jobs, atom_key),
                 ?assertEqual(some_atom, Value)
             end},
             {"store pid value", fun() ->
                 ok = flurm_dbd_storage:store(jobs, pid_key, self()),
                 {ok, Value} = flurm_dbd_storage:fetch(jobs, pid_key),
                 ?assertEqual(self(), Value)
             end},
             {"store reference value", fun() ->
                 Ref = make_ref(),
                 ok = flurm_dbd_storage:store(jobs, ref_key, Ref),
                 {ok, Value} = flurm_dbd_storage:fetch(jobs, ref_key),
                 ?assertEqual(Ref, Value)
             end},
             {"store large binary", fun() ->
                 LargeBinary = list_to_binary(lists:duplicate(10000, $x)),
                 ok = flurm_dbd_storage:store(jobs, large_binary_key, LargeBinary),
                 {ok, Value} = flurm_dbd_storage:fetch(jobs, large_binary_key),
                 ?assertEqual(LargeBinary, Value)
             end}
         ]
     end
    }.

%%====================================================================
%% Terminate Callback Test
%%====================================================================

terminate_test_() ->
    [
        {"terminate callback executes cleanly", fun() ->
             case whereis(flurm_dbd_storage) of
                 undefined -> ok;
                 OldPid ->
                     flurm_test_utils:kill_and_wait(OldPid)
             end,
             application:set_env(flurm_dbd, storage_backend, ets),
             {ok, Pid} = flurm_dbd_storage:start_link(),
             ?assert(is_process_alive(Pid)),

             %% Store some data
             ok = flurm_dbd_storage:store(jobs, terminate_test, #{data => test}),

             %% Terminate and verify clean shutdown
             gen_server:stop(Pid, normal, 5000),
             flurm_test_utils:wait_for_death(Pid),
             ?assertNot(is_process_alive(Pid))
         end}
    ].
