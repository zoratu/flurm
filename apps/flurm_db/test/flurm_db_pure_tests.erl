%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_db module
%%%
%%% Tests the legacy ETS-based API functions directly without mocking.
%%% These tests cover the init/0, put/3, get/2, delete/2, list/1, and
%%% list_keys/1 functions.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_db/include/flurm_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup and teardown for tests that need clean ETS tables
setup() ->
    %% Clean up any existing tables
    cleanup_tables(),
    ok.

cleanup() ->
    cleanup_tables(),
    ok.

cleanup_tables() ->
    Tables = [flurm_db_jobs, flurm_db_nodes, flurm_db_partitions,
              flurm_db_test_table, flurm_db_custom],
    lists:foreach(fun(T) ->
        case ets:whereis(T) of
            undefined -> ok;
            _ -> catch ets:delete(T)
        end
    end, Tables).

%%====================================================================
%% init/0 Tests
%%====================================================================

init_test_() ->
    {setup,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"init creates standard tables",
       fun() ->
           ?assertEqual(ok, flurm_db:init()),
           %% Verify tables exist
           ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs)),
           ?assertNotEqual(undefined, ets:whereis(flurm_db_nodes)),
           ?assertNotEqual(undefined, ets:whereis(flurm_db_partitions))
       end},

      {"init is idempotent",
       fun() ->
           ?assertEqual(ok, flurm_db:init()),
           ?assertEqual(ok, flurm_db:init()),
           %% Tables should still exist
           ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs))
       end}
     ]}.

%%====================================================================
%% put/3 Tests
%%====================================================================

put_test_() ->
    {setup,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"put stores a value in jobs table",
       fun() ->
           ?assertEqual(ok, flurm_db:put(jobs, 1, #{name => <<"test">>})),
           %% Verify it's stored
           [{1, Value}] = ets:lookup(flurm_db_jobs, 1),
           ?assertEqual(#{name => <<"test">>}, Value)
       end},

      {"put stores a value in nodes table",
       fun() ->
           ?assertEqual(ok, flurm_db:put(nodes, <<"node1">>, #{cpus => 8})),
           [{<<"node1">>, Value}] = ets:lookup(flurm_db_nodes, <<"node1">>),
           ?assertEqual(#{cpus => 8}, Value)
       end},

      {"put stores a value in partitions table",
       fun() ->
           ?assertEqual(ok, flurm_db:put(partitions, <<"default">>, #{state => up})),
           [{<<"default">>, Value}] = ets:lookup(flurm_db_partitions, <<"default">>),
           ?assertEqual(#{state => up}, Value)
       end},

      {"put creates custom table if not exists",
       fun() ->
           ?assertEqual(ok, flurm_db:put(custom, key1, value1)),
           ?assertNotEqual(undefined, ets:whereis(flurm_db_custom)),
           [{key1, value1}] = ets:lookup(flurm_db_custom, key1)
       end},

      {"put overwrites existing value",
       fun() ->
           ?assertEqual(ok, flurm_db:put(jobs, 100, first)),
           ?assertEqual(ok, flurm_db:put(jobs, 100, second)),
           [{100, second}] = ets:lookup(flurm_db_jobs, 100)
       end},

      {"put handles various key types",
       fun() ->
           ?assertEqual(ok, flurm_db:put(jobs, 1, value1)),
           ?assertEqual(ok, flurm_db:put(jobs, atom_key, value2)),
           ?assertEqual(ok, flurm_db:put(jobs, <<"binary_key">>, value3)),
           ?assertEqual(ok, flurm_db:put(jobs, {tuple, key}, value4)),

           [{1, value1}] = ets:lookup(flurm_db_jobs, 1),
           [{atom_key, value2}] = ets:lookup(flurm_db_jobs, atom_key),
           [{<<"binary_key">>, value3}] = ets:lookup(flurm_db_jobs, <<"binary_key">>),
           [{{tuple, key}, value4}] = ets:lookup(flurm_db_jobs, {tuple, key})
       end},

      {"put handles various value types",
       fun() ->
           ?assertEqual(ok, flurm_db:put(jobs, k1, 42)),
           ?assertEqual(ok, flurm_db:put(jobs, k2, <<"binary">>)),
           ?assertEqual(ok, flurm_db:put(jobs, k3, [1,2,3])),
           ?assertEqual(ok, flurm_db:put(jobs, k4, #{map => key})),
           ?assertEqual(ok, flurm_db:put(jobs, k5, {tuple, value})),

           [{k1, 42}] = ets:lookup(flurm_db_jobs, k1),
           [{k2, <<"binary">>}] = ets:lookup(flurm_db_jobs, k2),
           [{k3, [1,2,3]}] = ets:lookup(flurm_db_jobs, k3),
           [{k4, #{map := key}}] = ets:lookup(flurm_db_jobs, k4),
           [{k5, {tuple, value}}] = ets:lookup(flurm_db_jobs, k5)
       end}
     ]}.

%%====================================================================
%% get/2 Tests
%%====================================================================

get_test_() ->
    {setup,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"get retrieves existing value",
       fun() ->
           flurm_db:put(jobs, 1, #{name => <<"job1">>}),
           ?assertEqual({ok, #{name => <<"job1">>}}, flurm_db:get(jobs, 1))
       end},

      {"get returns not_found for missing key",
       fun() ->
           flurm_db:init(),
           ?assertEqual({error, not_found}, flurm_db:get(jobs, 999))
       end},

      {"get retrieves from nodes table",
       fun() ->
           flurm_db:put(nodes, <<"node1">>, #{hostname => <<"host1">>}),
           ?assertEqual({ok, #{hostname => <<"host1">>}}, flurm_db:get(nodes, <<"node1">>))
       end},

      {"get retrieves from partitions table",
       fun() ->
           flurm_db:put(partitions, <<"part1">>, #{max_time => 3600}),
           ?assertEqual({ok, #{max_time => 3600}}, flurm_db:get(partitions, <<"part1">>))
       end},

      {"get creates table if not exists and returns not_found",
       fun() ->
           cleanup_tables(),
           ?assertEqual({error, not_found}, flurm_db:get(jobs, 1)),
           ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs))
       end},

      {"get handles complex keys",
       fun() ->
           flurm_db:put(jobs, {complex, key, 123}, complex_value),
           ?assertEqual({ok, complex_value}, flurm_db:get(jobs, {complex, key, 123}))
       end}
     ]}.

%%====================================================================
%% delete/2 Tests
%%====================================================================

delete_test_() ->
    {setup,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"delete removes existing key",
       fun() ->
           flurm_db:put(jobs, 1, value1),
           ?assertEqual({ok, value1}, flurm_db:get(jobs, 1)),
           ?assertEqual(ok, flurm_db:delete(jobs, 1)),
           ?assertEqual({error, not_found}, flurm_db:get(jobs, 1))
       end},

      {"delete is idempotent for missing key",
       fun() ->
           flurm_db:init(),
           ?assertEqual(ok, flurm_db:delete(jobs, 999)),
           ?assertEqual(ok, flurm_db:delete(jobs, 999))
       end},

      {"delete from nodes table",
       fun() ->
           flurm_db:put(nodes, <<"node1">>, node_data),
           ?assertEqual(ok, flurm_db:delete(nodes, <<"node1">>)),
           ?assertEqual({error, not_found}, flurm_db:get(nodes, <<"node1">>))
       end},

      {"delete from partitions table",
       fun() ->
           flurm_db:put(partitions, <<"part1">>, part_data),
           ?assertEqual(ok, flurm_db:delete(partitions, <<"part1">>)),
           ?assertEqual({error, not_found}, flurm_db:get(partitions, <<"part1">>))
       end},

      {"delete only removes specified key",
       fun() ->
           flurm_db:put(jobs, 1, value1),
           flurm_db:put(jobs, 2, value2),
           flurm_db:put(jobs, 3, value3),
           ?assertEqual(ok, flurm_db:delete(jobs, 2)),
           ?assertEqual({ok, value1}, flurm_db:get(jobs, 1)),
           ?assertEqual({error, not_found}, flurm_db:get(jobs, 2)),
           ?assertEqual({ok, value3}, flurm_db:get(jobs, 3))
       end}
     ]}.

%%====================================================================
%% list/1 Tests
%%====================================================================

list_test_() ->
    {setup,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"list returns empty list for empty table",
       fun() ->
           flurm_db:init(),
           ?assertEqual([], flurm_db:list(jobs))
       end},

      {"list returns all values",
       fun() ->
           flurm_db:put(jobs, 1, value1),
           flurm_db:put(jobs, 2, value2),
           flurm_db:put(jobs, 3, value3),
           Result = flurm_db:list(jobs),
           ?assertEqual(3, length(Result)),
           ?assert(lists:member(value1, Result)),
           ?assert(lists:member(value2, Result)),
           ?assert(lists:member(value3, Result))
       end},

      {"list works with nodes table",
       fun() ->
           flurm_db:put(nodes, <<"n1">>, node1_data),
           flurm_db:put(nodes, <<"n2">>, node2_data),
           Result = flurm_db:list(nodes),
           ?assertEqual(2, length(Result)),
           ?assert(lists:member(node1_data, Result)),
           ?assert(lists:member(node2_data, Result))
       end},

      {"list works with partitions table",
       fun() ->
           flurm_db:put(partitions, <<"p1">>, part1_data),
           Result = flurm_db:list(partitions),
           ?assertEqual([part1_data], Result)
       end},

      {"list creates table if not exists",
       fun() ->
           cleanup_tables(),
           ?assertEqual([], flurm_db:list(jobs)),
           ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs))
       end},

      {"list handles complex values",
       fun() ->
           flurm_db:put(jobs, 1, #{id => 1, name => <<"job1">>}),
           flurm_db:put(jobs, 2, #{id => 2, name => <<"job2">>}),
           Result = flurm_db:list(jobs),
           ?assertEqual(2, length(Result)),
           ?assert(lists:any(fun(M) -> maps:get(id, M) =:= 1 end, Result)),
           ?assert(lists:any(fun(M) -> maps:get(id, M) =:= 2 end, Result))
       end}
     ]}.

%%====================================================================
%% list_keys/1 Tests
%%====================================================================

list_keys_test_() ->
    {setup,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"list_keys returns empty list for empty table",
       fun() ->
           flurm_db:init(),
           ?assertEqual([], flurm_db:list_keys(jobs))
       end},

      {"list_keys returns all keys",
       fun() ->
           flurm_db:put(jobs, 1, value1),
           flurm_db:put(jobs, 2, value2),
           flurm_db:put(jobs, 3, value3),
           Result = flurm_db:list_keys(jobs),
           ?assertEqual(3, length(Result)),
           ?assert(lists:member(1, Result)),
           ?assert(lists:member(2, Result)),
           ?assert(lists:member(3, Result))
       end},

      {"list_keys works with binary keys",
       fun() ->
           flurm_db:put(nodes, <<"node1">>, data1),
           flurm_db:put(nodes, <<"node2">>, data2),
           Result = flurm_db:list_keys(nodes),
           ?assertEqual(2, length(Result)),
           ?assert(lists:member(<<"node1">>, Result)),
           ?assert(lists:member(<<"node2">>, Result))
       end},

      {"list_keys creates table if not exists",
       fun() ->
           cleanup_tables(),
           ?assertEqual([], flurm_db:list_keys(partitions)),
           ?assertNotEqual(undefined, ets:whereis(flurm_db_partitions))
       end},

      {"list_keys handles mixed key types",
       fun() ->
           flurm_db:put(jobs, 1, v1),
           flurm_db:put(jobs, atom_key, v2),
           flurm_db:put(jobs, <<"binary">>, v3),
           Result = flurm_db:list_keys(jobs),
           ?assertEqual(3, length(Result)),
           ?assert(lists:member(1, Result)),
           ?assert(lists:member(atom_key, Result)),
           ?assert(lists:member(<<"binary">>, Result))
       end}
     ]}.

%%====================================================================
%% Table Name Conversion Tests
%%====================================================================

table_name_test_() ->
    {setup,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"custom table names are generated correctly",
       fun() ->
           %% Test custom table name creation
           flurm_db:put(test_table, key1, value1),
           ?assertNotEqual(undefined, ets:whereis(flurm_db_test_table)),
           ?assertEqual({ok, value1}, flurm_db:get(test_table, key1))
       end},

      {"standard tables use predefined names",
       fun() ->
           flurm_db:init(),
           ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs)),
           ?assertNotEqual(undefined, ets:whereis(flurm_db_nodes)),
           ?assertNotEqual(undefined, ets:whereis(flurm_db_partitions))
       end}
     ]}.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      fun(_) -> {"put with undefined value",
       fun() ->
           ?assertEqual(ok, flurm_db:put(jobs, 1, undefined)),
           ?assertEqual({ok, undefined}, flurm_db:get(jobs, 1))
       end} end,

      fun(_) -> {"put with empty binary key",
       fun() ->
           ?assertEqual(ok, flurm_db:put(jobs, <<>>, empty_key_value)),
           ?assertEqual({ok, empty_key_value}, flurm_db:get(jobs, <<>>))
       end} end,

      fun(_) -> {"put with empty list value",
       fun() ->
           ?assertEqual(ok, flurm_db:put(jobs, key1, [])),
           ?assertEqual({ok, []}, flurm_db:get(jobs, key1))
       end} end,

      fun(_) -> {"put with empty map value",
       fun() ->
           ?assertEqual(ok, flurm_db:put(jobs, key2, #{})),
           ?assertEqual({ok, #{}}, flurm_db:get(jobs, key2))
       end} end,

      fun(_) -> {"large number of entries",
       fun() ->
           lists:foreach(fun(N) ->
               flurm_db:put(jobs, N, {job, N})
           end, lists:seq(1, 100)),
           ?assertEqual(100, length(flurm_db:list(jobs))),
           ?assertEqual(100, length(flurm_db:list_keys(jobs))),
           ?assertEqual({ok, {job, 50}}, flurm_db:get(jobs, 50))
       end} end
     ]}.

%%====================================================================
%% Concurrent Access Tests
%%====================================================================

concurrent_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      fun(_) -> {"concurrent puts do not conflict",
       fun() ->
           flurm_db:init(),
           Parent = self(),
           %% Spawn multiple processes to write
           Pids = [spawn(fun() ->
               flurm_db:put(jobs, N, {value, N}),
               Parent ! {done, N}
           end) || N <- lists:seq(1, 10)],

           %% Wait for all to complete
           lists:foreach(fun(_) ->
               receive {done, _} -> ok after 1000 -> error(timeout) end
           end, Pids),

           %% Verify all writes succeeded
           ?assertEqual(10, length(flurm_db:list(jobs)))
       end} end,

      fun(_) -> {"concurrent reads during writes",
       fun() ->
           flurm_db:init(),
           %% Pre-populate
           lists:foreach(fun(N) ->
               flurm_db:put(jobs, N, {value, N})
           end, lists:seq(1, 10)),

           Parent = self(),
           %% Spawn readers
           ReaderPids = [spawn(fun() ->
               Result = flurm_db:get(jobs, N rem 10 + 1),
               Parent ! {read_done, N, Result}
           end) || N <- lists:seq(1, 5)],

           %% Spawn writers
           WriterPids = [spawn(fun() ->
               flurm_db:put(jobs, N + 100, {new_value, N}),
               Parent ! {write_done, N}
           end) || N <- lists:seq(1, 5)],

           %% Wait for all
           lists:foreach(fun(_) ->
               receive
                   {read_done, _, {ok, _}} -> ok;
                   {write_done, _} -> ok
               after 1000 -> error(timeout)
               end
           end, ReaderPids ++ WriterPids),

           ?assertEqual(15, length(flurm_db:list(jobs)))
       end} end
     ]}.

%%====================================================================
%% Integration Tests
%%====================================================================

integration_test_() ->
    {foreach,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      fun(_) -> {"full CRUD cycle",
       fun() ->
           %% Create
           flurm_db:init(),
           ?assertEqual([], flurm_db:list(jobs)),

           %% Insert
           ?assertEqual(ok, flurm_db:put(jobs, 1, #{id => 1, state => pending})),
           ?assertEqual(ok, flurm_db:put(jobs, 2, #{id => 2, state => running})),

           %% Read
           ?assertEqual({ok, #{id => 1, state => pending}}, flurm_db:get(jobs, 1)),
           ?assertEqual({ok, #{id => 2, state => running}}, flurm_db:get(jobs, 2)),
           ?assertEqual({error, not_found}, flurm_db:get(jobs, 3)),

           %% Update
           ?assertEqual(ok, flurm_db:put(jobs, 1, #{id => 1, state => running})),
           ?assertEqual({ok, #{id => 1, state => running}}, flurm_db:get(jobs, 1)),

           %% List
           ?assertEqual(2, length(flurm_db:list(jobs))),
           ?assertEqual(2, length(flurm_db:list_keys(jobs))),

           %% Delete
           ?assertEqual(ok, flurm_db:delete(jobs, 1)),
           ?assertEqual({error, not_found}, flurm_db:get(jobs, 1)),
           ?assertEqual(1, length(flurm_db:list(jobs)))
       end} end,

      fun(_) -> {"multiple tables workflow",
       fun() ->
           flurm_db:init(),

           %% Add job
           flurm_db:put(jobs, 1, #{id => 1, partition => <<"default">>}),

           %% Add node
           flurm_db:put(nodes, <<"node1">>, #{hostname => <<"host1">>, cpus => 8}),

           %% Add partition
           flurm_db:put(partitions, <<"default">>, #{nodes => [<<"node1">>]}),

           %% Verify all tables have data
           ?assertEqual(1, length(flurm_db:list(jobs))),
           ?assertEqual(1, length(flurm_db:list(nodes))),
           ?assertEqual(1, length(flurm_db:list(partitions))),

           %% Cross-reference
           {ok, Job} = flurm_db:get(jobs, 1),
           PartName = maps:get(partition, Job),
           {ok, Part} = flurm_db:get(partitions, PartName),
           [NodeName|_] = maps:get(nodes, Part),
           {ok, Node} = flurm_db:get(nodes, NodeName),
           ?assertEqual(8, maps:get(cpus, Node))
       end} end
     ]}.
