%%%-------------------------------------------------------------------
%%% @doc FLURM Database High-Level API Tests
%%%
%%% Tests for the flurm_db module which provides the unified interface
%%% to the FLURM distributed database.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Initialize legacy ETS tables
    ok = flurm_db:init(),
    ok.

cleanup(_) ->
    %% Clean up ETS tables
    catch ets:delete(flurm_db_jobs),
    catch ets:delete(flurm_db_nodes),
    catch ets:delete(flurm_db_partitions),
    ok.

%%====================================================================
%% Legacy ETS API Tests
%%====================================================================

legacy_api_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"Test init creates tables", fun test_init/0},
         {"Test put and get", fun test_put_get/0},
         {"Test delete", fun test_delete/0},
         {"Test list", fun test_list/0},
         {"Test list_keys", fun test_list_keys/0},
         {"Test get not found", fun test_get_not_found/0},
         {"Test custom table name", fun test_custom_table/0}
     ]
    }.

test_init() ->
    %% Tables should be created by setup
    ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_nodes)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_partitions)),
    ok.

test_put_get() ->
    ok = flurm_db:put(jobs, job1, #{id => 1, name => <<"test">>}),
    {ok, Value} = flurm_db:get(jobs, job1),
    ?assertEqual(#{id => 1, name => <<"test">>}, Value),
    ok.

test_delete() ->
    ok = flurm_db:put(jobs, job2, #{id => 2}),
    {ok, _} = flurm_db:get(jobs, job2),
    ok = flurm_db:delete(jobs, job2),
    ?assertEqual({error, not_found}, flurm_db:get(jobs, job2)),
    ok.

test_list() ->
    %% Clear and add fresh data
    ets:delete_all_objects(flurm_db_jobs),
    ok = flurm_db:put(jobs, j1, #{id => 1}),
    ok = flurm_db:put(jobs, j2, #{id => 2}),
    ok = flurm_db:put(jobs, j3, #{id => 3}),
    Values = flurm_db:list(jobs),
    ?assertEqual(3, length(Values)),
    ok.

test_list_keys() ->
    %% Clear and add fresh data
    ets:delete_all_objects(flurm_db_nodes),
    ok = flurm_db:put(nodes, n1, #{name => <<"node1">>}),
    ok = flurm_db:put(nodes, n2, #{name => <<"node2">>}),
    Keys = flurm_db:list_keys(nodes),
    ?assertEqual(2, length(Keys)),
    ?assert(lists:member(n1, Keys)),
    ?assert(lists:member(n2, Keys)),
    ok.

test_get_not_found() ->
    Result = flurm_db:get(jobs, nonexistent_key),
    ?assertEqual({error, not_found}, Result),
    ok.

test_custom_table() ->
    %% Test with a custom table name
    ok = flurm_db:put(custom_table, key1, value1),
    {ok, Value} = flurm_db:get(custom_table, key1),
    ?assertEqual(value1, Value),
    ok.

%%====================================================================
%% Table Name Helper Tests
%%====================================================================

table_name_test_() ->
    [
        {"jobs table name", fun() ->
            %% Make sure we can access the jobs table
            flurm_db:init(),
            ok = flurm_db:put(jobs, test_key, test_value),
            ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs))
        end},
        {"nodes table name", fun() ->
            flurm_db:init(),
            ok = flurm_db:put(nodes, test_key, test_value),
            ?assertNotEqual(undefined, ets:whereis(flurm_db_nodes))
        end},
        {"partitions table name", fun() ->
            flurm_db:init(),
            ok = flurm_db:put(partitions, test_key, test_value),
            ?assertNotEqual(undefined, ets:whereis(flurm_db_partitions))
        end}
    ].
