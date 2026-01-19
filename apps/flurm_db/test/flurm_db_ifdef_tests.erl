%%%-------------------------------------------------------------------
%%% @doc FLURM Database -ifdef(TEST) Exports Tests
%%%
%%% Tests for internal helper functions exported via -ifdef(TEST).
%%% These tests directly call the internal functions to achieve coverage.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_db.hrl").

%%====================================================================
%% table_name/1 Tests
%%====================================================================

table_name_test_() ->
    [
        {"table_name/1 for jobs", fun() ->
            ?assertEqual(flurm_db_jobs, flurm_db:table_name(jobs))
        end},

        {"table_name/1 for nodes", fun() ->
            ?assertEqual(flurm_db_nodes, flurm_db:table_name(nodes))
        end},

        {"table_name/1 for partitions", fun() ->
            ?assertEqual(flurm_db_partitions, flurm_db:table_name(partitions))
        end},

        {"table_name/1 for custom table", fun() ->
            Result = flurm_db:table_name(custom),
            ?assertEqual(flurm_db_custom, Result)
        end},

        {"table_name/1 for another custom table", fun() ->
            Result = flurm_db:table_name(my_table),
            ?assertEqual(flurm_db_my_table, Result)
        end}
    ].

%%====================================================================
%% ensure_table/1 Tests
%%====================================================================

ensure_table_jobs_test() ->
    %% Creates the table if it doesn't exist
    Result = flurm_db:ensure_table(jobs),
    %% Should return ok or the table reference
    ?assertNotEqual(error, Result),
    %% Verify table exists
    TableName = flurm_db:table_name(jobs),
    ?assertNotEqual(undefined, ets:whereis(TableName)).

ensure_table_nodes_test() ->
    Result = flurm_db:ensure_table(nodes),
    ?assertNotEqual(error, Result),
    TableName = flurm_db:table_name(nodes),
    ?assertNotEqual(undefined, ets:whereis(TableName)).

ensure_table_partitions_test() ->
    Result = flurm_db:ensure_table(partitions),
    ?assertNotEqual(error, Result),
    TableName = flurm_db:table_name(partitions),
    ?assertNotEqual(undefined, ets:whereis(TableName)).

ensure_table_idempotent_test() ->
    %% First call creates table
    _ = flurm_db:ensure_table(test_idempotent),
    TableName = flurm_db:table_name(test_idempotent),
    ?assertNotEqual(undefined, ets:whereis(TableName)),

    %% Second call should not fail
    Result = flurm_db:ensure_table(test_idempotent),
    ?assertNotEqual(error, Result).

ensure_table_custom_test() ->
    %% Custom table names should also work
    TableAtom = test_custom_table,
    _ = flurm_db:ensure_table(TableAtom),
    ExpectedName = flurm_db:table_name(TableAtom),
    ?assertNotEqual(undefined, ets:whereis(ExpectedName)).

%%====================================================================
%% ensure_tables/0 Tests
%%====================================================================

ensure_tables_test() ->
    %% Creates jobs, nodes, and partitions tables
    ?assertEqual(ok, flurm_db:ensure_tables()),

    %% Verify all standard tables exist
    ?assertNotEqual(undefined, ets:whereis(flurm_db_jobs)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_nodes)),
    ?assertNotEqual(undefined, ets:whereis(flurm_db_partitions)).

ensure_tables_idempotent_test() ->
    %% First call
    ?assertEqual(ok, flurm_db:ensure_tables()),
    %% Second call should also succeed
    ?assertEqual(ok, flurm_db:ensure_tables()).

%%====================================================================
%% Legacy API integration tests (exercises internal functions)
%%====================================================================

init_test() ->
    %% init/0 calls ensure_tables/0 internally
    ?assertEqual(ok, flurm_db:init()).

put_get_delete_test() ->
    %% Test the legacy ETS-based API which uses ensure_table and table_name internally
    ?assertEqual(ok, flurm_db:init()),

    %% Put a value
    ?assertEqual(ok, flurm_db:put(jobs, test_key, #{name => <<"test">>})),

    %% Get the value
    ?assertEqual({ok, #{name => <<"test">>}}, flurm_db:get(jobs, test_key)),

    %% Delete the value
    ?assertEqual(ok, flurm_db:delete(jobs, test_key)),

    %% Verify deletion
    ?assertEqual({error, not_found}, flurm_db:get(jobs, test_key)).

list_test() ->
    ?assertEqual(ok, flurm_db:init()),

    %% Clear any existing data
    ets:delete_all_objects(flurm_db_jobs),

    %% Add some values
    ?assertEqual(ok, flurm_db:put(jobs, key1, value1)),
    ?assertEqual(ok, flurm_db:put(jobs, key2, value2)),
    ?assertEqual(ok, flurm_db:put(jobs, key3, value3)),

    %% List all values
    Values = flurm_db:list(jobs),
    ?assertEqual(3, length(Values)),
    ?assert(lists:member(value1, Values)),
    ?assert(lists:member(value2, Values)),
    ?assert(lists:member(value3, Values)).

list_keys_test() ->
    ?assertEqual(ok, flurm_db:init()),

    %% Clear any existing data
    ets:delete_all_objects(flurm_db_nodes),

    %% Add some values
    ?assertEqual(ok, flurm_db:put(nodes, <<"node1">>, node1_data)),
    ?assertEqual(ok, flurm_db:put(nodes, <<"node2">>, node2_data)),

    %% List all keys
    Keys = flurm_db:list_keys(nodes),
    ?assertEqual(2, length(Keys)),
    ?assert(lists:member(<<"node1">>, Keys)),
    ?assert(lists:member(<<"node2">>, Keys)).

get_not_found_test() ->
    ?assertEqual(ok, flurm_db:init()),
    ?assertEqual({error, not_found}, flurm_db:get(jobs, nonexistent_key)).

%%====================================================================
%% Edge case tests
%%====================================================================

table_name_preserves_prefix_test() ->
    %% Verify the naming convention is consistent
    ?assertEqual(flurm_db_jobs, flurm_db:table_name(jobs)),
    ?assertEqual(flurm_db_nodes, flurm_db:table_name(nodes)),
    ?assertEqual(flurm_db_partitions, flurm_db:table_name(partitions)),
    %% Custom table should follow same pattern
    ?assertEqual(flurm_db_foo, flurm_db:table_name(foo)),
    ?assertEqual(flurm_db_bar_baz, flurm_db:table_name(bar_baz)).

multiple_values_same_key_test() ->
    ?assertEqual(ok, flurm_db:init()),

    %% Put a value
    ?assertEqual(ok, flurm_db:put(partitions, <<"test_part">>, #{version => 1})),
    ?assertEqual({ok, #{version => 1}}, flurm_db:get(partitions, <<"test_part">>)),

    %% Overwrite with new value
    ?assertEqual(ok, flurm_db:put(partitions, <<"test_part">>, #{version => 2})),
    ?assertEqual({ok, #{version => 2}}, flurm_db:get(partitions, <<"test_part">>)).

empty_table_operations_test() ->
    ?assertEqual(ok, flurm_db:init()),

    %% Create a fresh custom table
    _ = flurm_db:ensure_table(empty_test),
    ets:delete_all_objects(flurm_db:table_name(empty_test)),

    %% List on empty table should return empty list
    ?assertEqual([], flurm_db:list(empty_test)),
    ?assertEqual([], flurm_db:list_keys(empty_test)).
