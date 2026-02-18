%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_pmi_kvs module
%%% Coverage target: 100% of all functions and branches
%%%
%%% This file supplements the existing flurm_pmi_kvs_tests.erl
%%% with additional edge cases and branch coverage.
%%%-------------------------------------------------------------------
-module(flurm_pmi_kvs_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% new/0 Tests
%%%===================================================================

new_returns_empty_map_test() ->
    KVS = flurm_pmi_kvs:new(),
    ?assertEqual(#{}, KVS).

new_has_zero_size_test() ->
    KVS = flurm_pmi_kvs:new(),
    ?assertEqual(0, flurm_pmi_kvs:size(KVS)).

new_has_empty_keys_test() ->
    KVS = flurm_pmi_kvs:new(),
    ?assertEqual([], flurm_pmi_kvs:keys(KVS)).

%%%===================================================================
%%% new/1 Tests
%%%===================================================================

new_with_empty_map_test() ->
    KVS = flurm_pmi_kvs:new(#{}),
    ?assertEqual(0, flurm_pmi_kvs:size(KVS)).

new_with_single_entry_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"key">> => <<"value">>}),
    ?assertEqual(1, flurm_pmi_kvs:size(KVS)).

new_with_multiple_entries_test() ->
    Initial = #{
        <<"k1">> => <<"v1">>,
        <<"k2">> => <<"v2">>,
        <<"k3">> => <<"v3">>
    },
    KVS = flurm_pmi_kvs:new(Initial),
    ?assertEqual(3, flurm_pmi_kvs:size(KVS)).

new_preserves_values_test() ->
    Initial = #{<<"mykey">> => <<"myvalue">>},
    KVS = flurm_pmi_kvs:new(Initial),
    ?assertEqual({ok, <<"myvalue">>}, flurm_pmi_kvs:get(KVS, <<"mykey">>)).

%%%===================================================================
%%% put/3 Tests - Binary Key and Value
%%%===================================================================

put_binary_key_binary_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, <<"value">>),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

put_updates_existing_key_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, <<"value1">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"key">>, <<"value2">>),
    ?assertEqual({ok, <<"value2">>}, flurm_pmi_kvs:get(KVS2, <<"key">>)),
    ?assertEqual(1, flurm_pmi_kvs:size(KVS2)).

put_empty_key_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<>>, <<"value">>),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(KVS1, <<>>)).

put_empty_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, <<>>),
    ?assertEqual({ok, <<>>}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

put_large_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    LargeValue = binary:copy(<<"x">>, 10000),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, LargeValue),
    {ok, Retrieved} = flurm_pmi_kvs:get(KVS1, <<"key">>),
    ?assertEqual(10000, byte_size(Retrieved)).

%%%===================================================================
%%% put/3 Tests - Type Conversions (to_binary paths)
%%%===================================================================

put_list_key_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, "list_key", <<"value">>),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(KVS1, <<"list_key">>)).

put_atom_key_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, atom_key, <<"value">>),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(KVS1, <<"atom_key">>)).

put_integer_key_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, 123, <<"value">>),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(KVS1, <<"123">>)).

put_list_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, "list_value"),
    ?assertEqual({ok, <<"list_value">>}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

put_atom_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, atom_value),
    ?assertEqual({ok, <<"atom_value">>}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

put_integer_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, 42),
    ?assertEqual({ok, <<"42">>}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

put_negative_integer_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, -999),
    ?assertEqual({ok, <<"-999">>}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

put_zero_integer_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, 0),
    ?assertEqual({ok, <<"0">>}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

put_mixed_type_conversions_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, "list_k", "list_v"),
    KVS2 = flurm_pmi_kvs:put(KVS1, atom_k, atom_v),
    KVS3 = flurm_pmi_kvs:put(KVS2, 100, 200),
    ?assertEqual({ok, <<"list_v">>}, flurm_pmi_kvs:get(KVS3, <<"list_k">>)),
    ?assertEqual({ok, <<"atom_v">>}, flurm_pmi_kvs:get(KVS3, <<"atom_k">>)),
    ?assertEqual({ok, <<"200">>}, flurm_pmi_kvs:get(KVS3, <<"100">>)).

%%%===================================================================
%%% get/2 Tests
%%%===================================================================

get_existing_key_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"key">> => <<"value">>}),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(KVS, <<"key">>)).

get_non_existing_key_test() ->
    KVS = flurm_pmi_kvs:new(),
    ?assertEqual({error, not_found}, flurm_pmi_kvs:get(KVS, <<"missing">>)).

get_with_list_key_conversion_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"mykey">> => <<"myvalue">>}),
    ?assertEqual({ok, <<"myvalue">>}, flurm_pmi_kvs:get(KVS, "mykey")).

get_with_atom_key_conversion_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"mykey">> => <<"myvalue">>}),
    ?assertEqual({ok, <<"myvalue">>}, flurm_pmi_kvs:get(KVS, mykey)).

get_with_integer_key_conversion_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"42">> => <<"value">>}),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(KVS, 42)).

get_empty_kvs_test() ->
    KVS = flurm_pmi_kvs:new(),
    ?assertEqual({error, not_found}, flurm_pmi_kvs:get(KVS, <<"any_key">>)).

%%%===================================================================
%%% get_all/1 Tests
%%%===================================================================

get_all_empty_test() ->
    KVS = flurm_pmi_kvs:new(),
    ?assertEqual([], flurm_pmi_kvs:get_all(KVS)).

get_all_single_entry_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"k">> => <<"v">>}),
    All = flurm_pmi_kvs:get_all(KVS),
    ?assertEqual([{<<"k">>, <<"v">>}], All).

get_all_multiple_entries_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"a">> => <<"1">>, <<"b">> => <<"2">>}),
    All = flurm_pmi_kvs:get_all(KVS),
    ?assertEqual(2, length(All)),
    ?assert(lists:member({<<"a">>, <<"1">>}, All)),
    ?assert(lists:member({<<"b">>, <<"2">>}, All)).

%%%===================================================================
%%% delete/2 Tests
%%%===================================================================

delete_existing_key_test() ->
    KVS0 = flurm_pmi_kvs:new(#{<<"key">> => <<"value">>}),
    KVS1 = flurm_pmi_kvs:delete(KVS0, <<"key">>),
    ?assertEqual({error, not_found}, flurm_pmi_kvs:get(KVS1, <<"key">>)),
    ?assertEqual(0, flurm_pmi_kvs:size(KVS1)).

delete_non_existing_key_test() ->
    KVS0 = flurm_pmi_kvs:new(#{<<"key">> => <<"value">>}),
    KVS1 = flurm_pmi_kvs:delete(KVS0, <<"missing">>),
    ?assertEqual(1, flurm_pmi_kvs:size(KVS1)),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

delete_with_list_key_conversion_test() ->
    KVS0 = flurm_pmi_kvs:new(#{<<"mykey">> => <<"value">>}),
    KVS1 = flurm_pmi_kvs:delete(KVS0, "mykey"),
    ?assertEqual({error, not_found}, flurm_pmi_kvs:get(KVS1, <<"mykey">>)).

delete_with_atom_key_conversion_test() ->
    KVS0 = flurm_pmi_kvs:new(#{<<"mykey">> => <<"value">>}),
    KVS1 = flurm_pmi_kvs:delete(KVS0, mykey),
    ?assertEqual({error, not_found}, flurm_pmi_kvs:get(KVS1, <<"mykey">>)).

delete_with_integer_key_conversion_test() ->
    KVS0 = flurm_pmi_kvs:new(#{<<"123">> => <<"value">>}),
    KVS1 = flurm_pmi_kvs:delete(KVS0, 123),
    ?assertEqual({error, not_found}, flurm_pmi_kvs:get(KVS1, <<"123">>)).

delete_empty_kvs_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:delete(KVS0, <<"any">>),
    ?assertEqual(0, flurm_pmi_kvs:size(KVS1)).

%%%===================================================================
%%% size/1 Tests
%%%===================================================================

size_empty_test() ->
    ?assertEqual(0, flurm_pmi_kvs:size(flurm_pmi_kvs:new())).

size_one_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"k">> => <<"v">>}),
    ?assertEqual(1, flurm_pmi_kvs:size(KVS)).

size_many_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS = lists:foldl(fun(I, Acc) ->
        Key = integer_to_binary(I),
        flurm_pmi_kvs:put(Acc, Key, <<"v">>)
    end, KVS0, lists:seq(1, 100)),
    ?assertEqual(100, flurm_pmi_kvs:size(KVS)).

size_after_delete_test() ->
    KVS0 = flurm_pmi_kvs:new(#{<<"a">> => <<"1">>, <<"b">> => <<"2">>}),
    ?assertEqual(2, flurm_pmi_kvs:size(KVS0)),
    KVS1 = flurm_pmi_kvs:delete(KVS0, <<"a">>),
    ?assertEqual(1, flurm_pmi_kvs:size(KVS1)).

%%%===================================================================
%%% keys/1 Tests
%%%===================================================================

keys_empty_test() ->
    ?assertEqual([], flurm_pmi_kvs:keys(flurm_pmi_kvs:new())).

keys_single_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"single">> => <<"v">>}),
    ?assertEqual([<<"single">>], flurm_pmi_kvs:keys(KVS)).

keys_multiple_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"a">> => <<"1">>, <<"b">> => <<"2">>, <<"c">> => <<"3">>}),
    Keys = flurm_pmi_kvs:keys(KVS),
    ?assertEqual(3, length(Keys)),
    ?assert(lists:member(<<"a">>, Keys)),
    ?assert(lists:member(<<"b">>, Keys)),
    ?assert(lists:member(<<"c">>, Keys)).

keys_returns_binaries_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, "list_key", <<"v">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, atom_key, <<"v">>),
    Keys = flurm_pmi_kvs:keys(KVS2),
    lists:foreach(fun(K) -> ?assert(is_binary(K)) end, Keys).

%%%===================================================================
%%% get_by_index/2 Tests
%%%===================================================================

get_by_index_first_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"a">> => <<"1">>, <<"b">> => <<"2">>}),
    {ok, Key, Value} = flurm_pmi_kvs:get_by_index(KVS, 0),
    %% Keys are sorted, so "a" should be first
    ?assertEqual(<<"a">>, Key),
    ?assertEqual(<<"1">>, Value).

get_by_index_second_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"a">> => <<"1">>, <<"b">> => <<"2">>}),
    {ok, Key, Value} = flurm_pmi_kvs:get_by_index(KVS, 1),
    ?assertEqual(<<"b">>, Key),
    ?assertEqual(<<"2">>, Value).

get_by_index_out_of_bounds_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"a">> => <<"1">>}),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_kvs:get_by_index(KVS, 1)).

get_by_index_empty_kvs_test() ->
    KVS = flurm_pmi_kvs:new(),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_kvs:get_by_index(KVS, 0)).

get_by_index_large_index_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"k">> => <<"v">>}),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_kvs:get_by_index(KVS, 1000000)).

get_by_index_sorted_order_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"z">> => <<"3">>, <<"a">> => <<"1">>, <<"m">> => <<"2">>}),
    {ok, K0, _} = flurm_pmi_kvs:get_by_index(KVS, 0),
    {ok, K1, _} = flurm_pmi_kvs:get_by_index(KVS, 1),
    {ok, K2, _} = flurm_pmi_kvs:get_by_index(KVS, 2),
    ?assertEqual(<<"a">>, K0),
    ?assertEqual(<<"m">>, K1),
    ?assertEqual(<<"z">>, K2).

get_by_index_iteration_test() ->
    KVS = flurm_pmi_kvs:new(#{<<"k1">> => <<"v1">>, <<"k2">> => <<"v2">>, <<"k3">> => <<"v3">>}),
    %% Iterate through all indices
    Results = lists:map(fun(I) ->
        flurm_pmi_kvs:get_by_index(KVS, I)
    end, lists:seq(0, 5)),
    %% First 3 should succeed
    ?assertMatch({ok, _, _}, lists:nth(1, Results)),
    ?assertMatch({ok, _, _}, lists:nth(2, Results)),
    ?assertMatch({ok, _, _}, lists:nth(3, Results)),
    %% Rest should return end_of_kvs
    ?assertEqual({error, end_of_kvs}, lists:nth(4, Results)),
    ?assertEqual({error, end_of_kvs}, lists:nth(5, Results)),
    ?assertEqual({error, end_of_kvs}, lists:nth(6, Results)).

%%%===================================================================
%%% merge/2 Tests
%%%===================================================================

merge_two_empty_test() ->
    KVS1 = flurm_pmi_kvs:new(),
    KVS2 = flurm_pmi_kvs:new(),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(0, flurm_pmi_kvs:size(Merged)).

merge_empty_with_nonempty_test() ->
    KVS1 = flurm_pmi_kvs:new(),
    KVS2 = flurm_pmi_kvs:new(#{<<"k">> => <<"v">>}),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(1, flurm_pmi_kvs:size(Merged)),
    ?assertEqual({ok, <<"v">>}, flurm_pmi_kvs:get(Merged, <<"k">>)).

merge_nonempty_with_empty_test() ->
    KVS1 = flurm_pmi_kvs:new(#{<<"k">> => <<"v">>}),
    KVS2 = flurm_pmi_kvs:new(),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(1, flurm_pmi_kvs:size(Merged)),
    ?assertEqual({ok, <<"v">>}, flurm_pmi_kvs:get(Merged, <<"k">>)).

merge_disjoint_keys_test() ->
    KVS1 = flurm_pmi_kvs:new(#{<<"a">> => <<"1">>}),
    KVS2 = flurm_pmi_kvs:new(#{<<"b">> => <<"2">>}),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(2, flurm_pmi_kvs:size(Merged)),
    ?assertEqual({ok, <<"1">>}, flurm_pmi_kvs:get(Merged, <<"a">>)),
    ?assertEqual({ok, <<"2">>}, flurm_pmi_kvs:get(Merged, <<"b">>)).

merge_overlapping_keys_test() ->
    KVS1 = flurm_pmi_kvs:new(#{<<"k">> => <<"from_first">>}),
    KVS2 = flurm_pmi_kvs:new(#{<<"k">> => <<"from_second">>}),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(1, flurm_pmi_kvs:size(Merged)),
    %% Second KVS values should override first
    ?assertEqual({ok, <<"from_second">>}, flurm_pmi_kvs:get(Merged, <<"k">>)).

merge_many_keys_test() ->
    KVS1 = flurm_pmi_kvs:new(#{<<"a">> => <<"1">>, <<"b">> => <<"2">>, <<"c">> => <<"3">>}),
    KVS2 = flurm_pmi_kvs:new(#{<<"d">> => <<"4">>, <<"e">> => <<"5">>}),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(5, flurm_pmi_kvs:size(Merged)).

merge_partial_overlap_test() ->
    KVS1 = flurm_pmi_kvs:new(#{<<"a">> => <<"1">>, <<"b">> => <<"2">>}),
    KVS2 = flurm_pmi_kvs:new(#{<<"b">> => <<"new">>, <<"c">> => <<"3">>}),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(3, flurm_pmi_kvs:size(Merged)),
    ?assertEqual({ok, <<"1">>}, flurm_pmi_kvs:get(Merged, <<"a">>)),
    ?assertEqual({ok, <<"new">>}, flurm_pmi_kvs:get(Merged, <<"b">>)),
    ?assertEqual({ok, <<"3">>}, flurm_pmi_kvs:get(Merged, <<"c">>)).

%%%===================================================================
%%% to_binary Internal Function Coverage (via put/3)
%%%===================================================================

%% All to_binary paths are covered via put tests:
%% - Binary: put_binary_key_binary_value_test
%% - List: put_list_key_test, put_list_value_test
%% - Atom: put_atom_key_test, put_atom_value_test
%% - Integer: put_integer_key_test, put_integer_value_test

%%%===================================================================
%%% PMI Use Case Tests
%%%===================================================================

%% Simulate MPI address exchange
mpi_address_exchange_test() ->
    KVS0 = flurm_pmi_kvs:new(),

    %% Each rank puts its connection info
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"pmi_rank_0">>, <<"tcp://192.168.1.1:5000">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"pmi_rank_1">>, <<"tcp://192.168.1.2:5001">>),
    KVS3 = flurm_pmi_kvs:put(KVS2, <<"pmi_rank_2">>, <<"tcp://192.168.1.3:5002">>),
    KVS4 = flurm_pmi_kvs:put(KVS3, <<"pmi_rank_3">>, <<"tcp://192.168.1.4:5003">>),

    %% Each rank can retrieve all others
    ?assertEqual({ok, <<"tcp://192.168.1.1:5000">>}, flurm_pmi_kvs:get(KVS4, <<"pmi_rank_0">>)),
    ?assertEqual({ok, <<"tcp://192.168.1.2:5001">>}, flurm_pmi_kvs:get(KVS4, <<"pmi_rank_1">>)),
    ?assertEqual({ok, <<"tcp://192.168.1.3:5002">>}, flurm_pmi_kvs:get(KVS4, <<"pmi_rank_2">>)),
    ?assertEqual({ok, <<"tcp://192.168.1.4:5003">>}, flurm_pmi_kvs:get(KVS4, <<"pmi_rank_3">>)).

%% Simulate PMI getbyidx iteration pattern
pmi_getbyidx_pattern_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key1">>, <<"val1">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"key2">>, <<"val2">>),
    KVS3 = flurm_pmi_kvs:put(KVS2, <<"key3">>, <<"val3">>),

    %% PMI client iterates with getbyidx until end_of_kvs
    iterate_and_collect(KVS3, 0, []).

iterate_and_collect(KVS, Index, Acc) ->
    case flurm_pmi_kvs:get_by_index(KVS, Index) of
        {ok, K, V} ->
            iterate_and_collect(KVS, Index + 1, [{K, V} | Acc]);
        {error, end_of_kvs} ->
            ?assertEqual(3, length(Acc))
    end.

%% Simulate merging KVS from multiple nodes
multi_node_kvs_merge_test() ->
    Node1KVS = flurm_pmi_kvs:new(#{
        <<"rank_0_host">> => <<"node1">>,
        <<"rank_0_port">> => <<"5000">>
    }),
    Node2KVS = flurm_pmi_kvs:new(#{
        <<"rank_1_host">> => <<"node2">>,
        <<"rank_1_port">> => <<"5001">>
    }),
    CombinedKVS = flurm_pmi_kvs:merge(Node1KVS, Node2KVS),
    ?assertEqual(4, flurm_pmi_kvs:size(CombinedKVS)),
    ?assertEqual({ok, <<"node1">>}, flurm_pmi_kvs:get(CombinedKVS, <<"rank_0_host">>)),
    ?assertEqual({ok, <<"node2">>}, flurm_pmi_kvs:get(CombinedKVS, <<"rank_1_host">>)).

%%%===================================================================
%%% Edge Cases
%%%===================================================================

%% Test with binary containing null bytes
binary_with_null_bytes_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    Value = <<0, 1, 2, 0, 3, 4, 0>>,
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, Value),
    ?assertEqual({ok, Value}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

%% Test with unicode in key/value
unicode_key_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    Key = <<"key_", 195, 169, 195, 168>>,  % UTF-8 encoded
    Value = <<"val_", 226, 128, 147>>,
    KVS1 = flurm_pmi_kvs:put(KVS0, Key, Value),
    ?assertEqual({ok, Value}, flurm_pmi_kvs:get(KVS1, Key)).

%% Test rapid put/get cycles
rapid_put_get_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS = lists:foldl(fun(I, Acc) ->
        Key = <<"key_", (integer_to_binary(I))/binary>>,
        Value = <<"value_", (integer_to_binary(I))/binary>>,
        flurm_pmi_kvs:put(Acc, Key, Value)
    end, KVS0, lists:seq(1, 1000)),
    ?assertEqual(1000, flurm_pmi_kvs:size(KVS)),
    ?assertEqual({ok, <<"value_500">>}, flurm_pmi_kvs:get(KVS, <<"key_500">>)).

%% Test key ordering with numeric strings
numeric_string_keys_sorted_test() ->
    KVS = flurm_pmi_kvs:new(#{
        <<"10">> => <<"v10">>,
        <<"2">> => <<"v2">>,
        <<"1">> => <<"v1">>,
        <<"20">> => <<"v20">>
    }),
    %% String sorting: "1" < "10" < "2" < "20"
    {ok, K0, _} = flurm_pmi_kvs:get_by_index(KVS, 0),
    {ok, K1, _} = flurm_pmi_kvs:get_by_index(KVS, 1),
    {ok, K2, _} = flurm_pmi_kvs:get_by_index(KVS, 2),
    {ok, K3, _} = flurm_pmi_kvs:get_by_index(KVS, 3),
    ?assertEqual(<<"1">>, K0),
    ?assertEqual(<<"10">>, K1),
    ?assertEqual(<<"2">>, K2),
    ?assertEqual(<<"20">>, K3).
