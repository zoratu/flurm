%%%-------------------------------------------------------------------
%%% @doc Unit tests for flurm_pmi_kvs module
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_kvs_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Basic Operations Tests
%%%===================================================================

new_test() ->
    KVS = flurm_pmi_kvs:new(),
    ?assertEqual(0, flurm_pmi_kvs:size(KVS)),
    ?assertEqual([], flurm_pmi_kvs:keys(KVS)).

new_with_initial_test() ->
    Initial = #{<<"key1">> => <<"value1">>, <<"key2">> => <<"value2">>},
    KVS = flurm_pmi_kvs:new(Initial),
    ?assertEqual(2, flurm_pmi_kvs:size(KVS)),
    ?assertEqual({ok, <<"value1">>}, flurm_pmi_kvs:get(KVS, <<"key1">>)).

put_get_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"test_key">>, <<"test_value">>),
    ?assertEqual({ok, <<"test_value">>}, flurm_pmi_kvs:get(KVS1, <<"test_key">>)).

put_overwrite_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, <<"value1">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"key">>, <<"value2">>),
    ?assertEqual({ok, <<"value2">>}, flurm_pmi_kvs:get(KVS2, <<"key">>)).

get_not_found_test() ->
    KVS = flurm_pmi_kvs:new(),
    ?assertEqual({error, not_found}, flurm_pmi_kvs:get(KVS, <<"nonexistent">>)).

delete_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, <<"value">>),
    KVS2 = flurm_pmi_kvs:delete(KVS1, <<"key">>),
    ?assertEqual({error, not_found}, flurm_pmi_kvs:get(KVS2, <<"key">>)).

size_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    ?assertEqual(0, flurm_pmi_kvs:size(KVS0)),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key1">>, <<"value1">>),
    ?assertEqual(1, flurm_pmi_kvs:size(KVS1)),
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"key2">>, <<"value2">>),
    ?assertEqual(2, flurm_pmi_kvs:size(KVS2)).

keys_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key1">>, <<"value1">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"key2">>, <<"value2">>),
    Keys = flurm_pmi_kvs:keys(KVS2),
    ?assertEqual(2, length(Keys)),
    ?assert(lists:member(<<"key1">>, Keys)),
    ?assert(lists:member(<<"key2">>, Keys)).

get_all_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key1">>, <<"value1">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"key2">>, <<"value2">>),
    All = flurm_pmi_kvs:get_all(KVS2),
    ?assertEqual(2, length(All)),
    ?assert(lists:member({<<"key1">>, <<"value1">>}, All)),
    ?assert(lists:member({<<"key2">>, <<"value2">>}, All)).

%%%===================================================================
%%% get_by_index Tests
%%%===================================================================

get_by_index_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"aaa">>, <<"value_a">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"bbb">>, <<"value_b">>),
    KVS3 = flurm_pmi_kvs:put(KVS2, <<"ccc">>, <<"value_c">>),

    %% Keys should be sorted alphabetically
    ?assertEqual({ok, <<"aaa">>, <<"value_a">>}, flurm_pmi_kvs:get_by_index(KVS3, 0)),
    ?assertEqual({ok, <<"bbb">>, <<"value_b">>}, flurm_pmi_kvs:get_by_index(KVS3, 1)),
    ?assertEqual({ok, <<"ccc">>, <<"value_c">>}, flurm_pmi_kvs:get_by_index(KVS3, 2)).

get_by_index_end_of_kvs_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, <<"value">>),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_kvs:get_by_index(KVS1, 1)),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_kvs:get_by_index(KVS1, 100)).

get_by_index_empty_test() ->
    KVS = flurm_pmi_kvs:new(),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_kvs:get_by_index(KVS, 0)).

%%%===================================================================
%%% Merge Tests
%%%===================================================================

merge_test() ->
    KVS1 = flurm_pmi_kvs:new(#{<<"key1">> => <<"value1">>}),
    KVS2 = flurm_pmi_kvs:new(#{<<"key2">> => <<"value2">>}),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(2, flurm_pmi_kvs:size(Merged)),
    ?assertEqual({ok, <<"value1">>}, flurm_pmi_kvs:get(Merged, <<"key1">>)),
    ?assertEqual({ok, <<"value2">>}, flurm_pmi_kvs:get(Merged, <<"key2">>)).

merge_overwrite_test() ->
    KVS1 = flurm_pmi_kvs:new(#{<<"key">> => <<"value1">>}),
    KVS2 = flurm_pmi_kvs:new(#{<<"key">> => <<"value2">>}),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(1, flurm_pmi_kvs:size(Merged)),
    %% Second KVS overwrites first
    ?assertEqual({ok, <<"value2">>}, flurm_pmi_kvs:get(Merged, <<"key">>)).

%%%===================================================================
%%% Type Conversion Tests
%%%===================================================================

put_list_key_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, "list_key", <<"value">>),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(KVS1, <<"list_key">>)).

put_atom_key_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, atom_key, <<"value">>),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(KVS1, <<"atom_key">>)).

put_integer_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, 42),
    ?assertEqual({ok, <<"42">>}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

%%%===================================================================
%%% MPI Address Exchange Simulation Tests
%%%===================================================================

mpi_address_exchange_test() ->
    %% Simulate what MPI processes do during initialization
    %% Each rank puts its connection info, then reads others after barrier

    KVS0 = flurm_pmi_kvs:new(),

    %% Rank 0 puts its address
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"rank_0_addr">>, <<"192.168.1.1:5000">>),

    %% Rank 1 puts its address
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"rank_1_addr">>, <<"192.168.1.2:5001">>),

    %% After barrier, all ranks can read each other's addresses
    ?assertEqual({ok, <<"192.168.1.1:5000">>}, flurm_pmi_kvs:get(KVS2, <<"rank_0_addr">>)),
    ?assertEqual({ok, <<"192.168.1.2:5001">>}, flurm_pmi_kvs:get(KVS2, <<"rank_1_addr">>)).

%% Test iterating through KVS (PMI getbyidx use case)
iterate_kvs_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key1">>, <<"val1">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"key2">>, <<"val2">>),

    %% Iterate through all entries
    {Results, _} = lists:foldl(
        fun(Idx, {Acc, KVS}) ->
            case flurm_pmi_kvs:get_by_index(KVS, Idx) of
                {ok, K, V} -> {[{K, V} | Acc], KVS};
                {error, end_of_kvs} -> {Acc, KVS}
            end
        end,
        {[], KVS2},
        lists:seq(0, 10)  % Try more indices than entries
    ),

    ?assertEqual(2, length(Results)).

%%%===================================================================
%%% Additional KVS Operations Tests
%%%===================================================================

%% Test put with binary key and list value
put_list_value_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, "list_value"),
    ?assertEqual({ok, <<"list_value">>}, flurm_pmi_kvs:get(KVS1, <<"key">>)).

%% Test multiple puts and gets with different key types
multiple_key_types_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"binary_key">>, <<"val1">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, "string_key", <<"val2">>),
    KVS3 = flurm_pmi_kvs:put(KVS2, atom_key, <<"val3">>),
    ?assertEqual({ok, <<"val1">>}, flurm_pmi_kvs:get(KVS3, <<"binary_key">>)),
    ?assertEqual({ok, <<"val2">>}, flurm_pmi_kvs:get(KVS3, <<"string_key">>)),
    ?assertEqual({ok, <<"val3">>}, flurm_pmi_kvs:get(KVS3, <<"atom_key">>)).

%% Test delete on nonexistent key (should be idempotent)
delete_nonexistent_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:delete(KVS0, <<"nonexistent">>),
    ?assertEqual(0, flurm_pmi_kvs:size(KVS1)).

%% Test get_by_index with very large index
get_by_index_large_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"key">>, <<"value">>),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_kvs:get_by_index(KVS1, 999999)).

%% Test merging empty KVS with non-empty
merge_empty_first_test() ->
    KVS1 = flurm_pmi_kvs:new(),
    KVS2 = flurm_pmi_kvs:new(#{<<"key">> => <<"value">>}),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(1, flurm_pmi_kvs:size(Merged)),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(Merged, <<"key">>)).

%% Test merging non-empty with empty KVS
merge_empty_second_test() ->
    KVS1 = flurm_pmi_kvs:new(#{<<"key">> => <<"value">>}),
    KVS2 = flurm_pmi_kvs:new(),
    Merged = flurm_pmi_kvs:merge(KVS1, KVS2),
    ?assertEqual(1, flurm_pmi_kvs:size(Merged)),
    ?assertEqual({ok, <<"value">>}, flurm_pmi_kvs:get(Merged, <<"key">>)).

%% Test keys returns sorted order
keys_sorted_test() ->
    KVS0 = flurm_pmi_kvs:new(),
    KVS1 = flurm_pmi_kvs:put(KVS0, <<"zzz">>, <<"1">>),
    KVS2 = flurm_pmi_kvs:put(KVS1, <<"aaa">>, <<"2">>),
    KVS3 = flurm_pmi_kvs:put(KVS2, <<"mmm">>, <<"3">>),
    Keys = flurm_pmi_kvs:keys(KVS3),
    ?assertEqual([<<"aaa">>, <<"mmm">>, <<"zzz">>], lists:sort(Keys)).
