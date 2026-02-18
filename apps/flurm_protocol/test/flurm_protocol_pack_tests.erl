%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit Tests for FLURM Protocol Pack Module
%%%
%%% Tests for all packing/unpacking functions with focus on:
%%% - Success cases
%%% - Error handling paths
%%% - Edge cases (empty inputs, boundary values)
%%% - Special SLURM values (NO_VAL, NO_VAL64)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_pack_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% String Packing Tests
%%%===================================================================

pack_string_test_() ->
    [
        {"pack undefined returns zero length", fun() ->
            Result = flurm_protocol_pack:pack_string(undefined),
            ?assertEqual(<<0:32/big>>, Result)
        end},

        {"pack null returns zero length", fun() ->
            Result = flurm_protocol_pack:pack_string(null),
            ?assertEqual(<<0:32/big>>, Result)
        end},

        {"pack empty binary returns length 1 with null", fun() ->
            Result = flurm_protocol_pack:pack_string(<<>>),
            ?assertEqual(<<1:32/big, 0:8>>, Result)
        end},

        {"pack normal string includes null terminator in length", fun() ->
            Result = flurm_protocol_pack:pack_string(<<"test">>),
            ?assertEqual(<<5:32/big, "test", 0:8>>, Result)
        end},

        {"pack list string", fun() ->
            Result = flurm_protocol_pack:pack_string("hello"),
            ?assertEqual(<<6:32/big, "hello", 0:8>>, Result)
        end},

        {"pack binary with embedded nulls", fun() ->
            Result = flurm_protocol_pack:pack_string(<<1, 0, 2, 0, 3>>),
            ?assertEqual(<<6:32/big, 1, 0, 2, 0, 3, 0:8>>, Result)
        end}
    ].

unpack_string_test_() ->
    [
        {"unpack NO_VAL returns undefined", fun() ->
            Binary = <<?SLURM_NO_VAL:32/big, "rest">>,
            {ok, Value, Rest} = flurm_protocol_pack:unpack_string(Binary),
            ?assertEqual(undefined, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack zero length returns undefined", fun() ->
            Binary = <<0:32/big, "rest">>,
            {ok, Value, Rest} = flurm_protocol_pack:unpack_string(Binary),
            %% Zero length indicates NULL pointer in SLURM protocol
            ?assertEqual(undefined, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack normal string strips null terminator", fun() ->
            Binary = <<5:32/big, "test", 0:8, "extra">>,
            {ok, Value, Rest} = flurm_protocol_pack:unpack_string(Binary),
            ?assertEqual(<<"test">>, Value),
            ?assertEqual(<<"extra">>, Rest)
        end},

        {"unpack insufficient data returns error", fun() ->
            Binary = <<100:32/big, "abc">>,
            Result = flurm_protocol_pack:unpack_string(Binary),
            ?assertMatch({error, {insufficient_string_data, 100}}, Result)
        end},

        {"unpack incomplete length returns error", fun() ->
            Binary = <<1, 2>>,
            Result = flurm_protocol_pack:unpack_string(Binary),
            ?assertMatch({error, {incomplete_string_length, 2}}, Result)
        end},

        {"unpack empty binary returns error", fun() ->
            Result = flurm_protocol_pack:unpack_string(<<>>),
            ?assertMatch({error, {incomplete_string_length, 0}}, Result)
        end}
    ].

string_roundtrip_test_() ->
    [
        {"undefined roundtrip", fun() ->
            Packed = flurm_protocol_pack:pack_string(undefined),
            {ok, Value, <<>>} = flurm_protocol_pack:unpack_string(Packed),
            ?assertEqual(undefined, Value)
        end},

        {"null roundtrip", fun() ->
            Packed = flurm_protocol_pack:pack_string(null),
            {ok, Value, <<>>} = flurm_protocol_pack:unpack_string(Packed),
            ?assertEqual(undefined, Value)
        end},

        {"empty string roundtrip", fun() ->
            Packed = flurm_protocol_pack:pack_string(<<>>),
            {ok, Value, <<>>} = flurm_protocol_pack:unpack_string(Packed),
            ?assertEqual(<<>>, Value)
        end},

        {"normal string roundtrip", fun() ->
            Packed = flurm_protocol_pack:pack_string(<<"hello world">>),
            {ok, Value, <<>>} = flurm_protocol_pack:unpack_string(Packed),
            ?assertEqual(<<"hello world">>, Value)
        end},

        {"unicode string roundtrip", fun() ->
            Str = unicode:characters_to_binary("cafe" ++ [233]),  % cafe with accent
            Packed = flurm_protocol_pack:pack_string(Str),
            {ok, Value, <<>>} = flurm_protocol_pack:unpack_string(Packed),
            ?assertEqual(Str, Value)
        end},

        {"long string roundtrip", fun() ->
            LongStr = list_to_binary(lists:duplicate(10000, $x)),
            Packed = flurm_protocol_pack:pack_string(LongStr),
            {ok, Value, <<>>} = flurm_protocol_pack:unpack_string(Packed),
            ?assertEqual(LongStr, Value)
        end}
    ].

%%%===================================================================
%%% Integer Packing Tests - uint8
%%%===================================================================

uint8_test_() ->
    [
        {"pack uint8 zero", fun() ->
            Result = flurm_protocol_pack:pack_uint8(0),
            ?assertEqual(<<0>>, Result)
        end},

        {"pack uint8 max", fun() ->
            Result = flurm_protocol_pack:pack_uint8(255),
            ?assertEqual(<<255>>, Result)
        end},

        {"pack uint8 mid value", fun() ->
            Result = flurm_protocol_pack:pack_uint8(127),
            ?assertEqual(<<127>>, Result)
        end},

        {"unpack uint8 success", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_uint8(<<42, "rest">>),
            ?assertEqual(42, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack uint8 empty returns error", fun() ->
            Result = flurm_protocol_pack:unpack_uint8(<<>>),
            ?assertMatch({error, incomplete_uint8}, Result)
        end},

        {"uint8 roundtrip", fun() ->
            lists:foreach(fun(V) ->
                Packed = flurm_protocol_pack:pack_uint8(V),
                {ok, Result, <<>>} = flurm_protocol_pack:unpack_uint8(Packed),
                ?assertEqual(V, Result)
            end, [0, 1, 127, 128, 254, 255])
        end}
    ].

%%%===================================================================
%%% Integer Packing Tests - uint16
%%%===================================================================

uint16_test_() ->
    [
        {"pack uint16 zero", fun() ->
            Result = flurm_protocol_pack:pack_uint16(0),
            ?assertEqual(<<0:16/big>>, Result)
        end},

        {"pack uint16 max", fun() ->
            Result = flurm_protocol_pack:pack_uint16(65535),
            ?assertEqual(<<65535:16/big>>, Result)
        end},

        {"pack uint16 is big endian", fun() ->
            Result = flurm_protocol_pack:pack_uint16(256),
            ?assertEqual(<<1, 0>>, Result)
        end},

        {"unpack uint16 success", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_uint16(<<1, 0, "rest">>),
            ?assertEqual(256, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack uint16 incomplete returns error", fun() ->
            Result = flurm_protocol_pack:unpack_uint16(<<1>>),
            ?assertMatch({error, {incomplete_uint16, 1}}, Result)
        end},

        {"unpack uint16 empty returns error", fun() ->
            Result = flurm_protocol_pack:unpack_uint16(<<>>),
            ?assertMatch({error, {incomplete_uint16, 0}}, Result)
        end},

        {"uint16 roundtrip", fun() ->
            lists:foreach(fun(V) ->
                Packed = flurm_protocol_pack:pack_uint16(V),
                {ok, Result, <<>>} = flurm_protocol_pack:unpack_uint16(Packed),
                ?assertEqual(V, Result)
            end, [0, 1, 255, 256, 32767, 32768, 65534, 65535])
        end}
    ].

%%%===================================================================
%%% Integer Packing Tests - uint32
%%%===================================================================

uint32_test_() ->
    [
        {"pack uint32 zero", fun() ->
            Result = flurm_protocol_pack:pack_uint32(0),
            ?assertEqual(<<0:32/big>>, Result)
        end},

        {"pack uint32 max", fun() ->
            Result = flurm_protocol_pack:pack_uint32(4294967295),
            ?assertEqual(<<255, 255, 255, 255>>, Result)
        end},

        {"pack uint32 undefined returns NO_VAL", fun() ->
            Result = flurm_protocol_pack:pack_uint32(undefined),
            ?assertEqual(<<?SLURM_NO_VAL:32/big>>, Result)
        end},

        {"pack uint32 is big endian", fun() ->
            Result = flurm_protocol_pack:pack_uint32(16#01020304),
            ?assertEqual(<<1, 2, 3, 4>>, Result)
        end},

        {"unpack uint32 success", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_uint32(<<0, 0, 1, 0, "rest">>),
            ?assertEqual(256, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack uint32 NO_VAL returns undefined", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_uint32(<<?SLURM_NO_VAL:32/big, "rest">>),
            ?assertEqual(undefined, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack uint32 incomplete returns error", fun() ->
            Result = flurm_protocol_pack:unpack_uint32(<<1, 2, 3>>),
            ?assertMatch({error, {incomplete_uint32, 3}}, Result)
        end},

        {"unpack uint32 empty returns error", fun() ->
            Result = flurm_protocol_pack:unpack_uint32(<<>>),
            ?assertMatch({error, {incomplete_uint32, 0}}, Result)
        end},

        {"uint32 roundtrip", fun() ->
            lists:foreach(fun(V) ->
                Packed = flurm_protocol_pack:pack_uint32(V),
                {ok, Result, <<>>} = flurm_protocol_pack:unpack_uint32(Packed),
                ?assertEqual(V, Result)
            %% Note: SLURM_NO_VAL is 16#FFFFFFFE, so skip both NO_VAL and max uint32
            end, [0, 1, 255, 65535, 16#FFFFFF, 16#FFFFFFFD])  % Skip NO_VAL (16#FFFFFFFE)
        end},

        {"uint32 undefined roundtrip", fun() ->
            Packed = flurm_protocol_pack:pack_uint32(undefined),
            {ok, Result, <<>>} = flurm_protocol_pack:unpack_uint32(Packed),
            ?assertEqual(undefined, Result)
        end}
    ].

%%%===================================================================
%%% Integer Packing Tests - uint64
%%%===================================================================

uint64_test_() ->
    [
        {"pack uint64 zero", fun() ->
            Result = flurm_protocol_pack:pack_uint64(0),
            ?assertEqual(<<0:64/big>>, Result)
        end},

        {"pack uint64 large value", fun() ->
            Result = flurm_protocol_pack:pack_uint64(16#0102030405060708),
            ?assertEqual(<<1, 2, 3, 4, 5, 6, 7, 8>>, Result)
        end},

        {"pack uint64 undefined returns NO_VAL64", fun() ->
            Result = flurm_protocol_pack:pack_uint64(undefined),
            ?assertEqual(<<?SLURM_NO_VAL64:64/big>>, Result)
        end},

        {"unpack uint64 success", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_uint64(<<0:64/big, "rest">>),
            ?assertEqual(0, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack uint64 NO_VAL64 returns undefined", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_uint64(<<?SLURM_NO_VAL64:64/big, "rest">>),
            ?assertEqual(undefined, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack uint64 incomplete returns error", fun() ->
            Result = flurm_protocol_pack:unpack_uint64(<<1, 2, 3, 4, 5, 6, 7>>),
            ?assertMatch({error, {incomplete_uint64, 7}}, Result)
        end},

        {"unpack uint64 empty returns error", fun() ->
            Result = flurm_protocol_pack:unpack_uint64(<<>>),
            ?assertMatch({error, {incomplete_uint64, 0}}, Result)
        end},

        {"uint64 roundtrip", fun() ->
            lists:foreach(fun(V) ->
                Packed = flurm_protocol_pack:pack_uint64(V),
                {ok, Result, <<>>} = flurm_protocol_pack:unpack_uint64(Packed),
                ?assertEqual(V, Result)
            end, [0, 1, 16#FFFFFFFF, 16#100000000, 16#FFFFFFFFFFFFFFFD])  % Skip NO_VAL64
        end}
    ].

%%%===================================================================
%%% Integer Packing Tests - int32 (signed)
%%%===================================================================

int32_test_() ->
    [
        {"pack int32 zero", fun() ->
            Result = flurm_protocol_pack:pack_int32(0),
            ?assertEqual(<<0:32/big-signed>>, Result)
        end},

        {"pack int32 positive", fun() ->
            Result = flurm_protocol_pack:pack_int32(2147483647),
            ?assertEqual(<<127, 255, 255, 255>>, Result)
        end},

        {"pack int32 negative", fun() ->
            Result = flurm_protocol_pack:pack_int32(-1),
            ?assertEqual(<<255, 255, 255, 255>>, Result)
        end},

        {"pack int32 min value", fun() ->
            Result = flurm_protocol_pack:pack_int32(-2147483648),
            ?assertEqual(<<128, 0, 0, 0>>, Result)
        end},

        {"unpack int32 positive", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_int32(<<0, 0, 0, 42, "rest">>),
            ?assertEqual(42, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack int32 negative", fun() ->
            {ok, Value, _Rest} = flurm_protocol_pack:unpack_int32(<<255, 255, 255, 255>>),
            ?assertEqual(-1, Value)
        end},

        {"unpack int32 min value", fun() ->
            {ok, Value, _Rest} = flurm_protocol_pack:unpack_int32(<<128, 0, 0, 0>>),
            ?assertEqual(-2147483648, Value)
        end},

        {"unpack int32 incomplete returns error", fun() ->
            Result = flurm_protocol_pack:unpack_int32(<<1, 2, 3>>),
            ?assertMatch({error, {incomplete_int32, 3}}, Result)
        end},

        {"unpack int32 empty returns error", fun() ->
            Result = flurm_protocol_pack:unpack_int32(<<>>),
            ?assertMatch({error, {incomplete_int32, 0}}, Result)
        end},

        {"int32 roundtrip", fun() ->
            lists:foreach(fun(V) ->
                Packed = flurm_protocol_pack:pack_int32(V),
                {ok, Result, <<>>} = flurm_protocol_pack:unpack_int32(Packed),
                ?assertEqual(V, Result)
            end, [-2147483648, -1, 0, 1, 2147483647])
        end}
    ].

%%%===================================================================
%%% Boolean Packing Tests
%%%===================================================================

bool_test_() ->
    [
        {"pack true", fun() ->
            Result = flurm_protocol_pack:pack_bool(true),
            ?assertEqual(<<1>>, Result)
        end},

        {"pack false", fun() ->
            Result = flurm_protocol_pack:pack_bool(false),
            ?assertEqual(<<0>>, Result)
        end},

        {"pack 1 as boolean", fun() ->
            Result = flurm_protocol_pack:pack_bool(1),
            ?assertEqual(<<1>>, Result)
        end},

        {"pack 0 as boolean", fun() ->
            Result = flurm_protocol_pack:pack_bool(0),
            ?assertEqual(<<0>>, Result)
        end},

        {"unpack 0 is false", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_bool(<<0, "rest">>),
            ?assertEqual(false, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack 1 is true", fun() ->
            {ok, Value, _Rest} = flurm_protocol_pack:unpack_bool(<<1>>),
            ?assertEqual(true, Value)
        end},

        {"unpack any non-zero is true", fun() ->
            {ok, Value1, _} = flurm_protocol_pack:unpack_bool(<<42>>),
            {ok, Value2, _} = flurm_protocol_pack:unpack_bool(<<255>>),
            ?assertEqual(true, Value1),
            ?assertEqual(true, Value2)
        end},

        {"unpack bool empty returns error", fun() ->
            Result = flurm_protocol_pack:unpack_bool(<<>>),
            ?assertMatch({error, incomplete_bool}, Result)
        end},

        {"bool roundtrip", fun() ->
            Packed1 = flurm_protocol_pack:pack_bool(true),
            {ok, true, <<>>} = flurm_protocol_pack:unpack_bool(Packed1),
            Packed2 = flurm_protocol_pack:pack_bool(false),
            {ok, false, <<>>} = flurm_protocol_pack:unpack_bool(Packed2)
        end}
    ].

%%%===================================================================
%%% Time Packing Tests
%%%===================================================================

time_test_() ->
    [
        {"pack time undefined returns NO_VAL64", fun() ->
            Result = flurm_protocol_pack:pack_time(undefined),
            ?assertEqual(<<?SLURM_NO_VAL64:64/big>>, Result)
        end},

        {"pack time zero", fun() ->
            Result = flurm_protocol_pack:pack_time(0),
            ?assertEqual(<<0:64/big>>, Result)
        end},

        {"pack normal timestamp", fun() ->
            Timestamp = 1704067200,
            Result = flurm_protocol_pack:pack_time(Timestamp),
            %% Verify it's a valid 64-bit big-endian encoding
            ?assertEqual(8, byte_size(Result)),
            %% Verify roundtrip
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_time(Result),
            ?assertEqual(Timestamp, Decoded)
        end},

        {"pack erlang timestamp tuple", fun() ->
            Result = flurm_protocol_pack:pack_time({1704, 67200, 123456}),
            Expected = 1704 * 1000000 + 67200,
            ?assertEqual(<<Expected:64/big>>, Result)
        end},

        {"unpack time NO_VAL64 returns undefined", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_time(<<?SLURM_NO_VAL64:64/big, "rest">>),
            ?assertEqual(undefined, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack time normal value", fun() ->
            {ok, Value, _Rest} = flurm_protocol_pack:unpack_time(<<0, 0, 0, 0, 0, 0, 0, 42>>),
            ?assertEqual(42, Value)
        end},

        {"unpack time incomplete returns error", fun() ->
            Result = flurm_protocol_pack:unpack_time(<<1, 2, 3, 4>>),
            ?assertMatch({error, {incomplete_timestamp, 4}}, Result)
        end},

        {"unpack time empty returns error", fun() ->
            Result = flurm_protocol_pack:unpack_time(<<>>),
            ?assertMatch({error, {incomplete_timestamp, 0}}, Result)
        end},

        {"time roundtrip", fun() ->
            lists:foreach(fun(T) ->
                Packed = flurm_protocol_pack:pack_time(T),
                {ok, Result, <<>>} = flurm_protocol_pack:unpack_time(Packed),
                ?assertEqual(T, Result)
            end, [0, 1, 1704067200, 16#FFFFFFFF])
        end},

        {"time undefined roundtrip", fun() ->
            Packed = flurm_protocol_pack:pack_time(undefined),
            {ok, Result, <<>>} = flurm_protocol_pack:unpack_time(Packed),
            ?assertEqual(undefined, Result)
        end}
    ].

%%%===================================================================
%%% Double/Float Packing Tests
%%%===================================================================

double_test_() ->
    [
        {"pack double zero", fun() ->
            Result = flurm_protocol_pack:pack_double(0.0),
            ?assertEqual(<<0:64/big-float>>, Result)
        end},

        {"pack double positive", fun() ->
            Result = flurm_protocol_pack:pack_double(3.14159265359),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_double(Result),
            ?assert(abs(3.14159265359 - Decoded) < 0.0000001)
        end},

        {"pack double negative", fun() ->
            Result = flurm_protocol_pack:pack_double(-123.456),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_double(Result),
            ?assert(abs(-123.456 - Decoded) < 0.001)
        end},

        {"pack integer as double", fun() ->
            Result = flurm_protocol_pack:pack_double(42),
            {ok, Decoded, <<>>} = flurm_protocol_pack:unpack_double(Result),
            ?assertEqual(42.0, Decoded)
        end},

        {"unpack double success", fun() ->
            Binary = <<64, 9, 33, 251, 84, 68, 45, 24>>,  % pi
            {ok, Value, <<>>} = flurm_protocol_pack:unpack_double(Binary),
            ?assert(abs(3.14159265359 - Value) < 0.0000001)
        end},

        {"unpack double incomplete returns error", fun() ->
            Result = flurm_protocol_pack:unpack_double(<<1, 2, 3, 4, 5, 6, 7>>),
            ?assertMatch({error, {incomplete_double, 7}}, Result)
        end},

        {"unpack double empty returns error", fun() ->
            Result = flurm_protocol_pack:unpack_double(<<>>),
            ?assertMatch({error, {incomplete_double, 0}}, Result)
        end},

        {"double roundtrip", fun() ->
            lists:foreach(fun(V) ->
                Packed = flurm_protocol_pack:pack_double(V),
                {ok, Result, <<>>} = flurm_protocol_pack:unpack_double(Packed),
                ?assert(abs(V - Result) < 0.0001)
            end, [0.0, 1.0, -1.0, 3.14159, -273.15, 1.0e10, 1.0e-10])
        end}
    ].

%%%===================================================================
%%% List Packing Tests
%%%===================================================================

list_test_() ->
    [
        {"pack empty list", fun() ->
            Result = flurm_protocol_pack:pack_list([], fun flurm_protocol_pack:pack_uint32/1),
            ?assertEqual(<<0:32/big>>, Result)
        end},

        {"pack list of uint32", fun() ->
            Result = flurm_protocol_pack:pack_list([1, 2, 3], fun flurm_protocol_pack:pack_uint32/1),
            Expected = <<3:32/big, 1:32/big, 2:32/big, 3:32/big>>,
            ?assertEqual(Expected, Result)
        end},

        {"unpack empty list", fun() ->
            {ok, List, Rest} = flurm_protocol_pack:unpack_list(<<0:32/big, "rest">>,
                                                               fun flurm_protocol_pack:unpack_uint32/1),
            ?assertEqual([], List),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack list of uint32", fun() ->
            Binary = <<3:32/big, 1:32/big, 2:32/big, 3:32/big, "rest">>,
            {ok, List, Rest} = flurm_protocol_pack:unpack_list(Binary,
                                                               fun flurm_protocol_pack:unpack_uint32/1),
            ?assertEqual([1, 2, 3], List),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack list incomplete count returns error", fun() ->
            Result = flurm_protocol_pack:unpack_list(<<1, 2>>,
                                                      fun flurm_protocol_pack:unpack_uint32/1),
            ?assertMatch({error, {incomplete_list_count, 2}}, Result)
        end},

        {"unpack list with element error propagates", fun() ->
            Binary = <<2:32/big, 1:32/big, 1, 2>>,  % Second element incomplete
            Result = flurm_protocol_pack:unpack_list(Binary,
                                                      fun flurm_protocol_pack:unpack_uint32/1),
            ?assertMatch({error, _}, Result)
        end},

        {"list roundtrip", fun() ->
            %% Note: Skip NO_VAL (16#FFFFFFFE) as it gets interpreted as undefined
            Lists = [[], [1], [1, 2, 3], [0, 16#FFFFFFFD]],
            lists:foreach(fun(L) ->
                Packed = flurm_protocol_pack:pack_list(L, fun flurm_protocol_pack:pack_uint32/1),
                {ok, Result, <<>>} = flurm_protocol_pack:unpack_list(Packed,
                                                                     fun flurm_protocol_pack:unpack_uint32/1),
                ?assertEqual(L, Result)
            end, Lists)
        end}
    ].

%%%===================================================================
%%% String Array Packing Tests
%%%===================================================================

%%%===================================================================
%%% Memory (packmem) Packing Tests - Coverage for lines 119-137
%%%===================================================================

mem_test_() ->
    [
        {"pack_mem undefined returns zero length", fun() ->
            Result = flurm_protocol_pack:pack_mem(undefined),
            ?assertEqual(<<0:32/big>>, Result)
        end},

        {"pack_mem empty binary returns zero length", fun() ->
            Result = flurm_protocol_pack:pack_mem(<<>>),
            ?assertEqual(<<0:32/big>>, Result)
        end},

        {"pack_mem normal binary", fun() ->
            Result = flurm_protocol_pack:pack_mem(<<"hello">>),
            ?assertEqual(<<5:32/big, "hello">>, Result)
        end},

        {"unpack_mem zero length returns empty binary", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_mem(<<0:32/big, "rest">>),
            ?assertEqual(<<>>, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack_mem normal data", fun() ->
            {ok, Value, Rest} = flurm_protocol_pack:unpack_mem(<<5:32/big, "hello", "rest">>),
            ?assertEqual(<<"hello">>, Value),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack_mem insufficient data returns error", fun() ->
            Result = flurm_protocol_pack:unpack_mem(<<10:32/big, "abc">>),
            ?assertMatch({error, {insufficient_mem_data, 10}}, Result)
        end},

        {"unpack_mem incomplete length returns error", fun() ->
            Result = flurm_protocol_pack:unpack_mem(<<1, 2>>),
            ?assertMatch({error, {incomplete_mem_length, 2}}, Result)
        end},

        {"unpack_mem invalid data returns error", fun() ->
            %% Not a binary at all - triggers the catch-all clause
            Result = flurm_protocol_pack:unpack_mem(not_a_binary),
            ?assertMatch({error, invalid_mem_data}, Result)
        end},

        {"mem roundtrip", fun() ->
            lists:foreach(fun(V) ->
                Packed = flurm_protocol_pack:pack_mem(V),
                {ok, Result, <<>>} = flurm_protocol_pack:unpack_mem(Packed),
                Expected = case V of undefined -> <<>>; _ -> V end,
                ?assertEqual(Expected, Result)
            end, [undefined, <<>>, <<"test">>, <<1,2,3,4,5>>])
        end}
    ].

%%%===================================================================
%%% Invalid Data Error Path Tests - Coverage for catch-all clauses
%%%===================================================================

invalid_data_test_() ->
    [
        {"unpack_string invalid data", fun() ->
            Result = flurm_protocol_pack:unpack_string(not_a_binary),
            ?assertMatch({error, invalid_string_data}, Result)
        end},

        {"unpack_list invalid data", fun() ->
            Result = flurm_protocol_pack:unpack_list(not_a_binary, fun flurm_protocol_pack:unpack_uint32/1),
            ?assertMatch({error, invalid_list_data}, Result)
        end},

        {"unpack_time invalid data", fun() ->
            Result = flurm_protocol_pack:unpack_time(not_a_binary),
            ?assertMatch({error, invalid_timestamp_data}, Result)
        end},

        {"unpack_uint8 invalid data", fun() ->
            Result = flurm_protocol_pack:unpack_uint8(not_a_binary),
            ?assertMatch({error, invalid_uint8_data}, Result)
        end},

        {"unpack_uint16 invalid data", fun() ->
            Result = flurm_protocol_pack:unpack_uint16(not_a_binary),
            ?assertMatch({error, invalid_uint16_data}, Result)
        end},

        {"unpack_uint32 invalid data", fun() ->
            Result = flurm_protocol_pack:unpack_uint32(not_a_binary),
            ?assertMatch({error, invalid_uint32_data}, Result)
        end},

        {"unpack_uint64 invalid data", fun() ->
            Result = flurm_protocol_pack:unpack_uint64(not_a_binary),
            ?assertMatch({error, invalid_uint64_data}, Result)
        end},

        {"unpack_int32 invalid data", fun() ->
            Result = flurm_protocol_pack:unpack_int32(not_a_binary),
            ?assertMatch({error, invalid_int32_data}, Result)
        end},

        {"unpack_bool invalid data", fun() ->
            Result = flurm_protocol_pack:unpack_bool(not_a_binary),
            ?assertMatch({error, invalid_bool_data}, Result)
        end},

        {"unpack_double invalid data", fun() ->
            Result = flurm_protocol_pack:unpack_double(not_a_binary),
            ?assertMatch({error, invalid_double_data}, Result)
        end}
    ].

string_array_test_() ->
    [
        {"pack empty string array", fun() ->
            Result = flurm_protocol_pack:pack_string_array([]),
            ?assertEqual(<<0:32/big>>, Result)
        end},

        {"pack string array with elements", fun() ->
            Result = flurm_protocol_pack:pack_string_array([<<"a">>, <<"bb">>]),
            Expected = <<2:32/big,
                         2:32/big, "a", 0,
                         3:32/big, "bb", 0>>,
            ?assertEqual(Expected, Result)
        end},

        {"unpack empty string array", fun() ->
            {ok, List, Rest} = flurm_protocol_pack:unpack_string_array(<<0:32/big, "rest">>),
            ?assertEqual([], List),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"unpack string array with elements", fun() ->
            Binary = <<2:32/big,
                       2:32/big, "a", 0,
                       3:32/big, "bb", 0,
                       "rest">>,
            {ok, List, Rest} = flurm_protocol_pack:unpack_string_array(Binary),
            ?assertEqual([<<"a">>, <<"bb">>], List),
            ?assertEqual(<<"rest">>, Rest)
        end},

        {"string array roundtrip", fun() ->
            Arrays = [
                [],
                [<<"single">>],
                [<<"one">>, <<"two">>, <<"three">>],
                [<<"">>, <<"with spaces">>, <<"!@#$%">>]
            ],
            lists:foreach(fun(A) ->
                Packed = flurm_protocol_pack:pack_string_array(A),
                {ok, Result, <<>>} = flurm_protocol_pack:unpack_string_array(Packed),
                ?assertEqual(A, Result)
            end, Arrays)
        end}
    ].
