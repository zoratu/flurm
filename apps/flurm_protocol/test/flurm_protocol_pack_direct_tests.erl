%%%-------------------------------------------------------------------
%%% @doc Direct unit tests for flurm_protocol_pack module.
%%%
%%% These tests call actual module functions directly (no mocking)
%%% to achieve code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_pack_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Fixtures
%%%===================================================================

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Test Generators
%%%===================================================================

flurm_protocol_pack_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% String packing tests
      {"pack_string with binary", fun pack_string_binary_test/0},
      {"pack_string with undefined", fun pack_string_undefined_test/0},
      {"pack_string with null", fun pack_string_null_test/0},
      {"pack_string with empty", fun pack_string_empty_test/0},
      {"pack_string with list", fun pack_string_list_test/0},
      {"unpack_string success", fun unpack_string_success_test/0},
      {"unpack_string errors", fun unpack_string_errors_test/0},
      {"string roundtrip", fun string_roundtrip_test/0},

      %% List packing tests
      {"pack_list", fun pack_list_test/0},
      {"unpack_list success", fun unpack_list_success_test/0},
      {"unpack_list errors", fun unpack_list_errors_test/0},
      {"list roundtrip", fun list_roundtrip_test/0},

      %% Time packing tests
      {"pack_time", fun pack_time_test/0},
      {"unpack_time", fun unpack_time_test/0},
      {"time roundtrip", fun time_roundtrip_test/0},

      %% Integer packing tests
      {"pack/unpack uint8", fun uint8_test/0},
      {"pack/unpack uint16", fun uint16_test/0},
      {"pack/unpack uint32", fun uint32_test/0},
      {"pack/unpack uint64", fun uint64_test/0},
      {"pack/unpack int32", fun int32_test/0},
      {"integer errors", fun integer_errors_test/0},

      %% Boolean packing tests
      {"pack/unpack bool", fun bool_test/0},
      {"bool errors", fun bool_errors_test/0},

      %% Float packing tests
      {"pack/unpack double", fun double_test/0},
      {"double errors", fun double_errors_test/0},

      %% String array packing tests
      {"pack/unpack string_array", fun string_array_test/0}
     ]}.

%%%===================================================================
%%% String Packing Tests
%%%===================================================================

pack_string_binary_test() ->
    %% Test packing a normal string
    Bin = <<"hello">>,
    Packed = flurm_protocol_pack:pack_string(Bin),
    %% Length should be strlen + 1 (for null terminator)
    ExpectedLen = byte_size(Bin) + 1,
    <<Len:32/big, Data/binary>> = Packed,
    ?assertEqual(ExpectedLen, Len),
    ?assertEqual(<<Bin/binary, 0:8>>, Data),
    ok.

pack_string_undefined_test() ->
    %% Test packing undefined (NULL string)
    Packed = flurm_protocol_pack:pack_string(undefined),
    ?assertEqual(<<0:32/big>>, Packed),
    ok.

pack_string_null_test() ->
    %% Test packing null atom (NULL string)
    Packed = flurm_protocol_pack:pack_string(null),
    ?assertEqual(<<0:32/big>>, Packed),
    ok.

pack_string_empty_test() ->
    %% Test packing empty binary
    Packed = flurm_protocol_pack:pack_string(<<>>),
    %% Empty string has length 1 (just the null terminator)
    ?assertEqual(<<1:32/big, 0:8>>, Packed),
    ok.

pack_string_list_test() ->
    %% Test packing a list (converts to binary)
    List = "hello",
    Packed = flurm_protocol_pack:pack_string(List),
    ExpectedLen = length(List) + 1,
    <<Len:32/big, _/binary>> = Packed,
    ?assertEqual(ExpectedLen, Len),
    ok.

unpack_string_success_test() ->
    %% Test unpacking a normal string
    Str = <<"world">>,
    StrLen = byte_size(Str) + 1,
    Packed = <<StrLen:32/big, Str/binary, 0:8, "extra">>,
    {ok, Unpacked, Rest} = flurm_protocol_pack:unpack_string(Packed),
    ?assertEqual(Str, Unpacked),
    ?assertEqual(<<"extra">>, Rest),

    %% Test unpacking zero-length string (undefined)
    ZeroPacked = <<0:32/big, "rest">>,
    {ok, ZeroUnpacked, ZeroRest} = flurm_protocol_pack:unpack_string(ZeroPacked),
    ?assertEqual(undefined, ZeroUnpacked),
    ?assertEqual(<<"rest">>, ZeroRest),

    %% Test unpacking NO_VAL string
    NoValPacked = <<?SLURM_NO_VAL:32/big, "rest">>,
    {ok, NoValUnpacked, NoValRest} = flurm_protocol_pack:unpack_string(NoValPacked),
    ?assertEqual(undefined, NoValUnpacked),
    ?assertEqual(<<"rest">>, NoValRest),
    ok.

unpack_string_errors_test() ->
    %% Test insufficient data for length
    Result1 = flurm_protocol_pack:unpack_string(<<1, 2, 3>>),
    ?assertMatch({error, {incomplete_string_length, _}}, Result1),

    %% Test insufficient string data
    Result2 = flurm_protocol_pack:unpack_string(<<100:32/big, "short">>),
    ?assertMatch({error, {insufficient_string_data, _}}, Result2),

    %% Test invalid data
    Result3 = flurm_protocol_pack:unpack_string(not_binary),
    ?assertMatch({error, invalid_string_data}, Result3),
    ok.

string_roundtrip_test() ->
    %% Test roundtrip for various strings
    Strings = [
        <<"hello">>,
        <<"a">>,
        <<"longer string with spaces">>,
        <<"unicode: cafe"/utf8>>
    ],
    lists:foreach(fun(Str) ->
        Packed = flurm_protocol_pack:pack_string(Str),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_string(Packed),
        ?assertEqual(Str, Unpacked)
    end, Strings),

    %% Test empty string - pack creates a string with just null terminator
    %% unpack returns the empty binary
    PackedEmpty = flurm_protocol_pack:pack_string(<<>>),
    {ok, UnpackedEmpty, <<>>} = flurm_protocol_pack:unpack_string(PackedEmpty),
    %% Empty string packs as length=1 (just null), unpacks as <<>>
    ?assertEqual(<<>>, UnpackedEmpty),

    %% Test undefined roundtrip
    PackedUndef = flurm_protocol_pack:pack_string(undefined),
    {ok, UnpackedUndef, <<>>} = flurm_protocol_pack:unpack_string(PackedUndef),
    ?assertEqual(undefined, UnpackedUndef),
    ok.

%%%===================================================================
%%% List Packing Tests
%%%===================================================================

pack_list_test() ->
    %% Test packing a list of integers
    List = [1, 2, 3],
    PackFun = fun(N) -> <<N:32/big>> end,
    Packed = flurm_protocol_pack:pack_list(List, PackFun),
    <<Count:32/big, _/binary>> = Packed,
    ?assertEqual(3, Count),

    %% Test packing empty list
    EmptyPacked = flurm_protocol_pack:pack_list([], PackFun),
    <<EmptyCount:32/big>> = EmptyPacked,
    ?assertEqual(0, EmptyCount),
    ok.

unpack_list_success_test() ->
    %% Test unpacking a list of uint32s
    UnpackFun = fun(<<V:32/big, Rest/binary>>) -> {ok, V, Rest} end,
    ListData = <<3:32/big, 10:32/big, 20:32/big, 30:32/big, "rest">>,
    {ok, Unpacked, Rest} = flurm_protocol_pack:unpack_list(ListData, UnpackFun),
    ?assertEqual([10, 20, 30], Unpacked),
    ?assertEqual(<<"rest">>, Rest),

    %% Test unpacking empty list
    EmptyData = <<0:32/big, "rest">>,
    {ok, EmptyUnpacked, EmptyRest} = flurm_protocol_pack:unpack_list(EmptyData, UnpackFun),
    ?assertEqual([], EmptyUnpacked),
    ?assertEqual(<<"rest">>, EmptyRest),
    ok.

unpack_list_errors_test() ->
    UnpackFun = fun(<<V:32/big, Rest/binary>>) -> {ok, V, Rest} end,

    %% Test insufficient data for count
    Result1 = flurm_protocol_pack:unpack_list(<<1, 2, 3>>, UnpackFun),
    ?assertMatch({error, {incomplete_list_count, _}}, Result1),

    %% Test invalid data
    Result2 = flurm_protocol_pack:unpack_list(not_binary, UnpackFun),
    ?assertMatch({error, invalid_list_data}, Result2),

    %% Test unpack function returning error
    ErrorUnpackFun = fun(_) -> {error, test_error} end,
    ErrorData = <<1:32/big, 1, 2, 3, 4>>,
    Result3 = flurm_protocol_pack:unpack_list(ErrorData, ErrorUnpackFun),
    ?assertMatch({error, test_error}, Result3),
    ok.

list_roundtrip_test() ->
    List = [<<"one">>, <<"two">>, <<"three">>],
    PackFun = fun(S) -> flurm_protocol_pack:pack_string(S) end,
    UnpackFun = fun(Bin) -> flurm_protocol_pack:unpack_string(Bin) end,

    Packed = flurm_protocol_pack:pack_list(List, PackFun),
    {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_list(Packed, UnpackFun),
    ?assertEqual(List, Unpacked),
    ok.

%%%===================================================================
%%% Time Packing Tests
%%%===================================================================

pack_time_test() ->
    %% Test packing a positive timestamp
    Time1 = 1609459200,
    Packed1 = flurm_protocol_pack:pack_time(Time1),
    ?assertEqual(<<Time1:64/big>>, Packed1),

    %% Test packing zero
    Packed2 = flurm_protocol_pack:pack_time(0),
    ?assertEqual(<<0:64/big>>, Packed2),

    %% Test packing undefined (NO_VAL64)
    Packed3 = flurm_protocol_pack:pack_time(undefined),
    ?assertEqual(<<?SLURM_NO_VAL64:64/big>>, Packed3),

    %% Test packing erlang timestamp tuple
    ErlTs = {1609, 459200, 123456},
    Packed4 = flurm_protocol_pack:pack_time(ErlTs),
    ExpectedTime = 1609 * 1000000 + 459200,
    ?assertEqual(<<ExpectedTime:64/big>>, Packed4),
    ok.

unpack_time_test() ->
    %% Test unpacking normal timestamp
    Time1 = 1609459200,
    Packed1 = <<Time1:64/big, "rest">>,
    {ok, Unpacked1, Rest1} = flurm_protocol_pack:unpack_time(Packed1),
    ?assertEqual(Time1, Unpacked1),
    ?assertEqual(<<"rest">>, Rest1),

    %% Test unpacking NO_VAL64 (undefined)
    Packed2 = <<?SLURM_NO_VAL64:64/big, "rest">>,
    {ok, Unpacked2, Rest2} = flurm_protocol_pack:unpack_time(Packed2),
    ?assertEqual(undefined, Unpacked2),
    ?assertEqual(<<"rest">>, Rest2),

    %% Test error cases
    Result1 = flurm_protocol_pack:unpack_time(<<1, 2, 3>>),
    ?assertMatch({error, {incomplete_timestamp, _}}, Result1),

    Result2 = flurm_protocol_pack:unpack_time(not_binary),
    ?assertMatch({error, invalid_timestamp_data}, Result2),
    ok.

time_roundtrip_test() ->
    Times = [0, 1, 1609459200, 2147483647],
    lists:foreach(fun(T) ->
        Packed = flurm_protocol_pack:pack_time(T),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_time(Packed),
        ?assertEqual(T, Unpacked)
    end, Times),

    %% Test undefined roundtrip
    PackedUndef = flurm_protocol_pack:pack_time(undefined),
    {ok, UnpackedUndef, <<>>} = flurm_protocol_pack:unpack_time(PackedUndef),
    ?assertEqual(undefined, UnpackedUndef),
    ok.

%%%===================================================================
%%% Integer Packing Tests
%%%===================================================================

uint8_test() ->
    %% Test pack/unpack uint8
    Values = [0, 1, 127, 255],
    lists:foreach(fun(V) ->
        Packed = flurm_protocol_pack:pack_uint8(V),
        ?assertEqual(<<V:8>>, Packed),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_uint8(Packed),
        ?assertEqual(V, Unpacked)
    end, Values),

    %% Test with extra data
    {ok, Val, Rest} = flurm_protocol_pack:unpack_uint8(<<42:8, "extra">>),
    ?assertEqual(42, Val),
    ?assertEqual(<<"extra">>, Rest),
    ok.

uint16_test() ->
    %% Test pack/unpack uint16
    Values = [0, 1, 255, 256, 32767, 65535],
    lists:foreach(fun(V) ->
        Packed = flurm_protocol_pack:pack_uint16(V),
        ?assertEqual(<<V:16/big>>, Packed),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_uint16(Packed),
        ?assertEqual(V, Unpacked)
    end, Values),

    %% Test with extra data
    {ok, Val, Rest} = flurm_protocol_pack:unpack_uint16(<<1000:16/big, "extra">>),
    ?assertEqual(1000, Val),
    ?assertEqual(<<"extra">>, Rest),
    ok.

uint32_test() ->
    %% Test pack/unpack uint32
    Values = [0, 1, 255, 65535, 16777215, 4294967295],
    lists:foreach(fun(V) ->
        Packed = flurm_protocol_pack:pack_uint32(V),
        ?assertEqual(<<V:32/big>>, Packed),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_uint32(Packed),
        ?assertEqual(V, Unpacked)
    end, Values),

    %% Test undefined (NO_VAL)
    PackedUndef = flurm_protocol_pack:pack_uint32(undefined),
    ?assertEqual(<<?SLURM_NO_VAL:32/big>>, PackedUndef),
    {ok, UnpackedUndef, <<>>} = flurm_protocol_pack:unpack_uint32(PackedUndef),
    ?assertEqual(undefined, UnpackedUndef),

    %% Test with extra data
    {ok, Val, Rest} = flurm_protocol_pack:unpack_uint32(<<100000:32/big, "extra">>),
    ?assertEqual(100000, Val),
    ?assertEqual(<<"extra">>, Rest),
    ok.

uint64_test() ->
    %% Test pack/unpack uint64
    Values = [0, 1, 4294967295, 4294967296, 9223372036854775807],
    lists:foreach(fun(V) ->
        Packed = flurm_protocol_pack:pack_uint64(V),
        ?assertEqual(<<V:64/big>>, Packed),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_uint64(Packed),
        ?assertEqual(V, Unpacked)
    end, Values),

    %% Test undefined (NO_VAL64)
    PackedUndef = flurm_protocol_pack:pack_uint64(undefined),
    ?assertEqual(<<?SLURM_NO_VAL64:64/big>>, PackedUndef),
    {ok, UnpackedUndef, <<>>} = flurm_protocol_pack:unpack_uint64(PackedUndef),
    ?assertEqual(undefined, UnpackedUndef),

    %% Test with extra data
    {ok, Val, Rest} = flurm_protocol_pack:unpack_uint64(<<100000000000:64/big, "extra">>),
    ?assertEqual(100000000000, Val),
    ?assertEqual(<<"extra">>, Rest),
    ok.

int32_test() ->
    %% Test pack/unpack int32 (signed)
    Values = [0, 1, -1, 2147483647, -2147483648],
    lists:foreach(fun(V) ->
        Packed = flurm_protocol_pack:pack_int32(V),
        ?assertEqual(<<V:32/big-signed>>, Packed),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_int32(Packed),
        ?assertEqual(V, Unpacked)
    end, Values),

    %% Test with extra data
    {ok, Val, Rest} = flurm_protocol_pack:unpack_int32(<<-100:32/big-signed, "extra">>),
    ?assertEqual(-100, Val),
    ?assertEqual(<<"extra">>, Rest),
    ok.

integer_errors_test() ->
    %% Test uint8 errors
    Result1 = flurm_protocol_pack:unpack_uint8(<<>>),
    ?assertMatch({error, incomplete_uint8}, Result1),
    Result2 = flurm_protocol_pack:unpack_uint8(not_binary),
    ?assertMatch({error, invalid_uint8_data}, Result2),

    %% Test uint16 errors
    Result3 = flurm_protocol_pack:unpack_uint16(<<1>>),
    ?assertMatch({error, {incomplete_uint16, _}}, Result3),
    Result4 = flurm_protocol_pack:unpack_uint16(not_binary),
    ?assertMatch({error, invalid_uint16_data}, Result4),

    %% Test uint32 errors
    Result5 = flurm_protocol_pack:unpack_uint32(<<1, 2, 3>>),
    ?assertMatch({error, {incomplete_uint32, _}}, Result5),
    Result6 = flurm_protocol_pack:unpack_uint32(not_binary),
    ?assertMatch({error, invalid_uint32_data}, Result6),

    %% Test uint64 errors
    Result7 = flurm_protocol_pack:unpack_uint64(<<1, 2, 3, 4, 5, 6, 7>>),
    ?assertMatch({error, {incomplete_uint64, _}}, Result7),
    Result8 = flurm_protocol_pack:unpack_uint64(not_binary),
    ?assertMatch({error, invalid_uint64_data}, Result8),

    %% Test int32 errors
    Result9 = flurm_protocol_pack:unpack_int32(<<1, 2, 3>>),
    ?assertMatch({error, {incomplete_int32, _}}, Result9),
    Result10 = flurm_protocol_pack:unpack_int32(not_binary),
    ?assertMatch({error, invalid_int32_data}, Result10),
    ok.

%%%===================================================================
%%% Boolean Packing Tests
%%%===================================================================

bool_test() ->
    %% Test true
    PackedTrue = flurm_protocol_pack:pack_bool(true),
    ?assertEqual(<<1:8>>, PackedTrue),
    {ok, UnpackedTrue, <<>>} = flurm_protocol_pack:unpack_bool(PackedTrue),
    ?assertEqual(true, UnpackedTrue),

    %% Test false
    PackedFalse = flurm_protocol_pack:pack_bool(false),
    ?assertEqual(<<0:8>>, PackedFalse),
    {ok, UnpackedFalse, <<>>} = flurm_protocol_pack:unpack_bool(PackedFalse),
    ?assertEqual(false, UnpackedFalse),

    %% Test integer 1
    Packed1 = flurm_protocol_pack:pack_bool(1),
    ?assertEqual(<<1:8>>, Packed1),

    %% Test integer 0
    Packed0 = flurm_protocol_pack:pack_bool(0),
    ?assertEqual(<<0:8>>, Packed0),

    %% Test unpacking non-zero as true
    {ok, NonZeroResult, <<>>} = flurm_protocol_pack:unpack_bool(<<255:8>>),
    ?assertEqual(true, NonZeroResult),

    %% Test with extra data
    {ok, Val, Rest} = flurm_protocol_pack:unpack_bool(<<1:8, "extra">>),
    ?assertEqual(true, Val),
    ?assertEqual(<<"extra">>, Rest),
    ok.

bool_errors_test() ->
    %% Test empty binary
    Result1 = flurm_protocol_pack:unpack_bool(<<>>),
    ?assertMatch({error, incomplete_bool}, Result1),

    %% Test invalid data
    Result2 = flurm_protocol_pack:unpack_bool(not_binary),
    ?assertMatch({error, invalid_bool_data}, Result2),
    ok.

%%%===================================================================
%%% Float Packing Tests
%%%===================================================================

double_test() ->
    %% Test various float values
    Values = [0.0, 1.0, -1.0, 3.14159, 1.0e10, -1.0e-10],
    lists:foreach(fun(V) ->
        Packed = flurm_protocol_pack:pack_double(V),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_double(Packed),
        ?assertEqual(V, Unpacked)
    end, Values),

    %% Test integer conversion to float
    PackedInt = flurm_protocol_pack:pack_double(42),
    {ok, UnpackedInt, <<>>} = flurm_protocol_pack:unpack_double(PackedInt),
    ?assertEqual(42.0, UnpackedInt),

    %% Test with extra data
    {ok, Val, Rest} = flurm_protocol_pack:unpack_double(<<1.5:64/big-float, "extra">>),
    ?assertEqual(1.5, Val),
    ?assertEqual(<<"extra">>, Rest),
    ok.

double_errors_test() ->
    %% Test incomplete data
    Result1 = flurm_protocol_pack:unpack_double(<<1, 2, 3, 4, 5, 6, 7>>),
    ?assertMatch({error, {incomplete_double, _}}, Result1),

    %% Test invalid data
    Result2 = flurm_protocol_pack:unpack_double(not_binary),
    ?assertMatch({error, invalid_double_data}, Result2),
    ok.

%%%===================================================================
%%% String Array Packing Tests
%%%===================================================================

string_array_test() ->
    %% Test packing an array of strings
    Strings = [<<"one">>, <<"two">>, <<"three">>],
    Packed = flurm_protocol_pack:pack_string_array(Strings),

    %% Verify count
    <<Count:32/big, _/binary>> = Packed,
    ?assertEqual(3, Count),

    %% Test roundtrip
    {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_string_array(Packed),
    ?assertEqual(Strings, Unpacked),

    %% Test empty array
    EmptyPacked = flurm_protocol_pack:pack_string_array([]),
    {ok, EmptyUnpacked, <<>>} = flurm_protocol_pack:unpack_string_array(EmptyPacked),
    ?assertEqual([], EmptyUnpacked),

    %% Test single element array
    SinglePacked = flurm_protocol_pack:pack_string_array([<<"single">>]),
    {ok, SingleUnpacked, <<>>} = flurm_protocol_pack:unpack_string_array(SinglePacked),
    ?assertEqual([<<"single">>], SingleUnpacked),
    ok.
