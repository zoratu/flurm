%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_protocol_pack module
%%% Coverage target: 100% of all functions and branches
%%%-------------------------------------------------------------------
-module(flurm_protocol_pack_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Generator
%%%===================================================================

flurm_protocol_pack_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% pack_string tests
      {"pack_string undefined",
       fun pack_string_undefined/0},
      {"pack_string null",
       fun pack_string_null/0},
      {"pack_string empty binary",
       fun pack_string_empty_binary/0},
      {"pack_string binary",
       fun pack_string_binary/0},
      {"pack_string list",
       fun pack_string_list/0},

      %% unpack_string tests
      {"unpack_string NO_VAL",
       fun unpack_string_no_val/0},
      {"unpack_string zero length",
       fun unpack_string_zero_length/0},
      {"unpack_string normal",
       fun unpack_string_normal/0},
      {"unpack_string insufficient data",
       fun unpack_string_insufficient/0},
      {"unpack_string incomplete length",
       fun unpack_string_incomplete_length/0},
      {"unpack_string invalid",
       fun unpack_string_invalid/0},

      %% pack_mem tests
      {"pack_mem undefined",
       fun pack_mem_undefined/0},
      {"pack_mem empty",
       fun pack_mem_empty/0},
      {"pack_mem binary",
       fun pack_mem_binary/0},

      %% unpack_mem tests
      {"unpack_mem zero length",
       fun unpack_mem_zero_length/0},
      {"unpack_mem normal",
       fun unpack_mem_normal/0},
      {"unpack_mem insufficient",
       fun unpack_mem_insufficient/0},
      {"unpack_mem incomplete length",
       fun unpack_mem_incomplete_length/0},
      {"unpack_mem invalid",
       fun unpack_mem_invalid/0},

      %% pack_list tests
      {"pack_list empty",
       fun pack_list_empty/0},
      {"pack_list single element",
       fun pack_list_single/0},
      {"pack_list multiple elements",
       fun pack_list_multiple/0},

      %% unpack_list tests
      {"unpack_list empty",
       fun unpack_list_empty/0},
      {"unpack_list single",
       fun unpack_list_single/0},
      {"unpack_list multiple",
       fun unpack_list_multiple/0},
      {"unpack_list incomplete count",
       fun unpack_list_incomplete_count/0},
      {"unpack_list invalid",
       fun unpack_list_invalid/0},
      {"unpack_list element error",
       fun unpack_list_element_error/0},

      %% pack_time tests
      {"pack_time undefined",
       fun pack_time_undefined/0},
      {"pack_time zero",
       fun pack_time_zero/0},
      {"pack_time tuple",
       fun pack_time_tuple/0},
      {"pack_time integer",
       fun pack_time_integer/0},

      %% unpack_time tests
      {"unpack_time NO_VAL64",
       fun unpack_time_no_val64/0},
      {"unpack_time normal",
       fun unpack_time_normal/0},
      {"unpack_time incomplete",
       fun unpack_time_incomplete/0},
      {"unpack_time invalid",
       fun unpack_time_invalid/0},

      %% pack_uint8 tests
      {"pack_uint8 zero",
       fun pack_uint8_zero/0},
      {"pack_uint8 value",
       fun pack_uint8_value/0},
      {"pack_uint8 max",
       fun pack_uint8_max/0},

      %% unpack_uint8 tests
      {"unpack_uint8 normal",
       fun unpack_uint8_normal/0},
      {"unpack_uint8 empty",
       fun unpack_uint8_empty/0},
      {"unpack_uint8 invalid",
       fun unpack_uint8_invalid/0},

      %% pack_uint16 tests
      {"pack_uint16 zero",
       fun pack_uint16_zero/0},
      {"pack_uint16 value",
       fun pack_uint16_value/0},
      {"pack_uint16 max",
       fun pack_uint16_max/0},

      %% unpack_uint16 tests
      {"unpack_uint16 normal",
       fun unpack_uint16_normal/0},
      {"unpack_uint16 incomplete",
       fun unpack_uint16_incomplete/0},
      {"unpack_uint16 invalid",
       fun unpack_uint16_invalid/0},

      %% pack_uint32 tests
      {"pack_uint32 undefined",
       fun pack_uint32_undefined/0},
      {"pack_uint32 zero",
       fun pack_uint32_zero/0},
      {"pack_uint32 value",
       fun pack_uint32_value/0},
      {"pack_uint32 max",
       fun pack_uint32_max/0},

      %% unpack_uint32 tests
      {"unpack_uint32 NO_VAL",
       fun unpack_uint32_no_val/0},
      {"unpack_uint32 normal",
       fun unpack_uint32_normal/0},
      {"unpack_uint32 incomplete",
       fun unpack_uint32_incomplete/0},
      {"unpack_uint32 invalid",
       fun unpack_uint32_invalid/0},

      %% pack_uint64 tests
      {"pack_uint64 undefined",
       fun pack_uint64_undefined/0},
      {"pack_uint64 zero",
       fun pack_uint64_zero/0},
      {"pack_uint64 value",
       fun pack_uint64_value/0},
      {"pack_uint64 max",
       fun pack_uint64_max/0},

      %% unpack_uint64 tests
      {"unpack_uint64 NO_VAL64",
       fun unpack_uint64_no_val64/0},
      {"unpack_uint64 normal",
       fun unpack_uint64_normal/0},
      {"unpack_uint64 incomplete",
       fun unpack_uint64_incomplete/0},
      {"unpack_uint64 invalid",
       fun unpack_uint64_invalid/0},

      %% pack_int32 tests
      {"pack_int32 zero",
       fun pack_int32_zero/0},
      {"pack_int32 positive",
       fun pack_int32_positive/0},
      {"pack_int32 negative",
       fun pack_int32_negative/0},
      {"pack_int32 max",
       fun pack_int32_max/0},
      {"pack_int32 min",
       fun pack_int32_min/0},

      %% unpack_int32 tests
      {"unpack_int32 normal",
       fun unpack_int32_normal/0},
      {"unpack_int32 negative",
       fun unpack_int32_negative/0},
      {"unpack_int32 incomplete",
       fun unpack_int32_incomplete/0},
      {"unpack_int32 invalid",
       fun unpack_int32_invalid/0},

      %% pack_bool tests
      {"pack_bool true",
       fun pack_bool_true/0},
      {"pack_bool false",
       fun pack_bool_false/0},
      {"pack_bool 1",
       fun pack_bool_1/0},
      {"pack_bool 0",
       fun pack_bool_0/0},

      %% unpack_bool tests
      {"unpack_bool false",
       fun unpack_bool_false/0},
      {"unpack_bool true",
       fun unpack_bool_true/0},
      {"unpack_bool non-zero",
       fun unpack_bool_non_zero/0},
      {"unpack_bool empty",
       fun unpack_bool_empty/0},
      {"unpack_bool invalid",
       fun unpack_bool_invalid/0},

      %% pack_double tests
      {"pack_double float",
       fun pack_double_float/0},
      {"pack_double integer",
       fun pack_double_integer/0},
      {"pack_double zero",
       fun pack_double_zero/0},
      {"pack_double negative",
       fun pack_double_negative/0},

      %% unpack_double tests
      {"unpack_double normal",
       fun unpack_double_normal/0},
      {"unpack_double incomplete",
       fun unpack_double_incomplete/0},
      {"unpack_double invalid",
       fun unpack_double_invalid/0},

      %% pack_string_array tests
      {"pack_string_array empty",
       fun pack_string_array_empty/0},
      {"pack_string_array single",
       fun pack_string_array_single/0},
      {"pack_string_array multiple",
       fun pack_string_array_multiple/0},

      %% unpack_string_array tests
      {"unpack_string_array empty",
       fun unpack_string_array_empty/0},
      {"unpack_string_array single",
       fun unpack_string_array_single/0},
      {"unpack_string_array multiple",
       fun unpack_string_array_multiple/0},

      %% Roundtrip tests
      {"roundtrip string",
       fun roundtrip_string/0},
      {"roundtrip mem",
       fun roundtrip_mem/0},
      {"roundtrip time",
       fun roundtrip_time/0},
      {"roundtrip uint32",
       fun roundtrip_uint32/0},
      {"roundtrip int32",
       fun roundtrip_int32/0},
      {"roundtrip bool",
       fun roundtrip_bool/0},
      {"roundtrip double",
       fun roundtrip_double/0},
      {"roundtrip string array",
       fun roundtrip_string_array/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% pack_string Tests
%%%===================================================================

pack_string_undefined() ->
    Result = flurm_protocol_pack:pack_string(undefined),
    ?assertEqual(<<0:32/big>>, Result).

pack_string_null() ->
    Result = flurm_protocol_pack:pack_string(null),
    ?assertEqual(<<0:32/big>>, Result).

pack_string_empty_binary() ->
    Result = flurm_protocol_pack:pack_string(<<>>),
    ?assertEqual(<<1:32/big, 0:8>>, Result).

pack_string_binary() ->
    Result = flurm_protocol_pack:pack_string(<<"hello">>),
    ?assertEqual(<<6:32/big, "hello", 0:8>>, Result).

pack_string_list() ->
    Result = flurm_protocol_pack:pack_string("world"),
    ?assertEqual(<<6:32/big, "world", 0:8>>, Result).

%%%===================================================================
%%% unpack_string Tests
%%%===================================================================

unpack_string_no_val() ->
    Binary = <<?SLURM_NO_VAL:32/big, "rest">>,
    Result = flurm_protocol_pack:unpack_string(Binary),
    ?assertEqual({ok, undefined, <<"rest">>}, Result).

unpack_string_zero_length() ->
    Binary = <<0:32/big, "rest">>,
    Result = flurm_protocol_pack:unpack_string(Binary),
    ?assertEqual({ok, undefined, <<"rest">>}, Result).

unpack_string_normal() ->
    Binary = <<6:32/big, "hello", 0:8, "rest">>,
    Result = flurm_protocol_pack:unpack_string(Binary),
    ?assertEqual({ok, <<"hello">>, <<"rest">>}, Result).

unpack_string_insufficient() ->
    Binary = <<10:32/big, "short">>,  % Says 10 bytes but only 5 available
    Result = flurm_protocol_pack:unpack_string(Binary),
    ?assertMatch({error, {insufficient_string_data, 10}}, Result).

unpack_string_incomplete_length() ->
    Binary = <<1, 2, 3>>,  % Less than 4 bytes
    Result = flurm_protocol_pack:unpack_string(Binary),
    ?assertMatch({error, {incomplete_string_length, 3}}, Result).

unpack_string_invalid() ->
    Result = flurm_protocol_pack:unpack_string(not_a_binary),
    ?assertMatch({error, invalid_string_data}, Result).

%%%===================================================================
%%% pack_mem Tests
%%%===================================================================

pack_mem_undefined() ->
    Result = flurm_protocol_pack:pack_mem(undefined),
    ?assertEqual(<<0:32/big>>, Result).

pack_mem_empty() ->
    Result = flurm_protocol_pack:pack_mem(<<>>),
    ?assertEqual(<<0:32/big>>, Result).

pack_mem_binary() ->
    Result = flurm_protocol_pack:pack_mem(<<"data">>),
    ?assertEqual(<<4:32/big, "data">>, Result).

%%%===================================================================
%%% unpack_mem Tests
%%%===================================================================

unpack_mem_zero_length() ->
    Binary = <<0:32/big, "rest">>,
    Result = flurm_protocol_pack:unpack_mem(Binary),
    ?assertEqual({ok, <<>>, <<"rest">>}, Result).

unpack_mem_normal() ->
    Binary = <<5:32/big, "hello", "rest">>,
    Result = flurm_protocol_pack:unpack_mem(Binary),
    ?assertEqual({ok, <<"hello">>, <<"rest">>}, Result).

unpack_mem_insufficient() ->
    Binary = <<10:32/big, "short">>,
    Result = flurm_protocol_pack:unpack_mem(Binary),
    ?assertMatch({error, {insufficient_mem_data, 10}}, Result).

unpack_mem_incomplete_length() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_protocol_pack:unpack_mem(Binary),
    ?assertMatch({error, {incomplete_mem_length, 3}}, Result).

unpack_mem_invalid() ->
    Result = flurm_protocol_pack:unpack_mem(not_a_binary),
    ?assertMatch({error, invalid_mem_data}, Result).

%%%===================================================================
%%% pack_list Tests
%%%===================================================================

pack_list_empty() ->
    Result = flurm_protocol_pack:pack_list([], fun(X) -> X end),
    ?assertEqual(<<0:32/big>>, Result).

pack_list_single() ->
    PackFun = fun(X) -> <<X:32/big>> end,
    Result = flurm_protocol_pack:pack_list([100], PackFun),
    ?assertEqual(<<1:32/big, 100:32/big>>, Result).

pack_list_multiple() ->
    PackFun = fun(X) -> <<X:32/big>> end,
    Result = flurm_protocol_pack:pack_list([1, 2, 3], PackFun),
    ?assertEqual(<<3:32/big, 1:32/big, 2:32/big, 3:32/big>>, Result).

%%%===================================================================
%%% unpack_list Tests
%%%===================================================================

unpack_list_empty() ->
    Binary = <<0:32/big, "rest">>,
    UnpackFun = fun(B) -> {ok, elem, B} end,
    Result = flurm_protocol_pack:unpack_list(Binary, UnpackFun),
    ?assertEqual({ok, [], <<"rest">>}, Result).

unpack_list_single() ->
    Binary = <<1:32/big, 42:32/big, "rest">>,
    UnpackFun = fun(<<V:32/big, R/binary>>) -> {ok, V, R} end,
    Result = flurm_protocol_pack:unpack_list(Binary, UnpackFun),
    ?assertEqual({ok, [42], <<"rest">>}, Result).

unpack_list_multiple() ->
    Binary = <<3:32/big, 1:32/big, 2:32/big, 3:32/big, "rest">>,
    UnpackFun = fun(<<V:32/big, R/binary>>) -> {ok, V, R} end,
    Result = flurm_protocol_pack:unpack_list(Binary, UnpackFun),
    ?assertEqual({ok, [1, 2, 3], <<"rest">>}, Result).

unpack_list_incomplete_count() ->
    Binary = <<1, 2, 3>>,
    UnpackFun = fun(_) -> {ok, x, <<>>} end,
    Result = flurm_protocol_pack:unpack_list(Binary, UnpackFun),
    ?assertMatch({error, {incomplete_list_count, 3}}, Result).

unpack_list_invalid() ->
    Result = flurm_protocol_pack:unpack_list(not_a_binary, fun(_) -> ok end),
    ?assertMatch({error, invalid_list_data}, Result).

unpack_list_element_error() ->
    Binary = <<2:32/big, "some_data">>,
    UnpackFun = fun(_) -> {error, parse_error} end,
    Result = flurm_protocol_pack:unpack_list(Binary, UnpackFun),
    ?assertEqual({error, parse_error}, Result).

%%%===================================================================
%%% pack_time Tests
%%%===================================================================

pack_time_undefined() ->
    Result = flurm_protocol_pack:pack_time(undefined),
    ?assertEqual(<<?SLURM_NO_VAL64:64/big>>, Result).

pack_time_zero() ->
    Result = flurm_protocol_pack:pack_time(0),
    ?assertEqual(<<0:64/big>>, Result).

pack_time_tuple() ->
    Result = flurm_protocol_pack:pack_time({1234, 567890, 0}),
    Expected = 1234 * 1000000 + 567890,
    ?assertEqual(<<Expected:64/big>>, Result).

pack_time_integer() ->
    Result = flurm_protocol_pack:pack_time(1234567890),
    ?assertEqual(<<1234567890:64/big>>, Result).

%%%===================================================================
%%% unpack_time Tests
%%%===================================================================

unpack_time_no_val64() ->
    Binary = <<?SLURM_NO_VAL64:64/big, "rest">>,
    Result = flurm_protocol_pack:unpack_time(Binary),
    ?assertEqual({ok, undefined, <<"rest">>}, Result).

unpack_time_normal() ->
    Binary = <<1234567890:64/big, "rest">>,
    Result = flurm_protocol_pack:unpack_time(Binary),
    ?assertEqual({ok, 1234567890, <<"rest">>}, Result).

unpack_time_incomplete() ->
    Binary = <<1, 2, 3, 4, 5, 6, 7>>,
    Result = flurm_protocol_pack:unpack_time(Binary),
    ?assertMatch({error, {incomplete_timestamp, 7}}, Result).

unpack_time_invalid() ->
    Result = flurm_protocol_pack:unpack_time(not_a_binary),
    ?assertMatch({error, invalid_timestamp_data}, Result).

%%%===================================================================
%%% pack_uint8 Tests
%%%===================================================================

pack_uint8_zero() ->
    Result = flurm_protocol_pack:pack_uint8(0),
    ?assertEqual(<<0:8>>, Result).

pack_uint8_value() ->
    Result = flurm_protocol_pack:pack_uint8(128),
    ?assertEqual(<<128:8>>, Result).

pack_uint8_max() ->
    Result = flurm_protocol_pack:pack_uint8(255),
    ?assertEqual(<<255:8>>, Result).

%%%===================================================================
%%% unpack_uint8 Tests
%%%===================================================================

unpack_uint8_normal() ->
    Binary = <<200:8, "rest">>,
    Result = flurm_protocol_pack:unpack_uint8(Binary),
    ?assertEqual({ok, 200, <<"rest">>}, Result).

unpack_uint8_empty() ->
    Result = flurm_protocol_pack:unpack_uint8(<<>>),
    ?assertEqual({error, incomplete_uint8}, Result).

unpack_uint8_invalid() ->
    Result = flurm_protocol_pack:unpack_uint8(not_a_binary),
    ?assertMatch({error, invalid_uint8_data}, Result).

%%%===================================================================
%%% pack_uint16 Tests
%%%===================================================================

pack_uint16_zero() ->
    Result = flurm_protocol_pack:pack_uint16(0),
    ?assertEqual(<<0:16/big>>, Result).

pack_uint16_value() ->
    Result = flurm_protocol_pack:pack_uint16(12345),
    ?assertEqual(<<12345:16/big>>, Result).

pack_uint16_max() ->
    Result = flurm_protocol_pack:pack_uint16(65535),
    ?assertEqual(<<65535:16/big>>, Result).

%%%===================================================================
%%% unpack_uint16 Tests
%%%===================================================================

unpack_uint16_normal() ->
    Binary = <<54321:16/big, "rest">>,
    Result = flurm_protocol_pack:unpack_uint16(Binary),
    ?assertEqual({ok, 54321, <<"rest">>}, Result).

unpack_uint16_incomplete() ->
    Binary = <<1>>,
    Result = flurm_protocol_pack:unpack_uint16(Binary),
    ?assertMatch({error, {incomplete_uint16, 1}}, Result).

unpack_uint16_invalid() ->
    Result = flurm_protocol_pack:unpack_uint16(not_a_binary),
    ?assertMatch({error, invalid_uint16_data}, Result).

%%%===================================================================
%%% pack_uint32 Tests
%%%===================================================================

pack_uint32_undefined() ->
    Result = flurm_protocol_pack:pack_uint32(undefined),
    ?assertEqual(<<?SLURM_NO_VAL:32/big>>, Result).

pack_uint32_zero() ->
    Result = flurm_protocol_pack:pack_uint32(0),
    ?assertEqual(<<0:32/big>>, Result).

pack_uint32_value() ->
    Result = flurm_protocol_pack:pack_uint32(123456789),
    ?assertEqual(<<123456789:32/big>>, Result).

pack_uint32_max() ->
    Result = flurm_protocol_pack:pack_uint32(16#FFFFFFFF),
    ?assertEqual(<<16#FFFFFFFF:32/big>>, Result).

%%%===================================================================
%%% unpack_uint32 Tests
%%%===================================================================

unpack_uint32_no_val() ->
    Binary = <<?SLURM_NO_VAL:32/big, "rest">>,
    Result = flurm_protocol_pack:unpack_uint32(Binary),
    ?assertEqual({ok, undefined, <<"rest">>}, Result).

unpack_uint32_normal() ->
    Binary = <<987654321:32/big, "rest">>,
    Result = flurm_protocol_pack:unpack_uint32(Binary),
    ?assertEqual({ok, 987654321, <<"rest">>}, Result).

unpack_uint32_incomplete() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_protocol_pack:unpack_uint32(Binary),
    ?assertMatch({error, {incomplete_uint32, 3}}, Result).

unpack_uint32_invalid() ->
    Result = flurm_protocol_pack:unpack_uint32(not_a_binary),
    ?assertMatch({error, invalid_uint32_data}, Result).

%%%===================================================================
%%% pack_uint64 Tests
%%%===================================================================

pack_uint64_undefined() ->
    Result = flurm_protocol_pack:pack_uint64(undefined),
    ?assertEqual(<<?SLURM_NO_VAL64:64/big>>, Result).

pack_uint64_zero() ->
    Result = flurm_protocol_pack:pack_uint64(0),
    ?assertEqual(<<0:64/big>>, Result).

pack_uint64_value() ->
    Result = flurm_protocol_pack:pack_uint64(12345678901234567890),
    ?assertEqual(<<12345678901234567890:64/big>>, Result).

pack_uint64_max() ->
    Result = flurm_protocol_pack:pack_uint64(16#FFFFFFFFFFFFFFFF),
    ?assertEqual(<<16#FFFFFFFFFFFFFFFF:64/big>>, Result).

%%%===================================================================
%%% unpack_uint64 Tests
%%%===================================================================

unpack_uint64_no_val64() ->
    Binary = <<?SLURM_NO_VAL64:64/big, "rest">>,
    Result = flurm_protocol_pack:unpack_uint64(Binary),
    ?assertEqual({ok, undefined, <<"rest">>}, Result).

unpack_uint64_normal() ->
    %% Use a value that fits in 64 bits
    Value = 9876543210987654321,
    Binary = <<Value:64/big, "rest">>,
    Result = flurm_protocol_pack:unpack_uint64(Binary),
    ?assertEqual({ok, Value, <<"rest">>}, Result).

unpack_uint64_incomplete() ->
    Binary = <<1, 2, 3, 4, 5, 6, 7>>,
    Result = flurm_protocol_pack:unpack_uint64(Binary),
    ?assertMatch({error, {incomplete_uint64, 7}}, Result).

unpack_uint64_invalid() ->
    Result = flurm_protocol_pack:unpack_uint64(not_a_binary),
    ?assertMatch({error, invalid_uint64_data}, Result).

%%%===================================================================
%%% pack_int32 Tests
%%%===================================================================

pack_int32_zero() ->
    Result = flurm_protocol_pack:pack_int32(0),
    ?assertEqual(<<0:32/big-signed>>, Result).

pack_int32_positive() ->
    Result = flurm_protocol_pack:pack_int32(12345),
    ?assertEqual(<<12345:32/big-signed>>, Result).

pack_int32_negative() ->
    Result = flurm_protocol_pack:pack_int32(-12345),
    ?assertEqual(<<(-12345):32/big-signed>>, Result).

pack_int32_max() ->
    Result = flurm_protocol_pack:pack_int32(2147483647),
    ?assertEqual(<<2147483647:32/big-signed>>, Result).

pack_int32_min() ->
    Result = flurm_protocol_pack:pack_int32(-2147483648),
    ?assertEqual(<<(-2147483648):32/big-signed>>, Result).

%%%===================================================================
%%% unpack_int32 Tests
%%%===================================================================

unpack_int32_normal() ->
    Binary = <<54321:32/big-signed, "rest">>,
    Result = flurm_protocol_pack:unpack_int32(Binary),
    ?assertEqual({ok, 54321, <<"rest">>}, Result).

unpack_int32_negative() ->
    Binary = <<(-54321):32/big-signed, "rest">>,
    Result = flurm_protocol_pack:unpack_int32(Binary),
    ?assertEqual({ok, -54321, <<"rest">>}, Result).

unpack_int32_incomplete() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_protocol_pack:unpack_int32(Binary),
    ?assertMatch({error, {incomplete_int32, 3}}, Result).

unpack_int32_invalid() ->
    Result = flurm_protocol_pack:unpack_int32(not_a_binary),
    ?assertMatch({error, invalid_int32_data}, Result).

%%%===================================================================
%%% pack_bool Tests
%%%===================================================================

pack_bool_true() ->
    Result = flurm_protocol_pack:pack_bool(true),
    ?assertEqual(<<1:8>>, Result).

pack_bool_false() ->
    Result = flurm_protocol_pack:pack_bool(false),
    ?assertEqual(<<0:8>>, Result).

pack_bool_1() ->
    Result = flurm_protocol_pack:pack_bool(1),
    ?assertEqual(<<1:8>>, Result).

pack_bool_0() ->
    Result = flurm_protocol_pack:pack_bool(0),
    ?assertEqual(<<0:8>>, Result).

%%%===================================================================
%%% unpack_bool Tests
%%%===================================================================

unpack_bool_false() ->
    Binary = <<0:8, "rest">>,
    Result = flurm_protocol_pack:unpack_bool(Binary),
    ?assertEqual({ok, false, <<"rest">>}, Result).

unpack_bool_true() ->
    Binary = <<1:8, "rest">>,
    Result = flurm_protocol_pack:unpack_bool(Binary),
    ?assertEqual({ok, true, <<"rest">>}, Result).

unpack_bool_non_zero() ->
    Binary = <<255:8, "rest">>,
    Result = flurm_protocol_pack:unpack_bool(Binary),
    ?assertEqual({ok, true, <<"rest">>}, Result).

unpack_bool_empty() ->
    Result = flurm_protocol_pack:unpack_bool(<<>>),
    ?assertEqual({error, incomplete_bool}, Result).

unpack_bool_invalid() ->
    Result = flurm_protocol_pack:unpack_bool(not_a_binary),
    ?assertMatch({error, invalid_bool_data}, Result).

%%%===================================================================
%%% pack_double Tests
%%%===================================================================

pack_double_float() ->
    Result = flurm_protocol_pack:pack_double(3.14159),
    ?assertEqual(<<3.14159:64/big-float>>, Result).

pack_double_integer() ->
    Result = flurm_protocol_pack:pack_double(42),
    ?assertEqual(<<42.0:64/big-float>>, Result).

pack_double_zero() ->
    Result = flurm_protocol_pack:pack_double(0.0),
    ?assertEqual(<<0.0:64/big-float>>, Result).

pack_double_negative() ->
    Result = flurm_protocol_pack:pack_double(-123.456),
    ?assertEqual(<<(-123.456):64/big-float>>, Result).

%%%===================================================================
%%% unpack_double Tests
%%%===================================================================

unpack_double_normal() ->
    Binary = <<2.71828:64/big-float, "rest">>,
    {ok, Value, Rest} = flurm_protocol_pack:unpack_double(Binary),
    ?assert(abs(Value - 2.71828) < 0.00001),
    ?assertEqual(<<"rest">>, Rest).

unpack_double_incomplete() ->
    Binary = <<1, 2, 3, 4, 5, 6, 7>>,
    Result = flurm_protocol_pack:unpack_double(Binary),
    ?assertMatch({error, {incomplete_double, 7}}, Result).

unpack_double_invalid() ->
    Result = flurm_protocol_pack:unpack_double(not_a_binary),
    ?assertMatch({error, invalid_double_data}, Result).

%%%===================================================================
%%% pack_string_array Tests
%%%===================================================================

pack_string_array_empty() ->
    Result = flurm_protocol_pack:pack_string_array([]),
    ?assertEqual(<<0:32/big>>, Result).

pack_string_array_single() ->
    Result = flurm_protocol_pack:pack_string_array([<<"hello">>]),
    Expected = <<1:32/big, 6:32/big, "hello", 0:8>>,
    ?assertEqual(Expected, Result).

pack_string_array_multiple() ->
    Result = flurm_protocol_pack:pack_string_array([<<"a">>, <<"bb">>, <<"ccc">>]),
    Expected = <<3:32/big,
                 2:32/big, "a", 0:8,
                 3:32/big, "bb", 0:8,
                 4:32/big, "ccc", 0:8>>,
    ?assertEqual(Expected, Result).

%%%===================================================================
%%% unpack_string_array Tests
%%%===================================================================

unpack_string_array_empty() ->
    Binary = <<0:32/big, "rest">>,
    Result = flurm_protocol_pack:unpack_string_array(Binary),
    ?assertEqual({ok, [], <<"rest">>}, Result).

unpack_string_array_single() ->
    Binary = <<1:32/big, 6:32/big, "hello", 0:8, "rest">>,
    Result = flurm_protocol_pack:unpack_string_array(Binary),
    ?assertEqual({ok, [<<"hello">>], <<"rest">>}, Result).

unpack_string_array_multiple() ->
    Binary = <<3:32/big,
               2:32/big, "a", 0:8,
               3:32/big, "bb", 0:8,
               4:32/big, "ccc", 0:8,
               "rest">>,
    Result = flurm_protocol_pack:unpack_string_array(Binary),
    ?assertEqual({ok, [<<"a">>, <<"bb">>, <<"ccc">>], <<"rest">>}, Result).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_string() ->
    Tests = [<<"hello">>, <<"">>, <<"with spaces">>, <<"unicode ", 195, 169>>],
    lists:foreach(fun(Str) ->
        Packed = flurm_protocol_pack:pack_string(Str),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_string(Packed),
        case Str of
            <<>> -> ?assertEqual(<<>>, Unpacked);  % Empty string roundtrips to empty
            _ -> ?assertEqual(Str, Unpacked)
        end
    end, Tests).

roundtrip_mem() ->
    Tests = [<<"raw">>, <<0, 1, 2, 255>>, <<"binary data">>],
    lists:foreach(fun(Data) ->
        Packed = flurm_protocol_pack:pack_mem(Data),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_mem(Packed),
        ?assertEqual(Data, Unpacked)
    end, Tests).

roundtrip_time() ->
    Tests = [0, 1234567890, erlang:system_time(second)],
    lists:foreach(fun(Time) ->
        Packed = flurm_protocol_pack:pack_time(Time),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_time(Packed),
        ?assertEqual(Time, Unpacked)
    end, Tests).

roundtrip_uint32() ->
    Tests = [0, 1, 65535, 123456789, 16#FFFFFFFD],
    lists:foreach(fun(Val) ->
        Packed = flurm_protocol_pack:pack_uint32(Val),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_uint32(Packed),
        ?assertEqual(Val, Unpacked)
    end, Tests).

roundtrip_int32() ->
    Tests = [0, 1, -1, 2147483647, -2147483648, 12345, -12345],
    lists:foreach(fun(Val) ->
        Packed = flurm_protocol_pack:pack_int32(Val),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_int32(Packed),
        ?assertEqual(Val, Unpacked)
    end, Tests).

roundtrip_bool() ->
    Tests = [{true, true}, {false, false}],
    lists:foreach(fun({Input, Expected}) ->
        Packed = flurm_protocol_pack:pack_bool(Input),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_bool(Packed),
        ?assertEqual(Expected, Unpacked)
    end, Tests).

roundtrip_double() ->
    Tests = [0.0, 1.0, -1.0, 3.14159, 2.71828, 1.0e10, 1.0e-10],
    lists:foreach(fun(Val) ->
        Packed = flurm_protocol_pack:pack_double(Val),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_double(Packed),
        ?assert(abs(Val - Unpacked) < 1.0e-10)
    end, Tests).

roundtrip_string_array() ->
    Tests = [[], [<<"single">>], [<<"a">>, <<"b">>, <<"c">>]],
    lists:foreach(fun(Arr) ->
        Packed = flurm_protocol_pack:pack_string_array(Arr),
        {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_string_array(Packed),
        ?assertEqual(Arr, Unpacked)
    end, Tests).

%%%===================================================================
%%% Additional Edge Cases
%%%===================================================================

%% Test with extra trailing data
trailing_data_test_() ->
    {"unpack with trailing data preserved",
     fun() ->
         Binary = <<123:32/big, "trailing">>,
         {ok, Value, Rest} = flurm_protocol_pack:unpack_uint32(Binary),
         ?assertEqual(123, Value),
         ?assertEqual(<<"trailing">>, Rest)
     end}.

%% Test NO_VAL boundary
no_val_boundary_test_() ->
    {"pack_uint32 near NO_VAL",
     fun() ->
         %% NO_VAL is 0xFFFFFFFE
         Result1 = flurm_protocol_pack:pack_uint32(16#FFFFFFFD),
         ?assertEqual(<<16#FFFFFFFD:32/big>>, Result1),

         %% NO_VAL itself should work
         Result2 = flurm_protocol_pack:pack_uint32(16#FFFFFFFE),
         %% When unpacked, this becomes NO_VAL -> undefined
         {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_uint32(Result2),
         ?assertEqual(undefined, Unpacked)
     end}.

%% Test large strings
large_string_test_() ->
    {"pack/unpack large string",
     fun() ->
         LargeStr = binary:copy(<<"x">>, 10000),
         Packed = flurm_protocol_pack:pack_string(LargeStr),
         {ok, Unpacked, <<>>} = flurm_protocol_pack:unpack_string(Packed),
         ?assertEqual(LargeStr, Unpacked)
     end}.

%% Test empty binary handling
empty_binary_handling_test_() ->
    [
        {"unpack_string empty binary",
         fun() ->
             Result = flurm_protocol_pack:unpack_string(<<>>),
             ?assertMatch({error, {incomplete_string_length, 0}}, Result)
         end},
        {"unpack_uint32 empty binary",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint32(<<>>),
             ?assertMatch({error, {incomplete_uint32, 0}}, Result)
         end},
        {"unpack_uint64 empty binary",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint64(<<>>),
             ?assertMatch({error, {incomplete_uint64, 0}}, Result)
         end}
    ].

%%%===================================================================
%%% Additional Edge Case Tests for 90%+ Coverage
%%%===================================================================

%% Tests for catch-all invalid data clauses
invalid_data_edge_cases_test_() ->
    [
        {"unpack_string with tuple input",
         fun() ->
             Result = flurm_protocol_pack:unpack_string({invalid, data}),
             ?assertMatch({error, invalid_string_data}, Result)
         end},
        {"unpack_mem with tuple input",
         fun() ->
             Result = flurm_protocol_pack:unpack_mem({invalid, data}),
             ?assertMatch({error, invalid_mem_data}, Result)
         end},
        {"unpack_list with tuple input",
         fun() ->
             UnpackFun = fun(_) -> {ok, x, <<>>} end,
             Result = flurm_protocol_pack:unpack_list({invalid, data}, UnpackFun),
             ?assertMatch({error, invalid_list_data}, Result)
         end},
        {"unpack_time with tuple input",
         fun() ->
             Result = flurm_protocol_pack:unpack_time({invalid, data}),
             ?assertMatch({error, invalid_timestamp_data}, Result)
         end},
        {"unpack_uint8 with tuple input",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint8({invalid}),
             ?assertMatch({error, invalid_uint8_data}, Result)
         end},
        {"unpack_uint16 with tuple input",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint16({invalid}),
             ?assertMatch({error, invalid_uint16_data}, Result)
         end},
        {"unpack_uint32 with tuple input",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint32({invalid}),
             ?assertMatch({error, invalid_uint32_data}, Result)
         end},
        {"unpack_uint64 with tuple input",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint64({invalid}),
             ?assertMatch({error, invalid_uint64_data}, Result)
         end},
        {"unpack_int32 with tuple input",
         fun() ->
             Result = flurm_protocol_pack:unpack_int32({invalid}),
             ?assertMatch({error, invalid_int32_data}, Result)
         end},
        {"unpack_bool with tuple input",
         fun() ->
             Result = flurm_protocol_pack:unpack_bool({invalid}),
             ?assertMatch({error, invalid_bool_data}, Result)
         end},
        {"unpack_double with tuple input",
         fun() ->
             Result = flurm_protocol_pack:unpack_double({invalid}),
             ?assertMatch({error, invalid_double_data}, Result)
         end}
    ].

%% Tests for 1-byte and 2-byte incomplete data
incomplete_data_edge_cases_test_() ->
    [
        {"unpack_string with 1 byte",
         fun() ->
             Result = flurm_protocol_pack:unpack_string(<<1>>),
             ?assertMatch({error, {incomplete_string_length, 1}}, Result)
         end},
        {"unpack_mem with 1 byte",
         fun() ->
             Result = flurm_protocol_pack:unpack_mem(<<1>>),
             ?assertMatch({error, {incomplete_mem_length, 1}}, Result)
         end},
        {"unpack_mem with 2 bytes",
         fun() ->
             Result = flurm_protocol_pack:unpack_mem(<<1, 2>>),
             ?assertMatch({error, {incomplete_mem_length, 2}}, Result)
         end},
        {"unpack_list with 1 byte",
         fun() ->
             UnpackFun = fun(_) -> {ok, x, <<>>} end,
             Result = flurm_protocol_pack:unpack_list(<<1>>, UnpackFun),
             ?assertMatch({error, {incomplete_list_count, 1}}, Result)
         end},
        {"unpack_list with 2 bytes",
         fun() ->
             UnpackFun = fun(_) -> {ok, x, <<>>} end,
             Result = flurm_protocol_pack:unpack_list(<<1, 2>>, UnpackFun),
             ?assertMatch({error, {incomplete_list_count, 2}}, Result)
         end},
        {"unpack_time with 1 byte",
         fun() ->
             Result = flurm_protocol_pack:unpack_time(<<1>>),
             ?assertMatch({error, {incomplete_timestamp, 1}}, Result)
         end},
        {"unpack_time with 4 bytes",
         fun() ->
             Result = flurm_protocol_pack:unpack_time(<<1, 2, 3, 4>>),
             ?assertMatch({error, {incomplete_timestamp, 4}}, Result)
         end},
        {"unpack_uint16 empty binary",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint16(<<>>),
             ?assertMatch({error, {incomplete_uint16, 0}}, Result)
         end},
        {"unpack_uint32 with 1 byte",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint32(<<1>>),
             ?assertMatch({error, {incomplete_uint32, 1}}, Result)
         end},
        {"unpack_uint32 with 2 bytes",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint32(<<1, 2>>),
             ?assertMatch({error, {incomplete_uint32, 2}}, Result)
         end},
        {"unpack_uint64 with 1 byte",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint64(<<1>>),
             ?assertMatch({error, {incomplete_uint64, 1}}, Result)
         end},
        {"unpack_uint64 with 4 bytes",
         fun() ->
             Result = flurm_protocol_pack:unpack_uint64(<<1, 2, 3, 4>>),
             ?assertMatch({error, {incomplete_uint64, 4}}, Result)
         end},
        {"unpack_int32 with 1 byte",
         fun() ->
             Result = flurm_protocol_pack:unpack_int32(<<1>>),
             ?assertMatch({error, {incomplete_int32, 1}}, Result)
         end},
        {"unpack_int32 with 2 bytes",
         fun() ->
             Result = flurm_protocol_pack:unpack_int32(<<1, 2>>),
             ?assertMatch({error, {incomplete_int32, 2}}, Result)
         end},
        {"unpack_double with 1 byte",
         fun() ->
             Result = flurm_protocol_pack:unpack_double(<<1>>),
             ?assertMatch({error, {incomplete_double, 1}}, Result)
         end},
        {"unpack_double with 4 bytes",
         fun() ->
             Result = flurm_protocol_pack:unpack_double(<<1, 2, 3, 4>>),
             ?assertMatch({error, {incomplete_double, 4}}, Result)
         end}
    ].

%% Tests for pack_mem undefined edge case
pack_mem_edge_cases_test_() ->
    [
        {"pack_mem undefined",
         fun() ->
             Result = flurm_protocol_pack:pack_mem(undefined),
             ?assertEqual(<<0:32/big>>, Result)
         end}
    ].

%% Tests for string array error propagation
string_array_error_test_() ->
    [
        {"unpack_string_array with insufficient data",
         fun() ->
             %% Array says 2 elements but only 1 string
             Binary = <<2:32/big, 2:32/big, "a", 0>>,
             Result = flurm_protocol_pack:unpack_string_array(Binary),
             ?assertMatch({error, _}, Result)
         end},
        {"unpack_string_array with truncated string",
         fun() ->
             %% Array says 1 element with 10-byte string but data too short
             Binary = <<1:32/big, 10:32/big, "abc">>,
             Result = flurm_protocol_pack:unpack_string_array(Binary),
             ?assertMatch({error, {insufficient_string_data, 10}}, Result)
         end}
    ].
