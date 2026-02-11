%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_io_protocol
%%%
%%% Covers I/O forwarding protocol encoding and decoding.
%%% Pure encoder/decoder tests with no processes or mocking.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_io_protocol_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_io_protocol_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

io_protocol_test_() ->
    [
        {"encode_io_init_msg/4 default version",
         fun test_encode_io_init_msg_default/0},
        {"encode_io_init_msg/5 explicit version",
         fun test_encode_io_init_msg_explicit_version/0},
        {"encode_io_init_msg/5 payload length correct",
         fun test_encode_io_init_msg_length/0},
        {"encode_io_init_msg/5 empty key",
         fun test_encode_io_init_msg_empty_key/0},
        {"encode_io_init_msg/5 fields roundtrip",
         fun test_encode_io_init_msg_fields_roundtrip/0},
        {"encode_io_init_msg_fixed_key/4 legacy format",
         fun test_encode_io_init_msg_fixed_key/0},
        {"encode_io_init_msg_fixed_key/4 exactly 46 bytes",
         fun test_encode_io_init_msg_fixed_key_size/0},
        {"encode_io_init_msg_fixed_key/4 32-byte zero key",
         fun test_encode_io_init_msg_fixed_key_zero_key/0},
        {"encode_io_hdr/4 produces 10 bytes",
         fun test_encode_io_hdr_size/0},
        {"encode_io_hdr/4 field layout",
         fun test_encode_io_hdr_fields/0},
        {"encode_stdout/3 header type + data",
         fun test_encode_stdout/0},
        {"encode_stderr/3 header type + data",
         fun test_encode_stderr/0},
        {"encode_eof/2 produces 10 bytes with length 0",
         fun test_encode_eof/0},
        {"decode_io_hdr/1 valid decode",
         fun test_decode_io_hdr_valid/0},
        {"decode_io_hdr/1 roundtrip with encode",
         fun test_decode_io_hdr_roundtrip/0},
        {"decode_io_hdr/1 incomplete binary errors",
         fun test_decode_io_hdr_incomplete/0},
        {"decode_io_hdr/1 empty binary error",
         fun test_decode_io_hdr_empty/0},
        {"large data (64KB) correct header length",
         fun test_large_data/0},
        {"full roundtrip encode_stdout -> decode_io_hdr",
         fun test_full_roundtrip/0}
    ].

%%====================================================================
%% Tests
%%====================================================================

test_encode_io_init_msg_default() ->
    Key = <<"mykey">>,
    Bin = flurm_io_protocol:encode_io_init_msg(0, 1, 1, Key),
    ?assert(is_binary(Bin)),
    %% Should have length prefix (4) + payload
    ?assert(byte_size(Bin) > 4).

test_encode_io_init_msg_explicit_version() ->
    Key = <<"testkey">>,
    Bin = flurm_io_protocol:encode_io_init_msg(16#2600, 0, 1, 1, Key),
    %% Parse: length(4) + version(2) + nodeid(4) + stdout(4) + stderr(4) + keylen(4) + key
    <<PayloadLen:32/big, Version:16/big, _Rest/binary>> = Bin,
    ?assertEqual(16#2600, Version),
    ExpectedLen = 2 + 4 + 4 + 4 + 4 + byte_size(Key),
    ?assertEqual(ExpectedLen, PayloadLen).

test_encode_io_init_msg_length() ->
    Key = <<"abcdefgh">>,
    Bin = flurm_io_protocol:encode_io_init_msg(16#b001, 5, 2, 3, Key),
    <<PayloadLen:32/big, _Payload/binary>> = Bin,
    %% Payload = version(2) + nodeid(4) + stdout(4) + stderr(4) + keylen(4) + key(8)
    ?assertEqual(2 + 4 + 4 + 4 + 4 + 8, PayloadLen),
    ?assertEqual(4 + PayloadLen, byte_size(Bin)).

test_encode_io_init_msg_empty_key() ->
    Bin = flurm_io_protocol:encode_io_init_msg(16#2600, 0, 1, 1, <<>>),
    <<PayloadLen:32/big, _Version:16/big, _NodeId:32/big,
      _Stdout:32/big, _Stderr:32/big, IoKeyLen:32/big, Rest/binary>> = Bin,
    ?assertEqual(0, IoKeyLen),
    ?assertEqual(<<>>, Rest),
    ?assertEqual(2 + 4 + 4 + 4 + 4, PayloadLen).

test_encode_io_init_msg_fields_roundtrip() ->
    Key = <<"credential_key_data">>,
    Bin = flurm_io_protocol:encode_io_init_msg(16#2600, 7, 3, 2, Key),
    <<_PayloadLen:32/big, Version:16/big, NodeId:32/big,
      StdoutObjs:32/big, StderrObjs:32/big,
      IoKeyLen:32/big, IoKey/binary>> = Bin,
    ?assertEqual(16#2600, Version),
    ?assertEqual(7, NodeId),
    ?assertEqual(3, StdoutObjs),
    ?assertEqual(2, StderrObjs),
    ?assertEqual(byte_size(Key), IoKeyLen),
    ?assertEqual(Key, IoKey).

test_encode_io_init_msg_fixed_key() ->
    Bin = flurm_io_protocol:encode_io_init_msg_fixed_key(16#b001, 0, 1, 1),
    <<Version:16/big, _Rest/binary>> = Bin,
    ?assertEqual(16#b001, Version).

test_encode_io_init_msg_fixed_key_size() ->
    Bin = flurm_io_protocol:encode_io_init_msg_fixed_key(16#b001, 0, 1, 1),
    %% version(2) + nodeid(4) + stdout(4) + stderr(4) + key(32) = 46
    ?assertEqual(46, byte_size(Bin)).

test_encode_io_init_msg_fixed_key_zero_key() ->
    Bin = flurm_io_protocol:encode_io_init_msg_fixed_key(16#b001, 0, 1, 1),
    <<_Version:16/big, _NodeId:32/big, _Stdout:32/big, _Stderr:32/big, Key:256>> = Bin,
    ?assertEqual(0, Key).

test_encode_io_hdr_size() ->
    Hdr = flurm_io_protocol:encode_io_hdr(1, 0, 0, 100),
    ?assertEqual(10, byte_size(Hdr)).

test_encode_io_hdr_fields() ->
    Hdr = flurm_io_protocol:encode_io_hdr(2, 5, 3, 1024),
    <<Type:16/big, Gtaskid:16/big, Ltaskid:16/big, Length:32/big>> = Hdr,
    ?assertEqual(2, Type),
    ?assertEqual(5, Gtaskid),
    ?assertEqual(3, Ltaskid),
    ?assertEqual(1024, Length).

test_encode_stdout() ->
    Data = <<"Hello, World!">>,
    Bin = flurm_io_protocol:encode_stdout(0, 0, Data),
    %% Header (10 bytes) + data
    ?assertEqual(10 + byte_size(Data), byte_size(Bin)),
    <<Type:16/big, _Gtaskid:16/big, _Ltaskid:16/big, Length:32/big, Payload/binary>> = Bin,
    ?assertEqual(?SLURM_IO_STDOUT, Type),
    ?assertEqual(byte_size(Data), Length),
    ?assertEqual(Data, Payload).

test_encode_stderr() ->
    Data = <<"Error occurred">>,
    Bin = flurm_io_protocol:encode_stderr(1, 1, Data),
    ?assertEqual(10 + byte_size(Data), byte_size(Bin)),
    <<Type:16/big, Gtaskid:16/big, Ltaskid:16/big, Length:32/big, _/binary>> = Bin,
    ?assertEqual(?SLURM_IO_STDERR, Type),
    ?assertEqual(1, Gtaskid),
    ?assertEqual(1, Ltaskid),
    ?assertEqual(byte_size(Data), Length).

test_encode_eof() ->
    Bin = flurm_io_protocol:encode_eof(0, 0),
    ?assertEqual(10, byte_size(Bin)),
    <<Type:16/big, _:16/big, _:16/big, Length:32/big>> = Bin,
    ?assertEqual(?SLURM_IO_STDOUT, Type),
    ?assertEqual(0, Length).

test_decode_io_hdr_valid() ->
    Bin = <<1:16/big, 0:16/big, 0:16/big, 512:32/big>>,
    ?assertEqual({ok, 1, 0, 0, 512}, flurm_io_protocol:decode_io_hdr(Bin)).

test_decode_io_hdr_roundtrip() ->
    Hdr = flurm_io_protocol:encode_io_hdr(2, 3, 4, 999),
    {ok, Type, Gtaskid, Ltaskid, Length} = flurm_io_protocol:decode_io_hdr(Hdr),
    ?assertEqual(2, Type),
    ?assertEqual(3, Gtaskid),
    ?assertEqual(4, Ltaskid),
    ?assertEqual(999, Length).

test_decode_io_hdr_incomplete() ->
    %% 0 bytes
    ?assertMatch({error, {incomplete, need, _}}, flurm_io_protocol:decode_io_hdr(<<>>)),
    %% 1 byte
    ?assertMatch({error, {incomplete, need, _}}, flurm_io_protocol:decode_io_hdr(<<1>>)),
    %% 5 bytes
    ?assertMatch({error, {incomplete, need, _}}, flurm_io_protocol:decode_io_hdr(<<1,2,3,4,5>>)),
    %% 9 bytes
    ?assertMatch({error, {incomplete, need, _}}, flurm_io_protocol:decode_io_hdr(<<1,2,3,4,5,6,7,8,9>>)).

test_decode_io_hdr_empty() ->
    ?assertMatch({error, {incomplete, need, _}}, flurm_io_protocol:decode_io_hdr(<<>>)).

test_large_data() ->
    Data = binary:copy(<<"X">>, 65536),
    Bin = flurm_io_protocol:encode_stdout(0, 0, Data),
    <<_Type:16/big, _G:16/big, _L:16/big, Length:32/big, _Payload/binary>> = Bin,
    ?assertEqual(65536, Length),
    ?assertEqual(10 + 65536, byte_size(Bin)).

test_full_roundtrip() ->
    Data = <<"test output data for roundtrip">>,
    Bin = flurm_io_protocol:encode_stdout(2, 1, Data),
    %% Split into header and payload
    <<HdrBin:10/binary, Payload/binary>> = Bin,
    {ok, Type, Gtaskid, Ltaskid, Length} = flurm_io_protocol:decode_io_hdr(HdrBin),
    ?assertEqual(?SLURM_IO_STDOUT, Type),
    ?assertEqual(2, Gtaskid),
    ?assertEqual(1, Ltaskid),
    ?assertEqual(byte_size(Data), Length),
    ?assertEqual(Data, Payload).
