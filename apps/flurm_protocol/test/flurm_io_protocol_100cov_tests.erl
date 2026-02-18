%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_io_protocol module
%%% Coverage target: 100% of all functions and branches
%%%-------------------------------------------------------------------
-module(flurm_io_protocol_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Generator
%%%===================================================================

flurm_io_protocol_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% encode_io_init_msg/4 tests
      {"encode_io_init_msg/4 with empty key",
       fun encode_io_init_msg_4_empty_key/0},
      {"encode_io_init_msg/4 with non-empty key",
       fun encode_io_init_msg_4_with_key/0},
      {"encode_io_init_msg/4 with large key",
       fun encode_io_init_msg_4_large_key/0},

      %% encode_io_init_msg/5 tests
      {"encode_io_init_msg/5 with custom version",
       fun encode_io_init_msg_5_custom_version/0},
      {"encode_io_init_msg/5 with IO_PROTOCOL_VERSION",
       fun encode_io_init_msg_5_io_version/0},
      {"encode_io_init_msg/5 with zeros",
       fun encode_io_init_msg_5_zeros/0},
      {"encode_io_init_msg/5 with max values",
       fun encode_io_init_msg_5_max_values/0},

      %% encode_io_init_msg_fixed_key/4 tests
      {"encode_io_init_msg_fixed_key basic",
       fun encode_io_init_msg_fixed_key_basic/0},
      {"encode_io_init_msg_fixed_key with version",
       fun encode_io_init_msg_fixed_key_version/0},
      {"encode_io_init_msg_fixed_key zeros",
       fun encode_io_init_msg_fixed_key_zeros/0},
      {"encode_io_init_msg_fixed_key max values",
       fun encode_io_init_msg_fixed_key_max/0},

      %% encode_io_hdr/4 tests
      {"encode_io_hdr STDIN",
       fun encode_io_hdr_stdin/0},
      {"encode_io_hdr STDOUT",
       fun encode_io_hdr_stdout/0},
      {"encode_io_hdr STDERR",
       fun encode_io_hdr_stderr/0},
      {"encode_io_hdr ALLSTDIN",
       fun encode_io_hdr_allstdin/0},
      {"encode_io_hdr CONNECTION_TEST",
       fun encode_io_hdr_connection_test/0},
      {"encode_io_hdr with zero length",
       fun encode_io_hdr_zero_length/0},
      {"encode_io_hdr with max values",
       fun encode_io_hdr_max_values/0},

      %% encode_stdout/3 tests
      {"encode_stdout basic",
       fun encode_stdout_basic/0},
      {"encode_stdout empty data",
       fun encode_stdout_empty/0},
      {"encode_stdout large data",
       fun encode_stdout_large/0},
      {"encode_stdout with task ids",
       fun encode_stdout_task_ids/0},

      %% encode_stderr/3 tests
      {"encode_stderr basic",
       fun encode_stderr_basic/0},
      {"encode_stderr empty data",
       fun encode_stderr_empty/0},
      {"encode_stderr large data",
       fun encode_stderr_large/0},
      {"encode_stderr with task ids",
       fun encode_stderr_task_ids/0},

      %% encode_eof/2 tests
      {"encode_eof basic",
       fun encode_eof_basic/0},
      {"encode_eof with task ids",
       fun encode_eof_task_ids/0},
      {"encode_eof zero ids",
       fun encode_eof_zero_ids/0},

      %% decode_io_hdr/1 tests
      {"decode_io_hdr STDIN",
       fun decode_io_hdr_stdin/0},
      {"decode_io_hdr STDOUT",
       fun decode_io_hdr_stdout/0},
      {"decode_io_hdr STDERR",
       fun decode_io_hdr_stderr/0},
      {"decode_io_hdr ALLSTDIN",
       fun decode_io_hdr_allstdin/0},
      {"decode_io_hdr incomplete 0 bytes",
       fun decode_io_hdr_incomplete_0/0},
      {"decode_io_hdr incomplete 5 bytes",
       fun decode_io_hdr_incomplete_5/0},
      {"decode_io_hdr incomplete 9 bytes",
       fun decode_io_hdr_incomplete_9/0},
      {"decode_io_hdr invalid",
       fun decode_io_hdr_invalid/0},
      {"decode_io_hdr extra data",
       fun decode_io_hdr_extra_data/0},

      %% Roundtrip tests
      {"roundtrip io_hdr",
       fun roundtrip_io_hdr/0},
      {"roundtrip stdout",
       fun roundtrip_stdout/0},
      {"roundtrip stderr",
       fun roundtrip_stderr/0},
      {"roundtrip eof",
       fun roundtrip_eof/0},

      %% Edge cases
      {"binary data with special chars",
       fun binary_special_chars/0},
      {"binary data with unicode",
       fun binary_unicode/0},
      {"header size verification",
       fun header_size_verification/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% encode_io_init_msg/4 Tests
%%%===================================================================

encode_io_init_msg_4_empty_key() ->
    Binary = flurm_io_protocol:encode_io_init_msg(0, 1, 1, <<>>),
    ?assert(is_binary(Binary)),
    %% Format: length:32, version:16, nodeid:32, stdout:32, stderr:32, keylen:32, key
    %% PayloadLen = 2 + 4 + 4 + 4 + 4 + 0 = 18
    <<PayloadLen:32/big, _Version:16/big, NodeId:32/big, _/binary>> = Binary,
    ?assertEqual(18, PayloadLen),
    ?assertEqual(0, NodeId).

encode_io_init_msg_4_with_key() ->
    Key = <<"test_io_key_1234567890">>,
    Binary = flurm_io_protocol:encode_io_init_msg(5, 2, 3, Key),
    ?assert(is_binary(Binary)),
    KeyLen = byte_size(Key),
    ExpectedPayloadLen = 2 + 4 + 4 + 4 + 4 + KeyLen,
    <<PayloadLen:32/big, _Version:16/big, NodeId:32/big, Stdout:32/big, Stderr:32/big, IoKeyLen:32/big, _/binary>> = Binary,
    ?assertEqual(ExpectedPayloadLen, PayloadLen),
    ?assertEqual(5, NodeId),
    ?assertEqual(2, Stdout),
    ?assertEqual(3, Stderr),
    ?assertEqual(KeyLen, IoKeyLen).

encode_io_init_msg_4_large_key() ->
    Key = binary:copy(<<"x">>, 1024),
    Binary = flurm_io_protocol:encode_io_init_msg(10, 8, 8, Key),
    ?assert(is_binary(Binary)),
    <<PayloadLen:32/big, _/binary>> = Binary,
    ExpectedPayloadLen = 2 + 4 + 4 + 4 + 4 + 1024,
    ?assertEqual(ExpectedPayloadLen, PayloadLen).

%%%===================================================================
%%% encode_io_init_msg/5 Tests
%%%===================================================================

encode_io_init_msg_5_custom_version() ->
    Binary = flurm_io_protocol:encode_io_init_msg(16#1234, 0, 1, 1, <<"key">>),
    ?assert(is_binary(Binary)),
    <<_PayloadLen:32/big, Version:16/big, _/binary>> = Binary,
    ?assertEqual(16#1234, Version).

encode_io_init_msg_5_io_version() ->
    Binary = flurm_io_protocol:encode_io_init_msg(?IO_PROTOCOL_VERSION, 0, 1, 1, <<>>),
    ?assert(is_binary(Binary)),
    <<_PayloadLen:32/big, Version:16/big, _/binary>> = Binary,
    ?assertEqual(?IO_PROTOCOL_VERSION, Version).

encode_io_init_msg_5_zeros() ->
    Binary = flurm_io_protocol:encode_io_init_msg(0, 0, 0, 0, <<>>),
    ?assert(is_binary(Binary)),
    <<PayloadLen:32/big, Version:16/big, NodeId:32/big, Stdout:32/big, Stderr:32/big, KeyLen:32/big>> = Binary,
    ?assertEqual(18, PayloadLen),
    ?assertEqual(0, Version),
    ?assertEqual(0, NodeId),
    ?assertEqual(0, Stdout),
    ?assertEqual(0, Stderr),
    ?assertEqual(0, KeyLen).

encode_io_init_msg_5_max_values() ->
    Binary = flurm_io_protocol:encode_io_init_msg(16#FFFF, 16#FFFFFFFF, 16#FFFFFFFF, 16#FFFFFFFF, <<>>),
    ?assert(is_binary(Binary)),
    <<_PayloadLen:32/big, Version:16/big, NodeId:32/big, Stdout:32/big, Stderr:32/big, _/binary>> = Binary,
    ?assertEqual(16#FFFF, Version),
    ?assertEqual(16#FFFFFFFF, NodeId),
    ?assertEqual(16#FFFFFFFF, Stdout),
    ?assertEqual(16#FFFFFFFF, Stderr).

%%%===================================================================
%%% encode_io_init_msg_fixed_key/4 Tests
%%%===================================================================

encode_io_init_msg_fixed_key_basic() ->
    Binary = flurm_io_protocol:encode_io_init_msg_fixed_key(?IO_PROTOCOL_VERSION, 0, 1, 1),
    ?assert(is_binary(Binary)),
    %% Format: version:16, nodeid:32, stdout:32, stderr:32, io_key:32_bytes
    ?assertEqual(2 + 4 + 4 + 4 + 32, byte_size(Binary)),
    <<Version:16/big, _/binary>> = Binary,
    ?assertEqual(?IO_PROTOCOL_VERSION, Version).

encode_io_init_msg_fixed_key_version() ->
    Binary = flurm_io_protocol:encode_io_init_msg_fixed_key(16#5678, 10, 5, 5),
    ?assert(is_binary(Binary)),
    <<Version:16/big, NodeId:32/big, Stdout:32/big, Stderr:32/big, IoKey:32/binary>> = Binary,
    ?assertEqual(16#5678, Version),
    ?assertEqual(10, NodeId),
    ?assertEqual(5, Stdout),
    ?assertEqual(5, Stderr),
    ?assertEqual(<<0:256>>, IoKey).

encode_io_init_msg_fixed_key_zeros() ->
    Binary = flurm_io_protocol:encode_io_init_msg_fixed_key(0, 0, 0, 0),
    ?assert(is_binary(Binary)),
    ?assertEqual(2 + 4 + 4 + 4 + 32, byte_size(Binary)).

encode_io_init_msg_fixed_key_max() ->
    Binary = flurm_io_protocol:encode_io_init_msg_fixed_key(16#FFFF, 16#FFFFFFFF, 16#FFFFFFFF, 16#FFFFFFFF),
    ?assert(is_binary(Binary)),
    <<Version:16/big, NodeId:32/big, Stdout:32/big, Stderr:32/big, _IoKey:32/binary>> = Binary,
    ?assertEqual(16#FFFF, Version),
    ?assertEqual(16#FFFFFFFF, NodeId),
    ?assertEqual(16#FFFFFFFF, Stdout),
    ?assertEqual(16#FFFFFFFF, Stderr).

%%%===================================================================
%%% encode_io_hdr/4 Tests
%%%===================================================================

encode_io_hdr_stdin() ->
    Binary = flurm_io_protocol:encode_io_hdr(?SLURM_IO_STDIN, 0, 0, 100),
    ?assert(is_binary(Binary)),
    ?assertEqual(10, byte_size(Binary)),
    <<Type:16/big, Gtaskid:16/big, Ltaskid:16/big, Length:32/big>> = Binary,
    ?assertEqual(?SLURM_IO_STDIN, Type),
    ?assertEqual(0, Gtaskid),
    ?assertEqual(0, Ltaskid),
    ?assertEqual(100, Length).

encode_io_hdr_stdout() ->
    Binary = flurm_io_protocol:encode_io_hdr(?SLURM_IO_STDOUT, 1, 2, 200),
    ?assert(is_binary(Binary)),
    <<Type:16/big, Gtaskid:16/big, Ltaskid:16/big, Length:32/big>> = Binary,
    ?assertEqual(?SLURM_IO_STDOUT, Type),
    ?assertEqual(1, Gtaskid),
    ?assertEqual(2, Ltaskid),
    ?assertEqual(200, Length).

encode_io_hdr_stderr() ->
    Binary = flurm_io_protocol:encode_io_hdr(?SLURM_IO_STDERR, 3, 4, 300),
    ?assert(is_binary(Binary)),
    <<Type:16/big, Gtaskid:16/big, Ltaskid:16/big, Length:32/big>> = Binary,
    ?assertEqual(?SLURM_IO_STDERR, Type),
    ?assertEqual(3, Gtaskid),
    ?assertEqual(4, Ltaskid),
    ?assertEqual(300, Length).

encode_io_hdr_allstdin() ->
    Binary = flurm_io_protocol:encode_io_hdr(?SLURM_IO_ALLSTDIN, 5, 6, 400),
    ?assert(is_binary(Binary)),
    <<Type:16/big, _/binary>> = Binary,
    ?assertEqual(?SLURM_IO_ALLSTDIN, Type).

encode_io_hdr_connection_test() ->
    Binary = flurm_io_protocol:encode_io_hdr(?SLURM_IO_CONNECTION_TEST, 7, 8, 0),
    ?assert(is_binary(Binary)),
    <<Type:16/big, _/binary>> = Binary,
    ?assertEqual(?SLURM_IO_CONNECTION_TEST, Type).

encode_io_hdr_zero_length() ->
    Binary = flurm_io_protocol:encode_io_hdr(?SLURM_IO_STDOUT, 0, 0, 0),
    ?assert(is_binary(Binary)),
    <<_:48, Length:32/big>> = Binary,
    ?assertEqual(0, Length).

encode_io_hdr_max_values() ->
    Binary = flurm_io_protocol:encode_io_hdr(16#FFFF, 16#FFFF, 16#FFFF, 16#FFFFFFFF),
    ?assert(is_binary(Binary)),
    <<Type:16/big, Gtaskid:16/big, Ltaskid:16/big, Length:32/big>> = Binary,
    ?assertEqual(16#FFFF, Type),
    ?assertEqual(16#FFFF, Gtaskid),
    ?assertEqual(16#FFFF, Ltaskid),
    ?assertEqual(16#FFFFFFFF, Length).

%%%===================================================================
%%% encode_stdout/3 Tests
%%%===================================================================

encode_stdout_basic() ->
    Data = <<"Hello, World!\n">>,
    Binary = flurm_io_protocol:encode_stdout(0, 0, Data),
    ?assert(is_binary(Binary)),
    ?assertEqual(10 + byte_size(Data), byte_size(Binary)),
    <<Type:16/big, _Gtaskid:16/big, _Ltaskid:16/big, Length:32/big, OutputData/binary>> = Binary,
    ?assertEqual(?SLURM_IO_STDOUT, Type),
    ?assertEqual(byte_size(Data), Length),
    ?assertEqual(Data, OutputData).

encode_stdout_empty() ->
    Binary = flurm_io_protocol:encode_stdout(0, 0, <<>>),
    ?assert(is_binary(Binary)),
    ?assertEqual(10, byte_size(Binary)),
    <<_:48, Length:32/big>> = Binary,
    ?assertEqual(0, Length).

encode_stdout_large() ->
    Data = binary:copy(<<"x">>, 65536),
    Binary = flurm_io_protocol:encode_stdout(0, 0, Data),
    ?assert(is_binary(Binary)),
    ?assertEqual(10 + 65536, byte_size(Binary)).

encode_stdout_task_ids() ->
    Data = <<"output">>,
    Binary = flurm_io_protocol:encode_stdout(100, 50, Data),
    <<_Type:16/big, Gtaskid:16/big, Ltaskid:16/big, _/binary>> = Binary,
    ?assertEqual(100, Gtaskid),
    ?assertEqual(50, Ltaskid).

%%%===================================================================
%%% encode_stderr/3 Tests
%%%===================================================================

encode_stderr_basic() ->
    Data = <<"Error message\n">>,
    Binary = flurm_io_protocol:encode_stderr(0, 0, Data),
    ?assert(is_binary(Binary)),
    ?assertEqual(10 + byte_size(Data), byte_size(Binary)),
    <<Type:16/big, _Gtaskid:16/big, _Ltaskid:16/big, Length:32/big, ErrorData/binary>> = Binary,
    ?assertEqual(?SLURM_IO_STDERR, Type),
    ?assertEqual(byte_size(Data), Length),
    ?assertEqual(Data, ErrorData).

encode_stderr_empty() ->
    Binary = flurm_io_protocol:encode_stderr(0, 0, <<>>),
    ?assert(is_binary(Binary)),
    ?assertEqual(10, byte_size(Binary)),
    <<_:48, Length:32/big>> = Binary,
    ?assertEqual(0, Length).

encode_stderr_large() ->
    Data = binary:copy(<<"e">>, 32768),
    Binary = flurm_io_protocol:encode_stderr(0, 0, Data),
    ?assert(is_binary(Binary)),
    ?assertEqual(10 + 32768, byte_size(Binary)).

encode_stderr_task_ids() ->
    Data = <<"error">>,
    Binary = flurm_io_protocol:encode_stderr(200, 150, Data),
    <<_Type:16/big, Gtaskid:16/big, Ltaskid:16/big, _/binary>> = Binary,
    ?assertEqual(200, Gtaskid),
    ?assertEqual(150, Ltaskid).

%%%===================================================================
%%% encode_eof/2 Tests
%%%===================================================================

encode_eof_basic() ->
    Binary = flurm_io_protocol:encode_eof(0, 0),
    ?assert(is_binary(Binary)),
    ?assertEqual(10, byte_size(Binary)),
    <<Type:16/big, Gtaskid:16/big, Ltaskid:16/big, Length:32/big>> = Binary,
    ?assertEqual(?SLURM_IO_STDOUT, Type),
    ?assertEqual(0, Gtaskid),
    ?assertEqual(0, Ltaskid),
    ?assertEqual(0, Length).

encode_eof_task_ids() ->
    Binary = flurm_io_protocol:encode_eof(500, 250),
    <<_Type:16/big, Gtaskid:16/big, Ltaskid:16/big, Length:32/big>> = Binary,
    ?assertEqual(500, Gtaskid),
    ?assertEqual(250, Ltaskid),
    ?assertEqual(0, Length).

encode_eof_zero_ids() ->
    Binary = flurm_io_protocol:encode_eof(0, 0),
    <<_Type:16/big, Gtaskid:16/big, Ltaskid:16/big, _/binary>> = Binary,
    ?assertEqual(0, Gtaskid),
    ?assertEqual(0, Ltaskid).

%%%===================================================================
%%% decode_io_hdr/1 Tests
%%%===================================================================

decode_io_hdr_stdin() ->
    Hdr = flurm_io_protocol:encode_io_hdr(?SLURM_IO_STDIN, 1, 2, 100),
    Result = flurm_io_protocol:decode_io_hdr(Hdr),
    ?assertEqual({ok, ?SLURM_IO_STDIN, 1, 2, 100}, Result).

decode_io_hdr_stdout() ->
    Hdr = flurm_io_protocol:encode_io_hdr(?SLURM_IO_STDOUT, 3, 4, 200),
    Result = flurm_io_protocol:decode_io_hdr(Hdr),
    ?assertEqual({ok, ?SLURM_IO_STDOUT, 3, 4, 200}, Result).

decode_io_hdr_stderr() ->
    Hdr = flurm_io_protocol:encode_io_hdr(?SLURM_IO_STDERR, 5, 6, 300),
    Result = flurm_io_protocol:decode_io_hdr(Hdr),
    ?assertEqual({ok, ?SLURM_IO_STDERR, 5, 6, 300}, Result).

decode_io_hdr_allstdin() ->
    Hdr = flurm_io_protocol:encode_io_hdr(?SLURM_IO_ALLSTDIN, 7, 8, 400),
    Result = flurm_io_protocol:decode_io_hdr(Hdr),
    ?assertEqual({ok, ?SLURM_IO_ALLSTDIN, 7, 8, 400}, Result).

decode_io_hdr_incomplete_0() ->
    Result = flurm_io_protocol:decode_io_hdr(<<>>),
    ?assertEqual({error, {incomplete, need, 10}}, Result).

decode_io_hdr_incomplete_5() ->
    Result = flurm_io_protocol:decode_io_hdr(<<1, 2, 3, 4, 5>>),
    ?assertEqual({error, {incomplete, need, 5}}, Result).

decode_io_hdr_incomplete_9() ->
    Result = flurm_io_protocol:decode_io_hdr(<<1, 2, 3, 4, 5, 6, 7, 8, 9>>),
    ?assertEqual({error, {incomplete, need, 1}}, Result).

decode_io_hdr_invalid() ->
    %% 11 bytes - more than header but not valid
    Result = flurm_io_protocol:decode_io_hdr(<<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11>>),
    %% Should succeed with first 10 bytes
    ?assertMatch({ok, _, _, _, _}, Result).

decode_io_hdr_extra_data() ->
    %% Header with extra trailing data - should only decode header portion
    Hdr = flurm_io_protocol:encode_io_hdr(?SLURM_IO_STDOUT, 0, 0, 5),
    HdrWithData = <<Hdr/binary, "extra">>,
    %% decode_io_hdr expects exactly 10 bytes, but let's verify behavior
    Result = flurm_io_protocol:decode_io_hdr(<<Hdr/binary>>),
    ?assertEqual({ok, ?SLURM_IO_STDOUT, 0, 0, 5}, Result),
    %% With extra data it still works (matches first 10 bytes)
    Result2 = flurm_io_protocol:decode_io_hdr(HdrWithData),
    ?assertEqual({ok, ?SLURM_IO_STDOUT, 0, 0, 5}, Result2).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_io_hdr() ->
    Types = [?SLURM_IO_STDIN, ?SLURM_IO_STDOUT, ?SLURM_IO_STDERR,
             ?SLURM_IO_ALLSTDIN, ?SLURM_IO_CONNECTION_TEST],
    lists:foreach(fun(Type) ->
        Gtaskid = rand:uniform(65535),
        Ltaskid = rand:uniform(65535),
        Length = rand:uniform(1000000),
        Encoded = flurm_io_protocol:encode_io_hdr(Type, Gtaskid, Ltaskid, Length),
        {ok, DType, DGtaskid, DLtaskid, DLength} = flurm_io_protocol:decode_io_hdr(Encoded),
        ?assertEqual(Type, DType),
        ?assertEqual(Gtaskid, DGtaskid),
        ?assertEqual(Ltaskid, DLtaskid),
        ?assertEqual(Length, DLength)
    end, Types).

roundtrip_stdout() ->
    Data = <<"Test stdout data with special chars: !@#$%^&*()">>,
    Gtaskid = 42,
    Ltaskid = 17,
    Encoded = flurm_io_protocol:encode_stdout(Gtaskid, Ltaskid, Data),
    <<Hdr:10/binary, RecoveredData/binary>> = Encoded,
    {ok, Type, RGtaskid, RLtaskid, Length} = flurm_io_protocol:decode_io_hdr(Hdr),
    ?assertEqual(?SLURM_IO_STDOUT, Type),
    ?assertEqual(Gtaskid, RGtaskid),
    ?assertEqual(Ltaskid, RLtaskid),
    ?assertEqual(byte_size(Data), Length),
    ?assertEqual(Data, RecoveredData).

roundtrip_stderr() ->
    Data = <<"Test stderr data\nWith newlines\nAnd more">>,
    Gtaskid = 99,
    Ltaskid = 88,
    Encoded = flurm_io_protocol:encode_stderr(Gtaskid, Ltaskid, Data),
    <<Hdr:10/binary, RecoveredData/binary>> = Encoded,
    {ok, Type, RGtaskid, RLtaskid, Length} = flurm_io_protocol:decode_io_hdr(Hdr),
    ?assertEqual(?SLURM_IO_STDERR, Type),
    ?assertEqual(Gtaskid, RGtaskid),
    ?assertEqual(Ltaskid, RLtaskid),
    ?assertEqual(byte_size(Data), Length),
    ?assertEqual(Data, RecoveredData).

roundtrip_eof() ->
    Gtaskid = 123,
    Ltaskid = 456,
    Encoded = flurm_io_protocol:encode_eof(Gtaskid, Ltaskid),
    {ok, Type, RGtaskid, RLtaskid, Length} = flurm_io_protocol:decode_io_hdr(Encoded),
    ?assertEqual(?SLURM_IO_STDOUT, Type),
    ?assertEqual(Gtaskid, RGtaskid),
    ?assertEqual(Ltaskid, RLtaskid),
    ?assertEqual(0, Length).

%%%===================================================================
%%% Edge Cases
%%%===================================================================

binary_special_chars() ->
    Data = <<0, 1, 2, 3, 255, 254, 253, "\t\r\n", 127>>,
    Stdout = flurm_io_protocol:encode_stdout(0, 0, Data),
    <<_Hdr:10/binary, RecoveredData/binary>> = Stdout,
    ?assertEqual(Data, RecoveredData).

binary_unicode() ->
    Data = <<"Hello ", 195, 169, 195, 168, 195, 160, " World ", 226, 128, 147>>,  % e-accents and en-dash
    Stdout = flurm_io_protocol:encode_stdout(0, 0, Data),
    <<_Hdr:10/binary, RecoveredData/binary>> = Stdout,
    ?assertEqual(Data, RecoveredData).

header_size_verification() ->
    %% Verify SLURM_IO_HDR_SIZE constant matches actual header size
    Hdr = flurm_io_protocol:encode_io_hdr(0, 0, 0, 0),
    ?assertEqual(?SLURM_IO_HDR_SIZE, byte_size(Hdr)).

%%%===================================================================
%%% Additional Edge Cases
%%%===================================================================

%% Test all task ID combinations
task_id_boundary_test_() ->
    TaskIds = [0, 1, 255, 256, 65534, 65535],
    [
        {lists:flatten(io_lib:format("stdout gtaskid=~p ltaskid=~p", [G, L])),
         fun() ->
             Data = <<"test">>,
             Binary = flurm_io_protocol:encode_stdout(G, L, Data),
             <<_Type:16/big, Gtaskid:16/big, Ltaskid:16/big, _/binary>> = Binary,
             ?assertEqual(G, Gtaskid),
             ?assertEqual(L, Ltaskid)
         end}
    || G <- TaskIds, L <- TaskIds].

%% Test different data lengths
data_length_test_() ->
    Lengths = [0, 1, 255, 256, 65535, 65536],
    [
        {lists:flatten(io_lib:format("stdout with length ~p", [Len])),
         fun() ->
             Data = binary:copy(<<"x">>, Len),
             Binary = flurm_io_protocol:encode_stdout(0, 0, Data),
             ?assertEqual(10 + Len, byte_size(Binary))
         end}
    || Len <- Lengths].

%% Test init message key sizes
key_size_test_() ->
    KeySizes = [0, 1, 32, 64, 128, 256, 512, 1024],
    [
        {lists:flatten(io_lib:format("init_msg with key size ~p", [Size])),
         fun() ->
             Key = binary:copy(<<"k">>, Size),
             Binary = flurm_io_protocol:encode_io_init_msg(0, 1, 1, Key),
             ?assert(is_binary(Binary)),
             ExpectedPayloadLen = 2 + 4 + 4 + 4 + 4 + Size,
             <<PayloadLen:32/big, _/binary>> = Binary,
             ?assertEqual(ExpectedPayloadLen, PayloadLen)
         end}
    || Size <- KeySizes].
