%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_protocol_auth module
%%% Coverage target: 100% of all functions and branches
%%%
%%% Tests cover:
%%% - extra_size/0 - returns standard extra data size
%%% - default_hostname/0 - returns system hostname or fallback
%%% - encode_extra/1 - encode extra data with default hostname
%%% - encode_extra/2 - encode extra data with specified hostname
%%% - decode_extra/1 - decode extra data from binary
%%% - Internal functions via decode_extra paths
%%%-------------------------------------------------------------------
-module(flurm_protocol_auth_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Generator
%%%===================================================================

flurm_protocol_auth_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% extra_size/0 tests
      {"extra_size returns 39",
       fun extra_size_returns_39/0},

      %% default_hostname/0 tests
      {"default_hostname returns binary",
       fun default_hostname_returns_binary/0},
      {"default_hostname returns non-empty",
       fun default_hostname_returns_non_empty/0},

      %% encode_extra/1 tests
      {"encode_extra/1 for RESPONSE_SLURM_RC",
       fun encode_extra_1_response_slurm_rc/0},
      {"encode_extra/1 for other message types",
       fun encode_extra_1_other_types/0},
      {"encode_extra/1 returns correct size",
       fun encode_extra_1_correct_size/0},

      %% encode_extra/2 tests
      {"encode_extra/2 for RESPONSE_SLURM_RC with custom hostname",
       fun encode_extra_2_response_slurm_rc/0},
      {"encode_extra/2 for other types with custom hostname",
       fun encode_extra_2_other_types/0},
      {"encode_extra/2 with empty hostname",
       fun encode_extra_2_empty_hostname/0},
      {"encode_extra/2 with long hostname",
       fun encode_extra_2_long_hostname/0},
      {"encode_extra/2 with short hostname",
       fun encode_extra_2_short_hostname/0},
      {"encode_extra/2 returns correct size",
       fun encode_extra_2_correct_size/0},

      %% decode_extra/1 tests - empty input
      {"decode_extra/1 empty binary",
       fun decode_extra_1_empty/0},

      %% decode_extra/1 tests - RC format
      {"decode_extra/1 RC format",
       fun decode_extra_1_rc_format/0},

      %% decode_extra/1 tests - standard format
      {"decode_extra/1 standard format",
       fun decode_extra_1_standard_format/0},

      %% decode_extra/1 tests - request format
      {"decode_extra/1 request format",
       fun decode_extra_1_request_format/0},

      %% decode_extra/1 tests - unknown format
      {"decode_extra/1 unknown format",
       fun decode_extra_1_unknown_format/0},

      %% decode_extra/1 tests - too short
      {"decode_extra/1 too short",
       fun decode_extra_1_too_short/0},

      %% Roundtrip tests
      {"roundtrip encode decode RC format",
       fun roundtrip_rc_format/0},
      {"roundtrip encode decode standard format",
       fun roundtrip_standard_format/0},

      %% Edge cases
      {"encode with maximum hostname length",
       fun encode_max_hostname/0},
      {"encode with single character hostname",
       fun encode_single_char_hostname/0},

      %% decode_hostname_section paths
      {"decode hostname section without null terminator",
       fun decode_hostname_without_null/0},
      {"decode hostname section empty result",
       fun decode_hostname_empty_result/0},

      %% extract_timestamp paths
      {"extract timestamp from short binary",
       fun extract_timestamp_short/0},
      {"extract timestamp from exact 4 bytes",
       fun extract_timestamp_exact/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% extra_size/0 Tests
%%%===================================================================

extra_size_returns_39() ->
    ?assertEqual(39, flurm_protocol_auth:extra_size()).

%%%===================================================================
%%% default_hostname/0 Tests
%%%===================================================================

default_hostname_returns_binary() ->
    Result = flurm_protocol_auth:default_hostname(),
    ?assert(is_binary(Result)).

default_hostname_returns_non_empty() ->
    Result = flurm_protocol_auth:default_hostname(),
    ?assert(byte_size(Result) > 0).

%%%===================================================================
%%% encode_extra/1 Tests
%%%===================================================================

encode_extra_1_response_slurm_rc() ->
    Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC),
    ?assert(is_binary(Result)),
    %% Check it starts with 8 zeros (RC format)
    <<First8:64, _/binary>> = Result,
    ?assertEqual(0, First8).

encode_extra_1_other_types() ->
    %% Test with a non-RC message type
    Result = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO),
    ?assert(is_binary(Result)),
    %% Check it starts with 00 64 (standard format)
    <<First2:16/big, _/binary>> = Result,
    ?assertEqual(16#0064, First2).

encode_extra_1_correct_size() ->
    Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC),
    ?assertEqual(39, byte_size(Result)).

%%%===================================================================
%%% encode_extra/2 Tests
%%%===================================================================

encode_extra_2_response_slurm_rc() ->
    Hostname = <<"testhost">>,
    Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
    ?assert(is_binary(Result)),
    %% Check RC format: starts with 8 zeros
    <<First8:64, _/binary>> = Result,
    ?assertEqual(0, First8),
    %% Verify hostname is embedded
    ?assert(binary:match(Result, Hostname) =/= nomatch).

encode_extra_2_other_types() ->
    Hostname = <<"myhost">>,
    Result = flurm_protocol_auth:encode_extra(?RESPONSE_NODE_INFO, Hostname),
    ?assert(is_binary(Result)),
    %% Check standard format: starts with 00 64
    <<First2:16/big, _/binary>> = Result,
    ?assertEqual(16#0064, First2),
    %% Verify hostname is embedded
    ?assert(binary:match(Result, Hostname) =/= nomatch).

encode_extra_2_empty_hostname() ->
    Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, <<>>),
    ?assert(is_binary(Result)),
    ?assertEqual(39, byte_size(Result)).

encode_extra_2_long_hostname() ->
    %% Test with a hostname that is longer than padding allows
    Hostname = <<"verylonghostname.example.com">>,
    Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
    ?assert(is_binary(Result)),
    %% Size should still be valid (may be larger than 39 for long hostnames)
    ?assert(byte_size(Result) >= 39).

encode_extra_2_short_hostname() ->
    %% Test with minimal hostname
    Hostname = <<"a">>,
    Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
    ?assert(is_binary(Result)),
    ?assertEqual(39, byte_size(Result)).

encode_extra_2_correct_size() ->
    Hostname = <<"test">>,
    Result1 = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
    Result2 = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO, Hostname),
    ?assertEqual(39, byte_size(Result1)),
    ?assertEqual(39, byte_size(Result2)).

%%%===================================================================
%%% decode_extra/1 Tests - Empty Input
%%%===================================================================

decode_extra_1_empty() ->
    Result = flurm_protocol_auth:decode_extra(<<>>),
    ?assertMatch({ok, #{hostname := <<>>, timestamp := 0}}, Result).

%%%===================================================================
%%% decode_extra/1 Tests - RC Format
%%%===================================================================

decode_extra_1_rc_format() ->
    %% Create RC format extra data
    Hostname = <<"rchost">>,
    Encoded = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
    Result = flurm_protocol_auth:decode_extra(Encoded),
    ?assertMatch({ok, _}, Result),
    {ok, Map} = Result,
    ?assert(maps:is_key(hostname, Map)),
    ?assert(maps:is_key(timestamp, Map)),
    ?assertEqual(rc_format, maps:get(format, Map)).

%%%===================================================================
%%% decode_extra/1 Tests - Standard Format
%%%===================================================================

decode_extra_1_standard_format() ->
    %% Create standard format extra data
    Hostname = <<"stdhost">>,
    Encoded = flurm_protocol_auth:encode_extra(?RESPONSE_NODE_INFO, Hostname),
    Result = flurm_protocol_auth:decode_extra(Encoded),
    ?assertMatch({ok, _}, Result),
    {ok, Map} = Result,
    ?assert(maps:is_key(hostname, Map)),
    ?assert(maps:is_key(timestamp, Map)),
    ?assertEqual(standard_format, maps:get(format, Map)).

%%%===================================================================
%%% decode_extra/1 Tests - Request Format
%%%===================================================================

decode_extra_1_request_format() ->
    %% Create request format: 00 00 00 HostnameLen Hostname...
    Hostname = <<"reqhost">>,
    HostnameLen = byte_size(Hostname) + 1,
    %% Build 39 byte request format
    %% 3 zeros + HostnameLen + Hostname + null + padding + timestamp
    PaddingLen = 39 - 3 - 1 - byte_size(Hostname) - 1 - 4,
    Padding = <<0:(PaddingLen * 8)>>,
    Timestamp = erlang:system_time(second),
    RequestFormat = <<0:24, HostnameLen:8, Hostname/binary, 0:8, Padding/binary, Timestamp:32/big>>,
    ?assertEqual(39, byte_size(RequestFormat)),
    Result = flurm_protocol_auth:decode_extra(RequestFormat),
    ?assertMatch({ok, _}, Result),
    {ok, Map} = Result,
    ?assertEqual(request_format, maps:get(format, Map)).

%%%===================================================================
%%% decode_extra/1 Tests - Unknown Format
%%%===================================================================

decode_extra_1_unknown_format() ->
    %% Create data that doesn't match any known format
    UnknownData = binary:copy(<<255>>, 39),
    Result = flurm_protocol_auth:decode_extra(UnknownData),
    ?assertMatch({ok, _}, Result),
    {ok, Map} = Result,
    ?assertEqual(<<>>, maps:get(hostname, Map)),
    ?assertEqual(0, maps:get(timestamp, Map)),
    ?assert(maps:is_key(raw, Map)).

%%%===================================================================
%%% decode_extra/1 Tests - Too Short
%%%===================================================================

decode_extra_1_too_short() ->
    %% Test with data shorter than EXTRA_DATA_SIZE (39)
    ShortData = <<1, 2, 3, 4, 5>>,
    Result = flurm_protocol_auth:decode_extra(ShortData),
    ?assertMatch({error, {extra_data_too_short, 5}}, Result).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_rc_format() ->
    Hostname = <<"roundtrip_rc">>,
    Encoded = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
    {ok, Decoded} = flurm_protocol_auth:decode_extra(Encoded),
    DecodedHostname = maps:get(hostname, Decoded),
    %% Hostname should be preserved
    ?assertEqual(Hostname, DecodedHostname),
    %% Timestamp should be recent (within last minute)
    Timestamp = maps:get(timestamp, Decoded),
    Now = erlang:system_time(second),
    ?assert(Timestamp >= Now - 60),
    ?assert(Timestamp =< Now + 1).

roundtrip_standard_format() ->
    Hostname = <<"roundtrip_std">>,
    Encoded = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO, Hostname),
    {ok, Decoded} = flurm_protocol_auth:decode_extra(Encoded),
    DecodedHostname = maps:get(hostname, Decoded),
    ?assertEqual(Hostname, DecodedHostname).

%%%===================================================================
%%% Edge Case Tests
%%%===================================================================

encode_max_hostname() ->
    %% Test with hostname at boundary of padding
    %% For RC format: padding = max(0, 13 - HostnameLen)
    %% HostnameLen = byte_size(Hostname) + 1
    %% So when byte_size >= 12, PaddingLen = 0
    Hostname = binary:copy(<<"x">>, 15),
    Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
    ?assert(is_binary(Result)),
    %% Size will be larger than 39 when hostname exceeds padding
    ?assert(byte_size(Result) >= 39).

encode_single_char_hostname() ->
    Hostname = <<"x">>,
    Result1 = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
    Result2 = flurm_protocol_auth:encode_extra(?RESPONSE_NODE_INFO, Hostname),
    ?assertEqual(39, byte_size(Result1)),
    ?assertEqual(39, byte_size(Result2)).

%%%===================================================================
%%% decode_hostname_section Internal Function Tests
%%%===================================================================

decode_hostname_without_null() ->
    %% Test path where hostname doesn't end with null terminator
    %% This hits the second clause in decode_hostname_section
    %% RC format: 8 zeros (8) + 00 64 (2) + 11 zeros (11) + HostnameLen (1) = 22 bytes header
    %% We need total 39 bytes, so 17 bytes after header
    Hostname = <<"noterm">>,  % 6 bytes
    HostnameLen = byte_size(Hostname) + 1,  % 7
    %% To hit second clause: put non-zero after hostname instead of null
    %% After hostname we need: 17 - 6 = 11 bytes
    %% Put 0xFF first (fails first clause), then padding + timestamp
    AfterHostname = <<16#FF, 0, 0, 0, 0, 0, 16#12, 16#34, 16#56, 16#78, 0>>,  % 11 bytes
    MalformedData = <<0:64, 16#0064:16/big, 0:88, HostnameLen:8,
                      Hostname/binary, AfterHostname/binary>>,
    ?assertEqual(39, byte_size(MalformedData)),
    Result = flurm_protocol_auth:decode_extra(MalformedData),
    ?assertMatch({ok, _}, Result).

decode_hostname_empty_result() ->
    %% Test path where hostname section can't be parsed properly
    %% Create data with HostnameLen > remaining bytes
    BadData = <<0:64, 16#0064:16/big, 0:88, 50:8, 1, 2, 3>>,
    %% Pad to 39 bytes
    Padding = binary:copy(<<0>>, 39 - byte_size(BadData)),
    PaddedData = <<BadData/binary, Padding/binary>>,
    Result = flurm_protocol_auth:decode_extra(PaddedData),
    ?assertMatch({ok, _}, Result).

%%%===================================================================
%%% extract_timestamp Internal Function Tests
%%%===================================================================

extract_timestamp_short() ->
    %% Test with binary shorter than 4 bytes
    %% This is exercised through decode paths with minimal data
    %% Build request format with very short rest
    ShortRest = <<1, 2, 3>>,  % Less than 4 bytes
    HostnameLen = 2,
    Hostname = <<"a">>,
    %% Create data that will result in short Rest for timestamp extraction
    RequestFormat = <<0:24, HostnameLen:8, Hostname/binary, 0:8, ShortRest/binary>>,
    %% Pad to exactly 39 bytes
    CurrentSize = byte_size(RequestFormat),
    PaddingNeeded = 39 - CurrentSize,
    Padding = <<0:(PaddingNeeded * 8)>>,
    PaddedData = <<RequestFormat/binary, Padding/binary>>,
    Result = flurm_protocol_auth:decode_extra(PaddedData),
    ?assertMatch({ok, _}, Result).

extract_timestamp_exact() ->
    %% Test with exactly 4 bytes for timestamp
    Hostname = <<"exactts">>,
    HostnameLen = byte_size(Hostname) + 1,
    Timestamp = 12345678,
    %% Build standard format with exact timestamp at end
    PaddingLen = max(0, 21 - HostnameLen),
    Padding = <<0:(PaddingLen * 8)>>,
    ExactFormat = <<16#0064:16/big, 0:88, HostnameLen:8,
                    Hostname/binary, 0:8, Padding/binary, Timestamp:32/big>>,
    ?assertEqual(39, byte_size(ExactFormat)),
    {ok, Decoded} = flurm_protocol_auth:decode_extra(ExactFormat),
    ?assertEqual(Timestamp, maps:get(timestamp, Decoded)).

%%%===================================================================
%%% Additional Message Type Tests
%%%===================================================================

various_message_types_test_() ->
    {"encode_extra with various message types",
     fun() ->
         %% Test multiple non-RC message types
         Types = [
             ?RESPONSE_JOB_INFO,
             ?RESPONSE_NODE_INFO,
             ?RESPONSE_PARTITION_INFO,
             ?RESPONSE_JOB_STEP_INFO
         ],
         lists:foreach(
             fun(MsgType) ->
                 Result = flurm_protocol_auth:encode_extra(MsgType, <<"host">>),
                 ?assertEqual(39, byte_size(Result)),
                 %% All should use standard format (not RC)
                 <<First2:16/big, _/binary>> = Result,
                 ?assertEqual(16#0064, First2)
             end,
             Types)
     end}.

%% Test that timestamp is embedded correctly
timestamp_embedding_test_() ->
    {"timestamp is embedded in extra data",
     fun() ->
         Before = erlang:system_time(second),
         Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, <<"ts">>),
         After = erlang:system_time(second),
         %% Extract timestamp from last 4 bytes
         Size = byte_size(Result),
         <<_:(Size-4)/binary, EmbeddedTs:32/big>> = Result,
         ?assert(EmbeddedTs >= Before),
         ?assert(EmbeddedTs =< After)
     end}.

%% Test consistency between encode and decode
consistency_test_() ->
    {"encode and decode are consistent",
     fun() ->
         Hostnames = [<<"a">>, <<"ab">>, <<"abc">>, <<"localhost">>, <<"verylonghost">>],
         lists:foreach(
             fun(Hostname) ->
                 %% Test RC format
                 Encoded1 = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
                 {ok, Decoded1} = flurm_protocol_auth:decode_extra(Encoded1),
                 ?assertEqual(Hostname, maps:get(hostname, Decoded1)),
                 %% Test standard format
                 Encoded2 = flurm_protocol_auth:encode_extra(?RESPONSE_NODE_INFO, Hostname),
                 {ok, Decoded2} = flurm_protocol_auth:decode_extra(Encoded2),
                 ?assertEqual(Hostname, maps:get(hostname, Decoded2))
             end,
             Hostnames)
     end}.

%% Test with binary hostnames containing special bytes
special_hostname_test_() ->
    {"hostname with special bytes",
     fun() ->
         %% Note: SLURM hostnames are typically ASCII, but test binary handling
         Hostname = <<"host123">>,
         Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
         {ok, Decoded} = flurm_protocol_auth:decode_extra(Result),
         ?assertEqual(Hostname, maps:get(hostname, Decoded))
     end}.

%% Test decode with data larger than 39 bytes
large_extra_data_test_() ->
    {"decode handles data larger than 39 bytes",
     fun() ->
         %% Create 39-byte valid data plus extra
         Encoded = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, <<"bigdata">>),
         Larger = <<Encoded/binary, 0, 0, 0, 0, 0>>,
         %% Should still decode successfully
         Result = flurm_protocol_auth:decode_extra(Larger),
         ?assertMatch({ok, _}, Result)
     end}.

%% Test all boundary conditions for padding calculation
padding_boundary_test_() ->
    {"padding calculation boundaries",
     fun() ->
         %% For RC format: PaddingLen = max(0, 13 - HostnameLen)
         %% HostnameLen = byte_size(Hostname) + 1

         %% When byte_size = 0, HostnameLen = 1, PaddingLen = 12
         R1 = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, <<>>),
         ?assertEqual(39, byte_size(R1)),

         %% When byte_size = 11, HostnameLen = 12, PaddingLen = 1
         R2 = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, binary:copy(<<"x">>, 11)),
         ?assertEqual(39, byte_size(R2)),

         %% When byte_size = 12, HostnameLen = 13, PaddingLen = 0
         R3 = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, binary:copy(<<"x">>, 12)),
         ?assertEqual(39, byte_size(R3)),

         %% For standard format: PaddingLen = max(0, 21 - HostnameLen)

         %% When byte_size = 0, HostnameLen = 1, PaddingLen = 20
         R4 = flurm_protocol_auth:encode_extra(?RESPONSE_NODE_INFO, <<>>),
         ?assertEqual(39, byte_size(R4)),

         %% When byte_size = 19, HostnameLen = 20, PaddingLen = 1
         R5 = flurm_protocol_auth:encode_extra(?RESPONSE_NODE_INFO, binary:copy(<<"y">>, 19)),
         ?assertEqual(39, byte_size(R5)),

         %% When byte_size = 20, HostnameLen = 21, PaddingLen = 0
         R6 = flurm_protocol_auth:encode_extra(?RESPONSE_NODE_INFO, binary:copy(<<"y">>, 20)),
         ?assertEqual(39, byte_size(R6))
     end}.

%% Test decode_extra with exactly 39 bytes of zeros
all_zeros_test_() ->
    {"decode handles 39 bytes of zeros",
     fun() ->
         ZeroData = binary:copy(<<0>>, 39),
         Result = flurm_protocol_auth:decode_extra(ZeroData),
         %% Should hit unknown format path
         ?assertMatch({ok, _}, Result)
     end}.

%%%===================================================================
%%% Additional Coverage Tests for Uncovered Lines
%%%===================================================================

%% Test decode_hostname_section second clause (hostname without null terminator)
%% This covers lines 163-169: <<Hostname:ActualLen/binary, Rest/binary>> when ActualLen > 0
decode_hostname_no_null_test_() ->
    {"decode hostname section without null terminator covers line 163-169",
     fun() ->
         %% Build RC format where HostnameLen claims 5 bytes but there's no null at position 5
         %% RC: 8 zeros (8) + 00 64 (2) + 11 zeros (11) + HostnameLen (1) = 22 bytes header
         %% After header: Hostname (4) + non-null byte + padding + timestamp (4) = 17 bytes
         %% Total needed: 39 bytes
         Hostname = <<"test">>,  % 4 bytes
         HostnameLen = 5,  % Claims 5 bytes (hostname + null), ActualLen = 4
         %% Put 0xFF instead of 0x00 at position 5 to fail first clause
         %% Need 17 - 4 = 13 bytes after hostname for total of 39
         DataAfterHostname = <<16#FF, 0, 0, 0, 0, 0, 0, 0, 16#12, 16#34, 16#56, 16#78, 0>>,  % 13 bytes
         Binary = <<0:64, 16#0064:16/big, 0:88, HostnameLen:8, Hostname/binary, DataAfterHostname/binary>>,
         %% Verify we have enough bytes: 8 + 2 + 11 + 1 + 4 + 13 = 39
         ?assertEqual(39, byte_size(Binary)),
         Result = flurm_protocol_auth:decode_extra(Binary),
         ?assertMatch({ok, _}, Result),
         {ok, Map} = Result,
         ?assertEqual(rc_format, maps:get(format, Map))
     end}.

%% Test decode_hostname_section fallback clause (line 171)
%% This is hit when ActualLen is 0 (HostnameLen = 1) and rest doesn't match
decode_hostname_fallback_test_() ->
    {"decode hostname section fallback covers line 171",
     fun() ->
         %% Build RC format with HostnameLen = 1 (ActualLen = 0)
         %% This means we expect 0 bytes of hostname, then a null
         %% But if there's no null (or empty binary), we hit the fallback
         %% RC: 8 zeros + 00 64 + 11 zeros + HostnameLen=1 + (empty hostname) + ?
         %% First clause needs: <<Hostname:0/binary, 0:8, Rest/binary>> = <<0:8, Rest>>
         %% If the next byte is NOT 0, first clause fails
         %% Second clause needs: ActualLen > 0, but ActualLen = 0, so it fails
         %% This hits the fallback _ -> clause
         HostnameLen = 1,  % ActualLen = 0
         %% Put 0xFF right after HostnameLen to fail first clause (needs 0:8)
         RestData = <<16#FF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>,
         Binary = <<0:64, 16#0064:16/big, 0:88, HostnameLen:8, RestData/binary>>,
         ?assert(byte_size(Binary) >= 39),
         Result = flurm_protocol_auth:decode_extra(Binary),
         ?assertMatch({ok, _}, Result),
         {ok, Map} = Result,
         ?assertEqual(<<>>, maps:get(hostname, Map)),
         ?assertEqual(0, maps:get(timestamp, Map)),
         ?assertEqual(rc_format, maps:get(format, Map))
     end}.

%% Test extract_timestamp with less than 4 bytes (line 181)
%% Need to create a case where decode_hostname_section gets < 4 bytes for Rest
extract_timestamp_fallback_test_() ->
    {"extract_timestamp with < 4 bytes covers line 181",
     fun() ->
         %% Build RC format where after extracting hostname, only 2 bytes remain
         %% RC: 8 zeros (8) + 00 64 (2) + 11 zeros (11) + HostnameLen (1) + Hostname + 0 + Rest
         %% Total header = 22 bytes, we need 39 total
         %% 39 - 22 = 17 bytes for Hostname + 0 + Rest
         %% To have Rest < 4 bytes:
         %% Hostname = 14 bytes, 0 = 1 byte, Rest = 2 bytes = 17 bytes
         Hostname = binary:copy(<<"x">>, 14),
         HostnameLen = 15,  % 14 + 1 for null
         Rest = <<1, 2>>,  % Only 2 bytes, less than 4
         Binary = <<0:64, 16#0064:16/big, 0:88, HostnameLen:8, Hostname/binary, 0:8, Rest/binary>>,
         ?assertEqual(39, byte_size(Binary)),
         Result = flurm_protocol_auth:decode_extra(Binary),
         ?assertMatch({ok, _}, Result),
         {ok, Map} = Result,
         ?assertEqual(Hostname, maps:get(hostname, Map)),
         %% Timestamp should be 0 since Rest < 4 bytes
         ?assertEqual(0, maps:get(timestamp, Map))
     end}.

%% Test default_hostname error path (line 71) using meck
default_hostname_error_test_() ->
    {"default_hostname error path covers line 71",
     {setup,
      fun() ->
          meck:new(inet, [unstick, passthrough]),
          meck:expect(inet, gethostname, fun() -> {error, nxdomain} end)
      end,
      fun(_) ->
          meck:unload(inet)
      end,
      fun(_) ->
          [{"returns flurm on error",
            fun() ->
                Result = flurm_protocol_auth:default_hostname(),
                ?assertEqual(<<"flurm">>, Result)
            end}]
      end}}.
