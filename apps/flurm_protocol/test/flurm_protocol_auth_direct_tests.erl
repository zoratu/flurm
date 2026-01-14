%%%-------------------------------------------------------------------
%%% @doc Direct unit tests for flurm_protocol_auth module.
%%%
%%% These tests call actual module functions directly (no mocking)
%%% to achieve code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_auth_direct_tests).

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

flurm_protocol_auth_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {"extra_size", fun extra_size_test/0},
      {"default_hostname", fun default_hostname_test/0},
      {"encode_extra/1 for RESPONSE_SLURM_RC", fun encode_extra_rc_test/0},
      {"encode_extra/1 for other types", fun encode_extra_other_test/0},
      {"encode_extra/2 with custom hostname", fun encode_extra_custom_hostname_test/0},
      {"decode_extra empty", fun decode_extra_empty_test/0},
      {"decode_extra RC format", fun decode_extra_rc_format_test/0},
      {"decode_extra standard format", fun decode_extra_standard_format_test/0},
      {"decode_extra request format", fun decode_extra_request_format_test/0},
      {"decode_extra unknown format", fun decode_extra_unknown_format_test/0},
      {"decode_extra errors", fun decode_extra_errors_test/0},
      {"encode/decode roundtrip", fun encode_decode_roundtrip_test/0}
     ]}.

%%%===================================================================
%%% Constants Tests
%%%===================================================================

extra_size_test() ->
    Size = flurm_protocol_auth:extra_size(),
    ?assert(is_integer(Size)),
    ?assertEqual(39, Size),
    ok.

default_hostname_test() ->
    Hostname = flurm_protocol_auth:default_hostname(),
    ?assert(is_binary(Hostname)),
    %% Should be non-empty (either actual hostname or "flurm")
    ?assert(byte_size(Hostname) > 0),
    ok.

%%%===================================================================
%%% Encode Extra Tests
%%%===================================================================

encode_extra_rc_test() ->
    %% Test encode_extra/1 for RESPONSE_SLURM_RC message type
    ExtraData = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC),
    ?assert(is_binary(ExtraData)),
    ?assertEqual(39, byte_size(ExtraData)),

    %% Verify RC format: starts with 8 zeros
    <<First8:64, _/binary>> = ExtraData,
    ?assertEqual(0, First8),
    ok.

encode_extra_other_test() ->
    %% Test encode_extra/1 for other message types (non-RC)
    MsgTypes = [
        ?REQUEST_PING,
        ?RESPONSE_JOB_INFO,
        ?RESPONSE_SUBMIT_BATCH_JOB,
        ?RESPONSE_NODE_INFO
    ],
    lists:foreach(fun(MsgType) ->
        ExtraData = flurm_protocol_auth:encode_extra(MsgType),
        ?assert(is_binary(ExtraData)),
        ?assertEqual(39, byte_size(ExtraData))
    end, MsgTypes),
    ok.

encode_extra_custom_hostname_test() ->
    %% Test encode_extra/2 with custom hostname
    Hostname = <<"testhost">>,

    %% RC format
    ExtraRC = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
    ?assert(is_binary(ExtraRC)),
    ?assertEqual(39, byte_size(ExtraRC)),

    %% Other format
    ExtraOther = flurm_protocol_auth:encode_extra(?REQUEST_PING, Hostname),
    ?assert(is_binary(ExtraOther)),
    ?assertEqual(39, byte_size(ExtraOther)),

    %% Test with short hostname
    ShortHost = <<"a">>,
    ExtraShort = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, ShortHost),
    ?assertEqual(39, byte_size(ExtraShort)),

    %% Test with longer hostname
    LongHost = <<"verylonghostname.example.com">>,
    ExtraLong = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, LongHost),
    %% Size may exceed 39 for long hostnames
    ?assert(byte_size(ExtraLong) >= 39),
    ok.

%%%===================================================================
%%% Decode Extra Tests
%%%===================================================================

decode_extra_empty_test() ->
    %% Test decoding empty extra data
    Result = flurm_protocol_auth:decode_extra(<<>>),
    ?assertMatch({ok, #{hostname := <<>>, timestamp := 0}}, Result),
    ok.

decode_extra_rc_format_test() ->
    %% Create RC format extra data:
    %% 8 zeros + 00 64 + 11 zeros + hostname_len + hostname + null + padding + timestamp
    Hostname = <<"testhost">>,
    HostnameLen = byte_size(Hostname) + 1,
    Timestamp = 1609459200,

    %% Calculate padding
    PaddingLen = max(0, 13 - HostnameLen),
    Padding = <<0:(PaddingLen * 8)>>,

    RCData = <<0:64,               % 8 zeros
               16#0064:16/big,     % Response type indicator
               0:88,               % 11 zeros
               HostnameLen:8,      % hostname length
               Hostname/binary,    % hostname
               0:8,                % null terminator
               Padding/binary,     % padding
               Timestamp:32/big>>, % timestamp

    Result = flurm_protocol_auth:decode_extra(RCData),
    ?assertMatch({ok, #{format := rc_format}}, Result),
    {ok, Decoded} = Result,
    ?assertEqual(Timestamp, maps:get(timestamp, Decoded)),
    ok.

decode_extra_standard_format_test() ->
    %% Create standard format extra data:
    %% 00 64 + 11 zeros + hostname_len + hostname + null + padding + timestamp
    Hostname = <<"myhost">>,
    HostnameLen = byte_size(Hostname) + 1,
    Timestamp = 1609459200,

    %% Calculate padding for standard format
    PaddingLen = max(0, 21 - HostnameLen),
    Padding = <<0:(PaddingLen * 8)>>,

    StandardData = <<16#0064:16/big,     % Response type indicator
                     0:88,               % 11 zeros
                     HostnameLen:8,      % hostname length
                     Hostname/binary,    % hostname
                     0:8,                % null terminator
                     Padding/binary,     % padding
                     Timestamp:32/big>>, % timestamp

    Result = flurm_protocol_auth:decode_extra(StandardData),
    ?assertMatch({ok, #{format := standard_format}}, Result),
    {ok, Decoded} = Result,
    ?assertEqual(Timestamp, maps:get(timestamp, Decoded)),
    ok.

decode_extra_request_format_test() ->
    %% Create request format extra data:
    %% 00 00 00 + hostname_len + hostname + padding + timestamp
    Hostname = <<"client">>,
    HostnameLen = byte_size(Hostname) + 1,
    Timestamp = 1609459200,

    %% Create enough padding to reach 39 bytes total
    HeaderLen = 3 + 1 + HostnameLen + 4,  % 00 00 00 + len byte + hostname + null + timestamp
    PaddingLen = max(0, 39 - HeaderLen),
    Padding = <<0:(PaddingLen * 8)>>,

    RequestData = <<0:24,                % 3 zeros
                    HostnameLen:8,       % hostname length
                    Hostname/binary,     % hostname
                    0:8,                 % null terminator
                    Padding/binary,      % padding
                    Timestamp:32/big>>,  % timestamp

    Result = flurm_protocol_auth:decode_extra(RequestData),
    ?assertMatch({ok, #{format := request_format}}, Result),
    ok.

decode_extra_unknown_format_test() ->
    %% Create data that doesn't match any known format
    UnknownData = <<16#FFFF:16/big, 0:296>>,  % 39 bytes of mostly zeros with different header
    ?assertEqual(39, byte_size(UnknownData)),

    Result = flurm_protocol_auth:decode_extra(UnknownData),
    ?assertMatch({ok, #{raw := _}}, Result),
    ok.

decode_extra_errors_test() ->
    %% Test decoding data that is too short
    ShortData = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>,  % Only 10 bytes
    Result = flurm_protocol_auth:decode_extra(ShortData),
    ?assertMatch({error, {extra_data_too_short, _}}, Result),

    %% Test data exactly at boundary
    BoundaryData = <<0:304>>,  % 38 bytes, just under threshold
    ?assertEqual(38, byte_size(BoundaryData)),
    BoundaryResult = flurm_protocol_auth:decode_extra(BoundaryData),
    ?assertMatch({error, {extra_data_too_short, _}}, BoundaryResult),
    ok.

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

encode_decode_roundtrip_test() ->
    %% Test that we can encode and then decode extra data
    Hostnames = [
        <<"host1">>,
        <<"myhost.example.com">>,
        <<"a">>,
        <<"flurm">>
    ],

    lists:foreach(fun(Hostname) ->
        %% Test RC format
        RCExtra = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
        RCResult = flurm_protocol_auth:decode_extra(RCExtra),
        ?assertMatch({ok, _}, RCResult),

        %% Test standard format
        StdExtra = flurm_protocol_auth:encode_extra(?REQUEST_PING, Hostname),
        StdResult = flurm_protocol_auth:decode_extra(StdExtra),
        ?assertMatch({ok, _}, StdResult)
    end, Hostnames),

    %% Test using default hostname
    DefaultExtra = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC),
    DefaultResult = flurm_protocol_auth:decode_extra(DefaultExtra),
    ?assertMatch({ok, _}, DefaultResult),
    ok.
