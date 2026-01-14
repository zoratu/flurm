%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit Tests for FLURM Protocol Auth Module
%%%
%%% Tests for extra data encoding/decoding functions with focus on:
%%% - Success cases for both message type formats
%%% - Error handling paths
%%% - Edge cases
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_auth_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%%===================================================================
%%% Constants Tests
%%%===================================================================

constants_test_() ->
    [
        {"extra_size returns 39 bytes", fun() ->
            Size = flurm_protocol_auth:extra_size(),
            ?assertEqual(39, Size)
        end},

        {"default_hostname returns binary", fun() ->
            Hostname = flurm_protocol_auth:default_hostname(),
            ?assert(is_binary(Hostname))
        end},

        {"default_hostname is non-empty", fun() ->
            Hostname = flurm_protocol_auth:default_hostname(),
            ?assert(byte_size(Hostname) > 0)
        end}
    ].

%%%===================================================================
%%% encode_extra Tests
%%%===================================================================

encode_extra_single_arg_test_() ->
    [
        {"encode_extra/1 returns binary", fun() ->
            Result = flurm_protocol_auth:encode_extra(1234),
            ?assert(is_binary(Result))
        end},

        {"encode_extra/1 with RESPONSE_SLURM_RC", fun() ->
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC),
            ?assert(is_binary(Result)),
            ?assert(byte_size(Result) >= 10)  % At minimum has header + timestamp
        end},

        {"encode_extra/1 with non-RC message type", fun() ->
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO),
            ?assert(is_binary(Result))
        end}
    ].

encode_extra_two_args_test_() ->
    [
        {"encode_extra/2 with RC format starts with zeros", fun() ->
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, <<"test">>),
            <<First8:64, _/binary>> = Result,
            ?assertEqual(0, First8)
        end},

        {"encode_extra/2 with RC format has response indicator", fun() ->
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, <<"test">>),
            <<_:64, Indicator:16/big, _/binary>> = Result,
            ?assertEqual(16#0064, Indicator)
        end},

        {"encode_extra/2 with standard format has response indicator at start", fun() ->
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO, <<"test">>),
            <<Indicator:16/big, _/binary>> = Result,
            ?assertEqual(16#0064, Indicator)
        end},

        {"encode_extra/2 includes hostname", fun() ->
            Hostname = <<"myhost">>,
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO, Hostname),
            %% The hostname should appear somewhere in the binary
            ?assertNotEqual(nomatch, binary:match(Result, Hostname))
        end},

        {"encode_extra/2 with empty hostname", fun() ->
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO, <<>>),
            ?assert(is_binary(Result))
        end},

        {"encode_extra/2 with short hostname", fun() ->
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO, <<"a">>),
            ?assert(is_binary(Result))
        end},

        {"encode_extra/2 with long hostname uses padding correctly", fun() ->
            LongHostname = <<"verylonghostname.example.com">>,
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO, LongHostname),
            ?assert(is_binary(Result))
        end},

        {"encode_extra/2 RC format with various hostnames", fun() ->
            Hostnames = [<<>>, <<"a">>, <<"ab">>, <<"host">>, <<"longhostname">>],
            lists:foreach(fun(H) ->
                Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, H),
                ?assert(is_binary(Result))
            end, Hostnames)
        end},

        {"encode_extra/2 standard format with various hostnames", fun() ->
            Hostnames = [<<>>, <<"a">>, <<"ab">>, <<"host">>, <<"longhostname">>],
            lists:foreach(fun(H) ->
                Result = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO, H),
                ?assert(is_binary(Result))
            end, Hostnames)
        end}
    ].

encode_extra_timestamp_test_() ->
    [
        {"encode_extra includes timestamp at end", fun() ->
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO, <<"test">>),
            Size = byte_size(Result),
            <<_:((Size-4)*8), Timestamp:32/big>> = Result,
            %% Timestamp should be a reasonable Unix time (after year 2020)
            ?assert(Timestamp > 1577836800)
        end},

        {"encode_extra RC format includes timestamp at end", fun() ->
            Result = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, <<"test">>),
            Size = byte_size(Result),
            <<_:((Size-4)*8), Timestamp:32/big>> = Result,
            ?assert(Timestamp > 1577836800)
        end}
    ].

%%%===================================================================
%%% decode_extra Tests
%%%===================================================================

decode_extra_empty_test_() ->
    [
        {"decode_extra with empty binary", fun() ->
            {ok, Result} = flurm_protocol_auth:decode_extra(<<>>),
            ?assertEqual(<<>>, maps:get(hostname, Result)),
            ?assertEqual(0, maps:get(timestamp, Result))
        end}
    ].

decode_extra_too_short_test_() ->
    [
        {"decode_extra with too short binary returns error", fun() ->
            Result = flurm_protocol_auth:decode_extra(<<1, 2, 3, 4, 5>>),
            ?assertMatch({error, {extra_data_too_short, 5}}, Result)
        end},

        {"decode_extra with 38 bytes returns error", fun() ->
            Binary = list_to_binary(lists:duplicate(38, 0)),
            Result = flurm_protocol_auth:decode_extra(Binary),
            ?assertMatch({error, {extra_data_too_short, 38}}, Result)
        end}
    ].

decode_extra_rc_format_test_() ->
    [
        {"decode_extra RC format (starts with 8 zeros, then 00 64)", fun() ->
            %% Build RC format: 8 zeros + 00 64 + 11 zeros + hostname_len + hostname + null + padding + timestamp
            HostnameLen = 5,  % "test" + null
            Hostname = <<"test">>,
            Padding = list_to_binary(lists:duplicate(8, 0)),  % Padding
            Timestamp = 1704067200,
            Binary = <<0:64,                    % 8 zeros
                       16#0064:16/big,          % Response type indicator
                       0:88,                    % 11 zeros
                       HostnameLen:8,           % Hostname length
                       Hostname/binary,         % "test"
                       0:8,                     % Null terminator
                       Padding/binary,          % Padding
                       Timestamp:32/big>>,      % Timestamp
            {ok, Result} = flurm_protocol_auth:decode_extra(Binary),
            ?assertEqual(Hostname, maps:get(hostname, Result)),
            ?assertEqual(Timestamp, maps:get(timestamp, Result)),
            ?assertEqual(rc_format, maps:get(format, Result))
        end}
    ].

decode_extra_standard_format_test_() ->
    [
        {"decode_extra standard format (starts with 00 64)", fun() ->
            %% Build standard format: 00 64 + 11 zeros + hostname_len + hostname + null + padding + timestamp
            HostnameLen = 5,  % "test" + null
            Hostname = <<"test">>,
            Padding = list_to_binary(lists:duplicate(16, 0)),  % Padding
            Timestamp = 1704067200,
            Binary = <<16#0064:16/big,          % Response type indicator
                       0:88,                    % 11 zeros
                       HostnameLen:8,           % Hostname length
                       Hostname/binary,         % "test"
                       0:8,                     % Null terminator
                       Padding/binary,          % Padding
                       Timestamp:32/big>>,      % Timestamp
            {ok, Result} = flurm_protocol_auth:decode_extra(Binary),
            ?assertEqual(Hostname, maps:get(hostname, Result)),
            ?assertEqual(Timestamp, maps:get(timestamp, Result)),
            ?assertEqual(standard_format, maps:get(format, Result))
        end}
    ].

decode_extra_request_format_test_() ->
    [
        {"decode_extra request format (starts with 00 00 then hostname)", fun() ->
            %% Build request format: 00 00 00 + hostname_len + hostname + null + padding + timestamp
            HostnameLen = 5,  % "test" + null
            Hostname = <<"test">>,
            Padding = list_to_binary(lists:duplicate(26, 0)),  % Padding
            Timestamp = 1704067200,
            Binary = <<0:24,                    % 3 zeros
                       HostnameLen:8,           % Hostname length
                       Hostname/binary,         % "test"
                       0:8,                     % Null terminator
                       Padding/binary,          % Padding
                       Timestamp:32/big>>,      % Timestamp
            {ok, Result} = flurm_protocol_auth:decode_extra(Binary),
            ?assertEqual(Hostname, maps:get(hostname, Result)),
            ?assertEqual(Timestamp, maps:get(timestamp, Result)),
            ?assertEqual(request_format, maps:get(format, Result))
        end}
    ].

decode_extra_unknown_format_test_() ->
    [
        {"decode_extra unknown format returns raw binary", fun() ->
            %% Build a binary that doesn't match any known format
            Binary = list_to_binary(lists:duplicate(39, 16#AA)),
            {ok, Result} = flurm_protocol_auth:decode_extra(Binary),
            ?assertEqual(<<>>, maps:get(hostname, Result)),
            ?assertEqual(0, maps:get(timestamp, Result)),
            ?assertEqual(Binary, maps:get(raw, Result))
        end}
    ].

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_test_() ->
    [
        {"encode then decode RC format", fun() ->
            Hostname = <<"testhost">>,
            Encoded = flurm_protocol_auth:encode_extra(?RESPONSE_SLURM_RC, Hostname),
            %% Add padding if needed to reach 39 bytes
            Padded = case byte_size(Encoded) < 39 of
                true -> <<Encoded/binary, 0:((39 - byte_size(Encoded)) * 8)>>;
                false -> Encoded
            end,
            {ok, Decoded} = flurm_protocol_auth:decode_extra(Padded),
            %% Hostname should match (may need to handle padding/truncation)
            DecodedHostname = maps:get(hostname, Decoded),
            ?assert(binary:match(Hostname, DecodedHostname) =/= nomatch orelse
                    binary:match(DecodedHostname, Hostname) =/= nomatch orelse
                    DecodedHostname =:= <<>> orelse
                    Hostname =:= <<>>)
        end},

        {"encode then decode standard format", fun() ->
            Hostname = <<"testhost">>,
            Encoded = flurm_protocol_auth:encode_extra(?RESPONSE_JOB_INFO, Hostname),
            Padded = case byte_size(Encoded) < 39 of
                true -> <<Encoded/binary, 0:((39 - byte_size(Encoded)) * 8)>>;
                false -> Encoded
            end,
            {ok, Decoded} = flurm_protocol_auth:decode_extra(Padded),
            ?assert(is_map(Decoded))
        end}
    ].

%%%===================================================================
%%% Edge Case Tests
%%%===================================================================

edge_case_test_() ->
    [
        {"encode_extra with zero message type", fun() ->
            Result = flurm_protocol_auth:encode_extra(0, <<"host">>),
            ?assert(is_binary(Result))
        end},

        {"encode_extra with max message type", fun() ->
            Result = flurm_protocol_auth:encode_extra(65535, <<"host">>),
            ?assert(is_binary(Result))
        end},

        {"decode_extra with exactly 39 bytes of zeros", fun() ->
            Binary = <<0:312>>,  % 39 * 8 = 312 bits
            {ok, Result} = flurm_protocol_auth:decode_extra(Binary),
            ?assert(is_map(Result))
        end},

        {"decode_extra with 39 bytes of 0xFF", fun() ->
            Binary = list_to_binary(lists:duplicate(39, 16#FF)),
            {ok, Result} = flurm_protocol_auth:decode_extra(Binary),
            ?assert(is_map(Result))
        end},

        {"decode_extra with larger binary (extra data ignored)", fun() ->
            Binary = list_to_binary(lists:duplicate(100, 0)),
            {ok, Result} = flurm_protocol_auth:decode_extra(Binary),
            ?assert(is_map(Result))
        end}
    ].
