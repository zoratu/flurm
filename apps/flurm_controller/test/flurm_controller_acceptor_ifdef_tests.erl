%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_controller_acceptor -ifdef(TEST) exports
%%%
%%% Tests the helper functions exported under -ifdef(TEST) that handle
%%% pure data transformations without requiring mocks or network state.
%%%
%%% These tests focus on:
%%% - Binary to hex conversion
%%% - Hex prefix extraction
%%% - Initial state creation
%%% - Buffer status checking
%%% - Message extraction
%%% - Duration calculation
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_acceptor_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% binary_to_hex Tests
%%====================================================================

binary_to_hex_test_() ->
    [
        {"empty binary", fun test_binary_to_hex_empty/0},
        {"single byte zero", fun test_binary_to_hex_zero/0},
        {"single byte max", fun test_binary_to_hex_max/0},
        {"multiple bytes", fun test_binary_to_hex_multiple/0},
        {"DEADBEEF pattern", fun test_binary_to_hex_deadbeef/0},
        {"SLURM header example", fun test_binary_to_hex_slurm_header/0}
    ].

test_binary_to_hex_empty() ->
    ?assertEqual(<<>>, flurm_controller_acceptor:binary_to_hex(<<>>)).

test_binary_to_hex_zero() ->
    ?assertEqual(<<"00">>, flurm_controller_acceptor:binary_to_hex(<<0>>)).

test_binary_to_hex_max() ->
    ?assertEqual(<<"FF">>, flurm_controller_acceptor:binary_to_hex(<<255>>)).

test_binary_to_hex_multiple() ->
    ?assertEqual(<<"0102030405">>, flurm_controller_acceptor:binary_to_hex(<<1,2,3,4,5>>)).

test_binary_to_hex_deadbeef() ->
    ?assertEqual(<<"DEADBEEF">>, flurm_controller_acceptor:binary_to_hex(<<222,173,190,239>>)).

test_binary_to_hex_slurm_header() ->
    %% Typical SLURM header bytes
    Header = <<16#00, 16#26, 16#00, 16#00, 16#03, 16#F0, 16#00, 16#00, 16#00, 16#10>>,
    Expected = <<"0026000003F000000010">>,
    ?assertEqual(Expected, flurm_controller_acceptor:binary_to_hex(Header)).

%%====================================================================
%% get_hex_prefix Tests
%%====================================================================

get_hex_prefix_test_() ->
    [
        {"short data - less than 24 bytes", fun test_get_hex_prefix_short/0},
        {"exactly 24 bytes", fun test_get_hex_prefix_exact/0},
        {"more than 24 bytes - truncated", fun test_get_hex_prefix_long/0},
        {"empty data", fun test_get_hex_prefix_empty/0}
    ].

test_get_hex_prefix_short() ->
    Data = <<1, 2, 3, 4, 5>>,
    Result = flurm_controller_acceptor:get_hex_prefix(Data),
    ?assertEqual(<<"0102030405">>, Result).

test_get_hex_prefix_exact() ->
    Data = binary:copy(<<16#AB>>, 24),
    Result = flurm_controller_acceptor:get_hex_prefix(Data),
    Expected = binary:copy(<<"AB">>, 24),
    ?assertEqual(Expected, Result).

test_get_hex_prefix_long() ->
    %% 50 bytes of data, should only get first 24
    Data = binary:copy(<<16#CD>>, 50),
    Result = flurm_controller_acceptor:get_hex_prefix(Data),
    Expected = binary:copy(<<"CD">>, 24),
    ?assertEqual(Expected, Result).

test_get_hex_prefix_empty() ->
    Result = flurm_controller_acceptor:get_hex_prefix(<<>>),
    ?assertEqual(<<>>, Result).

%%====================================================================
%% create_initial_state Tests
%%====================================================================

create_initial_state_test_() ->
    [
        {"creates state with all fields", fun test_create_state_fields/0},
        {"buffer is empty initially", fun test_create_state_empty_buffer/0},
        {"request count starts at zero", fun test_create_state_zero_count/0},
        {"start_time is set", fun test_create_state_start_time/0},
        {"preserves socket and transport", fun test_create_state_socket_transport/0}
    ].

test_create_state_fields() ->
    Socket = make_ref(),
    State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, #{test => true}),

    ?assert(maps:is_key(socket, State)),
    ?assert(maps:is_key(transport, State)),
    ?assert(maps:is_key(opts, State)),
    ?assert(maps:is_key(buffer, State)),
    ?assert(maps:is_key(request_count, State)),
    ?assert(maps:is_key(start_time, State)).

test_create_state_empty_buffer() ->
    Socket = make_ref(),
    State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, #{}),
    ?assertEqual(<<>>, maps:get(buffer, State)).

test_create_state_zero_count() ->
    Socket = make_ref(),
    State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, #{}),
    ?assertEqual(0, maps:get(request_count, State)).

test_create_state_start_time() ->
    Before = erlang:system_time(millisecond),
    Socket = make_ref(),
    State = flurm_controller_acceptor:create_initial_state(Socket, ranch_tcp, #{}),
    After = erlang:system_time(millisecond),

    StartTime = maps:get(start_time, State),
    ?assert(StartTime >= Before),
    ?assert(StartTime =< After).

test_create_state_socket_transport() ->
    Socket = make_ref(),
    State = flurm_controller_acceptor:create_initial_state(Socket, ranch_ssl, #{key => value}),

    ?assertEqual(Socket, maps:get(socket, State)),
    ?assertEqual(ranch_ssl, maps:get(transport, State)),
    ?assertEqual(#{key => value}, maps:get(opts, State)).

%%====================================================================
%% check_buffer_status Tests
%%====================================================================

check_buffer_status_test_() ->
    [
        {"empty buffer needs more data", fun test_check_buffer_empty/0},
        {"partial length prefix needs more data", fun test_check_buffer_partial_length/0},
        {"invalid length - too small", fun test_check_buffer_invalid_length/0},
        {"incomplete message needs more data", fun test_check_buffer_incomplete/0},
        {"complete message with minimum size", fun test_check_buffer_complete_min/0},
        {"complete message with body", fun test_check_buffer_complete_body/0}
    ].

test_check_buffer_empty() ->
    Result = flurm_controller_acceptor:check_buffer_status(<<>>),
    ?assertEqual(need_more_data, Result).

test_check_buffer_partial_length() ->
    %% Only 3 bytes, need 4 for length prefix
    Result = flurm_controller_acceptor:check_buffer_status(<<0, 0, 0>>),
    ?assertEqual(need_more_data, Result).

test_check_buffer_invalid_length() ->
    %% Length = 5, but SLURM_HEADER_SIZE is 10
    Buffer = <<5:32/big, 1, 2, 3, 4, 5>>,
    Result = flurm_controller_acceptor:check_buffer_status(Buffer),
    ?assertEqual(invalid_length, Result).

test_check_buffer_incomplete() ->
    %% Length says 100 bytes, but only have 10
    Buffer = <<100:32/big, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>,
    Result = flurm_controller_acceptor:check_buffer_status(Buffer),
    ?assertEqual(need_more_data, Result).

test_check_buffer_complete_min() ->
    %% Minimum valid message (just header, no body)
    Length = ?SLURM_HEADER_SIZE,
    Buffer = <<Length:32/big, 0:(Length*8)>>,
    Result = flurm_controller_acceptor:check_buffer_status(Buffer),
    ?assertEqual({complete, Length}, Result).

test_check_buffer_complete_body() ->
    %% Header + 20 bytes body
    Length = ?SLURM_HEADER_SIZE + 20,
    Buffer = <<Length:32/big, 0:(Length*8)>>,
    Result = flurm_controller_acceptor:check_buffer_status(Buffer),
    ?assertEqual({complete, Length}, Result).

%%====================================================================
%% extract_message Tests
%%====================================================================

extract_message_test_() ->
    [
        {"extract complete message", fun test_extract_complete/0},
        {"extract with remainder", fun test_extract_with_remainder/0},
        {"incomplete buffer returns incomplete", fun test_extract_incomplete/0},
        {"empty buffer returns incomplete", fun test_extract_empty/0},
        {"extract preserves length prefix", fun test_extract_preserves_prefix/0}
    ].

test_extract_complete() ->
    Length = 15,
    MessageData = binary:copy(<<$A>>, Length),
    Buffer = <<Length:32/big, MessageData/binary>>,

    {ok, FullMsg, Remaining} = flurm_controller_acceptor:extract_message(Buffer),

    ?assertEqual(<<Length:32/big, MessageData/binary>>, FullMsg),
    ?assertEqual(<<>>, Remaining).

test_extract_with_remainder() ->
    Length = 10,
    MessageData = binary:copy(<<$B>>, Length),
    ExtraData = <<"extra data here">>,
    Buffer = <<Length:32/big, MessageData/binary, ExtraData/binary>>,

    {ok, FullMsg, Remaining} = flurm_controller_acceptor:extract_message(Buffer),

    ?assertEqual(<<Length:32/big, MessageData/binary>>, FullMsg),
    ?assertEqual(ExtraData, Remaining).

test_extract_incomplete() ->
    %% Length says 100, but only 10 bytes of data
    Buffer = <<100:32/big, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>,

    Result = flurm_controller_acceptor:extract_message(Buffer),
    ?assertMatch({incomplete, Buffer}, Result).

test_extract_empty() ->
    Result = flurm_controller_acceptor:extract_message(<<>>),
    ?assertMatch({incomplete, <<>>}, Result).

test_extract_preserves_prefix() ->
    %% Verify the extracted message includes the length prefix
    Length = 20,
    MessageData = binary:copy(<<$X>>, Length),
    Buffer = <<Length:32/big, MessageData/binary>>,

    {ok, FullMsg, _} = flurm_controller_acceptor:extract_message(Buffer),

    %% Full message should start with the length prefix
    <<ExtractedLen:32/big, ExtractedData/binary>> = FullMsg,
    ?assertEqual(Length, ExtractedLen),
    ?assertEqual(MessageData, ExtractedData).

%%====================================================================
%% calculate_duration Tests
%%====================================================================

calculate_duration_test_() ->
    [
        {"zero duration", fun test_duration_zero/0},
        {"positive duration", fun test_duration_positive/0},
        {"large duration", fun test_duration_large/0}
    ].

test_duration_zero() ->
    Now = erlang:system_time(millisecond),
    Result = flurm_controller_acceptor:calculate_duration(Now, Now),
    ?assertEqual(0, Result).

test_duration_positive() ->
    StartTime = 1000,
    EndTime = 2500,
    Result = flurm_controller_acceptor:calculate_duration(EndTime, StartTime),
    ?assertEqual(1500, Result).

test_duration_large() ->
    %% Simulate 1 hour duration
    StartTime = 0,
    EndTime = 3600000,  % 1 hour in milliseconds
    Result = flurm_controller_acceptor:calculate_duration(EndTime, StartTime),
    ?assertEqual(3600000, Result).

%%====================================================================
%% Integration Tests - Combining Functions
%%====================================================================

integration_test_() ->
    [
        {"buffer check then extract flow", fun test_check_then_extract/0},
        {"multiple message extraction", fun test_multiple_extraction/0},
        {"hex prefix for debug logging", fun test_hex_for_logging/0}
    ].

test_check_then_extract() ->
    %% Simulate the typical flow: check buffer, then extract if complete
    Length = ?SLURM_HEADER_SIZE + 10,
    MessageData = binary:copy(<<$M>>, Length),
    Buffer = <<Length:32/big, MessageData/binary>>,

    %% First check status
    case flurm_controller_acceptor:check_buffer_status(Buffer) of
        {complete, ExtractLen} ->
            ?assertEqual(Length, ExtractLen),
            %% Then extract
            {ok, FullMsg, <<>>} = flurm_controller_acceptor:extract_message(Buffer),
            ?assertEqual(4 + Length, byte_size(FullMsg));
        _ ->
            ?assert(false)
    end.

test_multiple_extraction() ->
    %% Buffer with two complete messages
    Len1 = 10,
    Len2 = 15,
    Msg1Data = binary:copy(<<$1>>, Len1),
    Msg2Data = binary:copy(<<$2>>, Len2),
    Buffer = <<Len1:32/big, Msg1Data/binary, Len2:32/big, Msg2Data/binary>>,

    %% Extract first message
    {ok, FullMsg1, Rest1} = flurm_controller_acceptor:extract_message(Buffer),
    ?assertEqual(<<Len1:32/big, Msg1Data/binary>>, FullMsg1),

    %% Extract second message
    {ok, FullMsg2, Rest2} = flurm_controller_acceptor:extract_message(Rest1),
    ?assertEqual(<<Len2:32/big, Msg2Data/binary>>, FullMsg2),
    ?assertEqual(<<>>, Rest2).

test_hex_for_logging() ->
    %% Simulate logging hex prefix for incoming data
    IncomingData = <<16#00, 16#26, 16#00, 16#00, 16#03, 16#F0,
                     16#00, 16#00, 16#00, 16#10, 16#AA, 16#BB>>,

    HexPrefix = flurm_controller_acceptor:get_hex_prefix(IncomingData),
    ?assertEqual(<<"0026000003F000000010AABB">>, HexPrefix).

%%====================================================================
%% Edge Cases and Boundary Tests
%%====================================================================

edge_cases_test_() ->
    [
        {"exactly 4 bytes (just length)", fun test_just_length_prefix/0},
        {"maximum uint32 length", fun test_max_length_incomplete/0},
        {"binary with null bytes", fun test_null_bytes/0}
    ].

test_just_length_prefix() ->
    %% Buffer has exactly 4 bytes (length prefix only)
    Buffer = <<100:32/big>>,
    Result = flurm_controller_acceptor:check_buffer_status(Buffer),
    ?assertEqual(need_more_data, Result).

test_max_length_incomplete() ->
    %% Very large length value - buffer obviously incomplete
    Buffer = <<16#FFFFFFFF:32/big, 0, 1, 2>>,
    Result = flurm_controller_acceptor:check_buffer_status(Buffer),
    ?assertEqual(need_more_data, Result).

test_null_bytes() ->
    %% Binary containing null bytes
    Data = <<0, 0, 0, 0, 0>>,
    Hex = flurm_controller_acceptor:binary_to_hex(Data),
    ?assertEqual(<<"0000000000">>, Hex).

%%====================================================================
%% SLURM Protocol Specific Tests
%%====================================================================

slurm_protocol_test_() ->
    [
        {"valid SLURM header size", fun test_valid_slurm_header_size/0},
        {"SLURM ping message buffer", fun test_slurm_ping_buffer/0},
        {"SLURM version in header hex", fun test_slurm_version_hex/0}
    ].

test_valid_slurm_header_size() ->
    %% SLURM_HEADER_SIZE should be 10
    Length = ?SLURM_HEADER_SIZE,
    Buffer = <<Length:32/big, 0:(Length*8)>>,
    Result = flurm_controller_acceptor:check_buffer_status(Buffer),
    ?assertEqual({complete, ?SLURM_HEADER_SIZE}, Result).

test_slurm_ping_buffer() ->
    %% Construct a minimal PING message buffer
    %% Header: version=0x2600, flags=0, msg_type=REQUEST_PING(1008), body_len=0
    Version = ?SLURM_PROTOCOL_VERSION,
    Flags = 0,
    MsgType = ?REQUEST_PING,
    BodyLen = 0,
    Header = <<Version:16/big, Flags:16/big, MsgType:16/big, BodyLen:32/big>>,
    Length = byte_size(Header),
    Buffer = <<Length:32/big, Header/binary>>,

    Result = flurm_controller_acceptor:check_buffer_status(Buffer),
    ?assertEqual({complete, Length}, Result).

test_slurm_version_hex() ->
    %% SLURM protocol version 0x2600 = 22.05.x
    VersionBin = <<?SLURM_PROTOCOL_VERSION:16/big>>,
    Hex = flurm_controller_acceptor:binary_to_hex(VersionBin),
    ?assertEqual(<<"2600">>, Hex).
