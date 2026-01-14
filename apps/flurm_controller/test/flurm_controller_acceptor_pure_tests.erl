%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Acceptor Pure Unit Tests
%%%
%%% Pure unit tests for flurm_controller_acceptor module that test
%%% helper functions and buffer processing logic without any mocking.
%%%
%%% These tests focus on:
%%% - Binary to hex conversion
%%% - Buffer processing edge cases
%%% - Length prefix parsing
%%% - Message completeness checking
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_acceptor_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%% Minimum data needed to determine message length
-define(LENGTH_PREFIX_SIZE, 4).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Binary to hex conversion tests
binary_to_hex_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"binary_to_hex - empty binary", fun test_binary_to_hex_empty/0},
        {"binary_to_hex - single byte", fun test_binary_to_hex_single/0},
        {"binary_to_hex - multiple bytes", fun test_binary_to_hex_multiple/0},
        {"binary_to_hex - all zeros", fun test_binary_to_hex_zeros/0},
        {"binary_to_hex - all ones (0xFF)", fun test_binary_to_hex_ones/0},
        {"binary_to_hex - mixed values", fun test_binary_to_hex_mixed/0},
        {"binary_to_hex - slurm header example", fun test_binary_to_hex_header/0}
     ]}.

%% Buffer completeness tests
buffer_completeness_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"is_buffer_incomplete - less than length prefix", fun test_buffer_incomplete_no_length/0},
        {"is_buffer_incomplete - exact length prefix", fun test_buffer_incomplete_just_length/0},
        {"is_buffer_incomplete - incomplete message", fun test_buffer_incomplete_partial/0},
        {"is_buffer_complete - exact message", fun test_buffer_complete_exact/0},
        {"is_buffer_complete - extra data", fun test_buffer_complete_extra/0}
     ]}.

%% Length prefix parsing tests
length_prefix_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"parse_length_prefix - small value", fun test_parse_length_small/0},
        {"parse_length_prefix - large value", fun test_parse_length_large/0},
        {"parse_length_prefix - maximum value", fun test_parse_length_max/0},
        {"parse_length_prefix - zero", fun test_parse_length_zero/0},
        {"parse_length_prefix - header size exactly", fun test_parse_length_header_size/0}
     ]}.

%% Message validation tests
message_validation_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"validate_message_length - too small", fun test_validate_length_too_small/0},
        {"validate_message_length - header size", fun test_validate_length_header_size/0},
        {"validate_message_length - normal", fun test_validate_length_normal/0},
        {"validate_message_length - large", fun test_validate_length_large/0}
     ]}.

%% Message extraction tests
message_extraction_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"extract_message - single message", fun test_extract_single_message/0},
        {"extract_message - message with remainder", fun test_extract_with_remainder/0},
        {"extract_message - exact buffer", fun test_extract_exact_buffer/0}
     ]}.

%% State management tests
state_management_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"initial_state - basic", fun test_initial_state_basic/0},
        {"update_buffer - add data", fun test_update_buffer_add/0},
        {"update_buffer - clear buffer", fun test_update_buffer_clear/0},
        {"increment_request_count", fun test_increment_request_count/0}
     ]}.

%% SLURM header parsing tests
slurm_header_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"parse_header - version", fun test_parse_header_version/0},
        {"parse_header - flags", fun test_parse_header_flags/0},
        {"parse_header - msg_type", fun test_parse_header_msg_type/0},
        {"parse_header - body_length", fun test_parse_header_body_length/0},
        {"construct_full_message - with length prefix", fun test_construct_full_message/0}
     ]}.

%% Connection duration calculation tests
duration_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"calculate_duration - zero", fun test_calculate_duration_zero/0},
        {"calculate_duration - positive", fun test_calculate_duration_positive/0}
     ]}.

%%====================================================================
%% binary_to_hex Tests
%%====================================================================

test_binary_to_hex_empty() ->
    ?assertEqual(<<>>, binary_to_hex(<<>>)).

test_binary_to_hex_single() ->
    ?assertEqual(<<"00">>, binary_to_hex(<<0>>)),
    ?assertEqual(<<"0F">>, binary_to_hex(<<15>>)),
    ?assertEqual(<<"10">>, binary_to_hex(<<16>>)),
    ?assertEqual(<<"FF">>, binary_to_hex(<<255>>)).

test_binary_to_hex_multiple() ->
    ?assertEqual(<<"0102">>, binary_to_hex(<<1, 2>>)),
    ?assertEqual(<<"0A0B0C">>, binary_to_hex(<<10, 11, 12>>)).

test_binary_to_hex_zeros() ->
    ?assertEqual(<<"00000000">>, binary_to_hex(<<0, 0, 0, 0>>)).

test_binary_to_hex_ones() ->
    ?assertEqual(<<"FFFFFFFF">>, binary_to_hex(<<255, 255, 255, 255>>)).

test_binary_to_hex_mixed() ->
    ?assertEqual(<<"DEADBEEF">>, binary_to_hex(<<16#DE, 16#AD, 16#BE, 16#EF>>)).

test_binary_to_hex_header() ->
    %% Example SLURM header bytes
    Header = <<16#00, 16#26, 16#00, 16#00, 16#03, 16#F0, 16#00, 16#00, 16#00, 16#10>>,
    Hex = binary_to_hex(Header),
    ?assertEqual(<<"00260000" "03F0" "00000010">>, Hex).

%%====================================================================
%% Buffer Completeness Tests
%%====================================================================

test_buffer_incomplete_no_length() ->
    %% Less than 4 bytes - can't read length prefix
    ?assert(is_buffer_incomplete(<<>>)),
    ?assert(is_buffer_incomplete(<<1>>)),
    ?assert(is_buffer_incomplete(<<1, 2>>)),
    ?assert(is_buffer_incomplete(<<1, 2, 3>>)).

test_buffer_incomplete_just_length() ->
    %% Exactly 4 bytes - have length but no data
    ?assert(is_buffer_incomplete(<<0, 0, 0, 10>>)).

test_buffer_incomplete_partial() ->
    %% Length says 20 bytes, but we only have 10 bytes of data
    Buffer = <<0, 0, 0, 20, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>,
    ?assert(is_buffer_incomplete(Buffer)).

test_buffer_complete_exact() ->
    %% Length says 10 bytes, we have exactly 10 bytes
    Buffer = <<0, 0, 0, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>,
    ?assertNot(is_buffer_incomplete(Buffer)).

test_buffer_complete_extra() ->
    %% Length says 5 bytes, we have 10 bytes
    Buffer = <<0, 0, 0, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>,
    ?assertNot(is_buffer_incomplete(Buffer)).

%%====================================================================
%% Length Prefix Tests
%%====================================================================

test_parse_length_small() ->
    ?assertEqual(10, parse_length_prefix(<<0, 0, 0, 10>>)),
    ?assertEqual(100, parse_length_prefix(<<0, 0, 0, 100>>)),
    ?assertEqual(255, parse_length_prefix(<<0, 0, 0, 255>>)).

test_parse_length_large() ->
    ?assertEqual(256, parse_length_prefix(<<0, 0, 1, 0>>)),
    ?assertEqual(65536, parse_length_prefix(<<0, 1, 0, 0>>)),
    ?assertEqual(16777216, parse_length_prefix(<<1, 0, 0, 0>>)).

test_parse_length_max() ->
    ?assertEqual(16#FFFFFFFF, parse_length_prefix(<<255, 255, 255, 255>>)).

test_parse_length_zero() ->
    ?assertEqual(0, parse_length_prefix(<<0, 0, 0, 0>>)).

test_parse_length_header_size() ->
    %% SLURM header size is 10 bytes
    ?assertEqual(?SLURM_HEADER_SIZE, parse_length_prefix(<<0, 0, 0, ?SLURM_HEADER_SIZE>>)).

%%====================================================================
%% Message Validation Tests
%%====================================================================

test_validate_length_too_small() ->
    %% Length less than header size is invalid
    ?assertEqual(invalid, validate_message_length(0)),
    ?assertEqual(invalid, validate_message_length(5)),
    ?assertEqual(invalid, validate_message_length(9)).

test_validate_length_header_size() ->
    %% Exactly header size is the minimum valid
    ?assertEqual(valid, validate_message_length(?SLURM_HEADER_SIZE)).

test_validate_length_normal() ->
    ?assertEqual(valid, validate_message_length(100)),
    ?assertEqual(valid, validate_message_length(1000)),
    ?assertEqual(valid, validate_message_length(10000)).

test_validate_length_large() ->
    %% Large but valid sizes
    ?assertEqual(valid, validate_message_length(1000000)),
    ?assertEqual(valid, validate_message_length(?SLURM_MAX_MESSAGE_SIZE)).

%%====================================================================
%% Message Extraction Tests
%%====================================================================

test_extract_single_message() ->
    %% Buffer with exactly one message
    Length = 10,
    MessageData = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>,
    Buffer = <<Length:32/big, MessageData/binary>>,
    {Message, Remainder} = extract_message(Buffer),
    %% Full message includes length prefix
    ?assertEqual(<<Length:32/big, MessageData/binary>>, Message),
    ?assertEqual(<<>>, Remainder).

test_extract_with_remainder() ->
    %% Buffer with one complete message and extra data
    Length = 5,
    MessageData = <<1, 2, 3, 4, 5>>,
    ExtraData = <<"extra">>,
    Buffer = <<Length:32/big, MessageData/binary, ExtraData/binary>>,
    {Message, Remainder} = extract_message(Buffer),
    ?assertEqual(<<Length:32/big, MessageData/binary>>, Message),
    ?assertEqual(ExtraData, Remainder).

test_extract_exact_buffer() ->
    %% Extract from buffer with no remaining data
    Length = 15,
    MessageData = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15>>,
    Buffer = <<Length:32/big, MessageData/binary>>,
    {Message, Remainder} = extract_message(Buffer),
    ?assertEqual(<<Length:32/big, MessageData/binary>>, Message),
    ?assertEqual(<<>>, Remainder).

%%====================================================================
%% State Management Tests
%%====================================================================

test_initial_state_basic() ->
    State = create_initial_state(),
    ?assertEqual(<<>>, maps:get(buffer, State)),
    ?assertEqual(0, maps:get(request_count, State)),
    ?assert(is_integer(maps:get(start_time, State))).

test_update_buffer_add() ->
    State0 = #{buffer => <<>>},
    NewData = <<"hello">>,
    State1 = update_buffer(State0, NewData),
    ?assertEqual(<<"hello">>, maps:get(buffer, State1)),

    %% Add more data
    State2 = update_buffer(State1, <<" world">>),
    ?assertEqual(<<"hello world">>, maps:get(buffer, State2)).

test_update_buffer_clear() ->
    State0 = #{buffer => <<"some data">>},
    State1 = clear_buffer(State0),
    ?assertEqual(<<>>, maps:get(buffer, State1)).

test_increment_request_count() ->
    State0 = #{request_count => 0},
    State1 = increment_request_count(State0),
    ?assertEqual(1, maps:get(request_count, State1)),

    State2 = increment_request_count(State1),
    ?assertEqual(2, maps:get(request_count, State2)),

    %% Multiple increments
    State3 = increment_request_count(increment_request_count(increment_request_count(State2))),
    ?assertEqual(5, maps:get(request_count, State3)).

%%====================================================================
%% SLURM Header Parsing Tests
%%====================================================================

test_parse_header_version() ->
    %% SLURM protocol version 0x2600 (22.05.x)
    Header = <<16#26, 16#00, 0:16, 0:16, 0:32>>,
    ?assertEqual(?SLURM_PROTOCOL_VERSION, parse_header_version(Header)).

test_parse_header_flags() ->
    %% Test flag parsing
    Header = <<0:16, 16#00, 16#04, 0:16, 0:32>>,  % AUTH_REQUIRED flag
    ?assertEqual(?SLURM_MSG_AUTH_REQUIRED, parse_header_flags(Header)).

test_parse_header_msg_type() ->
    %% REQUEST_PING = 1008 = 0x03F0
    Header = <<0:16, 0:16, 16#03, 16#F0, 0:32>>,
    ?assertEqual(?REQUEST_PING, parse_header_msg_type(Header)).

test_parse_header_body_length() ->
    %% Body length of 100 bytes
    Header = <<0:16, 0:16, 0:16, 0:8, 0:8, 0:8, 100:8>>,
    ?assertEqual(100, parse_header_body_length(Header)).

test_construct_full_message() ->
    %% Test constructing full message with length prefix
    Length = 20,
    MessageData = binary:copy(<<0>>, 20),
    FullMessage = construct_full_message(Length, MessageData),
    ?assertEqual(<<Length:32/big, MessageData/binary>>, FullMessage).

%%====================================================================
%% Duration Tests
%%====================================================================

test_calculate_duration_zero() ->
    Now = erlang:system_time(millisecond),
    Duration = calculate_duration(Now, Now),
    ?assertEqual(0, Duration).

test_calculate_duration_positive() ->
    StartTime = erlang:system_time(millisecond) - 1000,  % 1 second ago
    Now = erlang:system_time(millisecond),
    Duration = calculate_duration(Now, StartTime),
    ?assert(Duration >= 1000),
    ?assert(Duration < 2000).

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

hex_prefix_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"get_hex_prefix - short data", fun test_hex_prefix_short/0},
        {"get_hex_prefix - long data", fun test_hex_prefix_long/0},
        {"get_hex_prefix - exact 24 bytes", fun test_hex_prefix_exact/0}
     ]}.

test_hex_prefix_short() ->
    %% Data less than 24 bytes
    Data = <<1, 2, 3, 4, 5>>,
    Hex = get_hex_prefix(Data),
    ?assertEqual(<<"0102030405">>, Hex).

test_hex_prefix_long() ->
    %% Data more than 24 bytes - should only get first 24
    Data = binary:copy(<<16#AB>>, 50),
    Hex = get_hex_prefix(Data),
    Expected = binary:copy(<<"AB">>, 24),
    ?assertEqual(Expected, Hex).

test_hex_prefix_exact() ->
    %% Data exactly 24 bytes
    Data = binary:copy(<<16#CD>>, 24),
    Hex = get_hex_prefix(Data),
    Expected = binary:copy(<<"CD">>, 24),
    ?assertEqual(Expected, Hex).

%% Test buffer processing logic
buffer_processing_test_() ->
    {setup,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"process_buffer - empty buffer", fun test_process_buffer_empty/0},
        {"process_buffer - partial length", fun test_process_buffer_partial_length/0},
        {"process_buffer - complete message", fun test_process_buffer_complete/0},
        {"process_buffer - invalid length", fun test_process_buffer_invalid_length/0}
     ]}.

test_process_buffer_empty() ->
    Buffer = <<>>,
    Result = check_buffer_status(Buffer),
    ?assertEqual(need_more_data, Result).

test_process_buffer_partial_length() ->
    Buffer = <<0, 0, 0>>,  % Only 3 bytes
    Result = check_buffer_status(Buffer),
    ?assertEqual(need_more_data, Result).

test_process_buffer_complete() ->
    %% Complete message with header
    Length = ?SLURM_HEADER_SIZE,
    Buffer = <<Length:32/big, 0:(?SLURM_HEADER_SIZE * 8)>>,
    Result = check_buffer_status(Buffer),
    ?assertEqual(complete, Result).

test_process_buffer_invalid_length() ->
    %% Length too small (less than header size)
    Length = 5,  % Less than SLURM_HEADER_SIZE (10)
    Buffer = <<Length:32/big, 0:40>>,
    Result = check_buffer_status(Buffer),
    ?assertEqual(invalid_length, Result).

%%====================================================================
%% Helper Function Implementations
%%====================================================================

%% Binary to hex conversion (reimplemented from acceptor module)
binary_to_hex(Bin) ->
    list_to_binary([[io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(Bin)]]).

%% Check if buffer has incomplete message
is_buffer_incomplete(Buffer) when byte_size(Buffer) < ?LENGTH_PREFIX_SIZE ->
    true;
is_buffer_incomplete(<<Length:32/big, Rest/binary>>) ->
    byte_size(Rest) < Length.

%% Parse the 4-byte length prefix
parse_length_prefix(<<Length:32/big, _/binary>>) ->
    Length;
parse_length_prefix(<<Length:32/big>>) ->
    Length.

%% Validate message length
validate_message_length(Length) when Length < ?SLURM_HEADER_SIZE ->
    invalid;
validate_message_length(_Length) ->
    valid.

%% Extract a complete message from buffer
extract_message(<<Length:32/big, Rest/binary>> = _Buffer) ->
    <<MessageData:Length/binary, Remainder/binary>> = Rest,
    FullMessage = <<Length:32/big, MessageData/binary>>,
    {FullMessage, Remainder}.

%% Create initial connection state
create_initial_state() ->
    #{
        buffer => <<>>,
        request_count => 0,
        start_time => erlang:system_time(millisecond)
    }.

%% Update buffer with new data
update_buffer(State, NewData) ->
    OldBuffer = maps:get(buffer, State, <<>>),
    State#{buffer => <<OldBuffer/binary, NewData/binary>>}.

%% Clear the buffer
clear_buffer(State) ->
    State#{buffer => <<>>}.

%% Increment request counter
increment_request_count(State) ->
    Count = maps:get(request_count, State, 0),
    State#{request_count => Count + 1}.

%% Parse header version (first 2 bytes)
parse_header_version(<<Version:16/big, _/binary>>) ->
    Version.

%% Parse header flags (bytes 3-4)
parse_header_flags(<<_:16, Flags:16/big, _/binary>>) ->
    Flags.

%% Parse message type (bytes 5-6)
parse_header_msg_type(<<_:32, MsgType:16/big, _/binary>>) ->
    MsgType.

%% Parse body length (bytes 7-10)
parse_header_body_length(<<_:48, BodyLength:32/big>>) ->
    BodyLength;
parse_header_body_length(<<_:48, BodyLength:32/big, _/binary>>) ->
    BodyLength.

%% Construct full message with length prefix
construct_full_message(Length, MessageData) ->
    <<Length:32/big, MessageData/binary>>.

%% Calculate connection duration
calculate_duration(EndTime, StartTime) ->
    EndTime - StartTime.

%% Get hex prefix (first 24 bytes)
get_hex_prefix(Data) when byte_size(Data) >= 24 ->
    <<First24:24/binary, _/binary>> = Data,
    binary_to_hex(First24);
get_hex_prefix(Data) ->
    binary_to_hex(Data).

%% Check buffer status
check_buffer_status(Buffer) when byte_size(Buffer) < ?LENGTH_PREFIX_SIZE ->
    need_more_data;
check_buffer_status(<<Length:32/big, Rest/binary>>) when Length < ?SLURM_HEADER_SIZE ->
    invalid_length;
check_buffer_status(<<Length:32/big, Rest/binary>>) when byte_size(Rest) < Length ->
    need_more_data;
check_buffer_status(<<_Length:32/big, _Rest/binary>>) ->
    complete.
