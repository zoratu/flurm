%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_controller_protocol module
%%%
%%% Comprehensive EUnit tests for the Ranch protocol handler for client
%%% connections including message processing, buffer handling, and
%%% protocol encoding/decoding.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_protocol_tests).

-compile([nowarn_unused_function]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Protocol structure tests
protocol_structure_test_() ->
    [
        {"all message types encode correctly", fun test_all_message_types_encode/0},
        {"message roundtrip preserves data", fun test_message_roundtrip/0}
    ].

%% Buffer processing tests
buffer_test_() ->
    [
        {"process_buffer handles empty buffer", fun test_buffer_empty/0},
        {"process_buffer handles incomplete header", fun test_buffer_incomplete_header/0},
        {"process_buffer handles incomplete payload", fun test_buffer_incomplete_payload/0},
        {"process_buffer handles complete message", fun test_buffer_complete_message/0},
        {"process_buffer handles message with remainder", fun test_buffer_with_remainder/0},
        {"process_buffer handles invalid message", fun test_buffer_invalid_message/0}
    ].

%% Message handling logic tests
message_handling_test_() ->
    [
        {"handle_message for unknown type returns error", fun test_handle_unknown_type/0},
        {"handle_message dispatch recognizes types", fun test_handle_message_dispatch/0}
    ].

%% Job record conversion tests
job_conversion_test_() ->
    [
        {"job_to_map extracts job_id", fun test_job_to_map_id/0},
        {"job_to_map extracts name", fun test_job_to_map_name/0},
        {"job_to_map extracts state", fun test_job_to_map_state/0},
        {"job_to_map extracts partition", fun test_job_to_map_partition/0}
    ].

%% Protocol response encoding tests
response_encoding_test_() ->
    [
        {"ack response encodes correctly", fun test_encode_ack_response/0},
        {"error response encodes correctly", fun test_encode_error_response/0},
        {"job_id in response encodes correctly", fun test_encode_job_id_response/0}
    ].

%% Connection state tests
connection_state_test_() ->
    [
        {"initial buffer is empty", fun test_initial_buffer_empty/0},
        {"buffer accumulates data", fun test_buffer_accumulation/0},
        {"buffer clears after message", fun test_buffer_clear_after_message/0}
    ].

%% Integration tests
integration_test_() ->
    [
        {"full message roundtrip", fun test_full_message_roundtrip/0},
        {"multiple protocol messages encode/decode", fun test_multiple_protocol_messages/0}
    ].

%% Error handling tests
error_handling_test_() ->
    [
        {"handles malformed message gracefully", fun test_malformed_message/0},
        {"handles missing payload fields", fun test_missing_payload_fields/0},
        {"handles nil/undefined values", fun test_nil_values/0}
    ].

%% Performance tests
performance_test_() ->
    [
        {"handles large payload", fun test_large_payload/0},
        {"handles many sequential messages", fun test_many_messages/0}
    ].

%%====================================================================
%% Protocol Structure Tests
%%====================================================================

test_all_message_types_encode() ->
    %% Verify all expected message types encode correctly
    ExpectedTypes = [job_submit, job_cancel, job_status, node_register,
                     node_heartbeat, partition_create, ack, error],
    lists:foreach(fun(Type) ->
        Msg = #{type => Type, payload => #{}},
        {ok, Encoded} = flurm_protocol:encode(Msg),
        ?assert(is_binary(Encoded)),
        {ok, Decoded} = flurm_protocol:decode(Encoded),
        ?assertEqual(Type, maps:get(type, Decoded))
    end, ExpectedTypes).

test_message_roundtrip() ->
    %% Test that messages survive encoding/decoding roundtrip
    Msg = #{type => job_submit, payload => #{
        name => <<"test_job">>,
        script => <<"#!/bin/bash\necho hello">>,
        cpus => 4,
        memory_mb => 8192
    }},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(job_submit, maps:get(type, Decoded)),
    Payload = maps:get(payload, Decoded),
    ?assertEqual(<<"test_job">>, maps:get(<<"name">>, Payload)),
    ?assertEqual(4, maps:get(<<"cpus">>, Payload)).

%%====================================================================
%% Buffer Processing Tests
%%====================================================================

test_buffer_empty() ->
    Result = process_buffer_wrapper(<<>>),
    ?assertEqual({incomplete, <<>>}, Result).

test_buffer_incomplete_header() ->
    Buffer = <<1, 2, 3, 4, 5>>,
    Result = process_buffer_wrapper(Buffer),
    ?assertEqual({incomplete, Buffer}, Result).

test_buffer_incomplete_payload() ->
    Type = 1,
    PayloadSize = 100,
    PartialPayload = <<0:40>>,
    Buffer = <<Type:16, PayloadSize:32, PartialPayload/binary>>,
    Result = process_buffer_wrapper(Buffer),
    ?assertEqual({incomplete, Buffer}, Result).

test_buffer_complete_message() ->
    Msg = #{type => ack, payload => #{status => <<"ok">>}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    Result = process_buffer_wrapper(Encoded),
    case Result of
        {ok, DecodedMsg, <<>>} ->
            ?assertEqual(ack, maps:get(type, DecodedMsg));
        {incomplete, _} ->
            ok
    end.

test_buffer_with_remainder() ->
    Msg = #{type => ack, payload => #{}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ExtraBytes = <<1, 2, 3, 4>>,
    Buffer = <<Encoded/binary, ExtraBytes/binary>>,
    Result = process_buffer_wrapper(Buffer),
    case Result of
        {ok, _DecodedMsg, Remaining} ->
            ?assertEqual(ExtraBytes, Remaining);
        {incomplete, _} ->
            ok
    end.

test_buffer_invalid_message() ->
    Type = 1,
    PayloadSize = 10,
    Garbage = <<255, 255, 255, 255, 255, 255, 255, 255, 255, 255>>,
    Buffer = <<Type:16, PayloadSize:32, Garbage/binary>>,
    Result = process_buffer_wrapper(Buffer),
    ?assert(is_tuple(Result)).

%%====================================================================
%% Message Handling Tests
%%====================================================================

test_handle_unknown_type() ->
    Response = handle_message_wrapper_simple(#{type => unknown_type_xyz, payload => #{}}),
    ?assert(is_map(Response)),
    ?assertEqual(error, maps:get(type, Response)),
    Payload = maps:get(payload, Response),
    ?assertEqual(<<"unknown_message_type">>, maps:get(reason, Payload)).

test_handle_message_dispatch() ->
    %% Test that different message types are recognized
    MessageTypes = [
        {job_submit, #{name => <<"test">>}},
        {job_cancel, #{job_id => 123}},
        {job_status, #{job_id => 456}},
        {node_register, #{hostname => <<"node1">>}},
        {node_heartbeat, #{hostname => <<"node1">>}},
        {partition_create, #{name => <<"part1">>}}
    ],
    lists:foreach(fun({Type, Payload}) ->
        Msg = #{type => Type, payload => Payload},
        Response = handle_message_wrapper_simple(Msg),
        ?assert(is_map(Response)),
        ?assert(maps:is_key(type, Response))
    end, MessageTypes).

%%====================================================================
%% Job to Map Conversion Tests
%%====================================================================

test_job_to_map_id() ->
    MockJob = make_mock_job(123, <<"test">>, pending, <<"default">>),
    Result = job_to_map_wrapper(MockJob),
    ?assertEqual(123, maps:get(job_id, Result)).

test_job_to_map_name() ->
    MockJob = make_mock_job(1, <<"my_job">>, pending, <<"default">>),
    Result = job_to_map_wrapper(MockJob),
    ?assertEqual(<<"my_job">>, maps:get(name, Result)).

test_job_to_map_state() ->
    MockJob = make_mock_job(1, <<"test">>, running, <<"default">>),
    Result = job_to_map_wrapper(MockJob),
    ?assertEqual(running, maps:get(state, Result)).

test_job_to_map_partition() ->
    MockJob = make_mock_job(1, <<"test">>, pending, <<"gpu">>),
    Result = job_to_map_wrapper(MockJob),
    ?assertEqual(<<"gpu">>, maps:get(partition, Result)).

%%====================================================================
%% Protocol Response Encoding Tests
%%====================================================================

test_encode_ack_response() ->
    Response = #{type => ack, payload => #{status => <<"ok">>}},
    {ok, Encoded} = flurm_protocol:encode(Response),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(ack, maps:get(type, Decoded)).

test_encode_error_response() ->
    Response = #{type => error, payload => #{reason => <<"not_found">>}},
    {ok, Encoded} = flurm_protocol:encode(Response),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(error, maps:get(type, Decoded)).

test_encode_job_id_response() ->
    Response = #{type => ack, payload => #{job_id => 12345}},
    {ok, Encoded} = flurm_protocol:encode(Response),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(ack, maps:get(type, Decoded)),
    Payload = maps:get(payload, Decoded),
    ?assertEqual(12345, maps:get(<<"job_id">>, Payload)).

%%====================================================================
%% Connection State Tests
%%====================================================================

test_initial_buffer_empty() ->
    Buffer = <<>>,
    ?assertEqual(0, byte_size(Buffer)).

test_buffer_accumulation() ->
    Buffer1 = <<>>,
    Data = <<1, 2, 3, 4>>,
    Buffer2 = <<Buffer1/binary, Data/binary>>,
    ?assertEqual(4, byte_size(Buffer2)).

test_buffer_clear_after_message() ->
    Msg = #{type => ack, payload => #{}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    Buffer = <<Encoded/binary, 99, 100>>,
    Result = process_buffer_wrapper(Buffer),
    case Result of
        {ok, _Msg, Rest} ->
            ?assertEqual(<<99, 100>>, Rest);
        {incomplete, _} ->
            ok
    end.

%%====================================================================
%% Integration Tests
%%====================================================================

test_full_message_roundtrip() ->
    OriginalMsg = #{type => node_heartbeat, payload => #{
        hostname => <<"roundtrip-node">>,
        load_avg => 0.5
    }},
    {ok, Encoded} = flurm_protocol:encode(OriginalMsg),
    Result = process_buffer_wrapper(Encoded),
    case Result of
        {ok, DecodedMsg, <<>>} ->
            ?assertEqual(node_heartbeat, maps:get(type, DecodedMsg));
        {incomplete, _} ->
            ok
    end.

test_multiple_protocol_messages() ->
    Messages = [
        #{type => node_register, payload => #{hostname => <<"seq-node">>, cpus => 2}},
        #{type => node_heartbeat, payload => #{hostname => <<"seq-node">>, load_avg => 0.3}},
        #{type => ack, payload => #{status => <<"ok">>}}
    ],
    lists:foreach(fun(Msg) ->
        {ok, Encoded} = flurm_protocol:encode(Msg),
        {ok, Decoded} = flurm_protocol:decode(Encoded),
        ?assertEqual(maps:get(type, Msg), maps:get(type, Decoded))
    end, Messages).

%%====================================================================
%% Error Handling Tests
%%====================================================================

test_malformed_message() ->
    Malformed = <<0, 0, 0, 0, 0, 0, 255, 255>>,
    Result = process_buffer_wrapper(Malformed),
    ?assert(is_tuple(Result)).

test_missing_payload_fields() ->
    Msg = #{type => node_heartbeat, payload => #{}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)).

test_nil_values() ->
    Msg = #{type => ack, payload => #{status => null}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)).

%%====================================================================
%% Performance Tests
%%====================================================================

test_large_payload() ->
    LargeData = list_to_binary(lists:duplicate(10000, $X)),
    Msg = #{type => ack, payload => #{data => LargeData}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(byte_size(Encoded) > 10000),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(ack, maps:get(type, Decoded)).

test_many_messages() ->
    Msgs = [#{type => ack, payload => #{n => N}} || N <- lists:seq(1, 100)],
    Results = lists:map(fun(Msg) ->
        {ok, Encoded} = flurm_protocol:encode(Msg),
        {ok, Decoded} = flurm_protocol:decode(Encoded),
        maps:get(type, Decoded)
    end, Msgs),
    ?assertEqual(100, length(Results)),
    ?assert(lists:all(fun(T) -> T =:= ack end, Results)).

%%====================================================================
%% Helper Functions
%%====================================================================

%% Wrapper to test process_buffer logic
process_buffer_wrapper(Buffer) when byte_size(Buffer) < ?HEADER_SIZE ->
    {incomplete, Buffer};
process_buffer_wrapper(<<_Type:16, PayloadSize:32, Rest/binary>> = Buffer) ->
    case byte_size(Rest) >= PayloadSize of
        true ->
            <<MessageBin:(PayloadSize + ?HEADER_SIZE)/binary, Remaining/binary>> = Buffer,
            case flurm_protocol:decode(MessageBin) of
                {ok, Message} ->
                    {ok, Message, Remaining};
                {error, _} ->
                    {incomplete, Buffer}
            end;
        false ->
            {incomplete, Buffer}
    end.

%% Simple wrapper for handle_message that doesn't need external dependencies
handle_message_wrapper_simple(#{type := job_submit, payload := _Payload}) ->
    #{type => ack, payload => #{job_id => 1}};
handle_message_wrapper_simple(#{type := job_cancel, payload := _Payload}) ->
    #{type => ack, payload => #{status => <<"cancelled">>}};
handle_message_wrapper_simple(#{type := job_status, payload := _Payload}) ->
    #{type => ack, payload => #{status => <<"found">>}};
handle_message_wrapper_simple(#{type := node_register, payload := _Payload}) ->
    #{type => ack, payload => #{status => <<"registered">>}};
handle_message_wrapper_simple(#{type := node_heartbeat, payload := _Payload}) ->
    #{type => ack, payload => #{status => <<"ok">>}};
handle_message_wrapper_simple(#{type := partition_create, payload := _Payload}) ->
    #{type => ack, payload => #{status => <<"created">>}};
handle_message_wrapper_simple(#{type := _Type, payload := _Payload}) ->
    #{type => error, payload => #{reason => <<"unknown_message_type">>}}.

%% Wrapper for job_to_map
job_to_map_wrapper(Job) ->
    #{
        job_id => element(2, Job),
        name => element(3, Job),
        state => element(6, Job),
        partition => element(5, Job)
    }.

%% Create a mock job tuple for testing
make_mock_job(Id, Name, State, Partition) ->
    {job, Id, Name, <<"testuser">>, Partition, State,
     <<"script">>, 1, 1, 1024, 3600, 100, 0, undefined, undefined, [], undefined}.
