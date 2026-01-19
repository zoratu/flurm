%%%-------------------------------------------------------------------
%%% @doc Tests for -ifdef(TEST) exported functions in flurm_controller_protocol
%%%
%%% These tests directly call the internal helper functions that are
%%% exported via -ifdef(TEST) to provide real code coverage.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_protocol_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%====================================================================
%% process_buffer/1 Tests
%%====================================================================

process_buffer_empty_test() ->
    %% Empty buffer should return incomplete
    Result = flurm_controller_protocol:process_buffer(<<>>),
    ?assertEqual({incomplete, <<>>}, Result).

process_buffer_short_header_test() ->
    %% Buffer shorter than HEADER_SIZE (6 bytes) should return incomplete
    Buffer1 = <<1>>,
    ?assertEqual({incomplete, Buffer1}, flurm_controller_protocol:process_buffer(Buffer1)),

    Buffer2 = <<1, 2>>,
    ?assertEqual({incomplete, Buffer2}, flurm_controller_protocol:process_buffer(Buffer2)),

    Buffer3 = <<1, 2, 3>>,
    ?assertEqual({incomplete, Buffer3}, flurm_controller_protocol:process_buffer(Buffer3)),

    Buffer4 = <<1, 2, 3, 4>>,
    ?assertEqual({incomplete, Buffer4}, flurm_controller_protocol:process_buffer(Buffer4)),

    Buffer5 = <<1, 2, 3, 4, 5>>,
    ?assertEqual({incomplete, Buffer5}, flurm_controller_protocol:process_buffer(Buffer5)).

process_buffer_incomplete_payload_test() ->
    %% Header present but payload incomplete
    %% Type = 1, PayloadSize = 100, but only a few bytes of payload
    Buffer = <<1:16, 100:32, 1, 2, 3>>,
    Result = flurm_controller_protocol:process_buffer(Buffer),
    ?assertEqual({incomplete, Buffer}, Result).

process_buffer_zero_payload_size_test() ->
    %% Header with zero payload size - valid message with empty payload
    Buffer = <<1:16, 0:32>>,
    Result = flurm_controller_protocol:process_buffer(Buffer),
    %% Zero payload decodes successfully to a message
    ?assertMatch({ok, #{type := _}, _}, Result).

%%====================================================================
%% handle_message/1 Tests
%%====================================================================

handle_message_unknown_type_test() ->
    %% Unknown message type should return error
    Message = #{type => unknown_message_type_xyz, payload => #{}},
    Result = flurm_controller_protocol:handle_message(Message),
    ?assertEqual(#{type => error, payload => #{reason => <<"unknown_message_type">>}}, Result).

handle_message_unknown_type_with_payload_test() ->
    %% Unknown type with arbitrary payload
    Message = #{type => some_random_type, payload => #{data => <<"test">>}},
    Result = flurm_controller_protocol:handle_message(Message),
    ?assertEqual(#{type => error, payload => #{reason => <<"unknown_message_type">>}}, Result).

handle_message_unknown_type_empty_payload_test() ->
    %% Unknown type with empty payload
    Message = #{type => undefined_operation, payload => #{}},
    Result = flurm_controller_protocol:handle_message(Message),
    ?assertEqual(#{type => error, payload => #{reason => <<"unknown_message_type">>}}, Result).

%%====================================================================
%% job_to_map/1 Tests
%%====================================================================

job_to_map_basic_test() ->
    %% Test basic job to map conversion
    %% Create a tuple that looks like a job record:
    %% {job, Id, Name, User, Partition, State, ...}
    Job = {job, 123, <<"test_job">>, <<"user1">>, <<"default">>, running,
           <<"script">>, 1, 4, 4096, 3600, 100, 0, undefined, undefined, [],
           undefined, <<"/tmp">>, <<>>, <<>>, <<"account1">>, <<"normal">>,
           <<>>, [], <<>>, <<>>, <<>>, <<>>, 0, true},
    Result = flurm_controller_protocol:job_to_map(Job),
    ?assertEqual(123, maps:get(job_id, Result)),
    ?assertEqual(<<"test_job">>, maps:get(name, Result)),
    ?assertEqual(running, maps:get(state, Result)),
    ?assertEqual(<<"default">>, maps:get(partition, Result)).

job_to_map_with_different_state_test() ->
    %% Test job in pending state
    Job = {job, 456, <<"pending_job">>, <<"user2">>, <<"compute">>, pending,
           <<"script">>, 2, 8, 8192, 7200, 50, 0, undefined, undefined, [],
           undefined, <<"/home">>, <<>>, <<>>, <<"account2">>, <<"high">>,
           <<>>, [], <<>>, <<>>, <<>>, <<>>, 0, true},
    Result = flurm_controller_protocol:job_to_map(Job),
    ?assertEqual(456, maps:get(job_id, Result)),
    ?assertEqual(<<"pending_job">>, maps:get(name, Result)),
    ?assertEqual(pending, maps:get(state, Result)),
    ?assertEqual(<<"compute">>, maps:get(partition, Result)).

job_to_map_extracts_correct_fields_test() ->
    %% Verify the map has exactly the expected keys
    Job = {job, 789, <<"job_name">>, <<"user">>, <<"partition_name">>, completed,
           <<"script">>, 1, 1, 1024, 60, 100, 0, undefined, undefined, [],
           undefined, <<"/tmp">>, <<>>, <<>>, <<>>, <<>>,
           <<>>, [], <<>>, <<>>, <<>>, <<>>, 0, true},
    Result = flurm_controller_protocol:job_to_map(Job),
    ExpectedKeys = [job_id, name, state, partition],
    ?assertEqual(lists:sort(ExpectedKeys), lists:sort(maps:keys(Result))).

job_to_map_handles_binary_name_test() ->
    %% Verify binary name is preserved
    JobName = <<"my_complex_job_name_123">>,
    Job = {job, 1, JobName, <<"user">>, <<"part">>, running,
           <<"script">>, 1, 1, 1024, 60, 100, 0, undefined, undefined, [],
           undefined, <<"/tmp">>, <<>>, <<>>, <<>>, <<>>,
           <<>>, [], <<>>, <<>>, <<>>, <<>>, 0, true},
    Result = flurm_controller_protocol:job_to_map(Job),
    ?assertEqual(JobName, maps:get(name, Result)).

job_to_map_handles_binary_partition_test() ->
    %% Verify binary partition is preserved
    PartitionName = <<"gpu_partition">>,
    Job = {job, 2, <<"name">>, <<"user">>, PartitionName, running,
           <<"script">>, 1, 1, 1024, 60, 100, 0, undefined, undefined, [],
           undefined, <<"/tmp">>, <<>>, <<>>, <<>>, <<>>,
           <<>>, [], <<>>, <<>>, <<>>, <<>>, 0, true},
    Result = flurm_controller_protocol:job_to_map(Job),
    ?assertEqual(PartitionName, maps:get(partition, Result)).

%%====================================================================
%% Test Fixtures
%%====================================================================

process_buffer_test_() ->
    [
        {"empty buffer returns incomplete", fun process_buffer_empty_test/0},
        {"short header returns incomplete", fun process_buffer_short_header_test/0},
        {"incomplete payload returns incomplete", fun process_buffer_incomplete_payload_test/0},
        {"zero payload size succeeds", fun process_buffer_zero_payload_size_test/0}
    ].

handle_message_test_() ->
    [
        {"unknown type returns error", fun handle_message_unknown_type_test/0},
        {"unknown type with payload returns error", fun handle_message_unknown_type_with_payload_test/0},
        {"unknown type empty payload returns error", fun handle_message_unknown_type_empty_payload_test/0}
    ].

job_to_map_test_() ->
    [
        {"basic job conversion", fun job_to_map_basic_test/0},
        {"pending state job", fun job_to_map_with_different_state_test/0},
        {"extracts correct fields", fun job_to_map_extracts_correct_fields_test/0},
        {"preserves binary name", fun job_to_map_handles_binary_name_test/0},
        {"preserves binary partition", fun job_to_map_handles_binary_partition_test/0}
    ].
