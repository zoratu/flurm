%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_node_acceptor -ifdef(TEST) exports
%%%
%%% Tests the helper functions exported under -ifdef(TEST) that handle
%%% pure data transformations without requiring mocks or network state.
%%%
%%% These tests focus on:
%%% - Node spec building from payloads
%%% - Heartbeat data extraction
%%% - Exit code to state mapping
%%% - Failure reason to state mapping
%%% - Message framing and extraction
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_acceptor_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% build_node_spec Tests
%%====================================================================

build_node_spec_test_() ->
    [
        {"build_node_spec with all fields", fun test_build_node_spec_full/0},
        {"build_node_spec with minimal fields", fun test_build_node_spec_minimal/0},
        {"build_node_spec uses defaults", fun test_build_node_spec_defaults/0},
        {"build_node_spec with custom values", fun test_build_node_spec_custom/0}
    ].

test_build_node_spec_full() ->
    Payload = #{
        <<"hostname">> => <<"node001">>,
        <<"cpus">> => 16,
        <<"memory_mb">> => 32768
    },
    Result = flurm_node_acceptor:build_node_spec(Payload),

    ?assertEqual(<<"node001">>, maps:get(hostname, Result)),
    ?assertEqual(16, maps:get(cpus, Result)),
    ?assertEqual(32768, maps:get(memory_mb, Result)),
    ?assertEqual(idle, maps:get(state, Result)),
    ?assertEqual([<<"default">>], maps:get(partitions, Result)).

test_build_node_spec_minimal() ->
    Payload = #{<<"hostname">> => <<"minimal-node">>},
    Result = flurm_node_acceptor:build_node_spec(Payload),

    ?assertEqual(<<"minimal-node">>, maps:get(hostname, Result)),
    ?assertEqual(1, maps:get(cpus, Result)),       % Default
    ?assertEqual(1024, maps:get(memory_mb, Result)), % Default
    ?assertEqual(idle, maps:get(state, Result)),
    ?assertEqual([<<"default">>], maps:get(partitions, Result)).

test_build_node_spec_defaults() ->
    %% Verify that missing cpus and memory_mb get default values
    Payload = #{<<"hostname">> => <<"test-node">>},
    Result = flurm_node_acceptor:build_node_spec(Payload),

    %% Default cpus is 1
    ?assertEqual(1, maps:get(cpus, Result)),
    %% Default memory_mb is 1024
    ?assertEqual(1024, maps:get(memory_mb, Result)).

test_build_node_spec_custom() ->
    Payload = #{
        <<"hostname">> => <<"gpu-node-01">>,
        <<"cpus">> => 64,
        <<"memory_mb">> => 262144  % 256 GB
    },
    Result = flurm_node_acceptor:build_node_spec(Payload),

    ?assertEqual(<<"gpu-node-01">>, maps:get(hostname, Result)),
    ?assertEqual(64, maps:get(cpus, Result)),
    ?assertEqual(262144, maps:get(memory_mb, Result)).

%%====================================================================
%% build_heartbeat_data Tests
%%====================================================================

build_heartbeat_data_test_() ->
    [
        {"build_heartbeat_data with all fields", fun test_build_heartbeat_full/0},
        {"build_heartbeat_data with minimal fields", fun test_build_heartbeat_minimal/0},
        {"build_heartbeat_data uses defaults", fun test_build_heartbeat_defaults/0},
        {"build_heartbeat_data with running jobs", fun test_build_heartbeat_with_jobs/0}
    ].

test_build_heartbeat_full() ->
    Payload = #{
        <<"hostname">> => <<"node001">>,
        <<"load_avg">> => 2.5,
        <<"free_memory_mb">> => 8192,
        <<"running_jobs">> => [100, 101, 102]
    },
    Result = flurm_node_acceptor:build_heartbeat_data(Payload),

    ?assertEqual(<<"node001">>, maps:get(hostname, Result)),
    ?assertEqual(2.5, maps:get(load_avg, Result)),
    ?assertEqual(8192, maps:get(free_memory_mb, Result)),
    ?assertEqual([100, 101, 102], maps:get(running_jobs, Result)).

test_build_heartbeat_minimal() ->
    Payload = #{<<"hostname">> => <<"idle-node">>},
    Result = flurm_node_acceptor:build_heartbeat_data(Payload),

    ?assertEqual(<<"idle-node">>, maps:get(hostname, Result)),
    ?assertEqual(0.0, maps:get(load_avg, Result)),      % Default
    ?assertEqual(0, maps:get(free_memory_mb, Result)),  % Default
    ?assertEqual([], maps:get(running_jobs, Result)).   % Default

test_build_heartbeat_defaults() ->
    %% Verify defaults are applied correctly
    Payload = #{<<"hostname">> => <<"test">>},
    Result = flurm_node_acceptor:build_heartbeat_data(Payload),

    ?assertEqual(0.0, maps:get(load_avg, Result)),
    ?assertEqual(0, maps:get(free_memory_mb, Result)),
    ?assertEqual([], maps:get(running_jobs, Result)).

test_build_heartbeat_with_jobs() ->
    Payload = #{
        <<"hostname">> => <<"busy-node">>,
        <<"load_avg">> => 8.0,
        <<"free_memory_mb">> => 1024,
        <<"running_jobs">> => [1, 2, 3, 4, 5, 6, 7, 8]
    },
    Result = flurm_node_acceptor:build_heartbeat_data(Payload),

    ?assertEqual(8, length(maps:get(running_jobs, Result))),
    ?assertEqual(8.0, maps:get(load_avg, Result)).

%%====================================================================
%% exit_code_to_state Tests
%%====================================================================

exit_code_to_state_test_() ->
    [
        {"exit code 0 -> completed", fun test_exit_code_zero/0},
        {"exit code 1 -> failed", fun test_exit_code_one/0},
        {"exit code 137 (SIGKILL) -> failed", fun test_exit_code_sigkill/0},
        {"exit code 255 -> failed", fun test_exit_code_255/0},
        {"negative exit code -> failed", fun test_exit_code_negative/0}
    ].

test_exit_code_zero() ->
    ?assertEqual(completed, flurm_node_acceptor:exit_code_to_state(0)).

test_exit_code_one() ->
    ?assertEqual(failed, flurm_node_acceptor:exit_code_to_state(1)).

test_exit_code_sigkill() ->
    %% 128 + 9 (SIGKILL) = 137
    ?assertEqual(failed, flurm_node_acceptor:exit_code_to_state(137)).

test_exit_code_255() ->
    ?assertEqual(failed, flurm_node_acceptor:exit_code_to_state(255)).

test_exit_code_negative() ->
    ?assertEqual(failed, flurm_node_acceptor:exit_code_to_state(-1)).

%%====================================================================
%% failure_reason_to_state Tests
%%====================================================================

failure_reason_to_state_test_() ->
    [
        {"timeout reason -> timeout state", fun test_failure_reason_timeout/0},
        {"out_of_memory reason -> failed state", fun test_failure_reason_oom/0},
        {"empty reason -> failed state", fun test_failure_reason_empty/0},
        {"unknown reason -> failed state", fun test_failure_reason_unknown/0}
    ].

test_failure_reason_timeout() ->
    ?assertEqual(timeout, flurm_node_acceptor:failure_reason_to_state(<<"timeout">>)).

test_failure_reason_oom() ->
    ?assertEqual(failed, flurm_node_acceptor:failure_reason_to_state(<<"out_of_memory">>)).

test_failure_reason_empty() ->
    ?assertEqual(failed, flurm_node_acceptor:failure_reason_to_state(<<>>)).

test_failure_reason_unknown() ->
    ?assertEqual(failed, flurm_node_acceptor:failure_reason_to_state(<<"unknown">>)),
    ?assertEqual(failed, flurm_node_acceptor:failure_reason_to_state(<<"node_failure">>)),
    ?assertEqual(failed, flurm_node_acceptor:failure_reason_to_state(<<"script_error">>)).

%%====================================================================
%% build_register_ack_payload Tests
%%====================================================================

build_register_ack_payload_test_() ->
    [
        {"build ack for simple hostname", fun test_build_ack_simple/0},
        {"build ack for FQDN hostname", fun test_build_ack_fqdn/0},
        {"ack payload structure", fun test_build_ack_structure/0}
    ].

test_build_ack_simple() ->
    Result = flurm_node_acceptor:build_register_ack_payload(<<"node001">>),

    ?assertEqual(<<"node001">>, maps:get(<<"node_id">>, Result)),
    ?assertEqual(<<"accepted">>, maps:get(<<"status">>, Result)).

test_build_ack_fqdn() ->
    Result = flurm_node_acceptor:build_register_ack_payload(<<"node001.cluster.local">>),

    ?assertEqual(<<"node001.cluster.local">>, maps:get(<<"node_id">>, Result)).

test_build_ack_structure() ->
    Result = flurm_node_acceptor:build_register_ack_payload(<<"test">>),

    %% Should have exactly 2 keys
    ?assertEqual(2, maps:size(Result)),
    ?assert(maps:is_key(<<"node_id">>, Result)),
    ?assert(maps:is_key(<<"status">>, Result)).

%%====================================================================
%% extract_message_from_buffer Tests
%%====================================================================

extract_message_from_buffer_test_() ->
    [
        {"extract complete message", fun test_extract_complete/0},
        {"extract with remaining data", fun test_extract_with_remainder/0},
        {"incomplete buffer - no length", fun test_extract_incomplete_no_len/0},
        {"incomplete buffer - partial message", fun test_extract_incomplete_partial/0},
        {"extract zero-length message", fun test_extract_zero_length/0},
        {"extract from exact buffer", fun test_extract_exact/0}
    ].

test_extract_complete() ->
    MsgData = <<"hello world">>,
    Len = byte_size(MsgData),
    Buffer = <<Len:32, MsgData/binary>>,

    Result = flurm_node_acceptor:extract_message_from_buffer(Buffer),
    ?assertMatch({ok, MsgData, <<>>}, Result).

test_extract_with_remainder() ->
    MsgData = <<"message1">>,
    Remainder = <<"extra data">>,
    Len = byte_size(MsgData),
    Buffer = <<Len:32, MsgData/binary, Remainder/binary>>,

    Result = flurm_node_acceptor:extract_message_from_buffer(Buffer),
    ?assertMatch({ok, MsgData, Remainder}, Result).

test_extract_incomplete_no_len() ->
    %% Only 3 bytes, not enough for 32-bit length prefix
    Buffer = <<1, 2, 3>>,

    Result = flurm_node_acceptor:extract_message_from_buffer(Buffer),
    ?assertMatch({incomplete, Buffer}, Result).

test_extract_incomplete_partial() ->
    %% Length says 100 bytes, but only 10 bytes of data
    Buffer = <<100:32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>,

    Result = flurm_node_acceptor:extract_message_from_buffer(Buffer),
    ?assertMatch({incomplete, Buffer}, Result).

test_extract_zero_length() ->
    %% Zero-length message (just the length prefix)
    Buffer = <<0:32>>,

    Result = flurm_node_acceptor:extract_message_from_buffer(Buffer),
    ?assertMatch({ok, <<>>, <<>>}, Result).

test_extract_exact() ->
    %% Buffer contains exactly one complete message
    MsgData = <<"test message content">>,
    Len = byte_size(MsgData),
    Buffer = <<Len:32, MsgData/binary>>,

    {ok, Extracted, Remaining} = flurm_node_acceptor:extract_message_from_buffer(Buffer),
    ?assertEqual(MsgData, Extracted),
    ?assertEqual(<<>>, Remaining).

%%====================================================================
%% frame_message Tests
%%====================================================================

frame_message_test_() ->
    [
        {"frame simple message", fun test_frame_simple/0},
        {"frame empty message", fun test_frame_empty/0},
        {"frame large message", fun test_frame_large/0},
        {"frame binary data", fun test_frame_binary/0}
    ].

test_frame_simple() ->
    Data = <<"hello">>,
    Result = flurm_node_acceptor:frame_message(Data),

    ?assertEqual(<<5:32, "hello">>, Result),
    ?assertEqual(4 + 5, byte_size(Result)).

test_frame_empty() ->
    Result = flurm_node_acceptor:frame_message(<<>>),

    ?assertEqual(<<0:32>>, Result),
    ?assertEqual(4, byte_size(Result)).

test_frame_large() ->
    %% 1000 byte message
    Data = binary:copy(<<$X>>, 1000),
    Result = flurm_node_acceptor:frame_message(Data),

    <<Len:32, Body/binary>> = Result,
    ?assertEqual(1000, Len),
    ?assertEqual(Data, Body).

test_frame_binary() ->
    %% Binary data with various byte values
    Data = <<0, 1, 255, 128, 64, 32, 16, 8, 4, 2>>,
    Result = flurm_node_acceptor:frame_message(Data),

    <<Len:32, Body/binary>> = Result,
    ?assertEqual(10, Len),
    ?assertEqual(Data, Body).

%%====================================================================
%% Round-trip Tests (frame then extract)
%%====================================================================

roundtrip_test_() ->
    [
        {"roundtrip simple message", fun test_roundtrip_simple/0},
        {"roundtrip complex message", fun test_roundtrip_complex/0},
        {"roundtrip multiple messages", fun test_roundtrip_multiple/0}
    ].

test_roundtrip_simple() ->
    Original = <<"test data">>,
    Framed = flurm_node_acceptor:frame_message(Original),
    {ok, Extracted, <<>>} = flurm_node_acceptor:extract_message_from_buffer(Framed),

    ?assertEqual(Original, Extracted).

test_roundtrip_complex() ->
    %% Simulate an encoded protocol message
    Original = term_to_binary(#{type => node_heartbeat, payload => #{}}),
    Framed = flurm_node_acceptor:frame_message(Original),
    {ok, Extracted, <<>>} = flurm_node_acceptor:extract_message_from_buffer(Framed),

    ?assertEqual(Original, Extracted),
    ?assertEqual(#{type => node_heartbeat, payload => #{}}, binary_to_term(Extracted)).

test_roundtrip_multiple() ->
    Msg1 = <<"first message">>,
    Msg2 = <<"second message">>,

    Framed1 = flurm_node_acceptor:frame_message(Msg1),
    Framed2 = flurm_node_acceptor:frame_message(Msg2),
    Buffer = <<Framed1/binary, Framed2/binary>>,

    %% Extract first message
    {ok, Extracted1, Remainder} = flurm_node_acceptor:extract_message_from_buffer(Buffer),
    ?assertEqual(Msg1, Extracted1),

    %% Extract second message
    {ok, Extracted2, <<>>} = flurm_node_acceptor:extract_message_from_buffer(Remainder),
    ?assertEqual(Msg2, Extracted2).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    [
        {"very large node spec values", fun test_large_node_spec_values/0},
        {"float load average precision", fun test_float_precision/0},
        {"empty running jobs list", fun test_empty_running_jobs/0},
        {"binary hostname edge cases", fun test_hostname_edge_cases/0}
    ].

test_large_node_spec_values() ->
    Payload = #{
        <<"hostname">> => <<"mega-node">>,
        <<"cpus">> => 1024,
        <<"memory_mb">> => 4194304  % 4 TB
    },
    Result = flurm_node_acceptor:build_node_spec(Payload),

    ?assertEqual(1024, maps:get(cpus, Result)),
    ?assertEqual(4194304, maps:get(memory_mb, Result)).

test_float_precision() ->
    Payload = #{
        <<"hostname">> => <<"test">>,
        <<"load_avg">> => 3.14159265359
    },
    Result = flurm_node_acceptor:build_heartbeat_data(Payload),

    ?assertEqual(3.14159265359, maps:get(load_avg, Result)).

test_empty_running_jobs() ->
    Payload = #{
        <<"hostname">> => <<"idle">>,
        <<"running_jobs">> => []
    },
    Result = flurm_node_acceptor:build_heartbeat_data(Payload),

    ?assertEqual([], maps:get(running_jobs, Result)).

test_hostname_edge_cases() ->
    %% Single character hostname
    Result1 = flurm_node_acceptor:build_node_spec(#{<<"hostname">> => <<"a">>}),
    ?assertEqual(<<"a">>, maps:get(hostname, Result1)),

    %% Long hostname
    LongName = binary:copy(<<"node">>, 50),
    Result2 = flurm_node_acceptor:build_node_spec(#{<<"hostname">> => LongName}),
    ?assertEqual(LongName, maps:get(hostname, Result2)).
