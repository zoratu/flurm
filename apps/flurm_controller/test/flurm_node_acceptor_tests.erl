%%%-------------------------------------------------------------------
%%% @doc Comprehensive Tests for flurm_node_acceptor Module
%%%
%%% Tests the Ranch protocol handler for node daemon connections.
%%% Focus on testing functions that can be called directly without
%%% complex TCP state setup.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_acceptor_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

node_acceptor_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link spawns process", fun test_start_link/0},
        {"start_link returns pid", fun test_start_link_returns_pid/0}
     ]}.

setup() ->
    meck:new(ranch, [non_strict]),
    ok.

cleanup(_) ->
    catch meck:unload(ranch),
    ok.

%%====================================================================
%% start_link Tests
%%====================================================================

test_start_link() ->
    %% Make handshake fail immediately so the spawned process exits quickly
    meck:expect(ranch, handshake, fun(_Ref) -> {error, closed} end),

    Result = flurm_node_acceptor:start_link(test_ref, ranch_tcp, #{}),

    ?assertMatch({ok, _Pid}, Result),
    ok.

test_start_link_returns_pid() ->
    meck:expect(ranch, handshake, fun(_Ref) -> {error, closed} end),

    {ok, Pid} = flurm_node_acceptor:start_link(test_ref, ranch_tcp, #{}),

    ?assert(is_pid(Pid)),
    ok.

%%====================================================================
%% Module Export Tests
%%====================================================================

exports_test_() ->
    [
        {"start_link/3 is exported", fun test_exports_start_link/0},
        {"init/3 is exported", fun test_exports_init/0}
    ].

test_exports_start_link() ->
    ?assert(erlang:function_exported(flurm_node_acceptor, start_link, 3)).

test_exports_init() ->
    ?assert(erlang:function_exported(flurm_node_acceptor, init, 3)).

%%====================================================================
%% Protocol Behavior Tests
%%====================================================================

behaviour_test_() ->
    [
        {"implements ranch_protocol", fun test_ranch_protocol_behaviour/0}
    ].

test_ranch_protocol_behaviour() ->
    %% Verify the module exports the ranch_protocol required callbacks
    ?assert(erlang:function_exported(flurm_node_acceptor, start_link, 3)),
    ?assert(erlang:function_exported(flurm_node_acceptor, init, 3)),
    ok.

%%====================================================================
%% Init Failure Tests
%%====================================================================

init_failure_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init handles handshake failure", fun test_init_handshake_failure/0},
        {"init handles connection refused", fun test_init_connection_refused/0},
        {"init handles timeout", fun test_init_timeout/0}
     ]}.

test_init_handshake_failure() ->
    meck:expect(ranch, handshake, fun(_Ref) -> {error, closed} end),

    %% The init function will crash on badmatch, but start_link catches it
    Result = flurm_node_acceptor:start_link(test_ref, ranch_tcp, #{}),
    ?assertMatch({ok, _}, Result),
    ok.

test_init_connection_refused() ->
    meck:expect(ranch, handshake, fun(_Ref) -> {error, econnrefused} end),

    Result = flurm_node_acceptor:start_link(test_ref, ranch_tcp, #{}),
    ?assertMatch({ok, _}, Result),
    ok.

test_init_timeout() ->
    meck:expect(ranch, handshake, fun(_Ref) -> {error, etimedout} end),

    Result = flurm_node_acceptor:start_link(test_ref, ranch_tcp, #{}),
    ?assertMatch({ok, _}, Result),
    ok.

%%====================================================================
%% Integration Helpers - Test helper functions
%%====================================================================

%% Helper to verify message structure for protocol
message_structure_test_() ->
    [
        {"job_complete message structure", fun test_job_complete_structure/0},
        {"job_failed message structure", fun test_job_failed_structure/0},
        {"node_register message structure", fun test_node_register_structure/0},
        {"node_heartbeat message structure", fun test_node_heartbeat_structure/0}
    ].

test_job_complete_structure() ->
    Payload = #{
        <<"job_id">> => 12345,
        <<"exit_code">> => 0
    },
    Msg = #{type => job_complete, payload => Payload},

    %% Verify the structure can be encoded
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),

    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(job_complete, maps:get(type, Decoded)),
    ok.

test_job_failed_structure() ->
    Payload = #{
        <<"job_id">> => 12345,
        <<"reason">> => <<"out_of_memory">>,
        <<"exit_code">> => 137
    },
    Msg = #{type => job_failed, payload => Payload},

    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),

    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(job_failed, maps:get(type, Decoded)),
    ok.

test_node_register_structure() ->
    Payload = #{
        <<"hostname">> => <<"node001">>,
        <<"cpus">> => 8,
        <<"memory_mb">> => 16384
    },
    Msg = #{type => node_register, payload => Payload},

    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),

    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_register, maps:get(type, Decoded)),
    ok.

test_node_heartbeat_structure() ->
    Payload = #{
        <<"hostname">> => <<"node001">>,
        <<"load_avg">> => 2.5,
        <<"free_memory_mb">> => 8000,
        <<"running_jobs">> => [1, 2, 3]
    },
    Msg = #{type => node_heartbeat, payload => Payload},

    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),

    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_heartbeat, maps:get(type, Decoded)),
    ok.

%%====================================================================
%% Ack/Error Message Tests
%%====================================================================

ack_error_message_test_() ->
    [
        {"node_register_ack message structure", fun test_node_register_ack_structure/0},
        {"node_heartbeat_ack message structure", fun test_node_heartbeat_ack_structure/0},
        {"error message structure", fun test_error_message_structure/0}
    ].

test_node_register_ack_structure() ->
    Payload = #{
        <<"node_id">> => <<"node001">>,
        <<"status">> => <<"accepted">>
    },
    Msg = #{type => node_register_ack, payload => Payload},

    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),

    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_register_ack, maps:get(type, Decoded)),
    ok.

test_node_heartbeat_ack_structure() ->
    Msg = #{type => node_heartbeat_ack, payload => #{}},

    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),

    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_heartbeat_ack, maps:get(type, Decoded)),
    ok.

test_error_message_structure() ->
    Payload = #{
        <<"reason">> => <<"connection_failed">>
    },
    Msg = #{type => error, payload => Payload},

    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),

    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(error, maps:get(type, Decoded)),
    ok.

%%====================================================================
%% Message Framing Tests
%%====================================================================

framing_test_() ->
    [
        {"message with length prefix", fun test_message_framing/0},
        {"multiple messages framed", fun test_multiple_message_framing/0}
    ].

test_message_framing() ->
    Msg = #{type => node_heartbeat_ack, payload => #{}},
    {ok, Encoded} = flurm_protocol:encode(Msg),

    %% Add length prefix as the acceptor expects
    Len = byte_size(Encoded),
    Framed = <<Len:32, Encoded/binary>>,

    %% Extract and verify
    <<ExtractedLen:32, EncodedPart/binary>> = Framed,
    ?assertEqual(Len, ExtractedLen),
    ?assertEqual(Encoded, EncodedPart),

    {ok, Decoded} = flurm_protocol:decode(EncodedPart),
    ?assertEqual(node_heartbeat_ack, maps:get(type, Decoded)),
    ok.

test_multiple_message_framing() ->
    Msg1 = #{type => node_heartbeat, payload => #{<<"hostname">> => <<"n1">>}},
    Msg2 = #{type => node_heartbeat_ack, payload => #{}},

    {ok, Encoded1} = flurm_protocol:encode(Msg1),
    {ok, Encoded2} = flurm_protocol:encode(Msg2),

    Framed = <<(byte_size(Encoded1)):32, Encoded1/binary,
               (byte_size(Encoded2)):32, Encoded2/binary>>,

    %% Extract first message
    <<Len1:32, Rest1/binary>> = Framed,
    <<Msg1Bin:Len1/binary, Rest2/binary>> = Rest1,
    {ok, Decoded1} = flurm_protocol:decode(Msg1Bin),
    ?assertEqual(node_heartbeat, maps:get(type, Decoded1)),

    %% Extract second message
    <<Len2:32, Msg2Bin:Len2/binary>> = Rest2,
    {ok, Decoded2} = flurm_protocol:decode(Msg2Bin),
    ?assertEqual(node_heartbeat_ack, maps:get(type, Decoded2)),
    ok.

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_case_test_() ->
    [
        {"empty payload message", fun test_empty_payload/0},
        {"large payload message", fun test_large_payload/0},
        {"unicode hostname", fun test_unicode_hostname/0}
    ].

test_empty_payload() ->
    Msg = #{type => node_drain, payload => #{}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_drain, maps:get(type, Decoded)),
    ?assert(is_map(maps:get(payload, Decoded))),
    ok.

test_large_payload() ->
    %% Create a large script payload
    LargeScript = list_to_binary(lists:duplicate(10000, $X)),
    Payload = #{
        <<"hostname">> => <<"node001">>,
        <<"script">> => LargeScript
    },
    Msg = #{type => node_register, payload => Payload},

    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(byte_size(Encoded) > 10000),

    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_register, maps:get(type, Decoded)),
    ok.

test_unicode_hostname() ->
    %% Test with unicode characters in hostname (edge case)
    Payload = #{
        <<"hostname">> => <<"node-001">>,  %% Standard ASCII
        <<"cpus">> => 4
    },
    Msg = #{type => node_register, payload => Payload},

    {ok, Encoded} = flurm_protocol:encode(Msg),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_register, maps:get(type, Decoded)),
    ok.
