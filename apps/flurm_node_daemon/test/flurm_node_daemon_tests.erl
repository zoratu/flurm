%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon Tests
%%%
%%% Tests for the node daemon protocol components.
%%% Note: System monitor tests require lager and are skipped in eunit.
%%% Use common_test for full integration tests.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures - Protocol Tests (don't require lager)
%%====================================================================

protocol_test_() ->
    [
     {"Protocol encodes job_launch", fun test_protocol_job_launch/0},
     {"Protocol encodes job_complete", fun test_protocol_job_complete/0},
     {"Protocol encodes job_failed", fun test_protocol_job_failed/0},
     {"Protocol encodes node_register", fun test_protocol_node_register/0},
     {"Protocol encodes node_heartbeat", fun test_protocol_node_heartbeat/0},
     {"Protocol round trip all message types", fun test_protocol_round_trip/0}
    ].

%%====================================================================
%% Protocol Tests
%%====================================================================

test_protocol_job_launch() ->
    Msg = #{type => job_launch, payload => #{job_id => 123, script => <<"test">>}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(job_launch, maps:get(type, Decoded)),
    ok.

test_protocol_job_complete() ->
    Msg = #{type => job_complete, payload => #{job_id => 123, exit_code => 0}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(job_complete, maps:get(type, Decoded)),
    ok.

test_protocol_job_failed() ->
    Msg = #{type => job_failed, payload => #{job_id => 123, exit_code => 1, reason => <<"timeout">>}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(job_failed, maps:get(type, Decoded)),
    ok.

test_protocol_node_register() ->
    Msg = #{type => node_register, payload => #{hostname => <<"node1">>, cpus => 8, memory_mb => 32768}},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_register, maps:get(type, Decoded)),
    %% Verify payload is preserved
    Payload = maps:get(payload, Decoded),
    ?assertEqual(<<"node1">>, maps:get(<<"hostname">>, Payload)),
    ?assertEqual(8, maps:get(<<"cpus">>, Payload)),
    ok.

test_protocol_node_heartbeat() ->
    Msg = #{type => node_heartbeat, payload => #{
        hostname => <<"node1">>,
        load_avg => 1.5,
        running_jobs => [1, 2, 3],
        free_memory_mb => 16384
    }},
    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(is_binary(Encoded)),
    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(node_heartbeat, maps:get(type, Decoded)),
    ok.

test_protocol_round_trip() ->
    %% Test all new message types encode/decode correctly
    TestCases = [
        #{type => job_launch, payload => #{job_id => 123}},
        #{type => job_complete, payload => #{job_id => 123, exit_code => 0}},
        #{type => job_failed, payload => #{job_id => 123, exit_code => 1}},
        #{type => node_register, payload => #{hostname => <<"node1">>}},
        #{type => node_register_ack, payload => #{node_id => <<"n001">>}},
        #{type => node_heartbeat, payload => #{load_avg => 1.5}},
        #{type => node_heartbeat_ack, payload => #{}},
        #{type => node_drain, payload => #{}},
        #{type => node_resume, payload => #{}}
    ],

    lists:foreach(fun(Msg) ->
        {ok, Encoded} = flurm_protocol:encode(Msg),
        ?assert(is_binary(Encoded)),
        ?assert(byte_size(Encoded) > 0),
        {ok, Decoded} = flurm_protocol:decode(Encoded),
        ?assertEqual(maps:get(type, Msg), maps:get(type, Decoded))
    end, TestCases),

    ok.

%%====================================================================
%% Unit tests for helper functions
%%====================================================================

message_framing_test() ->
    %% Test length-prefixed framing
    Msg = #{type => job_launch, payload => #{job_id => 42}},
    {ok, Encoded} = flurm_protocol:encode(Msg),

    %% Add length prefix like the connector does
    Len = byte_size(Encoded),
    Framed = <<Len:32, Encoded/binary>>,

    %% Verify we can extract the length and decode
    <<ExtractedLen:32, EncodedPart/binary>> = Framed,
    ?assertEqual(Len, ExtractedLen),
    ?assertEqual(Encoded, EncodedPart),

    {ok, DecodedMsg} = flurm_protocol:decode(EncodedPart),
    ?assertEqual(job_launch, maps:get(type, DecodedMsg)),
    ok.

empty_payload_test() ->
    %% Test messages with empty payloads
    EmptyMsgs = [
        #{type => node_heartbeat_ack, payload => #{}},
        #{type => node_drain, payload => #{}},
        #{type => node_resume, payload => #{}}
    ],

    lists:foreach(fun(Msg) ->
        {ok, Encoded} = flurm_protocol:encode(Msg),
        {ok, Decoded} = flurm_protocol:decode(Encoded),
        ?assertEqual(maps:get(type, Msg), maps:get(type, Decoded)),
        ?assert(is_map(maps:get(payload, Decoded)))
    end, EmptyMsgs),
    ok.

large_payload_test() ->
    %% Test with larger payloads (simulating job output)
    LargeScript = list_to_binary(lists:duplicate(10000, $X)),
    Msg = #{type => job_launch, payload => #{
        job_id => 999,
        script => LargeScript,
        working_dir => <<"/home/user/work">>,
        environment => #{<<"PATH">> => <<"/usr/bin:/bin">>}
    }},

    {ok, Encoded} = flurm_protocol:encode(Msg),
    ?assert(byte_size(Encoded) > 10000),

    {ok, Decoded} = flurm_protocol:decode(Encoded),
    ?assertEqual(job_launch, maps:get(type, Decoded)),

    Payload = maps:get(payload, Decoded),
    ?assertEqual(999, maps:get(<<"job_id">>, Payload)),
    DecodedScript = maps:get(<<"script">>, Payload),
    ?assertEqual(byte_size(LargeScript), byte_size(DecodedScript)),
    ok.
