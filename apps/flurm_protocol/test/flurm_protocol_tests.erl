%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit Tests for FLURM Protocol Module
%%%
%%% Tests for the high-level protocol encoding/decoding functions
%%% with focus on:
%%% - Success cases for all message types
%%% - Error handling paths
%%% - Edge cases
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

%%%===================================================================
%%% encode/1 Tests
%%%===================================================================

encode_success_test_() ->
    [
        {"encode simple message", fun() ->
            Msg = #{type => job_submit, payload => #{name => <<"test">>}},
            {ok, Binary} = flurm_protocol:encode(Msg),
            ?assert(is_binary(Binary))
        end},

        {"encode with empty payload", fun() ->
            Msg = #{type => job_status, payload => #{}},
            {ok, Binary} = flurm_protocol:encode(Msg),
            ?assert(is_binary(Binary))
        end},

        {"encode all known message types", fun() ->
            Types = [job_submit, job_cancel, job_status, job_launch, job_complete, job_failed,
                     node_register, node_register_ack, node_heartbeat, node_heartbeat_ack,
                     node_status, node_drain, node_resume,
                     partition_create, partition_update, partition_delete,
                     ack, error],
            lists:foreach(fun(Type) ->
                Msg = #{type => Type, payload => #{}},
                {ok, Binary} = flurm_protocol:encode(Msg),
                ?assert(is_binary(Binary))
            end, Types)
        end},

        {"encode unknown type uses type 0", fun() ->
            Msg = #{type => unknown_type, payload => #{}},
            {ok, Binary} = flurm_protocol:encode(Msg),
            ?assert(is_binary(Binary)),
            %% Type should be encoded as 0
            <<0:16, _/binary>> = Binary
        end},

        {"encode with binary payload", fun() ->
            Msg = #{type => job_submit, payload => <<"raw binary">>},
            {ok, Binary} = flurm_protocol:encode(Msg),
            ?assert(is_binary(Binary))
        end},

        {"encode with complex payload", fun() ->
            Msg = #{type => job_submit, payload => #{
                name => <<"test_job">>,
                nodes => 4,
                tasks => 16,
                memory => 1024,
                nested => #{key => <<"value">>}
            }},
            {ok, Binary} = flurm_protocol:encode(Msg),
            ?assert(is_binary(Binary))
        end}
    ].

encode_error_test_() ->
    [
        {"encode missing type field", fun() ->
            Msg = #{payload => #{}},
            Result = flurm_protocol:encode(Msg),
            ?assertMatch({error, _}, Result)
        end},

        {"encode missing payload field", fun() ->
            Msg = #{type => job_submit},
            Result = flurm_protocol:encode(Msg),
            ?assertMatch({error, _}, Result)
        end},

        {"encode non-map message", fun() ->
            Result = flurm_protocol:encode(not_a_map),
            ?assertMatch({error, _}, Result)
        end},

        {"encode with invalid payload type", fun() ->
            Msg = #{type => job_submit, payload => invalid_payload_atom},
            Result = flurm_protocol:encode(Msg),
            ?assertMatch({error, _}, Result)
        end}
    ].

%%%===================================================================
%%% decode/1 Tests
%%%===================================================================

decode_success_test_() ->
    [
        {"decode simple message", fun() ->
            %% Create a valid encoded message
            Msg = #{type => job_status, payload => #{}},
            {ok, Encoded} = flurm_protocol:encode(Msg),
            {ok, Decoded} = flurm_protocol:decode(Encoded),
            ?assert(is_map(Decoded))
        end},

        {"decode preserves type", fun() ->
            Msg = #{type => ack, payload => #{}},
            {ok, Encoded} = flurm_protocol:encode(Msg),
            {ok, Decoded} = flurm_protocol:decode(Encoded),
            ?assertEqual(ack, maps:get(type, Decoded))
        end},

        {"decode preserves payload", fun() ->
            Payload = #{key => <<"value">>, num => 42},
            Msg = #{type => job_submit, payload => Payload},
            {ok, Encoded} = flurm_protocol:encode(Msg),
            {ok, Decoded} = flurm_protocol:decode(Encoded),
            DecodedPayload = maps:get(payload, Decoded),
            ?assertEqual(<<"value">>, maps:get(<<"key">>, DecodedPayload))
        end}
    ].

decode_error_test_() ->
    [
        {"decode empty binary", fun() ->
            Result = flurm_protocol:decode(<<>>),
            ?assertMatch({error, _}, Result)
        end},

        {"decode incomplete binary", fun() ->
            Result = flurm_protocol:decode(<<1, 2, 3>>),
            ?assertMatch({error, _}, Result)
        end},

        {"decode non-binary", fun() ->
            %% Non-binary input causes function_clause error since decode/1 pattern matches on binary
            Result = try flurm_protocol:decode(not_a_binary) of
                R -> R
            catch
                error:function_clause -> {error, invalid_input}
            end,
            ?assertMatch({error, _}, Result)
        end},

        {"decode malformed binary", fun() ->
            %% Create binary with invalid length field
            Result = flurm_protocol:decode(<<0, 0, 255, 255, 0, 0>>),
            ?assertMatch({error, _}, Result)
        end}
    ].

%%%===================================================================
%%% encode_message/2 Tests
%%%===================================================================

encode_message_test_() ->
    [
        {"encode_message with type and payload", fun() ->
            {ok, Binary} = flurm_protocol:encode_message(job_submit, #{name => <<"test">>}),
            ?assert(is_binary(Binary))
        end},

        {"encode_message with empty payload", fun() ->
            {ok, Binary} = flurm_protocol:encode_message(ack, #{}),
            ?assert(is_binary(Binary))
        end},

        {"encode_message delegates to encode", fun() ->
            Payload = #{key => <<"value">>},
            {ok, Binary1} = flurm_protocol:encode_message(job_submit, Payload),
            {ok, Binary2} = flurm_protocol:encode(#{type => job_submit, payload => Payload}),
            ?assertEqual(Binary1, Binary2)
        end}
    ].

%%%===================================================================
%%% decode_message/1 Tests
%%%===================================================================

decode_message_test_() ->
    [
        {"decode_message returns type and payload", fun() ->
            Msg = #{type => job_submit, payload => #{name => <<"test">>}},
            {ok, Encoded} = flurm_protocol:encode(Msg),
            {ok, Type, Payload} = flurm_protocol:decode_message(Encoded),
            ?assertEqual(job_submit, Type),
            ?assert(is_map(Payload))
        end},

        {"decode_message with empty payload", fun() ->
            Msg = #{type => ack, payload => #{}},
            {ok, Encoded} = flurm_protocol:encode(Msg),
            {ok, Type, Payload} = flurm_protocol:decode_message(Encoded),
            ?assertEqual(ack, Type),
            ?assertEqual(#{}, Payload)
        end},

        {"decode_message error propagates", fun() ->
            Result = flurm_protocol:decode_message(<<>>),
            ?assertMatch({error, _}, Result)
        end}
    ].

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_test_() ->
    [
        {"simple roundtrip", fun() ->
            Original = #{type => job_submit, payload => #{name => <<"test">>}},
            {ok, Encoded} = flurm_protocol:encode(Original),
            {ok, Decoded} = flurm_protocol:decode(Encoded),
            ?assertEqual(maps:get(type, Original), maps:get(type, Decoded))
        end},

        {"roundtrip with all message types", fun() ->
            Types = [job_submit, job_cancel, job_status, ack, error],
            lists:foreach(fun(Type) ->
                Original = #{type => Type, payload => #{test => <<"data">>}},
                {ok, Encoded} = flurm_protocol:encode(Original),
                {ok, Decoded} = flurm_protocol:decode(Encoded),
                ?assertEqual(Type, maps:get(type, Decoded))
            end, Types)
        end},

        {"roundtrip with complex payload", fun() ->
            Payload = #{
                string => <<"hello">>,
                number => 12345,
                list => [1, 2, 3],
                nested => #{key => <<"value">>}
            },
            Original = #{type => job_submit, payload => Payload},
            {ok, Encoded} = flurm_protocol:encode(Original),
            {ok, Decoded} = flurm_protocol:decode(Encoded),
            DecodedPayload = maps:get(payload, Decoded),
            ?assertEqual(<<"hello">>, maps:get(<<"string">>, DecodedPayload)),
            ?assertEqual(12345, maps:get(<<"number">>, DecodedPayload))
        end},

        {"roundtrip with empty payload", fun() ->
            Original = #{type => ack, payload => #{}},
            {ok, Encoded} = flurm_protocol:encode(Original),
            {ok, Decoded} = flurm_protocol:decode(Encoded),
            ?assertEqual(ack, maps:get(type, Decoded)),
            ?assertEqual(#{}, maps:get(payload, Decoded))
        end}
    ].

%%%===================================================================
%%% Type Encoding Tests
%%%===================================================================

type_encoding_test_() ->
    [
        {"job_submit encodes to 1", fun() ->
            Msg = #{type => job_submit, payload => #{}},
            {ok, Binary} = flurm_protocol:encode(Msg),
            <<1:16, _/binary>> = Binary,
            ?assert(true)
        end},

        {"job_cancel encodes to 2", fun() ->
            Msg = #{type => job_cancel, payload => #{}},
            {ok, Binary} = flurm_protocol:encode(Msg),
            <<2:16, _/binary>> = Binary,
            ?assert(true)
        end},

        {"node_register encodes to 10", fun() ->
            Msg = #{type => node_register, payload => #{}},
            {ok, Binary} = flurm_protocol:encode(Msg),
            <<10:16, _/binary>> = Binary,
            ?assert(true)
        end},

        {"ack encodes to 100", fun() ->
            Msg = #{type => ack, payload => #{}},
            {ok, Binary} = flurm_protocol:encode(Msg),
            <<100:16, _/binary>> = Binary,
            ?assert(true)
        end},

        {"error encodes to 101", fun() ->
            Msg = #{type => error, payload => #{}},
            {ok, Binary} = flurm_protocol:encode(Msg),
            <<101:16, _/binary>> = Binary,
            ?assert(true)
        end},

        {"unknown decodes from 999", fun() ->
            %% Manually construct a message with type 999
            Payload = jsx:encode(#{}),
            PayloadSize = byte_size(Payload),
            Binary = <<999:16, PayloadSize:32, Payload/binary>>,
            {ok, Decoded} = flurm_protocol:decode(Binary),
            ?assertEqual(unknown, maps:get(type, Decoded))
        end}
    ].

%%%===================================================================
%%% Wire Format Tests
%%%===================================================================

wire_format_test_() ->
    [
        {"encoded message has correct structure", fun() ->
            Msg = #{type => job_submit, payload => #{}},
            {ok, Binary} = flurm_protocol:encode(Msg),
            %% Type (2 bytes) + PayloadSize (4 bytes) + Payload
            <<_Type:16, PayloadSize:32, PayloadBin/binary>> = Binary,
            ?assertEqual(PayloadSize, byte_size(PayloadBin))
        end},

        {"payload size is correct", fun() ->
            Payload = #{key => <<"a long value string">>},
            Msg = #{type => job_submit, payload => Payload},
            {ok, Binary} = flurm_protocol:encode(Msg),
            <<_Type:16, PayloadSize:32, PayloadBin/binary>> = Binary,
            ?assertEqual(PayloadSize, byte_size(PayloadBin)),
            %% Verify we can decode the payload
            DecodedPayload = jsx:decode(PayloadBin, [return_maps]),
            ?assertEqual(<<"a long value string">>, maps:get(<<"key">>, DecodedPayload))
        end}
    ].

%%%===================================================================
%%% Edge Case Tests
%%%===================================================================

edge_case_test_() ->
    [
        {"encode with unicode in payload", fun() ->
            Payload = #{name => unicode:characters_to_binary("cafe" ++ [233])},  % cafe with accent
            Msg = #{type => job_submit, payload => Payload},
            {ok, Binary} = flurm_protocol:encode(Msg),
            ?assert(is_binary(Binary))
        end},

        {"encode with large payload", fun() ->
            LargeString = list_to_binary(lists:duplicate(10000, $x)),
            Payload = #{data => LargeString},
            Msg = #{type => job_submit, payload => Payload},
            {ok, Binary} = flurm_protocol:encode(Msg),
            ?assert(byte_size(Binary) > 10000)
        end},

        {"encode with nested structures", fun() ->
            Payload = #{
                level1 => #{
                    level2 => #{
                        level3 => <<"deep value">>
                    }
                }
            },
            Msg = #{type => job_submit, payload => Payload},
            {ok, Binary} = flurm_protocol:encode(Msg),
            {ok, Decoded} = flurm_protocol:decode(Binary),
            DecodedPayload = maps:get(payload, Decoded),
            Level1 = maps:get(<<"level1">>, DecodedPayload),
            Level2 = maps:get(<<"level2">>, Level1),
            Level3 = maps:get(<<"level3">>, Level2),
            ?assertEqual(<<"deep value">>, Level3)
        end},

        {"encode with arrays in payload", fun() ->
            Payload = #{items => [1, 2, 3, 4, 5]},
            Msg = #{type => job_submit, payload => Payload},
            {ok, Binary} = flurm_protocol:encode(Msg),
            {ok, Decoded} = flurm_protocol:decode(Binary),
            DecodedPayload = maps:get(payload, Decoded),
            ?assertEqual([1, 2, 3, 4, 5], maps:get(<<"items">>, DecodedPayload))
        end},

        {"encode with special characters", fun() ->
            Payload = #{script => <<"#!/bin/bash\necho \"Hello\\nWorld\"">>},
            Msg = #{type => job_submit, payload => Payload},
            {ok, Binary} = flurm_protocol:encode(Msg),
            ?assert(is_binary(Binary))
        end}
    ].
