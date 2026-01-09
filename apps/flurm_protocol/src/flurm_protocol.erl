%%%-------------------------------------------------------------------
%%% @doc FLURM Protocol - SLURM Binary Protocol Codec
%%%
%%% This module provides encoding and decoding functions for the SLURM
%%% wire protocol. It handles message framing, serialization, and
%%% deserialization of all SLURM protocol messages.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol).

-export([
    encode/1,
    decode/1,
    encode_message/2,
    decode_message/1
]).

-include("flurm_protocol.hrl").

%%====================================================================
%% API
%%====================================================================

%% @doc Encode a protocol message to binary
-spec encode(message()) -> {ok, binary()} | {error, term()}.
encode(Message) ->
    try
        {ok, do_encode(Message)}
    catch
        _:Reason ->
            {error, Reason}
    end.

%% @doc Decode binary data into a protocol message
-spec decode(binary()) -> {ok, message()} | {error, term()}.
decode(Binary) when is_binary(Binary) ->
    try
        {ok, do_decode(Binary)}
    catch
        _:Reason ->
            {error, Reason}
    end.

%% @doc Encode a message with a specific type
-spec encode_message(message_type(), map()) -> {ok, binary()} | {error, term()}.
encode_message(Type, Payload) ->
    encode(#{type => Type, payload => Payload}).

%% @doc Decode a message and return type and payload
-spec decode_message(binary()) -> {ok, message_type(), map()} | {error, term()}.
decode_message(Binary) ->
    case decode(Binary) of
        {ok, #{type := Type, payload := Payload}} ->
            {ok, Type, Payload};
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Internal functions
%%====================================================================

do_encode(#{type := Type, payload := Payload}) ->
    TypeBin = encode_type(Type),
    PayloadBin = encode_payload(Payload),
    PayloadSize = byte_size(PayloadBin),
    <<TypeBin:16, PayloadSize:32, PayloadBin/binary>>.

do_decode(<<TypeBin:16, PayloadSize:32, PayloadBin:PayloadSize/binary>>) ->
    Type = decode_type(TypeBin),
    Payload = decode_payload(PayloadBin),
    #{type => Type, payload => Payload}.

encode_type(job_submit) -> 1;
encode_type(job_cancel) -> 2;
encode_type(job_status) -> 3;
encode_type(node_register) -> 10;
encode_type(node_heartbeat) -> 11;
encode_type(node_status) -> 12;
encode_type(partition_create) -> 20;
encode_type(partition_update) -> 21;
encode_type(partition_delete) -> 22;
encode_type(ack) -> 100;
encode_type(error) -> 101;
encode_type(_) -> 0.

decode_type(1) -> job_submit;
decode_type(2) -> job_cancel;
decode_type(3) -> job_status;
decode_type(10) -> node_register;
decode_type(11) -> node_heartbeat;
decode_type(12) -> node_status;
decode_type(20) -> partition_create;
decode_type(21) -> partition_update;
decode_type(22) -> partition_delete;
decode_type(100) -> ack;
decode_type(101) -> error;
decode_type(_) -> unknown.

encode_payload(Payload) when is_map(Payload) ->
    jsx:encode(Payload);
encode_payload(Payload) when is_binary(Payload) ->
    Payload.

decode_payload(<<>>) ->
    #{};
decode_payload(Binary) ->
    jsx:decode(Binary, [return_maps]).
