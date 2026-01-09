%%%-------------------------------------------------------------------
%%% @doc FLURM Protocol Header - SLURM Message Header Codec
%%%
%%% Handles encoding and decoding of the 10-byte SLURM message header.
%%% Wire format (big-endian/network byte order):
%%%   - version:     2 bytes (uint16)
%%%   - flags:       2 bytes (uint16)
%%%   - msg_index:   2 bytes (uint16)
%%%   - msg_type:    2 bytes (uint16)
%%%   - body_length: 2 bytes (uint16)
%%%
%%% Note: The 4-byte length prefix that precedes the header is handled
%%% separately by the codec module.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_header).

-export([
    parse_header/1,
    encode_header/1,
    protocol_version/0,
    header_size/0
]).

-include("flurm_protocol.hrl").

%%%===================================================================
%%% Constants
%%%===================================================================

%% Default protocol version (SLURM 22.05)
-define(DEFAULT_PROTOCOL_VERSION, ?SLURM_PROTOCOL_VERSION).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the current protocol version
-spec protocol_version() -> non_neg_integer().
protocol_version() ->
    ?DEFAULT_PROTOCOL_VERSION.

%% @doc Return the header size in bytes
-spec header_size() -> non_neg_integer().
header_size() ->
    ?SLURM_HEADER_SIZE.

%% @doc Parse a 10-byte binary into a slurm_header record
%%
%% The header format is:
%%   <<Version:16/big, Flags:16/big, MsgIndex:16/big, MsgType:16/big, BodyLength:16/big>>
%%
%% Returns {ok, Header, Rest} on success where Rest is any remaining data,
%% or {error, Reason} on failure.
-spec parse_header(binary()) -> {ok, #slurm_header{}, binary()} | {error, term()}.
parse_header(<<Version:16/big,
               Flags:16/big,
               MsgIndex:16/big,
               MsgType:16/big,
               BodyLength:16/big,
               Rest/binary>>) ->
    Header = #slurm_header{
        version = Version,
        flags = Flags,
        msg_index = MsgIndex,
        msg_type = MsgType,
        body_length = BodyLength
    },
    {ok, Header, Rest};
parse_header(Binary) when is_binary(Binary), byte_size(Binary) < ?SLURM_HEADER_SIZE ->
    {error, {incomplete_header, byte_size(Binary), ?SLURM_HEADER_SIZE}};
parse_header(_) ->
    {error, invalid_header_data}.

%% @doc Encode a slurm_header record into a 10-byte binary
%%
%% Takes a #slurm_header{} record and produces a big-endian binary.
-spec encode_header(#slurm_header{}) -> {ok, binary()} | {error, term()}.
encode_header(#slurm_header{
    version = Version,
    flags = Flags,
    msg_index = MsgIndex,
    msg_type = MsgType,
    body_length = BodyLength
}) when is_integer(Version), Version >= 0, Version =< 65535,
        is_integer(Flags), Flags >= 0, Flags =< 65535,
        is_integer(MsgIndex), MsgIndex >= 0, MsgIndex =< 65535,
        is_integer(MsgType), MsgType >= 0, MsgType =< 65535,
        is_integer(BodyLength), BodyLength >= 0, BodyLength =< 65535 ->
    Binary = <<Version:16/big,
               Flags:16/big,
               MsgIndex:16/big,
               MsgType:16/big,
               BodyLength:16/big>>,
    {ok, Binary};
encode_header(#slurm_header{} = Header) ->
    {error, {invalid_header_values, Header}};
encode_header(_) ->
    {error, invalid_header_record}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% None currently needed - header encoding/decoding is straightforward.
