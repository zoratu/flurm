%%%-------------------------------------------------------------------
%%% @doc FLURM Protocol Header - SLURM Message Header Codec
%%%
%%% Handles encoding and decoding of SLURM message headers.
%%% Wire format (big-endian/network byte order):
%%%   - version:     2 bytes (uint16)
%%%   - flags:       2 bytes (uint16)
%%%   - msg_type:    2 bytes (uint16)
%%%   - body_length: 4 bytes (uint32) - supports messages up to 4GB
%%%   - forward_cnt: 2 bytes (uint16) - number of forward targets (usually 0)
%%%   - ret_cnt:     2 bytes (uint16) - number of return messages (usually 0)
%%%   - orig_addr:   variable length based on address family:
%%%                  - AF_UNSPEC (0): just family (2 bytes)
%%%                  - AF_INET (2): family(2) + addr(4) + port(2) = 8 bytes
%%%                  - AF_INET6 (10): family(2) + addr(16) + port(2) = 20 bytes
%%%
%%% If forward_cnt > 0, additional fields follow (nodelist, timeout, tree_width, etc.)
%%% If ret_cnt > 0, ret_list messages follow.
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

%% @doc Parse a variable-length SLURM header binary into a slurm_header record
%%
%% The header format is:
%%   <<Version:16/big, Flags:16/big, MsgType:16/big, BodyLength:32/big,
%%     ForwardCnt:16/big, RetCnt:16/big, OrigAddrFamily:16/big, [OrigAddr, OrigPort based on family]>>
%%
%% orig_addr length varies by family:
%%   - AF_UNSPEC (0): just family (2 bytes) - total header 16 bytes
%%   - AF_INET (2): family(2) + addr(4) + port(2) = 8 bytes - total header 22 bytes
%%   - AF_INET6 (10): family(2) + addr(16) + port(2) = 20 bytes - total header 34 bytes
%%
%% Returns {ok, Header, Rest} on success where Rest is any remaining data,
%% or {error, Reason} on failure.
-spec parse_header(binary()) -> {ok, #slurm_header{}, binary()} | {error, term()}.
%% AF_INET (family=2): 8 bytes for orig_addr
parse_header(<<Version:16/big,
               Flags:16/big,
               MsgType:16/big,
               BodyLength:32/big,
               ForwardCnt:16/big,
               RetCnt:16/big,
               2:16/big,  %% AF_INET
               _OrigAddr:32/big,
               _OrigPort:16/big,
               Rest/binary>>) ->
    Header = #slurm_header{
        version = Version,
        flags = Flags,
        msg_index = 0,
        msg_type = MsgType,
        body_length = BodyLength,
        forward_cnt = ForwardCnt,
        ret_cnt = RetCnt
    },
    {ok, Header, Rest};
%% AF_INET6 (family=10): 20 bytes for orig_addr
parse_header(<<Version:16/big,
               Flags:16/big,
               MsgType:16/big,
               BodyLength:32/big,
               ForwardCnt:16/big,
               RetCnt:16/big,
               10:16/big,  %% AF_INET6
               _OrigAddr:128/big,  %% 16 bytes for IPv6
               _OrigPort:16/big,
               Rest/binary>>) ->
    Header = #slurm_header{
        version = Version,
        flags = Flags,
        msg_index = 0,
        msg_type = MsgType,
        body_length = BodyLength,
        forward_cnt = ForwardCnt,
        ret_cnt = RetCnt
    },
    {ok, Header, Rest};
%% AF_UNSPEC (family=0) or other: just 2 bytes for family
parse_header(<<Version:16/big,
               Flags:16/big,
               MsgType:16/big,
               BodyLength:32/big,
               ForwardCnt:16/big,
               RetCnt:16/big,
               _OrigAddrFamily:16/big,  %% AF_UNSPEC or unknown
               Rest/binary>>) ->
    Header = #slurm_header{
        version = Version,
        flags = Flags,
        msg_index = 0,
        msg_type = MsgType,
        body_length = BodyLength,
        forward_cnt = ForwardCnt,
        ret_cnt = RetCnt
    },
    {ok, Header, Rest};
parse_header(Binary) when is_binary(Binary), byte_size(Binary) < ?SLURM_HEADER_SIZE_MIN ->
    {error, {incomplete_header, byte_size(Binary), ?SLURM_HEADER_SIZE_MIN}};
parse_header(_) ->
    {error, invalid_header_data}.

%% @doc Encode a slurm_header record into a 22-byte binary
%%
%% Takes a #slurm_header{} record and produces a big-endian binary.
%% Wire format: version(2) + flags(2) + msg_type(2) + body_length(4) + forward_cnt(2) + ret_cnt(2) + orig_addr(8)
%% orig_addr format for IPv4: family(2) + addr(4) + port(2) = 8 bytes
-spec encode_header(#slurm_header{}) -> {ok, binary()} | {error, term()}.
encode_header(#slurm_header{
    version = Version,
    flags = Flags,
    msg_type = MsgType,
    body_length = BodyLength,
    forward_cnt = ForwardCnt,
    ret_cnt = RetCnt
}) when is_integer(Version), Version >= 0, Version =< 65535,
        is_integer(Flags), Flags >= 0, Flags =< 65535,
        is_integer(MsgType), MsgType >= 0, MsgType =< 65535,
        is_integer(BodyLength), BodyLength >= 0, BodyLength =< 4294967295,
        is_integer(ForwardCnt), ForwardCnt >= 0, ForwardCnt =< 65535,
        is_integer(RetCnt), RetCnt >= 0, RetCnt =< 65535 ->
    %% orig_addr: IPv4 format - family(2) + addr(4) + port(2)
    %% AF_INET = 2, address = 0.0.0.0, port = 0
    OrigAddr = <<2:16/big, 0:32/big, 0:16/big>>,
    Binary = <<Version:16/big,
               Flags:16/big,
               MsgType:16/big,
               BodyLength:32/big,
               ForwardCnt:16/big,
               RetCnt:16/big,
               OrigAddr/binary>>,
    {ok, Binary};
encode_header(#slurm_header{} = Header) ->
    {error, {invalid_header_values, Header}};
encode_header(_) ->
    {error, invalid_header_record}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% None currently needed - header encoding/decoding is straightforward.
