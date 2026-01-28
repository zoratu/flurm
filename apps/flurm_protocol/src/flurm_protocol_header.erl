%%%-------------------------------------------------------------------
%%% @doc FLURM Protocol Header - SLURM Message Header Codec
%%%
%%% Handles encoding and decoding of SLURM message headers.
%%% Wire format (big-endian/network byte order):
%%%   - version:     2 bytes (uint16)
%%%   - flags:       2 bytes (uint16)
%%%   - msg_type:    2 bytes (uint16)
%%%   - body_length: 4 bytes (uint32) - body size ONLY (auth size determined separately)
%%%   - forward_cnt: 2 bytes (uint16) - number of forward targets (usually 0)
%%%   - ret_cnt:     2 bytes (uint16) - number of return messages (usually 0)
%%%   - orig_addr:   variable length based on address family:
%%%                  - AF_UNSPEC (0): just family (2 bytes)
%%%                  - AF_INET (2): family(2) + port(2) + addr(4) = 8 bytes
%%%                  - AF_INET6 (10): family(2) + port(2) + addr(16) = 20 bytes
%%%
%%% Fixed fields = 14 bytes (version + flags + msg_type + body_length + forward_cnt + ret_cnt)
%%% With AF_UNSPEC orig_addr = 14 + 2 = 16 bytes total
%%%
%%% NOTE: Empirical testing with SLURM 22.05.9 srun shows that messages do NOT
%%% include msg_index in the header, despite what the SLURM source code suggests
%%% for version >= 0x2500. Both requests from srun and responses to srun use
%%% the 16-byte format WITHOUT msg_index.
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

%% @doc Return the header size in bytes (16 bytes with AF_UNSPEC, no msg_index)
-spec header_size() -> non_neg_integer().
header_size() ->
    16.  %% version(2) + flags(2) + msg_type(2) + body_length(4) + forward_cnt(2) + ret_cnt(2) + orig_addr(2)

%% @doc Parse a variable-length SLURM header binary into a slurm_header record
%%
%% NOTE: Empirical testing with SLURM 22.05.9 shows that messages do NOT
%% include msg_index in the header, despite SLURM source code suggesting
%% msg_index for version >= 0x2500. We use 16-byte format WITHOUT msg_index.
%%
%% The header format is:
%%   <<Version:16, Flags:16, MsgType:16, BodyLength:32,
%%     ForwardCnt:16, RetCnt:16, OrigAddrFamily:16, [OrigAddr based on family]>>
%%
%% Fixed fields = 14 bytes (version + flags + msg_type + body_length + forward_cnt + ret_cnt)
%% orig_addr length varies by family:
%%   - AF_UNSPEC (0): just family (2 bytes) - total header 16 bytes
%%   - AF_INET (2): family(2) + port(2) + addr(4) = 8 bytes - total header 22 bytes
%%   - AF_INET6 (10): family(2) + port(2) + addr(16) = 20 bytes - total header 34 bytes
%%
%% Returns {ok, Header, Rest} on success where Rest is any remaining data,
%% or {error, Reason} on failure.
-spec parse_header(binary()) -> {ok, #slurm_header{}, binary()} | {error, term()}.
%% AF_INET (family=2): 8 bytes for orig_addr - 22 byte total header
parse_header(<<Version:16/big,
               Flags:16/big,
               MsgType:16/big,
               BodyLength:32/big,
               ForwardCnt:16/big,
               RetCnt:16/big,
               2:16/big,  %% AF_INET
               _OrigPort:16/big,
               _OrigAddr:32/big,
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
%% AF_INET6 (family=10): 20 bytes for orig_addr - 34 byte total header
parse_header(<<Version:16/big,
               Flags:16/big,
               MsgType:16/big,
               BodyLength:32/big,
               ForwardCnt:16/big,
               RetCnt:16/big,
               10:16/big,  %% AF_INET6
               _OrigPort:16/big,
               _OrigAddr:128/big,  %% 16 bytes for IPv6
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
%% AF_UNSPEC (family=0) or other: just 2 bytes for family - 16 byte total header
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

%% @doc Encode a slurm_header record into 16-byte binary (with AF_UNSPEC)
%%
%% Takes a #slurm_header{} record and produces a big-endian binary.
%%
%% IMPORTANT: Empirical testing with SLURM 22.05.9 confirms that srun
%% uses 16-byte headers WITHOUT msg_index, even though version is 0x2600.
%%
%% Wire format (16 bytes with AF_UNSPEC, no msg_index):
%%   version(2) + flags(2) + msg_type(2) + body_length(4) +
%%   forward_cnt(2) + ret_cnt(2) + orig_addr(2) = 16 bytes
%%
%% orig_addr format: AF_UNSPEC (family=0) - minimal 2-byte format
-spec encode_header(#slurm_header{}) -> {ok, binary()} | {error, term()}.
encode_header(#slurm_header{
    version = Version,
    flags = Flags,
    msg_index = _MsgIndex,
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
    %% Use 16-byte header WITHOUT msg_index (confirmed with SLURM 22.05.9)
    OrigAddr = <<0:16/big>>,    % AF_UNSPEC = 0
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
