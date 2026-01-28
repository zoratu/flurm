%%%-------------------------------------------------------------------
%%% @doc FLURM I/O Protocol Encoder/Decoder
%%%
%%% Handles the SLURM I/O forwarding protocol used between slurmstepd
%%% and srun for stdout/stderr streaming.
%%%
%%% Wire formats:
%%%   io_init_msg: <<Version:16, NodeId:32, StdoutObjs:32, StderrObjs:32,
%%%                  IoKeyLen:32, IoKey/binary>>
%%%   io_hdr_t:    <<Type:16, Gtaskid:16, Ltaskid:16, Length:32>> (10 bytes)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_io_protocol).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").

-export([
    encode_io_init_msg/4,
    encode_io_init_msg/5,
    encode_io_init_msg_fixed_key/4,
    encode_io_hdr/4,
    encode_stdout/3,
    encode_stderr/3,
    encode_eof/2,
    decode_io_hdr/1
]).

%%====================================================================
%% API Functions
%%====================================================================

%% @doc Encode an I/O initialization message.
%% Sent immediately after connecting to srun's I/O port.
%% Uses empty io_key (will need credential extraction later).
%%
%% Two formats exist:
%% 1. IO_PROTOCOL_VERSION (older, for protocol < 21.08):
%%    version:16, nodeid:32, stdout_objs:32, stderr_objs:32, io_key:32_bytes_fixed
%%
%% 2. SLURM_PROTOCOL_VERSION (newer, for protocol >= 21.08):
%%    length:32, version:16, nodeid:32, stdout_objs:32, stderr_objs:32, io_key_len:32, io_key:variable
%%
%% We try the newer format first. If that fails, we can try the older format.
-spec encode_io_init_msg(non_neg_integer(), non_neg_integer(),
                         non_neg_integer(), binary()) -> binary().
encode_io_init_msg(NodeId, StdoutObjs, StderrObjs, IoKey) ->
    encode_io_init_msg(?IO_PROTOCOL_VERSION, NodeId, StdoutObjs, StderrObjs, IoKey).

%% @doc Encode an I/O initialization message with explicit version.
%% SLURM io_init_msg format (WITH length prefix as per io_init_msg_pack):
%%   length:32, version:16, nodeid:32, stdout_objs:32, stderr_objs:32, io_key_len:32, io_key:variable
%% Length = total bytes after the length field (version through io_key)
-spec encode_io_init_msg(non_neg_integer(), non_neg_integer(),
                         non_neg_integer(), non_neg_integer(), binary()) -> binary().
encode_io_init_msg(Version, NodeId, StdoutObjs, StderrObjs, IoKey) ->
    IoKeyLen = byte_size(IoKey),
    %% Payload after length prefix: version(2) + nodeid(4) + stdout(4) + stderr(4) + keylen(4) + key
    PayloadLen = 2 + 4 + 4 + 4 + 4 + IoKeyLen,
    <<PayloadLen:32/big,
      Version:16/big,
      NodeId:32/big,
      StdoutObjs:32/big,
      StderrObjs:32/big,
      IoKeyLen:32/big,
      IoKey/binary>>.

%% @doc Encode an I/O initialization message using the older fixed-key format.
%% Uses IO_PROTOCOL_VERSION format (protocol < 21.08)
-spec encode_io_init_msg_fixed_key(non_neg_integer(), non_neg_integer(),
                                    non_neg_integer(), non_neg_integer()) -> binary().
encode_io_init_msg_fixed_key(Version, NodeId, StdoutObjs, StderrObjs) ->
    %% Fixed 32-byte io_key (SLURM_IO_KEY_SIZE = 32 in older versions)
    IoKey = <<0:256>>,  % 32 bytes of zeros
    <<Version:16/big,
      NodeId:32/big,
      StdoutObjs:32/big,
      StderrObjs:32/big,
      IoKey/binary>>.

%% @doc Encode an I/O header.
%% Type: SLURM_IO_STDIN(0), SLURM_IO_STDOUT(1), SLURM_IO_STDERR(2), etc.
%% Returns 10-byte header binary.
-spec encode_io_hdr(non_neg_integer(), non_neg_integer(),
                    non_neg_integer(), non_neg_integer()) -> binary().
encode_io_hdr(Type, Gtaskid, Ltaskid, Length) ->
    <<Type:16/big, Gtaskid:16/big, Ltaskid:16/big, Length:32/big>>.

%% @doc Encode stdout data with header.
%% Returns header + data ready to send.
-spec encode_stdout(non_neg_integer(), non_neg_integer(), binary()) -> binary().
encode_stdout(Gtaskid, Ltaskid, Data) ->
    Length = byte_size(Data),
    Header = encode_io_hdr(?SLURM_IO_STDOUT, Gtaskid, Ltaskid, Length),
    <<Header/binary, Data/binary>>.

%% @doc Encode stderr data with header.
%% Returns header + data ready to send.
-spec encode_stderr(non_neg_integer(), non_neg_integer(), binary()) -> binary().
encode_stderr(Gtaskid, Ltaskid, Data) ->
    Length = byte_size(Data),
    Header = encode_io_hdr(?SLURM_IO_STDERR, Gtaskid, Ltaskid, Length),
    <<Header/binary, Data/binary>>.

%% @doc Encode an EOF marker (zero-length message).
%% Signals end of output stream to srun.
-spec encode_eof(non_neg_integer(), non_neg_integer()) -> binary().
encode_eof(Gtaskid, Ltaskid) ->
    %% EOF is signaled by a zero-length stdout message
    encode_io_hdr(?SLURM_IO_STDOUT, Gtaskid, Ltaskid, 0).

%% @doc Decode an I/O header from binary.
%% Returns {Type, Gtaskid, Ltaskid, Length} or error.
-spec decode_io_hdr(binary()) -> {ok, non_neg_integer(), non_neg_integer(),
                                  non_neg_integer(), non_neg_integer()} |
                                 {error, term()}.
decode_io_hdr(<<Type:16/big, Gtaskid:16/big, Ltaskid:16/big, Length:32/big>>) ->
    {ok, Type, Gtaskid, Ltaskid, Length};
decode_io_hdr(Bin) when byte_size(Bin) < ?SLURM_IO_HDR_SIZE ->
    {error, {incomplete, need, ?SLURM_IO_HDR_SIZE - byte_size(Bin)}};
decode_io_hdr(_) ->
    {error, invalid_io_header}.
