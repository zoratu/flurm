%%%-------------------------------------------------------------------
%%% @doc FLURM Protocol Pack - Low-level Packing Utilities
%%%
%%% Provides utilities for packing and unpacking primitive data types
%%% used in the SLURM binary protocol. All integers use big-endian
%%% (network byte order) encoding.
%%%
%%% String encoding: 4-byte length prefix followed by UTF-8 data.
%%% List encoding: 4-byte count followed by packed elements.
%%% Time encoding: 8-byte Unix timestamp (seconds since epoch).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_pack).

-export([
    %% String packing
    pack_string/1,
    unpack_string/1,

    %% List packing
    pack_list/2,
    unpack_list/2,

    %% Time packing
    pack_time/1,
    unpack_time/1,

    %% Integer packing
    pack_uint8/1,
    unpack_uint8/1,
    pack_uint16/1,
    unpack_uint16/1,
    pack_uint32/1,
    unpack_uint32/1,
    pack_uint64/1,
    unpack_uint64/1,
    pack_int32/1,
    unpack_int32/1,

    %% Boolean packing
    pack_bool/1,
    unpack_bool/1,

    %% Float packing
    pack_double/1,
    unpack_double/1,

    %% String array packing
    pack_string_array/1,
    unpack_string_array/1
]).

-include("flurm_protocol.hrl").

%%%===================================================================
%%% String Packing
%%%===================================================================

%% @doc Pack a string as length-prefixed binary with null terminator.
%%
%% Format: <<Length:32/big, Data/binary, 0:8>>
%% SLURM uses strlen(str)+1 which INCLUDES the null terminator in the length.
%% An empty binary, undefined, or null is encoded as <<0:32/big>> (zero length).
-spec pack_string(binary() | undefined | null) -> binary().
pack_string(undefined) ->
    <<0:32/big>>;  % NULL string - length 0, no data (NULL pointer in C)
pack_string(null) ->
    <<0:32/big>>;  % NULL string - length 0, no data (NULL pointer in C)
pack_string(<<>>) ->
    %% Empty string in SLURM: strlen("") + 1 = 1, just the null terminator
    <<1:32/big, 0:8>>;
pack_string(Binary) when is_binary(Binary) ->
    %% SLURM includes null terminator in length (strlen+1)
    Length = byte_size(Binary) + 1,
    <<Length:32/big, Binary/binary, 0:8>>;
pack_string(List) when is_list(List) ->
    pack_string(list_to_binary(List)).

%% @doc Unpack a length-prefixed string from binary.
%%
%% Returns {ok, String, Rest} where String is the unpacked binary
%% and Rest is the remaining data.
%% Returns {ok, undefined, Rest} for NO_VAL encoded strings or zero-length strings.
%% Note: SLURM includes null terminator in length, so we strip it during decode.
-spec unpack_string(binary()) -> {ok, binary() | undefined, binary()} | {error, term()}.
unpack_string(<<?SLURM_NO_VAL:32/big, Rest/binary>>) ->
    {ok, undefined, Rest};
unpack_string(<<0:32/big, Rest/binary>>) ->
    %% Zero length means undefined/empty - treat as undefined for roundtrip consistency
    {ok, undefined, Rest};
unpack_string(<<Length:32/big, Data:Length/binary, Rest/binary>>) when Length > 0 ->
    %% Strip the null terminator that SLURM includes in the length
    %% The null byte is the last byte of Data
    DataLen = Length - 1,
    <<StrippedData:DataLen/binary, 0:8>> = Data,
    {ok, StrippedData, Rest};
unpack_string(<<Length:32/big, _/binary>>) ->
    {error, {insufficient_string_data, Length}};
unpack_string(Binary) when byte_size(Binary) < 4 ->
    {error, {incomplete_string_length, byte_size(Binary)}};
unpack_string(_) ->
    {error, invalid_string_data}.

%%%===================================================================
%%% List Packing
%%%===================================================================

%% @doc Pack a list using a packing function for each element.
%%
%% Format: <<Count:32/big, Element1/binary, Element2/binary, ...>>
-spec pack_list([term()], fun((term()) -> binary())) -> binary().
pack_list(List, PackFun) when is_list(List), is_function(PackFun, 1) ->
    Count = length(List),
    Elements = [PackFun(Elem) || Elem <- List],
    iolist_to_binary([<<Count:32/big>> | Elements]).

%% @doc Unpack a list using an unpacking function for each element.
%%
%% Returns {ok, List, Rest} on success.
-spec unpack_list(binary(), fun((binary()) -> {ok, term(), binary()} | {error, term()})) ->
    {ok, [term()], binary()} | {error, term()}.
unpack_list(<<Count:32/big, Rest/binary>>, UnpackFun) when is_function(UnpackFun, 1) ->
    unpack_list_elements(Count, Rest, UnpackFun, []);
unpack_list(Binary, _UnpackFun) when byte_size(Binary) < 4 ->
    {error, {incomplete_list_count, byte_size(Binary)}};
unpack_list(_, _) ->
    {error, invalid_list_data}.

unpack_list_elements(0, Rest, _UnpackFun, Acc) ->
    {ok, lists:reverse(Acc), Rest};
unpack_list_elements(Count, Binary, UnpackFun, Acc) when Count > 0 ->
    case UnpackFun(Binary) of
        {ok, Element, Rest} ->
            unpack_list_elements(Count - 1, Rest, UnpackFun, [Element | Acc]);
        {error, _} = Error ->
            Error
    end.

%%%===================================================================
%%% Time Packing
%%%===================================================================

%% @doc Pack a Unix timestamp as 64-bit integer.
%%
%% Accepts:
%% - Non-negative integer (Unix timestamp in seconds)
%% - {MegaSec, Sec, MicroSec} tuple (erlang:timestamp/0 format)
%% - undefined -> NO_VAL64
-spec pack_time(non_neg_integer() | {non_neg_integer(), non_neg_integer(), non_neg_integer()} | undefined) -> binary().
pack_time(undefined) ->
    <<?SLURM_NO_VAL64:64/big>>;
pack_time(0) ->
    <<0:64/big>>;
pack_time({MegaSec, Sec, _MicroSec}) when is_integer(MegaSec), is_integer(Sec) ->
    Timestamp = MegaSec * 1000000 + Sec,
    <<Timestamp:64/big>>;
pack_time(Timestamp) when is_integer(Timestamp), Timestamp >= 0 ->
    <<Timestamp:64/big>>.

%% @doc Unpack a 64-bit Unix timestamp.
%%
%% Returns {ok, Timestamp, Rest} where Timestamp is an integer
%% or undefined for NO_VAL64.
-spec unpack_time(binary()) -> {ok, non_neg_integer() | undefined, binary()} | {error, term()}.
unpack_time(<<?SLURM_NO_VAL64:64/big, Rest/binary>>) ->
    {ok, undefined, Rest};
unpack_time(<<Timestamp:64/big, Rest/binary>>) ->
    {ok, Timestamp, Rest};
unpack_time(Binary) when byte_size(Binary) < 8 ->
    {error, {incomplete_timestamp, byte_size(Binary)}};
unpack_time(_) ->
    {error, invalid_timestamp_data}.

%%%===================================================================
%%% Integer Packing
%%%===================================================================

%% @doc Pack an unsigned 8-bit integer.
-spec pack_uint8(non_neg_integer()) -> binary().
pack_uint8(Value) when is_integer(Value), Value >= 0, Value =< 255 ->
    <<Value:8>>.

%% @doc Unpack an unsigned 8-bit integer.
-spec unpack_uint8(binary()) -> {ok, non_neg_integer(), binary()} | {error, term()}.
unpack_uint8(<<Value:8, Rest/binary>>) ->
    {ok, Value, Rest};
unpack_uint8(<<>>) ->
    {error, incomplete_uint8};
unpack_uint8(_) ->
    {error, invalid_uint8_data}.

%% @doc Pack an unsigned 16-bit integer (big-endian).
-spec pack_uint16(non_neg_integer()) -> binary().
pack_uint16(Value) when is_integer(Value), Value >= 0, Value =< 65535 ->
    <<Value:16/big>>.

%% @doc Unpack an unsigned 16-bit integer (big-endian).
-spec unpack_uint16(binary()) -> {ok, non_neg_integer(), binary()} | {error, term()}.
unpack_uint16(<<Value:16/big, Rest/binary>>) ->
    {ok, Value, Rest};
unpack_uint16(Binary) when byte_size(Binary) < 2 ->
    {error, {incomplete_uint16, byte_size(Binary)}};
unpack_uint16(_) ->
    {error, invalid_uint16_data}.

%% @doc Pack an unsigned 32-bit integer (big-endian).
-spec pack_uint32(non_neg_integer() | undefined) -> binary().
pack_uint32(undefined) ->
    <<?SLURM_NO_VAL:32/big>>;
pack_uint32(Value) when is_integer(Value), Value >= 0 ->
    <<Value:32/big>>.

%% @doc Unpack an unsigned 32-bit integer (big-endian).
-spec unpack_uint32(binary()) -> {ok, non_neg_integer() | undefined, binary()} | {error, term()}.
unpack_uint32(<<?SLURM_NO_VAL:32/big, Rest/binary>>) ->
    {ok, undefined, Rest};
unpack_uint32(<<Value:32/big, Rest/binary>>) ->
    {ok, Value, Rest};
unpack_uint32(Binary) when byte_size(Binary) < 4 ->
    {error, {incomplete_uint32, byte_size(Binary)}};
unpack_uint32(_) ->
    {error, invalid_uint32_data}.

%% @doc Pack an unsigned 64-bit integer (big-endian).
-spec pack_uint64(non_neg_integer() | undefined) -> binary().
pack_uint64(undefined) ->
    <<?SLURM_NO_VAL64:64/big>>;
pack_uint64(Value) when is_integer(Value), Value >= 0 ->
    <<Value:64/big>>.

%% @doc Unpack an unsigned 64-bit integer (big-endian).
-spec unpack_uint64(binary()) -> {ok, non_neg_integer() | undefined, binary()} | {error, term()}.
unpack_uint64(<<?SLURM_NO_VAL64:64/big, Rest/binary>>) ->
    {ok, undefined, Rest};
unpack_uint64(<<Value:64/big, Rest/binary>>) ->
    {ok, Value, Rest};
unpack_uint64(Binary) when byte_size(Binary) < 8 ->
    {error, {incomplete_uint64, byte_size(Binary)}};
unpack_uint64(_) ->
    {error, invalid_uint64_data}.

%% @doc Pack a signed 32-bit integer (big-endian).
-spec pack_int32(integer()) -> binary().
pack_int32(Value) when is_integer(Value) ->
    <<Value:32/big-signed>>.

%% @doc Unpack a signed 32-bit integer (big-endian).
-spec unpack_int32(binary()) -> {ok, integer(), binary()} | {error, term()}.
unpack_int32(<<Value:32/big-signed, Rest/binary>>) ->
    {ok, Value, Rest};
unpack_int32(Binary) when byte_size(Binary) < 4 ->
    {error, {incomplete_int32, byte_size(Binary)}};
unpack_int32(_) ->
    {error, invalid_int32_data}.

%%%===================================================================
%%% Boolean Packing
%%%===================================================================

%% @doc Pack a boolean as a single byte.
-spec pack_bool(boolean()) -> binary().
pack_bool(true) -> <<1:8>>;
pack_bool(false) -> <<0:8>>;
pack_bool(1) -> <<1:8>>;
pack_bool(0) -> <<0:8>>.

%% @doc Unpack a boolean from a single byte.
-spec unpack_bool(binary()) -> {ok, boolean(), binary()} | {error, term()}.
unpack_bool(<<0:8, Rest/binary>>) ->
    {ok, false, Rest};
unpack_bool(<<_:8, Rest/binary>>) ->
    {ok, true, Rest};
unpack_bool(<<>>) ->
    {error, incomplete_bool};
unpack_bool(_) ->
    {error, invalid_bool_data}.

%%%===================================================================
%%% Float Packing
%%%===================================================================

%% @doc Pack a double-precision float (64-bit IEEE 754, big-endian).
-spec pack_double(float()) -> binary().
pack_double(Value) when is_float(Value) ->
    <<Value:64/big-float>>;
pack_double(Value) when is_integer(Value) ->
    <<(float(Value)):64/big-float>>.

%% @doc Unpack a double-precision float.
-spec unpack_double(binary()) -> {ok, float(), binary()} | {error, term()}.
unpack_double(<<Value:64/big-float, Rest/binary>>) ->
    {ok, Value, Rest};
unpack_double(Binary) when byte_size(Binary) < 8 ->
    {error, {incomplete_double, byte_size(Binary)}};
unpack_double(_) ->
    {error, invalid_double_data}.

%%%===================================================================
%%% String Array Packing
%%%===================================================================

%% @doc Pack an array of strings.
%%
%% Format: <<Count:32/big, String1, String2, ...>>
%% Each string is length-prefixed.
-spec pack_string_array([binary()]) -> binary().
pack_string_array(Strings) when is_list(Strings) ->
    pack_list(Strings, fun pack_string/1).

%% @doc Unpack an array of strings.
-spec unpack_string_array(binary()) -> {ok, [binary()], binary()} | {error, term()}.
unpack_string_array(Binary) ->
    unpack_list(Binary, fun unpack_string/1).
