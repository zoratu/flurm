%%%-------------------------------------------------------------------
%%% @doc FLURM Protocol Authentication/Credential Module
%%%
%%% This module handles the "extra data" section that follows the body
%%% in SLURM wire format. This section contains authentication credentials
%%% and is required for real SLURM client compatibility.
%%%
%%% Wire format:
%%%   <<Length:32/big, Header:12/binary, Body/binary, ExtraData:39/binary>>
%%%
%%% Extra Data Format (39 bytes):
%%%
%%% For RESPONSE_SLURM_RC (8001):
%%%   <<0:64,                    % 8 bytes of zeros
%%%     16#0064:16/big,          % Response type indicator
%%%     0:88,                    % 11 bytes of zeros
%%%     HostnameLen:8,           % Hostname length (including null)
%%%     Hostname/binary,         % Hostname string
%%%     0:8,                     % Null terminator
%%%     Padding/binary,          % Variable padding
%%%     Timestamp:32/big>>       % 4-byte timestamp
%%%
%%% For other response types:
%%%   <<16#0064:16/big,          % Response type indicator
%%%     0:88,                    % 11 bytes of zeros
%%%     HostnameLen:8,           % Hostname length (including null)
%%%     Hostname/binary,         % Hostname string
%%%     0:8,                     % Null terminator
%%%     Padding/binary,          % Variable padding
%%%     Timestamp:32/big>>       % 4-byte timestamp
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_protocol_auth).

-export([
    %% Extra data encoding
    encode_extra/1,
    encode_extra/2,

    %% Extra data decoding
    decode_extra/1,

    %% Utilities
    extra_size/0,
    default_hostname/0
]).

-include("flurm_protocol.hrl").

%% Standard extra data size
-define(EXTRA_DATA_SIZE, 39).

%% Response type indicator (appears in extra data)
-define(RESPONSE_TYPE_INDICATOR, 16#0064).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Get the standard extra data size in bytes.
-spec extra_size() -> non_neg_integer().
extra_size() ->
    ?EXTRA_DATA_SIZE.

%% @doc Get the default hostname used in responses.
-spec default_hostname() -> binary().
default_hostname() ->
    case inet:gethostname() of
        {ok, Name} -> list_to_binary(Name);
        {error, _} -> <<"flurm">>
    end.

%% @doc Encode extra data for a response message.
%% Uses default hostname and current timestamp.
-spec encode_extra(non_neg_integer()) -> binary().
encode_extra(MsgType) ->
    encode_extra(MsgType, default_hostname()).

%% @doc Encode extra data for a response message with specified hostname.
-spec encode_extra(non_neg_integer(), binary()) -> binary().
encode_extra(MsgType, Hostname) ->
    Now = erlang:system_time(second),
    HostnameLen = byte_size(Hostname) + 1,  % Including null terminator

    case MsgType of
        ?RESPONSE_SLURM_RC ->
            %% RC format: 8 zeros + 00 64 + 11 zeros + hostname + padding + timestamp
            %% Total: 8 + 2 + 11 + 1 + HostnameLen + Padding + 4 = 39
            %% Padding = 39 - 26 - HostnameLen = 13 - HostnameLen
            PaddingLen = max(0, 13 - HostnameLen),
            Padding = <<0:(PaddingLen * 8)>>,
            <<0:64,                           % 8 zeros at start
              ?RESPONSE_TYPE_INDICATOR:16/big, % 00 64
              0:88,                           % 11 zeros
              HostnameLen:8,                  % hostname length
              Hostname/binary,                % hostname
              0:8,                            % null terminator
              Padding/binary,                 % padding
              Now:32/big>>;                   % 4-byte timestamp

        _ ->
            %% Standard format: 00 64 + 11 zeros + hostname + padding + timestamp
            %% Total: 2 + 11 + 1 + HostnameLen + Padding + 4 = 39
            %% Padding = 39 - 18 - HostnameLen = 21 - HostnameLen
            PaddingLen = max(0, 21 - HostnameLen),
            Padding = <<0:(PaddingLen * 8)>>,
            <<?RESPONSE_TYPE_INDICATOR:16/big, % 00 64
              0:88,                           % 11 zeros
              HostnameLen:8,                  % hostname length
              Hostname/binary,                % hostname
              0:8,                            % null terminator
              Padding/binary,                 % padding
              Now:32/big>>                    % 4-byte timestamp
    end.

%% @doc Decode extra data from a received message.
%% Returns a map with parsed fields.
-spec decode_extra(binary()) -> {ok, map()} | {error, term()}.
decode_extra(<<>>) ->
    {ok, #{hostname => <<>>, timestamp => 0}};

decode_extra(Binary) when byte_size(Binary) >= ?EXTRA_DATA_SIZE ->
    %% Try to detect format by checking first bytes
    case Binary of
        %% RC format: starts with 8 zeros, then 00 64
        <<0:64, ?RESPONSE_TYPE_INDICATOR:16/big, 0:88, HostnameLen:8, Rest/binary>> ->
            decode_hostname_section(HostnameLen, Rest, rc_format);

        %% Standard format: starts with 00 64
        <<?RESPONSE_TYPE_INDICATOR:16/big, 0:88, HostnameLen:8, Rest/binary>> ->
            decode_hostname_section(HostnameLen, Rest, standard_format);

        %% Request format: starts with 00 00 then hostname at offset 3
        <<0:24, HostnameLen:8, Rest/binary>> when HostnameLen > 0, HostnameLen < 64 ->
            decode_hostname_section(HostnameLen, Rest, request_format);

        %% Unknown format - try to extract what we can
        _ ->
            {ok, #{hostname => <<>>, timestamp => 0, raw => Binary}}
    end;

decode_extra(Binary) when byte_size(Binary) < ?EXTRA_DATA_SIZE ->
    {error, {extra_data_too_short, byte_size(Binary)}}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% Decode hostname section from extra data
decode_hostname_section(HostnameLen, Binary, Format) ->
    %% HostnameLen includes null terminator
    ActualLen = HostnameLen - 1,
    case Binary of
        <<Hostname:ActualLen/binary, 0:8, Rest/binary>> ->
            %% Extract timestamp from last 4 bytes
            Timestamp = extract_timestamp(Rest),
            {ok, #{
                hostname => Hostname,
                timestamp => Timestamp,
                format => Format
            }};
        <<Hostname:ActualLen/binary, Rest/binary>> when ActualLen > 0 ->
            Timestamp = extract_timestamp(Rest),
            {ok, #{
                hostname => Hostname,
                timestamp => Timestamp,
                format => Format
            }};
        _ ->
            {ok, #{hostname => <<>>, timestamp => 0, format => Format}}
    end.

%% Extract timestamp from the end of extra data
extract_timestamp(Binary) when byte_size(Binary) >= 4 ->
    %% Timestamp is in last 4 bytes
    Offset = byte_size(Binary) - 4,
    <<_:Offset/binary, Timestamp:32/big>> = Binary,
    Timestamp;
extract_timestamp(_) ->
    0.
