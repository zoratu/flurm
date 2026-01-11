#!/usr/bin/env escript
%%! -pa _build/default/lib/flurm_protocol/ebin

%%%-------------------------------------------------------------------
%%% @doc Test SLURM Server
%%%
%%% A simple server that responds to SLURM protocol messages.
%%% Used for testing compatibility with real SLURM clients.
%%% @end
%%%-------------------------------------------------------------------

-record(slurm_header, {
    version = 0,
    flags = 0,
    msg_index = 0,
    msg_type = 0,
    body_length = 0
}).

-define(REQUEST_JOB_INFO, 2003).
-define(RESPONSE_JOB_INFO, 2004).
-define(REQUEST_NODE_INFO, 2007).
-define(RESPONSE_NODE_INFO, 2008).
-define(REQUEST_PARTITION_INFO, 2009).
-define(RESPONSE_PARTITION_INFO, 2010).
-define(REQUEST_PING, 1008).
-define(REQUEST_SUBMIT_BATCH_JOB, 4003).
-define(RESPONSE_SUBMIT_BATCH_JOB, 4004).
-define(REQUEST_CANCEL_JOB, 4006).
-define(REQUEST_SIGNAL_JOB, 5032).
-define(RESPONSE_SLURM_RC, 8001).

main([]) ->
    main(["16817"]);
main([PortStr]) ->
    Port = list_to_integer(PortStr),
    io:format("FLURM Test Server~n"),
    io:format("=================~n"),
    io:format("Listening on port ~p~n~n", [Port]),

    case gen_tcp:listen(Port, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]) of
        {ok, ListenSocket} ->
            accept_loop(ListenSocket);
        {error, Reason} ->
            io:format("Error: ~p~n", [Reason]),
            halt(1)
    end.

accept_loop(ListenSocket) ->
    case gen_tcp:accept(ListenSocket) of
        {ok, Socket} ->
            {ok, {Addr, Port}} = inet:peername(Socket),
            io:format("~n=== Connection from ~s:~p ===~n", [inet:ntoa(Addr), Port]),
            spawn(fun() -> handle_connection(Socket) end),
            accept_loop(ListenSocket);
        {error, Reason} ->
            io:format("Accept error: ~p~n", [Reason]),
            accept_loop(ListenSocket)
    end.

handle_connection(Socket) ->
    handle_connection(Socket, <<>>).

handle_connection(Socket, Buffer) ->
    case gen_tcp:recv(Socket, 0, 30000) of
        {ok, Data} ->
            NewBuffer = <<Buffer/binary, Data/binary>>,
            case process_message(NewBuffer, Socket) of
                {ok, Rest} ->
                    handle_connection(Socket, Rest);
                {error, _Reason} ->
                    gen_tcp:close(Socket)
            end;
        {error, closed} ->
            io:format("Connection closed~n"),
            ok;
        {error, timeout} ->
            io:format("Timeout~n"),
            gen_tcp:close(Socket)
    end.

process_message(Buffer, Socket) when byte_size(Buffer) < 4 ->
    {ok, Buffer};
process_message(<<Length:32/big, Rest/binary>> = Buffer, Socket) when byte_size(Rest) < Length ->
    {ok, Buffer};
process_message(<<Length:32/big, Rest/binary>>, Socket) ->
    <<MsgData:Length/binary, Remaining/binary>> = Rest,

    %% Parse header (12 bytes)
    <<Version:16/big, Flags:16/big, MsgIndex:16/big, MsgType:16/big,
      BodyLen:32/big, BodyAndExtra/binary>> = MsgData,

    %% Extract body and extra data (auth/forward info)
    <<_Body:BodyLen/binary, ExtraData/binary>> = BodyAndExtra,

    Header = #slurm_header{
        version = Version,
        flags = Flags,
        msg_index = MsgIndex,
        msg_type = MsgType,
        body_length = BodyLen
    },

    io:format("Received: ~s (type ~p, body ~p bytes, extra ~p bytes)~n",
              [msg_type_name(MsgType), MsgType, BodyLen, byte_size(ExtraData)]),
    io:format("  Extra data hex: ~s~n", [hex_encode(ExtraData)]),

    %% Handle the message
    Response = handle_message(Header, BodyAndExtra),

    %% Send response - batch jobs use different extra data format
    case Response of
        {reply, RespType, RespBody} ->
            send_response(Socket, Version, MsgIndex, RespType, RespBody, ExtraData);
        {reply_batch, RespType, RespBody, JobId, _ClientHostname} ->
            %% Try using same extra format as squeue (which works)
            io:format("  <- Using squeue-style response format~n"),
            send_response(Socket, Version, MsgIndex, RespType, RespBody, ExtraData);
        noreply ->
            ok
    end,

    process_message(Remaining, Socket).

handle_message(#slurm_header{msg_type = ?REQUEST_JOB_INFO}, _Body) ->
    io:format("  -> Responding with empty job list~n"),
    %% RESPONSE_JOB_INFO format: last_update (8 bytes) + job_count (4 bytes) + jobs
    Now = erlang:system_time(second),
    RespBody = <<Now:64/big, 0:32/big>>,  % 0 jobs
    {reply, ?RESPONSE_JOB_INFO, RespBody};

handle_message(#slurm_header{msg_type = ?REQUEST_NODE_INFO}, _Body) ->
    io:format("  -> Responding with empty node list~n"),
    Now = erlang:system_time(second),
    RespBody = <<Now:64/big, 0:32/big>>,  % 0 nodes
    {reply, ?RESPONSE_NODE_INFO, RespBody};

handle_message(#slurm_header{msg_type = ?REQUEST_PARTITION_INFO}, _Body) ->
    io:format("  -> Responding with empty partition list~n"),
    Now = erlang:system_time(second),
    RespBody = <<Now:64/big, 0:32/big>>,  % 0 partitions
    {reply, ?RESPONSE_PARTITION_INFO, RespBody};

handle_message(#slurm_header{msg_type = ?REQUEST_PING}, _Body) ->
    io:format("  -> Responding with OK~n"),
    {reply, ?RESPONSE_SLURM_RC, <<0:32/big-signed>>};

handle_message(#slurm_header{msg_type = ?REQUEST_CANCEL_JOB}, _Body) ->
    io:format("  -> Cancel job request, responding with OK~n"),
    {reply, ?RESPONSE_SLURM_RC, <<0:32/big-signed>>};

handle_message(#slurm_header{msg_type = ?REQUEST_SIGNAL_JOB}, _Body) ->
    io:format("  -> Signal job request, responding with OK~n"),
    {reply, ?RESPONSE_SLURM_RC, <<0:32/big-signed>>};

handle_message(#slurm_header{msg_type = ?REQUEST_SUBMIT_BATCH_JOB}, Body) ->
    %% Generate a fake job ID
    JobId = (erlang:unique_integer([positive]) rem 200) + 1,
    io:format("  -> Submitting batch job, assigned job_id ~p~n", [JobId]),

    %% Try to extract hostname from request body (around offset 25-26)
    ClientHostname = extract_hostname(Body),
    io:format("  -> Client hostname: ~p~n", [ClientHostname]),

    %% Based on captured traffic from real slurmctld:
    %% Body is 16 bytes: 00 00 00 00 00 00 00 00 00 00 00 00 00 64 00 00
    RespBody = <<0:96, 16#0064:16/big, 0:16>>,
    {reply_batch, ?RESPONSE_SUBMIT_BATCH_JOB, RespBody, JobId, ClientHostname};

handle_message(#slurm_header{msg_type = MsgType}, _Body) ->
    io:format("  -> Unknown message type ~p, responding with OK~n", [MsgType]),
    {reply, ?RESPONSE_SLURM_RC, <<0:32/big-signed>>}.

%% Extract hostname from batch job request body
%% Looking at captured traffic, hostname length is at offset 25 (1 byte),
%% followed by the hostname string
extract_hostname(Body) when byte_size(Body) > 40 ->
    case Body of
        <<_:200, Len:8, Rest/binary>> when Len > 0, Len < 64 ->
            case Rest of
                <<Hostname:Len/binary, _/binary>> ->
                    %% Remove null terminator if present
                    case binary:match(Hostname, <<0>>) of
                        {Pos, 1} -> binary:part(Hostname, 0, Pos);
                        nomatch -> Hostname
                    end;
                _ -> <<"flurm">>
            end;
        _ -> <<"flurm">>
    end;
extract_hostname(_) -> <<"flurm">>.

send_response(Socket, ClientVersion, MsgIndex, MsgType, Body, RequestExtraData) ->
    %% Build header with body length (not including extra data)
    BodyLen = byte_size(Body),
    Header = <<ClientVersion:16/big, 0:16/big, MsgIndex:16/big, MsgType:16/big, BodyLen:32/big>>,

    %% Different response types need different extra formats:
    %% - RESPONSE_SLURM_RC: 8 zeros + 00 64 + 11 zeros + hostname + timestamp (39 bytes)
    %% - Other responses: 00 64 + 11 zeros + hostname + padding + timestamp (39 bytes)
    Now = erlang:system_time(second),
    ServerHostname = <<"flurm">>,
    HostnameLen = byte_size(ServerHostname) + 1,  % Including null

    ResponseExtra = case MsgType of
        ?RESPONSE_SLURM_RC ->
            %% RC format: 8 zeros + 00 64 + 11 zeros + hostname_len + hostname + padding + timestamp
            PaddingLen = max(0, 13 - HostnameLen),
            Padding = <<0:(PaddingLen*8)>>,
            <<0:64, 16#0064:16/big, 0:88, HostnameLen:8, ServerHostname/binary, 0:8,
              Padding/binary, Now:32/big>>;
        _ ->
            %% Standard format: 00 64 + 11 zeros + hostname_len + hostname + padding + timestamp
            PaddingLen = max(0, 21 - HostnameLen),
            Padding = <<0:(PaddingLen*8)>>,
            <<16#0064:16/big, 0:88, HostnameLen:8, ServerHostname/binary, 0:8,
              Padding/binary, Now:32/big>>
    end,
    _ = RequestExtraData, % suppress unused warning

    %% Build full message: header + body + extra data
    Msg = <<Header/binary, Body/binary, ResponseExtra/binary>>,
    Length = byte_size(Msg),
    FullMsg = <<Length:32/big, Msg/binary>>,

    io:format("  <- Sending ~s (~p bytes + ~p extra)~n",
              [msg_type_name(MsgType), BodyLen, byte_size(ResponseExtra)]),
    io:format("  <- Response extra hex: ~s~n", [hex_encode(ResponseExtra)]),
    gen_tcp:send(Socket, FullMsg).

send_batch_response(Socket, ClientVersion, MsgIndex, MsgType, Body, JobId, ClientHostname) ->
    %% Build header with body length
    BodyLen = byte_size(Body),
    Header = <<ClientVersion:16/big, 0:16/big, MsgIndex:16/big, MsgType:16/big, BodyLen:32/big>>,

    %% Real slurmctld batch response extra data format (39 bytes total):
    %% 00 00 00 00 00 00 00 00 00 0D [hostname 13 bytes] 00 00 00 00 02 FF FF FF FE 00 00 08 14 00 00 00
    %%
    %% Breakdown: 8 + 2 + hostname_len + padding + 1 + 4 + 7 = 39

    HostnameWithNull = <<ClientHostname/binary, 0>>,
    HWNLen = byte_size(HostnameWithNull),

    %% Formula: 39 = 8 + 2 + HWNLen + padding + 1 + 4 + 7
    %% padding = 39 - 22 - HWNLen = 17 - HWNLen
    PaddingLen = max(0, 17 - HWNLen),

    io:format("  <- HWNLen=~p, PaddingLen=~p, JobId=~p~n", [HWNLen, PaddingLen, JobId]),

    %% Build extra data piece by piece for debugging
    Prefix = <<0:64, HWNLen:16/big>>,
    Padding = <<0:(PaddingLen * 8)>>,
    JobIdByte = <<(JobId band 16#FF):8>>,
    StepId = <<(-2):32/big-signed>>,
    Trailing = <<0:16, 16#0814:16/big, 0:24>>,

    io:format("  <- Prefix=~p bytes, Hostname=~p bytes, Padding=~p bytes~n",
              [byte_size(Prefix), byte_size(HostnameWithNull), byte_size(Padding)]),
    io:format("  <- JobIdByte=~p bytes, StepId=~p bytes, Trailing=~p bytes~n",
              [byte_size(JobIdByte), byte_size(StepId), byte_size(Trailing)]),

    BatchExtra = <<Prefix/binary, HostnameWithNull/binary, Padding/binary,
                   JobIdByte/binary, StepId/binary, Trailing/binary>>,

    %% Build full message
    Msg = <<Header/binary, Body/binary, BatchExtra/binary>>,
    Length = byte_size(Msg),
    FullMsg = <<Length:32/big, Msg/binary>>,

    io:format("  <- Sending ~s (~p bytes + ~p extra)~n",
              [msg_type_name(MsgType), BodyLen, byte_size(BatchExtra)]),
    io:format("  <- Batch extra hex: ~s~n", [hex_encode(BatchExtra)]),
    gen_tcp:send(Socket, FullMsg).

msg_type_name(?REQUEST_JOB_INFO) -> "REQUEST_JOB_INFO";
msg_type_name(?RESPONSE_JOB_INFO) -> "RESPONSE_JOB_INFO";
msg_type_name(?REQUEST_NODE_INFO) -> "REQUEST_NODE_INFO";
msg_type_name(?RESPONSE_NODE_INFO) -> "RESPONSE_NODE_INFO";
msg_type_name(?REQUEST_PARTITION_INFO) -> "REQUEST_PARTITION_INFO";
msg_type_name(?RESPONSE_PARTITION_INFO) -> "RESPONSE_PARTITION_INFO";
msg_type_name(?REQUEST_PING) -> "REQUEST_PING";
msg_type_name(?REQUEST_SUBMIT_BATCH_JOB) -> "REQUEST_SUBMIT_BATCH_JOB";
msg_type_name(?RESPONSE_SUBMIT_BATCH_JOB) -> "RESPONSE_SUBMIT_BATCH_JOB";
msg_type_name(?RESPONSE_SLURM_RC) -> "RESPONSE_SLURM_RC";
msg_type_name(N) -> io_lib:format("MSG_~p", [N]).

hex_encode(Binary) ->
    [[io_lib:format("~2.16.0B", [Byte]) || Byte <- binary_to_list(Binary)]].
