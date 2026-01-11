#!/usr/bin/env escript
%%! -pa _build/default/lib/flurm_protocol/ebin

%%%-------------------------------------------------------------------
%%% @doc SLURM Protocol Traffic Analyzer
%%%
%%% Captures TCP traffic on port 6817 (or specified port) and decodes
%%% SLURM protocol messages for analysis.
%%%
%%% Usage:
%%%   ./capture_slurm_protocol.escript [port]
%%%
%%% This tool is useful for:
%%% - Understanding real SLURM client/server communication
%%% - Capturing message formats for protocol implementation
%%% - Debugging protocol compatibility issues
%%%
%%% Note: Requires root/sudo for packet capture on port 6817.
%%% Alternatively, run a local test server on a high port.
%%% @end
%%%-------------------------------------------------------------------

main([]) ->
    main(["6817"]);
main([PortStr]) ->
    Port = list_to_integer(PortStr),
    io:format("SLURM Protocol Traffic Analyzer~n"),
    io:format("================================~n"),
    io:format("Listening on port ~p~n~n", [Port]),

    case gen_tcp:listen(Port, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]) of
        {ok, ListenSocket} ->
            io:format("Server started. Waiting for connections...~n~n"),
            accept_loop(ListenSocket);
        {error, eacces} ->
            io:format("Error: Permission denied. Try running with sudo or use a port > 1024~n"),
            halt(1);
        {error, eaddrinuse} ->
            io:format("Error: Port ~p already in use~n", [Port]),
            halt(1);
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
    handle_connection(Socket, <<>>, 1).

handle_connection(Socket, Buffer, MsgNum) ->
    case gen_tcp:recv(Socket, 0, 30000) of
        {ok, Data} ->
            NewBuffer = <<Buffer/binary, Data/binary>>,
            {Rest, NextMsgNum} = process_buffer(NewBuffer, MsgNum),
            handle_connection(Socket, Rest, NextMsgNum);
        {error, closed} ->
            io:format("~n=== Connection closed ===~n~n"),
            ok;
        {error, timeout} ->
            io:format("~n=== Connection timeout ===~n~n"),
            gen_tcp:close(Socket);
        {error, Reason} ->
            io:format("~nError: ~p~n", [Reason]),
            gen_tcp:close(Socket)
    end.

process_buffer(Buffer, MsgNum) when byte_size(Buffer) < 4 ->
    {Buffer, MsgNum};
process_buffer(<<Length:32/big, Rest/binary>> = Buffer, MsgNum) when byte_size(Rest) < Length ->
    {Buffer, MsgNum};
process_buffer(<<Length:32/big, Rest/binary>>, MsgNum) ->
    <<MsgData:Length/binary, Remaining/binary>> = Rest,
    analyze_message(MsgNum, Length, MsgData),
    process_buffer(Remaining, MsgNum + 1).

analyze_message(MsgNum, Length, MsgData) ->
    io:format("~n--- Message #~p (Length: ~p bytes) ---~n", [MsgNum, Length]),

    %% Show raw header bytes first (first 16 bytes)
    io:format("Raw header bytes:~n"),
    show_hex_dump(binary:part(MsgData, 0, min(16, byte_size(MsgData))), 16),

    %% Try to parse the header (12 bytes)
    case MsgData of
        <<Version:16/big, Flags:16/big, MsgIndex:16/big, MsgType:16/big,
          BodyLen:32/big, Body/binary>> ->
            io:format("Header:~n"),
            io:format("  Version:     ~p (~s)~n", [Version, format_version(Version)]),
            io:format("  Flags:       0x~4.16.0B~n", [Flags]),
            io:format("  Msg Index:   ~p~n", [MsgIndex]),
            io:format("  Msg Type:    ~p (~s)~n", [MsgType, message_type_name(MsgType)]),
            io:format("  Body Length: ~p~n", [BodyLen]),
            io:format("Body: ~p bytes~n", [byte_size(Body)]),

            %% Show hex dump of body (first 128 bytes)
            show_hex_dump(Body, 128);
        _ ->
            io:format("Unable to parse header. Raw hex:~n"),
            show_hex_dump(MsgData, 256)
    end.

format_version(Version) ->
    Major = Version div 1000,
    Minor = (Version rem 1000) div 10,
    io_lib:format("~p.~2.10.0B", [Major, Minor]).

message_type_name(1001) -> "REQUEST_NODE_REGISTRATION_STATUS";
message_type_name(1002) -> "MESSAGE_NODE_REGISTRATION_STATUS";
message_type_name(1003) -> "REQUEST_RECONFIGURE";
message_type_name(1008) -> "REQUEST_PING";
message_type_name(2001) -> "REQUEST_BUILD_INFO";
message_type_name(2002) -> "RESPONSE_BUILD_INFO";
message_type_name(2003) -> "REQUEST_JOB_INFO";
message_type_name(2004) -> "RESPONSE_JOB_INFO";
message_type_name(2007) -> "REQUEST_NODE_INFO";
message_type_name(2008) -> "RESPONSE_NODE_INFO";
message_type_name(2009) -> "REQUEST_PARTITION_INFO";
message_type_name(2010) -> "RESPONSE_PARTITION_INFO";
message_type_name(4001) -> "REQUEST_RESOURCE_ALLOCATION";
message_type_name(4002) -> "RESPONSE_RESOURCE_ALLOCATION";
message_type_name(4003) -> "REQUEST_SUBMIT_BATCH_JOB";
message_type_name(4004) -> "RESPONSE_SUBMIT_BATCH_JOB";
message_type_name(4006) -> "REQUEST_CANCEL_JOB";
message_type_name(5001) -> "REQUEST_JOB_STEP_CREATE";
message_type_name(5002) -> "RESPONSE_JOB_STEP_CREATE";
message_type_name(8001) -> "RESPONSE_SLURM_RC";
message_type_name(8002) -> "RESPONSE_SLURM_RC_MSG";
message_type_name(_) -> "UNKNOWN".

show_hex_dump(Binary, MaxBytes) ->
    Bytes = binary_to_list(binary:part(Binary, 0, min(byte_size(Binary), MaxBytes))),
    show_hex_lines(Bytes, 0).

show_hex_lines([], _Offset) ->
    ok;
show_hex_lines(Bytes, Offset) ->
    {Line, Rest} = lists:split(min(16, length(Bytes)), Bytes),
    HexStr = string:join([io_lib:format("~2.16.0B", [B]) || B <- Line], " "),
    AsciiStr = [printable_char(B) || B <- Line],
    io:format("  ~4.16.0B: ~-48s ~s~n", [Offset, HexStr, AsciiStr]),
    show_hex_lines(Rest, Offset + 16).

printable_char(B) when B >= 32, B < 127 -> B;
printable_char(_) -> $..
