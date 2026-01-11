#!/usr/bin/env escript
%%! -pa _build/default/lib/flurm_protocol/ebin

%%%-------------------------------------------------------------------
%%% @doc SLURM Protocol Proxy
%%%
%%% Sits between client and server, logging all traffic in hex.
%%% @end
%%%-------------------------------------------------------------------

main([ListenPortStr, TargetHost, TargetPortStr]) ->
    ListenPort = list_to_integer(ListenPortStr),
    TargetPort = list_to_integer(TargetPortStr),

    io:format("SLURM Protocol Proxy~n"),
    io:format("====================~n"),
    io:format("Listening on port ~p~n", [ListenPort]),
    io:format("Proxying to ~s:~p~n~n", [TargetHost, TargetPort]),

    case gen_tcp:listen(ListenPort, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]) of
        {ok, ListenSocket} ->
            accept_loop(ListenSocket, TargetHost, TargetPort);
        {error, Reason} ->
            io:format("Error: ~p~n", [Reason]),
            halt(1)
    end;
main(_) ->
    io:format("Usage: protocol_proxy.escript <listen_port> <target_host> <target_port>~n"),
    halt(1).

accept_loop(ListenSocket, TargetHost, TargetPort) ->
    case gen_tcp:accept(ListenSocket) of
        {ok, ClientSocket} ->
            io:format("~n=== New connection ===~n"),
            spawn(fun() -> handle_client(ClientSocket, TargetHost, TargetPort) end),
            accept_loop(ListenSocket, TargetHost, TargetPort);
        {error, Reason} ->
            io:format("Accept error: ~p~n", [Reason]),
            accept_loop(ListenSocket, TargetHost, TargetPort)
    end.

handle_client(ClientSocket, TargetHost, TargetPort) ->
    case gen_tcp:connect(TargetHost, TargetPort, [binary, {packet, 0}, {active, false}], 5000) of
        {ok, ServerSocket} ->
            proxy_loop(ClientSocket, ServerSocket);
        {error, Reason} ->
            io:format("Connect error: ~p~n", [Reason]),
            gen_tcp:close(ClientSocket)
    end.

proxy_loop(ClientSocket, ServerSocket) ->
    %% Simple relay: read from client, forward to server, read response, forward back
    case gen_tcp:recv(ClientSocket, 0, 30000) of
        {ok, ClientData} ->
            io:format("~n>>> CLIENT -> SERVER (~p bytes):~n", [byte_size(ClientData)]),
            dump_hex(ClientData),
            analyze_message(ClientData, "Request"),

            gen_tcp:send(ServerSocket, ClientData),

            case gen_tcp:recv(ServerSocket, 0, 30000) of
                {ok, ServerData} ->
                    io:format("~n<<< SERVER -> CLIENT (~p bytes):~n", [byte_size(ServerData)]),
                    dump_hex(ServerData),
                    analyze_message(ServerData, "Response"),

                    gen_tcp:send(ClientSocket, ServerData),
                    proxy_loop(ClientSocket, ServerSocket);
                {error, ServerReason} ->
                    io:format("Server recv error: ~p~n", [ServerReason])
            end;
        {error, closed} ->
            io:format("Client closed~n"),
            gen_tcp:close(ServerSocket);
        {error, Reason} ->
            io:format("Client recv error: ~p~n", [Reason]),
            gen_tcp:close(ServerSocket)
    end.

dump_hex(Binary) ->
    Bytes = binary_to_list(Binary),
    dump_hex_lines(Bytes, 0).

dump_hex_lines([], _) -> ok;
dump_hex_lines(Bytes, Offset) ->
    {Line, Rest} = lists:split(min(16, length(Bytes)), Bytes),
    HexStr = string:join([io_lib:format("~2.16.0B", [B]) || B <- Line], " "),
    AsciiStr = [printable(B) || B <- Line],
    io:format("~4.16.0B: ~-48s ~s~n", [Offset, HexStr, AsciiStr]),
    dump_hex_lines(Rest, Offset + 16).

printable(B) when B >= 32, B < 127 -> B;
printable(_) -> $..

analyze_message(<<Length:32/big, MsgData/binary>>, Label) when byte_size(MsgData) >= 12 ->
    <<Version:16/big, Flags:16/big, MsgIndex:16/big, MsgType:16/big,
      BodyLen:32/big, Rest/binary>> = MsgData,
    <<_Body:BodyLen/binary, Extra/binary>> = Rest,

    io:format("~s analysis:~n", [Label]),
    io:format("  Total length: ~p~n", [Length]),
    io:format("  Version: ~p (0x~4.16.0B)~n", [Version, Version]),
    io:format("  Flags: 0x~4.16.0B~n", [Flags]),
    io:format("  Msg Index: ~p~n", [MsgIndex]),
    io:format("  Msg Type: ~p (~s)~n", [MsgType, msg_type_name(MsgType)]),
    io:format("  Body Length: ~p~n", [BodyLen]),
    io:format("  Extra Data: ~p bytes~n", [byte_size(Extra)]);
analyze_message(_, _) ->
    io:format("  (Unable to parse)~n").

msg_type_name(2003) -> "REQUEST_JOB_INFO";
msg_type_name(2004) -> "RESPONSE_JOB_INFO";
msg_type_name(2007) -> "REQUEST_NODE_INFO";
msg_type_name(2008) -> "RESPONSE_NODE_INFO";
msg_type_name(2009) -> "REQUEST_PARTITION_INFO";
msg_type_name(2010) -> "RESPONSE_PARTITION_INFO";
msg_type_name(1008) -> "REQUEST_PING";
msg_type_name(8001) -> "RESPONSE_SLURM_RC";
msg_type_name(N) -> io_lib:format("MSG_~p", [N]).
