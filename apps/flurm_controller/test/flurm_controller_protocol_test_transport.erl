%%%-------------------------------------------------------------------
%%% @doc Deterministic transport shim for protocol loop tests.
%%%-------------------------------------------------------------------
-module(flurm_controller_protocol_test_transport).

-export([set_sequence/1, calls/0, reset/0]).
-export([recv/3, send/2, close/1]).

-define(TABLE, flurm_controller_protocol_test_transport_tab).

set_sequence(Seq) ->
    ensure_table(),
    ets:insert(?TABLE, {recv_seq, Seq}),
    ets:insert(?TABLE, {sent, []}),
    ets:insert(?TABLE, {closed, false}),
    ok.

calls() ->
    ensure_table(),
    Sent = case ets:lookup(?TABLE, sent) of
               [{sent, S}] -> S;
               _ -> []
           end,
    Closed = case ets:lookup(?TABLE, closed) of
                 [{closed, C}] -> C;
                 _ -> false
             end,
    #{sent_count => length(Sent),
      sent => lists:reverse(Sent),
      closed => Closed}.

reset() ->
    case ets:info(?TABLE) of
        undefined ->
            ok;
        _ ->
            ets:delete(?TABLE)
    end.

recv(_Socket, _Len, _Timeout) ->
    ensure_table(),
    case ets:lookup(?TABLE, recv_seq) of
        [{recv_seq, [Head | Tail]}] ->
            ets:insert(?TABLE, {recv_seq, Tail}),
            Head;
        [{recv_seq, []}] ->
            {error, closed};
        _ ->
            {error, closed}
    end.

send(_Socket, Bin) ->
    ensure_table(),
    Sent0 = case ets:lookup(?TABLE, sent) of
                [{sent, S}] -> S;
                _ -> []
            end,
    ets:insert(?TABLE, {sent, [Bin | Sent0]}),
    ok.

close(_Socket) ->
    ensure_table(),
    ets:insert(?TABLE, {closed, true}),
    ok.

ensure_table() ->
    case ets:info(?TABLE) of
        undefined ->
            ets:new(?TABLE, [named_table, public, set]),
            ok;
        _ ->
            ok
    end.
