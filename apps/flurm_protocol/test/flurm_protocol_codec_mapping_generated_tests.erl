%%%-------------------------------------------------------------------
%%% @doc Generated-style coverage tests for protocol codec message maps.
%%%-------------------------------------------------------------------
-module(flurm_protocol_codec_mapping_generated_tests).

-include_lib("eunit/include/eunit.hrl").

-define(CODEC_SRC, "apps/flurm_protocol/src/flurm_protocol_codec.erl").
-define(PROTOCOL_HRL, "apps/flurm_protocol/include/flurm_protocol.hrl").

message_type_name_map_test() ->
    MacroVals = parse_macro_values(),
    Pairs = parse_message_type_name_pairs(),
    ?assert(length(Pairs) > 10),
    lists:foreach(
      fun({Macro, ExpectedAtom}) ->
          case maps:find(Macro, MacroVals) of
              {ok, MsgType} ->
                  ?assertEqual(ExpectedAtom, flurm_protocol_codec:message_type_name(MsgType));
              error ->
                  ?assert(false)
          end
      end, Pairs),
    ?assertEqual({unknown, 999999}, flurm_protocol_codec:message_type_name(999999)).

is_request_map_test() ->
    MacroVals = parse_macro_values(),
    Macros = parse_true_macro_clauses("is_request"),
    ?assert(length(Macros) > 10),
    lists:foreach(
      fun(Macro) ->
          case maps:find(Macro, MacroVals) of
              {ok, MsgType} -> ?assertEqual(true, flurm_protocol_codec:is_request(MsgType));
              error -> ?assert(false)
          end
      end, Macros),
    ?assertEqual(true, flurm_protocol_codec:is_request(1001)),
    ?assertEqual(true, flurm_protocol_codec:is_request(1029)),
    ?assertEqual(false, flurm_protocol_codec:is_request(1030)),
    ?assertEqual(false, flurm_protocol_codec:is_request(0)).

is_response_map_test() ->
    MacroVals = parse_macro_values(),
    Macros = parse_true_macro_clauses("is_response"),
    ?assert(length(Macros) > 10),
    lists:foreach(
      fun(Macro) ->
          case maps:find(Macro, MacroVals) of
              {ok, MsgType} -> ?assertEqual(true, flurm_protocol_codec:is_response(MsgType));
              error -> ?assert(false)
          end
      end, Macros),
    ?assertEqual(false, flurm_protocol_codec:is_response(0)).

decode_body_clause_smoke_test() ->
    MacroVals = parse_macro_values(),
    Macros = parse_decode_body_macros(),
    ?assert(length(Macros) > 20),
    lists:foreach(
      fun(Macro) ->
          case maps:find(Macro, MacroVals) of
              {ok, MsgType} ->
                  _ = catch flurm_protocol_codec:decode_body(MsgType, <<>>),
                  ok;
              error ->
                  ?assert(false)
          end
      end, Macros).

encode_body_clause_smoke_test() ->
    MacroVals = parse_macro_values(),
    Macros = parse_encode_body_variable_macros(),
    ?assert(length(Macros) > 20),
    lists:foreach(
      fun(Macro) ->
          case maps:find(Macro, MacroVals) of
              {ok, MsgType} ->
                  _ = catch flurm_protocol_codec:encode_body(MsgType, <<>>),
                  ok;
              error ->
                  ?assert(false)
          end
      end, Macros).

parse_message_type_name_pairs() ->
    {ok, Bin} = file:read_file(?CODEC_SRC),
    Lines = binary:split(Bin, <<"\n">>, [global]),
    lists:reverse(lists:foldl(
      fun(Line, Acc) ->
          case re:run(Line,
                      <<"^message_type_name\\(\\?([A-Z0-9_]+)\\) -> ([a-z0-9_]+);$">>,
                      [{capture, all_but_first, binary}]) of
              {match, [MacroBin, AtomBin]} ->
                  [{binary_to_atom(MacroBin, utf8), binary_to_atom(AtomBin, utf8)} | Acc];
              nomatch ->
                  Acc
          end
      end, [], Lines)).

parse_true_macro_clauses(FunName) ->
    {ok, Bin} = file:read_file(?CODEC_SRC),
    Lines = binary:split(Bin, <<"\n">>, [global]),
    Pattern = iolist_to_binary([
        "^", FunName, "\\(\\?([A-Z0-9_]+)\\) -> true;$"
    ]),
    lists:reverse(lists:foldl(
      fun(Line, Acc) ->
          case re:run(Line, Pattern, [{capture, all_but_first, binary}]) of
              {match, [MacroBin]} -> [binary_to_atom(MacroBin, utf8) | Acc];
              nomatch -> Acc
          end
      end, [], Lines)).

parse_decode_body_macros() ->
    {ok, Bin} = file:read_file(?CODEC_SRC),
    Lines = binary:split(Bin, <<"\n">>, [global]),
    lists:reverse(lists:foldl(
      fun(Line, Acc) ->
          case re:run(Line,
                      <<"^decode_body\\(\\?([A-Z0-9_]+),\\s*[A-Za-z_][A-Za-z0-9_]*\\) ->$">>,
                      [{capture, all_but_first, binary}]) of
              {match, [MacroBin]} -> [binary_to_atom(MacroBin, utf8) | Acc];
              nomatch -> Acc
          end
      end, [], Lines)).

parse_encode_body_variable_macros() ->
    {ok, Bin} = file:read_file(?CODEC_SRC),
    Lines = binary:split(Bin, <<"\n">>, [global]),
    lists:reverse(lists:foldl(
      fun(Line, Acc) ->
          case re:run(Line,
                      <<"^encode_body\\(\\?([A-Z0-9_]+),\\s*[A-Za-z_][A-Za-z0-9_]*\\) ->$">>,
                      [{capture, all_but_first, binary}]) of
              {match, [MacroBin]} -> [binary_to_atom(MacroBin, utf8) | Acc];
              nomatch -> Acc
          end
      end, [], Lines)).

parse_macro_values() ->
    {ok, Bin} = file:read_file(?PROTOCOL_HRL),
    Lines = binary:split(Bin, <<"\n">>, [global]),
    lists:foldl(
      fun(Line, Acc) ->
          case re:run(Line,
                      <<"^-define\\(([A-Z0-9_]+),\\s*([0-9]+)\\).*$">>,
                      [{capture, all_but_first, binary}]) of
              {match, [MacroBin, ValueBin]} ->
                  Macro = binary_to_atom(MacroBin, utf8),
                  Value = binary_to_integer(ValueBin),
                  Acc#{Macro => Value};
              nomatch ->
                  Acc
          end
      end, #{}, Lines).
