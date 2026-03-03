%%%-------------------------------------------------------------------
%%% @doc Tail coverage tests for flurm_pmi_protocol edge branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_pmi_protocol_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").

decode_only_newline_returns_no_command_test() ->
    ?assertEqual({error, no_command}, flurm_pmi_protocol:decode(<<"\n">>)).

decode_cmd_from_attrs_fallback_test() ->
    ?assertEqual({ok, {init, #{rank => 1}}},
                 flurm_pmi_protocol:decode(<<"rank=1 cmd=init\n">>)).

decode_unknown_attr_key_uses_atom_fallback_test() ->
    Unique = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    Key = <<"tail_cov_key_", Unique/binary>>,
    Msg = <<"cmd=init ", Key/binary, "=1\n">>,
    {ok, {init, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(1, map_size(Attrs)),
    [{DecodedKey, Value}] = maps:to_list(Attrs),
    ?assert(is_atom(DecodedKey)),
    ?assertEqual(1, Value).

