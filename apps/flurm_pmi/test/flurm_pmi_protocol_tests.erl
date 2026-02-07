%%%-------------------------------------------------------------------
%%% @doc Unit tests for flurm_pmi_protocol module
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_protocol_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Decode Tests
%%%===================================================================

decode_init_test() ->
    Msg = <<"cmd=init pmi_version=1 pmi_subversion=1\n">>,
    ?assertMatch({ok, {init, #{pmi_version := 1, pmi_subversion := 1}}},
                 flurm_pmi_protocol:decode(Msg)).

decode_init_no_newline_test() ->
    Msg = <<"cmd=init pmi_version=1 pmi_subversion=1">>,
    ?assertMatch({ok, {init, #{pmi_version := 1, pmi_subversion := 1}}},
                 flurm_pmi_protocol:decode(Msg)).

decode_get_maxes_test() ->
    Msg = <<"cmd=get_maxes\n">>,
    ?assertMatch({ok, {get_maxes, #{}}}, flurm_pmi_protocol:decode(Msg)).

decode_get_my_kvsname_test() ->
    Msg = <<"cmd=get_my_kvsname\n">>,
    ?assertMatch({ok, {get_my_kvsname, #{}}}, flurm_pmi_protocol:decode(Msg)).

decode_barrier_in_test() ->
    Msg = <<"cmd=barrier_in\n">>,
    ?assertMatch({ok, {barrier_in, #{}}}, flurm_pmi_protocol:decode(Msg)).

decode_put_test() ->
    Msg = <<"cmd=put key=test_key value=test_value\n">>,
    {ok, {put, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(<<"test_key">>, maps:get(key, Attrs)),
    ?assertEqual(<<"test_value">>, maps:get(value, Attrs)).

decode_get_test() ->
    Msg = <<"cmd=get key=test_key\n">>,
    {ok, {get, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(<<"test_key">>, maps:get(key, Attrs)).

decode_getbyidx_test() ->
    Msg = <<"cmd=getbyidx idx=5\n">>,
    {ok, {getbyidx, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(5, maps:get(idx, Attrs)).

decode_finalize_test() ->
    Msg = <<"cmd=finalize\n">>,
    ?assertMatch({ok, {finalize, #{}}}, flurm_pmi_protocol:decode(Msg)).

decode_empty_message_test() ->
    ?assertEqual({error, empty_message}, flurm_pmi_protocol:decode(<<>>)).

decode_response_to_init_test() ->
    Msg = <<"cmd=response_to_init rc=0 pmi_version=1\n">>,
    ?assertMatch({ok, {init_ack, _}}, flurm_pmi_protocol:decode(Msg)).

%%%===================================================================
%%% Encode Tests
%%%===================================================================

encode_init_ack_test() ->
    Result = flurm_pmi_protocol:encode(init_ack, #{
        rc => 0,
        pmi_version => 1,
        pmi_subversion => 1,
        size => 4,
        rank => 0
    }),
    ?assert(is_binary(Result)),
    ?assert(binary:match(Result, <<"cmd=response_to_init">>) =/= nomatch),
    ?assert(binary:match(Result, <<"rc=0">>) =/= nomatch).

encode_maxes_test() ->
    Result = flurm_pmi_protocol:encode(maxes, #{
        rc => 0,
        kvsname_max => 256,
        keylen_max => 256,
        vallen_max => 1024
    }),
    ?assert(is_binary(Result)),
    ?assert(binary:match(Result, <<"cmd=maxes">>) =/= nomatch).

encode_barrier_out_test() ->
    Result = flurm_pmi_protocol:encode(barrier_out, #{rc => 0}),
    ?assert(is_binary(Result)),
    ?assert(binary:match(Result, <<"cmd=barrier_out">>) =/= nomatch).

encode_put_ack_test() ->
    Result = flurm_pmi_protocol:encode(put_ack, #{rc => 0}),
    ?assert(is_binary(Result)),
    ?assert(binary:match(Result, <<"cmd=put_ack">>) =/= nomatch).

encode_get_ack_test() ->
    Result = flurm_pmi_protocol:encode(get_ack, #{
        rc => 0,
        value => <<"test_value">>
    }),
    ?assert(is_binary(Result)),
    ?assert(binary:match(Result, <<"value=test_value">>) =/= nomatch).

encode_finalize_ack_test() ->
    Result = flurm_pmi_protocol:encode(finalize_ack, #{rc => 0}),
    ?assert(is_binary(Result)),
    ?assert(binary:match(Result, <<"cmd=finalize_ack">>) =/= nomatch).

encode_empty_attrs_test() ->
    Result = flurm_pmi_protocol:encode(barrier_out, #{}),
    ?assertEqual(<<"cmd=barrier_out\n">>, Result).

%%%===================================================================
%%% Parse Attrs Tests
%%%===================================================================

parse_attrs_simple_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"key1=value1 key2=value2">>),
    ?assertEqual(<<"value1">>, maps:get(key1, Attrs)),
    ?assertEqual(<<"value2">>, maps:get(key2, Attrs)).

parse_attrs_integer_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"count=42 name=test">>),
    ?assertEqual(42, maps:get(count, Attrs)),
    ?assertEqual(<<"test">>, maps:get(name, Attrs)).

format_attrs_test() ->
    Attrs = #{key1 => <<"value1">>, count => 42},
    Result = flurm_pmi_protocol:format_attrs(Attrs),
    ?assert(is_binary(Result)),
    %% Order may vary, so check both are present
    ?assert(binary:match(Result, <<"key1=value1">>) =/= nomatch orelse
            binary:match(Result, <<"count=42">>) =/= nomatch).
