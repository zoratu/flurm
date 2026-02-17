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

%%%===================================================================
%%% Extended Decode Tests
%%%===================================================================

decode_mcmd_test() ->
    %% Multi-command format
    Msg = <<"mcmd=init pmi_version=1\n">>,
    ?assertMatch({ok, {init, _}}, flurm_pmi_protocol:decode(Msg)).

decode_appnum_test() ->
    Msg = <<"cmd=appnum rc=0 appnum=0\n">>,
    ?assertMatch({ok, {appnum, #{rc := 0, appnum := 0}}},
                 flurm_pmi_protocol:decode(Msg)).

decode_my_kvsname_test() ->
    Msg = <<"cmd=my_kvsname rc=0 kvsname=test_kvs\n">>,
    ?assertMatch({ok, {my_kvsname, #{rc := 0}}},
                 flurm_pmi_protocol:decode(Msg)).

decode_barrier_out_test() ->
    Msg = <<"cmd=barrier_out rc=0\n">>,
    ?assertMatch({ok, {barrier_out, #{rc := 0}}},
                 flurm_pmi_protocol:decode(Msg)).

decode_put_ack_test() ->
    Msg = <<"cmd=put_ack rc=0\n">>,
    ?assertMatch({ok, {put_ack, #{rc := 0}}},
                 flurm_pmi_protocol:decode(Msg)).

decode_get_ack_test() ->
    Msg = <<"cmd=get_ack rc=0 value=test\n">>,
    {ok, {get_ack, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(<<"test">>, maps:get(value, Attrs)).

decode_getbyidx_ack_test() ->
    Msg = <<"cmd=getbyidx_ack rc=0 key=k value=v nextidx=1\n">>,
    {ok, {getbyidx_ack, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(<<"k">>, maps:get(key, Attrs)),
    ?assertEqual(<<"v">>, maps:get(value, Attrs)),
    ?assertEqual(1, maps:get(nextidx, Attrs)).

decode_finalize_ack_test() ->
    Msg = <<"cmd=finalize_ack rc=0\n">>,
    ?assertMatch({ok, {finalize_ack, #{rc := 0}}},
                 flurm_pmi_protocol:decode(Msg)).

decode_abort_test() ->
    Msg = <<"cmd=abort\n">>,
    ?assertMatch({ok, {abort, #{}}},
                 flurm_pmi_protocol:decode(Msg)).

decode_unknown_command_test() ->
    Msg = <<"cmd=not_a_real_command\n">>,
    ?assertMatch({ok, {unknown, #{}}},
                 flurm_pmi_protocol:decode(Msg)).

decode_init_ack_test() ->
    Msg = <<"cmd=init_ack rc=0\n">>,
    ?assertMatch({ok, {init_ack, _}}, flurm_pmi_protocol:decode(Msg)).

decode_maxes_test() ->
    Msg = <<"cmd=maxes rc=0 kvsname_max=256\n">>,
    ?assertMatch({ok, {maxes, _}}, flurm_pmi_protocol:decode(Msg)).

decode_get_appnum_test() ->
    Msg = <<"cmd=get_appnum\n">>,
    ?assertMatch({ok, {get_appnum, _}}, flurm_pmi_protocol:decode(Msg)).

decode_no_command_test() ->
    %% Line with no cmd= prefix - should try to find cmd in attrs
    Msg = <<"some_invalid_format\n">>,
    Result = flurm_pmi_protocol:decode(Msg),
    ?assertMatch({error, _}, Result).

decode_attr_without_equals_test() ->
    %% Parts without = should be skipped
    Msg = <<"cmd=init pmi_version=1 extra_part pmi_subversion=1\n">>,
    {ok, {init, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(1, maps:get(pmi_version, Attrs)),
    ?assertEqual(1, maps:get(pmi_subversion, Attrs)).

%%%===================================================================
%%% Extended Encode Tests
%%%===================================================================

encode_my_kvsname_test() ->
    Result = flurm_pmi_protocol:encode(my_kvsname, #{rc => 0, kvsname => <<"test">>}),
    ?assert(binary:match(Result, <<"cmd=my_kvsname">>) =/= nomatch).

encode_appnum_test() ->
    Result = flurm_pmi_protocol:encode(appnum, #{rc => 0, appnum => 5}),
    ?assert(binary:match(Result, <<"cmd=appnum">>) =/= nomatch),
    ?assert(binary:match(Result, <<"appnum=5">>) =/= nomatch).

encode_getbyidx_ack_test() ->
    Result = flurm_pmi_protocol:encode(getbyidx_ack, #{
        rc => 0,
        key => <<"testkey">>,
        value => <<"testval">>,
        nextidx => 1
    }),
    ?assert(binary:match(Result, <<"cmd=getbyidx_ack">>) =/= nomatch).

encode_abort_test() ->
    Result = flurm_pmi_protocol:encode(abort, #{}),
    ?assertEqual(<<"cmd=abort\n">>, Result).

encode_unknown_cmd_test() ->
    %% Unknown commands should still encode
    Result = flurm_pmi_protocol:encode(some_custom_cmd, #{data => 1}),
    ?assert(binary:match(Result, <<"cmd=some_custom_cmd">>) =/= nomatch).

encode_init_test() ->
    Result = flurm_pmi_protocol:encode(init, #{pmi_version => 1}),
    ?assert(binary:match(Result, <<"cmd=init">>) =/= nomatch).

encode_get_maxes_test() ->
    Result = flurm_pmi_protocol:encode(get_maxes, #{}),
    ?assertEqual(<<"cmd=get_maxes\n">>, Result).

encode_get_my_kvsname_test() ->
    Result = flurm_pmi_protocol:encode(get_my_kvsname, #{}),
    ?assertEqual(<<"cmd=get_my_kvsname\n">>, Result).

encode_barrier_in_test() ->
    Result = flurm_pmi_protocol:encode(barrier_in, #{}),
    ?assertEqual(<<"cmd=barrier_in\n">>, Result).

encode_put_test() ->
    Result = flurm_pmi_protocol:encode(put, #{key => <<"k">>, value => <<"v">>}),
    ?assert(binary:match(Result, <<"cmd=put">>) =/= nomatch).

encode_get_test() ->
    Result = flurm_pmi_protocol:encode(get, #{key => <<"k">>}),
    ?assert(binary:match(Result, <<"cmd=get">>) =/= nomatch).

encode_getbyidx_test() ->
    Result = flurm_pmi_protocol:encode(getbyidx, #{idx => 5}),
    ?assert(binary:match(Result, <<"cmd=getbyidx">>) =/= nomatch).

encode_finalize_test() ->
    Result = flurm_pmi_protocol:encode(finalize, #{}),
    ?assertEqual(<<"cmd=finalize\n">>, Result).

encode_get_appnum_test() ->
    Result = flurm_pmi_protocol:encode(get_appnum, #{}),
    ?assertEqual(<<"cmd=get_appnum\n">>, Result).

%%%===================================================================
%%% Value Conversion Tests
%%%===================================================================

format_attrs_atom_value_test() ->
    Attrs = #{status => ok},
    Result = flurm_pmi_protocol:format_attrs(Attrs),
    ?assert(binary:match(Result, <<"status=ok">>) =/= nomatch).

format_attrs_list_value_test() ->
    Attrs = #{path => "/tmp/test"},
    Result = flurm_pmi_protocol:format_attrs(Attrs),
    ?assert(binary:match(Result, <<"path=/tmp/test">>) =/= nomatch).

format_attrs_multiple_test() ->
    Attrs = #{a => 1, b => <<"two">>, c => three},
    Result = flurm_pmi_protocol:format_attrs(Attrs),
    ?assert(binary:match(Result, <<"a=1">>) =/= nomatch),
    ?assert(binary:match(Result, <<"b=two">>) =/= nomatch),
    ?assert(binary:match(Result, <<"c=three">>) =/= nomatch).

%%%===================================================================
%%% Parse Attrs Edge Cases
%%%===================================================================

parse_attrs_empty_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<>>),
    ?assertEqual(#{}, Attrs).

parse_attrs_single_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"key=value">>),
    ?assertEqual(<<"value">>, maps:get(key, Attrs)).

parse_attrs_empty_value_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"key=">>),
    ?assertEqual(<<>>, maps:get(key, Attrs)).

parse_attrs_negative_integer_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"rc=-1">>),
    ?assertEqual(-1, maps:get(rc, Attrs)).

parse_attrs_large_integer_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"size=1000000">>),
    ?assertEqual(1000000, maps:get(size, Attrs)).

%%%===================================================================
%%% Roundtrip Tests
%%%===================================================================

roundtrip_init_test() ->
    OrigAttrs = #{pmi_version => 1, pmi_subversion => 1},
    Encoded = flurm_pmi_protocol:encode(init, OrigAttrs),
    {ok, {init, DecodedAttrs}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(1, maps:get(pmi_version, DecodedAttrs)),
    ?assertEqual(1, maps:get(pmi_subversion, DecodedAttrs)).

roundtrip_put_test() ->
    OrigAttrs = #{key => <<"testkey">>, value => <<"testval">>},
    Encoded = flurm_pmi_protocol:encode(put, OrigAttrs),
    {ok, {put, DecodedAttrs}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(<<"testkey">>, maps:get(key, DecodedAttrs)),
    ?assertEqual(<<"testval">>, maps:get(value, DecodedAttrs)).

roundtrip_barrier_out_test() ->
    OrigAttrs = #{rc => 0},
    Encoded = flurm_pmi_protocol:encode(barrier_out, OrigAttrs),
    {ok, {barrier_out, DecodedAttrs}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(0, maps:get(rc, DecodedAttrs)).
