%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_pmi_protocol module
%%% Coverage target: 100% of all functions and branches
%%%
%%% This file supplements the existing flurm_pmi_protocol_tests.erl
%%% with additional edge cases and branch coverage.
%%%-------------------------------------------------------------------
-module(flurm_pmi_protocol_100cov_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Decode Tests - Additional Edge Cases
%%%===================================================================

%% Test decode with only command, no attributes
decode_cmd_only_test() ->
    {ok, {init, Attrs}} = flurm_pmi_protocol:decode(<<"cmd=init\n">>),
    ?assertEqual(#{}, Attrs).

%% Test decode with empty parts (multiple spaces)
decode_extra_spaces_test() ->
    %% Extra spaces result in empty parts that should be skipped
    Msg = <<"cmd=init  pmi_version=1\n">>,
    {ok, {init, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(1, maps:get(pmi_version, Attrs)).

%% Test decode with equals in value
decode_equals_in_value_test() ->
    %% binary:split with default only splits on first =
    Msg = <<"cmd=put key=a=b=c value=test\n">>,
    {ok, {put, Attrs}} = flurm_pmi_protocol:decode(Msg),
    %% Key should be "a=b=c" after first split
    ?assertEqual(<<"a=b=c">>, maps:get(key, Attrs)).

%% Test decode with trailing whitespace
decode_trailing_whitespace_test() ->
    Msg = <<"cmd=init pmi_version=1 \n">>,
    {ok, {init, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(1, maps:get(pmi_version, Attrs)).

%% Test decode with no newline at end
decode_no_newline_test() ->
    Msg = <<"cmd=init pmi_version=2">>,
    {ok, {init, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(2, maps:get(pmi_version, Attrs)).

%% Test decode with tab character (not space)
decode_with_tab_test() ->
    %% Tabs are not standard separators in PMI, should be part of value
    Msg = <<"cmd=put key=k\tvalue\n">>,
    Result = flurm_pmi_protocol:decode(Msg),
    ?assertMatch({ok, {put, _}}, Result).

%% Test all command types decode correctly
decode_all_commands_test_() ->
    Commands = [
        {<<"init">>, init},
        {<<"response_to_init">>, init_ack},
        {<<"init_ack">>, init_ack},
        {<<"get_maxes">>, get_maxes},
        {<<"maxes">>, maxes},
        {<<"get_appnum">>, get_appnum},
        {<<"appnum">>, appnum},
        {<<"get_my_kvsname">>, get_my_kvsname},
        {<<"my_kvsname">>, my_kvsname},
        {<<"barrier_in">>, barrier_in},
        {<<"barrier_out">>, barrier_out},
        {<<"put">>, put},
        {<<"put_ack">>, put_ack},
        {<<"get">>, get},
        {<<"get_ack">>, get_ack},
        {<<"getbyidx">>, getbyidx},
        {<<"getbyidx_ack">>, getbyidx_ack},
        {<<"finalize">>, finalize},
        {<<"finalize_ack">>, finalize_ack},
        {<<"abort">>, abort}
    ],
    [{"decode command " ++ binary_to_list(CmdBin),
      fun() ->
          Msg = <<"cmd=", CmdBin/binary, "\n">>,
          {ok, {Atom, _}} = flurm_pmi_protocol:decode(Msg),
          ?assertEqual(Expected, Atom)
      end}
     || {CmdBin, Expected} <- Commands].

%% Test decode with attrs before cmd=
decode_attrs_format_test() ->
    %% When first part doesn't have cmd=, try to find cmd in attrs
    Msg = <<"pmi_version=1 cmd=init\n">>,
    {ok, {init, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(1, maps:get(pmi_version, Attrs)),
    %% cmd should be removed from attrs
    ?assertEqual(error, maps:find(cmd, Attrs)).

%% Test decode with zero integer
decode_zero_integer_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"rc=0">>),
    ?assertEqual(0, maps:get(rc, Attrs)).

%%%===================================================================
%%% Encode Tests - Additional Edge Cases
%%%===================================================================

%% Test all command types encode correctly
encode_all_commands_test_() ->
    Commands = [init, init_ack, get_maxes, maxes, get_appnum, appnum,
                get_my_kvsname, my_kvsname, barrier_in, barrier_out,
                put, put_ack, get, get_ack, getbyidx, getbyidx_ack,
                finalize, finalize_ack, abort],
    [{"encode command " ++ atom_to_list(Cmd),
      fun() ->
          Result = flurm_pmi_protocol:encode(Cmd, #{}),
          ?assert(is_binary(Result)),
          ?assertEqual($\n, binary:last(Result)),
          ?assertNotEqual(nomatch, binary:match(Result, <<"cmd=">>))
      end}
     || Cmd <- Commands].

%% Test encode with empty map uses short format
encode_empty_map_short_format_test() ->
    Result = flurm_pmi_protocol:encode(finalize, #{}),
    %% Should be exactly "cmd=finalize\n" with no trailing space
    ?assertEqual(<<"cmd=finalize\n">>, Result).

%% Test encode with single attr
encode_single_attr_test() ->
    Result = flurm_pmi_protocol:encode(barrier_out, #{rc => 0}),
    ?assertNotEqual(nomatch, binary:match(Result, <<"rc=0">>)).

%% Test encode with multiple attrs
encode_multiple_attrs_test() ->
    Result = flurm_pmi_protocol:encode(init_ack, #{
        rc => 0,
        size => 4,
        rank => 0
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"rc=0">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"size=4">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"rank=0">>)).

%% Test encode with negative integer
encode_negative_integer_test() ->
    Result = flurm_pmi_protocol:encode(get_ack, #{rc => -1}),
    ?assertNotEqual(nomatch, binary:match(Result, <<"rc=-1">>)).

%% Test encode with large integer
encode_large_integer_test() ->
    Result = flurm_pmi_protocol:encode(maxes, #{vallen_max => 1000000}),
    ?assertNotEqual(nomatch, binary:match(Result, <<"vallen_max=1000000">>)).

%% Test encode preserves spaces in value via binary
encode_spaces_in_value_test() ->
    Result = flurm_pmi_protocol:encode(get_ack, #{value => <<"hello world">>}),
    ?assertNotEqual(nomatch, binary:match(Result, <<"value=hello world">>)).

%%%===================================================================
%%% parse_attrs Tests - Additional Edge Cases
%%%===================================================================

%% Test parse_attrs with only spaces
parse_attrs_only_spaces_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"   ">>),
    ?assertEqual(#{}, Attrs).

%% Test parse_attrs with leading/trailing spaces
parse_attrs_leading_trailing_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<" key=value ">>),
    ?assertEqual(<<"value">>, maps:get(key, Attrs)).

%% Test parse_attrs with multiple consecutive spaces
parse_attrs_multiple_spaces_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"k1=v1   k2=v2">>),
    ?assertEqual(<<"v1">>, maps:get(k1, Attrs)),
    ?assertEqual(<<"v2">>, maps:get(k2, Attrs)).

%% Test parse_attrs with integer parsing
parse_attrs_various_integers_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"a=0 b=1 c=-1 d=999999">>),
    ?assertEqual(0, maps:get(a, Attrs)),
    ?assertEqual(1, maps:get(b, Attrs)),
    ?assertEqual(-1, maps:get(c, Attrs)),
    ?assertEqual(999999, maps:get(d, Attrs)).

%% Test parse_attrs non-integer stays as binary
parse_attrs_non_integer_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"name=test123abc version=1.0">>),
    ?assertEqual(<<"test123abc">>, maps:get(name, Attrs)),
    ?assertEqual(<<"1.0">>, maps:get(version, Attrs)).

%% Test parse_attrs with empty value
parse_attrs_empty_value_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"key=">>),
    ?assertEqual(<<>>, maps:get(key, Attrs)).

%% Test parse_attrs single part without equals
parse_attrs_single_no_equals_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"singlepart">>),
    ?assertEqual(#{}, Attrs).

%%%===================================================================
%%% format_attrs Tests - Additional Edge Cases
%%%===================================================================

%% Test format_attrs empty map
format_attrs_empty_test() ->
    ?assertEqual(<<>>, flurm_pmi_protocol:format_attrs(#{})).

%% Test format_attrs with binary value containing special chars
format_attrs_special_chars_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{key => <<"a/b:c@d">>}),
    ?assertNotEqual(nomatch, binary:match(Result, <<"key=a/b:c@d">>)).

%% Test format_attrs with atom value
format_attrs_atom_value_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{status => success}),
    ?assertNotEqual(nomatch, binary:match(Result, <<"status=success">>)).

%% Test format_attrs with integer value
format_attrs_integer_value_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{count => 42}),
    ?assertNotEqual(nomatch, binary:match(Result, <<"count=42">>)).

%% Test format_attrs with list value (converted to binary)
format_attrs_list_value_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{path => "/tmp/test"}),
    ?assertNotEqual(nomatch, binary:match(Result, <<"path=/tmp/test">>)).

%% Test format_attrs with mixed types
format_attrs_mixed_types_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{
        a => 1,
        b => <<"two">>,
        c => three,
        d => "four"
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"a=1">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"b=two">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"c=three">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"d=four">>)).

%%%===================================================================
%%% Roundtrip Tests - Additional Cases
%%%===================================================================

%% Test roundtrip with all value types
roundtrip_value_types_test() ->
    Attrs = #{
        int_val => 123,
        bin_val => <<"binary">>,
        atom_val => atomval
    },
    Encoded = flurm_pmi_protocol:encode(put_ack, Attrs),
    {ok, {put_ack, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    %% Integers come back as integers
    ?assertEqual(123, maps:get(int_val, Decoded)),
    %% Binaries stay as binaries
    ?assertEqual(<<"binary">>, maps:get(bin_val, Decoded)),
    %% Atoms become binaries
    ?assertEqual(<<"atomval">>, maps:get(atom_val, Decoded)).

%% Test roundtrip with empty attrs
roundtrip_empty_attrs_test() ->
    Encoded = flurm_pmi_protocol:encode(finalize, #{}),
    {ok, {finalize, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(#{}, Decoded).

%% Test roundtrip for every command type
roundtrip_all_commands_test_() ->
    Commands = [init, get_maxes, get_appnum, get_my_kvsname,
                barrier_in, finalize, abort],
    [{"roundtrip " ++ atom_to_list(Cmd),
      fun() ->
          Encoded = flurm_pmi_protocol:encode(Cmd, #{testkey => 1}),
          {ok, {Cmd, Decoded}} = flurm_pmi_protocol:decode(Encoded),
          ?assertEqual(1, maps:get(testkey, Decoded))
      end}
     || Cmd <- Commands].

%%%===================================================================
%%% Error Handling Tests
%%%===================================================================

%% Test decode returns error for empty
decode_error_empty_test() ->
    ?assertEqual({error, empty_message}, flurm_pmi_protocol:decode(<<>>)).

%% Test decode with invalid format returns error
decode_error_invalid_format_test() ->
    %% No cmd= and no equals at all
    Result = flurm_pmi_protocol:decode(<<"justtext\n">>),
    ?assertMatch({error, _}, Result).

%% Test decode with equals but no cmd key
decode_error_no_cmd_key_test() ->
    Result = flurm_pmi_protocol:decode(<<"key=value\n">>),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% safe_binary_to_atom Tests (via parse_attrs)
%%%===================================================================

%% Test that existing atoms are reused
safe_binary_to_atom_existing_test() ->
    %% 'key' already exists as atom from previous usage
    Attrs1 = flurm_pmi_protocol:parse_attrs(<<"key=v1">>),
    Attrs2 = flurm_pmi_protocol:parse_attrs(<<"key=v2">>),
    ?assertEqual(<<"v1">>, maps:get(key, Attrs1)),
    ?assertEqual(<<"v2">>, maps:get(key, Attrs2)).

%% Test new atoms are created for unknown keys
safe_binary_to_atom_new_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"newuniquetestkey123=value">>),
    ?assertEqual(<<"value">>, maps:get(newuniquetestkey123, Attrs)).

%%%===================================================================
%%% mcmd Format Tests
%%%===================================================================

%% Test mcmd with various commands
mcmd_all_commands_test() ->
    lists:foreach(fun(Cmd) ->
        Msg = <<"mcmd=", Cmd/binary, " rc=0\n">>,
        {ok, {_, Attrs}} = flurm_pmi_protocol:decode(Msg),
        ?assertEqual(0, maps:get(rc, Attrs))
    end, [<<"init">>, <<"get_maxes">>, <<"finalize">>]).

%% Test mcmd with attributes
mcmd_with_attrs_test() ->
    Msg = <<"mcmd=init pmi_version=2 pmi_subversion=1\n">>,
    {ok, {init, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(2, maps:get(pmi_version, Attrs)),
    ?assertEqual(1, maps:get(pmi_subversion, Attrs)).

%%%===================================================================
%%% Value Parsing Edge Cases
%%%===================================================================

%% Test parse_value with empty string
parse_value_empty_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"key=">>),
    ?assertEqual(<<>>, maps:get(key, Attrs)).

%% Test parse_value with leading zeros
parse_value_leading_zeros_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"num=007">>),
    ?assertEqual(7, maps:get(num, Attrs)).

%% Test parse_value with hex-like string (stays as binary)
parse_value_hex_string_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"val=0x10">>),
    ?assertEqual(<<"0x10">>, maps:get(val, Attrs)).

%% Test parse_value with decimal point (stays as binary)
parse_value_decimal_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"val=1.5">>),
    ?assertEqual(<<"1.5">>, maps:get(val, Attrs)).

%% Test parse_value with exponential notation (stays as binary)
parse_value_exponential_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"val=1e10">>),
    ?assertEqual(<<"1e10">>, maps:get(val, Attrs)).

%%%===================================================================
%%% command_to_binary Coverage Tests
%%%===================================================================

%% Test unknown command encodes to atom binary
encode_unknown_command_test() ->
    Result = flurm_pmi_protocol:encode(some_unknown_cmd, #{}),
    ?assertEqual(<<"cmd=some_unknown_cmd\n">>, Result).

%% Test custom command with attrs
encode_custom_command_with_attrs_test() ->
    Result = flurm_pmi_protocol:encode(custom_cmd, #{data => <<"test">>}),
    ?assertNotEqual(nomatch, binary:match(Result, <<"cmd=custom_cmd">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"data=test">>)).

%%%===================================================================
%%% Integration Tests
%%%===================================================================

%% Test typical MPI init sequence
mpi_init_sequence_test() ->
    %% Client sends init
    InitMsg = <<"cmd=init pmi_version=1 pmi_subversion=1\n">>,
    {ok, {init, InitAttrs}} = flurm_pmi_protocol:decode(InitMsg),
    ?assertEqual(1, maps:get(pmi_version, InitAttrs)),

    %% Server responds with init_ack
    InitAck = flurm_pmi_protocol:encode(init_ack, #{
        rc => 0,
        pmi_version => 1,
        pmi_subversion => 1,
        size => 4,
        rank => 0
    }),
    {ok, {init_ack, AckAttrs}} = flurm_pmi_protocol:decode(InitAck),
    ?assertEqual(0, maps:get(rc, AckAttrs)),
    ?assertEqual(4, maps:get(size, AckAttrs)).

%% Test typical put/get sequence
put_get_sequence_test() ->
    %% Put
    PutMsg = <<"cmd=put key=rank0_addr value=tcp://host:1234\n">>,
    {ok, {put, PutAttrs}} = flurm_pmi_protocol:decode(PutMsg),
    ?assertEqual(<<"rank0_addr">>, maps:get(key, PutAttrs)),
    ?assertEqual(<<"tcp://host:1234">>, maps:get(value, PutAttrs)),

    %% Put ack
    PutAck = flurm_pmi_protocol:encode(put_ack, #{rc => 0}),
    {ok, {put_ack, PutAckAttrs}} = flurm_pmi_protocol:decode(PutAck),
    ?assertEqual(0, maps:get(rc, PutAckAttrs)),

    %% Get
    GetMsg = <<"cmd=get key=rank0_addr\n">>,
    {ok, {get, GetAttrs}} = flurm_pmi_protocol:decode(GetMsg),
    ?assertEqual(<<"rank0_addr">>, maps:get(key, GetAttrs)),

    %% Get ack
    GetAck = flurm_pmi_protocol:encode(get_ack, #{
        rc => 0,
        value => <<"tcp://host:1234">>
    }),
    {ok, {get_ack, GetAckAttrs}} = flurm_pmi_protocol:decode(GetAck),
    ?assertEqual(0, maps:get(rc, GetAckAttrs)),
    ?assertEqual(<<"tcp://host:1234">>, maps:get(value, GetAckAttrs)).

%%%===================================================================
%%% Additional Decode Tests for Full Coverage
%%%===================================================================

%% Test decode of each command with full attribute sets
decode_init_with_all_attrs_test() ->
    Msg = <<"cmd=init pmi_version=2 pmi_subversion=0 rank=5\n">>,
    {ok, {init, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(2, maps:get(pmi_version, Attrs)),
    ?assertEqual(0, maps:get(pmi_subversion, Attrs)),
    ?assertEqual(5, maps:get(rank, Attrs)).

decode_maxes_response_test() ->
    Msg = <<"cmd=maxes rc=0 kvsname_max=256 keylen_max=256 vallen_max=1024\n">>,
    {ok, {maxes, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(256, maps:get(kvsname_max, Attrs)),
    ?assertEqual(256, maps:get(keylen_max, Attrs)),
    ?assertEqual(1024, maps:get(vallen_max, Attrs)).

decode_appnum_response_test() ->
    Msg = <<"cmd=appnum rc=0 appnum=0\n">>,
    {ok, {appnum, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(0, maps:get(appnum, Attrs)).

decode_my_kvsname_response_test() ->
    Msg = <<"cmd=my_kvsname rc=0 kvsname=kvs_1_0\n">>,
    {ok, {my_kvsname, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(<<"kvs_1_0">>, maps:get(kvsname, Attrs)).

decode_barrier_out_response_test() ->
    Msg = <<"cmd=barrier_out rc=0\n">>,
    {ok, {barrier_out, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(0, maps:get(rc, Attrs)).

decode_getbyidx_full_test() ->
    Msg = <<"cmd=getbyidx idx=5\n">>,
    {ok, {getbyidx, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(5, maps:get(idx, Attrs)).

decode_getbyidx_ack_full_test() ->
    Msg = <<"cmd=getbyidx_ack rc=0 key=mykey value=myval nextidx=6\n">>,
    {ok, {getbyidx_ack, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(0, maps:get(rc, Attrs)),
    ?assertEqual(<<"mykey">>, maps:get(key, Attrs)),
    ?assertEqual(<<"myval">>, maps:get(value, Attrs)),
    ?assertEqual(6, maps:get(nextidx, Attrs)).

decode_finalize_ack_full_test() ->
    Msg = <<"cmd=finalize_ack rc=0\n">>,
    {ok, {finalize_ack, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(0, maps:get(rc, Attrs)).

decode_abort_with_reason_test() ->
    Msg = <<"cmd=abort exitcode=1 message=error\n">>,
    {ok, {abort, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(1, maps:get(exitcode, Attrs)),
    ?assertEqual(<<"error">>, maps:get(message, Attrs)).

%%%===================================================================
%%% Additional Encode Tests for Full Coverage
%%%===================================================================

encode_init_ack_full_test() ->
    Result = flurm_pmi_protocol:encode(init_ack, #{
        rc => 0,
        pmi_version => 1,
        pmi_subversion => 1,
        size => 8,
        rank => 3,
        debug => 0,
        appnum => 0
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"cmd=response_to_init">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"rc=0">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"size=8">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"rank=3">>)).

encode_maxes_full_test() ->
    Result = flurm_pmi_protocol:encode(maxes, #{
        rc => 0,
        kvsname_max => 256,
        keylen_max => 256,
        vallen_max => 1024
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"cmd=maxes">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"kvsname_max=256">>)).

encode_my_kvsname_full_test() ->
    Result = flurm_pmi_protocol:encode(my_kvsname, #{
        rc => 0,
        kvsname => <<"kvs_test_123">>
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"kvsname=kvs_test_123">>)).

encode_put_full_test() ->
    Result = flurm_pmi_protocol:encode(put, #{
        key => <<"test_key">>,
        value => <<"test_value">>
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"cmd=put">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"key=test_key">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"value=test_value">>)).

encode_get_full_test() ->
    Result = flurm_pmi_protocol:encode(get, #{
        key => <<"lookup_key">>
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"cmd=get">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"key=lookup_key">>)).

encode_get_ack_full_test() ->
    Result = flurm_pmi_protocol:encode(get_ack, #{
        rc => 0,
        value => <<"found_value">>
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"cmd=get_ack">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"value=found_value">>)).

encode_getbyidx_full_test() ->
    Result = flurm_pmi_protocol:encode(getbyidx, #{
        idx => 10
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"cmd=getbyidx">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"idx=10">>)).

encode_getbyidx_ack_full_test() ->
    Result = flurm_pmi_protocol:encode(getbyidx_ack, #{
        rc => 0,
        key => <<"indexed_key">>,
        value => <<"indexed_value">>,
        nextidx => 11
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"cmd=getbyidx_ack">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"nextidx=11">>)).

encode_abort_full_test() ->
    Result = flurm_pmi_protocol:encode(abort, #{
        exitcode => 1,
        message => <<"fatal error">>
    }),
    ?assertNotEqual(nomatch, binary:match(Result, <<"cmd=abort">>)),
    ?assertNotEqual(nomatch, binary:match(Result, <<"exitcode=1">>)).

%%%===================================================================
%%% format_attrs Additional Tests
%%%===================================================================

format_attrs_single_binary_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{name => <<"test">>}),
    ?assertEqual(<<"name=test">>, Result).

format_attrs_single_integer_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{val => 42}),
    ?assertEqual(<<"val=42">>, Result).

format_attrs_single_atom_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{status => success}),
    ?assertEqual(<<"status=success">>, Result).

format_attrs_single_list_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{path => "/tmp/test"}),
    ?assertEqual(<<"path=/tmp/test">>, Result).

format_attrs_negative_int_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{rc => -1}),
    ?assertEqual(<<"rc=-1">>, Result).

format_attrs_zero_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{count => 0}),
    ?assertEqual(<<"count=0">>, Result).

format_attrs_large_int_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{big => 999999999}),
    ?assertEqual(<<"big=999999999">>, Result).

format_attrs_empty_binary_test() ->
    Result = flurm_pmi_protocol:format_attrs(#{key => <<>>}),
    ?assertEqual(<<"key=">>, Result).

%%%===================================================================
%%% parse_attrs Additional Tests
%%%===================================================================

parse_attrs_empty_binary_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<>>),
    ?assertEqual(#{}, Attrs).

parse_attrs_single_space_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<" ">>),
    ?assertEqual(#{}, Attrs).

parse_attrs_many_spaces_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"     ">>),
    ?assertEqual(#{}, Attrs).

parse_attrs_key_only_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"keyonly">>),
    ?assertEqual(#{}, Attrs).

parse_attrs_multiple_keys_only_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"key1 key2 key3">>),
    ?assertEqual(#{}, Attrs).

parse_attrs_mixed_valid_invalid_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"valid=1 invalid valid2=2">>),
    ?assertEqual(1, maps:get(valid, Attrs)),
    ?assertEqual(2, maps:get(valid2, Attrs)),
    ?assertEqual(2, map_size(Attrs)).

parse_attrs_unicode_key_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"unicodekey=value">>),
    ?assertEqual(<<"value">>, maps:get(unicodekey, Attrs)).

parse_attrs_underscore_key_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"key_with_underscores=val">>),
    ?assertEqual(<<"val">>, maps:get(key_with_underscores, Attrs)).

parse_attrs_number_key_test() ->
    Attrs = flurm_pmi_protocol:parse_attrs(<<"key123=val">>),
    ?assertEqual(<<"val">>, maps:get(key123, Attrs)).

%%%===================================================================
%%% Binary Edge Cases
%%%===================================================================

decode_binary_with_cr_test() ->
    %% Carriage return before newline - CR becomes part of command
    %% which results in unknown command since "init\r" is not "init"
    Msg = <<"cmd=init\r\n">>,
    Result = flurm_pmi_protocol:decode(Msg),
    %% The \r is part of the command, making it unknown
    ?assertMatch({ok, {unknown, _}}, Result).

decode_binary_with_unicode_value_test() ->
    Msg = <<"cmd=put key=name value=test\n">>,
    {ok, {put, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(<<"test">>, maps:get(value, Attrs)).

decode_very_long_value_test() ->
    LongVal = binary:copy(<<"x">>, 1000),
    Msg = <<"cmd=put key=long value=", LongVal/binary, "\n">>,
    {ok, {put, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(LongVal, maps:get(value, Attrs)).

decode_many_attrs_test() ->
    Msg = <<"cmd=init a=1 b=2 c=3 d=4 e=5 f=6 g=7 h=8 i=9 j=10\n">>,
    {ok, {init, Attrs}} = flurm_pmi_protocol:decode(Msg),
    ?assertEqual(10, map_size(Attrs)).

%%%===================================================================
%%% Roundtrip Tests for All Commands
%%%===================================================================

roundtrip_init_test() ->
    Original = #{pmi_version => 1, pmi_subversion => 1},
    Encoded = flurm_pmi_protocol:encode(init, Original),
    {ok, {init, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(1, maps:get(pmi_version, Decoded)),
    ?assertEqual(1, maps:get(pmi_subversion, Decoded)).

roundtrip_init_ack_test() ->
    Original = #{rc => 0, size => 4, rank => 2},
    Encoded = flurm_pmi_protocol:encode(init_ack, Original),
    {ok, {init_ack, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(0, maps:get(rc, Decoded)),
    ?assertEqual(4, maps:get(size, Decoded)).

roundtrip_maxes_test() ->
    Original = #{rc => 0, kvsname_max => 256},
    Encoded = flurm_pmi_protocol:encode(maxes, Original),
    {ok, {maxes, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(0, maps:get(rc, Decoded)),
    ?assertEqual(256, maps:get(kvsname_max, Decoded)).

roundtrip_appnum_test() ->
    Original = #{rc => 0, appnum => 0},
    Encoded = flurm_pmi_protocol:encode(appnum, Original),
    {ok, {appnum, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(0, maps:get(rc, Decoded)).

roundtrip_my_kvsname_test() ->
    Original = #{rc => 0, kvsname => <<"test_kvs">>},
    Encoded = flurm_pmi_protocol:encode(my_kvsname, Original),
    {ok, {my_kvsname, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(<<"test_kvs">>, maps:get(kvsname, Decoded)).

roundtrip_barrier_in_test() ->
    Encoded = flurm_pmi_protocol:encode(barrier_in, #{}),
    {ok, {barrier_in, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(#{}, Decoded).

roundtrip_barrier_out_test() ->
    Original = #{rc => 0},
    Encoded = flurm_pmi_protocol:encode(barrier_out, Original),
    {ok, {barrier_out, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(0, maps:get(rc, Decoded)).

roundtrip_put_test() ->
    Original = #{key => <<"k">>, value => <<"v">>},
    Encoded = flurm_pmi_protocol:encode(put, Original),
    {ok, {put, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(<<"k">>, maps:get(key, Decoded)),
    ?assertEqual(<<"v">>, maps:get(value, Decoded)).

roundtrip_put_ack_test() ->
    Original = #{rc => 0},
    Encoded = flurm_pmi_protocol:encode(put_ack, Original),
    {ok, {put_ack, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(0, maps:get(rc, Decoded)).

roundtrip_get_test() ->
    Original = #{key => <<"mykey">>},
    Encoded = flurm_pmi_protocol:encode(get, Original),
    {ok, {get, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(<<"mykey">>, maps:get(key, Decoded)).

roundtrip_get_ack_test() ->
    Original = #{rc => 0, value => <<"myval">>},
    Encoded = flurm_pmi_protocol:encode(get_ack, Original),
    {ok, {get_ack, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(<<"myval">>, maps:get(value, Decoded)).

roundtrip_getbyidx_test() ->
    Original = #{idx => 5},
    Encoded = flurm_pmi_protocol:encode(getbyidx, Original),
    {ok, {getbyidx, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(5, maps:get(idx, Decoded)).

roundtrip_getbyidx_ack_test() ->
    Original = #{rc => 0, key => <<"k">>, value => <<"v">>, nextidx => 6},
    Encoded = flurm_pmi_protocol:encode(getbyidx_ack, Original),
    {ok, {getbyidx_ack, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(6, maps:get(nextidx, Decoded)).

roundtrip_finalize_test() ->
    Encoded = flurm_pmi_protocol:encode(finalize, #{}),
    {ok, {finalize, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(#{}, Decoded).

roundtrip_finalize_ack_test() ->
    Original = #{rc => 0},
    Encoded = flurm_pmi_protocol:encode(finalize_ack, Original),
    {ok, {finalize_ack, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(0, maps:get(rc, Decoded)).

roundtrip_abort_test() ->
    Original = #{exitcode => 1},
    Encoded = flurm_pmi_protocol:encode(abort, Original),
    {ok, {abort, Decoded}} = flurm_pmi_protocol:decode(Encoded),
    ?assertEqual(1, maps:get(exitcode, Decoded)).

%%%===================================================================
%%% Stress Tests
%%%===================================================================

encode_decode_stress_test() ->
    %% Test many encode/decode cycles
    lists:foreach(fun(_) ->
        Attrs = #{
            rc => rand:uniform(100),
            key => <<"key_", (integer_to_binary(rand:uniform(1000)))/binary>>,
            value => <<"val_", (integer_to_binary(rand:uniform(1000)))/binary>>
        },
        Encoded = flurm_pmi_protocol:encode(put_ack, Attrs),
        {ok, {put_ack, _Decoded}} = flurm_pmi_protocol:decode(Encoded)
    end, lists:seq(1, 100)).

parse_attrs_stress_test() ->
    %% Test parsing many attributes
    AttrStr = iolist_to_binary(
        lists:join(<<" ">>,
            [<<"key", (integer_to_binary(I))/binary, "=value", (integer_to_binary(I))/binary>>
             || I <- lists:seq(1, 50)])),
    Attrs = flurm_pmi_protocol:parse_attrs(AttrStr),
    ?assertEqual(50, map_size(Attrs)).

format_attrs_stress_test() ->
    %% Test formatting many attributes
    Attrs = maps:from_list([{list_to_atom("key" ++ integer_to_list(I)), I}
                             || I <- lists:seq(1, 50)]),
    Formatted = flurm_pmi_protocol:format_attrs(Attrs),
    ?assert(byte_size(Formatted) > 0),
    %% Parse it back
    Parsed = flurm_pmi_protocol:parse_attrs(Formatted),
    ?assertEqual(50, map_size(Parsed)).
