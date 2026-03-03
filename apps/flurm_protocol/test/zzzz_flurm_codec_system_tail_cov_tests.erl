%%%-------------------------------------------------------------------
%%% @doc Tail coverage tests for flurm_codec_system edge branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_codec_system_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

codec_system_tail_cov_test_() ->
    [
     {"decode slurm_rc exact 4-byte branch", fun decode_slurm_rc_exact_test/0},
     {"decode reservation info fallback branch", fun decode_reservation_fallback_test/0},
     {"decode topo info fallback branch", fun decode_topo_fallback_test/0},
     {"decode front-end info fallback branch", fun decode_front_end_fallback_test/0},
     {"decode burst-buffer info fallback branch", fun decode_burst_buffer_fallback_test/0},
     {"decode reconfigure_with_config full parse branch",
      fun decode_reconfigure_with_config_full_parse_test/0},
     {"decode reconfigure_with_config inner catch branch",
      fun decode_reconfigure_with_config_inner_catch_test/0},
     {"decode reconfigure_with_config safe unpack error branch",
      fun decode_reconfigure_with_config_safe_unpack_error_test/0},
     {"decode reconfigure_with_config safe unpack throw branch",
      fun decode_reconfigure_with_config_safe_unpack_throw_test/0},
     {"decode settings zero-count branch", fun decode_settings_zero_count_test/0},
     {"decode settings catch fallback branch", fun decode_settings_catch_fallback_test/0},
     {"encode config_info key/value conversion branches",
      fun encode_config_info_key_value_conversion_test/0},
     {"ensure_binary variant branches", fun ensure_binary_variants_test/0}
    ].

decode_slurm_rc_exact_test() ->
    {ok, Resp} = flurm_codec_system:decode_slurm_rc_response(<<-5:32/signed-big>>),
    ?assertEqual(-5, Resp#slurm_rc_response.return_code).

decode_reservation_fallback_test() ->
    {ok, Req} = flurm_codec_system:decode_body(?REQUEST_RESERVATION_INFO, <<1, 2, 3>>),
    ?assertEqual(0, Req#reservation_info_request.show_flags),
    ?assertEqual(<<>>, Req#reservation_info_request.reservation_name).

decode_topo_fallback_test() ->
    {ok, Req} = flurm_codec_system:decode_body(?REQUEST_TOPO_INFO, <<1, 2, 3>>),
    ?assertEqual(0, Req#topo_info_request.show_flags).

decode_front_end_fallback_test() ->
    {ok, Req} = flurm_codec_system:decode_body(?REQUEST_FRONT_END_INFO, <<1, 2, 3>>),
    ?assertEqual(0, Req#front_end_info_request.show_flags).

decode_burst_buffer_fallback_test() ->
    {ok, Req} = flurm_codec_system:decode_body(?REQUEST_BURST_BUFFER_INFO, <<1, 2, 3>>),
    ?assertEqual(0, Req#burst_buffer_info_request.show_flags).

decode_reconfigure_with_config_full_parse_test() ->
    KeyBin = <<"tail_runtime_key">>,
    Bin = iolist_to_binary([
        flurm_protocol_pack:pack_string(<<"/etc/slurm.conf">>),
        <<3:32/big>>,  %% force + notify_nodes
        <<1:32/big>>,
        flurm_protocol_pack:pack_string(KeyBin),
        flurm_protocol_pack:pack_string(<<"tail_val">>)
    ]),
    {ok, Req} = flurm_codec_system:decode_reconfigure_with_config_request(Bin),
    ?assertEqual(<<"/etc/slurm.conf">>, Req#reconfigure_with_config_request.config_file),
    ?assertEqual(true, Req#reconfigure_with_config_request.force),
    ?assertEqual(true, Req#reconfigure_with_config_request.notify_nodes),
    Settings = Req#reconfigure_with_config_request.settings,
    ?assertEqual(1, map_size(Settings)),
    ?assertEqual([<<"tail_val">>], maps:values(Settings)).

decode_reconfigure_with_config_inner_catch_test() ->
    %% Valid config_file, but missing flags/settings causes inner try-catch fallback.
    Bin = flurm_protocol_pack:pack_string(<<"cfg">>),
    {ok, Req} = flurm_codec_system:decode_reconfigure_with_config_request(Bin),
    ?assertEqual(#reconfigure_with_config_request{}, Req).

decode_reconfigure_with_config_safe_unpack_error_test() ->
    %% Too short for a packed string; unpack returns {error, ...}.
    {ok, Req} = flurm_codec_system:decode_reconfigure_with_config_request(<<1, 2, 3>>),
    ?assertEqual(#reconfigure_with_config_request{}, Req).

decode_reconfigure_with_config_safe_unpack_throw_test() ->
    %% Declared string with missing null terminator triggers unpack exception.
    Bin = <<4:32/big, "abcd">>,
    {ok, Req} = flurm_codec_system:decode_reconfigure_with_config_request(Bin),
    ?assertEqual(#reconfigure_with_config_request{}, Req).

decode_settings_zero_count_test() ->
    {Settings, Rest} = flurm_codec_system:decode_settings(0, <<1, 2, 3>>, #{}),
    ?assertEqual(#{}, Settings),
    ?assertEqual(<<1, 2, 3>>, Rest).

decode_settings_catch_fallback_test() ->
    {Settings, Rest} = flurm_codec_system:decode_settings(1, <<1, 2, 3>>, #{existing => <<"x">>}),
    ?assertEqual(#{existing => <<"x">>}, Settings),
    ?assertEqual(<<>>, Rest).

encode_config_info_key_value_conversion_test() ->
    Resp = #config_info_response{
        last_update = 1,
        config = #{
            <<"bin_key">> => #{nested => true},
            "list_key" => <<"list_val">>,
            123 => 456
        }
    },
    {ok, Bin} = flurm_codec_system:encode_body(?RESPONSE_CONFIG_INFO, Resp),
    ?assert(is_binary(Bin)),
    ?assertNotEqual(nomatch, binary:match(Bin, <<"bin_key">>)),
    ?assertNotEqual(nomatch, binary:match(Bin, <<"list_key">>)),
    ?assertNotEqual(nomatch, binary:match(Bin, <<"123">>)).

ensure_binary_variants_test() ->
    ?assertEqual(<<>>, flurm_codec_system:ensure_binary(undefined)),
    ?assertEqual(<<>>, flurm_codec_system:ensure_binary(null)),
    ?assertEqual(<<"abc">>, flurm_codec_system:ensure_binary("abc")),
    ?assertEqual(<<>>, flurm_codec_system:ensure_binary(42)).
