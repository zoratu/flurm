%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_codec_system module
%%% Coverage target: 100% of all functions and branches
%%%-------------------------------------------------------------------
-module(flurm_codec_system_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_protocol.hrl").

%%%===================================================================
%%% Test Generator
%%%===================================================================

flurm_codec_system_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      %% Decode REQUEST_PING tests
      {"decode REQUEST_PING empty",
       fun decode_ping_empty/0},
      {"decode REQUEST_PING with data",
       fun decode_ping_with_data/0},

      %% Decode REQUEST_RESERVATION_INFO tests
      {"decode REQUEST_RESERVATION_INFO full",
       fun decode_reservation_info_full/0},
      {"decode REQUEST_RESERVATION_INFO with name",
       fun decode_reservation_info_with_name/0},
      {"decode REQUEST_RESERVATION_INFO empty",
       fun decode_reservation_info_empty/0},
      {"decode REQUEST_RESERVATION_INFO other",
       fun decode_reservation_info_other/0},

      %% Decode REQUEST_LICENSE_INFO tests
      {"decode REQUEST_LICENSE_INFO full",
       fun decode_license_info_full/0},
      {"decode REQUEST_LICENSE_INFO empty",
       fun decode_license_info_empty/0},
      {"decode REQUEST_LICENSE_INFO other",
       fun decode_license_info_other/0},

      %% Decode REQUEST_TOPO_INFO tests
      {"decode REQUEST_TOPO_INFO full",
       fun decode_topo_info_full/0},
      {"decode REQUEST_TOPO_INFO empty",
       fun decode_topo_info_empty/0},
      {"decode REQUEST_TOPO_INFO other",
       fun decode_topo_info_other/0},

      %% Decode REQUEST_FRONT_END_INFO tests
      {"decode REQUEST_FRONT_END_INFO full",
       fun decode_front_end_info_full/0},
      {"decode REQUEST_FRONT_END_INFO empty",
       fun decode_front_end_info_empty/0},
      {"decode REQUEST_FRONT_END_INFO other",
       fun decode_front_end_info_other/0},

      %% Decode REQUEST_BURST_BUFFER_INFO tests
      {"decode REQUEST_BURST_BUFFER_INFO full",
       fun decode_burst_buffer_info_full/0},
      {"decode REQUEST_BURST_BUFFER_INFO empty",
       fun decode_burst_buffer_info_empty/0},
      {"decode REQUEST_BURST_BUFFER_INFO other",
       fun decode_burst_buffer_info_other/0},

      %% Decode REQUEST_RECONFIGURE tests
      {"decode REQUEST_RECONFIGURE empty",
       fun decode_reconfigure_empty/0},
      {"decode REQUEST_RECONFIGURE with flags",
       fun decode_reconfigure_with_flags/0},
      {"decode REQUEST_RECONFIGURE other",
       fun decode_reconfigure_other/0},

      %% Decode REQUEST_RECONFIGURE_WITH_CONFIG tests
      {"decode REQUEST_RECONFIGURE_WITH_CONFIG full",
       fun decode_reconfigure_with_config_full/0},
      {"decode REQUEST_RECONFIGURE_WITH_CONFIG empty",
       fun decode_reconfigure_with_config_empty/0},
      {"decode REQUEST_RECONFIGURE_WITH_CONFIG with settings",
       fun decode_reconfigure_with_config_settings/0},

      %% Decode REQUEST_SHUTDOWN tests
      {"decode REQUEST_SHUTDOWN",
       fun decode_shutdown/0},

      %% Decode REQUEST_BUILD_INFO tests
      {"decode REQUEST_BUILD_INFO",
       fun decode_build_info/0},

      %% Decode REQUEST_CONFIG_INFO tests
      {"decode REQUEST_CONFIG_INFO",
       fun decode_config_info/0},

      %% Decode REQUEST_STATS_INFO tests
      {"decode REQUEST_STATS_INFO",
       fun decode_stats_info/0},

      %% Decode RESPONSE_SLURM_RC tests
      {"decode RESPONSE_SLURM_RC full with rest",
       fun decode_slurm_rc_full/0},
      {"decode RESPONSE_SLURM_RC just code",
       fun decode_slurm_rc_just_code/0},
      {"decode RESPONSE_SLURM_RC empty",
       fun decode_slurm_rc_empty/0},
      {"decode RESPONSE_SLURM_RC invalid",
       fun decode_slurm_rc_invalid/0},

      %% Decode unsupported tests
      {"decode unsupported message type",
       fun decode_unsupported/0},

      %% Encode REQUEST_PING tests
      {"encode REQUEST_PING record",
       fun encode_ping_record/0},
      {"encode REQUEST_PING non-record",
       fun encode_ping_non_record/0},

      %% Encode RESPONSE_SLURM_RC tests
      {"encode RESPONSE_SLURM_RC record",
       fun encode_slurm_rc_record/0},
      {"encode RESPONSE_SLURM_RC map",
       fun encode_slurm_rc_map/0},
      {"encode RESPONSE_SLURM_RC other",
       fun encode_slurm_rc_other/0},

      %% Encode SRUN_JOB_COMPLETE tests
      {"encode SRUN_JOB_COMPLETE record",
       fun encode_srun_job_complete_record/0},
      {"encode SRUN_JOB_COMPLETE map",
       fun encode_srun_job_complete_map/0},
      {"encode SRUN_JOB_COMPLETE other",
       fun encode_srun_job_complete_other/0},

      %% Encode SRUN_PING tests
      {"encode SRUN_PING record",
       fun encode_srun_ping_record/0},
      {"encode SRUN_PING map",
       fun encode_srun_ping_map/0},
      {"encode SRUN_PING other",
       fun encode_srun_ping_other/0},

      %% Encode RESPONSE_RESERVATION_INFO tests
      {"encode RESPONSE_RESERVATION_INFO record with reservations",
       fun encode_reservation_info_response_record/0},
      {"encode RESPONSE_RESERVATION_INFO empty",
       fun encode_reservation_info_response_empty/0},
      {"encode RESPONSE_RESERVATION_INFO non-record",
       fun encode_reservation_info_response_non_record/0},
      {"encode single reservation info",
       fun encode_single_reservation_info/0},
      {"encode single reservation info non-record",
       fun encode_single_reservation_info_non_record/0},

      %% Encode RESPONSE_LICENSE_INFO tests
      {"encode RESPONSE_LICENSE_INFO record with licenses",
       fun encode_license_info_response_record/0},
      {"encode RESPONSE_LICENSE_INFO empty",
       fun encode_license_info_response_empty/0},
      {"encode RESPONSE_LICENSE_INFO non-record",
       fun encode_license_info_response_non_record/0},
      {"encode single license info",
       fun encode_single_license_info/0},
      {"encode single license info non-record",
       fun encode_single_license_info_non_record/0},

      %% Encode RESPONSE_TOPO_INFO tests
      {"encode RESPONSE_TOPO_INFO record with topos",
       fun encode_topo_info_response_record/0},
      {"encode RESPONSE_TOPO_INFO empty",
       fun encode_topo_info_response_empty/0},
      {"encode RESPONSE_TOPO_INFO non-record",
       fun encode_topo_info_response_non_record/0},
      {"encode single topo info",
       fun encode_single_topo_info/0},
      {"encode single topo info non-record",
       fun encode_single_topo_info_non_record/0},

      %% Encode RESPONSE_FRONT_END_INFO tests
      {"encode RESPONSE_FRONT_END_INFO record with front ends",
       fun encode_front_end_info_response_record/0},
      {"encode RESPONSE_FRONT_END_INFO empty",
       fun encode_front_end_info_response_empty/0},
      {"encode RESPONSE_FRONT_END_INFO non-record",
       fun encode_front_end_info_response_non_record/0},
      {"encode single front end info",
       fun encode_single_front_end_info/0},
      {"encode single front end info non-record",
       fun encode_single_front_end_info_non_record/0},

      %% Encode RESPONSE_BURST_BUFFER_INFO tests
      {"encode RESPONSE_BURST_BUFFER_INFO record with buffers",
       fun encode_burst_buffer_info_response_record/0},
      {"encode RESPONSE_BURST_BUFFER_INFO empty",
       fun encode_burst_buffer_info_response_empty/0},
      {"encode RESPONSE_BURST_BUFFER_INFO non-record",
       fun encode_burst_buffer_info_response_non_record/0},
      {"encode single burst buffer info",
       fun encode_single_burst_buffer_info/0},
      {"encode single burst buffer info non-record",
       fun encode_single_burst_buffer_info_non_record/0},
      {"encode burst buffer pool",
       fun encode_burst_buffer_pool/0},
      {"encode burst buffer pool non-record",
       fun encode_burst_buffer_pool_non_record/0},

      %% Encode RESPONSE_BUILD_INFO tests
      {"encode RESPONSE_BUILD_INFO record",
       fun encode_build_info_response_record/0},
      {"encode RESPONSE_BUILD_INFO non-record",
       fun encode_build_info_response_non_record/0},

      %% Encode RESPONSE_CONFIG_INFO tests
      {"encode RESPONSE_CONFIG_INFO record",
       fun encode_config_info_response_record/0},
      {"encode RESPONSE_CONFIG_INFO non-record",
       fun encode_config_info_response_non_record/0},

      %% Encode RESPONSE_STATS_INFO tests
      {"encode RESPONSE_STATS_INFO record",
       fun encode_stats_info_response_record/0},
      {"encode RESPONSE_STATS_INFO non-record",
       fun encode_stats_info_response_non_record/0},

      %% Encode reconfigure response tests
      {"encode reconfigure response record",
       fun encode_reconfigure_response_record/0},
      {"encode reconfigure response non-record",
       fun encode_reconfigure_response_non_record/0},

      %% Encode unsupported tests
      {"encode unsupported message type",
       fun encode_unsupported/0},

      %% Edge cases
      {"roundtrip ping",
       fun roundtrip_ping/0},
      {"roundtrip slurm rc",
       fun roundtrip_slurm_rc/0},
      {"encode multiple reservations",
       fun encode_multiple_reservations/0},
      {"encode multiple licenses",
       fun encode_multiple_licenses/0},
      {"encode complex build info",
       fun encode_complex_build_info/0},
      {"encode config with various types",
       fun encode_config_various_types/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%%===================================================================
%%% Helper to pack a string like flurm_protocol_pack
%%%===================================================================

pack_string(undefined) -> <<16#FFFFFFFF:32/big>>;
pack_string(<<>>) -> <<0:32/big>>;
pack_string(Str) when is_binary(Str) ->
    Len = byte_size(Str),
    <<Len:32/big, Str/binary>>;
pack_string(Str) when is_list(Str) ->
    pack_string(list_to_binary(Str)).

%%%===================================================================
%%% Decode REQUEST_PING Tests
%%%===================================================================

decode_ping_empty() ->
    Result = flurm_codec_system:decode_body(?REQUEST_PING, <<>>),
    ?assertMatch({ok, #ping_request{}}, Result).

decode_ping_with_data() ->
    Result = flurm_codec_system:decode_body(?REQUEST_PING, <<"some_data">>),
    ?assertMatch({ok, #ping_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_RESERVATION_INFO Tests
%%%===================================================================

decode_reservation_info_full() ->
    Binary = <<255:32/big, (pack_string(<<"resv1">>))/binary>>,
    Result = flurm_codec_system:decode_body(?REQUEST_RESERVATION_INFO, Binary),
    ?assertMatch({ok, #reservation_info_request{show_flags = 255, reservation_name = <<"resv1">>}}, Result).

decode_reservation_info_with_name() ->
    Binary = <<0:32/big, (pack_string(<<"maintenance">>))/binary>>,
    Result = flurm_codec_system:decode_body(?REQUEST_RESERVATION_INFO, Binary),
    ?assertMatch({ok, #reservation_info_request{reservation_name = <<"maintenance">>}}, Result).

decode_reservation_info_empty() ->
    Result = flurm_codec_system:decode_body(?REQUEST_RESERVATION_INFO, <<>>),
    ?assertMatch({ok, #reservation_info_request{}}, Result).

decode_reservation_info_other() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_system:decode_body(?REQUEST_RESERVATION_INFO, Binary),
    ?assertMatch({ok, #reservation_info_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_LICENSE_INFO Tests
%%%===================================================================

decode_license_info_full() ->
    Binary = <<123:32/big, "extra">>,
    Result = flurm_codec_system:decode_body(?REQUEST_LICENSE_INFO, Binary),
    ?assertMatch({ok, #license_info_request{show_flags = 123}}, Result).

decode_license_info_empty() ->
    Result = flurm_codec_system:decode_body(?REQUEST_LICENSE_INFO, <<>>),
    ?assertMatch({ok, #license_info_request{}}, Result).

decode_license_info_other() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_system:decode_body(?REQUEST_LICENSE_INFO, Binary),
    ?assertMatch({ok, #license_info_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_TOPO_INFO Tests
%%%===================================================================

decode_topo_info_full() ->
    Binary = <<456:32/big, "extra">>,
    Result = flurm_codec_system:decode_body(?REQUEST_TOPO_INFO, Binary),
    ?assertMatch({ok, #topo_info_request{show_flags = 456}}, Result).

decode_topo_info_empty() ->
    Result = flurm_codec_system:decode_body(?REQUEST_TOPO_INFO, <<>>),
    ?assertMatch({ok, #topo_info_request{}}, Result).

decode_topo_info_other() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_system:decode_body(?REQUEST_TOPO_INFO, Binary),
    ?assertMatch({ok, #topo_info_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_FRONT_END_INFO Tests
%%%===================================================================

decode_front_end_info_full() ->
    Binary = <<789:32/big, "extra">>,
    Result = flurm_codec_system:decode_body(?REQUEST_FRONT_END_INFO, Binary),
    ?assertMatch({ok, #front_end_info_request{show_flags = 789}}, Result).

decode_front_end_info_empty() ->
    Result = flurm_codec_system:decode_body(?REQUEST_FRONT_END_INFO, <<>>),
    ?assertMatch({ok, #front_end_info_request{}}, Result).

decode_front_end_info_other() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_system:decode_body(?REQUEST_FRONT_END_INFO, Binary),
    ?assertMatch({ok, #front_end_info_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_BURST_BUFFER_INFO Tests
%%%===================================================================

decode_burst_buffer_info_full() ->
    Binary = <<100:32/big, "extra">>,
    Result = flurm_codec_system:decode_body(?REQUEST_BURST_BUFFER_INFO, Binary),
    ?assertMatch({ok, #burst_buffer_info_request{show_flags = 100}}, Result).

decode_burst_buffer_info_empty() ->
    Result = flurm_codec_system:decode_body(?REQUEST_BURST_BUFFER_INFO, <<>>),
    ?assertMatch({ok, #burst_buffer_info_request{}}, Result).

decode_burst_buffer_info_other() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_system:decode_body(?REQUEST_BURST_BUFFER_INFO, Binary),
    ?assertMatch({ok, #burst_buffer_info_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_RECONFIGURE Tests
%%%===================================================================

decode_reconfigure_empty() ->
    Result = flurm_codec_system:decode_body(?REQUEST_RECONFIGURE, <<>>),
    ?assertMatch({ok, #reconfigure_request{}}, Result).

decode_reconfigure_with_flags() ->
    Binary = <<255:32/big, "extra">>,
    Result = flurm_codec_system:decode_body(?REQUEST_RECONFIGURE, Binary),
    ?assertMatch({ok, #reconfigure_request{flags = 255}}, Result).

decode_reconfigure_other() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_system:decode_body(?REQUEST_RECONFIGURE, Binary),
    ?assertMatch({ok, #reconfigure_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_RECONFIGURE_WITH_CONFIG Tests
%%%===================================================================

decode_reconfigure_with_config_full() ->
    Binary = <<
        (pack_string(<<"/etc/slurm/slurm.conf">>))/binary,
        3:32/big,  % flags: force=1, notify_nodes=1
        0:32/big   % settings_count
    >>,
    Result = flurm_codec_system:decode_body(?REQUEST_RECONFIGURE_WITH_CONFIG, Binary),
    ?assertMatch({ok, #reconfigure_with_config_request{force = true, notify_nodes = true}}, Result).

decode_reconfigure_with_config_empty() ->
    Result = flurm_codec_system:decode_body(?REQUEST_RECONFIGURE_WITH_CONFIG, <<>>),
    ?assertMatch({ok, #reconfigure_with_config_request{}}, Result).

decode_reconfigure_with_config_settings() ->
    Key1 = <<"SchedulerType">>,
    Val1 = <<"sched/backfill">>,
    Binary = <<
        (pack_string(<<>>))/binary,
        0:32/big,  % flags
        1:32/big,  % settings_count
        (pack_string(Key1))/binary,
        (pack_string(Val1))/binary
    >>,
    Result = flurm_codec_system:decode_body(?REQUEST_RECONFIGURE_WITH_CONFIG, Binary),
    ?assertMatch({ok, #reconfigure_with_config_request{}}, Result).

%%%===================================================================
%%% Decode REQUEST_SHUTDOWN Tests
%%%===================================================================

decode_shutdown() ->
    Binary = <<"some_data">>,
    Result = flurm_codec_system:decode_body(?REQUEST_SHUTDOWN, Binary),
    ?assertEqual({ok, Binary}, Result).

%%%===================================================================
%%% Decode REQUEST_BUILD_INFO Tests
%%%===================================================================

decode_build_info() ->
    Result = flurm_codec_system:decode_body(?REQUEST_BUILD_INFO, <<"any">>),
    ?assertEqual({ok, #{}}, Result).

%%%===================================================================
%%% Decode REQUEST_CONFIG_INFO Tests
%%%===================================================================

decode_config_info() ->
    Result = flurm_codec_system:decode_body(?REQUEST_CONFIG_INFO, <<"any">>),
    ?assertEqual({ok, #{}}, Result).

%%%===================================================================
%%% Decode REQUEST_STATS_INFO Tests
%%%===================================================================

decode_stats_info() ->
    Binary = <<"stats_request_data">>,
    Result = flurm_codec_system:decode_body(?REQUEST_STATS_INFO, Binary),
    ?assertEqual({ok, Binary}, Result).

%%%===================================================================
%%% Decode RESPONSE_SLURM_RC Tests
%%%===================================================================

decode_slurm_rc_full() ->
    Binary = <<0:32/signed-big, "rest">>,
    Result = flurm_codec_system:decode_body(?RESPONSE_SLURM_RC, Binary),
    ?assertMatch({ok, #slurm_rc_response{return_code = 0}}, Result).

decode_slurm_rc_just_code() ->
    Binary = <<(-1):32/signed-big>>,
    Result = flurm_codec_system:decode_body(?RESPONSE_SLURM_RC, Binary),
    ?assertMatch({ok, #slurm_rc_response{return_code = -1}}, Result).

decode_slurm_rc_empty() ->
    Result = flurm_codec_system:decode_body(?RESPONSE_SLURM_RC, <<>>),
    ?assertMatch({ok, #slurm_rc_response{}}, Result).

decode_slurm_rc_invalid() ->
    Binary = <<1, 2, 3>>,
    Result = flurm_codec_system:decode_body(?RESPONSE_SLURM_RC, Binary),
    ?assertMatch({error, _}, Result).

%%%===================================================================
%%% Decode Unsupported Tests
%%%===================================================================

decode_unsupported() ->
    Result = flurm_codec_system:decode_body(99999, <<1, 2, 3>>),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% Encode REQUEST_PING Tests
%%%===================================================================

encode_ping_record() ->
    Req = #ping_request{},
    Result = flurm_codec_system:encode_body(?REQUEST_PING, Req),
    ?assertEqual({ok, <<>>}, Result).

encode_ping_non_record() ->
    Result = flurm_codec_system:encode_body(?REQUEST_PING, not_a_record),
    ?assertEqual({ok, <<>>}, Result).

%%%===================================================================
%%% Encode RESPONSE_SLURM_RC Tests
%%%===================================================================

encode_slurm_rc_record() ->
    Resp = #slurm_rc_response{return_code = 123},
    Result = flurm_codec_system:encode_body(?RESPONSE_SLURM_RC, Resp),
    ?assertEqual({ok, <<123:32/signed-big>>}, Result).

encode_slurm_rc_map() ->
    Map = #{return_code => -5},
    Result = flurm_codec_system:encode_body(?RESPONSE_SLURM_RC, Map),
    ?assertEqual({ok, <<(-5):32/signed-big>>}, Result).

encode_slurm_rc_other() ->
    Result = flurm_codec_system:encode_body(?RESPONSE_SLURM_RC, not_a_record),
    ?assertEqual({ok, <<0:32/signed-big>>}, Result).

%%%===================================================================
%%% Encode SRUN_JOB_COMPLETE Tests
%%%===================================================================

encode_srun_job_complete_record() ->
    Msg = #srun_job_complete{job_id = 100, step_id = 5},
    Result = flurm_codec_system:encode_body(?SRUN_JOB_COMPLETE, Msg),
    ?assertEqual({ok, <<100:32/big, 5:32/big>>}, Result).

encode_srun_job_complete_map() ->
    Map = #{job_id => 200, step_id => 10},
    Result = flurm_codec_system:encode_body(?SRUN_JOB_COMPLETE, Map),
    ?assertEqual({ok, <<200:32/big, 10:32/big>>}, Result).

encode_srun_job_complete_other() ->
    Result = flurm_codec_system:encode_body(?SRUN_JOB_COMPLETE, not_a_record),
    ?assertEqual({ok, <<0:32, 0:32>>}, Result).

%%%===================================================================
%%% Encode SRUN_PING Tests
%%%===================================================================

encode_srun_ping_record() ->
    Msg = #srun_ping{job_id = 300, step_id = 15},
    Result = flurm_codec_system:encode_body(?SRUN_PING, Msg),
    ?assertEqual({ok, <<300:32/big, 15:32/big>>}, Result).

encode_srun_ping_map() ->
    Map = #{job_id => 400, step_id => 20},
    Result = flurm_codec_system:encode_body(?SRUN_PING, Map),
    ?assertEqual({ok, <<400:32/big, 20:32/big>>}, Result).

encode_srun_ping_other() ->
    Result = flurm_codec_system:encode_body(?SRUN_PING, not_a_record),
    ?assertEqual({ok, <<0:32, 0:32>>}, Result).

%%%===================================================================
%%% Encode RESPONSE_RESERVATION_INFO Tests
%%%===================================================================

encode_reservation_info_response_record() ->
    Resvs = [
        #reservation_info{
            name = <<"resv1">>,
            accounts = <<"acct1,acct2">>,
            burst_buffer = <<>>,
            core_cnt = 16,
            core_spec_cnt = 0,
            end_time = 1234567890,
            features = <<"gpu">>,
            flags = 0,
            groups = <<"grp1">>,
            licenses = <<"matlab:5">>,
            max_start_delay = 0,
            node_cnt = 4,
            node_list = <<"node[01-04]">>,
            partition = <<"default">>,
            purge_comp_time = 0,
            resv_watts = 0,
            start_time = 1234560000,
            tres_str = <<"cpu=64,mem=128G">>,
            users = <<"user1,user2">>
        }
    ],
    Resp = #reservation_info_response{
        last_update = 1234567890,
        reservation_count = 1,
        reservations = Resvs
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_RESERVATION_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 50).

encode_reservation_info_response_empty() ->
    Resp = #reservation_info_response{
        last_update = 1234567890,
        reservation_count = 0,
        reservations = []
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_RESERVATION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_reservation_info_response_non_record() ->
    Result = flurm_codec_system:encode_body(?RESPONSE_RESERVATION_INFO, not_a_record),
    ?assertEqual({ok, <<0:32, 0:64>>}, Result).

encode_single_reservation_info() ->
    Resv = #reservation_info{
        name = <<"test_resv">>,
        accounts = <<"*">>,
        features = <<>>,
        node_list = <<"all">>,
        users = <<"*">>
    },
    Resp = #reservation_info_response{
        last_update = 0,
        reservation_count = 1,
        reservations = [Resv]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_RESERVATION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_single_reservation_info_non_record() ->
    Resp = #reservation_info_response{
        last_update = 0,
        reservation_count = 1,
        reservations = [not_a_record]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_RESERVATION_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_LICENSE_INFO Tests
%%%===================================================================

encode_license_info_response_record() ->
    Lics = [
        #license_info{
            name = <<"matlab">>,
            total = 100,
            in_use = 50,
            available = 50,
            reserved = 10,
            remote = 0
        },
        #license_info{
            name = <<"ansys">>,
            total = 20,
            in_use = 5,
            available = 15,
            reserved = 0,
            remote = 1
        }
    ],
    Resp = #license_info_response{
        last_update = 1234567890,
        license_count = 2,
        licenses = Lics
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_LICENSE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_license_info_response_empty() ->
    Resp = #license_info_response{
        last_update = 1234567890,
        license_count = 0,
        licenses = []
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_LICENSE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_license_info_response_non_record() ->
    Result = flurm_codec_system:encode_body(?RESPONSE_LICENSE_INFO, not_a_record),
    ?assertEqual({ok, <<0:64, 0:32>>}, Result).

encode_single_license_info() ->
    Lic = #license_info{
        name = <<"test_license">>,
        total = 10,
        in_use = 5,
        available = 5,
        reserved = 0,
        remote = 0
    },
    Resp = #license_info_response{
        last_update = 0,
        license_count = 1,
        licenses = [Lic]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_LICENSE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_single_license_info_non_record() ->
    Resp = #license_info_response{
        last_update = 0,
        license_count = 1,
        licenses = [not_a_record]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_LICENSE_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_TOPO_INFO Tests
%%%===================================================================

encode_topo_info_response_record() ->
    Topos = [
        #topo_info{
            level = 0,
            link_speed = 100000,
            name = <<"switch0">>,
            nodes = <<"node[01-16]">>,
            switches = <<>>
        },
        #topo_info{
            level = 1,
            link_speed = 200000,
            name = <<"router0">>,
            nodes = <<>>,
            switches = <<"switch0,switch1">>
        }
    ],
    Resp = #topo_info_response{
        topo_count = 2,
        topos = Topos
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_TOPO_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_topo_info_response_empty() ->
    Resp = #topo_info_response{
        topo_count = 0,
        topos = []
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_TOPO_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_topo_info_response_non_record() ->
    Result = flurm_codec_system:encode_body(?RESPONSE_TOPO_INFO, not_a_record),
    ?assertEqual({ok, <<0:32>>}, Result).

encode_single_topo_info() ->
    Topo = #topo_info{
        level = 0,
        link_speed = 50000,
        name = <<"leaf">>,
        nodes = <<"node01">>,
        switches = <<>>
    },
    Resp = #topo_info_response{
        topo_count = 1,
        topos = [Topo]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_TOPO_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_single_topo_info_non_record() ->
    Resp = #topo_info_response{
        topo_count = 1,
        topos = [not_a_record]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_TOPO_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_FRONT_END_INFO Tests
%%%===================================================================

encode_front_end_info_response_record() ->
    FEs = [
        #front_end_info{
            allow_groups = <<"users">>,
            allow_users = <<"*">>,
            boot_time = 1234500000,
            deny_groups = <<>>,
            deny_users = <<>>,
            name = <<"login01">>,
            node_state = 2,
            reason = <<>>,
            reason_time = 0,
            reason_uid = 0,
            slurmd_start_time = 1234500001,
            version = <<"22.05.9">>
        }
    ],
    Resp = #front_end_info_response{
        last_update = 1234567890,
        front_end_count = 1,
        front_ends = FEs
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_FRONT_END_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_front_end_info_response_empty() ->
    Resp = #front_end_info_response{
        last_update = 1234567890,
        front_end_count = 0,
        front_ends = []
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_FRONT_END_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_front_end_info_response_non_record() ->
    Result = flurm_codec_system:encode_body(?RESPONSE_FRONT_END_INFO, not_a_record),
    ?assertEqual({ok, <<0:32, 0:64>>}, Result).

encode_single_front_end_info() ->
    FE = #front_end_info{
        name = <<"login02">>,
        version = <<"22.05.0">>,
        node_state = 2
    },
    Resp = #front_end_info_response{
        last_update = 0,
        front_end_count = 1,
        front_ends = [FE]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_FRONT_END_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_single_front_end_info_non_record() ->
    Resp = #front_end_info_response{
        last_update = 0,
        front_end_count = 1,
        front_ends = [not_a_record]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_FRONT_END_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_BURST_BUFFER_INFO Tests
%%%===================================================================

encode_burst_buffer_info_response_record() ->
    Pools = [
        #burst_buffer_pool{
            name = <<"pool1">>,
            total_space = 1000000000000,
            granularity = 1073741824,
            unfree_space = 500000000000,
            used_space = 300000000000
        }
    ],
    BBs = [
        #burst_buffer_info{
            name = <<"datawarp">>,
            default_pool = <<"pool1">>,
            allow_users = <<"*">>,
            create_buffer = <<"/opt/cray/dw/script/create">>,
            deny_users = <<>>,
            destroy_buffer = <<"/opt/cray/dw/script/destroy">>,
            flags = 0,
            get_sys_state = <<"/opt/cray/dw/script/state">>,
            get_sys_status = <<"/opt/cray/dw/script/status">>,
            granularity = 1073741824,
            pool_cnt = 1,
            pools = Pools,
            other_timeout = 300,
            stage_in_timeout = 600,
            stage_out_timeout = 600,
            start_stage_in = <<"/opt/cray/dw/script/stage_in">>,
            start_stage_out = <<"/opt/cray/dw/script/stage_out">>,
            stop_stage_in = <<"/opt/cray/dw/script/stop_in">>,
            stop_stage_out = <<"/opt/cray/dw/script/stop_out">>,
            total_space = 1000000000000,
            unfree_space = 500000000000,
            used_space = 300000000000,
            validate_timeout = 60
        }
    ],
    Resp = #burst_buffer_info_response{
        last_update = 1234567890,
        burst_buffer_count = 1,
        burst_buffers = BBs
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_BURST_BUFFER_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 100).

encode_burst_buffer_info_response_empty() ->
    Resp = #burst_buffer_info_response{
        last_update = 1234567890,
        burst_buffer_count = 0,
        burst_buffers = []
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_BURST_BUFFER_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_burst_buffer_info_response_non_record() ->
    Result = flurm_codec_system:encode_body(?RESPONSE_BURST_BUFFER_INFO, not_a_record),
    ?assertEqual({ok, <<0:32, 0:64>>}, Result).

encode_single_burst_buffer_info() ->
    BB = #burst_buffer_info{
        name = <<"simple_bb">>,
        pool_cnt = 0,
        pools = []
    },
    Resp = #burst_buffer_info_response{
        last_update = 0,
        burst_buffer_count = 1,
        burst_buffers = [BB]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_BURST_BUFFER_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_single_burst_buffer_info_non_record() ->
    Resp = #burst_buffer_info_response{
        last_update = 0,
        burst_buffer_count = 1,
        burst_buffers = [not_a_record]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_BURST_BUFFER_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_burst_buffer_pool() ->
    Pool = #burst_buffer_pool{
        name = <<"test_pool">>,
        granularity = 1048576,
        total_space = 100000000,
        unfree_space = 50000000,
        used_space = 30000000
    },
    BB = #burst_buffer_info{
        name = <<"bb_with_pool">>,
        pool_cnt = 1,
        pools = [Pool]
    },
    Resp = #burst_buffer_info_response{
        last_update = 0,
        burst_buffer_count = 1,
        burst_buffers = [BB]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_BURST_BUFFER_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_burst_buffer_pool_non_record() ->
    BB = #burst_buffer_info{
        name = <<"bb_invalid_pool">>,
        pool_cnt = 1,
        pools = [not_a_record]
    },
    Resp = #burst_buffer_info_response{
        last_update = 0,
        burst_buffer_count = 1,
        burst_buffers = [BB]
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_BURST_BUFFER_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Encode RESPONSE_BUILD_INFO Tests
%%%===================================================================

encode_build_info_response_record() ->
    Resp = #build_info_response{
        version = <<"22.05.9">>,
        version_major = 22,
        version_minor = 5,
        version_micro = 9,
        release = <<"flurm-0.1.0">>,
        build_host = <<"buildhost">>,
        build_user = <<"builder">>,
        build_date = <<"2024-01-01">>,
        cluster_name = <<"test_cluster">>,
        control_machine = <<"controller">>,
        backup_controller = <<"backup">>,
        accounting_storage_type = <<"accounting_storage/none">>,
        auth_type = <<"auth/munge">>,
        slurm_user_name = <<"slurm">>,
        slurmd_user_name = <<"root">>,
        slurmctld_host = <<"controller.local">>,
        slurmctld_port = 6817,
        slurmd_port = 6818,
        spool_dir = <<"/var/spool/slurmd">>,
        state_save_location = <<"/var/spool/slurmctld">>,
        plugin_dir = <<"/usr/lib64/slurm">>,
        priority_type = <<"priority/multifactor">>,
        select_type = <<"select/cons_tres">>,
        scheduler_type = <<"sched/backfill">>,
        job_comp_type = <<"jobcomp/none">>
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_BUILD_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 200).

encode_build_info_response_non_record() ->
    Result = flurm_codec_system:encode_body(?RESPONSE_BUILD_INFO, not_a_record),
    ?assertEqual({ok, <<>>}, Result).

%%%===================================================================
%%% Encode RESPONSE_CONFIG_INFO Tests
%%%===================================================================

encode_config_info_response_record() ->
    Config = #{
        'ClusterName' => <<"test_cluster">>,
        'ControlMachine' => <<"controller">>,
        'SlurmctldPort' => 6817,
        'SlurmdPort' => 6818,
        scheduler_type => <<"sched/backfill">>
    },
    Resp = #config_info_response{
        last_update = 1234567890,
        config = Config
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_CONFIG_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_config_info_response_non_record() ->
    Result = flurm_codec_system:encode_body(?RESPONSE_CONFIG_INFO, not_a_record),
    ?assertEqual({ok, <<>>}, Result).

%%%===================================================================
%%% Encode RESPONSE_STATS_INFO Tests
%%%===================================================================

encode_stats_info_response_record() ->
    Resp = #stats_info_response{
        parts_packed = 0,
        req_time = 1234567890,
        req_time_start = 1234560000,
        server_thread_count = 10,
        agent_queue_size = 5,
        agent_count = 2,
        agent_thread_count = 4,
        dbd_agent_queue_size = 0,
        jobs_submitted = 1000,
        jobs_started = 950,
        jobs_completed = 900,
        jobs_canceled = 30,
        jobs_failed = 20,
        jobs_pending = 50,
        jobs_running = 100,
        schedule_cycle_max = 5000,
        schedule_cycle_last = 1000,
        schedule_cycle_sum = 100000,
        schedule_cycle_counter = 1000,
        schedule_cycle_depth = 100,
        schedule_queue_len = 50,
        bf_backfilled_jobs = 200,
        bf_last_backfilled_jobs = 5,
        bf_cycle_counter = 500,
        bf_cycle_sum = 50000,
        bf_cycle_last = 100,
        bf_cycle_max = 500,
        bf_depth_sum = 10000,
        bf_depth_try_sum = 8000,
        bf_queue_len = 20,
        bf_queue_len_sum = 5000,
        bf_when_last_cycle = 1234567800,
        bf_active = true,
        rpc_type_stats = [],
        rpc_user_stats = []
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_STATS_INFO, Resp),
    ?assert(is_binary(Binary)),
    ?assert(byte_size(Binary) > 100).

encode_stats_info_response_non_record() ->
    Result = flurm_codec_system:encode_body(?RESPONSE_STATS_INFO, not_a_record),
    ?assertEqual({ok, <<>>}, Result).

%%%===================================================================
%%% Encode Reconfigure Response Tests
%%%===================================================================

encode_reconfigure_response_record() ->
    Resp = #reconfigure_response{
        return_code = 0,
        message = <<"Configuration updated">>,
        changed_keys = [scheduler_type, priority_type],
        version = 2
    },
    {ok, Binary} = flurm_codec_system:encode_reconfigure_response(Resp),
    ?assert(is_binary(Binary)).

encode_reconfigure_response_non_record() ->
    {ok, Binary} = flurm_codec_system:encode_reconfigure_response(not_a_record),
    ?assertEqual(<<0:32/big-signed, 0:32, 0:32, 0:32>>, Binary).

%%%===================================================================
%%% Encode Unsupported Tests
%%%===================================================================

encode_unsupported() ->
    Result = flurm_codec_system:encode_body(99999, some_body),
    ?assertEqual(unsupported, Result).

%%%===================================================================
%%% Edge Cases
%%%===================================================================

roundtrip_ping() ->
    Req = #ping_request{},
    {ok, Encoded} = flurm_codec_system:encode_body(?REQUEST_PING, Req),
    {ok, Decoded} = flurm_codec_system:decode_body(?REQUEST_PING, Encoded),
    ?assertMatch(#ping_request{}, Decoded).

roundtrip_slurm_rc() ->
    Resp = #slurm_rc_response{return_code = -123},
    {ok, Encoded} = flurm_codec_system:encode_body(?RESPONSE_SLURM_RC, Resp),
    {ok, Decoded} = flurm_codec_system:decode_body(?RESPONSE_SLURM_RC, Encoded),
    ?assertEqual(-123, Decoded#slurm_rc_response.return_code).

encode_multiple_reservations() ->
    Resvs = [
        #reservation_info{name = <<"resv1">>, start_time = 1000, end_time = 2000},
        #reservation_info{name = <<"resv2">>, start_time = 3000, end_time = 4000},
        #reservation_info{name = <<"resv3">>, start_time = 5000, end_time = 6000}
    ],
    Resp = #reservation_info_response{
        last_update = 1234567890,
        reservation_count = 3,
        reservations = Resvs
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_RESERVATION_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_multiple_licenses() ->
    Lics = [
        #license_info{name = <<"lic1">>, total = 10, in_use = 5},
        #license_info{name = <<"lic2">>, total = 20, in_use = 10},
        #license_info{name = <<"lic3">>, total = 30, in_use = 15},
        #license_info{name = <<"lic4">>, total = 40, in_use = 20}
    ],
    Resp = #license_info_response{
        last_update = 1234567890,
        license_count = 4,
        licenses = Lics
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_LICENSE_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_complex_build_info() ->
    Resp = #build_info_response{
        version = <<"22.05.9-flurm">>,
        cluster_name = <<"complex_cluster_with_long_name">>,
        control_machine = <<"controller-001.datacenter.example.com">>,
        auth_type = <<"auth/munge">>,
        scheduler_type = <<"sched/backfill">>,
        select_type = <<"select/cons_tres">>,
        slurmctld_port = 6817,
        slurmd_port = 6818
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_BUILD_INFO, Resp),
    ?assert(is_binary(Binary)).

encode_config_various_types() ->
    Config = #{
        string_key => <<"string_value">>,
        list_key => "list_value",
        integer_key => 12345,
        atom_key => some_atom,
        tuple_key => {1, 2, 3}
    },
    Resp = #config_info_response{
        last_update = 1234567890,
        config = Config
    },
    {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_CONFIG_INFO, Resp),
    ?assert(is_binary(Binary)).

%%%===================================================================
%%% Additional Edge Cases
%%%===================================================================

%% Test negative return codes
negative_rc_test_() ->
    Codes = [-1, -2, -127, -255, -1000],
    [
        {lists:flatten(io_lib:format("encode slurm rc ~p", [Code])),
         fun() ->
             Resp = #slurm_rc_response{return_code = Code},
             {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_SLURM_RC, Resp),
             {ok, Decoded} = flurm_codec_system:decode_body(?RESPONSE_SLURM_RC, Binary),
             ?assertEqual(Code, Decoded#slurm_rc_response.return_code)
         end}
    || Code <- Codes].

%% Test bf_active false
bf_inactive_test_() ->
    {"encode stats with bf_active false",
     fun() ->
         Resp = #stats_info_response{bf_active = false},
         {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_STATS_INFO, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test empty config
empty_config_test_() ->
    {"encode empty config",
     fun() ->
         Resp = #config_info_response{
             last_update = 0,
             config = #{}
         },
         {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_CONFIG_INFO, Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test reconfigure with empty changed_keys
empty_changed_keys_test_() ->
    {"encode reconfigure with no changes",
     fun() ->
         Resp = #reconfigure_response{
             return_code = 0,
             message = <<"No changes">>,
             changed_keys = [],
             version = 1
         },
         {ok, Binary} = flurm_codec_system:encode_reconfigure_response(Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test reconfigure with negative return code
reconfigure_error_test_() ->
    {"encode reconfigure error response",
     fun() ->
         Resp = #reconfigure_response{
             return_code = -1,
             message = <<"Configuration error">>,
             changed_keys = [],
             version = 0
         },
         {ok, Binary} = flurm_codec_system:encode_reconfigure_response(Resp),
         ?assert(is_binary(Binary))
     end}.

%% Test large values
large_values_test_() ->
    {"encode stats with large values",
     fun() ->
         Resp = #stats_info_response{
             jobs_submitted = 16#FFFFFFFF,
             jobs_started = 16#FFFFFFFF,
             schedule_cycle_sum = 16#FFFFFFFFFFFFFFFF,
             bf_cycle_sum = 16#FFFFFFFFFFFFFFFF
         },
         {ok, Binary} = flurm_codec_system:encode_body(?RESPONSE_STATS_INFO, Resp),
         ?assert(is_binary(Binary))
     end}.
