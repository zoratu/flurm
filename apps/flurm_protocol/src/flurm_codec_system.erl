%%%-------------------------------------------------------------------
%%% @doc FLURM Codec - System Message Encoding/Decoding
%%%
%%% This module handles encoding and decoding of system-related SLURM
%%% protocol messages:
%%%   - REQUEST_PING (1008)
%%%   - REQUEST_BUILD_INFO (2001) / RESPONSE_BUILD_INFO (2002)
%%%   - REQUEST_CONFIG_INFO (2016) / RESPONSE_CONFIG_INFO (2017)
%%%   - REQUEST_STATS_INFO (2026) / RESPONSE_STATS_INFO (2027)
%%%   - REQUEST_SHUTDOWN (1005)
%%%   - REQUEST_RECONFIGURE (1003) / REQUEST_RECONFIGURE_WITH_CONFIG (1004)
%%%   - REQUEST_RESERVATION_INFO (2012) / RESPONSE_RESERVATION_INFO (2013)
%%%   - REQUEST_LICENSE_INFO (1017) / RESPONSE_LICENSE_INFO (1018)
%%%   - REQUEST_TOPO_INFO (2018) / RESPONSE_TOPO_INFO (2019)
%%%   - REQUEST_FRONT_END_INFO (2028) / RESPONSE_FRONT_END_INFO (2029)
%%%   - REQUEST_BURST_BUFFER_INFO (2020) / RESPONSE_BURST_BUFFER_INFO (2021)
%%%   - RESPONSE_SLURM_RC (8001)
%%%   - SRUN_JOB_COMPLETE (7004)
%%%   - SRUN_PING (7001)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_codec_system).

-export([
    decode_body/2,
    encode_body/2,
    encode_reconfigure_response/1
]).

-include("flurm_protocol.hrl").

%%%===================================================================
%%% Decode Functions
%%%===================================================================

%% REQUEST_PING (1008)
decode_body(?REQUEST_PING, <<>>) ->
    {ok, #ping_request{}};
decode_body(?REQUEST_PING, _Binary) ->
    {ok, #ping_request{}};

%% REQUEST_RESERVATION_INFO (2012)
decode_body(?REQUEST_RESERVATION_INFO, Binary) ->
    decode_reservation_info_request(Binary);

%% REQUEST_LICENSE_INFO (1017)
decode_body(?REQUEST_LICENSE_INFO, Binary) ->
    decode_license_info_request(Binary);

%% REQUEST_TOPO_INFO (2018)
decode_body(?REQUEST_TOPO_INFO, Binary) ->
    decode_topo_info_request(Binary);

%% REQUEST_FRONT_END_INFO (2028)
decode_body(?REQUEST_FRONT_END_INFO, Binary) ->
    decode_front_end_info_request(Binary);

%% REQUEST_BURST_BUFFER_INFO (2020)
decode_body(?REQUEST_BURST_BUFFER_INFO, Binary) ->
    decode_burst_buffer_info_request(Binary);

%% REQUEST_RECONFIGURE (1003)
decode_body(?REQUEST_RECONFIGURE, Binary) ->
    decode_reconfigure_request(Binary);

%% REQUEST_RECONFIGURE_WITH_CONFIG (1004)
decode_body(?REQUEST_RECONFIGURE_WITH_CONFIG, Binary) ->
    decode_reconfigure_with_config_request(Binary);

%% REQUEST_SHUTDOWN (1005)
decode_body(?REQUEST_SHUTDOWN, Binary) ->
    {ok, Binary};

%% REQUEST_BUILD_INFO (2001)
decode_body(?REQUEST_BUILD_INFO, _Binary) ->
    {ok, #{}};

%% REQUEST_CONFIG_INFO (2016)
decode_body(?REQUEST_CONFIG_INFO, _Binary) ->
    {ok, #{}};

%% REQUEST_STATS_INFO (2026)
decode_body(?REQUEST_STATS_INFO, Binary) ->
    {ok, Binary};

%% RESPONSE_SLURM_RC (8001)
decode_body(?RESPONSE_SLURM_RC, Binary) ->
    decode_slurm_rc_response(Binary);

%% Unsupported message type
decode_body(_MsgType, _Binary) ->
    unsupported.

%%%===================================================================
%%% Encode Functions
%%%===================================================================

%% REQUEST_PING (1008)
encode_body(?REQUEST_PING, #ping_request{}) ->
    {ok, <<>>};
encode_body(?REQUEST_PING, _) ->
    {ok, <<>>};

%% RESPONSE_SLURM_RC (8001)
encode_body(?RESPONSE_SLURM_RC, Resp) ->
    encode_slurm_rc_response(Resp);

%% SRUN_JOB_COMPLETE (7004)
encode_body(?SRUN_JOB_COMPLETE, Resp) ->
    encode_srun_job_complete(Resp);

%% SRUN_PING (7001)
encode_body(?SRUN_PING, Resp) ->
    encode_srun_ping(Resp);

%% RESPONSE_RESERVATION_INFO (2013)
encode_body(?RESPONSE_RESERVATION_INFO, Resp) ->
    encode_reservation_info_response(Resp);

%% RESPONSE_LICENSE_INFO (1018)
encode_body(?RESPONSE_LICENSE_INFO, Resp) ->
    encode_license_info_response(Resp);

%% RESPONSE_TOPO_INFO (2019)
encode_body(?RESPONSE_TOPO_INFO, Resp) ->
    encode_topo_info_response(Resp);

%% RESPONSE_FRONT_END_INFO (2029)
encode_body(?RESPONSE_FRONT_END_INFO, Resp) ->
    encode_front_end_info_response(Resp);

%% RESPONSE_BURST_BUFFER_INFO (2021)
encode_body(?RESPONSE_BURST_BUFFER_INFO, Resp) ->
    encode_burst_buffer_info_response(Resp);

%% RESPONSE_BUILD_INFO (2002)
encode_body(?RESPONSE_BUILD_INFO, Resp) ->
    encode_build_info_response(Resp);

%% RESPONSE_CONFIG_INFO (2017)
encode_body(?RESPONSE_CONFIG_INFO, Resp) ->
    encode_config_info_response(Resp);

%% RESPONSE_STATS_INFO (2027)
encode_body(?RESPONSE_STATS_INFO, Resp) ->
    encode_stats_info_response(Resp);

%% Unsupported message type
encode_body(_MsgType, _Body) ->
    unsupported.

%%%===================================================================
%%% Internal: Decode Helpers
%%%===================================================================

%% Decode RESPONSE_SLURM_RC (8001)
decode_slurm_rc_response(Binary) ->
    case Binary of
        <<ReturnCode:32/signed-big, _Rest/binary>> ->
            {ok, #slurm_rc_response{return_code = ReturnCode}};
        <<ReturnCode:32/signed-big>> ->
            {ok, #slurm_rc_response{return_code = ReturnCode}};
        <<>> ->
            {ok, #slurm_rc_response{}};
        _ ->
            {error, invalid_slurm_rc_response}
    end.

%% Decode REQUEST_RESERVATION_INFO (2012)
decode_reservation_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, Rest/binary>> ->
            {ResvName, _Rest2} = case flurm_protocol_pack:unpack_string(Rest) of
                {ok, N, R} -> {ensure_binary(N), R};
                _ -> {<<>>, Rest}
            end,
            {ok, #reservation_info_request{
                show_flags = ShowFlags,
                reservation_name = ResvName
            }};
        <<>> ->
            {ok, #reservation_info_request{}};
        _ ->
            {ok, #reservation_info_request{}}
    end.

%% Decode REQUEST_LICENSE_INFO (1017)
decode_license_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, _Rest/binary>> ->
            {ok, #license_info_request{show_flags = ShowFlags}};
        <<>> ->
            {ok, #license_info_request{}};
        _ ->
            {ok, #license_info_request{}}
    end.

%% Decode REQUEST_TOPO_INFO (2018)
decode_topo_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, _Rest/binary>> ->
            {ok, #topo_info_request{show_flags = ShowFlags}};
        <<>> ->
            {ok, #topo_info_request{}};
        _ ->
            {ok, #topo_info_request{}}
    end.

%% Decode REQUEST_FRONT_END_INFO (2028)
decode_front_end_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, _Rest/binary>> ->
            {ok, #front_end_info_request{show_flags = ShowFlags}};
        <<>> ->
            {ok, #front_end_info_request{}};
        _ ->
            {ok, #front_end_info_request{}}
    end.

%% Decode REQUEST_BURST_BUFFER_INFO (2020)
decode_burst_buffer_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, _Rest/binary>> ->
            {ok, #burst_buffer_info_request{show_flags = ShowFlags}};
        <<>> ->
            {ok, #burst_buffer_info_request{}};
        _ ->
            {ok, #burst_buffer_info_request{}}
    end.

%% Decode REQUEST_RECONFIGURE (1003)
decode_reconfigure_request(<<>>) ->
    {ok, #reconfigure_request{}};
decode_reconfigure_request(<<Flags:32/big, _Rest/binary>>) ->
    {ok, #reconfigure_request{flags = Flags}};
decode_reconfigure_request(_Binary) ->
    {ok, #reconfigure_request{}}.

%% Decode REQUEST_RECONFIGURE_WITH_CONFIG (1004)
decode_reconfigure_with_config_request(<<>>) ->
    {ok, #reconfigure_with_config_request{}};
decode_reconfigure_with_config_request(Binary) ->
    try
        {ConfigFile, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        <<Flags:32/big, Rest2/binary>> = Rest1,
        Force = (Flags band 1) =/= 0,
        NotifyNodes = (Flags band 2) =/= 0,
        <<SettingsCount:32/big, Rest3/binary>> = Rest2,
        {Settings, _Remaining} = decode_settings(SettingsCount, Rest3, #{}),
        {ok, #reconfigure_with_config_request{
            config_file = ConfigFile,
            settings = Settings,
            force = Force,
            notify_nodes = NotifyNodes
        }}
    catch
        _:_ ->
            {ok, #reconfigure_with_config_request{}}
    end.

%% Decode settings
decode_settings(0, Binary, Acc) ->
    {Acc, Binary};
decode_settings(Count, Binary, Acc) when Count > 0 ->
    try
        {Key, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {Value, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        KeyAtom = try binary_to_existing_atom(Key, utf8)
                  catch _:_ -> binary_to_atom(Key, utf8)
                  end,
        decode_settings(Count - 1, Rest2, Acc#{KeyAtom => Value})
    catch
        _:_ ->
            {Acc, <<>>}
    end.

%%%===================================================================
%%% Internal: Encode Helpers
%%%===================================================================

%% Encode RESPONSE_SLURM_RC (8001)
encode_slurm_rc_response(#slurm_rc_response{return_code = ReturnCode}) ->
    {ok, <<ReturnCode:32/signed-big>>};
encode_slurm_rc_response(#{return_code := ReturnCode}) ->
    {ok, <<ReturnCode:32/signed-big>>};
encode_slurm_rc_response(_) ->
    {ok, <<0:32/signed-big>>}.

%% Encode SRUN_JOB_COMPLETE (7004)
encode_srun_job_complete(#srun_job_complete{job_id = JobId, step_id = StepId}) ->
    {ok, <<JobId:32/big, StepId:32/big>>};
encode_srun_job_complete(#{job_id := JobId, step_id := StepId}) ->
    {ok, <<JobId:32/big, StepId:32/big>>};
encode_srun_job_complete(_) ->
    {ok, <<0:32, 0:32>>}.

%% Encode SRUN_PING (7001)
encode_srun_ping(#srun_ping{job_id = JobId, step_id = StepId}) ->
    {ok, <<JobId:32/big, StepId:32/big>>};
encode_srun_ping(#{job_id := JobId, step_id := StepId}) ->
    {ok, <<JobId:32/big, StepId:32/big>>};
encode_srun_ping(_) ->
    {ok, <<0:32, 0:32>>}.

%% Encode RESPONSE_RESERVATION_INFO (2013)
encode_reservation_info_response(#reservation_info_response{
    last_update = LastUpdate,
    reservation_count = ResvCount,
    reservations = Reservations
}) ->
    ResvsBin = [encode_single_reservation_info(R) || R <- Reservations],
    Parts = [
        <<ResvCount:32/big, LastUpdate:64/big>>,
        ResvsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_reservation_info_response(_) ->
    {ok, <<0:32, 0:64>>}.

encode_single_reservation_info(#reservation_info{} = R) ->
    [
        flurm_protocol_pack:pack_string(R#reservation_info.accounts),
        flurm_protocol_pack:pack_string(R#reservation_info.burst_buffer),
        flurm_protocol_pack:pack_uint32(R#reservation_info.core_cnt),
        flurm_protocol_pack:pack_uint32(R#reservation_info.core_spec_cnt),
        flurm_protocol_pack:pack_time(R#reservation_info.end_time),
        flurm_protocol_pack:pack_string(R#reservation_info.features),
        flurm_protocol_pack:pack_uint64(R#reservation_info.flags),
        flurm_protocol_pack:pack_string(R#reservation_info.groups),
        flurm_protocol_pack:pack_string(R#reservation_info.licenses),
        flurm_protocol_pack:pack_uint32(R#reservation_info.max_start_delay),
        flurm_protocol_pack:pack_string(R#reservation_info.name),
        flurm_protocol_pack:pack_uint32(R#reservation_info.node_cnt),
        flurm_protocol_pack:pack_string(R#reservation_info.node_list),
        <<?SLURM_NO_VAL:32/big>>,  %% node_inx
        flurm_protocol_pack:pack_string(R#reservation_info.partition),
        flurm_protocol_pack:pack_uint32(R#reservation_info.purge_comp_time),
        flurm_protocol_pack:pack_uint32(R#reservation_info.resv_watts),
        flurm_protocol_pack:pack_time(R#reservation_info.start_time),
        flurm_protocol_pack:pack_string(R#reservation_info.tres_str),
        flurm_protocol_pack:pack_string(R#reservation_info.users)
    ];
encode_single_reservation_info(_) ->
    [].

%% Encode RESPONSE_LICENSE_INFO (1018)
encode_license_info_response(#license_info_response{
    last_update = LastUpdate,
    license_count = LicCount,
    licenses = Licenses
}) ->
    LicsBin = [encode_single_license_info(L) || L <- Licenses],
    Parts = [
        flurm_protocol_pack:pack_time(LastUpdate),
        <<LicCount:32/big>>,
        LicsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_license_info_response(_) ->
    {ok, <<0:64, 0:32>>}.

encode_single_license_info(#license_info{} = L) ->
    [
        flurm_protocol_pack:pack_string(L#license_info.name),
        flurm_protocol_pack:pack_uint32(L#license_info.total),
        flurm_protocol_pack:pack_uint32(L#license_info.in_use),
        flurm_protocol_pack:pack_uint32(L#license_info.available),
        flurm_protocol_pack:pack_uint32(L#license_info.reserved),
        flurm_protocol_pack:pack_uint8(L#license_info.remote)
    ];
encode_single_license_info(_) ->
    [].

%% Encode RESPONSE_TOPO_INFO (2019)
encode_topo_info_response(#topo_info_response{
    topo_count = TopoCount,
    topos = Topos
}) ->
    ToposBin = [encode_single_topo_info(T) || T <- Topos],
    Parts = [
        <<TopoCount:32/big>>,
        ToposBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_topo_info_response(_) ->
    {ok, <<0:32>>}.

encode_single_topo_info(#topo_info{} = T) ->
    [
        flurm_protocol_pack:pack_uint16(T#topo_info.level),
        flurm_protocol_pack:pack_uint32(T#topo_info.link_speed),
        flurm_protocol_pack:pack_string(T#topo_info.name),
        flurm_protocol_pack:pack_string(T#topo_info.nodes),
        flurm_protocol_pack:pack_string(T#topo_info.switches)
    ];
encode_single_topo_info(_) ->
    [].

%% Encode RESPONSE_FRONT_END_INFO (2029)
encode_front_end_info_response(#front_end_info_response{
    last_update = LastUpdate,
    front_end_count = FECount,
    front_ends = FrontEnds
}) ->
    FEsBin = [encode_single_front_end_info(FE) || FE <- FrontEnds],
    Parts = [
        <<FECount:32/big, LastUpdate:64/big>>,
        FEsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_front_end_info_response(_) ->
    {ok, <<0:32, 0:64>>}.

encode_single_front_end_info(#front_end_info{} = FE) ->
    [
        flurm_protocol_pack:pack_string(FE#front_end_info.allow_groups),
        flurm_protocol_pack:pack_string(FE#front_end_info.allow_users),
        flurm_protocol_pack:pack_time(FE#front_end_info.boot_time),
        flurm_protocol_pack:pack_string(FE#front_end_info.deny_groups),
        flurm_protocol_pack:pack_string(FE#front_end_info.deny_users),
        flurm_protocol_pack:pack_string(FE#front_end_info.name),
        flurm_protocol_pack:pack_uint32(FE#front_end_info.node_state),
        flurm_protocol_pack:pack_string(FE#front_end_info.reason),
        flurm_protocol_pack:pack_time(FE#front_end_info.reason_time),
        flurm_protocol_pack:pack_uint32(FE#front_end_info.reason_uid),
        flurm_protocol_pack:pack_time(FE#front_end_info.slurmd_start_time),
        flurm_protocol_pack:pack_string(FE#front_end_info.version)
    ];
encode_single_front_end_info(_) ->
    [].

%% Encode RESPONSE_BURST_BUFFER_INFO (2021)
encode_burst_buffer_info_response(#burst_buffer_info_response{
    last_update = LastUpdate,
    burst_buffer_count = BBCount,
    burst_buffers = BurstBuffers
}) ->
    BBsBin = [encode_single_burst_buffer_info(BB) || BB <- BurstBuffers],
    Parts = [
        <<BBCount:32/big, LastUpdate:64/big>>,
        BBsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_burst_buffer_info_response(_) ->
    {ok, <<0:32, 0:64>>}.

encode_single_burst_buffer_info(#burst_buffer_info{} = BB) ->
    PoolsBin = [encode_single_bb_pool(P) || P <- BB#burst_buffer_info.pools],
    [
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.name),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.allow_users),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.create_buffer),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.default_pool),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.deny_users),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.destroy_buffer),
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.flags),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.get_sys_state),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.get_sys_status),
        flurm_protocol_pack:pack_uint64(BB#burst_buffer_info.granularity),
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.pool_cnt),
        PoolsBin,
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.other_timeout),
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.stage_in_timeout),
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.stage_out_timeout),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.start_stage_in),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.start_stage_out),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.stop_stage_in),
        flurm_protocol_pack:pack_string(BB#burst_buffer_info.stop_stage_out),
        flurm_protocol_pack:pack_uint64(BB#burst_buffer_info.total_space),
        flurm_protocol_pack:pack_uint64(BB#burst_buffer_info.unfree_space),
        flurm_protocol_pack:pack_uint64(BB#burst_buffer_info.used_space),
        flurm_protocol_pack:pack_uint32(BB#burst_buffer_info.validate_timeout)
    ];
encode_single_burst_buffer_info(_) ->
    [].

encode_single_bb_pool(#burst_buffer_pool{} = P) ->
    [
        flurm_protocol_pack:pack_uint64(P#burst_buffer_pool.granularity),
        flurm_protocol_pack:pack_string(P#burst_buffer_pool.name),
        flurm_protocol_pack:pack_uint64(P#burst_buffer_pool.total_space),
        flurm_protocol_pack:pack_uint64(P#burst_buffer_pool.unfree_space),
        flurm_protocol_pack:pack_uint64(P#burst_buffer_pool.used_space)
    ];
encode_single_bb_pool(_) ->
    [].

%% Encode RESPONSE_BUILD_INFO (2002)
encode_build_info_response(#build_info_response{} = R) ->
    S = fun flurm_protocol_pack:pack_string/1,
    U16 = fun flurm_protocol_pack:pack_uint16/1,
    U32 = fun flurm_protocol_pack:pack_uint32/1,
    U64 = fun flurm_protocol_pack:pack_uint64/1,
    T = fun flurm_protocol_pack:pack_time/1,
    SA = fun flurm_protocol_pack:pack_string_array/1,
    EmptyList = <<0:32/big>>,
    ControlMachine = R#build_info_response.control_machine,
    Parts = [
        T(erlang:system_time(second)),
        U16(0), S(<<>>), S(<<>>), S(<<>>), S(<<>>), U16(0), S(<<>>),
        S(R#build_info_response.accounting_storage_type), S(<<>>),
        EmptyList, S(<<>>), S(<<>>), S(<<>>), U16(0), S(<<>>),
        S(<<>>), S(<<>>), S(<<>>), S(R#build_info_response.auth_type),
        U16(120), T(erlang:system_time(second)), S(<<>>), S(<<>>), S(<<>>),
        EmptyList, S(<<>>), S(R#build_info_response.cluster_name), S(<<>>),
        U16(0), U32(0), SA([ControlMachine]), SA([ControlMachine]),
        S(<<>>), U32(0), U32(0), S(<<"cred/none">>), U64(0), U64(0),
        S(<<>>), U16(0), U16(0), S(<<>>), U32(0), S(<<>>),
        EmptyList, S(<<>>), U16(0), S(<<>>), U32(1), U16(1), U16(2),
        S(<<>>), U16(600), U16(1), S(<<>>), U32(0), U16(0), U16(0),
        S(<<>>), U16(0), S(<<>>), S(<<>>), S(<<"jobacct_gather/none">>),
        S(<<>>), S(<<>>), S(<<>>), S(<<>>), U32(0),
        S(R#build_info_response.job_comp_type), S(<<>>), S(<<>>),
        S(<<>>), S(<<>>), EmptyList, U16(0), U16(1), S(<<>>),
        U16(0), U16(60), S(<<>>), S(<<"launch/slurm">>), S(<<>>),
        U16(0), U32(1001), U32(0), S(<<>>), S(<<"/bin/mail">>),
        U32(10000), U32(16#03ffffff), U64(0), U32(65536), U32(40000),
        U16(512), S(<<>>), S(<<>>), U32(300), EmptyList,
        S(<<"mpi/none">>), S(<<>>), U16(10), U32(1), EmptyList,
        S(<<>>), S(<<>>), U16(0), S(R#build_info_response.plugin_dir),
        S(<<>>), S(<<>>), S(<<>>), U16(0), S(<<"preempt/none">>),
        U32(0), S(<<>>), S(<<>>), U32(0), U32(300), U16(0), U16(0),
        U32(0), S(<<>>), U16(0), S(R#build_info_response.priority_type),
        U32(0), U32(0), U32(0), U32(0), U32(0), U32(0), S(<<>>),
        U16(0), S(<<"proctrack/linuxproc">>), S(<<>>), U16(0), S(<<>>),
        U16(0), U16(0), S(<<>>), S(<<>>), S(<<>>), U16(0), S(<<>>),
        S(<<>>), S(<<>>), S(<<>>), U16(300), U16(60), S(<<>>),
        U16(0), S(<<>>), U16(0), S(<<>>), S(<<>>), S(<<>>), U16(0),
        U16(30), S(R#build_info_response.scheduler_type), S(<<>>),
        S(R#build_info_response.select_type), EmptyList, U16(0),
        S(<<"/etc/slurm/slurm.conf">>), U32(0),
        S(R#build_info_response.slurm_user_name), U32(0),
        S(R#build_info_response.slurmd_user_name), S(<<>>), U16(7),
        S(<<"/var/log/slurmctld.log">>), S(<<>>),
        S(<<"/var/run/slurmctld.pid">>), S(<<>>), EmptyList,
        U32(R#build_info_response.slurmctld_port), U16(1), S(<<>>),
        S(<<>>), U16(0), U16(120), U16(7), S(<<"/var/log/slurmd.log">>),
        S(<<>>), S(<<"/var/run/slurmd.pid">>),
        U32(R#build_info_response.slurmd_port),
        S(R#build_info_response.spool_dir), U16(0), U16(300), S(<<>>),
        U16(0), U16(0), S(<<>>), S(R#build_info_response.state_save_location),
        S(<<>>), S(<<>>), S(<<>>), U16(0), U32(0), U16(0), S(<<>>),
        S(<<"switch/none">>), S(<<>>), S(<<>>), S(<<"task/none">>),
        U32(0), U16(2), S(<<"/tmp">>), S(<<>>), S(<<"topology/none">>),
        U16(50), S(<<>>), U16(60), S(R#build_info_response.version),
        U16(0), U16(0), S(<<>>)
    ],
    {ok, iolist_to_binary(Parts)};
encode_build_info_response(_Response) ->
    {ok, <<>>}.

%% Encode RESPONSE_CONFIG_INFO (2017)
encode_config_info_response(#config_info_response{last_update = LastUpdate, config = Config}) ->
    ConfigList = maps:fold(fun(K, V, Acc) ->
        KeyBin = if
            is_atom(K) -> atom_to_binary(K, utf8);
            is_binary(K) -> K;
            is_list(K) -> list_to_binary(K);
            true -> iolist_to_binary(io_lib:format("~p", [K]))
        end,
        ValBin = if
            is_binary(V) -> V;
            is_list(V) -> list_to_binary(V);
            is_integer(V) -> integer_to_binary(V);
            is_atom(V) -> atom_to_binary(V, utf8);
            true -> iolist_to_binary(io_lib:format("~p", [V]))
        end,
        [<<KeyBin/binary, "=", ValBin/binary>> | Acc]
    end, [], Config),
    ConfigCount = length(ConfigList),
    ConfigBins = [flurm_protocol_pack:pack_string(S) || S <- ConfigList],
    Parts = [
        flurm_protocol_pack:pack_time(LastUpdate),
        <<ConfigCount:32/big>>,
        ConfigBins
    ],
    {ok, iolist_to_binary(Parts)};
encode_config_info_response(_Response) ->
    {ok, <<>>}.

%% Encode RESPONSE_STATS_INFO (2027)
encode_stats_info_response(#stats_info_response{} = S) ->
    Parts = [
        flurm_protocol_pack:pack_uint32(S#stats_info_response.parts_packed),
        flurm_protocol_pack:pack_time(S#stats_info_response.req_time),
        flurm_protocol_pack:pack_time(S#stats_info_response.req_time_start),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.server_thread_count),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.agent_queue_size),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.agent_count),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.agent_thread_count),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.dbd_agent_queue_size),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_submitted),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_started),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_completed),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_canceled),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_failed),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_pending),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.jobs_running),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.schedule_cycle_max),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.schedule_cycle_last),
        flurm_protocol_pack:pack_uint64(S#stats_info_response.schedule_cycle_sum),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.schedule_cycle_counter),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.schedule_cycle_depth),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.schedule_queue_len),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_backfilled_jobs),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_last_backfilled_jobs),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_cycle_counter),
        flurm_protocol_pack:pack_uint64(S#stats_info_response.bf_cycle_sum),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_cycle_last),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_cycle_max),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_depth_sum),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_depth_try_sum),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_queue_len),
        flurm_protocol_pack:pack_uint32(S#stats_info_response.bf_queue_len_sum),
        flurm_protocol_pack:pack_time(S#stats_info_response.bf_when_last_cycle),
        flurm_protocol_pack:pack_uint8(case S#stats_info_response.bf_active of true -> 1; false -> 0 end),
        <<0:32/big>>,  %% rpc_type_cnt
        <<0:32/big>>   %% rpc_user_cnt
    ],
    {ok, iolist_to_binary(Parts)};
encode_stats_info_response(_Response) ->
    {ok, <<>>}.

%% Encode reconfigure response
encode_reconfigure_response(#reconfigure_response{} = R) ->
    ChangedKeysStr = iolist_to_binary(lists:join(<<",">>,
        [atom_to_binary(K, utf8) || K <- R#reconfigure_response.changed_keys])),
    Parts = [
        flurm_protocol_pack:pack_int32(R#reconfigure_response.return_code),
        flurm_protocol_pack:pack_string(R#reconfigure_response.message),
        flurm_protocol_pack:pack_string(ChangedKeysStr),
        flurm_protocol_pack:pack_uint32(R#reconfigure_response.version)
    ],
    {ok, iolist_to_binary(Parts)};
encode_reconfigure_response(_) ->
    {ok, <<0:32/big-signed, 0:32, 0:32, 0:32>>}.

%%%===================================================================
%%% Internal: Utility Functions
%%%===================================================================

%% Ensure value is binary
ensure_binary(undefined) -> <<>>;
ensure_binary(null) -> <<>>;
ensure_binary(Bin) when is_binary(Bin) -> Bin;
ensure_binary(List) when is_list(List) -> list_to_binary(List);
ensure_binary(_) -> <<>>.
