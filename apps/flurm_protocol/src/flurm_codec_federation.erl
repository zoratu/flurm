%%%-------------------------------------------------------------------
%%% @doc FLURM Codec - Federation Message Encoding/Decoding
%%%
%%% This module handles encoding and decoding of federation-related SLURM
%%% protocol messages:
%%%   - REQUEST_FED_INFO (2049) / RESPONSE_FED_INFO (2050)
%%%   - REQUEST_FEDERATION_SUBMIT (2032) / RESPONSE_FEDERATION_SUBMIT (2033)
%%%   - REQUEST_FEDERATION_JOB_STATUS (2034) / RESPONSE_FEDERATION_JOB_STATUS (2035)
%%%   - REQUEST_FEDERATION_JOB_CANCEL (2036) / RESPONSE_FEDERATION_JOB_CANCEL (2037)
%%%   - REQUEST_UPDATE_FEDERATION (2064) / RESPONSE_UPDATE_FEDERATION (2065)
%%%   - MSG_FED_JOB_SUBMIT (2070)
%%%   - MSG_FED_JOB_STARTED (2071)
%%%   - MSG_FED_SIBLING_REVOKE (2072)
%%%   - MSG_FED_JOB_COMPLETED (2073)
%%%   - MSG_FED_JOB_FAILED (2074)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_codec_federation).

-export([
    decode_body/2,
    encode_body/2
]).

-include("flurm_protocol.hrl").

%%%===================================================================
%%% Decode Functions
%%%===================================================================

%% REQUEST_FED_INFO (2049)
decode_body(?REQUEST_FED_INFO, Binary) ->
    decode_fed_info_request(Binary);

%% REQUEST_FEDERATION_SUBMIT (2032)
decode_body(?REQUEST_FEDERATION_SUBMIT, Binary) ->
    decode_federation_submit_request(Binary);

%% REQUEST_FEDERATION_JOB_STATUS (2034)
decode_body(?REQUEST_FEDERATION_JOB_STATUS, Binary) ->
    decode_federation_job_status_request(Binary);

%% REQUEST_FEDERATION_JOB_CANCEL (2036)
decode_body(?REQUEST_FEDERATION_JOB_CANCEL, Binary) ->
    decode_federation_job_cancel_request(Binary);

%% RESPONSE_FED_INFO (2050)
decode_body(?RESPONSE_FED_INFO, Binary) ->
    decode_fed_info_response(Binary);

%% RESPONSE_FEDERATION_SUBMIT (2033)
decode_body(?RESPONSE_FEDERATION_SUBMIT, Binary) ->
    decode_federation_submit_response(Binary);

%% RESPONSE_FEDERATION_JOB_STATUS (2035)
decode_body(?RESPONSE_FEDERATION_JOB_STATUS, Binary) ->
    decode_federation_job_status_response(Binary);

%% RESPONSE_FEDERATION_JOB_CANCEL (2037)
decode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Binary) ->
    decode_federation_job_cancel_response(Binary);

%% REQUEST_UPDATE_FEDERATION (2064)
decode_body(?REQUEST_UPDATE_FEDERATION, Binary) ->
    decode_update_federation_request(Binary);

%% RESPONSE_UPDATE_FEDERATION (2065)
decode_body(?RESPONSE_UPDATE_FEDERATION, Binary) ->
    decode_update_federation_response(Binary);

%% MSG_FED_JOB_SUBMIT (2070)
decode_body(?MSG_FED_JOB_SUBMIT, Binary) ->
    decode_fed_job_submit_msg(Binary);

%% MSG_FED_JOB_STARTED (2071)
decode_body(?MSG_FED_JOB_STARTED, Binary) ->
    decode_fed_job_started_msg(Binary);

%% MSG_FED_SIBLING_REVOKE (2072)
decode_body(?MSG_FED_SIBLING_REVOKE, Binary) ->
    decode_fed_sibling_revoke_msg(Binary);

%% MSG_FED_JOB_COMPLETED (2073)
decode_body(?MSG_FED_JOB_COMPLETED, Binary) ->
    decode_fed_job_completed_msg(Binary);

%% MSG_FED_JOB_FAILED (2074)
decode_body(?MSG_FED_JOB_FAILED, Binary) ->
    decode_fed_job_failed_msg(Binary);

%% Unsupported message type
decode_body(_MsgType, _Binary) ->
    unsupported.

%%%===================================================================
%%% Encode Functions
%%%===================================================================

%% REQUEST_FED_INFO (2049)
encode_body(?REQUEST_FED_INFO, Req) ->
    encode_fed_info_request(Req);

%% REQUEST_FEDERATION_SUBMIT (2032)
encode_body(?REQUEST_FEDERATION_SUBMIT, Req) ->
    encode_federation_submit_request(Req);

%% REQUEST_FEDERATION_JOB_STATUS (2034)
encode_body(?REQUEST_FEDERATION_JOB_STATUS, Req) ->
    encode_federation_job_status_request(Req);

%% REQUEST_FEDERATION_JOB_CANCEL (2036)
encode_body(?REQUEST_FEDERATION_JOB_CANCEL, Req) ->
    encode_federation_job_cancel_request(Req);

%% RESPONSE_FED_INFO (2050)
encode_body(?RESPONSE_FED_INFO, Resp) ->
    encode_fed_info_response(Resp);

%% RESPONSE_FEDERATION_SUBMIT (2033)
encode_body(?RESPONSE_FEDERATION_SUBMIT, Resp) ->
    encode_federation_submit_response(Resp);

%% RESPONSE_FEDERATION_JOB_STATUS (2035)
encode_body(?RESPONSE_FEDERATION_JOB_STATUS, Resp) ->
    encode_federation_job_status_response(Resp);

%% RESPONSE_FEDERATION_JOB_CANCEL (2037)
encode_body(?RESPONSE_FEDERATION_JOB_CANCEL, Resp) ->
    encode_federation_job_cancel_response(Resp);

%% REQUEST_UPDATE_FEDERATION (2064)
encode_body(?REQUEST_UPDATE_FEDERATION, Req) ->
    encode_update_federation_request(Req);

%% RESPONSE_UPDATE_FEDERATION (2065)
encode_body(?RESPONSE_UPDATE_FEDERATION, Resp) ->
    encode_update_federation_response(Resp);

%% MSG_FED_JOB_SUBMIT (2070)
encode_body(?MSG_FED_JOB_SUBMIT, Msg) ->
    encode_fed_job_submit_msg(Msg);

%% MSG_FED_JOB_STARTED (2071)
encode_body(?MSG_FED_JOB_STARTED, Msg) ->
    encode_fed_job_started_msg(Msg);

%% MSG_FED_SIBLING_REVOKE (2072)
encode_body(?MSG_FED_SIBLING_REVOKE, Msg) ->
    encode_fed_sibling_revoke_msg(Msg);

%% MSG_FED_JOB_COMPLETED (2073)
encode_body(?MSG_FED_JOB_COMPLETED, Msg) ->
    encode_fed_job_completed_msg(Msg);

%% MSG_FED_JOB_FAILED (2074)
encode_body(?MSG_FED_JOB_FAILED, Msg) ->
    encode_fed_job_failed_msg(Msg);

%% Unsupported message type
encode_body(_MsgType, _Body) ->
    unsupported.

%%%===================================================================
%%% Internal: Decode Helpers
%%%===================================================================

%% Decode REQUEST_FED_INFO (2049)
decode_fed_info_request(<<>>) ->
    {ok, #fed_info_request{}};
decode_fed_info_request(<<ShowFlags:32/big, _Rest/binary>>) ->
    {ok, #fed_info_request{show_flags = ShowFlags}};
decode_fed_info_request(_) ->
    {ok, #fed_info_request{}}.

%% Decode RESPONSE_FED_INFO (2050)
decode_fed_info_response(Binary) ->
    try
        {FedName, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {LocalCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<ClusterCount:32/big, Rest3/binary>> = Rest2,
        {Clusters, _} = decode_fed_clusters(ClusterCount, Rest3, []),
        {ok, #fed_info_response{
            federation_name = FedName,
            local_cluster = LocalCluster,
            cluster_count = ClusterCount,
            clusters = Clusters
        }}
    catch
        _:_ ->
            {ok, #fed_info_response{}}
    end.

%% Decode federation cluster list
decode_fed_clusters(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_fed_clusters(Count, Binary, Acc) ->
    try
        {Name, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {Host, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<Port:32/big, Rest3/binary>> = Rest2,
        {State, Rest4} = flurm_protocol_pack:unpack_string(Rest3),
        <<Weight:32/big, FeatureCount:32/big, Rest5/binary>> = Rest4,
        {Features, Rest6} = decode_string_list(FeatureCount, Rest5, []),
        <<PartitionCount:32/big, Rest7/binary>> = Rest6,
        {Partitions, Rest8} = decode_string_list(PartitionCount, Rest7, []),
        Cluster = #fed_cluster_info{
            name = Name,
            host = Host,
            port = Port,
            state = State,
            weight = Weight,
            features = Features,
            partitions = Partitions
        },
        decode_fed_clusters(Count - 1, Rest8, [Cluster | Acc])
    catch
        _:_ ->
            {lists:reverse(Acc), <<>>}
    end.

%% Decode REQUEST_FEDERATION_SUBMIT (2032)
decode_federation_submit_request(Binary) ->
    try
        {SourceCluster, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {TargetCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<JobId:32/big, Rest3/binary>> = Rest2,
        {Name, Rest4} = flurm_protocol_pack:unpack_string(Rest3),
        {Script, Rest5} = flurm_protocol_pack:unpack_string(Rest4),
        {Partition, Rest6} = flurm_protocol_pack:unpack_string(Rest5),
        <<NumCpus:32/big, NumNodes:32/big, MemoryMb:32/big,
          TimeLimit:32/big, UserId:32/big, GroupId:32/big,
          Priority:32/big, Rest7/binary>> = Rest6,
        {WorkDir, Rest8} = flurm_protocol_pack:unpack_string(Rest7),
        {StdOut, Rest9} = flurm_protocol_pack:unpack_string(Rest8),
        {StdErr, Rest10} = flurm_protocol_pack:unpack_string(Rest9),
        <<EnvCount:32/big, Rest11/binary>> = Rest10,
        {Environment, Rest12} = decode_string_list(EnvCount, Rest11, []),
        {Features, _} = flurm_protocol_pack:unpack_string(Rest12),
        {ok, #federation_submit_request{
            source_cluster = SourceCluster,
            target_cluster = TargetCluster,
            job_id = JobId,
            name = Name,
            script = Script,
            partition = Partition,
            num_cpus = NumCpus,
            num_nodes = NumNodes,
            memory_mb = MemoryMb,
            time_limit = TimeLimit,
            user_id = UserId,
            group_id = GroupId,
            priority = Priority,
            work_dir = WorkDir,
            std_out = StdOut,
            std_err = StdErr,
            environment = Environment,
            features = Features
        }}
    catch
        _:_ ->
            {ok, #federation_submit_request{}}
    end.

%% Decode RESPONSE_FEDERATION_SUBMIT (2033)
decode_federation_submit_response(Binary) ->
    try
        {SourceCluster, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        <<JobId:32/big, ErrorCode:32/big, Rest2/binary>> = Rest1,
        {ErrorMsg, _} = flurm_protocol_pack:unpack_string(Rest2),
        {ok, #federation_submit_response{
            source_cluster = SourceCluster,
            job_id = JobId,
            error_code = ErrorCode,
            error_msg = ErrorMsg
        }}
    catch
        _:_ ->
            {ok, #federation_submit_response{}}
    end.

%% Decode REQUEST_FEDERATION_JOB_STATUS (2034)
decode_federation_job_status_request(Binary) ->
    try
        {SourceCluster, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        <<JobId:32/big, Rest2/binary>> = Rest1,
        {JobIdStr, _} = flurm_protocol_pack:unpack_string(Rest2),
        {ok, #federation_job_status_request{
            source_cluster = SourceCluster,
            job_id = JobId,
            job_id_str = JobIdStr
        }}
    catch
        _:_ ->
            {ok, #federation_job_status_request{}}
    end.

%% Decode RESPONSE_FEDERATION_JOB_STATUS (2035)
decode_federation_job_status_response(Binary) ->
    try
        <<JobId:32/big, JobState:32/big, StateReason:32/big,
          ExitCode:32/big, StartTime:64/big, EndTime:64/big, Rest/binary>> = Binary,
        {Nodes, _} = flurm_protocol_pack:unpack_string(Rest),
        {ok, #federation_job_status_response{
            job_id = JobId,
            job_state = JobState,
            state_reason = StateReason,
            exit_code = ExitCode,
            start_time = StartTime,
            end_time = EndTime,
            nodes = Nodes
        }}
    catch
        _:_ ->
            {ok, #federation_job_status_response{}}
    end.

%% Decode REQUEST_FEDERATION_JOB_CANCEL (2036)
decode_federation_job_cancel_request(Binary) ->
    try
        {SourceCluster, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        <<JobId:32/big, Rest2/binary>> = Rest1,
        {JobIdStr, Rest3} = flurm_protocol_pack:unpack_string(Rest2),
        <<Signal:32/big, _/binary>> = Rest3,
        {ok, #federation_job_cancel_request{
            source_cluster = SourceCluster,
            job_id = JobId,
            job_id_str = JobIdStr,
            signal = Signal
        }}
    catch
        _:_ ->
            {ok, #federation_job_cancel_request{}}
    end.

%% Decode RESPONSE_FEDERATION_JOB_CANCEL (2037)
decode_federation_job_cancel_response(Binary) ->
    try
        <<JobId:32/big, ErrorCode:32/big, Rest/binary>> = Binary,
        {ErrorMsg, _} = flurm_protocol_pack:unpack_string(Rest),
        {ok, #federation_job_cancel_response{
            job_id = JobId,
            error_code = ErrorCode,
            error_msg = ErrorMsg
        }}
    catch
        _:_ ->
            {ok, #federation_job_cancel_response{}}
    end.

%% Decode REQUEST_UPDATE_FEDERATION (2064)
decode_update_federation_request(Binary) ->
    try
        <<ActionCode:32/big, Rest1/binary>> = Binary,
        Action = case ActionCode of
            1 -> add_cluster;
            2 -> remove_cluster;
            3 -> update_settings;
            _ -> unknown
        end,
        {ClusterName, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        {Host, Rest3} = flurm_protocol_pack:unpack_string(Rest2),
        <<Port:32/big, SettingsCount:32/big, Rest4/binary>> = Rest3,
        Settings = decode_settings_map(SettingsCount, Rest4, #{}),
        {ok, #update_federation_request{
            action = Action,
            cluster_name = ClusterName,
            host = Host,
            port = Port,
            settings = Settings
        }}
    catch
        _:_ ->
            {ok, #update_federation_request{action = unknown}}
    end.

%% Decode RESPONSE_UPDATE_FEDERATION (2065)
decode_update_federation_response(Binary) ->
    try
        <<ErrorCode:32/big, Rest/binary>> = Binary,
        {ErrorMsg, _} = flurm_protocol_pack:unpack_string(Rest),
        {ok, #update_federation_response{
            error_code = ErrorCode,
            error_msg = ErrorMsg
        }}
    catch
        _:_ ->
            {ok, #update_federation_response{error_code = 1, error_msg = <<"decode error">>}}
    end.

%% Decode MSG_FED_JOB_SUBMIT (2070)
decode_fed_job_submit_msg(Binary) ->
    try
        {FedJobId, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {OriginCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        {TargetCluster, Rest3} = flurm_protocol_pack:unpack_string(Rest2),
        <<SubmitTime:64/big, JobSpecLen:32/big, Rest4/binary>> = Rest3,
        <<JobSpecBin:JobSpecLen/binary, _/binary>> = Rest4,
        JobSpec = binary_to_term(JobSpecBin),
        {ok, #fed_job_submit_msg{
            federation_job_id = FedJobId,
            origin_cluster = OriginCluster,
            target_cluster = TargetCluster,
            job_spec = JobSpec,
            submit_time = SubmitTime
        }}
    catch
        _:_ ->
            {ok, #fed_job_submit_msg{
                federation_job_id = <<>>,
                origin_cluster = <<>>,
                target_cluster = <<>>,
                job_spec = #{}
            }}
    end.

%% Decode MSG_FED_JOB_STARTED (2071)
decode_fed_job_started_msg(Binary) ->
    try
        {FedJobId, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {RunningCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<LocalJobId:32/big, StartTime:64/big, _/binary>> = Rest2,
        {ok, #fed_job_started_msg{
            federation_job_id = FedJobId,
            running_cluster = RunningCluster,
            local_job_id = LocalJobId,
            start_time = StartTime
        }}
    catch
        _:_ ->
            {ok, #fed_job_started_msg{
                federation_job_id = <<>>,
                running_cluster = <<>>
            }}
    end.

%% Decode MSG_FED_SIBLING_REVOKE (2072)
decode_fed_sibling_revoke_msg(Binary) ->
    try
        {FedJobId, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {RunningCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        {RevokeReason, _} = flurm_protocol_pack:unpack_string(Rest2),
        {ok, #fed_sibling_revoke_msg{
            federation_job_id = FedJobId,
            running_cluster = RunningCluster,
            revoke_reason = RevokeReason
        }}
    catch
        _:_ ->
            {ok, #fed_sibling_revoke_msg{
                federation_job_id = <<>>,
                running_cluster = <<>>
            }}
    end.

%% Decode MSG_FED_JOB_COMPLETED (2073)
decode_fed_job_completed_msg(Binary) ->
    try
        {FedJobId, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {RunningCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<LocalJobId:32/big, EndTime:64/big, ExitCode:32/signed-big, _/binary>> = Rest2,
        {ok, #fed_job_completed_msg{
            federation_job_id = FedJobId,
            running_cluster = RunningCluster,
            local_job_id = LocalJobId,
            end_time = EndTime,
            exit_code = ExitCode
        }}
    catch
        _:_ ->
            {ok, #fed_job_completed_msg{
                federation_job_id = <<>>,
                running_cluster = <<>>
            }}
    end.

%% Decode MSG_FED_JOB_FAILED (2074)
decode_fed_job_failed_msg(Binary) ->
    try
        {FedJobId, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {RunningCluster, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        <<LocalJobId:32/big, EndTime:64/big, ExitCode:32/signed-big, Rest3/binary>> = Rest2,
        {ErrorMsg, _} = flurm_protocol_pack:unpack_string(Rest3),
        {ok, #fed_job_failed_msg{
            federation_job_id = FedJobId,
            running_cluster = RunningCluster,
            local_job_id = LocalJobId,
            end_time = EndTime,
            exit_code = ExitCode,
            error_msg = ErrorMsg
        }}
    catch
        _:_ ->
            {ok, #fed_job_failed_msg{
                federation_job_id = <<>>,
                running_cluster = <<>>
            }}
    end.

%%%===================================================================
%%% Internal: Encode Helpers
%%%===================================================================

%% Encode REQUEST_FED_INFO (2049)
encode_fed_info_request(#fed_info_request{show_flags = ShowFlags}) ->
    {ok, <<ShowFlags:32/big>>};
encode_fed_info_request(_) ->
    {ok, <<0:32/big>>}.

%% Encode RESPONSE_FED_INFO (2050)
encode_fed_info_response(#fed_info_response{} = R) ->
    ClustersBin = encode_fed_clusters(R#fed_info_response.clusters),
    Parts = [
        flurm_protocol_pack:pack_string(R#fed_info_response.federation_name),
        flurm_protocol_pack:pack_string(R#fed_info_response.local_cluster),
        <<(R#fed_info_response.cluster_count):32/big>>,
        ClustersBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_info_response(_) ->
    {ok, <<0:32, 0:32, 0:32>>}.

%% Encode federation cluster list
encode_fed_clusters(Clusters) ->
    lists:map(fun(#fed_cluster_info{} = C) ->
        FeaturesBin = encode_string_list(C#fed_cluster_info.features),
        PartitionsBin = encode_string_list(C#fed_cluster_info.partitions),
        [
            flurm_protocol_pack:pack_string(C#fed_cluster_info.name),
            flurm_protocol_pack:pack_string(C#fed_cluster_info.host),
            <<(C#fed_cluster_info.port):32/big>>,
            flurm_protocol_pack:pack_string(C#fed_cluster_info.state),
            <<(C#fed_cluster_info.weight):32/big>>,
            <<(length(C#fed_cluster_info.features)):32/big>>,
            FeaturesBin,
            <<(length(C#fed_cluster_info.partitions)):32/big>>,
            PartitionsBin
        ]
    end, Clusters).

%% Encode REQUEST_FEDERATION_SUBMIT (2032)
encode_federation_submit_request(#federation_submit_request{} = R) ->
    EnvBin = encode_string_list(R#federation_submit_request.environment),
    Parts = [
        flurm_protocol_pack:pack_string(R#federation_submit_request.source_cluster),
        flurm_protocol_pack:pack_string(R#federation_submit_request.target_cluster),
        <<(R#federation_submit_request.job_id):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_submit_request.name),
        flurm_protocol_pack:pack_string(R#federation_submit_request.script),
        flurm_protocol_pack:pack_string(R#federation_submit_request.partition),
        <<(R#federation_submit_request.num_cpus):32/big,
          (R#federation_submit_request.num_nodes):32/big,
          (R#federation_submit_request.memory_mb):32/big,
          (R#federation_submit_request.time_limit):32/big,
          (R#federation_submit_request.user_id):32/big,
          (R#federation_submit_request.group_id):32/big,
          (R#federation_submit_request.priority):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_submit_request.work_dir),
        flurm_protocol_pack:pack_string(R#federation_submit_request.std_out),
        flurm_protocol_pack:pack_string(R#federation_submit_request.std_err),
        <<(length(R#federation_submit_request.environment)):32/big>>,
        EnvBin,
        flurm_protocol_pack:pack_string(R#federation_submit_request.features)
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_submit_request(_) ->
    {ok, <<>>}.

%% Encode RESPONSE_FEDERATION_SUBMIT (2033)
encode_federation_submit_response(#federation_submit_response{} = R) ->
    Parts = [
        flurm_protocol_pack:pack_string(R#federation_submit_response.source_cluster),
        <<(R#federation_submit_response.job_id):32/big,
          (R#federation_submit_response.error_code):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_submit_response.error_msg)
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_submit_response(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:32>>}.

%% Encode REQUEST_FEDERATION_JOB_STATUS (2034)
encode_federation_job_status_request(#federation_job_status_request{} = R) ->
    Parts = [
        flurm_protocol_pack:pack_string(R#federation_job_status_request.source_cluster),
        <<(R#federation_job_status_request.job_id):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_job_status_request.job_id_str)
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_job_status_request(_) ->
    {ok, <<>>}.

%% Encode RESPONSE_FEDERATION_JOB_STATUS (2035)
encode_federation_job_status_response(#federation_job_status_response{} = R) ->
    Parts = [
        <<(R#federation_job_status_response.job_id):32/big,
          (R#federation_job_status_response.job_state):32/big,
          (R#federation_job_status_response.state_reason):32/big,
          (R#federation_job_status_response.exit_code):32/big,
          (R#federation_job_status_response.start_time):64/big,
          (R#federation_job_status_response.end_time):64/big>>,
        flurm_protocol_pack:pack_string(R#federation_job_status_response.nodes)
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_job_status_response(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:32, 0:64, 0:64, 0:32>>}.

%% Encode REQUEST_FEDERATION_JOB_CANCEL (2036)
encode_federation_job_cancel_request(#federation_job_cancel_request{} = R) ->
    Parts = [
        flurm_protocol_pack:pack_string(R#federation_job_cancel_request.source_cluster),
        <<(R#federation_job_cancel_request.job_id):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_job_cancel_request.job_id_str),
        <<(R#federation_job_cancel_request.signal):32/big>>
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_job_cancel_request(_) ->
    {ok, <<>>}.

%% Encode RESPONSE_FEDERATION_JOB_CANCEL (2037)
encode_federation_job_cancel_response(#federation_job_cancel_response{} = R) ->
    Parts = [
        <<(R#federation_job_cancel_response.job_id):32/big,
          (R#federation_job_cancel_response.error_code):32/big>>,
        flurm_protocol_pack:pack_string(R#federation_job_cancel_response.error_msg)
    ],
    {ok, iolist_to_binary(Parts)};
encode_federation_job_cancel_response(_) ->
    {ok, <<0:32, 0:32, 0:32>>}.

%% Encode REQUEST_UPDATE_FEDERATION (2064)
encode_update_federation_request(#update_federation_request{} = R) ->
    ActionCode = case R#update_federation_request.action of
        add_cluster -> 1;
        remove_cluster -> 2;
        update_settings -> 3;
        _ -> 0
    end,
    SettingsList = maps:to_list(R#update_federation_request.settings),
    SettingsBin = encode_settings_map(SettingsList),
    Parts = [
        <<ActionCode:32/big>>,
        flurm_protocol_pack:pack_string(R#update_federation_request.cluster_name),
        flurm_protocol_pack:pack_string(R#update_federation_request.host),
        <<(R#update_federation_request.port):32/big,
          (length(SettingsList)):32/big>>,
        SettingsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_update_federation_request(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:32, 0:32>>}.

%% Encode RESPONSE_UPDATE_FEDERATION (2065)
encode_update_federation_response(#update_federation_response{} = R) ->
    Parts = [
        <<(R#update_federation_response.error_code):32/big>>,
        flurm_protocol_pack:pack_string(R#update_federation_response.error_msg)
    ],
    {ok, iolist_to_binary(Parts)};
encode_update_federation_response(_) ->
    {ok, <<0:32, 0:32>>}.

%% Encode MSG_FED_JOB_SUBMIT (2070)
encode_fed_job_submit_msg(#fed_job_submit_msg{} = M) ->
    JobSpecBin = term_to_binary(M#fed_job_submit_msg.job_spec),
    Parts = [
        flurm_protocol_pack:pack_string(M#fed_job_submit_msg.federation_job_id),
        flurm_protocol_pack:pack_string(M#fed_job_submit_msg.origin_cluster),
        flurm_protocol_pack:pack_string(M#fed_job_submit_msg.target_cluster),
        <<(M#fed_job_submit_msg.submit_time):64/big>>,
        <<(byte_size(JobSpecBin)):32/big>>,
        JobSpecBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_job_submit_msg(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:64, 0:32>>}.

%% Encode MSG_FED_JOB_STARTED (2071)
encode_fed_job_started_msg(#fed_job_started_msg{} = M) ->
    Parts = [
        flurm_protocol_pack:pack_string(M#fed_job_started_msg.federation_job_id),
        flurm_protocol_pack:pack_string(M#fed_job_started_msg.running_cluster),
        <<(M#fed_job_started_msg.local_job_id):32/big,
          (M#fed_job_started_msg.start_time):64/big>>
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_job_started_msg(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:64>>}.

%% Encode MSG_FED_SIBLING_REVOKE (2072)
encode_fed_sibling_revoke_msg(#fed_sibling_revoke_msg{} = M) ->
    Parts = [
        flurm_protocol_pack:pack_string(M#fed_sibling_revoke_msg.federation_job_id),
        flurm_protocol_pack:pack_string(M#fed_sibling_revoke_msg.running_cluster),
        flurm_protocol_pack:pack_string(M#fed_sibling_revoke_msg.revoke_reason)
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_sibling_revoke_msg(_) ->
    {ok, <<0:32, 0:32, 0:32>>}.

%% Encode MSG_FED_JOB_COMPLETED (2073)
encode_fed_job_completed_msg(#fed_job_completed_msg{} = M) ->
    Parts = [
        flurm_protocol_pack:pack_string(M#fed_job_completed_msg.federation_job_id),
        flurm_protocol_pack:pack_string(M#fed_job_completed_msg.running_cluster),
        <<(M#fed_job_completed_msg.local_job_id):32/big,
          (M#fed_job_completed_msg.end_time):64/big,
          (M#fed_job_completed_msg.exit_code):32/signed-big>>
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_job_completed_msg(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:64, 0:32>>}.

%% Encode MSG_FED_JOB_FAILED (2074)
encode_fed_job_failed_msg(#fed_job_failed_msg{} = M) ->
    Parts = [
        flurm_protocol_pack:pack_string(M#fed_job_failed_msg.federation_job_id),
        flurm_protocol_pack:pack_string(M#fed_job_failed_msg.running_cluster),
        <<(M#fed_job_failed_msg.local_job_id):32/big,
          (M#fed_job_failed_msg.end_time):64/big,
          (M#fed_job_failed_msg.exit_code):32/signed-big>>,
        flurm_protocol_pack:pack_string(M#fed_job_failed_msg.error_msg)
    ],
    {ok, iolist_to_binary(Parts)};
encode_fed_job_failed_msg(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:64, 0:32, 0:32>>}.

%%%===================================================================
%%% Internal: Utility Functions
%%%===================================================================

%% Decode a list of strings
decode_string_list(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_string_list(Count, Binary, Acc) ->
    try
        {Str, Rest} = flurm_protocol_pack:unpack_string(Binary),
        decode_string_list(Count - 1, Rest, [Str | Acc])
    catch
        _:_ ->
            {lists:reverse(Acc), <<>>}
    end.

%% Encode a list of strings
encode_string_list(Strings) ->
    [flurm_protocol_pack:pack_string(S) || S <- Strings].

%% Decode settings map from binary
decode_settings_map(0, _Rest, Acc) ->
    Acc;
decode_settings_map(Count, Binary, Acc) ->
    try
        {Key, Rest1} = flurm_protocol_pack:unpack_string(Binary),
        {Value, Rest2} = flurm_protocol_pack:unpack_string(Rest1),
        decode_settings_map(Count - 1, Rest2, Acc#{Key => Value})
    catch
        _:_ ->
            Acc
    end.

%% Encode settings map to binary
encode_settings_map(SettingsList) ->
    lists:map(fun({Key, Value}) ->
        KeyBin = if is_binary(Key) -> Key;
                    is_atom(Key) -> atom_to_binary(Key, utf8);
                    true -> term_to_binary(Key)
                 end,
        ValueBin = if is_binary(Value) -> Value;
                      is_atom(Value) -> atom_to_binary(Value, utf8);
                      is_integer(Value) -> integer_to_binary(Value);
                      true -> term_to_binary(Value)
                   end,
        [flurm_protocol_pack:pack_string(KeyBin),
         flurm_protocol_pack:pack_string(ValueBin)]
    end, SettingsList).
