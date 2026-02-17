%%%-------------------------------------------------------------------
%%% @doc FLURM Codec - Job Message Encoding/Decoding
%%%
%%% This module handles encoding and decoding of job-related SLURM
%%% protocol messages:
%%%   - REQUEST_SUBMIT_BATCH_JOB (4003) / RESPONSE_SUBMIT_BATCH_JOB (4004)
%%%   - REQUEST_JOB_INFO (2003) / RESPONSE_JOB_INFO (2004)
%%%   - REQUEST_RESOURCE_ALLOCATION (4001) / RESPONSE_RESOURCE_ALLOCATION (4002)
%%%   - REQUEST_CANCEL_JOB (4006)
%%%   - REQUEST_KILL_JOB (5032)
%%%   - REQUEST_SUSPEND (5014)
%%%   - REQUEST_SIGNAL_JOB (5018)
%%%   - REQUEST_UPDATE_JOB (3001)
%%%   - REQUEST_JOB_WILL_RUN (4012) / RESPONSE_JOB_WILL_RUN (4013)
%%%   - REQUEST_JOB_READY (4019) / RESPONSE_JOB_READY (4020)
%%%   - REQUEST_JOB_ALLOCATION_INFO (4014) / RESPONSE_JOB_ALLOCATION_INFO (4015)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_codec_job).

-export([
    decode_body/2,
    encode_body/2
]).

-include("flurm_protocol.hrl").

%%%===================================================================
%%% Decode Functions
%%%===================================================================

%% REQUEST_JOB_INFO (2003)
decode_body(?REQUEST_JOB_INFO, Binary) ->
    decode_job_info_request(Binary);

%% REQUEST_RESOURCE_ALLOCATION (4001)
decode_body(?REQUEST_RESOURCE_ALLOCATION, Binary) ->
    decode_resource_allocation_request(Binary);

%% REQUEST_SUBMIT_BATCH_JOB (4003)
decode_body(?REQUEST_SUBMIT_BATCH_JOB, Binary) ->
    decode_batch_job_request(Binary);

%% REQUEST_CANCEL_JOB (4006)
decode_body(?REQUEST_CANCEL_JOB, Binary) ->
    decode_cancel_job_request(Binary);

%% REQUEST_KILL_JOB (5032)
decode_body(?REQUEST_KILL_JOB, Binary) ->
    decode_kill_job_request(Binary);

%% REQUEST_SUSPEND (5014)
decode_body(?REQUEST_SUSPEND, Binary) ->
    decode_suspend_request(Binary);

%% REQUEST_SIGNAL_JOB (5018)
decode_body(?REQUEST_SIGNAL_JOB, Binary) ->
    decode_signal_job_request(Binary);

%% REQUEST_UPDATE_JOB (3001)
decode_body(?REQUEST_UPDATE_JOB, Binary) ->
    decode_update_job_request(Binary);

%% REQUEST_JOB_WILL_RUN (4012)
decode_body(?REQUEST_JOB_WILL_RUN, Binary) ->
    decode_job_will_run_request(Binary);

%% RESPONSE_SUBMIT_BATCH_JOB (4004)
decode_body(?RESPONSE_SUBMIT_BATCH_JOB, Binary) ->
    decode_batch_job_response(Binary);

%% RESPONSE_JOB_INFO (2004)
decode_body(?RESPONSE_JOB_INFO, Binary) ->
    decode_job_info_response(Binary);

%% Unsupported message type
decode_body(_MsgType, _Binary) ->
    unsupported.

%%%===================================================================
%%% Encode Functions
%%%===================================================================

%% REQUEST_JOB_INFO (2003)
encode_body(?REQUEST_JOB_INFO, Req) ->
    encode_job_info_request(Req);

%% REQUEST_SUBMIT_BATCH_JOB (4003)
encode_body(?REQUEST_SUBMIT_BATCH_JOB, Req) ->
    encode_batch_job_request(Req);

%% REQUEST_CANCEL_JOB (4006)
encode_body(?REQUEST_CANCEL_JOB, Req) ->
    encode_cancel_job_request(Req);

%% REQUEST_KILL_JOB (5032)
encode_body(?REQUEST_KILL_JOB, Req) ->
    encode_kill_job_request(Req);

%% RESPONSE_JOB_WILL_RUN (4013)
encode_body(?RESPONSE_JOB_WILL_RUN, Resp) ->
    encode_job_will_run_response(Resp);

%% RESPONSE_JOB_READY (4020)
encode_body(?RESPONSE_JOB_READY, Resp) ->
    encode_job_ready_response(Resp);

%% RESPONSE_SUBMIT_BATCH_JOB (4004)
encode_body(?RESPONSE_SUBMIT_BATCH_JOB, Resp) ->
    encode_batch_job_response(Resp);

%% RESPONSE_JOB_INFO (2004)
encode_body(?RESPONSE_JOB_INFO, Resp) ->
    encode_job_info_response(Resp);

%% RESPONSE_RESOURCE_ALLOCATION (4002)
encode_body(?RESPONSE_RESOURCE_ALLOCATION, Resp) ->
    encode_resource_allocation_response(Resp);

%% RESPONSE_JOB_ALLOCATION_INFO (4015)
encode_body(?RESPONSE_JOB_ALLOCATION_INFO, Resp) ->
    encode_resource_allocation_response(Resp);

%% Unsupported message type
encode_body(_MsgType, _Body) ->
    unsupported.

%%%===================================================================
%%% Internal: Decode Helpers
%%%===================================================================

%% Decode REQUEST_JOB_INFO (2003)
decode_job_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, JobId:32/big, UserId:32/big, _Rest/binary>> ->
            {ok, #job_info_request{
                show_flags = ShowFlags,
                job_id = JobId,
                user_id = UserId
            }};
        <<ShowFlags:32/big, JobId:32/big>> ->
            {ok, #job_info_request{
                show_flags = ShowFlags,
                job_id = JobId
            }};
        <<ShowFlags:32/big>> ->
            {ok, #job_info_request{show_flags = ShowFlags}};
        <<>> ->
            {ok, #job_info_request{}};
        _ ->
            {error, invalid_job_info_request}
    end.

%% Decode REQUEST_RESOURCE_ALLOCATION (4001)
decode_resource_allocation_request(Binary) ->
    %% Resource allocation request has similar format to batch job
    %% but for interactive srun jobs
    decode_batch_job_request(Binary).

%% Decode REQUEST_SUBMIT_BATCH_JOB (4003)
%% SLURM batch job submission is a complex structure with many fields
decode_batch_job_request(Binary) ->
    try
        %% Parse the job description fields
        %% This is a simplified version - the full format is very complex
        case extract_batch_job_fields(Binary) of
            {ok, Fields} ->
                {ok, build_batch_job_record(Fields)};
            {error, _} = Error ->
                Error
        end
    catch
        _:Reason ->
            {error, {batch_job_decode_failed, Reason}}
    end.

%% Extract batch job fields from binary
extract_batch_job_fields(Binary) when byte_size(Binary) < 32 ->
    {ok, #{}};
extract_batch_job_fields(Binary) ->
    %% SLURM 22.05 job_desc_msg_t pack order starts with:
    %% site_factor(32), batch_features(str), cluster_features(str), clusters(str),
    %% contiguous(16), container(str), core_spec(16), task_dist(32),
    %% kill_on_node_fail(16), features(str), ...
    try
        <<_SiteFactor:32/big, Rest0/binary>> = Binary,
        {Rest1, _BatchFeatures} = skip_packstr(Rest0),
        {Rest2, _ClusterFeatures} = skip_packstr(Rest1),
        {Rest3, _Clusters} = skip_packstr(Rest2),
        <<_Contiguous:16/big, Rest4/binary>> = Rest3,
        {Rest5, _Container} = skip_packstr(Rest4),
        <<_CoreSpec:16/big, _TaskDist:32/big, _KillOnFail:16/big, Rest6/binary>> = Rest5,
        {Rest7, _Features} = skip_packstr(Rest6),
        <<_FedActive:64/big, _FedViable:64/big, Rest8/binary>> = Rest7,
        <<JobId:32/big, Rest9/binary>> = Rest8,
        {Rest10, JobIdStr} = skip_packstr(Rest9),
        {Rest11, Name} = skip_packstr(Rest10),
        {_RestFinal, Partition} = skip_packstr(Rest11),

        %% Extract more fields from later in the structure
        {MinNodes, MaxNodes, NumTasks, CpusPerTask, TimeLimit, UserId, GroupId} =
            extract_job_resources(Binary),

        {ok, #{
            job_id => JobId,
            job_id_str => strip_null(JobIdStr),
            name => strip_null(Name),
            partition => strip_null(Partition),
            min_nodes => MinNodes,
            max_nodes => MaxNodes,
            num_tasks => NumTasks,
            cpus_per_task => CpusPerTask,
            time_limit => TimeLimit,
            user_id => UserId,
            group_id => GroupId
        }}
    catch
        _:_ ->
            {ok, #{}}
    end.

%% Build batch job record from extracted fields
build_batch_job_record(Fields) ->
    #batch_job_request{
        job_id = maps:get(job_id, Fields, 0),
        job_id_str = maps:get(job_id_str, Fields, <<>>),
        name = maps:get(name, Fields, <<>>),
        partition = maps:get(partition, Fields, <<>>),
        min_nodes = maps:get(min_nodes, Fields, 1),
        max_nodes = maps:get(max_nodes, Fields, 1),
        num_tasks = maps:get(num_tasks, Fields, 1),
        cpus_per_task = maps:get(cpus_per_task, Fields, 1),
        time_limit = maps:get(time_limit, Fields, 0),
        user_id = maps:get(user_id, Fields, 0),
        group_id = maps:get(group_id, Fields, 0)
    }.

%% Extract job resource fields from binary
extract_job_resources(Binary) when byte_size(Binary) < 100 ->
    {1, 1, 1, 1, 0, 0, 0};
extract_job_resources(Binary) ->
    %% These fields are at known offsets in the structure
    %% For now, return defaults - full implementation would parse the complete structure
    Size = byte_size(Binary),
    try
        %% Try to extract from known positions (these may vary by SLURM version)
        MinNodes = extract_uint32_at(Binary, Size - 200, 1),
        MaxNodes = extract_uint32_at(Binary, Size - 196, 1),
        NumTasks = extract_uint32_at(Binary, Size - 188, 1),
        CpusPerTask = extract_uint32_at(Binary, Size - 184, 1),
        TimeLimit = extract_uint32_at(Binary, Size - 179, 0),
        UserId = extract_uint32_at(Binary, Size - 50, 0),
        GroupId = extract_uint32_at(Binary, Size - 46, 0),
        {MinNodes, MaxNodes, NumTasks, CpusPerTask, TimeLimit, UserId, GroupId}
    catch
        _:_ ->
            {1, 1, 1, 1, 0, 0, 0}
    end.

%% Extract uint32 at offset, with default fallback
extract_uint32_at(Binary, Offset, Default) when Offset >= 0, Offset + 4 =< byte_size(Binary) ->
    <<_:Offset/binary, Value:32/big, _/binary>> = Binary,
    case Value of
        16#FFFFFFFE -> Default;  % NO_VAL
        16#FFFFFFFF -> Default;  % NO_VAL
        _ -> Value
    end;
extract_uint32_at(_, _, Default) ->
    Default.

%% Decode REQUEST_CANCEL_JOB (4006)
decode_cancel_job_request(Binary) ->
    case Binary of
        <<JobId:32/big, StepId:32/big, Signal:32/big, Flags:32/big, _Rest/binary>> ->
            {ok, #cancel_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = Signal,
                flags = Flags
            }};
        <<JobId:32/big, StepId:32/big, Signal:32/big>> ->
            {ok, #cancel_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = Signal
            }};
        <<JobId:32/big, StepId:32/big>> ->
            {ok, #cancel_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = 9  % SIGKILL default
            }};
        <<JobId:32/big>> ->
            {ok, #cancel_job_request{
                job_id = JobId,
                signal = 9
            }};
        <<>> ->
            {ok, #cancel_job_request{}};
        _ ->
            {error, invalid_cancel_job_request}
    end.

%% Decode REQUEST_KILL_JOB (5032)
decode_kill_job_request(Binary) ->
    case Binary of
        <<JobId:32/big, StepId:32/big, Signal:32/big, Flags:32/big, Rest/binary>> ->
            {Sibling, _} = unpack_string_safe(Rest),
            {ok, #kill_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = Signal,
                flags = Flags,
                sibling = Sibling
            }};
        <<JobId:32/big, StepId:32/big, Signal:32/big, Flags:32/big>> ->
            {ok, #kill_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = Signal,
                flags = Flags
            }};
        <<JobId:32/big, StepId:32/big, Signal:32/big>> ->
            {ok, #kill_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = Signal
            }};
        <<JobId:32/big>> ->
            {ok, #kill_job_request{job_id = JobId}};
        <<>> ->
            {ok, #kill_job_request{}};
        _ ->
            {error, invalid_kill_job_request}
    end.

%% Decode REQUEST_SUSPEND (5014)
decode_suspend_request(Binary) ->
    case Binary of
        <<JobId:32/big, Suspend:32/big, _Rest/binary>> ->
            {ok, #suspend_request{
                job_id = JobId,
                suspend = Suspend =/= 0
            }};
        <<JobId:32/big>> ->
            {ok, #suspend_request{job_id = JobId}};
        <<>> ->
            {ok, #suspend_request{}};
        _ ->
            {error, invalid_suspend_request}
    end.

%% Decode REQUEST_SIGNAL_JOB (5018)
decode_signal_job_request(Binary) ->
    case Binary of
        <<JobId:32/big, StepId:32/big, Signal:32/big, Flags:32/big, _Rest/binary>> ->
            {ok, #signal_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = Signal,
                flags = Flags
            }};
        <<JobId:32/big, StepId:32/big, Signal:32/big>> ->
            {ok, #signal_job_request{
                job_id = JobId,
                step_id = StepId,
                signal = Signal
            }};
        <<JobId:32/big, Signal:32/big>> ->
            {ok, #signal_job_request{
                job_id = JobId,
                signal = Signal
            }};
        <<JobId:32/big>> ->
            {ok, #signal_job_request{job_id = JobId}};
        <<>> ->
            {ok, #signal_job_request{}};
        _ ->
            {error, invalid_signal_job_request}
    end.

%% Decode REQUEST_UPDATE_JOB (3001)
decode_update_job_request(Binary) ->
    try
        decode_update_job_request_walk(Binary)
    catch
        _:Reason ->
            lager:error("update_job decode failed: ~p", [Reason]),
            {error, {update_job_decode_failed, Reason}}
    end.

decode_update_job_request_walk(Binary) when byte_size(Binary) < 64 ->
    case flurm_protocol_pack:unpack_string(Binary) of
        {ok, JobIdStr, _Rest} when byte_size(JobIdStr) > 0 ->
            JobId = parse_job_id(JobIdStr),
            {ok, #update_job_request{
                job_id = JobId,
                job_id_str = ensure_binary(JobIdStr)
            }};
        _ ->
            {ok, #update_job_request{}}
    end;
decode_update_job_request_walk(Binary) ->
    Size = byte_size(Binary),
    <<_SiteFactor:32/big, Rest0/binary>> = Binary,
    {Rest1, _} = skip_packstr(Rest0),
    {Rest2, _} = skip_packstr(Rest1),
    {Rest3, _} = skip_packstr(Rest2),
    <<_Contiguous:16/big, Rest4/binary>> = Rest3,
    {Rest5, _} = skip_packstr(Rest4),
    <<_CoreSpec:16/big, _TaskDist:32/big, _KillOnFail:16/big, Rest6/binary>> = Rest5,
    {Rest7, _} = skip_packstr(Rest6),
    <<_FedActive:64/big, _FedViable:64/big, Rest8/binary>> = Rest7,
    <<PackedJobId:32/big, Rest9/binary>> = Rest8,
    {Rest10, JobIdStr} = skip_packstr(Rest9),
    {_Rest11, Name} = skip_packstr(Rest10),

    JobId = case PackedJobId of
        16#FFFFFFFE ->
            case JobIdStr of
                <<>> -> 0;
                _ -> parse_job_id(JobIdStr)
            end;
        _ -> PackedJobId
    end,

    PriorityOffset = Size - 358,
    TimeLimitOffset = Size - 179,

    Priority = case PriorityOffset >= 0 andalso PriorityOffset + 4 =< Size of
        true ->
            <<_:PriorityOffset/binary, P:32/big, _/binary>> = Binary,
            P;
        false -> ?SLURM_NO_VAL
    end,

    TimeLimit = case TimeLimitOffset >= 0 andalso TimeLimitOffset + 4 =< Size of
        true ->
            <<_:TimeLimitOffset/binary, T:32/big, _/binary>> = Binary,
            T;
        false -> ?SLURM_NO_VAL
    end,

    {ok, #update_job_request{
        job_id = JobId,
        job_id_str = strip_null(JobIdStr),
        priority = Priority,
        time_limit = TimeLimit,
        name = strip_null(Name)
    }}.

%% Decode REQUEST_JOB_WILL_RUN (4012)
decode_job_will_run_request(Binary) ->
    try
        case Binary of
            <<JobId:32/big, Rest/binary>> when JobId > 0 ->
                Partition = find_partition_in_binary(Rest),
                MinNodes = find_nodes_in_binary(Rest),
                {ok, #job_will_run_request{
                    job_id = JobId,
                    partition = Partition,
                    min_nodes = MinNodes
                }};
            _ ->
                Partition = find_partition_in_binary(Binary),
                MinNodes = find_nodes_in_binary(Binary),
                MinCpus = find_cpus_in_binary(Binary),
                {ok, #job_will_run_request{
                    partition = Partition,
                    min_nodes = MinNodes,
                    min_cpus = MinCpus
                }}
        end
    catch
        _:Reason ->
            {error, {job_will_run_decode_failed, Reason}}
    end.

%% Decode RESPONSE_SUBMIT_BATCH_JOB (4004)
decode_batch_job_response(Binary) ->
    case Binary of
        <<JobId:32/big, StepId:32/big, ErrorCode:32/big, Rest/binary>> ->
            {JobSubmitUserMsg, _} = unpack_string_safe(Rest),
            {ok, #batch_job_response{
                job_id = JobId,
                step_id = StepId,
                error_code = ErrorCode,
                job_submit_user_msg = JobSubmitUserMsg
            }};
        <<JobId:32/big, StepId:32/big, ErrorCode:32/big>> ->
            {ok, #batch_job_response{
                job_id = JobId,
                step_id = StepId,
                error_code = ErrorCode
            }};
        <<JobId:32/big, StepId:32/big>> ->
            {ok, #batch_job_response{
                job_id = JobId,
                step_id = StepId
            }};
        <<JobId:32/big>> ->
            {ok, #batch_job_response{job_id = JobId}};
        <<>> ->
            {ok, #batch_job_response{}};
        _ ->
            {error, invalid_batch_job_response}
    end.

%% Decode RESPONSE_JOB_INFO (2004)
decode_job_info_response(Binary) ->
    case Binary of
        <<LastUpdate:64/big, JobCount:32/big, Rest/binary>> ->
            %% Jobs are encoded after count, parse them
            {ok, #job_info_response{
                last_update = LastUpdate,
                job_count = JobCount,
                jobs = decode_job_info_list(Rest, JobCount, [])
            }};
        <<LastUpdate:64/big, JobCount:32/big>> ->
            {ok, #job_info_response{
                last_update = LastUpdate,
                job_count = JobCount
            }};
        <<>> ->
            {ok, #job_info_response{}};
        _ ->
            {ok, #job_info_response{}}
    end.

%% Decode list of job_info records (simplified)
decode_job_info_list(_Binary, 0, Acc) ->
    lists:reverse(Acc);
decode_job_info_list(_Binary, _Count, Acc) ->
    %% Full job_info parsing is complex, return empty for now
    lists:reverse(Acc).

%%%===================================================================
%%% Internal: Encode Helpers
%%%===================================================================

%% Encode REQUEST_JOB_INFO
encode_job_info_request(#job_info_request{show_flags = ShowFlags, job_id = JobId, user_id = UserId}) ->
    {ok, <<ShowFlags:32/big, JobId:32/big, UserId:32/big>>};
encode_job_info_request(_) ->
    {ok, <<0:32, 0:32, 0:32>>}.

%% Encode REQUEST_SUBMIT_BATCH_JOB (simplified - full implementation is complex)
encode_batch_job_request(#batch_job_request{} = _Req) ->
    %% Full batch job encoding is very complex
    %% This is a placeholder - the real implementation would need to pack
    %% all fields in the exact SLURM wire format
    {error, batch_job_encode_not_implemented};
encode_batch_job_request(_) ->
    {error, invalid_batch_job_request}.

%% Encode REQUEST_CANCEL_JOB
encode_cancel_job_request(#cancel_job_request{job_id = JobId, step_id = StepId, signal = Signal, flags = Flags}) ->
    {ok, <<JobId:32/big, StepId:32/big, Signal:32/big, Flags:32/big>>};
encode_cancel_job_request(_) ->
    {ok, <<0:32, ?SLURM_NO_VAL:32, 9:32, 0:32>>}.

%% Encode REQUEST_KILL_JOB
encode_kill_job_request(#kill_job_request{job_id = JobId, step_id = StepId, signal = Signal, flags = Flags, sibling = Sibling}) ->
    SiblingBin = flurm_protocol_pack:pack_string(ensure_binary(Sibling)),
    {ok, <<JobId:32/big, StepId:32/big, Signal:32/big, Flags:32/big, SiblingBin/binary>>};
encode_kill_job_request(_) ->
    SiblingBin = flurm_protocol_pack:pack_string(<<>>),
    {ok, <<0:32, ?SLURM_NO_VAL:32, 9:32, 0:32, SiblingBin/binary>>}.

%% Encode RESPONSE_JOB_WILL_RUN
encode_job_will_run_response(#job_will_run_response{
    job_id = JobId,
    start_time = StartTime,
    node_list = NodeList,
    proc_cnt = ProcCnt,
    error_code = ErrorCode
}) ->
    Parts = [
        <<JobId:32/big>>,
        flurm_protocol_pack:pack_time(StartTime),
        flurm_protocol_pack:pack_string(ensure_binary(NodeList)),
        <<ProcCnt:32/big>>,
        <<ErrorCode:32/big>>
    ],
    {ok, iolist_to_binary(Parts)};
encode_job_will_run_response(_) ->
    {ok, <<0:32, 0:64, 0:32, 0:32, 0:32>>}.

%% Encode RESPONSE_JOB_READY
encode_job_ready_response(#{return_code := ReturnCode}) ->
    {ok, <<ReturnCode:32/signed-big>>};
encode_job_ready_response(_) ->
    {ok, <<1:32/signed-big>>}.

%% Encode RESPONSE_SUBMIT_BATCH_JOB
encode_batch_job_response(#batch_job_response{job_id = JobId, step_id = StepId, error_code = ErrorCode, job_submit_user_msg = Msg}) ->
    MsgBin = flurm_protocol_pack:pack_string(ensure_binary(Msg)),
    {ok, <<JobId:32/big, StepId:32/big, ErrorCode:32/big, MsgBin/binary>>};
encode_batch_job_response(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:32>>}.

%% Encode RESPONSE_JOB_INFO (2004)
encode_job_info_response(#job_info_response{last_update = LastUpdate, job_count = JobCount, jobs = Jobs}) ->
    JobsBin = [encode_single_job_info(J) || J <- Jobs],
    Parts = [
        flurm_protocol_pack:pack_time(LastUpdate),
        <<JobCount:32/big>>,
        JobsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_job_info_response(_) ->
    {ok, <<0:64, 0:32>>}.

%% Encode a single job_info record (simplified)
encode_single_job_info(#job_info{} = J) ->
    [
        <<(J#job_info.job_id):32/big>>,
        flurm_protocol_pack:pack_string(J#job_info.name),
        <<(J#job_info.user_id):32/big>>,
        <<(J#job_info.group_id):32/big>>,
        <<(J#job_info.job_state):32/big>>,
        flurm_protocol_pack:pack_string(J#job_info.partition),
        <<(J#job_info.num_nodes):32/big>>,
        <<(J#job_info.num_cpus):32/big>>,
        <<(J#job_info.time_limit):32/big>>,
        flurm_protocol_pack:pack_time(J#job_info.start_time),
        flurm_protocol_pack:pack_time(J#job_info.end_time),
        flurm_protocol_pack:pack_string(J#job_info.nodes)
    ];
encode_single_job_info(_) ->
    [].

%% Encode RESPONSE_RESOURCE_ALLOCATION (4002)
encode_resource_allocation_response(#resource_allocation_response{} = Resp) ->
    %% SLURM 22.05 resource_allocation_response_msg format
    CpusPerNodeBin = encode_uint16_array(Resp#resource_allocation_response.cpus_per_node),
    CpuCountReps = lists:duplicate(Resp#resource_allocation_response.num_cpu_groups, 1),
    CpuCountRepsBin = encode_uint32_array(CpuCountReps),
    NodeAddrsBin = encode_node_addrs(Resp#resource_allocation_response.node_addrs),

    Parts = [
        %% error_code
        <<(Resp#resource_allocation_response.error_code):32/big>>,
        %% job_id
        <<(Resp#resource_allocation_response.job_id):32/big>>,
        %% node_list
        flurm_protocol_pack:pack_string(ensure_binary(Resp#resource_allocation_response.node_list)),
        %% num_cpu_groups
        <<(Resp#resource_allocation_response.num_cpu_groups):32/big>>,
        %% cpus_per_node (pack16_array)
        CpusPerNodeBin,
        %% cpu_count_reps (pack32_array)
        CpuCountRepsBin,
        %% node_cnt
        <<(Resp#resource_allocation_response.node_cnt):32/big>>,
        %% partition
        flurm_protocol_pack:pack_string(ensure_binary(Resp#resource_allocation_response.partition)),
        %% alias_list
        flurm_protocol_pack:pack_string(ensure_binary(Resp#resource_allocation_response.alias_list)),
        %% select_jobinfo (plugin_id + empty data)
        <<109:32/big>>,  %% cons_tres plugin
        %% node_addr (slurm_addr_t array)
        NodeAddrsBin,
        %% job_submit_user_msg
        flurm_protocol_pack:pack_string(ensure_binary(Resp#resource_allocation_response.job_submit_user_msg))
    ],
    {ok, iolist_to_binary(Parts)};
encode_resource_allocation_response(_) ->
    {ok, <<0:32, 0:32, 0:32, 0:32, 0:32, 0:32, 0:32, 0:32, 0:32, 109:32, 0:32, 0:32>>}.

%% Encode uint16 array
encode_uint16_array([]) ->
    <<0:32/big>>;
encode_uint16_array(List) ->
    Count = length(List),
    Elements = << <<V:16/big>> || V <- List >>,
    <<Count:32/big, Elements/binary>>.

%% Encode uint32 array
encode_uint32_array([]) ->
    <<0:32/big>>;
encode_uint32_array(List) ->
    Count = length(List),
    Elements = << <<V:32/big>> || V <- List >>,
    <<Count:32/big, Elements/binary>>.

%% Encode node addresses
encode_node_addrs([]) ->
    <<0:32/big>>;
encode_node_addrs(Addrs) ->
    Count = length(Addrs),
    AddrsBin = [encode_single_addr(A) || A <- Addrs],
    [<<Count:32/big>>, AddrsBin].

encode_single_addr({IP, Port}) when is_tuple(IP), tuple_size(IP) =:= 4 ->
    {A, B, C, D} = IP,
    %% AF_INET = 2, pack as sockaddr_in
    <<2:16/big, Port:16/big, A, B, C, D, 0:64>>;
encode_single_addr(_) ->
    <<0:16/big, 0:16/big, 0:32, 0:64>>.

%%%===================================================================
%%% Internal: Utility Functions
%%%===================================================================

%% Skip a packstr and return remaining binary + extracted string
skip_packstr(<<16#FFFFFFFF:32/big, Rest/binary>>) ->
    %% NULL string (NO_VAL)
    {Rest, <<>>};
skip_packstr(<<Len:32/big, Rest/binary>>) when Len =< byte_size(Rest) ->
    <<Str:Len/binary, Rest2/binary>> = Rest,
    {Rest2, Str};
skip_packstr(Binary) ->
    {Binary, <<>>}.

%% Unpack string safely
unpack_string_safe(<<Len:32/big, Rest/binary>>) when Len =< byte_size(Rest), Len < 16#FFFFFFFE ->
    <<Str:Len/binary, Rest2/binary>> = Rest,
    {Str, Rest2};
unpack_string_safe(<<16#FFFFFFFF:32, Rest/binary>>) ->
    {<<>>, Rest};
unpack_string_safe(_) ->
    {<<>>, <<>>}.

%% Strip null terminators
strip_null(<<>>) -> <<>>;
strip_null(Bin) when is_binary(Bin) ->
    case binary:match(Bin, <<0>>) of
        {Pos, _} -> binary:part(Bin, 0, Pos);
        nomatch -> Bin
    end;
strip_null(_) -> <<>>.

%% Ensure value is binary
ensure_binary(undefined) -> <<>>;
ensure_binary(null) -> <<>>;
ensure_binary(Bin) when is_binary(Bin) -> Bin;
ensure_binary(List) when is_list(List) -> list_to_binary(List);
ensure_binary(_) -> <<>>.

%% Parse job ID from string
parse_job_id(<<>>) -> 0;
parse_job_id(Str) when is_binary(Str) ->
    try
        %% Handle array job notation like "123_45"
        StrClean = strip_null(Str),
        case binary:split(StrClean, <<"_">>) of
            [BaseId, _ArrayIdx] ->
                binary_to_integer(BaseId);
            [BaseId] ->
                binary_to_integer(BaseId)
        end
    catch
        _:_ -> 0
    end;
parse_job_id(_) -> 0.

%% Find partition in binary
find_partition_in_binary(Binary) ->
    case binary:match(Binary, <<"partition=">>) of
        {Start, Len} ->
            <<_:Start/binary, _:Len/binary, Rest/binary>> = Binary,
            extract_until_delimiter(Rest);
        nomatch ->
            <<>>
    end.

find_nodes_in_binary(_Binary) -> 1.
find_cpus_in_binary(_Binary) -> 1.

extract_until_delimiter(Binary) ->
    extract_until_delimiter(Binary, 0).

extract_until_delimiter(Binary, Pos) when Pos < byte_size(Binary) ->
    case binary:at(Binary, Pos) of
        C when C =:= $\s; C =:= $\n; C =:= $\r; C =:= 0 ->
            <<Result:Pos/binary, _/binary>> = Binary,
            Result;
        _ ->
            extract_until_delimiter(Binary, Pos + 1)
    end;
extract_until_delimiter(Binary, Pos) ->
    binary:part(Binary, 0, min(Pos, byte_size(Binary))).
