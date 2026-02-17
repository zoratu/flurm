%%%-------------------------------------------------------------------
%%% @doc FLURM Codec - Step Message Encoding/Decoding
%%%
%%% This module handles encoding and decoding of job step-related SLURM
%%% protocol messages:
%%%   - REQUEST_JOB_STEP_CREATE (5001) / RESPONSE_JOB_STEP_CREATE (5002)
%%%   - REQUEST_JOB_STEP_INFO (5003) / RESPONSE_JOB_STEP_INFO (5004)
%%%   - REQUEST_LAUNCH_TASKS (6001) / RESPONSE_LAUNCH_TASKS (6002)
%%%   - MESSAGE_TASK_EXIT (6003)
%%%   - REQUEST_COMPLETE_PROLOG (5019)
%%%   - MESSAGE_EPILOG_COMPLETE (6012)
%%%   - RESPONSE_REATTACH_TASKS (6008)
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_codec_step).

-export([
    decode_body/2,
    encode_body/2
]).

-include("flurm_protocol.hrl").

%%%===================================================================
%%% Decode Functions
%%%===================================================================

%% REQUEST_JOB_STEP_CREATE (5001)
decode_body(?REQUEST_JOB_STEP_CREATE, Binary) ->
    decode_job_step_create_request(Binary);

%% REQUEST_JOB_STEP_INFO (5003)
decode_body(?REQUEST_JOB_STEP_INFO, Binary) ->
    decode_job_step_info_request(Binary);

%% REQUEST_COMPLETE_PROLOG (5019)
decode_body(?REQUEST_COMPLETE_PROLOG, Binary) ->
    decode_complete_prolog_request(Binary);

%% MESSAGE_EPILOG_COMPLETE (6012)
decode_body(?MESSAGE_EPILOG_COMPLETE, Binary) ->
    decode_epilog_complete_msg(Binary);

%% MESSAGE_TASK_EXIT (6003)
decode_body(?MESSAGE_TASK_EXIT, Binary) ->
    decode_task_exit_msg(Binary);

%% REQUEST_LAUNCH_TASKS (6001)
decode_body(?REQUEST_LAUNCH_TASKS, Binary) ->
    decode_launch_tasks_request(Binary);

%% Unsupported message type
decode_body(_MsgType, _Binary) ->
    unsupported.

%%%===================================================================
%%% Encode Functions
%%%===================================================================

%% RESPONSE_JOB_STEP_CREATE (5002)
encode_body(?RESPONSE_JOB_STEP_CREATE, Resp) ->
    encode_job_step_create_response(Resp);

%% RESPONSE_JOB_STEP_INFO (5004)
encode_body(?RESPONSE_JOB_STEP_INFO, Resp) ->
    encode_job_step_info_response(Resp);

%% RESPONSE_LAUNCH_TASKS (6002)
encode_body(?RESPONSE_LAUNCH_TASKS, Resp) ->
    encode_launch_tasks_response(Resp);

%% RESPONSE_REATTACH_TASKS (6008)
encode_body(?RESPONSE_REATTACH_TASKS, Resp) ->
    encode_reattach_tasks_response(Resp);

%% MESSAGE_TASK_EXIT (6003)
encode_body(?MESSAGE_TASK_EXIT, Resp) ->
    encode_task_exit_msg(Resp);

%% Unsupported message type
encode_body(_MsgType, _Body) ->
    unsupported.

%%%===================================================================
%%% Internal: Decode Helpers
%%%===================================================================

%% Decode REQUEST_JOB_STEP_CREATE (5001)
decode_job_step_create_request(Binary) ->
    try
        case Binary of
            <<JobId:32/big, StepId:32/big, UserId:32/big, MinNodes:32/big,
              MaxNodes:32/big, NumTasks:32/big, CpusPerTask:32/big, TimeLimit:32/big,
              Flags:32/big, Immediate:32/big, Rest/binary>> ->
                {Name, _Rest2} = case flurm_protocol_pack:unpack_string(Rest) of
                    {ok, N, R} -> {ensure_binary(N), R};
                    _ -> {<<>>, Rest}
                end,
                {ok, #job_step_create_request{
                    job_id = JobId,
                    step_id = normalize_step_id(StepId),
                    user_id = UserId,
                    min_nodes = max(1, MinNodes),
                    max_nodes = max(1, MaxNodes),
                    num_tasks = max(1, NumTasks),
                    cpus_per_task = max(1, CpusPerTask),
                    time_limit = TimeLimit,
                    flags = Flags,
                    immediate = Immediate,
                    name = Name
                }};
            <<JobId:32/big, StepId:32/big, Rest/binary>> ->
                {ok, #job_step_create_request{
                    job_id = JobId,
                    step_id = normalize_step_id(StepId),
                    name = extract_name_from_binary(Rest)
                }};
            <<JobId:32/big>> ->
                {ok, #job_step_create_request{job_id = JobId}};
            _ ->
                {error, invalid_job_step_create_request}
        end
    catch
        _:Reason ->
            {error, {job_step_create_decode_failed, Reason}}
    end.

%% Decode REQUEST_JOB_STEP_INFO (5003)
decode_job_step_info_request(Binary) ->
    case Binary of
        <<ShowFlags:32/big, JobId:32/big, StepId:32/big, _Rest/binary>> ->
            {ok, #job_step_info_request{
                show_flags = ShowFlags,
                job_id = JobId,
                step_id = normalize_step_id(StepId)
            }};
        <<ShowFlags:32/big, JobId:32/big>> ->
            {ok, #job_step_info_request{
                show_flags = ShowFlags,
                job_id = JobId
            }};
        <<ShowFlags:32/big>> ->
            {ok, #job_step_info_request{
                show_flags = ShowFlags
            }};
        <<>> ->
            {ok, #job_step_info_request{}};
        _ ->
            {error, invalid_job_step_info_request}
    end.

%% Decode REQUEST_COMPLETE_PROLOG (5019)
decode_complete_prolog_request(Binary) ->
    case Binary of
        <<JobId:32/big, PrologRc:32/signed-big, Rest/binary>> ->
            {NodeName, _} = unpack_string_safe(Rest),
            {ok, #complete_prolog_request{
                job_id = JobId,
                prolog_rc = PrologRc,
                node_name = NodeName
            }};
        <<JobId:32/big, PrologRc:32/signed-big>> ->
            {ok, #complete_prolog_request{
                job_id = JobId,
                prolog_rc = PrologRc
            }};
        <<JobId:32/big>> ->
            {ok, #complete_prolog_request{job_id = JobId}};
        <<>> ->
            {ok, #complete_prolog_request{}};
        _ ->
            {error, invalid_complete_prolog_request}
    end.

%% Decode MESSAGE_EPILOG_COMPLETE (6012)
decode_epilog_complete_msg(Binary) ->
    case Binary of
        <<JobId:32/big, EpilogRc:32/signed-big, Rest/binary>> ->
            {NodeName, _} = unpack_string_safe(Rest),
            {ok, #epilog_complete_msg{
                job_id = JobId,
                epilog_rc = EpilogRc,
                node_name = NodeName
            }};
        <<JobId:32/big, EpilogRc:32/signed-big>> ->
            {ok, #epilog_complete_msg{
                job_id = JobId,
                epilog_rc = EpilogRc
            }};
        <<JobId:32/big>> ->
            {ok, #epilog_complete_msg{job_id = JobId}};
        <<>> ->
            {ok, #epilog_complete_msg{}};
        _ ->
            {error, invalid_epilog_complete_msg}
    end.

%% Decode MESSAGE_TASK_EXIT (6003)
decode_task_exit_msg(Binary) ->
    case Binary of
        <<ReturnCode:32/signed-big, _NumTasks:32/big, Rest0/binary>> ->
            %% Parse task_id_list (pack32_array format: count + elements)
            <<ArrayCount:32/big, Rest1/binary>> = Rest0,
            {TaskIds, Rest2} = unpack_uint32_array(ArrayCount, Rest1),
            %% Parse step_id: job_id(32) + step_id(32) + step_het_comp(32)
            <<JobId:32/big, StepId:32/big, StepHetComp:32/big, _/binary>> = Rest2,
            {ok, #task_exit_msg{
                job_id = JobId,
                step_id = normalize_step_id(StepId),
                step_het_comp = StepHetComp,
                task_ids = TaskIds,
                return_code = ReturnCode
            }};
        _ ->
            {ok, #task_exit_msg{}}
    end.

%% Decode REQUEST_LAUNCH_TASKS (6001)
%% This is a complex message format - simplified parsing
decode_launch_tasks_request(Binary) ->
    try
        %% Parse step_id structure
        <<JobId:32/big, StepId:32/big, StepHetComp:32/big, Rest0/binary>> = Binary,

        %% Parse uid, gid
        <<Uid:32/big, Gid:32/big, Rest1/binary>> = Rest0,

        %% Parse user_name string
        {UserName, Rest2} = unpack_string_safe(Rest1),

        %% Parse gids array (count + array)
        <<NgIds:32/big, Rest3/binary>> = Rest2,
        {GIds, Rest4} = unpack_uint32_array(NgIds, Rest3),

        %% Parse het_job fields
        <<_HetJobNodeOffset:32/big, _HetJobId:32/big, HetJobNnodes:32/big, Rest5/binary>> = Rest4,

        %% Skip het_job_tids arrays if HetJobNnodes > 0
        Rest6 = skip_het_job_tids(HetJobNnodes, Rest5),

        %% Parse more het_job fields
        <<_HetJobNtasks:32/big, Rest7/binary>> = Rest6,

        %% Skip het_job_tid_offsets if HetJobNnodes > 0
        Rest8 = skip_het_job_tid_offsets(HetJobNnodes, Rest7),

        %% Continue with remaining het_job fields
        <<_HetJobOffset:32/big, _HetJobStepCnt:32/big, _HetJobTaskOffset:32/big, Rest9/binary>> = Rest8,

        %% Parse het_job_node_list string
        {_HetJobNodeList, Rest10} = unpack_string_safe(Rest9),

        %% Parse mpi_plugin_id
        <<_MpiPluginId:32/big, Rest11/binary>> = Rest10,

        %% Parse task/node counts
        <<Ntasks:32/big, _NtasksPerBoard:16/big, _NtasksPerCore:16/big,
          _NtasksPerTres:16/big, _NtasksPerSocket:16/big, Rest12/binary>> = Rest11,

        %% Parse memory limits
        <<JobMemLim:64/big, StepMemLim:64/big, Rest13/binary>> = Rest12,

        %% Parse nnodes
        <<Nnodes:32/big, Rest14/binary>> = Rest13,

        %% Parse cpus_per_task
        <<_CpusPerTask:16/big, Rest15/binary>> = Rest14,

        %% Parse tres_per_task string
        {_TresPerTask, Rest16} = unpack_string_safe(Rest15),

        %% Parse threads_per_core, task_dist, node_cpus, job_core_spec, accel_bind_type
        <<_ThreadsPerCore:16/big, TaskDist:32/big, _NodeCpus:16/big,
          _JobCoreSpec:16/big, _AccelBindType:16/big, _Rest17/binary>> = Rest16,

        %% Find env vars position in the ENTIRE binary
        case find_env_vars_position(Binary) of
            {ok, EnvOffset} ->
                <<_Skip:EnvOffset/binary, RestFromEnv/binary>> = Binary,

                %% Parse env array
                <<Envc:32/big, RestEnvStrings/binary>> = RestFromEnv,
                {Env, RestAfterEnv} = unpack_string_array(Envc, RestEnvStrings),

                %% Parse spank_job_env array
                <<SpankEnvCount:32/big, RestSpank/binary>> = RestAfterEnv,
                {_SpankEnv, RestAfterSpank} = unpack_string_array(SpankEnvCount, RestSpank),

                %% Parse container string
                {_Container, RestAfterContainer} = unpack_string_safe(RestAfterSpank),

                %% Parse cwd
                {Cwd, RestAfterCwd} = unpack_string_safe(RestAfterContainer),

                %% Parse cpu_bind_type and cpu_bind
                <<CpuBindType:16/big, RestCpuBind/binary>> = RestAfterCwd,
                {CpuBind, RestAfterCpuBind} = unpack_string_safe(RestCpuBind),

                %% Parse mem_bind_type and mem_bind
                <<_MemBindType:16/big, RestMemBind/binary>> = RestAfterCpuBind,
                {_MemBind, RestAfterMemBind} = unpack_string_safe(RestMemBind),

                %% Parse argv array
                <<Argc:32/big, RestArgvStrings/binary>> = RestAfterMemBind,
                {Argv, RestAfterArgv} = unpack_string_array(Argc, RestArgvStrings),

                %% Extract ports
                RespPort = extract_resp_port_before_env(Binary, EnvOffset),
                IoPort = extract_io_port_after_argv(RestAfterArgv),

                %% Use defaults for the rest
                Flags = 0,
                Ofname = <<>>,
                Efname = <<>>,
                Ifname = <<>>,
                TasksToLaunch = [Ntasks],
                GlobalTaskIds = [[0]],
                CompleteNodelist = <<>>,

                %% Extract partition from env if available
                Partition = extract_partition_from_env(Env),

                %% Extract io_key (credential signature) for I/O authentication
                IoKey = extract_io_key(Binary),

                %% Build the record
                Result = #launch_tasks_request{
                    job_id = JobId,
                    step_id = normalize_step_id(StepId),
                    step_het_comp = StepHetComp,
                    uid = Uid,
                    gid = Gid,
                    user_name = UserName,
                    gids = GIds,
                    ntasks = Ntasks,
                    nnodes = Nnodes,
                    argc = length(Argv),
                    argv = Argv,
                    envc = length(Env),
                    env = Env,
                    cwd = Cwd,
                    cpu_bind_type = CpuBindType,
                    cpu_bind = CpuBind,
                    task_dist = TaskDist,
                    flags = Flags,
                    tasks_to_launch = TasksToLaunch,
                    global_task_ids = GlobalTaskIds,
                    resp_port = RespPort,
                    io_port = IoPort,
                    ofname = Ofname,
                    efname = Efname,
                    ifname = Ifname,
                    complete_nodelist = CompleteNodelist,
                    partition = Partition,
                    job_mem_lim = JobMemLim,
                    step_mem_lim = StepMemLim,
                    io_key = IoKey
                },
                {ok, Result};

            not_found ->
                %% Minimal fallback
                {ok, #launch_tasks_request{
                    job_id = JobId,
                    step_id = normalize_step_id(StepId),
                    step_het_comp = StepHetComp,
                    uid = Uid,
                    gid = Gid,
                    user_name = UserName,
                    gids = GIds,
                    ntasks = Ntasks,
                    nnodes = Nnodes,
                    env = [],
                    cwd = <<"/tmp">>,
                    argv = [<<"/bin/hostname">>],
                    job_mem_lim = JobMemLim,
                    step_mem_lim = StepMemLim
                }}
        end
    catch
        error:{badmatch, _} = E ->
            {error, {launch_tasks_request_decode_failed, E}};
        Class:Reason:_Stack ->
            {error, {launch_tasks_request_decode_failed, {Class, Reason}}}
    end.

%%%===================================================================
%%% Internal: Encode Helpers
%%%===================================================================

%% Encode RESPONSE_JOB_STEP_CREATE (5002)
encode_job_step_create_response(#job_step_create_response{
    job_step_id = StepId,
    job_id = JobId,
    user_id = Uid,
    group_id = Gid,
    user_name = UserName0,
    node_list = NodeList0,
    num_tasks = NumTasks0,
    error_code = _ErrorCode,
    error_msg = _ErrorMsg
} = _Resp) ->
    NodeList = case NodeList0 of
        <<>> -> <<"flurm-node1">>;
        _ -> NodeList0
    end,
    UserName = case UserName0 of
        <<>> -> <<"root">>;
        _ -> UserName0
    end,
    NumTasks = max(1, NumTasks0),

    Parts = [
        <<0:32/big>>,  %% def_cpu_bind_type
        flurm_protocol_pack:pack_string(undefined),  %% resv_ports (NULL)
        <<StepId:32/big>>,  %% job_step_id
        encode_step_layout(NodeList, 1, NumTasks),
        encode_minimal_cred(JobId, StepId, Uid, Gid, NumTasks, UserName, NodeList),
        encode_select_jobinfo(),
        <<100:32/big>>,  %% switch_job (SWITCH_PLUGIN_NONE)
        <<16#2600:16/big>>  %% use_protocol_ver
    ],
    {ok, iolist_to_binary(Parts)};
encode_job_step_create_response(_) ->
    {ok, <<0:32, 16#FFFFFFFF:32, 0:32, 0:16, 16#FFFFFFFF:32, 0:32, 0:32, 100:32, 16#2600:16>>}.

%% Encode RESPONSE_JOB_STEP_INFO (5004)
encode_job_step_info_response(#job_step_info_response{
    last_update = LastUpdate,
    step_count = StepCount,
    steps = Steps
}) ->
    StepsBin = [encode_single_job_step_info(S) || S <- Steps],
    Parts = [
        flurm_protocol_pack:pack_time(LastUpdate),
        <<StepCount:32/big>>,
        StepsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_job_step_info_response(_) ->
    {ok, <<0:64, 0:32>>}.

%% Encode a single job_step_info record
encode_single_job_step_info(#job_step_info{} = S) ->
    [
        <<(S#job_step_info.job_id):32/big>>,
        <<(S#job_step_info.step_id):32/big>>,
        flurm_protocol_pack:pack_string(S#job_step_info.step_name),
        flurm_protocol_pack:pack_string(S#job_step_info.partition),
        <<(S#job_step_info.user_id):32/big>>,
        <<(S#job_step_info.state):32/big>>,
        <<(S#job_step_info.num_tasks):32/big>>,
        <<(S#job_step_info.num_cpus):32/big>>,
        <<(S#job_step_info.time_limit):32/big>>,
        flurm_protocol_pack:pack_time(S#job_step_info.start_time),
        <<(S#job_step_info.run_time):32/big>>,
        flurm_protocol_pack:pack_string(S#job_step_info.nodes),
        <<(S#job_step_info.node_cnt):32/big>>,
        flurm_protocol_pack:pack_string(S#job_step_info.tres_alloc_str),
        <<(S#job_step_info.exit_code):32/big>>
    ];
encode_single_job_step_info(_) ->
    [].

%% Encode RESPONSE_LAUNCH_TASKS (6002)
encode_launch_tasks_response(#launch_tasks_response{
    job_id = JobId,
    step_id = StepId,
    step_het_comp = StepHetComp,
    return_code = ReturnCode,
    node_name = NodeName,
    count_of_pids = CountOfPids,
    local_pids = LocalPids,
    gtids = Gtids
}) ->
    PidsBin = [<<P:32/big>> || P <- LocalPids],
    GtidsBin = [<<G:32/big>> || G <- Gtids],
    Parts = [
        <<JobId:32/big, StepId:32/big, StepHetComp:32/big>>,
        <<ReturnCode:32/signed-big>>,
        flurm_protocol_pack:pack_string(ensure_binary(NodeName)),
        <<CountOfPids:32/big>>,
        <<CountOfPids:32/big>>,
        PidsBin,
        <<CountOfPids:32/big>>,
        GtidsBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_launch_tasks_response(#{return_code := ReturnCode} = Map) ->
    JobId = maps:get(job_id, Map, 0),
    StepId = maps:get(step_id, Map, 0),
    StepHetComp = maps:get(step_het_comp, Map, 0),
    NodeName = maps:get(node_name, Map, <<>>),
    LocalPids = maps:get(local_pids, Map, []),
    Gtids = maps:get(gtids, Map, []),
    CountOfPids = length(LocalPids),
    encode_launch_tasks_response(#launch_tasks_response{
        job_id = JobId,
        step_id = StepId,
        step_het_comp = StepHetComp,
        return_code = ReturnCode,
        node_name = NodeName,
        count_of_pids = CountOfPids,
        local_pids = LocalPids,
        gtids = Gtids
    });
encode_launch_tasks_response(_) ->
    encode_launch_tasks_response(#launch_tasks_response{}).

%% Encode RESPONSE_REATTACH_TASKS (6008)
encode_reattach_tasks_response(#reattach_tasks_response{
    return_code = ReturnCode,
    node_name = NodeName,
    count_of_pids = CountOfPids,
    local_pids = LocalPids,
    gtids = Gtids,
    executable_names = ExecNames
}) ->
    PidsBin = [<<P:32/big>> || P <- LocalPids],
    GtidsBin = [<<G:32/big>> || G <- Gtids],
    ExecNamesBin = [flurm_protocol_pack:pack_string(ensure_binary(N)) || N <- ExecNames],
    Parts = [
        <<ReturnCode:32/signed-big>>,
        flurm_protocol_pack:pack_string(ensure_binary(NodeName)),
        <<CountOfPids:32/big>>,
        PidsBin,
        GtidsBin,
        ExecNamesBin
    ],
    {ok, iolist_to_binary(Parts)};
encode_reattach_tasks_response(#{return_code := ReturnCode} = Map) ->
    NodeName = maps:get(node_name, Map, <<>>),
    LocalPids = maps:get(local_pids, Map, []),
    Gtids = maps:get(gtids, Map, []),
    ExecNames = maps:get(executable_names, Map, []),
    CountOfPids = length(LocalPids),
    encode_reattach_tasks_response(#reattach_tasks_response{
        return_code = ReturnCode,
        node_name = NodeName,
        count_of_pids = CountOfPids,
        local_pids = LocalPids,
        gtids = Gtids,
        executable_names = ExecNames
    });
encode_reattach_tasks_response(_) ->
    encode_reattach_tasks_response(#reattach_tasks_response{}).

%% Encode MESSAGE_TASK_EXIT (6003)
encode_task_exit_msg(#{job_id := JobId, step_id := StepId} = Map) ->
    StepHetComp = maps:get(step_het_comp, Map, 16#FFFFFFFE),
    TaskIds = maps:get(task_ids, Map, [0]),
    ReturnCodes = maps:get(return_codes, Map, [0]),
    ReturnCode = case ReturnCodes of
        [RC | _] -> RC;
        _ -> 0
    end,
    NumTasks = length(TaskIds),
    TaskIdsBin = [<<T:32/big>> || T <- TaskIds],
    Parts = [
        <<ReturnCode:32/signed-big>>,
        <<NumTasks:32/big>>,
        <<NumTasks:32/big>>,
        TaskIdsBin,
        <<JobId:32/big, StepId:32/big, StepHetComp:32/big>>
    ],
    {ok, iolist_to_binary(Parts)};
encode_task_exit_msg(_) ->
    encode_task_exit_msg(#{job_id => 0, step_id => 0}).

%%%===================================================================
%%% Internal: Step Layout and Credential Encoding
%%%===================================================================

%% Encode a minimal step_layout for SLURM 22.05
encode_step_layout(NodeList, NodeCnt, TaskCnt) ->
    ExistsFlag = 1,
    TaskDist = 1,  %% SLURM_DIST_BLOCK
    TaskArrays = lists:duplicate(NodeCnt,
        <<1:32/big, 0:32/big>>  %% count=1, task_id=0
    ),
    iolist_to_binary([
        <<ExistsFlag:16/big>>,
        flurm_protocol_pack:pack_string(undefined),  %% front_end (NULL)
        flurm_protocol_pack:pack_string(NodeList),
        <<NodeCnt:32/big>>,
        <<16#2600:16/big>>,  %% start_protocol_ver (22.05)
        <<TaskCnt:32/big>>,
        <<TaskDist:32/big>>,
        TaskArrays
    ]).

%% Encode a minimal valid credential for SLURM 22.05
encode_minimal_cred(JobId, StepId, Uid, Gid, NumTasks, UserName, NodeList) ->
    CredBuffer = encode_cred_buffer(JobId, StepId, Uid, Gid, NumTasks, UserName, NodeList),
    Signature = <<>>,
    SigLen = 0,
    <<CredBuffer/binary, SigLen:32/big, Signature/binary>>.

%% Encode the credential buffer
encode_cred_buffer(JobId, StepId, Uid, Gid, NumTasks, UserName, NodeList) ->
    Now = erlang:system_time(second),
    iolist_to_binary([
        <<JobId:32/big, StepId:32/big, 0:32/big>>,  %% step_id
        <<Uid:32/big, Gid:32/big>>,
        flurm_protocol_pack:pack_string(UserName),
        flurm_protocol_pack:pack_string(undefined),  %% pw_gecos
        flurm_protocol_pack:pack_string(<<"/root">>),
        flurm_protocol_pack:pack_string(<<"/bin/bash">>),
        <<1:32/big, Gid:32/big>>,  %% gid_array
        <<1:32/big>>,
        flurm_protocol_pack:pack_string(<<"root">>),
        <<0:16/big>>,  %% gres_job_list
        <<0:16/big>>,  %% gres_step_list
        <<16#FFFF:16/big>>,  %% job_core_spec
        flurm_protocol_pack:pack_string(undefined),  %% account
        flurm_protocol_pack:pack_string(undefined),  %% alias_list
        flurm_protocol_pack:pack_string(undefined),  %% comment
        flurm_protocol_pack:pack_string(undefined),  %% constraints
        flurm_protocol_pack:pack_string(<<"default">>),  %% partition
        flurm_protocol_pack:pack_string(undefined),  %% reservation
        <<0:16/big>>,  %% job_restart_cnt
        flurm_protocol_pack:pack_string(undefined),  %% std_err
        flurm_protocol_pack:pack_string(undefined),  %% std_in
        flurm_protocol_pack:pack_string(undefined),  %% std_out
        flurm_protocol_pack:pack_string(NodeList),  %% step_hostlist
        <<0:16/big>>,  %% x11
        <<Now:64/big>>,  %% ctime
        <<1:32/big>>,  %% tot_core_cnt
        <<16#FFFFFFFE:32/big>>,  %% job_core_bitmap
        <<16#FFFFFFFE:32/big>>,  %% step_core_bitmap
        <<0:16/big>>,  %% core_array_size
        <<0:32/big>>,  %% cpu_array_count
        <<1:32/big>>,  %% nhosts
        <<NumTasks:32/big>>,  %% ntasks
        flurm_protocol_pack:pack_string(NodeList),  %% job_hostlist
        <<0:32/big>>,  %% job_mem_alloc_size
        <<0:32/big>>,  %% step_mem_alloc_size
        flurm_protocol_pack:pack_string(undefined)  %% selinux_context
    ]).

%% Encode select_jobinfo for cons_tres plugin
encode_select_jobinfo() ->
    <<109:32/big>>.

%%%===================================================================
%%% Internal: Utility Functions
%%%===================================================================

%% Normalize step ID
normalize_step_id(16#FFFFFFFE) -> 0;  %% NO_VAL
normalize_step_id(16#FFFFFFFF) -> 0;  %% NO_VAL
normalize_step_id(StepId) -> StepId.

%% Extract name from binary
extract_name_from_binary(Binary) ->
    case flurm_protocol_pack:unpack_string(Binary) of
        {ok, Name, _Rest} -> ensure_binary(Name);
        _ -> <<>>
    end.

%% Unpack string safely
unpack_string_safe(<<Len:32/big, Rest/binary>>) when Len =< byte_size(Rest), Len < 16#FFFFFFFE ->
    <<Str:Len/binary, Rest2/binary>> = Rest,
    {Str, Rest2};
unpack_string_safe(<<16#FFFFFFFF:32, Rest/binary>>) ->
    {<<>>, Rest};
unpack_string_safe(_) ->
    {<<>>, <<>>}.

%% Unpack uint32 array
unpack_uint32_array(0, Rest) ->
    {[], Rest};
unpack_uint32_array(Count, Binary) ->
    unpack_uint32_array(Count, Binary, []).

unpack_uint32_array(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
unpack_uint32_array(N, <<Val:32/big, Rest/binary>>, Acc) ->
    unpack_uint32_array(N - 1, Rest, [Val | Acc]);
unpack_uint32_array(_, Rest, Acc) ->
    {lists:reverse(Acc), Rest}.

%% Unpack string array
unpack_string_array(0, Rest) ->
    {[], Rest};
unpack_string_array(Count, Binary) ->
    unpack_string_array(Count, Binary, []).

unpack_string_array(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
unpack_string_array(N, Binary, Acc) ->
    {Str, Rest} = unpack_string_safe(Binary),
    unpack_string_array(N - 1, Rest, [Str | Acc]).

%% Skip het_job_tids arrays
skip_het_job_tids(0, Rest) -> Rest;
skip_het_job_tids(HetJobNnodes, Rest) when HetJobNnodes >= 16#FFFFFFFE -> Rest;
skip_het_job_tids(HetJobNnodes, Binary) ->
    skip_het_job_tids_loop(HetJobNnodes, Binary).

skip_het_job_tids_loop(0, Rest) -> Rest;
skip_het_job_tids_loop(N, <<Count:32/big, Rest/binary>>) when Count < 16#FFFFFFFE ->
    SkipBytes = Count * 4,
    <<_:SkipBytes/binary, Rest2/binary>> = Rest,
    skip_het_job_tids_loop(N - 1, Rest2);
skip_het_job_tids_loop(N, <<_Count:32/big, Rest/binary>>) ->
    skip_het_job_tids_loop(N - 1, Rest);
skip_het_job_tids_loop(_, Rest) -> Rest.

%% Skip het_job_tid_offsets
skip_het_job_tid_offsets(0, Rest) -> Rest;
skip_het_job_tid_offsets(HetJobNnodes, Rest) when HetJobNnodes >= 16#FFFFFFFE -> Rest;
skip_het_job_tid_offsets(HetJobNnodes, Binary) ->
    SkipBytes = HetJobNnodes * 4,
    <<_:SkipBytes/binary, Rest/binary>> = Binary,
    Rest.

%% Find env vars position in binary
find_env_vars_position(Binary) ->
    case binary:match(Binary, <<"SLURM_">>) of
        {StrPos, _} when StrPos >= 8 ->
            PossibleCountPos = StrPos - 8,
            case PossibleCountPos >= 0 of
                true ->
                    <<_:PossibleCountPos/binary, PossibleCount:32/big, PossibleLen:32/big, _/binary>> = Binary,
                    if
                        PossibleCount >= 10 andalso PossibleCount =< 100 andalso
                        PossibleLen >= 10 andalso PossibleLen < 200 ->
                            {ok, PossibleCountPos};
                        true ->
                            find_env_vars_position_scan(Binary, 0)
                    end;
                false ->
                    find_env_vars_position_scan(Binary, 0)
            end;
        _ ->
            find_env_vars_position_scan(Binary, 0)
    end.

find_env_vars_position_scan(Binary, Offset) when Offset + 50 < byte_size(Binary) ->
    <<_:Offset/binary, MaybeCount:32/big, Rest/binary>> = Binary,
    if
        MaybeCount >= 20 andalso MaybeCount =< 100 ->
            case Rest of
                <<StrLen:32/big, _/binary>> when StrLen >= 5, StrLen < 200 ->
                    case Rest of
                        <<_:32, FirstChars:5/binary, _/binary>> ->
                            case FirstChars of
                                <<"HOME=">> -> {ok, Offset};
                                <<"PATH=">> -> {ok, Offset};
                                <<"SLURM">> -> {ok, Offset};
                                <<"HOSTN">> -> {ok, Offset};
                                _ -> find_env_vars_position_scan(Binary, Offset + 1)
                            end;
                        _ -> find_env_vars_position_scan(Binary, Offset + 1)
                    end;
                _ -> find_env_vars_position_scan(Binary, Offset + 1)
            end;
        true -> find_env_vars_position_scan(Binary, Offset + 1)
    end;
find_env_vars_position_scan(_Binary, _Offset) -> not_found.

%% Extract partition from env
extract_partition_from_env(EnvList) when is_list(EnvList) ->
    case lists:search(fun(EnvVar) ->
        case EnvVar of
            <<"SLURM_JOB_PARTITION=", _/binary>> -> true;
            _ -> false
        end
    end, EnvList) of
        {value, <<"SLURM_JOB_PARTITION=", Partition/binary>>} -> Partition;
        _ -> <<>>
    end;
extract_partition_from_env(_) -> <<>>.

%% Extract resp_port from binary BEFORE env offset
extract_resp_port_before_env(Binary, EnvOffset) when EnvOffset > 50 ->
    SearchEnd = min(EnvOffset + 50, byte_size(Binary)),
    SearchBin = binary:part(Binary, 0, SearchEnd),
    AllArrays = find_all_port_arrays(SearchBin, 0, []),
    case AllArrays of
        [First | _] -> First;
        [] -> []
    end;
extract_resp_port_before_env(_, _) -> [].

%% Extract io_port from binary AFTER argv section
extract_io_port_after_argv(Binary) when byte_size(Binary) >= 4 ->
    find_io_port_16bit(Binary, 0);
extract_io_port_after_argv(_) -> [].

find_io_port_16bit(Binary, Offset) when Offset + 4 =< byte_size(Binary) ->
    <<_:Offset/binary, NumPorts:16/big, Port1:16/big, _/binary>> = Binary,
    if
        NumPorts >= 1 andalso NumPorts =< 4 andalso
        Port1 >= 30000 andalso Port1 < 60000 ->
            Ports = extract_port_array_16(Binary, Offset + 2, NumPorts),
            ValidPorts = [P || P <- Ports, P >= 30000, P < 60000],
            if
                length(ValidPorts) =:= NumPorts -> Ports;
                true -> find_io_port_16bit(Binary, Offset + 1)
            end;
        true -> find_io_port_16bit(Binary, Offset + 1)
    end;
find_io_port_16bit(_, _) -> [].

extract_port_array_16(Binary, Offset, Count) ->
    extract_port_array_16(Binary, Offset, Count, []).

extract_port_array_16(_, _, 0, Acc) -> lists:reverse(Acc);
extract_port_array_16(Binary, Offset, Count, Acc) when Offset + 2 =< byte_size(Binary) ->
    <<_:Offset/binary, Port:16/big, _/binary>> = Binary,
    extract_port_array_16(Binary, Offset + 2, Count - 1, [Port | Acc]);
extract_port_array_16(_, _, _, Acc) -> lists:reverse(Acc).

find_all_port_arrays(Binary, Offset, Acc) when Offset + 6 < byte_size(Binary) ->
    <<_:Offset/binary, NumPorts:32/big, Port1:16/big, _/binary>> = Binary,
    if
        NumPorts >= 1 andalso NumPorts =< 4 andalso
        Port1 >= 30000 andalso Port1 < 60000 ->
            Ports = extract_port_array(Binary, Offset + 4, NumPorts),
            ValidPorts = [P || P <- Ports, P >= 30000, P < 60000],
            if
                length(ValidPorts) =:= NumPorts ->
                    NextOffset = Offset + 4 + (NumPorts * 2),
                    find_all_port_arrays(Binary, NextOffset, [Ports | Acc]);
                true -> find_all_port_arrays(Binary, Offset + 1, Acc)
            end;
        true -> find_all_port_arrays(Binary, Offset + 1, Acc)
    end;
find_all_port_arrays(_, _, Acc) -> lists:reverse(Acc).

extract_port_array(Binary, Offset, Count) ->
    extract_port_array(Binary, Offset, Count, []).

extract_port_array(_, _, 0, Acc) -> lists:reverse(Acc);
extract_port_array(Binary, Offset, Count, Acc) when Offset + 2 =< byte_size(Binary) ->
    <<_:Offset/binary, Port:16/big, _/binary>> = Binary,
    extract_port_array(Binary, Offset + 2, Count - 1, [Port | Acc]);
extract_port_array(_, _, _, Acc) -> lists:reverse(Acc).

%% Extract io_key from binary
extract_io_key(Binary) when byte_size(Binary) < 100 -> <<>>;
extract_io_key(Binary) ->
    case find_signature_pattern(Binary) of
        {ok, Signature} ->
            case byte_size(Signature) >= 32 of
                true ->
                    <<IoKey:32/binary, _/binary>> = Signature,
                    IoKey;
                false ->
                    Padding = 32 - byte_size(Signature),
                    <<Signature/binary, 0:(Padding*8)>>
            end;
        not_found ->
            Hash = crypto:hash(sha256, Binary),
            <<IoKey:32/binary, _/binary>> = Hash,
            IoKey
    end.

find_signature_pattern(Binary) ->
    Size = byte_size(Binary),
    find_signature_pattern(Binary, Size - 4).

find_signature_pattern(_Binary, Offset) when Offset < 100 -> not_found;
find_signature_pattern(Binary, Offset) ->
    case Binary of
        <<_:Offset/binary, Len:32/big, Rest/binary>>
          when Len >= 80, Len =< 200, byte_size(Rest) >= Len ->
            <<Signature:Len/binary, _/binary>> = Rest,
            case is_likely_signature(Signature) of
                true -> {ok, Signature};
                false -> find_signature_pattern(Binary, Offset - 1)
            end;
        _ -> find_signature_pattern(Binary, Offset - 1)
    end.

is_likely_signature(<<>>) -> false;
is_likely_signature(Bin) ->
    case Bin of
        <<"MUNGE:", _/binary>> -> true;
        _ ->
            PrintableCount = count_printable_bytes(Bin),
            PrintableRatio = PrintableCount / byte_size(Bin),
            PrintableRatio > 0.7
    end.

count_printable_bytes(Bin) -> count_printable_bytes(Bin, 0).
count_printable_bytes(<<>>, Count) -> Count;
count_printable_bytes(<<B, Rest/binary>>, Count) when B >= 32, B =< 126 ->
    count_printable_bytes(Rest, Count + 1);
count_printable_bytes(<<_, Rest/binary>>, Count) ->
    count_printable_bytes(Rest, Count).

%% Ensure value is binary
ensure_binary(undefined) -> <<>>;
ensure_binary(null) -> <<>>;
ensure_binary(Bin) when is_binary(Bin) -> Bin;
ensure_binary(List) when is_list(List) -> list_to_binary(List);
ensure_binary(_) -> <<>>.
