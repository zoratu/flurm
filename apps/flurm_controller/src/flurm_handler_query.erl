%%%-------------------------------------------------------------------
%%% @doc FLURM Query Handler
%%%
%%% Handles query/info operations including:
%%% - Job info queries (including array job expansion)
%%% - Partition info queries
%%% - Build info (version) queries
%%% - Config info queries
%%% - Statistics/diagnostics queries
%%% - Federation info and update operations
%%%
%%% Split from flurm_controller_handler.erl for maintainability.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_query).

-export([handle/2]).

%% Exported for use by main handler
-export([
    job_to_job_info/1,
    job_state_to_slurm/1,
    partition_to_partition_info/2,
    partition_state_to_slurm/1,
    get_jobs_with_array_expansion/1,
    format_fed_clusters/1,
    format_fed_clusters_with_sibling_counts/2,
    get_sibling_job_counts_per_cluster/0,
    get_federation_stats/0,
    count_jobs_by_state/2,
    compute_node_inx/2
]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% Import helpers from main handler
-import(flurm_controller_handler, [
    ensure_binary/1,
    error_to_binary/1,
    default_time/1,
    format_allocated_nodes/1,
    format_node_list/1,
    format_licenses/1
]).

%%====================================================================
%% API
%%====================================================================

%% REQUEST_JOB_INFO (2003) -> RESPONSE_JOB_INFO
handle(#slurm_header{msg_type = ?REQUEST_JOB_INFO},
       #job_info_request{job_id = RawJobId}) ->
    %% NO_VAL (0xFFFFFFFE) means "all jobs" - normalize to 0
    JobId = case RawJobId of
        16#FFFFFFFE -> 0;
        _ -> RawJobId
    end,
    lager:info("Handling job info request for job_id=~p", [JobId]),
    Jobs = get_jobs_with_array_expansion(JobId),
    %% Convert internal jobs to job_info records
    JobInfos = [job_to_job_info(J) || J <- Jobs],
    Response = #job_info_response{
        last_update = erlang:system_time(second),
        job_count = length(JobInfos),
        jobs = JobInfos
    },
    {ok, ?RESPONSE_JOB_INFO, Response};

%% REQUEST_JOB_INFO_SINGLE (2005) -> RESPONSE_JOB_INFO
handle(#slurm_header{msg_type = ?REQUEST_JOB_INFO_SINGLE}, Body) ->
    %% Delegate to standard job info handler
    handle(#slurm_header{msg_type = ?REQUEST_JOB_INFO}, Body);

%% REQUEST_JOB_USER_INFO (2021) - Used by scontrol show job in newer SLURM
handle(#slurm_header{msg_type = ?REQUEST_JOB_USER_INFO}, Body) ->
    lager:debug("Handling job user info request (2021), body size=~p", [byte_size(Body)]),
    %% Body is raw binary, parse job_id from it
    %% Format: job_id:32/big, ...
    JobId = case Body of
        <<JId:32/big, _/binary>> when JId > 0, JId =/= 16#FFFFFFFE -> JId;
        _ -> 0  % Return all jobs
    end,
    Jobs = get_jobs_with_array_expansion(JobId),
    JobInfos = [job_to_job_info(J) || J <- Jobs],
    Response = #job_info_response{
        last_update = erlang:system_time(second),
        job_count = length(JobInfos),
        jobs = JobInfos
    },
    {ok, ?RESPONSE_JOB_INFO, Response};

%% REQUEST_PARTITION_INFO (2009) -> RESPONSE_PARTITION_INFO
handle(#slurm_header{msg_type = ?REQUEST_PARTITION_INFO}, _Body) ->
    lager:debug("Handling partition info request (sinfo)"),
    %% Get global node list (same order as REQUEST_NODE_INFO response)
    AllNodes = flurm_node_manager_server:list_nodes(),
    AllNodeNames = [N#node.hostname || N <- AllNodes],
    Partitions = flurm_partition_manager:list_partitions(),
    PartitionInfoList = [partition_to_partition_info(P, AllNodeNames) || P <- Partitions],
    Response = #partition_info_response{
        last_update = erlang:system_time(second),
        partition_count = length(PartitionInfoList),
        partitions = PartitionInfoList
    },
    {ok, ?RESPONSE_PARTITION_INFO, Response};

%% REQUEST_BUILD_INFO (2001) -> RESPONSE_BUILD_INFO
handle(#slurm_header{msg_type = ?REQUEST_BUILD_INFO}, _Body) ->
    lager:debug("Handling build info request"),
    %% Return version and build configuration info
    ClusterName = try flurm_config_server:get(cluster_name, <<"flurm">>) catch _:_ -> <<"flurm">> end,
    ControlMachine = try flurm_config_server:get(control_machine, <<"localhost">>) catch _:_ -> <<"localhost">> end,
    SlurmctldPort = try flurm_config_server:get(slurmctld_port, 6817) catch _:_ -> 6817 end,
    SlurmdPort = try flurm_config_server:get(slurmd_port, 6818) catch _:_ -> 6818 end,
    StateSaveLocation = try flurm_config_server:get(state_save_location, <<"/var/spool/flurm">>) catch _:_ -> <<"/var/spool/flurm">> end,

    Response = #build_info_response{
        version = <<"22.05.0">>,
        version_major = 22,
        version_minor = 5,
        version_micro = 0,
        release = <<"flurm-0.1.0">>,
        build_host = <<"erlang">>,
        build_user = <<"flurm">>,
        build_date = <<"2024">>,
        cluster_name = ensure_binary(ClusterName),
        control_machine = ensure_binary(ControlMachine),
        backup_controller = <<>>,
        accounting_storage_type = <<"accounting_storage/none">>,
        auth_type = <<"auth/munge">>,
        slurm_user_name = <<"slurm">>,
        slurmd_user_name = <<"root">>,
        slurmctld_host = ensure_binary(ControlMachine),
        slurmctld_port = SlurmctldPort,
        slurmd_port = SlurmdPort,
        spool_dir = <<"/var/spool/slurmd">>,
        state_save_location = ensure_binary(StateSaveLocation),
        plugin_dir = <<"/usr/lib64/slurm">>,
        priority_type = <<"priority/basic">>,
        select_type = <<"select/linear">>,
        scheduler_type = <<"sched/backfill">>,
        job_comp_type = <<"jobcomp/none">>
    },
    {ok, ?RESPONSE_BUILD_INFO, Response};

%% REQUEST_CONFIG_INFO (2016) -> RESPONSE_CONFIG_INFO
%% scontrol show config (detailed configuration dump)
handle(#slurm_header{msg_type = ?REQUEST_CONFIG_INFO}, _Body) ->
    lager:debug("Handling config info request"),
    Config = try flurm_config_server:get_all() catch _:_ -> #{} end,
    Response = #config_info_response{
        last_update = erlang:system_time(second),
        config = Config
    },
    {ok, ?RESPONSE_CONFIG_INFO, Response};

%% REQUEST_STATS_INFO (2026) -> RESPONSE_STATS_INFO
%% sdiag - scheduler diagnostics
handle(#slurm_header{msg_type = ?REQUEST_STATS_INFO}, _Body) ->
    lager:debug("Handling stats info request"),
    %% Collect statistics from various sources
    Jobs = try flurm_job_manager:list_jobs() catch _:_ -> [] end,
    GetState = fun
        (J) when is_record(J, job) -> J#job.state;
        (J) when is_map(J) -> maps:get(state, J, undefined);
        (_) -> undefined
    end,
    JobsPending = length([J || J <- Jobs, GetState(J) =:= pending]),
    JobsRunning = length([J || J <- Jobs, GetState(J) =:= running]),
    JobsCompleted = length([J || J <- Jobs, GetState(J) =:= completed]),
    JobsCanceled = length([J || J <- Jobs, GetState(J) =:= cancelled]),
    JobsFailed = length([J || J <- Jobs, GetState(J) =:= failed]),

    %% Get scheduler stats if available
    SchedulerStats = try
        case flurm_scheduler:get_stats() of
            {ok, Stats} when is_map(Stats) -> Stats;
            Stats when is_map(Stats) -> Stats;
            _ -> #{}
        end
    catch _:_ -> #{}
    end,

    Response = #stats_info_response{
        parts_packed = 1,
        req_time = erlang:system_time(second),
        req_time_start = maps:get(start_time, SchedulerStats, erlang:system_time(second)),
        server_thread_count = erlang:system_info(schedulers_online),
        agent_queue_size = 0,
        agent_count = 0,
        agent_thread_count = 0,
        dbd_agent_queue_size = 0,
        jobs_submitted = maps:get(jobs_submitted, SchedulerStats, length(Jobs)),
        jobs_started = maps:get(jobs_started, SchedulerStats, 0),
        jobs_completed = JobsCompleted,
        jobs_canceled = JobsCanceled,
        jobs_failed = JobsFailed,
        jobs_pending = JobsPending,
        jobs_running = JobsRunning,
        schedule_cycle_max = maps:get(schedule_cycle_max, SchedulerStats, 0),
        schedule_cycle_last = maps:get(schedule_cycle_last, SchedulerStats, 0),
        schedule_cycle_sum = maps:get(schedule_cycle_sum, SchedulerStats, 0),
        schedule_cycle_counter = maps:get(schedule_cycle_counter, SchedulerStats, 0),
        schedule_cycle_depth = maps:get(schedule_cycle_depth, SchedulerStats, 0),
        schedule_queue_len = JobsPending,
        bf_backfilled_jobs = 0,
        bf_last_backfilled_jobs = 0,
        bf_cycle_counter = 0,
        bf_cycle_sum = 0,
        bf_cycle_last = 0,
        bf_cycle_max = 0,
        bf_depth_sum = 0,
        bf_depth_try_sum = 0,
        bf_queue_len = 0,
        bf_queue_len_sum = 0,
        bf_when_last_cycle = 0,
        bf_active = false,
        rpc_type_stats = [],
        rpc_user_stats = []
    },
    {ok, ?RESPONSE_STATS_INFO, Response};

%% REQUEST_FED_INFO (2049) -> RESPONSE_FED_INFO (2050)
%% Federation info request - returns info about federated clusters (SLURM-compatible)
%% Phase 7E: Enhanced to include sibling job counts and detailed cluster info
handle(#slurm_header{msg_type = ?REQUEST_FED_INFO}, _Body) ->
    lager:info("Handling federation info request"),
    case catch flurm_federation:get_federation_info() of
        {ok, FedInfo} ->
            %% Build federation info response with sibling job counts
            Clusters = maps:get(clusters, FedInfo, []),
            LocalCluster = maps:get(local_cluster, FedInfo, <<"local">>),
            FedName = maps:get(name, FedInfo, <<"default">>),
            %% Get sibling job counts per cluster
            SiblingJobCounts = get_sibling_job_counts_per_cluster(),
            %% Enhance cluster info with sibling job counts
            EnhancedClusters = format_fed_clusters_with_sibling_counts(Clusters, SiblingJobCounts),
            %% Get federation-wide stats
            FedStats = get_federation_stats(),
            Response = #{
                federation_name => FedName,
                local_cluster => LocalCluster,
                clusters => EnhancedClusters,
                cluster_count => length(Clusters),
                %% Phase 7E: Additional federation status fields
                total_sibling_jobs => maps:get(total_sibling_jobs, FedStats, 0),
                total_pending_jobs => maps:get(total_pending_jobs, FedStats, 0),
                total_running_jobs => maps:get(total_running_jobs, FedStats, 0),
                federation_state => maps:get(state, FedStats, <<"active">>)
            },
            lager:info("Federation info: ~p clusters in ~s, ~p sibling jobs",
                       [length(Clusters), FedName, maps:get(total_sibling_jobs, FedStats, 0)]),
            {ok, ?RESPONSE_FED_INFO, Response};
        {error, not_federated} ->
            lager:info("Not in a federation"),
            Response = #{
                federation_name => <<>>,
                local_cluster => <<"local">>,
                clusters => [],
                cluster_count => 0,
                total_sibling_jobs => 0,
                total_pending_jobs => 0,
                total_running_jobs => 0,
                federation_state => <<"inactive">>
            },
            {ok, ?RESPONSE_FED_INFO, Response};
        {'EXIT', {noproc, _}} ->
            %% Federation module not running
            lager:info("Federation module not running"),
            Response = #{
                federation_name => <<>>,
                local_cluster => <<"local">>,
                clusters => [],
                cluster_count => 0,
                total_sibling_jobs => 0,
                total_pending_jobs => 0,
                total_running_jobs => 0,
                federation_state => <<"inactive">>
            },
            {ok, ?RESPONSE_FED_INFO, Response};
        Error ->
            lager:warning("Error getting federation info: ~p", [Error]),
            {error, Error}
    end;

%% REQUEST_UPDATE_FEDERATION (2064) -> RESPONSE_UPDATE_FEDERATION (2065)
%% Federation update request - add/remove clusters or update federation settings
handle(#slurm_header{msg_type = ?REQUEST_UPDATE_FEDERATION},
       #update_federation_request{action = Action, cluster_name = ClusterName,
                                  host = Host, port = Port, settings = Settings}) ->
    lager:info("Handling federation update request: action=~p, cluster=~s", [Action, ClusterName]),
    Result = case Action of
        add_cluster ->
            Config = #{host => Host, port => Port},
            case catch flurm_federation:add_cluster(ClusterName, Config) of
                ok ->
                    lager:info("Added cluster ~s to federation", [ClusterName]),
                    {ok, #update_federation_response{error_code = 0, error_msg = <<>>}};
                {error, Reason} ->
                    ErrMsg = error_to_binary(Reason),
                    lager:warning("Failed to add cluster ~s: ~p", [ClusterName, Reason]),
                    {ok, #update_federation_response{error_code = 1, error_msg = ErrMsg}};
                {'EXIT', {noproc, _}} ->
                    lager:warning("Federation module not running"),
                    {ok, #update_federation_response{error_code = 1, error_msg = <<"federation not running">>}}
            end;
        remove_cluster ->
            case catch flurm_federation:remove_cluster(ClusterName) of
                ok ->
                    lager:info("Removed cluster ~s from federation", [ClusterName]),
                    {ok, #update_federation_response{error_code = 0, error_msg = <<>>}};
                {error, Reason} ->
                    ErrMsg = error_to_binary(Reason),
                    lager:warning("Failed to remove cluster ~s: ~p", [ClusterName, Reason]),
                    {ok, #update_federation_response{error_code = 1, error_msg = ErrMsg}};
                {'EXIT', {noproc, _}} ->
                    lager:warning("Federation module not running"),
                    {ok, #update_federation_response{error_code = 1, error_msg = <<"federation not running">>}}
            end;
        update_settings ->
            case catch flurm_federation:update_settings(Settings) of
                ok ->
                    lager:info("Updated federation settings"),
                    {ok, #update_federation_response{error_code = 0, error_msg = <<>>}};
                {error, Reason} ->
                    ErrMsg = error_to_binary(Reason),
                    lager:warning("Failed to update federation settings: ~p", [Reason]),
                    {ok, #update_federation_response{error_code = 1, error_msg = ErrMsg}};
                {'EXIT', {noproc, _}} ->
                    lager:warning("Federation module not running"),
                    {ok, #update_federation_response{error_code = 1, error_msg = <<"federation not running">>}}
            end;
        _ ->
            lager:warning("Unknown federation update action: ~p", [Action]),
            {ok, #update_federation_response{error_code = 1, error_msg = <<"unknown action">>}}
    end,
    case Result of
        {ok, Response} -> {ok, ?RESPONSE_UPDATE_FEDERATION, Response};
        {error, E} -> {error, E}
    end;

%% Fallback for raw binary body in federation update
handle(#slurm_header{msg_type = ?REQUEST_UPDATE_FEDERATION}, _Body) ->
    lager:warning("Could not decode federation update request"),
    Response = #update_federation_response{error_code = 1, error_msg = <<"invalid request format">>},
    {ok, ?RESPONSE_UPDATE_FEDERATION, Response}.

%%====================================================================
%% Internal Functions - Job Info
%%====================================================================

%% @doc Get jobs, expanding array tasks when a specific array parent is requested
get_jobs_with_array_expansion(0) ->
    %% For "show all jobs", include active + recently completed jobs.
    %% Filter out jobs that have been in terminal state (cancelled/completed/failed)
    %% for more than MinJobAge (300s) to match SLURM behavior and avoid
    %% huge responses that can't be parsed by the client.
    Now = erlang:system_time(second),
    MinJobAge = 60,
    lists:filter(fun(J) ->
        case J#job.state of
            running -> true;
            pending -> true;
            configuring -> true;
            held -> true;
            _ ->
                %% Terminal state - check if recently ended
                EndTime = case J#job.end_time of
                    undefined -> Now;
                    0 -> Now;
                    T -> T
                end,
                (Now - EndTime) < MinJobAge
        end
    end, flurm_job_manager:list_jobs());
get_jobs_with_array_expansion(JobId) ->
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            %% Check if this job has array task children
            ArrayTasks = [J || J <- flurm_job_manager:list_jobs(),
                          J#job.array_job_id =:= JobId],
            case ArrayTasks of
                [] -> [Job];
                _ -> ArrayTasks  % Return task jobs instead of parent
            end;
        {error, not_found} -> []
    end.

%% @doc Convert internal job record to SLURM job_info record
%% Populates all 120+ fields required by SLURM protocol
-spec job_to_job_info(#job{}) -> #job_info{}.
job_to_job_info(#job{} = Job) ->
    NodeList = format_allocated_nodes(Job#job.allocated_nodes),
    BatchHost = case Job#job.allocated_nodes of
        [FirstNode | _] -> FirstNode;
        _ -> <<>>
    end,
    #job_info{
        %% Core fields for squeue display
        job_id = Job#job.id,
        name = ensure_binary(Job#job.name),
        user_id = 0,
        user_name = <<"root">>,
        group_id = 0,
        group_name = <<"root">>,
        job_state = job_state_to_slurm(Job#job.state),
        partition = ensure_binary(Job#job.partition),
        num_nodes = max(1, Job#job.num_nodes),
        num_cpus = max(1, Job#job.num_cpus),
        num_tasks = max(1, Job#job.num_tasks),
        time_limit = max(1, (Job#job.time_limit + 59) div 60),  % Internal seconds -> protocol minutes (round up like SLURM)
        priority = Job#job.priority,
        submit_time = Job#job.submit_time,
        start_time = default_time(Job#job.start_time),
        end_time = default_time(Job#job.end_time),
        eligible_time = Job#job.submit_time,
        nodes = NodeList,
        %% Batch job fields
        batch_flag = 1,
        batch_host = BatchHost,
        command = ensure_binary(Job#job.script),
        work_dir = <<"/tmp">>,
        %% Resource limits
        min_cpus = max(1, Job#job.num_cpus),
        max_cpus = max(1, Job#job.num_cpus),
        max_nodes = max(1, Job#job.num_nodes),
        cpus_per_task = max(1, Job#job.cpus_per_task),
        pn_min_cpus = 1,
        %% Default/empty values for remaining fields (MUST be present for protocol)
        account = Job#job.account,
        accrue_time = 0,
        admin_comment = <<>>,
        alloc_node = <<>>,
        alloc_sid = 0,
        array_job_id = case Job#job.array_job_id of
            0 -> 0;
            ArrId -> ArrId
        end,
        array_max_tasks = 0,
        array_task_id = case Job#job.array_task_id of
            undefined -> 16#FFFFFFFE;  % NO_VAL for non-array jobs
            ArrTaskId -> ArrTaskId
        end,
        array_task_str = undefined,   % NULL - not an array job
        assoc_id = 0,
        billable_tres = 0.0,
        bitflags = 0,
        boards_per_node = 0,
        burst_buffer = undefined,
        burst_buffer_state = undefined,
        cluster = undefined,
        cluster_features = undefined,
        comment = undefined,
        container = undefined,
        contiguous = 0,
        core_spec = 16#FFFF,  % SLURM_NO_VAL16
        cores_per_socket = 0,
        cpus_per_tres = undefined,
        deadline = 0,
        delay_boot = 0,
        dependency = undefined,
        derived_ec = 0,
        exc_nodes = undefined,
        exit_code = 0,
        features = undefined,
        fed_origin_str = undefined,
        fed_siblings_active = 0,
        fed_siblings_active_str = undefined,
        fed_siblings_viable = 0,
        fed_siblings_viable_str = undefined,
        gres_detail_cnt = 0,
        gres_detail_str = [],
        het_job_id = 0,
        het_job_id_set = undefined,
        het_job_offset = 16#FFFFFFFF,
        last_sched_eval = 0,
        licenses = format_licenses(Job#job.licenses),
        mail_type = 0,
        mail_user = undefined,
        mcs_label = undefined,
        mem_per_tres = undefined,
        min_mem_per_cpu = 0,
        min_mem_per_node = Job#job.memory_mb,
        network = undefined,
        nice = 0,
        ntasks_per_core = 16#FFFF,
        ntasks_per_node = 16#FFFF,
        ntasks_per_socket = 16#FFFF,
        ntasks_per_tres = 16#FFFF,
        pn_min_memory = Job#job.memory_mb,
        pn_min_tmp_disk = 0,
        power_flags = 0,
        preempt_time = 0,
        preemptable_time = 0,
        pre_sus_time = 0,
        profile = 0,
        qos = Job#job.qos,
        reboot = 0,
        req_nodes = undefined,
        req_switch = 0,
        requeue = 1,
        resize_time = 0,
        restart_cnt = 0,
        resv_name = undefined,
        sched_nodes = undefined,
        shared = 0,
        show_flags = 0,
        site_factor = 0,
        sockets_per_board = 0,
        sockets_per_node = 0,
        state_desc = undefined,
        state_reason = 0,
        std_err = case Job#job.std_err of
            <<>> -> undefined;
            undefined -> undefined;
            ErrPath -> ErrPath
        end,
        std_in = undefined,
        std_out = case Job#job.std_out of
            <<>> -> undefined;
            undefined -> undefined;
            OutPath -> OutPath
        end,
        suspend_time = 0,
        system_comment = undefined,
        threads_per_core = 0,
        time_min = 0,
        tres_alloc_str = undefined,
        tres_bind = undefined,
        tres_freq = undefined,
        tres_per_job = undefined,
        tres_per_node = undefined,
        tres_per_socket = undefined,
        tres_per_task = undefined,
        tres_req_str = undefined,
        wait4switch = 0,
        wckey = undefined
    }.

%% @doc Convert internal job state to SLURM job state integer
-spec job_state_to_slurm(job_state()) -> non_neg_integer().
job_state_to_slurm(pending) -> ?JOB_PENDING;
job_state_to_slurm(configuring) -> ?JOB_PENDING;
job_state_to_slurm(running) -> ?JOB_RUNNING;
job_state_to_slurm(completing) -> ?JOB_RUNNING;
job_state_to_slurm(completed) -> ?JOB_COMPLETE;
job_state_to_slurm(cancelled) -> ?JOB_CANCELLED;
job_state_to_slurm(failed) -> ?JOB_FAILED;
job_state_to_slurm(timeout) -> ?JOB_TIMEOUT;
job_state_to_slurm(node_fail) -> ?JOB_NODE_FAIL;
job_state_to_slurm(_) -> ?JOB_PENDING.

%%====================================================================
%% Internal Functions - Partition Info
%%====================================================================

%% @doc Convert internal partition record to SLURM partition_info record
-spec partition_to_partition_info(#partition{}, [binary()]) -> #partition_info{}.
partition_to_partition_info(#partition{} = Part, AllNodeNames) ->
    %% Get nodes that belong to this partition
    %% We filter to only include nodes that:
    %% 1. Are actually registered (connected and up)
    %% 2. Claim membership in this partition (via their partitions list)
    PartitionName = Part#partition.name,
    AllRegisteredNodes = flurm_node_manager_server:list_nodes(),

    %% Filter to nodes that are both registered AND claim this partition
    PartitionNodes = [N#node.hostname || N <- AllRegisteredNodes,
                      %% Node must claim membership in this partition OR it's default
                      (lists:member(PartitionName, N#node.partitions) orelse
                       PartitionName =:= <<"default">>),
                      %% If partition has explicit node list, node must be in it
                      (Part#partition.nodes =:= [] orelse
                       lists:member(N#node.hostname, Part#partition.nodes))],

    %% Calculate total CPUs from the nodes in this partition
    NodesInPartition = [N || N <- AllRegisteredNodes,
                             lists:member(N#node.hostname, PartitionNodes)],
    TotalCpus = lists:sum([N#node.cpus || N <- NodesInPartition]),

    %% Compute node_inx: indices into the global node list for this partition's nodes
    NodeInx = compute_node_inx(PartitionNodes, AllNodeNames),

    #partition_info{
        name = Part#partition.name,
        state_up = partition_state_to_slurm(Part#partition.state),
        max_time = Part#partition.max_time,
        default_time = Part#partition.default_time,
        max_nodes = Part#partition.max_nodes,
        min_nodes = 1,
        total_nodes = length(PartitionNodes),
        total_cpus = TotalCpus,
        nodes = format_node_list(PartitionNodes),
        priority_tier = Part#partition.priority,
        priority_job_factor = 1,
        node_inx = NodeInx
    }.

%% @doc Compute node_inx for a partition.
%% Returns a list of 0-based indices into the global node list representing
%% which nodes belong to this partition. Used to encode the SLURM node bitmap.
-spec compute_node_inx([binary()], [binary()]) -> [non_neg_integer()].
compute_node_inx([], _AllNodeNames) ->
    [];
compute_node_inx(PartitionNodeNames, AllNodeNames) ->
    lists:sort([Idx || {Name, Idx} <- lists:zip(AllNodeNames,
                                  lists:seq(0, length(AllNodeNames) - 1)),
                       lists:member(Name, PartitionNodeNames)]).

%% @doc Convert internal partition state to SLURM state
%% SLURM partition state flags from slurm.h:
%% PARTITION_SUBMIT = 0x01, PARTITION_SCHED = 0x02
%% PARTITION_UP = SUBMIT | SCHED = 3
%% PARTITION_DOWN = SUBMIT = 1
%% PARTITION_DRAIN = SCHED = 2
%% PARTITION_INACTIVE = 0
-spec partition_state_to_slurm(partition_state()) -> non_neg_integer().
partition_state_to_slurm(up) -> 3;       % PARTITION_UP
partition_state_to_slurm(down) -> 1;     % PARTITION_DOWN
partition_state_to_slurm(drain) -> 2;    % PARTITION_DRAIN
partition_state_to_slurm(inactive) -> 0; % PARTITION_INACTIVE
partition_state_to_slurm(_) -> 3.        % Default to UP

%%====================================================================
%% Internal Functions - Federation
%%====================================================================

%% @doc Format federation clusters list to maps for response
-spec format_fed_clusters([tuple()] | [map()]) -> [map()].
format_fed_clusters([]) -> [];
format_fed_clusters(Clusters) ->
    lists:map(fun(Cluster) ->
        case Cluster of
            #{name := Name} ->
                %% Already a map
                #{
                    name => ensure_binary(Name),
                    host => ensure_binary(maps:get(host, Cluster, <<>>)),
                    port => maps:get(port, Cluster, 6817),
                    state => maps:get(state, Cluster, <<"up">>)
                };
            {fed_cluster, Name, Host, Port, _Weight, _Auth, State, _Features, _Partitions, _LastCheck, _Failures} ->
                %% Record format
                #{
                    name => ensure_binary(Name),
                    host => ensure_binary(Host),
                    port => Port,
                    state => ensure_binary(atom_to_list(State))
                };
            _ ->
                %% Unknown format, return as-is wrapped in map
                #{cluster => Cluster}
        end
    end, Clusters).

%% @doc Format federation clusters with sibling job counts (Phase 7E)
%% Enhances cluster info with sibling job count for each cluster
-spec format_fed_clusters_with_sibling_counts([tuple()] | [map()], map()) -> [map()].
format_fed_clusters_with_sibling_counts([], _Counts) -> [];
format_fed_clusters_with_sibling_counts(Clusters, SiblingCounts) ->
    lists:map(fun(Cluster) ->
        BaseInfo = case Cluster of
            #{name := Name} ->
                #{
                    name => ensure_binary(Name),
                    host => ensure_binary(maps:get(host, Cluster, <<>>)),
                    port => maps:get(port, Cluster, 6817),
                    state => maps:get(state, Cluster, <<"up">>),
                    weight => maps:get(weight, Cluster, 1),
                    features => maps:get(features, Cluster, []),
                    partitions => maps:get(partitions, Cluster, []),
                    pending_jobs => maps:get(pending_jobs, Cluster, 0),
                    running_jobs => maps:get(running_jobs, Cluster, 0)
                };
            {fed_cluster, Name, Host, Port, Weight, _Auth, State, Features, Partitions, _LastCheck, _Failures} ->
                #{
                    name => ensure_binary(Name),
                    host => ensure_binary(Host),
                    port => Port,
                    state => ensure_binary(atom_to_list(State)),
                    weight => Weight,
                    features => Features,
                    partitions => Partitions,
                    pending_jobs => 0,
                    running_jobs => 0
                };
            _ ->
                #{cluster => Cluster}
        end,
        %% Add sibling job count
        ClusterName = maps:get(name, BaseInfo, <<>>),
        SiblingCount = maps:get(ClusterName, SiblingCounts, 0),
        BaseInfo#{sibling_job_count => SiblingCount}
    end, Clusters).

%% @doc Get sibling job counts grouped by cluster (Phase 7E)
%% Returns a map of cluster_name => sibling_job_count
-spec get_sibling_job_counts_per_cluster() -> map().
get_sibling_job_counts_per_cluster() ->
    try
        case catch flurm_federation:get_federation_jobs() of
            Jobs when is_list(Jobs) ->
                %% Count jobs per origin cluster
                lists:foldl(fun(Job, Acc) ->
                    Cluster = case Job of
                        #{origin_cluster := C} -> C;
                        #{cluster := C} -> C;
                        _ -> <<"unknown">>
                    end,
                    Count = maps:get(Cluster, Acc, 0),
                    maps:put(Cluster, Count + 1, Acc)
                end, #{}, Jobs);
            _ ->
                #{}
        end
    catch
        _:_ -> #{}
    end.

%% @doc Get federation-wide statistics (Phase 7E)
%% Returns aggregate stats across all federated clusters
-spec get_federation_stats() -> map().
get_federation_stats() ->
    try
        case catch flurm_federation:get_federation_stats() of
            Stats when is_map(Stats) ->
                Stats;
            _ ->
                %% Calculate basic stats from available data
                Jobs = try flurm_federation:get_federation_jobs() catch _:_ -> [] end,
                Clusters = try flurm_federation:list_clusters() catch _:_ -> [] end,
                #{
                    total_sibling_jobs => length(Jobs),
                    total_pending_jobs => count_jobs_by_state(Jobs, pending),
                    total_running_jobs => count_jobs_by_state(Jobs, running),
                    state => case length(Clusters) > 0 of true -> <<"active">>; false -> <<"inactive">> end
                }
        end
    catch
        _:_ ->
            #{
                total_sibling_jobs => 0,
                total_pending_jobs => 0,
                total_running_jobs => 0,
                state => <<"inactive">>
            }
    end.

%% @doc Count jobs by state
-spec count_jobs_by_state([map()], atom()) -> non_neg_integer().
count_jobs_by_state(Jobs, State) ->
    length([J || J <- Jobs, maps:get(state, J, undefined) =:= State]).
