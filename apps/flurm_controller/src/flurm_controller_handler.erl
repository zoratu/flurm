%%%-------------------------------------------------------------------
%%% @doc FLURM Controller RPC Handler
%%%
%%% Dispatches incoming SLURM protocol messages to appropriate domain
%%% logic handlers and returns appropriate response records.
%%%
%%% In a clustered setup, write operations (job submission, cancellation)
%%% are forwarded to the leader if this node is not the leader. Read
%%% operations (job info, node info, partition info) can be served
%%% locally from any node.
%%%
%%% Supported request types:
%%% - REQUEST_PING (1008) -> responds with PONG (RESPONSE_SLURM_RC)
%%% - REQUEST_SUBMIT_BATCH_JOB (4003) -> creates job via flurm_job_manager
%%% - REQUEST_JOB_INFO (2003) -> queries job registry
%%% - REQUEST_CANCEL_JOB (4006) -> cancels job
%%% - REQUEST_NODE_INFO (2007) -> queries node registry
%%% - REQUEST_PARTITION_INFO (2009) -> queries partition config
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_handler).

-export([handle/2]).


-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% SLURM error codes
-define(ESLURM_CONTROLLER_NOT_FOUND, 1).
-define(ESLURM_NOT_LEADER, 2).

%%====================================================================
%% API
%%====================================================================

%% @doc Main dispatch function for handling SLURM protocol messages.
%% Takes a header and body record, returns appropriate response.
-spec handle(#slurm_header{}, term()) ->
    {ok, non_neg_integer(), term()} | {error, term()}.

%% REQUEST_PING (1008) -> RESPONSE_SLURM_RC with return_code = 0
handle(#slurm_header{msg_type = ?REQUEST_PING}, _Body) ->
    lager:debug("Handling PING request"),
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response};

%% REQUEST_SUBMIT_BATCH_JOB (4003) -> RESPONSE_SUBMIT_BATCH_JOB
%% Write operation - requires leader or forwarding to leader
handle(#slurm_header{msg_type = ?REQUEST_SUBMIT_BATCH_JOB},
       #batch_job_request{} = Request) ->
    lager:info("Handling batch job submission: ~s", [Request#batch_job_request.name]),
    JobSpec = batch_request_to_job_spec(Request),
    Result = case is_cluster_enabled() of
        true ->
            %% Cluster mode - check leadership
            case flurm_controller_cluster:is_leader() of
                true ->
                    %% We are leader, process locally
                    flurm_job_manager:submit_job(JobSpec);
                false ->
                    %% Forward to leader
                    case flurm_controller_cluster:forward_to_leader(submit_job, JobSpec) of
                        {ok, JobResult} -> JobResult;
                        {error, no_leader} -> {error, controller_not_found};
                        {error, cluster_not_ready} -> {error, cluster_not_ready};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            %% Single node mode
            flurm_job_manager:submit_job(JobSpec)
    end,
    case Result of
        {ok, JobId} ->
            lager:info("Job ~p submitted successfully", [JobId]),
            Response = #batch_job_response{
                job_id = JobId,
                step_id = 0,
                error_code = 0,
                job_submit_user_msg = <<"Job submitted successfully">>
            },
            {ok, ?RESPONSE_SUBMIT_BATCH_JOB, Response};
        {error, Reason2} ->
            lager:warning("Job submission failed: ~p", [Reason2]),
            Response = #batch_job_response{
                job_id = 0,
                step_id = 0,
                error_code = ?ESLURM_CONTROLLER_NOT_FOUND,
                job_submit_user_msg = error_to_binary(Reason2)
            },
            {ok, ?RESPONSE_SUBMIT_BATCH_JOB, Response}
    end;

%% REQUEST_RESOURCE_ALLOCATION (4001) -> RESPONSE_RESOURCE_ALLOCATION
%% Used by srun for interactive jobs
handle(#slurm_header{msg_type = ?REQUEST_RESOURCE_ALLOCATION},
       #resource_allocation_request{} = Request) ->
    lager:info("Handling srun resource allocation: ~s", [Request#resource_allocation_request.name]),
    JobSpec = resource_request_to_job_spec(Request),
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> flurm_job_manager:submit_job(JobSpec);
                false ->
                    case flurm_controller_cluster:forward_to_leader(submit_job, JobSpec) of
                        {ok, JobResult} -> JobResult;
                        {error, no_leader} -> {error, controller_not_found};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_job_manager:submit_job(JobSpec)
    end,
    case Result of
        {ok, JobId} ->
            lager:info("srun job ~p allocated", [JobId]),
            %% Get allocated nodes for the response
            {ok, Job} = flurm_job_manager:get_job(JobId),
            NodeList = format_allocated_nodes(Job#job.allocated_nodes),
            Response = #resource_allocation_response{
                job_id = JobId,
                node_list = NodeList,
                num_nodes = Job#job.num_nodes,
                partition = Job#job.partition,
                error_code = 0,
                job_submit_user_msg = <<"Job allocated successfully">>
            },
            {ok, ?RESPONSE_RESOURCE_ALLOCATION, Response};
        {error, Reason2} ->
            lager:warning("srun allocation failed: ~p", [Reason2]),
            Response = #resource_allocation_response{
                job_id = 0,
                error_code = 1,
                job_submit_user_msg = error_to_binary(Reason2)
            },
            {ok, ?RESPONSE_RESOURCE_ALLOCATION, Response}
    end;

%% Fallback for raw binary body (when decoder fails)
handle(#slurm_header{msg_type = ?REQUEST_RESOURCE_ALLOCATION}, _Body) ->
    lager:warning("srun: could not decode resource allocation request"),
    Response = #resource_allocation_response{
        job_id = 0,
        error_code = 1,
        job_submit_user_msg = <<"Failed to decode request">>
    },
    {ok, ?RESPONSE_RESOURCE_ALLOCATION, Response};

%% REQUEST_JOB_INFO (2003) -> RESPONSE_JOB_INFO
handle(#slurm_header{msg_type = ?REQUEST_JOB_INFO},
       #job_info_request{job_id = JobId}) ->
    lager:info("Handling job info request for job_id=~p", [JobId]),
    Jobs = case JobId of
        0 ->
            %% Return all jobs
            flurm_job_manager:list_jobs();
        _ ->
            %% Return specific job
            case flurm_job_manager:get_job(JobId) of
                {ok, Job} -> [Job];
                {error, not_found} -> []
            end
    end,
    lager:info("Found ~p jobs to return", [length(Jobs)]),
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
    lager:info("Handling job user info request (2021), body size=~p", [byte_size(Body)]),
    %% Body is raw binary, parse job_id from it
    %% Format: job_id:32/big, ...
    JobId = case Body of
        <<JId:32/big, _/binary>> when JId > 0 -> JId;
        _ -> 0  % Return all jobs
    end,
    lager:info("Parsed job_id=~p from request", [JobId]),
    %% Call job info handler directly
    Jobs = case JobId of
        0 ->
            flurm_job_manager:list_jobs();
        _ ->
            case flurm_job_manager:get_job(JobId) of
                {ok, Job} -> [Job];
                {error, not_found} -> []
            end
    end,
    lager:info("Found ~p jobs for 2021 request", [length(Jobs)]),
    JobInfos = [job_to_job_info(J) || J <- Jobs],
    Response = #job_info_response{
        last_update = erlang:system_time(second),
        job_count = length(JobInfos),
        jobs = JobInfos
    },
    {ok, ?RESPONSE_JOB_INFO, Response};

%% REQUEST_SCONTROL_INFO (2049) - Used by scontrol in newer SLURM
handle(#slurm_header{msg_type = ?REQUEST_SCONTROL_INFO}, _Body) ->
    lager:info("Handling scontrol info request (2049)"),
    %% Return success - this is often a status check
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response};

%% REQUEST_KILL_JOB (5032) -> RESPONSE_SLURM_RC
%% Used by scancel in SLURM 19.05+
%% Write operation - requires leader or forwarding to leader
handle(#slurm_header{msg_type = ?REQUEST_KILL_JOB},
       #kill_job_request{job_id = JobId, job_id_str = JobIdStr}) ->
    %% Use job_id if non-zero, otherwise parse from job_id_str
    EffectiveJobId = case JobId of
        0 when byte_size(JobIdStr) > 0 ->
            try binary_to_integer(JobIdStr) catch _:_ -> 0 end;
        _ ->
            JobId
    end,
    lager:info("Handling kill job request for job_id=~p (str=~p)", [EffectiveJobId, JobIdStr]),
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> flurm_job_manager:cancel_job(EffectiveJobId);
                false ->
                    case flurm_controller_cluster:forward_to_leader(cancel_job, EffectiveJobId) of
                        {ok, CancelResult} -> CancelResult;
                        {error, no_leader} -> {error, controller_not_found};
                        {error, cluster_not_ready} -> {error, cluster_not_ready};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_job_manager:cancel_job(EffectiveJobId)
    end,
    case Result of
        ok ->
            lager:info("Job ~p killed successfully", [EffectiveJobId]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, not_found} ->
            lager:warning("Kill failed: job ~p not found", [EffectiveJobId]),
            Response = #slurm_rc_response{return_code = 0},  % SLURM returns 0 even for not found
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Kill failed for job ~p: ~p", [EffectiveJobId, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_CANCEL_JOB (4006) -> RESPONSE_SLURM_RC
%% Write operation - requires leader or forwarding to leader
handle(#slurm_header{msg_type = ?REQUEST_CANCEL_JOB},
       #cancel_job_request{job_id = JobId}) ->
    lager:info("Handling cancel job request for job_id=~p", [JobId]),
    Result = case is_cluster_enabled() of
        true ->
            %% Cluster mode - check leadership
            case flurm_controller_cluster:is_leader() of
                true ->
                    %% We are leader, process locally
                    flurm_job_manager:cancel_job(JobId);
                false ->
                    %% Forward to leader
                    case flurm_controller_cluster:forward_to_leader(cancel_job, JobId) of
                        {ok, CancelResult} -> CancelResult;
                        {error, no_leader} -> {error, controller_not_found};
                        {error, cluster_not_ready} -> {error, cluster_not_ready};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            %% Single node mode
            flurm_job_manager:cancel_job(JobId)
    end,
    case Result of
        ok ->
            lager:info("Job ~p cancelled successfully", [JobId]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, not_found} ->
            lager:warning("Cancel failed: job ~p not found", [JobId]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Cancel failed for job ~p: ~p", [JobId, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_NODE_INFO (2007) -> RESPONSE_NODE_INFO
handle(#slurm_header{msg_type = ?REQUEST_NODE_INFO}, _Body) ->
    lager:debug("Handling node info request"),
    Nodes = flurm_node_manager:list_nodes(),
    lager:info("DEBUG: Found ~p nodes", [length(Nodes)]),
    NodeInfoList = [node_to_node_info(N) || N <- Nodes],
    Response = #node_info_response{
        last_update = erlang:system_time(second),
        node_count = length(NodeInfoList),
        nodes = NodeInfoList
    },
    {ok, ?RESPONSE_NODE_INFO, Response};

%% REQUEST_PARTITION_INFO (2009) -> RESPONSE_PARTITION_INFO
handle(#slurm_header{msg_type = ?REQUEST_PARTITION_INFO}, _Body) ->
    lager:debug("Handling partition info request"),
    Partitions = flurm_partition_manager:list_partitions(),
    PartitionInfoList = [partition_to_partition_info(P) || P <- Partitions],
    Response = #partition_info_response{
        last_update = erlang:system_time(second),
        partition_count = length(PartitionInfoList),
        partitions = PartitionInfoList
    },
    {ok, ?RESPONSE_PARTITION_INFO, Response};

%% REQUEST_NODE_REGISTRATION_STATUS (1001) -> MESSAGE_NODE_REGISTRATION_STATUS
handle(#slurm_header{msg_type = ?REQUEST_NODE_REGISTRATION_STATUS},
       #node_registration_request{}) ->
    lager:debug("Handling node registration status request"),
    %% For now, return a simple acknowledgment
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response};

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

%% REQUEST_RECONFIGURE (1003) -> RESPONSE_SLURM_RC
%% Hot-reload configuration from slurm.conf
handle(#slurm_header{msg_type = ?REQUEST_RECONFIGURE}, _Body) ->
    lager:info("Handling reconfigure request"),
    Result = case is_cluster_enabled() of
        true ->
            %% In cluster mode, only leader processes reconfigure
            case flurm_controller_cluster:is_leader() of
                true ->
                    do_reconfigure();
                false ->
                    %% Forward to leader
                    case flurm_controller_cluster:forward_to_leader(reconfigure, []) of
                        {ok, ReconfigResult} -> ReconfigResult;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            do_reconfigure()
    end,
    case Result of
        ok ->
            lager:info("Reconfiguration completed successfully"),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Reconfiguration failed: ~p", [Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_JOB_STEP_CREATE (5001) -> RESPONSE_JOB_STEP_CREATE
%% Create a new step within an existing job
handle(#slurm_header{msg_type = ?REQUEST_JOB_STEP_CREATE},
       #job_step_create_request{} = Request) ->
    JobId = Request#job_step_create_request.job_id,
    lager:info("Handling job step create for job ~p: ~s",
               [JobId, Request#job_step_create_request.name]),
    StepSpec = #{
        name => Request#job_step_create_request.name,
        num_tasks => max(1, Request#job_step_create_request.num_tasks),
        num_nodes => max(1, Request#job_step_create_request.min_nodes),
        command => <<>>
    },
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> flurm_step_manager:create_step(JobId, StepSpec);
                false ->
                    case flurm_controller_cluster:forward_to_leader(create_step, {JobId, StepSpec}) of
                        {ok, StepResult} -> StepResult;
                        {error, no_leader} -> {error, controller_not_found};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_step_manager:create_step(JobId, StepSpec)
    end,
    case Result of
        {ok, StepId} ->
            lager:info("Step ~p.~p created successfully", [JobId, StepId]),
            Response = #job_step_create_response{
                job_step_id = StepId,
                error_code = 0,
                error_msg = <<"Step created successfully">>
            },
            {ok, ?RESPONSE_JOB_STEP_CREATE, Response};
        {error, Reason2} ->
            lager:warning("Step creation failed: ~p", [Reason2]),
            Response = #job_step_create_response{
                job_step_id = 0,
                error_code = 1,
                error_msg = error_to_binary(Reason2)
            },
            {ok, ?RESPONSE_JOB_STEP_CREATE, Response}
    end;

%% REQUEST_JOB_STEP_INFO (5003) -> RESPONSE_JOB_STEP_INFO
%% Query step information
handle(#slurm_header{msg_type = ?REQUEST_JOB_STEP_INFO},
       #job_step_info_request{} = Request) ->
    JobId = Request#job_step_info_request.job_id,
    StepId = Request#job_step_info_request.step_id,
    lager:debug("Handling job step info request for job ~p step ~p", [JobId, StepId]),
    %% Get steps from step manager
    Steps = case StepId of
        -1 ->
            %% All steps for the job (or all jobs if JobId is NO_VAL)
            flurm_step_manager:list_steps(JobId);
        _ ->
            %% Specific step
            case flurm_step_manager:get_step(JobId, StepId) of
                {ok, Step} -> [Step];
                {error, not_found} -> []
            end
    end,
    %% Convert to protocol records
    StepInfos = [step_map_to_info(S) || S <- Steps],
    Response = #job_step_info_response{
        last_update = erlang:system_time(second),
        step_count = length(StepInfos),
        steps = StepInfos
    },
    {ok, ?RESPONSE_JOB_STEP_INFO, Response};

%% REQUEST_UPDATE_JOB (4014) -> RESPONSE_SLURM_RC
%% Used by scontrol update job, hold, release
handle(#slurm_header{msg_type = ?REQUEST_UPDATE_JOB},
       #update_job_request{} = Request) ->
    JobId = case Request#update_job_request.job_id of
        0 -> parse_job_id_str(Request#update_job_request.job_id_str);
        Id -> Id
    end,
    lager:info("Handling update job request for job_id=~p", [JobId]),

    %% Determine what type of update this is
    Priority = Request#update_job_request.priority,
    TimeLimit = Request#update_job_request.time_limit,
    Requeue = Request#update_job_request.requeue,

    Updates = build_job_updates(Priority, TimeLimit, Requeue),

    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> flurm_job_manager:update_job(JobId, Updates);
                false ->
                    case flurm_controller_cluster:forward_to_leader(update_job, {JobId, Updates}) of
                        {ok, UpdateResult} -> UpdateResult;
                        {error, no_leader} -> {error, controller_not_found};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_job_manager:update_job(JobId, Updates)
    end,
    case Result of
        ok ->
            lager:info("Job ~p updated successfully", [JobId]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, not_found} ->
            lager:warning("Update failed: job ~p not found", [JobId]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Update failed for job ~p: ~p", [JobId, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_UPDATE_JOB fallback for raw binary
handle(#slurm_header{msg_type = ?REQUEST_UPDATE_JOB}, Body) when is_binary(Body) ->
    lager:warning("Could not decode update job request"),
    Response = #slurm_rc_response{return_code = -1},
    {ok, ?RESPONSE_SLURM_RC, Response};

%% REQUEST_JOB_WILL_RUN (4012) -> RESPONSE_JOB_WILL_RUN
%% Used by sbatch --test-only to check if job could run
handle(#slurm_header{msg_type = ?REQUEST_JOB_WILL_RUN},
       #job_will_run_request{} = Request) ->
    lager:info("Handling job will run request"),

    %% Check if resources are available for this job
    Partition = case Request#job_will_run_request.partition of
        <<>> -> <<"default">>;
        P -> P
    end,
    MinNodes = Request#job_will_run_request.min_nodes,
    MinCpus = Request#job_will_run_request.min_cpus,

    %% Check available resources using existing API
    AvailNodes = try
        flurm_node_manager:get_available_nodes_for_job(Partition, MinNodes, MinCpus)
    catch
        _:_ -> []
    end,

    Response = case length(AvailNodes) >= MinNodes of
        true ->
            %% Job could run now
            NodeList = iolist_to_binary(lists:join(<<",">>, AvailNodes)),
            #job_will_run_response{
                job_id = 0,
                start_time = erlang:system_time(second),  % Could start now
                node_list = NodeList,
                proc_cnt = MinCpus,
                error_code = 0
            };
        false ->
            %% Job cannot run - estimate when it might
            #job_will_run_response{
                job_id = 0,
                start_time = 0,  % Never/unknown
                node_list = <<>>,
                proc_cnt = 0,
                error_code = 0  % No error, just can't run
            }
    end,
    {ok, ?RESPONSE_JOB_WILL_RUN, Response};

%% REQUEST_JOB_WILL_RUN fallback
handle(#slurm_header{msg_type = ?REQUEST_JOB_WILL_RUN}, _Body) ->
    Response = #job_will_run_response{
        job_id = 0,
        start_time = 0,
        node_list = <<>>,
        proc_cnt = 0,
        error_code = 0
    },
    {ok, ?RESPONSE_JOB_WILL_RUN, Response};

%% REQUEST_RESERVATION_INFO (2012) -> RESPONSE_RESERVATION_INFO
%% scontrol show reservation
handle(#slurm_header{msg_type = ?REQUEST_RESERVATION_INFO}, _Body) ->
    lager:debug("Handling reservation info request"),
    Reservations = try flurm_reservation:list() catch _:_ -> [] end,
    ResvInfoList = [reservation_to_reservation_info(R) || R <- Reservations],
    Response = #reservation_info_response{
        last_update = erlang:system_time(second),
        reservation_count = length(ResvInfoList),
        reservations = ResvInfoList
    },
    {ok, ?RESPONSE_RESERVATION_INFO, Response};

%% REQUEST_LICENSE_INFO (1017) -> RESPONSE_LICENSE_INFO
%% scontrol show license
handle(#slurm_header{msg_type = ?REQUEST_LICENSE_INFO}, _Body) ->
    lager:debug("Handling license info request"),
    Licenses = try flurm_license:list() catch _:_ -> [] end,
    LicInfoList = [license_to_license_info(L) || L <- Licenses],
    Response = #license_info_response{
        last_update = erlang:system_time(second),
        license_count = length(LicInfoList),
        licenses = LicInfoList
    },
    {ok, ?RESPONSE_LICENSE_INFO, Response};

%% REQUEST_TOPO_INFO (2018) -> RESPONSE_TOPO_INFO
%% scontrol show topology
handle(#slurm_header{msg_type = ?REQUEST_TOPO_INFO}, _Body) ->
    lager:debug("Handling topology info request"),
    %% Return empty topology (no complex network topology configured)
    Response = #topo_info_response{
        topo_count = 0,
        topos = []
    },
    {ok, ?RESPONSE_TOPO_INFO, Response};

%% REQUEST_FRONT_END_INFO (2028) -> RESPONSE_FRONT_END_INFO
%% scontrol show frontend
handle(#slurm_header{msg_type = ?REQUEST_FRONT_END_INFO}, _Body) ->
    lager:debug("Handling front-end info request"),
    %% Return empty (no front-end nodes - typically used on Cray systems)
    Response = #front_end_info_response{
        last_update = erlang:system_time(second),
        front_end_count = 0,
        front_ends = []
    },
    {ok, ?RESPONSE_FRONT_END_INFO, Response};

%% REQUEST_BURST_BUFFER_INFO (2020) -> RESPONSE_BURST_BUFFER_INFO
%% scontrol show burstbuffer
handle(#slurm_header{msg_type = ?REQUEST_BURST_BUFFER_INFO}, _Body) ->
    lager:debug("Handling burst buffer info request"),
    Pools = try flurm_burst_buffer:list_pools() catch _:_ -> [] end,
    Stats = try flurm_burst_buffer:get_stats() catch _:_ -> #{} end,
    BBInfoList = build_burst_buffer_info(Pools, Stats),
    Response = #burst_buffer_info_response{
        last_update = erlang:system_time(second),
        burst_buffer_count = length(BBInfoList),
        burst_buffers = BBInfoList
    },
    {ok, ?RESPONSE_BURST_BUFFER_INFO, Response};

%% REQUEST_SHUTDOWN (1005) -> RESPONSE_SLURM_RC
%% scontrol shutdown
handle(#slurm_header{msg_type = ?REQUEST_SHUTDOWN}, _Body) ->
    lager:info("Handling shutdown request"),
    %% Initiate graceful shutdown
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true ->
                    do_graceful_shutdown();
                false ->
                    case flurm_controller_cluster:forward_to_leader(shutdown, []) of
                        {ok, ShutdownResult} -> ShutdownResult;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            do_graceful_shutdown()
    end,
    case Result of
        ok ->
            lager:info("Shutdown initiated successfully"),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Shutdown failed: ~p", [Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

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
    JobsPending = length([J || J <- Jobs, maps:get(state, J, undefined) =:= pending]),
    JobsRunning = length([J || J <- Jobs, maps:get(state, J, undefined) =:= running]),
    JobsCompleted = length([J || J <- Jobs, maps:get(state, J, undefined) =:= completed]),
    JobsCanceled = length([J || J <- Jobs, maps:get(state, J, undefined) =:= cancelled]),
    JobsFailed = length([J || J <- Jobs, maps:get(state, J, undefined) =:= failed]),

    %% Get scheduler stats if available
    SchedulerStats = try flurm_scheduler:get_stats() catch _:_ -> #{} end,

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

%% Unknown/unsupported message types
handle(#slurm_header{msg_type = MsgType}, _Body) ->
    TypeName = flurm_protocol_codec:message_type_name(MsgType),
    lager:warning("Unsupported message type: ~p (~p)", [MsgType, TypeName]),
    Response = #slurm_rc_response{return_code = -1},
    {ok, ?RESPONSE_SLURM_RC, Response}.

%%====================================================================
%% Internal Functions - Request Conversion
%%====================================================================

%% @doc Convert batch job request record to job spec map for flurm_job_manager
-spec batch_request_to_job_spec(#batch_job_request{}) -> map().
batch_request_to_job_spec(#batch_job_request{} = Req) ->
    #{
        name => Req#batch_job_request.name,
        script => Req#batch_job_request.script,
        partition => default_partition(Req#batch_job_request.partition),
        num_nodes => max(1, Req#batch_job_request.min_nodes),
        num_cpus => max(1, Req#batch_job_request.min_cpus),
        memory_mb => 256,  % Default 256 MB for container compatibility
        time_limit => default_time_limit(Req#batch_job_request.time_limit),
        priority => default_priority(Req#batch_job_request.priority),
        user_id => Req#batch_job_request.user_id,
        group_id => Req#batch_job_request.group_id,
        work_dir => default_work_dir(Req#batch_job_request.work_dir),
        std_out => Req#batch_job_request.std_out,
        std_err => Req#batch_job_request.std_err,
        account => Req#batch_job_request.account
    }.

%% @doc Convert resource allocation request (srun) to job spec map
-spec resource_request_to_job_spec(#resource_allocation_request{}) -> map().
resource_request_to_job_spec(#resource_allocation_request{} = Req) ->
    #{
        name => Req#resource_allocation_request.name,
        script => <<>>,  % No script for srun - command passed separately
        partition => default_partition(Req#resource_allocation_request.partition),
        num_nodes => max(1, Req#resource_allocation_request.min_nodes),
        num_cpus => max(1, Req#resource_allocation_request.min_cpus),
        memory_mb => 256,
        time_limit => default_time_limit(Req#resource_allocation_request.time_limit),
        priority => default_priority(Req#resource_allocation_request.priority),
        user_id => Req#resource_allocation_request.user_id,
        group_id => Req#resource_allocation_request.group_id,
        work_dir => default_work_dir(Req#resource_allocation_request.work_dir),
        std_out => <<>>,
        std_err => <<>>,
        account => Req#resource_allocation_request.account,
        interactive => true  % Mark as interactive job
    }.

%% @doc Provide default work directory if not specified
-spec default_work_dir(binary()) -> binary().
default_work_dir(<<>>) -> <<"/tmp">>;
default_work_dir(WorkDir) -> WorkDir.

%% @doc Provide default partition if not specified
-spec default_partition(binary()) -> binary().
default_partition(<<>>) -> <<"default">>;
default_partition(Partition) -> Partition.

%% @doc Provide default time limit if not specified (1 hour in seconds)
-spec default_time_limit(non_neg_integer()) -> pos_integer().
default_time_limit(0) -> 3600;
default_time_limit(TimeLimit) -> TimeLimit.

%% @doc Provide default priority if not specified
-spec default_priority(non_neg_integer()) -> non_neg_integer().
default_priority(0) -> 100;
default_priority(Priority) -> Priority.

%%====================================================================
%% Internal Functions - Response Conversion
%%====================================================================

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
        num_tasks = 1,
        time_limit = Job#job.time_limit,
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
        cpus_per_task = 1,
        pn_min_cpus = 1,
        %% Default/empty values for remaining fields (MUST be present for protocol)
        account = <<>>,
        accrue_time = 0,
        admin_comment = <<>>,
        alloc_node = <<>>,
        alloc_sid = 0,
        array_job_id = 0,
        array_max_tasks = 0,
        array_task_id = 16#FFFFFFFE,  % NO_VAL for non-array jobs
        array_task_str = <<>>,
        assoc_id = 0,
        billable_tres = 0.0,
        bitflags = 0,
        boards_per_node = 0,
        burst_buffer = <<>>,
        burst_buffer_state = <<>>,
        cluster = <<>>,
        cluster_features = <<>>,
        comment = <<>>,
        container = <<>>,
        contiguous = 0,
        core_spec = 16#FFFF,  % SLURM_NO_VAL16
        cores_per_socket = 0,
        cpus_per_tres = <<>>,
        deadline = 0,
        delay_boot = 0,
        dependency = <<>>,
        derived_ec = 0,
        exc_nodes = <<>>,
        exit_code = 0,
        features = <<>>,
        fed_origin_str = <<>>,
        fed_siblings_active = 0,
        fed_siblings_active_str = <<>>,
        fed_siblings_viable = 0,
        fed_siblings_viable_str = <<>>,
        gres_detail_cnt = 0,
        gres_detail_str = [],
        het_job_id = 0,
        het_job_id_set = <<>>,
        het_job_offset = 16#FFFFFFFF,
        last_sched_eval = 0,
        licenses = <<>>,
        mail_type = 0,
        mail_user = <<>>,
        mcs_label = <<>>,
        mem_per_tres = <<>>,
        min_mem_per_cpu = 0,
        min_mem_per_node = 0,
        network = <<>>,
        nice = 0,
        ntasks_per_core = 16#FFFF,
        ntasks_per_node = 16#FFFF,
        ntasks_per_socket = 16#FFFF,
        ntasks_per_tres = 16#FFFF,
        pn_min_memory = 0,
        pn_min_tmp_disk = 0,
        power_flags = 0,
        preempt_time = 0,
        preemptable_time = 0,
        pre_sus_time = 0,
        profile = 0,
        qos = <<"normal">>,
        reboot = 0,
        req_nodes = <<>>,
        req_switch = 0,
        requeue = 1,
        resize_time = 0,
        restart_cnt = 0,
        resv_name = <<>>,
        sched_nodes = <<>>,
        shared = 0,
        show_flags = 0,
        site_factor = 0,
        sockets_per_board = 0,
        sockets_per_node = 0,
        state_desc = <<>>,
        state_reason = 0,
        std_err = <<>>,
        std_in = <<>>,
        std_out = <<>>,
        suspend_time = 0,
        system_comment = <<>>,
        threads_per_core = 0,
        time_min = 0,
        tres_alloc_str = <<>>,
        tres_bind = <<>>,
        tres_freq = <<>>,
        tres_per_job = <<>>,
        tres_per_node = <<>>,
        tres_per_socket = <<>>,
        tres_per_task = <<>>,
        tres_req_str = <<>>,
        wait4switch = 0,
        wckey = <<>>
    }.

%% @doc Convert internal node record to SLURM node_info record
-spec node_to_node_info(#node{}) -> #node_info{}.
node_to_node_info(#node{} = Node) ->
    #node_info{
        name = Node#node.hostname,
        node_hostname = Node#node.hostname,
        node_addr = Node#node.hostname,
        port = 6818,  % Default slurmd port
        node_state = node_state_to_slurm(Node#node.state),
        cpus = Node#node.cpus,
        real_memory = Node#node.memory_mb,
        free_mem = Node#node.free_memory_mb,
        cpu_load = trunc(Node#node.load_avg * 100),
        features = format_features(Node#node.features),
        partitions = format_partitions(Node#node.partitions),
        version = <<"22.05.0">>,
        arch = <<"x86_64">>,
        os = <<"Linux">>
    }.

%% @doc Convert internal partition record to SLURM partition_info record
-spec partition_to_partition_info(#partition{}) -> #partition_info{}.
partition_to_partition_info(#partition{} = Part) ->
    %% For now, auto-include all registered nodes in the partition
    %% In production, this would be based on configuration
    AllNodes = [N#node.hostname || N <- flurm_node_manager:list_nodes()],
    TotalCpus = lists:sum([N#node.cpus || N <- flurm_node_manager:list_nodes()]),
    #partition_info{
        name = Part#partition.name,
        state_up = partition_state_to_slurm(Part#partition.state),
        max_time = Part#partition.max_time,
        default_time = Part#partition.default_time,
        max_nodes = Part#partition.max_nodes,
        min_nodes = 1,
        total_nodes = length(AllNodes),
        total_cpus = TotalCpus,
        nodes = format_node_list(AllNodes),
        priority_tier = Part#partition.priority,
        priority_job_factor = 1
    }.

%% @doc Convert step manager map to SLURM job_step_info record
-spec step_map_to_info(map()) -> #job_step_info{}.
step_map_to_info(StepMap) ->
    State = step_state_to_slurm(maps:get(state, StepMap, pending)),
    StartTime = maps:get(start_time, StepMap, 0),
    RunTime = case StartTime of
        0 -> 0;
        undefined -> 0;
        _ -> erlang:system_time(second) - StartTime
    end,
    #job_step_info{
        job_id = maps:get(job_id, StepMap, 0),
        step_id = maps:get(step_id, StepMap, 0),
        step_name = ensure_binary(maps:get(name, StepMap, <<>>)),
        partition = <<>>,  % Steps inherit partition from job
        user_id = 0,
        user_name = <<"root">>,
        state = State,
        num_tasks = maps:get(num_tasks, StepMap, 1),
        num_cpus = maps:get(num_tasks, StepMap, 1),  % Approximate
        time_limit = 0,  % No separate time limit for steps
        start_time = default_time(StartTime),
        run_time = RunTime,
        nodes = format_allocated_nodes(maps:get(allocated_nodes, StepMap, [])),
        node_cnt = maps:get(num_nodes, StepMap, 0),
        tres_alloc_str = <<>>,
        exit_code = maps:get(exit_code, StepMap, 0)
    }.

%% @doc Convert internal step state to SLURM step state integer
%% SLURM step states are similar to job states
-spec step_state_to_slurm(atom()) -> non_neg_integer().
step_state_to_slurm(pending) -> ?JOB_PENDING;
step_state_to_slurm(running) -> ?JOB_RUNNING;
step_state_to_slurm(completing) -> ?JOB_RUNNING;
step_state_to_slurm(completed) -> ?JOB_COMPLETE;
step_state_to_slurm(cancelled) -> ?JOB_CANCELLED;
step_state_to_slurm(failed) -> ?JOB_FAILED;
step_state_to_slurm(_) -> ?JOB_PENDING.

%%====================================================================
%% Internal Functions - State Conversion
%%====================================================================

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

%% @doc Convert internal node state to SLURM node state integer
-spec node_state_to_slurm(node_state()) -> non_neg_integer().
node_state_to_slurm(up) -> ?NODE_STATE_IDLE;
node_state_to_slurm(down) -> ?NODE_STATE_DOWN;
node_state_to_slurm(drain) -> ?NODE_STATE_DOWN;
node_state_to_slurm(idle) -> ?NODE_STATE_IDLE;
node_state_to_slurm(allocated) -> ?NODE_STATE_ALLOCATED;
node_state_to_slurm(mixed) -> ?NODE_STATE_MIXED;
node_state_to_slurm(_) -> ?NODE_STATE_UNKNOWN.

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
%% Internal Functions - Formatting Helpers
%%====================================================================

%% @doc Ensure value is binary
-spec ensure_binary(term()) -> binary().
ensure_binary(undefined) -> <<>>;
ensure_binary(Bin) when is_binary(Bin) -> Bin;
ensure_binary(List) when is_list(List) -> list_to_binary(List);
ensure_binary(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
ensure_binary(_) -> <<>>.

%% @doc Convert error reason to binary
-spec error_to_binary(term()) -> binary().
error_to_binary(Reason) when is_binary(Reason) -> Reason;
error_to_binary(Reason) when is_atom(Reason) -> atom_to_binary(Reason, utf8);
error_to_binary(Reason) when is_list(Reason) -> list_to_binary(Reason);
error_to_binary(Reason) ->
    list_to_binary(io_lib:format("~p", [Reason])).

%% @doc Provide default time value
-spec default_time(non_neg_integer() | undefined) -> non_neg_integer().
default_time(undefined) -> 0;
default_time(Time) -> Time.

%% @doc Format allocated nodes list to binary
-spec format_allocated_nodes([binary()]) -> binary().
format_allocated_nodes([]) -> <<>>;
format_allocated_nodes(Nodes) ->
    iolist_to_binary(lists:join(<<",">>, Nodes)).

%% @doc Format features list to binary
-spec format_features([binary()]) -> binary().
format_features([]) -> <<>>;
format_features(Features) ->
    iolist_to_binary(lists:join(<<",">>, Features)).

%% @doc Format partitions list to binary
-spec format_partitions([binary()]) -> binary().
format_partitions([]) -> <<>>;
format_partitions(Partitions) ->
    iolist_to_binary(lists:join(<<",">>, Partitions)).

%% @doc Format node list to binary
-spec format_node_list([binary()]) -> binary().
format_node_list([]) -> <<>>;
format_node_list(Nodes) ->
    iolist_to_binary(lists:join(<<",">>, Nodes)).

%%====================================================================
%% Internal Functions - Job Control Helpers
%%====================================================================

%% @doc Parse job ID from job ID string
-spec parse_job_id_str(binary()) -> non_neg_integer().
parse_job_id_str(<<>>) -> 0;
parse_job_id_str(JobIdStr) when is_binary(JobIdStr) ->
    %% Strip null terminator if present
    Stripped = case binary:match(JobIdStr, <<0>>) of
        {Pos, _} -> binary:part(JobIdStr, 0, Pos);
        nomatch -> JobIdStr
    end,
    %% Extract numeric part (handle "123_4" format for array jobs)
    case binary:split(Stripped, <<"_">>) of
        [BaseId | _] -> safe_binary_to_integer(BaseId);
        _ -> safe_binary_to_integer(Stripped)
    end.

%% @doc Safe binary to integer conversion
-spec safe_binary_to_integer(binary()) -> non_neg_integer().
safe_binary_to_integer(Bin) ->
    try binary_to_integer(Bin)
    catch _:_ -> 0
    end.

%% @doc Build job updates map from update request fields
%% SLURM_NO_VAL = 0xFFFFFFFE indicates field not set
-spec build_job_updates(non_neg_integer(), non_neg_integer(), non_neg_integer()) -> map().
build_job_updates(Priority, TimeLimit, Requeue) ->
    Updates0 = #{},
    Updates1 = case Priority of
        16#FFFFFFFE -> Updates0;
        0 -> maps:put(state, held, Updates0);  % Priority 0 = hold
        P -> maps:put(priority, P, Updates0)
    end,
    Updates2 = case TimeLimit of
        16#FFFFFFFE -> Updates1;
        T -> maps:put(time_limit, T, Updates1)
    end,
    case Requeue of
        16#FFFFFFFE -> Updates2;
        1 -> maps:put(state, pending, Updates2);  % Requeue = put back in pending
        _ -> Updates2
    end.

%%====================================================================
%% Internal Functions - Cluster Helpers
%%====================================================================

%% @doc Check if cluster mode is enabled.
%% Cluster mode is enabled when there are multiple nodes configured.
-spec is_cluster_enabled() -> boolean().
is_cluster_enabled() ->
    case application:get_env(flurm_controller, cluster_nodes) of
        {ok, Nodes} when is_list(Nodes), length(Nodes) > 1 ->
            true;
        _ ->
            %% Also check if the cluster process is running
            case whereis(flurm_controller_cluster) of
                undefined -> false;
                _Pid -> true
            end
    end.

%%====================================================================
%% Internal Functions - Reconfiguration
%%====================================================================

%% @doc Perform hot reconfiguration by reloading slurm.conf
-spec do_reconfigure() -> ok | {error, term()}.
do_reconfigure() ->
    lager:info("Starting hot reconfiguration"),

    %% Step 1: Reload configuration from file
    case flurm_config_server:reconfigure() of
        ok ->
            lager:info("Configuration reloaded from file"),

            %% Step 2: Apply node changes
            apply_node_changes(),

            %% Step 3: Apply partition changes
            apply_partition_changes(),

            %% Step 4: Notify scheduler to refresh
            flurm_scheduler:trigger_schedule(),

            lager:info("Reconfiguration complete"),
            ok;
        {error, Reason} ->
            lager:error("Failed to reload configuration: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Perform graceful shutdown of the controller
-spec do_graceful_shutdown() -> ok | {error, term()}.
do_graceful_shutdown() ->
    lager:info("Starting graceful shutdown"),
    try
        %% Step 1: Stop accepting new jobs
        lager:info("Stopping acceptance of new jobs"),

        %% Step 2: Allow running jobs to continue (don't kill them)
        %% They will be orphaned but nodes can clean them up

        %% Step 3: Schedule application stop
        %% Use a brief delay to ensure response is sent first
        spawn(fun() ->
            timer:sleep(500),
            lager:info("Initiating application stop"),
            application:stop(flurm_controller),
            application:stop(flurm_config),
            application:stop(flurm_protocol),
            init:stop()
        end),
        ok
    catch
        _:Reason ->
            lager:error("Error during graceful shutdown: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Apply node configuration changes
-spec apply_node_changes() -> ok.
apply_node_changes() ->
    %% Get nodes from config
    ConfigNodes = flurm_config_server:get_nodes(),
    lager:debug("Config has ~p node definitions", [length(ConfigNodes)]),

    %% For each node definition, ensure it exists in the node manager
    lists:foreach(fun(NodeDef) ->
        case maps:get(nodename, NodeDef, undefined) of
            undefined -> ok;
            NodePattern ->
                %% Expand hostlist pattern
                ExpandedNodes = flurm_config_slurm:expand_hostlist(NodePattern),
                lists:foreach(fun(NodeName) ->
                    ensure_node_exists(NodeName, NodeDef)
                end, ExpandedNodes)
        end
    end, ConfigNodes),
    ok.

%% @doc Ensure a node exists in the node manager (for pre-configured nodes)
-spec ensure_node_exists(binary(), map()) -> ok.
ensure_node_exists(NodeName, NodeDef) ->
    case flurm_node_manager:get_node(NodeName) of
        {ok, _Node} ->
            %% Node exists, could update if needed
            ok;
        {error, not_found} ->
            %% Node not registered yet, could pre-register or just log
            lager:debug("Node ~s defined in config but not registered", [NodeName]),
            ok
    end,
    %% Extract CPUs and memory from config if available
    _Cpus = maps:get(cpus, NodeDef, maps:get(cpu, NodeDef, 1)),
    _Memory = maps:get(realmemory, NodeDef, maps:get(memory_mb, NodeDef, 1024)),
    ok.

%% @doc Apply partition configuration changes
-spec apply_partition_changes() -> ok.
apply_partition_changes() ->
    %% Get partitions from config
    ConfigPartitions = flurm_config_server:get_partitions(),
    lager:debug("Config has ~p partition definitions", [length(ConfigPartitions)]),

    %% For each partition, ensure it exists
    lists:foreach(fun(PartDef) ->
        case maps:get(partitionname, PartDef, undefined) of
            undefined -> ok;
            PartName ->
                ensure_partition_exists(PartName, PartDef)
        end
    end, ConfigPartitions),
    ok.

%% @doc Ensure a partition exists in the partition manager
-spec ensure_partition_exists(binary(), map()) -> ok.
ensure_partition_exists(PartName, PartDef) ->
    case flurm_partition_manager:get_partition(PartName) of
        {ok, _Part} ->
            %% Partition exists, update if needed
            update_partition(PartName, PartDef);
        {error, not_found} ->
            %% Create partition
            create_partition(PartName, PartDef)
    end.

%% @doc Create a new partition from config
-spec create_partition(binary(), map()) -> ok.
create_partition(PartName, PartDef) ->
    %% Extract nodes from definition
    NodesPattern = maps:get(nodes, PartDef, <<>>),
    Nodes = case NodesPattern of
        <<>> -> [];
        _ -> flurm_config_slurm:expand_hostlist(NodesPattern)
    end,

    PartSpec = #{
        name => PartName,
        nodes => Nodes,
        state => case maps:get(state, PartDef, up) of
            <<"UP">> -> up;
            <<"DOWN">> -> down;
            Other when is_atom(Other) -> Other;
            _ -> up
        end,
        default => maps:get(default, PartDef, false) =:= true,
        max_time => maps:get(maxtime, PartDef, infinity),
        priority => maps:get(prioritytier, PartDef, 1)
    },

    case flurm_partition_manager:create_partition(PartSpec) of
        ok ->
            lager:info("Created partition ~s with ~p nodes", [PartName, length(Nodes)]);
        {error, Reason} ->
            lager:warning("Failed to create partition ~s: ~p", [PartName, Reason])
    end,
    ok.

%% @doc Update an existing partition from config
-spec update_partition(binary(), map()) -> ok.
update_partition(PartName, PartDef) ->
    %% Update partition state if changed
    %% Note: set_state may not be implemented yet
    NewState = case maps:get(state, PartDef, undefined) of
        undefined -> undefined;
        <<"UP">> -> up;
        <<"DOWN">> -> down;
        up -> up;
        down -> down;
        _ -> undefined
    end,
    case NewState of
        undefined ->
            ok;
        State ->
            %% Try to update state if the function exists
            try
                flurm_partition_manager:update_partition(PartName, #{state => State})
            catch
                error:undef ->
                    lager:debug("Partition state update not supported yet for ~s", [PartName]);
                _:_ ->
                    ok
            end
    end,
    ok.

%%====================================================================
%% Internal Functions - Info/Query Conversion (Batch 3)
%%====================================================================

%% @doc Convert internal reservation record to SLURM reservation_info record
-spec reservation_to_reservation_info(tuple()) -> #reservation_info{}.
reservation_to_reservation_info(Resv) ->
    %% Handle both record and map formats from flurm_reservation
    {Name, StartTime, EndTime, Nodes, Users, State, _Flags} = extract_reservation_fields(Resv),
    NodeList = format_allocated_nodes(Nodes),
    UserList = iolist_to_binary(lists:join(<<",">>, Users)),
    #reservation_info{
        name = ensure_binary(Name),
        accounts = <<>>,
        burst_buffer = <<>>,
        core_cnt = 0,
        core_spec_cnt = 0,
        end_time = EndTime,
        features = <<>>,
        flags = reservation_state_to_flags(State),
        groups = <<>>,
        licenses = <<>>,
        max_start_delay = 0,
        node_cnt = length(Nodes),
        node_list = NodeList,
        partition = <<>>,
        purge_comp_time = 0,
        resv_watts = 0,
        start_time = StartTime,
        tres_str = <<>>,
        users = UserList
    }.

%% @doc Extract fields from reservation record/tuple
extract_reservation_fields(Resv) when is_tuple(Resv) ->
    %% Assume record format from flurm_reservation
    %% #reservation{name, type, start_time, end_time, duration, nodes, node_count,
    %%              partition, features, users, accounts, flags, tres, state, ...}
    case tuple_size(Resv) of
        N when N >= 11 ->
            Name = element(2, Resv),
            StartTime = element(4, Resv),
            EndTime = element(5, Resv),
            Nodes = element(7, Resv),
            Users = element(11, Resv),
            State = element(14, Resv),
            Flags = element(12, Resv),
            {Name, StartTime, EndTime, Nodes, Users, State, Flags};
        _ ->
            {<<>>, 0, 0, [], [], inactive, []}
    end;
extract_reservation_fields(_) ->
    {<<>>, 0, 0, [], [], inactive, []}.

%% @doc Convert reservation state to SLURM flags
reservation_state_to_flags(active) -> 1;
reservation_state_to_flags(inactive) -> 0;
reservation_state_to_flags(expired) -> 2;
reservation_state_to_flags(_) -> 0.

%% @doc Convert internal license record/map to SLURM license_info record
-spec license_to_license_info(tuple() | map()) -> #license_info{}.
license_to_license_info(Lic) when is_map(Lic) ->
    #license_info{
        name = ensure_binary(maps:get(name, Lic, <<>>)),
        total = maps:get(total, Lic, 0),
        in_use = maps:get(in_use, Lic, 0),
        available = maps:get(available, Lic, maps:get(total, Lic, 0) - maps:get(in_use, Lic, 0)),
        reserved = maps:get(reserved, Lic, 0),
        remote = case maps:get(remote, Lic, false) of true -> 1; _ -> 0 end
    };
license_to_license_info(Lic) when is_tuple(Lic) ->
    %% Handle record format if used
    case tuple_size(Lic) of
        N when N >= 5 ->
            #license_info{
                name = ensure_binary(element(2, Lic)),
                total = element(3, Lic),
                in_use = element(4, Lic),
                available = element(3, Lic) - element(4, Lic),
                reserved = 0,
                remote = 0
            };
        _ ->
            #license_info{}
    end;
license_to_license_info(_) ->
    #license_info{}.

%% @doc Build burst buffer info from pools and stats
-spec build_burst_buffer_info([tuple()], map()) -> [#burst_buffer_info{}].
build_burst_buffer_info([], _Stats) ->
    [];
build_burst_buffer_info(Pools, Stats) ->
    %% Group pools into a single burst_buffer_info entry
    %% In SLURM, each plugin (datawarp, generic, etc.) is one entry
    PoolInfos = [pool_to_bb_pool(P) || P <- Pools],
    TotalSpace = maps:get(total_size, Stats, 0),
    UsedSpace = maps:get(used_size, Stats, 0),
    UnfreeSpace = TotalSpace - maps:get(free_size, Stats, TotalSpace),
    [#burst_buffer_info{
        name = <<"generic">>,
        default_pool = <<"default">>,
        allow_users = <<>>,
        create_buffer = <<>>,
        deny_users = <<>>,
        destroy_buffer = <<>>,
        flags = 0,
        get_sys_state = <<>>,
        get_sys_status = <<>>,
        granularity = 1048576,  % 1 MB
        pool_cnt = length(PoolInfos),
        pools = PoolInfos,
        other_timeout = 300,
        stage_in_timeout = 300,
        stage_out_timeout = 300,
        start_stage_in = <<>>,
        start_stage_out = <<>>,
        stop_stage_in = <<>>,
        stop_stage_out = <<>>,
        total_space = TotalSpace,
        unfree_space = UnfreeSpace,
        used_space = UsedSpace,
        validate_timeout = 60
    }].

%% @doc Convert pool record to burst_buffer_pool
-spec pool_to_bb_pool(tuple()) -> #burst_buffer_pool{}.
pool_to_bb_pool(Pool) when is_tuple(Pool) ->
    %% #bb_pool{name, type, total_size, free_size, granularity, nodes, state, properties}
    case tuple_size(Pool) of
        N when N >= 5 ->
            Name = element(2, Pool),
            TotalSize = element(4, Pool),
            FreeSize = element(5, Pool),
            Granularity = element(6, Pool),
            #burst_buffer_pool{
                name = ensure_binary(Name),
                total_space = TotalSize,
                granularity = Granularity,
                unfree_space = TotalSize - FreeSize,
                used_space = TotalSize - FreeSize
            };
        _ ->
            #burst_buffer_pool{name = <<"default">>}
    end;
pool_to_bb_pool(_) ->
    #burst_buffer_pool{name = <<"default">>}.

