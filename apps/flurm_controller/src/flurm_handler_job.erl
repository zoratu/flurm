%%%-------------------------------------------------------------------
%%% @doc FLURM Job Handler
%%%
%%% Handles job-related operations including:
%%% - Job submission (batch and interactive/srun)
%%% - Job cancellation and killing
%%% - Job suspension/resume
%%% - Job signaling
%%% - Job updates (hold, release, requeue)
%%% - Job will_run checks
%%%
%%% Split from flurm_controller_handler.erl for maintainability.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_job).

-export([handle/2]).

%% Exported for use by main handler
-export([
    batch_request_to_job_spec/1,
    resource_request_to_job_spec/1,
    submit_job_with_federation/1,
    submit_job_locally/1,
    is_federation_routing_enabled/0,
    get_local_cluster_name/0,
    is_cluster_enabled/0,
    execute_suspend/2,
    execute_job_update/5,
    build_field_updates/2,
    apply_base_updates/2,
    parse_job_id_str/1,
    safe_binary_to_integer/1,
    validate_partition/1
]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% Import helpers from main handler
-import(flurm_controller_handler, [
    error_to_binary/1,
    default_partition/1,
    default_time_limit/1,
    default_priority/1,
    default_work_dir/1
]).

%% SLURM error codes
-define(ESLURM_CONTROLLER_NOT_FOUND, 1).

%%====================================================================
%% API
%%====================================================================

%% REQUEST_SUBMIT_BATCH_JOB (4003) -> RESPONSE_SUBMIT_BATCH_JOB
%% Write operation - requires leader or forwarding to leader
%% Supports federation routing when federation is enabled
handle(#slurm_header{msg_type = ?REQUEST_SUBMIT_BATCH_JOB},
       #batch_job_request{} = Request) ->
    lager:info("Handling batch job submission: ~s", [Request#batch_job_request.name]),
    JobSpec = batch_request_to_job_spec(Request),
    %% Validate partition exists before submitting
    PartitionName = maps:get(partition, JobSpec),
    Result = case validate_partition(PartitionName) of
        ok ->
            submit_job_with_federation(JobSpec);
        {error, _} = PartErr ->
            PartErr
    end,
    case Result of
        {ok, {array, ArrayJobId}} ->
            lager:info("Job {array,~p} submitted successfully", [ArrayJobId]),
            Response = #batch_job_response{
                job_id = ArrayJobId,
                step_id = 0,
                error_code = 0,
                job_submit_user_msg = <<"Job submitted successfully">>
            },
            {ok, ?RESPONSE_SUBMIT_BATCH_JOB, Response};
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
%% Returns {ok, MsgType, Response, CallbackInfo} where CallbackInfo contains
%% the callback port for the acceptor to register.
handle(#slurm_header{msg_type = ?REQUEST_RESOURCE_ALLOCATION},
       #resource_allocation_request{} = Request) ->
    lager:info("Handling srun resource allocation: ~s", [Request#resource_allocation_request.name]),

    %% Extract callback info from the request
    CallbackPort = Request#resource_allocation_request.alloc_resp_port,
    SrunPid = Request#resource_allocation_request.srun_pid,
    lager:info("srun callback port=~p, pid=~p", [CallbackPort, SrunPid]),

    JobSpec = resource_request_to_job_spec(Request),
    %% srun is interactive - always submit locally (can't route to remote clusters)
    Result = submit_job_locally(JobSpec),
    case Result of
        {ok, JobId} ->
            lager:info("srun job ~p allocated", [JobId]),
            %% Get actual node for allocation
            %% Note: Node may not be registered yet in Docker startup race condition
            %% Use configured default or "flurm-node" for Docker networking
            DefaultNode = try flurm_config_server:get(default_node, <<"flurm-node">>) catch _:_ -> <<"flurm-node">> end,
            Nodes = flurm_node_manager_server:list_nodes(),
            NodeName = case Nodes of
                [] ->
                    lager:info("No nodes registered yet, using default: ~s", [DefaultNode]),
                    DefaultNode;
                [FirstNode | _] ->
                    %% Node is a #node{} record with hostname field
                    case FirstNode of
                        #node{hostname = H} when is_binary(H) -> H;
                        #{hostname := H} -> H;
                        _ -> DefaultNode
                    end
            end,
            lager:info("srun job ~p assigned to node ~s", [JobId, NodeName]),
            %% Resolve node hostname to IP address for srun to connect to slurmd
            SlurmdPort = try flurm_config_server:get(slurmd_port, 6818) catch _:_ -> 6818 end,
            NodeAddrs = case inet:getaddr(binary_to_list(NodeName), inet) of
                {ok, IP} ->
                    lager:info("Resolved ~s to ~p:~p for srun step connection", [NodeName, IP, SlurmdPort]),
                    [{IP, SlurmdPort}];
                {error, _} ->
                    lager:warning("Could not resolve ~s, trying localhost", [NodeName]),
                    [{{127, 0, 0, 1}, SlurmdPort}]
            end,
            %% Return successful allocation with SLURM 22.05 format
            %% IMPORTANT: Must set cpus_per_node array for srun to proceed
            %% Format: num_cpu_groups=N, cpus_per_node=[CPUs_per_group], each group repeated for some nodes
            %% For simplicity: 1 group with all nodes having same CPUs
            NumCpus = 1,  %% Default to 1 CPU per node (srun can override with -c)
            lager:info("Returning allocation for job_id=~p, node=~s, node_addrs=~p, cpus=~p, error_code=0",
                       [JobId, NodeName, NodeAddrs, NumCpus]),
            %% Return proper allocation response
            Response = #resource_allocation_response{
                job_id = JobId,
                node_list = NodeName,
                num_nodes = 1,
                partition = <<"default">>,
                error_code = 0,
                job_submit_user_msg = <<>>,
                cpus_per_node = [1],        %% 1 CPU
                num_cpu_groups = 1,         %% 1 group
                node_addrs = NodeAddrs
            },
            lager:info("Allocation response: job=~p node=~s cpus=[1] addrs=~p", [JobId, NodeName, NodeAddrs]),
            CallbackInfo = #{
                job_id => JobId,
                port => CallbackPort,
                srun_pid => SrunPid,
                node_list => NodeName
            },
            {ok, ?RESPONSE_RESOURCE_ALLOCATION, Response, CallbackInfo};
        {error, Reason2} ->
            lager:warning("srun allocation failed: ~p", [Reason2]),
            Response = #resource_allocation_response{
                job_id = 0,
                node_list = <<>>,
                num_nodes = 0,
                partition = <<"default">>,
                error_code = 1,
                job_submit_user_msg = error_to_binary(Reason2),
                cpus_per_node = [],
                num_cpu_groups = 0
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

%% REQUEST_JOB_ALLOCATION_INFO (4014) -> RESPONSE_JOB_ALLOCATION_INFO (4015)
%% srun may send this to query allocation info (less common in 22.05, see REQUEST_JOB_READY)
handle(#slurm_header{msg_type = ?REQUEST_JOB_ALLOCATION_INFO}, Body) ->
    %% Body should contain job_id
    {JobId, BodyHex} = case Body of
        <<JId:32/big, _Rest/binary>> ->
            Hex = [io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(Body)],
            {JId, iolist_to_binary(Hex)};
        _ ->
            {0, <<"invalid">>}
    end,
    lager:info("Handling job allocation info request for job_id=~p, body_hex=~s", [JobId, BodyHex]),
    %% Get actual node for the allocation
    %% Note: Node may not be registered yet in Docker startup race condition
    DefaultNode = try flurm_config_server:get(default_node, <<"flurm-node">>) catch _:_ -> <<"flurm-node">> end,
    Nodes = flurm_node_manager_server:list_nodes(),
    NodeName = case Nodes of
        [] ->
            lager:info("No nodes registered yet, using default: ~s", [DefaultNode]),
            DefaultNode;
        [FirstNode | _] ->
            %% Node is a #node{} record with hostname field
            case FirstNode of
                #node{hostname = H} when is_binary(H) -> H;
                #{hostname := H} -> H;
                _ -> DefaultNode
            end
    end,
    %% Resolve node hostname to IP address for srun to connect to slurmd
    SlurmdPort = try flurm_config_server:get(slurmd_port, 6818) catch _:_ -> 6818 end,
    NodeAddrs = case inet:getaddr(binary_to_list(NodeName), inet) of
        {ok, IP} ->
            lager:info("Verification: resolved ~s to ~p:~p", [NodeName, IP, SlurmdPort]),
            [{IP, SlurmdPort}];
        {error, _} ->
            lager:warning("Verification: could not resolve ~s, trying localhost", [NodeName]),
            [{{127, 0, 0, 1}, SlurmdPort}]
    end,
    %% Return the allocation info with actual node and addresses
    %% IMPORTANT: Must match the original RESPONSE_RESOURCE_ALLOCATION exactly!
    %% srun verifies these fields match, so cpus_per_node and num_cpu_groups must be same
    Response = #resource_allocation_response{
        job_id = JobId,
        node_list = NodeName,
        num_nodes = 1,
        partition = <<"default">>,
        error_code = 0,
        job_submit_user_msg = <<>>,
        cpus_per_node = [1],    %% 1 CPU per node (must match allocation)
        num_cpu_groups = 1,     %% 1 CPU group (must match allocation)
        node_addrs = NodeAddrs  %% Include node addresses for srun to connect to slurmd
    },
    lager:info("Returning allocation info for job_id=~p, node=~s, num_cpu_groups=1, node_addrs=~p", [JobId, NodeName, NodeAddrs]),
    %% SLURM expects RESPONSE_JOB_ALLOCATION_INFO (4015)
    %% The body format is the same as RESPONSE_RESOURCE_ALLOCATION
    {ok, ?RESPONSE_JOB_ALLOCATION_INFO, Response};

%% REQUEST_JOB_READY (4019) -> RESPONSE_JOB_READY (4020)
%% srun sends this to controller to check if nodes are ready for the job
%% This is the message srun sends AFTER receiving RESPONSE_RESOURCE_ALLOCATION
handle(#slurm_header{msg_type = ?REQUEST_JOB_READY}, Body) ->
    %% Body format: job_id:32 + step_id:16 (job_id_msg_t in SLURM 22.05)
    JobId = case Body of
        <<JId:32/big, _/binary>> -> JId;
        _ -> 0
    end,
    lager:info("REQUEST_JOB_READY (4019): job_id=~p - reporting READY", [JobId]),
    %% RESPONSE_JOB_READY format: return_code(32)
    %% IMPORTANT: return_code is a bitmask:
    %%   READY_NODE_STATE  = 0x01 (nodes are ready)
    %%   READY_JOB_STATE   = 0x02 (job is ready)
    %%   READY_PROLOG_STATE = 0x04 (PrologSlurmctld is done)
    %% srun may wait for all bits to be set (7) before proceeding
    Response = #{return_code => 7},  %% READY_NODE_STATE | READY_JOB_STATE | READY_PROLOG_STATE
    {ok, ?RESPONSE_JOB_READY, Response};

%% REQUEST_KILL_TIMELIMIT (5017) - srun sends this to cancel jobs
handle(#slurm_header{msg_type = ?REQUEST_KILL_TIMELIMIT}, Body) ->
    BodyHex = iolist_to_binary([io_lib:format("~2.16.0B", [B]) || B <- binary_to_list(Body)]),
    lager:info("REQUEST_KILL_TIMELIMIT (5017): body_len=~p, hex=~s", [byte_size(Body), BodyHex]),
    %% Try to decode: typically job_id:32, step_id:32 or similar
    {JobId, StepId} = case Body of
        <<JId:32/big, SId:32/big, _/binary>> -> {JId, SId};
        <<JId2:32/big, _/binary>> -> {JId2, 0};
        _ -> {0, 0}
    end,
    lager:info("Kill timelimit request: job_id=~p, step_id=~p", [JobId, StepId]),
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

%% REQUEST_SUSPEND (5014) -> RESPONSE_SLURM_RC
%% Used by scontrol suspend/resume
handle(#slurm_header{msg_type = ?REQUEST_SUSPEND},
       #suspend_request{job_id = JobId, suspend = Suspend}) ->
    lager:info("Handling suspend request for job_id=~p suspend=~p", [JobId, Suspend]),
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true ->
                    execute_suspend(JobId, Suspend);
                false ->
                    Op = case Suspend of true -> suspend_job; false -> resume_job end,
                    case flurm_controller_cluster:forward_to_leader(Op, JobId) of
                        {ok, SuspendResult} -> SuspendResult;
                        {error, no_leader} -> {error, controller_not_found};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            execute_suspend(JobId, Suspend)
    end,
    case Result of
        ok ->
            Action = case Suspend of true -> "suspended"; false -> "resumed" end,
            lager:info("Job ~p ~s successfully", [JobId, Action]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Suspend/resume failed for job ~p: ~p", [JobId, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_SIGNAL_JOB (5018) -> RESPONSE_SLURM_RC
%% Used by scancel -s SIGNAL
handle(#slurm_header{msg_type = ?REQUEST_SIGNAL_JOB},
       #signal_job_request{job_id = JobId, signal = Signal}) ->
    lager:info("Handling signal job request for job_id=~p signal=~p", [JobId, Signal]),
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true ->
                    flurm_job_manager:signal_job(JobId, Signal);
                false ->
                    case flurm_controller_cluster:forward_to_leader(signal_job, {JobId, Signal}) of
                        {ok, SignalResult} -> SignalResult;
                        {error, no_leader} -> {error, controller_not_found};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_job_manager:signal_job(JobId, Signal)
    end,
    case Result of
        ok ->
            lager:info("Signal ~p sent to job ~p successfully", [Signal, JobId]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Signal job failed for job ~p: ~p", [JobId, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_UPDATE_JOB (4014) -> RESPONSE_SLURM_RC
%% Used by scontrol update job, hold, release, requeue
%% Properly routes to hold_job/release_job/requeue_job for correct side effects
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
    Name = Request#update_job_request.name,

    %% Route to appropriate job manager function for proper side effects
    %% Priority=0 means hold, Priority=non-zero+non-NO_VAL means release (restore priority)
    %% Requeue=1 means requeue the job
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> execute_job_update(JobId, Priority, TimeLimit, Requeue, Name);
                false ->
                    case flurm_controller_cluster:forward_to_leader(update_job_ext, {JobId, Priority, TimeLimit, Requeue, Name}) of
                        {ok, UpdateResult} -> UpdateResult;
                        {error, no_leader} -> {error, controller_not_found};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            execute_job_update(JobId, Priority, TimeLimit, Requeue, Name)
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
        flurm_node_manager_server:get_available_nodes_for_job(Partition, MinNodes, MinCpus)
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
    {ok, ?RESPONSE_JOB_WILL_RUN, Response}.

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
        num_tasks => max(1, Req#batch_job_request.num_tasks),
        cpus_per_task => max(1, Req#batch_job_request.cpus_per_task),
        memory_mb => case Req#batch_job_request.min_mem_per_node of
            M when M > 0 -> M;
            _ -> 256  % Default 256 MB for container compatibility
        end,
        time_limit => default_time_limit(Req#batch_job_request.time_limit),
        priority => default_priority(Req#batch_job_request.priority),
        user_id => Req#batch_job_request.user_id,
        group_id => Req#batch_job_request.group_id,
        work_dir => default_work_dir(Req#batch_job_request.work_dir),
        std_out => Req#batch_job_request.std_out,
        std_err => Req#batch_job_request.std_err,
        account => Req#batch_job_request.account,
        licenses => Req#batch_job_request.licenses,
        dependency => Req#batch_job_request.dependency,
        array => case Req#batch_job_request.array_inx of
            <<>> -> undefined;
            ArrayStr -> ArrayStr
        end
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
        licenses => Req#resource_allocation_request.licenses,
        interactive => true  % Mark as interactive job
    }.

%% @doc Submit job with federation routing support.
%% If federation is enabled and a remote cluster is selected, submits to that cluster.
%% Otherwise, submits locally via job manager.
-spec submit_job_with_federation(map()) -> {ok, pos_integer()} | {error, term()}.
submit_job_with_federation(JobSpec) ->
    %% Check if federation routing is enabled
    case is_federation_routing_enabled() of
        true ->
            %% Get local cluster name for comparison
            LocalClusterName = get_local_cluster_name(),
            %% Try to route via federation
            case catch flurm_federation:route_job(JobSpec) of
                {ok, RoutedCluster} when RoutedCluster =:= LocalClusterName ->
                    %% Routed to local cluster - submit locally
                    lager:info("Federation routed job to local cluster"),
                    submit_job_locally(JobSpec);
                {ok, RemoteCluster} ->
                    %% Routed to remote cluster
                    lager:info("Federation routing job to cluster: ~s", [RemoteCluster]),
                    case flurm_federation:submit_job(RemoteCluster, JobSpec) of
                        {ok, RemoteJobId} ->
                            %% Track the remote job locally
                            flurm_federation:track_remote_job(RemoteCluster, RemoteJobId, JobSpec),
                            {ok, RemoteJobId};
                        {error, Reason} ->
                            lager:warning("Remote job submission failed: ~p, falling back to local", [Reason]),
                            submit_job_locally(JobSpec)
                    end;
                {error, no_eligible_clusters} ->
                    %% No remote clusters available, submit locally
                    lager:info("No eligible remote clusters, submitting locally"),
                    submit_job_locally(JobSpec);
                {'EXIT', {noproc, _}} ->
                    %% Federation server not running
                    submit_job_locally(JobSpec);
                _ ->
                    submit_job_locally(JobSpec)
            end;
        false ->
            submit_job_locally(JobSpec)
    end.

%% @doc Submit job locally via job manager, handling cluster mode.
-spec submit_job_locally(map()) -> {ok, pos_integer()} | {error, term()}.
submit_job_locally(JobSpec) ->
    case is_cluster_enabled() of
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
    end.

%% @doc Check if federation routing is enabled for job submission.
-spec is_federation_routing_enabled() -> boolean().
is_federation_routing_enabled() ->
    %% Check config setting and whether federation server is available
    case application:get_env(flurm_controller, federation_routing, false) of
        true ->
            %% Config enabled, check if federation is actually active
            case catch flurm_federation:is_federated() of
                true -> true;
                _ -> false
            end;
        _ ->
            false
    end.

%% @doc Get local cluster name from federation or config.
-spec get_local_cluster_name() -> binary().
get_local_cluster_name() ->
    case catch flurm_federation:get_local_cluster() of
        Name when is_binary(Name) -> Name;
        _ ->
            %% Fallback to config or default
            case application:get_env(flurm_controller, cluster_name) of
                {ok, Name} when is_binary(Name) -> Name;
                {ok, Name} when is_list(Name) -> list_to_binary(Name);
                _ -> <<"local">>
            end
    end.

%% @doc Validate that the requested partition exists
-spec validate_partition(binary()) -> ok | {error, term()}.
validate_partition(PartitionName) ->
    case catch flurm_partition_manager:get_partition(PartitionName) of
        {ok, _} -> ok;
        {error, not_found} -> {error, {invalid_partition, PartitionName}};
        {'EXIT', _} -> ok  % Partition manager not running, allow submission
    end.

%%====================================================================
%% Internal Functions - Job Control Helpers
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

%% @doc Execute suspend or resume based on the suspend flag
-spec execute_suspend(non_neg_integer(), boolean()) -> ok | {error, term()}.
execute_suspend(JobId, true) ->
    flurm_job_manager:suspend_job(JobId);
execute_suspend(JobId, false) ->
    flurm_job_manager:resume_job(JobId).

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

%% @doc Execute job update with proper routing to job manager functions
%% Routes to hold_job/release_job/requeue_job for correct side effects
%% Priority=0 -> hold, Priority=non-NO_VAL and job held -> release, Requeue=1 -> requeue
-spec execute_job_update(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer(), binary()) -> ok | {error, term()}.
execute_job_update(JobId, Priority, TimeLimit, Requeue, Name) ->
    %% First handle requeue - it takes precedence
    case Requeue of
        1 ->
            %% Requeue the job - uses proper requeue_job function
            lager:info("Requeue requested for job ~p", [JobId]),
            flurm_job_manager:requeue_job(JobId);
        _ ->
            %% Build base updates from name and time_limit
            BaseUpdates = build_field_updates(TimeLimit, Name),

            %% Handle hold/release/priority update
            case Priority of
                0 ->
                    %% Priority 0 = hold the job
                    lager:info("Hold requested for job ~p", [JobId]),
                    %% Apply name/timelimit first if any, then hold
                    apply_base_updates(JobId, BaseUpdates),
                    flurm_job_manager:hold_job(JobId);
                16#FFFFFFFE ->
                    %% NO_VAL/INFINITE - check if job is held or running with priority=0
                    %% (scontrol release sends priority=INFINITE = NO_VAL in SLURM 22.05)
                    case flurm_job_manager:get_job(JobId) of
                        {ok, Job} when Job#job.state =:= held;
                                       (Job#job.state =:= running andalso Job#job.priority =:= 0) ->
                            %% Release the held/priority-0 job
                            lager:info("Release requested for job ~p (priority=NO_VAL, state=~p)", [JobId, Job#job.state]),
                            apply_base_updates(JobId, BaseUpdates),
                            flurm_job_manager:release_job(JobId);
                        _ ->
                            %% Not held, just apply base updates if any
                            case maps:size(BaseUpdates) of
                                0 -> ok;
                                _ -> flurm_job_manager:update_job(JobId, BaseUpdates)
                            end
                    end;
                P when P > 0 ->
                    %% Non-zero priority - check if job is held and release it
                    %% Otherwise just update the priority
                    case flurm_job_manager:get_job(JobId) of
                        {ok, Job} when Job#job.state =:= held ->
                            %% Release the held job
                            lager:info("Release requested for job ~p (priority ~p)", [JobId, P]),
                            apply_base_updates(JobId, BaseUpdates),
                            case flurm_job_manager:release_job(JobId) of
                                ok when P =/= 100 ->
                                    flurm_job_manager:update_job(JobId, #{priority => P});
                                Result -> Result
                            end;
                        {ok, _Job} ->
                            %% Not held, just update priority and other fields
                            Updates = maps:merge(BaseUpdates, #{priority => P}),
                            flurm_job_manager:update_job(JobId, Updates);
                        {error, _} = Error -> Error
                    end
            end
    end.

%% Build updates map from time_limit and name fields
%% SLURM sends time_limit in minutes; internally we store seconds
build_field_updates(TimeLimit, Name) ->
    U0 = #{},
    U1 = case TimeLimit of
        16#FFFFFFFE -> U0;
        T -> maps:put(time_limit, T * 60, U0)  % minutes -> seconds
    end,
    case Name of
        <<>> -> U1;
        _ -> maps:put(name, Name, U1)
    end.

%% Apply base updates if any exist (used before hold/release operations)
apply_base_updates(_JobId, Updates) when map_size(Updates) =:= 0 -> ok;
apply_base_updates(JobId, Updates) ->
    flurm_job_manager:update_job(JobId, Updates).
