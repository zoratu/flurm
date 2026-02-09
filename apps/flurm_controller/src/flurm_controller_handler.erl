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
%%% - REQUEST_FED_INFO (2049) -> queries federation status
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_handler).

%% Suppress warning for helper function reserved for future use
-compile([{nowarn_unused_function, [{format_fed_clusters, 1}]}]).

-export([handle/2]).

%% Exports for unit testing pure helper functions
-ifdef(TEST).
-export([
    %% Type conversion helpers
    ensure_binary/1,
    error_to_binary/1,
    default_time/1,
    %% Formatting helpers
    format_allocated_nodes/1,
    format_features/1,
    format_partitions/1,
    format_node_list/1,
    format_licenses/1,
    %% Job ID parsing
    parse_job_id_str/1,
    safe_binary_to_integer/1,
    build_job_updates/3,
    execute_job_update/4,
    %% State conversions
    job_state_to_slurm/1,
    node_state_to_slurm/1,
    partition_state_to_slurm/1,
    step_state_to_slurm/1,
    %% Default value helpers
    default_partition/1,
    default_time_limit/1,
    default_priority/1,
    default_work_dir/1,
    %% Reservation helpers
    reservation_state_to_flags/1,
    determine_reservation_type/2,
    parse_reservation_flags/1,
    generate_reservation_name/0,
    extract_reservation_fields/1
]).
-endif.

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
%% Supports federation routing when federation is enabled
handle(#slurm_header{msg_type = ?REQUEST_SUBMIT_BATCH_JOB},
       #batch_job_request{} = Request) ->
    lager:info("Handling batch job submission: ~s", [Request#batch_job_request.name]),
    JobSpec = batch_request_to_job_spec(Request),
    Result = submit_job_with_federation(JobSpec),
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

%% REQUEST_JOB_STEP_CREATE (5001) - handled later in file at the step operations section

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
    %% Log job details for debugging
    lists:foreach(fun(J) ->
        lager:info("Job record: id=~p, name=~p, state=~p",
                   [element(2, J), element(3, J), element(6, J)])
    end, Jobs),
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

%% NOTE: Message type 2049 is REQUEST_FED_INFO (federation info request)
%% Handler is defined later in this file with the other federation handlers

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

%% REQUEST_COMPLETE_PROLOG (5019) -> RESPONSE_SLURM_RC
%% Sent by slurmd when prolog completes
handle(#slurm_header{msg_type = ?REQUEST_COMPLETE_PROLOG},
       #complete_prolog_request{job_id = JobId, prolog_rc = PrologRc, node_name = NodeName}) ->
    lager:info("Prolog complete for job ~p on ~s, rc=~p", [JobId, NodeName, PrologRc]),
    Status = case PrologRc of
        0 -> complete;
        _ -> failed
    end,
    %% Update job prolog status
    case is_cluster_enabled() andalso not flurm_controller_cluster:is_leader() of
        true ->
            flurm_controller_cluster:forward_to_leader(update_prolog_status, {JobId, Status});
        false ->
            update_job_prolog_status(JobId, Status)
    end,
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response};

%% MESSAGE_EPILOG_COMPLETE (6012) -> RESPONSE_SLURM_RC
%% Sent by slurmd when epilog completes
handle(#slurm_header{msg_type = ?MESSAGE_EPILOG_COMPLETE},
       #epilog_complete_msg{job_id = JobId, epilog_rc = EpilogRc, node_name = NodeName}) ->
    lager:info("Epilog complete for job ~p on ~s, rc=~p", [JobId, NodeName, EpilogRc]),
    Status = case EpilogRc of
        0 -> complete;
        _ -> failed
    end,
    %% Update job epilog status
    case is_cluster_enabled() andalso not flurm_controller_cluster:is_leader() of
        true ->
            flurm_controller_cluster:forward_to_leader(update_epilog_status, {JobId, Status});
        false ->
            update_job_epilog_status(JobId, Status)
    end,
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response};

%% MESSAGE_TASK_EXIT (6003) -> RESPONSE_SLURM_RC
%% Sent by slurmd when a task exits (completes or fails)
handle(#slurm_header{msg_type = ?MESSAGE_TASK_EXIT},
       #task_exit_msg{job_id = JobId, step_id = StepId, return_code = ReturnCode,
                      task_ids = TaskIds, node_name = NodeName}) ->
    lager:info("Task exit for job ~p step ~p, tasks=~p, rc=~p, node=~s",
               [JobId, StepId, TaskIds, ReturnCode, NodeName]),
    %% Update step status in step manager
    %% If return code is 0, task completed successfully
    %% Otherwise, the step has a failed task
    case is_cluster_enabled() andalso not flurm_controller_cluster:is_leader() of
        true ->
            flurm_controller_cluster:forward_to_leader(complete_step, {JobId, StepId, ReturnCode});
        false ->
            %% Update step completion status
            case flurm_step_manager:complete_step(JobId, StepId, ReturnCode) of
                ok ->
                    lager:debug("Step ~p.~p marked as completed", [JobId, StepId]);
                {error, not_found} ->
                    %% Step may have been created by srun directly, not tracked
                    lager:debug("Step ~p.~p not found in step manager", [JobId, StepId])
            end
    end,
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response};

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
    Nodes = flurm_node_manager_server:list_nodes(),
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
%% Hot-reload configuration from slurm.conf (full reconfigure)
handle(#slurm_header{msg_type = ?REQUEST_RECONFIGURE}, Body) ->
    lager:info("Handling full reconfigure request (REQUEST_RECONFIGURE 1003)"),
    StartTime = erlang:system_time(millisecond),

    %% Parse the request body if it's a record
    _Flags = case Body of
        #reconfigure_request{flags = F} -> F;
        _ -> 0
    end,

    Result = case is_cluster_enabled() of
        true ->
            %% In cluster mode, only leader processes reconfigure
            case flurm_controller_cluster:is_leader() of
                true ->
                    lager:info("Processing reconfigure as cluster leader"),
                    do_reconfigure();
                false ->
                    %% Forward to leader
                    lager:info("Forwarding reconfigure request to cluster leader"),
                    case flurm_controller_cluster:forward_to_leader(reconfigure, []) of
                        {ok, ReconfigResult} -> ReconfigResult;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            do_reconfigure()
    end,

    ElapsedMs = erlang:system_time(millisecond) - StartTime,

    case Result of
        ok ->
            lager:info("Full reconfiguration completed successfully in ~p ms", [ElapsedMs]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {ok, ChangedKeys} when is_list(ChangedKeys) ->
            lager:info("Full reconfiguration completed successfully in ~p ms, ~p keys changed: ~p",
                      [ElapsedMs, length(ChangedKeys), ChangedKeys]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Full reconfiguration failed after ~p ms: ~p", [ElapsedMs, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_RECONFIGURE_WITH_CONFIG (1004) -> RESPONSE_SLURM_RC
%% Partial reconfiguration with specific settings
handle(#slurm_header{msg_type = ?REQUEST_RECONFIGURE_WITH_CONFIG}, Body) ->
    lager:info("Handling partial reconfigure request (REQUEST_RECONFIGURE_WITH_CONFIG 1004)"),
    StartTime = erlang:system_time(millisecond),

    %% Parse the request
    Request = case Body of
        #reconfigure_with_config_request{} = Req -> Req;
        _ -> #reconfigure_with_config_request{}
    end,

    ConfigFile = Request#reconfigure_with_config_request.config_file,
    Settings = Request#reconfigure_with_config_request.settings,
    Force = Request#reconfigure_with_config_request.force,
    NotifyNodes = Request#reconfigure_with_config_request.notify_nodes,

    lager:info("Partial reconfigure: config_file=~s, settings_count=~p, force=~p, notify_nodes=~p",
               [ConfigFile, maps:size(Settings), Force, NotifyNodes]),

    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true ->
                    lager:info("Processing partial reconfigure as cluster leader"),
                    do_partial_reconfigure(ConfigFile, Settings, Force, NotifyNodes);
                false ->
                    lager:info("Forwarding partial reconfigure request to cluster leader"),
                    case flurm_controller_cluster:forward_to_leader(
                           partial_reconfigure,
                           {ConfigFile, Settings, Force, NotifyNodes}) of
                        {ok, ReconfigResult} -> ReconfigResult;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            do_partial_reconfigure(ConfigFile, Settings, Force, NotifyNodes)
    end,

    ElapsedMs = erlang:system_time(millisecond) - StartTime,

    case Result of
        ok ->
            lager:info("Partial reconfiguration completed successfully in ~p ms", [ElapsedMs]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {ok, ChangedKeys} when is_list(ChangedKeys) ->
            lager:info("Partial reconfiguration completed in ~p ms, ~p keys changed: ~p",
                      [ElapsedMs, length(ChangedKeys), ChangedKeys]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, {validation_failed, Details}} ->
            lager:warning("Partial reconfiguration validation failed after ~p ms: ~p",
                         [ElapsedMs, Details]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Partial reconfiguration failed after ~p ms: ~p", [ElapsedMs, Reason2]),
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
            %% Get job info for credential generation
            {Uid, Gid, UserName, NodeList, Partition} = get_job_cred_info(JobId),
            NumTasks = max(1, Request#job_step_create_request.num_tasks),

            %% Dispatch the step to the node daemon for execution
            dispatch_step_to_nodes(JobId, StepId, Request),
            Response = #job_step_create_response{
                job_step_id = StepId,
                job_id = JobId,
                user_id = Uid,
                group_id = Gid,
                user_name = UserName,
                node_list = NodeList,
                num_tasks = NumTasks,
                partition = Partition,
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

    %% Route to appropriate job manager function for proper side effects
    %% Priority=0 means hold, Priority=non-zero+non-NO_VAL means release (restore priority)
    %% Requeue=1 means requeue the job
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> execute_job_update(JobId, Priority, TimeLimit, Requeue);
                false ->
                    case flurm_controller_cluster:forward_to_leader(update_job_ext, {JobId, Priority, TimeLimit, Requeue}) of
                        {ok, UpdateResult} -> UpdateResult;
                        {error, no_leader} -> {error, controller_not_found};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            execute_job_update(JobId, Priority, TimeLimit, Requeue)
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

%% REQUEST_CREATE_RESERVATION (2050) -> RESPONSE_CREATE_RESERVATION
%% scontrol create reservation
handle(#slurm_header{msg_type = ?REQUEST_CREATE_RESERVATION},
       #create_reservation_request{} = Request) ->
    lager:info("Handling create reservation request: ~s", [Request#create_reservation_request.name]),
    ResvSpec = create_reservation_request_to_spec(Request),
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> flurm_reservation:create(ResvSpec);
                false ->
                    case flurm_controller_cluster:forward_to_leader(create_reservation, ResvSpec) of
                        {ok, R} -> R;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_reservation:create(ResvSpec)
    end,
    case Result of
        {ok, ResvName} ->
            lager:info("Reservation ~s created successfully", [ResvName]),
            Response = #create_reservation_response{
                name = ResvName,
                error_code = 0,
                error_msg = <<"Reservation created successfully">>
            },
            {ok, ?RESPONSE_CREATE_RESERVATION, Response};
        {error, Reason2} ->
            lager:warning("Create reservation failed: ~p", [Reason2]),
            Response = #create_reservation_response{
                name = <<>>,
                error_code = 1,
                error_msg = error_to_binary(Reason2)
            },
            {ok, ?RESPONSE_CREATE_RESERVATION, Response}
    end;

%% REQUEST_UPDATE_RESERVATION (2052) -> RESPONSE_SLURM_RC
%% scontrol update reservation
handle(#slurm_header{msg_type = ?REQUEST_UPDATE_RESERVATION},
       #update_reservation_request{} = Request) ->
    Name = Request#update_reservation_request.name,
    lager:info("Handling update reservation request: ~s", [Name]),
    Updates = update_reservation_request_to_updates(Request),
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> flurm_reservation:update(Name, Updates);
                false ->
                    case flurm_controller_cluster:forward_to_leader(update_reservation, {Name, Updates}) of
                        {ok, R} -> R;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_reservation:update(Name, Updates)
    end,
    case Result of
        ok ->
            lager:info("Reservation ~s updated successfully", [Name]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Update reservation ~s failed: ~p", [Name, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_DELETE_RESERVATION (2053) -> RESPONSE_SLURM_RC
%% scontrol delete reservation
handle(#slurm_header{msg_type = ?REQUEST_DELETE_RESERVATION},
       #delete_reservation_request{name = Name}) ->
    lager:info("Handling delete reservation request: ~s", [Name]),
    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true -> flurm_reservation:delete(Name);
                false ->
                    case flurm_controller_cluster:forward_to_leader(delete_reservation, Name) of
                        {ok, R} -> R;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            flurm_reservation:delete(Name)
    end,
    case Result of
        ok ->
            lager:info("Reservation ~s deleted successfully", [Name]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Delete reservation ~s failed: ~p", [Name, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

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
    {ok, ?RESPONSE_UPDATE_FEDERATION, Response};

%% Unknown/unsupported message types
handle(#slurm_header{msg_type = MsgType}, _Body) ->
    TypeName = flurm_protocol_codec:message_type_name(MsgType),
    lager:warning("Unsupported message type: ~p (~p)", [MsgType, TypeName]),
    Response = #slurm_rc_response{return_code = -1},
    {ok, ?RESPONSE_SLURM_RC, Response}.

%%====================================================================
%% Internal Functions - Step Dispatch
%%====================================================================

%% @doc Dispatch a job step to the allocated node daemons for execution.
%% This is called after step creation to actually launch the tasks.
-spec dispatch_step_to_nodes(non_neg_integer(), non_neg_integer(), #job_step_create_request{}) -> ok.
dispatch_step_to_nodes(JobId, StepId, Request) ->
    lager:info("Dispatching step ~p.~p to node daemons", [JobId, StepId]),
    %% Get the job info to find allocated nodes
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} ->
            AllocatedNodes = Job#job.allocated_nodes,
            lager:info("Step ~p.~p: allocated nodes = ~p", [JobId, StepId, AllocatedNodes]),
            case AllocatedNodes of
                [] ->
                    lager:warning("No allocated nodes for job ~p, cannot dispatch step", [JobId]),
                    %% Notify srun of failure
                    flurm_srun_callback:notify_job_complete(JobId, 1, <<"No nodes allocated">>);
                Nodes ->
                    %% Build step launch message
                    StepLaunchMsg = build_step_launch_message(JobId, StepId, Request, Job),
                    %% Send to all allocated nodes
                    Results = flurm_node_connection_manager:send_to_nodes(Nodes, StepLaunchMsg),
                    lager:info("Step ~p.~p dispatch results: ~p", [JobId, StepId, Results]),
                    %% Check for failures
                    case lists:all(fun({_, ok}) -> true; (_) -> false end, Results) of
                        true ->
                            lager:info("Step ~p.~p dispatched successfully to ~p nodes",
                                       [JobId, StepId, length(Nodes)]);
                        false ->
                            FailedNodes = [N || {N, R} <- Results, R =/= ok],
                            lager:warning("Step ~p.~p failed to dispatch to some nodes: ~p",
                                          [JobId, StepId, FailedNodes])
                    end
            end;
        {error, not_found} ->
            lager:error("Job ~p not found when dispatching step ~p", [JobId, StepId]),
            flurm_srun_callback:notify_job_complete(JobId, 1, <<"Job not found">>)
    end,
    ok.

%% @doc Build the step launch message to send to node daemons.
-spec build_step_launch_message(non_neg_integer(), non_neg_integer(), #job_step_create_request{}, #job{}) -> map().
build_step_launch_message(JobId, StepId, Request, Job) ->
    %% Get the command from the job (for srun, the command is in the request name or the job script)
    Command = case Request#job_step_create_request.name of
        <<>> -> <<"hostname">>;  % Default command
        Name -> Name
    end,
    #{
        type => step_launch,
        payload => #{
            <<"job_id">> => JobId,
            <<"step_id">> => StepId,
            <<"command">> => Command,
            <<"num_tasks">> => max(1, Request#job_step_create_request.num_tasks),
            <<"cpus_per_task">> => max(1, Request#job_step_create_request.cpus_per_task),
            <<"time_limit">> => Request#job_step_create_request.time_limit,
            <<"working_dir">> => Job#job.work_dir,
            <<"environment">> => #{
                <<"SLURM_JOB_ID">> => integer_to_binary(JobId),
                <<"SLURM_STEP_ID">> => integer_to_binary(StepId),
                <<"SLURM_JOB_NAME">> => Job#job.name,
                <<"SLURM_NTASKS">> => integer_to_binary(max(1, Request#job_step_create_request.num_tasks)),
                <<"SLURM_CPUS_PER_TASK">> => integer_to_binary(max(1, Request#job_step_create_request.cpus_per_task))
            }
        }
    }.

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
        dependency => Req#batch_job_request.dependency
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
        account = Job#job.account,
        accrue_time = 0,
        admin_comment = <<>>,
        alloc_node = <<>>,
        alloc_sid = 0,
        array_job_id = 0,
        array_max_tasks = 0,
        array_task_id = 16#FFFFFFFE,  % NO_VAL for non-array jobs
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
        min_mem_per_node = 0,
        network = undefined,
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
        std_err = undefined,
        std_in = undefined,
        std_out = undefined,
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

%% @doc Get first registered node name from node manager
%% Returns the name of the first available node, or <<"unknown">> if none
-spec get_first_registered_node() -> binary().
get_first_registered_node() ->
    try
        case flurm_node_manager_server:list_nodes() of
            Nodes when is_list(Nodes), length(Nodes) > 0 ->
                %% Get the first node - nodes are records or maps
                Node = hd(Nodes),
                NodeName = case Node of
                    N when is_map(N) -> maps:get(name, N, maps:get(node_name, N, <<"unknown">>));
                    N when is_tuple(N) ->
                        %% Assume it's a record with name as second element
                        case element(2, N) of
                            Name when is_binary(Name) -> Name;
                            Name when is_atom(Name) -> atom_to_binary(Name, utf8);
                            _ -> <<"unknown">>
                        end;
                    N when is_binary(N) -> N;
                    N when is_atom(N) -> atom_to_binary(N, utf8);
                    _ -> <<"unknown">>
                end,
                ensure_binary(NodeName);
            _ ->
                <<"unknown">>
        end
    catch
        _:_ -> <<"unknown">>
    end.

%% @doc Get job credential info for step creation
%% Returns {Uid, Gid, UserName, NodeList, Partition}
-spec get_job_cred_info(non_neg_integer()) -> {non_neg_integer(), non_neg_integer(), binary(), binary(), binary()}.
get_job_cred_info(JobId) ->
    case flurm_job_manager:get_job(JobId) of
        {ok, Job} when is_record(Job, job) ->
            %% Job is a #job{} record from flurm_core.hrl
            UserName = Job#job.user,
            Partition = Job#job.partition,
            %% Get allocated nodes or query node manager for available nodes
            NodeList = case Job#job.allocated_nodes of
                [] -> get_first_registered_node();
                undefined -> get_first_registered_node();
                [Node | _] when is_binary(Node) -> Node;
                Nodes when is_list(Nodes) ->
                    NodeBins = [ensure_binary(N) || N <- Nodes],
                    iolist_to_binary(lists:join(<<",">>, NodeBins))
            end,
            %% The job record doesn't store uid/gid, use defaults
            %% In a real system, we'd look up the user's UID/GID
            Uid = 0,
            Gid = 0,
            {Uid, Gid, ensure_binary(UserName), ensure_binary(NodeList), ensure_binary(Partition)};
        {ok, Job} when is_map(Job) ->
            %% Handle map format for backwards compatibility
            Uid = maps:get(user_id, Job, 0),
            Gid = maps:get(group_id, Job, 0),
            UserName = maps:get(user_name, Job, maps:get(user, Job, <<"root">>)),
            NodeList = case maps:get(allocated_nodes, Job, []) of
                [] -> get_first_registered_node();
                [Node | _] when is_binary(Node) -> Node;
                Nodes when is_list(Nodes) ->
                    NodeBins = [ensure_binary(N) || N <- Nodes],
                    iolist_to_binary(lists:join(<<",">>, NodeBins))
            end,
            Partition = maps:get(partition, Job, <<"default">>),
            {Uid, Gid, ensure_binary(UserName), ensure_binary(NodeList), ensure_binary(Partition)};
        {error, _} ->
            %% Job not found, use defaults with actual node name
            lager:warning("Job ~p not found for credential info, using defaults", [JobId]),
            {0, 0, <<"root">>, get_first_registered_node(), <<"default">>}
    end.

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

%% @doc Format licenses list to SLURM format (name:count,name:count)
-spec format_licenses([{binary(), non_neg_integer()}]) -> binary().
format_licenses([]) -> <<>>;
format_licenses(Licenses) ->
    Formatted = lists:map(fun({Name, Count}) ->
        iolist_to_binary([Name, <<":">>, integer_to_binary(Count)])
    end, Licenses),
    iolist_to_binary(lists:join(<<",">>, Formatted)).

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

%% @doc Execute suspend or resume based on the suspend flag
-spec execute_suspend(non_neg_integer(), boolean()) -> ok | {error, term()}.
execute_suspend(JobId, true) ->
    flurm_job_manager:suspend_job(JobId);
execute_suspend(JobId, false) ->
    flurm_job_manager:resume_job(JobId).

%% @doc Execute job update with proper routing to job manager functions
%% Routes to hold_job/release_job/requeue_job for correct side effects
%% Priority=0 -> hold, Priority=non-NO_VAL and job held -> release, Requeue=1 -> requeue
-spec execute_job_update(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
execute_job_update(JobId, Priority, TimeLimit, Requeue) ->
    %% First handle requeue - it takes precedence
    case Requeue of
        1 ->
            %% Requeue the job - uses proper requeue_job function
            lager:info("Requeue requested for job ~p", [JobId]),
            flurm_job_manager:requeue_job(JobId);
        _ ->
            %% Handle hold/release/priority update
            case Priority of
                0 ->
                    %% Priority 0 = hold the job
                    lager:info("Hold requested for job ~p", [JobId]),
                    flurm_job_manager:hold_job(JobId);
                16#FFFFFFFE ->
                    %% NO_VAL - no priority change, just apply time_limit if set
                    case TimeLimit of
                        16#FFFFFFFE -> ok;  % Nothing to update
                        T ->
                            flurm_job_manager:update_job(JobId, #{time_limit => T})
                    end;
                P when P > 0 ->
                    %% Non-zero priority - check if job is held and release it
                    %% Otherwise just update the priority
                    case flurm_job_manager:get_job(JobId) of
                        {ok, Job} when Job#job.state =:= held ->
                            %% Release the held job
                            lager:info("Release requested for job ~p (priority ~p)", [JobId, P]),
                            case flurm_job_manager:release_job(JobId) of
                                ok when P =/= 100 ->
                                    %% Also update priority if not default
                                    flurm_job_manager:update_job(JobId, #{priority => P});
                                Result -> Result
                            end;
                        {ok, _Job} ->
                            %% Not held, just update priority
                            Updates = build_job_updates(Priority, TimeLimit, 16#FFFFFFFE),
                            flurm_job_manager:update_job(JobId, Updates);
                        {error, _} = Error -> Error
                    end
            end
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
-spec do_reconfigure() -> ok | {ok, [atom()]} | {error, term()}.
do_reconfigure() ->
    lager:info("Starting full hot reconfiguration"),

    %% Get current config version before reload
    OldVersion = try flurm_config_server:get_version() catch _:_ -> 0 end,

    %% Step 1: Reload configuration from file
    case flurm_config_server:reconfigure() of
        ok ->
            NewVersion = try flurm_config_server:get_version() catch _:_ -> 0 end,
            lager:info("Configuration reloaded from file (version ~p -> ~p)",
                      [OldVersion, NewVersion]),

            %% Step 2: Apply node changes
            lager:debug("Applying node configuration changes"),
            apply_node_changes(),

            %% Step 3: Apply partition changes
            lager:debug("Applying partition configuration changes"),
            apply_partition_changes(),

            %% Step 4: Notify scheduler to refresh
            lager:debug("Triggering scheduler refresh"),
            notify_scheduler_reconfigure(),

            %% Step 5: Broadcast to compute nodes if needed
            broadcast_reconfigure_to_nodes(),

            lager:info("Full reconfiguration complete"),
            ok;
        {error, Reason} ->
            lager:error("Failed to reload configuration: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Perform partial reconfiguration with specific settings
%% This allows updating specific configuration values without a full reload.
-spec do_partial_reconfigure(binary(), map(), boolean(), boolean()) ->
    ok | {ok, [atom()]} | {error, term()}.
do_partial_reconfigure(ConfigFile, Settings, Force, NotifyNodes) ->
    lager:info("Starting partial reconfiguration"),

    %% Get current config version
    OldVersion = try flurm_config_server:get_version() catch _:_ -> 0 end,

    %% Step 1: If a config file is specified, reload from that file first
    FileResult = case ConfigFile of
        <<>> ->
            ok;
        _ ->
            lager:info("Reloading configuration from specified file: ~s", [ConfigFile]),
            case flurm_config_server:reconfigure(binary_to_list(ConfigFile)) of
                ok -> ok;
                {error, R} when Force ->
                    lager:warning("Config file reload failed but force=true, continuing: ~p", [R]),
                    ok;
                {error, R} ->
                    {error, {file_reload_failed, R}}
            end
    end,

    case FileResult of
        ok ->
            %% Step 2: Apply specific settings
            ChangedKeys = apply_settings(Settings, Force),
            lager:info("Applied ~p configuration settings: ~p",
                      [length(ChangedKeys), ChangedKeys]),

            %% Step 3: Apply node and partition changes based on what was changed
            case lists:member(nodes, ChangedKeys) of
                true ->
                    lager:debug("Nodes configuration changed, applying updates"),
                    apply_node_changes();
                false ->
                    ok
            end,

            case lists:member(partitions, ChangedKeys) of
                true ->
                    lager:debug("Partitions configuration changed, applying updates"),
                    apply_partition_changes();
                false ->
                    ok
            end,

            %% Step 4: Notify scheduler if scheduling-related settings changed
            SchedulerKeys = [schedulertype, schedulerparameters, prioritytype,
                            priorityweightage, priorityweightfairshare,
                            priorityweightjobsize, priorityweightpartition,
                            priorityweightqos],
            case lists:any(fun(K) -> lists:member(K, ChangedKeys) end, SchedulerKeys) of
                true ->
                    lager:info("Scheduler-related settings changed, refreshing scheduler"),
                    notify_scheduler_reconfigure();
                false ->
                    ok
            end,

            %% Step 5: Notify compute nodes if requested
            case NotifyNodes of
                true ->
                    broadcast_reconfigure_to_nodes();
                false ->
                    lager:debug("Skipping node notification (notify_nodes=false)")
            end,

            NewVersion = try flurm_config_server:get_version() catch _:_ -> 0 end,
            lager:info("Partial reconfiguration complete (version ~p -> ~p)",
                      [OldVersion, NewVersion]),
            {ok, ChangedKeys};
        {error, _} = Error ->
            Error
    end.

%% @doc Apply specific settings to the configuration
-spec apply_settings(map(), boolean()) -> [atom()].
apply_settings(Settings, _Force) when map_size(Settings) == 0 ->
    [];
apply_settings(Settings, Force) ->
    ChangedKeys = maps:fold(fun(Key, Value, Acc) ->
        try
            %% Get current value to check if it's actually changing
            CurrentValue = flurm_config_server:get(Key, undefined),
            case CurrentValue =:= Value of
                true ->
                    lager:debug("Setting ~p unchanged (value=~p)", [Key, Value]),
                    Acc;
                false ->
                    lager:info("Updating setting ~p: ~p -> ~p", [Key, CurrentValue, Value]),
                    flurm_config_server:set(Key, Value),
                    [Key | Acc]
            end
        catch
            Error:Reason ->
                case Force of
                    true ->
                        lager:warning("Failed to set ~p=~p, but force=true: ~p:~p",
                                     [Key, Value, Error, Reason]),
                        Acc;
                    false ->
                        lager:error("Failed to set ~p=~p: ~p:~p",
                                   [Key, Value, Error, Reason]),
                        Acc
                end
        end
    end, [], Settings),
    lists:reverse(ChangedKeys).

%% @doc Notify the scheduler of configuration changes
-spec notify_scheduler_reconfigure() -> ok.
notify_scheduler_reconfigure() ->
    try
        flurm_scheduler:trigger_schedule(),
        lager:debug("Scheduler notified of reconfiguration")
    catch
        _:_ ->
            lager:debug("Could not notify scheduler (may not be running)")
    end,
    ok.

%% @doc Broadcast reconfiguration message to all compute nodes
-spec broadcast_reconfigure_to_nodes() -> ok.
broadcast_reconfigure_to_nodes() ->
    try
        Nodes = flurm_node_manager_server:list_nodes(),
        lager:info("Broadcasting reconfigure to ~p compute nodes", [length(Nodes)]),
        lists:foreach(fun(Node) ->
            NodeName = Node#node.hostname,
            try
                %% Send reconfigure notification to the node
                %% The node daemon will reload its local configuration
                case flurm_node_manager_server:send_command(NodeName, reconfigure) of
                    ok ->
                        lager:debug("Reconfigure sent to node ~s", [NodeName]);
                    {error, Reason} ->
                        lager:warning("Failed to send reconfigure to node ~s: ~p",
                                     [NodeName, Reason])
                end
            catch
                _:NodeError ->
                    lager:warning("Error notifying node ~s: ~p", [NodeName, NodeError])
            end
        end, Nodes)
    catch
        _:_ ->
            lager:debug("Could not broadcast to nodes (node manager may not be running)")
    end,
    ok.

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
    case flurm_node_manager_server:get_node(NodeName) of
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

%%====================================================================
%% Internal Functions - Reservation Management
%%====================================================================

%% @doc Convert create reservation request to spec for flurm_reservation:create/1
-spec create_reservation_request_to_spec(#create_reservation_request{}) -> map().
create_reservation_request_to_spec(#create_reservation_request{} = Req) ->
    Now = erlang:system_time(second),

    %% Determine start time
    StartTime = case Req#create_reservation_request.start_time of
        0 -> Now;  % Start now
        S -> S
    end,

    %% Determine end time (from end_time or duration)
    EndTime = case Req#create_reservation_request.end_time of
        0 ->
            %% No end time specified, use duration
            Duration = case Req#create_reservation_request.duration of
                0 -> 60;  % Default 1 hour
                D -> D
            end,
            StartTime + (Duration * 60);  % Convert minutes to seconds
        E -> E
    end,

    %% Parse node list
    Nodes = case Req#create_reservation_request.nodes of
        <<>> -> [];
        NodeList ->
            %% Expand hostlist pattern if needed
            try flurm_config_slurm:expand_hostlist(NodeList)
            catch _:_ -> binary:split(NodeList, <<",">>, [global])
            end
    end,

    %% Parse users list
    Users = case Req#create_reservation_request.users of
        <<>> -> [];
        UserList -> binary:split(UserList, <<",">>, [global])
    end,

    %% Parse accounts list
    Accounts = case Req#create_reservation_request.accounts of
        <<>> -> [];
        AccountList -> binary:split(AccountList, <<",">>, [global])
    end,

    %% Determine reservation type from flags or type field
    Type = determine_reservation_type(
        Req#create_reservation_request.type,
        Req#create_reservation_request.flags
    ),

    %% Build flags list from flags integer
    Flags = parse_reservation_flags(Req#create_reservation_request.flags),

    #{
        name => case Req#create_reservation_request.name of
            <<>> -> generate_reservation_name();
            N -> N
        end,
        type => Type,
        start_time => StartTime,
        end_time => EndTime,
        nodes => Nodes,
        node_count => case Req#create_reservation_request.node_cnt of
            0 -> length(Nodes);
            NC -> NC
        end,
        partition => Req#create_reservation_request.partition,
        users => Users,
        accounts => Accounts,
        features => case Req#create_reservation_request.features of
            <<>> -> [];
            F -> binary:split(F, <<",">>, [global])
        end,
        flags => Flags,
        state => case StartTime =< Now of
            true -> active;
            false -> inactive
        end
    }.

%% @doc Convert update reservation request to updates map for flurm_reservation:update/2
-spec update_reservation_request_to_updates(#update_reservation_request{}) -> map().
update_reservation_request_to_updates(#update_reservation_request{} = Req) ->
    Updates0 = #{},

    %% Only include fields that are actually set (non-zero, non-empty)
    Updates1 = case Req#update_reservation_request.start_time of
        0 -> Updates0;
        S -> maps:put(start_time, S, Updates0)
    end,

    Updates2 = case Req#update_reservation_request.end_time of
        0 -> Updates1;
        E -> maps:put(end_time, E, Updates1)
    end,

    Updates3 = case Req#update_reservation_request.duration of
        0 -> Updates2;
        D -> maps:put(duration, D * 60, Updates2)  % Convert minutes to seconds
    end,

    Updates4 = case Req#update_reservation_request.nodes of
        <<>> -> Updates3;
        NodeList ->
            Nodes = try flurm_config_slurm:expand_hostlist(NodeList)
                    catch _:_ -> binary:split(NodeList, <<",">>, [global])
                    end,
            maps:put(nodes, Nodes, Updates3)
    end,

    Updates5 = case Req#update_reservation_request.users of
        <<>> -> Updates4;
        UserList ->
            Users = binary:split(UserList, <<",">>, [global]),
            maps:put(users, Users, Updates4)
    end,

    Updates6 = case Req#update_reservation_request.accounts of
        <<>> -> Updates5;
        AccountList ->
            Accounts = binary:split(AccountList, <<",">>, [global]),
            maps:put(accounts, Accounts, Updates5)
    end,

    Updates7 = case Req#update_reservation_request.partition of
        <<>> -> Updates6;
        P -> maps:put(partition, P, Updates6)
    end,

    Updates8 = case Req#update_reservation_request.flags of
        0 -> Updates7;
        F -> maps:put(flags, parse_reservation_flags(F), Updates7)
    end,

    Updates8.

%% @doc Determine reservation type from type string or flags
-spec determine_reservation_type(binary(), non_neg_integer()) -> atom().
determine_reservation_type(<<"maint">>, _) -> maint;
determine_reservation_type(<<"MAINT">>, _) -> maint;
determine_reservation_type(<<"maintenance">>, _) -> maintenance;
determine_reservation_type(<<"flex">>, _) -> flex;
determine_reservation_type(<<"FLEX">>, _) -> flex;
determine_reservation_type(<<"user">>, _) -> user;
determine_reservation_type(<<"USER">>, _) -> user;
determine_reservation_type(<<>>, Flags) ->
    %% Infer from flags
    %% SLURM reservation flags:
    %% RESERVE_FLAG_MAINT = 0x0001
    %% RESERVE_FLAG_FLEX = 0x8000
    case Flags band 16#0001 of
        0 ->
            case Flags band 16#8000 of
                0 -> user;
                _ -> flex
            end;
        _ -> maint
    end;
determine_reservation_type(_, _) -> user.

%% @doc Parse reservation flags integer to list of atoms
-spec parse_reservation_flags(non_neg_integer()) -> [atom()].
parse_reservation_flags(0) -> [];
parse_reservation_flags(Flags) ->
    %% SLURM reservation flag definitions
    FlagDefs = [
        {16#0001, maint},
        {16#0002, ignore_jobs},
        {16#0004, daily},
        {16#0008, weekly},
        {16#0010, weekday},
        {16#0020, weekend},
        {16#0040, any},
        {16#0080, first_cores},
        {16#0100, time_float},
        {16#0200, purge_comp},
        {16#0400, part_nodes},
        {16#0800, overlap},
        {16#1000, no_hold_jobs_after},
        {16#2000, static_alloc},
        {16#4000, no_hold_jobs},
        {16#8000, flex}
    ],
    lists:foldl(fun({Mask, Flag}, Acc) ->
        case Flags band Mask of
            0 -> Acc;
            _ -> [Flag | Acc]
        end
    end, [], FlagDefs).

%% @doc Generate a unique reservation name
-spec generate_reservation_name() -> binary().
generate_reservation_name() ->
    Timestamp = erlang:system_time(microsecond),
    Random = rand:uniform(9999),
    iolist_to_binary(io_lib:format("resv_~p_~4..0B", [Timestamp, Random])).

%%====================================================================
%% Internal Functions - Prolog/Epilog Status Updates
%%====================================================================

%% @doc Update job prolog status
%% Called when a node reports prolog completion
-spec update_job_prolog_status(non_neg_integer(), atom()) -> ok.
update_job_prolog_status(JobId, Status) ->
    case flurm_job_manager:update_prolog_status(JobId, Status) of
        ok ->
            lager:debug("Updated prolog status for job ~p to ~p", [JobId, Status]),
            ok;
        {error, Reason} ->
            lager:warning("Failed to update prolog status for job ~p: ~p", [JobId, Reason]),
            ok
    end.

%% @doc Update job epilog status
%% Called when a node reports epilog completion
-spec update_job_epilog_status(non_neg_integer(), atom()) -> ok.
update_job_epilog_status(JobId, Status) ->
    case flurm_job_manager:update_epilog_status(JobId, Status) of
        ok ->
            lager:debug("Updated epilog status for job ~p to ~p", [JobId, Status]),
            ok;
        {error, Reason} ->
            lager:warning("Failed to update epilog status for job ~p: ~p", [JobId, Reason]),
            ok
    end.

