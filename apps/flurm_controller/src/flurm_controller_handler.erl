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

%% REQUEST_JOB_INFO (2003) -> RESPONSE_JOB_INFO
handle(#slurm_header{msg_type = ?REQUEST_JOB_INFO},
       #job_info_request{job_id = JobId}) ->
    lager:debug("Handling job info request for job_id=~p", [JobId]),
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
    JobInfoList = [job_to_job_info(J) || J <- Jobs],
    Response = #job_info_response{
        last_update = erlang:system_time(second),
        job_count = length(JobInfoList),
        jobs = JobInfoList
    },
    {ok, ?RESPONSE_JOB_INFO, Response};

%% REQUEST_JOB_INFO_SINGLE (2005) -> RESPONSE_JOB_INFO
handle(#slurm_header{msg_type = ?REQUEST_JOB_INFO_SINGLE}, Body) ->
    %% Delegate to standard job info handler
    handle(#slurm_header{msg_type = ?REQUEST_JOB_INFO}, Body);

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
    %% Return version info as a return code response for now
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response};

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
        memory_mb => 1024,  % Default, could be derived from request
        time_limit => default_time_limit(Req#batch_job_request.time_limit),
        priority => default_priority(Req#batch_job_request.priority),
        user_id => Req#batch_job_request.user_id,
        group_id => Req#batch_job_request.group_id,
        work_dir => Req#batch_job_request.work_dir,
        account => Req#batch_job_request.account
    }.

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
-spec job_to_job_info(#job{}) -> #job_info{}.
job_to_job_info(#job{} = Job) ->
    #job_info{
        job_id = Job#job.id,
        name = ensure_binary(Job#job.name),
        user_id = 0,  % Would need user lookup
        group_id = 0,
        job_state = job_state_to_slurm(Job#job.state),
        partition = ensure_binary(Job#job.partition),
        num_nodes = Job#job.num_nodes,
        num_cpus = Job#job.num_cpus,
        num_tasks = 1,
        time_limit = Job#job.time_limit,
        priority = Job#job.priority,
        submit_time = Job#job.submit_time,
        start_time = default_time(Job#job.start_time),
        end_time = default_time(Job#job.end_time),
        nodes = format_allocated_nodes(Job#job.allocated_nodes),
        account = <<>>,
        accrue_time = 0,
        admin_comment = <<>>,
        alloc_node = <<>>,
        alloc_sid = 0
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
    #partition_info{
        name = Part#partition.name,
        state_up = partition_state_to_slurm(Part#partition.state),
        max_time = Part#partition.max_time,
        default_time = Part#partition.default_time,
        max_nodes = Part#partition.max_nodes,
        min_nodes = 1,
        total_nodes = length(Part#partition.nodes),
        total_cpus = 0,  % Would need node lookup to calculate
        nodes = format_node_list(Part#partition.nodes),
        priority_tier = Part#partition.priority,
        priority_job_factor = 1
    }.

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
-spec partition_state_to_slurm(partition_state()) -> non_neg_integer().
partition_state_to_slurm(up) -> 1;
partition_state_to_slurm(down) -> 0;
partition_state_to_slurm(drain) -> 0;
partition_state_to_slurm(inactive) -> 0;
partition_state_to_slurm(_) -> 0.

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
