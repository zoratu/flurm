%%%-------------------------------------------------------------------
%%% @doc FLURM Database Ra State Machine
%%%
%%% Implements the ra_machine behaviour for distributed consensus.
%%% This module replicates job, node, and partition state across
%%% all controller nodes using the Raft consensus algorithm.
%%%
%%% Commands are replicated to all nodes before being applied.
%%% Queries can be performed locally (stale) or through the leader
%%% (consistent).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra).
-behaviour(ra_machine).

-include("flurm_db.hrl").

%% Ra machine callbacks
-export([init/1, apply/3, state_enter/2, snapshot_module/0]).

%% API - Commands (replicated)
-export([
    start_cluster/1,
    submit_job/1,
    cancel_job/1,
    update_job_state/2,
    register_node/1,
    update_node_state/2,
    unregister_node/1,
    create_partition/1,
    delete_partition/1,
    allocate_job/2,
    set_job_exit_code/2,
    allocate_job_id/0
]).

%% API - Queries (local read)
-export([
    get_job/1,
    get_node/1,
    get_partition/1,
    list_jobs/0,
    list_nodes/0,
    list_partitions/0,
    get_jobs_by_state/1,
    get_nodes_by_state/1,
    get_nodes_in_partition/1
]).

%% API - Consistent queries (through leader)
-export([
    consistent_get_job/1,
    consistent_list_jobs/0
]).

%% Internal exports for testing
-export([
    make_job_record/2,
    make_node_record/1,
    make_partition_record/1
]).

%% DEFAULT_PRIORITY comes from flurm_core.hrl via flurm_db.hrl
-define(CLUSTER_NAME, ?RA_CLUSTER_NAME).

%%====================================================================
%% Ra Machine Callbacks
%%====================================================================

%% @doc Initialize the Ra state machine.
%% Called when the Ra server starts.
-spec init(term()) -> #ra_state{}.
init(_Config) ->
    #ra_state{
        jobs = #{},
        nodes = #{},
        partitions = #{},
        job_counter = 1,
        version = 1
    }.

%% @doc Apply a command to the state machine.
%% This is called after the command has been replicated to a quorum.
%% Always returns a 3-tuple for consistency: {State, Result, Effects}
-spec apply(ra_machine:command_meta_data(), term(), #ra_state{}) ->
    {#ra_state{}, term(), ra_machine:effects()}.

%% Allocate a job ID (for distributed job ID generation)
apply(_Meta, allocate_job_id, State) ->
    JobId = State#ra_state.job_counter,
    NewState = State#ra_state{
        job_counter = JobId + 1
    },
    %% No effects - just return the allocated ID
    {NewState, {ok, JobId}, []};

%% Submit a new job
apply(_Meta, {submit_job, #ra_job_spec{} = JobSpec}, State) ->
    JobId = State#ra_state.job_counter,
    Job = make_job_record(JobId, JobSpec),
    NewState = State#ra_state{
        jobs = maps:put(JobId, Job, State#ra_state.jobs),
        job_counter = JobId + 1
    },
    %% Effect: notify job manager on leader that job was submitted
    Effects = [{mod_call, flurm_db_ra_effects, job_submitted, [Job]}],
    {NewState, {ok, JobId}, Effects};

%% Cancel a job
apply(_Meta, {cancel_job, JobId}, State) ->
    case maps:find(JobId, State#ra_state.jobs) of
        {ok, Job} ->
            case Job#ra_job.state of
                S when S =:= completed; S =:= cancelled; S =:= failed ->
                    {State, {error, already_terminal}, []};
                _ ->
                    Now = erlang:system_time(second),
                    UpdatedJob = Job#ra_job{
                        state = cancelled,
                        end_time = Now
                    },
                    NewState = State#ra_state{
                        jobs = maps:put(JobId, UpdatedJob, State#ra_state.jobs)
                    },
                    Effects = [{mod_call, flurm_db_ra_effects, job_cancelled, [UpdatedJob]}],
                    {NewState, ok, Effects}
            end;
        error ->
            {State, {error, not_found}, []}
    end;

%% Update job state
apply(_Meta, {update_job_state, JobId, NewJobState}, State) ->
    case maps:find(JobId, State#ra_state.jobs) of
        {ok, Job} ->
            UpdatedJob = update_job_state_record(Job, NewJobState),
            NewState = State#ra_state{
                jobs = maps:put(JobId, UpdatedJob, State#ra_state.jobs)
            },
            Effects = [{mod_call, flurm_db_ra_effects, job_state_changed,
                       [JobId, Job#ra_job.state, NewJobState]}],
            {NewState, ok, Effects};
        error ->
            {State, {error, not_found}, []}
    end;

%% Allocate nodes to a job
apply(_Meta, {allocate_job, JobId, Nodes}, State) when is_list(Nodes) ->
    case maps:find(JobId, State#ra_state.jobs) of
        {ok, Job} ->
            case Job#ra_job.state of
                pending ->
                    Now = erlang:system_time(second),
                    UpdatedJob = Job#ra_job{
                        state = configuring,
                        allocated_nodes = Nodes,
                        start_time = Now
                    },
                    NewState = State#ra_state{
                        jobs = maps:put(JobId, UpdatedJob, State#ra_state.jobs)
                    },
                    Effects = [{mod_call, flurm_db_ra_effects, job_allocated,
                               [UpdatedJob, Nodes]}],
                    {NewState, ok, Effects};
                Other ->
                    {State, {error, {invalid_state, Other}}, []}
            end;
        error ->
            {State, {error, not_found}, []}
    end;

%% Set job exit code
apply(_Meta, {set_job_exit_code, JobId, ExitCode}, State) ->
    case maps:find(JobId, State#ra_state.jobs) of
        {ok, Job} ->
            Now = erlang:system_time(second),
            FinalState = case ExitCode of
                0 -> completed;
                _ -> failed
            end,
            UpdatedJob = Job#ra_job{
                state = FinalState,
                exit_code = ExitCode,
                end_time = Now
            },
            NewState = State#ra_state{
                jobs = maps:put(JobId, UpdatedJob, State#ra_state.jobs)
            },
            Effects = [{mod_call, flurm_db_ra_effects, job_completed,
                       [UpdatedJob, ExitCode]}],
            {NewState, ok, Effects};
        error ->
            {State, {error, not_found}, []}
    end;

%% Register a node
apply(_Meta, {register_node, #ra_node_spec{} = NodeSpec}, State) ->
    NodeName = NodeSpec#ra_node_spec.name,
    case maps:is_key(NodeName, State#ra_state.nodes) of
        true ->
            %% Node already exists, update it
            Node = make_node_record(NodeSpec),
            NewState = State#ra_state{
                nodes = maps:put(NodeName, Node, State#ra_state.nodes)
            },
            Effects = [{mod_call, flurm_db_ra_effects, node_updated, [Node]}],
            {NewState, {ok, updated}, Effects};
        false ->
            Node = make_node_record(NodeSpec),
            NewState = State#ra_state{
                nodes = maps:put(NodeName, Node, State#ra_state.nodes)
            },
            Effects = [{mod_call, flurm_db_ra_effects, node_registered, [Node]}],
            {NewState, {ok, registered}, Effects}
    end;

%% Update node state
apply(_Meta, {update_node_state, NodeName, NewNodeState}, State) ->
    case maps:find(NodeName, State#ra_state.nodes) of
        {ok, Node} ->
            Now = erlang:system_time(second),
            UpdatedNode = Node#ra_node{
                state = NewNodeState,
                last_heartbeat = Now
            },
            NewState = State#ra_state{
                nodes = maps:put(NodeName, UpdatedNode, State#ra_state.nodes)
            },
            Effects = [{mod_call, flurm_db_ra_effects, node_state_changed,
                       [NodeName, Node#ra_node.state, NewNodeState]}],
            {NewState, ok, Effects};
        error ->
            {State, {error, not_found}, []}
    end;

%% Unregister a node
apply(_Meta, {unregister_node, NodeName}, State) ->
    case maps:find(NodeName, State#ra_state.nodes) of
        {ok, Node} ->
            NewState = State#ra_state{
                nodes = maps:remove(NodeName, State#ra_state.nodes)
            },
            Effects = [{mod_call, flurm_db_ra_effects, node_unregistered, [Node]}],
            {NewState, ok, Effects};
        error ->
            {State, {error, not_found}, []}
    end;

%% Create a partition
apply(_Meta, {create_partition, #ra_partition_spec{} = PartSpec}, State) ->
    PartName = PartSpec#ra_partition_spec.name,
    case maps:is_key(PartName, State#ra_state.partitions) of
        true ->
            {State, {error, already_exists}, []};
        false ->
            Partition = make_partition_record(PartSpec),
            NewState = State#ra_state{
                partitions = maps:put(PartName, Partition, State#ra_state.partitions)
            },
            Effects = [{mod_call, flurm_db_ra_effects, partition_created, [Partition]}],
            {NewState, ok, Effects}
    end;

%% Delete a partition
apply(_Meta, {delete_partition, PartName}, State) ->
    case maps:find(PartName, State#ra_state.partitions) of
        {ok, Partition} ->
            NewState = State#ra_state{
                partitions = maps:remove(PartName, State#ra_state.partitions)
            },
            Effects = [{mod_call, flurm_db_ra_effects, partition_deleted, [Partition]}],
            {NewState, ok, Effects};
        error ->
            {State, {error, not_found}, []}
    end;

%% Unknown command
apply(_Meta, Unknown, State) ->
    {State, {error, {unknown_command, Unknown}}, []}.

%% @doc Called when the Ra server enters a new state (leader, follower, etc.)
-spec state_enter(ra_server:ra_state() | eol, #ra_state{}) -> ra_machine:effects().
state_enter(leader, _State) ->
    %% We became the leader - notify effects module
    [{mod_call, flurm_db_ra_effects, became_leader, [node()]}];
state_enter(follower, _State) ->
    %% We became a follower
    [{mod_call, flurm_db_ra_effects, became_follower, [node()]}];
state_enter(recover, _State) ->
    %% Recovering from snapshot
    [];
state_enter(eol, _State) ->
    %% End of life - cluster is being deleted
    [];
state_enter(_RaState, _State) ->
    [].

%% @doc Return the snapshot module for this state machine.
%% Using default snapshot handling.
-spec snapshot_module() -> module().
snapshot_module() ->
    ra_machine_simple.

%%====================================================================
%% API - Cluster Management
%%====================================================================

%% @doc Start a Ra cluster for the FLURM database.
-spec start_cluster([node()]) -> ok | {error, term()}.
start_cluster(Nodes) when is_list(Nodes) ->
    ClusterName = ?CLUSTER_NAME,
    _DataDir = application:get_env(flurm_db, data_dir, ?DEFAULT_DATA_DIR),

    %% Build server configuration for each node
    Servers = [{ClusterName, N} || N <- Nodes],

    MachineConfig = {module, ?MODULE, #{}},

    %% Start Ra servers on each node
    case ra:start_cluster(default, ClusterName, MachineConfig, Servers) of
        {ok, Started, _NotStarted} when length(Started) > 0 ->
            ok;
        {ok, [], NotStarted} ->
            {error, {no_servers_started, NotStarted}};
        {error, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% API - Commands (Replicated)
%%====================================================================

%% @doc Submit a new job for execution.
-spec submit_job(#ra_job_spec{}) -> {ok, job_id()} | {error, term()}.
submit_job(#ra_job_spec{} = JobSpec) ->
    ra_command({submit_job, JobSpec}).

%% @doc Cancel a job.
-spec cancel_job(job_id()) -> ok | {error, term()}.
cancel_job(JobId) when is_integer(JobId) ->
    ra_command({cancel_job, JobId}).

%% @doc Update the state of a job.
-spec update_job_state(job_id(), atom()) -> ok | {error, term()}.
update_job_state(JobId, NewState) when is_integer(JobId), is_atom(NewState) ->
    ra_command({update_job_state, JobId, NewState}).

%% @doc Allocate nodes to a job.
-spec allocate_job(job_id(), [node_name()]) -> ok | {error, term()}.
allocate_job(JobId, Nodes) when is_integer(JobId), is_list(Nodes) ->
    ra_command({allocate_job, JobId, Nodes}).

%% @doc Set the exit code for a completed job.
-spec set_job_exit_code(job_id(), integer()) -> ok | {error, term()}.
set_job_exit_code(JobId, ExitCode) when is_integer(JobId), is_integer(ExitCode) ->
    ra_command({set_job_exit_code, JobId, ExitCode}).

%% @doc Register a compute node.
-spec register_node(#ra_node_spec{}) -> {ok, registered | updated} | {error, term()}.
register_node(#ra_node_spec{} = NodeSpec) ->
    ra_command({register_node, NodeSpec}).

%% @doc Update the state of a node.
-spec update_node_state(node_name(), atom()) -> ok | {error, term()}.
update_node_state(NodeName, NewState) when is_binary(NodeName), is_atom(NewState) ->
    ra_command({update_node_state, NodeName, NewState}).

%% @doc Unregister a compute node.
-spec unregister_node(node_name()) -> ok | {error, term()}.
unregister_node(NodeName) when is_binary(NodeName) ->
    ra_command({unregister_node, NodeName}).

%% @doc Create a partition.
-spec create_partition(#ra_partition_spec{}) -> ok | {error, term()}.
create_partition(#ra_partition_spec{} = PartSpec) ->
    ra_command({create_partition, PartSpec}).

%% @doc Delete a partition.
-spec delete_partition(partition_name()) -> ok | {error, term()}.
delete_partition(PartName) when is_binary(PartName) ->
    ra_command({delete_partition, PartName}).

%% @doc Allocate a new job ID.
%% This atomically increments the job counter through Ra consensus,
%% ensuring unique IDs across the distributed cluster.
-spec allocate_job_id() -> {ok, job_id()} | {error, term()}.
allocate_job_id() ->
    ra_command(allocate_job_id).

%%====================================================================
%% API - Queries (Local Read - May Be Stale)
%%====================================================================

%% @doc Get a job by ID (local read, may be stale).
-spec get_job(job_id()) -> {ok, #ra_job{}} | {error, not_found}.
get_job(JobId) when is_integer(JobId) ->
    case ra_local_query(fun(State) ->
        maps:find(JobId, State#ra_state.jobs)
    end) of
        {ok, Job} -> {ok, Job};
        error -> {error, not_found};
        Other -> Other
    end.

%% @doc Get a node by name (local read, may be stale).
-spec get_node(node_name()) -> {ok, #ra_node{}} | {error, not_found}.
get_node(NodeName) when is_binary(NodeName) ->
    case ra_local_query(fun(State) ->
        maps:find(NodeName, State#ra_state.nodes)
    end) of
        {ok, Node} -> {ok, Node};
        error -> {error, not_found};
        Other -> Other
    end.

%% @doc Get a partition by name (local read, may be stale).
-spec get_partition(partition_name()) -> {ok, #ra_partition{}} | {error, not_found}.
get_partition(PartName) when is_binary(PartName) ->
    case ra_local_query(fun(State) ->
        maps:find(PartName, State#ra_state.partitions)
    end) of
        {ok, Partition} -> {ok, Partition};
        error -> {error, not_found};
        Other -> Other
    end.

%% @doc List all jobs (local read, may be stale).
-spec list_jobs() -> {ok, [#ra_job{}]}.
list_jobs() ->
    ra_local_query(fun(State) ->
        {ok, maps:values(State#ra_state.jobs)}
    end).

%% @doc List all nodes (local read, may be stale).
-spec list_nodes() -> {ok, [#ra_node{}]}.
list_nodes() ->
    ra_local_query(fun(State) ->
        {ok, maps:values(State#ra_state.nodes)}
    end).

%% @doc List all partitions (local read, may be stale).
-spec list_partitions() -> {ok, [#ra_partition{}]}.
list_partitions() ->
    ra_local_query(fun(State) ->
        {ok, maps:values(State#ra_state.partitions)}
    end).

%% @doc Get jobs by state (local read, may be stale).
-spec get_jobs_by_state(atom()) -> {ok, [#ra_job{}]}.
get_jobs_by_state(JobState) when is_atom(JobState) ->
    ra_local_query(fun(State) ->
        Jobs = maps:filter(fun(_Id, Job) ->
            Job#ra_job.state =:= JobState
        end, State#ra_state.jobs),
        {ok, maps:values(Jobs)}
    end).

%% @doc Get nodes by state (local read, may be stale).
-spec get_nodes_by_state(atom()) -> {ok, [#ra_node{}]}.
get_nodes_by_state(NodeState) when is_atom(NodeState) ->
    ra_local_query(fun(State) ->
        Nodes = maps:filter(fun(_Name, Node) ->
            Node#ra_node.state =:= NodeState
        end, State#ra_state.nodes),
        {ok, maps:values(Nodes)}
    end).

%% @doc Get nodes in a partition (local read, may be stale).
-spec get_nodes_in_partition(partition_name()) -> {ok, [#ra_node{}]} | {error, not_found}.
get_nodes_in_partition(PartName) when is_binary(PartName) ->
    ra_local_query(fun(State) ->
        case maps:find(PartName, State#ra_state.partitions) of
            {ok, Partition} ->
                NodeNames = Partition#ra_partition.nodes,
                Nodes = [Node || NodeName <- NodeNames,
                                 {ok, Node} <- [maps:find(NodeName, State#ra_state.nodes)]],
                {ok, Nodes};
            error ->
                {error, not_found}
        end
    end).

%%====================================================================
%% API - Consistent Queries (Through Leader)
%%====================================================================

%% @doc Get a job by ID (consistent read through leader).
-spec consistent_get_job(job_id()) -> {ok, #ra_job{}} | {error, term()}.
consistent_get_job(JobId) when is_integer(JobId) ->
    ra_consistent_query(fun(State) ->
        maps:find(JobId, State#ra_state.jobs)
    end).

%% @doc List all jobs (consistent read through leader).
-spec consistent_list_jobs() -> {ok, [#ra_job{}]} | {error, term()}.
consistent_list_jobs() ->
    ra_consistent_query(fun(State) ->
        {ok, maps:values(State#ra_state.jobs)}
    end).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
%% Execute a command through Ra (replicated).
ra_command(Command) ->
    Server = {?CLUSTER_NAME, node()},
    case ra:process_command(Server, Command, ?RA_TIMEOUT) of
        {ok, Result, _Leader} ->
            Result;
        {timeout, _} ->
            {error, timeout};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% Execute a local query (may return stale data).
ra_local_query(QueryFun) ->
    Server = {?CLUSTER_NAME, node()},
    case ra:local_query(Server, QueryFun, ?RA_TIMEOUT) of
        {ok, {_RaftIdx, Result}, _Leader} ->
            Result;
        {timeout, _} ->
            {error, timeout};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% Execute a consistent query through the leader.
ra_consistent_query(QueryFun) ->
    Server = {?CLUSTER_NAME, node()},
    case ra:consistent_query(Server, QueryFun, ?RA_TIMEOUT) of
        {ok, {_RaftIdx, Result}, _Leader} ->
            Result;
        {timeout, _} ->
            {error, timeout};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
%% Create a job record from a job spec.
make_job_record(JobId, #ra_job_spec{} = Spec) ->
    Now = erlang:system_time(second),
    Priority = case Spec#ra_job_spec.priority of
        undefined -> ?DEFAULT_PRIORITY;
        P -> P
    end,
    #ra_job{
        id = JobId,
        name = Spec#ra_job_spec.name,
        user = Spec#ra_job_spec.user,
        group = Spec#ra_job_spec.group,
        partition = Spec#ra_job_spec.partition,
        state = pending,
        script = Spec#ra_job_spec.script,
        num_nodes = Spec#ra_job_spec.num_nodes,
        num_cpus = Spec#ra_job_spec.num_cpus,
        memory_mb = Spec#ra_job_spec.memory_mb,
        time_limit = Spec#ra_job_spec.time_limit,
        priority = Priority,
        submit_time = Now,
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined
    }.

%% @private
%% Create a node record from a node spec.
make_node_record(#ra_node_spec{} = Spec) ->
    Now = erlang:system_time(second),
    #ra_node{
        name = Spec#ra_node_spec.name,
        hostname = Spec#ra_node_spec.hostname,
        port = Spec#ra_node_spec.port,
        cpus = Spec#ra_node_spec.cpus,
        cpus_used = 0,
        memory_mb = Spec#ra_node_spec.memory_mb,
        memory_used = 0,
        gpus = Spec#ra_node_spec.gpus,
        gpus_used = 0,
        state = up,
        features = Spec#ra_node_spec.features,
        partitions = Spec#ra_node_spec.partitions,
        running_jobs = [],
        last_heartbeat = Now
    }.

%% @private
%% Create a partition record from a partition spec.
make_partition_record(#ra_partition_spec{} = Spec) ->
    #ra_partition{
        name = Spec#ra_partition_spec.name,
        state = up,
        nodes = Spec#ra_partition_spec.nodes,
        max_time = Spec#ra_partition_spec.max_time,
        default_time = Spec#ra_partition_spec.default_time,
        max_nodes = Spec#ra_partition_spec.max_nodes,
        priority = Spec#ra_partition_spec.priority
    }.

%% @private
%% Update a job record with a new state.
update_job_state_record(Job, NewState) ->
    Now = erlang:system_time(second),
    case NewState of
        running when Job#ra_job.start_time =:= undefined ->
            Job#ra_job{state = NewState, start_time = Now};
        S when S =:= completed; S =:= failed; S =:= cancelled;
               S =:= timeout; S =:= node_fail ->
            Job#ra_job{state = NewState, end_time = Now};
        _ ->
            Job#ra_job{state = NewState}
    end.
