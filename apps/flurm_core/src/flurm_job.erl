%%%-------------------------------------------------------------------
%%% @doc FLURM Job State Machine
%%%
%%% A gen_statem implementing the job lifecycle for the FLURM workload
%%% manager. Handles job states from submission through completion.
%%%
%%% States:
%%%   pending     - Job submitted, waiting for resources
%%%   configuring - Resources allocated, configuring on nodes
%%%   running     - Job executing
%%%   completing  - Job finished, cleaning up
%%%   completed   - Job done successfully (terminal)
%%%   cancelled   - Job was cancelled (terminal)
%%%   failed      - Job failed (terminal)
%%%   timeout     - Job exceeded time limit (terminal)
%%%   node_fail   - Job failed due to node failure (terminal)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job).
-behaviour(gen_statem).

-include("flurm_core.hrl").

%% API
-export([
    start_link/1,
    submit/1,
    cancel/1,
    get_info/1,
    allocate/2,
    signal_config_complete/1,
    signal_job_complete/2,
    signal_cleanup_complete/1,
    signal_node_failure/2,
    get_state/1,
    preempt/3,
    suspend/1,
    resume/1,
    set_priority/2
]).

%% gen_statem callbacks
-export([
    init/1,
    callback_mode/0,
    terminate/3,
    code_change/4
]).

%% State callbacks
-export([
    pending/3,
    configuring/3,
    running/3,
    suspended/3,
    completing/3,
    completed/3,
    cancelled/3,
    failed/3,
    timeout/3,
    node_fail/3
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a job process with a job specification.
%% The job is started under the job supervisor.
-spec start_link(#job_spec{}) -> {ok, pid()} | {error, term()}.
start_link(#job_spec{} = JobSpec) ->
    gen_statem:start_link(?MODULE, [JobSpec], []).

%% @doc Submit a new job. Creates the job process and returns the job ID.
-spec submit(#job_spec{}) -> {ok, pid(), job_id()} | {error, term()}.
submit(#job_spec{} = JobSpec) ->
    case flurm_job_sup:start_job(JobSpec) of
        {ok, Pid} ->
            JobId = gen_statem:call(Pid, get_job_id),
            ok = flurm_job_registry:register_job(JobId, Pid),
            {ok, Pid, JobId};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Cancel a job by pid or job_id.
-spec cancel(pid() | job_id()) -> ok | {error, term()}.
cancel(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, cancel);
cancel(JobId) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> cancel(Pid);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Get information about a job.
-spec get_info(pid() | job_id()) -> {ok, map()} | {error, term()}.
get_info(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, get_info);
get_info(JobId) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> get_info(Pid);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Allocate nodes to a job (called by scheduler).
-spec allocate(pid() | job_id(), [binary()]) -> ok | {error, term()}.
allocate(Pid, Nodes) when is_pid(Pid), is_list(Nodes) ->
    gen_statem:call(Pid, {allocate, Nodes});
allocate(JobId, Nodes) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> allocate(Pid, Nodes);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Signal that job configuration is complete.
-spec signal_config_complete(pid() | job_id()) -> ok | {error, term()}.
signal_config_complete(Pid) when is_pid(Pid) ->
    gen_statem:cast(Pid, config_complete);
signal_config_complete(JobId) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> signal_config_complete(Pid);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Signal that a job has completed with an exit code.
-spec signal_job_complete(pid() | job_id(), integer()) -> ok | {error, term()}.
signal_job_complete(Pid, ExitCode) when is_pid(Pid), is_integer(ExitCode) ->
    gen_statem:cast(Pid, {job_complete, ExitCode});
signal_job_complete(JobId, ExitCode) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> signal_job_complete(Pid, ExitCode);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Signal that cleanup is complete.
-spec signal_cleanup_complete(pid() | job_id()) -> ok | {error, term()}.
signal_cleanup_complete(Pid) when is_pid(Pid) ->
    gen_statem:cast(Pid, cleanup_complete);
signal_cleanup_complete(JobId) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> signal_cleanup_complete(Pid);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Signal that a node has failed.
-spec signal_node_failure(pid() | job_id(), binary()) -> ok | {error, term()}.
signal_node_failure(Pid, NodeName) when is_pid(Pid), is_binary(NodeName) ->
    gen_statem:cast(Pid, {node_failure, NodeName});
signal_node_failure(JobId, NodeName) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> signal_node_failure(Pid, NodeName);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Get the current state of a job.
-spec get_state(pid() | job_id()) -> {ok, job_state()} | {error, term()}.
get_state(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, get_state);
get_state(JobId) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> get_state(Pid);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Preempt a job (for scheduler preemption).
%% Mode: requeue | cancel | checkpoint
%% GraceTime: seconds to allow graceful shutdown
-spec preempt(pid() | job_id(), atom(), pos_integer()) -> ok | {error, term()}.
preempt(Pid, Mode, GraceTime) when is_pid(Pid) ->
    gen_statem:call(Pid, {preempt, Mode, GraceTime});
preempt(JobId, Mode, GraceTime) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> preempt(Pid, Mode, GraceTime);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Suspend a running job.
-spec suspend(pid() | job_id()) -> ok | {error, term()}.
suspend(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, suspend);
suspend(JobId) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> suspend(Pid);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Resume a suspended job.
-spec resume(pid() | job_id()) -> ok | {error, term()}.
resume(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, resume);
resume(JobId) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> resume(Pid);
        {error, not_found} -> {error, job_not_found}
    end.

%% @doc Set job priority (for priority adjustments).
-spec set_priority(pid() | job_id(), integer()) -> ok | {error, term()}.
set_priority(Pid, Priority) when is_pid(Pid), is_integer(Priority) ->
    gen_statem:call(Pid, {set_priority, Priority});
set_priority(JobId, Priority) when is_integer(JobId) ->
    case flurm_job_registry:lookup_job(JobId) of
        {ok, Pid} -> set_priority(Pid, Priority);
        {error, not_found} -> {error, job_not_found}
    end.

%%====================================================================
%% gen_statem callbacks
%%====================================================================

%% @private
callback_mode() ->
    [state_functions, state_enter].

%% @private
init([#job_spec{} = JobSpec]) ->
    JobId = erlang:unique_integer([positive, monotonic]),
    Priority = case JobSpec#job_spec.priority of
        undefined -> ?DEFAULT_PRIORITY;
        P -> max(?MIN_PRIORITY, min(?MAX_PRIORITY, P))
    end,
    Data = #job_data{
        job_id = JobId,
        user_id = JobSpec#job_spec.user_id,
        group_id = JobSpec#job_spec.group_id,
        partition = JobSpec#job_spec.partition,
        num_nodes = JobSpec#job_spec.num_nodes,
        num_cpus = JobSpec#job_spec.num_cpus,
        time_limit = JobSpec#job_spec.time_limit,
        script = JobSpec#job_spec.script,
        allocated_nodes = [],
        submit_time = erlang:timestamp(),
        start_time = undefined,
        end_time = undefined,
        exit_code = undefined,
        priority = Priority,
        state_version = ?JOB_STATE_VERSION
    },
    %% Record job submission in accounting database
    flurm_accounting:record_job_submit(Data),
    {ok, pending, Data}.

%% @private
terminate(_Reason, _State, #job_data{job_id = JobId}) ->
    flurm_job_registry:unregister_job(JobId),
    ok.

%% @private
%% Handle hot code upgrades
code_change(_OldVsn, State, Data, _Extra) ->
    %% Upgrade state data if version is different
    NewData = maybe_upgrade_state_data(Data),
    {ok, State, NewData}.

%%====================================================================
%% State: pending
%% Job is waiting for resource allocation
%%====================================================================

pending(enter, _OldState, Data) ->
    %% Notify registry of state change
    notify_state_change(Data#job_data.job_id, pending),
    {keep_state, Data, [{state_timeout, ?PENDING_TIMEOUT, pending_timeout}]};

pending({call, From}, get_job_id, #job_data{job_id = JobId} = Data) ->
    {keep_state, Data, [{reply, From, JobId}]};

pending({call, From}, get_info, Data) ->
    Info = build_job_info(pending, Data),
    {keep_state, Data, [{reply, From, {ok, Info}}]};

pending({call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, {ok, pending}}]};

pending({call, From}, cancel, Data) ->
    NewData = Data#job_data{end_time = erlang:timestamp()},
    {next_state, cancelled, NewData, [{reply, From, ok}]};

pending({call, From}, {allocate, Nodes}, Data) when is_list(Nodes) ->
    case length(Nodes) >= Data#job_data.num_nodes of
        true ->
            NewData = Data#job_data{allocated_nodes = Nodes},
            {next_state, configuring, NewData, [{reply, From, ok}]};
        false ->
            {keep_state, Data, [{reply, From, {error, insufficient_nodes}}]}
    end;

pending({call, From}, _Msg, Data) ->
    {keep_state, Data, [{reply, From, {error, invalid_operation}}]};

pending(cast, _Msg, Data) ->
    {keep_state, Data};

pending(state_timeout, pending_timeout, Data) ->
    NewData = Data#job_data{end_time = erlang:timestamp()},
    {next_state, timeout, NewData};

pending(info, _Msg, Data) ->
    {keep_state, Data}.

%%====================================================================
%% State: configuring
%% Resources allocated, setting up on nodes
%%====================================================================

configuring(enter, _OldState, Data) ->
    notify_state_change(Data#job_data.job_id, configuring),
    %% In a real system, trigger node configuration here
    {keep_state, Data, [{state_timeout, ?CONFIGURING_TIMEOUT, config_timeout}]};

configuring({call, From}, get_job_id, #job_data{job_id = JobId} = Data) ->
    {keep_state, Data, [{reply, From, JobId}]};

configuring({call, From}, get_info, Data) ->
    Info = build_job_info(configuring, Data),
    {keep_state, Data, [{reply, From, {ok, Info}}]};

configuring({call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, {ok, configuring}}]};

configuring({call, From}, cancel, Data) ->
    NewData = Data#job_data{end_time = erlang:timestamp()},
    {next_state, cancelled, NewData, [{reply, From, ok}]};

configuring({call, From}, _Msg, Data) ->
    {keep_state, Data, [{reply, From, {error, invalid_operation}}]};

configuring(cast, config_complete, Data) ->
    NewData = Data#job_data{start_time = erlang:timestamp()},
    %% Set running timeout based on job time limit
    TimeoutMs = Data#job_data.time_limit * 1000,
    {next_state, running, NewData, [{state_timeout, TimeoutMs, job_timeout}]};

configuring(cast, {node_failure, NodeName}, Data) ->
    case lists:member(NodeName, Data#job_data.allocated_nodes) of
        true ->
            NewData = Data#job_data{end_time = erlang:timestamp()},
            {next_state, node_fail, NewData};
        false ->
            {keep_state, Data}
    end;

configuring(cast, _Msg, Data) ->
    {keep_state, Data};

configuring(state_timeout, config_timeout, Data) ->
    NewData = Data#job_data{end_time = erlang:timestamp()},
    {next_state, failed, NewData};

configuring(info, _Msg, Data) ->
    {keep_state, Data}.

%%====================================================================
%% State: running
%% Job is executing
%%====================================================================

running(enter, _OldState, Data) ->
    notify_state_change(Data#job_data.job_id, running),
    %% Record job start in accounting database
    flurm_accounting:record_job_start(Data#job_data.job_id, Data#job_data.allocated_nodes),
    {keep_state, Data};

running({call, From}, get_job_id, #job_data{job_id = JobId} = Data) ->
    {keep_state, Data, [{reply, From, JobId}]};

running({call, From}, get_info, Data) ->
    Info = build_job_info(running, Data),
    {keep_state, Data, [{reply, From, {ok, Info}}]};

running({call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, {ok, running}}]};

running({call, From}, cancel, Data) ->
    %% Cancel during running - need to clean up
    NewData = Data#job_data{end_time = erlang:timestamp()},
    {next_state, cancelled, NewData, [{reply, From, ok}]};

running({call, From}, {preempt, requeue, _GraceTime}, Data) ->
    %% Preempt and requeue - return to pending
    NewData = Data#job_data{
        allocated_nodes = [],
        start_time = undefined
    },
    {next_state, pending, NewData, [{reply, From, ok}]};

running({call, From}, {preempt, cancel, _GraceTime}, Data) ->
    %% Preempt and cancel - terminate job
    NewData = Data#job_data{end_time = erlang:timestamp()},
    {next_state, cancelled, NewData, [{reply, From, ok}]};

running({call, From}, {preempt, checkpoint, _GraceTime}, Data) ->
    %% Checkpoint preemption - save state then requeue
    %% In real implementation, would save checkpoint here
    NewData = Data#job_data{
        allocated_nodes = [],
        start_time = undefined
    },
    {next_state, pending, NewData, [{reply, From, ok}]};

running({call, From}, suspend, Data) ->
    %% Suspend the job
    {next_state, suspended, Data, [{reply, From, ok}]};

running({call, From}, {set_priority, NewPriority}, Data) ->
    ClampedPriority = max(?MIN_PRIORITY, min(?MAX_PRIORITY, NewPriority)),
    NewData = Data#job_data{priority = ClampedPriority},
    {keep_state, NewData, [{reply, From, ok}]};

running({call, From}, _Msg, Data) ->
    {keep_state, Data, [{reply, From, {error, invalid_operation}}]};

running(cast, {job_complete, ExitCode}, Data) ->
    NewData = Data#job_data{
        end_time = erlang:timestamp(),
        exit_code = ExitCode
    },
    {next_state, completing, NewData};

running(cast, {node_failure, NodeName}, Data) ->
    case lists:member(NodeName, Data#job_data.allocated_nodes) of
        true ->
            NewData = Data#job_data{end_time = erlang:timestamp()},
            {next_state, node_fail, NewData};
        false ->
            {keep_state, Data}
    end;

running(cast, _Msg, Data) ->
    {keep_state, Data};

running(state_timeout, job_timeout, Data) ->
    NewData = Data#job_data{end_time = erlang:timestamp()},
    {next_state, timeout, NewData};

running(info, _Msg, Data) ->
    {keep_state, Data}.

%%====================================================================
%% State: suspended
%% Job is paused, resources still allocated
%%====================================================================

suspended(enter, _OldState, Data) ->
    notify_state_change(Data#job_data.job_id, suspended),
    {keep_state, Data};

suspended({call, From}, get_job_id, #job_data{job_id = JobId} = Data) ->
    {keep_state, Data, [{reply, From, JobId}]};

suspended({call, From}, get_info, Data) ->
    Info = build_job_info(suspended, Data),
    {keep_state, Data, [{reply, From, {ok, Info}}]};

suspended({call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, {ok, suspended}}]};

suspended({call, From}, resume, Data) ->
    %% Resume the job - return to running
    {next_state, running, Data, [{reply, From, ok}]};

suspended({call, From}, cancel, Data) ->
    NewData = Data#job_data{end_time = erlang:timestamp()},
    {next_state, cancelled, NewData, [{reply, From, ok}]};

suspended({call, From}, {preempt, requeue, _GraceTime}, Data) ->
    %% Preempt suspended job - return to pending
    NewData = Data#job_data{
        allocated_nodes = [],
        start_time = undefined
    },
    {next_state, pending, NewData, [{reply, From, ok}]};

suspended({call, From}, {preempt, cancel, _GraceTime}, Data) ->
    NewData = Data#job_data{end_time = erlang:timestamp()},
    {next_state, cancelled, NewData, [{reply, From, ok}]};

suspended({call, From}, {set_priority, NewPriority}, Data) ->
    ClampedPriority = max(?MIN_PRIORITY, min(?MAX_PRIORITY, NewPriority)),
    NewData = Data#job_data{priority = ClampedPriority},
    {keep_state, NewData, [{reply, From, ok}]};

suspended({call, From}, _Msg, Data) ->
    {keep_state, Data, [{reply, From, {error, invalid_operation}}]};

suspended(cast, {node_failure, NodeName}, Data) ->
    case lists:member(NodeName, Data#job_data.allocated_nodes) of
        true ->
            NewData = Data#job_data{end_time = erlang:timestamp()},
            {next_state, node_fail, NewData};
        false ->
            {keep_state, Data}
    end;

suspended(cast, _Msg, Data) ->
    {keep_state, Data};

suspended(info, _Msg, Data) ->
    {keep_state, Data}.

%%====================================================================
%% State: completing
%% Job finished, cleaning up resources
%%====================================================================

completing(enter, _OldState, Data) ->
    notify_state_change(Data#job_data.job_id, completing),
    %% In a real system, trigger cleanup on nodes here
    {keep_state, Data, [{state_timeout, ?COMPLETING_TIMEOUT, cleanup_timeout}]};

completing({call, From}, get_job_id, #job_data{job_id = JobId} = Data) ->
    {keep_state, Data, [{reply, From, JobId}]};

completing({call, From}, get_info, Data) ->
    Info = build_job_info(completing, Data),
    {keep_state, Data, [{reply, From, {ok, Info}}]};

completing({call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, {ok, completing}}]};

completing({call, From}, cancel, Data) ->
    %% Already completing, just acknowledge
    {keep_state, Data, [{reply, From, ok}]};

completing({call, From}, _Msg, Data) ->
    {keep_state, Data, [{reply, From, {error, invalid_operation}}]};

completing(cast, cleanup_complete, Data) ->
    case Data#job_data.exit_code of
        0 -> {next_state, completed, Data};
        _ -> {next_state, failed, Data}
    end;

completing(cast, _Msg, Data) ->
    {keep_state, Data};

completing(state_timeout, cleanup_timeout, Data) ->
    %% Cleanup timed out - still transition based on exit code
    case Data#job_data.exit_code of
        0 -> {next_state, completed, Data};
        _ -> {next_state, failed, Data}
    end;

completing(info, _Msg, Data) ->
    {keep_state, Data}.

%%====================================================================
%% Terminal States
%% These states do not transition to other states
%%====================================================================

%% completed - Job finished successfully
completed(enter, _OldState, Data) ->
    notify_state_change(Data#job_data.job_id, completed),
    %% Record job completion in accounting database
    ExitCode = case Data#job_data.exit_code of
        undefined -> 0;
        Code -> Code
    end,
    flurm_accounting:record_job_end(Data#job_data.job_id, ExitCode, completed),
    {keep_state, Data};

completed({call, From}, get_job_id, #job_data{job_id = JobId} = Data) ->
    {keep_state, Data, [{reply, From, JobId}]};

completed({call, From}, get_info, Data) ->
    Info = build_job_info(completed, Data),
    {keep_state, Data, [{reply, From, {ok, Info}}]};

completed({call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, {ok, completed}}]};

completed({call, From}, cancel, Data) ->
    {keep_state, Data, [{reply, From, {error, already_completed}}]};

completed({call, From}, _Msg, Data) ->
    {keep_state, Data, [{reply, From, {error, job_completed}}]};

completed(cast, _Msg, Data) ->
    {keep_state, Data};

completed(info, _Msg, Data) ->
    {keep_state, Data}.

%% cancelled - Job was cancelled
cancelled(enter, _OldState, Data) ->
    notify_state_change(Data#job_data.job_id, cancelled),
    %% Record job cancellation in accounting database
    flurm_accounting:record_job_cancelled(Data#job_data.job_id, user_cancelled),
    {keep_state, Data};

cancelled({call, From}, get_job_id, #job_data{job_id = JobId} = Data) ->
    {keep_state, Data, [{reply, From, JobId}]};

cancelled({call, From}, get_info, Data) ->
    Info = build_job_info(cancelled, Data),
    {keep_state, Data, [{reply, From, {ok, Info}}]};

cancelled({call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, {ok, cancelled}}]};

cancelled({call, From}, cancel, Data) ->
    {keep_state, Data, [{reply, From, {error, already_cancelled}}]};

cancelled({call, From}, _Msg, Data) ->
    {keep_state, Data, [{reply, From, {error, job_cancelled}}]};

cancelled(cast, _Msg, Data) ->
    {keep_state, Data};

cancelled(info, _Msg, Data) ->
    {keep_state, Data}.

%% failed - Job failed
failed(enter, _OldState, Data) ->
    notify_state_change(Data#job_data.job_id, failed),
    %% Record job failure in accounting database
    ExitCode = case Data#job_data.exit_code of
        undefined -> 1;
        Code -> Code
    end,
    flurm_accounting:record_job_end(Data#job_data.job_id, ExitCode, failed),
    {keep_state, Data};

failed({call, From}, get_job_id, #job_data{job_id = JobId} = Data) ->
    {keep_state, Data, [{reply, From, JobId}]};

failed({call, From}, get_info, Data) ->
    Info = build_job_info(failed, Data),
    {keep_state, Data, [{reply, From, {ok, Info}}]};

failed({call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, {ok, failed}}]};

failed({call, From}, cancel, Data) ->
    {keep_state, Data, [{reply, From, {error, already_failed}}]};

failed({call, From}, _Msg, Data) ->
    {keep_state, Data, [{reply, From, {error, job_failed}}]};

failed(cast, _Msg, Data) ->
    {keep_state, Data};

failed(info, _Msg, Data) ->
    {keep_state, Data}.

%% timeout - Job exceeded time limit
timeout(enter, _OldState, Data) ->
    notify_state_change(Data#job_data.job_id, timeout),
    %% Record job timeout in accounting database
    flurm_accounting:record_job_end(Data#job_data.job_id, -1, timeout),
    {keep_state, Data};

timeout({call, From}, get_job_id, #job_data{job_id = JobId} = Data) ->
    {keep_state, Data, [{reply, From, JobId}]};

timeout({call, From}, get_info, Data) ->
    Info = build_job_info(timeout, Data),
    {keep_state, Data, [{reply, From, {ok, Info}}]};

timeout({call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, {ok, timeout}}]};

timeout({call, From}, cancel, Data) ->
    {keep_state, Data, [{reply, From, {error, already_timed_out}}]};

timeout({call, From}, _Msg, Data) ->
    {keep_state, Data, [{reply, From, {error, job_timed_out}}]};

timeout(cast, _Msg, Data) ->
    {keep_state, Data};

timeout(info, _Msg, Data) ->
    {keep_state, Data}.

%% node_fail - Job failed due to node failure
node_fail(enter, _OldState, Data) ->
    notify_state_change(Data#job_data.job_id, node_fail),
    %% Record node failure in accounting database
    flurm_accounting:record_job_end(Data#job_data.job_id, -1, node_fail),
    {keep_state, Data};

node_fail({call, From}, get_job_id, #job_data{job_id = JobId} = Data) ->
    {keep_state, Data, [{reply, From, JobId}]};

node_fail({call, From}, get_info, Data) ->
    Info = build_job_info(node_fail, Data),
    {keep_state, Data, [{reply, From, {ok, Info}}]};

node_fail({call, From}, get_state, Data) ->
    {keep_state, Data, [{reply, From, {ok, node_fail}}]};

node_fail({call, From}, cancel, Data) ->
    {keep_state, Data, [{reply, From, {error, already_failed}}]};

node_fail({call, From}, _Msg, Data) ->
    {keep_state, Data, [{reply, From, {error, node_failure}}]};

node_fail(cast, _Msg, Data) ->
    {keep_state, Data};

node_fail(info, _Msg, Data) ->
    {keep_state, Data}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Build a map of job information for external queries
build_job_info(State, #job_data{} = Data) ->
    #{
        job_id => Data#job_data.job_id,
        user_id => Data#job_data.user_id,
        group_id => Data#job_data.group_id,
        partition => Data#job_data.partition,
        state => State,
        num_nodes => Data#job_data.num_nodes,
        num_cpus => Data#job_data.num_cpus,
        time_limit => Data#job_data.time_limit,
        script => Data#job_data.script,
        allocated_nodes => Data#job_data.allocated_nodes,
        submit_time => Data#job_data.submit_time,
        start_time => Data#job_data.start_time,
        end_time => Data#job_data.end_time,
        exit_code => Data#job_data.exit_code,
        priority => Data#job_data.priority
    }.

%% @private
%% Notify the registry of a state change
notify_state_change(JobId, NewState) ->
    %% Best effort - don't crash if registry is unavailable
    catch flurm_job_registry:update_state(JobId, NewState),
    ok.

%% @private
%% Handle state data upgrades during hot code changes
maybe_upgrade_state_data(#job_data{state_version = ?JOB_STATE_VERSION} = Data) ->
    Data;
maybe_upgrade_state_data(#job_data{state_version = OldVersion} = Data)
  when OldVersion < ?JOB_STATE_VERSION ->
    %% Add upgrade logic here as the state version increases
    %% For now, just update the version
    Data#job_data{state_version = ?JOB_STATE_VERSION};
maybe_upgrade_state_data(Data) when is_record(Data, job_data) ->
    %% Handle case where state_version field might not exist (pre-version data)
    Data#job_data{state_version = ?JOB_STATE_VERSION}.
