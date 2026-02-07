%%%-------------------------------------------------------------------
%%% @doc FLURM Job Dispatcher
%%%
%%% Dispatches jobs to node daemons for execution. When a job is
%%% allocated to nodes, this module sends the job_launch message
%%% to all allocated nodes.
%%%
%%% Also handles job cancellation and cleanup signals to nodes.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher_server).

-behaviour(gen_server).

-export([start_link/0]).
-export([
    dispatch_job/2,
    cancel_job/2,
    signal_job/3,
    preempt_job/2,
    requeue_job/1,
    drain_node/1,
    resume_node/1
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Test exports for internal functions
-ifdef(TEST).
-export([
    build_job_launch_message/2,
    signal_to_name/1
]).
-endif.

-record(state, {
    dispatched_jobs = #{} :: #{pos_integer() => [binary()]}  % JobId => [Hostnames]
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Dispatch a job to the allocated nodes.
%% Called by the scheduler after job allocation succeeds.
-spec dispatch_job(pos_integer(), map()) -> ok | {error, term()}.
dispatch_job(JobId, JobInfo) when is_integer(JobId), is_map(JobInfo) ->
    gen_server:call(?MODULE, {dispatch_job, JobId, JobInfo}).

%% @doc Cancel a job on the specified nodes.
-spec cancel_job(pos_integer(), [binary()]) -> ok.
cancel_job(JobId, Nodes) when is_integer(JobId), is_list(Nodes) ->
    gen_server:cast(?MODULE, {cancel_job, JobId, Nodes}).

%% @doc Send a signal to a job on a specific node.
%% Signal can be an integer (e.g., 15 for SIGTERM) or an atom (e.g., sigterm).
-spec signal_job(pos_integer(), non_neg_integer() | atom(), binary()) -> ok | {error, term()}.
signal_job(JobId, Signal, Node) when is_integer(JobId), is_binary(Node) ->
    preempt_job(JobId, #{signal => Signal, nodes => [Node]}).

%% @doc Send drain signal to a node.
-spec drain_node(binary()) -> ok | {error, term()}.
drain_node(Hostname) when is_binary(Hostname) ->
    gen_server:call(?MODULE, {drain_node, Hostname}).

%% @doc Send resume signal to a node.
-spec resume_node(binary()) -> ok | {error, term()}.
resume_node(Hostname) when is_binary(Hostname) ->
    gen_server:call(?MODULE, {resume_node, Hostname}).

%% @doc Preempt a running job by sending signals to the nodes.
%% Options can include:
%%   - signal: sigterm | sigkill | sigstop | sigcont (default: sigterm)
%%   - nodes: list of nodes to send signal to (default: all allocated nodes)
%% Returns ok on success, {error, Reason} on failure.
-spec preempt_job(pos_integer(), map()) -> ok | {error, term()}.
preempt_job(JobId, Options) when is_integer(JobId), is_map(Options) ->
    gen_server:call(?MODULE, {preempt_job, JobId, Options}).

%% @doc Requeue a job after preemption.
%% This cleans up dispatcher state and allows the job to be re-dispatched.
-spec requeue_job(pos_integer()) -> ok.
requeue_job(JobId) when is_integer(JobId) ->
    gen_server:cast(?MODULE, {requeue_job, JobId}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    log(info, "Job Dispatcher started", []),
    {ok, #state{}}.

handle_call({dispatch_job, JobId, JobInfo}, _From, #state{dispatched_jobs = Jobs} = State) ->
    AllocatedNodes = maps:get(allocated_nodes, JobInfo, []),
    %% Debug: Log script content being dispatched
    Script = maps:get(script, JobInfo, <<>>),
    ScriptLen = byte_size(Script),
    ScriptPreview = case ScriptLen > 80 of
        true -> <<(binary:part(Script, 0, 80))/binary, "...">>;
        false -> Script
    end,
    log(debug, "Dispatching job ~p: script_size=~p, script_preview=~p",
        [JobId, ScriptLen, ScriptPreview]),
    case AllocatedNodes of
        [] ->
            log(warning, "No nodes allocated for job ~p", [JobId]),
            {reply, {error, no_nodes}, State};
        Nodes ->
            %% Build the job launch message
            LaunchMsg = build_job_launch_message(JobId, JobInfo),

            %% Send to all allocated nodes
            Results = flurm_node_connection_manager:send_to_nodes(Nodes, LaunchMsg),

            %% Check results
            {Succeeded, Failed} = lists:partition(
                fun({_Node, Result}) -> Result =:= ok end,
                Results
            ),

            case Failed of
                [] ->
                    log(debug, "Dispatched job ~p to ~p nodes", [JobId, length(Succeeded)]),
                    %% Job state is updated to 'running' by the scheduler
                    NewJobs = maps:put(JobId, Nodes, Jobs),
                    {reply, ok, State#state{dispatched_jobs = NewJobs}};
                _ ->
                    %% Some nodes failed - log but continue
                    FailedNodes = [N || {N, _} <- Failed],
                    log(warning, "Job ~p: failed to dispatch to nodes: ~p", [JobId, FailedNodes]),
                    case Succeeded of
                        [] ->
                            {reply, {error, all_nodes_failed}, State};
                        _ ->
                            %% At least some nodes got the job
                            SucceededNodes = [N || {N, _} <- Succeeded],
                            NewJobs = maps:put(JobId, SucceededNodes, Jobs),
                            {reply, ok, State#state{dispatched_jobs = NewJobs}}
                    end
            end
    end;

handle_call({drain_node, Hostname}, _From, State) ->
    Msg = #{type => node_drain, payload => #{}},
    Result = flurm_node_connection_manager:send_to_node(Hostname, Msg),
    {reply, Result, State};

handle_call({resume_node, Hostname}, _From, State) ->
    Msg = #{type => node_resume, payload => #{}},
    Result = flurm_node_connection_manager:send_to_node(Hostname, Msg),
    {reply, Result, State};

handle_call({preempt_job, JobId, Options}, _From, #state{dispatched_jobs = Jobs} = State) ->
    %% Get nodes for this job
    Nodes = case maps:get(nodes, Options, undefined) of
        undefined ->
            %% Use nodes from our tracking
            maps:get(JobId, Jobs, []);
        ExplicitNodes ->
            ExplicitNodes
    end,

    case Nodes of
        [] ->
            log(warning, "Preempt job ~p: no nodes found", [JobId]),
            {reply, {error, no_nodes_for_job}, State};
        _ ->
            %% Get signal type (default to sigterm for graceful preemption)
            Signal = maps:get(signal, Options, sigterm),
            SignalName = signal_to_name(Signal),

            %% Build the preempt message
            PreemptMsg = #{
                type => job_signal,
                payload => #{
                    <<"job_id">> => JobId,
                    <<"signal">> => SignalName
                }
            },

            %% Send to all nodes
            Results = flurm_node_connection_manager:send_to_nodes(Nodes, PreemptMsg),

            %% Check results
            {Succeeded, Failed} = lists:partition(
                fun({_Node, Result}) -> Result =:= ok end,
                Results
            ),

            case Failed of
                [] ->
                    log(info, "Sent ~s to job ~p on ~p nodes",
                       [SignalName, JobId, length(Succeeded)]),
                    {reply, ok, State};
                _ ->
                    FailedNodes = [N || {N, _} <- Failed],
                    log(warning, "Job ~p: failed to send ~s to nodes: ~p",
                       [JobId, SignalName, FailedNodes]),
                    case Succeeded of
                        [] ->
                            {reply, {error, all_nodes_failed}, State};
                        _ ->
                            %% Partial success - return ok but log warning
                            {reply, ok, State}
                    end
            end
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({cancel_job, JobId, Nodes}, #state{dispatched_jobs = Jobs} = State) ->
    CancelMsg = #{
        type => job_cancel,
        payload => #{
            <<"job_id">> => JobId
        }
    },

    %% Send cancel to all nodes
    _ = flurm_node_connection_manager:send_to_nodes(Nodes, CancelMsg),
    log(debug, "Sent cancel for job ~p to ~p nodes", [JobId, length(Nodes)]),

    NewJobs = maps:remove(JobId, Jobs),
    {noreply, State#state{dispatched_jobs = NewJobs}};

handle_cast({requeue_job, JobId}, #state{dispatched_jobs = Jobs} = State) ->
    %% Remove job from dispatched jobs tracking
    %% This allows the job to be re-dispatched when rescheduled
    log(info, "Requeuing job ~p - removing from dispatch tracking", [JobId]),
    NewJobs = maps:remove(JobId, Jobs),
    {noreply, State#state{dispatched_jobs = NewJobs}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

build_job_launch_message(JobId, JobInfo) ->
    WorkDir = maps:get(work_dir, JobInfo, <<"/tmp">>),
    %% Default output file is slurm-<jobid>.out in work_dir
    DefaultOut = iolist_to_binary([WorkDir, <<"/slurm-">>,
                                   integer_to_binary(JobId), <<".out">>]),
    StdOut = case maps:get(std_out, JobInfo, <<>>) of
        <<>> -> DefaultOut;
        Path -> Path
    end,

    %% Build environment variables map
    Env = #{
        <<"SLURM_JOB_ID">> => integer_to_binary(JobId),
        <<"SLURM_JOB_NAME">> => maps:get(name, JobInfo, <<"job">>),
        <<"SLURM_NTASKS">> => integer_to_binary(maps:get(num_tasks, JobInfo, 1)),
        <<"SLURM_CPUS_PER_TASK">> => integer_to_binary(maps:get(num_cpus, JobInfo, 1)),
        <<"SLURM_JOB_PARTITION">> => maps:get(partition, JobInfo, <<"default">>),
        <<"SLURM_SUBMIT_DIR">> => WorkDir
    },

    #{
        type => job_launch,
        payload => #{
            <<"job_id">> => JobId,
            <<"script">> => maps:get(script, JobInfo, <<>>),
            <<"working_dir">> => WorkDir,
            <<"environment">> => Env,
            <<"num_cpus">> => maps:get(num_cpus, JobInfo, 1),
            <<"memory_mb">> => maps:get(memory_mb, JobInfo, 1024),
            <<"time_limit">> => maps:get(time_limit, JobInfo, 3600),
            <<"user_id">> => maps:get(user_id, JobInfo, 0),
            <<"group_id">> => maps:get(group_id, JobInfo, 0),
            <<"std_out">> => StdOut,
            <<"std_err">> => maps:get(std_err, JobInfo, <<>>),
            <<"prolog">> => maps:get(prolog, JobInfo, <<>>),
            <<"epilog">> => maps:get(epilog, JobInfo, <<>>)
        }
    }.

log(Level, Fmt, Args) ->
    case Level of
        debug -> lager:debug(Fmt, Args);
        info -> lager:info(Fmt, Args);
        warning -> lager:warning(Fmt, Args);
        error -> lager:error(Fmt, Args)
    end.

%% @private
%% Convert signal atom to binary name for node communication
signal_to_name(sigterm) -> <<"SIGTERM">>;
signal_to_name(sigkill) -> <<"SIGKILL">>;
signal_to_name(sigstop) -> <<"SIGSTOP">>;
signal_to_name(sigcont) -> <<"SIGCONT">>;
signal_to_name(sighup) -> <<"SIGHUP">>;
signal_to_name(sigusr1) -> <<"SIGUSR1">>;
signal_to_name(sigusr2) -> <<"SIGUSR2">>;
signal_to_name(sigint) -> <<"SIGINT">>;
signal_to_name(Other) when is_atom(Other) ->
    list_to_binary(string:to_upper(atom_to_list(Other)));
signal_to_name(Num) when is_integer(Num) ->
    integer_to_binary(Num).
