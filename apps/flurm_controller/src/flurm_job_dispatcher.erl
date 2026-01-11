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
-module(flurm_job_dispatcher).

-behaviour(gen_server).

-export([start_link/0]).
-export([
    dispatch_job/2,
    cancel_job/2,
    drain_node/1,
    resume_node/1
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

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

%% @doc Send drain signal to a node.
-spec drain_node(binary()) -> ok | {error, term()}.
drain_node(Hostname) when is_binary(Hostname) ->
    gen_server:call(?MODULE, {drain_node, Hostname}).

%% @doc Send resume signal to a node.
-spec resume_node(binary()) -> ok | {error, term()}.
resume_node(Hostname) when is_binary(Hostname) ->
    gen_server:call(?MODULE, {resume_node, Hostname}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    log(info, "Job Dispatcher started", []),
    {ok, #state{}}.

handle_call({dispatch_job, JobId, JobInfo}, _From, #state{dispatched_jobs = Jobs} = State) ->
    AllocatedNodes = maps:get(allocated_nodes, JobInfo, []),
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
                    log(info, "Dispatched job ~p to ~p nodes", [JobId, length(Succeeded)]),
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
    log(info, "Sent cancel for job ~p to ~p nodes", [JobId, length(Nodes)]),

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
    %% Build environment variables map
    Env = #{
        <<"SLURM_JOB_ID">> => integer_to_binary(JobId),
        <<"SLURM_JOB_NAME">> => maps:get(name, JobInfo, <<"job">>),
        <<"SLURM_NTASKS">> => integer_to_binary(maps:get(num_tasks, JobInfo, 1)),
        <<"SLURM_CPUS_PER_TASK">> => integer_to_binary(maps:get(num_cpus, JobInfo, 1)),
        <<"SLURM_JOB_PARTITION">> => maps:get(partition, JobInfo, <<"default">>)
    },

    #{
        type => job_launch,
        payload => #{
            <<"job_id">> => JobId,
            <<"script">> => maps:get(script, JobInfo, <<>>),
            <<"working_dir">> => maps:get(work_dir, JobInfo, <<"/tmp">>),
            <<"environment">> => Env,
            <<"num_cpus">> => maps:get(num_cpus, JobInfo, 1),
            <<"memory_mb">> => maps:get(memory_mb, JobInfo, 1024),
            <<"time_limit">> => maps:get(time_limit, JobInfo, 3600),
            <<"user_id">> => maps:get(user_id, JobInfo, 0),
            <<"group_id">> => maps:get(group_id, JobInfo, 0)
        }
    }.

log(Level, Fmt, Args) ->
    Msg = io_lib:format(Fmt, Args),
    case Level of
        debug -> ok;
        info -> error_logger:info_msg("[job_dispatcher] ~s~n", [Msg]);
        warning -> error_logger:warning_msg("[job_dispatcher] ~s~n", [Msg]);
        error -> error_logger:error_msg("[job_dispatcher] ~s~n", [Msg])
    end.
