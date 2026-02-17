%%%-------------------------------------------------------------------
%%% @doc FLURM Step Handler
%%%
%%% Handles step-related operations including:
%%% - Job step creation
%%% - Job step info queries
%%% - Prolog completion notifications
%%% - Epilog completion notifications
%%% - Task exit notifications
%%%
%%% Split from flurm_controller_handler.erl for maintainability.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_step).

-export([handle/2]).

%% Exported for use by main handler
-export([
    step_map_to_info/1,
    step_state_to_slurm/1,
    dispatch_step_to_nodes/3,
    build_step_launch_message/4,
    get_job_cred_info/1,
    get_first_registered_node/0,
    update_job_prolog_status/2,
    update_job_epilog_status/2,
    is_cluster_enabled/0
]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% Import helpers from main handler
-import(flurm_controller_handler, [
    ensure_binary/1,
    error_to_binary/1,
    default_time/1,
    format_allocated_nodes/1
]).

%%====================================================================
%% API
%%====================================================================

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
%% Internal Functions - Response Conversion
%%====================================================================

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
%% Internal Functions - Credential Info
%%====================================================================

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
