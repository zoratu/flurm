%%%-------------------------------------------------------------------
%%% @doc FLURM Database Persistence Service
%%%
%%% Provides a unified API for job persistence that works with:
%%% - Ra consensus (when available, for distributed mode)
%%% - ETS fallback (for non-distributed/development mode)
%%%
%%% This module automatically detects which backend is available
%%% and routes operations appropriately.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_persist).

-include("flurm_db.hrl").
%% flurm_core.hrl comes through flurm_db.hrl

%% API
-export([
    %% Job operations
    store_job/1,
    update_job/2,
    get_job/1,
    delete_job/1,
    list_jobs/0,
    next_job_id/0,

    %% Check persistence mode
    is_ra_available/0,
    persistence_mode/0
]).

-ifdef(TEST).
-export([ra_job_to_job/1, apply_job_updates/2]).
-endif.

%%====================================================================
%% API
%%====================================================================

%% @doc Store a new job record.
-spec store_job(#job{}) -> ok | {error, term()}.
store_job(Job) when is_record(Job, job) ->
    case is_ra_available() of
        true ->
            store_job_ra(Job);
        false ->
            store_job_ets(Job)
    end.

%% @doc Update an existing job.
-spec update_job(job_id(), map()) -> ok | {error, not_found}.
update_job(JobId, Updates) when is_integer(JobId), is_map(Updates) ->
    case is_ra_available() of
        true ->
            update_job_ra(JobId, Updates);
        false ->
            update_job_ets(JobId, Updates)
    end.

%% @doc Get a job by ID.
-spec get_job(job_id()) -> {ok, #job{}} | {error, not_found}.
get_job(JobId) when is_integer(JobId) ->
    case is_ra_available() of
        true ->
            get_job_ra(JobId);
        false ->
            get_job_ets(JobId)
    end.

%% @doc Delete a job.
-spec delete_job(job_id()) -> ok.
delete_job(JobId) when is_integer(JobId) ->
    case is_ra_available() of
        true ->
            delete_job_ra(JobId);
        false ->
            delete_job_ets(JobId)
    end.

%% @doc List all jobs.
-spec list_jobs() -> [#job{}].
list_jobs() ->
    case is_ra_available() of
        true ->
            list_jobs_ra();
        false ->
            list_jobs_ets()
    end.

%% @doc Get the next job ID.
-spec next_job_id() -> job_id().
next_job_id() ->
    case is_ra_available() of
        true ->
            %% Ra handles job ID generation internally
            %% This is just used for ETS fallback
            next_job_id_ra();
        false ->
            next_job_id_ets()
    end.

%% @doc Check if Ra is available.
-spec is_ra_available() -> boolean().
is_ra_available() ->
    case node() of
        nonode@nohost ->
            false;
        _ ->
            %% Check if Ra cluster is running
            ServerId = {?RA_CLUSTER_NAME, node()},
            case catch ra:members(ServerId) of
                {ok, _, _} -> true;
                _ -> false
            end
    end.

%% @doc Get the current persistence mode.
-spec persistence_mode() -> ra | ets | none.
persistence_mode() ->
    case is_ra_available() of
        true -> ra;
        false ->
            case ets:whereis(flurm_db_jobs_ets) of
                undefined -> none;
                _ -> ets
            end
    end.

%%====================================================================
%% Ra Backend Implementation
%%====================================================================

store_job_ra(Job) ->
    %% Convert #job to #ra_job_spec for Ra submission
    JobSpec = #ra_job_spec{
        name = Job#job.name,
        user = Job#job.user,
        group = <<"users">>,
        partition = Job#job.partition,
        script = Job#job.script,
        num_nodes = Job#job.num_nodes,
        num_cpus = Job#job.num_cpus,
        memory_mb = Job#job.memory_mb,
        time_limit = Job#job.time_limit,
        priority = Job#job.priority
    },
    case flurm_db_ra:submit_job(JobSpec) of
        {ok, _JobId} -> ok;
        Error -> Error
    end.

update_job_ra(JobId, Updates) ->
    %% For state updates, use Ra's update_job_state
    case maps:find(state, Updates) of
        {ok, NewState} ->
            flurm_db_ra:update_job_state(JobId, NewState);
        error ->
            %% For other updates, we need to handle them differently
            %% Ra doesn't have a generic update, so we handle specific fields
            handle_ra_updates(JobId, Updates)
    end.

handle_ra_updates(JobId, Updates) ->
    %% Handle exit_code updates
    case maps:find(exit_code, Updates) of
        {ok, ExitCode} ->
            flurm_db_ra:set_job_exit_code(JobId, ExitCode);
        error ->
            ok
    end,
    %% Handle allocated_nodes updates
    case maps:find(allocated_nodes, Updates) of
        {ok, Nodes} ->
            flurm_db_ra:allocate_job(JobId, Nodes);
        error ->
            ok
    end,
    ok.

get_job_ra(JobId) ->
    case flurm_db_ra:get_job(JobId) of
        {ok, RaJob} ->
            {ok, ra_job_to_job(RaJob)};
        Error ->
            Error
    end.

delete_job_ra(_JobId) ->
    %% Ra doesn't support job deletion (jobs are immutable once completed)
    %% We could add a delete command to the state machine if needed
    ok.

list_jobs_ra() ->
    case flurm_db_ra:list_jobs() of
        {ok, RaJobs} ->
            [ra_job_to_job(J) || J <- RaJobs];
        _ ->
            []
    end.

next_job_id_ra() ->
    %% Allocate a job ID through Ra consensus.
    %% This ensures unique IDs across the distributed cluster by
    %% atomically incrementing the job counter in the replicated state.
    case flurm_db_ra:allocate_job_id() of
        {ok, JobId} -> JobId;
        {error, Reason} ->
            error_logger:error_msg("Failed to allocate job ID via Ra: ~p~n", [Reason]),
            %% Fallback to ETS if Ra fails (graceful degradation)
            next_job_id_ets()
    end.

%% Convert #ra_job to #job
ra_job_to_job(#ra_job{} = R) ->
    #job{
        id = R#ra_job.id,
        name = R#ra_job.name,
        user = R#ra_job.user,
        partition = R#ra_job.partition,
        state = R#ra_job.state,
        script = R#ra_job.script,
        num_nodes = R#ra_job.num_nodes,
        num_cpus = R#ra_job.num_cpus,
        memory_mb = R#ra_job.memory_mb,
        time_limit = R#ra_job.time_limit,
        priority = R#ra_job.priority,
        submit_time = R#ra_job.submit_time,
        start_time = R#ra_job.start_time,
        end_time = R#ra_job.end_time,
        allocated_nodes = R#ra_job.allocated_nodes,
        exit_code = R#ra_job.exit_code,
        %% Account and QOS fields - default values since ra_job does not have these yet
        account = <<>>,
        qos = <<"normal">>
    }.

%%====================================================================
%% ETS Backend Implementation (with DETS for disk persistence)
%%====================================================================

store_job_ets(Job) ->
    %% Write to ETS for fast reads
    ets:insert(flurm_db_jobs_ets, {Job#job.id, Job}),
    %% Also write to DETS for persistence
    persist_to_dets({Job#job.id, Job}),
    ok.

update_job_ets(JobId, Updates) ->
    case ets:lookup(flurm_db_jobs_ets, JobId) of
        [{JobId, Job}] ->
            UpdatedJob = apply_job_updates(Job, Updates),
            %% Write to ETS
            ets:insert(flurm_db_jobs_ets, {JobId, UpdatedJob}),
            %% Also write to DETS for persistence
            persist_to_dets({JobId, UpdatedJob}),
            ok;
        [] ->
            {error, not_found}
    end.

get_job_ets(JobId) ->
    case ets:lookup(flurm_db_jobs_ets, JobId) of
        [{JobId, Job}] ->
            {ok, Job};
        [] ->
            {error, not_found}
    end.

delete_job_ets(JobId) ->
    ets:delete(flurm_db_jobs_ets, JobId),
    %% Also delete from DETS
    delete_from_dets(JobId),
    ok.

list_jobs_ets() ->
    case ets:whereis(flurm_db_jobs_ets) of
        undefined -> [];
        _ -> [Job || {_Id, Job} <- ets:tab2list(flurm_db_jobs_ets)]
    end.

next_job_id_ets() ->
    Counter = ets:update_counter(flurm_db_job_counter_ets, counter, 1),
    %% Also update DETS counter
    persist_counter(Counter),
    Counter.

%% Persist job entry to DETS
persist_to_dets(Entry) ->
    case dets:info(flurm_db_jobs_dets) of
        undefined -> ok;
        _ ->
            dets:insert(flurm_db_jobs_dets, Entry),
            %% Sync immediately for safety
            dets:sync(flurm_db_jobs_dets)
    end.

%% Delete job from DETS
delete_from_dets(JobId) ->
    case dets:info(flurm_db_jobs_dets) of
        undefined -> ok;
        _ ->
            dets:delete(flurm_db_jobs_dets, JobId),
            dets:sync(flurm_db_jobs_dets)
    end.

%% Persist job counter to DETS
persist_counter(Counter) ->
    case dets:info(flurm_db_counter_dets) of
        undefined -> ok;
        _ ->
            dets:insert(flurm_db_counter_dets, {counter, Counter}),
            dets:sync(flurm_db_counter_dets)
    end.

%% Apply updates to a job record
apply_job_updates(Job, Updates) ->
    maps:fold(fun
        (state, Value, J) ->
            case Value of
                running when J#job.start_time =:= undefined ->
                    J#job{state = Value, start_time = erlang:system_time(second)};
                S when S =:= completed; S =:= failed; S =:= cancelled;
                       S =:= timeout; S =:= node_fail ->
                    J#job{state = Value, end_time = erlang:system_time(second)};
                _ ->
                    J#job{state = Value}
            end;
        (allocated_nodes, Value, J) -> J#job{allocated_nodes = Value};
        (start_time, Value, J) -> J#job{start_time = Value};
        (end_time, Value, J) -> J#job{end_time = Value};
        (exit_code, Value, J) -> J#job{exit_code = Value};
        (_, _, J) -> J
    end, Job, Updates).
