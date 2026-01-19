%%%-------------------------------------------------------------------
%%% @doc FLURM Job Manager
%%%
%%% Manages job lifecycle including submission, tracking, completion,
%%% and cancellation. Integrates with flurm_db_persist for state
%%% persistence across controller restarts.
%%%
%%% The job manager maintains an in-memory cache for fast lookups,
%%% while persisting all changes to the underlying storage (Ra or ETS).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_manager).

-behaviour(gen_server).

-export([start_link/0]).
-export([submit_job/1, cancel_job/1, get_job/1, list_jobs/0, update_job/2]).
-export([hold_job/1, release_job/1, requeue_job/1]).
-export([import_job/1]).  %% For SLURM migration
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Test exports for internal functions
-ifdef(TEST).
-export([
    create_job/2,
    apply_job_updates/2,
    build_limit_check_spec/1
]).
-endif.

-include_lib("flurm_core/include/flurm_core.hrl").

%% Array task record for pattern matching (must match flurm_job_array)
-record(array_task, {
    id,
    array_job_id,
    task_id,
    job_id,
    state,
    exit_code,
    start_time,
    end_time,
    node
}).

-record(state, {
    jobs = #{} :: #{job_id() => #job{}},
    job_counter = 1 :: pos_integer(),
    persistence_mode = none :: ra | ets | none
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec submit_job(map()) -> {ok, job_id()} | {error, term()}.
submit_job(JobSpec) ->
    gen_server:call(?MODULE, {submit_job, JobSpec}).

-spec cancel_job(job_id()) -> ok | {error, term()}.
cancel_job(JobId) ->
    gen_server:call(?MODULE, {cancel_job, JobId}).

-spec get_job(job_id()) -> {ok, #job{}} | {error, not_found}.
get_job(JobId) ->
    gen_server:call(?MODULE, {get_job, JobId}).

-spec list_jobs() -> [#job{}].
list_jobs() ->
    gen_server:call(?MODULE, list_jobs).

-spec update_job(job_id(), map()) -> ok | {error, not_found}.
update_job(JobId, Updates) ->
    gen_server:call(?MODULE, {update_job, JobId, Updates}).

-spec hold_job(job_id()) -> ok | {error, term()}.
hold_job(JobId) ->
    gen_server:call(?MODULE, {hold_job, JobId}).

-spec release_job(job_id()) -> ok | {error, term()}.
release_job(JobId) ->
    gen_server:call(?MODULE, {release_job, JobId}).

-spec requeue_job(job_id()) -> ok | {error, term()}.
requeue_job(JobId) ->
    gen_server:call(?MODULE, {requeue_job, JobId}).

%% @doc Import a job from SLURM migration (preserves job ID)
-spec import_job(map()) -> {ok, job_id()} | {error, term()}.
import_job(JobSpec) ->
    gen_server:call(?MODULE, {import_job, JobSpec}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Load existing jobs from persistence on startup
    {Jobs, Counter, Mode} = load_persisted_jobs(),
    lager:info("Job Manager started (persistence: ~p, loaded ~p jobs)",
               [Mode, maps:size(Jobs)]),
    {ok, #state{jobs = Jobs, job_counter = Counter, persistence_mode = Mode}}.

handle_call({submit_job, JobSpec}, _From, #state{jobs = Jobs, job_counter = Counter} = State) ->
    %% Check if this is an array job submission
    case maps:get(array, JobSpec, undefined) of
        undefined ->
            %% Regular job submission
            submit_regular_job(JobSpec, Jobs, Counter, State);
        ArraySpec ->
            %% Array job submission
            submit_array_job(JobSpec, ArraySpec, Jobs, Counter, State)
    end;

%% Import a job from SLURM migration (preserves job ID)
handle_call({import_job, JobSpec}, _From, #state{jobs = Jobs, job_counter = Counter} = State) ->
    JobId = maps:get(id, JobSpec),
    case maps:is_key(JobId, Jobs) of
        true ->
            {reply, {error, already_exists}, State};
        false ->
            %% Create job record from import spec
            Job = #job{
                id = JobId,
                name = maps:get(name, JobSpec, <<"imported">>),
                user = maps:get(user, JobSpec, <<"unknown">>),
                partition = maps:get(partition, JobSpec, <<"default">>),
                num_cpus = maps:get(num_cpus, JobSpec, 1),
                num_nodes = maps:get(num_nodes, JobSpec, 1),
                memory_mb = maps:get(memory_mb, JobSpec, 256),
                priority = maps:get(priority, JobSpec, 100),
                state = maps:get(state, JobSpec, pending),
                time_limit = maps:get(time_limit, JobSpec, 60),
                submit_time = maps:get(submit_time, JobSpec, erlang:system_time(second)),
                start_time = maps:get(start_time, JobSpec, undefined)
            },
            NewJobs = maps:put(JobId, Job, Jobs),
            NewCounter = max(Counter, JobId + 1),
            lager:info("Imported job ~p from SLURM (state: ~p)", [JobId, Job#job.state]),
            persist_job(Job),
            {reply, {ok, JobId}, State#state{jobs = NewJobs, job_counter = NewCounter}}
    end;

handle_call({cancel_job, JobId}, _From, #state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, Job} ->
            %% Get allocated nodes before updating state
            AllocatedNodes = Job#job.allocated_nodes,

            %% Update job state to cancelled
            UpdatedJob = flurm_core:update_job_state(Job, cancelled),
            NewJobs = maps:put(JobId, UpdatedJob, Jobs),
            lager:info("Job ~p cancelled", [JobId]),

            %% Record metrics
            catch flurm_metrics:increment(flurm_jobs_cancelled_total),

            %% Persist the update
            persist_job_update(JobId, #{state => cancelled}),

            %% Send cancel to node daemon if job was running
            case AllocatedNodes of
                [] -> ok;
                Nodes ->
                    flurm_job_dispatcher_server:cancel_job(JobId, Nodes)
            end,

            %% Notify job dependencies module about cancellation
            %% This releases dependent jobs with afternotok/afterany dependencies
            notify_job_deps_state_change(JobId, cancelled),

            %% Clean up any dependencies this job had
            cleanup_job_dependencies(JobId),

            %% Notify scheduler to release resources
            flurm_scheduler:job_failed(JobId),

            {reply, ok, State#state{jobs = NewJobs}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_job, JobId}, _From, #state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, Job} ->
            {reply, {ok, Job}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(list_jobs, _From, #state{jobs = Jobs} = State) ->
    {reply, maps:values(Jobs), State};

handle_call({update_job, JobId, Updates}, _From, #state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, Job} ->
            UpdatedJob = apply_job_updates(Job, Updates),
            NewJobs = maps:put(JobId, UpdatedJob, Jobs),

            %% Record metrics for state changes and notify dependencies
            case maps:get(state, Updates, undefined) of
                completed ->
                    catch flurm_metrics:increment(flurm_jobs_completed_total),
                    %% Notify dependencies module about completion
                    notify_job_deps_state_change(JobId, completed),
                    %% Clean up this job's own dependencies
                    cleanup_job_dependencies(JobId);
                failed ->
                    catch flurm_metrics:increment(flurm_jobs_failed_total),
                    %% Notify dependencies module about failure
                    notify_job_deps_state_change(JobId, failed),
                    %% Clean up this job's own dependencies
                    cleanup_job_dependencies(JobId);
                timeout ->
                    %% Timeout is similar to failure for dependency purposes
                    notify_job_deps_state_change(JobId, timeout),
                    cleanup_job_dependencies(JobId);
                running ->
                    %% Job started running - satisfy 'after' dependencies
                    notify_job_deps_state_change(JobId, running);
                _ ->
                    ok
            end,

            %% Persist the update
            persist_job_update(JobId, Updates),

            {reply, ok, State#state{jobs = NewJobs}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({hold_job, JobId}, _From, #state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, #job{state = pending} = Job} ->
            %% Can only hold pending jobs
            UpdatedJob = Job#job{state = held},
            NewJobs = maps:put(JobId, UpdatedJob, Jobs),
            lager:info("Job ~p held", [JobId]),
            catch flurm_metrics:increment(flurm_jobs_held_total),
            persist_job_update(JobId, #{state => held}),
            {reply, ok, State#state{jobs = NewJobs}};
        {ok, #job{state = held}} ->
            %% Already held
            {reply, ok, State};
        {ok, #job{state = CurrentState}} ->
            lager:warning("Cannot hold job ~p in state ~p", [JobId, CurrentState]),
            {reply, {error, {invalid_state, CurrentState}}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({release_job, JobId}, _From, #state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, #job{state = held} = Job} ->
            %% Release held job back to pending
            UpdatedJob = Job#job{state = pending},
            NewJobs = maps:put(JobId, UpdatedJob, Jobs),
            lager:info("Job ~p released", [JobId]),
            persist_job_update(JobId, #{state => pending}),
            %% Notify scheduler about released job
            flurm_scheduler:submit_job(JobId),
            {reply, ok, State#state{jobs = NewJobs}};
        {ok, #job{state = pending}} ->
            %% Already pending (not held)
            {reply, ok, State};
        {ok, #job{state = CurrentState}} ->
            lager:warning("Cannot release job ~p in state ~p", [JobId, CurrentState]),
            {reply, {error, {invalid_state, CurrentState}}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({requeue_job, JobId}, _From, #state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, #job{state = running} = Job} ->
            %% Get allocated nodes to send cancel signal
            AllocatedNodes = Job#job.allocated_nodes,
            %% Move job back to pending
            UpdatedJob = Job#job{
                state = pending,
                allocated_nodes = [],
                start_time = undefined
            },
            NewJobs = maps:put(JobId, UpdatedJob, Jobs),
            lager:info("Job ~p requeued", [JobId]),
            catch flurm_metrics:increment(flurm_jobs_requeued_total),
            persist_job_update(JobId, #{state => pending, allocated_nodes => [], start_time => undefined}),
            %% Cancel on nodes
            case AllocatedNodes of
                [] -> ok;
                Nodes -> flurm_job_dispatcher_server:cancel_job(JobId, Nodes)
            end,
            %% Release resources and resubmit to scheduler
            flurm_scheduler:job_failed(JobId),
            flurm_scheduler:submit_job(JobId),
            {reply, ok, State#state{jobs = NewJobs}};
        {ok, #job{state = pending} = _Job} ->
            %% Already pending, nothing to do
            {reply, ok, State};
        {ok, #job{state = CurrentState}} ->
            lager:warning("Cannot requeue job ~p in state ~p", [JobId, CurrentState]),
            {reply, {error, {invalid_state, CurrentState}}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% Submit a regular (non-array) job
submit_regular_job(JobSpec, Jobs, Counter, State) ->
    %% First, parse and validate licenses if specified
    LicenseResult = case maps:get(licenses, JobSpec, <<>>) of
        <<>> -> {ok, []};
        LicenseSpec when is_binary(LicenseSpec) ->
            case flurm_license:parse_license_spec(LicenseSpec) of
                {ok, ParsedLicenses} ->
                    %% Validate that all requested licenses exist
                    case flurm_license:validate_licenses(ParsedLicenses) of
                        ok -> {ok, ParsedLicenses};
                        {error, LicErr} -> {error, LicErr}
                    end;
                {error, ParseErr} ->
                    {error, ParseErr}
            end;
        LicenseList when is_list(LicenseList) ->
            %% Already parsed, just validate
            case flurm_license:validate_licenses(LicenseList) of
                ok -> {ok, LicenseList};
                {error, LicErr} -> {error, LicErr}
            end
    end,
    case LicenseResult of
        {error, LicenseError} ->
            lager:warning("Job submission rejected due to invalid licenses: ~p", [LicenseError]),
            catch flurm_metrics:increment(flurm_jobs_rejected_limits_total),
            {reply, {error, {invalid_licenses, LicenseError}}, State};
        {ok, Licenses} ->
            %% Licenses OK, continue with limit check
            LimitCheckSpec = build_limit_check_spec(JobSpec),
            case flurm_limits:check_submit_limits(LimitCheckSpec) of
                ok ->
                    %% Check for dependency specification and validate before creating job
                    DepSpec = maps:get(dependency, JobSpec, <<>>),
                    case validate_dependencies(Counter, DepSpec) of
                        ok ->
                            %% Dependencies valid, create the job with the next available ID
                            JobSpecWithLicenses = JobSpec#{licenses => Licenses},
                            Job = create_job(Counter, JobSpecWithLicenses),
                            JobId = Job#job.id,
                            lager:info("Job ~p submitted: ~p (licenses: ~p)",
                                       [JobId, maps:get(name, JobSpec, <<"unnamed">>), Licenses]),

                            %% Record metrics
                            catch flurm_metrics:increment(flurm_jobs_submitted_total),

                            %% Persist the job
                            persist_job(Job),

                            %% Update in-memory cache
                            NewJobs = maps:put(JobId, Job, Jobs),

                            %% Register dependencies with flurm_job_deps
                            HasDeps = register_job_dependencies(JobId, DepSpec),

                            %% Notify scheduler about new job (unless held for dependencies)
                            case HasDeps of
                                true ->
                                    %% Job has dependencies - it will be held
                                    %% and released when deps are satisfied
                                    lager:info("Job ~p has dependencies, held pending", [JobId]),
                                    %% Still submit to scheduler queue but it will be skipped
                                    %% until dependencies are satisfied
                                    flurm_scheduler:submit_job(JobId);
                                false ->
                                    %% No dependencies, submit to scheduler immediately
                                    flurm_scheduler:submit_job(JobId)
                            end,

                            {reply, {ok, JobId}, State#state{jobs = NewJobs, job_counter = Counter + 1}};
                        {error, DepError} ->
                            %% Dependency validation failed (e.g., circular dependency)
                            lager:warning("Job submission rejected due to invalid dependencies: ~p", [DepError]),
                            catch flurm_metrics:increment(flurm_jobs_rejected_deps_total),
                            {reply, {error, {invalid_dependency, DepError}}, State}
                    end;
                {error, LimitReason} ->
                    %% Submit limits exceeded, reject the job
                    lager:warning("Job submission rejected due to limits: ~p", [LimitReason]),
                    catch flurm_metrics:increment(flurm_jobs_rejected_limits_total),
                    {reply, {error, {submit_limit_exceeded, LimitReason}}, State}
            end
    end.

%% Submit an array job - creates the array job record and individual task jobs
submit_array_job(JobSpec, ArraySpec, Jobs, Counter, State) ->
    LimitCheckSpec = build_limit_check_spec(JobSpec),
    case flurm_limits:check_submit_limits(LimitCheckSpec) of
        ok ->
            %% Parse the array spec if it's a binary string
            case flurm_job_array:parse_array_spec(ArraySpec) of
                {ok, ParsedSpec} ->
                    %% Create the base job template
                    BaseJob = create_job(Counter, JobSpec),

                    %% Create the array job via flurm_job_array
                    case flurm_job_array:create_array_job(BaseJob, ParsedSpec) of
                        {ok, ArrayJobId} ->
                            lager:info("Array job ~p submitted: ~p (base job ~p)",
                                       [ArrayJobId, maps:get(name, JobSpec, <<"unnamed">>), Counter]),

                            %% Record metrics
                            catch flurm_metrics:increment(flurm_jobs_submitted_total),
                            catch flurm_metrics:increment(flurm_array_jobs_submitted_total),

                            %% Get array tasks and create individual jobs for scheduling
                            ArrayTasks = flurm_job_array:get_schedulable_tasks(ArrayJobId),
                            {NewJobs, NewCounter} = create_array_task_jobs(
                                ArrayJobId, BaseJob, ArrayTasks, Jobs, Counter + 1
                            ),

                            %% Return the array job ID (formatted as "ArrayJobId_0" style for SLURM compat)
                            {reply, {ok, {array, ArrayJobId}},
                             State#state{jobs = NewJobs, job_counter = NewCounter}};
                        {error, Reason} ->
                            lager:error("Failed to create array job: ~p", [Reason]),
                            {reply, {error, {array_creation_failed, Reason}}, State}
                    end;
                {error, ParseError} ->
                    lager:error("Invalid array spec ~p: ~p", [ArraySpec, ParseError]),
                    {reply, {error, {invalid_array_spec, ParseError}}, State}
            end;
        {error, LimitReason} ->
            lager:warning("Array job submission rejected due to limits: ~p", [LimitReason]),
            catch flurm_metrics:increment(flurm_jobs_rejected_limits_total),
            {reply, {error, {submit_limit_exceeded, LimitReason}}, State}
    end.

%% Create individual jobs for array tasks that are schedulable
create_array_task_jobs(ArrayJobId, BaseJob, Tasks, Jobs, Counter) ->
    lists:foldl(
        fun(Task, {AccJobs, AccCounter}) ->
            TaskId = Task#array_task.task_id,

            %% Modify job name to include array task ID (SLURM style: jobname_taskid)
            TaskName = iolist_to_binary([
                BaseJob#job.name, <<"_">>, integer_to_binary(TaskId)
            ]),

            %% Create the task job
            %% Note: Array environment variables (SLURM_ARRAY_*) are injected
            %% by the job dispatcher using flurm_job_array:get_task_env/2
            TaskJob = BaseJob#job{
                id = AccCounter,
                name = TaskName
            },

            %% Update task with the job ID (task remains pending until actually scheduled)
            flurm_job_array:update_task_state(ArrayJobId, TaskId, #{
                job_id => AccCounter
            }),

            %% Persist and register the job
            persist_job(TaskJob),
            NewAccJobs = maps:put(AccCounter, TaskJob, AccJobs),

            %% Notify scheduler
            flurm_scheduler:submit_job(AccCounter),

            {NewAccJobs, AccCounter + 1}
        end,
        {Jobs, Counter},
        Tasks
    ).

%% Load persisted jobs from storage on startup
load_persisted_jobs() ->
    %% Wait briefly for persistence layer to initialize
    timer:sleep(100),

    Mode = flurm_db_persist:persistence_mode(),
    case Mode of
        none ->
            lager:info("No persistence available, starting with empty state"),
            {#{}, 1, none};
        _ ->
            Jobs = flurm_db_persist:list_jobs(),
            JobMap = lists:foldl(fun(Job, Acc) ->
                maps:put(Job#job.id, Job, Acc)
            end, #{}, Jobs),

            %% Calculate next job counter from existing jobs
            Counter = case Jobs of
                [] -> 1;
                _ ->
                    MaxId = lists:max([J#job.id || J <- Jobs]),
                    MaxId + 1
            end,

            lager:info("Loaded ~p jobs from persistence (mode: ~p)",
                       [length(Jobs), Mode]),
            {JobMap, Counter, Mode}
    end.

%% Create a new job record
create_job(JobId, JobSpec) ->
    Now = erlang:system_time(second),
    WorkDir = maps:get(work_dir, JobSpec, <<"/tmp">>),
    %% Default output file is slurm-<jobid>.out in work_dir
    DefaultOut = iolist_to_binary([WorkDir, <<"/slurm-">>,
                                   integer_to_binary(JobId), <<".out">>]),
    StdOut = case maps:get(std_out, JobSpec, <<>>) of
        <<>> -> DefaultOut;
        Path -> Path
    end,
    #job{
        id = JobId,
        name = maps:get(name, JobSpec, <<"unnamed">>),
        user = maps:get(user, JobSpec, <<"unknown">>),
        partition = maps:get(partition, JobSpec, <<"default">>),
        state = pending,
        script = maps:get(script, JobSpec, <<>>),
        num_nodes = maps:get(num_nodes, JobSpec, 1),
        num_cpus = maps:get(num_cpus, JobSpec, 1),
        memory_mb = maps:get(memory_mb, JobSpec, 1024),
        time_limit = maps:get(time_limit, JobSpec, 3600),
        priority = maps:get(priority, JobSpec, 100),
        submit_time = Now,
        start_time = undefined,
        end_time = undefined,
        allocated_nodes = [],
        exit_code = undefined,
        work_dir = WorkDir,
        std_out = StdOut,
        std_err = maps:get(std_err, JobSpec, <<>>),  % Empty = merge with stdout
        account = maps:get(account, JobSpec, <<>>),
        qos = maps:get(qos, JobSpec, <<"normal">>),
        licenses = maps:get(licenses, JobSpec, [])
    }.

%% Persist a new job
persist_job(Job) ->
    case flurm_db_persist:persistence_mode() of
        none -> ok;
        _ ->
            case flurm_db_persist:store_job(Job) of
                ok -> ok;
                {error, Reason} ->
                    lager:error("Failed to persist job ~p: ~p",
                               [Job#job.id, Reason])
            end
    end.

%% Persist a job update
persist_job_update(JobId, Updates) ->
    case flurm_db_persist:persistence_mode() of
        none -> ok;
        _ ->
            case flurm_db_persist:update_job(JobId, Updates) of
                ok -> ok;
                {error, Reason} ->
                    lager:error("Failed to persist job ~p update: ~p",
                               [JobId, Reason])
            end
    end.

%% Apply updates to a job record
apply_job_updates(Job, Updates) ->
    maps:fold(fun
        (state, Value, J) -> J#job{state = Value};
        (allocated_nodes, Value, J) -> J#job{allocated_nodes = Value};
        (start_time, Value, J) -> J#job{start_time = Value};
        (end_time, Value, J) -> J#job{end_time = Value};
        (exit_code, Value, J) -> J#job{exit_code = Value};
        (priority, Value, J) -> J#job{priority = Value};
        (time_limit, Value, J) -> J#job{time_limit = Value};
        (_, _, J) -> J
    end, Job, Updates).

%% Build a limit check spec from job spec for flurm_limits:check_submit_limits/1
%% The spec must include user, account, partition, and resource requirements
build_limit_check_spec(JobSpec) ->
    #{
        user => maps:get(user, JobSpec, maps:get(user_id, JobSpec, <<"unknown">>)),
        account => maps:get(account, JobSpec, <<>>),
        partition => maps:get(partition, JobSpec, <<"default">>),
        num_nodes => maps:get(num_nodes, JobSpec, 1),
        num_cpus => maps:get(num_cpus, JobSpec, 1),
        memory_mb => maps:get(memory_mb, JobSpec, 1024),
        time_limit => maps:get(time_limit, JobSpec, 3600)
    }.

%%====================================================================
%% Job Dependency Handling
%%====================================================================

%% @private
%% Validate dependencies before job submission
%% Checks for circular dependencies and that target jobs exist (for non-singleton deps)
%% Returns ok if valid, {error, Reason} if invalid
-spec validate_dependencies(job_id(), binary()) -> ok | {error, term()}.
validate_dependencies(_JobId, <<>>) ->
    ok;
validate_dependencies(JobId, DepSpec) when is_binary(DepSpec) ->
    case catch flurm_job_deps:parse_dependency_spec(DepSpec) of
        {ok, []} ->
            ok;
        {ok, Deps} ->
            %% Check each dependency
            validate_dependency_list(JobId, Deps);
        {error, ParseError} ->
            {error, {parse_error, ParseError}};
        {'EXIT', {noproc, _}} ->
            %% flurm_job_deps not running, skip validation
            ok;
        {'EXIT', Reason} ->
            {error, {validation_error, Reason}}
    end.

%% @private
%% Validate a list of parsed dependencies
validate_dependency_list(_JobId, []) ->
    ok;
validate_dependency_list(JobId, [{DepType, Target} | Rest]) ->
    case validate_single_dependency(JobId, DepType, Target) of
        ok ->
            validate_dependency_list(JobId, Rest);
        {error, _} = Error ->
            Error
    end.

%% @private
%% Validate a single dependency
%% Checks for circular dependencies and that target job exists
validate_single_dependency(_JobId, singleton, _Name) ->
    %% Singleton dependencies are always valid at submission time
    ok;
validate_single_dependency(JobId, _DepType, TargetJobId) when is_integer(TargetJobId) ->
    %% Check that target job exists
    case get_job(TargetJobId) of
        {ok, _Job} ->
            %% Target exists, check for circular dependency
            case catch flurm_job_deps:has_circular_dependency(JobId, TargetJobId) of
                true ->
                    {error, {circular_dependency, [JobId, TargetJobId]}};
                false ->
                    ok;
                {'EXIT', _} ->
                    %% flurm_job_deps not available, skip cycle check
                    ok
            end;
        {error, not_found} ->
            %% Target job doesn't exist
            {error, {dependency_not_found, TargetJobId}}
    end;
validate_single_dependency(JobId, DepType, Targets) when is_list(Targets) ->
    %% Multiple targets (job+job+job syntax)
    Results = [validate_single_dependency(JobId, DepType, T) || T <- Targets],
    case lists:filter(fun(R) -> R =/= ok end, Results) of
        [] -> ok;
        [Error | _] -> Error
    end.

%% @private
%% Register job dependencies with flurm_job_deps
%% Returns true if job has dependencies (and should be held), false otherwise
-spec register_job_dependencies(job_id(), binary()) -> boolean().
register_job_dependencies(_JobId, <<>>) ->
    false;
register_job_dependencies(JobId, DepSpec) when is_binary(DepSpec) ->
    case catch flurm_job_deps:add_dependencies(JobId, DepSpec) of
        ok ->
            %% Check if there are unsatisfied dependencies
            case catch flurm_job_deps:check_dependencies(JobId) of
                {ok, []} ->
                    %% All dependencies already satisfied
                    lager:info("Job ~p dependencies already satisfied", [JobId]),
                    false;
                {waiting, Deps} ->
                    lager:info("Job ~p waiting on ~p dependencies", [JobId, length(Deps)]),
                    true;
                {'EXIT', _} ->
                    false
            end;
        {error, Reason} ->
            lager:warning("Failed to register dependencies for job ~p: ~p", [JobId, Reason]),
            false;
        {'EXIT', {noproc, _}} ->
            %% flurm_job_deps not running
            lager:warning("flurm_job_deps not running, ignoring dependencies for job ~p", [JobId]),
            false;
        {'EXIT', Reason} ->
            lager:warning("Error registering dependencies for job ~p: ~p", [JobId, Reason]),
            false
    end.

%% @private
%% Notify flurm_job_deps about a job state change
%% This triggers dependency resolution for waiting jobs
-spec notify_job_deps_state_change(job_id(), atom()) -> ok.
notify_job_deps_state_change(JobId, NewState) ->
    case catch flurm_job_deps:on_job_state_change(JobId, NewState) of
        ok -> ok;
        {'EXIT', {noproc, _}} ->
            %% flurm_job_deps not running, ignore
            ok;
        {'EXIT', Reason} ->
            lager:warning("Failed to notify job deps state change for job ~p: ~p",
                         [JobId, Reason]),
            ok
    end.

%% @private
%% Clean up dependencies when a job completes/fails/is cancelled
%% Removes all dependencies this job had on other jobs
-spec cleanup_job_dependencies(job_id()) -> ok.
cleanup_job_dependencies(JobId) ->
    case catch flurm_job_deps:remove_all_dependencies(JobId) of
        ok -> ok;
        {'EXIT', {noproc, _}} ->
            %% flurm_job_deps not running, ignore
            ok;
        {'EXIT', Reason} ->
            lager:warning("Failed to cleanup dependencies for job ~p: ~p",
                         [JobId, Reason]),
            ok
    end.
