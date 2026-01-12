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
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("flurm_core/include/flurm_core.hrl").

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
    %% Check submit limits before accepting the job
    LimitCheckSpec = build_limit_check_spec(JobSpec),
    case flurm_limits:check_submit_limits(LimitCheckSpec) of
        ok ->
            %% Limits OK, create the job with the next available ID
            Job = create_job(Counter, JobSpec),
            JobId = Job#job.id,
            lager:info("Job ~p submitted: ~p", [JobId, maps:get(name, JobSpec, <<"unnamed">>)]),

            %% Record metrics
            catch flurm_metrics:increment(flurm_jobs_submitted_total),

            %% Persist the job
            persist_job(Job),

            %% Update in-memory cache
            NewJobs = maps:put(JobId, Job, Jobs),

            %% Notify scheduler about new job
            flurm_scheduler:submit_job(JobId),

            {reply, {ok, JobId}, State#state{jobs = NewJobs, job_counter = Counter + 1}};
        {error, LimitReason} ->
            %% Submit limits exceeded, reject the job
            lager:warning("Job submission rejected due to limits: ~p", [LimitReason]),
            catch flurm_metrics:increment(flurm_jobs_rejected_limits_total),
            {reply, {error, {submit_limit_exceeded, LimitReason}}, State}
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
                    flurm_job_dispatcher:cancel_job(JobId, Nodes)
            end,

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

            %% Record metrics for state changes
            case maps:get(state, Updates, undefined) of
                completed ->
                    catch flurm_metrics:increment(flurm_jobs_completed_total);
                failed ->
                    catch flurm_metrics:increment(flurm_jobs_failed_total);
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
                Nodes -> flurm_job_dispatcher:cancel_job(JobId, Nodes)
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
        std_err = maps:get(std_err, JobSpec, <<>>)  % Empty = merge with stdout
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
