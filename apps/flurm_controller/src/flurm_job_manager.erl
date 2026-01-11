%%%-------------------------------------------------------------------
%%% @doc FLURM Job Manager
%%%
%%% Manages job lifecycle including submission, tracking, completion,
%%% and cancellation.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_manager).

-behaviour(gen_server).

-export([start_link/0]).
-export([submit_job/1, cancel_job/1, get_job/1, list_jobs/0, update_job/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("flurm_core/include/flurm_core.hrl").

-record(state, {
    jobs = #{} :: #{job_id() => #job{}}
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

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("Job Manager started"),
    {ok, #state{}}.

handle_call({submit_job, JobSpec}, _From, #state{jobs = Jobs} = State) ->
    Job = flurm_core:new_job(JobSpec),
    JobId = flurm_core:job_id(Job),
    lager:info("Job ~p submitted: ~p", [JobId, maps:get(name, JobSpec, <<"unnamed">>)]),
    NewJobs = maps:put(JobId, Job, Jobs),
    %% Notify scheduler about new job
    flurm_scheduler:submit_job(JobId),
    {reply, {ok, JobId}, State#state{jobs = NewJobs}};

handle_call({cancel_job, JobId}, _From, #state{jobs = Jobs} = State) ->
    case maps:find(JobId, Jobs) of
        {ok, Job} ->
            UpdatedJob = flurm_core:update_job_state(Job, cancelled),
            NewJobs = maps:put(JobId, UpdatedJob, Jobs),
            lager:info("Job ~p cancelled", [JobId]),
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
            {reply, ok, State#state{jobs = NewJobs}};
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

%% Apply updates to a job record
apply_job_updates(Job, Updates) ->
    maps:fold(fun
        (state, Value, J) -> flurm_core:update_job_state(J, Value);
        (allocated_nodes, Value, J) -> J#job{allocated_nodes = Value};
        (start_time, Value, J) -> J#job{start_time = Value};
        (end_time, Value, J) -> J#job{end_time = Value};
        (exit_code, Value, J) -> J#job{exit_code = Value};
        (_, _, J) -> J
    end, Job, Updates).
