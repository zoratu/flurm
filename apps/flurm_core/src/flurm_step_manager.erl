%%%-------------------------------------------------------------------
%%% @doc FLURM Job Step Manager
%%%
%%% Manages job steps within jobs. A job step represents a unit of work
%%% within a job - for example, each `srun` command within an `sbatch`
%%% script creates a new step.
%%%
%%% Steps have their own lifecycle:
%%% - pending: Step created, waiting for resources
%%% - running: Step is executing
%%% - completing: Step is finishing up
%%% - completed: Step finished successfully
%%% - failed: Step failed
%%% - cancelled: Step was cancelled
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_step_manager).

-behaviour(gen_server).

-export([start_link/0]).
-export([
    create_step/2,
    get_step/2,
    list_steps/1,
    update_step/3,
    complete_step/3,
    cancel_step/2
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Test exports - internal helpers for direct callback testing
-ifdef(TEST).
-export([
    step_to_map/1,
    apply_step_updates/2
]).
-endif.

-include_lib("flurm_core/include/flurm_core.hrl").

-record(step, {
    job_id :: pos_integer(),
    step_id :: non_neg_integer(),
    name :: binary(),
    state :: pending | running | completing | completed | failed | cancelled,
    num_tasks :: pos_integer(),
    num_nodes :: pos_integer(),
    allocated_nodes :: [binary()],
    start_time :: integer() | undefined,
    end_time :: integer() | undefined,
    exit_code :: integer() | undefined,
    command :: binary()
}).

-record(state, {
    steps = #{} :: #{job_step_key() => #step{}},
    step_counters = #{} :: #{pos_integer() => non_neg_integer()}
}).

-type job_step_key() :: {pos_integer(), non_neg_integer()}.

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Create a new step within a job.
-spec create_step(pos_integer(), map()) -> {ok, non_neg_integer()} | {error, term()}.
create_step(JobId, StepSpec) ->
    gen_server:call(?MODULE, {create_step, JobId, StepSpec}).

%% @doc Get a specific step.
-spec get_step(pos_integer(), non_neg_integer()) -> {ok, map()} | {error, not_found}.
get_step(JobId, StepId) ->
    gen_server:call(?MODULE, {get_step, JobId, StepId}).

%% @doc List all steps for a job.
-spec list_steps(pos_integer()) -> [map()].
list_steps(JobId) ->
    gen_server:call(?MODULE, {list_steps, JobId}).

%% @doc Update a step's state.
-spec update_step(pos_integer(), non_neg_integer(), map()) -> ok | {error, not_found}.
update_step(JobId, StepId, Updates) ->
    gen_server:call(?MODULE, {update_step, JobId, StepId, Updates}).

%% @doc Mark a step as completed.
-spec complete_step(pos_integer(), non_neg_integer(), integer()) -> ok | {error, not_found}.
complete_step(JobId, StepId, ExitCode) ->
    gen_server:call(?MODULE, {complete_step, JobId, StepId, ExitCode}).

%% @doc Cancel a step.
-spec cancel_step(pos_integer(), non_neg_integer()) -> ok | {error, not_found}.
cancel_step(JobId, StepId) ->
    gen_server:call(?MODULE, {cancel_step, JobId, StepId}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("Step Manager started"),
    {ok, #state{}}.

handle_call({create_step, JobId, StepSpec}, _From, State) ->
    %% Get next step ID for this job
    Counter = maps:get(JobId, State#state.step_counters, 0),
    StepId = Counter,

    Step = #step{
        job_id = JobId,
        step_id = StepId,
        name = maps:get(name, StepSpec, <<"step">>),
        state = pending,
        num_tasks = maps:get(num_tasks, StepSpec, 1),
        num_nodes = maps:get(num_nodes, StepSpec, 1),
        allocated_nodes = [],
        start_time = undefined,
        end_time = undefined,
        exit_code = undefined,
        command = maps:get(command, StepSpec, <<>>)
    },

    Key = {JobId, StepId},
    NewSteps = maps:put(Key, Step, State#state.steps),
    NewCounters = maps:put(JobId, Counter + 1, State#state.step_counters),

    lager:info("Created step ~p.~p", [JobId, StepId]),
    {reply, {ok, StepId}, State#state{steps = NewSteps, step_counters = NewCounters}};

handle_call({get_step, JobId, StepId}, _From, State) ->
    Key = {JobId, StepId},
    case maps:find(Key, State#state.steps) of
        {ok, Step} ->
            {reply, {ok, step_to_map(Step)}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({list_steps, JobId}, _From, State) ->
    Steps = [step_to_map(S) || {K, S} <- maps:to_list(State#state.steps),
                               element(1, K) == JobId],
    {reply, Steps, State};

handle_call({update_step, JobId, StepId, Updates}, _From, State) ->
    Key = {JobId, StepId},
    case maps:find(Key, State#state.steps) of
        {ok, Step} ->
            UpdatedStep = apply_step_updates(Step, Updates),
            NewSteps = maps:put(Key, UpdatedStep, State#state.steps),
            {reply, ok, State#state{steps = NewSteps}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({complete_step, JobId, StepId, ExitCode}, _From, State) ->
    Key = {JobId, StepId},
    case maps:find(Key, State#state.steps) of
        {ok, Step} ->
            Now = erlang:system_time(second),
            UpdatedStep = Step#step{
                state = completed,
                end_time = Now,
                exit_code = ExitCode
            },
            NewSteps = maps:put(Key, UpdatedStep, State#state.steps),
            lager:info("Step ~p.~p completed with exit code ~p", [JobId, StepId, ExitCode]),
            {reply, ok, State#state{steps = NewSteps}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({cancel_step, JobId, StepId}, _From, State) ->
    Key = {JobId, StepId},
    case maps:find(Key, State#state.steps) of
        {ok, Step} ->
            Now = erlang:system_time(second),
            UpdatedStep = Step#step{
                state = cancelled,
                end_time = Now
            },
            NewSteps = maps:put(Key, UpdatedStep, State#state.steps),
            lager:info("Step ~p.~p cancelled", [JobId, StepId]),
            {reply, ok, State#state{steps = NewSteps}};
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

step_to_map(#step{} = S) ->
    #{
        job_id => S#step.job_id,
        step_id => S#step.step_id,
        name => S#step.name,
        state => S#step.state,
        num_tasks => S#step.num_tasks,
        num_nodes => S#step.num_nodes,
        allocated_nodes => S#step.allocated_nodes,
        start_time => S#step.start_time,
        end_time => S#step.end_time,
        exit_code => S#step.exit_code,
        command => S#step.command
    }.

apply_step_updates(Step, Updates) ->
    maps:fold(fun
        (state, Value, S) -> S#step{state = Value};
        (allocated_nodes, Value, S) -> S#step{allocated_nodes = Value};
        (start_time, Value, S) -> S#step{start_time = Value};
        (end_time, Value, S) -> S#step{end_time = Value};
        (exit_code, Value, S) -> S#step{exit_code = Value};
        (_, _, S) -> S
    end, Step, Updates).
