%%%-------------------------------------------------------------------
%%% @doc FLURM Job Registry
%%%
%%% A gen_server that maintains a registry of all job processes in
%%% the system. Uses ETS for fast lookups by job_id, user_id, and
%%% state.
%%%
%%% The registry monitors job processes and automatically removes
%%% them when they terminate.
%%%
%%% ETS Tables:
%%%   flurm_jobs_by_id    - Primary lookup by job_id
%%%   flurm_jobs_by_user  - Bag indexed by user_id
%%%   flurm_jobs_by_state - Bag indexed by state
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_registry).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    register_job/2,
    unregister_job/1,
    lookup_job/1,
    list_jobs/0,
    list_jobs_by_state/1,
    list_jobs_by_user/1,
    update_state/2,
    get_job_entry/1,
    count_by_state/0,
    count_by_user/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

%% ETS table names
-define(JOBS_BY_ID, flurm_jobs_by_id).
-define(JOBS_BY_USER, flurm_jobs_by_user).
-define(JOBS_BY_STATE, flurm_jobs_by_state).

%% Internal state
-record(state, {
    monitors = #{} :: #{reference() => job_id()}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the job registry.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    case gen_server:start_link({local, ?SERVER}, ?MODULE, [], []) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            %% Process already running - return existing pid
            %% This handles race conditions during startup and restarts
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Register a job with its pid.
-spec register_job(job_id(), pid()) -> ok | {error, already_registered}.
register_job(JobId, Pid) when is_integer(JobId), is_pid(Pid) ->
    gen_server:call(?SERVER, {register, JobId, Pid}).

%% @doc Unregister a job.
-spec unregister_job(job_id()) -> ok.
unregister_job(JobId) when is_integer(JobId) ->
    gen_server:call(?SERVER, {unregister, JobId}).

%% @doc Lookup a job pid by job_id.
%% This is a direct ETS lookup for performance.
-spec lookup_job(job_id()) -> {ok, pid()} | {error, not_found}.
lookup_job(JobId) when is_integer(JobId) ->
    case ets:lookup(?JOBS_BY_ID, JobId) of
        [#job_entry{pid = Pid}] -> {ok, Pid};
        [] -> {error, not_found}
    end.

%% @doc Get full job entry by job_id.
-spec get_job_entry(job_id()) -> {ok, #job_entry{}} | {error, not_found}.
get_job_entry(JobId) when is_integer(JobId) ->
    case ets:lookup(?JOBS_BY_ID, JobId) of
        [Entry] -> {ok, Entry};
        [] -> {error, not_found}
    end.

%% @doc List all registered jobs.
%% Returns a list of {job_id, pid} tuples.
-spec list_jobs() -> [{job_id(), pid()}].
list_jobs() ->
    ets:foldl(
        fun(#job_entry{job_id = Id, pid = Pid}, Acc) ->
            [{Id, Pid} | Acc]
        end,
        [],
        ?JOBS_BY_ID
    ).

%% @doc List jobs by state.
-spec list_jobs_by_state(job_state()) -> [{job_id(), pid()}].
list_jobs_by_state(State) when is_atom(State) ->
    case ets:lookup(?JOBS_BY_STATE, State) of
        [] -> [];
        Results ->
            [{JobId, Pid} || {_, JobId, Pid} <- Results]
    end.

%% @doc List jobs by user.
-spec list_jobs_by_user(user_id()) -> [{job_id(), pid()}].
list_jobs_by_user(UserId) when is_integer(UserId) ->
    case ets:lookup(?JOBS_BY_USER, UserId) of
        [] -> [];
        Results ->
            [{JobId, Pid} || {_, JobId, Pid} <- Results]
    end.

%% @doc Update the state of a job in the registry.
-spec update_state(job_id(), job_state()) -> ok | {error, not_found}.
update_state(JobId, NewState) when is_integer(JobId), is_atom(NewState) ->
    gen_server:call(?SERVER, {update_state, JobId, NewState}).

%% @doc Get count of jobs by state.
-spec count_by_state() -> #{job_state() => non_neg_integer()}.
count_by_state() ->
    States = [pending, configuring, running, completing,
              completed, cancelled, failed, timeout, node_fail],
    maps:from_list([{S, length(list_jobs_by_state(S))} || S <- States]).

%% @doc Get count of jobs by user.
-spec count_by_user() -> #{user_id() => non_neg_integer()}.
count_by_user() ->
    ets:foldl(
        fun(#job_entry{user_id = UserId}, Acc) ->
            maps:update_with(UserId, fun(V) -> V + 1 end, 1, Acc)
        end,
        #{},
        ?JOBS_BY_ID
    ).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init([]) ->
    %% Create ETS tables
    %% Primary table: set indexed by job_id
    ets:new(?JOBS_BY_ID, [
        named_table,
        set,
        public,
        {keypos, #job_entry.job_id},
        {read_concurrency, true}
    ]),
    %% Secondary index: bag by user_id
    ets:new(?JOBS_BY_USER, [
        named_table,
        bag,
        public,
        {read_concurrency, true}
    ]),
    %% Secondary index: bag by state
    ets:new(?JOBS_BY_STATE, [
        named_table,
        bag,
        public,
        {read_concurrency, true}
    ]),
    {ok, #state{}}.

%% @private
handle_call({register, JobId, Pid}, _From, State) ->
    case ets:lookup(?JOBS_BY_ID, JobId) of
        [_] ->
            {reply, {error, already_registered}, State};
        [] ->
            %% Get job info from the process
            {ok, Info} = flurm_job:get_info(Pid),
            UserId = maps:get(user_id, Info),
            Partition = maps:get(partition, Info),
            SubmitTime = maps:get(submit_time, Info),
            CurrentState = maps:get(state, Info),

            %% Create entry
            Entry = #job_entry{
                job_id = JobId,
                pid = Pid,
                user_id = UserId,
                state = CurrentState,
                partition = Partition,
                submit_time = SubmitTime
            },

            %% Insert into all tables
            ets:insert(?JOBS_BY_ID, Entry),
            ets:insert(?JOBS_BY_USER, {UserId, JobId, Pid}),
            ets:insert(?JOBS_BY_STATE, {CurrentState, JobId, Pid}),

            %% Monitor the process
            MonRef = erlang:monitor(process, Pid),
            NewMonitors = maps:put(MonRef, JobId, State#state.monitors),

            {reply, ok, State#state{monitors = NewMonitors}}
    end;

handle_call({unregister, JobId}, _From, State) ->
    NewState = do_unregister(JobId, State),
    {reply, ok, NewState};

handle_call({update_state, JobId, NewState}, _From, State) ->
    case ets:lookup(?JOBS_BY_ID, JobId) of
        [#job_entry{state = OldState, pid = Pid, user_id = _UserId} = Entry] ->
            %% Update main table
            NewEntry = Entry#job_entry{state = NewState},
            ets:insert(?JOBS_BY_ID, NewEntry),

            %% Update state index - remove old, add new
            ets:delete_object(?JOBS_BY_STATE, {OldState, JobId, Pid}),
            ets:insert(?JOBS_BY_STATE, {NewState, JobId, Pid}),

            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({'DOWN', MonRef, process, _Pid, _Reason}, State) ->
    case maps:get(MonRef, State#state.monitors, undefined) of
        undefined ->
            {noreply, State};
        JobId ->
            NewState = do_unregister(JobId, State),
            {noreply, NewState#state{
                monitors = maps:remove(MonRef, NewState#state.monitors)
            }}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Remove a job from all ETS tables
do_unregister(JobId, State) ->
    case ets:lookup(?JOBS_BY_ID, JobId) of
        [#job_entry{user_id = UserId, state = JobState, pid = Pid}] ->
            %% Remove from all tables
            ets:delete(?JOBS_BY_ID, JobId),
            ets:delete_object(?JOBS_BY_USER, {UserId, JobId, Pid}),
            ets:delete_object(?JOBS_BY_STATE, {JobState, JobId, Pid}),

            %% Find and remove monitor
            MonRef = find_monitor_ref(JobId, State#state.monitors),
            case MonRef of
                undefined ->
                    State;
                Ref ->
                    erlang:demonitor(Ref, [flush]),
                    State#state{monitors = maps:remove(Ref, State#state.monitors)}
            end;
        [] ->
            State
    end.

%% @private
%% Find the monitor reference for a job_id
find_monitor_ref(JobId, Monitors) ->
    case maps:to_list(maps:filter(fun(_, V) -> V =:= JobId end, Monitors)) of
        [{Ref, _}] -> Ref;
        [] -> undefined
    end.
