%%%-------------------------------------------------------------------
%%% @doc FLURM Job Dependencies
%%%
%%% Implements SLURM-compatible job dependencies for workflow scheduling.
%%%
%%% Supported dependency types:
%%% - after:jobid       - Start after job begins
%%% - afterok:jobid     - Start after job completes successfully
%%% - afternotok:jobid  - Start after job fails
%%% - afterany:jobid    - Start after job ends (any state)
%%% - aftercorr:jobid   - Start corresponding array task after
%%% - singleton         - Only one job with same name/user at a time
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_deps).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    add_dependency/3,
    add_dependencies/2,
    remove_dependency/2,
    remove_all_dependencies/1,
    get_dependencies/1,
    get_dependents/1,
    check_dependencies/1,
    are_dependencies_satisfied/1,
    on_job_state_change/2,
    notify_completion/2,
    release_job/1,
    hold_for_dependencies/1,
    parse_dependency_spec/1,
    format_dependency_spec/1,
    get_dependency_graph/0,
    detect_circular_dependency/2,
    has_circular_dependency/2,
    clear_completed_dependencies/0
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
-define(DEPS_TABLE, flurm_job_deps).
-define(DEPENDENTS_TABLE, flurm_job_dependents).
-define(SINGLETON_TABLE, flurm_singletons).
-define(GRAPH_TABLE, flurm_dep_graph).  % For circular dependency detection

-type dep_type() :: after_start | afterok | afternotok | afterany |
                    aftercorr | afterburstbuffer | singleton.

-record(dependency, {
    id :: {job_id(), dep_type(), job_id() | binary()},
    job_id :: job_id(),                    % Job that has dependency
    dep_type :: dep_type(),
    target :: job_id() | binary(),         % Target job or singleton name
    satisfied :: boolean(),
    satisfied_time :: non_neg_integer() | undefined
}).

-record(state, {}).

-export_type([dep_type/0]).

%% Test exports for internal functions
-ifdef(TEST).
-export([
    parse_single_dep/1,
    parse_dep_type/1,
    parse_target/1,
    format_single_dep/1,
    format_dep_type/1,
    state_satisfies/2,
    is_terminal_state/1,
    find_path/3
]).
-endif.

%%====================================================================
%% API
%%====================================================================

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

%% @doc Add a dependency for a job
%% Returns {error, circular_dependency} if adding would create a cycle
-spec add_dependency(job_id(), dep_type(), job_id() | binary()) -> ok | {error, term()}.
add_dependency(JobId, DepType, Target) ->
    gen_server:call(?SERVER, {add_dependency, JobId, DepType, Target}).

%% @doc Add multiple dependencies from a parsed dependency spec
%% Parses the spec string and adds all dependencies, checking for cycles
-spec add_dependencies(job_id(), binary()) -> ok | {error, term()}.
add_dependencies(JobId, DepSpec) when is_binary(DepSpec) ->
    case parse_dependency_spec(DepSpec) of
        {ok, []} ->
            ok;
        {ok, Deps} ->
            gen_server:call(?SERVER, {add_dependencies, JobId, Deps});
        {error, _} = Error ->
            Error
    end.

%% @doc Remove a specific dependency
-spec remove_dependency(job_id(), {dep_type(), job_id() | binary()}) -> ok.
remove_dependency(JobId, {DepType, Target}) ->
    gen_server:call(?SERVER, {remove_dependency, JobId, DepType, Target}).

%% @doc Remove all dependencies for a job (used when job is cancelled/completed)
-spec remove_all_dependencies(job_id()) -> ok.
remove_all_dependencies(JobId) ->
    gen_server:call(?SERVER, {remove_all_dependencies, JobId}).

%% @doc Get all dependencies for a job
-spec get_dependencies(job_id()) -> [#dependency{}].
get_dependencies(JobId) ->
    ets:select(?DEPS_TABLE, [{
        #dependency{job_id = JobId, _ = '_'},
        [],
        ['$_']
    }]).

%% @doc Get all jobs that depend on a given job
-spec get_dependents(job_id()) -> [job_id()].
get_dependents(TargetJobId) ->
    case ets:lookup(?DEPENDENTS_TABLE, TargetJobId) of
        [{TargetJobId, Dependents}] -> Dependents;
        [] -> []
    end.

%% @doc Check dependency status (returns unsatisfied deps)
-spec check_dependencies(job_id()) -> {ok, []} | {waiting, [#dependency{}]}.
check_dependencies(JobId) ->
    Deps = get_dependencies(JobId),
    Unsatisfied = [D || D <- Deps, not D#dependency.satisfied],
    case Unsatisfied of
        [] -> {ok, []};
        _ -> {waiting, Unsatisfied}
    end.

%% @doc Check if all dependencies are satisfied
-spec are_dependencies_satisfied(job_id()) -> boolean().
are_dependencies_satisfied(JobId) ->
    case check_dependencies(JobId) of
        {ok, []} -> true;
        {waiting, _} -> false
    end.

%% @doc Called when a job changes state (to update dependencies)
-spec on_job_state_change(job_id(), job_state()) -> ok.
on_job_state_change(JobId, NewState) ->
    gen_server:cast(?SERVER, {job_state_change, JobId, NewState}).

%% @doc Notify dependent jobs when a job completes with given result
%% Result can be: completed, failed, cancelled, timeout
-spec notify_completion(job_id(), job_state()) -> ok.
notify_completion(JobId, Result) ->
    gen_server:cast(?SERVER, {job_completed, JobId, Result}).

%% @doc Release a job for scheduling once dependencies are satisfied
%% This is called internally when all deps are met, but can also be called manually
-spec release_job(job_id()) -> ok | {error, term()}.
release_job(JobId) ->
    case are_dependencies_satisfied(JobId) of
        true ->
            %% Update job state from held to pending
            case catch flurm_job_manager:release_job(JobId) of
                ok ->
                    lager:info("Job ~p released (dependencies satisfied)", [JobId]),
                    ok;
                {error, Reason} ->
                    {error, Reason};
                {'EXIT', _} ->
                    {error, job_manager_unavailable}
            end;
        false ->
            {error, dependencies_not_satisfied}
    end.

%% @doc Hold a job until its dependencies are satisfied
%% Called during job submission when dependencies are specified
-spec hold_for_dependencies(job_id()) -> ok | {error, term()}.
hold_for_dependencies(JobId) ->
    case check_dependencies(JobId) of
        {ok, []} ->
            %% No unsatisfied dependencies, job can run
            ok;
        {waiting, _Deps} ->
            %% Has unsatisfied dependencies, hold the job
            case catch flurm_job_manager:hold_job(JobId) of
                ok ->
                    lager:info("Job ~p held pending dependencies", [JobId]),
                    ok;
                {error, Reason} ->
                    {error, Reason};
                {'EXIT', _} ->
                    {error, job_manager_unavailable}
            end
    end.

%% @doc Parse dependency specification string
%% Format: "afterok:123,afterany:456,singleton"
-spec parse_dependency_spec(binary()) -> {ok, [{dep_type(), job_id() | binary()}]} | {error, term()}.
parse_dependency_spec(<<>>) ->
    {ok, []};
parse_dependency_spec(Spec) when is_binary(Spec) ->
    Parts = binary:split(Spec, <<",">>, [global, trim_all]),
    try
        Deps = lists:map(fun parse_single_dep/1, Parts),
        {ok, Deps}
    catch
        throw:{parse_error, Reason} -> {error, Reason}
    end.

%% @doc Format dependencies as specification string
-spec format_dependency_spec([{dep_type(), job_id() | binary()}]) -> binary().
format_dependency_spec([]) ->
    <<>>;
format_dependency_spec(Deps) ->
    Parts = lists:map(fun format_single_dep/1, Deps),
    iolist_to_binary(lists:join(<<",">>, Parts)).

%% @doc Get the full dependency graph
%% Returns a map of JobId -> [{DepType, TargetJobId}]
-spec get_dependency_graph() -> map().
get_dependency_graph() ->
    AllDeps = ets:tab2list(?DEPS_TABLE),
    lists:foldl(fun(#dependency{job_id = JobId, dep_type = Type, target = Target}, Acc) ->
        Key = JobId,
        Current = maps:get(Key, Acc, []),
        maps:put(Key, [{Type, Target} | Current], Acc)
    end, #{}, AllDeps).

%% @doc Check if adding a dependency would create a circular dependency
%% Returns {error, circular_dependency, Path} if cycle would be created
-spec detect_circular_dependency(job_id(), job_id()) -> ok | {error, circular_dependency, [job_id()]}.
detect_circular_dependency(JobId, TargetJobId) when is_integer(TargetJobId) ->
    %% Check if TargetJobId (directly or indirectly) depends on JobId
    %% If so, adding JobId -> TargetJobId would create a cycle
    case find_path(TargetJobId, JobId, sets:new()) of
        {ok, Path} ->
            %% Found a path from Target to Job, adding Job->Target creates cycle
            {error, circular_dependency, [JobId | Path]};
        not_found ->
            ok
    end;
detect_circular_dependency(_JobId, _Target) ->
    %% Non-integer targets (singletons) can't create cycles
    ok.

%% @doc Check if a dependency from JobId to TargetJobId would be circular
-spec has_circular_dependency(job_id(), job_id()) -> boolean().
has_circular_dependency(JobId, TargetJobId) ->
    case detect_circular_dependency(JobId, TargetJobId) of
        ok -> false;
        {error, circular_dependency, _} -> true
    end.

%% @doc Clean up dependencies for completed jobs
-spec clear_completed_dependencies() -> non_neg_integer().
clear_completed_dependencies() ->
    gen_server:call(?SERVER, clear_completed_deps).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    ets:new(?DEPS_TABLE, [
        named_table, public, set,
        {keypos, #dependency.id}
    ]),
    ets:new(?DEPENDENTS_TABLE, [
        named_table, public, set
    ]),
    ets:new(?SINGLETON_TABLE, [
        named_table, public, set
    ]),
    %% Graph table for efficient cycle detection
    %% Stores {JobId, [TargetJobIds]} - direct dependencies only
    ets:new(?GRAPH_TABLE, [
        named_table, public, set
    ]),
    {ok, #state{}}.

handle_call({add_dependency, JobId, DepType, Target}, _From, State) ->
    Result = do_add_dependency(JobId, DepType, Target),
    {reply, Result, State};

handle_call({add_dependencies, JobId, Deps}, _From, State) ->
    %% Add multiple dependencies, checking for cycles first
    Result = do_add_dependencies(JobId, Deps),
    {reply, Result, State};

handle_call({remove_dependency, JobId, DepType, Target}, _From, State) ->
    do_remove_dependency(JobId, DepType, Target),
    {reply, ok, State};

handle_call({remove_all_dependencies, JobId}, _From, State) ->
    do_remove_all_dependencies(JobId),
    {reply, ok, State};

handle_call(clear_completed_deps, _From, State) ->
    Count = do_clear_completed_deps(),
    {reply, Count, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({job_state_change, JobId, NewState}, State) ->
    do_handle_state_change(JobId, NewState),
    {noreply, State};

handle_cast({job_completed, JobId, Result}, State) ->
    %% Same as state change but explicit completion notification
    do_handle_state_change(JobId, Result),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

do_add_dependency(JobId, singleton, Name) ->
    %% Singleton: only one job with this name can run at a time
    Key = {JobId, singleton, Name},
    case ets:lookup(?SINGLETON_TABLE, Name) of
        [] ->
            %% No other singleton, satisfied immediately
            Dep = #dependency{
                id = Key,
                job_id = JobId,
                dep_type = singleton,
                target = Name,
                satisfied = true,
                satisfied_time = erlang:system_time(second)
            },
            ets:insert(?DEPS_TABLE, Dep),
            ets:insert(?SINGLETON_TABLE, {Name, JobId}),
            ok;
        [{Name, OtherJobId}] ->
            %% Another job holds the singleton
            Dep = #dependency{
                id = Key,
                job_id = JobId,
                dep_type = singleton,
                target = Name,
                satisfied = false,
                satisfied_time = undefined
            },
            ets:insert(?DEPS_TABLE, Dep),
            %% Register as dependent
            add_to_dependents(OtherJobId, JobId),
            ok
    end;

do_add_dependency(JobId, DepType, TargetJobId) when is_integer(TargetJobId) ->
    %% First check for circular dependencies
    case detect_circular_dependency(JobId, TargetJobId) of
        {error, circular_dependency, Path} ->
            lager:warning("Circular dependency detected: ~p -> ~p (path: ~p)",
                         [JobId, TargetJobId, Path]),
            {error, {circular_dependency, Path}};
        ok ->
            Key = {JobId, DepType, TargetJobId},

            %% Check if target job is already in satisfying state
            Satisfied = check_target_satisfies(DepType, TargetJobId),

            Dep = #dependency{
                id = Key,
                job_id = JobId,
                dep_type = DepType,
                target = TargetJobId,
                satisfied = Satisfied,
                satisfied_time = case Satisfied of
                    true -> erlang:system_time(second);
                    false -> undefined
                end
            },
            ets:insert(?DEPS_TABLE, Dep),

            %% Update the graph table for cycle detection
            add_to_graph(JobId, TargetJobId),

            %% If not satisfied, register as dependent
            case Satisfied of
                false -> add_to_dependents(TargetJobId, JobId);
                true -> ok
            end,
            ok
    end;
do_add_dependency(JobId, DepType, Targets) when is_list(Targets) ->
    %% Handle multiple targets (job+job+job syntax)
    Results = [do_add_dependency(JobId, DepType, T) || T <- Targets],
    case lists:all(fun(R) -> R =:= ok end, Results) of
        true -> ok;
        false ->
            %% Return first error
            lists:foldl(fun
                ({error, _} = E, ok) -> E;
                (_, Acc) -> Acc
            end, ok, Results)
    end.

%% @private Add multiple dependencies with cycle checking
do_add_dependencies(JobId, Deps) ->
    %% First check all dependencies for cycles before adding any
    CycleChecks = [detect_circular_dependency(JobId, T) || {_Type, T} <- Deps, is_integer(T)],
    case lists:filter(fun(R) -> R =/= ok end, CycleChecks) of
        [{error, circular_dependency, Path} | _] ->
            {error, {circular_dependency, Path}};
        [] ->
            %% No cycles, add all dependencies
            Results = [do_add_dependency(JobId, Type, Target) || {Type, Target} <- Deps],
            case lists:all(fun(R) -> R =:= ok end, Results) of
                true ->
                    %% After adding dependencies, check if job should be held
                    hold_for_dependencies(JobId),
                    ok;
                false ->
                    %% Find first error
                    hd([E || E <- Results, E =/= ok])
            end
    end.

do_remove_dependency(JobId, DepType, Target) ->
    Key = {JobId, DepType, Target},
    ets:delete(?DEPS_TABLE, Key),

    %% Remove from dependents list
    case is_integer(Target) of
        true ->
            remove_from_dependents(Target, JobId),
            remove_from_graph(JobId, Target);
        false -> ok
    end.

%% @private Remove all dependencies for a job
do_remove_all_dependencies(JobId) ->
    %% Get all dependencies for this job
    Deps = get_dependencies(JobId),

    %% Remove each one
    lists:foreach(fun(#dependency{dep_type = Type, target = Target}) ->
        do_remove_dependency(JobId, Type, Target)
    end, Deps),

    %% Clean up graph entry
    ets:delete(?GRAPH_TABLE, JobId).

add_to_dependents(TargetJobId, DependentJobId) ->
    case ets:lookup(?DEPENDENTS_TABLE, TargetJobId) of
        [{TargetJobId, Deps}] ->
            case lists:member(DependentJobId, Deps) of
                true -> ok;
                false ->
                    ets:insert(?DEPENDENTS_TABLE, {TargetJobId, [DependentJobId | Deps]})
            end;
        [] ->
            ets:insert(?DEPENDENTS_TABLE, {TargetJobId, [DependentJobId]})
    end.

remove_from_dependents(TargetJobId, DependentJobId) ->
    case ets:lookup(?DEPENDENTS_TABLE, TargetJobId) of
        [{TargetJobId, Deps}] ->
            NewDeps = lists:delete(DependentJobId, Deps),
            case NewDeps of
                [] -> ets:delete(?DEPENDENTS_TABLE, TargetJobId);
                _ -> ets:insert(?DEPENDENTS_TABLE, {TargetJobId, NewDeps})
            end;
        [] ->
            ok
    end.

%% Graph management functions for cycle detection
add_to_graph(JobId, TargetJobId) when is_integer(TargetJobId) ->
    case ets:lookup(?GRAPH_TABLE, JobId) of
        [{JobId, Targets}] ->
            case lists:member(TargetJobId, Targets) of
                true -> ok;
                false ->
                    ets:insert(?GRAPH_TABLE, {JobId, [TargetJobId | Targets]})
            end;
        [] ->
            ets:insert(?GRAPH_TABLE, {JobId, [TargetJobId]})
    end;
add_to_graph(_JobId, _Target) ->
    ok.

remove_from_graph(JobId, TargetJobId) ->
    case ets:lookup(?GRAPH_TABLE, JobId) of
        [{JobId, Targets}] ->
            NewTargets = lists:delete(TargetJobId, Targets),
            case NewTargets of
                [] -> ets:delete(?GRAPH_TABLE, JobId);
                _ -> ets:insert(?GRAPH_TABLE, {JobId, NewTargets})
            end;
        [] ->
            ok
    end.

%% Find a path from StartJob to EndJob using BFS
%% Returns {ok, Path} if path exists, not_found otherwise
find_path(StartJob, EndJob, _Visited) when StartJob =:= EndJob ->
    {ok, [EndJob]};
find_path(StartJob, EndJob, Visited) ->
    case sets:is_element(StartJob, Visited) of
        true ->
            not_found;
        false ->
            NewVisited = sets:add_element(StartJob, Visited),
            %% Get direct dependencies of StartJob
            Targets = case ets:lookup(?GRAPH_TABLE, StartJob) of
                [{StartJob, T}] -> T;
                [] -> []
            end,
            find_path_in_targets(Targets, EndJob, NewVisited, StartJob)
    end.

find_path_in_targets([], _EndJob, _Visited, _CurrentJob) ->
    not_found;
find_path_in_targets([Target | Rest], EndJob, Visited, CurrentJob) ->
    case find_path(Target, EndJob, Visited) of
        {ok, Path} ->
            {ok, [CurrentJob | Path]};
        not_found ->
            find_path_in_targets(Rest, EndJob, Visited, CurrentJob)
    end.

check_target_satisfies(DepType, TargetJobId) ->
    case catch get_job_state(TargetJobId) of
        {ok, State} -> state_satisfies(DepType, State);
        _ -> false
    end.

state_satisfies(after_start, State) ->
    State =/= pending;
state_satisfies(afterok, State) ->
    State =:= completed;
state_satisfies(afternotok, State) ->
    State =:= failed orelse State =:= cancelled orelse State =:= timeout;
state_satisfies(afterany, State) ->
    lists:member(State, [completed, failed, cancelled, timeout]);
state_satisfies(aftercorr, State) ->
    %% For array tasks - similar to afterany for now
    lists:member(State, [completed, failed, cancelled, timeout]);
state_satisfies(_, _) ->
    false.

get_job_state(JobId) ->
    %% Try flurm_job_manager first (primary job storage)
    case catch flurm_job_manager:get_job(JobId) of
        {ok, Job} when is_tuple(Job) ->
            %% Job is a #job record, get state field
            State = element(5, Job),  % state is 5th field in #job record
            {ok, State};
        _ ->
            %% Fallback to job registry if available
            case catch flurm_job_registry:get_job_entry(JobId) of
                {ok, Entry} when is_tuple(Entry) ->
                    %% Entry is a #job_entry record, state is 4th field
                    State = element(4, Entry),
                    {ok, State};
                _ ->
                    {error, not_found}
            end
    end.

do_handle_state_change(JobId, NewState) ->
    %% Get all jobs that depend on this one
    Dependents = get_dependents(JobId),

    %% Check each dependent
    lists:foreach(fun(DepJobId) ->
        update_dependencies_for(DepJobId, JobId, NewState)
    end, Dependents),

    %% Handle singleton release
    case is_terminal_state(NewState) of
        true ->
            release_singleton(JobId),
            %% Clean up dependents table
            ets:delete(?DEPENDENTS_TABLE, JobId);
        false ->
            ok
    end.

update_dependencies_for(DepJobId, TargetJobId, TargetState) ->
    %% Find all dependencies this job has on the target
    Deps = get_dependencies(DepJobId),
    TargetDeps = [D || D <- Deps, D#dependency.target =:= TargetJobId],

    lists:foreach(fun(#dependency{dep_type = DepType} = Dep) ->
        case state_satisfies(DepType, TargetState) of
            true when not Dep#dependency.satisfied ->
                %% Mark as satisfied
                ets:insert(?DEPS_TABLE, Dep#dependency{
                    satisfied = true,
                    satisfied_time = erlang:system_time(second)
                }),
                %% Notify scheduler that job may be ready
                maybe_notify_scheduler(DepJobId);
            _ ->
                ok
        end
    end, TargetDeps).

release_singleton(JobId) ->
    %% Find singleton held by this job
    Singletons = ets:match_object(?SINGLETON_TABLE, {'_', JobId}),
    lists:foreach(fun({Name, _}) ->
        ets:delete(?SINGLETON_TABLE, Name),

        %% Find next job waiting for this singleton
        WaitingDeps = ets:match_object(?DEPS_TABLE,
            #dependency{dep_type = singleton, target = Name, satisfied = false, _ = '_'}),
        case WaitingDeps of
            [NextDep | _] ->
                %% Grant singleton to next job
                ets:insert(?SINGLETON_TABLE, {Name, NextDep#dependency.job_id}),
                ets:insert(?DEPS_TABLE, NextDep#dependency{
                    satisfied = true,
                    satisfied_time = erlang:system_time(second)
                }),
                maybe_notify_scheduler(NextDep#dependency.job_id);
            [] ->
                ok
        end
    end, Singletons).

is_terminal_state(State) ->
    lists:member(State, [completed, failed, cancelled, timeout]).

maybe_notify_scheduler(JobId) ->
    %% Check if all dependencies are now satisfied
    case are_dependencies_satisfied(JobId) of
        true ->
            %% Release the job from held state and notify scheduler
            lager:info("All dependencies satisfied for job ~p, releasing", [JobId]),
            %% Try to release the job (moves from held to pending)
            case catch flurm_job_manager:release_job(JobId) of
                ok ->
                    %% Notify scheduler that job is ready to be scheduled
                    catch flurm_scheduler:job_deps_satisfied(JobId),
                    ok;
                {error, {invalid_state, pending}} ->
                    %% Job is already pending, just notify scheduler
                    catch flurm_scheduler:job_deps_satisfied(JobId),
                    ok;
                {error, Reason} ->
                    lager:warning("Failed to release job ~p: ~p", [JobId, Reason]),
                    ok;
                {'EXIT', _} ->
                    %% Job manager not available
                    ok
            end;
        false ->
            ok
    end.

do_clear_completed_deps() ->
    %% Find dependencies for jobs that are in terminal states
    AllDeps = ets:tab2list(?DEPS_TABLE),
    ToDelete = lists:filter(fun(#dependency{job_id = JobId}) ->
        case get_job_state(JobId) of
            {ok, State} -> is_terminal_state(State);
            _ -> true  % Job doesn't exist, clean up
        end
    end, AllDeps),
    lists:foreach(fun(#dependency{id = Id}) ->
        ets:delete(?DEPS_TABLE, Id)
    end, ToDelete),
    length(ToDelete).

%%====================================================================
%% Parsing Functions
%%====================================================================

parse_single_dep(<<"singleton">>) ->
    {singleton, <<"default">>};
parse_single_dep(<<"singleton:", Name/binary>>) ->
    {singleton, Name};
parse_single_dep(Spec) ->
    case binary:split(Spec, <<":">>) of
        [TypeBin, TargetBin] ->
            Type = parse_dep_type(TypeBin),
            Target = parse_target(TargetBin),
            {Type, Target};
        _ ->
            throw({parse_error, {invalid_dependency, Spec}})
    end.

parse_dep_type(<<"after">>) -> after_start;
parse_dep_type(<<"afterok">>) -> afterok;
parse_dep_type(<<"afternotok">>) -> afternotok;
parse_dep_type(<<"afterany">>) -> afterany;
parse_dep_type(<<"aftercorr">>) -> aftercorr;
parse_dep_type(<<"afterburstbuffer">>) -> afterburstbuffer;
parse_dep_type(Other) -> throw({parse_error, {unknown_dep_type, Other}}).

parse_target(TargetBin) ->
    %% Could be job ID or job ID list
    case binary:match(TargetBin, <<"+">>) of
        nomatch ->
            binary_to_integer(TargetBin);
        _ ->
            %% Multiple targets (job+job+job)
            Parts = binary:split(TargetBin, <<"+">>, [global]),
            [binary_to_integer(P) || P <- Parts]
    end.

format_single_dep({singleton, <<"default">>}) ->
    <<"singleton">>;
format_single_dep({singleton, Name}) ->
    <<"singleton:", Name/binary>>;
format_single_dep({Type, Target}) when is_integer(Target) ->
    TypeBin = format_dep_type(Type),
    <<TypeBin/binary, ":", (integer_to_binary(Target))/binary>>;
format_single_dep({Type, Targets}) when is_list(Targets) ->
    TypeBin = format_dep_type(Type),
    TargetBin = iolist_to_binary(lists:join(<<"+">>,
        [integer_to_binary(T) || T <- Targets])),
    <<TypeBin/binary, ":", TargetBin/binary>>.

format_dep_type(after_start) -> <<"after">>;
format_dep_type(afterok) -> <<"afterok">>;
format_dep_type(afternotok) -> <<"afternotok">>;
format_dep_type(afterany) -> <<"afterany">>;
format_dep_type(aftercorr) -> <<"aftercorr">>;
format_dep_type(afterburstbuffer) -> <<"afterburstbuffer">>.
