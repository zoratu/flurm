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
    remove_dependency/2,
    get_dependencies/1,
    get_dependents/1,
    check_dependencies/1,
    are_dependencies_satisfied/1,
    on_job_state_change/2,
    parse_dependency_spec/1,
    format_dependency_spec/1,
    get_dependency_graph/0,
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

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Add a dependency for a job
-spec add_dependency(job_id(), dep_type(), job_id() | binary()) -> ok | {error, term()}.
add_dependency(JobId, DepType, Target) ->
    gen_server:call(?SERVER, {add_dependency, JobId, DepType, Target}).

%% @doc Remove a specific dependency
-spec remove_dependency(job_id(), {dep_type(), job_id() | binary()}) -> ok.
remove_dependency(JobId, {DepType, Target}) ->
    gen_server:call(?SERVER, {remove_dependency, JobId, DepType, Target}).

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
-spec get_dependency_graph() -> map().
get_dependency_graph() ->
    AllDeps = ets:tab2list(?DEPS_TABLE),
    lists:foldl(fun(#dependency{job_id = JobId, dep_type = Type, target = Target}, Acc) ->
        Key = JobId,
        Current = maps:get(Key, Acc, []),
        maps:put(Key, [{Type, Target} | Current], Acc)
    end, #{}, AllDeps).

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
    {ok, #state{}}.

handle_call({add_dependency, JobId, DepType, Target}, _From, State) ->
    Result = do_add_dependency(JobId, DepType, Target),
    {reply, Result, State};

handle_call({remove_dependency, JobId, DepType, Target}, _From, State) ->
    do_remove_dependency(JobId, DepType, Target),
    {reply, ok, State};

handle_call(clear_completed_deps, _From, State) ->
    Count = do_clear_completed_deps(),
    {reply, Count, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({job_state_change, JobId, NewState}, State) ->
    do_handle_state_change(JobId, NewState),
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

    %% If not satisfied, register as dependent
    case Satisfied of
        false -> add_to_dependents(TargetJobId, JobId);
        true -> ok
    end,
    ok.

do_remove_dependency(JobId, DepType, Target) ->
    Key = {JobId, DepType, Target},
    ets:delete(?DEPS_TABLE, Key),

    %% Remove from dependents list
    case is_integer(Target) of
        true -> remove_from_dependents(Target, JobId);
        false -> ok
    end.

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
    case catch flurm_job_registry:get_job_state(JobId) of
        {ok, State} -> {ok, State};
        _ -> {error, not_found}
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
            %% Notify scheduler that job is ready
            catch flurm_scheduler:job_deps_satisfied(JobId);
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
