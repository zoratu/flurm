%%%-------------------------------------------------------------------
%%% @doc FLURM Quality of Service (QOS) Management
%%%
%%% Implements SLURM-compatible QOS for priority tiers and limits.
%%%
%%% QOS features:
%%% - Priority adjustment per QOS level
%%% - Resource limits (jobs, TRES, wall time)
%%% - Preemption rules between QOS levels
%%% - Usage factors for fair-share calculations
%%% - Grace periods for preemption
%%%
%%% Standard QOS levels:
%%% - normal: Default QOS for regular jobs
%%% - high: Higher priority, may preempt normal
%%% - low: Lower priority, can be preempted
%%% - interactive: For short interactive jobs
%%% - standby: Low priority, uses idle resources
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_qos).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    create/1,
    create/2,
    get/1,
    list/0,
    update/2,
    delete/1,
    check_limits/2,
    check_tres_limits/3,
    get_priority_adjustment/1,
    get_preemptable_qos/1,
    can_preempt/2,
    apply_usage_factor/2,
    get_default/0,
    set_default/1,
    init_default_qos/0
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
-define(QOS_TABLE, flurm_qos).
-define(DEFAULT_QOS, <<"normal">>).

-record(state, {
    default_qos :: binary()
}).

%% Test exports
-ifdef(TEST).
-export([
    %% QOS creation and updates
    create_default_qos_entries/0,
    do_create/2,
    do_update/2,
    apply_updates/2,
    %% Limit checking
    do_check_limits/2,
    check_job_count_limits/2,
    check_user_tres/3,
    check_tres_map/2,
    %% TRES helpers
    combine_tres/2,
    get_user_job_counts/1,
    get_user_tres_usage/1
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

%% @doc Create a new QOS with default settings
-spec create(binary()) -> ok | {error, term()}.
create(Name) ->
    create(Name, #{}).

%% @doc Create a new QOS with specified settings
-spec create(binary(), map()) -> ok | {error, term()}.
create(Name, Options) ->
    gen_server:call(?SERVER, {create, Name, Options}).

%% @doc Get a QOS by name
-spec get(binary()) -> {ok, #qos{}} | {error, not_found}.
get(Name) ->
    case ets:lookup(?QOS_TABLE, Name) of
        [QOS] -> {ok, QOS};
        [] -> {error, not_found}
    end.

%% @doc List all QOS entries
-spec list() -> [#qos{}].
list() ->
    ets:tab2list(?QOS_TABLE).

%% @doc Update a QOS
-spec update(binary(), map()) -> ok | {error, term()}.
update(Name, Updates) ->
    gen_server:call(?SERVER, {update, Name, Updates}).

%% @doc Delete a QOS
-spec delete(binary()) -> ok | {error, term()}.
delete(Name) ->
    gen_server:call(?SERVER, {delete, Name}).

%% @doc Check if job submission is allowed under QOS limits
-spec check_limits(binary(), map()) -> ok | {error, term()}.
check_limits(QOSName, JobSpec) ->
    case ?MODULE:get(QOSName) of
        {ok, QOS} ->
            do_check_limits(QOS, JobSpec);
        {error, not_found} ->
            {error, {invalid_qos, QOSName}}
    end.

%% @doc Check TRES limits for a job
-spec check_tres_limits(binary(), binary(), map()) -> ok | {error, term()}.
check_tres_limits(QOSName, User, TRESRequest) ->
    case ?MODULE:get(QOSName) of
        {ok, QOS} ->
            check_user_tres(QOS, User, TRESRequest);
        {error, not_found} ->
            {error, {invalid_qos, QOSName}}
    end.

%% @doc Get priority adjustment for a QOS
-spec get_priority_adjustment(binary()) -> integer().
get_priority_adjustment(QOSName) ->
    case ?MODULE:get(QOSName) of
        {ok, #qos{priority = Priority}} -> Priority;
        {error, _} -> 0
    end.

%% @doc Get list of QOS names that can be preempted by this QOS
-spec get_preemptable_qos(binary()) -> [binary()].
get_preemptable_qos(QOSName) ->
    case ?MODULE:get(QOSName) of
        {ok, #qos{preempt = PreemptList}} -> PreemptList;
        {error, _} -> []
    end.

%% @doc Check if QOS A can preempt QOS B
-spec can_preempt(binary(), binary()) -> boolean().
can_preempt(PreemptorQOS, TargetQOS) ->
    PreemptableList = get_preemptable_qos(PreemptorQOS),
    lists:member(TargetQOS, PreemptableList).

%% @doc Apply usage factor for fair-share calculation
-spec apply_usage_factor(binary(), float()) -> float().
apply_usage_factor(QOSName, Usage) ->
    case ?MODULE:get(QOSName) of
        {ok, #qos{usage_factor = Factor}} -> Usage * Factor;
        {error, _} -> Usage
    end.

%% @doc Get the default QOS
-spec get_default() -> binary().
get_default() ->
    gen_server:call(?SERVER, get_default).

%% @doc Set the default QOS
-spec set_default(binary()) -> ok | {error, term()}.
set_default(QOSName) ->
    gen_server:call(?SERVER, {set_default, QOSName}).

%% @doc Initialize default QOS entries
-spec init_default_qos() -> ok.
init_default_qos() ->
    gen_server:call(?SERVER, init_defaults).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    ets:new(?QOS_TABLE, [
        named_table, public, set,
        {keypos, #qos.name}
    ]),

    %% Create default QOS entries
    create_default_qos_entries(),

    {ok, #state{default_qos = ?DEFAULT_QOS}}.

handle_call({create, Name, Options}, _From, State) ->
    Result = do_create(Name, Options),
    {reply, Result, State};

handle_call({update, Name, Updates}, _From, State) ->
    Result = do_update(Name, Updates),
    {reply, Result, State};

handle_call({delete, Name}, _From, State) ->
    case Name of
        <<"normal">> ->
            {reply, {error, cannot_delete_default}, State};
        _ ->
            ets:delete(?QOS_TABLE, Name),
            {reply, ok, State}
    end;

handle_call(get_default, _From, State) ->
    {reply, State#state.default_qos, State};

handle_call({set_default, QOSName}, _From, State) ->
    case ets:lookup(?QOS_TABLE, QOSName) of
        [_] ->
            {reply, ok, State#state{default_qos = QOSName}};
        [] ->
            {reply, {error, qos_not_found}, State}
    end;

handle_call(init_defaults, _From, State) ->
    create_default_qos_entries(),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

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

create_default_qos_entries() ->
    %% Normal QOS - default for regular jobs
    do_create(<<"normal">>, #{
        description => <<"Default QOS for regular jobs">>,
        priority => 0,
        max_wall_per_job => 86400  % 24 hours
    }),

    %% High priority QOS
    do_create(<<"high">>, #{
        description => <<"High priority jobs">>,
        priority => 1000,
        preempt => [<<"low">>, <<"standby">>],
        preempt_mode => requeue,
        max_wall_per_job => 172800,  % 48 hours
        max_jobs_pu => 50
    }),

    %% Low priority QOS
    do_create(<<"low">>, #{
        description => <<"Low priority jobs, can be preempted">>,
        priority => -500,
        usage_factor => 0.5,
        max_wall_per_job => 604800  % 7 days
    }),

    %% Interactive QOS - for short interactive sessions
    do_create(<<"interactive">>, #{
        description => <<"Short interactive jobs">>,
        priority => 500,
        max_wall_per_job => 3600,  % 1 hour
        max_jobs_pu => 5,
        grace_time => 30
    }),

    %% Standby QOS - uses idle resources only
    do_create(<<"standby">>, #{
        description => <<"Standby jobs using idle resources">>,
        priority => -1000,
        usage_factor => 0.0,  % Doesn't count toward fair-share
        preempt_mode => cancel
    }),

    ok.

do_create(Name, Options) ->
    %% Check if already exists
    case ets:lookup(?QOS_TABLE, Name) of
        [_] ->
            {error, already_exists};
        [] ->
            QOS = #qos{
                name = Name,
                description = maps:get(description, Options, <<>>),
                priority = maps:get(priority, Options, 0),
                flags = maps:get(flags, Options, []),
                grace_time = maps:get(grace_time, Options, 0),
                max_jobs_pa = maps:get(max_jobs_pa, Options, 0),
                max_jobs_pu = maps:get(max_jobs_pu, Options, 0),
                max_submit_jobs_pa = maps:get(max_submit_jobs_pa, Options, 0),
                max_submit_jobs_pu = maps:get(max_submit_jobs_pu, Options, 0),
                max_tres_pa = maps:get(max_tres_pa, Options, #{}),
                max_tres_pu = maps:get(max_tres_pu, Options, #{}),
                max_tres_per_job = maps:get(max_tres_per_job, Options, #{}),
                max_tres_per_node = maps:get(max_tres_per_node, Options, #{}),
                max_tres_per_user = maps:get(max_tres_per_user, Options, #{}),
                max_wall_per_job = maps:get(max_wall_per_job, Options, 0),
                min_tres_per_job = maps:get(min_tres_per_job, Options, #{}),
                preempt = maps:get(preempt, Options, []),
                preempt_mode = maps:get(preempt_mode, Options, off),
                usage_factor = maps:get(usage_factor, Options, 1.0),
                usage_threshold = maps:get(usage_threshold, Options, 0.0)
            },
            ets:insert(?QOS_TABLE, QOS),
            ok
    end.

do_update(Name, Updates) ->
    case ets:lookup(?QOS_TABLE, Name) of
        [QOS] ->
            UpdatedQOS = apply_updates(QOS, Updates),
            ets:insert(?QOS_TABLE, UpdatedQOS),
            ok;
        [] ->
            {error, not_found}
    end.

apply_updates(QOS, Updates) ->
    maps:fold(fun(Key, Value, Acc) ->
        case Key of
            description -> Acc#qos{description = Value};
            priority -> Acc#qos{priority = Value};
            flags -> Acc#qos{flags = Value};
            grace_time -> Acc#qos{grace_time = Value};
            max_jobs_pa -> Acc#qos{max_jobs_pa = Value};
            max_jobs_pu -> Acc#qos{max_jobs_pu = Value};
            max_submit_jobs_pa -> Acc#qos{max_submit_jobs_pa = Value};
            max_submit_jobs_pu -> Acc#qos{max_submit_jobs_pu = Value};
            max_tres_pa -> Acc#qos{max_tres_pa = Value};
            max_tres_pu -> Acc#qos{max_tres_pu = Value};
            max_tres_per_job -> Acc#qos{max_tres_per_job = Value};
            max_tres_per_node -> Acc#qos{max_tres_per_node = Value};
            max_tres_per_user -> Acc#qos{max_tres_per_user = Value};
            max_wall_per_job -> Acc#qos{max_wall_per_job = Value};
            min_tres_per_job -> Acc#qos{min_tres_per_job = Value};
            preempt -> Acc#qos{preempt = Value};
            preempt_mode -> Acc#qos{preempt_mode = Value};
            usage_factor -> Acc#qos{usage_factor = Value};
            usage_threshold -> Acc#qos{usage_threshold = Value};
            _ -> Acc
        end
    end, QOS, Updates).

do_check_limits(#qos{max_wall_per_job = MaxWall} = QOS, JobSpec) ->
    WallTime = maps:get(time_limit, JobSpec, 0),

    %% Check wall time limit
    case MaxWall > 0 andalso WallTime > MaxWall of
        true ->
            {error, {exceeds_wall_limit, WallTime, MaxWall}};
        false ->
            check_job_count_limits(QOS, JobSpec)
    end.

check_job_count_limits(#qos{max_jobs_pu = MaxJobsPU, max_submit_jobs_pu = MaxSubmitPU},
                       JobSpec) ->
    User = maps:get(user, JobSpec, <<>>),

    %% Get current job counts for user
    {RunningCount, PendingCount} = get_user_job_counts(User),
    TotalCount = RunningCount + PendingCount,

    %% Check running jobs limit
    case MaxJobsPU > 0 andalso RunningCount >= MaxJobsPU of
        true ->
            {error, {exceeds_max_jobs_per_user, RunningCount, MaxJobsPU}};
        false ->
            %% Check submit limit
            case MaxSubmitPU > 0 andalso TotalCount >= MaxSubmitPU of
                true ->
                    {error, {exceeds_max_submit_per_user, TotalCount, MaxSubmitPU}};
                false ->
                    ok
            end
    end.

check_user_tres(#qos{max_tres_pu = MaxTRES, max_tres_per_job = MaxPerJob},
                User, TRESRequest) ->
    %% Check per-job TRES limits
    case check_tres_map(TRESRequest, MaxPerJob) of
        ok ->
            %% Check per-user TRES limits
            CurrentTRES = get_user_tres_usage(User),
            CombinedTRES = combine_tres(CurrentTRES, TRESRequest),
            check_tres_map(CombinedTRES, MaxTRES);
        Error ->
            Error
    end.

check_tres_map(_Request, MaxTRES) when map_size(MaxTRES) =:= 0 ->
    ok;
check_tres_map(Request, MaxTRES) ->
    Violations = maps:fold(fun(TRESName, Requested, Acc) ->
        case maps:get(TRESName, MaxTRES, 0) of
            0 -> Acc;  % No limit
            Max when Requested > Max ->
                [{TRESName, Requested, Max} | Acc];
            _ ->
                Acc
        end
    end, [], Request),

    case Violations of
        [] -> ok;
        _ -> {error, {tres_limit_exceeded, Violations}}
    end.

combine_tres(Map1, Map2) ->
    maps:fold(fun(K, V, Acc) ->
        maps:update_with(K, fun(Existing) -> Existing + V end, V, Acc)
    end, Map1, Map2).

get_user_job_counts(User) ->
    %% Query job registry for user's job counts
    case catch flurm_job_registry:get_user_job_counts(User) of
        {ok, Running, Pending} -> {Running, Pending};
        _ -> {0, 0}
    end.

get_user_tres_usage(User) ->
    %% Query current TRES usage for user
    case catch flurm_job_registry:get_user_tres_usage(User) of
        {ok, TRES} -> TRES;
        _ -> #{}
    end.
