%%%-------------------------------------------------------------------
%%% @doc FLURM Resource Limits Management
%%%
%%% Implements SLURM-compatible resource limits per user, account,
%%% and partition.
%%%
%%% Limit types:
%%% - max_jobs: Maximum concurrent running jobs
%%% - max_submit: Maximum submitted (pending + running) jobs
%%% - max_tres: Maximum TRES (CPU, memory, GPU, etc.)
%%% - max_wall: Maximum wall time per job
%%% - max_nodes: Maximum nodes per job
%%% - grp_* : Group limits (shared across all users in account)
%%%
%%% Limit hierarchy (most restrictive wins):
%%% 1. QOS limits
%%% 2. Association limits (user+account+partition)
%%% 3. Account limits
%%% 4. User limits
%%% 5. Cluster defaults
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_limits).
-behaviour(gen_server).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    check_limits/1,
    check_submit_limits/1,
    get_effective_limits/3,
    set_user_limit/3,
    set_account_limit/3,
    set_partition_limit/3,
    get_user_limits/1,
    get_account_limits/1,
    get_usage/2,
    reset_usage/1,
    enforce_limit/3
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
-define(USER_LIMITS_TABLE, flurm_user_limits).
-define(ACCOUNT_LIMITS_TABLE, flurm_account_limits).
-define(PARTITION_LIMITS_TABLE, flurm_partition_limits).
-define(USAGE_TABLE, flurm_usage).

-record(limit_spec, {
    key :: {user | account | partition, binary()} |
           {user, binary(), binary()} |         % {user, User, Account}
           {user, binary(), binary(), binary()}, % {user, User, Account, Partition}
    max_jobs = 0 :: non_neg_integer(),           % 0 = unlimited
    max_submit = 0 :: non_neg_integer(),
    max_wall = 0 :: non_neg_integer(),           % seconds
    max_nodes_per_job = 0 :: non_neg_integer(),
    max_cpus_per_job = 0 :: non_neg_integer(),
    max_mem_per_job = 0 :: non_neg_integer(),    % MB
    max_tres = #{} :: map(),                     % #{tres_name => limit}
    grp_jobs = 0 :: non_neg_integer(),           % Group limits (account-wide)
    grp_submit = 0 :: non_neg_integer(),
    grp_tres = #{} :: map()
}).

-record(usage, {
    key :: {user | account, binary()} |
           {user, binary(), binary()},           % {user, User, Account}
    running_jobs = 0 :: non_neg_integer(),
    pending_jobs = 0 :: non_neg_integer(),
    tres_used = #{} :: map(),                    % Current TRES allocation
    tres_mins = #{} :: map()                     % Accumulated TRES minutes
}).

-record(state, {}).

%% Test exports
-ifdef(TEST).
-export([
    %% Limit computation
    compute_effective_limits/3,
    merge_limits/1,
    min_nonzero/2,
    merge_tres_limits/2,
    %% Limit checking
    check_max_jobs/2,
    check_grp_jobs/2,
    check_job_resources/2,
    check_resources/2,
    get_limit_field/2,
    %% Usage management
    get_or_create_usage/1,
    update_usage/2,
    %% TRES helpers
    add_tres/2,
    subtract_tres/2,
    add_tres_mins/3,
    %% Limit setting
    set_limit_field/3,
    %% Initialization
    init_default_limits/0
]).
-endif.

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Check all limits for a job (call before starting a job)
-spec check_limits(map()) -> ok | {error, term()}.
check_limits(JobSpec) ->
    gen_server:call(?SERVER, {check_limits, JobSpec}).

%% @doc Check submit limits (call before accepting job submission)
-spec check_submit_limits(map()) -> ok | {error, term()}.
check_submit_limits(JobSpec) ->
    gen_server:call(?SERVER, {check_submit_limits, JobSpec}).

%% @doc Get effective limits for a user/account/partition combination
-spec get_effective_limits(binary(), binary(), binary()) -> #limit_spec{}.
get_effective_limits(User, Account, Partition) ->
    gen_server:call(?SERVER, {get_effective, User, Account, Partition}).

%% @doc Set a limit for a user
-spec set_user_limit(binary(), atom(), term()) -> ok.
set_user_limit(User, LimitType, Value) ->
    gen_server:call(?SERVER, {set_limit, {user, User}, LimitType, Value}).

%% @doc Set a limit for an account
-spec set_account_limit(binary(), atom(), term()) -> ok.
set_account_limit(Account, LimitType, Value) ->
    gen_server:call(?SERVER, {set_limit, {account, Account}, LimitType, Value}).

%% @doc Set a limit for a partition
-spec set_partition_limit(binary(), atom(), term()) -> ok.
set_partition_limit(Partition, LimitType, Value) ->
    gen_server:call(?SERVER, {set_limit, {partition, Partition}, LimitType, Value}).

%% @doc Get all limits for a user
-spec get_user_limits(binary()) -> [#limit_spec{}].
get_user_limits(User) ->
    ets:match_object(?USER_LIMITS_TABLE, #limit_spec{key = {user, User, '_'}, _ = '_'}) ++
    ets:match_object(?USER_LIMITS_TABLE, #limit_spec{key = {user, User}, _ = '_'}).

%% @doc Get all limits for an account
-spec get_account_limits(binary()) -> #limit_spec{} | undefined.
get_account_limits(Account) ->
    case ets:lookup(?ACCOUNT_LIMITS_TABLE, {account, Account}) of
        [Limits] -> Limits;
        [] -> undefined
    end.

%% @doc Get current usage for a user or account
-spec get_usage(user | account, binary()) -> #usage{} | undefined.
get_usage(Type, Name) ->
    case ets:lookup(?USAGE_TABLE, {Type, Name}) of
        [Usage] -> Usage;
        [] -> undefined
    end.

%% @doc Reset usage counters (called at billing period start)
-spec reset_usage(user | account) -> ok.
reset_usage(Type) ->
    gen_server:call(?SERVER, {reset_usage, Type}).

%% @doc Called when a job starts/ends to enforce limits
-spec enforce_limit(start | stop, job_id(), map()) -> ok | {error, term()}.
enforce_limit(Action, JobId, JobInfo) ->
    gen_server:call(?SERVER, {enforce, Action, JobId, JobInfo}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    ets:new(?USER_LIMITS_TABLE, [
        named_table, public, set,
        {keypos, #limit_spec.key}
    ]),
    ets:new(?ACCOUNT_LIMITS_TABLE, [
        named_table, public, set,
        {keypos, #limit_spec.key}
    ]),
    ets:new(?PARTITION_LIMITS_TABLE, [
        named_table, public, set,
        {keypos, #limit_spec.key}
    ]),
    ets:new(?USAGE_TABLE, [
        named_table, public, set,
        {keypos, #usage.key}
    ]),

    %% Initialize default limits
    init_default_limits(),

    {ok, #state{}}.

handle_call({check_limits, JobSpec}, _From, State) ->
    Result = do_check_limits(JobSpec),
    {reply, Result, State};

handle_call({check_submit_limits, JobSpec}, _From, State) ->
    Result = do_check_submit_limits(JobSpec),
    {reply, Result, State};

handle_call({get_effective, User, Account, Partition}, _From, State) ->
    Limits = compute_effective_limits(User, Account, Partition),
    {reply, Limits, State};

handle_call({set_limit, Key, LimitType, Value}, _From, State) ->
    do_set_limit(Key, LimitType, Value),
    {reply, ok, State};

handle_call({reset_usage, Type}, _From, State) ->
    do_reset_usage(Type),
    {reply, ok, State};

handle_call({enforce, Action, JobId, JobInfo}, _From, State) ->
    Result = do_enforce(Action, JobId, JobInfo),
    {reply, Result, State};

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

init_default_limits() ->
    %% Set cluster-wide defaults
    DefaultLimits = #limit_spec{
        key = {partition, <<"default">>},
        max_jobs = 1000,
        max_submit = 5000,
        max_wall = 604800,        % 7 days
        max_nodes_per_job = 100,
        max_cpus_per_job = 10000,
        max_mem_per_job = 1024000  % 1 TB
    },
    ets:insert(?PARTITION_LIMITS_TABLE, DefaultLimits).

do_check_limits(JobSpec) ->
    User = maps:get(user, JobSpec),
    Account = maps:get(account, JobSpec, <<>>),
    Partition = maps:get(partition, JobSpec),

    %% Get effective limits
    Limits = compute_effective_limits(User, Account, Partition),

    %% Get current usage
    Usage = get_or_create_usage({user, User}),
    AccountUsage = get_or_create_usage({account, Account}),

    %% Check running jobs limit
    case check_max_jobs(Limits, Usage) of
        ok ->
            case check_grp_jobs(Limits, AccountUsage) of
                ok ->
                    check_job_resources(Limits, JobSpec);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

do_check_submit_limits(JobSpec) ->
    User = maps:get(user, JobSpec),
    Account = maps:get(account, JobSpec, <<>>),
    Partition = maps:get(partition, JobSpec),

    Limits = compute_effective_limits(User, Account, Partition),
    Usage = get_or_create_usage({user, User}),
    AccountUsage = get_or_create_usage({account, Account}),

    %% Check submit limits (pending + running)
    TotalJobs = Usage#usage.running_jobs + Usage#usage.pending_jobs,
    case Limits#limit_spec.max_submit of
        0 -> ok;
        Max when TotalJobs >= Max ->
            {error, {max_submit_exceeded, TotalJobs, Max}};
        _ ->
            %% Check group submit limit
            GroupTotal = AccountUsage#usage.running_jobs + AccountUsage#usage.pending_jobs,
            case Limits#limit_spec.grp_submit of
                0 -> ok;
                GrpMax when GroupTotal >= GrpMax ->
                    {error, {grp_submit_exceeded, GroupTotal, GrpMax}};
                _ ->
                    ok
            end
    end.

compute_effective_limits(User, Account, Partition) ->
    %% Start with defaults
    Default = #limit_spec{key = default},

    %% Layer on partition limits
    PartitionLimits = case ets:lookup(?PARTITION_LIMITS_TABLE, {partition, Partition}) of
        [PL] -> PL;
        [] -> Default
    end,

    %% Layer on account limits
    AccountLimits = case ets:lookup(?ACCOUNT_LIMITS_TABLE, {account, Account}) of
        [AL] -> AL;
        [] -> Default
    end,

    %% Layer on user limits
    UserLimits = case ets:lookup(?USER_LIMITS_TABLE, {user, User}) of
        [UL] -> UL;
        [] -> Default
    end,

    %% Layer on user+account+partition specific limits
    SpecificLimits = case ets:lookup(?USER_LIMITS_TABLE, {user, User, Account, Partition}) of
        [SL] -> SL;
        [] -> Default
    end,

    %% Merge limits (most restrictive wins for each field)
    merge_limits([PartitionLimits, AccountLimits, UserLimits, SpecificLimits]).

merge_limits(LimitsList) ->
    lists:foldl(fun(Limits, Acc) ->
        #limit_spec{
            key = effective,
            max_jobs = min_nonzero(Limits#limit_spec.max_jobs, Acc#limit_spec.max_jobs),
            max_submit = min_nonzero(Limits#limit_spec.max_submit, Acc#limit_spec.max_submit),
            max_wall = min_nonzero(Limits#limit_spec.max_wall, Acc#limit_spec.max_wall),
            max_nodes_per_job = min_nonzero(Limits#limit_spec.max_nodes_per_job,
                                            Acc#limit_spec.max_nodes_per_job),
            max_cpus_per_job = min_nonzero(Limits#limit_spec.max_cpus_per_job,
                                           Acc#limit_spec.max_cpus_per_job),
            max_mem_per_job = min_nonzero(Limits#limit_spec.max_mem_per_job,
                                          Acc#limit_spec.max_mem_per_job),
            max_tres = merge_tres_limits(Limits#limit_spec.max_tres, Acc#limit_spec.max_tres),
            grp_jobs = min_nonzero(Limits#limit_spec.grp_jobs, Acc#limit_spec.grp_jobs),
            grp_submit = min_nonzero(Limits#limit_spec.grp_submit, Acc#limit_spec.grp_submit),
            grp_tres = merge_tres_limits(Limits#limit_spec.grp_tres, Acc#limit_spec.grp_tres)
        }
    end, #limit_spec{key = effective}, LimitsList).

min_nonzero(0, B) -> B;
min_nonzero(A, 0) -> A;
min_nonzero(A, B) -> min(A, B).

merge_tres_limits(Map1, Map2) ->
    maps:fold(fun(K, V, Acc) ->
        case maps:get(K, Acc, 0) of
            0 -> maps:put(K, V, Acc);
            Existing -> maps:put(K, min(V, Existing), Acc)
        end
    end, Map2, Map1).

check_max_jobs(#limit_spec{max_jobs = 0}, _Usage) ->
    ok;
check_max_jobs(#limit_spec{max_jobs = Max}, #usage{running_jobs = Running}) ->
    case Running >= Max of
        true -> {error, {max_jobs_exceeded, Running, Max}};
        false -> ok
    end.

check_grp_jobs(#limit_spec{grp_jobs = 0}, _Usage) ->
    ok;
check_grp_jobs(#limit_spec{grp_jobs = Max}, #usage{running_jobs = Running}) ->
    case Running >= Max of
        true -> {error, {grp_jobs_exceeded, Running, Max}};
        false -> ok
    end.

check_job_resources(Limits, JobSpec) ->
    Checks = [
        {max_wall, time_limit, maps:get(time_limit, JobSpec, 0)},
        {max_nodes_per_job, num_nodes, maps:get(num_nodes, JobSpec, 1)},
        {max_cpus_per_job, num_cpus, maps:get(num_cpus, JobSpec, 1)},
        {max_mem_per_job, memory_mb, maps:get(memory_mb, JobSpec, 0)}
    ],

    check_resources(Checks, Limits).

check_resources([], _Limits) ->
    ok;
check_resources([{LimitField, JobField, Requested} | Rest], Limits) ->
    Limit = get_limit_field(LimitField, Limits),
    case Limit > 0 andalso Requested > Limit of
        true ->
            {error, {limit_exceeded, JobField, Requested, Limit}};
        false ->
            check_resources(Rest, Limits)
    end.

get_limit_field(max_wall, #limit_spec{max_wall = V}) -> V;
get_limit_field(max_nodes_per_job, #limit_spec{max_nodes_per_job = V}) -> V;
get_limit_field(max_cpus_per_job, #limit_spec{max_cpus_per_job = V}) -> V;
get_limit_field(max_mem_per_job, #limit_spec{max_mem_per_job = V}) -> V.

get_or_create_usage(Key) ->
    case ets:lookup(?USAGE_TABLE, Key) of
        [Usage] -> Usage;
        [] ->
            Usage = #usage{key = Key},
            ets:insert(?USAGE_TABLE, Usage),
            Usage
    end.

do_set_limit(Key, LimitType, Value) ->
    Table = case element(1, Key) of
        user -> ?USER_LIMITS_TABLE;
        account -> ?ACCOUNT_LIMITS_TABLE;
        partition -> ?PARTITION_LIMITS_TABLE
    end,

    Limits = case ets:lookup(Table, Key) of
        [L] -> L;
        [] -> #limit_spec{key = Key}
    end,

    UpdatedLimits = set_limit_field(LimitType, Value, Limits),
    ets:insert(Table, UpdatedLimits).

set_limit_field(max_jobs, V, L) -> L#limit_spec{max_jobs = V};
set_limit_field(max_submit, V, L) -> L#limit_spec{max_submit = V};
set_limit_field(max_wall, V, L) -> L#limit_spec{max_wall = V};
set_limit_field(max_nodes_per_job, V, L) -> L#limit_spec{max_nodes_per_job = V};
set_limit_field(max_cpus_per_job, V, L) -> L#limit_spec{max_cpus_per_job = V};
set_limit_field(max_mem_per_job, V, L) -> L#limit_spec{max_mem_per_job = V};
set_limit_field(max_tres, V, L) -> L#limit_spec{max_tres = V};
set_limit_field(grp_jobs, V, L) -> L#limit_spec{grp_jobs = V};
set_limit_field(grp_submit, V, L) -> L#limit_spec{grp_submit = V};
set_limit_field(grp_tres, V, L) -> L#limit_spec{grp_tres = V}.

do_reset_usage(Type) ->
    Pattern = #usage{key = {Type, '_'}, _ = '_'},
    Usages = ets:match_object(?USAGE_TABLE, Pattern),
    lists:foreach(fun(#usage{key = Key}) ->
        ets:insert(?USAGE_TABLE, #usage{key = Key})
    end, Usages).

do_enforce(start, _JobId, JobInfo) ->
    User = maps:get(user, JobInfo),
    Account = maps:get(account, JobInfo, <<>>),
    TRES = maps:get(tres, JobInfo, #{}),

    %% Increment running job counts
    update_usage({user, User}, fun(U) ->
        U#usage{running_jobs = U#usage.running_jobs + 1,
                pending_jobs = max(0, U#usage.pending_jobs - 1),
                tres_used = add_tres(U#usage.tres_used, TRES)}
    end),

    case Account of
        <<>> -> ok;
        _ ->
            update_usage({account, Account}, fun(U) ->
                U#usage{running_jobs = U#usage.running_jobs + 1,
                        pending_jobs = max(0, U#usage.pending_jobs - 1),
                        tres_used = add_tres(U#usage.tres_used, TRES)}
            end)
    end,
    ok;

do_enforce(stop, _JobId, JobInfo) ->
    User = maps:get(user, JobInfo),
    Account = maps:get(account, JobInfo, <<>>),
    TRES = maps:get(tres, JobInfo, #{}),
    WallTime = maps:get(wall_time, JobInfo, 0),

    %% Decrement running job counts and update TRES minutes
    update_usage({user, User}, fun(U) ->
        TRESMins = add_tres_mins(U#usage.tres_mins, TRES, WallTime),
        U#usage{running_jobs = max(0, U#usage.running_jobs - 1),
                tres_used = subtract_tres(U#usage.tres_used, TRES),
                tres_mins = TRESMins}
    end),

    case Account of
        <<>> -> ok;
        _ ->
            update_usage({account, Account}, fun(U) ->
                TRESMins = add_tres_mins(U#usage.tres_mins, TRES, WallTime),
                U#usage{running_jobs = max(0, U#usage.running_jobs - 1),
                        tres_used = subtract_tres(U#usage.tres_used, TRES),
                        tres_mins = TRESMins}
            end)
    end,
    ok.

update_usage(Key, UpdateFun) ->
    Usage = get_or_create_usage(Key),
    UpdatedUsage = UpdateFun(Usage),
    ets:insert(?USAGE_TABLE, UpdatedUsage).

add_tres(Map1, Map2) ->
    maps:fold(fun(K, V, Acc) ->
        maps:update_with(K, fun(E) -> E + V end, V, Acc)
    end, Map1, Map2).

subtract_tres(Map1, Map2) ->
    maps:fold(fun(K, V, Acc) ->
        maps:update_with(K, fun(E) -> max(0, E - V) end, 0, Acc)
    end, Map1, Map2).

add_tres_mins(TRESMins, TRES, WallTimeSeconds) ->
    WallTimeMins = WallTimeSeconds / 60,
    maps:fold(fun(K, V, Acc) ->
        Mins = V * WallTimeMins,
        maps:update_with(K, fun(E) -> E + Mins end, Mins, Acc)
    end, TRESMins, TRES).
