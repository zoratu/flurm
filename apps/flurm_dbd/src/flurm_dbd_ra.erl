%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Ra State Machine for Accounting
%%%
%%% Implements the ra_machine behaviour for distributed consensus on
%%% accounting data. This module replicates job completion records,
%%% TRES usage, and aggregated usage data across all DBD nodes.
%%%
%%% Phase 7C-1: Native FLURM accounting with Ra consensus
%%%
%%% Commands (replicated through Raft):
%%% - record_job: Record a completed job with full accounting data
%%% - update_tres: Update TRES usage for user/account/cluster
%%% - query_jobs: Query jobs with filters (consistent read)
%%%
%%% State:
%%% - job_records: Map of job_id -> job accounting record
%%% - recorded_jobs: Set of job IDs to prevent double-counting
%%% - usage_records: Aggregated TRES usage by entity (user/account)
%%% - user_totals: Running totals per user
%%% - account_totals: Running totals per account
%%%
%%% Safety Invariants (from TLA+ FlurmAccounting spec):
%%% - TRES Consistency: Same job has same TRES across all nodes
%%% - No Double Counting: Each job appears at most once in the log
%%% - Committed Records Durable: Committed records are never lost
%%% - TRES Non-Negative: All TRES values are >= 0
%%% - Log Prefix Consistency: Committed logs are prefix-consistent
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_ra).
-behaviour(ra_machine).

%% Disable auto-import of apply/3 to avoid clash with ra_machine callback
-compile({no_auto_import, [apply/3]}).

%% Suppress warnings for internal helper functions that are planned for future use
-compile([{nowarn_unused_function, [{make_usage_record, 1},
                                    {aggregate_tres, 2},
                                    {job_recorded_set, 1}]}]).

-include_lib("flurm_core/include/flurm_core.hrl").

%% Ra machine callbacks
-export([init/1, apply/3, state_enter/2, snapshot_module/0]).

%% API - Commands (replicated)
-export([
    start_cluster/1,
    record_job/1,              %% Phase 7C-1: record_job command
    record_job_completion/1,   %% Alias for record_job
    update_tres/1,             %% Phase 7C-1: update_tres command
    update_usage/1,            %% Alias for update_tres
    query_jobs/1,              %% Phase 7C-1: query_jobs command (consistent)
    query_tres/1
]).

%% API - Queries (local read)
-export([
    get_job_record/1,
    list_job_records/0,
    list_job_records/1,
    get_user_usage/1,
    get_account_usage/1,
    get_tres_usage/3,
    is_job_recorded/1,
    get_user_totals/1,         %% Phase 7C-1: user totals
    get_account_totals/1       %% Phase 7C-1: account totals
]).

%% API - Consistent queries (through leader)
-export([
    consistent_get_job_record/1,
    consistent_list_job_records/0
]).

%% Internal exports for testing
-ifdef(TEST).
-export([
    make_job_record/1,
    make_usage_record/1,
    aggregate_tres/2,
    job_recorded_set/1,
    calculate_tres_from_job/1
]).
-endif.

%%====================================================================
%% Records
%%====================================================================

%% Job accounting record
-record(job_record, {
    job_id          :: pos_integer(),
    job_name        :: binary(),
    user_name       :: binary(),
    user_id         :: non_neg_integer(),
    group_id        :: non_neg_integer(),
    account         :: binary(),
    partition       :: binary(),
    cluster         :: binary(),
    qos             :: binary(),
    state           :: atom(),
    exit_code       :: integer(),
    num_nodes       :: pos_integer(),
    num_cpus        :: pos_integer(),
    num_tasks       :: pos_integer(),
    req_mem         :: non_neg_integer(),
    submit_time     :: non_neg_integer(),
    start_time      :: non_neg_integer(),
    end_time        :: non_neg_integer(),
    elapsed         :: non_neg_integer(),
    tres_alloc      :: map(),
    tres_req        :: map()
}).

%% TRES usage record for aggregated tracking
-record(usage_record, {
    key             :: {user | account | cluster, binary(), binary()},
    %% key format: {EntityType, EntityId, Period}
    period          :: binary(),    %% "YYYY-MM" or "YYYY-MM-DD"
    cpu_seconds     :: non_neg_integer(),
    mem_seconds     :: non_neg_integer(),
    gpu_seconds     :: non_neg_integer(),
    node_seconds    :: non_neg_integer(),
    job_count       :: non_neg_integer(),
    job_time        :: non_neg_integer()
}).

%% TRES usage input for update_usage command
-record(tres_usage, {
    entity_type     :: user | account | cluster,
    entity_id       :: binary(),
    period          :: binary(),
    cpu_seconds     :: non_neg_integer(),
    mem_seconds     :: non_neg_integer(),
    gpu_seconds     :: non_neg_integer(),
    node_seconds    :: non_neg_integer(),
    job_count       :: non_neg_integer(),
    job_time        :: non_neg_integer()
}).

%% Ra state machine state
-record(ra_acct_state, {
    %% Job records indexed by job_id
    job_records     = #{} :: #{pos_integer() => #job_record{}},
    %% Set of recorded job IDs to prevent double-counting
    recorded_jobs   = sets:new() :: sets:set(pos_integer()),
    %% Usage records indexed by {EntityType, EntityId, Period}
    usage_records   = #{} :: #{tuple() => #usage_record{}},
    %% User totals: #{username => #{cpu_seconds => N, mem_seconds => N, ...}}
    user_totals     = #{} :: #{binary() => map()},
    %% Account totals: #{account => #{cpu_seconds => N, mem_seconds => N, ...}}
    account_totals  = #{} :: #{binary() => map()},
    %% Version for state machine evolution
    version         = 1 :: pos_integer()
}).

%%====================================================================
%% Configuration
%%====================================================================

-define(RA_CLUSTER_NAME, flurm_dbd_ra).
-define(RA_TIMEOUT, 5000).

%%====================================================================
%% Ra Machine Callbacks
%%====================================================================

%% @doc Initialize the Ra state machine.
-spec init(term()) -> #ra_acct_state{}.
init(_Config) ->
    #ra_acct_state{
        job_records = #{},
        recorded_jobs = sets:new(),
        usage_records = #{},
        user_totals = #{},
        account_totals = #{},
        version = 1
    }.

%% @doc Apply a command to the state machine.
%% Returns {State, Result, Effects}
-spec apply(ra_machine:command_meta_data(), term(), #ra_acct_state{}) ->
    {#ra_acct_state{}, term(), ra_machine:effects()}.

%% Record a job (Phase 7C-1 command) - prevents double counting
apply(_Meta, {record_job, JobInfo}, State) when is_map(JobInfo) ->
    JobId = maps:get(job_id, JobInfo),
    %% Check if job already recorded (TLA+ NoDoubleCounting invariant)
    case sets:is_element(JobId, State#ra_acct_state.recorded_jobs) of
        true ->
            %% Job already recorded - return error but don't fail
            {State, {error, already_recorded}, []};
        false ->
            %% Record the job
            Record = make_job_record(JobInfo),
            NewRecordedJobs = sets:add_element(JobId, State#ra_acct_state.recorded_jobs),
            NewJobRecords = maps:put(JobId, Record, State#ra_acct_state.job_records),

            %% Calculate TRES usage for this job
            TresUsage = calculate_tres_from_job(Record),

            %% Update usage aggregates by period
            UsageKey = {user, Record#job_record.user_name, current_period()},
            NewUsage = update_usage_from_job(Record, UsageKey, State#ra_acct_state.usage_records),

            %% Also update account usage
            AcctKey = {account, Record#job_record.account, current_period()},
            NewUsage2 = update_usage_from_job(Record, AcctKey, NewUsage),

            %% Update running user totals
            UserName = Record#job_record.user_name,
            NewUserTotals = update_entity_totals(
                UserName,
                TresUsage,
                State#ra_acct_state.user_totals
            ),

            %% Update running account totals
            Account = Record#job_record.account,
            NewAccountTotals = update_entity_totals(
                Account,
                TresUsage,
                State#ra_acct_state.account_totals
            ),

            NewState = State#ra_acct_state{
                job_records = NewJobRecords,
                recorded_jobs = NewRecordedJobs,
                usage_records = NewUsage2,
                user_totals = NewUserTotals,
                account_totals = NewAccountTotals
            },
            Effects = [{mod_call, flurm_dbd_ra_effects, job_recorded, [Record]}],
            {NewState, {ok, JobId}, Effects}
    end;

%% Alias: record_job_completion -> record_job
apply(Meta, {record_job_completion, JobInfo}, State) ->
    apply(Meta, {record_job, JobInfo}, State);

%% Update TRES usage (Phase 7C-1 command)
apply(_Meta, {update_tres, TresUpdate}, State) when is_map(TresUpdate) ->
    EntityType = maps:get(entity_type, TresUpdate),
    EntityId = maps:get(entity_id, TresUpdate),
    Period = maps:get(period, TresUpdate, current_period()),
    Key = {EntityType, EntityId, Period},

    TresValues = #{
        cpu_seconds => maps:get(cpu_seconds, TresUpdate, 0),
        mem_seconds => maps:get(mem_seconds, TresUpdate, 0),
        gpu_seconds => maps:get(gpu_seconds, TresUpdate, 0),
        node_seconds => maps:get(node_seconds, TresUpdate, 0),
        job_count => maps:get(job_count, TresUpdate, 0),
        job_time => maps:get(job_time, TresUpdate, 0)
    },

    Existing = maps:get(Key, State#ra_acct_state.usage_records, undefined),
    NewUsage = case Existing of
        undefined ->
            #usage_record{
                key = Key,
                period = Period,
                cpu_seconds = maps:get(cpu_seconds, TresValues),
                mem_seconds = maps:get(mem_seconds, TresValues),
                gpu_seconds = maps:get(gpu_seconds, TresValues),
                node_seconds = maps:get(node_seconds, TresValues),
                job_count = maps:get(job_count, TresValues),
                job_time = maps:get(job_time, TresValues)
            };
        _ ->
            %% Aggregate with existing
            Existing#usage_record{
                cpu_seconds = Existing#usage_record.cpu_seconds + maps:get(cpu_seconds, TresValues),
                mem_seconds = Existing#usage_record.mem_seconds + maps:get(mem_seconds, TresValues),
                gpu_seconds = Existing#usage_record.gpu_seconds + maps:get(gpu_seconds, TresValues),
                node_seconds = Existing#usage_record.node_seconds + maps:get(node_seconds, TresValues),
                job_count = Existing#usage_record.job_count + maps:get(job_count, TresValues),
                job_time = Existing#usage_record.job_time + maps:get(job_time, TresValues)
            }
    end,

    %% Also update running totals
    NewTotals = case EntityType of
        user ->
            update_entity_totals(EntityId, TresValues, State#ra_acct_state.user_totals);
        account ->
            update_entity_totals(EntityId, TresValues, State#ra_acct_state.account_totals);
        _ ->
            State#ra_acct_state.user_totals  %% No change for cluster or other types
    end,

    NewUsageRecords = maps:put(Key, NewUsage, State#ra_acct_state.usage_records),
    NewState = case EntityType of
        user ->
            State#ra_acct_state{usage_records = NewUsageRecords, user_totals = NewTotals};
        account ->
            State#ra_acct_state{usage_records = NewUsageRecords, account_totals = NewTotals};
        _ ->
            State#ra_acct_state{usage_records = NewUsageRecords}
    end,
    {NewState, ok, []};

%% Alias: update_usage with #tres_usage{} record
apply(_Meta, {update_usage, #tres_usage{} = TresUsage}, State) ->
    Key = {TresUsage#tres_usage.entity_type,
           TresUsage#tres_usage.entity_id,
           TresUsage#tres_usage.period},

    Existing = maps:get(Key, State#ra_acct_state.usage_records, undefined),
    NewUsage = case Existing of
        undefined ->
            #usage_record{
                key = Key,
                period = TresUsage#tres_usage.period,
                cpu_seconds = TresUsage#tres_usage.cpu_seconds,
                mem_seconds = TresUsage#tres_usage.mem_seconds,
                gpu_seconds = TresUsage#tres_usage.gpu_seconds,
                node_seconds = TresUsage#tres_usage.node_seconds,
                job_count = TresUsage#tres_usage.job_count,
                job_time = TresUsage#tres_usage.job_time
            };
        _ ->
            %% Aggregate with existing
            Existing#usage_record{
                cpu_seconds = Existing#usage_record.cpu_seconds + TresUsage#tres_usage.cpu_seconds,
                mem_seconds = Existing#usage_record.mem_seconds + TresUsage#tres_usage.mem_seconds,
                gpu_seconds = Existing#usage_record.gpu_seconds + TresUsage#tres_usage.gpu_seconds,
                node_seconds = Existing#usage_record.node_seconds + TresUsage#tres_usage.node_seconds,
                job_count = Existing#usage_record.job_count + TresUsage#tres_usage.job_count,
                job_time = Existing#usage_record.job_time + TresUsage#tres_usage.job_time
            }
    end,

    NewUsageRecords = maps:put(Key, NewUsage, State#ra_acct_state.usage_records),
    NewState = State#ra_acct_state{usage_records = NewUsageRecords},
    {NewState, ok, []};

%% Query jobs (replicated for consistency)
apply(_Meta, {query_jobs, Filters}, State) when is_map(Filters) ->
    Jobs = filter_jobs(State#ra_acct_state.job_records, Filters),
    {State, {ok, Jobs}, []};

%% Query TRES usage
apply(_Meta, {query_tres, {EntityType, EntityId, Period}}, State) ->
    Key = {EntityType, EntityId, Period},
    case maps:find(Key, State#ra_acct_state.usage_records) of
        {ok, Usage} ->
            {State, {ok, usage_record_to_map(Usage)}, []};
        error ->
            {State, {error, not_found}, []}
    end;

%% Unknown command
apply(_Meta, Unknown, State) ->
    {State, {error, {unknown_command, Unknown}}, []}.

%% @doc Called when the Ra server enters a new state
-spec state_enter(ra_server:ra_state() | eol, #ra_acct_state{}) -> ra_machine:effects().
state_enter(leader, _State) ->
    [{mod_call, flurm_dbd_ra_effects, became_leader, [node()]}];
state_enter(follower, _State) ->
    [{mod_call, flurm_dbd_ra_effects, became_follower, [node()]}];
state_enter(_RaState, _State) ->
    [].

%% @doc Return the snapshot module
-spec snapshot_module() -> module().
snapshot_module() ->
    ra_machine_simple.

%%====================================================================
%% API - Cluster Management
%%====================================================================

%% @doc Start a Ra cluster for the DBD accounting.
-spec start_cluster([node()]) -> ok | {error, term()}.
start_cluster(Nodes) when is_list(Nodes) ->
    ClusterName = ?RA_CLUSTER_NAME,
    Servers = [{ClusterName, N} || N <- Nodes],
    MachineConfig = {module, ?MODULE, #{}},

    case ra:start_cluster(default, ClusterName, MachineConfig, Servers) of
        {ok, Started, _NotStarted} when length(Started) > 0 ->
            ok;
        {ok, [], NotStarted} ->
            {error, {no_servers_started, NotStarted}};
        {error, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% API - Commands (Replicated)
%%====================================================================

%% @doc Record a job with full accounting data.
%% Phase 7C-1: Primary command for recording completed jobs.
%% Prevents double-counting by checking if job already recorded.
%% Also updates user and account totals.
-spec record_job(map()) -> {ok, pos_integer()} | {error, term()}.
record_job(JobInfo) when is_map(JobInfo) ->
    ra_command({record_job, JobInfo}).

%% @doc Alias for record_job (backward compatibility).
-spec record_job_completion(map()) -> {ok, pos_integer()} | {error, term()}.
record_job_completion(JobInfo) when is_map(JobInfo) ->
    record_job(JobInfo).

%% @doc Update TRES usage for user/account/cluster.
%% Phase 7C-1: Command to update TRES counters.
%% TresUpdate should contain:
%% - entity_type: user | account | cluster
%% - entity_id: binary name of entity
%% - period: optional period string (defaults to current month)
%% - cpu_seconds, mem_seconds, gpu_seconds, node_seconds: TRES values
-spec update_tres(map()) -> ok | {error, term()}.
update_tres(TresUpdate) when is_map(TresUpdate) ->
    ra_command({update_tres, TresUpdate}).

%% @doc Alias for update_tres (backward compatibility).
-spec update_usage(map()) -> ok | {error, term()}.
update_usage(TresUsage) when is_map(TresUsage) ->
    Usage = #tres_usage{
        entity_type = maps:get(entity_type, TresUsage),
        entity_id = maps:get(entity_id, TresUsage),
        period = maps:get(period, TresUsage, current_period()),
        cpu_seconds = maps:get(cpu_seconds, TresUsage, 0),
        mem_seconds = maps:get(mem_seconds, TresUsage, 0),
        gpu_seconds = maps:get(gpu_seconds, TresUsage, 0),
        node_seconds = maps:get(node_seconds, TresUsage, 0),
        job_count = maps:get(job_count, TresUsage, 0),
        job_time = maps:get(job_time, TresUsage, 0)
    },
    ra_command({update_usage, Usage}).

%% @doc Query jobs with filters (consistent read).
%% Phase 7C-1: Consistent query through Raft leader.
%% Filters can include:
%% - user_name: filter by user
%% - account: filter by account
%% - partition: filter by partition
%% - state: filter by job state
%% - start_time: jobs starting after this timestamp
%% - end_time: jobs ending before this timestamp
-spec query_jobs(map()) -> {ok, [map()]} | {error, term()}.
query_jobs(Filters) when is_map(Filters) ->
    ra_command({query_jobs, Filters}).

%% @doc Query TRES usage (consistent read).
-spec query_tres({user | account | cluster, binary(), binary()}) ->
    {ok, map()} | {error, term()}.
query_tres({EntityType, EntityId, Period}) ->
    ra_command({query_tres, {EntityType, EntityId, Period}}).

%%====================================================================
%% API - Queries (Local Read - May Be Stale)
%%====================================================================

%% @doc Get a job record by ID (local read, may be stale).
-spec get_job_record(pos_integer()) -> {ok, map()} | {error, not_found}.
get_job_record(JobId) when is_integer(JobId) ->
    case ra_local_query(fun(State) ->
        case maps:find(JobId, State#ra_acct_state.job_records) of
            {ok, Record} -> {ok, job_record_to_map(Record)};
            error -> {error, not_found}
        end
    end) of
        {ok, Result} -> Result;
        {error, not_found} -> {error, not_found};
        Other -> Other
    end.

%% @doc List all job records (local read, may be stale).
-spec list_job_records() -> {ok, [map()]}.
list_job_records() ->
    ra_local_query(fun(State) ->
        Records = [job_record_to_map(R) || R <- maps:values(State#ra_acct_state.job_records)],
        {ok, Records}
    end).

%% @doc List job records with filters (local read, may be stale).
-spec list_job_records(map()) -> {ok, [map()]}.
list_job_records(Filters) when is_map(Filters) ->
    ra_local_query(fun(State) ->
        Jobs = filter_jobs(State#ra_acct_state.job_records, Filters),
        {ok, Jobs}
    end).

%% @doc Get usage for a user (local read, may be stale).
-spec get_user_usage(binary()) -> {ok, map()} | {error, not_found}.
get_user_usage(Username) when is_binary(Username) ->
    get_tres_usage(user, Username, current_period()).

%% @doc Get usage for an account (local read, may be stale).
-spec get_account_usage(binary()) -> {ok, map()} | {error, not_found}.
get_account_usage(Account) when is_binary(Account) ->
    get_tres_usage(account, Account, current_period()).

%% @doc Get TRES usage for an entity (local read, may be stale).
-spec get_tres_usage(user | account | cluster, binary(), binary()) ->
    {ok, map()} | {error, not_found}.
get_tres_usage(EntityType, EntityId, Period) ->
    Key = {EntityType, EntityId, Period},
    case ra_local_query(fun(State) ->
        case maps:find(Key, State#ra_acct_state.usage_records) of
            {ok, Usage} -> {ok, usage_record_to_map(Usage)};
            error -> {error, not_found}
        end
    end) of
        {ok, Result} -> Result;
        {error, not_found} -> {error, not_found};
        Other -> Other
    end.

%% @doc Check if a job is already recorded (local read).
-spec is_job_recorded(pos_integer()) -> boolean().
is_job_recorded(JobId) when is_integer(JobId) ->
    case ra_local_query(fun(State) ->
        sets:is_element(JobId, State#ra_acct_state.recorded_jobs)
    end) of
        true -> true;
        false -> false;
        _ -> false
    end.

%% @doc Get running totals for a user (local read, may be stale).
%% Phase 7C-1: User totals include accumulated TRES across all periods.
-spec get_user_totals(binary()) -> {ok, map()} | {error, not_found}.
get_user_totals(Username) when is_binary(Username) ->
    case ra_local_query(fun(State) ->
        case maps:find(Username, State#ra_acct_state.user_totals) of
            {ok, Totals} -> {ok, Totals};
            error -> {error, not_found}
        end
    end) of
        {ok, Totals} -> {ok, Totals};
        {error, not_found} -> {error, not_found};
        Other -> Other
    end.

%% @doc Get running totals for an account (local read, may be stale).
%% Phase 7C-1: Account totals include accumulated TRES across all periods.
-spec get_account_totals(binary()) -> {ok, map()} | {error, not_found}.
get_account_totals(Account) when is_binary(Account) ->
    case ra_local_query(fun(State) ->
        case maps:find(Account, State#ra_acct_state.account_totals) of
            {ok, Totals} -> {ok, Totals};
            error -> {error, not_found}
        end
    end) of
        {ok, Totals} -> {ok, Totals};
        {error, not_found} -> {error, not_found};
        Other -> Other
    end.

%%====================================================================
%% API - Consistent Queries (Through Leader)
%%====================================================================

%% @doc Get a job record by ID (consistent read through leader).
-spec consistent_get_job_record(pos_integer()) -> {ok, map()} | {error, term()}.
consistent_get_job_record(JobId) when is_integer(JobId) ->
    ra_consistent_query(fun(State) ->
        case maps:find(JobId, State#ra_acct_state.job_records) of
            {ok, Record} -> {ok, job_record_to_map(Record)};
            error -> {error, not_found}
        end
    end).

%% @doc List all job records (consistent read through leader).
-spec consistent_list_job_records() -> {ok, [map()]} | {error, term()}.
consistent_list_job_records() ->
    ra_consistent_query(fun(State) ->
        Records = [job_record_to_map(R) || R <- maps:values(State#ra_acct_state.job_records)],
        {ok, Records}
    end).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Execute a command through Ra (replicated).
ra_command(Command) ->
    Server = {?RA_CLUSTER_NAME, node()},
    case ra:process_command(Server, Command, ?RA_TIMEOUT) of
        {ok, Result, _Leader} ->
            Result;
        {timeout, _} ->
            {error, timeout};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Execute a local query (may return stale data).
ra_local_query(QueryFun) ->
    Server = {?RA_CLUSTER_NAME, node()},
    case ra:local_query(Server, QueryFun, ?RA_TIMEOUT) of
        {ok, {_RaftIdx, Result}, _Leader} ->
            Result;
        {timeout, _} ->
            {error, timeout};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Execute a consistent query through the leader.
ra_consistent_query(QueryFun) ->
    Server = {?RA_CLUSTER_NAME, node()},
    case ra:consistent_query(Server, QueryFun, ?RA_TIMEOUT) of
        {ok, {_RaftIdx, Result}, _Leader} ->
            Result;
        {timeout, _} ->
            {error, timeout};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Create a job record from job info map.
make_job_record(JobInfo) when is_map(JobInfo) ->
    Now = erlang:system_time(second),
    #job_record{
        job_id = maps:get(job_id, JobInfo),
        job_name = maps:get(job_name, JobInfo, <<>>),
        user_name = maps:get(user_name, JobInfo, <<>>),
        user_id = maps:get(user_id, JobInfo, 0),
        group_id = maps:get(group_id, JobInfo, 0),
        account = maps:get(account, JobInfo, <<>>),
        partition = maps:get(partition, JobInfo, <<>>),
        cluster = maps:get(cluster, JobInfo, <<"flurm">>),
        qos = maps:get(qos, JobInfo, <<"normal">>),
        state = maps:get(state, JobInfo, completed),
        exit_code = maps:get(exit_code, JobInfo, 0),
        num_nodes = maps:get(num_nodes, JobInfo, 1),
        num_cpus = maps:get(num_cpus, JobInfo, 1),
        num_tasks = maps:get(num_tasks, JobInfo, 1),
        req_mem = maps:get(req_mem, JobInfo, 0),
        submit_time = maps:get(submit_time, JobInfo, Now),
        start_time = maps:get(start_time, JobInfo, 0),
        end_time = maps:get(end_time, JobInfo, Now),
        elapsed = maps:get(elapsed, JobInfo, 0),
        tres_alloc = maps:get(tres_alloc, JobInfo, #{}),
        tres_req = maps:get(tres_req, JobInfo, #{})
    }.

%% @private Convert job record to map.
job_record_to_map(#job_record{} = R) ->
    #{
        job_id => R#job_record.job_id,
        job_name => R#job_record.job_name,
        user_name => R#job_record.user_name,
        user_id => R#job_record.user_id,
        group_id => R#job_record.group_id,
        account => R#job_record.account,
        partition => R#job_record.partition,
        cluster => R#job_record.cluster,
        qos => R#job_record.qos,
        state => R#job_record.state,
        exit_code => R#job_record.exit_code,
        num_nodes => R#job_record.num_nodes,
        num_cpus => R#job_record.num_cpus,
        num_tasks => R#job_record.num_tasks,
        req_mem => R#job_record.req_mem,
        submit_time => R#job_record.submit_time,
        start_time => R#job_record.start_time,
        end_time => R#job_record.end_time,
        elapsed => R#job_record.elapsed,
        tres_alloc => R#job_record.tres_alloc,
        tres_req => R#job_record.tres_req
    }.

%% @private Convert usage record to map.
usage_record_to_map(#usage_record{} = U) ->
    #{
        period => U#usage_record.period,
        cpu_seconds => U#usage_record.cpu_seconds,
        mem_seconds => U#usage_record.mem_seconds,
        gpu_seconds => U#usage_record.gpu_seconds,
        node_seconds => U#usage_record.node_seconds,
        job_count => U#usage_record.job_count,
        job_time => U#usage_record.job_time
    }.

%% @private Make a usage record from a map (for testing).
make_usage_record(Usage) when is_map(Usage) ->
    #usage_record{
        key = maps:get(key, Usage),
        period = maps:get(period, Usage),
        cpu_seconds = maps:get(cpu_seconds, Usage, 0),
        mem_seconds = maps:get(mem_seconds, Usage, 0),
        gpu_seconds = maps:get(gpu_seconds, Usage, 0),
        node_seconds = maps:get(node_seconds, Usage, 0),
        job_count = maps:get(job_count, Usage, 0),
        job_time = maps:get(job_time, Usage, 0)
    }.

%% @private Update usage records from a job record.
update_usage_from_job(#job_record{} = Job, Key, UsageRecords) ->
    %% Calculate TRES from job
    Elapsed = Job#job_record.elapsed,
    CpuSeconds = Elapsed * Job#job_record.num_cpus,
    MemSeconds = Elapsed * Job#job_record.req_mem,
    NodeSeconds = Elapsed * Job#job_record.num_nodes,
    GpuSeconds = Elapsed * maps:get(gpu, Job#job_record.tres_alloc, 0),

    Existing = maps:get(Key, UsageRecords, undefined),
    NewUsage = case Existing of
        undefined ->
            {_, _, Period} = Key,
            #usage_record{
                key = Key,
                period = Period,
                cpu_seconds = CpuSeconds,
                mem_seconds = MemSeconds,
                gpu_seconds = GpuSeconds,
                node_seconds = NodeSeconds,
                job_count = 1,
                job_time = Elapsed
            };
        _ ->
            Existing#usage_record{
                cpu_seconds = Existing#usage_record.cpu_seconds + CpuSeconds,
                mem_seconds = Existing#usage_record.mem_seconds + MemSeconds,
                gpu_seconds = Existing#usage_record.gpu_seconds + GpuSeconds,
                node_seconds = Existing#usage_record.node_seconds + NodeSeconds,
                job_count = Existing#usage_record.job_count + 1,
                job_time = Existing#usage_record.job_time + Elapsed
            }
    end,
    maps:put(Key, NewUsage, UsageRecords).

%% @private Filter jobs based on criteria.
filter_jobs(JobRecords, Filters) ->
    Jobs = maps:values(JobRecords),
    Filtered = lists:filter(fun(Job) -> matches_filters(Job, Filters) end, Jobs),
    [job_record_to_map(J) || J <- Filtered].

%% @private Check if job matches all filters.
matches_filters(#job_record{} = Job, Filters) ->
    maps:fold(fun
        (user, V, Acc) -> Acc andalso Job#job_record.user_name =:= V;
        (user_name, V, Acc) -> Acc andalso Job#job_record.user_name =:= V;
        (account, V, Acc) -> Acc andalso Job#job_record.account =:= V;
        (partition, V, Acc) -> Acc andalso Job#job_record.partition =:= V;
        (state, V, Acc) -> Acc andalso Job#job_record.state =:= V;
        (start_time, V, Acc) -> Acc andalso Job#job_record.start_time >= V;
        (end_time, V, Acc) -> Acc andalso Job#job_record.end_time =< V;
        (_, _, Acc) -> Acc
    end, true, Filters).

%% @private Get current period (YYYY-MM format).
current_period() ->
    {{Year, Month, _Day}, _} = calendar:local_time(),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

%% @private Aggregate two TRES maps (for testing).
%% Delegates to flurm_tres:add/2 for consistency
aggregate_tres(Tres1, Tres2) when is_map(Tres1), is_map(Tres2) ->
    flurm_tres:add(Tres1, Tres2).

%% @private Get the set of recorded job IDs (for testing).
job_recorded_set(#ra_acct_state{recorded_jobs = Set}) ->
    Set.

%% @private Calculate TRES usage from a job record.
%% Delegates to flurm_tres:from_job/1 for consistency
calculate_tres_from_job(#job_record{} = Job) ->
    flurm_tres:from_job(#{
        elapsed => Job#job_record.elapsed,
        num_cpus => Job#job_record.num_cpus,
        req_mem => Job#job_record.req_mem,
        num_nodes => Job#job_record.num_nodes,
        tres_alloc => Job#job_record.tres_alloc
    }).

%% @private Update entity (user/account) running totals.
%% Uses flurm_tres:add/2 for consistent aggregation
update_entity_totals(EntityId, TresUsage, Totals) when is_binary(EntityId) ->
    Existing = maps:get(EntityId, Totals, flurm_tres:zero()),
    Updated = flurm_tres:add(Existing, TresUsage),
    maps:put(EntityId, Updated, Totals);
update_entity_totals(<<>>, _TresUsage, Totals) ->
    %% Empty entity ID, don't update
    Totals.

