%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Server
%%%
%%% Main server for the accounting daemon. Handles:
%%% - Job accounting record storage
%%% - User/account/association management
%%% - Usage aggregation and reporting
%%% - Communication with slurmctld and sacctmgr
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_server).

-behaviour(gen_server).

-export([start_link/0]).

%% Job accounting
-export([
    record_job_start/1,
    record_job_end/1,
    record_job_step/1,
    get_job_record/1,
    list_job_records/0,
    list_job_records/1
]).

%% Lifecycle event accounting (called from flurm_job state machine)
-export([
    record_job_submit/1,
    record_job_start/2,
    record_job_end/3,
    record_job_cancelled/2
]).

%% Usage tracking
-export([
    get_user_usage/1,
    get_account_usage/1,
    get_cluster_usage/0,
    reset_usage/0
]).

%% sacct-style queries
-export([
    get_jobs_by_user/1,
    get_jobs_by_account/1,
    get_jobs_in_range/2,
    format_sacct_output/1
]).

%% Association management
-export([
    add_association/3,
    add_association/4,
    get_associations/1,
    get_association/1,
    remove_association/1,
    update_association/2,
    get_user_associations/1,
    get_account_associations/1
]).

%% Archive/purge operations
-export([
    archive_old_jobs/1,
    purge_old_jobs/1,
    get_archived_jobs/0,
    get_archived_jobs/1
]).

%% Admin operations
-export([
    archive_old_records/1,
    purge_old_records/1,
    get_stats/0
]).

%% TRES usage calculation (detailed tracking)
-export([
    calculate_user_tres_usage/1,
    calculate_account_tres_usage/1,
    calculate_cluster_tres_usage/0
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("flurm_core/include/flurm_core.hrl").

%% Job accounting record
-record(job_record, {
    job_id :: pos_integer(),
    job_name :: binary(),
    user_name :: binary(),
    user_id :: non_neg_integer(),
    group_id :: non_neg_integer(),
    account :: binary(),
    partition :: binary(),
    cluster :: binary(),
    qos :: binary(),
    state :: atom(),
    exit_code :: integer(),
    num_nodes :: pos_integer(),
    num_cpus :: pos_integer(),
    num_tasks :: pos_integer(),
    req_mem :: non_neg_integer(),
    submit_time :: non_neg_integer(),
    eligible_time :: non_neg_integer(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer(),
    elapsed :: non_neg_integer(),
    tres_alloc :: map(),
    tres_req :: map(),
    work_dir :: binary(),
    std_out :: binary(),
    std_err :: binary()
}).

%% Step record
-record(step_record, {
    job_id :: pos_integer(),
    step_id :: non_neg_integer(),
    step_name :: binary(),
    state :: atom(),
    exit_code :: integer(),
    num_tasks :: pos_integer(),
    num_nodes :: pos_integer(),
    start_time :: non_neg_integer(),
    end_time :: non_neg_integer(),
    elapsed :: non_neg_integer(),
    tres_alloc :: map()
}).

%% Usage record
-record(usage_record, {
    key :: {binary(), binary()},  % {account/user, period}
    cluster :: binary(),
    account :: binary(),
    user :: binary(),
    period :: binary(),           % "YYYY-MM" or "YYYY-MM-DD"
    cpu_seconds :: non_neg_integer(),
    mem_seconds :: non_neg_integer(),
    gpu_seconds :: non_neg_integer(),
    node_seconds :: non_neg_integer(),
    job_count :: non_neg_integer(),
    job_time :: non_neg_integer()
}).

%% Association record for user-account linking
-record(assoc_record, {
    id :: pos_integer(),
    cluster :: binary(),
    account :: binary(),
    user :: binary(),
    partition = <<>> :: binary(),
    shares = 1 :: non_neg_integer(),
    max_jobs = 0 :: non_neg_integer(),
    max_submit = 0 :: non_neg_integer(),
    max_wall = 0 :: non_neg_integer(),
    grp_tres = #{} :: map(),
    max_tres_per_job = #{} :: map(),
    qos = [] :: [binary()],
    default_qos = <<>> :: binary(),
    usage = #{} :: map()  % Running usage counters
}).

-record(state, {
    job_records :: ets:tid(),
    step_records :: ets:tid(),
    usage_records :: ets:tid(),
    usage_by_user :: ets:tid(),
    usage_by_account :: ets:tid(),
    associations :: ets:tid(),
    archive :: ets:tid(),
    next_assoc_id :: pos_integer(),
    stats :: map()
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Record a job start event
-spec record_job_start(map()) -> ok.
record_job_start(JobInfo) ->
    gen_server:cast(?MODULE, {record_job_start, JobInfo}).

%% @doc Record a job end event
-spec record_job_end(map()) -> ok.
record_job_end(JobInfo) ->
    gen_server:cast(?MODULE, {record_job_end, JobInfo}).

%% @doc Record a job step
-spec record_job_step(map()) -> ok.
record_job_step(StepInfo) ->
    gen_server:cast(?MODULE, {record_job_step, StepInfo}).

%% @doc Record job submission event (called when job enters pending state)
%% JobData is the #job_data{} record from flurm_job state machine
-spec record_job_submit(map()) -> ok.
record_job_submit(JobData) ->
    gen_server:cast(?MODULE, {record_job_submit, JobData}).

%% @doc Record job start event (called when job enters running state)
%% JobId is the job identifier, AllocatedNodes is the list of nodes
-spec record_job_start(pos_integer(), [binary()]) -> ok.
record_job_start(JobId, AllocatedNodes) ->
    gen_server:cast(?MODULE, {record_job_start_event, JobId, AllocatedNodes}).

%% @doc Record job end event (called when job completes)
%% JobId is the job identifier, ExitCode is the job exit code, State is the final state
-spec record_job_end(pos_integer(), integer(), atom()) -> ok.
record_job_end(JobId, ExitCode, State) ->
    gen_server:cast(?MODULE, {record_job_end_event, JobId, ExitCode, State}).

%% @doc Record job cancelled event
%% JobId is the job identifier, Reason is the cancellation reason
-spec record_job_cancelled(pos_integer(), atom()) -> ok.
record_job_cancelled(JobId, Reason) ->
    gen_server:cast(?MODULE, {record_job_cancelled, JobId, Reason}).

%% @doc Get a job record by ID
-spec get_job_record(pos_integer()) -> {ok, map()} | {error, not_found}.
get_job_record(JobId) ->
    gen_server:call(?MODULE, {get_job_record, JobId}).

%% @doc List all job records
-spec list_job_records() -> [map()].
list_job_records() ->
    gen_server:call(?MODULE, list_job_records).

%% @doc List job records with filters
-spec list_job_records(map()) -> [map()].
list_job_records(Filters) ->
    gen_server:call(?MODULE, {list_job_records, Filters}).

%% @doc Get usage for a user
-spec get_user_usage(binary()) -> map().
get_user_usage(Username) ->
    gen_server:call(?MODULE, {get_user_usage, Username}).

%% @doc Get usage for an account
-spec get_account_usage(binary()) -> map().
get_account_usage(Account) ->
    gen_server:call(?MODULE, {get_account_usage, Account}).

%% @doc Get cluster-wide usage
-spec get_cluster_usage() -> map().
get_cluster_usage() ->
    gen_server:call(?MODULE, get_cluster_usage).

%% @doc Reset usage counters (for new billing period)
-spec reset_usage() -> ok.
reset_usage() ->
    gen_server:call(?MODULE, reset_usage).

%% @doc Archive records older than N days
-spec archive_old_records(pos_integer()) -> {ok, non_neg_integer()}.
archive_old_records(Days) ->
    gen_server:call(?MODULE, {archive_old_records, Days}).

%% @doc Purge records older than N days
-spec purge_old_records(pos_integer()) -> {ok, non_neg_integer()}.
purge_old_records(Days) ->
    gen_server:call(?MODULE, {purge_old_records, Days}).

%% @doc Get daemon statistics
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?MODULE, get_stats).

%% @doc Calculate detailed TRES usage for a user (includes GPU, memory)
-spec calculate_user_tres_usage(binary()) -> map().
calculate_user_tres_usage(Username) ->
    gen_server:call(?MODULE, {calculate_user_tres_usage, Username}).

%% @doc Calculate detailed TRES usage for an account (includes GPU, memory)
-spec calculate_account_tres_usage(binary()) -> map().
calculate_account_tres_usage(Account) ->
    gen_server:call(?MODULE, {calculate_account_tres_usage, Account}).

%% @doc Calculate detailed cluster-wide TRES usage (includes GPU, memory)
-spec calculate_cluster_tres_usage() -> map().
calculate_cluster_tres_usage() ->
    gen_server:call(?MODULE, calculate_cluster_tres_usage).

%%--------------------------------------------------------------------
%% sacct-style query functions
%%--------------------------------------------------------------------

%% @doc Get all jobs for a specific user
-spec get_jobs_by_user(binary()) -> [map()].
get_jobs_by_user(Username) ->
    gen_server:call(?MODULE, {get_jobs_by_user, Username}).

%% @doc Get all jobs for a specific account
-spec get_jobs_by_account(binary()) -> [map()].
get_jobs_by_account(Account) ->
    gen_server:call(?MODULE, {get_jobs_by_account, Account}).

%% @doc Get jobs within a time range (start_time, end_time as unix timestamps)
-spec get_jobs_in_range(non_neg_integer(), non_neg_integer()) -> [map()].
get_jobs_in_range(StartTime, EndTime) ->
    gen_server:call(?MODULE, {get_jobs_in_range, StartTime, EndTime}).

%% @doc Format job records for sacct-style output
-spec format_sacct_output([map()]) -> iolist().
format_sacct_output(Jobs) ->
    Header = io_lib:format("~-12s ~-10s ~-10s ~-12s ~-12s ~-10s ~-8s~n",
                           ["JobID", "User", "Account", "Partition", "State", "Elapsed", "ExitCode"]),
    Separator = lists:duplicate(80, $-) ++ "\n",
    Rows = [format_sacct_row(Job) || Job <- Jobs],
    [Header, Separator | Rows].

%%--------------------------------------------------------------------
%% Association management functions
%%--------------------------------------------------------------------

%% @doc Add a user-account association
-spec add_association(binary(), binary(), binary()) -> {ok, pos_integer()} | {error, term()}.
add_association(Cluster, Account, User) ->
    add_association(Cluster, Account, User, #{}).

%% @doc Add a user-account association with options
-spec add_association(binary(), binary(), binary(), map()) -> {ok, pos_integer()} | {error, term()}.
add_association(Cluster, Account, User, Opts) ->
    gen_server:call(?MODULE, {add_association, Cluster, Account, User, Opts}).

%% @doc Get associations by cluster
-spec get_associations(binary()) -> [map()].
get_associations(Cluster) ->
    gen_server:call(?MODULE, {get_associations, Cluster}).

%% @doc Get a specific association by ID
-spec get_association(pos_integer()) -> {ok, map()} | {error, not_found}.
get_association(AssocId) ->
    gen_server:call(?MODULE, {get_association, AssocId}).

%% @doc Remove an association by ID
-spec remove_association(pos_integer()) -> ok | {error, not_found}.
remove_association(AssocId) ->
    gen_server:call(?MODULE, {remove_association, AssocId}).

%% @doc Update an association's settings
-spec update_association(pos_integer(), map()) -> ok | {error, not_found}.
update_association(AssocId, Updates) ->
    gen_server:call(?MODULE, {update_association, AssocId, Updates}).

%% @doc Get all associations for a user
-spec get_user_associations(binary()) -> [map()].
get_user_associations(Username) ->
    gen_server:call(?MODULE, {get_user_associations, Username}).

%% @doc Get all associations for an account
-spec get_account_associations(binary()) -> [map()].
get_account_associations(Account) ->
    gen_server:call(?MODULE, {get_account_associations, Account}).

%%--------------------------------------------------------------------
%% Archive/purge operations
%%--------------------------------------------------------------------

%% @doc Archive old jobs (move to archive table)
-spec archive_old_jobs(pos_integer()) -> {ok, non_neg_integer()}.
archive_old_jobs(Days) ->
    gen_server:call(?MODULE, {archive_old_jobs, Days}).

%% @doc Purge old jobs (delete from archive)
-spec purge_old_jobs(pos_integer()) -> {ok, non_neg_integer()}.
purge_old_jobs(Days) ->
    gen_server:call(?MODULE, {purge_old_jobs, Days}).

%% @doc Get all archived jobs
-spec get_archived_jobs() -> [map()].
get_archived_jobs() ->
    gen_server:call(?MODULE, get_archived_jobs).

%% @doc Get archived jobs with filters
-spec get_archived_jobs(map()) -> [map()].
get_archived_jobs(Filters) ->
    gen_server:call(?MODULE, {get_archived_jobs, Filters}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("FLURM DBD Server starting"),

    %% Create ETS tables for accounting data
    JobRecords = ets:new(job_records, [set, protected, {keypos, #job_record.job_id}]),
    StepRecords = ets:new(step_records, [bag, protected]),
    UsageRecords = ets:new(usage_records, [set, protected, {keypos, #usage_record.key}]),

    %% Additional usage tracking tables
    UsageByUser = ets:new(usage_by_user, [set, protected]),
    UsageByAccount = ets:new(usage_by_account, [set, protected]),

    %% Association management
    Associations = ets:new(associations, [set, protected, {keypos, #assoc_record.id}]),

    %% Archive table for old jobs
    Archive = ets:new(archive, [set, protected, {keypos, #job_record.job_id}]),

    Stats = #{
        jobs_submitted => 0,
        jobs_started => 0,
        jobs_completed => 0,
        jobs_failed => 0,
        jobs_cancelled => 0,
        steps_recorded => 0,
        associations_count => 0,
        archived_jobs => 0,
        start_time => erlang:system_time(second)
    },

    %% Start the listener after init
    self() ! start_listener,

    {ok, #state{
        job_records = JobRecords,
        step_records = StepRecords,
        usage_records = UsageRecords,
        usage_by_user = UsageByUser,
        usage_by_account = UsageByAccount,
        associations = Associations,
        archive = Archive,
        next_assoc_id = 1,
        stats = Stats
    }}.

handle_call({get_job_record, JobId}, _From, State) ->
    case ets:lookup(State#state.job_records, JobId) of
        [Record] ->
            {reply, {ok, job_record_to_map(Record)}, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call(list_job_records, _From, State) ->
    Records = [job_record_to_map(R) || R <- ets:tab2list(State#state.job_records)],
    {reply, Records, State};

handle_call({list_job_records, Filters}, _From, State) ->
    AllRecords = ets:tab2list(State#state.job_records),
    Filtered = filter_job_records(AllRecords, Filters),
    Records = [job_record_to_map(R) || R <- Filtered],
    {reply, Records, State};

handle_call({get_user_usage, Username}, _From, State) ->
    Usage = calculate_user_usage(Username, State#state.job_records),
    {reply, Usage, State};

handle_call({get_account_usage, Account}, _From, State) ->
    Usage = calculate_account_usage(Account, State#state.job_records),
    {reply, Usage, State};

handle_call(get_cluster_usage, _From, State) ->
    Usage = calculate_cluster_usage(State#state.job_records),
    {reply, Usage, State};

%% TRES usage with full detail (including GPU, memory)
handle_call({calculate_user_tres_usage, Username}, _From, State) ->
    Usage = calculate_user_tres_usage(Username, State#state.job_records),
    {reply, Usage, State};

handle_call({calculate_account_tres_usage, Account}, _From, State) ->
    Usage = calculate_account_tres_usage(Account, State#state.job_records),
    {reply, Usage, State};

handle_call(calculate_cluster_tres_usage, _From, State) ->
    Usage = calculate_cluster_tres_usage(State#state.job_records),
    {reply, Usage, State};

handle_call(reset_usage, _From, State) ->
    ets:delete_all_objects(State#state.usage_records),
    {reply, ok, State};

handle_call({archive_old_records, Days}, _From, State) ->
    Cutoff = erlang:system_time(second) - (Days * 86400),
    %% In a real implementation, this would write to a file or archive table
    %% For now, just count matching records
    Count = ets:foldl(fun(#job_record{end_time = EndTime}, Acc) when EndTime > 0, EndTime < Cutoff ->
                              Acc + 1;
                         (_, Acc) -> Acc
                      end, 0, State#state.job_records),
    {reply, {ok, Count}, State};

handle_call({purge_old_records, Days}, _From, State) ->
    Cutoff = erlang:system_time(second) - (Days * 86400),
    %% Delete old records
    ToDelete = ets:foldl(fun(#job_record{job_id = Id, end_time = EndTime}, Acc)
                                when EndTime > 0, EndTime < Cutoff ->
                                 [Id | Acc];
                            (_, Acc) -> Acc
                         end, [], State#state.job_records),
    lists:foreach(fun(Id) -> ets:delete(State#state.job_records, Id) end, ToDelete),
    {reply, {ok, length(ToDelete)}, State};

handle_call(get_stats, _From, State) ->
    JobCount = ets:info(State#state.job_records, size),
    StepCount = ets:info(State#state.step_records, size),
    AssocCount = ets:info(State#state.associations, size),
    ArchivedCount = ets:info(State#state.archive, size),
    Uptime = erlang:system_time(second) - maps:get(start_time, State#state.stats),
    Stats = maps:merge(State#state.stats, #{
        total_job_records => JobCount,
        total_step_records => StepCount,
        total_associations => AssocCount,
        total_archived => ArchivedCount,
        uptime_seconds => Uptime
    }),
    {reply, Stats, State};

%% sacct-style queries
handle_call({get_jobs_by_user, Username}, _From, State) ->
    Jobs = ets:foldl(fun(#job_record{user_name = U} = R, Acc) when U =:= Username ->
                             [job_record_to_map(R) | Acc];
                        (_, Acc) -> Acc
                     end, [], State#state.job_records),
    {reply, lists:reverse(Jobs), State};

handle_call({get_jobs_by_account, Account}, _From, State) ->
    Jobs = ets:foldl(fun(#job_record{account = A} = R, Acc) when A =:= Account ->
                             [job_record_to_map(R) | Acc];
                        (_, Acc) -> Acc
                     end, [], State#state.job_records),
    {reply, lists:reverse(Jobs), State};

handle_call({get_jobs_in_range, StartTime, EndTime}, _From, State) ->
    Jobs = ets:foldl(fun(#job_record{start_time = ST, end_time = ET} = R, Acc)
                           when ST >= StartTime, (ET =< EndTime orelse ET =:= 0) ->
                             [job_record_to_map(R) | Acc];
                        (_, Acc) -> Acc
                     end, [], State#state.job_records),
    {reply, lists:reverse(Jobs), State};

%% Association management
handle_call({add_association, Cluster, Account, User, Opts}, _From, State) ->
    AssocId = State#state.next_assoc_id,
    Assoc = #assoc_record{
        id = AssocId,
        cluster = Cluster,
        account = Account,
        user = User,
        partition = maps:get(partition, Opts, <<>>),
        shares = maps:get(shares, Opts, 1),
        max_jobs = maps:get(max_jobs, Opts, 0),
        max_submit = maps:get(max_submit, Opts, 0),
        max_wall = maps:get(max_wall, Opts, 0),
        grp_tres = maps:get(grp_tres, Opts, #{}),
        max_tres_per_job = maps:get(max_tres_per_job, Opts, #{}),
        qos = maps:get(qos, Opts, []),
        default_qos = maps:get(default_qos, Opts, <<>>),
        usage = #{}
    },
    ets:insert(State#state.associations, Assoc),
    Stats = maps:update_with(associations_count, fun(V) -> V + 1 end, 1, State#state.stats),
    lager:debug("Added association ~p: ~s/~s/~s", [AssocId, Cluster, Account, User]),
    {reply, {ok, AssocId}, State#state{next_assoc_id = AssocId + 1, stats = Stats}};

handle_call({get_associations, Cluster}, _From, State) ->
    Assocs = ets:foldl(fun(#assoc_record{cluster = C} = R, Acc) when C =:= Cluster ->
                               [assoc_record_to_map(R) | Acc];
                          (_, Acc) -> Acc
                       end, [], State#state.associations),
    {reply, lists:reverse(Assocs), State};

handle_call({get_association, AssocId}, _From, State) ->
    case ets:lookup(State#state.associations, AssocId) of
        [Record] ->
            {reply, {ok, assoc_record_to_map(Record)}, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({remove_association, AssocId}, _From, State) ->
    case ets:lookup(State#state.associations, AssocId) of
        [_] ->
            ets:delete(State#state.associations, AssocId),
            Stats = maps:update_with(associations_count, fun(V) -> max(0, V - 1) end, 0, State#state.stats),
            {reply, ok, State#state{stats = Stats}};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({update_association, AssocId, Updates}, _From, State) ->
    case ets:lookup(State#state.associations, AssocId) of
        [Existing] ->
            Updated = update_assoc_record(Existing, Updates),
            ets:insert(State#state.associations, Updated),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_user_associations, Username}, _From, State) ->
    Assocs = ets:foldl(fun(#assoc_record{user = U} = R, Acc) when U =:= Username ->
                               [assoc_record_to_map(R) | Acc];
                          (_, Acc) -> Acc
                       end, [], State#state.associations),
    {reply, lists:reverse(Assocs), State};

handle_call({get_account_associations, Account}, _From, State) ->
    Assocs = ets:foldl(fun(#assoc_record{account = A} = R, Acc) when A =:= Account ->
                               [assoc_record_to_map(R) | Acc];
                          (_, Acc) -> Acc
                       end, [], State#state.associations),
    {reply, lists:reverse(Assocs), State};

%% Archive operations
handle_call({archive_old_jobs, Days}, _From, State) ->
    Cutoff = erlang:system_time(second) - (Days * 86400),
    %% Move old completed jobs to archive table
    ToArchive = ets:foldl(fun(#job_record{job_id = Id, end_time = EndTime, state = S} = R, Acc)
                                when EndTime > 0, EndTime < Cutoff, S =/= running, S =/= pending ->
                                 [{Id, R} | Acc];
                             (_, Acc) -> Acc
                          end, [], State#state.job_records),
    %% Insert into archive and delete from main table
    lists:foreach(fun({Id, Record}) ->
                          ets:insert(State#state.archive, Record),
                          ets:delete(State#state.job_records, Id)
                  end, ToArchive),
    Count = length(ToArchive),
    Stats = maps:update_with(archived_jobs, fun(V) -> V + Count end, Count, State#state.stats),
    lager:info("Archived ~p jobs older than ~p days", [Count, Days]),
    {reply, {ok, Count}, State#state{stats = Stats}};

handle_call({purge_old_jobs, Days}, _From, State) ->
    Cutoff = erlang:system_time(second) - (Days * 86400),
    %% Delete old jobs from archive table
    ToDelete = ets:foldl(fun(#job_record{job_id = Id, end_time = EndTime}, Acc)
                                when EndTime > 0, EndTime < Cutoff ->
                                 [Id | Acc];
                            (_, Acc) -> Acc
                         end, [], State#state.archive),
    lists:foreach(fun(Id) -> ets:delete(State#state.archive, Id) end, ToDelete),
    lager:info("Purged ~p archived jobs older than ~p days", [length(ToDelete), Days]),
    {reply, {ok, length(ToDelete)}, State};

handle_call(get_archived_jobs, _From, State) ->
    Records = [job_record_to_map(R) || R <- ets:tab2list(State#state.archive)],
    {reply, Records, State};

handle_call({get_archived_jobs, Filters}, _From, State) ->
    AllRecords = ets:tab2list(State#state.archive),
    Filtered = filter_job_records(AllRecords, Filters),
    Records = [job_record_to_map(R) || R <- Filtered],
    {reply, Records, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({record_job_start, JobInfo}, State) ->
    Record = job_info_to_record(JobInfo),
    ets:insert(State#state.job_records, Record),
    Stats = maps:update_with(jobs_started, fun(V) -> V + 1 end, State#state.stats),
    lager:debug("Recorded job start: ~p", [maps:get(job_id, JobInfo)]),
    {noreply, State#state{stats = Stats}};

handle_cast({record_job_end, JobInfo}, State) ->
    JobId = maps:get(job_id, JobInfo),
    case ets:lookup(State#state.job_records, JobId) of
        [Existing] ->
            %% Update existing record with end info
            Updated = update_job_record(Existing, JobInfo),
            ets:insert(State#state.job_records, Updated),
            %% Update usage
            update_usage(Updated, State#state.usage_records),
            %% Update stats
            StatsKey = case Updated#job_record.exit_code of
                0 -> jobs_completed;
                _ -> jobs_failed
            end,
            Stats = maps:update_with(StatsKey, fun(V) -> V + 1 end, State#state.stats),
            lager:debug("Recorded job end: ~p, exit_code=~p",
                       [JobId, Updated#job_record.exit_code]),
            {noreply, State#state{stats = Stats}};
        [] ->
            %% No existing record, create new one
            Record = job_info_to_record(JobInfo),
            ets:insert(State#state.job_records, Record),
            {noreply, State}
    end;

handle_cast({record_job_step, StepInfo}, State) ->
    Record = step_info_to_record(StepInfo),
    ets:insert(State#state.step_records, Record),
    Stats = maps:update_with(steps_recorded, fun(V) -> V + 1 end, State#state.stats),
    {noreply, State#state{stats = Stats}};

%% Handle job lifecycle events from flurm_job state machine
handle_cast({record_job_submit, JobData}, State) ->
    %% Create initial job record on submission
    Now = erlang:system_time(second),
    SubmitTime = case maps:get(submit_time, JobData, undefined) of
        undefined -> Now;
        {MegaSecs, Secs, _MicroSecs} -> MegaSecs * 1000000 + Secs;
        T when is_integer(T) -> T
    end,
    Record = #job_record{
        job_id = maps:get(job_id, JobData),
        job_name = <<>>,
        user_name = <<>>,
        user_id = maps:get(user_id, JobData, 0),
        group_id = maps:get(group_id, JobData, 0),
        account = <<>>,
        partition = maps:get(partition, JobData, <<>>),
        cluster = <<"flurm">>,
        qos = <<"normal">>,
        state = pending,
        exit_code = 0,
        num_nodes = maps:get(num_nodes, JobData, 1),
        num_cpus = maps:get(num_cpus, JobData, 1),
        num_tasks = 1,
        req_mem = 0,
        submit_time = SubmitTime,
        eligible_time = SubmitTime,
        start_time = 0,
        end_time = 0,
        elapsed = 0,
        tres_alloc = #{},
        tres_req = #{},
        work_dir = <<>>,
        std_out = <<>>,
        std_err = <<>>
    },
    ets:insert(State#state.job_records, Record),
    Stats = maps:update_with(jobs_submitted, fun(V) -> V + 1 end, 1, State#state.stats),
    lager:debug("Recorded job submit: ~p", [maps:get(job_id, JobData)]),
    {noreply, State#state{stats = Stats}};

handle_cast({record_job_start_event, JobId, AllocatedNodes}, State) ->
    %% Update existing record with start time and allocated nodes
    Now = erlang:system_time(second),
    case ets:lookup(State#state.job_records, JobId) of
        [Existing] ->
            Updated = Existing#job_record{
                state = running,
                start_time = Now,
                num_nodes = length(AllocatedNodes)
            },
            ets:insert(State#state.job_records, Updated),
            Stats = maps:update_with(jobs_started, fun(V) -> V + 1 end, State#state.stats),
            lager:debug("Recorded job start event: ~p on ~p nodes", [JobId, length(AllocatedNodes)]),
            {noreply, State#state{stats = Stats}};
        [] ->
            %% No existing record - create one
            Record = #job_record{
                job_id = JobId,
                job_name = <<>>,
                user_name = <<>>,
                user_id = 0,
                group_id = 0,
                account = <<>>,
                partition = <<>>,
                cluster = <<"flurm">>,
                qos = <<"normal">>,
                state = running,
                exit_code = 0,
                num_nodes = length(AllocatedNodes),
                num_cpus = 1,
                num_tasks = 1,
                req_mem = 0,
                submit_time = Now,
                eligible_time = Now,
                start_time = Now,
                end_time = 0,
                elapsed = 0,
                tres_alloc = #{},
                tres_req = #{},
                work_dir = <<>>,
                std_out = <<>>,
                std_err = <<>>
            },
            ets:insert(State#state.job_records, Record),
            Stats = maps:update_with(jobs_started, fun(V) -> V + 1 end, State#state.stats),
            lager:debug("Recorded job start event (new record): ~p", [JobId]),
            {noreply, State#state{stats = Stats}}
    end;

handle_cast({record_job_end_event, JobId, ExitCode, FinalState}, State) ->
    %% Update existing record with end time and exit code
    Now = erlang:system_time(second),
    case ets:lookup(State#state.job_records, JobId) of
        [Existing] ->
            StartTime = case Existing#job_record.start_time of
                0 -> Now;
                T -> T
            end,
            Elapsed = Now - StartTime,
            Updated = Existing#job_record{
                state = FinalState,
                exit_code = ExitCode,
                end_time = Now,
                elapsed = Elapsed
            },
            ets:insert(State#state.job_records, Updated),
            update_usage(Updated, State#state.usage_records),
            StatsKey = case ExitCode of
                0 -> jobs_completed;
                _ -> jobs_failed
            end,
            Stats = maps:update_with(StatsKey, fun(V) -> V + 1 end, State#state.stats),
            lager:debug("Recorded job end event: ~p, state=~p, exit_code=~p",
                       [JobId, FinalState, ExitCode]),
            {noreply, State#state{stats = Stats}};
        [] ->
            lager:warning("Job end event for unknown job: ~p", [JobId]),
            {noreply, State}
    end;

handle_cast({record_job_cancelled, JobId, Reason}, State) ->
    %% Update existing record as cancelled
    Now = erlang:system_time(second),
    case ets:lookup(State#state.job_records, JobId) of
        [Existing] ->
            StartTime = case Existing#job_record.start_time of
                0 -> Now;
                T -> T
            end,
            Elapsed = Now - StartTime,
            Updated = Existing#job_record{
                state = cancelled,
                exit_code = -1,
                end_time = Now,
                elapsed = Elapsed
            },
            ets:insert(State#state.job_records, Updated),
            Stats = maps:update_with(jobs_cancelled, fun(V) -> V + 1 end, 1, State#state.stats),
            lager:debug("Recorded job cancelled: ~p, reason=~p", [JobId, Reason]),
            {noreply, State#state{stats = Stats}};
        [] ->
            lager:warning("Job cancelled event for unknown job: ~p", [JobId]),
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(start_listener, State) ->
    %% Start the TCP listener
    case flurm_dbd_sup:start_listener() of
        {ok, _Pid} ->
            lager:info("DBD listener started successfully");
        {error, Reason} ->
            lager:error("Failed to start DBD listener: ~p", [Reason])
    end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

job_info_to_record(JobInfo) ->
    Now = erlang:system_time(second),
    #job_record{
        job_id = maps:get(job_id, JobInfo),
        job_name = maps:get(name, JobInfo, <<>>),
        user_name = maps:get(user_name, JobInfo, <<>>),
        user_id = maps:get(user_id, JobInfo, 0),
        group_id = maps:get(group_id, JobInfo, 0),
        account = maps:get(account, JobInfo, <<>>),
        partition = maps:get(partition, JobInfo, <<>>),
        cluster = maps:get(cluster, JobInfo, <<"flurm">>),
        qos = maps:get(qos, JobInfo, <<"normal">>),
        state = maps:get(state, JobInfo, pending),
        exit_code = maps:get(exit_code, JobInfo, 0),
        num_nodes = maps:get(num_nodes, JobInfo, 1),
        num_cpus = maps:get(num_cpus, JobInfo, 1),
        num_tasks = maps:get(num_tasks, JobInfo, 1),
        req_mem = maps:get(req_mem, JobInfo, 0),
        submit_time = maps:get(submit_time, JobInfo, Now),
        eligible_time = maps:get(eligible_time, JobInfo, Now),
        start_time = maps:get(start_time, JobInfo, 0),
        end_time = maps:get(end_time, JobInfo, 0),
        elapsed = maps:get(elapsed, JobInfo, 0),
        tres_alloc = maps:get(tres_alloc, JobInfo, #{}),
        tres_req = maps:get(tres_req, JobInfo, #{}),
        work_dir = maps:get(work_dir, JobInfo, <<>>),
        std_out = maps:get(std_out, JobInfo, <<>>),
        std_err = maps:get(std_err, JobInfo, <<>>)
    }.

update_job_record(Existing, JobInfo) ->
    Now = erlang:system_time(second),
    EndTime = maps:get(end_time, JobInfo, Now),
    StartTime = case Existing#job_record.start_time of
        0 -> maps:get(start_time, JobInfo, Now);
        T -> T
    end,
    Elapsed = case EndTime > 0 andalso StartTime > 0 of
        true -> EndTime - StartTime;
        false -> 0
    end,
    Existing#job_record{
        state = maps:get(state, JobInfo, Existing#job_record.state),
        exit_code = maps:get(exit_code, JobInfo, Existing#job_record.exit_code),
        start_time = StartTime,
        end_time = EndTime,
        elapsed = Elapsed,
        tres_alloc = maps:get(tres_alloc, JobInfo, Existing#job_record.tres_alloc)
    }.

step_info_to_record(StepInfo) ->
    #step_record{
        job_id = maps:get(job_id, StepInfo),
        step_id = maps:get(step_id, StepInfo),
        step_name = maps:get(name, StepInfo, <<>>),
        state = maps:get(state, StepInfo, pending),
        exit_code = maps:get(exit_code, StepInfo, 0),
        num_tasks = maps:get(num_tasks, StepInfo, 1),
        num_nodes = maps:get(num_nodes, StepInfo, 1),
        start_time = maps:get(start_time, StepInfo, 0),
        end_time = maps:get(end_time, StepInfo, 0),
        elapsed = maps:get(elapsed, StepInfo, 0),
        tres_alloc = maps:get(tres_alloc, StepInfo, #{})
    }.

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
        eligible_time => R#job_record.eligible_time,
        start_time => R#job_record.start_time,
        end_time => R#job_record.end_time,
        elapsed => R#job_record.elapsed,
        tres_alloc => R#job_record.tres_alloc,
        tres_req => R#job_record.tres_req,
        work_dir => R#job_record.work_dir,
        std_out => R#job_record.std_out,
        std_err => R#job_record.std_err
    }.

filter_job_records(Records, Filters) ->
    lists:filter(fun(R) -> matches_job_filters(R, Filters) end, Records).

matches_job_filters(Record, Filters) ->
    maps:fold(fun
        (user, V, Acc) -> Acc andalso Record#job_record.user_name =:= V;
        (account, V, Acc) -> Acc andalso Record#job_record.account =:= V;
        (partition, V, Acc) -> Acc andalso Record#job_record.partition =:= V;
        (state, V, Acc) -> Acc andalso Record#job_record.state =:= V;
        (start_time_after, V, Acc) -> Acc andalso Record#job_record.start_time >= V;
        (start_time_before, V, Acc) -> Acc andalso Record#job_record.start_time =< V;
        (end_time_after, V, Acc) -> Acc andalso Record#job_record.end_time >= V;
        (end_time_before, V, Acc) -> Acc andalso Record#job_record.end_time =< V;
        (_, _, Acc) -> Acc
    end, true, Filters).

update_usage(#job_record{} = Record, UsageTable) ->
    %% Calculate TRES usage
    TresUsage = calculate_tres_usage(Record),
    CpuSeconds = maps:get(cpu_seconds, TresUsage, 0),
    MemSeconds = maps:get(mem_seconds, TresUsage, 0),
    GpuSeconds = maps:get(gpu_seconds, TresUsage, 0),
    NodeSeconds = maps:get(node_seconds, TresUsage, 0),

    %% Update user usage
    UserKey = {Record#job_record.user_name, current_period()},
    case ets:lookup(UsageTable, UserKey) of
        [Existing] ->
            Updated = Existing#usage_record{
                cpu_seconds = Existing#usage_record.cpu_seconds + CpuSeconds,
                mem_seconds = Existing#usage_record.mem_seconds + MemSeconds,
                gpu_seconds = Existing#usage_record.gpu_seconds + GpuSeconds,
                node_seconds = Existing#usage_record.node_seconds + NodeSeconds,
                job_count = Existing#usage_record.job_count + 1,
                job_time = Existing#usage_record.job_time + Record#job_record.elapsed
            },
            ets:insert(UsageTable, Updated);
        [] ->
            New = #usage_record{
                key = UserKey,
                cluster = Record#job_record.cluster,
                account = Record#job_record.account,
                user = Record#job_record.user_name,
                period = current_period(),
                cpu_seconds = CpuSeconds,
                mem_seconds = MemSeconds,
                gpu_seconds = GpuSeconds,
                node_seconds = NodeSeconds,
                job_count = 1,
                job_time = Record#job_record.elapsed
            },
            ets:insert(UsageTable, New)
    end,
    ok.

current_period() ->
    {{Year, Month, _Day}, _} = calendar:local_time(),
    list_to_binary(io_lib:format("~4..0B-~2..0B", [Year, Month])).

calculate_user_usage(Username, JobRecords) ->
    ets:foldl(fun(#job_record{user_name = U} = R, Acc) when U =:= Username ->
                      #{
                          cpu_seconds => maps:get(cpu_seconds, Acc, 0) +
                              (R#job_record.elapsed * R#job_record.num_cpus),
                          node_seconds => maps:get(node_seconds, Acc, 0) +
                              (R#job_record.elapsed * R#job_record.num_nodes),
                          job_count => maps:get(job_count, Acc, 0) + 1,
                          elapsed_total => maps:get(elapsed_total, Acc, 0) + R#job_record.elapsed
                      };
                 (_, Acc) -> Acc
              end, #{cpu_seconds => 0, node_seconds => 0, job_count => 0, elapsed_total => 0},
              JobRecords).

calculate_account_usage(Account, JobRecords) ->
    ets:foldl(fun(#job_record{account = A} = R, Acc) when A =:= Account ->
                      #{
                          cpu_seconds => maps:get(cpu_seconds, Acc, 0) +
                              (R#job_record.elapsed * R#job_record.num_cpus),
                          node_seconds => maps:get(node_seconds, Acc, 0) +
                              (R#job_record.elapsed * R#job_record.num_nodes),
                          job_count => maps:get(job_count, Acc, 0) + 1,
                          elapsed_total => maps:get(elapsed_total, Acc, 0) + R#job_record.elapsed
                      };
                 (_, Acc) -> Acc
              end, #{cpu_seconds => 0, node_seconds => 0, job_count => 0, elapsed_total => 0},
              JobRecords).

calculate_cluster_usage(JobRecords) ->
    ets:foldl(fun(#job_record{} = R, Acc) ->
                      #{
                          cpu_seconds => maps:get(cpu_seconds, Acc, 0) +
                              (R#job_record.elapsed * R#job_record.num_cpus),
                          node_seconds => maps:get(node_seconds, Acc, 0) +
                              (R#job_record.elapsed * R#job_record.num_nodes),
                          job_count => maps:get(job_count, Acc, 0) + 1,
                          elapsed_total => maps:get(elapsed_total, Acc, 0) + R#job_record.elapsed
                      }
              end, #{cpu_seconds => 0, node_seconds => 0, job_count => 0, elapsed_total => 0},
              JobRecords).

%% Association record to map conversion
assoc_record_to_map(#assoc_record{} = R) ->
    #{
        id => R#assoc_record.id,
        cluster => R#assoc_record.cluster,
        account => R#assoc_record.account,
        user => R#assoc_record.user,
        partition => R#assoc_record.partition,
        shares => R#assoc_record.shares,
        max_jobs => R#assoc_record.max_jobs,
        max_submit => R#assoc_record.max_submit,
        max_wall => R#assoc_record.max_wall,
        grp_tres => R#assoc_record.grp_tres,
        max_tres_per_job => R#assoc_record.max_tres_per_job,
        qos => R#assoc_record.qos,
        default_qos => R#assoc_record.default_qos,
        usage => R#assoc_record.usage
    }.

%% Update association record with new values
update_assoc_record(Existing, Updates) ->
    Existing#assoc_record{
        partition = maps:get(partition, Updates, Existing#assoc_record.partition),
        shares = maps:get(shares, Updates, Existing#assoc_record.shares),
        max_jobs = maps:get(max_jobs, Updates, Existing#assoc_record.max_jobs),
        max_submit = maps:get(max_submit, Updates, Existing#assoc_record.max_submit),
        max_wall = maps:get(max_wall, Updates, Existing#assoc_record.max_wall),
        grp_tres = maps:get(grp_tres, Updates, Existing#assoc_record.grp_tres),
        max_tres_per_job = maps:get(max_tres_per_job, Updates, Existing#assoc_record.max_tres_per_job),
        qos = maps:get(qos, Updates, Existing#assoc_record.qos),
        default_qos = maps:get(default_qos, Updates, Existing#assoc_record.default_qos),
        usage = maps:get(usage, Updates, Existing#assoc_record.usage)
    }.

%% Format a single job record as a sacct output row
format_sacct_row(Job) ->
    JobId = integer_to_list(maps:get(job_id, Job, 0)),
    User = binary_to_list(maps:get(user_name, Job, <<>>)),
    Account = binary_to_list(maps:get(account, Job, <<>>)),
    Partition = binary_to_list(maps:get(partition, Job, <<>>)),
    State = atom_to_list(maps:get(state, Job, unknown)),
    Elapsed = format_elapsed(maps:get(elapsed, Job, 0)),
    ExitCode = format_exit_code(maps:get(exit_code, Job, 0)),
    io_lib:format("~-12s ~-10s ~-10s ~-12s ~-12s ~-10s ~-8s~n",
                  [JobId, User, Account, Partition, State, Elapsed, ExitCode]).

%% Format elapsed time as HH:MM:SS
format_elapsed(Seconds) when is_integer(Seconds), Seconds >= 0 ->
    Hours = Seconds div 3600,
    Minutes = (Seconds rem 3600) div 60,
    Secs = Seconds rem 60,
    io_lib:format("~2..0B:~2..0B:~2..0B", [Hours, Minutes, Secs]);
format_elapsed(_) ->
    "00:00:00".

%% Format exit code as ExitCode:Signal
format_exit_code(Code) when is_integer(Code) ->
    io_lib:format("~B:0", [Code]);
format_exit_code(_) ->
    "0:0".

%% Calculate TRES (Trackable Resources) usage from a job record
calculate_tres_usage(#job_record{} = R) ->
    Elapsed = R#job_record.elapsed,
    TresAlloc = R#job_record.tres_alloc,
    #{
        cpu_seconds => Elapsed * R#job_record.num_cpus,
        mem_seconds => Elapsed * R#job_record.req_mem,
        node_seconds => Elapsed * R#job_record.num_nodes,
        gpu_seconds => Elapsed * maps:get(gpu, TresAlloc, 0)
    }.

%% Aggregate TRES usage by user
calculate_user_tres_usage(Username, JobRecords) ->
    BaseUsage = #{
        cpu_seconds => 0,
        mem_seconds => 0,
        gpu_seconds => 0,
        node_seconds => 0,
        job_count => 0,
        elapsed_total => 0
    },
    ets:foldl(fun(#job_record{user_name = U} = R, Acc) when U =:= Username ->
                      TresUsage = calculate_tres_usage(R),
                      #{
                          cpu_seconds => maps:get(cpu_seconds, Acc, 0) + maps:get(cpu_seconds, TresUsage, 0),
                          mem_seconds => maps:get(mem_seconds, Acc, 0) + maps:get(mem_seconds, TresUsage, 0),
                          gpu_seconds => maps:get(gpu_seconds, Acc, 0) + maps:get(gpu_seconds, TresUsage, 0),
                          node_seconds => maps:get(node_seconds, Acc, 0) + maps:get(node_seconds, TresUsage, 0),
                          job_count => maps:get(job_count, Acc, 0) + 1,
                          elapsed_total => maps:get(elapsed_total, Acc, 0) + R#job_record.elapsed
                      };
                 (_, Acc) -> Acc
              end, BaseUsage, JobRecords).

%% Aggregate TRES usage by account
calculate_account_tres_usage(Account, JobRecords) ->
    BaseUsage = #{
        cpu_seconds => 0,
        mem_seconds => 0,
        gpu_seconds => 0,
        node_seconds => 0,
        job_count => 0,
        elapsed_total => 0
    },
    ets:foldl(fun(#job_record{account = A} = R, Acc) when A =:= Account ->
                      TresUsage = calculate_tres_usage(R),
                      #{
                          cpu_seconds => maps:get(cpu_seconds, Acc, 0) + maps:get(cpu_seconds, TresUsage, 0),
                          mem_seconds => maps:get(mem_seconds, Acc, 0) + maps:get(mem_seconds, TresUsage, 0),
                          gpu_seconds => maps:get(gpu_seconds, Acc, 0) + maps:get(gpu_seconds, TresUsage, 0),
                          node_seconds => maps:get(node_seconds, Acc, 0) + maps:get(node_seconds, TresUsage, 0),
                          job_count => maps:get(job_count, Acc, 0) + 1,
                          elapsed_total => maps:get(elapsed_total, Acc, 0) + R#job_record.elapsed
                      };
                 (_, Acc) -> Acc
              end, BaseUsage, JobRecords).

%% Aggregate TRES usage cluster-wide
calculate_cluster_tres_usage(JobRecords) ->
    BaseUsage = #{
        cpu_seconds => 0,
        mem_seconds => 0,
        gpu_seconds => 0,
        node_seconds => 0,
        job_count => 0,
        elapsed_total => 0
    },
    ets:foldl(fun(#job_record{} = R, Acc) ->
                      TresUsage = calculate_tres_usage(R),
                      #{
                          cpu_seconds => maps:get(cpu_seconds, Acc, 0) + maps:get(cpu_seconds, TresUsage, 0),
                          mem_seconds => maps:get(mem_seconds, Acc, 0) + maps:get(mem_seconds, TresUsage, 0),
                          gpu_seconds => maps:get(gpu_seconds, Acc, 0) + maps:get(gpu_seconds, TresUsage, 0),
                          node_seconds => maps:get(node_seconds, Acc, 0) + maps:get(node_seconds, TresUsage, 0),
                          job_count => maps:get(job_count, Acc, 0) + 1,
                          elapsed_total => maps:get(elapsed_total, Acc, 0) + R#job_record.elapsed
                      }
              end, BaseUsage, JobRecords).
