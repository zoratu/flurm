%%%-------------------------------------------------------------------
%%% @doc FLURM PMI Manager
%%%
%%% Central coordinator for PMI operations. Manages:
%%% - Job step PMI state (ranks, KVS)
%%% - Barrier synchronization across all ranks
%%% - Keyval store aggregation from nodes
%%%
%%% Each job step with MPI enabled gets a PMI context here.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_manager).

-behaviour(gen_server).

-export([
    start_link/0,
    %% Job PMI lifecycle
    init_job/3,
    finalize_job/2,
    get_job_info/2,
    %% Rank operations
    register_rank/4,
    get_rank_info/3,
    %% KVS operations
    kvs_put/4,
    kvs_get/3,
    kvs_get_by_index/3,
    kvs_commit/2,
    %% Barrier operations
    barrier_in/3,
    %% Utility
    get_kvs_name/2
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

%% PMI job context - one per job step
-record(rank_info, {
    rank :: non_neg_integer(),
    node :: binary(),
    pid :: pid(),                                % Erlang process handling this rank
    status :: initialized | at_barrier | done
}).

-type rank_info() :: #rank_info{}.

-record(pmi_job, {
    job_id :: pos_integer(),
    step_id :: non_neg_integer(),
    size :: pos_integer(),                      % Total rank count
    kvs_name :: binary(),                        % KVS namespace name
    kvs :: flurm_pmi_kvs:kvs(),                 % Key-value store
    ranks = #{} :: #{non_neg_integer() => rank_info()},
    barrier_count = 0 :: non_neg_integer(),     % Ranks at barrier
    barrier_waiters = [] :: [pid()],            % Processes waiting for barrier
    committed = false :: boolean()               % KVS committed
}).

-record(state, {
    jobs = #{} :: #{{pos_integer(), non_neg_integer()} => #pmi_job{}}
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Initialize PMI for a job step
-spec init_job(pos_integer(), non_neg_integer(), pos_integer()) -> ok | {error, term()}.
init_job(JobId, StepId, Size) ->
    gen_server:call(?SERVER, {init_job, JobId, StepId, Size}).

%% @doc Finalize PMI for a job step
-spec finalize_job(pos_integer(), non_neg_integer()) -> ok.
finalize_job(JobId, StepId) ->
    gen_server:call(?SERVER, {finalize_job, JobId, StepId}).

%% @doc Get job PMI info (size, KVS name, etc.)
-spec get_job_info(pos_integer(), non_neg_integer()) -> {ok, map()} | {error, not_found}.
get_job_info(JobId, StepId) ->
    gen_server:call(?SERVER, {get_job_info, JobId, StepId}).

%% @doc Register a rank with its handler process
-spec register_rank(pos_integer(), non_neg_integer(), non_neg_integer(), binary()) -> ok | {error, term()}.
register_rank(JobId, StepId, Rank, Node) ->
    gen_server:call(?SERVER, {register_rank, JobId, StepId, Rank, Node}).

%% @doc Get info about a specific rank
-spec get_rank_info(pos_integer(), non_neg_integer(), non_neg_integer()) -> {ok, map()} | {error, term()}.
get_rank_info(JobId, StepId, Rank) ->
    gen_server:call(?SERVER, {get_rank_info, JobId, StepId, Rank}).

%% @doc Put a key-value pair in the job's KVS
-spec kvs_put(pos_integer(), non_neg_integer(), binary(), binary()) -> ok | {error, term()}.
kvs_put(JobId, StepId, Key, Value) ->
    gen_server:call(?SERVER, {kvs_put, JobId, StepId, Key, Value}).

%% @doc Get a value from the job's KVS
-spec kvs_get(pos_integer(), non_neg_integer(), binary()) -> {ok, binary()} | {error, term()}.
kvs_get(JobId, StepId, Key) ->
    gen_server:call(?SERVER, {kvs_get, JobId, StepId, Key}).

%% @doc Get key-value by index
-spec kvs_get_by_index(pos_integer(), non_neg_integer(), non_neg_integer()) ->
    {ok, binary(), binary()} | {error, term()}.
kvs_get_by_index(JobId, StepId, Index) ->
    gen_server:call(?SERVER, {kvs_get_by_index, JobId, StepId, Index}).

%% @doc Commit KVS (make all puts visible)
-spec kvs_commit(pos_integer(), non_neg_integer()) -> ok | {error, term()}.
kvs_commit(JobId, StepId) ->
    gen_server:call(?SERVER, {kvs_commit, JobId, StepId}).

%% @doc Enter barrier - blocks until all ranks arrive
-spec barrier_in(pos_integer(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
barrier_in(JobId, StepId, Rank) ->
    gen_server:call(?SERVER, {barrier_in, JobId, StepId, Rank}, 60000).

%% @doc Get the KVS name for a job step
-spec get_kvs_name(pos_integer(), non_neg_integer()) -> {ok, binary()} | {error, not_found}.
get_kvs_name(JobId, StepId) ->
    gen_server:call(?SERVER, {get_kvs_name, JobId, StepId}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    lager:info("PMI Manager started"),
    {ok, #state{}}.

handle_call({init_job, JobId, StepId, Size}, _From, State) ->
    Key = {JobId, StepId},
    case maps:is_key(Key, State#state.jobs) of
        true ->
            {reply, {error, already_exists}, State};
        false ->
            KvsName = iolist_to_binary([<<"kvs_">>, integer_to_binary(JobId),
                                        <<"_">>, integer_to_binary(StepId)]),
            Job = #pmi_job{
                job_id = JobId,
                step_id = StepId,
                size = Size,
                kvs_name = KvsName,
                kvs = flurm_pmi_kvs:new()
            },
            lager:info("PMI initialized for job ~p step ~p with ~p ranks",
                      [JobId, StepId, Size]),
            NewJobs = maps:put(Key, Job, State#state.jobs),
            {reply, ok, State#state{jobs = NewJobs}}
    end;

handle_call({finalize_job, JobId, StepId}, _From, State) ->
    Key = {JobId, StepId},
    case maps:find(Key, State#state.jobs) of
        {ok, _Job} ->
            lager:info("PMI finalized for job ~p step ~p", [JobId, StepId]),
            NewJobs = maps:remove(Key, State#state.jobs),
            {reply, ok, State#state{jobs = NewJobs}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_job_info, JobId, StepId}, _From, State) ->
    Key = {JobId, StepId},
    case maps:find(Key, State#state.jobs) of
        {ok, Job} ->
            Info = #{
                job_id => Job#pmi_job.job_id,
                step_id => Job#pmi_job.step_id,
                size => Job#pmi_job.size,
                kvs_name => Job#pmi_job.kvs_name,
                committed => Job#pmi_job.committed,
                rank_count => maps:size(Job#pmi_job.ranks)
            },
            {reply, {ok, Info}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({register_rank, JobId, StepId, Rank, Node}, {Pid, _}, State) ->
    Key = {JobId, StepId},
    case maps:find(Key, State#state.jobs) of
        {ok, Job} ->
            case Rank < Job#pmi_job.size of
                true ->
                    RankInfo = #rank_info{
                        rank = Rank,
                        node = Node,
                        pid = Pid,
                        status = initialized
                    },
                    NewRanks = maps:put(Rank, RankInfo, Job#pmi_job.ranks),
                    NewJob = Job#pmi_job{ranks = NewRanks},
                    NewJobs = maps:put(Key, NewJob, State#state.jobs),
                    lager:debug("Rank ~p registered for job ~p step ~p on node ~s",
                               [Rank, JobId, StepId, Node]),
                    {reply, ok, State#state{jobs = NewJobs}};
                false ->
                    {reply, {error, invalid_rank}, State}
            end;
        error ->
            {reply, {error, job_not_found}, State}
    end;

handle_call({get_rank_info, JobId, StepId, Rank}, _From, State) ->
    Key = {JobId, StepId},
    case maps:find(Key, State#state.jobs) of
        {ok, Job} ->
            case maps:find(Rank, Job#pmi_job.ranks) of
                {ok, RankInfo} ->
                    Info = #{
                        rank => RankInfo#rank_info.rank,
                        node => RankInfo#rank_info.node,
                        status => RankInfo#rank_info.status
                    },
                    {reply, {ok, Info}, State};
                error ->
                    {reply, {error, rank_not_found}, State}
            end;
        error ->
            {reply, {error, job_not_found}, State}
    end;

handle_call({kvs_put, JobId, StepId, Key, Value}, _From, State) ->
    JobKey = {JobId, StepId},
    case maps:find(JobKey, State#state.jobs) of
        {ok, Job} ->
            NewKvs = flurm_pmi_kvs:put(Job#pmi_job.kvs, Key, Value),
            NewJob = Job#pmi_job{kvs = NewKvs},
            NewJobs = maps:put(JobKey, NewJob, State#state.jobs),
            lager:debug("PMI KVS put: ~s = ~s (job ~p)", [Key, Value, JobId]),
            {reply, ok, State#state{jobs = NewJobs}};
        error ->
            {reply, {error, job_not_found}, State}
    end;

handle_call({kvs_get, JobId, StepId, Key}, _From, State) ->
    JobKey = {JobId, StepId},
    case maps:find(JobKey, State#state.jobs) of
        {ok, Job} ->
            Result = flurm_pmi_kvs:get(Job#pmi_job.kvs, Key),
            {reply, Result, State};
        error ->
            {reply, {error, job_not_found}, State}
    end;

handle_call({kvs_get_by_index, JobId, StepId, Index}, _From, State) ->
    JobKey = {JobId, StepId},
    case maps:find(JobKey, State#state.jobs) of
        {ok, Job} ->
            Result = flurm_pmi_kvs:get_by_index(Job#pmi_job.kvs, Index),
            {reply, Result, State};
        error ->
            {reply, {error, job_not_found}, State}
    end;

handle_call({kvs_commit, JobId, StepId}, _From, State) ->
    JobKey = {JobId, StepId},
    case maps:find(JobKey, State#state.jobs) of
        {ok, Job} ->
            NewJob = Job#pmi_job{committed = true},
            NewJobs = maps:put(JobKey, NewJob, State#state.jobs),
            lager:debug("PMI KVS committed for job ~p step ~p", [JobId, StepId]),
            {reply, ok, State#state{jobs = NewJobs}};
        error ->
            {reply, {error, job_not_found}, State}
    end;

handle_call({barrier_in, JobId, StepId, Rank}, From, State) ->
    JobKey = {JobId, StepId},
    case maps:find(JobKey, State#state.jobs) of
        {ok, Job} ->
            NewCount = Job#pmi_job.barrier_count + 1,
            NewWaiters = [From | Job#pmi_job.barrier_waiters],
            lager:debug("Rank ~p at barrier (~p/~p)", [Rank, NewCount, Job#pmi_job.size]),

            case NewCount >= Job#pmi_job.size of
                true ->
                    %% All ranks at barrier - release everyone
                    lager:info("All ~p ranks at barrier for job ~p step ~p, releasing",
                              [Job#pmi_job.size, JobId, StepId]),
                    lists:foreach(fun(Waiter) ->
                        gen_server:reply(Waiter, ok)
                    end, NewWaiters),
                    NewJob = Job#pmi_job{barrier_count = 0, barrier_waiters = []},
                    NewJobs = maps:put(JobKey, NewJob, State#state.jobs),
                    {noreply, State#state{jobs = NewJobs}};
                false ->
                    %% Still waiting for more ranks
                    NewJob = Job#pmi_job{barrier_count = NewCount, barrier_waiters = NewWaiters},
                    NewJobs = maps:put(JobKey, NewJob, State#state.jobs),
                    {noreply, State#state{jobs = NewJobs}}
            end;
        error ->
            {reply, {error, job_not_found}, State}
    end;

handle_call({get_kvs_name, JobId, StepId}, _From, State) ->
    JobKey = {JobId, StepId},
    case maps:find(JobKey, State#state.jobs) of
        {ok, Job} ->
            {reply, {ok, Job#pmi_job.kvs_name}, State};
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
