%%%-------------------------------------------------------------------
%%% @doc FLURM Multi-Factor Priority System
%%%
%%% Calculates job priority using multiple weighted factors:
%%% - Age: How long job has been waiting (encourages FIFO)
%%% - JobSize: Number of nodes/CPUs requested
%%% - Partition: Partition priority modifier
%%% - QOS: Quality of Service priority
%%% - Nice: User-adjustable priority offset
%%% - FairShare: Usage-based priority (from flurm_fairshare)
%%%
%%% Final priority = Sum of (factor_weight * factor_value)
%%%
%%% Based on SLURM's multifactor priority plugin.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_priority).

-include("flurm_core.hrl").

%% API
-export([
    calculate_priority/1,
    calculate_priority/2,
    recalculate_all/0,
    get_priority_factors/1,
    set_weights/1,
    get_weights/0
]).

%% Internal exports for testing
-export([
    age_factor/2,
    size_factor/2,
    partition_factor/1,
    nice_factor/1
]).

%% Priority weight configuration (ETS table)
-define(WEIGHTS_TABLE, flurm_priority_weights).

%% Default weights (sum to ~1.0 for normalized output)
-define(DEFAULT_WEIGHTS, #{
    age => 1000,           % Age factor weight
    job_size => 200,       % Job size weight
    partition => 100,      % Partition priority weight
    qos => 500,            % QOS priority weight
    nice => 100,           % Nice value weight
    fairshare => 5000      % Fair-share weight (usually dominant)
}).

%% Age decay constant (half-life in seconds)
-define(AGE_DECAY_FACTOR, 86400).  % 1 day

%% Maximum age contribution (in seconds)
-define(MAX_AGE, 604800).  % 7 days

%%====================================================================
%% API
%%====================================================================

%% @doc Calculate priority for a job using all factors.
%% Returns a priority value where higher = more important.
-spec calculate_priority(map()) -> integer().
calculate_priority(JobInfo) ->
    calculate_priority(JobInfo, #{}).

%% @doc Calculate priority with optional context (fairshare value, etc.)
-spec calculate_priority(map(), map()) -> integer().
calculate_priority(JobInfo, Context) ->
    Weights = get_weights(),
    Factors = calculate_factors(JobInfo, Context, Weights),

    %% Sum weighted factors
    Priority = lists:sum([
        maps:get(Factor, Weights, 0) * Value
        || {Factor, Value} <- maps:to_list(Factors)
    ]),

    %% Clamp to valid range
    max(?MIN_PRIORITY, min(?MAX_PRIORITY * 100, round(Priority))).

%% @doc Recalculate priorities for all pending jobs.
%% Called periodically to update age-based priorities.
-spec recalculate_all() -> ok.
recalculate_all() ->
    %% Get all pending jobs from registry
    PendingJobs = flurm_job_registry:list_jobs_by_state(pending),

    %% Recalculate each job's priority
    lists:foreach(
        fun({_JobId, Pid}) ->
            case flurm_job:get_info(Pid) of
                {ok, Info} ->
                    NewPriority = calculate_priority(Info),
                    %% Update the job's priority
                    catch flurm_job:set_priority(Pid, NewPriority);
                _ ->
                    ok
            end
        end,
        PendingJobs
    ),
    ok.

%% @doc Get individual priority factors for a job (for debugging/display).
-spec get_priority_factors(map()) -> map().
get_priority_factors(JobInfo) ->
    Weights = get_weights(),
    calculate_factors(JobInfo, #{}, Weights).

%% @doc Set priority weights.
-spec set_weights(map()) -> ok.
set_weights(NewWeights) ->
    ensure_weights_table(),
    ets:insert(?WEIGHTS_TABLE, {weights, NewWeights}),
    ok.

%% @doc Get current priority weights.
-spec get_weights() -> map().
get_weights() ->
    ensure_weights_table(),
    case ets:lookup(?WEIGHTS_TABLE, weights) of
        [{weights, Weights}] -> Weights;
        [] -> ?DEFAULT_WEIGHTS
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
ensure_weights_table() ->
    case ets:whereis(?WEIGHTS_TABLE) of
        undefined ->
            ets:new(?WEIGHTS_TABLE, [named_table, public, set]),
            ets:insert(?WEIGHTS_TABLE, {weights, ?DEFAULT_WEIGHTS});
        _ ->
            ok
    end.

%% @private
%% Calculate all priority factors for a job
calculate_factors(JobInfo, Context, _Weights) ->
    Now = erlang:system_time(second),

    #{
        age => age_factor(JobInfo, Now),
        job_size => size_factor(JobInfo, get_cluster_size()),
        partition => partition_factor(JobInfo),
        qos => qos_factor(JobInfo),
        nice => nice_factor(JobInfo),
        fairshare => fairshare_factor(JobInfo, Context)
    }.

%% @doc Calculate age factor (0.0 to 1.0).
%% Older jobs get higher priority to prevent starvation.
-spec age_factor(map(), integer()) -> float().
age_factor(JobInfo, Now) ->
    SubmitTime = maps:get(submit_time, JobInfo, Now),
    %% Handle different time formats
    SubmitSeconds = case SubmitTime of
        {MegaSecs, Secs, _MicroSecs} ->
            MegaSecs * 1000000 + Secs;
        Ts when is_integer(Ts) ->
            Ts;
        _ ->
            Now
    end,

    Age = max(0, Now - SubmitSeconds),
    CappedAge = min(Age, ?MAX_AGE),

    %% Exponential growth toward 1.0
    1.0 - math:exp(-CappedAge / ?AGE_DECAY_FACTOR).

%% @doc Calculate job size factor (0.0 to 1.0).
%% Smaller jobs get slight priority boost (enables backfill).
-spec size_factor(map(), pos_integer()) -> float().
size_factor(JobInfo, ClusterSize) ->
    NumNodes = maps:get(num_nodes, JobInfo, 1),
    NumCpus = maps:get(num_cpus, JobInfo, 1),

    %% Normalize by cluster size
    NodeFraction = NumNodes / max(1, ClusterSize),

    %% Smaller jobs get higher factor (inverted)
    %% But very tiny jobs don't get too much boost
    1.0 - (0.5 * NodeFraction) - (0.5 * min(1.0, NumCpus / 64)).

%% @doc Calculate partition factor (0.0 to 1.0).
%% Based on partition priority configuration.
-spec partition_factor(map()) -> float().
partition_factor(JobInfo) ->
    Partition = maps:get(partition, JobInfo, <<"default">>),

    %% Try to get partition priority (catch in case registry not running)
    case catch flurm_partition_registry:get_partition_priority(Partition) of
        {ok, PartPriority} ->
            %% Normalize to 0.0-1.0 range (assuming max partition priority of 10000)
            min(1.0, PartPriority / 10000);
        _ ->
            0.5  % Default middle priority
    end.

%% @private
%% Calculate QOS factor (0.0 to 1.0)
qos_factor(JobInfo) ->
    QOS = maps:get(qos, JobInfo, <<"normal">>),
    case QOS of
        <<"high">> -> 1.0;
        <<"normal">> -> 0.5;
        <<"low">> -> 0.2;
        _ -> 0.5
    end.

%% @doc Calculate nice factor (-1.0 to 1.0).
%% Nice value from user (like Unix nice, lower = higher priority).
-spec nice_factor(map()) -> float().
nice_factor(JobInfo) ->
    Nice = maps:get(nice, JobInfo, 0),
    %% Nice ranges from -10000 to 10000 in SLURM
    %% Convert to -1.0 to 1.0 (inverted: lower nice = higher factor)
    -Nice / 10000.

%% @private
%% Calculate fairshare factor from context or flurm_fairshare module
fairshare_factor(JobInfo, Context) ->
    case maps:get(fairshare, Context, undefined) of
        undefined ->
            %% Try to get from fairshare module
            User = maps:get(user, JobInfo, <<"unknown">>),
            Account = maps:get(account, JobInfo, <<"default">>),
            case catch flurm_fairshare:get_priority_factor(User, Account) of
                Factor when is_float(Factor) -> Factor;
                _ -> 0.5  % Default if fairshare not available
            end;
        Factor ->
            Factor
    end.

%% @private
%% Get approximate cluster size for normalization
get_cluster_size() ->
    case catch flurm_node_registry:count_nodes() of
        Count when is_integer(Count), Count > 0 -> Count;
        _ -> 100  % Default assumption
    end.
