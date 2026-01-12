%%%-------------------------------------------------------------------
%%% @doc FLURM Backfill Scheduler
%%%
%%% Implements backfill scheduling to improve cluster utilization.
%%% While a high-priority job waits for sufficient resources, smaller
%%% jobs can "backfill" into available resources if they will complete
%%% before the high-priority job's resources become available.
%%%
%%% Algorithm:
%%% 1. Identify the highest-priority pending job (the "blocker")
%%% 2. Calculate when resources will become available for the blocker
%%% 3. For each lower-priority job, check if it can run AND complete
%%%    before the blocker needs those resources
%%% 4. Schedule backfill jobs that fit
%%%
%%% This significantly improves utilization on clusters with
%%% heterogeneous job sizes.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_backfill).

-include("flurm_core.hrl").

%% Suppress warnings for unused functions kept for future use
-compile({nowarn_unused_function, [estimate_node_free_time/2, estimate_job_end_times/2]}).

%% API
-export([
    %% Scheduler integration API
    run_backfill_cycle/2,
    get_backfill_candidates/1,
    is_backfill_enabled/0,
    %% Core API
    find_backfill_jobs/2,
    can_backfill/3,
    calculate_shadow_time/2,
    get_resource_timeline/1
]).

%% Configuration
-define(MAX_BACKFILL_JOBS, 100).       % Max jobs to consider for backfill
-define(BACKFILL_WINDOW, 86400).       % Look ahead window (24 hours)
-define(MIN_BACKFILL_TIME, 60).        % Minimum job time to consider (1 min)

%%====================================================================
%% API
%%====================================================================

%% @doc Find jobs that can backfill while waiting for high-priority job.
%% Returns list of {JobId, JobPid, NodeNames} that can be scheduled.
-spec find_backfill_jobs(map(), [map()]) -> [{pos_integer(), pid(), [binary()]}].
find_backfill_jobs(BlockerJob, CandidateJobs) ->
    %% Get current cluster state
    Timeline = get_resource_timeline(BlockerJob),

    %% Calculate when blocker can start (shadow time)
    ShadowTime = calculate_shadow_time(BlockerJob, Timeline),

    %% Filter and sort candidate jobs by priority (descending)
    SortedCandidates = lists:sort(
        fun(A, B) ->
            maps:get(priority, A, 0) > maps:get(priority, B, 0)
        end,
        CandidateJobs
    ),

    %% Try to fit jobs into backfill slots
    find_fitting_jobs(SortedCandidates, Timeline, ShadowTime, [], ?MAX_BACKFILL_JOBS).

%% @doc Check if a specific job can backfill.
%% Returns {true, Nodes} if it can run, false otherwise.
-spec can_backfill(map(), map(), integer()) -> {true, [binary()]} | false.
can_backfill(Job, Timeline, ShadowTime) ->
    NumNodes = maps:get(num_nodes, Job, 1),
    NumCpus = maps:get(num_cpus, Job, 1),
    MemoryMb = maps:get(memory_mb, Job, 1024),
    TimeLimit = maps:get(time_limit, Job, 3600),

    %% Check if job can complete before shadow time
    Now = erlang:system_time(second),
    EndTime = Now + TimeLimit,

    case EndTime =< ShadowTime of
        true ->
            %% Job can complete in time, find available nodes
            find_backfill_nodes(NumNodes, NumCpus, MemoryMb, TimeLimit, Timeline);
        false ->
            false
    end.

%% @doc Calculate the "shadow time" - when resources will be available
%% for the blocking job.
-spec calculate_shadow_time(map(), map()) -> integer().
calculate_shadow_time(BlockerJob, Timeline) ->
    NumNodes = maps:get(num_nodes, BlockerJob, 1),
    NumCpus = maps:get(num_cpus, BlockerJob, 1),
    MemoryMb = maps:get(memory_mb, BlockerJob, 1024),

    %% Get timeline of when nodes become free
    NodeEndTimes = maps:get(node_end_times, Timeline, []),

    %% Sort by end time
    SortedEndTimes = lists:sort(
        fun({_, T1}, {_, T2}) -> T1 =< T2 end,
        NodeEndTimes
    ),

    %% Find when enough nodes will be available
    find_shadow_time(SortedEndTimes, NumNodes, NumCpus, MemoryMb, Timeline).

%% @doc Get a timeline of resource availability.
%% Returns a map with current free resources and future availability.
-spec get_resource_timeline(map()) -> map().
get_resource_timeline(_BlockerJob) ->
    Now = erlang:system_time(second),

    %% Get all nodes
    AllNodes = case catch flurm_node_registry:list_all_nodes() of
        Nodes when is_list(Nodes) -> Nodes;
        _ -> []
    end,

    %% Build timeline of when each node becomes free
    {FreeNodes, BusyNodes, NodeEndTimes} = lists:foldl(
        fun({NodeName, _Pid}, {Free, Busy, EndTimes}) ->
            case flurm_node_registry:get_node_entry(NodeName) of
                {ok, #node_entry{state = up, cpus_avail = Cpus, memory_avail = Mem}} ->
                    case Cpus > 0 andalso Mem > 0 of
                        true ->
                            %% Node is free
                            {[{NodeName, Cpus, Mem} | Free], Busy, EndTimes};
                        false ->
                            %% Node is busy, estimate when it becomes free
                            EstEndTime = estimate_node_free_time(NodeName, Now),
                            {Free, [{NodeName, Cpus, Mem} | Busy],
                             [{NodeName, EstEndTime} | EndTimes]}
                    end;
                _ ->
                    {Free, Busy, EndTimes}
            end
        end,
        {[], [], []},
        AllNodes
    ),

    #{
        timestamp => Now,
        free_nodes => FreeNodes,
        busy_nodes => BusyNodes,
        node_end_times => NodeEndTimes,
        total_nodes => length(AllNodes)
    }.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Find jobs that fit in backfill slots
find_fitting_jobs([], _Timeline, _ShadowTime, Acc, _MaxJobs) ->
    lists:reverse(Acc);
find_fitting_jobs(_Jobs, _Timeline, _ShadowTime, Acc, 0) ->
    lists:reverse(Acc);
find_fitting_jobs([Job | Rest], Timeline, ShadowTime, Acc, MaxJobs) ->
    JobId = maps:get(job_id, Job),
    JobPid = maps:get(pid, Job, undefined),

    case can_backfill(Job, Timeline, ShadowTime) of
        {true, Nodes} ->
            %% Update timeline to reflect this allocation
            NewTimeline = reserve_nodes_in_timeline(Nodes, Job, Timeline),
            find_fitting_jobs(Rest, NewTimeline, ShadowTime,
                              [{JobId, JobPid, Nodes} | Acc], MaxJobs - 1);
        false ->
            find_fitting_jobs(Rest, Timeline, ShadowTime, Acc, MaxJobs)
    end.

%% @private
%% Find nodes available for backfill
find_backfill_nodes(NumNodes, NumCpus, MemoryMb, _TimeLimit, Timeline) ->
    FreeNodes = maps:get(free_nodes, Timeline, []),

    %% Filter nodes that meet resource requirements
    SuitableNodes = lists:filter(
        fun({_Name, Cpus, Mem}) ->
            Cpus >= NumCpus andalso Mem >= MemoryMb
        end,
        FreeNodes
    ),

    case length(SuitableNodes) >= NumNodes of
        true ->
            %% Take required number of nodes
            SelectedNodes = lists:sublist(SuitableNodes, NumNodes),
            NodeNames = [Name || {Name, _, _} <- SelectedNodes],
            {true, NodeNames};
        false ->
            false
    end.

%% @private
%% Calculate when blocker job can start
find_shadow_time([], _NumNodes, _NumCpus, _MemoryMb, Timeline) ->
    %% No busy nodes, blocker can start now (but might not have resources)
    Now = maps:get(timestamp, Timeline, erlang:system_time(second)),
    Now + ?BACKFILL_WINDOW;

find_shadow_time(EndTimes, NumNodes, NumCpus, MemoryMb, Timeline) ->
    Now = maps:get(timestamp, Timeline, erlang:system_time(second)),
    FreeNodes = maps:get(free_nodes, Timeline, []),

    %% Count how many free nodes already meet requirements
    CurrentSuitable = length([N || {_, C, M} = N <- FreeNodes,
                                   C >= NumCpus, M >= MemoryMb]),

    case CurrentSuitable >= NumNodes of
        true ->
            %% Already have enough resources
            Now;
        false ->
            %% Need to wait for more nodes
            Needed = NumNodes - CurrentSuitable,
            find_nth_end_time(EndTimes, Needed, Now + ?BACKFILL_WINDOW)
    end.

%% @private
%% Find when the Nth node becomes free
find_nth_end_time([], _N, Default) ->
    Default;
find_nth_end_time([{_Node, Time} | _Rest], 1, _Default) ->
    Time;
find_nth_end_time([_ | Rest], N, Default) when N > 1 ->
    find_nth_end_time(Rest, N - 1, Default).

%% @private
%% Estimate when a busy node will become free
estimate_node_free_time(NodeName, Now) ->
    %% Get jobs running on this node
    case flurm_node_registry:get_node_entry(NodeName) of
        {ok, #node_entry{}} ->
            %% Try to get job info from node process
            case catch flurm_node:get_jobs(NodeName) of
                Jobs when is_list(Jobs) ->
                    estimate_job_end_times(Jobs, Now);
                _ ->
                    Now + 3600  % Default 1 hour if unknown
            end;
        _ ->
            Now + 3600
    end.

%% @private
%% Estimate when jobs on a node will complete
estimate_job_end_times([], Now) ->
    Now;
estimate_job_end_times(JobIds, Now) ->
    EndTimes = lists:filtermap(
        fun(JobId) ->
            case flurm_job_registry:lookup_job(JobId) of
                {ok, Pid} ->
                    case catch flurm_job:get_info(Pid) of
                        {ok, Info} ->
                            StartTime = maps:get(start_time, Info, Now),
                            TimeLimit = maps:get(time_limit, Info, 3600),
                            StartSec = timestamp_to_seconds(StartTime),
                            {true, StartSec + TimeLimit};
                        _ ->
                            false
                    end;
                _ ->
                    false
            end
        end,
        JobIds
    ),
    case EndTimes of
        [] -> Now + 3600;
        _ -> lists:max(EndTimes)
    end.

%% @private
%% Convert various timestamp formats to seconds
timestamp_to_seconds({MegaSecs, Secs, _MicroSecs}) ->
    MegaSecs * 1000000 + Secs;
timestamp_to_seconds(Ts) when is_integer(Ts) ->
    Ts;
timestamp_to_seconds(_) ->
    erlang:system_time(second).

%% @private
%% Update timeline to show nodes as reserved
reserve_nodes_in_timeline(NodeNames, Job, Timeline) ->
    FreeNodes = maps:get(free_nodes, Timeline, []),
    TimeLimit = maps:get(time_limit, Job, 3600),
    Now = maps:get(timestamp, Timeline, erlang:system_time(second)),
    EndTime = Now + TimeLimit,

    %% Remove reserved nodes from free list
    NewFreeNodes = lists:filter(
        fun({Name, _, _}) -> not lists:member(Name, NodeNames) end,
        FreeNodes
    ),

    %% Add to end times
    NodeEndTimes = maps:get(node_end_times, Timeline, []),
    NewEndTimes = [{Name, EndTime} || Name <- NodeNames] ++ NodeEndTimes,

    Timeline#{
        free_nodes => NewFreeNodes,
        node_end_times => NewEndTimes
    }.

%%====================================================================
%% Scheduler Integration API
%%====================================================================

%% @doc Run a complete backfill cycle for the scheduler.
%% Takes the blocker job (highest priority pending job that can't run) and
%% a list of candidate jobs (lower priority pending jobs).
%% Returns a list of {JobId, NodeNames} for jobs that can be backfilled.
-spec run_backfill_cycle(map() | undefined, [map()]) -> [{pos_integer(), [binary()]}].
run_backfill_cycle(undefined, _CandidateJobs) ->
    %% No blocker job - no need for backfill
    [];
run_backfill_cycle(_BlockerJob, []) ->
    %% No candidates to backfill
    [];
run_backfill_cycle(BlockerJob, CandidateJobs) ->
    case is_backfill_enabled() of
        false ->
            [];
        true ->
            %% Run the backfill algorithm
            Results = find_backfill_jobs(BlockerJob, CandidateJobs),
            %% Convert to simplified format for scheduler
            [{JobId, Nodes} || {JobId, _Pid, Nodes} <- Results]
    end.

%% @doc Get candidate jobs for backfill from the job manager.
%% Returns pending jobs sorted by priority (descending).
-spec get_backfill_candidates([pos_integer()]) -> [map()].
get_backfill_candidates(JobIds) ->
    %% Retrieve job info for each pending job ID
    Jobs = lists:filtermap(
        fun(JobId) ->
            case catch flurm_job_manager:get_job(JobId) of
                {ok, #job{state = pending} = Job} ->
                    {true, job_record_to_map(Job)};
                {ok, #job{state = held}} ->
                    %% Skip held jobs
                    false;
                _ ->
                    false
            end
        end,
        JobIds
    ),
    %% Sort by priority (descending)
    lists:sort(
        fun(A, B) ->
            maps:get(priority, A, 0) > maps:get(priority, B, 0)
        end,
        Jobs
    ).

%% @doc Check if backfill scheduling is enabled.
%% This can be configured via application environment.
-spec is_backfill_enabled() -> boolean().
is_backfill_enabled() ->
    %% Check for scheduler type config
    case application:get_env(flurm_core, scheduler_type, fifo) of
        backfill -> true;
        fifo_backfill -> true;
        priority_backfill -> true;
        _ ->
            %% Also check explicit backfill_enabled flag
            application:get_env(flurm_core, backfill_enabled, true)
    end.

%% @private
%% Convert a job record to a map for backfill processing
job_record_to_map(#job{} = Job) ->
    #{
        job_id => Job#job.id,
        pid => undefined,
        name => Job#job.name,
        user => Job#job.user,
        partition => Job#job.partition,
        state => Job#job.state,
        num_nodes => Job#job.num_nodes,
        num_cpus => Job#job.num_cpus,
        memory_mb => Job#job.memory_mb,
        time_limit => Job#job.time_limit,
        priority => Job#job.priority,
        submit_time => Job#job.submit_time,
        start_time => Job#job.start_time,
        account => Job#job.account,
        qos => Job#job.qos
    }.
