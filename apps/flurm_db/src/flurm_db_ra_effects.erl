%%%-------------------------------------------------------------------
%%% @doc FLURM Database Ra Effects Handler
%%%
%%% This module handles side effects that are triggered after Ra
%%% consensus is reached. Effects are only executed on the leader
%%% node to avoid duplication.
%%%
%%% Effects include:
%%% - Spawning job processes after job submission
%%% - Notifying schedulers of state changes
%%% - Sending events to external systems
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_effects).

-include("flurm_db.hrl").

%% Effect handlers
-export([
    job_submitted/1,
    job_cancelled/1,
    job_state_changed/3,
    job_allocated/2,
    job_completed/2,
    node_registered/1,
    node_updated/1,
    node_state_changed/3,
    node_unregistered/1,
    partition_created/1,
    partition_deleted/1,
    became_leader/1,
    became_follower/1
]).

%% Event subscribers
-export([
    subscribe/1,
    unsubscribe/1
]).

%%====================================================================
%% Effect Handlers
%%====================================================================

%% @doc Called when a job is submitted.
%% Spawns the job process if running on the leader.
-spec job_submitted(#ra_job{}) -> ok.
job_submitted(#ra_job{id = JobId} = Job) ->
    %% Log the event
    log_event(job_submitted, #{job_id => JobId, job => job_to_map(Job)}),

    %% Notify the scheduler that a new job is pending
    notify_scheduler({job_pending, JobId}),

    %% Notify subscribers
    notify_subscribers({job_submitted, Job}),
    ok.

%% @doc Called when a job is cancelled.
-spec job_cancelled(#ra_job{}) -> ok.
job_cancelled(#ra_job{id = JobId} = Job) ->
    log_event(job_cancelled, #{job_id => JobId}),

    %% Tell the scheduler to remove this job from pending queue
    notify_scheduler({job_cancelled, JobId}),

    %% If job has allocated nodes, release them
    case Job#ra_job.allocated_nodes of
        [] -> ok;
        Nodes -> release_nodes(JobId, Nodes)
    end,

    %% Notify subscribers
    notify_subscribers({job_cancelled, Job}),
    ok.

%% @doc Called when a job state changes.
-spec job_state_changed(job_id(), atom(), atom()) -> ok.
job_state_changed(JobId, OldState, NewState) ->
    log_event(job_state_changed, #{
        job_id => JobId,
        old_state => OldState,
        new_state => NewState
    }),

    %% Handle specific state transitions
    case {OldState, NewState} of
        {pending, configuring} ->
            notify_scheduler({job_configuring, JobId});
        {configuring, running} ->
            notify_scheduler({job_running, JobId});
        {_, completed} ->
            notify_scheduler({job_completed, JobId});
        {_, failed} ->
            notify_scheduler({job_failed, JobId});
        _ ->
            ok
    end,

    %% Notify subscribers
    notify_subscribers({job_state_changed, JobId, OldState, NewState}),
    ok.

%% @doc Called when nodes are allocated to a job.
-spec job_allocated(#ra_job{}, [node_name()]) -> ok.
job_allocated(#ra_job{id = JobId} = Job, Nodes) ->
    log_event(job_allocated, #{job_id => JobId, nodes => Nodes}),

    %% Notify node manager to update node resource usage
    lists:foreach(fun(NodeName) ->
        notify_node_manager({allocate, NodeName, JobId,
                            {Job#ra_job.num_cpus,
                             Job#ra_job.memory_mb,
                             0}})  %% GPUs not tracked in job spec yet
    end, Nodes),

    %% Notify subscribers
    notify_subscribers({job_allocated, Job, Nodes}),
    ok.

%% @doc Called when a job completes (successfully or with error).
-spec job_completed(#ra_job{}, integer()) -> ok.
job_completed(#ra_job{id = JobId} = Job, ExitCode) ->
    log_event(job_completed, #{job_id => JobId, exit_code => ExitCode}),

    %% Release allocated nodes
    case Job#ra_job.allocated_nodes of
        [] -> ok;
        Nodes -> release_nodes(JobId, Nodes)
    end,

    %% Notify scheduler
    notify_scheduler({job_completed, JobId, ExitCode}),

    %% Notify subscribers
    notify_subscribers({job_completed, Job, ExitCode}),
    ok.

%% @doc Called when a node is registered.
-spec node_registered(#ra_node{}) -> ok.
node_registered(#ra_node{name = NodeName} = Node) ->
    log_event(node_registered, #{node_name => NodeName, node => node_to_map(Node)}),

    %% Notify scheduler of new capacity
    notify_scheduler({node_available, NodeName}),

    %% Notify subscribers
    notify_subscribers({node_registered, Node}),
    ok.

%% @doc Called when a node is updated.
-spec node_updated(#ra_node{}) -> ok.
node_updated(#ra_node{name = NodeName} = Node) ->
    log_event(node_updated, #{node_name => NodeName}),

    %% Notify subscribers
    notify_subscribers({node_updated, Node}),
    ok.

%% @doc Called when a node state changes.
-spec node_state_changed(node_name(), atom(), atom()) -> ok.
node_state_changed(NodeName, OldState, NewState) ->
    log_event(node_state_changed, #{
        node_name => NodeName,
        old_state => OldState,
        new_state => NewState
    }),

    %% Handle specific state transitions
    case NewState of
        down ->
            %% Node went down - notify scheduler
            notify_scheduler({node_down, NodeName});
        up when OldState =/= up ->
            %% Node came back up
            notify_scheduler({node_available, NodeName});
        drain ->
            %% Node is draining - don't schedule new jobs
            notify_scheduler({node_draining, NodeName});
        _ ->
            ok
    end,

    %% Notify subscribers
    notify_subscribers({node_state_changed, NodeName, OldState, NewState}),
    ok.

%% @doc Called when a node is unregistered.
-spec node_unregistered(#ra_node{}) -> ok.
node_unregistered(#ra_node{name = NodeName} = Node) ->
    log_event(node_unregistered, #{node_name => NodeName}),

    %% Notify scheduler that this node is no longer available
    notify_scheduler({node_unavailable, NodeName}),

    %% Handle any jobs that were running on this node
    lists:foreach(fun(JobId) ->
        %% Mark job as failed due to node failure
        catch flurm_db_ra:update_job_state(JobId, node_fail)
    end, Node#ra_node.running_jobs),

    %% Notify subscribers
    notify_subscribers({node_unregistered, Node}),
    ok.

%% @doc Called when a partition is created.
-spec partition_created(#ra_partition{}) -> ok.
partition_created(#ra_partition{name = PartName} = Partition) ->
    log_event(partition_created, #{partition_name => PartName}),

    %% Notify subscribers
    notify_subscribers({partition_created, Partition}),
    ok.

%% @doc Called when a partition is deleted.
-spec partition_deleted(#ra_partition{}) -> ok.
partition_deleted(#ra_partition{name = PartName} = Partition) ->
    log_event(partition_deleted, #{partition_name => PartName}),

    %% Notify subscribers
    notify_subscribers({partition_deleted, Partition}),
    ok.

%% @doc Called when this node becomes the Ra leader.
-spec became_leader(node()) -> ok.
became_leader(Node) ->
    log_event(became_leader, #{node => Node}),

    %% Start scheduler if not running
    maybe_start_scheduler(),

    %% Notify the failover handler that we became leader
    notify_failover_handler(became_leader),

    %% Notify subscribers
    notify_subscribers({became_leader, Node}),
    ok.

%% @doc Called when this node becomes a Ra follower.
-spec became_follower(node()) -> ok.
became_follower(Node) ->
    log_event(became_follower, #{node => Node}),

    %% Stop scheduler if running
    maybe_stop_scheduler(),

    %% Notify the failover handler that we lost leadership
    notify_failover_handler(became_follower),

    %% Notify subscribers
    notify_subscribers({became_follower, Node}),
    ok.

%%====================================================================
%% Subscriber Management
%%====================================================================

%% @doc Subscribe to Ra effects events.
-spec subscribe(pid()) -> ok.
subscribe(Pid) ->
    %% Store subscriber in a pg group
    case pg:start(pg) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    pg:join(flurm_db_effects, Pid),
    ok.

%% @doc Unsubscribe from Ra effects events.
-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) ->
    catch pg:leave(flurm_db_effects, Pid),
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
log_event(EventType, Data) ->
    %% In production, use proper logging
    error_logger:info_msg("FLURM DB Ra Effect: ~p ~p~n", [EventType, Data]).

%% @private
notify_scheduler(Event) ->
    %% Try to notify the scheduler process
    case whereis(flurm_scheduler) of
        undefined -> ok;
        Pid -> Pid ! {ra_event, Event}
    end.

%% @private
notify_node_manager(Event) ->
    %% Try to notify the node manager process
    case whereis(flurm_node_manager) of
        undefined -> ok;
        Pid -> Pid ! {ra_event, Event}
    end.

%% @private
release_nodes(JobId, Nodes) ->
    lists:foreach(fun(NodeName) ->
        notify_node_manager({release, NodeName, JobId})
    end, Nodes).

%% @private
maybe_start_scheduler() ->
    %% Try to start the scheduler if it's not running
    case whereis(flurm_scheduler) of
        undefined ->
            %% The scheduler should be started by the application
            %% This is just a notification that we're the leader now
            ok;
        Pid ->
            Pid ! activate_scheduling
    end.

%% @private
maybe_stop_scheduler() ->
    %% Tell scheduler to pause (if running)
    case whereis(flurm_scheduler) of
        undefined -> ok;
        Pid -> Pid ! pause_scheduling
    end.

%% @private
notify_failover_handler(Event) ->
    %% Try to notify the failover handler about leadership changes
    case whereis(flurm_controller_failover) of
        undefined -> ok;
        _Pid ->
            case Event of
                became_leader -> catch flurm_controller_failover:on_became_leader();
                became_follower -> catch flurm_controller_failover:on_lost_leadership()
            end
    end.

%% @private
notify_subscribers(Event) ->
    %% Send event to all subscribers
    case catch pg:get_members(flurm_db_effects) of
        Pids when is_list(Pids) ->
            lists:foreach(fun(Pid) ->
                Pid ! {flurm_db_event, Event}
            end, Pids);
        _ ->
            ok
    end.

%% @private
job_to_map(#ra_job{} = Job) ->
    #{
        id => Job#ra_job.id,
        name => Job#ra_job.name,
        user => Job#ra_job.user,
        partition => Job#ra_job.partition,
        state => Job#ra_job.state,
        num_nodes => Job#ra_job.num_nodes,
        num_cpus => Job#ra_job.num_cpus,
        memory_mb => Job#ra_job.memory_mb,
        priority => Job#ra_job.priority
    }.

%% @private
node_to_map(#ra_node{} = Node) ->
    #{
        name => Node#ra_node.name,
        hostname => Node#ra_node.hostname,
        cpus => Node#ra_node.cpus,
        memory_mb => Node#ra_node.memory_mb,
        state => Node#ra_node.state
    }.
