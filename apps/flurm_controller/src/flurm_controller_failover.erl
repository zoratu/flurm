%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Failover Handler
%%%
%%% Handles failover scenarios in the FLURM controller cluster:
%%% - Detects leader failure
%%% - Participates in new leader election
%%% - Recovers state from Ra log after becoming leader
%%% - Resumes job management after failover
%%%
%%% This module is notified by flurm_controller_cluster when
%%% leadership changes occur.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_failover).

-behaviour(gen_server).

%% API
-export([start_link/0,
         on_became_leader/0,
         on_lost_leadership/0,
         get_status/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-export([handle_became_leader/1, handle_lost_leadership/1,
         handle_recovery_complete/2, do_health_check/1,
         calculate_leader_uptime/1]).
-endif.

-define(SERVER, ?MODULE).
-define(RECOVERY_TIMEOUT, 30000).
-define(HEALTH_CHECK_INTERVAL, 5000).

-include_lib("flurm_core/include/flurm_core.hrl").

-record(state, {
    is_leader = false :: boolean(),
    became_leader_time :: erlang:timestamp() | undefined,
    recovery_status = idle :: idle | recovering | recovered | failed,
    recovery_error :: term() | undefined,
    health_check_ref :: reference() | undefined,
    pending_operations = [] :: [{reference(), term()}]
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the failover handler.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Called when this node becomes the leader.
-spec on_became_leader() -> ok.
on_became_leader() ->
    gen_server:cast(?SERVER, became_leader).

%% @doc Called when this node loses leadership.
-spec on_lost_leadership() -> ok.
on_lost_leadership() ->
    gen_server:cast(?SERVER, lost_leadership).

%% @doc Get the current failover status.
-spec get_status() -> map().
get_status() ->
    gen_server:call(?SERVER, get_status).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    process_flag(trap_exit, true),
    lager:info("FLURM Controller Failover Handler started"),
    {ok, #state{}}.

handle_call(get_status, _From, State) ->
    #state{
        is_leader = IsLeader,
        became_leader_time = BecameLeaderTime,
        recovery_status = RecoveryStatus,
        recovery_error = RecoveryError
    } = State,
    Status = #{
        is_leader => IsLeader,
        became_leader_time => BecameLeaderTime,
        recovery_status => RecoveryStatus,
        recovery_error => RecoveryError,
        uptime_as_leader => calculate_leader_uptime(BecameLeaderTime)
    },
    {reply, Status, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(became_leader, State) ->
    lager:info("Failover: This node became the LEADER"),
    NewState = handle_became_leader(State),
    {noreply, NewState};

handle_cast(lost_leadership, State) ->
    lager:info("Failover: This node lost leadership"),
    NewState = handle_lost_leadership(State),
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(start_recovery, #state{recovery_status = recovering} = State) ->
    %% Already recovering, ignore
    {noreply, State};

handle_info(start_recovery, State) ->
    lager:info("Starting state recovery after becoming leader"),
    NewState = do_recovery(State#state{recovery_status = recovering}),
    {noreply, NewState};

handle_info(health_check, State) ->
    NewState = do_health_check(State),
    %% Schedule next health check
    Ref = erlang:send_after(?HEALTH_CHECK_INTERVAL, self(), health_check),
    {noreply, NewState#state{health_check_ref = Ref}};

handle_info({recovery_complete, Result}, State) ->
    NewState = handle_recovery_complete(Result, State),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{health_check_ref = Ref}) ->
    case Ref of
        undefined -> ok;
        _ -> erlang:cancel_timer(Ref)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions - Leadership Changes
%%====================================================================

%% @doc Handle transition to leader.
handle_became_leader(State) ->
    Now = erlang:timestamp(),

    %% Cancel any existing health check
    case State#state.health_check_ref of
        undefined -> ok;
        OldRef -> erlang:cancel_timer(OldRef)
    end,

    %% Start recovery process
    self() ! start_recovery,

    %% Start health check timer
    HealthRef = erlang:send_after(?HEALTH_CHECK_INTERVAL, self(), health_check),

    State#state{
        is_leader = true,
        became_leader_time = Now,
        recovery_status = idle,
        recovery_error = undefined,
        health_check_ref = HealthRef
    }.

%% @doc Handle loss of leadership.
handle_lost_leadership(State) ->
    %% Cancel health check timer
    case State#state.health_check_ref of
        undefined -> ok;
        Ref -> erlang:cancel_timer(Ref)
    end,

    State#state{
        is_leader = false,
        became_leader_time = undefined,
        recovery_status = idle,
        health_check_ref = undefined
    }.

%%====================================================================
%% Internal functions - Recovery
%%====================================================================

%% @doc Perform state recovery after becoming leader.
do_recovery(State) ->
    lager:info("Performing state recovery..."),

    %% Recovery steps:
    %% 1. Sync state from Ra log
    %% 2. Check for incomplete operations
    %% 3. Resume pending jobs
    %% 4. Verify node connectivity

    %% Spawn recovery process to not block gen_server
    Self = self(),
    spawn_link(fun() ->
        Result = perform_recovery_steps(),
        Self ! {recovery_complete, Result}
    end),

    State.

%% @doc Perform the actual recovery steps.
perform_recovery_steps() ->
    try
        %% Step 1: Verify Ra cluster is healthy
        lager:info("Recovery step 1: Verifying Ra cluster health"),
        ok = verify_ra_cluster(),

        %% Step 2: Recover job state
        lager:info("Recovery step 2: Recovering job state"),
        ok = recover_job_state(),

        %% Step 3: Recover node registrations
        lager:info("Recovery step 3: Recovering node state"),
        ok = recover_node_state(),

        %% Step 4: Resume scheduler
        lager:info("Recovery step 4: Resuming scheduler"),
        ok = resume_scheduler(),

        lager:info("Recovery completed successfully"),
        {ok, recovered}
    catch
        Class:Reason:Stacktrace ->
            lager:error("Recovery failed: ~p:~p~n~p",
                        [Class, Reason, Stacktrace]),
            {error, {recovery_failed, Reason}}
    end.

%% @doc Verify the Ra cluster is healthy.
verify_ra_cluster() ->
    case flurm_controller_cluster:cluster_status() of
        #{ra_ready := true} ->
            ok;
        Status ->
            lager:warning("Ra cluster not ready: ~p", [Status]),
            %% Wait and retry
            timer:sleep(1000),
            verify_ra_cluster()
    end.

%% @doc Recover job state from Ra log.
recover_job_state() ->
    %% The Ra state machine maintains job state, so we just need
    %% to ensure the local job manager is synced
    try
        %% Get current jobs from the Ra cluster
        Jobs = flurm_job_manager:list_jobs(),
        lager:info("Recovered ~p jobs from state", [length(Jobs)]),

        %% Check for jobs that were running when failover occurred
        RunningJobs = [J || J <- Jobs, J#job.state =:= running],
        case RunningJobs of
            [] ->
                ok;
            _ ->
                lager:info("Found ~p running jobs that need status verification",
                           [length(RunningJobs)]),
                %% These jobs may need status verification
                %% The scheduler will handle this
                ok
        end,
        ok
    catch
        _:Error ->
            lager:warning("Job state recovery warning: ~p", [Error]),
            ok  %% Non-fatal
    end.

%% @doc Recover node state.
recover_node_state() ->
    try
        Nodes = flurm_node_manager_server:list_nodes(),
        lager:info("Recovered ~p nodes from state", [length(Nodes)]),

        %% Mark nodes that haven't sent heartbeat as potentially down
        Now = erlang:system_time(second),
        HeartbeatTimeout = 30,  %% seconds
        lists:foreach(fun(Node) ->
            case Node#node.last_heartbeat of
                undefined ->
                    ok;  %% New node, will update on first heartbeat
                LastHB when (Now - LastHB) > HeartbeatTimeout ->
                    lager:warning("Node ~s may be down (last heartbeat ~p seconds ago)",
                                  [Node#node.hostname, Now - LastHB]);
                _ ->
                    ok
            end
        end, Nodes),
        ok
    catch
        _:Error ->
            lager:warning("Node state recovery warning: ~p", [Error]),
            ok  %% Non-fatal
    end.

%% @doc Resume the scheduler after recovery.
resume_scheduler() ->
    try
        %% Trigger a scheduling cycle
        flurm_scheduler:trigger_schedule(),
        ok
    catch
        _:Error ->
            lager:warning("Scheduler resume warning: ~p", [Error]),
            ok  %% Non-fatal
    end.

%% @doc Handle recovery completion.
handle_recovery_complete({ok, recovered}, State) ->
    lager:info("State recovery completed successfully"),
    State#state{recovery_status = recovered, recovery_error = undefined};

handle_recovery_complete({error, Reason}, State) ->
    lager:error("State recovery failed: ~p", [Reason]),
    %% Schedule retry
    erlang:send_after(5000, self(), start_recovery),
    State#state{recovery_status = failed, recovery_error = Reason}.

%%====================================================================
%% Internal functions - Health Checks
%%====================================================================

%% @doc Perform periodic health check when leader.
do_health_check(#state{is_leader = false} = State) ->
    %% Not leader, skip health check
    State;
do_health_check(#state{recovery_status = RecStatus} = State) when RecStatus =/= recovered ->
    %% Still recovering, skip health check
    State;
do_health_check(State) ->
    %% Check cluster health
    case flurm_controller_cluster:is_leader() of
        true ->
            %% Still leader, all good
            check_component_health(),
            State;
        false ->
            %% Lost leadership but didn't get notified?
            lager:warning("Health check detected leadership loss"),
            handle_lost_leadership(State)
    end.

%% @doc Check health of various components.
check_component_health() ->
    %% Check job manager
    try
        _ = flurm_job_manager:list_jobs(),
        ok
    catch
        _:_ ->
            lager:warning("Job manager health check failed")
    end,

    %% Check node manager
    try
        _ = flurm_node_manager_server:list_nodes(),
        ok
    catch
        _:_ ->
            lager:warning("Node manager health check failed")
    end,

    ok.

%%====================================================================
%% Internal functions - Utilities
%%====================================================================

%% @doc Calculate how long this node has been leader.
calculate_leader_uptime(undefined) ->
    0;
calculate_leader_uptime(BecameLeaderTime) ->
    timer:now_diff(erlang:timestamp(), BecameLeaderTime) div 1000000.  %% seconds
