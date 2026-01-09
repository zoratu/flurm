%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon API
%%%
%%% Main API module for the FLURM node daemon (flurmnd).
%%% Provides convenience functions for interacting with daemon components.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon).

-export([
    %% Status functions
    get_status/0,
    get_metrics/0,
    is_connected/0,

    %% Job functions
    list_running_jobs/0,
    get_job_status/1,
    cancel_job/1
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Get overall node daemon status
-spec get_status() -> map().
get_status() ->
    ConnState = flurm_controller_connector:get_state(),
    Metrics = flurm_system_monitor:get_metrics(),
    #{
        connected => maps:get(connected, ConnState, false),
        registered => maps:get(registered, ConnState, false),
        node_id => maps:get(node_id, ConnState, undefined),
        metrics => Metrics
    }.

%% @doc Get system metrics
-spec get_metrics() -> map().
get_metrics() ->
    flurm_system_monitor:get_metrics().

%% @doc Check if connected to controller
-spec is_connected() -> boolean().
is_connected() ->
    State = flurm_controller_connector:get_state(),
    maps:get(connected, State, false).

%% @doc List currently running jobs
-spec list_running_jobs() -> [{pos_integer(), pid()}].
list_running_jobs() ->
    State = flurm_controller_connector:get_state(),
    case maps:get(running_jobs, State, 0) of
        Count when is_integer(Count) ->
            %% Just return count for now
            [{count, Count}];
        _ ->
            []
    end.

%% @doc Get status of a specific job
-spec get_job_status(pid()) -> map() | {error, not_found}.
get_job_status(Pid) when is_pid(Pid) ->
    try
        flurm_job_executor:get_status(Pid)
    catch
        exit:{noproc, _} ->
            {error, not_found}
    end.

%% @doc Cancel a running job
-spec cancel_job(pid()) -> ok | {error, not_found}.
cancel_job(Pid) when is_pid(Pid) ->
    try
        flurm_job_executor:cancel(Pid),
        ok
    catch
        exit:{noproc, _} ->
            {error, not_found}
    end.
