%%%-------------------------------------------------------------------
%%% @doc FLURM Job Dispatcher Facade
%%%
%%% This module provides a facade/API for job dispatching operations.
%%% It delegates to flurm_job_dispatcher_server when available (production),
%%% or operates in stub mode for testing when the server is not running.
%%%
%%% The server is started by flurm_controller_sup in the flurm_controller
%%% application. When running without the controller (e.g., in unit tests),
%%% this facade provides no-op stub behavior.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher).

-include("flurm_core.hrl").

%% API - matches flurm_job_dispatcher_server exports
-export([
    dispatch_job/2,
    cancel_job/2,
    preempt_job/2,
    requeue_job/1,
    drain_node/1,
    resume_node/1
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Dispatch a job to its allocated nodes.
%% Delegates to flurm_job_dispatcher_server if running, otherwise stub mode.
-spec dispatch_job(pos_integer(), map()) -> ok | {error, term()}.
dispatch_job(JobId, JobInfo) ->
    case server_running() of
        true ->
            flurm_job_dispatcher_server:dispatch_job(JobId, JobInfo);
        false ->
            %% Stub mode for testing
            AllocatedNodes = maps:get(allocated_nodes, JobInfo, []),
            lager:info("Stub dispatcher: job ~p to nodes: ~p", [JobId, AllocatedNodes]),
            ok
    end.

%% @doc Cancel a job on its allocated nodes.
%% Sends cancellation signal to node daemons to terminate the job.
-spec cancel_job(pos_integer(), [binary()]) -> ok | {error, term()}.
cancel_job(JobId, Nodes) ->
    case server_running() of
        true ->
            flurm_job_dispatcher_server:cancel_job(JobId, Nodes);
        false ->
            %% Stub mode for testing
            lager:info("Stub dispatcher: cancel job ~p on nodes: ~p", [JobId, Nodes]),
            ok
    end.

%% @doc Preempt a running job by sending signals to the nodes.
%% Options can include:
%%   - signal: sigterm | sigkill | sigstop | sigcont (default: sigterm)
%%   - nodes: list of nodes to send signal to (default: all allocated nodes)
%% Returns ok on success, {error, Reason} on failure.
-spec preempt_job(pos_integer(), map()) -> ok | {error, term()}.
preempt_job(JobId, Options) ->
    case server_running() of
        true ->
            flurm_job_dispatcher_server:preempt_job(JobId, Options);
        false ->
            %% Stub mode for testing
            Signal = maps:get(signal, Options, sigterm),
            lager:info("Stub dispatcher: preempt job ~p with signal ~p", [JobId, Signal]),
            ok
    end.

%% @doc Requeue a job after preemption.
%% This cleans up dispatcher state and allows the job to be re-dispatched.
-spec requeue_job(pos_integer()) -> ok.
requeue_job(JobId) ->
    case server_running() of
        true ->
            flurm_job_dispatcher_server:requeue_job(JobId);
        false ->
            %% Stub mode for testing
            lager:info("Stub dispatcher: requeue job ~p", [JobId]),
            ok
    end.

%% @doc Send drain signal to a node.
-spec drain_node(binary()) -> ok | {error, term()}.
drain_node(Hostname) ->
    case server_running() of
        true ->
            flurm_job_dispatcher_server:drain_node(Hostname);
        false ->
            %% Stub mode for testing
            lager:info("Stub dispatcher: drain node ~s", [Hostname]),
            ok
    end.

%% @doc Send resume signal to a node.
-spec resume_node(binary()) -> ok | {error, term()}.
resume_node(Hostname) ->
    case server_running() of
        true ->
            flurm_job_dispatcher_server:resume_node(Hostname);
        false ->
            %% Stub mode for testing
            lager:info("Stub dispatcher: resume node ~s", [Hostname]),
            ok
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Check if the dispatcher server is running.
%% Returns true if the server is registered and alive.
-spec server_running() -> boolean().
server_running() ->
    case whereis(flurm_job_dispatcher_server) of
        Pid when is_pid(Pid) -> is_process_alive(Pid);
        undefined -> false
    end.
