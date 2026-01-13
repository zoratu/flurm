%%%-------------------------------------------------------------------
%%% @doc FLURM Job Dispatcher
%%%
%%% Dispatches jobs to compute nodes for execution. This module handles
%%% the communication between the scheduler and the actual node daemons.
%%%
%%% For testing purposes, this is a stub that immediately acknowledges
%%% job dispatch requests without actually sending them to nodes.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_dispatcher).

-include("flurm_core.hrl").

%% API
-export([
    dispatch_job/2,
    cancel_job/2
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Dispatch a job to its allocated nodes.
%% In production, this would send the job script to the node daemon(s).
%% For testing, this is a no-op that returns ok.
-spec dispatch_job(pos_integer(), map()) -> ok | {error, term()}.
dispatch_job(JobId, JobInfo) ->
    AllocatedNodes = maps:get(allocated_nodes, JobInfo, []),
    lager:info("Dispatching job ~p to nodes: ~p", [JobId, AllocatedNodes]),
    %% In tests, we just return ok without actually sending to nodes
    %% In production, this would:
    %% 1. Send job script to each node daemon
    %% 2. Wait for acknowledgement
    %% 3. Return ok or {error, Reason}
    ok.

%% @doc Cancel a job on its allocated nodes.
%% Sends cancellation signal to node daemons to terminate the job.
-spec cancel_job(pos_integer(), [binary()]) -> ok | {error, term()}.
cancel_job(JobId, Nodes) ->
    lager:info("Cancelling job ~p on nodes: ~p", [JobId, Nodes]),
    %% In tests, this is a no-op
    %% In production, this would send SIGTERM/SIGKILL to the job processes
    ok.
