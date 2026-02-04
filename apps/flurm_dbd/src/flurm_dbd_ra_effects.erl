%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Ra Effects Module
%%%
%%% Handles side effects from the Ra state machine.
%%% Called when specific events occur during consensus operations.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_ra_effects).

-export([
    job_recorded/1,
    became_leader/1,
    became_follower/1
]).

%%====================================================================
%% API Functions
%%====================================================================

%% @doc Called when a job completion is recorded through Ra consensus.
-spec job_recorded(term()) -> ok.
job_recorded(JobRecord) ->
    lager:debug("Job recorded through Ra consensus: ~p", [JobRecord]),
    ok.

%% @doc Called when this node becomes the Ra leader.
-spec became_leader(node()) -> ok.
became_leader(Node) ->
    lager:info("FLURM DBD Ra node ~p became leader", [Node]),
    ok.

%% @doc Called when this node becomes a Ra follower.
-spec became_follower(node()) -> ok.
became_follower(Node) ->
    lager:debug("FLURM DBD Ra node ~p became follower", [Node]),
    ok.
