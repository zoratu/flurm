%%%-------------------------------------------------------------------
%%% @doc FLURM Database Daemon Application
%%%
%%% This application provides the accounting database daemon (flurmbd),
%%% which is the FLURM equivalent of SLURM's slurmdbd.
%%%
%%% The daemon:
%%% - Stores job accounting records
%%% - Manages users, accounts, associations, QOS
%%% - Provides persistence for accounting data
%%% - Handles sacctmgr and slurmctld connections
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_app).

-behaviour(application).

-export([start/2, stop/1]).

%%====================================================================
%% Application callbacks
%%====================================================================

start(_StartType, _StartArgs) ->
    lager:info("Starting FLURM Database Daemon (flurmbd)"),
    flurm_dbd_sup:start_link().

stop(_State) ->
    lager:info("Stopping FLURM Database Daemon"),
    ok.
