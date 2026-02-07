%%%-------------------------------------------------------------------
%%% @doc FLURM PMI Application
%%%
%%% Provides PMI (Process Management Interface) support for MPI jobs.
%%% This enables MPI programs to exchange connection information and
%%% synchronize across processes.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    lager:info("Starting FLURM PMI application"),
    flurm_pmi_sup:start_link().

stop(_State) ->
    ok.
