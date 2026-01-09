%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon Application
%%%
%%% This module implements the OTP application behaviour for the
%%% FLURM node daemon (flurmnd). It runs on each compute node and
%%% is responsible for executing jobs and reporting status to the
%%% controller.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_app).

-behaviour(application).

-export([start/2, stop/1]).

%%====================================================================
%% Application callbacks
%%====================================================================

start(_StartType, _StartArgs) ->
    lager:info("Starting FLURM Node Daemon (flurmnd)"),

    %% Get configuration
    {ok, ControllerHost} = application:get_env(flurm_node_daemon, controller_host),
    {ok, ControllerPort} = application:get_env(flurm_node_daemon, controller_port),

    lager:info("Node daemon will connect to controller at ~s:~p",
               [ControllerHost, ControllerPort]),

    %% Start the supervisor
    flurm_node_daemon_sup:start_link().

stop(_State) ->
    lager:info("Stopping FLURM Node Daemon"),
    ok.
