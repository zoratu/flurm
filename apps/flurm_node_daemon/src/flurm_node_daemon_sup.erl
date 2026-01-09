%%%-------------------------------------------------------------------
%%% @doc FLURM Node Daemon Top-Level Supervisor
%%%
%%% This supervisor manages all the child processes of the FLURM
%%% node daemon, including the controller connector, job executor,
%%% and system monitor.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_daemon_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },

    Children = [
        %% System Monitor - collects node metrics
        #{
            id => flurm_system_monitor,
            start => {flurm_system_monitor, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_system_monitor]
        },
        %% Controller Connector - maintains connection to controller
        #{
            id => flurm_controller_connector,
            start => {flurm_controller_connector, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_controller_connector]
        },
        %% Job Executor Supervisor - supervises job execution
        #{
            id => flurm_job_executor_sup,
            start => {flurm_job_executor_sup, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [flurm_job_executor_sup]
        }
    ],

    {ok, {SupFlags, Children}}.
