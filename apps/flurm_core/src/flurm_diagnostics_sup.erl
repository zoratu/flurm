%%%-------------------------------------------------------------------
%%% @doc FLURM Diagnostics Supervisor
%%%
%%% Optional supervisor for production monitoring. Can be started
%%% to enable continuous leak detection and health monitoring.
%%%
%%% Add to your supervision tree or start manually:
%%%   flurm_diagnostics_sup:start_link().
%%%   flurm_diagnostics_sup:start_link(#{interval => 30000}).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_diagnostics_sup).

-behaviour(supervisor).

-export([start_link/0, start_link/1]).
-export([init/1]).

-define(DEFAULT_INTERVAL, 60000).  % 1 minute

%%====================================================================
%% API
%%====================================================================

%% @doc Start with default settings
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start with custom settings
%% Options:
%%   interval - Leak detector check interval in ms (default: 60000)
%%   enable_leak_detector - Whether to start leak detector (default: true)
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Options).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init(Options) ->
    Interval = maps:get(interval, Options, ?DEFAULT_INTERVAL),
    EnableLeakDetector = maps:get(enable_leak_detector, Options, true),

    Children = case EnableLeakDetector of
        true ->
            [
                #{
                    id => flurm_leak_detector,
                    start => {flurm_diagnostics, start_link, [[Interval]]},
                    restart => permanent,
                    shutdown => 5000,
                    type => worker,
                    modules => [flurm_diagnostics]
                }
            ];
        false ->
            []
    end,

    {ok, {
        #{
            strategy => one_for_one,
            intensity => 5,
            period => 60
        },
        Children
    }}.
