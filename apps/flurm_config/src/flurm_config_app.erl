%%%-------------------------------------------------------------------
%%% @doc FLURM Configuration Application
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_app).

-behaviour(application).

-export([start/2, stop/1]).

-ifdef(TEST).
%% No internal functions to export for application module
-export([]).
-endif.

start(_StartType, _StartArgs) ->
    flurm_config_sup:start_link().

stop(_State) ->
    ok.
