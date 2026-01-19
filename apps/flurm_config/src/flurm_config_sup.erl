%%%-------------------------------------------------------------------
%%% @doc FLURM Configuration Supervisor
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_config_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-ifdef(TEST).
%% Export init for direct testing of supervisor spec
-export([]).
-endif.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },

    Children = [
        #{
            id => flurm_config_server,
            start => {flurm_config_server, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_config_server]
        }
    ],

    {ok, {SupFlags, Children}}.
