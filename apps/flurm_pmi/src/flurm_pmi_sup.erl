%%%-------------------------------------------------------------------
%%% @doc FLURM PMI Supervisor
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },

    Children = [
        #{
            id => flurm_pmi_manager,
            start => {flurm_pmi_manager, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [flurm_pmi_manager]
        }
    ],

    {ok, {SupFlags, Children}}.
