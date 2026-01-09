%%%-------------------------------------------------------------------
%%% @doc FLURM Database Supervisor
%%%
%%% Top-level supervisor for the FLURM database application.
%%% Responsible for starting and supervising the Ra cluster and
%%% related processes.
%%%
%%% The supervisor starts:
%%% 1. Ra application (if not already started)
%%% 2. The Ra cluster for FLURM DB
%%% 3. A process group for event subscribers
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_sup).
-behaviour(supervisor).

-include("flurm_db.hrl").

%% API
-export([
    start_link/0,
    start_link/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the supervisor with default configuration.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start the supervisor with custom configuration.
%% Options:
%%   cluster_nodes - List of nodes to form the cluster with
%%   join_node - Existing node to join (alternative to cluster_nodes)
-spec start_link(map()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Config) when is_map(Config) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Config).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
init(Config) ->
    %% Set restart strategy
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },

    %% Child specifications
    Children = [
        %% Ra cluster starter - initializes the Ra cluster
        #{
            id => flurm_db_ra_starter,
            start => {flurm_db_ra_starter, start_link, [Config]},
            restart => transient,
            shutdown => 5000,
            type => worker,
            modules => [flurm_db_ra_starter]
        }
    ],

    {ok, {SupFlags, Children}}.
