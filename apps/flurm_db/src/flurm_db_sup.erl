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
    %% Create ETS tables for fallback storage (before starting children)
    %% These are owned by the supervisor process and will persist
    %% even when flurm_db_ra_starter terminates
    init_ets_tables(),

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

%% @private
%% Initialize ETS tables for fallback storage.
%% Called from the supervisor so tables persist when ra_starter terminates.
init_ets_tables() ->
    Tables = [flurm_db_jobs_ets, flurm_db_nodes_ets, flurm_db_partitions_ets,
              flurm_db_job_counter_ets],
    lists:foreach(fun(Table) ->
        case ets:whereis(Table) of
            undefined ->
                ets:new(Table, [named_table, public, set, {read_concurrency, true}]);
            _ ->
                ok
        end
    end, Tables),
    %% Initialize job counter if not present
    case ets:lookup(flurm_db_job_counter_ets, counter) of
        [] -> ets:insert(flurm_db_job_counter_ets, {counter, 0});
        _ -> ok
    end,
    ok.
