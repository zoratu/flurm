%%%-------------------------------------------------------------------
%%% @doc FLURM Database Application
%%%
%%% Application module for the FLURM distributed database.
%%% Starts the supervision tree which initializes the Ra cluster.
%%%
%%% Configuration (in sys.config):
%%% ```
%%% {flurm_db, [
%%%     {data_dir, "/var/lib/flurm/db"},
%%%     {cluster_name, flurm_cluster},
%%%     %% Either start a new cluster with these nodes:
%%%     {cluster_nodes, ['flurm@node1', 'flurm@node2', 'flurm@node3']},
%%%     %% Or join an existing cluster:
%%%     {join_node, 'flurm@node1'}
%%% ]}
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_app).
-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% Application callbacks
%%====================================================================

%% @doc Start the application.
-spec start(term(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    %% Build configuration from application environment
    Config = build_config(),

    %% Start the supervisor tree
    flurm_db_sup:start_link(Config).

%% @doc Stop the application.
-spec stop(term()) -> ok.
stop(_State) ->
    %% Gracefully leave the cluster if we're a member
    case flurm_db_cluster:status() of
        {ok, _} ->
            error_logger:info_msg("FLURM DB: Leaving cluster...~n", []),
            catch flurm_db_cluster:leave_cluster();
        {error, _} ->
            ok
    end,
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
build_config() ->
    Config0 = #{},

    %% Add cluster_nodes if configured
    Config1 = case application:get_env(flurm_db, cluster_nodes) of
        {ok, Nodes} when is_list(Nodes) ->
            Config0#{cluster_nodes => Nodes};
        _ ->
            Config0
    end,

    %% Add join_node if configured
    Config2 = case application:get_env(flurm_db, join_node) of
        {ok, Node} when is_atom(Node) ->
            Config1#{join_node => Node};
        _ ->
            Config1
    end,

    Config2.
