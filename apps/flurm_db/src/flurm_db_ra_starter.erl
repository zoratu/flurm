%%%-------------------------------------------------------------------
%%% @doc FLURM Database Ra Starter
%%%
%%% A gen_server that initializes the Ra cluster on startup.
%%% This process handles the initialization logic and then
%%% terminates (transient restart).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_ra_starter).
-behaviour(gen_server).

-include("flurm_db.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    config :: map()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the Ra starter process.
-spec start_link(map()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init(Config) ->
    %% Start initialization asynchronously
    self() ! init_cluster,
    {ok, #state{config = Config}}.

%% @private
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(init_cluster, State) ->
    Config = State#state.config,

    %% Initialize Ra application
    case flurm_db_cluster:init_ra() of
        ok ->
            %% Determine cluster initialization mode
            case maps:get(join_node, Config, undefined) of
                undefined ->
                    %% Start a new cluster or join existing based on config
                    ClusterNodes = maps:get(cluster_nodes, Config, [node()]),
                    case init_or_join_cluster(ClusterNodes) of
                        ok ->
                            error_logger:info_msg(
                                "FLURM DB Ra cluster initialized~n", []),
                            {stop, normal, State};
                        {error, Reason} ->
                            error_logger:error_msg(
                                "Failed to initialize FLURM DB cluster: ~p~n",
                                [Reason]),
                            {stop, {error, Reason}, State}
                    end;
                JoinNode ->
                    %% Join an existing cluster
                    case flurm_db_cluster:join_cluster(JoinNode) of
                        ok ->
                            error_logger:info_msg(
                                "FLURM DB joined cluster via ~p~n",
                                [JoinNode]),
                            {stop, normal, State};
                        {error, Reason} ->
                            error_logger:error_msg(
                                "Failed to join cluster via ~p: ~p~n",
                                [JoinNode, Reason]),
                            {stop, {error, Reason}, State}
                    end
            end;
        {error, Reason} ->
            error_logger:error_msg("Failed to initialize Ra: ~p~n", [Reason]),
            {stop, {error, Reason}, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
%% Initialize or join the cluster based on the node list.
init_or_join_cluster([]) ->
    %% No nodes specified, start with just this node
    flurm_db_cluster:start_cluster([node()]);

init_or_join_cluster([Node]) when Node =:= node() ->
    %% Only this node, start a new cluster
    flurm_db_cluster:start_cluster([node()]);

init_or_join_cluster(Nodes) ->
    %% Multiple nodes specified
    case lists:member(node(), Nodes) of
        true ->
            %% We're in the cluster list, try to start or join
            %% First, check if any other node already has a cluster
            OtherNodes = Nodes -- [node()],
            case find_existing_cluster(OtherNodes) of
                {ok, ExistingNode} ->
                    %% Join the existing cluster
                    flurm_db_cluster:join_cluster(ExistingNode);
                not_found ->
                    %% No existing cluster, start a new one
                    flurm_db_cluster:start_cluster(Nodes)
            end;
        false ->
            %% We're not in the list, join the first available node
            case find_existing_cluster(Nodes) of
                {ok, ExistingNode} ->
                    flurm_db_cluster:join_cluster(ExistingNode);
                not_found ->
                    {error, no_cluster_available}
            end
    end.

%% @private
%% Find an existing cluster by checking each node.
find_existing_cluster([]) ->
    not_found;
find_existing_cluster([Node | Rest]) ->
    %% Try to ping the node and check if it has a Ra server
    case net_adm:ping(Node) of
        pong ->
            %% Node is reachable, check if Ra is running
            ServerId = flurm_db_cluster:server_id(Node),
            case catch ra:members(ServerId) of
                {ok, _Members, _Leader} ->
                    {ok, Node};
                _ ->
                    find_existing_cluster(Rest)
            end;
        pang ->
            find_existing_cluster(Rest)
    end.
