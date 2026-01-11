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

    %% Check if we're running in distributed mode
    case node() of
        nonode@nohost ->
            %% Non-distributed mode - use ETS fallback for persistence
            %% ETS tables are already initialized by flurm_db_sup
            lager:warning("[flurm_db] Running in non-distributed mode (nonode@nohost). "
                          "Ra cluster disabled. Using ETS fallback for persistence."),
            lager:info("[flurm_db] ETS fallback tables ready (owned by supervisor)"),
            {stop, normal, State};
        _ ->
            %% Named node - initialize Ra cluster (works even for single node)
            lager:info("[flurm_db] Node ~p starting Ra cluster", [node()]),
            init_ra_cluster(Config, State)
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
%% Initialize the Ra cluster when in distributed mode
%% On failure, gracefully fall back to ETS (don't crash supervisor)
init_ra_cluster(Config, State) ->
    case flurm_db_cluster:init_ra() of
        ok ->
            %% Determine cluster initialization mode
            case maps:get(join_node, Config, undefined) of
                undefined ->
                    %% Start a new cluster or join existing based on config
                    ClusterNodes = maps:get(cluster_nodes, Config, [node()]),
                    case init_or_join_cluster(ClusterNodes) of
                        ok ->
                            lager:info("[flurm_db] Ra cluster initialized successfully"),
                            {stop, normal, State};
                        {error, Reason} ->
                            lager:warning("[flurm_db] Ra cluster failed (~p), using ETS fallback",
                                         [Reason]),
                            %% Stop normally so supervisor doesn't restart us
                            %% ETS tables already exist from supervisor init
                            {stop, normal, State}
                    end;
                JoinNode ->
                    %% Join an existing cluster
                    case flurm_db_cluster:join_cluster(JoinNode) of
                        ok ->
                            lager:info("[flurm_db] Joined Ra cluster via ~p", [JoinNode]),
                            {stop, normal, State};
                        {error, Reason} ->
                            lager:warning("[flurm_db] Failed to join cluster via ~p (~p), using ETS fallback",
                                         [JoinNode, Reason]),
                            {stop, normal, State}
                    end
            end;
        {error, Reason} ->
            lager:warning("[flurm_db] Ra initialization failed (~p), using ETS fallback", [Reason]),
            {stop, normal, State}
    end.

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
