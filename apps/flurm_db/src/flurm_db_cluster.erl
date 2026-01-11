%%%-------------------------------------------------------------------
%%% @doc FLURM Database Cluster Management
%%%
%%% Handles Ra cluster lifecycle operations:
%%% - Starting the cluster
%%% - Joining an existing cluster
%%% - Leaving the cluster gracefully
%%% - Getting leader/member information
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_db_cluster).

-include("flurm_db.hrl").

%% API
-export([
    start_cluster/0,
    start_cluster/1,
    join_cluster/1,
    leave_cluster/0,
    get_leader/0,
    get_members/0,
    is_leader/0,
    status/0,
    force_election/0
]).

%% Internal exports
-export([
    init_ra/0,
    server_id/0,
    server_id/1
]).

-define(CLUSTER_NAME, ?RA_CLUSTER_NAME).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a new Ra cluster with this node as the initial member.
%% This should be called on the first node to start the cluster.
-spec start_cluster() -> ok | {error, term()}.
start_cluster() ->
    start_cluster([node()]).

%% @doc Start a Ra cluster with the specified nodes.
%% All nodes in the list should be reachable.
-spec start_cluster([node()]) -> ok | {error, term()}.
start_cluster(Nodes) when is_list(Nodes) ->
    %% Ensure Ra application is started
    case init_ra() of
        ok ->
            %% Build server IDs for all nodes
            ServerIds = [server_id(N) || N <- Nodes],

            %% Machine configuration
            Machine = {module, flurm_db_ra, #{}},

            %% Start the cluster
            case ra:start_cluster(default, ?CLUSTER_NAME, Machine, ServerIds) of
                {ok, Started, []} ->
                    error_logger:info_msg("FLURM DB cluster started on nodes: ~p~n",
                                         [Started]),
                    ok;
                {ok, Started, Failed} ->
                    error_logger:warning_msg(
                        "FLURM DB cluster partially started. "
                        "Started: ~p, Failed: ~p~n",
                        [Started, Failed]),
                    ok;
                {error, Reason} ->
                    error_logger:error_msg("Failed to start FLURM DB cluster: ~p~n",
                                          [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Join an existing Ra cluster.
%% The node must be able to reach at least one existing cluster member.
-spec join_cluster(node()) -> ok | {error, term()}.
join_cluster(ExistingNode) when is_atom(ExistingNode) ->
    case init_ra() of
        ok ->
            NewServerId = server_id(),
            ExistingServerId = server_id(ExistingNode),

            %% First, start a local Ra server
            Machine = {module, flurm_db_ra, #{}},
            case ra:start_server(default, ?CLUSTER_NAME, NewServerId, Machine, []) of
                ok ->
                    %% Now add ourselves to the cluster through the existing member
                    case ra:add_member(ExistingServerId, NewServerId) of
                        {ok, _, _Leader} ->
                            error_logger:info_msg(
                                "FLURM DB: Joined cluster via ~p~n",
                                [ExistingNode]),
                            ok;
                        {error, already_member} ->
                            error_logger:info_msg(
                                "FLURM DB: Already a member of the cluster~n", []),
                            ok;
                        {timeout, _} ->
                            {error, timeout};
                        {error, Reason} ->
                            %% Failed to join, stop local server
                            catch ra:stop_server(default, NewServerId),
                            {error, Reason}
                    end;
                {error, {already_started, _}} ->
                    %% Server already running, try to add to cluster
                    case ra:add_member(ExistingServerId, NewServerId) of
                        {ok, _, _} -> ok;
                        {error, already_member} -> ok;
                        Error -> Error
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Leave the cluster gracefully.
%% This removes the local node from the Ra cluster.
-spec leave_cluster() -> ok | {error, term()}.
leave_cluster() ->
    ServerId = server_id(),
    case get_leader() of
        {ok, LeaderId} when LeaderId =/= ServerId ->
            %% Ask the leader to remove us
            case ra:remove_member(LeaderId, ServerId) of
                {ok, _, _} ->
                    %% Stop local Ra server
                    ra:stop_server(default, ServerId),
                    error_logger:info_msg("FLURM DB: Left the cluster~n", []),
                    ok;
                {timeout, _} ->
                    {error, timeout};
                {error, Reason} ->
                    {error, Reason}
            end;
        {ok, ServerId} ->
            %% We are the leader, need to transfer leadership first
            case get_members() of
                {ok, Members} when length(Members) > 1 ->
                    %% Find another member to transfer to
                    [OtherMember | _] = [M || M <- Members, M =/= ServerId],
                    case ra:transfer_leadership(ServerId, OtherMember) of
                        ok ->
                            %% Wait a bit for transfer to complete
                            timer:sleep(500),
                            leave_cluster();  %% Retry now that we're not leader
                        already_leader ->
                            {error, cannot_transfer_leadership};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {ok, [_]} ->
                    %% We're the only member, just stop
                    ra:stop_server(default, ServerId),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get the current leader of the cluster.
-spec get_leader() -> {ok, ra:server_id()} | {error, term()}.
get_leader() ->
    ServerId = server_id(),
    case ra:members(ServerId) of
        {ok, _, Leader} when Leader =/= undefined ->
            {ok, Leader};
        {ok, _, undefined} ->
            {error, no_leader};
        {timeout, _} ->
            {error, timeout};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get all members of the cluster.
-spec get_members() -> {ok, [ra:server_id()]} | {error, term()}.
get_members() ->
    ServerId = server_id(),
    case ra:members(ServerId) of
        {ok, Members, _Leader} ->
            {ok, Members};
        {timeout, _} ->
            {error, timeout};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Check if this node is the current leader.
-spec is_leader() -> boolean().
is_leader() ->
    ServerId = server_id(),
    case get_leader() of
        {ok, ServerId} -> true;
        _ -> false
    end.

%% @doc Get the status of the local Ra server.
-spec status() -> {ok, map()} | {error, term()}.
status() ->
    ServerId = server_id(),
    try
        case ra:member_overview(ServerId) of
            {ok, Overview, _} ->
                {ok, format_status(Overview)};
            {error, Reason} ->
                {error, Reason}
        end
    catch
        _:_ ->
            {error, not_started}
    end.

%% @doc Force a new election (for testing/recovery).
-spec force_election() -> ok | {error, term()}.
force_election() ->
    ServerId = server_id(),
    case ra:trigger_election(ServerId) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Initialize the Ra application and data directory.
-spec init_ra() -> ok | {error, term()}.
init_ra() ->
    %% Determine data directory - use configured or fallback to temp for dev
    ConfiguredDir = application:get_env(flurm_db, data_dir, ?DEFAULT_DATA_DIR),
    DataDir = case filelib:ensure_dir(ConfiguredDir ++ "/") of
        ok -> ConfiguredDir;
        {error, _} ->
            %% Fall back to temp directory for development
            TempDir = filename:join(["/tmp", "flurm_db"]),
            case filelib:ensure_dir(TempDir ++ "/") of
                ok ->
                    lager:warning("Using fallback data directory: ~s (configured: ~s)",
                                  [TempDir, ConfiguredDir]),
                    TempDir;
                {error, Reason} ->
                    lager:error("Cannot create data directory ~s or ~s: ~p",
                                [ConfiguredDir, TempDir, Reason]),
                    error({data_dir_error, Reason})
            end
    end,

    %% Set Ra data dir if not already set
    case application:get_env(ra, data_dir) of
        undefined ->
            application:set_env(ra, data_dir, DataDir);
        _ ->
            ok
    end,

    %% Ensure Ra application is started
    case application:ensure_all_started(ra) of
        {ok, _} ->
            %% Also start the default Ra system (required for Ra 2.x)
            start_default_system();
        {error, {already_started, ra}} ->
            %% Ra already started, ensure default system is running
            start_default_system();
        {error, Reason2} ->
            {error, Reason2}
    end.

%% @private
%% Start the default Ra system if not already running.
start_default_system() ->
    case ra_system:fetch(default) of
        undefined ->
            %% Default system not started, start it
            case ra_system:start_default() of
                {ok, _Pid} ->
                    lager:info("[flurm_db] Ra default system started"),
                    ok;
                {error, {already_started, _}} ->
                    ok;
                {error, Reason} ->
                    lager:error("[flurm_db] Failed to start Ra default system: ~p", [Reason]),
                    {error, Reason}
            end;
        _Config ->
            %% Default system already running
            ok
    end.

%% @doc Get the server ID for the local node.
-spec server_id() -> ra:server_id().
server_id() ->
    server_id(node()).

%% @doc Get the server ID for a specific node.
-spec server_id(node()) -> ra:server_id().
server_id(Node) ->
    {?CLUSTER_NAME, Node}.

%% @doc Format the Ra overview into a more readable map.
format_status(Overview) when is_map(Overview) ->
    #{
        state => maps:get(state, Overview, unknown),
        leader => maps:get(leader, Overview, undefined),
        current_term => maps:get(current_term, Overview, 0),
        commit_index => maps:get(commit_index, Overview, 0),
        last_applied => maps:get(last_applied, Overview, 0),
        cluster_members => maps:get(cluster, Overview, [])
    };
format_status(Overview) when is_list(Overview) ->
    %% Handle proplist format
    #{
        state => proplists:get_value(state, Overview, unknown),
        leader => proplists:get_value(leader, Overview, undefined),
        current_term => proplists:get_value(current_term, Overview, 0)
    }.
