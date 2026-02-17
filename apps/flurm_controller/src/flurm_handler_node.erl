%%%-------------------------------------------------------------------
%%% @doc FLURM Node Handler
%%%
%%% Handles node-related operations including:
%%% - Node info queries
%%% - Node registration status
%%% - Controller reconfiguration (full and partial)
%%% - Node configuration changes
%%% - Partition configuration changes
%%%
%%% Split from flurm_controller_handler.erl for maintainability.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_handler_node).

-export([handle/2]).

%% Exported for use by main handler
-export([
    node_to_node_info/1,
    node_state_to_slurm/1,
    do_reconfigure/0,
    do_partial_reconfigure/4,
    apply_node_changes/0,
    apply_partition_changes/0,
    is_cluster_enabled/0
]).

-include_lib("flurm_protocol/include/flurm_protocol.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% Import helpers from main handler
-import(flurm_controller_handler, [
    format_features/1,
    format_partitions/1
]).

%%====================================================================
%% API
%%====================================================================

%% REQUEST_NODE_INFO (2007) -> RESPONSE_NODE_INFO
handle(#slurm_header{msg_type = ?REQUEST_NODE_INFO}, _Body) ->
    lager:debug("Handling node info request"),
    Nodes = flurm_node_manager_server:list_nodes(),
    lager:info("DEBUG: Found ~p nodes", [length(Nodes)]),
    NodeInfoList = [node_to_node_info(N) || N <- Nodes],
    Response = #node_info_response{
        last_update = erlang:system_time(second),
        node_count = length(NodeInfoList),
        nodes = NodeInfoList
    },
    {ok, ?RESPONSE_NODE_INFO, Response};

%% REQUEST_NODE_REGISTRATION_STATUS (1001) -> MESSAGE_NODE_REGISTRATION_STATUS
handle(#slurm_header{msg_type = ?REQUEST_NODE_REGISTRATION_STATUS},
       #node_registration_request{}) ->
    lager:debug("Handling node registration status request"),
    %% For now, return a simple acknowledgment
    Response = #slurm_rc_response{return_code = 0},
    {ok, ?RESPONSE_SLURM_RC, Response};

%% REQUEST_RECONFIGURE (1003) -> RESPONSE_SLURM_RC
%% Hot-reload configuration from slurm.conf (full reconfigure)
handle(#slurm_header{msg_type = ?REQUEST_RECONFIGURE}, Body) ->
    lager:info("Handling full reconfigure request (REQUEST_RECONFIGURE 1003)"),
    StartTime = erlang:system_time(millisecond),

    %% Parse the request body if it's a record
    _Flags = case Body of
        #reconfigure_request{flags = F} -> F;
        _ -> 0
    end,

    Result = case is_cluster_enabled() of
        true ->
            %% In cluster mode, only leader processes reconfigure
            case flurm_controller_cluster:is_leader() of
                true ->
                    lager:info("Processing reconfigure as cluster leader"),
                    do_reconfigure();
                false ->
                    %% Forward to leader
                    lager:info("Forwarding reconfigure request to cluster leader"),
                    case flurm_controller_cluster:forward_to_leader(reconfigure, []) of
                        {ok, ReconfigResult} -> ReconfigResult;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            do_reconfigure()
    end,

    ElapsedMs = erlang:system_time(millisecond) - StartTime,

    case Result of
        ok ->
            lager:info("Full reconfiguration completed successfully in ~p ms", [ElapsedMs]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {ok, ChangedKeys} when is_list(ChangedKeys) ->
            lager:info("Full reconfiguration completed successfully in ~p ms, ~p keys changed: ~p",
                      [ElapsedMs, length(ChangedKeys), ChangedKeys]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Full reconfiguration failed after ~p ms: ~p", [ElapsedMs, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end;

%% REQUEST_RECONFIGURE_WITH_CONFIG (1004) -> RESPONSE_SLURM_RC
%% Partial reconfiguration with specific settings
handle(#slurm_header{msg_type = ?REQUEST_RECONFIGURE_WITH_CONFIG}, Body) ->
    lager:info("Handling partial reconfigure request (REQUEST_RECONFIGURE_WITH_CONFIG 1004)"),
    StartTime = erlang:system_time(millisecond),

    %% Parse the request
    Request = case Body of
        #reconfigure_with_config_request{} = Req -> Req;
        _ -> #reconfigure_with_config_request{}
    end,

    ConfigFile = Request#reconfigure_with_config_request.config_file,
    Settings = Request#reconfigure_with_config_request.settings,
    Force = Request#reconfigure_with_config_request.force,
    NotifyNodes = Request#reconfigure_with_config_request.notify_nodes,

    lager:info("Partial reconfigure: config_file=~s, settings_count=~p, force=~p, notify_nodes=~p",
               [ConfigFile, maps:size(Settings), Force, NotifyNodes]),

    Result = case is_cluster_enabled() of
        true ->
            case flurm_controller_cluster:is_leader() of
                true ->
                    lager:info("Processing partial reconfigure as cluster leader"),
                    do_partial_reconfigure(ConfigFile, Settings, Force, NotifyNodes);
                false ->
                    lager:info("Forwarding partial reconfigure request to cluster leader"),
                    case flurm_controller_cluster:forward_to_leader(
                           partial_reconfigure,
                           {ConfigFile, Settings, Force, NotifyNodes}) of
                        {ok, ReconfigResult} -> ReconfigResult;
                        {error, no_leader} -> {error, no_leader};
                        {error, Reason} -> {error, Reason}
                    end
            end;
        false ->
            do_partial_reconfigure(ConfigFile, Settings, Force, NotifyNodes)
    end,

    ElapsedMs = erlang:system_time(millisecond) - StartTime,

    case Result of
        ok ->
            lager:info("Partial reconfiguration completed successfully in ~p ms", [ElapsedMs]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {ok, ChangedKeys} when is_list(ChangedKeys) ->
            lager:info("Partial reconfiguration completed in ~p ms, ~p keys changed: ~p",
                      [ElapsedMs, length(ChangedKeys), ChangedKeys]),
            Response = #slurm_rc_response{return_code = 0},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, {validation_failed, Details}} ->
            lager:warning("Partial reconfiguration validation failed after ~p ms: ~p",
                         [ElapsedMs, Details]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response};
        {error, Reason2} ->
            lager:warning("Partial reconfiguration failed after ~p ms: ~p", [ElapsedMs, Reason2]),
            Response = #slurm_rc_response{return_code = -1},
            {ok, ?RESPONSE_SLURM_RC, Response}
    end.

%%====================================================================
%% Internal Functions - Response Conversion
%%====================================================================

%% @doc Convert internal node record to SLURM node_info record
-spec node_to_node_info(#node{}) -> #node_info{}.
node_to_node_info(#node{} = Node) ->
    %% Calculate allocated CPUs and memory from the allocations map
    Allocs = case Node#node.allocations of
        M when is_map(M) -> M;
        _ -> #{}
    end,
    {AllocCpus, AllocMem} = maps:fold(
        fun(_JobId, {Cpus, Mem}, {AccC, AccM}) -> {AccC + Cpus, AccM + Mem} end,
        {0, 0}, Allocs),
    #node_info{
        name = Node#node.hostname,
        node_hostname = Node#node.hostname,
        node_addr = Node#node.hostname,
        port = 6818,  % Default slurmd port
        node_state = node_state_to_slurm(Node#node.state),
        cpus = Node#node.cpus,
        real_memory = Node#node.memory_mb,
        free_mem = max(0, Node#node.memory_mb - AllocMem),
        alloc_cpus = AllocCpus,
        alloc_memory = AllocMem,
        cpu_load = trunc(Node#node.load_avg * 100),
        features = format_features(Node#node.features),
        partitions = format_partitions(Node#node.partitions),
        version = <<"22.05.0">>,
        arch = <<"x86_64">>,
        os = <<"Linux">>
    }.

%% @doc Convert internal node state to SLURM node state integer
-spec node_state_to_slurm(node_state()) -> non_neg_integer().
node_state_to_slurm(up) -> ?NODE_STATE_IDLE;
node_state_to_slurm(down) -> ?NODE_STATE_DOWN;
node_state_to_slurm(drain) -> ?NODE_STATE_DOWN;
node_state_to_slurm(idle) -> ?NODE_STATE_IDLE;
node_state_to_slurm(allocated) -> ?NODE_STATE_ALLOCATED;
node_state_to_slurm(mixed) -> ?NODE_STATE_MIXED;
node_state_to_slurm(_) -> ?NODE_STATE_UNKNOWN.

%%====================================================================
%% Internal Functions - Cluster Helpers
%%====================================================================

%% @doc Check if cluster mode is enabled.
%% Cluster mode is enabled when there are multiple nodes configured.
-spec is_cluster_enabled() -> boolean().
is_cluster_enabled() ->
    case application:get_env(flurm_controller, cluster_nodes) of
        {ok, Nodes} when is_list(Nodes), length(Nodes) > 1 ->
            true;
        _ ->
            %% Also check if the cluster process is running
            case whereis(flurm_controller_cluster) of
                undefined -> false;
                _Pid -> true
            end
    end.

%%====================================================================
%% Internal Functions - Reconfiguration
%%====================================================================

%% @doc Perform hot reconfiguration by reloading slurm.conf
-spec do_reconfigure() -> ok | {ok, [atom()]} | {error, term()}.
do_reconfigure() ->
    lager:info("Starting full hot reconfiguration"),

    %% Get current config version before reload
    OldVersion = try flurm_config_server:get_version() catch _:_ -> 0 end,

    %% Step 1: Reload configuration from file
    case flurm_config_server:reconfigure() of
        ok ->
            NewVersion = try flurm_config_server:get_version() catch _:_ -> 0 end,
            lager:info("Configuration reloaded from file (version ~p -> ~p)",
                      [OldVersion, NewVersion]),

            %% Step 2: Apply node changes
            lager:debug("Applying node configuration changes"),
            apply_node_changes(),

            %% Step 3: Apply partition changes
            lager:debug("Applying partition configuration changes"),
            apply_partition_changes(),

            %% Step 4: Notify scheduler to refresh
            lager:debug("Triggering scheduler refresh"),
            notify_scheduler_reconfigure(),

            %% Step 5: Broadcast to compute nodes if needed
            broadcast_reconfigure_to_nodes(),

            lager:info("Full reconfiguration complete"),
            ok;
        {error, Reason} ->
            lager:error("Failed to reload configuration: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Perform partial reconfiguration with specific settings
%% This allows updating specific configuration values without a full reload.
-spec do_partial_reconfigure(binary(), map(), boolean(), boolean()) ->
    ok | {ok, [atom()]} | {error, term()}.
do_partial_reconfigure(ConfigFile, Settings, Force, NotifyNodes) ->
    lager:info("Starting partial reconfiguration"),

    %% Get current config version
    OldVersion = try flurm_config_server:get_version() catch _:_ -> 0 end,

    %% Step 1: If a config file is specified, reload from that file first
    FileResult = case ConfigFile of
        <<>> ->
            ok;
        _ ->
            lager:info("Reloading configuration from specified file: ~s", [ConfigFile]),
            case flurm_config_server:reconfigure(binary_to_list(ConfigFile)) of
                ok -> ok;
                {error, R} when Force ->
                    lager:warning("Config file reload failed but force=true, continuing: ~p", [R]),
                    ok;
                {error, R} ->
                    {error, {file_reload_failed, R}}
            end
    end,

    case FileResult of
        ok ->
            %% Step 2: Apply specific settings
            ChangedKeys = apply_settings(Settings, Force),
            lager:info("Applied ~p configuration settings: ~p",
                      [length(ChangedKeys), ChangedKeys]),

            %% Step 3: Apply node and partition changes based on what was changed
            case lists:member(nodes, ChangedKeys) of
                true ->
                    lager:debug("Nodes configuration changed, applying updates"),
                    apply_node_changes();
                false ->
                    ok
            end,

            case lists:member(partitions, ChangedKeys) of
                true ->
                    lager:debug("Partitions configuration changed, applying updates"),
                    apply_partition_changes();
                false ->
                    ok
            end,

            %% Step 4: Notify scheduler if scheduling-related settings changed
            SchedulerKeys = [schedulertype, schedulerparameters, prioritytype,
                            priorityweightage, priorityweightfairshare,
                            priorityweightjobsize, priorityweightpartition,
                            priorityweightqos],
            case lists:any(fun(K) -> lists:member(K, ChangedKeys) end, SchedulerKeys) of
                true ->
                    lager:info("Scheduler-related settings changed, refreshing scheduler"),
                    notify_scheduler_reconfigure();
                false ->
                    ok
            end,

            %% Step 5: Notify compute nodes if requested
            case NotifyNodes of
                true ->
                    broadcast_reconfigure_to_nodes();
                false ->
                    lager:debug("Skipping node notification (notify_nodes=false)")
            end,

            NewVersion = try flurm_config_server:get_version() catch _:_ -> 0 end,
            lager:info("Partial reconfiguration complete (version ~p -> ~p)",
                      [OldVersion, NewVersion]),
            {ok, ChangedKeys};
        {error, _} = Error ->
            Error
    end.

%% @doc Apply specific settings to the configuration
-spec apply_settings(map(), boolean()) -> [atom()].
apply_settings(Settings, _Force) when map_size(Settings) == 0 ->
    [];
apply_settings(Settings, Force) ->
    ChangedKeys = maps:fold(fun(Key, Value, Acc) ->
        try
            %% Get current value to check if it's actually changing
            CurrentValue = flurm_config_server:get(Key, undefined),
            case CurrentValue =:= Value of
                true ->
                    lager:debug("Setting ~p unchanged (value=~p)", [Key, Value]),
                    Acc;
                false ->
                    lager:info("Updating setting ~p: ~p -> ~p", [Key, CurrentValue, Value]),
                    flurm_config_server:set(Key, Value),
                    [Key | Acc]
            end
        catch
            Error:Reason ->
                case Force of
                    true ->
                        lager:warning("Failed to set ~p=~p, but force=true: ~p:~p",
                                     [Key, Value, Error, Reason]),
                        Acc;
                    false ->
                        lager:error("Failed to set ~p=~p: ~p:~p",
                                   [Key, Value, Error, Reason]),
                        Acc
                end
        end
    end, [], Settings),
    lists:reverse(ChangedKeys).

%% @doc Notify the scheduler of configuration changes
-spec notify_scheduler_reconfigure() -> ok.
notify_scheduler_reconfigure() ->
    try
        flurm_scheduler:trigger_schedule(),
        lager:debug("Scheduler notified of reconfiguration")
    catch
        _:_ ->
            lager:debug("Could not notify scheduler (may not be running)")
    end,
    ok.

%% @doc Broadcast reconfiguration message to all compute nodes
-spec broadcast_reconfigure_to_nodes() -> ok.
broadcast_reconfigure_to_nodes() ->
    try
        Nodes = flurm_node_manager_server:list_nodes(),
        lager:info("Broadcasting reconfigure to ~p compute nodes", [length(Nodes)]),
        lists:foreach(fun(Node) ->
            NodeName = Node#node.hostname,
            try
                %% Send reconfigure notification to the node
                %% The node daemon will reload its local configuration
                case flurm_node_manager_server:send_command(NodeName, reconfigure) of
                    ok ->
                        lager:debug("Reconfigure sent to node ~s", [NodeName]);
                    {error, Reason} ->
                        lager:warning("Failed to send reconfigure to node ~s: ~p",
                                     [NodeName, Reason])
                end
            catch
                _:NodeError ->
                    lager:warning("Error notifying node ~s: ~p", [NodeName, NodeError])
            end
        end, Nodes)
    catch
        _:_ ->
            lager:debug("Could not broadcast to nodes (node manager may not be running)")
    end,
    ok.

%% @doc Apply node configuration changes
-spec apply_node_changes() -> ok.
apply_node_changes() ->
    %% Get nodes from config
    ConfigNodes = flurm_config_server:get_nodes(),
    lager:debug("Config has ~p node definitions", [length(ConfigNodes)]),

    %% For each node definition, ensure it exists in the node manager
    lists:foreach(fun(NodeDef) ->
        case maps:get(nodename, NodeDef, undefined) of
            undefined -> ok;
            NodePattern ->
                %% Expand hostlist pattern
                ExpandedNodes = flurm_config_slurm:expand_hostlist(NodePattern),
                lists:foreach(fun(NodeName) ->
                    ensure_node_exists(NodeName, NodeDef)
                end, ExpandedNodes)
        end
    end, ConfigNodes),
    ok.

%% @doc Ensure a node exists in the node manager (for pre-configured nodes)
-spec ensure_node_exists(binary(), map()) -> ok.
ensure_node_exists(NodeName, NodeDef) ->
    case flurm_node_manager_server:get_node(NodeName) of
        {ok, _Node} ->
            %% Node exists, could update if needed
            ok;
        {error, not_found} ->
            %% Node not registered yet, could pre-register or just log
            lager:debug("Node ~s defined in config but not registered", [NodeName]),
            ok
    end,
    %% Extract CPUs and memory from config if available
    _Cpus = maps:get(cpus, NodeDef, maps:get(cpu, NodeDef, 1)),
    _Memory = maps:get(realmemory, NodeDef, maps:get(memory_mb, NodeDef, 1024)),
    ok.

%% @doc Apply partition configuration changes
-spec apply_partition_changes() -> ok.
apply_partition_changes() ->
    %% Get partitions from config
    ConfigPartitions = flurm_config_server:get_partitions(),
    lager:debug("Config has ~p partition definitions", [length(ConfigPartitions)]),

    %% For each partition, ensure it exists
    lists:foreach(fun(PartDef) ->
        case maps:get(partitionname, PartDef, undefined) of
            undefined -> ok;
            PartName ->
                ensure_partition_exists(PartName, PartDef)
        end
    end, ConfigPartitions),
    ok.

%% @doc Ensure a partition exists in the partition manager
-spec ensure_partition_exists(binary(), map()) -> ok.
ensure_partition_exists(PartName, PartDef) ->
    case flurm_partition_manager:get_partition(PartName) of
        {ok, _Part} ->
            %% Partition exists, update if needed
            update_partition(PartName, PartDef);
        {error, not_found} ->
            %% Create partition
            create_partition(PartName, PartDef)
    end.

%% @doc Create a new partition from config
-spec create_partition(binary(), map()) -> ok.
create_partition(PartName, PartDef) ->
    %% Extract nodes from definition
    NodesPattern = maps:get(nodes, PartDef, <<>>),
    Nodes = case NodesPattern of
        <<>> -> [];
        _ -> flurm_config_slurm:expand_hostlist(NodesPattern)
    end,

    PartSpec = #{
        name => PartName,
        nodes => Nodes,
        state => case maps:get(state, PartDef, up) of
            <<"UP">> -> up;
            <<"DOWN">> -> down;
            Other when is_atom(Other) -> Other;
            _ -> up
        end,
        default => maps:get(default, PartDef, false) =:= true,
        max_time => maps:get(maxtime, PartDef, infinity),
        priority => maps:get(prioritytier, PartDef, 1)
    },

    case flurm_partition_manager:create_partition(PartSpec) of
        ok ->
            lager:info("Created partition ~s with ~p nodes", [PartName, length(Nodes)]);
        {error, Reason} ->
            lager:warning("Failed to create partition ~s: ~p", [PartName, Reason])
    end,
    ok.

%% @doc Update an existing partition from config
-spec update_partition(binary(), map()) -> ok.
update_partition(PartName, PartDef) ->
    %% Update partition state if changed
    %% Note: set_state may not be implemented yet
    NewState = case maps:get(state, PartDef, undefined) of
        undefined -> undefined;
        <<"UP">> -> up;
        <<"DOWN">> -> down;
        up -> up;
        down -> down;
        _ -> undefined
    end,
    case NewState of
        undefined ->
            ok;
        State ->
            %% Try to update state if the function exists
            try
                flurm_partition_manager:update_partition(PartName, #{state => State})
            catch
                error:undef ->
                    lager:debug("Partition state update not supported yet for ~s", [PartName]);
                _:_ ->
                    ok
            end
    end,
    ok.
