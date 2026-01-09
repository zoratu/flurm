%%%-------------------------------------------------------------------
%%% @doc FLURM Node Manager
%%%
%%% Tracks compute node status, handles node registration, heartbeats,
%%% and maintains the cluster node inventory.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_manager).

-behaviour(gen_server).

-export([start_link/0]).
-export([register_node/1, update_node/2, get_node/1, list_nodes/0]).
-export([heartbeat/1, get_available_nodes/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("flurm_core/include/flurm_core.hrl").

-define(HEARTBEAT_TIMEOUT, 30000). % 30 seconds

-record(state, {
    nodes = #{} :: #{binary() => #node{}}
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register_node(map()) -> ok | {error, term()}.
register_node(NodeSpec) ->
    gen_server:call(?MODULE, {register_node, NodeSpec}).

-spec update_node(binary(), map()) -> ok | {error, term()}.
update_node(Hostname, Updates) ->
    gen_server:call(?MODULE, {update_node, Hostname, Updates}).

-spec get_node(binary()) -> {ok, #node{}} | {error, not_found}.
get_node(Hostname) ->
    gen_server:call(?MODULE, {get_node, Hostname}).

-spec list_nodes() -> [#node{}].
list_nodes() ->
    gen_server:call(?MODULE, list_nodes).

-spec heartbeat(map()) -> ok.
heartbeat(HeartbeatData) ->
    gen_server:cast(?MODULE, {heartbeat, HeartbeatData}).

-spec get_available_nodes() -> [#node{}].
get_available_nodes() ->
    gen_server:call(?MODULE, get_available_nodes).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("Node Manager started"),
    %% Start heartbeat checker
    erlang:send_after(?HEARTBEAT_TIMEOUT, self(), check_heartbeats),
    {ok, #state{}}.

handle_call({register_node, NodeSpec}, _From, #state{nodes = Nodes} = State) ->
    Node = flurm_core:new_node(NodeSpec),
    Hostname = flurm_core:node_hostname(Node),
    UpdatedNode = flurm_core:update_node_state(Node, idle),
    UpdatedNode2 = UpdatedNode#node{last_heartbeat = erlang:system_time(second)},
    lager:info("Node ~s registered", [Hostname]),
    NewNodes = maps:put(Hostname, UpdatedNode2, Nodes),
    {reply, ok, State#state{nodes = NewNodes}};

handle_call({update_node, Hostname, Updates}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            UpdatedNode = apply_node_updates(Node, Updates),
            NewNodes = maps:put(Hostname, UpdatedNode, Nodes),
            {reply, ok, State#state{nodes = NewNodes}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_node, Hostname}, _From, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            {reply, {ok, Node}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(list_nodes, _From, #state{nodes = Nodes} = State) ->
    {reply, maps:values(Nodes), State};

handle_call(get_available_nodes, _From, #state{nodes = Nodes} = State) ->
    Available = [N || N <- maps:values(Nodes),
                      flurm_core:node_state(N) =:= idle orelse
                      flurm_core:node_state(N) =:= mixed],
    {reply, Available, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({heartbeat, #{hostname := Hostname} = Data}, #state{nodes = Nodes} = State) ->
    case maps:find(Hostname, Nodes) of
        {ok, Node} ->
            UpdatedNode = Node#node{
                last_heartbeat = erlang:system_time(second),
                load_avg = maps:get(load_avg, Data, Node#node.load_avg),
                free_memory_mb = maps:get(free_memory_mb, Data, Node#node.free_memory_mb),
                running_jobs = maps:get(running_jobs, Data, Node#node.running_jobs)
            },
            NewNodes = maps:put(Hostname, UpdatedNode, Nodes),
            {noreply, State#state{nodes = NewNodes}};
        error ->
            lager:warning("Heartbeat from unknown node: ~s", [Hostname]),
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_heartbeats, #state{nodes = Nodes} = State) ->
    Now = erlang:system_time(second),
    Timeout = ?HEARTBEAT_TIMEOUT div 1000,
    NewNodes = maps:map(fun(_Hostname, Node) ->
        case Node#node.last_heartbeat of
            undefined ->
                Node;
            LastHB when Now - LastHB > Timeout * 2 ->
                lager:warning("Node ~s marked as down (no heartbeat)", [Node#node.hostname]),
                flurm_core:update_node_state(Node, down);
            _ ->
                Node
        end
    end, Nodes),
    erlang:send_after(?HEARTBEAT_TIMEOUT, self(), check_heartbeats),
    {noreply, State#state{nodes = NewNodes}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

apply_node_updates(Node, Updates) ->
    maps:fold(fun
        (state, Value, N) -> flurm_core:update_node_state(N, Value);
        (load_avg, Value, N) -> N#node{load_avg = Value};
        (free_memory_mb, Value, N) -> N#node{free_memory_mb = Value};
        (running_jobs, Value, N) -> N#node{running_jobs = Value};
        (_, _, N) -> N
    end, Node, Updates).
