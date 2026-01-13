%%%-------------------------------------------------------------------
%%% @doc FLURM Node Connection Manager
%%%
%%% Tracks active TCP connections from node daemons and provides
%%% an interface for sending messages to specific nodes.
%%%
%%% This module manages the mapping between node hostnames and their
%%% connection handler processes.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_connection_manager).

-behaviour(gen_server).

-export([start_link/0]).
-export([
    register_connection/2,
    unregister_connection/1,
    get_connection/1,
    find_by_socket/1,
    send_to_node/2,
    send_to_nodes/2,
    list_connected_nodes/0,
    is_node_connected/1
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    connections = #{} :: #{binary() => pid()},
    pids_to_nodes = #{} :: #{pid() => binary()}
}).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Register a connection for a node hostname.
-spec register_connection(binary(), pid()) -> ok.
register_connection(Hostname, Pid) when is_binary(Hostname), is_pid(Pid) ->
    gen_server:call(?MODULE, {register, Hostname, Pid}).

%% @doc Unregister a connection for a node hostname.
-spec unregister_connection(binary()) -> ok.
unregister_connection(Hostname) when is_binary(Hostname) ->
    gen_server:cast(?MODULE, {unregister, Hostname}).

%% @doc Get the connection pid for a node hostname.
-spec get_connection(binary()) -> {ok, pid()} | {error, not_connected}.
get_connection(Hostname) when is_binary(Hostname) ->
    gen_server:call(?MODULE, {get_connection, Hostname}).

%% @doc Find hostname by socket (actually by pid).
-spec find_by_socket(term()) -> {ok, binary()} | error.
find_by_socket(Socket) ->
    gen_server:call(?MODULE, {find_by_socket, Socket}).

%% @doc Send a message to a specific node.
-spec send_to_node(binary(), map()) -> ok | {error, not_connected}.
send_to_node(Hostname, Message) when is_binary(Hostname), is_map(Message) ->
    gen_server:call(?MODULE, {send_to_node, Hostname, Message}).

%% @doc Send a message to multiple nodes.
-spec send_to_nodes([binary()], map()) -> [{binary(), ok | {error, term()}}].
send_to_nodes(Hostnames, Message) when is_list(Hostnames), is_map(Message) ->
    gen_server:call(?MODULE, {send_to_nodes, Hostnames, Message}).

%% @doc List all connected node hostnames.
-spec list_connected_nodes() -> [binary()].
list_connected_nodes() ->
    gen_server:call(?MODULE, list_connected_nodes).

%% @doc Check if a node is currently connected.
-spec is_node_connected(binary()) -> boolean().
is_node_connected(Hostname) when is_binary(Hostname) ->
    gen_server:call(?MODULE, {is_connected, Hostname}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    log(info, "Node Connection Manager started", []),
    {ok, #state{}}.

handle_call({register, Hostname, Pid}, _From, #state{connections = Conns, pids_to_nodes = P2N} = State) ->
    %% Monitor the connection process
    erlang:monitor(process, Pid),

    %% Unregister any existing connection for this hostname
    NewState = case maps:find(Hostname, Conns) of
        {ok, OldPid} ->
            log(warning, "Replacing existing connection for ~s", [Hostname]),
            State#state{
                connections = maps:put(Hostname, Pid, Conns),
                pids_to_nodes = maps:put(Pid, Hostname, maps:remove(OldPid, P2N))
            };
        error ->
            State#state{
                connections = maps:put(Hostname, Pid, Conns),
                pids_to_nodes = maps:put(Pid, Hostname, P2N)
            }
    end,
    log(info, "Node ~s connected (pid: ~p)", [Hostname, Pid]),
    {reply, ok, NewState};

handle_call({get_connection, Hostname}, _From, #state{connections = Conns} = State) ->
    case maps:find(Hostname, Conns) of
        {ok, Pid} -> {reply, {ok, Pid}, State};
        error -> {reply, {error, not_connected}, State}
    end;

handle_call({find_by_socket, _Socket}, {FromPid, _}, #state{pids_to_nodes = P2N} = State) ->
    %% The socket is actually tracked by the acceptor process
    %% Use the caller's pid to find the hostname
    case maps:find(FromPid, P2N) of
        {ok, Hostname} -> {reply, {ok, Hostname}, State};
        error -> {reply, error, State}
    end;

handle_call({send_to_node, Hostname, Message}, _From, #state{connections = Conns} = State) ->
    Result = case maps:find(Hostname, Conns) of
        {ok, Pid} ->
            Pid ! {send, Message},
            ok;
        error ->
            {error, not_connected}
    end,
    {reply, Result, State};

handle_call({send_to_nodes, Hostnames, Message}, _From, #state{connections = Conns} = State) ->
    Results = lists:map(fun(Hostname) ->
        case maps:find(Hostname, Conns) of
            {ok, Pid} ->
                Pid ! {send, Message},
                {Hostname, ok};
            error ->
                {Hostname, {error, not_connected}}
        end
    end, Hostnames),
    {reply, Results, State};

handle_call(list_connected_nodes, _From, #state{connections = Conns} = State) ->
    {reply, maps:keys(Conns), State};

handle_call({is_connected, Hostname}, _From, #state{connections = Conns} = State) ->
    {reply, maps:is_key(Hostname, Conns), State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({unregister, Hostname}, #state{connections = Conns, pids_to_nodes = P2N} = State) ->
    NewState = case maps:find(Hostname, Conns) of
        {ok, Pid} ->
            log(info, "Node ~s unregistered", [Hostname]),
            State#state{
                connections = maps:remove(Hostname, Conns),
                pids_to_nodes = maps:remove(Pid, P2N)
            };
        error ->
            State
    end,
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, Reason}, #state{connections = Conns, pids_to_nodes = P2N} = State) ->
    NewState = case maps:find(Pid, P2N) of
        {ok, Hostname} ->
            log(info, "Node ~s connection died: ~p", [Hostname, Reason]),
            %% Update node state in node manager
            flurm_node_manager_server:update_node(Hostname, #{state => down}),
            State#state{
                connections = maps:remove(Hostname, Conns),
                pids_to_nodes = maps:remove(Pid, P2N)
            };
        error ->
            State
    end,
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

log(Level, Fmt, Args) ->
    Msg = io_lib:format(Fmt, Args),
    case Level of
        debug -> ok;
        info -> error_logger:info_msg("[node_conn_mgr] ~s~n", [Msg]);
        warning -> error_logger:warning_msg("[node_conn_mgr] ~s~n", [Msg]);
        error -> error_logger:error_msg("[node_conn_mgr] ~s~n", [Msg])
    end.
