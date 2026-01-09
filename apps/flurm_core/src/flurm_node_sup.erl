%%%-------------------------------------------------------------------
%%% @doc FLURM Node Supervisor
%%%
%%% A simple_one_for_one supervisor for dynamically spawning node
%%% processes. Each compute node runs as a separate gen_server process
%%% under this supervisor.
%%%
%%% The supervisor uses a temporary restart strategy - if a node
%%% process crashes, it is not automatically restarted. Node daemons
%%% should re-register with the controller if their representation
%%% process dies.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_node_sup).
-behaviour(supervisor).

-include("flurm_core.hrl").

%% API
-export([
    start_link/0,
    start_node/1,
    stop_node/1,
    which_nodes/0,
    count_nodes/0
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the node supervisor.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a new node process with the given node specification.
-spec start_node(#node_spec{}) -> {ok, pid()} | {error, term()}.
start_node(#node_spec{} = NodeSpec) ->
    supervisor:start_child(?SERVER, [NodeSpec]).

%% @doc Stop a node process.
-spec stop_node(pid()) -> ok | {error, term()}.
stop_node(Pid) when is_pid(Pid) ->
    supervisor:terminate_child(?SERVER, Pid).

%% @doc Get a list of all running node processes.
-spec which_nodes() -> [pid()].
which_nodes() ->
    [Pid || {_, Pid, _, _} <- supervisor:which_children(?SERVER),
            is_pid(Pid)].

%% @doc Count the number of running node processes.
-spec count_nodes() -> non_neg_integer().
count_nodes() ->
    length(which_nodes()).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 0,    %% Don't restart nodes automatically
        period => 1
    },
    ChildSpec = #{
        id => flurm_node,
        start => {flurm_node, start_link, []},
        restart => temporary,   %% Nodes are not restarted if they crash
        shutdown => 5000,       %% Give nodes 5 seconds to clean up
        type => worker,
        modules => [flurm_node]
    },
    {ok, {SupFlags, [ChildSpec]}}.
