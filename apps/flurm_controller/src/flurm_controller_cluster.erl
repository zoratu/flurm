%%%-------------------------------------------------------------------
%%% @doc FLURM Controller Cluster Coordination
%%%
%%% Manages cluster membership and leader election using Ra (Raft).
%%% Provides APIs for checking leadership status and forwarding
%%% requests to the current leader.
%%%
%%% The cluster uses Ra for consensus, ensuring:
%%% - Automatic leader election
%%% - State replication across nodes
%%% - Automatic failover when leader fails
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_controller_cluster).

-behaviour(gen_server).

%% API
-export([start_link/0,
         is_leader/0,
         get_leader/0,
         forward_to_leader/2,
         cluster_status/0,
         join_cluster/1,
         leave_cluster/0,
         get_members/0]).

%% RPC callback for forwarded requests
-export([handle_forwarded_request/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-export([
    is_this_node_leader/1,
    update_leader_status/1,
    get_ra_members/1,
    handle_local_operation/2
]).
-endif.

-define(SERVER, ?MODULE).
-define(RA_CLUSTER_NAME, flurm_controller).
-define(RA_MACHINE, flurm_controller_ra_machine).
-define(LEADER_CHECK_INTERVAL, 1000).
-define(FORWARD_TIMEOUT, 5000).
-define(ELECTION_TIMEOUT, 5000).

-record(state, {
    cluster_name :: atom(),
    ra_cluster_id :: term(),
    is_leader = false :: boolean(),
    current_leader :: {atom(), node()} | undefined,
    cluster_nodes = [] :: [node()],
    ra_ready = false :: boolean(),
    leader_check_ref :: reference() | undefined,
    data_dir :: string()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the cluster coordination server.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Check if this node is the current leader.
-spec is_leader() -> boolean().
is_leader() ->
    try
        gen_server:call(?SERVER, is_leader, 1000)
    catch
        exit:{timeout, _} -> false;
        exit:{noproc, _} -> false
    end.

%% @doc Get the current leader node.
-spec get_leader() -> {ok, {atom(), node()}} | {error, no_leader | not_ready}.
get_leader() ->
    try
        gen_server:call(?SERVER, get_leader, 2000)
    catch
        exit:{timeout, _} -> {error, not_ready};
        exit:{noproc, _} -> {error, not_ready}
    end.

%% @doc Forward a request to the current leader.
-spec forward_to_leader(atom(), term()) -> {ok, term()} | {error, term()}.
forward_to_leader(Operation, Args) ->
    gen_server:call(?SERVER, {forward_to_leader, Operation, Args}, ?FORWARD_TIMEOUT).

%% @doc Get cluster status information.
-spec cluster_status() -> map().
cluster_status() ->
    try
        gen_server:call(?SERVER, cluster_status, 2000)
    catch
        exit:{timeout, _} -> #{status => timeout};
        exit:{noproc, _} -> #{status => not_started}
    end.

%% @doc Join an existing cluster.
-spec join_cluster(node()) -> ok | {error, term()}.
join_cluster(Node) ->
    gen_server:call(?SERVER, {join_cluster, Node}, 10000).

%% @doc Leave the cluster.
-spec leave_cluster() -> ok | {error, term()}.
leave_cluster() ->
    gen_server:call(?SERVER, leave_cluster, 10000).

%% @doc Get list of cluster members.
-spec get_members() -> {ok, [{atom(), node()}]} | {error, term()}.
get_members() ->
    gen_server:call(?SERVER, get_members, 2000).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    process_flag(trap_exit, true),

    ClusterName = application:get_env(flurm_controller, cluster_name, ?RA_CLUSTER_NAME),
    DataDir = application:get_env(flurm_controller, ra_data_dir, "/var/lib/flurm/ra"),
    ClusterNodes = application:get_env(flurm_controller, cluster_nodes, [node()]),

    lager:info("Initializing FLURM controller cluster: ~p", [ClusterName]),
    lager:info("Cluster nodes: ~p", [ClusterNodes]),
    lager:info("Ra data directory: ~s", [DataDir]),

    State = #state{
        cluster_name = ClusterName,
        cluster_nodes = ClusterNodes,
        data_dir = DataDir
    },

    %% Initialize Ra cluster asynchronously to not block startup
    self() ! init_ra_cluster,

    {ok, State}.

handle_call(is_leader, _From, #state{is_leader = IsLeader, ra_ready = Ready} = State) ->
    %% If Ra not ready yet, we're definitely not the leader
    Result = Ready andalso IsLeader,
    {reply, Result, State};

handle_call(get_leader, _From, #state{ra_ready = false} = State) ->
    {reply, {error, not_ready}, State};

handle_call(get_leader, _From, #state{current_leader = undefined} = State) ->
    {reply, {error, no_leader}, State};

handle_call(get_leader, _From, #state{current_leader = Leader} = State) ->
    {reply, {ok, Leader}, State};

handle_call({forward_to_leader, _Operation, _Args}, _From,
            #state{ra_ready = false} = State) ->
    {reply, {error, cluster_not_ready}, State};

handle_call({forward_to_leader, Operation, Args}, _From,
            #state{is_leader = true} = State) ->
    %% We are the leader, process locally
    Result = handle_local_operation(Operation, Args),
    {reply, Result, State};

handle_call({forward_to_leader, _Operation, _Args}, _From,
            #state{current_leader = undefined} = State) ->
    {reply, {error, no_leader}, State};

handle_call({forward_to_leader, Operation, Args}, _From,
            #state{current_leader = {_Name, LeaderNode}} = State) ->
    %% Forward to leader
    Result = try
        rpc:call(LeaderNode, ?MODULE, handle_forwarded_request,
                 [Operation, Args], ?FORWARD_TIMEOUT)
    catch
        _:Reason ->
            {error, {forward_failed, Reason}}
    end,
    {reply, Result, State};

handle_call(cluster_status, _From, State) ->
    #state{
        cluster_name = ClusterName,
        is_leader = IsLeader,
        current_leader = Leader,
        cluster_nodes = Nodes,
        ra_ready = Ready
    } = State,

    Status = #{
        cluster_name => ClusterName,
        this_node => node(),
        is_leader => IsLeader,
        current_leader => Leader,
        cluster_nodes => Nodes,
        ra_ready => Ready,
        ra_members => get_ra_members(State)
    },
    {reply, Status, State};

handle_call({join_cluster, ExistingNode}, _From, State) ->
    case do_join_cluster(ExistingNode, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(leave_cluster, _From, State) ->
    case do_leave_cluster(State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(get_members, _From, #state{ra_ready = false} = State) ->
    {reply, {error, not_ready}, State};

handle_call(get_members, _From, #state{ra_cluster_id = ClusterId} = State) ->
    case ra:members(ClusterId) of
        {ok, Members, _Leader} ->
            {reply, {ok, Members}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {timeout, _} ->
            {reply, {error, timeout}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(init_ra_cluster, State) ->
    NewState = do_init_ra_cluster(State),
    %% Start periodic leader check
    Ref = erlang:send_after(?LEADER_CHECK_INTERVAL, self(), check_leader),
    {noreply, NewState#state{leader_check_ref = Ref}};

handle_info(check_leader, State) ->
    NewState = update_leader_status(State),
    %% Schedule next check
    Ref = erlang:send_after(?LEADER_CHECK_INTERVAL, self(), check_leader),
    {noreply, NewState#state{leader_check_ref = Ref}};

handle_info({ra_event, _From, {machine, leader_change, NewLeader}}, State) ->
    lager:info("Ra leader change event: ~p", [NewLeader]),
    NewState = State#state{
        current_leader = NewLeader,
        is_leader = is_this_node_leader(NewLeader)
    },
    maybe_notify_leadership_change(State, NewState),
    {noreply, NewState};

handle_info({ra_event, _From, Event}, State) ->
    lager:debug("Ra event: ~p", [Event]),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{leader_check_ref = Ref}) ->
    case Ref of
        undefined -> ok;
        _ -> erlang:cancel_timer(Ref)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions - Ra Cluster Management
%%====================================================================

%% @doc Initialize the Ra cluster.
do_init_ra_cluster(#state{cluster_name = ClusterName,
                          cluster_nodes = ClusterNodes,
                          data_dir = DataDir} = State) ->
    %% Ensure Ra application is started
    case application:ensure_all_started(ra) of
        {ok, _} ->
            ok;
        {error, {already_started, ra}} ->
            ok;
        {error, RaStartError} ->
            lager:error("Failed to start Ra: ~p", [RaStartError]),
            State
    end,

    %% Configure Ra data directory
    application:set_env(ra, data_dir, DataDir),

    %% Define Ra machine configuration
    Machine = {module, ?RA_MACHINE, #{}},

    %% Build server configurations for all nodes
    %% Server IDs are tuples {ClusterName, Node}, but cluster name is just an atom
    Servers = [{ClusterName, N} || N <- ClusterNodes, N =/= node()],
    ThisServer = {ClusterName, node()},

    lager:info("Attempting to start Ra cluster: ~p with servers: ~p", [ClusterName, [ThisServer | Servers]]),

    %% Try to start or join the cluster
    %% Note: ClusterName must be an atom, not a tuple
    case try_start_ra_cluster(ClusterName, ThisServer, Servers, Machine, State) of
        {ok, NewState} ->
            lager:info("Ra cluster started successfully"),
            %% Store ThisServer as the cluster_id for ra:members calls
            NewState#state{ra_ready = true, ra_cluster_id = ThisServer};
        {error, Reason} ->
            lager:warning("Ra cluster init failed: ~p, will retry", [Reason]),
            %% Schedule retry
            erlang:send_after(5000, self(), init_ra_cluster),
            State
    end.

%% @doc Try to start or join the Ra cluster.
%% ClusterName is an atom, ThisServer is {ClusterName, Node} tuple
try_start_ra_cluster(ClusterName, ThisServer, OtherServers, Machine, State) ->
    AllServers = [ThisServer | OtherServers],

    %% First, check if Ra server is already running
    case ra:members(ThisServer) of
        {ok, _Members, _Leader} ->
            lager:info("Ra cluster already running, reusing existing cluster"),
            {ok, State#state{ra_cluster_id = ThisServer}};
        {error, noproc} ->
            %% Server not running, try to start the cluster
            do_start_ra_cluster(ClusterName, ThisServer, AllServers, OtherServers, Machine, State);
        {timeout, _} ->
            %% Server might be starting up, try anyway
            do_start_ra_cluster(ClusterName, ThisServer, AllServers, OtherServers, Machine, State);
        {error, _} ->
            %% Other error, try to start
            do_start_ra_cluster(ClusterName, ThisServer, AllServers, OtherServers, Machine, State)
    end.

%% @doc Actually start the Ra cluster.
do_start_ra_cluster(ClusterName, ThisServer, AllServers, OtherServers, Machine, State) ->
    %% ra:start_cluster/4 expects: SystemName (atom), ClusterName (atom), Machine, Servers
    case ra:start_cluster(default, ClusterName, Machine, AllServers) of
        {ok, Started, _Failed} ->
            lager:info("Ra cluster started with servers: ~p", [Started]),
            {ok, State#state{ra_cluster_id = ThisServer}};
        {error, cluster_not_formed} ->
            %% Try joining existing cluster
            try_join_existing_cluster(ClusterName, ThisServer, OtherServers, Machine, State);
        {error, {already_started, _}} ->
            %% Already running, that's fine
            lager:info("Ra server already started"),
            {ok, State#state{ra_cluster_id = ThisServer}};
        {error, {shutdown, {failed_to_start_child, _, {already_started, _}}}} ->
            %% Server already started via supervisor - this is fine
            lager:info("Ra server already started (via supervisor)"),
            {ok, State#state{ra_cluster_id = ThisServer}};
        {error, Reason} ->
            lager:warning("Failed to start Ra cluster: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Try to join an existing cluster.
%% ClusterName is an atom, ThisServer is {ClusterName, Node} tuple
try_join_existing_cluster(ClusterName, ThisServer, OtherServers, Machine, State) ->
    %% Try to contact each other server
    JoinResult = lists:foldl(
        fun(Server, {error, _}) ->
            %% ra:start_server/5 expects: SystemName, ClusterName (atom), ServerId, Machine, ServerIds
            case ra:start_server(default, ClusterName, ThisServer, Machine, [Server]) of
                ok ->
                    %% Now add ourselves to the cluster
                    case ra:add_member(Server, ThisServer) of
                        {ok, _, _} ->
                            lager:info("Joined cluster via ~p", [Server]),
                            {ok, Server};
                        AddError ->
                            {error, AddError}
                    end;
                {error, {already_started, _}} ->
                    {ok, already_member};
                Error ->
                    {error, Error}
            end;
           (_, Success) ->
               Success
        end, {error, no_servers}, OtherServers),

    case JoinResult of
        {ok, _} ->
            {ok, State#state{ra_cluster_id = ThisServer}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Update leadership status by querying Ra.
update_leader_status(#state{ra_ready = false} = State) ->
    State;
update_leader_status(#state{ra_cluster_id = ClusterId} = State) ->
    case ra:members(ClusterId) of
        {ok, _Members, Leader} ->
            IsLeader = is_this_node_leader(Leader),
            OldLeader = State#state.is_leader,
            NewState = State#state{
                current_leader = Leader,
                is_leader = IsLeader
            },
            case OldLeader =/= IsLeader of
                true ->
                    maybe_notify_leadership_change(State, NewState);
                false ->
                    ok
            end,
            NewState;
        {error, Reason} ->
            lager:debug("Failed to get Ra members: ~p", [Reason]),
            State;
        {timeout, _} ->
            lager:debug("Timeout getting Ra members"),
            State
    end.

%% @doc Check if the given leader is this node.
is_this_node_leader(undefined) ->
    false;
is_this_node_leader({_Name, Node}) ->
    Node =:= node().

%% @doc Get Ra cluster members.
get_ra_members(#state{ra_ready = false}) ->
    [];
get_ra_members(#state{ra_cluster_id = ClusterId}) ->
    case ra:members(ClusterId) of
        {ok, Members, _Leader} ->
            Members;
        _ ->
            []
    end.

%% @doc Notify about leadership changes.
maybe_notify_leadership_change(#state{is_leader = OldLeader},
                                #state{is_leader = NewLeader})
  when OldLeader =/= NewLeader ->
    case NewLeader of
        true ->
            lager:info("This node became the LEADER"),
            %% Notify failover module
            catch flurm_controller_failover:on_became_leader();
        false ->
            lager:info("This node is no longer the leader"),
            catch flurm_controller_failover:on_lost_leadership()
    end;
maybe_notify_leadership_change(_, _) ->
    ok.

%%====================================================================
%% Internal functions - Cluster Membership
%%====================================================================

%% @doc Join an existing cluster.
do_join_cluster(ExistingNode, #state{cluster_name = ClusterName} = State) ->
    ThisServer = {ClusterName, node()},
    ExistingServer = {ClusterName, ExistingNode},

    lager:info("Attempting to join cluster via ~p", [ExistingNode]),

    %% First, get the existing cluster's configuration
    case rpc:call(ExistingNode, ra, members, [ExistingServer], 5000) of
        {ok, Members, _Leader} ->
            %% Start local Ra server
            %% ra:start_server/5 expects: SystemName, ClusterName (atom), ServerId, Machine, ServerIds
            Machine = {module, ?RA_MACHINE, #{}},
            case ra:start_server(default, ClusterName, ThisServer, Machine, Members) of
                ok ->
                    %% Request to be added to cluster
                    case ra:add_member(ExistingServer, ThisServer) of
                        {ok, _, _} ->
                            lager:info("Successfully joined cluster"),
                            {ok, State#state{ra_ready = true, ra_cluster_id = ThisServer}};
                        {error, Reason} ->
                            lager:error("Failed to add to cluster: ~p", [Reason]),
                            {error, Reason}
                    end;
                {error, {already_started, _}} ->
                    {ok, State#state{ra_ready = true, ra_cluster_id = ThisServer}};
                {error, Reason} ->
                    lager:error("Failed to start Ra server: ~p", [Reason]),
                    {error, Reason}
            end;
        {badrpc, Reason} ->
            lager:error("Failed to contact ~p: ~p", [ExistingNode, Reason]),
            {error, {node_unreachable, Reason}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Leave the cluster.
do_leave_cluster(#state{cluster_name = ClusterName,
                        current_leader = Leader,
                        ra_ready = true} = State) ->
    ThisServer = {ClusterName, node()},

    lager:info("Leaving cluster"),

    %% Ask leader to remove us
    case Leader of
        undefined ->
            {error, no_leader};
        {_Name, LeaderNode} when LeaderNode =:= node() ->
            %% We are leader, need to transfer leadership first
            case ra:transfer_leadership(ThisServer, undefined) of
                ok ->
                    timer:sleep(1000),
                    do_leave_cluster(update_leader_status(State));
                {error, Reason} ->
                    {error, Reason}
            end;
        LeaderServer ->
            case ra:remove_member(LeaderServer, ThisServer) of
                {ok, _, _} ->
                    ra:stop_server(default, ThisServer),
                    {ok, State#state{ra_ready = false, is_leader = false}};
                {error, Reason} ->
                    {error, Reason}
            end
    end;
do_leave_cluster(State) ->
    {ok, State}.

%%====================================================================
%% Internal functions - Request Handling
%%====================================================================

%% @doc Handle a local operation (when we are the leader).
handle_local_operation(submit_job, JobSpec) ->
    %% Use flurm_job_manager for job submission
    flurm_job_manager:submit_job(JobSpec);

handle_local_operation(cancel_job, JobId) ->
    flurm_job_manager:cancel_job(JobId);

handle_local_operation(get_job, JobId) ->
    flurm_job_manager:get_job(JobId);

handle_local_operation(list_jobs, _Args) ->
    flurm_job_manager:list_jobs();

handle_local_operation(Operation, Args) ->
    lager:warning("Unknown operation forwarded: ~p(~p)", [Operation, Args]),
    {error, unknown_operation}.

%% @doc Handle a request forwarded from another node.
%% This is called via RPC when another node forwards a request to us.
-spec handle_forwarded_request(atom(), term()) -> term().
handle_forwarded_request(Operation, Args) ->
    case is_leader() of
        true ->
            handle_local_operation(Operation, Args);
        false ->
            %% We're not the leader anymore, reject
            {error, not_leader}
    end.
