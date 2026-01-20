%%%-------------------------------------------------------------------
%%% @doc FLURM Account Manager
%%%
%%% Manages accounting entities: accounts, users, associations, and QOS.
%%% Provides the backend for sacctmgr-style operations.
%%%
%%% Data is stored in ETS tables for fast access, with optional
%%% persistence through the Ra consensus layer when cluster mode
%%% is enabled.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_account_manager).

-behaviour(gen_server).

-export([start_link/0]).
-export([
    %% Account operations
    add_account/1,
    modify_account/2,
    delete_account/1,
    get_account/1,
    list_accounts/0,
    list_accounts/1,

    %% User operations
    add_user/1,
    modify_user/2,
    delete_user/1,
    get_user/1,
    list_users/0,
    list_users/1,

    %% Association operations
    add_association/1,
    modify_association/2,
    delete_association/1,
    get_association/1,
    get_association/3,
    list_associations/0,
    list_associations/1,

    %% QOS operations
    add_qos/1,
    modify_qos/2,
    delete_qos/1,
    get_qos/1,
    list_qos/0,

    %% Cluster operations
    add_cluster/1,
    get_cluster/1,
    list_clusters/0,

    %% TRES operations
    add_tres/1,
    get_tres/1,
    list_tres/0,

    %% Utility
    get_user_association/2,
    check_limits/2
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Test exports for internal functions
-ifdef(TEST).
-export([
    normalize_account/1,
    normalize_user/1,
    normalize_association/1,
    normalize_qos/1,
    normalize_cluster/1,
    normalize_tres/1,
    apply_account_updates/2,
    apply_user_updates/2,
    apply_association_updates/2,
    apply_qos_updates/2,
    matches_pattern/2,
    build_tres_request/1,
    check_tres_limits/3,
    combine_tres_maps/2,
    run_checks/1
]).
-endif.

-include_lib("flurm_core/include/flurm_core.hrl").

%% Local copy of usage record from flurm_limits for pattern matching
%% This record is used by flurm_limits:get_usage/2 return values
-record(usage, {
    key :: {user | account, binary()} |
           {user, binary(), binary()},
    running_jobs = 0 :: non_neg_integer(),
    pending_jobs = 0 :: non_neg_integer(),
    tres_used = #{} :: map(),
    tres_mins = #{} :: map()
}).

-record(state, {
    accounts :: ets:tid(),
    users :: ets:tid(),
    associations :: ets:tid(),
    qos :: ets:tid(),
    clusters :: ets:tid(),
    tres :: ets:tid(),
    next_assoc_id = 1 :: pos_integer(),
    next_tres_id = 1 :: pos_integer()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            %% Process already running - return existing pid
            %% This handles race conditions during startup and restarts
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% Account operations

-spec add_account(#account{} | map()) -> ok | {error, term()}.
add_account(Account) ->
    gen_server:call(?MODULE, {add_account, normalize_account(Account)}).

-spec modify_account(binary(), map()) -> ok | {error, not_found}.
modify_account(Name, Updates) ->
    gen_server:call(?MODULE, {modify_account, Name, Updates}).

-spec delete_account(binary()) -> ok | {error, not_found | has_children}.
delete_account(Name) ->
    gen_server:call(?MODULE, {delete_account, Name}).

-spec get_account(binary()) -> {ok, #account{}} | {error, not_found}.
get_account(Name) ->
    gen_server:call(?MODULE, {get_account, Name}).

-spec list_accounts() -> [#account{}].
list_accounts() ->
    gen_server:call(?MODULE, list_accounts).

-spec list_accounts(map()) -> [#account{}].
list_accounts(Filters) ->
    gen_server:call(?MODULE, {list_accounts, Filters}).

%% User operations

-spec add_user(#acct_user{} | map()) -> ok | {error, term()}.
add_user(User) ->
    gen_server:call(?MODULE, {add_user, normalize_user(User)}).

-spec modify_user(binary(), map()) -> ok | {error, not_found}.
modify_user(Name, Updates) ->
    gen_server:call(?MODULE, {modify_user, Name, Updates}).

-spec delete_user(binary()) -> ok | {error, not_found}.
delete_user(Name) ->
    gen_server:call(?MODULE, {delete_user, Name}).

-spec get_user(binary()) -> {ok, #acct_user{}} | {error, not_found}.
get_user(Name) ->
    gen_server:call(?MODULE, {get_user, Name}).

-spec list_users() -> [#acct_user{}].
list_users() ->
    gen_server:call(?MODULE, list_users).

-spec list_users(map()) -> [#acct_user{}].
list_users(Filters) ->
    gen_server:call(?MODULE, {list_users, Filters}).

%% Association operations

-spec add_association(#association{} | map()) -> {ok, pos_integer()} | {error, term()}.
add_association(Assoc) ->
    gen_server:call(?MODULE, {add_association, normalize_association(Assoc)}).

-spec modify_association(pos_integer(), map()) -> ok | {error, not_found}.
modify_association(Id, Updates) ->
    gen_server:call(?MODULE, {modify_association, Id, Updates}).

-spec delete_association(pos_integer()) -> ok | {error, not_found}.
delete_association(Id) ->
    gen_server:call(?MODULE, {delete_association, Id}).

-spec get_association(pos_integer()) -> {ok, #association{}} | {error, not_found}.
get_association(Id) ->
    gen_server:call(?MODULE, {get_association, Id}).

-spec get_association(binary(), binary(), binary()) -> {ok, #association{}} | {error, not_found}.
get_association(Cluster, Account, User) ->
    gen_server:call(?MODULE, {get_association_by_key, Cluster, Account, User}).

-spec list_associations() -> [#association{}].
list_associations() ->
    gen_server:call(?MODULE, list_associations).

-spec list_associations(map()) -> [#association{}].
list_associations(Filters) ->
    gen_server:call(?MODULE, {list_associations, Filters}).

%% QOS operations

-spec add_qos(#qos{} | map()) -> ok | {error, term()}.
add_qos(Qos) ->
    gen_server:call(?MODULE, {add_qos, normalize_qos(Qos)}).

-spec modify_qos(binary(), map()) -> ok | {error, not_found}.
modify_qos(Name, Updates) ->
    gen_server:call(?MODULE, {modify_qos, Name, Updates}).

-spec delete_qos(binary()) -> ok | {error, not_found}.
delete_qos(Name) ->
    gen_server:call(?MODULE, {delete_qos, Name}).

-spec get_qos(binary()) -> {ok, #qos{}} | {error, not_found}.
get_qos(Name) ->
    gen_server:call(?MODULE, {get_qos, Name}).

-spec list_qos() -> [#qos{}].
list_qos() ->
    gen_server:call(?MODULE, list_qos).

%% Cluster operations

-spec add_cluster(#acct_cluster{} | map()) -> ok | {error, term()}.
add_cluster(Cluster) ->
    gen_server:call(?MODULE, {add_cluster, normalize_cluster(Cluster)}).

-spec get_cluster(binary()) -> {ok, #acct_cluster{}} | {error, not_found}.
get_cluster(Name) ->
    gen_server:call(?MODULE, {get_cluster, Name}).

-spec list_clusters() -> [#acct_cluster{}].
list_clusters() ->
    gen_server:call(?MODULE, list_clusters).

%% TRES operations

-spec add_tres(#tres{} | map()) -> {ok, pos_integer()} | {error, term()}.
add_tres(Tres) ->
    gen_server:call(?MODULE, {add_tres, normalize_tres(Tres)}).

-spec get_tres(pos_integer() | binary()) -> {ok, #tres{}} | {error, not_found}.
get_tres(IdOrType) ->
    gen_server:call(?MODULE, {get_tres, IdOrType}).

-spec list_tres() -> [#tres{}].
list_tres() ->
    gen_server:call(?MODULE, list_tres).

%% Utility functions

-spec get_user_association(binary(), binary()) -> {ok, #association{}} | {error, not_found}.
get_user_association(Username, Account) ->
    gen_server:call(?MODULE, {get_user_association, Username, Account}).

-spec check_limits(binary(), map()) -> ok | {error, term()}.
check_limits(Username, JobSpec) ->
    gen_server:call(?MODULE, {check_limits, Username, JobSpec}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    lager:info("Account Manager started"),

    %% Create ETS tables
    Accounts = ets:new(accounts, [set, protected, {keypos, #account.name}]),
    Users = ets:new(users, [set, protected, {keypos, #acct_user.name}]),
    Associations = ets:new(associations, [set, protected, {keypos, #association.id}]),
    Qos = ets:new(qos, [set, protected, {keypos, #qos.name}]),
    Clusters = ets:new(clusters, [set, protected, {keypos, #acct_cluster.name}]),
    Tres = ets:new(tres, [set, protected, {keypos, #tres.id}]),

    %% Initialize default QOS
    DefaultQos = #qos{name = <<"normal">>, description = <<"Default QOS">>},
    ets:insert(Qos, DefaultQos),

    %% Initialize default TRES
    ets:insert(Tres, #tres{id = 1, type = <<"cpu">>}),
    ets:insert(Tres, #tres{id = 2, type = <<"mem">>}),
    ets:insert(Tres, #tres{id = 3, type = <<"energy">>}),
    ets:insert(Tres, #tres{id = 4, type = <<"node">>}),

    %% Get cluster name from config
    ClusterName = case application:get_env(flurm_controller, cluster_name) of
        {ok, Name} -> list_to_binary(atom_to_list(Name));
        undefined -> <<"flurm">>
    end,

    %% Initialize default cluster
    DefaultCluster = #acct_cluster{name = ClusterName},
    ets:insert(Clusters, DefaultCluster),

    %% Initialize root account
    RootAccount = #account{
        name = <<"root">>,
        description = <<"Root account">>,
        organization = <<"System">>
    },
    ets:insert(Accounts, RootAccount),

    {ok, #state{
        accounts = Accounts,
        users = Users,
        associations = Associations,
        qos = Qos,
        clusters = Clusters,
        tres = Tres,
        next_assoc_id = 1,
        next_tres_id = 5
    }}.

handle_call({add_account, #account{} = Account}, _From, State) ->
    case ets:lookup(State#state.accounts, Account#account.name) of
        [] ->
            ets:insert(State#state.accounts, Account),
            lager:info("Added account: ~s", [Account#account.name]),
            {reply, ok, State};
        [_] ->
            {reply, {error, already_exists}, State}
    end;

handle_call({modify_account, Name, Updates}, _From, State) ->
    case ets:lookup(State#state.accounts, Name) of
        [Account] ->
            Updated = apply_account_updates(Account, Updates),
            ets:insert(State#state.accounts, Updated),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({delete_account, Name}, _From, State) ->
    %% Check for child accounts
    Children = ets:match_object(State#state.accounts, #account{parent = Name, _ = '_'}),
    case Children of
        [] ->
            case ets:lookup(State#state.accounts, Name) of
                [_] ->
                    ets:delete(State#state.accounts, Name),
                    lager:info("Deleted account: ~s", [Name]),
                    {reply, ok, State};
                [] ->
                    {reply, {error, not_found}, State}
            end;
        _ ->
            {reply, {error, has_children}, State}
    end;

handle_call({get_account, Name}, _From, State) ->
    case ets:lookup(State#state.accounts, Name) of
        [Account] -> {reply, {ok, Account}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call(list_accounts, _From, State) ->
    Accounts = ets:tab2list(State#state.accounts),
    {reply, Accounts, State};

handle_call({list_accounts, Filters}, _From, State) ->
    Accounts = filter_accounts(ets:tab2list(State#state.accounts), Filters),
    {reply, Accounts, State};

handle_call({add_user, #acct_user{} = User}, _From, State) ->
    case ets:lookup(State#state.users, User#acct_user.name) of
        [] ->
            ets:insert(State#state.users, User),
            lager:info("Added user: ~s", [User#acct_user.name]),
            {reply, ok, State};
        [_] ->
            {reply, {error, already_exists}, State}
    end;

handle_call({modify_user, Name, Updates}, _From, State) ->
    case ets:lookup(State#state.users, Name) of
        [User] ->
            Updated = apply_user_updates(User, Updates),
            ets:insert(State#state.users, Updated),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({delete_user, Name}, _From, State) ->
    case ets:lookup(State#state.users, Name) of
        [_] ->
            ets:delete(State#state.users, Name),
            lager:info("Deleted user: ~s", [Name]),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_user, Name}, _From, State) ->
    case ets:lookup(State#state.users, Name) of
        [User] -> {reply, {ok, User}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call(list_users, _From, State) ->
    Users = ets:tab2list(State#state.users),
    {reply, Users, State};

handle_call({list_users, Filters}, _From, State) ->
    Users = filter_users(ets:tab2list(State#state.users), Filters),
    {reply, Users, State};

handle_call({add_association, Assoc}, _From, State) ->
    Id = State#state.next_assoc_id,
    NewAssoc = Assoc#association{id = Id},
    ets:insert(State#state.associations, NewAssoc),
    lager:info("Added association ~p: cluster=~s account=~s user=~s",
               [Id, NewAssoc#association.cluster, NewAssoc#association.account,
                NewAssoc#association.user]),
    {reply, {ok, Id}, State#state{next_assoc_id = Id + 1}};

handle_call({modify_association, Id, Updates}, _From, State) ->
    case ets:lookup(State#state.associations, Id) of
        [Assoc] ->
            Updated = apply_association_updates(Assoc, Updates),
            ets:insert(State#state.associations, Updated),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({delete_association, Id}, _From, State) ->
    case ets:lookup(State#state.associations, Id) of
        [_] ->
            ets:delete(State#state.associations, Id),
            lager:info("Deleted association: ~p", [Id]),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_association, Id}, _From, State) ->
    case ets:lookup(State#state.associations, Id) of
        [Assoc] -> {reply, {ok, Assoc}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call({get_association_by_key, Cluster, Account, User}, _From, State) ->
    Pattern = #association{cluster = Cluster, account = Account, user = User, _ = '_'},
    case ets:match_object(State#state.associations, Pattern) of
        [Assoc | _] -> {reply, {ok, Assoc}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call(list_associations, _From, State) ->
    Assocs = ets:tab2list(State#state.associations),
    {reply, Assocs, State};

handle_call({list_associations, Filters}, _From, State) ->
    Assocs = filter_associations(ets:tab2list(State#state.associations), Filters),
    {reply, Assocs, State};

handle_call({add_qos, #qos{} = Qos}, _From, State) ->
    case ets:lookup(State#state.qos, Qos#qos.name) of
        [] ->
            ets:insert(State#state.qos, Qos),
            lager:info("Added QOS: ~s", [Qos#qos.name]),
            {reply, ok, State};
        [_] ->
            {reply, {error, already_exists}, State}
    end;

handle_call({modify_qos, Name, Updates}, _From, State) ->
    case ets:lookup(State#state.qos, Name) of
        [Qos] ->
            Updated = apply_qos_updates(Qos, Updates),
            ets:insert(State#state.qos, Updated),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({delete_qos, Name}, _From, State) ->
    case ets:lookup(State#state.qos, Name) of
        [_] ->
            ets:delete(State#state.qos, Name),
            lager:info("Deleted QOS: ~s", [Name]),
            {reply, ok, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_qos, Name}, _From, State) ->
    case ets:lookup(State#state.qos, Name) of
        [Qos] -> {reply, {ok, Qos}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call(list_qos, _From, State) ->
    QosList = ets:tab2list(State#state.qos),
    {reply, QosList, State};

handle_call({add_cluster, #acct_cluster{} = Cluster}, _From, State) ->
    case ets:lookup(State#state.clusters, Cluster#acct_cluster.name) of
        [] ->
            ets:insert(State#state.clusters, Cluster),
            lager:info("Added cluster: ~s", [Cluster#acct_cluster.name]),
            {reply, ok, State};
        [_] ->
            {reply, {error, already_exists}, State}
    end;

handle_call({get_cluster, Name}, _From, State) ->
    case ets:lookup(State#state.clusters, Name) of
        [Cluster] -> {reply, {ok, Cluster}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call(list_clusters, _From, State) ->
    Clusters = ets:tab2list(State#state.clusters),
    {reply, Clusters, State};

handle_call({add_tres, Tres}, _From, State) ->
    Id = State#state.next_tres_id,
    NewTres = Tres#tres{id = Id},
    ets:insert(State#state.tres, NewTres),
    lager:info("Added TRES ~p: type=~s", [Id, NewTres#tres.type]),
    {reply, {ok, Id}, State#state{next_tres_id = Id + 1}};

handle_call({get_tres, Id}, _From, State) when is_integer(Id) ->
    case ets:lookup(State#state.tres, Id) of
        [Tres] -> {reply, {ok, Tres}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call({get_tres, Type}, _From, State) when is_binary(Type) ->
    Pattern = #tres{type = Type, _ = '_'},
    case ets:match_object(State#state.tres, Pattern) of
        [Tres | _] -> {reply, {ok, Tres}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call(list_tres, _From, State) ->
    TresList = ets:tab2list(State#state.tres),
    {reply, TresList, State};

handle_call({get_user_association, Username, Account}, _From, State) ->
    %% Find association for user in the specified account
    Pattern = #association{account = Account, user = Username, _ = '_'},
    case ets:match_object(State#state.associations, Pattern) of
        [Assoc | _] -> {reply, {ok, Assoc}, State};
        [] -> {reply, {error, not_found}, State}
    end;

handle_call({check_limits, Username, JobSpec}, _From, State) ->
    %% Check account-based limits for the user
    Result = do_check_limits(Username, JobSpec, State),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

normalize_account(#account{} = A) -> A;
normalize_account(Map) when is_map(Map) ->
    #account{
        name = maps:get(name, Map),
        description = maps:get(description, Map, <<>>),
        organization = maps:get(organization, Map, <<>>),
        parent = maps:get(parent, Map, <<>>),
        coordinators = maps:get(coordinators, Map, []),
        default_qos = maps:get(default_qos, Map, <<>>),
        fairshare = maps:get(fairshare, Map, 1),
        max_jobs = maps:get(max_jobs, Map, 0),
        max_submit = maps:get(max_submit, Map, 0),
        max_wall = maps:get(max_wall, Map, 0)
    }.

normalize_user(#acct_user{} = U) -> U;
normalize_user(Map) when is_map(Map) ->
    #acct_user{
        name = maps:get(name, Map),
        default_account = maps:get(default_account, Map, <<>>),
        accounts = maps:get(accounts, Map, []),
        default_qos = maps:get(default_qos, Map, <<>>),
        admin_level = maps:get(admin_level, Map, none),
        fairshare = maps:get(fairshare, Map, 1),
        max_jobs = maps:get(max_jobs, Map, 0),
        max_submit = maps:get(max_submit, Map, 0),
        max_wall = maps:get(max_wall, Map, 0)
    }.

normalize_association(#association{} = A) -> A;
normalize_association(Map) when is_map(Map) ->
    #association{
        id = maps:get(id, Map, 0),
        cluster = maps:get(cluster, Map, <<"flurm">>),
        account = maps:get(account, Map),
        user = maps:get(user, Map, <<>>),
        partition = maps:get(partition, Map, <<>>),
        parent_id = maps:get(parent_id, Map, 0),
        shares = maps:get(shares, Map, 1),
        grp_tres_mins = maps:get(grp_tres_mins, Map, #{}),
        grp_tres = maps:get(grp_tres, Map, #{}),
        grp_jobs = maps:get(grp_jobs, Map, 0),
        grp_submit = maps:get(grp_submit, Map, 0),
        grp_wall = maps:get(grp_wall, Map, 0),
        max_tres_mins_per_job = maps:get(max_tres_mins_per_job, Map, #{}),
        max_tres_per_job = maps:get(max_tres_per_job, Map, #{}),
        max_tres_per_node = maps:get(max_tres_per_node, Map, #{}),
        max_jobs = maps:get(max_jobs, Map, 0),
        max_submit = maps:get(max_submit, Map, 0),
        max_wall_per_job = maps:get(max_wall_per_job, Map, 0),
        priority = maps:get(priority, Map, 0),
        qos = maps:get(qos, Map, []),
        default_qos = maps:get(default_qos, Map, <<>>)
    }.

normalize_qos(#qos{} = Q) -> Q;
normalize_qos(Map) when is_map(Map) ->
    #qos{
        name = maps:get(name, Map),
        description = maps:get(description, Map, <<>>),
        priority = maps:get(priority, Map, 0),
        flags = maps:get(flags, Map, []),
        grace_time = maps:get(grace_time, Map, 0),
        max_jobs_pa = maps:get(max_jobs_pa, Map, 0),
        max_jobs_pu = maps:get(max_jobs_pu, Map, 0),
        max_submit_jobs_pa = maps:get(max_submit_jobs_pa, Map, 0),
        max_submit_jobs_pu = maps:get(max_submit_jobs_pu, Map, 0),
        max_tres_pa = maps:get(max_tres_pa, Map, #{}),
        max_tres_pu = maps:get(max_tres_pu, Map, #{}),
        max_tres_per_job = maps:get(max_tres_per_job, Map, #{}),
        max_tres_per_node = maps:get(max_tres_per_node, Map, #{}),
        max_tres_per_user = maps:get(max_tres_per_user, Map, #{}),
        max_wall_per_job = maps:get(max_wall_per_job, Map, 0),
        min_tres_per_job = maps:get(min_tres_per_job, Map, #{}),
        preempt = maps:get(preempt, Map, []),
        preempt_mode = maps:get(preempt_mode, Map, off),
        usage_factor = maps:get(usage_factor, Map, 1.0),
        usage_threshold = maps:get(usage_threshold, Map, 0.0)
    }.

normalize_cluster(#acct_cluster{} = C) -> C;
normalize_cluster(Map) when is_map(Map) ->
    #acct_cluster{
        name = maps:get(name, Map),
        control_host = maps:get(control_host, Map, <<>>),
        control_port = maps:get(control_port, Map, 6817),
        rpc_version = maps:get(rpc_version, Map, 0),
        classification = maps:get(classification, Map, 0),
        tres = maps:get(tres, Map, #{}),
        flags = maps:get(flags, Map, [])
    }.

normalize_tres(#tres{} = T) -> T;
normalize_tres(Map) when is_map(Map) ->
    #tres{
        id = maps:get(id, Map, 0),
        type = maps:get(type, Map),
        name = maps:get(name, Map, <<>>),
        count = maps:get(count, Map, 0)
    }.

apply_account_updates(Account, Updates) ->
    maps:fold(fun
        (description, V, A) -> A#account{description = V};
        (organization, V, A) -> A#account{organization = V};
        (parent, V, A) -> A#account{parent = V};
        (coordinators, V, A) -> A#account{coordinators = V};
        (default_qos, V, A) -> A#account{default_qos = V};
        (fairshare, V, A) -> A#account{fairshare = V};
        (max_jobs, V, A) -> A#account{max_jobs = V};
        (max_submit, V, A) -> A#account{max_submit = V};
        (max_wall, V, A) -> A#account{max_wall = V};
        (_, _, A) -> A
    end, Account, Updates).

apply_user_updates(User, Updates) ->
    maps:fold(fun
        (default_account, V, U) -> U#acct_user{default_account = V};
        (accounts, V, U) -> U#acct_user{accounts = V};
        (default_qos, V, U) -> U#acct_user{default_qos = V};
        (admin_level, V, U) -> U#acct_user{admin_level = V};
        (fairshare, V, U) -> U#acct_user{fairshare = V};
        (max_jobs, V, U) -> U#acct_user{max_jobs = V};
        (max_submit, V, U) -> U#acct_user{max_submit = V};
        (max_wall, V, U) -> U#acct_user{max_wall = V};
        (_, _, U) -> U
    end, User, Updates).

apply_association_updates(Assoc, Updates) ->
    maps:fold(fun
        (shares, V, A) -> A#association{shares = V};
        (grp_tres_mins, V, A) -> A#association{grp_tres_mins = V};
        (grp_tres, V, A) -> A#association{grp_tres = V};
        (grp_jobs, V, A) -> A#association{grp_jobs = V};
        (grp_submit, V, A) -> A#association{grp_submit = V};
        (grp_wall, V, A) -> A#association{grp_wall = V};
        (max_tres_mins_per_job, V, A) -> A#association{max_tres_mins_per_job = V};
        (max_tres_per_job, V, A) -> A#association{max_tres_per_job = V};
        (max_tres_per_node, V, A) -> A#association{max_tres_per_node = V};
        (max_jobs, V, A) -> A#association{max_jobs = V};
        (max_submit, V, A) -> A#association{max_submit = V};
        (max_wall_per_job, V, A) -> A#association{max_wall_per_job = V};
        (priority, V, A) -> A#association{priority = V};
        (qos, V, A) -> A#association{qos = V};
        (default_qos, V, A) -> A#association{default_qos = V};
        (_, _, A) -> A
    end, Assoc, Updates).

apply_qos_updates(Qos, Updates) ->
    maps:fold(fun
        (description, V, Q) -> Q#qos{description = V};
        (priority, V, Q) -> Q#qos{priority = V};
        (flags, V, Q) -> Q#qos{flags = V};
        (grace_time, V, Q) -> Q#qos{grace_time = V};
        (max_jobs_pa, V, Q) -> Q#qos{max_jobs_pa = V};
        (max_jobs_pu, V, Q) -> Q#qos{max_jobs_pu = V};
        (max_submit_jobs_pa, V, Q) -> Q#qos{max_submit_jobs_pa = V};
        (max_submit_jobs_pu, V, Q) -> Q#qos{max_submit_jobs_pu = V};
        (max_tres_pa, V, Q) -> Q#qos{max_tres_pa = V};
        (max_tres_pu, V, Q) -> Q#qos{max_tres_pu = V};
        (max_tres_per_job, V, Q) -> Q#qos{max_tres_per_job = V};
        (max_tres_per_node, V, Q) -> Q#qos{max_tres_per_node = V};
        (max_tres_per_user, V, Q) -> Q#qos{max_tres_per_user = V};
        (max_wall_per_job, V, Q) -> Q#qos{max_wall_per_job = V};
        (min_tres_per_job, V, Q) -> Q#qos{min_tres_per_job = V};
        (preempt, V, Q) -> Q#qos{preempt = V};
        (preempt_mode, V, Q) -> Q#qos{preempt_mode = V};
        (usage_factor, V, Q) -> Q#qos{usage_factor = V};
        (usage_threshold, V, Q) -> Q#qos{usage_threshold = V};
        (_, _, Q) -> Q
    end, Qos, Updates).

filter_accounts(Accounts, Filters) ->
    lists:filter(fun(A) -> matches_account_filter(A, Filters) end, Accounts).

matches_account_filter(Account, Filters) ->
    maps:fold(fun
        (name, Pattern, Acc) -> Acc andalso matches_pattern(Account#account.name, Pattern);
        (parent, V, Acc) -> Acc andalso Account#account.parent =:= V;
        (organization, V, Acc) -> Acc andalso Account#account.organization =:= V;
        (_, _, Acc) -> Acc
    end, true, Filters).

filter_users(Users, Filters) ->
    lists:filter(fun(U) -> matches_user_filter(U, Filters) end, Users).

matches_user_filter(User, Filters) ->
    maps:fold(fun
        (name, Pattern, Acc) -> Acc andalso matches_pattern(User#acct_user.name, Pattern);
        (account, V, Acc) -> Acc andalso lists:member(V, User#acct_user.accounts);
        (admin_level, V, Acc) -> Acc andalso User#acct_user.admin_level =:= V;
        (_, _, Acc) -> Acc
    end, true, Filters).

filter_associations(Assocs, Filters) ->
    lists:filter(fun(A) -> matches_association_filter(A, Filters) end, Assocs).

matches_association_filter(Assoc, Filters) ->
    maps:fold(fun
        (cluster, V, Acc) -> Acc andalso Assoc#association.cluster =:= V;
        (account, V, Acc) -> Acc andalso Assoc#association.account =:= V;
        (user, V, Acc) -> Acc andalso Assoc#association.user =:= V;
        (partition, V, Acc) -> Acc andalso Assoc#association.partition =:= V;
        (_, _, Acc) -> Acc
    end, true, Filters).

matches_pattern(Value, Pattern) when is_binary(Value), is_binary(Pattern) ->
    %% Simple wildcard matching (* at end)
    case binary:last(Pattern) of
        $* ->
            Prefix = binary:part(Pattern, 0, byte_size(Pattern) - 1),
            binary:match(Value, Prefix) =:= {0, byte_size(Prefix)};
        _ ->
            Value =:= Pattern
    end;
matches_pattern(_, _) -> false.

%%====================================================================
%% Limit Checking Implementation
%%====================================================================

%% @doc Check all limits for a user submitting a job
%% Enforces limits in priority order:
%% 1. QOS limits (highest priority)
%% 2. Association limits (user+account+partition specific)
%% 3. Account limits
%% 4. User limits
-spec do_check_limits(binary(), map(), #state{}) -> ok | {error, term()}.
do_check_limits(Username, JobSpec, State) ->
    Account = maps:get(account, JobSpec, <<>>),
    Partition = maps:get(partition, JobSpec, <<>>),

    %% Step 1: Look up the user and their association
    case find_user_association(Username, Account, Partition, State) of
        {ok, Association, User} ->
            %% Step 2: Determine the effective QOS
            QosName = determine_effective_qos(JobSpec, Association, User, State),

            %% Step 3: Check QOS limits first (highest priority)
            case check_qos_limits(QosName, Username, JobSpec) of
                ok ->
                    %% Step 4: Check association-based limits
                    case check_association_limits(Association, JobSpec, State) of
                        ok ->
                            %% Step 5: Check account limits
                            case check_account_limits(Account, JobSpec, State) of
                                ok ->
                                    %% Step 6: Check user limits
                                    check_user_limits(User, JobSpec);
                                Error ->
                                    Error
                            end;
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        {error, no_association} ->
            %% No association found - check if we should allow based on config
            case application:get_env(flurm_core, require_association, false) of
                true ->
                    {error, {no_association, Username, Account}};
                false ->
                    %% Allow job without association, just check basic QOS limits
                    DefaultQos = get_default_qos(State),
                    check_qos_limits(DefaultQos, Username, JobSpec)
            end
    end.

%% @doc Find a user's association for the given account and partition
-spec find_user_association(binary(), binary(), binary(), #state{}) ->
    {ok, #association{}, #acct_user{}} | {error, no_association}.
find_user_association(Username, Account, Partition, State) ->
    %% First, look up the user
    case ets:lookup(State#state.users, Username) of
        [User] ->
            %% Determine which account to use
            EffectiveAccount = case Account of
                <<>> -> User#acct_user.default_account;
                _ -> Account
            end,

            %% Look for specific user+account+partition association
            case find_association(Username, EffectiveAccount, Partition, State) of
                {ok, Assoc} ->
                    {ok, Assoc, User};
                {error, not_found} ->
                    %% Try user+account association (any partition)
                    case find_association(Username, EffectiveAccount, <<>>, State) of
                        {ok, Assoc} ->
                            {ok, Assoc, User};
                        {error, not_found} ->
                            %% Try account-level association (no specific user)
                            case find_association(<<>>, EffectiveAccount, Partition, State) of
                                {ok, Assoc} ->
                                    {ok, Assoc, User};
                                {error, not_found} ->
                                    {error, no_association}
                            end
                    end
            end;
        [] ->
            {error, no_association}
    end.

%% @doc Find an association matching the given criteria
-spec find_association(binary(), binary(), binary(), #state{}) ->
    {ok, #association{}} | {error, not_found}.
find_association(Username, Account, Partition, State) ->
    Pattern = #association{
        user = Username,
        account = Account,
        partition = Partition,
        _ = '_'
    },
    case ets:match_object(State#state.associations, Pattern) of
        [Assoc | _] -> {ok, Assoc};
        [] -> {error, not_found}
    end.

%% @doc Determine the effective QOS for a job
-spec determine_effective_qos(map(), #association{}, #acct_user{}, #state{}) -> binary().
determine_effective_qos(JobSpec, Association, User, State) ->
    %% Priority: JobSpec > Association > User > Account > Default
    case maps:get(qos, JobSpec, <<>>) of
        <<>> ->
            %% Check association's default QOS
            case Association#association.default_qos of
                <<>> ->
                    %% Check user's default QOS
                    case User#acct_user.default_qos of
                        <<>> ->
                            %% Check account's default QOS
                            case ets:lookup(State#state.accounts, Association#association.account) of
                                [#account{default_qos = <<>>}] ->
                                    get_default_qos(State);
                                [#account{default_qos = AccountQos}] ->
                                    AccountQos;
                                [] ->
                                    get_default_qos(State)
                            end;
                        UserQos ->
                            UserQos
                    end;
                AssocQos ->
                    AssocQos
            end;
        RequestedQos ->
            %% Verify user is allowed to use requested QOS
            AllowedQos = Association#association.qos,
            case AllowedQos =:= [] orelse lists:member(RequestedQos, AllowedQos) of
                true -> RequestedQos;
                false ->
                    %% Fall back to default if not allowed
                    lager:warning("User ~s not allowed to use QOS ~s, using default",
                                 [User#acct_user.name, RequestedQos]),
                    get_default_qos(State)
            end
    end.

%% @doc Get the default QOS name
-spec get_default_qos(#state{}) -> binary().
get_default_qos(State) ->
    case ets:lookup(State#state.qos, <<"normal">>) of
        [_] -> <<"normal">>;
        [] ->
            %% Return first available QOS or empty
            case ets:first(State#state.qos) of
                '$end_of_table' -> <<>>;
                Name -> Name
            end
    end.

%% @doc Check QOS-based limits
-spec check_qos_limits(binary(), binary(), map()) -> ok | {error, term()}.
check_qos_limits(<<>>, _Username, _JobSpec) ->
    %% No QOS, allow
    ok;
check_qos_limits(QosName, Username, JobSpec) ->
    %% Check wall time and job count limits via flurm_qos
    case flurm_qos:check_limits(QosName, JobSpec) of
        ok ->
            %% Check TRES limits
            TresRequest = build_tres_request(JobSpec),
            flurm_qos:check_tres_limits(QosName, Username, TresRequest);
        Error ->
            Error
    end.

%% @doc Check association-based limits
-spec check_association_limits(#association{}, map(), #state{}) -> ok | {error, term()}.
check_association_limits(Association, JobSpec, _State) ->
    Checks = [
        %% Check max jobs per association
        fun() -> check_assoc_max_jobs(Association) end,
        %% Check max submit per association
        fun() -> check_assoc_max_submit(Association) end,
        %% Check max wall time per job
        fun() -> check_assoc_max_wall(Association, JobSpec) end,
        %% Check per-job TRES limits
        fun() -> check_assoc_tres_per_job(Association, JobSpec) end,
        %% Check per-node TRES limits
        fun() -> check_assoc_tres_per_node(Association, JobSpec) end,
        %% Check group TRES limits
        fun() -> check_assoc_grp_tres(Association, JobSpec) end
    ],
    run_checks(Checks).

%% @doc Check account-based limits
-spec check_account_limits(binary(), map(), #state{}) -> ok | {error, term()}.
check_account_limits(<<>>, _JobSpec, _State) ->
    ok;
check_account_limits(AccountName, JobSpec, State) ->
    case ets:lookup(State#state.accounts, AccountName) of
        [Account] ->
            Checks = [
                fun() -> check_account_max_jobs(Account) end,
                fun() -> check_account_max_submit(Account) end,
                fun() -> check_account_max_wall(Account, JobSpec) end
            ],
            run_checks(Checks);
        [] ->
            %% Account not found, allow (might be unconfigured)
            ok
    end.

%% @doc Check user-level limits
-spec check_user_limits(#acct_user{}, map()) -> ok | {error, term()}.
check_user_limits(User, JobSpec) ->
    Checks = [
        fun() -> check_user_max_jobs(User) end,
        fun() -> check_user_max_submit(User) end,
        fun() -> check_user_max_wall(User, JobSpec) end
    ],
    run_checks(Checks).

%% @doc Run a list of check functions, returning first error or ok
-spec run_checks([fun(() -> ok | {error, term()})]) -> ok | {error, term()}.
run_checks([]) ->
    ok;
run_checks([Check | Rest]) ->
    case Check() of
        ok -> run_checks(Rest);
        Error -> Error
    end.

%%====================================================================
%% Association Limit Checks
%%====================================================================

check_assoc_max_jobs(#association{max_jobs = 0}) ->
    ok;  % 0 = unlimited
check_assoc_max_jobs(#association{max_jobs = MaxJobs, user = User, account = Account}) ->
    %% Get current running job count for this association
    CurrentJobs = get_running_jobs_count(User, Account),
    case CurrentJobs >= MaxJobs of
        true ->
            {error, {assoc_max_jobs_exceeded, CurrentJobs, MaxJobs}};
        false ->
            ok
    end.

check_assoc_max_submit(#association{max_submit = 0}) ->
    ok;
check_assoc_max_submit(#association{max_submit = MaxSubmit, user = User, account = Account}) ->
    %% Get current total job count (running + pending) for this association
    TotalJobs = get_total_jobs_count(User, Account),
    case TotalJobs >= MaxSubmit of
        true ->
            {error, {assoc_max_submit_exceeded, TotalJobs, MaxSubmit}};
        false ->
            ok
    end.

check_assoc_max_wall(#association{max_wall_per_job = 0}, _JobSpec) ->
    ok;
check_assoc_max_wall(#association{max_wall_per_job = MaxWall}, JobSpec) ->
    RequestedWall = maps:get(time_limit, JobSpec, 0),
    case RequestedWall > MaxWall of
        true ->
            {error, {assoc_max_wall_exceeded, RequestedWall, MaxWall}};
        false ->
            ok
    end.

check_assoc_tres_per_job(#association{max_tres_per_job = MaxTres}, JobSpec) when map_size(MaxTres) =:= 0 ->
    _ = JobSpec, % Suppress unused variable warning
    ok;
check_assoc_tres_per_job(#association{max_tres_per_job = MaxTres}, JobSpec) ->
    RequestedTres = build_tres_request(JobSpec),
    check_tres_limits(RequestedTres, MaxTres, assoc_tres_per_job).

check_assoc_tres_per_node(#association{max_tres_per_node = MaxTres}, JobSpec) when map_size(MaxTres) =:= 0 ->
    _ = JobSpec, % Suppress unused variable warning
    ok;
check_assoc_tres_per_node(#association{max_tres_per_node = MaxTres}, JobSpec) ->
    %% Calculate per-node resources
    NumNodes = maps:get(num_nodes, JobSpec, 1),
    RequestedTres = build_tres_request(JobSpec),
    PerNodeTres = maps:map(fun(_K, V) -> V div max(1, NumNodes) end, RequestedTres),
    check_tres_limits(PerNodeTres, MaxTres, assoc_tres_per_node).

check_assoc_grp_tres(#association{grp_tres = GrpTres}, JobSpec) when map_size(GrpTres) =:= 0 ->
    _ = JobSpec, % Suppress unused variable warning
    ok;
check_assoc_grp_tres(#association{grp_tres = GrpTres, user = User, account = Account}, JobSpec) ->
    %% Check if adding this job would exceed group TRES limits
    RequestedTres = build_tres_request(JobSpec),
    CurrentTres = get_current_tres_usage(User, Account),
    CombinedTres = combine_tres_maps(CurrentTres, RequestedTres),
    check_tres_limits(CombinedTres, GrpTres, assoc_grp_tres).

%%====================================================================
%% Account Limit Checks
%%====================================================================

check_account_max_jobs(#account{max_jobs = 0}) ->
    ok;
check_account_max_jobs(#account{max_jobs = MaxJobs, name = AccountName}) ->
    CurrentJobs = get_account_running_jobs_count(AccountName),
    case CurrentJobs >= MaxJobs of
        true ->
            {error, {account_max_jobs_exceeded, CurrentJobs, MaxJobs}};
        false ->
            ok
    end.

check_account_max_submit(#account{max_submit = 0}) ->
    ok;
check_account_max_submit(#account{max_submit = MaxSubmit, name = AccountName}) ->
    TotalJobs = get_account_total_jobs_count(AccountName),
    case TotalJobs >= MaxSubmit of
        true ->
            {error, {account_max_submit_exceeded, TotalJobs, MaxSubmit}};
        false ->
            ok
    end.

check_account_max_wall(#account{max_wall = 0}, _JobSpec) ->
    ok;
check_account_max_wall(#account{max_wall = MaxWall}, JobSpec) ->
    %% Account max_wall is in minutes, job time_limit is in seconds
    RequestedWall = maps:get(time_limit, JobSpec, 0),
    MaxWallSeconds = MaxWall * 60,
    case RequestedWall > MaxWallSeconds of
        true ->
            {error, {account_max_wall_exceeded, RequestedWall, MaxWallSeconds}};
        false ->
            ok
    end.

%%====================================================================
%% User Limit Checks
%%====================================================================

check_user_max_jobs(#acct_user{max_jobs = 0}) ->
    ok;
check_user_max_jobs(#acct_user{max_jobs = MaxJobs, name = Username}) ->
    CurrentJobs = get_user_running_jobs_count(Username),
    case CurrentJobs >= MaxJobs of
        true ->
            {error, {user_max_jobs_exceeded, CurrentJobs, MaxJobs}};
        false ->
            ok
    end.

check_user_max_submit(#acct_user{max_submit = 0}) ->
    ok;
check_user_max_submit(#acct_user{max_submit = MaxSubmit, name = Username}) ->
    TotalJobs = get_user_total_jobs_count(Username),
    case TotalJobs >= MaxSubmit of
        true ->
            {error, {user_max_submit_exceeded, TotalJobs, MaxSubmit}};
        false ->
            ok
    end.

check_user_max_wall(#acct_user{max_wall = 0}, _JobSpec) ->
    ok;
check_user_max_wall(#acct_user{max_wall = MaxWall}, JobSpec) ->
    %% User max_wall is in minutes, job time_limit is in seconds
    RequestedWall = maps:get(time_limit, JobSpec, 0),
    MaxWallSeconds = MaxWall * 60,
    case RequestedWall > MaxWallSeconds of
        true ->
            {error, {user_max_wall_exceeded, RequestedWall, MaxWallSeconds}};
        false ->
            ok
    end.

%%====================================================================
%% TRES Helpers
%%====================================================================

%% @doc Build a TRES request map from a job spec
-spec build_tres_request(map()) -> map().
build_tres_request(JobSpec) ->
    NumNodes = maps:get(num_nodes, JobSpec, 1),
    NumCpus = maps:get(num_cpus, JobSpec, 1),
    MemoryMb = maps:get(memory_mb, JobSpec, 0),
    NumGpus = maps:get(num_gpus, JobSpec, 0),

    %% Build TRES map with standard TRES types
    Tres = #{
        <<"cpu">> => NumCpus,
        <<"mem">> => MemoryMb,
        <<"node">> => NumNodes
    },

    %% Add GPU if requested
    case NumGpus of
        0 -> Tres;
        N -> maps:put(<<"gres/gpu">>, N, Tres)
    end.

%% @doc Check if requested TRES exceeds limits
-spec check_tres_limits(map(), map(), atom()) -> ok | {error, term()}.
check_tres_limits(Requested, Limits, LimitType) ->
    Violations = maps:fold(fun(TresType, RequestedAmount, Acc) ->
        case maps:get(TresType, Limits, 0) of
            0 -> Acc;  % 0 = unlimited
            MaxAmount when RequestedAmount > MaxAmount ->
                [{TresType, RequestedAmount, MaxAmount} | Acc];
            _ ->
                Acc
        end
    end, [], Requested),

    case Violations of
        [] -> ok;
        _ -> {error, {LimitType, Violations}}
    end.

%% @doc Combine two TRES maps
-spec combine_tres_maps(map(), map()) -> map().
combine_tres_maps(Map1, Map2) ->
    maps:fold(fun(K, V, Acc) ->
        maps:update_with(K, fun(Existing) -> Existing + V end, V, Acc)
    end, Map1, Map2).

%%====================================================================
%% Usage Query Helpers
%% These functions query current job/resource usage from flurm_limits
%%====================================================================

%% @doc Get running jobs count for a user+account association
-spec get_running_jobs_count(binary(), binary()) -> non_neg_integer().
get_running_jobs_count(User, Account) ->
    case catch flurm_limits:get_usage(user, User) of
        #usage{running_jobs = Count} when is_map(Account) == false, Account =/= <<>> ->
            %% If we have account-specific tracking, use it
            Count;
        #usage{running_jobs = Count} ->
            Count;
        _ ->
            0
    end.

%% @doc Get total jobs count (running + pending) for a user+account
-spec get_total_jobs_count(binary(), binary()) -> non_neg_integer().
get_total_jobs_count(User, _Account) ->
    case catch flurm_limits:get_usage(user, User) of
        #usage{running_jobs = Running, pending_jobs = Pending} ->
            Running + Pending;
        _ ->
            0
    end.

%% @doc Get current TRES usage for a user+account
-spec get_current_tres_usage(binary(), binary()) -> map().
get_current_tres_usage(User, _Account) ->
    case catch flurm_limits:get_usage(user, User) of
        #usage{tres_used = Tres} ->
            Tres;
        _ ->
            #{}
    end.

%% @doc Get running jobs count for an account
-spec get_account_running_jobs_count(binary()) -> non_neg_integer().
get_account_running_jobs_count(AccountName) ->
    case catch flurm_limits:get_usage(account, AccountName) of
        #usage{running_jobs = Count} ->
            Count;
        _ ->
            0
    end.

%% @doc Get total jobs count for an account
-spec get_account_total_jobs_count(binary()) -> non_neg_integer().
get_account_total_jobs_count(AccountName) ->
    case catch flurm_limits:get_usage(account, AccountName) of
        #usage{running_jobs = Running, pending_jobs = Pending} ->
            Running + Pending;
        _ ->
            0
    end.

%% @doc Get running jobs count for a user (across all accounts)
-spec get_user_running_jobs_count(binary()) -> non_neg_integer().
get_user_running_jobs_count(Username) ->
    case catch flurm_limits:get_usage(user, Username) of
        #usage{running_jobs = Count} ->
            Count;
        _ ->
            0
    end.

%% @doc Get total jobs count for a user (across all accounts)
-spec get_user_total_jobs_count(binary()) -> non_neg_integer().
get_user_total_jobs_count(Username) ->
    case catch flurm_limits:get_usage(user, Username) of
        #usage{running_jobs = Running, pending_jobs = Pending} ->
            Running + Pending;
        _ ->
            0
    end.
