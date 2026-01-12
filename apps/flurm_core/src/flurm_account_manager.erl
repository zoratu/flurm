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

-include_lib("flurm_core/include/flurm_core.hrl").

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

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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

handle_call({check_limits, _Username, _JobSpec}, _From, State) ->
    %% TODO: Implement limit checking
    %% For now, always allow
    {reply, ok, State};

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
