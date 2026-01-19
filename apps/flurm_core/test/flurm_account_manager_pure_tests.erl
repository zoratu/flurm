%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_account_manager
%%%
%%% These tests directly test gen_server callbacks and internal logic
%%% WITHOUT using meck or any mocking framework. Tests work directly
%%% with the state record and ETS tables.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_account_manager_pure_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%% Mirror the internal state record for direct testing
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
%% Test Helpers - Create isolated state for testing
%%====================================================================

%% Create a fresh state with ETS tables for isolated testing
create_test_state() ->
    Accounts = ets:new(test_accounts, [set, public, {keypos, #account.name}]),
    Users = ets:new(test_users, [set, public, {keypos, #acct_user.name}]),
    Associations = ets:new(test_associations, [set, public, {keypos, #association.id}]),
    Qos = ets:new(test_qos, [set, public, {keypos, #qos.name}]),
    Clusters = ets:new(test_clusters, [set, public, {keypos, #acct_cluster.name}]),
    Tres = ets:new(test_tres, [set, public, {keypos, #tres.id}]),

    %% Insert default QOS
    ets:insert(Qos, #qos{name = <<"normal">>, description = <<"Default QOS">>}),

    %% Insert default TRES
    ets:insert(Tres, #tres{id = 1, type = <<"cpu">>}),
    ets:insert(Tres, #tres{id = 2, type = <<"mem">>}),
    ets:insert(Tres, #tres{id = 3, type = <<"energy">>}),
    ets:insert(Tres, #tres{id = 4, type = <<"node">>}),

    %% Insert default cluster
    ets:insert(Clusters, #acct_cluster{name = <<"flurm">>}),

    %% Insert root account
    ets:insert(Accounts, #account{name = <<"root">>, description = <<"Root account">>}),

    #state{
        accounts = Accounts,
        users = Users,
        associations = Associations,
        qos = Qos,
        clusters = Clusters,
        tres = Tres,
        next_assoc_id = 1,
        next_tres_id = 5
    }.

%% Clean up test state
cleanup_test_state(#state{accounts = A, users = U, associations = As,
                          qos = Q, clusters = C, tres = T}) ->
    catch ets:delete(A),
    catch ets:delete(U),
    catch ets:delete(As),
    catch ets:delete(Q),
    catch ets:delete(C),
    catch ets:delete(T),
    ok.

%%====================================================================
%% Direct handle_call Tests - Account Operations
%%====================================================================

handle_call_account_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_handle_call_add_account/1,
      fun test_handle_call_add_account_already_exists/1,
      fun test_handle_call_modify_account/1,
      fun test_handle_call_modify_account_not_found/1,
      fun test_handle_call_delete_account/1,
      fun test_handle_call_delete_account_not_found/1,
      fun test_handle_call_delete_account_has_children/1,
      fun test_handle_call_get_account/1,
      fun test_handle_call_get_account_not_found/1,
      fun test_handle_call_list_accounts/1,
      fun test_handle_call_list_accounts_filtered/1
     ]}.

test_handle_call_add_account(State) ->
    {"add_account via handle_call", fun() ->
        Account = #account{name = <<"new_account">>, description = <<"Test">>},
        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_account, Account}, {self(), make_ref()}, State),
        %% Verify it's in ETS
        [Retrieved] = ets:lookup(NewState#state.accounts, <<"new_account">>),
        ?assertEqual(<<"new_account">>, Retrieved#account.name)
    end}.

test_handle_call_add_account_already_exists(State) ->
    {"add_account already exists", fun() ->
        Account = #account{name = <<"root">>},  % root exists by default
        {reply, Result, _} = flurm_account_manager:handle_call(
            {add_account, Account}, {self(), make_ref()}, State),
        ?assertEqual({error, already_exists}, Result)
    end}.

test_handle_call_modify_account(State) ->
    {"modify_account via handle_call", fun() ->
        Updates = #{description => <<"Modified">>, max_jobs => 100},
        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {modify_account, <<"root">>, Updates}, {self(), make_ref()}, State),
        [Modified] = ets:lookup(NewState#state.accounts, <<"root">>),
        ?assertEqual(<<"Modified">>, Modified#account.description),
        ?assertEqual(100, Modified#account.max_jobs)
    end}.

test_handle_call_modify_account_not_found(State) ->
    {"modify_account not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {modify_account, <<"nonexistent">>, #{description => <<"test">>}},
            {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_delete_account(State) ->
    {"delete_account via handle_call", fun() ->
        %% Add an account to delete
        Account = #account{name = <<"to_delete">>},
        ets:insert(State#state.accounts, Account),

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {delete_account, <<"to_delete">>}, {self(), make_ref()}, State),
        ?assertEqual([], ets:lookup(NewState#state.accounts, <<"to_delete">>))
    end}.

test_handle_call_delete_account_not_found(State) ->
    {"delete_account not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {delete_account, <<"nonexistent">>}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_delete_account_has_children(State) ->
    {"delete_account with children fails", fun() ->
        %% Add parent and child
        ets:insert(State#state.accounts, #account{name = <<"parent">>}),
        ets:insert(State#state.accounts, #account{name = <<"child">>, parent = <<"parent">>}),

        {reply, Result, _} = flurm_account_manager:handle_call(
            {delete_account, <<"parent">>}, {self(), make_ref()}, State),
        ?assertEqual({error, has_children}, Result)
    end}.

test_handle_call_get_account(State) ->
    {"get_account via handle_call", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_account, <<"root">>}, {self(), make_ref()}, State),
        ?assertMatch({ok, #account{name = <<"root">>}}, Result)
    end}.

test_handle_call_get_account_not_found(State) ->
    {"get_account not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_account, <<"nonexistent">>}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_list_accounts(State) ->
    {"list_accounts via handle_call", fun() ->
        {reply, Accounts, _} = flurm_account_manager:handle_call(
            list_accounts, {self(), make_ref()}, State),
        ?assert(is_list(Accounts)),
        Names = [A#account.name || A <- Accounts],
        ?assert(lists:member(<<"root">>, Names))
    end}.

test_handle_call_list_accounts_filtered(State) ->
    {"list_accounts filtered via handle_call", fun() ->
        %% Add accounts with different organizations
        ets:insert(State#state.accounts, #account{name = <<"org1_acct">>, organization = <<"Org1">>}),
        ets:insert(State#state.accounts, #account{name = <<"org2_acct">>, organization = <<"Org2">>}),

        {reply, Filtered, _} = flurm_account_manager:handle_call(
            {list_accounts, #{organization => <<"Org1">>}}, {self(), make_ref()}, State),
        Names = [A#account.name || A <- Filtered],
        ?assert(lists:member(<<"org1_acct">>, Names)),
        ?assertNot(lists:member(<<"org2_acct">>, Names))
    end}.

%%====================================================================
%% Direct handle_call Tests - User Operations
%%====================================================================

handle_call_user_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_handle_call_add_user/1,
      fun test_handle_call_add_user_already_exists/1,
      fun test_handle_call_modify_user/1,
      fun test_handle_call_modify_user_not_found/1,
      fun test_handle_call_delete_user/1,
      fun test_handle_call_delete_user_not_found/1,
      fun test_handle_call_get_user/1,
      fun test_handle_call_get_user_not_found/1,
      fun test_handle_call_list_users/1,
      fun test_handle_call_list_users_filtered/1
     ]}.

test_handle_call_add_user(State) ->
    {"add_user via handle_call", fun() ->
        User = #acct_user{name = <<"testuser">>, default_account = <<"root">>},
        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_user, User}, {self(), make_ref()}, State),
        [Retrieved] = ets:lookup(NewState#state.users, <<"testuser">>),
        ?assertEqual(<<"testuser">>, Retrieved#acct_user.name)
    end}.

test_handle_call_add_user_already_exists(State) ->
    {"add_user already exists", fun() ->
        User = #acct_user{name = <<"dupuser">>},
        ets:insert(State#state.users, User),
        {reply, Result, _} = flurm_account_manager:handle_call(
            {add_user, User}, {self(), make_ref()}, State),
        ?assertEqual({error, already_exists}, Result)
    end}.

test_handle_call_modify_user(State) ->
    {"modify_user via handle_call", fun() ->
        User = #acct_user{name = <<"moduser">>},
        ets:insert(State#state.users, User),

        Updates = #{default_account => <<"root">>, max_jobs => 50},
        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {modify_user, <<"moduser">>, Updates}, {self(), make_ref()}, State),
        [Modified] = ets:lookup(NewState#state.users, <<"moduser">>),
        ?assertEqual(<<"root">>, Modified#acct_user.default_account),
        ?assertEqual(50, Modified#acct_user.max_jobs)
    end}.

test_handle_call_modify_user_not_found(State) ->
    {"modify_user not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {modify_user, <<"nonexistent">>, #{max_jobs => 10}},
            {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_delete_user(State) ->
    {"delete_user via handle_call", fun() ->
        User = #acct_user{name = <<"deluser">>},
        ets:insert(State#state.users, User),

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {delete_user, <<"deluser">>}, {self(), make_ref()}, State),
        ?assertEqual([], ets:lookup(NewState#state.users, <<"deluser">>))
    end}.

test_handle_call_delete_user_not_found(State) ->
    {"delete_user not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {delete_user, <<"nonexistent">>}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_get_user(State) ->
    {"get_user via handle_call", fun() ->
        User = #acct_user{name = <<"getuser">>},
        ets:insert(State#state.users, User),

        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_user, <<"getuser">>}, {self(), make_ref()}, State),
        ?assertMatch({ok, #acct_user{name = <<"getuser">>}}, Result)
    end}.

test_handle_call_get_user_not_found(State) ->
    {"get_user not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_user, <<"nonexistent">>}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_list_users(State) ->
    {"list_users via handle_call", fun() ->
        ets:insert(State#state.users, #acct_user{name = <<"user1">>}),
        ets:insert(State#state.users, #acct_user{name = <<"user2">>}),

        {reply, Users, _} = flurm_account_manager:handle_call(
            list_users, {self(), make_ref()}, State),
        ?assert(is_list(Users)),
        ?assertEqual(2, length(Users))
    end}.

test_handle_call_list_users_filtered(State) ->
    {"list_users filtered via handle_call", fun() ->
        ets:insert(State#state.users, #acct_user{name = <<"admin">>, admin_level = admin}),
        ets:insert(State#state.users, #acct_user{name = <<"regular">>, admin_level = none}),

        {reply, Filtered, _} = flurm_account_manager:handle_call(
            {list_users, #{admin_level => admin}}, {self(), make_ref()}, State),
        Names = [U#acct_user.name || U <- Filtered],
        ?assert(lists:member(<<"admin">>, Names)),
        ?assertNot(lists:member(<<"regular">>, Names))
    end}.

%%====================================================================
%% Direct handle_call Tests - Association Operations
%%====================================================================

handle_call_association_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_handle_call_add_association/1,
      fun test_handle_call_modify_association/1,
      fun test_handle_call_modify_association_not_found/1,
      fun test_handle_call_delete_association/1,
      fun test_handle_call_delete_association_not_found/1,
      fun test_handle_call_get_association_by_id/1,
      fun test_handle_call_get_association_by_id_not_found/1,
      fun test_handle_call_get_association_by_key/1,
      fun test_handle_call_get_association_by_key_not_found/1,
      fun test_handle_call_list_associations/1,
      fun test_handle_call_list_associations_filtered/1,
      fun test_handle_call_get_user_association/1,
      fun test_handle_call_get_user_association_not_found/1
     ]}.

test_handle_call_add_association(State) ->
    {"add_association via handle_call", fun() ->
        Assoc = #association{cluster = <<"flurm">>, account = <<"root">>, user = <<"assocuser">>},
        {reply, {ok, Id}, NewState} = flurm_account_manager:handle_call(
            {add_association, Assoc}, {self(), make_ref()}, State),
        ?assertEqual(1, Id),
        ?assertEqual(2, NewState#state.next_assoc_id),
        [Retrieved] = ets:lookup(NewState#state.associations, Id),
        ?assertEqual(<<"assocuser">>, Retrieved#association.user)
    end}.

test_handle_call_modify_association(State) ->
    {"modify_association via handle_call", fun() ->
        %% Add an association first
        Assoc = #association{id = 1, cluster = <<"flurm">>, account = <<"root">>, user = <<"modassoc">>},
        ets:insert(State#state.associations, Assoc),

        Updates = #{shares => 10, max_jobs => 25},
        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {modify_association, 1, Updates}, {self(), make_ref()}, State),
        [Modified] = ets:lookup(NewState#state.associations, 1),
        ?assertEqual(10, Modified#association.shares),
        ?assertEqual(25, Modified#association.max_jobs)
    end}.

test_handle_call_modify_association_not_found(State) ->
    {"modify_association not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {modify_association, 99999, #{shares => 5}}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_delete_association(State) ->
    {"delete_association via handle_call", fun() ->
        Assoc = #association{id = 1, cluster = <<"flurm">>, account = <<"root">>, user = <<"delassoc">>},
        ets:insert(State#state.associations, Assoc),

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {delete_association, 1}, {self(), make_ref()}, State),
        ?assertEqual([], ets:lookup(NewState#state.associations, 1))
    end}.

test_handle_call_delete_association_not_found(State) ->
    {"delete_association not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {delete_association, 99999}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_get_association_by_id(State) ->
    {"get_association by id via handle_call", fun() ->
        Assoc = #association{id = 1, cluster = <<"flurm">>, account = <<"root">>, user = <<"getassoc">>},
        ets:insert(State#state.associations, Assoc),

        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_association, 1}, {self(), make_ref()}, State),
        ?assertMatch({ok, #association{id = 1}}, Result)
    end}.

test_handle_call_get_association_by_id_not_found(State) ->
    {"get_association by id not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_association, 99999}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_get_association_by_key(State) ->
    {"get_association by key via handle_call", fun() ->
        Assoc = #association{id = 1, cluster = <<"flurm">>, account = <<"root">>, user = <<"keyuser">>},
        ets:insert(State#state.associations, Assoc),

        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_association_by_key, <<"flurm">>, <<"root">>, <<"keyuser">>},
            {self(), make_ref()}, State),
        ?assertMatch({ok, #association{user = <<"keyuser">>}}, Result)
    end}.

test_handle_call_get_association_by_key_not_found(State) ->
    {"get_association by key not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_association_by_key, <<"nonexistent">>, <<"none">>, <<"nouser">>},
            {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_list_associations(State) ->
    {"list_associations via handle_call", fun() ->
        ets:insert(State#state.associations,
            #association{id = 1, cluster = <<"flurm">>, account = <<"root">>, user = <<"list1">>}),
        ets:insert(State#state.associations,
            #association{id = 2, cluster = <<"flurm">>, account = <<"root">>, user = <<"list2">>}),

        {reply, Assocs, _} = flurm_account_manager:handle_call(
            list_associations, {self(), make_ref()}, State),
        ?assert(is_list(Assocs)),
        ?assertEqual(2, length(Assocs))
    end}.

test_handle_call_list_associations_filtered(State) ->
    {"list_associations filtered via handle_call", fun() ->
        ets:insert(State#state.associations,
            #association{id = 1, cluster = <<"cluster_a">>, account = <<"root">>, user = <<"u1">>}),
        ets:insert(State#state.associations,
            #association{id = 2, cluster = <<"cluster_b">>, account = <<"root">>, user = <<"u2">>}),

        {reply, Filtered, _} = flurm_account_manager:handle_call(
            {list_associations, #{cluster => <<"cluster_a">>}}, {self(), make_ref()}, State),
        Users = [A#association.user || A <- Filtered],
        ?assert(lists:member(<<"u1">>, Users)),
        ?assertNot(lists:member(<<"u2">>, Users))
    end}.

test_handle_call_get_user_association(State) ->
    {"get_user_association via handle_call", fun() ->
        Assoc = #association{id = 1, cluster = <<"flurm">>, account = <<"root">>, user = <<"ua_user">>},
        ets:insert(State#state.associations, Assoc),

        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_user_association, <<"ua_user">>, <<"root">>}, {self(), make_ref()}, State),
        ?assertMatch({ok, #association{user = <<"ua_user">>}}, Result)
    end}.

test_handle_call_get_user_association_not_found(State) ->
    {"get_user_association not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_user_association, <<"nonexistent">>, <<"noaccout">>},
            {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

%%====================================================================
%% Direct handle_call Tests - QOS Operations
%%====================================================================

handle_call_qos_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_handle_call_add_qos/1,
      fun test_handle_call_add_qos_already_exists/1,
      fun test_handle_call_modify_qos/1,
      fun test_handle_call_modify_qos_not_found/1,
      fun test_handle_call_delete_qos/1,
      fun test_handle_call_delete_qos_not_found/1,
      fun test_handle_call_get_qos/1,
      fun test_handle_call_get_qos_not_found/1,
      fun test_handle_call_list_qos/1
     ]}.

test_handle_call_add_qos(State) ->
    {"add_qos via handle_call", fun() ->
        Qos = #qos{name = <<"high">>, priority = 1000},
        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_qos, Qos}, {self(), make_ref()}, State),
        [Retrieved] = ets:lookup(NewState#state.qos, <<"high">>),
        ?assertEqual(<<"high">>, Retrieved#qos.name)
    end}.

test_handle_call_add_qos_already_exists(State) ->
    {"add_qos already exists", fun() ->
        Qos = #qos{name = <<"normal">>},  % normal exists by default
        {reply, Result, _} = flurm_account_manager:handle_call(
            {add_qos, Qos}, {self(), make_ref()}, State),
        ?assertEqual({error, already_exists}, Result)
    end}.

test_handle_call_modify_qos(State) ->
    {"modify_qos via handle_call", fun() ->
        Updates = #{priority => 500, description => <<"Modified">>},
        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {modify_qos, <<"normal">>, Updates}, {self(), make_ref()}, State),
        [Modified] = ets:lookup(NewState#state.qos, <<"normal">>),
        ?assertEqual(500, Modified#qos.priority),
        ?assertEqual(<<"Modified">>, Modified#qos.description)
    end}.

test_handle_call_modify_qos_not_found(State) ->
    {"modify_qos not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {modify_qos, <<"nonexistent">>, #{priority => 100}},
            {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_delete_qos(State) ->
    {"delete_qos via handle_call", fun() ->
        ets:insert(State#state.qos, #qos{name = <<"to_delete">>}),

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {delete_qos, <<"to_delete">>}, {self(), make_ref()}, State),
        ?assertEqual([], ets:lookup(NewState#state.qos, <<"to_delete">>))
    end}.

test_handle_call_delete_qos_not_found(State) ->
    {"delete_qos not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {delete_qos, <<"nonexistent">>}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_get_qos(State) ->
    {"get_qos via handle_call", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_qos, <<"normal">>}, {self(), make_ref()}, State),
        ?assertMatch({ok, #qos{name = <<"normal">>}}, Result)
    end}.

test_handle_call_get_qos_not_found(State) ->
    {"get_qos not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_qos, <<"nonexistent">>}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_list_qos(State) ->
    {"list_qos via handle_call", fun() ->
        ets:insert(State#state.qos, #qos{name = <<"extra">>}),

        {reply, QosList, _} = flurm_account_manager:handle_call(
            list_qos, {self(), make_ref()}, State),
        ?assert(is_list(QosList)),
        Names = [Q#qos.name || Q <- QosList],
        ?assert(lists:member(<<"normal">>, Names)),
        ?assert(lists:member(<<"extra">>, Names))
    end}.

%%====================================================================
%% Direct handle_call Tests - Cluster Operations
%%====================================================================

handle_call_cluster_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_handle_call_add_cluster/1,
      fun test_handle_call_add_cluster_already_exists/1,
      fun test_handle_call_get_cluster/1,
      fun test_handle_call_get_cluster_not_found/1,
      fun test_handle_call_list_clusters/1
     ]}.

test_handle_call_add_cluster(State) ->
    {"add_cluster via handle_call", fun() ->
        Cluster = #acct_cluster{name = <<"newcluster">>, control_host = <<"host1">>},
        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_cluster, Cluster}, {self(), make_ref()}, State),
        [Retrieved] = ets:lookup(NewState#state.clusters, <<"newcluster">>),
        ?assertEqual(<<"newcluster">>, Retrieved#acct_cluster.name)
    end}.

test_handle_call_add_cluster_already_exists(State) ->
    {"add_cluster already exists", fun() ->
        Cluster = #acct_cluster{name = <<"flurm">>},  % flurm exists by default
        {reply, Result, _} = flurm_account_manager:handle_call(
            {add_cluster, Cluster}, {self(), make_ref()}, State),
        ?assertEqual({error, already_exists}, Result)
    end}.

test_handle_call_get_cluster(State) ->
    {"get_cluster via handle_call", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_cluster, <<"flurm">>}, {self(), make_ref()}, State),
        ?assertMatch({ok, #acct_cluster{name = <<"flurm">>}}, Result)
    end}.

test_handle_call_get_cluster_not_found(State) ->
    {"get_cluster not found", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_cluster, <<"nonexistent">>}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_list_clusters(State) ->
    {"list_clusters via handle_call", fun() ->
        ets:insert(State#state.clusters, #acct_cluster{name = <<"extra_cluster">>}),

        {reply, Clusters, _} = flurm_account_manager:handle_call(
            list_clusters, {self(), make_ref()}, State),
        ?assert(is_list(Clusters)),
        Names = [C#acct_cluster.name || C <- Clusters],
        ?assert(lists:member(<<"flurm">>, Names)),
        ?assert(lists:member(<<"extra_cluster">>, Names))
    end}.

%%====================================================================
%% Direct handle_call Tests - TRES Operations
%%====================================================================

handle_call_tres_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_handle_call_add_tres/1,
      fun test_handle_call_get_tres_by_id/1,
      fun test_handle_call_get_tres_by_type/1,
      fun test_handle_call_get_tres_not_found_by_id/1,
      fun test_handle_call_get_tres_not_found_by_type/1,
      fun test_handle_call_list_tres/1
     ]}.

test_handle_call_add_tres(State) ->
    {"add_tres via handle_call", fun() ->
        Tres = #tres{type = <<"gres/gpu">>, name = <<"v100">>},
        {reply, {ok, Id}, NewState} = flurm_account_manager:handle_call(
            {add_tres, Tres}, {self(), make_ref()}, State),
        ?assertEqual(5, Id),  % next after default 1-4
        ?assertEqual(6, NewState#state.next_tres_id),
        [Retrieved] = ets:lookup(NewState#state.tres, Id),
        ?assertEqual(<<"gres/gpu">>, Retrieved#tres.type)
    end}.

test_handle_call_get_tres_by_id(State) ->
    {"get_tres by id via handle_call", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_tres, 1}, {self(), make_ref()}, State),
        ?assertMatch({ok, #tres{id = 1, type = <<"cpu">>}}, Result)
    end}.

test_handle_call_get_tres_by_type(State) ->
    {"get_tres by type via handle_call", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_tres, <<"mem">>}, {self(), make_ref()}, State),
        ?assertMatch({ok, #tres{type = <<"mem">>}}, Result)
    end}.

test_handle_call_get_tres_not_found_by_id(State) ->
    {"get_tres not found by id", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_tres, 99999}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_get_tres_not_found_by_type(State) ->
    {"get_tres not found by type", fun() ->
        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_tres, <<"nonexistent">>}, {self(), make_ref()}, State),
        ?assertEqual({error, not_found}, Result)
    end}.

test_handle_call_list_tres(State) ->
    {"list_tres via handle_call", fun() ->
        {reply, TresList, _} = flurm_account_manager:handle_call(
            list_tres, {self(), make_ref()}, State),
        ?assert(is_list(TresList)),
        ?assertEqual(4, length(TresList)),  % cpu, mem, energy, node
        Types = [T#tres.type || T <- TresList],
        ?assert(lists:member(<<"cpu">>, Types)),
        ?assert(lists:member(<<"mem">>, Types))
    end}.

%%====================================================================
%% handle_call Unknown Request Test
%%====================================================================

handle_call_unknown_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_handle_call_unknown_request/1
     ]}.

test_handle_call_unknown_request(State) ->
    {"unknown request returns error", fun() ->
        {reply, Result, ReturnedState} = flurm_account_manager:handle_call(
            {unknown_request, foo, bar}, {self(), make_ref()}, State),
        ?assertEqual({error, unknown_request}, Result),
        %% State should be unchanged
        ?assertEqual(State#state.next_assoc_id, ReturnedState#state.next_assoc_id)
    end}.

%%====================================================================
%% handle_cast Tests
%%====================================================================

handle_cast_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_handle_cast_unknown/1
     ]}.

test_handle_cast_unknown(State) ->
    {"handle_cast ignores unknown messages", fun() ->
        {noreply, ReturnedState} = flurm_account_manager:handle_cast(
            {unknown_cast, foo}, State),
        %% State should be unchanged
        ?assertEqual(State#state.next_assoc_id, ReturnedState#state.next_assoc_id)
    end}.

%%====================================================================
%% handle_info Tests
%%====================================================================

handle_info_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_handle_info_unknown/1
     ]}.

test_handle_info_unknown(State) ->
    {"handle_info ignores unknown messages", fun() ->
        {noreply, ReturnedState} = flurm_account_manager:handle_info(
            {unknown_info, foo, bar}, State),
        %% State should be unchanged
        ?assertEqual(State#state.next_assoc_id, ReturnedState#state.next_assoc_id)
    end}.

%%====================================================================
%% terminate Tests
%%====================================================================

terminate_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_terminate_normal/1,
      fun test_terminate_shutdown/1,
      fun test_terminate_with_reason/1
     ]}.

test_terminate_normal(State) ->
    {"terminate returns ok for normal", fun() ->
        Result = flurm_account_manager:terminate(normal, State),
        ?assertEqual(ok, Result)
    end}.

test_terminate_shutdown(State) ->
    {"terminate returns ok for shutdown", fun() ->
        Result = flurm_account_manager:terminate(shutdown, State),
        ?assertEqual(ok, Result)
    end}.

test_terminate_with_reason(State) ->
    {"terminate returns ok for any reason", fun() ->
        Result = flurm_account_manager:terminate({error, some_reason}, State),
        ?assertEqual(ok, Result)
    end}.

%%====================================================================
%% Comprehensive Account Field Update Tests
%%====================================================================

account_field_updates_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_update_account_all_fields/1,
      fun test_update_account_unknown_field_ignored/1
     ]}.

test_update_account_all_fields(State) ->
    {"update all account fields via handle_call", fun() ->
        %% Add account to modify
        ets:insert(State#state.accounts, #account{name = <<"fullmod">>}),

        Updates = #{
            description => <<"Full description">>,
            organization => <<"Full Org">>,
            parent => <<"root">>,
            coordinators => [<<"coord1">>, <<"coord2">>],
            default_qos => <<"high">>,
            fairshare => 10,
            max_jobs => 200,
            max_submit => 500,
            max_wall => 14400
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {modify_account, <<"fullmod">>, Updates}, {self(), make_ref()}, State),

        [Modified] = ets:lookup(NewState#state.accounts, <<"fullmod">>),
        ?assertEqual(<<"Full description">>, Modified#account.description),
        ?assertEqual(<<"Full Org">>, Modified#account.organization),
        ?assertEqual(<<"root">>, Modified#account.parent),
        ?assertEqual([<<"coord1">>, <<"coord2">>], Modified#account.coordinators),
        ?assertEqual(<<"high">>, Modified#account.default_qos),
        ?assertEqual(10, Modified#account.fairshare),
        ?assertEqual(200, Modified#account.max_jobs),
        ?assertEqual(500, Modified#account.max_submit),
        ?assertEqual(14400, Modified#account.max_wall)
    end}.

test_update_account_unknown_field_ignored(State) ->
    {"unknown update fields are ignored", fun() ->
        ets:insert(State#state.accounts, #account{name = <<"ignoretest">>}),

        Updates = #{
            description => <<"Valid">>,
            unknown_field => <<"Ignored">>,
            another_unknown => 12345
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {modify_account, <<"ignoretest">>, Updates}, {self(), make_ref()}, State),

        [Modified] = ets:lookup(NewState#state.accounts, <<"ignoretest">>),
        ?assertEqual(<<"Valid">>, Modified#account.description)
    end}.

%%====================================================================
%% Comprehensive User Field Update Tests
%%====================================================================

user_field_updates_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_update_user_all_fields/1
     ]}.

test_update_user_all_fields(State) ->
    {"update all user fields via handle_call", fun() ->
        ets:insert(State#state.users, #acct_user{name = <<"fulluser">>}),

        Updates = #{
            default_account => <<"root">>,
            accounts => [<<"root">>, <<"other">>],
            default_qos => <<"premium">>,
            admin_level => admin,
            fairshare => 5,
            max_jobs => 100,
            max_submit => 250,
            max_wall => 7200
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {modify_user, <<"fulluser">>, Updates}, {self(), make_ref()}, State),

        [Modified] = ets:lookup(NewState#state.users, <<"fulluser">>),
        ?assertEqual(<<"root">>, Modified#acct_user.default_account),
        ?assertEqual([<<"root">>, <<"other">>], Modified#acct_user.accounts),
        ?assertEqual(<<"premium">>, Modified#acct_user.default_qos),
        ?assertEqual(admin, Modified#acct_user.admin_level),
        ?assertEqual(5, Modified#acct_user.fairshare),
        ?assertEqual(100, Modified#acct_user.max_jobs),
        ?assertEqual(250, Modified#acct_user.max_submit),
        ?assertEqual(7200, Modified#acct_user.max_wall)
    end}.

%%====================================================================
%% Comprehensive Association Field Update Tests
%%====================================================================

association_field_updates_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_update_association_all_fields/1
     ]}.

test_update_association_all_fields(State) ->
    {"update all association fields via handle_call", fun() ->
        Assoc = #association{id = 1, cluster = <<"flurm">>, account = <<"root">>, user = <<"fullassoc">>},
        ets:insert(State#state.associations, Assoc),

        Updates = #{
            shares => 20,
            grp_tres_mins => #{<<"cpu">> => 10000},
            grp_tres => #{<<"cpu">> => 500},
            grp_jobs => 100,
            grp_submit => 200,
            grp_wall => 86400,
            max_tres_mins_per_job => #{<<"cpu">> => 1000},
            max_tres_per_job => #{<<"cpu">> => 64},
            max_tres_per_node => #{<<"cpu">> => 16},
            max_jobs => 50,
            max_submit => 100,
            max_wall_per_job => 7200,
            priority => 2000,
            qos => [<<"normal">>, <<"high">>],
            default_qos => <<"normal">>
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {modify_association, 1, Updates}, {self(), make_ref()}, State),

        [Modified] = ets:lookup(NewState#state.associations, 1),
        ?assertEqual(20, Modified#association.shares),
        ?assertEqual(#{<<"cpu">> => 10000}, Modified#association.grp_tres_mins),
        ?assertEqual(#{<<"cpu">> => 500}, Modified#association.grp_tres),
        ?assertEqual(100, Modified#association.grp_jobs),
        ?assertEqual(200, Modified#association.grp_submit),
        ?assertEqual(86400, Modified#association.grp_wall),
        ?assertEqual(#{<<"cpu">> => 1000}, Modified#association.max_tres_mins_per_job),
        ?assertEqual(#{<<"cpu">> => 64}, Modified#association.max_tres_per_job),
        ?assertEqual(#{<<"cpu">> => 16}, Modified#association.max_tres_per_node),
        ?assertEqual(50, Modified#association.max_jobs),
        ?assertEqual(100, Modified#association.max_submit),
        ?assertEqual(7200, Modified#association.max_wall_per_job),
        ?assertEqual(2000, Modified#association.priority),
        ?assertEqual([<<"normal">>, <<"high">>], Modified#association.qos),
        ?assertEqual(<<"normal">>, Modified#association.default_qos)
    end}.

%%====================================================================
%% Comprehensive QOS Field Update Tests
%%====================================================================

qos_field_updates_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_update_qos_all_fields/1
     ]}.

test_update_qos_all_fields(State) ->
    {"update all qos fields via handle_call", fun() ->
        Updates = #{
            description => <<"Full QOS description">>,
            priority => 5000,
            flags => [deny_limit, partition_maximum_priority],
            grace_time => 300,
            max_jobs_pa => 1000,
            max_jobs_pu => 100,
            max_submit_jobs_pa => 2000,
            max_submit_jobs_pu => 200,
            max_tres_pa => #{<<"cpu">> => 10000},
            max_tres_pu => #{<<"cpu">> => 1000},
            max_tres_per_job => #{<<"cpu">> => 128},
            max_tres_per_node => #{<<"cpu">> => 32},
            max_tres_per_user => #{<<"cpu">> => 512},
            max_wall_per_job => 259200,
            min_tres_per_job => #{<<"cpu">> => 2},
            preempt => [<<"low">>, <<"medium">>],
            preempt_mode => requeue,
            usage_factor => 2.5,
            usage_threshold => 0.75
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {modify_qos, <<"normal">>, Updates}, {self(), make_ref()}, State),

        [Modified] = ets:lookup(NewState#state.qos, <<"normal">>),
        ?assertEqual(<<"Full QOS description">>, Modified#qos.description),
        ?assertEqual(5000, Modified#qos.priority),
        ?assertEqual([deny_limit, partition_maximum_priority], Modified#qos.flags),
        ?assertEqual(300, Modified#qos.grace_time),
        ?assertEqual(1000, Modified#qos.max_jobs_pa),
        ?assertEqual(100, Modified#qos.max_jobs_pu),
        ?assertEqual(2000, Modified#qos.max_submit_jobs_pa),
        ?assertEqual(200, Modified#qos.max_submit_jobs_pu),
        ?assertEqual(#{<<"cpu">> => 10000}, Modified#qos.max_tres_pa),
        ?assertEqual(#{<<"cpu">> => 1000}, Modified#qos.max_tres_pu),
        ?assertEqual(#{<<"cpu">> => 128}, Modified#qos.max_tres_per_job),
        ?assertEqual(#{<<"cpu">> => 32}, Modified#qos.max_tres_per_node),
        ?assertEqual(#{<<"cpu">> => 512}, Modified#qos.max_tres_per_user),
        ?assertEqual(259200, Modified#qos.max_wall_per_job),
        ?assertEqual(#{<<"cpu">> => 2}, Modified#qos.min_tres_per_job),
        ?assertEqual([<<"low">>, <<"medium">>], Modified#qos.preempt),
        ?assertEqual(requeue, Modified#qos.preempt_mode),
        ?assertEqual(2.5, Modified#qos.usage_factor),
        ?assertEqual(0.75, Modified#qos.usage_threshold)
    end}.

%%====================================================================
%% Filter Pattern Matching Tests
%%====================================================================

filter_pattern_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_filter_accounts_name_wildcard/1,
      fun test_filter_accounts_exact_name/1,
      fun test_filter_users_name_wildcard/1,
      fun test_filter_users_by_account_membership/1,
      fun test_filter_associations_by_user/1,
      fun test_filter_associations_by_partition/1
     ]}.

test_filter_accounts_name_wildcard(State) ->
    {"filter accounts with wildcard pattern", fun() ->
        ets:insert(State#state.accounts, #account{name = <<"prefix_one">>}),
        ets:insert(State#state.accounts, #account{name = <<"prefix_two">>}),
        ets:insert(State#state.accounts, #account{name = <<"other_name">>}),

        {reply, Filtered, _} = flurm_account_manager:handle_call(
            {list_accounts, #{name => <<"prefix_*">>}}, {self(), make_ref()}, State),
        Names = [A#account.name || A <- Filtered],
        ?assert(lists:member(<<"prefix_one">>, Names)),
        ?assert(lists:member(<<"prefix_two">>, Names)),
        ?assertNot(lists:member(<<"other_name">>, Names))
    end}.

test_filter_accounts_exact_name(State) ->
    {"filter accounts with exact name", fun() ->
        ets:insert(State#state.accounts, #account{name = <<"exact_match">>}),
        ets:insert(State#state.accounts, #account{name = <<"exact_match_extra">>}),

        {reply, Filtered, _} = flurm_account_manager:handle_call(
            {list_accounts, #{name => <<"exact_match">>}}, {self(), make_ref()}, State),
        Names = [A#account.name || A <- Filtered],
        ?assert(lists:member(<<"exact_match">>, Names)),
        ?assertNot(lists:member(<<"exact_match_extra">>, Names))
    end}.

test_filter_users_name_wildcard(State) ->
    {"filter users with wildcard pattern", fun() ->
        ets:insert(State#state.users, #acct_user{name = <<"dev_user1">>}),
        ets:insert(State#state.users, #acct_user{name = <<"dev_user2">>}),
        ets:insert(State#state.users, #acct_user{name = <<"prod_user">>}),

        {reply, Filtered, _} = flurm_account_manager:handle_call(
            {list_users, #{name => <<"dev_*">>}}, {self(), make_ref()}, State),
        Names = [U#acct_user.name || U <- Filtered],
        ?assert(lists:member(<<"dev_user1">>, Names)),
        ?assert(lists:member(<<"dev_user2">>, Names)),
        ?assertNot(lists:member(<<"prod_user">>, Names))
    end}.

test_filter_users_by_account_membership(State) ->
    {"filter users by account membership", fun() ->
        ets:insert(State#state.users, #acct_user{name = <<"member">>, accounts = [<<"root">>, <<"dev">>]}),
        ets:insert(State#state.users, #acct_user{name = <<"nonmember">>, accounts = [<<"other">>]}),

        {reply, Filtered, _} = flurm_account_manager:handle_call(
            {list_users, #{account => <<"root">>}}, {self(), make_ref()}, State),
        Names = [U#acct_user.name || U <- Filtered],
        ?assert(lists:member(<<"member">>, Names)),
        ?assertNot(lists:member(<<"nonmember">>, Names))
    end}.

test_filter_associations_by_user(State) ->
    {"filter associations by user", fun() ->
        ets:insert(State#state.associations,
            #association{id = 1, cluster = <<"flurm">>, account = <<"root">>, user = <<"target_user">>}),
        ets:insert(State#state.associations,
            #association{id = 2, cluster = <<"flurm">>, account = <<"root">>, user = <<"other_user">>}),

        {reply, Filtered, _} = flurm_account_manager:handle_call(
            {list_associations, #{user => <<"target_user">>}}, {self(), make_ref()}, State),
        Users = [A#association.user || A <- Filtered],
        ?assert(lists:member(<<"target_user">>, Users)),
        ?assertNot(lists:member(<<"other_user">>, Users))
    end}.

test_filter_associations_by_partition(State) ->
    {"filter associations by partition", fun() ->
        ets:insert(State#state.associations,
            #association{id = 1, cluster = <<"flurm">>, account = <<"root">>,
                        user = <<"batch_user">>, partition = <<"batch">>}),
        ets:insert(State#state.associations,
            #association{id = 2, cluster = <<"flurm">>, account = <<"root">>,
                        user = <<"gpu_user">>, partition = <<"gpu">>}),

        {reply, Filtered, _} = flurm_account_manager:handle_call(
            {list_associations, #{partition => <<"batch">>}}, {self(), make_ref()}, State),
        Users = [A#association.user || A <- Filtered],
        ?assert(lists:member(<<"batch_user">>, Users)),
        ?assertNot(lists:member(<<"gpu_user">>, Users))
    end}.

%%====================================================================
%% Edge Cases and Boundary Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_empty_filter_returns_all/1,
      fun test_multiple_filters_combined/1,
      fun test_add_multiple_associations_increments_id/1,
      fun test_add_multiple_tres_increments_id/1
     ]}.

test_empty_filter_returns_all(State) ->
    {"empty filter returns all items", fun() ->
        ets:insert(State#state.accounts, #account{name = <<"acct1">>}),
        ets:insert(State#state.accounts, #account{name = <<"acct2">>}),

        {reply, All, _} = flurm_account_manager:handle_call(
            {list_accounts, #{}}, {self(), make_ref()}, State),
        %% Should return all accounts including root
        ?assert(length(All) >= 3)
    end}.

test_multiple_filters_combined(State) ->
    {"multiple filters are ANDed together", fun() ->
        ets:insert(State#state.accounts,
            #account{name = <<"target">>, parent = <<"root">>, organization = <<"Org1">>}),
        ets:insert(State#state.accounts,
            #account{name = <<"wrong_org">>, parent = <<"root">>, organization = <<"Org2">>}),
        ets:insert(State#state.accounts,
            #account{name = <<"wrong_parent">>, parent = <<"other">>, organization = <<"Org1">>}),

        {reply, Filtered, _} = flurm_account_manager:handle_call(
            {list_accounts, #{parent => <<"root">>, organization => <<"Org1">>}},
            {self(), make_ref()}, State),
        Names = [A#account.name || A <- Filtered],
        ?assert(lists:member(<<"target">>, Names)),
        ?assertNot(lists:member(<<"wrong_org">>, Names)),
        ?assertNot(lists:member(<<"wrong_parent">>, Names))
    end}.

test_add_multiple_associations_increments_id(State) ->
    {"adding associations increments id correctly", fun() ->
        Assoc1 = #association{cluster = <<"flurm">>, account = <<"root">>, user = <<"user1">>},
        Assoc2 = #association{cluster = <<"flurm">>, account = <<"root">>, user = <<"user2">>},
        Assoc3 = #association{cluster = <<"flurm">>, account = <<"root">>, user = <<"user3">>},

        {reply, {ok, Id1}, State1} = flurm_account_manager:handle_call(
            {add_association, Assoc1}, {self(), make_ref()}, State),
        {reply, {ok, Id2}, State2} = flurm_account_manager:handle_call(
            {add_association, Assoc2}, {self(), make_ref()}, State1),
        {reply, {ok, Id3}, _State3} = flurm_account_manager:handle_call(
            {add_association, Assoc3}, {self(), make_ref()}, State2),

        ?assertEqual(1, Id1),
        ?assertEqual(2, Id2),
        ?assertEqual(3, Id3)
    end}.

test_add_multiple_tres_increments_id(State) ->
    {"adding tres increments id correctly", fun() ->
        Tres1 = #tres{type = <<"gres/gpu">>, name = <<"v100">>},
        Tres2 = #tres{type = <<"gres/gpu">>, name = <<"a100">>},

        {reply, {ok, Id1}, State1} = flurm_account_manager:handle_call(
            {add_tres, Tres1}, {self(), make_ref()}, State),
        {reply, {ok, Id2}, _State2} = flurm_account_manager:handle_call(
            {add_tres, Tres2}, {self(), make_ref()}, State1),

        ?assertEqual(5, Id1),  % After default 1-4
        ?assertEqual(6, Id2)
    end}.

%%====================================================================
%% Association Lookup Edge Cases
%%====================================================================

association_lookup_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_get_association_multiple_matches/1
     ]}.

test_get_association_multiple_matches(State) ->
    {"get_association returns first match when multiple exist", fun() ->
        %% Add two associations with same key (shouldn't happen in practice, but test behavior)
        ets:insert(State#state.associations,
            #association{id = 1, cluster = <<"flurm">>, account = <<"root">>, user = <<"dup">>}),
        ets:insert(State#state.associations,
            #association{id = 2, cluster = <<"flurm">>, account = <<"root">>, user = <<"dup">>}),

        {reply, Result, _} = flurm_account_manager:handle_call(
            {get_association_by_key, <<"flurm">>, <<"root">>, <<"dup">>},
            {self(), make_ref()}, State),
        ?assertMatch({ok, #association{user = <<"dup">>}}, Result)
    end}.

%%====================================================================
%% Check Limits Tests - Test check_limits handle_call path
%% Note: These only test the early return paths (no_association) since
%% the full path requires flurm_qos ETS tables to be initialized
%%====================================================================

check_limits_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_check_limits_no_user_no_association_required/1,
      fun test_check_limits_user_without_association/1
     ]}.

test_check_limits_no_user_no_association_required(State) ->
    {"check_limits rejects unknown user when association required", fun() ->
        %% Set require_association to true
        application:set_env(flurm_core, require_association, true),

        JobSpec = #{account => <<"nonexistent">>, time_limit => 3600},
        {reply, Result, _} = flurm_account_manager:handle_call(
            {check_limits, <<"unknown_user">>, JobSpec}, {self(), make_ref()}, State),
        %% Should return error for no_association
        ?assertMatch({error, {no_association, _, _}}, Result),

        %% Reset
        application:set_env(flurm_core, require_association, false)
    end}.

test_check_limits_user_without_association(State) ->
    {"check_limits for user without association", fun() ->
        %% Create a user without any association
        User = #acct_user{name = <<"orphan_user">>, default_account = <<"root">>},
        ets:insert(State#state.users, User),

        application:set_env(flurm_core, require_association, true),

        JobSpec = #{account => <<"root">>, time_limit => 3600},
        {reply, Result, _} = flurm_account_manager:handle_call(
            {check_limits, <<"orphan_user">>, JobSpec}, {self(), make_ref()}, State),
        %% Without association, should fail when required
        %% Error format is {error, {no_association, Username, Account}}
        ?assertMatch({error, {no_association, _, _}}, Result),

        %% Reset
        application:set_env(flurm_core, require_association, false)
    end}.

%%====================================================================
%% Full Record Field Tests - Verify all record fields work via handle_call
%% Note: Normalization from maps is tested in API-level tests. These tests
%% use records directly with handle_call to test all field combinations.
%%====================================================================

full_record_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_full_account_record/1,
      fun test_full_user_record/1,
      fun test_full_association_record/1,
      fun test_full_qos_record/1,
      fun test_full_cluster_record/1,
      fun test_full_tres_record/1
     ]}.

test_full_account_record(State) ->
    {"add full account record with all fields", fun() ->
        Account = #account{
            name = <<"full_acct">>,
            description = <<"Full Description">>,
            organization = <<"Full Org">>,
            parent = <<"root">>,
            coordinators = [<<"coord1">>],
            default_qos = <<"high">>,
            fairshare = 5,
            max_jobs = 100,
            max_submit = 200,
            max_wall = 7200
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_account, Account}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.accounts, <<"full_acct">>),
        ?assertEqual(<<"Full Description">>, Retrieved#account.description),
        ?assertEqual(<<"Full Org">>, Retrieved#account.organization),
        ?assertEqual(<<"root">>, Retrieved#account.parent),
        ?assertEqual([<<"coord1">>], Retrieved#account.coordinators),
        ?assertEqual(<<"high">>, Retrieved#account.default_qos),
        ?assertEqual(5, Retrieved#account.fairshare),
        ?assertEqual(100, Retrieved#account.max_jobs),
        ?assertEqual(200, Retrieved#account.max_submit),
        ?assertEqual(7200, Retrieved#account.max_wall)
    end}.

test_full_user_record(State) ->
    {"add full user record with all fields", fun() ->
        User = #acct_user{
            name = <<"full_user">>,
            default_account = <<"root">>,
            accounts = [<<"root">>, <<"dev">>],
            default_qos = <<"premium">>,
            admin_level = admin,
            fairshare = 10,
            max_jobs = 50,
            max_submit = 100,
            max_wall = 3600
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_user, User}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.users, <<"full_user">>),
        ?assertEqual(<<"root">>, Retrieved#acct_user.default_account),
        ?assertEqual([<<"root">>, <<"dev">>], Retrieved#acct_user.accounts),
        ?assertEqual(<<"premium">>, Retrieved#acct_user.default_qos),
        ?assertEqual(admin, Retrieved#acct_user.admin_level),
        ?assertEqual(10, Retrieved#acct_user.fairshare),
        ?assertEqual(50, Retrieved#acct_user.max_jobs),
        ?assertEqual(100, Retrieved#acct_user.max_submit),
        ?assertEqual(3600, Retrieved#acct_user.max_wall)
    end}.

test_full_association_record(State) ->
    {"add full association record with all fields", fun() ->
        Assoc = #association{
            cluster = <<"testcluster">>,
            account = <<"root">>,
            user = <<"assoc_user">>,
            partition = <<"batch">>,
            parent_id = 0,
            shares = 5,
            grp_tres_mins = #{<<"cpu">> => 1000},
            grp_tres = #{<<"cpu">> => 50},
            grp_jobs = 10,
            grp_submit = 20,
            grp_wall = 3600,
            max_tres_mins_per_job = #{<<"cpu">> => 500},
            max_tres_per_job = #{<<"cpu">> => 32},
            max_tres_per_node = #{<<"cpu">> => 8},
            max_jobs = 25,
            max_submit = 50,
            max_wall_per_job = 1800,
            priority = 1000,
            qos = [<<"normal">>],
            default_qos = <<"normal">>
        },

        {reply, {ok, Id}, NewState} = flurm_account_manager:handle_call(
            {add_association, Assoc}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.associations, Id),
        ?assertEqual(<<"testcluster">>, Retrieved#association.cluster),
        ?assertEqual(<<"root">>, Retrieved#association.account),
        ?assertEqual(<<"assoc_user">>, Retrieved#association.user),
        ?assertEqual(<<"batch">>, Retrieved#association.partition),
        ?assertEqual(5, Retrieved#association.shares),
        ?assertEqual(#{<<"cpu">> => 1000}, Retrieved#association.grp_tres_mins),
        ?assertEqual(#{<<"cpu">> => 50}, Retrieved#association.grp_tres),
        ?assertEqual(10, Retrieved#association.grp_jobs),
        ?assertEqual(20, Retrieved#association.grp_submit),
        ?assertEqual(3600, Retrieved#association.grp_wall),
        ?assertEqual(#{<<"cpu">> => 500}, Retrieved#association.max_tres_mins_per_job),
        ?assertEqual(#{<<"cpu">> => 32}, Retrieved#association.max_tres_per_job),
        ?assertEqual(#{<<"cpu">> => 8}, Retrieved#association.max_tres_per_node),
        ?assertEqual(25, Retrieved#association.max_jobs),
        ?assertEqual(50, Retrieved#association.max_submit),
        ?assertEqual(1800, Retrieved#association.max_wall_per_job),
        ?assertEqual(1000, Retrieved#association.priority),
        ?assertEqual([<<"normal">>], Retrieved#association.qos),
        ?assertEqual(<<"normal">>, Retrieved#association.default_qos)
    end}.

test_full_qos_record(State) ->
    {"add full qos record with all fields", fun() ->
        Qos = #qos{
            name = <<"full_qos">>,
            description = <<"Full QOS">>,
            priority = 2000,
            flags = [deny_limit],
            grace_time = 120,
            max_jobs_pa = 500,
            max_jobs_pu = 50,
            max_submit_jobs_pa = 1000,
            max_submit_jobs_pu = 100,
            max_tres_pa = #{<<"cpu">> => 5000},
            max_tres_pu = #{<<"cpu">> => 500},
            max_tres_per_job = #{<<"cpu">> => 64},
            max_tres_per_node = #{<<"cpu">> => 16},
            max_tres_per_user = #{<<"cpu">> => 256},
            max_wall_per_job = 86400,
            min_tres_per_job = #{<<"cpu">> => 1},
            preempt = [<<"low">>],
            preempt_mode = cancel,
            usage_factor = 1.5,
            usage_threshold = 0.5
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_qos, Qos}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.qos, <<"full_qos">>),
        ?assertEqual(<<"Full QOS">>, Retrieved#qos.description),
        ?assertEqual(2000, Retrieved#qos.priority),
        ?assertEqual([deny_limit], Retrieved#qos.flags),
        ?assertEqual(120, Retrieved#qos.grace_time),
        ?assertEqual(500, Retrieved#qos.max_jobs_pa),
        ?assertEqual(50, Retrieved#qos.max_jobs_pu),
        ?assertEqual(1000, Retrieved#qos.max_submit_jobs_pa),
        ?assertEqual(100, Retrieved#qos.max_submit_jobs_pu),
        ?assertEqual(#{<<"cpu">> => 5000}, Retrieved#qos.max_tres_pa),
        ?assertEqual(#{<<"cpu">> => 500}, Retrieved#qos.max_tres_pu),
        ?assertEqual(#{<<"cpu">> => 64}, Retrieved#qos.max_tres_per_job),
        ?assertEqual(#{<<"cpu">> => 16}, Retrieved#qos.max_tres_per_node),
        ?assertEqual(#{<<"cpu">> => 256}, Retrieved#qos.max_tres_per_user),
        ?assertEqual(86400, Retrieved#qos.max_wall_per_job),
        ?assertEqual(#{<<"cpu">> => 1}, Retrieved#qos.min_tres_per_job),
        ?assertEqual([<<"low">>], Retrieved#qos.preempt),
        ?assertEqual(cancel, Retrieved#qos.preempt_mode),
        ?assertEqual(1.5, Retrieved#qos.usage_factor),
        ?assertEqual(0.5, Retrieved#qos.usage_threshold)
    end}.

test_full_cluster_record(State) ->
    {"add full cluster record with all fields", fun() ->
        Cluster = #acct_cluster{
            name = <<"full_cluster">>,
            control_host = <<"controller.example.com">>,
            control_port = 6820,
            rpc_version = 42,
            classification = 1,
            tres = #{<<"cpu">> => 1000},
            flags = [some_flag]
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_cluster, Cluster}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.clusters, <<"full_cluster">>),
        ?assertEqual(<<"controller.example.com">>, Retrieved#acct_cluster.control_host),
        ?assertEqual(6820, Retrieved#acct_cluster.control_port),
        ?assertEqual(42, Retrieved#acct_cluster.rpc_version),
        ?assertEqual(1, Retrieved#acct_cluster.classification),
        ?assertEqual(#{<<"cpu">> => 1000}, Retrieved#acct_cluster.tres),
        ?assertEqual([some_flag], Retrieved#acct_cluster.flags)
    end}.

test_full_tres_record(State) ->
    {"add full tres record with all fields", fun() ->
        Tres = #tres{
            type = <<"gres/gpu">>,
            name = <<"nvidia-a100">>,
            count = 8
        },

        {reply, {ok, Id}, NewState} = flurm_account_manager:handle_call(
            {add_tres, Tres}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.tres, Id),
        ?assertEqual(<<"gres/gpu">>, Retrieved#tres.type),
        ?assertEqual(<<"nvidia-a100">>, Retrieved#tres.name),
        ?assertEqual(8, Retrieved#tres.count)
    end}.

%%====================================================================
%% Record Pass-through Tests - Verify records pass through unchanged
%%====================================================================

record_passthrough_test_() ->
    {foreach,
     fun create_test_state/0,
     fun cleanup_test_state/1,
     [
      fun test_account_record_passthrough/1,
      fun test_user_record_passthrough/1,
      fun test_association_record_passthrough/1,
      fun test_qos_record_passthrough/1,
      fun test_cluster_record_passthrough/1,
      fun test_tres_record_passthrough/1
     ]}.

test_account_record_passthrough(State) ->
    {"account record passes through normalize unchanged", fun() ->
        Account = #account{
            name = <<"record_acct">>,
            description = <<"Record Desc">>,
            organization = <<"Record Org">>,
            parent = <<"root">>,
            coordinators = [<<"c1">>],
            default_qos = <<"high">>,
            fairshare = 7,
            max_jobs = 77,
            max_submit = 777,
            max_wall = 7777
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_account, Account}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.accounts, <<"record_acct">>),
        ?assertEqual(Account#account.description, Retrieved#account.description),
        ?assertEqual(Account#account.organization, Retrieved#account.organization),
        ?assertEqual(Account#account.parent, Retrieved#account.parent),
        ?assertEqual(Account#account.coordinators, Retrieved#account.coordinators),
        ?assertEqual(Account#account.default_qos, Retrieved#account.default_qos),
        ?assertEqual(Account#account.fairshare, Retrieved#account.fairshare),
        ?assertEqual(Account#account.max_jobs, Retrieved#account.max_jobs),
        ?assertEqual(Account#account.max_submit, Retrieved#account.max_submit),
        ?assertEqual(Account#account.max_wall, Retrieved#account.max_wall)
    end}.

test_user_record_passthrough(State) ->
    {"user record passes through normalize unchanged", fun() ->
        User = #acct_user{
            name = <<"record_user">>,
            default_account = <<"acct1">>,
            accounts = [<<"acct1">>, <<"acct2">>],
            default_qos = <<"premium">>,
            admin_level = operator,
            fairshare = 3,
            max_jobs = 33,
            max_submit = 333,
            max_wall = 3333
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_user, User}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.users, <<"record_user">>),
        ?assertEqual(User#acct_user.default_account, Retrieved#acct_user.default_account),
        ?assertEqual(User#acct_user.accounts, Retrieved#acct_user.accounts),
        ?assertEqual(User#acct_user.default_qos, Retrieved#acct_user.default_qos),
        ?assertEqual(User#acct_user.admin_level, Retrieved#acct_user.admin_level),
        ?assertEqual(User#acct_user.fairshare, Retrieved#acct_user.fairshare),
        ?assertEqual(User#acct_user.max_jobs, Retrieved#acct_user.max_jobs),
        ?assertEqual(User#acct_user.max_submit, Retrieved#acct_user.max_submit),
        ?assertEqual(User#acct_user.max_wall, Retrieved#acct_user.max_wall)
    end}.

test_association_record_passthrough(State) ->
    {"association record passes through normalize unchanged", fun() ->
        Assoc = #association{
            cluster = <<"mycluster">>,
            account = <<"myaccount">>,
            user = <<"myuser">>,
            partition = <<"mypartition">>,
            shares = 9,
            max_jobs = 99
        },

        {reply, {ok, Id}, NewState} = flurm_account_manager:handle_call(
            {add_association, Assoc}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.associations, Id),
        ?assertEqual(Assoc#association.cluster, Retrieved#association.cluster),
        ?assertEqual(Assoc#association.account, Retrieved#association.account),
        ?assertEqual(Assoc#association.user, Retrieved#association.user),
        ?assertEqual(Assoc#association.partition, Retrieved#association.partition),
        ?assertEqual(Assoc#association.shares, Retrieved#association.shares),
        ?assertEqual(Assoc#association.max_jobs, Retrieved#association.max_jobs)
    end}.

test_qos_record_passthrough(State) ->
    {"qos record passes through normalize unchanged", fun() ->
        Qos = #qos{
            name = <<"record_qos">>,
            description = <<"Record QOS">>,
            priority = 999,
            flags = [flag1, flag2],
            preempt_mode = suspend,
            usage_factor = 2.5
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_qos, Qos}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.qos, <<"record_qos">>),
        ?assertEqual(Qos#qos.description, Retrieved#qos.description),
        ?assertEqual(Qos#qos.priority, Retrieved#qos.priority),
        ?assertEqual(Qos#qos.flags, Retrieved#qos.flags),
        ?assertEqual(Qos#qos.preempt_mode, Retrieved#qos.preempt_mode),
        ?assertEqual(Qos#qos.usage_factor, Retrieved#qos.usage_factor)
    end}.

test_cluster_record_passthrough(State) ->
    {"cluster record passes through normalize unchanged", fun() ->
        Cluster = #acct_cluster{
            name = <<"record_cluster">>,
            control_host = <<"host.example.com">>,
            control_port = 9999,
            rpc_version = 123
        },

        {reply, ok, NewState} = flurm_account_manager:handle_call(
            {add_cluster, Cluster}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.clusters, <<"record_cluster">>),
        ?assertEqual(Cluster#acct_cluster.control_host, Retrieved#acct_cluster.control_host),
        ?assertEqual(Cluster#acct_cluster.control_port, Retrieved#acct_cluster.control_port),
        ?assertEqual(Cluster#acct_cluster.rpc_version, Retrieved#acct_cluster.rpc_version)
    end}.

test_tres_record_passthrough(State) ->
    {"tres record passes through normalize unchanged", fun() ->
        Tres = #tres{
            type = <<"custom">>,
            name = <<"custom_name">>,
            count = 42
        },

        {reply, {ok, Id}, NewState} = flurm_account_manager:handle_call(
            {add_tres, Tres}, {self(), make_ref()}, State),

        [Retrieved] = ets:lookup(NewState#state.tres, Id),
        ?assertEqual(Tres#tres.type, Retrieved#tres.type),
        ?assertEqual(Tres#tres.name, Retrieved#tres.name),
        ?assertEqual(Tres#tres.count, Retrieved#tres.count)
    end}.
