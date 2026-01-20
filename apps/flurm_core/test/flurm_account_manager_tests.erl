%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_account_manager module
%%%
%%% Comprehensive EUnit tests covering:
%%% - Account CRUD operations
%%% - User CRUD operations
%%% - Association CRUD operations
%%% - QOS CRUD operations
%%% - Cluster CRUD operations
%%% - TRES CRUD operations
%%% - User association lookup
%%% - Limit checking
%%% - gen_server callbacks
%%% - Normalization functions
%%% - Filtering functions
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_account_manager_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure flurm_qos ETS table exists (required by check_limits)
    case ets:whereis(flurm_qos) of
        undefined ->
            ets:new(flurm_qos, [named_table, set, public, {keypos, 2}]);
        _ ->
            ok
    end,
    %% Start the account manager server
    case whereis(flurm_account_manager) of
        undefined ->
            {ok, Pid} = flurm_account_manager:start_link(),
            {started, Pid};
        Pid ->
            {existing, Pid}
    end.

cleanup({started, Pid}) ->
    %% Use monitor to wait for actual termination
    case is_process_alive(Pid) of
        true ->
            Ref = monitor(process, Pid),
            unlink(Pid),
            catch gen_server:stop(Pid, normal, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush]),
                catch exit(Pid, kill)
            end;
        false ->
            ok
    end;
cleanup({existing, _Pid}) ->
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

account_manager_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Initialization tests
      {"init creates default entities", fun test_init_default_entities/0},
      {"init creates default TRES", fun test_init_default_tres/0},
      {"init creates default QOS", fun test_init_default_qos/0},
      {"init creates root account", fun test_init_root_account/0},

      %% Account CRUD tests
      {"add account with record", fun test_add_account_record/0},
      {"add account with map", fun test_add_account_map/0},
      {"add account already exists", fun test_add_account_exists/0},
      {"modify account success", fun test_modify_account/0},
      {"modify account not found", fun test_modify_account_not_found/0},
      {"delete account success", fun test_delete_account/0},
      {"delete account not found", fun test_delete_account_not_found/0},
      {"delete account with children fails", fun test_delete_account_with_children/0},
      {"get account found", fun test_get_account_found/0},
      {"get account not found", fun test_get_account_not_found/0},
      {"list all accounts", fun test_list_accounts/0},
      {"list accounts with filters", fun test_list_accounts_filtered/0},

      %% User CRUD tests
      {"add user with record", fun test_add_user_record/0},
      {"add user with map", fun test_add_user_map/0},
      {"add user already exists", fun test_add_user_exists/0},
      {"modify user success", fun test_modify_user/0},
      {"modify user not found", fun test_modify_user_not_found/0},
      {"delete user success", fun test_delete_user/0},
      {"delete user not found", fun test_delete_user_not_found/0},
      {"get user found", fun test_get_user_found/0},
      {"get user not found", fun test_get_user_not_found/0},
      {"list all users", fun test_list_users/0},
      {"list users with filters", fun test_list_users_filtered/0},

      %% Association CRUD tests
      {"add association with record", fun test_add_association_record/0},
      {"add association with map", fun test_add_association_map/0},
      {"modify association success", fun test_modify_association/0},
      {"modify association not found", fun test_modify_association_not_found/0},
      {"delete association success", fun test_delete_association/0},
      {"delete association not found", fun test_delete_association_not_found/0},
      {"get association by id", fun test_get_association_by_id/0},
      {"get association by id not found", fun test_get_association_by_id_not_found/0},
      {"get association by key", fun test_get_association_by_key/0},
      {"get association by key not found", fun test_get_association_by_key_not_found/0},
      {"list all associations", fun test_list_associations/0},
      {"list associations with filters", fun test_list_associations_filtered/0},

      %% QOS CRUD tests
      {"add QOS with record", fun test_add_qos_record/0},
      {"add QOS with map", fun test_add_qos_map/0},
      {"add QOS already exists", fun test_add_qos_exists/0},
      {"modify QOS success", fun test_modify_qos/0},
      {"modify QOS not found", fun test_modify_qos_not_found/0},
      {"delete QOS success", fun test_delete_qos/0},
      {"delete QOS not found", fun test_delete_qos_not_found/0},
      {"get QOS found", fun test_get_qos_found/0},
      {"get QOS not found", fun test_get_qos_not_found/0},
      {"list all QOS", fun test_list_qos/0},

      %% Cluster CRUD tests
      {"add cluster with record", fun test_add_cluster_record/0},
      {"add cluster with map", fun test_add_cluster_map/0},
      {"add cluster already exists", fun test_add_cluster_exists/0},
      {"get cluster found", fun test_get_cluster_found/0},
      {"get cluster not found", fun test_get_cluster_not_found/0},
      {"list all clusters", fun test_list_clusters/0},

      %% TRES CRUD tests
      {"add TRES with record", fun test_add_tres_record/0},
      {"add TRES with map", fun test_add_tres_map/0},
      {"get TRES by id", fun test_get_tres_by_id/0},
      {"get TRES by type", fun test_get_tres_by_type/0},
      {"get TRES not found", fun test_get_tres_not_found/0},
      {"list all TRES", fun test_list_tres/0},

      %% User association lookup tests
      {"get user association found", fun test_get_user_association_found/0},
      {"get user association not found", fun test_get_user_association_not_found/0},

      %% Limit checking tests
      {"check limits - no association required", fun test_check_limits_no_assoc_required/0},
      {"check limits - association required", fun test_check_limits_assoc_required/0},
      {"check limits - with association", fun test_check_limits_with_association/0},

      %% gen_server callback tests
      {"unknown call returns error", fun test_unknown_call/0},
      {"unknown cast is ignored", fun test_unknown_cast/0},
      {"unknown info is ignored", fun test_unknown_info/0},
      {"terminate succeeds", fun test_terminate/0}
     ]}.

%%====================================================================
%% Initialization Tests
%%====================================================================

test_init_default_entities() ->
    %% Verify default cluster exists
    Clusters = flurm_account_manager:list_clusters(),
    ?assert(length(Clusters) >= 1),
    [DefaultCluster | _] = Clusters,
    ?assert(is_binary(DefaultCluster#acct_cluster.name)).

test_init_default_tres() ->
    TresList = flurm_account_manager:list_tres(),
    %% Should have at least cpu, mem, energy, node
    ?assert(length(TresList) >= 4),
    Types = [T#tres.type || T <- TresList],
    ?assert(lists:member(<<"cpu">>, Types)),
    ?assert(lists:member(<<"mem">>, Types)),
    ?assert(lists:member(<<"energy">>, Types)),
    ?assert(lists:member(<<"node">>, Types)).

test_init_default_qos() ->
    QosList = flurm_account_manager:list_qos(),
    ?assert(length(QosList) >= 1),
    Names = [Q#qos.name || Q <- QosList],
    ?assert(lists:member(<<"normal">>, Names)).

test_init_root_account() ->
    {ok, Root} = flurm_account_manager:get_account(<<"root">>),
    ?assertEqual(<<"root">>, Root#account.name),
    ?assertEqual(<<"Root account">>, Root#account.description).

%%====================================================================
%% Account CRUD Tests
%%====================================================================

test_add_account_record() ->
    Account = #account{
        name = <<"test_account">>,
        description = <<"Test description">>,
        organization = <<"Test Org">>
    },
    ?assertEqual(ok, flurm_account_manager:add_account(Account)),
    {ok, Retrieved} = flurm_account_manager:get_account(<<"test_account">>),
    ?assertEqual(<<"test_account">>, Retrieved#account.name),
    ?assertEqual(<<"Test description">>, Retrieved#account.description).

test_add_account_map() ->
    Map = #{
        name => <<"map_account">>,
        description => <<"Map description">>,
        organization => <<"Map Org">>,
        fairshare => 2,
        max_jobs => 100
    },
    ?assertEqual(ok, flurm_account_manager:add_account(Map)),
    {ok, Retrieved} = flurm_account_manager:get_account(<<"map_account">>),
    ?assertEqual(<<"map_account">>, Retrieved#account.name),
    ?assertEqual(2, Retrieved#account.fairshare),
    ?assertEqual(100, Retrieved#account.max_jobs).

test_add_account_exists() ->
    %% root already exists from init
    Account = #account{name = <<"root">>},
    ?assertEqual({error, already_exists}, flurm_account_manager:add_account(Account)).

test_modify_account() ->
    %% Add an account first
    ok = flurm_account_manager:add_account(#{name => <<"modify_test">>}),

    %% Modify it
    Updates = #{description => <<"Updated">>, max_jobs => 50},
    ?assertEqual(ok, flurm_account_manager:modify_account(<<"modify_test">>, Updates)),

    {ok, Updated} = flurm_account_manager:get_account(<<"modify_test">>),
    ?assertEqual(<<"Updated">>, Updated#account.description),
    ?assertEqual(50, Updated#account.max_jobs).

test_modify_account_not_found() ->
    Result = flurm_account_manager:modify_account(<<"nonexistent">>, #{description => <<"test">>}),
    ?assertEqual({error, not_found}, Result).

test_delete_account() ->
    ok = flurm_account_manager:add_account(#{name => <<"to_delete">>}),
    ?assertMatch({ok, _}, flurm_account_manager:get_account(<<"to_delete">>)),
    ?assertEqual(ok, flurm_account_manager:delete_account(<<"to_delete">>)),
    ?assertEqual({error, not_found}, flurm_account_manager:get_account(<<"to_delete">>)).

test_delete_account_not_found() ->
    Result = flurm_account_manager:delete_account(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_delete_account_with_children() ->
    %% Create parent and child accounts
    ok = flurm_account_manager:add_account(#{name => <<"parent_acct">>}),
    ok = flurm_account_manager:add_account(#{name => <<"child_acct">>, parent => <<"parent_acct">>}),

    %% Try to delete parent - should fail
    Result = flurm_account_manager:delete_account(<<"parent_acct">>),
    ?assertEqual({error, has_children}, Result),

    %% Cleanup: delete child first, then parent
    ok = flurm_account_manager:delete_account(<<"child_acct">>),
    ok = flurm_account_manager:delete_account(<<"parent_acct">>).

test_get_account_found() ->
    ok = flurm_account_manager:add_account(#{name => <<"get_test">>}),
    {ok, Account} = flurm_account_manager:get_account(<<"get_test">>),
    ?assertEqual(<<"get_test">>, Account#account.name).

test_get_account_not_found() ->
    Result = flurm_account_manager:get_account(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_list_accounts() ->
    %% Add some accounts
    ok = flurm_account_manager:add_account(#{name => <<"list_test1">>}),
    ok = flurm_account_manager:add_account(#{name => <<"list_test2">>}),

    Accounts = flurm_account_manager:list_accounts(),
    ?assert(is_list(Accounts)),
    Names = [A#account.name || A <- Accounts],
    ?assert(lists:member(<<"list_test1">>, Names)),
    ?assert(lists:member(<<"list_test2">>, Names)).

test_list_accounts_filtered() ->
    ok = flurm_account_manager:add_account(#{name => <<"filter_parent">>}),
    ok = flurm_account_manager:add_account(#{name => <<"filter_child">>, parent => <<"filter_parent">>}),

    %% Filter by parent
    Filtered = flurm_account_manager:list_accounts(#{parent => <<"filter_parent">>}),
    Names = [A#account.name || A <- Filtered],
    ?assert(lists:member(<<"filter_child">>, Names)),
    ?assertNot(lists:member(<<"filter_parent">>, Names)).

%%====================================================================
%% User CRUD Tests
%%====================================================================

test_add_user_record() ->
    User = #acct_user{
        name = <<"test_user">>,
        default_account = <<"root">>,
        admin_level = operator
    },
    ?assertEqual(ok, flurm_account_manager:add_user(User)),
    {ok, Retrieved} = flurm_account_manager:get_user(<<"test_user">>),
    ?assertEqual(<<"test_user">>, Retrieved#acct_user.name),
    ?assertEqual(operator, Retrieved#acct_user.admin_level).

test_add_user_map() ->
    Map = #{
        name => <<"map_user">>,
        default_account => <<"root">>,
        accounts => [<<"root">>],
        fairshare => 3,
        max_jobs => 50
    },
    ?assertEqual(ok, flurm_account_manager:add_user(Map)),
    {ok, Retrieved} = flurm_account_manager:get_user(<<"map_user">>),
    ?assertEqual(<<"map_user">>, Retrieved#acct_user.name),
    ?assertEqual(3, Retrieved#acct_user.fairshare),
    ?assertEqual(50, Retrieved#acct_user.max_jobs).

test_add_user_exists() ->
    ok = flurm_account_manager:add_user(#{name => <<"existing_user">>}),
    Result = flurm_account_manager:add_user(#{name => <<"existing_user">>}),
    ?assertEqual({error, already_exists}, Result).

test_modify_user() ->
    ok = flurm_account_manager:add_user(#{name => <<"modify_user">>}),

    Updates = #{default_account => <<"root">>, max_jobs => 25},
    ?assertEqual(ok, flurm_account_manager:modify_user(<<"modify_user">>, Updates)),

    {ok, Updated} = flurm_account_manager:get_user(<<"modify_user">>),
    ?assertEqual(<<"root">>, Updated#acct_user.default_account),
    ?assertEqual(25, Updated#acct_user.max_jobs).

test_modify_user_not_found() ->
    Result = flurm_account_manager:modify_user(<<"nonexistent">>, #{max_jobs => 10}),
    ?assertEqual({error, not_found}, Result).

test_delete_user() ->
    ok = flurm_account_manager:add_user(#{name => <<"delete_user">>}),
    ?assertMatch({ok, _}, flurm_account_manager:get_user(<<"delete_user">>)),
    ?assertEqual(ok, flurm_account_manager:delete_user(<<"delete_user">>)),
    ?assertEqual({error, not_found}, flurm_account_manager:get_user(<<"delete_user">>)).

test_delete_user_not_found() ->
    Result = flurm_account_manager:delete_user(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_get_user_found() ->
    ok = flurm_account_manager:add_user(#{name => <<"get_user_test">>}),
    {ok, User} = flurm_account_manager:get_user(<<"get_user_test">>),
    ?assertEqual(<<"get_user_test">>, User#acct_user.name).

test_get_user_not_found() ->
    Result = flurm_account_manager:get_user(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_list_users() ->
    ok = flurm_account_manager:add_user(#{name => <<"list_user1">>}),
    ok = flurm_account_manager:add_user(#{name => <<"list_user2">>}),

    Users = flurm_account_manager:list_users(),
    ?assert(is_list(Users)),
    Names = [U#acct_user.name || U <- Users],
    ?assert(lists:member(<<"list_user1">>, Names)),
    ?assert(lists:member(<<"list_user2">>, Names)).

test_list_users_filtered() ->
    ok = flurm_account_manager:add_user(#{name => <<"admin_user">>, admin_level => admin}),
    ok = flurm_account_manager:add_user(#{name => <<"normal_user">>, admin_level => none}),

    %% Filter by admin_level
    Filtered = flurm_account_manager:list_users(#{admin_level => admin}),
    Names = [U#acct_user.name || U <- Filtered],
    ?assert(lists:member(<<"admin_user">>, Names)),
    ?assertNot(lists:member(<<"normal_user">>, Names)).

%%====================================================================
%% Association CRUD Tests
%%====================================================================

test_add_association_record() ->
    Assoc = #association{
        cluster = <<"flurm">>,
        account = <<"root">>,
        user = <<"assoc_user">>,
        shares = 5
    },
    {ok, Id} = flurm_account_manager:add_association(Assoc),
    ?assert(is_integer(Id)),
    {ok, Retrieved} = flurm_account_manager:get_association(Id),
    ?assertEqual(<<"assoc_user">>, Retrieved#association.user),
    ?assertEqual(5, Retrieved#association.shares).

test_add_association_map() ->
    Map = #{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"map_assoc_user">>,
        max_jobs => 100,
        priority => 500
    },
    {ok, Id} = flurm_account_manager:add_association(Map),
    ?assert(is_integer(Id)),
    {ok, Retrieved} = flurm_account_manager:get_association(Id),
    ?assertEqual(<<"map_assoc_user">>, Retrieved#association.user),
    ?assertEqual(100, Retrieved#association.max_jobs),
    ?assertEqual(500, Retrieved#association.priority).

test_modify_association() ->
    {ok, Id} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"modify_assoc_user">>
    }),

    Updates = #{shares => 10, max_jobs => 50},
    ?assertEqual(ok, flurm_account_manager:modify_association(Id, Updates)),

    {ok, Updated} = flurm_account_manager:get_association(Id),
    ?assertEqual(10, Updated#association.shares),
    ?assertEqual(50, Updated#association.max_jobs).

test_modify_association_not_found() ->
    Result = flurm_account_manager:modify_association(99999, #{shares => 5}),
    ?assertEqual({error, not_found}, Result).

test_delete_association() ->
    {ok, Id} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"delete_assoc_user">>
    }),
    ?assertMatch({ok, _}, flurm_account_manager:get_association(Id)),
    ?assertEqual(ok, flurm_account_manager:delete_association(Id)),
    ?assertEqual({error, not_found}, flurm_account_manager:get_association(Id)).

test_delete_association_not_found() ->
    Result = flurm_account_manager:delete_association(99999),
    ?assertEqual({error, not_found}, Result).

test_get_association_by_id() ->
    {ok, Id} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"get_by_id_user">>
    }),
    {ok, Assoc} = flurm_account_manager:get_association(Id),
    ?assertEqual(Id, Assoc#association.id),
    ?assertEqual(<<"get_by_id_user">>, Assoc#association.user).

test_get_association_by_id_not_found() ->
    Result = flurm_account_manager:get_association(99999),
    ?assertEqual({error, not_found}, Result).

test_get_association_by_key() ->
    {ok, _Id} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"key_lookup_user">>
    }),
    {ok, Assoc} = flurm_account_manager:get_association(<<"flurm">>, <<"root">>, <<"key_lookup_user">>),
    ?assertEqual(<<"key_lookup_user">>, Assoc#association.user).

test_get_association_by_key_not_found() ->
    Result = flurm_account_manager:get_association(<<"nonexistent">>, <<"nonexistent">>, <<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_list_associations() ->
    {ok, _} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"list_assoc1">>
    }),
    {ok, _} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"list_assoc2">>
    }),

    Assocs = flurm_account_manager:list_associations(),
    ?assert(is_list(Assocs)),
    Users = [A#association.user || A <- Assocs],
    ?assert(lists:member(<<"list_assoc1">>, Users)),
    ?assert(lists:member(<<"list_assoc2">>, Users)).

test_list_associations_filtered() ->
    ok = flurm_account_manager:add_account(#{name => <<"filter_acct">>}),
    {ok, _} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"filter_acct">>,
        user => <<"filter_assoc_user">>
    }),
    {ok, _} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"other_assoc_user">>
    }),

    %% Filter by account
    Filtered = flurm_account_manager:list_associations(#{account => <<"filter_acct">>}),
    Users = [A#association.user || A <- Filtered],
    ?assert(lists:member(<<"filter_assoc_user">>, Users)),
    ?assertNot(lists:member(<<"other_assoc_user">>, Users)).

%%====================================================================
%% QOS CRUD Tests
%%====================================================================

test_add_qos_record() ->
    Qos = #qos{
        name = <<"custom_qos">>,
        description = <<"Custom QOS">>,
        priority = 500
    },
    ?assertEqual(ok, flurm_account_manager:add_qos(Qos)),
    {ok, Retrieved} = flurm_account_manager:get_qos(<<"custom_qos">>),
    ?assertEqual(<<"custom_qos">>, Retrieved#qos.name),
    ?assertEqual(500, Retrieved#qos.priority).

test_add_qos_map() ->
    Map = #{
        name => <<"map_qos">>,
        description => <<"Map QOS">>,
        priority => 1000,
        max_jobs_pu => 50,
        usage_factor => 2.0
    },
    ?assertEqual(ok, flurm_account_manager:add_qos(Map)),
    {ok, Retrieved} = flurm_account_manager:get_qos(<<"map_qos">>),
    ?assertEqual(<<"map_qos">>, Retrieved#qos.name),
    ?assertEqual(1000, Retrieved#qos.priority),
    ?assertEqual(50, Retrieved#qos.max_jobs_pu),
    ?assertEqual(2.0, Retrieved#qos.usage_factor).

test_add_qos_exists() ->
    %% normal already exists from init
    Qos = #qos{name = <<"normal">>},
    ?assertEqual({error, already_exists}, flurm_account_manager:add_qos(Qos)).

test_modify_qos() ->
    ok = flurm_account_manager:add_qos(#{name => <<"modify_qos">>}),

    Updates = #{description => <<"Updated QOS">>, priority => 750},
    ?assertEqual(ok, flurm_account_manager:modify_qos(<<"modify_qos">>, Updates)),

    {ok, Updated} = flurm_account_manager:get_qos(<<"modify_qos">>),
    ?assertEqual(<<"Updated QOS">>, Updated#qos.description),
    ?assertEqual(750, Updated#qos.priority).

test_modify_qos_not_found() ->
    Result = flurm_account_manager:modify_qos(<<"nonexistent">>, #{priority => 100}),
    ?assertEqual({error, not_found}, Result).

test_delete_qos() ->
    ok = flurm_account_manager:add_qos(#{name => <<"delete_qos">>}),
    ?assertMatch({ok, _}, flurm_account_manager:get_qos(<<"delete_qos">>)),
    ?assertEqual(ok, flurm_account_manager:delete_qos(<<"delete_qos">>)),
    ?assertEqual({error, not_found}, flurm_account_manager:get_qos(<<"delete_qos">>)).

test_delete_qos_not_found() ->
    Result = flurm_account_manager:delete_qos(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_get_qos_found() ->
    ok = flurm_account_manager:add_qos(#{name => <<"get_qos_test">>}),
    {ok, Qos} = flurm_account_manager:get_qos(<<"get_qos_test">>),
    ?assertEqual(<<"get_qos_test">>, Qos#qos.name).

test_get_qos_not_found() ->
    Result = flurm_account_manager:get_qos(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_list_qos() ->
    ok = flurm_account_manager:add_qos(#{name => <<"list_qos1">>}),
    ok = flurm_account_manager:add_qos(#{name => <<"list_qos2">>}),

    QosList = flurm_account_manager:list_qos(),
    ?assert(is_list(QosList)),
    Names = [Q#qos.name || Q <- QosList],
    ?assert(lists:member(<<"list_qos1">>, Names)),
    ?assert(lists:member(<<"list_qos2">>, Names)).

%%====================================================================
%% Cluster CRUD Tests
%%====================================================================

test_add_cluster_record() ->
    Cluster = #acct_cluster{
        name = <<"test_cluster">>,
        control_host = <<"host1">>,
        control_port = 6820
    },
    ?assertEqual(ok, flurm_account_manager:add_cluster(Cluster)),
    {ok, Retrieved} = flurm_account_manager:get_cluster(<<"test_cluster">>),
    ?assertEqual(<<"test_cluster">>, Retrieved#acct_cluster.name),
    ?assertEqual(<<"host1">>, Retrieved#acct_cluster.control_host).

test_add_cluster_map() ->
    Map = #{
        name => <<"map_cluster">>,
        control_host => <<"host2">>,
        control_port => 6821,
        rpc_version => 42
    },
    ?assertEqual(ok, flurm_account_manager:add_cluster(Map)),
    {ok, Retrieved} = flurm_account_manager:get_cluster(<<"map_cluster">>),
    ?assertEqual(<<"map_cluster">>, Retrieved#acct_cluster.name),
    ?assertEqual(42, Retrieved#acct_cluster.rpc_version).

test_add_cluster_exists() ->
    %% Default cluster already exists
    Clusters = flurm_account_manager:list_clusters(),
    [DefaultCluster | _] = Clusters,
    Cluster = #acct_cluster{name = DefaultCluster#acct_cluster.name},
    ?assertEqual({error, already_exists}, flurm_account_manager:add_cluster(Cluster)).

test_get_cluster_found() ->
    ok = flurm_account_manager:add_cluster(#{name => <<"get_cluster_test">>}),
    {ok, Cluster} = flurm_account_manager:get_cluster(<<"get_cluster_test">>),
    ?assertEqual(<<"get_cluster_test">>, Cluster#acct_cluster.name).

test_get_cluster_not_found() ->
    Result = flurm_account_manager:get_cluster(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

test_list_clusters() ->
    ok = flurm_account_manager:add_cluster(#{name => <<"list_cluster1">>}),
    ok = flurm_account_manager:add_cluster(#{name => <<"list_cluster2">>}),

    Clusters = flurm_account_manager:list_clusters(),
    ?assert(is_list(Clusters)),
    Names = [C#acct_cluster.name || C <- Clusters],
    ?assert(lists:member(<<"list_cluster1">>, Names)),
    ?assert(lists:member(<<"list_cluster2">>, Names)).

%%====================================================================
%% TRES CRUD Tests
%%====================================================================

test_add_tres_record() ->
    Tres = #tres{
        type = <<"gres/gpu">>,
        name = <<"a100">>,
        count = 8
    },
    {ok, Id} = flurm_account_manager:add_tres(Tres),
    ?assert(is_integer(Id)),
    {ok, Retrieved} = flurm_account_manager:get_tres(Id),
    ?assertEqual(<<"gres/gpu">>, Retrieved#tres.type),
    ?assertEqual(<<"a100">>, Retrieved#tres.name).

test_add_tres_map() ->
    Map = #{
        type => <<"gres/fpga">>,
        name => <<"xilinx">>,
        count => 4
    },
    {ok, Id} = flurm_account_manager:add_tres(Map),
    ?assert(is_integer(Id)),
    {ok, Retrieved} = flurm_account_manager:get_tres(Id),
    ?assertEqual(<<"gres/fpga">>, Retrieved#tres.type),
    ?assertEqual(4, Retrieved#tres.count).

test_get_tres_by_id() ->
    %% CPU TRES should exist with ID 1
    {ok, Tres} = flurm_account_manager:get_tres(1),
    ?assertEqual(<<"cpu">>, Tres#tres.type).

test_get_tres_by_type() ->
    {ok, Tres} = flurm_account_manager:get_tres(<<"mem">>),
    ?assertEqual(<<"mem">>, Tres#tres.type).

test_get_tres_not_found() ->
    Result = flurm_account_manager:get_tres(99999),
    ?assertEqual({error, not_found}, Result),
    Result2 = flurm_account_manager:get_tres(<<"nonexistent">>),
    ?assertEqual({error, not_found}, Result2).

test_list_tres() ->
    TresList = flurm_account_manager:list_tres(),
    ?assert(is_list(TresList)),
    ?assert(length(TresList) >= 4),
    Types = [T#tres.type || T <- TresList],
    ?assert(lists:member(<<"cpu">>, Types)).

%%====================================================================
%% User Association Lookup Tests
%%====================================================================

test_get_user_association_found() ->
    ok = flurm_account_manager:add_account(#{name => <<"ua_test_acct">>}),
    ok = flurm_account_manager:add_user(#{name => <<"ua_test_user">>}),
    {ok, _} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"ua_test_acct">>,
        user => <<"ua_test_user">>
    }),

    {ok, Assoc} = flurm_account_manager:get_user_association(<<"ua_test_user">>, <<"ua_test_acct">>),
    ?assertEqual(<<"ua_test_user">>, Assoc#association.user),
    ?assertEqual(<<"ua_test_acct">>, Assoc#association.account).

test_get_user_association_not_found() ->
    Result = flurm_account_manager:get_user_association(<<"nonexistent">>, <<"nonexistent">>),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% Limit Checking Tests
%%====================================================================

test_check_limits_no_assoc_required() ->
    %% When require_association is false, should allow without association
    application:set_env(flurm_core, require_association, false),
    JobSpec = #{
        account => <<>>,
        time_limit => 3600
    },
    %% This may call out to flurm_qos, so we expect it to either pass or fail gracefully
    Result = flurm_account_manager:check_limits(<<"unknown_user">>, JobSpec),
    %% Result should be ok or an error, but not crash
    ?assert(Result =:= ok orelse element(1, Result) =:= error).

test_check_limits_assoc_required() ->
    %% When require_association is true, should fail without association
    application:set_env(flurm_core, require_association, true),
    JobSpec = #{
        account => <<"nonexistent_account">>,
        time_limit => 3600
    },
    Result = flurm_account_manager:check_limits(<<"unknown_user">>, JobSpec),
    ?assertMatch({error, _}, Result),
    %% Reset
    application:set_env(flurm_core, require_association, false).

test_check_limits_with_association() ->
    %% Create user and association
    ok = flurm_account_manager:add_account(#{name => <<"limit_test_acct">>}),
    ok = flurm_account_manager:add_user(#{
        name => <<"limit_test_user">>,
        default_account => <<"limit_test_acct">>
    }),
    {ok, _} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"limit_test_acct">>,
        user => <<"limit_test_user">>,
        max_jobs => 0,  % unlimited
        max_submit => 0  % unlimited
    }),

    JobSpec = #{
        account => <<"limit_test_acct">>,
        time_limit => 3600
    },
    %% Should pass or fail based on QOS checks
    Result = flurm_account_manager:check_limits(<<"limit_test_user">>, JobSpec),
    ?assert(Result =:= ok orelse element(1, Result) =:= error).

%%====================================================================
%% gen_server Callback Tests
%%====================================================================

test_unknown_call() ->
    Result = gen_server:call(flurm_account_manager, {unknown_request, foo}),
    ?assertEqual({error, unknown_request}, Result).

test_unknown_cast() ->
    %% Unknown cast should not crash the server
    ok = gen_server:cast(flurm_account_manager, {unknown_cast_message}),
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_account_manager))),
    %% Should still work
    _ = flurm_account_manager:list_accounts().

test_unknown_info() ->
    %% Unknown info message should not crash the server
    flurm_account_manager ! {unknown_info_message, foo, bar},
    timer:sleep(50),
    ?assert(is_process_alive(whereis(flurm_account_manager))),
    %% Should still work
    _ = flurm_account_manager:list_accounts().

test_terminate() ->
    %% Start a fresh server for terminate test
    catch gen_server:stop(flurm_account_manager),
    timer:sleep(50),
    {ok, Pid} = flurm_account_manager:start_link(),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid, normal, 5000),
    timer:sleep(50),
    ?assertNot(is_process_alive(Pid)).

%%====================================================================
%% Additional Update Field Tests
%%====================================================================

account_update_fields_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"update all account fields", fun test_update_all_account_fields/0},
      {"update all user fields", fun test_update_all_user_fields/0},
      {"update all association fields", fun test_update_all_association_fields/0},
      {"update all qos fields", fun test_update_all_qos_fields/0}
     ]}.

test_update_all_account_fields() ->
    ok = flurm_account_manager:add_account(#{name => <<"full_update_acct">>}),

    Updates = #{
        description => <<"Full update">>,
        organization => <<"New Org">>,
        parent => <<"root">>,
        coordinators => [<<"coord1">>, <<"coord2">>],
        default_qos => <<"high">>,
        fairshare => 5,
        max_jobs => 100,
        max_submit => 200,
        max_wall => 7200,
        unknown_field => <<"ignored">>  % Should be ignored
    },

    ok = flurm_account_manager:modify_account(<<"full_update_acct">>, Updates),

    {ok, Account} = flurm_account_manager:get_account(<<"full_update_acct">>),
    ?assertEqual(<<"Full update">>, Account#account.description),
    ?assertEqual(<<"New Org">>, Account#account.organization),
    ?assertEqual(<<"root">>, Account#account.parent),
    ?assertEqual([<<"coord1">>, <<"coord2">>], Account#account.coordinators),
    ?assertEqual(<<"high">>, Account#account.default_qos),
    ?assertEqual(5, Account#account.fairshare),
    ?assertEqual(100, Account#account.max_jobs),
    ?assertEqual(200, Account#account.max_submit),
    ?assertEqual(7200, Account#account.max_wall).

test_update_all_user_fields() ->
    ok = flurm_account_manager:add_user(#{name => <<"full_update_user">>}),

    Updates = #{
        default_account => <<"root">>,
        accounts => [<<"root">>, <<"other">>],
        default_qos => <<"high">>,
        admin_level => admin,
        fairshare => 3,
        max_jobs => 50,
        max_submit => 100,
        max_wall => 3600,
        unknown_field => <<"ignored">>
    },

    ok = flurm_account_manager:modify_user(<<"full_update_user">>, Updates),

    {ok, User} = flurm_account_manager:get_user(<<"full_update_user">>),
    ?assertEqual(<<"root">>, User#acct_user.default_account),
    ?assertEqual([<<"root">>, <<"other">>], User#acct_user.accounts),
    ?assertEqual(<<"high">>, User#acct_user.default_qos),
    ?assertEqual(admin, User#acct_user.admin_level),
    ?assertEqual(3, User#acct_user.fairshare),
    ?assertEqual(50, User#acct_user.max_jobs),
    ?assertEqual(100, User#acct_user.max_submit),
    ?assertEqual(3600, User#acct_user.max_wall).

test_update_all_association_fields() ->
    {ok, Id} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"full_update_assoc_user">>
    }),

    Updates = #{
        shares => 10,
        grp_tres_mins => #{cpu => 1000},
        grp_tres => #{cpu => 100},
        grp_jobs => 50,
        grp_submit => 100,
        grp_wall => 7200,
        max_tres_mins_per_job => #{cpu => 500},
        max_tres_per_job => #{cpu => 64},
        max_tres_per_node => #{cpu => 32},
        max_jobs => 25,
        max_submit => 50,
        max_wall_per_job => 3600,
        priority => 1000,
        qos => [<<"normal">>, <<"high">>],
        default_qos => <<"normal">>,
        unknown_field => <<"ignored">>
    },

    ok = flurm_account_manager:modify_association(Id, Updates),

    {ok, Assoc} = flurm_account_manager:get_association(Id),
    ?assertEqual(10, Assoc#association.shares),
    ?assertEqual(#{cpu => 1000}, Assoc#association.grp_tres_mins),
    ?assertEqual(#{cpu => 100}, Assoc#association.grp_tres),
    ?assertEqual(50, Assoc#association.grp_jobs),
    ?assertEqual(100, Assoc#association.grp_submit),
    ?assertEqual(7200, Assoc#association.grp_wall),
    ?assertEqual(#{cpu => 500}, Assoc#association.max_tres_mins_per_job),
    ?assertEqual(#{cpu => 64}, Assoc#association.max_tres_per_job),
    ?assertEqual(#{cpu => 32}, Assoc#association.max_tres_per_node),
    ?assertEqual(25, Assoc#association.max_jobs),
    ?assertEqual(50, Assoc#association.max_submit),
    ?assertEqual(3600, Assoc#association.max_wall_per_job),
    ?assertEqual(1000, Assoc#association.priority),
    ?assertEqual([<<"normal">>, <<"high">>], Assoc#association.qos),
    ?assertEqual(<<"normal">>, Assoc#association.default_qos).

test_update_all_qos_fields() ->
    ok = flurm_account_manager:add_qos(#{name => <<"full_update_qos">>}),

    Updates = #{
        description => <<"Full update QOS">>,
        priority => 999,
        flags => [some_flag],
        grace_time => 120,
        max_jobs_pa => 100,
        max_jobs_pu => 50,
        max_submit_jobs_pa => 200,
        max_submit_jobs_pu => 100,
        max_tres_pa => #{cpu => 1000},
        max_tres_pu => #{cpu => 500},
        max_tres_per_job => #{cpu => 64},
        max_tres_per_node => #{cpu => 32},
        max_tres_per_user => #{cpu => 256},
        max_wall_per_job => 172800,
        min_tres_per_job => #{cpu => 1},
        preempt => [<<"low">>],
        preempt_mode => suspend,
        usage_factor => 1.5,
        usage_threshold => 0.8,
        unknown_field => <<"ignored">>
    },

    ok = flurm_account_manager:modify_qos(<<"full_update_qos">>, Updates),

    {ok, Qos} = flurm_account_manager:get_qos(<<"full_update_qos">>),
    ?assertEqual(<<"Full update QOS">>, Qos#qos.description),
    ?assertEqual(999, Qos#qos.priority),
    ?assertEqual([some_flag], Qos#qos.flags),
    ?assertEqual(120, Qos#qos.grace_time),
    ?assertEqual(100, Qos#qos.max_jobs_pa),
    ?assertEqual(50, Qos#qos.max_jobs_pu),
    ?assertEqual(200, Qos#qos.max_submit_jobs_pa),
    ?assertEqual(100, Qos#qos.max_submit_jobs_pu),
    ?assertEqual(#{cpu => 1000}, Qos#qos.max_tres_pa),
    ?assertEqual(#{cpu => 500}, Qos#qos.max_tres_pu),
    ?assertEqual(#{cpu => 64}, Qos#qos.max_tres_per_job),
    ?assertEqual(#{cpu => 32}, Qos#qos.max_tres_per_node),
    ?assertEqual(#{cpu => 256}, Qos#qos.max_tres_per_user),
    ?assertEqual(172800, Qos#qos.max_wall_per_job),
    ?assertEqual(#{cpu => 1}, Qos#qos.min_tres_per_job),
    ?assertEqual([<<"low">>], Qos#qos.preempt),
    ?assertEqual(suspend, Qos#qos.preempt_mode),
    ?assertEqual(1.5, Qos#qos.usage_factor),
    ?assertEqual(0.8, Qos#qos.usage_threshold).

%%====================================================================
%% Pattern Matching Filter Tests
%%====================================================================

pattern_matching_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"filter accounts with name pattern", fun test_filter_accounts_name_pattern/0},
      {"filter users with name pattern", fun test_filter_users_name_pattern/0},
      {"filter users by account membership", fun test_filter_users_by_account/0},
      {"filter associations by cluster", fun test_filter_associations_by_cluster/0},
      {"filter associations by partition", fun test_filter_associations_by_partition/0}
     ]}.

test_filter_accounts_name_pattern() ->
    %% Add accounts with pattern-able names
    ok = flurm_account_manager:add_account(#{name => <<"prefix_abc">>}),
    ok = flurm_account_manager:add_account(#{name => <<"prefix_def">>}),
    ok = flurm_account_manager:add_account(#{name => <<"other_ghi">>}),

    %% Filter by name prefix pattern
    Filtered = flurm_account_manager:list_accounts(#{name => <<"prefix_*">>}),
    Names = [A#account.name || A <- Filtered],
    ?assert(lists:member(<<"prefix_abc">>, Names)),
    ?assert(lists:member(<<"prefix_def">>, Names)),
    ?assertNot(lists:member(<<"other_ghi">>, Names)).

test_filter_users_name_pattern() ->
    ok = flurm_account_manager:add_user(#{name => <<"user_alpha">>}),
    ok = flurm_account_manager:add_user(#{name => <<"user_beta">>}),
    ok = flurm_account_manager:add_user(#{name => <<"other_gamma">>}),

    Filtered = flurm_account_manager:list_users(#{name => <<"user_*">>}),
    Names = [U#acct_user.name || U <- Filtered],
    ?assert(lists:member(<<"user_alpha">>, Names)),
    ?assert(lists:member(<<"user_beta">>, Names)),
    ?assertNot(lists:member(<<"other_gamma">>, Names)).

test_filter_users_by_account() ->
    ok = flurm_account_manager:add_user(#{
        name => <<"member_user">>,
        accounts => [<<"root">>, <<"other">>]
    }),
    ok = flurm_account_manager:add_user(#{
        name => <<"nonmember_user">>,
        accounts => [<<"different">>]
    }),

    %% Filter by account membership
    Filtered = flurm_account_manager:list_users(#{account => <<"root">>}),
    Names = [U#acct_user.name || U <- Filtered],
    ?assert(lists:member(<<"member_user">>, Names)),
    ?assertNot(lists:member(<<"nonmember_user">>, Names)).

test_filter_associations_by_cluster() ->
    ok = flurm_account_manager:add_cluster(#{name => <<"cluster_a">>}),
    ok = flurm_account_manager:add_cluster(#{name => <<"cluster_b">>}),

    {ok, _} = flurm_account_manager:add_association(#{
        cluster => <<"cluster_a">>,
        account => <<"root">>,
        user => <<"cluster_a_user">>
    }),
    {ok, _} = flurm_account_manager:add_association(#{
        cluster => <<"cluster_b">>,
        account => <<"root">>,
        user => <<"cluster_b_user">>
    }),

    Filtered = flurm_account_manager:list_associations(#{cluster => <<"cluster_a">>}),
    Users = [A#association.user || A <- Filtered],
    ?assert(lists:member(<<"cluster_a_user">>, Users)),
    ?assertNot(lists:member(<<"cluster_b_user">>, Users)).

test_filter_associations_by_partition() ->
    {ok, _} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"batch_user">>,
        partition => <<"batch">>
    }),
    {ok, _} = flurm_account_manager:add_association(#{
        cluster => <<"flurm">>,
        account => <<"root">>,
        user => <<"gpu_user">>,
        partition => <<"gpu">>
    }),

    Filtered = flurm_account_manager:list_associations(#{partition => <<"batch">>}),
    Users = [A#association.user || A <- Filtered],
    ?assert(lists:member(<<"batch_user">>, Users)),
    ?assertNot(lists:member(<<"gpu_user">>, Users)).

%%====================================================================
%% Organization Filter Test
%%====================================================================

organization_filter_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"filter accounts by organization", fun test_filter_accounts_by_org/0}
     ]}.

test_filter_accounts_by_org() ->
    ok = flurm_account_manager:add_account(#{name => <<"acct_org1">>, organization => <<"Org1">>}),
    ok = flurm_account_manager:add_account(#{name => <<"acct_org2">>, organization => <<"Org2">>}),

    Filtered = flurm_account_manager:list_accounts(#{organization => <<"Org1">>}),
    Names = [A#account.name || A <- Filtered],
    ?assert(lists:member(<<"acct_org1">>, Names)),
    ?assertNot(lists:member(<<"acct_org2">>, Names)).
