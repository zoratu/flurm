%%%-------------------------------------------------------------------
%%% @doc Comprehensive tests for flurm_account_manager module
%%% Achieves 100% code coverage for account management.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_account_manager_100cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Stop any existing instance
    catch gen_server:stop(flurm_account_manager),
    timer:sleep(50),

    %% Mock lager
    meck:new(lager, [passthrough, non_strict]),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),

    %% Mock flurm_limits
    meck:new(flurm_limits, [passthrough, non_strict]),
    meck:expect(flurm_limits, get_usage, fun(_, _) ->
        {usage, {user, <<"test">>}, 0, 0, #{}, #{}}
    end),

    %% Mock flurm_qos
    meck:new(flurm_qos, [passthrough, non_strict]),
    meck:expect(flurm_qos, check_limits, fun(_, _) -> ok end),
    meck:expect(flurm_qos, check_tres_limits, fun(_, _, _) -> ok end),

    %% Mock flurm_tres
    meck:new(flurm_tres, [passthrough, non_strict]),
    meck:expect(flurm_tres, add, fun(M1, M2) ->
        maps:merge_with(fun(_, V1, V2) -> V1 + V2 end, M1, M2)
    end),

    %% Start the account manager
    {ok, Pid} = flurm_account_manager:start_link(),
    Pid.

cleanup(Pid) ->
    catch gen_server:stop(Pid),
    meck:unload(lager),
    meck:unload(flurm_limits),
    meck:unload(flurm_qos),
    meck:unload(flurm_tres),
    timer:sleep(50),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

account_manager_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        %% Account Tests
        {"add_account with record", fun test_add_account_record/0},
        {"add_account with map", fun test_add_account_map/0},
        {"add_account already exists", fun test_add_account_exists/0},
        {"modify_account success", fun test_modify_account/0},
        {"modify_account not found", fun test_modify_account_not_found/0},
        {"delete_account success", fun test_delete_account/0},
        {"delete_account not found", fun test_delete_account_not_found/0},
        {"delete_account has children", fun test_delete_account_children/0},
        {"get_account success", fun test_get_account/0},
        {"get_account not found", fun test_get_account_not_found/0},
        {"list_accounts", fun test_list_accounts/0},
        {"list_accounts with filter", fun test_list_accounts_filter/0},

        %% User Tests
        {"add_user with record", fun test_add_user_record/0},
        {"add_user with map", fun test_add_user_map/0},
        {"add_user already exists", fun test_add_user_exists/0},
        {"modify_user success", fun test_modify_user/0},
        {"modify_user not found", fun test_modify_user_not_found/0},
        {"delete_user success", fun test_delete_user/0},
        {"delete_user not found", fun test_delete_user_not_found/0},
        {"get_user success", fun test_get_user/0},
        {"get_user not found", fun test_get_user_not_found/0},
        {"list_users", fun test_list_users/0},
        {"list_users with filter", fun test_list_users_filter/0},

        %% Association Tests
        {"add_association with record", fun test_add_association_record/0},
        {"add_association with map", fun test_add_association_map/0},
        {"modify_association success", fun test_modify_association/0},
        {"modify_association not found", fun test_modify_association_not_found/0},
        {"delete_association success", fun test_delete_association/0},
        {"delete_association not found", fun test_delete_association_not_found/0},
        {"get_association by id", fun test_get_association_id/0},
        {"get_association by key", fun test_get_association_key/0},
        {"get_association not found", fun test_get_association_not_found/0},
        {"list_associations", fun test_list_associations/0},
        {"list_associations with filter", fun test_list_associations_filter/0},

        %% QOS Tests
        {"add_qos with record", fun test_add_qos_record/0},
        {"add_qos with map", fun test_add_qos_map/0},
        {"add_qos already exists", fun test_add_qos_exists/0},
        {"modify_qos success", fun test_modify_qos/0},
        {"modify_qos not found", fun test_modify_qos_not_found/0},
        {"delete_qos success", fun test_delete_qos/0},
        {"delete_qos not found", fun test_delete_qos_not_found/0},
        {"get_qos success", fun test_get_qos/0},
        {"get_qos not found", fun test_get_qos_not_found/0},
        {"list_qos", fun test_list_qos/0},

        %% Cluster Tests
        {"add_cluster with record", fun test_add_cluster_record/0},
        {"add_cluster with map", fun test_add_cluster_map/0},
        {"add_cluster already exists", fun test_add_cluster_exists/0},
        {"get_cluster success", fun test_get_cluster/0},
        {"get_cluster not found", fun test_get_cluster_not_found/0},
        {"list_clusters", fun test_list_clusters/0},

        %% TRES Tests
        {"add_tres", fun test_add_tres/0},
        {"get_tres by id", fun test_get_tres_id/0},
        {"get_tres by type", fun test_get_tres_type/0},
        {"get_tres not found", fun test_get_tres_not_found/0},
        {"list_tres", fun test_list_tres/0},

        %% Utility Tests
        {"get_user_association", fun test_get_user_association/0},
        {"check_limits success", fun test_check_limits_success/0},
        {"check_limits no association", fun test_check_limits_no_association/0}
     ]}.

internal_functions_test_() ->
    [
        {"normalize_account from record", fun test_normalize_account_record/0},
        {"normalize_account from map", fun test_normalize_account_map/0},
        {"normalize_user from record", fun test_normalize_user_record/0},
        {"normalize_user from map", fun test_normalize_user_map/0},
        {"normalize_association from record", fun test_normalize_association_record/0},
        {"normalize_association from map", fun test_normalize_association_map/0},
        {"normalize_qos from record", fun test_normalize_qos_record/0},
        {"normalize_qos from map", fun test_normalize_qos_map/0},
        {"normalize_cluster from record", fun test_normalize_cluster_record/0},
        {"normalize_cluster from map", fun test_normalize_cluster_map/0},
        {"normalize_tres from record", fun test_normalize_tres_record/0},
        {"normalize_tres from map", fun test_normalize_tres_map/0},
        {"apply_account_updates", fun test_apply_account_updates/0},
        {"apply_user_updates", fun test_apply_user_updates/0},
        {"apply_association_updates", fun test_apply_association_updates/0},
        {"apply_qos_updates", fun test_apply_qos_updates/0},
        {"matches_pattern exact", fun test_matches_pattern_exact/0},
        {"matches_pattern wildcard", fun test_matches_pattern_wildcard/0},
        {"matches_pattern no match", fun test_matches_pattern_no_match/0},
        {"build_tres_request basic", fun test_build_tres_request_basic/0},
        {"build_tres_request with gpus", fun test_build_tres_request_gpus/0},
        {"check_tres_limits ok", fun test_check_tres_limits_ok/0},
        {"check_tres_limits exceeded", fun test_check_tres_limits_exceeded/0},
        {"combine_tres_maps", fun test_combine_tres_maps/0},
        {"run_checks all pass", fun test_run_checks_pass/0},
        {"run_checks first fails", fun test_run_checks_fail/0}
    ].

%%====================================================================
%% Account Tests
%%====================================================================

test_add_account_record() ->
    Account = #account{name = <<"test_acct">>, description = <<"Test">>},
    ?assertEqual(ok, flurm_account_manager:add_account(Account)).

test_add_account_map() ->
    AccountMap = #{name => <<"map_acct">>, description => <<"Map Test">>},
    ?assertEqual(ok, flurm_account_manager:add_account(AccountMap)).

test_add_account_exists() ->
    Account = #account{name = <<"dup_acct">>},
    flurm_account_manager:add_account(Account),
    ?assertEqual({error, already_exists}, flurm_account_manager:add_account(Account)).

test_modify_account() ->
    Account = #account{name = <<"mod_acct">>},
    flurm_account_manager:add_account(Account),
    ?assertEqual(ok, flurm_account_manager:modify_account(<<"mod_acct">>, #{description => <<"New Desc">>})).

test_modify_account_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:modify_account(<<"nonexistent">>, #{})).

test_delete_account() ->
    Account = #account{name = <<"del_acct">>},
    flurm_account_manager:add_account(Account),
    ?assertEqual(ok, flurm_account_manager:delete_account(<<"del_acct">>)).

test_delete_account_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:delete_account(<<"nonexistent">>)).

test_delete_account_children() ->
    Parent = #account{name = <<"parent_acct">>},
    Child = #account{name = <<"child_acct">>, parent = <<"parent_acct">>},
    flurm_account_manager:add_account(Parent),
    flurm_account_manager:add_account(Child),
    ?assertEqual({error, has_children}, flurm_account_manager:delete_account(<<"parent_acct">>)).

test_get_account() ->
    Account = #account{name = <<"get_acct">>},
    flurm_account_manager:add_account(Account),
    {ok, Retrieved} = flurm_account_manager:get_account(<<"get_acct">>),
    ?assertEqual(<<"get_acct">>, Retrieved#account.name).

test_get_account_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:get_account(<<"nonexistent">>)).

test_list_accounts() ->
    Accounts = flurm_account_manager:list_accounts(),
    ?assert(is_list(Accounts)),
    ?assert(length(Accounts) >= 1). % At least root account

test_list_accounts_filter() ->
    Account = #account{name = <<"filter_acct">>, organization = <<"TestOrg">>},
    flurm_account_manager:add_account(Account),
    Accounts = flurm_account_manager:list_accounts(#{organization => <<"TestOrg">>}),
    ?assertEqual(1, length(Accounts)).

%%====================================================================
%% User Tests
%%====================================================================

test_add_user_record() ->
    User = #acct_user{name = <<"test_user">>},
    ?assertEqual(ok, flurm_account_manager:add_user(User)).

test_add_user_map() ->
    UserMap = #{name => <<"map_user">>},
    ?assertEqual(ok, flurm_account_manager:add_user(UserMap)).

test_add_user_exists() ->
    User = #acct_user{name = <<"dup_user">>},
    flurm_account_manager:add_user(User),
    ?assertEqual({error, already_exists}, flurm_account_manager:add_user(User)).

test_modify_user() ->
    User = #acct_user{name = <<"mod_user">>},
    flurm_account_manager:add_user(User),
    ?assertEqual(ok, flurm_account_manager:modify_user(<<"mod_user">>, #{admin_level => operator})).

test_modify_user_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:modify_user(<<"nonexistent">>, #{})).

test_delete_user() ->
    User = #acct_user{name = <<"del_user">>},
    flurm_account_manager:add_user(User),
    ?assertEqual(ok, flurm_account_manager:delete_user(<<"del_user">>)).

test_delete_user_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:delete_user(<<"nonexistent">>)).

test_get_user() ->
    User = #acct_user{name = <<"get_user">>},
    flurm_account_manager:add_user(User),
    {ok, Retrieved} = flurm_account_manager:get_user(<<"get_user">>),
    ?assertEqual(<<"get_user">>, Retrieved#acct_user.name).

test_get_user_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:get_user(<<"nonexistent">>)).

test_list_users() ->
    Users = flurm_account_manager:list_users(),
    ?assert(is_list(Users)).

test_list_users_filter() ->
    User = #acct_user{name = <<"filter_user">>, admin_level = operator},
    flurm_account_manager:add_user(User),
    Users = flurm_account_manager:list_users(#{admin_level => operator}),
    ?assert(length(Users) >= 1).

%%====================================================================
%% Association Tests
%%====================================================================

test_add_association_record() ->
    Assoc = #association{account = <<"root">>, user = <<"assoc_user">>},
    {ok, Id} = flurm_account_manager:add_association(Assoc),
    ?assert(is_integer(Id)).

test_add_association_map() ->
    AssocMap = #{account => <<"root">>, user => <<"map_assoc_user">>},
    {ok, Id} = flurm_account_manager:add_association(AssocMap),
    ?assert(is_integer(Id)).

test_modify_association() ->
    Assoc = #association{account = <<"root">>, user = <<"mod_assoc_user">>},
    {ok, Id} = flurm_account_manager:add_association(Assoc),
    ?assertEqual(ok, flurm_account_manager:modify_association(Id, #{shares => 10})).

test_modify_association_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:modify_association(99999, #{})).

test_delete_association() ->
    Assoc = #association{account = <<"root">>, user = <<"del_assoc_user">>},
    {ok, Id} = flurm_account_manager:add_association(Assoc),
    ?assertEqual(ok, flurm_account_manager:delete_association(Id)).

test_delete_association_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:delete_association(99999)).

test_get_association_id() ->
    Assoc = #association{account = <<"root">>, user = <<"get_assoc_user">>},
    {ok, Id} = flurm_account_manager:add_association(Assoc),
    {ok, Retrieved} = flurm_account_manager:get_association(Id),
    ?assertEqual(Id, Retrieved#association.id).

test_get_association_key() ->
    Assoc = #association{cluster = <<"flurm">>, account = <<"root">>, user = <<"key_assoc_user">>},
    flurm_account_manager:add_association(Assoc),
    {ok, Retrieved} = flurm_account_manager:get_association(<<"flurm">>, <<"root">>, <<"key_assoc_user">>),
    ?assertEqual(<<"key_assoc_user">>, Retrieved#association.user).

test_get_association_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:get_association(99999)).

test_list_associations() ->
    Assocs = flurm_account_manager:list_associations(),
    ?assert(is_list(Assocs)).

test_list_associations_filter() ->
    Assoc = #association{cluster = <<"flurm">>, account = <<"root">>, user = <<"filter_assoc">>},
    flurm_account_manager:add_association(Assoc),
    Assocs = flurm_account_manager:list_associations(#{account => <<"root">>}),
    ?assert(length(Assocs) >= 1).

%%====================================================================
%% QOS Tests
%%====================================================================

test_add_qos_record() ->
    Qos = #qos{name = <<"test_qos">>},
    ?assertEqual(ok, flurm_account_manager:add_qos(Qos)).

test_add_qos_map() ->
    QosMap = #{name => <<"map_qos">>},
    ?assertEqual(ok, flurm_account_manager:add_qos(QosMap)).

test_add_qos_exists() ->
    %% "normal" QOS exists by default
    ?assertEqual({error, already_exists}, flurm_account_manager:add_qos(#qos{name = <<"normal">>})).

test_modify_qos() ->
    Qos = #qos{name = <<"mod_qos">>},
    flurm_account_manager:add_qos(Qos),
    ?assertEqual(ok, flurm_account_manager:modify_qos(<<"mod_qos">>, #{priority => 100})).

test_modify_qos_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:modify_qos(<<"nonexistent">>, #{})).

test_delete_qos() ->
    Qos = #qos{name = <<"del_qos">>},
    flurm_account_manager:add_qos(Qos),
    ?assertEqual(ok, flurm_account_manager:delete_qos(<<"del_qos">>)).

test_delete_qos_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:delete_qos(<<"nonexistent">>)).

test_get_qos() ->
    {ok, Qos} = flurm_account_manager:get_qos(<<"normal">>),
    ?assertEqual(<<"normal">>, Qos#qos.name).

test_get_qos_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:get_qos(<<"nonexistent">>)).

test_list_qos() ->
    QosList = flurm_account_manager:list_qos(),
    ?assert(is_list(QosList)),
    ?assert(length(QosList) >= 1).

%%====================================================================
%% Cluster Tests
%%====================================================================

test_add_cluster_record() ->
    Cluster = #acct_cluster{name = <<"test_cluster">>},
    ?assertEqual(ok, flurm_account_manager:add_cluster(Cluster)).

test_add_cluster_map() ->
    ClusterMap = #{name => <<"map_cluster">>},
    ?assertEqual(ok, flurm_account_manager:add_cluster(ClusterMap)).

test_add_cluster_exists() ->
    %% Default cluster exists
    ?assertEqual({error, already_exists}, flurm_account_manager:add_cluster(#acct_cluster{name = <<"flurm">>})).

test_get_cluster() ->
    {ok, Cluster} = flurm_account_manager:get_cluster(<<"flurm">>),
    ?assertEqual(<<"flurm">>, Cluster#acct_cluster.name).

test_get_cluster_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:get_cluster(<<"nonexistent">>)).

test_list_clusters() ->
    Clusters = flurm_account_manager:list_clusters(),
    ?assert(is_list(Clusters)),
    ?assert(length(Clusters) >= 1).

%%====================================================================
%% TRES Tests
%%====================================================================

test_add_tres() ->
    Tres = #tres{type = <<"test_tres">>},
    {ok, Id} = flurm_account_manager:add_tres(Tres),
    ?assert(is_integer(Id)).

test_get_tres_id() ->
    {ok, Tres} = flurm_account_manager:get_tres(1),  % cpu TRES
    ?assertEqual(<<"cpu">>, Tres#tres.type).

test_get_tres_type() ->
    {ok, Tres} = flurm_account_manager:get_tres(<<"cpu">>),
    ?assertEqual(1, Tres#tres.id).

test_get_tres_not_found() ->
    ?assertEqual({error, not_found}, flurm_account_manager:get_tres(99999)).

test_list_tres() ->
    TresList = flurm_account_manager:list_tres(),
    ?assert(is_list(TresList)),
    ?assert(length(TresList) >= 4). % cpu, mem, energy, node

%%====================================================================
%% Utility Tests
%%====================================================================

test_get_user_association() ->
    User = #acct_user{name = <<"assoc_test_user">>},
    flurm_account_manager:add_user(User),
    Assoc = #association{account = <<"root">>, user = <<"assoc_test_user">>},
    flurm_account_manager:add_association(Assoc),
    {ok, Retrieved} = flurm_account_manager:get_user_association(<<"assoc_test_user">>, <<"root">>),
    ?assertEqual(<<"assoc_test_user">>, Retrieved#association.user).

test_check_limits_success() ->
    User = #acct_user{name = <<"limit_user">>, default_account = <<"root">>},
    flurm_account_manager:add_user(User),
    Assoc = #association{account = <<"root">>, user = <<"limit_user">>},
    flurm_account_manager:add_association(Assoc),
    JobSpec = #{num_cpus => 1, memory_mb => 1024},
    Result = flurm_account_manager:check_limits(<<"limit_user">>, JobSpec),
    ?assertEqual(ok, Result).

test_check_limits_no_association() ->
    JobSpec = #{num_cpus => 1, memory_mb => 1024},
    %% Should still work when require_association is false (default)
    Result = flurm_account_manager:check_limits(<<"no_assoc_user">>, JobSpec),
    ?assertEqual(ok, Result).

%%====================================================================
%% Internal Function Tests
%%====================================================================

test_normalize_account_record() ->
    Account = #account{name = <<"test">>},
    Result = flurm_account_manager:normalize_account(Account),
    ?assertEqual(Account, Result).

test_normalize_account_map() ->
    Map = #{name => <<"test">>, description => <<"Desc">>},
    Result = flurm_account_manager:normalize_account(Map),
    ?assertEqual(<<"test">>, Result#account.name),
    ?assertEqual(<<"Desc">>, Result#account.description).

test_normalize_user_record() ->
    User = #acct_user{name = <<"test">>},
    Result = flurm_account_manager:normalize_user(User),
    ?assertEqual(User, Result).

test_normalize_user_map() ->
    Map = #{name => <<"test">>, admin_level => operator},
    Result = flurm_account_manager:normalize_user(Map),
    ?assertEqual(<<"test">>, Result#acct_user.name),
    ?assertEqual(operator, Result#acct_user.admin_level).

test_normalize_association_record() ->
    Assoc = #association{id = 1, account = <<"test">>},
    Result = flurm_account_manager:normalize_association(Assoc),
    ?assertEqual(Assoc, Result).

test_normalize_association_map() ->
    Map = #{account => <<"test">>, user => <<"user">>},
    Result = flurm_account_manager:normalize_association(Map),
    ?assertEqual(<<"test">>, Result#association.account),
    ?assertEqual(<<"user">>, Result#association.user).

test_normalize_qos_record() ->
    Qos = #qos{name = <<"test">>},
    Result = flurm_account_manager:normalize_qos(Qos),
    ?assertEqual(Qos, Result).

test_normalize_qos_map() ->
    Map = #{name => <<"test">>, priority => 100},
    Result = flurm_account_manager:normalize_qos(Map),
    ?assertEqual(<<"test">>, Result#qos.name),
    ?assertEqual(100, Result#qos.priority).

test_normalize_cluster_record() ->
    Cluster = #acct_cluster{name = <<"test">>},
    Result = flurm_account_manager:normalize_cluster(Cluster),
    ?assertEqual(Cluster, Result).

test_normalize_cluster_map() ->
    Map = #{name => <<"test">>, control_port => 6817},
    Result = flurm_account_manager:normalize_cluster(Map),
    ?assertEqual(<<"test">>, Result#acct_cluster.name),
    ?assertEqual(6817, Result#acct_cluster.control_port).

test_normalize_tres_record() ->
    Tres = #tres{id = 1, type = <<"cpu">>},
    Result = flurm_account_manager:normalize_tres(Tres),
    ?assertEqual(Tres, Result).

test_normalize_tres_map() ->
    Map = #{type => <<"gpu">>, count => 4},
    Result = flurm_account_manager:normalize_tres(Map),
    ?assertEqual(<<"gpu">>, Result#tres.type),
    ?assertEqual(4, Result#tres.count).

test_apply_account_updates() ->
    Account = #account{name = <<"test">>, description = <<"Old">>},
    Updates = #{description => <<"New">>, fairshare => 2, unknown_field => ignored},
    Result = flurm_account_manager:apply_account_updates(Account, Updates),
    ?assertEqual(<<"New">>, Result#account.description),
    ?assertEqual(2, Result#account.fairshare).

test_apply_user_updates() ->
    User = #acct_user{name = <<"test">>, admin_level = none},
    Updates = #{admin_level => operator, max_jobs => 10},
    Result = flurm_account_manager:apply_user_updates(User, Updates),
    ?assertEqual(operator, Result#acct_user.admin_level),
    ?assertEqual(10, Result#acct_user.max_jobs).

test_apply_association_updates() ->
    Assoc = #association{id = 1, shares = 1},
    Updates = #{shares => 5, max_jobs => 100},
    Result = flurm_account_manager:apply_association_updates(Assoc, Updates),
    ?assertEqual(5, Result#association.shares),
    ?assertEqual(100, Result#association.max_jobs).

test_apply_qos_updates() ->
    Qos = #qos{name = <<"test">>, priority = 0},
    Updates = #{priority => 100, usage_factor => 2.0},
    Result = flurm_account_manager:apply_qos_updates(Qos, Updates),
    ?assertEqual(100, Result#qos.priority),
    ?assertEqual(2.0, Result#qos.usage_factor).

test_matches_pattern_exact() ->
    ?assertEqual(true, flurm_account_manager:matches_pattern(<<"test">>, <<"test">>)).

test_matches_pattern_wildcard() ->
    ?assertEqual(true, flurm_account_manager:matches_pattern(<<"test_account">>, <<"test*">>)),
    ?assertEqual(true, flurm_account_manager:matches_pattern(<<"test">>, <<"test*">>)).

test_matches_pattern_no_match() ->
    ?assertEqual(false, flurm_account_manager:matches_pattern(<<"other">>, <<"test">>)),
    ?assertEqual(false, flurm_account_manager:matches_pattern(<<"other">>, <<"test*">>)).

test_build_tres_request_basic() ->
    JobSpec = #{num_nodes => 2, num_cpus => 4, memory_mb => 8192},
    Result = flurm_account_manager:build_tres_request(JobSpec),
    ?assertEqual(4, maps:get(<<"cpu">>, Result)),
    ?assertEqual(8192, maps:get(<<"mem">>, Result)),
    ?assertEqual(2, maps:get(<<"node">>, Result)).

test_build_tres_request_gpus() ->
    JobSpec = #{num_nodes => 1, num_cpus => 8, memory_mb => 16384, num_gpus => 2},
    Result = flurm_account_manager:build_tres_request(JobSpec),
    ?assertEqual(2, maps:get(<<"gres/gpu">>, Result)).

test_check_tres_limits_ok() ->
    Requested = #{<<"cpu">> => 4, <<"mem">> => 8192},
    Limits = #{<<"cpu">> => 100, <<"mem">> => 100000},
    Result = flurm_account_manager:check_tres_limits(Requested, Limits, test_limit),
    ?assertEqual(ok, Result).

test_check_tres_limits_exceeded() ->
    Requested = #{<<"cpu">> => 100, <<"mem">> => 8192},
    Limits = #{<<"cpu">> => 10, <<"mem">> => 100000},
    Result = flurm_account_manager:check_tres_limits(Requested, Limits, test_limit),
    ?assertMatch({error, {test_limit, _}}, Result).

test_combine_tres_maps() ->
    Map1 = #{<<"cpu">> => 4, <<"mem">> => 1000},
    Map2 = #{<<"cpu">> => 2, <<"mem">> => 500, <<"node">> => 1},
    Result = flurm_account_manager:combine_tres_maps(Map1, Map2),
    ?assertEqual(6, maps:get(<<"cpu">>, Result)),
    ?assertEqual(1500, maps:get(<<"mem">>, Result)),
    ?assertEqual(1, maps:get(<<"node">>, Result)).

test_run_checks_pass() ->
    Checks = [fun() -> ok end, fun() -> ok end, fun() -> ok end],
    Result = flurm_account_manager:run_checks(Checks),
    ?assertEqual(ok, Result).

test_run_checks_fail() ->
    Checks = [fun() -> ok end, fun() -> {error, test_error} end, fun() -> ok end],
    Result = flurm_account_manager:run_checks(Checks),
    ?assertEqual({error, test_error}, Result).
