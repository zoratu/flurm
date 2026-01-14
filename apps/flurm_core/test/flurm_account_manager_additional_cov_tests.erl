%%%-------------------------------------------------------------------
%%% @doc Coverage Tests for FLURM Account Manager
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_account_manager_additional_cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

account_manager_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
      fun per_test_setup/0,
      fun per_test_cleanup/1,
      [
       fun account_tests/1,
       fun user_tests/1,
       fun qos_tests/1,
       fun cluster_tests/1,
       fun tres_tests/1
      ]}}.

setup() ->
    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    meck:new(flurm_limits, [non_strict, no_link]),
    meck:expect(flurm_limits, get_usage, fun(_, _) -> undefined end),
    ok.

cleanup(_) ->
    catch meck:unload(lager),
    catch meck:unload(flurm_limits),
    ok.

per_test_setup() ->
    {ok, Pid} = flurm_account_manager:start_link(),
    Pid.

per_test_cleanup(Pid) ->
    catch gen_server:stop(Pid),
    ok.

account_tests(_Pid) ->
    [
        {"add_account with record", fun() ->
            Account = #account{
                name = <<"testacct">>,
                description = <<"Test account">>,
                organization = <<"TestOrg">>
            },
            ?assertEqual(ok, flurm_account_manager:add_account(Account))
        end},

        {"add_account with map", fun() ->
            Account = #{
                name => <<"mapacct">>,
                description => <<"Map account">>,
                organization => <<"MapOrg">>
            },
            ?assertEqual(ok, flurm_account_manager:add_account(Account))
        end},

        {"add_account duplicate fails", fun() ->
            Account = #account{name = <<"dupacct">>},
            flurm_account_manager:add_account(Account),
            ?assertEqual({error, already_exists}, flurm_account_manager:add_account(Account))
        end},

        {"get_account existing", fun() ->
            Account = #account{name = <<"getacct">>, description = <<"Get account">>},
            flurm_account_manager:add_account(Account),
            {ok, Retrieved} = flurm_account_manager:get_account(<<"getacct">>),
            ?assertEqual(<<"getacct">>, Retrieved#account.name)
        end},

        {"get_account non-existent", fun() ->
            ?assertEqual({error, not_found}, flurm_account_manager:get_account(<<"nonexistent">>))
        end},

        {"modify_account updates fields", fun() ->
            Account = #account{name = <<"modacct">>, description = <<"Original">>},
            flurm_account_manager:add_account(Account),
            ?assertEqual(ok, flurm_account_manager:modify_account(<<"modacct">>, #{description => <<"Modified">>})),
            {ok, Modified} = flurm_account_manager:get_account(<<"modacct">>),
            ?assertEqual(<<"Modified">>, Modified#account.description)
        end},

        {"delete_account existing", fun() ->
            Account = #account{name = <<"delacct">>},
            flurm_account_manager:add_account(Account),
            ?assertEqual(ok, flurm_account_manager:delete_account(<<"delacct">>)),
            ?assertEqual({error, not_found}, flurm_account_manager:get_account(<<"delacct">>))
        end},

        {"list_accounts returns all", fun() ->
            Accounts = flurm_account_manager:list_accounts(),
            ?assert(is_list(Accounts)),
            ?assert(length(Accounts) >= 1)
        end}
    ].

user_tests(_Pid) ->
    [
        {"add_user with record", fun() ->
            User = #acct_user{
                name = <<"testuser">>,
                default_account = <<"root">>
            },
            ?assertEqual(ok, flurm_account_manager:add_user(User))
        end},

        {"add_user duplicate fails", fun() ->
            User = #acct_user{name = <<"dupuser">>},
            flurm_account_manager:add_user(User),
            ?assertEqual({error, already_exists}, flurm_account_manager:add_user(User))
        end},

        {"get_user existing", fun() ->
            User = #acct_user{name = <<"getuser">>},
            flurm_account_manager:add_user(User),
            {ok, Retrieved} = flurm_account_manager:get_user(<<"getuser">>),
            ?assertEqual(<<"getuser">>, Retrieved#acct_user.name)
        end},

        {"delete_user existing", fun() ->
            User = #acct_user{name = <<"deluser">>},
            flurm_account_manager:add_user(User),
            ?assertEqual(ok, flurm_account_manager:delete_user(<<"deluser">>)),
            ?assertEqual({error, not_found}, flurm_account_manager:get_user(<<"deluser">>))
        end},

        {"list_users returns all", fun() ->
            User = #acct_user{name = <<"listuser">>},
            flurm_account_manager:add_user(User),
            Users = flurm_account_manager:list_users(),
            ?assert(is_list(Users)),
            ?assert(length(Users) >= 1)
        end}
    ].

qos_tests(_Pid) ->
    [
        {"add_qos with record", fun() ->
            Qos = #qos{
                name = <<"highprio">>,
                description = <<"High priority QOS">>,
                priority = 1000
            },
            ?assertEqual(ok, flurm_account_manager:add_qos(Qos))
        end},

        {"add_qos duplicate fails", fun() ->
            Qos = #qos{name = <<"dupqos">>},
            flurm_account_manager:add_qos(Qos),
            ?assertEqual({error, already_exists}, flurm_account_manager:add_qos(Qos))
        end},

        {"get_qos existing", fun() ->
            Qos = #qos{name = <<"getqos">>, priority = 750},
            flurm_account_manager:add_qos(Qos),
            {ok, Retrieved} = flurm_account_manager:get_qos(<<"getqos">>),
            ?assertEqual(<<"getqos">>, Retrieved#qos.name)
        end},

        {"delete_qos existing", fun() ->
            Qos = #qos{name = <<"delqos">>},
            flurm_account_manager:add_qos(Qos),
            ?assertEqual(ok, flurm_account_manager:delete_qos(<<"delqos">>)),
            ?assertEqual({error, not_found}, flurm_account_manager:get_qos(<<"delqos">>))
        end},

        {"list_qos returns all", fun() ->
            QosList = flurm_account_manager:list_qos(),
            ?assert(is_list(QosList)),
            ?assert(length(QosList) >= 1)
        end}
    ].

cluster_tests(_Pid) ->
    [
        {"add_cluster with record", fun() ->
            Cluster = #acct_cluster{
                name = <<"testcluster">>,
                control_host = <<"controller.test.com">>
            },
            ?assertEqual(ok, flurm_account_manager:add_cluster(Cluster))
        end},

        {"get_cluster existing", fun() ->
            Cluster = #acct_cluster{name = <<"getcluster">>},
            flurm_account_manager:add_cluster(Cluster),
            {ok, Retrieved} = flurm_account_manager:get_cluster(<<"getcluster">>),
            ?assertEqual(<<"getcluster">>, Retrieved#acct_cluster.name)
        end},

        {"list_clusters returns all", fun() ->
            Clusters = flurm_account_manager:list_clusters(),
            ?assert(is_list(Clusters)),
            ?assert(length(Clusters) >= 1)
        end}
    ].

tres_tests(_Pid) ->
    [
        {"add_tres with record", fun() ->
            Tres = #tres{
                type = <<"gres/gpu">>,
                name = <<"v100">>
            },
            {ok, Id} = flurm_account_manager:add_tres(Tres),
            ?assert(is_integer(Id))
        end},

        {"get_tres by type", fun() ->
            {ok, Retrieved} = flurm_account_manager:get_tres(<<"cpu">>),
            ?assertEqual(<<"cpu">>, Retrieved#tres.type)
        end},

        {"list_tres returns all", fun() ->
            TresList = flurm_account_manager:list_tres(),
            ?assert(is_list(TresList)),
            ?assert(length(TresList) >= 4)
        end}
    ].
