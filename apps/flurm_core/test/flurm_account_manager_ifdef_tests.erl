%%%-------------------------------------------------------------------
%%% @doc Unit tests for flurm_account_manager internal functions
%%%
%%% Tests the pure internal functions exported via -ifdef(TEST).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_account_manager_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test: normalize_account/1
%%====================================================================

normalize_account_test_() ->
    [
     {"normalize map to account record",
      fun() ->
          Map = #{name => <<"test_account">>,
                  description => <<"Test Account">>,
                  organization => <<"TestOrg">>},
          Result = flurm_account_manager:normalize_account(Map),
          ?assertEqual(<<"test_account">>, Result#account.name),
          ?assertEqual(<<"Test Account">>, Result#account.description),
          ?assertEqual(<<"TestOrg">>, Result#account.organization)
      end},

     {"normalize map with defaults",
      fun() ->
          Map = #{name => <<"minimal">>},
          Result = flurm_account_manager:normalize_account(Map),
          ?assertEqual(<<"minimal">>, Result#account.name),
          ?assertEqual(<<>>, Result#account.description),
          ?assertEqual(1, Result#account.fairshare)
      end},

     {"pass through existing record",
      fun() ->
          Acct = #account{name = <<"existing">>, description = <<"Existing">>},
          Result = flurm_account_manager:normalize_account(Acct),
          ?assertEqual(Acct, Result)
      end}
    ].

%%====================================================================
%% Test: normalize_user/1
%%====================================================================

normalize_user_test_() ->
    [
     {"normalize map to user record",
      fun() ->
          Map = #{name => <<"testuser">>,
                  default_account => <<"default_acct">>,
                  admin_level => operator},
          Result = flurm_account_manager:normalize_user(Map),
          ?assertEqual(<<"testuser">>, Result#acct_user.name),
          ?assertEqual(<<"default_acct">>, Result#acct_user.default_account),
          ?assertEqual(operator, Result#acct_user.admin_level)
      end},

     {"normalize map with defaults",
      fun() ->
          Map = #{name => <<"simpleuser">>},
          Result = flurm_account_manager:normalize_user(Map),
          ?assertEqual(<<"simpleuser">>, Result#acct_user.name),
          ?assertEqual(<<>>, Result#acct_user.default_account),
          ?assertEqual(none, Result#acct_user.admin_level),
          ?assertEqual(1, Result#acct_user.fairshare)
      end},

     {"pass through existing record",
      fun() ->
          User = #acct_user{name = <<"existing">>, admin_level = admin},
          Result = flurm_account_manager:normalize_user(User),
          ?assertEqual(User, Result)
      end}
    ].

%%====================================================================
%% Test: normalize_qos/1
%%====================================================================

normalize_qos_test_() ->
    [
     {"normalize map to qos record",
      fun() ->
          Map = #{name => <<"high">>,
                  priority => 1000,
                  preempt_mode => cluster},
          Result = flurm_account_manager:normalize_qos(Map),
          ?assertEqual(<<"high">>, Result#qos.name),
          ?assertEqual(1000, Result#qos.priority),
          ?assertEqual(cluster, Result#qos.preempt_mode)
      end},

     {"normalize map with defaults",
      fun() ->
          Map = #{name => <<"normal">>},
          Result = flurm_account_manager:normalize_qos(Map),
          ?assertEqual(<<"normal">>, Result#qos.name),
          ?assertEqual(0, Result#qos.priority),
          ?assertEqual(off, Result#qos.preempt_mode),
          ?assertEqual(1.0, Result#qos.usage_factor)
      end}
    ].

%%====================================================================
%% Test: apply_account_updates/2
%%====================================================================

apply_account_updates_test_() ->
    [
     {"update description",
      fun() ->
          Acct = #account{name = <<"test">>, description = <<"Old">>},
          Updated = flurm_account_manager:apply_account_updates(
                      Acct, #{description => <<"New">>}),
          ?assertEqual(<<"New">>, Updated#account.description)
      end},

     {"update multiple fields",
      fun() ->
          Acct = #account{name = <<"test">>, fairshare = 1, max_jobs = 0},
          Updated = flurm_account_manager:apply_account_updates(
                      Acct, #{fairshare => 5, max_jobs => 100}),
          ?assertEqual(5, Updated#account.fairshare),
          ?assertEqual(100, Updated#account.max_jobs)
      end},

     {"unknown keys are ignored",
      fun() ->
          Acct = #account{name = <<"test">>},
          Updated = flurm_account_manager:apply_account_updates(
                      Acct, #{unknown_field => value}),
          ?assertEqual(Acct, Updated)
      end}
    ].

%%====================================================================
%% Test: apply_user_updates/2
%%====================================================================

apply_user_updates_test_() ->
    [
     {"update default account",
      fun() ->
          User = #acct_user{name = <<"user1">>, default_account = <<"old">>},
          Updated = flurm_account_manager:apply_user_updates(
                      User, #{default_account => <<"new">>}),
          ?assertEqual(<<"new">>, Updated#acct_user.default_account)
      end},

     {"update admin level",
      fun() ->
          User = #acct_user{name = <<"user1">>, admin_level = none},
          Updated = flurm_account_manager:apply_user_updates(
                      User, #{admin_level => admin}),
          ?assertEqual(admin, Updated#acct_user.admin_level)
      end}
    ].

%%====================================================================
%% Test: matches_pattern/2
%%====================================================================

matches_pattern_test_() ->
    [
     {"exact match returns true",
      ?_assertEqual(true, flurm_account_manager:matches_pattern(<<"test">>, <<"test">>))},

     {"non-match returns false",
      ?_assertEqual(false, flurm_account_manager:matches_pattern(<<"test">>, <<"other">>))},

     {"wildcard prefix match",
      ?_assertEqual(true, flurm_account_manager:matches_pattern(<<"testuser">>, <<"test*">>))},

     {"wildcard prefix no match",
      ?_assertEqual(false, flurm_account_manager:matches_pattern(<<"other">>, <<"test*">>))},

     {"non-binary returns false",
      ?_assertEqual(false, flurm_account_manager:matches_pattern(atom, <<"test">>))}
    ].

%%====================================================================
%% Test: build_tres_request/1
%%====================================================================

build_tres_request_test_() ->
    [
     {"build basic TRES request",
      fun() ->
          JobSpec = #{num_cpus => 8, memory_mb => 4096, num_nodes => 2},
          Result = flurm_account_manager:build_tres_request(JobSpec),
          ?assertEqual(8, maps:get(<<"cpu">>, Result)),
          ?assertEqual(4096, maps:get(<<"mem">>, Result)),
          ?assertEqual(2, maps:get(<<"node">>, Result))
      end},

     {"build TRES request with GPU",
      fun() ->
          JobSpec = #{num_cpus => 4, memory_mb => 8192, num_nodes => 1, num_gpus => 2},
          Result = flurm_account_manager:build_tres_request(JobSpec),
          ?assertEqual(4, maps:get(<<"cpu">>, Result)),
          ?assertEqual(2, maps:get(<<"gres/gpu">>, Result))
      end},

     {"build TRES request with defaults",
      fun() ->
          JobSpec = #{},
          Result = flurm_account_manager:build_tres_request(JobSpec),
          ?assertEqual(1, maps:get(<<"cpu">>, Result)),
          ?assertEqual(0, maps:get(<<"mem">>, Result)),
          ?assertEqual(1, maps:get(<<"node">>, Result)),
          ?assertEqual(false, maps:is_key(<<"gres/gpu">>, Result))
      end}
    ].

%%====================================================================
%% Test: check_tres_limits/3
%%====================================================================

check_tres_limits_test_() ->
    [
     {"passes when under limits",
      fun() ->
          Requested = #{<<"cpu">> => 4, <<"mem">> => 1024},
          Limits = #{<<"cpu">> => 8, <<"mem">> => 2048},
          ?assertEqual(ok, flurm_account_manager:check_tres_limits(
                            Requested, Limits, test_limit))
      end},

     {"passes when at limits",
      fun() ->
          Requested = #{<<"cpu">> => 8},
          Limits = #{<<"cpu">> => 8},
          ?assertEqual(ok, flurm_account_manager:check_tres_limits(
                            Requested, Limits, test_limit))
      end},

     {"passes when limit is 0 (unlimited)",
      fun() ->
          Requested = #{<<"cpu">> => 100},
          Limits = #{<<"cpu">> => 0},
          ?assertEqual(ok, flurm_account_manager:check_tres_limits(
                            Requested, Limits, test_limit))
      end},

     {"fails when over limit",
      fun() ->
          Requested = #{<<"cpu">> => 16},
          Limits = #{<<"cpu">> => 8},
          Result = flurm_account_manager:check_tres_limits(
                     Requested, Limits, test_limit),
          ?assertMatch({error, {test_limit, _}}, Result)
      end},

     {"reports all violations",
      fun() ->
          Requested = #{<<"cpu">> => 16, <<"mem">> => 4096},
          Limits = #{<<"cpu">> => 8, <<"mem">> => 2048},
          {error, {test_limit, Violations}} =
              flurm_account_manager:check_tres_limits(Requested, Limits, test_limit),
          ?assertEqual(2, length(Violations))
      end}
    ].

%%====================================================================
%% Test: combine_tres_maps/2
%%====================================================================

combine_tres_maps_test_() ->
    [
     {"combines two TRES maps",
      fun() ->
          Map1 = #{<<"cpu">> => 4, <<"mem">> => 1024},
          Map2 = #{<<"cpu">> => 2, <<"mem">> => 512},
          Result = flurm_account_manager:combine_tres_maps(Map1, Map2),
          ?assertEqual(6, maps:get(<<"cpu">>, Result)),
          ?assertEqual(1536, maps:get(<<"mem">>, Result))
      end},

     {"adds new keys from second map",
      fun() ->
          Map1 = #{<<"cpu">> => 4},
          Map2 = #{<<"mem">> => 1024},
          Result = flurm_account_manager:combine_tres_maps(Map1, Map2),
          ?assertEqual(4, maps:get(<<"cpu">>, Result)),
          ?assertEqual(1024, maps:get(<<"mem">>, Result))
      end},

     {"handles empty maps",
      fun() ->
          Map1 = #{<<"cpu">> => 4},
          Result = flurm_account_manager:combine_tres_maps(Map1, #{}),
          ?assertEqual(Map1, Result)
      end}
    ].

%%====================================================================
%% Test: run_checks/1
%%====================================================================

run_checks_test_() ->
    [
     {"empty list returns ok",
      ?_assertEqual(ok, flurm_account_manager:run_checks([]))},

     {"all passing checks return ok",
      fun() ->
          Checks = [fun() -> ok end, fun() -> ok end, fun() -> ok end],
          ?assertEqual(ok, flurm_account_manager:run_checks(Checks))
      end},

     {"first failing check stops execution",
      fun() ->
          Counter = counters:new(1, []),
          Checks = [
            fun() -> counters:add(Counter, 1, 1), ok end,
            fun() -> counters:add(Counter, 1, 1), {error, test_error} end,
            fun() -> counters:add(Counter, 1, 1), ok end
          ],
          Result = flurm_account_manager:run_checks(Checks),
          ?assertEqual({error, test_error}, Result),
          ?assertEqual(2, counters:get(Counter, 1))
      end},

     {"returns first error encountered",
      fun() ->
          Checks = [
            fun() -> ok end,
            fun() -> {error, first_error} end,
            fun() -> {error, second_error} end
          ],
          ?assertEqual({error, first_error},
                       flurm_account_manager:run_checks(Checks))
      end}
    ].
