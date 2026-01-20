%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_limits module
%%%
%%% Tests cover:
%%% - Limit setting (user, account, partition)
%%% - Limit checking (jobs, submit, resources)
%%% - Usage tracking
%%% - Enforcement (start/stop jobs)
%%% - Effective limit computation
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_limits_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure required applications
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    %% Stop any existing flurm_limits to ensure clean state for each test
    case whereis(flurm_limits) of
        undefined -> ok;
        ExistingPid ->
            Ref = monitor(process, ExistingPid),
            catch gen_server:stop(flurm_limits, shutdown, 5000),
            receive
                {'DOWN', Ref, process, ExistingPid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush]),
                catch exit(ExistingPid, kill),
                timer:sleep(50)
            end
    end,

    %% Start fresh limits server
    {ok, Pid} = flurm_limits:start_link(),
    unlink(Pid),  %% Unlink immediately to prevent EXIT propagation
    Pid.

cleanup(Pid) ->
    catch ets:delete(flurm_user_limits),
    catch ets:delete(flurm_account_limits),
    catch ets:delete(flurm_partition_limits),
    catch ets:delete(flurm_usage),
    %% Use monitor/wait pattern to ensure process terminates before next test
    case is_process_alive(Pid) of
        true ->
            Ref = monitor(process, Pid),
            catch gen_server:stop(flurm_limits, shutdown, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                demonitor(Ref, [flush]),
                catch exit(Pid, kill)
            end;
        false ->
            ok
    end.

%%====================================================================
%% Test Fixtures
%%====================================================================

limits_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      %% Limit setting tests
      {"set user limit", fun test_set_user_limit/0},
      {"set account limit", fun test_set_account_limit/0},
      {"set partition limit", fun test_set_partition_limit/0},
      {"set multiple limit types", fun test_set_multiple_limits/0},

      %% Get limits tests
      {"get user limits", fun test_get_user_limits/0},
      {"get account limits", fun test_get_account_limits/0},
      {"get effective limits", fun test_get_effective_limits/0},
      {"effective limits merging", fun test_effective_limits_merging/0},

      %% Check limits tests
      {"check limits - ok", fun test_check_limits_ok/0},
      {"check limits - max jobs exceeded", fun test_check_limits_max_jobs/0},
      {"check limits - grp jobs exceeded", fun test_check_limits_grp_jobs/0},
      {"check limits - resources exceeded", fun test_check_limits_resources/0},

      %% Check submit limits tests
      {"check submit limits - ok", fun test_check_submit_limits_ok/0},
      {"check submit limits - max submit exceeded", fun test_check_submit_limits_exceeded/0},
      {"check submit limits - grp submit exceeded", fun test_check_submit_grp_exceeded/0},

      %% Usage tracking tests
      {"get usage - user", fun test_get_usage_user/0},
      {"get usage - account", fun test_get_usage_account/0},
      {"reset usage", fun test_reset_usage/0},

      %% Enforcement tests
      {"enforce limit - start job", fun test_enforce_start/0},
      {"enforce limit - stop job", fun test_enforce_stop/0},
      {"enforce limit - with TRES", fun test_enforce_with_tres/0},
      {"enforce limit - with account", fun test_enforce_with_account/0}
     ]}.

%%====================================================================
%% Limit Setting Tests
%%====================================================================

test_set_user_limit() ->
    %% Set a max_jobs limit for a user
    ok = flurm_limits:set_user_limit(<<"testuser">>, max_jobs, 100),

    %% Verify via get_user_limits
    Limits = flurm_limits:get_user_limits(<<"testuser">>),
    ?assertEqual(1, length(Limits)),

    [Limit] = Limits,
    ?assertEqual(100, element(3, Limit)).  % max_jobs is at position 3

test_set_account_limit() ->
    %% Set limits for an account
    ok = flurm_limits:set_account_limit(<<"research">>, max_jobs, 500),
    ok = flurm_limits:set_account_limit(<<"research">>, grp_jobs, 200),

    %% Verify via get_account_limits
    Limit = flurm_limits:get_account_limits(<<"research">>),
    ?assertNotEqual(undefined, Limit),
    ?assertEqual(500, element(3, Limit)),  % max_jobs
    ?assertEqual(200, element(10, Limit)). % grp_jobs is at position 10

test_set_partition_limit() ->
    %% Default partition already has limits from init
    %% Set a custom partition limit
    ok = flurm_limits:set_partition_limit(<<"gpu">>, max_jobs, 50),
    ok = flurm_limits:set_partition_limit(<<"gpu">>, max_wall, 172800),

    %% Verify via effective limits
    Limits = flurm_limits:get_effective_limits(<<"user">>, <<>>, <<"gpu">>),
    ?assertEqual(50, element(3, Limits)).  % max_jobs

test_set_multiple_limits() ->
    User = <<"multiuser">>,

    %% Set multiple limits for the same user
    ok = flurm_limits:set_user_limit(User, max_jobs, 10),
    ok = flurm_limits:set_user_limit(User, max_submit, 20),
    ok = flurm_limits:set_user_limit(User, max_wall, 3600),
    ok = flurm_limits:set_user_limit(User, max_nodes_per_job, 4),
    ok = flurm_limits:set_user_limit(User, max_cpus_per_job, 32),
    ok = flurm_limits:set_user_limit(User, max_mem_per_job, 65536),
    ok = flurm_limits:set_user_limit(User, max_tres, #{cpu => 100, gpu => 10}),
    ok = flurm_limits:set_user_limit(User, grp_jobs, 5),
    ok = flurm_limits:set_user_limit(User, grp_submit, 10),
    ok = flurm_limits:set_user_limit(User, grp_tres, #{cpu => 50}),

    Limits = flurm_limits:get_user_limits(User),
    ?assertEqual(1, length(Limits)),

    [Limit] = Limits,
    ?assertEqual(10, element(3, Limit)),   % max_jobs
    ?assertEqual(20, element(4, Limit)),   % max_submit
    ?assertEqual(3600, element(5, Limit)). % max_wall

%%====================================================================
%% Get Limits Tests
%%====================================================================

test_get_user_limits() ->
    %% User with no limits set should return empty list
    Limits = flurm_limits:get_user_limits(<<"nonexistent">>),
    ?assertEqual([], Limits),

    %% Set a limit and verify
    ok = flurm_limits:set_user_limit(<<"user1">>, max_jobs, 50),
    Limits2 = flurm_limits:get_user_limits(<<"user1">>),
    ?assertEqual(1, length(Limits2)).

test_get_account_limits() ->
    %% Account with no limits should return undefined
    Result = flurm_limits:get_account_limits(<<"unknown_account">>),
    ?assertEqual(undefined, Result),

    %% Set and retrieve
    ok = flurm_limits:set_account_limit(<<"myaccount">>, max_jobs, 200),
    Result2 = flurm_limits:get_account_limits(<<"myaccount">>),
    ?assertNotEqual(undefined, Result2).

test_get_effective_limits() ->
    %% Set up various limits
    ok = flurm_limits:set_user_limit(<<"effuser">>, max_jobs, 100),
    ok = flurm_limits:set_account_limit(<<"effacct">>, max_jobs, 500),
    ok = flurm_limits:set_partition_limit(<<"effpart">>, max_jobs, 50),

    %% Get effective limits (should merge all)
    Limits = flurm_limits:get_effective_limits(<<"effuser">>, <<"effacct">>, <<"effpart">>),

    %% Most restrictive should win (50 from partition)
    ?assertEqual(50, element(3, Limits)).

test_effective_limits_merging() ->
    %% Test that most restrictive limit wins
    ok = flurm_limits:set_user_limit(<<"mergeuser">>, max_wall, 1000),
    ok = flurm_limits:set_account_limit(<<"mergeacct">>, max_wall, 500),
    ok = flurm_limits:set_partition_limit(<<"mergepart">>, max_wall, 2000),

    Limits = flurm_limits:get_effective_limits(<<"mergeuser">>, <<"mergeacct">>, <<"mergepart">>),

    %% 500 is the most restrictive non-zero value
    ?assertEqual(500, element(5, Limits)).  % max_wall

%%====================================================================
%% Check Limits Tests
%%====================================================================

test_check_limits_ok() ->
    %% Set generous limits
    ok = flurm_limits:set_partition_limit(<<"batch">>, max_jobs, 1000),

    JobSpec = #{
        user => <<"testuser">>,
        account => <<>>,
        partition => <<"batch">>,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 8192,
        time_limit => 3600
    },

    Result = flurm_limits:check_limits(JobSpec),
    ?assertEqual(ok, Result).

test_check_limits_max_jobs() ->
    User = <<"maxjobuser">>,

    %% Set strict limit
    ok = flurm_limits:set_user_limit(User, max_jobs, 2),

    %% Simulate running jobs by enforcing start
    flurm_limits:enforce_limit(start, 1, #{user => User, account => <<>>}),
    flurm_limits:enforce_limit(start, 2, #{user => User, account => <<>>}),

    JobSpec = #{
        user => User,
        account => <<>>,
        partition => <<"default">>,
        num_cpus => 1
    },

    Result = flurm_limits:check_limits(JobSpec),
    ?assertMatch({error, {max_jobs_exceeded, _, _}}, Result).

test_check_limits_grp_jobs() ->
    Account = <<"grpaccount">>,
    User = <<"grpuser">>,

    %% Set group job limit on account
    ok = flurm_limits:set_account_limit(Account, grp_jobs, 1),

    %% Start a job under this account
    flurm_limits:enforce_limit(start, 10, #{user => User, account => Account}),

    JobSpec = #{
        user => User,
        account => Account,
        partition => <<"default">>,
        num_cpus => 1
    },

    Result = flurm_limits:check_limits(JobSpec),
    ?assertMatch({error, {grp_jobs_exceeded, _, _}}, Result).

test_check_limits_resources() ->
    %% Set strict resource limits
    ok = flurm_limits:set_partition_limit(<<"strict">>, max_nodes_per_job, 2),
    ok = flurm_limits:set_partition_limit(<<"strict">>, max_cpus_per_job, 8),
    ok = flurm_limits:set_partition_limit(<<"strict">>, max_mem_per_job, 16384),
    ok = flurm_limits:set_partition_limit(<<"strict">>, max_wall, 3600),

    %% Test nodes limit exceeded
    JobSpec1 = #{
        user => <<"user">>,
        account => <<>>,
        partition => <<"strict">>,
        num_nodes => 5,
        num_cpus => 1,
        memory_mb => 1024,
        time_limit => 1800
    },
    Result1 = flurm_limits:check_limits(JobSpec1),
    ?assertMatch({error, {limit_exceeded, num_nodes, _, _}}, Result1),

    %% Test CPUs limit exceeded
    JobSpec2 = JobSpec1#{num_nodes => 1, num_cpus => 16},
    Result2 = flurm_limits:check_limits(JobSpec2),
    ?assertMatch({error, {limit_exceeded, num_cpus, _, _}}, Result2),

    %% Test memory limit exceeded
    JobSpec3 = JobSpec2#{num_cpus => 4, memory_mb => 32768},
    Result3 = flurm_limits:check_limits(JobSpec3),
    ?assertMatch({error, {limit_exceeded, memory_mb, _, _}}, Result3),

    %% Test wall time limit exceeded
    JobSpec4 = JobSpec3#{memory_mb => 8192, time_limit => 7200},
    Result4 = flurm_limits:check_limits(JobSpec4),
    ?assertMatch({error, {limit_exceeded, time_limit, _, _}}, Result4).

%%====================================================================
%% Check Submit Limits Tests
%%====================================================================

test_check_submit_limits_ok() ->
    ok = flurm_limits:set_user_limit(<<"submituser">>, max_submit, 100),

    JobSpec = #{
        user => <<"submituser">>,
        account => <<>>,
        partition => <<"default">>
    },

    Result = flurm_limits:check_submit_limits(JobSpec),
    ?assertEqual(ok, Result).

test_check_submit_limits_exceeded() ->
    User = <<"submitmaxuser">>,
    ok = flurm_limits:set_user_limit(User, max_submit, 2),

    %% Simulate pending + running jobs using enforce_limit
    %% First start two jobs to hit the limit
    flurm_limits:enforce_limit(start, 1001, #{user => User, account => <<>>}),
    flurm_limits:enforce_limit(start, 1002, #{user => User, account => <<>>}),

    JobSpec = #{
        user => User,
        account => <<>>,
        partition => <<"default">>
    },

    Result = flurm_limits:check_submit_limits(JobSpec),
    ?assertMatch({error, {max_submit_exceeded, _, _}}, Result).

test_check_submit_grp_exceeded() ->
    Account = <<"submitgrpacct">>,
    User = <<"submitgrpuser">>,

    ok = flurm_limits:set_account_limit(Account, grp_submit, 2),

    %% Simulate group usage using enforce_limit
    flurm_limits:enforce_limit(start, 2001, #{user => User, account => Account}),
    flurm_limits:enforce_limit(start, 2002, #{user => User, account => Account}),

    JobSpec = #{
        user => User,
        account => Account,
        partition => <<"default">>
    },

    Result = flurm_limits:check_submit_limits(JobSpec),
    ?assertMatch({error, {grp_submit_exceeded, _, _}}, Result).

%%====================================================================
%% Usage Tracking Tests
%%====================================================================

test_get_usage_user() ->
    User = <<"usageuser">>,

    %% Initially no usage
    Result = flurm_limits:get_usage(user, User),
    ?assertEqual(undefined, Result),

    %% Start a job to create usage
    flurm_limits:enforce_limit(start, 100, #{user => User, account => <<>>}),

    %% Now should have usage
    Usage = flurm_limits:get_usage(user, User),
    ?assertNotEqual(undefined, Usage),
    ?assertEqual(1, element(3, Usage)).  % running_jobs

test_get_usage_account() ->
    Account = <<"usageacct">>,
    User = <<"usageacctuser">>,

    %% Initially no usage
    Result = flurm_limits:get_usage(account, Account),
    ?assertEqual(undefined, Result),

    %% Start a job to create usage
    flurm_limits:enforce_limit(start, 101, #{user => User, account => Account}),

    %% Now should have usage
    Usage = flurm_limits:get_usage(account, Account),
    ?assertNotEqual(undefined, Usage),
    ?assertEqual(1, element(3, Usage)).  % running_jobs

test_reset_usage() ->
    User = <<"resetuser">>,

    %% Create some usage
    flurm_limits:enforce_limit(start, 200, #{user => User, account => <<>>}),

    %% Verify usage exists
    Usage1 = flurm_limits:get_usage(user, User),
    ?assertEqual(1, element(3, Usage1)),

    %% Reset user usage
    ok = flurm_limits:reset_usage(user),

    %% Usage should be reset (but entry still exists)
    Usage2 = flurm_limits:get_usage(user, User),
    ?assertEqual(0, element(3, Usage2)).  % running_jobs reset to 0

%%====================================================================
%% Enforcement Tests
%%====================================================================

test_enforce_start() ->
    User = <<"enforceuser">>,
    JobId = 300,

    JobInfo = #{
        user => User,
        account => <<>>
    },

    %% Start a job
    Result = flurm_limits:enforce_limit(start, JobId, JobInfo),
    ?assertEqual(ok, Result),

    %% Check usage updated
    Usage = flurm_limits:get_usage(user, User),
    ?assertEqual(1, element(3, Usage)).  % running_jobs

test_enforce_stop() ->
    User = <<"enforcestopuser">>,
    JobId = 301,

    %% Start a job first
    flurm_limits:enforce_limit(start, JobId, #{user => User, account => <<>>}),

    %% Verify running
    Usage1 = flurm_limits:get_usage(user, User),
    ?assertEqual(1, element(3, Usage1)),

    %% Stop the job
    JobInfo = #{
        user => User,
        account => <<>>,
        wall_time => 3600
    },
    Result = flurm_limits:enforce_limit(stop, JobId, JobInfo),
    ?assertEqual(ok, Result),

    %% Check usage updated
    Usage2 = flurm_limits:get_usage(user, User),
    ?assertEqual(0, element(3, Usage2)).  % running_jobs decremented

test_enforce_with_tres() ->
    User = <<"tresuser">>,
    JobId = 302,

    TRES = #{cpu => 8, mem => 16384, gpu => 2},

    JobInfo = #{
        user => User,
        account => <<>>,
        tres => TRES
    },

    %% Start job with TRES
    flurm_limits:enforce_limit(start, JobId, JobInfo),

    %% Check TRES usage tracked
    Usage = flurm_limits:get_usage(user, User),
    TRESUsed = element(5, Usage),  % tres_used
    ?assertEqual(8, maps:get(cpu, TRESUsed, 0)),
    ?assertEqual(16384, maps:get(mem, TRESUsed, 0)),
    ?assertEqual(2, maps:get(gpu, TRESUsed, 0)),

    %% Stop job
    StopInfo = JobInfo#{wall_time => 1800},  % 30 minutes
    flurm_limits:enforce_limit(stop, JobId, StopInfo),

    %% Check TRES released and minutes accumulated
    Usage2 = flurm_limits:get_usage(user, User),
    TRESUsed2 = element(5, Usage2),
    ?assertEqual(0, maps:get(cpu, TRESUsed2, 0)),

    TRESMins = element(6, Usage2),  % tres_mins
    %% CPU minutes = 8 CPUs * 30 minutes = 240
    ?assert(maps:get(cpu, TRESMins, 0) > 0).

test_enforce_with_account() ->
    User = <<"acctenfuser">>,
    Account = <<"acctenfacct">>,
    JobId = 303,

    JobInfo = #{
        user => User,
        account => Account,
        tres => #{cpu => 4}
    },

    %% Start job
    flurm_limits:enforce_limit(start, JobId, JobInfo),

    %% Check both user and account usage updated
    UserUsage = flurm_limits:get_usage(user, User),
    ?assertEqual(1, element(3, UserUsage)),

    AccountUsage = flurm_limits:get_usage(account, Account),
    ?assertEqual(1, element(3, AccountUsage)),

    %% Stop job
    flurm_limits:enforce_limit(stop, JobId, JobInfo#{wall_time => 600}),

    %% Both should be decremented
    UserUsage2 = flurm_limits:get_usage(user, User),
    ?assertEqual(0, element(3, UserUsage2)),

    AccountUsage2 = flurm_limits:get_usage(account, Account),
    ?assertEqual(0, element(3, AccountUsage2)).

%%====================================================================
%% Edge Case Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"zero limits mean unlimited", fun test_zero_limits_unlimited/0},
      {"empty account handling", fun test_empty_account/0},
      {"tres subtraction doesn't go negative", fun test_tres_no_negative/0}
     ]}.

test_zero_limits_unlimited() ->
    %% Zero means unlimited, should not trigger limit errors
    ok = flurm_limits:set_user_limit(<<"zerouser">>, max_jobs, 0),

    JobSpec = #{
        user => <<"zerouser">>,
        account => <<>>,
        partition => <<"default">>,
        num_cpus => 1
    },

    %% Should pass even with "0" limit (meaning unlimited)
    Result = flurm_limits:check_limits(JobSpec),
    ?assertEqual(ok, Result).

test_empty_account() ->
    %% Jobs with empty account should work
    JobSpec = #{
        user => <<"emptyacctuser">>,
        account => <<>>,
        partition => <<"default">>,
        num_cpus => 1
    },

    Result = flurm_limits:check_limits(JobSpec),
    ?assertEqual(ok, Result),

    %% Enforcement should handle empty account
    ok = flurm_limits:enforce_limit(start, 400, #{
        user => <<"emptyacctuser">>,
        account => <<>>
    }).

test_tres_no_negative() ->
    User = <<"negtest">>,

    %% Start with some TRES
    flurm_limits:enforce_limit(start, 500, #{
        user => User,
        account => <<>>,
        tres => #{cpu => 4}
    }),

    %% Stop with MORE TRES than started (edge case)
    flurm_limits:enforce_limit(stop, 500, #{
        user => User,
        account => <<>>,
        tres => #{cpu => 10},  % More than allocated
        wall_time => 100
    }),

    %% TRES used should be 0, not negative
    Usage = flurm_limits:get_usage(user, User),
    TRESUsed = element(5, Usage),
    ?assertEqual(0, maps:get(cpu, TRESUsed, 0)).
