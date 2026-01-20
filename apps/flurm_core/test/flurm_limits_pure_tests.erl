%%%-------------------------------------------------------------------
%%% @doc Pure Unit Tests for flurm_limits module
%%%
%%% These tests directly test the gen_server callbacks and internal
%%% functions without any mocking (NO MECK).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_limits_pure_tests).

-include_lib("eunit/include/eunit.hrl").

%% Define record structures locally for testing since they're internal to the module
-record(limit_spec, {
    key :: term(),
    max_jobs = 0 :: non_neg_integer(),
    max_submit = 0 :: non_neg_integer(),
    max_wall = 0 :: non_neg_integer(),
    max_nodes_per_job = 0 :: non_neg_integer(),
    max_cpus_per_job = 0 :: non_neg_integer(),
    max_mem_per_job = 0 :: non_neg_integer(),
    max_tres = #{} :: map(),
    grp_jobs = 0 :: non_neg_integer(),
    grp_submit = 0 :: non_neg_integer(),
    grp_tres = #{} :: map()
}).

-record(usage, {
    key :: term(),
    running_jobs = 0 :: non_neg_integer(),
    pending_jobs = 0 :: non_neg_integer(),
    tres_used = #{} :: map(),
    tres_mins = #{} :: map()
}).

-record(state, {}).

%% ETS table names
-define(USER_LIMITS_TABLE, flurm_user_limits).
-define(ACCOUNT_LIMITS_TABLE, flurm_account_limits).
-define(PARTITION_LIMITS_TABLE, flurm_partition_limits).
-define(USAGE_TABLE, flurm_usage).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup function to create ETS tables
setup() ->
    %% Clean up any existing tables first
    cleanup_tables(),
    ok.

%% Cleanup function to destroy ETS tables
cleanup() ->
    cleanup_tables(),
    ok.

cleanup_tables() ->
    catch ets:delete(?USER_LIMITS_TABLE),
    catch ets:delete(?ACCOUNT_LIMITS_TABLE),
    catch ets:delete(?PARTITION_LIMITS_TABLE),
    catch ets:delete(?USAGE_TABLE),
    ok.

%%====================================================================
%% init/1 Tests
%%====================================================================

init_test_() ->
    {setup,
     fun setup/0,
     fun(_) -> cleanup() end,
     [
      {"init creates ETS tables and returns ok state",
       fun() ->
           Result = flurm_limits:init([]),
           ?assertMatch({ok, #state{}}, Result),

           %% Verify tables were created
           ?assertNotEqual(undefined, ets:info(?USER_LIMITS_TABLE)),
           ?assertNotEqual(undefined, ets:info(?ACCOUNT_LIMITS_TABLE)),
           ?assertNotEqual(undefined, ets:info(?PARTITION_LIMITS_TABLE)),
           ?assertNotEqual(undefined, ets:info(?USAGE_TABLE)),

           %% Verify default partition limits were set
           DefaultLimits = ets:lookup(?PARTITION_LIMITS_TABLE, {partition, <<"default">>}),
           ?assertEqual(1, length(DefaultLimits)),
           [Limit] = DefaultLimits,
           ?assertEqual(1000, Limit#limit_spec.max_jobs),
           ?assertEqual(5000, Limit#limit_spec.max_submit),
           ?assertEqual(604800, Limit#limit_spec.max_wall),
           ?assertEqual(100, Limit#limit_spec.max_nodes_per_job),
           ?assertEqual(10000, Limit#limit_spec.max_cpus_per_job),
           ?assertEqual(1024000, Limit#limit_spec.max_mem_per_job)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - set_limit
%%====================================================================

set_limit_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"set_user_limit creates new limit entry",
       fun() ->
           State = #state{},
           Result = flurm_limits:handle_call(
               {set_limit, {user, <<"testuser">>}, max_jobs, 10},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           %% Verify limit was stored
           [Limit] = ets:lookup(?USER_LIMITS_TABLE, {user, <<"testuser">>}),
           ?assertEqual(10, Limit#limit_spec.max_jobs)
       end},

      {"set_user_limit updates existing limit entry",
       fun() ->
           State = #state{},
           %% Set initial limit
           flurm_limits:handle_call(
               {set_limit, {user, <<"user2">>}, max_jobs, 5},
               {self(), make_ref()},
               State),

           %% Update limit
           Result = flurm_limits:handle_call(
               {set_limit, {user, <<"user2">>}, max_submit, 20},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           %% Verify both limits are set
           [Limit] = ets:lookup(?USER_LIMITS_TABLE, {user, <<"user2">>}),
           ?assertEqual(5, Limit#limit_spec.max_jobs),
           ?assertEqual(20, Limit#limit_spec.max_submit)
       end},

      {"set_account_limit stores in account table",
       fun() ->
           State = #state{},
           Result = flurm_limits:handle_call(
               {set_limit, {account, <<"testacct">>}, grp_jobs, 100},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [Limit] = ets:lookup(?ACCOUNT_LIMITS_TABLE, {account, <<"testacct">>}),
           ?assertEqual(100, Limit#limit_spec.grp_jobs)
       end},

      {"set_partition_limit stores in partition table",
       fun() ->
           State = #state{},
           Result = flurm_limits:handle_call(
               {set_limit, {partition, <<"compute">>}, max_wall, 3600},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [Limit] = ets:lookup(?PARTITION_LIMITS_TABLE, {partition, <<"compute">>}),
           ?assertEqual(3600, Limit#limit_spec.max_wall)
       end},

      {"set all limit types for user",
       fun() ->
           State = #state{},
           Key = {user, <<"alllimits">>},

           %% Set all limit types
           LimitTypes = [
               {max_jobs, 10},
               {max_submit, 20},
               {max_wall, 3600},
               {max_nodes_per_job, 5},
               {max_cpus_per_job, 100},
               {max_mem_per_job, 8192},
               {max_tres, #{cpu => 100, gpu => 4}},
               {grp_jobs, 50},
               {grp_submit, 100},
               {grp_tres, #{cpu => 500}}
           ],

           lists:foreach(fun({Type, Value}) ->
               flurm_limits:handle_call(
                   {set_limit, Key, Type, Value},
                   {self(), make_ref()},
                   State)
           end, LimitTypes),

           [Limit] = ets:lookup(?USER_LIMITS_TABLE, Key),
           ?assertEqual(10, Limit#limit_spec.max_jobs),
           ?assertEqual(20, Limit#limit_spec.max_submit),
           ?assertEqual(3600, Limit#limit_spec.max_wall),
           ?assertEqual(5, Limit#limit_spec.max_nodes_per_job),
           ?assertEqual(100, Limit#limit_spec.max_cpus_per_job),
           ?assertEqual(8192, Limit#limit_spec.max_mem_per_job),
           ?assertEqual(#{cpu => 100, gpu => 4}, Limit#limit_spec.max_tres),
           ?assertEqual(50, Limit#limit_spec.grp_jobs),
           ?assertEqual(100, Limit#limit_spec.grp_submit),
           ?assertEqual(#{cpu => 500}, Limit#limit_spec.grp_tres)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - get_effective
%%====================================================================

get_effective_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"get_effective returns default limits when none set",
       fun() ->
           State = #state{},
           Result = flurm_limits:handle_call(
               {get_effective, <<"unknown_user">>, <<"unknown_acct">>, <<"unknown_part">>},
               {self(), make_ref()},
               State),
           {reply, Limits, _} = Result,
           ?assertEqual(effective, Limits#limit_spec.key)
       end},

      {"get_effective merges partition limits",
       fun() ->
           State = #state{},
           %% Set partition limit
           flurm_limits:handle_call(
               {set_limit, {partition, <<"mypart">>}, max_jobs, 500},
               {self(), make_ref()},
               State),

           Result = flurm_limits:handle_call(
               {get_effective, <<"user1">>, <<"acct1">>, <<"mypart">>},
               {self(), make_ref()},
               State),
           {reply, Limits, _} = Result,
           ?assertEqual(500, Limits#limit_spec.max_jobs)
       end},

      {"get_effective merges user and account limits (most restrictive wins)",
       fun() ->
           State = #state{},
           %% User allows 20 jobs
           flurm_limits:handle_call(
               {set_limit, {user, <<"limiteduser">>}, max_jobs, 20},
               {self(), make_ref()},
               State),
           %% Account allows 10 jobs (more restrictive)
           flurm_limits:handle_call(
               {set_limit, {account, <<"limitedacct">>}, max_jobs, 10},
               {self(), make_ref()},
               State),

           Result = flurm_limits:handle_call(
               {get_effective, <<"limiteduser">>, <<"limitedacct">>, <<"default">>},
               {self(), make_ref()},
               State),
           {reply, Limits, _} = Result,
           %% Most restrictive (10) should win
           ?assertEqual(10, Limits#limit_spec.max_jobs)
       end},

      {"get_effective handles zero values as unlimited",
       fun() ->
           State = #state{},
           %% User has limit of 0 (unlimited)
           flurm_limits:handle_call(
               {set_limit, {user, <<"unlimiteduser">>}, max_jobs, 0},
               {self(), make_ref()},
               State),
           %% Account has limit of 50
           flurm_limits:handle_call(
               {set_limit, {account, <<"someacct">>}, max_jobs, 50},
               {self(), make_ref()},
               State),

           Result = flurm_limits:handle_call(
               {get_effective, <<"unlimiteduser">>, <<"someacct">>, <<"default">>},
               {self(), make_ref()},
               State),
           {reply, Limits, _} = Result,
           %% 50 should win because 0 means unlimited
           ?assertEqual(50, Limits#limit_spec.max_jobs)
       end},

      {"get_effective combines TRES limits",
       fun() ->
           State = #state{},
           %% User has CPU limit
           flurm_limits:handle_call(
               {set_limit, {user, <<"tresuser">>}, max_tres, #{cpu => 100}},
               {self(), make_ref()},
               State),
           %% Account has GPU limit
           flurm_limits:handle_call(
               {set_limit, {account, <<"tresacct">>}, max_tres, #{gpu => 4}},
               {self(), make_ref()},
               State),

           Result = flurm_limits:handle_call(
               {get_effective, <<"tresuser">>, <<"tresacct">>, <<"default">>},
               {self(), make_ref()},
               State),
           {reply, Limits, _} = Result,
           TRES = Limits#limit_spec.max_tres,
           %% Both should be present
           ?assertEqual(100, maps:get(cpu, TRES, undefined)),
           ?assertEqual(4, maps:get(gpu, TRES, undefined))
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - check_limits
%%====================================================================

check_limits_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"check_limits returns ok when under all limits",
       fun() ->
           State = #state{},
           JobSpec = #{
               user => <<"checkuser1">>,
               account => <<"checkacct1">>,
               partition => <<"default">>,
               time_limit => 3600,
               num_nodes => 1,
               num_cpus => 4,
               memory_mb => 1024
           },

           Result = flurm_limits:handle_call(
               {check_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result)
       end},

      {"check_limits fails when max_jobs exceeded",
       fun() ->
           State = #state{},
           %% Set low max_jobs limit
           flurm_limits:handle_call(
               {set_limit, {user, <<"busyuser">>}, max_jobs, 2},
               {self(), make_ref()},
               State),

           %% Simulate 2 running jobs via usage table
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"busyuser">>},
               running_jobs = 2,
               pending_jobs = 0
           }),

           JobSpec = #{
               user => <<"busyuser">>,
               account => <<>>,
               partition => <<"default">>
           },

           Result = flurm_limits:handle_call(
               {check_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, {error, {max_jobs_exceeded, 2, 2}}, #state{}}, Result)
       end},

      {"check_limits fails when grp_jobs exceeded",
       fun() ->
           State = #state{},
           %% Set grp_jobs limit on account
           flurm_limits:handle_call(
               {set_limit, {account, <<"busyacct">>}, grp_jobs, 5},
               {self(), make_ref()},
               State),

           %% Simulate 5 running jobs for account
           ets:insert(?USAGE_TABLE, #usage{
               key = {account, <<"busyacct">>},
               running_jobs = 5,
               pending_jobs = 0
           }),

           JobSpec = #{
               user => <<"anyuser">>,
               account => <<"busyacct">>,
               partition => <<"default">>
           },

           Result = flurm_limits:handle_call(
               {check_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, {error, {grp_jobs_exceeded, 5, 5}}, #state{}}, Result)
       end},

      {"check_limits fails when time_limit exceeds max_wall",
       fun() ->
           State = #state{},
           %% Set max_wall to 1 hour
           flurm_limits:handle_call(
               {set_limit, {partition, <<"shortpart">>}, max_wall, 3600},
               {self(), make_ref()},
               State),

           JobSpec = #{
               user => <<"timeuser">>,
               account => <<>>,
               partition => <<"shortpart">>,
               time_limit => 7200  %% 2 hours, exceeds limit
           },

           Result = flurm_limits:handle_call(
               {check_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, {error, {limit_exceeded, time_limit, 7200, 3600}}, #state{}}, Result)
       end},

      {"check_limits fails when num_nodes exceeds max_nodes_per_job",
       fun() ->
           State = #state{},
           flurm_limits:handle_call(
               {set_limit, {partition, <<"smallpart">>}, max_nodes_per_job, 4},
               {self(), make_ref()},
               State),

           JobSpec = #{
               user => <<"biguser">>,
               account => <<>>,
               partition => <<"smallpart">>,
               num_nodes => 10  %% Exceeds 4 node limit
           },

           Result = flurm_limits:handle_call(
               {check_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, {error, {limit_exceeded, num_nodes, 10, 4}}, #state{}}, Result)
       end},

      {"check_limits fails when num_cpus exceeds max_cpus_per_job",
       fun() ->
           State = #state{},
           flurm_limits:handle_call(
               {set_limit, {user, <<"cpuuser">>}, max_cpus_per_job, 8},
               {self(), make_ref()},
               State),

           JobSpec = #{
               user => <<"cpuuser">>,
               account => <<>>,
               partition => <<"default">>,
               num_cpus => 16
           },

           Result = flurm_limits:handle_call(
               {check_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, {error, {limit_exceeded, num_cpus, 16, 8}}, #state{}}, Result)
       end},

      {"check_limits fails when memory_mb exceeds max_mem_per_job",
       fun() ->
           State = #state{},
           flurm_limits:handle_call(
               {set_limit, {user, <<"memuser">>}, max_mem_per_job, 4096},
               {self(), make_ref()},
               State),

           JobSpec = #{
               user => <<"memuser">>,
               account => <<>>,
               partition => <<"default">>,
               memory_mb => 8192
           },

           Result = flurm_limits:handle_call(
               {check_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, {error, {limit_exceeded, memory_mb, 8192, 4096}}, #state{}}, Result)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - check_submit_limits
%%====================================================================

check_submit_limits_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"check_submit_limits returns ok when under limits",
       fun() ->
           State = #state{},
           JobSpec = #{
               user => <<"submituser1">>,
               account => <<"submitacct1">>,
               partition => <<"default">>
           },

           Result = flurm_limits:handle_call(
               {check_submit_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result)
       end},

      {"check_submit_limits fails when max_submit exceeded",
       fun() ->
           State = #state{},
           flurm_limits:handle_call(
               {set_limit, {user, <<"submitlimited">>}, max_submit, 5},
               {self(), make_ref()},
               State),

           %% Simulate 3 running + 2 pending = 5 total jobs
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"submitlimited">>},
               running_jobs = 3,
               pending_jobs = 2
           }),

           JobSpec = #{
               user => <<"submitlimited">>,
               account => <<>>,
               partition => <<"default">>
           },

           Result = flurm_limits:handle_call(
               {check_submit_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, {error, {max_submit_exceeded, 5, 5}}, #state{}}, Result)
       end},

      {"check_submit_limits fails when grp_submit exceeded",
       fun() ->
           State = #state{},
           flurm_limits:handle_call(
               {set_limit, {account, <<"grpsubmitacct">>}, grp_submit, 10},
               {self(), make_ref()},
               State),

           %% User is fine
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"grpuser">>},
               running_jobs = 1,
               pending_jobs = 1
           }),
           %% Account is at limit
           ets:insert(?USAGE_TABLE, #usage{
               key = {account, <<"grpsubmitacct">>},
               running_jobs = 5,
               pending_jobs = 5
           }),

           JobSpec = #{
               user => <<"grpuser">>,
               account => <<"grpsubmitacct">>,
               partition => <<"default">>
           },

           Result = flurm_limits:handle_call(
               {check_submit_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, {error, {grp_submit_exceeded, 10, 10}}, #state{}}, Result)
       end},

      {"check_submit_limits passes with zero limit (unlimited)",
       fun() ->
           State = #state{},
           %% Zero means unlimited
           flurm_limits:handle_call(
               {set_limit, {user, <<"unlimitedsubmit">>}, max_submit, 0},
               {self(), make_ref()},
               State),

           %% Lots of jobs but unlimited
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"unlimitedsubmit">>},
               running_jobs = 1000,
               pending_jobs = 1000
           }),

           JobSpec = #{
               user => <<"unlimitedsubmit">>,
               account => <<>>,
               partition => <<"default">>
           },

           Result = flurm_limits:handle_call(
               {check_submit_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - enforce
%%====================================================================

enforce_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"enforce start increments running jobs and decrements pending",
       fun() ->
           State = #state{},
           %% Set initial usage
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"enforceuser1">>},
               running_jobs = 1,
               pending_jobs = 3
           }),

           JobInfo = #{
               user => <<"enforceuser1">>,
               account => <<>>,
               tres => #{cpu => 4, memory => 8192}
           },

           Result = flurm_limits:handle_call(
               {enforce, start, 1001, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [Usage] = ets:lookup(?USAGE_TABLE, {user, <<"enforceuser1">>}),
           ?assertEqual(2, Usage#usage.running_jobs),
           ?assertEqual(2, Usage#usage.pending_jobs),
           ?assertEqual(4, maps:get(cpu, Usage#usage.tres_used)),
           ?assertEqual(8192, maps:get(memory, Usage#usage.tres_used))
       end},

      {"enforce start updates account usage when account provided",
       fun() ->
           State = #state{},
           JobInfo = #{
               user => <<"enforceuser2">>,
               account => <<"enforceacct2">>,
               tres => #{cpu => 8}
           },

           Result = flurm_limits:handle_call(
               {enforce, start, 1002, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [AcctUsage] = ets:lookup(?USAGE_TABLE, {account, <<"enforceacct2">>}),
           ?assertEqual(1, AcctUsage#usage.running_jobs),
           ?assertEqual(8, maps:get(cpu, AcctUsage#usage.tres_used))
       end},

      {"enforce stop decrements running jobs and updates TRES mins",
       fun() ->
           State = #state{},
           %% Set initial usage with running job
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"enforceuser3">>},
               running_jobs = 2,
               pending_jobs = 0,
               tres_used = #{cpu => 8, memory => 16384}
           }),

           JobInfo = #{
               user => <<"enforceuser3">>,
               account => <<>>,
               tres => #{cpu => 4, memory => 8192},
               wall_time => 600  %% 10 minutes
           },

           Result = flurm_limits:handle_call(
               {enforce, stop, 1003, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [Usage] = ets:lookup(?USAGE_TABLE, {user, <<"enforceuser3">>}),
           ?assertEqual(1, Usage#usage.running_jobs),
           ?assertEqual(4, maps:get(cpu, Usage#usage.tres_used)),
           ?assertEqual(8192, maps:get(memory, Usage#usage.tres_used)),
           %% TRES mins: 4 cpu * 10 min = 40 cpu-mins
           ?assertEqual(40.0, maps:get(cpu, Usage#usage.tres_mins))
       end},

      {"enforce stop handles empty account",
       fun() ->
           State = #state{},
           JobInfo = #{
               user => <<"enforceuser4">>,
               account => <<>>,
               tres => #{cpu => 2},
               wall_time => 120
           },

           %% Ensure user has running job
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"enforceuser4">>},
               running_jobs = 1,
               tres_used = #{cpu => 2}
           }),

           Result = flurm_limits:handle_call(
               {enforce, stop, 1004, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result)
       end},

      {"enforce stop updates account when provided",
       fun() ->
           State = #state{},
           %% Set up account with running job
           ets:insert(?USAGE_TABLE, #usage{
               key = {account, <<"enforceacct5">>},
               running_jobs = 1,
               tres_used = #{gpu => 2}
           }),

           JobInfo = #{
               user => <<"enforceuser5">>,
               account => <<"enforceacct5">>,
               tres => #{gpu => 2},
               wall_time => 300
           },

           Result = flurm_limits:handle_call(
               {enforce, stop, 1005, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [AcctUsage] = ets:lookup(?USAGE_TABLE, {account, <<"enforceacct5">>}),
           ?assertEqual(0, AcctUsage#usage.running_jobs),
           ?assertEqual(0, maps:get(gpu, AcctUsage#usage.tres_used, 0)),
           %% 2 GPU * 5 min = 10 gpu-mins
           ?assertEqual(10.0, maps:get(gpu, AcctUsage#usage.tres_mins))
       end},

      {"enforce stop does not go negative on running_jobs",
       fun() ->
           State = #state{},
           %% Start with 0 running jobs (edge case)
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"enforceuser6">>},
               running_jobs = 0,
               tres_used = #{}
           }),

           JobInfo = #{
               user => <<"enforceuser6">>,
               account => <<>>,
               tres => #{cpu => 1},
               wall_time => 60
           },

           Result = flurm_limits:handle_call(
               {enforce, stop, 1006, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [Usage] = ets:lookup(?USAGE_TABLE, {user, <<"enforceuser6">>}),
           ?assertEqual(0, Usage#usage.running_jobs)  %% max(0, 0-1) = 0
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - reset_usage
%%====================================================================

reset_usage_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"reset_usage resets all user usage records",
       fun() ->
           State = #state{},
           %% Create some user usage records
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"resetuser1">>},
               running_jobs = 5,
               pending_jobs = 10,
               tres_used = #{cpu => 100},
               tres_mins = #{cpu => 5000.0}
           }),
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"resetuser2">>},
               running_jobs = 3,
               pending_jobs = 7,
               tres_used = #{gpu => 4},
               tres_mins = #{gpu => 2000.0}
           }),
           %% Also add an account record that should NOT be reset
           ets:insert(?USAGE_TABLE, #usage{
               key = {account, <<"keepacct">>},
               running_jobs = 10,
               tres_mins = #{cpu => 10000.0}
           }),

           Result = flurm_limits:handle_call(
               {reset_usage, user},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           %% User records should be reset
           [U1] = ets:lookup(?USAGE_TABLE, {user, <<"resetuser1">>}),
           ?assertEqual(0, U1#usage.running_jobs),
           ?assertEqual(0, U1#usage.pending_jobs),
           ?assertEqual(#{}, U1#usage.tres_used),
           ?assertEqual(#{}, U1#usage.tres_mins),

           [U2] = ets:lookup(?USAGE_TABLE, {user, <<"resetuser2">>}),
           ?assertEqual(0, U2#usage.running_jobs),

           %% Account record should be unchanged
           [A1] = ets:lookup(?USAGE_TABLE, {account, <<"keepacct">>}),
           ?assertEqual(10, A1#usage.running_jobs)
       end},

      {"reset_usage resets all account usage records",
       fun() ->
           State = #state{},
           %% Create account usage records
           ets:insert(?USAGE_TABLE, #usage{
               key = {account, <<"resetacct1">>},
               running_jobs = 20,
               tres_mins = #{cpu => 50000.0}
           }),
           %% User record should NOT be reset
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"keepuser">>},
               running_jobs = 5,
               tres_mins = #{cpu => 1000.0}
           }),

           Result = flurm_limits:handle_call(
               {reset_usage, account},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           %% Account should be reset
           [A1] = ets:lookup(?USAGE_TABLE, {account, <<"resetacct1">>}),
           ?assertEqual(0, A1#usage.running_jobs),
           ?assertEqual(#{}, A1#usage.tres_mins),

           %% User should be unchanged
           [U1] = ets:lookup(?USAGE_TABLE, {user, <<"keepuser">>}),
           ?assertEqual(5, U1#usage.running_jobs)
       end}
     ]}.

%%====================================================================
%% handle_call/3 Tests - unknown request
%%====================================================================

unknown_request_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"unknown request returns error",
       fun() ->
           State = #state{},
           Result = flurm_limits:handle_call(
               {unknown_operation, some, args},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, {error, unknown_request}, #state{}}, Result)
       end}
     ]}.

%%====================================================================
%% handle_cast/2 Tests
%%====================================================================

handle_cast_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"handle_cast ignores messages and returns noreply",
       fun() ->
           State = #state{},
           Result = flurm_limits:handle_cast(some_message, State),
           ?assertMatch({noreply, #state{}}, Result),

           Result2 = flurm_limits:handle_cast({any, tuple, here}, State),
           ?assertMatch({noreply, #state{}}, Result2)
       end}
     ]}.

%%====================================================================
%% handle_info/2 Tests
%%====================================================================

handle_info_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"handle_info ignores messages and returns noreply",
       fun() ->
           State = #state{},
           Result = flurm_limits:handle_info(any_message, State),
           ?assertMatch({noreply, #state{}}, Result),

           Result2 = flurm_limits:handle_info({timeout, make_ref(), check}, State),
           ?assertMatch({noreply, #state{}}, Result2)
       end}
     ]}.

%%====================================================================
%% terminate/2 Tests
%%====================================================================

terminate_test_() ->
    [
     {"terminate returns ok",
      fun() ->
          State = #state{},
          ?assertEqual(ok, flurm_limits:terminate(normal, State)),
          ?assertEqual(ok, flurm_limits:terminate(shutdown, State)),
          ?assertEqual(ok, flurm_limits:terminate({error, reason}, State))
      end}
    ].

%%====================================================================
%% code_change/3 Tests
%%====================================================================

code_change_test_() ->
    [
     {"code_change returns ok with state unchanged",
      fun() ->
          State = #state{},
          ?assertEqual({ok, State}, flurm_limits:code_change("1.0.0", State, [])),
          ?assertEqual({ok, State}, flurm_limits:code_change(undefined, State, extra))
      end}
    ].

%%====================================================================
%% API Function Tests (through gen_server)
%%====================================================================

%% These tests verify the API functions work correctly when the gen_server is running

api_integration_test_() ->
    {setup,
     fun() ->
         setup(),
         %% Start the gen_server for API tests
         {ok, Pid} = flurm_limits:start_link(),
         Pid
     end,
     fun(Pid) ->
         %% Stop the gen_server with proper monitor/wait pattern
         case is_process_alive(Pid) of
             true ->
                 Ref = monitor(process, Pid),
                 unlink(Pid),
                 catch gen_server:stop(Pid, shutdown, 5000),
                 receive
                     {'DOWN', Ref, process, Pid, _} -> ok
                 after 5000 ->
                     demonitor(Ref, [flush]),
                     catch exit(Pid, kill)
                 end;
             false ->
                 ok
         end,
         cleanup()
     end,
     fun(_Pid) ->
         [
          {"set_user_limit API works",
           fun() ->
               ?assertEqual(ok, flurm_limits:set_user_limit(<<"apiuser">>, max_jobs, 15))
           end},

          {"set_account_limit API works",
           fun() ->
               ?assertEqual(ok, flurm_limits:set_account_limit(<<"apiacct">>, grp_jobs, 30))
           end},

          {"set_partition_limit API works",
           fun() ->
               ?assertEqual(ok, flurm_limits:set_partition_limit(<<"apipart">>, max_wall, 7200))
           end},

          {"get_effective_limits API works",
           fun() ->
               Limits = flurm_limits:get_effective_limits(<<"apiuser">>, <<"apiacct">>, <<"default">>),
               ?assertEqual(effective, Limits#limit_spec.key),
               ?assertEqual(15, Limits#limit_spec.max_jobs)
           end},

          {"check_limits API works",
           fun() ->
               JobSpec = #{
                   user => <<"checkuser">>,
                   account => <<"checkacct">>,
                   partition => <<"default">>
               },
               ?assertEqual(ok, flurm_limits:check_limits(JobSpec))
           end},

          {"check_submit_limits API works",
           fun() ->
               JobSpec = #{
                   user => <<"submitcheckuser">>,
                   account => <<"submitcheckacct">>,
                   partition => <<"default">>
               },
               ?assertEqual(ok, flurm_limits:check_submit_limits(JobSpec))
           end},

          {"enforce_limit API works for start",
           fun() ->
               JobInfo = #{
                   user => <<"enforceapiuser">>,
                   account => <<"enforceapiacct">>,
                   tres => #{cpu => 4}
               },
               ?assertEqual(ok, flurm_limits:enforce_limit(start, 9001, JobInfo))
           end},

          {"enforce_limit API works for stop",
           fun() ->
               JobInfo = #{
                   user => <<"enforceapiuser">>,
                   account => <<"enforceapiacct">>,
                   tres => #{cpu => 4},
                   wall_time => 120
               },
               ?assertEqual(ok, flurm_limits:enforce_limit(stop, 9001, JobInfo))
           end},

          {"reset_usage API works",
           fun() ->
               ?assertEqual(ok, flurm_limits:reset_usage(user)),
               ?assertEqual(ok, flurm_limits:reset_usage(account))
           end},

          {"get_user_limits returns limits",
           fun() ->
               flurm_limits:set_user_limit(<<"getlimitsuser">>, max_jobs, 25),
               Limits = flurm_limits:get_user_limits(<<"getlimitsuser">>),
               ?assert(is_list(Limits)),
               ?assert(length(Limits) >= 1)
           end},

          {"get_account_limits returns limit spec",
           fun() ->
               flurm_limits:set_account_limit(<<"getlimitsacct">>, grp_jobs, 50),
               Limit = flurm_limits:get_account_limits(<<"getlimitsacct">>),
               ?assertNotEqual(undefined, Limit),
               ?assertEqual(50, Limit#limit_spec.grp_jobs)
           end},

          {"get_account_limits returns undefined for unknown account",
           fun() ->
               ?assertEqual(undefined, flurm_limits:get_account_limits(<<"nonexistent">>))
           end},

          {"get_usage returns usage record",
           fun() ->
               %% Trigger usage creation via enforce
               JobInfo = #{
                   user => <<"usageuser">>,
                   account => <<>>,
                   tres => #{cpu => 2}
               },
               flurm_limits:enforce_limit(start, 9002, JobInfo),

               Usage = flurm_limits:get_usage(user, <<"usageuser">>),
               ?assertNotEqual(undefined, Usage),
               ?assertEqual(1, Usage#usage.running_jobs)
           end},

          {"get_usage returns undefined for unknown user",
           fun() ->
               ?assertEqual(undefined, flurm_limits:get_usage(user, <<"nonexistentuser">>))
           end}
         ]
     end}.

%%====================================================================
%% Edge Cases and Corner Cases
%%====================================================================

edge_cases_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"check_limits with default values in job spec",
       fun() ->
           State = #state{},
           %% Job spec with only required fields - optional should default
           JobSpec = #{
               user => <<"minimaluser">>,
               partition => <<"default">>
           },

           Result = flurm_limits:handle_call(
               {check_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result)
       end},

      {"check_submit_limits with empty account",
       fun() ->
           State = #state{},
           JobSpec = #{
               user => <<"noacctuser">>,
               partition => <<"default">>
               %% account not specified, should default to <<>>
           },

           Result = flurm_limits:handle_call(
               {check_submit_limits, JobSpec},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result)
       end},

      {"enforce start with empty tres",
       fun() ->
           State = #state{},
           JobInfo = #{
               user => <<"notresuser">>,
               account => <<>>
               %% tres not specified, should default to #{}
           },

           Result = flurm_limits:handle_call(
               {enforce, start, 8001, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result)
       end},

      {"enforce stop with zero wall_time",
       fun() ->
           State = #state{},
           %% Set up existing usage
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"zerowalluser">>},
               running_jobs = 1,
               tres_used = #{cpu => 4}
           }),

           JobInfo = #{
               user => <<"zerowalluser">>,
               account => <<>>,
               tres => #{cpu => 4}
               %% wall_time not specified, should default to 0
           },

           Result = flurm_limits:handle_call(
               {enforce, stop, 8002, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [Usage] = ets:lookup(?USAGE_TABLE, {user, <<"zerowalluser">>}),
           %% TRES mins should be 0 when wall_time is 0
           ?assertEqual(0.0, maps:get(cpu, Usage#usage.tres_mins, 0.0))
       end},

      {"TRES subtraction does not go negative",
       fun() ->
           State = #state{},
           %% Set up usage with less TRES than we're trying to subtract
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"subtractuser">>},
               running_jobs = 1,
               tres_used = #{cpu => 2}  %% Only 2 CPUs allocated
           }),

           JobInfo = #{
               user => <<"subtractuser">>,
               account => <<>>,
               tres => #{cpu => 5},  %% Trying to free 5 CPUs
               wall_time => 60
           },

           Result = flurm_limits:handle_call(
               {enforce, stop, 8003, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [Usage] = ets:lookup(?USAGE_TABLE, {user, <<"subtractuser">>}),
           %% Should be max(0, 2-5) = 0, not -3
           ?assertEqual(0, maps:get(cpu, Usage#usage.tres_used, 0))
       end},

      {"pending jobs does not go negative on enforce start",
       fun() ->
           State = #state{},
           %% Set up usage with 0 pending jobs
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"nopendinguser">>},
               running_jobs = 0,
               pending_jobs = 0
           }),

           JobInfo = #{
               user => <<"nopendinguser">>,
               account => <<>>,
               tres => #{}
           },

           Result = flurm_limits:handle_call(
               {enforce, start, 8004, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [Usage] = ets:lookup(?USAGE_TABLE, {user, <<"nopendinguser">>}),
           ?assertEqual(1, Usage#usage.running_jobs),
           ?assertEqual(0, Usage#usage.pending_jobs)  %% max(0, 0-1) = 0
       end},

      {"specific user+account+partition limits override general limits",
       fun() ->
           State = #state{},
           %% General user limit: 100 jobs
           flurm_limits:handle_call(
               {set_limit, {user, <<"specificuser">>}, max_jobs, 100},
               {self(), make_ref()},
               State),

           %% Specific user+account+partition limit: 5 jobs (very restrictive)
           ets:insert(?USER_LIMITS_TABLE, #limit_spec{
               key = {user, <<"specificuser">>, <<"specificacct">>, <<"specificpart">>},
               max_jobs = 5
           }),

           Result = flurm_limits:handle_call(
               {get_effective, <<"specificuser">>, <<"specificacct">>, <<"specificpart">>},
               {self(), make_ref()},
               State),
           {reply, Limits, _} = Result,
           %% Should use the more restrictive 5
           ?assertEqual(5, Limits#limit_spec.max_jobs)
       end},

      {"multiple TRES types are tracked correctly",
       fun() ->
           State = #state{},
           JobInfo = #{
               user => <<"multitresuser">>,
               account => <<"multitresacct">>,
               tres => #{cpu => 8, memory => 32768, gpu => 2, nic => 1}
           },

           Result = flurm_limits:handle_call(
               {enforce, start, 8005, JobInfo},
               {self(), make_ref()},
               State),
           ?assertMatch({reply, ok, #state{}}, Result),

           [Usage] = ets:lookup(?USAGE_TABLE, {user, <<"multitresuser">>}),
           ?assertEqual(8, maps:get(cpu, Usage#usage.tres_used)),
           ?assertEqual(32768, maps:get(memory, Usage#usage.tres_used)),
           ?assertEqual(2, maps:get(gpu, Usage#usage.tres_used)),
           ?assertEqual(1, maps:get(nic, Usage#usage.tres_used))
       end}
     ]}.

%%====================================================================
%% TRES Accumulation Tests
%%====================================================================

tres_accumulation_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      {"TRES used accumulates across multiple job starts",
       fun() ->
           State = #state{},
           User = <<"accumuser">>,

           %% Start first job
           Job1 = #{user => User, account => <<>>, tres => #{cpu => 4, memory => 8192}},
           flurm_limits:handle_call({enforce, start, 7001, Job1}, {self(), make_ref()}, State),

           %% Start second job
           Job2 = #{user => User, account => <<>>, tres => #{cpu => 8, memory => 16384}},
           flurm_limits:handle_call({enforce, start, 7002, Job2}, {self(), make_ref()}, State),

           [Usage] = ets:lookup(?USAGE_TABLE, {user, User}),
           ?assertEqual(2, Usage#usage.running_jobs),
           ?assertEqual(12, maps:get(cpu, Usage#usage.tres_used)),  %% 4 + 8
           ?assertEqual(24576, maps:get(memory, Usage#usage.tres_used))  %% 8192 + 16384
       end},

      {"TRES minutes accumulate across multiple job completions",
       fun() ->
           State = #state{},
           User = <<"tresminsuser">>,

           %% Set up usage for 2 running jobs
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, User},
               running_jobs = 2,
               tres_used = #{cpu => 12},
               tres_mins = #{}
           }),

           %% Complete first job (4 CPUs for 10 minutes = 40 cpu-mins)
           Job1 = #{user => User, account => <<>>, tres => #{cpu => 4}, wall_time => 600},
           flurm_limits:handle_call({enforce, stop, 7003, Job1}, {self(), make_ref()}, State),

           %% Complete second job (8 CPUs for 5 minutes = 40 cpu-mins)
           Job2 = #{user => User, account => <<>>, tres => #{cpu => 8}, wall_time => 300},
           flurm_limits:handle_call({enforce, stop, 7004, Job2}, {self(), make_ref()}, State),

           [Usage] = ets:lookup(?USAGE_TABLE, {user, User}),
           ?assertEqual(0, Usage#usage.running_jobs),
           ?assertEqual(0, maps:get(cpu, Usage#usage.tres_used)),
           ?assertEqual(80.0, maps:get(cpu, Usage#usage.tres_mins))  %% 40 + 40
       end}
     ]}.

%%====================================================================
%% Additional Coverage Tests - targeting uncovered lines
%%====================================================================

additional_coverage_test_() ->
    {setup,
     fun() ->
         setup(),
         flurm_limits:init([])
     end,
     fun(_) -> cleanup() end,
     [
      %% Test for line 278: max_submit = 0 path in do_check_submit_limits
      %% This path is taken when effective max_submit is 0 (unlimited)
      %% Need to use a partition that doesn't have max_submit set (not 'default')
      {"check_submit_limits with effective zero max_submit limit",
       fun() ->
           State = #state{},
           %% Use a partition that has no limits set (not 'default' which has max_submit=5000)
           %% This ensures compute_effective_limits returns max_submit=0

           %% Simulate having many jobs (should still pass because 0 = unlimited)
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"zeromaxsubmituser">>},
               running_jobs = 5000,
               pending_jobs = 5000
           }),

           %% Use a partition name that has no limits defined
           JobSpec = #{
               user => <<"zeromaxsubmituser">>,
               account => <<>>,
               partition => <<"unlimitedpart">>  %% This partition has no limits set
           },

           Result = flurm_limits:handle_call(
               {check_submit_limits, JobSpec},
               {self(), make_ref()},
               State),
           %% Should return ok because effective max_submit=0 means unlimited
           ?assertMatch({reply, ok, #state{}}, Result)
       end},

      %% Test for line 289: success path with non-zero grp_submit under limit
      {"check_submit_limits passes when under both max_submit and grp_submit limits",
       fun() ->
           State = #state{},
           %% Set non-zero limits for both user and account
           flurm_limits:handle_call(
               {set_limit, {user, <<"undersubmituser">>}, max_submit, 100},
               {self(), make_ref()},
               State),
           flurm_limits:handle_call(
               {set_limit, {account, <<"undersubmitacct">>}, grp_submit, 200},
               {self(), make_ref()},
               State),

           %% User has some jobs but under limit
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"undersubmituser">>},
               running_jobs = 10,
               pending_jobs = 10
           }),
           %% Account has some jobs but under limit
           ets:insert(?USAGE_TABLE, #usage{
               key = {account, <<"undersubmitacct">>},
               running_jobs = 50,
               pending_jobs = 50
           }),

           JobSpec = #{
               user => <<"undersubmituser">>,
               account => <<"undersubmitacct">>,
               partition => <<"default">>
           },

           Result = flurm_limits:handle_call(
               {check_submit_limits, JobSpec},
               {self(), make_ref()},
               State),
           %% Should return ok (line 289) because both are under their limits
           ?assertMatch({reply, ok, #state{}}, Result)
       end},

      %% Test for line 352: merge_tres_limits when both maps have same key with values
      {"merge TRES limits selects minimum when both have same key with non-zero values",
       fun() ->
           State = #state{},
           %% User has CPU limit of 100
           flurm_limits:handle_call(
               {set_limit, {user, <<"mergetresuser">>}, max_tres, #{cpu => 100, gpu => 8}},
               {self(), make_ref()},
               State),
           %% Account has CPU limit of 50 (more restrictive)
           flurm_limits:handle_call(
               {set_limit, {account, <<"mergetresacct">>}, max_tres, #{cpu => 50, gpu => 16}},
               {self(), make_ref()},
               State),

           Result = flurm_limits:handle_call(
               {get_effective, <<"mergetresuser">>, <<"mergetresacct">>, <<"default">>},
               {self(), make_ref()},
               State),
           {reply, Limits, _} = Result,
           TRES = Limits#limit_spec.max_tres,
           %% Should have minimum: 50 for cpu (min 100, 50), 8 for gpu (min 8, 16)
           ?assertEqual(50, maps:get(cpu, TRES)),
           ?assertEqual(8, maps:get(gpu, TRES))
       end},

      %% Test for line 369: check_grp_jobs returns ok when under limit (non-zero limit)
      {"check_limits passes when under non-zero grp_jobs limit",
       fun() ->
           State = #state{},
           %% Set grp_jobs to 50 (non-zero limit)
           flurm_limits:handle_call(
               {set_limit, {account, <<"undergrpacct">>}, grp_jobs, 50},
               {self(), make_ref()},
               State),

           %% Account has 25 running jobs (under the 50 limit)
           ets:insert(?USAGE_TABLE, #usage{
               key = {account, <<"undergrpacct">>},
               running_jobs = 25,
               pending_jobs = 0
           }),

           JobSpec = #{
               user => <<"undergrpuser">>,
               account => <<"undergrpacct">>,
               partition => <<"default">>
           },

           Result = flurm_limits:handle_call(
               {check_limits, JobSpec},
               {self(), make_ref()},
               State),
           %% Should return ok (line 369) because 25 < 50
           ?assertMatch({reply, ok, #state{}}, Result)
       end},

      %% Extra test to ensure max_jobs under-limit path is covered
      {"check_limits passes when under non-zero max_jobs limit",
       fun() ->
           State = #state{},
           %% Set max_jobs to 20 (non-zero limit)
           flurm_limits:handle_call(
               {set_limit, {user, <<"underjobsuser">>}, max_jobs, 20},
               {self(), make_ref()},
               State),

           %% User has 5 running jobs (under the 20 limit)
           ets:insert(?USAGE_TABLE, #usage{
               key = {user, <<"underjobsuser">>},
               running_jobs = 5,
               pending_jobs = 0
           }),

           JobSpec = #{
               user => <<"underjobsuser">>,
               account => <<>>,
               partition => <<"default">>
           },

           Result = flurm_limits:handle_call(
               {check_limits, JobSpec},
               {self(), make_ref()},
               State),
           %% Should return ok because 5 < 20
           ?assertMatch({reply, ok, #state{}}, Result)
       end}
     ]}.
