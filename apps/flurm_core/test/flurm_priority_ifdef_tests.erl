%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_priority internal functions via -ifdef(TEST) exports
%%%
%%% These tests exercise pure calculation functions. Some functions
%%% are already exported for testing (age_factor, size_factor, etc.)
%%% while others are exported via -ifdef(TEST).
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_priority_ifdef_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% ETS table name used by flurm_priority
-define(WEIGHTS_TABLE, flurm_priority_weights).

%%====================================================================
%% Test Fixtures
%%====================================================================

sample_job_info() ->
    #{
        job_id => 1001,
        user => <<"testuser">>,
        account => <<"testaccount">>,
        partition => <<"compute">>,
        num_nodes => 4,
        num_cpus => 16,
        memory_mb => 8192,
        time_limit => 3600,
        submit_time => erlang:system_time(second) - 1800,  % 30 minutes ago
        nice => 0,
        qos => <<"normal">>
    }.

setup_weights_table() ->
    %% Ensure weights table exists with default values
    catch ets:delete(?WEIGHTS_TABLE),
    ets:new(?WEIGHTS_TABLE, [named_table, public, set]),
    ets:insert(?WEIGHTS_TABLE, {weights, #{
        age => 1000,
        job_size => 200,
        partition => 100,
        qos => 500,
        nice => 100,
        fairshare => 5000
    }}),
    ok.

cleanup_weights_table() ->
    catch ets:delete(?WEIGHTS_TABLE),
    ok.

%%====================================================================
%% age_factor/2 Tests
%%====================================================================

age_factor_test_() ->
    {"age_factor/2 calculates age-based priority factor",
     [
      {"returns 0.0 for newly submitted job",
       fun() ->
           Now = erlang:system_time(second),
           JobInfo = #{submit_time => Now},
           Factor = flurm_priority:age_factor(JobInfo, Now),
           ?assert(Factor >= 0.0),
           ?assert(Factor < 0.01)  % Very small for new job
       end},

      {"returns higher factor for older jobs",
       fun() ->
           Now = erlang:system_time(second),
           OneHourAgo = Now - 3600,
           OneDayAgo = Now - 86400,

           NewJob = #{submit_time => Now},
           HourJob = #{submit_time => OneHourAgo},
           DayJob = #{submit_time => OneDayAgo},

           NewFactor = flurm_priority:age_factor(NewJob, Now),
           HourFactor = flurm_priority:age_factor(HourJob, Now),
           DayFactor = flurm_priority:age_factor(DayJob, Now),

           ?assert(HourFactor > NewFactor),
           ?assert(DayFactor > HourFactor)
       end},

      {"caps at maximum age",
       fun() ->
           Now = erlang:system_time(second),
           VeryOld = Now - (7 * 86400),  % 7 days ago
           ExtremelyOld = Now - (30 * 86400),  % 30 days ago

           VeryOldJob = #{submit_time => VeryOld},
           ExtremeJob = #{submit_time => ExtremelyOld},

           VeryOldFactor = flurm_priority:age_factor(VeryOldJob, Now),
           ExtremeFactor = flurm_priority:age_factor(ExtremeJob, Now),

           % Both should be capped at similar high values
           ?assert(VeryOldFactor > 0.9),
           ?assert(ExtremeFactor > 0.9),
           ?assert(abs(VeryOldFactor - ExtremeFactor) < 0.1)
       end},

      {"handles erlang:now() format timestamp",
       fun() ->
           Now = erlang:system_time(second),
           MegaSecs = Now div 1000000,
           Secs = Now rem 1000000,
           OldTimestamp = {MegaSecs - 1, Secs, 0},  % ~11.5 days ago

           JobInfo = #{submit_time => OldTimestamp},
           Factor = flurm_priority:age_factor(JobInfo, Now),
           ?assert(Factor > 0.5)  % Should be significant
       end},

      {"handles missing submit_time",
       fun() ->
           Now = erlang:system_time(second),
           JobInfo = #{},
           Factor = flurm_priority:age_factor(JobInfo, Now),
           ?assert(Factor >= 0.0),
           ?assert(Factor < 0.01)
       end}
     ]}.

%%====================================================================
%% size_factor/2 Tests
%%====================================================================

size_factor_test_() ->
    {"size_factor/2 calculates job size priority factor",
     [
      {"returns higher factor for smaller jobs",
       fun() ->
           ClusterSize = 100,
           SmallJob = #{num_nodes => 1, num_cpus => 1},
           LargeJob = #{num_nodes => 50, num_cpus => 32},

           SmallFactor = flurm_priority:size_factor(SmallJob, ClusterSize),
           LargeFactor = flurm_priority:size_factor(LargeJob, ClusterSize),

           ?assert(SmallFactor > LargeFactor)
       end},

      {"returns factor between 0 and 1",
       fun() ->
           ClusterSize = 100,
           Jobs = [
               #{num_nodes => 1, num_cpus => 1},
               #{num_nodes => 10, num_cpus => 8},
               #{num_nodes => 50, num_cpus => 32},
               #{num_nodes => 100, num_cpus => 64}
           ],

           lists:foreach(
               fun(Job) ->
                   Factor = flurm_priority:size_factor(Job, ClusterSize),
                   ?assert(Factor >= 0.0),
                   ?assert(Factor =< 1.0)
               end,
               Jobs)
       end},

      {"handles single node cluster",
       fun() ->
           ClusterSize = 1,
           Job = #{num_nodes => 1, num_cpus => 4},
           Factor = flurm_priority:size_factor(Job, ClusterSize),
           ?assert(Factor >= 0.0),
           ?assert(Factor =< 1.0)
       end},

      {"handles missing fields with defaults",
       fun() ->
           ClusterSize = 100,
           Job = #{},  % Empty job info
           Factor = flurm_priority:size_factor(Job, ClusterSize),
           ?assert(Factor >= 0.0),
           ?assert(Factor =< 1.0)
       end}
     ]}.

%%====================================================================
%% nice_factor/1 Tests
%%====================================================================

nice_factor_test_() ->
    {"nice_factor/1 calculates nice value priority factor",
     [
      {"returns 0.0 for nice=0",
       fun() ->
           JobInfo = #{nice => 0},
           Factor = flurm_priority:nice_factor(JobInfo),
           ?assertEqual(0.0, Factor)
       end},

      {"returns positive factor for negative nice (higher priority)",
       fun() ->
           JobInfo = #{nice => -5000},
           Factor = flurm_priority:nice_factor(JobInfo),
           ?assert(Factor > 0.0),
           ?assertEqual(0.5, Factor)  % -(-5000)/10000 = 0.5
       end},

      {"returns negative factor for positive nice (lower priority)",
       fun() ->
           JobInfo = #{nice => 5000},
           Factor = flurm_priority:nice_factor(JobInfo),
           ?assert(Factor < 0.0),
           ?assertEqual(-0.5, Factor)  % -(5000)/10000 = -0.5
       end},

      {"handles extreme nice values",
       fun() ->
           HighPriority = #{nice => -10000},
           LowPriority = #{nice => 10000},

           HighFactor = flurm_priority:nice_factor(HighPriority),
           LowFactor = flurm_priority:nice_factor(LowPriority),

           ?assertEqual(1.0, HighFactor),
           ?assertEqual(-1.0, LowFactor)
       end},

      {"handles missing nice value",
       fun() ->
           JobInfo = #{},
           Factor = flurm_priority:nice_factor(JobInfo),
           ?assertEqual(0.0, Factor)
       end}
     ]}.

%%====================================================================
%% partition_factor/1 Tests
%%====================================================================

partition_factor_test_() ->
    {"partition_factor/1 calculates partition priority factor",
     [
      {"returns 0.5 for default partition when registry unavailable",
       fun() ->
           JobInfo = #{partition => <<"default">>},
           Factor = flurm_priority:partition_factor(JobInfo),
           % Should return 0.5 as default when registry not available
           ?assertEqual(0.5, Factor)
       end},

      {"returns 0.5 for unknown partition",
       fun() ->
           JobInfo = #{partition => <<"nonexistent_partition">>},
           Factor = flurm_priority:partition_factor(JobInfo),
           ?assertEqual(0.5, Factor)
       end},

      {"handles missing partition field",
       fun() ->
           JobInfo = #{},
           Factor = flurm_priority:partition_factor(JobInfo),
           ?assertEqual(0.5, Factor)
       end}
     ]}.

%%====================================================================
%% qos_factor/1 Tests (via -ifdef(TEST))
%%====================================================================

qos_factor_test_() ->
    {"qos_factor/1 calculates QOS priority factor",
     [
      {"returns 1.0 for high QOS",
       fun() ->
           JobInfo = #{qos => <<"high">>},
           Factor = flurm_priority:qos_factor(JobInfo),
           ?assertEqual(1.0, Factor)
       end},

      {"returns 0.5 for normal QOS",
       fun() ->
           JobInfo = #{qos => <<"normal">>},
           Factor = flurm_priority:qos_factor(JobInfo),
           ?assertEqual(0.5, Factor)
       end},

      {"returns 0.2 for low QOS",
       fun() ->
           JobInfo = #{qos => <<"low">>},
           Factor = flurm_priority:qos_factor(JobInfo),
           ?assertEqual(0.2, Factor)
       end},

      {"returns 0.5 for unknown QOS",
       fun() ->
           JobInfo = #{qos => <<"unknown">>},
           Factor = flurm_priority:qos_factor(JobInfo),
           ?assertEqual(0.5, Factor)
       end},

      {"handles missing QOS field",
       fun() ->
           JobInfo = #{},
           Factor = flurm_priority:qos_factor(JobInfo),
           ?assertEqual(0.5, Factor)
       end}
     ]}.

%%====================================================================
%% fairshare_factor/2 Tests (via -ifdef(TEST))
%%====================================================================

fairshare_factor_test_() ->
    {"fairshare_factor/2 calculates fairshare priority factor",
     [
      {"uses context fairshare value when provided",
       fun() ->
           JobInfo = #{user => <<"testuser">>, account => <<"testaccount">>},
           Context = #{fairshare => 0.75},
           Factor = flurm_priority:fairshare_factor(JobInfo, Context),
           ?assertEqual(0.75, Factor)
       end},

      {"returns 0.5 default when fairshare module unavailable",
       fun() ->
           JobInfo = #{user => <<"testuser">>, account => <<"testaccount">>},
           Context = #{},  % No fairshare in context
           Factor = flurm_priority:fairshare_factor(JobInfo, Context),
           % Should return 0.5 as default when module not available
           ?assertEqual(0.5, Factor)
       end},

      {"handles missing user/account fields",
       fun() ->
           JobInfo = #{},
           Context = #{},
           Factor = flurm_priority:fairshare_factor(JobInfo, Context),
           ?assertEqual(0.5, Factor)
       end}
     ]}.

%%====================================================================
%% calculate_factors/3 Tests (via -ifdef(TEST))
%%====================================================================

calculate_factors_test_() ->
    {"calculate_factors/3 computes all priority factors",
     {setup,
      fun setup_weights_table/0,
      fun(_) -> cleanup_weights_table() end,
      [
       {"returns map with all factor keys",
        fun() ->
            JobInfo = sample_job_info(),
            Context = #{},
            Weights = flurm_priority:get_weights(),
            Factors = flurm_priority:calculate_factors(JobInfo, Context, Weights),

            ?assert(maps:is_key(age, Factors)),
            ?assert(maps:is_key(job_size, Factors)),
            ?assert(maps:is_key(partition, Factors)),
            ?assert(maps:is_key(qos, Factors)),
            ?assert(maps:is_key(nice, Factors)),
            ?assert(maps:is_key(fairshare, Factors))
        end},

       {"all factors are numeric",
        fun() ->
            JobInfo = sample_job_info(),
            Context = #{},
            Weights = flurm_priority:get_weights(),
            Factors = flurm_priority:calculate_factors(JobInfo, Context, Weights),

            maps:foreach(
                fun(_Key, Value) ->
                    ?assert(is_number(Value))
                end,
                Factors)
        end},

       {"factors are in expected ranges",
        fun() ->
            JobInfo = sample_job_info(),
            Context = #{fairshare => 0.6},
            Weights = flurm_priority:get_weights(),
            Factors = flurm_priority:calculate_factors(JobInfo, Context, Weights),

            %% Age: 0.0 to 1.0
            ?assert(maps:get(age, Factors) >= 0.0),
            ?assert(maps:get(age, Factors) =< 1.0),

            %% Size: 0.0 to 1.0
            ?assert(maps:get(job_size, Factors) >= 0.0),
            ?assert(maps:get(job_size, Factors) =< 1.0),

            %% Partition: 0.0 to 1.0
            ?assert(maps:get(partition, Factors) >= 0.0),
            ?assert(maps:get(partition, Factors) =< 1.0),

            %% QOS: 0.0 to 1.0
            ?assert(maps:get(qos, Factors) >= 0.0),
            ?assert(maps:get(qos, Factors) =< 1.0),

            %% Nice: -1.0 to 1.0
            ?assert(maps:get(nice, Factors) >= -1.0),
            ?assert(maps:get(nice, Factors) =< 1.0),

            %% Fairshare: 0.0 to 1.0
            ?assert(maps:get(fairshare, Factors) >= 0.0),
            ?assert(maps:get(fairshare, Factors) =< 1.0)
        end}
      ]}}.

%%====================================================================
%% ensure_weights_table/0 Tests (via -ifdef(TEST))
%%====================================================================

ensure_weights_table_test_() ->
    {"ensure_weights_table/0 creates and initializes weights table",
     [
      {"creates table if not exists",
       fun() ->
           catch ets:delete(?WEIGHTS_TABLE),
           ?assertEqual(undefined, ets:whereis(?WEIGHTS_TABLE)),
           flurm_priority:ensure_weights_table(),
           ?assertNotEqual(undefined, ets:whereis(?WEIGHTS_TABLE)),
           cleanup_weights_table()
       end},

      {"does not recreate existing table",
       fun() ->
           catch ets:delete(?WEIGHTS_TABLE),
           flurm_priority:ensure_weights_table(),
           Tid1 = ets:whereis(?WEIGHTS_TABLE),
           flurm_priority:ensure_weights_table(),
           Tid2 = ets:whereis(?WEIGHTS_TABLE),
           ?assertEqual(Tid1, Tid2),
           cleanup_weights_table()
       end},

      {"initializes with default weights",
       fun() ->
           catch ets:delete(?WEIGHTS_TABLE),
           flurm_priority:ensure_weights_table(),
           [{weights, Weights}] = ets:lookup(?WEIGHTS_TABLE, weights),
           ?assert(maps:is_key(age, Weights)),
           ?assert(maps:is_key(fairshare, Weights)),
           cleanup_weights_table()
       end}
     ]}.

%%====================================================================
%% Integration Tests
%%====================================================================

calculate_priority_integration_test_() ->
    {"calculate_priority integrates all factors correctly",
     {setup,
      fun setup_weights_table/0,
      fun(_) -> cleanup_weights_table() end,
      [
       {"older jobs get higher priority than newer jobs",
        fun() ->
            Now = erlang:system_time(second),
            NewJob = sample_job_info(),
            OldJob = NewJob#{submit_time => Now - 86400},  % 1 day old

            NewPriority = flurm_priority:calculate_priority(NewJob),
            OldPriority = flurm_priority:calculate_priority(OldJob),

            ?assert(OldPriority > NewPriority)
        end},

       {"high QOS jobs get higher priority",
        fun() ->
            NormalJob = sample_job_info(),
            HighJob = NormalJob#{qos => <<"high">>},
            LowJob = NormalJob#{qos => <<"low">>},

            NormalPriority = flurm_priority:calculate_priority(NormalJob),
            HighPriority = flurm_priority:calculate_priority(HighJob),
            LowPriority = flurm_priority:calculate_priority(LowJob),

            ?assert(HighPriority > NormalPriority),
            ?assert(NormalPriority > LowPriority)
        end},

       {"negative nice increases priority",
        fun() ->
            BaseJob = sample_job_info(),
            HighPrioJob = BaseJob#{nice => -5000},
            LowPrioJob = BaseJob#{nice => 5000},

            BasePriority = flurm_priority:calculate_priority(BaseJob),
            HighPriority = flurm_priority:calculate_priority(HighPrioJob),
            LowPriority = flurm_priority:calculate_priority(LowPrioJob),

            ?assert(HighPriority > BasePriority),
            ?assert(BasePriority > LowPriority)
        end}
      ]}}.
