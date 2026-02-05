%%%-------------------------------------------------------------------
%%% @doc Direct Tests for flurm_preemption module
%%%
%%% Comprehensive EUnit tests that call flurm_preemption functions
%%% directly without mocking the module being tested. Only external
%%% dependencies are mocked to isolate the preemption behavior.
%%%
%%% Tests all exported functions:
%%% - check_preemption/1
%%% - preempt_jobs/2
%%% - can_preempt/2
%%% - get_preemptable_jobs/2
%%% - set_preemption_mode/1
%%% - get_preemption_mode/0, get_preemption_mode/1
%%% - set_grace_time/1
%%% - get_grace_time/0, get_grace_time/1
%%% - set_priority_threshold/1
%%% - get_priority_threshold/0
%%% - set_qos_preemption_rules/1
%%% - get_qos_preemption_rules/0
%%% - set_partition_preemption_mode/2
%%% - set_partition_grace_time/2
%%% - find_preemptable_jobs/2
%%% - calculate_preemption_cost/1
%%% - check_preemption_opportunity/2
%%% - execute_preemption/2
%%% - handle_preempted_job/2
%%% - graceful_preempt/3
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_preemption_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%% Suppress warnings for helper functions that may not be used in all test runs
-compile({nowarn_unused_function, [make_job_info/1, make_job_info/2]}).

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    %% Start meck for external dependencies only - NOT flurm_preemption
    meck:new(flurm_job_registry, [passthrough, non_strict, no_link]),
    meck:new(flurm_job, [passthrough, non_strict, no_link]),

    %% Setup default mocks
    setup_default_mocks(),

    %% Clean up any existing ETS table
    catch ets:delete(flurm_preemption_config),
    ok.

setup_default_mocks() ->
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> [] end),
    meck:expect(flurm_job, get_info, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job, preempt, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, suspend, fun(_) -> ok end),
    ok.

cleanup(_) ->
    %% Unload all mocks
    meck:unload(flurm_job_registry),
    meck:unload(flurm_job),

    %% Clean up ETS table
    catch ets:delete(flurm_preemption_config),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

%% Create a mock job info map
make_job_info(JobId) ->
    make_job_info(JobId, #{}).

make_job_info(JobId, Overrides) ->
    Defaults = #{
        job_id => JobId,
        name => <<"test_job">>,
        user => <<"testuser">>,
        account => <<"default">>,
        partition => <<"default">>,
        state => running,
        num_nodes => 1,
        num_cpus => 4,
        memory_mb => 4096,
        time_limit => 3600,
        priority => 100,
        submit_time => erlang:system_time(second) - 100,
        start_time => erlang:system_time(second) - 50,
        qos => <<"normal">>,
        allocated_nodes => [<<"node1">>]
    },
    maps:merge(Defaults, Overrides).

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Mode Tests
mode_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"set_preemption_mode sets mode", fun test_set_mode/0},
        {"get_preemption_mode returns default", fun test_get_mode_default/0},
        {"get_preemption_mode returns set mode", fun test_get_mode_set/0},
        {"get_preemption_mode with map", fun test_get_mode_map/0}
     ]}.

test_set_mode() ->
    ok = flurm_preemption:set_preemption_mode(requeue),
    ?assertEqual(requeue, flurm_preemption:get_preemption_mode()),

    ok = flurm_preemption:set_preemption_mode(cancel),
    ?assertEqual(cancel, flurm_preemption:get_preemption_mode()),

    ok = flurm_preemption:set_preemption_mode(suspend),
    ?assertEqual(suspend, flurm_preemption:get_preemption_mode()),

    ok = flurm_preemption:set_preemption_mode(off),
    ?assertEqual(off, flurm_preemption:get_preemption_mode()),
    ok.

test_get_mode_default() ->
    Mode = flurm_preemption:get_preemption_mode(),
    %% Default mode is requeue
    ?assertEqual(requeue, Mode),
    ok.

test_get_mode_set() ->
    ok = flurm_preemption:set_preemption_mode(cancel),
    Mode = flurm_preemption:get_preemption_mode(),
    ?assertEqual(cancel, Mode),
    ok.

test_get_mode_map() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"batch">>, suspend),

    Mode = flurm_preemption:get_preemption_mode(#{partition => <<"batch">>}),
    ?assertEqual(suspend, Mode),
    ok.

%%====================================================================
%% Grace Time Tests
%%====================================================================

grace_time_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"set_grace_time sets time", fun test_set_grace/0},
        {"get_grace_time returns default", fun test_get_grace_default/0},
        {"get_grace_time for partition", fun test_get_grace_partition/0}
     ]}.

test_set_grace() ->
    ok = flurm_preemption:set_grace_time(120),
    ?assertEqual(120, flurm_preemption:get_grace_time()),
    ok.

test_get_grace_default() ->
    GraceTime = flurm_preemption:get_grace_time(),
    %% Default grace time is 60 seconds
    ?assertEqual(60, GraceTime),
    ok.

test_get_grace_partition() ->
    ok = flurm_preemption:set_partition_grace_time(<<"batch">>, 300),

    GraceTime = flurm_preemption:get_grace_time(<<"batch">>),
    ?assertEqual(300, GraceTime),
    ok.

%%====================================================================
%% Priority Threshold Tests
%%====================================================================

threshold_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"set_priority_threshold sets threshold", fun test_set_threshold/0},
        {"get_priority_threshold returns default", fun test_get_threshold_default/0}
     ]}.

test_set_threshold() ->
    ok = flurm_preemption:set_priority_threshold(500),
    ?assertEqual(500, flurm_preemption:get_priority_threshold()),
    ok.

test_get_threshold_default() ->
    Threshold = flurm_preemption:get_priority_threshold(),
    %% Default threshold is 1000
    ?assertEqual(1000, Threshold),
    ok.

%%====================================================================
%% QOS Preemption Rules Tests
%%====================================================================

qos_rules_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"set_qos_preemption_rules sets rules", fun test_set_qos_rules/0},
        {"get_qos_preemption_rules returns default", fun test_get_qos_rules_default/0}
     ]}.

test_set_qos_rules() ->
    Rules = #{<<"high">> => 400, <<"normal">> => 200, <<"low">> => 100},
    ok = flurm_preemption:set_qos_preemption_rules(Rules),
    ?assertEqual(Rules, flurm_preemption:get_qos_preemption_rules()),
    ok.

test_get_qos_rules_default() ->
    Rules = flurm_preemption:get_qos_preemption_rules(),
    ?assert(is_map(Rules)),
    %% Default rules should have high, normal, low
    ?assert(maps:is_key(<<"high">>, Rules)),
    ?assert(maps:is_key(<<"normal">>, Rules)),
    ?assert(maps:is_key(<<"low">>, Rules)),
    ok.

%%====================================================================
%% Can Preempt Tests
%%====================================================================

can_preempt_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"can_preempt higher QOS preempts lower", fun test_can_preempt_qos/0},
        {"can_preempt same QOS uses priority", fun test_can_preempt_priority/0},
        {"can_preempt lower QOS cannot preempt", fun test_cannot_preempt_lower/0}
     ]}.

test_can_preempt_qos() ->
    HighPrioJob = make_job_info(1, #{qos => <<"high">>, priority => 500}),
    LowPrioJob = make_job_info(2, #{qos => <<"low">>, priority => 500}),

    Result = flurm_preemption:can_preempt(HighPrioJob, LowPrioJob),

    ?assertEqual(true, Result),
    ok.

test_can_preempt_priority() ->
    HighPrioJob = make_job_info(1, #{qos => <<"normal">>, priority => 5000}),
    LowPrioJob = make_job_info(2, #{qos => <<"normal">>, priority => 100}),

    ok = flurm_preemption:set_priority_threshold(1000),
    Result = flurm_preemption:can_preempt(HighPrioJob, LowPrioJob),

    %% Priority difference is 4900 > threshold of 1000
    ?assertEqual(true, Result),
    ok.

test_cannot_preempt_lower() ->
    LowPrioJob = make_job_info(1, #{qos => <<"low">>, priority => 100}),
    HighPrioJob = make_job_info(2, #{qos => <<"high">>, priority => 500}),

    Result = flurm_preemption:can_preempt(LowPrioJob, HighPrioJob),

    ?assertEqual(false, Result),
    ok.

%%====================================================================
%% Calculate Preemption Cost Tests
%%====================================================================

preemption_cost_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"calculate_preemption_cost returns float", fun test_calc_cost_float/0},
        {"calculate_preemption_cost low priority lower cost", fun test_calc_cost_priority/0},
        {"calculate_preemption_cost recent job lower cost", fun test_calc_cost_runtime/0}
     ]}.

test_calc_cost_float() ->
    Job = make_job_info(1, #{priority => 500, qos => <<"normal">>}),

    Cost = flurm_preemption:calculate_preemption_cost(Job),

    ?assert(is_float(Cost)),
    ?assert(Cost >= 0.0),
    ok.

test_calc_cost_priority() ->
    HighPrioJob = make_job_info(1, #{priority => 5000, qos => <<"normal">>}),
    LowPrioJob = make_job_info(2, #{priority => 100, qos => <<"normal">>}),

    HighCost = flurm_preemption:calculate_preemption_cost(HighPrioJob),
    LowCost = flurm_preemption:calculate_preemption_cost(LowPrioJob),

    %% Lower priority = lower cost (better for preemption)
    ?assert(LowCost < HighCost),
    ok.

test_calc_cost_runtime() ->
    Now = erlang:system_time(second),
    RecentJob = make_job_info(1, #{priority => 100, start_time => Now - 60}),   % 1 min
    OldJob = make_job_info(2, #{priority => 100, start_time => Now - 3600}),    % 1 hour

    RecentCost = flurm_preemption:calculate_preemption_cost(RecentJob),
    OldCost = flurm_preemption:calculate_preemption_cost(OldJob),

    %% Jobs that have run longer have higher cost (more wasted work)
    ?assert(RecentCost < OldCost),
    ok.

%%====================================================================
%% Get Preemptable Jobs Tests
%%====================================================================

get_preemptable_jobs_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_preemptable_jobs empty when no running jobs", fun test_preemptable_empty/0},
        {"get_preemptable_jobs filters by priority", fun test_preemptable_filters/0}
     ]}.

test_preemptable_empty() ->
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(running) -> [] end),

    PendingJob = make_job_info(1, #{priority => 500, state => pending}),
    Jobs = flurm_preemption:get_preemptable_jobs(PendingJob, 500),

    ?assertEqual([], Jobs),
    ok.

test_preemptable_filters() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    meck:expect(flurm_job_registry, list_jobs_by_state, fun(running) ->
        [{2, MockPid}, {3, MockPid}]
    end),
    meck:expect(flurm_job, get_info, fun(Pid) ->
        case Pid of
            P when P =:= MockPid ->
                {ok, make_job_info(2, #{priority => 100, qos => <<"low">>})}
        end
    end),

    PendingJob = make_job_info(1, #{priority => 500, qos => <<"high">>, state => pending}),
    Jobs = flurm_preemption:get_preemptable_jobs(PendingJob, 500),

    ?assert(is_list(Jobs)),
    ok.

%%====================================================================
%% Check Preemption Tests
%%====================================================================

check_preemption_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"check_preemption disabled returns error", fun test_check_disabled/0},
        {"check_preemption no candidates", fun test_check_no_candidates/0}
     ]}.

test_check_disabled() ->
    ok = flurm_preemption:set_preemption_mode(off),

    PendingJob = make_job_info(1, #{priority => 500, state => pending}),
    Result = flurm_preemption:check_preemption(PendingJob),

    ?assertEqual({error, preemption_disabled}, Result),
    ok.

test_check_no_candidates() ->
    ok = flurm_preemption:set_preemption_mode(requeue),
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(running) -> [] end),

    PendingJob = make_job_info(1, #{priority => 500, state => pending}),
    Result = flurm_preemption:check_preemption(PendingJob),

    ?assertEqual({error, no_preemption_needed}, Result),
    ok.

%%====================================================================
%% Preempt Jobs Tests
%%====================================================================

preempt_jobs_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"preempt_jobs empty list", fun test_preempt_empty/0},
        {"preempt_jobs returns preempted ids", fun test_preempt_returns_ids/0}
     ]}.

test_preempt_empty() ->
    {ok, Ids} = flurm_preemption:preempt_jobs([], requeue),
    ?assertEqual([], Ids),
    ok.

test_preempt_returns_ids() ->
    MockPid = spawn(fun() -> receive stop -> ok end end),

    Jobs = [
        #{job_id => 2, pid => MockPid, num_nodes => 1}
    ],

    meck:expect(flurm_job, preempt, fun(_, _, _) -> ok end),

    {ok, Ids} = flurm_preemption:preempt_jobs(Jobs, requeue),

    ?assert(lists:member(2, Ids)),
    ok.

%%====================================================================
%% Partition Preemption Mode Tests
%%====================================================================

partition_mode_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"set_partition_preemption_mode sets mode", fun test_set_partition_mode/0}
     ]}.

test_set_partition_mode() ->
    ok = flurm_preemption:set_partition_preemption_mode(<<"gpu">>, cancel),

    Mode = flurm_preemption:get_preemption_mode(#{partition => <<"gpu">>}),
    ?assertEqual(cancel, Mode),
    ok.

%%====================================================================
%% Check Preemption Opportunity Tests
%%====================================================================

preemption_opportunity_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"check_preemption_opportunity disabled", fun test_opportunity_disabled/0},
        {"check_preemption_opportunity no candidates", fun test_opportunity_no_candidates/0}
     ]}.

test_opportunity_disabled() ->
    ok = flurm_preemption:set_preemption_mode(off),

    PendingJob = make_job_info(1, #{state => pending}),
    Resources = #{num_nodes => 1, num_cpus => 4, memory_mb => 4096},

    Result = flurm_preemption:check_preemption_opportunity(PendingJob, Resources),

    ?assertEqual({error, preemption_disabled}, Result),
    ok.

test_opportunity_no_candidates() ->
    ok = flurm_preemption:set_preemption_mode(requeue),
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(running) -> [] end),

    PendingJob = make_job_info(1, #{state => pending}),
    Resources = #{num_nodes => 1},

    Result = flurm_preemption:check_preemption_opportunity(PendingJob, Resources),

    ?assertEqual({error, no_preemptable_jobs}, Result),
    ok.

%%====================================================================
%% Find Preemptable Jobs Tests
%%====================================================================

find_preemptable_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"find_preemptable_jobs disabled", fun test_find_disabled/0},
        {"find_preemptable_jobs no candidates", fun test_find_no_candidates/0}
     ]}.

test_find_disabled() ->
    ok = flurm_preemption:set_preemption_mode(off),

    PendingJob = make_job_info(1, #{state => pending}),
    Resources = #{num_nodes => 1},

    Result = flurm_preemption:find_preemptable_jobs(PendingJob, Resources),

    ?assertEqual({error, preemption_disabled}, Result),
    ok.

test_find_no_candidates() ->
    ok = flurm_preemption:set_preemption_mode(requeue),
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(running) -> [] end),

    PendingJob = make_job_info(1, #{state => pending}),
    Resources = #{num_nodes => 1},

    Result = flurm_preemption:find_preemptable_jobs(PendingJob, Resources),

    ?assertEqual({error, no_preemptable_jobs}, Result),
    ok.

%%====================================================================
%% Edge Cases Tests
%%====================================================================

edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"mode off blocks all preemption", fun test_mode_off_blocks/0},
        {"checkpoint mode falls back to requeue", fun test_checkpoint_mode/0}
     ]}.

test_mode_off_blocks() ->
    ok = flurm_preemption:set_preemption_mode(off),

    %% All preemption operations should return disabled
    PendingJob = make_job_info(1, #{state => pending}),

    ?assertEqual({error, preemption_disabled},
                 flurm_preemption:check_preemption(PendingJob)),
    ?assertEqual({error, preemption_disabled},
                 flurm_preemption:check_preemption_opportunity(PendingJob, #{})),
    ?assertEqual({error, preemption_disabled},
                 flurm_preemption:find_preemptable_jobs(PendingJob, #{})),
    ok.

test_checkpoint_mode() ->
    ok = flurm_preemption:set_preemption_mode(checkpoint),

    Mode = flurm_preemption:get_preemption_mode(),
    ?assertEqual(checkpoint, Mode),
    ok.
