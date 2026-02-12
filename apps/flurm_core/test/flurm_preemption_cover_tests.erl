%%%-------------------------------------------------------------------
%%% @doc Comprehensive EUnit Coverage Tests for flurm_preemption module
%%%
%%% Uses meck to mock ALL external dependencies so that every line of
%%% flurm_preemption is executed. This includes:
%%% - All config get/set paths (including defaults and partition overrides)
%%% - All preemption modes: off, requeue, cancel, checkpoint, suspend
%%% - Checkpoint failure -> requeue fallback
%%% - Suspend failure -> requeue fallback (invalid_state and generic error)
%%% - handle_preempted_job for each mode, including not_found paths
%%% - execute_preemption with success and all_preemptions_failed
%%% - find_preemptable_jobs with sorting, candidate filtering, sufficient
%%%   and insufficient resource sets
%%% - check_preemption and check_preemption_opportunity end-to-end
%%% - preempt_single_job and preempt_single_job_with_handling code paths
%%% - graceful_preempt including SIGTERM, SIGKILL, timeout, and failure
%%% - do_graceful_preempt with graceful exit, forced kill, SIGKILL fail
%%% - wait_for_job_exit with terminal states and timeout
%%% - calculate_preemption_cost edge cases for start_time handling
%%% - get_qos_preemption_mode with flurm_qos returning a real #qos{}
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_preemption_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Clean up any existing config table
    catch ets:delete(flurm_preemption_config),

    %% Mock all external dependencies
    Mods = [flurm_job_registry, flurm_job, flurm_job_manager, flurm_node_manager,
            flurm_scheduler, flurm_qos, flurm_metrics, flurm_job_dispatcher],
    lists:foreach(fun(M) ->
        catch meck:unload(M),
        meck:new(M, [passthrough, non_strict, no_link])
    end, Mods),

    %% Default mocks
    meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> [] end),
    meck:expect(flurm_job, get_info, fun(_) -> {error, not_found} end),
    meck:expect(flurm_job, preempt, fun(_, _, _) -> ok end),
    meck:expect(flurm_job, suspend, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_, _) -> ok end),
    meck:expect(flurm_job_dispatcher, preempt_job, fun(_, _) -> ok end),
    meck:expect(flurm_scheduler, submit_job, fun(_) -> ok end),
    meck:expect(flurm_scheduler, job_failed, fun(_) -> ok end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),
    ok.

cleanup(_) ->
    Mods = [flurm_job_registry, flurm_job, flurm_job_manager, flurm_node_manager,
            flurm_scheduler, flurm_qos, flurm_metrics, flurm_job_dispatcher],
    lists:foreach(fun(M) -> catch meck:unload(M) end, Mods),
    catch ets:delete(flurm_preemption_config),
    ok.

%%====================================================================
%% Helper to build a #job{} record for flurm_job_manager:get_job
%%====================================================================

make_job_record(JobId, Overrides) ->
    Defaults = #{
        partition => <<"default">>,
        state => running,
        allocated_nodes => [<<"node1">>, <<"node2">>]
    },
    M = maps:merge(Defaults, Overrides),
    #job{
        id = JobId,
        name = <<"test">>,
        user = <<"tester">>,
        partition = maps:get(partition, M),
        state = maps:get(state, M),
        script = <<"#!/bin/bash\necho hello">>,
        num_nodes = 1,
        num_cpus = 4,
        memory_mb = 4096,
        time_limit = 3600,
        priority = 100,
        submit_time = erlang:system_time(second) - 100,
        start_time = erlang:system_time(second) - 50,
        allocated_nodes = maps:get(allocated_nodes, M),
        qos = <<"normal">>
    }.

%%====================================================================
%% Config: ensure_config_table/0
%%====================================================================

ensure_config_table_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"creates table when none exists", fun() ->
            ?assertEqual(undefined, ets:whereis(flurm_preemption_config)),
            flurm_preemption:ensure_config_table(),
            ?assertNotEqual(undefined, ets:whereis(flurm_preemption_config))
        end},
        {"idempotent when table already exists", fun() ->
            flurm_preemption:ensure_config_table(),
            Ref1 = ets:whereis(flurm_preemption_config),
            Result = flurm_preemption:ensure_config_table(),
            ?assertEqual(ok, Result),
            Ref2 = ets:whereis(flurm_preemption_config),
            ?assertEqual(Ref1, Ref2)
        end}
    ]}.

%%====================================================================
%% Config: set/get preemption_mode
%%====================================================================

preemption_mode_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"default mode is requeue", fun() ->
            ?assertEqual(requeue, flurm_preemption:get_preemption_mode())
        end},
        {"set each valid mode", fun() ->
            lists:foreach(fun(Mode) ->
                ok = flurm_preemption:set_preemption_mode(Mode),
                ?assertEqual(Mode, flurm_preemption:get_preemption_mode())
            end, [requeue, cancel, checkpoint, suspend, off])
        end}
    ]}.

%%====================================================================
%% Config: set/get grace_time
%%====================================================================

grace_time_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"default grace time is 60", fun() ->
            ?assertEqual(60, flurm_preemption:get_grace_time())
        end},
        {"set and get grace time", fun() ->
            ok = flurm_preemption:set_grace_time(120),
            ?assertEqual(120, flurm_preemption:get_grace_time())
        end}
    ]}.

%%====================================================================
%% Config: set/get priority_threshold
%%====================================================================

priority_threshold_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"default threshold is 1000", fun() ->
            ?assertEqual(1000, flurm_preemption:get_priority_threshold())
        end},
        {"set and get threshold", fun() ->
            ok = flurm_preemption:set_priority_threshold(500),
            ?assertEqual(500, flurm_preemption:get_priority_threshold())
        end},
        {"set threshold to zero", fun() ->
            ok = flurm_preemption:set_priority_threshold(0),
            ?assertEqual(0, flurm_preemption:get_priority_threshold())
        end}
    ]}.

%%====================================================================
%% Config: set/get qos_preemption_rules
%%====================================================================

qos_rules_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"default rules contain high/normal/low", fun() ->
            Rules = flurm_preemption:get_qos_preemption_rules(),
            ?assertEqual(300, maps:get(<<"high">>, Rules)),
            ?assertEqual(200, maps:get(<<"normal">>, Rules)),
            ?assertEqual(100, maps:get(<<"low">>, Rules))
        end},
        {"set custom rules", fun() ->
            Custom = #{<<"gold">> => 999, <<"silver">> => 500},
            ok = flurm_preemption:set_qos_preemption_rules(Custom),
            ?assertEqual(Custom, flurm_preemption:get_qos_preemption_rules())
        end}
    ]}.

%%====================================================================
%% Config: partition-specific modes
%%====================================================================

partition_mode_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"set and get partition mode", fun() ->
            ok = flurm_preemption:set_partition_preemption_mode(<<"gpu">>, cancel),
            ?assertEqual(cancel, flurm_preemption:get_preemption_mode(#{partition => <<"gpu">>}))
        end},
        {"unconfigured partition falls back to global", fun() ->
            ok = flurm_preemption:set_preemption_mode(suspend),
            ?assertEqual(suspend, flurm_preemption:get_preemption_mode(#{partition => <<"nope">>}))
        end},
        {"set_partition_preemption_mode with no prior partition_modes entry", fun() ->
            %% First set creates the partition_modes key
            ok = flurm_preemption:set_partition_preemption_mode(<<"first">>, requeue),
            ?assertEqual(requeue, flurm_preemption:get_preemption_mode(#{partition => <<"first">>}))
        end},
        {"multiple partitions coexist", fun() ->
            ok = flurm_preemption:set_partition_preemption_mode(<<"a">>, cancel),
            ok = flurm_preemption:set_partition_preemption_mode(<<"b">>, suspend),
            ?assertEqual(cancel, flurm_preemption:get_preemption_mode(#{partition => <<"a">>})),
            ?assertEqual(suspend, flurm_preemption:get_preemption_mode(#{partition => <<"b">>}))
        end}
    ]}.

%%====================================================================
%% Config: partition-specific grace time
%%====================================================================

partition_grace_time_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"set and get partition grace time", fun() ->
            ok = flurm_preemption:set_partition_grace_time(<<"fast">>, 10),
            ?assertEqual(10, flurm_preemption:get_grace_time(<<"fast">>))
        end},
        {"unconfigured partition falls back to global grace time", fun() ->
            ok = flurm_preemption:set_grace_time(90),
            ?assertEqual(90, flurm_preemption:get_grace_time(<<"unknown">>))
        end},
        {"partition_grace_times key absent falls back to global", fun() ->
            %% No partition_grace_times in ETS yet
            ?assertEqual(60, flurm_preemption:get_grace_time(<<"x">>))
        end},
        {"set_partition_grace_time with no prior entry", fun() ->
            ok = flurm_preemption:set_partition_grace_time(<<"new">>, 42),
            ?assertEqual(42, flurm_preemption:get_grace_time(<<"new">>))
        end}
    ]}.

%%====================================================================
%% get_preemption_mode/1 - all clause patterns
%%====================================================================

get_mode_context_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"empty map returns global", fun() ->
            ?assertEqual(requeue, flurm_preemption:get_preemption_mode(#{}))
        end},
        {"atom key returns global", fun() ->
            ?assertEqual(requeue, flurm_preemption:get_preemption_mode(#{foo => bar}))
        end},
        {"partition present, partition configured", fun() ->
            ok = flurm_preemption:set_partition_preemption_mode(<<"p1">>, cancel),
            ?assertEqual(cancel, flurm_preemption:get_preemption_mode(#{partition => <<"p1">>}))
        end},
        {"partition present but not configured, qos absent -> global", fun() ->
            ok = flurm_preemption:set_preemption_mode(suspend),
            ?assertEqual(suspend, flurm_preemption:get_preemption_mode(#{partition => <<"miss">>}))
        end},
        {"partition present but not configured, qos present -> qos lookup", fun() ->
            %% Mock flurm_qos:get_qos to return a qos record with preempt_mode
            meck:expect(flurm_qos, get_qos, fun(<<"special">>) ->
                {ok, #qos{name = <<"special">>, preempt_mode = cancel}}
            end),
            ?assertEqual(cancel, flurm_preemption:get_preemption_mode(
                #{partition => <<"nopart">>, qos => <<"special">>}))
        end},
        {"partition present but not configured, qos present but qos returns off", fun() ->
            meck:expect(flurm_qos, get_qos, fun(<<"offqos">>) ->
                {ok, #qos{name = <<"offqos">>, preempt_mode = off}}
            end),
            ok = flurm_preemption:set_preemption_mode(requeue),
            ?assertEqual(requeue, flurm_preemption:get_preemption_mode(
                #{partition => <<"nopart">>, qos => <<"offqos">>}))
        end},
        {"qos-only key (no partition key)", fun() ->
            meck:expect(flurm_qos, get_qos, fun(<<"highqos">>) ->
                {ok, #qos{name = <<"highqos">>, preempt_mode = suspend}}
            end),
            ?assertEqual(suspend, flurm_preemption:get_preemption_mode(#{qos => <<"highqos">>}))
        end},
        {"qos-only key, flurm_qos errors -> falls to global", fun() ->
            meck:expect(flurm_qos, get_qos, fun(_) -> {error, not_found} end),
            ok = flurm_preemption:set_preemption_mode(cancel),
            ?assertEqual(cancel, flurm_preemption:get_preemption_mode(#{qos => <<"nope">>}))
        end},
        {"qos-only key, flurm_qos crashes -> falls to global", fun() ->
            meck:expect(flurm_qos, get_qos, fun(_) -> error(boom) end),
            ok = flurm_preemption:set_preemption_mode(checkpoint),
            ?assertEqual(checkpoint, flurm_preemption:get_preemption_mode(#{qos => <<"crash">>}))
        end}
    ]}.

%%====================================================================
%% get_qos_preemption_mode/1
%%====================================================================

get_qos_preemption_mode_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"returns mode from qos record when not off", fun() ->
            meck:expect(flurm_qos, get_qos, fun(<<"prem">>) ->
                {ok, #qos{name = <<"prem">>, preempt_mode = requeue}}
            end),
            ?assertEqual(requeue, flurm_preemption:get_qos_preemption_mode(<<"prem">>))
        end},
        {"falls back to global when qos has off", fun() ->
            meck:expect(flurm_qos, get_qos, fun(<<"off">>) ->
                {ok, #qos{name = <<"off">>, preempt_mode = off}}
            end),
            ok = flurm_preemption:set_preemption_mode(cancel),
            ?assertEqual(cancel, flurm_preemption:get_qos_preemption_mode(<<"off">>))
        end},
        {"falls back to global when qos not found", fun() ->
            meck:expect(flurm_qos, get_qos, fun(_) -> {error, not_found} end),
            ok = flurm_preemption:set_preemption_mode(suspend),
            ?assertEqual(suspend, flurm_preemption:get_qos_preemption_mode(<<"missing">>))
        end},
        {"falls back to global when qos crashes", fun() ->
            meck:expect(flurm_qos, get_qos, fun(_) -> error(kaboom) end),
            ok = flurm_preemption:set_preemption_mode(requeue),
            ?assertEqual(requeue, flurm_preemption:get_qos_preemption_mode(<<"bad">>))
        end}
    ]}.

%%====================================================================
%% can_preempt/2
%%====================================================================

can_preempt_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"higher QOS can preempt lower QOS", fun() ->
            High = #{qos => <<"high">>, priority => 100},
            Low = #{qos => <<"low">>, priority => 100},
            ?assert(flurm_preemption:can_preempt(High, Low))
        end},
        {"lower QOS cannot preempt higher QOS", fun() ->
            Low = #{qos => <<"low">>, priority => 9999},
            High = #{qos => <<"high">>, priority => 100},
            ?assertNot(flurm_preemption:can_preempt(Low, High))
        end},
        {"same QOS with priority above threshold can preempt", fun() ->
            Job1 = #{qos => <<"normal">>, priority => 5000},
            Job2 = #{qos => <<"normal">>, priority => 1000},
            %% default threshold 1000: 5000 > 1000 + 1000
            ?assert(flurm_preemption:can_preempt(Job1, Job2))
        end},
        {"same QOS with priority below threshold cannot preempt", fun() ->
            Job1 = #{qos => <<"normal">>, priority => 1500},
            Job2 = #{qos => <<"normal">>, priority => 1000},
            ?assertNot(flurm_preemption:can_preempt(Job1, Job2))
        end},
        {"missing qos defaults to normal", fun() ->
            Job1 = #{priority => 5000},
            Job2 = #{qos => <<"low">>, priority => 100},
            ?assert(flurm_preemption:can_preempt(Job1, Job2))
        end},
        {"missing priority defaults to DEFAULT_PRIORITY (100)", fun() ->
            High = #{qos => <<"high">>},
            Low = #{qos => <<"low">>},
            ?assert(flurm_preemption:can_preempt(High, Low))
        end}
    ]}.

%%====================================================================
%% calculate_preemption_cost/1
%%====================================================================

preemption_cost_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"returns non-negative float", fun() ->
            Cost = flurm_preemption:calculate_preemption_cost(#{priority => 1000}),
            ?assert(is_float(Cost)),
            ?assert(Cost >= 0.0)
        end},
        {"lower priority gives lower cost", fun() ->
            Now = erlang:system_time(second),
            LC = flurm_preemption:calculate_preemption_cost(
                #{priority => 100, qos => <<"normal">>, start_time => Now}),
            HC = flurm_preemption:calculate_preemption_cost(
                #{priority => 9000, qos => <<"normal">>, start_time => Now}),
            ?assert(LC < HC)
        end},
        {"undefined start_time uses RunTimeMinutes=0", fun() ->
            Cost = flurm_preemption:calculate_preemption_cost(
                #{priority => 500, start_time => undefined}),
            ?assert(is_float(Cost))
        end},
        {"non-integer start_time uses RunTimeMinutes=0", fun() ->
            Cost = flurm_preemption:calculate_preemption_cost(
                #{priority => 500, start_time => "bad"}),
            ?assert(is_float(Cost))
        end},
        {"integer start_time computes run time", fun() ->
            Now = erlang:system_time(second),
            Cost = flurm_preemption:calculate_preemption_cost(
                #{priority => 500, start_time => Now - 3600}),
            ?assert(is_float(Cost))
        end},
        {"missing start_time key uses system_time default", fun() ->
            Cost = flurm_preemption:calculate_preemption_cost(#{priority => 500}),
            ?assert(is_float(Cost))
        end},
        {"resource benefit lowers cost", fun() ->
            Now = erlang:system_time(second),
            Small = flurm_preemption:calculate_preemption_cost(
                #{priority => 1000, qos => <<"normal">>, start_time => Now,
                  num_nodes => 1, num_cpus => 1}),
            Big = flurm_preemption:calculate_preemption_cost(
                #{priority => 1000, qos => <<"normal">>, start_time => Now,
                  num_nodes => 100, num_cpus => 1000}),
            ?assert(Big < Small)
        end},
        {"cost max floors at 0.0", fun() ->
            Cost = flurm_preemption:calculate_preemption_cost(
                #{priority => 0, qos => <<"low">>, num_nodes => 10000, num_cpus => 100000,
                  start_time => erlang:system_time(second)}),
            ?assert(Cost >= 0.0)
        end}
    ]}.

%%====================================================================
%% calculate_freed_resources/1
%%====================================================================

freed_resources_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"empty list returns zeros", fun() ->
            ?assertEqual(#{nodes => 0, cpus => 0, memory_mb => 0},
                         flurm_preemption:calculate_freed_resources([]))
        end},
        {"single job sums correctly", fun() ->
            ?assertEqual(#{nodes => 2, cpus => 8, memory_mb => 4096},
                         flurm_preemption:calculate_freed_resources(
                             [#{num_nodes => 2, num_cpus => 8, memory_mb => 4096}]))
        end},
        {"multiple jobs accumulate", fun() ->
            Jobs = [
                #{num_nodes => 1, num_cpus => 4, memory_mb => 2048},
                #{num_nodes => 3, num_cpus => 16, memory_mb => 8192}
            ],
            ?assertEqual(#{nodes => 4, cpus => 20, memory_mb => 10240},
                         flurm_preemption:calculate_freed_resources(Jobs))
        end},
        {"defaults used for missing keys", fun() ->
            ?assertEqual(#{nodes => 1, cpus => 1, memory_mb => 1024},
                         flurm_preemption:calculate_freed_resources([#{}]))
        end}
    ]}.

%%====================================================================
%% find_preemption_set/4 and find_sufficient_set/8
%%====================================================================

find_preemption_set_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"empty candidates returns insufficient error", fun() ->
            ?assertEqual({error, insufficient_preemptable_resources},
                         flurm_preemption:find_preemption_set([], 1, 1, 1024))
        end},
        {"single candidate with sufficient nodes", fun() ->
            C = #{priority => 50, num_nodes => 2, num_cpus => 4, memory_mb => 2048},
            {ok, Jobs} = flurm_preemption:find_preemption_set([C], 1, 2, 1024),
            ?assertEqual(1, length(Jobs))
        end},
        {"multiple candidates accumulated until threshold", fun() ->
            C1 = #{priority => 50, num_nodes => 1, num_cpus => 2, memory_mb => 1024},
            C2 = #{priority => 100, num_nodes => 1, num_cpus => 2, memory_mb => 1024},
            C3 = #{priority => 150, num_nodes => 1, num_cpus => 2, memory_mb => 1024},
            {ok, Jobs} = flurm_preemption:find_preemption_set([C1, C2, C3], 2, 4, 2048),
            ?assert(length(Jobs) >= 2)
        end},
        {"insufficient candidates returns error", fun() ->
            C = #{priority => 50, num_nodes => 1, num_cpus => 2, memory_mb => 512},
            ?assertEqual({error, insufficient_preemptable_resources},
                         flurm_preemption:find_preemption_set([C], 10, 20, 10240))
        end},
        {"candidates sorted by priority (lowest first)", fun() ->
            C1 = #{priority => 200, num_nodes => 2, num_cpus => 4, memory_mb => 2048},
            C2 = #{priority => 50, num_nodes => 2, num_cpus => 4, memory_mb => 2048},
            {ok, Jobs} = flurm_preemption:find_preemption_set([C1, C2], 1, 2, 1024),
            %% First selected should be the lowest priority (50)
            ?assertEqual(1, length(Jobs)),
            [Selected] = Jobs,
            ?assertEqual(50, maps:get(priority, Selected))
        end}
    ]}.

find_sufficient_set_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"already satisfied returns empty accumulator", fun() ->
            Jobs = [#{num_nodes => 1, num_cpus => 2, memory_mb => 1024}],
            {ok, Acc} = flurm_preemption:find_sufficient_set(Jobs, 2, 4, 2048, [], 2, 4, 2048),
            ?assertEqual([], Acc)
        end},
        {"empty list with unmet need returns error", fun() ->
            ?assertEqual({error, insufficient_preemptable_resources},
                         flurm_preemption:find_sufficient_set([], 2, 4, 2048, [], 0, 0, 0))
        end},
        {"accumulates jobs step by step", fun() ->
            J1 = #{num_nodes => 1, num_cpus => 2, memory_mb => 1024},
            J2 = #{num_nodes => 1, num_cpus => 2, memory_mb => 1024},
            {ok, Acc} = flurm_preemption:find_sufficient_set([J1, J2], 2, 4, 2048, [], 0, 0, 0),
            ?assertEqual(2, length(Acc))
        end},
        {"stops early when threshold met", fun() ->
            J1 = #{num_nodes => 3, num_cpus => 8, memory_mb => 4096},
            J2 = #{num_nodes => 1, num_cpus => 2, memory_mb => 1024},
            {ok, Acc} = flurm_preemption:find_sufficient_set([J1, J2], 2, 4, 2048, [], 0, 0, 0),
            ?assertEqual(1, length(Acc))
        end}
    ]}.

%%====================================================================
%% check_preemption/1
%%====================================================================

check_preemption_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"disabled returns preemption_disabled", fun() ->
            ok = flurm_preemption:set_preemption_mode(off),
            ?assertEqual({error, preemption_disabled},
                         flurm_preemption:check_preemption(#{priority => 9000}))
        end},
        {"no candidates returns no_preemption_needed", fun() ->
            ok = flurm_preemption:set_preemption_mode(requeue),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> [] end),
            ?assertEqual({error, no_preemption_needed},
                         flurm_preemption:check_preemption(
                             #{priority => 9000, num_nodes => 1, num_cpus => 1, memory_mb => 1024}))
        end},
        {"finds preemptable set with candidates", fun() ->
            ok = flurm_preemption:set_preemption_mode(requeue),
            ok = flurm_preemption:set_priority_threshold(0),
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) ->
                {ok, #{priority => 50, qos => <<"low">>, num_nodes => 2, num_cpus => 4, memory_mb => 2048}}
            end),
            PendingJob = #{priority => 5000, qos => <<"high">>, num_nodes => 1, num_cpus => 2, memory_mb => 1024},
            {ok, ToPreempt} = flurm_preemption:check_preemption(PendingJob),
            ?assert(length(ToPreempt) > 0),
            Pid ! stop
        end},
        {"insufficient resources returns no_preemption_needed", fun() ->
            ok = flurm_preemption:set_preemption_mode(requeue),
            ok = flurm_preemption:set_priority_threshold(0),
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) ->
                {ok, #{priority => 50, qos => <<"low">>, num_nodes => 1, num_cpus => 1, memory_mb => 512}}
            end),
            PendingJob = #{priority => 5000, qos => <<"high">>, num_nodes => 100, num_cpus => 200, memory_mb => 102400},
            ?assertEqual({error, no_preemption_needed},
                         flurm_preemption:check_preemption(PendingJob)),
            Pid ! stop
        end}
    ]}.

%%====================================================================
%% get_preemptable_jobs/2
%%====================================================================

get_preemptable_jobs_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"no running jobs returns empty", fun() ->
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> [] end),
            ?assertEqual([], flurm_preemption:get_preemptable_jobs(#{priority => 5000, qos => <<"high">>}, 5000))
        end},
        {"filters by priority and can_preempt", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}, {20, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(P) when P =:= Pid ->
                {ok, #{priority => 50, qos => <<"low">>}}
            end),
            Pending = #{priority => 5000, qos => <<"high">>},
            Jobs = flurm_preemption:get_preemptable_jobs(Pending, 5000),
            ?assert(length(Jobs) >= 1),
            Pid ! stop
        end},
        {"skips jobs with higher priority", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) ->
                {ok, #{priority => 9999, qos => <<"high">>}}
            end),
            Pending = #{priority => 100, qos => <<"low">>},
            ?assertEqual([], flurm_preemption:get_preemptable_jobs(Pending, 100)),
            Pid ! stop
        end},
        {"handles get_info error gracefully", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) -> {error, not_found} end),
            ?assertEqual([], flurm_preemption:get_preemptable_jobs(#{priority => 5000, qos => <<"high">>}, 5000)),
            Pid ! stop
        end},
        {"handles get_info crash gracefully", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) -> error(crash) end),
            ?assertEqual([], flurm_preemption:get_preemptable_jobs(#{priority => 5000, qos => <<"high">>}, 5000)),
            Pid ! stop
        end}
    ]}.

%%====================================================================
%% preempt_jobs/2
%%====================================================================

preempt_jobs_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"empty list", fun() ->
            {ok, []} = flurm_preemption:preempt_jobs([], requeue)
        end},
        {"job with undefined pid fails", fun() ->
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 1, pid => undefined}], requeue),
            ?assertEqual([], Ids)
        end},
        {"job with no pid key defaults to undefined -> fails", fun() ->
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 1}], requeue),
            ?assertEqual([], Ids)
        end},
        {"job with real pid succeeds in requeue mode", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, requeue, _) -> ok end),
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 42, pid => Pid}], requeue),
            ?assertEqual([42], Ids),
            Pid ! stop
        end},
        {"job with real pid succeeds in cancel mode", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, cancel, _) -> ok end),
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 43, pid => Pid}], cancel),
            ?assertEqual([43], Ids),
            Pid ! stop
        end},
        {"job with real pid in checkpoint mode - success", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, checkpoint, _) -> ok end),
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 44, pid => Pid}], checkpoint),
            ?assertEqual([44], Ids),
            Pid ! stop
        end},
        {"job with real pid in checkpoint mode - failure falls back to requeue", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(P, checkpoint, _) when P =:= Pid ->
                {error, checkpoint_not_supported};
                                               (_, requeue, _) -> ok end),
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 45, pid => Pid}], checkpoint),
            ?assertEqual([45], Ids),
            Pid ! stop
        end},
        {"job with real pid in suspend mode - success", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, suspend, fun(_) -> ok end),
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 46, pid => Pid}], suspend),
            ?assertEqual([46], Ids),
            Pid ! stop
        end},
        {"job with real pid in suspend mode - failure", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, suspend, fun(_) -> {error, not_running} end),
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 47, pid => Pid}], suspend),
            ?assertEqual([], Ids),
            Pid ! stop
        end},
        {"job with off mode returns disabled", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 48, pid => Pid}], off),
            ?assertEqual([], Ids),
            Pid ! stop
        end},
        {"preempt requeue failure path", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, requeue, _) -> {error, timeout} end),
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 49, pid => Pid}], requeue),
            ?assertEqual([], Ids),
            Pid ! stop
        end},
        {"preempt cancel failure path", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, cancel, _) -> {error, gone} end),
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 50, pid => Pid}], cancel),
            ?assertEqual([], Ids),
            Pid ! stop
        end},
        {"checkpoint fail then requeue also fails", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, _, _) -> {error, fail} end),
            {ok, Ids} = flurm_preemption:preempt_jobs([#{job_id => 51, pid => Pid}], checkpoint),
            ?assertEqual([], Ids),
            Pid ! stop
        end}
    ]}.

%%====================================================================
%% preempt_single_job/4 (TEST export)
%%====================================================================

preempt_single_job_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"undefined pid returns job_not_found", fun() ->
            ?assertEqual({error, job_not_found},
                         flurm_preemption:preempt_single_job(1, undefined, requeue, 60))
        end},
        {"requeue with pid calls flurm_job:preempt", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, requeue, _) -> ok end),
            ?assertEqual({ok, 1}, flurm_preemption:preempt_single_job(1, Pid, requeue, 60)),
            Pid ! stop
        end},
        {"cancel with pid", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, cancel, _) -> ok end),
            ?assertEqual({ok, 2}, flurm_preemption:preempt_single_job(2, Pid, cancel, 60)),
            Pid ! stop
        end},
        {"checkpoint success", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, checkpoint, _) -> ok end),
            ?assertEqual({ok, 3}, flurm_preemption:preempt_single_job(3, Pid, checkpoint, 60)),
            Pid ! stop
        end},
        {"checkpoint failure falls back to requeue", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun
                (_, checkpoint, _) -> {error, not_supported};
                (_, requeue, _) -> ok
            end),
            ?assertEqual({ok, 4}, flurm_preemption:preempt_single_job(4, Pid, checkpoint, 60)),
            Pid ! stop
        end},
        {"suspend success", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, suspend, fun(_) -> ok end),
            ?assertEqual({ok, 5}, flurm_preemption:preempt_single_job(5, Pid, suspend, 60)),
            Pid ! stop
        end},
        {"suspend failure returns error", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, suspend, fun(_) -> {error, not_running} end),
            ?assertEqual({error, not_running}, flurm_preemption:preempt_single_job(6, Pid, suspend, 60)),
            Pid ! stop
        end},
        {"off mode returns disabled", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            ?assertEqual({error, preemption_disabled},
                         flurm_preemption:preempt_single_job(7, Pid, off, 60)),
            Pid ! stop
        end}
    ]}.

%%====================================================================
%% preempt_single_job_with_handling/4 (TEST export)
%%====================================================================

preempt_single_job_with_handling_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"undefined pid - direct handling via handle_preempted_job", fun() ->
            Job = #{job_id => 1},
            %% handle_preempted_job(1, off) returns {error, preemption_disabled}
            Result = flurm_preemption:preempt_single_job_with_handling(1, Job, off, 60),
            ?assertEqual({error, preemption_disabled}, Result)
        end},
        {"pid present - preempt succeeds, handle succeeds", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            Job = #{job_id => 2, pid => Pid},
            meck:expect(flurm_job, preempt, fun(_, _, _) -> ok end),
            %% handle_preempted_job(2, off) -> {error, preemption_disabled}
            Result = flurm_preemption:preempt_single_job_with_handling(2, Job, off, 60),
            ?assertEqual({error, preemption_disabled}, Result),
            Pid ! stop
        end},
        {"pid present - preempt returns error, falls back to handle", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            Job = #{job_id => 3, pid => Pid},
            meck:expect(flurm_job, preempt, fun(_, _, _) -> {error, oops} end),
            Result = flurm_preemption:preempt_single_job_with_handling(3, Job, off, 60),
            ?assertEqual({error, preemption_disabled}, Result),
            Pid ! stop
        end},
        {"pid present - preempt crashes (EXIT), falls back to handle", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            Job = #{job_id => 4, pid => Pid},
            meck:expect(flurm_job, preempt, fun(_, _, _) -> error(boom) end),
            Result = flurm_preemption:preempt_single_job_with_handling(4, Job, off, 60),
            ?assertEqual({error, preemption_disabled}, Result),
            Pid ! stop
        end},
        {"requeue mode with real job_manager mock - success", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            Job = #{job_id => 5, pid => Pid},
            meck:expect(flurm_job, preempt, fun(_, _, _) -> ok end),
            meck:expect(flurm_job_manager, get_job, fun(5) ->
                {ok, make_job_record(5, #{})}
            end),
            meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
            Result = flurm_preemption:preempt_single_job_with_handling(5, Job, requeue, 60),
            ?assertEqual({ok, 5}, Result),
            Pid ! stop
        end},
        {"cancel mode with real job_manager mock - success", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            Job = #{job_id => 6, pid => Pid},
            meck:expect(flurm_job, preempt, fun(_, _, _) -> ok end),
            meck:expect(flurm_job_manager, get_job, fun(6) ->
                {ok, make_job_record(6, #{})}
            end),
            meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
            Result = flurm_preemption:preempt_single_job_with_handling(6, Job, cancel, 60),
            ?assertEqual({ok, 6}, Result),
            Pid ! stop
        end}
    ]}.

%%====================================================================
%% handle_preempted_job/2
%%====================================================================

handle_preempted_job_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"off mode returns preemption_disabled", fun() ->
            ?assertEqual({error, preemption_disabled},
                         flurm_preemption:handle_preempted_job(999, off))
        end},
        {"requeue mode - job found, releases resources and requeues", fun() ->
            Rec = make_job_record(10, #{allocated_nodes => [<<"n1">>, <<"n2">>]}),
            meck:expect(flurm_job_manager, get_job, fun(10) -> {ok, Rec} end),
            meck:expect(flurm_job_manager, update_job, fun(10, _) -> ok end),
            ?assertEqual(ok, flurm_preemption:handle_preempted_job(10, requeue)),
            %% Verify release_resources was called for each node
            ?assert(meck:called(flurm_node_manager, release_resources, [<<"n1">>, 10])),
            ?assert(meck:called(flurm_node_manager, release_resources, [<<"n2">>, 10])),
            ?assert(meck:called(flurm_scheduler, submit_job, [10]))
        end},
        {"requeue mode - job not found", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),
            ?assertEqual({error, job_not_found},
                         flurm_preemption:handle_preempted_job(11, requeue))
        end},
        {"cancel mode - job found, releases resources and cancels", fun() ->
            Rec = make_job_record(20, #{allocated_nodes => [<<"n1">>]}),
            meck:expect(flurm_job_manager, get_job, fun(20) -> {ok, Rec} end),
            meck:expect(flurm_job_manager, update_job, fun(20, _) -> ok end),
            ?assertEqual(ok, flurm_preemption:handle_preempted_job(20, cancel)),
            ?assert(meck:called(flurm_scheduler, job_failed, [20]))
        end},
        {"cancel mode - job not found", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),
            ?assertEqual({error, job_not_found},
                         flurm_preemption:handle_preempted_job(21, cancel))
        end},
        {"checkpoint mode delegates to requeue", fun() ->
            Rec = make_job_record(30, #{allocated_nodes => []}),
            meck:expect(flurm_job_manager, get_job, fun(30) -> {ok, Rec} end),
            meck:expect(flurm_job_manager, update_job, fun(30, _) -> ok end),
            ?assertEqual(ok, flurm_preemption:handle_preempted_job(30, checkpoint))
        end},
        {"suspend mode - success", fun() ->
            meck:expect(flurm_job_manager, suspend_job, fun(40) -> ok end),
            ?assertEqual(ok, flurm_preemption:handle_preempted_job(40, suspend))
        end},
        {"suspend mode - invalid_state falls back to requeue", fun() ->
            meck:expect(flurm_job_manager, suspend_job, fun(41) ->
                {error, {invalid_state, completed}}
            end),
            %% Falls back to handle_preempted_job(41, requeue)
            Rec = make_job_record(41, #{allocated_nodes => []}),
            meck:expect(flurm_job_manager, get_job, fun(41) -> {ok, Rec} end),
            meck:expect(flurm_job_manager, update_job, fun(41, _) -> ok end),
            ?assertEqual(ok, flurm_preemption:handle_preempted_job(41, suspend))
        end},
        {"suspend mode - not_found error", fun() ->
            meck:expect(flurm_job_manager, suspend_job, fun(42) ->
                {error, not_found}
            end),
            ?assertEqual({error, job_not_found},
                         flurm_preemption:handle_preempted_job(42, suspend))
        end},
        {"suspend mode - generic error", fun() ->
            meck:expect(flurm_job_manager, suspend_job, fun(43) ->
                {error, some_reason}
            end),
            ?assertEqual({error, some_reason},
                         flurm_preemption:handle_preempted_job(43, suspend))
        end}
    ]}.

%%====================================================================
%% find_preemptable_jobs/2 (scheduler integration)
%%====================================================================

find_preemptable_jobs_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"disabled mode returns preemption_disabled", fun() ->
            ok = flurm_preemption:set_preemption_mode(off),
            ?assertEqual({error, preemption_disabled},
                         flurm_preemption:find_preemptable_jobs(
                             #{partition => <<"default">>, qos => <<"high">>, priority => 9000},
                             #{num_nodes => 1}))
        end},
        {"no candidates returns no_preemptable_jobs", fun() ->
            ok = flurm_preemption:set_preemption_mode(requeue),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> [] end),
            ?assertEqual({error, no_preemptable_jobs},
                         flurm_preemption:find_preemptable_jobs(
                             #{partition => <<"default">>, qos => <<"high">>, priority => 9000},
                             #{num_nodes => 1}))
        end},
        {"finds sufficient set from candidates", fun() ->
            ok = flurm_preemption:set_preemption_mode(requeue),
            ok = flurm_preemption:set_priority_threshold(0),
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) ->
                {ok, #{priority => 50, qos => <<"low">>, num_nodes => 2, num_cpus => 4, memory_mb => 2048}}
            end),
            {ok, Jobs} = flurm_preemption:find_preemptable_jobs(
                #{partition => <<"default">>, qos => <<"high">>, priority => 9000},
                #{num_nodes => 1, num_cpus => 2, memory_mb => 1024}),
            ?assert(length(Jobs) > 0),
            Pid ! stop
        end},
        {"insufficient resources returns error", fun() ->
            ok = flurm_preemption:set_preemption_mode(requeue),
            ok = flurm_preemption:set_priority_threshold(0),
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) ->
                {ok, #{priority => 50, qos => <<"low">>, num_nodes => 1, num_cpus => 1, memory_mb => 512}}
            end),
            ?assertEqual({error, insufficient_preemptable_resources},
                         flurm_preemption:find_preemptable_jobs(
                             #{partition => <<"default">>, qos => <<"high">>, priority => 9000},
                             #{num_nodes => 100, num_cpus => 200, memory_mb => 102400})),
            Pid ! stop
        end},
        {"uses resource fields from PendingJob when not in ResourcesNeeded", fun() ->
            ok = flurm_preemption:set_preemption_mode(requeue),
            ok = flurm_preemption:set_priority_threshold(0),
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) ->
                {ok, #{priority => 50, qos => <<"low">>, num_nodes => 5, num_cpus => 20, memory_mb => 8192}}
            end),
            %% ResourcesNeeded is empty; values come from PendingJob
            {ok, Jobs} = flurm_preemption:find_preemptable_jobs(
                #{partition => <<"default">>, qos => <<"high">>, priority => 9000,
                  num_nodes => 1, num_cpus => 2, memory_mb => 1024},
                #{}),
            ?assert(length(Jobs) > 0),
            Pid ! stop
        end},
        {"empty preemption set returns insufficient_preemptable_resources", fun() ->
            %% This covers the {ok, []} branch from find_preemption_set
            ok = flurm_preemption:set_preemption_mode(requeue),
            ok = flurm_preemption:set_priority_threshold(0),
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) ->
                {ok, #{priority => 50, qos => <<"low">>, num_nodes => 1, num_cpus => 1, memory_mb => 512}}
            end),
            %% Need 10 nodes, only 1 available => {error, insufficient_preemptable_resources}
            %% That translates via the {error, _} = Err branch
            ?assertEqual({error, insufficient_preemptable_resources},
                         flurm_preemption:find_preemptable_jobs(
                             #{partition => <<"default">>, qos => <<"high">>, priority => 9000},
                             #{num_nodes => 10})),
            Pid ! stop
        end}
    ]}.

%%====================================================================
%% check_preemption_opportunity/2
%%====================================================================

check_preemption_opportunity_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"disabled returns preemption_disabled", fun() ->
            ok = flurm_preemption:set_preemption_mode(off),
            ?assertEqual({error, preemption_disabled},
                         flurm_preemption:check_preemption_opportunity(#{priority => 9000}, #{}))
        end},
        {"no candidates returns no_preemptable_jobs", fun() ->
            ok = flurm_preemption:set_preemption_mode(requeue),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) -> [] end),
            ?assertEqual({error, no_preemptable_jobs},
                         flurm_preemption:check_preemption_opportunity(
                             #{priority => 9000, num_nodes => 1, num_cpus => 1, memory_mb => 1024},
                             #{num_nodes => 1}))
        end},
        {"success returns plan with resources_freed", fun() ->
            ok = flurm_preemption:set_preemption_mode(cancel),
            ok = flurm_preemption:set_priority_threshold(0),
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) ->
                {ok, #{priority => 50, qos => <<"low">>, num_nodes => 2, num_cpus => 8, memory_mb => 4096}}
            end),
            {ok, Plan} = flurm_preemption:check_preemption_opportunity(
                #{priority => 9000, qos => <<"high">>, num_nodes => 1, num_cpus => 4, memory_mb => 2048},
                #{num_nodes => 1, num_cpus => 4, memory_mb => 2048}),
            ?assert(is_map(Plan)),
            ?assert(maps:is_key(jobs_to_preempt, Plan)),
            ?assert(maps:is_key(resources_freed, Plan)),
            ?assert(maps:is_key(preemption_mode, Plan)),
            ?assert(maps:is_key(grace_time, Plan)),
            Pid ! stop
        end},
        {"insufficient resources returns error", fun() ->
            ok = flurm_preemption:set_preemption_mode(requeue),
            ok = flurm_preemption:set_priority_threshold(0),
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) ->
                {ok, #{priority => 50, qos => <<"low">>, num_nodes => 1, num_cpus => 1, memory_mb => 512}}
            end),
            ?assertEqual({error, insufficient_preemptable_resources},
                         flurm_preemption:check_preemption_opportunity(
                             #{priority => 9000, qos => <<"high">>},
                             #{num_nodes => 100, num_cpus => 200, memory_mb => 102400})),
            Pid ! stop
        end},
        {"uses PendingJob fields when ResourcesNeeded is missing them", fun() ->
            ok = flurm_preemption:set_preemption_mode(requeue),
            ok = flurm_preemption:set_priority_threshold(0),
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job_registry, list_jobs_by_state, fun(_) ->
                [{10, Pid}]
            end),
            meck:expect(flurm_job, get_info, fun(_) ->
                {ok, #{priority => 50, qos => <<"low">>, num_nodes => 5, num_cpus => 20, memory_mb => 8192}}
            end),
            {ok, _Plan} = flurm_preemption:check_preemption_opportunity(
                #{priority => 9000, qos => <<"high">>, num_nodes => 1, num_cpus => 4, memory_mb => 2048},
                #{}),
            Pid ! stop
        end}
    ]}.

%%====================================================================
%% execute_preemption/2
%%====================================================================

execute_preemption_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"empty job list returns all_preemptions_failed", fun() ->
            Plan = #{jobs_to_preempt => []},
            ?assertEqual({error, all_preemptions_failed},
                         flurm_preemption:execute_preemption(Plan, #{}))
        end},
        {"successful preemption of job with no pid (uses handle)", fun() ->
            %% Job without pid: preempt_single_job_with_handling calls handle_preempted_job directly
            meck:expect(flurm_job_manager, get_job, fun(1) ->
                {ok, make_job_record(1, #{allocated_nodes => []})}
            end),
            meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
            Plan = #{jobs_to_preempt => [#{job_id => 1}], preemption_mode => requeue, grace_time => 30},
            {ok, Ids} = flurm_preemption:execute_preemption(Plan, #{}),
            ?assertEqual([1], Ids)
        end},
        {"plan without preemption_mode uses global", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(2) ->
                {ok, make_job_record(2, #{allocated_nodes => []})}
            end),
            meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
            Plan = #{jobs_to_preempt => [#{job_id => 2}]},
            {ok, Ids} = flurm_preemption:execute_preemption(Plan, #{}),
            ?assertEqual([2], Ids)
        end},
        {"plan without grace_time uses global", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(3) ->
                {ok, make_job_record(3, #{allocated_nodes => []})}
            end),
            meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
            Plan = #{jobs_to_preempt => [#{job_id => 3}], preemption_mode => requeue},
            {ok, Ids} = flurm_preemption:execute_preemption(Plan, #{}),
            ?assertEqual([3], Ids)
        end},
        {"all preemptions fail returns error", fun() ->
            %% Job with pid but preempt fails, and handle also fails
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, _, _) -> {error, oops} end),
            %% handle_preempted_job(4, off) -> {error, preemption_disabled}
            Plan = #{jobs_to_preempt => [#{job_id => 4, pid => Pid}], preemption_mode => off, grace_time => 1},
            ?assertEqual({error, all_preemptions_failed},
                         flurm_preemption:execute_preemption(Plan, #{})),
            Pid ! stop
        end},
        {"metrics increment is called", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(5) ->
                {ok, make_job_record(5, #{allocated_nodes => []})}
            end),
            meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
            Plan = #{jobs_to_preempt => [#{job_id => 5}], preemption_mode => requeue, grace_time => 60},
            {ok, _} = flurm_preemption:execute_preemption(Plan, #{}),
            ?assert(meck:called(flurm_metrics, increment, [flurm_preemptions_total, 1]))
        end}
    ]}.

%%====================================================================
%% graceful_preempt/3
%%====================================================================

graceful_preempt_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"graceful preempt with job found uses partition grace time", fun() ->
            ok = flurm_preemption:set_partition_grace_time(<<"test_part">>, 1),
            Rec = make_job_record(100, #{partition => <<"test_part">>}),
            meck:expect(flurm_job_manager, get_job, fun(100) -> {ok, Rec} end),
            meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
            meck:expect(flurm_job_dispatcher, preempt_job, fun(_, _) -> ok end),
            %% wait_for_job_exit: make job appear completed quickly
            meck:expect(flurm_job_manager, get_job, fun(100) ->
                {ok, Rec#job{state = completed}}
            end),
            Result = flurm_preemption:graceful_preempt(100, [<<"n1">>], requeue),
            ?assertMatch({ok, 100}, Result)
        end},
        {"graceful preempt with job not found uses global grace time", fun() ->
            ok = flurm_preemption:set_grace_time(1),
            meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),
            meck:expect(flurm_job_dispatcher, preempt_job, fun(_, _) -> ok end),
            %% wait_for_job_exit: job not found -> true (exited)
            Result = flurm_preemption:graceful_preempt(101, [], requeue),
            ?assertMatch({ok, 101}, Result)
        end}
    ]}.

%%====================================================================
%% do_graceful_preempt/4 (TEST export)
%%====================================================================

do_graceful_preempt_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"sigterm succeeds, job exits gracefully", fun() ->
            Rec = make_job_record(200, #{allocated_nodes => []}),
            meck:expect(flurm_job_dispatcher, preempt_job, fun(_, _) -> ok end),
            %% wait_for_job_exit sees completed state
            meck:expect(flurm_job_manager, get_job, fun(200) ->
                {ok, Rec#job{state = completed}}
            end),
            meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
            Result = flurm_preemption:do_graceful_preempt(200, [<<"n1">>], requeue, 1),
            ?assertMatch({ok, 200}, Result)
        end},
        {"sigterm succeeds, job does not exit, sigkill succeeds", fun() ->
            Rec = make_job_record(201, #{allocated_nodes => []}),
            CallCount = counters:new(1, [atomics]),
            meck:expect(flurm_job_dispatcher, preempt_job, fun(_, _) -> ok end),
            %% First call during wait: still running. After sigkill: completed.
            meck:expect(flurm_job_manager, get_job, fun(201) ->
                C = counters:get(CallCount, 1),
                counters:add(CallCount, 1, 1),
                if C < 3 ->
                    {ok, Rec#job{state = running}};
                true ->
                    {ok, Rec#job{state = completed}}
                end
            end),
            meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
            %% Use grace time of 1 second so wait_for_job_exit times out quickly
            Result = flurm_preemption:do_graceful_preempt(201, [<<"n1">>], requeue, 1),
            ?assertMatch({ok, 201}, Result)
        end},
        {"sigterm succeeds, job does not exit, sigkill fails", fun() ->
            Rec = make_job_record(202, #{allocated_nodes => []}),
            SigCount = counters:new(1, [atomics]),
            meck:expect(flurm_job_dispatcher, preempt_job, fun(_, #{signal := Sig}) ->
                case Sig of
                    sigterm ->
                        ok;
                    sigkill ->
                        counters:add(SigCount, 1, 1),
                        {error, no_such_process}
                end
            end),
            meck:expect(flurm_job_manager, get_job, fun(202) ->
                {ok, Rec#job{state = running}}
            end),
            meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
            Result = flurm_preemption:do_graceful_preempt(202, [<<"n1">>], requeue, 1),
            %% Still returns {ok, 202} because it handles the preempted job anyway
            ?assertMatch({ok, 202}, Result)
        end},
        {"sigterm fails returns error", fun() ->
            meck:expect(flurm_job_dispatcher, preempt_job, fun(_, _) -> {error, no_such_job} end),
            Result = flurm_preemption:do_graceful_preempt(203, [<<"n1">>], requeue, 1),
            ?assertEqual({error, no_such_job}, Result)
        end}
    ]}.

%%====================================================================
%% wait_for_job_exit/3 (TEST export)
%%====================================================================

wait_for_job_exit_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"timeout returns false", fun() ->
            ?assertEqual(false, flurm_preemption:wait_for_job_exit(300, 0, 100))
        end},
        {"negative time remaining returns false", fun() ->
            ?assertEqual(false, flurm_preemption:wait_for_job_exit(301, -1, 100))
        end},
        {"completed state returns true", fun() ->
            Rec = make_job_record(302, #{}),
            meck:expect(flurm_job_manager, get_job, fun(302) ->
                {ok, Rec#job{state = completed}}
            end),
            ?assertEqual(true, flurm_preemption:wait_for_job_exit(302, 2000, 100))
        end},
        {"failed state returns true", fun() ->
            Rec = make_job_record(303, #{}),
            meck:expect(flurm_job_manager, get_job, fun(303) ->
                {ok, Rec#job{state = failed}}
            end),
            ?assertEqual(true, flurm_preemption:wait_for_job_exit(303, 2000, 100))
        end},
        {"cancelled state returns true", fun() ->
            Rec = make_job_record(304, #{}),
            meck:expect(flurm_job_manager, get_job, fun(304) ->
                {ok, Rec#job{state = cancelled}}
            end),
            ?assertEqual(true, flurm_preemption:wait_for_job_exit(304, 2000, 100))
        end},
        {"not_found returns true (job gone)", fun() ->
            meck:expect(flurm_job_manager, get_job, fun(305) -> {error, not_found} end),
            ?assertEqual(true, flurm_preemption:wait_for_job_exit(305, 2000, 100))
        end},
        {"running state loops until timeout", fun() ->
            Rec = make_job_record(306, #{}),
            meck:expect(flurm_job_manager, get_job, fun(306) ->
                {ok, Rec#job{state = running}}
            end),
            %% With 200ms remaining and 100ms check interval, will loop twice then false
            ?assertEqual(false, flurm_preemption:wait_for_job_exit(306, 200, 100))
        end}
    ]}.

%%====================================================================
%% preempt_requeue/3 (TEST export)
%%====================================================================

preempt_requeue_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"success returns ok with job_id", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, requeue, _) -> ok end),
            ?assertEqual({ok, 1}, flurm_preemption:preempt_requeue(1, Pid, 60)),
            Pid ! stop
        end},
        {"failure returns error", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, requeue, _) -> {error, timeout} end),
            ?assertEqual({error, timeout}, flurm_preemption:preempt_requeue(2, Pid, 60)),
            Pid ! stop
        end}
    ]}.

%%====================================================================
%% preempt_cancel/3 (TEST export)
%%====================================================================

preempt_cancel_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"success returns ok with job_id", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, cancel, _) -> ok end),
            ?assertEqual({ok, 10}, flurm_preemption:preempt_cancel(10, Pid, 60)),
            Pid ! stop
        end},
        {"failure returns error", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, cancel, _) -> {error, already_dead} end),
            ?assertEqual({error, already_dead}, flurm_preemption:preempt_cancel(11, Pid, 60)),
            Pid ! stop
        end}
    ]}.

%%====================================================================
%% preempt_checkpoint/3 (TEST export)
%%====================================================================

preempt_checkpoint_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"success returns ok with job_id", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, checkpoint, _) -> ok end),
            ?assertEqual({ok, 20}, flurm_preemption:preempt_checkpoint(20, Pid, 60)),
            Pid ! stop
        end},
        {"failure falls back to preempt_requeue", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun
                (_, checkpoint, _) -> {error, not_supported};
                (_, requeue, _) -> ok
            end),
            ?assertEqual({ok, 21}, flurm_preemption:preempt_checkpoint(21, Pid, 60)),
            Pid ! stop
        end},
        {"failure falls back to requeue which also fails", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, preempt, fun(_, _, _) -> {error, nope} end),
            ?assertEqual({error, nope}, flurm_preemption:preempt_checkpoint(22, Pid, 60)),
            Pid ! stop
        end}
    ]}.

%%====================================================================
%% preempt_suspend/2 (TEST export)
%%====================================================================

preempt_suspend_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        {"success returns ok with job_id", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, suspend, fun(_) -> ok end),
            ?assertEqual({ok, 30}, flurm_preemption:preempt_suspend(30, Pid)),
            Pid ! stop
        end},
        {"failure returns error", fun() ->
            Pid = spawn(fun() -> receive stop -> ok end end),
            meck:expect(flurm_job, suspend, fun(_) -> {error, cannot_suspend} end),
            ?assertEqual({error, cannot_suspend}, flurm_preemption:preempt_suspend(31, Pid)),
            Pid ! stop
        end}
    ]}.
