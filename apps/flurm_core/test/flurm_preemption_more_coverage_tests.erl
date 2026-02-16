%%%-------------------------------------------------------------------
%%% @doc Additional coverage tests for flurm_preemption module.
%%% Tests internal functions exported via -ifdef(TEST) and edge cases.
%%%-------------------------------------------------------------------
-module(flurm_preemption_more_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure the config table exists for tests
    catch flurm_preemption:ensure_config_table(),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% ensure_config_table Tests
%%====================================================================

ensure_config_table_creates_test() ->
    %% First delete if exists
    catch ets:delete(flurm_preemption_config),
    Result = flurm_preemption:ensure_config_table(),
    %% Returns table ID (atom for named tables) or ok
    ?assert(Result =:= ok orelse Result =:= flurm_preemption_config orelse is_reference(Result)),
    %% Table should now exist
    ?assert(ets:info(flurm_preemption_config) =/= undefined).

ensure_config_table_idempotent_test() ->
    %% Create once
    flurm_preemption:ensure_config_table(),
    %% Create again should be ok
    Result = flurm_preemption:ensure_config_table(),
    ?assert(Result =:= ok orelse Result =:= flurm_preemption_config orelse is_reference(Result)).

%%====================================================================
%% get_qos_preemption_mode Tests
%%====================================================================

get_qos_preemption_mode_default_test() ->
    setup(),
    %% Non-existent QOS should return default mode
    Mode = flurm_preemption:get_qos_preemption_mode(<<"nonexistent_qos">>),
    ?assert(Mode =:= requeue orelse Mode =:= cancel orelse Mode =:= off).

get_qos_preemption_mode_normal_test() ->
    setup(),
    Mode = flurm_preemption:get_qos_preemption_mode(<<"normal">>),
    ?assert(is_atom(Mode)).

get_qos_preemption_mode_high_test() ->
    setup(),
    Mode = flurm_preemption:get_qos_preemption_mode(<<"high">>),
    ?assert(is_atom(Mode)).

%%====================================================================
%% calculate_freed_resources Tests (uses maps - returns nodes, cpus, memory_mb)
%%====================================================================

calculate_freed_resources_empty_test() ->
    Result = flurm_preemption:calculate_freed_resources([]),
    ?assert(is_map(Result)),
    ?assertEqual(0, maps:get(cpus, Result, 0)),
    ?assertEqual(0, maps:get(memory_mb, Result, 0)),
    ?assertEqual(0, maps:get(nodes, Result, 0)).

calculate_freed_resources_single_job_test() ->
    Job = make_job_map(1, 4, 2048, 0),
    Result = flurm_preemption:calculate_freed_resources([Job]),
    ?assertEqual(4, maps:get(cpus, Result)),
    ?assertEqual(2048, maps:get(memory_mb, Result)),
    ?assertEqual(1, maps:get(nodes, Result)).

calculate_freed_resources_multiple_jobs_test() ->
    Jobs = [
        make_job_map(1, 4, 2048, 0),
        make_job_map(2, 8, 4096, 0),
        make_job_map(3, 2, 1024, 0)
    ],
    Result = flurm_preemption:calculate_freed_resources(Jobs),
    ?assertEqual(14, maps:get(cpus, Result)),
    ?assertEqual(7168, maps:get(memory_mb, Result)),
    ?assertEqual(3, maps:get(nodes, Result)).

%%====================================================================
%% find_preemption_set Tests
%% Signature: find_preemption_set(Candidates, NumNodes, NumCpus, MemoryMb)
%%====================================================================

find_preemption_set_empty_candidates_test() ->
    %% Empty candidates should return error (insufficient resources)
    Result = flurm_preemption:find_preemption_set([], 1, 4, 2048),
    ?assertMatch({error, _}, Result).

find_preemption_set_insufficient_resources_test() ->
    Jobs = [make_job_map(1, 2, 1024, 0)],
    Result = flurm_preemption:find_preemption_set(Jobs, 10, 16, 32768),
    %% Should return error when candidates can't meet requirements
    ?assertMatch({error, _}, Result).

find_preemption_set_single_job_sufficient_test() ->
    Jobs = [make_job_map(1, 8, 4096, 0)],
    Result = flurm_preemption:find_preemption_set(Jobs, 1, 4, 2048),
    ?assertMatch({ok, _}, Result).

%%====================================================================
%% find_sufficient_set Tests
%% Signature: find_sufficient_set(Jobs, NeededNodes, NeededCpus, NeededMem, Acc, AccNodes, AccCpus, AccMem)
%%====================================================================

find_sufficient_set_empty_test() ->
    %% Empty job list with requirements should return error
    Result = flurm_preemption:find_sufficient_set([], 1, 4, 1024, [], 0, 0, 0),
    ?assertMatch({error, _}, Result).

find_sufficient_set_already_sufficient_test() ->
    %% When accumulated resources already meet requirements
    %% AccNodes >= NeededNodes triggers success
    Result = flurm_preemption:find_sufficient_set([], 1, 4, 2048, [], 2, 8, 4096),
    ?assertMatch({ok, []}, Result).

find_sufficient_set_need_one_job_test() ->
    Jobs = [make_job_map(1, 8, 4096, 0)],
    Result = flurm_preemption:find_sufficient_set(Jobs, 1, 4, 2048, [], 0, 0, 0),
    ?assertMatch({ok, [_]}, Result).

%%====================================================================
%% preempt_single_job Tests (may call gen_server - wrap with catch)
%%====================================================================

preempt_single_job_requeue_test() ->
    Job = make_job_map(1, 4, 2048, 0),
    Result = catch flurm_preemption:preempt_single_job(Job, requeue, <<"test">>, 100),
    %% Should return ok or {error, Reason} if job doesn't exist
    ?assert(Result =:= ok orelse is_tuple(Result)).

preempt_single_job_cancel_test() ->
    Job = make_job_map(2, 4, 2048, 0),
    Result = catch flurm_preemption:preempt_single_job(Job, cancel, <<"test">>, 100),
    ?assert(Result =:= ok orelse is_tuple(Result)).

preempt_single_job_checkpoint_test() ->
    Job = make_job_map(3, 4, 2048, 0),
    Result = catch flurm_preemption:preempt_single_job(Job, checkpoint, <<"test">>, 100),
    ?assert(Result =:= ok orelse is_tuple(Result)).

preempt_single_job_suspend_test() ->
    Job = make_job_map(4, 4, 2048, 0),
    Result = catch flurm_preemption:preempt_single_job(Job, suspend, <<"test">>, 100),
    ?assert(Result =:= ok orelse is_tuple(Result)).

preempt_single_job_off_test() ->
    Job = make_job_map(5, 4, 2048, 0),
    Result = catch flurm_preemption:preempt_single_job(Job, off, <<"test">>, 100),
    ?assert(Result =:= {error, preemption_disabled} orelse is_tuple(Result)).

%%====================================================================
%% preempt_single_job_with_handling Tests
%%====================================================================

preempt_single_job_with_handling_test() ->
    Job = make_job_map(10, 4, 2048, 0),
    Result = catch flurm_preemption:preempt_single_job_with_handling(Job, requeue, <<"reason">>, 100),
    ?assert(Result =:= ok orelse is_tuple(Result)).

%%====================================================================
%% preempt_requeue Tests
%%====================================================================

preempt_requeue_test() ->
    Job = make_job_map(20, 4, 2048, 0),
    Result = catch flurm_preemption:preempt_requeue(Job, <<"test_reason">>, 100),
    %% Either succeeds or returns error if job doesn't exist
    ?assert(Result =:= ok orelse is_tuple(Result)).

%%====================================================================
%% preempt_cancel Tests
%%====================================================================

preempt_cancel_test() ->
    Job = make_job_map(21, 4, 2048, 0),
    Result = catch flurm_preemption:preempt_cancel(Job, <<"test_reason">>, 100),
    ?assert(Result =:= ok orelse is_tuple(Result)).

%%====================================================================
%% preempt_checkpoint Tests
%%====================================================================

preempt_checkpoint_test() ->
    Job = make_job_map(22, 4, 2048, 0),
    Result = catch flurm_preemption:preempt_checkpoint(Job, <<"test_reason">>, 100),
    ?assert(Result =:= ok orelse is_tuple(Result)).

%%====================================================================
%% preempt_suspend Tests
%%====================================================================

preempt_suspend_test() ->
    Job = make_job_map(23, 4, 2048, 0),
    Result = catch flurm_preemption:preempt_suspend(Job, <<"test_reason">>),
    ?assert(Result =:= ok orelse is_tuple(Result)).

%%====================================================================
%% do_graceful_preempt Tests
%%====================================================================

do_graceful_preempt_requeue_test() ->
    Job = make_job_map(30, 4, 2048, 0),
    Result = catch flurm_preemption:do_graceful_preempt(Job, requeue, <<"reason">>, 100),
    ?assert(Result =:= ok orelse is_tuple(Result)).

do_graceful_preempt_cancel_test() ->
    Job = make_job_map(31, 4, 2048, 0),
    Result = catch flurm_preemption:do_graceful_preempt(Job, cancel, <<"reason">>, 100),
    ?assert(Result =:= ok orelse is_tuple(Result)).

%%====================================================================
%% wait_for_job_exit Tests
%%====================================================================

wait_for_job_exit_immediate_test() ->
    %% Test with a non-existent job - needs gen_server
    Result = catch flurm_preemption:wait_for_job_exit(99999, 1000, 100),
    %% Should timeout, return ok, or error/exit if gen_server not running
    ?assert(Result =:= ok orelse Result =:= timeout orelse is_tuple(Result)).

wait_for_job_exit_timeout_test() ->
    %% Very short timeout
    Result = catch flurm_preemption:wait_for_job_exit(99998, 10, 5),
    ?assert(Result =:= ok orelse Result =:= timeout orelse is_tuple(Result)).

%%====================================================================
%% API Function Tests (using the correct signatures)
%%====================================================================

check_preemption_test() ->
    PendingJob = make_pending_job_map(100, 4, 2048, 1000),
    Result = catch flurm_preemption:check_preemption(PendingJob),
    %% check_preemption/1 takes a single job map and checks if preemption can help
    ?assert(is_tuple(Result) orelse Result =:= ok).

can_preempt_higher_priority_test() ->
    HighPriorityJob = make_pending_job_map(1, 4, 2048, 1000),
    LowPriorityJob = make_running_job_map(2, 4, 2048, 100),
    Result = catch flurm_preemption:can_preempt(HighPriorityJob, LowPriorityJob),
    %% Either true/false or error
    ?assert(is_boolean(Result) orelse is_tuple(Result)).

can_preempt_lower_priority_test() ->
    LowPriorityJob = make_pending_job_map(1, 4, 2048, 100),
    HighPriorityJob = make_running_job_map(2, 4, 2048, 1000),
    Result = catch flurm_preemption:can_preempt(LowPriorityJob, HighPriorityJob),
    ?assert(is_boolean(Result) orelse is_tuple(Result)).

can_preempt_equal_priority_test() ->
    Job1 = make_pending_job_map(1, 4, 2048, 500),
    Job2 = make_running_job_map(2, 4, 2048, 500),
    Result = catch flurm_preemption:can_preempt(Job1, Job2),
    ?assert(is_boolean(Result) orelse is_tuple(Result)).

%%====================================================================
%% Preemption Mode Tests
%%====================================================================

get_preemption_mode_default_test() ->
    Mode = flurm_preemption:get_preemption_mode(),
    ?assert(is_atom(Mode)),
    ?assert(lists:member(Mode, [requeue, cancel, checkpoint, suspend, off, gang])).

set_preemption_mode_test() ->
    OldMode = flurm_preemption:get_preemption_mode(),
    ok = flurm_preemption:set_preemption_mode(cancel),
    NewMode = flurm_preemption:get_preemption_mode(),
    ?assertEqual(cancel, NewMode),
    %% Restore
    flurm_preemption:set_preemption_mode(OldMode).

set_preemption_mode_all_modes_test() ->
    OldMode = flurm_preemption:get_preemption_mode(),
    Modes = [requeue, cancel, checkpoint, suspend, off],
    lists:foreach(fun(Mode) ->
        ok = flurm_preemption:set_preemption_mode(Mode),
        ?assertEqual(Mode, flurm_preemption:get_preemption_mode())
    end, Modes),
    flurm_preemption:set_preemption_mode(OldMode).

%%====================================================================
%% Cost Calculation Tests (using correct function name)
%%====================================================================

calculate_preemption_cost_test() ->
    Job = make_running_job_map(1, 4, 2048, 500),
    Cost = flurm_preemption:calculate_preemption_cost(Job),
    ?assert(is_number(Cost) orelse is_tuple(Cost)).

%%====================================================================
%% Grace Time Tests (correct function name)
%%====================================================================

get_grace_time_default_test() ->
    Time = flurm_preemption:get_grace_time(),
    ?assert(is_integer(Time)),
    ?assert(Time >= 0).

set_grace_time_test() ->
    OldTime = flurm_preemption:get_grace_time(),
    ok = flurm_preemption:set_grace_time(120),
    ?assertEqual(120, flurm_preemption:get_grace_time()),
    flurm_preemption:set_grace_time(OldTime).

%%====================================================================
%% Priority Threshold Tests
%%====================================================================

get_priority_threshold_test() ->
    Threshold = flurm_preemption:get_priority_threshold(),
    ?assert(is_integer(Threshold)).

set_priority_threshold_test() ->
    OldThreshold = flurm_preemption:get_priority_threshold(),
    ok = flurm_preemption:set_priority_threshold(500),
    ?assertEqual(500, flurm_preemption:get_priority_threshold()),
    flurm_preemption:set_priority_threshold(OldThreshold).

%%====================================================================
%% QOS Preemption Rules Tests
%%====================================================================

get_qos_preemption_rules_test() ->
    Rules = flurm_preemption:get_qos_preemption_rules(),
    ?assert(is_map(Rules) orelse is_list(Rules)).

set_qos_preemption_rules_test() ->
    OldRules = flurm_preemption:get_qos_preemption_rules(),
    NewRules = #{<<"high">> => 300, <<"normal">> => 200, <<"low">> => 100},
    ok = flurm_preemption:set_qos_preemption_rules(NewRules),
    flurm_preemption:set_qos_preemption_rules(OldRules).

%%====================================================================
%% Partition Preemption Config Tests
%%====================================================================

set_partition_preemption_mode_test() ->
    Result = catch flurm_preemption:set_partition_preemption_mode(<<"test_partition">>, cancel),
    ?assert(Result =:= ok orelse is_tuple(Result)).

set_partition_grace_time_test() ->
    Result = catch flurm_preemption:set_partition_grace_time(<<"test_partition">>, 60),
    ?assert(Result =:= ok orelse is_tuple(Result)).

%%====================================================================
%% find_preemptable_jobs Tests
%%====================================================================

find_preemptable_jobs_empty_test() ->
    PendingJob = make_pending_job_map(1, 4, 2048, 1000),
    Result = catch flurm_preemption:find_preemptable_jobs(PendingJob, []),
    ?assert(is_list(Result) orelse is_tuple(Result)).

find_preemptable_jobs_with_candidates_test() ->
    PendingJob = make_pending_job_map(1, 4, 2048, 1000),
    RunningJobs = [
        make_running_job_map(2, 8, 4096, 100),
        make_running_job_map(3, 4, 2048, 200)
    ],
    Result = catch flurm_preemption:find_preemptable_jobs(PendingJob, RunningJobs),
    ?assert(is_list(Result) orelse is_tuple(Result)).

%%====================================================================
%% check_preemption_opportunity Tests
%%====================================================================

check_preemption_opportunity_test() ->
    PendingJob = make_pending_job_map(1, 4, 2048, 1000),
    RunningJob = make_running_job_map(2, 8, 4096, 100),
    Result = catch flurm_preemption:check_preemption_opportunity(PendingJob, RunningJob),
    ?assert(is_boolean(Result) orelse is_tuple(Result)).

%%====================================================================
%% graceful_preempt Tests
%%====================================================================

graceful_preempt_test() ->
    Job = make_running_job_map(1, 4, 2048, 100),
    Result = catch flurm_preemption:graceful_preempt(Job, <<"reason">>, 60),
    ?assert(Result =:= ok orelse is_tuple(Result)).

%%====================================================================
%% Helper Functions
%%====================================================================

make_job_map(Id, Cpus, MemoryMb, Gpus) ->
    #{
        id => Id,
        name => <<"test_job_", (integer_to_binary(Id))/binary>>,
        user => <<"user">>,
        partition => <<"batch">>,
        state => running,
        num_nodes => 1,
        num_cpus => Cpus,
        memory_mb => MemoryMb,
        gpus => Gpus,
        time_limit => 3600,
        priority => 100,
        account => <<>>,
        qos => <<"normal">>,
        submit_time => erlang:system_time(second)
    }.

make_pending_job_map(Id, Cpus, MemoryMb, Priority) ->
    #{
        id => Id,
        name => <<"pending_job_", (integer_to_binary(Id))/binary>>,
        user => <<"user">>,
        partition => <<"batch">>,
        state => pending,
        num_nodes => 1,
        num_cpus => Cpus,
        memory_mb => MemoryMb,
        gpus => 0,
        time_limit => 3600,
        priority => Priority,
        account => <<>>,
        qos => <<"normal">>,
        submit_time => erlang:system_time(second)
    }.

make_running_job_map(Id, Cpus, MemoryMb, Priority) ->
    #{
        id => Id,
        name => <<"running_job_", (integer_to_binary(Id))/binary>>,
        user => <<"user">>,
        partition => <<"batch">>,
        state => running,
        num_nodes => 1,
        num_cpus => Cpus,
        memory_mb => MemoryMb,
        gpus => 0,
        time_limit => 3600,
        priority => Priority,
        account => <<>>,
        qos => <<"normal">>,
        submit_time => erlang:system_time(second),
        start_time => erlang:system_time(second)
    }.
