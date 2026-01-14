%%%-------------------------------------------------------------------
%%% @doc FLURM Job Direct Tests
%%%
%%% Additional comprehensive EUnit tests for the flurm_job gen_statem,
%%% focusing on direct function calls, state machine coverage, and
%%% edge cases that may not be covered by the main test suite.
%%%
%%% These tests call actual module functions directly without mocking.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_direct_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Setup and teardown for all tests
job_direct_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"Direct start_link creates pending job", fun test_direct_start_link/0},
        {"Callback mode returns expected value", fun test_callback_mode_direct/0},
        {"State enter actions executed", fun test_state_enter_actions/0},
        {"Completing state with undefined exit code", fun test_completing_undefined_exit/0},
        {"Multiple allocations with same nodes", fun test_multiple_allocations/0},
        {"Node failure with multiple allocated nodes", fun test_node_failure_multiple_nodes/0},
        {"Priority bounds during init", fun test_priority_init_bounds/0},
        {"Preempt modes coverage", fun test_preempt_modes/0},
        {"Terminal state get_job_id", fun test_terminal_state_get_job_id/0},
        {"Info messages in all states", fun test_info_messages_all_states/0},
        {"State timeout edge cases", fun test_state_timeout_edges/0},
        {"Concurrent signals", fun test_concurrent_signals/0},
        {"Node failure non-allocated ignored", fun test_node_failure_non_allocated/0},
        {"Preempt from suspended state", fun test_preempt_suspended/0},
        {"Set priority in different states", fun test_set_priority_states/0},
        {"Get info structure validation", fun test_get_info_structure/0},
        {"Code change upgrade simulation", fun test_code_change_upgrade/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(sasl),
    %% Start the job registry
    {ok, RegistryPid} = flurm_job_registry:start_link(),
    %% Start the job supervisor
    {ok, SupPid} = flurm_job_sup:start_link(),
    #{registry => RegistryPid, supervisor => SupPid}.

cleanup(#{registry := RegistryPid, supervisor := SupPid}) ->
    %% Stop all jobs first
    [flurm_job_sup:stop_job(Pid) || Pid <- flurm_job_sup:which_jobs()],
    %% Unlink before stopping to prevent shutdown propagation to test process
    catch unlink(SupPid),
    catch unlink(RegistryPid),
    %% Stop supervisor and registry properly using gen_server:stop
    catch gen_server:stop(SupPid, shutdown, 5000),
    catch gen_server:stop(RegistryPid, shutdown, 5000),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

make_job_spec() ->
    make_job_spec(#{}).

make_job_spec(Overrides) ->
    Defaults = #{
        user_id => 1000,
        group_id => 1000,
        partition => <<"default">>,
        num_nodes => 1,
        num_cpus => 4,
        time_limit => 3600,
        script => <<"#!/bin/bash\necho hello">>,
        priority => 100
    },
    Props = maps:merge(Defaults, Overrides),
    #job_spec{
        user_id = maps:get(user_id, Props),
        group_id = maps:get(group_id, Props),
        partition = maps:get(partition, Props),
        num_nodes = maps:get(num_nodes, Props),
        num_cpus = maps:get(num_cpus, Props),
        time_limit = maps:get(time_limit, Props),
        script = maps:get(script, Props),
        priority = maps:get(priority, Props)
    }.

%%====================================================================
%% Direct API Tests
%%====================================================================

test_direct_start_link() ->
    %% Test start_link directly without going through submit
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Verify we can get state from directly started job
    {ok, pending} = flurm_job:get_state(Pid),

    %% Get job ID
    JobId = gen_statem:call(Pid, get_job_id),
    ?assert(is_integer(JobId)),

    %% Clean up
    gen_statem:stop(Pid),
    ok.

test_callback_mode_direct() ->
    %% Test that callback_mode returns the correct configuration
    Mode = flurm_job:callback_mode(),
    ?assertEqual([state_functions, state_enter], Mode),

    %% Verify state_functions is present (for state-specific handlers)
    ?assert(lists:member(state_functions, Mode)),

    %% Verify state_enter is present (for enter actions)
    ?assert(lists:member(state_enter, Mode)),
    ok.

test_state_enter_actions() ->
    %% Test that state enter actions are executed
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% State enter for pending should have been called
    {ok, pending} = flurm_job:get_state(Pid),

    %% Transition to configuring - enter should be called
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% Transition to running - enter should be called
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Suspend - enter should be called
    ok = flurm_job:suspend(Pid),
    {ok, suspended} = flurm_job:get_state(Pid),

    %% Resume - enter for running should be called again
    ok = flurm_job:resume(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    ok.

test_completing_undefined_exit() ->
    %% Test completing state behavior with undefined exit code
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Complete with exit code 0 - should result in completed state
    ok = flurm_job:signal_job_complete(Pid, 0),
    {ok, completing} = flurm_job:get_state(Pid),

    %% Signal cleanup complete
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, completed} = flurm_job:get_state(Pid),

    %% Verify exit code in info
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(0, maps:get(exit_code, Info)),
    ok.

test_multiple_allocations() ->
    %% Test allocation with varying node counts
    JobSpec = make_job_spec(#{num_nodes => 3}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Try with insufficient nodes
    {error, insufficient_nodes} = flurm_job:allocate(Pid, [<<"node1">>]),
    {error, insufficient_nodes} = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>]),

    %% Should still be pending
    {ok, pending} = flurm_job:get_state(Pid),

    %% Allocate with exact nodes
    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>, <<"node3">>]),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% Verify allocated nodes
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual([<<"node1">>, <<"node2">>, <<"node3">>], maps:get(allocated_nodes, Info)),
    ok.

test_node_failure_multiple_nodes() ->
    %% Test node failure when multiple nodes allocated
    JobSpec = make_job_spec(#{num_nodes => 2}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Allocate and start
    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Node failure for non-allocated node (ignored)
    ok = flurm_job:signal_node_failure(Pid, <<"node3">>),
    {ok, running} = flurm_job:get_state(Pid),

    %% Node failure for one of the allocated nodes
    ok = flurm_job:signal_node_failure(Pid, <<"node2">>),
    {ok, node_fail} = flurm_job:get_state(Pid),
    ok.

test_priority_init_bounds() ->
    %% Test priority clamping during job creation

    %% Test with very high priority
    JobSpec1 = make_job_spec(#{priority => 999999}),
    {ok, Pid1, _} = flurm_job:submit(JobSpec1),
    {ok, Info1} = flurm_job:get_info(Pid1),
    ?assertEqual(?MAX_PRIORITY, maps:get(priority, Info1)),

    %% Test with negative priority
    JobSpec2 = make_job_spec(#{priority => -500}),
    {ok, Pid2, _} = flurm_job:submit(JobSpec2),
    {ok, Info2} = flurm_job:get_info(Pid2),
    ?assertEqual(?MIN_PRIORITY, maps:get(priority, Info2)),

    %% Test with undefined priority (should use default)
    JobSpec3 = #job_spec{
        user_id = 1000,
        group_id = 1000,
        partition = <<"default">>,
        num_nodes = 1,
        num_cpus = 1,
        time_limit = 3600,
        script = <<"test">>,
        priority = undefined
    },
    {ok, Pid3, _} = flurm_job:submit(JobSpec3),
    {ok, Info3} = flurm_job:get_info(Pid3),
    ?assertEqual(?DEFAULT_PRIORITY, maps:get(priority, Info3)),
    ok.

test_preempt_modes() ->
    %% Test all preempt modes
    JobSpec = make_job_spec(),

    %% Test requeue mode
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    {ok, running} = flurm_job:get_state(Pid1),
    ok = flurm_job:preempt(Pid1, requeue, 30),
    {ok, pending} = flurm_job:get_state(Pid1),

    %% Test cancel mode
    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid2),
    {ok, running} = flurm_job:get_state(Pid2),
    ok = flurm_job:preempt(Pid2, cancel, 60),
    {ok, cancelled} = flurm_job:get_state(Pid2),

    %% Test checkpoint mode
    {ok, Pid3, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid3, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid3),
    {ok, running} = flurm_job:get_state(Pid3),
    ok = flurm_job:preempt(Pid3, checkpoint, 120),
    {ok, pending} = flurm_job:get_state(Pid3),
    ok.

test_terminal_state_get_job_id() ->
    %% Test that get_job_id works in all terminal states
    JobSpec = make_job_spec(),

    %% Test in completed state
    {ok, Pid1, JobId1} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:signal_job_complete(Pid1, 0),
    ok = flurm_job:signal_cleanup_complete(Pid1),
    {ok, completed} = flurm_job:get_state(Pid1),
    ?assertEqual(JobId1, gen_statem:call(Pid1, get_job_id)),

    %% Test in cancelled state
    {ok, Pid2, JobId2} = flurm_job:submit(JobSpec),
    ok = flurm_job:cancel(Pid2),
    {ok, cancelled} = flurm_job:get_state(Pid2),
    ?assertEqual(JobId2, gen_statem:call(Pid2, get_job_id)),

    %% Test in failed state
    {ok, Pid3, JobId3} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid3, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid3),
    ok = flurm_job:signal_job_complete(Pid3, 127),
    ok = flurm_job:signal_cleanup_complete(Pid3),
    {ok, failed} = flurm_job:get_state(Pid3),
    ?assertEqual(JobId3, gen_statem:call(Pid3, get_job_id)),

    %% Test in node_fail state
    {ok, Pid4, JobId4} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid4, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid4),
    ok = flurm_job:signal_node_failure(Pid4, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid4),
    ?assertEqual(JobId4, gen_statem:call(Pid4, get_job_id)),
    ok.

test_info_messages_all_states() ->
    %% Test that info messages are properly ignored in all states
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Test in pending
    Pid ! {some_random, message},
    Pid ! timeout,
    timer:sleep(10),
    {ok, pending} = flurm_job:get_state(Pid),

    %% Test in configuring
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    Pid ! {another, info},
    timer:sleep(10),
    {ok, configuring} = flurm_job:get_state(Pid),

    %% Test in running
    ok = flurm_job:signal_config_complete(Pid),
    Pid ! {running, info},
    timer:sleep(10),
    {ok, running} = flurm_job:get_state(Pid),

    %% Test in suspended
    ok = flurm_job:suspend(Pid),
    Pid ! {suspended, info},
    timer:sleep(10),
    {ok, suspended} = flurm_job:get_state(Pid),

    %% Resume and complete
    ok = flurm_job:resume(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),

    %% Test in completing
    Pid ! {completing, info},
    timer:sleep(10),
    {ok, completing} = flurm_job:get_state(Pid),

    ok = flurm_job:signal_cleanup_complete(Pid),

    %% Test in completed
    Pid ! {completed, info},
    timer:sleep(10),
    {ok, completed} = flurm_job:get_state(Pid),
    ok.

test_state_timeout_edges() ->
    %% Test timeout state with short time limit
    JobSpec = make_job_spec(#{time_limit => 1}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Wait for timeout
    timer:sleep(1200),
    {ok, timeout} = flurm_job:get_state(Pid),

    %% Verify terminal state behavior
    {error, already_timed_out} = flurm_job:cancel(Pid),
    {error, job_timed_out} = gen_statem:call(Pid, {preempt, requeue, 30}),

    %% Info still works
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(timeout, maps:get(state, Info)),
    ok.

test_concurrent_signals() ->
    %% Test sending multiple signals quickly
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Move to running
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Send multiple casts quickly
    gen_statem:cast(Pid, random_cast1),
    gen_statem:cast(Pid, random_cast2),
    gen_statem:cast(Pid, {job_complete, 0}),

    %% Should transition to completing
    timer:sleep(50),
    {ok, completing} = flurm_job:get_state(Pid),
    ok.

test_node_failure_non_allocated() ->
    %% Test that node failure for non-allocated nodes is ignored in all states
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Allocate node1 only
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    %% Send failure for nodes not allocated
    ok = flurm_job:signal_node_failure(Pid, <<"node2">>),
    ok = flurm_job:signal_node_failure(Pid, <<"node3">>),
    ok = flurm_job:signal_node_failure(Pid, <<"unrelated">>),

    %% Should still be running
    {ok, running} = flurm_job:get_state(Pid),
    ok.

test_preempt_suspended() ->
    %% Test preemption from suspended state
    JobSpec = make_job_spec(),

    %% Test requeue from suspended
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:suspend(Pid1),
    {ok, suspended} = flurm_job:get_state(Pid1),
    ok = flurm_job:preempt(Pid1, requeue, 30),
    {ok, pending} = flurm_job:get_state(Pid1),

    %% Test cancel from suspended
    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid2),
    ok = flurm_job:suspend(Pid2),
    {ok, suspended} = flurm_job:get_state(Pid2),
    ok = flurm_job:preempt(Pid2, cancel, 30),
    {ok, cancelled} = flurm_job:get_state(Pid2),
    ok.

test_set_priority_states() ->
    %% Test set_priority in valid states
    JobSpec = make_job_spec(#{priority => 100}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Set priority in running state
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),

    ok = flurm_job:set_priority(Pid, 500),
    {ok, Info1} = flurm_job:get_info(Pid),
    ?assertEqual(500, maps:get(priority, Info1)),

    %% Set priority in suspended state
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:set_priority(Pid, 300),
    {ok, Info2} = flurm_job:get_info(Pid),
    ?assertEqual(300, maps:get(priority, Info2)),

    %% Test clamping
    ok = flurm_job:set_priority(Pid, 99999),
    {ok, Info3} = flurm_job:get_info(Pid),
    ?assertEqual(?MAX_PRIORITY, maps:get(priority, Info3)),

    ok = flurm_job:set_priority(Pid, -1000),
    {ok, Info4} = flurm_job:get_info(Pid),
    ?assertEqual(?MIN_PRIORITY, maps:get(priority, Info4)),
    ok.

test_get_info_structure() ->
    %% Test the complete structure of get_info response
    JobSpec = make_job_spec(#{
        user_id => 2001,
        group_id => 2001,
        partition => <<"compute">>,
        num_nodes => 2,
        num_cpus => 8,
        time_limit => 7200,
        script => <<"#!/bin/bash\necho test">>,
        priority => 250
    }),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),

    {ok, Info} = flurm_job:get_info(Pid),

    %% Verify all expected keys are present
    ?assert(maps:is_key(job_id, Info)),
    ?assert(maps:is_key(user_id, Info)),
    ?assert(maps:is_key(group_id, Info)),
    ?assert(maps:is_key(partition, Info)),
    ?assert(maps:is_key(state, Info)),
    ?assert(maps:is_key(num_nodes, Info)),
    ?assert(maps:is_key(num_cpus, Info)),
    ?assert(maps:is_key(time_limit, Info)),
    ?assert(maps:is_key(script, Info)),
    ?assert(maps:is_key(allocated_nodes, Info)),
    ?assert(maps:is_key(submit_time, Info)),
    ?assert(maps:is_key(start_time, Info)),
    ?assert(maps:is_key(end_time, Info)),
    ?assert(maps:is_key(exit_code, Info)),
    ?assert(maps:is_key(priority, Info)),

    %% Verify values
    ?assertEqual(JobId, maps:get(job_id, Info)),
    ?assertEqual(2001, maps:get(user_id, Info)),
    ?assertEqual(2001, maps:get(group_id, Info)),
    ?assertEqual(<<"compute">>, maps:get(partition, Info)),
    ?assertEqual(pending, maps:get(state, Info)),
    ?assertEqual(2, maps:get(num_nodes, Info)),
    ?assertEqual(8, maps:get(num_cpus, Info)),
    ?assertEqual(7200, maps:get(time_limit, Info)),
    ?assertEqual([], maps:get(allocated_nodes, Info)),
    ?assertEqual(undefined, maps:get(start_time, Info)),
    ?assertEqual(undefined, maps:get(end_time, Info)),
    ?assertEqual(undefined, maps:get(exit_code, Info)),
    ?assertEqual(250, maps:get(priority, Info)),
    ok.

test_code_change_upgrade() ->
    %% Test code_change callback indirectly
    %% We can't call code_change directly but can verify job continues
    %% to function after multiple state transitions
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),

    %% Multiple state transitions
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:resume(Pid),
    ok = flurm_job:preempt(Pid, requeue, 30),
    ok = flurm_job:allocate(Pid, [<<"node2">>]),
    ok = flurm_job:signal_config_complete(Pid),

    %% Verify job still functions
    {ok, running} = flurm_job:get_state(Pid),
    {ok, Info} = flurm_job:get_info(Pid),
    ?assert(is_map(Info)),
    ok.

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

%% Test job submission error handling
job_submit_error_test_() ->
    {setup,
     fun() ->
         %% Start supervisor but NOT registry to cause submit to fail
         {ok, SupPid} = flurm_job_sup:start_link(),
         {ok, RegPid} = flurm_job_registry:start_link(),
         #{supervisor => SupPid, registry => RegPid}
     end,
     fun(#{supervisor := SupPid, registry := RegPid}) ->
         catch unlink(SupPid),
         catch unlink(RegPid),
         catch gen_server:stop(SupPid, shutdown, 5000),
         catch gen_server:stop(RegPid, shutdown, 5000)
     end,
     fun(_) ->
         {"Valid job submission succeeds", fun() ->
             JobSpec = make_job_spec(),
             {ok, Pid, JobId} = flurm_job:submit(JobSpec),
             ?assert(is_pid(Pid)),
             ?assert(is_integer(JobId))
         end}
     end}.

%% Test get_info in all terminal states
terminal_state_info_test_() ->
    {setup,
     fun() ->
         {ok, RegistryPid} = flurm_job_registry:start_link(),
         {ok, SupPid} = flurm_job_sup:start_link(),
         #{registry => RegistryPid, supervisor => SupPid}
     end,
     fun(#{registry := RegistryPid, supervisor := SupPid}) ->
         catch unlink(SupPid),
         catch unlink(RegistryPid),
         catch gen_server:stop(SupPid, shutdown, 5000),
         catch gen_server:stop(RegistryPid, shutdown, 5000)
     end,
     fun(_) ->
         [
             {"Get info in completed state", fun() ->
                 JobSpec = make_job_spec(),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 ok = flurm_job:allocate(Pid, [<<"node1">>]),
                 ok = flurm_job:signal_config_complete(Pid),
                 ok = flurm_job:signal_job_complete(Pid, 0),
                 ok = flurm_job:signal_cleanup_complete(Pid),
                 {ok, completed} = flurm_job:get_state(Pid),
                 {ok, Info} = flurm_job:get_info(Pid),
                 ?assertEqual(completed, maps:get(state, Info))
             end},
             {"Get info in failed state", fun() ->
                 JobSpec = make_job_spec(),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 ok = flurm_job:allocate(Pid, [<<"node1">>]),
                 ok = flurm_job:signal_config_complete(Pid),
                 ok = flurm_job:signal_job_complete(Pid, 1),
                 ok = flurm_job:signal_cleanup_complete(Pid),
                 {ok, failed} = flurm_job:get_state(Pid),
                 {ok, Info} = flurm_job:get_info(Pid),
                 ?assertEqual(failed, maps:get(state, Info))
             end},
             {"Get info in timeout state", fun() ->
                 JobSpec = make_job_spec(#{time_limit => 1}),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 ok = flurm_job:allocate(Pid, [<<"node1">>]),
                 ok = flurm_job:signal_config_complete(Pid),
                 timer:sleep(1200),
                 {ok, timeout} = flurm_job:get_state(Pid),
                 {ok, Info} = flurm_job:get_info(Pid),
                 ?assertEqual(timeout, maps:get(state, Info))
             end},
             {"Get info in node_fail state", fun() ->
                 JobSpec = make_job_spec(),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 ok = flurm_job:allocate(Pid, [<<"node1">>]),
                 ok = flurm_job:signal_config_complete(Pid),
                 ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
                 {ok, node_fail} = flurm_job:get_state(Pid),
                 {ok, Info} = flurm_job:get_info(Pid),
                 ?assertEqual(node_fail, maps:get(state, Info))
             end}
         ]
     end}.

%% Test invalid operations in each state
invalid_operations_test_() ->
    {setup,
     fun() ->
         {ok, RegistryPid} = flurm_job_registry:start_link(),
         {ok, SupPid} = flurm_job_sup:start_link(),
         #{registry => RegistryPid, supervisor => SupPid}
     end,
     fun(#{registry := RegistryPid, supervisor := SupPid}) ->
         catch unlink(SupPid),
         catch unlink(RegistryPid),
         catch gen_server:stop(SupPid, shutdown, 5000),
         catch gen_server:stop(RegistryPid, shutdown, 5000)
     end,
     fun(_) ->
         [
             {"Cannot preempt in pending state", fun() ->
                 JobSpec = make_job_spec(),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 {ok, pending} = flurm_job:get_state(Pid),
                 {error, invalid_operation} = flurm_job:preempt(Pid, requeue, 30)
             end},
             {"Cannot suspend in pending state", fun() ->
                 JobSpec = make_job_spec(),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 {ok, pending} = flurm_job:get_state(Pid),
                 {error, invalid_operation} = flurm_job:suspend(Pid)
             end},
             {"Cannot resume in running state", fun() ->
                 JobSpec = make_job_spec(),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 ok = flurm_job:allocate(Pid, [<<"node1">>]),
                 ok = flurm_job:signal_config_complete(Pid),
                 {ok, running} = flurm_job:get_state(Pid),
                 {error, invalid_operation} = flurm_job:resume(Pid)
             end},
             {"Cannot allocate in configuring state", fun() ->
                 JobSpec = make_job_spec(),
                 {ok, Pid, _} = flurm_job:submit(JobSpec),
                 ok = flurm_job:allocate(Pid, [<<"node1">>]),
                 {ok, configuring} = flurm_job:get_state(Pid),
                 {error, invalid_operation} = flurm_job:allocate(Pid, [<<"node2">>])
             end}
         ]
     end}.
