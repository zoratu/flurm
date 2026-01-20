%%%-------------------------------------------------------------------
%%% @doc FLURM Job Coverage Integration Tests
%%%
%%% Integration tests designed to call through public APIs to improve
%%% code coverage tracking. Cover only tracks calls from test modules
%%% to the actual module code, so these tests call public functions
%%% that exercise internal code paths.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_job_cover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

job_cover_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link creates job process", fun test_start_link/0},
        {"submit creates and registers job", fun test_submit/0},
        {"cancel job in pending state", fun test_cancel_pending/0},
        {"cancel job in running state", fun test_cancel_running/0},
        {"get_info returns job data", fun test_get_info/0},
        {"get_state returns current state", fun test_get_state/0},
        {"allocate transitions to configuring", fun test_allocate/0},
        {"signal_config_complete transitions to running", fun test_config_complete/0},
        {"signal_job_complete transitions to completing", fun test_job_complete/0},
        {"signal_cleanup_complete transitions to completed/failed", fun test_cleanup_complete/0},
        {"signal_node_failure transitions to node_fail", fun test_node_failure/0},
        {"preempt with requeue", fun test_preempt_requeue/0},
        {"preempt with cancel", fun test_preempt_cancel/0},
        {"preempt with checkpoint", fun test_preempt_checkpoint/0},
        {"suspend running job", fun test_suspend/0},
        {"resume suspended job", fun test_resume/0},
        {"set_priority updates priority", fun test_set_priority/0},
        {"job ID API variants", fun test_job_id_variants/0},
        {"callback_mode returns expected value", fun test_callback_mode/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    {ok, RegistryPid} = flurm_job_registry:start_link(),
    {ok, SupPid} = flurm_job_sup:start_link(),
    #{registry => RegistryPid, supervisor => SupPid}.

cleanup(#{registry := RegistryPid, supervisor := SupPid}) ->
    [catch flurm_job_sup:stop_job(P) || P <- flurm_job_sup:which_jobs()],
    lists:foreach(fun(Pid) ->
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
        end
    end, [SupPid, RegistryPid]),
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
        script => <<"#!/bin/bash\necho test">>,
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
%% Coverage Tests - Public API Calls
%%====================================================================

test_start_link() ->
    %% Test start_link directly
    JobSpec = make_job_spec(),
    {ok, Pid} = flurm_job:start_link(JobSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    %% Get state to verify it started correctly
    {ok, pending} = flurm_job:get_state(Pid),
    gen_statem:stop(Pid),
    ok.

test_submit() ->
    %% Test submit which creates job via supervisor and registers it
    JobSpec = make_job_spec(),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),
    ?assert(is_pid(Pid)),
    ?assert(is_integer(JobId)),
    ?assert(JobId > 0),
    %% Verify registered
    {ok, Pid} = flurm_job_registry:lookup_job(JobId),
    ok.

test_cancel_pending() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    {ok, pending} = flurm_job:get_state(Pid),
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),
    %% Verify cannot cancel again
    {error, already_cancelled} = flurm_job:cancel(Pid),
    ok.

test_cancel_running() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),
    ok.

test_get_info() ->
    JobSpec = make_job_spec(#{
        user_id => 1001,
        partition => <<"compute">>,
        num_nodes => 2,
        num_cpus => 8,
        priority => 500
    }),
    {ok, Pid, JobId} = flurm_job:submit(JobSpec),
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(JobId, maps:get(job_id, Info)),
    ?assertEqual(1001, maps:get(user_id, Info)),
    ?assertEqual(<<"compute">>, maps:get(partition, Info)),
    ?assertEqual(2, maps:get(num_nodes, Info)),
    ?assertEqual(8, maps:get(num_cpus, Info)),
    ?assertEqual(500, maps:get(priority, Info)),
    ?assertEqual(pending, maps:get(state, Info)),
    ok.

test_get_state() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    {ok, pending} = flurm_job:get_state(Pid),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    ok.

test_allocate() ->
    JobSpec = make_job_spec(#{num_nodes => 2}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    %% Insufficient nodes should fail
    {error, insufficient_nodes} = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, pending} = flurm_job:get_state(Pid),
    %% Sufficient nodes should succeed
    ok = flurm_job:allocate(Pid, [<<"node1">>, <<"node2">>]),
    {ok, configuring} = flurm_job:get_state(Pid),
    %% Cannot allocate again
    {error, invalid_operation} = flurm_job:allocate(Pid, [<<"node3">>]),
    ok.

test_config_complete() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    ok.

test_job_complete() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    %% Signal job complete with exit code
    ok = flurm_job:signal_job_complete(Pid, 0),
    {ok, completing} = flurm_job:get_state(Pid),
    ok.

test_cleanup_complete() ->
    %% Test with exit code 0 -> completed
    JobSpec = make_job_spec(),
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:signal_job_complete(Pid1, 0),
    ok = flurm_job:signal_cleanup_complete(Pid1),
    {ok, completed} = flurm_job:get_state(Pid1),
    %% Verify terminal state errors
    {error, already_completed} = flurm_job:cancel(Pid1),
    {error, job_completed} = flurm_job:allocate(Pid1, [<<"node2">>]),

    %% Test with exit code 1 -> failed
    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid2),
    ok = flurm_job:signal_job_complete(Pid2, 1),
    ok = flurm_job:signal_cleanup_complete(Pid2),
    {ok, failed} = flurm_job:get_state(Pid2),
    {error, already_failed} = flurm_job:cancel(Pid2),
    ok.

test_node_failure() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    %% Node failure for unrelated node should be ignored
    ok = flurm_job:signal_node_failure(Pid, <<"node2">>),
    {ok, running} = flurm_job:get_state(Pid),
    %% Node failure for allocated node transitions to node_fail
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),
    {error, already_failed} = flurm_job:cancel(Pid),
    ok.

test_preempt_requeue() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    %% Preempt with requeue goes back to pending
    ok = flurm_job:preempt(Pid, requeue, 30),
    {ok, pending} = flurm_job:get_state(Pid),
    ok.

test_preempt_cancel() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    %% Preempt with cancel goes to cancelled
    ok = flurm_job:preempt(Pid, cancel, 30),
    {ok, cancelled} = flurm_job:get_state(Pid),
    ok.

test_preempt_checkpoint() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    %% Preempt with checkpoint goes back to pending
    ok = flurm_job:preempt(Pid, checkpoint, 30),
    {ok, pending} = flurm_job:get_state(Pid),
    ok.

test_suspend() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    ok = flurm_job:suspend(Pid),
    {ok, suspended} = flurm_job:get_state(Pid),
    %% Verify get_info in suspended state
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(suspended, maps:get(state, Info)),
    ok.

test_resume() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    {ok, suspended} = flurm_job:get_state(Pid),
    ok = flurm_job:resume(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    ok.

test_set_priority() ->
    JobSpec = make_job_spec(#{priority => 100}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    %% Set priority while running
    ok = flurm_job:set_priority(Pid, 500),
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(500, maps:get(priority, Info)),
    %% Test clamping to max
    ok = flurm_job:set_priority(Pid, 999999),
    {ok, Info2} = flurm_job:get_info(Pid),
    ?assertEqual(?MAX_PRIORITY, maps:get(priority, Info2)),
    %% Test clamping to min
    ok = flurm_job:set_priority(Pid, -100),
    {ok, Info3} = flurm_job:get_info(Pid),
    ?assertEqual(?MIN_PRIORITY, maps:get(priority, Info3)),
    %% Test in suspended state
    ok = flurm_job:suspend(Pid),
    ok = flurm_job:set_priority(Pid, 300),
    {ok, Info4} = flurm_job:get_info(Pid),
    ?assertEqual(300, maps:get(priority, Info4)),
    ok.

test_job_id_variants() ->
    JobSpec = make_job_spec(),
    {ok, _Pid, JobId} = flurm_job:submit(JobSpec),
    %% Test API calls with job ID instead of PID
    {ok, Info} = flurm_job:get_info(JobId),
    ?assertEqual(JobId, maps:get(job_id, Info)),
    {ok, pending} = flurm_job:get_state(JobId),
    ok = flurm_job:allocate(JobId, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(JobId),
    ok = flurm_job:signal_config_complete(JobId),
    {ok, running} = flurm_job:get_state(JobId),
    ok = flurm_job:suspend(JobId),
    {ok, suspended} = flurm_job:get_state(JobId),
    ok = flurm_job:resume(JobId),
    ok = flurm_job:set_priority(JobId, 200),
    ok = flurm_job:cancel(JobId),
    {ok, cancelled} = flurm_job:get_state(JobId),
    %% Test with non-existent job ID
    NonExistent = 999999999,
    {error, job_not_found} = flurm_job:get_info(NonExistent),
    {error, job_not_found} = flurm_job:get_state(NonExistent),
    {error, job_not_found} = flurm_job:cancel(NonExistent),
    {error, job_not_found} = flurm_job:allocate(NonExistent, [<<"n">>]),
    {error, job_not_found} = flurm_job:signal_config_complete(NonExistent),
    {error, job_not_found} = flurm_job:signal_job_complete(NonExistent, 0),
    {error, job_not_found} = flurm_job:signal_cleanup_complete(NonExistent),
    {error, job_not_found} = flurm_job:signal_node_failure(NonExistent, <<"n">>),
    {error, job_not_found} = flurm_job:preempt(NonExistent, requeue, 30),
    {error, job_not_found} = flurm_job:suspend(NonExistent),
    {error, job_not_found} = flurm_job:resume(NonExistent),
    {error, job_not_found} = flurm_job:set_priority(NonExistent, 100),
    ok.

test_callback_mode() ->
    Mode = flurm_job:callback_mode(),
    ?assertEqual([state_functions, state_enter], Mode),
    ok.

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

job_terminal_states_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"timeout state behaviors", fun test_timeout_state/0},
        {"node_fail state behaviors", fun test_node_fail_state/0},
        {"completed state behaviors", fun test_completed_state/0},
        {"failed state behaviors", fun test_failed_state/0},
        {"cancelled state behaviors", fun test_cancelled_state/0}
     ]}.

test_timeout_state() ->
    %% Create job with very short time limit
    JobSpec = make_job_spec(#{time_limit => 1}),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    {ok, running} = flurm_job:get_state(Pid),
    %% Wait for timeout (legitimate wait for 1-second job timeout to expire)
    timer:sleep(1500),
    {ok, timeout} = flurm_job:get_state(Pid),
    %% Verify terminal state errors
    {error, already_timed_out} = flurm_job:cancel(Pid),
    {error, job_timed_out} = gen_statem:call(Pid, {set_priority, 100}),
    %% Verify get_info works
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(timeout, maps:get(state, Info)),
    ok.

test_node_fail_state() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),
    %% Verify terminal state errors
    {error, already_failed} = flurm_job:cancel(Pid),
    {error, node_failure} = gen_statem:call(Pid, {set_priority, 100}),
    {error, node_failure} = flurm_job:resume(Pid),
    %% Verify get_info works
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(node_fail, maps:get(state, Info)),
    ok.

test_completed_state() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 0),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, completed} = flurm_job:get_state(Pid),
    %% Verify terminal state errors
    {error, already_completed} = flurm_job:cancel(Pid),
    {error, job_completed} = gen_statem:call(Pid, {set_priority, 100}),
    %% Verify get_info works
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(completed, maps:get(state, Info)),
    ok.

test_failed_state() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:signal_job_complete(Pid, 1),
    ok = flurm_job:signal_cleanup_complete(Pid),
    {ok, failed} = flurm_job:get_state(Pid),
    %% Verify terminal state errors
    {error, already_failed} = flurm_job:cancel(Pid),
    {error, job_failed} = gen_statem:call(Pid, {set_priority, 100}),
    {error, job_failed} = flurm_job:suspend(Pid),
    %% Verify get_info works
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(failed, maps:get(state, Info)),
    ok.

test_cancelled_state() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:cancel(Pid),
    {ok, cancelled} = flurm_job:get_state(Pid),
    %% Verify terminal state errors
    {error, already_cancelled} = flurm_job:cancel(Pid),
    {error, job_cancelled} = gen_statem:call(Pid, {set_priority, 100}),
    %% Verify get_info works
    {ok, Info} = flurm_job:get_info(Pid),
    ?assertEqual(cancelled, maps:get(state, Info)),
    ok.

%%====================================================================
%% State Transition Edge Cases
%%====================================================================

job_edge_cases_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"node failure during configuring", fun test_node_failure_configuring/0},
        {"preempt from suspended state", fun test_preempt_from_suspended/0},
        {"node failure in suspended state", fun test_node_failure_suspended/0},
        {"ignored casts in all states", fun test_ignored_casts/0},
        {"ignored info in all states", fun test_ignored_info/0}
     ]}.

test_node_failure_configuring() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    {ok, configuring} = flurm_job:get_state(Pid),
    %% Unrelated node failure ignored
    ok = flurm_job:signal_node_failure(Pid, <<"node2">>),
    {ok, configuring} = flurm_job:get_state(Pid),
    %% Allocated node failure transitions
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),
    ok.

test_preempt_from_suspended() ->
    JobSpec = make_job_spec(),
    {ok, Pid1, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid1, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid1),
    ok = flurm_job:suspend(Pid1),
    {ok, suspended} = flurm_job:get_state(Pid1),
    ok = flurm_job:preempt(Pid1, requeue, 30),
    {ok, pending} = flurm_job:get_state(Pid1),

    {ok, Pid2, _} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid2, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid2),
    ok = flurm_job:suspend(Pid2),
    ok = flurm_job:preempt(Pid2, cancel, 30),
    {ok, cancelled} = flurm_job:get_state(Pid2),
    ok.

test_node_failure_suspended() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    ok = flurm_job:signal_config_complete(Pid),
    ok = flurm_job:suspend(Pid),
    {ok, suspended} = flurm_job:get_state(Pid),
    %% Unrelated node failure ignored
    ok = flurm_job:signal_node_failure(Pid, <<"node2">>),
    {ok, suspended} = flurm_job:get_state(Pid),
    %% Allocated node failure transitions
    ok = flurm_job:signal_node_failure(Pid, <<"node1">>),
    {ok, node_fail} = flurm_job:get_state(Pid),
    ok.

test_ignored_casts() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    %% Unknown cast in pending
    gen_statem:cast(Pid, unknown_cast),
    _ = sys:get_state(Pid),
    {ok, pending} = flurm_job:get_state(Pid),
    %% Config complete in pending (ignored)
    gen_statem:cast(Pid, config_complete),
    _ = sys:get_state(Pid),
    {ok, pending} = flurm_job:get_state(Pid),
    ok.

test_ignored_info() ->
    JobSpec = make_job_spec(),
    {ok, Pid, _JobId} = flurm_job:submit(JobSpec),
    %% Info messages ignored
    Pid ! random_info_message,
    _ = sys:get_state(Pid),
    {ok, pending} = flurm_job:get_state(Pid),
    ok = flurm_job:allocate(Pid, [<<"node1">>]),
    Pid ! another_info,
    _ = sys:get_state(Pid),
    {ok, configuring} = flurm_job:get_state(Pid),
    ok.
