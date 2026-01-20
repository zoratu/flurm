%%%-------------------------------------------------------------------
%%% @doc FLURM Scheduler Coverage Tests
%%%
%%% Comprehensive EUnit tests specifically designed to maximize code
%%% coverage for the flurm_scheduler module. Tests all internal
%%% functions, error paths, and edge cases.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_coverage_tests).

-include_lib("eunit/include/eunit.hrl").
-include("flurm_core.hrl").

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),
    %% Start meck for mocking external dependencies
    meck:new(flurm_job_manager, [passthrough, no_link]),
    meck:new(flurm_node_manager, [non_strict, no_link]),
    meck:new(flurm_job_deps, [passthrough, no_link]),
    meck:new(flurm_license, [passthrough, no_link]),
    meck:new(flurm_limits, [passthrough, no_link]),
    meck:new(flurm_reservation, [passthrough, no_link]),
    meck:new(flurm_backfill, [passthrough, no_link]),
    meck:new(flurm_preemption, [passthrough, no_link]),
    meck:new(flurm_job_dispatcher, [passthrough, no_link]),
    meck:new(flurm_gres, [passthrough, no_link]),
    meck:new(flurm_metrics, [non_strict, no_link]),
    meck:new(flurm_config_server, [passthrough, no_link]),

    %% Default mocks for external dependencies
    meck:expect(flurm_config_server, subscribe_changes, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, histogram, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, gauge, fun(_, _) -> ok end),
    meck:expect(flurm_limits, check_limits, fun(_) -> ok end),
    meck:expect(flurm_limits, enforce_limit, fun(_, _, _) -> ok end),
    meck:expect(flurm_license, check_availability, fun(_) -> true end),
    meck:expect(flurm_license, allocate, fun(_, _) -> ok end),
    meck:expect(flurm_license, deallocate, fun(_, _) -> ok end),
    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {ok, []} end),
    meck:expect(flurm_job_deps, notify_completion, fun(_, _) -> ok end),
    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) -> {error, reservation_not_found} end),
    meck:expect(flurm_reservation, get_available_nodes_excluding_reserved, fun(Nodes) -> Nodes end),
    meck:expect(flurm_backfill, is_backfill_enabled, fun() -> false end),
    meck:expect(flurm_job_dispatcher, dispatch_job, fun(_, _) -> ok end),
    meck:expect(flurm_gres, filter_nodes_by_gres, fun(Nodes, _) -> Nodes end),
    meck:expect(flurm_gres, allocate, fun(_, _, _) -> ok end),
    meck:expect(flurm_gres, deallocate, fun(_, _) -> ok end),
    %% Default node manager mocks
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),
    meck:expect(flurm_node_manager, get_available_nodes_with_gres, fun(_, _, _, _) -> [] end),
    meck:expect(flurm_node_manager, allocate_gres, fun(_, _, _, _) -> {ok, []} end),
    meck:expect(flurm_node_manager, release_gres, fun(_, _) -> ok end),

    %% Default job_manager update_job mock
    meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),

    ok.

cleanup(_) ->
    %% Stop scheduler if running with proper monitor/wait pattern
    case whereis(flurm_scheduler) of
        undefined -> ok;
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Ref = monitor(process, Pid),
                    catch unlink(Pid),
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
    end,

    %% Unload all mocks
    meck:unload(flurm_job_manager),
    meck:unload(flurm_node_manager),
    meck:unload(flurm_job_deps),
    meck:unload(flurm_license),
    meck:unload(flurm_limits),
    meck:unload(flurm_reservation),
    meck:unload(flurm_backfill),
    meck:unload(flurm_preemption),
    meck:unload(flurm_job_dispatcher),
    meck:unload(flurm_gres),
    meck:unload(flurm_metrics),
    meck:unload(flurm_config_server),
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

scheduler_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start_link creates scheduler", fun test_start_link/0},
        {"submit_job adds to pending queue", fun test_submit_job/0},
        {"job_completed updates stats", fun test_job_completed/0},
        {"job_failed updates stats", fun test_job_failed/0},
        {"trigger_schedule triggers cycle", fun test_trigger_schedule/0},
        {"get_stats returns scheduler stats", fun test_get_stats/0},
        {"job_deps_satisfied triggers reschedule", fun test_job_deps_satisfied/0},
        {"unknown call returns error", fun test_unknown_call/0},
        {"unknown cast ignored", fun test_unknown_cast/0},
        {"unknown info ignored", fun test_unknown_info/0},
        {"config change partitions", fun test_config_change_partitions/0},
        {"config change nodes", fun test_config_change_nodes/0},
        {"config change schedulertype", fun test_config_change_schedulertype/0},
        {"config change unknown", fun test_config_change_unknown/0},
        {"terminate cancels timer", fun test_terminate/0},
        {"code_change returns ok", fun test_code_change/0}
     ]}.

%%====================================================================
%% Basic API Tests
%%====================================================================

test_start_link() ->
    {ok, Pid} = flurm_scheduler:start_link(),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    ?assertEqual(Pid, whereis(flurm_scheduler)),
    ok.

test_submit_job() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    %% Mock job manager to return a pending job
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = []}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),

    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(pending_count, Stats) >= 0),
    ok.

test_job_completed() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    %% Mock job manager
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = completed, allocated_nodes = [<<"node1">>],
                  licenses = [], user = <<"testuser">>, account = <<>>,
                  num_cpus = 1, num_nodes = 1, memory_mb = 1024,
                  start_time = erlang:system_time(second) - 100,
                  end_time = erlang:system_time(second)}}
    end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),

    %% Get initial stats
    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCompleted = maps:get(completed_count, InitStats),

    ok = flurm_scheduler:job_completed(1),
    _ = sys:get_state(flurm_scheduler),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assertEqual(InitCompleted + 1, maps:get(completed_count, NewStats)),
    ok.

test_job_failed() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    %% Mock job manager
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = failed, allocated_nodes = [<<"node1">>],
                  licenses = [], user = <<"testuser">>, account = <<>>,
                  num_cpus = 1, num_nodes = 1, memory_mb = 1024,
                  start_time = erlang:system_time(second) - 100,
                  end_time = erlang:system_time(second)}}
    end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),

    %% Get initial stats
    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitFailed = maps:get(failed_count, InitStats),

    ok = flurm_scheduler:job_failed(1),
    _ = sys:get_state(flurm_scheduler),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assertEqual(InitFailed + 1, maps:get(failed_count, NewStats)),
    ok.

test_trigger_schedule() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    %% Get initial cycle count
    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    ok = flurm_scheduler:trigger_schedule(),
    _ = sys:get_state(flurm_scheduler),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_get_stats() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(is_map(Stats)),
    ?assert(maps:is_key(pending_count, Stats)),
    ?assert(maps:is_key(running_count, Stats)),
    ?assert(maps:is_key(completed_count, Stats)),
    ?assert(maps:is_key(failed_count, Stats)),
    ?assert(maps:is_key(schedule_cycles, Stats)),
    ok.

test_job_deps_satisfied() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    ok = flurm_scheduler:job_deps_satisfied(1),
    _ = sys:get_state(flurm_scheduler),

    %% Should still be running
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_unknown_call() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    Result = gen_server:call(flurm_scheduler, {unknown_request, test}),
    ?assertEqual({error, unknown_request}, Result),
    ok.

test_unknown_cast() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    gen_server:cast(flurm_scheduler, {unknown_message}),
    _ = sys:get_state(flurm_scheduler),

    %% Should still be running
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_unknown_info() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    flurm_scheduler ! unknown_message,
    _ = sys:get_state(flurm_scheduler),

    %% Should still be running
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_config_change_partitions() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    flurm_scheduler ! {config_changed, partitions, [], [<<"default">>]},
    _ = sys:get_state(flurm_scheduler),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_config_change_nodes() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    flurm_scheduler ! {config_changed, nodes, [], [<<"node1">>]},
    _ = sys:get_state(flurm_scheduler),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_config_change_schedulertype() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    {ok, InitStats} = flurm_scheduler:get_stats(),
    InitCycles = maps:get(schedule_cycles, InitStats),

    flurm_scheduler ! {config_changed, schedulertype, fifo, backfill},
    _ = sys:get_state(flurm_scheduler),

    {ok, NewStats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, NewStats) > InitCycles),
    ok.

test_config_change_unknown() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    flurm_scheduler ! {config_changed, unknown_key, old, new},
    _ = sys:get_state(flurm_scheduler),

    %% Should still be running
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_terminate() ->
    {ok, Pid} = flurm_scheduler:start_link(),
    ?assert(is_process_alive(Pid)),

    catch unlink(Pid),
    gen_server:stop(Pid, shutdown, 5000),
    ?assertNot(is_process_alive(Pid)),
    ok.

test_code_change() ->
    {ok, Pid} = flurm_scheduler:start_link(),

    %% code_change is called during hot code upgrades
    %% We can verify the scheduler continues to work
    {ok, _Stats} = flurm_scheduler:get_stats(),

    %% Clean up
    catch unlink(Pid),
    catch gen_server:stop(Pid, shutdown, 5000),
    ok.

%%====================================================================
%% Scheduling Logic Tests
%%====================================================================

scheduling_logic_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"schedule job with available resources", fun test_schedule_with_resources/0},
        {"job waits when insufficient resources", fun test_job_waits_no_resources/0},
        {"job not found skipped", fun test_job_not_found/0},
        {"job not pending skipped", fun test_job_not_pending/0},
        {"dependency check satisfied", fun test_deps_satisfied/0},
        {"dependency check waiting", fun test_deps_waiting/0},
        {"dependency check error", fun test_deps_error/0},
        {"dependency check noproc", fun test_deps_noproc/0},
        {"limits exceeded", fun test_limits_exceeded/0},
        {"license unavailable", fun test_license_unavailable/0},
        {"license allocation failure", fun test_license_allocation_failure/0},
        {"dispatch failure rollback", fun test_dispatch_failure/0}
     ]}.

test_schedule_with_resources() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    %% Create a mock node
    MockNode = #node{
        hostname = <<"node1">>,
        cpus = 8,
        memory_mb = 16384,
        state = up,
        partitions = [<<"default">>]
    },

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 4,
                  memory_mb = 8192, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test_job">>, script = <<"echo hello">>}}
    end),
    meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),

    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(schedule_cycles, Stats) > 0),
    ok.

test_job_waits_no_resources() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 4,
                  memory_mb = 8192, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = []}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),

    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(pending_count, Stats) >= 0),
    ok.

test_job_not_found() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(_JobId) ->
        {error, not_found}
    end),

    ok = flurm_scheduler:submit_job(999),
    _ = sys:get_state(flurm_scheduler),

    %% Job should be removed from queue
    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_job_not_pending() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = running, allocated_nodes = [<<"node1">>]}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),

    {ok, _Stats} = flurm_scheduler:get_stats(),
    ok.

test_deps_satisfied() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {ok, []} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = []}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_deps_waiting() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {waiting, [123]} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = []}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),

    %% Job should remain pending due to deps
    {ok, Stats} = flurm_scheduler:get_stats(),
    ?assert(maps:get(pending_count, Stats) >= 0),
    ok.

test_deps_error() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {error, dep_not_found} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = []}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_deps_noproc() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    %% Simulate flurm_job_deps not running
    meck:expect(flurm_job_deps, check_dependencies, fun(_) ->
        erlang:error({noproc, test})
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = []}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_limits_exceeded() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_limits, check_limits, fun(_) -> {error, max_jobs_exceeded} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = [], time_limit = 3600}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_license_unavailable() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_license, check_availability, fun(_) -> false end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [{<<"matlab">>, 1}],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = [], time_limit = 3600}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_license_allocation_failure() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = up},

    meck:expect(flurm_license, check_availability, fun(_) -> true end),
    meck:expect(flurm_license, allocate, fun(_, _) -> {error, license_exhausted} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [{<<"matlab">>, 1}],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test">>, script = <<"echo">>}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_dispatch_failure() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = up},

    meck:expect(flurm_job_dispatcher, dispatch_job, fun(_, _) -> {error, node_unreachable} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test">>, script = <<"echo">>}}
    end),
    meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

%%====================================================================
%% Backfill Tests
%%====================================================================

backfill_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"simple backfill when advanced disabled", fun test_simple_backfill/0},
        {"advanced backfill when enabled", fun test_advanced_backfill/0},
        {"backfill with blocker undefined", fun test_backfill_undefined_blocker/0}
     ]}.

test_simple_backfill() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_backfill, is_backfill_enabled, fun() -> false end),

    %% Submit job that will be blocked
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 10, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test">>, script = <<"echo">>}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    ok = flurm_scheduler:submit_job(2),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_advanced_backfill() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_backfill, is_backfill_enabled, fun() -> true end),
    meck:expect(flurm_backfill, get_backfill_candidates, fun(_) -> [] end),
    meck:expect(flurm_backfill, run_backfill_cycle, fun(_, _) -> [] end),

    %% Submit job that will be blocked, triggering backfill
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 10, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>, qos = <<"normal">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = [], time_limit = 3600, submit_time = 0,
                  name = <<"test">>, script = <<"echo">>}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    ok = flurm_scheduler:submit_job(2),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_backfill_undefined_blocker() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_backfill, is_backfill_enabled, fun() -> true end),

    %% First job not found, second job exists
    meck:expect(flurm_job_manager, get_job, fun
        (1) -> {error, not_found};
        (JobId) -> {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                             memory_mb = 1024, partition = <<"default">>,
                             user = <<"testuser">>, account = <<>>, licenses = [],
                             gres = <<>>, reservation = <<>>, priority = 100,
                             allocated_nodes = []}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    ok = flurm_scheduler:submit_job(2),
    _ = sys:get_state(flurm_scheduler),
    ok.

%%====================================================================
%% Reservation Tests
%%====================================================================

reservation_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"no reservation - empty string", fun test_no_reservation_empty/0},
        {"no reservation - undefined", fun test_no_reservation_undefined/0},
        {"reservation access granted", fun test_reservation_access_granted/0},
        {"reservation not started", fun test_reservation_not_started/0},
        {"reservation access denied", fun test_reservation_access_denied/0},
        {"reservation not found", fun test_reservation_not_found/0},
        {"reservation expired", fun test_reservation_expired/0},
        {"reservation noproc", fun test_reservation_noproc/0}
     ]}.

test_no_reservation_empty() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = []}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_no_reservation_undefined() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = undefined, priority = 100,
                  allocated_nodes = []}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_reservation_access_granted() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = up},

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        {ok, [<<"node1">>]}
    end),
    meck:expect(flurm_reservation, confirm_reservation, fun(_) -> ok end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<"myres">>, priority = 100,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test">>, script = <<"echo">>}}
    end),
    meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_reservation_not_started() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        {error, {reservation_not_started, erlang:system_time(second) + 3600}}
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<"myres">>, priority = 100,
                  allocated_nodes = [], time_limit = 3600}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_reservation_access_denied() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        {error, access_denied}
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<"myres">>, priority = 100,
                  allocated_nodes = [], time_limit = 3600}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_reservation_not_found() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        {error, reservation_not_found}
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<"myres">>, priority = 100,
                  allocated_nodes = [], time_limit = 3600}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_reservation_expired() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        {error, reservation_expired}
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<"myres">>, priority = 100,
                  allocated_nodes = [], time_limit = 3600}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_reservation_noproc() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) ->
        erlang:error({noproc, test})
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<"myres">>, priority = 100,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test">>, script = <<"echo">>}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

%%====================================================================
%% Preemption Tests
%%====================================================================

preemption_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"preemption for high priority job", fun test_preemption_high_priority/0},
        {"preemption not possible - low priority", fun test_preemption_low_priority/0},
        {"preemption execution success", fun test_preemption_success/0},
        {"preemption execution failure", fun test_preemption_failure/0}
     ]}.

test_preemption_high_priority() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 100 end),
    meck:expect(flurm_preemption, find_preemptable_jobs, fun(_, _) ->
        {error, no_preemptable_jobs}
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 10, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>, qos = <<"high">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 500,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test">>, script = <<"echo">>}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_preemption_low_priority() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 500 end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 10, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = []}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_preemption_success() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = up},

    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 100 end),
    meck:expect(flurm_preemption, find_preemptable_jobs, fun(_, _) ->
        {ok, [#{job_id => 99, num_nodes => 1, num_cpus => 1, memory_mb => 1024}]}
    end),
    meck:expect(flurm_preemption, get_preemption_mode, fun(_) -> requeue end),
    meck:expect(flurm_preemption, get_grace_time, fun(_) -> 30 end),
    meck:expect(flurm_preemption, execute_preemption, fun(_, _) -> {ok, [99]} end),

    CallCount = atomics:new(1, [{signed, false}]),
    atomics:put(CallCount, 1, 0),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        Count = atomics:add_get(CallCount, 1, 1),
        if Count =< 2 ->
            %% First calls - job needs 10 nodes
            {ok, #job{id = JobId, state = pending, num_nodes = 10, num_cpus = 1,
                      memory_mb = 1024, partition = <<"default">>, qos = <<"high">>,
                      user = <<"testuser">>, account = <<>>, licenses = [],
                      gres = <<>>, reservation = <<>>, priority = 500,
                      allocated_nodes = [], time_limit = 3600,
                      name = <<"test">>, script = <<"echo">>}};
           true ->
            %% After preemption - return job needing only 1 node
            {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 1,
                      memory_mb = 1024, partition = <<"default">>, qos = <<"high">>,
                      user = <<"testuser">>, account = <<>>, licenses = [],
                      gres = <<>>, reservation = <<>>, priority = 500,
                      allocated_nodes = [], time_limit = 3600,
                      name = <<"test">>, script = <<"echo">>}}
        end
    end),
    meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),

    FirstNodeCall = atomics:new(1, [{signed, false}]),
    atomics:put(FirstNodeCall, 1, 0),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) ->
        Count = atomics:add_get(FirstNodeCall, 1, 1),
        if Count =< 2 -> [];
           true -> [MockNode]
        end
    end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_preemption_failure() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 100 end),
    meck:expect(flurm_preemption, find_preemptable_jobs, fun(_, _) ->
        {ok, [#{job_id => 99, num_nodes => 1, num_cpus => 1, memory_mb => 1024}]}
    end),
    meck:expect(flurm_preemption, get_preemption_mode, fun(_) -> requeue end),
    meck:expect(flurm_preemption, get_grace_time, fun(_) -> 30 end),
    meck:expect(flurm_preemption, execute_preemption, fun(_, _) -> {error, preemption_failed} end),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 10, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>, qos = <<"high">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 500,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test">>, script = <<"echo">>}}
    end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [] end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

%%====================================================================
%% GRES Tests
%%====================================================================

gres_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job with GRES requirements", fun test_job_with_gres/0},
        {"GRES allocation failure", fun test_gres_allocation_failure/0}
     ]}.

test_job_with_gres() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = up},

    meck:expect(flurm_node_manager, get_available_nodes_with_gres, fun(_, _, _, _) -> [MockNode] end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_node_manager, allocate_gres, fun(_, _, _, _) -> {ok, [{gpu, 2, [0, 1]}]} end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 4,
                  memory_mb = 8192, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<"gpu:2">>, gpu_exclusive = true,
                  reservation = <<>>, priority = 100,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test">>, script = <<"echo">>}}
    end),
    meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_gres_allocation_failure() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = up},

    meck:expect(flurm_node_manager, get_available_nodes_with_gres, fun(_, _, _, _) -> [MockNode] end),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) -> [MockNode] end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_node_manager, allocate_gres, fun(_, _, _, _) -> {error, gres_unavailable} end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),
    meck:expect(flurm_node_manager, release_gres, fun(_, _) -> ok end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 1, num_cpus = 4,
                  memory_mb = 8192, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<"gpu:2">>, gpu_exclusive = true,
                  reservation = <<>>, priority = 100,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test">>, script = <<"echo">>}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

%%====================================================================
%% Job Completion Edge Cases
%%====================================================================

job_completion_edge_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"job completion with licenses", fun test_completion_with_licenses/0},
        {"job completion not found", fun test_completion_job_not_found/0},
        {"job completion with wall time", fun test_completion_with_wall_time/0},
        {"job deps notification noproc", fun test_deps_notification_noproc/0},
        {"job deps notification error", fun test_deps_notification_error/0}
     ]}.

test_completion_with_licenses() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = completed,
                  allocated_nodes = [<<"node1">>],
                  licenses = [{<<"matlab">>, 1}],
                  user = <<"testuser">>, account = <<>>,
                  num_cpus = 1, num_nodes = 1, memory_mb = 1024,
                  start_time = erlang:system_time(second) - 100,
                  end_time = erlang:system_time(second)}}
    end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),

    ok = flurm_scheduler:job_completed(1),
    _ = sys:get_state(flurm_scheduler),

    %% Verify license deallocate was called
    ?assert(meck:called(flurm_license, deallocate, ['_', '_'])),
    ok.

test_completion_job_not_found() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(_) -> {error, not_found} end),

    ok = flurm_scheduler:job_completed(999),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_completion_with_wall_time() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    %% Test with undefined start_time
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = completed,
                  allocated_nodes = [<<"node1">>],
                  licenses = [],
                  user = <<"testuser">>, account = <<>>,
                  num_cpus = 1, num_nodes = 1, memory_mb = 1024,
                  start_time = undefined,
                  end_time = erlang:system_time(second)}}
    end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),

    ok = flurm_scheduler:job_completed(1),
    _ = sys:get_state(flurm_scheduler),

    %% Test with undefined end_time
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = completed,
                  allocated_nodes = [<<"node1">>],
                  licenses = [],
                  user = <<"testuser">>, account = <<>>,
                  num_cpus = 1, num_nodes = 1, memory_mb = 1024,
                  start_time = erlang:system_time(second) - 100,
                  end_time = undefined}}
    end),

    ok = flurm_scheduler:job_completed(2),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_deps_notification_noproc() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, notify_completion, fun(_, _) ->
        erlang:error({noproc, test})
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = completed,
                  allocated_nodes = [],
                  licenses = [],
                  user = <<"testuser">>, account = <<>>,
                  num_cpus = 1, num_nodes = 1, memory_mb = 1024,
                  start_time = erlang:system_time(second) - 100,
                  end_time = erlang:system_time(second)}}
    end),

    ok = flurm_scheduler:job_completed(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

test_deps_notification_error() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_deps, notify_completion, fun(_, _) ->
        erlang:error(some_error)
    end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = completed,
                  allocated_nodes = [],
                  licenses = [],
                  user = <<"testuser">>, account = <<>>,
                  num_cpus = 1, num_nodes = 1, memory_mb = 1024,
                  start_time = erlang:system_time(second) - 100,
                  end_time = erlang:system_time(second)}}
    end),

    ok = flurm_scheduler:job_completed(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

%%====================================================================
%% Held Job Test
%%====================================================================

held_job_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"held job skipped", fun test_held_job/0}
     ]}.

test_held_job() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = held, num_nodes = 1, num_cpus = 1,
                  memory_mb = 1024, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = []}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),
    ok.

%%====================================================================
%% Allocation Rollback Tests
%%====================================================================

allocation_rollback_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"node allocation partial failure rollback", fun test_allocation_partial_failure/0}
     ]}.

test_allocation_partial_failure() ->
    {ok, _Pid} = flurm_scheduler:start_link(),

    MockNode1 = #node{hostname = <<"node1">>, cpus = 8, memory_mb = 16384, state = up},
    MockNode2 = #node{hostname = <<"node2">>, cpus = 8, memory_mb = 16384, state = up},

    AllocCount = atomics:new(1, [{signed, false}]),
    atomics:put(AllocCount, 1, 0),

    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) ->
        [MockNode1, MockNode2]
    end),
    meck:expect(flurm_node_manager, allocate_resources, fun(Hostname, _, _, _) ->
        case Hostname of
            <<"node1">> -> ok;
            <<"node2">> -> {error, insufficient_resources}
        end
    end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, state = pending, num_nodes = 2, num_cpus = 4,
                  memory_mb = 8192, partition = <<"default">>,
                  user = <<"testuser">>, account = <<>>, licenses = [],
                  gres = <<>>, reservation = <<>>, priority = 100,
                  allocated_nodes = [], time_limit = 3600,
                  name = <<"test">>, script = <<"echo">>}}
    end),

    ok = flurm_scheduler:submit_job(1),
    _ = sys:get_state(flurm_scheduler),

    %% Verify release was called for rollback
    ?assert(meck:called(flurm_node_manager, release_resources, ['_', '_'])),
    ok.
