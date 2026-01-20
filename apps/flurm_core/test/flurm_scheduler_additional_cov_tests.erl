%%%-------------------------------------------------------------------
%%% @doc Coverage Tests for FLURM Scheduler
%%%
%%% Tests the main scheduler functionality with mocked dependencies.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_scheduler_additional_cov_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%%===================================================================
%%% Test Generators
%%%===================================================================

scheduler_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
      fun per_test_setup/0,
      fun per_test_cleanup/1,
      [
       fun basic_api_tests/1,
       fun job_submission_tests/1,
       fun job_completion_tests/1,
       fun scheduling_tests/1,
       fun message_handling_tests/1
      ]}}.

setup() ->
    %% Mock lager
    meck:new(lager, [non_strict, no_link]),
    meck:expect(lager, debug, fun(_) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    %% Mock flurm_config_server
    meck:new(flurm_config_server, [non_strict, no_link]),
    meck:expect(flurm_config_server, subscribe_changes, fun(_) -> ok end),
    meck:expect(flurm_config_server, get, fun(_, Default) -> Default end),

    %% Mock flurm_metrics
    meck:new(flurm_metrics, [non_strict, no_link]),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, histogram, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, gauge, fun(_, _) -> ok end),

    %% Mock flurm_job_manager
    meck:new(flurm_job_manager, [non_strict, no_link]),
    meck:expect(flurm_job_manager, get_job, fun(JobId) ->
        {ok, #job{id = JobId, name = <<"test_job">>, state = pending,
                  script = <<"#!/bin/bash\necho test">>,
                  partition = <<"default">>, num_cpus = 1, memory_mb = 1024,
                  allocated_nodes = [], licenses = []}}
    end),
    meck:expect(flurm_job_manager, update_job, fun(_, _) -> ok end),

    %% Mock flurm_node_manager
    meck:new(flurm_node_manager, [non_strict, no_link]),
    meck:expect(flurm_node_manager, get_available_nodes_for_job, fun(_, _, _) ->
        [#node{hostname = <<"node01">>, cpus = 8, memory_mb = 16384, state = idle}]
    end),
    meck:expect(flurm_node_manager, get_available_nodes_with_gres, fun(_, _, _, _) ->
        [#node{hostname = <<"node01">>, cpus = 8, memory_mb = 16384, state = idle}]
    end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),

    %% Mock flurm_job_deps
    meck:new(flurm_job_deps, [non_strict, no_link]),
    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> ok end),
    meck:expect(flurm_job_deps, notify_completion, fun(_, _) -> ok end),

    %% Mock flurm_limits
    meck:new(flurm_limits, [non_strict, no_link]),
    meck:expect(flurm_limits, check_limits, fun(_) -> ok end),
    meck:expect(flurm_limits, enforce_limit, fun(_, _, _) -> ok end),

    %% Mock flurm_job_dispatcher
    meck:new(flurm_job_dispatcher, [non_strict, no_link]),
    meck:expect(flurm_job_dispatcher, dispatch_job, fun(_, _) -> ok end),

    %% Mock flurm_backfill
    meck:new(flurm_backfill, [non_strict, no_link]),
    meck:expect(flurm_backfill, is_backfill_enabled, fun() -> false end),
    meck:expect(flurm_backfill, get_backfill_candidates, fun(_) -> [] end),
    meck:expect(flurm_backfill, run_backfill_cycle, fun(_, _) -> [] end),

    %% Mock flurm_reservation
    meck:new(flurm_reservation, [non_strict, no_link]),
    meck:expect(flurm_reservation, check_reservation_access, fun(_, _) -> ok end),
    meck:expect(flurm_reservation, get_available_nodes_excluding_reserved, fun(Nodes) -> Nodes end),
    meck:expect(flurm_reservation, confirm_reservation, fun(_) -> ok end),

    %% Mock flurm_preemption
    meck:new(flurm_preemption, [non_strict, no_link]),
    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 0 end),
    meck:expect(flurm_preemption, find_preemptable_jobs, fun(_, _) -> {error, none_available} end),
    meck:expect(flurm_preemption, get_preemption_mode, fun(_) -> off end),
    meck:expect(flurm_preemption, get_grace_time, fun(_) -> 0 end),
    meck:expect(flurm_preemption, execute_preemption, fun(_, _) -> ok end),

    %% Mock flurm_license
    meck:new(flurm_license, [non_strict, no_link]),
    meck:expect(flurm_license, deallocate, fun(_, _) -> ok end),

    %% Mock flurm_gres
    meck:new(flurm_gres, [non_strict, no_link]),
    meck:expect(flurm_gres, filter_nodes_by_gres, fun(Nodes, _) -> Nodes end),

    ok.

cleanup(_) ->
    catch meck:unload(lager),
    catch meck:unload(flurm_config_server),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_job_manager),
    catch meck:unload(flurm_node_manager),
    catch meck:unload(flurm_job_deps),
    catch meck:unload(flurm_limits),
    catch meck:unload(flurm_job_dispatcher),
    catch meck:unload(flurm_backfill),
    catch meck:unload(flurm_reservation),
    catch meck:unload(flurm_preemption),
    catch meck:unload(flurm_license),
    catch meck:unload(flurm_gres),
    ok.

per_test_setup() ->
    {ok, Pid} = flurm_scheduler:start_link(),
    Pid.

per_test_cleanup(Pid) ->
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
    ok.

%%%===================================================================
%%% Basic API Tests
%%%===================================================================

basic_api_tests(_Pid) ->
    [
        {"start_link creates process", fun() ->
            ?assert(is_pid(whereis(flurm_scheduler)))
        end},

        {"get_stats returns tuple with map", fun() ->
            Result = flurm_scheduler:get_stats(),
            ?assertMatch({ok, _}, Result),
            {ok, Stats} = Result,
            ?assert(is_map(Stats))
        end},

        {"trigger_schedule triggers cycle", fun() ->
            ?assertEqual(ok, flurm_scheduler:trigger_schedule())
        end}
    ].

%%%===================================================================
%%% Job Submission Tests
%%%===================================================================

job_submission_tests(_Pid) ->
    [
        {"submit_job with JobId", fun() ->
            %% submit_job takes a JobId integer, not a record
            Result = flurm_scheduler:submit_job(12345),
            ?assertEqual(ok, Result)
        end},

        {"submit_job queues job", fun() ->
            %% submit_job returns ok (async cast)
            Result = flurm_scheduler:submit_job(12346),
            ?assertEqual(ok, Result)
        end}
    ].

%%%===================================================================
%%% Job Completion Tests
%%%===================================================================

job_completion_tests(_Pid) ->
    [
        {"job_completed for job", fun() ->
            %% Submit a job first (by JobId)
            flurm_scheduler:submit_job(12350),
            %% Mark it as completed
            Result = flurm_scheduler:job_completed(12350),
            ?assertEqual(ok, Result)
        end},

        {"job_failed for job", fun() ->
            %% Submit a job first (by JobId)
            flurm_scheduler:submit_job(12351),
            %% Mark it as failed
            Result = flurm_scheduler:job_failed(12351),
            ?assertEqual(ok, Result)
        end},

        {"job_deps_satisfied notifies scheduler", fun() ->
            %% job_deps_satisfied is a cast that returns ok
            Result = flurm_scheduler:job_deps_satisfied(12345),
            ?assertEqual(ok, Result)
        end}
    ].

%%%===================================================================
%%% Scheduling Tests
%%%===================================================================

scheduling_tests(_Pid) ->
    [
        {"schedule cycle processes pending jobs", fun() ->
            %% Submit some jobs by JobId
            flurm_scheduler:submit_job(12360),
            flurm_scheduler:submit_job(12361),
            %% Trigger scheduling
            ?assertEqual(ok, flurm_scheduler:trigger_schedule()),
            %% Allow time for async processing
            timer:sleep(100)
        end}
    ].

%%%===================================================================
%%% Message Handling Tests
%%%===================================================================

message_handling_tests(Pid) ->
    [
        {"handle_info schedule_cycle", fun() ->
            Pid ! schedule_cycle,
            timer:sleep(50),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_info config_changed", fun() ->
            Pid ! {config_changed, partitions, []},
            timer:sleep(10),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_info unknown message", fun() ->
            Pid ! unknown_message,
            timer:sleep(10),
            ?assert(is_process_alive(Pid))
        end},

        {"handle_cast unknown message", fun() ->
            gen_server:cast(Pid, unknown_cast),
            timer:sleep(10),
            ?assert(is_process_alive(Pid))
        end}
    ].
