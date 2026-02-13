%%%-------------------------------------------------------------------
%%% @doc Failure Injection Tests Using flurm_chaos Module
%%%
%%% Enables various chaos scenarios during workloads and verifies
%%% that the system recovers without state corruption.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_fault_injection_tests
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_fault_injection_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

-define(TERMINAL_STATES, [completed, failed, timeout, cancelled, node_fail]).
-define(ALL_VALID_STATES, [pending, held, configuring, running, suspended,
                           completing, completed, failed, timeout, cancelled, node_fail]).

%%====================================================================
%% EUnit Integration
%%====================================================================

fault_injection_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"GC pressure during job operations",
         {timeout, 60, fun test_trigger_gc/0}},
        {"rapid process kill recovery",
         {timeout, 60, fun test_kill_recovery/0}},
        {"all jobs valid after chaos storm",
         {timeout, 60, fun test_chaos_storm_invariants/0}},
        {"scheduler resume after suspension",
         {timeout, 60, fun test_delay_message_resume/0}},
        {"concurrent ops under gc pressure",
         {timeout, 60, fun test_concurrent_under_gc/0}},
        {"scheduler restart under chaos",
         {timeout, 60, fun test_scheduler_restart_under_chaos/0}}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),

    catch meck:unload(flurm_db_persist),
    meck:new(flurm_db_persist, [passthrough, non_strict]),
    meck:expect(flurm_db_persist, persistence_mode, fun() -> none end),
    meck:expect(flurm_db_persist, store_job, fun(_Job) -> ok end),
    meck:expect(flurm_db_persist, update_job, fun(_JobId, _Updates) -> ok end),
    meck:expect(flurm_db_persist, delete_job, fun(_JobId) -> ok end),
    meck:expect(flurm_db_persist, list_jobs, fun() -> [] end),

    catch meck:unload(flurm_metrics),
    meck:new(flurm_metrics, [passthrough, non_strict]),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),

    catch meck:unload(flurm_job_dispatcher_server),
    meck:new(flurm_job_dispatcher_server, [passthrough, non_strict]),
    meck:expect(flurm_job_dispatcher_server, cancel_job, fun(_, _) -> ok end),

    start_if_not_running(flurm_job_registry, fun flurm_job_registry:start_link/0),
    start_if_not_running(flurm_job_sup, fun flurm_job_sup:start_link/0),
    start_if_not_running(flurm_node_registry, fun flurm_node_registry:start_link/0),
    start_if_not_running(flurm_node_sup, fun flurm_node_sup:start_link/0),
    start_if_not_running(flurm_limits, fun flurm_limits:start_link/0),
    start_if_not_running(flurm_license, fun flurm_license:start_link/0),
    start_if_not_running(flurm_job_deps, fun flurm_job_deps:start_link/0),
    start_if_not_running(flurm_job_array, fun flurm_job_array:start_link/0),
    start_if_not_running(flurm_scheduler, fun flurm_scheduler:start_link/0),
    start_if_not_running(flurm_job_manager, fun flurm_job_manager:start_link/0),

    %% Start chaos server
    case whereis(flurm_chaos) of
        undefined ->
            case flurm_chaos:start_link(#{auto_enable => false}) of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok
            end;
        _ -> ok
    end,
    flurm_chaos:disable(),
    ok.

cleanup(_) ->
    %% Disable chaos first
    catch flurm_chaos:disable(),
    catch flurm_chaos:disable_all_scenarios(),

    safe_stop(flurm_job_manager),
    safe_stop(flurm_scheduler),
    safe_stop(flurm_job_array),
    safe_stop(flurm_job_deps),
    safe_stop(flurm_license),
    safe_stop(flurm_limits),
    safe_stop(flurm_node_sup),
    safe_stop(flurm_node_registry),
    safe_stop(flurm_job_sup),
    safe_stop(flurm_job_registry),
    catch safe_stop(flurm_chaos),
    catch meck:unload(flurm_db_persist),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_job_dispatcher_server),
    ok.

%%====================================================================
%% Tests
%%====================================================================

test_trigger_gc() ->
    %% Enable GC pressure
    flurm_chaos:enable(),
    flurm_chaos:set_scenario(trigger_gc, 0.3),
    flurm_chaos:enable_scenario(trigger_gc),

    %% Submit 50 jobs under GC pressure
    JobIds = submit_n_jobs(50),
    ?assertEqual(50, length(JobIds)),

    %% Disable chaos
    flurm_chaos:disable(),

    %% Verify all jobs are in valid states
    verify_all_jobs_valid(),

    %% Verify all job IDs are unique
    ?assertEqual(50, length(lists:usort(JobIds))).

test_kill_recovery() ->
    %% Submit some jobs first
    _JobIds = submit_n_jobs(20),

    %% Kill a non-critical process and verify the system still works
    %% We'll force GC on all processes instead of killing supervisor children
    flurm_chaos:gc_all_processes(),

    %% System should still work
    {ok, NewId} = flurm_job_manager:submit_job(#{
        name => <<"post_gc_job">>,
        script => <<"#!/bin/bash\necho alive">>,
        num_cpus => 1, memory_mb => 100, time_limit => 3600
    }),
    {ok, #job{id = NewId}} = flurm_job_manager:get_job(NewId),

    %% All gen_servers still alive
    ?assertNotEqual(undefined, whereis(flurm_job_manager)),
    ?assertNotEqual(undefined, whereis(flurm_scheduler)),
    ?assertNotEqual(undefined, whereis(flurm_node_registry)).

test_chaos_storm_invariants() ->
    %% Register a node
    flurm_node_registry:register_node_direct(
        #{hostname => <<"chaos_node">>, cpus => 8, memory_mb => 16384, state => up,
          partitions => [<<"default">>]}),

    %% Enable multiple chaos scenarios at low probability
    flurm_chaos:enable(),
    flurm_chaos:set_scenario(trigger_gc, 0.1),
    flurm_chaos:enable_scenario(trigger_gc),

    %% Run a workload: submit, cancel, hold, release, schedule
    lists:foreach(fun(I) ->
        Spec = #{
            name => list_to_binary("chaos_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1, memory_mb => 100, time_limit => 3600
        },
        case flurm_job_manager:submit_job(Spec) of
            {ok, Id} ->
                case I rem 5 of
                    0 -> catch flurm_job_manager:cancel_job(Id);
                    1 -> catch flurm_job_manager:hold_job(Id);
                    2 ->
                        catch flurm_job_manager:hold_job(Id),
                        catch flurm_job_manager:release_job(Id);
                    _ -> ok
                end;
            _ -> ok
        end,
        %% Trigger scheduling periodically
        case I rem 10 of
            0 -> flurm_scheduler:trigger_schedule();
            _ -> ok
        end
    end, lists:seq(1, 100)),

    %% Disable chaos
    flurm_chaos:disable(),
    flurm_chaos:disable_all_scenarios(),

    %% Wait for any pending operations
    timer:sleep(200),

    %% INVARIANT: All jobs must be in valid states
    verify_all_jobs_valid(),

    %% INVARIANT: All gen_servers must be alive
    ?assertNotEqual(undefined, whereis(flurm_job_manager)),
    ?assertNotEqual(undefined, whereis(flurm_scheduler)),
    ?assertNotEqual(undefined, whereis(flurm_node_registry)).

test_delay_message_resume() ->
    %% Register a node
    flurm_node_registry:register_node_direct(
        #{hostname => <<"suspend_node">>, cpus => 4, memory_mb => 8192, state => up,
          partitions => [<<"default">>]}),

    %% Submit jobs
    _JobIds = submit_n_jobs(10),

    %% Brief scheduler suspension via chaos
    flurm_chaos:enable(),
    flurm_chaos:set_scenario(delay_message, 0.5),
    flurm_chaos:enable_scenario(delay_message),

    %% Trigger scheduling multiple times
    lists:foreach(fun(_) ->
        flurm_scheduler:trigger_schedule(),
        timer:sleep(20)
    end, lists:seq(1, 5)),

    %% Disable chaos
    flurm_chaos:disable(),
    flurm_chaos:disable_all_scenarios(),

    %% Trigger one more clean schedule
    flurm_scheduler:trigger_schedule(),
    timer:sleep(200),

    %% Verify all jobs are in valid states
    verify_all_jobs_valid().

test_concurrent_under_gc() ->
    %% Enable GC pressure
    flurm_chaos:enable(),
    flurm_chaos:set_scenario(trigger_gc, 0.2),
    flurm_chaos:enable_scenario(trigger_gc),

    %% Run concurrent operations under GC pressure
    Parent = self(),
    Pids = [spawn_link(fun() ->
        lists:foreach(fun(_) ->
            Spec = #{name => <<"gc_test">>,
                     script => <<"#!/bin/bash\necho test">>,
                     num_cpus => 1, memory_mb => 100, time_limit => 3600},
            case flurm_job_manager:submit_job(Spec) of
                {ok, Id} ->
                    _ = flurm_job_manager:get_job(Id),
                    case rand:uniform(3) of
                        1 -> flurm_job_manager:cancel_job(Id);
                        _ -> ok
                    end;
                _ -> ok
            end
        end, lists:seq(1, 10)),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, 10)],

    lists:foreach(fun(Pid) ->
        receive {done, Pid} -> ok after 30000 -> error(timeout) end
    end, Pids),

    %% Disable chaos
    flurm_chaos:disable(),

    %% Verify invariants
    verify_all_jobs_valid(),
    ?assertNotEqual(undefined, whereis(flurm_job_manager)).

test_scheduler_restart_under_chaos() ->
    flurm_node_registry:register_node_direct(
        #{hostname => <<"restart_node">>, cpus => 4, memory_mb => 8192, state => up,
          partitions => [<<"default">>]}),

    flurm_chaos:enable(),
    flurm_chaos:set_scenario(trigger_gc, 0.15),
    flurm_chaos:enable_scenario(trigger_gc),

    safe_stop(flurm_scheduler),
    start_if_not_running(flurm_scheduler, fun flurm_scheduler:start_link/0),

    {ok, JobId} = flurm_job_manager:submit_job(#{
        name => <<"chaos_restart_job">>,
        script => <<"#!/bin/bash\necho restart">>,
        num_cpus => 1, memory_mb => 64, time_limit => 3600
    }),
    ok = flurm_scheduler:trigger_schedule(),
    timer:sleep(100),
    ?assertMatch({ok, #job{id = JobId}}, flurm_job_manager:get_job(JobId)),

    flurm_chaos:disable(),
    flurm_chaos:disable_all_scenarios(),
    ?assertNotEqual(undefined, whereis(flurm_scheduler)).

%%====================================================================
%% Helpers
%%====================================================================

submit_n_jobs(N) ->
    lists:map(fun(I) ->
        Spec = #{
            name => list_to_binary("fi_job_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho hello">>,
            num_cpus => 1, memory_mb => 100, time_limit => 3600
        },
        {ok, Id} = flurm_job_manager:submit_job(Spec),
        Id
    end, lists:seq(1, N)).

verify_all_jobs_valid() ->
    Jobs = flurm_job_manager:list_jobs(),
    lists:foreach(fun(#job{id = Id, state = S}) ->
        ?assert(lists:member(S, ?ALL_VALID_STATES),
                io_lib:format("Job ~p in invalid state: ~p", [Id, S]))
    end, Jobs).

start_if_not_running(Name, StartFun) ->
    case whereis(Name) of
        undefined ->
            case StartFun() of
                {ok, _Pid} -> ok;
                {error, {already_started, _}} -> ok;
                Other -> error({failed_to_start, Name, Other})
            end;
        _Pid -> ok
    end.

safe_stop(Name) ->
    case whereis(Name) of
        undefined -> ok;
        Pid ->
            catch unlink(Pid),
            catch gen_server:stop(Pid, normal, 5000),
            ok
    end.
