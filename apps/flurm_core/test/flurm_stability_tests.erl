%%%-------------------------------------------------------------------
%%% @doc Long-Running Stability Tests
%%%
%%% Runs extended workloads to find memory leaks, timer accumulation,
%%% message queue growth, and process count drift.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_stability_tests
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_stability_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

stability_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"60 second continuous workload",
         {timeout, 120, fun test_sustained_workload/0}},
        {"no process leaks after churn",
         {timeout, 60, fun test_no_process_leaks/0}},
        {"ETS table sizes stay bounded",
         {timeout, 60, fun test_ets_bounded/0}}
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
    meck:expect(flurm_job_dispatcher_server, dispatch_job, fun(_, _) -> ok end),

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
    ok.

cleanup(_) ->
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
    catch meck:unload(flurm_db_persist),
    catch meck:unload(flurm_metrics),
    catch meck:unload(flurm_job_dispatcher_server),
    ok.

%%====================================================================
%% Tests
%%====================================================================

test_sustained_workload() ->
    %% Register a node for scheduling
    flurm_node_registry:register_node_direct(
        #{hostname => <<"stability_node">>, cpus => 16, memory_mb => 32768, state => up,
          partitions => [<<"default">>]}),

    %% Measure baseline
    gc_all_servers(),
    timer:sleep(100),
    MemBefore = erlang:memory(total),
    ProcessesBefore = length(erlang:processes()),
    ServersBefore = get_server_queue_lengths(),

    %% Run for 60 seconds
    Duration = 60000,
    EndTime = erlang:monotonic_time(millisecond) + Duration,
    Counter = run_workload_loop(EndTime, 0),

    %% Wait for pending operations to complete
    timer:sleep(500),

    %% Measure after
    gc_all_servers(),
    timer:sleep(100),
    MemAfter = erlang:memory(total),
    ProcessesAfter = length(erlang:processes()),
    ServersAfter = get_server_queue_lengths(),

    %% Report
    MemGrowthPct = (MemAfter - MemBefore) * 100 / max(MemBefore, 1),
    ProcessGrowth = ProcessesAfter - ProcessesBefore,

    io:format("~nStability results after ~p operations in ~ps:~n", [Counter, Duration div 1000]),
    io:format("  Memory: ~pMB -> ~pMB (~.1f% growth)~n",
              [MemBefore div (1024*1024), MemAfter div (1024*1024), MemGrowthPct]),
    io:format("  Processes: ~p -> ~p (delta: ~p)~n",
              [ProcessesBefore, ProcessesAfter, ProcessGrowth]),
    io:format("  Queue lengths before: ~p~n", [ServersBefore]),
    io:format("  Queue lengths after:  ~p~n", [ServersAfter]),

    %% Assert: memory growth < 50% (generous for test environment)
    ?assert(MemGrowthPct < 50,
            io_lib:format("Memory grew ~.1f% during sustained workload", [MemGrowthPct])),

    %% Assert: no significant process leak (allow small variance)
    ?assert(abs(ProcessGrowth) < 50,
            io_lib:format("Process count changed by ~p (leak?)", [ProcessGrowth])),

    %% Assert: no message queue > 50
    lists:foreach(fun({Name, Len}) ->
        ?assert(Len < 50,
                io_lib:format("~p message queue: ~p (too large)", [Name, Len]))
    end, ServersAfter),

    %% Assert: all gen_servers alive
    ?assertNotEqual(undefined, whereis(flurm_job_manager)),
    ?assertNotEqual(undefined, whereis(flurm_scheduler)),
    ?assertNotEqual(undefined, whereis(flurm_node_registry)).

test_no_process_leaks() ->
    %% Measure baseline
    ProcessesBefore = length(erlang:processes()),

    %% Submit and cancel 200 jobs (full lifecycle)
    lists:foreach(fun(I) ->
        Spec = #{
            name => list_to_binary("leak_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1, memory_mb => 100, time_limit => 3600
        },
        {ok, Id} = flurm_job_manager:submit_job(Spec),
        flurm_job_manager:cancel_job(Id)
    end, lists:seq(1, 200)),

    timer:sleep(200),
    erlang:garbage_collect(),

    ProcessesAfter = length(erlang:processes()),
    Growth = ProcessesAfter - ProcessesBefore,

    io:format("Process count: before=~p, after=~p, growth=~p~n",
              [ProcessesBefore, ProcessesAfter, Growth]),

    %% Allow small variance for timers etc, but no major leak
    ?assert(abs(Growth) < 20,
            io_lib:format("Process count grew by ~p after 200 submit/cancel cycles", [Growth])).

test_ets_bounded() ->
    %% Submit 300 jobs, cancel them all
    lists:foreach(fun(I) ->
        Spec = #{
            name => list_to_binary("ets_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1, memory_mb => 100, time_limit => 3600
        },
        {ok, Id} = flurm_job_manager:submit_job(Spec),
        flurm_job_manager:cancel_job(Id)
    end, lists:seq(1, 300)),

    %% Check that node registry ETS tables don't accumulate garbage
    NodeCount = flurm_node_registry:count_nodes(),
    %% We didn't register any nodes, so should be 0
    ?assertEqual(0, NodeCount),

    %% Check that the job manager's internal map is consistent
    Jobs = flurm_job_manager:list_jobs(),
    CancelledJobs = [J || #job{state = cancelled} = J <- Jobs],
    ?assertEqual(300, length(CancelledJobs)).

%%====================================================================
%% Workload Loop
%%====================================================================

run_workload_loop(EndTime, Counter) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= EndTime of
        true -> Counter;
        false ->
            %% Random operation
            case rand:uniform(6) of
                1 ->
                    Spec = #{
                        name => <<"stability_job">>,
                        script => <<"#!/bin/bash\necho test">>,
                        num_cpus => 1, memory_mb => 100, time_limit => 3600
                    },
                    catch flurm_job_manager:submit_job(Spec);
                2 ->
                    Jobs = catch flurm_job_manager:list_jobs(),
                    case Jobs of
                        L when is_list(L), length(L) > 0 ->
                            #job{id = Id} = lists:nth(rand:uniform(length(L)), L),
                            catch flurm_job_manager:cancel_job(Id);
                        _ -> ok
                    end;
                3 ->
                    Jobs = catch flurm_job_manager:list_jobs(),
                    case Jobs of
                        L when is_list(L), length(L) > 0 ->
                            #job{id = Id} = lists:nth(rand:uniform(length(L)), L),
                            catch flurm_job_manager:hold_job(Id);
                        _ -> ok
                    end;
                4 ->
                    Jobs = catch flurm_job_manager:list_jobs(),
                    case Jobs of
                        L when is_list(L), length(L) > 0 ->
                            #job{id = Id} = lists:nth(rand:uniform(length(L)), L),
                            catch flurm_job_manager:release_job(Id);
                        _ -> ok
                    end;
                5 ->
                    catch flurm_scheduler:trigger_schedule();
                6 ->
                    catch flurm_job_manager:list_jobs()
            end,
            run_workload_loop(EndTime, Counter + 1)
    end.

%%====================================================================
%% Helpers
%%====================================================================

get_server_queue_lengths() ->
    Servers = [flurm_job_manager, flurm_scheduler, flurm_node_registry,
               flurm_job_registry],
    lists:filtermap(fun(Name) ->
        case whereis(Name) of
            undefined -> false;
            Pid ->
                {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
                {true, {Name, Len}}
        end
    end, Servers).

gc_all_servers() ->
    Servers = [flurm_job_manager, flurm_scheduler, flurm_node_registry,
               flurm_job_registry, flurm_limits, flurm_license,
               flurm_job_deps, flurm_job_array],
    lists:foreach(fun(Name) ->
        case whereis(Name) of
            undefined -> ok;
            Pid -> erlang:garbage_collect(Pid)
        end
    end, Servers),
    erlang:garbage_collect().

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
