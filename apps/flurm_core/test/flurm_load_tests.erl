%%%-------------------------------------------------------------------
%%% @doc Load and Scale Testing
%%%
%%% Verifies the system handles realistic job volumes without
%%% degradation, message queue backup, or memory leaks.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_load_tests
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_load_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

load_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"1000 rapid submissions",
         {timeout, 120, fun test_1000_submissions/0}},
        {"500 jobs exceeding capacity",
         {timeout, 120, fun test_oversubscription/0}},
        {"500 simultaneous cancels",
         {timeout, 60, fun test_mass_cancel/0}},
        {"scheduler with 500 pending and 10 nodes",
         {timeout, 120, fun test_scheduler_throughput/0}},
        {"message queue stays bounded",
         {timeout, 120, fun test_message_queue_bounded/0}},
        {"memory stays bounded under load",
         {timeout, 120, fun test_memory_bounded/0}}
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

test_1000_submissions() ->
    N = 1000,
    T0 = erlang:monotonic_time(millisecond),
    JobIds = lists:map(fun(I) ->
        Spec = #{
            name => list_to_binary("load_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1, memory_mb => 100, time_limit => 3600
        },
        {ok, Id} = flurm_job_manager:submit_job(Spec),
        Id
    end, lists:seq(1, N)),
    T1 = erlang:monotonic_time(millisecond),
    Elapsed = T1 - T0,

    %% All IDs unique
    ?assertEqual(N, length(lists:usort(JobIds))),

    %% All jobs exist and valid
    Jobs = flurm_job_manager:list_jobs(),
    ?assertEqual(N, length(Jobs)),

    %% Average submission should be under 10ms each (10s total for 1000)
    ?assert(Elapsed < 10000,
            io_lib:format("1000 submissions took ~pms (>10s)", [Elapsed])),

    io:format("1000 submissions in ~pms (~.1fms avg)~n",
              [Elapsed, Elapsed / N]).

test_oversubscription() ->
    %% Register a small node: 4 CPUs, 4GB
    flurm_node_registry:register_node_direct(
        #{hostname => <<"small_node">>, cpus => 4, memory_mb => 4096, state => up,
          partitions => [<<"default">>]}),

    %% Submit 500 jobs each requesting 1 CPU
    N = 500,
    _JobIds = lists:map(fun(I) ->
        Spec = #{
            name => list_to_binary("over_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1, memory_mb => 100, time_limit => 3600
        },
        {ok, Id} = flurm_job_manager:submit_job(Spec),
        Id
    end, lists:seq(1, N)),

    %% Trigger scheduling
    flurm_scheduler:trigger_schedule(),
    timer:sleep(500),

    %% Node should not be overallocated
    case flurm_node_registry:get_node_entry(<<"small_node">>) of
        {ok, Entry} ->
            AllocCpus = Entry#node_entry.cpus_total - Entry#node_entry.cpus_avail,
            ?assert(AllocCpus =< 4,
                    io_lib:format("Node overallocated: ~p CPUs (max 4)", [AllocCpus]));
        _ -> ok
    end,

    %% Most jobs should still be pending (only 4 can run)
    Jobs = flurm_job_manager:list_jobs(),
    PendingCount = length([J || #job{state = S} = J <- Jobs,
                                S =:= pending orelse S =:= held]),
    RunningCount = length([J || #job{state = running} = J <- Jobs]),
    ?assert(PendingCount > 400,
            io_lib:format("Expected most jobs pending, got ~p pending, ~p running",
                          [PendingCount, RunningCount])).

test_mass_cancel() ->
    %% Submit 500 jobs
    N = 500,
    JobIds = lists:map(fun(I) ->
        Spec = #{
            name => list_to_binary("cancel_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1, memory_mb => 100, time_limit => 3600
        },
        {ok, Id} = flurm_job_manager:submit_job(Spec),
        Id
    end, lists:seq(1, N)),

    %% Cancel all simultaneously from 10 processes
    Parent = self(),
    Chunks = chunk_list(JobIds, 50),
    CancelPids = [spawn_link(fun() ->
        lists:foreach(fun(Id) ->
            flurm_job_manager:cancel_job(Id)
        end, Chunk),
        Parent ! {done, self()}
    end) || Chunk <- Chunks],

    lists:foreach(fun(Pid) ->
        receive {done, Pid} -> ok after 30000 -> error(timeout) end
    end, CancelPids),

    %% All jobs should be cancelled
    Jobs = flurm_job_manager:list_jobs(),
    CancelledCount = length([J || #job{state = cancelled} = J <- Jobs]),
    ?assertEqual(N, CancelledCount).

test_scheduler_throughput() ->
    %% Register 10 nodes with 8 CPUs each = 80 CPU slots
    lists:foreach(fun(I) ->
        Name = list_to_binary("node" ++ integer_to_list(I)),
        flurm_node_registry:register_node_direct(
            #{hostname => Name, cpus => 8, memory_mb => 16384, state => up,
              partitions => [<<"default">>]})
    end, lists:seq(1, 10)),

    %% Submit 500 pending jobs
    N = 500,
    _JobIds = lists:map(fun(I) ->
        Spec = #{
            name => list_to_binary("sched_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1, memory_mb => 100, time_limit => 3600
        },
        {ok, Id} = flurm_job_manager:submit_job(Spec),
        Id
    end, lists:seq(1, N)),

    %% Measure scheduling time
    T0 = erlang:monotonic_time(millisecond),
    flurm_scheduler:trigger_schedule(),
    timer:sleep(500),
    T1 = erlang:monotonic_time(millisecond),

    %% Check how many got scheduled (max 80 per round)
    Jobs = flurm_job_manager:list_jobs(),
    RunningCount = length([J || #job{state = S} = J <- Jobs,
                                S =:= running orelse S =:= configuring]),
    PendingCount = length([J || #job{state = pending} = J <- Jobs]),

    io:format("After schedule: ~p running, ~p pending, took ~pms~n",
              [RunningCount, PendingCount, T1 - T0]),

    %% At least some should have been scheduled
    ?assert(RunningCount >= 1 orelse PendingCount < N,
            "Scheduler should have scheduled at least 1 job").

test_message_queue_bounded() ->
    %% Submit 200 jobs rapidly
    lists:foreach(fun(I) ->
        Spec = #{
            name => list_to_binary("mq_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1, memory_mb => 100, time_limit => 3600
        },
        flurm_job_manager:submit_job(Spec)
    end, lists:seq(1, 200)),

    %% Wait for async notifications to be processed
    timer:sleep(2000),

    %% Check message queues of all gen_servers
    Servers = [flurm_job_manager, flurm_scheduler, flurm_node_registry,
               flurm_job_registry],
    lists:foreach(fun(Name) ->
        case whereis(Name) of
            undefined -> ok;
            Pid ->
                {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
                ?assert(Len < 100,
                        io_lib:format("~p message queue too large: ~p", [Name, Len]))
        end
    end, Servers).

test_memory_bounded() ->
    %% Measure baseline memory
    erlang:garbage_collect(),
    timer:sleep(100),
    MemBefore = erlang:memory(total),

    %% Submit 500 jobs
    lists:foreach(fun(I) ->
        Spec = #{
            name => list_to_binary("mem_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1, memory_mb => 100, time_limit => 3600
        },
        flurm_job_manager:submit_job(Spec)
    end, lists:seq(1, 500)),

    %% Cancel them all
    Jobs = flurm_job_manager:list_jobs(),
    lists:foreach(fun(#job{id = Id, state = S}) ->
        case S of
            cancelled -> ok;
            _ -> catch flurm_job_manager:cancel_job(Id)
        end
    end, Jobs),

    %% Force GC
    erlang:garbage_collect(),
    lists:foreach(fun(Name) ->
        case whereis(Name) of
            undefined -> ok;
            Pid -> erlang:garbage_collect(Pid)
        end
    end, [flurm_job_manager, flurm_scheduler, flurm_node_registry]),
    timer:sleep(200),

    MemAfter = erlang:memory(total),
    Growth = (MemAfter - MemBefore),
    GrowthPct = Growth * 100 / max(MemBefore, 1),

    io:format("Memory: before=~pMB, after=~pMB, growth=~.1f%~n",
              [MemBefore div (1024*1024), MemAfter div (1024*1024), GrowthPct]),

    %% Memory growth should be bounded — allow generous margin for test environment
    %% The key is that we don't see unbounded growth (e.g., 500% would be a leak)
    ?assert(GrowthPct < 200,
            io_lib:format("Memory grew ~.1f% (>200%), possible leak", [GrowthPct])).

%%====================================================================
%% Helpers
%%====================================================================

chunk_list(List, ChunkSize) ->
    chunk_list(List, ChunkSize, []).

chunk_list([], _ChunkSize, Acc) ->
    lists:reverse(Acc);
chunk_list(List, ChunkSize, Acc) when length(List) =< ChunkSize ->
    lists:reverse([List | Acc]);
chunk_list(List, ChunkSize, Acc) ->
    {Chunk, Rest} = lists:split(ChunkSize, List),
    chunk_list(Rest, ChunkSize, [Chunk | Acc]).

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
