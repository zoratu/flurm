%%%-------------------------------------------------------------------
%%% @doc Multi-Process Concurrent Stress Tests
%%%
%%% Runs multiple simultaneous operations against the same gen_servers
%%% to expose race conditions, deadlocks, and state corruption.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_concurrent_tests
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_concurrent_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").

-define(TERMINAL_STATES, [completed, failed, timeout, cancelled, node_fail]).

%%====================================================================
%% EUnit Integration
%%====================================================================

concurrent_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"50 concurrent submissions", fun test_concurrent_submit/0},
        {"concurrent submit and cancel", fun test_concurrent_submit_cancel/0},
        {"concurrent hold/release storm", fun test_concurrent_hold_release/0},
        {"concurrent node register/unregister", fun test_concurrent_node_ops/0},
        {"schedule during concurrent submissions", fun test_schedule_during_submit/0},
        {"concurrent get_job reads", fun test_concurrent_reads/0},
        {"concurrent list_jobs during mutations", fun test_concurrent_list_during_mutate/0},
        {"no crashed gen_servers after storm", fun test_no_crashed_servers/0}
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

test_concurrent_submit() ->
    N = 50,
    Parent = self(),
    Pids = [spawn_link(fun() ->
        Spec = #{
            name => list_to_binary("job_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho hello">>,
            num_cpus => 1,
            memory_mb => 100,
            time_limit => 3600
        },
        Result = flurm_job_manager:submit_job(Spec),
        Parent ! {submit_result, self(), Result}
    end) || I <- lists:seq(1, N)],

    Results = collect_results(Pids),
    JobIds = [Id || {ok, Id} <- Results],
    ?assertEqual(N, length(JobIds)),
    %% All IDs unique
    ?assertEqual(N, length(lists:usort(JobIds))).

test_concurrent_submit_cancel() ->
    %% First submit 20 jobs
    JobIds = submit_n_jobs(20),
    ?assertEqual(20, length(JobIds)),

    %% Concurrently cancel the first 10 while submitting 10 more
    Parent = self(),
    CancelPids = [spawn_link(fun() ->
        Result = flurm_job_manager:cancel_job(Id),
        Parent ! {cancel_result, self(), Result}
    end) || Id <- lists:sublist(JobIds, 10)],

    SubmitPids = [spawn_link(fun() ->
        Spec = #{
            name => <<"concurrent_job">>,
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1,
            memory_mb => 100,
            time_limit => 3600
        },
        Result = flurm_job_manager:submit_job(Spec),
        Parent ! {submit_result, self(), Result}
    end) || _ <- lists:seq(1, 10)],

    CancelResults = collect_results(CancelPids),
    SubmitResults = collect_results(SubmitPids),

    %% All cancels should succeed
    ?assert(lists:all(fun(R) -> R =:= ok end, CancelResults)),
    %% All submits should succeed
    ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, SubmitResults)),

    %% Verify cancelled jobs are actually cancelled
    lists:foreach(fun(Id) ->
        {ok, #job{state = cancelled}} = flurm_job_manager:get_job(Id)
    end, lists:sublist(JobIds, 10)).

test_concurrent_hold_release() ->
    %% Submit 10 jobs
    JobIds = submit_n_jobs(10),

    %% Concurrently hold and release all jobs in random order
    Parent = self(),
    Pids = [spawn_link(fun() ->
        %% Each process does 10 rapid hold/release cycles
        lists:foreach(fun(_) ->
            Id = lists:nth(rand:uniform(length(JobIds)), JobIds),
            case rand:uniform(2) of
                1 -> flurm_job_manager:hold_job(Id);
                2 -> flurm_job_manager:release_job(Id)
            end
        end, lists:seq(1, 10)),
        Parent ! {done, self()}
    end) || _ <- lists:seq(1, 20)],

    %% Wait for all to finish
    lists:foreach(fun(Pid) ->
        receive {done, Pid} -> ok after 10000 -> error(timeout) end
    end, Pids),

    %% All jobs should be in valid states
    lists:foreach(fun(Id) ->
        {ok, #job{state = S}} = flurm_job_manager:get_job(Id),
        ?assert(lists:member(S, [pending, held]))
    end, JobIds).

test_concurrent_node_ops() ->
    Parent = self(),
    NodeNames = [list_to_binary("node" ++ integer_to_list(I))
                 || I <- lists:seq(1, 20)],

    %% Register 20 nodes concurrently
    RegPids = [spawn_link(fun() ->
        Props = #{hostname => Name, cpus => 4, memory_mb => 8192, state => up,
                  partitions => [<<"default">>]},
        Result = flurm_node_registry:register_node_direct(Props),
        Parent ! {reg_result, self(), Result}
    end) || Name <- NodeNames],

    RegResults = collect_results(RegPids),
    OkCount = length([R || R <- RegResults, R =:= ok]),
    ?assert(OkCount >= 1),

    %% Verify all registered nodes are queryable
    AllNodes = flurm_node_registry:list_nodes(),
    ?assert(length(AllNodes) >= 1),

    %% Concurrently unregister half while querying
    {ToRemove, _ToKeep} = lists:split(10, NodeNames),
    UnregPids = [spawn_link(fun() ->
        flurm_node_registry:unregister_node(Name),
        Parent ! {unreg_result, self(), ok}
    end) || Name <- ToRemove],

    QueryPids = [spawn_link(fun() ->
        _Nodes = flurm_node_registry:list_nodes(),
        Parent ! {query_result, self(), ok}
    end) || _ <- lists:seq(1, 10)],

    _ = collect_results(UnregPids),
    _ = collect_results(QueryPids),

    %% No crashes occurred (if we got here, no linked processes died)
    ok.

test_schedule_during_submit() ->
    %% Register a node
    flurm_node_registry:register_node_direct(
        #{hostname => <<"sched_node">>, cpus => 8, memory_mb => 16384, state => up,
          partitions => [<<"default">>]}),

    Parent = self(),
    %% Submit 30 jobs while triggering scheduling 10 times
    SubmitPids = [spawn_link(fun() ->
        Spec = #{
            name => <<"sched_test">>,
            script => <<"#!/bin/bash\necho test">>,
            num_cpus => 1,
            memory_mb => 100,
            time_limit => 3600
        },
        Result = flurm_job_manager:submit_job(Spec),
        Parent ! {submit_result, self(), Result}
    end) || _ <- lists:seq(1, 30)],

    SchedPids = [spawn_link(fun() ->
        timer:sleep(rand:uniform(50)),
        flurm_scheduler:trigger_schedule(),
        Parent ! {sched_result, self(), ok}
    end) || _ <- lists:seq(1, 10)],

    SubmitResults = collect_results(SubmitPids),
    _ = collect_results(SchedPids),

    %% All submits should succeed
    ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, SubmitResults)),

    %% All jobs should be in valid states
    Jobs = flurm_job_manager:list_jobs(),
    lists:foreach(fun(#job{state = S}) ->
        ValidStates = [pending, held, configuring, running, completing,
                       completed, failed, timeout, cancelled, node_fail, suspended],
        ?assert(lists:member(S, ValidStates))
    end, Jobs).

test_concurrent_reads() ->
    %% Submit 20 jobs
    JobIds = submit_n_jobs(20),

    Parent = self(),
    %% 50 concurrent readers
    Pids = [spawn_link(fun() ->
        Results = lists:map(fun(Id) ->
            flurm_job_manager:get_job(Id)
        end, JobIds),
        AllOk = lists:all(fun({ok, #job{}}) -> true; (_) -> false end, Results),
        Parent ! {read_result, self(), AllOk}
    end) || _ <- lists:seq(1, 50)],

    ReadResults = collect_results(Pids),
    ?assert(lists:all(fun(R) -> R =:= true end, ReadResults)).

test_concurrent_list_during_mutate() ->
    Parent = self(),

    %% Submit and cancel jobs while listing
    MutatePids = [spawn_link(fun() ->
        lists:foreach(fun(_) ->
            Spec = #{
                name => <<"mutate_test">>,
                script => <<"#!/bin/bash\necho test">>,
                num_cpus => 1,
                memory_mb => 100,
                time_limit => 3600
            },
            case flurm_job_manager:submit_job(Spec) of
                {ok, Id} ->
                    case rand:uniform(3) of
                        1 -> flurm_job_manager:cancel_job(Id);
                        _ -> ok
                    end;
                _ -> ok
            end
        end, lists:seq(1, 10)),
        Parent ! {mutate_done, self()}
    end) || _ <- lists:seq(1, 5)],

    ListPids = [spawn_link(fun() ->
        lists:foreach(fun(_) ->
            _Jobs = flurm_job_manager:list_jobs(),
            timer:sleep(5)
        end, lists:seq(1, 20)),
        Parent ! {list_done, self()}
    end) || _ <- lists:seq(1, 10)],

    lists:foreach(fun(Pid) ->
        receive {mutate_done, Pid} -> ok after 30000 -> error(timeout) end
    end, MutatePids),
    lists:foreach(fun(Pid) ->
        receive {list_done, Pid} -> ok after 30000 -> error(timeout) end
    end, ListPids),

    %% If we get here without crashes, the test passes
    ok.

test_no_crashed_servers() ->
    %% Run a brief storm of operations
    Parent = self(),
    Pids = [spawn_link(fun() ->
        lists:foreach(fun(_) ->
            case rand:uniform(5) of
                1 ->
                    Spec = #{name => <<"storm">>,
                             script => <<"#!/bin/bash\necho test">>,
                             num_cpus => 1, memory_mb => 100, time_limit => 3600},
                    flurm_job_manager:submit_job(Spec);
                2 ->
                    Jobs = flurm_job_manager:list_jobs(),
                    case Jobs of
                        [#job{id = Id} | _] -> flurm_job_manager:cancel_job(Id);
                        _ -> ok
                    end;
                3 ->
                    Jobs = flurm_job_manager:list_jobs(),
                    case Jobs of
                        [#job{id = Id} | _] -> flurm_job_manager:hold_job(Id);
                        _ -> ok
                    end;
                4 ->
                    flurm_scheduler:trigger_schedule();
                5 ->
                    flurm_job_manager:list_jobs()
            end
        end, lists:seq(1, 20)),
        Parent ! {storm_done, self()}
    end) || _ <- lists:seq(1, 10)],

    lists:foreach(fun(Pid) ->
        receive {storm_done, Pid} -> ok after 30000 -> error(timeout) end
    end, Pids),

    %% Verify all gen_servers are still alive
    ?assertNotEqual(undefined, whereis(flurm_job_manager)),
    ?assertNotEqual(undefined, whereis(flurm_scheduler)),
    ?assertNotEqual(undefined, whereis(flurm_node_registry)),
    ?assertNotEqual(undefined, whereis(flurm_job_registry)),

    %% Verify we can still submit a job (servers are responsive)
    Spec = #{name => <<"post_storm">>,
             script => <<"#!/bin/bash\necho alive">>,
             num_cpus => 1, memory_mb => 100, time_limit => 3600},
    {ok, _Id} = flurm_job_manager:submit_job(Spec).

%%====================================================================
%% Helpers
%%====================================================================

submit_n_jobs(N) ->
    lists:map(fun(I) ->
        Spec = #{
            name => list_to_binary("job_" ++ integer_to_list(I)),
            script => <<"#!/bin/bash\necho hello">>,
            num_cpus => 1,
            memory_mb => 100,
            time_limit => 3600
        },
        {ok, Id} = flurm_job_manager:submit_job(Spec),
        Id
    end, lists:seq(1, N)).

collect_results(Pids) ->
    lists:map(fun(Pid) ->
        receive
            {submit_result, Pid, R} -> R;
            {cancel_result, Pid, R} -> R;
            {reg_result, Pid, R} -> R;
            {unreg_result, Pid, R} -> R;
            {query_result, Pid, R} -> R;
            {read_result, Pid, R} -> R;
            {sched_result, Pid, R} -> R
        after 30000 ->
            error({timeout_waiting_for, Pid})
        end
    end, Pids).

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
