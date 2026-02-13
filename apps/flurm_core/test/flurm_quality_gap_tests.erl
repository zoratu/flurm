%%%-------------------------------------------------------------------
%%% @doc Additional quality tests beyond line coverage.
%%%
%%% Focus areas:
%%% 1) integration lifecycle smoke
%%% 2) failure/recovery behavior
%%% 3) concurrency invariants
%%% 4) malformed protocol resilience
%%% 5) migration and upgrade compatibility contracts
%%%-------------------------------------------------------------------
-module(flurm_quality_gap_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("flurm_core/include/flurm_core.hrl").
-include_lib("flurm_protocol/include/flurm_protocol.hrl").

-define(VALID_JOB_STATES, [pending, held, configuring, running, suspended,
                           completing, completed, failed, timeout, cancelled, node_fail]).

quality_gap_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"integration lifecycle smoke", fun test_integration_lifecycle_smoke/0},
      {"failure recovery restart smoke", fun test_failure_recovery_restart_smoke/0},
      {"concurrency invariants", {timeout, 90, fun test_concurrency_invariants/0}},
      {"load submit smoke", {timeout, 90, fun test_load_submit_smoke/0}},
      {"protocol malformed resilience", fun test_protocol_malformed_resilience/0},
      {"migration import contract", fun test_migration_import_contract/0},
      {"upgrade compatibility contract", fun test_upgrade_compatibility_contract/0}
     ]}.

setup() ->
    application:ensure_all_started(sasl),
    application:ensure_all_started(lager),
    stop_runtime(),
    setup_mocks(),
    start_runtime(),
    ok.

cleanup(_) ->
    stop_runtime(),
    teardown_mocks(),
    ok.

test_integration_lifecycle_smoke() ->
    register_default_node(),
    {ok, JobId} = flurm_job_manager:submit_job(make_job_map(#{name => <<"quality_job_1">>})),
    ?assertMatch({ok, #job{id = JobId}}, flurm_job_manager:get_job(JobId)),
    ok = flurm_scheduler:trigger_schedule(),
    timer:sleep(100),
    ?assertMatch({ok, #job{}}, flurm_job_manager:get_job(JobId)).

test_failure_recovery_restart_smoke() ->
    register_default_node(),
    {ok, _} = flurm_job_manager:submit_job(make_job_map(#{name => <<"before_restart">>})),
    restart_server(flurm_scheduler, fun flurm_scheduler:start_link/0),
    ok = flurm_scheduler:trigger_schedule(),
    restart_server(flurm_job_manager, fun flurm_job_manager:start_link/0),
    {ok, NewId} = flurm_job_manager:submit_job(make_job_map(#{name => <<"after_restart">>})),
    ?assertMatch({ok, #job{id = NewId}}, flurm_job_manager:get_job(NewId)).

test_concurrency_invariants() ->
    register_default_node(),
    Parent = self(),
    Worker = fun(I) ->
        {ok, Id} = flurm_job_manager:submit_job(
                     make_job_map(#{
                        name => list_to_binary(io_lib:format("qgap_~p", [I]))
                     })),
        case I rem 4 of
            0 -> _ = flurm_job_manager:hold_job(Id);
            1 -> _ = flurm_job_manager:cancel_job(Id);
            2 ->
                _ = flurm_job_manager:hold_job(Id),
                _ = flurm_job_manager:release_job(Id);
            _ -> ok
        end,
        Parent ! {done, Id}
    end,
    _Pids = [spawn(fun() -> Worker(I) end) || I <- lists:seq(1, 80)],
    Ids = gather_done(80, []),
    ?assertEqual(length(Ids), length(lists:usort(Ids))),
    Jobs = flurm_job_manager:list_jobs(),
    ?assert(length(Jobs) >= 80),
    lists:foreach(fun(#job{state = S}) ->
        ?assert(lists:member(S, ?VALID_JOB_STATES))
    end, Jobs).

test_load_submit_smoke() ->
    StartMs = erlang:monotonic_time(millisecond),
    lists:foreach(fun(I) ->
        {ok, _} = flurm_job_manager:submit_job(
                    make_job_map(#{
                       name => list_to_binary(io_lib:format("load_~p", [I]))
                    }))
    end, lists:seq(1, 300)),
    EndMs = erlang:monotonic_time(millisecond),
    ElapsedMs = EndMs - StartMs,
    Jobs = flurm_job_manager:list_jobs(),
    ?assert(length(Jobs) >= 300),
    %% keep this generous to reduce flakiness while still catching pathological regressions
    ?assert(ElapsedMs < 60000).

test_protocol_malformed_resilience() ->
    BadInputs = [
        <<>>,
        <<1>>,
        <<0,0,0,255>>,
        <<16#FF,16#FF,16#FF,16#FF,0,0,0,0>>,
        <<0:256>>,
        crypto:strong_rand_bytes(3),
        crypto:strong_rand_bytes(17),
        crypto:strong_rand_bytes(64),
        crypto:strong_rand_bytes(257)
    ],
    lists:foreach(fun(Bin) ->
        Result = catch flurm_protocol_codec:decode(Bin),
        case Result of
            {'EXIT', {badarg, _}} -> ?assert(false);
            {'EXIT', {function_clause, _}} -> ?assert(false);
            {'EXIT', {{badmatch, _}, _}} -> ?assert(false);
            _ -> ok
        end
    end, BadInputs).

test_migration_import_contract() ->
    ImportedId = 900001,
    {ok, ImportedId} = flurm_job_manager:import_job(#{
        id => ImportedId,
        name => <<"imported_quality_job">>,
        user => <<"legacy">>,
        partition => <<"default">>,
        state => pending,
        submit_time => erlang:system_time(second) - 10
    }),
    {ok, #job{id = ImportedId, state = pending}} = flurm_job_manager:get_job(ImportedId).

test_upgrade_compatibility_contract() ->
    Versions = flurm_upgrade:versions(),
    ?assert(is_list(Versions)),
    ?assertMatch({error, _}, flurm_upgrade:check_upgrade("0.0.0-nonexistent-quality")),
    case flurm_upgrade:check_upgrade("0.0.0-nonexistent-quality") of
        {error, Failures} ->
            Names = [N || {N, _} <- Failures],
            ?assert(lists:member(release_exists, Names));
        _ ->
            ?assert(false)
    end.

setup_mocks() ->
    Modules = [
        flurm_config_server,
        flurm_backfill,
        flurm_reservation,
        flurm_preemption,
        flurm_job_deps,
        flurm_metrics,
        flurm_gres,
        flurm_dbd_server,
        flurm_job_dispatcher_server,
        flurm_node_manager_server,
        flurm_node_manager,
        flurm_job_dispatcher
    ],
    lists:foreach(fun(M) ->
        catch meck:unload(M),
        meck:new(M, [passthrough, non_strict, no_link])
    end, Modules),

    meck:expect(flurm_config_server, subscribe_changes, fun(_) -> ok end),
    meck:expect(flurm_backfill, is_backfill_enabled, fun() -> false end),
    meck:expect(flurm_reservation, get_available_nodes_excluding_reserved, fun(Nodes) -> Nodes end),
    meck:expect(flurm_preemption, get_priority_threshold, fun() -> 99999 end),
    meck:expect(flurm_job_deps, check_dependencies, fun(_) -> {ok, []} end),
    meck:expect(flurm_job_deps, add_dependencies, fun(_, _) -> ok end),
    meck:expect(flurm_job_deps, notify_completion, fun(_, _) -> ok end),
    meck:expect(flurm_job_deps, on_job_state_change, fun(_, _) -> ok end),
    meck:expect(flurm_job_deps, remove_all_dependencies, fun(_) -> ok end),
    meck:expect(flurm_job_deps, parse_dependency_spec, fun(_) -> {ok, []} end),
    meck:expect(flurm_job_deps, has_circular_dependency, fun(_, _) -> false end),
    meck:expect(flurm_metrics, increment, fun(_) -> ok end),
    meck:expect(flurm_metrics, increment, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, histogram, fun(_, _) -> ok end),
    meck:expect(flurm_metrics, gauge, fun(_, _) -> ok end),
    meck:expect(flurm_gres, filter_nodes_by_gres, fun(Names, _) -> Names end),
    meck:expect(flurm_gres, allocate, fun(_, _, _) -> {ok, []} end),
    meck:expect(flurm_gres, deallocate, fun(_, _) -> ok end),
    meck:expect(flurm_dbd_server, record_job_submit, fun(_) -> ok end),
    meck:expect(flurm_dbd_server, record_job_start, fun(_, _) -> ok end),
    meck:expect(flurm_dbd_server, record_job_end, fun(_, _, _) -> ok end),
    meck:expect(flurm_dbd_server, record_job_cancelled, fun(_, _) -> ok end),
    meck:expect(flurm_job_dispatcher_server, cancel_job, fun(_, _) -> ok end),
    meck:expect(flurm_job_dispatcher_server, signal_job, fun(_, _, _) -> ok end),
    meck:expect(flurm_node_manager_server, allocate_resources, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_node_manager_server, release_resources, fun(_, _) -> ok end),
    meck:expect(flurm_node_manager_server, list_nodes, fun() -> [] end),
    meck:expect(flurm_node_manager, allocate_resources, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_node_manager, release_resources, fun(_, _) -> ok end),
    meck:expect(flurm_node_manager, allocate_gres, fun(_, _, _, _) -> ok end),
    meck:expect(flurm_node_manager, release_gres, fun(_, _) -> ok end),
    meck:expect(flurm_job_dispatcher, dispatch_job, fun(_, _) -> ok end),
    ok.

teardown_mocks() ->
    lists:foreach(fun(M) ->
        catch meck:unload(M)
    end, [
        flurm_job_dispatcher,
        flurm_node_manager,
        flurm_node_manager_server,
        flurm_job_dispatcher_server,
        flurm_dbd_server,
        flurm_gres,
        flurm_metrics,
        flurm_job_deps,
        flurm_preemption,
        flurm_reservation,
        flurm_backfill,
        flurm_config_server
    ]),
    ok.

start_runtime() ->
    _ = ensure_started(flurm_job_registry, fun flurm_job_registry:start_link/0),
    _ = ensure_started(flurm_job_sup, fun flurm_job_sup:start_link/0),
    _ = ensure_started(flurm_node_registry, fun flurm_node_registry:start_link/0),
    _ = ensure_started(flurm_node_sup, fun flurm_node_sup:start_link/0),
    _ = ensure_started(flurm_limits, fun flurm_limits:start_link/0),
    _ = ensure_started(flurm_license, fun flurm_license:start_link/0),
    _ = ensure_started(flurm_job_manager, fun flurm_job_manager:start_link/0),
    _ = ensure_started(flurm_scheduler, fun flurm_scheduler:start_link/0),
    ok.

stop_runtime() ->
    lists:foreach(fun(Name) -> safe_stop(Name) end, [
        flurm_scheduler,
        flurm_job_manager,
        flurm_license,
        flurm_limits,
        flurm_node_sup,
        flurm_node_registry,
        flurm_job_sup,
        flurm_job_registry
    ]),
    ok.

ensure_started(Name, StartFun) ->
    case whereis(Name) of
        undefined ->
            case StartFun() of
                {ok, Pid} -> Pid;
                {error, {already_started, Pid}} -> Pid;
                {ok, Pid, _} -> Pid
            end;
        Pid when is_pid(Pid) ->
            Pid
    end.

safe_stop(Name) ->
    case whereis(Name) of
        undefined ->
            ok;
        Pid ->
            Ref = erlang:monitor(process, Pid),
            catch unlink(Pid),
            catch gen_server:stop(Pid, normal, 5000),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                erlang:demonitor(Ref, [flush]),
                catch exit(Pid, kill),
                ok
            end
    end.

restart_server(Name, StartFun) ->
    safe_stop(Name),
    _ = ensure_started(Name, StartFun),
    ?assert(is_pid(whereis(Name))),
    ok.

register_default_node() ->
    _ = flurm_node_registry:register_node_direct(#{
        hostname => <<"quality-node-1">>,
        cpus => 16,
        memory_mb => 65536,
        state => up,
        partitions => [<<"default">>]
    }),
    ok.

make_job_map(Overrides) ->
    maps:merge(#{
        name => <<"quality-job">>,
        user => <<"quality-user">>,
        partition => <<"default">>,
        script => <<"#!/bin/bash\necho quality\n">>,
        num_cpus => 1,
        memory_mb => 64,
        time_limit => 10
    }, Overrides).

gather_done(0, Acc) ->
    Acc;
gather_done(N, Acc) ->
    receive
        {done, Id} ->
            gather_done(N - 1, [Id | Acc])
    after 15000 ->
        ?assert(false)
    end.
