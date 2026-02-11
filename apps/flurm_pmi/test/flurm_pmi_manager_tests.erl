%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_pmi_manager
%%%
%%% Covers job lifecycle, rank registration, KVS operations,
%%% barrier synchronization, and multi-job isolation.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_pmi_manager_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_pmi_manager_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

pmi_manager_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init_job creates new PMI context",
         fun test_init_job/0},
        {"init_job returns error for duplicate",
         fun test_init_job_duplicate/0},
        {"finalize_job removes context",
         fun test_finalize_job/0},
        {"finalize_job returns error for unknown",
         fun test_finalize_not_found/0},
        {"get_job_info returns metadata",
         fun test_get_job_info/0},
        {"get_job_info returns error for unknown",
         fun test_get_job_info_not_found/0},
        {"get_job_info reports correct rank_count",
         fun test_get_job_info_rank_count/0},
        {"register_rank adds rank to job",
         fun test_register_rank/0},
        {"register_rank rejects invalid rank >= size",
         fun test_register_rank_invalid/0},
        {"register_rank returns error for unknown job",
         fun test_register_rank_no_job/0},
        {"get_rank_info returns registered rank",
         fun test_get_rank_info/0},
        {"get_rank_info returns error for unregistered",
         fun test_get_rank_info_not_found/0},
        {"get_rank_info returns error for unknown job",
         fun test_get_rank_info_no_job/0},
        {"kvs_put stores value",
         fun test_kvs_put/0},
        {"kvs_put returns error for unknown job",
         fun test_kvs_put_no_job/0},
        {"kvs_get retrieves stored value",
         fun test_kvs_get/0},
        {"kvs_get returns not_found for missing key",
         fun test_kvs_get_not_found/0},
        {"kvs_get returns error for unknown job",
         fun test_kvs_get_no_job/0},
        {"kvs_get_by_index retrieves by sorted position",
         fun test_kvs_get_by_index/0},
        {"kvs_get_by_index returns end_of_kvs at boundary",
         fun test_kvs_get_by_index_end/0},
        {"kvs_commit sets committed flag",
         fun test_kvs_commit/0},
        {"kvs_commit returns error for unknown job",
         fun test_kvs_commit_no_job/0},
        {"get_kvs_name returns correct namespace",
         fun test_get_kvs_name/0},
        {"get_kvs_name returns error for unknown",
         fun test_get_kvs_name_not_found/0},
        {"barrier_in blocks until all ranks arrive",
         fun test_barrier_all_ranks/0},
        {"barrier_in partial does not release",
         fun test_barrier_partial/0},
        {"barrier_in returns error for unknown job",
         fun test_barrier_no_job/0},
        {"barrier resets after completion",
         fun test_barrier_resets/0},
        {"multiple jobs are isolated",
         fun test_multiple_jobs_isolation/0},
        {"unknown request returns error",
         fun test_unknown_request/0}
     ]}.

%%====================================================================
%% Setup / Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(sasl),
    case whereis(flurm_pmi_manager) of
        undefined ->
            {ok, Pid} = flurm_pmi_manager:start_link(),
            unlink(Pid),
            Pid;
        Pid ->
            Pid
    end.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true -> gen_server:stop(Pid, normal, 5000);
        false -> ok
    end.

%%====================================================================
%% Tests
%%====================================================================

test_init_job() ->
    ?assertEqual(ok, flurm_pmi_manager:init_job(1, 0, 4)).

test_init_job_duplicate() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ?assertEqual({error, already_exists}, flurm_pmi_manager:init_job(1, 0, 4)).

test_finalize_job() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ?assertEqual(ok, flurm_pmi_manager:finalize_job(1, 0)),
    ?assertEqual({error, not_found}, flurm_pmi_manager:get_job_info(1, 0)).

test_finalize_not_found() ->
    ?assertEqual({error, not_found}, flurm_pmi_manager:finalize_job(999, 0)).

test_get_job_info() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(1, maps:get(job_id, Info)),
    ?assertEqual(0, maps:get(step_id, Info)),
    ?assertEqual(4, maps:get(size, Info)),
    ?assertEqual(false, maps:get(committed, Info)),
    ?assertEqual(0, maps:get(rank_count, Info)).

test_get_job_info_not_found() ->
    ?assertEqual({error, not_found}, flurm_pmi_manager:get_job_info(999, 0)).

test_get_job_info_rank_count() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 1, <<"node2">>),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(2, maps:get(rank_count, Info)).

test_register_rank() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ?assertEqual(ok, flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>)).

test_register_rank_invalid() ->
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    ?assertEqual({error, invalid_rank}, flurm_pmi_manager:register_rank(1, 0, 5, <<"node1">>)).

test_register_rank_no_job() ->
    ?assertEqual({error, job_not_found}, flurm_pmi_manager:register_rank(999, 0, 0, <<"n">>)).

test_get_rank_info() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),
    {ok, Info} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    ?assertEqual(0, maps:get(rank, Info)),
    ?assertEqual(<<"node1">>, maps:get(node, Info)),
    ?assertEqual(initialized, maps:get(status, Info)).

test_get_rank_info_not_found() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ?assertEqual({error, rank_not_found}, flurm_pmi_manager:get_rank_info(1, 0, 0)).

test_get_rank_info_no_job() ->
    ?assertEqual({error, job_not_found}, flurm_pmi_manager:get_rank_info(999, 0, 0)).

test_kvs_put() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ?assertEqual(ok, flurm_pmi_manager:kvs_put(1, 0, <<"key1">>, <<"val1">>)).

test_kvs_put_no_job() ->
    ?assertEqual({error, job_not_found}, flurm_pmi_manager:kvs_put(999, 0, <<"k">>, <<"v">>)).

test_kvs_get() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key1">>, <<"val1">>),
    ?assertEqual({ok, <<"val1">>}, flurm_pmi_manager:kvs_get(1, 0, <<"key1">>)).

test_kvs_get_not_found() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ?assertEqual({error, not_found}, flurm_pmi_manager:kvs_get(1, 0, <<"missing">>)).

test_kvs_get_no_job() ->
    ?assertEqual({error, job_not_found}, flurm_pmi_manager:kvs_get(999, 0, <<"k">>)).

test_kvs_get_by_index() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"bbb">>, <<"v2">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"aaa">>, <<"v1">>),
    %% Index 0 should return the alphabetically first key
    {ok, <<"aaa">>, <<"v1">>} = flurm_pmi_manager:kvs_get_by_index(1, 0, 0),
    {ok, <<"bbb">>, <<"v2">>} = flurm_pmi_manager:kvs_get_by_index(1, 0, 1).

test_kvs_get_by_index_end() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key">>, <<"val">>),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_manager:kvs_get_by_index(1, 0, 1)).

test_kvs_commit() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ?assertEqual(ok, flurm_pmi_manager:kvs_commit(1, 0)),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(true, maps:get(committed, Info)).

test_kvs_commit_no_job() ->
    ?assertEqual({error, job_not_found}, flurm_pmi_manager:kvs_commit(999, 0)).

test_get_kvs_name() ->
    ok = flurm_pmi_manager:init_job(42, 3, 1),
    ?assertEqual({ok, <<"kvs_42_3">>}, flurm_pmi_manager:get_kvs_name(42, 3)).

test_get_kvs_name_not_found() ->
    ?assertEqual({error, not_found}, flurm_pmi_manager:get_kvs_name(999, 0)).

test_barrier_all_ranks() ->
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    Parent = self(),
    %% Spawn two processes to hit barrier
    Pid1 = spawn_link(fun() ->
        Result = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {barrier_done, 0, Result}
    end),
    Pid2 = spawn_link(fun() ->
        Result = flurm_pmi_manager:barrier_in(1, 0, 1),
        Parent ! {barrier_done, 1, Result}
    end),
    %% Both should complete
    receive {barrier_done, 0, R0} -> ?assertEqual(ok, R0)
    after 5000 -> error(barrier_timeout_rank0) end,
    receive {barrier_done, 1, R1} -> ?assertEqual(ok, R1)
    after 5000 -> error(barrier_timeout_rank1) end,
    _ = {Pid1, Pid2}.

test_barrier_partial() ->
    ok = flurm_pmi_manager:init_job(1, 0, 3),
    Parent = self(),
    %% Only 1 of 3 ranks arrives - should not release
    _Pid = spawn_link(fun() ->
        %% This will block (60s timeout in barrier_in)
        %% We use a short gen_server:call timeout to detect blocking
        try
            gen_server:call(flurm_pmi_manager, {barrier_in, 1, 0, 0}, 200)
        catch
            exit:{timeout, _} -> Parent ! {barrier_timeout, 0}
        end
    end),
    receive {barrier_timeout, 0} -> ok
    after 2000 -> error(expected_barrier_to_block) end.

test_barrier_no_job() ->
    ?assertEqual({error, job_not_found}, flurm_pmi_manager:barrier_in(999, 0, 0)).

test_barrier_resets() ->
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    Parent = self(),

    %% First barrier cycle
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {cycle1, 0}
    end),
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 1),
        Parent ! {cycle1, 1}
    end),
    receive {cycle1, 0} -> ok after 5000 -> error(cycle1_timeout) end,
    receive {cycle1, 1} -> ok after 5000 -> error(cycle1_timeout) end,

    %% Second barrier cycle should also work (counter reset)
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {cycle2, 0}
    end),
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 1),
        Parent ! {cycle2, 1}
    end),
    receive {cycle2, 0} -> ok after 5000 -> error(cycle2_timeout) end,
    receive {cycle2, 1} -> ok after 5000 -> error(cycle2_timeout) end.

test_multiple_jobs_isolation() ->
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    ok = flurm_pmi_manager:init_job(2, 0, 2),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key">>, <<"job1_val">>),
    ok = flurm_pmi_manager:kvs_put(2, 0, <<"key">>, <<"job2_val">>),
    ?assertEqual({ok, <<"job1_val">>}, flurm_pmi_manager:kvs_get(1, 0, <<"key">>)),
    ?assertEqual({ok, <<"job2_val">>}, flurm_pmi_manager:kvs_get(2, 0, <<"key">>)).

test_unknown_request() ->
    ?assertEqual({error, unknown_request}, gen_server:call(flurm_pmi_manager, bogus)).
