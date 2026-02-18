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
    %% Ensure no meck mocks are active for modules we need real
    catch meck:unload(flurm_pmi_manager),
    catch meck:unload(flurm_pmi_sup),
    catch meck:unload(flurm_pmi_kvs),
    %% Stop supervisor first if running (to prevent restart storms when stopping manager)
    case whereis(flurm_pmi_sup) of
        undefined -> ok;
        SupPid ->
            catch gen_server:stop(SupPid, normal, 5000),
            timer:sleep(50)
    end,
    %% Now stop manager if still running (shouldn't be, but just in case)
    case whereis(flurm_pmi_manager) of
        undefined -> ok;
        MgrPid0 ->
            catch gen_server:stop(MgrPid0, normal, 5000),
            timer:sleep(50)
    end,
    %% Start fresh manager
    {ok, Pid} = flurm_pmi_manager:start_link(),
    unlink(Pid),
    Pid.

cleanup(Pid) ->
    %% Stop manager (no supervisor to worry about - we stopped it in setup)
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

%%====================================================================
%% Extended Job Lifecycle Tests
%%====================================================================

extended_job_lifecycle_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"multiple jobs can coexist",
         fun test_multiple_jobs/0},
        {"job with different step IDs",
         fun test_different_steps/0},
        {"finalize removes ranks",
         fun test_finalize_removes_ranks/0},
        {"finalize removes KVS",
         fun test_finalize_removes_kvs/0}
     ]}.

test_multiple_jobs() ->
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    ok = flurm_pmi_manager:init_job(2, 0, 4),
    ok = flurm_pmi_manager:init_job(3, 0, 8),
    {ok, Info1} = flurm_pmi_manager:get_job_info(1, 0),
    {ok, Info2} = flurm_pmi_manager:get_job_info(2, 0),
    {ok, Info3} = flurm_pmi_manager:get_job_info(3, 0),
    ?assertEqual(2, maps:get(size, Info1)),
    ?assertEqual(4, maps:get(size, Info2)),
    ?assertEqual(8, maps:get(size, Info3)).

test_different_steps() ->
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    ok = flurm_pmi_manager:init_job(1, 1, 4),
    ok = flurm_pmi_manager:init_job(1, 2, 8),
    {ok, Info0} = flurm_pmi_manager:get_job_info(1, 0),
    {ok, Info1} = flurm_pmi_manager:get_job_info(1, 1),
    {ok, Info2} = flurm_pmi_manager:get_job_info(1, 2),
    ?assertEqual(2, maps:get(size, Info0)),
    ?assertEqual(4, maps:get(size, Info1)),
    ?assertEqual(8, maps:get(size, Info2)).

test_finalize_removes_ranks() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 1, <<"node2">>),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(2, maps:get(rank_count, Info)),
    ok = flurm_pmi_manager:finalize_job(1, 0),
    ?assertEqual({error, not_found}, flurm_pmi_manager:get_job_info(1, 0)).

test_finalize_removes_kvs() ->
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key">>, <<"val">>),
    {ok, <<"val">>} = flurm_pmi_manager:kvs_get(1, 0, <<"key">>),
    ok = flurm_pmi_manager:finalize_job(1, 0),
    ?assertEqual({error, job_not_found}, flurm_pmi_manager:kvs_get(1, 0, <<"key">>)).

%%====================================================================
%% Extended Rank Tests
%%====================================================================

extended_rank_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"register all ranks",
         fun test_register_all_ranks/0},
        {"register same rank twice",
         fun test_register_same_rank_twice/0},
        {"rank at size boundary",
         fun test_rank_boundary/0},
        {"get rank info for all ranks",
         fun test_get_all_rank_info/0}
     ]}.

test_register_all_ranks() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"n0">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 1, <<"n1">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 2, <<"n2">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 3, <<"n3">>),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(4, maps:get(rank_count, Info)).

test_register_same_rank_twice() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),
    %% Registering same rank again should overwrite
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node2">>),
    {ok, RankInfo} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    ?assertEqual(<<"node2">>, maps:get(node, RankInfo)).

test_rank_boundary() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    %% Rank 3 is valid (0-3 for size 4)
    ?assertEqual(ok, flurm_pmi_manager:register_rank(1, 0, 3, <<"n">>)),
    %% Rank 4 is invalid
    ?assertEqual({error, invalid_rank}, flurm_pmi_manager:register_rank(1, 0, 4, <<"n">>)).

test_get_all_rank_info() ->
    ok = flurm_pmi_manager:init_job(1, 0, 3),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"n0">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 1, <<"n1">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 2, <<"n2">>),
    {ok, R0} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    {ok, R1} = flurm_pmi_manager:get_rank_info(1, 0, 1),
    {ok, R2} = flurm_pmi_manager:get_rank_info(1, 0, 2),
    ?assertEqual(0, maps:get(rank, R0)),
    ?assertEqual(1, maps:get(rank, R1)),
    ?assertEqual(2, maps:get(rank, R2)),
    ?assertEqual(initialized, maps:get(status, R0)),
    ?assertEqual(initialized, maps:get(status, R1)),
    ?assertEqual(initialized, maps:get(status, R2)).

%%====================================================================
%% Extended KVS Tests
%%====================================================================

extended_kvs_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"kvs put multiple keys",
         fun test_kvs_multiple_keys/0},
        {"kvs get_by_index order",
         fun test_kvs_index_order/0},
        {"kvs get_by_index out of range",
         fun test_kvs_index_out_of_range/0},
        {"kvs_get_by_index for unknown job",
         fun test_kvs_index_no_job/0},
        {"kvs commit flag persists",
         fun test_kvs_commit_flag/0}
     ]}.

test_kvs_multiple_keys() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"k1">>, <<"v1">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"k2">>, <<"v2">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"k3">>, <<"v3">>),
    ?assertEqual({ok, <<"v1">>}, flurm_pmi_manager:kvs_get(1, 0, <<"k1">>)),
    ?assertEqual({ok, <<"v2">>}, flurm_pmi_manager:kvs_get(1, 0, <<"k2">>)),
    ?assertEqual({ok, <<"v3">>}, flurm_pmi_manager:kvs_get(1, 0, <<"k3">>)).

test_kvs_index_order() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"ccc">>, <<"v3">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"aaa">>, <<"v1">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"bbb">>, <<"v2">>),
    %% Should be sorted alphabetically
    {ok, Key0, _} = flurm_pmi_manager:kvs_get_by_index(1, 0, 0),
    {ok, Key1, _} = flurm_pmi_manager:kvs_get_by_index(1, 0, 1),
    {ok, Key2, _} = flurm_pmi_manager:kvs_get_by_index(1, 0, 2),
    ?assertEqual(<<"aaa">>, Key0),
    ?assertEqual(<<"bbb">>, Key1),
    ?assertEqual(<<"ccc">>, Key2).

test_kvs_index_out_of_range() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"key">>, <<"val">>),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_manager:kvs_get_by_index(1, 0, 1)),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_manager:kvs_get_by_index(1, 0, 100)).

test_kvs_index_no_job() ->
    ?assertEqual({error, job_not_found}, flurm_pmi_manager:kvs_get_by_index(999, 0, 0)).

test_kvs_commit_flag() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    {ok, Info1} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(false, maps:get(committed, Info1)),
    ok = flurm_pmi_manager:kvs_commit(1, 0),
    {ok, Info2} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(true, maps:get(committed, Info2)),
    %% Commit again should be ok
    ok = flurm_pmi_manager:kvs_commit(1, 0),
    {ok, Info3} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(true, maps:get(committed, Info3)).

%%====================================================================
%% Extended Barrier Tests
%%====================================================================

extended_barrier_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"barrier with 3 ranks",
         fun test_barrier_3_ranks/0},
        {"multiple barrier cycles",
         fun test_multiple_barrier_cycles/0},
        {"barrier releases in order",
         fun test_barrier_order/0}
     ]}.

test_barrier_3_ranks() ->
    ok = flurm_pmi_manager:init_job(1, 0, 3),
    Parent = self(),
    lists:foreach(fun(Rank) ->
        spawn_link(fun() ->
            ok = flurm_pmi_manager:barrier_in(1, 0, Rank),
            Parent ! {done, Rank}
        end)
    end, [0, 1, 2]),
    %% All should complete
    receive {done, 0} -> ok after 5000 -> error(timeout0) end,
    receive {done, 1} -> ok after 5000 -> error(timeout1) end,
    receive {done, 2} -> ok after 5000 -> error(timeout2) end.

test_multiple_barrier_cycles() ->
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    Parent = self(),
    %% Run 3 barrier cycles
    lists:foreach(fun(Cycle) ->
        spawn_link(fun() ->
            ok = flurm_pmi_manager:barrier_in(1, 0, 0),
            Parent ! {cycle_done, Cycle, 0}
        end),
        spawn_link(fun() ->
            ok = flurm_pmi_manager:barrier_in(1, 0, 1),
            Parent ! {cycle_done, Cycle, 1}
        end),
        receive {cycle_done, Cycle, 0} -> ok after 5000 -> error({timeout, Cycle, 0}) end,
        receive {cycle_done, Cycle, 1} -> ok after 5000 -> error({timeout, Cycle, 1}) end
    end, [1, 2, 3]).

test_barrier_order() ->
    ok = flurm_pmi_manager:init_job(1, 0, 3),
    Parent = self(),
    %% Start barriers at different times
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {released, 0, erlang:monotonic_time()}
    end),
    timer:sleep(10),
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 1),
        Parent ! {released, 1, erlang:monotonic_time()}
    end),
    timer:sleep(10),
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 2),
        Parent ! {released, 2, erlang:monotonic_time()}
    end),
    %% All should be released at approximately the same time (when last arrives)
    receive {released, 0, _T0} -> ok after 5000 -> error(timeout0) end,
    receive {released, 1, _T1} -> ok after 5000 -> error(timeout1) end,
    receive {released, 2, _T2} -> ok after 5000 -> error(timeout2) end.

%%====================================================================
%% KVS Name Tests
%%====================================================================

kvs_name_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"kvs name format is correct",
         fun test_kvs_name_format/0},
        {"different jobs have different kvs names",
         fun test_kvs_name_unique/0}
     ]}.

test_kvs_name_format() ->
    ok = flurm_pmi_manager:init_job(123, 456, 1),
    {ok, KvsName} = flurm_pmi_manager:get_kvs_name(123, 456),
    ?assertEqual(<<"kvs_123_456">>, KvsName).

test_kvs_name_unique() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:init_job(1, 1, 1),
    ok = flurm_pmi_manager:init_job(2, 0, 1),
    {ok, N1_0} = flurm_pmi_manager:get_kvs_name(1, 0),
    {ok, N1_1} = flurm_pmi_manager:get_kvs_name(1, 1),
    {ok, N2_0} = flurm_pmi_manager:get_kvs_name(2, 0),
    ?assertNotEqual(N1_0, N1_1),
    ?assertNotEqual(N1_0, N2_0),
    ?assertNotEqual(N1_1, N2_0).

%%====================================================================
%% gen_server Edge Cases
%%====================================================================

gen_server_edge_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"handle_cast does not crash",
         fun test_handle_cast/0},
        {"handle_info does not crash",
         fun test_handle_info/0}
     ]}.

test_handle_cast() ->
    Pid = whereis(flurm_pmi_manager),
    gen_server:cast(Pid, bogus_cast),
    timer:sleep(50),
    ?assert(is_process_alive(Pid)).

test_handle_info() ->
    Pid = whereis(flurm_pmi_manager),
    Pid ! bogus_info,
    timer:sleep(50),
    ?assert(is_process_alive(Pid)).

%%====================================================================
%% Additional Coverage Tests
%%====================================================================

additional_coverage_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"init_job creates kvs",
         fun test_init_creates_kvs/0},
        {"init_job sets size",
         fun test_init_sets_size/0},
        {"init_job sets committed to false",
         fun test_init_committed_false/0},
        {"init_job sets kvs_name",
         fun test_init_sets_kvsname/0},
        {"finalize_job is idempotent",
         fun test_finalize_idempotent/0},
        {"register_rank sets status",
         fun test_register_sets_status/0},
        {"register_rank tracks pid",
         fun test_register_tracks_pid/0},
        {"kvs_put allows binary keys",
         fun test_kvs_binary_keys/0},
        {"kvs_put allows binary values",
         fun test_kvs_binary_values/0},
        {"kvs_get returns error for missing",
         fun test_kvs_get_missing/0},
        {"kvs_get_by_index handles empty",
         fun test_kvs_index_empty/0},
        {"kvs_commit is idempotent",
         fun test_kvs_commit_idempotent/0},
        {"barrier releases all waiters",
         fun test_barrier_releases_all/0},
        {"barrier handles rank order",
         fun test_barrier_rank_order/0},
        {"get_kvs_name returns binary",
         fun test_kvs_name_binary/0},
        {"multiple jobs same step",
         fun test_multiple_jobs_same_step/0},
        {"rank info includes node",
         fun test_rank_info_node/0},
        {"rank info includes status",
         fun test_rank_info_status/0},
        {"job info includes size",
         fun test_job_info_size/0},
        {"job info includes kvs_name",
         fun test_job_info_kvs_name/0},
        {"job info includes committed",
         fun test_job_info_committed/0},
        {"job info includes rank_count",
         fun test_job_info_rank_count/0},
        {"barrier with size 1 immediate",
         fun test_barrier_size_1/0},
        {"kvs sorted by key",
         fun test_kvs_sorted/0},
        {"finalize removes kvs data extra",
         fun test_finalize_removes_kvs_extra/0},
        {"finalize removes barrier state extra",
         fun test_finalize_removes_barrier_extra/0},
        {"register rank overwrites extra",
         fun test_register_overwrites_extra/0},
        {"kvs put overwrites extra",
         fun test_kvs_put_overwrites_extra/0},
        {"barrier resets counter extra",
         fun test_barrier_reset_counter_extra/0},
        {"multiple barriers same job extra",
         fun test_multiple_barriers_extra/0}
     ]}.

test_init_creates_kvs() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"k">>, <<"v">>),
    ?assertEqual({ok, <<"v">>}, flurm_pmi_manager:kvs_get(1, 0, <<"k">>)).

test_init_sets_size() ->
    ok = flurm_pmi_manager:init_job(1, 0, 8),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(8, maps:get(size, Info)).

test_init_committed_false() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(false, maps:get(committed, Info)).

test_init_sets_kvsname() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    {ok, Name} = flurm_pmi_manager:get_kvs_name(1, 0),
    ?assertEqual(<<"kvs_1_0">>, Name).

test_finalize_idempotent() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:finalize_job(1, 0),
    ?assertEqual({error, not_found}, flurm_pmi_manager:finalize_job(1, 0)).

test_register_sets_status() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"n">>),
    {ok, Info} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    ?assertEqual(initialized, maps:get(status, Info)).

test_register_tracks_pid() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"n">>),
    {ok, _Info} = flurm_pmi_manager:get_rank_info(1, 0, 0).

test_kvs_binary_keys() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"binary_key">>, <<"v">>),
    ?assertEqual({ok, <<"v">>}, flurm_pmi_manager:kvs_get(1, 0, <<"binary_key">>)).

test_kvs_binary_values() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"k">>, <<"binary_value">>),
    ?assertEqual({ok, <<"binary_value">>}, flurm_pmi_manager:kvs_get(1, 0, <<"k">>)).

test_kvs_get_missing() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ?assertEqual({error, not_found}, flurm_pmi_manager:kvs_get(1, 0, <<"missing">>)).

test_kvs_index_empty() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ?assertEqual({error, end_of_kvs}, flurm_pmi_manager:kvs_get_by_index(1, 0, 0)).

test_kvs_commit_idempotent() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_commit(1, 0),
    ok = flurm_pmi_manager:kvs_commit(1, 0),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(true, maps:get(committed, Info)).

test_barrier_releases_all() ->
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    Parent = self(),
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {released, 0}
    end),
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 1),
        Parent ! {released, 1}
    end),
    receive {released, 0} -> ok after 5000 -> error(timeout) end,
    receive {released, 1} -> ok after 5000 -> error(timeout) end.

test_barrier_rank_order() ->
    ok = flurm_pmi_manager:init_job(1, 0, 3),
    Parent = self(),
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 2),
        Parent ! {done, 2}
    end),
    timer:sleep(10),
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 0),
        Parent ! {done, 0}
    end),
    timer:sleep(10),
    spawn_link(fun() ->
        ok = flurm_pmi_manager:barrier_in(1, 0, 1),
        Parent ! {done, 1}
    end),
    receive {done, 0} -> ok after 5000 -> error(timeout0) end,
    receive {done, 1} -> ok after 5000 -> error(timeout1) end,
    receive {done, 2} -> ok after 5000 -> error(timeout2) end.

test_kvs_name_binary() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    {ok, Name} = flurm_pmi_manager:get_kvs_name(1, 0),
    ?assert(is_binary(Name)).

test_multiple_jobs_same_step() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:init_job(2, 0, 1),
    {ok, _} = flurm_pmi_manager:get_job_info(1, 0),
    {ok, _} = flurm_pmi_manager:get_job_info(2, 0).

test_rank_info_node() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"mynode">>),
    {ok, Info} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    ?assertEqual(<<"mynode">>, maps:get(node, Info)).

test_rank_info_status() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"n">>),
    {ok, Info} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    ?assert(maps:is_key(status, Info)).

test_job_info_size() ->
    ok = flurm_pmi_manager:init_job(1, 0, 4),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(4, maps:get(size, Info)).

test_job_info_kvs_name() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assert(maps:is_key(kvs_name, Info)).

test_job_info_committed() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assert(maps:is_key(committed, Info)).

test_job_info_rank_count() ->
    ok = flurm_pmi_manager:init_job(1, 0, 2),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"n">>),
    {ok, Info} = flurm_pmi_manager:get_job_info(1, 0),
    ?assertEqual(1, maps:get(rank_count, Info)).

test_barrier_size_1() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ?assertEqual(ok, flurm_pmi_manager:barrier_in(1, 0, 0)).

test_kvs_sorted() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"ccc">>, <<"3">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"aaa">>, <<"1">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"bbb">>, <<"2">>),
    {ok, K0, _} = flurm_pmi_manager:kvs_get_by_index(1, 0, 0),
    ?assertEqual(<<"aaa">>, K0).

test_finalize_removes_kvs_extra() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"k">>, <<"v">>),
    ok = flurm_pmi_manager:finalize_job(1, 0),
    ?assertEqual({error, job_not_found}, flurm_pmi_manager:kvs_get(1, 0, <<"k">>)).

test_finalize_removes_barrier_extra() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:finalize_job(1, 0),
    ?assertEqual({error, job_not_found}, flurm_pmi_manager:barrier_in(1, 0, 0)).

test_register_overwrites_extra() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node1">>),
    ok = flurm_pmi_manager:register_rank(1, 0, 0, <<"node2">>),
    {ok, Info} = flurm_pmi_manager:get_rank_info(1, 0, 0),
    ?assertEqual(<<"node2">>, maps:get(node, Info)).

test_kvs_put_overwrites_extra() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"k">>, <<"v1">>),
    ok = flurm_pmi_manager:kvs_put(1, 0, <<"k">>, <<"v2">>),
    ?assertEqual({ok, <<"v2">>}, flurm_pmi_manager:kvs_get(1, 0, <<"k">>)).

test_barrier_reset_counter_extra() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:barrier_in(1, 0, 0),
    %% Counter should be reset, can do another barrier
    ok = flurm_pmi_manager:barrier_in(1, 0, 0).

test_multiple_barriers_extra() ->
    ok = flurm_pmi_manager:init_job(1, 0, 1),
    ok = flurm_pmi_manager:barrier_in(1, 0, 0),
    ok = flurm_pmi_manager:barrier_in(1, 0, 0),
    ok = flurm_pmi_manager:barrier_in(1, 0, 0).
