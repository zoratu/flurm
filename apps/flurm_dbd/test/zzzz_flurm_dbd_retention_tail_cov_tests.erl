%%%-------------------------------------------------------------------
%%% @doc Tail coverage tests for flurm_dbd_retention edge branches.
%%%-------------------------------------------------------------------
-module(zzzz_flurm_dbd_retention_tail_cov_tests).

-include_lib("eunit/include/eunit.hrl").

retention_tail_cov_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"prune_usage_records catches logger exception",
       fun prune_usage_records_catch_branch_test/0},
      {"find_oldest_timestamp skips zero end_time branch",
       fun oldest_timestamp_skip_zero_branch_test/0}
     ]}.

setup() ->
    catch meck:unload(lager),
    meck:new(lager, [passthrough, no_passthrough_cover, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),

    catch meck:unload(flurm_dbd_server),
    meck:new(flurm_dbd_server, [non_strict, no_link]),
    meck:expect(flurm_dbd_server, get_archived_jobs, fun() -> [] end),
    meck:expect(flurm_dbd_server, get_stats, fun() -> #{archived_jobs => 0} end),
    ok.

cleanup(_) ->
    catch meck:unload(flurm_dbd_server),
    catch meck:unload(lager),
    ok.

prune_usage_records_catch_branch_test() ->
    %% Force timestamp_to_period/1 to crash in calendar conversion path.
    Result = flurm_dbd_retention:prune_usage_records(-62167219201),
    ?assertEqual(0, maps:get(count, Result)),
    ?assertMatch([{usage_prune_error, _}], maps:get(errors, Result)).

oldest_timestamp_skip_zero_branch_test() ->
    meck:expect(flurm_dbd_server, get_archived_jobs,
                fun() ->
                    [#{end_time => 100}, #{end_time => 0}, #{end_time => 50}]
                end),
    meck:expect(flurm_dbd_server, get_stats, fun() -> #{archived_jobs => 3} end),
    Stats = flurm_dbd_retention:get_archive_stats(),
    ?assertEqual(50, maps:get(oldest_archive, Stats)),
    ?assertEqual(3, maps:get(archived_job_count, Stats)).
