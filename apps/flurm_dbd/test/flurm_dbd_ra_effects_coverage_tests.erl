%%%-------------------------------------------------------------------
%%% @doc Comprehensive coverage tests for flurm_dbd_ra_effects module.
%%%
%%% Tests the Ra effects module for distributed accounting.
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_ra_effects_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Tests
%%====================================================================

effects_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"job_recorded", fun test_job_recorded/0},
      {"became_leader", fun test_became_leader/0},
      {"became_follower", fun test_became_follower/0}
     ]}.

setup() ->
    meck:new(lager, [passthrough, no_link, non_strict]),
    meck:expect(lager, info, fun(_) -> ok end),
    meck:expect(lager, info, fun(_, _) -> ok end),
    meck:expect(lager, debug, fun(_, _) -> ok end),
    meck:expect(lager, warning, fun(_, _) -> ok end),
    meck:expect(lager, error, fun(_, _) -> ok end),
    ok.

cleanup(_) ->
    catch meck:unload(lager),
    ok.

test_job_recorded() ->
    JobRecord = #{job_id => 1, state => completed},
    ?assertEqual(ok, flurm_dbd_ra_effects:job_recorded(JobRecord)).

test_became_leader() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:became_leader(node())).

test_became_follower() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:became_follower(node())).
