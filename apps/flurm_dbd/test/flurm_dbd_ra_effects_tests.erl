%%%-------------------------------------------------------------------
%%% @doc Tests for flurm_dbd_ra_effects
%%%
%%% Covers Ra consensus side effects (logging-only functions).
%%% Verifies no crashes with various input types.
%%%
%%% Run with:
%%%   rebar3 eunit --module=flurm_dbd_ra_effects_tests
%%% @end
%%%-------------------------------------------------------------------
-module(flurm_dbd_ra_effects_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% EUnit Integration
%%====================================================================

ra_effects_test_() ->
    [
        {"job_recorded with map returns ok",
         fun test_job_recorded_map/0},
        {"job_recorded with atom returns ok",
         fun test_job_recorded_atom/0},
        {"job_recorded with binary returns ok",
         fun test_job_recorded_binary/0},
        {"job_recorded with integer returns ok",
         fun test_job_recorded_integer/0},
        {"job_recorded with undefined returns ok",
         fun test_job_recorded_undefined/0},
        {"job_recorded with complex term returns ok",
         fun test_job_recorded_complex/0},
        {"became_leader with node() returns ok",
         fun test_became_leader_node/0},
        {"became_leader with atom returns ok",
         fun test_became_leader_atom/0},
        {"became_leader with undefined returns ok",
         fun test_became_leader_undefined/0},
        {"became_follower with node() returns ok",
         fun test_became_follower_node/0},
        {"became_follower with atom returns ok",
         fun test_became_follower_atom/0},
        {"became_follower with undefined returns ok",
         fun test_became_follower_undefined/0}
    ].

%%====================================================================
%% Tests
%%====================================================================

test_job_recorded_map() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:job_recorded(#{job_id => 1, status => completed})).

test_job_recorded_atom() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:job_recorded(completed)).

test_job_recorded_binary() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:job_recorded(<<"job_42">>)).

test_job_recorded_integer() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:job_recorded(42)).

test_job_recorded_undefined() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:job_recorded(undefined)).

test_job_recorded_complex() ->
    Record = #{job_id => 1, state => completed, nodes => [<<"n1">>, <<"n2">>],
               time => {2024, 1, 1}, metadata => #{user => <<"admin">>}},
    ?assertEqual(ok, flurm_dbd_ra_effects:job_recorded(Record)).

test_became_leader_node() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:became_leader(node())).

test_became_leader_atom() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:became_leader('flurm@host1')).

test_became_leader_undefined() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:became_leader(undefined)).

test_became_follower_node() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:became_follower(node())).

test_became_follower_atom() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:became_follower('flurm@host2')).

test_became_follower_undefined() ->
    ?assertEqual(ok, flurm_dbd_ra_effects:became_follower(undefined)).
